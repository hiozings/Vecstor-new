"""Controlled native-index and RPC cost studies at frozen recall settings."""
import argparse
from concurrent.futures import ThreadPoolExecutor
from contextlib import ExitStack
from itertools import product
import json
import os
from pathlib import Path
import resource
import runpy
import sys
from threading import Event,Thread
import time

sys.path.insert(0,str(Path(__file__).resolve().parents[2]/'src'))
import faiss
import numpy as np
from vecstor.datasets import load_dataset
from vecstor.index.faiss_backend import FaissIndex
from vecstor.query.search_options import SearchOptions
from vecstor.rpc.acceptance import ServiceProcess
from vecstor.rpc.sql_client import SessionClient


CAL=runpy.run_path(str(Path(__file__).with_name('calibrate.py')))


def native_study(calibration_dir,output_dir,size=100000,distribution='independent',nlists=(128,256,512),
                 iterations=(10,20),sample_caps=(256,512),calibration_count=64,evaluation_count=128,selectivities=(.01,.1,1.)):
    output=Path(output_dir);output.mkdir(parents=True,exist_ok=False)
    source=Path(calibration_dir)/f'{size}-{distribution}';data=load_dataset(source/'dataset.npz')
    with np.load(source/'queries.npz',allow_pickle=False) as q:
        if calibration_count>len(q['calibration']) or evaluation_count>len(q['evaluation']):raise ValueError('Too many study queries')
        cal=q['calibration'][:calibration_count];held=q['evaluation'][:evaluation_count]
    truths_cal=CAL['prepare_truths'](data,cal,selectivities,10);truths_eval=CAL['prepare_truths'](data,held,selectivities,10)
    models=[]
    for nlist,niter,cap in product(nlists,iterations,sample_caps):
        if not 1<=nlist<=data.size or niter<1 or cap<1:raise ValueError('Invalid training configuration')
        faiss.omp_set_num_threads(2)
        started=time.perf_counter();cpu=time.process_time()
        native=faiss.IndexIVFFlat(faiss.IndexFlatL2(data.dimension),data.dimension,nlist,faiss.METRIC_L2)
        native.cp.seed=20261013;native.cp.niter=niter;native.cp.max_points_per_centroid=cap
        native.train(data.vectors);native.add_with_ids(data.vectors,data.ids)
        index=FaissIndex(native,float(np.abs(data.vectors).max()))
        model={'nlist':nlist,'niter':niter,'max_points_per_centroid':cap,'byte_size':index.byte_size,
               'build_wall_ms':(time.perf_counter()-started)*1000,'build_cpu_ms':(time.process_time()-cpu)*1000,'groups':[]}
        faiss.omp_set_num_threads(1)
        grid=sorted({max(1,int(np.ceil(nlist*f))) for f in [.25,.5,.75,.875,.9375,1.]})
        for s in selectivities:
            workload={'selectivity':float(s),'path':'filter-first','k':10}
            frozen=CAL['calibrate'](data,cal,grid,workload,index=index,prepared=truths_cal[float(s)])
            faiss.cvar.indexIVF_stats.reset()
            result=CAL['evaluate'](data,held,frozen,index=index,prepared=truths_eval[float(s)])
            model['groups'].append({'selectivity':s,'settings':frozen['settings'],'summary':result['summary'],
                                    'passed':result['passed'],'faiss_stats_ndis_per_query':int(faiss.cvar.indexIVF_stats.ndis)/len(held)})
        models.append(model);del index,native
        print('native study',nlist,niter,cap,flush=True)
    report={'scope':'exploratory native-only training comparison, no model publication; failed-quality points retained',
            'size':size,'distribution':distribution,'calibration_queries':len(cal),'evaluation_queries':len(held),
            'models':models,'process_peak_rss_bytes':resource.getrusage(resource.RUSAGE_SELF).ru_maxrss*1024}
    (output/'report.json').write_text(json.dumps(report,indent=2));return report


def _process(pid):
    try:
        status=Path(f'/proc/{pid}/status').read_text().splitlines()
        rss=int(next(line.split()[1] for line in status if line.startswith('VmRSS:')))*1024
        stat=Path(f'/proc/{pid}/stat').read_text().rsplit(')',1)[1].split()
        cpu=(int(stat[11])+int(stat[12]))/os.sysconf('SC_CLK_TCK')
        return rss,cpu
    except (OSError,StopIteration):return 0,0.


def rpc_study(calibration_dir,rpc_dir,output_dir,size=100000,distribution='independent',samples=3,repeats=3,
              selectivities=(.001,.01,.1,1.),concurrencies=(1,4,8),modes=(('legacy',False),('legacy',True),('text',False),('text',True))):
    calibration=Path(calibration_dir);existing=Path(rpc_dir);output=Path(output_dir);output.mkdir(parents=True,exist_ok=False)
    source=calibration/f'{size}-{distribution}';root=(existing/f'{size}-{distribution}/storage').resolve()
    with np.load(source/'queries.npz',allow_pickle=False) as q:queries=q['evaluation'][:samples]
    if len(queries)!=samples or samples<1 or repeats<1 or any(c<1 for c in concurrencies):raise ValueError('Invalid cost sample count')
    build=json.loads((source/'build.json').read_text());expected={}
    for line in (calibration/'heldout.jsonl').read_text().splitlines():
        r=json.loads(line)
        if r['size']==size and r['distribution']==distribution and r['query_id']<samples:
            expected[(r['workload']['selectivity'],r['workload']['path'],r['query_id'])]=r['returned_ids']
    groups=[];processes=[]
    with (output/'cases.jsonl').open('x') as raw:
        for binding,cached in modes:
            directory=output/f'{binding}-cache-{int(cached)}';directory.mkdir()
            with ExitStack() as cleanup:
                args=['storage','--data-root',str(root),'--listen','127.0.0.1:0','--candidate-binding',binding]
                if not cached:args.append('--no-candidate-plan-cache')
                storage=ServiceProcess(args,directory/'storage.log');cleanup.callback(storage.stop);processes.append(storage.process)
                compute=ServiceProcess(['compute','--storage',storage.address,'--listen','127.0.0.1:0','--timeout','180',
                                        '--quality-profiles',str((existing/'quality-profiles.json').resolve())],directory/'compute.log')
                cleanup.callback(compute.stop);processes.append(compute.process)
                with SessionClient(compute.address,timeout=180) as setup:
                    setup.open_database('quality');setup.build_index('items',nlist=build['nlist'],seed=build['config']['seed'],timeout=60)
                for s in selectivities:
                    for path in ('filter-first','search-first'):
                        profile_id=f'q{size}-{distribution}-{s:g}-{path}'.replace('.','p')
                        sqls=[f'SELECT id, vector_distance(vector,{json.dumps(q.tolist())}) AS distance FROM items WHERE value < {int(size*s)} ORDER BY distance LIMIT 10' for q in queries]
                        for concurrent in concurrencies:
                            clients=[SessionClient(compute.address,timeout=180) for _ in range(concurrent)]
                            try:
                                for client in clients:
                                    client.open_database('quality');client.list_quality_profiles()
                                    client.search_options=SearchOptions(mode='approx',path=path,quality_profile_id=profile_id,fallback='error')
                                # Explicit warmup, excluded from recorded query costs.
                                clients[0].execute(sqls[0])
                                stop=Event();peak=[_process(compute.process.pid)[0],_process(storage.process.pid)[0]]
                                baseline_rss=peak.copy();cpu_before=[_process(p.process.pid)[1] for p in [compute,storage]]
                                def monitor():
                                    while not stop.wait(.005):
                                        for i,p in enumerate([compute,storage]):peak[i]=max(peak[i],_process(p.process.pid)[0])
                                watcher=Thread(target=monitor,daemon=True);watcher.start()
                                jobs=[i for _ in range(repeats) for i in range(samples)]
                                def work(worker):
                                    records=[]
                                    for qi in jobs[worker::concurrent]:
                                        started=time.perf_counter();result=clients[worker].execute(sqls[qi]);elapsed=(time.perf_counter()-started)*1000
                                        if [r[0] for r in result['rows']]!=expected[(float(s),path,qi)]:raise AssertionError('Cost run changed frozen result')
                                        records.append({'query_id':qi,'wall_ms':elapsed,'plan':result['execution_plan']})
                                    return records
                                began=time.perf_counter()
                                try:
                                    with ThreadPoolExecutor(concurrent) as pool:records=[r for batch in pool.map(work,range(concurrent)) for r in batch]
                                finally:stop.set();watcher.join(timeout=2)
                                duration=time.perf_counter()-began;cpu_after=[_process(p.process.pid)[1] for p in [compute,storage]]
                                identity={'binding':binding,'plan_cache':cached,'selectivity':s,'path':path,'concurrency':concurrent}
                                for r in records:raw.write(json.dumps(dict(identity,**r))+'\n')
                                raw.flush()
                                group=dict(identity,samples=len(records),p50_ms=float(np.percentile([r['wall_ms'] for r in records],50)),
                                    p95_ms=float(np.percentile([r['wall_ms'] for r in records],95)),throughput=len(records)/duration,
                                    compute_cpu_seconds=cpu_after[0]-cpu_before[0],storage_cpu_seconds=cpu_after[1]-cpu_before[1],
                                    compute_peak_rss_bytes=peak[0],storage_peak_rss_bytes=peak[1],baseline_rss_bytes=baseline_rss,
                                    median_rpc=float(np.median([r['plan']['counts']['rpc_calls'] for r in records])),
                                    median_payload_bytes=float(np.median([r['plan']['counts']['rpc_payload_bytes'] for r in records])))
                                groups.append(group);print('cost',identity,round(group['p50_ms'],2),flush=True)
                            finally:
                                for client in clients:client.close()
    report={'passed':True,'scope':'warm, frozen-quality, localhost cost ablation; RSS sampled every 5ms; short-run P95 is descriptive',
            'groups':groups,'processes_running_after_cleanup':any(p.poll() is None for p in processes)}
    (output/'report.json').write_text(json.dumps(report,indent=2));return report


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('study',choices=['native','rpc'])
    parser.add_argument('--calibration-dir',required=True);parser.add_argument('--rpc-dir');parser.add_argument('--output-dir',required=True)
    args=parser.parse_args()
    if args.study=='native':report=native_study(args.calibration_dir,args.output_dir)
    else:
        if not args.rpc_dir:parser.error('--rpc-dir is required')
        report=rpc_study(args.calibration_dir,args.rpc_dir,args.output_dir)
    print(json.dumps({'completed':True,'groups':len(report.get('groups',report.get('models',[])))}))
