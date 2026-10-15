"""Warm-session ablations with fixed ANN parameters and repeated query clusters."""
import argparse
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from contextlib import ExitStack
import hashlib,json,os
from pathlib import Path
import runpy,shutil,sys,threading,time
ROOT=Path(__file__).resolve().parents[2];sys.path.insert(0,str(ROOT/'src'))
import numpy as np
from vecstor.datasets import load_dataset
from vecstor.rpc.sql_client import SessionClient
from vecstor.rpc.acceptance import ServiceProcess
costs=runpy.run_path(str(ROOT/'benchmarks/gpu/run.py'))
helpers=runpy.run_path(str(ROOT/'benchmarks/p3/calibrate.py'))
DEFAULT_CASES=[(10000,'independent',.1,10),(100000,'independent',.1,10),(100000,'independent',1.,100)]
VARIANTS={'uncached':(False,[]),'cached':(True,[]),
          'batch4':(True,['--gpu-batch-size','4','--gpu-batch-wait-ms','1']),
          'batch8':(True,['--gpu-batch-size','8','--gpu-batch-wait-ms','1']),
          'fused':(True,['--fused-candidates']),
          'fused-batch4':(True,['--fused-candidates','--gpu-batch-size','4','--gpu-batch-wait-ms','1'])}


def intervals(records):
    by_query=defaultdict(list)
    for r in records:by_query[r['query_id']].append(r['elapsed_ms'])
    values=list(by_query.values());rng=np.random.default_rng(71);medians=[]
    for _ in range(500):
        draw=rng.integers(0,len(values),len(values));medians.append(np.median([x for i in draw for x in values[i]]))
    return np.percentile(medians,[2.5,97.5]).tolist()


def run(calibration_dir,output_dir,selected=None,variants=('uncached','cached'),samples=500,repeats=3,concurrencies=(1,4,8)):
    cal=Path(calibration_dir);out=Path(output_dir);out.mkdir(parents=True,exist_ok=False)
    selected=selected if selected is not None else DEFAULT_CASES
    config=json.loads((cal/'config.json').read_text());report=json.loads((cal/'report.json').read_text())
    if samples>config['evaluation_count'] or min(samples,repeats,*concurrencies)<1:raise ValueError('Invalid sample/repeat configuration')
    cases={(g['size'],g['distribution'],g['workload']['selectivity'],g['workload']['k']):g for g in report['groups'] if g['status']=='passed'}
    if any(tuple(case) not in cases for case in selected):raise ValueError('Selected workload lacks complete frozen quality evidence')
    expected={}
    for line in (cal/'heldout.jsonl').read_text().splitlines():
        r=json.loads(line);key=(r['size'],r['distribution'],r['workload']['selectivity'],r['workload']['k'])
        if key in map(tuple,selected) and r['query_id']<samples:expected[(*key,r['query_id'])]=r
    (out/'config.json').write_text(json.dumps(dict(calibration=str(cal.resolve()),selected=selected,variants=list(variants),samples=samples,repeats=repeats,concurrencies=list(concurrencies),
        source_sha256=hashlib.sha256(b''.join(p.read_bytes() for p in sorted((ROOT/'src').rglob('*.py')))).hexdigest(),runner_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        session='one reusable client per concurrent worker; alternate CPU/GPU order each repeat; ANN parameters frozen'),indent=2))
    groups=[];processes=[];memory=costs['DeviceMemory']()
    try:
        with (out/'cases.jsonl').open('x') as raw:
            for n,distribution in sorted({(x[0],x[1]) for x in selected}):
                source=cal/f'{n}-{distribution}';directory=out/f'{n}-{distribution}';root=directory/'storage';(root/'imports').mkdir(parents=True)
                data=load_dataset(source/'dataset.npz');shutil.copy2(source/'dataset.npz',root/'imports/source.npz')
                with np.load(source/'queries.npz',allow_pickle=False) as saved:queries=saved['evaluation'][:samples]
                build=json.loads((source/'build.json').read_text())['config']
                with ExitStack() as cleanup:
                    storage=ServiceProcess(['storage','--data-root',str(root.resolve()),'--listen','127.0.0.1:0'],directory/'storage.log');cleanup.callback(storage.stop);processes.append(storage.process)
                    imported=False
                    for variant in variants:
                        cache,flags=VARIANTS[variant];dest=directory/variant;dest.mkdir()
                        compute=ServiceProcess(['compute','--storage',storage.address,'--listen','127.0.0.1:0','--timeout','60','--isolate-storage-dir',str(root.resolve()),*flags],dest/'compute.log')
                        cleanup.callback(compute.stop);processes.append(compute.process)
                        with SessionClient(compute.address,timeout=60) as setup:
                            setup.open_database('quality',True)
                            if not imported:setup.import_dataset('source.npz','items');imported=True
                            built=setup.build_index('items',nlist=build['nlist'],seed=build['seed'],device='gpu')
                        (dest/'build.json').write_text(json.dumps(built,indent=2));pids=[compute.process.pid,storage.process.pid,built['worker_pid']]
                        for key in [tuple(x) for x in selected if x[0]==n and x[1]==distribution]:
                            _,_,s,k=key;g=cases[key];settings=g['settings'];frozen=json.loads((source/f'{s:g}-k{k}.json').read_text())
                            if frozen['settings']!=settings or frozen['dataset_sha256']!=helpers['dataset_digest'](data):raise ValueError('Frozen evidence mismatch')
                            for i,q in enumerate(queries):
                                if expected[(*key,i)]['query_digest']!=helpers['digest_query'](q):raise ValueError('Heldout query mismatch')
                            sqls=[f'SELECT id,vector_distance(vector,{json.dumps(q.tolist())}) AS distance FROM items WHERE value < {int(n*s)} ORDER BY distance LIMIT {k}' for q in queries]
                            suffix={d:" SETTINGS device='"+d+"',mode='approx',path='search-first',fallback='error',"+','.join(f'{name}={value}' for name,value in settings.items()) for d in ('cpu','gpu')}
                            for concurrency in concurrencies:
                                with ExitStack() as clients_cleanup:
                                    clients=[SessionClient(compute.address,timeout=60,cache_query_capabilities=cache) for _ in range(concurrency)]
                                    for client in clients:clients_cleanup.callback(client.close);client.open_database('quality')
                                    def execute(client,qi,device,repeat):
                                        start=time.perf_counter();result=client.execute(sqls[qi]+suffix[device]);elapsed=(time.perf_counter()-start)*1000
                                        ids=[r[0] for r in result['rows']];truth=expected[(*key,qi)]['truth_ids'];plan=result['execution_plan']
                                        assert plan['actual_device']==device and plan['actual_mode']=='approx'
                                        if device=='gpu':assert ids==expected[(*key,qi)]['returned_ids']
                                        assert len(ids)==k and len(set(ids))==k
                                        loc=np.searchsorted(data.ids,ids);assert np.array_equal(data.ids[loc],ids) and np.all(data.values[loc]<int(n*s))
                                        distances=np.sum((data.vectors[loc].astype(np.float64)-queries[qi].astype(np.float64))**2,axis=1)
                                        assert np.allclose([r[1] for r in result['rows']],distances,rtol=1e-12,atol=1e-12)
                                        return dict(size=n,distribution=distribution,selectivity=s,k=k,variant=variant,device=device,concurrency=concurrency,
                                            query_id=qi,repeat=repeat,elapsed_ms=elapsed,recall=len(set(ids)&set(truth))/len(truth),plan=plan)
                                    for client in clients:
                                        for device in ('cpu','gpu'):execute(client,0,device,-1)
                                    all_rows={'cpu':[],'gpu':[]};resources={d:[] for d in all_rows}
                                    for repeat in range(repeats):
                                        order=np.random.default_rng(900+repeat).permutation(samples)
                                        for device in (('cpu','gpu') if repeat%2==0 else ('gpu','cpu')):
                                            base=[costs['process_stats'](pid) for pid in pids];peak=[x[0] for x in base];vram=[memory.used()];stop=threading.Event()
                                            def sample():
                                                while not stop.wait(.005):
                                                    for j,pid in enumerate(pids):peak[j]=max(peak[j],costs['process_stats'](pid)[0])
                                                    vram[0]=max(vram[0],memory.used())
                                            watcher=threading.Thread(target=sample,daemon=True);watcher.start();start=time.perf_counter()
                                            def work(i):return [execute(clients[i],int(qi),device,repeat) for qi in order[i::concurrency]]
                                            try:
                                                with ThreadPoolExecutor(concurrency) as pool:rows=[r for batch in pool.map(work,range(concurrency)) for r in batch]
                                            finally:stop.set();watcher.join()
                                            wall=time.perf_counter()-start;after=[costs['process_stats'](pid) for pid in pids]
                                            resources[device].append(dict(wall_seconds=wall,peak_rss_bytes=peak,device_vram_peak_bytes=vram[0],cpu_seconds=[max(0,a[1]-b[1]) for a,b in zip(after,base)]))
                                            all_rows[device]+=rows
                                            for r in rows:raw.write(json.dumps(r)+'\n')
                                            raw.flush()
                                    for device,rows in all_rows.items():
                                        times=[r['elapsed_ms'] for r in rows];plans=[r['plan'] for r in rows]
                                        group=dict(size=n,distribution=distribution,selectivity=s,k=k,variant=variant,device=device,concurrency=concurrency,queries=len(rows),
                                            connections_created=concurrency,p50_ms=float(np.median(times)),p95_ms=float(np.percentile(times,95)),p50_cluster_bootstrap_95=intervals(rows),
                                            mean_recall=float(np.mean([r['recall'] for r in rows])),throughput=len(rows)/sum(x['wall_seconds'] for x in resources[device]),
                                            client_control_calls=sum(p['client']['control_rpc_calls'] for p in plans),
                                            median_storage_rpc=float(np.median([p['counts']['rpc_calls'] for p in plans])),
                                            timings={name:float(np.median([p.get(name,0) or 0 for p in plans])) for name in ('server_admission_ms','rpc_wait_ms','compute_elapsed_ms','faiss_search_ms','gpu_host_ms','gpu_queue_ms','gpu_ipc_ms','rerank_ms')},
                                            resources=resources[device])
                                        assert group['mean_recall']>=.95
                                        groups.append(group);print(n,s,k,variant,device,concurrency,round(group['p50_ms'],2),'control',group['client_control_calls'],flush=True)
                        compute.stop()
    finally:memory.close()
    result=dict(passed=True,groups=groups,processes_running_after_cleanup=[p.pid for p in processes if p.poll() is None],
        scope='warm persistent sessions; explicit equal frozen parameters; query-cluster bootstrap; no new ANN calibration or CPU fallback')
    assert not result['processes_running_after_cleanup'];(out/'report.json').write_text(json.dumps(result,indent=2));return result

if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--calibration-dir',required=True);parser.add_argument('--output-dir',required=True)
    parser.add_argument('--variants',nargs='+',choices=list(VARIANTS),default=['uncached','cached']);parser.add_argument('--samples',type=int,default=500);parser.add_argument('--repeats',type=int,default=3)
    args=parser.parse_args();result=run(args.calibration_dir,args.output_dir,variants=args.variants,samples=args.samples,repeats=args.repeats)
    print(json.dumps({'passed':result['passed'],'groups':len(result['groups'])}))
