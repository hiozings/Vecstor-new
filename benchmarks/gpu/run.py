"""Frozen-quality GPU/CPU RPC comparison and snapshot-bound profile export."""
import argparse
from concurrent.futures import ThreadPoolExecutor
from contextlib import ExitStack
import ctypes
import hashlib
import json
import os
from pathlib import Path
import runpy
import shutil
import sys
import threading
import time

ROOT=Path(__file__).resolve().parents[2];sys.path.insert(0,str(ROOT/'src'))
import numpy as np
from vecstor.datasets import load_dataset
from vecstor.query.search_options import SearchOptions
from vecstor.rpc.acceptance import ServiceProcess
from vecstor.rpc.sql_client import SessionClient

helper=runpy.run_path(str(ROOT/'benchmarks/p3/calibrate.py'))
bind_profile=runpy.run_path(str(ROOT/'benchmarks/p3/validate_quality.py'))['bind_profile']


def process_stats(pid):
    try:
        lines=Path(f'/proc/{pid}/status').read_text().splitlines()
        rss=int(next(x.split()[1] for x in lines if x.startswith('VmRSS:')))*1024
        parts=Path(f'/proc/{pid}/stat').read_text().rsplit(')',1)[1].split()
        return rss,(int(parts[11])+int(parts[12]))/os.sysconf('SC_CLK_TCK')
    except (OSError,StopIteration):return 0,0.


class DeviceMemory:
    def __init__(self):
        self.lib=ctypes.CDLL('libnvidia-ml.so.1');self.handle=ctypes.c_void_p()
        if self.lib.nvmlInit_v2() or self.lib.nvmlDeviceGetHandleByIndex_v2(0,ctypes.byref(self.handle)):raise RuntimeError('NVML initialization failed')
    def used(self):
        class Memory(ctypes.Structure):_fields_=[('total',ctypes.c_ulonglong),('free',ctypes.c_ulonglong),('used',ctypes.c_ulonglong)]
        value=Memory()
        if self.lib.nvmlDeviceGetMemoryInfo(self.handle,ctypes.byref(value)):raise RuntimeError('NVML memory query failed')
        return value.used
    def close(self):self.lib.nvmlShutdown()


def run(calibration_dir,output_dir,samples=3,repeats=3,concurrencies=(1,4,8)):
    calibration=Path(calibration_dir);out=Path(output_dir);out.mkdir(parents=True,exist_ok=False)
    config=json.loads((calibration/'config.json').read_text());offline=json.loads((calibration/'report.json').read_text())
    if min(samples,repeats,*concurrencies)<1 or samples>config['evaluation_count']:raise ValueError('Invalid benchmark sampling')
    supported=[g for g in offline['groups'] if g['status']=='passed'];expected={};digests={}
    for line in (calibration/'heldout.jsonl').read_text().splitlines():
        r=json.loads(line);key=(r['size'],r['distribution'],r['workload']['selectivity'],r['workload']['k'])
        digests.setdefault(key,[]).append(r['query_digest'])
        if r['query_id']<samples:expected[(*key,r['query_id'])]=r
    groups=[];profiles=[];processes=[];memory=DeviceMemory()
    formal=config['calibration_count']>=200 and config['evaluation_count']>=500
    source_hash=hashlib.sha256(b''.join(p.read_bytes() for p in sorted((ROOT/'src').rglob('*.py')))).hexdigest()
    (out/'config.json').write_text(json.dumps(dict(calibration=str(calibration.resolve()),samples=samples,repeats=repeats,concurrencies=list(concurrencies),source_sha256=source_hash,profile_validation=formal),indent=2))
    try:
        with (out/'cases.jsonl').open('x') as raw:
            for n,distribution in sorted({(g['size'],g['distribution']) for g in supported}):
                scenario=[g for g in supported if (g['size'],g['distribution'])==(n,distribution)]
                source=calibration/f'{n}-{distribution}';directory=out/f'{n}-{distribution}';root=directory/'storage';(root/'imports').mkdir(parents=True)
                data=load_dataset(source/'dataset.npz');fingerprint=helper['dataset_digest'](data)
                shutil.copy2(source/'dataset.npz',root/'imports/source.npz')
                with np.load(source/'queries.npz',allow_pickle=False) as saved:queries=saved['evaluation'][:samples]
                build=json.loads((source/'build.json').read_text())['config']
                with ExitStack() as cleanup:
                    storage=ServiceProcess(['storage','--data-root',str(root.resolve()),'--listen','127.0.0.1:0'],directory/'storage.log');cleanup.callback(storage.stop);processes.append(storage.process)
                    def compute_start(name,profile_file=None):
                        args=['compute','--storage',storage.address,'--listen','127.0.0.1:0','--timeout','60','--isolate-storage-dir',str(root.resolve())]
                        if profile_file:args+=['--quality-profiles',str(profile_file.resolve())]
                        process=ServiceProcess(args,directory/f'{name}.log');cleanup.callback(process.stop);processes.append(process.process);return process
                    compute=compute_start('prepare')
                    with SessionClient(compute.address,timeout=60) as c:
                        c.open_database('quality',True);c.import_dataset('source.npz','items')
                        built=c.build_index('items',nlist=build['nlist'],seed=build['seed'],device='gpu')
                    local_profiles=[]
                    for g in scenario:
                        work=g['workload'];s,k=work['selectivity'],work['k'];key=(n,distribution,s,k)
                        frozen=json.loads((source/f'{s:g}-k{k}.json').read_text())
                        if frozen['dataset_sha256']!=fingerprint:raise ValueError('GPU calibration dataset fingerprint mismatch')
                        if g['settings']!=frozen['settings']:raise ValueError('Frozen GPU parameter mismatch')
                        for i,q in enumerate(queries):
                            if expected[(*key,i)]['query_digest']!=helper['digest_query'](q):raise ValueError('GPU query identity mismatch')
                        if formal:
                            sql=f'SELECT id FROM items WHERE value < {int(n*s)} ORDER BY vector_distance(vector,{json.dumps(queries[0].tolist())}) LIMIT {k}'
                            profile_id=f'g{n}-{distribution}-{str(s).replace(".","p")}-k{k}'
                            profile=bind_profile(profile_id,built,sql,frozen,g['summary'],digests[key]);profile.update(format_version=2,execution=frozen['execution'])
                            profile['evidence']['scope']=f'empirical mean Recall@{k}; not a per-query guarantee'
                            local_profiles.append(profile);profiles.append(profile)
                    if formal:
                        profile_file=directory/'quality-profiles.json';profile_file.write_text(json.dumps({'format_version':2,'profiles':local_profiles},indent=2))
                        compute.stop();compute=compute_start('compute',profile_file)
                    with SessionClient(compute.address,timeout=60) as c:
                        c.open_database('quality');built=c.build_index('items',nlist=build['nlist'],seed=build['seed'],device='gpu')
                    pids=[compute.process.pid,storage.process.pid,built['worker_pid']]
                    (directory/'build.json').write_text(json.dumps(built,indent=2))
                    for g in scenario:
                        s,k=g['workload']['selectivity'],g['workload']['k'];key=(n,distribution,s,k)
                        profile_id=f'g{n}-{distribution}-{str(s).replace(".","p")}-k{k}' if formal else None
                        for device in ('cpu','gpu'):
                            for concurrency in concurrencies:
                                def execute(i):
                                    with SessionClient(compute.address,timeout=60) as client:
                                        client.open_database('quality')
                                        client.search_options=SearchOptions(mode='approx',path='search-first',fallback='error',device=device,
                                            quality_profile_id=profile_id if device=='gpu' else None,**({} if formal and device=='gpu' else g['settings']))
                                        q=queries[i%samples]
                                        sql=f'SELECT id,vector_distance(vector,{json.dumps(q.tolist())}) AS distance FROM items WHERE value < {int(n*s)} ORDER BY distance LIMIT {k}'
                                        start=time.perf_counter();result=client.execute(sql);elapsed=(time.perf_counter()-start)*1000
                                        ids=[r[0] for r in result['rows']];truth=expected[(*key,i%samples)]['truth_ids'];plan=result['execution_plan']
                                        assert plan['actual_device']==device and plan['actual_mode']=='approx'
                                        if device=='gpu':assert ids==expected[(*key,i%samples)]['returned_ids']
                                        assert len(ids)==k and len(set(ids))==k
                                        loc=np.searchsorted(data.ids,ids);assert np.all(data.values[loc]<int(n*s))
                                        delta=data.vectors[loc].astype(np.float64)-q.astype(np.float64)
                                        assert np.allclose([r[1] for r in result['rows']],np.sum(delta*delta,axis=1),rtol=1e-12,atol=1e-12)
                                        return dict(device=device,size=n,distribution=distribution,selectivity=s,k=k,concurrency=concurrency,elapsed_ms=elapsed,
                                            recall=len(set(ids)&set(truth))/len(truth),ids=ids,plan=plan)
                                execute(0) # warmed connection-independent index path
                                baseline=[process_stats(pid) for pid in pids];peak=[v[0] for v in baseline];vram_before=memory.used();vram_peak=[vram_before];stop=threading.Event()
                                def sample():
                                    while not stop.wait(.005):
                                        for j,pid in enumerate(pids):peak[j]=max(peak[j],process_stats(pid)[0])
                                        vram_peak[0]=max(vram_peak[0],memory.used())
                                watcher=threading.Thread(target=sample,daemon=True);watcher.start();began=time.perf_counter()
                                try:
                                    with ThreadPoolExecutor(concurrency) as pool:records=list(pool.map(execute,range(samples*repeats)))
                                finally:stop.set();watcher.join()
                                elapsed=time.perf_counter()-began;after=[process_stats(pid) for pid in pids]
                                for record in records:raw.write(json.dumps(record)+'\n')
                                raw.flush();times=[r['elapsed_ms'] for r in records]
                                group=dict(device=device,size=n,distribution=distribution,selectivity=s,k=k,concurrency=concurrency,queries=len(records),
                                    p50_ms=float(np.percentile(times,50)),p95_ms=float(np.percentile(times,95)),throughput=len(records)/elapsed,
                                    mean_recall=float(np.mean([r['recall'] for r in records])),cpu_seconds=[max(0,a[1]-b[1]) for a,b in zip(after,baseline)],
                                    process_order=['compute','storage','gpu_worker'],baseline_rss_bytes=[v[0] for v in baseline],peak_rss_bytes=peak,
                                    device_vram_before_bytes=vram_before,device_vram_peak_bytes=vram_peak[0],
                                    median_rpc=float(np.median([r['plan']['counts']['rpc_calls'] for r in records])),
                                    median_payload_bytes=float(np.median([r['plan']['counts']['rpc_payload_bytes'] for r in records])),
                                    cpu_comparison_quality_passed=g['cpu_summary']['incomplete_queries']==0 and g['cpu_summary']['mean_recall']>=.95)
                                groups.append(group);print(n,distribution,s,k,device,concurrency,round(group['p50_ms'],2),flush=True)
    finally:memory.close()
    (out/'quality-profiles.json').write_text(json.dumps({'format_version':2,'profiles':profiles},indent=2))
    report=dict(passed=True,groups=groups,excluded_groups=[g for g in offline['groups'] if g['status']!='passed'],
                profile_validation=formal,scope='warm localhost; per-group short-run P95; device-wide VRAM sampled every5ms, includes other programs',
                processes_running_after_cleanup=[p.pid for p in processes if p.poll() is None])
    assert not report['processes_running_after_cleanup'];(out/'report.json').write_text(json.dumps(report,indent=2));return report

if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--calibration-dir',required=True);p.add_argument('--output-dir',required=True)
    a=p.parse_args();r=run(a.calibration_dir,a.output_dir);print(json.dumps({'passed':r['passed'],'groups':len(r['groups'])}))
