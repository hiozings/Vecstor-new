"""Paired cached/projected read experiments; full manifests remain opt-in."""
import argparse
from concurrent.futures import ThreadPoolExecutor
from contextlib import ExitStack
from dataclasses import replace
import hashlib
from itertools import groupby
import json
import os
from pathlib import Path
import platform
import runpy
import shutil
import sys
import time

ROOT=Path(__file__).resolve().parents[2]
sys.path.insert(0,str(ROOT/'src'));sys.path.insert(0,str(Path(__file__).resolve().parent))
import numpy as np
from data_read_cases import matrix_cases,variants
from vecstor.datasets import generate_dataset,save_dataset,load_dataset
from vecstor.index.types import BuildConfig
from vecstor.query.search_options import SearchOptions
from vecstor.rpc import sql_pb2 as pb,sql_pb2_grpc as rpc
from vecstor.rpc.acceptance import ServiceProcess
from vecstor.rpc.p4_index_acceptance import oracle
from vecstor.rpc.sql_client import SessionClient
import grpc

Monitor=runpy.run_path(str(ROOT/'benchmarks/p4/index_lifecycle.py'))['Monitor']
start_proxy=runpy.run_path(str(ROOT/'tests/support/delayed_storage.py'))['start_proxy']
validate_quality=runpy.run_path(str(ROOT/'benchmarks/p4/validate_artifact_quality.py'))['run']


def _write(path,value):path.write_text(json.dumps(value,ensure_ascii=False,indent=2,allow_nan=False)+'\n')


def _sql(data,query,spec):
    return f'SELECT id,vector_distance(vector,{json.dumps(query.tolist())}) AS distance FROM "items" WHERE value < {int(data.size*spec["selectivity"])} ORDER BY distance LIMIT {spec["k"]}'


def _signature(plan):
    return {key:plan.get(key) for key in ('actual_mode','actual_path','actual_device','fallback_reason','artifact_id','rounds')}


def _options(spec,nlist,profile=None,settings=None):
    options=dict(mode=spec['mode'],path=spec['path'],device=spec['device'],data_cache='off',metadata_projection='full',candidate_data_path='separate')
    if spec['mode']=='approx':
        options.update(nprobe=nlist,fallback='error' if spec['path']=='filter-first' or spec['device']=='gpu' else 'exact')
    if settings:options.update(settings)
    if profile:options['quality_profile_id']=profile
    return SearchOptions(**options)


def query_pools(query_pool,extra_pool,sample_count):
    queries=query_pool[:sample_count];other=extra_pool[:sample_count]
    hotspot=np.array([query_pool[(i-i//5-1)%min(20,len(query_pool))] if i%5 else extra_pool[i] for i in range(sample_count)],dtype='<f4')
    return dict(cold=queries,warm=queries,independent=other,hotset=hotspot)


def _summary(name,samples,elapsed):
    ms=[s['ms'] for s in samples];plans=[s['plan'] for s in samples]
    return dict(name=name,requests=len(samples),p50_ms=float(np.median(ms)),
        p95_ms=float(np.percentile(ms,95)) if len(ms)>=20 else None,throughput=len(samples)/elapsed,
        mean_recall=float(np.mean([s['recall'] for s in samples])),incomplete=sum(s['incomplete'] for s in samples),
        exact_fallbacks=sum(p['actual_mode']=='exact' and p['requested_mode']=='approx' for p in plans),
        rpc_calls=sum(p['counts']['rpc_calls'] for p in plans),rpc_payload_bytes=sum(p['counts']['rpc_payload_bytes'] for p in plans),
        lookup_rows=sum(p['data_cache']['lookup_rows'] for p in plans),hit_rows=sum(p['data_cache']['hit_rows'] for p in plans),
        bypass_rows=sum(p['data_cache']['bypass_rows'] for p in plans),evictions=sum(p['data_cache']['evictions'] for p in plans),
        vectors_fetched=sum(p['counts']['vectors_fetched'] for p in plans),
        observed_accounted_bytes=max(p['data_cache']['accounted_bytes'] for p in plans),
        scope='descriptive quantiles; no P95 for fewer than20 samples; raw phase timings retained')


def run_data_read_matrix(output_dir,*,tier='smoke',seed=20261019,repeats=3,gpu_calibration_dir=None,case_ids=None):
    if type(repeats) is not int or repeats<3:raise ValueError('At least three repeats are required')
    if type(seed) is not int or seed<0:raise ValueError('Seed must be a nonnegative integer')
    all_cases=matrix_cases(tier)
    selected=list(range(len(all_cases))) if case_ids is None else list(case_ids)
    if not selected or len(set(selected))!=len(selected) or any(type(i) is not int or not 0<=i<len(all_cases) for i in selected):
        raise ValueError('Invalid matrix case selection')
    indexed_cases=[(i,all_cases[i]) for i in selected]
    cases=[case for _,case in indexed_cases]
    output=Path(output_dir).resolve();output.mkdir(parents=True,exist_ok=False)
    calibration=Path(gpu_calibration_dir or ROOT/'results/gpu-calibration-20261015')
    report=dict(passed=False,tier=tier,runs=[],skipped=[],scope='paired transfer/cache experiments; source-frozen query answers and explicit partial coverage',
        full_matrix_status='NOT_RUN' if tier in ('smoke','representative') or case_ids is not None else 'REQUESTED',
        selected_case_ids=selected,planned_case_count=len(all_cases),cases=cases)
    processes=[];start=time.monotonic();rng=np.random.default_rng(seed);reference_cache={};truth_cache={}
    _write(output/'config.json',dict(tier=tier,seed=seed,data_seed=20261016,repeats=repeats,cases=cases,
        selected_case_ids=selected,gpu_calibration=str(calibration),query_sequences='cold/warm same; independent seed+1; hotset80% first20 cold queries',os_cache='not_flushed'))
    _write(output/'source-sha256.json',{str(p.relative_to(ROOT)):hashlib.sha256(p.read_bytes()).hexdigest() for directory in ('src','benchmarks/p4','tests/support') for p in sorted((ROOT/directory).rglob('*.py'))})
    _write(output/'environment.json',dict(python=sys.version,platform=platform.platform(),numpy=np.__version__))
    groupkey=lambda c:(c['device'],c['rows'],c['distribution'],c['dimension'])
    ordered=sorted(indexed_cases,key=lambda pair:groupkey(pair[1]))
    try:
        with (output/'cases.jsonl').open('x') as raw:
            for key,items in groupby(ordered,key=lambda pair:groupkey(pair[1])):
                specs=list(items);device,n,distribution,dimension=key
                directory=output/f'{device}-{n}-{distribution}';directory.mkdir()
                root=directory/'storage'
                if device=='gpu':
                    source=calibration/f'{n}-{distribution}'
                    if not (calibration/'report.json').exists() or not (source/'dataset.npz').exists():
                        raise ValueError('GPU frozen calibration is required; pass --gpu-calibration-dir')
                    old=json.loads((calibration/'report.json').read_text())
                    supported={(g['workload']['selectivity'],g['workload']['k']) for g in old['groups'] if g['size']==n and g['distribution']==distribution and g['status']=='passed'}
                    retained=[]
                    for identity,spec in specs:
                        if (spec['selectivity'],spec['k']) in supported:retained.append((identity,spec))
                        else:report['skipped'].append(dict(case=identity,reason='frozen_GPU_workload_not_supported'))
                    specs=retained
                    if not specs:continue
                    data=load_dataset(source/'dataset.npz')
                    build=json.loads((source/'build.json').read_text())['config']
                    with np.load(source/'queries.npz',allow_pickle=False) as saved:query_pool=saved['evaluation'].copy()
                else:
                    data=generate_dataset(n,dimension,seed=20261016,distribution=distribution)
                    build=dict(nlist=BuildConfig().resolved(n).nlist,seed=20261013)
                    query_pool=np.random.default_rng(seed).random((500,dimension),dtype=np.float32)
                extra_pool=np.random.default_rng(seed+1).random((500,dimension),dtype=np.float32)
                np.savez(directory/'queries.npz',cold=query_pool,independent=extra_pool)
                save_dataset(data,root/'imports/source.npz')
                positions={int(identity):j for j,identity in enumerate(data.ids)}
                reference_cache={};truth_cache={}
                profiles={};settings={};profile_file=None
                with ExitStack() as cleanup:
                    storage=ServiceProcess(['storage','--data-root',str(root),'--listen','127.0.0.1:0'],directory/'storage.log')
                    cleanup.callback(storage.stop);processes.append(storage.process)
                    def compute(name,*,address=None,capacity=64,profile=None):
                        args=['compute','--storage',address or storage.address,'--listen','127.0.0.1:0','--timeout','180',
                            '--index-source','published','--isolate-storage-dir',str(root),'--fused-candidates',
                            '--data-cache-mib',str(capacity),'--data-cache-scan-admission','on']
                        if profile:args+=['--quality-profiles',str(profile)]
                        process=ServiceProcess(args,directory/(name+'.log'));cleanup.callback(process.stop);processes.append(process.process)
                        return process
                    prepare=compute('prepare')
                    with SessionClient(prepare.address,timeout=180) as client:
                        client.open_database('bench',True);client.import_dataset('source.npz','items')
                        publication=client.publish_index('items',nlist=build['nlist'],seed=build['seed'],operation_id='publish')
                        client.load_index('items',device=device)
                    if device=='gpu':
                        documents=[]
                        for selectivity,k in sorted({(s['selectivity'],s['k']) for _,s in specs}):
                            frozen=json.loads((source/f'{selectivity:g}-k{k}.json').read_text())
                            settings[(selectivity,k)]=frozen['settings']
                            target=directory/f'quality-{selectivity:g}-{k}'
                            validated=validate_quality(calibration,prepare.address,'bench','items',target,'gpu',selectivity,k)
                            if not validated['passed']:raise ValueError('Artifact GPU quality validation failed')
                            profile=json.loads((target/'quality-profiles.json').read_text())['profiles'][0]
                            documents.append(profile);profiles[(selectivity,k)]=profile['profile_id']
                        profile_file=directory/'quality-profiles.json';_write(profile_file,dict(format_version=3,profiles=documents))
                        prepare.stop();prepare=compute('reference',profile=profile_file)
                        with SessionClient(prepare.address,timeout=180) as client:
                            client.open_database('bench');client.load_index('items',device='gpu')
                    # Precompute references, then stop this process before measuring variants.
                    reference=cleanup.enter_context(SessionClient(prepare.address,timeout=180));reference.open_database('bench')
                    prepared_cases=[]
                    for case_id,spec in specs:
                        pools=query_pools(query_pool,extra_pool,spec['samples'])
                        work=(spec['selectivity'],spec['k'])
                        options=_options(spec,build['nlist'],profiles.get(work),settings.get(work))
                        reference.search_options=options
                        expected={};truths={}
                        for phase in spec['patterns']:
                            for query in pools[phase]:
                                digest=hashlib.sha256(query.tobytes()).hexdigest()
                                refkey=(*key,spec['selectivity'],spec['k'],spec['mode'],spec['path'],digest)
                                if refkey not in reference_cache:
                                    result=reference.execute(_sql(data,query,spec))
                                    reference_cache[refkey]=(result['rows'],_signature(result['execution_plan']))
                                expected[digest]=reference_cache[refkey]
                                truthkey=(*key,spec['selectivity'],spec['k'],digest)
                                if truthkey not in truth_cache:truth_cache[truthkey]=oracle(data,query,spec['k'],int(data.size*spec['selectivity']))
                                truths[digest]=truth_cache[truthkey]
                        prepared_cases.append((case_id,spec,pools,options,expected,truths))
                    reference.close();prepare.stop()
                    with (directory/'reference.jsonl').open('x') as references:
                        for refkey,(rows_value,signature) in reference_cache.items():
                            truthkey=(*refkey[:6],refkey[-1])
                            references.write(json.dumps(dict(key=refkey,rows=rows_value,signature=signature,truth=truth_cache[truthkey]))+'\n')
                    for case_id,spec,pools,options,expected,truths in prepared_cases:
                        proxy=None;address=storage.address;probe=[]
                        if spec['delay_ms'] is not None:
                            proxy=start_proxy(address,spec['delay_ms']);cleanup.callback(proxy.close);address=proxy.address
                            with grpc.insecure_channel(address) as channel:
                                stub=rpc.SQLStorageStub(channel)
                                for _ in range(3):
                                    began=time.perf_counter();lease=stub.BeginRead(pb.BeginReadRequest(database_id=reference.database_id,table='items',ttl_seconds=10),timeout=10)
                                    probe.append((time.perf_counter()-began)*1000)
                                    stub.EndRead(pb.LeaseRequest(database_id=reference.database_id,token=lease.token),timeout=10)
                        for repeat in range(1,repeats+1):
                            order=variants(spec);rng.shuffle(order)
                            for cache,projection,path in order:
                                name=f'case-{case_id}-r{repeat}-{cache}-{projection}-{path}'
                                monitor=Monitor(root/'indexes',device=='gpu');monitor.observe('storage',storage.process.pid)
                                if proxy:monitor.observe('proxy-and-benchmark-driver',os.getpid())
                                process=None;entry=dict(case=case_id,repeat=repeat,**spec,cache=cache,projection=projection,candidate_path=path,
                                    phases=[],artifact_id=publication['descriptor']['artifact_id'],probe_begin_read_roundtrip_ms=probe)
                                try:
                                    process=compute(name,address=address,capacity=spec['capacity_mib'],profile=profile_file)
                                    monitor.observe('compute',process.process.pid)
                                    entry['isolation']=next(e for e in process.events() if e.get('event')=='filesystem_isolation')
                                    if not (entry['isolation']['directory_denied'] and entry['isolation']['file_denied']):raise AssertionError('Compute filesystem isolation failed')
                                    with ExitStack() as clients_cleanup:
                                        clients=[clients_cleanup.enter_context(SessionClient(process.address,timeout=180)) for _ in range(spec['concurrency'])]
                                        active_options=replace(options,data_cache=cache,metadata_projection=projection,candidate_data_path=path)
                                        for client in clients:client.open_database('bench');client.search_options=active_options
                                        loaded=clients[0].load_index('items',device=device)
                                        if device=='gpu':monitor.observe('gpu-worker',next(r['worker_pid'] for r in clients[0].list_indexes('items') if r['device']=='gpu'))
                                        for client in clients:client.execute(_sql(data,pools[spec['patterns'][0]][0],spec))
                                        entry['session_preflight_primed']=True
                                        clients[0].clear_data_cache();entry['cache_start_entries']=clients[0].data_cache_stats()['database']['entry_count']
                                        for phase in spec['patterns']:
                                            phase_queries=pools[phase]
                                            def worker(worker_id):
                                                records=[];client=clients[worker_id]
                                                for i in range(worker_id,len(phase_queries),len(clients)):
                                                    query=phase_queries[i];digest=hashlib.sha256(query.tobytes()).hexdigest()
                                                    began=time.perf_counter();result=client.execute(_sql(data,query,spec));elapsed=(time.perf_counter()-began)*1000
                                                    records.append(dict(case=case_id,repeat=repeat,cache=cache,projection=projection,candidate_path=path,
                                                        phase=phase,query_id=i,query_digest=digest,query=query.tolist(),ms=elapsed,
                                                        plan=result['execution_plan'],rows=result['rows']))
                                                return records
                                            began=time.perf_counter()
                                            with ThreadPoolExecutor(len(clients)) as pool:groups=list(pool.map(worker,range(len(clients))))
                                            elapsed=time.perf_counter()-began;samples=sorted((r for group in groups for r in group),key=lambda r:r['query_id'])
                                            # Correctness checks are outside the timed throughput interval.
                                            for row in samples:
                                                digest=row['query_digest'];expected_rows,signature=expected[digest]
                                                plan=row['plan'];truth=truths[digest];query=np.asarray(row['query'],dtype='<f4')
                                                if row['rows']!=expected_rows or _signature(plan)!=signature:
                                                    raise AssertionError(f'Optimization changed query answer/plan: {name}/{phase}/{row["query_id"]}')
                                                if spec['mode']=='exact' and row['rows']!=truth:raise AssertionError('Exact result differs from NumPy oracle')
                                                ids={r[0] for r in row['rows']};truth_ids={r[0] for r in truth}
                                                for identity,distance in row['rows']:
                                                    pos=positions[identity]
                                                    if not data.values[pos]<int(data.size*spec['selectivity']):raise AssertionError('Predicate mismatch')
                                                    delta=data.vectors[pos].astype(np.float64)-query.astype(np.float64)
                                                    if distance!=float(np.sum(delta*delta,dtype=np.float64)):raise AssertionError('Distance mismatch')
                                                if spec['device']=='gpu' and plan.get('quality_profile',{}).get('state')!='applied':raise AssertionError('GPU quality profile not applied')
                                                row.update(recall=len(ids&truth_ids)/len(truth_ids) if truth_ids else 1.,incomplete=len(row['rows'])<len(truth),equivalent=True)
                                                raw.write(json.dumps(row,allow_nan=False)+'\n')
                                            raw.flush();entry['phases'].append(_summary(phase,samples,elapsed))
                                        entry['cache_end']=clients[0].data_cache_stats()
                                finally:
                                    entry['resources']=monitor.close()
                                    if process:process.stop()
                                report['runs'].append(entry);_write(output/'report.json',report)
                            print(json.dumps(dict(case=case_id,repeat=repeat,runs=len(report['runs']))),flush=True)
                        if proxy:proxy.close()
                    prepare.stop()
        report['passed']=bool(report['runs']) and (not any(c['device']=='gpu' for c in cases) or any(r['device']=='gpu' for r in report['runs']))
        if not report['passed']:raise ValueError('No completed requested backend matrix')
        if report['full_matrix_status']=='REQUESTED':report['full_matrix_status']='COMPLETED_REQUESTED_TIER'
    except Exception as error:
        report['error']=str(error);raise
    finally:
        report['elapsed_seconds']=time.monotonic()-start
        report['processes_running_after_cleanup']=[p.pid for p in processes if p.poll() is None]
        _write(output/'report.json',report)
    return report


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output-dir',required=True)
    parser.add_argument('--tier',choices=('smoke','representative','correctness','costs','gpu'),default='smoke')
    parser.add_argument('--seed',type=int,default=20261019)
    parser.add_argument('--repeats',type=int,default=3)
    parser.add_argument('--gpu-calibration-dir')
    parser.add_argument('--case-ids',type=int,nargs='+',help='Explicit manifest subset; selection is recorded, never called full coverage')
    args=parser.parse_args()
    result=run_data_read_matrix(args.output_dir,tier=args.tier,seed=args.seed,repeats=args.repeats,gpu_calibration_dir=args.gpu_calibration_dir,case_ids=args.case_ids)
    print(json.dumps(dict(passed=result['passed'],runs=len(result['runs']),skipped=result['skipped'])))
