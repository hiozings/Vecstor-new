"""Independent processes, PTY, artifact isolation and explicit recovery evidence."""
from concurrent.futures import ThreadPoolExecutor
from contextlib import ExitStack
import hashlib
import json
import os
from pathlib import Path
import platform
import sys
import time

import grpc
import numpy as np

from vecstor.data import Dataset
from vecstor.datasets import generate_dataset, save_dataset
from vecstor.index.types import BuildConfig
from vecstor.query.search_options import SearchOptions
from vecstor.rpc import sql_pb2 as pb, sql_pb2_grpc as rpc
from vecstor.rpc.acceptance import ServiceProcess
from vecstor.rpc.shell_acceptance import TerminalProcess
from vecstor.rpc.sql_client import SessionClient


def oracle(data, query, limit, threshold=None):
    selected=np.flatnonzero(np.ones(data.size,dtype=bool) if threshold is None else data.values<threshold)
    delta=data.vectors[selected].astype(np.float64)-query.astype(np.float64)
    distances=np.sum(delta*delta,axis=1,dtype=np.float64)
    order=np.lexsort((data.ids[selected],distances))[:limit]
    return [[int(data.ids[selected[i]]),float(distances[i])] for i in order]


def run_acceptance(output_dir, rows=10000, dimension=128, seed=20261016, require_gpu=False,
                   distribution='independent', concurrency=(), observer=None):
    if rows<1024 or dimension<1:raise ValueError('P4 acceptance requires at least 1024 rows and positive dimension')
    output=Path(output_dir).resolve();output.mkdir(parents=True,exist_ok=False)
    root=output/'storage'
    report=dict(passed=False,rows=rows,dimension=dimension,distribution=distribution,require_gpu=require_gpu,
        scope='published-index lifecycle correctness; deterministic crash/transport matrix is verified separately by pytest',
        cases=[],isolation=[],phases=[],nodes={},query_costs=[])
    processes=[];terminals=[];events=[]
    beginning=time.monotonic()
    def event(name,**values):
        item=dict(event=name,elapsed_ms=(time.monotonic()-beginning)*1000,**values)
        events.append(item)
        with (output/'events.jsonl').open('a') as target:target.write(json.dumps(item)+'\n')
    def phase(name,operation):
        start=time.monotonic();result=operation();elapsed=(time.monotonic()-start)*1000
        report['phases'].append(dict(name=name,elapsed_ms=elapsed));event(name,phase_ms=elapsed)
        return result
    def check(name,condition):
        if not condition:raise AssertionError(name)
        report['cases'].append(dict(name=name,status='PASS'))
    try:
        if require_gpu:
            import faiss
            if faiss.get_num_gpus()<1:raise RuntimeError('P4 GPU acceptance requires a real GPU')
        original=generate_dataset(rows,dimension,seed,distribution)
        data=Dataset(original.ids+2**53+1,original.categories,original.timestamps,original.values,original.vectors)
        save_dataset(data,root/'imports/source.npz')
        query=np.zeros(dimension,dtype=np.float32)
        vector=json.dumps(query.tolist())
        sql=f'SELECT id,vector_distance(vector,{vector}) AS distance FROM items WHERE value>=0 ORDER BY distance LIMIT 10'
        nlist=BuildConfig().resolved(rows).nlist
        source_root=Path(__file__).resolve().parents[3]
        hashes={str(p.relative_to(source_root)):hashlib.sha256(p.read_bytes()).hexdigest()
                for p in sorted((source_root/'src').rglob('*.py'))}
        (output/'source-sha256.json').write_text(json.dumps(hashes,indent=2)+'\n')
        (output/'config.json').write_text(json.dumps(dict(rows=rows,dimension=dimension,seed=seed,distribution=distribution,
            nlist=nlist,concurrency=list(concurrency),require_gpu=require_gpu),indent=2)+'\n')
        (output/'environment.json').write_text(json.dumps(dict(python=sys.version.split()[0],platform=platform.platform(),
            numpy=np.__version__),indent=2)+'\n')
        with ExitStack() as cleanup:
            def storage_start(name):
                process=ServiceProcess(['storage','--data-root',str(root),'--listen','127.0.0.1:0'],output/f'{name}.log')
                cleanup.callback(process.stop);processes.append(process.process)
                if observer:observer(name,process.process.pid)
                return process
            storage=storage_start('storage')
            def compute_start(name,preload=None):
                args=['compute','--storage',storage.address,'--listen','127.0.0.1:0','--timeout','180',
                      '--index-source','published','--isolate-storage-dir',str(root)]
                if preload:args+=['--index-preload',str(preload)]
                process=ServiceProcess(args,output/f'{name}.log')
                cleanup.callback(process.stop);processes.append(process.process)
                if observer:observer(name,process.process.pid)
                isolation=next(e for e in process.events() if e.get('event')=='filesystem_isolation')
                check(name+'_storage_denied',isolation['directory_denied'] and isolation['file_denied'])
                report['isolation'].append(isolation)
                return process
            def client_for(process):
                client=cleanup.enter_context(SessionClient(process.address,timeout=180))
                client.open_database('demo')
                return client
            bootstrap=compute_start('bootstrap')
            terminal=TerminalProcess(bootstrap.address,output/'session.txt',request_timeout=180,command_timeout=180);cleanup.callback(terminal.stop);terminals.append(terminal.process)
            for command in ('.open demo --create','.mode json','.import source.npz items',
                            f'.index publish items --nlist {nlist} --operation-id first --timeout 60'):
                terminal.command(command)
            terminal.quit();terminal.stop()
            check('pty_publish', 'Error:' not in (output/'session.txt').read_text())
            bootstrap_client=client_for(bootstrap)
            first=bootstrap_client.index_publication_status('first')
            check('first_publication_committed',first['state']=='COMMITTED')
            report['first_publication']=first
            replay=bootstrap_client.publish_index('items',nlist=nlist,operation_id='first',timeout=60)
            check('idempotent_publish',replay['descriptor']==first['descriptor'] and replay['generation']==first['generation'] and replay['statistics']['trained'] is False)
            bootstrap.stop()
            a,b=compute_start('compute-a'),compute_start('compute-b')
            ca,cb=client_for(a),client_for(b)
            for name,client in (('A',ca),('B',cb)):
                loaded=phase('load-'+name,lambda c=client:c.load_index('items',timeout=60))
                check('load-no-training-'+name,not loaded['statistics']['trained'] and loaded['statistics']['snapshot_calls']==0 and loaded['statistics']['artifact_download_bytes']>0)
                report['nodes'][name]=dict(artifact_id=loaded['descriptor']['artifact_id'],generation=loaded['generation'])
                client.search_options=SearchOptions(mode='approx',path='search-first',nprobe=nlist,fallback='error')
            check('two_node_artifact_identity',report['nodes']['A']==report['nodes']['B'])
            expected=oracle(data,query,10)
            ra,rb=ca.execute(sql),cb.execute(sql)
            check('two_node_answers',ra['rows']==rb['rows']==expected)
            check('actual_approximate',ra['execution_plan']['actual_mode']==rb['execution_plan']['actual_mode']=='approx')
            raw_cases=[]
            for selectivity in (.001,.01,.1,.5,1.):
                threshold=int(rows*selectivity)
                for limit in (10,100):
                    statement=f'SELECT id,vector_distance(vector,{vector}) AS distance FROM items WHERE value<{threshold} ORDER BY distance LIMIT {limit}'
                    ca.search_options=SearchOptions(mode='approx',path='filter-first',nprobe=nlist,fallback='error',max_candidates=rows)
                    result=ca.execute(statement)
                    truth=oracle(data,query,limit,threshold)
                    check(f'filtered-{selectivity}-{limit}',result['rows']==truth)
                    raw_cases.append(dict(selectivity=selectivity,k=limit,query=query.tolist(),sql=statement,truth=truth,result=result))
            (output/'cases.jsonl').write_text(''.join(json.dumps(case)+'\n' for case in raw_cases))
            for count in concurrency:
                with ExitStack() as sessions:
                    clients=[]
                    for _ in range(count):
                        c=sessions.enter_context(SessionClient(a.address,timeout=60));c.open_database('demo')
                        c.search_options=SearchOptions(mode='approx',path='search-first',nprobe=nlist,fallback='error')
                        c.execute(sql);clients.append(c)
                    def batch(client):
                        samples=[]
                        for _ in range(4):
                            t=time.perf_counter();r=client.execute(sql);elapsed=(time.perf_counter()-t)*1000
                            if r['rows']!=expected:raise AssertionError('Concurrent query differs from oracle')
                            samples.append(dict(ms=elapsed,plan=r['execution_plan']))
                        return samples
                    t=time.perf_counter()
                    with ThreadPoolExecutor(count) as pool:groups=list(pool.map(batch,clients))
                    elapsed=time.perf_counter()-t;samples=[s for group in groups for s in group]
                    report['query_costs'].append(dict(concurrency=count,requests=len(samples),p50_ms=float(np.percentile([s['ms'] for s in samples],50)),
                        p95_ms=float(np.percentile([s['ms'] for s in samples],95)),throughput=len(samples)/elapsed,samples=samples,
                        scope='same fixed query, warm artifact, full nprobe; descriptive lifecycle workload, not general ANN speed'))
            cutoff=int(data.ids[-8])
            phase('batch-update',lambda:cb.execute(f"UPDATE items SET category='updated',value=0,vector={vector} WHERE id>={cutoff}"))
            t_data_commit=time.monotonic()
            changed_vectors=data.vectors.copy();changed_values=data.values.copy();changed_categories=data.categories.astype(object).copy()
            mask=data.ids>=cutoff;changed_vectors[mask]=0;changed_values[mask]=0;changed_categories[mask]='updated'
            changed=Dataset(data.ids,changed_categories,data.timestamps,changed_values,changed_vectors)
            expected_new=oracle(changed,query,10)
            ca.search_options=SearchOptions(mode='approx',path='search-first',nprobe=nlist,fallback='error')
            try:ca.execute(sql)
            except ValueError as error:check('new_revision_strict_failure','index_not_published' in str(error))
            else:raise AssertionError('Stale ANN served a new data revision')
            ca.search_options=SearchOptions(mode='approx',path='search-first',nprobe=nlist,fallback='exact')
            gap=ca.execute(sql)
            check('write_gap_exact_fallback',gap['rows']==expected_new and gap['execution_plan']['actual_mode']=='exact' and gap['execution_plan']['artifact_id'] is None)
            second=phase('publish-after-update',lambda:ca.publish_index('items',nlist=nlist,operation_id='second',timeout=60))
            report['second_publication']=second
            report['lag_to_publish_ms']=(time.monotonic()-t_data_commit)*1000
            for client in (ca,cb):client.load_index('items',timeout=60)
            report['lag_to_ready_ms']=(time.monotonic()-t_data_commit)*1000
            for node,client in (('A',ca),('B',cb)):
                client.search_options=SearchOptions(mode='approx',path='search-first',nprobe=nlist,fallback='error')
                result=client.execute(sql)
                check('new_revision_answer-'+node,result['rows']==expected_new and result['execution_plan']['artifact_id']==second['descriptor']['artifact_id'])
            preload=output/'preload.json'
            preload.write_text(json.dumps(dict(format_version=1,targets=[dict(database='demo',table='items',device='cpu')]))+'\n')
            a.stop();a=phase('compute-a-restart-preload',lambda:compute_start('compute-a-restarted',preload));ca=client_for(a)
            ready=next(e for e in a.events() if e.get('event')=='ready')
            recovered=ready['preloaded_indexes'][0]
            report['recovery']=recovered['statistics']
            check('restart_without_training',not report['recovery']['trained'] and report['recovery']['snapshot_calls']==0 and report['recovery']['artifact_download_bytes']>0)
            check('other_node_survives_restart',cb.execute(sql)['rows']==expected_new)
            ca.search_options=SearchOptions(mode='approx',path='search-first',nprobe=nlist,fallback='error')
            check('preload_same_artifact',ca.execute(sql)['execution_plan']['artifact_id']==second['descriptor']['artifact_id'])
            with grpc.insecure_channel(storage.address) as channel:
                old=rpc.SQLStorageStub(channel).BeginRead(pb.BeginReadRequest(database_id=ca.database_id,table='items',ttl_seconds=60),timeout=5)
            storage.stop()
            try:ca.execute(sql)
            except ValueError as error:check('storage_stop_fails_closed','UNAVAILABLE' in str(error) or 'DEADLINE_EXCEEDED' in str(error))
            else:raise AssertionError('Query succeeded without storage')
            a.stop();b.stop()
            storage=storage_start('storage-restarted')
            with grpc.insecure_channel(storage.address) as channel:
                try:rpc.SQLStorageStub(channel).FetchVectors(pb.FetchRequest(database_id=old.database_id,token=old.token,ids=[int(data.ids[0])]),timeout=5)
                except grpc.RpcError as error:check('old_token_invalid',error.code()==grpc.StatusCode.FAILED_PRECONDITION)
                else:raise AssertionError('Restart accepted an old lease')
            a=compute_start('compute-a-full-restart',preload);b=compute_start('compute-b-full-restart',preload)
            ca,cb=client_for(a),client_for(b)
            for node,client in (('A',ca),('B',cb)):
                inspection=client.inspect_published_index('items')
                check('full_restart_same_artifact-'+node,inspection['cpu_ready'] and inspection['descriptor']['artifact_id']==second['descriptor']['artifact_id'])
                report['nodes'][node]=dict(artifact_id=inspection['descriptor']['artifact_id'],generation=inspection['generation'])
            if require_gpu:
                gpu=phase('gpu-restore',lambda:ca.load_index('items',device='gpu',timeout=60))
                replica=next(v for v in ca.list_indexes('items') if v['device']=='gpu')
                if observer:observer('gpu-worker',replica['worker_pid'])
                check('gpu_artifact_isolation',replica['isolation']['artifact_files_denied']>=2)
                ca.search_options=SearchOptions(mode='approx',path='search-first',device='gpu',nprobe=nlist,fallback='error')
                actual=ca.execute(sql)
                check('gpu_restore_identity',actual['rows']==expected_new and actual['execution_plan']['artifact_id']==second['descriptor']['artifact_id'])
                report['gpu_restore']=gpu;report['gpu_replica']=replica
            preview=ca.prune_index_artifacts(keep_last=1)
            pruned=ca.prune_index_artifacts(keep_last=1,dry_run=False,operation_id='prune-old')
            check('prune_obsolete_artifact',first['descriptor']['artifact_id'] in pruned['deleted'] and second['descriptor']['artifact_id'] in pruned['protected'])
            report['prune']=dict(preview=preview,result=pruned)
            report['passed']=True
    except Exception as error:
        report['error']=str(error)
        raise
    finally:
        report['elapsed_seconds']=time.monotonic()-beginning
        report['processes_running_after_cleanup']=[p.pid for p in [*processes,*terminals] if p.poll() is None]
        report['process_ids']=[p.pid for p in processes]
        report['gpu_verified']=require_gpu and report['passed']
        (output/'report.json').write_text(json.dumps(report,ensure_ascii=False,indent=2)+'\n')
        (output/'isolation.json').write_text(json.dumps(report['isolation'],indent=2)+'\n')
    return report
