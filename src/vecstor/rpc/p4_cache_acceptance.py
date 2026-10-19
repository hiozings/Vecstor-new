"""Real-process cache lifecycle checks; deterministic races remain pytest evidence."""
from contextlib import ExitStack
from dataclasses import replace
import hashlib
import json
from pathlib import Path
import platform
import sys
import time

import grpc
import numpy as np

from vecstor.data import Dataset
from vecstor.datasets import generate_dataset,save_dataset
from vecstor.index.types import BuildConfig
from vecstor.query.search_options import SearchOptions
from vecstor.rpc.acceptance import ServiceProcess
from vecstor.rpc.shell_acceptance import TerminalProcess
from vecstor.rpc.sql_client import SessionClient
from vecstor.rpc.p4_index_acceptance import oracle


def run_cache_acceptance(output_dir,rows=10000,dimension=128,require_gpu=False):
    if rows<1024 or dimension<1:raise ValueError('Cache acceptance needs at least1024 rows and positive dimension')
    output=Path(output_dir).resolve();output.mkdir(parents=True,exist_ok=False)
    root=output/'storage';processes=[];started=time.monotonic()
    report=dict(passed=False,rows=rows,dimension=dimension,require_gpu=require_gpu,cases=[],isolation=[],
                scope='cache/projected reads lifecycle; deterministic faults are separately verified by pytest')
    def check(name,condition):
        if not condition:raise AssertionError(name)
        report['cases'].append(dict(name=name,status='PASS'))
    def record(name,result):
        with (output/'cases.jsonl').open('a') as stream:stream.write(json.dumps(dict(name=name,result=result))+'\n')
    try:
        import faiss
        if require_gpu and faiss.get_num_gpus()<1:raise RuntimeError('Cache GPU acceptance requires a real GPU')
        original=generate_dataset(rows,dimension,seed=20261019)
        data=Dataset(original.ids+2**53+1,original.categories,original.timestamps,original.values,original.vectors)
        save_dataset(data,root/'imports/source.npz')
        query=np.zeros(dimension,dtype='<f4');vector=json.dumps(query.tolist())
        sql=f'SELECT id,vector_distance(vector,{vector}) AS distance FROM items WHERE value>=0 ORDER BY distance LIMIT 10'
        nlist=BuildConfig().resolved(rows).nlist
        options=SearchOptions(mode='approx',path='search-first',nprobe=nlist,fallback='error',data_cache='on',metadata_projection='projected',candidate_data_path='separate')
        source_root=Path(__file__).resolve().parents[3]
        (output/'source-sha256.json').write_text(json.dumps({str(p.relative_to(source_root)):hashlib.sha256(p.read_bytes()).hexdigest() for p in sorted((source_root/'src').rglob('*.py'))},indent=2)+'\n')
        (output/'config.json').write_text(json.dumps(dict(rows=rows,dimension=dimension,seed=20261019,nlist=nlist,data_cache_mib=64),indent=2)+'\n')
        (output/'environment.json').write_text(json.dumps(dict(python=sys.version,platform=platform.platform(),numpy=np.__version__,faiss=faiss.__version__),indent=2)+'\n')
        with ExitStack() as cleanup:
            def storage_start(name,address='127.0.0.1:0'):
                process=ServiceProcess(['storage','--data-root',str(root),'--listen',address],output/(name+'.log'))
                cleanup.callback(process.stop);processes.append(process.process)
                return process
            storage=storage_start('storage')
            def compute_start(name,preload=None):
                args=['compute','--storage',storage.address,'--listen','127.0.0.1:0','--timeout','180',
                    '--index-source','published','--isolate-storage-dir',str(root),'--data-cache-mib','64',
                    '--metadata-projection','projected','--fused-candidates']
                if preload is not None:args+=['--index-preload',str(preload)]
                process=ServiceProcess(args,output/(name+'.log'));cleanup.callback(process.stop);processes.append(process.process)
                isolation=next(e for e in process.events() if e.get('event')=='filesystem_isolation')
                report['isolation'].append(isolation)
                check(name+'_storage_denied',isolation['directory_denied'] and isolation['file_denied'])
                return process
            def client_for(process,create=False):
                client=cleanup.enter_context(SessionClient(process.address,timeout=180))
                client.open_database('demo',create=create);client.search_options=options
                return client
            bootstrap=compute_start('bootstrap');initial=client_for(bootstrap,True)
            initial.import_dataset('source.npz','items');publication=initial.publish_index('items',nlist=nlist,operation_id='initial')
            bootstrap.stop()
            a,b=compute_start('compute-a'),compute_start('compute-b');ca,cb=client_for(a),client_for(b)
            for client in (ca,cb):
                loaded=client.load_index('items')
                check('load_does_not_warm_data_'+('A' if client is ca else 'B'),client.data_cache_stats()['database']['entry_count']==0 and not loaded['statistics']['trained'])
            truth=oracle(data,query,10)
            cold=ca.execute(sql);warm=ca.execute(sql);other=cb.execute(sql)
            for name,result in [('cold-A',cold),('warm-A',warm),('cold-B',other)]:record(name,result)
            check('two_node_warm_cold',cold['rows']==warm['rows']==other['rows']==truth and
                warm['execution_plan']['counts']['vectors_fetched']==0 and other['execution_plan']['data_cache']['hit_rows']==0)
            check('warm_saves_real_rpc',cold['execution_plan']['counts']['rpc_calls']==warm['execution_plan']['counts']['rpc_calls']+1)
            for cache in ('off','on'):
                for projection in ('full','projected'):
                    for path in ('separate','fused'):
                        ca.search_options=replace(options,data_cache=cache,metadata_projection=projection,candidate_data_path=path)
                        result=ca.execute(sql);record(f'{cache}-{projection}-{path}',result)
                        check(f'equivalent-{cache}-{projection}-{path}',result['rows']==truth)
            terminal=TerminalProcess(a.address,output/'session.txt',request_timeout=180,command_timeout=180)
            cleanup.callback(terminal.stop);processes.append(terminal.process)
            for command in ('.open demo','.mode json','.cache stats','.cache clear','.search mode approx',
                            '.search path search-first',f'.search nprobe {nlist}',".search data-cache on",'.search metadata-projection projected',sql+';','.cache stats'):
                terminal.command(command)
            terminal.quit();terminal.stop()
            transcript=(output/'session.txt').read_text()
            check('pty_cache_controls','Error:' not in transcript and '"data_cache_version": 1' in transcript)
            ca.search_options=options
            ca.execute(sql);old_epoch=ca.execute(sql)['execution_plan']['data_cache']['scope']['storage_epoch']
            address=storage.address;storage.stop()
            try:ca.execute(sql)
            except ValueError as error:check('storage_down_cannot_serve_offline','UNAVAILABLE' in str(error) or 'DEADLINE_EXCEEDED' in str(error))
            else:raise AssertionError('Cache served query without storage')
            storage=storage_start('storage-restored',address)
            # Administrative readiness retries are bounded; query RPCs do not retry.
            probe_attempts={}
            for name,client in (('A',ca),('B',cb)):
                reconnect=time.monotonic()+10;attempts=0
                while True:
                    attempts+=1
                    try:client.describe('items');break
                    except ValueError:
                        if time.monotonic()>=reconnect:raise
                        time.sleep(.1)
                probe_attempts[name]=attempts
            report['storage_reconnect_probe_attempts']=probe_attempts
            restored=ca.execute(sql);record('storage-restored-resident-cache',restored)
            check('storage_epoch_invalidates_resident',restored['rows']==truth and
                restored['execution_plan']['data_cache']['scope']['storage_epoch']!=old_epoch and
                restored['execution_plan']['data_cache']['hit_rows']==0)
            target=int(data.ids[-1]);cb.execute(f'UPDATE items SET vector={vector},category=\'updated\',value=0 WHERE id={target}')
            try:ca.execute(sql)
            except ValueError as error:check('new_revision_strict_failure','index_not_published' in str(error))
            else:raise AssertionError('New revision used old index')
            ca.search_options=replace(options,fallback='exact')
            gap=ca.execute(sql)
            check('new_revision_exact_misses',gap['rows'][0][0]==target and gap['execution_plan']['data_cache']['hit_rows']==0)
            second=ca.publish_index('items',nlist=nlist,operation_id='updated')
            for client in (ca,cb):client.load_index('items');client.search_options=options
            changed_vectors=data.vectors.copy();changed_vectors[-1]=0
            changed_values=data.values.copy();changed_values[-1]=0
            changed=Dataset(data.ids,data.categories,data.timestamps,changed_values,changed_vectors)
            changed_truth=oracle(changed,query,10)
            check('updated_nodes_agree',ca.execute(sql)['rows']==cb.execute(sql)['rows']==changed_truth)
            preload=output/'preload.json';preload.write_text(json.dumps(dict(format_version=1,targets=[dict(database='demo',table='items',device='cpu')])))
            old_pid=a.process.pid;a.stop();a=compute_start('compute-a-restored',preload);ca=client_for(a)
            ready=next(e for e in a.events() if e.get('event')=='ready');statistics=ready['preloaded_indexes'][0]['statistics']
            check('restart_empty_cache',a.process.pid!=old_pid and ca.data_cache_stats()['database']['entry_count']==0 and
                  not statistics['trained'] and statistics['snapshot_calls']==0 and statistics['artifact_download_bytes']>0)
            check('restart_answers',ca.execute(sql)['rows']==cb.execute(sql)['rows']==changed_truth)
            if require_gpu:
                ca.load_index('items',device='gpu')
                replica=next(i for i in ca.list_indexes('items') if i['device']=='gpu')
                check('gpu_storage_denied',replica['isolation']['artifact_files_denied']>=2)
                ca.clear_data_cache();ca.search_options=replace(options,device='gpu',candidate_data_path='fused')
                first=ca.execute(sql);second_gpu=ca.execute(sql);record('gpu-cold',first);record('gpu-warm',second_gpu)
                check('gpu_cached_vectors',first['rows']==second_gpu['rows']==changed_truth and
                      second_gpu['execution_plan']['actual_device']=='gpu' and second_gpu['execution_plan']['counts']['vectors_fetched']==0)
            report.update(passed=True,first_artifact=publication['descriptor']['artifact_id'],updated_artifact=second['descriptor']['artifact_id'])
    except Exception as error:
        report['error']=str(error);raise
    finally:
        report.update(elapsed_seconds=time.monotonic()-started,gpu_verified=require_gpu and report['passed'],
                      processes_running_after_cleanup=[p.pid for p in processes if p.poll() is None])
        (output/'report.json').write_text(json.dumps(report,ensure_ascii=False,indent=2)+'\n')
        (output/'isolation.json').write_text(json.dumps(report['isolation'],indent=2)+'\n')
    return report
