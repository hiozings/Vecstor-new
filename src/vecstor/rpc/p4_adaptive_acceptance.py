"""Real-process adaptive lifecycle checks; optional certified calibration clone."""
from contextlib import ExitStack
from dataclasses import replace
import hashlib
import json
from pathlib import Path
import shutil
import time

import grpc
import numpy as np

from vecstor.data import Dataset
from vecstor.datasets import generate_dataset,save_dataset,load_dataset
from vecstor.query.search_options import SearchOptions
from vecstor.rpc import sql_pb2 as pb,sql_pb2_grpc as rpc
from vecstor.rpc.acceptance import ServiceProcess
from vecstor.rpc.p4_index_acceptance import oracle
from vecstor.rpc.shell_acceptance import TerminalProcess
from vecstor.rpc.sql_client import SessionClient


def run(output_dir,rows=10000,dimension=128,calibration_dir=None):
    if rows<1024:raise ValueError('At least 1024 rows are required')
    output=Path(output_dir).resolve();output.mkdir(parents=True,exist_ok=False)
    root=output/'storage';policy=output/'policy.json';quality=None;database='bench'
    if calibration_dir is not None:
        calibration=Path(calibration_dir).resolve()
        for name,digest in json.loads((calibration/'bundle.json').read_text()).items():
            path=(calibration/name).resolve()
            if not path.is_relative_to(calibration) or hashlib.sha256(path.read_bytes()).hexdigest()!=digest:
                raise ValueError('Calibration bundle changed')
        groups=list((calibration/'groups').glob(f'{rows}-independent-*'))
        if len(groups)!=1:raise ValueError('Calibration must identify one independent dataset')
        source=groups[0];data=load_dataset(source/'dataset.npz');dimension=data.dimension
        shutil.copytree(source/'storage',root)
        shutil.copyfile(calibration/'policy.json',policy)
        quality=output/'quality-profiles.json';shutil.copyfile(source/'quality-profiles.json',quality)
    else:
        original=generate_dataset(rows,dimension,20261016)
        data=Dataset(original.ids+2**53+1,original.categories,original.timestamps,original.values,original.vectors)
        save_dataset(data,root/'imports/source.npz')
        policy.write_text(json.dumps(dict(format_version=1,policy_id='lifecycle_exact_only',workloads=[])))
    report=dict(passed=False,scope='certified_artifact_lifecycle' if calibration_dir else 'exact_only_lifecycle',
                checks=[],queries=[],isolation=[]);processes=[];began=time.monotonic()
    query=np.zeros(dimension,dtype='<f4');vector=json.dumps(query.tolist())
    sql=f'SELECT id,vector_distance(vector,{vector}) AS distance FROM "items" WHERE value < {rows} ORDER BY distance LIMIT 10'
    options=SearchOptions(mode='approx',path='auto',data_cache='on',metadata_projection='projected',candidate_data_path='separate')
    def check(name,value):
        report['checks'].append(dict(name=name,passed=bool(value)))
        if not value:raise AssertionError(name)
    def record(name,result):report['queries'].append(dict(name=name,rows=result['rows'],plan=result['execution_plan']))
    try:
        with ExitStack() as cleanup:
            def storage_start(name,address='127.0.0.1:0'):
                process=ServiceProcess(['storage','--data-root',str(root),'--listen',address],output/(name+'.log'))
                cleanup.callback(process.stop);processes.append(process.process);return process
            storage=storage_start('storage')
            def compute_start(name,mode='apply',address='127.0.0.1:0',preload=None):
                args=['compute','--storage',storage.address,'--listen',address,'--timeout','180',
                    '--index-source','published','--isolate-storage-dir',str(root),'--data-cache-mib','64',
                    '--metadata-projection','projected','--adaptive-policy',str(policy),
                    '--adaptive-network-class','loopback','--adaptive-mode',mode]
                if quality:args+=['--quality-profiles',str(quality)]
                if preload:args+=['--index-preload',str(preload)]
                process=ServiceProcess(args,output/(name+'.log'));cleanup.callback(process.stop);processes.append(process.process)
                isolated=next(e for e in process.events() if e.get('event')=='filesystem_isolation')
                report['isolation'].append(isolated)
                check(name+'_isolated',isolated['directory_denied'] and isolated['file_denied'])
                return process
            def connect(process,create=False):
                client=cleanup.enter_context(SessionClient(process.address,timeout=180))
                client.open_database(database,create=create);client.search_options=options
                return client
            bootstrap=compute_start('bootstrap');initial=connect(bootstrap,True)
            if calibration_dir is None:
                initial.import_dataset('source.npz','items');initial.publish_index('items',operation_id='initial')
            descriptor=initial.inspect_published_index('items')['descriptor'];nlist=descriptor['nlist']
            bootstrap.stop()
            a=compute_start('compute-a');b=compute_start('compute-b',mode='shadow')
            ca,cb=connect(a),connect(b)
            for name,client in (('A',ca),('B',cb)):
                loaded=client.load_index('items')
                check('load_no_training_'+name,not loaded['statistics']['trained'])
            truth=oracle(data,query,10,rows)
            cold=ca.execute(sql);warm=ca.execute(sql);other=cb.execute(sql)
            for name,result in (('cold-A',cold),('warm-A',warm),('shadow-B',other)):record(name,result)
            check('two_nodes_same_revision',cold['rows']==warm['rows']==other['rows']==truth and cold['revision']==other['revision'])
            check('shadow_executes_reference',other['execution_plan']['adaptive']['executed_plan']==other['execution_plan']['adaptive']['reference_plan'])
            report['initial_paths']=dict(A=warm['execution_plan']['actual_path'],B=other['execution_plan']['actual_path'])
            before_b=cb.data_cache_stats()['database']['entry_count'];ca.clear_data_cache()
            check('node_local_cache_clear',ca.data_cache_stats()['database']['entry_count']==0 and cb.data_cache_stats()['database']['entry_count']==before_b)
            terminal=TerminalProcess(a.address,output/'session.txt',request_timeout=180,command_timeout=180)
            cleanup.callback(terminal.stop);processes.append(terminal.process)
            for command in ('.open bench','.mode json','.plan on','.search mode approx','.search path auto',sql+';'):
                terminal.command(command,prompt='bench>')
            terminal.quit();terminal.stop()
            transcript=(output/'session.txt').read_text()
            check('pty_auto','Error:' not in transcript and '"requested_path": "auto"' in transcript)
            target=int(data.ids[-1]);target_sql=sql.replace(f'value < {rows}',f'id = {target}')
            manual=SearchOptions(mode='approx',path='filter-first',nprobe=nlist,fallback='error',data_cache='on',metadata_projection='projected')
            ca.search_options=manual;ca.execute(target_sql);ca.search_options=options
            old_target=ca.execute(target_sql)
            check('target_warmed_before_write',old_target['execution_plan']['data_cache']['hit_rows']==1)
            channel=cleanup.enter_context(grpc.insecure_channel(storage.address));stub=rpc.SQLStorageStub(channel)
            lease=stub.BeginRead(pb.BeginReadRequest(database_id=ca.database_id,table='items',ttl_seconds=60),timeout=10)
            old_vector=stub.FetchVectors(pb.FetchRequest(database_id=ca.database_id,token=lease.token,ids=[target]),timeout=10)
            cb.execute(f'UPDATE items SET vector={vector} WHERE id={target}')
            still_old=stub.FetchVectors(pb.FetchRequest(database_id=ca.database_id,token=lease.token,ids=[target]),timeout=10)
            check('old_lease_keeps_old_vector',still_old.revision==lease.revision and still_old.vectors_f32_le==old_vector.vectors_f32_le)
            stub.EndRead(pb.LeaseRequest(database_id=ca.database_id,token=lease.token),timeout=10)
            gap=ca.execute(target_sql);record('write-gap',gap)
            check('write_gap_exact',gap['rows']==[[target,0.]] and gap['revision']>lease.revision and
                gap['execution_plan']['actual_mode']=='exact' and gap['execution_plan']['data_cache']['hit_rows']==0)
            updated=ca.publish_index('items',nlist=nlist,operation_id='updated')
            for client in (ca,cb):client.load_index('items')
            old_policy=ca.execute(target_sql)
            check('old_policy_not_reused',old_policy['execution_plan']['actual_mode']=='exact')
            values=data.vectors.copy();values[-1]=0
            changed=Dataset(data.ids,data.categories,data.timestamps,data.values,values)
            new_truth=oracle(changed,query,10,rows)
            for name,client in (('A',ca),('B',cb)):
                client.search_options=replace(manual,path='search-first')
                fixed=client.execute(sql);record('rebuilt-fixed-'+name,fixed)
                check('rebuilt_answers_'+name,fixed['rows']==new_truth and fixed['execution_plan']['artifact_id']==updated['descriptor']['artifact_id'])
                client.search_options=options
            # Preserve the old endpoint-scoped capability cache through a same-address restart.
            ca.execute(target_sql)
            preload=output/'preload.json';preload.write_text(json.dumps(dict(format_version=1,targets=[dict(database=database,table='items',device='cpu')])))
            address=a.address;a.stop();a=compute_start('compute-a-restored',address=address,preload=preload)
            ready=next(e for e in a.events() if e.get('event')=='ready');statistics=ready['preloaded_indexes'][0]['statistics']
            check('compute_restart_no_training',not statistics['trained'] and statistics['snapshot_calls']==0)
            deadline=time.monotonic()+10
            while True:
                try:ca.describe('items');break
                except ValueError:
                    if time.monotonic()>=deadline:raise
                    time.sleep(.1)
            try:ca.execute(target_sql)
            except ValueError as error:check('restart_stale_confirmation_rejected','adaptive server instance changed' in str(error))
            else:raise AssertionError('Old compute capability was silently accepted')
            check('compute_restart_answers',ca.execute(target_sql)['rows']==[[target,0.]])
            ca.search_options=manual;ca.execute(target_sql);ca.search_options=options
            before=ca.execute(target_sql);epoch=before['execution_plan']['data_cache']['scope']['storage_epoch']
            address=storage.address;storage.stop();ca.timeout=3
            try:ca.execute(target_sql)
            except ValueError as error:check('storage_down_fails','UNAVAILABLE' in str(error) or 'DEADLINE_EXCEEDED' in str(error))
            else:raise AssertionError('Query succeeded without storage')
            ca.timeout=180;storage=storage_start('storage-restored',address)
            deadline=time.monotonic()+10
            while True:
                try:ca.describe('items');break
                except ValueError:
                    if time.monotonic()>=deadline:raise
                    time.sleep(.1)
            restored=ca.execute(target_sql);record('storage-restored',restored)
            check('storage_restart_epoch',restored['rows']==[[target,0.]] and
                restored['execution_plan']['data_cache']['scope']['storage_epoch']!=epoch and
                restored['execution_plan']['data_cache']['hit_rows']==0)
            report['passed']=True
    except Exception as error:report['error']=str(error);raise
    finally:
        report['elapsed_seconds']=time.monotonic()-began
        report['processes_running_after_cleanup']=[p.pid for p in processes if p.poll() is None]
        (output/'report.json').write_text(json.dumps(report,indent=2,allow_nan=False)+'\n')
    return report
