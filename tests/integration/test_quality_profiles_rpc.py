from contextlib import contextmanager
from copy import deepcopy
import json
from pathlib import Path

import numpy as np
import pytest

from vecstor.data import Dataset
from vecstor.datasets import save_dataset
from vecstor.index.types import IndexKey
from vecstor.query.search_options import SearchOptions
from vecstor.query.statements import classify_statement
from vecstor.rpc.sql_client import SessionClient

SQL='SELECT id, vector_distance(vector,[0,0]) AS distance FROM items WHERE value >= 100 ORDER BY distance LIMIT 10'


@contextmanager
def profiled(root):
    from vecstor.storage.sql_service import create_catalog_server
    from vecstor.compute.service import create_compute_server
    storage,port,backend=create_catalog_server(root,'127.0.0.1:0');storage.start()
    compute,cp,cbackend=create_compute_server(f'127.0.0.1:{port}','127.0.0.1:0');compute.start()
    client=SessionClient(f'127.0.0.1:{cp}')
    try:
        client.open_database('demo',create=True)
        n=1024
        data=Dataset(np.arange(n,dtype=np.int64),['x']*n,np.zeros(n,dtype=np.int64),np.arange(n,dtype=float),
                     np.column_stack((np.arange(n,dtype=np.float32),np.zeros(n,dtype=np.float32))))
        save_dataset(data,root/'imports/source.npz');client.import_dataset('source.npz','items')
        built=client.build_index('items',nlist=1)
        # Evidence format fixture only; these unit workloads are not a quality estimate.
        doc=json.loads((Path(__file__).resolve().parents[2]/'configs/quality/recall95-example.json').read_text())
        p=doc['profiles'][0];p['profile_id']='tested'
        p['index_key']={name:built[name] for name in IndexKey.__dataclass_fields__}
        p['workload'].update(metadata_sql=classify_statement(SQL).vector.metadata_sql,k=10,path='filter-first')
        p['budget']={'nprobe_floor':1,'initial_candidates':128,'max_candidates':1024,'max_rounds':6}
        client.close();compute.stop(0).wait();cbackend.close()
        compute,cp,cbackend=create_compute_server(f'127.0.0.1:{port}','127.0.0.1:0',quality_document=doc);compute.start()
        client=SessionClient(f'127.0.0.1:{cp}');client.open_database('demo');client.build_index('items',nlist=1)
        yield client,backend,cbackend,doc,f'127.0.0.1:{cp}'
    finally:
        client.close();compute.stop(0).wait();cbackend.close();storage.stop(0).wait();backend.close()


def test_profile_resolves_calibrated_options_and_explain_is_readonly(tmp_path):
    with profiled(tmp_path) as (client,_,_,_,_):
        client.set_search('mode','approx');client.set_search('profile','tested');client.set_search('fallback','error')
        result=client.execute(SQL)
        assert [r[0] for r in result['rows']]==list(range(100,110))
        trace=result['execution_plan']
        assert trace['actual_mode']=='approx'
        assert trace['exhaustive_buckets'] is True
        assert trace['quality_profile']['state']=='applied'
        assert trace['effective_options']['candidates']==128
        plan=client.execute('EXPLAIN '+SQL)['execution_plan']
        assert plan['counts']['scan_metadata_calls']==0 and plan['counts']['vectors_fetched']==0


def test_profile_scope_and_revision_fail_closed_or_exactly_fallback(tmp_path):
    with profiled(tmp_path) as (client,_,_,_,_):
        client.search_options=SearchOptions(mode='approx',quality_profile_id='tested')
        changed=client.execute(SQL.replace('>= 100','>= 101'))
        assert changed['execution_plan']['actual_mode']=='exact'
        assert changed['execution_plan']['fallback_reason']=='quality_profile_scope_mismatch'
        client.execute('UPDATE items SET category=\'new\' WHERE id=100')
        client.build_index('items',nlist=1)
        stale=client.execute(SQL)
        assert stale['execution_plan']['fallback_reason']=='quality_profile_version_mismatch'
        client.set_search('fallback','error')
        with pytest.raises(ValueError,match='FAILED_PRECONDITION.*quality_profile_version_mismatch'):client.execute(SQL)


def test_profile_conflict_and_other_session_defaults(tmp_path):
    with profiled(tmp_path) as (client,_,_,_,address):
        client.search_options=SearchOptions(mode='approx',quality_profile_id='tested',candidates=1)
        with pytest.raises(ValueError,match='INVALID_ARGUMENT.*conflict'):client.execute(SQL)
        with SessionClient(address) as other:
            other.open_database('demo')
            assert other.execute(SQL)['execution_plan']['actual_mode']=='exact'
        client.search_options=SearchOptions(mode='approx',quality_profile_id='tested')
        client.set_search('profile','off')
        assert client.search_options.quality_profile_id is None


def test_profile_old_read_survives_write_and_new_read_rejects_old_profile(tmp_path):
    from concurrent.futures import ThreadPoolExecutor
    from threading import Event
    with profiled(tmp_path) as (client,backend,_,_,address):
        entered,release=Event(),Event();original=backend.catalog.vectors;first=[True]
        def paused(*args,**kwargs):
            if first[0]:first[0]=False;entered.set();assert release.wait(10)
            return original(*args,**kwargs)
        backend.catalog.vectors=paused
        client.search_options=SearchOptions(mode='approx',quality_profile_id='tested',fallback='error')
        with ThreadPoolExecutor(1) as pool, SessionClient(address) as writer:
            writer.open_database('demo');query=pool.submit(client.execute,SQL)
            try:
                assert entered.wait(5)
                writer.execute('UPDATE items SET vector=[0,0] WHERE id=100')
                writer.build_index('items',nlist=1)
            finally:release.set()
            old=query.result(timeout=5)
        assert old['rows'][0]==[100,10000.0]
        assert old['execution_plan']['quality_profile']['state']=='applied'
        with pytest.raises(ValueError,match='quality_profile_version_mismatch'):client.execute(SQL)


def test_warm_client_rejects_old_service_that_ignores_profile():
    from concurrent.futures import ThreadPoolExecutor
    import grpc
    from vecstor.rpc import sql_pb2 as pb, sql_pb2_grpc as rpc
    from vecstor.rpc.sql_codec import encode_result
    class Legacy(rpc.SQLComputeServicer):
        legacy=False
        def GetSearchCapabilities(self,request,context):
            value={'ann':True}
            if not self.legacy:value['quality_profiles']=[{'profile_id':'tested','database_id':'db'}]
            return pb.ControlResponse(json_result=json.dumps(value))
        def ExecuteSQL(self,request,context):
            yield from encode_result({'columns':[{'name':'id','type':'BIGINT'}],'rows':[[1]],
                                      'execution_plan':{'actual_mode':'approx'}})
    backend=Legacy();server=grpc.server(ThreadPoolExecutor(1));rpc.add_SQLComputeServicer_to_server(backend,server)
    port=server.add_insecure_port('127.0.0.1:0');server.start()
    try:
        with SessionClient(f'127.0.0.1:{port}') as client:
            client.database_id='db';client.list_quality_profiles();backend.legacy=True
            client.search_options=SearchOptions(mode='approx',quality_profile_id='tested')
            with pytest.raises(ValueError,match='acknowledge'):client.execute(SQL)
    finally:server.stop(0).wait()


def test_real_terminal_profile_loading_and_landlock(tmp_path):
    from contextlib import ExitStack
    from vecstor.rpc.acceptance import ServiceProcess
    from vecstor.rpc.shell_acceptance import TerminalProcess
    root=tmp_path/'data'
    with profiled(root) as (_,_,_,doc,_):
        configuration=tmp_path/'quality.json';configuration.write_text(json.dumps(doc))
    with ExitStack() as cleanup:
        storage=ServiceProcess(['storage','--data-root',str(root),'--listen','127.0.0.1:0'],tmp_path/'storage.log');cleanup.callback(storage.stop)
        compute=ServiceProcess(['compute','--storage',storage.address,'--listen','127.0.0.1:0',
                                '--quality-profiles',str(configuration),'--isolate-storage-dir',str(root)],tmp_path/'compute.log');cleanup.callback(compute.stop)
        terminal=TerminalProcess(compute.address,tmp_path/'terminal.txt');cleanup.callback(terminal.stop)
        for command in ['.open demo','.mode json','.index build items --nlist 1','.search mode approx',
                        '.search profiles','.search profile tested','.search fallback error',SQL+';']:
            terminal.command(command)
        terminal.quit()
        event=next(e for e in compute.events() if e.get('event')=='filesystem_isolation')
        assert event['directory_denied'] and event['file_denied']
    text=(tmp_path/'terminal.txt').read_text()
    assert 'Error:' not in text and '"state": "applied"' in text


def test_quality_config_cannot_be_preloaded_from_storage_root(tmp_path):
    import subprocess,sys
    root=tmp_path/'data';root.mkdir();configuration=root/'quality.json';configuration.write_text('{}')
    script=Path(__file__).resolve().parents[2]/'scripts/serve.py'
    result=subprocess.run([sys.executable,str(script),'compute','--storage','127.0.0.1:1',
                           '--quality-profiles',str(configuration),'--isolate-storage-dir',str(root)],
                          capture_output=True,text=True,timeout=10)
    assert result.returncode==2 and 'outside storage data root' in result.stderr
