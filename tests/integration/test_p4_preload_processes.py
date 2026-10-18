"""Startup barriers and simultaneous compute loss with surviving storage."""
from contextlib import ExitStack
import json

import pytest

from vecstor.datasets import generate_dataset, save_dataset
from vecstor.rpc.acceptance import ServiceProcess
from vecstor.rpc.sql_client import SessionClient
from vecstor.query.search_options import SearchOptions


def prepared(tmp_path, cleanup):
    root=tmp_path/'storage'
    save_dataset(generate_dataset(2048,128,seed=18),root/'imports/source.npz')
    storage=ServiceProcess(['storage','--data-root',str(root),'--listen','127.0.0.1:0'],tmp_path/'storage.log')
    cleanup.callback(storage.stop)
    def compute(name, *extra):
        process=ServiceProcess(['compute','--storage',storage.address,'--listen','127.0.0.1:0',
            '--index-source','published',*extra],tmp_path/(name+'.log'))
        cleanup.callback(process.stop)
        return process
    a=compute('a')
    client=cleanup.enter_context(SessionClient(a.address,timeout=60))
    client.open_database('demo',create=True);client.import_dataset('source.npz','items')
    publication=client.publish_index('items',nlist=1,operation_id='initial')
    config=tmp_path/'preload.json'
    config.write_text(json.dumps(dict(format_version=1,targets=[dict(database='demo',table='items',device='cpu')])))
    return root,storage,compute,a,client,publication,config


def test_both_compute_processes_restart_with_storage_alive(tmp_path):
    with ExitStack() as cleanup:
        root,storage,compute,a,ca,publication,config=prepared(tmp_path,cleanup)
        b=compute('b')
        cb=cleanup.enter_context(SessionClient(b.address,timeout=60));cb.open_database('demo')
        for client in (ca,cb):client.load_index('items')
        old_pids={a.process.pid,b.process.pid};storage_pid=storage.process.pid
        a.stop();b.stop()
        assert storage.process.poll() is None
        for name in ('a-restored','b-restored'):
            process=compute(name,'--index-preload',str(config))
            assert process.process.pid not in old_pids
            with SessionClient(process.address,timeout=60) as client:
                client.open_database('demo')
                inspection=client.inspect_published_index('items')
                assert inspection['cpu_ready']
                assert inspection['descriptor']['artifact_id']==publication['descriptor']['artifact_id']
                client.search_options=SearchOptions(mode='approx',path='search-first',nprobe=1,fallback='error')
                sql='SELECT id FROM items ORDER BY vector_distance(vector,'+json.dumps([0.]*128)+') LIMIT 10'
                result=client.execute(sql)
                assert result['execution_plan']['artifact_id']==publication['descriptor']['artifact_id']
        assert storage.process.pid==storage_pid and storage.process.poll() is None


@pytest.mark.parametrize('fault',['missing_file','corrupt_artifact','budget'])
def test_preload_fault_never_reports_ready(tmp_path,fault):
    with ExitStack() as cleanup:
        root,storage,compute,a,client,publication,config=prepared(tmp_path,cleanup)
        a.stop()
        extra=[]
        if fault=='missing_file':config.unlink()
        if fault=='corrupt_artifact':
            next((root/'indexes/objects').glob('*/*/index.faiss')).write_bytes(b'corrupt')
        if fault=='budget':extra=['--index-memory-mib','1']
        with pytest.raises(RuntimeError):compute('failed','--index-preload',str(config),*extra)
        assert '"event": "ready"' not in (tmp_path/'failed.log').read_text()
