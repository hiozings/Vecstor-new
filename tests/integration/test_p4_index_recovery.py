from contextlib import contextmanager, ExitStack
from types import SimpleNamespace

import grpc
import pytest

from vecstor.compute.service import create_compute_server
from vecstor.datasets import generate_dataset, save_dataset
from vecstor.rpc import sql_pb2 as pb, sql_pb2_grpc as rpc
from vecstor.rpc.sql_client import SessionClient
from vecstor.storage.sql_service import create_catalog_server


@contextmanager
def running(root):
    with ExitStack() as stack:
        storage, sport, backend = create_catalog_server(root, '127.0.0.1:0')
        stack.callback(backend.close); stack.callback(lambda:storage.stop(0).wait()); storage.start()
        compute, port, engine = create_compute_server(f'127.0.0.1:{sport}','127.0.0.1:0',timeout=60,index_source='published')
        stack.callback(engine.close); stack.callback(lambda:compute.stop(0).wait()); compute.start()
        channel=stack.enter_context(grpc.insecure_channel(f'127.0.0.1:{port}'))
        schannel=stack.enter_context(grpc.insecure_channel(f'127.0.0.1:{sport}'))
        client=stack.enter_context(SessionClient(f'127.0.0.1:{port}',timeout=60)); client.open_database('demo',create=True)
        yield SimpleNamespace(client=client,stub=rpc.SQLComputeStub(channel),storage=rpc.SQLStorageStub(schannel),backend=backend,engine=engine.sql)


def seed(root, env):
    save_dataset(generate_dataset(2048,4,seed=18),root/'imports/source.npz')
    env.client.import_dataset('source.npz','items')
    return env.stub.PublishIndex(pb.PublishIndexRequest(database_id=env.client.database_id,table='items',nlist=1,operation_id='published'),timeout=45).publication


def test_storage_restart_recovers_committed_only(tmp_path, monkeypatch):
    with running(tmp_path) as env:
        publication=seed(tmp_path,env)
        database_id=env.client.database_id
    from vecstor.compute import index_manager
    monkeypatch.setattr(index_manager,'build_in_worker',lambda *a,**k:pytest.fail('recovery must not train'))
    with running(tmp_path) as env:
        assert env.client.database_id==database_id
        assert env.stub.GetIndexPublication(pb.IndexPublicationRequest(database_id=database_id,operation_id='published'),timeout=5)==publication
        assert env.client.list_indexes()==[]
        loaded=env.stub.LoadIndex(pb.LoadIndexRequest(database_id=database_id,table='items',device='cpu'),timeout=10)
        assert loaded.descriptor==publication.descriptor
        assert env.backend.recovery_report['corrupt']==[]


def test_old_token_invalid_after_restart(tmp_path):
    with running(tmp_path) as env:
        seed(tmp_path,env)
        database_id=env.client.database_id
        old=env.storage.BeginRead(pb.BeginReadRequest(database_id=database_id,table='items'),timeout=5)
    with running(tmp_path) as env:
        with pytest.raises(grpc.RpcError) as error:
            env.storage.FetchVectors(pb.FetchRequest(database_id=database_id,token=old.token,ids=[1]),timeout=5)
        assert error.value.code()==grpc.StatusCode.FAILED_PRECONDITION


def test_recovery_marks_missing_blob_corrupt(tmp_path):
    with running(tmp_path) as env: publication=seed(tmp_path,env)
    next((tmp_path/'indexes/objects').glob('*/*/index.faiss')).unlink()
    with running(tmp_path) as env:
        inspection=env.stub.InspectPublishedIndex(pb.InspectPublishedIndexRequest(database_id=env.client.database_id,table='items'),timeout=5)
        assert inspection.state=='corrupt'
        assert inspection.descriptor==publication.descriptor
        assert env.client.execute('SELECT COUNT(*) FROM items')['rows']==[[2048]]
        with pytest.raises(grpc.RpcError) as error:
            env.stub.LoadIndex(pb.LoadIndexRequest(database_id=env.client.database_id,table='items',device='cpu'),timeout=5)
        assert error.value.code()==grpc.StatusCode.DATA_LOSS


def test_preload_has_no_training_or_snapshot_export(tmp_path,monkeypatch):
    import time
    from vecstor.compute.index_recovery import preload_indexes
    from vecstor.compute import index_manager
    with running(tmp_path) as env:publication=seed(tmp_path,env)
    with running(tmp_path) as env:
        def forbidden(*args,**kwargs):pytest.fail('preload must not train or export vectors')
        monkeypatch.setattr(index_manager,'build_in_worker',forbidden)
        monkeypatch.setattr(env.backend.catalog,'snapshot_page',forbidden)
        results=preload_indexes(env.engine.publication,[dict(database='demo',table='items',device='cpu')],time.monotonic()+60)
        assert results[0]['artifact_id']==publication.descriptor.artifact_id
        assert results[0]['statistics']['trained'] is False
        assert results[0]['statistics']['snapshot_calls']==0
        assert results[0]['statistics']['artifact_download_bytes']>0


def test_preload_failure_prevents_ready(tmp_path):
    import json
    from vecstor.rpc.acceptance import ServiceProcess
    root=tmp_path/'storage'
    with running(root) as env:seed(root,env)
    config=tmp_path/'preload.json'
    config.write_text(json.dumps(dict(format_version=1,targets=[dict(database='demo',table='missing',device='cpu')])))
    storage=ServiceProcess(['storage','--data-root',str(root),'--listen','127.0.0.1:0'],tmp_path/'storage.log')
    try:
        with pytest.raises(RuntimeError,match='registered vector table'):
            ServiceProcess(['compute','--storage',storage.address,'--listen','127.0.0.1:0','--index-source','published',
                            '--index-preload',str(config)],tmp_path/'compute.log')
        assert '"event": "ready"' not in (tmp_path/'compute.log').read_text()
    finally:storage.stop()


def test_real_terminal_publishes_loads_and_inspects(tmp_path):
    from vecstor.rpc.acceptance import ServiceProcess
    from vecstor.rpc.shell_acceptance import TerminalProcess
    root=tmp_path/'storage'
    save_dataset(generate_dataset(2048,4,seed=11),root/'imports/source.npz')
    with ExitStack() as cleanup:
        storage=ServiceProcess(['storage','--data-root',str(root),'--listen','127.0.0.1:0'],tmp_path/'storage.log');cleanup.callback(storage.stop)
        compute=ServiceProcess(['compute','--storage',storage.address,'--listen','127.0.0.1:0','--index-source','published'],tmp_path/'compute.log');cleanup.callback(compute.stop)
        terminal=TerminalProcess(compute.address,tmp_path/'session.txt');cleanup.callback(terminal.stop)
        for command in ('.open demo --create','.mode json','.import source.npz items',
            '.index publish items --nlist 1 --operation-id terminal-publish','.index publication-status terminal-publish',
            '.index published items','.index load items','.index inspect items','.index prune --keep-last 1'):
            terminal.command(command)
        terminal.quit();terminal.stop()
    transcript=(tmp_path/'session.txt').read_text()
    assert 'Error:' not in transcript and '"cpu_ready": true' in transcript
