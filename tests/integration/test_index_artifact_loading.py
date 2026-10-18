import json

import grpc
import pytest

from vecstor.compute.service import create_compute_server
from vecstor.datasets import generate_dataset, save_dataset
from vecstor.rpc import sql_pb2 as pb, sql_pb2_grpc as rpc
from vecstor.rpc.sql_client import SessionClient
from vecstor.storage.sql_service import create_catalog_server


@pytest.fixture
def cluster(tmp_path):
    storage, sport, backend = create_catalog_server(tmp_path, '127.0.0.1:0'); storage.start()
    compute, port, engine = create_compute_server(f'127.0.0.1:{sport}', '127.0.0.1:0', timeout=60); compute.start()
    try:
        save_dataset(generate_dataset(2048, 4, seed=51), tmp_path/'imports/source.npz')
        with SessionClient(f'127.0.0.1:{port}', timeout=60) as client:
            client.open_database('demo', True); client.import_dataset('source.npz', 'items')
            with grpc.insecure_channel(f'127.0.0.1:{port}') as channel:
                yield rpc.SQLComputeStub(channel), client, engine.sql, backend
    finally:
        compute.stop(0).wait(); engine.close(); storage.stop(0).wait(); backend.close()


def publish(cluster, operation='publish'):
    return cluster[0].PublishIndex(pb.PublishIndexRequest(database_id=cluster[1].database_id, table='items', nlist=1,
        seed=20261013, operation_id=operation), timeout=45)


def test_load_never_calls_build_worker(cluster, monkeypatch):
    publication = publish(cluster)
    from vecstor.compute import index_manager
    def forbidden(*args, **kwargs): pytest.fail('loading must not train')
    monkeypatch.setattr(index_manager, 'build_in_worker', forbidden)
    result = cluster[0].LoadIndex(pb.LoadIndexRequest(database_id=cluster[1].database_id, table='items', device='cpu'), timeout=20)
    assert result.descriptor.artifact_id == publication.publication.descriptor.artifact_id
    stats = json.loads(result.statistics_json)
    assert stats['trained'] is False and stats['artifact_download_bytes'] > 0
    assert stats['snapshot_calls'] == 0
    assert cluster[0].LoadIndex(pb.LoadIndexRequest(database_id=cluster[1].database_id, table='items', device='cpu'), timeout=10).reused


def test_wrong_expected_artifact_rejected(cluster):
    publish(cluster)
    with pytest.raises(grpc.RpcError) as error:
        cluster[0].LoadIndex(pb.LoadIndexRequest(database_id=cluster[1].database_id, table='items', device='cpu', expected_artifact_id='0'*64), timeout=10)
    assert error.value.code() == grpc.StatusCode.FAILED_PRECONDITION
    assert cluster[1].list_indexes() == []


def test_publish_success_survives_local_load_failure(cluster):
    publication = publish(cluster)
    cluster[2].index_manager.registry.max_bytes = 1
    with pytest.raises(grpc.RpcError):
        cluster[0].LoadIndex(pb.LoadIndexRequest(database_id=cluster[1].database_id, table='items', device='cpu'), timeout=10)
    assert publish(cluster).publication == publication.publication
    assert cluster[1].list_indexes() == []


def test_replay_after_data_write_does_not_retrain(cluster, monkeypatch):
    publication = publish(cluster)
    cluster[1].execute('UPDATE items SET value=999 WHERE id=1')
    from vecstor.compute import index_manager
    monkeypatch.setattr(index_manager, 'build_in_worker', lambda *a, **k:pytest.fail('idempotent replay must not train'))
    assert publish(cluster).publication == publication.publication
    with pytest.raises(grpc.RpcError) as error:
        cluster[0].PublishIndex(pb.PublishIndexRequest(database_id=cluster[1].database_id, table='items', nlist=2,
            seed=20261013, operation_id='publish'), timeout=10)
    assert error.value.code() == grpc.StatusCode.ALREADY_EXISTS


def test_partial_download_never_activates(cluster, monkeypatch):
    publish(cluster)
    service = cluster[3].index_service
    original = service.DownloadIndexArtifact
    def truncated(request, context):
        for frame in original(request, context):
            if frame.WhichOneof('content') == 'complete': return
            yield frame
    monkeypatch.setattr(service, 'DownloadIndexArtifact', truncated)
    with pytest.raises(grpc.RpcError) as error:
        cluster[0].LoadIndex(pb.LoadIndexRequest(database_id=cluster[1].database_id, table='items', device='cpu'), timeout=10)
    assert error.value.code() == grpc.StatusCode.DATA_LOSS
    assert cluster[1].list_indexes() == []
    monkeypatch.setattr(service, 'DownloadIndexArtifact', original)
    assert cluster[0].LoadIndex(pb.LoadIndexRequest(database_id=cluster[1].database_id, table='items', device='cpu'), timeout=10).descriptor.artifact_id


def test_maintenance_budget_released_after_cancel(cluster, monkeypatch):
    from contextlib import contextmanager
    from threading import Event
    publish(cluster)
    service = cluster[3].index_service
    manager = cluster[2].publication
    original_download, original_maintenance = service.DownloadIndexArtifact, manager.maintenance
    entered, release, finished = Event(), Event(), Event()
    def slow_download(request, context):
        for frame in original_download(request, context):
            if frame.WhichOneof('content') == 'chunk':
                entered.set()
                assert release.wait(5)
            yield frame
    @contextmanager
    def maintenance():
        try:
            with original_maintenance(): yield
        finally: finished.set()
    monkeypatch.setattr(service, 'DownloadIndexArtifact', slow_download)
    monkeypatch.setattr(manager, 'maintenance', maintenance)
    request = pb.LoadIndexRequest(database_id=cluster[1].database_id, table='items', device='cpu')
    call = cluster[0].LoadIndex.future(request, timeout=10)
    try:
        assert entered.wait(3)
        call.cancel()
        with pytest.raises(grpc.FutureCancelledError): call.result()
        assert finished.wait(3)
    finally: release.set()
    assert cluster[1].list_indexes() == []
    monkeypatch.setattr(service, 'DownloadIndexArtifact', original_download)
    assert cluster[0].LoadIndex(request, timeout=10).descriptor.artifact_id
