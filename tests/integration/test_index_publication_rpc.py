from contextlib import contextmanager
import json

import grpc
import numpy as np
import pytest

from vecstor.datasets import generate_dataset, save_dataset
from vecstor.index.artifact import encode_artifact
from vecstor.index.faiss_backend import FaissIndex
from vecstor.index.types import BuildConfig, IndexKey
from vecstor.rpc import sql_pb2 as pb, sql_pb2_grpc as rpc
from vecstor.rpc.index_codec import descriptor_to_proto, descriptor_from_proto, upload_frames, read_artifact_frames
from vecstor.storage.sql_service import create_catalog_server


@pytest.fixture
def service(tmp_path):
    server, port, backend = create_catalog_server(tmp_path, '127.0.0.1:0')
    server.start()
    try:
        dbid = backend.catalog.open_database('demo', True)['id']
        save_dataset(generate_dataset(2048, 4, seed=31), tmp_path/'imports/source.npz')
        backend.catalog.import_dataset(dbid, 'source.npz', 'items', 'import')
        with grpc.insecure_channel(f'127.0.0.1:{port}') as channel:
            yield rpc.SQLStorageStub(channel), backend, dbid
    finally:
        server.stop(0).wait(); backend.close()


def prepare(service, operation='publish'):
    stub, _, dbid = service
    lease = stub.BeginRead(pb.BeginReadRequest(database_id=dbid, table='items', ttl_seconds=60), timeout=10)
    config = BuildConfig(nlist=1)
    intent = dict(database_id=dbid, table='items', nlist=1, seed=config.seed, expected_generation=None, protocol_version=1)
    stub.BeginIndexPublication(pb.BeginIndexPublicationRequest(database_id=dbid, token=lease.token,
        operation_id=operation, config=pb.IndexBuildConfig(nlist=1, seed=config.seed, train_rows=config.train_rows),
        expected_generation=lease.publication_generation, intent_json=json.dumps(intent)), timeout=10)
    chunks = list(stub.StreamVectorSnapshot(pb.SnapshotRequest(database_id=dbid, token=lease.token), timeout=10))
    assert chunks[-1].snapshot_complete and chunks[-1].snapshot_sha256
    ids = np.array([i for c in chunks for i in c.ids], dtype=np.int64)
    vectors = np.frombuffer(b''.join(c.vectors_f32_le for c in chunks), dtype='<f4').reshape(-1, 4)
    key = IndexKey.from_lease(lease, config)
    descriptor, payload = encode_artifact(FaissIndex.build(ids, vectors, config), key, config, chunks[-1].snapshot_sha256)
    return lease, descriptor, payload


def upload(service, lease, descriptor, payload, operation='publish'):
    return service[0].UploadIndexArtifact(upload_frames(service[2], lease.token, operation, descriptor, payload), timeout=10)


def finalize(service, lease, operation='publish'):
    return service[0].FinalizeIndexPublication(pb.FinalizeIndexPublicationRequest(
        database_id=service[2], token=lease.token, operation_id=operation), timeout=10)


def test_upload_hidden_until_finalize_and_leased_download(service):
    stub, _, dbid = service
    lease, descriptor, payload = prepare(service)
    assert upload(service, lease, descriptor, payload).state == 'STAGED'
    before = stub.BeginRead(pb.BeginReadRequest(database_id=dbid, table='items'), timeout=3)
    assert before.published_index_state == 'missing'
    result = finalize(service, lease)
    assert result.state == 'COMMITTED' and result.generation == 1
    after = stub.BeginRead(pb.BeginReadRequest(database_id=dbid, table='items'), timeout=3)
    assert descriptor_from_proto(after.published_index) == descriptor
    stream = stub.DownloadIndexArtifact(pb.DownloadIndexArtifactRequest(database_id=dbid, token=after.token,
        artifact_id=descriptor.artifact_id), timeout=5)
    assert read_artifact_frames(stream, dbid, after.token, descriptor)[1] == payload
    with pytest.raises(grpc.RpcError):
        list(stub.DownloadIndexArtifact(pb.DownloadIndexArtifactRequest(database_id=dbid, token=before.token,
            artifact_id=descriptor.artifact_id), timeout=5))


def test_finalize_after_write_rejected(service):
    lease, descriptor, payload = prepare(service)
    upload(service, lease, descriptor, payload)
    service[1].catalog.execute(service[2], 'UPDATE items SET value=9 WHERE id=1', 'write')
    with pytest.raises(grpc.RpcError) as error: finalize(service, lease)
    assert error.value.code() == grpc.StatusCode.ABORTED


def test_valid_artifact_cannot_claim_another_snapshot_digest(service):
    from dataclasses import replace
    lease, descriptor, payload = prepare(service)
    wrong = replace(descriptor, snapshot_sha256='0'*64)
    wrong = replace(wrong, artifact_id=wrong.identity())
    upload(service, lease, wrong, payload)
    with pytest.raises(grpc.RpcError) as error: finalize(service, lease)
    assert error.value.code() == grpc.StatusCode.DATA_LOSS
    fresh = service[0].BeginRead(pb.BeginReadRequest(database_id=service[2], table='items'), timeout=3)
    assert fresh.published_index_state == 'missing'


def test_commit_response_lost_is_recoverable_and_idempotent(service, monkeypatch):
    lease, descriptor, payload = prepare(service)
    upload(service, lease, descriptor, payload)
    original = service[1].catalog.index_call
    lost = [False]
    def lose(dbid, method, *args, **kwargs):
        result = original(dbid, method, *args, **kwargs)
        if method == 'commit_index_publication' and not lost[0]:
            lost[0] = True
            raise TimeoutError('response lost after commit')
        return result
    monkeypatch.setattr(service[1].catalog, 'index_call', lose)
    with pytest.raises(grpc.RpcError): finalize(service, lease)
    status = service[0].GetIndexPublication(pb.IndexPublicationRequest(database_id=service[2], operation_id='publish'), timeout=3)
    assert status.state == 'COMMITTED' and status.generation == 1
    assert finalize(service, lease).generation == 1


def test_truncated_upload_fails_and_frees_reservation(service):
    lease, descriptor, payload = prepare(service)
    stream = list(upload_frames(service[2], lease.token, 'publish', descriptor, payload))[:-1]
    with pytest.raises(grpc.RpcError): service[0].UploadIndexArtifact(iter(stream), timeout=5)
    assert service[1].index_service.store.reserved_bytes == 0
    status = service[0].GetIndexPublication(pb.IndexPublicationRequest(database_id=service[2], operation_id='publish'), timeout=3)
    assert status.state == 'FAILED'


def test_ending_lease_reclaims_staged_upload_off_sql_queue(service, monkeypatch):
    from threading import Event, current_thread
    lease, descriptor, payload = prepare(service)
    upload(service, lease, descriptor, payload)
    store = service[1].index_service.store
    assert store.reserved_bytes > 0
    original = store.abort_upload
    cleaned = Event(); threads = []
    def cleanup(operation):
        threads.append(current_thread().name)
        original(operation)
        cleaned.set()
    monkeypatch.setattr(store, 'abort_upload', cleanup)
    service[0].EndRead(pb.LeaseRequest(database_id=service[2], token=lease.token), timeout=3)
    assert cleaned.wait(2), 'staged reservation must be reclaimed when its lease ends'
    assert store.reserved_bytes == 0
    assert all(not name.startswith('vecstor-demo') for name in threads)


def test_download_admission_preserves_vector_capacity(service, monkeypatch):
    from concurrent.futures import ThreadPoolExecutor
    from threading import Condition, Event
    lease, descriptor, payload = prepare(service)
    upload(service, lease, descriptor, payload); finalize(service, lease)
    stub, backend, dbid = service
    lease = stub.BeginRead(pb.BeginReadRequest(database_id=dbid, table='items'), timeout=3)
    request = pb.DownloadIndexArtifactRequest(database_id=dbid, token=lease.token, artifact_id=descriptor.artifact_id)
    original = backend.index_service.store.pin
    entered = []; condition = Condition(); release = Event()
    @contextmanager
    def paused(descriptor):
        with original(descriptor) as source:
            with condition: entered.append(1); condition.notify_all()
            assert release.wait(5)
            yield source
    monkeypatch.setattr(backend.index_service.store, 'pin', paused)
    with ThreadPoolExecutor(2) as pool:
        jobs = [pool.submit(lambda:list(stub.DownloadIndexArtifact(request, timeout=7))) for _ in range(2)]
        try:
            with condition: assert condition.wait_for(lambda:len(entered)==2, timeout=3)
            with pytest.raises(grpc.RpcError) as error: list(stub.DownloadIndexArtifact(request, timeout=1))
            assert error.value.code() == grpc.StatusCode.RESOURCE_EXHAUSTED
            assert list(stub.FetchVectors(pb.FetchRequest(database_id=dbid, token=lease.token, ids=[1]), timeout=2).ids) == [1]
        finally: release.set()
        for job in jobs: assert len(job.result()) >= 3
