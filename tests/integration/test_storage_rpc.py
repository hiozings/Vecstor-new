from contextlib import contextmanager
import hashlib

import grpc
import numpy as np
import pytest

from vecstor.data import Dataset
from vecstor.datasets import save_dataset
from vecstor.rpc import services_pb2 as pb, services_pb2_grpc as rpc

SQL = "SELECT id FROM items {} ORDER BY vector_distance(vector, :query_vector) LIMIT 10"


@contextmanager
def storage(path):
    from vecstor.storage.service import create_storage_server
    server, port, backend = create_storage_server(path, "127.0.0.1:0")
    server.start()
    try:
        with grpc.insecure_channel(f"127.0.0.1:{port}") as channel:
            grpc.channel_ready_future(channel).result(timeout=5)
            yield rpc.StorageStub(channel), backend
    finally:
        server.stop(0).wait()
        backend.close()


@pytest.fixture
def snapshot(tmp_path):
    path = tmp_path / "snapshot.npz"
    save_dataset(Dataset([90, 7, 2], ["book", "论文", "论文"], [-1, 2**60, 3],
                         [20, 5, 12], [[0, 0], [1, 0], [-1, 0]]), path)
    return path


def test_manifest_scan_and_ordered_vector_fetch(snapshot):
    with storage(snapshot) as (stub, _):
        manifest = stub.GetManifest(pb.Empty(), timeout=3)
        assert manifest.version == hashlib.sha256(snapshot.read_bytes()).hexdigest()
        assert (manifest.rows, manifest.dimension, manifest.metric) == (3, 2, "squared_l2")
        batches = list(stub.ScanMetadata(pb.ScanRequest(version=manifest.version,
                        sql=SQL.format("WHERE category = '论文'"), batch_rows=1), timeout=3))
        assert [b.offset for b in batches] == [0, 1]
        assert all(b.total_rows == 2 and b.version == manifest.version for b in batches)
        rows = [r for b in batches for r in b.rows]
        assert [r.id for r in rows] == [7, 2] and rows[0].timestamp == 2**60
        result = stub.FetchVectors(pb.FetchRequest(version=manifest.version, ids=[2, 90]), timeout=3)
        assert list(result.ids) == [2, 90] and result.dimension == 2
        np.testing.assert_array_equal(np.frombuffer(result.vectors_f32_le, dtype="<f4").reshape(2, 2), [[-1, 0], [0, 0]])


@pytest.mark.parametrize("method,payload,code", [
    ("ScanMetadata", pb.ScanRequest(version="wrong", sql=SQL.format("")), grpc.StatusCode.FAILED_PRECONDITION),
    ("FetchVectors", pb.FetchRequest(version="wrong", ids=[2]), grpc.StatusCode.FAILED_PRECONDITION),
    ("ScanMetadata", pb.ScanRequest(sql="DELETE FROM items"), grpc.StatusCode.INVALID_ARGUMENT),
    ("ScanMetadata", pb.ScanRequest(sql=SQL.format(""), batch_rows=1025), grpc.StatusCode.RESOURCE_EXHAUSTED),
    ("FetchVectors", pb.FetchRequest(ids=[123456]), grpc.StatusCode.NOT_FOUND),
    ("FetchVectors", pb.FetchRequest(ids=[2, 2]), grpc.StatusCode.INVALID_ARGUMENT),
    ("FetchVectors", pb.FetchRequest(ids=[-1]), grpc.StatusCode.INVALID_ARGUMENT),
])
def test_storage_rejects_invalid_requests(snapshot, method, payload, code):
    with storage(snapshot) as (stub, _):
        request = type(payload).FromString(payload.SerializeToString())
        if not request.version:
            request.version = stub.GetManifest(pb.Empty(), timeout=3).version
        with pytest.raises(grpc.RpcError) as caught:
            response = getattr(stub, method)(request, timeout=3)
            if method == "ScanMetadata":
                list(response)
        assert caught.value.code() == code


def test_empty_scan_and_snapshot_survives_file_removal(snapshot):
    with storage(snapshot) as (stub, _):
        manifest = stub.GetManifest(pb.Empty(), timeout=3)
        snapshot.unlink()
        batches = list(stub.ScanMetadata(pb.ScanRequest(version=manifest.version,
                        sql=SQL.format("WHERE id = 999")), timeout=3))
        assert len(batches) == 1 and batches[0].total_rows == 0 and not batches[0].rows
        assert stub.FetchVectors(pb.FetchRequest(version=manifest.version, ids=[90]), timeout=3).ids == [90]
