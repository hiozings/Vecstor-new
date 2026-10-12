from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager

import grpc
import numpy as np
import pytest

from vecstor.data import Dataset
from vecstor.datasets import generate_dataset, load_dataset, save_dataset
from vecstor.query.engine import LocalEngine
from vecstor.rpc import services_pb2 as pb, services_pb2_grpc as rpc

SQL = "SELECT {} FROM items {} ORDER BY vector_distance(vector, :query_vector) LIMIT {}"


@contextmanager
def running_storage(path):
    from vecstor.storage.service import create_storage_server
    server, port, backend = create_storage_server(path, "127.0.0.1:0")
    server.start()
    try:
        yield f"127.0.0.1:{port}"
    finally:
        server.stop(0).wait()
        backend.close()


@pytest.mark.parametrize("distribution", ["independent", "correlated"])
def test_remote_matches_p1_with_many_batches(tmp_path, distribution):
    from vecstor.compute.engine import RemoteEngine
    dataset = generate_dataset(123, 9, distribution=distribution)
    path = tmp_path / "snapshot.npz"
    save_dataset(dataset, path)
    vector = np.arange(9, dtype=np.float32) / 9
    with running_storage(path) as address, RemoteEngine(address, batch_rows=7) as remote, LocalEngine(dataset) as local:
        for condition in ["", "WHERE value < 10", "WHERE value < 1", "WHERE id = 999",
                          "WHERE (category = 'category_0' OR category = 'category_2') AND value >= 10"]:
            statement = SQL.format("*", condition, 10)
            expected = local.execute(statement, vector)
            actual = remote.execute(statement, vector)
            assert actual["rows"] == expected.rows
            np.testing.assert_allclose(actual["distances"], expected.distances, rtol=0, atol=0)
            assert actual["matched_count"] == expected.matched_count
            assert actual["plan"]["path"] == "grpc_filter_then_exact"
            assert actual["plan"]["rpc_calls"] >= 2
            assert actual["plan"]["rpc_payload_bytes"] > 0 and len(actual["snapshot_version"]) == 64


def test_compute_service_projection_int64_and_ties(tmp_path):
    from vecstor.compute.service import create_compute_server
    from vecstor.rpc.client import query
    dataset = Dataset([2**60, 7, 2], ["x"] * 3, [2**60, -1, 3], [1, 2, 3], [[0, 0], [1, 0], [-1, 0]])
    path = tmp_path / "snapshot.npz"
    save_dataset(dataset, path)
    with running_storage(path) as address:
        server, port, backend = create_compute_server(address, "127.0.0.1:0", batch_rows=1)
        server.start()
        try:
            actual = query(f"127.0.0.1:{port}", SQL.format("id, timestamp", "", 2), [0, 0])
            assert actual["rows"] == [{"id": 2**60, "timestamp": 2**60}, {"id": 2, "timestamp": 3}]
            assert actual["distances"] == [0, 1]
        finally:
            server.stop(0).wait()
            backend.close()


def test_empty_dataset_over_rpc(tmp_path):
    from vecstor.compute.engine import RemoteEngine
    path = tmp_path / "empty.npz"
    save_dataset(Dataset([], [], [], [], np.empty((0, 2))), path)
    with running_storage(path) as address, RemoteEngine(address) as remote:
        assert remote.execute(SQL.format("id", "", 10), [0, 0])["rows"] == []


def test_remote_query_cli_and_compute_invalid_input(tmp_path):
    from pathlib import Path
    import json
    import subprocess
    import sys
    from vecstor.compute.service import create_compute_server
    path = tmp_path / "sample.npz"
    save_dataset(generate_dataset(20, 2), path)
    with running_storage(path) as address:
        server, port, backend = create_compute_server(address, "127.0.0.1:0")
        server.start()
        target = f"127.0.0.1:{port}"
        try:
            process = subprocess.run([sys.executable, str(Path(__file__).resolve().parents[2] / "scripts/vecstor.py"),
                "remote-query", "--compute", target, "--sql", SQL.format("id", "WHERE value < 2", 10),
                "--vector", "[0,0]"], capture_output=True, text=True, timeout=15)
            assert process.returncode == 0, process.stderr
            result = json.loads(process.stdout)
            assert result["matched_count"] == 2 and len(result["rows"]) == 2
            with grpc.insecure_channel(target) as channel:
                stub = rpc.ComputeStub(channel)
                for request in [pb.QueryRequest(sql="DELETE FROM items", vector=[0, 0]),
                                pb.QueryRequest(sql=SQL.format("id", "", 1), vector=[0]),
                                pb.QueryRequest(sql=SQL.format("id", "", 1), vector=[float("nan"), 0])]:
                    with pytest.raises(grpc.RpcError) as caught:
                        stub.Query(request, timeout=3)
                    assert caught.value.code() == grpc.StatusCode.INVALID_ARGUMENT
        finally:
            server.stop(0).wait(); backend.close()


def test_total_deadline_is_not_reset_for_each_rpc():
    import time
    from vecstor.compute.engine import RemoteEngine
    from vecstor.rpc.common import RemoteError
    class SlowStorage(rpc.StorageServicer):
        def GetManifest(self, request, context):
            time.sleep(0.08)
            return pb.Manifest(version="v", rows=1, dimension=2, metric="squared_l2")
        def ScanMetadata(self, request, context):
            time.sleep(0.08)
            yield pb.MetadataBatch(version="v", total_rows=1, rows=[pb.Row(id=1, category="x")])
        def FetchVectors(self, request, context):
            return pb.VectorBatch(version="v", dimension=2, ids=[1], vectors_f32_le=b"\0" * 8)
    server = grpc.server(ThreadPoolExecutor(max_workers=2))
    rpc.add_StorageServicer_to_server(SlowStorage(), server)
    port = server.add_insecure_port("127.0.0.1:0"); server.start()
    try:
        with RemoteEngine(f"127.0.0.1:{port}", timeout=0.12) as engine:
            with pytest.raises(RemoteError) as caught:
                engine.execute(SQL.format("id", "", 1), [0, 0])
            assert caught.value.code == grpc.StatusCode.DEADLINE_EXCEEDED
    finally:
        server.stop(0).wait()


def test_concurrent_compute_calls_keep_independent_state(tmp_path):
    from vecstor.compute.service import create_compute_server
    from vecstor.rpc.client import query
    path = tmp_path / "sample.npz"
    data = generate_dataset(200, 4)
    save_dataset(data, path)
    requests = [(SQL.format("id, value", f"WHERE value < {threshold}", 10), [0] * 4)
                for threshold in [1, 20, 100, 200]]
    with LocalEngine(data) as local:
        expected = [local.execute(sql, vector).rows for sql, vector in requests]
    with running_storage(path) as address:
        server, port, backend = create_compute_server(address, "127.0.0.1:0", batch_rows=7)
        server.start()
        try:
            with ThreadPoolExecutor(max_workers=4) as pool:
                futures = [pool.submit(query, f"127.0.0.1:{port}", sql, vector) for sql, vector in requests]
                assert [future.result()["rows"] for future in futures] == expected
        finally:
            server.stop(0).wait(); backend.close()


@pytest.fixture
def large_metadata_snapshot(tmp_path):
    path = tmp_path / "large-metadata.npz"
    save_dataset(Dataset(np.arange(300), ["x" * 100000] * 300, [0] * 300,
                         [0] * 300, np.zeros((300, 2))), path)
    return path


def test_four_backpressured_queries_do_not_starve_vector_fetch(large_metadata_snapshot):
    from vecstor.compute.engine import RemoteEngine
    statement = SQL.format("id", "", 1)
    with running_storage(large_metadata_snapshot) as address:
        def execute():
            with RemoteEngine(address, timeout=5, batch_rows=1) as engine:
                return engine.execute(statement, [0, 0])["rows"]
        with ThreadPoolExecutor(max_workers=4) as pool:
            futures = [pool.submit(execute) for _ in range(4)]
            assert [future.result() for future in futures] == [[{"id": 0}]] * 4


def test_scan_admission_preserves_unary_capacity(large_metadata_snapshot):
    with running_storage(large_metadata_snapshot) as address, grpc.insecure_channel(address) as channel:
        stub = rpc.StorageStub(channel)
        version = stub.GetManifest(pb.Empty(), timeout=3).version
        request = pb.ScanRequest(version=version, sql=SQL.format("id", "", 1), batch_rows=1)
        streams = []
        try:
            for _ in range(4):
                stream = stub.ScanMetadata(request, timeout=10)
                streams.append(stream)
                assert next(stream).rows
            extra = stub.ScanMetadata(request, timeout=2)
            streams.append(extra)
            with pytest.raises(grpc.RpcError) as caught:
                next(extra)
            assert caught.value.code() == grpc.StatusCode.RESOURCE_EXHAUSTED
            response = stub.FetchVectors(pb.FetchRequest(version=version, ids=[0]), timeout=2)
            assert response.ids == [0]
        finally:
            for stream in streams:
                stream.cancel()


def test_downstream_unavailable_is_explicit_and_bounded():
    from vecstor.compute.engine import RemoteEngine
    from vecstor.rpc.common import RemoteError
    # Port is selected by an actual bound server, then deliberately shut down.
    server = grpc.server(ThreadPoolExecutor(max_workers=1))
    port = server.add_insecure_port("127.0.0.1:0")
    server.start(); server.stop(0).wait()
    with RemoteEngine(f"127.0.0.1:{port}", timeout=0.2) as engine:
        with pytest.raises(RemoteError) as error:
            engine.execute(SQL.format("id", "", 10), [0, 0])
        assert error.value.code in (grpc.StatusCode.UNAVAILABLE, grpc.StatusCode.DEADLINE_EXCEEDED)


@pytest.mark.parametrize("fault", ["version", "short_vectors", "wrong_ids", "missing_batch", "wrong_offset"])
def test_corrupt_or_incomplete_storage_response_never_returns_partial_result(fault):
    from vecstor.compute.engine import RemoteEngine
    from vecstor.rpc.common import RemoteError

    class BrokenStorage(rpc.StorageServicer):
        def GetManifest(self, request, context):
            return pb.Manifest(version="v1", rows=2, dimension=2, metric="squared_l2")

        def ScanMetadata(self, request, context):
            yield pb.MetadataBatch(version="bad" if fault == "version" else "v1", total_rows=2,
                offset=1 if fault == "wrong_offset" else 0, rows=[pb.Row(id=1, category="x", value=1)])
            if fault != "missing_batch":
                yield pb.MetadataBatch(version="v1", total_rows=2, offset=1, rows=[pb.Row(id=2, category="x", value=2)])

        def FetchVectors(self, request, context):
            return pb.VectorBatch(version="v1", dimension=2,
                ids=[999] if fault == "wrong_ids" else request.ids,
                vectors_f32_le=b"" if fault == "short_vectors" else np.array([[1, 0]], dtype="<f4").tobytes())

    server = grpc.server(ThreadPoolExecutor(max_workers=2))
    rpc.add_StorageServicer_to_server(BrokenStorage(), server)
    port = server.add_insecure_port("127.0.0.1:0"); server.start()
    try:
        with RemoteEngine(f"127.0.0.1:{port}") as engine:
            with pytest.raises(RemoteError) as error:
                engine.execute(SQL.format("id", "", 10), [0, 0])
            assert error.value.code == grpc.StatusCode.DATA_LOSS
    finally:
        server.stop(0).wait()
