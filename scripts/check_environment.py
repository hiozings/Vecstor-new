"""Exercise installed dependencies; this is not the VecStor engine test suite."""

import argparse
from concurrent.futures import ThreadPoolExecutor
import importlib
from importlib.metadata import version
import json
from pathlib import Path
import platform
import sys
import tempfile

import duckdb
import faiss
import grpc
from grpc_tools import protoc
import numpy as np
import sqlglot
from sqlglot import exp


def check_sql():
    sql = "SELECT id FROM items WHERE category = 'paper' AND value >= 10 ORDER BY id"
    tree = sqlglot.parse_one(sql, read="duckdb")
    assert isinstance(tree, exp.Select)
    assert tree.find(exp.And) is not None
    with duckdb.connect(":memory:") as db:
        db.execute("CREATE TABLE items(id INTEGER, category VARCHAR, value DOUBLE)")
        db.executemany("INSERT INTO items VALUES (?, ?, ?)", [(1, "paper", 12), (2, "paper", 5), (3, "book", 20)])
        assert db.execute(tree.sql(dialect="duckdb")).fetchall() == [(1,)]
        assert db.execute("SELECT id FROM items WHERE category = ?", ["book"]).fetchall() == [(3,)]
    vector_tree = sqlglot.parse_one(
        "SELECT id FROM items ORDER BY vector_distance(vector, :query_vector) LIMIT 10",
        read="duckdb",
    )
    assert vector_tree.find(exp.Placeholder) is not None
    return "PASS: SQL execution, parameter binding, SQL/vector-expression parsing"


def check_faiss(directory, require_gpu):
    faiss.omp_set_num_threads(2)
    rng = np.random.default_rng(20261010)
    data = rng.random((4096, 32), dtype=np.float32)
    queries = rng.random((16, 32), dtype=np.float32)
    distances = ((queries[:, None, :] - data[None, :, :]) ** 2).sum(axis=2)
    expected_ids = np.argsort(distances, axis=1)[:, :10]
    expected_distances = np.take_along_axis(distances, expected_ids, axis=1)

    def verify(index):
        found_distances, found_ids = index.search(queries, 10)
        np.testing.assert_array_equal(found_ids, expected_ids)
        np.testing.assert_allclose(found_distances, expected_distances, rtol=2e-5, atol=2e-5)

    flat = faiss.IndexFlatL2(32)
    flat.add(data)
    verify(flat)
    ivf = faiss.IndexIVFFlat(faiss.IndexFlatL2(32), 32, 32)
    ivf.train(data)
    ivf.add(data)
    ivf.nprobe = 32  # Scan every list to compare against exact ground truth.
    verify(ivf)
    path = str(directory / "cpu.index")
    faiss.write_index(ivf, path)
    verify(faiss.read_index(path))
    result = {"cpu": "PASS: Flat/IVFFlat, NumPy ground truth, index save/load"}
    if require_gpu:
        assert hasattr(faiss, "StandardGpuResources"), "Faiss has no GPU bindings"
        assert faiss.get_num_gpus() > 0, "No accessible GPU; check driver and sandbox permissions"
        resources = faiss.StandardGpuResources()
        resources.setTempMemory(64 * 1024 * 1024)
        for name, index in [("Flat", flat), ("IVFFlat", ivf)]:
            gpu = faiss.index_cpu_to_gpu(resources, 0, index)
            verify(gpu)
            restored_cpu = faiss.index_gpu_to_cpu(gpu)
            path = str(directory / f"gpu-{name}.index")
            faiss.write_index(restored_cpu, path)
            verify(faiss.index_cpu_to_gpu(resources, 0, faiss.read_index(path)))
        result["gpu"] = "PASS: GPU Flat/IVFFlat, CPU agreement, save/load and GPU restore"
    else:
        result["gpu"] = "SKIPPED: --cpu-only explicitly requested"
    return result


def check_grpc(directory):
    proto = directory / "vecstor_env_probe.proto"
    proto.write_text('''syntax = "proto3";
package vecstor_env_probe;
message Payload { string text = 1; }
service Probe { rpc Echo(Payload) returns (Payload); }
''')
    assert protoc.main(["grpc_tools.protoc", f"-I{directory}", f"--python_out={directory}",
                        f"--grpc_python_out={directory}", str(proto)]) == 0
    sys.path.insert(0, str(directory))
    server = None
    try:
        messages = importlib.import_module("vecstor_env_probe_pb2")
        services = importlib.import_module("vecstor_env_probe_pb2_grpc")

        class Probe(services.ProbeServicer):
            def Echo(self, request, context):
                return messages.Payload(text=request.text)

        with ThreadPoolExecutor(max_workers=2) as workers:
            server = grpc.server(workers)
            services.add_ProbeServicer_to_server(Probe(), server)
            port = server.add_insecure_port("127.0.0.1:0")
            assert port > 0
            server.start()
            try:
                with grpc.insecure_channel(f"127.0.0.1:{port}") as channel:
                    grpc.channel_ready_future(channel).result(timeout=10)
                    reply = services.ProbeStub(channel).Echo(messages.Payload(text="VecStor 环境验证"), timeout=5)
                    assert reply.text == "VecStor 环境验证"
            finally:
                server.stop(0).wait()
    finally:
        sys.path.remove(str(directory))
    return "PASS: protoc generation, Protobuf serialization, localhost TCP gRPC round trip"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cpu-only", action="store_true", help="Explicitly skip GPU verification")
    args = parser.parse_args()
    report = {
        "python": platform.python_version(),
        "versions": {name: version(name) for name in ["duckdb", "sqlglot", "numpy", "grpcio", "grpcio-tools", "protobuf", "pytest"]},
        "faiss": faiss.__version__,
        "sql": check_sql(),
    }
    with tempfile.TemporaryDirectory(prefix="vecstor-env-") as temporary:
        directory = Path(temporary)
        report["faiss_checks"] = check_faiss(directory, not args.cpu_only)
        report["grpc"] = check_grpc(directory)
    print(json.dumps(report, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
