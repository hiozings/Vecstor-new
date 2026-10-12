"""Reproducible two-process, kernel-isolated P2 correctness acceptance."""

from contextlib import ExitStack
import json
import os
from pathlib import Path
import subprocess
import sys
import time

import grpc
import numpy as np

from vecstor.data import Dataset
from vecstor.datasets import generate_dataset, save_dataset
from vecstor.evaluation import verify_baseline
from vecstor.query.engine import LocalEngine
from vecstor.rpc.client import query
from vecstor.rpc.common import RemoteError


class ServiceProcess:
    def __init__(self, arguments, log_path):
        self.log_path = log_path
        self.log = log_path.open("x", encoding="utf-8")
        self.process = None
        script = Path(__file__).resolve().parents[3] / "scripts/serve.py"
        env = dict(os.environ, PYTHONDONTWRITEBYTECODE="1", OPENBLAS_NUM_THREADS="1", OMP_NUM_THREADS="1")
        try:
            self.process = subprocess.Popen([sys.executable, str(script), *arguments],
                stdout=self.log, stderr=subprocess.STDOUT, env=env, close_fds=True)
            deadline = time.monotonic() + 30
            while time.monotonic() < deadline:
                events = self.events()
                ready = next((event for event in events if event.get("event") == "ready"), None)
                if ready:
                    self.address = ready["address"]
                    return
                if self.process.poll() is not None:
                    break
                time.sleep(0.02)
            raise RuntimeError(f"Service failed to become ready: {log_path.read_text()}")
        except Exception:
            self.stop()
            raise

    def events(self):
        events = []
        for line in self.log_path.read_text().splitlines():
            try:
                event = json.loads(line)
                if isinstance(event, dict):
                    events.append(event)
            except ValueError:
                pass
        return events

    def stop(self):
        if self.process is not None and self.process.poll() is None:
            self.process.terminate()
            try:
                self.process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait(timeout=5)
        self.log.close()


def _write(path, obj):
    with path.open("x", encoding="utf-8") as target:
        json.dump(obj, target, indent=2, ensure_ascii=False, allow_nan=False)
        target.write("\n")


def run_acceptance(output_dir, rows=10000, dimension=128, distribution="independent", seed=20261011):
    output = Path(output_dir).resolve()
    if rows < 2:
        raise ValueError("P2 acceptance requires at least two rows for tie validation")
    original = generate_dataset(rows, dimension, seed, distribution)
    vectors = original.vectors.copy()
    vectors[1] = vectors[0]
    dataset = Dataset(original.ids * 7 + 11, original.categories, original.timestamps, original.values, vectors)
    output.mkdir(parents=True, exist_ok=False)
    storage_dir = output / "private-storage"
    snapshot = storage_dir / "snapshot.npz"
    save_dataset(dataset, snapshot, {"seed": seed, "distribution": distribution, "p2_ties_and_sparse_ids": True})
    cases, _, baseline = verify_baseline(dataset, seed)
    with LocalEngine(dataset) as local:
        for name, projection, condition, k in [
            ("empty", "id", "WHERE id = -1", 10), ("fewer_than_k", "id", "WHERE id = 11", 100),
            ("ties", "id", "", 2), ("projection", "*", "WHERE id = 11 OR id = 18", 10),
        ]:
            sql = f"SELECT {projection} FROM items {condition} ORDER BY vector_distance(vector, :query_vector) LIMIT {k}"
            vector = dataset.vectors[0].tolist()
            cases.append({"name": name, "sql": sql, "vector": vector})
            baseline.append({"name": name, **local.execute(sql, vector).to_dict()})
    _write(output / "queries.json", cases)
    _write(output / "p1-results.json", baseline)
    with ExitStack() as cleanup:
        storage = ServiceProcess(["storage", "--dataset", str(snapshot), "--listen", "127.0.0.1:0"], output / "storage.log")
        cleanup.callback(storage.stop)
        compute = ServiceProcess(["compute", "--storage", storage.address, "--listen", "127.0.0.1:0",
                                  "--batch-rows", "64", "--isolate-storage-dir", str(storage_dir)], output / "compute.log")
        cleanup.callback(compute.stop)
        isolation = next((event for event in compute.events() if event.get("event") == "filesystem_isolation"), None)
        if isolation is None or not isolation["directory_denied"] or not isolation["file_denied"]:
            raise AssertionError("Missing kernel isolation evidence")
        actual = []
        for case, expected in zip(cases, baseline):
            result = query(compute.address, case["sql"], case["vector"], timeout=15)
            if result["rows"] != expected["rows"] or result["matched_count"] != expected["matched_count"]:
                raise AssertionError(f"{case['name']}: P1/P2 rows or count mismatch")
            np.testing.assert_allclose(result["distances"], expected["distances"], rtol=0, atol=0)
            actual.append({"name": case["name"], **result})
        _write(output / "p2-results.json", actual)
        storage.stop()
        try:
            query(compute.address, cases[0]["sql"], cases[0]["vector"], timeout=2)
        except RemoteError as error:
            if error.code not in {grpc.StatusCode.UNAVAILABLE, grpc.StatusCode.DEADLINE_EXCEEDED}:
                raise
            stopped_status = error.code.name
        else:
            raise AssertionError("Compute returned success after storage stopped")
        report = {"passed": True, "rows": rows, "dimension": dimension, "seed": seed,
                  "distribution": distribution, "case_count": len(cases), "isolation": isolation,
                  "storage_pid": storage.process.pid, "compute_pid": compute.process.pid,
                  "storage_stopped_query_status": stopped_status,
                  "scope": "localhost_correctness_and_landlock_isolation_not_performance"}
    report["compute_running_after_cleanup"] = compute.process.poll() is None
    report["storage_running_after_cleanup"] = storage.process.poll() is None
    _write(output / "report.json", report)
    return report
