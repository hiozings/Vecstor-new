import json
from pathlib import Path
import subprocess
import sys

import numpy as np
import pytest


SCRIPT = Path(__file__).resolve().parents[2] / "scripts" / "vecstor.py"


def run(*args):
    return subprocess.run([sys.executable, str(SCRIPT), *map(str, args)], text=True,
                          capture_output=True, timeout=45)


def test_generate_then_query_and_no_overwrite(tmp_path):
    dataset = tmp_path / "data.npz"
    created = run("generate", "--output", dataset, "--rows", 100, "--dimension", 4, "--seed", 23)
    assert created.returncode == 0, created.stderr
    assert json.loads(created.stdout)["rows"] == 100
    original = dataset.read_bytes()
    duplicate = run("generate", "--output", dataset)
    assert duplicate.returncode == 2 and dataset.read_bytes() == original
    response = run("query", "--dataset", dataset,
                   "--sql", "SELECT id FROM items WHERE value < 10 ORDER BY vector_distance(vector, :query_vector) LIMIT 3",
                   "--vector", "[0,0,0,0]")
    assert response.returncode == 0, response.stderr
    result = json.loads(response.stdout)
    with np.load(dataset, allow_pickle=False) as stored:
        mask = stored["values"] < 10
        ids = stored["ids"][mask]
        distances = (stored["vectors"][mask].astype(np.float64) ** 2).sum(axis=1)
        order = np.lexsort((ids, distances))[:3]
        assert result["rows"] == [{"id": int(i)} for i in ids[order]]
        np.testing.assert_allclose(result["distances"], distances[order])
    assert result["matched_count"] == 10


@pytest.mark.parametrize("distribution", ["independent", "correlated"])
def test_demo_writes_independent_ground_truth_and_report(tmp_path, distribution):
    destination = tmp_path / distribution
    result = run("demo", "--output-dir", destination, "--rows", 100, "--dimension", 4,
                 "--distribution", distribution)
    assert result.returncode == 0, result.stderr
    report = json.loads((destination / "report.json").read_text())
    assert report["passed"] is True and report["case_count"] == 12
    assert report["distribution"] == distribution
    expected = json.loads((destination / "ground_truth.json").read_text())
    actual = json.loads((destination / "actual.json").read_text())
    queries = json.loads((destination / "queries.json").read_text())
    assert len(expected) == len(actual) == len(queries) == 12
    assert [c["matched_count"] for c in expected[:10:2]] == [0, 1, 10, 50, 100]
    assert all(e["ids"] == [row["id"] for row in a["rows"]] for e, a in zip(expected, actual))
    rerun = run("demo", "--output-dir", destination, "--rows", 100, "--dimension", 4)
    assert rerun.returncode == 2


@pytest.mark.parametrize("args", [
    ["generate", "--rows", "-1"],
    ["query", "--sql", "DELETE FROM items", "--vector", "[0,0]"],
    ["query", "--sql", "SELECT id FROM items WHERE value > 1e1000000 ORDER BY vector_distance(vector, :query_vector) LIMIT 1", "--vector", "[0,0]"],
    ["query", "--sql", "SELECT id FROM items ORDER BY vector_distance(vector, :query_vector) LIMIT 1", "--vector", "[NaN,0]"],
    ["query", "--sql", "SELECT id FROM items ORDER BY vector_distance(vector, :query_vector) LIMIT 1", "--vector", "bad json"],
])
def test_cli_invalid_input_has_nonzero_exit_and_clear_error(tmp_path, args):
    path = tmp_path / "data.npz"
    assert run("generate", "--output", path, "--rows", 10, "--dimension", 2).returncode == 0
    arguments = args + (["--output", tmp_path / "bad.npz"] if args[0] == "generate" else ["--dataset", path])
    result = run(*arguments)
    assert result.returncode == 2
    assert "error:" in result.stderr.lower() and "Traceback" not in result.stderr


def test_oracle_detects_an_incorrect_engine_result(monkeypatch):
    from vecstor.datasets import generate_dataset
    from vecstor.evaluation import verify_baseline
    from vecstor.query.engine import LocalEngine, QueryResult

    # Deliberate mutation: an engine returning empty results must fail validation.
    monkeypatch.setattr(LocalEngine, "execute", lambda *args: QueryResult([], [], 0, 2))
    with pytest.raises(AssertionError):
        verify_baseline(generate_dataset(100, 2), seed=5)
