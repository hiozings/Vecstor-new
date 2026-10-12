"""Local generation, query and correctness-demo commands."""

import argparse
from importlib.metadata import version
import json
from pathlib import Path
import platform
import sys

from vecstor.datasets import generate_dataset, load_dataset, save_dataset
from vecstor.evaluation import verify_baseline
from vecstor.query.engine import LocalEngine


def _generation_arguments(parser):
    parser.add_argument("--rows", type=int, default=10000)
    parser.add_argument("--dimension", type=int, default=128)
    parser.add_argument("--seed", type=int, default=20261011)
    parser.add_argument("--distribution", choices=["independent", "correlated"], default="independent")


def _write_json(path, content):
    with path.open("x", encoding="utf-8") as target:
        json.dump(content, target, ensure_ascii=False, allow_nan=False, indent=2)
        target.write("\n")


def main(argv=None):
    parser = argparse.ArgumentParser(description="VecStor local reference and gRPC query client")
    commands = parser.add_subparsers(dest="command", required=True)
    generate = commands.add_parser("generate", help="Create a reproducible NPZ dataset")
    _generation_arguments(generate)
    generate.add_argument("--output", type=Path, required=True)
    query = commands.add_parser("query", help="Run a restricted hybrid SQL query locally")
    query.add_argument("--dataset", type=Path, required=True)
    query.add_argument("--sql", required=True)
    query.add_argument("--vector", required=True, help="JSON array, e.g. '[0, 0]'")
    remote = commands.add_parser("remote-query", help="Query an independent gRPC compute service")
    remote.add_argument("--compute", required=True, help="Compute host:port")
    remote.add_argument("--sql", required=True)
    remote.add_argument("--vector", required=True, help="JSON query vector")
    remote.add_argument("--timeout", type=float, default=15)
    demo = commands.add_parser("demo", help="Verify 12 queries against independent NumPy truth")
    _generation_arguments(demo)
    demo.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args(argv)
    try:
        if args.command == "remote-query":
            from vecstor.rpc.client import query as remote_query
            output = remote_query(args.compute, args.sql, json.loads(args.vector), args.timeout)
        elif args.command == "query":
            vector = json.loads(args.vector)
            with LocalEngine(load_dataset(args.dataset)) as engine:
                output = engine.execute(args.sql, vector).to_dict()
        else:
            destination = args.output if args.command == "generate" else args.output_dir
            if destination.exists():
                raise FileExistsError(f"Refusing to overwrite existing output: {destination}")
            data = generate_dataset(args.rows, args.dimension, args.seed, args.distribution)
            configuration = {"rows": args.rows, "dimension": args.dimension,
                             "seed": args.seed, "distribution": args.distribution}
            if args.command == "generate":
                save_dataset(data, args.output, configuration)
                output = {**configuration, "output": str(args.output)}
            else:
                queries, truths, actual = verify_baseline(data, args.seed)
                args.output_dir.mkdir(parents=True, exist_ok=False)
                save_dataset(data, args.output_dir / "dataset.npz", configuration)
                _write_json(args.output_dir / "queries.json", queries)
                _write_json(args.output_dir / "ground_truth.json", truths)
                _write_json(args.output_dir / "actual.json", actual)
                output = {**configuration, "passed": True, "case_count": len(queries),
                          "metric": "squared_l2", "mode": "exact", "purpose": "correctness_only",
                          "versions": {name: version(name) for name in ["numpy", "duckdb", "sqlglot"]},
                          "python": platform.python_version(), "output_dir": str(args.output_dir)}
                _write_json(args.output_dir / "report.json", output)
        print(json.dumps(output, ensure_ascii=False, allow_nan=False, indent=2))
        return 0
    except (ValueError, OSError, AssertionError) as error:
        print(f"error: {error}", file=sys.stderr)
        return 2
