"""Reproducible synthetic inputs and non-pickle NPZ snapshots."""

import json
from pathlib import Path
from zipfile import BadZipFile

import numpy as np

from vecstor.data import DataError, Dataset


def generate_dataset(rows=10000, dimension=128, seed=20261011, distribution="independent"):
    for name, value, minimum in [("rows", rows, 0), ("dimension", dimension, 1), ("seed", seed, 0)]:
        if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
            raise DataError(f"{name} must be an integer >= {minimum}")
    if distribution not in {"independent", "correlated"}:
        raise DataError("distribution must be independent or correlated")
    rng = np.random.default_rng(seed)
    vectors = rng.random((rows, dimension), dtype=np.float32)
    if distribution == "independent":
        ranks = rng.permutation(rows)
    else:
        ranks = np.empty(rows, dtype=np.int64)
        ranks[np.argsort(vectors[:, 0], kind="stable")] = np.arange(rows)
    categories = np.array([f"category_{min(4, int(rank) * 5 // max(1, rows))}" for rank in ranks])
    return Dataset(np.arange(rows, dtype=np.int64), categories,
                   1704067200000 + ranks * 1000, ranks.astype(np.float64), vectors)


def save_dataset(dataset, path, metadata=None):
    path = Path(path)
    manifest = {"format_version": 1, "rows": dataset.size, "dimension": dataset.dimension,
                "metric": "squared_l2", "generation": metadata or {}}
    encoded = json.dumps(manifest, ensure_ascii=False, allow_nan=False)
    path.parent.mkdir(parents=True, exist_ok=True)
    # Exclusive creation protects existing data, including on concurrent writers.
    with path.open("xb") as target:
        np.savez_compressed(target, manifest=np.array(encoded), ids=dataset.ids,
                            categories=dataset.categories, timestamps=dataset.timestamps,
                            values=dataset.values, vectors=dataset.vectors)


def load_dataset(path):
    try:
        with np.load(path, allow_pickle=False) as source:
            expected = {"manifest", "ids", "categories", "timestamps", "values", "vectors"}
            if set(source.files) != expected:
                raise DataError("Snapshot fields are missing or unexpected")
            manifest = json.loads(source["manifest"].item())
            if not isinstance(manifest, dict):
                raise DataError("Invalid manifest")
            dataset = Dataset(**{name: source[name] for name in expected - {"manifest"}})
            required = {"format_version": 1, "rows": dataset.size,
                        "dimension": dataset.dimension, "metric": "squared_l2"}
            for key, value in required.items():
                if type(manifest.get(key)) is not type(value) or manifest[key] != value:
                    raise DataError(f"Snapshot manifest mismatch: {key}")
            return dataset
    except (ValueError, TypeError, KeyError, AttributeError, BadZipFile, EOFError) as error:
        raise DataError(f"Invalid dataset snapshot: {error}") from error
