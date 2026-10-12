"""Local exhaustive reference query path; no network or ANN acceleration."""

from dataclasses import dataclass

import duckdb
import numpy as np

from vecstor.data import Dataset, query_vector
from vecstor.query.parser import METADATA, QueryError, parse_query


@dataclass(frozen=True)
class QueryResult:
    rows: list[dict]
    distances: list[float]
    matched_count: int
    dimension: int

    def to_dict(self):
        return {"rows": self.rows, "distances": self.distances,
                "matched_count": self.matched_count, "metric": "squared_l2", "mode": "exact",
                "plan": {"path": "local_filter_then_exact", "backend": "numpy_float64",
                         "dimension": self.dimension, "evaluated_vectors": self.matched_count}}


class LocalEngine:
    def __init__(self, dataset: Dataset, block_size: int = 4096):
        if isinstance(block_size, bool) or not isinstance(block_size, int) or block_size <= 0:
            raise ValueError("block_size must be a positive integer")
        self.dataset = dataset
        self.block_size = block_size
        self._closed = False
        self._db = duckdb.connect(":memory:", config={"threads": 1})
        try:
            self._db.execute('CREATE TABLE items (_row BIGINT, id BIGINT, category VARCHAR, "timestamp" BIGINT, value DOUBLE)')
            if dataset.size:
                records = list(zip(range(dataset.size), dataset.ids.tolist(), dataset.categories.tolist(),
                                   dataset.timestamps.tolist(), dataset.values.tolist()))
                self._db.executemany("INSERT INTO items VALUES (?, ?, ?, ?, ?)", records)
        except Exception:
            self._db.close()
            raise

    def close(self):
        if not self._closed:
            self._db.close()
            self._closed = True

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()

    def execute(self, sql: str, vector) -> QueryResult:
        if self._closed:
            raise QueryError("LocalEngine is closed")
        plan = parse_query(sql)
        query = query_vector(vector, self.dataset.dimension).astype(np.float64)
        candidates = self._db.execute(plan.filter_sql, plan.filter_params).fetchall()
        count = len(candidates)
        if not count:
            return QueryResult([], [], 0, self.dataset.dimension)

        row_positions = np.fromiter((row[0] for row in candidates), dtype=np.int64, count=count)
        candidate_ids = np.fromiter((row[1] for row in candidates), dtype=np.int64, count=count)
        keep_positions = np.empty(0, dtype=np.int64)
        keep_distances = np.empty(0, dtype=np.float64)
        # Each block is fully evaluated. Retaining its best K is exact under
        # the total (distance, id) ordering, including ties at block boundaries.
        for start in range(0, count, self.block_size):
            end = min(start + self.block_size, count)
            delta = self.dataset.vectors[row_positions[start:end]].astype(np.float64) - query
            distances = np.sum(delta * delta, axis=1, dtype=np.float64)
            merged_positions = np.concatenate((keep_positions, np.arange(start, end, dtype=np.int64)))
            merged_distances = np.concatenate((keep_distances, distances))
            selected = np.lexsort((candidate_ids[merged_positions], merged_distances))[:plan.limit]
            keep_positions = merged_positions[selected]
            keep_distances = merged_distances[selected]

        columns = {name: index + 1 for index, name in enumerate(METADATA)}
        rows = [{name: candidates[position][columns[name]] for name in plan.projection}
                for position in keep_positions]
        return QueryResult(rows, keep_distances.tolist(), count, self.dataset.dimension)
