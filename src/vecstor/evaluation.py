"""Correctness workloads with independently computed dense NumPy truth."""

import numpy as np

from vecstor.query.engine import LocalEngine


def verify_baseline(dataset, seed=20261011):
    # A separate random stream keeps query vectors independent of data generation.
    rng = np.random.default_rng(np.random.SeedSequence([seed, 1]))
    queries, truths, actual = [], [], []
    cases = []
    for fraction in [0.001, 0.01, 0.1, 0.5, 1.0]:
        threshold = int(dataset.size * fraction)
        for k in [10, 100]:
            cases.append((f"fraction_{fraction}_k_{k}", f"value < {threshold}",
                          dataset.values < threshold, k))
    cases.extend([
        ("and_or", "(category = 'category_0' OR category = 'category_2') AND value >= 10",
         ((dataset.categories == "category_0") | (dataset.categories == "category_2")) & (dataset.values >= 10), 10),
        ("precedence", "category = 'category_0' OR category = 'category_2' AND value >= 10",
         (dataset.categories == "category_0") | ((dataset.categories == "category_2") & (dataset.values >= 10)), 100),
    ])
    with LocalEngine(dataset) as engine:
        for name, condition, mask, k in cases:
            vector = rng.random(dataset.dimension, dtype=np.float32)
            sql = f"SELECT id FROM items WHERE {condition} ORDER BY vector_distance(vector, :query_vector) LIMIT {k}"
            # Oracle deliberately does not call the engine's parser, DuckDB
            # predicate builder, block scan or candidate merge.
            vectors = dataset.vectors[mask].astype(np.float64)
            distances = np.sum(np.square(vectors - vector.astype(np.float64)), axis=1)
            ids = dataset.ids[mask]
            order = np.lexsort((ids, distances))[:k]
            wanted_ids, wanted_distances = ids[order].tolist(), distances[order].tolist()
            result = engine.execute(sql, vector)
            found_ids = [row["id"] for row in result.rows]
            if found_ids != wanted_ids or result.matched_count != int(mask.sum()):
                raise AssertionError(f"{name}: ID or matched-count mismatch")
            np.testing.assert_allclose(result.distances, wanted_distances, rtol=1e-12, atol=1e-12,
                                       err_msg=f"{name}: distance mismatch")
            queries.append({"name": name, "sql": sql, "vector": vector.tolist()})
            truths.append({"name": name, "ids": wanted_ids, "distances": wanted_distances,
                           "matched_count": int(mask.sum())})
            actual.append({"name": name, **result.to_dict()})
    return queries, truths, actual
