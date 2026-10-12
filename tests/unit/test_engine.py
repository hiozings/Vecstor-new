import json

import numpy as np
import pytest


def sample():
    from vecstor.data import Dataset
    return Dataset(ids=[90, 7, 2, 30, 8], categories=["book", "paper", "paper", "paper", "O'Reilly"],
                   timestamps=[0, 1000, 2000, 3000, 4000], values=[20, 5, 12, 20, -1],
                   vectors=[[0, 0], [1, 0], [-1, 0], [2, 0], [0, 3]])


def sql(where="", projection="id, category", k=10):
    return f"SELECT {projection} FROM items {where} ORDER BY vector_distance(vector, :query_vector) LIMIT {k}"


def test_filter_before_topk_and_ties_not_input_order():
    from vecstor.query.engine import LocalEngine
    with LocalEngine(sample(), block_size=1) as engine:
        result = engine.execute(sql("WHERE category = 'paper'", k=2), [0, 0])
    assert result.rows == [{"id": 2, "category": "paper"}, {"id": 7, "category": "paper"}]
    assert result.distances == [1.0, 1.0]
    assert result.matched_count == 3
    payload = json.loads(json.dumps(result.to_dict(), allow_nan=False))
    assert payload["metric"] == "squared_l2" and payload["mode"] == "exact"


def test_conditions_precedence_and_projection():
    from vecstor.query.engine import LocalEngine
    with LocalEngine(sample()) as engine:
        result = engine.execute(sql("WHERE category = 'book' OR category = 'paper' AND value >= 12", "value, id"), [0, 0])
    assert result.rows == [{"value": 20.0, "id": 90}, {"value": 12.0, "id": 2}, {"value": 20.0, "id": 30}]
    assert result.distances == [0.0, 1.0, 4.0]


def test_parentheses_change_filter_and_no_padding():
    from vecstor.query.engine import LocalEngine
    with LocalEngine(sample()) as engine:
        result = engine.execute(sql("WHERE (category = 'paper' OR id = 90) AND value < 10"), [0, 0])
    assert result.rows == [{"id": 7, "category": "paper"}]
    assert result.matched_count == 1


def test_category_literal_cannot_execute_sql():
    from vecstor.query.engine import LocalEngine
    with LocalEngine(sample()) as engine:
        result = engine.execute(sql("WHERE category = 'x''; DROP TABLE items; --'"), [0, 0])
        assert result.rows == []
        assert engine.execute(sql("WHERE category = 'O''Reilly'"), [0, 0]).rows[0]["id"] == 8


def test_empty_dataset_and_zero_matches():
    from vecstor.data import Dataset
    from vecstor.query.engine import LocalEngine
    with LocalEngine(Dataset([], [], [], [], np.empty((0, 2)))) as engine:
        result = engine.execute(sql(), [0, 0])
        assert result.rows == [] and result.distances == [] and result.matched_count == 0
    with LocalEngine(sample()) as engine:
        assert engine.execute(sql("WHERE value > 999"), [0, 0]).matched_count == 0


@pytest.mark.parametrize("vector", [[0], [[0, 0]], [np.nan, 0], [np.inf, 0], [1e100, 0],
                                    ["0", "0"], [True, 0], [1j, 0], None])
def test_invalid_query_vector_rejected_even_without_candidates(vector):
    from vecstor.data import DataError
    from vecstor.query.engine import LocalEngine
    with LocalEngine(sample()) as engine:
        with pytest.raises(DataError):
            engine.execute(sql("WHERE id = 999"), vector)


@pytest.mark.parametrize("block_size", [1, 3, 128])
def test_random_queries_against_independent_dense_numpy(block_size):
    from vecstor.data import Dataset
    from vecstor.query.engine import LocalEngine
    rng = np.random.default_rng(91)
    vectors = rng.normal(size=(123, 9)).astype(np.float32)
    ids = rng.permutation(123) * 7 + 10
    values = rng.normal(size=123)
    data = Dataset(ids, ["x"] * 123, [0] * 123, values, vectors)
    query = rng.normal(size=9).astype(np.float32)
    mask = values >= 0
    eligible_ids = ids[mask]
    distances = np.sum((vectors[mask].astype(np.float64) - query.astype(np.float64)) ** 2, axis=1)
    order = np.lexsort((eligible_ids, distances))[:17]
    with LocalEngine(data, block_size=block_size) as engine:
        result = engine.execute(sql("WHERE value >= 0", "id", 17), query)
    assert [row["id"] for row in result.rows] == eligible_ids[order].tolist()
    np.testing.assert_allclose(result.distances, distances[order], rtol=1e-14)
    assert result.matched_count == int(mask.sum())


def test_near_ties_use_float64_and_large_values_remain_finite():
    from vecstor.data import Dataset
    from vecstor.query.engine import LocalEngine
    # float32 accumulation would collapse 1 + 1e-8 and 1 into a tie.
    data = Dataset([1, 2, 3], ["x"] * 3, [0] * 3, [0] * 3, [[1, 1e-4], [1, 0], [3e38, 0]])
    with LocalEngine(data) as engine:
        result = engine.execute(sql(projection="id"), [0, 0])
    assert [row["id"] for row in result.rows] == [2, 1, 3]
    assert np.isfinite(result.distances).all()


@pytest.mark.parametrize("block_size", [0, -1, True, 1.5])
def test_invalid_block_size_rejected(block_size):
    from vecstor.query.engine import LocalEngine
    with pytest.raises(ValueError):
        LocalEngine(sample(), block_size=block_size)


def test_closed_engine_reports_clear_error():
    from vecstor.query.engine import LocalEngine
    from vecstor.query.parser import QueryError
    engine = LocalEngine(sample())
    engine.close()
    with pytest.raises(QueryError, match="closed"):
        engine.execute(sql(), [0, 0])
