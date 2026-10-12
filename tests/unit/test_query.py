import pytest


BASE = "SELECT id, category FROM items {} ORDER BY vector_distance(vector, :query_vector) LIMIT 10"


def test_plan_binds_literals_and_preserves_logic():
    from vecstor.query.parser import parse_query
    plan = parse_query(BASE.format("WHERE (category = 'O''Reilly' OR value < -2.5) AND timestamp >= 100"))
    assert plan.projection == ("id", "category") and plan.limit == 10
    assert plan.filter_params == ("O'Reilly", -2.5, 100)
    assert "O'Reilly" not in plan.filter_sql
    assert "ORDER BY" not in plan.filter_sql and "LIMIT" not in plan.filter_sql


def test_case_insensitive_names_and_star():
    from vecstor.query.parser import parse_query
    plan = parse_query("SELECT * FROM ITEMS ORDER BY VECTOR_DISTANCE(VECTOR, :query_vector), ID ASC LIMIT 1;")
    assert plan.projection == ("id", "category", "timestamp", "value")
    assert plan.filter_params == ()


@pytest.mark.parametrize("condition", [
    "id = 7", "id != 3", "timestamp >= -1", "value < 1e2", "value <= 3.5",
    "value > -2", "value >= 0", "category <> 'book'", "(id = 1 OR id = 2) AND value != 0",
])
def test_supported_predicates(condition):
    from vecstor.query.parser import parse_query
    assert parse_query(BASE.format("WHERE " + condition)).filter_params


@pytest.mark.parametrize("sql", [
    "", "SELECT FROM", "DELETE FROM items", BASE.format("") + "; DROP TABLE items",
    BASE.format("").replace("items", "other"),
    BASE.format("").replace("items", "read_csv('/tmp/private.csv')"),
    BASE.format("").replace("items", "main.items"),
    BASE.format("").replace("items", "items i"),
    BASE.format("").replace("items", "items JOIN other ON items.id = other.id"),
    BASE.format("").replace("SELECT", "SELECT DISTINCT"),
    BASE.format("").replace("id, category", "id AS identifier"),
    BASE.format("").replace("id, category", "id, id"),
    BASE.format("").replace("id, category", "missing"),
    BASE.format("").replace("id, category", "vector"),
    BASE.format("").replace("id, category", "items.id"),
    BASE.format("").replace("id, category", "count(*)"),
    BASE.format("").replace("SELECT", "WITH t AS (SELECT 1) SELECT", 1),
    BASE.format("") + " UNION SELECT 1, 2", BASE.format("") + " OFFSET 1",
    BASE.format("GROUP BY id, category"), BASE.format("HAVING value > 1"),
    BASE.format("WHERE category > 'x'"), BASE.format("WHERE value = '1'"),
    BASE.format("WHERE id = 1.5"), BASE.format("WHERE timestamp = 9223372036854775808"),
    BASE.format("WHERE value > 1e999"), BASE.format("WHERE value > 1e1000000"),
    BASE.format("WHERE value > -1e1000000"), BASE.format("WHERE category = NULL"),
    BASE.format("WHERE id = TRUE"), BASE.format("WHERE 1 = id"),
    BASE.format("WHERE id = value"), BASE.format("WHERE id IN (1, 2)"),
    BASE.format("WHERE NOT id = 1"), BASE.format("WHERE lower(category) = 'x'"),
    BASE.format("WHERE value BETWEEN 1 AND 2"), BASE.format("WHERE id = :id"),
    BASE.format("WHERE id = (SELECT 1)"), BASE.format("WHERE vector = 1"),
    BASE.format("").replace(" LIMIT 10", ""),
    BASE.format("").replace("LIMIT 10", "LIMIT 0"),
    BASE.format("").replace("LIMIT 10", "LIMIT -1"),
    BASE.format("").replace("LIMIT 10", "LIMIT 1.5"),
    BASE.format("").replace("LIMIT 10", "LIMIT :k"),
    BASE.format("").replace("LIMIT 10", "LIMIT 9223372036854775808"),
    BASE.format("").replace("ORDER BY vector_distance(vector, :query_vector)", ""),
    BASE.format("").replace(") LIMIT", ") DESC LIMIT"),
    BASE.format("").replace(") LIMIT", "), value LIMIT"),
    BASE.format("").replace(") LIMIT", "), id DESC LIMIT"),
    BASE.format("").replace("vector_distance", "arbitrary_function"),
    BASE.format("").replace(":query_vector", ":other"),
    BASE.format("").replace(":query_vector", "[1, 2]"),
    BASE.format("").replace("(vector,", "(value,"),
])
def test_unsupported_sql_rejected(sql):
    from vecstor.query.parser import QueryError, parse_query
    with pytest.raises(QueryError):
        parse_query(sql)


def test_large_sql_rejected():
    from vecstor.query.parser import QueryError, parse_query
    with pytest.raises(QueryError):
        parse_query(BASE.format("WHERE category = '" + "x" * 9000 + "'"))
