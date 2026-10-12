"""Public SQL entry point rejects unsupported ASTs before storage execution."""
import duckdb
import numpy as np
import pytest

from vecstor.query.parser import QueryError
from vecstor.query.statements import classify_statement, safe_name


@pytest.mark.parametrize('sql,kind,table', [
    ('CREATE DATABASE Demo', 'create_database', None), ('USE Demo', 'use', None),
    ('SHOW DATABASES', 'show_databases', None), ('SHOW TABLES', 'show_tables', None),
    ('DESCRIBE things', 'describe', 'things'),
    ('CREATE TABLE things (id BIGINT PRIMARY KEY, title VARCHAR NOT NULL, ok BOOLEAN, n INTEGER, v DOUBLE, vector FLOAT[128])', 'create', 'things'),
    ('DROP TABLE things', 'drop', 'things'),
    ("INSERT INTO things (id,title) VALUES (1,'中文'),(2,NULL)", 'insert', 'things'),
    ('UPDATE things SET v=v+1, vector=[1,-2,3.5] WHERE id=1', 'update', 'things'),
    ('DELETE FROM things WHERE id=1 OR v IS NULL', 'delete', 'things'),
    ('SELECT id AS item, v*2 FROM things WHERE v>=1 AND title IS NOT NULL ORDER BY id DESC LIMIT 10 OFFSET 2', 'select', 'things'),
    ('SELECT title, COUNT(*) AS n, SUM(v), AVG(v), MIN(v), MAX(v) FROM things GROUP BY title HAVING COUNT(*)>1', 'select', 'things'),
])
def test_supported_classification(sql, kind, table):
    plan = classify_statement(sql)
    assert (plan.kind, plan.table) == (kind, table)
    assert plan.is_write == (kind in {'create','drop','insert','update','delete'})
    if kind in {'create_database', 'use'}:
        assert plan.database_name == 'demo'


@pytest.mark.parametrize('sql', [
    'SELECT * FROM read_csv(\'/tmp/x\')', 'SELECT read_blob(\'/tmp/x\') FROM t',
    'SELECT (SELECT 1) FROM t', 'SELECT * FROM t JOIN u USING(id)',
    'WITH x AS (SELECT * FROM t) SELECT * FROM x', 'SELECT * FROM t; DROP TABLE t',
    'SELECT * FROM main.t', 'SELECT * FROM _vecstor_operations', 'SELECT * FROM duckdb_tables()',
    "COPY t TO '/tmp/out'", "ATTACH '/tmp/x' AS x", 'PRAGMA version', 'INSTALL httpfs', 'LOAD httpfs',
    'BEGIN', 'COMMIT', 'ROLLBACK', 'INSERT INTO t SELECT * FROM u',
    "CREATE TABLE t AS SELECT * FROM read_csv('/tmp/x')", 'CREATE TABLE t (x INT DEFAULT 1)',
    'CREATE TABLE t (x INT CHECK(x>0))', 'CREATE TABLE t (x STRUCT(a INT))', 'CREATE TABLE t (x FLOAT[])',
    'CREATE TEMP TABLE t (x INT)', 'CREATE OR REPLACE TABLE t (x INT)',
    'UPDATE t SET x=1 FROM u', 'DELETE FROM t USING u', 'DROP TABLE t CASCADE',
    'SELECT row_number() OVER () FROM t', 'SELECT * EXCLUDE(id) FROM t',
    'SELECT * FROM t LIMIT 1 PERCENT', 'SELECT * FROM t UNION SELECT * FROM u',
    'SELECT * FROM t WHERE x LIKE \'a%\'', 'SELECT * FROM t WHERE x IN (1,2)',
    'SELECT * FROM t ORDER BY random()', 'SELECT DISTINCT x FROM t',
    'CREATE DATABASE "../bad"', 'SHOW TABLES FROM other',
    'INSERT INTO t VALUES ([1, NULL])', 'INSERT INTO t VALUES ([1e100])',
])
def test_rejected_syntax(sql):
    with pytest.raises(QueryError):
        classify_statement(sql)


def test_vector_alias_and_metadata_filter():
    plan = classify_statement("SELECT id, category AS kind, vector_distance(vector,[0.1,-2,3]) AS distance FROM Products WHERE category='论文' AND value>=5 ORDER BY distance, id LIMIT 5")
    assert plan.kind == 'vector'
    vector = plan.vector
    assert vector.table == 'products'
    assert vector.query_vector == tuple(float(x) for x in np.array([0.1,-2,3], dtype=np.float32))
    assert vector.limit == 5
    assert vector.projection == (('id','id'),('kind','category'),('distance','distance'))
    assert 'ORDER BY' not in vector.metadata_sql and 'LIMIT' not in vector.metadata_sql
    connection = duckdb.connect()
    connection.execute('CREATE TABLE products(id BIGINT, category VARCHAR, timestamp BIGINT, value DOUBLE)')
    connection.execute("INSERT INTO products VALUES (1,'论文',0,6),(2,'论文',0,4),(3,'其他',0,7)")
    assert connection.execute(vector.metadata_sql).fetchall() == [(1,'论文',0,6.0)]
    connection.close()


def test_vector_star_and_large_literal():
    plan = classify_statement('SELECT * FROM t ORDER BY vector_distance(vector,[' + ','.join(['0']*128) + ']) LIMIT 3')
    assert len(plan.vector.query_vector) == 128
    assert plan.vector.projection == tuple((x,x) for x in ('id','category','timestamp','value','vector'))


@pytest.mark.parametrize('sql', [
    'SELECT id FROM t ORDER BY vector_distance(vector,[]) LIMIT 1',
    'SELECT id FROM t ORDER BY vector_distance(vector,[1e100]) LIMIT 1',
    'SELECT id FROM t ORDER BY vector_distance(vector,[NULL]) LIMIT 1',
    'SELECT id FROM t ORDER BY vector_distance(vector,[1]) DESC LIMIT 1',
    'SELECT id FROM t ORDER BY vector_distance(vector,[1]) LIMIT 0',
    'SELECT id FROM t ORDER BY vector_distance(vector,[1]) LIMIT 1 OFFSET 1',
    'SELECT id FROM t ORDER BY vector_distance(vector,[1]), value LIMIT 1',
    'SELECT id FROM t WHERE id IS NULL ORDER BY vector_distance(vector,[1]) LIMIT 1',
    'SELECT vector_distance(vector,[2]) AS d FROM t ORDER BY vector_distance(vector,[1]) LIMIT 1',
    'SELECT vector_distance(vector,[1]) AS d FROM t',
    'SELECT COUNT(*) FROM t ORDER BY vector_distance(vector,[1]) LIMIT 1',
    'SELECT value AS d, vector_distance(vector,[1]) AS d FROM t ORDER BY d LIMIT 1',
])
def test_rejected_vector_semantics(sql):
    with pytest.raises(QueryError):
        classify_statement(sql)


def test_normalized_sql_runs_with_comments_literals_and_nulls():
    db = duckdb.connect()
    for sql in ['CREATE TABLE Things (id BIGINT PRIMARY KEY, x DOUBLE, title VARCHAR)',
                "INSERT INTO Things VALUES (1,2,'a;中文'),(2,NULL,'b') -- comment",
                'UPDATE Things SET x=x+2 WHERE id=1']:
        db.execute(classify_statement(sql).sql)
    result = db.execute(classify_statement('SELECT id, x FROM Things ORDER BY id').sql).fetchall()
    assert result == [(1,4.0),(2,None)]
    db.close()


@pytest.mark.parametrize('value', ['', '../a', '_internal', 'a.b', '数据库', 'main', 'information_schema', 'pg_catalog'])
def test_unsafe_names(value):
    with pytest.raises(QueryError):
        safe_name(value)


def test_name_normalizes():
    assert safe_name('Demo_12') == 'demo_12'


def test_resource_limits():
    for sql in ['', ' '*65537, 'SELECT ' + ','.join('1' for _ in range(4097)) + ' FROM t']:
        with pytest.raises(QueryError):
            classify_statement(sql)


@pytest.mark.parametrize('sql', ['SHOW /*comment*/ DATABASES;', 'SHOW DATABASES -- comment', 'SHOW DATABASES /*comment*/'])
def test_show_databases_ignores_comments(sql):
    assert classify_statement(sql).kind == 'show_databases'


@pytest.mark.parametrize('sql', ['SHOW DATABASES LIKE \'%\'', 'SHOW DATABASES; SELECT * FROM t', 'DROP TABLE t,u', 'SELECT x FROM t FOR UPDATE', 'SELECT x COLLATE NOCASE FROM t', 'CREATE TABLE t (x INT REFERENCES other(id))'])
def test_extra_arguments_rejected(sql):
    with pytest.raises(QueryError):
        classify_statement(sql)


def test_create_vector_schema_round_trip():
    db = duckdb.connect()
    plan = classify_statement('CREATE TABLE Items (ID BIGINT PRIMARY KEY, Category VARCHAR NOT NULL, Timestamp BIGINT NOT NULL, Value DOUBLE NOT NULL, Vector FLOAT[3] NOT NULL)')
    db.execute(plan.sql)
    assert [(row[0],row[1]) for row in db.execute('DESCRIBE items').fetchall()] == [('id','BIGINT'),('category','VARCHAR'),('timestamp','BIGINT'),('value','DOUBLE'),('vector','FLOAT[3]')]
    db.close()
