import duckdb
import pytest

from vecstor.query.statements import classify_statement
from vecstor.query.read_projection import required_metadata_fields, project_metadata_sql


def test_projection_keeps_predicate_and_source_aliases():
    plan=classify_statement("SELECT id AS x,vector_distance(vector,[0,0]) AS d FROM items WHERE category='kept' ORDER BY d LIMIT 2").vector
    original=plan.metadata_sql
    fields=required_metadata_fields(plan,True)
    assert fields==('id',)
    with duckdb.connect(':memory:') as connection:
        connection.execute('CREATE TABLE items(id BIGINT,category VARCHAR,timestamp BIGINT,value DOUBLE)')
        connection.execute("INSERT INTO items VALUES (0,'kept',0,0),(1,'other',0,0)")
        result=connection.execute(project_metadata_sql(original,fields))
        assert [d[0] for d in result.description]==['id']
        assert result.fetchall()==[(0,)]
    assert plan.metadata_sql==original


def test_star_and_full_require_all_metadata():
    star=classify_statement('SELECT * FROM items ORDER BY vector_distance(vector,[0,0]) LIMIT 1').vector
    assert required_metadata_fields(star,True)==('id','category','timestamp','value')
    narrow=classify_statement('SELECT value AS v FROM items ORDER BY vector_distance(vector,[0,0]) LIMIT 1').vector
    assert required_metadata_fields(narrow,True)==('id','value')
    assert required_metadata_fields(narrow,False)==('id','category','timestamp','value')


@pytest.mark.parametrize('fields',[(),('value',),('id','id'),('value','id'),('id','vector'),('id','value','category')])
def test_invalid_field_mask_rejected(fields):
    with pytest.raises(ValueError):project_metadata_sql('SELECT id FROM items',fields)
