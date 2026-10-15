import pytest
from vecstor.query.statements import classify_statement
from vecstor.query.search_options import SearchOptions

SQL="SELECT id FROM items WHERE category='SETTINGS;cpu' ORDER BY vector_distance(vector,[0,0]) LIMIT 10"


def test_settings_are_typed_and_do_not_change_query_or_session():
    from vecstor.query.execution_settings import resolve_execution_options
    p=classify_statement(SQL+" SETTINGS device='gpu', mode='approx', path='search-first', nprobe=16")
    assert dict(p.settings)=={'device':'gpu','mode':'approx','path':'search-first','nprobe':16}
    assert p.sql==classify_statement(SQL).sql
    original=SearchOptions();effective=resolve_execution_options(original,dict(p.settings))
    assert effective.device=='gpu' and effective.nprobe==16
    assert original.mode=='exact' and original.device=='cpu'
    assert effective.to_proto().device=='gpu'
    assert SearchOptions.from_proto(effective.to_proto())==effective


@pytest.mark.parametrize('tail',["device='gpu', device='cpu'","unknown='cpu'","device=1","gpu_id=1","nprobe=1+1","device=(SELECT 'gpu')","device='gpu' SETTINGS mode='approx'",''])
def test_malformed_settings_rejected(tail):
    with pytest.raises(ValueError):classify_statement(SQL+' SETTINGS '+tail)


def test_settings_only_allowed_on_vector_select_and_explain():
    with pytest.raises(ValueError):classify_statement("SELECT id FROM items LIMIT 10 SETTINGS device='cpu'")
    with pytest.raises(ValueError):classify_statement("DELETE FROM items SETTINGS device='cpu'")
    p=classify_statement('EXPLAIN ANALYZE '+SQL+" SETTINGS device='cpu'")
    assert p.explain=='analyze' and dict(p.settings)=={'device':'cpu'}
    assert not classify_statement(SQL+' /* SETTINGS device=unknown */').settings
    with pytest.raises(ValueError):classify_statement(SQL+" SETTINGS device='cpu'; SELECT id FROM items")
