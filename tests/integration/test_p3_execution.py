import numpy as np
import pytest

from vecstor.data import Dataset
from vecstor.datasets import save_dataset
from vecstor.rpc.sql_client import SessionClient
from vecstor.query.search_options import SearchOptions
from test_sql_rpc import servers

BASE=2**60
SQL="SELECT id, vector_distance(vector,[0,0]) AS distance FROM items WHERE value >= 100 ORDER BY distance LIMIT 5"


@pytest.fixture
def populated(tmp_path):
    with servers(tmp_path) as (address,backend), SessionClient(address) as client:
        client.open_database("demo",True)
        n=1024
        dataset=Dataset(BASE+np.arange(n,dtype=np.int64),["x"]*n,np.zeros(n,dtype=np.int64),
                        np.arange(n,dtype=np.float64),np.column_stack((np.arange(n,dtype=np.float32),np.zeros(n,dtype=np.float32))))
        save_dataset(dataset,tmp_path/"imports/source.npz")
        client.import_dataset("source.npz","items")
        yield client,backend,address


def test_two_ann_paths_and_candidate_expansion(populated):
    client,_,_=populated
    expected=client.execute(SQL)["rows"]
    built=client.build_index("items",nlist=1)
    assert built["row_count"]==1024
    assert client.build_index("items",nlist=1)["reused"] is True
    for path in ["filter-first","search-first"]:
        client.search_options=SearchOptions(mode="approx",path=path,nprobe=1,candidates=64,fallback="error")
        actual=client.execute(SQL)
        assert actual["rows"]==expected
        trace=actual["execution_plan"]
        assert trace["actual_mode"]=="approx" and trace["actual_path"]==path
        assert trace["index"]["revision"]==actual["revision"]
        if path=="search-first":
            assert len(trace["rounds"])>=2
            assert trace["counts"]["scan_metadata_calls"]==0
            assert trace["counts"]["filter_candidate_calls"]>=2


def test_budget_and_stale_index_fall_back_without_changing_sql(populated):
    client,_,_=populated
    client.build_index("items",nlist=1)
    expected=client.execute(SQL)["rows"]
    client.search_options=SearchOptions(mode="approx",path="search-first",nprobe=1,candidates=32,max_candidates=32)
    result=client.execute(SQL)
    assert result["rows"]==expected
    assert result["execution_plan"]["actual_mode"]=="exact"
    assert result["execution_plan"]["fallback_reason"]=="candidate_budget"
    client.execute(f"UPDATE items SET vector=[0,0] WHERE id={BASE+100}")
    result=client.execute(SQL)
    assert result["rows"][0]==[BASE+100,0.0]
    assert result["execution_plan"]["fallback_reason"]=="index_version_mismatch"


def test_explain_does_not_scan_or_fetch_and_analyze_reports_real_work(populated):
    client,_,_=populated
    result=client.execute("EXPLAIN "+SQL)
    import json
    trace=json.loads(result["rows"][0][0])
    assert trace["counts"]["vectors_fetched"]==0
    assert trace["counts"]["scan_metadata_calls"]==0
    assert client.list_indexes()==[]
    result=client.execute("EXPLAIN ANALYZE "+SQL)
    trace=json.loads(result["rows"][0][0])
    assert trace["counts"]["vectors_fetched"]==924
    assert trace["counts"]["scan_metadata_calls"]==1
    assert trace["actual_mode"]=="exact"


def test_strict_ann_errors_and_new_session_stays_exact(populated):
    client,_,address=populated
    client.set_search("mode","approx")
    client.set_search("fallback","error")
    with pytest.raises(ValueError,match="FAILED_PRECONDITION"):
        client.execute(SQL)
    with SessionClient(address) as other:
        other.open_database("demo")
        assert other.execute(SQL)["execution_plan"]["actual_mode"]=="exact"


def test_active_old_lease_index_survives_other_table_write(populated):
    client,backend,_=populated
    client.build_index("items",nlist=1)
    lease=backend.catalog.begin_read(client.database_id,"items")
    # Registry pins are tested separately; the RPC query must reject a changed database revision.
    client.execute("CREATE TABLE ordinary (id BIGINT)")
    client.search_options=SearchOptions(mode="approx")
    result=client.execute(SQL)
    assert result["execution_plan"]["fallback_reason"]=="index_version_mismatch"
    backend.catalog.end_read(client.database_id,lease["token"])


def test_empty_prefilter_needs_no_index_even_in_strict_ann(populated):
    client,_,_=populated
    client.search_options=SearchOptions(mode="approx",fallback="error")
    result=client.execute(SQL.replace("value >= 100","value < 0"))
    assert result["rows"]==[]
    assert result["execution_plan"]["actual_mode"]=="exact"
    assert result["execution_plan"]["fallback_reason"]=="exact_empty"


def test_query_pins_old_index_and_read_lease_across_write_and_rebuild(populated):
    from concurrent.futures import ThreadPoolExecutor
    from threading import Event
    client,backend,address=populated
    expected=client.execute(SQL)['rows']
    client.build_index('items',nlist=1)
    entered,release=Event(),Event()
    original=backend.catalog.vectors
    first=[True]
    def paused(*args,**kwargs):
        if first[0]:
            first[0]=False
            entered.set()
            assert release.wait(10)
        return original(*args,**kwargs)
    backend.catalog.vectors=paused
    with SessionClient(address) as old, ThreadPoolExecutor(1) as pool:
        old.open_database('demo')
        old.search_options=SearchOptions(mode='approx',path='search-first',nprobe=1,fallback='error')
        query=pool.submit(old.execute,SQL)
        try:
            assert entered.wait(5)
            assert client.clear_indexes('items')=={'removed':0,'busy':1}
            client.execute(f'UPDATE items SET vector=[0,0] WHERE id={BASE+100}')
            built=client.build_index('items',nlist=1)
        finally:
            release.set()
        previous=query.result(timeout=5)
    assert previous['rows']==expected
    assert previous['revision']<built['revision']
    assert previous['execution_plan']['index']['revision']==previous['revision']
    client.search_options=SearchOptions(mode='approx',nprobe=1,fallback='error')
    fresh=client.execute(SQL)
    assert fresh['rows'][0]==[BASE+100,0.0]
    assert fresh['revision']==built['revision']


def test_drop_and_recreate_same_name_never_uses_old_table_index(populated):
    client,_,_=populated
    built=client.build_index('items',nlist=1)
    client.execute('DROP TABLE items')
    client.import_dataset('source.npz','items')
    client.search_options=SearchOptions(mode='approx',path='search-first',fallback='error')
    with pytest.raises(ValueError,match='index_missing'):
        client.execute(SQL)
    new=client.build_index('items',nlist=1)
    assert new['table_id']!=built['table_id']
    assert client.execute(SQL)['execution_plan']['index']['table_id']==new['table_id']


def test_strict_candidate_budget_is_error_not_partial_success(populated):
    client,_,_=populated
    client.build_index('items',nlist=1)
    client.search_options=SearchOptions(mode='approx',path='search-first',nprobe=1,
        candidates=32,max_candidates=32,fallback='error')
    with pytest.raises(ValueError,match='RESOURCE_EXHAUSTED.*candidate_budget'):
        client.execute(SQL)


def test_unsafe_finite_query_uses_float64_exact_fallback(populated):
    client,_,_=populated
    sql=SQL.replace('[0,0]','[1e30,1e30]')
    expected=client.execute(sql)['rows']
    client.build_index('items',nlist=1)
    client.search_options=SearchOptions(mode='approx')
    result=client.execute(sql)
    assert result['rows']==expected
    assert result['execution_plan']['fallback_reason']=='unsafe_float32_range'
    client.search_options=SearchOptions(mode='approx',fallback='error')
    with pytest.raises(ValueError,match='FAILED_PRECONDITION.*unsafe_float32_range'):
        client.execute(sql)


def test_expired_ann_lease_cannot_be_replaced_by_local_index(populated):
    import time
    client,backend,_=populated
    client.build_index('items',nlist=1)
    begin=backend.catalog.begin_read
    candidates=backend.catalog.filter_candidates
    def short_lease(database_id,table,ttl=15,timeout=15):
        return begin(database_id,table,ttl=.03,timeout=timeout)
    def after_expiry(*args,**kwargs):
        time.sleep(.06)
        return candidates(*args,**kwargs)
    backend.catalog.begin_read=short_lease
    backend.catalog.filter_candidates=after_expiry
    client.search_options=SearchOptions(mode='approx',path='search-first',nprobe=1)
    with pytest.raises(ValueError,match='lease|token'):
        client.execute(SQL)


def test_filter_selector_is_prepared_once_across_candidate_rounds(populated):
    client,_,_=populated
    client.build_index('items',nlist=1)
    client.search_options=SearchOptions(mode='approx',nprobe=1,candidates=1,fallback='error')
    trace=client.execute(SQL)['execution_plan']
    assert len(trace['rounds'])>=3
    assert trace['counts']['selector_builds']==1
    assert trace['selector_full_coverage'] is False
    assert trace['native_search_ms']>=0 and trace['selector_build_ms']>=0
    full=client.execute(SQL.replace('value >= 100','value >= 0'))['execution_plan']
    assert full['counts']['selector_builds']==0
    assert full['selector_full_coverage'] is True
