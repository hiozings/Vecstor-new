from concurrent.futures import ThreadPoolExecutor
from contextlib import ExitStack
from dataclasses import replace
from threading import Event
from types import SimpleNamespace
import json

import pytest

from vecstor.cache.types import CacheConfig
from vecstor.compute.service import create_compute_server
from vecstor.datasets import generate_dataset,save_dataset
from vecstor.query.search_options import SearchOptions
from vecstor.rpc.sql_client import SessionClient
from vecstor.storage.sql_service import create_catalog_server

SQL='SELECT id FROM items WHERE value>=0 ORDER BY vector_distance(vector,[0,0,0,0]) LIMIT 10'


@pytest.fixture
def cached(tmp_path):
    with ExitStack() as cleanup:
        storage,sport,backend=create_catalog_server(tmp_path,'127.0.0.1:0');storage.start()
        cleanup.callback(backend.close);cleanup.callback(lambda:storage.stop(0).wait())
        server,port,compute=create_compute_server(f'127.0.0.1:{sport}','127.0.0.1:0',timeout=60,
            index_source='published',data_cache_config=CacheConfig(max_bytes=1024**2))
        server.start();cleanup.callback(compute.close);cleanup.callback(lambda:server.stop(0).wait())
        client=cleanup.enter_context(SessionClient(f'127.0.0.1:{port}',timeout=60));client.open_database('demo',create=True)
        save_dataset(generate_dataset(2048,4,seed=27),tmp_path/'imports/source.npz');client.import_dataset('source.npz','items')
        client.publish_index('items',nlist=1,operation_id='first');client.load_index('items')
        client.search_options=SearchOptions(mode='approx',path='search-first',nprobe=1,fallback='error')
        yield SimpleNamespace(client=client,compute=compute.sql,backend=backend,root=tmp_path,address=f'127.0.0.1:{port}',storage_address=f'127.0.0.1:{sport}')


def test_cold_warm_queries_save_actual_vector_rpc(cached):
    assert cached.compute.data_cache.stats(cached.client.database_id)['process']['entry_count']==0
    cold=cached.client.execute(SQL);warm=cached.client.execute(SQL)
    assert cold['rows']==warm['rows']
    a=cold['execution_plan'];b=warm['execution_plan']
    assert a['counts']['vectors_fetched']==64 and b['counts']['vectors_fetched']==0
    assert b['data_cache']['vectors_served_from_cache']==64
    assert a['counts']['rpc_calls']==b['counts']['rpc_calls']+1
    assert b['counts']['rpc_payload_bytes']<a['counts']['rpc_payload_bytes']
    assert b['artifact_id']==a['artifact_id']


def test_partial_hits_across_larger_candidate_budget(cached):
    cold=cached.client.execute(SQL)
    cached.client.search_options=replace(cached.client.search_options,candidates=128)
    partial=cached.client.execute(SQL)
    assert partial['rows']==cold['rows']
    stats=partial['execution_plan']['data_cache']
    assert stats['hit_rows']==64 and stats['miss_rows']==64
    assert partial['execution_plan']['counts']['vectors_fetched']==64


def test_other_predicate_still_filters_cached_vectors(cached):
    cached.client.execute(SQL)
    filtered=cached.client.execute(SQL.replace('value>=0','value<150'))
    cached.compute.data_cache.clear(cached.client.database_id)
    cold=cached.client.execute(SQL.replace('value>=0','value<150'))
    assert filtered['rows']==cold['rows']
    assert filtered['execution_plan']['counts']['filter_candidate_calls']>0
    assert filtered['execution_plan']['data_cache']['hit_rows']>0


def test_exact_scan_lookup_only_and_admission_switch(cached):
    cached.client.reset_search()
    cached.client.execute(SQL)
    assert cached.compute.data_cache.stats(cached.client.database_id)['process']['entry_count']==0
    cached.compute.data_cache.config=replace(cached.compute.data_cache.config,scan_admission=True)
    cold=cached.client.execute(SQL);warm=cached.client.execute(SQL)
    assert warm['rows']==cold['rows']
    assert cold['execution_plan']['counts']['vectors_fetched']==2048
    # 1 MiB cannot hold all2048 records with management charges: eviction is real.
    assert warm['execution_plan']['data_cache']['admitted_rows']>0


def test_explain_never_touches_cache_and_analyze_executes_once(cached):
    cached.client.execute(SQL)
    before=cached.compute.data_cache.stats(cached.client.database_id)
    explained=json.loads(cached.client.execute('EXPLAIN '+SQL)['rows'][0][0])
    assert explained['data_cache']['lookup_rows']==0 and explained['counts']['vectors_fetched']==0
    assert cached.compute.data_cache.stats(cached.client.database_id)==before
    analyzed=json.loads(cached.client.execute('EXPLAIN ANALYZE '+SQL)['rows'][0][0])
    assert analyzed['data_cache']['lookup_rows']==64


def test_write_and_other_table_change_cannot_reuse_cached_revision(cached):
    cached.client.execute(SQL)
    cached.client.execute('CREATE TABLE other (id BIGINT)')
    with pytest.raises(ValueError,match='index_not_published'):cached.client.execute(SQL)
    cached.client.search_options=replace(cached.client.search_options,fallback='exact')
    result=cached.client.execute(SQL)
    assert result['execution_plan']['data_cache']['hit_rows']==0
    cached.client.execute('UPDATE items SET vector=[0,0,0,0] WHERE id=1')
    assert cached.client.execute(SQL)['rows'][0]==[1]


def test_new_artifact_same_revision_reuses_data_but_corrupt_never_bypassed(cached):
    cached.client.execute(SQL)
    cached.client.publish_index('items',nlist=2,operation_id='second')
    with pytest.raises(ValueError,match='published_index_not_loaded'):cached.client.execute(SQL)
    cached.client.load_index('items')
    cached.client.search_options=replace(cached.client.search_options,nprobe=2)
    result=cached.client.execute(SQL)
    assert result['execution_plan']['data_cache']['hit_rows']>0
    db=cached.backend.catalog.database(cached.client.database_id)
    db.pool.submit(lambda:db.connection.execute("UPDATE __vecstor_index_artifacts SET state='CORRUPT'")).result()
    with pytest.raises(ValueError,match='DATA_LOSS'):cached.client.execute(SQL)


def test_old_warm_query_remains_bound_across_write(cached,monkeypatch):
    expected=cached.client.execute(SQL)
    entered=Event();release=Event();original=cached.backend.catalog.filter_candidates
    def pause(*a,**k):
        entered.set();assert release.wait(10)
        return original(*a,**k)
    monkeypatch.setattr(cached.backend.catalog,'filter_candidates',pause)
    with ThreadPoolExecutor(1) as pool:
        job=pool.submit(cached.client.execute,SQL)
        try:
            assert entered.wait(5)
            cached.backend.catalog.execute(cached.client.database_id,'UPDATE items SET vector=[0,0,0,0] WHERE id=1','write')
        finally:release.set()
        old=job.result(timeout=5)
    assert old['rows']==expected['rows'] and old['revision']==expected['revision']
    assert old['execution_plan']['data_cache']['hit_rows']==64


def test_parallel_request_stats_do_not_use_global_deltas(cached):
    cached.client.execute(SQL)
    def query(_):
        with SessionClient(cached.address,timeout=60) as client:
            client.open_database('demo');client.search_options=cached.client.search_options
            return client.execute(SQL)['execution_plan']['data_cache']
    with ThreadPoolExecutor(4) as pool:stats=list(pool.map(query,range(4)))
    assert all(s['lookup_rows']==64 and s['hit_rows']==64 for s in stats)


def test_exact_warm_scan_with_sufficient_capacity(cached):
    from vecstor.cache.vector_cache import VersionedVectorCache
    cached.client.reset_search()
    cached.compute.data_cache.close()
    cached.compute.data_cache=VersionedVectorCache(CacheConfig(max_bytes=2*1024**2,scan_admission=True))
    first=cached.client.execute(SQL);second=cached.client.execute(SQL)
    assert second['rows']==first['rows']
    assert second['execution_plan']['counts']['vectors_fetched']==0
    assert second['execution_plan']['data_cache']['vectors_served_from_cache']==2048


def test_drop_recreate_and_second_database_do_not_hit_old_scope(cached):
    cached.client.execute(SQL)
    cached.client.execute('DROP TABLE items')
    cached.client.import_dataset('source.npz','items')
    cached.client.search_options=replace(cached.client.search_options,fallback='exact')
    assert cached.client.execute(SQL)['execution_plan']['data_cache']['hit_rows']==0
    cached.client.open_database('another',create=True)
    cached.client.import_dataset('source.npz','items')
    assert cached.client.execute(SQL)['execution_plan']['data_cache']['hit_rows']==0


def test_warm_cache_does_not_allow_expired_final_result(cached,monkeypatch):
    import time
    from vecstor.compute.data_reader import CachedLeaseReader
    cached.client.execute(SQL)
    original=CachedLeaseReader.fetch
    def expire(self,*args,**kwargs):
        vectors=original(self,*args,**kwargs)
        assert self.statistics['hit_rows']==64
        self.io.deadline=time.monotonic()-1
        return vectors
    monkeypatch.setattr(CachedLeaseReader,'fetch',expire)
    with pytest.raises(ValueError,match='DEADLINE_EXCEEDED'):cached.client.execute(SQL)
