import numpy as np
import pytest
from vecstor.datasets import generate_dataset,save_dataset
from vecstor.rpc.sql_client import SessionClient
from test_sql_rpc import servers

SQL='SELECT id,vector_distance(vector,[0,0,0]) AS distance FROM items WHERE value<1000 ORDER BY distance LIMIT 10'
GPU=" SETTINGS device='gpu',mode='approx',path='search-first',nprobe=1"


def test_real_sql_gpu_query_device_switch_and_write_invalidation(tmp_path):
    with servers(tmp_path) as (address,backend),SessionClient(address) as c:
        c.open_database('demo',True);save_dataset(generate_dataset(2048,3),tmp_path/'imports/a.npz');c.import_dataset('a.npz','items')
        exact=c.execute(SQL)['rows']
        built=c.build_index('items',nlist=1,device='gpu')
        assert built['device']=='gpu' and built['state']=='ready'
        result=c.execute(SQL+GPU)
        assert result['rows']==exact
        assert result['execution_plan']['actual_device']=='gpu'
        assert c.execute(SQL)['execution_plan']['actual_device']=='cpu'
        plan=c.execute('EXPLAIN '+SQL+GPU)['execution_plan']
        assert plan['actual_device']=='not_executed' and plan['counts']['vectors_fetched']==0
        c.execute('UPDATE items SET value=value+1 WHERE id=0')
        with pytest.raises(ValueError,match='gpu_replica_missing'):
            c.execute(SQL+GPU)
        fallback=c.execute(SQL+GPU+",device_fallback='cpu'")
        assert fallback['execution_plan']['actual_device']=='cpu'
        assert fallback['execution_plan']['device_fallback_reason']
        c.build_index('items',nlist=1,device='gpu')
        assert c.execute(SQL+GPU)['execution_plan']['actual_device']=='gpu'


def test_gpu_unsupported_path_and_large_candidates_never_silently_clamp(tmp_path):
    with servers(tmp_path) as (address,_),SessionClient(address) as c:
        c.open_database('demo',True);save_dataset(generate_dataset(4096,3),tmp_path/'imports/a.npz');c.import_dataset('a.npz','items')
        c.build_index('items',nlist=1,device='gpu')
        with pytest.raises(ValueError,match='gpu_path_unsupported'):
            c.execute(SQL+GPU.replace('search-first','filter-first'))
        with pytest.raises(ValueError,match='gpu_candidate_limit'):
            c.execute(SQL+GPU+",candidates=3000")
        result=c.execute(SQL+GPU+",candidates=3000,device_fallback='cpu'")
        assert result['execution_plan']['actual_device']=='cpu'
        assert result['execution_plan']['device_fallback_reason']=='gpu_candidate_limit'


def test_concurrent_gpu_and_cpu_sessions_keep_request_options(tmp_path):
    from concurrent.futures import ThreadPoolExecutor
    with servers(tmp_path) as (address,_),SessionClient(address) as c:
        c.open_database('demo',True);save_dataset(generate_dataset(2048,3),tmp_path/'imports/a.npz');c.import_dataset('a.npz','items')
        c.build_index('items',nlist=1,device='gpu')
        expected=c.execute(SQL)['rows']
        def query(number):
            with SessionClient(address) as other:
                other.open_database('demo')
                result=other.execute(SQL+(GPU if number%2 else " SETTINGS device='cpu'"))
                assert result['rows']==expected
                assert result['execution_plan']['actual_device']==('gpu' if number%2 else 'cpu')
                assert other.search_options.device=='cpu'
        with ThreadPoolExecutor(4) as pool:list(pool.map(query,range(8)))


def test_gpu_read_lease_survives_concurrent_write(tmp_path,monkeypatch):
    import threading
    from concurrent.futures import ThreadPoolExecutor
    from vecstor.compute.lease_io import LeaseIO
    with servers(tmp_path) as (address,_),SessionClient(address) as c:
        c.open_database('demo',True);save_dataset(generate_dataset(2048,3),tmp_path/'imports/a.npz');c.import_dataset('a.npz','items');c.build_index('items',nlist=1,device='gpu')
        expected=c.execute(SQL)['rows'];ready=threading.Event();resume=threading.Event();original=LeaseIO.open
        def paused(io):
            result=original(io);ready.set();assert resume.wait(5);return result
        monkeypatch.setattr(LeaseIO,'open',paused)
        def old_read():
            with SessionClient(address) as other:other.open_database('demo');return other.execute(SQL+GPU)
        with ThreadPoolExecutor(1) as pool:
            pending=pool.submit(old_read)
            try:
                assert ready.wait(5);c.execute('UPDATE items SET value=2000 WHERE value<1000')
            finally:resume.set()
            assert pending.result(timeout=5)['rows']==expected
        monkeypatch.setattr(LeaseIO,'open',original)
        with pytest.raises(ValueError,match='gpu_replica_missing'):c.execute(SQL+GPU)


def test_clear_gpu_replica_preserves_cpu_index(tmp_path):
    with servers(tmp_path) as (address,_),SessionClient(address) as c:
        c.open_database('demo',True);save_dataset(generate_dataset(2048,3),tmp_path/'imports/a.npz');c.import_dataset('a.npz','items');c.build_index('items',nlist=1,device='gpu')
        assert c.clear_indexes('items',device='gpu')['removed']==1
        assert all(i['device']=='cpu' for i in c.list_indexes())
        with pytest.raises(ValueError,match='gpu_replica_missing'):c.execute(SQL+GPU)
        assert c.execute(SQL+GPU.replace("device='gpu'","device='cpu'"))['execution_plan']['actual_mode']=='approx'
