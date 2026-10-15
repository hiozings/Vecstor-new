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


def test_gpu_data_corruption_cannot_use_explicit_cpu_fallback(tmp_path,monkeypatch):
    from vecstor.index.gpu_backend import GpuIndex
    from vecstor.index.gpu_worker import GpuDataError
    with servers(tmp_path) as (address,_),SessionClient(address) as c:
        c.open_database('demo',True);save_dataset(generate_dataset(2048,3),tmp_path/'imports/a.npz');c.import_dataset('a.npz','items');c.build_index('items',nlist=1,device='gpu')
        def corrupt(*a,**kw):raise GpuDataError('gpu_nonfinite_result')
        monkeypatch.setattr(GpuIndex,'search',corrupt)
        with pytest.raises(ValueError,match='DATA_LOSS.*gpu_nonfinite_result'):
            c.execute(SQL+GPU+",device_fallback='cpu'")


def test_explicit_build_recovers_dead_worker_without_a_probe_query(tmp_path):
    import os,signal,time
    with servers(tmp_path) as (address,_),SessionClient(address) as c:
        c.open_database('demo',True);save_dataset(generate_dataset(2048,3),tmp_path/'imports/a.npz');c.import_dataset('a.npz','items')
        first=c.build_index('items',nlist=1,device='gpu');os.kill(first['worker_pid'],signal.SIGKILL)
        # SIGKILL delivery precedes GPU driver teardown; wait for actual exit.
        until=time.monotonic()+5
        while True:
            try:
                with open(f"/proc/{first['worker_pid']}/status") as source:status=source.read()
                if 'State:\tZ' in status:break
            except FileNotFoundError:break
            assert time.monotonic()<until,'GPU worker did not exit'
            time.sleep(.02)
        rebuilt=c.build_index('items',nlist=1,device='gpu')
        assert rebuilt['worker_pid']!=first['worker_pid'] and rebuilt['reused'] is False
        assert c.execute(SQL+GPU)['execution_plan']['actual_device']=='gpu'


@pytest.mark.parametrize('matches',[0,1,9])
def test_gpu_complete_snapshot_returns_all_legal_rows_without_cpu_fallback(tmp_path,matches):
    with servers(tmp_path) as (address,_),SessionClient(address) as c:
        c.open_database('demo',True);save_dataset(generate_dataset(1024,3),tmp_path/'imports/a.npz');c.import_dataset('a.npz','items');c.build_index('items',nlist=1,device='gpu')
        sql=SQL.replace('value<1000',f'value<{matches}')
        result=c.execute(sql+GPU+",candidates=1024,max_candidates=1024,max_rounds=1,fallback='error'")
        assert result['rows']==c.execute(sql)['rows']
        plan=result['execution_plan']
        assert plan['actual_device']=='gpu' and plan['actual_mode']=='approx'
        assert plan['candidate_space_complete'] is True and plan['stop_reason']=='candidate_space_exhausted'
        assert plan['counts']['scan_metadata_calls']==0


def test_full_nprobe_does_not_prove_full_candidate_coverage(tmp_path):
    with servers(tmp_path) as (address,_),SessionClient(address) as c:
        c.open_database('demo',True);save_dataset(generate_dataset(1024,3),tmp_path/'imports/a.npz');c.import_dataset('a.npz','items');c.build_index('items',nlist=1,device='gpu')
        with pytest.raises(ValueError):c.execute(SQL.replace('value<1000','value<0')+GPU+",candidates=64,max_candidates=64,max_rounds=1,fallback='error'")


def test_cpu_fallback_resolves_implicit_probe_against_cpu_index(tmp_path):
    with servers(tmp_path) as (address,_),SessionClient(address) as c:
        c.open_database('demo',True);save_dataset(generate_dataset(4096,3),tmp_path/'imports/a.npz');c.import_dataset('a.npz','items')
        c.build_index('items',nlist=16,device='gpu');c.build_index('items',nlist=1,device='cpu')
        result=c.execute(SQL+" SETTINGS device='gpu',mode='approx',path='search-first',device_fallback='cpu',candidates=3000")
        assert result['execution_plan']['actual_device']=='cpu'
        assert result['rows']==c.execute(SQL)['rows']
        assert result['execution_plan']['rounds'][0]['nprobe']==1


def test_explain_does_not_advertise_forbidden_gpu_to_cpu_exact_fallback(tmp_path):
    with servers(tmp_path) as (address,_),SessionClient(address) as c:
        c.open_database('demo',True);save_dataset(generate_dataset(1024,3),tmp_path/'imports/a.npz');c.import_dataset('a.npz','items');c.build_index('items',nlist=1,device='gpu')
        sql=SQL+GPU+",quality_profile_id='missing'"
        explained=c.execute('EXPLAIN '+sql)['execution_plan']
        assert explained['planned_fallback']=='error'
        with pytest.raises(ValueError):c.execute(sql)


def test_gpu_reuse_verifies_native_replica_when_liveness_metadata_is_stale(tmp_path,monkeypatch):
    import os,signal,time
    from vecstor.index.gpu_worker import GpuWorker
    with servers(tmp_path) as (address,_),SessionClient(address) as c:
        c.open_database('demo',True);save_dataset(generate_dataset(1024,3),tmp_path/'imports/a.npz');c.import_dataset('a.npz','items')
        first=c.build_index('items',nlist=1,device='gpu');os.kill(first['worker_pid'],signal.SIGKILL)
        monkeypatch.setattr(GpuWorker,'generation',property(lambda w:w._generation))
        rebuilt=c.build_index('items',nlist=1,device='gpu')
        assert not rebuilt['reused'] and rebuilt['worker_pid']!=first['worker_pid']
        assert c.execute(SQL+GPU)['execution_plan']['actual_device']=='gpu'


def test_busy_reuse_does_not_reset_other_gpu_replicas(tmp_path,monkeypatch):
    from vecstor.index.gpu_worker import GpuWorker,GpuError
    with servers(tmp_path) as (address,_),SessionClient(address) as c:
        c.open_database('demo',True)
        save_dataset(generate_dataset(1024,3),tmp_path/'imports/a.npz')
        c.import_dataset('a.npz','items');c.import_dataset('a.npz','other')
        first=c.build_index('items',nlist=1,device='gpu')
        c.build_index('other',nlist=1,device='gpu')
        expected=c.execute(SQL+GPU)['rows']
        original=GpuWorker.verify_replica
        def busy(*args,**kwargs):raise GpuError('gpu_queue_full')
        monkeypatch.setattr(GpuWorker,'verify_replica',busy)
        with pytest.raises(ValueError,match='RESOURCE_EXHAUSTED.*gpu_queue_full'):
            c.build_index('items',nlist=1,device='gpu')
        monkeypatch.setattr(GpuWorker,'verify_replica',original)
        replicas=[i for i in c.list_indexes() if i['device']=='gpu']
        assert len(replicas)==2
        assert all(i['worker_generation']==first['worker_generation'] and i['worker_pid']==first['worker_pid'] for i in replicas)
        assert c.execute(SQL+GPU)['rows']==expected
        assert c.execute(SQL.replace('FROM items','FROM other')+GPU)['rows']==expected
