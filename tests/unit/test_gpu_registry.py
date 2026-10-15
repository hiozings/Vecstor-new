from types import SimpleNamespace
import pytest
from vecstor.index.types import IndexKey


def test_gpu_registry_keeps_pinned_versions_and_invalidates_worker_generation():
    from vecstor.index.gpu_registry import GpuRegistry
    class Worker:
        generation=1
        def release(self,key):pass
    worker=Worker();registry=GpuRegistry(worker,max_bytes=100,max_entries=1)
    key=IndexKey('db','table',1,'schema',2)
    handle=SimpleNamespace(dimension=2,byte_size=60,nlist=1,row_count=10,max_abs=1,worker_generation=1,token='one')
    registry.publish(key,handle,'items')
    lease=SimpleNamespace(database_id='db',table_id='table',revision=1,schema_version='schema',dimension=2)
    with registry.acquire(lease) as entry:
        assert entry is not None
        with pytest.raises(ValueError,match='budget'):registry.reserve(60)
        assert registry.clear('db')['busy']==1
    worker.generation+=1
    with registry.acquire(lease) as entry:assert entry is None
    assert not registry.list('db')


def test_gpu_registry_reclaims_before_allocating_and_rejects_oversize():
    from vecstor.index.gpu_registry import GpuRegistry
    class Worker:
        generation=1
        def __init__(self):self.released=[]
        def release(self,key):self.released.append(key)
    worker=Worker();registry=GpuRegistry(worker,max_bytes=100,max_entries=2)
    handle=SimpleNamespace(dimension=2,byte_size=60,worker_generation=1,token='old')
    registry.publish(IndexKey('db','t',1,'s',2),handle,'items')
    registry.reserve(60)
    assert worker.released==['old'] and registry.total_bytes==0
    with pytest.raises(ValueError):registry.reserve(101)
