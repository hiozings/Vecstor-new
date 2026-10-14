from dataclasses import replace
from types import SimpleNamespace
from threading import Event
from concurrent.futures import ThreadPoolExecutor

import grpc
import numpy as np
import pytest

from vecstor.compute import index_manager as module
from vecstor.index.types import IndexKey
from vecstor.rpc import sql_pb2 as pb
from vecstor.rpc.common import RemoteError


@pytest.fixture
def harness(monkeypatch):
    state=SimpleNamespace(n=1024,revision=1,closed=0,exports=0,builds=[],active=True)
    class IO:
        def __init__(self,stub,db,table,context,deadline,trace):
            trace['counts']={'rpc_calls':1,'rpc_payload_bytes':1}
        def open(self):
            return pb.ReadLease(database_id='db',table_id='table',revision=state.revision,
                               schema_version='schema',dimension=1,row_count=state.n)
        def remaining(self):
            if not state.active: raise RemoteError(grpc.StatusCode.CANCELLED,'cancelled')
            return 10
        def close(self): state.closed+=1
        def snapshot(self):
            state.exports+=1
            yield np.arange(state.n,dtype=np.int64),np.zeros((state.n,1),dtype=np.float32)
    def worker(ids,vectors,config,*args,**kw):
        state.builds.append(config.nlist)
        return SimpleNamespace(row_count=len(ids),dimension=1,nlist=config.nlist,byte_size=10000,max_abs=0),{}
    monkeypatch.setattr(module,'LeaseIO',IO)
    monkeypatch.setattr(module,'build_in_worker',worker)
    state.context=SimpleNamespace(time_remaining=lambda:45,is_active=lambda:state.active)
    state.request=lambda nlist=1:pb.BuildIndexRequest(database_id='db',table='items',nlist=nlist)
    state.manager=module.IndexManager(None)
    return state


def test_reactivate_existing_full_key_without_export_or_training(harness):
    s=harness
    for nlist in (1,2): assert not s.manager.build(s.request(nlist),s.context)['reused']
    restored=s.manager.build(s.request(1),s.context)
    assert restored['reused'] and restored['nlist']==1
    assert s.builds==[1,2] and s.exports==2 and s.closed==3
    with s.manager.registry.acquire(pb.ReadLease(database_id='db',table_id='table',revision=1,schema_version='schema',dimension=1)) as active:
        assert active.handle.nlist==1


@pytest.mark.parametrize('n',[0,100])
@pytest.mark.parametrize('config',[{'nlist':0},{'seed':2**63},{'nlist':101}])
def test_small_and_empty_tables_validate_explicit_config(harness,n,config):
    s=harness;s.n=n
    request=pb.BuildIndexRequest(database_id='db',table='items',**config)
    with pytest.raises(ValueError):s.manager.build(request,s.context)
    assert s.closed==1 and not s.builds


@pytest.mark.parametrize('n',[0,100])
def test_small_table_default_remains_exact_only(harness,n):
    s=harness;s.n=n
    assert s.manager.build(pb.BuildIndexRequest(database_id='db',table='items'),s.context)['state']=='exact_only'
    assert s.closed==1


def test_budget_reclaims_old_unpinned_revision_before_build(harness):
    s=harness;s.manager=module.IndexManager(None,max_bytes=30000)
    s.manager.build(s.request(),s.context)
    s.revision=2
    assert s.manager.build(s.request(),s.context)['revision']==2
    assert [entry['revision'] for entry in s.manager.registry.list()]==[2]
    assert s.closed==2


def test_build_budget_preserves_pinned_old_revision(harness):
    s=harness;s.manager=module.IndexManager(None,max_bytes=30000)
    s.manager.build(s.request(),s.context)
    lease=pb.ReadLease(database_id='db',table_id='table',revision=1,schema_version='schema',dimension=1)
    s.revision=2
    with s.manager.registry.acquire(lease):
        with pytest.raises(RemoteError) as error:s.manager.build(s.request(),s.context)
        assert error.value.code is grpc.StatusCode.RESOURCE_EXHAUSTED
        assert len(s.manager.registry.list())==1
    assert s.closed==2 and s.exports==1


def test_cancel_while_waiting_for_publication_never_publishes_and_releases_lease(harness,monkeypatch):
    s=harness
    worker_ready,allow_worker,publishing=Event(),Event(),Event()
    worker=module.build_in_worker
    def paused_worker(*args,**kwargs):
        result=worker(*args,**kwargs)
        worker_ready.set()
        assert allow_worker.wait(5)
        return result
    monkeypatch.setattr(module,'build_in_worker',paused_worker)
    publish=s.manager.registry.publish
    def notify_publish(*args,**kwargs):
        publishing.set()
        return publish(*args,**kwargs)
    monkeypatch.setattr(s.manager.registry,'publish',notify_publish)
    with ThreadPoolExecutor(max_workers=1) as pool:
        future=pool.submit(s.manager.build,s.request(),s.context)
        assert worker_ready.wait(5)
        with s.manager.registry._lock:
            allow_worker.set()
            assert publishing.wait(5)
            s.active=False
        with pytest.raises(RemoteError) as error:future.result(timeout=5)
        assert error.value.code is grpc.StatusCode.CANCELLED
    assert s.manager.registry.list()==[] and s.closed==1
    assert s.manager.build_slot.acquire(blocking=False)
    s.manager.build_slot.release()
