"""Successful ordinary SQL and busy-device admission must not cause false failure/reset."""
from contextlib import nullcontext
from threading import BoundedSemaphore
from types import SimpleNamespace

import grpc
import pytest

from vecstor.compute.index_manager import IndexManager
from vecstor.index.gpu_worker import GpuError
from vecstor.query.search_options import SearchOptions
from vecstor.rpc.common import RemoteError
from vecstor.rpc.sql_client import SessionClient
from vecstor.rpc.sql_codec import encode_result


@pytest.mark.parametrize('device,data_path',[('cpu','separate'),('gpu','default'),('gpu','fused')])
@pytest.mark.parametrize('sql,is_write',[('SELECT id FROM ordinary',False),('INSERT INTO ordinary VALUES (1)',True)])
def test_ordinary_sql_ignores_vector_session_acknowledgments(monkeypatch,device,data_path,sql,is_write):
    with SessionClient('127.0.0.1:1') as client:
        client.search_options=SearchOptions(mode='approx',path='search-first',device=device,candidate_data_path=data_path)
        requests=[]
        class Call:
            def __iter__(self):
                return iter(encode_result({'columns':[{'name':'id','type':'BIGINT'}],
                                           'rows':[] if is_write else [[1]],'affected_rows':1 if is_write else 0}))
            def cancel(self):pass
        def execute(request,**kwargs):requests.append(request);return Call()
        monkeypatch.setattr(client.stub,'ExecuteSQL',execute)
        result=client.execute(sql)
        assert result['affected_rows']==(1 if is_write else 0)
        assert result['rows']==([] if is_write else [[1]])
        assert not requests[0].HasField('search_options')


@pytest.mark.parametrize('reason',['gpu_queue_full','gpu_worker_closed','gpu_memory_budget'])
def test_reuse_admission_failure_preserves_healthy_worker(monkeypatch,reason):
    class Worker:
        generation=7
        def verify_replica(self,*args,**kwargs):raise GpuError(reason)
        def reset(self):self.generation+=1
    worker=Worker()
    resident=SimpleNamespace(handle=SimpleNamespace(token='native'))
    registry=SimpleNamespace(reactivate=lambda *a,**k:resident)
    cpu=SimpleNamespace(key=SimpleNamespace(build_fingerprint='fingerprint'))
    manager=IndexManager.__new__(IndexManager)
    manager.gpu_build_slot=BoundedSemaphore(1)
    manager.stub=None;manager.gpu_worker=worker;manager.gpu_registry=registry
    manager.registry=SimpleNamespace(acquire=lambda lease:nullcontext(cpu))
    manager._build_cpu=lambda *args:{'state':'ready','build_fingerprint':'fingerprint'}
    io=SimpleNamespace(open=lambda:object(),remaining=lambda:10,close=lambda:None)
    monkeypatch.setattr('vecstor.compute.index_manager.LeaseIO',lambda *args:io)
    context=SimpleNamespace(time_remaining=lambda:30,is_active=lambda:True)
    request=SimpleNamespace(device='gpu',gpu_id=0,database_id='db',table='items')
    with pytest.raises(RemoteError,match=reason):manager.build(request,context)
    assert worker.generation==7
    assert registry.reactivate() is resident
    assert manager.gpu_build_slot.acquire(blocking=False)
