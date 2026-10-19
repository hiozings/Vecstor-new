from types import SimpleNamespace

import grpc
import pytest

from vecstor.compute.lease_io import LeaseIO
from vecstor.rpc import sql_pb2 as pb
from vecstor.rpc.common import RemoteError


def opened(monkeypatch, ttl=2, epoch='epoch'):
    clock=[100.]
    monkeypatch.setattr('vecstor.compute.lease_io.time.monotonic',lambda:clock[0])
    io=LeaseIO(SimpleNamespace(BeginRead=None),'db','items',SimpleNamespace(is_active=lambda:True),105.,{})
    def call(method, request):
        assert request.ttl_seconds==5
        clock[0]=101.
        return pb.ReadLease(database_id='db',table='items',table_id='table',schema_version='schema',revision=1,
            dimension=4,token='token',storage_epoch=epoch,effective_ttl_seconds=ttl)
    io.call=call;io.open()
    return io,clock


def test_cache_deadline_uses_send_time_and_granted_ttl(monkeypatch):
    io,clock=opened(monkeypatch)
    scope=io.configure_cache_lease()
    assert scope.storage_epoch=='epoch' and io.deadline==102
    clock[0]=102
    with pytest.raises(RemoteError) as error:io.remaining()
    assert error.value.code==grpc.StatusCode.DEADLINE_EXCEEDED


@pytest.mark.parametrize('ttl',[0.,-1.,float('nan'),float('inf'),6.])
def test_invalid_granted_ttl_is_rejected(monkeypatch,ttl):
    io,_=opened(monkeypatch,ttl)
    with pytest.raises(RemoteError) as error:io.configure_cache_lease()
    assert error.value.code==grpc.StatusCode.DATA_LOSS


def test_legacy_lease_works_without_cache_but_cannot_enable_cache(monkeypatch):
    io,_=opened(monkeypatch,0.,'')
    assert io.remaining()==4
    with pytest.raises(RemoteError) as error:io.configure_cache_lease()
    assert error.value.code==grpc.StatusCode.FAILED_PRECONDITION


def test_projected_zero_fields_have_presence_and_legacy_wire_unchanged():
    row=pb.ProjectedRow(id=0,value=0.)
    restored=pb.ProjectedRow.FromString(row.SerializeToString())
    assert restored.HasField('id') and restored.HasField('value')
    assert not restored.HasField('timestamp') and not restored.HasField('category')
    old=pb.ReadLease(token='t',revision=1,dimension=4)
    assert old.SerializeToString()==b'\x0a\x01t\x20\x01\x30\x04'
