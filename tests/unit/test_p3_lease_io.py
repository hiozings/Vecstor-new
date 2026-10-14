from types import SimpleNamespace
import time

import grpc
import numpy as np
import pytest

from vecstor.compute.lease_io import LeaseIO
from vecstor.rpc import sql_pb2 as pb
from vecstor.rpc.common import RemoteError


def exporter(batches):
    io=LeaseIO(SimpleNamespace(StreamVectorSnapshot=None),'db','items',
               SimpleNamespace(is_active=lambda:True),time.monotonic()+10,{})
    io.lease=pb.ReadLease(token='token',database_id='db',table_id='table',table='items',
                        revision=3,schema_version='schema',dimension=2,row_count=3)
    io.batch_size=2
    io.stream=lambda *args:iter(batches)
    return io


def batch(ids,offset=0,**changes):
    fields=dict(token='token',revision=3,table_id='table',schema_version='schema',
                dimension=2,total_rows=3,offset=offset,ids=ids,
                vectors_f32_le=np.zeros((len(ids),2),dtype='<f4').tobytes())
    fields.update(changes)
    return pb.SnapshotBatch(**fields)


def test_complete_snapshot_keeps_int64_ids_and_order():
    ids=[2**60+1,2**60+3,2**60+9]
    result=list(exporter([batch(ids[:2]),batch(ids[2:],2)]).snapshot())
    assert [int(i) for labels,_ in result for i in labels]==ids


@pytest.mark.parametrize('batches',[
    [], [batch([1,2])], [batch([1,2]),batch([3],1)],
    [batch([1,2]),batch([2],2)], [batch([-1,2]),batch([3],2)],
    [batch([1,2],revision=4),batch([3],2)],
    [batch([1,2],table_id='other'),batch([3],2)],
    [batch([1,2],schema_version='other'),batch([3],2)],
    [batch([1,2],vectors_f32_le=b'bad'),batch([3],2)],
    [batch([1,2],vectors_f32_le=np.full((2,2),np.nan,dtype='<f4').tobytes()),batch([3],2)],
    [batch([1,2,3])],
])
def test_corrupt_or_truncated_snapshot_is_data_loss(batches):
    with pytest.raises(RemoteError) as error:list(exporter(batches).snapshot())
    assert error.value.code==grpc.StatusCode.DATA_LOSS


def test_expired_or_cancelled_query_cannot_start_fallback_rpc():
    io=exporter([])
    io.deadline=time.monotonic()-1
    with pytest.raises(RemoteError) as error:io.remaining()
    assert error.value.code==grpc.StatusCode.DEADLINE_EXCEEDED
    io.deadline=time.monotonic()+10
    io.context=SimpleNamespace(is_active=lambda:False)
    with pytest.raises(RemoteError) as error:io.remaining()
    assert error.value.code==grpc.StatusCode.CANCELLED
