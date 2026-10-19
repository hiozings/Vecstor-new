from types import SimpleNamespace
import time

import grpc
import numpy as np
import pytest

from vecstor.compute.lease_io import LeaseIO
from vecstor.rpc import sql_pb2 as pb
from vecstor.rpc.common import RemoteError
from vecstor.rpc.projected_reads import decode_projected_candidates, validate_projected_row

LEASE=pb.ReadLease(token='t',revision=1,dimension=2,row_count=3)


def response():
    return pb.ProjectedCandidateResult(token='t',revision=1,checked_ids=[3,1,2],fields=['id'],
        rows=[pb.ProjectedRow(id=3),pb.ProjectedRow(id=2)],dimension=2,vector_ids=[2],
        vectors_f32_le=np.array([[0.,-0.]],dtype='<f4').tobytes())


def test_selective_vectors_have_exact_intersection_coverage():
    rows,ids,vectors=decode_projected_candidates(response(),LEASE,[3,1,2],('id',),[1,2])
    assert rows==[{'id':3},{'id':2}] and ids==[2]
    assert vectors.tobytes()==np.array([[0.,-0.]],dtype='<f4').tobytes()
    assert validate_projected_row(pb.ProjectedRow(id=0,value=0.),('id','value'))=={'id':0,'value':0.}


@pytest.mark.parametrize('change',['token','revision','checked','fields','extra','missing','order','vectors','dimension','payload','nan'])
def test_corrupt_projected_candidates_rejected(change):
    reply=response()
    if change=='token':reply.token='other'
    elif change=='revision':reply.revision=2
    elif change=='checked':reply.checked_ids.reverse()
    elif change=='fields':reply.fields.append('value')
    elif change=='extra':reply.rows[0].category='x'
    elif change=='missing':reply.rows[0].ClearField('id')
    elif change=='order':reply.rows.reverse()
    elif change=='vectors':reply.vector_ids[:]=[1]
    elif change=='dimension':reply.dimension=3
    elif change=='payload':reply.vectors_f32_le=b'bad'
    elif change=='nan':reply.vectors_f32_le=np.full((1,2),np.nan,dtype='<f4').tobytes()
    with pytest.raises(RemoteError) as error:decode_projected_candidates(reply,LEASE,[3,1,2],('id',),[1,2])
    assert error.value.code==grpc.StatusCode.DATA_LOSS


def io_for(batches):
    io=LeaseIO(SimpleNamespace(ScanProjectedMetadata=None),'db','items',SimpleNamespace(is_active=lambda:True),time.monotonic()+10,{})
    io.lease=LEASE;io.batch_size=2;io.stream=lambda *a:iter(batches)
    return io


def frame(ids,offset,total=3,complete=False):
    return pb.ProjectedMetadataBatch(token='t',revision=1,total_rows=total,offset=offset,fields=['id'],
                                    rows=[pb.ProjectedRow(id=i) for i in ids],complete=complete)


def test_scan_final_frame_and_empty_result():
    assert list(io_for([frame([0,1],0),frame([2],2,complete=True)]).scan_projected('sql',('id',)))==[[{'id':0},{'id':1}],[{'id':2}]]
    assert list(io_for([frame([],0,total=0,complete=True)]).scan_projected('sql',('id',)))==[[]]


@pytest.mark.parametrize('batches',[[],[frame([0,1],0)],[frame([0,1],0),frame([2],2)],
    [frame([0,1],0,complete=True),frame([2],2,complete=True)],
    [frame([0,1],0),frame([1],2,complete=True)],
    [frame([],0,total=0,complete=True),frame([],0,total=0,complete=True)],
    [frame([0,1],0),frame([2],1,complete=True)]])
def test_incomplete_or_invalid_scan_rejected(batches):
    with pytest.raises(RemoteError):list(io_for(batches).scan_projected('sql',('id',)))
