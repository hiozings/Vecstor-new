from threading import Condition, Event

import grpc
import numpy as np
import pytest

from test_p3_storage import running, setup
from vecstor.rpc import sql_pb2 as pb
from vecstor.rpc.common import MAX_MESSAGE_BYTES

SQL='SELECT id FROM items WHERE id <> 1 ORDER BY vector_distance(vector,[0,0,0,0]) LIMIT 10'


def begin(stub,db):return stub.BeginRead(pb.BeginReadRequest(database_id=db,table='items',ttl_seconds=30),timeout=5)


def test_selective_filter_preserves_checked_ids_and_projects_columns(tmp_path):
    with running(tmp_path) as (stub,catalog):
        db=setup(catalog,tmp_path);lease=begin(stub,db)
        reply=stub.ReadProjectedCandidates(pb.ProjectedCandidateRequest(database_id=db,token=lease.token,sql=SQL,
            checked_ids=[3,1,2],fields=['id'],vector_ids=[1,2]),timeout=5)
        assert list(reply.checked_ids)==[3,1,2] and [r.id for r in reply.rows]==[3,2]
        assert all(not r.HasField('category') for r in reply.rows)
        assert list(reply.vector_ids)==[2] and len(reply.vectors_f32_le)==16
        old=stub.FetchVectors(pb.FetchRequest(database_id=db,token=lease.token,ids=[2]),timeout=5)
        assert old.vectors_f32_le==reply.vectors_f32_le
        database=catalog.database(db)
        assert len(database._candidate_plans)==1


@pytest.mark.parametrize('ids,vector_ids,fields', [([999],[],['id']),([1,2],[2,1],['id']),([1],[2],['id']),([1],[],['value']),([1,1],[],['id'])])
def test_bad_requests_fail_even_when_no_vectors_requested(tmp_path,ids,vector_ids,fields):
    with running(tmp_path) as (stub,catalog):
        db=setup(catalog,tmp_path);lease=begin(stub,db)
        with pytest.raises(grpc.RpcError) as error:
            stub.ReadProjectedCandidates(pb.ProjectedCandidateRequest(database_id=db,token=lease.token,sql=SQL,
                checked_ids=ids,vector_ids=vector_ids,fields=fields),timeout=5)
        assert error.value.code()==grpc.StatusCode.INVALID_ARGUMENT


def test_scan_splits_large_rows_and_rejects_single_oversized_row(tmp_path):
    with running(tmp_path) as (stub,catalog):
        db=setup(catalog,tmp_path);database=catalog.database(db)
        def wide(size):
            database.connection.execute('UPDATE items SET category=? WHERE id<2',['x'*size])
        database.pool.submit(wide,2*1024**2).result()
        lease=begin(stub,db)
        sql='SELECT id,category FROM items WHERE id<2 ORDER BY vector_distance(vector,[0,0,0,0]) LIMIT 2'
        request=pb.ProjectedScanRequest(database_id=db,token=lease.token,sql=sql,batch_rows=1024,fields=['id','category'])
        batches=list(stub.ScanProjectedMetadata(request,timeout=5))
        assert len(batches)==2 and [b.offset for b in batches]==[0,1]
        assert [b.complete for b in batches]==[False,True]
        assert all(b.ByteSize()<=MAX_MESSAGE_BYTES for b in batches)
        stub.EndRead(pb.LeaseRequest(database_id=db,token=lease.token),timeout=5)
        database.pool.submit(wide,5*1024**2).result()
        request.token=begin(stub,db).token
        with pytest.raises(grpc.RpcError) as error:list(stub.ScanProjectedMetadata(request,timeout=5))
        assert error.value.code()==grpc.StatusCode.RESOURCE_EXHAUSTED


def test_projected_candidates_share_slots_but_vectors_remain_available(tmp_path):
    with running(tmp_path) as (stub,catalog):
        db=setup(catalog,tmp_path);lease=begin(stub,db)
        original=catalog.read_projected_candidates;condition=Condition();release=Event();entered=[]
        def slow(*a,**k):
            with condition:entered.append(1);condition.notify_all()
            assert release.wait(5)
            return original(*a,**k)
        catalog.read_projected_candidates=slow
        request=pb.ProjectedCandidateRequest(database_id=db,token=lease.token,sql=SQL,checked_ids=[2],fields=['id'])
        calls=[stub.ReadProjectedCandidates.future(request,timeout=7) for _ in range(4)]
        try:
            with condition:assert condition.wait_for(lambda:len(entered)==4,timeout=3)
            with pytest.raises(grpc.RpcError) as error:stub.ReadProjectedCandidates(request,timeout=1)
            assert error.value.code()==grpc.StatusCode.RESOURCE_EXHAUSTED
            assert list(stub.FetchVectors(pb.FetchRequest(database_id=db,token=lease.token,ids=[2]),timeout=2).ids)==[2]
        finally:
            release.set()
            for call in calls:call.result(timeout=5)


def test_selective_vector_limit_and_metadata_only_wide_candidates(tmp_path):
    from vecstor.datasets import generate_dataset,save_dataset
    with running(tmp_path) as (stub,catalog):
        db=catalog.open_database('demo',True)['id']
        save_dataset(generate_dataset(300,1024,seed=8),tmp_path/'imports/wide.npz')
        catalog.import_dataset(db,'wide.npz','items','wide')
        lease=begin(stub,db)
        sql='SELECT id FROM items ORDER BY vector_distance(vector,['+','.join(['0']*1024)+']) LIMIT 1'
        request=pb.ProjectedCandidateRequest(database_id=db,token=lease.token,sql=sql,checked_ids=list(range(257)),fields=['id'],vector_ids=list(range(257)))
        with pytest.raises(grpc.RpcError) as error:stub.ReadProjectedCandidates(request,timeout=5)
        assert error.value.code()==grpc.StatusCode.INVALID_ARGUMENT
        del request.vector_ids[:]
        reply=stub.ReadProjectedCandidates(request,timeout=5)
        assert len(reply.rows)==257 and not reply.vectors_f32_le


def test_oversized_candidate_response_is_specific_resource_error(tmp_path):
    with running(tmp_path) as (stub,catalog):
        db=setup(catalog,tmp_path);database=catalog.database(db)
        database.pool.submit(lambda:database.connection.execute('UPDATE items SET category=? WHERE id<2',['x'*(2*1024**2)])).result()
        lease=begin(stub,db)
        sql='SELECT category FROM items ORDER BY vector_distance(vector,[0,0,0,0]) LIMIT 2'
        request=pb.ProjectedCandidateRequest(database_id=db,token=lease.token,sql=sql,checked_ids=[0,1],fields=['id','category'])
        with pytest.raises(grpc.RpcError) as error:stub.ReadProjectedCandidates(request,timeout=5)
        assert error.value.code()==grpc.StatusCode.RESOURCE_EXHAUSTED
        assert error.value.details()=='projected_response_too_large'
