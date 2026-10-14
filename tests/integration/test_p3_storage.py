from contextlib import contextmanager

import grpc
import numpy as np
import pytest

from vecstor.datasets import generate_dataset, save_dataset
from vecstor.rpc import sql_pb2 as pb, sql_pb2_grpc as rpc
from vecstor.storage.sql_service import create_catalog_server


@contextmanager
def running(root):
    server,port,backend=create_catalog_server(root,"127.0.0.1:0")
    server.start()
    try:
        with grpc.insecure_channel(f"127.0.0.1:{port}") as channel:
            yield rpc.SQLStorageStub(channel),backend.catalog
    finally:
        server.stop(0).wait();backend.close()


def setup(catalog,root):
    db=catalog.open_database("demo",True)["id"]
    save_dataset(generate_dataset(300,4,seed=3),root/"imports/sample.npz")
    catalog.import_dataset(db,"sample.npz","items","seed")
    return db


def test_snapshot_export_and_candidate_filter_same_old_lease(tmp_path):
    with running(tmp_path) as (stub,catalog):
        db=setup(catalog,tmp_path)
        lease=stub.BeginRead(pb.BeginReadRequest(database_id=db,table="items",ttl_seconds=10),timeout=3)
        assert lease.HasField("row_count") and lease.row_count==300
        catalog.execute(db,"UPDATE items SET value=9999, vector=[0,0,0,0] WHERE id=1","change")
        chunks=list(stub.StreamVectorSnapshot(pb.SnapshotRequest(database_id=db,token=lease.token,batch_rows=37),timeout=3))
        ids=[];vectors=[]
        for chunk in chunks:
            assert chunk.token==lease.token and chunk.revision==lease.revision
            assert chunk.table_id==lease.table_id and chunk.schema_version==lease.schema_version
            assert chunk.total_rows==300 and chunk.offset==len(ids)
            ids.extend(chunk.ids)
            vectors.extend(np.frombuffer(chunk.vectors_f32_le,dtype="<f4").reshape(-1,4))
        assert ids==list(range(300))
        original=generate_dataset(300,4,seed=3)
        np.testing.assert_array_equal(vectors,original.vectors)
        requested=[7,1,9,3]
        sql="SELECT id FROM items WHERE value < 150 ORDER BY vector_distance(vector,[0,0,0,0]) LIMIT 10"
        reply=stub.FilterCandidates(pb.CandidateRequest(database_id=db,token=lease.token,sql=sql,ids=requested),timeout=3)
        assert reply.checked_count==4 and list(reply.checked_ids)==requested
        assert [r.id for r in reply.rows]==[i for i in requested if original.values[i]<150]


@pytest.mark.parametrize("ids",[[1,1],[-1],[999]])
def test_bad_candidate_ids_are_not_silently_filtered(tmp_path,ids):
    with running(tmp_path) as (stub,catalog):
        db=setup(catalog,tmp_path)
        lease=stub.BeginRead(pb.BeginReadRequest(database_id=db,table="items",ttl_seconds=10),timeout=3)
        with pytest.raises(grpc.RpcError) as error:
            stub.FilterCandidates(pb.CandidateRequest(database_id=db,token=lease.token,ids=ids,
                sql="SELECT id FROM items ORDER BY vector_distance(vector,[0,0,0,0]) LIMIT 1"),timeout=3)
        assert error.value.code()==grpc.StatusCode.INVALID_ARGUMENT


def test_empty_export_and_wrong_table(tmp_path):
    with running(tmp_path) as (stub,catalog):
        db=setup(catalog,tmp_path)
        catalog.execute(db,"DELETE FROM items","clear")
        lease=stub.BeginRead(pb.BeginReadRequest(database_id=db,table="items",ttl_seconds=10),timeout=3)
        chunks=list(stub.StreamVectorSnapshot(pb.SnapshotRequest(database_id=db,token=lease.token),timeout=3))
        assert len(chunks)==1 and chunks[0].total_rows==0 and not chunks[0].ids
        with pytest.raises(grpc.RpcError):
            stub.FilterCandidates(pb.CandidateRequest(database_id=db,token=lease.token,ids=[],
                sql="SELECT id FROM other ORDER BY vector_distance(vector,[0,0,0,0]) LIMIT 1"),timeout=3)


def test_candidate_filters_share_admission_capacity(tmp_path):
    from threading import Condition, Event
    with running(tmp_path) as (stub,catalog):
        db=setup(catalog,tmp_path)
        lease=stub.BeginRead(pb.BeginReadRequest(database_id=db,table="items",ttl_seconds=10),timeout=3)
        original=catalog.filter_candidates
        condition=Condition();release=Event();entered=[]
        def slow(*args,**kwargs):
            with condition:
                entered.append(1);condition.notify_all()
            release.wait(5)
            return original(*args,**kwargs)
        catalog.filter_candidates=slow
        request=pb.CandidateRequest(database_id=db,token=lease.token,ids=[1],
            sql="SELECT id FROM items ORDER BY vector_distance(vector,[0,0,0,0]) LIMIT 1")
        calls=[stub.FilterCandidates.future(request,timeout=7) for _ in range(4)]
        try:
            with condition: assert condition.wait_for(lambda:len(entered)==4,timeout=3)
            with pytest.raises(grpc.RpcError) as error:
                stub.FilterCandidates(request,timeout=.3)
            assert error.value.code()==grpc.StatusCode.RESOURCE_EXHAUSTED
            assert stub.FetchVectors(pb.FetchRequest(database_id=db,token=lease.token,ids=[1]),timeout=2).ids==[1]
        finally:
            release.set()
            for call in calls:call.result(timeout=5)
