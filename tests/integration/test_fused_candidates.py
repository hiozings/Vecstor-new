from contextlib import ExitStack
import numpy as np
import pytest
import grpc
from vecstor.data import Dataset
from vecstor.datasets import save_dataset
from vecstor.storage.sql_service import create_catalog_server
from vecstor.rpc import sql_pb2 as pb,sql_pb2_grpc as rpc
from vecstor.rpc.common import OPTIONS


def test_fused_rpc_preserves_lease_order_missing_id_and_scan_capacity(tmp_path):
    ids=np.array([2**60+3,2**60+1,2**60+2],np.int64)
    data=Dataset(ids,['a','b','a'],[0,1,2],[3.,1.,2.],np.array([[3,0],[1,0],[2,0]],np.float32))
    save_dataset(data,tmp_path/'imports/a.npz')
    server,port,backend=create_catalog_server(tmp_path,'127.0.0.1:0');server.start()
    try:
        db=backend.catalog.open_database('demo',True)['id'];backend.catalog.import_dataset(db,'a.npz','items','import')
        with grpc.insecure_channel(f'127.0.0.1:{port}',options=OPTIONS) as channel:
            stub=rpc.SQLStorageStub(channel);lease=stub.BeginRead(pb.BeginReadRequest(database_id=db,table='items',ttl_seconds=20))
            query="SELECT id FROM items WHERE value>=2 ORDER BY vector_distance(vector,[0,0]) LIMIT 10"
            req=pb.CandidateRequest(database_id=db,token=lease.token,sql=query,ids=[int(x) for x in ids])
            result=stub.FilterCandidateVectors(req)
            assert list(result.checked_ids)==ids.tolist() and result.checked_count==3
            assert [r.id for r in result.rows]==[int(ids[0]),int(ids[2])]
            assert result.revision==lease.revision and result.dimension==2
            assert np.array_equal(np.frombuffer(result.vectors_f32_le,dtype='<f4').reshape(2,2),[[3,0],[2,0]])
            req.ids.append(17)
            with pytest.raises(grpc.RpcError) as error:stub.FilterCandidateVectors(req)
            assert error.value.code()==grpc.StatusCode.INVALID_ARGUMENT
            del req.ids[-1]
            for _ in range(4):assert backend._stream_slots.acquire(False)
            try:
                with pytest.raises(grpc.RpcError) as error:stub.FilterCandidateVectors(req)
                assert error.value.code()==grpc.StatusCode.RESOURCE_EXHAUSTED
                assert stub.FetchVectors(pb.FetchRequest(database_id=db,token=lease.token,ids=[int(ids[0])])).dimension==2
            finally:
                for _ in range(4):backend._stream_slots.release()
    finally:server.stop(0).wait();backend.close()


def test_fused_rpc_returns_empty_vectors_and_enforces_transfer_limit(tmp_path):
    data=Dataset(np.arange(300,dtype=np.int64),['x']*300,np.zeros(300,np.int64),np.arange(300,dtype=float),np.zeros((300,1024),np.float32))
    save_dataset(data,tmp_path/'imports/a.npz')
    server,port,backend=create_catalog_server(tmp_path,'127.0.0.1:0');server.start()
    try:
        db=backend.catalog.open_database('demo',True)['id'];backend.catalog.import_dataset(db,'a.npz','items','import')
        with grpc.insecure_channel(f'127.0.0.1:{port}',options=OPTIONS) as channel:
            stub=rpc.SQLStorageStub(channel);lease=stub.BeginRead(pb.BeginReadRequest(database_id=db,table='items',ttl_seconds=20))
            sql='SELECT id FROM items WHERE value<0 ORDER BY vector_distance(vector,['+','.join(['0']*1024)+']) LIMIT 10'
            r=stub.FilterCandidateVectors(pb.CandidateRequest(database_id=db,token=lease.token,sql=sql,ids=[0,1]))
            assert not r.rows and not r.vectors_f32_le and r.dimension==1024
            with pytest.raises(grpc.RpcError) as error:stub.FilterCandidateVectors(pb.CandidateRequest(database_id=db,token=lease.token,sql=sql,ids=list(range(300))))
            assert error.value.code()==grpc.StatusCode.RESOURCE_EXHAUSTED
    finally:server.stop(0).wait();backend.close()


def test_fused_read_keeps_old_snapshot_after_write(tmp_path):
    data=Dataset([1,2],['a','b'],[0,0],[1.,2.],np.array([[1,0],[2,0]],np.float32));save_dataset(data,tmp_path/'imports/a.npz')
    server,port,backend=create_catalog_server(tmp_path,'127.0.0.1:0');server.start()
    try:
        db=backend.catalog.open_database('demo',True)['id'];backend.catalog.import_dataset(db,'a.npz','items','import')
        with grpc.insecure_channel(f'127.0.0.1:{port}',options=OPTIONS) as channel:
            stub=rpc.SQLStorageStub(channel);old=stub.BeginRead(pb.BeginReadRequest(database_id=db,table='items',ttl_seconds=20))
            backend.catalog.execute(db,'UPDATE items SET vector=[9,9] WHERE id=1','write')
            new=stub.BeginRead(pb.BeginReadRequest(database_id=db,table='items',ttl_seconds=20))
            sql='SELECT id FROM items ORDER BY vector_distance(vector,[0,0]) LIMIT 10'
            a=stub.FilterCandidateVectors(pb.CandidateRequest(database_id=db,token=old.token,sql=sql,ids=[1]))
            b=stub.FilterCandidateVectors(pb.CandidateRequest(database_id=db,token=new.token,sql=sql,ids=[1]))
            assert a.revision==old.revision and b.revision>old.revision
            assert np.frombuffer(a.vectors_f32_le,dtype='<f4').tolist()==[1.,0.]
            assert np.frombuffer(b.vectors_f32_le,dtype='<f4').tolist()==[9.,9.]
    finally:server.stop(0).wait();backend.close()


def test_fused_large_metadata_uses_bounded_single_row_transport_fallback(tmp_path):
    import time
    from types import SimpleNamespace
    from vecstor.compute.lease_io import LeaseIO
    data=Dataset([1],['x'*(4*1024*1024-1024)],[0],[1.],np.zeros((1,512),np.float32));save_dataset(data,tmp_path/'imports/a.npz')
    server,port,backend=create_catalog_server(tmp_path,'127.0.0.1:0');server.start()
    try:
        db=backend.catalog.open_database('demo',True)['id'];backend.catalog.import_dataset(db,'a.npz','items','import')
        with grpc.insecure_channel(f'127.0.0.1:{port}',options=OPTIONS) as channel:
            stub=rpc.SQLStorageStub(channel)
            context=SimpleNamespace(is_active=lambda:True,add_callback=lambda fn:None)
            trace={};io=LeaseIO(stub,db,'items',context,time.monotonic()+20,trace);io.open()
            try:
                sql='SELECT id FROM items ORDER BY vector_distance(vector,['+','.join(['0']*512)+']) LIMIT 10'
                rows,vectors=io.filter_fetch(sql,[1])
                assert rows[0]['id']==1 and vectors.shape==(1,512)
                assert trace['counts']['fused_single_row_fallbacks']==1
            finally:io.close()
    finally:server.stop(0).wait();backend.close()
