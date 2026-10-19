from pathlib import Path
import runpy
import time

import grpc
import pytest

from test_p3_storage import running,setup
from vecstor.storage.sql_service import create_catalog_server
from vecstor.rpc import sql_pb2 as pb,sql_pb2_grpc as rpc

ROOT=Path(__file__).resolve().parents[2]


def test_proxy_preserves_read_responses_and_delay(tmp_path):
    start=runpy.run_path(str(ROOT/'tests/support/delayed_storage.py'))['start_proxy']
    storage,port,backend=create_catalog_server(tmp_path,'127.0.0.1:0');storage.start()
    proxy=None
    try:
        db=setup(backend.catalog,tmp_path)
        proxy=start(f'127.0.0.1:{port}',delay_ms=5)
        with grpc.insecure_channel(proxy.address) as channel:
            stub=rpc.SQLStorageStub(channel)
            begin=time.perf_counter()
            lease=stub.BeginRead(pb.BeginReadRequest(database_id=db,table='items',ttl_seconds=5),timeout=3)
            assert time.perf_counter()-begin>=.009
            vectors=stub.FetchVectors(pb.FetchRequest(database_id=db,token=lease.token,ids=[1,2]),timeout=3)
            assert list(vectors.ids)==[1,2]
            sql='SELECT id FROM items ORDER BY vector_distance(vector,[0,0,0,0]) LIMIT 1'
            frames=list(stub.ScanProjectedMetadata(pb.ProjectedScanRequest(database_id=db,token=lease.token,sql=sql,fields=['id'],batch_rows=100),timeout=3))
            assert len(frames)==3 and frames[-1].complete
            with pytest.raises(grpc.RpcError) as error:
                stub.FetchVectors(pb.FetchRequest(database_id=db,token='bad',ids=[1]),timeout=3)
            assert error.value.code()==grpc.StatusCode.FAILED_PRECONDITION
            with pytest.raises(grpc.RpcError) as error:
                stub.BeginRead(pb.BeginReadRequest(database_id=db,table='items',ttl_seconds=5),timeout=.001)
            assert error.value.code()==grpc.StatusCode.DEADLINE_EXCEEDED
    finally:
        if proxy:proxy.close()
        storage.stop(0).wait();backend.close()
