"""RPC handler contracts backed by real DuckDB, without requiring sockets."""
import json
import time

import grpc
import numpy as np
import pytest

from vecstor.rpc import sql_pb2 as pb
from vecstor.rpc.sql_codec import decode_frame

DDL = 'CREATE TABLE items (id BIGINT PRIMARY KEY, category VARCHAR NOT NULL, timestamp BIGINT NOT NULL, value DOUBLE NOT NULL, vector FLOAT[2] NOT NULL)'
SEARCH = 'SELECT id FROM items ORDER BY vector_distance(vector,[0,0]) LIMIT 10'


class Aborted(Exception):
    def __init__(self, code): self.code = code


class Context:
    def is_active(self): return True
    def time_remaining(self): return 15
    def abort(self, code, details): raise Aborted(code)


@pytest.fixture
def service(tmp_path):
    from vecstor.storage.sql_service import SQLStorageService
    value = SQLStorageService(tmp_path)
    try: yield value
    finally: value.close()


def setup_database(service):
    db = json.loads(service.Control(pb.ControlRequest(action='open_database', name='demo', create=True), Context()).json_result)['id']
    for sql, op in [(DDL,'create'), ("INSERT INTO items VALUES (7,'论文',0,3,[1,0]),(2,'book',0,1,[0,1])", 'insert')]:
        events = [decode_frame(x) for x in service.ExecuteSQL(pb.SQLRequest(database_id=db,sql=sql,operation_id=op), Context())]
        assert events[-1]['kind'] == 'complete'
    return db


def test_storage_lease_snapshot_and_typed_batches(service):
    db = setup_database(service)
    lease = service.BeginRead(pb.BeginReadRequest(database_id=db,table='items'), Context())
    assert lease.dimension == 2 and lease.revision == 2
    list(service.ExecuteSQL(pb.SQLRequest(database_id=db,sql='UPDATE items SET vector=[9,9] WHERE id=7',operation_id='update'), Context()))
    batches = list(service.ScanMetadata(pb.ScanRequest(database_id=db,token=lease.token,sql=SEARCH,batch_rows=1), Context()))
    assert [x.offset for x in batches] == [0,1]
    assert all(x.total_rows == 2 and x.revision == 2 for x in batches)
    vectors = service.FetchVectors(pb.FetchRequest(database_id=db,token=lease.token,ids=[7,2]), Context())
    assert vectors.revision == 2
    np.testing.assert_array_equal(np.frombuffer(vectors.vectors_f32_le,dtype='<f4').reshape(2,2), [[1,0],[0,1]])
    service.EndRead(pb.LeaseRequest(database_id=db,token=lease.token), Context())
    with pytest.raises(Aborted) as error:
        service.FetchVectors(pb.FetchRequest(database_id=db,token=lease.token,ids=[7]), Context())
    assert error.value.code == grpc.StatusCode.FAILED_PRECONDITION


def test_invalid_control_and_sql(service):
    for request in [pb.ControlRequest(action='attach'), pb.ControlRequest(action='open_database',name='../x',create=True)]:
        with pytest.raises(Aborted) as error: service.Control(request, Context())
        assert error.value.code == grpc.StatusCode.INVALID_ARGUMENT
    with pytest.raises(Aborted) as error:
        list(service.ExecuteSQL(pb.SQLRequest(sql='SELECT 1'), Context()))
    assert error.value.code == grpc.StatusCode.INVALID_ARGUMENT


def test_expired_lease_and_invalid_ttl(service):
    db = setup_database(service)
    for ttl in [-1, float('nan'), float('inf')]:
        with pytest.raises(Aborted) as error:
            service.BeginRead(pb.BeginReadRequest(database_id=db,table='items',ttl_seconds=ttl), Context())
        assert error.value.code == grpc.StatusCode.INVALID_ARGUMENT
    lease = service.BeginRead(pb.BeginReadRequest(database_id=db,table='items',ttl_seconds=.01), Context())
    time.sleep(.02)
    with pytest.raises(Aborted) as error:
        list(service.ScanMetadata(pb.ScanRequest(database_id=db,token=lease.token,sql=SEARCH), Context()))
    assert error.value.code == grpc.StatusCode.FAILED_PRECONDITION


def test_shared_stream_limit_keeps_vector_fetch_available(service):
    db = setup_database(service)
    lease = service.BeginRead(pb.BeginReadRequest(database_id=db,table='items'), Context())
    streams = [service.ExecuteSQL(pb.SQLRequest(database_id=db,sql='SELECT * FROM items'), Context()) for _ in range(4)]
    try:
        for stream in streams: assert next(stream).kind == pb.ResultFrame.SCHEMA
        with pytest.raises(Aborted) as error:
            list(service.ScanMetadata(pb.ScanRequest(database_id=db,token=lease.token,sql=SEARCH), Context()))
        assert error.value.code == grpc.StatusCode.RESOURCE_EXHAUSTED
        assert service.FetchVectors(pb.FetchRequest(database_id=db,token=lease.token,ids=[7]), Context()).dimension == 2
    finally:
        for stream in streams: stream.close()
    assert len(list(service.ScanMetadata(pb.ScanRequest(database_id=db,token=lease.token,sql=SEARCH), Context()))) == 1


def test_explicit_longer_deadline_is_preserved():
    from vecstor.storage.sql_service import _remaining
    class LongContext(Context):
        def time_remaining(self): return 30.0
    assert _remaining(LongContext()) == 30.0
