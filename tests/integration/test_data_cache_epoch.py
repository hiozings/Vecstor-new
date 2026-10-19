import json

import duckdb
import grpc
import numpy as np
import pytest

from test_p3_storage import running, setup
from vecstor.rpc import sql_pb2 as pb


def lease(stub,db):return stub.BeginRead(pb.BeginReadRequest(database_id=db,table='items',ttl_seconds=3),timeout=5)


def test_storage_restart_changes_epoch_even_same_revision(tmp_path):
    with running(tmp_path) as (stub,catalog):
        db=setup(catalog,tmp_path);first=lease(stub,db)
        assert first.storage_epoch and 0<first.effective_ttl_seconds<=3
        assert json.loads(stub.Control(pb.ControlRequest(action='capabilities'),timeout=3).json_result)['data_cache_epoch_version']==1
    with running(tmp_path) as (stub,catalog):
        second=lease(stub,db)
        assert second.revision==first.revision and second.table_id==first.table_id
        assert second.storage_epoch!=first.storage_epoch


def test_offline_same_revision_content_change_gets_new_epoch(tmp_path):
    with running(tmp_path) as (stub,catalog):
        db=setup(catalog,tmp_path);first=lease(stub,db)
        path=catalog.database(db).path
    # Controlled offline recovery fixture: persistent identity/revision collide.
    with duckdb.connect(str(path)) as connection:
        connection.execute('UPDATE items SET vector=[0,0,0,0] WHERE id=1')
    with running(tmp_path) as (stub,catalog):
        second=lease(stub,db)
        assert (first.database_id,first.table_id,first.revision)==(second.database_id,second.table_id,second.revision)
        assert second.storage_epoch!=first.storage_epoch
        data=stub.FetchVectors(pb.FetchRequest(database_id=db,token=second.token,ids=[1]),timeout=3)
        assert np.frombuffer(data.vectors_f32_le,dtype='<f4').tolist()==[0.,0.,0.,0.]


def test_projected_rpc_is_unimplemented_until_storage_gate(tmp_path):
    with running(tmp_path) as (stub,catalog):
        db=setup(catalog,tmp_path);current=lease(stub,db)
        caps=json.loads(stub.Control(pb.ControlRequest(action='capabilities'),timeout=3).json_result)
        if caps.get('data_read_version')==1:
            # T3 activates this API; invalid requests must then be validated.
            expected=grpc.StatusCode.INVALID_ARGUMENT
        else:expected=grpc.StatusCode.UNIMPLEMENTED
        with pytest.raises(grpc.RpcError) as error:
            stub.ReadProjectedCandidates(pb.ProjectedCandidateRequest(database_id=db,token=current.token),timeout=3)
        assert error.value.code()==expected
