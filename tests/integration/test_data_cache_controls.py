from dataclasses import replace

import pytest

from test_data_cache_execution import cached,SQL
from test_sql_rpc import servers
from vecstor.compute.service import create_compute_server
from vecstor.cache.types import CacheConfig
from vecstor.rpc.sql_client import SessionClient
from vecstor.storage.sql_service import create_catalog_server


def test_request_off_does_not_touch_warm_cache_and_full_overrides_projected(cached,monkeypatch):
    cached.client.execute(SQL)
    cached.compute.metadata_projection='projected'
    original=cached.compute.data_cache.batch
    def forbidden(*a,**k):pytest.fail('off must not look up cache')
    monkeypatch.setattr(cached.compute.data_cache,'batch',forbidden)
    result=cached.client.execute(SQL+" SETTINGS data_cache='off',metadata_projection='full'")
    assert result['execution_plan']['data_cache']['effective']=='off'
    assert result['execution_plan']['metadata_projection']['effective']=='full'
    assert result['execution_plan']['counts']['vectors_fetched']==64
    monkeypatch.setattr(cached.compute.data_cache,'batch',original)
    assert cached.client.execute(SQL)['execution_plan']['counts']['vectors_fetched']==0


def test_clear_and_stats_local_to_database_preserve_index(cached):
    cached.client.execute(SQL)
    before=cached.client.inspect_published_index('items')
    stats=cached.client.data_cache_stats()
    assert stats['database']['entry_count']==64
    cached.client.clear_data_cache()
    assert cached.client.data_cache_stats()['database']['entry_count']==0
    assert cached.client.inspect_published_index('items')==before
    assert cached.client.execute(SQL)['execution_plan']['counts']['vectors_fetched']==64


def test_cache_session_settings_do_not_require_vector_ack_for_crud(cached):
    cached.client.search_options=replace(cached.client.search_options,data_cache='on',metadata_projection='projected')
    assert cached.client.execute('CREATE TABLE plain (id BIGINT)')['affected_rows'] is not None
    assert cached.client.execute('INSERT INTO plain VALUES (1)')['affected_rows']==1
    assert cached.client.execute('SELECT * FROM plain')['rows']==[[1]]


def test_zero_capacity_rejects_explicit_on(tmp_path):
    with servers(tmp_path) as (address,_),SessionClient(address) as client:
        client.open_database('demo',True)
        with pytest.raises(ValueError,match='cache'):
            client.execute(SQL+" SETTINGS data_cache='on'")


def test_enabled_cache_rejects_storage_missing_epoch_capability(tmp_path,monkeypatch):
    import json
    from vecstor.rpc import sql_pb2 as pb
    from vecstor.storage.sql_service import SQLStorageService
    actual=SQLStorageService.Control
    def old(self,request,context):
        if request.action=='capabilities':return pb.ControlResponse(json_result='{}')
        return actual(self,request,context)
    monkeypatch.setattr(SQLStorageService,'Control',old)
    storage,port,backend=create_catalog_server(tmp_path,'127.0.0.1:0');storage.start()
    try:
        with pytest.raises(ValueError,match='epoch'):create_compute_server(f'127.0.0.1:{port}','127.0.0.1:0',data_cache_config=CacheConfig(max_bytes=1024**2))
    finally:storage.stop(0).wait();backend.close()


def test_lazy_storage_capability_rpc_is_accounted(cached):
    cached.compute._read_capabilities=None
    first=cached.client.execute(SQL+" SETTINGS metadata_projection='projected'")['execution_plan']
    assert first['counts']['storage_capability_rpc_calls']==1
    assert first['counts']['rpc_calls']==5
    second=cached.client.execute(SQL+" SETTINGS metadata_projection='projected'")['execution_plan']
    assert second['counts']['storage_capability_rpc_calls']==0
    assert second['counts']['rpc_calls']==3
