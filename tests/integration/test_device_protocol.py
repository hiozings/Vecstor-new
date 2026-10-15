import pytest
from test_sql_rpc import servers
from vecstor.rpc.sql_client import SessionClient
from vecstor.rpc import sql_pb2 as pb

SQL='SELECT id FROM items ORDER BY vector_distance(vector,[0,0]) LIMIT 1'


def test_sql_settings_override_rpc_options_and_preserve_session(tmp_path):
    with servers(tmp_path) as (address,_),SessionClient(address) as c:
        c.open_database('demo',True)
        c.execute('CREATE TABLE items(id BIGINT PRIMARY KEY, category VARCHAR NOT NULL, timestamp BIGINT NOT NULL, value DOUBLE NOT NULL, vector FLOAT[2] NOT NULL)')
        c.execute("INSERT INTO items VALUES(1,'x',0,1,[0,0])")
        result=c.execute(SQL+" SETTINGS device='cpu'")
        assert result['execution_plan']['requested_device']=='cpu'
        assert result['execution_plan']['actual_device']=='cpu'
        assert c.search_options.mode=='exact'
        # Bypass client: server must parse SQL, never silently ignore GPU settings.
        import grpc
        with pytest.raises(grpc.RpcError):
            list(c.stub.ExecuteSQL(pb.SQLRequest(database_id=c.database_id,sql=SQL+" SETTINGS device='gpu',mode='approx',path='search-first'"),timeout=5))


def test_device_acknowledgement_required_even_on_warm_client(tmp_path,monkeypatch):
    from vecstor.rpc.sql_codec import encode_result
    with servers(tmp_path) as (address,_),SessionClient(address) as c:
        c.open_database('demo',True)
        monkeypatch.setattr(c,'get_search_capabilities',lambda:{'execution_settings_version':1})
        class Call:
            def __iter__(self):return iter(encode_result({'columns':[{'name':'id','type':'BIGINT'}],'rows':[[1]]}))
            def cancel(self):pass
        monkeypatch.setattr(c.stub,'ExecuteSQL',lambda *a,**k:Call())
        with pytest.raises(ValueError,match='device'):
            c.execute(SQL+" SETTINGS device='cpu'")


def test_old_service_cannot_ignore_gpu_index_controls(monkeypatch):
    client=SessionClient('127.0.0.1:1')
    try:
        client.database_id='db';calls=[]
        monkeypatch.setattr(client,'get_search_capabilities',lambda:{'execution_settings_version':1,'gpu':False})
        monkeypatch.setattr(client,'_typed_control',lambda *a,**k:calls.append(a) or {'state':'ready'})
        with pytest.raises(ValueError,match='device|GPU'):client.build_index('items',device='gpu')
        with pytest.raises(ValueError,match='device|GPU'):client.clear_indexes('items',device='gpu')
        assert not calls
        monkeypatch.setattr(client,'get_search_capabilities',lambda:{'index_devices_version':1})
        with pytest.raises(ValueError,match='acknowledge|device'):client.build_index('items',device='gpu')
    finally:client.close()
