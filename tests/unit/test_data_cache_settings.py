from dataclasses import replace
from types import SimpleNamespace

import pytest

from vecstor.query.search_options import SearchOptions
from vecstor.query.statements import classify_statement
from vecstor.query.execution_settings import resolve_execution_options
from vecstor.rpc.sql_client import SessionClient
from vecstor.rpc.sql_codec import encode_result

SQL='SELECT id FROM items ORDER BY vector_distance(vector,[0,0]) LIMIT 1'


def test_cache_projection_options_roundtrip_and_request_override():
    options=SearchOptions(data_cache='on',metadata_projection='projected')
    assert SearchOptions.from_proto(options.to_proto())==options
    plan=classify_statement(SQL+" SETTINGS data_cache='off',metadata_projection='full'")
    resolved=resolve_execution_options(options,plan.settings)
    assert resolved.data_cache=='off' and resolved.metadata_projection=='full'
    assert options.data_cache=='on'


@pytest.mark.parametrize('options',[dict(data_cache='auto'),dict(data_cache=True),dict(metadata_projection='partial')])
def test_invalid_data_read_options_rejected(options):
    with pytest.raises(ValueError):SearchOptions(**options).validate()


@pytest.mark.parametrize('suffix',["data_cache='on',data_cache='off'","data_cache=1","metadata_projection='no'"])
def test_invalid_settings_fail_before_rpc(suffix):
    with pytest.raises(ValueError):classify_statement(SQL+' SETTINGS '+suffix)


def test_old_compute_cannot_ignore_explicit_cache_option(monkeypatch):
    with SessionClient('127.0.0.1:1') as client:
        client.search_options=SearchOptions(data_cache='off')
        monkeypatch.setattr(client,'get_search_capabilities',lambda:{'server_instance_id':'legacy'})
        with pytest.raises(ValueError,match='cache'):client.execute(SQL)


def test_cache_completion_ack_and_instance_invalidation(monkeypatch):
    with SessionClient('127.0.0.1:1') as client:
        client.database_id='db';client.search_options=SearchOptions(data_cache='on',metadata_projection='projected')
        state=dict(instance='one',ack=True,controls=0)
        def caps():
            state['controls']+=1
            return dict(server_instance_id=state['instance'],data_cache_version=1,metadata_projection_version=1)
        monkeypatch.setattr(client,'get_search_capabilities',caps)
        class Call:
            def __iter__(self):
                plan=dict(server_instance_id=state['instance'],data_read_version=1,
                    data_cache=dict(requested='on',effective='on'),metadata_projection=dict(requested='projected',effective='projected')) if state['ack'] else {}
                return iter(encode_result(dict(columns=[dict(name='id',type='BIGINT')],rows=[[1]],execution_plan=plan)))
            def cancel(self):pass
        monkeypatch.setattr(client.stub,'ExecuteSQL',lambda *a,**k:Call())
        client.execute(SQL);client.execute(SQL);assert state['controls']==1
        state['instance']='two';client.execute(SQL);client.execute(SQL);assert state['controls']==2
        state['ack']=False
        with pytest.raises(ValueError,match='acknowledge'):client.execute(SQL)


def test_cache_command_parser_rejects_unscoped_or_extra_arguments():
    from vecstor.shell.cache_commands import execute_cache
    with pytest.raises(ValueError):execute_cache(SimpleNamespace(database_id=''),['clear'])
    with pytest.raises(ValueError):execute_cache(SimpleNamespace(database_id='db'),['clear','items'])
