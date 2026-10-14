import io
import json
import pytest
from rich.console import Console
from vecstor.query.statements import classify_statement
from vecstor.rpc import sql_pb2 as pb
from vecstor.rpc.sql_codec import encode_result, decode_frame
from vecstor.shell.render import render_stream

SQL = 'SELECT id FROM items ORDER BY vector_distance(vector, [0.1,0.2]) LIMIT 2'

@pytest.mark.parametrize('prefix,mode', [('EXPLAIN ', 'plan'), ('/* hi */ EXPLAIN /* next */ ANALYZE ', 'analyze')])
def test_explain_vector(prefix, mode):
    plan = classify_statement(prefix + SQL)
    assert plan.kind == 'vector' and plan.explain == mode
    assert plan.sql.startswith('SELECT') and plan.vector.limit == 2

@pytest.mark.parametrize('sql', ['EXPLAIN SELECT id FROM items', 'EXPLAIN DELETE FROM items', 'EXPLAIN ANALYZE ' + SQL + '; DROP TABLE items'])
def test_explain_reject(sql):
    with pytest.raises(ValueError): classify_statement(sql)


def test_plan_codec_and_display():
    plan = {'requested_mode':'approx', 'actual_mode':'exact', 'fallback_reason':'index_missing'}
    frames = list(encode_result({'execution_plan':plan}))
    assert decode_frame(frames[-1])['execution_plan'] == plan
    out = io.StringIO()
    render_stream(Console(file=out), map(decode_frame, frames), 'table', 10, lambda: True)
    assert 'exact' in out.getvalue() and 'index_missing' in out.getvalue()
    out = io.StringIO()
    render_stream(Console(file=out), map(decode_frame, frames), 'json', 10, lambda: True)
    assert 'execution_plan' in out.getvalue()

@pytest.mark.parametrize('payload', ['[]', '{"bad":NaN}', '{"bad":1e999}', 'bad'])
def test_invalid_plan_json(payload):
    frame = pb.ResultFrame(kind=pb.ResultFrame.COMPLETE, execution_plan=pb.QueryExecutionPlan(json_payload=payload))
    with pytest.raises(ValueError): decode_frame(frame)


def test_plan_only_completion():
    frame = pb.ResultFrame(kind=pb.ResultFrame.SCHEMA, execution_plan=pb.QueryExecutionPlan(json_payload='{}'))
    with pytest.raises(ValueError): decode_frame(frame)

from types import SimpleNamespace
from vecstor.rpc.sql_client import SessionClient
from vecstor.query.search_options import SearchOptions
from vecstor.shell.app import Shell

class Future:
    def __init__(self, value): self.value, self.cancelled = value, False
    def result(self): return self.value
    def cancel(self): self.cancelled = True

class Method:
    def __init__(self, payload): self.payload, self.calls = payload, []
    def future(self, request, timeout):
        self.calls.append((request, timeout))
        self.last = Future(pb.ControlResponse(json_result=json.dumps(self.payload)))
        return self.last

class Stream(list):
    def cancel(self): pass


def client_fixture(ann=True):
    client = SessionClient.__new__(SessionClient)
    client.timeout, client.database_id, client.database_name = 15, 'db', 'demo'
    client.search_options = SearchOptions()
    client.stub = SimpleNamespace(Control=Method({'columns':[], 'rows':[]}), GetSearchCapabilities=Method({'ann':ann}), BuildIndex=Method({'status':'ready'}), ListIndexes=Method([]), ClearIndexes=Method({'cleared':1}))
    client.requests = []
    def execute(request, timeout):
        client.requests.append(request)
        return Stream(encode_result({'columns':[], 'rows':[]}))
    client.stub.ExecuteSQL = execute
    return client


def test_client_options_snapshot_and_scalar_scope():
    client = client_fixture()
    client.set_search('mode', 'approx')
    stream = client.execute_stream(SQL)
    client.reset_search()
    list(stream)
    assert client.requests[-1].search_options.mode == 'approx'
    list(client.execute_stream('SELECT id FROM items'))
    assert not client.requests[-1].HasField('search_options')


def test_capabilities_fail_preserves_defaults():
    client = client_fixture(False)
    with pytest.raises(ValueError): client.set_search('mode', 'approx')
    assert client.search_options.mode == 'exact'


def test_index_requests_and_cancellation():
    client = client_fixture()
    assert client.build_index('items', nlist=4)['status'] == 'ready'
    call, timeout = client.stub.BuildIndex.calls[-1]
    assert call.database_id == 'db' and call.table == 'items' and call.nlist == 4 and timeout == 45
    assert client.stub.BuildIndex.last.cancelled
    assert client.list_indexes() == []
    assert client.clear_indexes('items') == {'cleared':1}
    with pytest.raises(ValueError): client.build_index('items', timeout=61)


def test_shell_search_index_and_plan_commands():
    client = client_fixture()
    out = io.StringIO()
    shell = Shell(client, Console(file=out))
    shell.submit('.search mode approx')
    shell.submit('.search max-candidates 256')
    shell.submit('.plan on')
    shell.submit('.index build items --nlist 4 --timeout 20')
    assert client.search_options.max_candidates == 256 and shell.show_plan
    assert client.stub.BuildIndex.calls[-1][1] == 20
    shell.submit(SQL + ';')
    assert 'approx' in out.getvalue()
    shell.submit('.search reset')
    assert client.search_options == SearchOptions()
