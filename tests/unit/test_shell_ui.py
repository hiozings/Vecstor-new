import io
import pytest
from vecstor.shell.app import Shell, submission_ready
from rich.console import Console

class Client:
    database_name = ''
    last_was_write = False
    last_operation_id = ''
    def __init__(self):
        self.calls = []
        self.closed_streams = 0
    def execute_stream(self, sql):
        self.calls.append(sql.strip())
        try:
            if 'bad' in sql:
                raise ValueError('bad SQL')
            yield {'kind': 'schema', 'columns': [{'name': '中文', 'type': 'VARCHAR'}]}
            yield {'kind': 'rows', 'rows': [['论文'], ['书籍']]}
            yield {'kind': 'complete', 'affected_rows': None, 'revision': 1}
        finally:
            self.closed_streams += 1
    def list_databases(self):
        return [{'id': 'db', 'name': 'demo'}]
    def open_database(self, name, create=False):
        if name == 'missing':
            raise ValueError('not found')
        self.database_name = name
        return {'id': 'db', 'name': name}
    def describe(self, table=None):
        return {'columns': [{'name': 'name'}], 'rows': [[table or 'items']]}
    def get_operation_status(self, oid):
        return {'operation_id': oid, 'status': 'committed'}


def make_shell(**kw):
    output = io.StringIO()
    client = Client()
    shell = Shell(client, console=Console(file=output, width=60, color_system=None), **kw)
    return shell, client, output


def test_batch_stops_after_failure_preserving_rest():
    shell, client, out = make_shell()
    shell.submit('SELECT 1; bad; SELECT 3; SELECT')
    assert client.calls == ['SELECT 1;', 'bad;']
    assert shell.buffer.pending == ' SELECT 3; SELECT'
    assert '1 completed' in out.getvalue()
    shell.submit(shell.buffer.pending + ' 4;')
    assert client.calls[-2:] == ['SELECT 3;', 'SELECT 4;']


def test_commands_validate_arguments_preserve_database_and_no_rpc_comments():
    shell, client, out = make_shell()
    shell.submit('.open demo --create')
    shell.submit('.open missing')
    assert client.database_name == 'demo'
    shell.submit('.mode xml')
    assert shell.mode == 'table'
    shell.submit('; -- comment\n /* nothing */')
    assert not client.calls
    shell.submit('SELECT')
    shell.submit('.clear')
    assert not shell.buffer.pending


def test_paging_stops_and_closes_stream_without_changing_sql():
    shell, client, out = make_shell(page_size=1, continue_page=lambda: False)
    shell.submit('SELECT * FROM items; SELECT 2;')
    assert client.calls == ['SELECT * FROM items;']
    assert client.closed_streams == 1
    assert '论文' in out.getvalue() and '书籍' not in out.getvalue()
    assert shell.buffer.pending.strip() == 'SELECT 2;'


def test_interrupt_reports_write_status_and_closes_stream():
    shell, client, out = make_shell(page_size=1, continue_page=lambda: (_ for _ in ()).throw(KeyboardInterrupt()))
    client.last_was_write, client.last_operation_id = True, 'op-123'
    shell.submit('INSERT INTO items VALUES (1);')
    assert client.closed_streams == 1
    assert 'committed' in out.getvalue() and 'op-123' in out.getvalue()


def test_json_output_preserves_bigint_and_chinese():
    shell, client, out = make_shell()
    shell.submit('.mode json')
    shell.submit('SELECT 1;')
    assert '"论文"' in out.getvalue()


@pytest.mark.parametrize('text, ready', [('SELECT 1', False), ("SELECT ';'", False), ('SELECT $$;$$', False), ('SELECT 1; SELECT', True), ('.help', True), ("SELECT '\n.clear", False)])
def test_submission_uses_lexer_state(text, ready):
    assert submission_ready(text) is ready


def test_clear_on_independent_normal_line_but_not_inside_literal():
    shell, client, out = make_shell()
    shell.submit('SELECT 1\n.clear')
    assert not shell.buffer.pending
    shell.submit("SELECT 'hello\n.clear")
    assert shell.buffer.pending == "SELECT 'hello\n.clear"


def test_actual_pty_multiline_completion_unicode_and_controls(tmp_path):
    import os
    import pty
    import select
    import subprocess
    import sys
    import time
    from pathlib import Path
    master, slave = pty.openpty()
    program = '''
from vecstor.shell.app import run_shell
class Client:
    database_name = ''
    last_was_write = False
    def execute_stream(self, sql):
        assert sql.strip().startswith('SELECT '), repr(sql)
        print('EXECUTED:' + repr(sql), flush=True)
        yield {'kind':'schema', 'columns':[{'name':'分类','type':'VARCHAR'}]}
        yield {'kind':'rows', 'rows':[['论文']]}
        yield {'kind':'complete','affected_rows':None}
    def describe(self, table=None): return {'columns': [], 'rows': []}
    def close(self): print('CLIENT-CLOSED', flush=True)
run_shell(Client())
'''
    env = dict(os.environ, PYTHONPATH=str(Path(__file__).resolve().parents[2] / 'src'), TERM='xterm-256color', PROMPT_TOOLKIT_NO_CPR='1')
    child = subprocess.Popen([sys.executable, '-c', program], stdin=slave, stdout=slave, stderr=slave, env=env)
    os.close(slave)
    received = bytearray()
    def expect(value):
        deadline = time.monotonic() + 8
        while value.encode() not in received:
            assert time.monotonic() < deadline, received.decode(errors='replace')
            ready, _, _ = select.select([master], [], [], 0.1)
            if ready:
                try:
                    received.extend(os.read(master, 65536))
                except OSError:
                    pytest.fail(received.decode(errors='replace'))
        del received[:received.index(value.encode()) + len(value.encode())]
    try:
        expect('vecstor>')
        os.write(master, b'SEL\t')
        expect('SELECT')
        os.write(master, " '中文'\r".encode())
        expect('->')
        os.write(master, b'\x04')
        expect('Unsubmitted input retained')
        os.write(master, b';\r')
        expect('EXECUTED:')
        expect('论文')
        expect('vecstor>')
        os.write(master, b'SELECT unfinished\x03')
        expect('Input cleared.')
        expect('vecstor>')
        os.write(master, b'\x04')
        child.wait(timeout=8)
        assert child.returncode == 0
    finally:
        if child.poll() is None:
            child.kill()
            child.wait()
        os.close(master)


def test_import_interrupt_checks_operation_instead_of_claiming_rollback():
    shell, client, out = make_shell()
    def interrupted_import(source, table):
        client.last_was_write, client.last_operation_id = True, 'import-1'
        raise KeyboardInterrupt()
    client.import_dataset = interrupted_import
    shell.submit('.import managed items')
    assert 'import-1' in out.getvalue() and 'committed' in out.getvalue()


def test_schema_completion_is_cached_and_preserves_unicode_names():
    from prompt_toolkit.document import Document
    from prompt_toolkit.completion import CompleteEvent
    shell, client, out = make_shell()
    shell.submit('.schema 中文表')
    completions = list(shell.completer.get_completions(Document('中文'), CompleteEvent()))
    assert [item.text for item in completions] == ['中文表']
    client.describe = lambda *args: (_ for _ in ()).throw(AssertionError('RPC while typing'))
    assert [item.text for item in shell.completer.get_completions(Document('中文'), CompleteEvent())] == ['中文表']


def test_incomplete_stream_is_error_and_preserves_next_statement():
    shell, client, out = make_shell()
    def truncated(sql):
        yield {'kind': 'schema', 'columns': [{'name': 'id', 'type': 'BIGINT'}]}
        yield {'kind': 'rows', 'rows': [[9223372036854775807]]}
    client.execute_stream = truncated
    shell.submit('SELECT id FROM items; SELECT 2;')
    assert 'without completion' in out.getvalue()
    assert shell.buffer.pending.strip() == 'SELECT 2;'


def test_tables_displays_table_list_without_describing_columns():
    shell, client, out = make_shell()
    def execute(sql):
        assert sql == 'SHOW TABLES;'
        return {'columns': [{'name': 'name', 'type': 'VARCHAR'}], 'rows': [['inventory']]}
    client.execute = execute
    client.describe = lambda *args: (_ for _ in ()).throw(AssertionError('.tables must list tables'))
    shell.submit('.tables')
    assert 'inventory' in out.getvalue()


def test_successful_sql_refreshes_schema_completion():
    from prompt_toolkit.document import Document
    from prompt_toolkit.completion import CompleteEvent
    shell, client, out = make_shell()
    client.describe = lambda *args: {'columns': [{'name': 'name'}], 'rows': [['new_table']]}
    shell.submit('CREATE TABLE new_table (id BIGINT);')
    matches = list(shell.completer.get_completions(Document('new_'), CompleteEvent()))
    assert [item.text for item in matches] == ['new_table']


def test_actual_describe_schema_completes_table_and_custom_column():
    from prompt_toolkit.document import Document
    from prompt_toolkit.completion import CompleteEvent
    shell, client, out = make_shell()
    client.describe = lambda *args: {
        'columns': [{'name': name} for name in ['table', 'column', 'type', 'nullable', 'key']],
        'rows': [['inventory', 'stock_count', 'BIGINT', 'YES', '']],
    }
    shell.submit('.open demo')
    assert [c.text for c in shell.completer.get_completions(Document('stock_'), CompleteEvent())] == ['stock_count']


def test_database_switch_replaces_cache_but_failed_open_preserves_it():
    from prompt_toolkit.document import Document
    from prompt_toolkit.completion import CompleteEvent
    shell, client, out = make_shell()
    client.describe = lambda *args: {'columns': [{'name': 'name'}], 'rows': [['inventory']] if client.database_name == 'demo' else []}
    shell.submit('.open demo')
    shell.submit('.open missing')
    assert [c.text for c in shell.completer.get_completions(Document('inv'), CompleteEvent())] == ['inventory']
    shell.submit('.open empty')
    assert list(shell.completer.get_completions(Document('inv'), CompleteEvent())) == []
