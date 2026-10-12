"""Real terminal workflow, persistence and kernel isolation acceptance evidence."""
from contextlib import ExitStack
from importlib.metadata import version
import json
import os
from pathlib import Path
import pty
import select
import subprocess
import sys
import time

import grpc

from vecstor.datasets import generate_dataset, save_dataset
from vecstor.query.engine import LocalEngine
from vecstor.rpc.acceptance import ServiceProcess
from vecstor.rpc.common import RemoteError
from vecstor.rpc.sql_client import SessionClient


class TerminalProcess:
    """Drive the real prompt_toolkit terminal through a PTY, retaining all output."""
    def __init__(self, address, transcript):
        self.transcript = transcript
        self.output = bytearray()
        self.pending = bytearray()
        self.master, slave = pty.openpty()
        script = Path(__file__).resolve().parents[3] / 'scripts/vecstor.py'
        env = dict(os.environ, TERM='xterm-256color', PROMPT_TOOLKIT_NO_CPR='1',
                   PYTHONDONTWRITEBYTECODE='1', OPENBLAS_NUM_THREADS='1', OMP_NUM_THREADS='1')
        self.process = None
        try:
            self.process = subprocess.Popen([sys.executable, str(script), '--compute', address],
                stdin=slave, stdout=slave, stderr=slave, env=env, close_fds=True)
            os.close(slave)
            slave = None
            self.expect('vecstor>')
        except BaseException:
            if slave is not None:
                os.close(slave)
            self.stop()
            raise

    def expect(self, text, timeout=30):
        needle, deadline = text.encode(), time.monotonic() + timeout
        while needle not in self.pending:
            if time.monotonic() >= deadline:
                raise AssertionError(f'Terminal timed out waiting for {text!r}: {self.pending.decode(errors="replace")}')
            ready, _, _ = select.select([self.master], [], [], 0.1)
            if ready:
                try:
                    data = os.read(self.master, 65536)
                except OSError:
                    data = b''
                if not data:
                    raise AssertionError(f'Terminal exited waiting for {text!r}: {self.pending.decode(errors="replace")}')
                self.pending.extend(data)
                self.output.extend(data)
        del self.pending[:self.pending.index(needle) + len(needle)]

    def command(self, text, prompt='demo>'):
        # Bracketed paste preserves embedded newlines; Enter submits afterward.
        os.write(self.master, b'\x1b[200~' + text.encode() + b'\x1b[201~\r')
        # Wait for this editor to finish and the next one to start. A prompt
        # echoed during redraw is not proof that the RPC has finished.
        self.expect('\x1b[?2004l')
        self.expect('\x1b[?2004h')
        self.expect(prompt)

    def quit(self):
        os.write(self.master, b'.quit\r')
        self.expect('\x1b[?2004l')
        self.process.wait(timeout=10)
        if self.process.returncode:
            raise AssertionError(f'Terminal exited with {self.process.returncode}')

    def stop(self):
        if self.process is not None and self.process.poll() is None:
            self.process.terminate()
            try:
                self.process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait(timeout=5)
        if self.master is not None:
            os.close(self.master)
            self.master = None
        self.transcript.write_text(self.output.decode(errors='replace'), encoding='utf-8')


def _services(output, root, suffix, cleanup, processes, isolation):
    storage = ServiceProcess(['storage', '--data-root', str(root), '--listen', '127.0.0.1:0'], output / f'storage{suffix}.log')
    cleanup.callback(storage.stop)
    processes.append(storage.process)
    compute = ServiceProcess(['compute', '--storage', storage.address, '--listen', '127.0.0.1:0',
                              '--isolate-storage-dir', str(root)], output / f'compute{suffix}.log')
    cleanup.callback(compute.stop)
    processes.append(compute.process)
    event = next((e for e in compute.events() if e.get('event') == 'filesystem_isolation'), None)
    if not event or not event['directory_denied'] or not event['file_denied']:
        raise AssertionError('Compute is missing Landlock directory/file denial evidence')
    if Path(event['storage_directory']) != root:
        raise AssertionError('Compute must be isolated from the entire managed data root')
    isolation.append(event)
    return storage, compute


def run_acceptance(output_dir, rows=128, seed=20261012):
    if rows < 1:
        raise ValueError('rows must be positive')
    output = Path(output_dir).resolve()
    output.mkdir(parents=True, exist_ok=False)
    root = output / 'private-storage'
    dataset = generate_dataset(rows, 2, seed)
    save_dataset(dataset, root / 'imports/sample.npz', {'seed': seed})
    processes, isolation = [], []
    expected = [[1, '论文;已更新', 12.0, [0.0, 0.0]], [2, "书籍'引号", 5.0, [2.0, 0.0]]]
    read_sql = 'SELECT id, category, value, vector FROM items ORDER BY id'
    vector_sql = 'SELECT id, vector_distance(vector, [0,0]) AS distance FROM items WHERE value >= 5 ORDER BY distance LIMIT 10'
    with ExitStack() as cleanup:
        storage, compute = _services(output, root, '', cleanup, processes, isolation)
        terminal = TerminalProcess(compute.address, output / 'session.txt')
        cleanup.callback(terminal.stop)
        terminal.command('.open demo --create')
        terminal.command('.mode json')
        terminal.command('.datasets')
        terminal.command('.import sample.npz imported')
        terminal.command('''CREATE TABLE items (
 id BIGINT PRIMARY KEY,
 category VARCHAR NOT NULL,
 timestamp BIGINT NOT NULL,
 value DOUBLE NOT NULL,
 vector FLOAT[2] NOT NULL
);''')
        terminal.command("""INSERT INTO items VALUES
 (1, '论文;一', 1152921504606846977, 10, [1,1]),
 (2, '书籍''引号', 0, 5, [2,0]),
 (3, '删除', 0, 1, [3,3]);
SELECT id, category FROM items ORDER BY id;""")
        terminal.command("UPDATE items SET category='论文;已更新', value=value+2, vector=[0,0] WHERE id=1;")
        terminal.command(vector_sql + ';')
        terminal.command('DELETE FROM items WHERE id=3;')
        terminal.command('.open other --create', 'other>')
        terminal.command('CREATE TABLE items (id BIGINT); INSERT INTO items VALUES (99);', 'other>')
        terminal.command('USE demo;')
        terminal.command('.tables')
        terminal.command('.schema items')
        terminal.command(read_sql + ';')
        terminal.quit()
        terminal.stop()
        transcript = (output / 'session.txt').read_text()
        if 'Error:' in transcript or 'remaining input retained' in transcript:
            raise AssertionError('Terminal workflow reported an error; inspect session.txt')
        with SessionClient(compute.address) as client:
            databases = client.list_databases()
            if {db['name'] for db in databases} != {'demo', 'other'}:
                raise AssertionError('Terminal did not create both managed databases')
            initial_database = client.open_database('demo')
            before = client.execute(read_sql)
            if before['rows'] != expected:
                raise AssertionError(f'Terminal CRUD mismatch: {before["rows"]}')
            if client.execute('SELECT timestamp FROM items WHERE id=1')['rows'] != [[1152921504606846977]]:
                raise AssertionError('Terminal/RPC lost BIGINT precision')
            if client.execute('SELECT COUNT(*) FROM imported')['rows'] != [[rows]]:
                raise AssertionError('Managed import count mismatch')
            import_cases = []
            query_vector = [0.25, -0.5]
            conditions = [
                ('all', ''),
                ('half', f'WHERE value < {rows // 2}'),
                ('single', 'WHERE id = 0'),
                ('category', "WHERE category = 'category_0'"),
                ('empty', 'WHERE id = -1'),
            ]
            with LocalEngine(dataset) as local:
                for name, condition in conditions:
                    baseline = local.execute(
                        f'SELECT id FROM items {condition} ORDER BY vector_distance(vector, :query_vector) LIMIT 10',
                        query_vector)
                    imported = client.execute(
                        f'SELECT id, vector_distance(vector, [0.25, -0.5]) AS distance FROM imported {condition} ORDER BY distance LIMIT 10')
                    wanted = [[row['id'], distance] for row, distance in zip(baseline.rows, baseline.distances)]
                    if imported['rows'] != wanted:
                        raise AssertionError(f'Imported P1 equivalence failed for {name}: {imported["rows"]} != {wanted}')
                    import_cases.append({'name': name, 'condition': condition, 'query_vector': query_vector,
                                         'matched_count': baseline.matched_count, 'rows': imported['rows']})
            vector = client.execute(vector_sql)
            if vector['rows'] != [[1, 0.0], [2, 4.0]]:
                raise AssertionError(f'Exact vector result mismatch: {vector["rows"]}')
            # The terminal generates opaque operation IDs. A named no-op write
            # independently verifies the same durable operation-status protocol.
            receipt = client.execute('UPDATE items SET value=value WHERE id=1', operation_id='shell-acceptance-restart')
            client.open_database('other')
            if client.execute('SELECT id FROM items')['rows'] != [[99]]:
                raise AssertionError('Database selection leaked across databases')
        compute.stop()
        storage.stop()
        restarted_storage, restarted_compute = _services(output, root, '-restart', cleanup, processes, isolation)
        with SessionClient(restarted_compute.address, timeout=2) as client:
            reopened_database = client.open_database('demo')
            after = client.execute(read_sql)
            if initial_database != reopened_database or after['rows'] != expected:
                raise AssertionError('Database identity/data did not survive restart')
            if after['revision'] != receipt['revision']:
                raise AssertionError('Revision did not persist across restart')
            status = client.get_operation_status('shell-acceptance-restart')
            if status['status'] != 'COMMITTED' or status['result']['revision'] != receipt['revision']:
                raise AssertionError('Committed operation status did not survive restart')
            restarted_storage.stop()
            try:
                client.execute(read_sql)
            except RemoteError as error:
                if error.code not in {grpc.StatusCode.UNAVAILABLE, grpc.StatusCode.DEADLINE_EXCEEDED}:
                    raise
                unavailable = error.code.name
            else:
                raise AssertionError('Compute returned success without live storage')
        report = {'passed': True, 'scope': 'real_pty_localhost_persistence_landlock_not_performance',
                  'seed': seed, 'dimension': 2, 'imported_rows': rows, 'database_count': len(databases),
                  'import_vector_comparisons': len(import_cases), 'import_vector_cases': import_cases,
                  'terminal_workflow_rows': before['rows'], 'vector_ids': [r[0] for r in vector['rows']],
                  'vector_distances': [r[1] for r in vector['rows']], 'restart_operation_status': status['status'],
                  'database_id': reopened_database['id'], 'revision_before_restart': receipt['revision'],
                  'revision_after_restart': after['revision'], 'isolation': isolation,
                  'service_pids': [p.pid for p in processes], 'terminal_pid': terminal.process.pid,
                  'storage_stopped_query_status': unavailable,
                  'versions': {'python': sys.version.split()[0], **{name: version(name) for name in ['duckdb', 'sqlglot', 'grpcio', 'numpy', 'prompt_toolkit', 'rich']}},
                  'operation_status_probe': 'named_noop_write_after_terminal_workflow'}
    report['processes_running_after_cleanup'] = any(p.poll() is None for p in [*processes, terminal.process])
    with (output / 'report.json').open('x', encoding='utf-8') as target:
        json.dump(report, target, indent=2, ensure_ascii=False, allow_nan=False)
        target.write('\n')
    return report
