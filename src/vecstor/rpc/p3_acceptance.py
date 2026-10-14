"""Real PTY, snapshot lifecycle, and Landlock P3 acceptance evidence."""
from contextlib import ExitStack
import json
from pathlib import Path
import sys

import grpc
import numpy as np

from vecstor.data import Dataset
from vecstor.datasets import save_dataset
from vecstor.query.engine import LocalEngine
from vecstor.query.search_options import SearchOptions
from vecstor.rpc.acceptance import ServiceProcess
from vecstor.rpc.common import RemoteError
from vecstor.rpc.shell_acceptance import TerminalProcess
from vecstor.rpc.sql_client import SessionClient


def start_compute(output, root, storage, suffix, cleanup, processes, isolation):
    compute = ServiceProcess(['compute', '--storage', storage.address, '--listen', '127.0.0.1:0',
                              '--timeout', '180', '--isolate-storage-dir', str(root)], output/f'compute{suffix}.log')
    cleanup.callback(compute.stop)
    processes.append(compute.process)
    event = next((event for event in compute.events() if event.get('event') == 'filesystem_isolation'), None)
    if not event or not event['directory_denied'] or not event['file_denied'] or Path(event['storage_directory']) != root:
        raise AssertionError('Missing full data-root Landlock evidence')
    isolation.append(event)
    return compute


def run_acceptance(output_dir, rows=2048, seed=20261013):
    if rows < 1024:
        raise ValueError('P3 acceptance requires at least 1024 rows for IVF')
    output = Path(output_dir).resolve()
    output.mkdir(parents=True, exist_ok=False)
    root = output/'private-storage'
    values = np.arange(rows, dtype=np.float64)
    vectors = np.column_stack((values, np.zeros(rows))).astype(np.float32)
    dataset = Dataset(np.arange(rows, dtype=np.int64)+2**53, np.full(rows, 'item'),
                      np.zeros(rows, dtype=np.int64), values, vectors)
    save_dataset(dataset, root/'imports/source.npz', {'seed': seed})
    sql = 'SELECT id, vector_distance(vector, [0,0]) AS distance FROM items WHERE value >= 100 ORDER BY distance LIMIT 10'
    with LocalEngine(dataset) as local:
        baseline = local.execute('SELECT id FROM items WHERE value >= 100 ORDER BY vector_distance(vector, :query_vector) LIMIT 10', [0,0])
        expected = [[row['id'], distance] for row, distance in zip(baseline.rows, baseline.distances)]
    processes, isolation = [], []
    with ExitStack() as cleanup:
        storage = ServiceProcess(['storage', '--data-root', str(root), '--listen', '127.0.0.1:0'], output/'storage.log')
        cleanup.callback(storage.stop)
        processes.append(storage.process)
        compute = start_compute(output, root, storage, '', cleanup, processes, isolation)
        terminal = TerminalProcess(compute.address, output/'session.txt')
        cleanup.callback(terminal.stop)
        terminal.command('.open demo --create')
        terminal.command('.mode json')
        terminal.command('.import source.npz items')
        terminal.command('.index build items --nlist 1')
        for command in ['.search mode approx', '.search path search-first', '.search nprobe 1', '.search candidates 64', '.plan on']:
            terminal.command(command)
        terminal.command(sql+';')
        terminal.command('.search path filter-first')
        terminal.command(sql+';')
        terminal.command('EXPLAIN '+sql+';')
        terminal.command('EXPLAIN ANALYZE '+sql+';')
        terminal.command('UPDATE items SET category=\'updated\' WHERE value=100;')
        terminal.command(sql+';')
        terminal.command('.index build items --nlist 1')
        terminal.command(sql+';')
        terminal.quit()
        terminal.stop()
        transcript = (output/'session.txt').read_text()
        if 'Error:' in transcript or 'remaining input retained' in transcript:
            raise AssertionError('PTY workflow reported errors; inspect session.txt')
        with SessionClient(compute.address, timeout=180) as client:
            client.open_database('demo')
            indexes = client.list_indexes()
            if not indexes:
                raise AssertionError('Terminal did not build an index')
            client.search_options = SearchOptions(mode='approx', path='search-first', nprobe=1, candidates=64)
            search = client.execute(sql)
            search_plan = search['execution_plan']
            if search['rows'] != expected or search_plan['actual_mode'] != 'approx' or len(search_plan['rounds']) < 2:
                raise AssertionError('Search-first did not expand to the P1 answer')
            if search_plan['counts']['scan_metadata_calls'] != 0:
                raise AssertionError('Search-first performed a full WHERE scan')
            client.search_options = SearchOptions(mode='approx', nprobe=1)
            filtered = client.execute(sql)
            if filtered['rows'] != expected or filtered['execution_plan']['actual_mode'] != 'approx':
                raise AssertionError('Filter-first differs from P1')
            explained = json.loads(client.execute('EXPLAIN '+sql)['rows'][0][0])
            if any(explained['counts'][name] for name in ['scan_metadata_calls', 'filter_candidate_calls', 'vectors_fetched']):
                raise AssertionError('EXPLAIN executed the query')
            analyzed = client.execute('EXPLAIN ANALYZE '+sql)
            analyze_plan = json.loads(analyzed['rows'][0][0])
            if analyze_plan['counts']['scan_metadata_calls'] != 1:
                raise AssertionError('ANALYZE must execute exactly once')
            client.execute("UPDATE items SET category='updated-again' WHERE value=100")
            after_update = client.execute(sql)
            if after_update['rows'] != expected or after_update['execution_plan']['fallback_reason'] != 'index_version_mismatch':
                raise AssertionError('Updated snapshot reused a stale index')
            build = client.build_index('items', nlist=1)
            after_rebuild = client.execute(sql)
            if after_rebuild['rows'] != expected or after_rebuild['execution_plan']['actual_mode'] != 'approx':
                raise AssertionError('Explicit rebuild did not restore ANN')
        compute.stop()
        restarted = start_compute(output, root, storage, '-restart', cleanup, processes, isolation)
        with SessionClient(restarted.address, timeout=2) as client:
            client.open_database('demo')
            if client.list_indexes():
                raise AssertionError('Compute restart retained derived indexes')
            client.search_options = SearchOptions(mode='approx', nprobe=1)
            after_restart = client.execute(sql)
            if after_restart['rows'] != expected or after_restart['execution_plan']['actual_mode'] != 'exact':
                raise AssertionError('Restart without index did not fall back exactly')
            storage.stop()
            try:
                client.execute(sql)
            except RemoteError as error:
                if error.code not in {grpc.StatusCode.UNAVAILABLE, grpc.StatusCode.DEADLINE_EXCEEDED}:
                    raise
                unavailable = error.code.name
            else:
                raise AssertionError('Query succeeded without storage')
        report = dict(passed=True, scope='real_pty_cpu_ivf_p1_landlock_lifecycle', seed=seed,
                      rows=rows, dimension=2, expected_rows=expected, search_first=search_plan,
                      filter_first=filtered['execution_plan'], explain=explained, analyze=analyze_plan,
                      after_update=after_update['execution_plan'], after_rebuild=after_rebuild['execution_plan'],
                      after_restart=after_restart['execution_plan'], build=build, isolation=isolation,
                      storage_stopped_query_status=unavailable, service_pids=[p.pid for p in processes],
                      terminal_pid=terminal.process.pid, python=sys.version.split()[0])
    report['processes_running_after_cleanup'] = any(p.poll() is None for p in [*processes, terminal.process])
    (output/'report.json').write_text(json.dumps(report, indent=2, ensure_ascii=False, allow_nan=False)+'\n')
    return report
