"""Real PTY plus isolated compute/storage process acceptance."""
import json

from vecstor.rpc.shell_acceptance import run_acceptance


def test_shell_crud_restart_and_landlock(tmp_path):
    output = tmp_path / 'shell-evidence'
    report = run_acceptance(output, rows=8, seed=20261012)
    assert report['passed']
    assert report['imported_rows'] == 8
    assert report['import_vector_comparisons'] == 5
    assert {case['matched_count'] for case in report['import_vector_cases']} >= {0, 1, 4, 8}
    assert report['database_count'] == 2
    assert report['terminal_workflow_rows'] == [[1, '论文;已更新', 12.0, [0.0, 0.0]], [2, "书籍'引号", 5.0, [2.0, 0.0]]]
    assert report['vector_ids'] == [1, 2]
    assert report['vector_distances'] == [0.0, 4.0]
    assert report['restart_operation_status'] == 'COMMITTED'
    assert all(event['directory_denied'] and event['file_denied'] for event in report['isolation'])
    assert len(set(report['service_pids'])) == 4
    assert not report['processes_running_after_cleanup']
    assert report['storage_stopped_query_status'] in {'UNAVAILABLE', 'DEADLINE_EXCEEDED'}
    assert json.loads((output / 'report.json').read_text()) == report
    assert '论文;已更新' in (output / 'session.txt').read_text()
