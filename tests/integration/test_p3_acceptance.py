import json
import subprocess
import sys
from pathlib import Path


def test_p3_real_terminal_isolation_and_lifecycle(tmp_path):
    from vecstor.rpc.p3_acceptance import run_acceptance
    report = run_acceptance(tmp_path / 'acceptance', rows=1024)
    assert report['passed']
    assert not report['processes_running_after_cleanup']
    assert report['search_first']['actual_mode'] == 'approx'
    assert len(report['search_first']['rounds']) >= 2
    assert report['filter_first']['actual_mode'] == 'approx'
    assert report['after_update']['fallback_reason'] == 'index_version_mismatch'
    assert report['after_rebuild']['actual_mode'] == 'approx'
    assert report['after_restart']['actual_mode'] == 'exact'
    assert all(event['directory_denied'] and event['file_denied'] for event in report['isolation'])
    assert (tmp_path / 'acceptance/session.txt').stat().st_size > 0


def test_p3_benchmark_small_matrix_retains_truth_and_actual_plans(tmp_path):
    script = Path(__file__).resolve().parents[2] / 'benchmarks/p3/run.py'
    result = subprocess.run([sys.executable, str(script), '--output-dir', str(tmp_path/'bench'),
                             '--sizes', '1024', '--dimension', '2', '--distributions', 'independent',
                             '--repeats', '1'], capture_output=True, text=True, timeout=120)
    assert result.returncode == 0, result.stderr
    report = json.loads((tmp_path/'bench/report.json').read_text())
    assert report['case_count'] == 30
    cases = [json.loads(line) for line in (tmp_path/'bench/cases.jsonl').read_text().splitlines()]
    assert len(cases) == 30
    assert all(row['recall_at_k'] == 1 for row in cases if row['requested_mode'] == 'exact')
    assert all(row['denominator'] == min(row['k'], row['matched_count']) for row in cases)
    assert all('execution_plan' in row and 'truth_ids' in row for row in cases)
    assert not report['processes_running_after_cleanup']
