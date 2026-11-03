from pathlib import Path
import json
import runpy


def test_smoke_matrix_is_explicit_exact_only_and_cleans_processes(tmp_path):
    path=Path(__file__).resolve().parents[2]/'benchmarks/p4/adaptive.py'
    run=runpy.run_path(str(path))['run_adaptive_matrix']
    report=run(tmp_path/'smoke',tier='smoke',policy_path=None,repeats=3)
    assert report['functional_passed']
    assert report['scope']=='smoke_exact_only'
    assert not report['quality_certified']
    assert report['configuration_runs']==6
    assert report['measured_queries']==24
    assert report['processes_running_after_cleanup']==[]
    assert all(r['equivalent'] for r in report['runs'])
    prepared=json.loads((tmp_path/'smoke/calibration/report.json').read_text())
    assert prepared['cases'][0]['eligible_paths']==[]
    repeated=runpy.run_path(str(path))['evaluate'](tmp_path/'smoke/calibration',tmp_path/'smoke/evaluation',tier='smoke',resume=True)
    assert repeated['reused_configuration_runs']==6 and repeated['configuration_runs']==6
