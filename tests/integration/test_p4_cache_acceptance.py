import json
import pytest


def test_cache_acceptance_real_terminal_nodes_and_restarts(tmp_path):
    from vecstor.rpc.p4_cache_acceptance import run_cache_acceptance
    output=tmp_path/'cache-check'
    report=run_cache_acceptance(output,rows=2048,dimension=4)
    assert report['passed'] and not report['processes_running_after_cleanup']
    names={case['name'] for case in report['cases']}
    assert {'two_node_warm_cold','restart_empty_cache','storage_epoch_invalidates_resident','pty_cache_controls'}<=names
    assert all(e['directory_denied'] and e['file_denied'] for e in report['isolation'])
    assert any(e.get('artifact_files_denied',0)>=2 for e in report['isolation'])
    with pytest.raises(FileExistsError):run_cache_acceptance(output,rows=2048,dimension=4)


def test_failed_cache_acceptance_preserves_report_and_reaps_processes(tmp_path,monkeypatch):
    from vecstor.rpc.p4_cache_acceptance import run_cache_acceptance
    from vecstor.rpc.sql_client import SessionClient
    def fail(*args,**kwargs):raise RuntimeError('injected cache acceptance failure')
    monkeypatch.setattr(SessionClient,'load_index',fail)
    output=tmp_path/'failed'
    with pytest.raises(RuntimeError,match='injected cache'):run_cache_acceptance(output,rows=2048,dimension=4)
    report=json.loads((output/'report.json').read_text())
    assert not report['passed'] and not report['processes_running_after_cleanup']


def test_cache_acceptance_script_starts_without_pythonpath():
    import os
    from pathlib import Path
    import subprocess
    import sys
    env=dict(os.environ);env.pop('PYTHONPATH',None)
    result=subprocess.run([sys.executable,'scripts/check_p4_cache.py','--help'],cwd=Path(__file__).resolve().parents[2],
                          env=env,capture_output=True,text=True,timeout=15)
    assert result.returncode==0,result.stderr
    assert '--output-dir' in result.stdout
