from pathlib import Path
import pytest


def test_small_p4_acceptance_and_output_no_overwrite(tmp_path):
    from vecstor.rpc.p4_index_acceptance import run_acceptance
    output=tmp_path/'acceptance'
    report=run_acceptance(output,rows=2048,dimension=4)
    assert report['passed'] and not report['processes_running_after_cleanup']
    assert report['recovery']['trained'] is False
    assert report['recovery']['artifact_download_bytes']>0
    assert report['nodes']['A']['artifact_id']==report['nodes']['B']['artifact_id']
    assert all(item['directory_denied'] and item['file_denied'] for item in report['isolation'])
    assert any(item['artifact_files_denied']>=2 for item in report['isolation'])
    with pytest.raises(FileExistsError):run_acceptance(output,rows=2048,dimension=4)


def test_failure_still_writes_report_and_cleans_processes(tmp_path,monkeypatch):
    from vecstor.rpc import p4_index_acceptance
    from vecstor.rpc.sql_client import SessionClient
    def fail(*args,**kwargs):raise RuntimeError('injected acceptance failure')
    monkeypatch.setattr(SessionClient,'load_index',fail)
    output=tmp_path/'failed'
    with pytest.raises(RuntimeError,match='injected'):p4_index_acceptance.run_acceptance(output,rows=2048,dimension=4)
    import json
    report=json.loads((output/'report.json').read_text())
    assert not report['passed'] and not report['processes_running_after_cleanup']
