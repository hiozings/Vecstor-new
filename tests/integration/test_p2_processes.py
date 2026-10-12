import json

import pytest


@pytest.mark.parametrize("distribution", ["independent", "correlated"])
def test_isolated_processes_match_p1_and_need_live_storage(tmp_path, distribution):
    from vecstor.rpc.acceptance import run_acceptance
    output = tmp_path / distribution
    report = run_acceptance(output, rows=100, dimension=4, distribution=distribution)
    assert report["passed"] is True and report["case_count"] == 16
    assert report["isolation"]["directory_denied"] is True
    assert report["isolation"]["file_denied"] is True
    assert report["isolation"]["mechanism"] == "landlock"
    assert report["storage_stopped_query_status"] in {"UNAVAILABLE", "DEADLINE_EXCEEDED"}
    assert report["storage_pid"] != report["compute_pid"]
    evidence = json.loads((output / "report.json").read_text())
    assert evidence == report
    assert not report["compute_running_after_cleanup"] and not report["storage_running_after_cleanup"]
