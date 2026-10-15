from pathlib import Path
import subprocess,sys,json


def test_capability_runner_reports_unavailable_without_false_success(tmp_path):
    result=subprocess.run([sys.executable,'scripts/check_gpu_ann.py','--output-dir',str(tmp_path/'report'),'--cpu-only'],capture_output=True,text=True)
    assert result.returncode==0,result.stderr
    report=json.loads((tmp_path/'report/report.json').read_text())
    assert report['gpu_verified'] is False
    assert report['status']=='skipped'
