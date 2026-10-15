import runpy


def test_gpu_acceptance_covers_isolation_restart_and_recovery(tmp_path):
    m=runpy.run_path('scripts/check_gpu.py')
    report=m['run'](tmp_path/'gpu',rows=2048)
    assert report['passed']
    assert report['query']['actual_device']=='gpu'
    assert report['worker_isolation']=={'directory_denied':True,'file_denied':True}
    assert report['after_restart']['actual_device']=='cpu'
    assert report['after_rebuild']['actual_device']=='gpu'
    assert not report['processes_running_after_cleanup']
