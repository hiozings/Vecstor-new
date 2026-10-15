import runpy


def test_gpu_calibration_freezes_settings_and_reports_unsupported_groups(tmp_path):
    module=runpy.run_path('benchmarks/gpu/calibrate.py')
    report=module['run'](tmp_path/'gpu',sizes=[1024],dimension=3,distributions=['independent'],calibration_count=3,evaluation_count=5,selectivities=[.001,1.],ks=[10])
    assert len(report['groups'])==2
    assert report['groups'][0]['status']=='unsupported'
    assert report['groups'][1]['status']=='passed'
    assert report['groups'][1]['summary']['exact_queries']==0
    assert (tmp_path/'gpu/1024-independent/1-k10.json').exists()
    assert len((tmp_path/'gpu/heldout.jsonl').read_text().splitlines())==5
