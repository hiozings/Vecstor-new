import runpy


def test_gpu_rpc_benchmark_retains_device_and_cost_evidence(tmp_path):
    calibration=runpy.run_path('benchmarks/gpu/calibrate.py')
    calibration['run'](tmp_path/'cal',sizes=[1024],dimension=3,distributions=['independent'],calibration_count=3,evaluation_count=5,selectivities=[1.],ks=[10])
    benchmark=runpy.run_path('benchmarks/gpu/run.py')
    report=benchmark['run'](tmp_path/'cal',tmp_path/'rpc',samples=1,repeats=1,concurrencies=[1])
    assert report['passed'] and len(report['groups'])==2
    assert {g['device'] for g in report['groups']}=={'cpu','gpu'}
    assert all(g['queries']==1 and g['p50_ms']>0 for g in report['groups'])
    assert not report['processes_running_after_cleanup']
