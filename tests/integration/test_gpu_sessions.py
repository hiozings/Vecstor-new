import runpy


def test_warm_benchmark_keeps_connections_and_separates_client_control(tmp_path):
    cal=runpy.run_path('benchmarks/gpu/calibrate.py')
    cal['run'](tmp_path/'cal',sizes=[1024],dimension=3,distributions=['independent'],calibration_count=3,evaluation_count=5,selectivities=[1.],ks=[10])
    module=runpy.run_path('benchmarks/gpu/sessions.py')
    r=module['run'](tmp_path/'cal',tmp_path/'sessions',selected=[(1024,'independent',1.,10)],variants=['uncached','cached'],samples=4,repeats=1,concurrencies=[1])
    assert r['passed'] and len(r['groups'])==4
    for g in r['groups']:
        assert g['client_control_calls']==(4 if g['variant']=='uncached' else 0)
        assert g['connections_created']==1 and g['queries']==4
        assert g['p50_ms']>0
