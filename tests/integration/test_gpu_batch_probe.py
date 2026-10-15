import runpy


def test_native_batch_probe_preserves_candidates_and_reports_per_query_cost(tmp_path):
    cal=runpy.run_path('benchmarks/gpu/calibrate.py')
    cal['run'](tmp_path/'cal',sizes=[1024],dimension=3,distributions=['independent'],calibration_count=3,evaluation_count=5,selectivities=[1.],ks=[10])
    probe=runpy.run_path('benchmarks/gpu/batch_probe.py')
    r=probe['run'](tmp_path/'cal',tmp_path/'probe',selected=[(1024,'independent',1.,10)],samples=5,repeats=1)
    assert r['passed'] and len(r['groups'])==3
    assert all(g['candidate_sets_equal'] and g['amortized_ms']>0 for g in r['groups'])
