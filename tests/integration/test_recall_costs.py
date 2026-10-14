from pathlib import Path
import runpy


def test_cost_study_runs_real_index_variants_and_rpc_ablation(tmp_path):
    base=Path(__file__).resolve().parents[2]/'benchmarks/p3'
    calibration=runpy.run_path(str(base/'calibrate.py'))
    calibration['run'](tmp_path/'cal',sizes=[1024],distributions=['independent'],dimension=4,
                       calibration_count=200,evaluation_count=500,selectivities=[1.])
    validation=runpy.run_path(str(base/'validate_quality.py'))
    validation['run'](tmp_path/'cal',tmp_path/'rpc',samples=1,repeats=1)
    assert (base/'costs.py').exists(), 'Cost study runner is not implemented'
    costs=runpy.run_path(str(base/'costs.py'))
    native=costs['native_study'](tmp_path/'cal',tmp_path/'native',size=1024,nlists=[8],iterations=[10],
                                sample_caps=[256],calibration_count=4,evaluation_count=8,selectivities=[1.])
    assert len(native['models'])==1
    assert native['models'][0]['byte_size']>0
    assert native['models'][0]['groups'][0]['passed']
    rpc=costs['rpc_study'](tmp_path/'cal',tmp_path/'rpc',tmp_path/'cost',size=1024,
                          samples=1,repeats=1,selectivities=[1.],concurrencies=[1],modes=[('text',True)])
    assert rpc['passed'] and not rpc['processes_running_after_cleanup']
    assert len(rpc['groups'])==2
    assert all(g['samples']==1 and g['compute_peak_rss_bytes']>0 for g in rpc['groups'])
