from pathlib import Path
import runpy


def test_lifecycle_benchmark_records_real_query_and_resource_samples(tmp_path):
    runner=runpy.run_path(str(Path(__file__).resolve().parents[2]/'benchmarks/p4/index_lifecycle.py'))['run_lifecycle_benchmark']
    report=runner(tmp_path/'costs',sizes=(2048,),repeats=1,concurrency=(1,),distributions=('independent',),dimension=4)
    assert report['passed'] and len(report['runs'])==1
    run=report['runs'][0]
    assert run['query_costs'][0]['requests']>0 and run['query_costs'][0]['throughput']>0
    assert run['resources']['index_disk_peak_bytes']>0
    assert all(item['peak_rss_bytes']>0 for item in run['resources']['processes'])
    assert run['resources']['verifier_peak_rss_bytes']>0
    assert not run['processes_running_after_cleanup']
