from pathlib import Path
import json
import runpy


def test_frozen_parameters_match_real_rpc_and_export_actual_snapshot(tmp_path):
    base=Path(__file__).resolve().parents[2]/'benchmarks/p3'
    cal=runpy.run_path(str(base/'calibrate.py'))
    cal['run'](tmp_path/'cal',sizes=[1024],distributions=['independent'],dimension=4,
               calibration_count=3,evaluation_count=4,selectivities=[.1,1.])
    assert (base/'validate_quality.py').exists(), 'RPC quality validation is not implemented'
    validator=runpy.run_path(str(base/'validate_quality.py'))
    report=validator['run'](tmp_path/'cal',tmp_path/'rpc',samples=2,repeats=1)
    assert report['passed'] and report['queries']==8
    assert report['processes_running_after_cleanup'] is False
    bundle=json.loads((tmp_path/'rpc/quality-profiles.json').read_text())
    assert len(bundle['profiles'])==4
    for profile in bundle['profiles']:
        assert profile['index_key']['revision']==1
        assert profile['index_key']['database_id']
        assert profile['workload']['k']==10
        assert profile['budget']['nprobe_floor']>=1
    records=[json.loads(line) for line in (tmp_path/'rpc/cases.jsonl').read_text().splitlines()]
    assert all(r['matches_frozen'] for r in records)
    assert all(r['plan']['actual_mode']=='approx' for r in records)
