from pathlib import Path
import json
import runpy

import pytest

ROOT=Path(__file__).resolve().parents[2]


def test_data_read_smoke_pairs_variants_and_keeps_raw_resources(tmp_path):
    runner=runpy.run_path(str(ROOT/'benchmarks/p4/data_reads.py'))
    output=tmp_path/'matrix'
    report=runner['run_data_read_matrix'](output,tier='smoke')
    assert report['passed'] and report['tier']=='smoke'
    assert not report['processes_running_after_cleanup']
    assert len(report['runs'])==24
    assert {r['repeat'] for r in report['runs']}=={1,2,3}
    variants={(r['cache'],r['projection'],r['candidate_path']) for r in report['runs']}
    assert len(variants)==8
    for run in report['runs']:
        assert [p['name'] for p in run['phases']]==['cold','warm']
        assert all(p['requests']==2 and p['p50_ms']>0 for p in run['phases'])
        assert run['resources']['processes'] and run['cache_start_entries']==0
    records=[json.loads(line) for line in (output/'cases.jsonl').read_text().splitlines()]
    assert len(records)==96 and all(r['equivalent'] for r in records)
    assert any(r['plan']['data_cache']['hit_rows']>0 for r in records if r['phase']=='warm' and r['cache']=='on')
    with pytest.raises(FileExistsError):runner['run_data_read_matrix'](output,tier='smoke')


def test_full_manifest_preserves_200_queries_and_three_repeats():
    runner=runpy.run_path(str(ROOT/'benchmarks/p4/data_reads.py'))
    cases=runner['matrix_cases']('correctness')
    assert {c['rows'] for c in cases}=={10000,100000}
    assert {c['distribution'] for c in cases}=={'independent','correlated'}
    assert {c['selectivity'] for c in cases}=={.001,.01,.1,.5,1.}
    assert {c['k'] for c in cases}=={10,100}
    assert all(c['samples']==200 for c in cases)
    assert {c['mode'] for c in cases}=={'exact','approx'}


def test_benchmark_failure_is_not_reported_as_success(tmp_path,monkeypatch):
    runner=runpy.run_path(str(ROOT/'benchmarks/p4/data_reads.py'))
    from vecstor.rpc.sql_client import SessionClient
    def fail(*args,**kwargs):raise ValueError('injected benchmark failure')
    monkeypatch.setattr(SessionClient,'load_index',fail)
    output=tmp_path/'failed'
    with pytest.raises(ValueError,match='injected'):runner['run_data_read_matrix'](output,tier='smoke')
    report=json.loads((output/'report.json').read_text())
    assert not report['passed'] and not report['processes_running_after_cleanup']


def test_matrix_selection_rejects_missing_case_before_creating_output(tmp_path):
    runner=runpy.run_path(str(ROOT/'benchmarks/p4/data_reads.py'))
    output=tmp_path/'invalid'
    with pytest.raises(ValueError,match='case'):runner['run_data_read_matrix'](output,tier='smoke',case_ids=[999])
    assert not output.exists()
