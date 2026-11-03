from pathlib import Path
import runpy

import numpy as np
import pytest


def helpers():
    return runpy.run_path(str(Path(__file__).resolve().parents[2]/'benchmarks/p4/adaptive_costs.py'))


def records(start,count):
    return [dict(query_digest=f'{i:064x}',features=dict(intercept=1.,payload_bytes=float(i*1024),
                rpc_calls=float(i*2),scan_rows=100000.),ms=3.+.0002*i*1024) for i in range(start,start+count)]


def test_cost_fit_prunes_redundancy_and_rejects_unusable_models():
    fit=helpers()['fit_cost_model'](records(1,96),records(97,32))
    assert fit['accepted']
    assert 'payload_bytes' in fit['coefficients'] and 'rpc_calls' not in fit['coefficients']
    assert abs(fit['coefficients']['intercept']-3)<.01
    assert fit['evidence']['fit_queries']==96 and fit['evidence']['validation_queries']==32
    bad=records(97,32);bad[0]['ms']=float('nan')
    with pytest.raises(ValueError):helpers()['fit_cost_model'](records(1,96),bad)
    with pytest.raises(ValueError,match='overlap'):helpers()['fit_cost_model'](records(1,96),records(1,32))


def test_dataset_query_splits_and_frozen_policy_hash():
    h=helpers();pools=h['make_query_splits'](4,20261103)
    assert [len(pools[k]) for k in ('calibration','quality','fit','evaluation')]==[500,500,128,500]
    hashes=[h['query_digest'](q) for group in pools.values() for q in group]
    assert len(hashes)==len(set(hashes))
    same=h['make_query_splits'](4,20261103)
    assert all(np.array_equal(pools[k],same[k]) for k in pools)


def test_repeat_count_is_not_independent_risk_sample_count():
    fit=helpers()['fit_cost_model'](records(1,96)*3,records(97,32)*3)
    assert fit['evidence']['fit_queries']==96 and fit['evidence']['validation_queries']==32


def test_small_exact_probe_has_no_p95():
    summary=helpers()['latency_summary']([1,2,3,4,5],elapsed_seconds=1.)
    assert summary['p50_ms']==3 and summary['p95_ms'] is None and summary['throughput']==5


def test_resume_rejects_changed_manifest():
    h=helpers()
    h['validate_resume']({'source':'old','seed':1},{'source':'old','seed':1})
    with pytest.raises(ValueError,match='manifest'):h['validate_resume']({'source':'old'},{'source':'new'})


def test_representative_manifest_has_expected_bounded_work():
    cases=runpy.run_path(str(Path(__file__).resolve().parents[2]/'benchmarks/p4/adaptive_cases.py'))['matrix_cases']('representative')
    assert len(cases)==9
    assert sum(c['samples']*2*3*3 for c in cases)==32400
    assert sum(c['exact_samples']*2*3 for c in cases)==270


def test_unsupported_quality_cell_is_not_exact_success():
    h=helpers()
    evidence=h['quality_summary']([dict(query_digest='a',recall=1.,actual_mode='exact',complete=True)])
    assert not evidence['eligible'] and evidence['exact_queries']==1
