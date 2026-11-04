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


def test_selection_accuracy_is_a_required_performance_gate():
    assess=helpers()['assess_performance']
    runs=[]
    for repeat in (1,2,3):
        for variant,latency in [('filter-first',100.),('search-first',50.),('auto',50.)]:
            phases=[dict(name=p,requests=200,p50_ms=latency,p95_ms=latency*1.2,
                         planner_feedback_p95_ms=.1,decisions={'filter-first':200,'search-first':0},
                         model_coverage={'ann-filter':200,'ann-search':200}) for p in ('cold','warm')]
            runs.append(dict(case=0,variant=variant,repeat=repeat,phases=phases))
    guards,passed=assess(runs,[0],quality_certified=True)
    assert not passed and all(g['selection_rate']==0 for g in guards)
    for r in runs:
        if r['variant']=='auto':
            for p in r['phases']:p['decisions']={'filter-first':0,'search-first':200}
    assert assess(runs,[0],quality_certified=True)[1]
    runs[-1]['phases'][0]['model_coverage']['ann-search']=0
    assert not assess(runs,[0],quality_certified=True)[1]


def test_correctness_short_samples_do_not_compute_p95():
    runs=[dict(case=0,variant=v,repeat=r,phases=[dict(name=p,requests=2,p50_ms=10.,p95_ms=None,
         planner_feedback_p95_ms=.1,decisions={},model_coverage={}) for p in ('cold','warm')])
         for r in (1,2,3) for v in ('filter-first','search-first','auto')]
    guards,passed=helpers()['assess_performance'](runs,[0],quality_certified=True)
    assert not passed and all(g['status']=='insufficient_evidence' for g in guards)


def test_validation_wire_bytes_do_not_fit_preprocessing(monkeypatch):
    import sys
    directory=Path(__file__).resolve().parents[2]/'benchmarks/p4'
    monkeypatch.syspath_prepend(str(directory))
    evidence=runpy.run_path(str(directory/'adaptive_evidence.py'))
    function=evidence['fit_training_wire_layout']
    def row(i,byte_count):
        return dict(query_id=i,plan={'counts':dict(vectors_fetched=1,rpc_calls=4,
            metadata_rows_returned=10,candidates_checked=10,rpc_payload_bytes=byte_count)})
    first=function([row(0,1000),row(1,2000),row(96,3000)],4,{},[96])
    second=function([row(0,1000),row(1,2000),row(96,999999)],4,{},[96])
    assert first==second
