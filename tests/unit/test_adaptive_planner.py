from copy import deepcopy
from dataclasses import replace
import math

import pytest

from vecstor.cache.types import DataScope
from vecstor.compute.adaptive_planner import choose_plan, estimate_candidate
from vecstor.compute.planner_types import DeploymentContext, PlanCandidate, PlanningInputs, plain
from vecstor.query.search_options import SearchOptions
from vecstor.rpc.common import RemoteError


def inputs(document, *, matched=1000, q=.5, fallback='error', coefficients=None):
    candidates=[];models=[]
    for name,path,fp,price in [('ann-filter','filter-first','1'*64,30.),('ann-search','search-first','2'*64,10.)]:
        options=SearchOptions(mode='approx',path=path,nprobe=1,candidates=64,max_candidates=256,max_rounds=3,fallback=fallback)
        candidates.append(PlanCandidate(name,'approx',path,options,{},fp))
        model=deepcopy(document['workloads'][0]['models'][0])
        model.update(candidate_id=name,execution_fingerprint=fp,coefficients={'intercept':price})
        if coefficients:model['coefficients']=coefficients
        model['statistics'].update(q=[1. if path=='filter-first' else q],unique_fractions=[1.])
        models.append(model)
    if fallback=='exact':
        candidates.append(PlanCandidate('exact','exact','filter-first',SearchOptions(),None,'3'*64))
        model=deepcopy(models[0]);model.update(candidate_id='exact',execution_fingerprint='3'*64,coefficients={'intercept':1000.})
        models.append(model)
    statistics=dict(matched_rows=matched,nlist=1,models=models,warm={},risk_unknown={},
                    read_settings=dict(cache_enabled=True,fused_candidates=False,scan_admission=False))
    snapshot=dict(scope_entry_count=1000,max_bytes=64*1024**2,active_workspaces=0,workspace_slots=4)
    return PlanningInputs(DataScope('e','db','t',1,'s',128),{},100000,128,10,'a'*64,'b'*64,
        tuple(candidates),'ann-filter',statistics,snapshot,DeploymentContext('hw','local',1),'policy','c'*64,10000.)


def test_low_selectivity_high_candidate_risk_prefers_filter(adaptive_document):
    data=inputs(adaptive_document,matched=100,q=.001)
    assert choose_plan(data,None).executed_id=='ann-filter'


def test_high_selectivity_prefers_search(adaptive_document):
    assert choose_plan(inputs(adaptive_document,matched=100000,q=1),None).executed_id=='ann-search'


def test_equal_s_opposite_q_changes_choice(adaptive_document):
    a=inputs(adaptive_document,matched=1000,q=.5)
    b=inputs(adaptive_document,matched=1000,q=.0001)
    assert choose_plan(a,None).executed_id!=choose_plan(b,None).executed_id


def test_cache_reuse_reduces_miss_bytes_not_filter_rpcs(adaptive_document):
    data=inputs(adaptive_document)
    cold=estimate_candidate(data.candidates[1],data,None)
    warm=replace(data,statistics=dict(data.statistics,warm={'ann-search':True}))
    hot=estimate_candidate(warm.candidates[1],warm,None)
    assert hot.vectors_missed==0 and cold.vectors_missed>0
    assert hot.components['filter_rpc_calls']==cold.components['filter_rpc_calls']>0
    assert hot.rpc_calls<cold.rpc_calls and hot.payload_bytes<cold.payload_bytes
    busy=replace(warm,cache_snapshot=dict(warm.cache_snapshot,active_workspaces=4))
    assert estimate_candidate(busy.candidates[1],busy,None).vectors_missed>0


def test_partial_misses_do_not_use_global_ceil_as_exact_rpc_count(adaptive_document):
    data=inputs(adaptive_document,matched=200,q=.002)
    candidate=replace(data.candidates[1],options=replace(data.candidates[1].options,candidates=2048,max_candidates=4096))
    estimate=estimate_candidate(candidate,data,None)
    assert estimate.components['vector_rpc_calls']>math.ceil(estimate.vectors_missed/1024)


def test_rpc_wait_and_nested_faiss_times_are_not_double_counted(adaptive_document):
    data=inputs(adaptive_document)
    candidate=data.candidates[1]
    # No opaque measured total may be added on top of the disjoint feature model.
    changed=replace(data,statistics=dict(data.statistics,rpc_wait_ms=1e9,faiss_search_ms=1e9,native_search_ms=1e9))
    assert estimate_candidate(candidate,data,None).mean_ms==estimate_candidate(candidate,changed,None).mean_ms


def test_error_fallback_risk_tier_precedes_latency(adaptive_document):
    data=inputs(adaptive_document)
    models=[plain(m) for m in data.statistics['models']]
    models[1]['statistics']=dict(models[1]['statistics'],failure_count=50)
    risky=replace(data,statistics=dict(data.statistics,models=models))
    assert choose_plan(risky,None).executed_id=='ann-filter'
    unknown=replace(data,statistics=dict(data.statistics,risk_unknown={'ann-filter':True,'ann-search':True}))
    decision=choose_plan(unknown,None)
    assert decision.executed_id=='ann-filter' and decision.reason=='risk_unknown_or_high'


def test_explicit_budget_and_small_filtered_exact_branch_are_modelled(adaptive_document):
    data=inputs(adaptive_document,matched=100,fallback='exact')
    estimate=estimate_candidate(data.candidates[0],data,None)
    assert estimate.vectors_reranked==100
    assert estimate.components['predicted_mode']=='exact'
    data=inputs(adaptive_document,matched=100000,q=.0001)
    estimate=estimate_candidate(data.candidates[1],data,None)
    assert estimate.components['unique_candidates']<=256
    assert len(estimate.components['rounds'])<=3


def test_uncertain_interval_uses_reference(adaptive_document):
    data=inputs(adaptive_document)
    models=[dict(m,residual_ms=100.) for m in data.statistics['models']]
    decision=choose_plan(replace(data,statistics=dict(data.statistics,models=models)),None)
    assert decision.executed_id=='ann-filter' and decision.reason=='uncertain_or_tie'


def test_deadline_and_memory_exclusions(adaptive_document):
    data=inputs(adaptive_document,matched=100000,q=1)
    with pytest.raises(RemoteError):choose_plan(replace(data,remaining_ms=0),None)
    models=[dict(m,statistics=dict(m['statistics'],selector_bytes_per_row=10000.)) for m in data.statistics['models']]
    decision=choose_plan(replace(data,statistics=dict(data.statistics,models=models)),None)
    assert not decision.estimates[0].eligible
    assert decision.executed_id=='ann-search'


def test_shadow_only_changes_executed_id(adaptive_document):
    data=inputs(adaptive_document)
    apply=choose_plan(data,None);shadow=choose_plan(data,None,mode='shadow')
    assert shadow.proposed_id==apply.proposed_id=='ann-search'
    assert shadow.executed_id==shadow.reference_id=='ann-filter'


def test_unknown_cost_never_becomes_zero_ms(adaptive_document):
    data=inputs(adaptive_document)
    decision=choose_plan(replace(data,statistics=dict(data.statistics,models=())),None)
    assert decision.executed_id=='ann-filter'
    assert all(e.mean_ms is None for e in decision.estimates)


def test_rejected_cost_does_not_erase_valid_failure_evidence(adaptive_document):
    data=inputs(adaptive_document)
    models=[plain(m) for m in data.statistics['models']]
    models[0]['cost_usable']=False
    changed=replace(data,statistics=dict(data.statistics,models=models))
    estimate=estimate_candidate(changed.candidates[0],changed,None)
    assert estimate.mean_ms is None
    assert estimate.failure_upper<.01
    assert choose_plan(changed,None).executed_id=='ann-filter'
    failed=replace(changed,statistics=dict(changed.statistics,risk_unknown={'ann-filter':True}))
    assert estimate_candidate(failed.candidates[0],failed,None).failure_upper is None


def test_failed_cost_validation_preserves_risk_without_publishing_cost(adaptive_document):
    data=inputs(adaptive_document)
    models=[plain(m) for m in data.statistics['models']]
    models[1]['evidence']['p90_relative_error']=.9
    changed=replace(data,statistics=dict(data.statistics,models=models))
    estimate=estimate_candidate(changed.candidates[1],changed,None)
    assert estimate.mean_ms is None
    assert estimate.failure_upper<.01


def test_predicted_fallback_adds_scan_without_second_read_lease(adaptive_document):
    data=inputs(adaptive_document,matched=1000,q=.0001,fallback='exact')
    estimate=estimate_candidate(data.candidates[1],data,None)
    assert estimate.components['forecast_exhausted']
    assert estimate.vectors_reranked>=1000
    assert estimate.components['fallback_rpc_calls']==2  # one scan and one fetch, no Begin/End
    assert estimate.rpc_calls==estimate.components['base_rpc_calls']+2


def test_forecasts_and_choices_are_bounded_and_deterministic(adaptive_document):
    import random
    rng=random.Random(20261103)
    for _ in range(40):
        data=inputs(adaptive_document,matched=rng.randrange(1,100001),q=rng.random())
        decision=choose_plan(data,None)
        assert decision==choose_plan(data,None)
        assert decision.executed_id in {c.candidate_id for c in data.candidates}
        for estimate in decision.estimates:
            assert 0<=estimate.vectors_missed<=estimate.vectors_reranked
            assert 0<=estimate.components['unique_candidates']<=256
            assert len(estimate.components['rounds'])<=3
            assert 0<=estimate.lower_ms<=estimate.mean_ms<=estimate.upper_ms


def test_exact_fallback_forecast_is_reused_only_within_one_decision(adaptive_document,monkeypatch):
    from vecstor.compute import adaptive_planner
    original=adaptive_planner.forecast_features;calls=[]
    def counted(candidate,*args,**kwargs):
        if candidate.mode=='exact':calls.append(kwargs.get('common_calls',2))
        return original(candidate,*args,**kwargs)
    monkeypatch.setattr(adaptive_planner,'forecast_features',counted)
    data=inputs(adaptive_document,matched=1000,fallback='exact')
    choose_plan(data,None)
    assert sorted(calls)==[0,2]
    choose_plan(data,None)
    assert sorted(calls)==[0,0,2,2]


def test_cost_input_ablations_do_not_change_candidate_options(adaptive_document):
    from types import SimpleNamespace
    from vecstor.query.adaptive_policy import DEFAULT_PARAMETERS
    data=inputs(adaptive_document,coefficients={'intercept':1.,'payload_bytes':.001,'rpc_calls':1.})
    candidate=data.candidates[1]
    warm=replace(data,statistics=dict(data.statistics,warm={'ann-search':True}))
    disabled=lambda **values:SimpleNamespace(parameters=dict(DEFAULT_PARAMETERS,**values))
    assert estimate_candidate(candidate,warm,disabled(use_cache_evidence=False)).vectors_missed>0
    omitted=estimate_candidate(candidate,data,disabled(include_transfer_cost=False))
    assert omitted.mean_ms==1. and omitted.rpc_calls>0 and omitted.payload_bytes>0
    independent=estimate_candidate(candidate,data,disabled(use_candidate_correlation=False))
    assert independent.vectors_reranked!=estimate_candidate(candidate,data,None).vectors_reranked
    assert candidate.options==data.candidates[1].options
