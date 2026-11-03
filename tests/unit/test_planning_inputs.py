from dataclasses import asdict, replace
from copy import deepcopy
import json
from pathlib import Path
from types import SimpleNamespace

import faiss
import pytest

from vecstor.compute.planning_inputs import qualify_candidates, collect_planning_inputs
from vecstor.compute.planner_types import DeploymentContext
from vecstor.compute.planner_observations import ObservationStore
from vecstor.query.adaptive_policy import PolicyMatch
from vecstor.query.quality_policy import QualityCatalog
from vecstor.query.search_options import SearchOptions
from vecstor.query.statements import classify_statement
from vecstor.index.artifact import ArtifactError


@pytest.fixture
def qualification(cpu_artifact):
    descriptor=cpu_artifact[0]
    sql='SELECT id FROM items WHERE value<2000 ORDER BY vector_distance(vector,[0.5,0.5]) LIMIT 10'
    plan=classify_statement(sql).vector
    source=json.loads((Path(__file__).resolve().parents[2]/'configs/quality/recall95-example.json').read_text())['profiles'][0]
    profiles=[]
    for path,pid,initial in [('filter-first','ff',64),('search-first','sf',128)]:
        p=deepcopy(source);p.update(format_version=3,profile_id=pid,index_key=asdict(descriptor.key))
        p['workload'].update(metadata_sql=plan.metadata_sql,path=path)
        p['budget'].update(nprobe_floor=1,initial_candidates=initial,max_candidates=2048)
        p['execution']=dict(device='cpu',precision='float32',faiss_version=faiss.__version__,gpu_model='not_applicable')
        p['artifact_identity']={k:getattr(descriptor,k) for k in ('artifact_id','payload_sha256','snapshot_sha256')}
        profiles.append(p)
    catalog=QualityCatalog.from_document(dict(format_version=3,profiles=profiles))
    lease=SimpleNamespace(**{k:v for k,v in asdict(descriptor.key).items() if k not in ('metric','build_fingerprint')},
        storage_epoch='epoch',row_count=2048,published_index_state='ready',HasField=lambda name:True)
    entry=SimpleNamespace(key=descriptor.key,descriptor=descriptor,handle=SimpleNamespace(nlist=1,row_count=2048),statistics={})
    match=PolicyMatch(matched_rows=100,table_rows=2048,profiles={'filter-first':'ff','search-first':'sf'},cost_reason=None)
    return SimpleNamespace(plan=plan,lease=lease,entry=entry,catalog=catalog,match=match)


def qualify(q, **overrides):
    options=SearchOptions(mode='approx',path='auto',**overrides)
    return qualify_candidates(q.plan,options,q.lease,q.entry,q.catalog,q.match)


def test_exact_auto_never_qualifies_ann(qualification):
    q=qualification
    result=qualify_candidates(q.plan,SearchOptions(path='auto'),q.lease,None,None,q.match)
    assert [(c.mode,c.path) for c in result]==[('exact','filter-first')]


def test_each_path_resolves_its_own_profile(qualification):
    candidates=qualify(qualification)
    assert [c.candidate_id for c in candidates]==['ann-filter','ann-search','exact']
    assert [c.options.candidates for c in candidates[:2]]==[64,128]
    assert all(c.options.path!='auto' for c in candidates)
    assert candidates[0].quality_description['requested_id']=='ff'
    assert candidates[1].quality_description['requested_id']=='sf'


def test_error_fallback_and_fused_prune_exact(qualification):
    assert all(c.mode=='approx' for c in qualify(qualification,fallback='error'))
    assert [c.path for c in qualify(qualification,candidate_data_path='fused')]==['search-first']


def test_explicit_profile_budget_conflict_is_not_cost_fallback(qualification):
    with pytest.raises(ValueError,match='budget conflict'):
        qualify(qualification,quality_profile_id='sf',candidates=64)
    explicit=qualify(qualification,quality_profile_id='sf')
    assert [c.candidate_id for c in explicit]==['ann-search','exact']
    assert explicit[-1].options.quality_profile_id is None
    assert explicit[-1].quality_description['requested_id']=='sf'


def test_optional_profile_conflict_excludes_only_that_candidate(qualification):
    assert [c.candidate_id for c in qualify(qualification,candidates=64)]==['ann-filter','exact']
    with pytest.raises(ValueError,match='budget'):
        qualify(qualification,candidates=1)


def test_unknown_cardinality_does_not_scan_or_count(qualification):
    q=qualification;store=ObservationStore()
    candidates=qualify(q)
    inputs=collect_planning_inputs(q.plan,q.lease,q.entry,candidates,
        replace(q.match,matched_rows=None),None,{},store,DeploymentContext('hw','local',1),1000.,explain=True)
    assert inputs.statistics['matched_rows'] is None
    assert inputs.statistics['cardinality_source']=='unknown'
    assert store.stats()['entries']==0


def test_sf_matches_do_not_become_table_cardinality(qualification):
    q=qualification;store=ObservationStore()
    inputs=collect_planning_inputs(q.plan,q.lease,q.entry,qualify(q),q.match,None,{},store,
                                  DeploymentContext('hw','local',1),1000.)
    assert inputs.statistics['matched_rows']==100
    assert inputs.statistics['cardinality_source']=='offline'
    assert inputs.statistics['nlist']==1


def test_corrupt_artifact_is_not_optimizer_downgrade(qualification):
    q=qualification;q.lease.published_index_state='corrupt'
    with pytest.raises(ArtifactError):qualify(q)


def test_missing_index_preserves_exact_fallback_even_with_requested_fused(qualification):
    q=qualification;q.entry=None
    result=qualify(q,candidate_data_path='fused')
    assert [c.mode for c in result]==['exact']


def test_observed_cardinality_conflict_disables_offline_shortcut(qualification):
    from vecstor.compute.planner_observations import observation_key
    q=qualification;store=ObservationStore()
    def collect():
        return collect_planning_inputs(q.plan,q.lease,q.entry,qualify(q),q.match,None,{},store,
                                       DeploymentContext('hw','local',1),1000.)
    first=collect();key=observation_key(first,'cardinality')
    store.record_cardinality(key,100)
    assert collect().statistics['cardinality_source']=='observed'
    store.record_cardinality(key,101)
    conflicted=collect()
    assert conflicted.statistics['matched_rows'] is None
    assert conflicted.statistics['cardinality_source']=='conflict'
