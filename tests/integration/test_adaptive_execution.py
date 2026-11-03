"""Internal auto execution over real storage RPC, before the public T6 entry."""
from copy import deepcopy
from dataclasses import replace
import json
from pathlib import Path
from types import SimpleNamespace
import time

import faiss
import numpy as np
import pytest

from test_data_cache_execution import cached, SQL
from vecstor.compute.adaptive_execution import effective_read_settings
from vecstor.compute.planning_inputs import execution_signature
from vecstor.compute.planner_observations import ObservationStore
from vecstor.compute.planner_types import DeploymentContext
from vecstor.compute.vector_execution import execute_vector
from vecstor.query.adaptive_policy import AdaptivePolicy
from vecstor.query.quality_policy import QualityCatalog
from vecstor.query.search_options import SearchOptions
from vecstor.query.statements import classify_statement
from vecstor.rpc import sql_pb2 as pb
from vecstor.rpc.common import DeadlineContext


@pytest.fixture
def adaptive(cached, adaptive_document):
    service=cached.compute
    descriptor=cached.client.inspect_published_index('items')['descriptor']
    plan=classify_statement(SQL).vector
    sample=json.loads((Path(__file__).resolve().parents[2]/'configs/quality/recall95-example.json').read_text())['profiles'][0]
    profiles=[]
    for path,pid in [('filter-first','ff'),('search-first','sf')]:
        profile=deepcopy(sample);profile.update(format_version=3,profile_id=pid,index_key=descriptor['key'])
        profile['workload'].update(metadata_sql=plan.metadata_sql,path=path,k=10)
        profile['budget'].update(nprobe_floor=1,initial_candidates=64,max_candidates=2048,max_rounds=6)
        profile['execution']=dict(device='cpu',precision='float32',faiss_version=faiss.__version__,gpu_model='not_applicable')
        profile['artifact_identity']={k:descriptor[k] for k in ('artifact_id','payload_sha256','snapshot_sha256')}
        profiles.append(profile)
    catalog=QualityCatalog.from_document(dict(format_version=3,profiles=profiles))
    observations=ObservationStore()
    context=DeploymentContext('hardware','loopback',1)
    read=effective_read_settings(plan,service.data_cache,True,'projected',False)

    def policy_for(options, winner='ann-search', matched_rows=2048, parameters=None):
        document=deepcopy(adaptive_document);work=document['workloads'][0]
        if parameters is not None:document['parameters']=parameters
        work.update(data_identity={k:v for k,v in descriptor['key'].items() if k!='build_fingerprint'},
            artifact_identity=dict(artifact_id=descriptor['artifact_id'],payload_sha256=descriptor['payload_sha256'],
                                   build_fingerprint=descriptor['key']['build_fingerprint']),
            metadata_sql=plan.metadata_sql,table_rows=2048,matched_rows=matched_rows,
            profiles={'filter-first':'ff','search-first':'sf'})
        template=deepcopy(work['models'][0]);models=[]
        for name,path,pid in [('ann-filter','filter-first','ff'),('ann-search','search-first','sf'),('exact','filter-first',None)]:
            effective=replace(options,path=path,mode='exact' if name=='exact' else 'approx',quality_profile_id=pid)
            if pid:
                effective=replace(effective,nprobe=1,candidates=64,max_candidates=2048)
            else:effective=replace(effective,candidate_data_path='separate')
            model=deepcopy(template);model.update(candidate_id=name,execution_fingerprint=execution_signature(effective,read),
                coefficients={'intercept':10. if winner==name else 100.},residual_ms=1.)
            model['statistics'].update(q=[1.],unique_fractions=[1.])
            models.append(model)
        work['models']=models
        return AdaptivePolicy.from_document(document)

    def run(options=None, *, winner='ann-search', explain=None, matched_rows=2048, mode='apply', ctx=None, policy=None):
        options=options or SearchOptions(mode='approx',path='auto')
        deadline=time.monotonic()+30
        return execute_vector(service.stub,service.index_manager.registry,
            pb.SQLRequest(database_id=cached.client.database_id,sql=SQL),plan,options,
            ctx or DeadlineContext(deadline),deadline,explain=explain,quality_catalog=catalog,
            index_source='published',data_cache=service.data_cache,metadata_projection='projected',cache_enabled=True,
            adaptive_policy=policy or policy_for(options,winner,matched_rows),observations=observations,
            deployment_context=context,adaptive_mode=mode)
    return SimpleNamespace(run=run,policy_for=policy_for,observations=observations,cached=cached,plan=plan,
                           catalog=catalog,quality_document=dict(format_version=3,profiles=profiles),read_settings=read,context=context,descriptor=descriptor)


@pytest.mark.parametrize('winner',['ann-filter','ann-search','exact'])
def test_auto_matches_selected_fixed_path_and_exact_truth(adaptive,winner):
    auto=adaptive.run(winner=winner);trace=auto['execution_plan']
    assert trace['adaptive']['executed_plan']==winner
    selected=SearchOptions(**trace['effective_options'])
    fixed=adaptive.run(selected)
    assert auto['rows']==fixed['rows']
    truth=adaptive.run(SearchOptions())
    assert auto['rows']==truth['rows']
    assert trace['adaptive']['planning_extra_storage_rpc_calls']==0
    assert trace['requested_path']=='auto' and trace['actual_path']!='auto'


def test_fixed_path_never_calls_planner_or_history(adaptive,monkeypatch):
    def forbidden(*args,**kwargs):raise AssertionError('fixed path used planner state')
    monkeypatch.setattr(adaptive.observations,'get',forbidden)
    monkeypatch.setattr(adaptive.observations,'record',forbidden)
    result=adaptive.run(SearchOptions())
    assert 'adaptive' not in result['execution_plan']


def test_explain_peeks_without_scan_search_or_observation_update(adaptive):
    result=adaptive.run(explain='plan');trace=result['execution_plan']
    assert trace['actual_mode']=='not_executed'
    assert trace['counts']['scan_metadata_calls']==trace['counts']['vectors_fetched']==0
    assert not trace['rounds'] and adaptive.observations.stats()['entries']==0


def test_analyze_and_shadow_execute_once(adaptive,monkeypatch):
    from vecstor.compute.lease_io import LeaseIO
    original=LeaseIO.open;calls=[]
    def opened(io):calls.append(1);return original(io)
    monkeypatch.setattr(LeaseIO,'open',opened)
    result=adaptive.run(explain='analyze',mode='shadow')
    assert len(calls)==1
    trace=result['execution_plan']
    assert trace['adaptive']['proposed_plan']=='ann-search'
    assert trace['adaptive']['executed_plan']=='ann-filter'
    assert trace['counts']['scan_metadata_calls']==1
    assert trace['counts']['vectors_used_for_rerank']==64


def test_estimated_zero_still_checks_storage(adaptive):
    result=adaptive.run(winner='exact',matched_rows=0)
    assert len(result['rows'])==10 and result['execution_plan']['counts']['scan_metadata_calls']==1


def test_warm_reuse_is_previous_query_evidence_not_global_occupancy(adaptive):
    cold=adaptive.run();warm=adaptive.run()
    assert cold['rows']==warm['rows']
    trace=warm['execution_plan']
    selected=next(e for e in trace['adaptive']['estimates'] if e['candidate_id']=='ann-search')
    assert selected['vectors_missed']==0 and trace['counts']['vectors_fetched']==0


def test_clear_during_query_cannot_publish_warm_reuse_hint(adaptive,monkeypatch):
    from vecstor.compute.data_reader import CachedLeaseReader
    original=CachedLeaseReader.fetch
    def clearing(reader,*args,**kwargs):
        result=original(reader,*args,**kwargs)
        reader.cache.clear(reader.io.database_id)
        return result
    monkeypatch.setattr(CachedLeaseReader,'fetch',clearing)
    adaptive.run();second=adaptive.run()
    selected=next(e for e in second['execution_plan']['adaptive']['estimates'] if e['candidate_id']=='ann-search')
    assert selected['vectors_missed']>0


def test_planner_timer_includes_adapter_import(adaptive,monkeypatch):
    import builtins
    original=builtins.__import__;delayed=[]
    def importing(name,*args,**kwargs):
        if name=='vecstor.compute.adaptive_execution' and not delayed:
            delayed.append(True);time.sleep(.01)
        return original(name,*args,**kwargs)
    monkeypatch.setattr(builtins,'__import__',importing)
    result=adaptive.run()
    assert delayed and result['execution_plan']['adaptive']['planner_ms']>=8


def test_observation_ablation_keeps_physical_cache_reads(adaptive):
    options=SearchOptions(mode='approx',path='auto')
    policy=adaptive.policy_for(options,parameters={'use_observations':False})
    cold=adaptive.run(options,policy=policy);warm=adaptive.run(options,policy=policy)
    assert cold['rows']==warm['rows']
    assert warm['execution_plan']['counts']['vectors_fetched']==0
    assert adaptive.observations.stats()['entries']==0
