from collections import OrderedDict
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from types import SimpleNamespace

import numpy as np
import pytest

from vecstor.cache.types import CacheConfig, DataScope
from vecstor.cache.vector_cache import VersionedVectorCache
from vecstor.compute.planner_observations import Observation, ObservationStore, make_reuse_observation, observation_key
from vecstor.compute.planner_types import PlanCandidate
from vecstor.compute.planner_types import DeploymentContext
from vecstor.query.search_options import SearchOptions

SCOPE = DataScope('epoch','db','table',1,'schema',4)


def test_cost_feedback_isolated_by_deployment_and_concurrency_bucket():
    a=context();b=context();candidate=a.candidates[0]
    a.context=DeploymentContext('hardware','loopback',1)
    b.context=DeploymentContext('hardware','loopback',4)
    assert observation_key(a,'cost',candidate)!=observation_key(b,'cost',candidate)
    assert observation_key(a,'reuse',candidate)==observation_key(b,'reuse',candidate)
    b.context=replace(b.context,active_vector_requests=2)
    c=context();c.context=DeploymentContext('hardware','loopback',4)
    assert observation_key(b,'cost',candidate)==observation_key(c,'cost',candidate)
    b.context=replace(a.context,network_class='remote')
    assert observation_key(a,'cost',candidate)!=observation_key(b,'cost',candidate)


def fill(cache, ids):
    ids = tuple(ids)
    with cache.batch(SCOPE, ids, lambda: None) as batch:
        batch.finish(ids, batch.missing_ids, np.zeros((len(batch.missing_ids),4),dtype='<f4'),
                     admit=True,check=lambda:None)


class NoTraversal(OrderedDict):
    def __iter__(self): raise AssertionError('traversal')
    def items(self): raise AssertionError('traversal')
    def values(self): raise AssertionError('traversal')
    def keys(self): raise AssertionError('traversal')


@pytest.mark.parametrize('size', [0, 2, 65535])
def test_scope_snapshot_without_entry_or_scope_iteration(size):
    cache = VersionedVectorCache(CacheConfig(max_bytes=64*1024**2))
    for start in range(0,size,1024): fill(cache,range(start,min(size,start+1024)))
    cache._entries = NoTraversal(cache._entries); cache._scopes = NoTraversal(cache._scopes)
    snapshot = cache.scope_snapshot(SCOPE)
    assert snapshot['scope_entry_count'] == size
    assert snapshot['scope_payload_bytes'] == size*16
    assert cache.scope_snapshot(replace(SCOPE,database_id='other'))['scope_entry_count'] == 0
    snapshot['scope_entry_count'] = -1
    assert cache.scope_snapshot(SCOPE)['scope_entry_count'] == size


def test_loss_generation_tracks_evict_clear_close_not_hits():
    cache = VersionedVectorCache(CacheConfig(max_bytes=10000,max_entries=1))
    fill(cache,[1]); first=cache.scope_snapshot(SCOPE)
    fill(cache,[1]); assert cache.scope_snapshot(SCOPE)['loss_generation']==first['loss_generation']
    fill(cache,[2]); second=cache.scope_snapshot(SCOPE)
    assert second['loss_generation']>first['loss_generation']
    cache.clear('db'); third=cache.scope_snapshot(SCOPE)
    assert third['loss_generation']>second['loss_generation']
    cache.close(); assert cache.scope_snapshot(SCOPE)['loss_generation']>third['loss_generation']


def test_history_bounds_ttl_peek_and_concurrent_eviction():
    store = ObservationStore()
    with ThreadPoolExecutor(4) as pool:
        list(pool.map(lambda n:store.record(Observation(('q',str(n)),{'matched_rows':n}),now=0),range(257)))
    assert store.stats()['entries']==256 and store.stats()['accounted_bytes']==2*1024**2
    tiny=ObservationStore(max_entries=2)
    tiny.record(Observation(('a',),{'matched_rows':1}),now=0)
    tiny.record(Observation(('b',),{'matched_rows':2}),now=0)
    assert tiny.peek(('a',),now=299).values['matched_rows']==1
    tiny.record(Observation(('c',),{'matched_rows':3}),now=299)
    assert tiny.peek(('a',),now=299) is None  # peek didn't promote LRU
    assert tiny.peek(('b',),now=300) is None
    assert tiny.stats()['entries']==2  # EXPLAIN doesn't delete expired entries
    assert tiny.get(('b',),now=300) is None and tiny.stats()['entries']==1
    with pytest.raises(ValueError):tiny.record(Observation(('d',),{'candidate_ids':[1,2]}),now=0)
    with pytest.raises(ValueError):tiny.record(Observation(('x'*9000,),{'warm':True}),now=0)


def test_cardinality_conflict_and_budget_failure_are_not_latency_samples():
    store=ObservationStore()
    store.record_cardinality(('m',),100,now=0)
    store.record_cardinality(('m',),101,now=1)
    assert store.peek(('m',),now=2).values == {'matched_rows':None,'conflict':True}
    store.note_budget_failure(('risk',),now=0)
    assert store.peek(('risk',),now=1).values == {'budget_failure':True}


def context():
    candidate=PlanCandidate('ann-search','approx','search-first',SearchOptions(mode='approx',path='search-first'),{},'c'*64)
    return SimpleNamespace(scope=SCOPE,artifact_identity={},query_hash='a'*64,metadata_sql_hash='b'*64,k=10,candidates=(candidate,))


def test_reuse_requires_same_query_budget_path_and_generations():
    inputs=context()
    snapshot=dict(scope_entry_count=100,admission_generation=0,loss_generation=0)
    trace=dict(actual_mode='approx',actual_path='search-first',fallback_reason=None,
        counts={'vectors_used_for_rerank':10},data_cache=dict(effective='on',lookup_rows=100,
            vectors_served_from_cache=4,admitted_rows=6,duplicate_inserts=0,bypass_rows=0,evictions=0))
    observation=make_reuse_observation(inputs,trace,snapshot,snapshot)
    assert observation and observation.values['warm']
    assert observation.key==observation_key(inputs,'reuse',inputs.candidates[0])
    inputs.query_hash='d'*64
    assert observation.key!=observation_key(inputs,'reuse',inputs.candidates[0])
    inputs.candidates=(replace(inputs.candidates[0],execution_fingerprint='e'*64),)
    assert observation.key!=observation_key(inputs,'reuse',inputs.candidates[0])
    for key in ('admission_generation','loss_generation'):
        assert make_reuse_observation(inputs,trace,snapshot,dict(snapshot,**{key:1})) is None
    trace['data_cache']['admitted_rows']=0  # scan not admitted / insufficient matched vectors
    assert make_reuse_observation(inputs,trace,snapshot,snapshot) is None
    trace['data_cache'].update(admitted_rows=6,bypass_rows=1)
    assert make_reuse_observation(inputs,trace,snapshot,snapshot) is None


def test_record_many_rollback_restores_values_and_evictions():
    store=ObservationStore(max_entries=2)
    store.record(Observation(('a',),{'matched_rows':1}),now=0)
    store.record(Observation(('b',),{'matched_rows':2}),now=0)
    calls=[]
    def check():
        calls.append(1)
        if len(calls)==2:raise RuntimeError('cancel')
    with pytest.raises(RuntimeError,match='cancel'):
        store.record_many([Observation(('a',),{'matched_rows':3}),Observation(('c',),{'warm':True})],now=1,check=check)
    assert store.stats()['entries']==2
    assert store.peek(('a',),now=2).values['matched_rows']==1
    assert store.peek(('b',),now=2).values['matched_rows']==2
    assert store.peek(('c',),now=2) is None


def test_atomic_cost_feedback_rollback_restores_value_and_expiry():
    store=ObservationStore();key=('cost','scope')
    store.record_cost(key,100.,now=0)
    calls=[]
    def check():
        calls.append(1)
        if len(calls)==2:raise RuntimeError('cancel')
    with pytest.raises(RuntimeError,match='cancel'):
        store.record_many([Observation(key,{'bias_sample_ms':200.})],now=1,check=check)
    assert store.peek(key,now=2).values=={'bias_ms':20.,'residual_ms':20.}
    assert store.peek(key,now=300) is None
