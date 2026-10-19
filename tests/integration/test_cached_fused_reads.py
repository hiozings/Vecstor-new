from dataclasses import replace

import numpy as np
import pytest

from test_data_cache_execution import cached,SQL
from vecstor.cache.types import CacheConfig
from vecstor.compute.lease_io import LeaseIO
from vecstor.rpc import sql_pb2 as pb


def enable(env):
    env.compute.fused_candidates=True
    env.compute.metadata_projection='projected'
    env.client.search_options=replace(env.client.search_options,candidate_data_path='fused')


def test_fused_cold_warm_partial_only_returns_missing_vectors(cached):
    enable(cached)
    cold=cached.client.execute(SQL);warm=cached.client.execute(SQL)
    assert cold['rows']==warm['rows']
    assert cold['execution_plan']['counts']['vectors_fetched']==64
    assert warm['execution_plan']['counts']['vectors_fetched']==0
    assert warm['execution_plan']['data_cache']['vectors_served_from_cache']==64
    assert warm['execution_plan']['counts']['rpc_calls']==3
    assert warm['execution_plan']['metadata_requested_fields']==['id']
    cached.client.search_options=replace(cached.client.search_options,candidates=128)
    partial=cached.client.execute(SQL)
    assert partial['execution_plan']['counts']['vectors_fetched']==64
    assert partial['execution_plan']['data_cache']['hit_rows']==64
    assert partial['rows']==cold['rows']


def test_fused_filtered_cache_hits_not_counted_as_served(cached):
    enable(cached);cached.client.execute(SQL)
    result=cached.client.execute(SQL.replace('value>=0','value<150'))
    trace=result['execution_plan'];stats=trace['data_cache']
    assert stats['hit_rows']>stats['vectors_served_from_cache']
    assert stats['vectors_served_from_cache']+trace['counts']['vectors_fetched']==trace['counts']['vectors_used_for_rerank']
    assert len(trace['rounds'])>1


def test_fused_workspace_bypass_preserves_requested_path(cached):
    from contextlib import ExitStack
    from vecstor.cache.types import DataScope
    enable(cached)
    with ExitStack() as held:
        scope=DataScope('reserved','db','table',1,'schema',4)
        for _ in range(4):held.enter_context(cached.compute.data_cache.batch(scope,(1,),lambda:None))
        result=cached.client.execute(SQL)['execution_plan']
    assert result['candidate_data_path']=='fused'
    assert result['data_cache']['bypass_rows']==64
    assert result['counts']['vectors_fetched']==64


def test_projection_star_and_aliases_keep_all_output_fields(cached):
    cached.client.reset_search()
    full=cached.client.execute(SQL.replace('SELECT id','SELECT *'))
    cached.compute.metadata_projection='projected'
    projected=cached.client.execute(SQL.replace('SELECT id','SELECT *'))
    assert projected['rows']==full['rows'] and projected['columns']==full['columns']
    aliased=cached.client.execute(SQL.replace('SELECT id','SELECT id AS identity, value AS amount'))
    assert aliased['execution_plan']['metadata_requested_fields']==['id','value']
    assert [c['name'] for c in aliased['columns']]==['identity','amount']


def test_selective_response_error_not_hidden_by_warm_cache(cached,monkeypatch):
    enable(cached);cached.client.execute(SQL)
    original=LeaseIO.call
    def corrupt(self,method,request):
        response=original(self,method,request)
        if isinstance(request,pb.ProjectedCandidateRequest):
            response.rows[0].id=99999
        return response
    monkeypatch.setattr(LeaseIO,'call',corrupt)
    with pytest.raises(ValueError,match='DATA_LOSS'):cached.client.execute(SQL)
