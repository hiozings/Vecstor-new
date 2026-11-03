from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from dataclasses import replace
from threading import Barrier, Event

import pytest

from test_adaptive_execution import adaptive, cached, SQL
from vecstor.compute import adaptive_context
from vecstor.compute.service import create_compute_server
from vecstor.query.search_options import SearchOptions
from vecstor.rpc.sql_client import SessionClient


@pytest.fixture
def public_adaptive(adaptive,monkeypatch):
    monkeypatch.setattr(adaptive_context,'deployment_fingerprint',lambda:'hardware')
    @contextmanager
    def create(mode='apply',network='loopback',policy=None):
        options=SearchOptions(mode='approx',path='auto')
        policy=policy or adaptive.policy_for(options)
        server,port,backend=create_compute_server(adaptive.cached.storage_address,'127.0.0.1:0',timeout=30,
            index_source='published',data_cache_config=adaptive.cached.compute.data_cache.config,
            metadata_projection='projected',quality_document=adaptive.quality_document,
            adaptive_policy=policy,adaptive_mode=mode,adaptive_network_class=network)
        server.start();client=SessionClient(f'127.0.0.1:{port}',timeout=30)
        try:
            client.open_database('demo');client.load_index('items')
            client.search_options=options
            yield client,backend.sql
        finally:client.close();server.stop(0).wait();backend.close()
    return create


def test_session_auto_checks_capability_without_settings(public_adaptive):
    with public_adaptive() as (client,service):
        client.reset_search();client.set_search('mode','approx');client.set_search('path','auto')
        result=client.execute(SQL);trace=result['execution_plan']
        assert trace['requested_path']=='auto' and trace['adaptive']['executed_plan']=='ann-search'
        assert client.get_search_capabilities()['adaptive_planning_version']==1
        assert service.active_vectors.value==0


def test_old_server_or_restarted_instance_cannot_fake_auto_ack(public_adaptive,monkeypatch):
    with public_adaptive() as (client,service):
        caps=client.get_search_capabilities();caps.pop('adaptive_planning_version')
        monkeypatch.setattr(client,'_query_capabilities',lambda:caps)
        with pytest.raises(ValueError,match='adaptive'):client.execute(SQL)
        assert service.observations.stats()['entries']==0


@pytest.mark.parametrize('change',['missing','policy','mode'])
def test_missing_adaptive_completion_is_error(public_adaptive,monkeypatch,change):
    from vecstor.compute import sql_service
    original=sql_service.execute_vector
    def changed(*args,**kwargs):
        result=original(*args,**kwargs)
        adaptive=result['execution_plan']['adaptive']
        if change=='missing':result['execution_plan'].pop('adaptive')
        elif change=='policy':adaptive['policy_id']='wrong'
        else:adaptive['mode']='shadow'
        return result
    with public_adaptive() as (client,_):
        monkeypatch.setattr(sql_service,'execute_vector',changed)
        with pytest.raises(ValueError,match='adaptive'):client.execute(SQL)
        assert client._query_caps is None


def test_ordinary_sql_does_not_require_adaptive_ack(public_adaptive):
    with public_adaptive() as (client,_):
        assert client.execute('SELECT COUNT(*) FROM items')['rows']==[[2048]]


def test_planned_exact_keeps_requested_quality_ack(public_adaptive,adaptive):
    options=SearchOptions(mode='approx',path='auto',quality_profile_id='sf')
    policy=adaptive.policy_for(options,winner='exact')
    with public_adaptive(policy=policy) as (client,_):
        client.search_options=options
        trace=client.execute(SQL)['execution_plan']
        assert trace['actual_mode']=='exact'
        assert trace['quality_profile']['requested_id']=='sf'
        assert trace['adaptive']['planned_exact_choice']


def test_context_mismatch_keeps_quality_but_disables_cost(public_adaptive):
    with public_adaptive(network='remote') as (client,_):
        trace=client.execute(SQL)['execution_plan']
        assert trace['adaptive']['cost_reason']=='deployment_context_mismatch'
        assert trace['adaptive']['executed_plan']=='ann-filter'
        assert trace['quality_profile']['state']=='applied'


def test_missing_network_label_and_bad_policy_fail_startup(public_adaptive):
    with pytest.raises(ValueError,match='network'):
        with public_adaptive(network=None):pass
    with pytest.raises(ValueError,match='policy'):
        with public_adaptive(policy=object()):pass


def test_active_count_includes_fixed_and_returns_to_zero_on_all_exits(public_adaptive,monkeypatch):
    from vecstor.compute.lease_io import LeaseIO
    barrier=Barrier(3);release=Event();seen=[];original=LeaseIO.open
    with public_adaptive() as (client,service):
        snapshot=service.active_vectors.snapshot
        def observed(*args,**kwargs):
            result=snapshot(*args,**kwargs);seen.append(result.active_vector_requests);release.set();return result
        monkeypatch.setattr(service.active_vectors,'snapshot',observed)
        def opened(io):
            result=original(io);barrier.wait(10)
            if io.trace['requested_path']!='auto':assert release.wait(10)
            return result
        monkeypatch.setattr(LeaseIO,'open',opened)
        def query(path):
            with SessionClient(client.address,timeout=30) as worker:
                worker.open_database('demo');worker.search_options=SearchOptions(mode='approx',path=path,nprobe=1)
                return worker.execute(SQL)
        with ThreadPoolExecutor(3) as pool:
            results=list(pool.map(query,['filter-first','search-first','auto']))
        assert seen==[3] and service.active_vectors.value==0
        assert all(len(result['rows'])==10 for result in results)


def test_nul_network_label_is_rejected(public_adaptive):
    with pytest.raises(ValueError,match='network'):
        with public_adaptive(network='loop\x00back'):pass


def test_changed_server_instance_rejects_stale_adaptive_confirmation(public_adaptive,monkeypatch):
    from vecstor.compute import sql_service
    original=sql_service.execute_vector
    with public_adaptive() as (client,service):
        client.execute(SQL)  # Prime the real endpoint-scoped capability cache.
        # Simulate another server instance at the same endpoint after successful use.
        assert client._query_caps is not None
        service.server_instance_id='restarted'
        with pytest.raises(ValueError,match='adaptive'):client.execute(SQL)
        assert client._query_caps is None


def test_sql_auto_override_preserves_session(public_adaptive):
    with public_adaptive() as (client,_):
        client.reset_search()
        result=client.execute(SQL+" SETTINGS mode='approx',path='auto'")
        assert result['execution_plan']['requested_path']=='auto'
        assert client.search_options==SearchOptions()


def test_exact_auto_without_policy_keeps_exact_semantics(cached):
    cached.client.search_options=SearchOptions(path='auto')
    result=cached.client.execute(SQL)
    trace=result['execution_plan']
    assert trace['actual_mode']=='exact' and trace['adaptive']['policy_id'] is None
    assert trace['adaptive']['decision_reason']=='exact_only'


def test_failed_query_and_explain_release_active_count(public_adaptive):
    with public_adaptive() as (client,service):
        client.execute('EXPLAIN '+SQL)
        assert service.active_vectors.value==0 and service.observations.stats()['entries']==0
        with pytest.raises(ValueError):client.execute(SQL.replace('[0,0,0,0]','[0,0]'))
        assert service.active_vectors.value==0 and service.observations.stats()['entries']==0
