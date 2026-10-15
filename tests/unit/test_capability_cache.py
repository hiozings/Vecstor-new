import pytest
from vecstor.rpc.sql_client import SessionClient
from vecstor.rpc.sql_codec import encode_result
from vecstor.query.search_options import SearchOptions

SQL='SELECT id FROM items ORDER BY vector_distance(vector,[0,0]) LIMIT 1'


def setup_client(monkeypatch,cache=True):
    c=SessionClient('127.0.0.1:1',cache_query_capabilities=cache);c.database_id='db'
    c.search_options=SearchOptions(mode='approx',path='search-first',device='gpu',quality_profile_id='q')
    state={'caps':0,'instance':'one','legacy':False,'ack':True}
    def capabilities():
        state['caps']+=1
        return {'server_instance_id':state['instance'],'execution_settings_version':0 if state['legacy'] else 1,
                'index_devices_version':1,'quality_profiles':[{'profile_id':'q','database_id':'db'}]}
    monkeypatch.setattr(c,'get_search_capabilities',capabilities)
    class Call:
        def __iter__(self):
            plan={'server_instance_id':state['instance'],'requested_device':'gpu','actual_device':'gpu','actual_mode':'approx',
                  'quality_profile':{'requested_id':'q','state':'applied'}} if state['ack'] else {}
            return iter(encode_result({'columns':[{'name':'id','type':'BIGINT'}],'rows':[[1]],'execution_plan':plan}))
        def cancel(self):pass
    monkeypatch.setattr(c.stub,'ExecuteSQL',lambda *a,**k:Call())
    return c,state


def test_query_capability_fetch_is_deduplicated_and_reused(monkeypatch):
    c,s=setup_client(monkeypatch)
    try:
        c.execute(SQL);assert s['caps']==1
        c.execute(SQL);assert s['caps']==1
        s['instance']='two';c.execute(SQL);c.execute(SQL);assert s['caps']==2
    finally:c.close()


def test_cached_capability_never_replaces_completion_ack(monkeypatch):
    c,s=setup_client(monkeypatch)
    try:
        c.execute(SQL);s.update(ack=False,legacy=True)
        with pytest.raises(ValueError,match='acknowledge'):c.execute(SQL)
        with pytest.raises(ValueError,match='support'):c.execute(SQL)
        assert s['caps']==2
    finally:c.close()


def test_device_controls_always_fetch_fresh_capabilities(monkeypatch):
    c,s=setup_client(monkeypatch)
    try:
        c.execute(SQL)
        monkeypatch.setattr(c,'_typed_control',lambda *a,**k:{'state':'ready','device':'gpu','gpu_id':0})
        c.build_index('items',device='gpu');c.build_index('items',device='gpu')
        assert s['caps']==3
    finally:c.close()


def test_cache_can_be_disabled_without_double_quality_fetch(monkeypatch):
    c,s=setup_client(monkeypatch,cache=False)
    try:c.execute(SQL);c.execute(SQL);assert s['caps']==2
    finally:c.close()


def test_negative_capabilities_do_not_poison_upgrade_retry(monkeypatch):
    c,s=setup_client(monkeypatch)
    try:
        s['legacy']=True
        with pytest.raises(ValueError,match='support'):c.execute(SQL)
        s['legacy']=False;s['instance']='upgraded'
        assert c.execute(SQL)['rows']==[[1]]
        assert s['caps']==2
    finally:c.close()
