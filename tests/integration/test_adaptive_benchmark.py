from pathlib import Path
import json
import runpy


def test_smoke_matrix_is_explicit_exact_only_and_cleans_processes(tmp_path):
    path=Path(__file__).resolve().parents[2]/'benchmarks/p4/adaptive.py'
    run=runpy.run_path(str(path))['run_adaptive_matrix']
    report=run(tmp_path/'smoke',tier='smoke',policy_path=None,repeats=3)
    assert report['functional_passed']
    assert report['scope']=='smoke_exact_only'
    assert not report['quality_certified']
    assert report['configuration_runs']==6
    assert report['measured_queries']==24
    assert report['processes_running_after_cleanup']==[]
    assert all(r['equivalent'] for r in report['runs'])
    prepared=json.loads((tmp_path/'smoke/calibration/report.json').read_text())
    assert prepared['cases'][0]['eligible_paths']==[]
    repeated=runpy.run_path(str(path))['evaluate'](tmp_path/'smoke/calibration',tmp_path/'smoke/evaluation',tier='smoke',resume=True)
    assert repeated['reused_configuration_runs']==6 and repeated['configuration_runs']==6


def test_failed_trial_is_durable_and_cannot_be_washed_by_resume(tmp_path,monkeypatch):
    from contextlib import contextmanager
    from types import SimpleNamespace
    import numpy as np
    import pytest
    path=Path(__file__).resolve().parents[2]/'benchmarks/p4/adaptive.py'
    trial=runpy.run_path(str(path))['_trial']
    class Monitor:
        def __init__(self,*args):pass
        def observe(self,*args):pass
        def close(self):return {}
    monkeypatch.setitem(trial.__globals__,'Monitor',Monitor)
    class Client:
        calls=0
        def open_database(self,*args,**kwargs):pass
        def load_index(self,*args):pass
        def get_search_capabilities(self):return {'adaptive_hardware_fingerprint':'h'}
        def clear_data_cache(self):pass
        def data_cache_stats(self):return {'database':{'entry_count':0}}
        def execute(self,sql):
            if sql.startswith('EXPLAIN'):return {}
            self.calls+=1
            if self.calls==2:raise RuntimeError('injected query failure')
            return {'rows':[[0,0.]],'revision':1,'execution_plan':{}}
    client=Client()
    @contextmanager
    def compute(*args,**kwargs):
        yield client,SimpleNamespace(process=SimpleNamespace(pid=1),events=lambda:[{'event':'filesystem_isolation'}])
    group=SimpleNamespace(root=tmp_path,storage=SimpleNamespace(address='unused',process=SimpleNamespace(pid=1)),compute=compute)
    spec=dict(rows=1024,k=10,selectivity=1.,capacity_mib=64,concurrency=1,patterns=['cold','warm'])
    with pytest.raises(RuntimeError,match='injected'):
        trial(group,spec,np.zeros((2,4),dtype='<f4'),'auto',None,None,None,1,tmp_path/'trial')
    checkpoint=json.loads((tmp_path/'trial/complete.json').read_text())
    assert checkpoint['state']=='failed'
    events=[json.loads(line) for line in (tmp_path/'trial/attempts.jsonl').read_text().splitlines()]
    assert any(e['event']=='error' for e in events)
    with pytest.raises(ValueError,match='failed trial'):
        trial(group,spec,np.zeros((2,4),dtype='<f4'),'auto',None,None,None,1,tmp_path/'trial')
    assert client.calls==2
