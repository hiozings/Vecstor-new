from pathlib import Path
import json
import runpy
import numpy as np
import pytest


def module():
    path=Path(__file__).resolve().parents[2]/'benchmarks/p3/calibrate.py'
    assert path.exists(), 'Formal calibration runner is not implemented'
    return runpy.run_path(str(path))


def test_calibration_freezes_parameters_without_heldout_feedback():
    m=module()
    from vecstor.datasets import generate_dataset
    from vecstor.index.faiss_backend import FaissIndex
    from vecstor.index.types import BuildConfig
    data=generate_dataset(1024,16,seed=11)
    index=FaissIndex.build(data.ids,data.vectors,BuildConfig(nlist=8))
    cal,held=m['query_splits'](16,8,12,932)
    assert not set(map(bytes,cal)) & set(map(bytes,held))
    workload={'selectivity':.5,'path':'filter-first','k':10}
    frozen=m['calibrate'](data,cal,[1,8],workload,index=index)
    before=json.dumps(frozen,sort_keys=True)
    result=m['evaluate'](data,held,frozen,index=index)
    assert result['summary']['mean_recall']==1.0
    assert result['summary']['incomplete_queries']==0
    m['evaluate'](data,np.zeros_like(held),frozen,index=index)
    assert json.dumps(frozen,sort_keys=True)==before
    with pytest.raises(ValueError,match='overlap'):
        m['evaluate'](data,cal,frozen,index=index)


def test_quality_summary_does_not_count_exact_as_ann_or_empty_as_perfect():
    summarize=module()['summarize']
    result=summarize([
        {'actual_mode':'approx','recall':.4,'complete':True,'elapsed_ms':1},
        {'actual_mode':'exact','recall':1.,'complete':True,'elapsed_ms':2},
        {'actual_mode':'exact','recall':None,'complete':True,'elapsed_ms':3},
    ])
    assert result['mean_recall']==.7
    assert result['ann_mean_recall']==.4
    assert result['empty_truth_queries']==1
    assert result['exact_queries']==2


def test_empty_filter_truth_is_reported_separately():
    m=module()
    from vecstor.datasets import generate_dataset
    from vecstor.index.faiss_backend import FaissIndex
    from vecstor.index.types import BuildConfig
    data=generate_dataset(1024,2)
    index=FaissIndex.build(data.ids,data.vectors,BuildConfig(nlist=1))
    cal,held=m['query_splits'](2,2,3,701)
    frozen=m['calibrate'](data,cal,[1],{'selectivity':0.,'path':'filter-first','k':10},index=index)
    report=m['evaluate'](data,held,frozen,index=index)
    assert report['summary']['empty_truth_queries']==3
    assert report['summary']['ann_mean_recall'] is None
    assert report['summary']['mean_recall'] is None


def test_small_truth_is_exact_fallback_not_ann_success():
    m=module()
    from vecstor.datasets import generate_dataset
    from vecstor.index.faiss_backend import FaissIndex
    from vecstor.index.types import BuildConfig
    data=generate_dataset(1024,2)
    index=FaissIndex.build(data.ids,data.vectors,BuildConfig(nlist=1))
    cal,held=m['query_splits'](2,2,3,702)
    frozen=m['calibrate'](data,cal,[1],{'selectivity':.001,'path':'search-first','k':10},index=index)
    report=m['evaluate'](data,held,frozen,index=index)
    assert report['summary']['exact_queries']==3
    assert report['summary']['ann_mean_recall'] is None
    assert all(r['fallback_reason']=='truth_smaller_than_k' for r in report['records'])


def test_small_full_runner_retains_frozen_grid_and_heldout_evidence(tmp_path):
    m=module()
    report=m['run'](tmp_path/'run',sizes=[1024],distributions=['clustered'],dimension=4,
                    calibration_count=3,evaluation_count=5,selectivities=[.1,1.])
    assert report['passed'] and len(report['groups'])==4
    assert len((tmp_path/'run/heldout.jsonl').read_text().splitlines())==20
    files=list((tmp_path/'run/1024-clustered').glob('*-first.json'))
    assert len(files)==4
    assert all(len(json.loads(p.read_text())['calibration_query_digests'])==3 for p in files)
    assert (tmp_path/'run/1024-clustered/candidate-ablation.json').exists()


def test_resume_preserves_frozen_parameters_and_completed_groups(tmp_path):
    m=module();directory=tmp_path/'run'
    arguments=dict(sizes=[1024],distributions=['independent'],dimension=4,
                   calibration_count=3,evaluation_count=4,selectivities=[1.])
    initial=m['run'](directory,**arguments)
    frozen={p.name:p.read_bytes() for p in (directory/'1024-independent').glob('*-first.json')}
    evidence=(directory/'heldout.jsonl').read_bytes()
    resumed=m['run'](directory,resume=True,**arguments)
    assert resumed['groups']==initial['groups']
    assert (directory/'heldout.jsonl').read_bytes()==evidence
    assert {p.name:p.read_bytes() for p in (directory/'1024-independent').glob('*-first.json')}==frozen
    with pytest.raises(ValueError,match='configuration'):
        m['run'](directory,resume=True,**dict(arguments,evaluation_count=5))


def test_resume_source_change_requires_explicit_audited_ruling(tmp_path):
    m=module();directory=tmp_path/'run'
    arguments=dict(sizes=[1024],distributions=['independent'],dimension=2,
                   calibration_count=2,evaluation_count=3,selectivities=[1.])
    m['run'](directory,**arguments)
    config=json.loads((directory/'config.json').read_text());config['source_sha256']='0'*64
    (directory/'config.json').write_text(json.dumps(config))
    with pytest.raises(ValueError,match='configuration'):
        m['run'](directory,resume=True,**arguments)
    evidence=(directory/'heldout.jsonl').read_bytes()
    m['run'](directory,resume=True,source_ruling='Ownership-only fix, no ANN math change',**arguments)
    assert (directory/'heldout.jsonl').read_bytes()==evidence
    audit=json.loads((directory/'source-transitions.json').read_text())
    assert audit[-1]['from']=='0'*64
    assert audit[-1]['reason']=='Ownership-only fix, no ANN math change'
    with pytest.raises(ValueError,match='configuration'):
        m['run'](directory,resume=True,source_ruling='Ownership-only fix',**dict(arguments,seed=1))


def test_resume_runner_change_requires_explicit_audit(tmp_path):
    m=module();directory=tmp_path/'run'
    arguments=dict(sizes=[1024],distributions=['independent'],dimension=2,
                   calibration_count=2,evaluation_count=3,selectivities=[1.])
    m['run'](directory,**arguments)
    path=directory/'config.json';config=json.loads(path.read_text());config['runner_sha256']='0'*64
    path.write_text(json.dumps(config))
    with pytest.raises(ValueError,match='runner'):
        m['run'](directory,resume=True,**arguments)
    m['run'](directory,resume=True,source_ruling='Resume validation only; query math unchanged',**arguments)
    history=json.loads((directory/'runner-transitions.json').read_text())
    assert history[-1]['from']=='0'*64 and history[-1]['reason']


def test_resume_cannot_relabel_completed_results_with_other_frozen_settings(tmp_path):
    m=module();directory=tmp_path/'run'
    arguments=dict(sizes=[1024],distributions=['independent'],dimension=4,
                   calibration_count=3,evaluation_count=4,selectivities=[1.])
    m['run'](directory,**arguments)
    path=directory/'1024-independent/1-filter-first.json';frozen=json.loads(path.read_text())
    frozen['settings']=next(g['settings'] for g in frozen['grid'] if g['settings']!=frozen['settings'])
    path.write_text(json.dumps(frozen))
    with pytest.raises(ValueError,match='frozen|parameters'):
        m['run'](directory,resume=True,**arguments)


def test_resume_rechecks_completed_group_dataset_and_query_identity(tmp_path):
    from vecstor.datasets import load_dataset,save_dataset
    from vecstor.data import Dataset
    m=module();directory=tmp_path/'run'
    arguments=dict(sizes=[1024],distributions=['independent'],dimension=2,
                   calibration_count=2,evaluation_count=3,selectivities=[1.])
    m['run'](directory,**arguments)
    source=directory/'1024-independent/dataset.npz';data=load_dataset(source);values=data.values.copy();values[0]+=1
    source.unlink();save_dataset(Dataset(data.ids,data.categories,data.timestamps,values,data.vectors),source)
    with pytest.raises(ValueError,match='fingerprint'):
        m['run'](directory,resume=True,**arguments)
