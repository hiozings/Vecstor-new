from copy import deepcopy
from dataclasses import replace
import faiss
import pytest
from test_quality_policy import document,setup
from vecstor.query.quality_policy import QualityCatalog,QualityUnavailable
from vecstor.query.search_options import SearchOptions


def test_cpu_quality_evidence_cannot_be_claimed_for_gpu():
    catalog,p,key,plan=setup()
    with pytest.raises(QualityUnavailable,match='backend'):
        catalog.resolve(p['profile_id'],key,256,100000,plan,SearchOptions(device='gpu',mode='approx'))


def test_gpu_quality_requires_actual_backend_model_and_candidate_capability():
    _,p,key,plan=setup();d=document();p=d['profiles'][0];d['format_version']=2;p['format_version']=2
    p['execution']={'device':'gpu','precision':'float32','faiss_version':faiss.__version__,'gpu_model':'Test GPU'}
    p['workload']['path']='search-first';p['budget'].update(initial_candidates=1024,max_candidates=2048)
    catalog=QualityCatalog.from_document(d);opts=SearchOptions(device='gpu',mode='approx',path='search-first')
    with pytest.raises(QualityUnavailable,match='backend'):
        catalog.resolve(p['profile_id'],key,256,100000,plan,opts,execution={'gpu_model':'Other GPU'})
    effective,description=catalog.resolve(p['profile_id'],key,256,100000,plan,opts,execution={'gpu_model':'Test GPU'})
    assert description['device']=='gpu' and effective.candidates==1024
    d['profiles'][0]['budget'].update(initial_candidates=4096,max_candidates=4096)
    with pytest.raises(QualityUnavailable,match='gpu_limits'):
        QualityCatalog.from_document(d).resolve(p['profile_id'],key,256,100000,plan,opts,execution={'gpu_model':'Test GPU'})
