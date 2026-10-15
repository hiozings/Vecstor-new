from copy import deepcopy
import faiss
from test_quality_profiles_rpc import profiled,SQL
from vecstor.query.quality_policy import QualityCatalog


def test_real_gpu_profile_and_explicit_cpu_evidence_fallback(tmp_path):
    with profiled(tmp_path) as (c,_,compute,doc,_):
        built=c.build_index('items',nlist=1,device='gpu')
        p=deepcopy(doc['profiles'][0]);p['format_version']=2;p['workload']['path']='search-first'
        p['execution']={'device':'gpu','precision':'float32','faiss_version':faiss.__version__,'gpu_model':built['gpu_model']}
        cpu=deepcopy(doc['profiles'][0]);cpu['profile_id']='cpu-tested';cpu['workload']['path']='search-first'
        compute.sql.quality_catalog=QualityCatalog.from_document({'format_version':2,'profiles':[p,cpu]})
        query=SQL+" SETTINGS device='gpu',mode='approx',path='search-first',quality_profile_id='tested'"
        result=c.execute(query)
        assert result['execution_plan']['actual_device']=='gpu'
        assert result['execution_plan']['quality_profile']['device']=='gpu'
        assert result['execution_plan']['quality_profile']['state']=='applied'
        assert [row[0] for row in result['rows']]==list(range(100,110))
        result=c.execute(query.replace("'tested'","'cpu-tested'")+",device_fallback='cpu'")
        assert result['execution_plan']['actual_device']=='cpu'
        assert result['execution_plan']['quality_profile']['device']=='cpu'
        assert result['execution_plan']['device_fallback_reason']=='quality_profile_backend_mismatch'
