import numpy as np
import pytest
from vecstor.data import Dataset
from vecstor.datasets import save_dataset
from vecstor.storage.catalog import DatabaseCatalog


@pytest.mark.parametrize('binding',['legacy','text'])
def test_candidate_binding_preserves_int64_and_predicates_and_order(tmp_path,binding):
    ids=np.array([2**60+1,2**63-2,2**63-1],dtype=np.int64)
    data=Dataset(ids,['a','b','a'],np.zeros(3,dtype=np.int64),np.array([1.,2.,3.]),np.zeros((3,2),dtype=np.float32))
    with DatabaseCatalog(tmp_path,candidate_binding=binding) as catalog:
        db=catalog.open_database('test',True)['id'];save_dataset(data,tmp_path/'imports/source.npz')
        catalog.import_dataset(db,'source.npz','items','import')
        lease=catalog.begin_read(db,'items')
        sql="SELECT id FROM items WHERE category = 'a' AND value < 3 ORDER BY vector_distance(vector,[0,0]) LIMIT 10"
        result=catalog.filter_candidates(db,lease['token'],sql,[int(ids[2]),int(ids[0]),int(ids[1])])
        assert [r[0] for r in result]==[2**60+1]
        with pytest.raises(ValueError,match='does not exist'):
            catalog.filter_candidates(db,lease['token'],sql,[123])
        with pytest.raises(ValueError,match='int64'):
            catalog.filter_candidates(db,lease['token'],sql,[2**63])
        catalog.end_read(db,lease['token'])


def test_candidate_plan_cache_is_lease_scoped_bounded_and_optional(tmp_path,monkeypatch):
    import vecstor.storage.database as module
    data=Dataset(np.arange(3,dtype=np.int64),['x']*3,np.zeros(3,dtype=np.int64),np.array([1.,2.,3.]),np.zeros((3,2),dtype=np.float32))
    with DatabaseCatalog(tmp_path,candidate_plan_cache=True) as catalog:
        dbid=catalog.open_database('test',True)['id'];save_dataset(data,tmp_path/'imports/source.npz')
        catalog.import_dataset(dbid,'source.npz','items','import');db=catalog.database(dbid)
        lease=catalog.begin_read(dbid,'items');original=module.classify_statement;calls=[]
        def observed(sql):calls.append(sql);return original(sql)
        monkeypatch.setattr(module,'classify_statement',observed)
        sql='SELECT id FROM items WHERE value < 3 ORDER BY vector_distance(vector,[0,0]) LIMIT 10'
        for ids in [[0],[0,1],[1,2]]:catalog.filter_candidates(dbid,lease['token'],sql,ids)
        assert len(calls)==1
        other=sql.replace('value < 3','value >= 3')
        assert [r[0] for r in catalog.filter_candidates(dbid,lease['token'],other,[0,1,2])]==[2]
        assert [r[0] for r in catalog.filter_candidates(dbid,lease['token'],sql,[0,1,2])]==[0,1]
        assert len(db._candidate_plans)==1
        catalog.end_read(dbid,lease['token'])
        assert not db._candidate_plans
    with DatabaseCatalog(tmp_path,candidate_plan_cache=False) as catalog:
        dbid=catalog.open_database('test')['id'];lease=catalog.begin_read(dbid,'items');calls.clear()
        for _ in range(2):catalog.filter_candidates(dbid,lease['token'],sql,[0,1])
        assert len(calls)==2
