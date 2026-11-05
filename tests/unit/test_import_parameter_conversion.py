import collections
import importlib.abc
import sys

from vecstor.storage.catalog import DatabaseCatalog
from vecstor.datasets import generate_dataset


def test_batch_import_does_not_probe_optional_modules_per_scalar(tmp_path):
    """DuckDB list conversion must not repeatedly scan sys.path for absent pandas."""
    class Probe(importlib.abc.MetaPathFinder):
        def __init__(self):self.calls=collections.Counter()
        def find_spec(self,fullname,path=None,target=None):
            self.calls[fullname]+=1
            if fullname=='pandas':raise ModuleNotFoundError("No module named 'pandas'",name='pandas')
    with DatabaseCatalog(tmp_path) as catalog:
        identity=catalog.open_database('demo',True)['id']
        data=generate_dataset(2048,4,seed=19)
        probe=Probe();sys.meta_path.insert(0,probe)
        try:
            result=catalog.database(identity).call('import','source','items',lambda:('digest',data),'import')
        finally:sys.meta_path.remove(probe)
        assert result['affected_rows']==2048
        # Allow fixed scalar probes for transaction metadata and the two batches.
        assert probe.calls['pandas']<=64


def test_import_json_metadata_preserves_escaping_and_int64(tmp_path):
    import numpy as np
    from vecstor.data import Dataset
    categories=['论文\'x','a"b','a\\b','x\ny','😀']
    ids=np.arange(5,dtype=np.int64)+(2**63-5)
    values=np.array([1e-200,1e200,-1.25,0.,3.141592653589793])
    data=Dataset(ids,categories,-ids,values,np.ones((5,2),dtype=np.float32))
    with DatabaseCatalog(tmp_path) as catalog:
        identity=catalog.open_database('demo',True)['id']
        catalog.database(identity).call('import','source','items',lambda:('digest',data),'import')
        rows=catalog.execute(identity,'SELECT id,category,timestamp,value FROM items ORDER BY id','read')['rows']
        assert rows==[[int(ids[i]),categories[i],int(-ids[i]),float(values[i])] for i in range(5)]
