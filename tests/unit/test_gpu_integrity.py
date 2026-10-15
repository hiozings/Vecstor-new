from types import SimpleNamespace
import time
import numpy as np
import pytest
from vecstor.index.gpu_backend import GpuIndex
from vecstor.index.gpu_worker import GpuDataError


@pytest.mark.parametrize('ids',[[1.9],[True],['1'],[2**63],[-1]])
def test_invalid_native_id_types_are_not_coerced(ids):
    class Peer:
        def search(self,*a):return dict(ids=ids,distances=[0.],gpu_host_ms=.1)
    cpu=SimpleNamespace(dimension=2,nlist=1,row_count=1,byte_size=10,max_abs=1,ids=np.array([1],np.int64))
    with pytest.raises(GpuDataError):GpuIndex(Peer(),'t',cpu,1).search(np.zeros(2,np.float32),1,1,deadline=time.monotonic()+1)


@pytest.mark.parametrize('cost',[float('nan'),float('inf'),-1,True,'0.1',None])
def test_invalid_native_timing_is_data_loss(cost):
    class Peer:
        def search(self,*a):return dict(ids=[1],distances=[0.],gpu_host_ms=cost)
    cpu=SimpleNamespace(dimension=2,nlist=1,row_count=1,byte_size=10,max_abs=1,ids=np.array([1],np.int64))
    with pytest.raises(GpuDataError):GpuIndex(Peer(),'t',cpu,1).search(np.zeros(2,np.float32),1,1,deadline=time.monotonic()+1,statistics={})
