"""Parent-side immutable GPU replica handle; native resources stay in worker."""
from dataclasses import dataclass
import time
import numpy as np
from .gpu_worker import GpuError


@dataclass(frozen=True)
class GpuReplicaKey:
    logical_key: object
    device_id: int
    precision: str
    worker_generation: int


class GpuIndex:
    def __init__(self,worker,token,cpu,worker_generation):
        self.worker=worker;self.token=token;self.worker_generation=worker_generation
        for name in ('dimension','nlist','row_count','byte_size','max_abs','ids'):setattr(self,name,getattr(cpu,name))

    def search(self,query,count,nprobe,*,deadline,cancelled=lambda:False,statistics=None,**kwargs):
        started=time.perf_counter()
        result=self.worker.search(self.token,query,count,nprobe,deadline,cancelled,self.worker_generation)
        ids=np.asarray(result['ids'],dtype=np.int64);distances=np.asarray(result['distances'],dtype=np.float32)
        if ids.ndim!=1 or distances.shape!=ids.shape or len(ids)>count or len(np.unique(ids))!=len(ids) or not np.isfinite(distances).all():
            raise RuntimeError('Invalid GPU response rows')
        positions=np.searchsorted(self.ids,ids)
        if np.any(positions>=len(self.ids)) or not np.array_equal(self.ids[positions],ids):raise RuntimeError('GPU returned unknown snapshot IDs')
        if statistics is not None:
            statistics['gpu_host_ms']=statistics.get('gpu_host_ms',0)+result['gpu_host_ms']
            statistics['gpu_ipc_queue_ms']=statistics.get('gpu_ipc_queue_ms',0)+max(0,(time.perf_counter()-started)*1000-result['gpu_host_ms'])
        return distances,ids
