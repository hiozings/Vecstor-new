"""Deployment evidence and O(1) service-local vector request occupancy."""
import os
from pathlib import Path
import platform
from threading import Lock

from vecstor.compute.planner_types import DeploymentContext, fingerprint


def deployment_fingerprint():
    """Call after filesystem isolation, before serving requests; never probe RPC RTT."""
    try:
        import duckdb
        import faiss
        import numpy
        import sqlglot
        omp=int(os.environ['OMP_NUM_THREADS']);blas=int(os.environ['OPENBLAS_NUM_THREADS'])
        if not 1<=omp<=1024 or not 1<=blas<=1024:return None
        with Path('/proc/cpuinfo').open() as source:info=source.read(65536)
        model=next(line.split(':',1)[1].strip() for line in info.splitlines() if line.startswith('model name'))
        return fingerprint(dict(cpu=model,architecture=platform.machine(),platform=platform.platform(),
            python=platform.python_version(),faiss=faiss.__version__,numpy=numpy.__version__,
            duckdb=duckdb.__version__,sqlglot=sqlglot.__version__,omp=omp,blas=blas,
            faiss_threads=faiss.omp_get_max_threads()))
    except (OSError,KeyError,ValueError,StopIteration,ImportError):return None


class ActiveVectorRequests:
    def __init__(self):self._lock=Lock();self._value=0

    def enter(self):
        with self._lock:self._value+=1

    def leave(self):
        with self._lock:
            if not self._value:raise RuntimeError('Unbalanced vector request count')
            self._value-=1

    @property
    def value(self):
        with self._lock:return self._value

    def snapshot(self, hardware_fingerprint, network_class):
        return DeploymentContext(hardware_fingerprint,network_class,self.value)
