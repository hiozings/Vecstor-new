"""Deployment evidence and O(1) service-local vector request occupancy."""
import hashlib
import os
from pathlib import Path
import platform
from threading import Lock

from vecstor.compute.planner_types import DeploymentContext, fingerprint


def runtime_source_fingerprint():
    root=Path(__file__).resolve().parents[1]
    paths=sorted(root.rglob('*.py'))
    if not paths:return None
    return fingerprint({str(path.relative_to(root)):hashlib.sha256(path.read_bytes()).hexdigest() for path in paths})


def deployment_fingerprint():
    """Call after filesystem isolation, before serving requests; never probe RPC RTT."""
    try:
        from importlib.metadata import version
        import faiss
        import numpy
        import sqlglot
        omp=int(os.environ['OMP_NUM_THREADS']);blas=int(os.environ['OPENBLAS_NUM_THREADS'])
        if not 1<=omp<=1024 or not 1<=blas<=1024:return None
        with Path('/proc/cpuinfo').open() as source:info=source.read(65536)
        model=next(line.split(':',1)[1].strip() for line in info.splitlines() if line.startswith('model name'))
        source=runtime_source_fingerprint()
        if source is None:return None
        return fingerprint(dict(cpu=model,architecture=platform.machine(),platform=platform.platform(),
            runtime_source_sha256=source,faiss_compile_options=getattr(faiss,'get_compile_options',lambda:None)(),
            python=platform.python_version(),faiss=faiss.__version__,numpy=numpy.__version__,
            duckdb=version('duckdb'),sqlglot=sqlglot.__version__,omp=omp,blas=blas,
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
