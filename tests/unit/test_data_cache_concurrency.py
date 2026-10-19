from concurrent.futures import ThreadPoolExecutor
from contextlib import ExitStack
from threading import Event, Barrier

import numpy as np

from vecstor.cache.types import CacheConfig, DataScope
from vecstor.cache.vector_cache import VersionedVectorCache

SCOPE=DataScope('epoch','db','table',1,'schema',4)


def test_clear_during_download_cannot_refill():
    cache=VersionedVectorCache(CacheConfig(max_bytes=10000));ready=Event();release=Event()
    def read():
        with cache.batch(SCOPE,(1,),lambda:None) as batch:
            ready.set();assert release.wait(5)
            return batch.finish((1,),(1,),np.ones((1,4),dtype='<f4'),admit=True,check=lambda:None)
    with ThreadPoolExecutor(1) as pool:
        future=pool.submit(read);assert ready.wait(5)
        cache.clear('db');release.set()
        assert future.result().tolist()==[[1.,1.,1.,1.]]
    assert cache.stats('db')['process']['entry_count']==0
    assert cache.stats('db')['process']['workspace_bytes']==0


def test_parallel_miss_is_not_single_flight():
    cache=VersionedVectorCache(CacheConfig(max_bytes=10000));barrier=Barrier(2)
    def read():
        with cache.batch(SCOPE,(1,),lambda:None) as batch:
            assert batch.missing_ids==(1,)
            barrier.wait(5)
            batch.finish((1,),(1,),np.ones((1,4),dtype='<f4'),admit=True,check=lambda:None)
            return dict(batch.statistics)
    with ThreadPoolExecutor(2) as pool:results=list(pool.map(lambda _:read(),range(2)))
    assert sum(r['admitted_rows'] for r in results)==1
    assert sum(r['duplicate_inserts'] for r in results)==1
    assert cache.stats('db')['process']['accounted_bytes']==1552


def test_workspace_exhaustion_bypasses():
    cache=VersionedVectorCache(CacheConfig(max_bytes=10000))
    with ExitStack() as stack:
        for _ in range(4):assert stack.enter_context(cache.batch(SCOPE,(1,),lambda:None)) is not None
        assert stack.enter_context(cache.batch(SCOPE,(1,),lambda:None)) is None
        assert cache.stats('db')['process']['workspace_bytes']==16*1024**2
    assert cache.stats('db')['process']['workspace_bytes']==0
    with cache.batch(SCOPE,(1,),lambda:None) as batch:assert batch is not None
