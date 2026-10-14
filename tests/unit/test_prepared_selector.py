from concurrent.futures import ThreadPoolExecutor
import gc
import weakref

import numpy as np
import pytest

from vecstor.index.faiss_backend import FaissIndex
from vecstor.index.types import BuildConfig


def fixture():
    ids=2**60+np.arange(1024,dtype=np.int64)
    vectors=np.random.default_rng(4).random((1024,8),dtype=np.float32)
    return FaissIndex.build(ids,vectors,BuildConfig(nlist=8)),ids


def test_prepared_selector_owns_ids_and_is_safe_for_concurrent_queries():
    index,ids=fixture();left=ids[:512].copy();right=ids[512:].copy()
    a=index.prepare_selector(left);b=index.prepare_selector(right)
    left[:]=ids[512:]
    def search(selector):
        return index.search(np.zeros(8,dtype=np.float32),20,8,prepared_selector=selector)[1]
    with ThreadPoolExecutor(4) as pool:
        results=list(pool.map(search,[a,b,a,b]))
    assert all(set(result).issubset(ids[:512] if i%2==0 else ids[512:]) for i,result in enumerate(results))
    ref=weakref.ref(a);del a;gc.collect();assert ref() is None


def test_full_coverage_requires_actual_id_equality_not_only_count():
    index,ids=fixture()
    assert index.prepare_selector(ids[::-1]) is None
    invalid=ids.copy();invalid[-1]+=10000
    with pytest.raises(ValueError,match='absent'):index.prepare_selector(invalid)
    assert index.prepare_selector(ids[:-1]) is not None


def test_prepared_search_reports_native_timing_and_rejects_ambiguous_filters():
    index,ids=fixture();selector=index.prepare_selector(ids[:256]);stats={}
    index.search(np.zeros(8,dtype=np.float32),10,8,prepared_selector=selector,statistics=stats)
    assert stats['native_search_ms']>=0
    with pytest.raises(ValueError,match='both'):
        index.search(np.zeros(8,dtype=np.float32),10,8,ids[:256],prepared_selector=selector)
