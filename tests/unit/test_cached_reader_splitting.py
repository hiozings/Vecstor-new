from types import SimpleNamespace

import grpc
import numpy as np
import pytest

from vecstor.cache.types import CacheConfig,DataScope
from vecstor.cache.vector_cache import VersionedVectorCache
from vecstor.compute.data_reader import CachedLeaseReader


class TooLarge(grpc.RpcError):
    def code(self):return grpc.StatusCode.RESOURCE_EXHAUSTED
    def details(self):return 'projected_response_too_large'


def reader(limit):
    calls=[]
    scope=DataScope('e','db','table',1,'schema',4)
    def read(sql,ids,fields,vector_ids):
        calls.append(tuple(ids))
        if len(ids)>limit:raise TooLarge()
        return ([{'id':i} for i in ids],vector_ids,
                np.array([[i]*4 for i in vector_ids],dtype='<f4').reshape(len(vector_ids),4))
    io=SimpleNamespace(trace={'counts':{}},remaining=lambda:10,configure_cache_lease=lambda:scope,
        read_projected_candidates=read,database_id='db',lease=SimpleNamespace(dimension=4))
    cache=VersionedVectorCache(CacheConfig(max_bytes=10000,workspace_slots=1))
    return CachedLeaseReader(io,cache,('id',),True,True),calls


def test_split_releases_parent_workspace_and_counts_logical_rows_once():
    read,calls=reader(2)
    rows,vectors=read.filter_fetch('sql',[3,1,2,0])
    assert calls==[(3,1,2,0),(3,1),(2,0)]
    assert [r['id'] for r in rows]==[3,1,2,0]
    assert vectors[:,0].tolist()==[3.,1.,2.,0.]
    stats=read.describe()
    assert stats['lookup_rows']==4 and stats['retry_lookup_rows']==4 and stats['bypass_rows']==0
    assert read.cache.stats('db')['process']['workspace_bytes']==0


def test_single_oversized_candidate_is_not_retried():
    read,calls=reader(0)
    with pytest.raises(TooLarge):read.filter_fetch('sql',[1])
    assert calls==[(1,)]
    assert read.cache.stats('db')['process']['workspace_bytes']==0
