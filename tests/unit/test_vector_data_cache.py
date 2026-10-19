"""Cache contracts protect identity, byte ownership, and admission bounds."""
from dataclasses import replace
import gc
import weakref

import grpc
import numpy as np
import pytest

from vecstor.cache.types import CacheConfig, DataScope
from vecstor.cache.vector_cache import VersionedVectorCache
from vecstor.rpc.common import RemoteError

SCOPE = DataScope('epoch', 'db', 'table', 1, 'schema', 4)


def read(cache, ids, *, scope=SCOPE, values=None, admit=True):
    ids=tuple(ids)
    with cache.batch(scope,ids,lambda:None) as batch:
        assert batch is not None
        missing=batch.missing_ids
        vectors=np.asarray(values if values is not None else [[i]*scope.dimension for i in missing],dtype='<f4').reshape(len(missing),scope.dimension)
        result=batch.finish(ids,missing,vectors,admit=admit,check=lambda:None)
        return result,dict(batch.statistics)


def test_budget_and_lru():
    cache=VersionedVectorCache(CacheConfig(max_bytes=2080))
    read(cache,[1,2])
    assert cache.stats('db')['process']['accounted_bytes']==2080
    assert read(cache,[1])[1]['hit_rows']==1
    read(cache,[3])
    with cache.batch(SCOPE,(1,2,3),lambda:None) as batch:assert batch.missing_ids==(2,)
    assert cache.stats('db')['process']['entry_count']==2
    assert cache.stats('db')['process']['accounted_bytes']==2080


def test_entry_limit_and_empty_scope_release():
    cache=VersionedVectorCache(CacheConfig(max_bytes=10000,max_entries=1))
    read(cache,[1]);read(cache,[2],scope=replace(SCOPE,database_id='other'))
    assert cache.stats('db')['database']['accounted_bytes']==0
    assert cache.stats('other')['process']['accounted_bytes']==1552
    cache.clear('other')
    assert cache.stats('db')['process']['accounted_bytes']==0


def test_large_entry_bypasses():
    cache=VersionedVectorCache(CacheConfig(max_bytes=1024))
    result,stats=read(cache,[7])
    assert result.tolist()==[[7.,7.,7.,7.]]
    assert stats['admitted_rows']==0
    assert cache.stats('db')['process']['entry_count']==0


def test_copy_owns_only_one_vector():
    cache=VersionedVectorCache(CacheConfig(max_bytes=2080))
    source=np.zeros((65536,4),dtype='<f4');source[0]=[-0.,1.,2.,3.]
    ref=weakref.ref(source)
    result,_=read(cache,[2**53+1],values=source[:1])
    del source;gc.collect()
    assert ref() is None
    result[0]=99
    again,stats=read(cache,[2**53+1])
    assert again.tobytes()==np.array([[-0.,1.,2.,3.]],dtype='<f4').tobytes()
    assert stats['hit_rows']==1
    assert cache.stats('db')['database']['payload_bytes']==16


@pytest.mark.parametrize('change',[dict(storage_epoch='new'),dict(database_id='other'),dict(table_id='new'),dict(revision=2),dict(schema_version='new'),dict(dimension=2)])
def test_every_data_identity_part_isolated(change):
    cache=VersionedVectorCache(CacheConfig(max_bytes=10000));read(cache,[1])
    with cache.batch(replace(SCOPE,**change),(1,),lambda:None) as batch:assert batch.missing_ids==(1,)


def test_conflicting_same_key_is_data_loss():
    cache=VersionedVectorCache(CacheConfig(max_bytes=10000))
    with cache.batch(SCOPE,(1,),lambda:None) as pending:
        read(cache,[1])
        with pytest.raises(RemoteError) as error:
            pending.finish((1,),(1,),np.zeros((1,4),dtype='<f4'),admit=True,check=lambda:None)
        assert error.value.code==grpc.StatusCode.DATA_LOSS
    assert read(cache,[1])[0].tolist()==[[1.,1.,1.,1.]]


def test_fused_match_subset_and_served_hits():
    cache=VersionedVectorCache(CacheConfig(max_bytes=10000));read(cache,[3,1])
    with cache.batch(SCOPE,(3,1,2),lambda:None) as batch:
        result=batch.finish((3,2),(2,),np.full((1,4),2,dtype='<f4'),admit=True,check=lambda:None)
        assert result.tolist()==[[3.,3.,3.,3.],[2.,2.,2.,2.]]
        assert batch.statistics['hit_rows']==2
        assert batch.statistics['vectors_served_from_cache']==1


@pytest.mark.parametrize('remote_ids,matched,values',[
    ((2,),(1,2),np.zeros((1,4),dtype='<f4')),
    ((2,1),(1,2),np.zeros((2,4),dtype='<f4')),
    ((1,2),(1,2),np.full((2,4),np.nan,dtype='<f4')),
    ((1,2),(1,2),np.zeros((2,3),dtype='<f4')),
    ((1,2),(2,1),np.zeros((2,4),dtype='<f4')),
])
def test_invalid_completion_never_fills_cache(remote_ids,matched,values):
    cache=VersionedVectorCache(CacheConfig(max_bytes=10000))
    with cache.batch(SCOPE,(1,2),lambda:None) as batch:
        with pytest.raises(RemoteError):batch.finish(matched,remote_ids,values,admit=True,check=lambda:None)
    assert cache.stats('db')['process']['entry_count']==0
    assert cache.stats('db')['process']['workspace_bytes']==0


def test_lookup_only_does_not_admit_and_cancellation_releases_workspace():
    cache=VersionedVectorCache(CacheConfig(max_bytes=10000))
    read(cache,[1],admit=False)
    assert cache.stats('db')['process']['entry_count']==0
    with pytest.raises(TimeoutError):
        with cache.batch(SCOPE,(1,),lambda:None) as batch:
            def expired():raise TimeoutError('deadline')
            batch.finish((1,),(1,),np.ones((1,4),dtype='<f4'),admit=True,check=expired)
    assert cache.stats('db')['process']['workspace_bytes']==0
    assert cache.stats('db')['process']['entry_count']==0


@pytest.mark.parametrize('options',[dict(max_bytes=True),dict(max_bytes=-1),dict(max_entries=0),dict(workspace_slots=0),dict(workspace_bytes=-1),dict(scan_admission=1)])
def test_invalid_config_rejected(options):
    with pytest.raises(ValueError):CacheConfig(**options)


@pytest.mark.parametrize('ids',[(-1,),(2**63,),(True,),(1,1),tuple(range(1025))])
def test_invalid_ids_cannot_enter_cache(ids):
    cache=VersionedVectorCache(CacheConfig(max_bytes=10000))
    with pytest.raises(ValueError):
        with cache.batch(SCOPE,ids,lambda:None):pass


def test_batch_cannot_finish_twice_or_after_release():
    cache=VersionedVectorCache(CacheConfig(max_bytes=10000))
    with cache.batch(SCOPE,(1,),lambda:None) as batch:
        batch.finish((1,),(1,),np.ones((1,4),dtype='<f4'),admit=True,check=lambda:None)
        with pytest.raises(ValueError):batch.finish((1,),(),np.empty((0,4),dtype='<f4'),admit=False,check=lambda:None)
    with pytest.raises(ValueError):batch.finish((1,),(),np.empty((0,4),dtype='<f4'),admit=False,check=lambda:None)


def test_clear_database_is_scoped_and_disabled_never_allocates():
    cache=VersionedVectorCache(CacheConfig(max_bytes=10000))
    read(cache,[1]);read(cache,[1],scope=replace(SCOPE,database_id='other'))
    cache.clear('db')
    assert read(cache,[1],scope=replace(SCOPE,database_id='other'))[1]['hit_rows']==1
    disabled=VersionedVectorCache(CacheConfig())
    with disabled.batch(SCOPE,(1,),lambda:None) as batch:assert batch is None
    assert disabled.stats('db')['process']['workspace_bytes']==0


@pytest.mark.parametrize('change',[dict(dimension=0),dict(dimension=True),dict(revision=-1),dict(revision=True),dict(storage_epoch=''),dict(dtype='float64')])
def test_invalid_scope_rejected(change):
    with pytest.raises(ValueError):replace(SCOPE,**change)
