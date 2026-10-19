"""Copy-owned vector LRU; network work never holds its lock or waits for peers."""
from collections import OrderedDict
from contextlib import contextmanager
from threading import RLock
import time

import grpc
import numpy as np

from vecstor.rpc.common import RemoteError
from .types import CacheConfig, DataScope

SCOPE_CHARGE=1024
ENTRY_CHARGE=512
WORKSPACE_CHARGE=4*1024**2


def _ids(values):
    values=tuple(values)
    if len(values)>1024 or len(set(values))!=len(values) or any(type(i) is not int or not 0<=i<2**63 for i in values):
        raise ValueError('Cache IDs must be distinct nonnegative int64 values, at most 1024')
    return values


def _consistent(condition, message):
    if not condition:raise RemoteError(grpc.StatusCode.DATA_LOSS,message)


class LookupBatch:
    def __init__(self, cache, scope, ids, check):
        self._cache,self._scope,self._ids=cache,scope,ids
        self._closed=False;self._done=False
        self.statistics=dict(lookup_rows=len(ids),hit_rows=0,miss_rows=0,admitted_rows=0,
            admitted_payload_bytes=0,evictions=0,duplicate_inserts=0,vectors_served_from_cache=0,
            lookup_ms=0.,copy_ms=0.,admit_ms=0.)
        self._vectors=np.empty((len(ids),scope.dimension),dtype='<f4')
        self._positions={identity:n for n,identity in enumerate(ids)}
        missing=[]
        started=time.monotonic()
        with cache._lock:
            check()
            self._generation=cache._generation
            for position,identity in enumerate(ids):
                key=(scope,identity);raw=cache._entries.get(key)
                if raw is None:missing.append(identity)
                else:
                    copy_started=time.monotonic()
                    self._vectors[position]=np.frombuffer(raw,dtype='<f4')
                    self.statistics['copy_ms']+=(time.monotonic()-copy_started)*1000
                    cache._entries.move_to_end(key)
            check()
        self._missing_ids=tuple(missing);self._missing_set=set(missing)
        self.statistics.update(hit_rows=len(ids)-len(missing),miss_rows=len(missing),
                               lookup_ms=(time.monotonic()-started)*1000)

    @property
    def ids(self):return self._ids

    @property
    def missing_ids(self):return self._missing_ids

    @property
    def generation(self):return self._generation

    def finish(self, matched_ids, remote_ids, remote_vectors, *, admit, check):
        if self._closed or self._done:raise ValueError('Cache batch is already finished or released')
        self._done=True
        check()
        matched_ids=_ids(matched_ids);remote_ids=_ids(remote_ids)
        matched_set=set(matched_ids)
        _consistent(tuple(i for i in self.ids if i in matched_set)==matched_ids,'Cache matched ID coverage/order mismatch')
        expected=tuple(i for i in matched_ids if i in self._missing_set)
        _consistent(remote_ids==expected,'Cache remote vector coverage/order mismatch')
        _consistent(isinstance(remote_vectors,np.ndarray) and remote_vectors.dtype==np.dtype('<f4')
                    and remote_vectors.shape==(len(remote_ids),self._scope.dimension),'Cache vector format mismatch')
        _consistent(np.isfinite(remote_vectors).all(),'Nonfinite cache vector')
        for i,vector in zip(remote_ids,remote_vectors):self._vectors[self._positions[i]]=vector
        copied=time.monotonic()
        result=self._vectors[[self._positions[i] for i in matched_ids]]
        self.statistics['copy_ms']+=(time.monotonic()-copied)*1000
        self.statistics['vectors_served_from_cache']=len(matched_ids)-len(remote_ids)
        check()
        if admit:
            started=time.monotonic()
            self._cache._admit(self,remote_ids,remote_vectors,check)
            self.statistics['admit_ms']+=(time.monotonic()-started)*1000
        check()
        return result


class VersionedVectorCache:
    def __init__(self, config: CacheConfig):
        if not isinstance(config,CacheConfig):raise ValueError('Invalid cache configuration')
        self.config=config
        self._lock=RLock()
        self._entries=OrderedDict();self._scopes={}
        self._bytes=0;self._payload_bytes=0;self._generation=0
        self._active=0;self._workspace_peak=0;self._closed=False

    @contextmanager
    def batch(self, scope: DataScope, ids, check):
        if not isinstance(scope,DataScope):raise ValueError('Invalid data scope')
        ids=_ids(ids)
        if len(ids)*scope.dimension*4>1024**2:raise ValueError('Cache batch exceeds 1 MiB')
        check()
        with self._lock:
            enabled=(not self._closed and self.config.max_bytes>0 and self._active<self.config.workspace_slots
                     and (self._active+1)*WORKSPACE_CHARGE<=self.config.workspace_bytes)
            if enabled:
                self._active+=1
                self._workspace_peak=max(self._workspace_peak,self._active*WORKSPACE_CHARGE)
        if not enabled:
            yield None
            return
        batch=None
        try:
            batch=LookupBatch(self,scope,ids,check)
            yield batch
        finally:
            if batch is not None:
                batch._closed=True
                batch._vectors=None
            with self._lock:self._active-=1

    def _drop(self, key):
        raw=self._entries.pop(key);scope=key[0]
        self._bytes-=ENTRY_CHARGE+len(raw);self._payload_bytes-=len(raw)
        self._scopes[scope]-=1
        if not self._scopes[scope]:
            del self._scopes[scope];self._bytes-=SCOPE_CHARGE

    def _admit(self, batch, ids, vectors, check):
        scope=batch._scope;statistics=batch.statistics
        with self._lock:
            check()
            if batch.generation!=self._generation:return
            # Validate conflicts across the whole completed response before mutating.
            for identity,vector in zip(ids,vectors):
                previous=self._entries.get((scope,identity))
                _consistent(previous is None or previous==vector.tobytes(),'cache_content_conflict')
            row_charge=ENTRY_CHARGE+scope.dimension*4
            if SCOPE_CHARGE+row_charge>self.config.max_bytes:return
            for identity,vector in zip(ids,vectors):
                check()
                key=(scope,identity)
                if key in self._entries:
                    self._entries.move_to_end(key);statistics['duplicate_inserts']+=1
                    continue
                while self._entries and (len(self._entries)>=self.config.max_entries or
                    self._bytes+row_charge+(0 if scope in self._scopes else SCOPE_CHARGE)>self.config.max_bytes):
                    self._drop(next(iter(self._entries)));statistics['evictions']+=1
                raw=vector.tobytes()
                if scope not in self._scopes:
                    self._scopes[scope]=0;self._bytes+=SCOPE_CHARGE
                self._scopes[scope]+=1;self._bytes+=row_charge;self._payload_bytes+=len(raw)
                self._entries[key]=raw
                statistics['admitted_rows']+=1;statistics['admitted_payload_bytes']+=len(raw)

    def stats(self, database_id):
        with self._lock:
            rows=[(scope,len(raw)) for (scope,_),raw in self._entries.items() if scope.database_id==database_id]
            scopes={s for s,_ in rows};payload=sum(size for _,size in rows)
            return dict(data_cache_version=1,process=dict(max_bytes=self.config.max_bytes,
                max_entries=self.config.max_entries,accounted_bytes=self._bytes,payload_bytes=self._payload_bytes,
                entry_count=len(self._entries),scope_count=len(self._scopes),workspace_bytes=self._active*WORKSPACE_CHARGE,
                workspace_peak_bytes=self._workspace_peak,admission_generation=self._generation),
                database=dict(database_id=database_id,entry_count=len(rows),scope_count=len(scopes),payload_bytes=payload,
                              accounted_bytes=len(scopes)*SCOPE_CHARGE+len(rows)*ENTRY_CHARGE+payload))

    def clear(self, database_id):
        with self._lock:
            self._generation+=1
            keys=[key for key in self._entries if key[0].database_id==database_id]
            for key in keys:self._drop(key)
            return dict(data_cache_version=1,database_id=database_id,removed=len(keys),admission_generation=self._generation)

    def close(self):
        with self._lock:
            self._closed=True;self._generation+=1
            self._entries.clear();self._scopes.clear()
            self._bytes=0;self._payload_bytes=0
