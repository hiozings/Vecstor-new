"""Query-local cache accounting over the validated, lease-bound RPC reader."""
from dataclasses import asdict

import grpc
import numpy as np


class CachedLeaseReader:
    def __init__(self,io,cache,fields,projected,cache_enabled):
        self.io,self.cache,self.fields,self.projected=io,cache,tuple(fields),projected
        self.enabled=bool(cache_enabled and cache is not None and cache.config.max_bytes)
        self.scope=io.configure_cache_lease() if self.enabled else None
        self.statistics=dict(requested='default',effective='on' if self.enabled else 'off',
            reason=None if self.enabled else 'disabled',scope=asdict(self.scope) if self.scope else None,
            lookup_rows=0,hit_rows=0,miss_rows=0,bypass_rows=0,admitted_rows=0,admitted_payload_bytes=0,
            evictions=0,duplicate_inserts=0,retry_lookup_rows=0,vectors_served_from_cache=0,lookup_ms=0.,copy_ms=0.,admit_ms=0.,
            scan_admission=cache.config.scan_admission if cache is not None else False)
        io.trace['candidate_read_protocol']='legacy'
        io.trace['counts'].setdefault('projected_size_splits',0)
        io.trace['metadata_requested_fields']=list(fields)
        io.trace['counts'].setdefault('read_subbatches',0)

    def _merge(self,statistics):
        for key,value in statistics.items():self.statistics[key]+=value

    def _fetch(self,ids):
        if ids:self.io.trace['counts']['read_subbatches']+=1
        return self.io.fetch(ids)

    def fetch(self,ids,*,scan=False):
        self.io.remaining()
        if not ids:return np.empty((0,self.io.lease.dimension),dtype='<f4')
        if not self.enabled:return self._fetch(ids)
        with self.cache.batch(self.scope,tuple(ids),self.io.remaining) as batch:
            if batch is None:
                self.statistics['bypass_rows']+=len(ids);self.statistics['reason']='workspace_busy'
                return self._fetch(ids)
            vectors=self._fetch(list(batch.missing_ids))
            result=batch.finish(tuple(ids),batch.missing_ids,vectors,
                                admit=not scan or self.cache.config.scan_admission,check=self.io.remaining)
            self._merge(batch.statistics)
            return result

    def scan(self,sql):
        return self.io.scan_projected(sql,self.fields) if self.projected else self.io.scan(sql)

    def filter(self,sql,ids):
        if not self.projected:return self.io.filter(sql,ids)
        self.io.remaining()
        self.io.trace['candidate_read_protocol']='selective'
        self.io.trace['counts']['read_subbatches']+=1
        try:
            rows,_,_=self.io.read_projected_candidates(sql,ids,self.fields,[])
            return rows
        except grpc.RpcError as error:
            if error.code()!=grpc.StatusCode.RESOURCE_EXHAUSTED or error.details()!='projected_response_too_large' or len(ids)<=1:raise
        self.io.trace['counts']['projected_size_splits']+=1
        middle=len(ids)//2
        return self.filter(sql,ids[:middle])+self.filter(sql,ids[middle:])

    def _selective_once(self,sql,ids):
        self.io.trace['candidate_read_protocol']='selective'
        if not self.enabled:
            self.io.trace['counts']['read_subbatches']+=1
            rows,_,vectors=self.io.read_projected_candidates(sql,ids,self.fields,ids)
            return rows,vectors
        with self.cache.batch(self.scope,tuple(ids),self.io.remaining) as batch:
            missing=list(batch.missing_ids) if batch is not None else ids
            self.io.trace['counts']['read_subbatches']+=1
            try:
                rows,returned,vectors=self.io.read_projected_candidates(sql,ids,self.fields,missing)
            except grpc.RpcError:
                if batch is not None:
                    self.statistics['retry_lookup_rows']+=len(ids)
                    for key in ('lookup_ms','copy_ms'):self.statistics[key]+=batch.statistics[key]
                raise
            if batch is None:
                self.statistics['bypass_rows']+=len(ids);self.statistics['reason']='workspace_busy'
                return rows,vectors
            result=batch.finish(tuple(row['id'] for row in rows),tuple(returned),vectors,admit=True,check=self.io.remaining)
            self._merge(batch.statistics)
            return rows,result

    def filter_fetch(self,sql,ids):
        if not self.enabled and not self.projected:return self.io.filter_fetch(sql,ids)
        if not ids:return [],np.empty((0,self.io.lease.dimension),dtype='<f4')
        self.io.remaining()
        try:return self._selective_once(sql,ids)
        except grpc.RpcError as error:
            if error.code()!=grpc.StatusCode.RESOURCE_EXHAUSTED or error.details()!='projected_response_too_large' or len(ids)==1:raise
        # The failed attempt's workspace is released before recursively splitting.
        self.io.trace['counts']['projected_size_splits']+=1
        middle=len(ids)//2
        left,lv=self.filter_fetch(sql,ids[:middle]);right,rv=self.filter_fetch(sql,ids[middle:])
        return left+right,np.concatenate((lv,rv),axis=0)

    def describe(self):
        stats=dict(self.statistics)
        stats['hit_rate']=stats['hit_rows']/stats['lookup_rows'] if stats['lookup_rows'] else None
        stats['cache_payload_bytes_avoided']=stats['vectors_served_from_cache']*self.io.lease.dimension*4
        if self.cache is not None:
            snapshot=self.cache.process_snapshot()
            stats.update(accounted_bytes=snapshot['accounted_bytes'],entry_count=snapshot['entry_count'])
        else:stats.update(accounted_bytes=0,entry_count=0)
        return stats
