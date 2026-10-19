"""Query-local cache accounting over the validated, lease-bound RPC reader."""
from dataclasses import asdict

import numpy as np


class CachedLeaseReader:
    def __init__(self,io,cache,fields,projected,cache_enabled):
        self.io,self.cache,self.fields,self.projected=io,cache,tuple(fields),projected
        self.enabled=bool(cache_enabled and cache is not None and cache.config.max_bytes)
        self.scope=io.configure_cache_lease() if self.enabled else None
        self.statistics=dict(requested='default',effective='on' if self.enabled else 'off',
            reason=None if self.enabled else 'disabled',scope=asdict(self.scope) if self.scope else None,
            lookup_rows=0,hit_rows=0,miss_rows=0,bypass_rows=0,admitted_rows=0,admitted_payload_bytes=0,
            evictions=0,duplicate_inserts=0,vectors_served_from_cache=0,lookup_ms=0.,copy_ms=0.,admit_ms=0.,
            scan_admission=cache.config.scan_admission if cache is not None else False)
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
        self.io.trace['counts']['read_subbatches']+=1
        rows,_,_=self.io.read_projected_candidates(sql,ids,self.fields,[])
        return rows

    def filter_fetch(self,sql,ids):
        # Legacy fused path remains unchanged until selective cache integration.
        return self.io.filter_fetch(sql,ids)

    def describe(self):
        stats=dict(self.statistics)
        stats['hit_rate']=stats['hit_rows']/stats['lookup_rows'] if stats['lookup_rows'] else None
        stats['cache_payload_bytes_avoided']=stats['vectors_served_from_cache']*self.io.lease.dimension*4
        if self.cache is not None:
            snapshot=self.cache.stats(self.io.database_id)
            stats.update(accounted_bytes=snapshot['process']['accounted_bytes'],entry_count=snapshot['process']['entry_count'])
        else:stats.update(accounted_bytes=0,entry_count=0)
        return stats
