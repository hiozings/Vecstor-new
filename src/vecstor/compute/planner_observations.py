"""Bounded scalar evidence, never a filter/result cache or query plan cache."""
from collections import OrderedDict
from dataclasses import dataclass
import json
import math
from threading import RLock
import time

from vecstor.compute.planner_types import FrozenValues, fingerprint, plain


@dataclass(frozen=True)
class Observation(FrozenValues):
    key: tuple
    values: dict


class ObservationStore:
    def __init__(self, max_entries=256, entry_charge=8192, ttl_seconds=300):
        if type(max_entries) is not int or not 1 <= max_entries <= 256 or entry_charge != 8192:
            raise ValueError('Invalid planner history bound')
        if type(ttl_seconds) not in (int,float) or not 0 < ttl_seconds <= 300:
            raise ValueError('Invalid planner history TTL')
        self.max_entries,self.entry_charge,self.ttl_seconds=max_entries,entry_charge,ttl_seconds
        self._lock=RLock();self._records=OrderedDict()

    @staticmethod
    def _now(now):
        now=time.monotonic() if now is None else now
        if type(now) not in (int,float) or not math.isfinite(now):raise ValueError('Invalid observation time')
        return now

    def peek(self, key, now=None):
        now=self._now(now)
        with self._lock:
            value=self._records.get(key)
            return value[1] if value is not None and now < value[0] else None

    def get(self, key, now=None):
        now=self._now(now)
        with self._lock:
            value=self._records.get(key)
            if value is None:return None
            if now >= value[0]:
                del self._records[key];return None
            self._records.move_to_end(key)
            return value[1]

    def record(self, observation, now=None):
        now=self._now(now)
        if not isinstance(observation,Observation):raise ValueError('Invalid observation')
        if not 1 <= len(observation.key) <= 8 or any(not isinstance(k,str) or len(k)>128 for k in observation.key):
            raise ValueError('Invalid observation key')
        fields={'matched_rows','conflict','warm','admission_generation','loss_generation',
                'budget_failure','residual_ms','bias_ms','rounds','hit_rows','lookup_rows'}
        if not set(observation.values).issubset(fields):raise ValueError('Invalid observation fields')
        for key,value in observation.values.items():
            if key in ('conflict','warm','budget_failure'):
                if type(value) is not bool:raise ValueError('Invalid observation flag')
            elif key=='matched_rows' and value is None:pass
            elif key in ('residual_ms','bias_ms'):
                if type(value) not in (int,float) or not math.isfinite(value) or (key=='residual_ms' and value<0):
                    raise ValueError('Invalid observation residual')
            elif key=='rounds':
                if len(value)>32:raise ValueError('Too many observed rounds')
                for row in value:
                    if set(row)!={'new_candidates','matched_candidates'} or any(type(v) is not int or not 0<=v<2**64 for v in row.values()):
                        raise ValueError('Invalid observed round')
            elif type(value) is not int or not 0<=value<2**64:raise ValueError('Invalid observation count')
        if len(json.dumps(plain(observation),allow_nan=False).encode())>self.entry_charge:
            raise ValueError('Observation exceeds fixed charge')
        with self._lock:
            if observation.key not in self._records and len(self._records)>=self.max_entries:
                self._records.popitem(last=False)
            self._records[observation.key]=(now+self.ttl_seconds,observation)
            self._records.move_to_end(observation.key)

    def record_cardinality(self, key, count, now=None):
        now=self._now(now)
        with self._lock:
            previous=self.peek(key,now)
            conflict=previous is not None and (previous.values.get('conflict',False) or previous.values.get('matched_rows')!=count)
            self.record(Observation(key,{'matched_rows':None,'conflict':True} if conflict else {'matched_rows':count}),now)

    def note_budget_failure(self, key, now=None):
        self.record(Observation(key,{'budget_failure':True}),now)

    def record_many(self, observations, now=None, check=lambda:None):
        """Commit up to four scalar observations, rolling back on cancellation.

        Rollback restores values/expiry and any evictions; recency may reflect
        this request's accesses, just like get() during a failed query.
        """
        now=self._now(now);observations=tuple(observations)
        if len(observations)>4:raise ValueError('Too many query observations')
        with self._lock:
            journal=[];check()
            try:
                for observation in observations:
                    key=observation.key;previous=self._records.get(key);evicted=None
                    if previous is None and len(self._records)>=self.max_entries:
                        oldest=next(iter(self._records));evicted=(oldest,self._records[oldest])
                    journal.append((key,previous,evicted))
                    if key[0]=='cardinality':
                        self.record_cardinality(key,observation.values['matched_rows'],now)
                    else:self.record(observation,now)
                check()
            except BaseException:
                for key,previous,evicted in reversed(journal):
                    self._records.pop(key,None)
                    if previous is not None:self._records[key]=previous
                    if evicted is not None:
                        self._records[evicted[0]]=evicted[1]
                        self._records.move_to_end(evicted[0],last=False)
                raise

    def stats(self):
        with self._lock:
            return dict(entries=len(self._records),accounted_bytes=len(self._records)*self.entry_charge,
                        max_entries=self.max_entries,ttl_seconds=self.ttl_seconds)


def observation_key(inputs, kind, candidate=None):
    statistics=getattr(inputs,'statistics',{})
    key=(kind,statistics.get('_scope_hash') or fingerprint(inputs.scope),inputs.metadata_sql_hash)
    if kind!='cardinality':
        key+=(statistics.get('_artifact_hash') or fingerprint(inputs.artifact_identity),str(inputs.k),candidate.execution_fingerprint)
    if kind=='reuse':key+=(inputs.query_hash,)
    return key


def make_reuse_observation(inputs, trace, before, after):
    stats=trace.get('data_cache',{})
    count=trace.get('counts',{}).get('vectors_used_for_rerank',0)
    if not count or stats.get('effective')!='on' or trace.get('fallback_reason'):
        return None
    if stats.get('bypass_rows',0) or stats.get('evictions',0):return None
    if any(before.get(k)!=after.get(k) for k in ('admission_generation','loss_generation')):
        return None
    if not after.get('scope_entry_count'):return None
    retained=sum(stats.get(k,0) for k in ('vectors_served_from_cache','admitted_rows','duplicate_inserts'))
    if retained<count:return None
    candidate=next((c for c in inputs.candidates if (c.mode,c.path)==(trace.get('actual_mode'),trace.get('actual_path'))),None)
    if candidate is None:return None
    return Observation(observation_key(inputs,'reuse',candidate),dict(warm=True,
        admission_generation=after['admission_generation'],loss_generation=after['loss_generation']))
