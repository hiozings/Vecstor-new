"""Detached planning values: no live cache, index, RPC, or database ownership."""
from collections.abc import Mapping
from dataclasses import dataclass, fields, is_dataclass
import hashlib
import json
from types import MappingProxyType

from vecstor.cache.types import DataScope
from vecstor.query.search_options import SearchOptions


class FrozenMap(Mapping):
    """Owned immutable mapping; safe to share in subsequent planning snapshots."""
    __slots__=('_items',)

    def __init__(self, values):
        object.__setattr__(self,'_items',MappingProxyType({key:freeze(value) for key,value in values.items()}))

    def __setattr__(self, name, value):raise TypeError('Immutable planning mapping')
    def __getitem__(self, key):return self._items[key]
    def __iter__(self):return iter(self._items)
    def __len__(self):return len(self._items)
    def get(self, key, default=None):return self._items.get(key,default)
    def items(self):return self._items.items()
    def keys(self):return self._items.keys()
    def values(self):return self._items.values()


def freeze(value):
    kind=type(value)
    if value is None or kind in (str,int,float,bool):return value
    if kind is dict:return FrozenMap(value)
    if kind in (list,tuple):return tuple(freeze(item) for item in value)
    if isinstance(value,(FrozenMap,DataScope,SearchOptions,FrozenValues,DeploymentContext)):
        return value
    # Preserve support for mapping/sequence subclasses without paying ABC
    # dispatch costs for the owned dict/tuple values used by every query.
    if isinstance(value,Mapping):return FrozenMap(value)
    if isinstance(value,(list,tuple)):return tuple(freeze(item) for item in value)
    raise TypeError('Unsupported mutable planning value')


def plain(value):
    if value is None or type(value) in (str, int, float, bool):return value
    if isinstance(value,FrozenMap) or type(value) is dict:
        return {key:plain(item) for key,item in value.items()}
    if type(value) in (list,tuple):return [plain(item) for item in value]
    if isinstance(value,(FrozenValues,DeploymentContext,DataScope,SearchOptions)):
        return {key:plain(item) for key,item in vars(value).items()}
    if is_dataclass(value):
        return {field.name: plain(getattr(value, field.name)) for field in fields(value)}
    if isinstance(value, Mapping):
        return {key: plain(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [plain(item) for item in value]
    return value


def fingerprint(value):
    return hashlib.sha256(json.dumps(plain(value), sort_keys=True, separators=(',', ':'),
                                     allow_nan=False).encode()).hexdigest()


class FrozenValues:
    def __post_init__(self):
        for name,value in vars(self).items():
            if value is not None and type(value) not in (str,int,float,bool):
                object.__setattr__(self,name,freeze(value))


@dataclass(frozen=True)
class DeploymentContext:
    hardware_fingerprint: str | None
    network_class: str | None
    active_vector_requests: int

    @property
    def concurrency_bucket(self):
        n = self.active_vector_requests
        return 1 if n == 1 else 4 if 2 <= n <= 4 else 8 if 5 <= n <= 8 else None


@dataclass(frozen=True)
class PlanCandidate(FrozenValues):
    candidate_id: str
    mode: str
    path: str
    options: SearchOptions
    quality_description: Mapping | None
    execution_fingerprint: str


@dataclass(frozen=True)
class PlanningInputs(FrozenValues):
    scope: DataScope
    artifact_identity: Mapping
    table_rows: int | None
    dimension: int
    k: int
    metadata_sql_hash: str
    query_hash: str
    candidates: tuple[PlanCandidate, ...]
    reference_id: str
    statistics: Mapping
    cache_snapshot: Mapping
    context: DeploymentContext
    policy_id: str | None
    policy_hash: str | None
    remaining_ms: float


@dataclass(frozen=True)
class CandidateEstimate(FrozenValues):
    candidate_id: str
    eligible: bool = True
    reason: str | None = None
    mean_ms: float | None = None
    lower_ms: float | None = None
    upper_ms: float | None = None
    rpc_calls: float | None = None
    payload_bytes: float | None = None
    vectors_reranked: float | None = None
    vectors_missed: float | None = None
    workspace_bytes: float | None = None
    failure_upper: float | None = None
    components: Mapping | None = None


@dataclass(frozen=True)
class PlannerDecision(FrozenValues):
    reference_id: str
    proposed_id: str
    executed_id: str
    reason: str
    estimates: tuple[CandidateEstimate, ...]
    policy_id: str | None
    policy_hash: str | None
    mode: str


@dataclass(frozen=True)
class ResolvedPlan:
    candidate: PlanCandidate
    decision: PlannerDecision
