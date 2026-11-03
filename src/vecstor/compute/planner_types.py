"""Detached planning values: no live cache, index, RPC, or database ownership."""
from collections.abc import Mapping
from dataclasses import dataclass, fields, is_dataclass
import hashlib
import json
from types import MappingProxyType

from vecstor.cache.types import DataScope
from vecstor.query.search_options import SearchOptions


def freeze(value):
    if isinstance(value, Mapping):
        return MappingProxyType({key: freeze(item) for key, item in value.items()})
    if isinstance(value, (list, tuple)):
        return tuple(freeze(item) for item in value)
    if value is None or type(value) in (str, int, float, bool):
        return value
    if is_dataclass(value) and getattr(value, '__dataclass_params__').frozen:
        return value
    raise TypeError('Unsupported mutable planning value')


def plain(value):
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
        for field in fields(self):
            object.__setattr__(self, field.name, freeze(getattr(self, field.name)))


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
