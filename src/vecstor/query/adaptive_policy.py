"""Strict, bounded, immutable offline cost evidence; not an ANN quality authority."""
from collections.abc import Mapping
from dataclasses import dataclass
import json
import math
from pathlib import Path
import re
from types import MappingProxyType

from vecstor.compute.planner_types import FrozenValues, fingerprint, freeze
from vecstor.query.search_options import validate_profile_id

MAX_BYTES = 2*1024**2
DEFAULT_PARAMETERS = MappingProxyType(dict(switch_min_ms=2., switch_margin=.1,
    risk_limit=.01, filter_workspace_bytes=64*1024**2, use_cache_evidence=True,
    include_transfer_cost=True, use_candidate_correlation=True, use_observations=True))
FEATURES = ('intercept', 'scan_rows', 'metadata_rows', 'candidate_rows', 'ann_work',
            'ann_candidates', 'selector_rows', 'rerank_values', 'cache_lookup_rows',
            'cache_copy_bytes', 'cache_admit_bytes', 'rpc_calls', 'payload_bytes')
DATA_FIELDS = ('database_id', 'table_id', 'revision', 'schema_version', 'dimension', 'metric')
ARTIFACT_FIELDS = ('artifact_id', 'payload_sha256', 'build_fingerprint')


def _fields(value, names):
    if not isinstance(value, dict) or set(value) != set(names):
        raise ValueError('Invalid adaptive policy fields')
    return value


def _text(value, limit=128):
    if not isinstance(value, str) or not value or '\x00' in value or len(value.encode()) > limit:
        raise ValueError('Invalid adaptive policy text')


def _int(value, low, high):
    if type(value) is not int or not low <= value <= high:
        raise ValueError('Invalid adaptive policy integer')


def _number(value, low=0., high=1e100):
    if type(value) not in (int, float) or not math.isfinite(value) or not low <= value <= high:
        raise ValueError('Invalid adaptive policy number')


def _digest(value):
    if not isinstance(value, str) or re.fullmatch('[0-9a-f]{64}', value) is None:
        raise ValueError('Invalid adaptive policy digest')


def _unique(pairs):
    result = {}
    for key, value in pairs:
        if key in result: raise ValueError('Duplicate adaptive JSON key')
        result[key] = value
    return result


def _model(value):
    _fields(value, ('candidate_id','execution_fingerprint','hardware_fingerprint','network_class',
                    'concurrency_bucket','coefficients','residual_ms','statistics','evidence'))
    if value['candidate_id'] not in ('exact','ann-filter','ann-search'):
        raise ValueError('Invalid adaptive candidate')
    _digest(value['execution_fingerprint'])
    for key in ('hardware_fingerprint','network_class'): _text(value[key])
    if type(value['concurrency_bucket']) is not int or value['concurrency_bucket'] not in (1,4,8):
        raise ValueError('Invalid adaptive concurrency bucket')
    coefficients = value['coefficients']
    if not isinstance(coefficients, dict) or not coefficients or not set(coefficients).issubset(FEATURES):
        raise ValueError('Invalid adaptive cost features')
    for coefficient in coefficients.values(): _number(coefficient)
    _number(value['residual_ms'])
    stats = _fields(value['statistics'], ('q','unique_fractions','failure_queries','failure_count',
                     'metadata_bytes_per_row','control_bytes','selector_bytes_per_row'))
    for key in ('q','unique_fractions'):
        if not isinstance(stats[key], list) or not 1 <= len(stats[key]) <= 32:
            raise ValueError('Invalid adaptive round statistics')
        for number in stats[key]: _number(number, 0, 1)
    _int(stats['failure_queries'], 0, 2**32-1)
    _int(stats['failure_count'], 0, stats['failure_queries'])
    for key in ('metadata_bytes_per_row','control_bytes','selector_bytes_per_row'): _number(stats[key])
    evidence = _fields(value['evidence'], ('fit_queries','validation_queries','median_relative_error',
                     'p90_relative_error','fit_queries_sha256','validation_queries_sha256'))
    _int(evidence['fit_queries'], 1, 2**32-1)
    _int(evidence['validation_queries'], 1, 2**32-1)
    for key in ('median_relative_error','p90_relative_error'): _number(evidence[key])
    for key in ('fit_queries_sha256','validation_queries_sha256'): _digest(evidence[key])
    if evidence['fit_queries_sha256'] == evidence['validation_queries_sha256']:
        raise ValueError('Adaptive fit and validation queries must differ')


@dataclass(frozen=True)
class PolicyMatch(FrozenValues):
    matched_rows: int | None = None
    table_rows: int | None = None
    profiles: Mapping | None = None
    models: tuple = ()
    cost_reason: str | None = 'workload_unknown'
    workload_id: str | None = None


class AdaptivePolicy:
    def __init__(self, document):
        self._document = freeze(document)
        self.policy_id = document['policy_id']
        self.policy_hash = fingerprint(document)
        self.parameters = freeze(dict(DEFAULT_PARAMETERS, **document.get('parameters',{})))
        self._workloads = MappingProxyType({self._key(w['data_identity'],w['metadata_sql'],w['k']): freeze(w)
                                           for w in self._document['workloads']})

    @staticmethod
    def _key(identity, sql, k):
        return tuple(identity[field] for field in DATA_FIELDS)+(sql,k)

    @classmethod
    def load(cls, path):
        with Path(path).open('rb') as file:
            raw = file.read(MAX_BYTES+1)
        if len(raw) > MAX_BYTES: raise ValueError('Adaptive policy exceeds 2 MiB')
        try: value = json.loads(raw, object_pairs_hook=_unique)
        except (UnicodeError, json.JSONDecodeError, RecursionError) as error:
            raise ValueError('Invalid adaptive policy JSON') from error
        return cls.from_document(value)

    @classmethod
    def from_document(cls, value):
        try: encoded = json.dumps(value, allow_nan=False).encode()
        except (TypeError, ValueError, RecursionError) as error:
            raise ValueError('Invalid adaptive policy JSON values') from error
        if len(encoded) > MAX_BYTES: raise ValueError('Adaptive policy exceeds 2 MiB')
        _fields(value, ('format_version','policy_id','workloads') + (('parameters',) if 'parameters' in value else ()))
        parameters=value.get('parameters',{})
        if not isinstance(parameters,dict) or not set(parameters).issubset(DEFAULT_PARAMETERS):
            raise ValueError('Invalid planner parameters')
        for key,number in parameters.items():
            if type(DEFAULT_PARAMETERS[key]) is bool:
                if type(number) is not bool:raise ValueError('Invalid ablation parameter')
            elif key=='filter_workspace_bytes':_int(number,1,2**40)
            else:_number(number,0,1000 if key=='switch_min_ms' else 1)
        if type(value['format_version']) is not int or value['format_version'] != 1:
            raise ValueError('Unsupported adaptive policy version')
        validate_profile_id(value['policy_id'])
        workloads = value['workloads']
        if not isinstance(workloads,list) or len(workloads) > 64:
            raise ValueError('Adaptive policy exceeds 64 workloads')
        keys = set(); names = set()
        for work in workloads:
            _fields(work, ('workload_id','data_identity','artifact_identity','metadata_sql','k',
                          'table_rows','matched_rows','profiles','models'))
            _text(work['workload_id']); _text(work['metadata_sql'],65536)
            identity = _fields(work['data_identity'], DATA_FIELDS)
            for key in ('database_id','table_id','schema_version'): _text(identity[key])
            _int(identity['revision'],0,2**64-1); _int(identity['dimension'],1,262144)
            if identity['metric'] != 'squared_l2': raise ValueError('Invalid adaptive metric')
            artifact = _fields(work['artifact_identity'], ARTIFACT_FIELDS)
            _text(artifact['artifact_id'])
            for key in ('payload_sha256','build_fingerprint'): _digest(artifact[key])
            _int(work['k'],1,2**32-1); _int(work['table_rows'],0,2**63-1)
            if work['matched_rows'] is not None: _int(work['matched_rows'],0,work['table_rows'])
            profiles = work['profiles']
            if not isinstance(profiles,dict) or not set(profiles).issubset(('filter-first','search-first')):
                raise ValueError('Invalid adaptive quality mappings')
            for profile in profiles.values(): validate_profile_id(profile)
            models = work['models']
            if not isinstance(models,list) or len(models) > 48: raise ValueError('Too many adaptive models')
            model_keys = set()
            for model in models:
                _model(model)
                mk = tuple(model[k] for k in ('candidate_id','execution_fingerprint','hardware_fingerprint',
                                               'network_class','concurrency_bucket'))
                if mk in model_keys: raise ValueError('Duplicate adaptive model')
                model_keys.add(mk)
            key = cls._key(identity,work['metadata_sql'],work['k'])
            if key in keys or work['workload_id'] in names: raise ValueError('Duplicate adaptive workload')
            keys.add(key); names.add(work['workload_id'])
        return cls(value)

    def lookup(self, scope, artifact, metadata_sql, k, context):
        identity = {name: getattr(scope,name) for name in DATA_FIELDS if name != 'metric'}
        identity['metric'] = 'squared_l2'
        work = self._workloads.get(self._key(identity, metadata_sql, k))
        if work is None: return PolicyMatch(profiles={})
        common = dict(matched_rows=work['matched_rows'],table_rows=work['table_rows'],workload_id=work['workload_id'])
        if not artifact or any(artifact.get(key) != work['artifact_identity'][key] for key in ARTIFACT_FIELDS):
            return PolicyMatch(**common,profiles={},cost_reason='artifact_mismatch')
        models = tuple(m for m in work['models'] if
            m['hardware_fingerprint'] == context.hardware_fingerprint and
            m['network_class'] == context.network_class and m['concurrency_bucket'] == context.concurrency_bucket)
        return PolicyMatch(**common, profiles=work['profiles'], models=models,
                           cost_reason=None if models else 'deployment_context_mismatch')

    def describe(self):
        return dict(format_version=1,policy_id=self.policy_id,policy_hash=self.policy_hash,
                    workload_count=len(self._workloads),parameters=dict(self.parameters))
