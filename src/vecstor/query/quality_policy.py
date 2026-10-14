"""Snapshot-bound empirical quality profiles; never an online Recall guarantee."""
from dataclasses import asdict, dataclass, replace
import math
from types import MappingProxyType

from vecstor.index.types import IndexKey
from vecstor.query.search_options import validate_profile_id


class QualityUnavailable(ValueError):
    def __init__(self,reason):self.reason=reason;super().__init__(reason)


def _fields(value,names):
    if not isinstance(value,dict) or set(value)!=set(names):raise ValueError('Invalid quality profile fields')
    return value


def _integer(value,low,high):
    if type(value) is not int or not low<=value<=high:raise ValueError('Invalid quality profile integer')
    return value


def _number(value,low=None,high=None):
    if type(value) not in (int,float) or not math.isfinite(value) or (low is not None and value<low) or (high is not None and value>high):
        raise ValueError('Invalid quality profile number')
    return float(value)


def _text(value,maximum=256):
    if not isinstance(value,str) or not value or len(value)>maximum or '\x00' in value:raise ValueError('Invalid quality profile text')
    return value


@dataclass(frozen=True)
class QualityBudget:
    nprobe_floor:int
    initial_candidates:int
    max_candidates:int
    max_rounds:int


@dataclass(frozen=True)
class QualityProfile:
    profile_id:str
    key:IndexKey
    metadata_sql:str
    k:int
    path:str
    query_min:float
    query_max:float
    budget:QualityBudget
    evidence:tuple

    @classmethod
    def parse(cls,value):
        _fields(value,['format_version','profile_id','index_key','workload','budget','evidence'])
        if type(value['format_version']) is not int or value['format_version']!=1:raise ValueError('Unsupported quality profile version')
        profile_id=validate_profile_id(value['profile_id'])
        raw=_fields(value['index_key'],IndexKey.__dataclass_fields__)
        for name in ['database_id','table_id','schema_version','build_fingerprint']:_text(raw[name])
        if raw['metric']!='squared_l2':raise ValueError('Quality metric mismatch')
        _integer(raw['revision'],0,2**64-1);_integer(raw['dimension'],1,262144)
        key=IndexKey(**raw)
        work=_fields(value['workload'],['metadata_sql','k','path','query_min','query_max'])
        _text(work['metadata_sql'],65536);_integer(work['k'],1,2**32-1)
        if work['path'] not in ('filter-first','search-first'):raise ValueError('Invalid quality path')
        minimum=_number(work['query_min']);maximum=_number(work['query_max'])
        if minimum>maximum:raise ValueError('Invalid quality query domain')
        raw_budget=_fields(value['budget'],QualityBudget.__dataclass_fields__)
        for name,x in raw_budget.items():_integer(x,1,32 if name=='max_rounds' else 2**32-1)
        if raw_budget['initial_candidates']>raw_budget['max_candidates']:raise ValueError('Invalid quality candidate budget')
        budget=QualityBudget(**raw_budget)
        evidence=_fields(value['evidence'],['calibration_queries','evaluation_queries','calibration_mean_recall','evaluation_mean_recall',
            'incomplete_queries','exact_queries','dataset_sha256','calibration_queries_sha256','evaluation_queries_sha256','scope'])
        _integer(evidence['calibration_queries'],200,1000000);_integer(evidence['evaluation_queries'],500,1000000)
        _number(evidence['calibration_mean_recall'],.97,1);_number(evidence['evaluation_mean_recall'],.95,1)
        for name in ['incomplete_queries','exact_queries']:
            if type(evidence[name]) is not int or evidence[name]!=0:raise ValueError('Quality evidence includes exact or incomplete queries')
        for name in ['dataset_sha256','calibration_queries_sha256','evaluation_queries_sha256']:
            x=_text(evidence[name],64)
            if len(x)!=64 or any(c not in '0123456789abcdef' for c in x):raise ValueError('Invalid quality evidence digest')
        _text(evidence['scope'])
        return cls(profile_id,key,work['metadata_sql'],work['k'],work['path'],minimum,maximum,budget,tuple(sorted(evidence.items())))

    def describe(self):
        evidence=dict(self.evidence)
        return {'profile_id':self.profile_id,'database_id':self.key.database_id,'table_id':self.key.table_id,'revision':self.key.revision,
                'path':self.path,'k':self.k,'metadata_sql':self.metadata_sql,'budget':asdict(self.budget),
                'calibration_queries':evidence['calibration_queries'],'evaluation_queries':evidence['evaluation_queries'],
                'evaluation_mean_recall':evidence['evaluation_mean_recall'],'quality_basis':'offline empirical mean, not per-query guarantee'}


class QualityCatalog:
    def __init__(self,profiles=()):self._profiles=MappingProxyType({p.profile_id:p for p in profiles})

    @classmethod
    def from_document(cls,value):
        if value is None:return cls()
        _fields(value,['format_version','profiles'])
        if type(value['format_version']) is not int or value['format_version']!=1:raise ValueError('Unsupported quality catalog version')
        raw=value['profiles']
        if not isinstance(raw,list) or len(raw)>128:raise ValueError('Quality catalog exceeds 128 profiles')
        profiles=[QualityProfile.parse(p) for p in raw]
        if len({p.profile_id for p in profiles})!=len(profiles):raise ValueError('Duplicate quality profile IDs')
        return cls(profiles)

    def list(self):return [p.describe() for p in self._profiles.values()]

    def resolve(self,profile_id,key,nlist,row_count,plan,options):
        profile=self._profiles.get(profile_id)
        if profile is None:raise QualityUnavailable('quality_profile_missing')
        if profile.key!=key:raise QualityUnavailable('quality_profile_version_mismatch')
        if (profile.metadata_sql!=plan.metadata_sql or profile.k!=plan.limit or profile.path!=options.path
            or any(not profile.query_min<=x<=profile.query_max for x in plan.query_vector)):
            raise QualityUnavailable('quality_profile_scope_mismatch')
        budget=profile.budget
        if budget.nprobe_floor>nlist or budget.max_candidates>row_count:
            raise QualityUnavailable('quality_profile_index_configuration_mismatch')
        required={'nprobe':budget.nprobe_floor,'candidates':budget.initial_candidates,
                  'max_candidates':budget.max_candidates,'max_rounds':budget.max_rounds}
        for name,floor in required.items():
            explicit=getattr(options,name)
            if explicit is not None and explicit<floor:raise ValueError(f'Quality profile {name} budget conflict')
        effective=replace(options,**{name:getattr(options,name) if getattr(options,name) is not None else floor for name,floor in required.items()}).validate()
        if effective.nprobe>nlist:raise ValueError('Quality profile nprobe exceeds nlist')
        return effective,dict(profile.describe(),state='applied',requested_id=profile_id)
