"""Immutable per-request search controls; absence preserves exact behavior."""
from dataclasses import asdict, dataclass
import re


def validate_profile_id(value):
    if not isinstance(value,str) or not re.fullmatch(r"[A-Za-z][A-Za-z0-9_-]{0,63}",value) or value in {"off","reset"}:
        raise ValueError("Invalid quality profile ID")
    return value


@dataclass(frozen=True)
class SearchOptions:
    data_cache: str = 'default'
    metadata_projection: str = 'default'
    candidate_data_path: str = 'default'
    device: str = 'cpu'
    gpu_id: int = 0
    device_fallback: str = 'error'
    mode: str = 'exact'
    path: str = 'filter-first'
    nprobe: int | None = None
    candidates: int | None = None
    max_candidates: int | None = None
    max_rounds: int = 6
    fallback: str = 'exact'
    quality_profile_id: str | None = None

    def validate(self):
        if self.data_cache not in ('default','off','on'):raise ValueError('Invalid data_cache option')
        if self.metadata_projection not in ('default','full','projected'):raise ValueError('Invalid metadata_projection option')
        if self.candidate_data_path not in ('default','separate','fused'):raise ValueError('Invalid candidate_data_path')
        if self.candidate_data_path=='fused' and (self.mode!='approx' or self.path not in ('search-first','auto')):
            raise ValueError('Fused candidate reads require approx/search-first')
        if self.device not in ('cpu','gpu'):raise ValueError('device must be cpu or gpu')
        if type(self.gpu_id) is not int or self.gpu_id!=0:raise ValueError('Only gpu_id=0 is supported')
        if self.device_fallback not in ('error','cpu'):raise ValueError('device_fallback must be error or cpu')
        if self.device=='gpu' and self.mode!='approx':raise ValueError('GPU requires mode=approx')
        if self.mode not in ('exact', 'approx'):
            raise ValueError('mode must be exact or approx')
        if self.path not in ('filter-first', 'search-first', 'auto'):
            raise ValueError('path must be filter-first, search-first or auto')
        if self.device=='gpu' and self.path=='auto':raise ValueError('Adaptive planning supports CPU only')
        if self.mode == 'exact' and self.path not in ('filter-first','auto'):
            raise ValueError('exact mode requires filter-first')
        if self.quality_profile_id is not None:
            validate_profile_id(self.quality_profile_id)
            if self.mode!='approx':raise ValueError('Quality profile requires approximate mode')
        if self.fallback not in ('exact', 'error'):
            raise ValueError('fallback must be exact or error')
        for name in ('nprobe', 'candidates', 'max_candidates', 'max_rounds'):
            value = getattr(self, name)
            if value is None and name != 'max_rounds':
                continue
            upper = 32 if name == 'max_rounds' else 2**32 - 1
            if type(value) is not int or not 1 <= value <= upper:
                raise ValueError(f'{name} must be a positive integer at most {upper}')
        return self

    @classmethod
    def from_proto(cls, value):
        options = {name: getattr(value, name) for name in ('mode', 'path', 'fallback', 'quality_profile_id', 'device', 'device_fallback', 'candidate_data_path', 'data_cache', 'metadata_projection') if getattr(value, name)}
        options.update({name: getattr(value, name) for name in ('nprobe', 'candidates', 'max_candidates', 'max_rounds') if value.HasField(name)})
        if value.HasField('gpu_id'):options['gpu_id']=value.gpu_id
        return cls(**options).validate()

    def to_dict(self):
        self.validate()
        return asdict(self)

    def to_proto(self):
        from vecstor.rpc.sql_pb2 import SearchOptions as Message
        return Message(**{key: value for key, value in self.to_dict().items() if value is not None})
