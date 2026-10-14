"""Immutable per-request search controls; absence preserves exact behavior."""
from dataclasses import asdict, dataclass


@dataclass(frozen=True)
class SearchOptions:
    mode: str = 'exact'
    path: str = 'filter-first'
    nprobe: int | None = None
    candidates: int | None = None
    max_candidates: int | None = None
    max_rounds: int = 6
    fallback: str = 'exact'

    def validate(self):
        if self.mode not in ('exact', 'approx'):
            raise ValueError('mode must be exact or approx')
        if self.path not in ('filter-first', 'search-first'):
            raise ValueError('path must be filter-first or search-first')
        if self.mode == 'exact' and self.path != 'filter-first':
            raise ValueError('exact mode requires filter-first')
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
        options = {name: getattr(value, name) for name in ('mode', 'path', 'fallback') if getattr(value, name)}
        options.update({name: getattr(value, name) for name in ('nprobe', 'candidates', 'max_candidates', 'max_rounds') if value.HasField(name)})
        return cls(**options).validate()

    def to_dict(self):
        self.validate()
        return asdict(self)

    def to_proto(self):
        from vecstor.rpc.sql_pb2 import SearchOptions as Message
        return Message(**{key: value for key, value in self.to_dict().items() if value is not None})
