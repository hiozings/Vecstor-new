"""Immutable snapshot identities and reproducible training settings."""
from dataclasses import asdict, dataclass, replace
import hashlib
import json
from math import isqrt


@dataclass(frozen=True)
class BuildConfig:
    nlist: int | None = None
    seed: int = 20261013
    train_rows: int = 100000

    def validate(self, row_count=None):
        """Validate explicit controls even when an empty/small table skips IVF."""
        for name, value in [('seed', self.seed), ('train_rows', self.train_rows)]:
            if isinstance(value, bool) or not isinstance(value, int) or value < (1 if name == 'train_rows' else 0):
                raise ValueError(f'invalid {name}')
        if self.seed > 2**31-1:
            raise ValueError('seed exceeds int32 range')
        if row_count is not None and (type(row_count) is not int or row_count < 0):
            raise ValueError('row count must be a nonnegative integer')
        if self.nlist is not None:
            upper = self.train_rows if row_count is None else min(row_count, self.train_rows)
            if type(self.nlist) is not int or not 1 <= self.nlist <= upper:
                raise ValueError('nlist exceeds training sample or row count')
        return self

    def resolved(self, row_count):
        self.validate(row_count)
        if row_count == 0:
            raise ValueError('index requires a nonempty table')
        nlist = self.nlist if self.nlist is not None else min(256, self.train_rows, isqrt(row_count), max(1, row_count//40))
        return replace(self, nlist=nlist)

    def fingerprint(self):
        import faiss
        data = dict(asdict(self), algorithm='IVFFlat', metric='squared_l2',
                    sampling='sorted-id-seeded-without-replacement', faiss_version=faiss.__version__)
        return hashlib.sha256(json.dumps(data, sort_keys=True).encode()).hexdigest()


@dataclass(frozen=True)
class IndexKey:
    database_id: str
    table_id: str
    revision: int
    schema_version: str
    dimension: int
    metric: str = 'squared_l2'
    build_fingerprint: str = ''

    @classmethod
    def from_lease(cls, lease, config):
        return cls(lease.database_id, lease.table_id, lease.revision,
                   lease.schema_version, lease.dimension, build_fingerprint=config.fingerprint())

    @property
    def snapshot(self):
        return (self.database_id, self.table_id, self.revision, self.schema_version, self.dimension, self.metric)
