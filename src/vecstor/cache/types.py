"""Immutable data identity and explicit logical cache budgets."""
from dataclasses import dataclass


@dataclass(frozen=True)
class DataScope:
    storage_epoch: str
    database_id: str
    table_id: str
    revision: int
    schema_version: str
    dimension: int
    dtype: str = 'float32-le'

    def __post_init__(self):
        for name in ('storage_epoch','database_id','table_id','schema_version'):
            value=getattr(self,name)
            if not isinstance(value,str) or not value or len(value.encode())>128 or '\x00' in value:
                raise ValueError('Invalid data cache identity')
        if type(self.revision) is not int or not 0<=self.revision<2**64:
            raise ValueError('Invalid data cache revision')
        if type(self.dimension) is not int or not 0<self.dimension<=1024**2//4 or self.dtype!='float32-le':
            raise ValueError('Invalid cached vector format')


@dataclass(frozen=True)
class CacheConfig:
    max_bytes: int = 0
    max_entries: int = 65536
    workspace_bytes: int = 16*1024**2
    workspace_slots: int = 4
    scan_admission: bool = False

    def __post_init__(self):
        if type(self.max_bytes) is not int or self.max_bytes<0:
            raise ValueError('Cache capacity must be a nonnegative integer')
        for name in ('max_entries','workspace_slots'):
            if type(getattr(self,name)) is not int or getattr(self,name)<=0:
                raise ValueError('Cache entry and workspace slot limits must be positive integers')
        if type(self.workspace_bytes) is not int or self.workspace_bytes<4*1024**2:
            raise ValueError('Cache workspace must allow at least one 4 MiB batch')
        if type(self.scan_admission) is not bool:
            raise ValueError('Cache scan admission must be boolean')
