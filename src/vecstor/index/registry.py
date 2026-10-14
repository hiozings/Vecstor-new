"""Bounded in-memory snapshot registry with pinned reader lifetimes."""
from collections import OrderedDict
from contextlib import contextmanager
from dataclasses import asdict, dataclass, field
from threading import RLock

from .types import IndexKey


@dataclass
class IndexEntry:
    key: IndexKey
    handle: object
    table: str
    statistics: dict = field(default_factory=dict)
    pins: int = 0

    def describe(self):
        return dict(asdict(self.key), table=self.table, nlist=self.handle.nlist,
                    row_count=self.handle.row_count, byte_size=self.handle.byte_size,
                    byte_size_kind='estimated_serialized', max_abs=self.handle.max_abs,
                    pins=self.pins, **self.statistics)


class IndexRegistry:
    def __init__(self, max_bytes=256*1024*1024, max_entries=8):
        if max_bytes <= 0 or max_entries <= 0:
            raise ValueError('registry budgets must be positive')
        self.max_bytes = max_bytes
        self.max_entries = max_entries
        self._entries = OrderedDict()
        self._active = {}
        self._lock = RLock()

    @property
    def total_bytes(self):
        with self._lock:
            return sum(entry.handle.byte_size for entry in self._entries.values())

    def _remove(self, key):
        del self._entries[key]
        if self._active.get(key.snapshot) == key:
            del self._active[key.snapshot]

    def reactivate(self, key, check=None):
        """Select a resident full key without rebuilding a nonactive config."""
        with self._lock:
            if check is not None:
                check()
            entry = self._entries.get(key)
            if entry is not None:
                self._active[key.snapshot] = key
            return entry

    def prepare_build(self, key, estimated_bytes, check=None):
        """Reclaim unpinned obsolete entries before allocating build buffers.

        Only commit eviction if it makes the full estimate fit. Current indexes
        for unrelated snapshots and readers holding any version are preserved.
        The manager serializes builds, so no competing build reserves this space.
        """
        with self._lock:
            if check is not None:
                check()
            total = self.total_bytes + estimated_bytes
            newest = {}
            for candidate in (*self._entries, key):
                identity = (candidate.database_id, candidate.table_id)
                newest[identity] = max(newest.get(identity, candidate.revision), candidate.revision)
            victims = []
            for old_key, entry in self._entries.items():
                if total <= 2 * self.max_bytes:
                    break
                obsolete = (self._active.get(old_key.snapshot) != old_key or
                            old_key.revision < newest[(old_key.database_id, old_key.table_id)])
                if obsolete and not entry.pins:
                    victims.append(old_key)
                    total -= entry.handle.byte_size
            if total > 2 * self.max_bytes:
                raise ValueError('Estimated build working set exceeds twice the index residency budget')
            if check is not None:
                check()
            for victim in victims:
                self._remove(victim)

    def publish(self, key, handle, table, statistics=None, check=None):
        with self._lock:
            if check is not None:
                check()
            if key in self._entries:
                self._active[key.snapshot] = key
                return self._entries[key]
            if handle.dimension != key.dimension:
                raise ValueError('index dimension does not match snapshot')
            size, count = self.total_bytes + handle.byte_size, len(self._entries)+1
            victims = []
            # Prefer superseded configurations/revisions. Remaining unpinned
            # current entries use publication FIFO when the budget still needs it.
            newest = {}
            for candidate in (*self._entries, key):
                identity = (candidate.database_id, candidate.table_id)
                newest[identity] = max(newest.get(identity, candidate.revision), candidate.revision)
            def is_current(item):
                candidate = item[0]
                return (self._active.get(candidate.snapshot) == candidate and
                        candidate.revision == newest[(candidate.database_id, candidate.table_id)] and
                        candidate.snapshot != key.snapshot)
            for old_key, old in sorted(self._entries.items(), key=is_current):
                if size <= self.max_bytes and count <= self.max_entries:
                    break
                if not old.pins:
                    victims.append(old_key)
                    size -= old.handle.byte_size
                    count -= 1
            if size > self.max_bytes or count > self.max_entries:
                raise ValueError('index registry budget exhausted; pinned entries cannot be evicted')
            if check is not None:
                check()
            for victim in victims:
                self._remove(victim)
            entry = IndexEntry(key, handle, table, dict(statistics or {}))
            self._entries[key] = entry
            self._active[key.snapshot] = key
            return entry

    @contextmanager
    def acquire(self, lease):
        snapshot = (lease.database_id, lease.table_id, lease.revision, lease.schema_version, lease.dimension, 'squared_l2')
        with self._lock:
            entry = self._entries.get(self._active.get(snapshot))
            if entry:
                entry.pins += 1
        try:
            yield entry
        finally:
            if entry:
                with self._lock:
                    entry.pins -= 1

    def list(self, database_id='', table=''):
        with self._lock:
            return [entry.describe() for entry in self._entries.values()
                    if (not database_id or entry.key.database_id == database_id) and (not table or entry.table == table)]

    def clear(self, database_id, table=''):
        removed = busy = 0
        with self._lock:
            for key, entry in list(self._entries.items()):
                if key.database_id != database_id or (table and table != entry.table):
                    continue
                if entry.pins:
                    busy += 1
                else:
                    self._remove(key)
                    removed += 1
        return dict(removed=removed, busy=busy)

    def has_stale(self, lease):
        with self._lock:
            return any(key.database_id == lease.database_id and key.table_id == lease.table_id and
                       key.revision != lease.revision for key in self._entries)
