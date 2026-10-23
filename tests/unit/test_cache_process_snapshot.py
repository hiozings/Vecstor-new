"""Query accounting must not scale with resident cache entries."""
from collections import OrderedDict
from dataclasses import replace
from types import SimpleNamespace

import numpy as np
import pytest

from vecstor.cache.types import CacheConfig, DataScope
from vecstor.cache.vector_cache import VersionedVectorCache, WORKSPACE_CHARGE
from vecstor.compute.data_reader import CachedLeaseReader


SCOPE = DataScope('epoch', 'db', 'table', 1, 'schema', 4)


def admit(cache, ids, scope=SCOPE):
    with cache.batch(scope, tuple(ids), lambda: None) as batch:
        vectors = np.zeros((len(batch.missing_ids), scope.dimension), dtype='<f4')
        batch.finish(tuple(ids), batch.missing_ids, vectors, admit=True, check=lambda: None)


class NoTraversal(OrderedDict):
    def __iter__(self):
        raise AssertionError('query statistics traversed cache')

    def items(self):
        raise AssertionError('query statistics traversed cache')

    def values(self):
        raise AssertionError('query statistics traversed cache')

    def keys(self):
        raise AssertionError('query statistics traversed cache')


@pytest.mark.parametrize('enabled', [False, True])
def test_query_description_never_traverses_cache(enabled):
    cache = VersionedVectorCache(CacheConfig(max_bytes=10000))
    admit(cache, [1, 2])
    expected = cache.stats('db')['process']
    cache._entries = NoTraversal(cache._entries)
    cache._scopes = NoTraversal(cache._scopes)
    io = SimpleNamespace(database_id='db', lease=SimpleNamespace(dimension=4),
                         trace={'counts': {}}, configure_cache_lease=lambda: SCOPE)
    reader = CachedLeaseReader(io, cache, ['id'], False, enabled)
    description = reader.describe()
    assert description['accounted_bytes'] == expected['accounted_bytes']
    assert description['entry_count'] == 2
    assert description['hit_rate'] is None
    assert cache.process_snapshot() == expected


def test_snapshot_tracks_lifecycle_and_is_detached():
    cache = VersionedVectorCache(CacheConfig(max_bytes=10000, max_entries=2))

    def check():
        snapshot = cache.process_snapshot()
        assert snapshot == cache.stats('db')['process']
        return snapshot

    empty = check()
    admit(cache, [1, 2])
    full = check()
    assert (full['entry_count'], full['scope_count'], full['accounted_bytes']) == (2, 1, 2080)
    admit(cache, [3], replace(SCOPE, database_id='other'))
    assert check()['scope_count'] == 2
    assert check()['payload_bytes'] == 32
    cache.clear('db')
    assert check()['entry_count'] == 1
    assert check()['accounted_bytes'] == 1552
    with cache.batch(SCOPE, (4,), lambda: None):
        assert check()['workspace_bytes'] == WORKSPACE_CHARGE
        cache.close()
        closed = check()
        assert closed['entry_count'] == closed['scope_count'] == closed['payload_bytes'] == closed['accounted_bytes'] == 0
        assert closed['workspace_bytes'] == WORKSPACE_CHARGE
    assert check()['workspace_bytes'] == 0
    assert check()['workspace_peak_bytes'] == WORKSPACE_CHARGE
    assert empty['entry_count'] == 0 and full['entry_count'] == 2
    closed['entry_count'] = 99
    assert check()['entry_count'] == 0
