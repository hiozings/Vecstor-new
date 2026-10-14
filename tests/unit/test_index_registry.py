from dataclasses import replace
from types import SimpleNamespace

import pytest


def setup():
    from vecstor.index.registry import IndexRegistry
    from vecstor.index.types import IndexKey
    lease = SimpleNamespace(database_id='db', table_id='table-id', revision=1, schema_version=1, dimension=2)
    key = IndexKey('db', 'table-id', 1, 1, 2, build_fingerprint='a')
    handle = SimpleNamespace(byte_size=100, row_count=80, dimension=2, nlist=2, max_abs=1)
    return IndexRegistry, lease, key, handle


def test_revision_identity_and_pinned_clear():
    Registry, lease, key, handle = setup()
    registry = Registry(max_bytes=200, max_entries=2)
    registry.publish(key, handle, 'items')
    with registry.acquire(lease) as entry:
        assert entry.key == key
        assert registry.clear('db') == {'removed': 0, 'busy': 1}
        lease.revision = 2
        assert registry.has_stale(lease)
        with registry.acquire(lease) as missing: assert missing is None
        registry.publish(replace(key, revision=2), handle, 'items')
        assert entry.key.revision == 1
    assert registry.clear('db', 'items') == {'removed': 2, 'busy': 0}
    assert registry.total_bytes == 0


def test_active_config_and_budget_do_not_evict_pins():
    Registry, lease, key, handle = setup()
    registry = Registry(max_bytes=100, max_entries=1)
    registry.publish(key, handle, 'items')
    newer = replace(key, build_fingerprint='b')
    with registry.acquire(lease):
        with pytest.raises(ValueError): registry.publish(newer, handle, 'items')
    registry.publish(newer, handle, 'items')
    with registry.acquire(lease) as current: assert current.key == newer
    assert registry.total_bytes == 100
    assert len(registry.list('db', 'items')) == 1
    assert registry.list('other') == []
    lease.table_id = 'new-table-id'
    assert not registry.has_stale(lease)
    with registry.acquire(lease) as missing: assert missing is None


def test_budget_reclaims_superseded_config_before_unrelated_active_index():
    Registry, lease, key, handle = setup()
    registry = Registry(max_bytes=300, max_entries=3)
    unrelated = replace(key, table_id='other-table')
    registry.publish(unrelated, handle, 'other')
    registry.publish(key, handle, 'items')
    registry.publish(replace(key, build_fingerprint='b'), handle, 'items')
    registry.publish(replace(key, table_id='third-table'), handle, 'third')
    assert len(registry.list('db', 'other')) == 1
    assert [entry['build_fingerprint'] for entry in registry.list('db', 'items')] == ['b']


def test_lease_schema_uuid_preserved_in_snapshot_key():
    from vecstor.index.types import BuildConfig, IndexKey
    Registry, lease, key, handle = setup()
    lease.schema_version = 'schema-uuid'
    key = IndexKey.from_lease(lease, BuildConfig(nlist=2))
    registry = Registry()
    registry.publish(key, handle, 'items')
    with registry.acquire(lease) as entry:
        assert entry.key.schema_version == 'schema-uuid'
    lease.schema_version = 'new-schema-uuid'
    with registry.acquire(lease) as entry:
        assert entry is None


def test_prepare_build_reclaims_realistic_obsolete_revision_budget():
    Registry, lease, key, handle = setup()
    registry = Registry()
    handle.byte_size = 200000 * (128 * 4 + 8)
    registry.publish(key, handle, 'items')
    estimate = 200000 * (128 * 4 + 8) * 4 + 100000 * 128 * 4
    assert estimate < 2 * registry.max_bytes < estimate + handle.byte_size
    registry.prepare_build(replace(key, revision=2), estimate)
    assert registry.list() == []


def test_prepare_build_failed_budget_keeps_all_entries():
    Registry, lease, key, handle = setup()
    registry = Registry(max_bytes=300)
    registry.publish(key, handle, 'items')
    unrelated = replace(key, table_id='other')
    registry.publish(unrelated, handle, 'other')
    with pytest.raises(ValueError):
        registry.prepare_build(replace(key, revision=2), 550)
    assert len(registry.list()) == 2
    with registry.acquire(lease) as entry:
        assert entry.key == key


def test_cancel_before_eviction_preserves_registry():
    Registry, lease, key, handle = setup()
    registry = Registry(max_bytes=100)
    registry.publish(key, handle, 'items')
    calls = 0
    def check():
        nonlocal calls
        calls += 1
        if calls == 2:
            raise InterruptedError('cancelled')
    with pytest.raises(InterruptedError):
        registry.prepare_build(replace(key, revision=2), 150, check=check)
    with registry.acquire(lease) as entry:
        assert entry.key == key


def test_default_nlist_respects_explicit_training_sample_size():
    from vecstor.index.types import BuildConfig
    assert BuildConfig(train_rows=1).resolved(1024).nlist == 1
