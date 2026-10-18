from types import SimpleNamespace

import pytest

from vecstor.index.artifact import ArtifactError, validate_artifact
from vecstor.index.registry import IndexRegistry


def test_exact_acquisition_requires_artifact_identity(cpu_artifact):
    descriptor, payload = cpu_artifact
    registry = IndexRegistry()
    handle = validate_artifact(payload, descriptor)
    registry.publish_artifact(descriptor, handle, 'items', {}, lambda:None)
    with registry.acquire_exact(descriptor.key, '0'*64) as entry: assert entry is None
    with registry.acquire_exact(descriptor.key, descriptor.artifact_id) as entry:
        assert entry.handle is handle
        assert registry.clear(descriptor.key.database_id)['busy'] == 1
    assert registry.clear(descriptor.key.database_id)['removed'] == 1


def test_pinned_local_conflict_is_not_overwritten(cpu_artifact):
    descriptor, payload = cpu_artifact
    registry = IndexRegistry()
    handle = validate_artifact(payload, descriptor)
    registry.publish(descriptor.key, handle, 'items')
    with registry.acquire(SimpleNamespace(**descriptor.to_dict()['key'])) as old:
        with pytest.raises(ArtifactError, match='conflict'):
            registry.publish_artifact(descriptor, handle, 'items', {}, lambda:None)
        assert old.handle is handle and old.pins == 1


def test_budget_failure_does_not_activate_artifact(cpu_artifact):
    descriptor, payload = cpu_artifact
    registry = IndexRegistry(max_bytes=1)
    with pytest.raises(ValueError):
        registry.publish_artifact(descriptor, validate_artifact(payload, descriptor), 'items', {}, lambda:None)
    assert registry.list() == []
