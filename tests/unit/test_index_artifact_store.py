import time

import pytest

from vecstor.index.artifact import ArtifactError, CHUNK_BYTES
from vecstor.storage.index_artifacts import ArtifactStore


def stage(store, artifact, operation=('test-database', 'upload')):
    descriptor, payload = artifact
    store.begin_upload(operation, descriptor)
    for offset in range(0, len(payload), 4096): store.append(operation, offset, payload[offset:offset+4096])
    store.seal(operation)
    return operation


def install(store, artifact, operation=('test-database', 'upload')):
    stage(store, artifact, operation)
    return store.validate_and_install(operation, artifact[0].snapshot_sha256, time.monotonic()+10, lambda:False)


def test_install_and_recovery_preserve_bytes(tmp_path, cpu_artifact):
    store = ArtifactStore(tmp_path)
    descriptor = install(store, cpu_artifact)
    assert store.reserved_bytes == 0
    assert store.prepared(descriptor)
    with store.pin(descriptor) as source: assert source.read() == cpu_artifact[1]
    restored = ArtifactStore(tmp_path)
    report = restored.recover([descriptor])
    assert not report['corrupt']
    assert restored.prepared(descriptor)


def test_reject_symlink_and_path_escape(tmp_path, cpu_artifact):
    real = tmp_path/'real'; real.mkdir()
    link = tmp_path/'link'; link.symlink_to(real)
    with pytest.raises(ArtifactError): ArtifactStore(link)
    store = ArtifactStore(real)
    with pytest.raises(ArtifactError): store.begin_upload(('test-database', '../escape'), cpu_artifact[0])
    assert not (tmp_path/'escape').exists()


def test_chunk_limit_and_offset(tmp_path, cpu_artifact):
    store = ArtifactStore(tmp_path)
    op = ('test-database', 'upload')
    store.begin_upload(op, cpu_artifact[0])
    with pytest.raises(ArtifactError): store.append(op, 1, b'abc')
    with pytest.raises(ArtifactError): store.append(op, 0, b'x'*(CHUNK_BYTES+1))
    store.abort_upload(op)
    assert store.reserved_bytes == 0


def test_quota_reserved_before_upload(tmp_path, cpu_artifact):
    size = cpu_artifact[0].payload_size
    store = ArtifactStore(tmp_path, max_total_bytes=2*size-1)
    store.begin_upload(('test-database', 'a'), cpu_artifact[0])
    with pytest.raises(ArtifactError, match='budget'): store.begin_upload(('test-database', 'b'), cpu_artifact[0])
    assert size <= store.reserved_bytes < 2*size
    store.abort_upload(('test-database', 'a'))
    assert store.reserved_bytes == 0


def test_fsync_failure_never_marks_prepared(tmp_path, cpu_artifact, monkeypatch):
    import os
    store = ArtifactStore(tmp_path)
    op = stage(store, cpu_artifact)
    def fail(fd): raise OSError('injected disk failure')
    monkeypatch.setattr(os, 'fsync', fail)
    with pytest.raises((OSError, ArtifactError)):
        store.validate_and_install(op, cpu_artifact[0].snapshot_sha256, time.monotonic()+10, lambda:False)
    assert not store.prepared(cpu_artifact[0])
    store.abort_upload(op)
    assert store.reserved_bytes == 0


def test_existing_object_never_overwritten(tmp_path, cpu_artifact):
    store = ArtifactStore(tmp_path)
    descriptor = install(store, cpu_artifact)
    blob = next(tmp_path.glob('objects/*/*/index.faiss'))
    before = blob.stat().st_ino
    install(store, cpu_artifact, ('test-database', 'again'))
    assert blob.stat().st_ino == before
    assert blob.read_bytes() == cpu_artifact[1]
    assert store.stored_bytes >= descriptor.payload_size


def test_incomplete_or_bad_checksum_is_not_sealed(tmp_path, cpu_artifact):
    store = ArtifactStore(tmp_path)
    descriptor, payload = cpu_artifact
    op = ('test-database', 'upload')
    store.begin_upload(op, descriptor)
    store.append(op, 0, b'x'*len(payload))
    with pytest.raises(ArtifactError, match='checksum'): store.seal(op)
    store.abort_upload(op)


def test_recovery_reports_corrupt_without_publishing_orphans(tmp_path, cpu_artifact):
    store = ArtifactStore(tmp_path)
    descriptor = install(store, cpu_artifact)
    next(tmp_path.glob('objects/*/*/index.faiss')).write_bytes(b'broken')
    recovered = ArtifactStore(tmp_path)
    assert recovered.recover([descriptor])['corrupt'] == [descriptor.artifact_id]
    assert not recovered.prepared(descriptor)


def test_download_pin_prevents_delete(tmp_path, cpu_artifact):
    store = ArtifactStore(tmp_path)
    descriptor = install(store, cpu_artifact)
    with store.pin(descriptor):
        with pytest.raises(ArtifactError, match='pinned'): store.delete_retired(descriptor)
    assert store.delete_retired(descriptor) >= descriptor.payload_size
    assert not store.prepared(descriptor)


def test_parent_fsync_failure_keeps_orphan_in_disk_budget(tmp_path, cpu_artifact, monkeypatch):
    from vecstor.storage import index_artifacts
    store = ArtifactStore(tmp_path)
    op = stage(store, cpu_artifact)
    original = index_artifacts.fsync_directory
    parent = tmp_path/'objects'/'test-database'
    def fail_after_install(path):
        if path == parent and list(parent.glob('*/index.faiss')): raise OSError('parent fsync failed')
        original(path)
    monkeypatch.setattr(index_artifacts, 'fsync_directory', fail_after_install)
    with pytest.raises(OSError):
        store.validate_and_install(op, cpu_artifact[0].snapshot_sha256, time.monotonic()+10, lambda:False)
    store.abort_upload(op)
    assert not store.prepared(cpu_artifact[0])
    assert store.stored_bytes >= cpu_artifact[0].payload_size


def test_rename_failure_releases_staging(tmp_path, cpu_artifact, monkeypatch):
    import os
    store = ArtifactStore(tmp_path)
    op = stage(store, cpu_artifact)
    def fail(*args): raise OSError('rename failed')
    monkeypatch.setattr(os, 'rename', fail)
    with pytest.raises(OSError):
        store.validate_and_install(op, cpu_artifact[0].snapshot_sha256, time.monotonic()+10, lambda:False)
    store.abort_upload(op)
    assert store.reserved_bytes == 0 and not store.prepared(cpu_artifact[0])
