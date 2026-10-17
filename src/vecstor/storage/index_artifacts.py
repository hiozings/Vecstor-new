"""Storage-owned immutable files, upload reservations, and transfer pins."""
from contextlib import contextmanager
from dataclasses import dataclass, field
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
from threading import Lock, RLock, BoundedSemaphore
import time
import uuid

from vecstor.index.artifact import ArtifactDescriptor, ArtifactError, canonical, require, CHUNK_BYTES, MAX_ARTIFACT_BYTES
from vecstor.index.artifact_worker import verify_in_worker


def fsync_directory(path):
    fd = os.open(path, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try: os.fsync(fd)
    finally: os.close(fd)


@dataclass
class Upload:
    descriptor: ArtifactDescriptor
    path: Path
    reserved: int
    offset: int = 0
    hasher: object = field(default_factory=hashlib.sha256)
    sealed: bool = False
    alive: bool = True
    lock: object = field(default_factory=Lock)


class ArtifactStore:
    def __init__(self, root, max_artifact_bytes=MAX_ARTIFACT_BYTES, max_total_bytes=2*1024**3, min_free_bytes=256*1024**2):
        root = Path(root)
        require(not root.is_symlink(), 'artifact_root_symlink')
        for value in (max_artifact_bytes, max_total_bytes): require(type(value) is int and value > 0, 'invalid_artifact_budget')
        require(type(min_free_bytes) is int and min_free_bytes >= 0, 'invalid_disk_margin')
        root.mkdir(parents=True, exist_ok=True)
        self.root = root.resolve()
        self.max_artifact_bytes, self.max_total_bytes, self.min_free_bytes = max_artifact_bytes, max_total_bytes, min_free_bytes
        self.lock = RLock()
        self._verifier = BoundedSemaphore(1)
        self._uploads, self._ready, self._pins, self._deleting = {}, {}, {}, set()
        self._reserved, self._stored = 0, 0
        self.epoch = uuid.uuid4().hex
        self._directory('objects')
        self._directory('staging', self.epoch)
        for path in (self.root/'objects').rglob('*'):
            require(not path.is_symlink(), 'artifact_symlink', 'DATA_LOSS')
            if path.is_file(): self._stored += path.stat().st_size

    def _directory(self, *parts):
        path = self.root
        for part in parts:
            require(isinstance(part, str) and re.fullmatch('[A-Za-z0-9_-]{1,128}', part), 'invalid_artifact_path')
            path = path/part
            require(not path.is_symlink(), 'artifact_directory_symlink', 'DATA_LOSS')
            if not path.exists():
                path.mkdir()
                fsync_directory(path.parent)
            require(path.is_dir(), 'invalid_artifact_directory', 'DATA_LOSS')
        return path

    def _object(self, descriptor):
        database = descriptor.key.database_id
        require(re.fullmatch('[A-Za-z0-9_-]{1,128}', database) is not None, 'invalid_artifact_database_id')
        path = self.root/'objects'/database/descriptor.artifact_id
        for component in (path.parent, path): require(not component.is_symlink(), 'artifact_directory_symlink', 'DATA_LOSS')
        return path

    @property
    def reserved_bytes(self):
        with self.lock: return self._reserved

    @property
    def stored_bytes(self):
        with self.lock: return self._stored

    @property
    def transfer_pins(self):
        with self.lock: return dict(self._pins)

    def begin_upload(self, operation, descriptor):
        descriptor = ArtifactDescriptor.from_dict(descriptor.to_dict())
        require(len(operation) == 2 and operation[0] == descriptor.key.database_id, 'artifact_upload_identity')
        require(all(isinstance(x, str) and re.fullmatch('[A-Za-z0-9_-]{1,128}', x) for x in operation), 'invalid_operation_path')
        require(descriptor.payload_size <= self.max_artifact_bytes, 'artifact_size_budget', 'RESOURCE_EXHAUSTED')
        reserved = descriptor.payload_size + len(canonical(descriptor.to_dict()))
        with self.lock:
            require(operation not in self._uploads, 'artifact_upload_busy', 'RESOURCE_EXHAUSTED')
            require(self._stored+self._reserved+reserved <= self.max_total_bytes, 'artifact_store_budget', 'RESOURCE_EXHAUSTED')
            require(shutil.disk_usage(self.root).free-self._reserved-reserved >= self.min_free_bytes, 'artifact_disk_margin', 'RESOURCE_EXHAUSTED')
            path = self._directory('staging', self.epoch, operation[0])/(operation[1]+'.part')
            fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
            os.close(fd)
            self._uploads[operation] = Upload(descriptor, path, reserved)
            self._reserved += reserved

    def _upload(self, operation):
        with self.lock:
            result = self._uploads.get(operation)
            require(result is not None, 'artifact_upload_missing', 'FAILED_PRECONDITION')
            return result

    def append(self, operation, offset, chunk):
        require(type(offset) is int and isinstance(chunk, bytes) and 0 < len(chunk) <= CHUNK_BYTES, 'invalid_artifact_chunk')
        upload = self._upload(operation)
        with upload.lock:
            require(upload.alive and not upload.sealed, 'artifact_upload_closed', 'FAILED_PRECONDITION')
            require(offset == upload.offset and offset+len(chunk) <= upload.descriptor.payload_size, 'artifact_chunk_offset', 'DATA_LOSS')
            fd = os.open(upload.path, os.O_WRONLY | os.O_APPEND | os.O_NOFOLLOW)
            with os.fdopen(fd, 'wb') as target: target.write(chunk)
            upload.hasher.update(chunk)
            upload.offset += len(chunk)

    def seal(self, operation):
        upload = self._upload(operation)
        with upload.lock:
            require(upload.alive and not upload.sealed, 'artifact_upload_closed', 'FAILED_PRECONDITION')
            require(upload.offset == upload.descriptor.payload_size, 'incomplete_artifact_upload', 'DATA_LOSS')
            require(upload.hasher.hexdigest() == upload.descriptor.payload_sha256, 'artifact_upload_checksum', 'DATA_LOSS')
            upload.sealed = True

    def validate_and_install(self, operation, snapshot_digest, deadline, cancelled):
        upload = self._upload(operation)
        require(self._verifier.acquire(blocking=False), 'artifact_verifier_busy', 'RESOURCE_EXHAUSTED')
        try:
            with upload.lock:
                require(upload.alive and upload.sealed, 'artifact_not_sealed', 'FAILED_PRECONDITION')
                descriptor = upload.descriptor
                require(descriptor.snapshot_sha256 == snapshot_digest, 'publication_snapshot_receipt_mismatch', 'DATA_LOSS')
                verify_in_worker(upload.path, descriptor, deadline, cancelled)
                if cancelled(): raise InterruptedError('artifact installation cancelled')
                if time.monotonic() >= deadline: raise TimeoutError('artifact installation deadline exceeded')
                parent = self._directory('objects', descriptor.key.database_id)
                destination = self._object(descriptor)
                if destination.exists():
                    self._check_files(descriptor)
                else:
                    temporary = upload.path.with_suffix('.object')
                    temporary.mkdir()
                    try:
                        with upload.path.open('rb') as source: os.fsync(source.fileno())
                        os.rename(upload.path, temporary/'index.faiss')
                        with (temporary/'manifest.json').open('xb') as target:
                            target.write(canonical(descriptor.to_dict()))
                            target.flush(); os.fsync(target.fileno())
                        fsync_directory(temporary)
                        os.rename(temporary, destination)
                        with self.lock: self._stored += upload.reserved
                        fsync_directory(parent)
                    except BaseException:
                        if temporary.exists(): shutil.rmtree(temporary)
                        raise
                with self.lock: self._ready[descriptor.artifact_id] = descriptor
            self.abort_upload(operation)
            return descriptor
        finally:
            self._verifier.release()

    def abort_upload(self, operation):
        with self.lock: upload = self._uploads.get(operation)
        if upload is None: return
        with upload.lock:
            if not upload.alive: return
            upload.alive = False
            upload.path.unlink(missing_ok=True)
            with self.lock:
                self._uploads.pop(operation, None)
                self._reserved -= upload.reserved

    def prepared(self, descriptor):
        with self.lock:
            return self._ready.get(descriptor.artifact_id) == descriptor and descriptor.artifact_id not in self._deleting

    def _check_files(self, descriptor):
        path = self._object(descriptor)
        try:
            require(not (path/'manifest.json').is_symlink() and not (path/'index.faiss').is_symlink(), 'artifact_file_symlink', 'DATA_LOSS')
            with (path/'manifest.json').open('rb') as manifest:
                content = manifest.read(64*1024+1)
            require(len(content) <= 64*1024 and json.loads(content) == descriptor.to_dict(), 'artifact_manifest_mismatch', 'DATA_LOSS')
            with (path/'index.faiss').open('rb') as source:
                require(os.fstat(source.fileno()).st_size == descriptor.payload_size, 'artifact_payload_size', 'DATA_LOSS')
                checksum = hashlib.file_digest(source, 'sha256').hexdigest()
            require(checksum == descriptor.payload_sha256, 'artifact_payload_checksum', 'DATA_LOSS')
        except (OSError, ValueError) as error:
            if isinstance(error, ArtifactError): raise
            raise ArtifactError('DATA_LOSS', 'artifact_files_missing_or_invalid: '+str(error)) from error

    @contextmanager
    def pin(self, descriptor):
        with self.lock:
            require(self.prepared(descriptor), 'artifact_not_prepared', 'DATA_LOSS')
            self._pins[descriptor.artifact_id] = self._pins.get(descriptor.artifact_id, 0)+1
        try:
            path = self._object(descriptor)/'index.faiss'
            fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
            with os.fdopen(fd, 'rb') as source: yield source
        finally:
            with self.lock:
                self._pins[descriptor.artifact_id] -= 1
                if not self._pins[descriptor.artifact_id]: del self._pins[descriptor.artifact_id]

    def recover(self, committed):
        require(not self._uploads, 'artifact_recovery_while_active', 'FAILED_PRECONDITION')
        report = dict(corrupt=[], orphans=[], staging=[])
        self._ready.clear()
        known = {descriptor.artifact_id for descriptor in committed}
        for descriptor in committed:
            try:
                self._check_files(descriptor)
                self._ready[descriptor.artifact_id] = descriptor
            except ArtifactError: report['corrupt'].append(descriptor.artifact_id)
        for path in (self.root/'objects').glob('*/*'):
            require(path.is_dir() and not path.is_symlink(), 'invalid_object_directory', 'DATA_LOSS')
            if path.name not in known:
                report['orphans'].append(path.name)
                shutil.rmtree(path)
                fsync_directory(path.parent)
        for path in (self.root/'staging').iterdir():
            require(not path.is_symlink(), 'invalid_staging_directory', 'DATA_LOSS')
            if path.name != self.epoch:
                report['staging'].append(path.name)
                shutil.rmtree(path)
        self._stored = sum(path.stat().st_size for path in (self.root/'objects').rglob('*') if path.is_file())
        return report

    def delete_retired(self, descriptor):
        with self.lock:
            require(not self._pins.get(descriptor.artifact_id), 'artifact_pinned', 'RESOURCE_EXHAUSTED')
            self._deleting.add(descriptor.artifact_id)
        try:
            path = self._object(descriptor)
            size = sum(p.stat().st_size for p in path.iterdir()) if path.exists() else 0
            if path.exists():
                shutil.rmtree(path); fsync_directory(path.parent)
            with self.lock:
                self._ready.pop(descriptor.artifact_id, None)
                self._stored -= size
            return size
        finally:
            with self.lock: self._deleting.discard(descriptor.artifact_id)
