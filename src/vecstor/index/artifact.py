"""Immutable CPU artifacts and a batch-independent digest of IVF inputs."""
from dataclasses import asdict, dataclass, field, replace
import hashlib
import json
import platform
import re
import struct
import sys

import numpy as np

from .types import BuildConfig, IndexKey

MAX_ARTIFACT_BYTES = 256 * 1024**2
MAX_DESCRIPTOR_BYTES = 64 * 1024
CHUNK_BYTES = 1024**2


class ArtifactError(ValueError):
    def __init__(self, code, reason):
        self.code, self.reason = code, reason
        super().__init__(reason)


def require(condition, reason, code='INVALID_ARGUMENT'):
    if not condition:
        raise ArtifactError(code, reason)


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=False,
                      allow_nan=False).encode('utf-8')


def digest(value):
    return hashlib.sha256(canonical(value)).hexdigest()


def _integer(value, minimum, maximum, name):
    require(type(value) is int and minimum <= value <= maximum, 'invalid_' + name)


def _hash(value, name):
    require(isinstance(value, str) and re.fullmatch('[0-9a-f]{64}', value) is not None,
            'invalid_' + name)


class SnapshotHasher:
    def __init__(self, row_count, dimension):
        _integer(row_count, 0, 2**63-1, 'row_count')
        _integer(dimension, 1, CHUNK_BYTES//4, 'dimension')
        self.row_count, self.dimension = row_count, dimension
        self.count, self.previous = 0, -1
        self._digest = hashlib.sha256(b'vecstor-index-snapshot-v1' + struct.pack('<QI', row_count, dimension))

    def add(self, ids, vectors):
        ids, vectors = np.asarray(ids), np.asarray(vectors)
        require(ids.ndim == 1 and ids.dtype == np.dtype('int64'), 'invalid_snapshot_ids')
        require(vectors.dtype == np.dtype('float32') and vectors.shape == (len(ids), self.dimension)
                and np.isfinite(vectors).all(), 'invalid_snapshot_vectors')
        require(self.count+len(ids) <= self.row_count, 'snapshot_count_overflow')
        if len(ids):
            require(int(ids[0]) > self.previous and np.all(ids[1:] > ids[:-1]), 'snapshot_id_order')
            # Packed records preserve exact int64 labels and float32 bit patterns.
            records = np.empty(len(ids), dtype=[('id', '<i8'), ('vector', '<f4', (self.dimension,))])
            records['id'], records['vector'] = ids, vectors
            self._digest.update(records.tobytes())
            self.previous = int(ids[-1])
        self.count += len(ids)

    def finish(self):
        require(self.count == self.row_count, 'incomplete_snapshot', 'DATA_LOSS')
        return self._digest.hexdigest()


@dataclass(frozen=True)
class ArtifactDescriptor:
    key: IndexKey
    build_config: BuildConfig
    faiss_version: str
    row_count: int
    nlist: int
    payload_size: int
    payload_sha256: str
    snapshot_sha256: str
    artifact_id: str = ''
    format_version: int = 1
    payload_format: str = 'faiss_cpu_ivfflat_v1'
    platform: str = 'linux-x86_64-le'

    def to_dict(self):
        return asdict(self)

    def identity(self):
        value = self.to_dict()
        del value['artifact_id']
        return digest(value)

    @classmethod
    def from_dict(cls, value):
        import faiss
        require(isinstance(value, dict) and set(value) == set(cls.__dataclass_fields__), 'invalid_descriptor_fields')
        try:
            require(len(canonical(value)) <= MAX_DESCRIPTOR_BYTES, 'descriptor_too_large')
            require(type(value['format_version']) is int and value['format_version'] == 1
                    and value['payload_format'] == 'faiss_cpu_ivfflat_v1', 'unsupported_artifact_format')
            require(value['faiss_version'] == faiss.__version__, 'artifact_faiss_version', 'FAILED_PRECONDITION')
            require(value['platform'] == 'linux-x86_64-le' and sys.platform == 'linux'
                    and platform.machine() == 'x86_64' and sys.byteorder == 'little',
                    'artifact_platform', 'FAILED_PRECONDITION')
            raw = value['key']
            require(isinstance(raw, dict) and set(raw) == set(IndexKey.__dataclass_fields__), 'invalid_artifact_key')
            for name in ('database_id', 'table_id', 'schema_version'):
                require(isinstance(raw[name], str) and 0 < len(raw[name]) <= 256 and '\x00' not in raw[name], 'invalid_' + name)
            _integer(raw['revision'], 0, 2**63-1, 'revision')
            _integer(raw['dimension'], 1, CHUNK_BYTES//4, 'dimension')
            require(raw['metric'] == 'squared_l2', 'artifact_metric')
            _hash(raw['build_fingerprint'], 'build_fingerprint')
            _integer(value['row_count'], 1, 2**63-1, 'row_count')
            _integer(value['nlist'], 1, value['row_count'], 'nlist')
            _integer(value['payload_size'], 1, MAX_ARTIFACT_BYTES, 'payload_size')
            require(value['row_count'] * (raw['dimension']*4+8) <= value['payload_size'], 'artifact_size_bound')
            config_value = value['build_config']
            require(isinstance(config_value, dict) and set(config_value) == set(BuildConfig.__dataclass_fields__), 'invalid_build_config')
            config = BuildConfig(**config_value).validate(value['row_count'])
            require(config.nlist == value['nlist'] and config.nlist is not None, 'artifact_nlist')
            require(config.fingerprint() == raw['build_fingerprint'], 'artifact_build_fingerprint')
            for name in ('artifact_id', 'payload_sha256', 'snapshot_sha256'): _hash(value[name], name)
            result = cls(**dict(value, key=IndexKey(**raw), build_config=config))
            require(result.identity() == result.artifact_id, 'artifact_identity_checksum', 'DATA_LOSS')
            return result
        except ArtifactError:
            raise
        except (TypeError, ValueError, OverflowError, UnicodeError) as error:
            raise ArtifactError('INVALID_ARGUMENT', 'invalid_descriptor: ' + str(error)) from error


def _content_digest(handle):
    import faiss
    index = handle._index
    centroids = index.quantizer.reconstruct_n(0, index.nlist)
    require(np.isfinite(centroids).all(), 'nonfinite_quantizer', 'DATA_LOSS')
    ids = np.empty(handle.row_count, dtype=np.int64)
    vectors = np.empty((handle.row_count, handle.dimension), dtype=np.float32)
    offset = 0
    for bucket in range(index.nlist):
        count = index.invlists.list_size(bucket)
        if not count: continue
        labels = index.invlists.get_ids(bucket)
        codes = index.invlists.get_codes(bucket)
        try:
            ids[offset:offset+count] = faiss.rev_swig_ptr(labels, count)
            vectors[offset:offset+count] = faiss.rev_swig_ptr(codes, count*index.code_size).view(np.float32).reshape(count, handle.dimension)
        finally:
            index.invlists.release_ids(bucket, labels)
            index.invlists.release_codes(bucket, codes)
        offset += count
    order = np.argsort(ids)
    hasher = SnapshotHasher(handle.row_count, handle.dimension)
    for start in range(0, len(order), 1024):
        selected = order[start:start+1024]
        hasher.add(ids[selected], vectors[selected])
    return hasher.finish()


def encode_artifact(handle, key, config, snapshot_sha256):
    import faiss
    require(handle.dimension == key.dimension and handle.nlist == config.nlist
            and key.build_fingerprint == config.fingerprint(), 'artifact_configuration')
    require(_content_digest(handle) == snapshot_sha256, 'artifact_snapshot_mismatch', 'DATA_LOSS')
    payload = faiss.serialize_index(handle._index).tobytes()
    descriptor = ArtifactDescriptor(key, config, faiss.__version__, handle.row_count, handle.nlist,
                                    len(payload), hashlib.sha256(payload).hexdigest(), snapshot_sha256)
    descriptor = replace(descriptor, artifact_id=descriptor.identity())
    return ArtifactDescriptor.from_dict(descriptor.to_dict()), payload


def validate_artifact(payload, descriptor):
    import faiss
    from .faiss_backend import FaissIndex
    descriptor = ArtifactDescriptor.from_dict(descriptor.to_dict())
    require(len(payload) == descriptor.payload_size, 'artifact_payload_size', 'DATA_LOSS')
    require(hashlib.sha256(payload).hexdigest() == descriptor.payload_sha256, 'artifact_payload_checksum', 'DATA_LOSS')
    try:
        index = faiss.deserialize_index(np.frombuffer(payload, dtype=np.uint8))
        handle = FaissIndex(index, 0)
        require((handle.row_count, handle.dimension, handle.nlist) ==
                (descriptor.row_count, descriptor.key.dimension, descriptor.nlist), 'artifact_structure_mismatch', 'DATA_LOSS')
        require(_content_digest(handle) == descriptor.snapshot_sha256, 'artifact_snapshot_mismatch', 'DATA_LOSS')
        return handle
    except ArtifactError:
        raise
    except (ValueError, RuntimeError, TypeError) as error:
        raise ArtifactError('DATA_LOSS', 'invalid_native_artifact: ' + str(error)) from error


@dataclass(frozen=True)
class PublicationRecord:
    operation_id: str
    operation_kind: str
    request_intent: dict
    intent_digest: str
    request_digest: str
    key: IndexKey | None
    expected_generation: int
    state: str
    descriptor: ArtifactDescriptor | None = None
    generation: int = 0
    error: dict | None = None
    result: dict | None = None


@dataclass(frozen=True)
class PublishedLease:
    database_id: str
    table_id: str
    revision: int
    schema_version: str
    dimension: int
    table: str
    token: str
    row_count: int
    publication_version: int = 1
    generation: int = 0
    descriptor: ArtifactDescriptor | None = None
    state: str = 'missing'


@dataclass(frozen=True)
class PruneResult:
    candidates: tuple = ()
    protected: tuple = ()
    deleted: tuple = ()
    deleted_bytes: int = 0


@dataclass(frozen=True)
class ArtifactInspection:
    descriptor: ArtifactDescriptor | None
    generation: int
    cpu_ready: bool
    gpu_ready: bool
    state: str
