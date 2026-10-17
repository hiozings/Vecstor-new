"""Published artifacts must bind actual bytes and vectors, not just IVF labels."""
from dataclasses import replace
import hashlib
import struct

import faiss
import numpy as np
import pytest

from vecstor.index.artifact import (
    ArtifactDescriptor, ArtifactError, SnapshotHasher, encode_artifact, validate_artifact,
)
from vecstor.index.faiss_backend import FaissIndex
from vecstor.index.types import BuildConfig, IndexKey


@pytest.fixture(scope='module')
def source():
    ids = np.arange(2048, dtype=np.int64) + 2**53 + 1
    vectors = np.random.default_rng(20261016).random((2048, 4), dtype=np.float32)
    vectors[0, 0] = -0.0
    config = BuildConfig(nlist=8).resolved(len(ids))
    handle = FaissIndex.build(ids, vectors, config)
    key = IndexKey('database-a', 'table-a', 3, 'schema-a', 4,
                   build_fingerprint=config.fingerprint())
    hasher = SnapshotHasher(len(ids), 4)
    hasher.add(ids, vectors)
    descriptor, payload = encode_artifact(handle, key, config, hasher.finish())
    return ids, vectors, handle, descriptor, payload


def test_digest_is_independent_of_batch_partition(source):
    ids, vectors, _, descriptor, _ = source
    many = SnapshotHasher(len(ids), 4)
    for start in range(0, len(ids), 37):
        many.add(ids[start:start+37], vectors[start:start+37])
    assert many.finish() == descriptor.snapshot_sha256


def test_large_int64_and_signed_zero_preserved():
    hasher = SnapshotHasher(1, 2)
    hasher.add(np.array([2**53+1], dtype=np.int64), np.array([[-0.0, 1.0]], dtype=np.float32))
    expected = hashlib.sha256(b'vecstor-index-snapshot-v1' + struct.pack('<QIqff', 1, 2, 2**53+1, -0.0, 1.0)).hexdigest()
    assert hasher.finish() == expected
    positive = SnapshotHasher(1, 2)
    positive.add(np.array([2**53+1], dtype=np.int64), np.array([[0.0, 1.0]], dtype=np.float32))
    assert positive.finish() != expected


@pytest.mark.parametrize('case', ['duplicate', 'unordered', 'negative', 'float_ids', 'nonfinite', 'dimension', 'overflow'])
def test_invalid_snapshot_input_is_rejected(case):
    ids = np.array([1, 2], dtype=np.int64)
    vectors = np.ones((2, 2), dtype=np.float32)
    if case == 'duplicate': ids[1] = 1
    elif case == 'unordered': ids = ids[::-1]
    elif case == 'negative': ids[0] = -1
    elif case == 'float_ids': ids = ids.astype(float)
    elif case == 'nonfinite': vectors[0, 0] = np.nan
    elif case == 'dimension': vectors = vectors[:, :1]
    hasher = SnapshotHasher(1 if case == 'overflow' else 2, 2)
    with pytest.raises(ArtifactError): hasher.add(ids, vectors)


def test_incomplete_snapshot_cannot_finish():
    with pytest.raises(ArtifactError): SnapshotHasher(1, 4).finish()


def test_raw_artifact_roundtrip_preserves_answers(source):
    ids, vectors, handle, descriptor, payload = source
    restored = validate_artifact(payload, descriptor)
    assert payload == faiss.serialize_index(handle._index).tobytes()
    assert np.array_equal(restored.ids, ids)
    expected = handle.search(vectors[17], 20, 8)
    actual = restored.search(vectors[17], 20, 8)
    np.testing.assert_array_equal(actual[0], expected[0])
    np.testing.assert_array_equal(actual[1], expected[1])
    assert ArtifactDescriptor.from_dict(descriptor.to_dict()) == descriptor


def test_descriptor_id_covers_key_and_payload(source):
    _, _, _, descriptor, _ = source
    for change in ({'payload_sha256': '0'*64}, {'key': replace(descriptor.key, revision=4)}):
        forged = replace(descriptor, **change)
        with pytest.raises(ArtifactError): ArtifactDescriptor.from_dict(forged.to_dict())


def test_valid_ids_wrong_vectors_rejected(source):
    ids, vectors, _, descriptor, _ = source
    wrong = FaissIndex.build(ids, vectors + np.float32(1), descriptor.build_config)
    with pytest.raises(ArtifactError, match='snapshot'):
        encode_artifact(wrong, descriptor.key, descriptor.build_config, descriptor.snapshot_sha256)


@pytest.mark.parametrize('field,value', [('format_version', 2), ('faiss_version', '0.0'),
                                        ('platform', 'other'), ('payload_size', True), ('row_count', -1)])
def test_unknown_format_or_faiss_version_rejected(source, field, value):
    data = source[3].to_dict()
    data[field] = value
    with pytest.raises(ArtifactError): ArtifactDescriptor.from_dict(data)


def test_digest_checked_before_native_read(source, monkeypatch):
    descriptor, payload = source[3:]
    def forbidden(*args): pytest.fail('native parser must not see corrupt bytes')
    monkeypatch.setattr(faiss, 'deserialize_index', forbidden)
    with pytest.raises(ArtifactError, match='checksum'):
        validate_artifact(payload[:-1] + bytes([payload[-1] ^ 1]), descriptor)
    with pytest.raises(ArtifactError, match='size'):
        validate_artifact(payload[:-1], descriptor)


def test_descriptor_unknown_fields_rejected(source):
    data = source[3].to_dict()
    data['path'] = '/tmp/untrusted'
    with pytest.raises(ArtifactError): ArtifactDescriptor.from_dict(data)


def test_build_fingerprint_and_structural_configuration_checked(source):
    _, _, handle, descriptor, _ = source
    with pytest.raises(ArtifactError):
        encode_artifact(handle, replace(descriptor.key, build_fingerprint='0'*64),
                        descriptor.build_config, descriptor.snapshot_sha256)
    with pytest.raises(ArtifactError):
        encode_artifact(handle, descriptor.key, BuildConfig(nlist=2), descriptor.snapshot_sha256)
