import pytest

from vecstor.index.artifact import ArtifactError
from vecstor.rpc import sql_pb2 as pb
from vecstor.rpc.index_codec import descriptor_to_proto, descriptor_from_proto, read_artifact_frames


def frames(artifact):
    descriptor, payload = artifact
    identity = dict(database_id=descriptor.key.database_id, token='lease', artifact_id=descriptor.artifact_id)
    return [pb.IndexArtifactFrame(**identity, descriptor=descriptor_to_proto(descriptor)),
            pb.IndexArtifactFrame(**identity, chunk=pb.IndexArtifactChunk(offset=0, data=payload)),
            pb.IndexArtifactFrame(**identity, complete=pb.IndexArtifactComplete(total_size=len(payload), payload_sha256=descriptor.payload_sha256))]


def test_artifact_protocol_roundtrip(cpu_artifact):
    descriptor, payload = cpu_artifact
    assert descriptor_from_proto(descriptor_to_proto(descriptor)) == descriptor
    actual, data = read_artifact_frames(iter(frames(cpu_artifact)), descriptor.key.database_id, 'lease', descriptor)
    assert actual == descriptor and data == payload


@pytest.mark.parametrize('case', ['no_complete','extra','offset','identity','oversize','no_header','truncated'])
def test_invalid_artifact_stream_never_returns_success(cpu_artifact, case):
    stream = frames(cpu_artifact)
    if case == 'no_complete': stream.pop()
    elif case == 'extra': stream.append(stream[-1])
    elif case == 'offset': stream[1].chunk.offset = 1
    elif case == 'identity': stream[1].token = 'other'
    elif case == 'oversize': stream[1].chunk.data = b'x'*(4*1024**2)
    elif case == 'no_header': stream.pop(0)
    elif case == 'truncated': stream[1].chunk.data = stream[1].chunk.data[:-1]
    with pytest.raises(ArtifactError):
        read_artifact_frames(iter(stream), cpu_artifact[0].key.database_id, 'lease', cpu_artifact[0])


def test_legacy_read_lease_fields_remain_stable():
    raw = b'\x0a\x01t\x12\x01d\x1a\x01i\x20\x05'
    lease = pb.ReadLease.FromString(raw)
    assert (lease.token, lease.database_id, lease.table_id, lease.revision) == ('t', 'd', 'i', 5)
    assert lease.index_publication_version == 0
