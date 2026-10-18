"""Typed publication messages and fail-closed artifact stream decoding."""
from dataclasses import asdict
import hashlib
import json

from vecstor.index.artifact import ArtifactDescriptor, ArtifactError, PublicationRecord, canonical, require, CHUNK_BYTES
from vecstor.index.types import IndexKey, BuildConfig
from . import sql_pb2 as pb


def descriptor_to_proto(value):
    return pb.IndexArtifactDescriptor(**value.to_dict())


def descriptor_from_proto(value):
    data = {name:getattr(value, name) for name in ArtifactDescriptor.__dataclass_fields__ if name not in ('key','build_config')}
    data['key'] = {name:getattr(value.key, name) for name in IndexKey.__dataclass_fields__}
    data['build_config'] = {name:getattr(value.build_config, name) for name in BuildConfig.__dataclass_fields__}
    return ArtifactDescriptor.from_dict(data)


def record_to_proto(record, operation_id=''):
    if record is None: return pb.IndexPublicationRecord(operation_id=operation_id, state='UNKNOWN')
    values = dict(operation_id=record.operation_id, operation_kind=record.operation_kind, state=record.state,
        intent_json=canonical(record.request_intent).decode(), intent_digest=record.intent_digest,
        request_digest=record.request_digest, expected_generation=record.expected_generation, generation=record.generation,
        error_code=(record.error or {}).get('code',''), error_reason=(record.error or {}).get('reason',''),
        result_json=canonical(record.result).decode())
    if record.key: values['key'] = asdict(record.key)
    if record.descriptor: values['descriptor'] = descriptor_to_proto(record.descriptor)
    return pb.IndexPublicationRecord(**values)


def record_from_proto(value):
    require(value.state in ('UNKNOWN','RESERVED','UPLOADING','STAGED','VALIDATING','PRUNING','COMMITTED','FAILED','ABORTED'), 'invalid_publication_state', 'DATA_LOSS')
    if value.state == 'UNKNOWN': return None
    return PublicationRecord(value.operation_id, value.operation_kind, json.loads(value.intent_json), value.intent_digest,
        value.request_digest, IndexKey(**{n:getattr(value.key,n) for n in IndexKey.__dataclass_fields__}) if value.HasField('key') else None,
        value.expected_generation, value.state, descriptor_from_proto(value.descriptor) if value.HasField('descriptor') else None,
        value.generation, dict(code=value.error_code,reason=value.error_reason) if value.error_code else None,
        json.loads(value.result_json) if value.result_json else None)


def upload_frames(database_id, token, operation_id, descriptor, payload, check=lambda:None):
    identity = dict(database_id=database_id, token=token, operation_id=operation_id)
    check()
    yield pb.IndexUploadFrame(**identity, header=descriptor_to_proto(descriptor))
    for offset in range(0, len(payload), CHUNK_BYTES):
        check()
        yield pb.IndexUploadFrame(**identity, chunk=pb.IndexArtifactChunk(offset=offset, data=payload[offset:offset+CHUNK_BYTES]))
    check()
    yield pb.IndexUploadFrame(**identity, trailer=pb.IndexArtifactComplete(total_size=len(payload), payload_sha256=descriptor.payload_sha256))


def read_artifact_frames(frames, database_id, token, expected, check=lambda:None):
    descriptor = None
    output = bytearray()
    complete = False
    for frame in frames:
        check()
        require(frame.ByteSize() <= 4*1024**2 and frame.database_id == database_id and frame.token == token
                and frame.artifact_id == expected.artifact_id and not complete, 'artifact_frame_identity_or_order', 'DATA_LOSS')
        kind = frame.WhichOneof('content')
        if kind == 'descriptor':
            require(descriptor is None and not output, 'duplicate_artifact_header', 'DATA_LOSS')
            descriptor = descriptor_from_proto(frame.descriptor)
            require(descriptor == expected, 'artifact_descriptor_mismatch', 'DATA_LOSS')
        elif kind == 'chunk':
            require(descriptor is not None and 0 < len(frame.chunk.data) <= CHUNK_BYTES
                    and frame.chunk.offset == len(output) and len(output)+len(frame.chunk.data) <= descriptor.payload_size,
                    'artifact_chunk_order_or_size', 'DATA_LOSS')
            output.extend(frame.chunk.data)
        elif kind == 'complete':
            require(descriptor is not None and frame.complete.total_size == len(output) == descriptor.payload_size
                    and frame.complete.payload_sha256 == descriptor.payload_sha256,
                    'artifact_completion_mismatch', 'DATA_LOSS')
            complete = True
        else: raise ArtifactError('DATA_LOSS', 'invalid_artifact_frame')
    require(complete and hashlib.sha256(output).hexdigest() == expected.payload_sha256, 'incomplete_artifact_or_checksum', 'DATA_LOSS')
    check()
    return descriptor, bytes(output)


def validate_publication_plan(plan, revision):
    if plan.get('index_source') != 'published': return
    require(plan.get('index_publication_version') == 1 and type(plan.get('publication_generation')) is int
            and plan['publication_generation'] >= 0 and type(plan.get('revision')) is int
            and plan['revision'] == revision, 'published_snapshot_confirmation', 'DATA_LOSS')
    if plan.get('actual_mode') != 'approx':
        require(plan.get('artifact_id') is None and plan.get('payload_sha256') is None,
                'unexpected_actual_artifact', 'DATA_LOSS')
        return
    index = plan.get('index') or {}
    for name in ('artifact_id', 'payload_sha256'):
        value = plan.get(name)
        require(isinstance(value,str) and len(value)==64 and all(c in '0123456789abcdef' for c in value)
                and value == index.get(name), 'published_artifact_confirmation', 'DATA_LOSS')
    require(plan['artifact_id'] == plan.get('published_artifact_id'), 'published_artifact_mismatch', 'DATA_LOSS')
    require(all(index.get(name) == plan.get(name) and index.get(name) is not None
                for name in ('database_id','table_id','schema_version','revision','dimension','metric')),
            'published_snapshot_mismatch', 'DATA_LOSS')
