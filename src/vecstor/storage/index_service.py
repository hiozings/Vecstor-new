"""Index management RPCs share bounded storage capacity with query scans."""
from contextlib import contextmanager
import errno
import hashlib
import json
from threading import BoundedSemaphore
import time

import grpc

from vecstor.index.artifact import ArtifactDescriptor, ArtifactError, require, CHUNK_BYTES
from vecstor.index.types import BuildConfig
from vecstor.rpc import sql_pb2 as pb
from vecstor.rpc.index_codec import descriptor_to_proto, descriptor_from_proto, record_to_proto


def remaining(context):
    if not context.is_active(): raise InterruptedError('index request cancelled')
    value = context.time_remaining()
    value = 15 if value is None else value
    if value <= 0: raise TimeoutError('index request deadline exceeded')
    return value


def error_code(error):
    if isinstance(error, ArtifactError): return error.code
    if isinstance(error, InterruptedError): return 'CANCELLED'
    if isinstance(error, TimeoutError): return 'DEADLINE_EXCEEDED'
    if isinstance(error, OSError): return 'RESOURCE_EXHAUSTED' if error.errno == errno.ENOSPC else 'DATA_LOSS'
    return 'INVALID_ARGUMENT'


def abort(context, error):
    context.abort(getattr(grpc.StatusCode, error_code(error)), str(error))


class IndexPublicationService:
    def __init__(self, catalog, store, stream_slots):
        self.catalog, self.store, self.stream_slots = catalog, store, stream_slots
        self._slots = BoundedSemaphore(2)

    @contextmanager
    def heavy(self):
        require(self.stream_slots.acquire(blocking=False), 'storage_scan_capacity', 'RESOURCE_EXHAUSTED')
        acquired = False
        try:
            acquired = self._slots.acquire(blocking=False)
            require(acquired, 'artifact_transfer_capacity', 'RESOURCE_EXHAUSTED')
            yield
        finally:
            if acquired: self._slots.release()
            self.stream_slots.release()

    def call(self, database_id, method, context, *args):
        return self.catalog.index_call(database_id, method, *args, timeout=remaining(context), cancelled=lambda:not context.is_active())

    def failed(self, database_id, operation_id, error):
        try: self.catalog.index_call(database_id, 'fail_index_publication', operation_id, error_code(error), str(error), timeout=1)
        except (ValueError, TimeoutError, OSError): pass

    def BeginIndexPublication(self, request, context):
        try:
            require(len(request.intent_json.encode()) <= 64*1024, 'publication_intent_too_large')
            intent = json.loads(request.intent_json)
            config = BuildConfig(request.config.nlist, request.config.seed, request.config.train_rows)
            result = self.call(request.database_id, 'begin_index_publication', context, request.token,
                               request.operation_id, config, request.expected_generation, intent)
            return record_to_proto(result)
        except (ValueError, OSError, TimeoutError) as error: abort(context, error)

    def UploadIndexArtifact(self, frames, context):
        identity = None
        owned = False
        try:
            with self.heavy():
                first = next(frames, None)
                require(first is not None and first.WhichOneof('content') == 'header' and first.ByteSize() <= 4*1024**2,
                        'artifact_header_required', 'DATA_LOSS')
                database_id, token, operation_id = first.database_id, first.token, first.operation_id
                identity = (database_id, operation_id)
                record = self.call(database_id, 'check_index_publication', context, operation_id, token)
                require(record.state == 'RESERVED', 'publication_upload_not_reserved', 'FAILED_PRECONDITION')
                descriptor = descriptor_from_proto(first.header)
                require(descriptor.key == record.key, 'publication_key_mismatch', 'DATA_LOSS')
                self.store.begin_upload(identity, descriptor)
                owned = True
                self.call(database_id, 'bind_index_upload', context, operation_id, descriptor)
                trailer = False
                for frame in frames:
                    remaining(context)
                    self.call(database_id, 'leased_index', context, token)
                    require((frame.database_id,frame.token,frame.operation_id) == (database_id,token,operation_id)
                            and frame.ByteSize() <= 4*1024**2 and not trailer, 'artifact_upload_frame_identity', 'DATA_LOSS')
                    kind = frame.WhichOneof('content')
                    if kind == 'chunk': self.store.append(identity, frame.chunk.offset, frame.chunk.data)
                    elif kind == 'trailer':
                        require(frame.trailer.total_size == descriptor.payload_size and frame.trailer.payload_sha256 == descriptor.payload_sha256,
                                'artifact_upload_trailer_mismatch', 'DATA_LOSS')
                        trailer = True
                    else: raise ArtifactError('DATA_LOSS', 'invalid_upload_frame_order')
                require(trailer, 'incomplete_artifact_upload', 'DATA_LOSS')
                self.call(database_id, 'leased_index', context, token)
                self.store.seal(identity)
                result = self.call(database_id, 'index_phase', context, operation_id, 'STAGED')
                owned = False  # reservation now belongs to Finalize or lease cleanup
                return record_to_proto(result)
        except (ValueError, OSError, TimeoutError) as error:
            if owned and identity: self.failed(identity[0], identity[1], error)
            abort(context, error)
        finally:
            if owned and identity: self.store.abort_upload(identity)

    def FinalizeIndexPublication(self, request, context):
        owned = False
        identity = (request.database_id, request.operation_id)
        try:
            with self.heavy():
                record, owned = self.call(request.database_id, 'claim_index_finalize', context, request.operation_id, request.token)
                if not owned: return record_to_proto(record)
                descriptor = record.descriptor
                require(descriptor is not None, 'publication_has_no_artifact', 'FAILED_PRECONDITION')
                if not self.store.prepared(descriptor):
                    receipt = self.call(request.database_id, 'snapshot_receipt', context, request.token)
                    ttl = self.call(request.database_id, 'index_lease_remaining', context, request.token)
                    self.store.validate_and_install(identity, receipt, time.monotonic()+min(ttl, remaining(context)), lambda:not context.is_active())
                result = self.call(request.database_id, 'commit_index_publication', context, request.operation_id, descriptor, request.token)
                return record_to_proto(result)
        except (ValueError, OSError, TimeoutError) as error:
            if owned: self.failed(request.database_id, request.operation_id, error)
            abort(context, error)
        finally:
            if owned: self.store.abort_upload(identity)

    def GetIndexPublication(self, request, context):
        try: return record_to_proto(self.call(request.database_id, 'get_index_publication', context, request.operation_id), request.operation_id)
        except (ValueError, OSError, TimeoutError) as error: abort(context, error)

    def ListPublishedIndexes(self, request, context):
        try:
            records, cursor = self.call(request.database_id, 'list_published_indexes', context, request.table, request.page_size or 128, request.cursor)
            return pb.ListPublishedIndexesResponse(indexes=[pb.PublishedIndexStatus(
                descriptor=descriptor_to_proto(ArtifactDescriptor.from_dict(r['descriptor'])), state=r['state'],
                generation=r['generation'], matches_current=r['matches_current']) for r in records], next_cursor=cursor)
        except (ValueError, OSError, TimeoutError) as error: abort(context, error)

    def DownloadIndexArtifact(self, request, context):
        try:
            with self.heavy():
                lease = self.call(request.database_id, 'leased_index', context, request.token)
                require(lease['published_index'] is not None and lease['published_index']['artifact_id'] == request.artifact_id,
                        'artifact_not_bound_to_lease', 'FAILED_PRECONDITION')
                require(lease['published_index_state'] == 'ready', 'published_artifact_corrupt', 'DATA_LOSS')
                descriptor = ArtifactDescriptor.from_dict(lease['published_index'])
                identity = dict(database_id=request.database_id, token=request.token, artifact_id=request.artifact_id)
                with self.store.pin(descriptor) as source:
                    yield pb.IndexArtifactFrame(**identity, descriptor=descriptor_to_proto(descriptor))
                    offset, hasher = 0, hashlib.sha256()
                    while True:
                        self.call(request.database_id, 'leased_index', context, request.token)
                        chunk = source.read(CHUNK_BYTES)
                        if not chunk: break
                        require(offset+len(chunk) <= descriptor.payload_size, 'artifact_file_size', 'DATA_LOSS')
                        hasher.update(chunk)
                        yield pb.IndexArtifactFrame(**identity, chunk=pb.IndexArtifactChunk(offset=offset, data=chunk))
                        offset += len(chunk)
                    require(offset == descriptor.payload_size and hasher.hexdigest() == descriptor.payload_sha256, 'artifact_file_checksum', 'DATA_LOSS')
                    remaining(context)
                    yield pb.IndexArtifactFrame(**identity, complete=pb.IndexArtifactComplete(total_size=offset, payload_sha256=descriptor.payload_sha256))
        except (ValueError, OSError, TimeoutError) as error: abort(context, error)

    def PruneIndexArtifacts(self, request, context):
        context.abort(grpc.StatusCode.UNIMPLEMENTED, 'Index pruning is not enabled yet')
