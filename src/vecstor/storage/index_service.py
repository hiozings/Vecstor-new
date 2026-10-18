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
        self._prune_slot = BoundedSemaphore(1)

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
        acquired=False
        try:
            with self.heavy():
                acquired=self._prune_slot.acquire(blocking=False)
                require(acquired,'index_prune_busy','RESOURCE_EXHAUSTED')
                keep=request.keep_last if request.HasField('keep_last') else 2
                dry=request.dry_run if request.HasField('dry_run') else True
                result=self.call(request.database_id,'begin_index_prune',context,request.operation_id or None,keep,dry)
                if not dry:result=self._finish_prune(request.database_id,request.operation_id,context)
                return pb.PruneIndexArtifactsResponse(candidates=result.candidates,protected=result.protected,deleted=result.deleted,
                    deleted_bytes=result.deleted_bytes,state='DRY_RUN' if dry else 'COMMITTED',byte_accounting='logical_payload_and_manifest')
        except (ValueError,OSError,TimeoutError) as error:abort(context,error)
        finally:
            if acquired:self._prune_slot.release()

    def _finish_prune(self,database_id,operation_id,context):
        from vecstor.index.artifact import PruneResult
        record=self.call(database_id,'get_index_publication',context,operation_id)
        require(record is not None and record.operation_kind=='PRUNE','unknown_prune_operation','NOT_FOUND')
        for identity in record.result['candidates']:
            if identity in record.result['deleted']:continue
            remaining(context)
            descriptor=self.call(database_id,'index_artifact',context,identity)
            self.store.mark_retired([descriptor])
            self.store.delete_retired(descriptor)
            self.call(database_id,'finish_index_prune',context,operation_id,[identity])
        record=self.call(database_id,'get_index_publication',context,operation_id)
        return PruneResult(tuple(record.result['candidates']),tuple(record.result['protected']),
                           tuple(record.result['deleted']),record.result['deleted_bytes'])

    def recover(self):
        from vecstor.rpc.common import DeadlineContext
        require(not any(path.is_symlink() for path in (self.catalog.root/'databases').glob('*.duckdb')),
                'cannot_recover_symlink_database','FAILED_PRECONDITION')
        databases=self.catalog.list_databases()
        records={db['id']:self.catalog.index_call(db['id'],'index_recovery_records',timeout=60) for db in databases}
        references=[ArtifactDescriptor.from_dict(row['descriptor']) for value in records.values()
                    for row in value['artifacts'] if row['state']!='DELETED']
        # Gather every database reference before considering any file an orphan.
        report=self.store.recover(references)
        for db in databases:
            pending=self.catalog.index_call(db['id'],'recover_indexes',report,timeout=60)
            for operation in pending:self._finish_prune(db['id'],operation,DeadlineContext(time.monotonic()+60))
        active={row['descriptor']['artifact_id'] for value in records.values() for row in value['artifacts'] if row['state'] in ('READY','CORRUPT')}
        report['corrupt']=[identity for identity in report['corrupt'] if identity in active]
        report['databases_checked']=len(databases)
        return report
