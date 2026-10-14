"""Managed catalog RPC boundary with bounded streams and leased vector reads."""
from concurrent.futures import ThreadPoolExecutor
import json
import math
from threading import BoundedSemaphore, Lock
import time

import grpc
import numpy as np

from vecstor.rpc import sql_pb2 as pb, sql_pb2_grpc as rpc, services_pb2 as v1
from vecstor.rpc.common import (DEFAULT_BATCH_ROWS, MAX_BATCH_ROWS, MAX_MESSAGE_BYTES,
                                OPTIONS, VECTOR_PAYLOAD_BYTES)
from vecstor.rpc.sql_codec import encode_result
from vecstor.storage.catalog import DatabaseCatalog


def _remaining(context):
    remaining = context.time_remaining()
    if remaining is None:
        remaining = 15.0
    if remaining <= 0:
        context.abort(grpc.StatusCode.DEADLINE_EXCEEDED, 'SQL deadline exceeded')
    if not context.is_active():
        context.abort(grpc.StatusCode.CANCELLED, 'SQL request cancelled')
    return remaining


def _abort(context, error):
    if isinstance(error, TimeoutError):
        code = grpc.StatusCode.DEADLINE_EXCEEDED
    elif isinstance(error, OSError):
        code = grpc.StatusCode.NOT_FOUND
    elif 'lease unavailable or expired' in str(error).lower():
        code = grpc.StatusCode.FAILED_PRECONDITION
    else:
        code = grpc.StatusCode.INVALID_ARGUMENT
    context.abort(code, str(error))


def _bounded(message, context):
    if message.ByteSize() > MAX_MESSAGE_BYTES:
        context.abort(grpc.StatusCode.RESOURCE_EXHAUSTED, 'SQL message exceeds 4 MiB')
    return message


class SQLStorageService(rpc.SQLStorageServicer):
    def __init__(self, root, candidate_binding="text",candidate_plan_cache=True):
        self.catalog = DatabaseCatalog(root,candidate_binding,candidate_plan_cache)
        # Shared occupancy cap prevents either stream type from starving the
        # FetchVectors, BeginRead and EndRead unary handlers.
        self._stream_slots = BoundedSemaphore(4)
        self._lease_lock = Lock()
        self._leases = {}

    def close(self):
        self.catalog.close()
        with self._lease_lock:
            self._leases.clear()

    def Control(self, request, context):
        timeout = _remaining(context)
        try:
            if request.action == 'list_databases':
                result = self.catalog.list_databases()
            elif request.action == 'open_database':
                result = self.catalog.open_database(request.name, request.create)
            elif request.action == 'list_datasets':
                result = self.catalog.list_datasets()
            elif request.action == 'describe':
                result = self.catalog.describe(request.database_id, request.table or None)
            elif request.action == 'status':
                result = self.catalog.status(request.database_id, request.operation_id)
            elif request.action == 'import':
                result = self.catalog.import_dataset(request.database_id, request.source,
                        request.table, request.operation_id, timeout=timeout,cancelled=lambda:not context.is_active())
            else:
                raise ValueError('Unknown catalog action')
            _remaining(context)
            return _bounded(pb.ControlResponse(json_result=json.dumps(result, ensure_ascii=False, allow_nan=False)), context)
        except (ValueError, OSError, TimeoutError) as error:
            _abort(context, error)

    def ExecuteSQL(self, request, context):
        timeout = _remaining(context)
        if not self._stream_slots.acquire(blocking=False):
            context.abort(grpc.StatusCode.RESOURCE_EXHAUSTED, 'At most four active SQL/metadata streams are allowed')
        try:
            start = time.monotonic()
            result = self.catalog.execute(request.database_id, request.sql, request.operation_id, timeout=timeout,cancelled=lambda:not context.is_active())
            result['elapsed_seconds'] = time.monotonic() - start
            for frame in encode_result(result):
                _remaining(context)
                yield frame
        except (ValueError, OSError, TimeoutError) as error:
            _abort(context, error)
        finally:
            self._stream_slots.release()

    def BeginRead(self, request, context):
        timeout = _remaining(context)
        try:
            ttl = request.ttl_seconds or timeout
            if not math.isfinite(ttl) or ttl <= 0:
                raise ValueError('Read TTL must be finite and positive')
            ttl = min(ttl, timeout, 60.0)
            lease = self.catalog.begin_read(request.database_id, request.table, ttl=ttl,timeout=timeout)
            with self._lease_lock:
                now = time.monotonic()
                self._leases = {key:value for key,value in self._leases.items() if value[1] > now}
                self._leases[lease['token']] = (lease, now + ttl)
            return pb.ReadLease(**lease)
        except (ValueError, OSError, TimeoutError) as error:
            _abort(context, error)

    def _lease(self, database_id, token):
        with self._lease_lock:
            item = self._leases.get(token)
            if item is None or item[1] <= time.monotonic() or item[0]['database_id'] != database_id:
                raise ValueError('Read lease unavailable or expired')
            return item[0]

    def EndRead(self, request, context):
        _remaining(context)
        try:
            self.catalog.end_read(request.database_id, request.token)
            with self._lease_lock:
                item = self._leases.get(request.token)
                if item is not None and item[0]['database_id'] == request.database_id:
                    del self._leases[request.token]
            return v1.Empty()
        except (ValueError, OSError, TimeoutError) as error:
            _abort(context, error)

    def ScanMetadata(self, request, context):
        _remaining(context)
        rows_per_batch = request.batch_rows or DEFAULT_BATCH_ROWS
        if rows_per_batch > MAX_BATCH_ROWS:
            context.abort(grpc.StatusCode.RESOURCE_EXHAUSTED, 'Metadata batch exceeds row limit')
        if not self._stream_slots.acquire(blocking=False):
            context.abort(grpc.StatusCode.RESOURCE_EXHAUSTED, 'At most four active SQL/metadata streams are allowed')
        try:
            lease = self._lease(request.database_id, request.token)
            rows = self.catalog.scan(request.database_id, request.token, request.sql,timeout=_remaining(context),cancelled=lambda:not context.is_active())
            for offset in range(0, max(1, len(rows)), rows_per_batch):
                _remaining(context)
                batch = pb.MetadataBatch(token=request.token, revision=lease['revision'],
                        total_rows=len(rows), offset=offset,
                        rows=[v1.Row(id=row[0], category=row[1], timestamp=row[2], value=row[3])
                              for row in rows[offset:offset + rows_per_batch]])
                yield _bounded(batch, context)
        except (ValueError, OSError, TimeoutError) as error:
            _abort(context, error)
        finally:
            self._stream_slots.release()

    def FetchVectors(self, request, context):
        _remaining(context)
        try:
            lease = self._lease(request.database_id, request.token)
            if len(request.ids) > MAX_BATCH_ROWS or len(request.ids) * lease['dimension'] * 4 > VECTOR_PAYLOAD_BYTES:
                context.abort(grpc.StatusCode.RESOURCE_EXHAUSTED, 'Vector batch exceeds transfer limit')
            vectors = self.catalog.vectors(request.database_id, request.token, request.ids,timeout=_remaining(context))
            _remaining(context)
            return _bounded(pb.VectorBatch(token=request.token, revision=lease['revision'],
                    dimension=lease['dimension'], ids=request.ids,
                    vectors_f32_le=vectors.astype('<f4', copy=False).tobytes(order='C')), context)
        except (ValueError, OSError, TimeoutError) as error:
            _abort(context, error)

    def StreamVectorSnapshot(self, request, context):
        _remaining(context)
        if not self._stream_slots.acquire(blocking=False):
            context.abort(grpc.StatusCode.RESOURCE_EXHAUSTED, 'At most four active streams are allowed')
        try:
            lease=self._lease(request.database_id,request.token)
            requested=request.batch_rows or DEFAULT_BATCH_ROWS
            if requested > MAX_BATCH_ROWS or lease['dimension']*4 > VECTOR_PAYLOAD_BYTES:
                context.abort(grpc.StatusCode.RESOURCE_EXHAUSTED,'Snapshot batch exceeds transfer limit')
            batch_rows=min(requested,VECTOR_PAYLOAD_BYTES//(lease['dimension']*4))
            total=lease['row_count'];offset=0;after=-1
            while True:
                rows=self.catalog.snapshot_page(request.database_id,request.token,after,batch_rows,
                        timeout=_remaining(context),cancelled=lambda:not context.is_active())
                if offset+len(rows)>total or (not rows and offset!=total):
                    context.abort(grpc.StatusCode.DATA_LOSS,'Snapshot count mismatch')
                if rows or total==0:
                    vectors=np.asarray([r[1] for r in rows],dtype='<f4').reshape(len(rows),lease['dimension'])
                    yield _bounded(pb.SnapshotBatch(token=request.token,revision=lease['revision'],
                        table_id=lease['table_id'],schema_version=lease['schema_version'],dimension=lease['dimension'],
                        total_rows=total,offset=offset,ids=[r[0] for r in rows],vectors_f32_le=vectors.tobytes()),context)
                offset+=len(rows)
                if offset==total: break
                after=rows[-1][0]
        except (ValueError,OSError,TimeoutError) as error:
            _abort(context,error)
        finally:
            self._stream_slots.release()

    def FilterCandidates(self, request, context):
        if not self._stream_slots.acquire(blocking=False):
            context.abort(grpc.StatusCode.RESOURCE_EXHAUSTED,'At most four active filtering/stream requests are allowed')
        try:
            lease=self._lease(request.database_id,request.token)
            rows=self.catalog.filter_candidates(request.database_id,request.token,request.sql,request.ids,
                    timeout=_remaining(context),cancelled=lambda:not context.is_active())
            return _bounded(pb.CandidateResult(token=request.token,revision=lease['revision'],
                checked_count=len(request.ids),checked_ids=request.ids,
                rows=[v1.Row(id=r[0],category=r[1],timestamp=r[2],value=r[3]) for r in rows]),context)
        except (ValueError,OSError,TimeoutError) as error:
            _abort(context,error)
        finally:
            self._stream_slots.release()


def create_catalog_server(root, address, candidate_binding="text",candidate_plan_cache=True):
    backend = SQLStorageService(root,candidate_binding,candidate_plan_cache)
    server = grpc.server(ThreadPoolExecutor(max_workers=8), options=OPTIONS)
    rpc.add_SQLStorageServicer_to_server(backend, server)
    try:
        port = server.add_insecure_port(address)
        if not port:
            raise ValueError(f'Cannot bind catalog address: {address}')
    except Exception:
        backend.close()
        raise
    return server, port, backend
