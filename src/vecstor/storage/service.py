"""Storage-only metadata filtering and ID-based vector transport."""

from concurrent.futures import ThreadPoolExecutor
import hashlib
from io import BytesIO
from pathlib import Path
from threading import BoundedSemaphore, Lock

import duckdb
import grpc

from vecstor.datasets import load_dataset
from vecstor.query.parser import QueryError, parse_query
from vecstor.rpc import services_pb2 as pb, services_pb2_grpc as rpc
from vecstor.rpc.common import (DEFAULT_BATCH_ROWS, MAX_BATCH_ROWS, MAX_MESSAGE_BYTES,
                                OPTIONS, VECTOR_PAYLOAD_BYTES, active)


class StorageService(rpc.StorageServicer):
    def __init__(self, path):
        raw = Path(path).read_bytes()
        self.version = hashlib.sha256(raw).hexdigest()
        self.dataset = load_dataset(BytesIO(raw))
        self._positions = {int(identifier): position for position, identifier in enumerate(self.dataset.ids)}
        self._lock = Lock()
        # Streaming handlers can block on HTTP/2 flow control. Bound their
        # occupancy below the executor size so FetchVectors can always run.
        self._scan_slots = BoundedSemaphore(4)
        self._db = duckdb.connect(":memory:", config={"threads": 1})
        try:
            self._db.execute('CREATE TABLE items (_row BIGINT, id BIGINT, category VARCHAR, "timestamp" BIGINT, value DOUBLE)')
            if self.dataset.size:
                data = self.dataset
                self._db.executemany("INSERT INTO items VALUES (?, ?, ?, ?, ?)", list(zip(
                    range(data.size), data.ids.tolist(), data.categories.tolist(),
                    data.timestamps.tolist(), data.values.tolist())))
        except Exception:
            self._db.close()
            raise

    def close(self):
        self._db.close()

    def _version(self, version, context):
        if version != self.version:
            context.abort(grpc.StatusCode.FAILED_PRECONDITION, "Snapshot version unavailable")

    def GetManifest(self, request, context):
        return pb.Manifest(version=self.version, rows=self.dataset.size,
                           dimension=self.dataset.dimension, metric="squared_l2")

    def ScanMetadata(self, request, context):
        self._version(request.version, context)
        batch_rows = request.batch_rows or DEFAULT_BATCH_ROWS
        if batch_rows > MAX_BATCH_ROWS:
            context.abort(grpc.StatusCode.RESOURCE_EXHAUSTED, "Metadata batch limit exceeded")
        try:
            plan = parse_query(request.sql)
        except QueryError as error:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, str(error))
        if not self._scan_slots.acquire(blocking=False):
            context.abort(grpc.StatusCode.RESOURCE_EXHAUSTED, "At most four active metadata scans are allowed")
        try:
            yield from self._scan_batches(plan, batch_rows, context)
        finally:
            self._scan_slots.release()

    def _scan_batches(self, plan, batch_rows, context):
        active(context)
        with self._lock:
            active(context)
            rows = self._db.execute(plan.filter_sql, plan.filter_params).fetchall()
        for offset in range(0, max(1, len(rows)), batch_rows):
            active(context)
            batch = pb.MetadataBatch(version=self.version, total_rows=len(rows), offset=offset,
                       rows=[pb.Row(id=row[1], category=row[2], timestamp=row[3], value=row[4])
                             for row in rows[offset:offset + batch_rows]])
            if batch.ByteSize() > MAX_MESSAGE_BYTES:
                context.abort(grpc.StatusCode.RESOURCE_EXHAUSTED, "Metadata message too large")
            yield batch

    def FetchVectors(self, request, context):
        self._version(request.version, context)
        identifiers = list(request.ids)
        if len(identifiers) > MAX_BATCH_ROWS or len(identifiers) * self.dataset.dimension * 4 > VECTOR_PAYLOAD_BYTES:
            context.abort(grpc.StatusCode.RESOURCE_EXHAUSTED, "Vector batch limit exceeded")
        if any(identifier < 0 for identifier in identifiers) or len(set(identifiers)) != len(identifiers):
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "IDs must be nonnegative and distinct")
        if any(identifier not in self._positions for identifier in identifiers):
            context.abort(grpc.StatusCode.NOT_FOUND, "One or more vector IDs do not exist")
        active(context)
        positions = [self._positions[identifier] for identifier in identifiers]
        vectors = self.dataset.vectors[positions].astype("<f4", copy=False)
        return pb.VectorBatch(version=self.version, dimension=self.dataset.dimension,
                              ids=identifiers, vectors_f32_le=vectors.tobytes(order="C"))


def create_storage_server(path, address):
    backend = StorageService(path)
    server = grpc.server(ThreadPoolExecutor(max_workers=8), options=OPTIONS)
    rpc.add_StorageServicer_to_server(backend, server)
    port = server.add_insecure_port(address)
    if not port:
        backend.close()
        raise ValueError(f"Cannot bind storage address: {address}")
    return server, port, backend
