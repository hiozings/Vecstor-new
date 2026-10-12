"""Remote-only exact query execution with a single end-to-end deadline."""

import math
import time

import grpc
import numpy as np

from vecstor.data import query_vector
from vecstor.query.parser import parse_query
from vecstor.rpc import services_pb2 as pb, services_pb2_grpc as rpc
from vecstor.rpc.client import response_dict
from vecstor.rpc.common import (DEFAULT_BATCH_ROWS, MAX_BATCH_ROWS, OPTIONS,
                                RemoteError, VECTOR_PAYLOAD_BYTES, positive_timeout)


def _consistent(condition, message):
    if not condition:
        raise RemoteError(grpc.StatusCode.DATA_LOSS, message)


class RemoteEngine:
    def __init__(self, storage_address, timeout=10, batch_rows=DEFAULT_BATCH_ROWS):
        self.timeout = positive_timeout(timeout)
        if isinstance(batch_rows, bool) or not isinstance(batch_rows, int) or not 1 <= batch_rows <= MAX_BATCH_ROWS:
            raise ValueError(f"batch_rows must be between 1 and {MAX_BATCH_ROWS}")
        self.batch_rows = batch_rows
        self.channel = grpc.insecure_channel(storage_address, options=OPTIONS)
        self.stub = rpc.StorageStub(self.channel)

    def close(self):
        self.channel.close()

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()

    def execute(self, sql, vector):
        return response_dict(self.query_response(sql, vector))

    def query_response(self, sql, vector, timeout=None, is_active=None):
        budget = self.timeout if timeout is None else min(self.timeout, positive_timeout(timeout))
        deadline = time.monotonic() + budget

        def remaining():
            left = deadline - time.monotonic()
            if left <= 0:
                raise RemoteError(grpc.StatusCode.DEADLINE_EXCEEDED, "Query deadline exceeded")
            if is_active is not None and not is_active():
                raise RemoteError(grpc.StatusCode.CANCELLED, "Query cancelled")
            return left

        plan = parse_query(sql)
        rpc_calls = 0
        payload_bytes = 0

        def call(method, request):
            nonlocal rpc_calls, payload_bytes
            rpc_calls += 1
            payload_bytes += request.ByteSize()
            response = method(request, timeout=remaining())
            payload_bytes += response.ByteSize()
            return response

        stream = None
        try:
            manifest = call(self.stub.GetManifest, pb.Empty())
            _consistent(bool(manifest.version) and manifest.dimension > 0 and manifest.metric == "squared_l2", "Invalid manifest")
            if manifest.dimension * 4 > VECTOR_PAYLOAD_BYTES:
                raise RemoteError(grpc.StatusCode.RESOURCE_EXHAUSTED, "Single vector exceeds transfer limit")
            query = query_vector(vector, manifest.dimension).astype(np.float64)
            batch_rows = min(self.batch_rows, VECTOR_PAYLOAD_BYTES // (manifest.dimension * 4))
            scan_request = pb.ScanRequest(version=manifest.version, sql=sql, batch_rows=batch_rows)
            rpc_calls += 1
            payload_bytes += scan_request.ByteSize()
            stream = self.stub.ScanMetadata(scan_request, timeout=remaining())
            count, total, batch_count = 0, None, 0
            seen_ids = set()
            best_rows = []
            best_distances = np.empty(0, dtype=np.float64)
            for batch in stream:
                remaining()
                batch_count += 1
                payload_bytes += batch.ByteSize()
                _consistent(batch.version == manifest.version, "Metadata snapshot mismatch")
                if total is None:
                    total = batch.total_rows
                _consistent(batch.total_rows == total <= manifest.rows and batch.offset == count, "Metadata count/offset mismatch")
                _consistent(len(batch.rows) <= batch_rows and count + len(batch.rows) <= total, "Metadata batch overflow")
                if not batch.rows:
                    _consistent(total == 0 and batch_count == 1, "Unexpected empty batch")
                    continue
                identifiers = [row.id for row in batch.rows]
                _consistent(len(set(identifiers)) == len(identifiers) and not seen_ids.intersection(identifiers), "Duplicate metadata ID")
                _consistent(all(row.id >= 0 and row.category and "\x00" not in row.category and math.isfinite(row.value)
                                for row in batch.rows), "Invalid metadata values")
                seen_ids.update(identifiers)
                fetched = call(self.stub.FetchVectors, pb.FetchRequest(version=manifest.version, ids=identifiers))
                _consistent(fetched.version == manifest.version and fetched.dimension == manifest.dimension, "Vector snapshot/dimension mismatch")
                _consistent(list(fetched.ids) == identifiers, "Vector ID order mismatch")
                _consistent(len(fetched.vectors_f32_le) == len(identifiers) * manifest.dimension * 4, "Vector payload length mismatch")
                vectors = np.frombuffer(fetched.vectors_f32_le, dtype="<f4").reshape(len(identifiers), manifest.dimension)
                _consistent(np.isfinite(vectors).all(), "Nonfinite remote vector")
                delta = vectors.astype(np.float64) - query
                distances = np.sum(delta * delta, axis=1, dtype=np.float64)
                rows = best_rows + list(batch.rows)
                all_distances = np.concatenate((best_distances, distances))
                all_ids = np.array([row.id for row in rows], dtype=np.int64)
                selected = np.lexsort((all_ids, all_distances))[:plan.limit]
                best_rows = [rows[index] for index in selected]
                best_distances = all_distances[selected]
                count += len(batch.rows)
            remaining()
            _consistent(total is not None and count == total, "Incomplete metadata stream")
            return pb.QueryResponse(rows=best_rows, projection=plan.projection,
                distances=best_distances.tolist(), matched_count=count, dimension=manifest.dimension,
                version=manifest.version, rpc_calls=rpc_calls, rpc_payload_bytes=payload_bytes)
        except grpc.RpcError as error:
            raise RemoteError(error.code(), error.details()) from error
        finally:
            if stream is not None:
                stream.cancel()
