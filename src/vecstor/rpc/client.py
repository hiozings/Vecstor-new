"""Typed public query client and JSON-compatible result conversion."""

import grpc

from vecstor.data import float32_array, DataError
from vecstor.rpc import services_pb2 as pb, services_pb2_grpc as rpc
from vecstor.rpc.common import OPTIONS, RemoteError, positive_timeout


def response_dict(response):
    return {
        "rows": [{name: getattr(row, name) for name in response.projection} for row in response.rows],
        "distances": list(response.distances), "matched_count": response.matched_count,
        "metric": "squared_l2", "mode": "exact", "snapshot_version": response.version,
        "plan": {"path": "grpc_filter_then_exact", "backend": "numpy_float64",
                 "dimension": response.dimension, "evaluated_vectors": response.matched_count,
                 "rpc_calls": response.rpc_calls, "rpc_payload_bytes": response.rpc_payload_bytes,
                 "byte_accounting": "storage_protobuf_request_and_response_payloads"},
    }


def query(address, sql, vector, timeout=15):
    timeout = positive_timeout(timeout)
    validated = float32_array(vector, "query vector")
    if validated.ndim != 1:
        raise DataError("query vector must be one-dimensional")
    with grpc.insecure_channel(address, options=OPTIONS) as channel:
        try:
            response = rpc.ComputeStub(channel).Query(pb.QueryRequest(sql=sql, vector=validated.tolist()), timeout=timeout)
        except grpc.RpcError as error:
            raise RemoteError(error.code(), error.details()) from error
    return response_dict(response)
