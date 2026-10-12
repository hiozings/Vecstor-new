"""Public query RPC; no storage path or Dataset is accepted."""

from concurrent.futures import ThreadPoolExecutor

import grpc

from vecstor.compute.engine import RemoteEngine
from vecstor.compute.sql_service import SQLComputeService
from vecstor.data import DataError
from vecstor.query.parser import QueryError
from vecstor.rpc import services_pb2_grpc as rpc
from vecstor.rpc import sql_pb2_grpc as sql_rpc
from vecstor.rpc.common import DEFAULT_BATCH_ROWS, OPTIONS, RemoteError


class ComputeService(rpc.ComputeServicer):
    def __init__(self, storage_address, timeout=10, batch_rows=DEFAULT_BATCH_ROWS):
        self.engine = RemoteEngine(storage_address, timeout, batch_rows)
        self.sql = SQLComputeService(storage_address, timeout)

    def close(self):
        self.engine.close()
        self.sql.close()

    def Query(self, request, context):
        timeout = context.time_remaining()
        if timeout is not None and timeout <= 0:
            context.abort(grpc.StatusCode.DEADLINE_EXCEEDED, "Query deadline exceeded")
        try:
            return self.engine.query_response(request.sql, list(request.vector), timeout, context.is_active)
        except RemoteError as error:
            context.abort(error.code, error.details)
        except (QueryError, DataError) as error:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, str(error))


def create_compute_server(storage_address, address, timeout=10, batch_rows=DEFAULT_BATCH_ROWS):
    backend = ComputeService(storage_address, timeout, batch_rows)
    server = grpc.server(ThreadPoolExecutor(max_workers=4), options=OPTIONS)
    rpc.add_ComputeServicer_to_server(backend, server)
    sql_rpc.add_SQLComputeServicer_to_server(backend.sql, server)
    port = server.add_insecure_port(address)
    if not port:
        backend.close()
        raise ValueError(f"Cannot bind compute address: {address}")
    return server, port, backend
