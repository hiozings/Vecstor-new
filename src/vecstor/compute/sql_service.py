"""v2 SQL routing and remote vector queries pinned to an MVCC lease."""

import json
import math
import time

import grpc
import numpy as np

from vecstor.data import query_vector
from vecstor.query.statements import classify_statement
from vecstor.rpc import sql_pb2 as pb, sql_pb2_grpc as rpc
from vecstor.rpc.common import OPTIONS, RemoteError, VECTOR_PAYLOAD_BYTES, active
from vecstor.rpc.sql_codec import encode_result


class SQLComputeService(rpc.SQLComputeServicer):
    def __init__(self, address, timeout=15):
        self.channel = grpc.insecure_channel(address, options=OPTIONS)
        self.stub = rpc.SQLStorageStub(self.channel)
        self.timeout = timeout

    def close(self): self.channel.close()

    def _remaining(self, context, deadline):
        left = deadline - time.monotonic()
        if left <= 0: context.abort(grpc.StatusCode.DEADLINE_EXCEEDED, "SQL deadline exceeded")
        active(context)
        return left

    def Control(self, request, context):
        call = self.stub.Control.future(request, timeout=min(context.time_remaining() or self.timeout, self.timeout))
        context.add_callback(call.cancel)
        try: return call.result()
        except grpc.FutureCancelledError: context.abort(grpc.StatusCode.CANCELLED, "Control cancelled")
        except grpc.RpcError as error: context.abort(error.code(), error.details())

    def ExecuteSQL(self, request, context):
        deadline = time.monotonic() + min(context.time_remaining() or self.timeout, self.timeout)
        stream = None
        started = time.monotonic()
        try:
            plan = classify_statement(request.sql)
            if plan.kind in {"create_database", "use", "show_databases"}:
                if plan.kind == "show_databases":
                    value = self.stub.Control(pb.ControlRequest(action="list_databases"), timeout=self._remaining(context, deadline))
                    databases = json.loads(value.json_result)
                    output = {"columns":[{"name":"database","type":"VARCHAR"}], "rows":[[d["name"]] for d in databases]}
                else:
                    value = self.stub.Control(pb.ControlRequest(action="open_database", name=plan.database_name,
                                             create=plan.kind=="create_database"), timeout=self._remaining(context, deadline))
                    database = json.loads(value.json_result)
                    output = {"columns":[{"name":"database","type":"VARCHAR"}],"rows":[[database["name"]]]}
                    if plan.kind == "use":
                        output.update(selected_database_id=database["id"], selected_database_name=database["name"])
                yield from encode_result(output)
            elif plan.vector is not None:
                output = self._vector(request, plan.vector, context, deadline)
                output["elapsed_seconds"] = time.monotonic() - started
                for frame in encode_result(output):
                    self._remaining(context, deadline)
                    yield frame
            else:
                stream = self.stub.ExecuteSQL(request, timeout=self._remaining(context, deadline))
                context.add_callback(stream.cancel)
                for frame in stream:
                    self._remaining(context, deadline)
                    yield frame
        except grpc.RpcError as error:
            context.abort(error.code(), error.details())
        except RemoteError as error:
            context.abort(error.code, error.details)
        except ValueError as error:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, str(error))
        finally:
            if stream is not None: stream.cancel()

    def _vector(self, request, plan, context, deadline):
        def remaining(): return self._remaining(context, deadline)
        def check(condition, message):
            if not condition: raise RemoteError(grpc.StatusCode.DATA_LOSS, message)
        lease = self.stub.BeginRead(pb.BeginReadRequest(database_id=request.database_id, table=plan.table,
                                                       ttl_seconds=min(60, remaining())), timeout=remaining())
        stream = None
        try:
            check(bool(lease.token) and lease.database_id == request.database_id and lease.table == plan.table
                  and 0 < lease.dimension * 4 <= VECTOR_PAYLOAD_BYTES, "Invalid read lease")
            vector = query_vector(plan.query_vector, lease.dimension).astype(np.float64)
            size = min(256, VECTOR_PAYLOAD_BYTES // (lease.dimension * 4))
            stream = self.stub.ScanMetadata(pb.ScanRequest(database_id=request.database_id, token=lease.token,
                                             sql=request.sql, batch_rows=size), timeout=remaining())
            context.add_callback(stream.cancel)
            count, total, batches = 0, None, 0
            seen = set()
            best_rows, best_distances = [], np.empty(0, dtype=np.float64)
            for batch in stream:
                remaining(); batches += 1
                check(batch.token == lease.token and batch.revision == lease.revision, "Metadata lease mismatch")
                if total is None: total = batch.total_rows
                check(batch.total_rows == total and batch.offset == count and count+len(batch.rows) <= total
                      and len(batch.rows) <= size, "Metadata stream count mismatch")
                if not batch.rows:
                    check(total==0 and batches==1, "Unexpected empty metadata batch")
                    continue
                ids = [row.id for row in batch.rows]
                check(len(set(ids))==len(ids) and not seen.intersection(ids), "Duplicate remote ID")
                check(all(r.id >= 0 and r.category and math.isfinite(r.value) for r in batch.rows), "Invalid remote metadata")
                seen.update(ids)
                fetched = self.stub.FetchVectors(pb.FetchRequest(database_id=request.database_id, token=lease.token, ids=ids), timeout=remaining())
                check(fetched.token==lease.token and fetched.revision==lease.revision and fetched.dimension==lease.dimension
                      and list(fetched.ids)==ids and len(fetched.vectors_f32_le)==len(ids)*lease.dimension*4, "Invalid vector response")
                vectors = np.frombuffer(fetched.vectors_f32_le,dtype="<f4").reshape(len(ids),lease.dimension)
                check(np.isfinite(vectors).all(), "Nonfinite remote vector")
                delta = vectors.astype(np.float64)-vector
                distances = np.sum(delta*delta,axis=1,dtype=np.float64)
                entries = [{"id":r.id,"category":r.category,"timestamp":r.timestamp,"value":r.value,
                            "vector":v.tolist()} for r,v in zip(batch.rows,vectors)]
                merged = best_rows+entries
                all_distances = np.concatenate((best_distances,distances))
                selection = np.lexsort((np.array([r["id"] for r in merged],dtype=np.int64),all_distances))[:plan.limit]
                best_rows=[merged[i] for i in selection];best_distances=all_distances[selection]
                count+=len(ids)
            check(total is not None and total==count,"Incomplete metadata stream")
            types={"id":"BIGINT","category":"VARCHAR","timestamp":"BIGINT","value":"DOUBLE",
                   "vector":f"FLOAT[{lease.dimension}]","distance":"DOUBLE"}
            return {"columns":[{"name":out,"type":types[source]} for out,source in plan.projection],
                    "rows":[[float(distance) if source=="distance" else row[source] for _,source in plan.projection]
                            for row,distance in zip(best_rows,best_distances)],
                    "revision":lease.revision,"execution_path":"grpc_vector_exact","affected_rows":None}
        finally:
            if stream is not None: stream.cancel()
            try:
                self.stub.EndRead(pb.LeaseRequest(database_id=request.database_id,token=lease.token),timeout=1)
            except grpc.RpcError:
                pass  # Server TTL is the bounded fallback when the connection is lost.
