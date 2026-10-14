"""v2 SQL routing and remote vector queries pinned to an MVCC lease."""

import json
import time

import grpc

from vecstor.query.statements import classify_statement, safe_name
from vecstor.query.search_options import SearchOptions
from vecstor.query.quality_policy import QualityCatalog
from vecstor.compute.index_manager import IndexManager
from vecstor.compute.vector_execution import execute_vector
from vecstor.rpc import sql_pb2 as pb, sql_pb2_grpc as rpc
from vecstor.rpc.common import OPTIONS, RemoteError, active
from vecstor.rpc.sql_codec import encode_result


class SQLComputeService(rpc.SQLComputeServicer):
    def __init__(self, address, timeout=15, index_memory_mib=256, index_max_entries=8, quality_document=None):
        self.quality_catalog=QualityCatalog.from_document(quality_document)
        self.channel = grpc.insecure_channel(address, options=OPTIONS)
        self.stub = rpc.SQLStorageStub(self.channel)
        self.timeout = timeout
        self.index_manager=IndexManager(self.stub,max_bytes=index_memory_mib*1024*1024,max_entries=index_max_entries)

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
                options=SearchOptions.from_proto(request.search_options)
                normalized=pb.SQLRequest.FromString(request.SerializeToString())
                normalized.sql=plan.sql
                output=execute_vector(self.stub,self.index_manager.registry,normalized,plan.vector,options,
                                      context,deadline,explain=plan.explain,quality_catalog=self.quality_catalog)
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

    def GetSearchCapabilities(self,request,context):
        return pb.ControlResponse(json_result=json.dumps({'ann':True,'algorithm':'cpu_ivfflat',
            'modes':['exact','approx'],'paths':['filter-first','search-first'],'plan_version':1,'gpu':False,
            'quality_profiles':self.quality_catalog.list()}))

    def BuildIndex(self,request,context):
        try:
            safe_name(request.table)
            output=self.index_manager.build(request,context)
            return pb.ControlResponse(json_result=json.dumps(output,allow_nan=False))
        except RemoteError as error: context.abort(error.code,error.details)
        except grpc.RpcError as error: context.abort(error.code(),error.details())
        except ValueError as error: context.abort(grpc.StatusCode.INVALID_ARGUMENT,str(error))

    def ListIndexes(self,request,context):
        if not request.database_id: context.abort(grpc.StatusCode.INVALID_ARGUMENT,'Select a database first')
        try:
            table=safe_name(request.table) if request.table else ''
            output=self.index_manager.registry.list(request.database_id,table)
            return pb.ControlResponse(json_result=json.dumps(output,allow_nan=False))
        except ValueError as error: context.abort(grpc.StatusCode.INVALID_ARGUMENT,str(error))

    def ClearIndexes(self,request,context):
        if not request.database_id: context.abort(grpc.StatusCode.INVALID_ARGUMENT,'Select a database first')
        try:
            table=safe_name(request.table) if request.table else ''
            output=self.index_manager.registry.clear(request.database_id,table)
            return pb.ControlResponse(json_result=json.dumps(output))
        except ValueError as error: context.abort(grpc.StatusCode.INVALID_ARGUMENT,str(error))
