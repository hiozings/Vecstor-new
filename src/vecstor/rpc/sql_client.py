"""Per-terminal database selection; never opens storage files."""

import json
import uuid
from dataclasses import replace

import grpc

from vecstor.query.statements import classify_statement, safe_name
from vecstor.query.search_options import SearchOptions
from vecstor.rpc.services_pb2 import Empty
from vecstor.rpc import sql_pb2 as pb, sql_pb2_grpc as rpc
from vecstor.rpc.common import OPTIONS, RemoteError, positive_timeout
from vecstor.rpc.sql_codec import decode_frame


class SessionClient:
    def __init__(self, address, timeout=15):
        self.address, self.timeout = address, positive_timeout(timeout)
        self.database_id = self.database_name = ""
        self.last_operation_id = ""
        self.last_was_write = False
        self.search_options = SearchOptions()
        self.last_search_mode = None
        self.channel = grpc.insecure_channel(address,options=OPTIONS)
        self.stub = rpc.SQLComputeStub(self.channel)

    def _control(self,action,**kwargs):
        call=self.stub.Control.future(pb.ControlRequest(action=action,database_id=self.database_id,**kwargs),timeout=self.timeout)
        try: return json.loads(call.result().json_result)
        except grpc.RpcError as error: raise RemoteError(error.code(),error.details()) from error
        finally: call.cancel()

    def list_databases(self): return self._control("list_databases")
    def list_datasets(self): return self._control("list_datasets")
    def open_database(self,name,create=False):
        value=self._control("open_database",name=name,create=create)
        self.database_id,self.database_name=value["id"],value["name"]
        return value
    def describe(self,table=None): return self._control("describe",table=table or "")
    def get_operation_status(self,operation_id): return self._control("status",operation_id=operation_id)
    def import_dataset(self,source,table,operation_id=None):
        self.last_was_write=True
        self.last_operation_id=operation_id or str(uuid.uuid4())
        return self._control("import",source=source,table=table,operation_id=self.last_operation_id)

    def _typed_control(self, method, request, timeout=None):
        call = method.future(request, timeout=self.timeout if timeout is None else timeout)
        try:
            return json.loads(call.result().json_result)
        except grpc.RpcError as error:
            raise RemoteError(error.code(), error.details()) from error
        finally:
            call.cancel()

    def get_search_capabilities(self):
        return self._typed_control(self.stub.GetSearchCapabilities, Empty())

    def set_search(self, name, value):
        name = name.replace('-', '_')
        if name not in self.search_options.to_dict():
            raise ValueError('Unknown search option')
        if name in ('nprobe', 'candidates', 'max_candidates', 'max_rounds') and isinstance(value, str):
            try:
                value = int(value)
            except ValueError as error:
                raise ValueError(f'{name} requires an integer') from error
        options = replace(self.search_options, **{name: value}).validate()
        if options.mode == 'approx' and self.search_options.mode != 'approx':
            capabilities = self.get_search_capabilities()
            if not isinstance(capabilities, dict) or capabilities.get('ann') is not True:
                raise ValueError('Compute service does not support ANN search')
        self.search_options = options
        return options.to_dict()

    def reset_search(self):
        self.search_options = SearchOptions()
        return self.search_options.to_dict()

    def build_index(self, table, nlist=None, timeout=45, seed=20261013):
        timeout = positive_timeout(timeout)
        if timeout > 60:
            raise ValueError('Index build timeout must be at most 60 seconds')
        if nlist is not None and (type(nlist) is not int or not 1 <= nlist < 2**32):
            raise ValueError('nlist must be a positive uint32')
        if type(seed) is not int or not 0 <= seed < 2**64:
            raise ValueError('seed must be a uint64')
        request = pb.BuildIndexRequest(database_id=self.database_id, table=safe_name(table), seed=seed)
        if nlist is not None:
            request.nlist = nlist
        return self._typed_control(self.stub.BuildIndex, request, timeout)

    def list_indexes(self, table=''):
        request = pb.IndexRequest(database_id=self.database_id, table=safe_name(table) if table else '')
        return self._typed_control(self.stub.ListIndexes, request)

    def clear_indexes(self, table=''):
        request = pb.IndexRequest(database_id=self.database_id, table=safe_name(table) if table else '')
        return self._typed_control(self.stub.ClearIndexes, request)

    def execute_stream(self,sql,operation_id=None):
        self.last_was_write=False
        self.last_search_mode = None
        plan=classify_statement(sql)
        self.last_was_write=plan.is_write
        self.last_operation_id=operation_id or str(uuid.uuid4())
        request=pb.SQLRequest(database_id=self.database_id,sql=sql,operation_id=self.last_operation_id)
        if plan.kind == 'vector':
            request.search_options.CopyFrom(self.search_options.to_proto())
            self.last_search_mode = self.search_options.mode
        def events():
            call=self.stub.ExecuteSQL(request,timeout=self.timeout)
            schema=complete=False
            width=0
            try:
                for frame in call:
                    event=decode_frame(frame)
                    if complete: raise ValueError("Unexpected result after completion")
                    if event["kind"]=="schema":
                        if schema: raise ValueError("Duplicate result schema")
                        schema=True
                        width=len(event["columns"])
                    elif not schema: raise ValueError("Result received before schema")
                    if event["kind"]=="rows" and any(len(row)!=width for row in event["rows"]):
                        raise ValueError("Result row width differs from schema")
                    if event["kind"]=="complete":
                        complete=True
                        if event.get("selected_database_id"):
                            self.database_id=event["selected_database_id"]
                            self.database_name=event["selected_database_name"]
                    yield event
                if not schema or not complete: raise ValueError("Incomplete result stream")
            except grpc.RpcError as error: raise RemoteError(error.code(),error.details()) from error
            finally: call.cancel()
        return events()

    def execute(self,sql,operation_id=None):
        output={"columns":[],"rows":[],"affected_rows":None}
        stream=self.execute_stream(sql,operation_id)
        try:
            for event in stream:
                if event["kind"]=="schema": output["columns"]=event["columns"]
                elif event["kind"]=="rows": output["rows"].extend(event["rows"])
                else: output.update({key:value for key,value in event.items() if key!="kind"})
        finally: stream.close()
        return output

    def close(self): self.channel.close()
    def __enter__(self): return self
    def __exit__(self,*_): self.close()
