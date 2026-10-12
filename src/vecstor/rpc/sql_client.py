"""Per-terminal database selection; never opens storage files."""

import json
import uuid

import grpc

from vecstor.query.statements import classify_statement
from vecstor.rpc import sql_pb2 as pb, sql_pb2_grpc as rpc
from vecstor.rpc.common import OPTIONS, RemoteError, positive_timeout
from vecstor.rpc.sql_codec import decode_frame


class SessionClient:
    def __init__(self, address, timeout=15):
        self.address, self.timeout = address, positive_timeout(timeout)
        self.database_id = self.database_name = ""
        self.last_operation_id = ""
        self.last_was_write = False
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

    def execute_stream(self,sql,operation_id=None):
        self.last_was_write=False
        plan=classify_statement(sql)
        self.last_was_write=plan.is_write
        self.last_operation_id=operation_id or str(uuid.uuid4())
        request=pb.SQLRequest(database_id=self.database_id,sql=sql,operation_id=self.last_operation_id)
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
