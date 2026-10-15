"""Per-terminal database selection; never opens storage files."""

import json
import uuid
import time
from dataclasses import replace

import grpc

from vecstor.query.statements import classify_statement, safe_name
from vecstor.query.search_options import SearchOptions
from vecstor.query.execution_settings import resolve_execution_options
from vecstor.rpc.services_pb2 import Empty
from vecstor.rpc import sql_pb2 as pb, sql_pb2_grpc as rpc
from vecstor.rpc.common import OPTIONS, RemoteError, positive_timeout
from vecstor.rpc.sql_codec import decode_frame


class SessionClient:
    def __init__(self, address, timeout=15, cache_query_capabilities=True):
        self.address, self.timeout = address, positive_timeout(timeout)
        self.database_id = self.database_name = ""
        self.last_operation_id = ""
        self.last_was_write = False
        self.search_options = SearchOptions()
        self.last_search_mode = None
        self._quality_supported=None
        self.cache_query_capabilities=cache_query_capabilities
        self._query_caps=None;self._caps_endpoint=None
        self._control_rpc_calls=0;self._control_payload_bytes=0
        self.last_query_stats=None
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
        self._control_rpc_calls+=1;self._control_payload_bytes+=request.ByteSize()
        call = method.future(request, timeout=self.timeout if timeout is None else timeout)
        try:
            response=call.result();self._control_payload_bytes+=response.ByteSize()
            return json.loads(response.json_result)
        except grpc.RpcError as error:
            raise RemoteError(error.code(), error.details()) from error
        finally:
            call.cancel()

    def get_search_capabilities(self):
        return self._typed_control(self.stub.GetSearchCapabilities, Empty())

    def _query_capabilities(self):
        endpoint=(self.address,id(self.stub))
        if self.cache_query_capabilities and self._caps_endpoint==endpoint and self._query_caps is not None:
            return self._query_caps
        caps=self.get_search_capabilities()
        if not isinstance(caps,dict):raise ValueError('Invalid compute capabilities')
        if self.cache_query_capabilities and isinstance(caps.get('server_instance_id'),str) and caps['server_instance_id']:
            self._query_caps=caps;self._caps_endpoint=endpoint
        return caps

    def _invalidate_query_capabilities(self):
        self._query_caps=None;self._caps_endpoint=None;self._quality_supported=None

    def list_quality_profiles(self):
        caps=self.get_search_capabilities()
        profiles=caps.get('quality_profiles') if isinstance(caps,dict) else None
        if not isinstance(profiles,list):raise ValueError('Compute service does not support quality profiles')
        self._quality_supported=True
        return [p for p in profiles if p.get('database_id')==self.database_id]

    def set_search(self, name, value):
        if name=='profile':name='quality_profile_id'
        if name=='quality_profile_id':
            if value=='off':value=None
            elif value is not None and not any(p.get('profile_id')==value for p in self.list_quality_profiles()):
                raise ValueError('Unknown quality profile for this database')
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

    def build_index(self, table, nlist=None, timeout=45, seed=20261013, device='cpu', gpu_id=0):
        timeout = positive_timeout(timeout)
        if timeout > 60:
            raise ValueError('Index build timeout must be at most 60 seconds')
        if nlist is not None and (type(nlist) is not int or not 1 <= nlist < 2**32):
            raise ValueError('nlist must be a positive uint32')
        if type(seed) is not int or not 0 <= seed < 2**64:
            raise ValueError('seed must be a uint64')
        if device not in ('cpu','gpu') or type(gpu_id) is not int or gpu_id!=0:raise ValueError('Invalid index device')
        if device=='gpu' and self.get_search_capabilities().get('index_devices_version',0)<1:
            raise ValueError('Compute service does not support GPU index device controls')
        request = pb.BuildIndexRequest(database_id=self.database_id, table=safe_name(table), seed=seed, device=device, gpu_id=gpu_id)
        if nlist is not None:
            request.nlist = nlist
        result=self._typed_control(self.stub.BuildIndex, request, timeout)
        if device=='gpu' and (result.get('device')!='gpu' or result.get('gpu_id')!=gpu_id or result.get('state')!='ready'):
            raise ValueError('Compute service did not acknowledge GPU index device')
        return result

    def list_indexes(self, table=''):
        request = pb.IndexRequest(database_id=self.database_id, table=safe_name(table) if table else '')
        return self._typed_control(self.stub.ListIndexes, request)

    def clear_indexes(self, table='', device=''):
        if device not in ('','cpu','gpu'):raise ValueError('Invalid index device')
        if device and self.get_search_capabilities().get('index_devices_version',0)<1:
            raise ValueError('Compute service does not support index device controls')
        request = pb.IndexRequest(database_id=self.database_id, table=safe_name(table) if table else '',device=device)
        result=self._typed_control(self.stub.ClearIndexes, request)
        if device and result.get('device')!=device:raise ValueError('Compute service did not acknowledge clear device')
        return result

    def execute_stream(self,sql,operation_id=None):
        preflight_started=time.perf_counter();control_before=self._control_rpc_calls;bytes_before=self._control_payload_bytes
        self.last_was_write=False
        self.last_search_mode = None
        plan=classify_statement(sql)
        self.last_was_write=plan.is_write
        self.last_operation_id=operation_id or str(uuid.uuid4())
        request=pb.SQLRequest(database_id=self.database_id,sql=sql,operation_id=self.last_operation_id)
        effective=resolve_execution_options(self.search_options,plan.settings) if plan.kind=='vector' else self.search_options
        needs_device_ack=plan.kind=='vector' and (bool(plan.settings) or effective.device=='gpu')
        needs_data_ack=plan.kind=='vector' and effective.candidate_data_path!='default'
        if plan.kind == 'vector':
            caps=self._query_capabilities() if needs_device_ack or effective.quality_profile_id or effective.candidate_data_path!='default' else None
            if needs_device_ack and (type(caps.get('execution_settings_version')) is not int or caps['execution_settings_version']<1):
                self._invalidate_query_capabilities()
                raise ValueError('Compute service does not support SQL device settings')
            if effective.quality_profile_id and not isinstance(caps.get('quality_profiles'),list):
                self._invalidate_query_capabilities()
                raise ValueError('Compute service does not support quality profiles')
            if effective.candidate_data_path!='default' and effective.candidate_data_path not in caps.get('candidate_data_paths',[]):
                self._invalidate_query_capabilities();raise ValueError('Compute service does not support requested candidate data path')
            request.search_options.CopyFrom(effective.to_proto())
            self.last_search_mode = effective.mode
        client_stats={'preflight_ms':(time.perf_counter()-preflight_started)*1000,
                      'control_rpc_calls':self._control_rpc_calls-control_before,
                      'control_payload_bytes':self._control_payload_bytes-bytes_before,'execute_rpc_calls':1}
        def events():
            execution_started=time.perf_counter();payload_bytes=request.ByteSize()
            call=self.stub.ExecuteSQL(request,timeout=self.timeout)
            schema=complete=False
            width=0
            try:
                for frame in call:
                    payload_bytes+=frame.ByteSize()
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
                        if needs_device_ack:
                            execution=event.get('execution_plan') or {}
                            if execution.get('requested_device')!=effective.device or execution.get('actual_device') not in ('cpu','gpu','not_executed'):
                                raise ValueError('Compute service did not acknowledge requested device')
                            if effective.device=='gpu' and effective.device_fallback=='error' and execution.get('actual_device')=='cpu':
                                raise ValueError('Compute service silently changed requested GPU device')
                        if needs_data_ack:
                            execution=event.get('execution_plan') or {}
                            accepted_paths=('fused','mixed') if effective.candidate_data_path=='fused' else ('separate',)
                            if execution.get('requested_candidate_data_path')!=effective.candidate_data_path or (execution.get('actual_mode')=='approx' and execution.get('candidate_data_path') not in accepted_paths):
                                raise ValueError('Compute service did not acknowledge candidate data path')
                        profile_id=request.search_options.quality_profile_id
                        if profile_id:
                            execution=event.get('execution_plan') or {}
                            quality=execution.get('quality_profile') or {}
                            if quality.get('requested_id')!=profile_id or (execution.get('actual_mode')=='approx' and quality.get('state')!='applied'):
                                raise ValueError('Compute service did not acknowledge/apply the quality profile')
                        execution=event.get('execution_plan')
                        if execution is not None:
                            if self._query_caps and execution.get('server_instance_id')!=self._query_caps.get('server_instance_id'):
                                self._invalidate_query_capabilities()
                            client_stats.update(execute_rpc_ms=(time.perf_counter()-execution_started)*1000,execute_payload_bytes=payload_bytes)
                            execution['client']=dict(client_stats)
                        self.last_query_stats=dict(client_stats)
                        complete=True
                        if event.get("selected_database_id"):
                            self.database_id=event["selected_database_id"]
                            self.database_name=event["selected_database_name"]
                    yield event
                if not schema or not complete: raise ValueError("Incomplete result stream")
            except grpc.RpcError as error:
                self._invalidate_query_capabilities();raise RemoteError(error.code(),error.details()) from error
            except ValueError:
                self._invalidate_query_capabilities();raise
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
