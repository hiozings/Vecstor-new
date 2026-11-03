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

    def _data_cache_control(self,method):
        self.last_was_write=False
        if not self.database_id:raise ValueError('Select a database first')
        if self.get_search_capabilities().get('data_cache_version')!=1:
            raise ValueError('Compute service does not support data cache controls')
        result=self._typed_control(method,pb.DataCacheRequest(database_id=self.database_id))
        if not isinstance(result,dict) or result.get('data_cache_version')!=1:
            raise ValueError('Invalid data cache control response')
        return result

    def data_cache_stats(self):return self._data_cache_control(self.stub.GetDataCacheStats)
    def clear_data_cache(self):return self._data_cache_control(self.stub.ClearDataCache)

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

    def _index_rpc(self,name,request,timeout=45):
        timeout=positive_timeout(timeout)
        if timeout>60:raise ValueError('Index management timeout must be at most 60 seconds')
        if self.get_search_capabilities().get('index_publication_version')!=1:
            raise ValueError('Compute service does not support index publication')
        call=getattr(self.stub,name).future(request,timeout=timeout)
        try:return call.result()
        except grpc.RpcError as error:raise RemoteError(error.code(),error.details()) from error
        finally:call.cancel()

    def publish_index(self,table,nlist=None,timeout=45,seed=20261013,operation_id=None,expected_generation=None):
        from vecstor.index.types import BuildConfig
        from vecstor.rpc.index_codec import record_from_proto
        from dataclasses import asdict
        BuildConfig(nlist=nlist,seed=seed).validate()
        operation_id=operation_id or str(uuid.uuid4())
        self.last_operation_id=operation_id
        request=pb.PublishIndexRequest(database_id=self.database_id,table=safe_name(table),seed=seed,operation_id=operation_id)
        if nlist is not None:request.nlist=nlist
        if expected_generation is not None:
            if type(expected_generation) is not int or not 0<=expected_generation<2**63:raise ValueError('Invalid expected generation')
            request.expected_generation=expected_generation
        try:response=self._index_rpc('PublishIndex',request,timeout)
        except RemoteError as error:
            raise RemoteError(error.code,error.details+f'; operation_id={operation_id}; use .index publication-status {operation_id}') from error
        record=record_from_proto(response.publication)
        if record is None or record.operation_id!=operation_id or record.state!='COMMITTED':raise ValueError('Publication was not committed')
        return dict(asdict(record),statistics=json.loads(response.statistics_json))

    def load_index(self,table,device='cpu',timeout=45,expected_artifact_id=None,gpu_id=0):
        from vecstor.rpc.index_codec import descriptor_from_proto
        if device not in ('cpu','gpu') or type(gpu_id) is not int or gpu_id!=0:raise ValueError('Invalid index device')
        response=self._index_rpc('LoadIndex',pb.LoadIndexRequest(database_id=self.database_id,table=safe_name(table),
            device=device,gpu_id=gpu_id,expected_artifact_id=expected_artifact_id or ''),timeout)
        descriptor=descriptor_from_proto(response.descriptor)
        if response.device!=device or (expected_artifact_id and descriptor.artifact_id!=expected_artifact_id):raise ValueError('Index load confirmation mismatch')
        return dict(state='ready',device=device,descriptor=descriptor.to_dict(),generation=response.publication_generation,
                    reused=response.reused,statistics=json.loads(response.statistics_json))

    def inspect_published_index(self,table):
        from vecstor.rpc.index_codec import descriptor_from_proto
        response=self._index_rpc('InspectPublishedIndex',pb.InspectPublishedIndexRequest(database_id=self.database_id,table=safe_name(table)))
        return dict(descriptor=descriptor_from_proto(response.descriptor).to_dict() if response.HasField('descriptor') else None,
            generation=response.generation,cpu_ready=response.cpu_ready,gpu_ready=response.gpu_ready,state=response.state,index_source=response.index_source)

    def list_published_indexes(self,table='',page_size=128,cursor=''):
        from vecstor.rpc.index_codec import descriptor_from_proto
        if type(page_size) is not int or not 1<=page_size<=128:raise ValueError('Page size must be 1..128')
        response=self._index_rpc('ListPublishedIndexes',pb.ListPublishedIndexesRequest(database_id=self.database_id,
            table=safe_name(table) if table else '',page_size=page_size,cursor=cursor))
        return dict(indexes=[dict(descriptor=descriptor_from_proto(r.descriptor).to_dict(),state=r.state,
                                 generation=r.generation,matches_current=r.matches_current) for r in response.indexes],next_cursor=response.next_cursor)

    def index_publication_status(self,operation_id):
        from dataclasses import asdict
        from vecstor.rpc.index_codec import record_from_proto
        response=self._index_rpc('GetIndexPublication',pb.IndexPublicationRequest(database_id=self.database_id,operation_id=operation_id))
        record=record_from_proto(response)
        return asdict(record) if record is not None else dict(operation_id=operation_id,state='UNKNOWN')

    def prune_index_artifacts(self,keep_last=2,dry_run=True,operation_id=None,timeout=45):
        if type(keep_last) is not int or not 1<=keep_last<2**32:raise ValueError('keep_last must be a positive uint32')
        if type(dry_run) is not bool:raise ValueError('dry_run must be boolean')
        if not dry_run and not operation_id:raise ValueError('Prune execution requires an operation_id')
        if operation_id:self.last_operation_id=operation_id
        response=self._index_rpc('PruneIndexArtifacts',pb.PruneIndexArtifactsRequest(database_id=self.database_id,
            operation_id=operation_id or '',keep_last=keep_last,dry_run=dry_run),timeout)
        return dict(candidates=list(response.candidates),protected=list(response.protected),deleted=list(response.deleted),
            deleted_bytes=response.deleted_bytes,state=response.state,byte_accounting=response.byte_accounting)

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
        needs_adaptive_ack=plan.kind=='vector' and effective.path=='auto'
        needs_device_ack=plan.kind=='vector' and (bool(plan.settings) or effective.device=='gpu')
        needs_data_ack=plan.kind=='vector' and effective.candidate_data_path!='default'
        needs_cache_ack=plan.kind=='vector' and effective.data_cache!='default'
        needs_projection_ack=plan.kind=='vector' and effective.metadata_projection!='default'
        if plan.kind == 'vector':
            caps=self._query_capabilities() if needs_device_ack or effective.quality_profile_id or effective.candidate_data_path!='default' or needs_cache_ack or needs_projection_ack or needs_adaptive_ack else None
            if needs_adaptive_ack:
                from vecstor.rpc.adaptive_codec import require_adaptive_capabilities
                try:require_adaptive_capabilities(caps,effective)
                except ValueError:self._invalidate_query_capabilities();raise
            if needs_device_ack and (type(caps.get('execution_settings_version')) is not int or caps['execution_settings_version']<1):
                self._invalidate_query_capabilities()
                raise ValueError('Compute service does not support SQL device settings')
            if effective.quality_profile_id and not isinstance(caps.get('quality_profiles'),list):
                self._invalidate_query_capabilities()
                raise ValueError('Compute service does not support quality profiles')
            if effective.candidate_data_path!='default' and effective.candidate_data_path not in caps.get('candidate_data_paths',[]):
                self._invalidate_query_capabilities();raise ValueError('Compute service does not support requested candidate data path')
            if needs_cache_ack and caps.get('data_cache_version')!=1:
                self._invalidate_query_capabilities();raise ValueError('Compute service does not support data cache settings')
            if needs_projection_ack and caps.get('metadata_projection_version')!=1:
                self._invalidate_query_capabilities();raise ValueError('Compute service does not support metadata projection settings')
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
                        if needs_adaptive_ack:
                            from vecstor.rpc.adaptive_codec import validate_adaptive_plan
                            validate_adaptive_plan(event.get('execution_plan'),effective,caps)
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
                        if needs_cache_ack or needs_projection_ack:
                            execution=event.get('execution_plan') or {}
                            if execution.get('data_read_version')!=1:
                                raise ValueError('Compute service did not acknowledge data read settings')
                            if needs_cache_ack:
                                cache=execution.get('data_cache') or {}
                                if cache.get('requested')!=effective.data_cache or cache.get('effective')!=effective.data_cache:
                                    raise ValueError('Compute service did not acknowledge data cache settings')
                            if needs_projection_ack:
                                projection=execution.get('metadata_projection') or {}
                                if projection.get('requested')!=effective.metadata_projection or projection.get('effective')!=effective.metadata_projection:
                                    raise ValueError('Compute service did not acknowledge metadata projection settings')
                        profile_id=request.search_options.quality_profile_id
                        if profile_id:
                            execution=event.get('execution_plan') or {}
                            quality=execution.get('quality_profile') or {}
                            if quality.get('requested_id')!=profile_id or (execution.get('actual_mode')=='approx' and quality.get('state')!='applied'):
                                raise ValueError('Compute service did not acknowledge/apply the quality profile')
                        execution=event.get('execution_plan')
                        if plan.kind == 'vector':
                            from vecstor.rpc.index_codec import validate_publication_plan
                            if self._query_caps and self._query_caps.get('index_source') == 'published':
                                if not execution or execution.get('index_source') != 'published':
                                    raise ValueError('Compute service did not acknowledge published index source')
                            if execution is not None: validate_publication_plan(execution, event['revision'])
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
