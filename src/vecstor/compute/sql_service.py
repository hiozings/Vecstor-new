"""v2 SQL routing and remote vector queries pinned to an MVCC lease."""

import json
import time
import uuid

import grpc

from vecstor.query.statements import classify_statement, safe_name
from vecstor.query.search_options import SearchOptions
from vecstor.query.execution_settings import resolve_execution_options
from vecstor.query.quality_policy import QualityCatalog
from vecstor.compute.index_manager import IndexManager
from vecstor.index.gpu_worker import GpuDataError
from vecstor.index.artifact import ArtifactError
from vecstor.compute.vector_execution import execute_vector
from vecstor.rpc import sql_pb2 as pb, sql_pb2_grpc as rpc
from vecstor.rpc.common import OPTIONS, RemoteError, active
from vecstor.rpc.sql_codec import encode_result


class SQLComputeService(rpc.SQLComputeServicer):
    def __init__(self, address, timeout=15, index_memory_mib=256, index_max_entries=8, quality_document=None,gpu_options=None,fused_candidates=False,index_source='local'):
        if index_source not in ('local','published'): raise ValueError('Invalid index source')
        self.index_source = index_source
        self.server_instance_id=uuid.uuid4().hex
        self.quality_catalog=QualityCatalog.from_document(quality_document)
        self.channel = grpc.insecure_channel(address, options=OPTIONS)
        self.stub = rpc.SQLStorageStub(self.channel)
        if index_source == 'published':
            try:
                caps = json.loads(self.stub.Control(pb.ControlRequest(action='capabilities'),timeout=min(timeout,5)).json_result)
                if caps.get('index_publication_version') != 1: raise ValueError('Storage does not support published indexes')
            except Exception:
                self.channel.close(); raise
        self.timeout = timeout
        self.fused_candidates=fused_candidates
        if fused_candidates:
            try:
                capabilities=json.loads(self.stub.Control(pb.ControlRequest(action='capabilities'),timeout=min(timeout,5)).json_result)
                if not isinstance(capabilities,dict) or capabilities.get('fused_candidate_vectors') is not True:raise ValueError('Missing fused capability')
            except (grpc.RpcError,ValueError,TypeError) as error:
                self.channel.close();raise ValueError('Storage does not support fused candidate reads') from error
        self.index_manager=IndexManager(self.stub,max_bytes=index_memory_mib*1024*1024,max_entries=index_max_entries,gpu_options=gpu_options)
        from vecstor.compute.index_publication import IndexPublicationManager
        manager = self.index_manager
        self.publication = IndexPublicationManager(self.stub, manager.registry, manager.build_from_lease,
                                                   manager.build_slot, manager.gpu_registry, manager.gpu_worker)

    def close(self):
        self.index_manager.close();self.channel.close()

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
                options=resolve_execution_options(SearchOptions.from_proto(request.search_options),plan.settings)
                if options.candidate_data_path=='fused' and not self.fused_candidates:
                    raise RemoteError(grpc.StatusCode.FAILED_PRECONDITION,'Fused candidate reads are not enabled')
                use_fused=self.fused_candidates and options.candidate_data_path!='separate'
                normalized=pb.SQLRequest.FromString(request.SerializeToString())
                normalized.sql=plan.sql
                output=execute_vector(self.stub,self.index_manager.registry,normalized,plan.vector,options,
                                      context,deadline,explain=plan.explain,quality_catalog=self.quality_catalog,gpu_registry=self.index_manager.gpu_registry,fused_candidates=use_fused,index_source=self.index_source)
                output['execution_plan']['server_instance_id']=self.server_instance_id
                output['execution_plan']['server_admission_ms']=getattr(context,'vecstor_admission_ms',None)
                if plan.explain:output['rows']=[[json.dumps(output['execution_plan'],ensure_ascii=False,allow_nan=False)]]
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
        except GpuDataError as error:context.abort(grpc.StatusCode.DATA_LOSS,str(error))
        except ArtifactError as error:context.abort(getattr(grpc.StatusCode,error.code),str(error))
        except TimeoutError as error:context.abort(grpc.StatusCode.DEADLINE_EXCEEDED,str(error))
        except InterruptedError as error:context.abort(grpc.StatusCode.CANCELLED,str(error))
        except ValueError as error:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, str(error))
        finally:
            if stream is not None: stream.cancel()

    def GetSearchCapabilities(self,request,context):
        return pb.ControlResponse(json_result=json.dumps({'server_instance_id':self.server_instance_id,'fused_candidates':self.fused_candidates,'candidate_data_paths':['separate','fused'] if self.fused_candidates else ['separate'],'ann':True,'algorithm':'cpu_ivfflat',
            'modes':['exact','approx'],'paths':['filter-first','search-first'],'plan_version':1,'gpu':True,'gpu_paths':['search-first'],'gpu_max_search_k':2048,'gpu_max_nprobe':2048,
            'execution_settings_version':1,'index_devices_version':1,'index_publication_version':1,
            'index_source':getattr(self,'index_source','local'),'quality_profiles':self.quality_catalog.list()}))

    def _index_error(self, context, error):
        from vecstor.index.artifact import ArtifactError
        if isinstance(error, RemoteError): context.abort(error.code, error.details)
        if isinstance(error, grpc.RpcError): context.abort(error.code(), error.details())
        if isinstance(error, ArtifactError): context.abort(getattr(grpc.StatusCode, error.code), str(error))
        if isinstance(error, InterruptedError): context.abort(grpc.StatusCode.CANCELLED, str(error))
        if isinstance(error, TimeoutError): context.abort(grpc.StatusCode.DEADLINE_EXCEEDED, str(error))
        context.abort(grpc.StatusCode.INVALID_ARGUMENT, str(error))

    def PublishIndex(self, request, context):
        from vecstor.rpc.index_codec import record_to_proto
        try:
            record, stats = self.publication.publish(request, context)
            return pb.PublishIndexResponse(publication=record_to_proto(record), statistics_json=json.dumps(stats))
        except (ValueError, OSError, grpc.RpcError, RemoteError) as error: self._index_error(context, error)

    def LoadIndex(self, request, context):
        from vecstor.rpc.index_codec import descriptor_to_proto
        try:
            output = self.publication.load(request, context)
            return pb.LoadIndexResponse(descriptor=descriptor_to_proto(output['descriptor']),
                publication_generation=output['publication_generation'], device=output['device'], reused=output['reused'],
                statistics_json=json.dumps(output['statistics']))
        except (ValueError, OSError, grpc.RpcError, RemoteError) as error: self._index_error(context, error)

    def InspectPublishedIndex(self, request, context):
        from vecstor.rpc.index_codec import descriptor_to_proto
        try:
            output = self.publication.inspect(request, context)
            return pb.InspectPublishedIndexResponse(descriptor=descriptor_to_proto(output.descriptor) if output.descriptor else None,
                generation=output.generation, cpu_ready=output.cpu_ready, gpu_ready=output.gpu_ready,
                state=output.state, index_source=getattr(self,'index_source','local'))
        except (ValueError, OSError, grpc.RpcError, RemoteError) as error: self._index_error(context, error)

    def _index_proxy(self, name, request, context):
        call = getattr(self.stub, name).future(request, timeout=min(60, context.time_remaining() or 15))
        context.add_callback(call.cancel)
        try: return call.result()
        except grpc.FutureCancelledError: context.abort(grpc.StatusCode.CANCELLED, 'Index management cancelled')
        except grpc.RpcError as error: context.abort(error.code(), error.details())

    def GetIndexPublication(self, request, context): return self._index_proxy('GetIndexPublication', request, context)
    def ListPublishedIndexes(self, request, context): return self._index_proxy('ListPublishedIndexes', request, context)
    def PruneIndexArtifacts(self, request, context): return self._index_proxy('PruneIndexArtifacts', request, context)

    def BuildIndex(self,request,context):
        if self.index_source == 'published':
            context.abort(grpc.StatusCode.FAILED_PRECONDITION, 'Use .index publish and .index load in published mode')
        try:
            safe_name(request.table)
            output=self.index_manager.build(request,context)
            return pb.ControlResponse(json_result=json.dumps(output,allow_nan=False))
        except RemoteError as error: context.abort(error.code,error.details)
        except grpc.RpcError as error: context.abort(error.code(),error.details())
        except GpuDataError as error:context.abort(grpc.StatusCode.DATA_LOSS,str(error))
        except ValueError as error: context.abort(grpc.StatusCode.INVALID_ARGUMENT,str(error))

    def ListIndexes(self,request,context):
        if not request.database_id: context.abort(grpc.StatusCode.INVALID_ARGUMENT,'Select a database first')
        try:
            table=safe_name(request.table) if request.table else ''
            output=[dict(v,device='cpu') for v in self.index_manager.registry.list(request.database_id,table)]+self.index_manager.gpu_registry.list(request.database_id,table)
            return pb.ControlResponse(json_result=json.dumps(output,allow_nan=False))
        except GpuDataError as error:context.abort(grpc.StatusCode.DATA_LOSS,str(error))
        except ValueError as error: context.abort(grpc.StatusCode.INVALID_ARGUMENT,str(error))

    def ClearIndexes(self,request,context):
        if not request.database_id: context.abort(grpc.StatusCode.INVALID_ARGUMENT,'Select a database first')
        try:
            table=safe_name(request.table) if request.table else ''
            if request.device not in ('','cpu','gpu'):raise ValueError('Invalid device')
            output={'removed':0,'busy':0}
            registries=([self.index_manager.registry] if request.device=='cpu' else [self.index_manager.gpu_registry] if request.device=='gpu' else [self.index_manager.registry,self.index_manager.gpu_registry])
            for registry in registries:
                result=registry.clear(request.database_id,table)
                for key in output:output[key]+=result[key]
            if request.device:output['device']=request.device
            return pb.ControlResponse(json_result=json.dumps(output))
        except GpuDataError as error:context.abort(grpc.StatusCode.DATA_LOSS,str(error))
        except ValueError as error: context.abort(grpc.StatusCode.INVALID_ARGUMENT,str(error))
