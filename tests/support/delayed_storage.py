"""Test-only transparent SQLStorage proxy with bounded propagation-delay injection."""
from concurrent.futures import ThreadPoolExecutor
from threading import Event
from types import SimpleNamespace

import grpc
from google.protobuf.message_factory import GetMessageClass

from vecstor.rpc import sql_pb2 as pb
from vecstor.rpc.common import OPTIONS

READS={'BeginRead','EndRead','ScanMetadata','FetchVectors','FilterCandidates','FilterCandidateVectors',
       'ScanProjectedMetadata','ReadProjectedCandidates','StreamVectorSnapshot'}


def start_proxy(upstream,delay_ms=0):
    if type(delay_ms) not in (int,float) or not 0<=delay_ms<=1000:raise ValueError('Invalid proxy delay')
    channel=grpc.insecure_channel(upstream,options=OPTIONS)
    server=grpc.server(ThreadPoolExecutor(max_workers=8),options=OPTIONS)
    def left(context):
        if not context.is_active():context.abort(grpc.StatusCode.CANCELLED,'Proxy call cancelled')
        remaining=context.time_remaining()
        if remaining is not None and remaining<=0:context.abort(grpc.StatusCode.DEADLINE_EXCEEDED,'Proxy deadline exceeded')
        return remaining
    def pause(context,enabled):
        if not enabled or not delay_ms:return
        stop=Event();context.add_callback(stop.set)
        remaining=left(context)
        stop.wait(min(delay_ms/1000,remaining) if remaining is not None else delay_ms/1000)
        left(context)
    def unary_handler(callable,enabled):
        def forward(request,context):
            pause(context,enabled)
            call=callable.future(request,timeout=left(context));context.add_callback(call.cancel)
            try:
                response=call.result();pause(context,enabled);return response
            except grpc.FutureCancelledError:context.abort(grpc.StatusCode.CANCELLED,'Proxy downstream cancelled')
            except grpc.RpcError as error:context.abort(error.code(),error.details())
            finally:call.cancel()
        return forward
    def stream_handler(callable,enabled):
        def forward(request,context):
            pause(context,enabled)
            call=callable(request,timeout=left(context));context.add_callback(call.cancel)
            try:
                first=True
                for response in call:
                    if first:pause(context,enabled);first=False
                    left(context);yield response
            except grpc.RpcError as error:context.abort(error.code(),error.details())
            finally:call.cancel()
        return forward
    service=pb.DESCRIPTOR.services_by_name['SQLStorage'].full_name
    handlers={}
    for method in pb.DESCRIPTOR.services_by_name['SQLStorage'].methods:
        request_type=GetMessageClass(method.input_type);response_type=GetMessageClass(method.output_type)
        kind=('stream' if method.client_streaming else 'unary')+'_'+('stream' if method.server_streaming else 'unary')
        downstream=getattr(channel,kind)('/'+service+'/'+method.name,
            request_serializer=request_type.SerializeToString,response_deserializer=response_type.FromString)
        handler=(stream_handler if method.server_streaming else unary_handler)(downstream,method.name in READS)
        handlers[method.name]=getattr(grpc,kind+'_rpc_method_handler')(handler,
            request_deserializer=request_type.FromString,response_serializer=response_type.SerializeToString)
    server.add_generic_rpc_handlers((grpc.method_handlers_generic_handler(service,handlers),))
    port=server.add_insecure_port('127.0.0.1:0');server.start()
    def close():server.stop(0).wait();channel.close()
    return SimpleNamespace(address=f'127.0.0.1:{port}',close=close,delay_ms=delay_ms)
