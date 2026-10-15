"""Read-only measurement from RPC interception to handler start."""
import time
import grpc


class AdmissionTiming(grpc.ServerInterceptor):
    def intercept_service(self,continuation,details):
        received=time.perf_counter();handler=continuation(details)
        if handler is None or not details.method.endswith('/ExecuteSQL') or handler.unary_stream is None:return handler
        def execute(request,context):
            context.vecstor_admission_ms=(time.perf_counter()-received)*1000
            yield from handler.unary_stream(request,context)
        return grpc.unary_stream_rpc_method_handler(execute,request_deserializer=handler.request_deserializer,response_serializer=handler.response_serializer)
