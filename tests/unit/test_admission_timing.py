import time
from types import SimpleNamespace
import grpc


def test_admission_timer_covers_delay_before_handler_execution():
    from vecstor.rpc.timing import AdmissionTiming
    seen=[]
    def execute(request,context):seen.append(context.vecstor_admission_ms);yield request
    original=grpc.unary_stream_rpc_method_handler(execute)
    wrapped=AdmissionTiming().intercept_service(lambda details:original,SimpleNamespace(method='/vecstor.v2.SQLCompute/ExecuteSQL'))
    time.sleep(.015)
    assert list(wrapped.unary_stream('row',SimpleNamespace()))==['row']
    assert seen[0]>=10
