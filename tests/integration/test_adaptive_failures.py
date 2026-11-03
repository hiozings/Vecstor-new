import time
import grpc
import pytest

from test_adaptive_execution import adaptive, cached
from vecstor.compute.lease_io import LeaseIO
from vecstor.rpc.common import RemoteError


def test_failure_does_not_record_fast_success(adaptive,monkeypatch):
    def corrupt(*args,**kwargs):raise RemoteError(grpc.StatusCode.DATA_LOSS,'bad vector frame')
    monkeypatch.setattr(LeaseIO,'fetch',corrupt)
    with pytest.raises(RemoteError,match='bad vector frame'):adaptive.run()
    assert adaptive.observations.stats()['entries']==0


@pytest.mark.parametrize('code',[grpc.StatusCode.CANCELLED,grpc.StatusCode.DEADLINE_EXCEEDED])
def test_cancel_corrupt_rpc_and_expired_lease_preserve_errors(adaptive,monkeypatch,code):
    def fail(*args,**kwargs):raise RemoteError(code,'query interrupted')
    monkeypatch.setattr(LeaseIO,'filter',fail)
    monkeypatch.setattr(LeaseIO,'read_projected_candidates',fail)
    with pytest.raises(RemoteError) as error:adaptive.run()
    assert error.value.code==code and adaptive.observations.stats()['entries']==0


def test_budget_error_records_only_risk(adaptive,monkeypatch):
    import numpy as np
    from vecstor.index.faiss_backend import FaissIndex
    from vecstor.query.search_options import SearchOptions
    recorded=[];original=adaptive.observations.note_budget_failure
    def note(key,*args,**kwargs):recorded.append(key);return original(key,*args,**kwargs)
    monkeypatch.setattr(adaptive.observations,'note_budget_failure',note)
    monkeypatch.setattr(FaissIndex,'search',lambda *args,**kwargs:(np.empty(0),np.empty(0,dtype=np.int64)))
    with pytest.raises(RemoteError) as error:
        adaptive.run(SearchOptions(mode='approx',path='auto',fallback='error'))
    assert error.value.code==grpc.StatusCode.RESOURCE_EXHAUSTED
    assert len(recorded)==1 and adaptive.observations.stats()['entries']==1
    assert adaptive.observations.peek(recorded[0]).values=={'budget_failure':True}


def test_cancellation_during_feedback_does_not_leave_success_evidence(adaptive,monkeypatch):
    from vecstor.rpc.common import DeadlineContext
    class Context(DeadlineContext):
        cancelled=False
        def is_active(self):return not self.cancelled and super().is_active()
    context=Context(time.monotonic()+30)
    original=adaptive.observations.record
    def cancel_after_record(*args,**kwargs):
        original(*args,**kwargs);context.cancelled=True
    monkeypatch.setattr(adaptive.observations,'record',cancel_after_record)
    with pytest.raises(RemoteError) as error:adaptive.run(ctx=context)
    assert error.value.code==grpc.StatusCode.CANCELLED
    assert adaptive.observations.stats()['entries']==0
