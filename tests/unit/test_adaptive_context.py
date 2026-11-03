import pytest
from vecstor.compute.adaptive_context import ActiveVectorRequests
from vecstor.query.search_options import SearchOptions
from vecstor.query.execution_settings import parse_execution_settings, resolve_execution_options


def test_auto_options_do_not_change_default_or_allow_gpu():
    assert SearchOptions().path=='filter-first'
    assert SearchOptions(path='auto').validate().mode=='exact'
    assert SearchOptions(mode='approx',path='auto',candidate_data_path='fused').validate()
    with pytest.raises(ValueError,match='CPU'):SearchOptions(mode='approx',path='auto',device='gpu').validate()


def test_active_counter_snapshot_is_detached():
    counter=ActiveVectorRequests();counter.enter()
    before=counter.snapshot('hw','local')
    counter.enter();assert counter.snapshot('hw','local').concurrency_bucket==4
    counter.leave();counter.leave()
    assert counter.value==0 and before.active_vector_requests==1


def test_policy_loading_before_landlock_imports_only_stdlib():
    import subprocess,sys
    run=subprocess.run([sys.executable,'-c',
        "import sys; from vecstor.query.adaptive_policy import AdaptivePolicy; "
        "assert not ({'numpy','faiss','grpc','duckdb'} & set(sys.modules))"],capture_output=True,text=True)
    assert run.returncode==0,run.stderr
