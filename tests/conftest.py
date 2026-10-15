"""Real-device tests skip explicitly on CPU hosts; acceptance can require GPU."""
from functools import lru_cache
import os
import subprocess
import sys
import pytest

GPU_FILES={'test_gpu_worker.py','test_gpu_execution.py','test_gpu_shell.py','test_gpu_quality_rpc.py','test_gpu_calibration.py','test_gpu_acceptance.py','test_gpu_benchmark.py'}


@lru_cache(maxsize=1)
def gpu_available():
    try:
        run=subprocess.run([sys.executable,'-c','import faiss; raise SystemExit(0 if faiss.get_num_gpus()>0 else 2)'],
            stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL,timeout=15)
        return run.returncode==0
    except subprocess.TimeoutExpired:return False


@pytest.fixture(autouse=True)
def real_gpu_requirement(request):
    if request.path.name not in GPU_FILES or request.node.name=='test_queued_expired_request_never_creates_replica':return
    if not gpu_available():
        if os.environ.get('VECSTOR_REQUIRE_GPU')=='1':pytest.fail('GPU acceptance requires a real accessible GPU')
        pytest.skip('No accessible GPU; this does not validate GPU behavior')
