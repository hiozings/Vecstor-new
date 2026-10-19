"""Real-device tests skip explicitly on CPU hosts; acceptance can require GPU."""
from functools import lru_cache
import os
import subprocess
import sys
import pytest


@pytest.fixture
def cpu_artifact():
    import numpy as np
    from vecstor.index.artifact import SnapshotHasher, encode_artifact
    from vecstor.index.faiss_backend import FaissIndex
    from vecstor.index.types import BuildConfig, IndexKey
    ids = np.arange(2048, dtype=np.int64) + 2**53
    vectors = np.random.default_rng(31).random((2048, 2), dtype=np.float32)
    config = BuildConfig(nlist=1)
    hasher = SnapshotHasher(len(ids), 2)
    hasher.add(ids, vectors)
    key = IndexKey('test-database', 'test-table', 1, 'test-schema', 2,
                   build_fingerprint=config.fingerprint())
    return encode_artifact(FaissIndex.build(ids, vectors, config), key, config, hasher.finish())

GPU_FILES={'test_gpu_batch_probe.py','test_gpu_sessions.py','test_gpu_worker.py','test_gpu_execution.py','test_gpu_shell.py','test_gpu_quality_rpc.py','test_gpu_calibration.py','test_gpu_acceptance.py','test_gpu_benchmark.py','test_p4_gpu_restore.py','test_gpu_cached_reads.py'}


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
