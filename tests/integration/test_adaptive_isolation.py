"""The compute fingerprint must not initialize a storage database runtime."""
import os
from pathlib import Path
import subprocess
import sys


def test_deployment_fingerprint_after_landlock_avoids_duckdb_runtime(tmp_path):
    root=tmp_path/'storage';root.mkdir();(root/'source.npz').write_bytes(b'isolation probe')
    code='''
import resource,sys
resource.setrlimit(resource.RLIMIT_CORE,(0,0))
from vecstor.isolation import restrict_compute_filesystem
restrict_compute_filesystem(sys.argv[1],gpu=True)
from vecstor.compute.adaptive_context import deployment_fingerprint
assert deployment_fingerprint() is not None
assert 'duckdb' not in sys.modules
'''
    result=subprocess.run([sys.executable,'-c',code,str(root)],capture_output=True,text=True,timeout=30,
        env=dict(os.environ,OMP_NUM_THREADS='1',OPENBLAS_NUM_THREADS='1'))
    assert result.returncode==0,result.stderr
