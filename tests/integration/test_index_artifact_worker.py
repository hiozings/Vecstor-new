import time

import pytest

from vecstor.index.artifact_worker import verify_in_worker


def test_verifier_reads_actual_artifact(tmp_path, cpu_artifact):
    descriptor, payload = cpu_artifact
    path = tmp_path/'index.faiss'; path.write_bytes(payload)
    result = verify_in_worker(path, descriptor, time.monotonic()+10, lambda:False)
    assert result['snapshot_sha256'] == descriptor.snapshot_sha256
    assert result['row_count'] == 2048
    assert result['worker_peak_rss_bytes'] > 0


def test_verifier_cancellation_reaps_process(tmp_path, cpu_artifact, monkeypatch):
    import subprocess
    import sys
    from vecstor.index import artifact_worker
    descriptor, payload = cpu_artifact
    path = tmp_path/'index.faiss'; path.write_bytes(payload)
    real = subprocess.Popen
    children = []
    def sleeping(*args, **kwargs):
        child = real([sys.executable, '-c', 'import time; time.sleep(30)'], **kwargs)
        children.append(child)
        return child
    monkeypatch.setattr(artifact_worker.subprocess, 'Popen', sleeping)
    with pytest.raises(TimeoutError):
        verify_in_worker(path, descriptor, time.monotonic()+.2, lambda:False)
    assert len(children) == 1 and children[0].poll() is not None
    assert all(pipe.closed for pipe in (children[0].stdin, children[0].stdout, children[0].stderr))
