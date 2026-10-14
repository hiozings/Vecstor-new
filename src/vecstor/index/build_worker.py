"""Cancellable training in a fresh interpreter using anonymous pipes only."""
from dataclasses import asdict
from io import BytesIO
import json
import os
import resource
import subprocess
import sys
import threading
import time

import numpy as np

from .types import BuildConfig


def _check(deadline, cancelled):
    if cancelled():
        raise InterruptedError('index build cancelled')
    if time.monotonic() >= deadline:
        raise TimeoutError('index build deadline exceeded')


def build_in_worker(ids, vectors, config, deadline, cancelled=lambda: False):
    from .faiss_backend import FaissIndex
    _check(deadline, cancelled)
    payload = BytesIO()
    np.savez(payload, ids=ids, vectors=vectors, config=np.array(json.dumps(asdict(config))))
    _check(deadline, cancelled)
    env = dict(os.environ, OMP_NUM_THREADS='2', OPENBLAS_NUM_THREADS='2')
    process = subprocess.Popen([sys.executable, '-m', 'vecstor.index.build_worker'],
                               stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=env)
    completed = threading.Event()
    result = []
    failures = []
    def exchange():
        try:
            # A single owner drains all three pipes. Retrying communicate after
            # a timeout can stop registering partially written stdin on Python
            # 3.12, leaving large snapshots stuck before the worker can train.
            result.append(process.communicate(payload.getvalue()))
        except BaseException as exc:
            failures.append(exc)
        finally:
            completed.set()
    transport = threading.Thread(target=exchange, name='vecstor-index-transport', daemon=True)
    started = False
    try:
        transport.start()
        started = True
        while not completed.wait(min(.05, max(.001, deadline-time.monotonic()))):
            _check(deadline, cancelled)
        _check(deadline, cancelled)
        if failures:
            raise failures[0]
        output, error = result[0]
        if process.returncode:
            raise ValueError('index worker failed: ' + error.decode(errors='replace')[-2000:])
        with np.load(BytesIO(output), allow_pickle=False) as archive:
            handle = FaissIndex.deserialize(archive['index'].tobytes())
            statistics = json.loads(str(archive['statistics']))
        resolved = config.resolved(int(len(ids)))
        if (handle.row_count != len(ids) or handle.dimension != vectors.shape[1] or
                handle.nlist != resolved.nlist or not np.array_equal(handle.ids, np.sort(ids))):
            raise ValueError('worker index identity/configuration does not match exported snapshot')
        _check(deadline, cancelled)
        return handle, statistics
    finally:
        if process.poll() is None:
            process.terminate()
            try:
                process.wait(timeout=1)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()
        else:
            process.wait()
        if started:
            transport.join()
        else:
            for pipe in (process.stdin, process.stdout, process.stderr):
                pipe.close()


def main():
    import faiss
    from .faiss_backend import FaissIndex
    faiss.omp_set_num_threads(2)
    started = time.monotonic()
    with np.load(BytesIO(sys.stdin.buffer.read()), allow_pickle=False) as archive:
        handle = FaissIndex.build(archive['ids'], archive['vectors'], BuildConfig(**json.loads(str(archive['config']))))
    serialized = np.frombuffer(handle.serialize(), dtype=np.uint8)
    stats = dict(worker_peak_rss_bytes=0, build_ms=0,
                 worker_peak_rss_scope='through_output_serialization_before_final_statistics_repack')
    output = BytesIO()
    np.savez(output, index=serialized, statistics=np.array(json.dumps(stats)))
    # Materialize the transport buffer before sampling peak RSS, including the
    # index archive and outer NPZ copies. The small final stats repack is excluded.
    output.getvalue()
    stats.update(worker_peak_rss_bytes=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss*1024,
                 build_ms=(time.monotonic()-started)*1000)
    output.seek(0)
    output.truncate()
    np.savez(output, index=serialized, statistics=np.array(json.dumps(stats)))
    sys.stdout.buffer.write(output.getvalue())


if __name__ == '__main__':
    main()
