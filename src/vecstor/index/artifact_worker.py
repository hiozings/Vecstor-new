"""Bounded-lifetime native artifact verifier; storage never parses native bytes."""
import json
import os
from pathlib import Path
import resource
import subprocess
import sys
import threading
import time

from .artifact import ArtifactDescriptor, ArtifactError, canonical, require, validate_artifact


def working_bytes(descriptor):
    return 3*descriptor.payload_size + 2*descriptor.row_count*(4*descriptor.key.dimension+8)


def _check(deadline, cancelled):
    if cancelled(): raise InterruptedError('artifact verification cancelled')
    if time.monotonic() >= deadline: raise TimeoutError('artifact verification deadline exceeded')


def verify_in_worker(path, descriptor, deadline, cancelled, max_working_bytes=512*1024**2):
    descriptor = ArtifactDescriptor.from_dict(descriptor.to_dict())
    require(working_bytes(descriptor) <= max_working_bytes, 'artifact_verifier_budget', 'RESOURCE_EXHAUSTED')
    _check(deadline, cancelled)
    request = canonical(dict(path=str(path), descriptor=descriptor.to_dict()))
    process = subprocess.Popen([sys.executable, '-m', 'vecstor.index.artifact_worker'],
        stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        env=dict(os.environ, OMP_NUM_THREADS='1', OPENBLAS_NUM_THREADS='1'))
    done = threading.Event()
    result, errors = [], []
    def exchange():
        try: result.append(process.communicate(request))
        except BaseException as error: errors.append(error)
        finally: done.set()
    transport = threading.Thread(target=exchange, name='vecstor-artifact-transport', daemon=True)
    transport.start()
    try:
        while not done.wait(.02): _check(deadline, cancelled)
        _check(deadline, cancelled)
        if errors: raise errors[0]
        output, stderr = result[0]
        require(process.returncode == 0, 'artifact_verifier_failed: ' + stderr.decode(errors='replace')[-1000:], 'DATA_LOSS')
        try: report = json.loads(output)
        except (ValueError, UnicodeError) as error: raise ArtifactError('DATA_LOSS', 'invalid_verifier_response') from error
        if not report.get('ok'): raise ArtifactError(report.get('code', 'DATA_LOSS'), report.get('reason', 'invalid_artifact'))
        require(report['snapshot_sha256'] == descriptor.snapshot_sha256 and report['row_count'] == descriptor.row_count,
                'verifier_identity_mismatch', 'DATA_LOSS')
        return report
    finally:
        if process.poll() is None:
            process.terminate()
            try: process.wait(timeout=1)
            except subprocess.TimeoutExpired:
                process.kill(); process.wait()
        transport.join()


def main():
    started = time.monotonic()
    try:
        request = json.loads(sys.stdin.buffer.read(128*1024+1))
        descriptor = ArtifactDescriptor.from_dict(request['descriptor'])
        fd = os.open(request['path'], os.O_RDONLY | os.O_NOFOLLOW)
        with os.fdopen(fd, 'rb') as source:
            require(os.fstat(source.fileno()).st_size == descriptor.payload_size, 'artifact_payload_size', 'DATA_LOSS')
            payload = source.read(descriptor.payload_size+1)
        handle = validate_artifact(payload, descriptor)
        report = dict(ok=True, row_count=handle.row_count, snapshot_sha256=descriptor.snapshot_sha256,
                      verify_ms=(time.monotonic()-started)*1000,
                      worker_peak_rss_bytes=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss*1024)
    except Exception as error:
        report = dict(ok=False, code=error.code if isinstance(error, ArtifactError) else 'DATA_LOSS', reason=str(error))
    sys.stdout.buffer.write(canonical(report))


if __name__ == '__main__': main()
