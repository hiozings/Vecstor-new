import time
from concurrent.futures import ThreadPoolExecutor

import numpy as np
import pytest


def backend():
    from vecstor.index.faiss_backend import FaissIndex
    from vecstor.index.types import BuildConfig
    return FaissIndex, BuildConfig


def fixture():
    ids = np.arange(80, dtype=np.int64) + 2**53
    vectors = np.arange(80, dtype=np.float32).reshape(-1, 1)
    return ids, vectors


def test_large_ids_selector_and_roundtrip():
    FaissIndex, BuildConfig = backend()
    ids, vectors = fixture()
    index = FaissIndex.build(ids, vectors, BuildConfig(nlist=2))
    restored = FaissIndex.deserialize(index.serialize())
    for handle in [index, restored]:
        distances, found = handle.search(np.array([0], dtype=np.float32), 3, 2)
        assert found.tolist() == [2**53, 2**53+1, 2**53+2]
        assert distances.tolist() == [0, 1, 4]
        _, selected = handle.search(np.array([0], dtype=np.float32), 3, 2, ids[70:])
        assert selected.tolist() == [2**53+70, 2**53+71, 2**53+72]
        assert handle.row_count == 80 and handle.dimension == 1
        assert handle.max_abs == 79 and handle.byte_size > vectors.nbytes


def test_concurrent_search_parameters_are_local():
    FaissIndex, BuildConfig = backend()
    ids, vectors = fixture()
    index = FaissIndex.build(ids, vectors, BuildConfig(nlist=2))
    def search(offset):
        return index.search(np.array([0], dtype=np.float32), 1, 2, ids[offset:offset+1])[1].tolist()
    with ThreadPoolExecutor(max_workers=2) as pool:
        assert list(pool.map(search, [1, 70]*5)) == [[2**53+v] for v in [1,70]*5]


@pytest.mark.parametrize('mutation', ['duplicate', 'negative', 'float_ids', 'nan', 'huge', 'shape', 'empty'])
def test_invalid_builds_rejected(mutation):
    FaissIndex, BuildConfig = backend()
    ids, vectors = fixture()
    if mutation == 'duplicate': ids[1] = ids[0]
    if mutation == 'negative': ids[0] = -1
    if mutation == 'float_ids': ids = ids.astype(float)
    if mutation == 'nan': vectors[0] = np.nan
    if mutation == 'huge': vectors[0] = 1e30
    if mutation == 'shape': vectors = vectors[:-1]
    if mutation == 'empty': ids, vectors = ids[:0], vectors[:0]
    with pytest.raises(ValueError): FaissIndex.build(ids, vectors, BuildConfig(nlist=2))


def test_build_config_validation_and_fingerprint():
    _, BuildConfig = backend()
    assert BuildConfig().resolved(10000).nlist == 100
    assert BuildConfig().resolved(100000).nlist == 256
    assert BuildConfig(nlist=2).fingerprint() != BuildConfig(nlist=3).fingerprint()
    for cfg in [BuildConfig(nlist=0), BuildConfig(nlist=100), BuildConfig(nlist=2, train_rows=1)]:
        with pytest.raises(ValueError): cfg.resolved(80)


def test_worker_build_and_expiration():
    from vecstor.index.build_worker import build_in_worker
    _, BuildConfig = backend()
    ids, vectors = fixture()
    handle, stats = build_in_worker(ids, vectors, BuildConfig(nlist=2), deadline=time.monotonic()+20)
    assert handle.search(np.array([0], dtype=np.float32), 1, 2)[1].tolist() == [2**53]
    assert stats['worker_peak_rss_bytes'] > 0 and stats['build_ms'] > 0
    with pytest.raises(TimeoutError):
        build_in_worker(ids, vectors, BuildConfig(nlist=2), deadline=time.monotonic()-1)
    calls = 0
    def cancel_after_spawn():
        nonlocal calls
        calls += 1
        return calls >= 2
    with pytest.raises(InterruptedError):
        build_in_worker(ids, vectors, BuildConfig(nlist=2), deadline=time.monotonic()+20, cancelled=cancel_after_spawn)


def test_search_rejects_invalid_bounds_and_discards_missing_labels():
    FaissIndex, BuildConfig = backend()
    ids, vectors = fixture()
    handle = FaissIndex.build(ids, vectors, BuildConfig(nlist=2))
    for query, count, nprobe in [(np.array([1e30]), 1, 2), (np.array([np.nan]), 1, 2),
                                  (np.array([0, 1]), 1, 2), (np.array([0]), 0, 2),
                                  (np.array([0]), 1, 0), (np.array([0]), 1, 3)]:
        with pytest.raises(ValueError): handle.search(query, count, nprobe)
    distances, found = handle.search(np.array([0]), 10, 2, ids[:1])
    assert found.tolist() == [2**53] and distances.tolist() == [0]
    assert handle.search(np.array([0]), 10, 2, ids[:0])[1].tolist() == []


def test_worker_cancellation_reaps_spawned_process(monkeypatch):
    import subprocess
    from vecstor.index import build_worker
    _, BuildConfig = backend()
    ids, vectors = fixture()
    real_popen = subprocess.Popen
    processes = []
    def recording_popen(*args, **kwargs):
        process = real_popen(*args, **kwargs)
        processes.append(process)
        return process
    monkeypatch.setattr(build_worker.subprocess, 'Popen', recording_popen)
    with pytest.raises(InterruptedError):
        build_worker.build_in_worker(ids, vectors, BuildConfig(nlist=2),
                                     deadline=time.monotonic()+20, cancelled=lambda: bool(processes))
    assert len(processes) == 1
    assert processes[0].poll() is not None


def test_training_is_deterministic_for_permuted_input():
    FaissIndex, BuildConfig = backend()
    ids, vectors = fixture()
    config = BuildConfig(nlist=2, train_rows=40)
    first = FaissIndex.build(ids, vectors, config)
    second = FaissIndex.build(ids[::-1], vectors[::-1], config)
    assert first.serialize() == second.serialize()


def test_extreme_finite_float64_query_rejected_as_value_error():
    FaissIndex, BuildConfig = backend()
    ids, vectors = fixture()
    handle = FaissIndex.build(ids, vectors, BuildConfig(nlist=2))
    with pytest.raises(ValueError):
        handle.search(np.array([np.finfo(np.float64).max]), 1, 2)


def test_worker_build_under_landlock_without_storage_access(tmp_path):
    import json
    import subprocess
    import sys
    if sys.platform != 'linux':
        pytest.skip('Landlock is Linux-only')
    (tmp_path / 'probe.npz').write_bytes(b'storage probe')
    program = '''
import json, sys, time
from vecstor.isolation import restrict_compute_filesystem
isolation = restrict_compute_filesystem(sys.argv[1])
import numpy as np
from vecstor.index.types import BuildConfig
from vecstor.index.build_worker import build_in_worker
ids = np.arange(80, dtype=np.int64) + 2**53
vectors = np.arange(80, dtype=np.float32).reshape(-1, 1)
handle, statistics = build_in_worker(ids, vectors, BuildConfig(nlist=2), deadline=time.monotonic()+20)
print(json.dumps(dict(isolation=isolation, ids=handle.search(np.array([0], dtype=np.float32), 1, 2)[1].tolist())))
'''
    result = subprocess.run([sys.executable, '-c', program, str(tmp_path)], capture_output=True, text=True, timeout=25)
    assert result.returncode == 0, result.stderr
    result = json.loads(result.stdout)
    assert result['isolation']['directory_denied'] and result['isolation']['file_denied']
    assert result['ids'] == [2**53]


@pytest.mark.parametrize('invalid_ids', ['duplicate', 'negative'])
def test_deserialize_rejects_invalid_actual_ivf_ids(invalid_ids):
    import faiss
    from io import BytesIO
    FaissIndex, _ = backend()
    ids, vectors = fixture()
    if invalid_ids == 'duplicate': ids[:] = 0
    else: ids[0] = -2
    index = faiss.IndexIVFFlat(faiss.IndexFlatL2(1), 1, 2)
    index.train(vectors)
    index.add_with_ids(vectors, ids)
    data = BytesIO()
    np.savez(data, index=faiss.serialize_index(index), max_abs=np.array(79.))
    with pytest.raises(ValueError): FaissIndex.deserialize(data.getvalue())


def test_deserialize_derives_numeric_bound_from_actual_vectors():
    from io import BytesIO
    import faiss
    FaissIndex, BuildConfig = backend()
    ids, vectors = fixture()
    handle = FaissIndex.build(ids, vectors, BuildConfig(nlist=2))
    archive = BytesIO()
    np.savez(archive, index=faiss.serialize_index(handle._index), max_abs=np.array(0.))
    restored = FaissIndex.deserialize(archive.getvalue())
    assert restored.max_abs == 79


def test_worker_validates_decoded_identity_against_export(monkeypatch):
    from vecstor.index import build_worker
    FaissIndex, BuildConfig = backend()
    ids, vectors = fixture()
    wrong = FaissIndex.build(ids+1000, vectors, BuildConfig(nlist=2))
    monkeypatch.setattr(FaissIndex, 'deserialize', classmethod(lambda cls, data: wrong))
    with pytest.raises(ValueError, match='identity'):
        build_worker.build_in_worker(ids, vectors, BuildConfig(nlist=2), deadline=time.monotonic()+20)


def test_worker_peak_rss_is_sampled_after_serialization(monkeypatch):
    from dataclasses import asdict
    from io import BytesIO
    import json
    from types import SimpleNamespace
    from vecstor.index import build_worker
    _, BuildConfig = backend()
    ids, vectors = fixture()
    source = BytesIO()
    np.savez(source, ids=ids, vectors=vectors, config=np.array(json.dumps(asdict(BuildConfig(nlist=2)))))
    monkeypatch.setattr(build_worker.sys, 'stdin', SimpleNamespace(buffer=BytesIO(source.getvalue())))
    target = BytesIO()
    monkeypatch.setattr(build_worker.sys, 'stdout', SimpleNamespace(buffer=target))
    saved = []
    observations = []
    real_savez = np.savez
    real_usage = build_worker.resource.getrusage
    def save(*args, **kwargs):
        result = real_savez(*args, **kwargs)
        saved.append(set(kwargs))
        return result
    def usage(*args):
        observations.append(list(saved))
        return real_usage(*args)
    monkeypatch.setattr(np, 'savez', save)
    monkeypatch.setattr(build_worker.resource, 'getrusage', usage)
    build_worker.main()
    assert any({'index', 'max_abs'} in event and {'index', 'statistics'} in event for event in observations)


@pytest.mark.parametrize('corruption', ['count', 'untrained', 'nonfinite_codes', 'unsafe_codes'])
def test_deserialize_rejects_corrupt_ivf_structure_or_codes(corruption):
    import faiss
    from io import BytesIO
    FaissIndex, BuildConfig = backend()
    ids, vectors = fixture()
    handle = FaissIndex.build(ids, vectors, BuildConfig(nlist=2))
    index = handle._index
    if corruption == 'count': index.ntotal += 1
    elif corruption == 'untrained': index.is_trained = False
    else:
        for bucket in range(index.nlist):
            count = index.invlists.list_size(bucket)
            if count:
                pointer = index.invlists.get_codes(bucket)
                try:
                    codes = faiss.rev_swig_ptr(pointer, count*index.code_size).view(np.float32)
                    codes[0] = np.nan if corruption == 'nonfinite_codes' else 1e30
                finally:
                    index.invlists.release_codes(bucket, pointer)
                break
    data = BytesIO()
    np.savez(data, index=faiss.serialize_index(index), max_abs=np.array(0.))
    with pytest.raises(ValueError): FaissIndex.deserialize(data.getvalue())


@pytest.mark.parametrize('mismatch', ['dimension', 'nlist'])
def test_worker_validates_decoded_configuration(monkeypatch, mismatch):
    from vecstor.index import build_worker
    FaissIndex, BuildConfig = backend()
    ids, vectors = fixture()
    other_vectors = np.repeat(vectors, 2, axis=1) if mismatch == 'dimension' else vectors
    wrong = FaissIndex.build(ids, other_vectors, BuildConfig(nlist=1 if mismatch == 'nlist' else 2))
    monkeypatch.setattr(FaissIndex, 'deserialize', classmethod(lambda cls, data: wrong))
    with pytest.raises(ValueError, match='configuration'):
        build_worker.build_in_worker(ids, vectors, BuildConfig(nlist=2), deadline=time.monotonic()+20)


def test_worker_drains_input_larger_than_pipe_capacity():
    from vecstor.index.build_worker import build_in_worker
    _, BuildConfig = backend()
    count = 10000
    ids = np.arange(count, dtype=np.int64)
    vectors = np.zeros((count, 2), dtype=np.float32)
    handle, _ = build_in_worker(ids, vectors, BuildConfig(nlist=1), deadline=time.monotonic()+5)
    assert handle.row_count == count
    assert np.array_equal(handle.ids, ids)


@pytest.mark.parametrize('stop', ['deadline', 'cancel'])
def test_worker_stop_while_large_input_is_blocked_reaps_child(monkeypatch, stop):
    import subprocess
    import sys
    import threading
    from vecstor.index import build_worker
    _, BuildConfig = backend()
    real_popen = subprocess.Popen
    processes = []
    spawned_at = []
    def sleeping_worker(*args, **kwargs):
        process = real_popen([sys.executable, '-c', 'import time; time.sleep(30)'], **kwargs)
        processes.append(process)
        spawned_at.append(time.monotonic())
        return process
    monkeypatch.setattr(build_worker.subprocess, 'Popen', sleeping_worker)
    ids = np.arange(10000, dtype=np.int64)
    vectors = np.zeros((10000, 2), dtype=np.float32)
    deadline = time.monotonic() + (.3 if stop == 'deadline' else 20)
    def cancelled():
        return stop == 'cancel' and bool(spawned_at) and time.monotonic()-spawned_at[0] >= .1
    expected = TimeoutError if stop == 'deadline' else InterruptedError
    started = time.monotonic()
    with pytest.raises(expected):
        build_worker.build_in_worker(ids, vectors, BuildConfig(nlist=1), deadline, cancelled)
    assert time.monotonic()-started < 3
    assert len(processes) == 1 and processes[0].poll() is not None
    assert all(pipe.closed for pipe in (processes[0].stdin, processes[0].stdout, processes[0].stderr))
    assert not any(thread.name == 'vecstor-index-transport' for thread in threading.enumerate())


def test_repeated_large_selectors_do_not_leak_native_memory():
    import json, os, subprocess, sys
    code='''
import gc,json
from pathlib import Path
import faiss,numpy as np
from vecstor.index.faiss_backend import FaissIndex
from vecstor.index.types import BuildConfig
faiss.omp_set_num_threads(1)
n=50000
ids=np.arange(n,dtype=np.int64)
vectors=np.random.default_rng(43).random((n,2),dtype=np.float32)
index=FaissIndex.build(ids,vectors,BuildConfig(nlist=8))
query=np.array([.5,.5],dtype=np.float32)
def rss():
    return int(next(line.split()[1] for line in Path('/proc/self/status').read_text().splitlines() if line.startswith('VmRSS:')))*1024
for _ in range(10):index.search(query,64,8,ids)
gc.collect();before=rss()
for i in range(100):
    index.search(query,64,8,ids)
    if rss()-before>80*1024*1024:break
gc.collect()
print(json.dumps({'growth_bytes':rss()-before,'iterations':i+1}))
'''
    result=subprocess.run([sys.executable,'-c',code],capture_output=True,text=True,timeout=30,
                          env=dict(os.environ,OMP_NUM_THREADS='1',OPENBLAS_NUM_THREADS='1'))
    assert result.returncode==0,result.stderr
    report=json.loads(result.stdout)
    assert report['growth_bytes']<32*1024*1024,report
    assert report['iterations']==100
