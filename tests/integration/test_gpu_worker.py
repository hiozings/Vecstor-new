import time
import numpy as np
import pytest
from vecstor.index.faiss_backend import FaissIndex
from vecstor.index.types import BuildConfig,IndexKey


def fixture_index():
    return FaissIndex.build(np.arange(1024,dtype=np.int64)+2**60,np.arange(2048,dtype=np.float32).reshape(1024,2),BuildConfig(nlist=1))


def test_real_gpu_worker_clone_search_release_and_generation():
    from vecstor.index.gpu_worker import GpuWorker
    cpu=fixture_index()
    with GpuWorker(scratch_bytes=16*1024**2) as worker:
        info=worker.submit_clone('first',cpu.serialize(),time.monotonic()+30)
        assert info['row_count']==1024
        result=worker.search('first',np.array([0,1],np.float32),10,1,time.monotonic()+5)
        assert result['ids'][0]==2**60
        assert result['distances'][0]==0
        with pytest.raises(ValueError,match='candidate'):
            worker.search('first',np.zeros(2,np.float32),2049,1,time.monotonic()+5)
        worker.release('first')
        with pytest.raises(ValueError,match='missing'):worker.search('first',np.zeros(2,np.float32),10,1,time.monotonic()+5)
        generation=worker.generation
        worker.reset()
        assert worker.generation>generation


def test_queued_expired_request_never_creates_replica():
    from vecstor.index.gpu_worker import GpuWorker
    with GpuWorker() as worker:
        with pytest.raises(TimeoutError):worker.submit_clone('expired',b'',time.monotonic()-1)
        assert worker.pid is None


def test_worker_timeout_invalidates_queued_jobs_and_can_be_rebuilt():
    import os,signal
    from concurrent.futures import ThreadPoolExecutor
    from vecstor.index.gpu_worker import GpuWorker
    cpu=fixture_index()
    with GpuWorker(scratch_bytes=16*1024**2) as worker,ThreadPoolExecutor(2) as pool:
        worker.submit_clone('first',cpu.serialize(),time.monotonic()+30)
        generation=worker.generation
        os.kill(worker.pid,signal.SIGSTOP)
        a=pool.submit(worker.search,'first',np.zeros(2,np.float32),10,1,time.monotonic()+.3)
        time.sleep(.05)
        b=pool.submit(worker.search,'first',np.zeros(2,np.float32),10,1,time.monotonic()+3)
        with pytest.raises(TimeoutError):a.result(timeout=3)
        with pytest.raises(ValueError):b.result(timeout=3)
        assert worker.generation>generation
        worker.submit_clone('new',cpu.serialize(),time.monotonic()+30)
        assert worker.search('new',np.array([0,1],np.float32),1,1,time.monotonic()+5)['ids']==[2**60]


def test_gpu_memory_preflight_rejects_without_publishing():
    from vecstor.index.gpu_worker import GpuWorker
    with GpuWorker(margin_bytes=2**50) as worker:
        with pytest.raises(ValueError,match='memory_budget'):
            worker.submit_clone('huge',fixture_index().serialize(),time.monotonic()+30)
        with pytest.raises(ValueError,match='missing'):
            worker.search('huge',np.zeros(2,np.float32),1,1,time.monotonic()+5)


def test_queue_limit_and_queued_cancel_do_not_reset_active_gpu():
    import os,signal,threading
    from concurrent.futures import ThreadPoolExecutor
    from vecstor.index.gpu_worker import GpuWorker
    with GpuWorker(max_pending=1,scratch_bytes=16*1024**2) as worker,ThreadPoolExecutor(2) as pool:
        worker.submit_clone('first',fixture_index().serialize(),time.monotonic()+30)
        generation=worker.generation;pid=worker.pid;os.kill(pid,signal.SIGSTOP)
        cancelled=threading.Event()
        try:
            first=pool.submit(worker.search,'first',np.zeros(2,np.float32),1,1,time.monotonic()+5)
            time.sleep(.05)
            second=pool.submit(worker.search,'first',np.zeros(2,np.float32),1,1,time.monotonic()+5,cancelled.is_set)
            time.sleep(.05)
            with pytest.raises(ValueError,match='queue_full'):
                worker.search('first',np.zeros(2,np.float32),1,1,time.monotonic()+5)
            cancelled.set()
            with pytest.raises(InterruptedError):second.result(timeout=2)
            assert worker.generation==generation
        finally:os.kill(pid,signal.SIGCONT)
        assert first.result(timeout=3)['ids'][0]==2**60


def test_native_worker_dies_if_parent_is_killed_while_worker_is_stopped():
    import os,signal,subprocess,sys
    code="""import time,faiss,numpy as np
from vecstor.index.faiss_backend import FaissIndex
from vecstor.index.types import BuildConfig
from vecstor.index.gpu_worker import GpuWorker
faiss.omp_set_num_threads(1)
cpu=FaissIndex.build(np.arange(1024,dtype=np.int64),np.arange(2048,dtype=np.float32).reshape(1024,2),BuildConfig(nlist=1))
w=GpuWorker(scratch_bytes=16*1024**2)
w.submit_clone('one',cpu.serialize(),time.monotonic()+30)
print(w.pid,flush=True)
time.sleep(60)
"""
    parent=subprocess.Popen([sys.executable,'-c',code],stdout=subprocess.PIPE,text=True)
    pid=None
    try:
        pid=int(parent.stdout.readline());os.kill(pid,signal.SIGSTOP);parent.kill();parent.wait(timeout=5)
        def alive():
            try:return 'State:\tZ' not in open(f'/proc/{pid}/status').read()
            except FileNotFoundError:return False
        until=time.monotonic()+2
        while alive() and time.monotonic()<until:time.sleep(.02)
        assert not alive(),'Stopped GPU worker survived parent death'
    finally:
        if parent.poll() is None:parent.kill();parent.wait()
        parent.stdout.close()
        if pid:
            try:os.kill(pid,signal.SIGKILL)
            except ProcessLookupError:pass


def test_wrong_ipc_identity_is_data_corruption_not_device_unavailability(monkeypatch):
    import vecstor.index.gpu_worker as module
    original=module.subprocess.Popen
    peer="""import sys,json
from multiprocessing.connection import Connection
r=Connection(int(sys.argv[1]),readable=True,writable=False);w=Connection(int(sys.argv[2]),readable=False,writable=True)
x=json.loads(r.recv_bytes());r.recv_bytes()
w.send_bytes(json.dumps(dict(request_id='wrong',generation=x['generation'],ok=True,result={})).encode())
"""
    def corrupt_peer(command,**kwargs):
        return original([module.sys.executable,'-c',peer,*command[3:]],**kwargs)
    monkeypatch.setattr(module.subprocess,'Popen',corrupt_peer)
    with module.GpuWorker() as worker:
        with pytest.raises(RuntimeError,match='identity'):
            worker.submit_clone('key',b'fake',time.monotonic()+5)
