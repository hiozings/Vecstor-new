"""Bounded GPU IPC owned by one dispatcher and one fresh interpreter."""
from dataclasses import dataclass,field
import json
import os
from multiprocessing.connection import Connection
from collections import deque
import subprocess
import sys
import threading
import time
import uuid

MAX_FRAME=512*1024*1024


class GpuDataError(RuntimeError):
    pass


class GpuError(ValueError):
    pass


@dataclass
class Job:
    message: dict
    payload: bytes | None
    deadline: float
    cancelled: object
    generation: int
    done: threading.Event=field(default_factory=threading.Event)
    active: bool=False
    abandoned: bool=False
    result: object=None
    error: Exception | None=None


class GpuWorker:
    def __init__(self,scratch_bytes=128*1024**2,margin_bytes=256*1024**2,max_pending=8,isolation_probe=None):
        if min(scratch_bytes,margin_bytes)<0 or max_pending<1:raise ValueError('Invalid GPU budgets')
        self.scratch_bytes=scratch_bytes;self.margin_bytes=margin_bytes;self.isolation_probe=isolation_probe
        self._queue=deque();self._max_pending=max_pending;self._lock=threading.RLock();self._condition=threading.Condition(self._lock);self._thread=None
        self._process=None;self._closed=False;self._generation=1

    @property
    def generation(self):
        with self._lock:
            if self._process is not None and self._process.poll() is not None:
                self._process.wait();self._process=None;self._generation+=1
            return self._generation

    @property
    def pid(self):
        with self._lock:return self._process.pid if self._process is not None else None

    def reset(self):
        with self._lock:
            process=self._process;self._process=None;self._generation+=1
            if process is not None:
                if process.poll() is None:process.kill()
                process.wait(timeout=5)

    def _start(self):
        rx,tx=os.pipe();rr,tt=os.pipe()
        try:
            process=subprocess.Popen([sys.executable,'-m','vecstor.index.gpu_worker',str(rx),str(tt),str(self.scratch_bytes),str(self.margin_bytes),json.dumps(self.isolation_probe),str(os.getpid())],
                                     pass_fds=(rx,tt),stdin=subprocess.DEVNULL,stdout=subprocess.DEVNULL,stderr=None,
                                     env=dict(os.environ,OMP_NUM_THREADS='1',OPENBLAS_NUM_THREADS='1',CUDA_CACHE_DISABLE='1'))
        except BaseException:
            os.close(tx);os.close(rr);raise
        finally:os.close(rx);os.close(tt)
        self._process=process
        return process,Connection(tx,writable=True,readable=False),Connection(rr,writable=False,readable=True)

    def _dispatch(self):
        process=sender=receiver=None
        try:
            while True:
                with self._condition:
                    while not self._queue and not self._closed:self._condition.wait()
                    if not self._queue and self._closed:return
                    job=self._queue.popleft()
                try:
                    with self._lock:
                        if self._closed:raise GpuError('gpu_worker_closed')
                        if job.generation!=self.generation:raise GpuError('gpu_worker_generation_changed')
                        if job.abandoned or job.cancelled():raise InterruptedError('GPU request cancelled')
                        if time.monotonic()>=job.deadline:raise TimeoutError('GPU queue deadline exceeded')
                        if process is not self._process or process is None or process.poll() is not None:
                            if sender:sender.close();receiver.close()
                            if self._process is not None:self.reset()
                            if job.message['action']!='clone':raise GpuError('gpu_replica_missing')
                            process,sender,receiver=self._start()
                        job.active=True
                    envelope=dict(job.message,request_id=uuid.uuid4().hex,generation=job.generation)
                    sender.send_bytes(json.dumps(envelope).encode())
                    if job.payload is not None:sender.send_bytes(job.payload)
                    try:response=json.loads(receiver.recv_bytes(MAX_FRAME))
                    except (json.JSONDecodeError,UnicodeDecodeError) as error:raise GpuDataError('Malformed GPU response') from error
                    if not isinstance(response,dict):raise GpuDataError('Malformed GPU response')
                    if response.get('request_id')!=envelope['request_id'] or response.get('generation')!=job.generation:
                        raise GpuDataError('GPU response identity mismatch')
                    if response.get('ok') is True and not isinstance(response.get('result'),dict):raise GpuDataError('Malformed GPU result')
                    if response.get('ok') is not True:
                        if response.get('error_kind')=='data':raise GpuDataError(response.get('error','GPU data corruption'))
                        if response.get('ok') is not False or response.get('error_kind')!='device':raise GpuDataError('Malformed GPU failure')
                        raise GpuError(response.get('error','gpu_native_error'))
                    with self._lock:
                        if job.generation!=self.generation:raise GpuError('gpu_worker_generation_changed')
                        job.result=response['result']
                except GpuDataError as error:
                    with self._lock:
                        if process is self._process:self.reset()
                    job.error=error
                except GpuError as error:job.error=error
                except (TimeoutError,InterruptedError) as error:job.error=error
                except Exception as error:
                    with self._lock:
                        if process is self._process:self.reset()
                    job.error=GpuError('gpu_worker_failed: '+str(error))
                finally:job.payload=None;job.active=False;job.done.set()
        finally:
            if sender:sender.close();receiver.close()

    def _call(self,message,payload=None,deadline=None,cancelled=lambda:False):
        deadline=deadline if deadline is not None else time.monotonic()+10
        if cancelled():raise InterruptedError('GPU request cancelled')
        if time.monotonic()>=deadline:raise TimeoutError('GPU deadline exceeded')
        if payload is not None and len(payload)>MAX_FRAME:raise GpuError('gpu_clone_payload_budget')
        with self._lock:
            if self._closed:raise GpuError('gpu_worker_closed')
            job=Job(message,payload,deadline,cancelled,self.generation)
            if self._thread is None:
                self._thread=threading.Thread(target=self._dispatch,name='vecstor-gpu-owner',daemon=True);self._thread.start()
            if len(self._queue)>=self._max_pending:raise GpuError('gpu_queue_full')
            self._queue.append(job);self._condition.notify()
        while not job.done.wait(.01):
            if cancelled() or time.monotonic()>=deadline:
                with self._lock:
                    job.abandoned=True
                    if job.active and job.generation==self.generation:self.reset()
                    elif not job.active:
                        try:self._queue.remove(job)
                        except ValueError:pass
                if cancelled():raise InterruptedError('GPU request cancelled')
                raise TimeoutError('GPU deadline exceeded')
        if cancelled():raise InterruptedError('GPU request cancelled')
        if time.monotonic()>=deadline:raise TimeoutError('GPU deadline exceeded')
        if job.error:raise job.error
        return job.result

    def submit_clone(self,key,cpu_bytes,deadline,cancelled=lambda:False):
        return self._call({'action':'clone','key':key},cpu_bytes,deadline,cancelled)

    def search(self,key,query,count,nprobe,deadline,cancelled=lambda:False,generation=None):
        if type(count) is not int or not 1<=count<=2048:raise GpuError('gpu_candidate_limit')
        if type(nprobe) is not int or not 1<=nprobe<=2048:raise GpuError('gpu_nprobe_limit')
        if generation is not None and generation!=self.generation:raise GpuError('gpu_worker_generation_changed')
        return self._call({'action':'search','key':key,'query':list(map(float,query)),'count':count,'nprobe':nprobe},deadline=deadline,cancelled=cancelled)

    def release(self,key):
        if self.pid is not None:return self._call({'action':'release','key':key})

    def close(self):
        with self._lock:
            if self._closed:return
            self._closed=True;self.reset();self._condition.notify_all()
        if self._thread:
            self._thread.join(timeout=5)

    def __enter__(self):return self
    def __exit__(self,*args):self.close()


def main():
    import ctypes
    import signal
    # A wedged/stopped CUDA worker must not outlive a killed compute process.
    libc=ctypes.CDLL(None,use_errno=True)
    if libc.prctl(1,signal.SIGKILL,0,0,0)!=0:raise OSError(ctypes.get_errno(),'GPU parent-death signal failed')
    if os.getppid()!=int(sys.argv[6]):return
    import numpy as np
    import faiss
    import ctypes
    from .faiss_backend import FaissIndex
    receiver=Connection(int(sys.argv[1]),readable=True,writable=False);sender=Connection(int(sys.argv[2]),readable=False,writable=True)
    scratch,margin=int(sys.argv[3]),int(sys.argv[4]);probe=json.loads(sys.argv[5])
    isolation=None
    if probe:
        directory,file=probe
        try:os.listdir(directory);raise RuntimeError('GPU worker can list storage')
        except PermissionError:pass
        try:
            with open(file,'rb') as source:source.read(1)
            raise RuntimeError('GPU worker can read storage')
        except PermissionError:pass
        isolation={'directory_denied':True,'file_denied':True}
    resource=None;indices={}
    def execute_native(request):
        nonlocal resource
        started=time.perf_counter()
        action=request['action'];key=request['key']
        if action=='clone':
            payload=receiver.recv_bytes(MAX_FRAME)
            if resource is None:
                if faiss.get_num_gpus()<1:raise GpuError('gpu_unavailable')
                resource=faiss.StandardGpuResources();resource.setTempMemory(scratch)
            cpu=FaissIndex.deserialize(payload)
            runtime=ctypes.CDLL(os.path.join(sys.prefix,'lib','libcudart.so.12'))
            if runtime.cudaSetDevice(0):raise GpuError('gpu_unavailable')
            free,total=ctypes.c_size_t(),ctypes.c_size_t()
            if runtime.cudaMemGetInfo(ctypes.byref(free),ctypes.byref(total)):raise GpuError('gpu_memory_query_failed')
            if free.value<cpu.byte_size*2+scratch+margin:raise GpuError('gpu_memory_budget')
            opts=faiss.GpuClonerOptions();opts.useFloat16=False;opts.indicesOptions=faiss.INDICES_64_BIT
            gpu=faiss.index_cpu_to_gpu(resource,0,cpu._index,opts)
            if gpu.ntotal!=cpu.row_count or gpu.d!=cpu.dimension:raise GpuDataError('gpu_clone_identity_mismatch')
            indices[key]=gpu
            name=ctypes.create_string_buffer(256)
            driver=ctypes.CDLL('libcuda.so.1')
            device_name=name.value.decode() if driver.cuDeviceGetName(name,256,0)==0 else 'unknown'
            result={'row_count':cpu.row_count,'dimension':cpu.dimension,'nlist':cpu.nlist,'free_before_bytes':free.value,
                    'total_vram_bytes':total.value,'gpu_model':device_name,'worker_pid':os.getpid(),'isolation':isolation}
        elif action=='release':
            indices.pop(key,None);result={}
        elif action=='search':
            if key not in indices:raise GpuError('gpu_replica_missing')
            gpu=indices[key];q=np.asarray(request['query'],dtype=np.float32)
            count,nprobe=request['count'],request['nprobe']
            if q.shape!=(gpu.d,) or not np.isfinite(q).all():raise GpuError('gpu_invalid_query')
            if not 1<=count<=2048 or not 1<=nprobe<=min(2048,gpu.nlist):raise GpuError('gpu_search_limits')
            d,i=gpu.search(q.reshape(1,-1),count,params=faiss.SearchParametersIVF(nprobe=nprobe))
            valid=i[0]!=-1
            if not np.isfinite(d[0][valid]).all():raise GpuDataError('gpu_nonfinite_result')
            result={'ids':i[0][valid].tolist(),'distances':d[0][valid].tolist()}
        else:raise GpuError('gpu_unknown_action')
        result['gpu_host_ms']=(time.perf_counter()-started)*1000
        return result
    try:
        while True:
            try:request=json.loads(receiver.recv_bytes(1024*1024))
            except EOFError:return
            started=time.perf_counter()
            try:
                response={'ok':True,'result':execute_native(request)}
            except GpuDataError as error:response={'ok':False,'error_kind':'data','error':str(error)}
            except Exception as error:response={'ok':False,'error_kind':'device','error':'gpu_native_error: '+str(error)}
            response.update(request_id=request['request_id'],generation=request['generation'])
            sender.send_bytes(json.dumps(response,allow_nan=False).encode())
    finally:indices.clear();receiver.close();sender.close()

if __name__=='__main__':main()
