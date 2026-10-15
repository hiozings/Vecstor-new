"""Native peer doubles exercise real request-loop ownership/error framing."""
import ctypes,json,os,sys,weakref
from types import SimpleNamespace
import faiss
import numpy as np
import pytest
from vecstor.index import gpu_worker as module
from vecstor.index.faiss_backend import FaissIndex


def native_loop(monkeypatch,actions,nan=False):
    references=[];responses=[];messages=[]
    for i,action in enumerate(actions):
        messages.append(json.dumps(dict(action=action,key='one',request_id=str(i),generation=1,query=[0,0],count=1,nprobe=1)).encode())
        if action=='clone':messages.append(b'index bytes')
    stream=iter(messages)
    class Reader:
        def recv_bytes(self,*args):
            try:return next(stream)
            except StopIteration:raise EOFError
        def close(self):pass
    class Writer:
        def send_bytes(self,data):responses.append((json.loads(data),bool(references and references[-1]() is not None)))
        def close(self):pass
    class GPU:
        ntotal=1024;d=2;nlist=1
        def search(self,*args,**kwargs):return np.array([[np.nan if nan else 0]],np.float32),np.array([[7]],np.int64)
    def clone(*args):
        obj=GPU();references.append(weakref.ref(obj));return obj
    class Runtime:
        def prctl(self,*args):return 0
        def cudaSetDevice(self,*args):return 0
        def cudaMemGetInfo(self,free,total):free._obj.value=16*1024**3;total._obj.value=16*1024**3;return 0
        def cuDeviceGetName(self,name,*args):name.value=b'Test GPU';return 0
    monkeypatch.setattr(ctypes,'CDLL',lambda *a,**k:Runtime())
    monkeypatch.setattr(module,'Connection',lambda fd,**kw:Reader() if fd==10 else Writer())
    monkeypatch.setattr(sys,'argv',['worker','10','11','0','0','null',str(os.getppid())])
    monkeypatch.setattr(faiss,'get_num_gpus',lambda:1)
    monkeypatch.setattr(faiss,'StandardGpuResources',lambda:SimpleNamespace(setTempMemory=lambda value:None))
    monkeypatch.setattr(faiss,'index_cpu_to_gpu',clone)
    monkeypatch.setattr(FaissIndex,'deserialize',lambda data:SimpleNamespace(byte_size=1024,row_count=1024,dimension=2,nlist=1,_index=object()))
    module.main()
    return responses


def test_release_drops_last_native_index_reference(monkeypatch):
    responses=native_loop(monkeypatch,['clone','search','release'])
    assert responses[0][1] is True
    assert responses[2][1] is False,'GPU object retained after release response'


def test_native_nonfinite_search_is_encoded_as_data_loss(monkeypatch):
    responses=native_loop(monkeypatch,['clone','search'],nan=True)
    response=responses[1][0]
    assert response['ok'] is False
    assert response.get('error_kind')=='data'
