"""Explicit, version-bound index construction; no query-time hidden training."""

import time
from threading import BoundedSemaphore

import grpc
import numpy as np

from vecstor.compute.lease_io import LeaseIO, consistent
from vecstor.index.types import BuildConfig, IndexKey
from vecstor.index.registry import IndexRegistry
from vecstor.index.build_worker import build_in_worker
from vecstor.rpc.common import RemoteError


class IndexManager:
    def __init__(self,stub,max_bytes=256*1024*1024,max_entries=8):
        self.stub=stub
        self.registry=IndexRegistry(max_bytes,max_entries)
        self.build_slot=BoundedSemaphore(1)

    def build(self,request,context):
        if not self.build_slot.acquire(blocking=False):
            raise RemoteError(grpc.StatusCode.RESOURCE_EXHAUSTED,'Another index build is running')
        deadline=time.monotonic()+min(60,context.time_remaining() or 45)
        trace={};io=LeaseIO(self.stub,request.database_id,request.table,context,deadline,trace)
        start=time.monotonic()
        try:
            lease=io.open()
            consistent(lease.HasField('row_count'),'Storage does not report snapshot row count')
            n=lease.row_count
            config=BuildConfig(nlist=request.nlist if request.HasField('nlist') else None,
                              seed=request.seed if request.HasField('seed') else 20261013).validate(n)
            io.remaining()
            if n<1024:
                return {'state':'exact_only','reason':'small_table','row_count':n,'revision':lease.revision,'reused':False}
            config=config.resolved(n)
            key=IndexKey.from_lease(lease,config)
            entry=self.registry.reactivate(key,check=io.remaining)
            if entry is not None:
                return dict(entry.describe(),state='ready',reused=True)
            # Residency budget and transient build estimate are separately named.
            estimate=n*(4*lease.dimension+8)*4+min(n,config.train_rows)*lease.dimension*4
            try: self.registry.prepare_build(key,estimate,check=io.remaining)
            except RemoteError: raise
            except ValueError as error: raise RemoteError(grpc.StatusCode.RESOURCE_EXHAUSTED,str(error)) from error
            ids=np.empty(n,dtype=np.int64);vectors=np.empty((n,lease.dimension),dtype=np.float32)
            offset=0
            for labels,values in io.snapshot():
                ids[offset:offset+len(labels)]=labels;vectors[offset:offset+len(labels)]=values;offset+=len(labels)
            consistent(offset==n,'Incomplete build input')
            maximum=float(np.abs(vectors).max())
            if maximum>np.sqrt(.25*float(np.finfo(np.float32).max)/(4*lease.dimension)):
                raise RemoteError(grpc.StatusCode.FAILED_PRECONDITION,'Snapshot exceeds safe Faiss float32 distance range')
            handle,statistics=build_in_worker(ids,vectors,config,deadline,cancelled=lambda:not context.is_active())
            io.remaining()
            consistent(handle.row_count==n and handle.dimension==lease.dimension and handle.nlist==config.nlist,'Built index metadata mismatch')
            statistics.update(estimated_build_bytes=estimate,build_total_ms=(time.monotonic()-start)*1000,
                              export_rpc_calls=trace['counts']['rpc_calls'],export_payload_bytes=trace['counts']['rpc_payload_bytes'])
            try: entry=self.registry.publish(key,handle,request.table,statistics,check=io.remaining)
            except RemoteError: raise
            except ValueError as error: raise RemoteError(grpc.StatusCode.RESOURCE_EXHAUSTED,str(error)) from error
            return dict(entry.describe(),state='ready',reused=False)
        except TimeoutError as error:
            raise RemoteError(grpc.StatusCode.DEADLINE_EXCEEDED,str(error)) from error
        except InterruptedError as error:
            raise RemoteError(grpc.StatusCode.CANCELLED,str(error)) from error
        finally:
            try: io.close()
            finally: self.build_slot.release()
