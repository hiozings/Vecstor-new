"""Explicit, version-bound index construction; no query-time hidden training."""

import time
import uuid
from threading import BoundedSemaphore

import grpc
import numpy as np

from vecstor.compute.lease_io import LeaseIO, consistent
from vecstor.index.types import BuildConfig, IndexKey
from vecstor.index.registry import IndexRegistry
from vecstor.index.build_worker import build_in_worker
from vecstor.rpc.common import RemoteError


class IndexManager:
    def __init__(self,stub,max_bytes=256*1024*1024,max_entries=8,gpu_options=None):
        self.stub=stub
        self.registry=IndexRegistry(max_bytes,max_entries)
        self.build_slot=BoundedSemaphore(1)
        from vecstor.index.gpu_worker import GpuWorker
        from vecstor.index.gpu_registry import GpuRegistry
        settings=dict(gpu_options or {})
        gpu_bytes=settings.pop('max_bytes',512*1024**2);gpu_entries=settings.pop('max_entries',8)
        self.gpu_worker=GpuWorker(**settings)
        self.gpu_registry=GpuRegistry(self.gpu_worker,gpu_bytes,gpu_entries)
        self.gpu_build_slot=BoundedSemaphore(1)

    def close(self):self.gpu_worker.close()

    def build_from_lease(self, io, config):
        from vecstor.index.artifact import SnapshotHasher
        lease = io.lease
        n = lease.row_count
        key = IndexKey.from_lease(lease, config)
        start = time.monotonic()
        estimate = n*(4*lease.dimension+8)*4+min(n, config.train_rows)*lease.dimension*4
        try: self.registry.prepare_build(key, estimate, check=io.remaining)
        except RemoteError: raise
        except ValueError as error: raise RemoteError(grpc.StatusCode.RESOURCE_EXHAUSTED, str(error)) from error
        ids = np.empty(n, dtype=np.int64)
        vectors = np.empty((n, lease.dimension), dtype=np.float32)
        hasher = SnapshotHasher(n, lease.dimension)
        offset = 0
        receipt = ''
        export_started=time.monotonic()
        for labels, values in io.snapshot():
            ids[offset:offset+len(labels)] = labels
            vectors[offset:offset+len(labels)] = values
            hasher.add(labels, values)
            offset += len(labels)
        consistent(offset == n, 'Incomplete build input')
        export_ms=(time.monotonic()-export_started)*1000
        receipt = hasher.finish()
        if lease.index_publication_version:
            consistent(getattr(io, 'snapshot_sha256', None) == receipt, 'Snapshot digest mismatch')
        maximum = float(np.abs(vectors).max())
        if maximum > np.sqrt(.25*float(np.finfo(np.float32).max)/(4*lease.dimension)):
            raise RemoteError(grpc.StatusCode.FAILED_PRECONDITION, 'Snapshot exceeds safe Faiss float32 distance range')
        handle, statistics = build_in_worker(ids, vectors, config, io.deadline, cancelled=lambda:not io.context.is_active())
        io.remaining()
        consistent(handle.row_count == n and handle.dimension == lease.dimension and handle.nlist == config.nlist, 'Built index metadata mismatch')
        statistics.update(estimated_build_bytes=estimate, build_total_ms=(time.monotonic()-start)*1000,
                          export_rpc_calls=io.trace['counts']['rpc_calls'], export_payload_bytes=io.trace['counts']['rpc_payload_bytes'],
                          snapshot_sha256=receipt,export_ms=export_ms)
        return handle, statistics

    def build(self,request,context):
        if request.device not in ('','cpu','gpu') or request.gpu_id!=0:raise ValueError('Invalid index device')
        if request.device!='gpu':return dict(self._build_cpu(request,context),device='cpu')
        if not self.gpu_build_slot.acquire(blocking=False):raise RemoteError(grpc.StatusCode.RESOURCE_EXHAUSTED,'GPU clone already running')
        io=None;token=None;published=False
        try:
            cpu_result=self._build_cpu(request,context)
            if cpu_result['state']!='ready':raise RemoteError(grpc.StatusCode.FAILED_PRECONDITION,'GPU requires an IVF index (at least 1024 rows)')
            deadline=time.monotonic()+min(60,context.time_remaining() or 45)
            io=LeaseIO(self.stub,request.database_id,request.table,context,deadline,{})
            lease=io.open()
            with self.registry.acquire(lease) as cpu:
                if cpu is None or cpu.key.build_fingerprint!=cpu_result['build_fingerprint']:raise RemoteError(grpc.StatusCode.FAILED_PRECONDITION,'gpu_build_snapshot_changed; CPU build retained')
                resident=self.gpu_registry.reactivate(cpu.key,check=io.remaining)
                if resident:
                    from vecstor.index.gpu_worker import GpuError,GpuDataError
                    try:
                        actual=self.gpu_worker.verify_replica(resident.handle.token,deadline,cancelled=lambda:not context.is_active())
                        if any(actual.get(name)!=getattr(resident.handle,name) for name in ('row_count','dimension','nlist')):
                            raise GpuDataError('GPU replica metadata mismatch')
                        return dict(resident.describe(),state='ready',reused=True)
                    except GpuError as error:
                        # Admission rejection does not imply a broken replica.
                        reason=str(error)
                        if reason not in ('gpu_replica_missing','gpu_worker_generation_changed') and not reason.startswith('gpu_worker_failed: '):
                            raise
                        # The dispatcher may already have retired this generation.
                        if self.gpu_worker.generation==resident.handle.worker_generation:
                            self.gpu_worker.reset()

                self.gpu_registry.reserve(cpu.handle.byte_size)
                token=uuid.uuid4().hex;generation=self.gpu_worker.generation
                stats=self.gpu_worker.submit_clone(token,cpu.handle.serialize(),deadline,cancelled=lambda:not context.is_active())
                for field in ('row_count','dimension','nlist'):stats.pop(field,None)
                io.remaining()
                if generation!=self.gpu_worker.generation:raise ValueError('gpu_worker_generation_changed')
                from vecstor.index.gpu_backend import GpuIndex
                handle=GpuIndex(self.gpu_worker,token,cpu.handle,generation)
                entry=self.gpu_registry.publish(cpu.key,handle,request.table,dict(stats,device='gpu',gpu_id=0,precision='float32',worker_generation=generation),check=io.remaining)
                published=True
                return dict(entry.describe(),state='ready',reused=False)
        except TimeoutError as error:raise RemoteError(grpc.StatusCode.DEADLINE_EXCEEDED,str(error)) from error
        except InterruptedError as error:raise RemoteError(grpc.StatusCode.CANCELLED,str(error)) from error
        except ValueError as error:raise RemoteError(grpc.StatusCode.RESOURCE_EXHAUSTED,'GPU clone failed; CPU index retained: '+str(error)) from error
        finally:
            try:
                if token and not published:
                    try:self.gpu_worker.release(token)
                    except ValueError:pass
                if io:io.close()
            finally:self.gpu_build_slot.release()

    def _build_cpu(self,request,context):
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
            io.lease, io.context, io.deadline, io.trace = lease, context, deadline, trace
            handle, statistics = self.build_from_lease(io, config)
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
