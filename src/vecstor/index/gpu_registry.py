"""GPU replicas use the logical snapshot identity and a worker generation."""
from contextlib import contextmanager
from .registry import IndexRegistry


class GpuRegistry(IndexRegistry):
    def __init__(self,worker,max_bytes=512*1024**2,max_entries=8):
        super().__init__(max_bytes,max_entries);self.worker=worker

    def _sync(self):
        with self._lock:
            for key,entry in list(self._entries.items()):
                if entry.handle.worker_generation!=self.worker.generation:super()._remove(key)

    def _remove(self,key):
        entry=self._entries[key]
        if entry.handle.worker_generation==self.worker.generation:self.worker.release(entry.handle.token)
        super()._remove(key)

    def reserve(self,size):
        self._sync()
        with self._lock:
            if size>self.max_bytes:raise ValueError('GPU index budget exhausted')
            victims=[];total=self.total_bytes;count=len(self._entries)
            for key,entry in self._entries.items():
                if total+size<=self.max_bytes and count+1<=self.max_entries:break
                if not entry.pins:victims.append(key);total-=entry.handle.byte_size;count-=1
            if total+size>self.max_bytes or count+1>self.max_entries:raise ValueError('GPU pinned index budget exhausted')
            for key in victims:self._remove(key)

    @contextmanager
    def acquire(self,lease):
        self._sync()
        with super().acquire(lease) as entry:
            if entry:
                with self._lock:self._entries.move_to_end(entry.key)
            yield entry

    def reactivate(self,key,check=None):
        self._sync();return super().reactivate(key,check)

    @contextmanager
    def acquire_exact(self,key,artifact_id):
        self._sync()
        with super().acquire_exact(key,artifact_id) as entry:
            if entry:
                with self._lock:self._entries.move_to_end(key)
            yield entry

    def publish_artifact(self,descriptor,handle,table,statistics,check):
        from .artifact import require
        self._sync()
        require(handle.artifact_id==descriptor.artifact_id and handle.worker_generation==self.worker.generation,
                'gpu_artifact_generation_mismatch','FAILED_PRECONDITION')
        return super().publish_artifact(descriptor,handle,table,statistics,check)

    def list(self,database_id='',table=''):
        self._sync();return super().list(database_id,table)

    def clear(self,database_id,table=''):
        self._sync();return super().clear(database_id,table)
