"""Explicit publishing/loading, always over RPC and bound to one read lease."""
from contextlib import contextmanager
from dataclasses import asdict
import json
import time
import uuid

import grpc

from vecstor.compute.lease_io import LeaseIO, consistent
from vecstor.index.artifact import ArtifactError, ArtifactInspection, canonical, encode_artifact, require, validate_artifact
from vecstor.index.artifact_worker import working_bytes
from vecstor.index.types import BuildConfig
from vecstor.query.statements import safe_name
from vecstor.rpc import sql_pb2 as pb
from vecstor.rpc.index_codec import descriptor_from_proto, record_from_proto, read_artifact_frames, upload_frames


class IndexPublicationManager:
    def __init__(self, stub, registry, build_from_lease, maintenance_slot, gpu_registry, gpu_worker):
        self.stub, self.registry, self.build_from_lease = stub, registry, build_from_lease
        self.maintenance_slot, self.gpu_registry, self.gpu_worker = maintenance_slot, gpu_registry, gpu_worker

    @contextmanager
    def maintenance(self):
        require(self.maintenance_slot.acquire(blocking=False), 'index_maintenance_busy', 'RESOURCE_EXHAUSTED')
        try: yield
        finally: self.maintenance_slot.release()

    def _io(self, request, context):
        safe_name(request.table)
        return LeaseIO(self.stub, request.database_id, request.table, context,
                       time.monotonic()+min(60, context.time_remaining() or 45), {})

    def _capabilities(self, io):
        try:
            response = io.call(self.stub.Control, pb.ControlRequest(action='capabilities'))
            capability = json.loads(response.json_result)
        except grpc.RpcError as error:
            if error.code() == grpc.StatusCode.UNIMPLEMENTED:
                raise ArtifactError('FAILED_PRECONDITION', 'storage_index_publication_unsupported') from error
            raise
        require(capability.get('index_publication_version') == 1, 'storage_index_publication_unsupported', 'FAILED_PRECONDITION')

    @staticmethod
    def _lease_descriptor(lease, required=True):
        require(lease.index_publication_version == 1, 'storage_publication_lease_unsupported', 'FAILED_PRECONDITION')
        descriptor = descriptor_from_proto(lease.published_index) if lease.HasField('published_index') else None
        if descriptor:
            consistent(descriptor.key.snapshot == (lease.database_id, lease.table_id, lease.revision,
                lease.schema_version, lease.dimension, 'squared_l2'), 'Published index does not match read snapshot')
        if required:
            require(descriptor is not None, 'index_not_published', 'FAILED_PRECONDITION')
            require(lease.published_index_state == 'ready', 'published_artifact_corrupt', 'DATA_LOSS')
        return descriptor

    @staticmethod
    def _result(record):
        if record.state in ('FAILED', 'ABORTED'):
            raise ArtifactError(record.error['code'], record.error['reason'])
        require(record.state == 'COMMITTED', 'index_publication_in_progress', 'RESOURCE_EXHAUSTED')
        return record

    def publish(self, request, context):
        with self.maintenance():
            io = self._io(request, context)
            statistics = {'trained':False, 'artifact_upload_bytes':0}
            try:
                self._capabilities(io)
                intent = dict(database_id=request.database_id, table=request.table,
                    nlist=request.nlist if request.HasField('nlist') else None,
                    seed=request.seed if request.HasField('seed') else 20261013,
                    expected_generation=request.expected_generation if request.HasField('expected_generation') else None,
                    protocol_version=1)
                old = record_from_proto(io.call(self.stub.GetIndexPublication,
                    pb.IndexPublicationRequest(database_id=request.database_id, operation_id=request.operation_id)))
                if old is not None:
                    require(old.operation_kind == 'PUBLISH' and old.request_intent == intent, 'operation_id_request_conflict', 'ALREADY_EXISTS')
                    return self._result(old), dict(statistics, counts=io.trace['counts'])
                lease = io.open()
                self._lease_descriptor(lease, required=False)
                config = BuildConfig(nlist=intent['nlist'], seed=intent['seed']).resolved(lease.row_count)
                generation = intent['expected_generation'] if intent['expected_generation'] is not None else lease.publication_generation
                record = record_from_proto(io.call(self.stub.BeginIndexPublication, pb.BeginIndexPublicationRequest(
                    database_id=request.database_id, token=lease.token, operation_id=request.operation_id,
                    config=pb.IndexBuildConfig(**asdict(config)), expected_generation=generation, intent_json=canonical(intent).decode())))
                if record.state != 'RESERVED': return self._result(record), statistics
                if record.descriptor is None:
                    handle, build_statistics = self.build_from_lease(io, config)
                    descriptor, payload = encode_artifact(handle, record.key, config, build_statistics['snapshot_sha256'])
                    io.remaining()
                    statistics.update(build_statistics, trained=True, artifact_upload_bytes=len(payload))
                    frames = upload_frames(request.database_id, lease.token, request.operation_id, descriptor, payload, io.remaining)
                    def measured_frames():
                        for frame in frames:
                            io.trace['counts']['rpc_payload_bytes'] += frame.ByteSize()
                            yield frame
                    io.trace['counts']['rpc_calls'] += 1
                    future = self.stub.UploadIndexArtifact.future(measured_frames(), timeout=io.remaining())
                    context.add_callback(future.cancel)
                    try: uploaded = future.result()
                    except grpc.FutureCancelledError as error: raise InterruptedError('artifact upload cancelled') from error
                    io.trace['counts']['rpc_payload_bytes'] += uploaded.ByteSize()
                    require(uploaded.state == 'STAGED', 'artifact_upload_not_acknowledged', 'DATA_LOSS')
                record = record_from_proto(io.call(self.stub.FinalizeIndexPublication, pb.FinalizeIndexPublicationRequest(
                    database_id=request.database_id, token=lease.token, operation_id=request.operation_id)))
                return self._result(record), dict(statistics, counts=io.trace['counts'])
            finally: io.close()

    def load(self, request, context):
        require(request.device in ('', 'cpu','gpu') and request.gpu_id == 0, 'artifact_load_device_unsupported', 'FAILED_PRECONDITION')
        with self.maintenance():
            io = self._io(request, context)
            try:
                self._capabilities(io)
                lease = io.open()
                descriptor = self._lease_descriptor(lease)
                require(not request.expected_artifact_id or request.expected_artifact_id == descriptor.artifact_id,
                        'expected_artifact_mismatch', 'FAILED_PRECONDITION')
                statistics = dict(trained=False, artifact_download_bytes=0, snapshot_calls=0, gpu_cloned=False, counts=io.trace['counts'])
                reused = self._load_cpu(io, request, descriptor, statistics)
                if request.device=='gpu': reused = self._load_gpu(io, request, descriptor, statistics)
                return dict(descriptor=descriptor, publication_generation=lease.publication_generation,
                            device=request.device or 'cpu', reused=reused, statistics=statistics)
            finally: io.close()

    def _load_cpu(self, io, request, descriptor, statistics):
        with self.registry.acquire_exact(descriptor.key, descriptor.artifact_id) as existing:
            if existing: return True
        self.registry.check_artifact_slot(descriptor)
        try: self.registry.prepare_build(descriptor.key, working_bytes(descriptor), check=io.remaining)
        except ValueError as error: raise ArtifactError('RESOURCE_EXHAUSTED', str(error)) from error
        started = time.monotonic()
        frames = io.stream(self.stub.DownloadIndexArtifact, pb.DownloadIndexArtifactRequest(
            database_id=request.database_id, token=io.lease.token, artifact_id=descriptor.artifact_id))
        _, payload = read_artifact_frames(frames, request.database_id, io.lease.token, descriptor, io.remaining)
        statistics.update(artifact_download_bytes=len(payload), download_ms=(time.monotonic()-started)*1000)
        started = time.monotonic()
        handle = validate_artifact(payload, descriptor)
        io.remaining()
        statistics['deserialize_ms'] = (time.monotonic()-started)*1000
        try: self.registry.publish_artifact(descriptor, handle, request.table,
            dict(source='published', publication_generation=io.lease.publication_generation), io.remaining)
        except ArtifactError: raise
        except ValueError as error: raise ArtifactError('RESOURCE_EXHAUSTED', str(error)) from error
        return False

    def _load_gpu(self, io, request, descriptor, statistics):
        from vecstor.index.gpu_backend import GpuIndex
        from vecstor.index.gpu_worker import GpuError, GpuDataError
        with self.registry.acquire_exact(descriptor.key, descriptor.artifact_id) as cpu:
            require(cpu is not None, 'published_cpu_index_not_loaded', 'FAILED_PRECONDITION')
            with self.gpu_registry.acquire_exact(descriptor.key, descriptor.artifact_id) as existing:
                if existing:
                    try:
                        actual = self.gpu_worker.verify_replica(existing.handle.token,io.deadline,cancelled=lambda:not io.context.is_active())
                        if any(actual.get(name)!=getattr(cpu.handle,name) for name in ('row_count','dimension','nlist')):
                            raise GpuDataError('GPU artifact replica metadata mismatch')
                        return True
                    except GpuError as error:
                        reason = str(error)
                        if reason not in ('gpu_replica_missing','gpu_worker_generation_changed') and not reason.startswith('gpu_worker_failed: '):raise
                        if self.gpu_worker.generation==existing.handle.worker_generation:self.gpu_worker.reset()
            self.gpu_registry.check_artifact_slot(descriptor)
            self.gpu_registry.reserve(cpu.handle.byte_size)
            token = uuid.uuid4().hex
            generation = self.gpu_worker.generation
            published = False
            started = time.monotonic()
            try:
                stats = self.gpu_worker.submit_clone(token,cpu.handle.serialize(),io.deadline,cancelled=lambda:not io.context.is_active())
                if any(stats.get(name)!=getattr(cpu.handle,name) for name in ('row_count','dimension','nlist')):
                    raise GpuDataError('GPU artifact clone metadata mismatch')
                io.remaining()
                require(generation==self.gpu_worker.generation,'gpu_worker_generation_changed','FAILED_PRECONDITION')
                handle = GpuIndex(self.gpu_worker,token,cpu.handle,generation,descriptor.artifact_id)
                for name in ('row_count','dimension','nlist'):stats.pop(name,None)
                stats.update(device='gpu',gpu_id=0,precision='float32',worker_generation=generation,source='published')
                self.gpu_registry.publish_artifact(descriptor,handle,request.table,stats,io.remaining)
                published = True
                statistics.update(gpu_cloned=True,gpu_clone_ms=(time.monotonic()-started)*1000,worker_generation=generation)
                return False
            finally:
                if not published:
                    try:self.gpu_worker.release(token)
                    except (ValueError,RuntimeError):pass

    def inspect(self, request, context):
        io = self._io(request, context)
        try:
            lease = io.open()
            descriptor = self._lease_descriptor(lease, required=False)
            cpu_ready = gpu_ready = False
            if descriptor and lease.published_index_state == 'ready':
                with self.registry.acquire_exact(descriptor.key, descriptor.artifact_id) as entry: cpu_ready = entry is not None
                with self.gpu_registry.acquire_exact(descriptor.key, descriptor.artifact_id) as entry: gpu_ready = entry is not None
            return ArtifactInspection(descriptor, lease.publication_generation, cpu_ready, gpu_ready, lease.published_index_state)
        finally: io.close()
