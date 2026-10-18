import grpc
import pytest

from vecstor.rpc import sql_pb2 as pb
from vecstor.rpc.index_codec import descriptor_from_proto
from test_p4_index_consistency import cluster, publish


def prune(cluster, operation='gc', execute=True):
    return cluster.stubs['A'].PruneIndexArtifacts(pb.PruneIndexArtifactsRequest(database_id=cluster.dbid,
        operation_id=operation,keep_last=1,dry_run=not execute),timeout=10)


def test_gc_protects_old_lease_and_download_pin(cluster):
    first=publish(cluster)
    lease=cluster.backend.catalog.begin_read(cluster.dbid,'items',ttl=60)
    publish(cluster,nlist=2,operation='second')
    assert first.descriptor.artifact_id in prune(cluster,execute=False).protected
    store=cluster.backend.index_service.store
    descriptor=descriptor_from_proto(first.descriptor)
    with store.pin(descriptor):
        cluster.backend.catalog.end_read(cluster.dbid,lease['token'])
        assert descriptor.artifact_id in prune(cluster,execute=False).protected
    result=prune(cluster)
    assert list(result.deleted)==[descriptor.artifact_id]
    assert not store.prepared(descriptor)


def test_prune_replay_keeps_original_set_and_tombstone(cluster):
    first=publish(cluster)
    publish(cluster,nlist=2,operation='second')
    result=prune(cluster)
    publish(cluster,nlist=3,operation='third')
    assert prune(cluster)==result
    assert first.descriptor.artifact_id in result.deleted
    # Rebuilding the same deterministic artifact may restore its exact identity.
    restored=publish(cluster,nlist=1,operation='restore')
    assert restored.descriptor.artifact_id==first.descriptor.artifact_id


def test_prune_defaults_to_read_only(cluster):
    first=publish(cluster); publish(cluster,nlist=2,operation='second')
    result=cluster.stubs['A'].PruneIndexArtifacts(pb.PruneIndexArtifactsRequest(database_id=cluster.dbid,keep_last=1),timeout=5)
    assert first.descriptor.artifact_id in result.candidates
    assert not result.deleted and result.state=='DRY_RUN'


def test_prune_requires_operation_and_valid_retention(cluster):
    publish(cluster)
    for request in (pb.PruneIndexArtifactsRequest(database_id=cluster.dbid,keep_last=0,dry_run=True),
                    pb.PruneIndexArtifactsRequest(database_id=cluster.dbid,keep_last=1,dry_run=False)):
        with pytest.raises(grpc.RpcError) as error:cluster.stubs['A'].PruneIndexArtifacts(request,timeout=5)
        assert error.value.code()==grpc.StatusCode.INVALID_ARGUMENT
