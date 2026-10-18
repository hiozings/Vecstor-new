import json
import grpc
import pytest

from vecstor.rpc import sql_pb2 as pb
from test_p4_index_consistency import cluster, publish, load, SQL


GPU_SQL = SQL + " SETTINGS mode='approx',path='search-first',device='gpu',nprobe=1,fallback='error'"


def test_gpu_restore_clones_loaded_cpu_without_training(cluster, monkeypatch):
    publication = publish(cluster)
    from vecstor.compute import index_manager
    monkeypatch.setattr(index_manager, 'build_in_worker', lambda *a, **k:pytest.fail('GPU load must not train'))
    restored = load(cluster, device='gpu')
    assert restored.descriptor.artifact_id == publication.descriptor.artifact_id
    stats = json.loads(restored.statistics_json)
    assert stats['trained'] is False and stats['gpu_cloned'] is True
    plan = cluster.clients['A'].execute(GPU_SQL)['execution_plan']
    assert plan['actual_device'] == 'gpu' and plan['artifact_id'] == publication.descriptor.artifact_id


def test_dead_worker_reload_preserves_published_cpu(cluster, monkeypatch):
    publication = publish(cluster); load(cluster, device='gpu')
    worker = cluster.engines['A'].index_manager.gpu_worker
    generation = worker.generation
    worker.reset()
    from vecstor.compute import index_manager
    monkeypatch.setattr(index_manager, 'build_in_worker', lambda *a, **k:pytest.fail('worker recovery must not train'))
    restored = load(cluster, device='gpu')
    assert worker.generation > generation
    stats = json.loads(restored.statistics_json)
    assert stats['artifact_download_bytes'] == 0 and stats['gpu_cloned'] is True
    assert cluster.clients['A'].execute(GPU_SQL)['execution_plan']['artifact_id'] == publication.descriptor.artifact_id


def test_gpu_fallback_cannot_choose_other_artifact(cluster):
    publish(cluster); load(cluster, device='gpu')
    publish(cluster, nlist=2, operation='other')
    with pytest.raises(ValueError, match='published_index_not_loaded'):
        cluster.clients['A'].execute(GPU_SQL+",device_fallback='cpu'")
    load(cluster)
    result = cluster.clients['A'].execute(GPU_SQL+",device_fallback='cpu'")
    assert result['execution_plan']['actual_device'] == 'cpu'


def test_published_gpu_budget_failure_is_resource_exhausted(cluster):
    publish(cluster)
    cluster.engines['A'].index_manager.gpu_registry.max_bytes=1
    with pytest.raises(grpc.RpcError) as error:load(cluster,device='gpu')
    assert error.value.code()==grpc.StatusCode.RESOURCE_EXHAUSTED
    inspection=cluster.stubs['A'].InspectPublishedIndex(pb.InspectPublishedIndexRequest(database_id=cluster.dbid,table='items'),timeout=5)
    assert inspection.cpu_ready and not inspection.gpu_ready
