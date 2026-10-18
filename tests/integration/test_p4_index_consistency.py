from contextlib import ExitStack
from types import SimpleNamespace
import json

import grpc
import numpy as np
import pytest

from vecstor.compute.service import create_compute_server
from vecstor.data import Dataset
from vecstor.datasets import generate_dataset, save_dataset
from vecstor.query.search_options import SearchOptions
from vecstor.rpc import sql_pb2 as pb, sql_pb2_grpc as rpc
from vecstor.rpc.sql_client import SessionClient
from vecstor.storage.sql_service import create_catalog_server

SQL = 'SELECT id,vector_distance(vector,[0,0,0,0]) AS distance FROM items WHERE value>=0 ORDER BY distance LIMIT 10'


@pytest.fixture
def cluster(tmp_path):
    with ExitStack() as stack:
        storage, sport, backend = create_catalog_server(tmp_path, '127.0.0.1:0')
        stack.callback(backend.close); stack.callback(lambda:storage.stop(0).wait())
        storage.start()
        clients, stubs, engines = {}, {}, {}
        for node in ('A', 'B'):
            server, port, engine = create_compute_server(f'127.0.0.1:{sport}', '127.0.0.1:0', timeout=60, index_source='published')
            stack.callback(engine.close); stack.callback(lambda s=server:s.stop(0).wait()); server.start()
            channel = stack.enter_context(grpc.insecure_channel(f'127.0.0.1:{port}'))
            stubs[node] = rpc.SQLComputeStub(channel)
            clients[node] = stack.enter_context(SessionClient(f'127.0.0.1:{port}', timeout=60))
            clients[node].open_database('demo', create=True)
            engines[node] = engine.sql
        original = generate_dataset(2048, 4, seed=27)
        data = Dataset(original.ids+2**53+1, original.categories, original.timestamps, original.values, original.vectors)
        save_dataset(data, tmp_path/'imports/source.npz')
        clients['A'].import_dataset('source.npz', 'items')
        yield SimpleNamespace(clients=clients, stubs=stubs, engines=engines, backend=backend, root=tmp_path,
                              dbid=clients['A'].database_id, data=data, update_id=int(data.ids[np.argmax(np.sum(data.vectors**2,axis=1))]))


def publish(cluster, nlist=1, operation='publish', node='A'):
    return cluster.stubs[node].PublishIndex(pb.PublishIndexRequest(database_id=cluster.dbid, table='items',
        operation_id=operation, nlist=nlist, seed=20261013), timeout=45).publication


def load(cluster, node='A', device='cpu'):
    return cluster.stubs[node].LoadIndex(pb.LoadIndexRequest(database_id=cluster.dbid, table='items', device=device), timeout=45)


def query(cluster, node='A', fallback='error'):
    client = cluster.clients[node]
    client.search_options = SearchOptions(mode='approx', path='search-first', nprobe=1, fallback=fallback)
    return client.execute(SQL)


def test_two_nodes_same_artifact_same_answers(cluster):
    published = publish(cluster)
    for node in ('A','B'): load(cluster, node)
    a, b = query(cluster, 'A'), query(cluster, 'B')
    assert a['rows'] == b['rows']
    for result in (a,b):
        plan = result['execution_plan']
        assert plan['index_source'] == 'published' and plan['actual_mode'] == 'approx'
        assert plan['artifact_id'] == published.descriptor.artifact_id
        assert plan['publication_generation'] == published.generation


def test_write_gap_never_serves_stale_ann(cluster):
    publish(cluster); load(cluster)
    cluster.clients['B'].execute(f'UPDATE items SET vector=[0,0,0,0] WHERE id={cluster.update_id}')
    with pytest.raises(ValueError, match='index_not_published'): query(cluster)
    result = query(cluster, fallback='exact')
    assert result['rows'][0][0] == cluster.update_id
    assert result['execution_plan']['actual_mode'] == 'exact'
    assert result['execution_plan']['artifact_id'] is None


def test_same_revision_head_switch_ignores_local_active(cluster):
    first = publish(cluster)
    load(cluster, 'A'); load(cluster, 'B')
    second = publish(cluster, nlist=2, operation='second')
    assert first.key.revision == second.key.revision
    assert second.generation > first.generation
    with pytest.raises(ValueError, match='published_index_not_loaded'): query(cluster,'B')
    load(cluster,'B')
    assert query(cluster,'B')['execution_plan']['artifact_id'] == second.descriptor.artifact_id


def test_old_query_completes_after_new_head(cluster, monkeypatch):
    from concurrent.futures import ThreadPoolExecutor
    from threading import Event
    first = publish(cluster); load(cluster)
    expected = query(cluster)['rows']
    entered, release = Event(), Event()
    original = cluster.backend.catalog.vectors
    paused = [False]
    def pause(*args, **kwargs):
        if not paused[0]:
            paused[0] = True; entered.set(); assert release.wait(15)
        return original(*args, **kwargs)
    monkeypatch.setattr(cluster.backend.catalog, 'vectors', pause)
    with ThreadPoolExecutor(1) as pool:
        job = pool.submit(query, cluster)
        try:
            assert entered.wait(5)
            cluster.clients['B'].execute(f'UPDATE items SET vector=[0,0,0,0] WHERE id={cluster.update_id}')
            second = publish(cluster, operation='second', node='B'); load(cluster,'B')
            assert query(cluster,'B')['rows'][0][0] == cluster.update_id
        finally: release.set()
        old = job.result(timeout=5)
    assert old['rows'] == expected and old['execution_plan']['artifact_id'] == first.descriptor.artifact_id
    assert second.key.revision > old['revision']


def test_inspect_is_read_only(cluster, monkeypatch):
    publish(cluster)
    def forbidden(*args, **kwargs): pytest.fail('inspection must not download')
    monkeypatch.setattr(cluster.backend.index_service, 'DownloadIndexArtifact', forbidden)
    result = cluster.stubs['A'].InspectPublishedIndex(pb.InspectPublishedIndexRequest(database_id=cluster.dbid, table='items'), timeout=5)
    assert result.state == 'ready' and not result.cpu_ready
    explained = json.loads(cluster.clients['A'].execute('EXPLAIN '+SQL)['rows'][0][0])
    assert explained['counts']['vectors_fetched'] == 0 and explained['artifact_id'] is None


def test_corrupt_publication_errors_but_exact_works(cluster):
    published = publish(cluster); load(cluster)
    database = cluster.backend.catalog.database(cluster.dbid)
    database.pool.submit(lambda:database.connection.execute("UPDATE __vecstor_index_artifacts SET state='CORRUPT' WHERE artifact_id=?", [published.descriptor.artifact_id])).result()
    with pytest.raises(ValueError, match='DATA_LOSS'): query(cluster, fallback='exact')
    cluster.clients['A'].reset_search()
    assert len(cluster.clients['A'].execute(SQL)['rows']) == 10


def test_published_mode_rejects_local_build_and_keeps_crud(cluster):
    with pytest.raises(ValueError, match='publish.*load'): cluster.clients['A'].build_index('items')
    cluster.clients['A'].execute('CREATE TABLE ordinary (id BIGINT)')
    assert cluster.clients['A'].execute('INSERT INTO ordinary VALUES (1)')['affected_rows'] == 1


@pytest.mark.parametrize('corruption', ['artifact','revision'])
def test_client_rejects_inconsistent_published_completion(cluster, monkeypatch, corruption):
    from vecstor.compute import sql_service
    publish(cluster); load(cluster)
    original = sql_service.execute_vector
    def changed(*args, **kwargs):
        result = original(*args, **kwargs)
        plan = result['execution_plan']
        if corruption == 'artifact': plan['artifact_id'] = '0'*64
        else: plan['index']['revision'] -= 1
        return result
    monkeypatch.setattr(sql_service, 'execute_vector', changed)
    with pytest.raises(ValueError, match='artifact|snapshot'): query(cluster)
