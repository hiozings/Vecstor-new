"""Real DuckDB transactions define the publication linearization point."""
from dataclasses import replace
import time

import duckdb
import numpy as np
import pytest

from vecstor.datasets import generate_dataset, save_dataset
from vecstor.index.artifact import ArtifactError, SnapshotHasher, encode_artifact
from vecstor.index.faiss_backend import FaissIndex
from vecstor.index.types import BuildConfig, IndexKey
from vecstor.storage.catalog import DatabaseCatalog
from vecstor.storage.index_catalog import IndexCatalog


class PreparedStore:
    """Only the prepared-object boundary is fake; metadata uses real DuckDB."""
    def __init__(self): self.objects = {}
    def prepared(self, descriptor): return self.objects.get(descriptor.artifact_id) == descriptor
    def abort_later(self, operation): pass
    def statistics(self, descriptor): return {}


@pytest.fixture
def catalog(tmp_path):
    store = PreparedStore()
    with DatabaseCatalog(tmp_path, artifact_store=store) as catalog:
        dbid = catalog.open_database('demo', True)['id']
        data = generate_dataset(2048, 4, seed=19)
        save_dataset(data, tmp_path/'imports/source.npz')
        catalog.import_dataset(dbid, 'source.npz', 'items', 'import')
        yield catalog, catalog.database(dbid), data, store


def prepare(fixture, operation='publish1', nlist=1):
    catalog, db, data, store = fixture
    lease = db.call('begin_read', 'items', 60, True)
    config = BuildConfig(nlist=nlist).resolved(data.size)
    intent = dict(database_id=db.id, table='items', nlist=nlist, seed=config.seed,
                  expected_generation=None, protocol_version=1)
    record = db.call('begin_index_publication', lease['token'], operation, config,
                     lease['publication_generation'], intent)
    hasher = SnapshotHasher(data.size, data.dimension)
    hasher.add(data.ids, data.vectors)
    descriptor, _ = encode_artifact(FaissIndex.build(data.ids, data.vectors, config),
        IndexKey(db.id, lease['table_id'], lease['revision'], lease['schema_version'], data.dimension,
                 build_fingerprint=config.fingerprint()), config, hasher.finish())
    db.call('record_snapshot_digest', lease['token'], hasher.finish(), data.size)
    db.call('bind_index_upload', operation, descriptor)
    db.call('index_phase', operation, 'STAGED')
    db.call('index_phase', operation, 'VALIDATING')
    store.objects[descriptor.artifact_id] = descriptor
    return lease, descriptor, record


def commit(fixture, operation, lease, descriptor):
    return fixture[1].call('commit_index_publication', operation, descriptor, lease['token'])


def test_publication_does_not_advance_data_revision(catalog):
    lease, descriptor, _ = prepare(catalog)
    result = commit(catalog, 'publish1', lease, descriptor)
    current = catalog[1].call('begin_read', 'items', 60, True)
    assert current['revision'] == lease['revision']
    assert result.generation == 1
    assert current['published_index']['artifact_id'] == descriptor.artifact_id


def test_old_lease_keeps_head_on_same_revision_switch(catalog):
    first, descriptor, _ = prepare(catalog)
    commit(catalog, 'publish1', first, descriptor)
    old = catalog[1].call('begin_read', 'items', 60, True)
    second, other, _ = prepare(catalog, 'publish2', 2)
    commit(catalog, 'publish2', second, other)
    old_head = catalog[1].call('leased_index', old['token'])
    current = catalog[1].call('begin_read', 'items', 60, True)
    assert old_head['published_index']['artifact_id'] == descriptor.artifact_id
    assert current['published_index']['artifact_id'] == other.artifact_id
    assert current['revision'] == old['revision']


def test_same_operation_replay_and_conflict(catalog):
    lease, descriptor, record = prepare(catalog)
    first = commit(catalog, 'publish1', lease, descriptor)
    catalog[1].call('end_read', lease['token'])
    assert commit(catalog, 'publish1', lease, descriptor) == first
    new = catalog[1].call('begin_read', 'items', 60, True)
    with pytest.raises(ArtifactError, match='operation_id'):
        catalog[1].call('begin_index_publication', new['token'], 'publish1', BuildConfig(nlist=2), 1,
                        dict(record.request_intent, nlist=2))
    assert catalog[1].call('get_index_publication', 'publish1').state == 'COMMITTED'


@pytest.mark.parametrize('sql', ['CREATE TABLE other (id BIGINT)', 'DROP TABLE items'])
def test_commit_after_other_table_write_aborts(catalog, sql):
    lease, descriptor, _ = prepare(catalog)
    catalog[0].execute(catalog[1].id, sql, 'change')
    with pytest.raises(ArtifactError, match='snapshot_changed'):
        commit(catalog, 'publish1', lease, descriptor)
    assert catalog[1].call('get_index_publication', 'publish1').state == 'ABORTED'


def test_drop_recreate_same_name_changes_identity(catalog):
    lease, descriptor, _ = prepare(catalog)
    commit(catalog, 'publish1', lease, descriptor)
    cat, db, _, _ = catalog
    cat.execute(db.id, 'DROP TABLE items', 'drop')
    cat.import_dataset(db.id, 'source.npz', 'items', 'again')
    fresh = db.call('begin_read', 'items', 60, True)
    assert fresh['table_id'] != lease['table_id']
    assert fresh['published_index_state'] == 'missing'


def test_ended_lease_releases_publication_slot(catalog):
    lease, _, _ = prepare(catalog)
    catalog[1].call('end_read', lease['token'])
    assert catalog[1].call('get_index_publication', 'publish1').state == 'ABORTED'
    prepare(catalog, 'next')


def test_unprepared_object_never_gets_a_head(catalog):
    lease, descriptor, _ = prepare(catalog)
    catalog[3].objects.clear()
    with pytest.raises(ArtifactError): commit(catalog, 'publish1', lease, descriptor)
    assert catalog[1].call('begin_read', 'items', 60, True)['published_index_state'] == 'missing'


def test_unknown_catalog_schema_rejected():
    with duckdb.connect(':memory:') as connection:
        connection.execute('CREATE TABLE __vecstor_index_meta (schema_version BIGINT, publication_generation BIGINT)')
        connection.execute('INSERT INTO __vecstor_index_meta VALUES (999, 0)')
        with pytest.raises(ArtifactError, match='schema'):
            IndexCatalog(connection, PreparedStore()).initialize()


def test_deleted_key_tombstone_rejects_different_native_bytes(catalog):
    lease, first, record = prepare(catalog)
    commit(catalog,'publish1',lease,first)
    cat,db,data,store=catalog
    db.call('end_read',lease['token'])
    def retire():
        db.connection.execute("UPDATE __vecstor_index_artifacts SET state='DELETED'")
        db.connection.execute('DELETE FROM __vecstor_index_heads')
    db.pool.submit(retire).result()
    lease=db.call('begin_read','items',60,True)
    config=first.build_config
    handle=FaissIndex.build(data.ids,data.vectors,config)
    handle._index.quantizer.reset()
    handle._index.quantizer.add(np.zeros((1,data.dimension),dtype=np.float32))
    changed,_=encode_artifact(handle,first.key,config,first.snapshot_sha256)
    assert changed.artifact_id!=first.artifact_id
    db.call('begin_index_publication',lease['token'],'changed',config,0,record.request_intent)
    db.call('record_snapshot_digest',lease['token'],first.snapshot_sha256,data.size)
    db.call('bind_index_upload','changed',changed)
    db.call('index_phase','changed','STAGED');db.call('index_phase','changed','VALIDATING')
    store.objects[changed.artifact_id]=changed
    with pytest.raises(ArtifactError,match='index_key_content_conflict'):
        commit(catalog,'changed',lease,changed)
    assert db.call('begin_read','items',60,True)['published_index_state']=='missing'
