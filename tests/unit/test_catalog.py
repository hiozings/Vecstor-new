import time
import uuid

import numpy as np
import pytest

DDL = "CREATE TABLE items (id BIGINT PRIMARY KEY, category VARCHAR NOT NULL, timestamp BIGINT NOT NULL, value DOUBLE NOT NULL, vector FLOAT[2] NOT NULL)"
INSERT = "INSERT INTO items VALUES (7, '论文', 1152921504606846976, 12, [1,0]), (2, 'book', -1, 5, [-1,0])"


def operate(catalog, database, sql, operation=None):
    return catalog.execute(database, sql, operation or str(uuid.uuid4()))


def test_persistent_crud_and_database_selection(tmp_path):
    from vecstor.storage.catalog import DatabaseCatalog
    with DatabaseCatalog(tmp_path) as catalog:
        first = catalog.open_database("demo", create=True)
        second = catalog.open_database("other", create=True)
        operate(catalog, first["id"], DDL)
        operate(catalog, first["id"], INSERT)
        operate(catalog, second["id"], "CREATE TABLE items (id BIGINT)")
        operate(catalog, second["id"], "INSERT INTO items VALUES (99)")
        operate(catalog, first["id"], "UPDATE items SET value=value+1, vector=[0,0] WHERE id=7")
        changed = operate(catalog, first["id"], "DELETE FROM items WHERE id=2")
        assert changed["affected_rows"] == 1
        assert operate(catalog, second["id"], "SELECT id FROM items")["rows"] == [[99]]
    with DatabaseCatalog(tmp_path) as reopened:
        assert reopened.open_database("demo")["id"] == first["id"]
        result = operate(reopened, first["id"], "SELECT id, timestamp, value, vector FROM items")
        assert result["rows"] == [[7, 2**60, 13.0, [0.0, 0.0]]]


def test_idempotent_write_and_durable_status(tmp_path):
    from vecstor.storage.catalog import DatabaseCatalog
    with DatabaseCatalog(tmp_path) as c:
        database = c.open_database("demo", True)["id"]
        operate(c, database, "CREATE TABLE counter (n BIGINT)")
        first = operate(c, database, "INSERT INTO counter VALUES (1)", "once")
        second = operate(c, database, "INSERT INTO counter VALUES (1)", "once")
        assert first == second
        with pytest.raises(ValueError):
            operate(c, database, "INSERT INTO counter VALUES (2)", "once")
    with DatabaseCatalog(tmp_path) as c:
        assert c.status(database, "once")["status"] == "COMMITTED"
        assert operate(c, database, "SELECT COUNT(*) FROM counter")["rows"] == [[1]]
        assert c.status(database, "never")["status"] == "UNKNOWN"


def test_failed_operation_id_cannot_be_rebound_after_restart(tmp_path):
    from vecstor.storage.catalog import DatabaseCatalog
    with DatabaseCatalog(tmp_path) as c:
        database=c.open_database("demo",True)["id"]
        operate(c,database,"CREATE TABLE counter (n BIGINT)")
        with pytest.raises(ValueError): operate(c,database,"INSERT INTO counter VALUES ('bad')","failed")
    with DatabaseCatalog(tmp_path) as c:
        assert c.status(database,"failed")["status"]=="FAILED"
        with pytest.raises(ValueError): operate(c,database,"INSERT INTO counter VALUES (1)","failed")
        assert operate(c,database,"SELECT COUNT(*) FROM counter")["rows"]==[[0]]


def test_read_timeout_releases_database_queue(tmp_path):
    from vecstor.storage.catalog import DatabaseCatalog
    with DatabaseCatalog(tmp_path) as c:
        database=c.open_database("demo",True)["id"]
        db=c.database(database)
        # Test-only setup avoids a million literals in the public SQL grammar.
        db.pool.submit(lambda: db.connection.execute("CREATE TABLE big AS SELECT range AS id FROM range(1000000)")).result()
        with pytest.raises(TimeoutError):
            c.execute(database,"SELECT * FROM big","",timeout=0.005)
        assert c.execute(database,"SELECT COUNT(*) FROM big","",timeout=0.2)["rows"]==[[1000000]]


@pytest.mark.parametrize("statement", [
    "INSERT INTO items VALUES (7, 'dup', 0, 1, [0,0])",
    "INSERT INTO items VALUES (-1, 'bad', 0, 1, [0,0])",
    "UPDATE items SET vector=[1,2,3] WHERE id=7",
    "UPDATE items SET vector=[NULL,0] WHERE id=7",
    "UPDATE items SET category='' WHERE id=7",
    "UPDATE items SET value=1e308*1e308 WHERE id=7",
])
def test_invalid_write_rolls_back_and_does_not_advance_revision(tmp_path, statement):
    from vecstor.storage.catalog import DatabaseCatalog
    with DatabaseCatalog(tmp_path) as c:
        database = c.open_database("demo", True)["id"]
        operate(c, database, DDL); operate(c, database, INSERT)
        before = operate(c, database, "SELECT * FROM items ORDER BY id")
        with pytest.raises(ValueError):
            operate(c, database, statement)
        after = operate(c, database, "SELECT * FROM items ORDER BY id")
        assert after["rows"] == before["rows"] and after["revision"] == before["revision"]


def test_read_lease_keeps_snapshot_across_update_and_delete(tmp_path):
    from vecstor.storage.catalog import DatabaseCatalog
    sql = "SELECT id FROM items ORDER BY vector_distance(vector, [0,0]) LIMIT 2"
    with DatabaseCatalog(tmp_path) as c:
        database = c.open_database("demo", True)["id"]
        operate(c, database, DDL); operate(c, database, INSERT)
        old = c.begin_read(database, "items")
        original = c.scan(database, old["token"], sql)
        operate(c, database, "UPDATE items SET vector=[5,5] WHERE id=7")
        operate(c, database, "DELETE FROM items WHERE id=2")
        assert c.scan(database, old["token"], sql) == original
        np.testing.assert_array_equal(c.vectors(database, old["token"], [2,7]), [[-1,0],[1,0]])
        fresh = c.begin_read(database, "items")
        assert fresh["revision"] > old["revision"]
        np.testing.assert_array_equal(c.vectors(database, fresh["token"], [7]), [[5,5]])
        c.end_read(database, old["token"])
        with pytest.raises(ValueError):
            c.scan(database, old["token"], sql)


def test_lease_expiry_and_restart_invalidates_tokens(tmp_path):
    from vecstor.storage.catalog import DatabaseCatalog
    with DatabaseCatalog(tmp_path) as c:
        database = c.open_database("demo", True)["id"]
        operate(c, database, DDL)
        lease = c.begin_read(database, "items", ttl=0.02)
        time.sleep(0.04)
        with pytest.raises(ValueError):
            c.vectors(database, lease["token"], [1])
        lease = c.begin_read(database, "items")
    with DatabaseCatalog(tmp_path) as c:
        with pytest.raises(ValueError):
            c.vectors(database, lease["token"], [1])


def test_generic_sql_null_aggregate_and_projection(tmp_path):
    from vecstor.storage.catalog import DatabaseCatalog
    with DatabaseCatalog(tmp_path) as c:
        db = c.open_database("demo", True)["id"]
        operate(c, db, "CREATE TABLE ordinary (id BIGINT PRIMARY KEY, label VARCHAR, amount DOUBLE, ok BOOLEAN)")
        operate(c, db, "INSERT INTO ordinary VALUES (1,'a',1,true),(2,'a',2,false),(3,NULL,NULL,true)")
        result = operate(c, db, "SELECT label, COUNT(*) AS n, AVG(amount) AS avg FROM ordinary GROUP BY label HAVING COUNT(*) > 0 ORDER BY n DESC")
        assert result["rows"] == [["a", 2, 1.5], [None, 1, None]]


def test_import_is_atomic_and_idempotent(tmp_path):
    from vecstor.storage.catalog import DatabaseCatalog
    from vecstor.datasets import generate_dataset, save_dataset
    with DatabaseCatalog(tmp_path) as c:
        db = c.open_database("demo", True)["id"]
        save_dataset(generate_dataset(10, 2), tmp_path/"imports/sample.npz")
        assert c.list_datasets() == [{"name":"sample.npz"}]
        first = c.import_dataset(db, "sample.npz", "items", "import-once")
        assert c.import_dataset(db, "sample.npz", "items", "import-once") == first
        assert operate(c, db, "SELECT COUNT(*) FROM items")["rows"] == [[10]]
        with pytest.raises(ValueError):
            c.import_dataset(db, "sample.npz", "items", "new-import")
        (tmp_path/"imports/sample.npz").unlink()
        assert c.import_dataset(db, "sample.npz", "items", "import-once") == first


@pytest.mark.parametrize("name", ["../escape", "/tmp/escape", "bad.name", "__vecstor_meta"])
def test_invalid_database_names(tmp_path, name):
    from vecstor.storage.catalog import DatabaseCatalog
    with DatabaseCatalog(tmp_path) as c:
        with pytest.raises(ValueError): c.open_database(name, True)


def test_import_batches_preserve_large_ids_and_float32_values(tmp_path):
    from vecstor.data import Dataset
    from vecstor.storage.catalog import DatabaseCatalog
    n = 2049
    vectors = np.random.default_rng(17).standard_normal((n, 3)).astype(np.float32)
    vectors[1023] = [np.finfo(np.float32).max, np.nextafter(np.float32(0), np.float32(1)), -0.0]
    vectors[1024] = -vectors[1023]
    ids = np.arange(n, dtype=np.int64) + (2**63 - n)
    data = Dataset(ids, ["论文';?" for _ in range(n)], -ids, np.arange(n, dtype=np.float64), vectors)
    with DatabaseCatalog(tmp_path) as catalog:
        database = catalog.open_database('demo', True)['id']
        db = catalog.database(database)
        first = db.call('import', 'test-source', 'items', lambda: ('digest', data), 'batch-import')
        rows = operate(catalog, database, 'SELECT * FROM items ORDER BY id')['rows']
        assert first['affected_rows'] == n and first['revision'] == 1
        assert [r[0] for r in rows] == ids.tolist()
        assert [r[1] for r in rows] == data.categories.tolist()
        assert [r[2] for r in rows] == data.timestamps.tolist()
        assert [r[3] for r in rows] == data.values.tolist()
        actual = np.asarray([r[4] for r in rows], dtype=np.float32)
        np.testing.assert_array_equal(actual.view(np.uint32), vectors.view(np.uint32))
        def unavailable_source():
            raise AssertionError('successful retry must not reload the source')
        assert db.call('import', 'test-source', 'items', unavailable_source, 'batch-import') == first


def test_import_cancel_at_batch_boundary_rolls_back_and_records_failure(tmp_path, monkeypatch):
    from vecstor.datasets import generate_dataset
    from vecstor.storage.catalog import DatabaseCatalog
    data = generate_dataset(2049, 2)
    with DatabaseCatalog(tmp_path) as catalog:
        database = catalog.open_database('demo', True)['id']
        db = catalog.database(database)
        check = db._check_time
        counts = []
        def cancel_after_first_batch():
            check()
            exists = db.connection.execute("SELECT count(*) FROM information_schema.tables WHERE table_name='items'").fetchone()[0]
            if exists:
                count = db.connection.execute('SELECT count(*) FROM items').fetchone()[0]
                counts.append(count)
                if count == 1024:
                    raise TimeoutError('cancelled at batch boundary')
        monkeypatch.setattr(db, '_check_time', cancel_after_first_batch)
        with pytest.raises(TimeoutError, match='batch boundary'):
            db.call('import', 'test-source', 'items', lambda: ('digest', data), 'cancel-import')
        monkeypatch.setattr(db, '_check_time', check)
        assert 1024 in counts
        result = operate(catalog, database, 'SHOW TABLES')
        assert result['rows'] == [] and result['revision'] == 0
        assert catalog.status(database, 'cancel-import')['status'] == 'FAILED'
        def unavailable_source():
            raise AssertionError('failed retry must not reload the source')
        with pytest.raises(ValueError, match='Previous operation failed'):
            db.call('import', 'test-source', 'items', unavailable_source, 'cancel-import')
        with pytest.raises(ValueError, match='different request'):
            db.call('import', 'different-source', 'items', unavailable_source, 'cancel-import')
