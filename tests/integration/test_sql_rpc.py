from contextlib import contextmanager
from concurrent.futures import ThreadPoolExecutor

import pytest

DDL = "CREATE TABLE items (id BIGINT PRIMARY KEY, category VARCHAR NOT NULL, timestamp BIGINT NOT NULL, value DOUBLE NOT NULL, vector FLOAT[2] NOT NULL)"
SEARCH = "SELECT id, vector_distance(vector, [0,0]) AS distance FROM items ORDER BY distance LIMIT 10"


@contextmanager
def servers(root):
    from vecstor.storage.sql_service import create_catalog_server
    from vecstor.compute.service import create_compute_server
    s, port, storage = create_catalog_server(root, "127.0.0.1:0")
    s.start()
    c, cp, compute = create_compute_server(f"127.0.0.1:{port}", "127.0.0.1:0")
    c.start()
    try: yield f"127.0.0.1:{cp}", storage
    finally:
        c.stop(0).wait(); compute.close()
        s.stop(0).wait(); storage.close()


def test_sql_crud_vector_and_runtime_switching(tmp_path):
    from vecstor.rpc.sql_client import SessionClient
    with servers(tmp_path) as (address, _), SessionClient(address) as client:
        client.execute("CREATE DATABASE demo;")
        client.execute("USE demo;")
        assert client.database_name == "demo"
        client.execute(DDL)
        client.execute("INSERT INTO items VALUES (7, '论文', 0, 12, [1,0]),(2, 'book', 0, 5, [-1,0])")
        assert client.execute(SEARCH)["rows"] == [[2, 1.0], [7, 1.0]]
        client.execute("UPDATE items SET vector=[0,0], value=value+1 WHERE id=7")
        assert client.execute(SEARCH)["rows"] == [[7, 0.0], [2, 1.0]]
        assert client.execute("DELETE FROM items WHERE id=2")["affected_rows"] == 1
        assert client.execute("SELECT COUNT(*) AS n FROM items")["rows"] == [[1]]
        client.open_database("other", create=True)
        assert client.execute("SHOW TABLES")["rows"] == []
        client.open_database("demo")
        assert client.execute(SEARCH)["rows"] == [[7, 0.0]]
        with pytest.raises(ValueError): client.open_database("missing")
        assert client.database_name == "demo"


def test_multiple_clients_and_persistent_status(tmp_path):
    from vecstor.rpc.sql_client import SessionClient
    with servers(tmp_path) as (address, _), SessionClient(address) as a, SessionClient(address) as b:
        for client, name in [(a,"one"),(b,"two")]:
            client.open_database(name, True); client.execute("CREATE TABLE items (id BIGINT)")
        with ThreadPoolExecutor(max_workers=2) as pool:
            assert [f.result()["affected_rows"] for f in [pool.submit(a.execute, "INSERT INTO items VALUES (1)", "once"),
                                                       pool.submit(b.execute, "INSERT INTO items VALUES (2)", "once")]] == [1,1]
        assert a.execute("SELECT id FROM items")["rows"] == [[1]]
        assert b.execute("SELECT id FROM items")["rows"] == [[2]]
    with servers(tmp_path) as (address, _), SessionClient(address) as c:
        c.open_database("one")
        assert c.get_operation_status("once")["status"] == "COMMITTED"
        c.execute("INSERT INTO items VALUES (1)", "once")
        assert c.execute("SELECT COUNT(*) FROM items")["rows"] == [[1]]


def test_import_and_invalid_sql_do_not_damage_database(tmp_path):
    from vecstor.rpc.sql_client import SessionClient
    from vecstor.datasets import save_dataset, generate_dataset
    with servers(tmp_path) as (address, _), SessionClient(address) as c:
        save_dataset(generate_dataset(10,2), tmp_path/"imports/sample.npz")
        c.open_database("demo", True)
        assert c.list_datasets() == [{"name":"sample.npz"}]
        c.import_dataset("sample.npz", "items")
        assert c.execute("SELECT COUNT(*) FROM items")["rows"] == [[10]]
        for sql in ["SELECT * FROM read_csv('/etc/passwd')", "SELECT * FROM __vecstor_meta", "DELETE FROM items; DROP TABLE items", "UPDATE items SET vector=[1] WHERE id=0"]:
            with pytest.raises(ValueError): c.execute(sql)
        assert len(c.execute(SEARCH)["rows"]) == 10


def test_bigint_null_and_vector_result_types(tmp_path):
    from vecstor.rpc.sql_client import SessionClient
    with servers(tmp_path) as (address, _), SessionClient(address) as c:
        c.open_database("demo", True); c.execute(DDL)
        c.execute("INSERT INTO items VALUES (1152921504606846976,'x',0,1,[0,0])")
        result = c.execute("SELECT * FROM items ORDER BY vector_distance(vector,[0,0]) LIMIT 1")
        assert result["rows"] == [[2**60,"x",0,1.0,[0.0,0.0]]]
        c.execute("CREATE TABLE optional (id BIGINT, ok BOOLEAN, text VARCHAR)")
        c.execute("INSERT INTO optional VALUES (1152921504606846976,true,NULL)")
        assert c.execute("SELECT * FROM optional")["rows"] == [[2**60,True,None]]


@pytest.mark.parametrize("cells", [1,3])
def test_client_rejects_result_rows_with_wrong_width(cells):
    import grpc
    from vecstor.rpc import sql_pb2 as pb, sql_pb2_grpc as rpc
    from vecstor.rpc.sql_client import SessionClient
    class Broken(rpc.SQLComputeServicer):
        def ExecuteSQL(self,request,context):
            yield pb.ResultFrame(kind=pb.ResultFrame.SCHEMA,columns=[pb.Column(name="a",type="BIGINT"),pb.Column(name="b",type="BIGINT")])
            yield pb.ResultFrame(kind=pb.ResultFrame.ROWS,rows=[pb.SQLRow(cells=[pb.Cell(integer=1)]*cells)])
            yield pb.ResultFrame(kind=pb.ResultFrame.COMPLETE)
    server=grpc.server(ThreadPoolExecutor(max_workers=1))
    rpc.add_SQLComputeServicer_to_server(Broken(),server)
    port=server.add_insecure_port("127.0.0.1:0");server.start()
    try:
        with SessionClient(f"127.0.0.1:{port}") as c:
            with pytest.raises(ValueError,match="width"):
                c.execute("SELECT id FROM items")
    finally: server.stop(0).wait()
