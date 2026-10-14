"""Persistent per-database SQL queue, atomic operations and MVCC read leases."""

from concurrent.futures import ThreadPoolExecutor, TimeoutError as FutureTimeout
import hashlib
import json
import re
import time
import uuid
from threading import Event, Lock

import duckdb
import numpy as np
import sqlglot
from sqlglot import exp

from vecstor.data import Dataset
from vecstor.query.statements import classify_statement, safe_name


def identifier(name):
    return '"' + safe_name(name) + '"'


def result(columns=(), rows=(), affected_rows=None, **extra):
    def plain(value):
        return [plain(item) for item in value] if isinstance(value, (tuple, list)) else value
    return {"columns": [{"name": name, "type": str(dtype)} for name, dtype in columns],
            "rows": [plain(row) for row in rows], "affected_rows": affected_rows, **extra}


class Database:
    def __init__(self, path, name):
        self.path, self.name = path, name
        self.pool = ThreadPoolExecutor(max_workers=1, thread_name_prefix=f"vecstor-{name}")
        self.leases = {}
        self.active_lock = Lock()
        self.active_token = None
        try:
            self.id = self.pool.submit(self._initialize).result()
        except Exception:
            self.pool.shutdown(wait=True)
            raise

    def _initialize(self):
        self.connection = duckdb.connect(str(self.path), config={"threads": 1, "enable_external_access": False})
        db = self.connection
        db.execute("CREATE TABLE IF NOT EXISTS __vecstor_meta (database_id VARCHAR, revision BIGINT)")
        db.execute("CREATE TABLE IF NOT EXISTS __vecstor_tables (name VARCHAR PRIMARY KEY, table_id VARCHAR, dimension INTEGER)")
        db.execute("CREATE TABLE IF NOT EXISTS __vecstor_operations (operation_id VARCHAR PRIMARY KEY, digest VARCHAR, result_json VARCHAR)")
        row = db.execute("SELECT database_id FROM __vecstor_meta").fetchone()
        if row is None:
            db.execute("INSERT INTO __vecstor_meta VALUES (?, 0)", [str(uuid.uuid4())])
            row = db.execute("SELECT database_id FROM __vecstor_meta").fetchone()
        return row[0]

    def call(self, method, *args, timeout=15, cancelled=None):
        deadline = time.monotonic() + timeout
        token = Event()
        future = self.pool.submit(self._run, method, args, deadline, token)
        while True:
            left=deadline-time.monotonic()
            if left <= 0 or (cancelled is not None and cancelled()):
                token.set(); future.cancel()
                with self.active_lock:
                    if self.active_token is token:
                        self.active_connection.interrupt()
                raise TimeoutError("SQL operation cancelled or timed out; write outcome may be unknown")
            try:
                return future.result(timeout=min(left,0.05))
            except FutureTimeout:
                if future.done(): return future.result()

    def _run(self, method, args, deadline, token):
        self.deadline = deadline
        self.cancelled = token
        self._check_time()
        self._cleanup()
        with self.active_lock:
            self.active_token = token
            self.active_connection = self.connection
        try:
            return getattr(self, "_" + method)(*args)
        except duckdb.Error as error:
            if token.is_set(): raise TimeoutError("SQL operation cancelled") from error
            raise ValueError(str(error)) from error
        finally:
            with self.active_lock: self.active_token = None

    def _check_time(self):
        if self.cancelled.is_set() or time.monotonic() >= self.deadline:
            raise TimeoutError("SQL operation deadline exceeded")

    def _fetch_rows(self, cursor):
        rows=[]
        while True:
            self._check_time()
            chunk=cursor.fetchmany(256)
            if not chunk: return rows
            rows.extend(chunk)

    def _revision(self, connection=None):
        return (connection or self.connection).execute("SELECT revision FROM __vecstor_meta").fetchone()[0]

    def _write(self, operation_id, digest, change):
        if not isinstance(operation_id, str) or not re.fullmatch(r"[A-Za-z0-9_-]{1,128}", operation_id):
            raise ValueError("A valid operation_id is required for writes")
        db = self.connection
        previous = db.execute("SELECT digest, result_json FROM __vecstor_operations WHERE operation_id=?", [operation_id]).fetchone()
        if previous:
            if previous[0] != digest:
                raise ValueError("operation_id was already used with a different request")
            stored=json.loads(previous[1])
            if stored.get("_failed"):
                raise ValueError(f"Previous operation failed: {stored['error']}")
            return stored
        db.execute("BEGIN TRANSACTION")
        try:
            output = change()
            self._check_time()
            db.execute("UPDATE __vecstor_meta SET revision=revision+1")
            output.update(revision=self._revision(), operation_id=operation_id)
            encoded = json.dumps(output, ensure_ascii=False, allow_nan=False)
            db.execute("INSERT INTO __vecstor_operations VALUES (?, ?, ?)", [operation_id, digest, encoded])
            self._check_time()
            db.execute("COMMIT")
            return output
        except Exception as error:
            db.execute("ROLLBACK")
            failure=json.dumps({"_failed":True,"error":str(error),"operation_id":operation_id})
            db.execute("INSERT INTO __vecstor_operations VALUES (?, ?, ?)", [operation_id,digest,failure])
            raise

    def _validate_table(self, name, creating=False):
        db, quoted = self.connection, identifier(name)
        schema = db.execute(f"DESCRIBE {quoted}").fetchall()
        names = [row[0] for row in schema]
        dimension = 0
        allowed = {"BOOLEAN", "INTEGER", "BIGINT", "DOUBLE", "VARCHAR"}
        for field, dtype, *_ in schema:
            safe_name(field)
            if dtype not in allowed:
                match = re.fullmatch(r"FLOAT\[(\d+)\]", dtype)
                if field != "vector" or match is None:
                    raise ValueError(f"Unsupported column type: {field} {dtype}")
                dimension = int(match.group(1))
        if "vector" in names:
            expected = {"id": "BIGINT", "category": "VARCHAR", "timestamp": "BIGINT", "value": "DOUBLE", "vector": f"FLOAT[{dimension}]"}
            if dimension < 1 or {r[0]: r[1] for r in schema} != expected:
                raise ValueError("Vector tables require the five-field P1 schema and FLOAT[D]")
            if any(row[2] != "NO" for row in schema) or next(r for r in schema if r[0] == "id")[3] != "PRI":
                raise ValueError("Vector tables require id PRIMARY KEY and non-null columns")
            rows = self._fetch_rows(db.execute(f'SELECT id, category, "timestamp", value, vector FROM {quoted}'))
            Dataset([r[0] for r in rows], [r[1] for r in rows], [r[2] for r in rows],
                    [r[3] for r in rows], [r[4] for r in rows] if rows else np.empty((0, dimension)))
        if creating:
            db.execute("INSERT INTO __vecstor_tables VALUES (?, ?, ?)", [name, str(uuid.uuid4()), dimension])

    def _execute(self, sql, operation_id):
        plan = classify_statement(sql)
        if plan.kind == "show_tables":
            rows = self.connection.execute("SELECT name FROM __vecstor_tables ORDER BY name").fetchall()
            return result([("name", "VARCHAR")], rows, revision=self._revision())
        if plan.kind == "describe":
            return self._describe(plan.table)
        if plan.kind not in {"select", "create", "drop", "insert", "update", "delete"}:
            raise ValueError("Statement requires compute routing")
        if not plan.is_write:
            cursor = self.connection.execute(plan.sql)
            columns=[(c[0],c[1]) for c in cursor.description]
            return result(columns, self._fetch_rows(cursor), revision=self._revision())
        digest = hashlib.sha256(plan.sql.encode()).hexdigest()
        def change():
            cursor = self.connection.execute(plan.sql)
            counts = cursor.fetchall()
            if plan.kind == "drop":
                self.connection.execute("DELETE FROM __vecstor_tables WHERE name=?", [plan.table])
            else:
                self._validate_table(plan.table, creating=plan.kind == "create")
            affected = int(counts[0][0]) if counts else 0
            return result(affected_rows=affected)
        return self._write(operation_id, digest, change)

    def _describe(self, table=None):
        names = [safe_name(table)] if table else [r[0] for r in self.connection.execute("SELECT name FROM __vecstor_tables ORDER BY name").fetchall()]
        rows = []
        for name in names:
            rows.extend((name, r[0], r[1], r[2], r[3] or "") for r in self.connection.execute(f"DESCRIBE {identifier(name)}").fetchall())
        return result([("table", "VARCHAR"), ("column", "VARCHAR"), ("type", "VARCHAR"), ("nullable", "VARCHAR"), ("key", "VARCHAR")], rows)

    def _status(self, operation_id):
        row = self.connection.execute("SELECT result_json FROM __vecstor_operations WHERE operation_id=?", [operation_id]).fetchone()
        if row:
            value=json.loads(row[0])
            if value.get("_failed"):
                return {"status":"FAILED","operation_id":operation_id,"error":value["error"]}
            return {"status": "COMMITTED", "operation_id": operation_id, "result": value}
        return {"status": "UNKNOWN", "operation_id": operation_id}

    def _import(self, source, table, load, operation_id):
        table = safe_name(table)
        # Bind the operation to the request, not to a source file that may
        # change/disappear after a successful import and lost response.
        digest = hashlib.sha256(f"import:{source}:{table}".encode()).hexdigest()
        def change():
            source_digest, data = load()
            self.connection.execute(f'CREATE TABLE {identifier(table)} (id BIGINT PRIMARY KEY, category VARCHAR NOT NULL, "timestamp" BIGINT NOT NULL, value DOUBLE NOT NULL, vector FLOAT[{data.dimension}] NOT NULL)')
            for offset in range(0, data.size, 1024):
                self._check_time()
                batch = slice(offset, offset + 1024)
                # Bind vector text once per batch: binding millions of individual
                # Python floats is expensive. The fixed-array cast remains SQL
                # parameter binding, and JSON preserves finite float32 values.
                vectors = json.dumps(data.vectors[batch].tolist(), allow_nan=False, separators=(',', ':'))
                self.connection.execute(
                    f"INSERT INTO {identifier(table)} SELECT unnest(?::BIGINT[]), unnest(?::VARCHAR[]), "
                    f"unnest(?::BIGINT[]), unnest(?::DOUBLE[]), unnest(?::FLOAT[{data.dimension}][])",
                    [data.ids[batch].tolist(), data.categories[batch].tolist(), data.timestamps[batch].tolist(),
                     data.values[batch].tolist(), vectors])
                self._check_time()
            self._validate_table(table, creating=True)
            return result(affected_rows=data.size, source_sha256=source_digest)
        return self._write(operation_id, digest, change)

    def _begin_read(self, table, ttl):
        table = safe_name(table)
        if not 0 < ttl <= 60 or len(self.leases) >= 16:
            raise ValueError("Read lease TTL must be <= 60 seconds and at most 16 leases may be active")
        connection = self.connection.cursor()
        try:
            connection.execute("BEGIN TRANSACTION")
            revision = self._revision(connection)
            row = connection.execute("SELECT table_id, dimension FROM __vecstor_tables WHERE name=?", [table]).fetchone()
            if row is None or row[1] < 1:
                raise ValueError("Table is not a registered vector table")
            token = str(uuid.uuid4())
            public = {"token": token, "database_id": self.id, "table_id": row[0], "revision": revision,
                      "schema_version": row[0], "dimension": row[1], "table": table,
                      "row_count": connection.execute(f"SELECT COUNT(*) FROM {identifier(table)}").fetchone()[0]}
            self.leases[token] = (connection, time.monotonic() + ttl, public)
            return public
        except Exception:
            connection.close()
            raise

    def _lease(self, token):
        if token not in self.leases:
            raise ValueError("Read lease unavailable or expired")
        with self.active_lock:
            self.active_connection=self.leases[token][0]
        return self.leases[token]

    def _scan(self, token, sql):
        connection, _, lease = self._lease(token)
        plan = classify_statement(sql)
        if plan.vector is None or plan.table != lease["table"]:
            raise ValueError("Query does not match leased vector table")
        return self._fetch_rows(connection.execute(plan.vector.metadata_sql))

    def _vectors(self, token, ids):
        connection, _, lease = self._lease(token)
        if len(ids) > 1024 or len(ids) * lease["dimension"] * 4 > 1024 * 1024:
            raise ValueError("Vector batch exceeds transfer limit")
        if len(set(ids)) != len(ids) or any(isinstance(i, bool) or not isinstance(i, int) or i < 0 for i in ids):
            raise ValueError("Vector IDs must be distinct nonnegative integers")
        if not ids:
            return np.empty((0, lease["dimension"]), dtype=np.float32)
        rows = connection.execute(f"SELECT id, vector FROM {identifier(lease['table'])} WHERE id IN ({','.join('?' for _ in ids)})", ids).fetchall()
        mapping = dict(rows)
        if set(mapping) != set(ids):
            raise ValueError("Vector ID not present in snapshot")
        return np.asarray([mapping[i] for i in ids], dtype=np.float32)

    def _snapshot_page(self, token, after_id, limit):
        connection, _, lease = self._lease(token)
        if not 1 <= limit <= 1024 or limit * lease["dimension"] * 4 > 1024 * 1024:
            raise ValueError("Snapshot batch exceeds transfer limit")
        return connection.execute(f"SELECT id, vector FROM {identifier(lease['table'])} WHERE id > ? ORDER BY id LIMIT ?",
                                  [after_id, limit]).fetchall()

    def _filter_candidates(self, token, sql, ids):
        connection, _, lease = self._lease(token)
        plan = classify_statement(sql)
        if plan.vector is None or plan.table != lease["table"]:
            raise ValueError("Candidate query does not match leased table")
        if len(ids) > 1024 or len(set(ids)) != len(ids) or any(isinstance(i,bool) or not isinstance(i,int) or i < 0 for i in ids):
            raise ValueError("Candidate IDs must be distinct nonnegative int64 values, at most 1024")
        if not ids: return []
        placeholders=','.join('?' for _ in ids)
        present=connection.execute(f"SELECT id FROM {identifier(lease['table'])} WHERE id IN ({placeholders})",ids).fetchall()
        if {r[0] for r in present} != set(ids):
            raise ValueError("Candidate ID does not exist in leased snapshot")
        statement=sqlglot.parse_one(plan.vector.metadata_sql,read="duckdb")
        statement=statement.where(exp.In(this=exp.column("id",quoted=True),expressions=[exp.Placeholder() for _ in ids]))
        rows=connection.execute(statement.sql(dialect="duckdb"),ids).fetchall()
        mapping={r[0]:r for r in rows}
        return [mapping[i] for i in ids if i in mapping]

    def _end_read(self, token):
        lease = self.leases.pop(token, None)
        if lease:
            lease[0].execute("ROLLBACK")
            lease[0].close()

    def _cleanup(self):
        for token, (_, expires, _) in list(self.leases.items()):
            if expires <= time.monotonic():
                self._end_read(token)

    def close(self):
        def shutdown():
            for token in list(self.leases): self._end_read(token)
            self.connection.close()
        self.pool.submit(shutdown).result()
        self.pool.shutdown(wait=True)
