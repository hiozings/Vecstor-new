"""Managed database names, sole file owner, and source imports."""

import hashlib
from io import BytesIO
from pathlib import Path
from threading import Event, RLock, Thread

from vecstor.datasets import load_dataset
from vecstor.query.statements import safe_name
from vecstor.storage.database import Database


class DatabaseCatalog:
    def __init__(self, root):
        self.root = Path(root).resolve()
        self.root.mkdir(parents=True, exist_ok=True)
        for name in ["databases", "imports"]:
            path = self.root / name
            if path.is_symlink(): raise ValueError("Managed subdirectories may not be symlinks")
            path.mkdir(exist_ok=True)
        self.opened = {}
        self.lock = RLock()
        self.stopping = Event()
        self.janitor = Thread(target=self._reap, daemon=True, name="vecstor-lease-cleaner")
        self.janitor.start()

    def _reap(self):
        while not self.stopping.wait(1):
            with self.lock:
                databases = list(self.opened.values())
            for database in databases:
                database.pool.submit(database._cleanup)

    def open_database(self, name, create=False):
        name = safe_name(name)
        with self.lock:
            path = self.root / "databases" / f"{name}.duckdb"
            if path.is_symlink(): raise ValueError("Database symlinks are not allowed")
            if not path.exists() and not create: raise ValueError(f"Database does not exist: {name}")
            if name not in self.opened:
                self.opened[name] = Database(path, name)
            db = self.opened[name]
            return {"id": db.id, "name": name}

    def list_databases(self):
        return [self.open_database(path.stem) for path in sorted((self.root / "databases").glob("*.duckdb")) if not path.is_symlink()]

    def database(self, database_id):
        if not database_id: raise ValueError("No database selected; use .open or USE")
        with self.lock:
            for database in self.opened.values():
                if database.id == database_id: return database
            self.list_databases()
            for database in self.opened.values():
                if database.id == database_id: return database
        raise ValueError("Unknown database ID")

    def execute(self, database_id, sql, operation_id, timeout=15, cancelled=None):
        return self.database(database_id).call("execute", sql, operation_id, timeout=timeout, cancelled=cancelled)

    def describe(self, database_id, table=None):
        return self.database(database_id).call("describe", table)

    def status(self, database_id, operation_id):
        return self.database(database_id).call("status", operation_id)

    def list_datasets(self):
        return [{"name": path.name} for path in sorted((self.root / "imports").glob("*.npz")) if not path.is_symlink()]

    def import_dataset(self, database_id, source, table, operation_id, timeout=15, cancelled=None):
        if not source.endswith(".npz") or Path(source).name != source:
            raise ValueError("Use a managed NPZ source name")
        safe_name(source[:-4])
        path = self.root / "imports" / source
        def load():
            if path.is_symlink(): raise ValueError("Import symlinks are not allowed")
            raw = path.read_bytes()
            return hashlib.sha256(raw).hexdigest(), load_dataset(BytesIO(raw))
        return self.database(database_id).call("import", source, table, load, operation_id, timeout=timeout,cancelled=cancelled)

    def begin_read(self, database_id, table, ttl=15, timeout=15):
        return self.database(database_id).call("begin_read", table, ttl,timeout=timeout)

    def scan(self, database_id, token, sql, timeout=15, cancelled=None):
        return self.database(database_id).call("scan", token, sql,timeout=timeout,cancelled=cancelled)

    def vectors(self, database_id, token, ids, timeout=15):
        return self.database(database_id).call("vectors", token, list(ids),timeout=timeout)

    def end_read(self, database_id, token):
        return self.database(database_id).call("end_read", token)

    def snapshot_page(self,database_id,token,after_id,limit,timeout=15,cancelled=None):
        return self.database(database_id).call("snapshot_page",token,after_id,limit,timeout=timeout,cancelled=cancelled)

    def filter_candidates(self,database_id,token,sql,ids,timeout=15,cancelled=None):
        return self.database(database_id).call("filter_candidates",token,sql,list(ids),timeout=timeout,cancelled=cancelled)

    def close(self):
        self.stopping.set(); self.janitor.join()
        with self.lock:
            for database in self.opened.values(): database.close()
            self.opened.clear()

    def __enter__(self): return self
    def __exit__(self, *_): self.close()
