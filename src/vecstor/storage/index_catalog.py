"""Publication metadata; every method runs on its owning Database SQL queue."""
from dataclasses import asdict, replace
import json
import re

from vecstor.index.artifact import ArtifactDescriptor, ArtifactError, PublicationRecord, PruneResult, canonical, digest, require
from vecstor.index.types import BuildConfig, IndexKey

TERMINAL = {'COMMITTED', 'FAILED', 'ABORTED'}


def record_dict(record):
    return asdict(record)


def record_from_dict(value):
    value = dict(value)
    if value['key'] is not None: value['key'] = IndexKey(**value['key'])
    if value['descriptor'] is not None: value['descriptor'] = ArtifactDescriptor.from_dict(value['descriptor'])
    return PublicationRecord(**value)


def snapshot_key(key):
    return canonical(list(key.snapshot)).decode()


class IndexCatalog:
    def __init__(self, connection, store, lease_lookup=None, active_artifacts=lambda:()):
        self.connection, self.store, self.lease_lookup = connection, store, lease_lookup
        self.active_artifacts = active_artifacts

    def initialize(self):
        db = self.connection
        db.execute('BEGIN TRANSACTION')
        try:
            db.execute('CREATE TABLE IF NOT EXISTS __vecstor_index_meta (schema_version BIGINT, publication_generation BIGINT)')
            rows = db.execute('SELECT * FROM __vecstor_index_meta').fetchall()
            require(not rows or (len(rows) == 1 and rows[0][0] == 1), 'unsupported_index_catalog_schema', 'FAILED_PRECONDITION')
            if not rows: db.execute('INSERT INTO __vecstor_index_meta VALUES (1, 0)')
            db.execute('CREATE TABLE IF NOT EXISTS __vecstor_index_artifacts (artifact_id VARCHAR PRIMARY KEY, key_json VARCHAR UNIQUE, snapshot_json VARCHAR, table_id VARCHAR, descriptor_json VARCHAR, state VARCHAR, first_generation BIGINT, last_generation BIGINT)')
            db.execute('CREATE TABLE IF NOT EXISTS __vecstor_index_heads (snapshot_json VARCHAR PRIMARY KEY, artifact_id VARCHAR, generation BIGINT)')
            db.execute('CREATE TABLE IF NOT EXISTS __vecstor_index_operations (operation_id VARCHAR PRIMARY KEY, lease_token VARCHAR, record_json VARCHAR)')
            db.execute('COMMIT')
        except BaseException:
            db.execute('ROLLBACK')
            raise

    def get_operation(self, operation_id):
        row = self.connection.execute('SELECT record_json FROM __vecstor_index_operations WHERE operation_id=?', [operation_id]).fetchone()
        return record_from_dict(json.loads(row[0])) if row else None

    def _save(self, record):
        self.connection.execute('UPDATE __vecstor_index_operations SET record_json=? WHERE operation_id=?',
                                [canonical(record_dict(record)).decode(), record.operation_id])
        return record

    def begin(self, lease, operation_id, config, expected_generation, request_intent):
        require(isinstance(operation_id, str) and re.fullmatch(r'[A-Za-z0-9_-]{1,128}', operation_id), 'invalid_operation_id')
        require(isinstance(request_intent, dict) and set(request_intent) == {'database_id','table','nlist','seed','expected_generation','protocol_version'}, 'invalid_publication_intent')
        require(request_intent['database_id'] == lease['database_id'] and request_intent['table'] == lease['table']
                and type(request_intent['protocol_version']) is int and request_intent['protocol_version'] == 1,
                'publication_intent_mismatch')
        intent_hash = digest(request_intent)
        previous = self.get_operation(operation_id)
        if previous:
            require(previous.operation_kind == 'PUBLISH' and previous.intent_digest == intent_hash, 'operation_id_request_conflict', 'ALREADY_EXISTS')
            return previous
        require(type(expected_generation) is int and 0 <= expected_generation < 2**63, 'invalid_expected_generation')
        require(request_intent['expected_generation'] is None or
                (type(request_intent['expected_generation']) is int and request_intent['expected_generation'] == expected_generation), 'publication_expected_generation_mismatch')
        requested = BuildConfig(nlist=request_intent['nlist'], seed=request_intent['seed']).resolved(lease['row_count'])
        config = config.resolved(lease['row_count'])
        require(config == requested, 'publication_config_intent_mismatch')
        key = IndexKey(lease['database_id'], lease['table_id'], lease['revision'], lease['schema_version'],
                       lease['dimension'], build_fingerprint=config.fingerprint())
        binding = dict(intent=request_intent, key=asdict(key), expected_generation=expected_generation)
        record = PublicationRecord(operation_id, 'PUBLISH', request_intent, intent_hash, digest(binding), key,
                                    expected_generation, 'RESERVED')
        active = self.connection.execute('SELECT record_json FROM __vecstor_index_operations').fetchall()
        require(not any((r := json.loads(row[0]))['operation_kind'] == 'PUBLISH' and r['state'] not in TERMINAL for row in active),
                'index_publication_busy', 'RESOURCE_EXHAUSTED')
        self.connection.execute('INSERT INTO __vecstor_index_operations VALUES (?, ?, ?)',
                                [operation_id, lease['token'], canonical(record_dict(record)).decode()])
        if lease['row_count'] < 1024:
            self.fail(operation_id, 'FAILED_PRECONDITION', 'small_table')
            raise ArtifactError('FAILED_PRECONDITION', 'small_table')
        existing = self.connection.execute('SELECT descriptor_json, state FROM __vecstor_index_artifacts WHERE key_json=?', [canonical(asdict(key)).decode()]).fetchone()
        if existing:
            require(existing[1] not in ('CORRUPT', 'RETIRED'), 'index_artifact_unavailable', 'FAILED_PRECONDITION')
            if existing[1] == 'READY':
                record = self._save(replace(record, descriptor=ArtifactDescriptor.from_dict(json.loads(existing[0])),result={'artifact_reused':True}))
        return record

    def bind_upload(self, operation_id, descriptor):
        descriptor = ArtifactDescriptor.from_dict(descriptor.to_dict())
        record = self.get_operation(operation_id)
        require(record is not None and record.state in ('RESERVED', 'UPLOADING'), 'publication_not_uploadable', 'FAILED_PRECONDITION')
        require(record.key == descriptor.key, 'publication_artifact_key_mismatch', 'DATA_LOSS')
        require(record.descriptor is None or record.descriptor == descriptor, 'publication_descriptor_conflict', 'ALREADY_EXISTS')
        return self._save(replace(record, descriptor=descriptor, state='UPLOADING'))

    def set_phase(self, operation_id, phase):
        record = self.get_operation(operation_id)
        allowed = {'UPLOADING': 'STAGED', 'STAGED': 'VALIDATING'}
        require(record is not None and allowed.get(record.state) == phase, 'publication_phase_mismatch', 'FAILED_PRECONDITION')
        return self._save(replace(record, state=phase))

    def fail(self, operation_id, code, reason):
        record = self.get_operation(operation_id)
        if record is None or record.state in TERMINAL: return record
        return self._save(replace(record, state='ABORTED' if code in ('ABORTED', 'CANCELLED', 'DEADLINE_EXCEEDED') else 'FAILED',
                                  error=dict(code=code, reason=reason)))

    def claim_finalize(self, operation_id, token):
        record = self.get_operation(operation_id)
        require(record is not None, 'unknown_publication', 'NOT_FOUND')
        if record.state in TERMINAL or record.state == 'VALIDATING': return record, False
        self.check_operation(operation_id, token)
        require(record.state == 'STAGED' or (record.state == 'RESERVED' and record.descriptor is not None),
                'publication_not_staged', 'FAILED_PRECONDITION')
        return self._save(replace(record, state='VALIDATING')), True

    def check_operation(self, operation_id, token):
        self.lease_lookup(token)
        row = self.connection.execute('SELECT lease_token FROM __vecstor_index_operations WHERE operation_id=?', [operation_id]).fetchone()
        require(row is not None and row[0] == token, 'publication_lease_mismatch', 'FAILED_PRECONDITION')
        return self.get_operation(operation_id)

    def abort_lease(self, token):
        aborted = []
        for row in self.connection.execute('SELECT operation_id FROM __vecstor_index_operations WHERE lease_token=?', [token]).fetchall():
            if self.get_operation(row[0]).state not in TERMINAL:
                self.fail(row[0], 'ABORTED', 'lease_ended')
                aborted.append(row[0])
        return aborted

    def resolve_head(self, connection, snapshot):
        row = connection.execute('SELECT a.descriptor_json, h.generation, a.state FROM __vecstor_index_heads h JOIN __vecstor_index_artifacts a USING (artifact_id) WHERE h.snapshot_json=?', [canonical(list(snapshot)).decode()]).fetchone()
        if row is None: return None, 0, 'missing'
        require(row[2] in ('READY', 'CORRUPT'), 'retired_index_head', 'DATA_LOSS')
        return ArtifactDescriptor.from_dict(json.loads(row[0])), row[1], row[2].lower()

    def commit(self, operation_id, prepared, lease_token, check):
        record = self.get_operation(operation_id)
        require(record is not None, 'unknown_publication', 'NOT_FOUND')
        if record.state == 'COMMITTED':
            require(record.descriptor == prepared, 'publication_descriptor_conflict', 'ALREADY_EXISTS')
            return record
        if record.state in TERMINAL:
            raise ArtifactError(record.error['code'], record.error['reason'])
        db = self.connection
        db.execute('BEGIN TRANSACTION')
        try:
            check()
            lease, snapshot_digest = self.lease_lookup(lease_token)
            token = db.execute('SELECT lease_token FROM __vecstor_index_operations WHERE operation_id=?', [operation_id]).fetchone()[0]
            require(token == lease_token, 'publication_lease_mismatch', 'FAILED_PRECONDITION')
            current = db.execute('SELECT table_id, dimension FROM __vecstor_tables WHERE name=?', [lease['table']]).fetchone()
            require(current == (record.key.table_id, record.key.dimension) and
                    db.execute('SELECT revision FROM __vecstor_meta').fetchone()[0] == record.key.revision,
                    'index_snapshot_changed', 'ABORTED')
            _, generation, _ = self.resolve_head(db, record.key.snapshot)
            require(generation == record.expected_generation, 'index_head_changed', 'ABORTED')
            require(prepared == record.descriptor and prepared.key == record.key and prepared.row_count == lease['row_count'],
                    'publication_descriptor_mismatch', 'DATA_LOSS')
            old = db.execute('SELECT artifact_id, state FROM __vecstor_index_artifacts WHERE key_json=?', [canonical(asdict(record.key)).decode()]).fetchone()
            if old:
                require(old[0] == prepared.artifact_id, 'index_key_content_conflict', 'ALREADY_EXISTS')
                require(old[1] in ('READY', 'DELETED'), 'index_artifact_unavailable', 'FAILED_PRECONDITION')
            if old is None or old[1] == 'DELETED':
                require(record.state == 'VALIDATING' and snapshot_digest == prepared.snapshot_sha256,
                        'publication_snapshot_receipt_mismatch', 'DATA_LOSS')
            require(self.store is not None and self.store.prepared(prepared), 'artifact_not_prepared', 'DATA_LOSS')
            head = db.execute('SELECT artifact_id FROM __vecstor_index_heads WHERE snapshot_json=?', [snapshot_key(record.key)]).fetchone()
            if not head or head[0] != prepared.artifact_id:
                db.execute('UPDATE __vecstor_index_meta SET publication_generation=publication_generation+1')
                generation = db.execute('SELECT publication_generation FROM __vecstor_index_meta').fetchone()[0]
            if old is None:
                db.execute('INSERT INTO __vecstor_index_artifacts VALUES (?, ?, ?, ?, ?, ?, ?, ?)',
                            [prepared.artifact_id, canonical(asdict(record.key)).decode(), snapshot_key(record.key), record.key.table_id,
                             canonical(prepared.to_dict()).decode(), 'READY', generation, generation])
            else:
                db.execute("UPDATE __vecstor_index_artifacts SET state='READY', last_generation=? WHERE artifact_id=?", [generation, prepared.artifact_id])
            db.execute('INSERT OR REPLACE INTO __vecstor_index_heads VALUES (?, ?, ?)', [snapshot_key(record.key), prepared.artifact_id, generation])
            statistics=(dict(storage_verify_ms=0.,storage_install_ms=0.,verifier_peak_rss_bytes=None,storage_catalog_commit_ms=None)
                        if (record.result or {}).get('artifact_reused') else self.store.statistics(prepared))
            result = self._save(replace(record, descriptor=prepared, generation=generation, state='COMMITTED',
                result=dict(record.result or {},storage_statistics=statistics)))
            check()
            db.execute('COMMIT')
            return result
        except BaseException as error:
            db.execute('ROLLBACK')
            code = error.code if isinstance(error, ArtifactError) else 'DEADLINE_EXCEEDED' if isinstance(error, TimeoutError) else 'INTERNAL'
            self.fail(operation_id, code, str(error))
            raise

    def list_artifacts(self, table_id=None, limit=128, cursor=''):
        require(type(limit) is int and 1 <= limit <= 128, 'invalid_page_size')
        rows = self.connection.execute('SELECT descriptor_json, state, last_generation FROM __vecstor_index_artifacts WHERE artifact_id>? AND (? IS NULL OR table_id=?) ORDER BY artifact_id LIMIT ?', [cursor, table_id, table_id, limit+1]).fetchall()
        result = [dict(descriptor=json.loads(row[0]), state=row[1], generation=row[2]) for row in rows[:limit]]
        return result, result[-1]['descriptor']['artifact_id'] if len(rows) > limit else ''

    def artifact(self, identity):
        row=self.connection.execute('SELECT descriptor_json FROM __vecstor_index_artifacts WHERE artifact_id=?',[identity]).fetchone()
        require(row is not None,'unknown_index_artifact','NOT_FOUND')
        return ArtifactDescriptor.from_dict(json.loads(row[0]))

    def recovery_records(self):
        artifacts=[dict(descriptor=ArtifactDescriptor.from_dict(json.loads(row[0])).to_dict(),state=row[1])
                   for row in self.connection.execute('SELECT descriptor_json,state FROM __vecstor_index_artifacts').fetchall()]
        operations=[record_dict(record_from_dict(json.loads(row[0])))
                    for row in self.connection.execute('SELECT record_json FROM __vecstor_index_operations').fetchall()]
        return dict(artifacts=artifacts,operations=operations)

    def recover(self, store_report):
        db=self.connection
        pending=[]
        db.execute('BEGIN TRANSACTION')
        try:
            for identity,state in db.execute('SELECT artifact_id,state FROM __vecstor_index_artifacts').fetchall():
                if state in ('READY','CORRUPT'):
                    db.execute('UPDATE __vecstor_index_artifacts SET state=? WHERE artifact_id=?',
                               ['CORRUPT' if identity in store_report['corrupt'] else 'READY',identity])
            for row in db.execute('SELECT record_json FROM __vecstor_index_operations').fetchall():
                record=record_from_dict(json.loads(row[0]))
                if record.state in TERMINAL:continue
                if record.operation_kind=='PUBLISH':self.fail(record.operation_id,'ABORTED','storage_restart')
                elif record.operation_kind=='PRUNE':pending.append(record.operation_id)
            db.execute('COMMIT')
        except BaseException:
            db.execute('ROLLBACK');raise
        return pending

    @staticmethod
    def _prune_result(value):
        return PruneResult(tuple(value['candidates']),tuple(value['protected']),tuple(value['deleted']),value['deleted_bytes'])

    def begin_prune(self, operation_id, keep_last, dry_run, check=lambda:None):
        require(type(keep_last) is int and keep_last>=1,'invalid_index_retention')
        require(self.store is not None,'artifact_store_unavailable','FAILED_PRECONDITION')
        database_id=self.connection.execute('SELECT database_id FROM __vecstor_meta').fetchone()[0]
        intent=dict(database_id=database_id,keep_last=keep_last,kind='PRUNE',protocol_version=1)
        if not dry_run:
            require(isinstance(operation_id,str) and re.fullmatch('[A-Za-z0-9_-]{1,128}',operation_id),'invalid_operation_id')
            previous=self.get_operation(operation_id)
            if previous:
                require(previous.operation_kind=='PRUNE' and previous.request_intent==intent,'operation_id_request_conflict','ALREADY_EXISTS')
                return self._prune_result(previous.result)
        db=self.connection
        with self.store.lock:
            check()
            records=db.execute("SELECT artifact_id,table_id,state,last_generation FROM __vecstor_index_artifacts WHERE state IN ('READY','CORRUPT') ORDER BY last_generation DESC,artifact_id").fetchall()
            protected=set(self.active_artifacts()) | set(self.store.transfer_pins)
            tables={row[0] for row in db.execute('SELECT table_id FROM __vecstor_tables').fetchall()}
            counts={}
            for identity,table_id,_,_ in records:
                if table_id not in tables:continue
                counts[table_id]=counts.get(table_id,0)+1
                if counts[table_id]<=keep_last:protected.add(identity)
            revision=db.execute('SELECT revision FROM __vecstor_meta').fetchone()[0]
            for snapshot,identity in db.execute('SELECT snapshot_json,artifact_id FROM __vecstor_index_heads').fetchall():
                key=json.loads(snapshot)
                if key[2]==revision and key[1] in tables:protected.add(identity)
            for row in db.execute('SELECT record_json FROM __vecstor_index_operations').fetchall():
                record=record_from_dict(json.loads(row[0]))
                if record.operation_kind=='PUBLISH' and record.state not in TERMINAL and record.descriptor:
                    protected.add(record.descriptor.artifact_id)
            candidates=sorted(row[0] for row in records if row[0] not in protected)
            result=dict(candidates=candidates,protected=sorted(protected),deleted=[],deleted_bytes=0)
            if dry_run:return self._prune_result(result)
            descriptors=[self.artifact(identity) for identity in candidates]
            db.execute('BEGIN TRANSACTION')
            try:
                check()
                self.store.mark_retired(descriptors)
                for identity in candidates:
                    db.execute("UPDATE __vecstor_index_artifacts SET state='RETIRED' WHERE artifact_id=?",[identity])
                    db.execute('DELETE FROM __vecstor_index_heads WHERE artifact_id=?',[identity])
                record=PublicationRecord(operation_id,'PRUNE',intent,digest(intent),digest(intent),None,0,
                    'PRUNING' if candidates else 'COMMITTED',result=result)
                db.execute('INSERT INTO __vecstor_index_operations VALUES (?, ?, ?)',[operation_id,'',canonical(record_dict(record)).decode()])
                check()
                db.execute('COMMIT')
            except BaseException:
                db.execute('ROLLBACK')
                self.store.unmark_retired(descriptors)
                raise
            return self._prune_result(result)

    def finish_prune(self, operation_id, deleted_ids, check=lambda:None):
        record=self.get_operation(operation_id)
        require(record is not None and record.operation_kind=='PRUNE','unknown_prune_operation','NOT_FOUND')
        if record.state=='COMMITTED':return self._prune_result(record.result)
        require(record.state=='PRUNING','prune_not_running','FAILED_PRECONDITION')
        result=dict(record.result)
        require(set(deleted_ids).issubset(result['candidates']),'unexpected_prune_artifact','DATA_LOSS')
        db=self.connection
        db.execute('BEGIN TRANSACTION')
        try:
            check()
            deleted=set(result['deleted'])
            for identity in deleted_ids:
                if identity in deleted:continue
                row=db.execute('SELECT state,descriptor_json FROM __vecstor_index_artifacts WHERE artifact_id=?',[identity]).fetchone()
                require(row is not None and row[0] in ('RETIRED','DELETED'),'prune_state_changed','DATA_LOSS')
                descriptor=json.loads(row[1])
                result['deleted_bytes']+=descriptor['payload_size']+len(canonical(descriptor))
                db.execute("UPDATE __vecstor_index_artifacts SET state='DELETED' WHERE artifact_id=?",[identity])
                deleted.add(identity)
            result['deleted']=sorted(deleted)
            state='COMMITTED' if deleted==set(result['candidates']) else 'PRUNING'
            self._save(replace(record,state=state,result=result))
            check();db.execute('COMMIT')
        except BaseException:
            db.execute('ROLLBACK');raise
        return self._prune_result(result)
