"""Instrumented, validated RPC access bound to one read lease."""

import math
import time

import grpc
import numpy as np

from vecstor.rpc import sql_pb2 as pb
from vecstor.rpc.common import RemoteError, VECTOR_PAYLOAD_BYTES


def consistent(condition, message):
    if not condition: raise RemoteError(grpc.StatusCode.DATA_LOSS,message)


class LeaseIO:
    def __init__(self,stub,database_id,table,context,deadline,trace):
        self.stub,self.context,self.deadline,self.trace=stub,context,deadline,trace
        self.database_id,self.table=database_id,table
        self.lease=None
        trace.setdefault('counts',{})
        for name in ['rpc_calls','rpc_payload_bytes','scan_metadata_calls','filter_candidate_calls','snapshot_calls',
                     'metadata_rows_returned','candidates_checked','vectors_fetched']:
            trace['counts'].setdefault(name,0)
        trace.setdefault('rpc_wait_ms',0.0)

    def remaining(self):
        left=self.deadline-time.monotonic()
        if left<=0: raise RemoteError(grpc.StatusCode.DEADLINE_EXCEEDED,'Query deadline exceeded')
        if not self.context.is_active(): raise RemoteError(grpc.StatusCode.CANCELLED,'Query cancelled')
        return left

    def call(self,method,request):
        self.trace['counts']['rpc_calls']+=1
        self.trace['counts']['rpc_payload_bytes']+=request.ByteSize()
        start=time.monotonic()
        call=method.future(request,timeout=self.remaining())
        self.context.add_callback(call.cancel)
        try:
            response=call.result()
            self.trace['counts']['rpc_payload_bytes']+=response.ByteSize()
            return response
        except grpc.FutureCancelledError as error:
            raise RemoteError(grpc.StatusCode.CANCELLED,'Downstream call cancelled') from error
        finally: self.trace['rpc_wait_ms']+=(time.monotonic()-start)*1000

    def stream(self,method,request):
        self.trace['counts']['rpc_calls']+=1
        self.trace['counts']['rpc_payload_bytes']+=request.ByteSize()
        call=method(request,timeout=self.remaining());self.context.add_callback(call.cancel)
        try:
            while True:
                self.remaining();started=time.monotonic()
                try: response=next(call)
                except StopIteration: break
                finally: self.trace['rpc_wait_ms']+=(time.monotonic()-started)*1000
                self.trace['counts']['rpc_payload_bytes']+=response.ByteSize()
                yield response
        finally: call.cancel()

    def open(self):
        self.lease=self.call(self.stub.BeginRead,pb.BeginReadRequest(database_id=self.database_id,table=self.table,
                                                ttl_seconds=min(60,self.remaining())))
        lease=self.lease
        consistent(bool(lease.token) and lease.database_id==self.database_id and lease.table==self.table
                   and bool(lease.table_id) and bool(lease.schema_version) and 0<lease.dimension*4<=VECTOR_PAYLOAD_BYTES,
                   'Invalid read lease')
        self.batch_size=min(1024,VECTOR_PAYLOAD_BYTES//(lease.dimension*4))
        self.trace.update(database_id=lease.database_id,table_id=lease.table_id,table=lease.table,
                          revision=lease.revision,schema_version=lease.schema_version,dimension=lease.dimension,
                          table_rows=lease.row_count if lease.HasField('row_count') else None)
        return lease

    def close(self):
        if self.lease is not None:
            request=pb.LeaseRequest(database_id=self.database_id,token=self.lease.token)
            started=time.monotonic()
            self.trace['counts']['rpc_calls']+=1
            self.trace['counts']['rpc_payload_bytes']+=request.ByteSize()
            try:
                reply=self.stub.EndRead(request,timeout=1)
                self.trace['counts']['rpc_payload_bytes']+=reply.ByteSize()
            except grpc.RpcError: self.trace['lease_cleanup']='server_ttl_fallback'
            finally: self.trace['rpc_wait_ms']+=(time.monotonic()-started)*1000

    def _version(self,response):
        consistent(response.token==self.lease.token and response.revision==self.lease.revision,'Read lease mismatch')

    @staticmethod
    def row(row):
        consistent(row.id>=0 and bool(row.category) and '\x00' not in row.category and math.isfinite(row.value),'Invalid metadata')
        return {'id':row.id,'category':row.category,'timestamp':row.timestamp,'value':row.value}

    def scan(self,sql):
        self.trace['counts']['scan_metadata_calls']+=1
        total=None;count=0;seen=set();batches=0
        request=pb.ScanRequest(database_id=self.database_id,token=self.lease.token,sql=sql,batch_rows=self.batch_size)
        for batch in self.stream(self.stub.ScanMetadata,request):
            batches+=1;self._version(batch)
            if total is None: total=batch.total_rows
            consistent(batch.total_rows==total and batch.offset==count and count+len(batch.rows)<=total
                       and len(batch.rows)<=self.batch_size,'Metadata count/offset mismatch')
            if self.lease.HasField('row_count'): consistent(total<=self.lease.row_count,'Metadata count exceeds table')
            if not batch.rows: consistent(total==0 and batches==1,'Unexpected empty metadata batch')
            rows=[self.row(row) for row in batch.rows]
            ids=[row['id'] for row in rows]
            consistent(len(set(ids))==len(ids) and not seen.intersection(ids),'Duplicate metadata ID')
            seen.update(ids);count+=len(rows)
            self.trace['counts']['metadata_rows_returned']+=len(rows)
            yield rows
        consistent(total is not None and count==total,'Incomplete metadata stream')

    def fetch(self,ids):
        if not ids: return np.empty((0,self.lease.dimension),dtype=np.float32)
        response=self.call(self.stub.FetchVectors,pb.FetchRequest(database_id=self.database_id,token=self.lease.token,ids=ids))
        self._version(response)
        consistent(response.dimension==self.lease.dimension and list(response.ids)==ids
                   and len(response.vectors_f32_le)==len(ids)*self.lease.dimension*4,'Vector payload/order mismatch')
        vectors=np.frombuffer(response.vectors_f32_le,dtype='<f4').reshape(len(ids),self.lease.dimension)
        consistent(np.isfinite(vectors).all(),'Nonfinite remote vector')
        self.trace['counts']['vectors_fetched']+=len(ids)
        return vectors

    def filter(self,sql,ids):
        self.trace['counts']['filter_candidate_calls']+=1
        reply=self.call(self.stub.FilterCandidates,pb.CandidateRequest(database_id=self.database_id,token=self.lease.token,sql=sql,ids=ids))
        self._version(reply)
        consistent(reply.checked_count==len(ids) and list(reply.checked_ids)==ids,'Candidate coverage mismatch')
        rows=[self.row(row) for row in reply.rows];found=[row['id'] for row in rows]
        consistent(len(set(found))==len(found) and set(found).issubset(ids),'Unexpected candidate ID')
        self.trace['counts']['metadata_rows_returned']+=len(rows)
        return rows

    def snapshot(self):
        consistent(self.lease.HasField('row_count'),'Snapshot export requires row count')
        self.trace['counts']['snapshot_calls']+=1
        total=self.lease.row_count;count=0;previous=-1;batches=0
        for batch in self.stream(self.stub.StreamVectorSnapshot,pb.SnapshotRequest(database_id=self.database_id,token=self.lease.token,batch_rows=self.batch_size)):
            batches+=1;self._version(batch)
            ids=np.asarray(batch.ids,dtype=np.int64)
            consistent(batch.table_id==self.lease.table_id and batch.schema_version==self.lease.schema_version
                       and batch.dimension==self.lease.dimension and batch.total_rows==total and batch.offset==count
                       and count+len(ids)<=total and len(ids)<=self.batch_size,'Snapshot identity/count mismatch')
            consistent(len(batch.vectors_f32_le)==len(ids)*self.lease.dimension*4,'Snapshot vector payload mismatch')
            if len(ids):
                consistent(int(ids[0])>previous and np.all(ids[1:]>ids[:-1]),'Snapshot IDs not uniquely ordered')
                previous=int(ids[-1])
            else: consistent(total==0 and batches==1,'Unexpected empty snapshot batch')
            vectors=np.frombuffer(batch.vectors_f32_le,dtype='<f4').reshape(len(ids),self.lease.dimension)
            consistent(np.isfinite(vectors).all(),'Nonfinite snapshot vectors')
            count+=len(ids)
            yield ids,vectors
        consistent(batches>0 and count==total,'Incomplete vector snapshot')
