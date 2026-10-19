from concurrent.futures import ThreadPoolExecutor
from contextlib import ExitStack
from dataclasses import replace
from threading import Event

import duckdb
import grpc
import numpy as np
import pytest

from test_data_cache_execution import cached,SQL
from vecstor.cache.types import CacheConfig,DataScope
from vecstor.compute.lease_io import LeaseIO
from vecstor.compute.service import create_compute_server
from vecstor.datasets import generate_dataset,save_dataset
from vecstor.rpc import sql_pb2 as pb, sql_pb2_grpc as rpc
from vecstor.rpc.sql_client import SessionClient
from vecstor.storage.sql_service import create_catalog_server


def test_clear_while_vector_rpc_in_flight_cannot_refill(cached,monkeypatch):
    entered=Event();release=Event();original=cached.backend.catalog.vectors
    def paused(*args,**kwargs):
        entered.set();assert release.wait(10)
        return original(*args,**kwargs)
    monkeypatch.setattr(cached.backend.catalog,'vectors',paused)
    with ThreadPoolExecutor(1) as pool:
        job=pool.submit(cached.client.execute,SQL)
        try:
            assert entered.wait(5)
            cached.client.clear_data_cache()
        finally:release.set()
        assert len(job.result(timeout=5)['rows'])==10
    stats=cached.client.data_cache_stats()
    assert stats['database']['entry_count']==0 and stats['process']['workspace_bytes']==0


def test_corrupt_vector_rpc_cannot_poison_cache(cached,monkeypatch):
    original=LeaseIO.call
    def corrupt(self,method,request):
        response=original(self,method,request)
        if isinstance(request,pb.FetchRequest):response.vectors_f32_le=b'broken'
        return response
    monkeypatch.setattr(LeaseIO,'call',corrupt)
    with pytest.raises(ValueError,match='DATA_LOSS'):cached.client.execute(SQL)
    stats=cached.client.data_cache_stats()
    assert stats['database']['entry_count']==0 and stats['process']['workspace_bytes']==0


@pytest.mark.parametrize('fused',[False,True])
def test_cached_unknown_id_never_skips_storage_existence_check(cached,monkeypatch,fused):
    warm=cached.client.execute(SQL)
    scope=DataScope(**warm['execution_plan']['data_cache']['scope'])
    with cached.compute.data_cache.batch(scope,(999999,),lambda:None) as batch:
        batch.finish((999999,),(999999,),np.zeros((1,4),dtype='<f4'),admit=True,check=lambda:None)
    entry=next(iter(cached.compute.index_manager.registry._entries.values()))
    monkeypatch.setattr(entry.handle,'search',lambda *a,**k:(np.zeros(1),np.array([999999],dtype=np.int64)))
    cached.compute.fused_candidates=fused
    cached.client.search_options=replace(cached.client.search_options,candidate_data_path='fused' if fused else 'separate')
    with pytest.raises(ValueError,match='does not exist'):cached.client.execute(SQL)


def test_warm_query_actual_rpc_cancellation(cached,monkeypatch):
    from vecstor.compute.data_reader import CachedLeaseReader
    cached.client.execute(SQL)
    entered=Event();release=Event();finished=Event();original=CachedLeaseReader.fetch
    def paused(self,*args,**kwargs):
        result=original(self,*args,**kwargs)
        entered.set();assert release.wait(10)
        try:self.io.remaining()
        finally:finished.set()
        return result
    monkeypatch.setattr(CachedLeaseReader,'fetch',paused)
    with grpc.insecure_channel(cached.address) as channel:
        call=rpc.SQLComputeStub(channel).ExecuteSQL(pb.SQLRequest(database_id=cached.client.database_id,sql=SQL,
                    search_options=cached.client.search_options.to_proto()),timeout=20)
        with ThreadPoolExecutor(1) as pool:
            job=pool.submit(list,call)
            try:
                assert entered.wait(5);call.cancel()
            finally:release.set()
            with pytest.raises(grpc.RpcError):job.result(timeout=5)
            assert finished.wait(5)
    assert cached.client.data_cache_stats()['process']['workspace_bytes']==0


def test_live_compute_rejects_same_revision_bytes_after_storage_restore(tmp_path):
    with ExitStack() as cleanup:
        storage,port,backend=create_catalog_server(tmp_path,'127.0.0.1:0');storage.start()
        save_dataset(generate_dataset(300,4,seed=32),tmp_path/'imports/source.npz')
        server,cport,compute=create_compute_server(f'127.0.0.1:{port}','127.0.0.1:0',timeout=30,
            data_cache_config=CacheConfig(max_bytes=1024**2,scan_admission=True))
        server.start();cleanup.callback(compute.close);cleanup.callback(lambda:server.stop(0).wait())
        try:
            with SessionClient(f'127.0.0.1:{cport}',timeout=30) as client:
                client.open_database('demo',create=True);client.import_dataset('source.npz','items')
                client.execute(SQL);warm=client.execute(SQL)
                assert warm['execution_plan']['data_cache']['hit_rows']==300
                path=backend.catalog.database(client.database_id).path
                storage.stop(0).wait();backend.close()
                with duckdb.connect(str(path)) as connection:connection.execute('UPDATE items SET vector=[0,0,0,0] WHERE id=1')
                storage,_,backend=create_catalog_server(tmp_path,f'127.0.0.1:{port}');storage.start()
                grpc.channel_ready_future(compute.sql.channel).result(timeout=10)
                restored=client.execute(SQL)
                assert restored['revision']==warm['revision'] and restored['rows'][0]==[1]
                assert restored['execution_plan']['data_cache']['hit_rows']==0
                assert restored['execution_plan']['data_cache']['scope']['storage_epoch']!=warm['execution_plan']['data_cache']['scope']['storage_epoch']
        finally:storage.stop(0).wait();backend.close()
