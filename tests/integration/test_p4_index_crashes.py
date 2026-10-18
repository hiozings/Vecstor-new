from contextlib import contextmanager
import json
from pathlib import Path
import subprocess
import sys
import time

import pytest

from vecstor.compute.service import create_compute_server
from vecstor.datasets import generate_dataset,save_dataset
from vecstor.rpc.sql_client import SessionClient
from test_p4_index_recovery import running


@contextmanager
def crashing_storage(root,point,output):
    path=output/'fault.log';marker=output/'crash-marker'
    with path.open('x') as log:
        process=subprocess.Popen([sys.executable,str(Path(__file__).resolve().parents[1]/'p4_fault_server.py'),
            '--root',str(root),'--point',point,'--marker',str(marker)],stdout=log,stderr=subprocess.STDOUT)
        try:
            deadline=time.monotonic()+20
            address=None
            while time.monotonic()<deadline and process.poll() is None:
                for line in path.read_text().splitlines():
                    try:event=json.loads(line)
                    except ValueError:continue
                    if event.get('event')=='ready':address=event['address']
                if address:break
                time.sleep(.02)
            assert address,path.read_text()
            yield address,process,marker
        finally:
            if process.poll() is None:process.terminate()
            try:process.wait(timeout=5)
            except subprocess.TimeoutExpired:process.kill();process.wait()


@pytest.mark.parametrize('point',['upload_before_seal','validate_before_install','before_commit','after_commit'])
def test_publication_crash_windows_recover_exact_commit_status(tmp_path,point):
    root=tmp_path/'storage'
    save_dataset(generate_dataset(2048,4,seed=22),root/'imports/source.npz')
    with crashing_storage(root,point,tmp_path) as (address,process,marker):
        server,port,backend=create_compute_server(address,'127.0.0.1:0',timeout=60,index_source='published');server.start()
        try:
            with SessionClient(f'127.0.0.1:{port}',timeout=10) as client:
                client.open_database('demo',True);client.import_dataset('source.npz','items')
                with pytest.raises(ValueError):client.publish_index('items',nlist=1,operation_id='crash-publication',timeout=10)
            assert process.wait(timeout=5)<0 and marker.read_text()==point
        finally:server.stop(0).wait();backend.close()
    with running(root) as env:
        status=env.client.index_publication_status('crash-publication')
        if point=='after_commit':
            assert status['state']=='COMMITTED'
            restored=env.client.load_index('items')
            assert restored['descriptor']==status['descriptor']
            assert restored['statistics']['trained'] is False
        else:
            assert status['state']=='ABORTED'
            assert env.client.inspect_published_index('items')['state']=='missing'
            assert not list((root/'indexes/objects').glob('*/*/index.faiss'))


def test_gc_crash_after_retire_resumes_fixed_set(tmp_path):
    root=tmp_path/'storage'
    save_dataset(generate_dataset(2048,4,seed=22),root/'imports/source.npz')
    with crashing_storage(root,'gc_before_delete',tmp_path) as (address,process,marker):
        server,port,backend=create_compute_server(address,'127.0.0.1:0',timeout=60,index_source='published');server.start()
        try:
            with SessionClient(f'127.0.0.1:{port}',timeout=10) as client:
                client.open_database('demo',True);client.import_dataset('source.npz','items')
                first=client.publish_index('items',nlist=1,operation_id='first')
                second=client.publish_index('items',nlist=2,operation_id='second')
                with pytest.raises(ValueError):client.prune_index_artifacts(keep_last=1,dry_run=False,operation_id='gc',timeout=10)
            assert process.wait(timeout=5)<0 and marker.read_text()=='gc_before_delete'
        finally:server.stop(0).wait();backend.close()
    with running(root) as env:
        status=env.client.index_publication_status('gc')
        assert status['state']=='COMMITTED'
        assert status['result']['deleted']==[first['descriptor']['artifact_id']]
        assert env.client.inspect_published_index('items')['descriptor']==second['descriptor']
        assert env.client.prune_index_artifacts(keep_last=1,dry_run=False,operation_id='gc')['deleted']==status['result']['deleted']
