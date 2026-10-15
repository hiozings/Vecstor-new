from pathlib import Path
from contextlib import ExitStack
import json
import pytest
from vecstor.datasets import generate_dataset,save_dataset
from vecstor.rpc.acceptance import ServiceProcess
from vecstor.rpc.shell_acceptance import TerminalProcess
from vecstor.rpc.sql_client import SessionClient


@pytest.mark.parametrize('fused',[False,True])
def test_real_gpu_terminal_and_worker_landlock(tmp_path,fused):
    root=tmp_path/'storage';save_dataset(generate_dataset(2048,3),root/'imports/a.npz')
    with ExitStack() as close:
        storage=ServiceProcess(['storage','--data-root',str(root),'--listen','127.0.0.1:0'],tmp_path/'storage.log');close.callback(storage.stop)
        compute=ServiceProcess(['compute','--storage',storage.address,'--listen','127.0.0.1:0','--isolate-storage-dir',str(root)]+(['--fused-candidates'] if fused else []),tmp_path/'compute.log');close.callback(compute.stop)
        terminal=TerminalProcess(compute.address,tmp_path/'session.txt');close.callback(terminal.stop)
        for command in ['.open demo --create','.mode json','.import a.npz items','.index build items --nlist 1 --device gpu',"SELECT id FROM items ORDER BY vector_distance(vector,[0,0,0]) LIMIT 10 SETTINGS device='gpu',mode='approx',path='search-first';"]:
            terminal.command(command)
        terminal.quit();terminal.stop()
        transcript=(tmp_path/'session.txt').read_text()
        assert 'Error:' not in transcript,transcript[-3000:]
        assert '"actual_device": "gpu"' in transcript
        with SessionClient(compute.address) as c:
            c.open_database('demo')
            gpu=next(i for i in c.list_indexes() if i['device']=='gpu')
            assert gpu['isolation']=={'directory_denied':True,'file_denied':True}
