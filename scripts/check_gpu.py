"""Real GPU, terminal, storage isolation and process lifecycle acceptance."""
import argparse
from contextlib import ExitStack
import json
import os
from pathlib import Path
import signal
import sys
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'src'))
from vecstor.datasets import generate_dataset,save_dataset
from vecstor.rpc.acceptance import ServiceProcess
from vecstor.rpc.shell_acceptance import TerminalProcess
from vecstor.rpc.sql_client import SessionClient


def run(output,rows=10000):
    out=Path(output).resolve();out.mkdir(parents=True,exist_ok=False)
    root=out/'storage';save_dataset(generate_dataset(rows,3),root/'imports/source.npz')
    sql='SELECT id,vector_distance(vector,[0,0,0]) AS distance FROM items WHERE value<1000 ORDER BY distance LIMIT 10'
    settings=" SETTINGS device='gpu',mode='approx',path='search-first',nprobe=1"
    processes=[];isolations=[]
    with ExitStack() as cleanup:
        storage=ServiceProcess(['storage','--data-root',str(root),'--listen','127.0.0.1:0'],out/'storage.log');cleanup.callback(storage.stop);processes.append(storage.process)
        def start(name,extra=()):
            compute=ServiceProcess(['compute','--storage',storage.address,'--listen','127.0.0.1:0','--isolate-storage-dir',str(root),*extra],out/f'{name}.log')
            cleanup.callback(compute.stop);processes.append(compute.process)
            event=next(json.loads(line) for line in (out/f'{name}.log').read_text().splitlines() if 'filesystem_isolation' in line)
            assert event['directory_denied'] and event['file_denied'];isolations.append(event)
            return compute
        compute=start('compute')
        terminal=TerminalProcess(compute.address,out/'session.txt');cleanup.callback(terminal.stop)
        for command in ['.open demo --create','.mode json','.import source.npz items','.index build items --nlist 1 --device gpu',sql+settings+';']:
            terminal.command(command)
        terminal.quit();terminal.stop();transcript=(out/'session.txt').read_text()
        assert 'Error:' not in transcript and '"actual_device": "gpu"' in transcript
        with SessionClient(compute.address) as c:
            c.open_database('demo');expected=c.execute(sql)['rows'];result=c.execute(sql+settings)
            assert result['rows']==expected and result['execution_plan']['actual_device']=='gpu'
            built=next(v for v in c.list_indexes() if v['device']=='gpu');worker_isolation=built['isolation']
            assert worker_isolation=={'directory_denied':True,'file_denied':True}
            os.kill(built['worker_pid'],signal.SIGKILL)
            try:c.execute(sql+settings);raise AssertionError('Dead worker query succeeded')
            except ValueError as error:assert 'gpu_' in str(error)
            assert c.execute(sql)['rows']==expected
            c.build_index('items',nlist=1,device='gpu');assert c.execute(sql+settings)['rows']==expected
        compute.stop();compute=start('restarted')
        with SessionClient(compute.address) as c:
            c.open_database('demo')
            try:c.execute(sql+settings);raise AssertionError('Restart reused GPU replica')
            except ValueError as error:assert 'gpu_replica_missing' in str(error)
            after_restart=c.execute(sql+settings+",device_fallback='cpu'")['execution_plan']
            c.build_index('items',nlist=1,device='gpu');after_rebuild=c.execute(sql+settings)['execution_plan']
            storage.stop()
            try:c.execute(sql+settings);raise AssertionError('Stopped storage query succeeded')
            except ValueError as error:assert 'UNAVAILABLE' in str(error)
    report=dict(passed=True,rows=rows,query=result['execution_plan'],worker_isolation=worker_isolation,isolation=isolations,
                after_restart=after_restart,after_rebuild=after_rebuild,processes_running_after_cleanup=[p.pid for p in processes if p.poll() is None])
    assert not report['processes_running_after_cleanup']
    (out/'report.json').write_text(json.dumps(report,indent=2));return report

if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--output-dir',required=True);p.add_argument('--rows',type=int,default=10000)
    a=p.parse_args();print(json.dumps({'passed':run(a.output_dir,a.rows)['passed']}))
