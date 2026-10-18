"""Repeated artifact publication/recovery costs with real process observations."""
import argparse
import json
from pathlib import Path
import runpy
import sys
import threading
import time

ROOT=Path(__file__).resolve().parents[2]
sys.path.insert(0,str(ROOT/'src'))
from vecstor.rpc.p4_index_acceptance import run_acceptance

resource_helpers=runpy.run_path(str(ROOT/'benchmarks/gpu/run.py'))


class Monitor:
    def __init__(self,index_root,gpu):
        self.index_root=index_root;self.records={};self.lock=threading.Lock();self.stop=threading.Event()
        self.memory=resource_helpers['DeviceMemory']() if gpu else None
        self.vram=0;self.disk=0
        self.thread=threading.Thread(target=self.loop,daemon=True);self.thread.start()

    def observe(self,label,pid):
        with self.lock:self.records[pid]=dict(role=label,pid=pid,peak_rss_bytes=0,cpu_seconds=0.)
        self.sample()

    def sample(self):
        with self.lock:
            for pid,record in self.records.items():
                rss,cpu=resource_helpers['process_stats'](pid)
                record['peak_rss_bytes']=max(record['peak_rss_bytes'],rss)
                record['cpu_seconds']=max(record['cpu_seconds'],cpu)
            if self.memory:self.vram=max(self.vram,self.memory.used())
            size=0
            try:
                for file in self.index_root.rglob('*'):
                    try:
                        if file.is_file():size+=file.stat().st_size
                    except OSError:pass
            except OSError:pass
            self.disk=max(self.disk,size)

    def loop(self):
        while not self.stop.wait(.005):self.sample()

    def close(self):
        self.stop.set();self.thread.join();self.sample()
        if self.memory:self.memory.close()
        return dict(processes=list(self.records.values()),index_disk_peak_bytes=self.disk,
                    sampled_device_vram_peak_bytes=self.vram if self.memory else None,
                    scope='5ms samples, may miss peaks; per-PID CPU is process-lifetime through final sample; device VRAM includes other applications')


def run_lifecycle_benchmark(output_dir,sizes=(10000,100000),repeats=3,concurrency=(1,4,8),
                            distributions=('independent','correlated'),dimension=128,require_gpu=False):
    if repeats<1 or any(n<1024 for n in sizes) or not concurrency or any(c<1 for c in concurrency):
        raise ValueError('Invalid lifecycle benchmark matrix')
    output=Path(output_dir).resolve();output.mkdir(parents=True,exist_ok=False)
    configuration=dict(sizes=list(sizes),repeats=repeats,concurrency=list(concurrency),distributions=list(distributions),
                       dimension=dimension,require_gpu=require_gpu,seed=20261016)
    (output/'config.json').write_text(json.dumps(configuration,indent=2)+'\n')
    report=dict(passed=False,runs=[],scope='localhost lifecycle costs; query samples use full nprobe and one fixed query; no universal latency or speedup claim')
    try:
        for size in sizes:
            for distribution in distributions:
                for repeat in range(repeats):
                    directory=output/f'{size}-{distribution}-r{repeat+1}'
                    monitor=Monitor(directory/'storage/indexes',require_gpu)
                    result=None
                    try:
                        result=run_acceptance(directory,size,dimension,20261016,require_gpu,distribution,concurrency,monitor.observe)
                    finally:resources=monitor.close()
                    stats=result['second_publication']['statistics']
                    resources['build_worker_peak_rss_bytes']=stats.get('worker_peak_rss_bytes')
                    resources['verifier_peak_rss_bytes']=stats.get('verifier_peak_rss_bytes')
                    compact=[{key:value for key,value in group.items() if key!='samples'} for group in result['query_costs']]
                    entry=dict(rows=size,distribution=distribution,repeat=repeat+1,report=str(directory.relative_to(output)/'report.json'),
                        phases=result['phases'],publication_statistics=stats,lag_to_publish_ms=result['lag_to_publish_ms'],lag_to_ready_ms=result['lag_to_ready_ms'],
                        query_costs=compact,resources=resources,processes_running_after_cleanup=result['processes_running_after_cleanup'])
                    report['runs'].append(entry)
                    (directory/'resources.json').write_text(json.dumps(resources,indent=2)+'\n')
                    (output/'report.json').write_text(json.dumps(report,indent=2)+'\n')
                    print(json.dumps(dict(rows=size,distribution=distribution,repeat=repeat+1,passed=result['passed'])),flush=True)
        report['passed']=all(not item['processes_running_after_cleanup'] for item in report['runs'])
    except Exception as error:
        report['error']=str(error);raise
    finally:(output/'report.json').write_text(json.dumps(report,indent=2)+'\n')
    return report


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output-dir',required=True)
    parser.add_argument('--sizes',type=int,nargs='+',default=[10000,100000])
    parser.add_argument('--repeats',type=int,default=3)
    parser.add_argument('--concurrency',type=int,nargs='+',default=[1,4,8])
    parser.add_argument('--distributions',nargs='+',choices=('independent','correlated'),default=['independent','correlated'])
    parser.add_argument('--dimension',type=int,default=128)
    parser.add_argument('--require-gpu',action='store_true')
    args=parser.parse_args()
    print(json.dumps({'passed':run_lifecycle_benchmark(args.output_dir,args.sizes,args.repeats,args.concurrency,args.distributions,args.dimension,args.require_gpu)['passed']}))
