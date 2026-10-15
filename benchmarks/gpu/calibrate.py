"""GPU-only frozen calibration; unsupported groups remain in the report."""
import argparse
from dataclasses import asdict
import ctypes
import hashlib
import json
import math
from pathlib import Path
import platform
import runpy
import subprocess
import sys
import time

ROOT=Path(__file__).resolve().parents[2];sys.path.insert(0,str(ROOT/'src'))
import faiss
import numpy as np
from vecstor.datasets import generate_dataset,save_dataset
from vecstor.index.types import BuildConfig
from vecstor.index.build_worker import build_in_worker

helper=runpy.run_path(str(ROOT/'benchmarks/p3/calibrate.py'))


class GpuProbe:
    def __init__(self,cpu):
        self.resource=faiss.StandardGpuResources();self.resource.setTempMemory(128*1024**2)
        options=faiss.GpuClonerOptions();options.useFloat16=False;options.indicesOptions=faiss.INDICES_64_BIT
        self.index=faiss.index_cpu_to_gpu(self.resource,0,cpu._index,options);self.nlist=cpu.nlist
    def search(self,q,count,nprobe,allowed=None):
        if allowed is not None:raise ValueError('GPU selector unsupported')
        if count>2048 or nprobe>2048:raise ValueError('GPU probe exceeds capability')
        d,i=self.index.search(np.asarray(q,dtype=np.float32).reshape(1,-1),count,params=faiss.SearchParametersIVF(nprobe=nprobe))
        mask=i[0]>=0;return d[0][mask],i[0][mask]


def run(output,sizes=(10000,100000),dimension=128,distributions=('independent','correlated','clustered'),
        calibration_count=200,evaluation_count=500,selectivities=(.001,.01,.1,.5,1.),ks=(10,100),seed=20261015):
    out=Path(output);out.mkdir(parents=True,exist_ok=False);faiss.omp_set_num_threads(1)
    if faiss.get_num_gpus()<1:raise ValueError('Real GPU is required')
    config=dict(sizes=list(sizes),dimension=dimension,distributions=list(distributions),calibration_count=calibration_count,
        evaluation_count=evaluation_count,selectivities=list(selectivities),ks=list(ks),seed=seed,faiss=faiss.__version__,numpy=np.__version__,python=platform.python_version(),device='gpu',precision='float32',max_search_k=2048)
    config['gpu_model']=subprocess.check_output(['nvidia-smi','--query-gpu=name','--format=csv,noheader'],text=True).splitlines()[0]
    config['source_sha256']=hashlib.sha256(b''.join(p.read_bytes() for p in sorted((ROOT/'src').rglob('*.py')))).hexdigest()
    config['runner_sha256']=hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
    (out/'config.json').write_text(json.dumps(config,indent=2));groups=[];began=time.monotonic()
    with (out/'heldout.jsonl').open('x') as raw:
        for n in sizes:
            for distribution in distributions:
                directory=out/f'{n}-{distribution}';directory.mkdir()
                data=helper['clustered_dataset'](n,dimension,seed) if distribution=='clustered' else generate_dataset(n,dimension,seed,distribution)
                save_dataset(data,directory/'dataset.npz')
                cal,held=helper['query_splits'](dimension,calibration_count,evaluation_count,seed+n)
                np.savez(directory/'queries.npz',calibration=cal,evaluation=held)
                cfg=BuildConfig().resolved(n);cpu,stats=build_in_worker(data.ids,data.vectors,cfg,time.monotonic()+60)
                clone_start=time.perf_counter();gpu=GpuProbe(cpu)
                stats.update(clone_ms=(time.perf_counter()-clone_start)*1000,config=asdict(cfg),byte_size=cpu.byte_size)
                (directory/'build.json').write_text(json.dumps(stats,indent=2))
                for k in ks:
                    truths=helper['prepare_truths'](data,np.concatenate((cal,held)),selectivities,k)
                    for selectivity in selectivities:
                        work=dict(selectivity=float(selectivity),path='search-first',k=k)
                        group=dict(size=n,distribution=distribution,workload=work)
                        ct=truths[float(selectivity)][:calibration_count];et=truths[float(selectivity)][calibration_count:]
                        for i,t in enumerate(et):t['query_id']=i
                        if any(len(t['truth_ids'])<k for t in ct):
                            group.update(status='unsupported',reason='truth_smaller_than_k_no_gpu_exact_path')
                            groups.append(group);print(n,distribution,k,selectivity,group['status'],flush=True);continue
                        ranks=[t['required_global_L'] for t in ct]
                        initial=min(n,2048,max(64,2**math.ceil(math.log2(max(1,1.25*np.percentile(ranks,99))))))
                        grid=[];chosen=None
                        for probe in sorted({min(gpu.nlist,p) for p in (8,32,64,80,96,128,160,192,224,240,gpu.nlist)}):
                            settings=dict(nprobe=probe,candidates=initial,max_candidates=min(n,2048),max_rounds=6)
                            measured=helper['_measure'](data,cal,work,settings,gpu,ct)
                            summary=helper['summarize'](measured);grid.append(dict(settings=settings,summary=summary))
                            if chosen is None and summary['incomplete_queries']==0 and summary['ann_mean_recall']>=.97:chosen=settings
                        frozen=dict(format_version=2,workload=work,settings=chosen,grid=grid,calibration_query_digests=list(map(helper['digest_query'],cal)),
                                    dataset_sha256=helper['dataset_digest'](data),nlist=gpu.nlist,
                                    execution=dict(device='gpu',precision='float32',faiss_version=faiss.__version__,gpu_model=config['gpu_model']))
                        (directory/f'{selectivity:g}-k{k}.json').write_text(json.dumps(frozen,indent=2))
                        if chosen is None:
                            group.update(status='unsupported',reason='gpu_candidate_cap_or_quality_target',best_calibration=max(g['summary']['ann_mean_recall'] for g in grid))
                        else:
                            measured=helper['_measure'](data,held,work,chosen,gpu,et)
                            summary=helper['summarize'](measured)
                            for r in measured:raw.write(json.dumps(dict(size=n,distribution=distribution,workload=work,settings=chosen,**r))+'\n')
                            raw.flush()
                            passed=summary['incomplete_queries']==0 and summary['exact_queries']==0 and summary['ann_mean_recall']>=.95
                            cpu_records=helper['_measure'](data,held,work,chosen,cpu,et)
                            cpu_summary=helper['summarize'](cpu_records)
                            group.update(status='passed' if passed else 'failed',settings=chosen,summary=summary,cpu_summary=cpu_summary)
                        groups.append(group);print(n,distribution,k,selectivity,group['status'],flush=True)
                del gpu,cpu,data
    report=dict(passed=all(g['status']!='failed' for g in groups),groups=groups,elapsed_seconds=time.monotonic()-began,
                scope='GPU search-first only; unsupported groups explicitly retained; no CPU fallback; timings exclude RPC/oracle preparation')
    (out/'report.json').write_text(json.dumps(report,indent=2));return report

if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--output-dir',required=True)
    p.add_argument('--sizes',type=int,nargs='+',default=[10000,100000]);p.add_argument('--dimension',type=int,default=128)
    p.add_argument('--calibration-queries',type=int,default=200);p.add_argument('--evaluation-queries',type=int,default=500)
    a=p.parse_args();r=run(a.output_dir,sizes=a.sizes,dimension=a.dimension,calibration_count=a.calibration_queries,evaluation_count=a.evaluation_queries)
    print(json.dumps({'passed':r['passed'],'groups':len(r['groups'])}));raise SystemExit(0 if r['passed'] else 2)
