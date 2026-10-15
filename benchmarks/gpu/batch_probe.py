"""Native batch feasibility/cost probe; not a deployed request scheduler."""
import argparse,json,sys,time
from pathlib import Path
import numpy as np
import faiss
ROOT=Path(__file__).resolve().parents[2];sys.path.insert(0,str(ROOT/'src'))
from vecstor.datasets import load_dataset
from vecstor.index.build_worker import build_in_worker
from vecstor.index.types import BuildConfig

DEFAULT_CASES=[(10000,'independent',.1,10),(100000,'independent',.1,10),(100000,'independent',1.,100)]


def run(calibration_dir,output_dir,selected=None,samples=500,repeats=3):
    source=Path(calibration_dir);out=Path(output_dir);out.mkdir(parents=True,exist_ok=False)
    faiss.omp_set_num_threads(1);groups=[]
    for n,distribution,s,k in selected or DEFAULT_CASES:
        folder=source/f'{n}-{distribution}';data=load_dataset(folder/'dataset.npz')
        cfg=BuildConfig(**json.loads((folder/'build.json').read_text())['config'])
        frozen=json.loads((folder/f'{s:g}-k{k}.json').read_text());settings=frozen['settings']
        with np.load(folder/'queries.npz',allow_pickle=False) as q:queries=np.ascontiguousarray(q['evaluation'][:samples])
        if len(queries)!=samples or settings is None:raise ValueError('Missing frozen workload')
        ids=data.ids+(2**53+1) # exercise exact int64 labels in both scalar and batch calls
        cpu,_=build_in_worker(ids,data.vectors,cfg,time.monotonic()+60)
        resources=faiss.StandardGpuResources();resources.setTempMemory(128*1024**2)
        options=faiss.GpuClonerOptions();options.useFloat16=False;options.indicesOptions=faiss.INDICES_64_BIT
        gpu=faiss.index_cpu_to_gpu(resources,0,cpu._index,options)
        params=faiss.SearchParametersIVF(nprobe=settings['nprobe']);count=settings['candidates']
        reference=[gpu.search(q.reshape(1,-1),count,params=params)[1][0] for q in queries]
        for size in (1,4,8):
            elapsed=[];equal=True;ordered=True;total=0
            for repeat in range(repeats):
                for start in range(0,len(queries),size):
                    batch=queries[start:start+size];began=time.perf_counter();distances,found=gpu.search(batch,count,params=params)
                    elapsed.append((time.perf_counter()-began)*1000);total+=len(batch)
                    assert found.dtype==np.int64 and np.isfinite(distances[found!=-1]).all()
                    for j,row in enumerate(found):
                        expected=reference[start+j];equal &= set(map(int,row))==set(map(int,expected));ordered &= np.array_equal(row,expected)
                        assert np.all((row==-1)|(row>=2**53+1))
            group=dict(size=n,distribution=distribution,selectivity=s,k=k,batch_size=size,queries=total,
                candidate_sets_equal=bool(equal),candidate_order_equal=bool(ordered),amortized_ms=sum(elapsed)/total,
                native_call_p50_ms=float(np.median(elapsed)),native_call_p95_ms=float(np.percentile(elapsed,95)),settings=settings)
            groups.append(group);print(json.dumps(group),flush=True)
        del gpu,resources,cpu
    report=dict(passed=all(g['candidate_sets_equal'] for g in groups),groups=groups,
                scope='Native-only batch search; same parameters, int64 labels >2^53; no runtime queue/RPC/end-to-end speed claim')
    (out/'report.json').write_text(json.dumps(report,indent=2));return report

if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--calibration-dir',required=True);p.add_argument('--output-dir',required=True)
    a=p.parse_args();r=run(a.calibration_dir,a.output_dir);raise SystemExit(0 if r['passed'] else 2)
