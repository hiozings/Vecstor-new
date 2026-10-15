"""Isolated native GPU capability probes. No application data is read."""
import argparse,json,os,subprocess,sys,time
from pathlib import Path


def probe(case):
    import faiss
    import numpy as np
    faiss.omp_set_num_threads(1)
    assert faiss.get_num_gpus()>0,'No accessible GPU'
    rng=np.random.default_rng(20261015)
    x=rng.random((4096,8),dtype=np.float32)
    ids=np.arange(len(x),dtype=np.int64)+2**53+1
    cpu=faiss.IndexIVFFlat(faiss.IndexFlatL2(8),8,16)
    cpu.train(x);cpu.add_with_ids(x,ids)
    resource=faiss.StandardGpuResources();resource.setTempMemory(32*1024**2)
    options=faiss.GpuClonerOptions();options.useFloat16=False;options.indicesOptions=faiss.INDICES_64_BIT
    gpu=faiss.index_cpu_to_gpu(resource,0,cpu,options)
    query=x[:1].copy();count=2049 if case=='k2049' else 2048 if case=='k2048' else 10
    selector=faiss.IDSelectorBatch(ids[100:200]) if case=='selector' else None
    params=faiss.SearchParametersIVF(nprobe=2049 if case=='nprobe2049' else 2048 if case=='nprobe2048' else 16,sel=selector) if selector else faiss.SearchParametersIVF(nprobe=2049 if case=='nprobe2049' else 2048 if case=='nprobe2048' else 16)
    d,i=gpu.search(query,count,params=params)
    assert np.isfinite(d).all()
    if case=='selector':assert set(i[0]).issubset(set(ids[100:200].tolist())),'GPU ignored selector'
    else:assert int(i[0,0])==int(ids[0]),'int64 ID changed'
    if case=='roundtrip':
        before=i.copy();gpu.search(x[3:4],10,params=faiss.SearchParametersIVF(nprobe=1))
        assert np.array_equal(before,gpu.search(query,10,params=faiss.SearchParametersIVF(nprobe=16))[1])
        restored=faiss.index_gpu_to_cpu(gpu)
        assert restored.ntotal==4096
        del gpu,resource
    print(json.dumps({'case':case,'passed':True,'faiss':faiss.__version__,'devices':faiss.get_num_gpus(),'first_id':int(i[0,0])}))


def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--output-dir');p.add_argument('--cpu-only',action='store_true');p.add_argument('--probe')
    a=p.parse_args()
    if a.probe:probe(a.probe);return
    if not a.output_dir:p.error('--output-dir is required')
    out=Path(a.output_dir);out.mkdir(parents=True,exist_ok=False)
    report={'gpu_verified':False,'status':'skipped' if a.cpu_only else 'failed','probes':[]}
    if not a.cpu_only:
        for case in ('roundtrip','k2048','k2049','nprobe2048','nprobe2049','selector'):
            run=subprocess.run([sys.executable,__file__,'--probe',case],capture_output=True,text=True,timeout=60,env=dict(os.environ,OMP_NUM_THREADS='1',OPENBLAS_NUM_THREADS='1'))
            (out/f'{case}.log').write_text(run.stdout+run.stderr)
            report['probes'].append({'case':case,'returncode':run.returncode,'passed':run.returncode==0})
        passed={x['case']:x['passed'] for x in report['probes']}
        report.update(gpu_verified=passed['roundtrip'] and passed['k2048'],status='passed' if passed['roundtrip'] and passed['k2048'] else 'failed',
                      max_search_k=2048 if passed['k2048'] and not passed['k2049'] else None,
                      max_nprobe=2048 if passed['nprobe2048'] and not passed['nprobe2049'] else None,
                      supports_selector=passed['selector'],supports_int64_ids=passed['roundtrip'],
                      memory_statistics='device nvidia-smi snapshots; subprocess termination releases context',
                      timing='synchronous host search, no kernel-only timer')
        try:report['device']=subprocess.check_output(['nvidia-smi','--query-gpu=name,driver_version,memory.total,memory.free','--format=csv'],text=True)
        except (OSError,subprocess.CalledProcessError):report['device']='unknown'
    (out/'report.json').write_text(json.dumps(report,indent=2));print(json.dumps(report))
    if not a.cpu_only and not report['gpu_verified']:raise SystemExit(2)

if __name__=='__main__':main()
