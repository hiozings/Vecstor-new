"""Frozen CPU IVF calibration and disjoint heldout evaluation (no online Recall)."""
import argparse
from dataclasses import asdict
import hashlib
import json
import math
from pathlib import Path
import platform
import resource
import sys
import time

sys.path.insert(0,str(Path(__file__).resolve().parents[2]/'src'))
import faiss
import numpy as np
from vecstor.data import Dataset
from vecstor.datasets import generate_dataset, save_dataset, load_dataset
from vecstor.index.build_worker import build_in_worker
from vecstor.index.types import BuildConfig


def digest_query(q):
    return hashlib.sha256(np.ascontiguousarray(q,dtype='<f4').tobytes()).hexdigest()


def query_splits(dimension, calibration_count, evaluation_count, seed):
    if min(dimension,calibration_count,evaluation_count)<1:raise ValueError('Query counts and dimension must be positive')
    seeds=np.random.SeedSequence(seed).spawn(2)
    arrays=[np.random.default_rng(s).random((n,dimension),dtype=np.float32)
            for s,n in zip(seeds,[calibration_count,evaluation_count])]
    if set(map(digest_query,arrays[0])) & set(map(digest_query,arrays[1])):raise ValueError('Query split overlap')
    return tuple(arrays)


def dataset_digest(dataset):
    digest=hashlib.sha256()
    for value in (dataset.ids,dataset.vectors,dataset.values):digest.update(np.ascontiguousarray(value).tobytes())
    return digest.hexdigest()


def prepare_truths(dataset,queries,selectivities,k):
    """Blockwise float64 oracle, one full ordering per query, compact retained truth."""
    result={float(s):[] for s in selectivities}
    for qi,q in enumerate(queries):
        distances=np.empty(dataset.size,dtype=np.float64)
        for start in range(0,dataset.size,4096):
            delta=dataset.vectors[start:start+4096].astype(np.float64)-q.astype(np.float64)
            distances[start:start+4096]=np.sum(delta*delta,axis=1)
        order=np.lexsort((dataset.ids,distances))
        for s in selectivities:
            positions=np.flatnonzero(dataset.values[order]<int(dataset.size*s))[:k]
            truth=dataset.ids[order[positions]].tolist()
            result[float(s)].append({'query_id':qi,'query_digest':digest_query(q),'truth_ids':truth,
                'required_global_L':int(positions[-1])+1 if len(positions) else 0})
    return result


def summarize(records):
    values=[r['recall'] for r in records if r['recall'] is not None]
    ann=[r['recall'] for r in records if r['actual_mode']=='approx' and r['recall'] is not None]
    interval=None
    if values:
        x=np.asarray(values);rng=np.random.default_rng(731)
        means=np.mean(x[rng.integers(0,len(x),(2000,len(x)))],axis=1)
        interval=np.percentile(means,[2.5,97.5]).tolist()
    elapsed=[r['elapsed_ms'] for r in records]
    return {'queries':len(records),'mean_recall':float(np.mean(values)) if values else None,
            'ann_mean_recall':float(np.mean(ann)) if ann else None,'min_recall':min(values) if values else None,
            'bootstrap_95':interval,'empty_truth_queries':len(records)-len(values),
            'exact_queries':sum(r['actual_mode']=='exact' for r in records),
            'incomplete_queries':sum(not r['complete'] for r in records),
            'p50_ms':float(np.percentile(elapsed,50)) if elapsed else None,
            'p95_ms':float(np.percentile(elapsed,95)) if elapsed else None}


def _measure(dataset,queries,workload,settings,index,truths):
    ids=dataset.ids;sort=np.argsort(ids);sorted_ids=ids[sort]
    s,k,path=workload['selectivity'],workload['k'],workload['path']
    legal=dataset.values<int(dataset.size*s);allowed=ids[legal]
    output=[]
    for q,truth in zip(queries,truths):
        began=time.perf_counter();expected=truth['truth_ids'];seen=set();passed=[];rounds=[]
        probe=settings['nprobe'];length=settings['candidates'];budget=settings['max_candidates']
        if not expected:
            output.append(dict(truth,returned_ids=[],recall=None,complete=True,actual_mode='exact',
                               fallback_reason='exact_empty',elapsed_ms=(time.perf_counter()-began)*1000,rounds=[]));continue
        if len(expected)<k:
            output.append(dict(truth,returned_ids=expected,recall=1.0,complete=True,actual_mode='exact',
                               fallback_reason='truth_smaller_than_k',elapsed_ms=0.0,rounds=[]));continue
        for step in range(settings['max_rounds']):
            _,labels=index.search(q,length,probe,allowed if path=='filter-first' else None)
            new=[int(i) for i in labels if i not in seen][:max(0,budget-len(seen))];seen.update(new)
            loc=sort[np.searchsorted(sorted_ids,new)]
            passed.extend(ids[loc[legal[loc]]].tolist())
            rounds.append({'nprobe':probe,'L':length,'unique_candidates':len(seen),'matched':len(passed)})
            if len(passed)>=k:break
            if len(seen)>=budget:break
            next_length=min(budget,length*2);next_probe=min(index.nlist,probe*2)
            if (next_length,next_probe)==(length,probe):break
            length,probe=next_length,next_probe
        labels=np.asarray(passed,dtype=np.int64)
        loc=sort[np.searchsorted(sorted_ids,labels)]
        delta=dataset.vectors[loc].astype(np.float64)-q.astype(np.float64)
        distances=np.sum(delta*delta,axis=1)
        found=labels[np.lexsort((labels,distances))][:k].tolist()
        complete=len(found)==len(expected)
        output.append(dict(truth,returned_ids=found,recall=len(set(found)&set(expected))/len(expected),
            complete=complete,actual_mode='approx',fallback_reason=None if complete else 'strict_incomplete',
            elapsed_ms=(time.perf_counter()-began)*1000,rounds=rounds,
            exhaustive_buckets=any(r['nprobe']==index.nlist for r in rounds)))
    return output


def calibrate(dataset,calibration_queries,options_grid,workload,*,index=None,prepared=None):
    if workload['path'] not in ('filter-first','search-first') or not 0<=workload['selectivity']<=1 or workload['k']<1:
        raise ValueError('Invalid workload')
    config=BuildConfig().resolved(dataset.size)
    if index is None:index,_=build_in_worker(dataset.ids,dataset.vectors,config,time.monotonic()+60)
    truth=prepared if prepared is not None else prepare_truths(dataset,calibration_queries,[workload['selectivity']],workload['k'])[float(workload['selectivity'])]
    if len(truth)!=len(calibration_queries) or any(t['query_digest']!=digest_query(q) for t,q in zip(truth,calibration_queries)):
        raise ValueError('Oracle does not match calibration queries')
    ranks=[r['required_global_L'] for r in truth]
    initial=64 if workload['path']=='filter-first' else max(64,2**math.ceil(math.log2(max(1,1.25*np.percentile(ranks,99)))))
    initial=min(dataset.size,initial);budget=min(dataset.size,max(4096,initial*2))
    grid=[];chosen=None
    for probe in sorted(set(options_grid)):
        if type(probe) is not int or not 1<=probe<=index.nlist:raise ValueError('Invalid nprobe grid')
        settings={'nprobe':probe,'candidates':initial,'max_candidates':budget,'max_rounds':6}
        rows=_measure(dataset,calibration_queries,workload,settings,index,truth)
        summary=summarize(rows)
        grid.append({'settings':settings,'summary':summary})
        if chosen is None and summary['incomplete_queries']==0 and (summary['mean_recall'] is None or summary['mean_recall']>=.97):
            chosen=settings
    if chosen is None:raise ValueError('No calibrated setting meets quality/completeness; expand grid')
    return {'format_version':1,'workload':dict(workload),'settings':chosen,'grid':grid,
            'calibration_query_digests':[digest_query(q) for q in calibration_queries],
            'dataset_sha256':dataset_digest(dataset),'nlist':index.nlist,
            'calibration_target':.97,'evaluation_target':.95,
            'required_global_L_percentiles':np.percentile(ranks,[50,95,99,100]).tolist()}


def evaluate(dataset,heldout_queries,frozen_config,*,index=None,prepared=None):
    if set(map(digest_query,heldout_queries)) & set(frozen_config['calibration_query_digests']):raise ValueError('Heldout query overlap')
    if dataset_digest(dataset)!=frozen_config['dataset_sha256']:raise ValueError('Dataset mismatch')
    if index is None:index,_=build_in_worker(dataset.ids,dataset.vectors,BuildConfig(nlist=frozen_config['nlist']),time.monotonic()+60)
    if index.nlist!=frozen_config['nlist']:raise ValueError('Index configuration mismatch')
    w=frozen_config['workload']
    truth=prepared if prepared is not None else prepare_truths(dataset,heldout_queries,[w['selectivity']],w['k'])[float(w['selectivity'])]
    if len(truth)!=len(heldout_queries) or any(t['query_digest']!=digest_query(q) for t,q in zip(truth,heldout_queries)):
        raise ValueError('Oracle does not match evaluation queries')
    rows=_measure(dataset,heldout_queries,w,frozen_config['settings'],index,truth);summary=summarize(rows)
    return {'records':rows,'summary':summary,'passed':summary['incomplete_queries']==0 and
            (summary['mean_recall'] is None or summary['mean_recall']>=frozen_config['evaluation_target'])}


def clustered_dataset(rows,dimension,seed):
    rng=np.random.default_rng(seed);centres=rng.uniform(.1,.9,(32,dimension)).astype(np.float32)
    vectors=(centres[rng.integers(0,32,rows)]+rng.normal(0,.025,(rows,dimension))).astype(np.float32)
    ranks=rng.permutation(rows)
    return Dataset(np.arange(rows,dtype=np.int64),np.full(rows,'clustered'),ranks.astype(np.int64),ranks.astype(float),vectors)


def run(output,sizes=(10000,100000),distributions=('independent','correlated','clustered'),dimension=128,
        calibration_count=200,evaluation_count=500,seed=20261014,selectivities=(.001,.01,.1,.5,1.0),resume=False,source_ruling=None):
    output=Path(output);faiss.omp_set_num_threads(1)
    if not resume:output.mkdir(parents=True,exist_ok=False)
    elif not output.is_dir():raise ValueError('Resume output directory does not exist')
    if any(n<1024 for n in sizes):raise ValueError('IVF calibration requires at least 1024 rows')
    summaries=[];began=time.monotonic()
    config={'sizes':list(sizes),'distributions':list(distributions),'dimension':dimension,'calibration_count':calibration_count,
            'evaluation_count':evaluation_count,'seed':seed,'selectivities':list(selectivities),'k':10,
            'search_threads':1,'build_threads':2,'faiss':faiss.__version__,'numpy':np.__version__,'python':platform.python_version()}
    digest=hashlib.sha256();root=Path(__file__).resolve().parents[2]
    for path in sorted((root/'src').rglob('*.py')):digest.update(str(path.relative_to(root)).encode());digest.update(path.read_bytes())
    config['source_sha256']=digest.hexdigest();config['runner_sha256']=hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
    complete={}
    if resume:
        old=json.loads((output/'config.json').read_text())
        stable=['sizes','distributions','dimension','calibration_count','evaluation_count','seed','selectivities','k','faiss','numpy']
        if any(old.get(k)!=config[k] for k in stable):raise ValueError('Resume configuration mismatch')
        for field, label in (('source_sha256','source'),('runner_sha256','runner')):
            if old.get(field)!=config[field]:
                if not isinstance(source_ruling,str) or not source_ruling.strip():
                    raise ValueError(f'Resume configuration {label} mismatch; an audited equivalent-source ruling is required')
                audit=output/f'{label}-transitions.json'
                history=json.loads(audit.read_text()) if audit.exists() else []
                transition={'from':old.get(field),'to':config[field],'reason':source_ruling}
                if transition not in history:history.append(transition)
                audit.write_text(json.dumps(history,indent=2))
        for line in (output/'heldout.jsonl').read_text().splitlines():
            r=json.loads(line);key=(r['size'],r['distribution'],r['workload']['selectivity'],r['workload']['path'])
            complete.setdefault(key,[]).append(r)
        for records in complete.values():
            if len(records)>evaluation_count or len({r['query_digest'] for r in records})!=len(records):
                raise ValueError('Duplicate or excessive resume query evidence')
        # Validate saved provenance even for fully completed scenarios before reusing evidence.
        expected={(n,d,float(v),p) for n in sizes for d in distributions for v in selectivities
                  for p in ('filter-first','search-first')}
        if set(complete)-expected:raise ValueError('Unexpected resume workload')
        for n,distribution in {(key[0],key[1]) for key in complete}:
            directory=output/f'{n}-{distribution}'
            fingerprint=dataset_digest(load_dataset(directory/'dataset.npz'))
            cal,held=query_splits(dimension,calibration_count,evaluation_count,seed+n)
            with np.load(directory/'queries.npz',allow_pickle=False) as saved:
                if not np.array_equal(saved['calibration'],cal) or not np.array_equal(saved['evaluation'],held):
                    raise ValueError('Resume query arrays differ from frozen split')
            for key,records in complete.items():
                if key[:2]!=(n,distribution):continue
                frozen=json.loads((directory/f'{key[2]:g}-{key[3]}.json').read_text())
                if frozen['dataset_sha256']!=fingerprint:raise ValueError('Resume dataset fingerprint mismatch')
                if frozen['calibration_query_digests']!=list(map(digest_query,cal)):
                    raise ValueError('Resume calibration query mismatch')
                workload={'selectivity':key[2],'path':key[3],'k':10}
                if frozen['workload']!=workload or not any(g['settings']==frozen['settings'] for g in frozen['grid']):
                    raise ValueError('Resume frozen parameters mismatch')
                for i,r in enumerate(records):
                    if r['settings']!=frozen['settings'] or r['workload']!=workload:
                        raise ValueError('Resume frozen parameters differ from recorded evidence')
                    if r['query_digest']!=digest_query(held[i]):
                        raise ValueError('Resume heldout query identity mismatch')
        partial=[key for key,records in complete.items() if len(records)!=evaluation_count]
        if partial:
            original=output/'heldout.jsonl';backup=output/'heldout-interrupted.jsonl'
            if backup.exists():raise ValueError('An interrupted evidence backup already exists')
            backup.write_bytes(original.read_bytes())
            for key in partial:del complete[key]
            original.write_text(''.join(json.dumps(r)+'\n' for records in complete.values() for r in records))
    else:(output/'config.json').write_text(json.dumps(config,indent=2))
    with (output/'heldout.jsonl').open('a' if resume else 'x') as raw:
        for n in sizes:
            for distribution in distributions:
                directory=output/f'{n}-{distribution}'
                keys=[(n,distribution,float(s),path) for s in selectivities for path in ('filter-first','search-first')]
                if all(key in complete for key in keys):
                    for key in keys:
                        frozen=json.loads((directory/f'{key[2]:g}-{key[3]}.json').read_text())
                        summary=summarize(complete[key])
                        summaries.append(dict(size=n,distribution=distribution,workload=frozen['workload'],settings=frozen['settings'],summary=summary,
                            passed=summary['incomplete_queries']==0 and (summary['mean_recall'] is None or summary['mean_recall']>=.95)))
                    continue
                directory.mkdir(exist_ok=resume)
                data=clustered_dataset(n,dimension,seed) if distribution=='clustered' else generate_dataset(n,dimension,seed,distribution)
                if (directory/'dataset.npz').exists():data=load_dataset(directory/'dataset.npz')
                else:save_dataset(data,directory/'dataset.npz')
                cal,held=query_splits(dimension,calibration_count,evaluation_count,seed+n)
                if (directory/'queries.npz').exists():
                    with np.load(directory/'queries.npz',allow_pickle=False) as q:
                        if not np.array_equal(q['calibration'],cal) or not np.array_equal(q['evaluation'],held):
                            raise ValueError('Resume query arrays differ from frozen split')
                else:np.savez(directory/'queries.npz',calibration=cal,evaluation=held)
                build_config=BuildConfig().resolved(n)
                index,stats=build_in_worker(data.ids,data.vectors,build_config,time.monotonic()+60)
                (directory/'build.json').write_text(json.dumps(dict(config=asdict(build_config),nlist=index.nlist,byte_size=index.byte_size,**stats),indent=2))
                prepared_cal=prepare_truths(data,cal,selectivities,10);prepared_eval=prepare_truths(data,held,selectivities,10)
                grid=sorted({min(index.nlist,p) for p in [8,16,32,64,80,96,128,160,192,224,240,index.nlist]})
                for s in selectivities:
                    for path in ('filter-first','search-first'):
                        workload={'selectivity':float(s),'path':path,'k':10}
                        key=(n,distribution,float(s),path)
                        file=directory/f'{s:g}-{path}.json'
                        if key in complete:
                            frozen=json.loads(file.read_text());summary=summarize(complete[key])
                            summaries.append(dict(size=n,distribution=distribution,workload=workload,settings=frozen['settings'],summary=summary,
                                passed=summary['incomplete_queries']==0 and (summary['mean_recall'] is None or summary['mean_recall']>=.95)))
                            continue
                        frozen=json.loads(file.read_text()) if resume and file.exists() else calibrate(data,cal,grid,workload,index=index,prepared=prepared_cal[float(s)])
                        # Freeze to disk before any heldout execution, never alter based on heldout output.
                        if not file.exists():file.write_text(json.dumps(frozen,indent=2))
                        evaluated=evaluate(data,held,frozen,index=index,prepared=prepared_eval[float(s)])
                        for record in evaluated['records']:
                            raw.write(json.dumps(dict(size=n,distribution=distribution,workload=workload,settings=frozen['settings'],**record))+'\n')
                        raw.flush()
                        summaries.append(dict(size=n,distribution=distribution,workload=workload,settings=frozen['settings'],
                                              summary=evaluated['summary'],passed=evaluated['passed']))
                        print(n,distribution,s,path,'recall',evaluated['summary']['mean_recall'],flush=True)
                # Isolate candidate-count vs probe ablation on a disjoint subset of heldout, diagnostic only.
                ablations=[]
                for probe in sorted({min(8,index.nlist),index.nlist}):
                    for count in (64,512,4096):
                        settings={'nprobe':probe,'candidates':min(n,count),'max_candidates':min(n,count),'max_rounds':1}
                        truth=prepare_truths(data,held[:16],[1.0],10)[1.0]
                        records=_measure(data,held[:16],{'selectivity':1.,'path':'filter-first','k':10},settings,index,truth)
                        ablations.append(dict(settings=settings,summary=summarize(records)))
                (directory/'candidate-ablation.json').write_text(json.dumps(ablations,indent=2))
                del index,prepared_cal,prepared_eval,data
    report={'passed':all(r['passed'] for r in summaries),'groups':summaries,'elapsed_seconds':time.monotonic()-began,
            'offline_process_peak_rss_bytes':resource.getrusage(resource.RUSAGE_SELF).ru_maxrss*1024,
            'scope':'CPU strict ANN calibration; exact oracle for empty or smaller-than-K truth; no RPC timings', 'resumed':resume}
    (output/'report.json').write_text(json.dumps(report,indent=2));return report


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output-dir',required=True);parser.add_argument('--sizes',nargs='+',type=int,default=[10000,100000])
    parser.add_argument('--distributions',nargs='+',choices=['independent','correlated','clustered'],default=['independent','correlated','clustered'])
    parser.add_argument('--dimension',type=int,default=128);parser.add_argument('--calibration-queries',type=int,default=200)
    parser.add_argument('--evaluation-queries',type=int,default=500);parser.add_argument('--seed',type=int,default=20261014)
    parser.add_argument('--resume',action='store_true')
    parser.add_argument('--resume-source-ruling',help='Audited explanation for a tested ownership-only source correction')
    args=parser.parse_args()
    report=run(args.output_dir,args.sizes,args.distributions,args.dimension,args.calibration_queries,args.evaluation_queries,args.seed,resume=args.resume,source_ruling=args.resume_source_ruling)
    print(json.dumps({'passed':report['passed'],'groups':len(report['groups'])}));raise SystemExit(0 if report['passed'] else 2)
