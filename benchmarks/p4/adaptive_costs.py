"""Reproducible splits and nonnegative empirical cost fitting, without ML dependencies."""
import hashlib
import json
import math
from pathlib import Path

import numpy as np

from vecstor.query.adaptive_policy import FEATURES

# Stable preference order; only nonnegative-cone redundancy may remove a feature.
FEATURE_PRIORITY=('intercept','payload_bytes','rpc_calls','candidate_rows','scan_rows','metadata_rows',
                  'ann_work','ann_candidates','selector_rows','rerank_values','cache_lookup_rows',
                  'cache_copy_bytes','cache_admit_bytes')


def query_digest(query):
    value=np.asarray(query,dtype='<f4')
    if value.ndim!=1 or not np.isfinite(value).all():raise ValueError('Invalid query')
    return hashlib.sha256(value.tobytes()).hexdigest()


def digest_list(values):
    return hashlib.sha256(json.dumps(list(values),separators=(',',':')).encode()).hexdigest()


def make_query_splits(dimension, seed, smoke=False, excluded_digests=()):
    if type(dimension) is not int or dimension<1 or type(seed) is not int or seed<0:
        raise ValueError('Invalid query split parameters')
    rng=np.random.default_rng(seed)
    sizes=dict(calibration=500,quality=500,fit=128,evaluation=500,pilot=10)
    if smoke:sizes=dict(calibration=2,quality=2,fit=4,evaluation=2,pilot=2)
    pools={name:rng.random((n,dimension),dtype=np.float32) for name,n in sizes.items()}
    hashes=[query_digest(q) for values in pools.values() for q in values]
    if len(hashes)!=len(set(hashes)):raise ValueError('Query splits overlap')
    if set(hashes).intersection(excluded_digests):raise ValueError('Queries overlap previous evidence')
    return pools


def validate_resume(existing, expected):
    if existing!=expected:raise ValueError('Resume manifest differs from frozen configuration/source')


def latency_summary(values, elapsed_seconds):
    if not values or any(not math.isfinite(x) or x<0 for x in values) or elapsed_seconds<=0:
        raise ValueError('Invalid latency samples')
    return dict(requests=len(values),p50_ms=float(np.median(values)),
        p95_ms=float(np.percentile(values,95)) if len(values)>=20 else None,
        min_ms=float(min(values)),max_ms=float(max(values)),throughput=len(values)/elapsed_seconds)


def quality_summary(records, minimum=500, required_recall=.95):
    n=len(records);unique=len({r['query_digest'] for r in records})
    exact=sum(r.get('actual_mode')=='exact' for r in records)
    incomplete=sum(not r.get('complete',False) for r in records)
    errors=sum(bool(r.get('error')) or r.get('actual_mode') not in ('exact','approx') for r in records)
    mean=float(np.mean([r.get('recall',0.) for r in records])) if n else 0.
    return dict(queries=n,unique_queries=unique,mean_recall=mean,exact_queries=exact,
        incomplete_queries=incomplete,errors=errors,
        eligible=bool(n>=minimum and n==unique and not exact and not incomplete and not errors and mean>=required_recall))


def _matrix(records):
    if not records:raise ValueError('No cost observations')
    x=[];y=[];queries=[]
    for row in records:
        features=row['features'];ms=row['ms'];query=row['query_digest']
        if row.get('error') or not isinstance(query,str) or len(query)!=64:
            raise ValueError('Invalid/failed timing observation')
        if not set(features).issubset(FEATURES):raise ValueError('Unknown cost feature')
        values=[features.get(key,0.) for key in FEATURE_PRIORITY]
        if any(type(v) not in (int,float) or not math.isfinite(v) or v<0 for v in [ms,*values]):
            raise ValueError('Invalid cost observation value')
        x.append(values);y.append(ms);queries.append(query)
    return np.asarray(x,dtype=np.float64),np.asarray(y,dtype=np.float64),queries


def _nonnegative_least_squares(a,target):
    """Bounded active-set solve; direct subproblems avoid slow gradient convergence."""
    size=a.shape[1];weights=np.zeros(size);active=np.zeros(size,dtype=bool)
    tolerance=10*np.finfo(float).eps*max(a.shape)*max(float(np.linalg.norm(a.T@target,np.inf)),1e-12)
    limit=max(100,20*size*size);iterations=0
    while iterations<limit:
        gradient=a.T@(target-a@weights)
        candidates=np.where(~active,gradient,-np.inf)
        if not np.any(candidates>tolerance):return weights,True,iterations
        active[int(np.argmax(candidates))]=True
        while iterations<limit:
            iterations+=1;trial=np.zeros(size)
            trial[active]=np.linalg.lstsq(a[:,active],target,rcond=None)[0]
            if np.all(trial[active]>0):
                weights=trial;break
            bad=active&(trial<=0);denominator=weights[bad]-trial[bad]
            ratios=np.divide(weights[bad],denominator,out=np.zeros_like(denominator),where=denominator>0)
            alpha=float(np.min(ratios))
            weights+=alpha*(trial-weights)
            drop=active&(weights<=np.finfo(float).eps*max(1.,float(np.max(weights))))
            active[drop]=False;weights[drop]=0.
    return weights,False,iterations


def fit_cost_model(training, validation):
    x,y,train_queries=_matrix(training);v,truth,val_queries=_matrix(validation)
    if set(train_queries)&set(val_queries):raise ValueError('Cost query splits overlap')
    scale=np.maximum(np.linalg.norm(x,axis=0),1.)
    normalized=x/scale
    kept=[];dropped=[]
    for column,name in enumerate(FEATURE_PRIORITY):
        target_column=normalized[:,column]
        redundant=np.linalg.norm(target_column)<=1e-8
        if kept and not redundant:
            combination,converged,_=_nonnegative_least_squares(normalized[:,kept],target_column)
            redundant=converged and np.linalg.norm(normalized[:,kept]@combination-target_column)<=1e-8
        # Signed dependence is not redundancy under nonnegative coefficients.
        if redundant:dropped.append(name)
        else:kept.append(column)
    rank=int(np.linalg.matrix_rank(normalized[:,kept],tol=1e-8)) if kept else 0
    if not kept or len(y)<rank:raise ValueError('Unidentifiable cost model')
    # Both objectives train on the same fit queries. Internal validation selects
    # between these two predefined candidates; the final holdout is never read.
    candidates=[]
    for name,weight in (('absolute',np.ones_like(y)),('relative',1./np.maximum(y,1.))):
        a=normalized[:,kept]*weight[:,None]
        weights,converged,iterations=_nonnegative_least_squares(a,y*weight)
        prediction=v[:,kept]@(weights/scale[kept])
        relative=np.abs(prediction-truth)/np.maximum(truth,1.)
        median=float(np.median(relative));p90=float(np.percentile(relative,90))
        score=max(median/.2,p90/.4) if converged else float('inf')
        candidates.append(dict(name=name,weights=weights,converged=converged,iterations=iterations,
                               median=median,p90=p90,score=score))
    chosen=min(candidates,key=lambda c:(c['score'],c['median'],c['name']))
    weights=chosen['weights'];converged=chosen['converged'];iterations=chosen['iterations']
    values=weights/scale[kept]
    predicted=v[:,kept]@values
    errors=np.abs(predicted-truth);relative=errors/np.maximum(truth,1.)
    median=float(np.median(relative));p90=float(np.percentile(relative,90))
    coefficients={FEATURE_PRIORITY[i]:float(coefficient) for i,coefficient in zip(kept,values)}
    return dict(accepted=bool(converged and median<=.2 and p90<=.4),coefficients=coefficients,
        residual_ms=float(np.percentile(errors,90)),evidence=dict(fit_queries=len(set(train_queries)),
            validation_queries=len(set(val_queries)),median_relative_error=median,p90_relative_error=p90,
            fit_queries_sha256=digest_list(sorted(set(train_queries))),
            validation_queries_sha256=digest_list(sorted(set(val_queries)))),
        diagnostics=dict(method='scaled active-set nonnegative least squares',converged=converged,
            weighting=chosen['name'],all_training_observations_retained=True,
            model_selection='two predefined objectives; internal validation only',
            alternatives=[{k:c[k] for k in ('name','converged','iterations','median','p90')} for c in candidates],
            iterations=iterations,rank=rank,individual_coefficients_identifiable=rank==len(kept),
            retained=[FEATURE_PRIORITY[i] for i in kept],dropped=dropped,
            training_samples=len(training),validation_samples=len(validation),
            validation_ms=truth.tolist(),prediction_ms=predicted.tolist()))


def source_manifest(root):
    root=Path(root)
    paths=[*root.joinpath('src').rglob('*.py'),*root.joinpath('benchmarks/p4').glob('*.py'),
           root/'tests/support/delayed_storage.py']
    return {str(p.relative_to(root)):hashlib.sha256(p.read_bytes()).hexdigest() for p in sorted(paths)}


def assess_performance(runs, case_ids, *, quality_certified):
    """Publication gates require complete comparable evidence, never just fast results."""
    guards=[]
    for case_id in case_ids:
        for phase in ('cold','warm'):
            groups={}
            for run in runs:
                if run['case']==case_id:
                    found=next((p for p in run['phases'] if p['name']==phase),None)
                    if found is not None:groups.setdefault(run['variant'],[]).append(found)
            fixed={name:rows for name,rows in groups.items() if name in ('filter-first','search-first')}
            auto=groups.get('auto',[])
            populations=[auto,*fixed.values()]
            enough=bool(fixed and all(len(rows)>=3 and all(p['requests']>=200 and p['p95_ms'] is not None for p in rows)
                                     for rows in populations))
            row=dict(case=case_id,phase=phase,status='insufficient_evidence',passed=False,
                     selection_rate=None,model_coverage=None)
            if not enough:guards.append(row);continue
            med50={name:float(np.median([p['p50_ms'] for p in rows])) for name,rows in fixed.items()}
            best=min(med50,key=med50.get);best50=med50[best]
            best95=min(float(np.median([p['p95_ms'] for p in rows])) for rows in fixed.values())
            auto50=float(np.median([p['p50_ms'] for p in auto]));auto95=float(np.median([p['p95_ms'] for p in auto]))
            requests=sum(p['requests'] for p in auto)
            coverage=min(sum(p.get('model_coverage',{}).get('ann-filter' if name=='filter-first' else 'ann-search',0)
                             for p in auto)/requests for name in fixed)
            gap=max(med50.values())/best50-1 if best50>0 else 0.
            selection=sum(p['decisions'].get(best,0) for p in auto)/requests
            overhead=max(p['planner_feedback_p95_ms'] for p in auto)
            row.update(status='evaluated' if coverage>=.95 else 'insufficient_model_coverage',best_fixed=best,
                p50_ratio=auto50/best50,p95_ratio=auto95/best95,model_coverage=coverage,
                fixed_gap_fraction=gap,selection_rate=selection,selection_gate_required=len(fixed)>1 and gap>=.2,
                planner_feedback_p95_ms=overhead)
            row['passed']=bool(coverage>=.95 and row['p50_ratio']<=1.1 and row['p95_ratio']<=1.2 and
                overhead<=min(.5,auto50*.02) and (not row['selection_gate_required'] or selection>=.9))
            guards.append(row)
    return guards,bool(quality_certified and len(guards)==len(case_ids)*2 and all(g['passed'] for g in guards))
