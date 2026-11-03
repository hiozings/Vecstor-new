"""Reproducible splits and nonnegative empirical cost fitting, without ML dependencies."""
import hashlib
import json
import math
from pathlib import Path

import numpy as np

from vecstor.query.adaptive_policy import FEATURES

# Prioritize identifiable transfer effects over collinear copy costs with opposite signs.
FEATURE_PRIORITY=('intercept','payload_bytes','rpc_calls','candidate_rows','scan_rows','metadata_rows',
                  'ann_work','ann_candidates','selector_rows','rerank_values','cache_lookup_rows',
                  'cache_copy_bytes','cache_admit_bytes')


def query_digest(query):
    value=np.asarray(query,dtype='<f4')
    if value.ndim!=1 or not np.isfinite(value).all():raise ValueError('Invalid query')
    return hashlib.sha256(value.tobytes()).hexdigest()


def digest_list(values):
    return hashlib.sha256(json.dumps(list(values),separators=(',',':')).encode()).hexdigest()


def make_query_splits(dimension, seed, smoke=False):
    if type(dimension) is not int or dimension<1 or type(seed) is not int or seed<0:
        raise ValueError('Invalid query split parameters')
    rng=np.random.default_rng(seed)
    sizes=dict(calibration=500,quality=500,fit=128,evaluation=500,pilot=10)
    if smoke:sizes=dict(calibration=2,quality=2,fit=4,evaluation=2,pilot=2)
    pools={name:rng.random((n,dimension),dtype=np.float32) for name,n in sizes.items()}
    hashes=[query_digest(q) for values in pools.values() for q in values]
    if len(hashes)!=len(set(hashes)):raise ValueError('Query splits overlap')
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


def fit_cost_model(training, validation):
    x,y,train_queries=_matrix(training);v,truth,val_queries=_matrix(validation)
    if set(train_queries)&set(val_queries):raise ValueError('Cost query splits overlap')
    scale=np.maximum(np.linalg.norm(x,axis=0),1.)
    normalized=x/scale
    kept=[];dropped=[]
    for column,name in enumerate(FEATURE_PRIORITY):
        trial=kept+[column]
        if np.linalg.matrix_rank(normalized[:,trial],tol=1e-8)>len(kept):kept.append(column)
        else:dropped.append(name)
    if not kept or len(y)<len(kept):raise ValueError('Unidentifiable cost model')
    a=normalized[:,kept]
    step=1./max(float(np.linalg.norm(a,ord=2)**2),1e-12)
    weights=np.zeros(len(kept));converged=False
    for iteration in range(20000):
        updated=np.maximum(0.,weights-step*(a.T@(a@weights-y)))
        if np.max(np.abs(updated-weights))<=1e-9*max(1.,float(np.max(np.abs(weights)))):
            weights=updated;converged=True;break
        weights=updated
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
        diagnostics=dict(method='scaled projected-gradient nonnegative least squares',converged=converged,
            iterations=iteration+1,rank=len(kept),retained=[FEATURE_PRIORITY[i] for i in kept],dropped=dropped,
            training_samples=len(training),validation_samples=len(validation),
            validation_ms=truth.tolist(),prediction_ms=predicted.tolist()))


def source_manifest(root):
    root=Path(root)
    paths=[*root.joinpath('src').rglob('*.py'),*root.joinpath('benchmarks/p4').glob('*.py'),
           root/'tests/support/delayed_storage.py']
    return {str(p.relative_to(root)):hashlib.sha256(p.read_bytes()).hexdigest() for p in sorted(paths)}
