"""Pure bounded forecasts. Estimates never establish query completeness."""
from dataclasses import replace
import math

import grpc

from vecstor.compute.planner_types import CandidateEstimate, PlannerDecision
from vecstor.query.adaptive_policy import DEFAULT_PARAMETERS, FEATURES
from vecstor.rpc.common import RemoteError


def failure_upper(attempts, failures):
    """Wilson upper bound for distinct calibration queries, not repetitions."""
    if attempts < 500:return None
    z=1.959963984540054;p=failures/attempts
    return (p+z*z/(2*attempts)+z*math.sqrt(p*(1-p)/attempts+z*z/(4*attempts**2)))/(1+z*z/attempts)


def forecast_features(candidate, inputs, model, *, common_calls=2, use_cache_evidence=True, use_candidate_correlation=True):
    """Keep request counts conservative when the within-batch miss layout is unknown."""
    options=candidate.options;stats=inputs.statistics;read=stats.get('read_settings',{})
    m=stats['matched_rows'];n=inputs.table_rows;d=inputs.dimension
    measured=model['statistics'];snapshot=inputs.cache_snapshot
    cache=bool(read.get('cache_enabled') and snapshot.get('max_bytes',0)>0 and not snapshot.get('closed',False)
               and snapshot.get('active_workspaces',0)<snapshot.get('workspace_slots',0))
    hit=float(use_cache_evidence and cache and snapshot.get('scope_entry_count',0)>0 and stats.get('warm',{}).get(candidate.candidate_id,False))
    batch=min(1024,(1024**2)//(4*d));rounds=[];unique=passed=ann_work=ann_candidates=0.
    filters=vector_calls=0;vectors=0.;scan_calls=0
    predicted_exact=(candidate.mode=='exact' or (candidate.path=='filter-first' and
                     (m==0 or (m<=256 and options.fallback=='exact'))))
    fused=bool(candidate.path=='search-first' and read.get('fused_candidates'))
    exhausted=False
    if predicted_exact:
        scan_calls=1;vectors=float(m)
        vector_calls=0 if hit else math.ceil(m/batch)
    else:
        nlist=stats['nlist'];pool=m if candidate.path=='filter-first' else n
        budget=min(pool,options.max_candidates or max(4096,8*min(inputs.k,n)))
        length=min(budget,options.candidates or max(64,4*min(inputs.k,n)))
        probe=options.nprobe if options.nprobe is not None else min(8,nlist)
        if candidate.path=='filter-first':scan_calls=1
        for number in range(options.max_rounds):
            q=1. if candidate.path=='filter-first' else measured['q'][min(number,len(measured['q'])-1)]
            if candidate.path=='search-first' and not use_candidate_correlation:q=m/n if n else 0.
            fraction=measured['unique_fractions'][min(number,len(measured['unique_fractions'])-1)]
            added=min(pool-unique,budget-unique,length*fraction)
            found=min(m-passed,added*q)
            if unique+added>=pool:found=m-passed  # a forecast only; executor must prove coverage
            unique+=added;passed+=found;vectors+=found
            count=math.ceil(added/batch)
            if candidate.path=='search-first':filters+=count
            if not hit and not fused:
                # A single miss in each subbatch still causes a separate vector RPC.
                vector_calls+=min(count,math.ceil(found))
            ann_work+=n*probe/nlist;ann_candidates+=length
            rounds.append(dict(L=length,nprobe=probe,new_candidates=added,matched_candidates=passed))
            if passed>=inputs.k or unique>=pool:break
            next_length=min(budget,length*2);next_probe=min(nlist,probe*2)
            if unique>=budget or (next_length==length and next_probe==probe):break
            length,probe=next_length,next_probe
        exhausted=passed<inputs.k and unique<pool
    misses=vectors*(1-hit)
    rpc=common_calls+scan_calls+filters+vector_calls
    returned_metadata=float(m) if scan_calls else vectors
    payload=(rpc*measured['control_bytes']+returned_metadata*measured['metadata_bytes_per_row']+
             misses*4*d+8*unique+8*misses)
    lookup=(unique if fused else vectors) if cache else 0.
    admission=cache and (not predicted_exact or read.get('scan_admission',False))
    features=dict.fromkeys(FEATURES,0.)
    features.update(intercept=1.,scan_rows=float(n) if scan_calls else 0.,metadata_rows=returned_metadata,
        candidate_rows=unique,ann_work=ann_work,ann_candidates=ann_candidates,
        selector_rows=float(m) if scan_calls and not predicted_exact else 0.,rerank_values=vectors*d,
        cache_lookup_rows=lookup,cache_copy_bytes=4*d*vectors*(1+hit) if cache else 0.,
        cache_admit_bytes=4*d*misses if admission else 0.,rpc_calls=rpc,payload_bytes=payload)
    workspace=m*measured['selector_bytes_per_row'] if candidate.path=='filter-first' and not predicted_exact else 0.
    details=dict(predicted_mode='exact' if predicted_exact else 'approx',rounds=rounds,
        unique_candidates=unique,filter_rpc_calls=filters,vector_rpc_calls=vector_calls,
        cache_hit_estimate=hit,forecast_exhausted=exhausted,features=features)
    return features,details,workspace,vectors,misses


def estimate_candidate(candidate, inputs, policy, *, _common_calls=2, _memo=None):
    memo={} if _memo is None else _memo
    key=(candidate.candidate_id,candidate.execution_fingerprint,_common_calls)
    if key not in memo:
        memo[key]=_estimate_candidate(candidate,inputs,policy,_common_calls=_common_calls,_memo=memo)
    return memo[key]


def _estimate_candidate(candidate, inputs, policy, *, _common_calls=2, _memo=None):
    model=next((m for m in inputs.statistics.get('models',()) if
        m['candidate_id']==candidate.candidate_id and m['execution_fingerprint']==candidate.execution_fingerprint),None)
    def unknown(reason='insufficient_cost_evidence',**values):
        return CandidateEstimate(candidate.candidate_id,reason=reason,**values)
    if model is None or inputs.table_rows is None or inputs.statistics.get('matched_rows') is None:
        return unknown()
    if candidate.mode=='approx' and not inputs.statistics.get('nlist'):return unknown()
    evidence=model['evidence']
    parameters=getattr(policy,'parameters',DEFAULT_PARAMETERS)
    features,details,workspace,vectors,misses=forecast_features(candidate,inputs,model,common_calls=_common_calls,
        use_cache_evidence=parameters['use_cache_evidence'],use_candidate_correlation=parameters['use_candidate_correlation'])
    if workspace>parameters['filter_workspace_bytes']:
        return unknown(eligible=False,reason='filter_working_set_budget',workspace_bytes=workspace,components=details)
    measured=model['statistics'];attempts=measured['failure_queries'];failures=measured['failure_count']
    risk=failure_upper(attempts,failures)
    if inputs.statistics.get('risk_unknown',{}).get(candidate.candidate_id) or details['forecast_exhausted']:
        risk=None
    if details['predicted_mode']=='exact':risk=0.
    if (not model.get('cost_usable',True) or evidence['median_relative_error']>.2
            or evidence['p90_relative_error']>.4):
        return unknown(reason='cost_validation_failed',failure_upper=risk,
                       workspace_bytes=workspace,components=details)
    mean=sum(features[key]*value for key,value in model['coefficients'].items()
             if parameters['include_transfer_cost'] or key not in ('rpc_calls','payload_bytes'))
    rpc=features['rpc_calls'];payload=features['payload_bytes']
    details['base_rpc_calls']=rpc
    residual=max(model['residual_ms'],inputs.statistics.get('residual_ms',{}).get(candidate.candidate_id,0.))
    upper=mean+residual;lower=mean-residual
    if details['predicted_mode']=='exact':risk=0.
    elif candidate.options.fallback=='exact':
        exact=next((c for c in inputs.candidates if c.mode=='exact'),None)
        # Unknown fallback costs cannot be treated as free, even with no observed failures.
        if exact is None:
            return unknown(reason='fallback_cost_unknown',components=details)
        remaining=estimate_candidate(exact,inputs,policy,_common_calls=0,_memo=_memo)
        if remaining.mean_ms is None:return unknown(reason='fallback_cost_unknown',components=details)
        probability=1. if details['forecast_exhausted'] or not attempts else failures/attempts
        mean+=probability*remaining.mean_ms
        upper+=(risk if risk is not None else 1.)*remaining.upper_ms
        lower+=probability*remaining.lower_ms
        rpc+=probability*remaining.rpc_calls;payload+=probability*remaining.payload_bytes
        vectors+=probability*remaining.vectors_reranked;misses+=probability*remaining.vectors_missed
        details.update(fallback_probability=probability,fallback_remaining_ms=remaining.mean_ms,
                       fallback_rpc_calls=remaining.rpc_calls)
    bias=inputs.statistics.get('bias_ms',{}).get(candidate.candidate_id,0.)
    details['base_mean_ms']=mean
    mean+=bias;lower+=bias;upper+=bias
    if not all(math.isfinite(v) for v in (mean,lower,upper)):
        return unknown(reason='nonfinite_cost')
    mean=max(0.,mean);lower=max(0.,lower);upper=max(0.,upper)
    details['observed_bias_ms']=bias
    upper=max(upper,mean);lower=min(lower,mean)
    return CandidateEstimate(candidate.candidate_id,mean_ms=mean,lower_ms=lower,upper_ms=upper,
        rpc_calls=rpc,payload_bytes=payload,vectors_reranked=vectors,
        vectors_missed=misses,workspace_bytes=workspace,failure_upper=risk,components=details)


def choose_plan(inputs, policy, mode='apply'):
    if mode not in ('apply','shadow'):raise ValueError('Invalid adaptive mode')
    if inputs.remaining_ms<=0:raise RemoteError(grpc.StatusCode.DEADLINE_EXCEEDED,'Query deadline exceeded')
    memo={}
    estimates=tuple(estimate_candidate(c,inputs,policy,_memo=memo) for c in inputs.candidates)
    by_id={e.candidate_id:e for e in estimates}
    eligible=[c for c in inputs.candidates if by_id[c.candidate_id].eligible]
    if not eligible:raise RemoteError(grpc.StatusCode.RESOURCE_EXHAUSTED,'adaptive_working_set_budget')
    parameters=getattr(policy,'parameters',DEFAULT_PARAMETERS)
    reference=next((c for c in eligible if c.candidate_id==inputs.reference_id),eligible[0])
    ranked=eligible;reason=None
    if all(c.mode=='approx' and c.options.fallback=='error' for c in eligible):
        ranked=[c for c in eligible if by_id[c.candidate_id].failure_upper is not None and
                by_id[c.candidate_id].failure_upper<=parameters['risk_limit']]
        if not ranked:
            ranked=[reference];reason='risk_unknown_or_high'
    anchor=reference if reference in ranked else ranked[0]
    selected=anchor;baseline=by_id[anchor.candidate_id]
    if reason is None:
        reason='exact_only' if len(eligible)==1 and anchor.mode=='exact' else 'insufficient_cost_evidence'
        if baseline.lower_ms is not None:
            margin=max(parameters['switch_min_ms'],parameters['switch_margin']*baseline.lower_ms)
            better=[c for c in ranked if c!=anchor and by_id[c.candidate_id].upper_ms is not None
                    and by_id[c.candidate_id].upper_ms+margin<baseline.lower_ms]
            if better:
                selected=min(better,key=lambda c:(by_id[c.candidate_id].upper_ms,c.candidate_id))
                reason='lower_predicted_cost'
            elif len(eligible)>1:reason='uncertain_or_tie'
        if anchor!=reference and selected==anchor:reason='lower_failure_risk'
    return PlannerDecision(reference.candidate_id,selected.candidate_id,
        reference.candidate_id if mode=='shadow' else selected.candidate_id,reason,estimates,
        inputs.policy_id,inputs.policy_hash,mode)
