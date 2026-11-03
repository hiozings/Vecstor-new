"""Dataset/query evidence helpers shared by adaptive calibration and evaluation."""
from dataclasses import replace
import hashlib
import json
from pathlib import Path
import time

import numpy as np

from vecstor.compute.adaptive_execution import effective_read_settings
from vecstor.compute.adaptive_planner import forecast_features
from vecstor.compute.planner_types import DeploymentContext, PlanCandidate, PlanningInputs
from vecstor.compute.planning_inputs import execution_signature
from vecstor.cache.types import CacheConfig, DataScope
from vecstor.query.quality_policy import QualityCatalog
from vecstor.query.search_options import SearchOptions
from vecstor.query.statements import classify_statement
from types import SimpleNamespace

from adaptive_costs import digest_list, query_digest, quality_summary


def write_json(path,value):
    path=Path(path);temporary=path.with_name(path.name+'.tmp')
    temporary.write_text(json.dumps(value,indent=2,allow_nan=False)+'\n')
    temporary.replace(path)


def sql_for(spec, query):
    threshold=int(spec['rows']*spec['selectivity'])
    return (f'SELECT id,vector_distance(vector,{json.dumps(query.tolist())}) AS distance '
            f'FROM "items" WHERE value < {threshold} ORDER BY distance LIMIT {spec["k"]}')


def workload_name(spec):
    return f"w{spec['rows']}_{spec['distribution']}_{str(spec['selectivity']).replace('.','p')}_k{spec['k']}"


def dataset_digest(data):
    h=hashlib.sha256()
    for values in (data.ids,data.timestamps,data.values,data.vectors):h.update(values.tobytes())
    h.update(json.dumps(data.categories.tolist(),ensure_ascii=False).encode())
    return h.hexdigest()


class Oracle:
    def __init__(self,data):self.data=data;self.saved={}

    def truth(self,spec,query):
        threshold=int(spec['rows']*spec['selectivity']);key=(threshold,spec['k'],query_digest(query))
        if key not in self.saved:
            mask=self.data.values<threshold
            ids=self.data.ids[mask];vectors=self.data.vectors[mask].astype(np.float64)
            distances=np.sum((vectors-query.astype(np.float64))**2,axis=1,dtype=np.float64)
            order=np.lexsort((ids,distances))[:spec['k']]
            self.saved[key]=[[int(ids[i]),float(distances[i])] for i in order]
        return self.saved[key]

    def validate(self,spec,query,result,descriptor):
        plan=result['execution_plan'];rows=result['rows'];truth=self.truth(spec,query)
        if result['revision']!=descriptor['key']['revision']:raise AssertionError('Query revision changed')
        if plan['actual_mode']=='approx' and plan.get('artifact_id')!=descriptor['artifact_id']:
            raise AssertionError('Query artifact changed')
        ids=[r[0] for r in rows]
        if len(ids)!=len(set(ids)):raise AssertionError('Duplicate result')
        if len(rows)!=len(truth):raise AssertionError('Incomplete result')
        order=[]
        for identity,distance in rows:
            if not 0<=identity<self.data.size or int(self.data.ids[identity])!=identity:
                raise AssertionError('Unknown result ID')
            if not self.data.values[identity]<int(spec['rows']*spec['selectivity']):
                raise AssertionError('Unfiltered result')
            delta=self.data.vectors[identity].astype(np.float64)-query.astype(np.float64)
            if distance!=float(np.sum(delta*delta,dtype=np.float64)):raise AssertionError('Wrong float64 distance')
            order.append((distance,identity))
        if order!=sorted(order):raise AssertionError('Unordered result')
        if plan['actual_mode']=='exact' and rows!=truth:raise AssertionError('Exact answer differs from NumPy')
        expected={r[0] for r in truth}
        return dict(query_digest=query_digest(query),recall=len(set(ids)&expected)/len(expected) if expected else 1.,
                    complete=True,actual_mode=plan['actual_mode'],truth_rows=truth)


def settings_for(spec,descriptor,path,*,fallback='error',profile_id=None,cache='on'):
    n=descriptor['row_count'];k=spec['k']
    initial=min(n,max(64,4*k))
    if path=='search-first':initial=min(n,max(initial,int(np.ceil(4*k/spec['selectivity']))))
    return SearchOptions(mode='approx',path=path,nprobe=descriptor['nlist'],candidates=initial,
        max_candidates=n,max_rounds=6,fallback=fallback,quality_profile_id=profile_id,
        data_cache=cache,metadata_projection='projected',candidate_data_path='separate')


def make_quality_profile(spec,descriptor,path,calibration,heldout,data_digest):
    cal=quality_summary(calibration,required_recall=.97);held=quality_summary(heldout)
    if not cal['eligible'] or not held['eligible']:return None
    options=settings_for(spec,descriptor,path)
    profile=dict(format_version=3,profile_id=workload_name(spec)+('_ff' if path=='filter-first' else '_sf'),
        index_key=descriptor['key'],execution=dict(device='cpu',precision='float32',faiss_version=descriptor['faiss_version'],gpu_model='not_applicable'),
        artifact_identity={k:descriptor[k] for k in ('artifact_id','payload_sha256','snapshot_sha256')},
        workload=dict(metadata_sql=classify_statement(sql_for(spec,np.zeros(spec['dimension'],dtype='<f4'))).vector.metadata_sql,
                      k=spec['k'],path=path,query_min=0.,query_max=1.),
        budget=dict(nprobe_floor=options.nprobe,initial_candidates=options.candidates,max_candidates=options.max_candidates,max_rounds=6),
        evidence=dict(calibration_queries=len(calibration),evaluation_queries=len(heldout),
            calibration_mean_recall=cal['mean_recall'],evaluation_mean_recall=held['mean_recall'],incomplete_queries=0,exact_queries=0,
            dataset_sha256=data_digest,calibration_queries_sha256=digest_list([r['query_digest'] for r in calibration]),
            evaluation_queries_sha256=digest_list([r['query_digest'] for r in heldout]),
            scope='independent component queries; CPU full nprobe; empirical mean over min(K,M), not per-query guarantee'))
    QualityCatalog.from_document(dict(format_version=3,profiles=[profile]))
    return profile


def round_statistics(records):
    rounds=[]
    for record in records:
        previous=0
        for i,row in enumerate(record['plan'].get('rounds',())):
            while len(rounds)<=i:rounds.append([0.,0.,0.])
            rounds[i][0]+=row['new_candidates'];rounds[i][1]+=row['matched_candidates']-previous;rounds[i][2]+=row['L']
            previous=row['matched_candidates']
    q=[min(1.,p/u) if u else 0. for u,p,l in rounds] or [1.]
    unique=[min(1.,u/l) if l else 0. for u,p,l in rounds] or [1.]
    return dict(q=q,unique_fractions=unique,failure_queries=len({r['query_digest'] for r in records}),
                failure_count=sum(bool(r.get('error')) for r in records),
                metadata_bytes_per_row=16.,control_bytes=512.,selector_bytes_per_row=512.)


def fit_wire_layout(records,dimension,stats):
    # Effective nonnegative framing/metadata terms; no physical bandwidth claim.
    x=[];y=[]
    for row in records:
        counts=row['plan']['counts'];miss=counts['vectors_fetched']
        x.append([counts['rpc_calls'],counts['metadata_rows_returned']])
        y.append(max(0.,counts['rpc_payload_bytes']-4*dimension*miss-8*counts['candidates_checked']-8*miss))
    if len(x)>1:
        x=np.asarray(x,float);y=np.asarray(y,float)
        unconstrained=np.linalg.lstsq(x,y,rcond=None)[0]
        alternatives=[np.zeros(2)]
        if np.all(unconstrained>=0):alternatives.append(unconstrained)
        for i in range(2):
            value=np.zeros(2);denominator=float(x[:,i]@x[:,i])
            value[i]=max(0.,float(x[:,i]@y)/denominator) if denominator else 0.
            alternatives.append(value)
        values=min(alternatives,key=lambda value:float(np.sum((x@value-y)**2)))
        stats=dict(stats,control_bytes=float(values[0]),metadata_bytes_per_row=float(values[1]))
    return stats


def forecast_record(spec,descriptor,options,stats,record,read_settings):
    candidate_id='exact' if options.mode=='exact' else 'ann-filter' if options.path=='filter-first' else 'ann-search'
    candidate=PlanCandidate(candidate_id,options.mode,options.path,options,None if options.mode=='exact' else {},execution_signature(options,read_settings))
    key=descriptor['key'];scope=DataScope('calibration',key['database_id'],key['table_id'],key['revision'],key['schema_version'],key['dimension'])
    snapshot=dict(max_bytes=read_settings['cache_max_bytes'],scope_entry_count=1,active_workspaces=0,workspace_slots=4)
    inputs=PlanningInputs(scope,{},spec['rows'],spec['dimension'],spec['k'],'','',(candidate,),candidate_id,
        dict(matched_rows=int(spec['rows']*spec['selectivity']),nlist=descriptor['nlist'],read_settings=read_settings,
             warm={candidate_id:record['predicted_warm']}),snapshot,DeploymentContext(None,'loopback',1),None,None,60000.)
    features,_,_,_,_=forecast_features(candidate,inputs,{'statistics':stats})
    return dict(query_digest=record['query_digest'],features=features,ms=record['plan']['compute_elapsed_ms'])


def read_configuration(spec,query):
    config=CacheConfig(max_bytes=spec['capacity_mib']*1024**2)
    plan=classify_statement(sql_for(spec,query)).vector
    return effective_read_settings(plan,SimpleNamespace(config=config),True,'projected',False)
