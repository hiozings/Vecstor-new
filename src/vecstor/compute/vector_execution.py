"""Snapshot-safe exact/IVF paths, explicit budgets and measured plans."""

import json
import time

import grpc
import numpy as np

from vecstor.compute.lease_io import LeaseIO, consistent
from vecstor.data import query_vector
from vecstor.query.quality_policy import QualityUnavailable
from vecstor.rpc.common import RemoteError


class TopK:
    def __init__(self,k,query,trace):
        self.k,self.query,self.trace=k,query,trace
        self.rows=[];self.distances=np.empty(0,dtype=np.float64)

    def add(self,rows,vectors):
        if not rows:return
        started=time.monotonic()
        delta=vectors.astype(np.float64)-self.query
        distances=np.sum(delta*delta,axis=1,dtype=np.float64)
        entries=[dict(row,vector=vector.tolist()) for row,vector in zip(rows,vectors)]
        merged=self.rows+entries;all_distances=np.concatenate((self.distances,distances))
        selected=np.lexsort((np.asarray([r['id'] for r in merged],dtype=np.int64),all_distances))[:self.k]
        self.rows=[merged[i] for i in selected];self.distances=all_distances[selected]
        self.trace['rerank_ms']+=(time.monotonic()-started)*1000


def execute_vector(stub,registry,request,plan,options,context,deadline,explain=None,quality_catalog=None):
    started=time.monotonic()
    trace={'plan_version':1,'requested_mode':options.mode,'requested_path':options.path,
           'actual_mode':None,'actual_path':None,'metric':'squared_l2','rerank_precision':'float64',
           'requested_k':plan.limit,'options':options.to_dict(),'rounds':[], 'index':None,
           'fallback_reason':None,'exhaustive_buckets':False,'selector_full_coverage':False,
           'selector_build_ms':0.,'native_search_ms':0.,'faiss_search_ms':0.0,'rerank_ms':0.0,
           'metadata_rows_scanned':None,'byte_accounting':'storage_protobuf_payloads'}
    io=LeaseIO(stub,request.database_id,plan.table,context,deadline,trace)
    trace['counts']['selector_builds']=0
    try:
        lease=io.open();query=query_vector(plan.query_vector,lease.dimension).astype(np.float64)
        with registry.acquire(lease) as entry:
            if entry: trace['index']=entry.describe()
            reason='index_version_mismatch' if registry.has_stale(lease) else 'index_missing'
            trace['index_state']='ready' if entry else reason
            quality_problem=None
            if options.quality_profile_id is not None:
                trace['quality_profile']={'requested_id':options.quality_profile_id,'state':'index_unavailable'}
                if entry:
                    try:
                        if quality_catalog is None:raise QualityUnavailable('quality_profile_missing')
                        options,description=quality_catalog.resolve(options.quality_profile_id,entry.key,entry.handle.nlist,
                                                                    entry.handle.row_count,plan,options)
                        trace['quality_profile']=description
                    except QualityUnavailable as error:
                        quality_problem=error.reason
                        trace['quality_profile'].update(state='unavailable',reason=quality_problem)
                trace['effective_options']=options.to_dict()
            if explain=='plan':
                trace.update(actual_mode='not_executed',actual_path='not_executed',
                    planned_path=options.path,planned_mode=options.mode,matched_rows=None)
                if options.mode=='approx' and (not entry or quality_problem):trace['planned_fallback']=(quality_problem or reason) if options.fallback=='exact' else 'error'
                output=None
            else:
                top=TopK(plan.limit,query,trace)
                def exact(fallback=None,known=None):
                    nonlocal top
                    trace.update(actual_mode='exact',actual_path='filter-first',fallback_reason=fallback)
                    top=TopK(plan.limit,query,trace)
                    batches=io.scan(request.sql) if known is None else (known[i:i+io.batch_size] for i in range(0,len(known),io.batch_size))
                    matched=0
                    for rows in batches:
                        io.remaining();top.add(rows,io.fetch([r['id'] for r in rows]));matched+=len(rows)
                    trace['matched_rows']=matched

                def fallback(why,known=None):
                    io.remaining()
                    if options.fallback=='error':
                        code=grpc.StatusCode.FAILED_PRECONDITION if why in {'index_missing','index_version_mismatch','unsafe_float32_range'} or why.startswith('quality_profile_') else grpc.StatusCode.RESOURCE_EXHAUSTED
                        raise RemoteError(code,why)
                    exact(why,known)

                allowed_rows=None;allowed_map=None
                if options.mode=='approx' and options.path=='filter-first':
                    allowed_rows=[row for batch in io.scan(request.sql) for row in batch]
                    allowed_map={row['id']:row for row in allowed_rows}
                    trace['matched_rows']=len(allowed_rows)
                if options.mode=='exact':exact()
                elif allowed_rows is not None and not allowed_rows:exact('exact_empty',allowed_rows)
                elif not entry:fallback(reason,allowed_rows)
                elif quality_problem:fallback(quality_problem,allowed_rows)
                elif max(entry.handle.max_abs,float(np.abs(query).max()))>np.sqrt(.25*float(np.finfo(np.float32).max)/(4*lease.dimension)):
                    fallback('unsafe_float32_range',allowed_rows)
                else:
                    handle=entry.handle
                    consistent(not lease.HasField('row_count') or handle.row_count==lease.row_count,'Index row count mismatch')
                    if allowed_rows is not None and len(allowed_rows)<=256 and options.fallback=='exact':
                        exact('small_filtered_set',allowed_rows)
                    else:
                        trace.update(actual_mode='approx',actual_path=options.path)
                        n=handle.row_count
                        limit_pool=n if allowed_rows is None else len(allowed_rows)
                        budget=min(limit_pool,options.max_candidates or max(4096,8*min(plan.limit,n)))
                        length=min(budget,options.candidates or max(64,4*min(plan.limit,n)))
                        nprobe=options.nprobe if options.nprobe is not None else min(8,handle.nlist)
                        if nprobe>handle.nlist:raise ValueError('Explicit nprobe exceeds nlist')
                        seen=set();matched=0;stop='round_budget'
                        selector=None
                        if allowed_map is not None:
                            selector_start=time.perf_counter()
                            try:selector=handle.prepare_selector(np.ascontiguousarray(list(allowed_map),dtype=np.int64))
                            except ValueError as error:consistent(False,str(error))
                            trace['selector_build_ms']=(time.perf_counter()-selector_start)*1000
                            trace['counts']['selector_builds']=int(selector is not None)
                            trace['selector_full_coverage']=selector is None
                        for round_number in range(1,options.max_rounds+1):
                            trace['exhaustive_buckets'] |= nprobe==handle.nlist
                            io.remaining();ann_start=time.monotonic()
                            _,labels=handle.search(query.astype(np.float32),int(length),int(nprobe),prepared_selector=selector,statistics=trace)
                            trace['faiss_search_ms']+=(time.monotonic()-ann_start)*1000
                            io.remaining()
                            labels=[int(i) for i in labels if i!=-1]
                            consistent(all(i>=0 for i in labels) and len(set(labels))==len(labels),'Invalid ANN candidate IDs')
                            if allowed_map is not None:consistent(set(labels).issubset(allowed_map),'ANN ignored ID selector')
                            new=[i for i in labels if i not in seen][:max(0,budget-len(seen))]
                            seen.update(new);passed=0
                            for start in range(0,len(new),io.batch_size):
                                ids=new[start:start+io.batch_size]
                                rows=io.filter(request.sql,ids) if allowed_map is None else [allowed_map[i] for i in ids]
                                passed+=len(rows)
                                top.add(rows,io.fetch([row['id'] for row in rows]))
                            matched+=passed
                            trace['counts']['candidates_checked']=len(seen)
                            trace['rounds'].append({'round':round_number,'L':length,'nprobe':nprobe,'new_candidates':len(new),
                                'unique_candidates':len(seen),'matched_candidates':matched})
                            if matched>=plan.limit:stop='enough_candidates';break
                            if len(seen)>=budget:stop='candidate_budget';break
                            next_length=min(budget,length*2);next_nprobe=min(handle.nlist,nprobe*2)
                            if next_length==length and next_nprobe==nprobe:stop='no_progress';break
                            length,nprobe=next_length,next_nprobe
                        trace['stop_reason']=stop
                        if matched<plan.limit:fallback(stop,allowed_rows)
                trace['returned_rows']=len(top.rows)
                types={'id':'BIGINT','category':'VARCHAR','timestamp':'BIGINT','value':'DOUBLE','vector':f'FLOAT[{lease.dimension}]','distance':'DOUBLE'}
                output={'columns':[{'name':out,'type':types[source]} for out,source in plan.projection],
                    'rows':[[float(distance) if source=='distance' else row[source] for _,source in plan.projection]
                            for row,distance in zip(top.rows,top.distances)],'revision':lease.revision,'affected_rows':None,
                    'execution_path':f"grpc_{trace['actual_mode']}_{trace['actual_path'].replace('-','_')}"}
    finally:io.close()
    trace['compute_elapsed_ms']=(time.monotonic()-started)*1000
    if explain is not None:
        return {'columns':[{'name':'plan','type':'VARCHAR'}],'rows':[[json.dumps(trace,ensure_ascii=False,allow_nan=False)]],
                'revision':lease.revision,'affected_rows':None,'execution_plan':trace,'execution_path':'explain' if explain=='plan' else 'explain_analyze'}
    output['execution_plan']=trace
    return output
