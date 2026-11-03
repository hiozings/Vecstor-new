"""Resolve legal candidates using already pinned artifacts and the same lease."""
from dataclasses import replace
import hashlib

import grpc
import numpy as np

from vecstor.cache.types import DataScope
from vecstor.compute.planner_observations import observation_key
from vecstor.compute.planner_types import PlanCandidate, PlanningInputs, fingerprint
from vecstor.index.artifact import ArtifactError
from vecstor.query.quality_policy import QualityUnavailable
from vecstor.rpc.common import RemoteError


def artifact_identity(entry):
    if entry is None or entry.descriptor is None:return {}
    descriptor=entry.descriptor
    return dict(artifact_id=descriptor.artifact_id,payload_sha256=descriptor.payload_sha256,
                build_fingerprint=descriptor.key.build_fingerprint)


def execution_signature(options, read_settings=None):
    return fingerprint(dict(options=options.to_dict(),read_settings=read_settings or {}))


def qualify_candidates(plan, options, lease, pinned_cpu_entry, quality_catalog, policy_match,
                       *, diagnostics=None, read_settings=None):
    diagnostics={} if diagnostics is None else diagnostics
    if options.device!='cpu':raise ValueError('Adaptive planning supports CPU only')
    if options.mode=='approx' and lease.published_index_state=='corrupt':
        raise ArtifactError('DATA_LOSS','published_artifact_corrupt')
    candidates=[];conflicts=[]
    def candidate(identity, effective, description):
        return PlanCandidate(identity,effective.mode,effective.path,effective,description,
                             execution_signature(effective,read_settings))
    if options.mode=='approx' and pinned_cpu_entry is not None:
        mappings=policy_match.profiles or {}
        if options.quality_profile_id:
            description=quality_catalog.describe_profile(options.quality_profile_id) if quality_catalog else None
            mappings={description['path']:options.quality_profile_id} if description else {}
            if not description:diagnostics['quality']='quality_profile_missing'
        for path,name in (('filter-first','ann-filter'),('search-first','ann-search')):
            if path=='filter-first' and options.candidate_data_path=='fused':
                diagnostics[name]='fused_requires_search_first';continue
            profile_id=mappings.get(path)
            if not profile_id or quality_catalog is None:
                diagnostics[name]='quality_profile_missing';continue
            concrete=replace(options,path=path,quality_profile_id=profile_id)
            entry=pinned_cpu_entry
            try:
                effective,description=quality_catalog.resolve(profile_id,entry.key,entry.handle.nlist,
                    entry.handle.row_count,plan,concrete,execution=entry.statistics,
                    artifact=entry.descriptor,index_source='published')
            except QualityUnavailable as error:
                diagnostics[name]=error.reason;continue
            except ValueError as error:
                if options.quality_profile_id:raise
                conflicts.append(str(error));diagnostics[name]='explicit_budget_conflict';continue
            candidates.append(candidate(name,effective,description))
        if conflicts and not candidates:raise ValueError('Adaptive quality budget conflict: '+conflicts[0])
    elif options.mode=='approx':
        diagnostics['index']='published_index_not_loaded'
    if options.mode=='exact' or (options.fallback=='exact' and
                               (options.candidate_data_path!='fused' or pinned_cpu_entry is None)):
        concrete=replace(options,mode='exact',path='filter-first',quality_profile_id=None,candidate_data_path='separate')
        description=(dict(requested_id=options.quality_profile_id,state='not_used_planned_exact')
                     if options.quality_profile_id else None)
        candidates.append(candidate('exact',concrete,description))
    if not candidates:
        reason=diagnostics.get('index') or diagnostics.get('quality') or 'adaptive_no_eligible_quality_path'
        raise RemoteError(grpc.StatusCode.FAILED_PRECONDITION,reason)
    return tuple(candidates)


def collect_planning_inputs(plan, lease, entry, candidates, policy_match, policy, cache_snapshot,
                            observation_store, deployment_context, remaining_ms, *, explain=False,
                            read_settings=None, diagnostics=None):
    scope=DataScope(lease.storage_epoch,lease.database_id,lease.table_id,lease.revision,
                    lease.schema_version,lease.dimension)
    n=lease.row_count if lease.HasField('row_count') else None
    mismatch=policy_match.table_rows is not None and policy_match.table_rows!=n
    matched=None if mismatch else policy_match.matched_rows
    statistics=dict(matched_rows=matched,cardinality_source='offline' if matched is not None else 'unknown',
        nlist=entry.handle.nlist if entry else None,models=() if mismatch else policy_match.models,
        cost_reason='table_row_count_mismatch' if mismatch else policy_match.cost_reason,
        read_settings=read_settings or {},exclusions=diagnostics or {},warm={},risk_unknown={})
    inputs=PlanningInputs(scope,artifact_identity(entry),n,lease.dimension,plan.limit,
        hashlib.sha256(plan.metadata_sql.encode()).hexdigest(),
        hashlib.sha256(np.asarray(plan.query_vector,dtype='<f4').tobytes()).hexdigest(),
        candidates,candidates[0].candidate_id,statistics,cache_snapshot,deployment_context,
        policy.policy_id if policy else None,policy.policy_hash if policy else None,remaining_ms)
    if observation_store is not None:
        lookup=observation_store.peek if explain else observation_store.get
        card=lookup(observation_key(inputs,'cardinality'))
        if card is not None:
            value=card.values.get('matched_rows')
            if card.values.get('conflict') or (value is not None and (n is None or value>n)):
                statistics.update(matched_rows=None,cardinality_source='conflict')
            else:statistics.update(matched_rows=value,cardinality_source='observed')
        for candidate in candidates:
            reuse=lookup(observation_key(inputs,'reuse',candidate))
            generations=('admission_generation','loss_generation')
            warm=bool(reuse and reuse.values.get('warm') and cache_snapshot.get('scope_entry_count',0)>0
                      and all(reuse.values.get(k)==cache_snapshot.get(k) for k in generations))
            statistics['warm'][candidate.candidate_id]=warm
            risk=lookup(observation_key(inputs,'risk',candidate))
            statistics['risk_unknown'][candidate.candidate_id]=bool(risk and risk.values.get('budget_failure'))
            residual=lookup(observation_key(inputs,'cost',candidate))
            if residual:statistics.setdefault('residual_ms',{})[candidate.candidate_id]=residual.values.get('residual_ms',0.)
    return replace(inputs,statistics=statistics)
