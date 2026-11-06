"""Small adapter between pure planning and the existing lease-bound executor."""
import time

from vecstor.cache.types import DataScope
from vecstor.compute.adaptive_planner import choose_plan
from vecstor.compute.planner_observations import Observation, make_reuse_observation, observation_key
from vecstor.compute.planner_types import ResolvedPlan, plain
from vecstor.compute.planning_inputs import artifact_identity, collect_planning_inputs, qualify_candidates
from vecstor.query.adaptive_policy import PolicyMatch

BUDGET_FAILURES=frozenset(('candidate_budget','round_budget','no_progress'))


def effective_read_settings(plan, cache, enabled, projection, fused):
    config=cache.config if cache is not None else None
    return dict(cache_enabled=bool(enabled),metadata_projection=projection,fused_candidates=bool(fused),
        scan_admission=config.scan_admission if config else False,
        cache_max_bytes=config.max_bytes if config else 0,cache_max_entries=config.max_entries if config else 0,
        workspace_slots=config.workspace_slots if config else 0,workspace_bytes=config.workspace_bytes if config else 0,
        projection_fields=tuple(plan.projection))


def prepare_adaptive(plan, options, lease, entry, catalog, policy, observations, cache,
                     read_settings, context, remaining_ms, *, explain=False, mode='apply'):
    scope=DataScope(lease.storage_epoch,lease.database_id,lease.table_id,lease.revision,
                    lease.schema_version,lease.dimension)
    match=policy.lookup(scope,artifact_identity(entry),plan.metadata_sql,plan.limit,context) if policy else PolicyMatch(profiles={})
    diagnostics={}
    candidates=qualify_candidates(plan,options,lease,entry,catalog,match,
                                   diagnostics=diagnostics,read_settings=read_settings)
    snapshot=cache.scope_snapshot(scope) if cache else {}
    inputs=collect_planning_inputs(plan,lease,entry,candidates,match,policy,snapshot,observations,context,
                                  remaining_ms,explain=explain,read_settings=read_settings,diagnostics=diagnostics)
    decision=choose_plan(inputs,policy,mode)
    selected=next(c for c in candidates if c.candidate_id==decision.executed_id)
    description=dict(version=1,requested_path='auto',policy_id=inputs.policy_id,policy_hash=inputs.policy_hash,
        mode=mode,parameters=dict(policy.parameters) if policy else {},deployment_context=plain(context),
        concurrency_bucket=context.concurrency_bucket,decision_reason=decision.reason,reference_plan=decision.reference_id,
        proposed_plan=decision.proposed_id,executed_plan=decision.executed_id,
        estimates=plain(decision.estimates),scope=plain(scope),artifact_identity=plain(inputs.artifact_identity),
        matched_rows=inputs.statistics['matched_rows'],cardinality_source=inputs.statistics['cardinality_source'],
        selectivity=(inputs.statistics['matched_rows']/inputs.table_rows if inputs.table_rows and inputs.statistics['matched_rows'] is not None else None),
        cache_snapshot=plain(snapshot),cost_reason=match.cost_reason,exclusions=diagnostics,
        candidate_budgets={c.candidate_id:c.options.to_dict() for c in candidates},
        planning_extra_storage_rpc_calls=0,feedback_ms=0.,planned_exact_choice=selected.mode=='exact')
    return inputs,ResolvedPlan(selected,decision),description


def record_budget_failure(inputs, resolved, observations, reason):
    if inputs is not None and resolved is not None and observations is not None and reason in BUDGET_FAILURES:
        observations.note_budget_failure(observation_key(inputs,'risk',resolved.candidate))


def finish_adaptive(inputs, resolved, trace, cache, observations, elapsed_ms, check):
    started=time.monotonic();check();pending=[]
    if observations is not None:
        matched=trace.get('matched_rows')
        if trace.get('actual_path')=='filter-first' and type(matched) is int:
            pending.append(Observation(observation_key(inputs,'cardinality'),{'matched_rows':matched}))
        after=cache.scope_snapshot(inputs.scope) if cache else {}
        reuse=make_reuse_observation(inputs,trace,inputs.cache_snapshot,after)
        if reuse is not None:pending.append(reuse)
        if trace.get('fallback_reason') in BUDGET_FAILURES:
            pending.append(Observation(observation_key(inputs,'risk',resolved.candidate),{'budget_failure':True}))
        estimate=next(e for e in resolved.decision.estimates if e.candidate_id==resolved.candidate.candidate_id)
        if estimate.mean_ms is not None:
            key=observation_key(inputs,'cost',resolved.candidate)
            base=(estimate.components or {}).get('base_mean_ms',estimate.mean_ms)
            target_bias=max(0.,elapsed_ms-trace['adaptive']['planner_ms'])-base
            pending.append(Observation(key,{'bias_sample_ms':target_bias}))
        observations.record_many(pending,check=check)
        trace['adaptive']['observation_entries']=observations.stats()['entries']
    trace['adaptive']['feedback_ms']=(time.monotonic()-started)*1000
