"""An explicit auto request requires both capability and completion confirmation."""


def require_adaptive_capabilities(capabilities, options):
    if (not isinstance(capabilities,dict) or type(capabilities.get('adaptive_planning_version')) is not int
        or capabilities['adaptive_planning_version']!=1 or options.device not in capabilities.get('adaptive_devices',[])
        or capabilities.get('adaptive_mode') not in ('apply','shadow')
        or not isinstance(capabilities.get('server_instance_id'),str) or not capabilities['server_instance_id']):
        raise ValueError('Compute service does not support adaptive planning')


def validate_adaptive_plan(execution, options, capabilities):
    if not isinstance(execution,dict):raise ValueError('Missing adaptive execution plan')
    if execution.get('server_instance_id')!=capabilities.get('server_instance_id'):
        raise ValueError('Compute adaptive server instance changed; refresh capabilities')
    adaptive=execution.get('adaptive')
    if (not isinstance(adaptive,dict) or type(adaptive.get('version')) is not int or adaptive['version']!=1
        or adaptive.get('requested_path')!='auto' or execution.get('requested_path')!='auto'
        or adaptive.get('policy_id')!=capabilities.get('adaptive_policy_id')
        or adaptive.get('policy_hash')!=capabilities.get('adaptive_policy_hash')
        or adaptive.get('mode')!=capabilities.get('adaptive_mode')):
        raise ValueError('Compute service did not acknowledge adaptive policy/mode')
    actual=execution.get('actual_mode');path=execution.get('actual_path')
    if actual=='not_executed':
        if path!='not_executed':raise ValueError('Invalid adaptive EXPLAIN plan')
        return
    if actual not in ('exact','approx') or path not in ('filter-first','search-first') or execution.get('actual_device')!='cpu':
        raise ValueError('Invalid adaptive actual path/device')
    if options.mode=='exact' and actual!='exact':raise ValueError('Adaptive request lost exact semantics')
    selected=adaptive.get('executed_plan')
    budgets=adaptive.get('candidate_budgets',{})
    concrete=budgets.get(selected)
    if not isinstance(concrete,dict) or concrete.get('path') not in ('filter-first','search-first'):
        raise ValueError('Missing adaptive selected budget')
    if actual=='approx' and (concrete.get('mode')!='approx' or concrete['path']!=path):
        raise ValueError('Adaptive actual path differs from selected plan')
    if actual=='approx':
        for name in ('nprobe','candidates','max_candidates','max_rounds'):
            explicit=getattr(options,name)
            if explicit is not None and concrete.get(name)!=explicit:
                raise ValueError('Compute adaptive plan changed an explicit budget')
        if options.quality_profile_id and concrete.get('quality_profile_id')!=options.quality_profile_id:
            raise ValueError('Compute adaptive plan changed explicit quality profile')
    if options.mode=='approx' and options.fallback=='error' and (concrete.get('mode')=='exact' or
            (actual=='exact' and execution.get('fallback_reason')!='exact_empty')):
        raise ValueError('Adaptive request ignored error fallback')
    if adaptive['mode']=='shadow' and selected!=adaptive.get('reference_plan'):
        raise ValueError('Adaptive shadow executed proposed plan')
    if adaptive['mode']=='apply' and selected!=adaptive.get('proposed_plan'):
        raise ValueError('Adaptive apply did not execute selected plan')
