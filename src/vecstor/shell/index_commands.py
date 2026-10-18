"""Strict options for explicit index lifecycle commands."""


def _options(args,allowed,flags=()):
    result={};position=0
    while position<len(args):
        key=args[position]
        if key in result or key not in allowed:raise ValueError('Unknown or duplicate index option: '+key)
        if key in flags:result[key]=True;position+=1;continue
        if position+1>=len(args) or args[position+1].startswith('--'):raise ValueError('Missing value for '+key)
        result[key]=args[position+1];position+=2
    return result


def execute_index(client,args):
    if not args:raise ValueError('Use .index build|publish|load|inspect|published|publication-status|prune|list|clear')
    action=args[0];rest=list(args[1:])
    if action=='prune':
        opts=_options(rest,{'--keep-last','--execute','--operation-id','--timeout'},{'--execute'})
        return client.prune_index_artifacts(keep_last=int(opts.get('--keep-last',2)),dry_run=not opts.get('--execute',False),
            operation_id=opts.get('--operation-id'),timeout=float(opts.get('--timeout',45)))
    if action=='publication-status':
        if len(rest)!=1:raise ValueError('Use .index publication-status OPERATION_ID')
        return client.index_publication_status(rest[0])
    if action in ('list','clear','published'):
        table=rest.pop(0) if rest and not rest[0].startswith('--') else ''
        if action=='list':
            if rest:raise ValueError('Use .index list [TABLE]')
            return client.list_indexes(table)
        if action=='clear':
            opts=_options(rest,{'--device'})
            if opts.get('--device','cpu') not in ('cpu','gpu'):raise ValueError('Invalid clear device')
            return client.clear_indexes(table,device=opts.get('--device',''))
        opts=_options(rest,{'--limit','--cursor'})
        return client.list_published_indexes(table,page_size=int(opts.get('--limit',128)),cursor=opts.get('--cursor',''))
    if action not in ('build','publish','load','inspect') or not rest:raise ValueError('Index command requires a table')
    table=rest.pop(0)
    if action=='inspect':
        if rest:raise ValueError('Use .index inspect TABLE')
        return client.inspect_published_index(table)
    if action=='publish':
        opts=_options(rest,{'--nlist','--seed','--timeout','--operation-id','--expected-generation'})
        return client.publish_index(table,nlist=int(opts['--nlist']) if '--nlist' in opts else None,
            seed=int(opts.get('--seed',20261013)),timeout=float(opts.get('--timeout',45)),operation_id=opts.get('--operation-id'),
            expected_generation=int(opts['--expected-generation']) if '--expected-generation' in opts else None)
    opts=_options(rest,{'--device','--gpu-id','--timeout', '--nlist' if action=='build' else '--artifact-id'})
    if opts.get('--device','cpu') not in ('cpu','gpu'):raise ValueError('Invalid index device')
    common=dict(device=opts.get('--device','cpu'),gpu_id=int(opts.get('--gpu-id',0)),timeout=float(opts.get('--timeout',45)))
    if action=='load':return client.load_index(table,expected_artifact_id=opts.get('--artifact-id'),**common)
    return client.build_index(table,nlist=int(opts['--nlist']) if '--nlist' in opts else None,**common)
