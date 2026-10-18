"""Explicit startup preload; configuration reading stays stdlib-only for Landlock."""
import json
from pathlib import Path
import re
import time


def read_preload(path, storage_root=None):
    path = Path(path).resolve(strict=True)
    if storage_root is not None and path.is_relative_to(Path(storage_root).resolve()):
        raise ValueError('Preload configuration must be outside storage data root')
    with path.open('rb') as source: payload = source.read(1024**2+1)
    if len(payload)>1024**2: raise ValueError('Preload configuration exceeds 1 MiB')
    value=json.loads(payload)
    if not isinstance(value,dict) or set(value)!={'format_version','targets'} or type(value['format_version']) is not int or value['format_version']!=1:
        raise ValueError('Invalid preload document')
    targets=value['targets']
    if not isinstance(targets,list) or len(targets)>128:raise ValueError('Preload exceeds 128 targets')
    seen=set();normalized=[]
    for target in targets:
        if not isinstance(target,dict) or set(target)!={'database','table','device'} or target['device'] not in ('cpu','gpu'):
            raise ValueError('Invalid preload target')
        for name in ('database','table'):
            value=target[name]
            if not isinstance(value,str) or not re.fullmatch(r'[A-Za-z][A-Za-z0-9_]*',value):
                raise ValueError('Invalid preload identifier')
        key=(target['database'].lower(),target['table'].lower())
        if key in seen:raise ValueError('Duplicate preload target')
        seen.add(key)
        normalized.append(dict(database=key[0],table=key[1],device=target['device']))
    return normalized


def preload_indexes(manager, targets, deadline):
    from vecstor.rpc import sql_pb2 as pb
    from vecstor.rpc.common import DeadlineContext
    results=[]
    for target in targets:
        remaining=min(60.,deadline-time.monotonic())
        if remaining<=0:raise TimeoutError('Index preload startup deadline exceeded')
        context=DeadlineContext(time.monotonic()+remaining)
        database=json.loads(manager.stub.Control(pb.ControlRequest(action='open_database',name=target['database']),timeout=context.time_remaining()).json_result)
        loaded=manager.load(pb.LoadIndexRequest(database_id=database['id'],table=target['table'],device=target['device']),context)
        results.append(dict(database_id=database['id'],table=target['table'],device=target['device'],
            artifact_id=loaded['descriptor'].artifact_id,generation=loaded['publication_generation'],statistics=loaded['statistics']))
    for loaded in results:
        context=DeadlineContext(min(deadline,time.monotonic()+60))
        if not context.is_active():raise TimeoutError('Index preload startup deadline exceeded')
        actual=manager.inspect(pb.InspectPublishedIndexRequest(database_id=loaded['database_id'],table=loaded['table']),context)
        if actual.descriptor is None or actual.descriptor.artifact_id!=loaded['artifact_id'] or actual.generation!=loaded['generation'] or not getattr(actual,loaded['device']+'_ready'):
            raise ValueError('Publication changed during startup preload')
    return results
