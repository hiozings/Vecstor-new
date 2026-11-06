"""Prepare frozen adaptive evidence and evaluate it without online training."""
import argparse
from concurrent.futures import ThreadPoolExecutor
from contextlib import ExitStack, contextmanager
from copy import deepcopy
from dataclasses import replace
import hashlib
import json
import math
import os
from pathlib import Path
import runpy
import shutil
import sys
import time
from threading import Event, Lock

ROOT=Path(__file__).resolve().parents[2]
sys.path.insert(0,str(Path(__file__).resolve().parent))
import numpy as np
from adaptive_cases import matrix_cases
from adaptive_costs import (make_query_splits,query_digest,digest_list,quality_summary,fit_cost_model,
                           latency_summary,source_manifest,validate_resume,assess_performance)
from adaptive_evidence import (write_json,sql_for,workload_name,dataset_digest,Oracle,settings_for,
                               make_quality_profile,round_statistics,fit_training_wire_layout,forecast_record,read_configuration)
from vecstor.datasets import generate_dataset,save_dataset,load_dataset
from vecstor.index.artifact import SnapshotHasher
from vecstor.compute.planning_inputs import execution_signature
from vecstor.query.adaptive_policy import AdaptivePolicy
from vecstor.query.search_options import SearchOptions
from vecstor.query.statements import classify_statement
from vecstor.rpc.acceptance import ServiceProcess
from vecstor.rpc.sql_client import SessionClient
from vecstor.rpc.common import RemoteError

Monitor=runpy.run_path(str(ROOT/'benchmarks/p4/index_lifecycle.py'))['Monitor']
start_proxy=runpy.run_path(str(ROOT/'tests/support/delayed_storage.py'))['start_proxy']


def selected_cases(tier,case_ids):
    cases=matrix_cases(tier)
    ids=list(range(len(cases))) if case_ids is None else list(case_ids)
    if not ids or len(ids)!=len(set(ids)) or any(type(i) is not int or not 0<=i<len(cases) for i in ids):
        raise ValueError('Invalid adaptive case selection')
    return [(i,cases[i]) for i in ids]


def config_manifest(tier,case_ids,seed,repeats,exact_fit_queries=5):
    return dict(format_version=1,tier=tier,case_ids=[i for i,_ in selected_cases(tier,case_ids)],
        seed=seed,data_seed=20261016,repeats=repeats,exact_fit_queries=exact_fit_queries,source=source_manifest(ROOT))


def freeze_bundle(root):
    files=[]
    for directory in ('groups','costs'):
        for path in (root/directory).rglob('*'):
            relative=path.relative_to(root)
            if path.is_file() and not {'storage','logs'}.intersection(relative.parts) and path.suffix in ('.json','.jsonl','.npz'):
                files.append(path)
    files.append(root/'policy.json')
    write_json(root/'bundle.json',{str(p.relative_to(root)):hashlib.sha256(p.read_bytes()).hexdigest() for p in sorted(files)})


def verify_bundle(root):
    for name,digest in json.loads((root/'bundle.json').read_text()).items():
        path=root/name
        if path.is_absolute() and not path.resolve().is_relative_to(root.resolve()):raise ValueError('Invalid evidence path')
        if not path.is_file() or hashlib.sha256(path.read_bytes()).hexdigest()!=digest:
            raise ValueError('Frozen calibration evidence changed: '+name)


def previous_query_digests(directories):
    excluded=set()
    for directory in directories:
        root=Path(directory).resolve();verify_bundle(root)
        bundle=json.loads((root/'bundle.json').read_text())
        paths=list((root/'groups').glob('*/queries.npz'))
        if not paths:raise ValueError('Previous evidence has no query pools')
        for path in paths:
            if str(path.relative_to(root)) not in bundle:raise ValueError('Previous query pool is not frozen')
            with np.load(path,allow_pickle=False) as pools:
                for name in pools.files:
                    for query in pools[name]:excluded.add(query_digest(query))
    return excluded


class Group:
    def __init__(self,directory,spec,*,create=False,processes=None):
        self.directory=Path(directory);self.spec=spec
        self.directory.mkdir(parents=True,exist_ok=True)
        self.root=self.directory/'storage';self.processes=processes if processes is not None else []
        self.stack=ExitStack();self.serial=0
        dataset=self.directory/'dataset.npz'
        if create and not dataset.exists():
            save_dataset(generate_dataset(spec['rows'],spec['dimension'],20261016,spec['distribution']),dataset)
            (self.root/'imports').mkdir(parents=True,exist_ok=True)
            shutil.copyfile(dataset,self.root/'imports/source.npz')
        self.data=load_dataset(dataset);self.oracle=Oracle(self.data)
        self.attempt=self.directory/'logs'/str(time.time_ns());self.attempt.mkdir(parents=True)
        self.storage=self._start(['storage','--data-root',str(self.root),'--listen','127.0.0.1:0'],'storage')
        self.empty_policy=self.directory/'empty-policy.json'
        if not self.empty_policy.exists():write_json(self.empty_policy,dict(format_version=1,policy_id='calibration',workloads=[]))
        info=self.directory/'artifact.json'
        try:
            with self.compute('prepare',capacity=0) as (client,process):
                client.open_database('bench',create=create)
                if create:
                    imported=client.import_dataset('source.npz','items',operation_id='adaptive-import')
                    expected=hashlib.sha256(dataset.read_bytes()).hexdigest()
                    if imported.get('source_sha256')!=expected:raise AssertionError('Import source receipt differs')
                    client.publish_index('items',operation_id='adaptive-publish')
                loaded=client.load_index('items')
                self.descriptor=loaded['descriptor']
                hasher=SnapshotHasher(self.data.size,self.data.dimension);hasher.add(self.data.ids,self.data.vectors)
                if self.descriptor['snapshot_sha256']!=hasher.finish():raise AssertionError('Published snapshot differs')
                if info.exists() and json.loads(info.read_text())!=self.descriptor:raise ValueError('Published artifact changed')
                if not info.exists():write_json(info,self.descriptor)
                self.database_id=client.database_id
        except BaseException:self.close();raise

    def _start(self,args,label):
        self.serial+=1
        process=ServiceProcess(args,self.attempt/f'{self.serial}-{label}.log')
        self.processes.append(process.process);self.stack.callback(process.stop)
        return process

    @contextmanager
    def compute(self,label,*,capacity=64,quality=None,policy=None,network='loopback',address=None,mode='apply'):
        args=['compute','--storage',address or self.storage.address,'--listen','127.0.0.1:0','--timeout','180',
              '--index-source','published','--isolate-storage-dir',str(self.root),'--metadata-projection','projected',
              '--data-cache-mib',str(capacity),'--adaptive-policy',str(policy or self.empty_policy),
              '--adaptive-network-class',network,'--adaptive-mode',mode]
        if quality:args+=['--quality-profiles',str(quality)]
        process=self._start(args,label)
        try:
            with SessionClient(process.address,timeout=180) as client:yield client,process
        finally:process.stop()

    def close(self):self.stack.close()


def timed_query(client,spec,query,group,*,validate=True):
    began=time.perf_counter();result=client.execute(sql_for(spec,query));elapsed=(time.perf_counter()-began)*1000
    record=dict(query_digest=query_digest(query),query=query.tolist(),ms=elapsed,plan=result['execution_plan'],rows=result['rows'],revision=result['revision'])
    if validate:record.update(group.oracle.validate(spec,query,result,group.descriptor))
    return record


def quality_component(group,spec,pools,output):
    output=Path(output);output.mkdir(parents=True,exist_ok=True)
    checkpoint=output/'report.json'
    if checkpoint.exists():
        report=json.loads(checkpoint.read_text())
        for name,digest in report['raw_sha256'].items():
            if hashlib.sha256((output/name).read_bytes()).hexdigest()!=digest:raise ValueError('Component quality evidence changed')
        return report
    profiles=[];summaries={};records_by_path={}
    with group.compute('quality',capacity=0) as (client,process):
        client.open_database('bench');client.load_index('items')
        count=client.execute(f'SELECT COUNT(*) FROM items WHERE value < {int(spec["rows"]*spec["selectivity"])}')['rows'][0][0]
        if count!=int(np.sum(group.data.values<int(spec['rows']*spec['selectivity']))):raise AssertionError('Metadata cardinality mismatch')
        for path in ('filter-first','search-first'):
            client.search_options=settings_for(spec,group.descriptor,path,cache='off')
            phases={}
            for phase in ('calibration','quality'):
                raw=output/f'{path}-{phase}.jsonl'
                if raw.exists():raw.rename(output/f'{raw.name}.incomplete-{time.time_ns()}')
                records=[]
                with raw.open('x') as sink:
                    for i,query in enumerate(pools[phase]):
                        try:record=timed_query(client,spec,query,group)
                        except RemoteError as error:
                            if error.details not in ('candidate_budget','round_budget','no_progress'):raise
                            record=dict(query_digest=query_digest(query),recall=0.,actual_mode='error',complete=False,error=str(error))
                        records.append(record);sink.write(json.dumps(record,allow_nan=False)+'\n');sink.flush()
                        if (i+1)%50==0:print(json.dumps(dict(workload=workload_name(spec),path=path,phase=phase,queries=i+1)),flush=True)
                phases[phase]=records
            profile=make_quality_profile(spec,group.descriptor,path,phases['calibration'],phases['quality'],dataset_digest(group.data))
            if profile:profiles.append(profile)
            summaries[path]={phase:quality_summary(records) for phase,records in phases.items()}
            records_by_path[path]=f'{path}-calibration.jsonl'
    report=dict(profiles=profiles,summaries=summaries,calibration_files=records_by_path,matched_rows=count,
                artifact_id=group.descriptor['artifact_id'],
                raw_sha256={p.name:hashlib.sha256(p.read_bytes()).hexdigest() for p in output.glob('*.jsonl')})
    write_json(checkpoint,report);return report


def cost_samples(group,spec,pools,quality_file,profile,mode,path,fallback,directory,smoke=False,exact_fit_queries=5):
    directory=Path(directory);directory.mkdir(parents=True,exist_ok=True)
    if (directory/'report.json').exists():return json.loads((directory/'report.json').read_text())
    if (directory/'failure.json').exists():raise ValueError('Previously failed cost sampling requires new experiment')
    options=(SearchOptions(mode='exact',data_cache='on',metadata_projection='projected',candidate_data_path='separate')
             if mode=='exact' else settings_for(spec,group.descriptor,path,fallback=fallback,profile_id=profile['profile_id']))
    if mode=='exact' and (smoke or exact_fit_queries==5):
        split=2 if smoke else 96
        train_indices=[0] if smoke else [0,1,2]
        validation_indices=[split] if smoke else [split,split+1]
        indices=train_indices+validation_indices;repeats=3
    else:
        split=2 if smoke else 96
        indices=list(range(len(pools['fit'])));validation_indices=list(range(split,len(pools['fit'])));repeats=1
    read=read_configuration(spec,pools['fit'][0]);records=[];stats_records=[]
    hardware=None;started=time.monotonic();resources={}
    order_seed=int(query_digest(pools['fit'][0])[:16],16)
    rng=np.random.default_rng(order_seed)
    raw=directory/'cases.jsonl';attempts=directory/'attempts.jsonl'
    for prior in (raw,attempts):
        if prior.exists():prior.rename(directory/f'{prior.stem}.incomplete-{time.time_ns()}.jsonl')
    with ExitStack() as cleanup, group.compute('cost',capacity=spec['capacity_mib'],quality=quality_file) as (client,process):
        monitor=Monitor(group.root/'indexes',False)
        cleanup.callback(lambda:resources.update(monitor.close()))
        monitor.observe('storage',group.storage.process.pid);monitor.observe('compute',process.process.pid)
        client.open_database('bench');client.load_index('items');client.search_options=options
        caps=client.get_search_capabilities();hardware=caps['adaptive_hardware_fingerprint']
        client.execute('EXPLAIN '+sql_for(spec,pools['fit'][0]))
        with attempts.open('x') as sink:
            for repeat in range(repeats):
                client.clear_data_cache();loss=0;reuse={}
                for phase in ('cold','warm'):
                    for i in rng.permutation(indices).tolist():
                        query=pools['fit'][i];digest=query_digest(query)
                        predicted_warm=reuse.get(digest)==loss
                        identity=dict(phase=phase,repeat=repeat+1,query_id=i,query_digest=digest)
                        sink.write(json.dumps(dict(event='started',**identity))+'\n');sink.flush()
                        try:record=timed_query(client,spec,query,group,validate=False)
                        except BaseException as error:
                            failure=dict(**identity,error=type(error).__name__+': '+str(error))
                            sink.write(json.dumps(dict(event='error',**failure))+'\n');sink.flush()
                            write_json(directory/'failure.json',failure);raise
                        record.update(phase=phase,repeat=repeat+1,query_id=i,predicted_warm=predicted_warm)
                        cache=record['plan']['data_cache'];loss+=cache['evictions']
                        retained=sum(cache.get(k,0) for k in ('vectors_served_from_cache','admitted_rows','duplicate_inserts'))
                        if not cache['evictions'] and not cache['bypass_rows'] and retained>=record['plan']['counts']['vectors_used_for_rerank']:
                            reuse[digest]=loss
                        else:reuse.pop(digest,None)
                        records.append(record);sink.write(json.dumps(dict(event='received',**record),allow_nan=False)+'\n');sink.flush()
    # Match evaluation: oracle work follows the timed query sequences.
    with raw.open('x') as sink:
        for record in records:
            try:
                record.update(group.oracle.validate(spec,np.asarray(record['query'],dtype='<f4'),
                    dict(rows=record['rows'],revision=record['revision'],execution_plan=record['plan']),group.descriptor))
            except BaseException as error:
                write_json(directory/'failure.json',dict(error=type(error).__name__+': '+str(error),query_id=record['query_id']))
                raise
            sink.write(json.dumps(record,allow_nan=False)+'\n')
    if profile:
        stats_records=[json.loads(line) for line in Path(profile['_calibration_file']).read_text().splitlines()]
    stats=round_statistics(stats_records)
    stats=fit_training_wire_layout(records,spec['dimension'],stats,validation_indices)
    transformed=[forecast_record(spec,group.descriptor,options,stats,r,read) for r in records]
    training=[r for r,raw in zip(transformed,records) if raw['query_id'] not in validation_indices]
    validation=[r for r,raw in zip(transformed,records) if raw['query_id'] in validation_indices]
    fit=fit_cost_model(training,validation)
    candidate='exact' if mode=='exact' else 'ann-filter' if path=='filter-first' else 'ann-search'
    model=dict(candidate_id=candidate,execution_fingerprint=execution_signature(options,read),hardware_fingerprint=hardware,
        network_class='loopback',concurrency_bucket=1,coefficients=fit['coefficients'],residual_ms=fit['residual_ms'],
        statistics=stats,evidence=fit['evidence'])
    report=dict(accepted=fit['accepted'],model=model,fit=fit,options=options.to_dict(),elapsed_seconds=time.monotonic()-started,
        samples=len(records),distinct_queries=len(indices),scope='limited exact probes' if mode=='exact' and exact_fit_queries==5 else 'disjoint fit/validation queries',
        raw_sha256=hashlib.sha256(raw.read_bytes()).hexdigest(),attempts_sha256=hashlib.sha256(attempts.read_bytes()).hexdigest(),
        sampling_order_seed=order_seed,resources=resources,timing_target='compute_elapsed_ms; oracle after timing')
    write_json(directory/'report.json',report);return report


def build_policy(calibration_dir, output_path):
    root=Path(calibration_dir);manifest=json.loads((root/'manifest.json').read_text())
    if manifest['source']!=source_manifest(ROOT):raise ValueError('Calibration source manifest changed')
    workloads={};quality={}
    for case_id,spec in selected_cases(manifest['tier'],manifest['case_ids']):
        group_name=f"{spec['rows']}-{spec['distribution']}-{spec['dimension']}"
        directory=root/'groups'/group_name;descriptor=json.loads((directory/'artifact.json').read_text())
        name=workload_name(spec);component=json.loads((directory/'quality'/name/'report.json').read_text())
        key=(group_name,name)
        work=workloads.setdefault(key,dict(workload_id=name,
            data_identity={k:v for k,v in descriptor['key'].items() if k!='build_fingerprint'},
            artifact_identity=dict(artifact_id=descriptor['artifact_id'],payload_sha256=descriptor['payload_sha256'],
                build_fingerprint=descriptor['key']['build_fingerprint']),
            metadata_sql=classify_statement(sql_for(spec,np.zeros(spec['dimension'],dtype='<f4'))).vector.metadata_sql,
            k=spec['k'],table_rows=spec['rows'],matched_rows=component['matched_rows'],
            profiles={p['workload']['path']:p['profile_id'] for p in component['profiles']},models=[]))
        for path in sorted((root/'costs'/f'case-{case_id}').glob('*/report.json')):
            report=json.loads(path.read_text());raw=path.parent/'cases.jsonl'
            if hashlib.sha256(raw.read_bytes()).hexdigest()!=report['raw_sha256']:raise ValueError('Calibration raw evidence changed')
            if 'attempts_sha256' in report and hashlib.sha256((path.parent/'attempts.jsonl').read_bytes()).hexdigest()!=report['attempts_sha256']:
                raise ValueError('Calibration attempt evidence changed')
            # A rejected latency fit must not discard independent failure evidence.
            work['models'].append(dict(report['model'],cost_usable=report['accepted']))
    document=dict(format_version=1,policy_id='p4_adaptive_frozen_v2',workloads=list(workloads.values()))
    AdaptivePolicy.from_document(document)
    write_json(output_path,document)
    return document


def prepare(output_dir,tier='representative',case_ids=None,seed=20261103,resume=False,exact_fit_queries=5,
            exclude_calibration_dirs=()):
    if exact_fit_queries not in (5,128):raise ValueError('Exact fit queries must be 5 or 128')
    output=Path(output_dir).resolve();configuration=config_manifest(tier,case_ids,seed,3,exact_fit_queries)
    excluded=previous_query_digests(exclude_calibration_dirs)
    configuration['previous_queries']=dict(count=len(excluded),sha256=digest_list(sorted(excluded)))
    configuration['previous_query_bundles']=sorted(hashlib.sha256((Path(p)/'bundle.json').read_bytes()).hexdigest()
                                                   for p in exclude_calibration_dirs)
    for dimension in {spec['dimension'] for _,spec in selected_cases(tier,case_ids)}:
        make_query_splits(dimension,seed,smoke=tier=='smoke',excluded_digests=excluded)
    if output.exists():
        if not resume:raise ValueError('Calibration output already exists')
        validate_resume(json.loads((output/'manifest.json').read_text()),configuration)
    else:output.mkdir(parents=True);write_json(output/'manifest.json',configuration)
    processes=[];report=dict(completed=False,cases=[],processes_running_after_cleanup=[]);start=time.monotonic()
    try:
        groups={}
        for case_id,spec in selected_cases(tier,case_ids):
            group_name=f"{spec['rows']}-{spec['distribution']}-{spec['dimension']}"
            groups.setdefault(group_name,[]).append((case_id,spec))
        for name,cases in groups.items():
            print(json.dumps(dict(phase='prepare_dataset',group=name)),flush=True)
            group=Group(output/'groups'/name,cases[0][1],create=True,processes=processes)
            try:
                pools=make_query_splits(cases[0][1]['dimension'],seed,smoke=tier=='smoke',excluded_digests=excluded)
                query_path=group.directory/'queries.npz'
                if query_path.exists():
                    with np.load(query_path,allow_pickle=False) as existing:
                        if set(existing.files)!=set(pools) or any(not np.array_equal(existing[k],v) for k,v in pools.items()):
                            raise ValueError('Frozen queries changed')
                else:
                    with query_path.open('xb') as target:np.savez(target,**pools)
                components={}
                for _,spec in cases:
                    name=workload_name(spec)
                    if name not in components:components[name]=quality_component(group,spec,pools,group.directory/'quality'/name)
                profiles=[p for component in components.values() for p in component['profiles']]
                quality_file=group.directory/'quality-profiles.json'
                write_json(quality_file,dict(format_version=3,profiles=profiles))
                for case_id,spec in cases:
                    component=components[workload_name(spec)];costs=[]
                    for path in ('filter-first','search-first'):
                        profile=next((p for p in component['profiles'] if p['workload']['path']==path),None)
                        if profile is None:continue
                        profile=dict(profile,_calibration_file=str(group.directory/'quality'/workload_name(spec)/component['calibration_files'][path]))
                        for fallback in ('error','exact'):
                            costs.append(cost_samples(group,spec,pools,quality_file,profile,'approx',path,fallback,
                                output/'costs'/f'case-{case_id}'/f'{path}-{fallback}',smoke=tier=='smoke'))
                    costs.append(cost_samples(group,spec,pools,quality_file,None,'exact','filter-first','exact',
                        output/'costs'/f'case-{case_id}'/'exact',smoke=tier=='smoke',exact_fit_queries=exact_fit_queries))
                    report['cases'].append(dict(case=case_id,eligible_paths=[p['workload']['path'] for p in component['profiles']],
                        quality_profiles=len(component['profiles']),cost_models_accepted=sum(c['accepted'] for c in costs)))
                    write_json(output/'report.json',report)
                    print(json.dumps(dict(phase='costs_complete',case=case_id,models=report['cases'][-1]['cost_models_accepted'])),flush=True)
            finally:group.close()
        build_policy(output,output/'policy.json');freeze_bundle(output);report['completed']=True
    except Exception as error:report['error']=str(error);raise
    finally:
        report['elapsed_seconds']=time.monotonic()-start
        report['processes_running_after_cleanup']=[p.pid for p in processes if p.poll() is None]
        write_json(output/'report.json',report)
    return report


def phase_summary(name,records,elapsed):
    summary=latency_summary([r['ms'] for r in records],elapsed)
    summary.update(name=name,mean_recall=float(np.mean([r['recall'] for r in records])),
        incomplete=sum(not r['complete'] for r in records),
        actual_modes={mode:sum(r['actual_mode']==mode for r in records) for mode in ('exact','approx')},
        rpc_calls=sum(r['plan']['counts']['rpc_calls'] for r in records),
        rpc_payload_bytes=sum(r['plan']['counts']['rpc_payload_bytes'] for r in records),
        vectors_fetched=sum(r['plan']['counts']['vectors_fetched'] for r in records),
        planner_feedback_p95_ms=float(np.percentile([r['plan'].get('adaptive',{}).get('planner_ms',0.)+
            r['plan'].get('adaptive',{}).get('feedback_ms',0.) for r in records],95)),
        decisions={path:sum(r['plan']['actual_path']==path for r in records) for path in ('filter-first','search-first')},
        model_coverage={cid:sum(any(e['candidate_id']==cid and e['eligible'] and e['mean_ms'] is not None
                                   for e in r['plan'].get('adaptive',{}).get('estimates',[])) for r in records)
                        for cid in ('ann-filter','ann-search')})
    return summary


def _evaluation_options(spec,descriptor,profiles,variant,smoke):
    if variant=='exact':
        return SearchOptions(data_cache='on',metadata_projection='projected',candidate_data_path='separate')
    if variant=='auto':
        return SearchOptions(mode='approx',path='auto',fallback='exact' if smoke else 'error',
                             data_cache='on',metadata_projection='projected',candidate_data_path='separate')
    if variant=='threshold':
        preferred='filter-first' if spec['selectivity']<=.01 else 'search-first'
        available=[p['workload']['path'] for p in profiles]
        variant=preferred if preferred in available else available[0]
    profile=next(p for p in profiles if p['workload']['path']==variant)
    return settings_for(spec,descriptor,variant,profile_id=profile['profile_id'])


def _trial(group,spec,queries,variant,options,policy_file,quality_file,repeat,output):
    output.mkdir(parents=True,exist_ok=True)
    if (output/'complete.json').exists():
        complete=json.loads((output/'complete.json').read_text())
        if complete.get('state')=='failed':raise ValueError('Previously failed trial cannot be promoted by resume')
        if complete.get('state')!='complete':raise ValueError('Trial completion state is unknown')
        if hashlib.sha256((output/'report.json').read_bytes()).hexdigest()!=complete['report_sha256']:
            raise ValueError('Completed trial summary changed')
        report=json.loads((output/'report.json').read_text());raw=output/'cases.jsonl'
        if hashlib.sha256(raw.read_bytes()).hexdigest()!=report['raw_sha256']:raise ValueError('Completed trial evidence changed')
        report['reused']=True
        return report,[json.loads(line) for line in raw.read_text().splitlines()]
    if (output/'attempts.jsonl').exists() and (output/'attempts.jsonl').stat().st_size:
        raise ValueError('Interrupted trial has recorded attempts; retain it as failed evidence and start a new experiment')
    if (output/'cases.jsonl').exists():
        (output/'cases.jsonl').rename(output/f'cases.incomplete-{time.time_ns()}.jsonl')
    process=None;monitor=Monitor(group.root/'indexes',False);monitor.observe('storage',group.storage.process.pid)
    report=dict(variant=variant,repeat=repeat,phases=[],equivalent=None,reused=False,
                timing_scope='query latency excludes journal; throughput includes journal, validation follows timing')
    records=[];received=[];proxy=None;failure=None
    lock=Lock();stop=Event();journal=(output/'attempts.jsonl').open('w');journal_seconds=0.
    def event(value):
        nonlocal journal_seconds
        with lock:
            started=time.perf_counter()
            journal.write(json.dumps(value,allow_nan=False)+'\n');journal.flush()
            journal_seconds+=time.perf_counter()-started
    try:
        address=group.storage.address;network='loopback'
        if spec.get('delay_ms') is not None:
            proxy=start_proxy(address,spec['delay_ms']);address=proxy.address
            network=f"proxy_{spec['delay_ms']:g}ms"
            monitor.observe('proxy-and-driver',os.getpid())
        with group.compute('evaluate-'+variant,capacity=spec['capacity_mib'],quality=quality_file,policy=policy_file,
                           address=address,network=network) as (client,process), ExitStack() as cleanup:
            monitor.observe('compute',process.process.pid)
            report['isolation']=next(e for e in process.events() if e.get('event')=='filesystem_isolation')
            client.open_database('bench');client.load_index('items');client.search_options=options
            report['deployment']=client.get_search_capabilities()['adaptive_hardware_fingerprint']
            clients=[client]+[cleanup.enter_context(SessionClient(process.address,timeout=180)) for _ in range(spec['concurrency']-1)]
            for worker in clients:
                worker.open_database('bench');worker.search_options=options
                worker.execute('EXPLAIN '+sql_for(spec,queries[0]))
            client.clear_data_cache();report['cache_start_entries']=client.data_cache_stats()['database']['entry_count']
            sequences=dict(cold=queries,warm=queries)
            if any(phase in ('independent','hotset') for phase in spec['patterns']):
                with np.load(group.directory/'queries.npz',allow_pickle=False) as source:pool=source['evaluation'].copy()
                other=pool[len(queries):2*len(queries)]
                if len(other)!=len(queries):raise ValueError('Insufficient independent heldout queries')
                sequences['independent']=other
                sequences['hotset']=np.array([other[(i//5)%len(other)] if i%5==0 else queries[(i-i//5-1)%min(20,len(queries))]
                                              for i in range(len(queries))],dtype='<f4')
            for phase in spec['patterns']:
                phase_queries=sequences[phase]
                samples=[];began=time.perf_counter()
                def query_worker(worker_id):
                    own=[]
                    for i in range(worker_id,len(phase_queries),len(clients)):
                        if stop.is_set():break
                        query=phase_queries[i]
                        identity=dict(query_id=i,query_digest=query_digest(query),phase=phase)
                        event(dict(event='started',**identity))
                        start=time.perf_counter()
                        try:result=clients[worker_id].execute(sql_for(spec,query))
                        except BaseException as error:
                            stop.set()
                            record=dict(**identity,ms=(time.perf_counter()-start)*1000,error=str(error),
                                        complete=False,actual_mode='error')
                            with lock:received.append(record)
                            event(dict(event='error',**record));raise
                        record=dict(**identity,query=query.tolist(),ms=(time.perf_counter()-start)*1000,
                            rows=result['rows'],revision=result['revision'],plan=result['execution_plan'],
                            complete=False,actual_mode='received_unvalidated')
                        with lock:received.append(record)
                        event(dict(event='received',**record));own.append(record)
                    return own
                if len(clients)==1:samples=query_worker(0)
                else:
                    with ThreadPoolExecutor(len(clients)) as workers:
                        samples=sorted((r for rows in workers.map(query_worker,range(len(clients))) for r in rows),key=lambda r:r['query_id'])
                elapsed=time.perf_counter()-began
                # All answer/oracle work is outside the timed throughput interval.
                for record,query in zip(samples,phase_queries):
                    try:
                        record.update(group.oracle.validate(spec,query,dict(rows=record['rows'],revision=record['revision'],
                                                                           execution_plan=record['plan']),group.descriptor))
                    except BaseException as error:
                        record.update(error=str(error),complete=False)
                        event(dict(event='validation_error',query_id=record['query_id'],phase=phase,error=str(error)))
                        raise
                    event(dict(event='validated',query_id=record['query_id'],phase=phase,recall=record['recall']))
                report['phases'].append(phase_summary(phase,samples,elapsed));records+=samples
            report['cache_end']=client.data_cache_stats()
    except BaseException as error:
        failure=error;report['error']=str(error)
    finally:
        journal.close()
        report['resources']=monitor.close()
        if proxy:proxy.close()
    raw=output/'cases.jsonl'
    with raw.open('x') as sink:
        for record in received:sink.write(json.dumps(record,allow_nan=False)+'\n')
    report.update(functional_passed=failure is None,attempted_queries=len(received),journal_seconds=journal_seconds)
    report['raw_sha256']=hashlib.sha256(raw.read_bytes()).hexdigest()
    write_json(output/'report.json',report)
    write_json(output/'complete.json',dict(state='complete' if failure is None else 'failed',
                                          report_sha256=hashlib.sha256((output/'report.json').read_bytes()).hexdigest()))
    if failure is not None:raise failure
    return report,records


def evaluate(calibration_dir,output_dir,*,tier='representative',case_ids=None,repeats=3,policy_path=None,
             resume=False,samples=None,full_exact=False,concurrency=1,delay_ms=None,patterns=None):
    if repeats<3:raise ValueError('At least three repeats are required')
    calibration=Path(calibration_dir).resolve();output=Path(output_dir).resolve()
    manifest=json.loads((calibration/'manifest.json').read_text())
    if manifest['source']!=source_manifest(ROOT):raise ValueError('Frozen calibration source differs')
    verify_bundle(calibration)
    if tier!=manifest['tier']:raise ValueError('Evaluation tier differs from prepared manifest')
    selected=selected_cases(tier,case_ids if case_ids is not None else manifest['case_ids'])
    if any(i not in manifest['case_ids'] for i,_ in selected):raise ValueError('Case was not calibrated')
    policy_file=Path(policy_path or calibration/'policy.json').resolve()
    policy=AdaptivePolicy.load(policy_file)
    if concurrency not in (1,4,8):raise ValueError('Concurrency must be 1, 4 or 8')
    if samples is not None and (type(samples) is not int or not 20<=samples<=200):raise ValueError('Formal samples must be 20..200')
    if patterns is not None and (list(patterns[:2])!=['cold','warm'] or len(set(patterns))!=len(patterns) or not set(patterns).issubset(('cold','warm','independent','hotset'))):
        raise ValueError('Invalid cache patterns')
    selected=[(i,dict(spec,samples=samples or spec['samples'],exact_samples=200 if full_exact else spec['exact_samples'],
                      concurrency=concurrency,delay_ms=delay_ms,patterns=patterns or spec['patterns'])) for i,spec in selected]
    expected_manifest=dict(calibration_manifest=manifest,cases=selected,repeats=repeats,
        policy_sha256=hashlib.sha256(policy_file.read_bytes()).hexdigest(),source=source_manifest(ROOT))
    # Normalize tuples before comparing JSON-based recovery records.
    expected_manifest=json.loads(json.dumps(expected_manifest))
    if output.exists():
        if not resume:raise ValueError('Evaluation output already exists')
        validate_resume(json.loads((output/'manifest.json').read_text()),expected_manifest)
        if (output/'report.json').exists():shutil.copyfile(output/'report.json',output/f'report.previous-{time.time_ns()}.json')
    else:output.mkdir(parents=True);write_json(output/'manifest.json',expected_manifest)
    smoke=tier=='smoke';processes=[];started=time.monotonic()
    report=dict(functional_passed=False,quality_certified=False,performance_passed=None,
        scope='smoke_exact_only' if smoke else 'representative adaptive CPU evidence',runs=[],quality=[],
        not_eligible=[],configuration_runs=0,reused_configuration_runs=0,measured_queries=0,processes_running_after_cleanup=[])
    rng=np.random.default_rng(manifest['seed'])
    try:
        groups={}
        for case_id,spec in selected:
            name=f"{spec['rows']}-{spec['distribution']}-{spec['dimension']}"
            groups.setdefault(name,[]).append((case_id,spec))
        for name,cases in groups.items():
            group=Group(calibration/'groups'/name,cases[0][1],processes=processes)
            try:
                with np.load(group.directory/'queries.npz',allow_pickle=False) as saved:
                    pool=saved['evaluation'].copy()
                quality_file=group.directory/'quality-profiles.json'
                for case_id,spec in cases:
                    component=json.loads((group.directory/'quality'/workload_name(spec)/'report.json').read_text())
                    profiles=component['profiles']
                    paths=[p['workload']['path'] for p in profiles]
                    variants=['exact','auto'] if smoke else paths+['auto','exact','threshold'] if paths else ['exact']
                    for path in ('filter-first','search-first'):
                        if path not in paths:report['not_eligible'].append(dict(case=case_id,path=path,reason='component_quality_not_certified'))
                    fixed={};automatic=[]
                    for repeat in range(1,repeats+1):
                        order=list(variants);rng.shuffle(order)
                        for variant in order:
                            options=_evaluation_options(spec,group.descriptor,profiles,variant,smoke)
                            queries=pool[:spec['exact_samples'] if variant=='exact' else spec['samples']]
                            trial,records=_trial(group,spec,queries,variant,options,policy_file,quality_file,repeat,
                                                 output/'runs'/f'case-{case_id}-r{repeat}-{variant}')
                            trial['case']=case_id;report['runs'].append(trial)
                            report['reused_configuration_runs']+=int(trial['reused'])
                            report['configuration_runs']+=1;report['measured_queries']+=len(records)
                            for record in records:
                                if variant=='auto':automatic.append(record)
                                else:
                                    key=(record['actual_mode'],record['plan']['actual_path'],record['query_digest'])
                                    if key in fixed and fixed[key]!=record['rows']:raise AssertionError('Fixed answer changed across repetitions')
                                    fixed[key]=record['rows']
                            write_json(output/'report.json',report)
                            print(json.dumps(dict(phase='evaluate',case=case_id,repeat=repeat,variant=variant,
                                                  queries=report['measured_queries'])),flush=True)
                    quality_records=[]
                    if paths and not smoke:
                        # A separate fresh history assesses the entire policy on 500 unseen vectors per state.
                        trial,quality_records=_trial(group,dict(spec,patterns=['cold','warm']),pool,'auto',_evaluation_options(spec,group.descriptor,profiles,'auto',False),
                            policy_file,quality_file,1,output/'quality'/f'case-{case_id}')
                        automatic+=quality_records
                    # Check selected-path equivalence outside performance intervals, using cache-off reads.
                    with group.compute('fixed-reference',capacity=0,quality=quality_file) as (reference,process):
                        reference.open_database('bench');reference.load_index('items')
                        for record in automatic:
                            plan=record['plan'];key=(plan['actual_mode'],plan['actual_path'],record['query_digest'])
                            if key not in fixed:
                                selected_options=SearchOptions(**plan['effective_options'])
                                reference.search_options=replace(selected_options,data_cache='off')
                                query=np.asarray(record['query'],dtype='<f4')
                                fixed[key]=reference.execute(sql_for(spec,query))['rows']
                            if record['rows']!=fixed[key]:raise AssertionError('Auto answer differs from selected fixed path')
                    if quality_records:
                        report['quality'].append(dict(case=case_id,phases={phase:quality_summary([r for r in quality_records if r['phase']==phase]) for phase in ('cold','warm')}))
                    for trial in report['runs']:
                        if trial['case']==case_id:trial['equivalent']=True
                    write_json(output/'report.json',report)
            finally:group.close()
        report['functional_passed']=bool(report['runs'])
        report['quality_certified']=not smoke and len(report['quality'])==len(selected) and all(
            phase['eligible'] for row in report['quality'] for phase in row['phases'].values())
        if not smoke and tier!='correctness':
            report['guards'],report['performance_passed']=assess_performance(report['runs'],[i for i,_ in selected],
                                                                             quality_certified=report['quality_certified'])
        else:
            report['performance_status']='NOT_RUN'
            report['performance_passed']=None
    except Exception as error:report['error']=str(error);raise
    finally:
        report['elapsed_seconds']=time.monotonic()-started
        report['processes_running_after_cleanup']=[p.pid for p in processes if p.poll() is None]
        write_json(output/'report.json',report)
    return report


def run_adaptive_matrix(output_dir, tier='smoke', policy_path=None, case_ids=None, repeats=3):
    output=Path(output_dir).resolve()
    if policy_path is None:
        if tier!='smoke':raise ValueError('Formal evaluation requires a prepared frozen policy')
        output.mkdir(parents=True,exist_ok=False)
        prepare(output/'calibration',tier=tier,case_ids=case_ids)
        report=evaluate(output/'calibration',output/'evaluation',tier=tier,case_ids=case_ids,repeats=repeats)
        write_json(output/'report.json',report);return report
    return evaluate(Path(policy_path).resolve().parent,output,tier=tier,case_ids=case_ids,repeats=repeats,policy_path=policy_path)



def pilot(output_dir,case_ids=(0,5),seed=20261103):
    output=Path(output_dir).resolve();output.mkdir(parents=True,exist_ok=False)
    write_json(output/'manifest.json',config_manifest('representative',case_ids,seed,1))
    processes=[];report=dict(completed=False,runs=[],scope='ten-query pilot for runtime estimates, not acceptance')
    try:
        grouped={}
        for case_id,spec in selected_cases('representative',case_ids):
            name=f"{spec['rows']}-{spec['distribution']}-{spec['dimension']}"
            grouped.setdefault(name,[]).append((case_id,spec))
        for name,cases in grouped.items():
            group=Group(output/'groups'/name,cases[0][1],create=True,processes=processes)
            try:
                queries=make_query_splits(cases[0][1]['dimension'],seed)['pilot']
                with group.compute('pilot',capacity=64) as (client,process):
                    client.open_database('bench');client.load_index('items')
                    for case_id,spec in cases:
                        for variant in ('filter-first','search-first','exact'):
                            client.search_options=(SearchOptions(data_cache='on',metadata_projection='projected',candidate_data_path='separate')
                                if variant=='exact' else settings_for(spec,group.descriptor,variant))
                            client.execute('EXPLAIN '+sql_for(spec,queries[0]));client.clear_data_cache()
                            records=[timed_query(client,spec,q,group) for q in queries]
                            row=dict(case=case_id,variant=variant,queries=len(records),
                                mean_ms=float(np.mean([r['ms'] for r in records])),p50_ms=float(np.median([r['ms'] for r in records])),
                                max_ms=max(r['ms'] for r in records))
                            report['runs'].append(row);write_json(output/'report.json',report)
                            print(json.dumps(row),flush=True)
            finally:group.close()
        report['completed']=True
    finally:
        report['processes_running_after_cleanup']=[p.pid for p in processes if p.poll() is None]
        write_json(output/'report.json',report)
    return report


def ablate(calibration_dir,output_dir,ablation,*,case_ids=None,repeats=3,samples=20):
    switches={'cache-input':'use_cache_evidence','transfer':'include_transfer_cost',
              'candidate-correlation':'use_candidate_correlation','observations':'use_observations'}
    if ablation not in switches:raise ValueError('Unknown ablation')
    output=Path(output_dir).resolve();output.mkdir(parents=True,exist_ok=False)
    policy=json.loads((Path(calibration_dir)/'policy.json').read_text())
    policy['policy_id']='p4_ablation_'+ablation.replace('-','_')
    policy.setdefault('parameters',{})[switches[ablation]]=False
    AdaptivePolicy.from_document(policy);write_json(output/'policy.json',policy)
    report=evaluate(calibration_dir,output/'evaluation',case_ids=case_ids,repeats=repeats,
                    policy_path=output/'policy.json',samples=samples)
    report['ablation']=ablation
    report['scope']='predeclared ablation; full-model fit error is not an ablation error guarantee'
    write_json(output/'report.json',report);return report


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('command',choices=('prepare','evaluate','smoke','pilot','ablate'))
    parser.add_argument('--output-dir',required=True)
    parser.add_argument('--calibration-dir')
    parser.add_argument('--tier',choices=('smoke','representative','correctness'),default='representative')
    parser.add_argument('--case-ids',nargs='+',type=int)
    parser.add_argument('--repeats',type=int,default=3)
    parser.add_argument('--seed',type=int,default=20261103)
    parser.add_argument('--resume',action='store_true')
    parser.add_argument('--exclude-calibration-dir',action='append',default=[])
    parser.add_argument('--exact-fit-queries',type=int,choices=(5,128),default=5)
    parser.add_argument('--full-exact',action='store_true')
    parser.add_argument('--samples',type=int)
    parser.add_argument('--concurrency',type=int,choices=(1,4,8),default=1)
    parser.add_argument('--delay-ms',type=float)
    parser.add_argument('--patterns',nargs='+')
    parser.add_argument('--ablation',choices=('cache-input','transfer','candidate-correlation','observations'))
    args=parser.parse_args()
    if args.exclude_calibration_dir and args.command!='prepare':parser.error('--exclude-calibration-dir requires prepare')
    if args.command=='prepare':result=prepare(args.output_dir,args.tier,args.case_ids,args.seed,args.resume,args.exact_fit_queries,
                                            args.exclude_calibration_dir)
    elif args.command=='smoke':result=run_adaptive_matrix(args.output_dir,'smoke',repeats=args.repeats)
    elif args.command=='pilot':result=pilot(args.output_dir,args.case_ids or (0,5),args.seed)
    elif args.command=='ablate':
        if not args.calibration_dir or not args.ablation:parser.error('--calibration-dir and --ablation are required')
        result=ablate(args.calibration_dir,args.output_dir,args.ablation,case_ids=args.case_ids,repeats=args.repeats,samples=args.samples or 20)
    else:
        if not args.calibration_dir:parser.error('--calibration-dir is required')
        result=evaluate(args.calibration_dir,args.output_dir,tier=args.tier,case_ids=args.case_ids,repeats=args.repeats,
                        resume=args.resume,samples=args.samples,full_exact=args.full_exact,concurrency=args.concurrency,delay_ms=args.delay_ms,patterns=args.patterns)
    print(json.dumps({k:v for k,v in result.items() if k in ('completed','functional_passed','quality_certified','performance_passed','elapsed_seconds')}))


if __name__=='__main__':main()
