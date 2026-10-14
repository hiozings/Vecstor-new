"""Validate frozen calibration via real RPC and bind profiles to actual snapshots."""
import argparse
from contextlib import ExitStack
import hashlib
import json
from pathlib import Path
import shutil
import sys
import time

sys.path.insert(0,str(Path(__file__).resolve().parents[2]/'src'))
import numpy as np
from vecstor.datasets import load_dataset
from vecstor.index.types import IndexKey
from vecstor.query.search_options import SearchOptions
from vecstor.query.statements import classify_statement
from vecstor.rpc.acceptance import ServiceProcess
from vecstor.rpc.sql_client import SessionClient


def _hash(value):return hashlib.sha256(json.dumps(value,sort_keys=True).encode()).hexdigest()


def bind_profile(profile_id, built, sql, frozen, summary, heldout_digests):
    plan=classify_statement(sql).vector
    selected=next(r['summary'] for r in frozen['grid'] if r['settings']==frozen['settings'])
    settings=frozen['settings']
    return {'format_version':1,'profile_id':profile_id,
            'index_key':{k:built[k] for k in IndexKey.__dataclass_fields__},
            'workload':{'metadata_sql':plan.metadata_sql,'k':plan.limit,'path':frozen['workload']['path'],
                        'query_min':0.,'query_max':1.},
            'budget':{'nprobe_floor':settings['nprobe'],'initial_candidates':settings['candidates'],
                      'max_candidates':settings['max_candidates'],'max_rounds':settings['max_rounds']},
            'evidence':{'calibration_queries':len(frozen['calibration_query_digests']),
                        'evaluation_queries':summary['queries'],'calibration_mean_recall':selected['ann_mean_recall'],
                        'evaluation_mean_recall':summary['ann_mean_recall'],
                        'incomplete_queries':summary['incomplete_queries'],'exact_queries':summary['exact_queries'],
                        'dataset_sha256':frozen['dataset_sha256'],
                        'calibration_queries_sha256':_hash(frozen['calibration_query_digests']),
                        'evaluation_queries_sha256':_hash(heldout_digests),
                        'scope':'empirical mean Recall@10; not an online or per-query guarantee'}}


def run(calibration_dir, output_dir, samples=4, repeats=3):
    calibration=Path(calibration_dir);output=Path(output_dir)
    offline=json.loads((calibration/'report.json').read_text())
    if not offline['passed']:raise ValueError('Offline quality evaluation failed')
    if samples<1 or repeats<1:raise ValueError('Samples and repeats must be positive')
    output.mkdir(parents=True,exist_ok=False)
    config=json.loads((calibration/'config.json').read_text())
    if samples>config['evaluation_count']:raise ValueError('Sample count exceeds heldout split')
    expected={};digests={}
    for line in (calibration/'heldout.jsonl').read_text().splitlines():
        r=json.loads(line);key=(r['size'],r['distribution'],r['workload']['selectivity'],r['workload']['path'])
        digests.setdefault(key,[]).append(r['query_digest'])
        if r['query_id']<samples:expected[(*key,r['query_id'])]=r
    profiles=[];records=[];processes=[];scenarios=[]
    with (output/'cases.jsonl').open('x') as raw:
        for n in config['sizes']:
            for distribution in config['distributions']:
                source=calibration/f'{n}-{distribution}';directory=output/f'{n}-{distribution}'
                root=directory/'storage';(root/'imports').mkdir(parents=True)
                shutil.copy2(source/'dataset.npz',root/'imports/source.npz')
                dataset=load_dataset(source/'dataset.npz')
                with np.load(source/'queries.npz',allow_pickle=False) as q:queries=q['evaluation'][:samples]
                build=json.loads((source/'build.json').read_text())
                with ExitStack() as cleanup:
                    storage=ServiceProcess(['storage','--data-root',str(root),'--listen','127.0.0.1:0'],directory/'storage.log')
                    cleanup.callback(storage.stop);processes.append(storage.process)
                    compute=ServiceProcess(['compute','--storage',storage.address,'--listen','127.0.0.1:0','--timeout','180'],directory/'compute.log')
                    cleanup.callback(compute.stop);processes.append(compute.process)
                    client=cleanup.enter_context(SessionClient(compute.address,timeout=180))
                    client.open_database('quality',create=True);client.import_dataset('source.npz','items')
                    built=client.build_index('items',nlist=build['nlist'],seed=build['config']['seed'],timeout=60)
                    for s in config['selectivities']:
                        for path in ('filter-first','search-first'):
                            key=(n,distribution,float(s),path)
                            frozen=json.loads((source/f'{s:g}-{path}.json').read_text())
                            group=next(g for g in offline['groups'] if (g['size'],g['distribution'],g['workload']['selectivity'],g['workload']['path'])==key)
                            settings=frozen['settings'];profile_id=f'q{n}-{distribution}-{s:g}-{path}'.replace('.','p')
                            example=f'SELECT id, vector_distance(vector,{json.dumps(queries[0].tolist())}) AS distance FROM items WHERE value < {int(n*s)} ORDER BY distance LIMIT 10'
                            profiles.append(bind_profile(profile_id,built,example,frozen,group['summary'],digests[key]))
                            baseline=[]
                            for qi,q in enumerate(queries):
                                sql=f'SELECT id, vector_distance(vector,{json.dumps(q.tolist())}) AS distance FROM items WHERE value < {int(n*s)} ORDER BY distance LIMIT 10'
                                oracle=expected[(*key,qi)]
                                for repeat in range(repeats):
                                    client.search_options=SearchOptions(mode='approx',path=path,nprobe=settings['nprobe'],
                                        candidates=settings['candidates'],max_candidates=settings['max_candidates'],
                                        max_rounds=settings['max_rounds'],fallback='error')
                                    started=time.perf_counter();result=client.execute(sql);ms=(time.perf_counter()-started)*1000
                                    found=[row[0] for row in result['rows']]
                                    matches=found==oracle['returned_ids']
                                    if not matches:raise AssertionError(f'RPC differs from frozen result: {key} query {qi}')
                                    truth=set(oracle['truth_ids']);recall=len(set(found)&truth)/len(truth) if truth else None
                                    for item,distance in result['rows']:
                                        delta=dataset.vectors[item].astype(np.float64)-q.astype(np.float64)
                                        if distance!=float(np.sum(delta*delta)):raise AssertionError('RPC distance mismatch')
                                    r={'size':n,'distribution':distribution,'selectivity':s,'path':path,'query_id':qi,'repeat':repeat,
                                       'matches_frozen':matches,'recall':recall,'wall_ms':ms,'plan':result['execution_plan']}
                                    records.append(r);raw.write(json.dumps(r)+'\n');raw.flush()
                                if qi==0:
                                    client.search_options=SearchOptions(mode='approx',path=path,nprobe=min(8,built['nlist']))
                                    b=client.execute(sql);ids=set(row[0] for row in b['rows'])
                                    baseline.append({'query_id':qi,'recall':len(ids&truth)/len(truth) if truth else None,
                                                     'plan':b['execution_plan']})
                            scenarios.append({'size':n,'distribution':distribution,'selectivity':s,'path':path,'baseline':baseline})
                            print('validated',n,distribution,s,path,flush=True)
    (output/'quality-profiles.json').write_text(json.dumps({'format_version':1,'profiles':profiles},indent=2))
    (output/'baseline.json').write_text(json.dumps(scenarios,indent=2))
    report={'passed':True,'queries':len(records),'groups':len(profiles),'samples_per_group':samples,'repeats':repeats,
            'scope':'RPC consistency with independent frozen heldout results; subset is not a new quality estimate',
            'processes_running_after_cleanup':any(p.poll() is None for p in processes)}
    (output/'report.json').write_text(json.dumps(report,indent=2));return report


def validate_loaded_profiles(calibration_dir, rpc_dir, output_dir, samples=4, repeats=3):
    calibration=Path(calibration_dir);existing=Path(rpc_dir);output=Path(output_dir)
    original=json.loads((existing/'report.json').read_text())
    if not original['passed'] or original['processes_running_after_cleanup']:raise ValueError('RPC snapshot evidence is not complete')
    config=json.loads((calibration/'config.json').read_text())
    if samples<1 or samples>config['evaluation_count'] or repeats<1:raise ValueError('Invalid sample/repeat counts')
    bundle=json.loads((existing/'quality-profiles.json').read_text())
    from vecstor.query.quality_policy import QualityCatalog
    QualityCatalog.from_document(bundle)
    expected={}
    for line in (calibration/'heldout.jsonl').read_text().splitlines():
        r=json.loads(line)
        if r['query_id']<samples:expected[(r['size'],r['distribution'],r['workload']['selectivity'],r['workload']['path'],r['query_id'])]=r
    output.mkdir(parents=True,exist_ok=False);processes=[];isolation=[];count=0
    with (output/'cases.jsonl').open('x') as raw:
        for n in config['sizes']:
            for distribution in config['distributions']:
                directory=output/f'{n}-{distribution}';directory.mkdir()
                root=(existing/f'{n}-{distribution}/storage').resolve()
                source=calibration/f'{n}-{distribution}'
                dataset=load_dataset(source/'dataset.npz')
                with np.load(source/'queries.npz',allow_pickle=False) as q:queries=q['evaluation'][:samples]
                build=json.loads((source/'build.json').read_text())
                with ExitStack() as cleanup:
                    storage=ServiceProcess(['storage','--data-root',str(root),'--listen','127.0.0.1:0'],directory/'storage.log')
                    cleanup.callback(storage.stop);processes.append(storage.process)
                    compute=ServiceProcess(['compute','--storage',storage.address,'--listen','127.0.0.1:0','--timeout','180',
                                            '--quality-profiles',str((existing/'quality-profiles.json').resolve()),
                                            '--isolate-storage-dir',str(root)],directory/'compute.log')
                    cleanup.callback(compute.stop);processes.append(compute.process)
                    event=next(e for e in compute.events() if e.get('event')=='filesystem_isolation')
                    if not event['directory_denied'] or not event['file_denied']:raise AssertionError('Missing isolation proof')
                    isolation.append(event)
                    client=cleanup.enter_context(SessionClient(compute.address,timeout=180));client.open_database('quality')
                    built=client.build_index('items',nlist=build['nlist'],seed=build['config']['seed'],timeout=60)
                    for s in config['selectivities']:
                        for path in ('filter-first','search-first'):
                            profile_id=f'q{n}-{distribution}-{s:g}-{path}'.replace('.','p')
                            profile=next(p for p in bundle['profiles'] if p['profile_id']==profile_id)
                            if profile['index_key']!={k:built[k] for k in IndexKey.__dataclass_fields__}:raise AssertionError('Snapshot changed before profile validation')
                            for qi,q in enumerate(queries):
                                sql=f'SELECT id, vector_distance(vector,{json.dumps(q.tolist())}) AS distance FROM items WHERE value < {int(n*s)} ORDER BY distance LIMIT 10'
                                oracle=expected[(n,distribution,float(s),path,qi)]
                                for repeat in range(repeats):
                                    client.search_options=SearchOptions(mode='approx',path=path,quality_profile_id=profile_id,fallback='error')
                                    started=time.perf_counter();result=client.execute(sql);ms=(time.perf_counter()-started)*1000
                                    trace=result['execution_plan']
                                    if trace['quality_profile']['state']!='applied' or trace['actual_mode']!='approx':raise AssertionError('Quality profile did not apply')
                                    if [r[0] for r in result['rows']]!=oracle['returned_ids']:raise AssertionError('Profile result differs from frozen oracle')
                                    for item,distance in result['rows']:
                                        delta=dataset.vectors[item].astype(np.float64)-q.astype(np.float64)
                                        if distance!=float(np.sum(delta*delta)):raise AssertionError('Profile distance mismatch')
                                    raw.write(json.dumps({'size':n,'distribution':distribution,'selectivity':s,'path':path,
                                                         'query_id':qi,'repeat':repeat,'wall_ms':ms,'plan':trace})+'\n');raw.flush();count+=1
                            print('loaded profile validated',profile_id,flush=True)
    report={'passed':True,'queries':count,'isolation':isolation,
            'processes_running_after_cleanup':any(p.poll() is None for p in processes),
            'scope':'loaded quality profiles, real RPC, frozen ID/distance equivalence, entire storage root denied'}
    (output/'report.json').write_text(json.dumps(report,indent=2));return report


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--calibration-dir',required=True)
    parser.add_argument('--output-dir',required=True);parser.add_argument('--samples',type=int,default=4)
    parser.add_argument('--repeats',type=int,default=3);parser.add_argument('--existing-rpc-dir');args=parser.parse_args()
    report=validate_loaded_profiles(args.calibration_dir,args.existing_rpc_dir,args.output_dir,args.samples,args.repeats) if args.existing_rpc_dir else run(args.calibration_dir,args.output_dir,args.samples,args.repeats)
    print(json.dumps(report,indent=2))
