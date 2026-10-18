"""Re-evaluate a frozen workload against the actual published CPU/GPU artifact."""
import argparse
from dataclasses import asdict
import hashlib
import json
from pathlib import Path
import runpy
import sys
import time

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0,str(ROOT/'src'))
import numpy as np
from vecstor.data import Dataset
from vecstor.datasets import load_dataset
from vecstor.index.types import BuildConfig
from vecstor.query.quality_policy import QualityCatalog
from vecstor.query.search_options import SearchOptions
from vecstor.query.statements import classify_statement, safe_name
from vecstor.rpc import sql_pb2 as pb
from vecstor.rpc.index_codec import descriptor_from_proto
from vecstor.rpc.sql_client import SessionClient

helper = runpy.run_path(str(ROOT/'benchmarks/p3/calibrate.py'))


def _hash(value):
    return hashlib.sha256(json.dumps(value,sort_keys=True).encode()).hexdigest()


def run(calibration_dir, compute_address, database, table, output_dir, device,
        selectivity=1.0, k=10, path='search-first'):
    source, output = Path(calibration_dir), Path(output_dir)
    config = json.loads((source/'config.json').read_text())
    if config['calibration_count']<200 or config['evaluation_count']<500:
        raise ValueError('Artifact profiles require at least 200 calibration and 500 evaluation queries')
    if device not in ('cpu','gpu') or path not in ('filter-first','search-first'):
        raise ValueError('Invalid device or path')
    if device=='gpu' and path!='search-first':raise ValueError('GPU requires search-first')
    table = safe_name(table)
    output.mkdir(parents=True,exist_ok=False)
    report = dict(passed=False,device=device,scope='artifact consistency regression on existing disjoint heldout queries; no new tuning or generalization claim')
    started = time.monotonic()
    try:
        with SessionClient(compute_address,timeout=60) as client:
            client.open_database(database)
            request = pb.InspectPublishedIndexRequest(database_id=client.database_id,table=table)
            inspection = client.stub.InspectPublishedIndex(request,timeout=10)
            if inspection.index_source!='published' or inspection.state!='ready' or not getattr(inspection,device+'_ready'):
                raise ValueError('Requested published artifact is not loaded')
            descriptor = descriptor_from_proto(inspection.descriptor)
            actual = client.execute(f'SELECT id, category, timestamp, value, vector FROM "{table}" ORDER BY id')
            if actual['revision']!=descriptor.key.revision:raise ValueError('Data revision changed before quality validation')
            rows = actual['rows']
            data = Dataset(np.array([r[0] for r in rows],dtype=np.int64),[r[1] for r in rows],
                np.array([r[2] for r in rows],dtype=np.int64),np.array([r[3] for r in rows],dtype=np.float64),
                np.array([r[4] for r in rows],dtype=np.float32))
            fingerprint = helper['dataset_digest'](data)
            candidates = []
            for candidate in source.glob('*/dataset.npz'):
                saved = load_dataset(candidate)
                if saved.size==data.size and saved.dimension==data.dimension and helper['dataset_digest'](saved)==fingerprint:
                    candidates.append(candidate.parent)
            if len(candidates)!=1:raise ValueError('Actual dataset fingerprint does not uniquely match calibration data')
            if config.get('faiss')!=descriptor.faiss_version:raise ValueError('Frozen Faiss version differs from published backend')
            scenario = candidates[0]
            distribution = scenario.name.split('-',1)[1]
            filename = f'{selectivity:g}-k{k}.json' if device=='gpu' else f'{selectivity:g}-{path}.json'
            frozen = json.loads((scenario/filename).read_text())
            work = dict(selectivity=float(selectivity),path=path,k=k)
            settings = frozen['settings']
            if frozen['workload']!=work or frozen['dataset_sha256']!=fingerprint or settings is None:
                raise ValueError('Frozen workload, dataset or settings mismatch')
            build = BuildConfig(**json.loads((scenario/'build.json').read_text())['config'])
            if build!=descriptor.build_config or build.fingerprint()!=descriptor.key.build_fingerprint:
                raise ValueError('Published artifact build configuration differs from frozen calibration')
            with np.load(scenario/'queries.npz',allow_pickle=False) as arrays:
                calibration, evaluation = arrays['calibration'], arrays['evaluation']
            if calibration.dtype!=np.dtype('float32') or evaluation.dtype!=np.dtype('float32'):
                raise ValueError('Frozen query arrays must preserve float32 identity')
            if calibration.shape!=(config['calibration_count'],data.dimension) or evaluation.shape!=(config['evaluation_count'],data.dimension):
                raise ValueError('Query split shape mismatch')
            if not np.isfinite(calibration).all() or not np.isfinite(evaluation).all() or min(calibration.min(),evaluation.min())<0 or max(calibration.max(),evaluation.max())>1:
                raise ValueError('Unsupported query domain')
            cal_hashes = list(map(helper['digest_query'],calibration))
            held_hashes = list(map(helper['digest_query'],evaluation))
            if cal_hashes!=frozen['calibration_query_digests'] or len(set(cal_hashes))!=len(cal_hashes) or len(set(held_hashes))!=len(held_hashes) or set(cal_hashes)&set(held_hashes):
                raise ValueError('Query identities or split independence mismatch')
            old_records = []
            with (source/'heldout.jsonl').open() as raw:
                for line in raw:
                    record=json.loads(line)
                    if record['size']==data.size and record['distribution']==distribution and record['workload']==work:old_records.append(record)
            old_records.sort(key=lambda r:r['query_id'])
            if [r['query_id'] for r in old_records]!=list(range(len(evaluation))) or [r['query_digest'] for r in old_records]!=held_hashes:
                raise ValueError('Heldout query evidence mismatch')
            selected = [r['summary'] for r in frozen['grid'] if r['settings']==settings]
            if len(selected)!=1 or selected[0]['queries']!=len(calibration) or selected[0]['ann_mean_recall']<.97 or selected[0]['incomplete_queries'] or selected[0]['exact_queries']:
                raise ValueError('Insufficient frozen calibration evidence')
            execution = dict(device=device,precision='float32',faiss_version=descriptor.faiss_version,gpu_model='not_applicable')
            if device=='gpu':
                replicas=[r for r in client.list_indexes(table) if r.get('device')=='gpu' and r.get('artifact_id')==descriptor.artifact_id]
                if len(replicas)!=1:raise ValueError('GPU artifact identity is ambiguous')
                execution['gpu_model']=replicas[0]['gpu_model']
                if execution!=frozen['execution']:raise ValueError('GPU backend differs from frozen calibration')
            client.search_options=SearchOptions(mode='approx',path=path,device=device,fallback='error',device_fallback='error',**settings)
            threshold = int(data.size*selectivity)
            allowed = np.flatnonzero(data.values<threshold)
            if len(allowed)<k:raise ValueError('Quality evidence requires at least K legal rows')
            allowed_ids=data.ids[allowed]
            vectors=data.vectors[allowed].astype(np.float64)
            recalls=[]
            with (output/'cases.jsonl').open('x') as raw:
                for qi, query in enumerate(evaluation):
                    sql=f'SELECT id,vector_distance(vector,{json.dumps(query.tolist())}) AS distance FROM "{table}" WHERE value < {threshold} ORDER BY distance LIMIT {k}'
                    distances=np.sum((vectors-query.astype(np.float64))**2,axis=1,dtype=np.float64)
                    truth=allowed_ids[np.lexsort((allowed_ids,distances))[:k]]
                    result=client.execute(sql); plan=result['execution_plan']
                    if result['revision']!=descriptor.key.revision or plan.get('artifact_id')!=descriptor.artifact_id or plan['actual_mode']!='approx' or plan['actual_device']!=device:
                        raise ValueError('Quality query changed artifact, mode, device or snapshot')
                    found=[r[0] for r in result['rows']]
                    if len(found)!=k or len(set(found))!=k or not set(found).issubset(set(allowed_ids.tolist())):
                        raise ValueError('Incomplete, duplicate or unfiltered quality result')
                    for identity,distance in result['rows']:
                        position=int(np.searchsorted(data.ids,identity))
                        expected=float(np.sum((data.vectors[position].astype(np.float64)-query.astype(np.float64))**2,dtype=np.float64))
                        if distance!=expected:raise ValueError('Quality result distance mismatch')
                    recall=len(set(found)&set(truth.tolist()))/k
                    recalls.append(recall)
                    raw.write(json.dumps(dict(query_id=qi,query_digest=held_hashes[qi],truth_ids=truth.tolist(),returned_ids=found,recall=recall,plan=plan))+'\n')
                    raw.flush()
            final=client.stub.InspectPublishedIndex(request,timeout=10)
            if final.generation!=inspection.generation or final.descriptor!=inspection.descriptor:raise ValueError('Publication changed during quality validation')
            mean=float(np.mean(recalls))
            if mean<.95:raise ValueError(f'Artifact evaluation Recall {mean} is below 0.95')
            profile=dict(format_version=3,profile_id=f'p4-{device}-{data.size}-{distribution}-{path}-{k}',
                index_key=asdict(descriptor.key),execution=execution,
                artifact_identity={name:getattr(descriptor,name) for name in ('artifact_id','payload_sha256','snapshot_sha256')},
                workload=dict(metadata_sql=classify_statement(sql).vector.metadata_sql,k=k,path=path,query_min=0.,query_max=1.),
                budget=dict(nprobe_floor=settings['nprobe'],initial_candidates=settings['candidates'],max_candidates=settings['max_candidates'],max_rounds=settings['max_rounds']),
                evidence=dict(calibration_queries=len(calibration),evaluation_queries=len(evaluation),
                    calibration_mean_recall=selected[0]['ann_mean_recall'],evaluation_mean_recall=mean,incomplete_queries=0,exact_queries=0,
                    dataset_sha256=fingerprint,calibration_queries_sha256=_hash(cal_hashes),evaluation_queries_sha256=_hash(held_hashes),
                    scope=report['scope']))
            document=dict(format_version=3,profiles=[profile]); QualityCatalog.from_document(document)
            (output/'quality-profiles.json').write_text(json.dumps(document,indent=2)+'\n')
            report.update(passed=True,artifact=descriptor.to_dict(),generation=inspection.generation,queries=len(evaluation),mean_recall=mean)
    except Exception as error:
        report['error']=str(error)
        raise
    finally:
        report['elapsed_seconds']=time.monotonic()-started
        (output/'report.json').write_text(json.dumps(report,indent=2)+'\n')
    return report


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    for name in ('calibration-dir','compute','database','table','output-dir'):parser.add_argument('--'+name,required=True)
    parser.add_argument('--device',choices=('cpu','gpu'),default='cpu')
    parser.add_argument('--selectivity',type=float,default=1.)
    parser.add_argument('--k',type=int,default=10)
    parser.add_argument('--path',choices=('filter-first','search-first'),default='search-first')
    args=parser.parse_args()
    print(json.dumps(run(args.calibration_dir,args.compute,args.database,args.table,args.output_dir,args.device,args.selectivity,args.k,args.path)))
