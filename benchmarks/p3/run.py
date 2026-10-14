"""Reproducible CPU P3 matrix; preserves raw actual plans and P1 truth."""
import argparse
from contextlib import ExitStack
import hashlib
from importlib.metadata import version
import json
import os
from pathlib import Path
import platform
import sys
import time

sys.path.insert(0, str(Path(__file__).resolve().parents[2]/'src'))
import faiss
import numpy as np

from vecstor.datasets import generate_dataset, save_dataset
from vecstor.query.engine import LocalEngine
from vecstor.query.search_options import SearchOptions
from vecstor.rpc.acceptance import ServiceProcess
from vecstor.rpc.sql_client import SessionClient

SELECTIVITIES = [.001, .01, .1, .5, 1.]
KS = [10, 100]


def write_json(path, value):
    path.write_text(json.dumps(value, indent=2, ensure_ascii=False, allow_nan=False)+'\n')


def source_fingerprint():
    digest = hashlib.sha256()
    root = Path(__file__).resolve().parents[2]
    for path in sorted((root/'src').rglob('*.py')):
        digest.update(str(path.relative_to(root)).encode())
        digest.update(path.read_bytes())
    return digest.hexdigest()


def peak_rss(pid):
    try:
        for line in Path(f'/proc/{pid}/status').read_text().splitlines():
            if line.startswith('VmHWM:'):
                return int(line.split()[1])*1024
    except OSError:
        pass
    return None


def run(output_dir, sizes=(10000,100000), dimension=128,
        distributions=('independent','correlated'), repeats=3, seed=20261013, code_version='unspecified'):
    if not sizes or any(size < 1024 for size in sizes) or dimension < 1 or repeats < 1:
        raise ValueError('Use sizes >=1024, positive dimension and repeats')
    if any(distribution not in ('independent','correlated') for distribution in distributions):
        raise ValueError('Unknown distribution')
    output = Path(output_dir).resolve()
    output.mkdir(parents=True, exist_ok=False)
    configuration = dict(sizes=list(sizes), dimension=dimension, distributions=list(distributions),
                         repeats=repeats, seed=seed, selectivities=SELECTIVITIES, ks=KS,
                         index='CPU_IVFFlat', nprobe=8, fallback='exact', code_version=code_version,
                         source_sha256=source_fingerprint(), query_policy='one fixed vector per selectivity/K, identical across paths and repetitions',
                         cache_policy='one explicit cold build per dataset, sequential warm queries; no data cache exists')
    write_json(output/'config.json', configuration)
    hardware = dict(platform=platform.platform(), machine=platform.machine(), processor=platform.processor(),
                    logical_cpus=os.cpu_count(), python=sys.version, faiss=faiss.__version__, numpy=np.__version__,
                    omp_threads=1, worker_omp_threads=2,
                    dependencies={name:version(name) for name in ['duckdb','sqlglot','grpcio','protobuf']})
    if Path('/proc/cpuinfo').exists():
        hardware['cpu_model'] = next((line.split(':',1)[1].strip() for line in Path('/proc/cpuinfo').read_text().splitlines()
                                      if line.startswith('model name')), 'unknown')
    if Path('/proc/meminfo').exists():
        hardware['memory_total_bytes'] = int(Path('/proc/meminfo').read_text().splitlines()[0].split()[1])*1024
    write_json(output/'environment.json', hardware)
    scenarios, processes, count, approximate, fallbacks = [], [], 0, 0, 0
    elapsed = time.monotonic()
    with (output/'cases.jsonl').open('x') as raw:
        for size in sizes:
            for distribution in distributions:
                directory = output/f'{size}-{distribution}'
                directory.mkdir()
                root = directory/'storage'
                dataset = generate_dataset(size, dimension, seed, distribution)
                save_dataset(dataset, root/'imports/data.npz', dict(seed=seed, distribution=distribution))
                with ExitStack() as cleanup:
                    storage = ServiceProcess(['storage','--data-root',str(root),'--listen','127.0.0.1:0'], directory/'storage.log')
                    cleanup.callback(storage.stop)
                    processes.append(storage.process)
                    compute = ServiceProcess(['compute','--storage',storage.address,'--listen','127.0.0.1:0',
                                              '--timeout','180'], directory/'compute.log')
                    cleanup.callback(compute.stop)
                    processes.append(compute.process)
                    client = cleanup.enter_context(SessionClient(compute.address, timeout=180))
                    client.open_database('benchmark', create=True)
                    started = time.monotonic()
                    client.import_dataset('data.npz','items')
                    import_ms = (time.monotonic()-started)*1000
                    client.clear_indexes()
                    started = time.monotonic()
                    build = client.build_index('items', timeout=60, seed=seed)
                    build_wall_ms = (time.monotonic()-started)*1000
                    local = cleanup.enter_context(LocalEngine(dataset))
                    for selectivity_index, selectivity in enumerate(SELECTIVITIES):
                        matched = max(1, int(size*selectivity))
                        condition = f'WHERE value < {matched}'
                        for k in KS:
                            query_seed = seed + selectivity_index*1000 + k
                            query_vector = np.random.default_rng(query_seed).random(dimension, dtype=np.float32)
                            baseline = local.execute(f'SELECT id FROM items {condition} ORDER BY vector_distance(vector, :query_vector) LIMIT {k}', query_vector)
                            truth = [row['id'] for row in baseline.rows]
                            wanted = [[row['id'], distance] for row,distance in zip(baseline.rows, baseline.distances)]
                            literal = json.dumps(query_vector.tolist(), separators=(',',':'))
                            sql = f'SELECT id, vector_distance(vector, {literal}) AS distance FROM items {condition} ORDER BY distance LIMIT {k}'
                            for repeat in range(repeats):
                                for mode,path in [('exact','filter-first'),('approx','filter-first'),('approx','search-first')]:
                                    client.search_options = SearchOptions(mode=mode,path=path,nprobe=8 if mode=='approx' else None)
                                    started = time.monotonic()
                                    result = client.execute(sql)
                                    wall_ms = (time.monotonic()-started)*1000
                                    plan = result['execution_plan']
                                    found = [row[0] for row in result['rows']]
                                    if len(found) != len(set(found)) or len(found) != len(truth):
                                        raise AssertionError('Duplicate IDs or incomplete query result')
                                    if any(not 0 <= item < size or dataset.values[item] >= matched for item in found):
                                        raise AssertionError('Invalid result or predicate violation')
                                    if result['rows'] != sorted(result['rows'], key=lambda row:(row[1],row[0])):
                                        raise AssertionError('Result order mismatch')
                                    # Independently validate distances on the original float32 vectors.
                                    for item,distance in result['rows']:
                                        delta = dataset.vectors[item].astype(np.float64)-query_vector.astype(np.float64)
                                        if distance != float(np.sum(delta*delta)):
                                            raise AssertionError('Distance differs from float64 squared L2')
                                    if plan['actual_mode']=='exact' and result['rows'] != wanted:
                                        raise AssertionError('Exact result differs from P1 truth')
                                    denominator = min(k, baseline.matched_count)
                                    recall = len(set(found)&set(truth))/denominator if denominator else None
                                    is_fallback = mode=='approx' and plan['actual_mode']=='exact'
                                    approximate += plan['actual_mode']=='approx'
                                    fallbacks += is_fallback
                                    entry = dict(size=size, dimension=dimension, distribution=distribution,
                                                 selectivity=selectivity, matched_count=baseline.matched_count,
                                                 k=k, repeat=repeat, query_seed=query_seed, query_vector=query_vector.tolist(),
                                                 requested_mode=mode, requested_path=path, actual_mode=plan['actual_mode'],
                                                 actual_path=plan['actual_path'], exact_fallback=is_fallback,
                                                 empty_truth=not denominator, denominator=denominator, recall_at_k=recall,
                                                 returned_count=len(found), insufficient_results=len(found)<len(truth),
                                                 truth_ids=truth, returned_ids=found, client_elapsed_ms=wall_ms, execution_plan=plan)
                                    raw.write(json.dumps(entry, allow_nan=False)+'\n')
                                    raw.flush()
                                    count += 1
                    scenario = dict(size=size, distribution=distribution, import_ms=import_ms, build_wall_ms=build_wall_ms,
                                    build=build, compute_peak_rss_bytes=peak_rss(compute.process.pid),
                                    storage_peak_rss_bytes=peak_rss(storage.process.pid), indexes=client.list_indexes())
                    scenarios.append(scenario)
                    write_json(directory/'scenario.json', scenario)
                print(f'completed {size} {distribution}: {count} cases', flush=True)
    groups = {}
    for line in (output/'cases.jsonl').read_text().splitlines():
        case = json.loads(line)
        key = tuple(case[name] for name in ['size','distribution','selectivity','k','requested_mode','requested_path'])
        groups.setdefault(key, []).append(case)
    summary = []
    for key, cases in groups.items():
        latencies = [case['client_elapsed_ms'] for case in cases]
        true_ann = [case['recall_at_k'] for case in cases if case['actual_mode']=='approx' and not case['empty_truth']]
        summary.append(dict(zip(['size','distribution','selectivity','k','requested_mode','requested_path'],key),
                            repeats=len(cases), p50_ms=float(np.percentile(latencies,50)),
                            p95_ms=float(np.percentile(latencies,95)),
                            serial_query_throughput_per_second=len(cases)*1000/sum(latencies),
                            mean_recall_at_k=float(np.mean([case['recall_at_k'] for case in cases if not case['empty_truth']])),
                            mean_true_ann_recall_at_k=float(np.mean(true_ann)) if true_ann else None,
                            exact_fallback_cases=sum(case['exact_fallback'] for case in cases),
                            insufficient_result_cases=sum(case['insufficient_results'] for case in cases)))
    write_json(output/'summary.json',summary)
    report = dict(passed=True, scope='CPU localhost serial warm-index experiments; no universal recall/speed guarantee',
                  case_count=count, actual_approximate_cases=approximate, exact_fallback_cases=fallbacks,
                  elapsed_seconds=time.monotonic()-elapsed, configuration=configuration, scenarios=scenarios,
                  processes_running_after_cleanup=any(process.poll() is None for process in processes))
    write_json(output/'report.json',report)
    return report


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output-dir',required=True)
    parser.add_argument('--sizes',type=int,nargs='+',default=[10000,100000])
    parser.add_argument('--dimension',type=int,default=128)
    parser.add_argument('--distributions',nargs='+',default=['independent','correlated'])
    parser.add_argument('--repeats',type=int,default=3)
    parser.add_argument('--seed',type=int,default=20261013)
    parser.add_argument('--code-version',default='unspecified')
    args=parser.parse_args()
    try:
        report=run(args.output_dir,args.sizes,args.dimension,args.distributions,args.repeats,args.seed,args.code_version)
        print(json.dumps({key:report[key] for key in ['case_count','actual_approximate_cases','exact_fallback_cases','elapsed_seconds']}))
    except (OSError,ValueError,RuntimeError,AssertionError) as error:
        print(f'error: {error}',file=sys.stderr)
        raise SystemExit(2)
