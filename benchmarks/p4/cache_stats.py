"""Isolate legacy database-stat traversal versus constant-time process snapshots.

Run separately from RPC experiments; these timings are not query latencies.
"""
import argparse
import hashlib
import json
from pathlib import Path
import platform
import random
import time

import numpy as np

from vecstor.cache.types import CacheConfig, DataScope
from vecstor.cache.vector_cache import VersionedVectorCache


def run(output_dir, samples=100, repeats=3):
    if samples < 20 or repeats < 3:
        raise ValueError('Use at least 20 samples and three repeats')
    output = Path(output_dir)
    output.mkdir(parents=True, exist_ok=False)
    scope = DataScope('probe', 'db', 'table', 1, 'schema', 128)
    root = Path(__file__).resolve().parents[2]
    paths = ['src/vecstor/cache/vector_cache.py', 'benchmarks/p4/cache_stats.py']
    report = dict(python=platform.python_version(), platform=platform.platform(),
                  samples=samples, repeats=repeats, seed=20261023,
                  scope='isolated single-thread call latency, not end-to-end query latency',
                  source_sha256={p: hashlib.sha256((root/p).read_bytes()).hexdigest() for p in paths}, runs=[])
    rng = random.Random(report['seed'])
    for size in (0, 8191, 42785, 65535):
        cache = VersionedVectorCache(CacheConfig(max_bytes=128*1024**2))
        for start in range(0, size, 1024):
            ids = tuple(range(start, min(start+1024, size)))
            with cache.batch(scope, ids, lambda: None) as batch:
                batch.finish(ids, ids, np.zeros((len(ids), 128), dtype='<f4'),
                             admit=True, check=lambda: None)
        snapshot = cache.process_snapshot()
        assert snapshot == cache.stats('db')['process']
        assert snapshot['entry_count'] == size
        methods = [('legacy_stats', lambda: cache.stats('db')['process']),
                   ('process_snapshot', cache.process_snapshot)]
        for repeat in range(1, repeats+1):
            rng.shuffle(methods)
            for name, function in methods:
                for _ in range(5):
                    function()
                timings = []
                for _ in range(samples):
                    start = time.perf_counter_ns()
                    result = function()
                    timings.append(time.perf_counter_ns()-start)
                    assert result == snapshot
                report['runs'].append(dict(entries=size, repeat=repeat, method=name,
                    p50_us=float(np.median(timings)/1000),
                    p95_us=float(np.percentile(timings, 95)/1000), elapsed_ns=timings))
        cache.close()
    (output/'report.json').write_text(json.dumps(report, indent=2)+'\n')
    return report


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output-dir', required=True)
    parser.add_argument('--samples', type=int, default=100)
    parser.add_argument('--repeats', type=int, default=3)
    args = parser.parse_args()
    result = run(args.output_dir, args.samples, args.repeats)
    print(json.dumps(dict(runs=len(result['runs']), samples=result['samples'])))
