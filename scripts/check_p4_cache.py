"""Run isolated cache/projection lifecycle acceptance in the registered environment."""
import argparse
import json
from pathlib import Path
import sys

# scripts/vecstor.py would otherwise shadow the registered package.
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'src'))

from vecstor.rpc.p4_cache_acceptance import run_cache_acceptance

if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output-dir',required=True)
    parser.add_argument('--rows',type=int,default=10000)
    parser.add_argument('--dimension',type=int,default=128)
    parser.add_argument('--require-gpu',action='store_true')
    args=parser.parse_args()
    report=run_cache_acceptance(args.output_dir,args.rows,args.dimension,args.require_gpu)
    print(json.dumps(dict(passed=report['passed'],cases=len(report['cases']),gpu_verified=report['gpu_verified'])))
