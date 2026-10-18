"""P4 isolated two-compute lifecycle acceptance; deterministic faults use pytest."""
import argparse
import json
from pathlib import Path
import sys

sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'src'))
from vecstor.rpc.p4_index_acceptance import run_acceptance

if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output-dir',required=True)
    parser.add_argument('--rows',type=int,default=10000)
    parser.add_argument('--dimension',type=int,default=128)
    parser.add_argument('--seed',type=int,default=20261016)
    parser.add_argument('--distribution',choices=('independent','correlated'),default='independent')
    parser.add_argument('--require-gpu',action='store_true')
    args=parser.parse_args()
    result=run_acceptance(args.output_dir,args.rows,args.dimension,args.seed,args.require_gpu,args.distribution)
    print(json.dumps(dict(passed=result['passed'],cases=len(result['cases']),gpu_verified=result['gpu_verified'])))
