"""Run process, PTY, version and recovery checks for explicit adaptive queries."""
import argparse
import json
from pathlib import Path
import sys

sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'src'))
from vecstor.rpc.p4_adaptive_acceptance import run

if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output-dir',required=True)
    parser.add_argument('--rows',type=int,default=10000)
    parser.add_argument('--dimension',type=int,default=128)
    parser.add_argument('--calibration-dir')
    args=parser.parse_args()
    report=run(args.output_dir,args.rows,args.dimension,args.calibration_dir)
    print(json.dumps(dict(passed=report['passed'],checks=len(report['checks']),scope=report['scope'])))
