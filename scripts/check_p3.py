"""Run P3 CPU IVF real terminal, lifecycle and Landlock acceptance."""
import argparse
import json
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]/'src'))
from vecstor.rpc.p3_acceptance import run_acceptance

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output-dir', required=True)
    parser.add_argument('--rows', type=int, default=2048)
    parser.add_argument('--seed', type=int, default=20261013)
    args = parser.parse_args()
    try:
        print(json.dumps(run_acceptance(args.output_dir, args.rows, args.seed), indent=2, ensure_ascii=False))
    except (OSError, ValueError, RuntimeError, AssertionError) as error:
        print(f'error: {error}', file=sys.stderr)
        raise SystemExit(2)
