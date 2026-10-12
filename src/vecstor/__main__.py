"""Console entry point; keep service isolation ahead of non-stdlib imports."""

import argparse
import sys


def main(argv=None):
    args=list(sys.argv[1:] if argv is None else argv)
    if args and args[0]=="serve":
        from vecstor.rpc.server import main as serve
        return serve(args[1:])
    if args and args[0] in {"generate","query","remote-query","demo"}:
        from vecstor.cli import main as legacy
        return legacy(args)
    if args and args[0]=="shell": args=args[1:]
    parser=argparse.ArgumentParser(description="VecStor interactive SQL terminal. Other commands: serve, generate, query, remote-query, demo")
    parser.add_argument("--compute",default="127.0.0.1:50052")
    parser.add_argument("--timeout",type=float,default=15)
    options=parser.parse_args(args)
    from vecstor.rpc.sql_client import SessionClient
    from vecstor.shell import run_shell
    try:
        return run_shell(SessionClient(options.compute,options.timeout))
    except (ValueError,OSError) as error:
        print(f"error: {error}",file=sys.stderr)
        return 2


if __name__=="__main__":
    raise SystemExit(main())
