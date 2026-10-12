"""Process entry point. Keep imports stdlib-only until isolation is installed."""

import argparse
import json
import os
import signal
import sys


def main(argv=None):
    parser = argparse.ArgumentParser(description="VecStor P2 gRPC services")
    commands = parser.add_subparsers(dest="role", required=True)
    storage = commands.add_parser("storage")
    storage.add_argument("--dataset", required=True)
    storage.add_argument("--listen", default="127.0.0.1:50051")
    compute = commands.add_parser("compute")
    compute.add_argument("--storage", required=True)
    compute.add_argument("--listen", default="127.0.0.1:50052")
    compute.add_argument("--timeout", type=float, default=10)
    compute.add_argument("--batch-rows", type=int, default=256)
    compute.add_argument("--isolate-storage-dir", help="Linux acceptance: deny filesystem access before starting any worker")
    args = parser.parse_args(argv)
    try:
        if args.role == "compute":
            if args.isolate_storage_dir:
                from vecstor.isolation import restrict_compute_filesystem
                print(json.dumps(restrict_compute_filesystem(args.isolate_storage_dir)), flush=True)
            from vecstor.compute.service import create_compute_server
            server, port, backend = create_compute_server(args.storage, args.listen, args.timeout, args.batch_rows)
        else:
            from vecstor.storage.service import create_storage_server
            server, port, backend = create_storage_server(args.dataset, args.listen)
        def stop(*_):
            server.stop(1)
        signal.signal(signal.SIGTERM, stop)
        signal.signal(signal.SIGINT, stop)
        server.start()
        print(json.dumps({"event": "ready", "role": args.role, "pid": os.getpid(),
                          "address": f"{args.listen.rsplit(':', 1)[0]}:{port}"}), flush=True)
        try:
            server.wait_for_termination()
        finally:
            server.stop(0).wait()
            backend.close()
        return 0
    except (OSError, ValueError, RuntimeError) as error:
        print(f"error: {error}", file=sys.stderr)
        return 2
