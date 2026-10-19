"""Process entry point. Keep imports stdlib-only until isolation is installed."""

import argparse
import json
import os
from pathlib import Path
import signal
import sys
import time


def main(argv=None):
    parser = argparse.ArgumentParser(description="VecStor P2 gRPC services")
    commands = parser.add_subparsers(dest="role", required=True)
    storage = commands.add_parser("storage")
    source=storage.add_mutually_exclusive_group(required=True)
    source.add_argument("--dataset", help="P2 legacy read-only snapshot")
    source.add_argument("--data-root", help="P2.5 managed persistent databases and imports")
    storage.add_argument("--no-candidate-plan-cache",action="store_true",help="Disable lease-local predicate plan reuse for ablation")
    storage.add_argument("--candidate-binding",choices=["text","legacy"],default="text",help="Candidate ID binding strategy for controlled ablation")
    storage.add_argument("--listen", default="127.0.0.1:50051")
    storage.add_argument('--index-artifact-max-mib',type=int,default=256)
    storage.add_argument('--index-storage-mib',type=int,default=2048)
    storage.add_argument('--index-disk-margin-mib',type=int,default=256)
    storage.add_argument('--index-verifier-memory-mib',type=int,default=512)
    compute = commands.add_parser("compute")
    compute.add_argument("--storage", required=True)
    compute.add_argument("--listen", default="127.0.0.1:50052")
    compute.add_argument("--timeout", type=float, default=10)
    compute.add_argument("--batch-rows", type=int, default=256)
    compute.add_argument('--data-cache-mib',type=int,default=0)
    compute.add_argument('--data-cache-max-entries',type=int,default=65536)
    compute.add_argument('--data-cache-scan-admission',choices=('off','on'),default='off')
    compute.add_argument('--metadata-projection',choices=('full','projected'),default='full')
    compute.add_argument("--index-memory-mib",type=int,default=256)
    compute.add_argument("--index-max-entries",type=int,default=8)
    compute.add_argument('--index-source',choices=('local','published'),default='local')
    compute.add_argument('--index-preload',help='Explicit compute-owned preload JSON; no training')
    compute.add_argument('--fused-candidates',action='store_true',help='Combine candidate filtering and vector reads (requires updated storage)')
    compute.add_argument('--gpu-index-memory-mib',type=int,default=512)
    compute.add_argument('--gpu-index-max-entries',type=int,default=8)
    compute.add_argument('--gpu-scratch-mib',type=int,default=128)
    compute.add_argument('--gpu-memory-margin-mib',type=int,default=256)
    compute.add_argument('--gpu-max-pending',type=int,default=8)
    compute.add_argument("--quality-profiles",help="Compute-owned snapshot-bound quality JSON (at most 1 MiB)")
    compute.add_argument("--isolate-storage-dir", help="Linux acceptance: deny filesystem access before starting any worker")
    args = parser.parse_args(argv)
    try:
        preload_report=[]
        if args.role == "compute":
            targets=[]
            if args.index_preload:
                if args.index_source!='published':raise ValueError('Index preload requires --index-source published')
                from vecstor.compute.index_recovery import read_preload
                targets=read_preload(args.index_preload,args.isolate_storage_dir)
            quality_document=None
            if args.quality_profiles:
                path=Path(args.quality_profiles).resolve(strict=True)
                if args.isolate_storage_dir and path.is_relative_to(Path(args.isolate_storage_dir).resolve()):
                    raise ValueError('Quality configuration must be outside storage data root')
                with path.open('rb') as source:payload=source.read(1024*1024+1)
                if len(payload)>1024*1024:raise ValueError('Quality configuration exceeds 1 MiB')
                quality_document=json.loads(payload)
            gpu_options=dict(max_bytes=args.gpu_index_memory_mib*1024**2,max_entries=args.gpu_index_max_entries,scratch_bytes=args.gpu_scratch_mib*1024**2,margin_bytes=args.gpu_memory_margin_mib*1024**2,max_pending=args.gpu_max_pending)
            if args.isolate_storage_dir:
                from vecstor.isolation import restrict_compute_filesystem
                isolation=restrict_compute_filesystem(args.isolate_storage_dir,gpu=True)
                print(json.dumps(isolation), flush=True)
                gpu_options['isolation_probe']=[isolation['storage_directory'],isolation['probe_file'],*isolation.get('artifact_probe_files',[])]
            from vecstor.cache.types import CacheConfig
            cache_config=CacheConfig(max_bytes=args.data_cache_mib*1024**2,max_entries=args.data_cache_max_entries,
                                     scan_admission=args.data_cache_scan_admission=='on')
            from vecstor.compute.service import create_compute_server
            server, port, backend = create_compute_server(args.storage, args.listen, args.timeout, args.batch_rows,
                                                        args.index_memory_mib,args.index_max_entries,quality_document,gpu_options,args.fused_candidates,args.index_source,cache_config,args.metadata_projection)
            if targets:
                try:
                    from vecstor.compute.index_recovery import preload_indexes
                    preload_report=preload_indexes(backend.sql.publication,targets,time.monotonic()+300)
                except Exception as error:
                    server.stop(0).wait();backend.close()
                    raise ValueError('Index preload failed: '+str(error)) from error
        else:
            if args.data_root:
                from vecstor.storage.sql_service import create_catalog_server
                artifact_options=dict(max_artifact_bytes=args.index_artifact_max_mib*1024**2,max_total_bytes=args.index_storage_mib*1024**2,
                    min_free_bytes=args.index_disk_margin_mib*1024**2,max_verify_bytes=args.index_verifier_memory_mib*1024**2)
                server, port, backend = create_catalog_server(args.data_root, args.listen,args.candidate_binding,not args.no_candidate_plan_cache,artifact_options)
            else:
                from vecstor.storage.service import create_storage_server
                server, port, backend = create_storage_server(args.dataset, args.listen)
        def stop(*_):
            server.stop(1)
        signal.signal(signal.SIGTERM, stop)
        signal.signal(signal.SIGINT, stop)
        server.start()
        print(json.dumps({"event": "ready", "role": args.role, "pid": os.getpid(),
                          "address": f"{args.listen.rsplit(':', 1)[0]}:{port}","preloaded_indexes":preload_report,
                          "index_recovery":getattr(backend,'recovery_report',None)}), flush=True)
        try:
            server.wait_for_termination()
        finally:
            server.stop(0).wait()
            backend.close()
        return 0
    except (OSError, ValueError, RuntimeError) as error:
        print(f"error: {error}", file=sys.stderr)
        return 2
