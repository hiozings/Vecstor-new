"""Regenerate checked-in gRPC modules using the locked grpcio-tools version."""

from pathlib import Path
from grpc_tools import protoc

root = Path(__file__).resolve().parents[1]
if __name__ == "__main__":
    raise SystemExit(protoc.main([
        "grpc_tools.protoc", f"-I{root / 'proto'}",
        f"--python_out={root / 'src'}", f"--grpc_python_out={root / 'src'}",
        *[str(path) for path in sorted((root / "proto/vecstor/rpc").glob("*.proto"))],
    ]))
