"""Start storage/compute without importing data libraries before isolation."""

from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from vecstor.rpc.server import main

if __name__ == "__main__":
    raise SystemExit(main())
