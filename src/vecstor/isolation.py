"""Linux Landlock read/execute allowlist for the compute acceptance process.

Must run before importing numerical/RPC libraries or starting any threads.
This is an experiment boundary, not a complete multi-tenant sandbox.
"""

import ctypes
import os
from pathlib import Path
import platform
import sys


def _gpu_device_paths(device_dir=Path('/dev')):
    """Only actual visible NVIDIA character devices, independent of CUDA ordinals."""
    paths=[device_dir/name for name in ('null','nvidiactl','nvidia-uvm','nvidia-uvm-tools')]
    for path in sorted(device_dir.glob('nvidia*')):
        suffix=path.name.removeprefix('nvidia')
        if suffix and suffix.isascii() and suffix.isdigit():
            paths.append(path)
    return tuple(path for path in paths
                 if path.exists() and not path.is_symlink() and path.is_char_device())


def restrict_compute_filesystem(storage_dir, gpu=False):
    if sys.platform != "linux" or platform.machine() not in {"x86_64", "aarch64"}:
        raise RuntimeError("Landlock acceptance requires Linux x86_64/aarch64")
    directory = Path(storage_dir).resolve(strict=True)
    probes = sorted([*directory.glob("*.npz"), *directory.glob("imports/*.npz"),
                     *directory.glob("databases/*.duckdb")])
    if not probes:
        raise ValueError("Isolation probe requires an existing NPZ source or managed database")
    probe = probes[0].resolve(strict=True)
    artifact_probes=sorted([*directory.glob('indexes/objects/*/*/index.faiss'),*directory.glob('indexes/objects/*/*/manifest.json')])
    if any(path.is_symlink() or not path.resolve().is_relative_to(directory) for path in artifact_probes):
        raise ValueError('Artifact isolation probes may not escape storage directory')
    if not probe.is_relative_to(directory):
        raise ValueError("Isolation probe may not escape the storage directory")
    project = Path(__file__).resolve().parents[2]
    allowed = [Path(sys.prefix), project / "src", project / "scripts",
               Path("/usr"), Path("/lib"), Path("/lib64"), Path("/etc"), Path("/dev")]
    if gpu:
        # CUDA reads dynamic /proc/self mappings in descendant processes.
        # Procfs access is read-only; storage path targets remain Landlock-denied.
        allowed.extend([Path('/proc'),Path('/sys/devices/system/cpu')])
    allowed = sorted({path.resolve() for path in allowed if path.exists()})
    if any(directory.is_relative_to(path) or path.is_relative_to(directory) for path in allowed):
        raise ValueError("Storage directory overlaps the compute runtime allowlist")
    libc = ctypes.CDLL(None, use_errno=True)

    def checked(result, operation):
        if result < 0:
            raise OSError(ctypes.get_errno(), f"Landlock {operation} failed")
        return result

    abi = checked(libc.syscall(444, 0, 0, 1), "ABI query")
    if abi < 3:
        raise RuntimeError("Landlock ABI >= 3 is required, refusing unisolated fallback")

    class Ruleset(ctypes.Structure):
        _fields_ = [("handled_access_fs", ctypes.c_uint64)]

    class PathRule(ctypes.Structure):
        _pack_ = 1
        _fields_ = [("allowed_access", ctypes.c_uint64), ("parent_fd", ctypes.c_int32)]

    # Handle read, execute, mutation, refer and truncate rights (ABI 3).
    rules = Ruleset((1 << 15) - 1)
    rules_fd = checked(libc.syscall(444, ctypes.byref(rules), ctypes.sizeof(rules), 0), "create ruleset")
    try:
        for path in allowed:
            parent_fd = os.open(path, os.O_PATH | os.O_CLOEXEC)
            try:
                attr = PathRule((1 << 0) | (1 << 2) | (1 << 3), parent_fd)
                checked(libc.syscall(445, rules_fd, 1, ctypes.byref(attr), 0), "add path rule")
            finally:
                os.close(parent_fd)
        if gpu:
            # The driver names descendant helper threads through procfs comm.
            # Existing procfs pseudo-files only: no create/delete rights.
            procfd=os.open('/proc',os.O_PATH|os.O_CLOEXEC)
            try:
                attr=PathRule((1<<1)|(1<<14),procfd)
                checked(libc.syscall(445,rules_fd,1,ctypes.byref(attr),0),'add CUDA procfs rule')
            finally:os.close(procfd)
            # CUDA needs writable character devices, never storage files/directories.
            for path in _gpu_device_paths():
                fd=os.open(path,os.O_PATH|os.O_CLOEXEC)
                try:
                    attr=PathRule((1<<1)|(1<<2),fd)
                    checked(libc.syscall(445,rules_fd,1,ctypes.byref(attr),0),'add GPU device rule')
                finally:os.close(fd)
        checked(libc.prctl(38, 1, 0, 0, 0), "no_new_privs")
        checked(libc.syscall(446, rules_fd, 0), "restrict self")
    finally:
        os.close(rules_fd)

    directory_denied = file_denied = False
    try:
        list(directory.iterdir())
    except PermissionError:
        directory_denied = True
    try:
        with probe.open("rb") as source:
            source.read(1)
    except PermissionError:
        file_denied = True
    if not directory_denied or not file_denied:
        raise RuntimeError("Isolation failed: compute can still read storage")
    for artifact in artifact_probes:
        try:
            with artifact.open('rb') as source:source.read(1)
            raise RuntimeError('Isolation failed: compute can read index artifact')
        except PermissionError:pass
    return {"event": "filesystem_isolation", "mechanism": "landlock", "abi": abi,
            "storage_directory": str(directory), "probe_file": str(probe),
            "directory_denied": directory_denied, "file_denied": file_denied,
            "artifact_probe_files":[str(path) for path in artifact_probes],"artifact_files_denied":len(artifact_probes)}
