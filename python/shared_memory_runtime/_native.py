"""Build and load the tiny C ABI bridge used by the Python client.

The Swift daemon owns the shared-memory region.  This module only compiles the
repository's C ABI shim into a cache when a Python process needs it.
"""

from __future__ import annotations

import ctypes
import hashlib
import os
import platform
import subprocess
import tempfile
from pathlib import Path


_ROOT = Path(__file__).resolve().parents[2]
_SOURCE = _ROOT / "Sources" / "CSharedMemory" / "CSharedMemory.c"
_HEADER = _ROOT / "Sources" / "CSharedMemory" / "include" / "CSharedMemory.h"


def library_path() -> Path:
    """Return a cached shared library built from the package's C ABI shim."""
    digest = hashlib.sha256(_SOURCE.read_bytes() + _HEADER.read_bytes()).hexdigest()[:16]
    suffix = ".dylib" if platform.system() == "Darwin" else ".so"
    cache = Path(tempfile.gettempdir()) / "shared-memory-runtime-python" / digest
    output = cache / f"libcsharedmemory{suffix}"
    if output.exists():
        return output

    cache.mkdir(parents=True, exist_ok=True)
    temporary = cache / f".libcsharedmemory-{os.getpid()}{suffix}"
    command = ["cc", "-std=c11", "-O2", "-fPIC"]
    if platform.system() == "Darwin":
        command.append("-dynamiclib")
    else:
        command.append("-shared")
    command += ["-I", str(_HEADER.parent), str(_SOURCE), "-o", str(temporary)]
    try:
        subprocess.run(command, check=True, capture_output=True, text=True)
        temporary.replace(output)
    except subprocess.CalledProcessError as error:
        temporary.unlink(missing_ok=True)
        raise RuntimeError(f"unable to build Shared Memory C ABI bridge: {error.stderr}") from error
    return output


def load() -> ctypes.CDLL:
    return ctypes.CDLL(str(library_path()), use_errno=True)
