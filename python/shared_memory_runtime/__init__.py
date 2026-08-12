"""Filesystem-style Python client for ``SharedMemoryRuntime``.

The native Swift daemon remains the owner of the POSIX shared-memory region.
This package exposes its atomic filesystem, listing, and checkpoint APIs to
Python; it intentionally does not expose conveyor operations.
"""

from __future__ import annotations

import contextlib
import ctypes
import errno
import os
import plistlib
import random
import signal
import subprocess
import threading
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Iterator, Literal, Mapping, Sequence

from ._native import load as _load_native


DEFAULT_REGION_BYTES = 256 << 20
DEFAULT_TIMEOUT = 30.0
CHECKPOINT_TIMEOUT = 600.0
MAX_CLIENTS = 64
MAX_NAME_BYTES = 127
MAX_PATH_BYTES = 1023
_ROOT = Path(__file__).resolve().parents[2]


class SharedMemoryError(RuntimeError):
    """Base exception raised for protocol or daemon failures."""


class ConnectionError(SharedMemoryError):
    """The daemon or client slot is unavailable."""


class OperationError(SharedMemoryError):
    """The daemon rejected an otherwise valid operation."""


class _Mapping(ctypes.Structure):
    _fields_ = [("address", ctypes.c_void_p), ("size", ctypes.c_uint64), ("fd", ctypes.c_int32), ("reserved", ctypes.c_int32)]


class _Response(ctypes.Structure):
    _fields_ = [("sequence", ctypes.c_uint64), ("status", ctypes.c_int64), ("value0", ctypes.c_uint64), ("value1", ctypes.c_uint64), ("value2", ctypes.c_uint64), ("value3", ctypes.c_uint64)]


@dataclass(frozen=True)
class VersionedValue:
    value: Any
    version: int


@dataclass(frozen=True)
class PathEntry:
    path: str
    version: int


@dataclass(frozen=True)
class Mutation:
    """A write or delete applied as part of an all-or-nothing transaction."""

    kind: Literal["write", "delete"]
    path: str
    value: Any = None
    expected_version: int | None = None

    @classmethod
    def write(cls, path: str, value: Any, expected_version: int | None = None) -> "Mutation":
        return cls("write", path, value, expected_version)

    @classmethod
    def delete(cls, path: str, expected_version: int | None = None) -> "Mutation":
        return cls("delete", path, None, expected_version)


class _Commands:
    REGISTER, UNREGISTER, ALLOCATE, ABANDON, WRITE, READ = range(1, 7)
    MEMORY_USED, RELEASE_LEASE, CHECKPOINT, PING, LIST, DELETE, TRANSACTION = 10, 12, 13, 14, 16, 17, 18


_LIB: ctypes.CDLL | None = None
_LIB_LOCK = threading.Lock()
_DAEMONS: dict[str, subprocess.Popen[bytes]] = {}


def _library() -> ctypes.CDLL:
    global _LIB
    with _LIB_LOCK:
        if _LIB is not None:
            return _LIB
        library = _load_native()
        library.smr_region_open.argtypes = [ctypes.c_char_p, ctypes.POINTER(_Mapping)]
        library.smr_region_open.restype = ctypes.c_int
        library.smr_mapping_close.argtypes = [ctypes.POINTER(_Mapping)]
        library.smr_region_unlink.argtypes = [ctypes.c_char_p]
        library.smr_region_unlink.restype = ctypes.c_int
        library.smr_region_validate.argtypes = [ctypes.c_void_p, ctypes.c_uint64]
        library.smr_region_validate.restype = ctypes.c_int
        library.smr_daemon_pid.argtypes = [ctypes.c_void_p]
        library.smr_daemon_pid.restype = ctypes.c_int32
        library.smr_daemon_heartbeat.argtypes = [ctypes.c_void_p]
        library.smr_daemon_heartbeat.restype = ctypes.c_uint64
        library.smr_monotonic_nanoseconds.restype = ctypes.c_uint64
        library.smr_pid_is_alive.argtypes = [ctypes.c_int32]
        library.smr_pid_is_alive.restype = ctypes.c_int
        library.smr_client_slot.argtypes = [ctypes.c_void_p, ctypes.c_uint32]
        library.smr_client_slot.restype = ctypes.c_void_p
        library.smr_slot_claim.argtypes = [ctypes.c_void_p]
        library.smr_slot_claim.restype = ctypes.c_int
        library.smr_slot_prepare.argtypes = [ctypes.c_void_p, ctypes.c_int32, ctypes.c_uint64, ctypes.c_char_p]
        library.smr_slot_prepare.restype = ctypes.c_int
        library.smr_slot_reset.argtypes = [ctypes.c_void_p]
        library.smr_client_submit.argtypes = [ctypes.c_void_p, ctypes.c_uint64, ctypes.c_uint32, ctypes.c_uint32, ctypes.c_uint64, ctypes.c_uint64, ctypes.c_uint64, ctypes.c_uint64, ctypes.c_uint64, ctypes.c_uint64, ctypes.c_char_p, ctypes.c_char_p]
        library.smr_client_submit.restype = ctypes.c_int
        library.smr_client_try_response.argtypes = [ctypes.c_void_p, ctypes.c_uint64, ctypes.POINTER(_Response)]
        library.smr_client_try_response.restype = ctypes.c_int
        library.smr_client_ack_response.argtypes = [ctypes.c_void_p]
        library.smr_cpu_relax.argtypes = []
        library.smr_sleep_nanoseconds.argtypes = [ctypes.c_uint64]
        _LIB = library
        return library


def _fnv1a(value: str) -> int:
    result = 14_695_981_039_346_656_037
    for byte in value.encode():
        result = ((result ^ byte) * 1_099_511_628_211) & 0xFFFF_FFFF_FFFF_FFFF
    return result


def region_name(instance_id: str | None = None) -> str:
    instance = os.environ.get("SMR_INSTANCE_ID", "") if instance_id is None else instance_id
    return "/smr_runtime_v1" if not instance else f"/smr_v1_{_fnv1a(instance):x}"


def normalize_path(path: str, *, allow_root: bool = False) -> str:
    if not isinstance(path, str) or not path.startswith("/") or "\0" in path:
        raise ValueError("paths must be absolute and contain no NUL")
    parts: list[str] = []
    for part in path.split("/"):
        if not part or part == ".":
            continue
        if part == "..":
            if not parts:
                raise ValueError("path escapes the Shared Memory root")
            parts.pop()
        else:
            parts.append(part)
    result = "/" + "/".join(parts)
    if len(result.encode()) > MAX_PATH_BYTES:
        raise ValueError("path is too long")
    if result == "/" and not allow_root:
        raise ValueError("the root is not a value path")
    return result


def _plist_value(value: Any) -> Any:
    if isinstance(value, (str, bytes, bool, int, float)):
        return value
    if isinstance(value, bytearray):
        return bytes(value)
    if isinstance(value, (list, tuple)):
        return [_plist_value(item) for item in value]
    if isinstance(value, Mapping) and all(isinstance(key, str) for key in value):
        return {key: _plist_value(item) for key, item in value.items()}
    raise TypeError(f"unsupported property-list value: {type(value).__name__}")


def encode_value(value: Any) -> bytes:
    return plistlib.dumps({"value": _plist_value(value)}, fmt=plistlib.FMT_BINARY, sort_keys=True)


def decode_value(data: bytes) -> Any:
    envelope = plistlib.loads(data)
    if not isinstance(envelope, dict) or set(envelope) != {"value"}:
        raise ValueError("invalid Shared Memory property-list envelope")
    return envelope["value"]


def _host_executable() -> Path:
    executable = _ROOT / ".build" / "release" / "shared-memory-host"
    if not executable.exists():
        subprocess.run(["swift", "build", "-c", "release", "--product", "shared-memory-host"], cwd=_ROOT, check=True)
    return executable


def _healthy(instance_id: str | None) -> bool:
    mapping, library = _Mapping(), _library()
    if library.smr_region_open(region_name(instance_id).encode(), ctypes.byref(mapping)) != 0:
        return False
    try:
        pid = library.smr_daemon_pid(mapping.address)
        return library.smr_region_validate(mapping.address, mapping.size) == 1 and pid > 0 and library.smr_pid_is_alive(pid) == 1
    finally:
        library.smr_mapping_close(ctypes.byref(mapping))


def start_daemon(*, instance_id: str | None = None, memory_bytes: int = DEFAULT_REGION_BYTES, timeout: float = DEFAULT_TIMEOUT) -> int:
    """Build (when needed) and start the filesystem-only Swift host."""
    if memory_bytes <= 0:
        raise ValueError("memory_bytes must be positive")
    if _healthy(instance_id):
        return SharedMemory.peek_daemon_pid(instance_id)
    environment = os.environ.copy()
    if instance_id is None:
        environment.pop("SMR_INSTANCE_ID", None)
    else:
        environment["SMR_INSTANCE_ID"] = instance_id
    process = subprocess.Popen([str(_host_executable()), "--memory-bytes", str(memory_bytes)], cwd=_ROOT, env=environment, stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, start_new_session=True)
    _DAEMONS[instance_id or ""] = process
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if _healthy(instance_id):
            return SharedMemory.peek_daemon_pid(instance_id)
        if process.poll() is not None:
            raise ConnectionError(f"Swift host exited with {process.returncode}")
        time.sleep(0.02)
    process.terminate()
    raise TimeoutError("timed out waiting for the Swift Shared Memory daemon")


def destroy_daemon(instance_id: str, *, timeout: float = 5.0) -> None:
    """Stop and unlink an explicitly named, isolated daemon."""
    if not instance_id:
        raise ValueError("destroy_daemon requires a non-empty instance_id")
    pid = SharedMemory.peek_daemon_pid(instance_id)
    if pid > 1:
        try: os.kill(pid, signal.SIGTERM)
        except ProcessLookupError: pass
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline and _library().smr_pid_is_alive(pid): time.sleep(0.02)
    result = _library().smr_region_unlink(region_name(instance_id).encode())
    if result != 0 and ctypes.get_errno() != errno.ENOENT:
        raise OSError(ctypes.get_errno(), "failed to unlink Shared Memory region")


class SharedMemory:
    """Synchronous filesystem client with one native request slot."""

    def __init__(self, *, instance_id: str | None = None, name: str | None = None, timeout: float = DEFAULT_TIMEOUT) -> None:
        self.instance_id, self.timeout, self._library = instance_id, timeout, _library()
        self._mapping, self._slot, self._closed = _Mapping(), None, False
        self._sequence, self._lock = random.randrange(1, 1 << 63), threading.RLock()
        encoded_name = (name or f"python-{os.getpid()}-{uuid.uuid4().hex[:12]}").encode()
        if not encoded_name or len(encoded_name) > MAX_NAME_BYTES or b"\0" in encoded_name:
            raise ValueError("invalid client name")
        if self._library.smr_region_open(region_name(instance_id).encode(), ctypes.byref(self._mapping)) != 0:
            raise ConnectionError("the Swift Shared Memory daemon is not running")
        try:
            if self._library.smr_region_validate(self._mapping.address, self._mapping.size) != 1:
                raise ConnectionError("Shared Memory region has an incompatible ABI")
            for index in range(MAX_CLIENTS):
                slot = self._library.smr_client_slot(self._mapping.address, index)
                if slot and self._library.smr_slot_claim(slot) == 1:
                    self._slot = slot; break
            if self._slot is None or self._library.smr_slot_prepare(self._slot, self._library.smr_current_pid(), random.randrange(1, 1 << 64), encoded_name) != 1:
                raise ConnectionError("no Shared Memory client slot is available")
            if self._request(_Commands.REGISTER).status != 1:
                raise ConnectionError("the daemon rejected client registration")
        except BaseException:
            self._cleanup(); raise

    @staticmethod
    def peek_daemon_pid(instance_id: str | None = None) -> int:
        mapping, library = _Mapping(), _library()
        if library.smr_region_open(region_name(instance_id).encode(), ctypes.byref(mapping)) != 0: return 0
        try: return int(library.smr_daemon_pid(mapping.address)) if library.smr_region_validate(mapping.address, mapping.size) else 0
        finally: library.smr_mapping_close(ctypes.byref(mapping))

    @property
    def daemon_pid(self) -> int:
        self._require_open(); return int(self._library.smr_daemon_pid(self._mapping.address))

    def _require_open(self) -> None:
        if self._closed or self._slot is None: raise ConnectionError("Shared Memory client is closed")

    def _request(self, opcode: int, *, flags: int = 0, args: Sequence[int] = (), path: str | None = None, timeout: float | None = None, allow_closed: bool = False) -> _Response:
        with self._lock:
            if not allow_closed: self._require_open()
            if self._slot is None: raise ConnectionError("Shared Memory slot is unavailable")
            values = list(args) + [0] * (6 - len(args))
            if len(values) != 6: raise ValueError("at most six request arguments are supported")
            self._sequence = (self._sequence + 1) & 0xFFFF_FFFF_FFFF_FFFF
            if self._library.smr_client_submit(self._slot, self._sequence, opcode, flags, *values, path.encode() if path else None, None) != 1:
                raise ConnectionError("failed to submit request")
            deadline, spins = time.monotonic() + (self.timeout if timeout is None else timeout), 0
            while time.monotonic() < deadline:
                response = _Response()
                result = self._library.smr_client_try_response(
                    self._slot, self._sequence, ctypes.byref(response)
                )
                if result == 1:
                    self._library.smr_client_ack_response(self._slot); return response
                if result < 0: self._library.smr_client_ack_response(self._slot); raise ConnectionError("mismatched daemon response")
                if self._library.smr_pid_is_alive(self._library.smr_daemon_pid(self._mapping.address)) != 1: raise ConnectionError("Swift daemon stopped")
                if spins < 10_000: self._library.smr_cpu_relax(); spins += 1
                else: self._library.smr_sleep_nanoseconds(50_000)
            raise TimeoutError(f"opcode {opcode} timed out")

    def _stage(self, data: bytes) -> int:
        response = self._request(_Commands.ALLOCATE, args=(len(data),))
        if response.status != 1 or response.value1 + len(data) > self._mapping.size: raise OperationError("cannot allocate Shared Memory storage")
        if data: ctypes.memmove(self._mapping.address + response.value1, data, len(data))
        return int(response.value0)

    def _release(self, block: int) -> None:
        if self._request(_Commands.RELEASE_LEASE, args=(block,)).status != 1: raise ConnectionError("failed to release read lease")

    def ping(self) -> int:
        response = self._request(_Commands.PING)
        if response.status != 1: raise OperationError("ping failed")
        return int(response.value0)

    def write(self, path: str, value: Any) -> bool:
        data, block = encode_value(value), None
        try:
            block = self._stage(data)
            if self._request(_Commands.WRITE, args=(block, len(data)), path=normalize_path(path)).status != 1: return False
            block = None; return True
        finally:
            if block is not None: self._request(_Commands.ABANDON, args=(block,))

    def read_versioned(self, path: str) -> VersionedValue | None:
        response = self._request(_Commands.READ, path=normalize_path(path))
        if response.status != 1: return None
        block, offset, count = map(int, (response.value0, response.value1, response.value2))
        try:
            if offset + count > self._mapping.size: raise ConnectionError("daemon returned out-of-range lease")
            return VersionedValue(decode_value(ctypes.string_at(self._mapping.address + offset, count)), int(response.value3))
        finally: self._release(block)

    def read(self, path: str) -> Any | None:
        result = self.read_versioned(path); return None if result is None else result.value

    def exists(self, path: str) -> bool: return self.read_versioned(path) is not None

    def list(self, prefix: str = "/") -> list[PathEntry]:
        response = self._request(_Commands.LIST, path=normalize_path(prefix, allow_root=True))
        if response.status != 1: raise OperationError("list failed")
        block, offset, count = map(int, (response.value0, response.value1, response.value2))
        try:
            entries = decode_value(ctypes.string_at(self._mapping.address + offset, count))
            return [PathEntry(str(item["path"]), int(item["version"])) for item in entries]
        finally: self._release(block)

    def delete(self, path: str, expected_version: int | None = None) -> bool:
        if expected_version is not None and (isinstance(expected_version, bool) or expected_version < 0): raise ValueError("expected_version must be a non-negative integer")
        return self._request(_Commands.DELETE, flags=int(expected_version is not None), args=(expected_version or 0,), path=normalize_path(path)).status == 1

    def transaction(self, mutations: Iterable[Mutation]) -> bool:
        prepared, blocks, descriptor = [], [], None
        seen: set[str] = set()
        try:
            for mutation in mutations:
                if not isinstance(mutation, Mutation) or mutation.kind not in {"write", "delete"}: raise TypeError("transactions accept write/delete Mutation objects")
                path = normalize_path(mutation.path)
                if path in seen: raise ValueError("a transaction cannot mutate a path twice")
                seen.add(path); item: dict[str, Any] = {"kind": mutation.kind, "path": path}
                if mutation.expected_version is not None: item["expectedVersion"] = mutation.expected_version
                if mutation.kind == "write":
                    data, block = encode_value(mutation.value), None
                    block = self._stage(data); blocks.append(block); item.update(stagedBlock=block, payloadSize=len(data))
                prepared.append(item)
            if not prepared: raise ValueError("a transaction must contain at least one mutation")
            data, descriptor = encode_value({"mutations": prepared}), None
            descriptor = self._stage(data)
            return self._request(_Commands.TRANSACTION, args=(descriptor, len(data))).status == 1
        finally:
            for block in blocks + ([descriptor] if descriptor is not None else []):
                try: self._request(_Commands.ABANDON, args=(block,))
                except SharedMemoryError: pass

    def memory_used(self) -> int:
        response = self._request(_Commands.MEMORY_USED)
        if response.status != 1: raise OperationError("memory_used failed")
        return int(response.value0)

    def checkpoint(self, path: str | os.PathLike[str]) -> None:
        target = str(Path(path).expanduser().resolve())
        if "\0" in target or len(target.encode()) > MAX_PATH_BYTES: raise ValueError("invalid checkpoint path")
        if self._request(_Commands.CHECKPOINT, path=target, timeout=CHECKPOINT_TIMEOUT).status != 1: raise OperationError("checkpoint failed")

    def close(self) -> None:
        with self._lock:
            if self._closed: return
            self._closed = True
            try:
                if self._slot is not None: self._request(_Commands.UNREGISTER, allow_closed=True)
            except SharedMemoryError: pass
            self._cleanup()

    def _cleanup(self) -> None:
        if self._slot is not None: self._library.smr_slot_reset(self._slot); self._slot = None
        if self._mapping.address: self._library.smr_mapping_close(ctypes.byref(self._mapping))

    def __enter__(self) -> "SharedMemory": return self
    def __exit__(self, *_: object) -> None: self.close()


__all__ = ["ConnectionError", "Mutation", "OperationError", "PathEntry", "SharedMemory", "SharedMemoryError", "VersionedValue", "decode_value", "destroy_daemon", "encode_value", "normalize_path", "region_name", "start_daemon"]
