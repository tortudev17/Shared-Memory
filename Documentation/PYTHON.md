# Python client

`python/shared_memory_runtime` is a filesystem-only client for the Swift
`SharedMemoryRuntime` daemon. It uses the repository's C ABI shim to attach to
the same local POSIX shared-memory region. Conveyors, buckets, and receive
handlers remain Swift-only by design.

## Requirements

- Python 3.10+
- A C compiler (`cc`) to build the small ABI shim into the system temporary
  directory on first use
- Swift 6.2+ when starting a host from Python

The package has no Python runtime dependencies. From the repository root,
install it in editable mode with:

```sh
python3 -m pip install -e python
```

## Start and connect

Use `start_daemon` for a filesystem-only host. The initial call builds the
release host if it does not already exist. Alternatively, start
`shared-memory-host` yourself and construct `SharedMemory` to connect to it.

```python
from shared_memory_runtime import Mutation, SharedMemory, start_daemon

start_daemon(instance_id="models", memory_bytes=256 << 20)

with SharedMemory(instance_id="models") as memory:
    memory.write("/models/current", {"revision": 1, "enabled": True})

    current = memory.read_versioned("/models/current")
    assert current is not None

    # Publish only if no other client changed this file after the read.
    memory.transaction([
        Mutation.write(
            "/models/current",
            {"revision": current.value["revision"] + 1, "enabled": True},
            expected_version=current.version,
        )
    ])
```

`SharedMemory` does not start a daemon implicitly. This keeps the client safe
to use from a process that should only attach to an already-managed runtime.

## Filesystem semantics

- Values must be property-list-compatible: strings, bytes, booleans, numbers,
  lists/tuples, and dictionaries with string keys.
- Paths are absolute Unix paths and are normalized before requests are sent.
- `write` atomically replaces one value; `read` returns a complete snapshot or
  `None` for a missing path.
- `read_versioned` returns `VersionedValue(value, version)` for compare-and-swap
  writes and deletes.
- `list(prefix)` returns sorted `PathEntry(path, version)` values for the exact
  prefix and its descendants.
- `transaction` applies all supplied `Mutation.write` and `Mutation.delete`
  operations atomically. An expected version of `0` requires an absent path.
- `checkpoint(path)` writes a CRC-protected archive through the daemon.

Call `close()` when a client is no longer needed, or use it as a context
manager as in the example. For isolated tests, `destroy_daemon(instance_id)`
stops and unlinks a named instance; it intentionally rejects the default
instance.

## Interoperability

Python values use the same binary property-list envelope as Swift's `Codable`
filesystem operations. This makes primitive property-list values portable in
both directions. Swift-specific `Codable` models should use an explicit
cross-language schema rather than relying on implementation details of a
compiler-synthesized encoding.
