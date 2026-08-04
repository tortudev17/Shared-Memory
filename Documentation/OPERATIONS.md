# Deployment and operations

## Installing the two products

Build release artifacts with:

```sh
swift build -c release
```

Link the `SharedMemoryRuntime` library product into the application and ship the `shared-memory-daemon` executable. Put the daemon beside the application executable, install it on `PATH`, or set:

```sh
export SMR_DAEMON_PATH=/absolute/path/to/shared-memory-daemon
```

The library automatically launches the daemon with its shared-memory name, lock path, memory size, and encoded conveyor bootstrap. The daemon calls `setsid`, nulls inherited standard streams through the launcher, and runs until `SIGINT` or `SIGTERM`.

## Startup order

Start the creator first when practical. If a non-creator wins the launch race, it starts an empty default-size daemon; the later creator installs its conveyors through an idempotent shared-memory command. Once live items exist, a conflicting configuration is rejected.

The default production namespace is one daemon per host. `SMR_INSTANCE_ID` exists for isolated tests and benchmarks; do not set it in production unless multiple deliberately isolated runtimes are desired.

The shared-memory object is mode `0600`. This gives one host daemon with same-user clients. A multi-user installation should run the application set under one service account or adjust the C creation policy after a security review; the default intentionally does not expose arbitrary Codable payloads across Unix users.

## Memory sizing and locking

The default size is 256 MiB. A creator may request positive whole GiB with `memoryLimitGB`. The mapping is sparse until pages are touched, but `mlock` can make the configured capacity count against resident-memory and locked-memory limits.

For deterministic locking on Linux, raise `LimitMEMLOCK` in systemd or the process's `RLIMIT_MEMLOCK`. On macOS, configure the launch service and host policy accordingly. Failure to lock is non-fatal because many desktop environments set a small default limit.

Sizing should include:

- the fixed control area (approximately 1.1 MiB with the current ABI);
- 64 bytes per allocated block;
- encoded filesystem values;
- live and unfinished conveyor values;
- temporary staging blocks during writes and transformed passes;
- concurrent read/event references, which delay reuse but do not duplicate blocks.

No object is silently evicted. Monitor `memoryUsed()` and apply backpressure when operations return `false`.

## Shutdown and crash recovery

Graceful `SIGTERM` or `SIGINT` clears the daemon PID and unlinks its POSIX shared-memory name. Existing mappings remain valid at the OS level but no longer receive responses. On a crash, the bootstrap lock is released by the kernel while the stale shared-memory name may remain. The next daemon unlinks that stale object before creating a new boot.

All in-memory filesystem and conveyor contents are lost on daemon restart by design. Use periodic `checkpoint(path:)` calls when disk recovery data is required. The project validates checkpoint integrity but deliberately has no automatic restore API; applications retain control over schema and recovery policy.

## Upgrades

The header contains a strict ABI version. A client refuses to attach to a different layout. Stop the old daemon before deploying a new ABI; the new daemon recreates the region. Do not attempt rolling mixed-ABI upgrades because there is intentionally only one host daemon.

## Failure diagnosis without logs

Runtime logging is intentionally absent. Diagnose in this order:

1. Verify `SharedMemory.isConnected` after initialization.
2. Verify `shared-memory-daemon` is executable and discoverable through the documented search order.
3. Confirm POSIX shared memory is allowed by the container or sandbox.
4. Check name/path limits and conveyor membership.
5. Check `memoryUsed()` and the configured capacity.
6. Run the focused test suite and release benchmark on the target host.

Boolean failures never mutate the target file or conveyor batch partially. Retrying after correcting capacity or connectivity is safe; application-level operations may still need their own idempotency rules.

## Linux notes

Linux normally backs POSIX objects with `/dev/shm`; make sure a container's `--shm-size` exceeds the configured region. The code uses no Darwin-only synchronization primitive. C atomics, `mmap`, `flock`, monotonic clocks, `mlock`, signals, and Foundation APIs are shared across supported platforms.
