# Benchmarks

The `shared-memory-benchmarks` executable measures the complete façade/daemon path plus isolated allocator and ring primitives. Build in release mode for decisions:

```sh
SMR_BENCHMARK_ITERATIONS=100000 swift run -c release shared-memory-benchmarks
```

If `SMR_INSTANCE_ID` is absent, the executable creates a PID-scoped benchmark daemon. `SMR_MEMORY_BYTES` defaults to 512 MiB for this executable. Metrics are emitted as stable comma-separated key/value records.

## Metrics

- `write`: encode, shared allocation, ingress copy, daemon commit, response.
- `read`: lookup, lease, borrowed decode, lease release.
- `pass`: encoding and ordered next-stage commit.
- `send`: encoding and direct-stage commit.
- `receive`: timestamp immediately before pass/send through typed callback entry.
- `shared_allocation`: daemon arena allocate plus release for a 256-byte payload.
- `spsc_queue`: one in-process shared-ring push/pop round trip.
- `throughput`: sequential write payload bytes/s and four-client daemon contention operations/s.
- `zero_copy`: committed-byte delta while an unchanged 1 MiB item moves stages.
- `memory`: shared committed arena bytes and process peak resident bytes.

Receive latency includes queueing when the benchmark intentionally submits faster than the callback drains. This is useful for throughput behavior; run a smaller iteration count for unloaded tail latency.

## Local release smoke result

On the development arm64 macOS host, a 1,000-iteration release build produced:

```text
write       avg 5.57 µs  p50 5.33 µs  p99 11.08 µs
read        avg 5.71 µs  p50 5.29 µs  p99 12.08 µs
pass        avg 8.35 µs  p50 6.50 µs  p99 17.67 µs
send        avg 7.34 µs  p50 6.46 µs  p99 19.00 µs
receive     avg 135.3 µs p50 178.7 µs p99 248.8 µs (queued load)
allocation  avg 114 ns
SPSC push+pop avg 13 ns
four-client contention 238,065 operations/s
zero-copy unchanged 1 MiB move: 0 committed-byte delta
```

These numbers verify the harness, not a service-level objective. CPU, Swift optimization mode, mapping lock success, payload schema, memory pressure, callback work, and iteration count materially change results. Keep raw output with any optimization claim and compare identical release builds on the deployment hardware.

## Optimization acceptance rule

Performance changes should land only when the correctness suite remains green and the relevant release metric improves across repeated runs. In particular, allocator changes must retain corruption/coalescing tests, and queue changes must retain acquire/release ordering and serial callback tests.
