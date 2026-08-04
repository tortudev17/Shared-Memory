# Test coverage

Run all tests with:

```sh
swift test
```

The suite uses a uniquely named 32 MiB integration region and terminates its daemon after the runtime test class.

| Required area | Coverage |
|---|---|
| Stress | 10,000 SPSC handoffs, 500 callback batch, 2,000 contended read/write calls |
| Corruption | ABI magic rejection, allocator cookie failure, truncated batch, checkpoint CRC failure |
| Synchronization | Multi-thread invariant reads during writes; mailbox publish ordering |
| Pipeline ordering | Three-stage typed transformation, serial ordered batch delivery |
| Filesystem | Nested implicit directories, normalization, missing reads, invalid paths |
| Checkpoint | Disk snapshot, archive parse, typed payload decode, checksum validation |
| Reconnect | Unfinished item redelivery and UUID preservation after disconnect |
| UUID tracking | Same UUID across three stages; owner finish; finished reclamation |
| Allocator | Split, reference counts, exhaustion, no eviction, bidirectional coalescing, corruption stop |
| Notifications | One callback for every committed exact-path change |
| Capacity | Oversized write returns `false` while an existing value remains readable |
| Zero-copy | Unchanged 1 MiB pass has zero committed-byte delta |

The runtime tests exercise the real daemon executable and POSIX shared memory rather than an in-process transport mock. Core tests isolate binary formats, ABI atomics, queue behavior, and allocator invariants for deterministic failure localization.
