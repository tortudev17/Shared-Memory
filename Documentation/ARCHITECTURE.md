# Architecture

## Process and data flow

```text
Application process A        Application process B
  SharedMemory façade          SharedMemory façade
          │                           │
          ├── request mailbox ────────┤
          │      one mmap region      │
          └── SPSC event ring ────────┘
                         │
             Swift daemon engine
            (first client's thread)
              allocator · files · UUIDs
              buckets · subscriptions
```

There is no socket, pipe, FIFO, or RPC transport. The only bootstrap mechanisms are a deterministic POSIX shared-memory name and a small non-payload launch lock. Requests, responses, events, serialized values, and all payload movement use the one mapped region.

## Region layout

The region begins with a versioned, cache-line-aligned control area:

```text
┌──────────────────────────────────────────────────────────┐
│ ABI header: magic, version, size, boot ID, PID, heartbeat│
├──────────────────────────────────────────────────────────┤
│ 64 client slots                                          │
│   state + generation + PID + name                        │
│   one request/response mailbox                           │
│   256-entry SPSC event ring                              │
├──────────────────────────────────────────────────────────┤
│ 64-byte-aligned arena                                    │
│   [block header | payload] [block header | payload] ...  │
└──────────────────────────────────────────────────────────┘
```

The ABI uses fixed-width C fields so Swift compiler layout changes cannot alter the on-memory protocol. Region validation checks magic, ABI version, mapping size, and heap bounds before a client claims a slot. Darwin-compatible names stay below its 31-byte POSIX limit.

## Synchronization

Each client owns one synchronous mailbox. A local `NSLock` serializes that client's public calls; C atomic release/acquire transitions publish complete requests and responses. The daemon is the only consumer and the only writer of logical runtime state, which removes allocator and filesystem lock contention entirely.

Daemon-to-client events use a bounded single-producer/single-consumer ring with monotonically increasing head and tail counters. The daemon never drops a receive item when the ring is full: the item remains in its bucket and the delivery cursor retries it. Notification events similarly wait in a bounded daemon backlog. One client executor consumes events and invokes user handlers serially.

The daemon adaptively spins under load and sleeps for 50 microseconds after an idle threshold. This avoids kernel IPC and preserves low wake latency without permanently busy-spinning every idle client.

## Allocator

Only the daemon mutates the arena. Blocks are aligned to 64 bytes and use 64-byte headers containing physical size, previous-block size, free-list links, logical payload size, reference count, state, and a boot-secret corruption cookie.

Free blocks are organized into logarithmic segregated bins. Allocation is a bin search plus split; release uses boundary tags to coalesce both neighbors in constant time. There is no compaction and no eviction. Fragmentation can therefore cause a correctly reported allocation failure even when aggregate free bytes exceed the requested size.

When a block's final reference is released, complete payload pages inside the coalesced free range
are marked reusable and marked active again before a later allocation writes them. Clients also
drop stale page residency after callback bursts. This keeps logical durability unchanged while
preventing acknowledged historical traffic from remaining active memory indefinitely.

References cover:

- one owner for a filesystem value or live item;
- a temporary read lease;
- a queued or executing receive event;
- batch items sharing one framed allocation.

A dead-client reap releases staging blocks and delivery leases. Allocator cookie failure permanently stops new allocations for that daemon lifetime rather than risk overlapping blocks.

## Codable format and copies

Values use Foundation's binary property-list encoder inside a stable keyed envelope. The envelope makes top-level scalars legal, and the binary format preserves `Data`, dates, UUID-backed Codable values, integers, and nested structures without JSON text or base64 payload expansion.

The generic `Codable` API necessarily creates a Swift encoded `Data` value. One copy places those bytes into a daemon-allocated staging block. From that point:

- reads decode a `Data(bytesNoCopy:)` view protected by a lease;
- receive callbacks borrow the mapped bytes directly;
- events and buckets contain offsets and lengths only;
- unchanged `pass`/`send` values are detected byte-for-byte and retain their old allocation;
- transformed values switch atomically to their new staging allocation.

Checkpointing intentionally copies bytes to a disk archive. No other transport serialization occurs after ingress.

## Filesystem consistency

The daemon keeps a normalized path index pointing at reference-counted arena blocks. Because it processes requests serially, write commit is one index replacement. Reads processed before that replacement lease the old block; reads processed after it lease the new block. The old block is reclaimed only after all leases end.

Directories are implicit prefixes, so nested writes do not allocate directory nodes. The checkpoint codec sorts paths for stable output and protects each payload plus the entire archive with CRC-32.

## Conveyor state

Daemon-private indexes provide fast UUID and path lookup; user payloads remain in shared memory. Every item record stores its UUID, owner/bucket, pipeline and stage indexes, payload allocation, intrusive bucket links, and last delivery generation. Intrusive links provide ordered append/removal without copying payloads.

A reconnect gets a fresh generation. Existing bucket items no longer match their recorded delivery generation and are delivered again in arrival order. Every `pass` value carries its delivered UUID, and the daemon validates all UUIDs, owners, and delivery generations before committing the batch. `finish` removes the record and owner reference. Moving an item updates its record and bucket links but never its UUID.

## Crash model

The daemon owns a heartbeat and PID in the header. A bootstrap lock guarantees one default daemon per host. On daemon restart, the stale shared-memory object is unlinked and a new boot ID, control area, and arena are created; all old RAM state is intentionally lost.

Client death does not affect bucket ownership. The daemon periodically uses `kill(pid, 0)` as lightweight liveness detection solely to reclaim connection slots, staging allocations, and event leases. It does not infer that unfinished work is complete.

An already constructed client does not migrate its mapping after daemon death. Operations fail, and constructing a replacement façade triggers stale-region replacement and registration.

## Limits

| Resource | Limit |
|---|---:|
| Conveyors | 3 |
| Connected clients | 64 |
| Events per client ring | 256 |
| Notification backlog per connection | 65,536 |
| Batch items | 65,536 |
| Client/stage name | 127 UTF-8 bytes |
| Filesystem/checkpoint path | 1,023 UTF-8 bytes |

These bounds keep the control layout fixed, cache-local, and allocation-free on the communication hot path.
