# SharedMemoryRuntime

`SharedMemoryRuntime` is a local, daemon-backed IPC runtime for macOS and Linux. Applications import one library product and instantiate one public class, `SharedMemory`. A daemon engine inside that library owns one POSIX shared-memory region, the allocator, in-memory filesystem, conveyor state, UUID registry, subscriptions, and synchronization.

Payload bytes enter shared memory once. Filesystem reads borrow a stable snapshot while decoding, queue entries contain offsets rather than payloads, and an unchanged item passed between conveyor stages keeps the same allocation and UUID.

## Requirements

- Swift 6.2 or newer
- macOS 12 or newer, or a modern glibc-based Linux distribution
- POSIX shared memory (`shm_open`, `mmap`) and process signaling

The package has no third-party dependencies.

## Build and test

```sh
swift build
swift test
swift run -c release shared-memory-benchmarks
```

The tests create isolated POSIX shared-memory objects and run isolated daemon engines. A restrictive container must allow `shm_open` and shared mappings.

## Use

```swift
import SharedMemoryRuntime

struct Job: Codable {
    var imageID: String
    var score: Double
}

let decoder = SharedMemory(
    creator: true,
    name: "decode",
    conveyor: [["decode", "classify", "persist"]],
    memoryLimitGB: 8
)

let classifier = SharedMemory(name: "classify")
classifier.setReceiveHandler(for: Job.self) { [weak classifier] uuid, job in
    var classified = job
    classified.score = 0.99
    if classifier?.pass([(uuid: uuid, value: classified)]) != true {
        // The item remains in classify's bucket and can be retried.
    }
}

let persistence = SharedMemory(name: "persist")
persistence.setReceiveHandler(for: Job.self) { [weak persistence] uuid, job in
    // Persist job, then explicitly retire its UUID.
    _ = persistence?.finish(uuid: uuid)
}

_ = decoder.send(to: "classify", values: [Job(imageID: "42", score: 0)])
```

The first value sent to `classify` receives a UUID. At each receiving stage, `pass(_:)` requires one delivered UUID per value and advances those exact items atomically. Each value retains its paired UUID. `send(to:values:)` introduces work directly into a known stage and may also move delivered items oldest-first.

Filesystem and notifications use ordinary absolute Unix paths:

```swift
_ = decoder.write(path: "/cache/models/current", value: modelMetadata)
let metadata: ModelMetadata? = decoder.read(path: "/cache/models/current")
let versioned: SharedMemory.Versioned<ModelMetadata>? =
    decoder.readVersioned(path: "/cache/models/current")
let files = decoder.list(prefix: "/cache/models")

if let versioned,
   let replacement = SharedMemory.Mutation.write(
       path: "/cache/models/current",
       value: nextModelMetadata,
       expectedVersion: versioned.version) {
    _ = decoder.transaction([replacement])
}
_ = decoder.write(path: "/cache/models/obsolete", value: nil as ModelMetadata?)

decoder.notificationHandler = { path in
    // Runs serially on the client's receive executor.
}
_ = decoder.notify(path: "/cache/models/current")

_ = decoder.checkpoint(path: "/var/backups/runtime.smr")
```

## Daemon startup

The client checks the fixed shared-memory header and daemon PID. When no healthy daemon exists, the library starts its daemon engine on a dedicated background thread in the first client process. A non-payload file lock makes concurrent launch attempts collapse to one daemon. No helper executable, environment variable, or additional package product is required.

The creator's conveyor configuration is installed idempotently through shared memory. This also handles a non-creator starting an empty daemon just before the creator. A conflicting second creator is rejected rather than mutating live pipelines.

For services that need a persistent filesystem without a conveyor, the package
also builds `shared-memory-host`. It starts with no conveyor configuration and
defaults to an 8 GiB region:

```sh
swift run -c release shared-memory-host
# Isolated tests may use: --memory-bytes 67108864
```

`shared-memory-tool` is a small filesystem interop executable used by the
cross-language test suite.

## Semantics worth knowing

- All API calls are thread-safe and linearized by the daemon.
- A read sees either the complete old value or the complete new value, never a partial encoding.
- Versions change monotonically with committed writes. Prefix listings are sorted, and conditional deletes/transactions fail without partial mutation. Transaction expected version zero means the path must be absent.
- Missing files return `nil`. Parent directories are implicit.
- Capacity exhaustion returns `false`; data is never evicted.
- Receive callbacks are strictly serial and preserve bucket arrival order.
- Buckets and unfinished UUIDs survive client disconnects. Reconnection redelivers unfinished items.
- `finish(uuid:)` succeeds only for the current owning stage and permanently removes the item.
- The raw `receiveHandler` buffer is borrowed and valid only during the callback. Prefer `setReceiveHandler(for:_:)` for typed values.
- `mlock` is attempted only for the fixed control plane. The payload arena stays sparse and pageable, so configured capacity is not forced resident.
- Exiting the process that hosts the daemon loses all runtime state by design. A newly constructed client in another process replaces the stale region and starts a fresh daemon.

The default region is 256 MiB. A positive `memoryLimitGB` is honored when a creator starts a new daemon; `-1` selects the default. Non-creators ignore both conveyor and memory arguments.

## Documentation

- [Public API and behavioral contract](Documentation/API.md)
- [Architecture and memory model](Documentation/ARCHITECTURE.md)
- [Deployment and operations](Documentation/OPERATIONS.md)
- [Benchmarks](Documentation/BENCHMARKS.md)
- [Test coverage](Documentation/TESTING.md)
