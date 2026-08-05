# SharedMemoryRuntime

`SharedMemoryRuntime` is a local, daemon-backed IPC runtime for macOS and Linux. Applications import one library product and instantiate one public class, `SharedMemory`. A single Swift daemon owns one POSIX shared-memory region, the allocator, in-memory filesystem, conveyor state, UUID registry, subscriptions, and synchronization.

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

The tests create isolated POSIX shared-memory objects and launch the daemon executable. A restrictive container must allow `shm_open` and shared mappings.

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
    if classifier?.pass([classified]) != true {
        // The item remains in classify's bucket and can be retried.
    }
}

let persistence = SharedMemory(name: "persist")
persistence.setReceiveHandler(for: Job.self) { [weak persistence] uuid, job in
    // Persist job, then explicitly retire its UUID.
    _ = persistence?.finish(uuid: uuid)
}

_ = decoder.pass([Job(imageID: "42", score: 0)])
```

The first value passed by `decode` receives a UUID and enters `classify`. At each receiving stage, `pass(_:)` matches values to that stage's oldest delivered, unfinished items. Matching items retain their UUID; surplus values become new items. `send(to:values:)` uses the same rule but bypasses conveyor order.

Filesystem and notifications use ordinary absolute Unix paths:

```swift
_ = decoder.write(path: "/cache/models/current", value: modelMetadata)
let metadata: ModelMetadata? = decoder.read(path: "/cache/models/current")

decoder.notificationHandler = { path in
    // Runs serially on the client's receive executor.
}
_ = decoder.notify(path: "/cache/models/current")

_ = decoder.checkpoint(path: "/var/backups/runtime.smr")
```

## Daemon discovery

The client checks the fixed shared-memory header and daemon PID. When no healthy daemon exists it launches `shared-memory-daemon`; a non-payload file lock makes concurrent launch attempts collapse to one process. The launcher searches:

1. `SMR_DAEMON_PATH`
2. beside the current executable and nearby SwiftPM build directories
3. `PATH`

Deploy the daemon beside the application, put it on `PATH`, or set `SMR_DAEMON_PATH` to an absolute executable path. Standard input, output, and error are connected to the null device. The runtime itself emits no logs.

The creator's conveyor configuration is installed idempotently through shared memory. This also handles a non-creator starting an empty daemon just before the creator. A conflicting second creator is rejected rather than mutating live pipelines.

## Semantics worth knowing

- All API calls are thread-safe and linearized by the daemon.
- A read sees either the complete old value or the complete new value, never a partial encoding.
- Missing files return `nil`. Parent directories are implicit.
- Capacity exhaustion returns `false`; data is never evicted.
- Receive callbacks are strictly serial and preserve bucket arrival order.
- Buckets and unfinished UUIDs survive client disconnects. Reconnection redelivers unfinished items.
- `finish(uuid:)` succeeds only for the current owning stage and permanently removes the item.
- The raw `receiveHandler` buffer is borrowed and valid only during the callback. Prefer `setReceiveHandler(for:_:)` for typed values.
- `mlock` is attempted for the whole region. The daemon continues when host policy or `RLIMIT_MEMLOCK` denies it.
- A daemon crash loses all runtime state by design. A newly constructed client replaces the stale region and starts a fresh daemon.

The default region is 256 MiB. A positive `memoryLimitGB` is honored when a creator starts a new daemon; `-1` selects the default. Non-creators ignore both conveyor and memory arguments.

## Documentation

- [Public API and behavioral contract](Documentation/API.md)
- [Architecture and memory model](Documentation/ARCHITECTURE.md)
- [Deployment and operations](Documentation/OPERATIONS.md)
- [Benchmarks](Documentation/BENCHMARKS.md)
- [Test coverage](Documentation/TESTING.md)
