# Public API

The `SharedMemoryRuntime` product exports one public type: `SharedMemory`. Internal targets are not products and use Swift package access control.

## Initialization

```swift
public init(
    creator: Bool = false,
    name: String,
    conveyor: [[String]]? = nil,
    memoryLimitGB: Int = -1
)
```

`name` is unique among connected clients and is limited to 127 UTF-8 bytes. An empty, duplicate, or overlong name produces a disconnected instance; inspect `isConnected`. Initialization is intentionally non-throwing to match the requested façade.

A creator may install zero to three conveyors. Every conveyor is a non-empty ordered list, stage names are globally unique, and a stage name is also the client name that owns that bucket. Non-creators ignore `conveyor` and `memoryLimitGB`.

## Filesystem

```swift
func write<T: Codable>(path: String, value: T) -> Bool
func read<T: Codable>(path: String) -> T?
func checkpoint(path: String) -> Bool
```

Paths must be absolute UTF-8 Unix paths of at most 1,023 bytes. Repeated separators and `.` are normalized. `..` may move toward the root but cannot escape it. The root itself is not a file.

`write` is copy-on-commit: serialization and staging complete before the daemon replaces the file record. An old payload remains valid while any read lease exists. Missing parent directories are implicit. `read` returns `nil` for a missing path, invalid path, daemon failure, type mismatch, or corrupt encoding.

`checkpoint` writes a sorted, versioned binary archive with per-value and whole-archive CRC-32 checksums. It creates missing disk parent directories and uses an atomic file replacement. Checkpointing is synchronous and temporarily pauses other daemon commands; it is the only operation designed to copy every filesystem payload out of shared memory.

## Conveyors and UUIDs

```swift
func pass<T: Codable>(_ items: [(uuid: UUID, value: T)]) -> Bool
func send<T: Codable>(to: String, values: [T]) -> Bool
func finish(uuid: UUID) -> Bool
```

For `pass`, every value is paired with the UUID of an unfinished item delivered to the current connection. UUIDs must be unique within the batch, and the daemon validates the entire batch before moving anything. For `send`, values are matched positionally to the caller's oldest delivered, unfinished bucket items:

- A matched value updates and moves the existing item, retaining its UUID.
- An identical serialized value reuses the existing shared allocation.
- A changed value replaces the payload only after its new encoding is committed.
- A surplus `send` value creates a new item and UUID.
- Unmatched existing items stay in the caller's bucket.

`pass` atomically places results in the next stage. It returns `false` if any UUID is duplicated, unknown, foreign, or not yet delivered; for a client outside a conveyor; at a final stage; for invalid data; or with insufficient memory. `send` atomically places results in a known stage's bucket and returns `false` for an unknown target. Empty batches succeed when the route is valid and perform no mutation.

`finish` is explicit acknowledgement and reclamation. Only the current owner can finish an item. Event and read leases may delay physical block reclamation for a few microseconds after logical finish.

## Receive handling

```swift
var receiveHandler: ((UUID, UnsafeRawBufferPointer) -> Void)?

func setReceiveHandler<T: Codable>(
    for type: T.Type = T.self,
    _ handler: @escaping (UUID, T) -> Void
)
```

Assigning either form replaces the previous receive handler. One dedicated client executor drains the SPSC event ring, so handlers never overlap and bucket arrival order is preserved. A handler may call back into the same `SharedMemory` instance.

The raw pointer is a zero-copy borrowed view into the mapped region. It must not escape the callback. The typed helper decodes while the daemon-held delivery lease is active. Decode failure drops that callback invocation but does not finish or move the item; reconnecting or replacing the client allows retry.

## Notifications

```swift
var notificationHandler: ((String) -> Void)?

@discardableResult
func notify(path: String) -> Bool
```

`notify` subscribes the current connection to exact changes at one normalized filesystem path. Multiple clients and multiple subscriptions are supported. Every successfully committed change adds a notification. A slow subscriber has a bounded 65,536-entry daemon backlog; writes return `false` before commit rather than drop notifications if that backlog is exhausted.

Subscriptions are connection-scoped and must be reinstalled after constructing a replacement client. Notification and receive callbacks share the same serial executor.

## Memory and status

```swift
var isConnected: Bool { get }
func memoryUsed() -> UInt64
func disconnect()
```

`memoryUsed` reports committed arena block bytes, including 64-byte allocation headers. It does not include the fixed ABI control area or small daemon-private indexes. A failed query returns zero.

`disconnect` synchronously unregisters the client and stops its callback executor. It never finishes bucket items; constructing a new client with the same name redelivers them. Deinitialization calls `disconnect` automatically, but explicit use is preferable when reconnect timing matters.

All mutating methods return `false` for disconnected clients, serialization failure, invalid input, rejected ownership, full memory, or daemon failure. The no-logging design deliberately leaves detailed diagnostics to tests and application-level policy.
