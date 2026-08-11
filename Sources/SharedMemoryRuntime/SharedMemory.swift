import Foundation
import SharedMemoryCore

/// A high-performance, local IPC runtime backed by one daemon-owned shared-memory region.
///
/// `SharedMemory` is the package's only public type. Each logical payload version crosses the
/// serialization boundary once, then remains in shared memory while it moves through stages.
public final class SharedMemory: @unchecked Sendable {
  /// A filesystem value paired with the version used by conditional mutations.
  public struct Versioned<Value> {
    public let value: Value
    public let version: UInt64

    fileprivate init(value: Value, version: UInt64) {
      self.value = value
      self.version = version
    }
  }

  /// A normalized filesystem path and its current version.
  public struct PathEntry: Equatable, Sendable {
    public let path: String
    public let version: UInt64

    fileprivate init(_ value: FilesystemPathEntry) {
      path = value.path
      version = value.version
    }
  }

  /// One write or delete in an atomic filesystem transaction.
  public struct Mutation: Sendable {
    fileprivate let value: FilesystemMutation

    /// Encodes a value for an atomic write. Zero means the path must be absent.
    public static func write<T: Codable>(
      path: String, value: T, expectedVersion: UInt64? = nil
    ) -> Mutation? {
      guard let normalized = RuntimeValidation.normalize(path: path), normalized != "/",
        let payload = try? BinaryCodable.encode(value)
      else { return nil }
      return Mutation(
        value: FilesystemMutation(
          kind: .write, path: normalized, expectedVersion: expectedVersion, payload: payload))
    }

    /// Creates a conditional or unconditional delete mutation.
    public static func delete(path: String, expectedVersion: UInt64? = nil) -> Mutation {
      Mutation(
        value: FilesystemMutation(
          kind: .delete, path: path, expectedVersion: expectedVersion))
    }
  }

  private let handlers: RuntimeHandlers
  private let client: RuntimeClient?

  /// Called serially, in bucket arrival order, whenever this client's stage receives an item.
  /// The buffer is a borrowed, zero-copy view and is valid only for the duration of the call.
  public var receiveHandler: ((UUID, UnsafeRawBufferPointer) -> Void)? {
    get { handlers.receive }
    set { handlers.receive = newValue }
  }

  /// Called serially when a subscribed in-memory filesystem path changes.
  public var notificationHandler: ((String) -> Void)? {
    get { handlers.notification }
    set { handlers.notification = newValue }
  }

  /// Whether this instance registered successfully with the daemon.
  public var isConnected: Bool { client?.isConnected == true }

  /// Creates or attaches a client. The first creator defines memory and up to three conveyors.
  /// Duplicate client names leave the instance disconnected.
  public init(
    creator: Bool = false,
    name: String,
    conveyor: [[String]]? = nil,
    memoryLimitGB: Int = -1
  ) {
    let handlers = RuntimeHandlers()
    self.handlers = handlers
    client = RuntimeClient(
      creator: creator,
      name: name,
      conveyor: conveyor,
      memoryLimitGB: memoryLimitGB,
      handlers: handlers
    )
  }

  deinit {
    client?.close()
  }

  /// Installs a typed receive handler. Replaces any previously installed receive handler.
  public func setReceiveHandler<T: Codable>(
    for type: T.Type = T.self,
    _ handler: @escaping (UUID, T) -> Void
  ) {
    handlers.receive = { uuid, bytes in
      guard let value = try? BinaryCodable.decode(type, from: bytes) else { return }
      handler(uuid, value)
    }
  }

  /// Installs a typed handler that automatically finishes each successfully decoded item.
  /// Use this for terminal consumers; use `setReceiveHandler` when passing or sending onward.
  public func setConsumingReceiveHandler<T: Codable>(
    for type: T.Type = T.self,
    _ handler: @escaping (UUID, T) -> Void
  ) {
    handlers.receive = { [weak client] uuid, bytes in
      guard let value = try? BinaryCodable.decode(type, from: bytes) else { return }
      handler(uuid, value)
      _ = client?.finish(uuid: uuid)
    }
  }

  /// Atomically creates or replaces a file, or removes it when `value` is `nil`.
  public func write<T: Codable>(path: String, value: T?) -> Bool {
    guard let value else {
      return client?.delete(path: path, expectedVersion: nil) == true
    }
    guard let data = try? BinaryCodable.encode(value) else { return false }
    return client?.write(path: path, data: data) == true
  }

  /// Removes a file when it exists.
  ///
  /// This overload lets callers delete with `write(path: value: nil)` without
  /// providing a type. It returns `false` when the path is absent.
  public func write(path: String, value: Never?) -> Bool {
    client?.delete(path: path, expectedVersion: nil) == true
  }

  /// Reads and decodes one complete snapshot, or returns `nil` when the path is absent or invalid.
  public func read<T: Codable>(path: String) -> T? {
    guard
      let result = client?.withRead(
        path: path,
        { bytes in
          try? BinaryCodable.decode(T.self, from: bytes)
        })
    else { return nil }
    return result
  }

  /// Reads and decodes one snapshot together with its current version.
  public func readVersioned<T: Codable>(path: String) -> Versioned<T>? {
    client?.withVersionedRead(path: path) { bytes, version in
      guard let value = try? BinaryCodable.decode(T.self, from: bytes) else { return nil }
      return Versioned(value: value, version: version)
    } ?? nil
  }

  /// Lists exact and descendant paths under a normalized prefix.
  public func list(prefix: String = "/") -> [PathEntry]? {
    client?.list(prefix: prefix)?.map(PathEntry.init)
  }

  /// Applies all mutations atomically after validating every condition.
  public func transaction(_ mutations: [Mutation]) -> Bool {
    guard !mutations.isEmpty else { return false }
    return client?.transaction(FilesystemTransaction(mutations: mutations.map(\.value))) == true
  }

  /// Advances items to the next conveyor stage as one atomic batch.
  /// A first-stage client may introduce a new item by supplying an unused UUID;
  /// later stages must supply UUIDs of items delivered to that client.
  public func pass<T: Codable>(_ items: [(uuid: UUID, value: T)]) -> Bool {
    var encoded: [Data] = []
    encoded.reserveCapacity(items.count)
    do {
      for item in items { encoded.append(try BinaryCodable.encode(item.value)) }
    } catch {
      return false
    }
    return client?.pass(uuids: items.map(\.uuid), encoded: encoded) == true
  }

  /// Moves values directly to another known stage, preserving delivered UUIDs oldest-first.
  public func send<T: Codable>(to: String, values: [T]) -> Bool {
    var encoded: [Data] = []
    encoded.reserveCapacity(values.count)
    do {
      for value in values { encoded.append(try BinaryCodable.encode(value)) }
    } catch {
      return false
    }
    return client?.send(to: to, encoded: encoded) == true
  }

  /// Writes a CRC-protected binary snapshot of the in-memory filesystem to disk atomically.
  public func checkpoint(path: String) -> Bool {
    client?.checkpoint(path: path) == true
  }

  /// Permanently removes an item from UUID tracking and releases its payload when unreferenced.
  public func finish(uuid: UUID) -> Bool {
    client?.finish(uuid: uuid) == true
  }

  /// Returns bytes currently committed in the shared arena, including allocation headers.
  public func memoryUsed() -> UInt64 {
    client?.memoryUsed() ?? 0
  }

  /// Synchronously unregisters this client. Unfinished bucket items remain for reconnection.
  public func disconnect() {
    client?.close()
  }

  /// Subscribes this instance's `notificationHandler` to exact changes at `path`.
  @discardableResult
  public func notify(path: String) -> Bool {
    client?.subscribe(path: path) == true
  }
}
