import Foundation
import SharedMemoryCore

/// A high-performance, local IPC runtime backed by one daemon-owned shared-memory region.
///
/// `SharedMemory` is the package's only public type. Each logical payload version crosses the
/// serialization boundary once, then remains in shared memory while it moves through stages.
public final class SharedMemory: @unchecked Sendable {
  public struct Versioned<Value: Codable>: Codable {
    public let value: Value
    public let version: UInt64
  }

  public struct PathEntry: Codable, Equatable, Sendable {
    public let path: String
    public let version: UInt64
  }

  public struct Mutation: Sendable {
    public enum Kind: Sendable { case write(Data), delete }
    public let kind: Kind
    public let path: String
    /// `nil` is unconditional, `0` requires an absent path, and a positive
    /// value requires the exact current file version.
    public let expectedVersion: UInt64?

    public init(kind: Kind, path: String, expectedVersion: UInt64? = nil) {
      self.kind = kind
      self.path = path
      self.expectedVersion = expectedVersion
    }

    public static func write<T: Codable>(
      path: String, value: T, expectedVersion: UInt64? = nil
    ) -> Mutation? {
      guard let payload = try? BinaryCodable.encode(value) else { return nil }
      return Mutation(kind: .write(payload), path: path, expectedVersion: expectedVersion)
    }

    public static func delete(path: String, expectedVersion: UInt64? = nil) -> Mutation {
      Mutation(kind: .delete, path: path, expectedVersion: expectedVersion)
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

  /// Atomically creates or replaces a file, creating parent directories implicitly.
  public func write<T: Codable>(path: String, value: T) -> Bool {
    guard let data = try? BinaryCodable.encode(value) else { return false }
    return client?.write(path: path, data: data) == true
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

  /// Reads one complete value together with its monotonically changing file version.
  public func readVersioned<T: Codable>(path: String) -> Versioned<T>? {
    client?.withVersionedRead(path: path) { bytes, version in
      guard let value = try? BinaryCodable.decode(T.self, from: bytes) else { return nil }
      return Versioned(value: value, version: version)
    } ?? nil
  }

  /// Returns sorted files at `prefix`, including the exact path when present.
  public func list(prefix: String = "/") -> [PathEntry]? {
    client?.list(prefix: prefix)?.map { PathEntry(path: $0.path, version: $0.version) }
  }

  /// Deletes an existing file, optionally only when its version still matches.
  public func delete(path: String, expectedVersion: UInt64? = nil) -> Bool {
    client?.delete(path: path, expectedVersion: expectedVersion) == true
  }

  /// Atomically applies pre-encoded filesystem writes and deletes. An
  /// `expectedVersion` of zero is a create-only precondition.
  public func transaction(_ mutations: [Mutation]) -> Bool {
    let encoded = mutations.map { mutation -> FilesystemMutation in
      switch mutation.kind {
      case .write(let payload):
        return FilesystemMutation(
          kind: .write, path: mutation.path,
          expectedVersion: mutation.expectedVersion, payload: payload)
      case .delete:
        return FilesystemMutation(
          kind: .delete, path: mutation.path,
          expectedVersion: mutation.expectedVersion)
      }
    }
    return client?.transaction(FilesystemTransaction(mutations: encoded)) == true
  }

  /// Advances delivered items to the next conveyor stage as one atomic batch.
  /// Every value must include the UUID of the item it replaces.
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
