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
  private let debug: Bool

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
    memoryLimitGB: Int = -1,
    debug: Bool = false
  ) {
    let handlers = RuntimeHandlers()
    self.handlers = handlers
    self.debug = debug
    client = RuntimeClient(
      creator: creator,
      name: name,
      conveyor: conveyor,
      memoryLimitGB: memoryLimitGB,
      handlers: handlers
    )
    if debug, client?.isConnected != true {
      reportError("failed to connect client '\(name)'")
    }
  }

  deinit {
    client?.close()
  }

  /// Installs a typed receive handler. Replaces any previously installed receive handler.
  public func setReceiveHandler<T: Codable>(
    for type: T.Type = T.self,
    _ handler: @escaping (UUID, T) -> Void
  ) {
    handlers.receive = { [weak self] uuid, bytes in
      guard let value = try? BinaryCodable.decode(type, from: bytes) else {
        self?.reportError("failed to decode received value as \(type)")
        return
      }
      handler(uuid, value)
    }
  }

  /// Installs a typed handler that automatically finishes each successfully decoded item.
  /// Use this for terminal consumers; use `setReceiveHandler` when passing or sending onward.
  public func setConsumingReceiveHandler<T: Codable>(
    for type: T.Type = T.self,
    _ handler: @escaping (UUID, T) -> Void
  ) {
    handlers.receive = { [weak client, weak self] uuid, bytes in
      guard let value = try? BinaryCodable.decode(type, from: bytes) else {
        self?.reportError("failed to decode received value as \(type)")
        return
      }
      handler(uuid, value)
      if client?.finish(uuid: uuid) != true {
        self?.reportError("failed to finish consumed item \(uuid)")
      }
    }
  }

  /// Atomically creates or replaces a file, or removes it when `value` is `nil`.
  public func write<T: Codable>(path: String, value: T?) -> Bool {
    guard let value else {
      return checked(client?.delete(path: path, expectedVersion: nil) == true, "delete '\(path)'")
    }
    guard let data = try? BinaryCodable.encode(value) else {
      return failed("encode value for '\(path)'")
    }
    return checked(client?.write(path: path, data: data) == true, "write '\(path)'")
  }

  /// Removes a file when it exists.
  ///
  /// This overload lets callers delete with `write(path: value: nil)` without
  /// providing a type. It returns `false` when the path is absent.
  public func write(path: String, value: Never?) -> Bool {
    checked(client?.delete(path: path, expectedVersion: nil) == true, "delete '\(path)'")
  }

  /// Reads one complete snapshot as owned raw bytes.
  ///
  /// `Data` owns the snapshot, so it remains valid after this method returns. Use
  /// `withUnsafeBytes` when an `UnsafeRawBufferPointer` view is needed.
  public func read(path: String) -> Data? {
    guard let data = client?.withRead(path: path, { Data($0) }) else {
      reportError("failed to read '\(path)'")
      return nil
    }
    return data
  }

  /// Reads and decodes one complete snapshot as the explicitly supplied type.
  public func read<T: Codable>(path: String, as type: T.Type) -> T? {
    guard
      let result = client?.withRead(
        path: path,
        { bytes in
          try? BinaryCodable.decode(type, from: bytes)
        })
    else {
      reportError("failed to read or decode '\(path)' as \(type)")
      return nil
    }
    return result
  }

  /// Reads and decodes one complete snapshot using the type inferred by the caller.
  /// Prefer `read(path:as:)` in new code when an explicit type is clearer.
  public func read<T: Codable>(path: String) -> T? {
    read(path: path, as: T.self)
  }

  /// Reads and decodes one snapshot together with its current version.
  public func readVersioned<T: Codable>(path: String) -> Versioned<T>? {
    let result: Versioned<T>? = client?.withVersionedRead(path: path) { bytes, version in
      guard let value = try? BinaryCodable.decode(T.self, from: bytes) else { return nil }
      return Versioned(value: value, version: version)
    } ?? nil
    if result == nil { reportError("failed to read or decode versioned value at '\(path)'") }
    return result
  }

  /// Lists exact and descendant paths under a normalized prefix.
  public func list(prefix: String = "/") -> [PathEntry]? {
    guard let entries = client?.list(prefix: prefix)?.map(PathEntry.init) else {
      reportError("failed to list '\(prefix)'")
      return nil
    }
    return entries
  }

  /// Applies all mutations atomically after validating every condition.
  public func transaction(_ mutations: [Mutation]) -> Bool {
    guard !mutations.isEmpty else { return failed("apply an empty transaction") }
    return checked(
      client?.transaction(FilesystemTransaction(mutations: mutations.map(\.value))) == true,
      "apply transaction")
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
      return failed("encode values for pass: \(error)")
    }
    return checked(client?.pass(uuids: items.map(\.uuid), encoded: encoded) == true, "pass items")
  }

  /// Moves values directly to another known stage, preserving delivered UUIDs oldest-first.
  public func send<T: Codable>(to: String, values: [T]) -> Bool {
    var encoded: [Data] = []
    encoded.reserveCapacity(values.count)
    do {
      for value in values { encoded.append(try BinaryCodable.encode(value)) }
    } catch {
      return failed("encode values for send: \(error)")
    }
    return checked(client?.send(to: to, encoded: encoded) == true, "send items to '\(to)'")
  }

  /// Writes a CRC-protected binary snapshot of the in-memory filesystem to disk atomically.
  public func checkpoint(path: String) -> Bool {
    checked(client?.checkpoint(path: path) == true, "checkpoint to '\(path)'")
  }

  /// Permanently removes an item from UUID tracking and releases its payload when unreferenced.
  public func finish(uuid: UUID) -> Bool {
    checked(client?.finish(uuid: uuid) == true, "finish \(uuid)")
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
    checked(client?.subscribe(path: path) == true, "subscribe to '\(path)'")
  }

  private func checked(_ success: Bool, _ operation: @autoclosure () -> String) -> Bool {
    guard success else { return failed(operation()) }
    return true
  }

  private func failed(_ operation: String) -> Bool {
    reportError("failed to \(operation)")
    return false
  }

  private func reportError(_ message: String) {
    guard debug else { return }
    let line = "SharedMemory error: \(message)\n"
    FileHandle.standardError.write(Data(line.utf8))
  }
}
