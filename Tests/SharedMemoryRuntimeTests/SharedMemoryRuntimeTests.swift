import CSharedMemory
import Foundation
import SharedMemoryRuntime
import XCTest

@testable import SharedMemoryCore

#if canImport(Darwin)
  import Darwin
#else
  import Glibc
#endif

private final class Locked<Value>: @unchecked Sendable {
  private let lock = NSLock()
  private var value: Value

  init(_ value: Value) { self.value = value }

  func withValue<R>(_ body: (inout Value) -> R) -> R {
    lock.lock()
    defer { lock.unlock() }
    return body(&value)
  }

  func snapshot() -> Value { withValue { $0 } }
}

private final class IntegrationEnvironment: @unchecked Sendable {
  static let shared = IntegrationEnvironment()

  let names: RuntimeNames
  let root: SharedMemory

  private init() {
    let instance =
      "tests-\(ProcessInfo.processInfo.processIdentifier)-\(UInt64.random(in: 1...UInt64.max))"
    setenv("SMR_INSTANCE_ID", instance, 1)
    setenv("SMR_MEMORY_BYTES", "33554432", 1)
    names = RuntimeNames()
    root = SharedMemory(
      creator: true,
      name: "test-root",
      conveyor: [
        ["source", "middle", "sink"],
        ["reconnect-source", "middle", "reconnect-sink"],
        ["send-source", "send-sink"],
      ]
    )
    precondition(root.isConnected, "Integration daemon did not start")
  }

  func stopDaemon() {
    root.receiveHandler = nil
    root.notificationHandler = nil
    guard let region = MappedRegion.open(name: names.sharedMemory) else { return }
    let pid = smr_daemon_pid(region.baseAddress)
    guard pid != smr_current_pid() else { return }
    if pid > 0 { _ = kill(pid, SIGTERM) }
    let deadline = smr_monotonic_nanoseconds() + 2_000_000_000
    while smr_pid_is_alive(pid) == 1, smr_monotonic_nanoseconds() < deadline {
      smr_sleep_nanoseconds(1_000_000)
    }
  }
}

final class SharedMemoryRuntimeTests: XCTestCase {
  private static let serializationLock = NSLock()
  private var environment: IntegrationEnvironment!

  override func setUp() {
    super.setUp()
    Self.serializationLock.lock()
    environment = .shared
    environment.root.receiveHandler = nil
    environment.root.notificationHandler = nil
  }

  override func tearDown() {
    environment.root.receiveHandler = nil
    environment.root.notificationHandler = nil
    environment = nil
    Self.serializationLock.unlock()
    super.tearDown()
  }

  override class func tearDown() {
    IntegrationEnvironment.shared.stopDaemon()
    super.tearDown()
  }

  func testFilesystemCreatesParentsAndReturnsSnapshots() {
    struct Settings: Codable, Equatable {
      let theme: String
      let scale: Double
    }
    let expected = Settings(theme: "dark", scale: 1.5)
    XCTAssertTrue(environment.root.write(path: "/users/me/settings", value: expected))
    let value: Settings? = environment.root.read(path: "/users/me/settings")
    XCTAssertEqual(value, expected)
    let explicit = environment.root.read(path: "/users/me/settings", as: Settings.self)
    XCTAssertEqual(explicit, expected)
    let raw = environment.root.read(path: "/users/me/settings")
    XCTAssertFalse(try XCTUnwrap(raw).isEmpty)
    let normalized: Settings? = environment.root.read(path: "/users//me/./settings")
    XCTAssertEqual(normalized, expected)
    let missing: Settings? = environment.root.read(path: "/missing")
    XCTAssertNil(missing)
    XCTAssertFalse(environment.root.write(path: "relative", value: expected))
  }

  func testVersionedListWriteNilAndConditionalTransaction() throws {
    let prefix = "/extended/\(UUID().uuidString)"
    let first = prefix + "/first"
    let second = prefix + "/second"
    XCTAssertTrue(environment.root.write(path: first, value: 10))

    let initial: SharedMemory.Versioned<Int> = try XCTUnwrap(
      environment.root.readVersioned(path: first))
    XCTAssertEqual(initial.value, 10)
    XCTAssertGreaterThan(initial.version, 0)
    XCTAssertEqual(environment.root.list(prefix: prefix)?.map(\.path), [first])

    let write = try XCTUnwrap(
      SharedMemory.Mutation.write(path: first, value: 11, expectedVersion: initial.version))
    let create = try XCTUnwrap(
      SharedMemory.Mutation.write(path: second, value: "created", expectedVersion: 0))
    XCTAssertTrue(environment.root.transaction([write, create]))

    let createAgain = try XCTUnwrap(
      SharedMemory.Mutation.write(path: second, value: "must-not-replace", expectedVersion: 0))
    XCTAssertFalse(environment.root.transaction([createAgain]))
    let unchanged: String? = environment.root.read(path: second)
    XCTAssertEqual(unchanged, "created")

    let changed: SharedMemory.Versioned<Int> = try XCTUnwrap(
      environment.root.readVersioned(path: first))
    XCTAssertEqual(changed.value, 11)
    XCTAssertNotEqual(changed.version, initial.version)
    XCTAssertEqual(environment.root.list(prefix: prefix)?.map(\.path), [first, second])

    let stale = try XCTUnwrap(
      SharedMemory.Mutation.write(path: first, value: 12, expectedVersion: initial.version))
    XCTAssertFalse(environment.root.transaction([stale]))
    XCTAssertTrue(environment.root.write(path: first, value: nil))
    XCTAssertFalse(environment.root.write(path: first, value: nil))
    let missing: Int? = environment.root.read(path: first)
    XCTAssertNil(missing)

    let secondVersion = try XCTUnwrap(
      (environment.root.readVersioned(path: second) as SharedMemory.Versioned<String>?)?.version)
    XCTAssertTrue(
      environment.root.transaction([
        SharedMemory.Mutation.delete(path: second, expectedVersion: secondVersion)
      ]))
    XCTAssertEqual(environment.root.list(prefix: prefix), [])
  }

  func testNotificationsAreDeliveredForEveryCommittedChange() {
    let path = "/notifications/\(UUID().uuidString)"
    let expectation = expectation(description: "three notifications")
    expectation.expectedFulfillmentCount = 3
    let received = Locked<[String]>([])
    environment.root.notificationHandler = { changedPath in
      received.withValue { $0.append(changedPath) }
      expectation.fulfill()
    }
    XCTAssertTrue(environment.root.notify(path: path))
    let writer = SharedMemory(name: "writer-\(UUID().uuidString)")
    XCTAssertTrue(writer.isConnected)
    for value in 0..<3 { XCTAssertTrue(writer.write(path: path, value: value)) }
    wait(for: [expectation], timeout: 5)
    XCTAssertEqual(received.snapshot(), [path, path, path])
  }

  func testPipelineOrderingAndUUIDContinuity() {
    let source = SharedMemory(name: "source")
    let middle = SharedMemory(name: "middle")
    let sink = SharedMemory(name: "sink")
    XCTAssertTrue(source.isConnected && middle.isConnected && sink.isConnected)

    let completed = expectation(description: "pipeline completed")
    let ids = Locked<[UUID]>([])
    let values = Locked<[Int]>([])
    let successes = Locked<[Bool]>([])
    source.setReceiveHandler(for: Int.self) { [weak source] uuid, value in
      ids.withValue { $0.append(uuid) }
      values.withValue { $0.append(value) }
      successes.withValue { $0.append(source?.pass([(uuid, value + 1)]) == true) }
    }
    middle.setReceiveHandler(for: Int.self) { [weak middle] uuid, value in
      ids.withValue { $0.append(uuid) }
      values.withValue { $0.append(value) }
      successes.withValue { $0.append(middle?.pass([(uuid, value + 1)]) == true) }
    }
    sink.setReceiveHandler(for: Int.self) { [weak sink] uuid, value in
      ids.withValue { $0.append(uuid) }
      values.withValue { $0.append(value) }
      successes.withValue { $0.append(sink?.finish(uuid: uuid) == true) }
      completed.fulfill()
    }

    XCTAssertTrue(environment.root.send(to: "source", values: [10]))
    wait(for: [completed], timeout: 5)
    XCTAssertEqual(values.snapshot(), [10, 11, 12])
    XCTAssertEqual(Set(ids.snapshot()).count, 1)
    XCTAssertEqual(successes.snapshot(), [true, true, true])
  }

  func testSharedWorkerRoutesEachUUIDAlongItsOriginalConveyor() {
    let firstSource = SharedMemory(name: "source")
    let secondSource = SharedMemory(name: "reconnect-source")
    let sharedWorker = SharedMemory(name: "middle")
    let firstSink = SharedMemory(name: "sink")
    let secondSink = SharedMemory(name: "reconnect-sink")
    XCTAssertTrue(
      firstSource.isConnected && secondSource.isConnected && sharedWorker.isConnected
        && firstSink.isConnected && secondSink.isConnected)

    let sharedDeliveries = expectation(description: "shared worker receives both conveyors")
    sharedDeliveries.expectedFulfillmentCount = 2
    let sharedItems = Locked<[(UUID, Int)]>([])
    firstSource.setReceiveHandler(for: Int.self) { [weak firstSource] uuid, value in
      XCTAssertTrue(firstSource?.pass([(uuid, value + 10)]) == true)
    }
    secondSource.setReceiveHandler(for: Int.self) { [weak secondSource] uuid, value in
      XCTAssertTrue(secondSource?.pass([(uuid, value + 20)]) == true)
    }
    sharedWorker.setReceiveHandler(for: Int.self) { uuid, value in
      sharedItems.withValue { $0.append((uuid, value)) }
      sharedDeliveries.fulfill()
    }

    let completed = expectation(description: "both UUIDs reach their own sinks")
    completed.expectedFulfillmentCount = 2
    let firstResults = Locked<[(UUID, Int)]>([])
    let secondResults = Locked<[(UUID, Int)]>([])
    firstSink.setReceiveHandler(for: Int.self) { [weak firstSink] uuid, value in
      firstResults.withValue { $0.append((uuid, value)) }
      XCTAssertTrue(firstSink?.finish(uuid: uuid) == true)
      completed.fulfill()
    }
    secondSink.setReceiveHandler(for: Int.self) { [weak secondSink] uuid, value in
      secondResults.withValue { $0.append((uuid, value)) }
      XCTAssertTrue(secondSink?.finish(uuid: uuid) == true)
      completed.fulfill()
    }

    XCTAssertTrue(environment.root.send(to: "source", values: [1]))
    XCTAssertTrue(environment.root.send(to: "reconnect-source", values: [2]))
    wait(for: [sharedDeliveries], timeout: 5)

    let items = sharedItems.snapshot()
    XCTAssertEqual(items.count, 2)
    XCTAssertTrue(sharedWorker.pass(items.map { ($0.0, $0.1 + 100) }))
    wait(for: [completed], timeout: 5)

    XCTAssertEqual(firstResults.snapshot().map(\.1), [111])
    XCTAssertEqual(secondResults.snapshot().map(\.1), [122])
    XCTAssertEqual(Set(firstResults.snapshot().map(\.0)), Set(items.filter { $0.1 == 11 }.map(\.0)))
    XCTAssertEqual(Set(secondResults.snapshot().map(\.0)), Set(items.filter { $0.1 == 22 }.map(\.0)))
  }

  func testPassRequiresTheDeliveredUUIDAndCanAdvanceOutOfOrder() {
    let source = SharedMemory(name: "source")
    let sink = SharedMemory(name: "middle")
    XCTAssertTrue(source.isConnected && sink.isConnected)

    let delivered = expectation(description: "source deliveries")
    delivered.expectedFulfillmentCount = 2
    let sourceItems = Locked<[(UUID, Int)]>([])
    source.setReceiveHandler(for: Int.self) { uuid, value in
      sourceItems.withValue { $0.append((uuid, value)) }
      delivered.fulfill()
    }
    XCTAssertTrue(environment.root.send(to: "source", values: [1, 2]))
    wait(for: [delivered], timeout: 5)

    let items = sourceItems.snapshot()
    guard items.count == 2 else {
      XCTFail("expected two source deliveries")
      return
    }
    let advanced = expectation(description: "out-of-order advances")
    advanced.expectedFulfillmentCount = 3
    let sinkItems = Locked<[(UUID, Int)]>([])
    sink.setReceiveHandler(for: Int.self) { [weak sink] uuid, value in
      sinkItems.withValue { $0.append((uuid, value)) }
      _ = sink?.finish(uuid: uuid)
      advanced.fulfill()
    }

    // First-stage clients may inject new work directly with pass.
    let injected = UUID()
    XCTAssertTrue(source.pass([(injected, 999)]))
    XCTAssertTrue(source.pass([(items[1].0, 102), (items[0].0, 101)]))
    wait(for: [advanced], timeout: 5)

    let results = sinkItems.snapshot()
    XCTAssertEqual(results.map(\.0), [injected, items[1].0, items[0].0])
    XCTAssertEqual(results.map(\.1), [999, 102, 101])

    // A non-first stage cannot invent a UUID.
    XCTAssertFalse(sink.pass([(UUID(), 999)]))
  }

  func testStressReceiveCallbacksAreSerialAndInBatchOrder() {
    let sink = SharedMemory(name: "send-sink")
    XCTAssertTrue(sink.isConnected)
    let count = 500
    let expectation = expectation(description: "ordered batch")
    expectation.expectedFulfillmentCount = count
    let received = Locked<[Int]>([])
    let activeCallbacks = Locked(0)
    let maximumCallbacks = Locked(0)
    sink.setReceiveHandler(for: Int.self) { [weak sink] uuid, value in
      let active = activeCallbacks.withValue { current -> Int in
        current += 1
        return current
      }
      maximumCallbacks.withValue { $0 = max($0, active) }
      received.withValue { $0.append(value) }
      _ = sink?.finish(uuid: uuid)
      activeCallbacks.withValue { $0 -= 1 }
      expectation.fulfill()
    }
    XCTAssertTrue(environment.root.send(to: "send-sink", values: Array(0..<count)))
    wait(for: [expectation], timeout: 10)
    XCTAssertEqual(received.snapshot(), Array(0..<count))
    XCTAssertEqual(maximumCallbacks.snapshot(), 1)
  }

  func testConsumingReceiveHandlerReclaimsRepeatedSendPayloads() {
    struct Payload: Codable {
      let sequence: Int
      let bytes: Data
    }
    let name = "consume-sink-\(UUID().uuidString)"
    let sink = SharedMemory(name: name)
    XCTAssertTrue(sink.isConnected)
    let baseline = environment.root.memoryUsed()
    let received = DispatchSemaphore(value: 0)
    let receivedCount = Locked(0)
    sink.setConsumingReceiveHandler(for: Payload.self) { _, _ in
      receivedCount.withValue { $0 += 1 }
      received.signal()
    }
    let bytes = Data(repeating: 0xa5, count: 256 << 10)
    for sequence in 0..<1_000 {
      XCTAssertTrue(
        environment.root.send(
          to: name, values: [Payload(sequence: sequence, bytes: bytes)]))
      XCTAssertEqual(received.wait(timeout: .now() + 2), .success)
    }
    let deadline = smr_monotonic_nanoseconds() + 2_000_000_000
    while environment.root.memoryUsed() != baseline, smr_monotonic_nanoseconds() < deadline {
      smr_sleep_nanoseconds(1_000_000)
    }
    XCTAssertEqual(receivedCount.snapshot(), 1_000)
    XCTAssertEqual(environment.root.memoryUsed(), baseline)
  }

  func testUnchangedPassReusesTheExistingSharedPayloadBlock() {
    struct Blob: Codable, Equatable {
      let id: Int
      let bytes: Data
    }
    let source = SharedMemory(name: "send-source")
    let sink = SharedMemory(name: "send-sink")
    XCTAssertTrue(source.isConnected && sink.isConnected)
    let completed = expectation(description: "zero-copy pass completed")
    let measurements = Locked<(before: UInt64, after: UInt64)?>(nil)
    source.setReceiveHandler(for: Blob.self) { [weak source] uuid, value in
      guard let source else { return }
      let before = source.memoryUsed()
      let succeeded = source.pass([(uuid, value)])
      let after = source.memoryUsed()
      if succeeded { measurements.withValue { $0 = (before, after) } }
    }
    sink.setReceiveHandler(for: Blob.self) { [weak sink] uuid, _ in
      _ = sink?.finish(uuid: uuid)
      completed.fulfill()
    }
    let value = Blob(id: 1, bytes: Data(repeating: 0x7f, count: 1 << 20))
    XCTAssertTrue(environment.root.send(to: "send-source", values: [value]))
    wait(for: [completed], timeout: 5)
    guard let result = measurements.snapshot() else {
      XCTFail("pass did not complete")
      return
    }
    XCTAssertEqual(result.before, result.after)
  }

  func testBucketSurvivesDisconnectAndReconnect() {
    let firstDelivery = expectation(description: "first delivery")
    let firstUUID = Locked<UUID?>(nil)
    var first: SharedMemory? = SharedMemory(name: "reconnect-sink")
    XCTAssertTrue(first?.isConnected == true)
    first?.setReceiveHandler(for: Int.self) { uuid, _ in
      firstUUID.withValue { $0 = uuid }
      firstDelivery.fulfill()
    }
    XCTAssertTrue(environment.root.send(to: "reconnect-sink", values: [1]))
    wait(for: [firstDelivery], timeout: 5)
    let usedBeforeDisconnect = environment.root.memoryUsed()
    first?.disconnect()
    first = nil
    let usedAfterDisconnect = environment.root.memoryUsed()
    XCTAssertEqual(
      usedAfterDisconnect, usedBeforeDisconnect, "disconnect released an unfinished item payload")

    XCTAssertTrue(environment.root.send(to: "reconnect-sink", values: [2]))
    XCTAssertGreaterThan(environment.root.memoryUsed(), usedAfterDisconnect)
    let reconnected = SharedMemory(name: "reconnect-sink")
    XCTAssertTrue(reconnected.isConnected)
    let redelivery = expectation(description: "pending items redelivered")
    redelivery.expectedFulfillmentCount = 2
    let values = Locked<[Int]>([])
    let ids = Locked<[UUID]>([])
    reconnected.setReceiveHandler(for: Int.self) { [weak reconnected] uuid, value in
      ids.withValue { $0.append(uuid) }
      values.withValue { $0.append(value) }
      _ = reconnected?.finish(uuid: uuid)
      redelivery.fulfill()
    }
    wait(for: [redelivery], timeout: 5)
    XCTAssertEqual(values.snapshot(), [1, 2])
    XCTAssertEqual(ids.snapshot().first, firstUUID.snapshot())
  }

  func testDuplicateNamesAreRejectedAndReusableAfterDisconnect() {
    let name = "duplicate-\(UUID().uuidString)"
    var first: SharedMemory? = SharedMemory(name: name)
    XCTAssertTrue(first?.isConnected == true)
    let duplicate = SharedMemory(name: name)
    XCTAssertFalse(duplicate.isConnected)
    first?.disconnect()
    first = nil
    let replacement = SharedMemory(name: name)
    XCTAssertTrue(replacement.isConnected)
  }

  func testConcurrentReadersNeverObservePartialWrites() {
    struct Invariant: Codable {
      let value: Int
      let complement: Int
    }
    let path = "/synchronization/value"
    XCTAssertTrue(environment.root.write(path: path, value: Invariant(value: 0, complement: ~0)))
    let failures = Locked(0)
    let group = DispatchGroup()
    let queue = DispatchQueue(label: "SharedMemoryRuntimeTests.contention", attributes: .concurrent)
    for worker in 0..<8 {
      group.enter()
      queue.async { [root = environment.root] in
        defer { group.leave() }
        for iteration in 0..<250 {
          if worker % 2 == 0 {
            let value = worker * 10_000 + iteration
            if !root.write(path: path, value: Invariant(value: value, complement: ~value)) {
              failures.withValue { $0 += 1 }
            }
          } else {
            let value: Invariant? = root.read(path: path)
            if let value, value.complement != ~value.value {
              failures.withValue { $0 += 1 }
            }
          }
        }
      }
    }
    XCTAssertEqual(group.wait(timeout: .now() + 20), .success)
    XCTAssertEqual(failures.snapshot(), 0)
  }

  func testCheckpointContainsCompleteBinaryFilesystemValues() throws {
    struct Record: Codable, Equatable {
      let name: String
      let values: [Int]
    }
    let path = "/checkpoint/\(UUID().uuidString)"
    let record = Record(name: "complete", values: Array(0..<100))
    XCTAssertTrue(environment.root.write(path: path, value: record))
    let diskPath = "/tmp/smr-checkpoint-\(UUID().uuidString).bin"
    defer { try? FileManager.default.removeItem(atPath: diskPath) }
    XCTAssertTrue(environment.root.checkpoint(path: diskPath))
    let archive = try Data(contentsOf: URL(fileURLWithPath: diskPath))
    let entries = try XCTUnwrap(CheckpointArchive.decode(archive))
    let entry = try XCTUnwrap(entries.first { $0.path == path })
    let decoded: Record = try entry.payload.withUnsafeBytes {
      try BinaryCodable.decode(Record.self, from: $0)
    }
    XCTAssertEqual(decoded, record)
  }

  func testFullMemoryReturnsFalseWithoutEviction() {
    let preservedPath = "/capacity/preserved"
    XCTAssertTrue(environment.root.write(path: preservedPath, value: "keep"))
    let oversized = Data(repeating: 0xab, count: 40 << 20)
    XCTAssertFalse(environment.root.write(path: "/capacity/too-large", value: oversized))
    let preserved: String? = environment.root.read(path: preservedPath)
    XCTAssertEqual(preserved, "keep")
  }
}
