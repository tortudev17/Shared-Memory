import CSharedMemory
import Foundation
import XCTest

@testable import SharedMemoryCore

private final class RegionFixture {
  let name: String
  let region: MappedRegion

  init(bytes: UInt64 = 8 << 20) {
    name = "/smr_t_\(String(UInt64.random(in: 1...UInt64.max), radix: 16))"
    MappedRegion.unlink(name: name)
    guard
      let mapping = MappedRegion.create(name: name, bytes: max(bytes, smr_minimum_region_size())),
      mapping.initialize(bootID: 0x1234_5678, daemonPID: smr_current_pid())
    else { fatalError("Unable to create test shared memory") }
    region = mapping
  }

  deinit {
    MappedRegion.unlink(name: name)
  }
}

final class SharedArenaTests: XCTestCase {
  func testAllocationReferenceCountingAndCoalescing() throws {
    let fixture = RegionFixture()
    let arena = try XCTUnwrap(SharedArena(region: fixture.region, secret: 42))
    let first = try XCTUnwrap(arena.allocate(payloadBytes: 256_000))
    let second = try XCTUnwrap(arena.allocate(payloadBytes: 256_000))
    XCTAssertGreaterThan(arena.usedBytes, 512_000)
    XCTAssertTrue(arena.retain(first.blockOffset))
    XCTAssertTrue(arena.release(first.blockOffset))
    XCTAssertNotNil(arena.allocation(at: first.blockOffset))
    XCTAssertTrue(arena.release(first.blockOffset))
    XCTAssertTrue(arena.release(second.blockOffset))
    XCTAssertEqual(arena.usedBytes, 0)
    XCTAssertTrue(arena.validateAllBlocks())

    let coalesced = try XCTUnwrap(arena.allocate(payloadBytes: 600_000))
    XCTAssertEqual(coalesced.blockOffset, first.blockOffset)
    XCTAssertTrue(arena.release(coalesced.blockOffset))
  }

  func testFullArenaFailsWithoutEvictionAndRecovers() throws {
    let fixture = RegionFixture(bytes: 4 << 20)
    let arena = try XCTUnwrap(SharedArena(region: fixture.region, secret: 99))
    var allocations: [ArenaAllocation] = []
    while let allocation = arena.allocate(payloadBytes: 64 << 10) {
      allocations.append(allocation)
    }
    while let allocation = arena.allocate(payloadBytes: 1) {
      allocations.append(allocation)
    }
    XCTAssertFalse(allocations.isEmpty)
    XCTAssertNil(arena.allocate(payloadBytes: 1))
    for allocation in allocations { XCTAssertTrue(arena.release(allocation.blockOffset)) }
    XCTAssertEqual(arena.usedBytes, 0)
    XCTAssertTrue(arena.validateAllBlocks())
    XCTAssertNotNil(arena.allocate(payloadBytes: 512 << 10))
  }

  func testCorruptedHeaderIsDetected() throws {
    let fixture = RegionFixture()
    let arena = try XCTUnwrap(SharedArena(region: fixture.region, secret: 7))
    let allocation = try XCTUnwrap(arena.allocate(payloadBytes: 128))
    arena.unsafeCorruptCookieForTesting(blockOffset: allocation.blockOffset)
    XCTAssertFalse(arena.release(allocation.blockOffset))
    XCTAssertTrue(arena.corruptionDetected)
    XCTAssertNil(arena.allocate(payloadBytes: 64))
  }
}

final class SharedMemoryABITests: XCTestCase {
  func testMailboxPublishesOnlyCompleteRequestsAndResponses() throws {
    let fixture = RegionFixture()
    let slot = try XCTUnwrap(fixture.region.slot(at: 0))
    XCTAssertEqual(smr_slot_claim(slot), 1)
    XCTAssertEqual("client".withCString { smr_slot_prepare(slot, smr_current_pid(), 55, $0) }, 1)
    XCTAssertEqual(
      "/a".withCString { path in
        smr_client_submit(slot, 9, RuntimeCommand.write.rawValue, 0, 1, 2, 3, 4, 5, 6, path, nil)
      },
      1
    )
    var request = SMRRequest()
    XCTAssertEqual(smr_daemon_take_request(slot, &request), 1)
    XCTAssertEqual(request.sequence, 9)
    XCTAssertEqual(request.arg0, 1)
    XCTAssertEqual(request.arg5, 6)
    var response = SMRResponse()
    response.sequence = 9
    response.status = 1
    response.value0 = 123
    smr_daemon_complete_request(slot, &response)
    var received = SMRResponse()
    XCTAssertEqual(smr_client_try_response(slot, 9, &received), 1)
    XCTAssertEqual(received.value0, 123)
    smr_client_ack_response(slot)
    smr_slot_reset(slot)
  }

  func testSPSCEventRingPreservesOrder() throws {
    let fixture = RegionFixture()
    let slot = try XCTUnwrap(fixture.region.slot(at: 0))
    XCTAssertEqual(smr_slot_claim(slot), 1)
    XCTAssertEqual("events".withCString { smr_slot_prepare(slot, smr_current_pid(), 77, $0) }, 1)
    smr_slot_activate(slot)
    for value in 0..<10_000 {
      var event = SMREvent()
      event.kind = RuntimeEventKind.notification.rawValue
      event.generation = 77
      event.value = UInt64(value)
      XCTAssertEqual(smr_daemon_push_event(slot, &event), 1)
      var received = SMREvent()
      XCTAssertEqual(smr_client_pop_event(slot, 77, &received), 1)
      XCTAssertEqual(received.value, UInt64(value))
    }
    XCTAssertEqual(smr_event_count(slot), 0)
  }

  func testRegionHeaderCorruptionIsRejected() {
    let fixture = RegionFixture()
    XCTAssertTrue(fixture.region.isValid())
    let original = fixture.region.baseAddress.load(as: UInt8.self)
    fixture.region.baseAddress.storeBytes(of: original ^ 0xff, as: UInt8.self)
    XCTAssertFalse(fixture.region.isValid())
    fixture.region.baseAddress.storeBytes(of: original, as: UInt8.self)
    XCTAssertTrue(fixture.region.isValid())
  }
}

final class DaemonStateLifecycleTests: XCTestCase {
  func testEmptyTransientClientMetadataIsRemovedOnUnregister() throws {
    let fixture = RegionFixture()
    let state = try XCTUnwrap(
      DaemonState(region: fixture.region, configuration: PipelineConfiguration(nil)))
    let slot = try XCTUnwrap(fixture.region.slot(at: 0))
    XCTAssertEqual(smr_slot_claim(slot), 1)
    XCTAssertEqual(
      "transient".withCString { smr_slot_prepare(slot, smr_current_pid(), 91, $0) }, 1)

    var register = SMRRequest()
    register.sequence = 1
    register.opcode = RuntimeCommand.register.rawValue
    XCTAssertEqual(state.handle(slotIndex: 0, slot: slot, request: register).status, 1)
    XCTAssertEqual(state.bucketCount(), 1)
    XCTAssertEqual(state.knownNameCount(), 1)

    var unregister = SMRRequest()
    unregister.sequence = 2
    unregister.opcode = RuntimeCommand.unregister.rawValue
    XCTAssertEqual(state.handle(slotIndex: 0, slot: slot, request: unregister).status, 1)
    XCTAssertEqual(state.connectionCount(), 0)
    XCTAssertEqual(state.bucketCount(), 0)
    XCTAssertEqual(state.knownNameCount(), 0)
  }
}

final class BinaryFormatTests: XCTestCase {
  func testBatchFramingRoundTripsAndRejectsTruncation() throws {
    let values = [Data([1, 2, 3]), Data(), Data(repeating: 9, count: 128)]
    let uuids = values.map { _ in UUID() }
    let required = try XCTUnwrap(BatchLayout.requiredBytes(for: values, uuids: uuids))
    let memory = UnsafeMutableRawPointer.allocate(byteCount: required, alignment: 64)
    defer { memory.deallocate() }
    XCTAssertTrue(BatchLayout.write(values, uuids: uuids, to: memory, capacity: required))
    let payloads = try XCTUnwrap(
      BatchLayout.parse(
        at: UnsafeRawPointer(memory),
        byteCount: UInt64(required),
        absolutePayloadOffset: 1_000
      ))
    XCTAssertEqual(payloads.map(\.payloadSize), values.map { UInt64($0.count) })
    XCTAssertEqual(payloads.compactMap(\.uuid), uuids)
    XCTAssertNil(
      BatchLayout.parse(
        at: UnsafeRawPointer(memory),
        byteCount: UInt64(required - 1),
        absolutePayloadOffset: 1_000
      ))
  }

  func testCheckpointRoundTripAndChecksumCorruption() throws {
    let firstPayload = Data([1, 2, 3])
    let secondPayload = Data(repeating: 4, count: 100)
    let entries = [
      CheckpointEntry(path: "/a", payload: firstPayload),
      CheckpointEntry(path: "/nested/b", payload: secondPayload),
    ]
    let archive = try XCTUnwrap(CheckpointArchive.encode(entries))
    XCTAssertEqual(CheckpointArchive.decode(archive), entries)

    let streamedURL = FileManager.default.temporaryDirectory.appendingPathComponent(
      "smr-checkpoint-\(UUID().uuidString)")
    defer { try? FileManager.default.removeItem(at: streamedURL) }
    firstPayload.withUnsafeBytes { first in
      secondPayload.withUnsafeBytes { second in
        XCTAssertTrue(
          CheckpointArchive.writeMapped(
            [
              MappedCheckpointEntry(path: "/nested/b", payload: second),
              MappedCheckpointEntry(path: "/a", payload: first),
            ],
            to: streamedURL.path))
      }
    }
    XCTAssertEqual(try Data(contentsOf: streamedURL), archive)

    var corrupted = archive
    corrupted[corrupted.count / 2] ^= 1
    XCTAssertNil(CheckpointArchive.decode(corrupted))
    XCTAssertNil(CheckpointArchive.decode(archive.dropLast()))
  }

  func testBinaryCodableSupportsTopLevelAndStructuredValues() throws {
    struct Value: Codable, Equatable {
      let id: UUID
      let bytes: Data
      let values: [Int]
    }
    let topLevel = try BinaryCodable.encode(42)
    let decodedInteger: Int = try topLevel.withUnsafeBytes {
      try BinaryCodable.decode(Int.self, from: $0)
    }
    XCTAssertEqual(decodedInteger, 42)
    let value = Value(id: UUID(), bytes: Data([8, 9]), values: [1, 2, 3])
    let encoded = try BinaryCodable.encode(value)
    let decoded: Value = try encoded.withUnsafeBytes {
      try BinaryCodable.decode(Value.self, from: $0)
    }
    XCTAssertEqual(decoded, value)
  }
}

final class ConfigurationTests: XCTestCase {
  func testPathNormalizationAndTraversalRules() {
    XCTAssertEqual(RuntimeValidation.normalize(path: "/a//b/./c"), "/a/b/c")
    XCTAssertEqual(RuntimeValidation.normalize(path: "/a/b/../c"), "/a/c")
    XCTAssertNil(RuntimeValidation.normalize(path: "/../escape"))
    XCTAssertNil(RuntimeValidation.normalize(path: "relative"))
  }

  func testConveyorsAllowSharedWorkersButRequireUniqueStarts() {
    XCTAssertNotNil(PipelineConfiguration([["a", "b"], ["c"]]).validated())
    XCTAssertNotNil(PipelineConfiguration([["abcd", "a", "b"], ["dcab", "h", "a"]]).validated())
    XCTAssertNotNil(PipelineConfiguration([["a", "b"], ["c", "a"]]).validated())
    XCTAssertNil(PipelineConfiguration([["a"], ["a"]]).validated())
    XCTAssertNil(PipelineConfiguration([["a", "b", "a"]]).validated())
    XCTAssertNil(PipelineConfiguration([["a"], ["b"], ["c"], ["d"]]).validated())
    XCTAssertNil(PipelineConfiguration([[]]).validated())
  }

  func testDarwinCompatibleSharedMemoryNames() {
    let names = RuntimeNames(environment: ["SMR_INSTANCE_ID": String(repeating: "x", count: 1_000)])
    XCTAssertLessThanOrEqual(names.sharedMemory.utf8.count, 31)
    XCTAssertTrue(names.sharedMemory.hasPrefix("/"))
  }
}
