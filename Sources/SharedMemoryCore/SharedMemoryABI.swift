import CSharedMemory
import Foundation

package enum RuntimeCommand: UInt32, Sendable {
  case register = 1
  case unregister = 2
  case allocate = 3
  case abandon = 4
  case write = 5
  case read = 6
  case pass = 7
  case send = 8
  case finish = 9
  case memoryUsed = 10
  case subscribe = 11
  case releaseLease = 12
  case checkpoint = 13
  case ping = 14
  case configure = 15
}

package enum RuntimeEventKind: UInt64, Sendable {
  case receive = 1
  case notification = 2
}

package struct RuntimeResponse: Sendable {
  package let succeeded: Bool
  package let status: Int64
  package let value0: UInt64
  package let value1: UInt64
  package let value2: UInt64
  package let value3: UInt64

  package init(_ value: SMRResponse) {
    status = value.status
    succeeded = value.status == 1
    value0 = value.value0
    value1 = value.value1
    value2 = value.value2
    value3 = value.value3
  }
}

package final class MappedRegion: @unchecked Sendable {
  private var mapping: SMRMapping

  package var baseAddress: UnsafeMutableRawPointer {
    precondition(mapping.address != nil)
    return mapping.address!
  }

  package var size: UInt64 { mapping.size }
  package var heapOffset: UInt64 { smr_heap_offset(baseAddress) }
  package var heapSize: UInt64 { smr_heap_size(baseAddress) }
  package var bootID: UInt64 { smr_boot_id(baseAddress) }

  private init(mapping: SMRMapping) {
    self.mapping = mapping
  }

  deinit {
    smr_mapping_close(&mapping)
  }

  package static func create(name: String, bytes: UInt64) -> MappedRegion? {
    var mapping = SMRMapping()
    let result = name.withCString { smr_region_create($0, bytes, &mapping) }
    guard result == 0 else { return nil }
    return MappedRegion(mapping: mapping)
  }

  package static func open(name: String) -> MappedRegion? {
    var mapping = SMRMapping()
    let result = name.withCString { smr_region_open($0, &mapping) }
    guard result == 0 else { return nil }
    return MappedRegion(mapping: mapping)
  }

  package static func unlink(name: String) {
    _ = name.withCString { smr_region_unlink($0) }
  }

  package func initialize(bootID: UInt64, daemonPID: Int32) -> Bool {
    smr_region_initialize(baseAddress, size, bootID, daemonPID) == 1
  }

  package func isValid() -> Bool {
    smr_region_validate(baseAddress, size) == 1
  }

  package func slot(at index: Int) -> UnsafeMutableRawPointer? {
    guard index >= 0, index < Int(SMR_MAX_CLIENTS) else { return nil }
    return smr_client_slot(baseAddress, UInt32(index))
  }

  package func contains(offset: UInt64, count: UInt64) -> Bool {
    guard offset <= size, count <= size else { return false }
    return offset <= size - count
  }

  package func pointer(offset: UInt64, count: UInt64 = 0) -> UnsafeMutableRawPointer? {
    guard contains(offset: offset, count: count), offset <= UInt64(Int.max) else { return nil }
    return baseAddress.advanced(by: Int(offset))
  }
}

package enum UUIDBits {
  package static func split(_ uuid: UUID) -> (UInt64, UInt64) {
    var value = uuid.uuid
    return withUnsafeBytes(of: &value) { bytes in
      let high = bytes.loadUnaligned(fromByteOffset: 0, as: UInt64.self)
      let low = bytes.loadUnaligned(fromByteOffset: 8, as: UInt64.self)
      return (high, low)
    }
  }

  package static func join(high: UInt64, low: UInt64) -> UUID {
    var bytes = [UInt8](repeating: 0, count: 16)
    withUnsafeBytes(of: high) { bytes.replaceSubrange(0..<8, with: $0) }
    withUnsafeBytes(of: low) { bytes.replaceSubrange(8..<16, with: $0) }
    return UUID(
      uuid: (
        bytes[0], bytes[1], bytes[2], bytes[3],
        bytes[4], bytes[5], bytes[6], bytes[7],
        bytes[8], bytes[9], bytes[10], bytes[11],
        bytes[12], bytes[13], bytes[14], bytes[15]
      ))
  }
}
