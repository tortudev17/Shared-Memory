import Foundation

package enum BinaryCodable {
  private struct Envelope<Value: Codable>: Codable {
    let value: Value
  }

  package static func encode<T: Codable>(_ value: T) throws -> Data {
    try withRuntimeAutoreleasePool {
      let encoder = PropertyListEncoder()
      encoder.outputFormat = .binary
      return try encoder.encode(Envelope(value: value))
    }
  }

  package static func decode<T: Codable>(_ type: T.Type, from bytes: UnsafeRawBufferPointer) throws
    -> T
  {
    return try withRuntimeAutoreleasePool {
      let data = Data(
        bytesNoCopy: UnsafeMutableRawPointer(mutating: bytes.baseAddress!), count: bytes.count,
        deallocator: .none)
      return try PropertyListDecoder().decode(Envelope<T>.self, from: data).value
    }
  }
}

package struct BatchPayload: Sendable {
  package let payloadOffset: UInt64
  package let payloadSize: UInt64
  package let uuid: UUID?
}

package enum BatchLayout {
  private static let fixedBytes = 8
  private static let lengthBytes = 8
  private static let uuidBytes = 16
  private static let hasUUIDsFlag: UInt32 = 1
  package static let maximumCount = 65_536

  package static func requiredBytes(for values: [Data], uuids: [UUID]? = nil) -> Int? {
    guard values.count <= maximumCount, uuids == nil || uuids?.count == values.count else {
      return nil
    }
    var total = fixedBytes
    let descriptorBytes = lengthBytes + (uuids == nil ? 0 : uuidBytes)
    let (tableBytes, tableOverflow) = values.count.multipliedReportingOverflow(
      by: descriptorBytes)
    let (withTable, headerOverflow) = total.addingReportingOverflow(tableBytes)
    guard !tableOverflow, !headerOverflow else { return nil }
    total = withTable
    for value in values {
      let (next, overflow) = total.addingReportingOverflow(value.count)
      guard !overflow else { return nil }
      total = next
    }
    return total
  }

  package static func write(
    _ values: [Data], uuids: [UUID]? = nil, to destination: UnsafeMutableRawPointer, capacity: Int
  ) -> Bool {
    guard let required = requiredBytes(for: values, uuids: uuids), required <= capacity else {
      return false
    }
    let descriptorBytes = lengthBytes + (uuids == nil ? 0 : uuidBytes)
    destination.storeBytes(of: UInt32(values.count).littleEndian, as: UInt32.self)
    destination.advanced(by: 4).storeBytes(
      of: (uuids == nil ? UInt32(0) : hasUUIDsFlag).littleEndian, as: UInt32.self)
    var dataOffset = fixedBytes + values.count * descriptorBytes
    for (index, value) in values.enumerated() {
      let descriptor = destination.advanced(by: fixedBytes + index * descriptorBytes)
      descriptor
        .storeBytes(of: UInt64(value.count).littleEndian, as: UInt64.self)
      if let uuid = uuids?[index] {
        let bits = UUIDBits.split(uuid)
        descriptor.advanced(by: lengthBytes).storeBytes(
          of: bits.0.littleEndian, as: UInt64.self)
        descriptor.advanced(by: lengthBytes + 8).storeBytes(
          of: bits.1.littleEndian, as: UInt64.self)
      }
      value.withUnsafeBytes { bytes in
        if let source = bytes.baseAddress, !bytes.isEmpty {
          destination.advanced(by: dataOffset).copyMemory(from: source, byteCount: bytes.count)
        }
      }
      dataOffset += value.count
    }
    return true
  }

  package static func parse(
    at source: UnsafeRawPointer,
    byteCount: UInt64,
    absolutePayloadOffset: UInt64
  ) -> [BatchPayload]? {
    guard byteCount >= UInt64(fixedBytes), byteCount <= UInt64(Int.max) else { return nil }
    let count = Int(UInt32(littleEndian: source.loadUnaligned(as: UInt32.self)))
    let flags = UInt32(
      littleEndian: source.advanced(by: 4).loadUnaligned(as: UInt32.self))
    guard count <= maximumCount, flags == 0 || flags == hasUUIDsFlag else { return nil }
    let descriptorBytes = lengthBytes + (flags == hasUUIDsFlag ? uuidBytes : 0)
    let (tableBytes, tableOverflow) = count.multipliedReportingOverflow(by: descriptorBytes)
    let (dataStart, startOverflow) = fixedBytes.addingReportingOverflow(tableBytes)
    guard !tableOverflow, !startOverflow, dataStart <= Int(byteCount) else { return nil }
    var cursor = dataStart
    var result: [BatchPayload] = []
    result.reserveCapacity(count)
    for index in 0..<count {
      let descriptor = source.advanced(by: fixedBytes + index * descriptorBytes)
      let rawLength = descriptor.loadUnaligned(as: UInt64.self)
      let length = UInt64(littleEndian: rawLength)
      guard length <= UInt64(Int.max) else { return nil }
      let (end, overflow) = cursor.addingReportingOverflow(Int(length))
      guard !overflow, end <= Int(byteCount) else { return nil }
      let (offset, offsetOverflow) = absolutePayloadOffset.addingReportingOverflow(UInt64(cursor))
      guard !offsetOverflow else { return nil }
      let uuid: UUID?
      if flags == hasUUIDsFlag {
        let high = UInt64(
          littleEndian: descriptor.advanced(by: lengthBytes).loadUnaligned(as: UInt64.self))
        let low = UInt64(
          littleEndian: descriptor.advanced(by: lengthBytes + 8).loadUnaligned(as: UInt64.self))
        uuid = UUIDBits.join(high: high, low: low)
      } else {
        uuid = nil
      }
      result.append(BatchPayload(payloadOffset: offset, payloadSize: length, uuid: uuid))
      cursor = end
    }
    guard cursor == Int(byteCount) else { return nil }
    return result
  }
}
