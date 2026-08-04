import Foundation

package enum BinaryCodable {
  private struct Envelope<Value: Codable>: Codable {
    let value: Value
  }

  package static func encode<T: Codable>(_ value: T) throws -> Data {
    let encoder = PropertyListEncoder()
    encoder.outputFormat = .binary
    return try encoder.encode(Envelope(value: value))
  }

  package static func decode<T: Codable>(_ type: T.Type, from bytes: UnsafeRawBufferPointer) throws
    -> T
  {
    let data = Data(
      bytesNoCopy: UnsafeMutableRawPointer(mutating: bytes.baseAddress!), count: bytes.count,
      deallocator: .none)
    return try PropertyListDecoder().decode(Envelope<T>.self, from: data).value
  }
}

package struct BatchPayload: Sendable {
  package let payloadOffset: UInt64
  package let payloadSize: UInt64
}

package enum BatchLayout {
  private static let fixedBytes = 8
  private static let lengthBytes = 8
  package static let maximumCount = 65_536

  package static func requiredBytes(for values: [Data]) -> Int? {
    guard values.count <= maximumCount else { return nil }
    var total = fixedBytes
    let (tableBytes, tableOverflow) = values.count.multipliedReportingOverflow(by: lengthBytes)
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
    _ values: [Data], to destination: UnsafeMutableRawPointer, capacity: Int
  ) -> Bool {
    guard let required = requiredBytes(for: values), required <= capacity else { return false }
    destination.storeBytes(of: UInt32(values.count).littleEndian, as: UInt32.self)
    destination.advanced(by: 4).storeBytes(of: UInt32(0), as: UInt32.self)
    var dataOffset = fixedBytes + values.count * lengthBytes
    for (index, value) in values.enumerated() {
      destination.advanced(by: fixedBytes + index * lengthBytes)
        .storeBytes(of: UInt64(value.count).littleEndian, as: UInt64.self)
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
    guard count <= maximumCount else { return nil }
    let (tableBytes, tableOverflow) = count.multipliedReportingOverflow(by: lengthBytes)
    let (dataStart, startOverflow) = fixedBytes.addingReportingOverflow(tableBytes)
    guard !tableOverflow, !startOverflow, dataStart <= Int(byteCount) else { return nil }
    var cursor = dataStart
    var result: [BatchPayload] = []
    result.reserveCapacity(count)
    for index in 0..<count {
      let rawLength = source.advanced(by: fixedBytes + index * lengthBytes).loadUnaligned(
        as: UInt64.self)
      let length = UInt64(littleEndian: rawLength)
      guard length <= UInt64(Int.max) else { return nil }
      let (end, overflow) = cursor.addingReportingOverflow(Int(length))
      guard !overflow, end <= Int(byteCount) else { return nil }
      let (offset, offsetOverflow) = absolutePayloadOffset.addingReportingOverflow(UInt64(cursor))
      guard !offsetOverflow else { return nil }
      result.append(BatchPayload(payloadOffset: offset, payloadSize: length))
      cursor = end
    }
    guard cursor == Int(byteCount) else { return nil }
    return result
  }
}
