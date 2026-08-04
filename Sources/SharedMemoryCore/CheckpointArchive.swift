import CSharedMemory
import Foundation

package struct CheckpointEntry: Equatable, Sendable {
  package let path: String
  package let payload: Data

  package init(path: String, payload: Data) {
    self.path = path
    self.payload = payload
  }
}

package enum CheckpointArchive {
  private static let magic = Data([0x53, 0x4d, 0x52, 0x43, 0x4b, 0x50, 0x31, 0x00])
  private static let version: UInt32 = 1

  package static func encode(_ entries: [CheckpointEntry]) -> Data? {
    guard entries.count <= Int(UInt32.max) else { return nil }
    var result = Data()
    result.reserveCapacity(
      16 + entries.reduce(0) { $0 + $1.path.utf8.count + $1.payload.count + 24 })
    result.append(magic)
    append(version, to: &result)
    append(UInt32(entries.count), to: &result)
    for entry in entries.sorted(by: { $0.path < $1.path }) {
      let path = Data(entry.path.utf8)
      guard path.count <= Int(UInt32.max) else { return nil }
      append(UInt32(path.count), to: &result)
      append(UInt32(0), to: &result)
      append(UInt64(entry.payload.count), to: &result)
      append(crc32(entry.payload), to: &result)
      append(UInt32(0), to: &result)
      result.append(path)
      result.append(entry.payload)
    }
    append(crc32(result), to: &result)
    return result
  }

  package static func decode(_ data: Data) -> [CheckpointEntry]? {
    guard data.count >= 20 else { return nil }
    let bodyCount = data.count - 4
    let expectedCRC: UInt32 = data.withUnsafeBytes { bytes in
      UInt32(littleEndian: bytes.loadUnaligned(fromByteOffset: bodyCount, as: UInt32.self))
    }
    guard crc32(data.prefix(bodyCount)) == expectedCRC else { return nil }
    guard data.prefix(8) == magic else { return nil }
    var cursor = 8
    guard read(UInt32.self, from: data, cursor: &cursor) == version,
      let count = read(UInt32.self, from: data, cursor: &cursor)
    else { return nil }
    var entries: [CheckpointEntry] = []
    entries.reserveCapacity(Int(count))
    for _ in 0..<count {
      guard
        let pathCount = read(UInt32.self, from: data, cursor: &cursor),
        read(UInt32.self, from: data, cursor: &cursor) != nil,
        let payloadCount = read(UInt64.self, from: data, cursor: &cursor),
        let payloadCRC = read(UInt32.self, from: data, cursor: &cursor),
        read(UInt32.self, from: data, cursor: &cursor) != nil,
        UInt64(pathCount) <= UInt64(Int.max),
        payloadCount <= UInt64(Int.max)
      else { return nil }
      let required = Int(pathCount) + Int(payloadCount)
      guard cursor <= bodyCount, required <= bodyCount - cursor else { return nil }
      let pathData = data[cursor..<(cursor + Int(pathCount))]
      cursor += Int(pathCount)
      let payload = Data(data[cursor..<(cursor + Int(payloadCount))])
      cursor += Int(payloadCount)
      guard let path = String(data: pathData, encoding: .utf8), crc32(payload) == payloadCRC else {
        return nil
      }
      entries.append(CheckpointEntry(path: path, payload: payload))
    }
    return cursor == bodyCount ? entries : nil
  }

  package static func write(_ entries: [CheckpointEntry], to path: String) -> Bool {
    guard let archive = encode(entries) else { return false }
    let url = URL(fileURLWithPath: path)
    do {
      let parent = url.deletingLastPathComponent()
      try FileManager.default.createDirectory(at: parent, withIntermediateDirectories: true)
      try archive.write(to: url, options: .atomic)
      return true
    } catch {
      return false
    }
  }

  private static func append<T: FixedWidthInteger>(_ value: T, to data: inout Data) {
    var littleEndian = value.littleEndian
    withUnsafeBytes(of: &littleEndian) { data.append(contentsOf: $0) }
  }

  private static func read<T: FixedWidthInteger>(_ type: T.Type, from data: Data, cursor: inout Int)
    -> T?
  {
    guard cursor <= data.count, MemoryLayout<T>.size <= data.count - cursor else { return nil }
    let value: T = data.withUnsafeBytes { bytes in
      bytes.loadUnaligned(fromByteOffset: cursor, as: T.self)
    }
    cursor += MemoryLayout<T>.size
    return T(littleEndian: value)
  }

  private static func crc32<D: DataProtocol>(_ data: D) -> UInt32 {
    let contiguous = Data(data)
    return contiguous.withUnsafeBytes { bytes in
      smr_crc32(bytes.baseAddress, UInt64(bytes.count))
    }
  }
}
