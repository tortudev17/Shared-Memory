import CSharedMemory
import Foundation
#if canImport(Darwin)
import Darwin
#else
import Glibc
#endif

package struct CheckpointEntry: Equatable, Sendable {
  package let path: String
  package let payload: Data

  package init(path: String, payload: Data) {
    self.path = path
    self.payload = payload
  }
}

/// A borrowed filesystem value that remains owned by the shared arena for the
/// duration of a synchronous checkpoint request.
package struct MappedCheckpointEntry {
  package let path: String
  package let payload: UnsafeRawBufferPointer

  package init(path: String, payload: UnsafeRawBufferPointer) {
    self.path = path
    self.payload = payload
  }
}

package enum CheckpointArchive {
  private static let magic = Data([0x53, 0x4d, 0x52, 0x43, 0x4b, 0x50, 0x31, 0x00])
  private static let version: UInt32 = 1
  // FileHandle cannot reliably bridge a single multi-gigabyte Data value on
  // every supported Foundation version. Bounded no-copy views preserve the
  // exact archive bytes while keeping each write comfortably below that API
  // boundary.
  private static let mappedWriteChunkBytes = 8 * 1024 * 1024

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

  /// Writes the same archive format as `encode`, but streams borrowed arena
  /// payloads directly to a same-directory temporary file. This keeps a model
  /// checkpoint atomic without materializing a second multi-gigabyte archive.
  package static func writeMapped(_ entries: [MappedCheckpointEntry], to path: String) -> Bool {
    guard entries.count <= Int(UInt32.max) else { return false }
    let destination = URL(fileURLWithPath: path)
    let parent = destination.deletingLastPathComponent()
    let temporary = parent.appendingPathComponent(
      ".\(destination.lastPathComponent).\(UUID().uuidString).tmp")
    let manager = FileManager.default
    var handle: FileHandle?
    do {
      try manager.createDirectory(at: parent, withIntermediateDirectories: true)
      guard manager.createFile(atPath: temporary.path, contents: nil) else { return false }
      let opened = try FileHandle(forWritingTo: temporary)
      handle = opened
      var archiveCRC: UInt32 = 0

      func emit(_ data: Data, includeInCRC: Bool = true) throws {
        try opened.write(contentsOf: data)
        if includeInCRC {
          archiveCRC = data.withUnsafeBytes { bytes in
            smr_crc32_extend(archiveCRC, bytes.baseAddress, UInt64(bytes.count))
          }
        }
      }

      var header = Data()
      header.append(magic)
      append(version, to: &header)
      append(UInt32(entries.count), to: &header)
      try emit(header)

      for entry in entries.sorted(by: { $0.path < $1.path }) {
        let pathData = Data(entry.path.utf8)
        guard pathData.count <= Int(UInt32.max) else { throw CheckpointWriteError.invalidEntry }
        var payloadCRC: UInt32 = 0
        if let baseAddress = entry.payload.baseAddress {
          var cursor = 0
          while cursor < entry.payload.count {
            let count = min(mappedWriteChunkBytes, entry.payload.count - cursor)
            payloadCRC = smr_crc32_extend(
              payloadCRC, baseAddress.advanced(by: cursor), UInt64(count))
            cursor += count
          }
        } else if entry.payload.count != 0 {
          throw CheckpointWriteError.invalidEntry
        }
        var entryHeader = Data()
        append(UInt32(pathData.count), to: &entryHeader)
        append(UInt32(0), to: &entryHeader)
        append(UInt64(entry.payload.count), to: &entryHeader)
        append(payloadCRC, to: &entryHeader)
        append(UInt32(0), to: &entryHeader)
        try emit(entryHeader)
        try emit(pathData)
        if entry.payload.count > 0 {
          guard let baseAddress = entry.payload.baseAddress else {
            throw CheckpointWriteError.invalidEntry
          }
          var cursor = 0
          while cursor < entry.payload.count {
            let count = min(mappedWriteChunkBytes, entry.payload.count - cursor)
            let chunkAddress = baseAddress.advanced(by: cursor)
            let borrowed = Data(
              bytesNoCopy: UnsafeMutableRawPointer(mutating: chunkAddress),
              count: count,
              deallocator: .none)
            try opened.write(contentsOf: borrowed)
            archiveCRC = smr_crc32_extend(archiveCRC, chunkAddress, UInt64(count))
            cursor += count
          }
        }
      }
      var footer = Data()
      append(archiveCRC, to: &footer)
      try emit(footer, includeInCRC: false)
      try opened.synchronize()
      try opened.close()
      handle = nil

      let renamed = temporary.path.withCString { source in
        destination.path.withCString { target in rename(source, target) }
      }
      guard renamed == 0 else { throw CheckpointWriteError.renameFailed }
      return true
    } catch {
      try? handle?.close()
      try? manager.removeItem(at: temporary)
      return false
    }
  }

  private enum CheckpointWriteError: Error {
    case invalidEntry
    case renameFailed
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

  // Prefer this overload for the very large Data values produced by model
  // checkpoints. The generic DataProtocol form must make a contiguous copy;
  // Data is already contiguous and can be checksummed in place.
  private static func crc32(_ data: Data) -> UInt32 {
    data.withUnsafeBytes { bytes in
      smr_crc32(bytes.baseAddress, UInt64(bytes.count))
    }
  }
}
