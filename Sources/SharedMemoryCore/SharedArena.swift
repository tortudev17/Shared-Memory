import CSharedMemory
import Foundation

package struct ArenaAllocation: Equatable, Sendable {
  package let blockOffset: UInt64
  package let payloadOffset: UInt64
  package let payloadSize: UInt64
  package let capacity: UInt64
}

package final class SharedArena {
  private struct Header {
    var size: UInt64
    var previousSize: UInt64
    var nextFree: UInt64
    var previousFree: UInt64
    var payloadSize: UInt64
    var cookie: UInt64
    var references: UInt32
    var flags: UInt32
    var reserved: UInt64
  }

  private static let headerBytes: UInt64 = 64
  private static let alignment: UInt64 = 64
  private static let minimumFreeBlock: UInt64 = 128
  private static let freeFlag: UInt32 = 0x4652_4545
  private static let allocatedFlag: UInt32 = 0x414C_4C4F
  private static let binCount = 64

  private let region: MappedRegion
  private let heapStart: UInt64
  private let heapEnd: UInt64
  private let secret: UInt64
  private var freeHeads = [UInt64](repeating: 0, count: binCount)
  private(set) package var usedBytes: UInt64 = 0
  private(set) package var corruptionDetected = false

  package init?(region: MappedRegion, secret: UInt64) {
    guard MemoryLayout<Header>.size == Int(Self.headerBytes) else { return nil }
    let start = Self.alignUp(region.heapOffset)
    let end = (region.size / Self.alignment) * Self.alignment
    guard end > start, end - start >= Self.minimumFreeBlock else { return nil }
    self.region = region
    heapStart = start
    heapEnd = end
    self.secret = secret == 0 ? 0x9e37_79b9_7f4a_7c15 : secret
    writeHeader(
      at: start,
      size: end - start,
      previousSize: 0,
      nextFree: 0,
      previousFree: 0,
      payloadSize: 0,
      references: 0,
      flags: Self.freeFlag
    )
    insertFree(start)
  }

  package func allocate(payloadBytes: UInt64) -> ArenaAllocation? {
    guard !corruptionDetected else { return nil }
    let (rawRequired, overflow) = payloadBytes.addingReportingOverflow(Self.headerBytes)
    guard !overflow else { return nil }
    let required = max(Self.minimumFreeBlock, Self.alignUp(rawRequired))
    var bin = Self.binIndex(required)
    var selected: UInt64 = 0
    while bin < Self.binCount, selected == 0 {
      var candidate = freeHeads[bin]
      var visited = 0
      while candidate != 0, visited < 1_000_000 {
        guard let header = checkedHeader(at: candidate, expectedFlag: Self.freeFlag) else {
          return nil
        }
        if header.size >= required {
          selected = candidate
          break
        }
        candidate = header.nextFree
        visited += 1
      }
      if visited >= 1_000_000 {
        corruptionDetected = true
        return nil
      }
      bin += 1
    }
    guard selected != 0, var header = checkedHeader(at: selected, expectedFlag: Self.freeFlag)
    else {
      return nil
    }
    unlinkFree(selected, header: header)
    let remainder = header.size - required
    let allocatedSize: UInt64
    if remainder >= Self.minimumFreeBlock {
      allocatedSize = required
      reusePages(offset: selected, size: min(header.size, required + Self.headerBytes))
      let remainderOffset = selected + required
      writeHeader(
        at: remainderOffset,
        size: remainder,
        previousSize: required,
        nextFree: 0,
        previousFree: 0,
        payloadSize: 0,
        references: 0,
        flags: Self.freeFlag
      )
      updateFollowingPreviousSize(after: remainderOffset, blockSize: remainder)
      insertFree(remainderOffset)
      discardPages(offset: remainderOffset, size: remainder)
    } else {
      allocatedSize = header.size
      reusePages(offset: selected, size: allocatedSize)
    }
    header.size = allocatedSize
    writeHeader(
      at: selected,
      size: allocatedSize,
      previousSize: header.previousSize,
      nextFree: 0,
      previousFree: 0,
      payloadSize: payloadBytes,
      references: 1,
      flags: Self.allocatedFlag
    )
    if remainder < Self.minimumFreeBlock {
      updateFollowingPreviousSize(after: selected, blockSize: allocatedSize)
    }
    usedBytes &+= allocatedSize
    return ArenaAllocation(
      blockOffset: selected,
      payloadOffset: selected + Self.headerBytes,
      payloadSize: payloadBytes,
      capacity: allocatedSize - Self.headerBytes
    )
  }

  package func allocation(at blockOffset: UInt64) -> ArenaAllocation? {
    guard let header = checkedHeader(at: blockOffset, expectedFlag: Self.allocatedFlag) else {
      return nil
    }
    return ArenaAllocation(
      blockOffset: blockOffset,
      payloadOffset: blockOffset + Self.headerBytes,
      payloadSize: header.payloadSize,
      capacity: header.size - Self.headerBytes
    )
  }

  package func retain(_ blockOffset: UInt64) -> Bool {
    guard var header = checkedHeader(at: blockOffset, expectedFlag: Self.allocatedFlag),
      header.references < UInt32.max
    else {
      return false
    }
    header.references += 1
    store(header, at: blockOffset)
    return true
  }

  package func setReferenceCount(_ count: Int, for blockOffset: UInt64) -> Bool {
    guard count > 0, count <= Int(UInt32.max),
      var header = checkedHeader(at: blockOffset, expectedFlag: Self.allocatedFlag)
    else {
      return false
    }
    header.references = UInt32(count)
    store(header, at: blockOffset)
    return true
  }

  @discardableResult
  package func release(_ blockOffset: UInt64) -> Bool {
    guard var header = checkedHeader(at: blockOffset, expectedFlag: Self.allocatedFlag),
      header.references > 0
    else {
      return false
    }
    header.references -= 1
    if header.references > 0 {
      store(header, at: blockOffset)
      return true
    }
    usedBytes = usedBytes >= header.size ? usedBytes - header.size : 0
    var mergedOffset = blockOffset
    var mergedSize = header.size
    var previousSize = header.previousSize

    let nextOffset = blockOffset + header.size
    if nextOffset < heapEnd, let next = checkedHeader(at: nextOffset), next.flags == Self.freeFlag {
      unlinkFree(nextOffset, header: next)
      mergedSize += next.size
    }
    if previousSize > 0, previousSize <= blockOffset - heapStart {
      let previousOffset = blockOffset - previousSize
      if let previous = checkedHeader(at: previousOffset), previous.flags == Self.freeFlag {
        unlinkFree(previousOffset, header: previous)
        mergedOffset = previousOffset
        mergedSize += previous.size
        previousSize = previous.previousSize
      }
    }
    writeHeader(
      at: mergedOffset,
      size: mergedSize,
      previousSize: previousSize,
      nextFree: 0,
      previousFree: 0,
      payloadSize: 0,
      references: 0,
      flags: Self.freeFlag
    )
    updateFollowingPreviousSize(after: mergedOffset, blockSize: mergedSize)
    insertFree(mergedOffset)
    // Once the final lease is gone, the contents of complete payload pages are dead.
    // Keep the boundary-tag header resident but let the OS reclaim the large shared pages.
    discardPages(offset: mergedOffset, size: mergedSize)
    return true
  }

  private func discardPages(offset: UInt64, size: UInt64) {
    guard size > Self.headerBytes,
      let start = region.pointer(offset: offset + Self.headerBytes, count: size - Self.headerBytes)
    else { return }
    _ = smr_discard_memory(start, size - Self.headerBytes)
  }

  private func reusePages(offset: UInt64, size: UInt64) {
    guard size > 0, let start = region.pointer(offset: offset, count: size) else { return }
    _ = smr_reuse_memory(start, size)
  }

  package func validateAllBlocks() -> Bool {
    var offset = heapStart
    var expectedPrevious: UInt64 = 0
    var iterations = 0
    while offset < heapEnd, iterations < 1_000_000 {
      guard let header = checkedHeader(at: offset), header.previousSize == expectedPrevious else {
        return false
      }
      guard header.flags == Self.freeFlag || header.flags == Self.allocatedFlag else {
        return false
      }
      expectedPrevious = header.size
      offset += header.size
      iterations += 1
    }
    return offset == heapEnd && iterations < 1_000_000
  }

  package func unsafeCorruptCookieForTesting(blockOffset: UInt64) {
    guard var header = rawHeader(at: blockOffset) else { return }
    header.cookie ^= 1
    store(header, at: blockOffset)
  }

  private static func alignUp(_ value: UInt64) -> UInt64 {
    (value + alignment - 1) & ~(alignment - 1)
  }

  private static func binIndex(_ size: UInt64) -> Int {
    guard size > 0 else { return 0 }
    return min(binCount - 1, 63 - size.leadingZeroBitCount)
  }

  private func cookie(offset: UInt64, size: UInt64, flags: UInt32) -> UInt64 {
    secret ^ offset &* 0x9e37_79b9_7f4a_7c15 ^ size &* 0xbf58_476d_1ce4_e5b9 ^ UInt64(flags)
  }

  private func rawHeader(at offset: UInt64) -> Header? {
    guard offset >= heapStart, offset <= heapEnd - Self.headerBytes,
      offset % Self.alignment == 0,
      let pointer = region.pointer(offset: offset, count: Self.headerBytes)
    else { return nil }
    return pointer.load(as: Header.self)
  }

  private func checkedHeader(at offset: UInt64, expectedFlag: UInt32? = nil) -> Header? {
    guard let header = rawHeader(at: offset),
      header.size >= Self.minimumFreeBlock,
      header.size % Self.alignment == 0,
      header.size <= heapEnd - offset,
      header.cookie == cookie(offset: offset, size: header.size, flags: header.flags),
      expectedFlag == nil || header.flags == expectedFlag
    else {
      corruptionDetected = true
      return nil
    }
    return header
  }

  private func store(_ header: Header, at offset: UInt64) {
    region.pointer(offset: offset, count: Self.headerBytes)!.storeBytes(of: header, as: Header.self)
  }

  private func writeHeader(
    at offset: UInt64,
    size: UInt64,
    previousSize: UInt64,
    nextFree: UInt64,
    previousFree: UInt64,
    payloadSize: UInt64,
    references: UInt32,
    flags: UInt32
  ) {
    let header = Header(
      size: size,
      previousSize: previousSize,
      nextFree: nextFree,
      previousFree: previousFree,
      payloadSize: payloadSize,
      cookie: cookie(offset: offset, size: size, flags: flags),
      references: references,
      flags: flags,
      reserved: 0
    )
    store(header, at: offset)
  }

  private func insertFree(_ offset: UInt64) {
    guard var header = checkedHeader(at: offset, expectedFlag: Self.freeFlag) else { return }
    let bin = Self.binIndex(header.size)
    let oldHead = freeHeads[bin]
    header.previousFree = 0
    header.nextFree = oldHead
    store(header, at: offset)
    if oldHead != 0, var old = checkedHeader(at: oldHead, expectedFlag: Self.freeFlag) {
      old.previousFree = offset
      store(old, at: oldHead)
    }
    freeHeads[bin] = offset
  }

  private func unlinkFree(_ offset: UInt64, header: Header) {
    let bin = Self.binIndex(header.size)
    if header.previousFree == 0 {
      if freeHeads[bin] == offset {
        freeHeads[bin] = header.nextFree
      } else {
        corruptionDetected = true
      }
    } else if var previous = checkedHeader(at: header.previousFree, expectedFlag: Self.freeFlag) {
      previous.nextFree = header.nextFree
      store(previous, at: header.previousFree)
    }
    if header.nextFree != 0,
      var next = checkedHeader(at: header.nextFree, expectedFlag: Self.freeFlag)
    {
      next.previousFree = header.previousFree
      store(next, at: header.nextFree)
    }
  }

  private func updateFollowingPreviousSize(after offset: UInt64, blockSize: UInt64) {
    let followingOffset = offset + blockSize
    guard followingOffset < heapEnd, var following = rawHeader(at: followingOffset) else { return }
    following.previousSize = blockSize
    store(following, at: followingOffset)
  }
}
