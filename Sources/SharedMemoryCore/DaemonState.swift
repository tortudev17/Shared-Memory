import CSharedMemory
import Foundation

private final class FileRecord {
  let allocationOffset: UInt64
  let payloadOffset: UInt64
  let payloadSize: UInt64
  let version: UInt64

  init(allocationOffset: UInt64, payloadOffset: UInt64, payloadSize: UInt64, version: UInt64) {
    self.allocationOffset = allocationOffset
    self.payloadOffset = payloadOffset
    self.payloadSize = payloadSize
    self.version = version
  }
}

private final class ItemRecord {
  let uuid: UUID
  var owner: String
  var pipelineIndex: Int
  var stageIndex: Int
  var allocationOffset: UInt64
  var payloadOffset: UInt64
  var payloadSize: UInt64
  var previous: UUID?
  var next: UUID?
  var deliveredGeneration: UInt64 = 0

  init(
    uuid: UUID,
    owner: String,
    pipelineIndex: Int,
    stageIndex: Int,
    allocationOffset: UInt64,
    payloadOffset: UInt64,
    payloadSize: UInt64
  ) {
    self.uuid = uuid
    self.owner = owner
    self.pipelineIndex = pipelineIndex
    self.stageIndex = stageIndex
    self.allocationOffset = allocationOffset
    self.payloadOffset = payloadOffset
    self.payloadSize = payloadSize
  }
}

private struct Bucket {
  var head: UUID?
  var tail: UUID?
  var count = 0
}

private struct PendingNotification {
  let subscription: UInt64
}

private final class ConnectionRecord {
  let slotIndex: Int
  let generation: UInt64
  let name: String
  let pid: Int32
  var subscriptions: [UInt64: String] = [:]
  var nextSubscription: UInt64 = 1
  var notifications: [PendingNotification] = []
  var notificationHead = 0
  var receiveCursor: UUID?
  var leases: [UInt64: Int] = [:]
  var stagingBlocks: Set<UInt64> = []

  init(slotIndex: Int, generation: UInt64, name: String, pid: Int32) {
    self.slotIndex = slotIndex
    self.generation = generation
    self.name = name
    self.pid = pid
  }

  var pendingNotificationCount: Int { notifications.count - notificationHead }

  func compactNotificationsIfNeeded() {
    if notificationHead > 1_024, notificationHead * 2 > notifications.count {
      notifications.removeFirst(notificationHead)
      notificationHead = 0
    }
  }
}

package final class DaemonState {
  private static let maximumNotificationBacklog = 65_536

  package let region: MappedRegion
  package let arena: SharedArena
  private var pipelines: [[String]]
  private var stagePositions: [String: (pipeline: Int, stage: Int)] = [:]
  private var files: [String: FileRecord] = [:]
  private var items: [UUID: ItemRecord] = [:]
  private var buckets: [String: Bucket] = [:]
  private var connections = [ConnectionRecord?](repeating: nil, count: Int(SMR_MAX_CLIENTS))
  private var connectionsByName: [String: ConnectionRecord] = [:]
  private var knownNames = Set<String>()
  private var nextFileVersion: UInt64 = 1

  package init?(region: MappedRegion, configuration: PipelineConfiguration) {
    guard let validated = configuration.validated(),
      let arena = SharedArena(region: region, secret: region.bootID)
    else { return nil }
    self.region = region
    self.arena = arena
    pipelines = []
    installPipelines(validated.pipelines)
  }

  package func handle(slotIndex: Int, slot: UnsafeMutableRawPointer, request: SMRRequest)
    -> SMRResponse
  {
    var response = SMRResponse()
    response.sequence = request.sequence
    response.status = 0

    guard let command = RuntimeCommand(rawValue: request.opcode) else { return response }
    if command == .register {
      return register(slotIndex: slotIndex, slot: slot, sequence: request.sequence)
    }
    guard
      let connection = connections[slotIndex],
      connection.generation == smr_slot_generation(slot),
      smr_slot_state(slot) == SMR_SLOT_ACTIVE
    else { return response }

    switch command {
    case .register:
      break
    case .unregister:
      cleanup(connection: connection)
      response.status = 1
    case .allocate:
      if let allocation = arena.allocate(payloadBytes: request.arg0) {
        connection.stagingBlocks.insert(allocation.blockOffset)
        response.status = 1
        response.value0 = allocation.blockOffset
        response.value1 = allocation.payloadOffset
        response.value2 = allocation.capacity
      }
    case .abandon:
      if connection.stagingBlocks.remove(request.arg0) != nil, arena.release(request.arg0) {
        response.status = 1
      }
    case .write:
      response.status = write(connection: connection, request: request) ? 1 : 0
    case .read:
      if let path = requestPath(request), let record = files[path],
        arena.retain(record.allocationOffset)
      {
        connection.leases[record.allocationOffset, default: 0] += 1
        response.status = 1
        response.value0 = record.allocationOffset
        response.value1 = record.payloadOffset
        response.value2 = record.payloadSize
        response.value3 = record.version
      }
    case .pass:
      response.status =
        transfer(connection: connection, request: request, directTarget: nil) ? 1 : 0
    case .send:
      guard let target = requestTarget(request) else { break }
      response.status =
        transfer(connection: connection, request: request, directTarget: target) ? 1 : 0
    case .finish:
      let uuid = UUIDBits.join(high: request.arg0, low: request.arg1)
      if let item = items[uuid], item.owner == connection.name {
        remove(item)
        _ = arena.release(item.allocationOffset)
        response.status = 1
      }
    case .memoryUsed:
      response.status = 1
      response.value0 = arena.usedBytes
    case .subscribe:
      guard let path = requestPath(request) else { break }
      let token = connection.nextSubscription
      connection.nextSubscription &+= 1
      connection.subscriptions[token] = path
      response.status = 1
      response.value0 = token
    case .releaseLease:
      if let count = connection.leases[request.arg0], count > 0 {
        if count == 1 {
          connection.leases.removeValue(forKey: request.arg0)
        } else {
          connection.leases[request.arg0] = count - 1
        }
        response.status = arena.release(request.arg0) ? 1 : 0
      }
    case .checkpoint:
      guard let diskPath = rawRequestPath(request), diskPath.first == "/" else { break }
      response.status = checkpoint(to: diskPath) ? 1 : 0
    case .ping:
      response.status = 1
      response.value0 = region.bootID
    case .configure:
      response.status = configure(connection: connection, request: request) ? 1 : 0
    case .list:
      guard let prefix = requestPrefix(request), let encoded = list(prefix: prefix),
        let allocation = arena.allocate(payloadBytes: UInt64(encoded.count)),
        let pointer = region.pointer(offset: allocation.payloadOffset, count: UInt64(encoded.count))
      else { break }
      encoded.copyBytes(to: pointer.assumingMemoryBound(to: UInt8.self), count: encoded.count)
      connection.leases[allocation.blockOffset, default: 0] += 1
      response.status = 1
      response.value0 = allocation.blockOffset
      response.value1 = allocation.payloadOffset
      response.value2 = UInt64(encoded.count)
    case .delete:
      let expected = request.flags & 1 == 1 ? request.arg0 : nil
      response.status = delete(request: request, expectedVersion: expected) ? 1 : 0
    case .transaction:
      response.status = transaction(connection: connection, request: request) ? 1 : 0
    }
    return response
  }

  package func pumpEvents() -> Bool {
    var didWork = false
    for connection in connections.compactMap({ $0 }) {
      guard let slot = region.slot(at: connection.slotIndex),
        smr_slot_state(slot) == SMR_SLOT_ACTIVE
      else {
        continue
      }
      while connection.notificationHead < connection.notifications.count {
        var event = SMREvent()
        event.kind = RuntimeEventKind.notification.rawValue
        event.generation = connection.generation
        event.value = connection.notifications[connection.notificationHead].subscription
        guard smr_daemon_push_event(slot, &event) == 1 else { break }
        connection.notificationHead += 1
        didWork = true
      }
      connection.compactNotificationsIfNeeded()

      while let uuid = connection.receiveCursor {
        guard let item = items[uuid], item.owner == connection.name else {
          connection.receiveCursor = buckets[connection.name]?.head
          continue
        }
        if item.deliveredGeneration == connection.generation {
          connection.receiveCursor = item.next
          continue
        }
        guard arena.retain(item.allocationOffset) else { break }
        let bits = UUIDBits.split(item.uuid)
        var event = SMREvent()
        event.kind = RuntimeEventKind.receive.rawValue
        event.generation = connection.generation
        event.allocation_offset = item.allocationOffset
        event.payload_offset = item.payloadOffset
        event.payload_size = item.payloadSize
        event.uuid_high = bits.0
        event.uuid_low = bits.1
        guard smr_daemon_push_event(slot, &event) == 1 else {
          _ = arena.release(item.allocationOffset)
          break
        }
        connection.leases[item.allocationOffset, default: 0] += 1
        item.deliveredGeneration = connection.generation
        connection.receiveCursor = item.next
        didWork = true
      }
    }
    return didWork
  }

  package func reapDeadClients() -> Bool {
    var didWork = false
    for index in connections.indices {
      if let connection = connections[index], smr_pid_is_alive(connection.pid) == 0 {
        cleanup(connection: connection)
        if let slot = region.slot(at: index) { smr_slot_reset(slot) }
        didWork = true
      } else if connections[index] == nil,
        let slot = region.slot(at: index),
        smr_slot_state(slot) == SMR_SLOT_CLAIMING,
        smr_pid_is_alive(smr_slot_pid(slot)) == 0
      {
        smr_slot_reset(slot)
        didWork = true
      }
    }
    return didWork
  }

  package func connectionCount() -> Int {
    connectionsByName.count
  }

  package func itemCount() -> Int { items.count }

  private func register(slotIndex: Int, slot: UnsafeMutableRawPointer, sequence: UInt64)
    -> SMRResponse
  {
    var response = SMRResponse()
    response.sequence = sequence
    response.status = 0
    guard connections[slotIndex] == nil else { return response }
    let name = String(cString: smr_slot_name(slot))
    let pid = smr_slot_pid(slot)
    let generation = smr_slot_generation(slot)
    guard RuntimeValidation.validName(name), pid > 0, generation != 0,
      connectionsByName[name] == nil
    else {
      return response
    }
    let connection = ConnectionRecord(
      slotIndex: slotIndex, generation: generation, name: name, pid: pid)
    connection.receiveCursor = buckets[name]?.head
    connections[slotIndex] = connection
    connectionsByName[name] = connection
    knownNames.insert(name)
    if buckets[name] == nil { buckets[name] = Bucket() }
    smr_slot_activate(slot)
    response.status = 1
    response.value0 = region.bootID
    return response
  }

  private func cleanup(connection: ConnectionRecord) {
    guard connections[connection.slotIndex] === connection else { return }
    for (block, count) in connection.leases {
      for _ in 0..<count { _ = arena.release(block) }
    }
    for block in connection.stagingBlocks { _ = arena.release(block) }
    connections[connection.slotIndex] = nil
    if connectionsByName[connection.name] === connection {
      connectionsByName.removeValue(forKey: connection.name)
    }
  }

  private func write(connection: ConnectionRecord, request: SMRRequest) -> Bool {
    guard let path = requestPath(request),
      let allocation = takeStaging(
        connection: connection, blockOffset: request.arg0, byteCount: request.arg1)
    else { return false }
    let subscribers = connections.compactMap { candidate -> (ConnectionRecord, Int)? in
      guard let candidate else { return nil }
      let matches = candidate.subscriptions.values.lazy.filter { $0 == path }.count
      return matches == 0 ? nil : (candidate, matches)
    }
    guard
      subscribers.allSatisfy({ pair in
        pair.0.pendingNotificationCount <= Self.maximumNotificationBacklog - pair.1
      })
    else {
      _ = arena.release(allocation.blockOffset)
      return false
    }
    let previous = files.updateValue(
      FileRecord(
        allocationOffset: allocation.blockOffset,
        payloadOffset: allocation.payloadOffset,
        payloadSize: request.arg1,
        version: takeFileVersion()
      ),
      forKey: path
    )
    if let previous { _ = arena.release(previous.allocationOffset) }
    for (subscriber, _) in subscribers {
      for (token, subscribedPath) in subscriber.subscriptions where subscribedPath == path {
        subscriber.notifications.append(PendingNotification(subscription: token))
      }
    }
    return true
  }

  private func takeFileVersion() -> UInt64 {
    let result = nextFileVersion
    nextFileVersion = nextFileVersion == UInt64.max ? 1 : nextFileVersion + 1
    return result
  }

  private func list(prefix: String) -> Data? {
    let childPrefix = prefix == "/" ? "/" : prefix + "/"
    let entries = files.compactMap { path, record -> FilesystemPathEntry? in
      guard prefix == "/" || path == prefix || path.hasPrefix(childPrefix) else { return nil }
      return FilesystemPathEntry(path: path, version: record.version)
    }.sorted { $0.path < $1.path }
    return try? BinaryCodable.encode(entries)
  }

  private func delete(request: SMRRequest, expectedVersion: UInt64?) -> Bool {
    guard let path = requestPath(request), let record = files[path],
      expectedVersion == nil || expectedVersion == record.version,
      canNotify(paths: [path])
    else { return false }
    files.removeValue(forKey: path)
    _ = arena.release(record.allocationOffset)
    enqueueNotifications(paths: [path])
    return true
  }

  private func transaction(connection: ConnectionRecord, request: SMRRequest) -> Bool {
    guard let staging = takeStaging(
      connection: connection, blockOffset: request.arg0, byteCount: request.arg1),
      request.arg1 <= UInt64(Int.max),
      let pointer = region.pointer(offset: staging.payloadOffset, count: request.arg1)
    else {
      abandonStagingIfPresent(connection: connection, blockOffset: request.arg0)
      return false
    }
    defer { _ = arena.release(staging.blockOffset) }
    let bytes = UnsafeRawBufferPointer(start: pointer, count: Int(request.arg1))
    guard let value = try? BinaryCodable.decode(FilesystemTransaction.self, from: bytes),
      !value.mutations.isEmpty
    else { return false }

    var normalized: [(FilesystemMutation, String)] = []
    normalized.reserveCapacity(value.mutations.count)
    var seen = Set<String>()
    var stagedBlocks = Set<UInt64>()
    for mutation in value.mutations {
      guard let path = RuntimeValidation.normalize(path: mutation.path), path != "/",
        seen.insert(path).inserted
      else { return false }
      switch mutation.kind {
      case .write:
        let hasInline = mutation.payload != nil
        let hasStaged = mutation.stagedBlock != nil || mutation.payloadSize != nil
        guard hasInline != hasStaged else { return false }
        if hasStaged {
          guard let block = mutation.stagedBlock, let payloadSize = mutation.payloadSize,
            stagedBlocks.insert(block).inserted,
            connection.stagingBlocks.contains(block),
            let allocation = arena.allocation(at: block),
            allocation.payloadSize == payloadSize,
            payloadSize <= allocation.capacity
          else { return false }
        }
      case .delete:
        guard mutation.payload == nil, mutation.stagedBlock == nil,
          mutation.payloadSize == nil, files[path] != nil
        else { return false }
      }
      if let expected = mutation.expectedVersion {
        if expected == 0 {
          guard files[path] == nil else { return false }
        } else {
          guard files[path]?.version == expected else { return false }
        }
      }
      normalized.append((mutation, path))
    }
    let paths = normalized.map(\.1)
    guard canNotify(paths: paths) else { return false }

    var allocations: [String: ArenaAllocation] = [:]
    var directAllocations = Set<UInt64>()
    for (mutation, path) in normalized where mutation.kind == .write {
      if let payload = mutation.payload {
        guard let allocation = arena.allocate(payloadBytes: UInt64(payload.count)),
          let destination = region.pointer(
            offset: allocation.payloadOffset, count: UInt64(payload.count))
        else {
          for block in directAllocations { _ = arena.release(block) }
          return false
        }
        payload.copyBytes(to: destination.assumingMemoryBound(to: UInt8.self), count: payload.count)
        allocations[path] = allocation
        directAllocations.insert(allocation.blockOffset)
      } else if let block = mutation.stagedBlock, let allocation = arena.allocation(at: block) {
        allocations[path] = allocation
      }
    }
    // Validation above guarantees ownership and uniqueness. Transfer staged
    // allocations out of the connection only after every possible failure.
    for block in stagedBlocks {
      guard connection.stagingBlocks.remove(block) != nil else {
        for directBlock in directAllocations { _ = arena.release(directBlock) }
        return false
      }
    }

    var retired: [FileRecord] = []
    for (mutation, path) in normalized {
      if let previous = files.removeValue(forKey: path) { retired.append(previous) }
      if mutation.kind == .write, let allocation = allocations[path] {
        let payloadCount = mutation.payload.map { UInt64($0.count) } ?? mutation.payloadSize ?? 0
        files[path] = FileRecord(
          allocationOffset: allocation.blockOffset,
          payloadOffset: allocation.payloadOffset,
          payloadSize: payloadCount,
          version: takeFileVersion())
      }
    }
    for record in retired { _ = arena.release(record.allocationOffset) }
    enqueueNotifications(paths: paths)
    return true
  }

  private func canNotify(paths: [String]) -> Bool {
    for connection in connections.compactMap({ $0 }) {
      let additions = paths.reduce(0) { count, path in
        count + connection.subscriptions.values.lazy.filter { $0 == path }.count
      }
      if connection.pendingNotificationCount > Self.maximumNotificationBacklog - additions {
        return false
      }
    }
    return true
  }

  private func enqueueNotifications(paths: [String]) {
    for connection in connections.compactMap({ $0 }) {
      for path in paths {
        for (token, subscribedPath) in connection.subscriptions where subscribedPath == path {
          connection.notifications.append(PendingNotification(subscription: token))
        }
      }
    }
  }

  private func configure(connection: ConnectionRecord, request: SMRRequest) -> Bool {
    guard
      let allocation = takeStaging(
        connection: connection, blockOffset: request.arg0, byteCount: request.arg1)
    else {
      abandonStagingIfPresent(connection: connection, blockOffset: request.arg0)
      return false
    }
    guard request.arg1 <= UInt64(Int.max),
      let pointer = region.pointer(offset: allocation.payloadOffset, count: request.arg1)
    else {
      _ = arena.release(allocation.blockOffset)
      return false
    }
    defer { _ = arena.release(allocation.blockOffset) }
    let data = Data(bytesNoCopy: pointer, count: Int(request.arg1), deallocator: .none)
    guard let configuration = PipelineConfiguration.decode(data: data) else { return false }
    if configuration.pipelines == pipelines { return true }
    guard pipelines.isEmpty, items.isEmpty else { return false }
    installPipelines(configuration.pipelines)
    return true
  }

  private func transfer(connection: ConnectionRecord, request: SMRRequest, directTarget: String?)
    -> Bool
  {
    let destination: String
    let position: (pipeline: Int, stage: Int)
    if let directTarget {
      guard RuntimeValidation.validTarget(directTarget), knownNames.contains(directTarget) else {
        abandonStagingIfPresent(connection: connection, blockOffset: request.arg0)
        return false
      }
      destination = directTarget
      position = stagePositions[destination] ?? (pipeline: -1, stage: -1)
    } else {
      guard let current = stagePositions[connection.name],
        current.stage + 1 < pipelines[current.pipeline].count
      else {
        abandonStagingIfPresent(connection: connection, blockOffset: request.arg0)
        return false
      }
      destination = pipelines[current.pipeline][current.stage + 1]
      position = (current.pipeline, current.stage + 1)
    }
    guard
      let allocation = takeStaging(
        connection: connection, blockOffset: request.arg0, byteCount: request.arg1)
    else {
      abandonStagingIfPresent(connection: connection, blockOffset: request.arg0)
      return false
    }
    guard let pointer = region.pointer(offset: allocation.payloadOffset, count: request.arg1),
      let payloads = BatchLayout.parse(
        at: UnsafeRawPointer(pointer),
        byteCount: request.arg1,
        absolutePayloadOffset: allocation.payloadOffset
      )
    else {
      _ = arena.release(allocation.blockOffset)
      return false
    }
    if payloads.isEmpty {
      _ = arena.release(allocation.blockOffset)
      return true
    }
    let advancing: [UUID]
    if directTarget == nil {
      let uuids = payloads.compactMap(\.uuid)
      guard uuids.count == payloads.count, Set(uuids).count == uuids.count,
        uuids.allSatisfy({ uuid in
          guard let item = items[uuid] else { return false }
          return item.owner == connection.name
            && item.deliveredGeneration == connection.generation
        })
      else {
        _ = arena.release(allocation.blockOffset)
        return false
      }
      advancing = uuids
    } else {
      guard payloads.allSatisfy({ $0.uuid == nil }) else {
        _ = arena.release(allocation.blockOffset)
        return false
      }
      advancing = deliveredItems(
        in: connection.name, generation: connection.generation, limit: payloads.count)
    }
    var reusesExistingPayload = [Bool](repeating: false, count: payloads.count)
    for index in 0..<min(advancing.count, payloads.count) {
      guard let item = items[advancing[index]], item.payloadSize == payloads[index].payloadSize,
        let oldBytes = region.pointer(offset: item.payloadOffset, count: item.payloadSize),
        let newBytes = region.pointer(
          offset: payloads[index].payloadOffset, count: payloads[index].payloadSize)
      else { continue }
      reusesExistingPayload[index] = smr_bytes_equal(oldBytes, newBytes, item.payloadSize) == 1
    }
    let newPayloadReferences = payloads.count - reusesExistingPayload.lazy.filter({ $0 }).count
    if newPayloadReferences == 0 {
      _ = arena.release(allocation.blockOffset)
    } else if !arena.setReferenceCount(newPayloadReferences, for: allocation.blockOffset) {
      _ = arena.release(allocation.blockOffset)
      return false
    }
    for index in payloads.indices {
      let payload = payloads[index]
      if index < advancing.count, let item = items[advancing[index]] {
        remove(item)
        item.owner = destination
        item.pipelineIndex = position.pipeline
        item.stageIndex = position.stage
        if !reusesExistingPayload[index] {
          _ = arena.release(item.allocationOffset)
          item.allocationOffset = allocation.blockOffset
          item.payloadOffset = payload.payloadOffset
          item.payloadSize = payload.payloadSize
        }
        item.deliveredGeneration = 0
        items[item.uuid] = item
        append(item, to: destination)
      } else {
        var uuid = UUID()
        while items[uuid] != nil { uuid = UUID() }
        let item = ItemRecord(
          uuid: uuid,
          owner: destination,
          pipelineIndex: position.pipeline,
          stageIndex: position.stage,
          allocationOffset: allocation.blockOffset,
          payloadOffset: payload.payloadOffset,
          payloadSize: payload.payloadSize
        )
        items[uuid] = item
        append(item, to: destination)
      }
    }
    return true
  }

  private func takeStaging(connection: ConnectionRecord, blockOffset: UInt64, byteCount: UInt64)
    -> ArenaAllocation?
  {
    guard connection.stagingBlocks.remove(blockOffset) != nil,
      let allocation = arena.allocation(at: blockOffset),
      byteCount == allocation.payloadSize,
      byteCount <= allocation.capacity
    else { return nil }
    return allocation
  }

  private func abandonStagingIfPresent(connection: ConnectionRecord, blockOffset: UInt64) {
    if connection.stagingBlocks.remove(blockOffset) != nil { _ = arena.release(blockOffset) }
  }

  private func deliveredItems(in bucketName: String, generation: UInt64, limit: Int) -> [UUID] {
    var result: [UUID] = []
    result.reserveCapacity(limit)
    var cursor = buckets[bucketName]?.head
    while let uuid = cursor, result.count < limit, let item = items[uuid] {
      if item.deliveredGeneration == generation { result.append(uuid) }
      cursor = item.next
    }
    return result
  }

  private func append(_ item: ItemRecord, to bucketName: String) {
    var bucket = buckets[bucketName] ?? Bucket()
    item.previous = bucket.tail
    item.next = nil
    if let tail = bucket.tail { items[tail]?.next = item.uuid } else { bucket.head = item.uuid }
    bucket.tail = item.uuid
    bucket.count += 1
    buckets[bucketName] = bucket
    if let destination = connectionsByName[bucketName], destination.receiveCursor == nil {
      destination.receiveCursor = item.uuid
    }
  }

  private func remove(_ item: ItemRecord) {
    var bucket = buckets[item.owner] ?? Bucket()
    if let previous = item.previous {
      items[previous]?.next = item.next
    } else {
      bucket.head = item.next
    }
    if let next = item.next {
      items[next]?.previous = item.previous
    } else {
      bucket.tail = item.previous
    }
    bucket.count = max(0, bucket.count - 1)
    buckets[item.owner] = bucket
    if let connection = connectionsByName[item.owner], connection.receiveCursor == item.uuid {
      connection.receiveCursor = item.next
    }
    item.previous = nil
    item.next = nil
    items.removeValue(forKey: item.uuid)
  }

  private func checkpoint(to diskPath: String) -> Bool {
    var entries: [MappedCheckpointEntry] = []
    entries.reserveCapacity(files.count)
    for (path, record) in files {
      guard let pointer = region.pointer(offset: record.payloadOffset, count: record.payloadSize),
        record.payloadSize <= UInt64(Int.max)
      else { return false }
      entries.append(
        MappedCheckpointEntry(
          path: path,
          payload: UnsafeRawBufferPointer(start: pointer, count: Int(record.payloadSize))))
    }
    return CheckpointArchive.writeMapped(entries, to: diskPath)
  }

  private func installPipelines(_ value: [[String]]) {
    pipelines = value
    stagePositions.removeAll(keepingCapacity: true)
    for (pipelineIndex, pipeline) in pipelines.enumerated() {
      for (stageIndex, stage) in pipeline.enumerated() {
        stagePositions[stage] = (pipelineIndex, stageIndex)
        if buckets[stage] == nil { buckets[stage] = Bucket() }
        knownNames.insert(stage)
      }
    }
  }

  private func requestPath(_ request: SMRRequest) -> String? {
    guard let raw = rawRequestPath(request) else { return nil }
    return RuntimeValidation.normalize(path: raw).flatMap { $0 == "/" ? nil : $0 }
  }

  private func requestPrefix(_ request: SMRRequest) -> String? {
    guard let raw = rawRequestPath(request) else { return nil }
    return RuntimeValidation.normalize(path: raw)
  }

  private func rawRequestPath(_ request: SMRRequest) -> String? {
    var copy = request
    let string = withUnsafePointer(to: &copy) { pointer in
      String(cString: smr_request_path(pointer))
    }
    return string.isEmpty ? nil : string
  }

  private func requestTarget(_ request: SMRRequest) -> String? {
    var copy = request
    let string = withUnsafePointer(to: &copy) { pointer in
      String(cString: smr_request_target(pointer))
    }
    return string.isEmpty ? nil : string
  }
}
