import CSharedMemory
import Foundation

package final class RuntimeHandlers: @unchecked Sendable {
  private let lock = NSLock()
  private var receiveValue: ((UUID, UnsafeRawBufferPointer) -> Void)?
  private var notificationValue: ((String) -> Void)?
  private var subscriptionPaths: [UInt64: String] = [:]

  package init() {}

  package var receive: ((UUID, UnsafeRawBufferPointer) -> Void)? {
    get { lock.withLock { receiveValue } }
    set { lock.withLock { receiveValue = newValue } }
  }

  package var notification: ((String) -> Void)? {
    get { lock.withLock { notificationValue } }
    set { lock.withLock { notificationValue = newValue } }
  }

  package func install(subscription: UInt64, path: String) {
    lock.withLock { subscriptionPaths[subscription] = path }
  }

  package func path(for subscription: UInt64) -> String? {
    lock.withLock { subscriptionPaths[subscription] }
  }
}

package final class RuntimeClient: @unchecked Sendable {
  private static let standardTimeout: UInt64 = 5_000_000_000
  private let region: MappedRegion
  private let slot: UnsafeMutableRawPointer
  private let slotIndex: Int
  private let generation: UInt64
  private let handlers: RuntimeHandlers
  private let requestLock = NSLock()
  private let stateLock = NSLock()
  private let eventQueue = DispatchQueue(
    label: "SharedMemoryRuntime.receive", qos: .userInteractive)
  private let eventGroup = DispatchGroup()
  private let eventQueueKey = DispatchSpecificKey<UInt8>()
  private var sequence: UInt64 = 1
  private var stopped = false
  private var closed = false
  private var pendingEvent: SMREvent?

  package init?(
    creator: Bool,
    name: String,
    conveyor: [[String]]?,
    memoryLimitGB: Int,
    handlers: RuntimeHandlers
  ) {
    guard RuntimeValidation.validName(name),
      let region = DaemonLauncher.connect(
        creator: creator,
        conveyor: conveyor,
        memoryLimitGB: memoryLimitGB
      )
    else { return nil }

    var claimedSlot: UnsafeMutableRawPointer?
    var claimedIndex = -1
    for index in 0..<Int(SMR_MAX_CLIENTS) {
      guard let candidate = region.slot(at: index), smr_slot_claim(candidate) == 1 else { continue }
      claimedSlot = candidate
      claimedIndex = index
      break
    }
    guard let claimedSlot else { return nil }

    let generation = UInt64.random(in: 1...UInt64.max)
    let prepared = name.withCString { pointer in
      smr_slot_prepare(claimedSlot, smr_current_pid(), generation, pointer)
    }
    guard prepared == 1 else {
      smr_slot_reset(claimedSlot)
      return nil
    }

    self.region = region
    slot = claimedSlot
    slotIndex = claimedIndex
    self.generation = generation
    self.handlers = handlers

    guard let response = performRequest(command: .register), response.succeeded else {
      stateLock.withLock {
        stopped = true
        closed = true
      }
      smr_slot_reset(claimedSlot)
      return nil
    }
    if creator, let conveyor {
      let configuration = PipelineConfiguration(conveyor)
      guard let data = configuration.validated()?.encodedData(), configure(data: data) else {
        _ = performRequest(command: .unregister)
        stateLock.withLock {
          stopped = true
          closed = true
        }
        smr_slot_reset(claimedSlot)
        return nil
      }
    }
    eventQueue.setSpecific(key: eventQueueKey, value: 1)
    startEventPump()
  }

  deinit {
    close()
  }

  package var isConnected: Bool {
    stateLock.withLock {
      !closed && region.isValid() && smr_slot_state(slot) == SMR_SLOT_ACTIVE
        && smr_pid_is_alive(smr_daemon_pid(region.baseAddress)) == 1
    }
  }

  package func close() {
    let shouldClose = stateLock.withLock { () -> Bool in
      guard !closed else { return false }
      stopped = true
      closed = true
      return true
    }
    guard shouldClose else { return }
    if DispatchQueue.getSpecific(key: eventQueueKey) == nil {
      _ = eventGroup.wait(timeout: .now() + 2)
    }
    _ = performRequest(command: .unregister, allowClosed: true)
    smr_slot_reset(slot)
  }

  package func write(path: String, data: Data) -> Bool {
    guard let normalized = RuntimeValidation.normalize(path: path), normalized != "/",
      let staged = stage(data: data)
    else { return false }
    let response = performRequest(
      command: .write,
      arg0: staged.block,
      arg1: UInt64(data.count),
      path: normalized
    )
    if response?.succeeded != true { _ = performRequest(command: .abandon, arg0: staged.block) }
    return response?.succeeded == true
  }

  package func withRead<R>(path: String, _ body: (UnsafeRawBufferPointer) throws -> R) rethrows
    -> R?
  {
    guard let normalized = RuntimeValidation.normalize(path: path), normalized != "/",
      let response = performRequest(command: .read, path: normalized), response.succeeded,
      response.value2 <= UInt64(Int.max),
      let pointer = region.pointer(offset: response.value1, count: response.value2)
    else { return nil }
    defer { _ = performRequest(command: .releaseLease, arg0: response.value0) }
    return try body(UnsafeRawBufferPointer(start: pointer, count: Int(response.value2)))
  }

  package func pass(uuids: [UUID], encoded values: [Data]) -> Bool {
    transfer(encoded: values, target: nil, uuids: uuids)
  }

  package func send(to target: String, encoded values: [Data]) -> Bool {
    guard RuntimeValidation.validTarget(target) else { return false }
    return transfer(encoded: values, target: target, uuids: nil)
  }

  package func finish(uuid: UUID) -> Bool {
    let bits = UUIDBits.split(uuid)
    return performRequest(command: .finish, arg0: bits.0, arg1: bits.1)?.succeeded == true
  }

  package func checkpoint(path: String) -> Bool {
    guard path.first == "/", !path.utf8.contains(0), path.utf8.count <= Int(SMR_MAX_PATH_BYTES)
    else {
      return false
    }
    return performRequest(command: .checkpoint, path: path, timeout: 120_000_000_000)?.succeeded
      == true
  }

  package func memoryUsed() -> UInt64 {
    performRequest(command: .memoryUsed)?.value0 ?? 0
  }

  package func subscribe(path: String) -> Bool {
    guard let normalized = RuntimeValidation.normalize(path: path), normalized != "/",
      let response = performRequest(command: .subscribe, path: normalized), response.succeeded
    else { return false }
    handlers.install(subscription: response.value0, path: normalized)
    return true
  }

  private func transfer(encoded values: [Data], target: String?, uuids: [UUID]?) -> Bool {
    guard let byteCount = BatchLayout.requiredBytes(for: values, uuids: uuids),
      let allocation = allocate(bytes: byteCount),
      let pointer = region.pointer(offset: allocation.payload, count: UInt64(byteCount)),
      BatchLayout.write(values, uuids: uuids, to: pointer, capacity: byteCount)
    else { return false }
    let command: RuntimeCommand = target == nil ? .pass : .send
    let response = performRequest(
      command: command,
      arg0: allocation.block,
      arg1: UInt64(byteCount),
      target: target
    )
    if response?.succeeded != true { _ = performRequest(command: .abandon, arg0: allocation.block) }
    return response?.succeeded == true
  }

  private func configure(data: Data) -> Bool {
    guard let staged = stage(data: data) else { return false }
    let response = performRequest(
      command: .configure,
      arg0: staged.block,
      arg1: UInt64(data.count)
    )
    if response?.succeeded != true { _ = performRequest(command: .abandon, arg0: staged.block) }
    return response?.succeeded == true
  }

  private func stage(data: Data) -> (block: UInt64, payload: UInt64)? {
    guard let allocation = allocate(bytes: data.count),
      let destination = region.pointer(offset: allocation.payload, count: UInt64(data.count))
    else { return nil }
    data.withUnsafeBytes { source in
      if let baseAddress = source.baseAddress, !source.isEmpty {
        destination.copyMemory(from: baseAddress, byteCount: source.count)
      }
    }
    return (allocation.block, allocation.payload)
  }

  private func allocate(bytes: Int) -> (block: UInt64, payload: UInt64, capacity: UInt64)? {
    guard bytes >= 0,
      let response = performRequest(command: .allocate, arg0: UInt64(bytes)),
      response.succeeded,
      response.value2 >= UInt64(bytes),
      region.contains(offset: response.value1, count: UInt64(bytes))
    else { return nil }
    return (response.value0, response.value1, response.value2)
  }

  private func startEventPump() {
    eventGroup.enter()
    eventQueue.async { [self] in
      defer { eventGroup.leave() }
      var idle = 0
      while !stateLock.withLock({ stopped }) {
        if pollEvent() {
          idle = 0
        } else if idle < 2_000 {
          smr_cpu_relax()
          idle += 1
        } else {
          smr_sleep_nanoseconds(50_000)
        }
      }
    }
  }

  private func pollEvent() -> Bool {
    var event: SMREvent
    if let pendingEvent {
      event = pendingEvent
    } else {
      var popped = SMREvent()
      let result = smr_client_pop_event(slot, generation, &popped)
      guard result != 0 else { return false }
      guard result == 1 else { return true }
      event = popped
    }
    guard let kind = RuntimeEventKind(rawValue: event.kind) else {
      pendingEvent = nil
      return true
    }
    switch kind {
    case .receive:
      guard let callback = handlers.receive else {
        pendingEvent = event
        return false
      }
      pendingEvent = nil
      defer { _ = performRequest(command: .releaseLease, arg0: event.allocation_offset) }
      guard event.payload_size <= UInt64(Int.max),
        let pointer = region.pointer(offset: event.payload_offset, count: event.payload_size)
      else { return true }
      let uuid = UUIDBits.join(high: event.uuid_high, low: event.uuid_low)
      callback(uuid, UnsafeRawBufferPointer(start: pointer, count: Int(event.payload_size)))
    case .notification:
      guard let path = handlers.path(for: event.value), let callback = handlers.notification else {
        pendingEvent = event
        return false
      }
      pendingEvent = nil
      callback(path)
    }
    return true
  }

  private func performRequest(
    command: RuntimeCommand,
    flags: UInt32 = 0,
    arg0: UInt64 = 0,
    arg1: UInt64 = 0,
    arg2: UInt64 = 0,
    arg3: UInt64 = 0,
    arg4: UInt64 = 0,
    arg5: UInt64 = 0,
    path: String? = nil,
    target: String? = nil,
    timeout: UInt64 = standardTimeout,
    allowClosed: Bool = false
  ) -> RuntimeResponse? {
    requestLock.lock()
    defer { requestLock.unlock() }
    if !allowClosed && stateLock.withLock({ closed }) { return nil }
    sequence &+= 1
    let requestSequence = sequence
    let submitted: Int32
    if let path {
      submitted = path.withCString { pathPointer in
        if let target {
          return target.withCString { targetPointer in
            smr_client_submit(
              slot, requestSequence, command.rawValue, flags, arg0, arg1, arg2, arg3, arg4, arg5,
              pathPointer, targetPointer)
          }
        }
        return smr_client_submit(
          slot, requestSequence, command.rawValue, flags, arg0, arg1, arg2, arg3, arg4, arg5,
          pathPointer, nil)
      }
    } else if let target {
      submitted = target.withCString { targetPointer in
        smr_client_submit(
          slot, requestSequence, command.rawValue, flags, arg0, arg1, arg2, arg3, arg4, arg5, nil,
          targetPointer)
      }
    } else {
      submitted = smr_client_submit(
        slot, requestSequence, command.rawValue, flags, arg0, arg1, arg2, arg3, arg4, arg5, nil, nil
      )
    }
    guard submitted == 1 else { return nil }

    let start = smr_monotonic_nanoseconds()
    let deadline = start > UInt64.max - timeout ? UInt64.max : start + timeout
    var spins = 0
    while smr_monotonic_nanoseconds() < deadline {
      var raw = SMRResponse()
      let result = smr_client_try_response(slot, requestSequence, &raw)
      if result == 1 {
        smr_client_ack_response(slot)
        return RuntimeResponse(raw)
      }
      if result < 0 {
        smr_client_ack_response(slot)
        return nil
      }
      if smr_pid_is_alive(smr_daemon_pid(region.baseAddress)) == 0 { return nil }
      if spins < 10_000 {
        smr_cpu_relax()
        spins += 1
      } else {
        smr_sleep_nanoseconds(50_000)
      }
    }
    return nil
  }
}
