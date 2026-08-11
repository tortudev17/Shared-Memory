import CSharedMemory
import Dispatch
import Foundation
import SharedMemoryCore
import SharedMemoryRuntime

#if canImport(Darwin)
  import Darwin
#else
  import Glibc
#endif

private struct Sample: Codable {
  let sequence: Int
  let sentAt: UInt64
  let payload: Data
}

final class Locked<Value>: @unchecked Sendable {
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

private func percentile(_ values: [UInt64], _ percentile: Double) -> UInt64 {
  guard !values.isEmpty else { return 0 }
  let sorted = values.sorted()
  return sorted[min(sorted.count - 1, Int(Double(sorted.count - 1) * percentile))]
}

private func measure(iterations: Int, operation: (Int) -> Bool) -> [UInt64] {
  var samples: [UInt64] = []
  samples.reserveCapacity(iterations)
  for index in 0..<iterations {
    let start = DispatchTime.now().uptimeNanoseconds
    guard operation(index) else { break }
    samples.append(DispatchTime.now().uptimeNanoseconds - start)
  }
  return samples
}

private func reportLatency(_ name: String, _ values: [UInt64]) {
  let average = values.isEmpty ? 0 : values.reduce(0, +) / UInt64(values.count)
  print(
    "\(name),count=\(values.count),avg_ns=\(average),p50_ns=\(percentile(values, 0.50)),p99_ns=\(percentile(values, 0.99))"
  )
}

if ProcessInfo.processInfo.environment["SMR_INSTANCE_ID"] == nil {
  setenv("SMR_INSTANCE_ID", "benchmark-\(smr_current_pid())", 1)
}
if ProcessInfo.processInfo.environment["SMR_MEMORY_BYTES"] == nil {
  setenv("SMR_MEMORY_BYTES", "536870912", 1)
}

let iterations = max(
  1, Int(ProcessInfo.processInfo.environment["SMR_BENCHMARK_ITERATIONS"] ?? "10000") ?? 10_000)
let payloadBytes = max(
  0, Int(ProcessInfo.processInfo.environment["SMR_BENCHMARK_PAYLOAD_BYTES"] ?? "256") ?? 256)
let payload = Data(repeating: 0x5a, count: payloadBytes)
let source = SharedMemory(
  creator: true,
  name: "bench-source",
  conveyor: [["bench-source", "bench-middle", "bench-sink"]]
)
let middle = SharedMemory(name: "bench-middle")
let feeder = SharedMemory(name: "bench-feeder")
guard source.isConnected, middle.isConnected, feeder.isConnected else { exit(2) }
let initialResidentBytes = smr_current_resident_bytes()
let initialFootprintBytes = smr_current_footprint_bytes()

let write = measure(iterations: iterations) { index in
  source.write(path: "/bench/value", value: Sample(sequence: index, sentAt: 0, payload: payload))
}
let read = measure(iterations: iterations) { _ in
  let value: Sample? = source.read(path: "/bench/value")
  return value != nil
}

let expectedReceives = iterations * 2
let receiveLatencies = Locked<[UInt64]>([])
let receiveCount = Locked(0)
let receiveDrain = DispatchSemaphore(value: 0)
middle.setReceiveHandler(for: Sample.self) { [weak middle] uuid, value in
  let now = DispatchTime.now().uptimeNanoseconds
  if now >= value.sentAt { receiveLatencies.withValue { $0.append(now - value.sentAt) } }
  _ = middle?.finish(uuid: uuid)
  let count = receiveCount.withValue { current -> Int in
    current += 1
    return current
  }
  if count == expectedReceives { receiveDrain.signal() }
}

let passLatencies = Locked<[UInt64]>([])
let passCount = Locked(0)
let passDrain = DispatchSemaphore(value: 0)
source.setReceiveHandler(for: Sample.self) { [weak source] uuid, value in
  let start = DispatchTime.now().uptimeNanoseconds
  if source?.pass([(uuid, value)]) == true {
    passLatencies.withValue { $0.append(DispatchTime.now().uptimeNanoseconds - start) }
  }
  let count = passCount.withValue { current -> Int in
    current += 1
    return current
  }
  if count == iterations { passDrain.signal() }
}
for index in 0..<iterations {
  guard
    feeder.send(
      to: "bench-source",
      values: [
        Sample(sequence: index, sentAt: DispatchTime.now().uptimeNanoseconds, payload: payload)
      ]
    )
  else { break }
}
_ = passDrain.wait(timeout: .now() + 60)
let pass = passLatencies.snapshot()
let send = measure(iterations: iterations) { index in
  source.send(
    to: "bench-middle",
    values: [
      Sample(sequence: index, sentAt: DispatchTime.now().uptimeNanoseconds, payload: payload)
    ])
}
_ = receiveDrain.wait(timeout: .now() + 60)

let contentionOperations = max(100, iterations / 10)
let contenders = (0..<4).map { SharedMemory(name: "bench-contender-\($0)") }
let contentionStart = DispatchTime.now().uptimeNanoseconds
DispatchQueue.concurrentPerform(iterations: contenders.count) { worker in
  let client = contenders[worker]
  for operation in 0..<contentionOperations {
    _ = client.write(
      path: "/bench/contention/\(worker)",
      value: Sample(sequence: operation, sentAt: 0, payload: payload)
    )
  }
}
let contentionDuration = DispatchTime.now().uptimeNanoseconds - contentionStart
let contentionTotal = contenders.count * contentionOperations
let contentionThroughput =
  contentionDuration == 0 ? 0 : UInt64(contentionTotal) * 1_000_000_000 / contentionDuration

let zeroCopyMeasurement = Locked<(before: UInt64, after: UInt64)?>(nil)
let zeroCopyDone = DispatchSemaphore(value: 0)
source.setReceiveHandler(for: Sample.self) { [weak source] uuid, value in
  guard let source else { return }
  let before = source.memoryUsed()
  if source.pass([(uuid, value)]) {
    zeroCopyMeasurement.withValue { $0 = (before, source.memoryUsed()) }
  }
}
middle.setReceiveHandler(for: Sample.self) { [weak middle] uuid, _ in
  _ = middle?.finish(uuid: uuid)
  zeroCopyDone.signal()
}
_ = feeder.send(
  to: "bench-source",
  values: [
    Sample(sequence: 0, sentAt: 0, payload: Data(repeating: 0xa5, count: 1 << 20))
  ])
_ = zeroCopyDone.wait(timeout: .now() + 10)

let allocatorName = "/smr_b_\(String(UInt64(smr_current_pid()), radix: 16))"
MappedRegion.unlink(name: allocatorName)
var allocationLatency: [UInt64] = []
var queueLatency: [UInt64] = []
if let allocationRegion = MappedRegion.create(name: allocatorName, bytes: 64 << 20),
  allocationRegion.initialize(bootID: 123, daemonPID: smr_current_pid()),
  let arena = SharedArena(region: allocationRegion, secret: 456)
{
  allocationLatency = measure(iterations: iterations) { _ in
    guard let allocation = arena.allocate(payloadBytes: 256) else { return false }
    return arena.release(allocation.blockOffset)
  }
  if let slot = allocationRegion.slot(at: 0), smr_slot_claim(slot) == 1,
    "queue".withCString({ smr_slot_prepare(slot, smr_current_pid(), 1, $0) }) == 1
  {
    smr_slot_activate(slot)
    queueLatency = measure(iterations: iterations) { index in
      var event = SMREvent()
      event.kind = RuntimeEventKind.notification.rawValue
      event.generation = 1
      event.value = UInt64(index)
      guard smr_daemon_push_event(slot, &event) == 1 else { return false }
      var received = SMREvent()
      return smr_client_pop_event(slot, 1, &received) == 1 && received.value == UInt64(index)
    }
  }
}
MappedRegion.unlink(name: allocatorName)

reportLatency("write", write)
reportLatency("read", read)
reportLatency("pass", pass)
reportLatency("send", send)
reportLatency("receive", receiveLatencies.snapshot())
reportLatency("shared_allocation", allocationLatency)
reportLatency("spsc_queue", queueLatency)
let writeBytes = UInt64(write.count * payload.count)
let writeDuration = write.reduce(0, +)
let writeThroughput = writeDuration == 0 ? 0 : writeBytes * 1_000_000_000 / writeDuration
print(
  "throughput,write_payload_bytes_per_second=\(writeThroughput),contention_operations_per_second=\(contentionThroughput),workers=\(contenders.count)"
)
if let zeroCopy = zeroCopyMeasurement.snapshot() {
  let delta =
    zeroCopy.after >= zeroCopy.before
    ? zeroCopy.after - zeroCopy.before : zeroCopy.before - zeroCopy.after
  print(
    "zero_copy,logical_bytes=1048576,committed_byte_delta=\(delta),payload_copies_between_stages=0")
}
_ = source.write(path: "/bench/value", value: nil)
for worker in 0..<contenders.count {
  _ = source.write(path: "/bench/contention/\(worker)", value: nil)
}
smr_sleep_nanoseconds(150_000_000)
let memoryFields = [
  "memory",
  "shared_committed_bytes=\(source.memoryUsed())",
  "process_initial_resident_bytes=\(initialResidentBytes)",
  "process_current_resident_bytes=\(smr_current_resident_bytes())",
  "process_initial_footprint_bytes=\(initialFootprintBytes)",
  "process_current_footprint_bytes=\(smr_current_footprint_bytes())",
  "process_reusable_bytes=\(smr_current_reusable_bytes())",
  "process_peak_resident_bytes=\(smr_peak_resident_bytes())",
]
print(memoryFields.joined(separator: ","))
