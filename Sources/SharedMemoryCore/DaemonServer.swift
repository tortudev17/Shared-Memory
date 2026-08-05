import CSharedMemory
import Foundation

package struct DaemonOptions: Sendable {
  package let names: RuntimeNames
  package let memoryBytes: UInt64
  package let pipelines: PipelineConfiguration

  package init(names: RuntimeNames, memoryBytes: UInt64, pipelines: PipelineConfiguration) {
    self.names = names
    self.memoryBytes = memoryBytes
    self.pipelines = pipelines
  }
}

package enum DaemonServer {
  package static func run(options: DaemonOptions) -> Int32 {
    guard options.memoryBytes >= smr_minimum_region_size(), options.pipelines.validated() != nil
    else {
      return 2
    }
    let lockFD = options.names.lockFile.withCString { smr_bootstrap_lock($0) }
    guard lockFD >= 0 else { return 0 }
    defer { smr_bootstrap_unlock(lockFD) }

    MappedRegion.unlink(name: options.names.sharedMemory)
    guard
      let region = MappedRegion.create(name: options.names.sharedMemory, bytes: options.memoryBytes)
    else {
      return 100 + smr_error_code()
    }
    let bootID = UInt64.random(in: 1...UInt64.max)
    guard region.initialize(bootID: bootID, daemonPID: smr_current_pid()),
      let state = DaemonState(region: region, configuration: options.pipelines)
    else {
      MappedRegion.unlink(name: options.names.sharedMemory)
      return 4
    }
    _ = smr_lock_memory(region.baseAddress, region.size)
    var lastReap = smr_monotonic_nanoseconds()
    var idleIterations = 0
    while smr_should_terminate() == 0 {
      let now = smr_monotonic_nanoseconds()
      smr_set_daemon_heartbeat(region.baseAddress, now)
      var didWork = false
      for index in 0..<Int(SMR_MAX_CLIENTS) {
        guard let slot = region.slot(at: index) else { continue }
        let slotState = smr_slot_state(slot)
        guard slotState == SMR_SLOT_CLAIMING || slotState == SMR_SLOT_ACTIVE else { continue }
        var request = SMRRequest()
        if smr_daemon_take_request(slot, &request) == 1 {
          var response = state.handle(slotIndex: index, slot: slot, request: request)
          smr_daemon_complete_request(slot, &response)
          didWork = true
        }
      }
      if state.pumpEvents() { didWork = true }
      if now &- lastReap >= 250_000_000 {
        if state.reapDeadClients() { didWork = true }
        lastReap = now
      }
      if didWork {
        idleIterations = 0
      } else if idleIterations < 2_000 {
        smr_cpu_relax()
        idleIterations += 1
      } else {
        smr_sleep_nanoseconds(50_000)
      }
    }
    smr_set_daemon_pid(region.baseAddress, 0)
    MappedRegion.unlink(name: options.names.sharedMemory)
    return 0
  }
}
