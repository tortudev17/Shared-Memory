import CSharedMemory
import Foundation

#if canImport(Darwin)
  import Darwin
#else
  import Glibc
#endif

package enum DaemonLauncher {
  private static let embeddedQueue = DispatchQueue(
    label: "SharedMemoryRuntime.daemon", qos: .userInteractive, attributes: .concurrent)

  package static func connect(
    creator: Bool,
    conveyor: [[String]]?,
    memoryLimitGB: Int,
    environment: [String: String] = ProcessInfo.processInfo.environment
  ) -> MappedRegion? {
    let names = RuntimeNames(environment: environment)
    if creator, !replaceExistingDaemon(names: names) { return nil }
    if let existing = openHealthy(names: names) { return existing }
    guard
      let memoryBytes = RuntimeValidation.memoryBytes(
        limitGB: creator ? memoryLimitGB : -1, environment: environment)
    else {
      return nil
    }
    let configuration = PipelineConfiguration(creator ? conveyor : nil)
    guard configuration.validated() != nil else { return nil }

    let options = DaemonOptions(
      names: names, memoryBytes: memoryBytes, pipelines: configuration)
    startEmbedded(options: options)

    let deadline = smr_monotonic_nanoseconds() &+ 5_000_000_000
    while smr_monotonic_nanoseconds() < deadline {
      if let region = openHealthy(names: names) { return region }
      smr_sleep_nanoseconds(5_000_000)
    }
    return nil
  }

  private static func startEmbedded(options: DaemonOptions) {
    embeddedQueue.async {
      _ = DaemonServer.run(options: options)
    }
  }

  /// A creator owns the lifetime of its named runtime. Replacing an external daemon
  /// discards its volatile data and lets the new creator start with its own settings.
  /// An embedded daemon shares the caller's PID, so signalling it would terminate the
  /// application itself; that case intentionally reuses the existing daemon.
  private static func replaceExistingDaemon(names: RuntimeNames) -> Bool {
    guard let region = openHealthy(names: names) else { return true }
    let pid = smr_daemon_pid(region.baseAddress)
    guard pid != smr_current_pid() else { return true }
    guard kill(pid, SIGTERM) == 0 else { return false }

    let deadline = smr_monotonic_nanoseconds() &+ 2_000_000_000
    while smr_pid_is_alive(pid) == 1 && smr_monotonic_nanoseconds() < deadline {
      smr_sleep_nanoseconds(5_000_000)
    }
    return smr_pid_is_alive(pid) == 0
  }

  package static func openHealthy(names: RuntimeNames) -> MappedRegion? {
    guard let region = MappedRegion.open(name: names.sharedMemory), region.isValid() else {
      return nil
    }
    let pid = smr_daemon_pid(region.baseAddress)
    guard pid > 0, smr_pid_is_alive(pid) == 1 else { return nil }
    let now = smr_monotonic_nanoseconds()
    let heartbeat = smr_daemon_heartbeat(region.baseAddress)
    guard heartbeat > 0, now >= heartbeat, now - heartbeat < 30_000_000_000 else { return nil }
    return region
  }

}
