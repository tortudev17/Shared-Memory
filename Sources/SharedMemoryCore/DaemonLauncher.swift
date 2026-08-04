import CSharedMemory
import Foundation

package enum DaemonLauncher {
  package static func connect(
    creator: Bool,
    conveyor: [[String]]?,
    memoryLimitGB: Int,
    environment: [String: String] = ProcessInfo.processInfo.environment
  ) -> MappedRegion? {
    let names = RuntimeNames(environment: environment)
    if let existing = openHealthy(names: names) { return existing }
    guard
      let memoryBytes = RuntimeValidation.memoryBytes(
        limitGB: creator ? memoryLimitGB : -1, environment: environment)
    else {
      return nil
    }
    let configuration = PipelineConfiguration(creator ? conveyor : nil)
    guard configuration.validated() != nil,
      let encodedPipelines = configuration.base64Encoded(),
      let executable = daemonExecutable(environment: environment)
    else { return nil }

    let process = Process()
    process.executableURL = executable
    process.arguments = [
      "--shm-name", names.sharedMemory,
      "--lock-file", names.lockFile,
      "--memory-bytes", String(memoryBytes),
      "--pipelines", encodedPipelines,
    ]
    process.standardInput = FileHandle.nullDevice
    process.standardOutput = FileHandle.nullDevice
    process.standardError = FileHandle.nullDevice
    do {
      try process.run()
    } catch {
      return nil
    }

    let deadline = smr_monotonic_nanoseconds() &+ 5_000_000_000
    while smr_monotonic_nanoseconds() < deadline {
      if let region = openHealthy(names: names) { return region }
      if !process.isRunning, process.terminationStatus != 0 { return nil }
      smr_sleep_nanoseconds(5_000_000)
    }
    return nil
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

  private static func daemonExecutable(environment: [String: String]) -> URL? {
    let manager = FileManager.default
    if let configured = environment["SMR_DAEMON_PATH"], manager.isExecutableFile(atPath: configured)
    {
      return URL(fileURLWithPath: configured)
    }

    var candidates: [URL] = []
    let current = URL(fileURLWithPath: ProcessInfo.processInfo.arguments.first ?? "")
      .standardizedFileURL
      .deletingLastPathComponent()
    var ancestor = current
    for _ in 0..<8 {
      candidates.append(ancestor.appendingPathComponent("shared-memory-daemon"))
      candidates.append(ancestor.appendingPathComponent("debug/shared-memory-daemon"))
      ancestor.deleteLastPathComponent()
    }

    let working = URL(fileURLWithPath: manager.currentDirectoryPath)
    candidates.append(working.appendingPathComponent(".build/debug/shared-memory-daemon"))
    let buildDirectory = working.appendingPathComponent(".build")
    if let children = try? manager.contentsOfDirectory(
      at: buildDirectory,
      includingPropertiesForKeys: nil,
      options: [.skipsHiddenFiles]
    ) {
      for child in children {
        candidates.append(child.appendingPathComponent("debug/shared-memory-daemon"))
      }
    }

    if let path = environment["PATH"] {
      for directory in path.split(separator: ":") {
        candidates.append(
          URL(fileURLWithPath: String(directory)).appendingPathComponent("shared-memory-daemon"))
      }
    }
    return candidates.first { manager.isExecutableFile(atPath: $0.path) }
  }
}
