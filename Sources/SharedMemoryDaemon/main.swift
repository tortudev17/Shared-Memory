import Foundation
import SharedMemoryCore

private func argument(named name: String) -> String? {
  let arguments = CommandLine.arguments
  guard let index = arguments.firstIndex(of: name), index + 1 < arguments.count else { return nil }
  return arguments[index + 1]
}

guard
  let sharedMemory = argument(named: "--shm-name"),
  let lockFile = argument(named: "--lock-file"),
  let memoryString = argument(named: "--memory-bytes"),
  let memoryBytes = UInt64(memoryString),
  let encodedPipelines = argument(named: "--pipelines"),
  let pipelines = PipelineConfiguration.decode(base64: encodedPipelines)
else {
  exit(2)
}

let names = RuntimeNames(sharedMemory: sharedMemory, lockFile: lockFile)
let options = DaemonOptions(names: names, memoryBytes: memoryBytes, pipelines: pipelines)
exit(DaemonServer.run(options: options))
