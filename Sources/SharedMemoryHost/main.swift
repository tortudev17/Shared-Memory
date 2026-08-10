import Foundation
import SharedMemoryRuntime

let arguments = CommandLine.arguments
let limit: Int
if let index = arguments.firstIndex(of: "--memory-bytes"), index + 1 < arguments.count,
  let parsed = UInt64(arguments[index + 1]), parsed > 0
{
  // RuntimeValidation reads this value when memoryLimitGB is negative.  The
  // byte form is useful for isolated integration tests; production callers
  // normally use the 8 GiB default or --memory-limit-gb.
  setenv("SMR_MEMORY_BYTES", String(parsed), 1)
  limit = -1
} else if let index = arguments.firstIndex(of: "--memory-limit-gb"), index + 1 < arguments.count,
  let parsed = Int(arguments[index + 1]), parsed > 0
{
  limit = parsed
} else {
  limit = 8
}

let host = SharedMemory(
  creator: true,
  name: "filesystem-host-\(ProcessInfo.processInfo.processIdentifier)",
  conveyor: nil,
  memoryLimitGB: limit)

guard host.isConnected else {
  FileHandle.standardError.write(Data("shared-memory-host: failed to connect\n".utf8))
  exit(1)
}

print("READY")
fflush(stdout)
RunLoop.main.run()
