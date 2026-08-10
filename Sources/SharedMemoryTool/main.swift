import Foundation
import SharedMemoryRuntime

func fail(_ message: String) -> Never {
  FileHandle.standardError.write(Data((message + "\n").utf8))
  exit(2)
}

let arguments = Array(CommandLine.arguments.dropFirst())
guard arguments.count >= 2 else {
  fail("usage: shared-memory-tool <write-string|read-string> <path> [value]")
}

let memory = SharedMemory(
  creator: false,
  name: "interop-tool-\(ProcessInfo.processInfo.processIdentifier)",
  conveyor: nil,
  memoryLimitGB: -1)
guard memory.isConnected else { fail("cannot connect to Shared Memory") }

switch arguments[0] {
case "write-string":
  guard arguments.count == 3, memory.write(path: arguments[1], value: arguments[2]) else {
    fail("write failed")
  }
case "read-string":
  guard arguments.count == 2,
    let value: String = memory.read(path: arguments[1])
  else {
    fail("read failed")
  }
  print(value)
default:
  fail("unknown command")
}
