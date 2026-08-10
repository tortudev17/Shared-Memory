// swift-tools-version: 6.2

import PackageDescription

let package = Package(
  name: "SharedMemoryRuntime",
  platforms: [
    .macOS(.v12)
  ],
  products: [
    .library(name: "SharedMemoryRuntime", targets: ["SharedMemoryRuntime"]),
    .executable(name: "shared-memory-benchmarks", targets: ["SharedMemoryBenchmarks"]),
    .executable(name: "shared-memory-host", targets: ["SharedMemoryHost"]),
    .executable(name: "shared-memory-tool", targets: ["SharedMemoryTool"]),
  ],
  targets: [
    .target(
      name: "CSharedMemory",
      publicHeadersPath: "include"
    ),
    .target(
      name: "SharedMemoryCore",
      dependencies: ["CSharedMemory"]
    ),
    .target(
      name: "SharedMemoryRuntime",
      dependencies: ["SharedMemoryCore"]
    ),
    .executableTarget(
      name: "SharedMemoryBenchmarks",
      dependencies: ["SharedMemoryRuntime", "SharedMemoryCore", "CSharedMemory"]
    ),
    .executableTarget(
      name: "SharedMemoryHost",
      dependencies: ["SharedMemoryRuntime"]
    ),
    .executableTarget(
      name: "SharedMemoryTool",
      dependencies: ["SharedMemoryRuntime"]
    ),
    .testTarget(
      name: "SharedMemoryCoreTests",
      dependencies: ["SharedMemoryCore", "CSharedMemory"]
    ),
    .testTarget(
      name: "SharedMemoryRuntimeTests",
      dependencies: ["SharedMemoryRuntime", "SharedMemoryCore", "CSharedMemory"]
    ),
  ]
)
