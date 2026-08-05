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
