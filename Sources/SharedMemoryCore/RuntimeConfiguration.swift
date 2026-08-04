import CSharedMemory
import Foundation

package struct PipelineConfiguration: Codable, Equatable, Sendable {
  package let pipelines: [[String]]

  package init(_ pipelines: [[String]]?) {
    self.pipelines = pipelines ?? []
  }

  package func validated() -> PipelineConfiguration? {
    guard pipelines.count <= 3 else { return nil }
    var names = Set<String>()
    for pipeline in pipelines {
      guard !pipeline.isEmpty else { return nil }
      for name in pipeline {
        guard RuntimeValidation.validName(name), names.insert(name).inserted else {
          return nil
        }
      }
    }
    return self
  }

  package func base64Encoded() -> String? {
    guard let data = encodedData() else { return nil }
    return data.base64EncodedString()
  }

  package func encodedData() -> Data? {
    try? JSONEncoder().encode(self)
  }

  package static func decode(base64: String) -> PipelineConfiguration? {
    guard let data = Data(base64Encoded: base64) else { return nil }
    return decode(data: data)
  }

  package static func decode(data: Data) -> PipelineConfiguration? {
    guard let value = try? JSONDecoder().decode(PipelineConfiguration.self, from: data) else {
      return nil
    }
    return value.validated()
  }
}

package struct RuntimeNames: Sendable {
  package let sharedMemory: String
  package let lockFile: String

  package init(environment: [String: String] = ProcessInfo.processInfo.environment) {
    let instance = environment["SMR_INSTANCE_ID"] ?? ""
    if instance.isEmpty {
      // Darwin limits POSIX shared-memory names to 31 bytes.
      sharedMemory = "/smr_runtime_v1"
      lockFile = "/tmp/swift-shared-memory-runtime-v1.lock"
    } else {
      let suffix = String(RuntimeNames.fnv1a(instance), radix: 16)
      sharedMemory = "/smr_v1_\(suffix)"
      lockFile = "/tmp/swift-shared-memory-runtime-v1-\(suffix).lock"
    }
  }

  package init(sharedMemory: String, lockFile: String) {
    self.sharedMemory = sharedMemory
    self.lockFile = lockFile
  }

  private static func fnv1a(_ string: String) -> UInt64 {
    var hash: UInt64 = 14_695_981_039_346_656_037
    for byte in string.utf8 {
      hash ^= UInt64(byte)
      hash &*= 1_099_511_628_211
    }
    return hash
  }
}

package enum RuntimeValidation {
  package static func validName(_ value: String) -> Bool {
    !value.isEmpty && !value.utf8.contains(0) && value.utf8.count <= Int(SMR_MAX_NAME_BYTES)
  }

  package static func validTarget(_ value: String) -> Bool {
    !value.isEmpty && !value.utf8.contains(0) && value.utf8.count <= Int(SMR_MAX_TARGET_BYTES)
  }

  package static func normalize(path: String) -> String? {
    guard path.first == "/", !path.utf8.contains(0), path.utf8.count <= Int(SMR_MAX_PATH_BYTES)
    else {
      return nil
    }
    var components: [Substring] = []
    for component in path.split(separator: "/", omittingEmptySubsequences: true) {
      if component == "." { continue }
      if component == ".." {
        guard !components.isEmpty else { return nil }
        components.removeLast()
      } else {
        components.append(component)
      }
    }
    return "/" + components.joined(separator: "/")
  }

  package static func memoryBytes(
    limitGB: Int, environment: [String: String] = ProcessInfo.processInfo.environment
  ) -> UInt64? {
    if limitGB > 0 {
      let (value, overflow) = UInt64(limitGB).multipliedReportingOverflow(by: 1 << 30)
      return overflow ? nil : value
    }
    if limitGB != -1 { return nil }
    if let override = environment["SMR_MEMORY_BYTES"], let value = UInt64(override) {
      return max(value, smr_minimum_region_size())
    }
    return 256 << 20
  }
}
