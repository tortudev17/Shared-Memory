import Foundation

/// Internal, stable wire records used by both the Swift and Python clients.
package struct FilesystemPathEntry: Codable, Equatable, Sendable {
  package let path: String
  package let version: UInt64

  package init(path: String, version: UInt64) {
    self.path = path
    self.version = version
  }
}

package enum FilesystemMutationKind: String, Codable, Sendable {
  case write
  case delete
}

package struct FilesystemMutation: Codable, Equatable, Sendable {
  package let kind: FilesystemMutationKind
  package let path: String
  package let expectedVersion: UInt64?
  package let payload: Data?
  // Native clients can stage a large encoded value separately and transfer
  // that allocation into the filesystem atomically. This avoids embedding a
  // multi-gigabyte model twice inside the transaction descriptor.
  package let stagedBlock: UInt64?
  package let payloadSize: UInt64?

  package init(
    kind: FilesystemMutationKind,
    path: String,
    expectedVersion: UInt64? = nil,
    payload: Data? = nil,
    stagedBlock: UInt64? = nil,
    payloadSize: UInt64? = nil
  ) {
    self.kind = kind
    self.path = path
    self.expectedVersion = expectedVersion
    self.payload = payload
    self.stagedBlock = stagedBlock
    self.payloadSize = payloadSize
  }
}

package struct FilesystemTransaction: Codable, Equatable, Sendable {
  package let mutations: [FilesystemMutation]

  package init(mutations: [FilesystemMutation]) {
    self.mutations = mutations
  }
}
