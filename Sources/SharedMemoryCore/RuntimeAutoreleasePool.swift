import Foundation

@inline(__always)
package func withRuntimeAutoreleasePool<Result>(_ body: () throws -> Result) rethrows -> Result {
  #if canImport(ObjectiveC)
    return try autoreleasepool(invoking: body)
  #else
    return try body()
  #endif
}
