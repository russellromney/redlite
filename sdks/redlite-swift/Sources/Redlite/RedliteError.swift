import Foundation

/// Errors that can occur during Redlite operations
public enum RedliteError: Error, Sendable, LocalizedError, Equatable {
    /// Database failed to open
    case openFailed(String)

    /// Operation failed with message from FFI
    case operationFailed(String)

    /// Key does not exist
    case keyNotFound

    /// Type mismatch (e.g., calling LPUSH on a string key)
    case typeMismatch(expected: String, actual: String)

    /// Invalid argument
    case invalidArgument(String)

    /// Unknown error
    case unknown

    public var errorDescription: String? {
        switch self {
        case .openFailed(let msg):
            return "Failed to open database: \(msg)"
        case .operationFailed(let msg):
            return msg
        case .keyNotFound:
            return "Key not found"
        case .typeMismatch(let expected, let actual):
            return "Type mismatch: expected \(expected), got \(actual)"
        case .invalidArgument(let msg):
            return "Invalid argument: \(msg)"
        case .unknown:
            return "Unknown error occurred"
        }
    }
}
