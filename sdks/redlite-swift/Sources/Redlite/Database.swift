import CRedlite
import Foundation

/// Thread-safe Redlite database handle
///
/// A Redis-compatible embedded database with SQLite durability.
///
/// ## Example
/// ```swift
/// let db = try Database.openMemory()
/// try db.set("key", value: "value")
/// let value = try db.getString("key")  // "value"
/// ```
public final class Database: @unchecked Sendable {
    private let handle: OpaquePointer
    private let lock = NSLock()

    /// Open a database at the given path
    ///
    /// - Parameter path: File path for the database. Use ":memory:" for in-memory database.
    /// - Throws: `RedliteError.openFailed` if the database cannot be opened
    public init(path: String) throws {
        guard let db = path.withCString({ redlite_open($0) }) else {
            throw RedliteError.openFailed(getLastError().localizedDescription)
        }
        self.handle = db
    }

    /// Open an in-memory database
    ///
    /// - Returns: A new in-memory Database instance
    /// - Throws: `RedliteError.openFailed` if the database cannot be created
    public static func openMemory() throws -> Database {
        guard let db = redlite_open_memory() else {
            throw RedliteError.openFailed(getLastError().localizedDescription)
        }
        return Database(handle: db)
    }

    /// Open a database with custom cache size
    ///
    /// - Parameters:
    ///   - path: File path for the database
    ///   - cacheMB: Cache size in megabytes
    /// - Throws: `RedliteError.openFailed` if the database cannot be opened
    public static func open(path: String, cacheMB: Int64) throws -> Database {
        guard let db = path.withCString({ redlite_open_with_cache($0, cacheMB) }) else {
            throw RedliteError.openFailed(getLastError().localizedDescription)
        }
        return Database(handle: db)
    }

    /// Private initializer for factory methods
    private init(handle: OpaquePointer) {
        self.handle = handle
    }

    deinit {
        redlite_close(handle)
    }

    /// Execute a synchronized operation on the database
    internal func withHandle<T>(_ body: (OpaquePointer) throws -> T) rethrows -> T {
        lock.lock()
        defer { lock.unlock() }
        return try body(handle)
    }

    /// Get library version
    public static var version: String {
        guard let v = redlite_version() else { return "" }
        let version = String(cString: v)
        redlite_free_string(v)
        return version
    }
}
