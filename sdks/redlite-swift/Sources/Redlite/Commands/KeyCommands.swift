import CRedlite
import Foundation

// MARK: - Key Commands
extension Database {

    /// DEL key [key ...] - Delete one or more keys
    ///
    /// - Parameter keys: Keys to delete
    /// - Returns: Number of keys deleted
    @discardableResult
    public func del(_ keys: String...) throws -> Int64 {
        try del(Array(keys))
    }

    /// DEL with array of keys
    @discardableResult
    public func del(_ keys: [String]) throws -> Int64 {
        guard !keys.isEmpty else { return 0 }
        return try withHandle { db in
            withCStringArray(keys) { keysPtr, count in
                redlite_del(db, keysPtr, count)
            }
        }
    }

    /// EXISTS key [key ...] - Check if keys exist
    ///
    /// - Parameter keys: Keys to check
    /// - Returns: Number of existing keys
    public func exists(_ keys: String...) throws -> Int64 {
        try exists(Array(keys))
    }

    /// EXISTS with array of keys
    public func exists(_ keys: [String]) throws -> Int64 {
        guard !keys.isEmpty else { return 0 }
        return try withHandle { db in
            withCStringArray(keys) { keysPtr, count in
                redlite_exists(db, keysPtr, count)
            }
        }
    }

    /// TYPE key - Get the type of a key
    ///
    /// - Parameter key: The key to check
    /// - Returns: Type string ("string", "list", "set", "zset", "hash") or nil if key doesn't exist
    public func type(_ key: String) throws -> String? {
        try withHandle { db in
            guard let result = key.withCString({ redlite_type(db, $0) }) else {
                return nil
            }
            let typeStr = String(cString: result)
            redlite_free_string(result)
            return typeStr
        }
    }

    /// TTL key - Get time to live in seconds
    ///
    /// - Parameter key: The key to check
    /// - Returns: -2 if key doesn't exist, -1 if no TTL, otherwise seconds remaining
    public func ttl(_ key: String) throws -> Int64 {
        try withHandle { db in
            key.withCString { redlite_ttl(db, $0) }
        }
    }

    /// PTTL key - Get time to live in milliseconds
    ///
    /// - Parameter key: The key to check
    /// - Returns: -2 if key doesn't exist, -1 if no TTL, otherwise milliseconds remaining
    public func pttl(_ key: String) throws -> Int64 {
        try withHandle { db in
            key.withCString { redlite_pttl(db, $0) }
        }
    }

    /// EXPIRE key seconds - Set TTL in seconds
    ///
    /// - Parameters:
    ///   - key: The key to set TTL on
    ///   - seconds: TTL in seconds
    /// - Returns: true if TTL was set, false if key doesn't exist
    @discardableResult
    public func expire(_ key: String, seconds: Int64) throws -> Bool {
        try withHandle { db in
            key.withCString { redlite_expire(db, $0, seconds) } == 1
        }
    }

    /// PEXPIRE key milliseconds - Set TTL in milliseconds
    ///
    /// - Parameters:
    ///   - key: The key to set TTL on
    ///   - milliseconds: TTL in milliseconds
    /// - Returns: true if TTL was set, false if key doesn't exist
    @discardableResult
    public func pexpire(_ key: String, milliseconds: Int64) throws -> Bool {
        try withHandle { db in
            key.withCString { redlite_pexpire(db, $0, milliseconds) } == 1
        }
    }

    /// EXPIREAT key timestamp - Set expiration as Unix timestamp (seconds)
    ///
    /// - Parameters:
    ///   - key: The key to set expiration on
    ///   - unixTime: Unix timestamp in seconds
    /// - Returns: true if expiration was set, false if key doesn't exist
    @discardableResult
    public func expireat(_ key: String, unixTime: Int64) throws -> Bool {
        try withHandle { db in
            key.withCString { redlite_expireat(db, $0, unixTime) } == 1
        }
    }

    /// PEXPIREAT key timestamp - Set expiration as Unix timestamp (milliseconds)
    ///
    /// - Parameters:
    ///   - key: The key to set expiration on
    ///   - unixTimeMs: Unix timestamp in milliseconds
    /// - Returns: true if expiration was set, false if key doesn't exist
    @discardableResult
    public func pexpireat(_ key: String, unixTimeMs: Int64) throws -> Bool {
        try withHandle { db in
            key.withCString { redlite_pexpireat(db, $0, unixTimeMs) } == 1
        }
    }

    /// PERSIST key - Remove TTL from key
    ///
    /// - Parameter key: The key to persist
    /// - Returns: true if TTL was removed, false if key doesn't exist or has no TTL
    @discardableResult
    public func persist(_ key: String) throws -> Bool {
        try withHandle { db in
            key.withCString { redlite_persist(db, $0) } == 1
        }
    }

    /// RENAME key newkey - Rename a key
    ///
    /// - Parameters:
    ///   - key: The key to rename
    ///   - newKey: The new key name
    /// - Throws: Error if key doesn't exist
    public func rename(_ key: String, to newKey: String) throws {
        try withHandle { db in
            let result = key.withCString { kPtr in
                newKey.withCString { nPtr in
                    redlite_rename(db, kPtr, nPtr)
                }
            }
            if result != 0 {
                throw getLastError()
            }
        }
    }

    /// RENAMENX key newkey - Rename key only if new key doesn't exist
    ///
    /// - Parameters:
    ///   - key: The key to rename
    ///   - newKey: The new key name
    /// - Returns: true if renamed, false if newkey already exists
    @discardableResult
    public func renamenx(_ key: String, to newKey: String) throws -> Bool {
        try withHandle { db in
            key.withCString { kPtr in
                newKey.withCString { nPtr in
                    redlite_renamenx(db, kPtr, nPtr)
                }
            } == 1
        }
    }

    /// KEYS pattern - Find keys matching pattern
    ///
    /// - Parameter pattern: Glob-style pattern (e.g., "user:*", "key??")
    /// - Returns: Array of matching key names
    public func keys(_ pattern: String = "*") throws -> [String] {
        try withHandle { db in
            var result = pattern.withCString { redlite_keys(db, $0) }
            defer {
                var wrapper = FFIStringArray(result)
                wrapper.free()
            }
            return FFIStringArray(result).toStrings()
        }
    }

    /// DBSIZE - Get number of keys in database
    ///
    /// - Returns: Number of keys
    public func dbsize() throws -> Int64 {
        try withHandle { db in
            redlite_dbsize(db)
        }
    }

    /// FLUSHDB - Delete all keys in the current database
    public func flushdb() throws {
        try withHandle { db in
            let result = redlite_flushdb(db)
            if result != 0 {
                throw getLastError()
            }
        }
    }

    /// SELECT db - Select database by index
    ///
    /// - Parameter dbNum: Database index (0-15 typically)
    public func select(_ dbNum: Int32) throws {
        try withHandle { db in
            let result = redlite_select(db, dbNum)
            if result != 0 {
                throw getLastError()
            }
        }
    }

    /// VACUUM - Compact the database file
    ///
    /// - Returns: Number of bytes freed
    @discardableResult
    public func vacuum() throws -> Int64 {
        try withHandle { db in
            redlite_vacuum(db)
        }
    }
}
