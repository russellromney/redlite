import CRedlite
import Foundation

// MARK: - JSON Commands (ReJSON-compatible)
extension Database {

    /// JSON.SET key path value [NX|XX]
    ///
    /// - Parameters:
    ///   - key: The key to set
    ///   - path: JSON path (use "$" for root)
    ///   - value: JSON-encoded value
    ///   - options: NX/XX options
    /// - Returns: true if set, false if NX/XX condition not met
    public func jsonSet(_ key: String, path: String, value: String, options: JsonSetOptions = JsonSetOptions()) throws -> Bool {
        try withHandle { db in
            let result = key.withCString { keyPtr in
                path.withCString { pathPtr in
                    value.withCString { valuePtr in
                        redlite_json_set(db, keyPtr, pathPtr, valuePtr, options.nx ? 1 : 0, options.xx ? 1 : 0)
                    }
                }
            }
            if result < 0 {
                throw getLastError()
            }
            return result == 1
        }
    }

    /// JSON.GET key [path ...]
    ///
    /// - Parameters:
    ///   - key: The key to get
    ///   - paths: JSON paths to get (defaults to ["$"])
    /// - Returns: JSON-encoded result or nil if not found
    public func jsonGet(_ key: String, paths: [String] = ["$"]) throws -> String? {
        try withHandle { db in
            let pathPtrs = paths.map { UnsafeMutablePointer(mutating: ($0 as NSString).utf8String) }
            defer {
                // No need to free - these are autoreleased
            }
            return pathPtrs.withUnsafeBufferPointer { buffer in
                let result = key.withCString { keyPtr in
                    redlite_json_get(db, keyPtr, buffer.baseAddress, paths.count)
                }
                guard let ptr = result else { return nil }
                let str = String(cString: ptr)
                redlite_free_string(ptr)
                return str
            }
        }
    }

    /// JSON.DEL key [path]
    ///
    /// - Parameters:
    ///   - key: The key
    ///   - path: JSON path (defaults to "$")
    /// - Returns: Number of paths deleted
    public func jsonDel(_ key: String, path: String = "$") throws -> Int64 {
        try withHandle { db in
            key.withCString { keyPtr in
                path.withCString { pathPtr in
                    redlite_json_del(db, keyPtr, pathPtr)
                }
            }
        }
    }

    /// JSON.TYPE key [path]
    ///
    /// - Parameters:
    ///   - key: The key
    ///   - path: JSON path (defaults to "$")
    /// - Returns: Type name or nil if not found
    public func jsonType(_ key: String, path: String = "$") throws -> String? {
        try withHandle { db in
            let result = key.withCString { keyPtr in
                path.withCString { pathPtr in
                    redlite_json_type(db, keyPtr, pathPtr)
                }
            }
            guard let ptr = result else { return nil }
            let str = String(cString: ptr)
            redlite_free_string(ptr)
            return str
        }
    }

    /// JSON.NUMINCRBY key path increment
    ///
    /// - Parameters:
    ///   - key: The key
    ///   - path: JSON path
    ///   - increment: Amount to increment
    /// - Returns: New value as JSON string
    public func jsonNumIncrBy(_ key: String, path: String, increment: Double) throws -> String? {
        try withHandle { db in
            let result = key.withCString { keyPtr in
                path.withCString { pathPtr in
                    redlite_json_numincrby(db, keyPtr, pathPtr, increment)
                }
            }
            guard let ptr = result else { return nil }
            let str = String(cString: ptr)
            redlite_free_string(ptr)
            return str
        }
    }

    /// JSON.STRAPPEND key path value
    ///
    /// - Parameters:
    ///   - key: The key
    ///   - path: JSON path
    ///   - value: String to append (JSON-encoded)
    /// - Returns: New length of string
    public func jsonStrAppend(_ key: String, path: String, value: String) throws -> Int64 {
        try withHandle { db in
            let result = key.withCString { keyPtr in
                path.withCString { pathPtr in
                    value.withCString { valuePtr in
                        redlite_json_strappend(db, keyPtr, pathPtr, valuePtr)
                    }
                }
            }
            if result < 0 {
                throw getLastError()
            }
            return result
        }
    }

    /// JSON.STRLEN key [path]
    ///
    /// - Parameters:
    ///   - key: The key
    ///   - path: JSON path (defaults to "$")
    /// - Returns: Length of string
    public func jsonStrLen(_ key: String, path: String = "$") throws -> Int64 {
        try withHandle { db in
            key.withCString { keyPtr in
                path.withCString { pathPtr in
                    redlite_json_strlen(db, keyPtr, pathPtr)
                }
            }
        }
    }

    /// JSON.ARRAPPEND key path value [value ...]
    ///
    /// - Parameters:
    ///   - key: The key
    ///   - path: JSON path
    ///   - values: JSON-encoded values to append
    /// - Returns: New length of array
    public func jsonArrAppend(_ key: String, path: String, values: [String]) throws -> Int64 {
        try withHandle { db in
            let valuePtrs = values.map { UnsafeMutablePointer(mutating: ($0 as NSString).utf8String) }
            return valuePtrs.withUnsafeBufferPointer { buffer in
                let result = key.withCString { keyPtr in
                    path.withCString { pathPtr in
                        redlite_json_arrappend(db, keyPtr, pathPtr, buffer.baseAddress, values.count)
                    }
                }
                if result < 0 {
                    // Error - would need to throw but we're in a closure
                    return result
                }
                return result
            }
        }
    }

    /// JSON.ARRLEN key [path]
    ///
    /// - Parameters:
    ///   - key: The key
    ///   - path: JSON path (defaults to "$")
    /// - Returns: Length of array
    public func jsonArrLen(_ key: String, path: String = "$") throws -> Int64 {
        try withHandle { db in
            key.withCString { keyPtr in
                path.withCString { pathPtr in
                    redlite_json_arrlen(db, keyPtr, pathPtr)
                }
            }
        }
    }

    /// JSON.ARRPOP key [path [index]]
    ///
    /// - Parameters:
    ///   - key: The key
    ///   - path: JSON path (defaults to "$")
    ///   - index: Index to pop from (defaults to -1, last element)
    /// - Returns: Popped value as JSON string
    public func jsonArrPop(_ key: String, path: String = "$", index: Int64 = -1) throws -> String? {
        try withHandle { db in
            let result = key.withCString { keyPtr in
                path.withCString { pathPtr in
                    redlite_json_arrpop(db, keyPtr, pathPtr, index)
                }
            }
            guard let ptr = result else { return nil }
            let str = String(cString: ptr)
            redlite_free_string(ptr)
            return str
        }
    }

    /// JSON.CLEAR key [path]
    ///
    /// - Parameters:
    ///   - key: The key
    ///   - path: JSON path (defaults to "$")
    /// - Returns: Number of values cleared
    public func jsonClear(_ key: String, path: String = "$") throws -> Int64 {
        try withHandle { db in
            key.withCString { keyPtr in
                path.withCString { pathPtr in
                    redlite_json_clear(db, keyPtr, pathPtr)
                }
            }
        }
    }
}

// MARK: - History Commands
extension Database {

    /// Enable history tracking globally
    ///
    /// - Parameters:
    ///   - retentionType: "unlimited", "time", or "count"
    ///   - retentionValue: Value for time (ms) or count retention
    public func historyEnableGlobal(retentionType: String = "unlimited", retentionValue: Int64 = 0) throws {
        try withHandle { db in
            let result = retentionType.withCString { typePtr in
                redlite_history_enable_global(db, typePtr, retentionValue)
            }
            if result < 0 {
                throw getLastError()
            }
        }
    }

    /// Enable history tracking for a specific database
    ///
    /// - Parameters:
    ///   - dbNum: Database number
    ///   - retentionType: "unlimited", "time", or "count"
    ///   - retentionValue: Value for time (ms) or count retention
    public func historyEnableDatabase(_ dbNum: Int32, retentionType: String = "unlimited", retentionValue: Int64 = 0) throws {
        try withHandle { db in
            let result = retentionType.withCString { typePtr in
                redlite_history_enable_database(db, dbNum, typePtr, retentionValue)
            }
            if result < 0 {
                throw getLastError()
            }
        }
    }

    /// Enable history tracking for a specific key
    ///
    /// - Parameters:
    ///   - key: Key to enable history for
    ///   - retentionType: "unlimited", "time", or "count"
    ///   - retentionValue: Value for time (ms) or count retention
    public func historyEnableKey(_ key: String, retentionType: String = "unlimited", retentionValue: Int64 = 0) throws {
        try withHandle { db in
            let result = key.withCString { keyPtr in
                retentionType.withCString { typePtr in
                    redlite_history_enable_key(db, keyPtr, typePtr, retentionValue)
                }
            }
            if result < 0 {
                throw getLastError()
            }
        }
    }

    /// Disable history tracking globally
    public func historyDisableGlobal() throws {
        try withHandle { db in
            let result = redlite_history_disable_global(db)
            if result < 0 {
                throw getLastError()
            }
        }
    }

    /// Disable history tracking for a specific database
    ///
    /// - Parameter dbNum: Database number
    public func historyDisableDatabase(_ dbNum: Int32) throws {
        try withHandle { db in
            let result = redlite_history_disable_database(db, dbNum)
            if result < 0 {
                throw getLastError()
            }
        }
    }

    /// Disable history tracking for a specific key
    ///
    /// - Parameter key: Key to disable history for
    public func historyDisableKey(_ key: String) throws {
        try withHandle { db in
            let result = key.withCString { keyPtr in
                redlite_history_disable_key(db, keyPtr)
            }
            if result < 0 {
                throw getLastError()
            }
        }
    }

    /// Check if history tracking is enabled for a key
    ///
    /// - Parameter key: Key to check
    /// - Returns: true if history is enabled
    public func isHistoryEnabled(_ key: String) throws -> Bool {
        try withHandle { db in
            key.withCString { keyPtr in
                redlite_is_history_enabled(db, keyPtr) == 1
            }
        }
    }
}

// MARK: - FTS (Full-Text Search) Commands
extension Database {

    /// Enable FTS indexing globally
    public func ftsEnableGlobal() throws {
        try withHandle { db in
            let result = redlite_fts_enable_global(db)
            if result < 0 {
                throw getLastError()
            }
        }
    }

    /// Enable FTS indexing for a specific database
    ///
    /// - Parameter dbNum: Database number
    public func ftsEnableDatabase(_ dbNum: Int32) throws {
        try withHandle { db in
            let result = redlite_fts_enable_database(db, dbNum)
            if result < 0 {
                throw getLastError()
            }
        }
    }

    /// Enable FTS indexing for keys matching a pattern
    ///
    /// - Parameter pattern: Glob pattern
    public func ftsEnablePattern(_ pattern: String) throws {
        try withHandle { db in
            let result = pattern.withCString { patternPtr in
                redlite_fts_enable_pattern(db, patternPtr)
            }
            if result < 0 {
                throw getLastError()
            }
        }
    }

    /// Enable FTS indexing for a specific key
    ///
    /// - Parameter key: Key to enable FTS for
    public func ftsEnableKey(_ key: String) throws {
        try withHandle { db in
            let result = key.withCString { keyPtr in
                redlite_fts_enable_key(db, keyPtr)
            }
            if result < 0 {
                throw getLastError()
            }
        }
    }

    /// Disable FTS indexing globally
    public func ftsDisableGlobal() throws {
        try withHandle { db in
            let result = redlite_fts_disable_global(db)
            if result < 0 {
                throw getLastError()
            }
        }
    }

    /// Disable FTS indexing for a specific database
    ///
    /// - Parameter dbNum: Database number
    public func ftsDisableDatabase(_ dbNum: Int32) throws {
        try withHandle { db in
            let result = redlite_fts_disable_database(db, dbNum)
            if result < 0 {
                throw getLastError()
            }
        }
    }

    /// Disable FTS indexing for keys matching a pattern
    ///
    /// - Parameter pattern: Glob pattern
    public func ftsDisablePattern(_ pattern: String) throws {
        try withHandle { db in
            let result = pattern.withCString { patternPtr in
                redlite_fts_disable_pattern(db, patternPtr)
            }
            if result < 0 {
                throw getLastError()
            }
        }
    }

    /// Disable FTS indexing for a specific key
    ///
    /// - Parameter key: Key to disable FTS for
    public func ftsDisableKey(_ key: String) throws {
        try withHandle { db in
            let result = key.withCString { keyPtr in
                redlite_fts_disable_key(db, keyPtr)
            }
            if result < 0 {
                throw getLastError()
            }
        }
    }

    /// Check if FTS indexing is enabled for a key
    ///
    /// - Parameter key: Key to check
    /// - Returns: true if FTS is enabled
    public func isFtsEnabled(_ key: String) throws -> Bool {
        try withHandle { db in
            key.withCString { keyPtr in
                redlite_is_fts_enabled(db, keyPtr) == 1
            }
        }
    }
}

// MARK: - KeyInfo Command
extension Database {

    /// KEYINFO - Get detailed information about a key
    ///
    /// - Parameter key: Key to get info for
    /// - Returns: KeyInfo or nil if key doesn't exist
    public func keyinfo(_ key: String) throws -> KeyInfo? {
        try withHandle { db in
            let info = key.withCString { keyPtr in
                redlite_keyinfo(db, keyPtr)
            }

            guard info.valid != 0 else {
                var mutableInfo = info
                redlite_free_keyinfo(mutableInfo)
                return nil
            }

            let keyType = info.key_type != nil ? String(cString: info.key_type) : "none"
            let result = KeyInfo(
                keyType: keyType,
                ttl: info.ttl,
                createdAt: info.created_at,
                updatedAt: info.updated_at
            )

            var mutableInfo = info
            redlite_free_keyinfo(mutableInfo)
            return result
        }
    }
}
