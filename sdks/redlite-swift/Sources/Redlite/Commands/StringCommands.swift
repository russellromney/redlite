import CRedlite
import Foundation

// MARK: - String Commands
extension Database {

    // MARK: - GET/SET

    /// GET key - Returns the value of key, or nil if key does not exist
    ///
    /// - Parameter key: The key to get
    /// - Returns: The value as Data, or nil if key doesn't exist
    public func get(_ key: String) throws -> Data? {
        try withHandle { db in
            var result = key.withCString { redlite_get(db, $0) }
            defer {
                var wrapper = FFIBytes(result)
                wrapper.free()
            }
            return FFIBytes(result).toData()
        }
    }

    /// GET key as String (UTF-8 decoded)
    ///
    /// - Parameter key: The key to get
    /// - Returns: The value as String, or nil if key doesn't exist or isn't valid UTF-8
    public func getString(_ key: String) throws -> String? {
        guard let data = try get(key) else { return nil }
        return String(data: data, encoding: .utf8)
    }

    /// SET key value with optional TTL
    ///
    /// - Parameters:
    ///   - key: The key to set
    ///   - value: The value to store as Data
    ///   - ttl: Optional TTL in seconds (0 or nil for no expiration)
    /// - Throws: `RedliteError` on failure
    public func set(_ key: String, value: Data, ttl: Int64? = nil) throws {
        try withHandle { db in
            let result = key.withCString { keyPtr in
                value.withUnsafeBytes { buffer in
                    let valuePtr = buffer.baseAddress?.assumingMemoryBound(to: UInt8.self)
                    return redlite_set(db, keyPtr, valuePtr, buffer.count, ttl ?? 0)
                }
            }
            if result != 0 {
                throw getLastError()
            }
        }
    }

    /// SET key value (String convenience)
    ///
    /// - Parameters:
    ///   - key: The key to set
    ///   - value: The value to store as String
    ///   - ttl: Optional TTL in seconds
    public func set(_ key: String, value: String, ttl: Int64? = nil) throws {
        try set(key, value: Data(value.utf8), ttl: ttl)
    }

    /// SETEX key seconds value - Set with expiration in seconds
    ///
    /// - Parameters:
    ///   - key: The key to set
    ///   - seconds: TTL in seconds
    ///   - value: The value to store
    public func setex(_ key: String, seconds: Int64, value: Data) throws {
        try withHandle { db in
            let result = key.withCString { keyPtr in
                value.withUnsafeBytes { buffer in
                    let valuePtr = buffer.baseAddress?.assumingMemoryBound(to: UInt8.self)
                    return redlite_setex(db, keyPtr, seconds, valuePtr, buffer.count)
                }
            }
            if result != 0 {
                throw getLastError()
            }
        }
    }

    /// SETEX with String value
    public func setex(_ key: String, seconds: Int64, value: String) throws {
        try setex(key, seconds: seconds, value: Data(value.utf8))
    }

    /// PSETEX key milliseconds value - Set with expiration in milliseconds
    ///
    /// - Parameters:
    ///   - key: The key to set
    ///   - milliseconds: TTL in milliseconds
    ///   - value: The value to store
    public func psetex(_ key: String, milliseconds: Int64, value: Data) throws {
        try withHandle { db in
            let result = key.withCString { keyPtr in
                value.withUnsafeBytes { buffer in
                    let valuePtr = buffer.baseAddress?.assumingMemoryBound(to: UInt8.self)
                    return redlite_psetex(db, keyPtr, milliseconds, valuePtr, buffer.count)
                }
            }
            if result != 0 {
                throw getLastError()
            }
        }
    }

    /// PSETEX with String value
    public func psetex(_ key: String, milliseconds: Int64, value: String) throws {
        try psetex(key, milliseconds: milliseconds, value: Data(value.utf8))
    }

    /// GETDEL key - Get value and delete key atomically
    ///
    /// - Parameter key: The key to get and delete
    /// - Returns: The value, or nil if key doesn't exist
    public func getdel(_ key: String) throws -> Data? {
        try withHandle { db in
            var result = key.withCString { redlite_getdel(db, $0) }
            defer {
                var wrapper = FFIBytes(result)
                wrapper.free()
            }
            return FFIBytes(result).toData()
        }
    }

    /// GETDEL as String
    public func getdelString(_ key: String) throws -> String? {
        guard let data = try getdel(key) else { return nil }
        return String(data: data, encoding: .utf8)
    }

    // MARK: - INCR/DECR

    /// INCR key - Increment integer value by 1
    ///
    /// - Parameter key: The key to increment
    /// - Returns: The new value after incrementing
    @discardableResult
    public func incr(_ key: String) throws -> Int64 {
        try withHandle { db in
            let result = key.withCString { redlite_incr(db, $0) }
            if result == Int64.min {
                throw getLastError()
            }
            return result
        }
    }

    /// DECR key - Decrement integer value by 1
    ///
    /// - Parameter key: The key to decrement
    /// - Returns: The new value after decrementing
    @discardableResult
    public func decr(_ key: String) throws -> Int64 {
        try withHandle { db in
            let result = key.withCString { redlite_decr(db, $0) }
            if result == Int64.min {
                throw getLastError()
            }
            return result
        }
    }

    /// INCRBY key increment - Increment by specified amount
    ///
    /// - Parameters:
    ///   - key: The key to increment
    ///   - increment: Amount to increment by
    /// - Returns: The new value
    @discardableResult
    public func incrby(_ key: String, increment: Int64) throws -> Int64 {
        try withHandle { db in
            let result = key.withCString { redlite_incrby(db, $0, increment) }
            if result == Int64.min {
                throw getLastError()
            }
            return result
        }
    }

    /// DECRBY key decrement - Decrement by specified amount
    ///
    /// - Parameters:
    ///   - key: The key to decrement
    ///   - decrement: Amount to decrement by
    /// - Returns: The new value
    @discardableResult
    public func decrby(_ key: String, decrement: Int64) throws -> Int64 {
        try withHandle { db in
            let result = key.withCString { redlite_decrby(db, $0, decrement) }
            if result == Int64.min {
                throw getLastError()
            }
            return result
        }
    }

    /// INCRBYFLOAT key increment - Increment by float amount
    ///
    /// - Parameters:
    ///   - key: The key to increment
    ///   - increment: Float amount to increment by
    /// - Returns: The new value as Double
    @discardableResult
    public func incrbyfloat(_ key: String, increment: Double) throws -> Double {
        try withHandle { db in
            guard let resultPtr = key.withCString({ redlite_incrbyfloat(db, $0, increment) }) else {
                throw getLastError()
            }
            let resultStr = String(cString: resultPtr)
            redlite_free_string(resultPtr)
            guard let result = Double(resultStr) else {
                throw RedliteError.operationFailed("Invalid float result: \(resultStr)")
            }
            return result
        }
    }

    // MARK: - String Manipulation

    /// APPEND key value - Append to existing value
    ///
    /// - Parameters:
    ///   - key: The key to append to
    ///   - value: The value to append
    /// - Returns: The new length of the string
    @discardableResult
    public func append(_ key: String, value: Data) throws -> Int64 {
        try withHandle { db in
            let result = key.withCString { keyPtr in
                value.withUnsafeBytes { buffer in
                    let valuePtr = buffer.baseAddress?.assumingMemoryBound(to: UInt8.self)
                    return redlite_append(db, keyPtr, valuePtr, buffer.count)
                }
            }
            if result < 0 {
                throw getLastError()
            }
            return result
        }
    }

    /// APPEND with String value
    @discardableResult
    public func append(_ key: String, value: String) throws -> Int64 {
        try append(key, value: Data(value.utf8))
    }

    /// STRLEN key - Get length of string value
    ///
    /// - Parameter key: The key to check
    /// - Returns: Length of the string, or 0 if key doesn't exist
    public func strlen(_ key: String) throws -> Int64 {
        try withHandle { db in
            key.withCString { redlite_strlen(db, $0) }
        }
    }

    /// GETRANGE key start end - Get substring
    ///
    /// - Parameters:
    ///   - key: The key to get from
    ///   - start: Start index (0-based, negative counts from end)
    ///   - end: End index (inclusive, negative counts from end)
    /// - Returns: The substring as Data
    public func getrange(_ key: String, start: Int64, end: Int64) throws -> Data {
        try withHandle { db in
            var result = key.withCString { redlite_getrange(db, $0, start, end) }
            defer {
                var wrapper = FFIBytes(result)
                wrapper.free()
            }
            return FFIBytes(result).toData() ?? Data()
        }
    }

    /// GETRANGE as String
    public func getrangeString(_ key: String, start: Int64, end: Int64) throws -> String {
        let data = try getrange(key, start: start, end: end)
        return String(data: data, encoding: .utf8) ?? ""
    }

    /// SETRANGE key offset value - Overwrite part of string
    ///
    /// - Parameters:
    ///   - key: The key to modify
    ///   - offset: Starting offset
    ///   - value: The value to write
    /// - Returns: The new length of the string
    @discardableResult
    public func setrange(_ key: String, offset: Int64, value: Data) throws -> Int64 {
        try withHandle { db in
            let result = key.withCString { keyPtr in
                value.withUnsafeBytes { buffer in
                    let valuePtr = buffer.baseAddress?.assumingMemoryBound(to: UInt8.self)
                    return redlite_setrange(db, keyPtr, offset, valuePtr, buffer.count)
                }
            }
            if result < 0 {
                throw getLastError()
            }
            return result
        }
    }

    /// SETRANGE with String value
    @discardableResult
    public func setrange(_ key: String, offset: Int64, value: String) throws -> Int64 {
        try setrange(key, offset: offset, value: Data(value.utf8))
    }

    // MARK: - Multi-Key Operations

    /// MGET key [key ...] - Get multiple keys
    ///
    /// - Parameter keys: The keys to get
    /// - Returns: Array of values (nil for missing keys)
    public func mget(_ keys: [String]) throws -> [Data?] {
        guard !keys.isEmpty else { return [] }
        return try withHandle { db in
            var result = withCStringArray(keys) { keysPtr, count in
                redlite_mget(db, keysPtr, count)
            }
            defer {
                var wrapper = FFIBytesArray(result)
                wrapper.free()
            }
            return FFIBytesArray(result).toDataArray()
        }
    }

    /// MGET as Strings
    public func mgetStrings(_ keys: [String]) throws -> [String?] {
        try mget(keys).map { data in
            data.flatMap { String(data: $0, encoding: .utf8) }
        }
    }

    /// MSET key value [key value ...] - Set multiple key-value pairs
    ///
    /// - Parameter pairs: Array of (key, value) tuples
    public func mset(_ pairs: [(String, Data)]) throws {
        guard !pairs.isEmpty else { return }
        try withHandle { db in
            // We need to build arrays that stay alive during the call
            var keyBuffers: [[CChar]] = pairs.map { Array($0.0.utf8CString) }
            var kvArray: [RedliteKV] = []
            kvArray.reserveCapacity(pairs.count)

            for i in 0..<pairs.count {
                keyBuffers[i].withUnsafeBufferPointer { keyBuffer in
                    pairs[i].1.withUnsafeBytes { valueBuffer in
                        let kv = RedliteKV(
                            key: keyBuffer.baseAddress,
                            value: valueBuffer.baseAddress?.assumingMemoryBound(to: UInt8.self),
                            value_len: valueBuffer.count
                        )
                        kvArray.append(kv)
                    }
                }
            }

            let result = kvArray.withUnsafeBufferPointer { buffer in
                redlite_mset(db, buffer.baseAddress, pairs.count)
            }

            if result != 0 {
                throw getLastError()
            }
        }
    }

    /// MSET with String values
    public func mset(_ pairs: [(String, String)]) throws {
        try mset(pairs.map { ($0.0, Data($0.1.utf8)) })
    }

    /// MSET with dictionary
    public func mset(_ dict: [String: String]) throws {
        try mset(dict.map { ($0.key, Data($0.value.utf8)) })
    }
}
