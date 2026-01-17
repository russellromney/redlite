import CRedlite
import Foundation

// MARK: - Hash Commands
extension Database {

    /// HSET key field value - Set hash field
    ///
    /// - Parameters:
    ///   - key: The hash key
    ///   - field: The field name
    ///   - value: The value to set
    /// - Returns: Number of fields added (1 if new, 0 if updated)
    @discardableResult
    public func hset(_ key: String, field: String, value: Data) throws -> Int64 {
        try hset(key, fields: [(field, value)])
    }

    /// HSET with String value
    @discardableResult
    public func hset(_ key: String, field: String, value: String) throws -> Int64 {
        try hset(key, field: field, value: Data(value.utf8))
    }

    /// HSET key field value [field value ...] - Set multiple hash fields
    ///
    /// - Parameters:
    ///   - key: The hash key
    ///   - fields: Array of (field, value) tuples
    /// - Returns: Number of fields added
    @discardableResult
    public func hset(_ key: String, fields: [(String, Data)]) throws -> Int64 {
        guard !fields.isEmpty else { return 0 }

        return try withHandle { db in
            // Build field names array
            var fieldBuffers: [[CChar]] = fields.map { Array($0.0.utf8CString) }

            // Build values array
            var valuesArray: [RedliteBytes] = []
            valuesArray.reserveCapacity(fields.count)

            for (_, data) in fields {
                data.withUnsafeBytes { buffer in
                    let bytes = RedliteBytes(
                        data: UnsafeMutablePointer(mutating: buffer.baseAddress?.assumingMemoryBound(to: UInt8.self)),
                        len: buffer.count
                    )
                    valuesArray.append(bytes)
                }
            }

            // Get field pointers
            var fieldPtrs: [UnsafePointer<CChar>?] = []
            for i in 0..<fieldBuffers.count {
                fieldBuffers[i].withUnsafeBufferPointer { buffer in
                    fieldPtrs.append(buffer.baseAddress)
                }
            }

            return fieldPtrs.withUnsafeBufferPointer { fieldBuffer in
                valuesArray.withUnsafeBufferPointer { valueBuffer in
                    key.withCString { keyPtr in
                        redlite_hset(db, keyPtr, fieldBuffer.baseAddress, valueBuffer.baseAddress, fields.count)
                    }
                }
            }
        }
    }

    /// HSET with dictionary
    @discardableResult
    public func hset(_ key: String, _ dict: [String: String]) throws -> Int64 {
        try hset(key, fields: dict.map { ($0.key, Data($0.value.utf8)) })
    }

    /// HGET key field - Get hash field value
    ///
    /// - Parameters:
    ///   - key: The hash key
    ///   - field: The field name
    /// - Returns: The field value, or nil if field doesn't exist
    public func hget(_ key: String, field: String) throws -> Data? {
        try withHandle { db in
            var result = key.withCString { kPtr in
                field.withCString { fPtr in
                    redlite_hget(db, kPtr, fPtr)
                }
            }
            defer {
                var wrapper = FFIBytes(result)
                wrapper.free()
            }
            return FFIBytes(result).toData()
        }
    }

    /// HGET as String
    public func hgetString(_ key: String, field: String) throws -> String? {
        guard let data = try hget(key, field: field) else { return nil }
        return String(data: data, encoding: .utf8)
    }

    /// HDEL key field [field ...] - Delete hash fields
    ///
    /// - Parameters:
    ///   - key: The hash key
    ///   - fields: Fields to delete
    /// - Returns: Number of fields deleted
    @discardableResult
    public func hdel(_ key: String, fields: [String]) throws -> Int64 {
        guard !fields.isEmpty else { return 0 }
        return try withHandle { db in
            withCStringArray(fields) { fieldsPtr, count in
                key.withCString { redlite_hdel(db, $0, fieldsPtr, count) }
            }
        }
    }

    /// HDEL with variadic fields
    @discardableResult
    public func hdel(_ key: String, _ fields: String...) throws -> Int64 {
        try hdel(key, fields: fields)
    }

    /// HEXISTS key field - Check if field exists in hash
    ///
    /// - Parameters:
    ///   - key: The hash key
    ///   - field: The field name
    /// - Returns: true if field exists
    public func hexists(_ key: String, field: String) throws -> Bool {
        try withHandle { db in
            key.withCString { kPtr in
                field.withCString { fPtr in
                    redlite_hexists(db, kPtr, fPtr)
                }
            } == 1
        }
    }

    /// HLEN key - Get number of fields in hash
    ///
    /// - Parameter key: The hash key
    /// - Returns: Number of fields
    public func hlen(_ key: String) throws -> Int64 {
        try withHandle { db in
            key.withCString { redlite_hlen(db, $0) }
        }
    }

    /// HKEYS key - Get all field names in hash
    ///
    /// - Parameter key: The hash key
    /// - Returns: Array of field names
    public func hkeys(_ key: String) throws -> [String] {
        try withHandle { db in
            var result = key.withCString { redlite_hkeys(db, $0) }
            defer {
                var wrapper = FFIStringArray(result)
                wrapper.free()
            }
            return FFIStringArray(result).toStrings()
        }
    }

    /// HVALS key - Get all values in hash
    ///
    /// - Parameter key: The hash key
    /// - Returns: Array of values
    public func hvals(_ key: String) throws -> [Data] {
        try withHandle { db in
            var result = key.withCString { redlite_hvals(db, $0) }
            defer {
                var wrapper = FFIBytesArray(result)
                wrapper.free()
            }
            return FFIBytesArray(result).toNonNilDataArray()
        }
    }

    /// HVALS as Strings
    public func hvalsStrings(_ key: String) throws -> [String] {
        try hvals(key).compactMap { String(data: $0, encoding: .utf8) }
    }

    /// HINCRBY key field increment - Increment hash field by integer
    ///
    /// - Parameters:
    ///   - key: The hash key
    ///   - field: The field name
    ///   - increment: Amount to increment by
    /// - Returns: New value after increment
    @discardableResult
    public func hincrby(_ key: String, field: String, increment: Int64) throws -> Int64 {
        try withHandle { db in
            key.withCString { kPtr in
                field.withCString { fPtr in
                    redlite_hincrby(db, kPtr, fPtr, increment)
                }
            }
        }
    }

    /// HGETALL key - Get all fields and values
    ///
    /// - Parameter key: The hash key
    /// - Returns: Dictionary of field names to values
    public func hgetall(_ key: String) throws -> [String: Data] {
        try withHandle { db in
            var result = key.withCString { redlite_hgetall(db, $0) }
            defer {
                var wrapper = FFIBytesArray(result)
                wrapper.free()
            }

            let items = FFIBytesArray(result).toDataArray()
            var dict: [String: Data] = [:]

            // Items are field, value, field, value...
            for i in stride(from: 0, to: items.count - 1, by: 2) {
                if let fieldData = items[i],
                   let field = String(data: fieldData, encoding: .utf8),
                   let value = items[i + 1] {
                    dict[field] = value
                }
            }

            return dict
        }
    }

    /// HGETALL as String dictionary
    public func hgetallStrings(_ key: String) throws -> [String: String] {
        let dict = try hgetall(key)
        var result: [String: String] = [:]
        for (k, v) in dict {
            if let str = String(data: v, encoding: .utf8) {
                result[k] = str
            }
        }
        return result
    }

    /// HMGET key field [field ...] - Get multiple hash fields
    ///
    /// - Parameters:
    ///   - key: The hash key
    ///   - fields: Fields to get
    /// - Returns: Array of values (nil for non-existent fields)
    public func hmget(_ key: String, fields: [String]) throws -> [Data?] {
        guard !fields.isEmpty else { return [] }
        return try withHandle { db in
            var result = withCStringArray(fields) { fieldsPtr, count in
                key.withCString { redlite_hmget(db, $0, fieldsPtr, count) }
            }
            defer {
                var wrapper = FFIBytesArray(result)
                wrapper.free()
            }
            return FFIBytesArray(result).toDataArray()
        }
    }

    /// HMGET as Strings
    public func hmgetStrings(_ key: String, fields: [String]) throws -> [String?] {
        try hmget(key, fields: fields).map { data in
            data.flatMap { String(data: $0, encoding: .utf8) }
        }
    }
}
