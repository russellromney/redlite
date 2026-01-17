import CRedlite
import Foundation

// MARK: - List Commands
extension Database {

    /// LPUSH key value [value ...] - Push values to head of list
    ///
    /// - Parameters:
    ///   - key: The list key
    ///   - values: Values to push
    /// - Returns: New length of list
    @discardableResult
    public func lpush(_ key: String, values: [Data]) throws -> Int64 {
        guard !values.isEmpty else { return try llen(key) }

        return try withHandle { db in
            var bytesArray: [RedliteBytes] = []
            bytesArray.reserveCapacity(values.count)

            for data in values {
                data.withUnsafeBytes { buffer in
                    let bytes = RedliteBytes(
                        data: UnsafeMutablePointer(mutating: buffer.baseAddress?.assumingMemoryBound(to: UInt8.self)),
                        len: buffer.count
                    )
                    bytesArray.append(bytes)
                }
            }

            return bytesArray.withUnsafeBufferPointer { ptr in
                key.withCString { redlite_lpush(db, $0, ptr.baseAddress, values.count) }
            }
        }
    }

    /// LPUSH with String values
    @discardableResult
    public func lpush(_ key: String, values: [String]) throws -> Int64 {
        try lpush(key, values: values.map { Data($0.utf8) })
    }

    /// LPUSH with variadic values
    @discardableResult
    public func lpush(_ key: String, _ values: String...) throws -> Int64 {
        try lpush(key, values: values)
    }

    /// RPUSH key value [value ...] - Push values to tail of list
    ///
    /// - Parameters:
    ///   - key: The list key
    ///   - values: Values to push
    /// - Returns: New length of list
    @discardableResult
    public func rpush(_ key: String, values: [Data]) throws -> Int64 {
        guard !values.isEmpty else { return try llen(key) }

        return try withHandle { db in
            var bytesArray: [RedliteBytes] = []
            bytesArray.reserveCapacity(values.count)

            for data in values {
                data.withUnsafeBytes { buffer in
                    let bytes = RedliteBytes(
                        data: UnsafeMutablePointer(mutating: buffer.baseAddress?.assumingMemoryBound(to: UInt8.self)),
                        len: buffer.count
                    )
                    bytesArray.append(bytes)
                }
            }

            return bytesArray.withUnsafeBufferPointer { ptr in
                key.withCString { redlite_rpush(db, $0, ptr.baseAddress, values.count) }
            }
        }
    }

    /// RPUSH with String values
    @discardableResult
    public func rpush(_ key: String, values: [String]) throws -> Int64 {
        try rpush(key, values: values.map { Data($0.utf8) })
    }

    /// RPUSH with variadic values
    @discardableResult
    public func rpush(_ key: String, _ values: String...) throws -> Int64 {
        try rpush(key, values: values)
    }

    /// LPOP key [count] - Pop values from head of list
    ///
    /// - Parameters:
    ///   - key: The list key
    ///   - count: Number of elements to pop (default 1)
    /// - Returns: Array of popped values
    public func lpop(_ key: String, count: Int = 1) throws -> [Data] {
        guard count > 0 else { return [] }
        return try withHandle { db in
            var result = key.withCString { redlite_lpop(db, $0, count) }
            defer {
                var wrapper = FFIBytesArray(result)
                wrapper.free()
            }
            return FFIBytesArray(result).toNonNilDataArray()
        }
    }

    /// LPOP single element as Data
    public func lpopOne(_ key: String) throws -> Data? {
        let results = try lpop(key, count: 1)
        return results.first
    }

    /// LPOP as Strings
    public func lpopStrings(_ key: String, count: Int = 1) throws -> [String] {
        try lpop(key, count: count).compactMap { String(data: $0, encoding: .utf8) }
    }

    /// RPOP key [count] - Pop values from tail of list
    ///
    /// - Parameters:
    ///   - key: The list key
    ///   - count: Number of elements to pop (default 1)
    /// - Returns: Array of popped values
    public func rpop(_ key: String, count: Int = 1) throws -> [Data] {
        guard count > 0 else { return [] }
        return try withHandle { db in
            var result = key.withCString { redlite_rpop(db, $0, count) }
            defer {
                var wrapper = FFIBytesArray(result)
                wrapper.free()
            }
            return FFIBytesArray(result).toNonNilDataArray()
        }
    }

    /// RPOP single element as Data
    public func rpopOne(_ key: String) throws -> Data? {
        let results = try rpop(key, count: 1)
        return results.first
    }

    /// RPOP as Strings
    public func rpopStrings(_ key: String, count: Int = 1) throws -> [String] {
        try rpop(key, count: count).compactMap { String(data: $0, encoding: .utf8) }
    }

    /// LLEN key - Get list length
    ///
    /// - Parameter key: The list key
    /// - Returns: Length of list
    public func llen(_ key: String) throws -> Int64 {
        try withHandle { db in
            key.withCString { redlite_llen(db, $0) }
        }
    }

    /// LRANGE key start stop - Get range of elements
    ///
    /// - Parameters:
    ///   - key: The list key
    ///   - start: Start index (0-based, negative counts from end)
    ///   - stop: Stop index (inclusive, negative counts from end)
    /// - Returns: Array of elements in range
    public func lrange(_ key: String, start: Int64, stop: Int64) throws -> [Data] {
        try withHandle { db in
            var result = key.withCString { redlite_lrange(db, $0, start, stop) }
            defer {
                var wrapper = FFIBytesArray(result)
                wrapper.free()
            }
            return FFIBytesArray(result).toNonNilDataArray()
        }
    }

    /// LRANGE as Strings
    public func lrangeStrings(_ key: String, start: Int64, stop: Int64) throws -> [String] {
        try lrange(key, start: start, stop: stop).compactMap { String(data: $0, encoding: .utf8) }
    }

    /// LINDEX key index - Get element by index
    ///
    /// - Parameters:
    ///   - key: The list key
    ///   - index: Element index (negative counts from end)
    /// - Returns: Element at index, or nil if out of range
    public func lindex(_ key: String, index: Int64) throws -> Data? {
        try withHandle { db in
            var result = key.withCString { redlite_lindex(db, $0, index) }
            defer {
                var wrapper = FFIBytes(result)
                wrapper.free()
            }
            return FFIBytes(result).toData()
        }
    }

    /// LINDEX as String
    public func lindexString(_ key: String, index: Int64) throws -> String? {
        guard let data = try lindex(key, index: index) else { return nil }
        return String(data: data, encoding: .utf8)
    }
}
