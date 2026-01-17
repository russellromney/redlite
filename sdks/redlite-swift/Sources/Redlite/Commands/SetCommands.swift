import CRedlite
import Foundation

// MARK: - Set Commands
extension Database {

    /// SADD key member [member ...] - Add members to set
    ///
    /// - Parameters:
    ///   - key: The set key
    ///   - members: Members to add
    /// - Returns: Number of members added (not counting existing)
    @discardableResult
    public func sadd(_ key: String, members: [Data]) throws -> Int64 {
        guard !members.isEmpty else { return 0 }

        return try withHandle { db in
            var bytesArray: [RedliteBytes] = []
            bytesArray.reserveCapacity(members.count)

            for data in members {
                data.withUnsafeBytes { buffer in
                    let bytes = RedliteBytes(
                        data: UnsafeMutablePointer(mutating: buffer.baseAddress?.assumingMemoryBound(to: UInt8.self)),
                        len: buffer.count
                    )
                    bytesArray.append(bytes)
                }
            }

            return bytesArray.withUnsafeBufferPointer { ptr in
                key.withCString { redlite_sadd(db, $0, ptr.baseAddress, members.count) }
            }
        }
    }

    /// SADD with String members
    @discardableResult
    public func sadd(_ key: String, members: [String]) throws -> Int64 {
        try sadd(key, members: members.map { Data($0.utf8) })
    }

    /// SADD with variadic members
    @discardableResult
    public func sadd(_ key: String, _ members: String...) throws -> Int64 {
        try sadd(key, members: members)
    }

    /// SREM key member [member ...] - Remove members from set
    ///
    /// - Parameters:
    ///   - key: The set key
    ///   - members: Members to remove
    /// - Returns: Number of members removed
    @discardableResult
    public func srem(_ key: String, members: [Data]) throws -> Int64 {
        guard !members.isEmpty else { return 0 }

        return try withHandle { db in
            var bytesArray: [RedliteBytes] = []
            bytesArray.reserveCapacity(members.count)

            for data in members {
                data.withUnsafeBytes { buffer in
                    let bytes = RedliteBytes(
                        data: UnsafeMutablePointer(mutating: buffer.baseAddress?.assumingMemoryBound(to: UInt8.self)),
                        len: buffer.count
                    )
                    bytesArray.append(bytes)
                }
            }

            return bytesArray.withUnsafeBufferPointer { ptr in
                key.withCString { redlite_srem(db, $0, ptr.baseAddress, members.count) }
            }
        }
    }

    /// SREM with String members
    @discardableResult
    public func srem(_ key: String, members: [String]) throws -> Int64 {
        try srem(key, members: members.map { Data($0.utf8) })
    }

    /// SREM with variadic members
    @discardableResult
    public func srem(_ key: String, _ members: String...) throws -> Int64 {
        try srem(key, members: members)
    }

    /// SMEMBERS key - Get all members of set
    ///
    /// - Parameter key: The set key
    /// - Returns: Array of set members
    public func smembers(_ key: String) throws -> [Data] {
        try withHandle { db in
            var result = key.withCString { redlite_smembers(db, $0) }
            defer {
                var wrapper = FFIBytesArray(result)
                wrapper.free()
            }
            return FFIBytesArray(result).toNonNilDataArray()
        }
    }

    /// SMEMBERS as Strings
    public func smembersStrings(_ key: String) throws -> [String] {
        try smembers(key).compactMap { String(data: $0, encoding: .utf8) }
    }

    /// SMEMBERS as Set<String>
    public func smembersSet(_ key: String) throws -> Set<String> {
        Set(try smembersStrings(key))
    }

    /// SISMEMBER key member - Check if member exists in set
    ///
    /// - Parameters:
    ///   - key: The set key
    ///   - member: Member to check
    /// - Returns: true if member exists
    public func sismember(_ key: String, member: Data) throws -> Bool {
        try withHandle { db in
            key.withCString { keyPtr in
                member.withUnsafeBytes { buffer in
                    let memberPtr = buffer.baseAddress?.assumingMemoryBound(to: UInt8.self)
                    return redlite_sismember(db, keyPtr, memberPtr, buffer.count)
                }
            } == 1
        }
    }

    /// SISMEMBER with String member
    public func sismember(_ key: String, member: String) throws -> Bool {
        try sismember(key, member: Data(member.utf8))
    }

    /// SCARD key - Get number of members in set
    ///
    /// - Parameter key: The set key
    /// - Returns: Number of members
    public func scard(_ key: String) throws -> Int64 {
        try withHandle { db in
            key.withCString { redlite_scard(db, $0) }
        }
    }
}
