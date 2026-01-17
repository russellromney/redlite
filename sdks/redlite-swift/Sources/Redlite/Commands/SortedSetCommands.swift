import CRedlite
import Foundation

// MARK: - Sorted Set Commands
extension Database {

    /// ZADD key score member [score member ...] - Add members to sorted set
    ///
    /// - Parameters:
    ///   - key: The sorted set key
    ///   - members: Array of ZMember (score + member)
    /// - Returns: Number of members added (not counting updates)
    @discardableResult
    public func zadd(_ key: String, members: [ZMember]) throws -> Int64 {
        guard !members.isEmpty else { return 0 }

        return try withHandle { db in
            var zmemberArray: [RedliteZMember] = []
            zmemberArray.reserveCapacity(members.count)

            for member in members {
                member.member.withUnsafeBytes { buffer in
                    let zmember = RedliteZMember(
                        score: member.score,
                        member: buffer.baseAddress?.assumingMemoryBound(to: UInt8.self),
                        member_len: buffer.count
                    )
                    zmemberArray.append(zmember)
                }
            }

            return zmemberArray.withUnsafeBufferPointer { buffer in
                key.withCString { redlite_zadd(db, $0, buffer.baseAddress, members.count) }
            }
        }
    }

    /// ZADD single member with score
    @discardableResult
    public func zadd(_ key: String, score: Double, member: String) throws -> Int64 {
        try zadd(key, members: [ZMember(score: score, member: member)])
    }

    /// ZADD with (score, member) tuples
    @discardableResult
    public func zadd(_ key: String, _ pairs: (Double, String)...) throws -> Int64 {
        try zadd(key, members: pairs.map { ZMember(score: $0.0, member: $0.1) })
    }

    /// ZREM key member [member ...] - Remove members from sorted set
    ///
    /// - Parameters:
    ///   - key: The sorted set key
    ///   - members: Members to remove
    /// - Returns: Number of members removed
    @discardableResult
    public func zrem(_ key: String, members: [Data]) throws -> Int64 {
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
                key.withCString { redlite_zrem(db, $0, ptr.baseAddress, members.count) }
            }
        }
    }

    /// ZREM with String members
    @discardableResult
    public func zrem(_ key: String, members: [String]) throws -> Int64 {
        try zrem(key, members: members.map { Data($0.utf8) })
    }

    /// ZREM with variadic members
    @discardableResult
    public func zrem(_ key: String, _ members: String...) throws -> Int64 {
        try zrem(key, members: members)
    }

    /// ZSCORE key member - Get score of member
    ///
    /// - Parameters:
    ///   - key: The sorted set key
    ///   - member: Member to get score for
    /// - Returns: Score of member, or nil if member doesn't exist
    public func zscore(_ key: String, member: Data) throws -> Double? {
        try withHandle { db in
            let result = key.withCString { keyPtr in
                member.withUnsafeBytes { buffer in
                    let memberPtr = buffer.baseAddress?.assumingMemoryBound(to: UInt8.self)
                    return redlite_zscore(db, keyPtr, memberPtr, buffer.count)
                }
            }
            // NaN indicates not found
            if result.isNaN {
                return nil
            }
            return result
        }
    }

    /// ZSCORE with String member
    public func zscore(_ key: String, member: String) throws -> Double? {
        try zscore(key, member: Data(member.utf8))
    }

    /// ZCARD key - Get number of members in sorted set
    ///
    /// - Parameter key: The sorted set key
    /// - Returns: Number of members
    public func zcard(_ key: String) throws -> Int64 {
        try withHandle { db in
            key.withCString { redlite_zcard(db, $0) }
        }
    }

    /// ZCOUNT key min max - Count members with score in range
    ///
    /// - Parameters:
    ///   - key: The sorted set key
    ///   - min: Minimum score (inclusive)
    ///   - max: Maximum score (inclusive)
    /// - Returns: Number of members in score range
    public func zcount(_ key: String, min: Double, max: Double) throws -> Int64 {
        try withHandle { db in
            key.withCString { redlite_zcount(db, $0, min, max) }
        }
    }

    /// ZINCRBY key increment member - Increment member's score
    ///
    /// - Parameters:
    ///   - key: The sorted set key
    ///   - member: Member to increment
    ///   - increment: Amount to increment by
    /// - Returns: New score after increment
    @discardableResult
    public func zincrby(_ key: String, member: Data, increment: Double) throws -> Double {
        try withHandle { db in
            key.withCString { keyPtr in
                member.withUnsafeBytes { buffer in
                    let memberPtr = buffer.baseAddress?.assumingMemoryBound(to: UInt8.self)
                    return redlite_zincrby(db, keyPtr, increment, memberPtr, buffer.count)
                }
            }
        }
    }

    /// ZINCRBY with String member
    @discardableResult
    public func zincrby(_ key: String, member: String, increment: Double) throws -> Double {
        try zincrby(key, member: Data(member.utf8), increment: increment)
    }

    /// ZRANGE key start stop [WITHSCORES] - Get range of members by rank
    ///
    /// - Parameters:
    ///   - key: The sorted set key
    ///   - start: Start index (0-based, negative counts from end)
    ///   - stop: Stop index (inclusive, negative counts from end)
    ///   - withScores: If true, returns members with scores
    /// - Returns: Array of members (or ZMember if withScores)
    public func zrange(_ key: String, start: Int64, stop: Int64, withScores: Bool = false) throws -> [Data] {
        try withHandle { db in
            var result = key.withCString { redlite_zrange(db, $0, start, stop, withScores ? 1 : 0) }
            defer {
                var wrapper = FFIBytesArray(result)
                wrapper.free()
            }
            return FFIBytesArray(result).toNonNilDataArray()
        }
    }

    /// ZRANGE as Strings
    public func zrangeStrings(_ key: String, start: Int64, stop: Int64) throws -> [String] {
        try zrange(key, start: start, stop: stop).compactMap { String(data: $0, encoding: .utf8) }
    }

    /// ZRANGE with scores as ZMember array
    public func zrangeWithScores(_ key: String, start: Int64, stop: Int64) throws -> [ZMember] {
        let results = try zrange(key, start: start, stop: stop, withScores: true)
        var members: [ZMember] = []

        // Results alternate: member, score (as string), member, score...
        for i in stride(from: 0, to: results.count - 1, by: 2) {
            let memberData = results[i]
            if let scoreStr = String(data: results[i + 1], encoding: .utf8),
               let score = Double(scoreStr) {
                members.append(ZMember(score: score, member: memberData))
            }
        }

        return members
    }

    /// ZREVRANGE key start stop [WITHSCORES] - Get range of members by rank (high to low)
    ///
    /// - Parameters:
    ///   - key: The sorted set key
    ///   - start: Start index (0-based from highest score)
    ///   - stop: Stop index (inclusive)
    ///   - withScores: If true, returns members with scores
    /// - Returns: Array of members (or ZMember if withScores)
    public func zrevrange(_ key: String, start: Int64, stop: Int64, withScores: Bool = false) throws -> [Data] {
        try withHandle { db in
            var result = key.withCString { redlite_zrevrange(db, $0, start, stop, withScores ? 1 : 0) }
            defer {
                var wrapper = FFIBytesArray(result)
                wrapper.free()
            }
            return FFIBytesArray(result).toNonNilDataArray()
        }
    }

    /// ZREVRANGE as Strings
    public func zrevrangeStrings(_ key: String, start: Int64, stop: Int64) throws -> [String] {
        try zrevrange(key, start: start, stop: stop).compactMap { String(data: $0, encoding: .utf8) }
    }

    /// ZREVRANGE with scores as ZMember array
    public func zrevrangeWithScores(_ key: String, start: Int64, stop: Int64) throws -> [ZMember] {
        let results = try zrevrange(key, start: start, stop: stop, withScores: true)
        var members: [ZMember] = []

        // Results alternate: member, score (as string), member, score...
        for i in stride(from: 0, to: results.count - 1, by: 2) {
            let memberData = results[i]
            if let scoreStr = String(data: results[i + 1], encoding: .utf8),
               let score = Double(scoreStr) {
                members.append(ZMember(score: score, member: memberData))
            }
        }

        return members
    }
}
