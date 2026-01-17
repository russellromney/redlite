import CRedlite
import Foundation

/// Get last error from FFI and convert to RedliteError
internal func getLastError() -> RedliteError {
    if let errPtr = redlite_last_error() {
        let message = String(cString: errPtr)
        redlite_free_string(errPtr)
        return .operationFailed(message)
    }
    return .unknown
}

/// Execute a closure with an array of C strings
/// The pointers are only valid within the closure
internal func withCStringArray<T>(
    _ strings: [String],
    _ body: (UnsafePointer<UnsafePointer<CChar>?>, Int) throws -> T
) rethrows -> T {
    // Convert strings to ContiguousArrays of CChars
    var buffers: [ContiguousArray<CChar>] = strings.map { ContiguousArray($0.utf8CString) }

    // Get pointers to each buffer
    var pointers: [UnsafePointer<CChar>?] = []
    pointers.reserveCapacity(buffers.count)

    for i in 0..<buffers.count {
        let ptr = buffers[i].withUnsafeBufferPointer { $0.baseAddress }
        pointers.append(ptr)
    }

    return try pointers.withUnsafeBufferPointer { ptrBuffer in
        try body(ptrBuffer.baseAddress!, strings.count)
    }
}

/// Convert Data array to RedliteBytes array for FFI calls
internal func withBytesArray<T>(
    _ dataArray: [Data],
    _ body: (UnsafePointer<RedliteBytes>, Int) throws -> T
) rethrows -> T {
    // We need to keep the Data objects alive during the call
    var bytesArray: [RedliteBytes] = []
    bytesArray.reserveCapacity(dataArray.count)

    for data in dataArray {
        let bytes = data.withUnsafeBytes { buffer -> RedliteBytes in
            let ptr = buffer.baseAddress?.assumingMemoryBound(to: UInt8.self)
            return RedliteBytes(
                data: UnsafeMutablePointer(mutating: ptr),
                len: buffer.count
            )
        }
        bytesArray.append(bytes)
    }

    return try bytesArray.withUnsafeBufferPointer { buffer in
        try body(buffer.baseAddress!, dataArray.count)
    }
}

/// Convert ZMember array to RedliteZMember array for FFI calls
internal func withZMemberArray<T>(
    _ members: [ZMember],
    _ body: (UnsafePointer<RedliteZMember>, Int) throws -> T
) rethrows -> T {
    var zmemberArray: [RedliteZMember] = []
    zmemberArray.reserveCapacity(members.count)

    for member in members {
        let zmember = member.member.withUnsafeBytes { buffer -> RedliteZMember in
            let ptr = buffer.baseAddress?.assumingMemoryBound(to: UInt8.self)
            return RedliteZMember(
                score: member.score,
                member: ptr,
                member_len: buffer.count
            )
        }
        zmemberArray.append(zmember)
    }

    return try zmemberArray.withUnsafeBufferPointer { buffer in
        try body(buffer.baseAddress!, members.count)
    }
}

/// Helper to create RedliteKV array for MSET operations
internal func withKVArray<T>(
    _ pairs: [(String, Data)],
    _ body: (UnsafePointer<RedliteKV>, Int) throws -> T
) rethrows -> T {
    // Keep keys as ContiguousArrays
    var keyBuffers: [ContiguousArray<CChar>] = pairs.map { ContiguousArray($0.0.utf8CString) }

    var kvArray: [RedliteKV] = []
    kvArray.reserveCapacity(pairs.count)

    for i in 0..<pairs.count {
        let keyPtr = keyBuffers[i].withUnsafeBufferPointer { $0.baseAddress }
        let value = pairs[i].1

        let kv = value.withUnsafeBytes { buffer -> RedliteKV in
            let valuePtr = buffer.baseAddress?.assumingMemoryBound(to: UInt8.self)
            return RedliteKV(
                key: keyPtr,
                value: valuePtr,
                value_len: buffer.count
            )
        }
        kvArray.append(kv)
    }

    return try kvArray.withUnsafeBufferPointer { buffer in
        try body(buffer.baseAddress!, pairs.count)
    }
}
