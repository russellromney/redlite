import CRedlite
import Foundation

/// RAII wrapper for RedliteBytes with automatic memory management
internal struct FFIBytes {
    private var raw: RedliteBytes
    private var freed: Bool = false

    init(_ bytes: RedliteBytes) {
        self.raw = bytes
    }

    var isEmpty: Bool {
        raw.data == nil || raw.len == 0
    }

    /// Convert to Swift Data (copies memory)
    func toData() -> Data? {
        guard let ptr = raw.data, raw.len > 0 else { return nil }
        return Data(bytes: ptr, count: raw.len)
    }

    /// Free the underlying C memory
    mutating func free() {
        guard !freed else { return }
        if raw.data != nil {
            redlite_free_bytes(raw)
        }
        raw = RedliteBytes(data: nil, len: 0)
        freed = true
    }
}

/// RAII wrapper for RedliteStringArray
internal struct FFIStringArray {
    private var raw: RedliteStringArray
    private var freed: Bool = false

    init(_ arr: RedliteStringArray) {
        self.raw = arr
    }

    var isEmpty: Bool {
        raw.strings == nil || raw.len == 0
    }

    /// Convert to Swift [String]
    func toStrings() -> [String] {
        guard let strings = raw.strings, raw.len > 0 else { return [] }
        var result: [String] = []
        result.reserveCapacity(Int(raw.len))
        for i in 0..<Int(raw.len) {
            if let cstr = strings[i] {
                result.append(String(cString: cstr))
            }
        }
        return result
    }

    /// Free the underlying C memory
    mutating func free() {
        guard !freed else { return }
        if raw.strings != nil {
            redlite_free_string_array(raw)
        }
        raw = RedliteStringArray(strings: nil, len: 0)
        freed = true
    }
}

/// RAII wrapper for RedliteBytesArray
internal struct FFIBytesArray {
    private var raw: RedliteBytesArray
    private var freed: Bool = false

    init(_ arr: RedliteBytesArray) {
        self.raw = arr
    }

    var isEmpty: Bool {
        raw.items == nil || raw.len == 0
    }

    var count: Int {
        Int(raw.len)
    }

    /// Convert to Swift [Data?] (null entries become nil)
    func toDataArray() -> [Data?] {
        guard let items = raw.items, raw.len > 0 else { return [] }
        var result: [Data?] = []
        result.reserveCapacity(Int(raw.len))
        for i in 0..<Int(raw.len) {
            let item = items[i]
            if let ptr = item.data, item.len > 0 {
                result.append(Data(bytes: ptr, count: item.len))
            } else {
                result.append(nil)
            }
        }
        return result
    }

    /// Convert to Swift [Data] (skipping nil entries)
    func toNonNilDataArray() -> [Data] {
        toDataArray().compactMap { $0 }
    }

    /// Free the underlying C memory
    mutating func free() {
        guard !freed else { return }
        if raw.items != nil {
            redlite_free_bytes_array(raw)
        }
        raw = RedliteBytesArray(items: nil, len: 0)
        freed = true
    }
}

/// Sorted set member with score
public struct ZMember: Sendable, Equatable {
    public let score: Double
    public let member: Data

    public init(score: Double, member: Data) {
        self.score = score
        self.member = member
    }

    public init(score: Double, member: String) {
        self.score = score
        self.member = Data(member.utf8)
    }

    /// Get member as String (UTF-8)
    public var memberString: String? {
        String(data: member, encoding: .utf8)
    }
}
