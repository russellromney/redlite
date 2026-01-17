/// Redlite Zig SDK
///
/// A Zig-idiomatic wrapper around the Redlite C FFI.
/// Provides slices, optionals, and error unions for a native Zig experience.
///
/// Example:
/// ```zig
/// const db = try Database.openMemory();
/// defer db.close();
///
/// try db.set("key", "value", null);
/// const value = try db.get("key");
/// if (value) |v| {
///     defer v.deinit();
///     std.debug.print("value: {s}\n", .{v.data()});
/// }
/// ```

const std = @import("std");
const c = @import("c.zig");

/// Error types for Redlite operations
pub const Error = error{
    /// Failed to open database
    OpenFailed,
    /// Operation failed - check lastError() for details
    OperationFailed,
    /// Key does not exist
    KeyNotFound,
    /// Type mismatch error
    WrongType,
    /// Out of memory
    OutOfMemory,
};

/// Owned bytes that must be freed after use.
/// Wraps C-allocated memory with RAII-style cleanup.
pub const OwnedBytes = struct {
    bytes: c.RedliteBytes,

    const Self = @This();

    /// Get the data as a slice
    pub fn data(self: Self) []const u8 {
        if (self.bytes.data == null or self.bytes.len == 0) {
            return &[_]u8{};
        }
        return self.bytes.data[0..self.bytes.len];
    }

    /// Check if empty
    pub fn isEmpty(self: Self) bool {
        return self.bytes.data == null or self.bytes.len == 0;
    }

    /// Free the underlying memory
    pub fn deinit(self: Self) void {
        if (self.bytes.data != null) {
            c.free_bytes(self.bytes);
        }
    }

    /// Convert to owned string (caller must free with allocator)
    pub fn toOwnedSlice(self: Self, allocator: std.mem.Allocator) ![]u8 {
        if (self.bytes.data == null) return allocator.alloc(u8, 0);
        const slice = try allocator.alloc(u8, self.bytes.len);
        @memcpy(slice, self.data());
        return slice;
    }
};

/// Owned string array that must be freed after use.
pub const OwnedStringArray = struct {
    arr: c.RedliteStringArray,

    const Self = @This();

    /// Get the number of strings
    pub fn len(self: Self) usize {
        return self.arr.len;
    }

    /// Get a string at the given index
    pub fn get(self: Self, index: usize) ?[]const u8 {
        if (index >= self.arr.len) return null;
        const ptr = self.arr.strings[index];
        if (ptr == null) return null;
        return std.mem.span(ptr);
    }

    /// Iterate over all strings
    pub fn iterator(self: Self) Iterator {
        return Iterator{ .arr = self, .index = 0 };
    }

    pub const Iterator = struct {
        arr: OwnedStringArray,
        index: usize,

        pub fn next(self: *Iterator) ?[]const u8 {
            if (self.index >= self.arr.len()) return null;
            const result = self.arr.get(self.index);
            self.index += 1;
            return result;
        }
    };

    /// Free the underlying memory
    pub fn deinit(self: Self) void {
        c.free_string_array(self.arr);
    }
};

/// Owned bytes array that must be freed after use.
pub const OwnedBytesArray = struct {
    arr: c.RedliteBytesArray,

    const Self = @This();

    /// Get the number of items
    pub fn len(self: Self) usize {
        return self.arr.len;
    }

    /// Get bytes at the given index
    pub fn get(self: Self, index: usize) ?[]const u8 {
        if (index >= self.arr.len) return null;
        const item = self.arr.items[index];
        if (item.data == null) return null;
        return item.data[0..item.len];
    }

    /// Check if item at index is null
    pub fn isNull(self: Self, index: usize) bool {
        if (index >= self.arr.len) return true;
        return self.arr.items[index].data == null;
    }

    /// Iterate over all items
    pub fn iterator(self: Self) Iterator {
        return Iterator{ .arr = self, .index = 0 };
    }

    pub const Iterator = struct {
        arr: OwnedBytesArray,
        index: usize,

        pub fn next(self: *Iterator) ??[]const u8 {
            if (self.index >= self.arr.len()) return null;
            const result = self.arr.get(self.index);
            self.index += 1;
            return result;
        }
    };

    /// Free the underlying memory
    pub fn deinit(self: Self) void {
        c.free_bytes_array(self.arr);
    }
};

/// Sorted set member with score
pub const ZMember = struct {
    score: f64,
    member: []const u8,
};

/// Key-value pair for hash/mset operations
pub const KV = struct {
    key: []const u8,
    value: []const u8,
};

/// Main database handle.
/// Thread-safe for concurrent use.
pub const Database = struct {
    handle: *c.RedliteDb,

    const Self = @This();

    // ============== Lifecycle ==============

    /// Open a database at the given file path.
    pub fn open(path: [:0]const u8) Error!Self {
        const handle = c.open(path.ptr);
        if (handle == null) {
            return Error.OpenFailed;
        }
        return Self{ .handle = handle.? };
    }

    /// Open an in-memory database.
    pub fn openMemory() Error!Self {
        const handle = c.open_memory();
        if (handle == null) {
            return Error.OpenFailed;
        }
        return Self{ .handle = handle.? };
    }

    /// Open a database with custom cache size (in MB).
    pub fn openWithCache(path: [:0]const u8, cache_mb: i64) Error!Self {
        const handle = c.open_with_cache(path.ptr, cache_mb);
        if (handle == null) {
            return Error.OpenFailed;
        }
        return Self{ .handle = handle.? };
    }

    /// Close the database and release resources.
    pub fn close(self: Self) void {
        c.close(self.handle);
    }

    /// Get the last error message, if any.
    pub fn lastError() ?[]const u8 {
        const err = c.last_error();
        if (err == null) return null;
        defer c.free_string(err);
        return std.mem.span(err);
    }

    // ============== String Commands ==============

    /// GET key
    /// Returns the value, or null if key doesn't exist.
    pub fn get(self: Self, key: []const u8) Error!?OwnedBytes {
        const result = c.get(self.handle, key.ptr);
        if (result.data == null) {
            return null;
        }
        return OwnedBytes{ .bytes = result };
    }

    /// SET key value [ttl_seconds]
    /// ttl_seconds: 0 or null means no expiration
    pub fn set(self: Self, key: []const u8, value: []const u8, ttl_seconds: ?i64) Error!void {
        const ttl_val = ttl_seconds orelse 0;
        const result = c.set(self.handle, key.ptr, value.ptr, value.len, ttl_val);
        if (result != 0) {
            return Error.OperationFailed;
        }
    }

    /// SETEX key seconds value
    pub fn setex(self: Self, key: []const u8, seconds: i64, value: []const u8) Error!void {
        const result = c.setex(self.handle, key.ptr, seconds, value.ptr, value.len);
        if (result != 0) {
            return Error.OperationFailed;
        }
    }

    /// PSETEX key milliseconds value
    pub fn psetex(self: Self, key: []const u8, milliseconds: i64, value: []const u8) Error!void {
        const result = c.psetex(self.handle, key.ptr, milliseconds, value.ptr, value.len);
        if (result != 0) {
            return Error.OperationFailed;
        }
    }

    /// GETDEL key - Get and delete atomically
    pub fn getdel(self: Self, key: []const u8) Error!?OwnedBytes {
        const result = c.getdel(self.handle, key.ptr);
        if (result.data == null) {
            return null;
        }
        return OwnedBytes{ .bytes = result };
    }

    /// APPEND key value
    /// Returns the new length of the string.
    pub fn append(self: Self, key: []const u8, value: []const u8) i64 {
        return c.append(self.handle, key.ptr, value.ptr, value.len);
    }

    /// STRLEN key
    pub fn strlen(self: Self, key: []const u8) i64 {
        return c.strlen(self.handle, key.ptr);
    }

    /// GETRANGE key start end
    pub fn getrange(self: Self, key: []const u8, start: i64, end: i64) OwnedBytes {
        const result = c.getrange(self.handle, key.ptr, start, end);
        return OwnedBytes{ .bytes = result };
    }

    /// SETRANGE key offset value
    /// Returns the new length of the string.
    pub fn setrange(self: Self, key: []const u8, offset: i64, value: []const u8) i64 {
        return c.setrange(self.handle, key.ptr, offset, value.ptr, value.len);
    }

    /// INCR key
    pub fn incr(self: Self, key: []const u8) i64 {
        return c.incr(self.handle, key.ptr);
    }

    /// DECR key
    pub fn decr(self: Self, key: []const u8) i64 {
        return c.decr(self.handle, key.ptr);
    }

    /// INCRBY key increment
    pub fn incrby(self: Self, key: []const u8, increment: i64) i64 {
        return c.incrby(self.handle, key.ptr, increment);
    }

    /// DECRBY key decrement
    pub fn decrby(self: Self, key: []const u8, decrement: i64) i64 {
        return c.decrby(self.handle, key.ptr, decrement);
    }

    /// INCRBYFLOAT key increment
    /// Returns the new value as a string, or error.
    pub fn incrbyfloat(self: Self, key: []const u8, increment: f64, allocator: std.mem.Allocator) Error![]u8 {
        const result = c.incrbyfloat(self.handle, key.ptr, increment);
        if (result == null) {
            return Error.OperationFailed;
        }
        defer c.free_string(result);
        const span = std.mem.span(result);
        const owned = try allocator.alloc(u8, span.len);
        @memcpy(owned, span);
        return owned;
    }

    /// MGET key [key ...]
    pub fn mget(self: Self, key_list: []const [:0]const u8) OwnedBytesArray {
        const ptrs = @as([*]const [*c]const u8, @ptrCast(key_list.ptr));
        const result = c.mget(self.handle, ptrs, key_list.len);
        return OwnedBytesArray{ .arr = result };
    }

    /// MSET key value [key value ...]
    pub fn mset(self: Self, pairs: []const KV) Error!void {
        var c_pairs: []c.RedliteKV = undefined;
        var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
        defer arena.deinit();
        const alloc = arena.allocator();

        c_pairs = alloc.alloc(c.RedliteKV, pairs.len) catch return Error.OutOfMemory;
        for (pairs, 0..) |pair, i| {
            c_pairs[i] = c.RedliteKV{
                .key = pair.key.ptr,
                .value = pair.value.ptr,
                .value_len = pair.value.len,
            };
        }

        const result = c.mset(self.handle, c_pairs.ptr, c_pairs.len);
        if (result != 0) {
            return Error.OperationFailed;
        }
    }

    // ============== Key Commands ==============

    /// DEL key [key ...]
    /// Returns the number of keys deleted.
    pub fn del(self: Self, key_list: []const [:0]const u8) i64 {
        const ptrs = @as([*]const [*c]const u8, @ptrCast(key_list.ptr));
        return c.del(self.handle, ptrs, key_list.len);
    }

    /// EXISTS key [key ...]
    /// Returns the number of keys that exist.
    pub fn exists(self: Self, key_list: []const [:0]const u8) i64 {
        const ptrs = @as([*]const [*c]const u8, @ptrCast(key_list.ptr));
        return c.exists(self.handle, ptrs, key_list.len);
    }

    /// TYPE key
    /// Returns the type of the key, or null if key doesn't exist.
    pub fn keyType(self: Self, key: []const u8, allocator: std.mem.Allocator) Error!?[]u8 {
        const result = c.@"type"(self.handle, key.ptr);
        if (result == null) {
            return null;
        }
        defer c.free_string(result);
        const span = std.mem.span(result);
        const owned = try allocator.alloc(u8, span.len);
        @memcpy(owned, span);
        return owned;
    }

    /// TTL key
    /// Returns -2 if key doesn't exist, -1 if no TTL, else seconds.
    pub fn ttl(self: Self, key: []const u8) i64 {
        return c.ttl(self.handle, key.ptr);
    }

    /// PTTL key
    /// Returns -2 if key doesn't exist, -1 if no TTL, else milliseconds.
    pub fn pttl(self: Self, key: []const u8) i64 {
        return c.pttl(self.handle, key.ptr);
    }

    /// EXPIRE key seconds
    /// Returns true if TTL was set.
    pub fn expire(self: Self, key: []const u8, seconds: i64) bool {
        return c.expire(self.handle, key.ptr, seconds) == 1;
    }

    /// PEXPIRE key milliseconds
    pub fn pexpire(self: Self, key: []const u8, milliseconds: i64) bool {
        return c.pexpire(self.handle, key.ptr, milliseconds) == 1;
    }

    /// EXPIREAT key unix_timestamp
    pub fn expireat(self: Self, key: []const u8, unix_seconds: i64) bool {
        return c.expireat(self.handle, key.ptr, unix_seconds) == 1;
    }

    /// PEXPIREAT key unix_timestamp_ms
    pub fn pexpireat(self: Self, key: []const u8, unix_ms: i64) bool {
        return c.pexpireat(self.handle, key.ptr, unix_ms) == 1;
    }

    /// PERSIST key - Remove TTL
    pub fn persist(self: Self, key: []const u8) bool {
        return c.persist(self.handle, key.ptr) == 1;
    }

    /// RENAME key newkey
    pub fn rename(self: Self, key: []const u8, newkey: []const u8) Error!void {
        const result = c.rename(self.handle, key.ptr, newkey.ptr);
        if (result != 0) {
            return Error.OperationFailed;
        }
    }

    /// RENAMENX key newkey
    /// Returns true if rename succeeded (newkey didn't exist).
    pub fn renamenx(self: Self, key: []const u8, newkey: []const u8) bool {
        return c.renamenx(self.handle, key.ptr, newkey.ptr) == 1;
    }

    /// KEYS pattern
    pub fn keys(self: Self, pattern: [:0]const u8) OwnedStringArray {
        const result = c.keys(self.handle, pattern.ptr);
        return OwnedStringArray{ .arr = result };
    }

    /// DBSIZE
    pub fn dbsize(self: Self) i64 {
        return c.dbsize(self.handle);
    }

    /// FLUSHDB
    pub fn flushdb(self: Self) Error!void {
        const result = c.flushdb(self.handle);
        if (result != 0) {
            return Error.OperationFailed;
        }
    }

    /// SELECT db
    pub fn selectDb(self: Self, db_num: i32) Error!void {
        const result = c.select(self.handle, db_num);
        if (result != 0) {
            return Error.OperationFailed;
        }
    }

    // ============== Hash Commands ==============

    /// HSET key field value
    /// Returns the number of fields added.
    pub fn hset(self: Self, key: []const u8, field: [:0]const u8, value: []const u8) i64 {
        var fields = [_][*c]const u8{field.ptr};
        var values = [_]c.RedliteBytes{.{ .data = @constCast(value.ptr), .len = value.len }};
        return c.hset(self.handle, key.ptr, &fields, &values, 1);
    }

    /// HSET key field value [field value ...]
    pub fn hsetMultiple(self: Self, key: []const u8, field_values: []const KV) Error!i64 {
        var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
        defer arena.deinit();
        const alloc = arena.allocator();

        const fields = alloc.alloc([*c]const u8, field_values.len) catch return Error.OutOfMemory;
        const values = alloc.alloc(c.RedliteBytes, field_values.len) catch return Error.OutOfMemory;

        for (field_values, 0..) |fv, i| {
            fields[i] = fv.key.ptr;
            values[i] = .{ .data = @constCast(fv.value.ptr), .len = fv.value.len };
        }

        return c.hset(self.handle, key.ptr, fields.ptr, values.ptr, field_values.len);
    }

    /// HGET key field
    pub fn hget(self: Self, key: []const u8, field: [:0]const u8) ?OwnedBytes {
        const result = c.hget(self.handle, key.ptr, field.ptr);
        if (result.data == null) {
            return null;
        }
        return OwnedBytes{ .bytes = result };
    }

    /// HDEL key field [field ...]
    pub fn hdel(self: Self, key: []const u8, fields: []const [:0]const u8) i64 {
        const ptrs = @as([*]const [*c]const u8, @ptrCast(fields.ptr));
        return c.hdel(self.handle, key.ptr, ptrs, fields.len);
    }

    /// HEXISTS key field
    pub fn hexists(self: Self, key: []const u8, field: [:0]const u8) bool {
        return c.hexists(self.handle, key.ptr, field.ptr) == 1;
    }

    /// HLEN key
    pub fn hlen(self: Self, key: []const u8) i64 {
        return c.hlen(self.handle, key.ptr);
    }

    /// HKEYS key
    pub fn hkeys(self: Self, key: []const u8) OwnedStringArray {
        const result = c.hkeys(self.handle, key.ptr);
        return OwnedStringArray{ .arr = result };
    }

    /// HVALS key
    pub fn hvals(self: Self, key: []const u8) OwnedBytesArray {
        const result = c.hvals(self.handle, key.ptr);
        return OwnedBytesArray{ .arr = result };
    }

    /// HINCRBY key field increment
    pub fn hincrby(self: Self, key: []const u8, field: [:0]const u8, increment: i64) i64 {
        return c.hincrby(self.handle, key.ptr, field.ptr, increment);
    }

    /// HGETALL key
    /// Returns alternating field-value pairs.
    pub fn hgetall(self: Self, key: []const u8) OwnedBytesArray {
        const result = c.hgetall(self.handle, key.ptr);
        return OwnedBytesArray{ .arr = result };
    }

    /// HMGET key field [field ...]
    pub fn hmget(self: Self, key: []const u8, fields: []const [:0]const u8) OwnedBytesArray {
        const ptrs = @as([*]const [*c]const u8, @ptrCast(fields.ptr));
        const result = c.hmget(self.handle, key.ptr, ptrs, fields.len);
        return OwnedBytesArray{ .arr = result };
    }

    // ============== List Commands ==============

    /// LPUSH key value [value ...]
    /// Returns the new list length.
    pub fn lpush(self: Self, key: []const u8, values: []const []const u8) Error!i64 {
        var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
        defer arena.deinit();
        const alloc = arena.allocator();

        const c_values = alloc.alloc(c.RedliteBytes, values.len) catch return Error.OutOfMemory;
        for (values, 0..) |v, i| {
            c_values[i] = .{ .data = @constCast(v.ptr), .len = v.len };
        }

        return c.lpush(self.handle, key.ptr, c_values.ptr, c_values.len);
    }

    /// RPUSH key value [value ...]
    pub fn rpush(self: Self, key: []const u8, values: []const []const u8) Error!i64 {
        var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
        defer arena.deinit();
        const alloc = arena.allocator();

        const c_values = alloc.alloc(c.RedliteBytes, values.len) catch return Error.OutOfMemory;
        for (values, 0..) |v, i| {
            c_values[i] = .{ .data = @constCast(v.ptr), .len = v.len };
        }

        return c.rpush(self.handle, key.ptr, c_values.ptr, c_values.len);
    }

    /// LPOP key [count]
    pub fn lpop(self: Self, key: []const u8, count: usize) OwnedBytesArray {
        const result = c.lpop(self.handle, key.ptr, count);
        return OwnedBytesArray{ .arr = result };
    }

    /// RPOP key [count]
    pub fn rpop(self: Self, key: []const u8, count: usize) OwnedBytesArray {
        const result = c.rpop(self.handle, key.ptr, count);
        return OwnedBytesArray{ .arr = result };
    }

    /// LLEN key
    pub fn llen(self: Self, key: []const u8) i64 {
        return c.llen(self.handle, key.ptr);
    }

    /// LRANGE key start stop
    pub fn lrange(self: Self, key: []const u8, start: i64, stop: i64) OwnedBytesArray {
        const result = c.lrange(self.handle, key.ptr, start, stop);
        return OwnedBytesArray{ .arr = result };
    }

    /// LINDEX key index
    pub fn lindex(self: Self, key: []const u8, index: i64) ?OwnedBytes {
        const result = c.lindex(self.handle, key.ptr, index);
        if (result.data == null) {
            return null;
        }
        return OwnedBytes{ .bytes = result };
    }

    // ============== Set Commands ==============

    /// SADD key member [member ...]
    /// Returns the number of elements added.
    pub fn sadd(self: Self, key: []const u8, members: []const []const u8) Error!i64 {
        var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
        defer arena.deinit();
        const alloc = arena.allocator();

        const c_members = alloc.alloc(c.RedliteBytes, members.len) catch return Error.OutOfMemory;
        for (members, 0..) |m, i| {
            c_members[i] = .{ .data = @constCast(m.ptr), .len = m.len };
        }

        return c.sadd(self.handle, key.ptr, c_members.ptr, c_members.len);
    }

    /// SREM key member [member ...]
    pub fn srem(self: Self, key: []const u8, members: []const []const u8) Error!i64 {
        var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
        defer arena.deinit();
        const alloc = arena.allocator();

        const c_members = alloc.alloc(c.RedliteBytes, members.len) catch return Error.OutOfMemory;
        for (members, 0..) |m, i| {
            c_members[i] = .{ .data = @constCast(m.ptr), .len = m.len };
        }

        return c.srem(self.handle, key.ptr, c_members.ptr, c_members.len);
    }

    /// SMEMBERS key
    pub fn smembers(self: Self, key: []const u8) OwnedBytesArray {
        const result = c.smembers(self.handle, key.ptr);
        return OwnedBytesArray{ .arr = result };
    }

    /// SISMEMBER key member
    pub fn sismember(self: Self, key: []const u8, member: []const u8) bool {
        return c.sismember(self.handle, key.ptr, member.ptr, member.len) == 1;
    }

    /// SCARD key
    pub fn scard(self: Self, key: []const u8) i64 {
        return c.scard(self.handle, key.ptr);
    }

    // ============== Sorted Set Commands ==============

    /// ZADD key score member [score member ...]
    /// Returns the number of elements added.
    pub fn zadd(self: Self, key: []const u8, members: []const ZMember) Error!i64 {
        var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
        defer arena.deinit();
        const alloc = arena.allocator();

        const c_members = alloc.alloc(c.RedliteZMember, members.len) catch return Error.OutOfMemory;
        for (members, 0..) |m, i| {
            c_members[i] = .{
                .score = m.score,
                .member = m.member.ptr,
                .member_len = m.member.len,
            };
        }

        return c.zadd(self.handle, key.ptr, c_members.ptr, c_members.len);
    }

    /// ZREM key member [member ...]
    pub fn zrem(self: Self, key: []const u8, members: []const []const u8) Error!i64 {
        var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
        defer arena.deinit();
        const alloc = arena.allocator();

        const c_members = alloc.alloc(c.RedliteBytes, members.len) catch return Error.OutOfMemory;
        for (members, 0..) |m, i| {
            c_members[i] = .{ .data = @constCast(m.ptr), .len = m.len };
        }

        return c.zrem(self.handle, key.ptr, c_members.ptr, c_members.len);
    }

    /// ZSCORE key member
    /// Returns null if member doesn't exist.
    pub fn zscore(self: Self, key: []const u8, member: []const u8) ?f64 {
        const result = c.zscore(self.handle, key.ptr, member.ptr, member.len);
        if (std.math.isNan(result)) {
            return null;
        }
        return result;
    }

    /// ZCARD key
    pub fn zcard(self: Self, key: []const u8) i64 {
        return c.zcard(self.handle, key.ptr);
    }

    /// ZCOUNT key min max
    pub fn zcount(self: Self, key: []const u8, min: f64, max: f64) i64 {
        return c.zcount(self.handle, key.ptr, min, max);
    }

    /// ZINCRBY key increment member
    pub fn zincrby(self: Self, key: []const u8, increment: f64, member: []const u8) f64 {
        return c.zincrby(self.handle, key.ptr, increment, member.ptr, member.len);
    }

    /// ZRANGE key start stop
    pub fn zrange(self: Self, key: []const u8, start: i64, stop: i64) OwnedBytesArray {
        const result = c.zrange(self.handle, key.ptr, start, stop, 0);
        return OwnedBytesArray{ .arr = result };
    }

    /// ZRANGE key start stop WITHSCORES
    /// Returns alternating member-score pairs (score as bytes).
    pub fn zrangeWithScores(self: Self, key: []const u8, start: i64, stop: i64) OwnedBytesArray {
        const result = c.zrange(self.handle, key.ptr, start, stop, 1);
        return OwnedBytesArray{ .arr = result };
    }

    /// ZREVRANGE key start stop
    pub fn zrevrange(self: Self, key: []const u8, start: i64, stop: i64) OwnedBytesArray {
        const result = c.zrevrange(self.handle, key.ptr, start, stop, 0);
        return OwnedBytesArray{ .arr = result };
    }

    /// ZREVRANGE key start stop WITHSCORES
    pub fn zrevrangeWithScores(self: Self, key: []const u8, start: i64, stop: i64) OwnedBytesArray {
        const result = c.zrevrange(self.handle, key.ptr, start, stop, 1);
        return OwnedBytesArray{ .arr = result };
    }

    // ============== Server Commands ==============

    /// VACUUM - Compact the database
    pub fn vacuum(self: Self) i64 {
        return c.vacuum(self.handle);
    }

    /// Get library version
    pub fn version(allocator: std.mem.Allocator) ![]u8 {
        const result = c.version();
        if (result == null) {
            return allocator.alloc(u8, 0);
        }
        defer c.free_string(result);
        const span = std.mem.span(result);
        const owned = try allocator.alloc(u8, span.len);
        @memcpy(owned, span);
        return owned;
    }
};

// Tests
test "open memory database" {
    const db = try Database.openMemory();
    defer db.close();
}

test "set and get" {
    const db = try Database.openMemory();
    defer db.close();

    try db.set("key", "value", null);
    const result = try db.get("key");
    if (result) |v| {
        defer v.deinit();
        try std.testing.expectEqualSlices(u8, "value", v.data());
    } else {
        return error.TestUnexpectedResult;
    }
}

test "get nonexistent key returns null" {
    const db = try Database.openMemory();
    defer db.close();

    const result = try db.get("nonexistent");
    try std.testing.expect(result == null);
}

test "incr and decr" {
    const db = try Database.openMemory();
    defer db.close();

    try std.testing.expectEqual(@as(i64, 1), db.incr("counter"));
    try std.testing.expectEqual(@as(i64, 2), db.incr("counter"));
    try std.testing.expectEqual(@as(i64, 1), db.decr("counter"));
}
