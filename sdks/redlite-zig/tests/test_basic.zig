const std = @import("std");
const redlite = @import("redlite");

const Database = redlite.Database;
const OwnedBytes = redlite.OwnedBytes;
const ZMember = redlite.ZMember;

// ============== String Commands ==============

test "SET and GET roundtrip" {
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

test "GET nonexistent key returns null" {
    const db = try Database.openMemory();
    defer db.close();

    const result = try db.get("nonexistent");
    try std.testing.expect(result == null);
}

test "SET with TTL" {
    const db = try Database.openMemory();
    defer db.close();

    try db.set("expiring", "value", 60);
    const ttl_result = db.ttl("expiring");
    try std.testing.expect(ttl_result > 0);
    try std.testing.expect(ttl_result <= 60);
}

test "SETEX and TTL" {
    const db = try Database.openMemory();
    defer db.close();

    try db.setex("key", 60, "value");
    const ttl_result = db.ttl("key");
    try std.testing.expect(ttl_result > 0);
    try std.testing.expect(ttl_result <= 60);
}

test "GETDEL gets and deletes" {
    const db = try Database.openMemory();
    defer db.close();

    try db.set("key", "value", null);

    const result = try db.getdel("key");
    if (result) |v| {
        defer v.deinit();
        try std.testing.expectEqualSlices(u8, "value", v.data());
    } else {
        return error.TestUnexpectedResult;
    }

    // Key should be gone now
    const second = try db.get("key");
    try std.testing.expect(second == null);
}

test "APPEND" {
    const db = try Database.openMemory();
    defer db.close();

    try db.set("key", "hello", null);
    const new_len = db.append("key", " world");
    try std.testing.expectEqual(@as(i64, 11), new_len);

    const result = try db.get("key");
    if (result) |v| {
        defer v.deinit();
        try std.testing.expectEqualSlices(u8, "hello world", v.data());
    }
}

test "STRLEN" {
    const db = try Database.openMemory();
    defer db.close();

    try db.set("key", "hello", null);
    try std.testing.expectEqual(@as(i64, 5), db.strlen("key"));
    try std.testing.expectEqual(@as(i64, 0), db.strlen("nonexistent"));
}

test "GETRANGE" {
    const db = try Database.openMemory();
    defer db.close();

    try db.set("key", "hello world", null);
    const result = db.getrange("key", 0, 4);
    defer result.deinit();
    try std.testing.expectEqualSlices(u8, "hello", result.data());
}

test "INCR and DECR" {
    const db = try Database.openMemory();
    defer db.close();

    try std.testing.expectEqual(@as(i64, 1), db.incr("counter"));
    try std.testing.expectEqual(@as(i64, 2), db.incr("counter"));
    try std.testing.expectEqual(@as(i64, 1), db.decr("counter"));
    try std.testing.expectEqual(@as(i64, 0), db.decr("counter"));
    try std.testing.expectEqual(@as(i64, -1), db.decr("counter"));
}

test "INCRBY and DECRBY" {
    const db = try Database.openMemory();
    defer db.close();

    try std.testing.expectEqual(@as(i64, 10), db.incrby("counter", 10));
    try std.testing.expectEqual(@as(i64, 15), db.incrby("counter", 5));
    try std.testing.expectEqual(@as(i64, 12), db.decrby("counter", 3));
}

// ============== Key Commands ==============

test "DEL and EXISTS" {
    const db = try Database.openMemory();
    defer db.close();

    try db.set("key1", "val1", null);
    try db.set("key2", "val2", null);

    var keys = [_][:0]const u8{ "key1", "key2" };
    try std.testing.expectEqual(@as(i64, 2), db.exists(&keys));

    var del_keys = [_][:0]const u8{"key1"};
    try std.testing.expectEqual(@as(i64, 1), db.del(&del_keys));

    try std.testing.expectEqual(@as(i64, 1), db.exists(&keys));
}

test "TTL returns -2 for nonexistent key" {
    const db = try Database.openMemory();
    defer db.close();

    try std.testing.expectEqual(@as(i64, -2), db.ttl("nonexistent"));
}

test "TTL returns -1 for key without expiration" {
    const db = try Database.openMemory();
    defer db.close();

    try db.set("key", "value", null);
    try std.testing.expectEqual(@as(i64, -1), db.ttl("key"));
}

test "EXPIRE and PERSIST" {
    const db = try Database.openMemory();
    defer db.close();

    try db.set("key", "value", null);
    try std.testing.expect(db.expire("key", 60));

    const ttl = db.ttl("key");
    try std.testing.expect(ttl > 0);

    try std.testing.expect(db.persist("key"));
    try std.testing.expectEqual(@as(i64, -1), db.ttl("key"));
}

test "DBSIZE" {
    const db = try Database.openMemory();
    defer db.close();

    try std.testing.expectEqual(@as(i64, 0), db.dbsize());

    try db.set("key1", "val1", null);
    try db.set("key2", "val2", null);

    try std.testing.expectEqual(@as(i64, 2), db.dbsize());
}

test "FLUSHDB" {
    const db = try Database.openMemory();
    defer db.close();

    try db.set("key1", "val1", null);
    try db.set("key2", "val2", null);
    try std.testing.expectEqual(@as(i64, 2), db.dbsize());

    try db.flushdb();
    try std.testing.expectEqual(@as(i64, 0), db.dbsize());
}

// ============== Hash Commands ==============

test "HSET and HGET" {
    const db = try Database.openMemory();
    defer db.close();

    _ = db.hset("hash", "field", "value");

    const result = db.hget("hash", "field");
    if (result) |v| {
        defer v.deinit();
        try std.testing.expectEqualSlices(u8, "value", v.data());
    } else {
        return error.TestUnexpectedResult;
    }
}

test "HGET nonexistent field returns null" {
    const db = try Database.openMemory();
    defer db.close();

    const result = db.hget("hash", "nonexistent");
    try std.testing.expect(result == null);
}

test "HEXISTS" {
    const db = try Database.openMemory();
    defer db.close();

    _ = db.hset("hash", "field", "value");
    try std.testing.expect(db.hexists("hash", "field"));
    try std.testing.expect(!db.hexists("hash", "nonexistent"));
}

test "HLEN" {
    const db = try Database.openMemory();
    defer db.close();

    _ = db.hset("hash", "f1", "v1");
    _ = db.hset("hash", "f2", "v2");
    _ = db.hset("hash", "f3", "v3");

    try std.testing.expectEqual(@as(i64, 3), db.hlen("hash"));
}

test "HDEL" {
    const db = try Database.openMemory();
    defer db.close();

    _ = db.hset("hash", "f1", "v1");
    _ = db.hset("hash", "f2", "v2");

    var fields = [_][:0]const u8{"f1"};
    try std.testing.expectEqual(@as(i64, 1), db.hdel("hash", &fields));
    try std.testing.expectEqual(@as(i64, 1), db.hlen("hash"));
}

test "HINCRBY" {
    const db = try Database.openMemory();
    defer db.close();

    try std.testing.expectEqual(@as(i64, 5), db.hincrby("hash", "counter", 5));
    try std.testing.expectEqual(@as(i64, 8), db.hincrby("hash", "counter", 3));
    try std.testing.expectEqual(@as(i64, 6), db.hincrby("hash", "counter", -2));
}

// ============== List Commands ==============

test "LPUSH and RPUSH" {
    const db = try Database.openMemory();
    defer db.close();

    var values1 = [_][]const u8{"c"};
    _ = try db.lpush("list", &values1);

    var values2 = [_][]const u8{"b"};
    _ = try db.lpush("list", &values2);

    var values3 = [_][]const u8{"a"};
    _ = try db.lpush("list", &values3);

    // List should be: a, b, c
    try std.testing.expectEqual(@as(i64, 3), db.llen("list"));

    const result = db.lrange("list", 0, -1);
    defer result.deinit();
    try std.testing.expectEqual(@as(usize, 3), result.len());
    try std.testing.expectEqualSlices(u8, "a", result.get(0).?);
    try std.testing.expectEqualSlices(u8, "b", result.get(1).?);
    try std.testing.expectEqualSlices(u8, "c", result.get(2).?);
}

test "LPOP and RPOP" {
    const db = try Database.openMemory();
    defer db.close();

    var values = [_][]const u8{ "a", "b", "c" };
    _ = try db.rpush("list", &values);

    const left = db.lpop("list", 1);
    defer left.deinit();
    try std.testing.expectEqualSlices(u8, "a", left.get(0).?);

    const right = db.rpop("list", 1);
    defer right.deinit();
    try std.testing.expectEqualSlices(u8, "c", right.get(0).?);

    try std.testing.expectEqual(@as(i64, 1), db.llen("list"));
}

test "LINDEX" {
    const db = try Database.openMemory();
    defer db.close();

    var values = [_][]const u8{ "a", "b", "c" };
    _ = try db.rpush("list", &values);

    const result = db.lindex("list", 1);
    if (result) |v| {
        defer v.deinit();
        try std.testing.expectEqualSlices(u8, "b", v.data());
    } else {
        return error.TestUnexpectedResult;
    }

    // Negative index
    const last = db.lindex("list", -1);
    if (last) |v| {
        defer v.deinit();
        try std.testing.expectEqualSlices(u8, "c", v.data());
    }
}

// ============== Set Commands ==============

test "SADD and SMEMBERS" {
    const db = try Database.openMemory();
    defer db.close();

    var members = [_][]const u8{ "a", "b", "c" };
    _ = try db.sadd("set", &members);

    try std.testing.expectEqual(@as(i64, 3), db.scard("set"));

    // Adding duplicate should return 0
    var dup = [_][]const u8{"a"};
    const added = try db.sadd("set", &dup);
    try std.testing.expectEqual(@as(i64, 0), added);
}

test "SISMEMBER" {
    const db = try Database.openMemory();
    defer db.close();

    var members = [_][]const u8{ "a", "b" };
    _ = try db.sadd("set", &members);

    try std.testing.expect(db.sismember("set", "a"));
    try std.testing.expect(db.sismember("set", "b"));
    try std.testing.expect(!db.sismember("set", "c"));
}

test "SREM" {
    const db = try Database.openMemory();
    defer db.close();

    var members = [_][]const u8{ "a", "b", "c" };
    _ = try db.sadd("set", &members);

    var to_remove = [_][]const u8{"b"};
    try std.testing.expectEqual(@as(i64, 1), try db.srem("set", &to_remove));
    try std.testing.expectEqual(@as(i64, 2), db.scard("set"));
}

// ============== Sorted Set Commands ==============

test "ZADD and ZSCORE" {
    const db = try Database.openMemory();
    defer db.close();

    var members = [_]ZMember{
        .{ .score = 1.0, .member = "a" },
        .{ .score = 2.0, .member = "b" },
        .{ .score = 3.0, .member = "c" },
    };
    _ = try db.zadd("zset", &members);

    try std.testing.expectEqual(@as(i64, 3), db.zcard("zset"));
    try std.testing.expectEqual(@as(f64, 2.0), db.zscore("zset", "b").?);
}

test "ZSCORE returns null for nonexistent member" {
    const db = try Database.openMemory();
    defer db.close();

    try std.testing.expect(db.zscore("zset", "nonexistent") == null);
}

test "ZINCRBY" {
    const db = try Database.openMemory();
    defer db.close();

    var members = [_]ZMember{
        .{ .score = 1.0, .member = "a" },
    };
    _ = try db.zadd("zset", &members);

    const new_score = db.zincrby("zset", 2.5, "a");
    try std.testing.expect(@abs(new_score - 3.5) < 0.001);
}

test "ZRANGE" {
    const db = try Database.openMemory();
    defer db.close();

    var members = [_]ZMember{
        .{ .score = 3.0, .member = "c" },
        .{ .score = 1.0, .member = "a" },
        .{ .score = 2.0, .member = "b" },
    };
    _ = try db.zadd("zset", &members);

    const result = db.zrange("zset", 0, -1);
    defer result.deinit();

    try std.testing.expectEqual(@as(usize, 3), result.len());
    try std.testing.expectEqualSlices(u8, "a", result.get(0).?);
    try std.testing.expectEqualSlices(u8, "b", result.get(1).?);
    try std.testing.expectEqualSlices(u8, "c", result.get(2).?);
}

test "ZREVRANGE" {
    const db = try Database.openMemory();
    defer db.close();

    var members = [_]ZMember{
        .{ .score = 1.0, .member = "a" },
        .{ .score = 2.0, .member = "b" },
        .{ .score = 3.0, .member = "c" },
    };
    _ = try db.zadd("zset", &members);

    const result = db.zrevrange("zset", 0, -1);
    defer result.deinit();

    try std.testing.expectEqual(@as(usize, 3), result.len());
    try std.testing.expectEqualSlices(u8, "c", result.get(0).?);
    try std.testing.expectEqualSlices(u8, "b", result.get(1).?);
    try std.testing.expectEqualSlices(u8, "a", result.get(2).?);
}

test "ZCOUNT" {
    const db = try Database.openMemory();
    defer db.close();

    var members = [_]ZMember{
        .{ .score = 1.0, .member = "a" },
        .{ .score = 2.0, .member = "b" },
        .{ .score = 3.0, .member = "c" },
        .{ .score = 4.0, .member = "d" },
    };
    _ = try db.zadd("zset", &members);

    try std.testing.expectEqual(@as(i64, 2), db.zcount("zset", 2.0, 3.0));
    try std.testing.expectEqual(@as(i64, 4), db.zcount("zset", 0.0, 5.0));
}

test "ZREM" {
    const db = try Database.openMemory();
    defer db.close();

    var members = [_]ZMember{
        .{ .score = 1.0, .member = "a" },
        .{ .score = 2.0, .member = "b" },
        .{ .score = 3.0, .member = "c" },
    };
    _ = try db.zadd("zset", &members);

    var to_remove = [_][]const u8{ "a", "c" };
    try std.testing.expectEqual(@as(i64, 2), try db.zrem("zset", &to_remove));
    try std.testing.expectEqual(@as(i64, 1), db.zcard("zset"));
}
