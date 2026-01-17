const std = @import("std");
const redlite = @import("redlite");

pub fn main() !void {
    const stdout = std.io.getStdOut().writer();

    // Open an in-memory database
    const db = try redlite.Database.openMemory();
    defer db.close();

    try stdout.print("Redlite Zig SDK Example\n", .{});
    try stdout.print("========================\n\n", .{});

    // String operations
    try stdout.print("String Operations:\n", .{});
    try db.set("greeting", "Hello, World!", null);
    if (try db.get("greeting")) |value| {
        defer value.deinit();
        try stdout.print("  GET greeting = {s}\n", .{value.data()});
    }

    _ = db.append("greeting", " (from Zig)");
    if (try db.get("greeting")) |value| {
        defer value.deinit();
        try stdout.print("  After APPEND = {s}\n", .{value.data()});
    }

    // Counter operations
    try stdout.print("\nCounter Operations:\n", .{});
    try stdout.print("  INCR counter = {d}\n", .{db.incr("counter")});
    try stdout.print("  INCR counter = {d}\n", .{db.incr("counter")});
    try stdout.print("  INCRBY counter 10 = {d}\n", .{db.incrby("counter", 10)});
    try stdout.print("  DECR counter = {d}\n", .{db.decr("counter")});

    // Hash operations
    try stdout.print("\nHash Operations:\n", .{});
    _ = db.hset("user:1", "name", "Alice");
    _ = db.hset("user:1", "email", "alice@example.com");
    _ = db.hset("user:1", "age", "30");

    if (db.hget("user:1", "name")) |name| {
        defer name.deinit();
        try stdout.print("  HGET user:1 name = {s}\n", .{name.data()});
    }
    try stdout.print("  HLEN user:1 = {d}\n", .{db.hlen("user:1")});

    // List operations
    try stdout.print("\nList Operations:\n", .{});
    var values = [_][]const u8{ "task1", "task2", "task3" };
    _ = try db.rpush("tasks", &values);
    try stdout.print("  LLEN tasks = {d}\n", .{db.llen("tasks")});

    const range = db.lrange("tasks", 0, -1);
    defer range.deinit();
    try stdout.print("  LRANGE tasks 0 -1 = [ ", .{});
    for (0..range.len()) |i| {
        if (range.get(i)) |item| {
            try stdout.print("{s} ", .{item});
        }
    }
    try stdout.print("]\n", .{});

    // Set operations
    try stdout.print("\nSet Operations:\n", .{});
    var members = [_][]const u8{ "redis", "sqlite", "zig" };
    _ = try db.sadd("tags", &members);
    try stdout.print("  SCARD tags = {d}\n", .{db.scard("tags")});
    try stdout.print("  SISMEMBER tags zig = {}\n", .{db.sismember("tags", "zig")});
    try stdout.print("  SISMEMBER tags rust = {}\n", .{db.sismember("tags", "rust")});

    // Sorted set operations
    try stdout.print("\nSorted Set Operations:\n", .{});
    var scores = [_]redlite.ZMember{
        .{ .score = 100.0, .member = "alice" },
        .{ .score = 85.0, .member = "bob" },
        .{ .score = 92.0, .member = "charlie" },
    };
    _ = try db.zadd("leaderboard", &scores);

    try stdout.print("  ZCARD leaderboard = {d}\n", .{db.zcard("leaderboard")});

    const top = db.zrevrange("leaderboard", 0, 2);
    defer top.deinit();
    try stdout.print("  ZREVRANGE leaderboard 0 2 = [ ", .{});
    for (0..top.len()) |i| {
        if (top.get(i)) |member| {
            try stdout.print("{s} ", .{member});
        }
    }
    try stdout.print("]\n", .{});

    if (db.zscore("leaderboard", "alice")) |score| {
        try stdout.print("  ZSCORE leaderboard alice = {d}\n", .{score});
    }

    // Database info
    try stdout.print("\nDatabase Info:\n", .{});
    try stdout.print("  DBSIZE = {d}\n", .{db.dbsize()});

    try stdout.print("\nDone!\n", .{});
}
