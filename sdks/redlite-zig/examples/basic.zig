const std = @import("std");
const redlite = @import("redlite");

fn print(comptime fmt: []const u8, args: anytype) void {
    std.debug.print(fmt, args);
}

pub fn main() !void {
    // Open an in-memory database
    const db = try redlite.Database.openMemory();
    defer db.close();

    print("Redlite Zig SDK Example\n", .{});
    print("========================\n\n", .{});

    // String operations
    print("String Operations:\n", .{});
    try db.set("greeting", "Hello, World!", null);
    if (try db.get("greeting")) |value| {
        defer value.deinit();
        print("  GET greeting = {s}\n", .{value.data()});
    }

    _ = db.append("greeting", " (from Zig)");
    if (try db.get("greeting")) |value| {
        defer value.deinit();
        print("  After APPEND = {s}\n", .{value.data()});
    }

    // Counter operations
    print("\nCounter Operations:\n", .{});
    print("  INCR counter = {d}\n", .{db.incr("counter")});
    print("  INCR counter = {d}\n", .{db.incr("counter")});
    print("  INCRBY counter 10 = {d}\n", .{db.incrby("counter", 10)});
    print("  DECR counter = {d}\n", .{db.decr("counter")});

    // Hash operations
    print("\nHash Operations:\n", .{});
    _ = db.hset("user:1", "name", "Alice");
    _ = db.hset("user:1", "email", "alice@example.com");
    _ = db.hset("user:1", "age", "30");

    if (db.hget("user:1", "name")) |name| {
        defer name.deinit();
        print("  HGET user:1 name = {s}\n", .{name.data()});
    }
    print("  HLEN user:1 = {d}\n", .{db.hlen("user:1")});

    // List operations
    print("\nList Operations:\n", .{});
    var values = [_][]const u8{ "task1", "task2", "task3" };
    _ = try db.rpush("tasks", &values);
    print("  LLEN tasks = {d}\n", .{db.llen("tasks")});

    const range = db.lrange("tasks", 0, -1);
    defer range.deinit();
    print("  LRANGE tasks 0 -1 = [ ", .{});
    for (0..range.len()) |i| {
        if (range.get(i)) |item| {
            print("{s} ", .{item});
        }
    }
    print("]\n", .{});

    // Set operations
    print("\nSet Operations:\n", .{});
    var members = [_][]const u8{ "redis", "sqlite", "zig" };
    _ = try db.sadd("tags", &members);
    print("  SCARD tags = {d}\n", .{db.scard("tags")});
    print("  SISMEMBER tags zig = {}\n", .{db.sismember("tags", "zig")});
    print("  SISMEMBER tags rust = {}\n", .{db.sismember("tags", "rust")});

    // Sorted set operations
    print("\nSorted Set Operations:\n", .{});
    var scores = [_]redlite.ZMember{
        .{ .score = 100.0, .member = "alice" },
        .{ .score = 85.0, .member = "bob" },
        .{ .score = 92.0, .member = "charlie" },
    };
    _ = try db.zadd("leaderboard", &scores);

    print("  ZCARD leaderboard = {d}\n", .{db.zcard("leaderboard")});

    const top = db.zrevrange("leaderboard", 0, 2);
    defer top.deinit();
    print("  ZREVRANGE leaderboard 0 2 = [ ", .{});
    for (0..top.len()) |i| {
        if (top.get(i)) |member| {
            print("{s} ", .{member});
        }
    }
    print("]\n", .{});

    if (db.zscore("leaderboard", "alice")) |score| {
        print("  ZSCORE leaderboard alice = {d}\n", .{score});
    }

    // Database info
    print("\nDatabase Info:\n", .{});
    print("  DBSIZE = {d}\n", .{db.dbsize()});

    print("\nDone!\n", .{});
}
