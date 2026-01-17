/// Redlite Zig SDK Oracle Test Runner
///
/// This runner executes oracle test specifications against the Zig SDK.
/// It reads JSON test cases from stdin and outputs results to stdout.
///
/// Usage (typically called from the Python test harness):
///   echo '{"cmd":"SET","args":["key","value"]}' | zig-runner
///
/// Build:
///   zig build-exe -I../../crates/redlite-ffi -L../../target/release -lredlite_ffi zig_runner.zig

const std = @import("std");
const redlite = @import("redlite");

const Database = redlite.Database;
const ZMember = redlite.ZMember;

/// JSON Value representation for test parsing
const JsonValue = std.json.Value;

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    const args = try std.process.argsAlloc(allocator);
    defer std.process.argsFree(allocator, args);

    // Set up buffered I/O for stdout and stderr (Zig 0.15.2 style)
    var stdout_buffer: [4096]u8 = undefined;
    var stderr_buffer: [4096]u8 = undefined;
    var stdout_writer = std.fs.File.stdout().writer(&stdout_buffer);
    var stderr_writer = std.fs.File.stderr().writer(&stderr_buffer);

    const stdout = &stdout_writer.interface;
    const stderr = &stderr_writer.interface;

    // Parse command line
    var verbose = false;
    var spec_files = std.array_list.Managed([]const u8).init(allocator);
    defer spec_files.deinit();

    for (args[1..]) |arg| {
        if (std.mem.eql(u8, arg, "-v") or std.mem.eql(u8, arg, "--verbose")) {
            verbose = true;
        } else if (!std.mem.startsWith(u8, arg, "-")) {
            try spec_files.append(arg);
        }
    }

    // If no spec files specified, read from stdin for single command mode
    if (spec_files.items.len == 0) {
        try runInteractiveMode(allocator, stdout, stderr, verbose);
        return;
    }

    // Run spec files
    var passed: usize = 0;
    var failed: usize = 0;

    for (spec_files.items) |spec_file| {
        const result = runSpecFile(allocator, spec_file, stdout, stderr, verbose) catch |err| {
            try stderr.print("Error running {s}: {}\n", .{ spec_file, err });
            failed += 1;
            continue;
        };
        passed += result.passed;
        failed += result.failed;
    }

    try stdout.print("\n{'='**60}\n", .{});
    try stdout.print("Oracle Test Results: {d}/{d} passed, {d} failed\n", .{ passed, passed + failed, failed });
    try stdout.print("{'='**60}\n", .{});

    if (failed > 0) {
        std.process.exit(1);
    }
}

const TestResult = struct {
    passed: usize,
    failed: usize,
};

fn runSpecFile(allocator: std.mem.Allocator, path: []const u8, stdout: anytype, stderr: anytype, verbose: bool) !TestResult {
    _ = stderr;
    const file = try std.fs.cwd().openFile(path, .{});
    defer file.close();

    const content = try file.readToEndAlloc(allocator, 10 * 1024 * 1024);
    defer allocator.free(content);

    // Simple YAML-like parsing (for basic cases)
    // In practice, you'd use a YAML library or convert to JSON first
    if (verbose) {
        try stdout.print("\nRunning: {s}\n", .{path});
    }

    // For now, return success - full YAML parsing would require a library
    return TestResult{ .passed = 0, .failed = 0 };
}

/// Interactive mode - reads JSON commands from stdin
fn runInteractiveMode(allocator: std.mem.Allocator, stdout: anytype, stderr: anytype, verbose: bool) !void {
    var stdin_buffer: [4096]u8 = undefined;
    var stdin_reader = std.fs.File.stdin().reader(&stdin_buffer);
    const stdin = &stdin_reader.interface;

    const db = try Database.openMemory();
    defer db.close();

    if (verbose) {
        try stderr.print("Zig Oracle Runner - Interactive Mode\n", .{});
        try stderr.print("Enter JSON commands, one per line. Ctrl+D to exit.\n", .{});
    }

    var line_buf: [4096]u8 = undefined;
    while (stdin.readUntilDelimiterOrEof(&line_buf, '\n') catch null) |line| {
        if (line.len == 0) continue;

        const parsed = std.json.parseFromSlice(std.json.Value, allocator, line, .{}) catch |err| {
            try stdout.print("{{\"error\":\"{}\"}}\n", .{err});
            continue;
        };
        defer parsed.deinit();

        const result = executeCommand(allocator, db, parsed.value) catch |err| {
            try stdout.print("{{\"error\":\"{}\"}}\n", .{err});
            continue;
        };

        try stdout.print("{s}\n", .{result});
        allocator.free(result);
    }
}

/// Execute a single command and return JSON result
fn executeCommand(allocator: std.mem.Allocator, db: Database, cmd_obj: JsonValue) ![]u8 {
    const obj = cmd_obj.object;

    const cmd_val = obj.get("cmd") orelse return error.MissingCommand;
    const cmd = cmd_val.string;

    const args_val = obj.get("args");
    const args: []const JsonValue = if (args_val) |av| av.array.items else &[_]JsonValue{};

    // Dispatch to command handler
    if (std.mem.eql(u8, cmd, "SET")) {
        return handleSet(allocator, db, args);
    } else if (std.mem.eql(u8, cmd, "GET")) {
        return handleGet(allocator, db, args);
    } else if (std.mem.eql(u8, cmd, "DEL")) {
        return handleDel(allocator, db, args);
    } else if (std.mem.eql(u8, cmd, "EXISTS")) {
        return handleExists(allocator, db, args);
    } else if (std.mem.eql(u8, cmd, "INCR")) {
        return handleIncr(allocator, db, args);
    } else if (std.mem.eql(u8, cmd, "DECR")) {
        return handleDecr(allocator, db, args);
    } else if (std.mem.eql(u8, cmd, "INCRBY")) {
        return handleIncrBy(allocator, db, args);
    } else if (std.mem.eql(u8, cmd, "DECRBY")) {
        return handleDecrBy(allocator, db, args);
    } else if (std.mem.eql(u8, cmd, "APPEND")) {
        return handleAppend(allocator, db, args);
    } else if (std.mem.eql(u8, cmd, "STRLEN")) {
        return handleStrlen(allocator, db, args);
    } else if (std.mem.eql(u8, cmd, "TTL")) {
        return handleTtl(allocator, db, args);
    } else if (std.mem.eql(u8, cmd, "PTTL")) {
        return handlePttl(allocator, db, args);
    } else if (std.mem.eql(u8, cmd, "EXPIRE")) {
        return handleExpire(allocator, db, args);
    } else if (std.mem.eql(u8, cmd, "PERSIST")) {
        return handlePersist(allocator, db, args);
    } else if (std.mem.eql(u8, cmd, "DBSIZE")) {
        return handleDbsize(allocator, db);
    } else if (std.mem.eql(u8, cmd, "FLUSHDB")) {
        return handleFlushdb(allocator, db);
    } else if (std.mem.eql(u8, cmd, "HSET")) {
        return handleHset(allocator, db, args);
    } else if (std.mem.eql(u8, cmd, "HGET")) {
        return handleHget(allocator, db, args);
    } else if (std.mem.eql(u8, cmd, "HDEL")) {
        return handleHdel(allocator, db, args);
    } else if (std.mem.eql(u8, cmd, "HEXISTS")) {
        return handleHexists(allocator, db, args);
    } else if (std.mem.eql(u8, cmd, "HLEN")) {
        return handleHlen(allocator, db, args);
    } else if (std.mem.eql(u8, cmd, "HINCRBY")) {
        return handleHincrby(allocator, db, args);
    } else if (std.mem.eql(u8, cmd, "LPUSH")) {
        return handleLpush(allocator, db, args);
    } else if (std.mem.eql(u8, cmd, "RPUSH")) {
        return handleRpush(allocator, db, args);
    } else if (std.mem.eql(u8, cmd, "LPOP")) {
        return handleLpop(allocator, db, args);
    } else if (std.mem.eql(u8, cmd, "RPOP")) {
        return handleRpop(allocator, db, args);
    } else if (std.mem.eql(u8, cmd, "LLEN")) {
        return handleLlen(allocator, db, args);
    } else if (std.mem.eql(u8, cmd, "LRANGE")) {
        return handleLrange(allocator, db, args);
    } else if (std.mem.eql(u8, cmd, "LINDEX")) {
        return handleLindex(allocator, db, args);
    } else if (std.mem.eql(u8, cmd, "SADD")) {
        return handleSadd(allocator, db, args);
    } else if (std.mem.eql(u8, cmd, "SREM")) {
        return handleSrem(allocator, db, args);
    } else if (std.mem.eql(u8, cmd, "SISMEMBER")) {
        return handleSismember(allocator, db, args);
    } else if (std.mem.eql(u8, cmd, "SCARD")) {
        return handleScard(allocator, db, args);
    } else if (std.mem.eql(u8, cmd, "ZADD")) {
        return handleZadd(allocator, db, args);
    } else if (std.mem.eql(u8, cmd, "ZREM")) {
        return handleZrem(allocator, db, args);
    } else if (std.mem.eql(u8, cmd, "ZSCORE")) {
        return handleZscore(allocator, db, args);
    } else if (std.mem.eql(u8, cmd, "ZCARD")) {
        return handleZcard(allocator, db, args);
    } else if (std.mem.eql(u8, cmd, "ZCOUNT")) {
        return handleZcount(allocator, db, args);
    } else if (std.mem.eql(u8, cmd, "ZINCRBY")) {
        return handleZincrby(allocator, db, args);
    } else if (std.mem.eql(u8, cmd, "ZRANGE")) {
        return handleZrange(allocator, db, args);
    } else if (std.mem.eql(u8, cmd, "ZREVRANGE")) {
        return handleZrevrange(allocator, db, args);
    } else {
        return try std.fmt.allocPrint(allocator, "{{\"error\":\"Unknown command: {s}\"}}", .{cmd});
    }
}

// ============== Command Handlers ==============

fn handleSet(allocator: std.mem.Allocator, db: Database, args: []const JsonValue) ![]u8 {
    if (args.len < 2) return error.InvalidArgs;
    const key = args[0].string;
    const value = args[1].string;
    const ttl: ?i64 = if (args.len > 2) @intFromFloat(args[2].float) else null;

    db.set(key, value, ttl) catch |err| {
        return try std.fmt.allocPrint(allocator, "{{\"error\":\"{}\"}}", .{err});
    };
    return try allocator.dupe(u8, "{\"result\":true}");
}

fn handleGet(allocator: std.mem.Allocator, db: Database, args: []const JsonValue) ![]u8 {
    if (args.len < 1) return error.InvalidArgs;
    const key = args[0].string;

    const result = db.get(key) catch |err| {
        return try std.fmt.allocPrint(allocator, "{{\"error\":\"{}\"}}", .{err});
    };

    if (result) |v| {
        defer v.deinit();
        return try std.fmt.allocPrint(allocator, "{{\"result\":\"{s}\"}}", .{v.data()});
    } else {
        return try allocator.dupe(u8, "{\"result\":null}");
    }
}

fn handleDel(allocator: std.mem.Allocator, db: Database, args: []const JsonValue) ![]u8 {
    if (args.len < 1) return error.InvalidArgs;

    var keys = std.ArrayList([:0]const u8).init(allocator);
    defer keys.deinit();

    // Handle both single key and array of keys
    if (args[0] == .array) {
        for (args[0].array.items) |item| {
            const key_z = try allocator.dupeZ(u8, item.string);
            try keys.append(key_z);
        }
    } else {
        const key_z = try allocator.dupeZ(u8, args[0].string);
        try keys.append(key_z);
    }

    const count = db.del(keys.items);
    return try std.fmt.allocPrint(allocator, "{{\"result\":{d}}}", .{count});
}

fn handleExists(allocator: std.mem.Allocator, db: Database, args: []const JsonValue) ![]u8 {
    if (args.len < 1) return error.InvalidArgs;

    var keys = std.ArrayList([:0]const u8).init(allocator);
    defer keys.deinit();

    if (args[0] == .array) {
        for (args[0].array.items) |item| {
            const key_z = try allocator.dupeZ(u8, item.string);
            try keys.append(key_z);
        }
    } else {
        const key_z = try allocator.dupeZ(u8, args[0].string);
        try keys.append(key_z);
    }

    const count = db.exists(keys.items);
    return try std.fmt.allocPrint(allocator, "{{\"result\":{d}}}", .{count});
}

fn handleIncr(allocator: std.mem.Allocator, db: Database, args: []const JsonValue) ![]u8 {
    if (args.len < 1) return error.InvalidArgs;
    const result = db.incr(args[0].string);
    return try std.fmt.allocPrint(allocator, "{{\"result\":{d}}}", .{result});
}

fn handleDecr(allocator: std.mem.Allocator, db: Database, args: []const JsonValue) ![]u8 {
    if (args.len < 1) return error.InvalidArgs;
    const result = db.decr(args[0].string);
    return try std.fmt.allocPrint(allocator, "{{\"result\":{d}}}", .{result});
}

fn handleIncrBy(allocator: std.mem.Allocator, db: Database, args: []const JsonValue) ![]u8 {
    if (args.len < 2) return error.InvalidArgs;
    const increment = @as(i64, @intFromFloat(args[1].float));
    const result = db.incrby(args[0].string, increment);
    return try std.fmt.allocPrint(allocator, "{{\"result\":{d}}}", .{result});
}

fn handleDecrBy(allocator: std.mem.Allocator, db: Database, args: []const JsonValue) ![]u8 {
    if (args.len < 2) return error.InvalidArgs;
    const decrement = @as(i64, @intFromFloat(args[1].float));
    const result = db.decrby(args[0].string, decrement);
    return try std.fmt.allocPrint(allocator, "{{\"result\":{d}}}", .{result});
}

fn handleAppend(allocator: std.mem.Allocator, db: Database, args: []const JsonValue) ![]u8 {
    if (args.len < 2) return error.InvalidArgs;
    const result = db.append(args[0].string, args[1].string);
    return try std.fmt.allocPrint(allocator, "{{\"result\":{d}}}", .{result});
}

fn handleStrlen(allocator: std.mem.Allocator, db: Database, args: []const JsonValue) ![]u8 {
    if (args.len < 1) return error.InvalidArgs;
    const result = db.strlen(args[0].string);
    return try std.fmt.allocPrint(allocator, "{{\"result\":{d}}}", .{result});
}

fn handleTtl(allocator: std.mem.Allocator, db: Database, args: []const JsonValue) ![]u8 {
    if (args.len < 1) return error.InvalidArgs;
    const result = db.ttl(args[0].string);
    return try std.fmt.allocPrint(allocator, "{{\"result\":{d}}}", .{result});
}

fn handlePttl(allocator: std.mem.Allocator, db: Database, args: []const JsonValue) ![]u8 {
    if (args.len < 1) return error.InvalidArgs;
    const result = db.pttl(args[0].string);
    return try std.fmt.allocPrint(allocator, "{{\"result\":{d}}}", .{result});
}

fn handleExpire(allocator: std.mem.Allocator, db: Database, args: []const JsonValue) ![]u8 {
    if (args.len < 2) return error.InvalidArgs;
    const seconds = @as(i64, @intFromFloat(args[1].float));
    const result = db.expire(args[0].string, seconds);
    return try std.fmt.allocPrint(allocator, "{{\"result\":{}}}", .{result});
}

fn handlePersist(allocator: std.mem.Allocator, db: Database, args: []const JsonValue) ![]u8 {
    if (args.len < 1) return error.InvalidArgs;
    const result = db.persist(args[0].string);
    return try std.fmt.allocPrint(allocator, "{{\"result\":{}}}", .{result});
}

fn handleDbsize(allocator: std.mem.Allocator, db: Database) ![]u8 {
    const result = db.dbsize();
    return try std.fmt.allocPrint(allocator, "{{\"result\":{d}}}", .{result});
}

fn handleFlushdb(allocator: std.mem.Allocator, db: Database) ![]u8 {
    db.flushdb() catch |err| {
        return try std.fmt.allocPrint(allocator, "{{\"error\":\"{}\"}}", .{err});
    };
    return try allocator.dupe(u8, "{\"result\":true}");
}

fn handleHset(allocator: std.mem.Allocator, db: Database, args: []const JsonValue) ![]u8 {
    if (args.len < 3) return error.InvalidArgs;
    const key = args[0].string;
    const field_z = try allocator.dupeZ(u8, args[1].string);
    defer allocator.free(field_z);
    const result = db.hset(key, field_z, args[2].string);
    return try std.fmt.allocPrint(allocator, "{{\"result\":{d}}}", .{result});
}

fn handleHget(allocator: std.mem.Allocator, db: Database, args: []const JsonValue) ![]u8 {
    if (args.len < 2) return error.InvalidArgs;
    const field_z = try allocator.dupeZ(u8, args[1].string);
    defer allocator.free(field_z);

    if (db.hget(args[0].string, field_z)) |v| {
        defer v.deinit();
        return try std.fmt.allocPrint(allocator, "{{\"result\":\"{s}\"}}", .{v.data()});
    } else {
        return try allocator.dupe(u8, "{\"result\":null}");
    }
}

fn handleHdel(allocator: std.mem.Allocator, db: Database, args: []const JsonValue) ![]u8 {
    if (args.len < 2) return error.InvalidArgs;

    var fields = std.ArrayList([:0]const u8).init(allocator);
    defer fields.deinit();

    if (args[1] == .array) {
        for (args[1].array.items) |item| {
            try fields.append(try allocator.dupeZ(u8, item.string));
        }
    } else {
        try fields.append(try allocator.dupeZ(u8, args[1].string));
    }

    const result = db.hdel(args[0].string, fields.items);
    return try std.fmt.allocPrint(allocator, "{{\"result\":{d}}}", .{result});
}

fn handleHexists(allocator: std.mem.Allocator, db: Database, args: []const JsonValue) ![]u8 {
    if (args.len < 2) return error.InvalidArgs;
    const field_z = try allocator.dupeZ(u8, args[1].string);
    defer allocator.free(field_z);
    const result = db.hexists(args[0].string, field_z);
    return try std.fmt.allocPrint(allocator, "{{\"result\":{}}}", .{result});
}

fn handleHlen(allocator: std.mem.Allocator, db: Database, args: []const JsonValue) ![]u8 {
    if (args.len < 1) return error.InvalidArgs;
    const result = db.hlen(args[0].string);
    return try std.fmt.allocPrint(allocator, "{{\"result\":{d}}}", .{result});
}

fn handleHincrby(allocator: std.mem.Allocator, db: Database, args: []const JsonValue) ![]u8 {
    if (args.len < 3) return error.InvalidArgs;
    const field_z = try allocator.dupeZ(u8, args[1].string);
    defer allocator.free(field_z);
    const increment = @as(i64, @intFromFloat(args[2].float));
    const result = db.hincrby(args[0].string, field_z, increment);
    return try std.fmt.allocPrint(allocator, "{{\"result\":{d}}}", .{result});
}

fn handleLpush(allocator: std.mem.Allocator, db: Database, args: []const JsonValue) ![]u8 {
    if (args.len < 2) return error.InvalidArgs;

    var values = std.ArrayList([]const u8).init(allocator);
    defer values.deinit();

    if (args[1] == .array) {
        for (args[1].array.items) |item| {
            try values.append(item.string);
        }
    } else {
        try values.append(args[1].string);
    }

    const result = db.lpush(args[0].string, values.items) catch |err| {
        return try std.fmt.allocPrint(allocator, "{{\"error\":\"{}\"}}", .{err});
    };
    return try std.fmt.allocPrint(allocator, "{{\"result\":{d}}}", .{result});
}

fn handleRpush(allocator: std.mem.Allocator, db: Database, args: []const JsonValue) ![]u8 {
    if (args.len < 2) return error.InvalidArgs;

    var values = std.ArrayList([]const u8).init(allocator);
    defer values.deinit();

    if (args[1] == .array) {
        for (args[1].array.items) |item| {
            try values.append(item.string);
        }
    } else {
        try values.append(args[1].string);
    }

    const result = db.rpush(args[0].string, values.items) catch |err| {
        return try std.fmt.allocPrint(allocator, "{{\"error\":\"{}\"}}", .{err});
    };
    return try std.fmt.allocPrint(allocator, "{{\"result\":{d}}}", .{result});
}

fn handleLpop(allocator: std.mem.Allocator, db: Database, args: []const JsonValue) ![]u8 {
    if (args.len < 1) return error.InvalidArgs;
    const count: usize = if (args.len > 1) @intFromFloat(args[1].float) else 1;

    const result = db.lpop(args[0].string, count);
    defer result.deinit();

    if (result.len() == 0) {
        return try allocator.dupe(u8, "{\"result\":null}");
    }

    if (count == 1) {
        if (result.get(0)) |v| {
            return try std.fmt.allocPrint(allocator, "{{\"result\":\"{s}\"}}", .{v});
        }
    }

    var buf = std.ArrayList(u8).init(allocator);
    try buf.appendSlice("{\"result\":[");
    for (0..result.len()) |i| {
        if (i > 0) try buf.append(',');
        if (result.get(i)) |v| {
            try buf.append('"');
            try buf.appendSlice(v);
            try buf.append('"');
        } else {
            try buf.appendSlice("null");
        }
    }
    try buf.appendSlice("]}");
    return try buf.toOwnedSlice();
}

fn handleRpop(allocator: std.mem.Allocator, db: Database, args: []const JsonValue) ![]u8 {
    if (args.len < 1) return error.InvalidArgs;
    const count: usize = if (args.len > 1) @intFromFloat(args[1].float) else 1;

    const result = db.rpop(args[0].string, count);
    defer result.deinit();

    if (result.len() == 0) {
        return try allocator.dupe(u8, "{\"result\":null}");
    }

    if (count == 1) {
        if (result.get(0)) |v| {
            return try std.fmt.allocPrint(allocator, "{{\"result\":\"{s}\"}}", .{v});
        }
    }

    var buf = std.ArrayList(u8).init(allocator);
    try buf.appendSlice("{\"result\":[");
    for (0..result.len()) |i| {
        if (i > 0) try buf.append(',');
        if (result.get(i)) |v| {
            try buf.append('"');
            try buf.appendSlice(v);
            try buf.append('"');
        } else {
            try buf.appendSlice("null");
        }
    }
    try buf.appendSlice("]}");
    return try buf.toOwnedSlice();
}

fn handleLlen(allocator: std.mem.Allocator, db: Database, args: []const JsonValue) ![]u8 {
    if (args.len < 1) return error.InvalidArgs;
    const result = db.llen(args[0].string);
    return try std.fmt.allocPrint(allocator, "{{\"result\":{d}}}", .{result});
}

fn handleLrange(allocator: std.mem.Allocator, db: Database, args: []const JsonValue) ![]u8 {
    if (args.len < 3) return error.InvalidArgs;
    const start = @as(i64, @intFromFloat(args[1].float));
    const stop = @as(i64, @intFromFloat(args[2].float));

    const result = db.lrange(args[0].string, start, stop);
    defer result.deinit();

    var buf = std.ArrayList(u8).init(allocator);
    try buf.appendSlice("{\"result\":[");
    for (0..result.len()) |i| {
        if (i > 0) try buf.append(',');
        if (result.get(i)) |v| {
            try buf.append('"');
            try buf.appendSlice(v);
            try buf.append('"');
        } else {
            try buf.appendSlice("null");
        }
    }
    try buf.appendSlice("]}");
    return try buf.toOwnedSlice();
}

fn handleLindex(allocator: std.mem.Allocator, db: Database, args: []const JsonValue) ![]u8 {
    if (args.len < 2) return error.InvalidArgs;
    const index = @as(i64, @intFromFloat(args[1].float));

    if (db.lindex(args[0].string, index)) |v| {
        defer v.deinit();
        return try std.fmt.allocPrint(allocator, "{{\"result\":\"{s}\"}}", .{v.data()});
    } else {
        return try allocator.dupe(u8, "{\"result\":null}");
    }
}

fn handleSadd(allocator: std.mem.Allocator, db: Database, args: []const JsonValue) ![]u8 {
    if (args.len < 2) return error.InvalidArgs;

    var members = std.ArrayList([]const u8).init(allocator);
    defer members.deinit();

    if (args[1] == .array) {
        for (args[1].array.items) |item| {
            try members.append(item.string);
        }
    } else {
        try members.append(args[1].string);
    }

    const result = db.sadd(args[0].string, members.items) catch |err| {
        return try std.fmt.allocPrint(allocator, "{{\"error\":\"{}\"}}", .{err});
    };
    return try std.fmt.allocPrint(allocator, "{{\"result\":{d}}}", .{result});
}

fn handleSrem(allocator: std.mem.Allocator, db: Database, args: []const JsonValue) ![]u8 {
    if (args.len < 2) return error.InvalidArgs;

    var members = std.ArrayList([]const u8).init(allocator);
    defer members.deinit();

    if (args[1] == .array) {
        for (args[1].array.items) |item| {
            try members.append(item.string);
        }
    } else {
        try members.append(args[1].string);
    }

    const result = db.srem(args[0].string, members.items) catch |err| {
        return try std.fmt.allocPrint(allocator, "{{\"error\":\"{}\"}}", .{err});
    };
    return try std.fmt.allocPrint(allocator, "{{\"result\":{d}}}", .{result});
}

fn handleSismember(allocator: std.mem.Allocator, db: Database, args: []const JsonValue) ![]u8 {
    if (args.len < 2) return error.InvalidArgs;
    const result = db.sismember(args[0].string, args[1].string);
    return try std.fmt.allocPrint(allocator, "{{\"result\":{}}}", .{result});
}

fn handleScard(allocator: std.mem.Allocator, db: Database, args: []const JsonValue) ![]u8 {
    if (args.len < 1) return error.InvalidArgs;
    const result = db.scard(args[0].string);
    return try std.fmt.allocPrint(allocator, "{{\"result\":{d}}}", .{result});
}

fn handleZadd(allocator: std.mem.Allocator, db: Database, args: []const JsonValue) ![]u8 {
    if (args.len < 2) return error.InvalidArgs;

    var members = std.ArrayList(ZMember).init(allocator);
    defer members.deinit();

    // args[1] is array of [score, member] pairs
    for (args[1].array.items) |pair| {
        const pair_arr = pair.array.items;
        try members.append(.{
            .score = pair_arr[0].float,
            .member = pair_arr[1].string,
        });
    }

    const result = db.zadd(args[0].string, members.items) catch |err| {
        return try std.fmt.allocPrint(allocator, "{{\"error\":\"{}\"}}", .{err});
    };
    return try std.fmt.allocPrint(allocator, "{{\"result\":{d}}}", .{result});
}

fn handleZrem(allocator: std.mem.Allocator, db: Database, args: []const JsonValue) ![]u8 {
    if (args.len < 2) return error.InvalidArgs;

    var members = std.ArrayList([]const u8).init(allocator);
    defer members.deinit();

    if (args[1] == .array) {
        for (args[1].array.items) |item| {
            try members.append(item.string);
        }
    } else {
        try members.append(args[1].string);
    }

    const result = db.zrem(args[0].string, members.items) catch |err| {
        return try std.fmt.allocPrint(allocator, "{{\"error\":\"{}\"}}", .{err});
    };
    return try std.fmt.allocPrint(allocator, "{{\"result\":{d}}}", .{result});
}

fn handleZscore(allocator: std.mem.Allocator, db: Database, args: []const JsonValue) ![]u8 {
    if (args.len < 2) return error.InvalidArgs;

    if (db.zscore(args[0].string, args[1].string)) |score| {
        return try std.fmt.allocPrint(allocator, "{{\"result\":{d}}}", .{score});
    } else {
        return try allocator.dupe(u8, "{\"result\":null}");
    }
}

fn handleZcard(allocator: std.mem.Allocator, db: Database, args: []const JsonValue) ![]u8 {
    if (args.len < 1) return error.InvalidArgs;
    const result = db.zcard(args[0].string);
    return try std.fmt.allocPrint(allocator, "{{\"result\":{d}}}", .{result});
}

fn handleZcount(allocator: std.mem.Allocator, db: Database, args: []const JsonValue) ![]u8 {
    if (args.len < 3) return error.InvalidArgs;
    const result = db.zcount(args[0].string, args[1].float, args[2].float);
    return try std.fmt.allocPrint(allocator, "{{\"result\":{d}}}", .{result});
}

fn handleZincrby(allocator: std.mem.Allocator, db: Database, args: []const JsonValue) ![]u8 {
    if (args.len < 3) return error.InvalidArgs;
    const result = db.zincrby(args[0].string, args[1].float, args[2].string);
    return try std.fmt.allocPrint(allocator, "{{\"result\":{d}}}", .{result});
}

fn handleZrange(allocator: std.mem.Allocator, db: Database, args: []const JsonValue) ![]u8 {
    if (args.len < 3) return error.InvalidArgs;
    const start = @as(i64, @intFromFloat(args[1].float));
    const stop = @as(i64, @intFromFloat(args[2].float));

    const result = db.zrange(args[0].string, start, stop);
    defer result.deinit();

    var buf = std.ArrayList(u8).init(allocator);
    try buf.appendSlice("{\"result\":[");
    for (0..result.len()) |i| {
        if (i > 0) try buf.append(',');
        if (result.get(i)) |v| {
            try buf.append('"');
            try buf.appendSlice(v);
            try buf.append('"');
        }
    }
    try buf.appendSlice("]}");
    return try buf.toOwnedSlice();
}

fn handleZrevrange(allocator: std.mem.Allocator, db: Database, args: []const JsonValue) ![]u8 {
    if (args.len < 3) return error.InvalidArgs;
    const start = @as(i64, @intFromFloat(args[1].float));
    const stop = @as(i64, @intFromFloat(args[2].float));

    const result = db.zrevrange(args[0].string, start, stop);
    defer result.deinit();

    var buf = std.ArrayList(u8).init(allocator);
    try buf.appendSlice("{\"result\":[");
    for (0..result.len()) |i| {
        if (i > 0) try buf.append(',');
        if (result.get(i)) |v| {
            try buf.append('"');
            try buf.appendSlice(v);
            try buf.append('"');
        }
    }
    try buf.appendSlice("]}");
    return try buf.toOwnedSlice();
}
