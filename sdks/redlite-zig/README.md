# Redlite Zig SDK

A Zig-idiomatic wrapper around the Redlite C FFI, providing Redis-compatible embedded database functionality with SQLite durability.

## Features

- **Zig-native API**: Uses slices, optionals, and error unions
- **RAII resource management**: Automatic cleanup with `defer`
- **Full Redis command coverage**: Strings, hashes, lists, sets, sorted sets
- **Comptime features**: Type-safe API with compile-time checks

## Requirements

- Zig 0.11.0 or later
- Rust toolchain (for building the FFI library)

## Quick Start

### 1. Build the FFI library

```bash
cd sdks/redlite-zig
make ffi
```

### 2. Build and test

```bash
make build
make test
```

### 3. Run the example

```bash
make example
```

## Usage

```zig
const std = @import("std");
const redlite = @import("redlite");

pub fn main() !void {
    // Open an in-memory database
    const db = try redlite.Database.openMemory();
    defer db.close();

    // String operations
    try db.set("key", "value", null);

    if (try db.get("key")) |value| {
        defer value.deinit();
        std.debug.print("value: {s}\n", .{value.data()});
    }

    // Counter operations
    _ = db.incr("counter");
    _ = db.incrby("counter", 10);

    // Hash operations
    _ = db.hset("user:1", "name", "Alice");
    if (db.hget("user:1", "name")) |name| {
        defer name.deinit();
        std.debug.print("name: {s}\n", .{name.data()});
    }

    // List operations
    var values = [_][]const u8{"a", "b", "c"};
    _ = try db.rpush("list", &values);

    const range = db.lrange("list", 0, -1);
    defer range.deinit();

    // Set operations
    var members = [_][]const u8{"redis", "sqlite"};
    _ = try db.sadd("tags", &members);

    // Sorted set operations
    var scores = [_]redlite.ZMember{
        .{ .score = 100.0, .member = "alice" },
        .{ .score = 85.0, .member = "bob" },
    };
    _ = try db.zadd("leaderboard", &scores);
}
```

## API Reference

### Database Lifecycle

```zig
// Open file-backed database
const db = try Database.open("/path/to/db.sqlite");
defer db.close();

// Open in-memory database
const db = try Database.openMemory();
defer db.close();

// Open with custom cache size
const db = try Database.openWithCache("/path/to/db.sqlite", 64);
defer db.close();
```

### String Commands

| Method | Description |
|--------|-------------|
| `get(key)` | GET key - returns `?OwnedBytes` |
| `set(key, value, ttl)` | SET key value [TTL] |
| `setex(key, seconds, value)` | SETEX |
| `psetex(key, ms, value)` | PSETEX |
| `getdel(key)` | GETDEL - get and delete |
| `append(key, value)` | APPEND - returns new length |
| `strlen(key)` | STRLEN |
| `getrange(key, start, end)` | GETRANGE |
| `setrange(key, offset, value)` | SETRANGE |
| `incr(key)` | INCR |
| `decr(key)` | DECR |
| `incrby(key, n)` | INCRBY |
| `decrby(key, n)` | DECRBY |
| `incrbyfloat(key, n, alloc)` | INCRBYFLOAT |

### Key Commands

| Method | Description |
|--------|-------------|
| `del(keys)` | DEL - returns count deleted |
| `exists(keys)` | EXISTS - returns count existing |
| `keyType(key, alloc)` | TYPE |
| `ttl(key)` | TTL - returns seconds or -1/-2 |
| `pttl(key)` | PTTL - returns milliseconds |
| `expire(key, seconds)` | EXPIRE |
| `pexpire(key, ms)` | PEXPIRE |
| `persist(key)` | PERSIST - remove TTL |
| `rename(key, newkey)` | RENAME |
| `renamenx(key, newkey)` | RENAMENX |
| `keys(pattern)` | KEYS |
| `dbsize()` | DBSIZE |
| `flushdb()` | FLUSHDB |
| `selectDb(num)` | SELECT |

### Hash Commands

| Method | Description |
|--------|-------------|
| `hset(key, field, value)` | HSET single field |
| `hsetMultiple(key, pairs)` | HSET multiple fields |
| `hget(key, field)` | HGET |
| `hdel(key, fields)` | HDEL |
| `hexists(key, field)` | HEXISTS |
| `hlen(key)` | HLEN |
| `hkeys(key)` | HKEYS |
| `hvals(key)` | HVALS |
| `hincrby(key, field, n)` | HINCRBY |
| `hgetall(key)` | HGETALL |
| `hmget(key, fields)` | HMGET |

### List Commands

| Method | Description |
|--------|-------------|
| `lpush(key, values)` | LPUSH |
| `rpush(key, values)` | RPUSH |
| `lpop(key, count)` | LPOP |
| `rpop(key, count)` | RPOP |
| `llen(key)` | LLEN |
| `lrange(key, start, stop)` | LRANGE |
| `lindex(key, index)` | LINDEX |

### Set Commands

| Method | Description |
|--------|-------------|
| `sadd(key, members)` | SADD |
| `srem(key, members)` | SREM |
| `smembers(key)` | SMEMBERS |
| `sismember(key, member)` | SISMEMBER |
| `scard(key)` | SCARD |

### Sorted Set Commands

| Method | Description |
|--------|-------------|
| `zadd(key, members)` | ZADD |
| `zrem(key, members)` | ZREM |
| `zscore(key, member)` | ZSCORE - returns `?f64` |
| `zcard(key)` | ZCARD |
| `zcount(key, min, max)` | ZCOUNT |
| `zincrby(key, incr, member)` | ZINCRBY |
| `zrange(key, start, stop)` | ZRANGE |
| `zrangeWithScores(key, start, stop)` | ZRANGE WITHSCORES |
| `zrevrange(key, start, stop)` | ZREVRANGE |
| `zrevrangeWithScores(key, start, stop)` | ZREVRANGE WITHSCORES |

### Server Commands

| Method | Description |
|--------|-------------|
| `vacuum()` | VACUUM - compact database |
| `version(alloc)` | Get library version |

## Memory Management

The SDK uses owned types that must be cleaned up:

```zig
// OwnedBytes - for string results
if (try db.get("key")) |value| {
    defer value.deinit();  // IMPORTANT: free memory
    const data = value.data();  // []const u8 slice
}

// OwnedStringArray - for KEYS, HKEYS
const keys = db.keys("*");
defer keys.deinit();
for (0..keys.len()) |i| {
    if (keys.get(i)) |key| {
        // use key
    }
}

// OwnedBytesArray - for LRANGE, SMEMBERS, etc
const items = db.lrange("list", 0, -1);
defer items.deinit();
```

## Error Handling

```zig
const db = Database.openMemory() catch |err| switch (err) {
    error.OpenFailed => {
        if (Database.lastError()) |msg| {
            std.debug.print("Error: {s}\n", .{msg});
        }
        return err;
    },
    else => return err,
};
```

## License

Apache-2.0
