---
title: Commands Overview
description: Redis commands supported by Redlite
---

Redlite implements the Redis protocol and supports most Redis commands. For detailed command documentation, refer to the [official Redis documentation](https://redis.io/commands/).

## Supported Commands

See [COMMANDS.md](https://github.com/russellromney/redlite/blob/main/COMMANDS.md) for the complete list of supported commands.

### Data Types

- ✅ **Strings** - GET, SET, MGET, MSET, INCR, APPEND, etc.
- ✅ **Hashes** - HGET, HSET, HGETALL, HDEL, HINCRBY, etc.
- ✅ **Lists** - LPUSH, RPUSH, LPOP, RPOP, LRANGE, etc.
- ✅ **Sets** - SADD, SREM, SMEMBERS, SDIFF, SINTER, SUNION, etc.
- ✅ **Sorted Sets** - ZADD, ZREM, ZRANGE, ZRANGEBYSCORE, etc.
- ✅ **Streams** - XADD, XREAD, XRANGE, XGROUP, XREADGROUP, etc.

### Transactions & Scripting

- ✅ **Transactions** - MULTI, EXEC, DISCARD, WATCH, UNWATCH
- ❌ **Lua Scripting** - EVAL, EVALSHA (not supported)

### Connection & Authentication

- ✅ **Authentication** - AUTH, password support via `--password` flag
- ✅ **Client Commands** - CLIENT SETNAME, GETNAME, LIST, ID, INFO, KILL, PAUSE, UNPAUSE

## Server-Only Features

These features require server mode and are **not available** in embedded library mode:

### Blocking Operations

Commands that wait for data with timeouts:
- `BLPOP`, `BRPOP` - Blocking list operations
- `XREAD BLOCK` - Blocking stream reads
- `BRPOPLPUSH`, `BLMOVE` - Blocking list moves

**Why server-only?** Cross-client coordination requires a central server process.

### Pub/Sub

Fire-and-forget messaging via channels:
- `SUBSCRIBE`, `UNSUBSCRIBE` - Channel subscriptions
- `PSUBSCRIBE`, `PUNSUBSCRIBE` - Pattern subscriptions
- `PUBLISH` - Publish messages

**Why server-only?** Message routing requires a central broker to coordinate subscribers.

## Custom Commands

Redlite adds commands that Redis doesn't have:

### VACUUM

Delete expired keys and run SQLite VACUUM:

```bash
127.0.0.1:6379> VACUUM
OK
```

**Library mode:**
```rust
db.vacuum()?;
```

Useful for:
- Cleaning up expired keys (lazy expiration only deletes on read)
- Reclaiming disk space (SQLite VACUUM)
- Periodic maintenance

### KEYINFO

Get detailed metadata about a key:

```bash
127.0.0.1:6379> KEYINFO mykey
1) "type"
2) "string"
3) "ttl"
4) (integer) 3600000
5) "created_at"
6) (integer) 1704067200000
7) "updated_at"
8) (integer) 1704067200000
```

**Library mode:**
```rust
let info = db.keyinfo("mykey")?;
println!("Type: {:?}, TTL: {:?}", info.key_type, info.ttl);
```

**Fields:**
- `type` - Key type (string, hash, list, set, zset, stream)
- `ttl` - Time-to-live in milliseconds (nil if no expiration)
- `created_at` - Creation timestamp (milliseconds)
- `updated_at` - Last update timestamp (milliseconds)

### HISTORY

Track and query historical data with time-travel queries:

```bash
# Enable history tracking
127.0.0.1:6379> HISTORY ENABLE KEY mykey RETENTION COUNT 100
OK

# Query history
127.0.0.1:6379> HISTORY GET mykey LIMIT 10
[... history entries ...]

# Time-travel query
127.0.0.1:6379> HISTORY GETAT mykey 1704067200000
"historical_value"
```

See [History Tracking](/reference/history) for full documentation.

## Differences from Redis

### Expiration

Redlite uses **lazy expiration** by default:
- Expired keys checked on read
- Manual cleanup via `VACUUM` command
- No background expiration daemon (optional in future)

**Why?** Disk is cheap. Expired keys sitting on disk is fine until you need the space.

### Persistence

Redlite **persists data by default**:
- All data written to SQLite database file
- ACID transactions out of the box
- No separate `SAVE` or `BGSAVE` commands needed

**Why?** SQLite handles durability. No need for Redis-style persistence configuration.

### Transactions

Server mode provides `MULTI/EXEC` for command batching:

```bash
127.0.0.1:6379> MULTI
OK
127.0.0.1:6379> SET key1 value1
QUEUED
127.0.0.1:6379> SET key2 value2
QUEUED
127.0.0.1:6379> EXEC
1) OK
2) OK
```

Use `WATCH` for optimistic locking (check-and-set):

```bash
127.0.0.1:6379> WATCH mykey
OK
127.0.0.1:6379> GET mykey
"100"
127.0.0.1:6379> MULTI
OK
127.0.0.1:6379> SET mykey 101
QUEUED
127.0.0.1:6379> EXEC
1) OK
# Returns nil if mykey was modified by another client
```

## Redis Documentation

For detailed command syntax and behavior, see:
- [Redis Commands Reference](https://redis.io/commands/)
- [Redis Data Types](https://redis.io/docs/data-types/)
- [Redis Protocol Specification](https://redis.io/docs/reference/protocol-spec/)

Redlite implements the RESP protocol and aims for command-level compatibility with Redis.
