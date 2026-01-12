# redlite 🔴

SQLite-backed Redis-compatible embedded key-value store written in Rust.

## Why?

- **Embedded-first** - Use as a library, no separate server needed
- **Disk is cheap** - Persistent storage without Redis's memory constraints
- **SQLite foundation** - ACID transactions, durability, zero config
- **Redis compatible** - Existing clients just work (for most operations)
- **Easy backup** - just use using SQLite WAL-tailing tools like  [litestream](url) or [walsync](url)
- **Extra features**
  - Key metadata: `KEYINFO mykey` (type, ttl, created_at, updated_at)
  - Manual cleanup: `VACUUM` (delete expired + reclaim disk)
  - Auto cleanup: `AUTOVACUUM ON` (default, cleans every 60s)
  - **Planned**: Version history, time-travel queries, full-text search

## Install

```bash
cargo add redlite
```

## Usage

### Embedded (Library)

```rust
use redlite::Db;

let db = Db::open("mydata.db")?;  // Persistent
// let db = Db::open_memory()?;   // In-memory

// String operations
db.set("key", b"value", None)?;
let value = db.get("key")?;  // Some(b"value")

// With TTL
use std::time::Duration;
db.set("temp", b"expires", Some(Duration::from_secs(60)))?;

// SET NX/XX
use redlite::SetOptions;
db.set_opts("key", b"value", SetOptions::new().nx())?;  // Only if not exists
db.set_opts("key", b"value", SetOptions::new().xx())?;  // Only if exists

// Delete
db.del(&["key1", "key2"])?;

// Multiple databases (like Redis SELECT 0-15)
let mut db = Db::open("mydata.db")?;
db.select(1)?;  // Switch to database 1
db.set("key", b"in db 1", None)?;

// Multiple sessions sharing the same backend
let db1 = Db::open("mydata.db")?;
let mut db2 = db1.session();  // New session, starts at db 0
db2.select(1)?;               // Switch db2 to database 1

// db1 and db2 share data but have independent selected database
db1.set("key", b"db0 value", None)?;  // Goes to db 0
db2.set("key", b"db1 value", None)?;  // Goes to db 1

// Key metadata
if let Some(info) = db.keyinfo("key")? {
    println!("Type: {:?}, TTL: {}s, Created: {}",
             info.key_type, info.ttl, info.created_at);
}

// Manual cleanup (deletes expired + SQLite VACUUM)
let deleted = db.vacuum()?;

// Autovacuum config (default: ON @ 60s)
db.set_autovacuum(true);
db.set_autovacuum_interval(30_000);  // 30 seconds
```

### Server Mode

```bash
# Build
cargo build --release

# Run server (default port 6767)
./target/release/redlite --db=mydata.db

# In-memory mode
./target/release/redlite --db=:memory:

# Custom port
./target/release/redlite --db=mydata.db --addr=127.0.0.1:6379
```

Connect with any Redis client:

```bash
redis-cli -p 6767 SET foo bar
redis-cli -p 6767 GET foo
```

## Supported Commands

### Strings
- `GET`, `SET` (with EX, PX, NX, XX options)
- `INCR`, `DECR`, `INCRBY`, `DECRBY`, `INCRBYFLOAT`
- `MGET`, `MSET`
- `APPEND`, `STRLEN`
- `GETRANGE`, `SETRANGE`

### Keys
- `DEL`, `EXISTS`, `TYPE`
- `EXPIRE`, `TTL`, `PTTL`
- `KEYS` (with glob patterns)
- `SCAN` (with MATCH, COUNT)

### Hashes
- `HSET`, `HGET`, `HSETNX`
- `HMGET`, `HGETALL`
- `HDEL`, `HEXISTS`
- `HKEYS`, `HVALS`, `HLEN`
- `HINCRBY`, `HINCRBYFLOAT`

### Lists
- `LPUSH`, `RPUSH`
- `LPOP`, `RPOP` (with optional count)
- `LLEN`, `LRANGE`
- `LINDEX`, `LSET`
- `LTRIM`

### Sets
- `SADD`, `SREM`
- `SMEMBERS`, `SISMEMBER`
- `SCARD`
- `SPOP`, `SRANDMEMBER` (with optional count)
- `SDIFF`, `SINTER`, `SUNION`

### Sorted Sets
- `ZADD`, `ZREM`
- `ZSCORE`, `ZRANK`, `ZREVRANK`
- `ZCARD`
- `ZRANGE`, `ZREVRANGE` (with WITHSCORES)
- `ZRANGEBYSCORE` (with LIMIT)
- `ZCOUNT`
- `ZINCRBY`
- `ZREMRANGEBYRANK`, `ZREMRANGEBYSCORE`

### Server
- `PING`, `ECHO`, `QUIT`, `COMMAND`
- `SELECT` (databases 0-15, per-connection isolation)
- `DBSIZE` (key count in current database)
- `FLUSHDB` (delete all keys in current database)
- `INFO` (server stats, keyspace)

### Custom Commands (redlite extensions)
- `VACUUM` - Delete all expired keys + SQLite VACUUM to reclaim disk space
- `KEYINFO key` - Returns type, ttl, created_at, updated_at
- `AUTOVACUUM [ON|OFF|INTERVAL <ms>]` - Automatic expiration cleanup (default: ON @ 60s)

## Testing

```bash
# Unit tests (memory + disk)
cargo test --lib

# Integration tests (requires redis-cli)
cargo build && cargo test --test integration -- --test-threads=1

# All tests
cargo test -- --test-threads=1
```

## Schema

Data stored in SQLite with these tables:
- `keys` - Metadata (type, expiration, timestamps)
- `strings` - String values
- `hashes` - Hash field-value pairs
- `lists` - List elements (gap-based positioning)
- `sets` - Set members
- `zsets` - Sorted set members with scores

## License

Apache 2.0
