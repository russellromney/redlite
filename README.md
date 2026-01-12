# redlite 🔴

SQLite-backed Redis-compatible embedded key-value store written in Rust.

**Documentation:** [redlite.dev](https://redlite.dev)

## Why?

- **Embedded-first** - Use as a library, no separate server needed
- **Disk is cheap** - Persistent storage without Redis's memory constraints
- **SQLite foundation** - ACID transactions, durability, zero config
- **Redis compatible** - Existing clients just work (for most operations)
- **Easy backup** - SQLite WAL files work with [litestream](https://litestream.io) or similar tools

## Differences from Redis

**Embedded mode** is fully Redis-compatible for strings, hashes, lists, sets, sorted sets, and streams.

**Server mode only** features (not available in embedded mode):
- **Pub/Sub** - At-most-once, fire-and-forget (messages not persisted if no subscribers)
- **Transactions** - MULTI/EXEC/DISCARD (WATCH/UNWATCH not supported; blocking commands not allowed)
- **Blocking reads** - BLPOP, BRPOP, XREAD BLOCK, XREADGROUP BLOCK

**Architectural differences:**
- **Persistence** - Data always disk-backed; no Redis SAVE/BGSAVE (use SQLite WAL tools)
- **Memory** - Uses SQLite storage, not RAM-optimized for speed
- **Replication** - No built-in replication; use SQLite backup mechanisms

**Not implemented:**
- Cluster mode, Sentinel, Lua scripting, Modules, ACL, Stream XAUTOCLAIM

## Extensions

- `VACUUM` - Delete expired keys and reclaim disk space
- `KEYINFO key` - Returns type, ttl, created_at, updated_at
- `AUTOVACUUM ON|OFF` - Auto-cleanup interval (default: ON, 60s)
- `HISTORY` - Track and query operation history with time-travel queries (Session 17)

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

## Commands

**Strings:** `GET`, `SET` (EX, PX, NX, XX), `INCR`, `DECR`, `INCRBY`, `DECRBY`, `INCRBYFLOAT`, `MGET`, `MSET`, `APPEND`, `STRLEN`, `GETRANGE`, `SETRANGE`

**Keys:** `DEL`, `EXISTS`, `TYPE`, `EXPIRE`, `TTL`, `PTTL`, `KEYS`, `SCAN`

**Hashes:** `HSET`, `HGET`, `HSETNX`, `HMGET`, `HGETALL`, `HDEL`, `HEXISTS`, `HKEYS`, `HVALS`, `HLEN`, `HINCRBY`, `HINCRBYFLOAT`

**Lists:** `LPUSH`, `RPUSH`, `LPOP`, `RPOP`, `LLEN`, `LRANGE`, `LINDEX`, `LSET`, `LTRIM`

**Sets:** `SADD`, `SREM`, `SMEMBERS`, `SISMEMBER`, `SCARD`, `SPOP`, `SRANDMEMBER`, `SDIFF`, `SINTER`, `SUNION`

**Sorted Sets:** `ZADD`, `ZREM`, `ZSCORE`, `ZRANK`, `ZREVRANK`, `ZCARD`, `ZRANGE`, `ZREVRANGE` (WITHSCORES), `ZRANGEBYSCORE`, `ZCOUNT`, `ZINCRBY`, `ZREMRANGEBYRANK`, `ZREMRANGEBYSCORE`

**Streams:** `XADD`, `XLEN`, `XRANGE`, `XREVRANGE`, `XREAD`, `XTRIM`, `XDEL`, `XINFO STREAM`, `XGROUP CREATE`, `XGROUP DESTROY`, `XGROUP SETID`, `XREADGROUP`, `XACK`, `XPENDING`, `XCLAIM`

**Blocking Reads (server mode):** `BLPOP`, `BRPOP`, `XREAD BLOCK`, `XREADGROUP BLOCK`

**Pub/Sub (server mode):** `SUBSCRIBE`, `UNSUBSCRIBE`, `PUBLISH`, `PSUBSCRIBE`, `PUNSUBSCRIBE`

**Transactions:** `MULTI`, `EXEC`, `DISCARD`

**History Tracking:** `HISTORY ENABLE`, `HISTORY DISABLE`, `HISTORY GET`, `HISTORY GETAT`, `HISTORY STATS`, `HISTORY CLEAR`, `HISTORY PRUNE`, `HISTORY LIST`

**Server:** `PING`, `ECHO`, `QUIT`, `COMMAND`, `SELECT`, `DBSIZE`, `FLUSHDB`, `INFO`

## Recently Completed

**Session 17: History Tracking & Time-Travel Queries** ✅
- Track value changes per key with three-tier opt-in (global, database, key-level)
- Time-travel queries: `HISTORY GETAT key timestamp`
- Configurable retention policies (unlimited, time-based, count-based)
- Automatic instrumentation on write operations (SET, DEL, HSET, LPUSH, XADD, etc.)
- Full HISTORY command suite with enable/disable/get/stats/clear/prune/list subcommands

## Upcoming Features

See [ROADMAP.md](./ROADMAP.md) for detailed plans.

**Session 18: Performance Testing & Benchmarking** (Next)
- Establish baseline QPS metrics in embedded mode
- Profile and optimize hot paths (SQLite, RESP parsing, expiration)
- Target: 10,000+ QPS

**Sessions 19-21: Language Bindings** (After S18)
- **Python** (`redlite-py`) - PyO3 bindings via PyPI
- **Node.js/Bun** (`redlite-js`) - NAPI-RS bindings via npm
- **C FFI + Go** - C bindings via cbindgen + Go cgo wrapper

**V3+ Features:**
- Full-text search (SQLite FTS5)
- Replication (walsync-based)
- Active expiration daemon
- History replay & reconstruction

## Testing

```bash
# Unit tests (memory + disk)
cargo test --lib

# Integration tests (requires redis-cli)
cargo build && cargo test --test integration -- --test-threads=1

# All tests
cargo test -- --test-threads=1
```

## License

Apache 2.0
