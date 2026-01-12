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
  - Transactions: `MULTI`/`EXEC`/`DISCARD` (atomic multi-command execution)
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

### Streams
- `XADD` (with NOMKSTREAM, MAXLEN, MINID options)
- `XLEN`, `XRANGE`, `XREVRANGE`
- `XREAD` (with COUNT)
- `XTRIM` (MAXLEN, MINID)
- `XDEL`
- `XINFO STREAM`, `XINFO GROUPS`, `XINFO CONSUMERS`

### Stream Consumer Groups
- `XGROUP CREATE`, `XGROUP DESTROY`, `XGROUP SETID`
- `XGROUP CREATECONSUMER`, `XGROUP DELCONSUMER`
- `XREADGROUP` (with COUNT, NOACK)
- `XACK`
- `XPENDING` (summary and range forms, with IDLE filter)
- `XCLAIM` (with IDLE, TIME, RETRYCOUNT, FORCE, JUSTID)

### Blocking Reads (Server Mode Only, Session 15+) ✅
**Status**: Complete (Session 15.1-15.3 ✅)
- `BLPOP key [key ...] timeout` (blocking list pop) ✅
- `BRPOP key [key ...] timeout` (blocking list pop from right) ✅
- `XREAD BLOCK milliseconds ...` (blocking stream read) ✅
- `XREADGROUP BLOCK milliseconds ...` (blocking stream read with consumer groups) ✅

### Pub/Sub (Server Mode Only, Session 15.4) ✅
**Status**: Complete (Session 15.4 ✅) — Fire-and-forget messaging with at-most-once semantics

Fire-and-forget messaging via channels (server mode required). Excellent for notifications and event streaming.

```bash
# Terminal 1: Subscribe to channels
redis-cli -p 6767 SUBSCRIBE events notifications

# Terminal 2: Subscribe to patterns
redis-cli -p 6767 PSUBSCRIBE "events.*" "alerts.*"

# Terminal 3: Publish messages
redis-cli -p 6767 PUBLISH events "hello"       # → Delivers to "events" subscribers
redis-cli -p 6767 PUBLISH events.login "user"  # → Delivers to pattern subscribers
redis-cli -p 6767 PUBLISH other "data"         # → No subscribers, returns 0
```

**Commands:**
- `SUBSCRIBE channel [channel ...]` — Subscribe to one or more channels
- `UNSUBSCRIBE [channel ...]` — Unsubscribe from channels (all if none specified)
- `PUBLISH channel message` — Publish message, returns count of subscribers that received it
- `PSUBSCRIBE pattern [pattern ...]` — Subscribe to channel patterns (glob syntax: `*`, `?`, `[abc]`)
- `PUNSUBSCRIBE [pattern ...]` — Unsubscribe from patterns

**Characteristics:**
- Subscription mode: Connection restricted to pub/sub commands + PING/QUIT
- At-most-once semantics: Messages lost if no subscribers (no persistence)
- Broadcast delivery: Multiple subscribers receive same message
- Lazy channel creation: Channels created on first subscriber
- Auto-cleanup: Channels removed when all subscribers disconnect

### Transactions (Server Mode Only, Session 16) 🚧
**Status**: Under Development (Session 16.1 ✅) — Atomic multi-command execution with ACID guarantees

Execute multiple commands atomically using SQLite transactions.

```bash
# Terminal: Execute multiple commands atomically
redis-cli -p 6767 MULTI
redis-cli -p 6767 SET counter 0
redis-cli -p 6767 INCR counter
redis-cli -p 6767 INCR counter
redis-cli -p 6767 GET counter
redis-cli -p 6767 EXEC  # Returns: [OK, (integer) 1, (integer) 2, "0"]
```

**Commands:**
- `MULTI` — Enter transaction mode, queue subsequent commands
- `EXEC` — Execute all queued commands atomically in a single SQLite transaction
- `DISCARD` — Cancel transaction and clear the command queue

**Characteristics:**
- Atomic execution: All commands succeed or all rollback (no partial writes)
- SQLite-backed: True ACID transactions with durability
- Per-connection: Each connection has independent transaction state
- Queueing: Commands return "QUEUED" immediately in transaction mode
- Error handling: Transaction aborted if any command fails, all changes rolled back
- Restrictions: WATCH/UNWATCH, blocking commands, and pub/sub commands not allowed in transactions

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
- `streams` - Stream entries (timestamp-sequence IDs, MessagePack fields)
- `stream_groups` - Consumer groups (name, last delivered ID)
- `stream_consumers` - Consumers in groups (name, last seen time)
- `stream_pending` - Pending entries (entry, consumer, delivery count)

## License

Apache 2.0
