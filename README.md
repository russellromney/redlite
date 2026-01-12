# redlite 🔴

SQLite-backed Redis-compatible embedded key-value store written in Rust.

## Why?

- **Embedded-first** - Use as a library, no separate server needed
- **Disk is cheap** - Persistent storage without Redis's memory constraints
- **SQLite foundation** - ACID transactions, durability, zero config
- **Redis compatible** - Existing clients just work (for most operations)
- **Easy backup** - just use using SQLite WAL-tailing tools like  [litestream](url) or [walsync](url)
- **Extra features**
  - created/updated - `KEYINFO mykey`
  - save version history: `SET mykey 1 HISTORY`
  - fetch version ranges:
    - before: `GET mykey HISTORY 0 676767` [LIMIT n]
    - including and after: `GET mykey 676767` [LIMIT n]
  - configure retention: `CONFIG SET history-retention-days 30`
  - remove old versions: `COMPACT HISTORY mykey`
  - time travel queries: `GET mykey AS OF 6767676767`
  - full-text search: `FTSEARCH "hello world" [LIMIT n]`
  - hash search: `HFTSEARCH user:* "alice" [LIMIT n]`

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

// Multiple databases (like Redis SELECT)
db.select(1)?;
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

### Server
- `PING`, `ECHO`, `QUIT`, `COMMAND`

### Planned
- **Lists**: LPUSH, RPUSH, LPOP, RPOP, LRANGE
- **Sets**: SADD, SREM, SMEMBERS, SINTER, SUNION
- **Sorted Sets**: ZADD, ZRANGE, ZSCORE, ZRANK
- **Custom**: VACUUM, KEYINFO (type, ttl, created_at, updated_at)

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
