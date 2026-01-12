# Redlite Roadmap

## Implementation Sessions

Incremental implementation plan. Each session = one commit = one testable feature.

---

## Principles

1. **Embedded-first** — Library mode is the primary use case
2. **Disk is cheap** — Don't optimize for memory like Redis
3. **SQLite is the foundation** — Leverage its strengths (ACID, durability)
4. **Redis-compatible** — Existing clients should just work
5. **Extend thoughtfully** — Add features Redis doesn't have (KEYINFO, history, FTS)


### Session 1: Foundation
- [ ] Create project structure (Cargo.toml, src/lib.rs, src/main.rs)
- [ ] Schema + migrations (schema.sql)
- [ ] Db struct with open/open_memory
- [ ] GET command
- [ ] SET command (basic, no options yet)
- [ ] RESP parser (arrays + bulk strings only)
- [ ] TCP server (minimal)
- [ ] **Test:** `redis-cli SET foo bar` + `GET foo` works

### Session 2: Key Management Basics
- [ ] DEL
- [ ] EXISTS
- [ ] TYPE
- [ ] KEYS (with GLOB pattern)
- [ ] Unit tests for all
- [ ] **Test:** redis-cli verification

### Session 3: Expiration
- [ ] SET with EX/PX options
- [ ] EXPIRE, PEXPIRE
- [ ] EXPIREAT, PEXPIREAT
- [ ] TTL, PTTL
- [ ] PERSIST
- [ ] Lazy expiration (delete on read if expired)
- [ ] **Test:** Set key with TTL, wait, verify gone

### Session 4: Atomic Operations
- [ ] INCR, DECR
- [ ] INCRBY, DECRBY
- [ ] INCRBYFLOAT
- [ ] Unit tests
- [ ] **Test:** redis-cli INCR counter

### Session 5: String Extras
- [ ] MGET, MSET
- [ ] APPEND
- [ ] STRLEN
- [ ] SETNX
- [ ] SETEX, PSETEX
- [ ] SET with NX/XX options
- [ ] **Test:** Batch operations work

### Session 6: Hashes
- [ ] HSET, HGET
- [ ] HMGET, HGETALL
- [ ] HDEL, HEXISTS
- [ ] HKEYS, HVALS, HLEN
- [ ] HINCRBY, HINCRBYFLOAT
- [ ] HSETNX
- [ ] **Test:** redis-cli hash operations

### Session 7: Lists
- [ ] LPUSH, RPUSH
- [ ] LPOP, RPOP
- [ ] LLEN, LRANGE
- [ ] LINDEX, LSET
- [ ] LTRIM
- [ ] Integer gap positioning
- [ ] **Test:** redis-cli list operations

### Session 8: Sets
- [ ] SADD, SREM
- [ ] SMEMBERS, SISMEMBER
- [ ] SCARD
- [ ] SPOP, SRANDMEMBER
- [ ] SDIFF, SINTER, SUNION
- [ ] **Test:** redis-cli set operations

### Session 9: Sorted Sets
- [ ] ZADD, ZREM
- [ ] ZSCORE, ZRANK, ZREVRANK
- [ ] ZCARD
- [ ] ZRANGE, ZREVRANGE
- [ ] ZRANGEBYSCORE
- [ ] ZCOUNT
- [ ] ZINCRBY
- [ ] ZREMRANGEBYRANK, ZREMRANGEBYSCORE
- [ ] **Test:** redis-cli sorted set operations

### Session 10: Server Operations
- [ ] PING, ECHO
- [ ] SELECT (multiple databases)
- [ ] INFO
- [ ] DBSIZE
- [ ] FLUSHDB
- [ ] SCAN (with MATCH, COUNT)
- [ ] QUIT
- [ ] **Test:** Full server command suite

### Session 11: Custom Commands
- [ ] VACUUM (delete expired + SQLite VACUUM)
- [ ] KEYINFO (type, ttl, created_at, updated_at)
- [ ] **Test:** Custom commands work

### Session 12: Polish & Release
- [ ] Error messages match Redis
- [ ] Edge cases handled
- [ ] README with usage examples
- [ ] `cargo publish` ready
- [ ] **Test:** Full compatibility test suite

---

## V1 — Core Redis Compatibility

**Goal:** Drop-in Redis replacement for 90% of use cases.

### Commands

- **Strings:** GET, SET, MGET, MSET, INCR, INCRBY, DECR, DECRBY, INCRBYFLOAT, APPEND, STRLEN, SETNX, SETEX, PSETEX
- **Keys:** DEL, EXISTS, EXPIRE, PEXPIRE, EXPIREAT, PEXPIREAT, TTL, PTTL, PERSIST, TYPE, KEYS, SCAN, DBSIZE, FLUSHDB
- **Hashes:** HGET, HSET, HMGET, HGETALL, HDEL, HEXISTS, HKEYS, HVALS, HLEN, HINCRBY, HINCRBYFLOAT, HSETNX
- **Lists:** LPUSH, RPUSH, LPOP, RPOP, LLEN, LRANGE, LINDEX, LSET, LTRIM
- **Sets:** SADD, SREM, SMEMBERS, SISMEMBER, SCARD, SPOP, SRANDMEMBER, SDIFF, SINTER, SUNION
- **Sorted Sets:** ZADD, ZREM, ZSCORE, ZRANK, ZREVRANK, ZRANGE, ZREVRANGE, ZRANGEBYSCORE, ZCOUNT, ZCARD, ZINCRBY, ZREMRANGEBYRANK, ZREMRANGEBYSCORE
- **Server:** PING, ECHO, SELECT, INFO, QUIT

### Custom Commands

- **VACUUM** — Delete expired keys, run SQLite VACUUM
- **KEYINFO** — Get metadata: type, ttl, created_at, updated_at

### Features

- [x] SQLite schema design (documented)
- [ ] RESP protocol parser (Session 1)
- [ ] TCP server mode (Session 1)
- [ ] Embedded library mode (Session 1+)
- [ ] Lazy expiration (Session 3)
- [ ] Multiple databases (Session 10)
- [ ] Pattern matching (Session 2, 10)
- [ ] Integer-gap list positioning (Session 7)

### Testing

- [ ] Unit tests for each command (every session)
- [ ] Integration tests with redis-cli (every session)
- [ ] Compatibility tests against real Redis (Session 12)

---

## V2 — Extended Commands & Features

**Goal:** Fill gaps, add unique features.

### Additional Commands

- **Transactions:** MULTI, EXEC, DISCARD
- **Keys:** RENAME, RENAMENX
- **Lists:** LINSERT, LREM, LMOVE
- **Sets:** SMOVE, SDIFFSTORE, SINTERSTORE, SUNIONSTORE
- **Sorted Sets:** ZINTERSTORE, ZUNIONSTORE
- **Iteration:** HSCAN, SSCAN, ZSCAN

### Pub/Sub (Embedded)

Fire-and-forget messaging via channels:

```rust
// Subscriber
let sub = db.subscribe("events");
std::thread::spawn(move || {
    while let Some(msg) = sub.recv() {
        println!("Got: {:?}", msg);
    }
});

// Publisher
db.publish("events", b"hello")?;  // Returns immediately
```

- Channel-based delivery (like Go channels)
- At-most-once semantics (matches Redis)
- No persistence (messages lost if no subscribers)
- Potential improvement: delivery tracking, receipt confirmation

Commands: SUBSCRIBE, UNSUBSCRIBE, PUBLISH, PSUBSCRIBE, PUNSUBSCRIBE

### History Tracking (Opt-in)

Track value changes per key:

```
SET mykey value HISTORY    -- Enable history for this key
SET mykey newvalue         -- Change logged to history table
HISTORY mykey [COUNT n]    -- Get previous values
GETVERSION mykey 3         -- Get specific version
```

Schema addition:
```sql
CREATE TABLE key_history (
    id INTEGER PRIMARY KEY,
    key_id INTEGER NOT NULL REFERENCES keys(id),
    old_value BLOB,
    new_value BLOB,
    operation TEXT NOT NULL,  -- 'set', 'incr', 'del', etc.
    changed_at INTEGER NOT NULL
);
```

- Separate table for history
- Writes in transactions (atomic with main write)
- Disk is cheap — history rarely accessed but valuable

### Active Expiration

Opt-in background daemon:

```rust
let db = Db::open("data.db")?;
db.start_expiration_daemon();  // Spawns background thread
```

---

## V3 — Advanced Features

**Goal:** Unique differentiators, production-grade features.

### Time-Travel Queries

Query historical state:

```
GET mykey AS OF 1704067200000    -- Value at timestamp
GETRANGE mykey 1704000000000 1704100000000  -- Changes in time range
```

Requires V2 history tracking.

### History Retention & Compaction

```
CONFIG SET history-retention-days 30
COMPACT HISTORY mykey    -- Remove old versions
```

### Full-Text Search

FTS5 over string values:

```
FTSEARCH "hello world" [LIMIT n]   -- Search all string values
HFTSEARCH user:* "alice" [LIMIT n] -- Search hash values matching pattern
```

Implementation:
- FTS5 virtual table mirroring string/hash values
- Triggers to keep FTS index in sync
- Opt-in per key or global

### Replication (walsync)

Leader-follower replication via walsync:

```
# Leader
redlite --db=data.db --walsync-publish=s3://bucket/wal/

# Follower
redlite --db=replica.db --walsync-subscribe=s3://bucket/wal/ --readonly
```

### In-Memory Mode

Optional memory-first with WAL sync:

```rust
let db = Db::open_memory_with_wal("backup.db")?;
```

- Fast reads/writes in memory
- Periodic WAL sync to disk
- Configurable sync interval

### Server-Mode Pub/Sub

Full Redis pub/sub for multi-client scenarios:

- Persistent TCP connections
- Server pushes to subscribers
- RESP3 push notifications

### Maybe (If Requested)

- WATCH/UNWATCH (optimistic locking)
- Streams (append-only logs with consumer groups)
- Lua scripting (probably not)

