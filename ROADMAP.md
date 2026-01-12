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
6. **Blocking = Server mode** — BLOCK, Pub/Sub require server mode (cross-process coordination)


### Session 1: Foundation ✅
- [x] Create project structure (Cargo.toml, src/lib.rs, src/main.rs)
- [x] Schema + migrations (schema.sql)
- [x] Db struct with open/open_memory
- [x] GET command
- [x] SET command (with EX/PX/NX/XX options)
- [x] DEL command (db layer)
- [x] RESP parser (arrays + bulk strings only)
- [x] TCP server (minimal)
- [x] Lazy expiration (delete on read if expired)
- [x] **Test:** 24 unit tests + 7 integration tests passing

### Session 2: Key Management ✅
- [x] DEL (server handler)
- [x] EXISTS
- [x] TYPE
- [x] KEYS (with GLOB pattern)
- [x] SCAN (with MATCH, COUNT)
- [x] TTL, PTTL
- [x] EXPIRE
- [x] Unit tests for all (14 new tests)
- [x] Integration tests (10 new tests)
- [x] **Test:** 37 unit tests + 16 integration tests passing

### Session 3: String Operations ✅
- [x] INCR, DECR
- [x] INCRBY, DECRBY
- [x] INCRBYFLOAT
- [x] MGET, MSET
- [x] APPEND
- [x] STRLEN
- [x] GETRANGE, SETRANGE
- [x] Unit tests (13 new memory + 6 new disk tests)
- [x] Integration tests (7 new tests)
- [x] **Test:** 56 unit tests + 23 integration tests passing

### Session 6: Hashes ✅
- [x] HSET, HGET
- [x] HMGET, HGETALL
- [x] HDEL, HEXISTS
- [x] HKEYS, HVALS, HLEN
- [x] HINCRBY, HINCRBYFLOAT
- [x] HSETNX
- [x] Unit tests (30 new tests)
- [x] Integration tests (13 new tests)
- [x] **Test:** 86 unit tests + 36 integration tests passing

### Session 7: Lists ✅
- [x] LPUSH, RPUSH
- [x] LPOP, RPOP (with optional count)
- [x] LLEN, LRANGE
- [x] LINDEX, LSET
- [x] LTRIM
- [x] Integer gap positioning (POS_GAP = 1,000,000)
- [x] Unit tests (22 new tests)
- [x] Integration tests (10 new tests)
- [x] **Test:** 108 unit tests + 46 integration tests passing

### Session 8: Sets ✅
- [x] SADD, SREM
- [x] SMEMBERS, SISMEMBER
- [x] SCARD
- [x] SPOP (with optional count), SRANDMEMBER (with positive/negative count)
- [x] SDIFF, SINTER, SUNION
- [x] Unit tests (23 new tests)
- [x] Integration tests (11 new tests)
- [x] **Test:** 131 unit tests + 57 integration tests passing

### Session 9: Sorted Sets ✅
- [x] ZADD, ZREM
- [x] ZSCORE, ZRANK, ZREVRANK
- [x] ZCARD
- [x] ZRANGE, ZREVRANGE
- [x] ZRANGEBYSCORE
- [x] ZCOUNT
- [x] ZINCRBY
- [x] ZREMRANGEBYRANK, ZREMRANGEBYSCORE
- [x] Unit tests (24 new tests)
- [x] Integration tests (13 new tests)
- [x] **Test:** 155 unit tests + 70 integration tests passing

### Session 10: Server Operations ✅
- [x] PING, ECHO (already implemented)
- [x] SELECT (multiple databases 0-15)
- [x] INFO (basic server stats)
- [x] DBSIZE (key count per database)
- [x] FLUSHDB (delete all keys in current db)
- [x] SCAN (already implemented with MATCH, COUNT)
- [x] QUIT (already implemented)
- [x] **Architecture:** Per-connection database isolation
  - Refactored `Db` into `DbCore` (shared backend) + `Db` (per-session wrapper)
  - `Db::session()` creates new session sharing same SQLite connection
  - Each session has its own `selected_db` (no more race conditions)
  - `Db` is now `Clone` (cheap Arc clone)
  - `select(&mut self)` for explicit mutability
- [x] Unit tests (11 new tests)
- [x] Integration tests (9 new tests)
- [x] **Test:** 166 unit tests + 79 integration tests passing

### Session 11: Custom Commands ✅
- [x] VACUUM (delete expired keys across all dbs + SQLite VACUUM)
- [x] KEYINFO key (returns type, ttl, created_at, updated_at as hash-like array)
- [x] AUTOVACUUM ON/OFF/INTERVAL (automatic expiration cleanup, default ON @ 60s)
  - `AUTOVACUUM` - show status (enabled + interval_ms)
  - `AUTOVACUUM ON/OFF` - enable/disable
  - `AUTOVACUUM INTERVAL <ms>` - set interval (min 1000ms)
  - Shared AtomicI64 timestamp across sessions
  - Triggered on read operations (GET, HGET, SMEMBERS, ZRANGE, LRANGE, EXISTS)
  - Compare-exchange ensures only one connection does cleanup per interval
- [x] Unit tests (11 new tests)
- [x] Integration tests (9 new tests)
- [x] **Test:** 177 unit tests + 88 integration tests passing

### Session 12: Polish & Release ✅
- [x] Error messages match Redis (fixed double ERR prefix)
- [x] Edge cases handled (WRONGTYPE for string ops: GET, APPEND, INCR, INCRBYFLOAT)
- [x] README with usage examples (already complete from Session 11)
- [x] `cargo publish` ready (keywords, categories, docs, license fixed)
- [x] **Test:** 178 unit tests + 88 integration tests passing

### Session 13: Streams (Basic) ✅
- [x] Schema: `streams` table (key_id, entry_ms, entry_seq, data BLOB, created_at)
- [x] KeyType::Stream (type = 6)
- [x] XADD key [NOMKSTREAM] [MAXLEN|MINID [=|~] threshold] *|id field value [field value ...]
- [x] XLEN key
- [x] XRANGE key start end [COUNT count]
- [x] XREVRANGE key end start [COUNT count]
- [x] XREAD [COUNT count] STREAMS key [key ...] id [id ...]
- [x] XTRIM key MAXLEN|MINID [=|~] threshold
- [x] XDEL key id [id ...]
- [x] XINFO STREAM key
- [x] Entry ID format: `{timestamp}-{seq}` for Redis compat
- [x] Store fields as MessagePack blob (rmp-serde)
- [x] Unit tests (17 new tests) + integration tests (13 new tests)
- [x] **Test:** 195 unit tests + 101 integration tests passing

### Session 14: Streams (Consumer Groups) ✅
- [x] Schema: `stream_groups`, `stream_consumers`, `stream_pending` (already in schema.sql)
- [x] XGROUP CREATE key groupname id|$ [MKSTREAM]
- [x] XGROUP DESTROY key groupname
- [x] XGROUP SETID key groupname id|$
- [x] XGROUP CREATECONSUMER key groupname consumername
- [x] XGROUP DELCONSUMER key groupname consumername
- [x] XREADGROUP GROUP group consumer [COUNT count] [NOACK] STREAMS key [key ...] id [id ...]
- [x] XACK key group id [id ...]
- [x] XPENDING key group [[IDLE min-idle-time] start end count [consumer]]
- [x] XCLAIM key group consumer min-idle-time id [id ...] [IDLE ms] [TIME ms] [RETRYCOUNT count] [FORCE] [JUSTID]
- [x] XINFO GROUPS key
- [x] XINFO CONSUMERS key groupname
- [x] Unit tests (15 new tests) + integration tests (20 new tests)
- [x] **Test:** 210 unit tests + 121 integration tests passing (before Session 15.1)

### Session 15.1: Blocking Reads — Notification Infrastructure ✅
- [x] Server: Add `notify: Arc<RwLock<HashMap<String, broadcast::Sender<()>>>>` for key notifications
- [x] Db/DbCore: Add optional notifier field for server mode detection
- [x] Add helper methods: `is_server_mode()`, `notify_key()`, `subscribe_key()`
- [x] Update `Server::new()` to initialize and pass notifier
- [x] Update `handle_connection()` to have notifier context
- [x] Embedded mode returns Closed for blocking operations (subscribe_key)
- [x] **Test:** 7 unit tests, all 340 tests passing (217 lib + 121 integration + 2 doc)

### Session 15.2: Blocking Reads — Broadcasting on Writes ✅
- [x] Make LPUSH broadcast to channel after insert
- [x] Make RPUSH broadcast to channel after insert
- [x] Make XADD broadcast to channel after insert
- [x] Implement async notification spawning for sync methods
- [x] **Test:** 8 unit tests + 5 integration tests, all 352 tests passing (224 lib + 126 integration + 2 doc)

### Session 15.3: Blocking Reads — Blocking Commands ✅
- [x] Make execute_command async
- [x] Implement BLPOP key [key ...] timeout
- [x] Implement BRPOP key [key ...] timeout
- [x] Add XREAD BLOCK milliseconds STREAMS key [key ...] id [id ...]
- [x] Add XREADGROUP BLOCK milliseconds ... GROUP group consumer STREAMS key [key ...]
- [x] Timeout handling (return nil on timeout)
- [x] `tokio::select!` for multi-key blocking (handles up to 5 keys per select)
- [x] Timeout=0 blocks indefinitely with far-future deadline
- [x] Unit tests + integration tests (all 352 existing tests pass)
- [x] **Test:** 224 lib + 126 integration + 2 doc tests passing

### Session 15.4: Pub/Sub Messaging ✅
- [x] Architecture: `pubsub_channels: Arc<RwLock<HashMap<String, broadcast::Sender<PubSubMessage>>>>`
- [x] `ConnectionState` enum: Normal vs Subscribed (with channels, patterns, receivers)
- [x] New module: `src/server/pubsub.rs` (450+ lines, 8 unit tests)
- [x] Command PUBLISH channel message — fire-and-forget, returns subscriber count
- [x] Command SUBSCRIBE channel [channel ...] — enter subscription mode
- [x] Command UNSUBSCRIBE [channel ...] — exit subscription mode or unsubscribe from channels
- [x] Command PSUBSCRIBE pattern [pattern ...] — glob pattern subscriptions
- [x] Command PUNSUBSCRIBE [pattern ...] — unsubscribe from patterns
- [x] Glob pattern matching — supports `*` (any sequence), `?` (single char), `[abc]` (char set)
- [x] Subscription mode restrictions: only SUBSCRIBE/UNSUBSCRIBE/PSUBSCRIBE/PUNSUBSCRIBE/PING/QUIT
- [x] Modified `handle_connection()` with `tokio::select!` for subscription mode
- [x] RESP2 message format: `["message", "channel", "payload"]`, `["pmessage", "pattern", "channel", "payload"]`
- [x] Confirmation messages: `["subscribe", "channel", count]`, `["psubscribe", "pattern", count]`
- [x] Connection state machine: Normal → Subscribed → Normal
- [x] Unit tests: 8 glob matching + state tracking tests
- [x] Integration tests: 5 pub/sub tests (PUBLISH with/without subscribers)
- [x] **Test:** 232 lib + 131 integration + 2 doc tests passing (365 total)

### Session 16: Transactions (MULTI/EXEC/DISCARD) ✅

Split into 6 focused sessions (like Session 15).

#### Session 16.1: Transaction State Management ✅
- [x] QueuedCommand struct for buffering commands
- [x] ConnectionState::Transaction variant + helper methods
- [x] is_transaction() state checker
- [x] Handle Transaction variant in all pub/sub match statements
- [x] Unit tests: 5 transaction state tests + existing 8 glob tests
- [x] **Test:** 237 lib + 131 integration + 2 doc tests passing (370 total)

**Commit:** "feat(transactions): Session 16.1 - Transaction State Management"

#### Session 16.2: Command Queueing ✅
- [x] cmd_multi() — Enter transaction mode
- [x] cmd_discard() — Exit transaction, clear queue
- [x] execute_transaction_command() dispatcher
- [x] Integrate into handle_connection() loop
- [x] Command validation (reject WATCH, blocking, pub/sub)
- [x] Unit tests: 12 queueing tests
- [x] Integration tests: 3 basic redis-cli tests
- [x] **Test:** 249 lib + 134 integration + 2 doc tests passing

#### Session 16.3: EXEC Implementation ✅
- [x] Async execute_transaction_command() — Routes DISCARD and EXEC
- [x] Async execute_transaction() with command replay
- [x] Atomic execution of queued commands
- [x] State management: queue extraction, return to Normal mode
- [x] Result array formatting
- [x] Unit tests: 6 execution tests
- [x] Integration tests: 3 atomicity tests
- [x] **Test:** 255 lib + 133+ integration + 2 doc tests passing

#### Session 16.4: Error Handling & Restrictions ✅
- [x] Nested MULTI rejection with error message
- [x] WATCH/UNWATCH rejection in transaction mode
- [x] Blocking command rejection (BLPOP, BRPOP, BRPOPLPUSH)
- [x] Pub/Sub command rejection (SUBSCRIBE, PSUBSCRIBE, UNSUBSCRIBE, PUNSUBSCRIBE)
- [x] DISCARD/EXEC without MULTI error handling
- [x] Unit tests: 6 restriction/error tests + 6 edge case tests
- [x] Integration tests: 3 error scenario tests
- [x] **Test:** 255 lib + 133+ integration tests passing (388 total)

#### Session 16.5: Integration Testing (Placeholder)
- [ ] Comprehensive redis-cli tests (multi-connection scenarios)
- [ ] Atomicity verification across multiple keys
- [ ] Edge cases: large transactions, concurrent access

#### Session 16.6: Documentation & Polish (Placeholder)
- [ ] Doc comments refinement
- [ ] Error message consistency review
- [ ] Performance optimization if needed

---

## MVP — Core Redis Compatibility

**Goal:** Drop-in Redis replacement for almost all use cases.

### Commands

- **Strings:** GET, SET, MGET, MSET, INCR, INCRBY, DECR, DECRBY, INCRBYFLOAT, APPEND, STRLEN, SETNX, SETEX, PSETEX
- **Keys:** DEL, EXISTS, EXPIRE, PEXPIRE, EXPIREAT, PEXPIREAT, TTL, PTTL, PERSIST, TYPE, KEYS, SCAN, DBSIZE, FLUSHDB
- **Hashes:** HGET, HSET, HMGET, HGETALL, HDEL, HEXISTS, HKEYS, HVALS, HLEN, HINCRBY, HINCRBYFLOAT, HSETNX
- **Lists:** LPUSH, RPUSH, LPOP, RPOP, LLEN, LRANGE, LINDEX, LSET, LTRIM
- **Sets:** SADD, SREM, SMEMBERS, SISMEMBER, SCARD, SPOP, SRANDMEMBER, SDIFF, SINTER, SUNION
- **Sorted Sets:** ZADD, ZREM, ZSCORE, ZRANK, ZREVRANK, ZRANGE, ZREVRANGE, ZRANGEBYSCORE, ZCOUNT, ZCARD, ZINCRBY, ZREMRANGEBYRANK, ZREMRANGEBYSCORE
- **Server:** PING, ECHO, SELECT, INFO, QUIT
- **Streams:** XADD, XREAD, XRANGE, XREVRANGE, XLEN, XTRIM, XDEL, XINFO STREAM, XGROUP CREATE/DESTROY/SETID/CREATECONSUMER/DELCONSUMER, XREADGROUP, XACK, XPENDING, XCLAIM, XINFO GROUPS/CONSUMERS
- **Blocking (Server Mode Only):** BLPOP, BRPOP, XREAD BLOCK, XREADGROUP BLOCK, SUBSCRIBE, PUBLISH

_Blocking commands require server mode. Embedded mode returns error._

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

### Pub/Sub (Server Mode Only)

Fire-and-forget messaging via channels. **Server mode required** — notifications need a central coordinator.

```bash
# Terminal 1: Subscriber
redis-cli -p 6767 SUBSCRIBE events

# Terminal 2: Publisher
redis-cli -p 6767 PUBLISH events "hello"
```

- Channel-based delivery (tokio broadcast channels in server)
- At-most-once semantics (matches Redis)
- No persistence (messages lost if no subscribers)
- RESP3 push notifications to subscribers

Commands: SUBSCRIBE, UNSUBSCRIBE, PUBLISH, PSUBSCRIBE, PUNSUBSCRIBE

_Embedded mode returns error for all Pub/Sub commands._

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

**AUTOVACUUM** - ✅ Implemented in V1 (Session 11)

Future enhancement: **Background daemon**
```rust
let db = Db::open("data.db")?;
db.start_expiration_daemon();  // Spawns background thread
```
- Periodically scans and deletes expired keys
- Optionally runs SQLite VACUUM
- Useful for long-running servers with idle periods

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

### Maybe (If Requested)

- WATCH/UNWATCH (optimistic locking)
- Lua scripting (probably not)
- XAUTOCLAIM (auto-reassign stuck messages)

