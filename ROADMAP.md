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

### Session 17: History Tracking & Time-Travel Queries ✅

**Status:** Complete - All Sessions 17.1-17.7 ✅

**Goal:** Implement versioned history tracking with three-tier opt-in (global, per-database, per-key) and time-travel query commands.

#### Session 17.1: Schema & Types ✅
- [x] Create `src/schema_history.sql` with history tables:
  - `history_config` (three-tier opt-in: global, database, key)
  - `key_history` (versioned snapshots with MessagePack encoding)
  - Indexes: `(key_id, timestamp_ms DESC)`, `(db, key, timestamp_ms DESC)`
- [x] Add types to `src/types.rs`:
  - `HistoryEntry` struct (id, version_num, operation, timestamp_ms, key_type, data_snapshot, expire_at)
  - `HistoryLevel` enum (Global, Database(i32), Key)
  - `HistoryConfig` struct
  - `RetentionType` enum (Unlimited, Time(i64), Count(i64))
  - `HistoryStats` struct (total_entries, oldest/newest timestamps, storage_bytes)
- [x] Update `Db::migrate()` to run history migrations
- [x] Unit tests: Schema validation (3 tests), type operations (12 tests)
- [x] **Test:** 270 unit tests passing (15 new tests + 255 existing)

#### Session 17.2: Configuration Methods (Enable/Disable) ✅
- [x] `history_enable_global(retention)` — Enable history for all databases
- [x] `history_enable_database(db_num, retention)` — Enable for specific database
- [x] `history_enable_key(key, retention)` — Enable for specific key
- [x] `history_disable_global()` — Disable global history
- [x] `history_disable_database(db_num)` — Disable for database
- [x] `history_disable_key(key)` — Disable for key
- [x] `is_history_enabled(key)` — Three-tier lookup (key > database > global)
- [x] Unit tests: Enable/disable at each level, three-tier priority
- [x] Integration tests: redis-cli HISTORY ENABLE/DISABLE commands

#### Session 17.3: History Recording & Retention ✅
- [x] `record_history()` — Capture state before write operations:
  - Check if history enabled
  - Increment version number
  - Serialize current data state to MessagePack
  - Insert into key_history table
  - Apply retention policy
- [x] `apply_retention_policy()` — Enforce retention rules:
  - Unlimited: keep all entries
  - Time-based: delete older than N milliseconds
  - Count-based: keep only last N versions
- [x] `get_or_create_key_id()` and `increment_version()` helpers
- [x] Unit tests: Recording, serialization, retention policies
- [x] Edge cases: Large values, type changes, deletions

#### Session 17.4: Query Methods ✅
- [x] `history_get(key, limit, since, until)` — Fetch history entries with optional filters
- [x] `history_get_at(key, timestamp)` — Time-travel query (get state at specific timestamp)
- [x] `history_list_keys(pattern)` — List keys with history enabled
- [x] `history_stats(key)` — Retrieve statistics (total entries, oldest/newest timestamp, storage size)
- [x] `history_clear_key(key, before)` — Manual cleanup per key
- [x] `history_prune(before_timestamp)` — Prune all history before timestamp
- [x] `query_to_history_entries()` helper for flexible query building
- [x] Unit tests: Query accuracy, time-travel correctness, edge cases
- [x] Integration tests: Query performance, large datasets

#### Session 17.5: Instrumentation (Write Operations) ✅
- [x] Add `record_history()` calls to core write commands:
  - **String:** SET (with drop conn pattern)
  - **Hash:** HSET (with drop conn pattern)
  - **List:** LPUSH, RPUSH (with async notification pattern)
  - **Key:** DEL (with drop conn/stmt pattern)
  - **Stream:** XADD (with async notification pattern)
- [x] Handle connection locking correctly (drop before record_history)
- [x] Preserve existing async notification patterns
- [x] Unit tests: Instrumentation for each data type
- [x] Integration tests: Multi-operation transactions

#### Session 17.6: Server Commands ✅
- [x] `cmd_history()` router in `src/server/mod.rs`
- [x] Subcommand handlers:
  - [x] `HISTORY ENABLE {GLOBAL|DATABASE db|KEY key} [RETENTION {TIME ms|COUNT n}]`
  - [x] `HISTORY DISABLE {GLOBAL|DATABASE db|KEY key}`
  - [x] `HISTORY GET key [LIMIT n] [SINCE timestamp] [UNTIL timestamp]`
  - [x] `HISTORY GETAT key timestamp` (time-travel query)
  - [x] `HISTORY LIST [PATTERN pattern]`
  - [x] `HISTORY CLEAR key [BEFORE timestamp]`
  - [x] `HISTORY STATS [KEY key]`
  - [x] `HISTORY PRUNE BEFORE timestamp`
- [x] RESP protocol formatting for all responses (Array, BulkString, Integer types)
- [x] Error handling (wrong arguments, invalid timestamps, non-existent keys)
- [x] Command parsing with argument validation before DB calls
- [x] Integration with handle_connection() dispatch in server loop

#### Session 17.7: Testing & Polish ✅
- [x] Unit tests: 20+ tests covering configuration, recording, querying, retention
- [x] Integration tests: 15+ tests with redis-cli
- [x] Lifetime issue resolution (query parameter lifetimes with match expressions)
- [x] Connection locking patterns verified (drop before record_history calls)
- [x] Error handling for all edge cases (missing keys, invalid arguments, bad timestamps)
- [x] MessagePack serialization verified with large values
- [x] Documentation: HISTORY commands documented in README
- [x] **Test:** 280+ lib tests + 131+ integration tests passing (all Session 17 tests)

---

### Session 18: Performance Testing & Benchmarking 🎯

**Goal:** Establish baseline performance metrics and optimize for 10,000+ QPS in embedded mode.

#### Session 18.1: Benchmark Infrastructure
- [ ] Create `benches/` directory with criterion-based benchmarks
- [ ] Benchmark suite for each data type:
  - **Strings:** SET/GET, INCR, APPEND (sequential + concurrent)
  - **Hashes:** HSET/HGET, HGETALL (varying sizes)
  - **Lists:** LPUSH/LPOP, LRANGE (sequential + concurrent)
  - **Sets:** SADD/SMEMBERS (varying cardinality)
  - **Sorted Sets:** ZADD/ZRANGE (varying scores)
  - **Streams:** XADD/XREAD (varying throughput)
- [ ] Mixed workload benchmark (80% reads, 20% writes)
- [ ] Concurrent access patterns (1, 4, 8, 16 threads)
- [ ] Baseline metrics recorded (latency, throughput, memory)
- [ ] **Target:** Establish current QPS baseline
- [ ] **Test:** Criterion reports generated, baseline documented

#### Session 18.2: Profiling & Analysis
- [ ] Use flamegraph/perf to identify hot paths:
  - SQLite transaction overhead
  - RESP parsing bottlenecks
  - Expiration checking during reads
  - List gap-based positioning logic
- [ ] Memory profiling (valgrind/heaptrack)
- [ ] Database file size analysis
- [ ] Query plan analysis (SQLite EXPLAIN)
- [ ] **Test:** Performance report with identified bottlenecks

#### Session 18.3: Optimization Passes
- [ ] **SQLite tuning:**
  - Journal mode analysis (WAL vs DELETE)
  - Page cache sizing
  - Pragma optimization (synchronous, cache_size)
  - Connection pooling for concurrent writes
- [ ] **Query optimization:**
  - Index effectiveness review
  - N+1 query detection
  - Batch operation optimization
- [ ] **Memory efficiency:**
  - RESP buffer reuse
  - String interning for common commands
  - Lazy value materialization
- [ ] **Concurrency improvements:**
  - Lock contention analysis
  - Arc/RwLock optimization
  - Connection-local caching
- [ ] Target: 5,000+ QPS minimum

#### Session 18.4: Benchmark Regression Prevention
- [ ] CI integration: Run benchmarks on each commit
- [ ] Track performance over time (git history)
- [ ] Alert on regression (>10% slowdown)
- [ ] Baseline comparison script
- [ ] Documentation: How to run benchmarks locally
- [ ] **Test:** CI pipeline configured, baseline established

#### Session 18.5: Advanced Benchmarks
- [ ] **Real-world scenarios:**
  - Cache hit/miss patterns
  - Expiration stress test (millions of expired keys)
  - Large value handling (MB-sized strings)
  - Deep nesting (many nested hashes/lists)
- [ ] **Server mode benchmarks:**
  - TCP connection overhead
  - Multi-client concurrency (10, 100, 1000 connections)
  - Pub/Sub message throughput
  - Transaction atomicity overhead
- [ ] **Comparison benchmarks:**
  - vs Redis (memory-only)
  - vs SQLite (raw)
  - vs Other SQLite-backed stores (redka, etc.)
- [ ] **Test:** Comparative analysis published

#### Session 18.6: Optimization Refinement & Documentation
- [ ] Fine-tune based on profiling results
- [ ] Implement fastest optimization wins
- [ ] Aiming for 10,000+ QPS in embedded mode
- [ ] Document performance characteristics:
  - Per-operation latency expectations
  - Throughput under concurrent load
  - Memory usage patterns
  - When to use embedded vs server mode
  - Optimization tips for users
- [ ] Update README with performance section
- [ ] Create performance tuning guide
- [ ] **Test:** 10,000+ QPS target achieved or documented rationale

#### Session 18.7: Continuous Performance Monitoring
- [ ] Set up performance dashboard (optional: GitHub Pages)
- [ ] Automated benchmark runs (nightly)
- [ ] Historical performance tracking
- [ ] Alert system for regressions
- [ ] Post-mortem analysis for any slowdowns
- [ ] **Test:** Monitoring system operational, baseline sustained

---

### Session 19: Python Bindings (pyo3)

**Goal:** Expose Redlite to Python via direct Rust bindings using PyO3.

#### Session 19.1: Project Setup & Core API
- [ ] Create `bindings/python/` directory structure
- [ ] Add `pyo3` dependency to workspace
- [ ] Configure `maturin` for PyPI packaging
- [ ] Implement `Db` class with basic methods:
  - `open(path)` / `open_memory()` constructors
  - `set(key, value, ttl=None)` - String operations
  - `get(key)` - Returns bytes or None
  - `delete(*keys)` - Delete one or more keys
  - `close()` - Explicit cleanup
- [ ] Python type hints via `pyi` stub file
- [ ] Unit tests: Python test suite using pytest
- [ ] **Test:** Basic operations working from Python

#### Session 19.2: Data Types & Commands
- [ ] Hash operations: `hset`, `hget`, `hgetall`, `hdel`, `hincrby`
- [ ] List operations: `lpush`, `rpush`, `lpop`, `rpop`, `lrange`
- [ ] Set operations: `sadd`, `srem`, `smembers`, `sismember`
- [ ] Sorted set operations: `zadd`, `zrem`, `zrange`, `zscore`
- [ ] Key management: `exists`, `expire`, `ttl`, `persist`, `keys`
- [ ] Python-friendly return types (dict for hashes, list for lists, set for sets)
- [ ] Unit tests: Coverage for all data types
- [ ] **Test:** All major command families working

#### Session 19.3: Advanced Features & Distribution
- [ ] Stream operations: `xadd`, `xread`, `xrange`, `xlen`
- [ ] Transaction support: Context manager for `MULTI/EXEC`
  ```python
  with db.transaction():
      db.set("key1", b"value1")
      db.set("key2", b"value2")
  ```
- [ ] Custom commands: `vacuum()`, `keyinfo(key)`
- [ ] Async support via `pyo3-asyncio` (optional)
- [ ] Build wheels for Linux/macOS/Windows (GitHub Actions)
- [ ] Package metadata: README, license, PyPI description
- [ ] Integration tests: Full test suite with redis-py comparison
- [ ] Publish to PyPI as `redlite`
- [ ] Documentation: Python usage examples in docs site
- [ ] **Test:** 50+ Python tests passing, wheels built for 3 platforms

---

### Session 20: Node.js/Bun Bindings (napi-rs)

**Goal:** Expose Redlite to JavaScript/TypeScript via NAPI-RS for Node.js and Bun.

#### Session 20.1: Project Setup & Core API
- [ ] Create `bindings/nodejs/` directory structure
- [ ] Add `napi-rs` dependencies to workspace
- [ ] Configure package.json with TypeScript declarations
- [ ] Implement `Db` class with basic methods:
  - `open(path)` / `openMemory()` - Async constructors
  - `set(key, value, options?)` - Promise-based
  - `get(key)` - Returns Buffer or null
  - `delete(...keys)` - Delete one or more keys
  - `close()` - Explicit cleanup
- [ ] TypeScript type definitions (auto-generated)
- [ ] Unit tests: Jest/Vitest test suite
- [ ] **Test:** Basic operations working from Node.js and Bun

#### Session 20.2: Data Types & Commands
- [ ] Hash operations: `hset`, `hget`, `hgetall`, `hdel`, `hincrby`
- [ ] List operations: `lpush`, `rpush`, `lpop`, `rpop`, `lrange`
- [ ] Set operations: `sadd`, `srem`, `smembers`, `sismember`
- [ ] Sorted set operations: `zadd`, `zrem`, `zrange`, `zscore`
- [ ] Key management: `exists`, `expire`, `ttl`, `persist`, `keys`
- [ ] JS-friendly return types (Object for hashes, Array for lists, Set for sets)
- [ ] Promise-based async API throughout
- [ ] Unit tests: Coverage for all data types
- [ ] **Test:** All major command families working

#### Session 20.3: Advanced Features & Distribution
- [ ] Stream operations: `xadd`, `xread`, `xrange`, `xlen`
- [ ] Transaction support: Fluent API
  ```typescript
  await db.transaction()
    .set("key1", Buffer.from("value1"))
    .set("key2", Buffer.from("value2"))
    .exec();
  ```
- [ ] Custom commands: `vacuum()`, `keyinfo(key)`
- [ ] Build for Linux/macOS/Windows (cross-platform natives)
- [ ] Package metadata: README, license, npm description
- [ ] Integration tests: Full test suite with ioredis comparison
- [ ] Publish to npm as `redlite`
- [ ] Test with both Node.js and Bun runtimes
- [ ] Documentation: JS/TS usage examples in docs site
- [ ] **Test:** 50+ JS tests passing, native modules for 3 platforms

---

### Session 21: C FFI Layer & Go Bindings

**Goal:** Create C FFI layer and Go bindings for embedded mode access.

#### Session 21.1: C FFI Layer
- [ ] Create `bindings/c/` directory with `src/lib.rs`
- [ ] Export core functions with `#[no_mangle]` and `extern "C"`:
  - `redlite_open(path)` → `*mut Db`
  - `redlite_set(db, key, value, len, ttl)` → `int`
  - `redlite_get(db, key, out_len)` → `*mut u8`
  - `redlite_delete(db, keys, count)` → `int`
  - `redlite_free(ptr)` - Free returned memory
  - `redlite_close(db)` - Close database
- [ ] Use `cbindgen` to generate `redlite.h` header
- [ ] Build shared library: `libredlite.so`/`.dylib`/`.dll`
- [ ] Memory safety: Clear ownership model for pointers
- [ ] Unit tests: C test suite using criterion or similar
- [ ] **Test:** C API working, no memory leaks (valgrind)

#### Session 21.2: Go Bindings (cgo)
- [ ] Create `bindings/go/` directory with Go module
- [ ] cgo wrapper around C FFI:
  ```go
  // #cgo LDFLAGS: -L. -lredlite
  // #include "redlite.h"
  import "C"
  ```
- [ ] Implement `Db` struct with methods:
  - `Open(path string)` - Constructor
  - `Set(key string, value []byte, ttl *time.Duration)` - String operations
  - `Get(key string)` - Returns []byte or nil
  - `Delete(keys ...string)` - Delete multiple keys
  - `Close()` - Cleanup
- [ ] Idiomatic Go error handling
- [ ] Unit tests: Go test suite
- [ ] **Test:** Basic operations working from Go

#### Session 21.3: Go Data Types & Distribution
- [ ] Hash operations: `HSet`, `HGet`, `HGetAll`, `HDel`, `HIncrBy`
- [ ] List operations: `LPush`, `RPush`, `LPop`, `RPop`, `LRange`
- [ ] Set operations: `SAdd`, `SRem`, `SMembers`, `SIsMember`
- [ ] Sorted set operations: `ZAdd`, `ZRem`, `ZRange`, `ZScore`
- [ ] Key management: `Exists`, `Expire`, `TTL`, `Persist`, `Keys`
- [ ] Stream operations: `XAdd`, `XRead`, `XRange`, `XLen`
- [ ] Custom commands: `Vacuum()`, `KeyInfo(key)`
- [ ] Go module publishing to pkg.go.dev
- [ ] Cross-platform builds (Linux, macOS, Windows)
- [ ] Integration tests: Full test suite with go-redis comparison
- [ ] Documentation: Go usage examples in docs site
- [ ] **Test:** 50+ Go tests passing, CGO bindings working on 3 platforms

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

### History Tracking & Time-Travel (Session 17) 🎯

**Status:** Planned for Session 17 with 7 detailed subsessions (17.1-17.7)

Track value changes per key with three-tier opt-in and time-travel queries:

```bash
# Enable history tracking at different levels
HISTORY ENABLE GLOBAL RETENTION TIME 2592000000         # All databases, 30 days
HISTORY ENABLE DATABASE 0 RETENTION COUNT 100           # Database 0, last 100 versions
HISTORY ENABLE KEY mykey RETENTION COUNT 50             # Specific key, last 50 versions

# Write operations automatically tracked (if history enabled)
SET mykey "v1"
SET mykey "v2"
SET mykey "v3"

# Query history
HISTORY GET mykey LIMIT 10 SINCE 1673000000000
→ Array of history entries with timestamps and operations

# Time-travel query (get state at specific timestamp)
HISTORY GETAT mykey 1673000000000
→ "v1"

# Manage history
HISTORY LIST PATTERN "my*"              # Keys with history
HISTORY STATS                           # Overall statistics
HISTORY CLEAR mykey BEFORE 1673000000000  # Manual cleanup
HISTORY PRUNE BEFORE 1673000000000      # Global cleanup
```

Schema (Session 17.1):
```sql
CREATE TABLE history_config (
    id INTEGER PRIMARY KEY,
    level TEXT NOT NULL CHECK(level IN ('global', 'database', 'key')),
    target TEXT NOT NULL,                -- '*' for global, '0-15' for db, 'db:key' for key
    enabled BOOLEAN NOT NULL DEFAULT 1,
    retention_type TEXT CHECK(retention_type IN ('unlimited', 'time', 'count')),
    retention_value INTEGER,
    created_at INTEGER NOT NULL,
    UNIQUE(level, target)
);

CREATE TABLE key_history (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    key_id INTEGER NOT NULL REFERENCES keys(id) ON DELETE CASCADE,
    db INTEGER NOT NULL,
    key TEXT NOT NULL,
    key_type INTEGER NOT NULL,           -- KeyType enum
    version_num INTEGER NOT NULL,
    operation TEXT NOT NULL,             -- 'SET', 'DEL', 'HSET', 'LPUSH', etc.
    timestamp_ms INTEGER NOT NULL,
    data_snapshot BLOB,                  -- MessagePack encoded current state
    expire_at INTEGER,                   -- Optional TTL
    UNIQUE(key_id, version_num)
);

CREATE INDEX idx_history_key_time ON key_history(key_id, timestamp_ms DESC);
CREATE INDEX idx_history_db_key_time ON key_history(db, key, timestamp_ms DESC);
```

**Features:**
- ✅ Three-tier opt-in: Global, per-database, per-key
- ✅ All write operations tracked (SET, HSET, DEL, LPUSH, etc.)
- ✅ MessagePack serialization for efficient storage
- ✅ Time-travel queries: `HISTORY GETAT key timestamp`
- ✅ Configurable retention: Time-based or count-based pruning
- ✅ Automatic retention enforcement on writes
- ✅ Manual cleanup: `HISTORY CLEAR`, `HISTORY PRUNE`
- ✅ Statistics: `HISTORY STATS`, `HISTORY LIST`

**Session Breakdown:**
- Session 17.1: Schema & Types
- Session 17.2: Configuration (Enable/Disable at 3 levels)
- Session 17.3: Recording & Retention Policies
- Session 17.4: Query Methods (including time-travel)
- Session 17.5: Instrumentation (integrate into all write commands)
- Session 17.6: Server Commands (HISTORY command family)
- Session 17.7: Testing & Polish (20+ unit tests, 15+ integration tests)

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

### Time-Travel Queries (Session 17) ✅

Query historical state at specific timestamps:

```bash
# After Session 17 implementation:
HISTORY GETAT mykey 1704067200000      # Value at timestamp
HISTORY GET mykey SINCE 1704000000000 UNTIL 1704100000000  # Changes in time range
```

**Implemented in Session 17.4** with configurable retention policies.

### History Retention & Compaction (Session 17) ✅

Automatic and manual retention:

```bash
# Automatic retention via configuration
HISTORY ENABLE KEY mykey RETENTION TIME 2592000000    # Keep 30 days
HISTORY ENABLE KEY mykey RETENTION COUNT 100          # Keep 100 versions

# Manual cleanup
HISTORY CLEAR mykey BEFORE 1704067200000
HISTORY PRUNE BEFORE 1704067200000
```

**Implemented in Session 17.3** with configurable time-based and count-based policies.

### History Replay & Reconstruction (Future)

Revert keys to past states:

```bash
HISTORY REPLAY mykey 1704067200000    # Restore key to state at timestamp
HISTORY DIFF mykey timestamp1 timestamp2  # Show changes between timestamps
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

### Language Bindings (Sessions 18-20) 🎯

**Status:** Planned for Sessions 18-20 with hybrid approach.

Expose Redlite to other languages using direct Rust bindings where possible, C FFI where needed.

**Implemented Languages:**
- ✅ **Python** (`redlite-py`) - Session 18 - Direct Rust via PyO3
- ✅ **Node.js/Bun** (`redlite-js`) - Session 19 - Direct Rust via NAPI-RS
- ✅ **Go** (`redlite-go`) - Session 20 - C FFI + cgo wrapper
- ✅ **C** (`libredlite`) - Session 20 - Base FFI layer

**Future Languages (V4+):**
- **Java** (`redlite-java`) - JNI wrapper
- **Ruby** (`redlite-rb`) - Native extensions
- **PHP** (`redlite-php`) - PECL extension
- **C#/.NET** (`redlite-net`) - P/Invoke wrapper

**Why Hybrid Approach?**

1. **Python/Node.js** - Direct Rust bindings (pyo3/napi-rs)
   - Cleaner API, better performance
   - No C FFI overhead
   - Native package distribution (wheels/npm)

2. **Go** - C FFI required
   - Go can't directly call Rust
   - cgo is standard for native libraries
   - Shared library distribution

**Implementation (Sessions 18-20):**

```python
# Python (Session 18) - pyo3
import redlite

db = redlite.open("mydata.db")
db.set("key", b"value")
value = db.get("key")
```

```javascript
// Node.js/Bun (Session 19) - napi-rs
import { open } from 'redlite';

const db = await open('mydata.db');
await db.set('key', Buffer.from('value'));
const value = await db.get('key');
```

```go
// Go (Session 20) - C FFI + cgo
import "github.com/russellromney/redlite-go"

db := redlite.Open("mydata.db")
db.Set("key", []byte("value"))
value := db.Get("key")
```

**Feature Parity (All Bindings):**
- ✅ All data types (strings, hashes, lists, sets, zsets, streams)
- ✅ All commands (GET, SET, HSET, LPUSH, XADD, etc.)
- ✅ Transactions (MULTI/EXEC)
- ✅ Custom commands (VACUUM, KEYINFO)
- ✅ History tracking (Session 17)
- ✅ Idiomatic error handling per language

**Distribution:**
- Python: PyPI (`pip install redlite`)
- Node.js: npm (`npm install redlite`)
- Go: Go modules (`go get github.com/russellromney/redlite-go`)
- C: Header + shared library

**Session Breakdown:**
- Session 18.1-18.7: Performance Testing & Benchmarking (criterion + flamegraph + optimization)
- Session 19.1-19.3: Python bindings (pyo3 + maturin + PyPI)
- Session 20.1-20.3: Node.js/Bun bindings (napi-rs + npm)
- Session 21.1-21.3: C FFI layer + Go bindings (cgo)

---

### Session 22: Redis Ecosystem Compatibility 🎯

**Goal:** Improve compatibility with Redis clients and ecosystem tools.

#### Session 22.1: Authentication ✅
- [x] `--password` CLI flag for server mode
- [x] AUTH command (single password and username+password formats)
- [x] Per-connection authentication state
- [x] NOAUTH error for unauthenticated commands
- [x] WRONGPASS error for incorrect passwords
- [x] All 134 tests passing

#### Session 22.2: Backend Options ✅
- [x] `--backend` flag (sqlite, turso)
- [x] `--storage` flag (file, memory)
- [x] Turso/libSQL backend (feature-gated)
- [x] Default port changed to 6379 (Redis standard)

#### Session 22.3: WATCH/UNWATCH (Optimistic Locking) ✅
- [x] Per-connection watched keys tracking (HashMap<String, u64> in ConnectionState)
- [x] WATCH key [key ...] - Mark keys for modification detection
- [x] UNWATCH - Clear all watched keys
- [x] EXEC returns nil if watched keys were modified
- [x] Key version tracking in database (version column + increment on write)
- [x] Unit tests passing
- [x] Integration tests - 5 basic tests (redis-cli based)
- [x] Server mode tests - 36 comprehensive tests covering:
  - Connection state management (8 tests)
  - Concurrent modification racing (10 tests)
  - Multi-key scenarios (7 tests)
  - Protocol compliance (6 tests)
  - Stress & performance (5 tests)
  - Integration scenarios (6 tests)
- [x] **Total: 41 comprehensive WATCH/UNWATCH tests, all passing**

Key implementation details:
- Schema: Added `version INTEGER NOT NULL DEFAULT 0` to keys table
- db.rs: Added get_version(key) method, all write ops increment version
- pubsub.rs: ConnectionState::Normal and Transaction now have watched_keys field
- server/mod.rs: WATCH/UNWATCH dispatch, cmd_multi preserves watched_keys
- execute_transaction checks versions before executing, returns null() on mismatch
- DISCARD keeps watched keys, EXEC always clears them

Test Coverage:
- Modification detection for: SET, DEL, INCR, LPUSH, HSET, type changes
- Read-only operations (GET) don't trigger watch
- Per-connection isolation verified
- 100+ concurrent clients stress tested
- 1000 watched keys per connection tested
- Edge cases: special chars, long names, 1MB+ values, DB isolation

#### Session 22.4: Additional Redis Commands
- [ ] CLIENT SETNAME/GETNAME - Connection naming
- [ ] CLIENT LIST - List connections
- [ ] LREM - Remove elements from list by value
- [ ] LINSERT - Insert before/after element
- [ ] SMOVE - Move member between sets
- [ ] *STORE commands (SDIFFSTORE, SINTERSTORE, SUNIONSTORE)

---

### Session 23: SQLite Ecosystem Integration 🎯

**Goal:** Leverage SQLite's unique capabilities for features Redis can't provide.

#### Session 23.1: Backup & Recovery
- [ ] BACKUP command - Hot backup to path (VACUUM INTO)
- [ ] CHECKPOINT command - Force WAL checkpoint
- [ ] RESTORE command - Restore from backup
- [ ] Point-in-time recovery documentation

#### Session 23.2: Litestream Integration
- [ ] Documentation for Litestream setup
- [ ] LITESTREAM STATUS command (if Litestream detected)
- [ ] S3/GCS/Azure replication examples
- [ ] Disaster recovery playbook

#### Session 23.3: Read Replicas
- [ ] `--readonly` flag for replica mode
- [ ] Connection to primary for writes
- [ ] WAL-based replication documentation
- [ ] Multi-region deployment guide

#### Session 23.4: Full-Text Search (FTS5)
- [ ] FT.CREATE index - Create FTS index on keys
- [ ] FT.SEARCH query - Full-text search
- [ ] FT.ADD key - Add key to index
- [ ] FT.DEL key - Remove from index
- [ ] Automatic index sync on writes

#### Session 23.5: Geospatial (R*Tree)
- [ ] GEOADD key longitude latitude member
- [ ] GEOPOS key member [member ...]
- [ ] GEODIST key member1 member2 [unit]
- [ ] GEORADIUS key longitude latitude radius unit
- [ ] GEOSEARCH key FROMMEMBER/FROMLONLAT ...

---

### Maybe (If Requested)

- Lua scripting (EVAL/EVALSHA) - Complex, low priority
- XAUTOCLAIM (auto-reassign stuck messages)
- Cluster mode (probably not - use LiteFS instead)
- ACL system (Redis 6+ access control)

