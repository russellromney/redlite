# Changelog

## Session 21: Deterministic Simulation Testing Foundation

### Added - redlite-dst Testing Framework
- Created `redlite-dst/` crate for deterministic simulation testing
- Integrated actual Redlite library (replaced in-memory mock)
- Implemented 7 comprehensive smoke tests:
  - `basic_set_get` - SET/GET roundtrip validation
  - `basic_incr_decr` - INCR/DECR monotonicity checks
  - `basic_list_ops` - LPUSH/RPUSH/LRANGE/LPOP verification
  - `basic_hash_ops` - HSET/HGET/HGETALL validation
  - `basic_set_ops` - SADD/SMEMBERS/SISMEMBER checks
  - `basic_sorted_set` - ZADD/ZRANGE/ZSCORE ordering verification
  - `basic_persistence` - File-backed database recovery testing
- All smoke tests: **7/7 passing** (34ms)

### Added - Regression Seed Management
- `redlite-dst seeds list` - Display all regression seeds
- `redlite-dst seeds add` - Add failing seeds with descriptions to permanent bank
- `redlite-dst seeds test` - Replay all regression seeds with property tests
- Seed file format: `SEED TEST_TYPE DESCRIPTION` in `tests/regression_seeds.txt`

### Added - Property-Based Testing
- 7 properties testing core Redis operations:
  - `set_get_roundtrip` - SET k v; GET k => v
  - `incr_is_monotonic` - INCR always increases
  - `list_order_preserved` - LPUSH/RPUSH preserve order
  - `hash_fields_unique` - Hash field updates work correctly
  - `sorted_set_ordering` - ZRANGE returns sorted results
  - `expire_removes_key` - TTL/expiration behavior
  - `crash_recovery_consistent` - Persistence verification
- All property tests: **70/70 passing** (223ms, 10 seeds × 7 properties)

### Infrastructure
- Added tempfile dependency for persistence testing
- Created `.gitignore` for redlite-dst target directory
- Updated README with testing framework section
- Updated ROADMAP marking Phase 5 partially complete

### Test Results Summary
```
✅ Smoke tests: 7/7 passed (34ms)
✅ Property tests: 70/70 passed (223ms)
✅ Regression seeds: 1/1 passed (80ms)
```

## Session 20: Monorepo & Multi-Language SDKs

### Changed - Monorepo Migration
- **BREAKING**: Reorganized into monorepo structure
  - Rust core moved to `crates/redlite/`
  - Python SDK moved to `bindings/python/`
  - TypeScript SDK added at `bindings/node/`
- Fixed Rust build: Disabled turso backend in server mode
- Updated all SDK build scripts to reference monorepo paths

### Added - TypeScript/Node SDK
- Created `@redlite/node` package with full feature parity
- Embedded mode with binary bundling (darwin-arm64, darwin-x86_64, linux-x86_64, win32-x86_64)
- Extends ioredis for full Redis compatibility
- FTS namespace implementation
- Works with both Node.js and Bun
- Vitest test suite with embedded mode support

### Added - Python SDK Enhancements
- Binary bundling infrastructure with platform-specific builds
- Updated build scripts for monorepo structure
- Maturin-ready for future PyO3 optimization
- 98 unit tests passing (including embedded mode)

### Added - Documentation System
- LLM-powered doc generation templates
- Language-agnostic pseudocode template (`docs/templates/sdk-guide.template.md`)
- Focus on use cases: "Embedded Redis + SQLite durability"
- Emphasizes CLI/desktop/serverless use cases over feature lists

### Infrastructure
- Unified GitHub Actions CI workflow (`monorepo-ci.yml`)
- Cross-platform binary builds (macOS, Linux, Windows)
- Automated SDK testing with real binaries
- Root-level Makefile for building all SDKs
- Migration guide (`MONOREPO_MIGRATION.md`)

### Roadmap Updates
- Async client for Python: Decided to skip for v1 (use run_in_executor)
- SDK priority: TypeScript ✅, WASM (Q1 2026), Go (Q2), Kotlin (Q2)
- Documentation approach: Template-based, LLM-assisted generation

## Sessions 1-23.2 (Complete)

### Benchmark Suite Enhancements
- File-backed database size measurement (db + WAL + shm files)
- History entry count and storage bytes tracking
- `get_history_count()` trait method for global history stats
- `bytes_per_history_entry` calculation in BenchmarkResult
- Enhanced `print_summary()` with history metrics output

### Session 23.2: FT.SEARCH Implementation
- `src/search.rs` query parser module for RediSearch syntax
- Query translation: AND/OR/NOT, phrases, prefix, field-scoped
- Numeric range queries (@field:[min max])
- Tag exact match queries (@field:{tag1|tag2})
- FT.SEARCH with NOCONTENT, VERBATIM, WITHSCORES, LIMIT, SORTBY, RETURN
- In-memory text matching fallback for unindexed documents
- 26 new tests (14 query parser + 12 ft_search integration)

### Session 23.1: RediSearch Index Management
- FT.CREATE, FT.DROPINDEX, FT._LIST, FT.INFO, FT.ALTER
- FT.ALIASADD/DEL/UPDATE for index aliases
- FT.SYNUPDATE/DUMP for synonym groups
- FT.SUGADD/GET/DEL/LEN for autocomplete suggestions
- Schema support: TEXT, NUMERIC, TAG field types
- 22 comprehensive unit tests

### Session 23: Per-Connection State & CLIENT Commands
- CLIENT LIST with TYPE/ID filters
- CLIENT INFO, CLIENT KILL, CLIENT PAUSE/UNPAUSE
- Connection lifecycle management with ConnectionPool

### Session 22: Redis Ecosystem Compatibility
- `--password` flag and AUTH command
- `--backend` (sqlite/turso), `--storage` (file/memory) flags
- WATCH/UNWATCH optimistic locking with version tracking
- CLIENT SETNAME/GETNAME/LIST/ID
- LREM, LINSERT list operations
- SMOVE, SDIFFSTORE, SINTERSTORE, SUNIONSTORE set operations

### Session 18: Performance & Cache Configuration
- `redlite-bench` benchmarking suite (35+ scenarios)
- `--cache` flag for SQLite page cache tuning
- `Db::open_with_cache()` and `db.set_cache_mb()` API

### Session 17: History Tracking & Time-Travel
- Three-tier opt-in (global, database, key)
- HISTORY ENABLE/DISABLE/GET/GETAT/STATS/CLEAR/PRUNE/LIST
- MessagePack serialization for efficient storage
- Configurable retention (time-based, count-based)

### Session 16: Transactions
- MULTI/EXEC/DISCARD command batching
- WATCH/UNWATCH optimistic locking
- Command queueing with validation

### Session 15: Blocking & Pub/Sub (Server Mode)
- BLPOP, BRPOP blocking list operations
- XREAD BLOCK, XREADGROUP BLOCK stream operations
- SUBSCRIBE/UNSUBSCRIBE/PUBLISH/PSUBSCRIBE/PUNSUBSCRIBE
- Tokio broadcast channels for notifications

### Session 14: Stream Consumer Groups
- XGROUP CREATE/DESTROY/SETID/CREATECONSUMER/DELCONSUMER
- XREADGROUP with consumer tracking
- XACK, XPENDING, XCLAIM
- XINFO GROUPS/CONSUMERS

### Session 13: Streams
- XADD, XLEN, XRANGE, XREVRANGE, XREAD
- XTRIM, XDEL, XINFO STREAM
- MessagePack field encoding
- Entry ID format: `{timestamp}-{seq}`

### Session 11-12: Custom Commands & Polish
- VACUUM (delete expired + SQLite VACUUM)
- KEYINFO (type, ttl, created_at, updated_at)
- AUTOVACUUM ON/OFF/INTERVAL
- Error message Redis compatibility

### Session 10: Server Operations
- SELECT (multiple databases 0-15)
- INFO, DBSIZE, FLUSHDB
- Per-connection database isolation

### Session 9: Sorted Sets
- ZADD, ZREM, ZSCORE, ZRANK, ZREVRANK
- ZRANGE, ZREVRANGE, ZRANGEBYSCORE
- ZCOUNT, ZCARD, ZINCRBY
- ZREMRANGEBYRANK, ZREMRANGEBYSCORE

### Session 8: Sets
- SADD, SREM, SMEMBERS, SISMEMBER, SCARD
- SPOP, SRANDMEMBER (with count)
- SDIFF, SINTER, SUNION

### Session 7: Lists
- LPUSH, RPUSH, LPOP, RPOP (with count)
- LLEN, LRANGE, LINDEX, LSET, LTRIM
- Integer gap positioning for O(1) operations

### Session 6: Hashes
- HSET, HGET, HMGET, HGETALL
- HDEL, HEXISTS, HKEYS, HVALS, HLEN
- HINCRBY, HINCRBYFLOAT, HSETNX

### Session 3: String Operations
- INCR, DECR, INCRBY, DECRBY, INCRBYFLOAT
- MGET, MSET, APPEND, STRLEN
- GETRANGE, SETRANGE

### Session 2: Key Management
- DEL, EXISTS, TYPE
- KEYS (glob pattern), SCAN (with MATCH, COUNT)
- TTL, PTTL, EXPIRE, PERSIST

### Session 1: Foundation
- GET, SET (with EX/PX/NX/XX)
- RESP protocol parser
- TCP server mode
- Lazy expiration
- SQLite schema with WAL mode
