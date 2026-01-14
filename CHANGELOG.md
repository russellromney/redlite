# Changelog

## Session 25: Geospatial Commands (R*Tree)

### Added - GEO* Command Family
- **GEOADD** - Add members with longitude/latitude to a geo set
  - Supports NX (only add new), XX (only update), CH (return changed count) flags
  - Coordinate validation: lon -180 to 180, lat -85.05112878 to 85.05112878
- **GEOPOS** - Get coordinates (longitude, latitude) for members
- **GEODIST** - Calculate distance between two members
  - Supports units: M (meters), KM (kilometers), MI (miles), FT (feet)
- **GEOHASH** - Get 11-character geohash strings for members
- **GEOSEARCH** - Search for members within radius or bounding box
  - FROMMEMBER / FROMLONLAT center options
  - BYRADIUS / BYBOX shape options
  - ASC/DESC sorting, COUNT limit, ANY flag
  - WITHCOORD, WITHDIST, WITHHASH response options
- **GEOSEARCHSTORE** - Store GEOSEARCH results as sorted set
  - STOREDIST option to store distances instead of geohashes

### Implementation Details
- Uses SQLite's built-in R*Tree extension for efficient spatial indexing
- Haversine formula for accurate great-circle distance calculations
- Base32 geohash encoding (11 chars = ~0.6mm precision)
- Bounding box pre-filtering for radius queries

### Feature Flag
- `geo` feature flag enables geo commands (R*Tree is built into SQLite)
- Included in `full` feature: `--features full` or `--features geo`

### Files Modified
- `src/schema_geo.sql` - New schema for geo_data + geo_rtree tables
- `src/types.rs` - Added GeoUnit, GeoMember, GeoSearchOptions types
- `src/db.rs` - Added geo* methods + helper functions (haversine, encode_geohash, bounding_box)
- `src/server/mod.rs` - Added cmd_geo* server handlers
- `Cargo.toml` - Added `geo` and `full` features

### Test Results
- ✅ 17 new geo-specific tests
- ✅ **Total: 469 unit tests + 4 doctests = 473 tests passing** with `--features geo`

## Session 24: Redis 8 Vector Commands

### Added - V* Command Family (Redis 8 Compatible)
- **VADD** - Add vector elements to a set with embeddings
- **VREM** - Remove vector elements from a set
- **VCARD** - Get cardinality (number of elements) in vector set
- **VEXISTS** - Check if element exists in vector set
- **VSIM** - KNN similarity search within a vector set
- **VSIMBATCH** - Batch similarity search across multiple vector sets
- **VGET** - Get embedding for specific element
- **VGETALL** - Get all elements and embeddings in a set
- **VGETATTRIBUTES** - Get attributes for elements
- **VSETATTRIBUTES** - Set attributes for elements
- **VDELATTRIBUTES** - Delete attributes from elements
- **VDIM** - Get dimensions of vectors in a set

### Schema Migration
- Migrated from old `vector_settings`/`vectors` tables to Redis 8-compatible `vector_sets` table
- New schema supports element-based storage with optional attributes
- Auto-detects vector dimensions from first element
- Supports quantization types: NOQUANT, Q8, BF16

### Implementation
- Vector DB methods: `db.rs:8301-8710` (410 lines)
- V* command handlers: `server/mod.rs:4835-5329` (495 lines)
- Vector types: `types.rs:785-858` (74 lines)
- Commands registered in dispatcher at lines 504-513 and 6893-6902

### Fixed
- PRAGMA execution in `open_with_cache()` and `set_cache_mb()`
- Changed `execute()` to `execute_batch()` to avoid `ExecuteReturnedResults` error
- Fixed failing doctest at `db.rs:201` for `Db::open_with_cache`

### Test Results
- ✅ All 487 unit tests passing
- ✅ All 4 doctests passing
- ✅ **Total: 491/491 tests passing** with `--features vectors`

## Session 22: Complete DST Command Implementation

### Added - ORACLE Command (Redis Comparison)
- Full Redis compatibility testing via `redlite-dst oracle`
- Compares Redlite against real Redis instance for 5 data types:
  - **Strings**: SET, GET, INCR, APPEND
  - **Lists**: LPUSH, RPUSH, LPOP, RPOP, LRANGE
  - **Hashes**: HSET, HGET, HDEL, HGETALL
  - **Sets**: SADD, SREM, SISMEMBER, SMEMBERS
  - **Sorted Sets**: ZADD, ZSCORE, ZRANGE
- Reports divergence count and compatibility percentage
- Requires Redis running: `docker run -d -p 6379:6379 redis`

### Added - SIMULATE Command (Deterministic Simulation)
- Seed-reproducible simulation testing via `redlite-dst simulate`
- Three scenarios per seed:
  - **concurrent_operations**: Virtual connections with deterministic interleaving
  - **crash_recovery**: Write data, simulate crash (drop client), verify recovery
  - **connection_storm**: Rapid open/close cycles to test connection churn
- Uses ChaCha8Rng for cross-platform reproducibility
- Tracks expected state for verification

### Added - CHAOS Command (Fault Injection)
- Fault injection testing via `redlite-dst chaos`
- Four fault scenarios:
  - **crash_mid_write**: Verify data survives simulated crashes
  - **corrupt_read**: Test graceful handling of read errors
  - **disk_full**: Verify database remains usable under write pressure
  - **slow_write**: Test operation completion under delays

### Added - STRESS Command (Load Testing)
- Concurrent load testing via `redlite-dst stress`
- Spawns multiple tokio tasks for parallel operations
- Metrics: throughput (ops/sec), latency percentiles (p50, p99)
- Memory monitoring via sysinfo crate

### Added - FUZZ Command (In-Process Fuzzing)
- Random input testing via `redlite-dst fuzz`
- Three fuzz targets:
  - **resp_parser**: Random RESP protocol data
  - **query_parser**: Random FT.SEARCH query syntax
  - **command_handler**: Random Redis commands
- Catches panics, reports crash seeds for reproduction
- Seed-based reproducibility for crash replay

### Added - SOAK Command (Stability Testing)
- Long-running stability testing via `redlite-dst soak`
- Memory growth monitoring with leak detection warnings
- Throughput stability analysis (coefficient of variation)
- Background operation generator with mixed workload

### Dependencies Added
- `redis = "0.27"` for oracle testing
- `sysinfo = "0.32"` for memory monitoring
- `parking_lot = "0.12"` for synchronization

### Test Results Summary
```
✅ Smoke tests: 7/7 passed
✅ Property tests: 70/70 passed (10 seeds × 7 properties)
✅ All DST commands implemented with real Redlite library
```

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
