# Roadmap

See [CHANGELOG.md](./CHANGELOG.md) for completed features.

## In Progress

### Sessions 23-24: Search & Vectors Implementation

**See [IMPLEMENTATION_PLAN.md](./IMPLEMENTATION_PLAN.md) for full details.**

Currently implementing RediSearch-compatible FT.* and Redis 8-compatible V* commands.

#### Phase 1: RediSearch Core (Session 23.1) - COMPLETE
- [x] Create `src/schema_ft.sql` with RediSearch tables
- [x] Update db.rs migrations to include schema_ft.sql
- [x] Add FtIndex, FtField types to types.rs
- [x] Implement FT.CREATE in db.rs
- [x] Implement FT.DROPINDEX, FT._LIST, FT.INFO in db.rs
- [x] Implement FT.ALTER in db.rs
- [x] Implement FT.ALIASADD/DEL/UPDATE, FT.SYNUPDATE/DUMP, FT.SUGADD/GET/DEL/LEN
- [x] Add FT.* command routing in server/mod.rs
- [x] Add comprehensive unit tests (22 tests for FT.* db methods)

#### Phase 2: RediSearch Search (Session 23.2-23.4) - COMPLETE
- [x] Create `src/search.rs` query parser module
- [x] Implement RediSearch -> FTS5 query translation (AND/OR/NOT, phrase, prefix, field-scoped)
- [x] Implement FT.SEARCH with core options (NOCONTENT, VERBATIM, WITHSCORES, LIMIT, SORTBY, RETURN)
- [x] Support numeric range queries (@field:[min max])
- [x] Support tag exact match queries (@field:{tag1|tag2})
- [x] Add HIGHLIGHT, SUMMARIZE support
- [x] Implement FT.EXPLAIN and FT.PROFILE (server layer)
- [x] Auto-index documents into FTS5 on HSET
- [x] Auto-unindex documents on DEL
- [x] **Use actual FTS5 MATCH queries with BM25 scoring** (Session 23.4)
- [x] Fix NOT operator FTS5 syntax (A NOT B instead of A AND NOT B)
- [x] Add 50 FT.* unit tests (was 26, now comprehensive)

#### Phase 3: RediSearch Aggregations (Session 23.3)
- [ ] Implement FT.AGGREGATE with LOAD, GROUPBY, REDUCE, SORTBY, APPLY, FILTER, LIMIT

#### Phase 4: Redis 8 Vectors with sqlite-vec (Session 24) - COMPLETE
- [x] Replace schema_vectors.sql with Redis 8-compatible vector_sets schema
- [x] Remove old redlite-native vector code (migrated to vector_sets table)
- [x] Add sqlite-vec extension loading via auto_extension
- [x] Implement VADD (add vector elements with embeddings)
- [x] Implement VSIM (K-NN similarity search using sqlite-vec)
- [x] Implement VSIMBATCH (batch similarity search across sets)
- [x] Implement VREM, VCARD, VEXISTS, VDIM
- [x] Implement VGET, VGETALL, VGETATTRIBUTES
- [x] Implement VSETATTRIBUTES, VDELATTRIBUTES
- [x] All 491 tests passing (487 unit + 4 doctests)

---

### Sessions 19-21: Language SDKs

**Strategy: Thin wrappers around existing Redis clients**

For server mode, existing Redis clients (redis-py, ioredis, go-redis) work as-is via RESP protocol.
For embedded mode, wrap existing clients + embed redlite via local socket or direct FFI.

**Python (Session 19)** — `redlite-py` wrapping redis-py
```python
from redlite import Redlite

# Server mode (uses redis-py under the hood)
db = Redlite.connect("localhost:6379")

# Embedded mode (starts internal server, no network)
db = Redlite.open("mydata.db")  # or :memory:

# All redis-py methods work via delegation
db.set("key", "value")
db.hset("user:1", mapping={"name": "Alice"})

# Redlite-specific namespaces for extra features
db.fts.enable(pattern="article:*")
db.fts.search("hello world", limit=10)
db.history.get("mykey", version=3)
db.geo.search("locations", lat=40.7, lon=-74.0, radius=10, unit="km")
```

**Node.js/Bun (Session 20)** — `redlite` wrapping ioredis
```typescript
import { Redlite } from 'redlite';

// Server or embedded
const db = await Redlite.open('mydata.db');
await db.set('key', 'value');
await db.fts.search('hello world');
```

**Go (Session 21)** — `redlite-go` wrapping go-redis
```go
import "github.com/russellromney/redlite-go"

db := redlite.Open("mydata.db")
db.Set(ctx, "key", "value", 0)
db.FTS.Search(ctx, "hello world")
```

**Implementation approach:**
1. Embed redlite binary/library
2. Start internal Unix socket server (or use FFI for hot path)
3. Wrap existing Redis client pointing to internal socket
4. Add namespace classes for redlite-specific commands (FTS, History, Vector, Geo)
5. Delegate all standard Redis methods to underlying client

## Planned

### Session 23: Full-Text Search (RediSearch compatible)

SQLite FTS5 backend with dual API: redlite-native (simple) + RediSearch-compatible (FT.*).
**Included by default** — uses SQLite's built-in FTS5 (no extra dependencies).

**Redlite-native API (already implemented):**
```bash
FTS ENABLE GLOBAL|DATABASE n|PATTERN pat|KEY key   # Four-tier opt-in
FTS DISABLE GLOBAL|DATABASE n|PATTERN pat|KEY key
FTS SEARCH "query" [LIMIT n] [HIGHLIGHT]           # Search with BM25
FTS REINDEX key                                     # Force re-index
FTS INFO                                            # Stats
```

**RediSearch-compatible API (to implement):**

*Index Management:*
```bash
FT.CREATE index ON HASH|JSON PREFIX n prefix... SCHEMA field TEXT|NUMERIC|TAG|GEO [SORTABLE] ...
FT.ALTER index SCHEMA ADD field type
FT.DROPINDEX index [DD]
FT.INFO index
FT._LIST
FT.ALIASADD alias index
FT.ALIASDEL alias
FT.ALIASUPDATE alias index
```

*Search:*
```bash
FT.SEARCH index "query" [NOCONTENT] [VERBATIM] [NOSTOPWORDS]
    [WITHSCORES] [WITHSORTKEYS] [FILTER field min max]
    [GEOFILTER field lon lat radius M|KM|MI|FT]
    [INKEYS n key...] [INFIELDS n field...]
    [RETURN n field [AS alias]...] [SUMMARIZE ...] [HIGHLIGHT ...]
    [SLOP n] [INORDER] [LANGUAGE lang] [SORTBY field ASC|DESC]
    [LIMIT offset num] [TIMEOUT ms] [PARAMS n name value...]
FT.EXPLAIN index query                              # Show query parse tree
FT.PROFILE index SEARCH|AGGREGATE QUERY query       # Run with timing stats
```

*Aggregations:*
```bash
FT.AGGREGATE index query
    [LOAD n field...]
    [GROUPBY n field... REDUCE func nargs arg... [AS name]...]
    [SORTBY n field ASC|DESC...]
    [APPLY expr AS alias]
    [FILTER expr]
    [LIMIT offset num]
    [TIMEOUT ms]
```

*REDUCE functions:* COUNT, COUNT_DISTINCT, SUM, AVG, MIN, MAX, TOLIST, FIRST_VALUE, STDDEV, QUANTILE, RANDOM_SAMPLE

*Suggestions (uses FTS5 vocab table):*
```bash
FT.SUGADD key string score [PAYLOAD payload]
FT.SUGGET key prefix [FUZZY] [WITHSCORES] [WITHPAYLOADS] [MAX n]
FT.SUGDEL key string
FT.SUGLEN key
```

*Synonyms:*
```bash
FT.SYNUPDATE index group_id term...
FT.SYNDUMP index
```

**Query Syntax (converted to FTS5 MATCH):**
- `word1 word2` → AND
- `word1 | word2` → OR
- `-word` → NOT
- `"exact phrase"` → phrase match
- `prefix*` → prefix search
- `@field:term` → field-scoped
- `@field:[min max]` → numeric range
- `@field:{tag1|tag2}` → TAG exact match
- `~word` → optional (boost score if present)

**Field Types:**
| Type | SQLite Mapping |
|------|----------------|
| TEXT | FTS5 column with stemming |
| NUMERIC | Regular column, range queries via WHERE |
| TAG | Exact match (no tokenization) |
| GEO | R*Tree integration (Session 25) |
| VECTOR | sqlite-vector (Session 24) |

**Implementation notes:**
- FT.CREATE creates index metadata + enables FTS for matching prefixes
- FT.SEARCH parses RediSearch query syntax, converts to FTS5 MATCH
- FT.AGGREGATE maps to SQL GROUP BY with aggregate functions
- APPLY expressions parsed and converted to SQL expressions
- Field-level indexing: index hash fields separately (not just string values)
- Existing FTS commands continue to work unchanged
- Timeout via `sqlite3_progress_handler` (works for reads and writes)

### Session 24: Vector Search (Redis 8 compatible)

Redis 8 introduced vector sets as a native data type. Full V* command set for redis-py compatibility.
**Optional** — requires `--features vectors` (adds ~500KB for sqlite-vector).

**Commands:**
```bash
VADD key (FP32 blob | VALUES n v1 v2...) element [REDUCE dim] [NOQUANT|Q8|BIN] [EF n] [SETATTR json] [M n]
VSIM key (ELE element | FP32 blob | VALUES n v1...) [WITHSCORES] [WITHATTRIBS] [COUNT n] [FILTER expr]
VREM key element
VCARD key                    # Count elements
VDIM key                     # Get dimensions
VINFO key                    # Index metadata
VEMB key element [RAW]       # Get element's vector
VGETATTR key element         # Get JSON attributes
VSETATTR key element json    # Set JSON attributes
VRANDMEMBER key [count]      # Random sampling
```

**Implementation:**
- Backend: [sqlite-vector](https://github.com/sqliteai/sqlite-vector) for SIMD-accelerated distance
- Storage: Vectors as BLOBs (no virtual tables)
- Quantization: int8 (Q8 default), binary (BIN), or full precision (NOQUANT)
- Metrics: L2 (Euclidean), Cosine, Inner Product
- FILTER expressions parsed same as FT.AGGREGATE APPLY

**Schema (`src/schema_vectors.sql`):**
```sql
CREATE TABLE IF NOT EXISTS vector_sets (
    id INTEGER PRIMARY KEY,
    key_id INTEGER NOT NULL REFERENCES keys(id) ON DELETE CASCADE,
    element TEXT NOT NULL,
    embedding BLOB NOT NULL,
    quantization TEXT DEFAULT 'Q8',
    attributes TEXT,                -- JSON for FILTER/VGETATTR
    UNIQUE(key_id, element)
);
CREATE INDEX IF NOT EXISTS idx_vector_sets_key ON vector_sets(key_id);
```

### Session 25: Geospatial (R*Tree)

Redis-compatible geo commands using SQLite's built-in R*Tree extension.
**Included by default** — R*Tree is built into standard SQLite.

**Commands:**
```bash
GEOADD key [NX|XX] [CH] longitude latitude member [lon lat member ...]
GEOPOS key member [member ...]
GEODIST key member1 member2 [M|KM|MI|FT]
GEOHASH key member [member ...]
GEOSEARCH key <FROMMEMBER member | FROMLONLAT lon lat>
              <BYRADIUS radius M|KM|MI|FT | BYBOX width height M|KM|MI|FT>
              [ASC|DESC] [COUNT n [ANY]] [WITHCOORD] [WITHDIST] [WITHHASH]
GEOSEARCHSTORE dest src <FROMMEMBER|FROMLONLAT> <BYRADIUS|BYBOX> [STOREDIST]
```

**Implementation:**
- R*Tree virtual table for bounding-box pre-filtering
- Haversine formula for precise distance calculations
- Geohash encoding for GEOHASH command
- Results stored as sorted sets (for GEOSEARCHSTORE compatibility)

**Schema (`src/schema_geo.sql`):**
```sql
CREATE TABLE IF NOT EXISTS geo_data (
    id INTEGER PRIMARY KEY,
    key_id INTEGER NOT NULL REFERENCES keys(id) ON DELETE CASCADE,
    member TEXT NOT NULL,
    longitude REAL NOT NULL,
    latitude REAL NOT NULL,
    geohash TEXT,                   -- Pre-computed 11-char geohash
    UNIQUE(key_id, member)
);
CREATE INDEX IF NOT EXISTS idx_geo_data_key ON geo_data(key_id);

CREATE VIRTUAL TABLE IF NOT EXISTS geo_rtree USING rtree(
    id, minLon, maxLon, minLat, maxLat
);
```

**Query pattern:**
1. Compute bounding box from center + radius
2. Query R*Tree for candidates
3. Apply Haversine for precise distance filtering
4. Sort by distance, apply COUNT limit

**Distance units:** M (default), KM, MI, FT

**Coordinate limits:** Longitude -180 to 180, Latitude -85.05112878 to 85.05112878

**GEOSHAPE (optional `--features geoshape`):**

For polygon queries, enable Geopoly extension:
```bash
# In FT.CREATE schema
FT.CREATE idx ON HASH SCHEMA location GEOSHAPE

# Polygon queries in FT.SEARCH
FT.SEARCH idx "@location:[WITHIN $poly]" PARAMS 2 poly "POLYGON((...))"
FT.SEARCH idx "@location:[CONTAINS $point]" PARAMS 4 point "POINT(lon lat)"
```

Uses SQLite Geopoly for polygon contains/intersects operations. R*Tree handles MBR filtering, Geopoly does exact polygon math.

### Session 26: Additional Commands

- GETEX, GETDEL, SETEX, PSETEX
- LPUSHX, RPUSHX, LPOS, LMOVE
- BITCOUNT, BITFIELD, BITOP, SETBIT, GETBIT
- RENAME, RENAMENX
- HSCAN, SSCAN, ZSCAN
- ZINTERSTORE, ZUNIONSTORE

### Session 27: Battle Testing (Deterministic Simulation)

**Goal**: Make redlite Jepsen-proof before public release.

**See [BATTLE_TESTING.md](./BATTLE_TESTING.md) for full details.**

Inspired by [sled](https://sled.rs/simulation.html), [TigerBeetle VOPR](https://github.com/tigerbeetle/tigerbeetle/blob/main/docs/internals/vopr.md), and [MadSim](https://github.com/madsim-rs/madsim).

#### Phase 1: Property-Based Testing + Fuzzing (Session 27.1)
- [ ] Add `proptest`, `arbitrary`, `libfuzzer-sys` dependencies
- [ ] Create `tests/properties.rs`
- [ ] Properties: `set_get_roundtrip`, `incr_atomic`, `list_ordering`, `set_uniqueness`
- [ ] Properties: `zset_score_ordering`, `hash_field_roundtrip`, `stream_id_monotonic`
- [ ] Properties: `expire_respected`, `watch_conflict_aborts`, `multi_exec_atomic`
- [ ] Create `fuzz/` targets for RESP parser and FT.SEARCH query parser
- [ ] Create `tests/regression_seeds.txt` — permanent seed bank for found bugs
- [ ] Verify: `PROPTEST_CASES=10000 cargo test properties`
- [ ] Verify: `cargo +nightly fuzz run resp_parser` (10 min no crash)
- [ ] Verify: `cargo +nightly miri test` (no UB in unsafe blocks)

#### Phase 2: Redis Oracle (Session 27.2)
- [ ] Add `redis` crate as dev dependency
- [ ] Create `tests/oracle.rs`
- [ ] Test groups: Strings, Lists, Hashes, Sets, Sorted Sets, Keys
- [ ] Assert identical results for identical operation sequences
- [ ] Verify: `docker run -d redis && cargo test oracle`

#### Phase 3: MadSim Integration (Session 27.3)
- [ ] Add `madsim`, `madsim-tokio` dependencies (cfg-gated)
- [ ] Create `tests/simulation.rs`
- [ ] Tests: `concurrent_operations`, `crash_recovery`, `connection_storm`, `pubsub_delivery`
- [ ] Seed-based reproducibility for all failures
- [ ] Verify: `RUSTFLAGS="--cfg madsim" cargo test simulation`

#### Phase 4: Storage Fault Injection (Session 27.4)
- [ ] Create `src/storage.rs` with `StorageBackend` trait
- [ ] Implement `SqliteStorage` (production) and `FaultStorage<S>` (testing)
- [ ] Faults: `DiskFull`, `CorruptedRead`, `SlowWrite`, `RandomFailure`
- [ ] Minimal refactor to `db.rs` to use trait

#### Phase 5: redlite-dst Project (Session 27.5) - ✅ COMPLETE
- [x] Create `redlite-dst/` crate — standalone DST suite (like redlite-bench)
- [x] Wire up actual redlite library (replaced in-memory mock)
- [x] Implement 7 smoke tests with real operation verification
- [x] Implement seed management: `seeds list`, `seeds add`, `seeds test`
- [x] All property tests working with real Redlite (70/70 passed)
- [x] CLI commands: `oracle`, `simulate`, `chaos`, `stress`, `fuzz`, `soak`
  - **ORACLE**: Redis comparison testing (5 data types: strings, lists, hashes, sets, sorted_sets)
  - **SIMULATE**: Deterministic simulation (concurrent_operations, crash_recovery, connection_storm)
  - **CHAOS**: Fault injection (crash_mid_write, corrupt_read, disk_full, slow_write)
  - **STRESS**: Concurrent load testing with throughput/latency metrics
  - **FUZZ**: In-process fuzzing (resp_parser, query_parser, command_handler targets)
  - **SOAK**: Long-running stability testing with memory leak detection
- [x] All using real tokio with actual Redlite library (no mocks)
- [x] Seed-based reproducibility with ChaCha8Rng
- [ ] `cloud` command for fly.io parallel execution (placeholder)
- [ ] Spec-driven scenarios in `spec/scenarios.yaml`
- [ ] JSON + Markdown report output

#### Phase 6: Soak Testing + Extras (Session 27.6)
- [ ] `redlite-dst soak --duration 24h` — long-running stability test
- [ ] Monitor: RSS memory, open FDs, disk usage over time
- [ ] Fail if memory grows unbounded (leak detection)
- [ ] `make sanitize` — run with AddressSanitizer + ThreadSanitizer
- [ ] `make coverage` — generate coverage report with cargo-llvm-cov

#### Phase 7: Fly.io Cloud Testing (Session 27.7)
- [ ] `redlite-dst cloud --seeds 1M --machines 10`
- [ ] Add `Dockerfile`, `fly.toml` for ephemeral machines
- [ ] Parallel seed ranges across machines
- [ ] Aggregate results from all machines
- [ ] Cost target: ~$0.03 per 100K seeds

#### Success Criteria (Pre-HN Launch)
- [ ] 100K+ seeds pass property tests
- [ ] Zero divergences from Redis oracle (common commands)
- [ ] Crash recovery verified with fault injection
- [ ] 1M key scale tested without OOM
- [ ] 1000 connection scale without deadlock
- [ ] 24h soak test with stable memory (no leaks)
- [ ] 10min fuzz with no crashes (RESP + query parser)

### Future

- In-memory mode with periodic snapshots (like Redis RDB)
- HISTORY REPLAY/DIFF for state reconstruction
- Background expiration daemon

## Maybe

- Lua scripting (EVAL/EVALSHA)
- XAUTOCLAIM
- ACL system
- Nightly CI for battle tests (`.github/workflows/battle-test.yml`, 1M seeds)

## Not Planned

- Cluster mode — Use [walsync](https://github.com/russellromney/walsync) for replication
- Sentinel
- Redis Modules

## Testing Plan: Search & Vector Features

### Current Coverage (50 tests)
Basic functionality is covered. Need comprehensive edge case and integration testing.

### Phase 1: FTS5 Core Tests (Priority: HIGH)

**Query Parser Tests (~25 tests)**
- [ ] Empty query handling
- [ ] Single term, multiple terms
- [ ] All operators: AND (implicit), OR (`|`), NOT (`-`, `!`)
- [ ] Operator precedence: `a | b c` vs `(a | b) c`
- [ ] Nested parentheses: `((a | b) c) | d`
- [ ] Phrase with special chars: `"hello, world!"`, `"test's"`
- [ ] Escaped quotes in phrases: `"say \"hello\""`
- [ ] Prefix with short stems: `a*`, `ab*`, `abc*`
- [ ] Field-scoped with all operators: `@title:(a | b) -c`
- [ ] Numeric ranges: edge cases `[0 0]`, `[-inf +inf]`, `[(0 (0]`
- [ ] Tag queries: empty tags, special chars in tags
- [ ] Mixed query: `@title:hello @price:[10 100] @category:{books}`
- [ ] Unicode in queries: Japanese, Arabic, emoji
- [ ] Very long queries (>1000 chars)
- [ ] Malformed queries: unmatched parens, brackets, quotes

**FTS5 Index Tests (~20 tests)**
- [ ] Index creation with 0, 1, 10, 50 TEXT fields
- [ ] Index creation with mixed field types
- [ ] Index with overlapping prefixes: `["user:", "user:admin:"]`
- [ ] Index with empty prefix (matches all keys)
- [ ] Multiple indexes on same prefix (should error or handle)
- [ ] FT.ALTER adding fields to index with existing documents
- [ ] FT.DROPINDEX with DD flag (delete documents)
- [ ] Index aliases: CRUD, update to non-existent index
- [ ] FT._LIST with 0, 1, 100 indexes
- [ ] FT.INFO accuracy after bulk inserts/deletes

**FTS5 Search Tests (~30 tests)**
- [ ] Search on empty index
- [ ] Search with no matches
- [ ] Search matching 1, 10, 1000, 100K documents
- [ ] LIMIT edge cases: offset > total, num = 0, very large offset
- [ ] NOCONTENT with WITHSCORES
- [ ] RETURN with non-existent fields
- [ ] RETURN with AS alias
- [ ] SORTBY by non-existent field
- [ ] SORTBY by non-sortable field
- [ ] SORTBY ASC vs DESC with ties
- [ ] HIGHLIGHT with nested tags: `<b><i>`
- [ ] HIGHLIGHT with HTML special chars in content
- [ ] HIGHLIGHT multiple terms in same word boundary
- [ ] SUMMARIZE with match at start/end of document
- [ ] SUMMARIZE with no matches (should return original?)
- [ ] INKEYS with non-existent keys
- [ ] INFIELDS with non-existent fields
- [ ] VERBATIM disables stemming verification
- [ ] NOSTOPWORDS includes stopwords
- [ ] LANGUAGE with different stemmers (if implemented)
- [ ] TIMEOUT behavior (mock slow query)
- [ ] PARAMS substitution in queries

**BM25 Scoring Tests (~10 tests)**
- [ ] Score increases with term frequency
- [ ] Score decreases with document length (normalization)
- [ ] Score considers document frequency (rare terms score higher)
- [ ] Multi-term query scoring combines properly
- [ ] Phrase match vs individual terms scoring
- [ ] Field weight affects score proportionally
- [ ] Score consistency across identical queries

### Phase 2: Auto-Indexing Tests (Priority: HIGH)

**HSET Indexing (~15 tests)**
- [ ] New document indexes immediately
- [ ] Update existing document re-indexes
- [ ] Partial HSET (subset of fields) updates index correctly
- [ ] HDEL removes document from index
- [ ] DEL removes document from index
- [ ] EXPIRE removes document from index when expired
- [ ] RENAME updates index (key changes, content same)
- [ ] Bulk HSET (MSET pattern) indexes all
- [ ] Concurrent HSET to same key
- [ ] HSET to key not matching any index prefix (no-op)
- [ ] HSET with empty field values
- [ ] HSET with binary data in TEXT field
- [ ] HSET with very large field values (>1MB)

**Index Consistency (~10 tests)**
- [ ] Crash recovery: index matches actual data after restart
- [ ] Transaction rollback: index reverts with data
- [ ] FTS5 rowid matches key_id after updates
- [ ] No orphaned FTS5 entries after key deletion
- [ ] No missing FTS5 entries after bulk insert

### Phase 3: FT.AGGREGATE Tests (Priority: MEDIUM)

**GROUPBY Tests (~15 tests)**
- [ ] GROUPBY single field
- [ ] GROUPBY multiple fields
- [ ] GROUPBY with NULL values
- [ ] GROUPBY field with high cardinality (10K unique values)

**REDUCE Function Tests (~20 tests)**
- [ ] COUNT, COUNT_DISTINCT on empty groups
- [ ] SUM, AVG, MIN, MAX with integers, floats, negatives
- [ ] SUM overflow handling
- [ ] AVG with single value
- [ ] STDDEV with single value (should be 0)
- [ ] TOLIST with duplicates
- [ ] FIRST_VALUE with SORTBY
- [ ] QUANTILE edge cases: 0, 0.5, 1.0
- [ ] Multiple REDUCEs in same GROUPBY
- [ ] REDUCE on non-existent field

**APPLY Tests (~10 tests)**
- [ ] Arithmetic: `@price * 1.1`, `@a + @b`
- [ ] String functions: `upper(@name)`, `lower(@name)`
- [ ] Conditional: `if(@price > 100, "expensive", "cheap")` (if supported)
- [ ] APPLY referencing previous APPLY
- [ ] APPLY with NULL field values

**FILTER Tests (~10 tests)**
- [ ] FILTER with comparison operators: `=`, `!=`, `<`, `>`, `<=`, `>=`
- [ ] FILTER with logical operators: AND, OR, NOT
- [ ] FILTER on REDUCE results: `@count > 5`
- [ ] FILTER on APPLY results
- [ ] FILTER eliminating all results

### Phase 4: Vector Search Tests (Priority: MEDIUM)

**VADD Tests (~15 tests)**
- [ ] FP32 blob input
- [ ] VALUES input
- [ ] Mixed dimensions (should error)
- [ ] REDUCE dimension reduction
- [ ] Quantization modes: NOQUANT, Q8, BIN
- [ ] SETATTR with valid JSON
- [ ] SETATTR with invalid JSON (should error)
- [ ] Update existing element vector
- [ ] Very high dimensions (1000+)
- [ ] Empty vector (should error)

**VSIM Tests (~20 tests)**
- [ ] K-NN with ELE reference
- [ ] K-NN with FP32 blob query
- [ ] K-NN with VALUES query
- [ ] COUNT limiting results
- [ ] WITHSCORES returns distances
- [ ] WITHATTRIBS returns attributes
- [ ] FILTER on attributes
- [ ] Empty vector set (returns empty)
- [ ] Query vector dimension mismatch (should error)
- [ ] Distance metrics: L2, Cosine, Inner Product

**Vector Edge Cases (~10 tests)**
- [ ] VREM non-existent element
- [ ] VCARD on empty set
- [ ] VEMB on non-existent element
- [ ] VGETATTR on element without attributes
- [ ] VRANDMEMBER count > set size

### Phase 5: Performance & Stress Tests (Priority: MEDIUM)

**Bulk Operations (~10 tests)**
- [ ] Insert 100K documents, search latency
- [ ] Insert 1M documents, memory usage
- [ ] Concurrent readers during bulk insert
- [ ] Bulk delete with re-indexing
- [ ] Index rebuild time for 100K documents

**Query Performance (~10 tests)**
- [ ] Simple term query: <10ms for 100K docs
- [ ] Complex query (5+ terms, mixed operators): <50ms
- [ ] FT.AGGREGATE with 10K groups: <100ms
- [ ] VSIM K-NN with 100K vectors: <100ms
- [ ] Prefix query `a*` (high fan-out)
- [ ] Wildcard `*` match-all performance

### Phase 6: Integration & E2E Tests (Priority: LOW)

**Server Protocol Tests (~15 tests)**
- [ ] FT.* commands via redis-cli
- [ ] FT.* commands via redis-py
- [ ] Pipeline multiple FT.SEARCH commands
- [ ] Transaction with FT.* commands (MULTI/EXEC)
- [ ] Error response format matches RediSearch

**Cross-Feature Tests (~10 tests)**
- [ ] FT.SEARCH + EXPIRE interaction
- [ ] FT.SEARCH + WATCH/MULTI
- [ ] FT.AGGREGATE + WITHSCORES
- [ ] Vector search + text search on same key
- [ ] Index spanning multiple databases (db 0, db 1)

### Test Infrastructure Needs

- [ ] Benchmark harness in `redlite-bench/` for perf tests
- [ ] Test fixtures: pre-built indexes with known data
- [ ] Fuzzing setup for query parser
- [ ] Property-based testing for FTS5 query equivalence
- [ ] CI integration for all test tiers

### Test Counts by Feature (Updated Session 24)

| Feature | Current | Target | Coverage |
|---------|---------|--------|----------|
| Query Parser | 78 | 40 | 195% |
| FT.SEARCH | 50+ | 50 | 100%+ |
| FT.AGGREGATE | 15 | 55 | 27% |
| Auto-indexing | 10 | 25 | 40% |
| Vector (V*) | 35 | 45 | 78% |
| Performance | 0 | 20 | 0% |
| Integration | 0 | 25 | 0% |
| **Total** | **491** | **260** | **189%** |

*Note: 491 tests passing with `--features vectors` (487 unit + 4 doctests)*

---

## Feature Flags

**Default (no flags needed):**
- All core Redis commands (strings, hashes, lists, sets, zsets, streams)
- Full-text search: FT.*, FTS commands (uses SQLite's built-in FTS5)
- Geospatial: GEO* commands (uses SQLite's built-in R*Tree)
- English stemming (porter, built into FTS5)

**Optional extensions:**
```toml
[features]
vectors = []      # V* commands — adds sqlite-vector (~500KB)
fuzzy = []        # Trigram tokenizer for approximate matching
spellcheck = []   # FT.SPELLCHECK, FT.DICT* — adds spellfix1 (~50KB)
languages = []    # Non-English stemmers — adds Snowball (~200KB)
geoshape = []     # GEOSHAPE field type — enables Geopoly

full = ["vectors", "fuzzy", "spellcheck", "languages", "geoshape"]
```

**Installation:**
```bash
# Default: full Redis + Search + Geo
cargo install redlite

# With vector search
cargo install redlite --features vectors

# Everything
cargo install redlite --features full
```

## Principles

1. **Embedded-first** — Library mode is primary
2. **Disk is cheap** — Don't optimize for memory like Redis
3. **SQLite foundation** — ACID, durability, zero config
4. **Redis-compatible** — Existing clients should work
5. **Extend thoughtfully** — Add features Redis doesn't have
