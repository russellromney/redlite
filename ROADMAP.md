# Roadmap

See [CHANGELOG.md](./CHANGELOG.md) for completed features.

## Recently Completed

### Session 35: Blocking Operations (BLPOP/BRPOP) - PLANNED

**Goal**: Implement BLPOP and BRPOP with adaptive polling for both embedded and server modes.

**Rationale**: SQLite with warm page cache returns queries in microseconds. In file mode, other processes can connect and push data. Polling at 250μs-1ms intervals is efficient and provides near-instant response when data becomes available.

#### Implementation

**Adaptive Polling Strategy**:
- Start at 250μs polling interval
- After 100 iterations with no data, increase to 1ms
- Cap at 1ms for long waits
- SQLite cached reads are ~1μs, so polling overhead is minimal

```rust
pub fn blpop(&self, keys: &[&str], timeout: f64) -> Result<Option<(String, Vec<u8>)>> {
    let deadline = if timeout > 0.0 {
        Some(Instant::now() + Duration::from_secs_f64(timeout))
    } else {
        None // timeout=0 means wait forever (Redis behavior)
    };

    let mut poll_interval = Duration::from_micros(250);
    let mut iterations = 0;

    loop {
        // Try each key in priority order
        for key in keys {
            if let Some(value) = self.lpop(key, Some(1))?.pop() {
                return Ok(Some((key.to_string(), value)));
            }
        }

        // Check timeout
        if let Some(d) = deadline {
            if Instant::now() >= d {
                return Ok(None);
            }
        }

        // Adaptive backoff
        iterations += 1;
        if iterations > 100 && poll_interval < Duration::from_millis(1) {
            poll_interval = Duration::from_millis(1);
        }

        std::thread::sleep(poll_interval);
    }
}
```

**Commands**:
- `BLPOP key [key ...] timeout` - Blocking left pop
- `BRPOP key [key ...] timeout` - Blocking right pop

**Tests** (~12 scenarios):
- [ ] `test_blpop_immediate_data` - Data already in list, returns immediately
- [ ] `test_blpop_timeout_empty` - Empty list, timeout returns nil
- [ ] `test_blpop_multiple_keys` - First non-empty key wins
- [ ] `test_blpop_key_priority` - Keys checked in order
- [ ] `test_blpop_timeout_zero` - Infinite wait (test with concurrent push)
- [ ] `test_blpop_binary_data` - Binary values work correctly
- [ ] `test_brpop_immediate_data` - Right pop variant
- [ ] `test_brpop_timeout_empty` - Right pop timeout
- [ ] `test_blpop_concurrent_push` - Another thread pushes during wait
- [ ] `test_blpop_wrong_type` - Error on non-list key
- [ ] `test_blpop_nonexistent_key` - Non-existent keys skipped
- [ ] `test_blpop_mixed_keys` - Mix of existing/non-existing keys

**Server Mode**:
- Same polling implementation works
- RESP handler converts timeout from seconds to Duration

#### Success Criteria
- [ ] BLPOP/BRPOP implemented with adaptive 250μs→1ms polling
- [ ] All 12 oracle tests passing
- [ ] Works in both embedded and server modes
- [ ] Timeout=0 works correctly (infinite wait)
- [ ] Multi-key priority ordering matches Redis

---

### Session 33: Fuzzy Search with Built-in Trigram Tokenizer - IN PROGRESS

**Goal**: Enable fuzzy/substring matching in FT.SEARCH using SQLite FTS5's built-in trigram tokenizer.

**Rationale**: FTS5 has included a built-in `trigram` tokenizer since SQLite 3.34.0 (Dec 2020). This enables:
- Substring matching (like SQL LIKE '%pattern%' but indexed)
- GLOB/LIKE queries that use the FTS5 index
- Typo-tolerant search via trigram overlap
- No custom C code or external extensions required

**Reference**: [SQLite FTS5 Trigram Tokenizer](https://sqlite.org/fts5.html#the_trigram_tokenizer)

#### Phase 1: Trigram Index Support (~6 tests)

**Implementation**:
1. Add `TOKENIZE trigram` option to FT.CREATE
2. Update schema to track tokenizer type per field
3. Generate trigram-tokenized FTS5 tables

```bash
# Create index with trigram tokenizer for fuzzy matching
FT.CREATE idx ON HASH PREFIX 1 doc:
  SCHEMA title TEXT TOKENIZE trigram   # Enables substring/fuzzy
         body TEXT                      # Default porter stemming
```

**Tests**:
- [ ] `test_ft_create_with_trigram_tokenizer` - Create index with TOKENIZE trigram
- [ ] `test_ft_search_trigram_substring` - Find "hello" in "say hello world"
- [ ] `test_ft_search_trigram_prefix` - Prefix match with trigrams
- [ ] `test_ft_search_trigram_suffix` - Suffix match (unlike standard FTS5)
- [ ] `test_ft_search_trigram_case_insensitive` - Case handling
- [ ] `test_ft_info_shows_tokenizer` - FT.INFO displays tokenizer type

#### Phase 2: Fuzzy Query Syntax (~8 tests)

**Implementation**:
1. Add `%%term%%` syntax for fuzzy substring matching
2. Map to FTS5 LIKE/GLOB when trigram index exists
3. Fall back to standard MATCH for non-trigram indexes

```bash
# Fuzzy substring search (requires trigram tokenizer)
FT.SEARCH idx "%%helo%%"              # Matches "hello", "helo", etc.
FT.SEARCH idx "@title:%%wrld%%"       # Field-scoped fuzzy
```

**Tests**:
- [ ] `test_ft_search_fuzzy_syntax` - Basic %%term%% query
- [ ] `test_ft_search_fuzzy_typo_single` - 1-char typo matches
- [ ] `test_ft_search_fuzzy_typo_double` - 2-char typo matches (lower rank)
- [ ] `test_ft_search_fuzzy_field_scoped` - @field:%%term%%
- [ ] `test_ft_search_fuzzy_mixed_query` - Fuzzy + exact in same query
- [ ] `test_ft_search_fuzzy_on_non_trigram` - Graceful fallback or error
- [ ] `test_ft_search_fuzzy_unicode` - Unicode fuzzy matching
- [ ] `test_ft_search_fuzzy_short_terms` - 1-2 char terms (edge case)

#### Phase 3: Levenshtein Ranking (Optional, ~6 tests)

**Implementation**:
1. Add pure Rust Levenshtein distance function (~50 lines)
2. Post-filter trigram results with edit distance for precision
3. Add DISTANCE parameter for max edit threshold

```bash
# Fuzzy with edit distance limit
FT.SEARCH idx "%%helo%%" DISTANCE 2   # Max 2 edits allowed
```

**Tests**:
- [ ] `test_ft_search_levenshtein_basic` - Edit distance calculation
- [ ] `test_ft_search_levenshtein_insertion` - "helo" → "hello" (1 edit)
- [ ] `test_ft_search_levenshtein_deletion` - "helllo" → "hello" (1 edit)
- [ ] `test_ft_search_levenshtein_substitution` - "jello" → "hello" (1 edit)
- [ ] `test_ft_search_levenshtein_distance_filter` - DISTANCE param works
- [ ] `test_ft_search_levenshtein_ranking` - Closer matches rank higher

#### Success Criteria
- [ ] FT.CREATE supports TOKENIZE trigram option
- [ ] Substring matching works on trigram indexes
- [ ] %%term%% fuzzy syntax implemented
- [ ] 20+ new fuzzy search tests passing
- [ ] All existing tests continue to pass
- [ ] Performance: <100ms fuzzy search on 10K documents

---

### Session 28: Keyset Pagination (Performance)
- [x] Refactor SCAN to use `WHERE key > last_seen` instead of OFFSET
- [x] Refactor HSCAN to use `WHERE field > last_seen`
- [x] Refactor SSCAN to use `WHERE member > last_seen`
- [x] Refactor ZSCAN to use compound `(score, member)` keyset
- [x] Update server handlers for string cursor format
- [x] All 16 scan-related unit tests passing

### Session 30: Documentation Audit & Roadmap Sync
- [x] Review Session 28 keyset pagination implementation
- [x] Verify SDK compatibility with string cursors (WASM uses separate SQLite, standard clients work)
- [x] Discover FT.AGGREGATE is complete (Phase 3 was already implemented)
- [x] Update ROADMAP to reflect actual implementation status
- [x] Update CHANGELOG with Session 30 summary
- [x] All tests verified (16 scan tests + 14 FT.AGGREGATE tests + 509 others = 539 total)

**Key Finding**: FT.AGGREGATE is fully implemented with all REDUCE functions, APPLY expressions, FILTER, SORTBY, LIMIT. The feature was marked as "Next" in ROADMAP but is actually production-ready with comprehensive test coverage.

### Session 32: Vector Search Test Expansion (35 → 61 tests) - ✅ COMPLETE

**Goal**: Comprehensive test coverage for all V* command features to ensure production-readiness.

**Result**: 61 vector tests passing (35 existing + 26 new), 592 total tests with `--features "vectors geo"`

#### Completed Test Categories:

**1. Distance Metrics & Accuracy (3 tests)** - ✅ COMPLETE
- [x] `test_vsim_l2_distance_accuracy` - L2 distance calculation with known vectors
- [x] `test_vsim_cosine_accuracy` - Cosine similarity (parallel/orthogonal vectors)
- [x] `test_vsim_inner_product` - Inner product metric verification

**2. Quantization (1 test)** - ✅ COMPLETE
- [x] `test_vadd_quantization_preserves_similarity` - Q8 vs NoQuant ranking consistency

**3. Scale & Dimensions (3 tests)** - ✅ COMPLETE
- [x] `test_vadd_large_scale` - 1000 vectors performance test
- [x] `test_vadd_very_high_dimensions` - 1536 dimensions (OpenAI embeddings)
- [x] `test_vadd_single_dimension` - 1D vector edge case

**4. Vector Properties (3 tests)** - ✅ COMPLETE
- [x] `test_vadd_normalized_vectors` - Unit-length vectors (cosine similarity)
- [x] `test_vadd_zero_vector_handling` - Degenerate zero vector acceptance
- [x] `test_vadd_negative_values` - Negative embedding values

**5. Query Behavior (3 tests)** - ✅ COMPLETE
- [x] `test_vsim_dimension_mismatch_query` - Mismatched query dimensions
- [x] `test_vsim_count_zero` - COUNT=0 edge case
- [x] `test_vsim_count_exceeds_available` - COUNT > total elements

**6. Attributes (3 tests)** - ✅ COMPLETE
- [x] `test_vgetattr_complex_json` - Nested JSON attribute storage
- [x] `test_vsetattr_update_existing` - In-place attribute updates
- [x] `test_vsetattr_remove_attributes` - Empty JSON attribute removal

**7. Operations (4 tests)** - ✅ COMPLETE
- [x] `test_vrandmember_count_negative` - Random sampling with count
- [x] `test_vrem_multiple_elements` - Bulk element removal
- [x] `test_vector_cross_database_isolation` - Database scoping behavior
- [x] `test_vinfo_with_mixed_quantization` - Mixed quantization metadata

**8. Search Features (3 tests)** - ✅ COMPLETE
- [x] `test_vsim_with_filter_complex` - Attribute-based filtering in VSIM
- [x] `test_vsim_exact_match_score` - Perfect match scoring validation
- [x] `test_vcard_nonexistent_key` - VCARD on missing key returns 0

**Test Summary**:
- **26 new tests added** to `crates/redlite/src/db.rs`
- **61 total vector tests** (35 existing + 26 new)
- **All tests passing** in 0.49s
- **Coverage**: All distance metrics, quantization modes, 1-1536 dimensions, 1-1000 vectors/set, complex JSON attributes, filter integration

---

### Session 31: FT.AGGREGATE Test Expansion (14 → 41 tests) - ✅ COMPLETE

**Goal**: Comprehensive test coverage for all FT.AGGREGATE features to ensure production-readiness.

**Result**: 41 FT.AGGREGATE tests passing (14 existing + 27 new), 566 total tests with `--features geo`

#### Completed Test Categories:

**1. REDUCE Functions (8 tests)** - ✅ COMPLETE
- [x] `test_ft_aggregate_reduce_sum` - SUM reducer on numeric field
- [x] `test_ft_aggregate_reduce_avg` - AVG reducer calculating mean
- [x] `test_ft_aggregate_reduce_min_max` - MIN and MAX reducers in same query
- [x] `test_ft_aggregate_reduce_stddev` - STDDEV for variance analysis
- [x] `test_ft_aggregate_reduce_count_distinct` - COUNT_DISTINCT for unique values
- [x] `test_ft_aggregate_reduce_count_distinctish` - Approximate unique count
- [x] `test_ft_aggregate_reduce_tolist` - TOLIST collecting values
- [x] `test_ft_aggregate_reduce_first_value` - FIRST_VALUE from group

**2. SORTBY Variations (5 tests)** - ✅ COMPLETE
- [x] `test_ft_aggregate_sortby_desc` - Descending sort order
- [x] `test_ft_aggregate_sortby_multiple_fields` - Sort by 2+ fields
- [x] `test_ft_aggregate_sortby_with_max` - SORTBY MAX to limit results
- [x] `test_ft_aggregate_sortby_on_original_field` - Sort without APPLY
- [x] `test_ft_aggregate_sortby_numeric_vs_string` - Numeric vs lexical sorting

**3. GROUPBY Variations (3 tests)** - ✅ COMPLETE
- [x] `test_ft_aggregate_groupby_multiple_fields` - Group by category + status
- [x] `test_ft_aggregate_groupby_multiple_reducers` - Multiple REDUCE in one GROUPBY
- [x] `test_ft_aggregate_groupby_missing_fields` - Handle docs without group field

**4. LOAD Feature (2 tests)** - ✅ COMPLETE
- [x] `test_ft_aggregate_load_specific_fields` - LOAD only requested fields
- [x] `test_ft_aggregate_load_with_groupby` - LOAD additional fields with GROUPBY

**5. LIMIT with Offset (2 tests)** - ✅ COMPLETE
- [x] `test_ft_aggregate_limit_offset` - Pagination with LIMIT offset num
- [x] `test_ft_aggregate_limit_edge_cases` - LIMIT 0, out of bounds offset

**6. Query Integration (3 tests)** - ✅ COMPLETE
- [x] `test_ft_aggregate_with_text_query` - Non-wildcard FTS query
- [x] `test_ft_aggregate_with_field_query` - @field:value aggregation
- [x] `test_ft_aggregate_with_numeric_range` - @price:[10 100] aggregation

**7. Full Pipeline (2 tests)** - ✅ COMPLETE
- [x] `test_ft_aggregate_full_pipeline` - LOAD + GROUPBY + REDUCE + SORTBY + FILTER + LIMIT
- [x] `test_ft_aggregate_complex_ecommerce` - Real-world e-commerce analytics scenario

**8. Edge Cases (2 tests)** - ✅ COMPLETE
- [x] `test_ft_aggregate_empty_results` - Query matches zero documents
- [x] `test_ft_aggregate_single_document` - Aggregation with 1 match

**Test Summary**:
- **27 new tests added** to `crates/redlite/src/db.rs`
- **41 total FT.AGGREGATE tests** (14 existing + 27 new)
- **All tests passing** in 0.35s
- **Coverage**: All REDUCE functions, SORTBY variations, GROUPBY combinations, LOAD, LIMIT, query integration, full pipelines, edge cases

### Session 29: Oracle Test Expansion (66 → 85 tests)
- [x] Added 19 new Redis oracle comparison tests
- [x] Expanded coverage to streams, sorted sets, keys, string options
- [x] Added type mismatch tests and edge case tests
- [x] Zero divergences across all data types
- [x] See CHANGELOG.md for details

---

## Completed Major Features

### Sessions 23-24: Search & Vectors Implementation - COMPLETE

**See [IMPLEMENTATION_PLAN.md](./IMPLEMENTATION_PLAN.md) for full details.**

RediSearch-compatible FT.* and Redis 8-compatible V* commands are fully implemented.

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

#### Phase 3: RediSearch Aggregations - COMPLETE
- [x] Implement FT.AGGREGATE with LOAD, GROUPBY, REDUCE, SORTBY, APPLY, FILTER, LIMIT
- [x] All REDUCE functions: COUNT, COUNT_DISTINCT, SUM, AVG, MIN, MAX, STDDEV, TOLIST, FIRST_VALUE, QUANTILE, RANDOM_SAMPLE
- [x] APPLY expressions with arithmetic operations and string functions (upper/lower)
- [x] FILTER expressions with comparison operators (>, <, ==, !=, AND, OR)
- [x] SORTBY with ASC/DESC and MAX limit
- [x] Full command parser in server/mod.rs
- [x] 14 comprehensive unit tests passing

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

### Session 25: Geospatial (R*Tree) - ✅ COMPLETE

Redis-compatible geo commands using SQLite's built-in R*Tree extension.
**Enabled via `--features geo`** — R*Tree is built into standard SQLite (no extra dependencies).

**Commands (all implemented):**
- [x] GEOADD key [NX|XX] [CH] longitude latitude member [lon lat member ...]
- [x] GEOPOS key member [member ...]
- [x] GEODIST key member1 member2 [M|KM|MI|FT]
- [x] GEOHASH key member [member ...]
- [x] GEOSEARCH key FROMMEMBER/FROMLONLAT BYRADIUS/BYBOX [ASC|DESC] [COUNT n [ANY]] [WITHCOORD] [WITHDIST] [WITHHASH]
- [x] GEOSEARCHSTORE dest src FROMMEMBER/FROMLONLAT BYRADIUS/BYBOX [STOREDIST]

**Implementation:**
- R*Tree virtual table for bounding-box pre-filtering
- Haversine formula for precise distance calculations (Earth radius = 6371000m)
- Base32 geohash encoding (11 chars = ~0.6mm precision)
- GEOSEARCHSTORE stores results as sorted sets (compatible with ZRANGE)

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
    id, min_lon, max_lon, min_lat, max_lat
);
```

**Test Results:** 17 geo tests + 473 total tests passing with `--features geo`

**GEOSHAPE (optional `--features geoshape`):** (Future - not implemented)

For polygon queries, enable Geopoly extension.

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

#### Phase 1: Property-Based Testing + Fuzzing (Session 27.1) - ✅ COMPLETE
- [x] Add `proptest`, `arbitrary`, `libfuzzer-sys` dependencies
- [x] Create `tests/properties.rs` with 34 comprehensive proptest-based tests
- [x] Properties: `set_get_roundtrip`, `incr_atomic`, `list_ordering`, `set_uniqueness`
- [x] Properties: `zset_score_ordering`, `hash_field_roundtrip`
- [x] Properties: `expire_respected`, `type_commands`, `del_exists`, `append`
- [x] Properties: `set_nx_behavior`, `set_xx_behavior`, `hash_hgetall`, `hash_hdel`, `hash_hincrby`
- [x] Create `fuzz/` targets for RESP parser, FT.SEARCH query parser, and command handler
- [x] Create `tests/regression_seeds.txt` — permanent seed bank with categorized sections
- [x] Verify: All 34 property tests passing with default cases
- [ ] Verify: `cargo +nightly fuzz run resp_parser` (10 min no crash)
- [ ] Verify: `cargo +nightly miri test` (no UB in unsafe blocks)

#### Phase 2: Redis Oracle (Session 27.2) - IN PROGRESS
- [x] Add `redis` crate as dev dependency
- [x] Create `tests/oracle.rs` with 85 comprehensive tests
- [x] Test groups: Strings, Lists, Hashes, Sets, Sorted Sets, Keys, Streams, Bitmaps
- [x] Assert identical results for identical operation sequences
- [x] Verify: `redis-server & cargo test --test oracle -- --test-threads=1`
- [x] Tests: Basic operations, random operations, comprehensive mixed operations
- [x] All 85 oracle tests passing with zero divergences

##### Oracle Test Expansion Checklist

**Target: 200+ tests covering all Redis-compatible commands**

**Strings (22 commands - all tested)**
- [x] GET, SET, INCR, DECR, INCRBY, DECRBY, INCRBYFLOAT
- [x] MGET, MSET, APPEND, STRLEN, GETRANGE, SETRANGE
- [x] GETEX, GETDEL, SETEX, PSETEX
- [x] SETBIT, GETBIT, BITCOUNT, BITOP
- [x] SET with options (NX/XX/EX/PX tested)
- [x] Empty value and large value edge cases

**Keys (14 commands - all tested)**
- [x] DEL, EXISTS, EXPIRE, PEXPIRE, EXPIREAT, PEXPIREAT, PERSIST
- [x] TTL, PTTL, TYPE, RENAME, RENAMENX, KEYS
- [x] SCAN (proper cursor iteration test)

**Hashes (13 commands - all tested)**
- [x] HSET, HGET, HMGET, HGETALL, HDEL, HEXISTS
- [x] HKEYS, HVALS, HLEN, HINCRBY, HINCRBYFLOAT, HSETNX, HSCAN
- [x] Empty hash edge cases

**Lists (17 commands - 15 tested)**
- [x] LPUSH, RPUSH, LPOP, RPOP, LLEN, LRANGE, LINDEX
- [x] LSET, LTRIM, LREM, LINSERT, LPUSHX, RPUSHX, LPOS, LMOVE
- [x] Empty list edge cases
- [ ] BLPOP (blocking - async test needed)
- [ ] BRPOP (blocking - async test needed)

**Sets (15 commands - all tested)**
- [x] SADD, SREM, SMEMBERS, SISMEMBER, SCARD, SPOP, SRANDMEMBER
- [x] SDIFF, SINTER, SUNION, SMOVE, SDIFFSTORE, SINTERSTORE, SUNIONSTORE, SSCAN
- [x] Empty set edge cases

**Sorted Sets (16 commands - all tested)**
- [x] ZADD, ZREM, ZSCORE, ZRANK, ZREVRANK, ZCARD
- [x] ZRANGE, ZREVRANGE, ZRANGEBYSCORE, ZCOUNT, ZINCRBY
- [x] ZREMRANGEBYRANK, ZREMRANGEBYSCORE, ZINTERSTORE, ZUNIONSTORE, ZSCAN
- [x] Empty sorted set edge cases

**Streams (13 commands - 7 tested)**
- [x] XADD, XLEN, XTRIM
- [x] XRANGE, XREVRANGE, XDEL, XINFO STREAM
- [ ] XGROUP (CREATE, DESTROY, SETID, CREATECONSUMER, DELCONSUMER)
- [ ] XREAD (async/blocking)
- [ ] XREADGROUP (async/blocking)
- [ ] XACK, XPENDING, XCLAIM

**Transactions (5 commands - 0 tested)**
- [ ] MULTI, EXEC, DISCARD
- [ ] WATCH, UNWATCH

**GEO (6 commands - requires --features geo)**
- [ ] GEOADD, GEOPOS, GEODIST, GEOHASH, GEOSEARCH, GEOSEARCHSTORE

**Server/Connection (tested via integration)**
- [x] DBSIZE, FLUSHDB, PING, ECHO

**Additional Test Categories**
- [x] Type mismatch tests (WRONGTYPE errors - 4 tests covering all type combinations)
- [x] Edge case tests (empty values, large values - 1MB strings)
- [ ] Expiration edge cases (keys that just expired, negative TTL)
- [ ] Error response format matching (error messages match Redis exactly)

**Summary: ~100 commands, 85 tests, remaining: blocking commands, transactions, GEO**

#### Phase 3: MadSim Integration (Session 27.3) - ✅ COMPLETE
- [x] Add `madsim`, `madsim-tokio` dependencies (cfg-gated)
- [x] Create `src/sim.rs` module with unified runtime API
- [x] `SimConfig` and `SimContext` for deterministic simulation
- [x] `runtime::spawn`, `runtime::sleep`, `runtime::yield_now` work with both runtimes
- [x] Conditional main function for madsim/tokio compatibility
- [x] Tests: `concurrent_operations`, `crash_recovery`, `connection_storm`
- [x] Seed-based reproducibility with ChaCha8Rng
- [x] Verify: `RUSTFLAGS="--cfg madsim" cargo run --features madsim -- simulate`

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
  - **SIMULATE**: Deterministic simulation (concurrent_operations, crash_recovery, connection_storm, write_contention)
  - **CHAOS**: Fault injection (crash_mid_write, corrupt_read, disk_full, slow_write)
  - **STRESS**: Concurrent load testing with throughput/latency metrics
  - **FUZZ**: In-process fuzzing (resp_parser, query_parser, command_handler targets)
  - **SOAK**: Long-running stability testing with memory leak detection
- [x] All using real tokio with actual Redlite library (no mocks)
- [x] Seed-based reproducibility with ChaCha8Rng
- [x] Code review cleanup: removed dead code (libsql_db.rs, distributed concepts in sim.rs, unused types)
- [ ] `cloud` command for fly.io parallel execution (placeholder)
- [ ] Spec-driven scenarios in `spec/scenarios.yaml`
- [x] JSON + Markdown report output (code exists in report.rs, needs wiring)

#### Phase 5.5: Report Output Wiring (Session 27.5.5)
**Goal**: Wire up `--format json` and `--format markdown` output for all redlite-dst commands.

**Implementation Steps**:
1. [x] Add `format` and `output` fields to `TestRunner`
2. [x] Update `TestRunner::new()` to accept format/output params
3. [x] Create `output_results()` method that:
   - If format == "console": call `print_summary` (existing behavior)
   - If format == "json": generate via `JsonReport::from_summary().to_json()`
   - If format == "markdown": generate via `generate_markdown()`
4. [x] Write output to file if `--output` specified, otherwise stdout
5. [x] Track results Vec in smoke() and other commands that don't have it
6. [x] Call `output_results(&summary, &results)` at end of each command
7. [x] Update `main.rs` to pass `cli.format` and `cli.output` to TestRunner

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

## Planned

### Session 32: Fuzzy Search & Spell Correction

Approximate string matching for typo-tolerant search. **Optional feature** via `--features fuzzy`.

**Goal**: Enable fuzzy search in FT.SEARCH and FT.SUGGET with Levenshtein distance for typo tolerance.

#### Phase 1: Trigram Tokenizer (Session 32.1)

**Approach**: Custom FTS5 tokenizer that generates character trigrams for approximate matching.

**How Trigrams Work**:
- "hello" → ["hel", "ell", "llo"] (3-character sliding window)
- "helo" (typo) → ["hel", "elo"] (2/3 match with "hello" = similarity)
- Jaccard similarity: `|intersection| / |union|` for matching

**Implementation**:
```rust
// src/tokenizers/trigram.rs (~150 lines)
pub struct TrigramTokenizer;

impl Fts5Tokenizer for TrigramTokenizer {
    fn tokenize(&self, text: &str, callback: impl FnMut(&str, usize, usize)) {
        // Generate sliding window of 3 chars
        for i in 0..text.len().saturating_sub(2) {
            let trigram = &text[i..i+3];
            callback(trigram, i, i+3);
        }
    }
}
```

**FT.CREATE integration**:
```bash
FT.CREATE idx ON HASH PREFIX 1 doc:
  SCHEMA title TEXT FUZZY   # Enable trigram tokenizer for this field
         body TEXT           # Regular porter tokenizer
```

**Query syntax**:
```bash
# Exact match (default)
FT.SEARCH idx "hello world"

# Fuzzy match with edit distance threshold
FT.SEARCH idx "%helo% %wrld%" DISTANCE 2    # Up to 2 character edits

# Or via query operator
FT.SEARCH idx "~hello ~world"  # ~ prefix = fuzzy match
```

**Schema Changes**:
```sql
-- Add tokenizer field to ft_fields table
ALTER TABLE ft_fields ADD COLUMN tokenizer TEXT DEFAULT 'porter';
-- Options: 'porter' (default), 'trigram', 'unicode61', 'ascii'
```

**Files to Create**:
- `src/tokenizers/mod.rs` - Tokenizer registry
- `src/tokenizers/trigram.rs` - Trigram implementation
- `src/tokenizers/fts5_api.rs` - FTS5 C API bindings for custom tokenizers

**Tests** (~15 tests):
- [ ] Trigram generation for ASCII strings
- [ ] Trigram generation for Unicode strings
- [ ] Fuzzy match with 1-char typo
- [ ] Fuzzy match with 2-char typos
- [ ] Fuzzy match with transposition (hello → ehllo)
- [ ] Fuzzy match with insertion (hello → helllo)
- [ ] Fuzzy match with deletion (hello → helo)
- [ ] Distance threshold filtering
- [ ] Performance: fuzzy search on 10K documents

#### Phase 2: Levenshtein Distance (Session 32.2)

**Approach**: Post-filter FTS5 results with actual Levenshtein distance for precise ranking.

**Why Both Trigrams + Levenshtein**:
- Trigrams = Fast pre-filter (uses FTS5 index)
- Levenshtein = Precise ranking (edit distance scoring)

**Implementation**:
```rust
// src/search/levenshtein.rs (~80 lines)
pub fn levenshtein_distance(a: &str, b: &str) -> usize {
    // Wagner-Fischer algorithm
    // Returns minimum edit operations to transform a → b
}

pub fn fuzzy_score(query: &str, result: &str, max_distance: usize) -> Option<f64> {
    let dist = levenshtein_distance(query, result);
    if dist <= max_distance {
        Some(1.0 - (dist as f64 / query.len() as f64))
    } else {
        None
    }
}
```

**FT.SEARCH with fuzzy scoring**:
```bash
# Trigrams find candidates, Levenshtein ranks them
FT.SEARCH idx "%helo%" DISTANCE 2 WITHSCORES SORTBY score DESC
```

**Result**:
```
1) "doc:1"
2) "0.95"  # 1 edit distance, 4-char word = 0.95 similarity
3) 1) "title"
   2) "hello world"
```

**Tests** (~10 tests):
- [ ] Levenshtein("hello", "hello") = 0
- [ ] Levenshtein("hello", "helo") = 1 (deletion)
- [ ] Levenshtein("hello", "helllo") = 1 (insertion)
- [ ] Levenshtein("hello", "ehllo") = 1 (transposition via swap)
- [ ] Levenshtein("hello", "world") = 4 (all substitutions)
- [ ] Fuzzy scoring with max_distance filter
- [ ] Combined trigram + Levenshtein ranking
- [ ] Unicode string distances (emoji, CJK)

#### Phase 3: FT.SPELLCHECK (Session 32.3) - Optional

**Approach**: Use SQLite's built-in spellfix1 extension for dictionary-based spell correction.

**Commands**:
```bash
FT.SPELLCHECK index query [DISTANCE n] [TERMS INCLUDE dict] [TERMS EXCLUDE dict]

# Example
FT.SPELLCHECK idx "helo wrld"
1) "helo"
2) 1) 0.8
   2) "hello"
3) "wrld"
4) 1) 0.75
   2) "world"
```

**FT.DICTADD/DEL**:
```bash
FT.DICTADD dict term [term ...]   # Add words to custom dictionary
FT.DICTDEL dict term [term ...]   # Remove words
FT.DICTDUMP dict                   # List all words in dictionary
```

**Schema** (using spellfix1 extension):
```sql
-- Spellfix1 virtual table for each FTS index
CREATE VIRTUAL TABLE spellfix_idx USING spellfix1;

-- Auto-populate from FTS5 vocabulary
INSERT INTO spellfix_idx(word)
  SELECT term FROM idx_vocab WHERE col='*';
```

**Implementation**:
- Enable spellfix1 extension (~50KB)
- Auto-sync FTS5 vocab → spellfix1 on HSET/DEL
- Query spellfix1 for suggestions with edit distance

**Tests** (~10 tests):
- [ ] FT.SPELLCHECK with single typo
- [ ] FT.SPELLCHECK with multiple typos
- [ ] FT.DICTADD custom dictionary
- [ ] TERMS INCLUDE/EXCLUDE filters
- [ ] Auto-sync vocab on document updates
- [ ] Spelling suggestions ranking

#### Feature Flag

```toml
[features]
fuzzy = []        # Trigram tokenizer + Levenshtein distance
spellcheck = []   # FT.SPELLCHECK, FT.DICT* (requires spellfix1 extension ~50KB)
full = ["vectors", "geo", "fuzzy"]  # Add fuzzy to full feature set
```

#### Success Criteria

- [ ] Trigram tokenizer integrated with FTS5
- [ ] Fuzzy queries with `~term` syntax work
- [ ] Levenshtein distance ranking implemented
- [ ] 25+ tests passing (trigrams + levenshtein + spellcheck)
- [ ] Performance: <100ms for fuzzy search on 10K documents
- [ ] Documentation in README and docs site

#### References

- [FTS5 Extension Architecture](https://www.sqlite.org/fts5.html#custom_tokenizers)
- [Wagner-Fischer Algorithm](https://en.wikipedia.org/wiki/Wagner%E2%80%93Fischer_algorithm)
- [SQLite spellfix1](https://www.sqlite.org/spellfix1.html)
- [Trigram Matching in PostgreSQL](https://www.postgresql.org/docs/current/pgtrgm.html)

---

### HyperLogLog (Probabilistic Cardinality)

Approximate COUNT DISTINCT with O(1) memory per key. Useful for unique visitor counts, distinct element estimation.

**Commands:**
```bash
PFADD key element [element ...]    # Add elements to HLL
PFCOUNT key [key ...]              # Get cardinality estimate (0.81% error)
PFMERGE destkey sourcekey [sourcekey ...]  # Merge HLLs
PFDEBUG DECODE|ENCODING|GETREG key  # Debug commands (optional)
```

**Implementation:**
- Build our own in Rust (~100 lines, algorithm is well-documented)
- Store 16KB register array per key as BLOB
- Use 14-bit prefix (16384 registers) like Redis
- Compare against [sqlite_hll](https://github.com/wperron/sqlite_hll) for correctness verification
- Reference: [hyperloglog-rs](https://github.com/LucaCappelletti94/hyperloglog-rs) (MIT) for algorithm details

**Schema:**
```sql
CREATE TABLE IF NOT EXISTS hll (
    id INTEGER PRIMARY KEY,
    key_id INTEGER NOT NULL REFERENCES keys(id) ON DELETE CASCADE,
    registers BLOB NOT NULL,  -- 16KB packed registers
    UNIQUE(key_id)
);
```

**Why build our own:** Avoid crate dependency for ~100 lines of code. Protects against supply chain issues. Algorithm is public domain (Flajolet et al. 2007).

---

### Bloom Filters (Probabilistic Set Membership)

Probabilistic "is this possibly in the set?" with configurable false positive rate. O(1) memory per filter.

**Commands:**
```bash
BF.ADD key item                    # Add item to filter
BF.EXISTS key item                 # Check if possibly present
BF.MADD key item [item ...]        # Batch add
BF.MEXISTS key item [item ...]     # Batch check
BF.RESERVE key error_rate capacity # Create with specific params
BF.INFO key                        # Get filter info
BF.CARD key                        # Estimated cardinality
```

**Implementation:**
- Bit array stored as BLOB in SQLite
- Configurable hash count (k) and size (m) based on desired error rate
- Default: 1% false positive rate
- ~100 lines of Rust

**Schema:**
```sql
CREATE TABLE IF NOT EXISTS bloom_filters (
    id INTEGER PRIMARY KEY,
    key_id INTEGER NOT NULL REFERENCES keys(id) ON DELETE CASCADE,
    bits BLOB NOT NULL,
    size INTEGER NOT NULL,        -- m: bit array size
    num_hashes INTEGER NOT NULL,  -- k: hash function count
    items_added INTEGER DEFAULT 0,
    UNIQUE(key_id)
);
```

**Why implement:** Command parity with Redis Stack. Users migrating from Redis shouldn't have to rewrite deduplication logic. Even if B-tree lookups are fast, Bloom filters are the established pattern.

---

### Time Series

High-frequency time-stamped data with aggregation and retention.

**Phase 1 (Now): Sorted Set Sugar**

Time series as sorted sets with timestamp scores:
```bash
TS.ADD key timestamp value [LABELS label value ...]
  → ZADD key timestamp value (+ metadata in hash)

TS.RANGE key fromTimestamp toTimestamp [AGGREGATION type bucketSize]
  → ZRANGEBYSCORE key from to (+ post-processing for aggregation)

TS.GET key
  → ZRANGE key -1 -1 WITHSCORES

TS.INFO key
  → ZCARD + metadata
```

**Phase 2 (Future): Native Time Series Extension**

SQLite extension optimized for append-only time series:
- Append-only B-tree (no rebalancing on insert)
- Automatic time-based partitioning
- Built-in downsampling (1s → 1m → 1h → 1d)
- Retention policies (auto-delete old data)
- Compression (delta encoding timestamps, gorilla for values)
- Aggregation queries: AVG, SUM, MIN, MAX, COUNT, FIRST, LAST, RANGE

**Open source opportunity:** SQLite time series extension doesn't exist in a good form. Could be a standalone project.

---

## Maybe

- Lua scripting (EVAL/EVALSHA)
- XAUTOCLAIM
- ACL system
- Nightly CI for battle tests (`.github/workflows/battle-test.yml`, 1M seeds)

### Soft Delete + PURGE

Mark keys as deleted without removing data. Enables recovery and audit trails.

**Concept:**
```bash
SOFT DEL key [key ...]     # Mark as deleted (recoverable)
UNDELETE key               # Recover soft-deleted key
PURGE key [key ...]        # Permanently delete
PURGE DELETED BEFORE timestamp  # Bulk purge old deletions
```

**Implementation consideration:** Similar to TTL filtering - we already filter on `expire_at`. Could add `deleted_at`:
- NULL = not deleted
- timestamp = soft deleted at this time
- Index: `CREATE INDEX idx_keys_deleted ON keys(db, key, deleted_at)`
- All reads add `WHERE deleted_at IS NULL` (or use partial index)

**Open questions:**
1. Should this require HISTORY to be enabled? Or standalone?
2. Per-key vs global setting?
3. Auto-purge schedules? (`SOFT DEL key PURGE_AFTER 86400`)
4. Performance: every read pays filter cost (but indexed, so minimal)

**Alternative:** Just move row to `soft_deleted` table on SOFT DEL. Cleaner separation, no filter cost on normal reads. UNDELETE moves back.

**Who uses this:**
- Audit/compliance (retain deleted data for N days)
- Undo functionality
- Debugging ("what happened to this key?")
- Paranoid users who want recoverability

## Not Planned

- **Pub/Sub (PUBLISH, SUBSCRIBE, PSUBSCRIBE)** — Doesn't make sense with SQLite file as communication layer. Use Streams instead (XADD/XREAD/XREADGROUP). See docs: "Pub/Sub Migration Guide"
- Cluster mode — Use [walsync](https://github.com/russellromney/walsync) for replication
- Sentinel
- Redis Modules

---

## Documentation Planned

### Performance Guide: "How to Make Redlite Fast"

Key insight: SQLite with large page cache = memory-speed reads with disk durability.

**Topics to cover:**

1. **Cache sizing** — `cache_mb` parameter, sweet spot is "as much RAM as you can spare"
   ```python
   # 50GB cache, terabytes on disk
   db = Redlite("/data/cache.db", cache_mb=50000)
   ```

2. **Separate databases for separate workloads** — Different files = different locks = parallel writes
   ```python
   cache = Redlite("/fast-nvme/hot-cache.db", cache_mb=50000)
   jobs = Redlite("/data/jobs.db", cache_mb=1000)
   events = Redlite("/data/events.db", cache_mb=2000)
   ```

3. **WAL mode** — Concurrent readers, single writer (enabled by default)

4. **NVMe vs SSD vs HDD** — Disk speed matters for cold reads and writes

5. **When to use `:memory:`** — Tests only. File mode with large cache is the sweet spot.

### Pub/Sub Migration Guide

Document how to use Streams for pub/sub patterns:
- Broadcast → XREAD (no consumer group)
- Work queue → XREADGROUP with consumer groups
- Cache invalidation → XADD with MAXLEN
- Live updates → XREAD BLOCK with $ (latest only)

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
- English stemming (porter, built into FTS5)

**Optional extensions:**
```toml
[features]
geo = []          # GEO* commands — uses SQLite's built-in R*Tree (no extra deps)
vectors = []      # V* commands — adds sqlite-vector (~500KB)
fuzzy = []        # Trigram tokenizer + Levenshtein distance for fuzzy search
spellcheck = []   # FT.SPELLCHECK, FT.DICT* — adds spellfix1 (~50KB)
languages = []    # Non-English stemmers — adds Snowball (~200KB)
geoshape = []     # GEOSHAPE field type — enables Geopoly

full = ["vectors", "geo", "fuzzy"]  # Production-ready features
```

**Installation:**
```bash
# Default: full Redis + Search (no geo, no vectors)
cargo install redlite

# With geospatial commands
cargo install redlite --features geo

# With vector search
cargo install redlite --features vectors

# Everything (vectors + geo)
cargo install redlite --features full
```

## Principles

1. **Embedded-first** — Library mode is primary
2. **Disk is cheap** — Don't optimize for memory like Redis
3. **SQLite foundation** — ACID, durability, zero config
4. **Redis-compatible** — Existing clients should work
5. **Extend thoughtfully** — Add features Redis doesn't have
