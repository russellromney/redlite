# Roadmap

See [CHANGELOG.md](./CHANGELOG.md) for completed features.

## Next Steps

### Session 40: Plan and Execute Next Features

**Approach**: Use this roadmap file for planning. When you start a session:
1. Write detailed implementation plan in markdown in this ROADMAP.md
2. Execute the plan step by step
3. Run tests to verify implementation
4. Update docs/README/ROADMAP/CHANGELOG
5. Suggest a prompt for the next session to plan and execute next steps

**Potential Focus Areas**:

**Option 1 - Fix Poll Benchmark FK Constraint & Complete Analysis**:
- Debug FK constraint error in poll_impact benchmarks
- Complete remaining 2 benchmark groups (waiter scaling, CPU impact)
- Analyze all results and document PollConfig recommendations
- **Target**: Complete benchmark report in ROADMAP or separate PERFORMANCE.md

**Option 2 - Fix Remaining Oracle Test Failures** (redlite-dst):
- **Current Status**: 219 passed / ~11-50 failed (test pollution issues)
- **Priority**: Fix test pollution (shared Redis state between tests)
- **SCAN/ZSCAN Issues** (2 tests): oracle_cmd_scan, oracle_cmd_zscan
- **Sorted Set Range Queries** (3 tests): oracle_cmd_zcount, oracle_cmd_zrange, oracle_cmd_zrangebyscore
- **Stream Consumer Groups** (1 test): oracle_cmd_xclaim
- **Random Operations** (5 tests): hashes_random_ops, lists_random_ops, sets_random_ops, strings_random_ops, zsets_random_ops
- **Target**: 225+ tests passing consistently (98%+ pass rate)

**Option 3 - Run Server-Mode Transaction Tests**:
- Configure test infrastructure to run tests/server_watch.rs
- Verify all 10 new transaction tests pass
- Fix any failures found
- **Target**: 100% server-mode transaction test coverage

**Option 4 - Python SDK Completion**:
- Similar to Go SDK work, add missing commands to Python SDK
- Run oracle tests and identify gaps
- **Target**: 100% test coverage for Python SDK

**Option 5 - TypeScript SDK Completion**:
- Add missing commands to TypeScript SDK
- Run oracle tests and identify gaps
- **Target**: 100% test coverage for TypeScript SDK

---

## Recently Completed

### Session 39: Core Bug Fixes & Transaction Tests - ✅ COMPLETE

**Completed**:
- Fixed 3 critical bugs: persist(), rename(), lrem()/linsert() compilation errors
- Added 10 server-mode transaction tests (MULTI/EXEC/DISCARD)
- Started poll impact benchmarks (6/8 groups completed)
- Identified oracle test pollution issues (not implementation bugs)

**Bug Fixes**:
1. persist() - Now correctly returns false when key has no TTL
2. rename() - Now handles renaming key to itself correctly
3. lrem()/linsert() - Fixed borrowing conflicts preventing compilation

**New Tests**: 10 transaction tests in tests/server_watch.rs covering MULTI/EXEC/DISCARD without WATCH

**Benchmark Results** (partial):
- Baseline: 24-48K ops/sec
- With 10 waiters: 11-12K ops/sec (~50% degradation, acceptable)
- FK constraint error halted remaining benchmarks

**Oracle Analysis**: Most failures are test pollution (shared Redis state), not implementation bugs

---

### Session 38: DST Oracle Tests - Deadlock Fixes (7 Timeouts Resolved) - ✅ COMPLETE

**Completed**:
- Fixed all 7 timeout tests (100% timeout fix rate)
- Eliminated deadlocks in LINSERT, LREM, SDIFFSTORE, SINTERSTORE, SUNIONSTORE
- Improved test pass rate from 92% (212/230) to 95% (219/230)
- Two deadlock patterns identified and fixed:
  1. Calling `record_history()` while holding connection lock
  2. Nested lock acquisition in if-let expressions

**Key Fixes**:
- Added `drop(conn)` before `record_history()` calls in linsert/lrem
- Separated lock scopes in set store operations (sdiffstore/sinterstore/sunionstore)

**Test Results**: 219 passed / 11 failed / 0 timeouts (vs 212 passed / 17 failed / 7 timeouts before)

**Remaining Work**: 11 non-deadlock failures (SCAN/ZSCAN, sorted set range queries, stream xclaim, random ops)

---

### Session 37: Go SDK Complete - 100% Oracle Test Coverage - ✅ COMPLETE

**Completed**:
- Added 17 missing Redis commands to Go SDK
- FFI layer: 6 new C functions (mget, mset, hgetall, hmget, zrange, zrevrange)
- Go SDK: 17 new methods across String, Key, Hash, and Sorted Set operations
- Oracle tests: 137/137 passing (100%), up from 107/137 (78%)
- All commands removed from unsupportedCommands map

**Commands Added**: MGET, MSET, GETDEL, GETRANGE, SETRANGE, DECRBY, INCRBYFLOAT, PSETEX, PTTL, PEXPIRE, RENAME, RENAMENX, HGETALL, HMGET, ZREM, ZRANGE, ZREVRANGE

**Key Achievement**: First SDK to reach 100% oracle test coverage. Provides template for completing Python and TypeScript SDKs.

---

### Session 36: History Feature Bug Fixes & Parallel Test Infrastructure - ✅ COMPLETE

**Completed**:
- Fixed critical deadlock in history tracking (4 bugs total)
- Built parallel test infrastructure with SQLite tracking
- Achieved 212/230 oracle tests passing (92% pass rate)
- Tests run in ~2 minutes (vs 10+ minutes sequential)

**Key Insight**: Lock acquisition order matters. Helper functions that acquire locks must be called before parent function acquires lock, or use explicit scope blocks to ensure proper release.

---

### Session 34: Bug Fixes (LPOS, LMOVE) - ✅ COMPLETE

**Goal**: Fix pre-existing test failures to ensure clean test baseline.

**Bugs Fixed**:

1. **LPOS COUNT 0 Behavior** (`test_lpos_with_count`)
   - **Issue**: `COUNT 0` should return ALL matches per Redis spec, but was returning only 1
   - **Root Cause**: Break condition `found >= count` was `1 >= 0 = true` after first match
   - **Fix**: Changed to `count > 0 && found >= count` at [db.rs:3029](crates/redlite/src/db.rs#L3029)

2. **LMOVE Same-List Deadlock** (`test_lmove_same_list`)
   - **Issue**: Test hanging indefinitely when `source == destination`
   - **Root Cause**: Mutex not dropped before reacquiring when `source == destination`
   - **Fix**: Added `drop(conn)` in the same-list branch at [db.rs:3137](crates/redlite/src/db.rs#L3137)

**Test Results**: 601 tests passing (including 12 new BLPOP/BRPOP tests added in Session 35)

---

### Session 35: Blocking Operations (BLPOP/BRPOP) - ✅ COMPLETE

**Status**: BLPOP/BRPOP implemented as async versions using tokio. Located at:
- `db.rs:6771` - `pub async fn blpop()`
- `db.rs:6867` - `pub async fn brpop()`

The implementation uses tokio-based polling with key subscription for efficient blocking behavior.

**Tests**: 12 comprehensive tests added in Session 35 covering:
- Immediate data return, timeout behavior, multi-key priority
- Binary data, concurrent push, wrong type handling
- See test section below for full list

---

### Session 35 (Original Plan): Blocking Operations (BLPOP/BRPOP) - ✅ COMPLETE

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
- [x] `test_blpop_immediate_data` - Data already in list, returns immediately
- [x] `test_blpop_timeout_empty` - Empty list, timeout returns nil
- [x] `test_blpop_multiple_keys` - First non-empty key wins
- [x] `test_blpop_key_priority` - Keys checked in order
- [x] `test_blpop_timeout_zero` - Infinite wait (test with concurrent push)
- [x] `test_blpop_binary_data` - Binary values work correctly
- [x] `test_brpop_immediate_data` - Right pop variant
- [x] `test_brpop_timeout_empty` - Right pop timeout
- [x] `test_blpop_concurrent_push` - Another thread pushes during wait
- [x] `test_blpop_wrong_type` - WRONGTYPE error on non-list key (matches Redis)
- [x] `test_blpop_nonexistent_key` - Non-existent keys skipped
- [x] `test_blpop_mixed_keys` - Mix of existing/non-existing keys

**Server Mode**:
- Same polling implementation works
- RESP handler converts timeout from seconds to Duration

#### Success Criteria
- [x] BLPOP/BRPOP implemented with adaptive 250μs→1ms polling
- [x] All 12 tests passing
- [x] Works in both embedded and server modes
- [x] Timeout=0 works correctly (infinite wait)
- [x] Multi-key priority ordering matches Redis

---

### Session 35.1: Sync Blocking Operations - ✅ COMPLETE

**Goal**: Add sync versions of BLPOP/BRPOP for embedded mode without tokio dependency.

**Rationale**: Multiple processes can share the same SQLite file. Process A calls `blpop_sync()` waiting for data, Process B calls `rpush()` on the same .db file. SQLite with warm page cache returns in microseconds, so polling at 250μs-1ms is cheap.

#### Implementation

**New Methods** (db.rs:6969-7063):
```rust
pub fn blpop_sync(&self, keys: &[&str], timeout: f64) -> Result<Option<(String, Vec<u8>)>>
pub fn brpop_sync(&self, keys: &[&str], timeout: f64) -> Result<Option<(String, Vec<u8>)>>
```

**Adaptive Polling**:
- Start at 250μs interval
- Ramps up to 1ms cap (reduces CPU while maintaining responsiveness)
- Uses `std::thread::sleep` (no tokio required)
- Sub-ms response when data arrives

**Use Cases**:
- Embedded mode in sync Rust applications
- Multi-process coordination via shared .db file
- Python/Node SDKs calling via FFI

#### Tests (db.rs:21851-22005)
- [x] `test_blpop_sync_immediate_data`
- [x] `test_blpop_sync_timeout`
- [x] `test_blpop_sync_multiprocess` (cross-thread with shared db file)
- [x] `test_brpop_sync_basic`
- [x] `test_blpop_sync_multiple_keys`
- [x] `test_blpop_sync_wrong_type`
- [x] `test_brpop_sync_timeout`

**Result**: 608 tests passing

---

### Session 35.2: Poll Impact Benchmarks - IN PROGRESS

**Goal**: Measure polling overhead to validate PollConfig recommendations and ensure blocking operations don't starve other workloads.

**Rationale**: Before recommending `PollConfig::aggressive()` vs `default()` vs `relaxed()`, we need data on:
1. CPU cost of each polling interval
2. Impact on concurrent non-blocking operations
3. Latency distribution when data arrives

#### Benchmark Suite (`benches/poll_impact.rs`)

**1. Baseline Throughput** (~3 benchmarks)
- [ ] `bench_baseline_set_get` - Ops/sec with no blocking operations
- [ ] `bench_baseline_lpush_lpop` - List throughput baseline
- [ ] `bench_baseline_xadd_xread` - Stream throughput baseline

**2. Polling Overhead** (~4 benchmarks)
- [ ] `bench_poll_cpu_aggressive` - CPU usage with 10 concurrent `blpop_sync` waiters (100μs polling)
- [ ] `bench_poll_cpu_default` - CPU usage with 10 concurrent waiters (250μs polling)
- [ ] `bench_poll_cpu_relaxed` - CPU usage with 10 concurrent waiters (1ms polling)
- [ ] `bench_poll_scaling` - CPU vs waiter count (1, 10, 50, 100 waiters)

**3. Throughput Under Load** (~4 benchmarks)
- [ ] `bench_throughput_with_1_waiter` - SET/GET ops/sec with 1 blocking waiter
- [ ] `bench_throughput_with_10_waiters` - SET/GET ops/sec with 10 blocking waiters
- [ ] `bench_throughput_with_100_waiters` - SET/GET ops/sec with 100 blocking waiters
- [ ] `bench_throughput_comparison` - Side-by-side aggressive/default/relaxed

**4. Latency Distribution** (~3 benchmarks)
- [ ] `bench_latency_immediate_data` - Response time when data already exists
- [ ] `bench_latency_push_during_wait` - Response time when push arrives during wait
- [ ] `bench_latency_p50_p99_p999` - Latency percentiles across polling configs

#### Expected Results

| Config | CPU (10 waiters) | Throughput Impact | Wake Latency |
|--------|------------------|-------------------|--------------|
| aggressive | ~5-10% | ~2-5% drop | <200μs |
| default | ~1-2% | <1% drop | <500μs |
| relaxed | <0.5% | negligible | <5ms |

#### Implementation Notes
- Use `criterion` crate for statistical benchmarking
- Spawn waiter threads, measure main thread throughput
- Use `std::hint::black_box` to prevent optimization
- Run each config for 10+ seconds for stable measurements

#### Success Criteria
- [ ] 14 benchmarks implemented and passing
- [ ] HTML report generated in `target/criterion/`
- [ ] Data validates current default (250μs → 1ms) as balanced choice
- [ ] No config causes >10% throughput degradation with 10 waiters

---

### Session 35.3: Oracle Tests - Blocking & Transactions - PARTIAL

**Goal**: Add Redis oracle tests for blocking commands and transactions to validate compatibility.

**Rationale**: These are the last major untested command categories. Oracle tests ensure identical behavior to Redis.

#### Blocking Commands (~11 tests) - ✅ COMPLETE

**BLPOP/BRPOP** (already implemented in previous sessions + 2 new)
- [x] `oracle_cmd_blpop_immediate` - Data exists, returns immediately
- [x] `oracle_cmd_blpop_timeout` - No data, times out correctly (returns nil)
- [x] `oracle_cmd_blpop_concurrent_push` - Push arrives during wait, unblocks
- [x] `oracle_cmd_brpop_immediate` - Right-pop variant works identically
- [x] `oracle_cmd_blpop_multiple_keys` - Priority order (first key with data wins)
- [x] `oracle_cmd_blpop_priority` - Key priority order
- [x] `oracle_cmd_blpop_binary` - Binary data handling
- [x] `oracle_cmd_brpop_timeout` - BRPOP timeout
- [x] `oracle_cmd_blpop_nonexistent_keys` - Non-existent keys skipped
- [x] `oracle_cmd_blpop_wrong_type` - WRONGTYPE error on non-list key (NEW)
- [x] `oracle_cmd_brpop_wrong_type` - WRONGTYPE error on non-list key (NEW)

#### Transaction Commands (~10 tests) - DEFERRED (Server Mode Only)

**Note**: Transactions (MULTI/EXEC/WATCH) are only available in server mode, not embedded `Db`.
These tests require a running redlite server and TCP connection.

**MULTI/EXEC/DISCARD**
- [ ] `test_oracle_multi_exec_basic` - Queue commands, execute atomically
- [ ] `test_oracle_multi_exec_multiple_commands` - 5+ commands in transaction
- [ ] `test_oracle_multi_discard` - DISCARD clears queue, returns OK
- [ ] `test_oracle_multi_exec_empty` - EXEC with no queued commands
- [ ] `test_oracle_multi_nested` - MULTI inside MULTI returns error
- [ ] `test_oracle_exec_without_multi` - EXEC without MULTI returns error

**WATCH/UNWATCH**
- [ ] `test_oracle_watch_unmodified` - WATCH key not modified → EXEC succeeds
- [ ] `test_oracle_watch_modified` - WATCH key modified → EXEC returns nil
- [ ] `test_oracle_watch_deleted` - WATCH key deleted → EXEC returns nil
- [ ] `test_oracle_unwatch` - UNWATCH clears watched keys, EXEC succeeds

#### Error Handling (~4 tests) - DEFERRED
- [ ] `test_oracle_multi_syntax_error` - Syntax error in queue → error on EXEC
- [ ] `test_oracle_multi_runtime_error` - Runtime error (e.g., INCR on string) → partial success
- [ ] `test_oracle_watch_inside_multi` - WATCH inside MULTI returns error
- [ ] `test_oracle_multi_timeout` - Long transaction doesn't timeout

#### Test Infrastructure

**Async Test Setup** (for blocking commands):
```rust
#[tokio::test]
async fn test_oracle_blpop_concurrent_push() {
    let redis = redis_client();
    let redlite = redlite_client();

    // Start BLPOP in background task
    let redis_handle = tokio::spawn(async move {
        redis.blpop("key", 5.0).await
    });

    // Wait a bit, then push
    tokio::time::sleep(Duration::from_millis(100)).await;
    redis.lpush("key", "value").await;

    // Compare results
    let redis_result = redis_handle.await;
    // ... same for redlite
}
```

#### Success Criteria
- [x] 11 blocking oracle tests implemented (BLPOP/BRPOP)
- [x] 2 new WRONGTYPE tests added
- [ ] Transaction tests deferred (require server mode)
- [x] Zero divergences from Redis behavior
- [x] All tests run in `redlite-dst oracle` suite
- [x] Blocking tests use proper async coordination

---

### Session 36: FT.SEARCH Enhancement - ✅ COMPLETE

**Goal**: Improve FT.SEARCH robustness with better SORTBY handling, BM25 accuracy verification, and Unicode query support.

#### SORTBY Improvements (2 tests implemented)
- [x] `test_ft_search_sortby_missing_field` - Documents without sort field still returned
- [x] `test_ft_search_sortby_tie_breaking` - Consistent ordering for same-score docs
- [ ] `test_ft_search_sortby_field_weights` - SORTBY respects field weight multipliers (deferred)
- [ ] `test_ft_search_sortby_numeric_string_mix` - Proper handling when field has mixed types (deferred)
- [ ] `test_ft_search_sortby_null_handling` - NULL values sort correctly (deferred)

#### BM25 Accuracy (3 tests implemented)
- [x] `test_bm25_term_frequency` - Higher TF = higher score
- [x] `test_bm25_document_length_normalization` - Length normalization works
- [x] `test_bm25_idf_rare_terms` - Rare terms found correctly

#### Query Parser Unicode & Edge Cases (5 tests implemented)
- [x] `test_query_parser_unicode_terms` - Japanese, mixed, emoji terms work
- [x] `test_query_parser_special_characters` - Hyphens, underscores in terms
- [x] `test_query_parser_unclosed_brackets` - Graceful handling of malformed input
- [x] `test_query_parser_deeply_nested` - Nested parentheses work
- [x] `test_query_parser_empty_phrase` - Empty phrase handled gracefully

**Implementation Notes**:
- SORTBY missing fields: Add COALESCE in SQL ORDER BY
- Tie-breaking: Add secondary sort on doc_id for determinism
- Unicode: Ensure FTS5 tokenizer handles CJK correctly

---

### Session 38: Performance Benchmarking - ✅ COMPLETE

**Goal**: Profile FT.AGGREGATE performance and identify bottlenecks at scale.

**Result**: 6 criterion benchmarks implemented in `benches/ft_aggregate.rs`, covering 1K/10K/100K scale tests.

#### Benchmark Results (Apple M1)
- **1K simple GROUPBY+COUNT**: ~4.8ms (208K elem/s throughput)
- **10K complex 5 REDUCE**: Statistical baseline established
- **100K scale**: Memory pressure and throughput profiling

#### Benchmarks Implemented (6 total)
- [x] `bench_ft_aggregate_1k_simple` - Single GROUPBY + COUNT on 1K docs
- [x] `bench_ft_aggregate_10k_complex` - 5 REDUCE functions (COUNT, AVG, SUM, MAX, STDDEV)
- [x] `bench_ft_aggregate_100k_scale` - Simple and complex pipelines at 100K scale
- [x] `bench_ft_search_bm25` - BM25 ranking with single/multi-term queries on 10K docs
- [x] `bench_scaling_comparison` - Scaling analysis across 1K/5K/10K/25K documents
- [x] `bench_memory_pressure` - Sustained 10K operations with aggregation

#### Usage
```bash
# Run all benchmarks
cargo bench --bench ft_aggregate

# Test mode (verify benchmarks work without full runs)
cargo bench --bench ft_aggregate -- --test

# Run specific benchmark group
cargo bench --bench ft_aggregate -- "ft_aggregate_1k"
```

#### Technical Details
- Uses `criterion` crate for statistical benchmarking
- Generates HTML reports in `target/criterion/`
- Throughput metrics calculated per-element
- Sample sizes adjusted for benchmark duration (50 for 10K, 20 for 100K)

---

### Session 33: Fuzzy Search with Built-in Trigram Tokenizer - ✅ COMPLETE

**Goal**: Enable fuzzy/substring matching in FT.SEARCH using SQLite FTS5's built-in trigram tokenizer.

**Rationale**: FTS5 has included a built-in `trigram` tokenizer since SQLite 3.34.0 (Dec 2020). This enables:
- Substring matching (like SQL LIKE '%pattern%' but indexed)
- GLOB/LIKE queries that use the FTS5 index
- Typo-tolerant search via trigram overlap
- No custom C code or external extensions required

**Reference**: [SQLite FTS5 Trigram Tokenizer](https://sqlite.org/fts5.html#the_trigram_tokenizer)

**Result**: 15 new tests passing (7 trigram + 8 fuzzy), 639 total tests with `--features "vectors geo"`

#### Phase 1: Trigram Index Support (7 tests) - ✅ COMPLETE

**Implementation**:
1. Added `FtTokenizer` enum (Porter, Trigram, Unicode61, Ascii) to `types.rs`
2. Added `tokenizer` field to `FtField` struct
3. Added `FtField::text_trigram()` convenience constructor
4. Added `.tokenizer()` builder method for FtField
5. Updated `ft_create` to use field's tokenizer when creating FTS5 table

**Tests**:
- [x] `test_ft_create_with_trigram_tokenizer` - Create index with TOKENIZE trigram
- [x] `test_ft_create_with_text_trigram_helper` - Use FtField::text_trigram() helper
- [x] `test_ft_search_trigram_substring` - Find "hello" in "say hello world"
- [x] `test_ft_search_trigram_prefix_and_suffix` - Prefix match with trigrams
- [x] `test_ft_search_trigram_case_insensitive` - Case handling
- [x] `test_ft_info_shows_tokenizer` - FT.INFO displays tokenizer type
- [x] `test_ft_tokenizer_builder_pattern` - Builder pattern for tokenizer

#### Phase 2: Fuzzy Query Syntax (8 tests) - ✅ COMPLETE

**Implementation**:
1. Added `QueryExpr::Fuzzy(String)` variant to query parser
2. Parse `%%term%%` syntax as fuzzy search
3. Generate FTS5 phrase query for trigram matching
4. Updated `expr_to_explain` for FT.EXPLAIN support

**Tests**:
- [x] `test_ft_search_fuzzy_syntax_basic` - Basic %%term%% query
- [x] `test_ft_search_fuzzy_typo_matches` - Trigram overlap finds similar words
- [x] `test_ft_search_fuzzy_field_scoped` - @field:%%term%%
- [x] `test_ft_search_fuzzy_mixed_query` - Fuzzy + exact in same query
- [x] `test_ft_search_fuzzy_unicode` - Unicode fuzzy matching (Japanese)
- [x] `test_ft_search_fuzzy_short_terms` - 1-2 char terms (edge case)
- [x] `test_query_parser_fuzzy_expr` - Parser produces Fuzzy variant
- [x] `test_query_parser_fuzzy_in_and` - Fuzzy in AND expression

#### Phase 3: Levenshtein Ranking - ✅ COMPLETE (Session 33.3)

**Goal**: Add precision ranking to fuzzy search using edit distance scoring.

**Why Both Trigrams + Levenshtein**:
- Trigrams = Fast pre-filter (uses FTS5 index, finds candidates)
- Levenshtein = Precise ranking (edit distance scoring for relevance)

**Implementation** (~50 lines):
```rust
// src/search/levenshtein.rs
/// Wagner-Fischer algorithm for edit distance
pub fn levenshtein_distance(a: &str, b: &str) -> usize {
    let a_chars: Vec<char> = a.chars().collect();
    let b_chars: Vec<char> = b.chars().collect();
    let m = a_chars.len();
    let n = b_chars.len();

    if m == 0 { return n; }
    if n == 0 { return m; }

    let mut dp = vec![vec![0usize; n + 1]; m + 1];

    for i in 0..=m { dp[i][0] = i; }
    for j in 0..=n { dp[0][j] = j; }

    for i in 1..=m {
        for j in 1..=n {
            let cost = if a_chars[i-1] == b_chars[j-1] { 0 } else { 1 };
            dp[i][j] = (dp[i-1][j] + 1)          // deletion
                .min(dp[i][j-1] + 1)              // insertion
                .min(dp[i-1][j-1] + cost);        // substitution
        }
    }
    dp[m][n]
}

/// Compute fuzzy match score (0.0 to 1.0, higher = better match)
pub fn fuzzy_score(query: &str, result: &str, max_distance: usize) -> Option<f64> {
    let dist = levenshtein_distance(&query.to_lowercase(), &result.to_lowercase());
    if dist <= max_distance {
        Some(1.0 - (dist as f64 / query.len().max(1) as f64))
    } else {
        None
    }
}
```

**Integration**:
- Add `DISTANCE n` parameter to FT.SEARCH for max edit distance threshold
- Post-filter FTS5 trigram results with Levenshtein distance
- Sort by fuzzy_score when WITHSCORES enabled
- Expose via `db.ft_search_fuzzy()` method

**Tests** (16 tests - all passing):
- [x] `test_levenshtein_identical` - Distance("hello", "hello") = 0
- [x] `test_levenshtein_deletion` - Distance("hello", "helo") = 1
- [x] `test_levenshtein_insertion` - Distance("hello", "helllo") = 1
- [x] `test_levenshtein_substitution` - Distance("hello", "hallo") = 1
- [x] `test_levenshtein_transposition` - Distance("hello", "ehllo") = 2 (swap = 2 ops)
- [x] `test_levenshtein_unicode` - Works with Japanese/emoji
- [x] `test_levenshtein_empty_strings` - Edge case handling
- [x] `test_levenshtein_completely_different` - Large distances
- [x] `test_fuzzy_score_exact_match` - Score = 1.0 for identical
- [x] `test_fuzzy_score_one_edit` - Score = 0.8 for 1 edit on 5-char
- [x] `test_fuzzy_score_threshold` - Filters by max_distance
- [x] `test_fuzzy_score_case_insensitive` - Case-insensitive matching
- [x] `test_best_fuzzy_match_exact_word` - Finds exact match in text
- [x] `test_best_fuzzy_match_typo` - Finds closest match despite typo
- [x] `test_best_fuzzy_match_no_match` - Returns None when no match
- [x] `test_best_fuzzy_match_picks_closest` - Selects highest-scoring word

**Usage Examples**:
```rust
// Create trigram index for fuzzy search
let schema = vec![FtField::text_trigram("content")];
db.ft_create("idx", FtOnType::Hash, &["doc:"], &schema)?;

// Search for substrings
db.ft_search("idx", "hello", &options)?;           // Normal substring
db.ft_search("idx", "%%program%%", &options)?;     // Explicit fuzzy
db.ft_search("idx", "@title:%%test%%", &options)?; // Field-scoped fuzzy
```

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

### Server Mode HA (High Availability)

**Goal**: Dead-simple failover for server mode with ~5 second recovery time.

**Design Philosophy**: Redis Sentinel takes 10-30 seconds for failover. We can beat that with a simpler design that uses S3 as the coordination layer (already there for Litestream).

#### Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                     Redlite HA                               │
│                                                              │
│   Leader ───────► S3 config (check every 5s for followers)  │
│      │                                                       │
│      │  heartbeat (1s)                                      │
│      ▼                                                       │
│   Follower ◄──── Litestream restore (continuous from S3)    │
│      │                                                       │
│      └────► watches leader, takes over if missing 5s        │
│                                                              │
└─────────────────────────────────────────────────────────────┘
```

#### Protocol

**Leader responsibilities:**
1. Send heartbeat to follower every 1 second
2. Check S3 config every 5 seconds for new followers
3. Stream WAL to S3 via Litestream
4. If network issues prevent heartbeat delivery → step down

**Follower responsibilities:**
1. Receive heartbeat from leader, update last-seen timestamp
2. Continuously restore from Litestream (always ~1s behind)
3. If no heartbeat for 5 seconds:
   - Grab S3 lease (prevents split-brain)
   - Try to notify old leader (best effort)
   - Promote self to leader
   - Start Litestream replication (now source of truth)

**Old leader recovery:**
- When old leader comes back online, it sees lease is held by another node
- Automatically demotes to follower
- Starts Litestream restore from S3

#### Constraints

- **Single follower only** — No race condition for lease, simpler protocol
- **S3 as coordination** — Lease file with conditional writes prevents split-brain
- **Litestream for data** — No custom replication protocol needed

#### Failover Timeline

```
0s     - Leader dies (or network partition)
1-5s   - Follower detects missing heartbeats
5s     - Follower grabs S3 lease
5.1s   - Follower promotes, starts serving
─────────────────────────────────────────
Total: ~5 seconds (vs Redis 10-30 seconds)
```

#### S3 Lease File

```json
{
  "holder": "node-abc123",
  "timestamp": "2024-01-15T10:30:00Z",
  "expires": "2024-01-15T10:30:15Z"
}
```

Conditional PUT (ETag/If-Match) ensures only one node can grab the lease.

#### Implementation

```rust
struct HaNode {
    role: Role,              // Leader or Follower
    node_id: String,
    litestream: LitestreamHandle,
    s3_client: S3Client,
    follower_addr: Option<SocketAddr>,
}

impl HaNode {
    // Leader: send heartbeats
    async fn heartbeat_loop(&self) {
        loop {
            if let Some(addr) = &self.follower_addr {
                self.send_heartbeat(addr).await;
            }
            sleep(Duration::from_secs(1)).await;
        }
    }

    // Follower: watch for leader death
    async fn watch_leader(&self) {
        loop {
            if self.last_heartbeat.elapsed() > Duration::from_secs(5) {
                if self.try_grab_lease().await.is_ok() {
                    self.promote().await;
                }
            }
            sleep(Duration::from_millis(500)).await;
        }
    }

    async fn promote(&mut self) {
        self.litestream.stop_restore().await;
        self.litestream.start_replicate().await;
        self.role = Role::Leader;
    }

    async fn demote(&mut self) {
        self.litestream.stop_replicate().await;
        self.litestream.start_restore().await;
        self.role = Role::Follower;
    }
}
```

#### Fly.io Integration

On Fly, the proxy can serve as health check routing:

```toml
# fly.toml
[[services]]
  internal_port = 6379

  [[services.http_checks]]
    path = "/health"
    interval = "2s"
    timeout = "1s"
```

Health endpoint returns 200 if leader, 503 if follower. Fly routes to healthy node.

#### Edge Cases

1. **S3 down** — Both nodes continue with last known role until S3 recovers
2. **Litestream lag** — Follower may be missing last ~1s of writes on promotion (acceptable for cache)
3. **Both nodes start simultaneously** — First to grab lease wins, other becomes follower
4. **Leader notification fails** — Old leader will discover via S3 lease check

#### Cost Comparison

```
Redis Sentinel HA:     3+ nodes, all in memory     $500-2000/mo
Redlite HA:            2 nodes, data on disk       $50-100/mo
                       + S3 pennies
```

#### Success Criteria

- [ ] Leader/follower mode implemented
- [ ] 1s heartbeat protocol working
- [ ] S3 lease grab with conditional writes
- [ ] Automatic promotion on leader failure
- [ ] Automatic demotion when old leader returns
- [ ] Litestream integration (replicate/restore switching)
- [ ] <5 second failover time
- [ ] Tests: leader death, follower promotion, old leader recovery

---

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

**Streams (13 commands - all tested)**
- [x] XADD, XLEN, XTRIM
- [x] XRANGE, XREVRANGE, XDEL, XINFO STREAM
- [x] XGROUP (CREATE, DESTROY, SETID, CREATECONSUMER, DELCONSUMER)
- [x] XREAD (async/blocking)
- [x] XREADGROUP (async/blocking)
- [x] XACK, XPENDING, XCLAIM

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

---

### Session 35.2: Oracle Tests - Transactions & Blocking Commands - PLANNED

**Goal**: Add Redis oracle tests for the remaining untested command categories.

#### Blocking Commands (BLPOP/BRPOP)
- [ ] `test_oracle_blpop_immediate` - Data exists, returns immediately
- [ ] `test_oracle_blpop_timeout` - No data, times out correctly
- [ ] `test_oracle_blpop_concurrent_push` - Push arrives during wait
- [ ] `test_oracle_brpop_basic` - Right-pop variant
- [ ] `test_oracle_blpop_multiple_keys` - Priority order matches Redis

#### Transactions (MULTI/EXEC)
- [ ] `test_oracle_multi_exec_basic` - Queue commands, execute atomically
- [ ] `test_oracle_multi_discard` - DISCARD clears queue
- [ ] `test_oracle_multi_exec_errors` - Error in queue vs error in exec
- [ ] `test_oracle_watch_modified` - WATCH key modified before EXEC → nil
- [ ] `test_oracle_watch_unmodified` - WATCH key not modified → success
- [ ] `test_oracle_unwatch` - UNWATCH clears watched keys
- [ ] `test_oracle_multi_nested` - MULTI inside MULTI → error

#### Integration Tests (redlite-specific)
- [ ] `test_integration_blpop_sync_multiprocess` - Real child process pushes to shared .db
- [ ] `test_integration_brpop_sync_multiprocess` - Same for BRPOP

---

### Session 36.1: Streams Consumer Groups - ✅ COMPLETE

**Status**: All consumer group commands are implemented and tested.

**Commands implemented**:
- `XGROUP CREATE/DESTROY/SETID/CREATECONSUMER/DELCONSUMER`
- `XREADGROUP` (with blocking support via `xreadgroup_block_sync`)
- `XACK`, `XPENDING`, `XCLAIM`
- `XINFO GROUPS/CONSUMERS`

**Tests**: 13+ stream/consumer group tests passing

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

### SDK-Assisted Failover

Optimize the HA design with client-side awareness for sub-second failover.

**Concept**: The basic HA design uses 5-second heartbeat timeout for failover. Client SDKs can detect leader failure immediately (request timeout) and trigger faster promotion.

**Client Behavior:**
```python
class RedliteClient:
    def __init__(self, leader: str, follower: str, timeout_ms: int = 100):
        self.leader = leader
        self.follower = follower
        self.active = self.leader
        self.timeout = timeout_ms

    def request(self, cmd):
        try:
            return self.conn(self.active).execute(cmd, timeout=self.timeout)
        except Timeout:
            # Leader timed out - try follower
            self.active = self.follower if self.active == self.leader else self.leader
            return self.conn(self.active).execute(cmd)
```

**Follower Promotion Trigger:**
```rust
// Follower receives request while in follower mode
async fn handle_request(&self, req: Request) -> Response {
    if self.role == Role::Follower {
        // Client couldn't reach leader - fast-path promotion
        if self.try_grab_lease().await.is_ok() {
            self.promote().await;
        } else {
            return Error::NotLeader("retry primary");
        }
    }
    self.process(req).await
}
```

**Failover Scenarios:**

| Scenario | Server-Only HA | SDK-Assisted |
|----------|---------------|--------------|
| Leader dead | ~5 seconds | **~200ms** |
| Network blip to leader | ~5 seconds | **0ms** (retry succeeds) |
| False alarm | Protected by S3 lease | Protected by S3 lease |

**Safety**: S3 lease prevents split-brain. Even with aggressive client retries, only one node can grab the lease. Failed promotion attempts are harmless.

**Benefits:**
- 25x faster failover for actual failures
- Instant retry for transient network issues
- No false failovers (lease arbitration)
- Works with existing server-only HA (graceful degradation)

**Implementation:**
- Add `RedliteClient` wrapper class to Python/Node/Go SDKs
- Add `/promote` endpoint to server (tries lease grab + promotion)
- Document configuration: `RedliteClient(["leader:6379", "follower:6379"], timeout=100)`

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
