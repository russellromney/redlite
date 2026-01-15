# Oracle Test Expansion Roadmap - Comprehensive Analysis

## Executive Summary

**Current State (Updated Session 35):**
- **~242 total oracle tests** in `tests/oracle.rs` (~10,600 lines)
- **~157 command-specific tests** (oracle_cmd_*) with ~1,400 test scenarios
- **Average: ~8.9 scenarios per command**

**Target State:**
- **163 commands** supported by Redlite (per COMMANDS.md)
- **~2,445 test scenarios needed** (163 commands × 15 avg configurations)
- **Missing: ~6 commands + ~1,045 additional scenarios**

**Gap Analysis:**
- ✅ Tested: ~157/163 commands (96%)
- ❌ Missing: ~6 commands (4%) - mainly Pub/Sub (server-only)
- ⚠️ Under-configured: Some commands need more scenario coverage
- ⚠️ **CRITICAL**: Session 34 tests don't compile - API mismatches need fixing

---

## Session 35 Progress

**Completed:**
1. ✅ **BLPOP/BRPOP already implemented** in db.rs with tokio async/await
   - Uses broadcast channel notifications (not polling)
   - 100ms sleep between checks with notification wakeup
   - Supports timeout=0 for infinite wait
2. ✅ **Added 12 BLPOP/BRPOP oracle tests** (~350 lines)
   - `oracle_cmd_blpop_immediate` - Data already in list
   - `oracle_cmd_blpop_timeout` - Empty list timeout
   - `oracle_cmd_blpop_multiple_keys` - First non-empty key wins
   - `oracle_cmd_blpop_priority` - Key priority order
   - `oracle_cmd_blpop_binary` - Binary data handling
   - `oracle_cmd_brpop_immediate` - Right pop with data
   - `oracle_cmd_brpop_timeout` - Right pop timeout
   - `oracle_cmd_brpop_multiple_keys` - Multiple keys
   - `oracle_cmd_blpop_concurrent_push` - Data pushed during wait
   - `oracle_cmd_blpop_nonexistent_keys` - Skip non-existent keys
   - `oracle_cmd_blpop_empties_list` - List deletion after empty
3. ✅ **Fixed search.rs** - Added missing `QueryExpr::Fuzzy` case in `expr_to_explain`
4. ✅ **Fixed 9 FT.* tests** - Changed from builder pattern to direct API calls
   - `ft_create(name, on_type, prefixes, schema)` instead of `FtIndex::new().with_*`

**Discovered Issues (Session 34 tests don't compile):**
- FT.* tests: ~~Fixed~~ - now use direct API calls
- FTS.* tests: Method signatures look correct but need verification
- HISTORY.* tests: Field access wrong (`.0` vs `.timestamp_ms`)
- Vector tests: May have QuantizationType path issues

**Tests still need fixing:**
1. `oracle_cmd_fts_index_search` - verify `fts_search` return type
2. `oracle_cmd_fts_deindex_reindex` - verify method signatures
3. `oracle_cmd_fts_info` - verify `FtsStats` field names
4. `oracle_cmd_history_*` (6 tests) - fix `HistoryEntry` field access
5. Vector tests - verify `QuantizationType` import path

---

## Session 34 Progress

**Added 36 new test functions (~1,850 lines):**

1. **Database Commands:** SELECT, VACUUM, AUTOVACUUM
2. **Geo Commands:** GEOSEARCHSTORE (with STOREDIST, BYBOX, etc.)
3. **Stream Commands:** XCLAIM, XINFO (STREAM, GROUPS, CONSUMERS)
4. **FT.* Commands (11 tests):** CREATE, DROPINDEX, _LIST, INFO, ALTER, SEARCH, SUGADD/GET/DEL/LEN, ALIASADD/DEL/UPDATE, SYNUPDATE/SYNDUMP
5. **FTS Commands (4 tests):** ENABLE/DISABLE, INDEX/SEARCH, DEINDEX/REINDEX, INFO
6. **HISTORY Commands (6 tests):** ENABLE/DISABLE, GET, GET AT, LIST KEYS, STATS, CLEAR/PRUNE
7. **Vector Commands (9 tests):** VADD, VREM, VSIM, VCARD, VRANDMEMBER, VGETATTR/VSETATTR, VINFO, VEMB, VDIM

---

## Detailed Progress

### What We've Accomplished

**109 Commands with Oracle Tests:**

1. **STRING Commands** (17 tested)
   - SET, GET, INCR, DECR, INCRBY, DECRBY, INCRBYFLOAT
   - APPEND, STRLEN, GETRANGE, SETRANGE
   - MGET, MSET, GETEX, GETDEL, SETEX, PSETEX

2. **BIT Commands** (4 tested)
   - SETBIT, GETBIT, BITCOUNT, BITOP

3. **LIST Commands** (17 tested)
   - LPUSH, RPUSH, LPUSHX, RPUSHX, LPOP, RPOP
   - LLEN, LINDEX, LSET, LRANGE, LTRIM, LREM
   - LINSERT, LPOS, LMOVE

4. **HASH Commands** (14 tested)
   - HSET, HGET, HMGET, HGETALL, HDEL, HEXISTS
   - HKEYS, HVALS, HLEN
   - HINCRBY, HINCRBYFLOAT, HSETNX, HSCAN

5. **SET Commands** (10 tested)
   - SADD, SREM, SMEMBERS, SISMEMBER, SCARD
   - SDIFF, SINTER, SUNION, SMOVE

6. **SORTED SET Commands** (22 tested)
   - ZADD, ZREM, ZSCORE, ZRANK, ZREVRANK, ZCARD, ZCOUNT
   - ZRANGE, ZRANGE_WITHSCORES, ZREVRANGE
   - ZINCRBY, ZRANGEBYSCORE, ZREVRANGEBYSCORE
   - ZREMRANGEBYRANK, ZREMRANGEBYSCORE
   - ZPOPMIN, ZPOPMAX, ZSCAN

7. **KEY Commands** (16 tested)
   - DEL, EXISTS, EXPIRE, TTL, PTTL, PERSIST
   - RENAME, RENAMENX, TYPE, KEYS, SCAN
   - EXPIREAT, PEXPIRE, UNLINK, COPY

8. **STREAM Commands** (13 tested)
   - XADD, XLEN, XRANGE, XREVRANGE, XTRIM, XDEL
   - XREAD, XGROUP (CREATE/DESTROY), XREADGROUP
   - XACK, XPENDING (summary/detail)

9. **GEO Commands** (6 tested)
   - GEOADD, GEOPOS, GEODIST, GEORADIUS
   - GEORADIUSBYMEMBER, GEOSEARCH, GEOHASH

---

## Remaining Missing Commands (~18 total)

### Blocking Operations (2) - Requires async/timeout design
1. **BLPOP** - Blocking list pop (left)
2. **BRPOP** - Blocking list pop (right)

### Pub/Sub Commands (5) - Server mode only
3. **PUBLISH** - Publish message
4. **SUBSCRIBE** - Subscribe to channel
5. **UNSUBSCRIBE** - Unsubscribe from channel
6. **PSUBSCRIBE** - Pattern subscribe
7. **PUNSUBSCRIBE** - Pattern unsubscribe

### Transaction Commands (1)
8. **MULTI** - Transaction start (EXEC, DISCARD, WATCH, UNWATCH)

### System Commands (4) - Server mode specific
9. **PING** - Server health check
10. **ECHO** - Echo message
11. **CLIENT** - Client connection management
12. **COMMAND** - Command introspection

### Set Store Operations (5) - May already be tested, verify
13. **SDIFFSTORE** - Store set difference
14. **SINTERSTORE** - Store set intersection
15. **SUNIONSTORE** - Store set union
16. **ZINTERSTORE** - Store sorted set intersection
17. **ZUNIONSTORE** - Store sorted set union

### FT Commands Not Yet Tested (2)
18. **FT.AGGREGATE** - Full aggregate pipeline tests
19. **FT.EXPLAIN/FT.PROFILE** - Query explanation

---

## Recently Completed (Session 34)

### ✅ Database Commands
- SELECT - Database selection with isolation tests
- VACUUM - Expired key cleanup
- AUTOVACUUM - Auto-vacuum configuration

### ✅ Geo Commands
- GEOSEARCHSTORE - Store geo search results (STOREDIST, BYBOX, etc.)

### ✅ Stream Extended Commands
- XCLAIM - Claim pending messages
- XINFO STREAM - Stream metadata
- XINFO GROUPS - Consumer groups info
- XINFO CONSUMERS - Consumer info

### ✅ RediSearch/FT Commands (11 tests)
- FT.CREATE, FT.DROPINDEX, FT._LIST, FT.INFO, FT.ALTER
- FT.SEARCH (queries, options, NOCONTENT, WITHSCORES, LIMIT)
- FT.SUGADD/SUGGET/SUGDEL/SUGLEN
- FT.ALIASADD/ALIASDEL/ALIASUPDATE
- FT.SYNUPDATE/SYNDUMP

### ✅ FTS Commands (Redlite native, 4 tests)
- FTS ENABLE/DISABLE (GLOBAL, DATABASE, PATTERN, KEY)
- FTS INDEX/SEARCH
- FTS DEINDEX/REINDEX
- FTS INFO

### ✅ History Commands (6 tests)
- HISTORY ENABLE/DISABLE (with retention types)
- HISTORY GET, HISTORY GET AT
- HISTORY LIST KEYS, HISTORY STATS
- HISTORY CLEAR/PRUNE

### ✅ Vector Commands (9 tests, feature-gated)
- VADD, VREM, VSIM, VCARD, VRANDMEMBER
- VGETATTR/VSETATTR, VINFO, VEMB, VDIM

---

## Missing Configurations Analysis

### Commands Needing Major Expansion (Examples)

#### 1. ZADD (currently 8 scenarios → need 25+)
**Currently missing:**
- `GT` option (only add if score greater)
- `LT` option (only add if score less)
- `CH` option (return changed count instead of added count)
- `INCR` option (increment mode, acts like ZINCRBY)
- **All combinations:** NX+GT, XX+LT, GT+CH, INCR+GT, etc.
- Score boundaries: `-inf`, `+inf`, very large numbers
- Multiple members with same score
- Score precision edge cases

**Total additional scenarios needed:** ~17

#### 2. SET (currently 8 scenarios → need 30+)
**Currently missing:**
- `GET` option (return old value)
- `EXAT` option (expire at unix timestamp in seconds)
- `PXAT` option (expire at unix millisecond timestamp)
- `KEEPTTL` option (retain existing TTL)
- **All combinations:** NX+EX+GET, XX+PXAT+GET, NX+KEEPTTL, etc.
- Overwrite with different options
- TTL edge cases (expired, about to expire)
- Binary safety with all options

**Total additional scenarios needed:** ~22

#### 3. ZRANGE (currently 7 scenarios → need 25+)
**Currently missing:**
- `BYSCORE` variant (range by score instead of rank)
- `BYLEX` variant (range by lexicographical order)
- `REV` modifier (reverse order)
- `LIMIT offset count` (pagination)
- **All combinations:** BYSCORE+WITHSCORES, BYSCORE+REV+LIMIT, BYLEX+REV, etc.
- Inclusive/exclusive bounds for BYSCORE
- Empty ranges, single element ranges
- Negative indices edge cases

**Total additional scenarios needed:** ~18

#### 4. GEOSEARCH (currently 4 scenarios → need 40+)
**Currently missing:**
- `BYBOX` option (instead of BYRADIUS)
- `WITHHASH` option (return geohash)
- `COUNT ANY` option (don't sort, faster)
- `ASC/DESC` sorting
- **All combinations:**
  - Source: FROMMEMBER × FROMLONLAT = 2 options
  - Shape: BYRADIUS × BYBOX = 2 options
  - Output: WITHDIST, WITHCOORD, WITHHASH, none = 4 options
  - Modifiers: COUNT, ASC/DESC, ANY = multiple combinations
  - Total: 2 × 2 × 4 × 4 = **64 potential combinations**
- Different units (m, km, mi, ft) for BYRADIUS
- Various width/height for BYBOX

**Total additional scenarios needed:** ~36

#### 5. LPOS (currently 3 scenarios → need 15+)
**Currently missing:**
- `RANK` option (find nth occurrence)
- `COUNT` option (return multiple matches)
- `MAXLEN` option (limit search length)
- **All combinations:** RANK+COUNT, RANK+MAXLEN, COUNT+MAXLEN, all three
- Different rank values (positive, negative, large)
- COUNT 0 (return all matches)
- Edge cases: element not found, first element, last element

**Total additional scenarios needed:** ~12

#### 6. XREAD/XREADGROUP (currently 4 scenarios → need 20+)
**Currently missing:**
- `BLOCK` timeout (note: skip actual blocking in tests, but test syntax)
- Multiple streams with different IDs
- `COUNT` with very small/large values
- Reading from `$` (latest)
- **Combinations:** BLOCK+COUNT, multiple streams+BLOCK+COUNT
- Edge cases: stream doesn't exist, empty stream, deleted entries
- NOACK option for XREADGROUP

**Total additional scenarios needed:** ~16

#### 7. FT.SEARCH (NOT TESTED → need 50+ scenarios)
**All configurations needed:**
- Query syntax: exact match, prefix, suffix, fuzzy, wildcards
- Boolean operators: AND, OR, NOT, parentheses
- Field-specific searches: @field:query
- Numeric filters: @price:[100 200]
- Geo filters: @location:[lon lat radius unit]
- Tag filters: @tags:{tag1|tag2}
- `RETURN` fields (specific fields only)
- `SORTBY` field ASC/DESC
- `LIMIT offset count` pagination
- `HIGHLIGHT` with tags
- `SUMMARIZE` with LEN, FRAGS, SEPARATOR
- `INFIELDS` count field1 field2
- `INKEYS` count key1 key2
- `SLOP` num (word distance for phrases)
- `INORDER` (enforce word order in phrases)
- `WITHSCORES` (return relevance scores)
- **Combinations:** dozens of option permutations

**Total scenarios needed:** ~50

#### 8. BLPOP/BRPOP (NOT TESTED → need 10+ scenarios)
**All configurations needed:**
- Single key vs multiple keys
- Timeout: 0 (infinite), positive timeout
- Non-blocking behavior (data immediately available)
- List ordering when blocking on multiple lists
- Different data types in values
- Binary safety

**Total scenarios needed:** ~10

---

## Scenario Count by Command (Detailed)

| Command | Current | Needed | Gap | Notes |
|---------|---------|--------|-----|-------|
| **ZADD** | 8 | 25 | +17 | Missing GT, LT, CH, INCR combinations |
| **SET** | 8 | 30 | +22 | Missing GET, EXAT, PXAT, KEEPTTL combos |
| **ZRANGE** | 7 | 25 | +18 | Missing BYSCORE, BYLEX, REV+LIMIT combos |
| **GEOSEARCH** | 4 | 40 | +36 | Missing BYBOX, WITHHASH, COUNT ANY, 64 combos |
| **LPOS** | 3 | 15 | +12 | Missing RANK, COUNT, MAXLEN combinations |
| **XREAD** | 4 | 20 | +16 | Missing BLOCK, COUNT, multiple streams |
| **GEOADD** | 5 | 12 | +7 | Missing NX, XX, CH options |
| **ZADD (variants)** | - | - | - | ZPOPMIN, ZPOPMAX need blocking variants |
| **BLPOP** | 0 | 10 | +10 | Not tested |
| **BRPOP** | 0 | 10 | +10 | Not tested |
| **FT.SEARCH** | 0 | 50 | +50 | Not tested |
| **FT.AGGREGATE** | 0 | 40 | +40 | Not tested |
| **XINFO** | 0 | 15 | +15 | STREAM, GROUPS, CONSUMERS not tested |
| **XCLAIM** | 0 | 12 | +12 | Not tested |
| **HISTORY** | 0 | 30 | +30 | Not tested |
| **FTS** | 0 | 25 | +25 | Not tested |
| **Vectors** | 0 | 35 | +35 | VADD, VREM, VSIM etc not tested |
| **Pub/Sub** | 0 | 20 | +20 | Not tested |
| **(Other 40 commands)** | varies | 800+ | +600+ | Additional configurations needed |

**Total Additional Scenarios Needed: ~1,624**

---

## Comprehensive Testing Requirements

### Total Calculation
- **163 commands** supported
- **Average 15 configurations per command** (conservative estimate)
- **Target: 163 × 15 = 2,445 total scenarios**
- **Current: 821 scenarios**
- **Gap: 1,624 scenarios**

### Breakdown by Complexity

**Simple Commands** (5-8 scenarios each): ~40 commands
- Examples: GET, STRLEN, SCARD, LLEN, HLEN
- Total: 40 × 6 avg = 240 scenarios

**Medium Commands** (10-15 scenarios each): ~60 commands
- Examples: LPUSH, SADD, HDEL, ZREM, DEL
- Total: 60 × 12 avg = 720 scenarios

**Complex Commands** (20-30 scenarios each): ~40 commands
- Examples: SET, ZADD, ZRANGE, LPOS, GEOSEARCH
- Total: 40 × 25 avg = 1,000 scenarios

**Highly Complex** (40-60 scenarios each): ~23 commands
- Examples: FT.SEARCH, FT.AGGREGATE, GEOSEARCH (all combos), XREADGROUP
- Total: 23 × 50 avg = 1,150 scenarios

**Grand Total: 3,110 scenarios** (with thorough coverage)

---

## Priority Implementation Order

### Phase 1: Missing Core Redis Commands (54 commands)
Priority: Critical ⚠️
- Commands that real Redis users expect
- 54 commands × 12 avg = **~650 scenarios**

**High Priority (15 commands):**
1. BLPOP, BRPOP - Blocking operations
2. SPOP, SRANDMEMBER - Random set operations
3. SSCAN - Set scanning
4. SDIFFSTORE, SINTERSTORE, SUNIONSTORE - Set store operations
5. ZINTERSTORE, ZUNIONSTORE - Sorted set store operations
6. PEXPIREAT - Millisecond expiry
7. GEOSEARCHSTORE - Geo with storage
8. PING, ECHO - Connection testing
9. DBSIZE, FLUSHDB - Database management

**Medium Priority (20 commands):**
- XCLAIM, XINFO subcommands - Stream management
- SELECT, VACUUM - Database operations
- Pub/Sub commands - If server mode

**Lower Priority (19 commands):**
- FT.* commands - RediSearch compatibility
- FTS.* commands - Full-text search features
- HISTORY.* commands - Time-travel features
- V* commands - Vector operations

### Phase 2: Expand Existing Command Configurations
Priority: High ⚠️
- Add missing option combinations to 109 tested commands
- **~1,000 additional scenarios**

**Focus areas:**
1. **ZADD** - Add GT, LT, CH, INCR, all combinations
2. **SET** - Add GET, EXAT, PXAT, KEEPTTL, all combinations
3. **ZRANGE** - Add BYSCORE, BYLEX, REV, LIMIT, all combinations
4. **GEOSEARCH** - Add BYBOX, all 64 option combinations
5. **LPOS** - Add RANK, COUNT, MAXLEN combinations
6. **XREAD/XREADGROUP** - Add BLOCK, COUNT, multiple streams
7. **All SCAN commands** - Test MATCH, COUNT, TYPE thoroughly
8. **EXPIRE commands** - Test NX, XX, GT, LT options (Redis 7.0+)
9. **Geo commands** - Test all units, all output formats
10. **Stream commands** - Test all ID formats, MAXLEN/MINID strategies

### Phase 3: Edge Cases & Error Conditions
Priority: Medium
- **~400 additional scenarios**

**Categories:**
1. **Type Mismatches** - Every command on wrong type
2. **Boundary Values** - INT_MAX, INT_MIN, empty strings, huge lists
3. **Unicode & Binary** - UTF-8, null bytes, special characters
4. **Concurrent Access** - Race conditions (if applicable)
5. **Memory Limits** - Very large values, many keys
6. **Expired Keys** - Operations on keys about to expire
7. **Transaction Context** - Commands in MULTI/EXEC
8. **Pub/Sub State** - Commands while subscribed

### Phase 4: Performance & Stress Tests
Priority: Low
- Test with large datasets
- Benchmark against Redis
- Memory usage comparison

---

## Test Quality Standards

Each comprehensive test must include:

### ✅ Required Scenarios
1. **Basic operation** - Happy path
2. **Non-existent key/member** - Handle missing data
3. **Wrong type** - Error handling
4. **Empty values** - Edge case
5. **Binary data** - Null bytes, high bytes (128-255)
6. **Unicode** - UTF-8, emoji, multi-byte characters
7. **Each option individually** - Every flag/option
8. **Common option combinations** - Real-world usage
9. **Boundary values** - MIN, MAX, 0, -1
10. **Error conditions** - Invalid arguments, out of range

### 📊 Coverage Targets
- **Minimum:** 8 scenarios per simple command
- **Target:** 15 scenarios per medium command
- **Goal:** 25+ scenarios per complex command

### 🧪 Test Pattern
```rust
#[test]
fn oracle_cmd_COMMAND() {
    let mut redis = require_redis!();
    let redlite = Db::open_memory().unwrap();
    let _: () = redis::cmd("FLUSHDB").query(&mut redis).unwrap();

    // 1. Non-existent key/member
    // 2. Basic operation
    // 3. Each option individually
    // 4. Common option combinations
    // 5. Edge cases (empty, boundaries)
    // 6. Binary/unicode values
    // 7. Wrong type errors
    // 8. Additional error conditions
}
```

---

## Estimated Effort

### By Phase

| Phase | Commands | Scenarios | Estimated Lines | Time Estimate |
|-------|----------|-----------|-----------------|---------------|
| **Phase 1** | 54 new commands | 650 | ~6,500 lines | 15-20 hours |
| **Phase 2** | 109 expansions | 1,000 | ~8,000 lines | 20-25 hours |
| **Phase 3** | Edge cases | 400 | ~3,000 lines | 8-10 hours |
| **Phase 4** | Stress tests | 100 | ~1,000 lines | 5-8 hours |
| **TOTAL** | **163 commands** | **2,150+** | **~18,500 lines** | **48-63 hours** |

### Current vs Target

| Metric | Current | Target | Gap |
|--------|---------|--------|-----|
| **Commands** | 109 | 163 | +54 |
| **Scenarios** | 821 | 2,445 | +1,624 |
| **Lines of Code** | 7,430 | 25,000+ | +17,570 |
| **Coverage** | 67% | 100% | +33% |
| **Avg Scenarios/Cmd** | 7.5 | 15.0 | +7.5 |

---

## How to Run Tests

```bash
# Start Redis server (required for oracle tests)
redis-server &

# Run all oracle tests (must be sequential)
cargo test --test oracle -- --test-threads=1

# Run specific command test
cargo test --test oracle oracle_cmd_zadd -- --test-threads=1

# Run with verbose output
cargo test --test oracle -- --test-threads=1 --nocapture

# Count total tests
grep -c "^fn oracle" tests/oracle.rs

# Count command tests only
grep -c "^fn oracle_cmd_" tests/oracle.rs

# Count test scenarios (comment lines)
grep -cE "^\s+//" tests/oracle.rs
```

---

## Next Steps

1. **Review this roadmap** - Validate priorities and estimates
2. **Phase 1A** - Add 15 highest-priority missing commands (~200 scenarios)
3. **Phase 1B** - Add remaining 39 missing commands (~450 scenarios)
4. **Phase 2** - Expand all 109 existing commands (~1,000 scenarios)
5. **Phase 3** - Add comprehensive edge case coverage (~400 scenarios)
6. **Continuous** - Update this roadmap as we discover more configurations

---

## Success Metrics

**Completion Criteria:**
- ✅ All 163 supported commands have oracle tests
- ✅ Average 15+ scenarios per command
- ✅ Every command option/flag tested individually
- ✅ Common option combinations covered
- ✅ Binary, unicode, and boundary values tested
- ✅ Error conditions verified
- ✅ 100% compatibility with Redis for tested scenarios

**Quality Gate:**
- Zero oracle test failures
- All tests pass with `--test-threads=1`
- Test execution time < 5 minutes (with Redis)
- No flaky tests (consistent results)
