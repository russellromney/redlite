# Session 18.2: Profiling & Analysis Report

**Generated:** 2026-01-12
**Status:** Bottlenecks identified - Ready for optimization in Session 18.3

## Executive Summary

Analysis of the redlite codebase identified **ONE CRITICAL architectural bottleneck** and **SEVEN MEDIUM optimization opportunities**. The critical issue affects all concurrent workloads, while the medium issues compound latency and throughput for specific operations.

### Key Findings

- **Global serialization point**: Single `Mutex<Connection>` forces all database operations to wait sequentially
- **Redundant database queries**: 1000+ unnecessary queries for bulk operations (HSET with 1000 fields = 1000+ queries)
- **Per-element loop queries**: HSET, ZADD, and other bulk operations execute N+3 database queries instead of 1-2
- **Concurrent throughput degradation**: 16-thread concurrent test shows linear degradation due to lock contention
- **Inefficient query patterns**: Existence checks before INSERT operations duplicate SQLite's built-in conflict handling

## Bottleneck Analysis

### CRITICAL: Global Serialization - Single Mutex<Connection>

**Location:** `src/db.rs` lines 14-27
**Severity:** ⚠️⚠️⚠️ **CRITICAL**
**Impact on Benchmarks:** Concurrent operations (16 threads) scale linearly down; cannot reach 10,000+ QPS goal with multiple threads

```rust
// Current architecture
struct DbCore {
    conn: Mutex<Connection>,  // ← ALL operations lock this single connection
    // ... other fields ...
}

// Every operation does:
pub fn set(&self, key: &str, value: &[u8], ttl: Option<Duration>) -> Result<()> {
    let conn = self.core.conn.lock().unwrap_or_else(|e| e.into_inner());
    // ... operation ...
}
```

**Why it matters:**
- Each thread must wait for all prior threads to release the lock
- Benchmark results show: 1 thread = 363 ops/ms, 16 threads = 22 ops/ms (16.5x degradation)
- This is the primary limitation preventing concurrent throughput from scaling

**Expected improvement with connection pooling:** 3-5x concurrent throughput

### HIGH PRIORITY #1: HSET Bulk Insert - N+2 Database Queries

**Location:** `src/db.rs` lines 1019-1066
**Severity:** 🔴 **HIGH**
**Benchmark Impact:** `bench_hash_operations/hset_1000_fields` - 15.3ms (baseline)

```rust
pub fn hset(&self, key: &str, pairs: &[(&str, &[u8])]) -> Result<i64> {
    let key_id = self.get_or_create_hash_key(&conn, key)?;

    let mut new_fields = 0i64;
    for (field, value) in pairs {
        // REDUNDANT Query 1: Check if field exists
        let exists: bool = conn.query_row(
            "SELECT 1 FROM hashes WHERE key_id = ?1 AND field = ?2", ...
        ).unwrap_or(false);

        // Query 2: Insert or update
        conn.execute(
            "INSERT INTO hashes (key_id, field, value) VALUES (?1, ?2, ?3)
             ON CONFLICT(key_id, field) DO UPDATE SET value = excluded.value", ...
        )?;
    }

    // Query N+2: Update timestamp
    conn.execute("UPDATE keys SET updated_at = ?1 WHERE id = ?2", ...)?;
}
```

**Problem Analysis:**
- For 1000 fields: Executes **~1000 existence checks + 1000 INSERT OR UPDATE + 1 timestamp update = 2001 queries**
- The existence check (Query 1) is **redundant**: SQLite's `ON CONFLICT` clause already determines if the field exists
- Current baseline: 15.3ms for 1000 fields = 65 QPS

**Current Query Pattern:**
```
SELECT (exists?) + INSERT OR REPLACE (upsert) × N fields + UPDATE timestamp + INSERT history
= 2N + 2 queries per operation
```

**Optimized Query Pattern:**
```
INSERT OR REPLACE all fields in single batch + UPDATE timestamp + INSERT history
= 1 + 1 + 1 = 3 queries per operation
```

**Expected improvement:** 5-10x (15.3ms → 1.5-3ms)

### HIGH PRIORITY #2: ZADD Bulk Insert - N+2 Database Queries

**Location:** `src/db.rs` lines 2479-2522
**Severity:** 🔴 **HIGH**
**Benchmark Impact:** `bench_sorted_set_operations/zadd` - 14.37µs per operation

```rust
pub fn zadd(&self, key: &str, members: &[ZMember]) -> Result<i64> {
    let key_id = self.get_or_create_zset_key(&conn, key)?;

    let mut added = 0i64;
    for m in members {
        // REDUNDANT Query 1: Check if member exists
        let exists: bool = conn.query_row(
            "SELECT 1 FROM zsets WHERE key_id = ?1 AND member = ?2", ...
        ).unwrap_or(false);

        if exists {
            // Query 2a: UPDATE score
            conn.execute("UPDATE zsets SET score = ?1 WHERE key_id = ?2 AND member = ?3", ...)?;
        } else {
            // Query 2b: INSERT new member
            conn.execute("INSERT INTO zsets (key_id, member, score) VALUES (?1, ?2, ?3)", ...)?;
            added += 1;
        }
    }

    conn.execute("UPDATE keys SET updated_at = ?1 WHERE id = ?2", ...)?;
}
```

**Problem:** Same as HSET - redundant existence check before conditional logic that SQLite can handle natively.

**SQL Optimization:**
```sql
-- Instead of: SELECT (check) + conditional INSERT/UPDATE for each member
-- Use:
INSERT INTO zsets (key_id, member, score) VALUES (?, ?, ?)
ON CONFLICT(key_id, member) DO UPDATE SET score = excluded.score;
```

**Expected improvement:** 3-5x (14.37µs → 3-5µs per operation)

### HIGH PRIORITY #3: Concurrent Write Serialization

**Location:** `src/db.rs` line 16 (Mutex<Connection>)
**Severity:** 🔴 **HIGH**
**Benchmark Impact:** `bench_concurrent_operations` - Shows 16x throughput degradation with 16 threads

**Current Metrics:**
```
1 thread:  363 ops/ms (100 ops per iteration, 2.75ms per iteration)
4 threads: 89.5 ops/ms (400 ops per iteration, 11.17ms per iteration)
8 threads: 39.6 ops/ms (800 ops per iteration, 25.22ms per iteration)
16 threads: 22.2 ops/ms (1600 ops per iteration, 45.09ms per iteration)
```

**Root Cause:** Every write operation must acquire the global Mutex before writing. Reads don't help because they also need the lock.

**Solution Architecture:**
1. Move to connection pooling (sqlite3 supports multiple connections with WAL mode)
2. Use thread-local connections with thread-safe sharing
3. Or: Switch to RwLock to allow concurrent reads (SQLite 3.8.0+ supports this)

**Expected improvement:** 3-5x for concurrent writes (45ms → 15-20ms for 16 threads)

### MEDIUM PRIORITY #1: LRANGE Full COUNT Query

**Location:** `src/db.rs` lines 1784-1833
**Severity:** 🟠 **MEDIUM**
**Benchmark Impact:** `bench_list_operations/lrange_*` - 7.6µs to 200µs depending on list size

```rust
pub fn lrange(&self, key: &str, start: i64, stop: i64) -> Result<Vec<Vec<u8>>> {
    // Query 1: Get key_id + type + expire_at (from get_list_key_id)
    let key_id = self.get_list_key_id(&conn, key)?;

    // Query 2: COUNT(*) - Used to convert negative indices
    let len: i64 = conn.query_row(
        "SELECT COUNT(*) FROM lists WHERE key_id = ?1", ...
    )?;

    // Calculate actual range...

    // Query 3: Fetch range with LIMIT/OFFSET
    let mut stmt = conn.prepare(
        "SELECT value FROM lists WHERE key_id = ?1 ORDER BY pos ASC LIMIT ?2 OFFSET ?3"
    )?;
}
```

**Problem:** The COUNT query is executed even when not needed (for positive indices that don't need conversion).

**Optimization:**
```sql
-- Use SQLite's window functions or conditional logic to avoid redundant COUNT
SELECT value FROM lists WHERE key_id = ? ORDER BY pos ASC LIMIT ? OFFSET ?
-- Then handle negative indices by querying once more only if needed
```

**Expected improvement:** 1.5-2x (eliminate redundant COUNT in 50% of cases)

### MEDIUM PRIORITY #2: ZRANGE Full COUNT Query

**Location:** `src/db.rs` lines 2672-2736
**Severity:** 🟠 **MEDIUM**
**Benchmark Impact:** `bench_sorted_set_operations/zrange` - 20.17µs

```rust
pub fn zrange(&self, key: &str, start: i64, stop: i64, with_scores: bool) -> Result<Vec<ZMember>> {
    // Query 1: COUNT(*) for negative index conversion
    let total: i64 = conn.query_row(
        "SELECT COUNT(*) FROM zsets WHERE key_id = ?1", ...
    )?;

    // Handle negative indices...

    // Query 2: Fetch range
    let mut stmt = conn.prepare(
        "SELECT member, score FROM zsets WHERE key_id = ?1 ORDER BY score ASC, member ASC LIMIT ?2 OFFSET ?3"
    )?;
}
```

**Same issue as LRANGE** - redundant COUNT query.

**Expected improvement:** 1.5-2x

### MEDIUM PRIORITY #3: History Recording Overhead

**Location:** `src/db.rs` lines 299, 1563, 1639, 3298 (and more)
**Severity:** 🟠 **MEDIUM**
**Benchmark Impact:** Every write operation (SET, LPUSH, etc.) includes history recording

```rust
pub fn set(&self, key: &str, value: &[u8], ttl: Option<Duration>) -> Result<()> {
    // ... perform main operation (5 queries) ...

    // Record history - adds 5+ more queries
    self.record_history(db, key, "SET", Some(value.to_vec()))?;
}

fn record_history(...) -> Result<()> {
    // Query 1: get_or_create_key_id (SELECT + INSERT)
    let key_id = self.get_or_create_key_id(db, key)?;

    // Query 2: increment_version
    let version = self.increment_version(key_id)?;

    // Query 3: SELECT type from keys
    let key_type: i32 = conn.query_row(...)?;

    // Query 4: INSERT into key_history
    conn.execute("INSERT INTO key_history ...", ...)?;

    // Query 5+: apply_retention_policy
    self.apply_retention_policy(db, key)?;
}
```

**Problem:** History recording is called even when history might be disabled or not configured for that key.

**Current baseline:** SET operation = 5 queries, with history = 10+ queries

**Optimization:**
1. Check if history is enabled at the key level before recording
2. Batch history inserts instead of recording after every operation
3. Make history optional per-operation

**Expected improvement:** 1.5x (reduce queries by ~25-33%)

### MEDIUM PRIORITY #4: List Rebalancing Catastrophic Cost

**Location:** `src/db.rs` lines 1399-1427
**Severity:** 🟠 **MEDIUM** (low frequency, high impact)
**Benchmark Impact:** Not directly tested, but would cause 100x+ latency spike

```rust
fn rebalance_list(&self, conn: &Connection, key_id: i64) -> Result<()> {
    // Query 1: SELECT all items (O(n))
    let items: Vec<(i64, Vec<u8>)> = conn.query_map(
        "SELECT pos, value FROM lists WHERE key_id = ?1 ORDER BY pos ASC",
        |row| Ok((row.get::<_, i64>(0)?, row.get::<_, Vec<u8>>(1)?))
    ).collect();

    // Query 2: DELETE all items
    conn.execute("DELETE FROM lists WHERE key_id = ?1", ...)?;

    // Queries 3..n+2: Re-INSERT each item
    for (i, (_, value)) in items.iter().enumerate() {
        let new_pos = ((i as i64) + 1) * Self::LIST_GAP;
        conn.execute(
            "INSERT INTO lists (key_id, pos, value) VALUES (?1, ?2, ?3)",
            ...
        )?;
    }
}
```

**Problem:** Rebalancing requires 2 + N queries for a list of N items. Threshold is i64::MIN/10*9, so it's rare but catastrophic (e.g., 1000-item list = 1002 queries in one operation).

**Better approach:** Use a better gap algorithm or dynamically allocate position space.

**Expected improvement:** Eliminate rebalancing in most cases (3-5x for pathological cases)

### MEDIUM PRIORITY #5: SQLite Configuration Gaps

**Location:** `src/db.rs` lines 60-66
**Severity:** 🟠 **MEDIUM**
**Impact:** 1.5-2x potential throughput improvement

```rust
conn.execute_batch(
    "PRAGMA journal_mode = WAL;      -- ✅ Good
     PRAGMA synchronous = NORMAL;     -- ✅ Balanced
     PRAGMA foreign_keys = ON;        -- ✅ Required
     PRAGMA busy_timeout = 5000;"     -- ⚠️ Hides contention
)?;
```

**Missing optimizations:**
1. **PRAGMA cache_size** - Default is 2000 pages (8MB), should increase for workloads with large datasets
2. **PRAGMA temp_store = MEMORY** - Use memory for temp tables instead of disk
3. **PRAGMA mmap_size** - Memory-map I/O for faster access
4. **PRAGMA query_only** - Could optimize read-only connections

**Current baseline:** Generic defaults chosen for safety, not performance

**Expected improvement:** 1.2-1.5x (cache size tuning + memory temp store)

## Summarized Optimization Opportunities

### Top 5 Bottlenecks (Priority Order)

| Priority | Issue | Current | Target | Effort | Potential Gain |
|----------|-------|---------|--------|--------|----------------|
| 🔴 **1** | Global Mutex<Connection> | 22 ops/ms (16 threads) | 75+ ops/ms | Medium | **3-5x** |
| 🔴 **2** | HSET redundant queries | 15.3ms (1000 fields) | 1.5-3ms | Low | **3-10x** |
| 🔴 **3** | ZADD redundant queries | 14.37µs | 3-5µs | Low | **2-4x** |
| 🟠 **4** | Concurrent write lock | 45ms (16 threads) | 15-20ms | Medium | **2-3x** |
| 🟠 **5** | History overhead | +50% latency | +20% | Low | **1.5x** |

### Session 18.3 Implementation Plan

**Phase 1 (High Impact, Low Effort):**
- Fix HSET bulk insert (remove existence check) - 30 min
- Fix ZADD bulk insert (remove existence check) - 30 min
- Fix LRANGE/ZRANGE COUNT optimization - 30 min

**Phase 2 (Medium Impact, Medium Effort):**
- Add connection pooling or RwLock for concurrent reads - 2 hours
- SQLite PRAGMA tuning - 30 min

**Phase 3 (Optimization):**
- History recording optimizations - 1 hour
- List rebalancing algorithm improvement - 1 hour

**Target:** Achieve 5,000+ QPS minimum (currently ~155K QPS single-threaded, goal is 10K+ with better concurrency)

## Benchmark Implications

### Current Bottleneck Manifestation

**String Operations:**
- SET (17.47µs) ← Limited by: Single connection mutex
- GET (3.17µs) ← Limited by: Query efficiency (2 queries)

**Hash Operations:**
- HSET 1000 fields (15.3ms) ← Limited by: N+2 queries per field
- HGET (3.57µs) ← Single lookup, efficient

**List Operations:**
- LPUSH (20.66µs) ← Limited by: Lock contention
- LRANGE 1000 items (200µs) ← Limited by: Full scan + COUNT

**Concurrent Operations:**
- 1 thread: 363 ops/ms
- 16 threads: 22 ops/ms ← 16.5x degradation due to lock

### Expected Improvements After Session 18.3

If all optimizations implemented:
```
Current (baseline):
- Single-threaded: 155K QPS (mixed workload)
- 16 threads: 22 ops/ms × 100 ops/iter = 2,200 ops/ms = 22,000 QPS

After optimizations:
- Single-threaded: 200K+ QPS (10% improvement from HSET/ZADD/History)
- 16 threads: 70+ ops/ms × 100 ops/iter = 7,000+ ops/ms = 70,000+ QPS

Target achieved: 10,000+ QPS goal with concurrent workloads ✅
```

## Next Steps - Session 18.3

1. **Implement HSET/ZADD fix** (Remove redundant existence checks)
2. **Implement LRANGE/ZRANGE fix** (Optimize COUNT query)
3. **Add connection pooling** or switch to RwLock for concurrent reads
4. **Run benchmarks** after each optimization to measure improvement
5. **Target: 5,000+ QPS minimum** (documented in PROFILING_REPORT.md)

## Files Referenced

- `src/db.rs` - Main bottlenecks identified (lines 16, 233-302, 1019-1066, 1399-1427, 1784-1833, 2479-2522, 2672-2736)
- `benches/redlite_benchmarks.rs` - Benchmark tests revealing bottlenecks
- `PERFORMANCE.md` - Baseline metrics

---

**Report Status:** Complete - Ready for optimization implementation
**Next Session:** Session 18.3 - Optimization Passes
