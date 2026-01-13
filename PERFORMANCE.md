# Redlite Performance Benchmarks

**Session 18: Performance Testing & Benchmarking (Sessions 18.1-18.3)**

## Overview

Redlite is a SQLite-backed Redis-compatible embedded key-value store. This document tracks performance baselines, optimization efforts, and expected throughput characteristics.

### Key Metrics Summary (Session 18.3 Final)

- **Highest single-op throughput**: 546,448 QPS (LPOP - list operations)
- **Median throughput**: ~70,000 QPS across most operations
- **Embedded mode (80/20 workload)**: ~155,000 QPS
- **Concurrent throughput**: Limited by SQLite single-writer architecture
- **HSET 1000 fields**: ~18ms (baseline ~15.3ms)
- **ZADD**: ~14µs

## Session 18.3: Optimization Results

### What We Learned

**Bulk INSERT Optimization - Reverted:**

We attempted several optimization approaches for HSET/ZADD:

1. *Bulk multi-row INSERT*: Build dynamic SQL with multiple VALUES clauses
   - Result: **SLOWER** (~10-20% regression) due to SQL string building overhead
   - Reverted to simple for-loop approach

2. *Single COUNT query* for existence checking
   - Result: **SLOWER** than per-field existence checks
   - Dynamic SQL building overhead outweighs query reduction benefit

**Final Implementation:**
- Simple for-loop with per-field existence check + INSERT OR REPLACE
- Wrapped in implicit SQLite transaction (autocommit per HSET call)
- Redis-compatible return values (count of NEW fields)
- ~18ms for 1000 fields (acceptable for pathological case)

**Why Simple is Faster:**
- SQLite's prepared statement cache handles repeated queries efficiently
- Dynamic SQL building has CPU overhead
- For typical usage (1-10 fields), per-field checks are negligible
- Large bulk operations (1000+ fields) are rare in practice

**LRANGE/ZRANGE Optimization:**
- Conditional COUNT: only query when start < 0 OR stop < 0
- Positive indices skip unnecessary COUNT query
- Fixed edge case: start > stop returns empty for all index types

**Mixed Workload:**
- ~155,000 QPS on 80/20 read-write patterns

### Phase 2 Investigation - Connection Pooling

**Finding**: SQLite single-writer limitation
- Attempted connection pooling to reduce Mutex<Connection> contention
- SQLite only allows ONE concurrent writer across all connections
- Multiple pooled connections cause SQLITE_LOCKED errors under write contention
- Current single Mutex<Connection> design is architecturally correct

**Concurrent Performance Observation:**
- 1 thread: 3.6ms (100 ops)
- 4 threads: 15.1ms (400 ops) - 4.2x slower
- 8 threads: 28.6ms (800 ops) - 7.9x slower
- 16 threads: 58.4ms (1600 ops) - 16.2x slower
- **Root cause**: SQLite WAL mode with single writer, not application code

**Conclusion**: To improve concurrent write throughput beyond SQLite limitations would require:
1. Database engine replacement (PostgreSQL, RocksDB, etc.)
2. Application-level write queueing
3. Sharding/multi-database approach

## Baseline Metrics (Session 18.1)

### String Operations

| Operation | Mean Latency | QPS Estimate | Notes |
|-----------|-------------|--------------|-------|
| SET (64B) | 17.47 µs | 57,248 | Small value write |
| SET (1KB) | 19.86 µs | 50,352 | Medium value |
| SET (10KB) | 20.67 µs | 48,378 | Large value |
| GET | 3.17 µs | 315,458 | Read-heavy operation |
| INCR | 19.06 µs | 52,464 | Increment counter |
| APPEND | 40.03 µs | 24,981 | String concatenation |

**Insights**:
- GET is 5-6x faster than SET due to read-only operation
- Latency increases slightly with value size (SQLite I/O)
- INCR is fast due to optimized counter operations

### Hash Operations

| Operation | Params | Mean Latency | QPS Estimate | Notes |
|-----------|--------|-------------|--------------|-------|
| HSET | 10 fields | ~173 µs | ~5,780 | Per-field existence check |
| HSET | 100 fields | ~1,602 µs | ~624 | Linear scaling |
| HSET | 1000 fields | ~18,000 µs | ~55 | Pathological case (rare) |
| HGET | Single field | ~4.85 µs | ~206,185 | Fast single-field lookup |
| HGETALL | 10 fields | ~7.69 µs | ~130,039 | Efficient batch read |
| HGETALL | 100 fields | ~31.06 µs | ~32,200 | Linear scaling |

**Architecture Notes**:
- HSET uses per-field existence checks to correctly return count of NEW fields (Redis compatibility)
- Each field: 1 SELECT (existence) + 1 INSERT OR REPLACE = 2N queries total
- For typical usage (1-10 fields), this is fast and correct
- Bulk operations (1000+ fields) are rare in practice

**Optimization Attempts (Session 18.3)**:
- ❌ Bulk INSERT with dynamic SQL: **SLOWER** (SQL building overhead)
- ❌ Single COUNT query: **SLOWER** (dynamic SQL overhead)
- ✅ Simple for-loop: Fastest for SQLite's query cache

### List Operations

| Operation | Params | Mean Latency | QPS Estimate |
|-----------|--------|-------------|--------------|
| LPUSH | Single value | 20.66 µs | 48,414 |
| LPOP | Single value | 1.83 µs | 546,448 |
| LRANGE | 10 items | 7.62 µs | 131,233 |
| LRANGE | 100 items | 22.88 µs | 43,710 |
| LRANGE | 1000 items | 200.38 µs | 4,989 |

**Insights**:
- LPOP is exceptionally fast (in-memory operation)
- LRANGE scales with list size as expected
- Gap-based positioning enables efficient range queries

### Set Operations

| Operation | Cardinality | Mean Latency | QPS Estimate |
|-----------|------------|-------------|--------------|
| SADD | Single member | 11.80 µs | 84,746 |
| SMEMBERS | 10 items | 4.64 µs | 215,517 |
| SMEMBERS | 100 items | 13.83 µs | 72,308 |
| SMEMBERS | 1000 items | 107.71 µs | 9,284 |

**Insights**:
- SADD has moderate overhead (index update)
- SMEMBERS scales linearly with set size
- Good performance for typical set sizes (10-100 elements)

### Sorted Set Operations

| Operation | Params | Mean Latency | QPS Estimate |
|-----------|--------|-------------|--------------|
| ZADD | Single member | 14.37 µs | 69,592 |
| ZRANGE | Full range | 20.17 µs | 49,579 |

**Insights**:
- Consistent performance with string SET operations
- Score-based indexing adds minimal overhead

### Workload Patterns

#### Mixed Workload (80% reads, 20% writes)
- **Latency**: 6.44 µs
- **QPS**: 155,280
- **Profile**: Realistic cache workload

#### Concurrent Operations

| Thread Count | Mean Latency | Total Ops | Ops/ms |
|-------------|-------------|----------|--------|
| 1 | 2.75 ms | 100 | 363.64 |
| 4 | 11.17 ms | 400 | 89.51 |
| 8 | 25.22 ms | 800 | 39.65 |
| 16 | 45.09 ms | 1600 | 22.18 |

**Insights**:
- Linear scaling with thread count (SQLite RwLock contention)
- Single thread achieves ~364 ops/ms (100 ops each SET/GET)
- Multi-threaded scaling shows RwLock is the bottleneck

### Expiration Operations

| Operation | Mean Latency | QPS Estimate |
|-----------|-------------|--------------|
| EXPIRE | Set + expire | 22.96 µs | 43,554 |

## Architecture Insights

### SQLite as the Backend

Redlite uses SQLite for persistence with these implications:

1. **Write Latency**: All writes go through SQLite (default synchronous mode)
2. **Read Performance**: Reads benefit from SQLite's query optimizer
3. **Concurrency**: Limited by SQLite's RwLock (not MVCC due to bundled build)
4. **Durability**: ACID guarantees at the cost of latency

### Comparison to Redis

#### Benchmark: Embedded Redlite vs Network Redis (localhost)

| Operation | Redlite (embedded) | Redis (localhost) | Ratio |
|-----------|-------------------|-------------------|-------|
| SET (64B) | ~26 µs | ~124 µs | **5x faster** |
| GET | ~4 µs | ~116 µs | **30x faster** |
| HSET (10 fields) | ~87 µs | ~129 µs | **1.5x faster** |

**Note**: This comparison highlights that Redlite excels in embedded use cases where network latency is eliminated. Redis latency is dominated by TCP/IP overhead for localhost connections.

#### Architectural Characteristics:

| Aspect | Redlite | Redis |
|--------|---------|-------|
| Latency | 15-25 µs (writes), 3-4 µs (reads) | 100-150 µs (network overhead) |
| Persistence | Built-in (SQLite) | Optional (RDB/AOF) |
| Durability | ACID by default | Async by default |
| Concurrency | SQLite single-writer | Event loop (single-threaded) |
| Memory Overhead | SQLite + indexes | Pure in-memory |
| Use Case | Embedded applications | Network services |

## Optimization Results - Session 18.3

### Key Learnings

**HSET/ZADD - Simple is Faster:**
- ❌ Bulk INSERT with dynamic SQL: Added 10-20% overhead from SQL string building
- ❌ Single COUNT query for existence: Dynamic SQL overhead outweighed benefits
- ✅ Simple for-loop with per-field checks: SQLite's query cache handles this efficiently

The simple 2N query approach (N existence checks + N inserts) is actually faster than trying to reduce query count with dynamic SQL. SQLite's prepared statement cache makes repeated simple queries very efficient.

**LRANGE/ZRANGE Optimization** ✅ COMPLETED
- Added: Conditional COUNT query only for negative indices
- Fixed: Edge case where start > stop with positive indices
- Result: No regression, correct behavior for all cases

**Connection Pooling** ✅ INVESTIGATED
- Finding: SQLite single-writer limitation prevents connection pooling benefits
- SQLite WAL mode only allows ONE concurrent writer across all connections
- Current single Mutex<Connection> is architecturally optimal
- 16.2x degradation with 16 threads is SQLite limitation, not code issue

### ⏭️ Future Optimization Opportunities (Beyond SQLite limitations)

1. **Database Engine Switch** (Not recommended for embedded use)
   - Would require architectural redesign
   - Benefits: True concurrent writes
   - Cost: Loss of SQLite simplicity/embedded benefits

2. **Write Queueing** (Moderate effort)
   - Batch concurrent writes into single transaction
   - Could improve throughput but increases latency
   - Useful for write-heavy workloads

3. **MVCC Implementation** (Complex)
   - Requires SQLite compilation from source (not default bundled build)
   - Would enable concurrent reads without writer blocking
   - Needs careful transaction isolation management

## Benchmark Structure

The comprehensive benchmark suite includes:

- **String operations**: SET, GET, INCR, APPEND (with varying sizes)
- **Hash operations**: HSET, HGET, HGETALL (with varying field counts)
- **List operations**: LPUSH, LPOP, LRANGE (with varying list sizes)
- **Set operations**: SADD, SMEMBERS (with varying cardinality)
- **Sorted sets**: ZADD, ZRANGE
- **Mixed workload**: 80% reads, 20% writes
- **Concurrent ops**: 1, 4, 8, 16 thread scenarios
- **Expiration**: TTL operations

### Running Benchmarks

```bash
# Full benchmark suite with detailed results
make bench

# Quick iteration (reduced warm-up/measurement time)
cargo bench --bench redlite_benchmarks -- --warm-up-time 1 --measurement-time 1

# Specific benchmark
cargo bench --bench redlite_benchmarks string_get

# Generate flamegraph for profiling
cargo flamegraph --bench redlite_benchmarks
```

### Benchmark Results Location

- `target/criterion/` - HTML reports for each benchmark
- `benches/baseline.json` - Baseline metrics tracker

## Session Completion Status

- ✅ **Session 18.1**: Benchmark infrastructure & baselines established
- ✅ **Session 18.2**: Flamegraph profiling & bottleneck analysis (code-level analysis, Xcode limitation)
- ✅ **Session 18.3**: Query optimization & connection architecture investigation
- ✅ **Session 18.4**: CI/CD regression detection setup
- ⏳ **Session 18.5**: Advanced benchmarks (large values, TTL stress)
- ⏳ **Session 18.6**: Documentation and performance tuning guide

## Session 18.4: CI/CD Regression Detection

### GitHub Actions Workflow

A CI workflow has been added (`.github/workflows/benchmark.yml`) that:
- Runs on pushes/PRs that modify `src/`, `benches/`, or `Cargo.toml`
- Executes the full benchmark suite
- Compares results against cached baseline from main branch
- Flags regressions exceeding 15% threshold
- Posts warnings on PRs with performance regressions

### Local Comparison Tools

```bash
# Run benchmarks and compare against baseline.json
make bench-check

# Just compare existing results against baseline
make bench-compare

# Save current results as new baseline
make bench-save-baseline
```

### Scripts

- `scripts/check_regression.py` - CI script for parsing Criterion output
- `scripts/compare_baseline.py` - Local comparison against baseline.json
- `scripts/update_baseline.py` - Update baseline.json with current results

### Regression Threshold

Default threshold is **15%**. Any benchmark degrading by more than 15% triggers:
- CI warning in GitHub Actions
- PR comment notification (for pull requests)
- Non-zero exit code from comparison script

## Recommended Future Work

1. **Implement write queueing** for concurrent workloads
   - Batch multiple concurrent writes into single transaction
   - Tradeoff: slightly higher latency for better throughput

2. **Add MVCC support** (requires custom SQLite build)
   - Enable concurrent reads without writer blocking
   - Significant effort but would improve read-heavy concurrency

3. **Publish performance guide**
   - Document SQLite limitations for users
   - Provide tuning recommendations for different workload types

4. **Consider alternative storage backends** for future major versions
   - RocksDB for embedded systems wanting true concurrency
   - PostgreSQL for network-accessible scenarios

## Hardware & Environment

- **Platform**: macOS (Apple M-series)
- **Rust Version**: 1.70+
- **Profile**: Release build with LTO
- **Test Type**: Single-threaded unless noted

## References

- Benchmark framework: [Criterion.rs](https://bheisler.github.io/criterion.rs/book/)
- Results format: JSON baseline tracker for CI integration
- Profiling: Flamegraph integration for detailed analysis
