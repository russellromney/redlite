# Redlite-Bench Rust Implementation Plan

## Overview
Build a high-performance, comprehensive Redis protocol benchmark suite in Rust that fills gaps in existing tools. The implementation will extend the existing `redlite/benches/comprehensive_comparison.rs` with full data type coverage and realistic workload scenarios.

---

## Phase 1: Specification Improvements (1-2 hours)

### 1.1 Fix Workload Scenarios
**Current Problem**: "balanced" workload only tests 4 operations (GET, HGET, SET, HSET) - not representative of real usage.

**Solution**: Create comprehensive, realistic scenarios

#### Essential Scenarios
1. **truly_balanced** - Equal mix of ALL operations across ALL data types
   - Strings: 15% (GET, SET, INCR)
   - Lists: 15% (LPUSH, LPOP, LRANGE)
   - Hashes: 15% (HSET, HGET, HGETALL)
   - Sets: 15% (SADD, SREM, SMEMBERS)
   - Sorted Sets: 20% (ZADD, ZRANGE, ZSCORE)
   - Streams: 20% (XADD, XREAD, XRANGE)

2. **read_only** - Pure read workload
   - GET: 30%, HGET: 20%, LRANGE: 15%, SMEMBERS: 15%, ZRANGE: 10%, XRANGE: 10%

3. **write_only** - Pure write workload
   - SET: 30%, HSET: 20%, LPUSH: 15%, SADD: 15%, ZADD: 10%, XADD: 10%

4. **read_heavy** - 80% read, 20% write (realistic caching)
   - Reads: GET (40%), HGET (20%), LRANGE (10%), SMEMBERS (10%)
   - Writes: SET (10%), HSET (5%), LPUSH (5%)

5. **write_heavy** - 20% read, 80% write (logging/analytics)
   - Reads: GET (10%), HGET (10%)
   - Writes: SET (30%), HSET (20%), LPUSH (10%), SADD (10%), XADD (10%)

#### Data Structure Specific Scenarios
6. **cache_pattern** - String-heavy (KV cache)
   - GET: 70%, SET: 20%, INCR: 5%, DEL: 5%

7. **session_store** - Hash-heavy (user sessions)
   - HGET: 50%, HSET: 30%, HGETALL: 10%, HDEL: 5%, EXPIRE: 5%

8. **message_queue** - List operations (task queue)
   - LPUSH: 45%, RPOP: 45%, LLEN: 10%

9. **leaderboard** - Sorted set operations (gaming, ranking)
   - ZADD: 40%, ZRANGE: 30%, ZRANK: 20%, ZSCORE: 10%

10. **event_stream** - Stream operations (event sourcing)
    - XADD: 50%, XREAD: 30%, XRANGE: 15%, XLEN: 5%

11. **social_graph** - Set operations (followers, tags)
    - SADD: 30%, SISMEMBER: 40%, SMEMBERS: 20%, SINTER: 10%

#### Extreme Scenarios
12. **tiny_keys** - 1-10 byte keys/values (metadata)
13. **huge_keys** - 10KB-1MB values (blob storage)
14. **hot_keys** - 90% of ops hit 10% of keys (skewed access)
15. **cold_keys** - Uniform distribution (no cache hits)
16. **write_storm** - Burst writes at maximum rate
17. **read_storm** - Burst reads at maximum rate
18. **mixed_storm** - Alternating read/write bursts

#### Pathological Scenarios
19. **range_scan_heavy** - Expensive range operations
    - LRANGE (large ranges), ZRANGE, XRANGE, SMEMBERS, HGETALL
20. **delete_heavy** - High deletion rate
    - DEL: 50%, SET: 30%, SADD: 10%, ZADD: 10%
21. **expire_heavy** - Heavy TTL usage
    - SET with EXPIRE: 40%, GET: 40%, TTL checks: 20%

### 1.2 Add Extreme Dataset Sizes
**Current**: 1K, 10K, 100K
**Add**: 1M, 10M, 100M, 1B

**Purpose**: Test memory limits, disk I/O, cache behavior

### 1.3 Output to SQLite
**Current**: Console, JSON, CSV, Markdown, HTML
**Add**: SQLite database (primary storage)

**Why**:
- Query historical results
- Compare across runs
- Track regressions
- Generate dynamic reports

**Schema**:
```sql
CREATE TABLE benchmarks (
    id INTEGER PRIMARY KEY,
    run_id TEXT,
    timestamp INTEGER,
    backend TEXT,
    operation TEXT,
    data_type TEXT,
    dataset_size INTEGER,
    concurrency INTEGER,
    avg_latency_us REAL,
    p50_latency_us REAL,
    p95_latency_us REAL,
    p99_latency_us REAL,
    throughput_ops_sec REAL,
    scenario TEXT,
    metadata TEXT  -- JSON blob
);
```

---

## Phase 2: Extend Client Enum (4-6 hours)

### 2.1 Add List Operations
```rust
impl Client {
    fn lpush(&self, key: &str, value: &[u8]) -> Result<i64, Error>;
    fn rpush(&self, key: &str, value: &[u8]) -> Result<i64, Error>;
    fn lpop(&self, key: &str) -> Result<Option<Vec<u8>>, Error>;
    fn rpop(&self, key: &str) -> Result<Option<Vec<u8>>, Error>;
    fn llen(&self, key: &str) -> Result<i64, Error>;
    fn lrange(&self, key: &str, start: i64, stop: i64) -> Result<Vec<Vec<u8>>, Error>;
    fn lindex(&self, key: &str, index: i64) -> Result<Option<Vec<u8>>, Error>;
}
```

**Implementation notes**:
- For Redis clients: Use `redis::Commands` trait
- For Redlite embedded: Call `db.lpush()`, `db.lpop()`, etc.
- Handle Arc<Db> cloning properly

### 2.2 Add Hash Operations
```rust
impl Client {
    fn hset(&self, key: &str, field: &str, value: &[u8]) -> Result<i64, Error>;
    fn hget(&self, key: &str, field: &str) -> Result<Option<Vec<u8>>, Error>;
    fn hgetall(&self, key: &str) -> Result<Vec<(String, Vec<u8>)>, Error>;
    fn hmget(&self, key: &str, fields: &[&str]) -> Result<Vec<Option<Vec<u8>>>, Error>;
    fn hlen(&self, key: &str) -> Result<i64, Error>;
    fn hdel(&self, key: &str, fields: &[&str]) -> Result<i64, Error>;
    fn hincrby(&self, key: &str, field: &str, delta: i64) -> Result<i64, Error>;
}
```

### 2.3 Add Set Operations
```rust
impl Client {
    fn sadd(&self, key: &str, members: &[&[u8]]) -> Result<i64, Error>;
    fn srem(&self, key: &str, members: &[&[u8]]) -> Result<i64, Error>;
    fn smembers(&self, key: &str) -> Result<Vec<Vec<u8>>, Error>;
    fn sismember(&self, key: &str, member: &[u8]) -> Result<bool, Error>;
    fn scard(&self, key: &str) -> Result<i64, Error>;
    fn spop(&self, key: &str, count: usize) -> Result<Vec<Vec<u8>>, Error>;
    fn srandmember(&self, key: &str, count: i64) -> Result<Vec<Vec<u8>>, Error>;
    fn sinter(&self, keys: &[&str]) -> Result<Vec<Vec<u8>>, Error>;
}
```

### 2.4 Add Sorted Set Operations
```rust
impl Client {
    fn zadd(&self, key: &str, members: &[(f64, &[u8])]) -> Result<i64, Error>;
    fn zrem(&self, key: &str, members: &[&[u8]]) -> Result<i64, Error>;
    fn zrange(&self, key: &str, start: i64, stop: i64) -> Result<Vec<Vec<u8>>, Error>;
    fn zrangebyscore(&self, key: &str, min: f64, max: f64) -> Result<Vec<Vec<u8>>, Error>;
    fn zscore(&self, key: &str, member: &[u8]) -> Result<Option<f64>, Error>;
    fn zrank(&self, key: &str, member: &[u8]) -> Result<Option<i64>, Error>;
    fn zcard(&self, key: &str) -> Result<i64, Error>;
    fn zcount(&self, key: &str, min: f64, max: f64) -> Result<i64, Error>;
}
```

### 2.5 Add Stream Operations
```rust
impl Client {
    fn xadd(&self, key: &str, id: &str, items: &[(&str, &[u8])]) -> Result<String, Error>;
    fn xlen(&self, key: &str) -> Result<i64, Error>;
    fn xrange(&self, key: &str, start: &str, end: &str, count: Option<usize>) -> Result<Vec<StreamEntry>, Error>;
    fn xrevrange(&self, key: &str, end: &str, start: &str, count: Option<usize>) -> Result<Vec<StreamEntry>, Error>;
    fn xread(&self, keys: &[&str], ids: &[&str], count: Option<usize>) -> Result<Vec<(String, Vec<StreamEntry>)>, Error>;
    fn xdel(&self, key: &str, ids: &[&str]) -> Result<i64, Error>;
    fn xtrim(&self, key: &str, maxlen: usize) -> Result<i64, Error>;
}
```

**Challenge**: Redis crate might not have full stream support. May need to use raw commands.

---

## Phase 3: Benchmark Functions (6-8 hours)

### 3.1 Generic Benchmark Template
```rust
async fn bench_operation<F, R>(
    client: &Client,
    operation_name: &str,
    setup_fn: F,
    operation_fn: impl Fn(&Client, usize) -> R,
    size: usize,
    iterations: usize,
    concurrency: usize,
) -> Result<BenchResult, Error>
where
    F: Fn(&Client, usize) -> Result<(), Error>,
    R: Future<Output = Result<(), Error>>,
{
    // 1. Setup phase (populate data)
    setup_fn(client, size)?;

    // 2. Warmup phase
    warmup(client, &operation_fn, 1000).await?;

    // 3. Measurement phase
    let latencies = measure_concurrent(client, operation_fn, iterations, concurrency).await?;

    // 4. Calculate metrics
    Ok(BenchResult {
        operation: operation_name.to_string(),
        avg_latency_us: calculate_avg(&latencies),
        p50_latency_us: calculate_percentile(&latencies, 0.50),
        p95_latency_us: calculate_percentile(&latencies, 0.95),
        p99_latency_us: calculate_percentile(&latencies, 0.99),
        throughput: iterations as f64 / total_time.as_secs_f64(),
        // ...
    })
}
```

### 3.2 List Benchmarks
```rust
fn bench_list_lpush(client: &Client, size: usize, conns: usize) -> Result<BenchResult, Error>
fn bench_list_rpush(client: &Client, size: usize, conns: usize) -> Result<BenchResult, Error>
fn bench_list_lpop(client: &Client, size: usize, conns: usize) -> Result<BenchResult, Error>
fn bench_list_llen(client: &Client, size: usize, conns: usize) -> Result<BenchResult, Error>
fn bench_list_lrange(client: &Client, size: usize, conns: usize) -> Result<BenchResult, Error>
```

### 3.3 Hash Benchmarks
```rust
fn bench_hash_hset(client: &Client, size: usize, conns: usize) -> Result<BenchResult, Error>
fn bench_hash_hget(client: &Client, size: usize, conns: usize) -> Result<BenchResult, Error>
fn bench_hash_hgetall(client: &Client, size: usize, conns: usize) -> Result<BenchResult, Error>
fn bench_hash_hlen(client: &Client, size: usize, conns: usize) -> Result<BenchResult, Error>
fn bench_hash_hdel(client: &Client, size: usize, conns: usize) -> Result<BenchResult, Error>
```

### 3.4 Set Benchmarks
```rust
fn bench_set_sadd(client: &Client, size: usize, conns: usize) -> Result<BenchResult, Error>
fn bench_set_srem(client: &Client, size: usize, conns: usize) -> Result<BenchResult, Error>
fn bench_set_smembers(client: &Client, size: usize, conns: usize) -> Result<BenchResult, Error>
fn bench_set_scard(client: &Client, size: usize, conns: usize) -> Result<BenchResult, Error>
fn bench_set_sismember(client: &Client, size: usize, conns: usize) -> Result<BenchResult, Error>
```

### 3.5 Sorted Set Benchmarks
```rust
fn bench_zset_zadd(client: &Client, size: usize, conns: usize) -> Result<BenchResult, Error>
fn bench_zset_zrem(client: &Client, size: usize, conns: usize) -> Result<BenchResult, Error>
fn bench_zset_zrange(client: &Client, size: usize, conns: usize) -> Result<BenchResult, Error>
fn bench_zset_zscore(client: &Client, size: usize, conns: usize) -> Result<BenchResult, Error>
fn bench_zset_zcard(client: &Client, size: usize, conns: usize) -> Result<BenchResult, Error>
```

### 3.6 Stream Benchmarks
```rust
fn bench_stream_xadd(client: &Client, size: usize, conns: usize) -> Result<BenchResult, Error>
fn bench_stream_xlen(client: &Client, size: usize, conns: usize) -> Result<BenchResult, Error>
fn bench_stream_xrange(client: &Client, size: usize, conns: usize) -> Result<BenchResult, Error>
fn bench_stream_xread(client: &Client, size: usize, conns: usize) -> Result<BenchResult, Error>
```

---

## Phase 4: Workload Scenarios (3-4 hours)

### 4.1 Scenario Runner
```rust
struct Scenario {
    name: String,
    operations: Vec<(String, f64)>,  // (operation_name, weight)
}

async fn run_scenario(
    client: &Client,
    scenario: &Scenario,
    size: usize,
    duration_secs: u64,
    concurrency: usize,
) -> Result<ScenarioResult, Error> {
    // Weighted random operation selection
    let mut rng = rand::thread_rng();
    let dist = WeightedIndex::new(scenario.operations.iter().map(|(_, w)| w)).unwrap();

    // Run for fixed duration
    let start = Instant::now();
    while start.elapsed() < Duration::from_secs(duration_secs) {
        let op_idx = dist.sample(&mut rng);
        let (op_name, _) = &scenario.operations[op_idx];

        // Execute operation
        execute_operation(client, op_name, size).await?;
    }

    // Collect metrics
    Ok(ScenarioResult { /* ... */ })
}
```

### 4.2 Load Scenarios from Spec
```rust
use serde::{Deserialize, Serialize};

#[derive(Deserialize)]
struct WorkloadSpec {
    name: String,
    description: String,
    operations: Vec<OperationWeight>,
}

#[derive(Deserialize)]
struct OperationWeight {
    #[serde(rename = "type")]
    op_type: String,
    weight: f64,
}

fn load_scenarios() -> Vec<WorkloadSpec> {
    let spec_path = "../redlite-bench/spec/benchmark-spec.yaml";
    let spec: BenchmarkSpec = serde_yaml::from_str(&fs::read_to_string(spec_path).unwrap()).unwrap();
    spec.workloads
}
```

---

## Phase 5: SQLite Results Storage (2-3 hours)

### 5.1 Database Schema
```rust
use rusqlite::{Connection, params};

fn init_results_db(path: &str) -> Result<Connection, Error> {
    let conn = Connection::open(path)?;

    conn.execute(
        "CREATE TABLE IF NOT EXISTS benchmarks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id TEXT NOT NULL,
            timestamp INTEGER NOT NULL,
            backend TEXT NOT NULL,
            operation TEXT NOT NULL,
            data_type TEXT NOT NULL,
            scenario TEXT,
            dataset_size INTEGER NOT NULL,
            concurrency INTEGER NOT NULL,
            avg_latency_us REAL NOT NULL,
            min_latency_us REAL,
            max_latency_us REAL,
            p50_latency_us REAL,
            p95_latency_us REAL,
            p99_latency_us REAL,
            stddev_latency_us REAL,
            throughput_ops_sec REAL NOT NULL,
            total_ops INTEGER NOT NULL,
            duration_secs REAL NOT NULL,
            metadata TEXT
        )",
        [],
    )?;

    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_backend ON benchmarks(backend)",
        [],
    )?;

    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_operation ON benchmarks(operation)",
        [],
    )?;

    Ok(conn)
}
```

### 5.2 Store Results
```rust
fn store_result(conn: &Connection, run_id: &str, result: &BenchResult) -> Result<(), Error> {
    conn.execute(
        "INSERT INTO benchmarks (
            run_id, timestamp, backend, operation, data_type, scenario,
            dataset_size, concurrency, avg_latency_us, p50_latency_us,
            p95_latency_us, p99_latency_us, throughput_ops_sec, total_ops,
            duration_secs
        ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15)",
        params![
            run_id,
            SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs(),
            result.backend,
            result.operation,
            result.data_type,
            result.scenario,
            result.size,
            result.connections,
            result.avg_latency_us,
            result.p50_latency_us,
            result.p95_latency_us,
            result.p99_latency_us,
            result.throughput_ops_per_sec,
            result.total_ops,
            result.duration_secs,
        ],
    )?;
    Ok(())
}
```

### 5.3 Query Results
```rust
fn compare_backends(conn: &Connection, operation: &str) -> Result<Vec<ComparisonRow>, Error> {
    let mut stmt = conn.prepare(
        "SELECT backend, AVG(avg_latency_us), AVG(throughput_ops_sec)
         FROM benchmarks
         WHERE operation = ?1
         GROUP BY backend
         ORDER BY AVG(avg_latency_us)"
    )?;

    let rows = stmt.query_map([operation], |row| {
        Ok(ComparisonRow {
            backend: row.get(0)?,
            avg_latency: row.get(1)?,
            throughput: row.get(2)?,
        })
    })?;

    rows.collect()
}
```

---

## Phase 6: CLI Interface (2-3 hours)

### 6.1 Command Structure
```rust
use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(name = "redlite-bench")]
#[command(about = "Comprehensive Redis protocol benchmark suite")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Run benchmarks
    Run {
        /// Backends to test (comma-separated)
        #[arg(short, long, default_value = "redis://127.0.0.1:6379")]
        backends: String,

        /// Dataset sizes (comma-separated)
        #[arg(short, long, default_value = "1000,10000")]
        sizes: String,

        /// Concurrency levels (comma-separated)
        #[arg(short, long, default_value = "1,2,4,8")]
        concurrency: String,

        /// Operations to test (comma-separated, or "all")
        #[arg(short, long, default_value = "all")]
        operations: String,

        /// Scenarios to run (comma-separated, or "all")
        #[arg(short = 's', long)]
        scenarios: Option<String>,

        /// Output database path
        #[arg(short, long, default_value = "results/benchmarks.db")]
        output: String,
    },

    /// Generate report from results
    Report {
        /// Results database
        #[arg(short, long, default_value = "results/benchmarks.db")]
        database: String,

        /// Output format (console, markdown, html, json)
        #[arg(short, long, default_value = "console")]
        format: String,

        /// Filter by run ID
        #[arg(short, long)]
        run_id: Option<String>,
    },

    /// Compare backends
    Compare {
        /// Results database
        #[arg(short, long, default_value = "results/benchmarks.db")]
        database: String,

        /// Backends to compare (comma-separated)
        #[arg(short, long)]
        backends: String,

        /// Operation to compare
        #[arg(short, long)]
        operation: Option<String>,
    },
}
```

---

## Phase 7: Testing & Validation (2-3 hours)

### 7.1 Unit Tests
```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_list_operations() {
        let db = Arc::new(Db::open_memory().unwrap());
        let client = Client::RedliteEmbedded(db);

        // Test LPUSH
        let result = client.lpush("mylist", b"value1").unwrap();
        assert_eq!(result, 1);

        // Test LPOP
        let value = client.lpop("mylist").unwrap();
        assert_eq!(value, Some(b"value1".to_vec()));
    }

    #[tokio::test]
    async fn test_concurrent_operations() {
        // Test that concurrent operations work correctly
    }
}
```

### 7.2 Integration Tests
```rust
#[tokio::test]
async fn test_full_benchmark_run() {
    // Start Redis in Docker
    // Run benchmark
    // Verify results stored in SQLite
    // Clean up
}
```

### 7.3 Validation Checklist
- [ ] All 48 operations work with Redis
- [ ] All operations work with Redlite embedded
- [ ] All operations work with Redlite server
- [ ] Concurrent operations produce correct results
- [ ] Metrics are accurate (compare with redis-benchmark)
- [ ] SQLite storage works
- [ ] Report generation works
- [ ] CLI interface is user-friendly

---

## Phase 8: Documentation (1-2 hours)

### 8.1 Usage Examples
```markdown
# Quick Start

## Run basic benchmark
redlite-bench run --backends redis://localhost:6379

## Run with specific operations
redlite-bench run --operations LPUSH,LPOP,HSET,HGET

## Run scenarios
redlite-bench run --scenarios cache_pattern,message_queue

## Compare backends
redlite-bench compare --backends redis,redlite --operation GET

## Generate report
redlite-bench report --format markdown > RESULTS.md
```

### 8.2 Architecture Documentation
- Component diagram
- Data flow
- Extension points
- Performance considerations

---

## Timeline Estimate

| Phase | Duration | Cumulative |
|-------|----------|------------|
| 1. Spec improvements | 1-2 hours | 2 hours |
| 2. Client enum extensions | 4-6 hours | 8 hours |
| 3. Benchmark functions | 6-8 hours | 16 hours |
| 4. Workload scenarios | 3-4 hours | 20 hours |
| 5. SQLite storage | 2-3 hours | 23 hours |
| 6. CLI interface | 2-3 hours | 26 hours |
| 7. Testing | 2-3 hours | 29 hours |
| 8. Documentation | 1-2 hours | 31 hours |

**Total: ~30 hours (4-5 days of focused work)**

---

## Success Criteria

### Must Have (v0.2.0)
- [x] Spec with 20+ workload scenarios
- [ ] All 48 operations implemented
- [ ] Concurrent benchmarks working (1-16 connections)
- [ ] SQLite results storage
- [ ] Basic CLI interface
- [ ] Works with Redis and Redlite

### Nice to Have (v0.3.0)
- [ ] HTML report generation with charts
- [ ] Historical comparison queries
- [ ] CI/CD integration
- [ ] Performance regression detection
- [ ] Docker compose for test environments

### Future (v1.0.0)
- [ ] Distributed benchmarking (multiple machines)
- [ ] Real-time metrics dashboard
- [ ] Plugin system for custom operations
- [ ] Cloud benchmark service

---

## Risk Mitigation

| Risk | Likelihood | Impact | Mitigation |
|------|------------|--------|------------|
| Redis crate lacks stream support | Medium | High | Use raw Redis commands if needed |
| Concurrent benchmarks have race conditions | Low | High | Extensive testing, use channels properly |
| SQLite locking issues | Low | Medium | Use WAL mode, connection pooling |
| Compilation time too slow | Medium | Low | Use `sccache`, optimize dependencies |
| Results storage grows too large | Medium | Low | Add retention policy, compression |

---

## Open Questions

1. **Percentile calculation**: HDR Histogram (accurate, memory-intensive) vs simple sort (fast, limited accuracy)?
   - **Recommendation**: Start with simple sort, optimize later if needed

2. **Warmup iterations**: How many needed for stable results?
   - **Recommendation**: 10% of total iterations, min 1000

3. **Error handling**: Fail fast or collect error rates as metric?
   - **Recommendation**: Collect error rates, continue on single failures

4. **Memory limits**: How to handle 1B key benchmarks?
   - **Recommendation**: Start with 100K, add memory checks before larger sizes

5. **Network jitter**: How to handle variance in network latency?
   - **Recommendation**: Multiple runs, report median + stddev

---

## Next Steps

After reviewing and improving this plan:

1. Update `benchmark-spec.yaml` with 20+ scenarios
2. Add SQLite as primary output format
3. Extend `Client` enum with all operations
4. Implement benchmark functions
5. Test with 10K dataset
6. Generate comprehensive results

---

**Status**: 📋 Plan complete, ready for review and implementation
**Last Updated**: 2026-01-13
