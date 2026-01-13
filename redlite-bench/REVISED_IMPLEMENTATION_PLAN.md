# Revised Redlite-Bench Implementation Plan (v0.2.0)

**Status**: 🟢 **PHASE 3 COMPLETE** - CLI integration and async concurrency done
**Based on**: Critical analysis of original plan + architectural improvements
**Target**: Bulletproof, maintainable, realistic timeline

---

## Progress Update (2026-01-13) - Session 2

### Completed This Session

**Phase 3 - CLI Integration**: COMPLETE
- [x] Scenario subcommand added to CLI for YAML-defined workloads
- [x] --output-format (console/json) flag on all subcommands
- [x] --output-file flag for writing results to file
- [x] run_scenario_benchmark() using dispatcher for weighted operation mixes

**True Async Concurrent Execution**: COMPLETE
- [x] tokio::spawn for real concurrent task execution in async mode
- [x] StdRng::from_entropy() for Send-safe RNG in spawned tasks
- [x] Added 'static bound to BenchmarkRunner generic
- [x] Results aggregated from all concurrent workers

**Test Scenarios**: COMPLETE
- [x] test-scenarios.yaml with read_heavy, write_heavy, balanced workloads
- [x] Full JSON output with metadata, latency percentiles, throughput

### Files Modified
- `src/bin/main.rs` - Added Scenario subcommand, output flags, scenario runner (+200 lines)
- `src/concurrency.rs` - True async execution with tokio::spawn (+100 lines)
- `src/benchmark/mod.rs` - Added 'static bound for concurrent execution
- `src/scenarios.rs` - Cleaned up unused import

### Files Created
- `test-scenarios.yaml` - Sample scenario definitions

### Build & Test Status
- cargo build --release: SUCCESS
- cargo test: 16 tests passed

---

## Previous Session (2026-01-13) - Session 1

**Phase 2 - Trait-Based Client Architecture**: COMPLETE
- [x] RedisLikeClient trait with 48 Redis + 4 Redlite-specific ops (52 total)
- [x] RedisClient adapter (redis_adapter.rs)
- [x] RedliteEmbeddedClient adapter (redlite_embedded_adapter.rs)
- [x] Clone trait implemented on both clients

**Phase 4.2 - YAML Scenario Loading**: COMPLETE
- [x] scenarios.rs - Parses workloads from YAML
- [x] Weighted operation selection with normalized cumulative probabilities
- [x] WorkloadScenario and OperationWeight structs with serde support

**Phase 4.1 - Operation Dispatcher**: COMPLETE
- [x] dispatcher.rs - Runtime operation dispatch for all 52 operations
- [x] Returns latency in microseconds per operation
- [x] Full error handling

**JSON Output Format**: COMPLETE
- [x] output.rs - JSON serialization with metadata, latency percentiles, throughput
- [x] OutputFormat enum (Console/Json)
- [x] JsonBenchmarkResult and JsonConcurrentResult structs
- [x] File output support

---

## Next Session Tasks
1. SQLite results storage (Phase 5) - optional, JSON works for now
2. Additional concurrent benchmark operations (LPUSH, HSET currently return "not implemented")
3. Run comprehensive benchmarks against Redis for comparison
4. Documentation and usage examples

---

## Phase 0: Dependency Validation (2-3 hours) ⚡ DO THIS FIRST

### 0.1 Redis-RS Stream Support Spike (1 hour)

**Goal**: Verify redis-rs supports all required stream operations

```rust
// Spike: Test what redis-rs actually supports
#[test]
fn test_redis_rs_stream_ops() {
    // XADD - existing?
    // XREAD - existing?
    // XREVRANGE - existing?
    // XTRIM - existing?
}
```

**If missing**:
- Option A: Implement raw Redis protocol for missing ops (adds 2-3h)
- Option B: Drop Stream benchmarking for v0.2.0 (scope reduction)
- Option C: Use `redis-cli` fallback (not recommended, adds latency)

**Expected outcome**: Either "all supported", "need raw protocol", or "drop streams v0.2.0"

**Action**: @user investigates and reports findings

### 0.2 Redlite API Coverage Spike (1 hour)

**Goal**: Verify Redlite has all 48 operations needed

```rust
// Spike: Check Redlite Db API
impl Client {
    // String ops - supported?
    fn set(&self, key: &str, value: &[u8]) -> Result<()> { ... }
    fn get(&self, key: &str) -> Result<Option<Vec<u8>>> { ... }
    fn incr(&self, key: &str) -> Result<i64> { ... }

    // List ops - supported?
    fn lpush(&self, key: &str, value: &[u8]) -> Result<i64> { ... }
    fn lpop(&self, key: &str) -> Result<Option<Vec<u8>>> { ... }

    // etc. - do these exist on Arc<Db>?
}
```

**Expected outcome**:
- "All 48 ops available" ✅
- "X ops need custom implementation" ⚠️ (adds time)
- "Y ops not available" 🔴 (scope reduction needed)

**Action**: @user explores Redlite codebase and reports

### 0.3 Architecture Spike: Trait-Based Approach (0.5 hours)

**Goal**: Verify trait-based client architecture compiles and works

```rust
// Spike code
use async_trait::async_trait;

#[async_trait]
pub trait RedisLikeClient: Send + Sync {
    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>>;
    async fn set(&self, key: &str, value: &[u8]) -> Result<()>;
}

pub struct RedisAdapter(redis::Client);
pub struct RedliteAdapter(Arc<Db>);

#[async_trait]
impl RedisLikeClient for RedisAdapter {
    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
        // Implementation
    }
}

#[async_trait]
impl RedisLikeClient for RedliteAdapter {
    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
        // Implementation
    }
}

// Usage:
async fn bench<C: RedisLikeClient>(client: &C) {
    let val = client.get("key").await?;
}
```

**Expected outcome**: Compiles, demonstrates architecture works

**Decision**: Approve this approach or propose alternative

---

## Phase 1: Specification Finalization (2-3 hours)

### 1.1 Complete Workload Scenarios in YAML

**Current**: 5 workloads in spec.yaml
**Target**: 18-21 realistic, well-defined workloads

**Add these to benchmark-spec.yaml**:

#### Core Scenarios (5 base + 3 variants)
```yaml
workloads:
  - name: "read_heavy"
    description: "80% read, 20% write - caching pattern"
    operations:
      - { type: "GET", weight: 50 }
      - { type: "HGET", weight: 15 }
      - { type: "LRANGE", weight: 10 }
      - { type: "SMEMBERS", weight: 5 }
      - { type: "SET", weight: 15 }
      - { type: "HSET", weight: 5 }

  - name: "write_heavy"
    description: "20% read, 80% write - logging/analytics"
    operations:
      - { type: "SET", weight: 40 }
      - { type: "HSET", weight: 20 }
      - { type: "LPUSH", weight: 15 }
      - { type: "SADD", weight: 10 }
      - { type: "XADD", weight: 10 }
      - { type: "GET", weight: 5 }

  - name: "truly_balanced"
    description: "Equal mix across ALL data types"
    operations:
      # Strings: 17%
      - { type: "GET", weight: 10 }
      - { type: "SET", weight: 7 }
      # Lists: 17%
      - { type: "LPUSH", weight: 10 }
      - { type: "LPOP", weight: 7 }
      # Hashes: 17%
      - { type: "HGET", weight: 10 }
      - { type: "HSET", weight: 7 }
      # Sets: 17%
      - { type: "SADD", weight: 10 }
      - { type: "SREM", weight: 7 }
      # Sorted Sets: 16%
      - { type: "ZADD", weight: 9 }
      - { type: "ZRANGE", weight: 7 }
      # Streams: 16%
      - { type: "XADD", weight: 9 }
      - { type: "XREAD", weight: 7 }

  - name: "read_only"
    description: "100% read - pure read workload"
    operations:
      - { type: "GET", weight: 30 }
      - { type: "HGET", weight: 20 }
      - { type: "LRANGE", weight: 15 }
      - { type: "SMEMBERS", weight: 15 }
      - { type: "ZRANGE", weight: 10 }
      - { type: "XRANGE", weight: 10 }

  - name: "write_only"
    description: "100% write - pure write workload"
    operations:
      - { type: "SET", weight: 25 }
      - { type: "HSET", weight: 20 }
      - { type: "LPUSH", weight: 15 }
      - { type: "SADD", weight: 15 }
      - { type: "ZADD", weight: 15 }
      - { type: "XADD", weight: 10 }

#### Data Structure Specific (6 scenarios)
```yaml
  - name: "cache_pattern"
    description: "KV cache - mostly GET/SET on strings"
    operations:
      - { type: "GET", weight: 70 }
      - { type: "SET", weight: 20 }
      - { type: "INCR", weight: 5 }
      - { type: "DEL", weight: 5 }

  - name: "session_store"
    description: "User sessions - hash operations"
    operations:
      - { type: "HGET", weight: 50 }
      - { type: "HSET", weight: 30 }
      - { type: "HGETALL", weight: 10 }
      - { type: "HDEL", weight: 5 }
      - { type: "EXPIRE", weight: 5 }

  - name: "message_queue"
    description: "Task queue - FIFO list operations"
    operations:
      - { type: "LPUSH", weight: 45 }
      - { type: "RPOP", weight: 45 }
      - { type: "LLEN", weight: 10 }

  - name: "leaderboard"
    description: "Ranking system - sorted set operations"
    operations:
      - { type: "ZADD", weight: 40 }
      - { type: "ZRANGE", weight: 30 }
      - { type: "ZRANK", weight: 20 }
      - { type: "ZSCORE", weight: 10 }

  - name: "event_stream"
    description: "Event sourcing - stream operations"
    operations:
      - { type: "XADD", weight: 50 }
      - { type: "XREAD", weight: 30 }
      - { type: "XRANGE", weight: 15 }
      - { type: "XLEN", weight: 5 }

  - name: "social_graph"
    description: "Social network - set operations"
    operations:
      - { type: "SADD", weight: 30 }
      - { type: "SISMEMBER", weight: 40 }
      - { type: "SMEMBERS", weight: 20 }
      - { type: "SINTER", weight: 10 }

#### Extreme/Stress Scenarios (5 scenarios)
```yaml
  - name: "hot_keys"
    description: "Skewed access - 90% of ops on 10% of keys"
    operations:
      - { type: "GET", weight: 90 }
      - { type: "SET", weight: 10 }
    note: "Benchmark runner will concentrate ops on first 10% of keys"

  - name: "write_storm"
    description: "Burst write load - stress memory and throughput"
    operations:
      - { type: "SET", weight: 50 }
      - { type: "LPUSH", weight: 30 }
      - { type: "SADD", weight: 20 }
    note: "Run with high concurrency (16+) and short duration"

  - name: "read_storm"
    description: "Burst read load - stress memory and caching"
    operations:
      - { type: "GET", weight: 50 }
      - { type: "LRANGE", weight: 30 }
      - { type: "SMEMBERS", weight: 20 }

  - name: "mixed_storm"
    description: "Alternating read/write bursts"
    operations:
      - { type: "GET", weight: 45 }
      - { type: "SET", weight: 45 }
      - { type: "DEL", weight: 10 }

  - name: "range_operations_heavy"
    description: "High volume of expensive range scans"
    operations:
      - { type: "LRANGE", weight: 25 }
      - { type: "ZRANGE", weight: 25 }
      - { type: "XRANGE", weight: 25 }
      - { type: "SMEMBERS", weight: 15 }
      - { type: "HGETALL", weight: 10 }
```

### 1.2 Benchmarking Protocol Definition

**Add to README or new PROTOCOL.md**:

#### Setup/Teardown Rules
```markdown
## Benchmarking Protocol

### Setup Phase
1. Connect to backend
2. FLUSHDB to clear any previous data
3. Run setup code for the operation:
   - GET: Populate 1K-100K keys with 100-byte values
   - LPUSH: Create empty list
   - HSET: Create empty hash
   - ZADD: Create empty sorted set
   - XADD: Create empty stream
   - etc.
4. WAIT for all data to be committed (important for file-backed systems)
5. Run warmup iterations (1000 ops)

### Measurement Phase
1. Start timer (high-resolution)
2. Execute N iterations of the operation
3. Stop timer
4. Record all latency values

### Teardown Phase
1. FLUSHDB to clear data
2. Next operation's setup begins

### Latency Boundaries
- Include: Time from client.operation() call to Result return
- Include: Network round-trip (for server tests)
- Exclude: Setup/teardown time
- Exclude: Warmup iterations
```

#### Error Handling
```markdown
## Error Handling

**Approach**: Collect error rates separately from latency

For each operation:
- Success count: Number of operations that completed successfully
- Error count: Number that failed (e.g., key not found)
- Error rate %: Errors / (Success + Errors)
- Latency: ONLY for successful operations

**Policy**:
- If error rate > 5% across all ops in a scenario, flag the run
- Individual operation failures don't stop the benchmark
- Network timeouts fail the benchmark (indicate server down)
```

#### Percentile Calculation
```markdown
## Metrics

For each operation:
- Count: Total successful operations
- Min latency: Fastest operation (µs)
- Max latency: Slowest operation (µs)
- Average: Sum / Count (µs)
- P50: 50th percentile / median (µs)
- P95: 95th percentile (µs)
- P99: 99th percentile (µs)
- Stddev: Standard deviation (µs)
- Throughput: Count / Duration (ops/sec)

Implementation: Use `tdigest-rs` for memory-efficient percentile calculation
```

---

## Phase 2: Trait-Based Client Architecture (11-14 hours)

### 2.1 Define RedisLikeClient Trait (1 hour)

**Create: src/client/mod.rs**

```rust
use async_trait::async_trait;

#[async_trait]
pub trait RedisLikeClient: Send + Sync + Clone {
    // ========== STRING OPERATIONS ==========
    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>>;
    async fn set(&self, key: &str, value: &[u8]) -> Result<()>;
    async fn incr(&self, key: &str) -> Result<i64>;
    // ... 4 more string ops

    // ========== LIST OPERATIONS ==========
    async fn lpush(&self, key: &str, value: &[u8]) -> Result<i64>;
    async fn lpop(&self, key: &str) -> Result<Option<Vec<u8>>>;
    // ... 5 more list ops

    // ========== HASH OPERATIONS ==========
    async fn hget(&self, key: &str, field: &str) -> Result<Option<Vec<u8>>>;
    async fn hset(&self, key: &str, field: &str, value: &[u8]) -> Result<i64>;
    // ... 5 more hash ops

    // ========== SET OPERATIONS ==========
    async fn sadd(&self, key: &str, members: &[&[u8]]) -> Result<i64>;
    async fn srem(&self, key: &str, members: &[&[u8]]) -> Result<i64>;
    // ... 5 more set ops

    // ========== SORTED SET OPERATIONS ==========
    async fn zadd(&self, key: &str, members: &[(f64, &[u8])]) -> Result<i64>;
    async fn zrem(&self, key: &str, members: &[&[u8]]) -> Result<i64>;
    // ... 6 more zset ops

    // ========== STREAM OPERATIONS ==========
    async fn xadd(&self, key: &str, id: &str, items: &[(&str, &[u8])]) -> Result<String>;
    async fn xread(&self, keys: &[&str], ids: &[&str]) -> Result<Vec<StreamEntry>>;
    // ... 5 more stream ops

    // ========== KEY OPERATIONS ==========
    async fn del(&self, key: &str) -> Result<i64>;
    async fn expire(&self, key: &str, seconds: usize) -> Result<bool>;
    // ... 3 more key ops

    // ========== UTILITY ==========
    async fn flushdb(&self) -> Result<()>;
    async fn ping(&self) -> Result<()>;
}

// Type aliases for convenience
pub type ClientResult<T> = Result<T, ClientError>;

#[derive(Debug)]
pub struct ClientError(pub String);
```

**Benefits**:
- ✅ Single definition of all 48 operations
- ✅ Each backend implements once
- ✅ Mockable for tests
- ✅ Easy to add new backends

### 2.2 Redis Adapter (2 hours)

**Create: src/client/redis_adapter.rs**

```rust
use async_trait::async_trait;
use super::*;

#[derive(Clone)]
pub struct RedisClient(redis::Client);

impl RedisClient {
    pub fn new(url: &str) -> ClientResult<Self> {
        let client = redis::Client::open(url)
            .map_err(|e| ClientError(e.to_string()))?;
        Ok(RedisClient(client))
    }
}

#[async_trait]
impl RedisLikeClient for RedisClient {
    async fn get(&self, key: &str) -> ClientResult<Option<Vec<u8>>> {
        let mut conn = self.0.get_connection()
            .map_err(|e| ClientError(e.to_string()))?;
        let result: Option<Vec<u8>> = redis::cmd("GET")
            .arg(key)
            .query(&mut conn)
            .map_err(|e| ClientError(e.to_string()))?;
        Ok(result)
    }

    async fn set(&self, key: &str, value: &[u8]) -> ClientResult<()> {
        let mut conn = self.0.get_connection()
            .map_err(|e| ClientError(e.to_string()))?;
        redis::cmd("SET")
            .arg(key)
            .arg(value)
            .execute(&mut conn);
        Ok(())
    }

    // ... 46 more operations

    async fn flushdb(&self) -> ClientResult<()> {
        let mut conn = self.0.get_connection()
            .map_err(|e| ClientError(e.to_string()))?;
        redis::cmd("FLUSHDB").execute(&mut conn);
        Ok(())
    }
}
```

**Key points**:
- Error mapping from redis crate to ClientError
- Each operation wraps redis commands
- Blocking calls (redis crate doesn't have async yet, so we just make them sync in async context)

### 2.3 Redlite Embedded Adapter (4-6 hours)

**Create: src/client/redlite_adapter.rs**

Challenges:
- Arc<Db> needs careful cloning
- Type conversions between Redlite's types and generic Vec<u8>
- Stream operations might not exist
- Need to handle Redlite-specific errors

```rust
use async_trait::async_trait;
use super::*;
use redlite::{Db};
use std::sync::Arc;

#[derive(Clone)]
pub struct RedliteEmbeddedClient(Arc<Db>);

#[async_trait]
impl RedisLikeClient for RedliteEmbeddedClient {
    async fn get(&self, key: &str) -> ClientResult<Option<Vec<u8>>> {
        self.0.get(key)
            .map_err(|e| ClientError(e.to_string()))
    }

    async fn set(&self, key: &str, value: &[u8]) -> ClientResult<()> {
        self.0.set(key, value, None)
            .map_err(|e| ClientError(e.to_string()))
    }

    // ... implementation details
}
```

**Estimated breakdown**:
- String ops: 1 hour (straightforward)
- List ops: 1 hour (LPUSH, LPOP, LRANGE, etc.)
- Hash ops: 1 hour (HGET, HSET, etc.)
- Set ops: 0.5 hours (SADD, SREM, etc.)
- Sorted Set ops: 1 hour (ZADD, ZRANGE, type conversions)
- Stream ops: 1-2 hours (might not exist, need investigation)
- Total: 5-6 hours

### 2.4 Redlite Server Adapter (1 hour)

**Create: src/client/redlite_server_adapter.rs**

- Same as Redis adapter but connects to different port
- Reuse Redis code with small modifications

### 2.5 Tests & Validation (3-4 hours)

**Create: src/client/tests.rs**

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_redis_get_set() {
        let client = RedisClient::new("redis://127.0.0.1:6379").unwrap();
        client.flushdb().await.unwrap();

        client.set("key1", b"value1").await.unwrap();
        let result = client.get("key1").await.unwrap();
        assert_eq!(result, Some(b"value1".to_vec()));
    }

    #[tokio::test]
    async fn test_redlite_list_ops() {
        let client = RedliteEmbeddedClient::new_memory().unwrap();
        client.flushdb().await.unwrap();

        let pushed = client.lpush("list1", b"item1").await.unwrap();
        assert_eq!(pushed, 1);

        let popped = client.lpop("list1").await.unwrap();
        assert_eq!(popped, Some(b"item1".to_vec()));
    }

    // ... many more tests covering each operation × each client
}
```

**Testing strategy**:
- Unit test each operation for each client
- Property tests for consistency (GET/SET roundtrip)
- Integration tests with actual servers

---

## Phase 3: Benchmark Functions (10-13 hours)

### 3.1 Generic Measurement Infrastructure (1-2 hours)

**Create: src/benchmark/mod.rs**

```rust
use std::time::Instant;
use std::collections::BTreeMap;

pub struct BenchmarkResult {
    pub operation: String,
    pub operation_type: OperationType, // Get, Set, Lpush, etc.
    pub data_type: DataType,           // String, List, Hash, etc.
    pub dataset_size: usize,
    pub concurrency: usize,
    pub scenario: String,
    pub iterations: usize,
    pub successful_ops: usize,
    pub failed_ops: usize,
    pub latencies_us: Vec<f64>,        // All latencies in microseconds
    pub throughput_ops_per_sec: f64,
    pub duration_secs: f64,
}

impl BenchmarkResult {
    pub fn min_latency_us(&self) -> f64 { self.latencies_us.iter().copied().fold(f64::INFINITY, f64::min) }
    pub fn max_latency_us(&self) -> f64 { self.latencies_us.iter().copied().fold(0.0, f64::max) }
    pub fn avg_latency_us(&self) -> f64 { self.latencies_us.iter().sum::<f64>() / self.latencies_us.len() as f64 }
    pub fn p50_latency_us(&self) -> f64 { self.percentile(0.50) }
    pub fn p95_latency_us(&self) -> f64 { self.percentile(0.95) }
    pub fn p99_latency_us(&self) -> f64 { self.percentile(0.99) }
    pub fn error_rate(&self) -> f64 { self.failed_ops as f64 / (self.successful_ops + self.failed_ops) as f64 }

    fn percentile(&self, p: f64) -> f64 {
        let mut sorted = self.latencies_us.clone();
        sorted.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let idx = ((p * sorted.len() as f64) as usize).min(sorted.len() - 1);
        sorted[idx]
    }
}
```

### 3.2 Operation-Specific Benchmarks (6-8 hours)

**Create: src/benchmark/operations.rs**

Key insight: Can't use one generic template. Each operation needs custom setup.

```rust
pub struct OperationBenchmark {
    operation: String,
    setup: Box<dyn Fn(&Client) -> Result<()>>,
    execute: Box<dyn Fn(&Client) -> Result<()>>,
    iterations: usize,
}

// Example: GET benchmark
pub async fn bench_get<C: RedisLikeClient>(
    client: &C,
    dataset_size: usize,
    iterations: usize,
) -> Result<BenchmarkResult> {
    // Setup: Populate keys
    for i in 0..dataset_size {
        client.set(&format!("key_{}", i), b"value_100_bytes_long").await?;
    }

    let mut rng = rand::thread_rng();
    let mut latencies = Vec::with_capacity(iterations);

    for i in 0..iterations {
        let key = format!("key_{}", rng.gen_range(0..dataset_size));
        let start = Instant::now();
        match client.get(&key).await {
            Ok(_) => latencies.push(start.elapsed().as_secs_f64() * 1_000_000.0),
            Err(_) => { /* count error */ }
        }
    }

    Ok(BenchmarkResult {
        operation: "GET".to_string(),
        latencies_us: latencies,
        // ... other fields
    })
}

// Example: HGETALL benchmark (more complex)
pub async fn bench_hgetall<C: RedisLikeClient>(
    client: &C,
    dataset_size: usize,
    iterations: usize,
) -> Result<BenchmarkResult> {
    // Setup: Create hashes with 100 fields each
    for hash_id in 0..dataset_size {
        for field_id in 0..100 {
            client.hset(
                &format!("hash_{}", hash_id),
                &format!("field_{}", field_id),
                b"value_100_bytes"
            ).await?;
        }
    }

    let mut rng = rand::thread_rng();
    let mut latencies = Vec::with_capacity(iterations);

    for _ in 0..iterations {
        let hash_id = rng.gen_range(0..dataset_size);
        let start = Instant::now();
        match client.hgetall(&format!("hash_{}", hash_id)).await {
            Ok(_) => latencies.push(start.elapsed().as_secs_f64() * 1_000_000.0),
            Err(_) => { /* count error */ }
        }
    }

    Ok(BenchmarkResult { /* ... */ })
}
```

**Benchmarks needed** (organized by data type):

String (7):
- bench_get, bench_set, bench_incr, bench_append, bench_strlen, bench_mget, bench_mset

List (7):
- bench_lpush, bench_rpush, bench_lpop, bench_rpop, bench_llen, bench_lrange, bench_lindex

Hash (7):
- bench_hset, bench_hget, bench_hgetall, bench_hmget, bench_hlen, bench_hdel, bench_hincrby

Set (7):
- bench_sadd, bench_srem, bench_smembers, bench_sismember, bench_scard, bench_spop, bench_srandmember

Sorted Set (8):
- bench_zadd, bench_zrem, bench_zrange, bench_zrangebyscore, bench_zscore, bench_zrank, bench_zcard, bench_zcount

Stream (7):
- bench_xadd, bench_xlen, bench_xrange, bench_xrevrange, bench_xread, bench_xdel, bench_xtrim

Key (5):
- bench_del, bench_exists, bench_type, bench_expire, bench_ttl

**Total: 48 functions, each 10-30 lines, custom setup**

### 3.3 Concurrent Execution (2-3 hours)

**Create: src/benchmark/concurrent.rs**

Support both async and OS-thread concurrency:

```rust
pub async fn run_benchmark_async<C: RedisLikeClient>(
    client: &C,
    num_connections: usize,
    benchmark_fn: async fn(&C, ...) -> Result<BenchmarkResult>,
) -> Result<BenchmarkResult> {
    let mut tasks = vec![];
    let ops_per_connection = iterations / num_connections;

    for _ in 0..num_connections {
        let client = client.clone();
        tasks.push(tokio::spawn(async move {
            // Execute benchmark on this "connection"
            benchmark_fn(&client, ops_per_connection).await
        }));
    }

    // Aggregate results from all connections
    // ...
}

pub fn run_benchmark_threaded<C: RedisLikeClient>(
    client: &C,
    num_connections: usize,
    benchmark_fn: fn(&C, ...) -> Result<BenchmarkResult>,
) -> Result<BenchmarkResult> {
    let mut handles = vec![];

    for _ in 0..num_connections {
        let client = client.clone();
        handles.push(std::thread::spawn(move || {
            // Execute benchmark on this thread
            benchmark_fn(&client, ops_per_connection)
        }));
    }

    // Aggregate results...
}
```

---

## Phase 4: Workload Scenarios (4-6 hours)

### 4.1 Scenario Runner (2-3 hours)

**Create: src/benchmark/scenarios.rs**

```rust
use rand::distributions::WeightedIndex;
use rand::Rng;
use std::time::Duration;

pub struct WorkloadScenario {
    pub name: String,
    pub operations: Vec<(String, f64)>, // (op_name, weight)
}

pub async fn run_scenario<C: RedisLikeClient>(
    client: &C,
    scenario: &WorkloadScenario,
    dataset_size: usize,
    duration: Duration,
    concurrency: usize,
) -> Result<ScenarioResult> {
    let mut rng = rand::thread_rng();
    let weights: Vec<_> = scenario.operations.iter().map(|(_, w)| w).collect();
    let dist = WeightedIndex::new(weights)?;

    let start = Instant::now();
    let mut operation_results: HashMap<String, Vec<BenchmarkResult>> = HashMap::new();

    while start.elapsed() < duration {
        let op_idx = dist.sample(&mut rng);
        let (op_name, _weight) = &scenario.operations[op_idx];

        // Execute the operation (dispatch to correct benchmark)
        let result = execute_operation(client, op_name, dataset_size).await?;
        operation_results.entry(op_name.clone()).or_insert_with(Vec::new).push(result);
    }

    // Aggregate results
    let scenario_result = ScenarioResult {
        name: scenario.name.clone(),
        total_duration: start.elapsed(),
        total_operations: operation_results.values().map(|v| v.len()).sum(),
        operation_results,
    };

    Ok(scenario_result)
}

async fn execute_operation<C: RedisLikeClient>(
    client: &C,
    op_name: &str,
    dataset_size: usize,
) -> Result<BenchmarkResult> {
    match op_name {
        "GET" => bench_get(client, dataset_size, 1).await,
        "SET" => bench_set(client, dataset_size, 1).await,
        "LPUSH" => bench_lpush(client, dataset_size, 1).await,
        // ... 45 more operations
        _ => Err(Error::UnknownOperation(op_name.to_string())),
    }
}
```

### 4.2 Load Scenarios from YAML (1-2 hours)

**Create: src/benchmark/spec_loader.rs**

```rust
use serde::{Deserialize, Serialize};
use std::fs;

#[derive(Deserialize)]
pub struct BenchmarkSpec {
    pub workloads: Vec<WorkloadSpec>,
}

#[derive(Deserialize)]
pub struct WorkloadSpec {
    pub name: String,
    pub description: String,
    pub operations: Vec<OperationWeight>,
}

#[derive(Deserialize)]
pub struct OperationWeight {
    #[serde(rename = "type")]
    pub op_type: String,
    pub weight: f64,
}

pub fn load_scenarios(spec_path: &str) -> Result<Vec<WorkloadScenario>> {
    let content = fs::read_to_string(spec_path)?;
    let spec: BenchmarkSpec = serde_yaml::from_str(&content)?;

    let scenarios = spec.workloads
        .into_iter()
        .map(|w| WorkloadScenario {
            name: w.name,
            operations: w.operations
                .into_iter()
                .map(|o| (o.op_type, o.weight))
                .collect(),
        })
        .collect();

    Ok(scenarios)
}
```

### 4.3 Scenario Validation (1 hour)

**Create: tests/scenarios_test.rs**

```rust
#[test]
fn test_scenarios_valid() {
    let scenarios = load_scenarios("../spec/benchmark-spec.yaml").unwrap();

    // Each scenario must have operations
    for scenario in &scenarios {
        assert!(!scenario.operations.is_empty(),
                "Scenario {} has no operations", scenario.name);

        // Weights must sum to ~100
        let total_weight: f64 = scenario.operations.iter().map(|(_, w)| w).sum();
        assert!((100.0 - total_weight).abs() < 1.0,
                "Scenario {} weights don't sum to 100 (got {})", scenario.name, total_weight);
    }
}
```

---

## Phase 5: SQLite Results Storage (2-3 hours)

**Create: src/storage/sqlite.rs**

- Schema creation
- Result insertion
- Query examples
- No changes from original plan (this part was solid)

---

## Phase 6: CLI Interface (2-3 hours)

**Create: src/bin/redlite-bench.rs**

- Clap argument parsing
- Integration with benchmark runner
- Output formatting

---

## Phase 7: Testing & Validation (4-6 hours)

### Test Scope:
- Unit tests for each client impl (48 ops × 3 clients = 144 tests)
- Integration tests with real Redis
- Scenario validation tests
- Correctness tests (do operations return expected results?)
- Performance sanity checks (results are within expected range)

---

## Revised Timeline

| Phase | Task | Hours | Cumulative |
|---|---|---|---|
| **0** | **Dependency validation** | **2-3** | **2-3h** |
| 0.1 | Redis-RS stream ops spike | 1 | |
| 0.2 | Redlite API spike | 1 | |
| 0.3 | Trait architecture spike | 0.5 | |
| **1** | **Spec finalization** | **2-3** | **5-6h** |
| 1.1 | Complete workload scenarios | 1.5 | |
| 1.2 | Benchmarking protocol definition | 0.5-1 | |
| **2** | **Trait client architecture** | **11-14** | **17-23h** |
| 2.1 | Define trait | 1 | |
| 2.2 | Redis adapter | 2 | |
| 2.3 | Redlite embedded adapter | 4-6 | |
| 2.4 | Redlite server adapter | 1 | |
| 2.5 | Tests & validation | 3-4 | |
| **3** | **Benchmark functions** | **10-13** | **28-40h** |
| 3.1 | Measurement infrastructure | 1-2 | |
| 3.2 | 48 operation benchmarks | 6-8 | |
| 3.3 | Concurrent execution | 2-3 | |
| **4** | **Workload scenarios** | **4-6** | **33-46h** |
| 4.1 | Scenario runner | 2-3 | |
| 4.2 | Load from YAML | 1-2 | |
| 4.3 | Validation tests | 1 | |
| **5** | **SQLite storage** | **2-3** | **36-50h** |
| **6** | **CLI interface** | **2-3** | **39-53h** |
| **7** | **Testing & validation** | **4-6** | **44-60h** |
| **8** | **Documentation** | **1-2** | **46-62h** |

**Most Likely Estimate: 45-55 hours** (vs. original 30 hours)

**Best Case**: 44 hours (everything goes smoothly, no blockers)
**Worst Case**: 60+ hours (discovery of missing features in redis-rs or Redlite)

---

## Critical Path for v0.2.0

**MUST HAVE** (minimum viable):
1. ✅ Trait-based client architecture (Phase 2)
2. ✅ All 48 operations for Redis backend (Phase 2)
3. ✅ All 48 operations for Redlite embedded (Phase 2)
4. ✅ 8 core workload scenarios (subset of Phase 4)
5. ✅ Basic benchmarking (Phase 3)
6. ✅ Console output (already have)
7. ✅ Works with Redis + Redlite (Phase 2)

**OPTIONAL** (Phase 2 can defer):
- ❓ Redlite server adapter (defer to v0.2.1)
- ❓ SQLite storage (can use JSON initially)
- ❓ CLI interface (can use code examples initially)

**MINIMUM FOR v0.2.0**: Phases 0-3 + 4 (core) + basic output = ~40 hours

---

## Success Criteria v0.2.0

- [ ] All 48 operations implemented in trait
- [ ] All 48 ops work with Redis backend
- [ ] All 48 ops work with Redlite embedded
- [ ] 8 core scenarios defined and validated
- [ ] Concurrent benchmarking works (both async + threaded)
- [ ] Results accurate compared to redis-benchmark
- [ ] Console output showing latency percentiles
- [ ] Basic error handling and reporting
- [ ] Test coverage > 70% of critical paths

---

## Dependencies to Resolve (Phase 0)

Before starting Phase 2, you MUST:

1. [ ] Confirm redis-rs supports all stream ops
   - [ ] XADD ✓
   - [ ] XREAD ✓
   - [ ] XREVRANGE ✓
   - [ ] XTRIM ✓

2. [ ] Confirm Redlite Arc<Db> has all 48 operations
   - [ ] List ops: lpush, lpop, lrange, etc.
   - [ ] Stream ops: xadd, xread, etc.
   - [ ] All sorted set ops

3. [ ] Test trait-based architecture compiles
   - [ ] async_trait works or choose alternative
   - [ ] Polymorphism works as expected

4. [ ] Identify any Redlite-specific issues
   - [ ] Type conversions
   - [ ] Error handling
   - [ ] Arc cloning semantics

---

## Risk Register

| Risk | Likelihood | Impact | Mitigation |
|---|---|---|---|
| Redis-rs lacks stream ops | Medium | High | Phase 0 spike |
| Redlite missing ops | Medium | High | Phase 0 spike |
| Trait-based design has issues | Low | Medium | Phase 0 spike (0.5h) |
| Phase 2 takes 18+ hours | Medium | High | Identify early, adjust scope |
| Concurrent benchmarking complex | Medium | Medium | Use both async + threads |
| Testing takes >6 hours | Low | Low | Can defer to v0.2.1 |

---

## Next Steps

### Immediate (Today)

1. **Phase 0.1**: Spike redis-rs stream support (1h)
2. **Phase 0.2**: Spike Redlite API (1h)
3. **Phase 0.3**: Test trait architecture (0.5h)
4. **Decision**: Adjust timeline based on findings

### Week 1

5. **Phase 1**: Finalize spec with 18 workloads (2h)
6. **Phase 2.1**: Start trait definition (1h)
7. **Phase 2.2-2.5**: Implement adapters (10-14h)

### Week 2

8. **Phase 3**: Benchmark functions (10-13h)
9. **Phase 4**: Scenarios (4-6h)

### Week 3

10. **Phase 5-7**: Storage, CLI, testing (8-12h)

**Total: 45-55 hours over 2-3 weeks**

---

**Status**: 🟢 READY FOR EXECUTION (after Phase 0 spikes)

**Last Updated**: 2026-01-13
