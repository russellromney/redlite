# Redlite-Bench Benchmarking Protocol v0.2.0

This document defines the standard benchmarking protocol used across all redlite-bench implementations.

## Overview

The benchmarking process has three phases: **Setup**, **Measurement**, and **Teardown**. The goal is to produce consistent, reproducible latency metrics that reflect true operation performance.

---

## 1. Setup Phase

Before running any benchmarks, the test environment must be prepared.

### 1.1 Connection Establishment
1. Connect to the backend (Redis, Redlite, etc.)
2. Verify connectivity with a `PING` command
3. Confirm server is responsive (if not, fail loudly)

### 1.2 Data Clearing
```
FLUSHDB
```
- Clear all previous data in the database
- Ensure a clean slate for reproducibility
- Wait for FLUSHDB to complete

### 1.3 Data Population (Operation-Specific)

Before measuring each operation, populate the dataset with test data:

#### String Operations
- Create 1K-100K keys with 100-byte values
- Example: `SET key_0 "value...100 bytes"`

#### List Operations
- Create empty lists or pre-populated lists (depends on operation)
- LPUSH: Empty list
- LRANGE: List with 1K-100K elements
- LINDEX: List with 1K-100K elements

#### Hash Operations
- Create hashes with 100 fields each
- Example: `HSET hash_0 field_0 "value" field_1 "value" ...`

#### Set Operations
- Create sets with 1K-100K members
- Example: `SADD set_0 member_0 member_1 ...`

#### Sorted Set Operations
- Create sorted sets with 1K-100K members and scores
- Example: `ZADD zset_0 1.0 member_0 2.0 member_1 ...`

#### Stream Operations
- Create empty streams
- Some operations (XREAD, XRANGE) need pre-populated streams

#### Key Management
- Create keys for DEL, EXPIRE, TYPE, etc.

### 1.4 Commit & Persistence
```
// For file-based backends (Redlite):
BGSAVE or equivalent
// Wait for commit to disk
```
- Important for SQLite-backed systems like Redlite
- Ensures data is written before measurement

### 1.5 Warmup Iterations
Execute 1,000 iterations of the operation without measuring:
- Allows caches to warm up
- System stabilizes
- Results in more stable latency measurements
- Discard all warmup measurements

### 1.6 Final Verification
```
PING
```
- Confirm server is still responsive before measurement

---

## 2. Measurement Phase

Measure the actual operation latency.

### 2.1 Timer Initialization
- Use high-resolution timer (nanosecond precision minimum)
- Reset any performance monitoring

### 2.2 Operation Loop

For each iteration of N total iterations:

```pseudocode
for i in 0..N:
    start_time = clock_ns()
    try:
        result = client.operation(args...)
        end_time = clock_ns()
        latency_us = (end_time - start_time) / 1000.0
        latencies.push(latency_us)
        successes += 1
    catch error:
        end_time = clock_ns()
        latency_us = (end_time - start_time) / 1000.0
        failures += 1
        // Still record time but mark as error
        error_latencies.push(latency_us)
```

### 2.3 Timing Boundaries

**INCLUDE in latency measurement:**
- Time from `client.operation()` call
- To `Result` return (success or error)
- Network round-trip time (for server-based tests)
- Serialization and deserialization time
- Deserialization of response

**EXCLUDE from latency measurement:**
- Setup phase time
- Data preparation time
- Warmup iterations
- Teardown time

### 2.4 Latency Recording

Store all latencies in microseconds with float precision:
- Min latency: 0.1 µs is possible
- Expected range: 10 µs - 10,000 µs depending on operation
- Keep full array of all measurements for percentile calculation

### 2.5 Stopping Criteria

One of:
- Reach N iterations (common approach)
- Reach time limit (e.g., 30 seconds)
- Operations complete (for bounded operations)

---

## 3. Teardown Phase

Clean up after each operation benchmark.

### 3.1 Data Clearing
```
FLUSHDB
```
- Clear all test data
- Ready for next operation's setup

### 3.2 Connection Cleanup
- Don't close connection (reuse for next operation)
- Just clear database state

---

## 4. Error Handling

### 4.1 Error Types

**Transient Errors** (continue benchmarking):
- Key not found
- Type mismatch (e.g., HSET on a string)
- Out of range (e.g., LINDEX beyond list length)
- Wrong number of arguments

**Fatal Errors** (fail benchmark):
- Connection lost
- Network timeout (> 30 seconds)
- Server crash
- Out of memory (server-side)
- Permission denied

### 4.2 Error Metrics

For each operation benchmark:
- **success_count**: Number of operations that completed successfully
- **error_count**: Number that returned an error
- **error_rate**: error_count / (success_count + error_count)
- **latency_on_errors**: Track latency separately for errors?
  - Recommendation: Include in same measurement (error still takes time)

### 4.3 Error Handling Policy

- **Error Rate Threshold**: If error_rate > 5% across all ops in a scenario:
  - Flag the run as "degraded"
  - Mark results clearly
  - Don't discard results (still useful)

- **Individual Operation Failures**: Don't stop benchmark
  - Continue to next iteration
  - Record both success and error metrics

- **Network Timeouts**: Stop immediately
  - Indicates server is down or unreachable
  - No point continuing

---

## 5. Latency Percentile Calculation

### 5.1 Percentile Definitions

For a dataset of latencies sorted in ascending order:

- **Min (p0)**: Minimum latency value
- **Max (p100)**: Maximum latency value
- **P50 (median)**: 50th percentile - half of ops faster, half slower
- **P95**: 95th percentile - 95% of ops faster than this
- **P99**: 99th percentile - 99% of ops faster than this

### 5.2 Calculation Method

**Simple method** (sufficient for most cases):
```python
def percentile(values, p):
    """Calculate pth percentile"""
    sorted_vals = sorted(values)
    idx = int((p / 100.0) * len(sorted_vals))
    return sorted_vals[min(idx, len(sorted_vals) - 1)]
```

**Recommended method** (more accurate):
Use linear interpolation between indices:
```python
def percentile_interpolated(values, p):
    sorted_vals = sorted(values)
    k = (p / 100.0) * (len(sorted_vals) - 1)
    floor_idx = int(k)
    ceil_idx = min(floor_idx + 1, len(sorted_vals) - 1)

    if floor_idx == ceil_idx:
        return sorted_vals[floor_idx]

    frac = k - floor_idx
    return sorted_vals[floor_idx] * (1 - frac) + sorted_vals[ceil_idx] * frac
```

### 5.3 High-Volume Datasets

For benchmarks with 1M+ latency samples:
- Consider using **T-Digest** for memory efficiency
- T-Digest: 1MB memory can store 1M samples and calculate percentiles
- See: [tdigest-rs](https://docs.rs/tdigest/)

---

## 6. Metrics Collected

### 6.1 Per-Operation Metrics

For each operation benchmark, collect:

**Counts:**
- `count`: Total successful operations
- `errors`: Total failed operations
- `error_rate`: Errors / (Errors + Count) %

**Latency (microseconds):**
- `min_us`: Minimum latency
- `max_us`: Maximum latency
- `avg_us`: Average (arithmetic mean)
- `stddev_us`: Standard deviation
- `p50_us`: 50th percentile (median)
- `p95_us`: 95th percentile
- `p99_us`: 99th percentile

**Throughput:**
- `throughput_ops_sec`: count / duration_seconds
- `throughput_mb_sec`: (total_bytes_transferred) / duration_seconds (if applicable)

**Metadata:**
- `operation`: Operation name (GET, SET, LPUSH, etc.)
- `backend`: Backend tested (Redis, Redlite, etc.)
- `dataset_size`: Number of keys/objects tested
- `scenario`: Workload scenario (if part of a scenario run)
- `timestamp`: When the benchmark was run
- `duration_sec`: Total duration of measurement phase

### 6.2 Aggregated Scenario Metrics

When running a workload scenario (mix of operations):

- `scenario_name`: e.g., "read_heavy"
- `total_operations`: Sum of all ops in scenario
- `total_duration_sec`: Total scenario runtime
- `operations`: Array of per-operation results
- `combined_throughput_ops_sec`: total_operations / total_duration_sec
- `combined_p95_us`: Percentile across all operations (not per-op)

---

## 7. Backend-Specific Considerations

### 7.1 Redis (tcp://localhost:6379)
- Standard Redis protocol via redis-rs
- Typical latencies: 100-500 µs (local network)
- Handle pipelining if enabled

### 7.2 Redlite Embedded (in-process)
- Uses Arc<Db> directly
- No network latency
- Typical latencies: 1-100 µs (in-process)
- Includes mutex contention

### 7.3 Redlite Server (tcp://localhost:6380)
- Redlite instance running as separate process
- Network latency similar to Redis
- Typical latencies: 100-500 µs

### 7.4 Timing Overhead
**Network-based backends**: Includes full round-trip
**Embedded backends**: Includes function call and mutex overhead

---

## 8. Data Value Generation

### 8.1 String Values
- All benchmark string values are 100 bytes
- Content: `"value_" + random 94 bytes`
- Consistent across all operations
- Rationale: Represents typical cache value size

### 8.2 Keys
- Format: `key_N` where N is 0 to dataset_size-1
- ASCII, no special characters
- Used across all operations for consistency

### 8.3 Scores (Sorted Sets)
- Uniform random float between 0.0 and 1000.0
- Used for ZADD, ZRANGE, ZRANGEBYSCORE

### 8.4 Members/Hashes
- String format: `member_N` or `field_N`
- Consistent naming across operations

---

## 9. Concurrency Modes

### 9.1 Async (Tokio-based)
- Each "connection" is a client clone in a separate task
- Measured latency is wall-clock time
- Concurrency: Multiple operations may overlap

### 9.2 Threaded (OS threads)
- Each "connection" is an OS thread
- Measured latency is wall-clock time
- Concurrency: True parallelism on multi-core

### 9.3 Sequential (Baseline)
- All operations on single connection
- No concurrency
- Baseline for comparison

---

## 10. Validation & Sanity Checks

### 10.1 Pre-Benchmark Checks
- [ ] Server is responsive (PING succeeds)
- [ ] Database is empty (DBSIZE = 0 after FLUSHDB)
- [ ] Network connectivity is stable
- [ ] Correct number of iterations configured

### 10.2 During Benchmark
- [ ] No unexpected errors (error_rate < 5%)
- [ ] Latencies in reasonable range (not all 0 or all max_int)
- [ ] Standard deviation is not zero (indicates timing resolution)

### 10.3 Post-Benchmark
- [ ] Calculate summary statistics
- [ ] Flag anomalies (e.g., p99 > 10x average)
- [ ] Compare with baseline if available

### 10.4 Anomaly Detection

Log warnings for:
- **Bimodal distribution**: p99 > 5x p50 (indicates GC pauses or OS scheduling)
- **All zeros**: All latencies = 0 (timing resolution too coarse)
- **Outlier operations**: One operation 10x slower than others
- **High variance**: stddev > average (unstable system)

---

## 11. Reproducibility

To ensure reproducible results:

1. **Fixed random seed**: Use same seed for key generation
2. **Consistent dataset**: Same keys, values, sizes
3. **Same operation order**: Always GET, SET, LPUSH, etc. in same order
4. **Isolated environment**: No other processes competing
5. **Stable machine**: No CPU frequency scaling, power saving
6. **Documented environment**:
   - OS version
   - CPU model
   - Memory available
   - Network latency
   - Other running processes

---

## 12. Output Format

### 12.1 Console (Human-Readable)
```
Operation: GET
Backend: Redis
Dataset Size: 10000
Iterations: 100000
Duration: 5.23 seconds

Results:
  Throughput: 19,121 ops/sec
  Min Latency: 12.3 µs
  Max Latency: 512.4 µs
  Average Latency: 52.3 µs
  P50 (median): 48.2 µs
  P95: 78.5 µs
  P99: 125.3 µs
  Stddev: 18.7 µs
  Errors: 0 (0.0% error rate)
```

### 12.2 JSON (Machine-Readable)
```json
{
  "operation": "GET",
  "backend": "redis",
  "dataset_size": 10000,
  "concurrency": 1,
  "iterations": 100000,
  "duration_sec": 5.23,
  "successful_ops": 100000,
  "failed_ops": 0,
  "error_rate_percent": 0.0,
  "throughput_ops_sec": 19121.0,
  "latency": {
    "min_us": 12.3,
    "max_us": 512.4,
    "avg_us": 52.3,
    "stddev_us": 18.7,
    "p50_us": 48.2,
    "p95_us": 78.5,
    "p99_us": 125.3
  },
  "timestamp": "2026-01-13T10:30:00Z",
  "metadata": {
    "run_id": "bench-001",
    "notes": "Local Redis instance"
  }
}
```

### 12.3 CSV (Spreadsheet-Compatible)
```
operation,backend,dataset_size,concurrency,iterations,duration_sec,throughput_ops_sec,min_us,max_us,avg_us,stddev_us,p50_us,p95_us,p99_us,error_rate_percent
GET,redis,10000,1,100000,5.23,19121.0,12.3,512.4,52.3,18.7,48.2,78.5,125.3,0.0
```

---

## 13. Example Benchmark Run

### Setup
```rust
client.flushdb().await?;

// Populate 10K keys
for i in 0..10000 {
    client.set(&format!("key_{}", i), b"value_100_bytes_long").await?;
}

// Wait for disk sync (Redlite)
client.bgsave().await?;

// Warmup
for _ in 0..1000 {
    let key = format!("key_{}", rand::random::<usize>() % 10000);
    client.get(&key).await?;
}
```

### Measurement
```rust
let mut rng = thread_rng();
let mut latencies = Vec::with_capacity(100_000);

let start = Instant::now();
for _ in 0..100_000 {
    let key = format!("key_{}", rng.gen_range(0..10000));
    let op_start = Instant::now();
    match client.get(&key).await {
        Ok(_) => {
            let latency_us = op_start.elapsed().as_secs_f64() * 1_000_000.0;
            latencies.push(latency_us);
        }
        Err(_) => {
            errors += 1;
        }
    }
}
let duration = start.elapsed();
```

### Results
```
Operation: GET
Duration: 5.23 seconds
Iterations: 100,000
Throughput: 19,121 ops/sec
P50: 48.2 µs
P95: 78.5 µs
P99: 125.3 µs
```

---

## 14. Deviation from Protocol

If you need to deviate from this protocol:
1. Document the deviation clearly
2. Explain why (e.g., "Warmup disabled for comparison")
3. Mark results as "non-standard"
4. Still report all metrics

Example:
```
Note: This benchmark was run with WARMUP_ITERATIONS=0 to measure cold-cache performance
```

---

## References

- [Redis Protocol Specification](https://redis.io/docs/latest/develop/reference/protocol-spec/)
- [HdrHistogram](http://hdrhistogram.org/) - Accurate latency measurement
- [T-Digest](https://github.com/tdunning/t-digest) - Streaming percentiles

**Protocol Version**: 0.2.0
**Last Updated**: 2026-01-13
**Status**: Finalized
