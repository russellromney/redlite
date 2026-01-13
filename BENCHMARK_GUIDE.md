# Redlite Benchmark System - Quick Reference Guide

## Setup

### Start Required Services
```bash
# Start Redis (if not already running)
redis-server &

# Start Redlite servers
bash benches/setup_services.sh
# Or manually:
target/release/redlite --addr 127.0.0.1:7381 --backend sqlite --storage memory &
target/release/redlite --addr 127.0.0.1:7382 --backend sqlite --storage file --db /tmp/redlite.db &
```

### Verify Backend Connectivity
```bash
redis-cli -p 6379 PING  # Redis
redis-cli -p 7381 PING  # Redlite Server (Memory)
redis-cli -p 7382 PING  # Redlite Server (File)
```

## Running Benchmarks

### Comprehensive Benchmark (All Backends)
```bash
cargo bench --bench comprehensive_comparison
```

**Output includes**:
- Availability check for all backends
- GET and SET latencies across 1-8 connections
- Throughput (ops/sec) for each configuration
- Summary tables comparing all backends

### Quick Verification Tests

**Test 1: Concurrent Operations**
```bash
cargo bench --bench test_concurrent
```
Verifies async/await integration works correctly.
- Single connection baseline: ~3.28ms for 1000 ops
- 4-connection concurrent: ~4.61ms for 1000 ops

**Test 2: Server Connectivity**
```bash
cargo bench --bench test_server_connection
```
Tests Rust redis client connection and operations on Redlite Server.
- Connection establishment
- PING/SET/GET operations
- Bulk operations (100 keys)

**Test 3: Step-by-Step Diagnostics**
```bash
cargo bench --bench test_step_by_step
```
Tests each backend component individually:
- Redlite Embedded (Arc<Db> cloning)
- Redis Client (connection reuse)
- Redlite Server (network operations)

## Customizing Benchmarks

### Modify Dataset Size
Edit `comprehensive_comparison.rs` line 455:
```rust
let sizes = vec![1_000, 10_000, 100_000];  // Increase dataset sizes
```

### Modify Connection Counts
Edit `comprehensive_comparison.rs` line 456:
```rust
let connection_counts = vec![1, 2, 4, 8, 16];  // Test up to 16 connections
```

### Modify Operation Count
Edit `comprehensive_comparison.rs` line 464:
```rust
let iterations = 100_000;  // More iterations = longer test, better averages
```

### Enable/Disable Backends
Edit `comprehensive_comparison.rs` lines 445-452:
```rust
let backends = vec![
    Backend::Redis,
    // Backend::Dragonfly,  // Comment out to disable
    Backend::RedliteEmbeddedMemorySqlite,
    // Backend::RedliteEmbeddedFileSqlite,  // Comment out to disable
    Backend::RedliteServerMemorySqlite,
    Backend::RedliteServerFileSqlite,
];
```

## Understanding Results

### Latency Values
- **Redlite Embedded**: 3-8µs (micro-seconds) ⚡ Ultra-fast in-process access
- **Redlite Server**: 275-300µs (micro-seconds) - Network round-trip overhead
- **Redis**: 285-390µs (micro-seconds) - Similar network overhead

### Throughput Values
- **Redlite Embedded**: 100k-300k ops/sec - Excellent for single-threaded workloads
- **Redlite Server**: 3k-5k ops/sec - Network-bound but consistent
- **Redis**: 2.5k-3.5k ops/sec - Expected for network latency

### Scaling Characteristics
- **Ideal**: Constant latency (server perfectly handles concurrency)
- **Expected**: 10-50% latency increase (contention, scheduling overhead)
- **Concerning**: 100%+ latency increase (resource saturation)

## Common Scenarios

### Test Embedded Storage Performance
```bash
# Edit comprehensive_comparison.rs to keep only:
let backends = vec![
    Backend::RedliteEmbeddedMemorySqlite,
    Backend::RedliteEmbeddedFileSqlite,
];

cargo bench --bench comprehensive_comparison
```

### Test Network Overhead
```bash
# Edit to keep Redis and Servers:
let backends = vec![
    Backend::Redis,
    Backend::RedliteServerMemorySqlite,
    Backend::RedliteServerFileSqlite,
];

cargo bench --bench comprehensive_comparison
```

### Quick Smoke Test
```bash
cargo bench --bench test_concurrent
cargo bench --bench test_server_connection
```
Runs in ~20 seconds total.

## Troubleshooting

### "Can't assign requested address" Error
**Problem**: Connection failed during population
**Solution**:
1. Check if servers are running: `redis-cli -p XXXX PING`
2. Restart servers: `bash benches/setup_services.sh`
3. Verify no port conflicts: `lsof -i :6379,7381,7382`

### Server Not Available
**Problem**: `✗ Backend Not Available` message
**Solution**:
1. Ensure Redis is running: `redis-server &` (if needed)
2. Start Redlite servers: `bash benches/setup_services.sh`
3. Wait 2-3 seconds for servers to initialize

### Timeout During Benchmark
**Problem**: Test hangs or takes too long
**Solution**:
1. Reduce dataset size (see "Modify Dataset Size" above)
2. Reduce iterations (see "Modify Operation Count" above)
3. Disable expensive backends (File-based storage)
4. Run quick tests first: `cargo bench --bench test_concurrent`

### Low Performance Numbers
**Problem**: Results show unusually high latencies
**Solution**:
1. Check system load: `top` or `Activity Monitor`
2. Restart servers for clean state
3. Close other applications consuming CPU/memory
4. Run on release build only: `cargo bench --release`

## Performance Interpretation

### When to Use Each Backend

**Use Redlite Embedded** when:
- Single-process application
- Maximum performance needed
- Data fits in memory (~GB range)
- Durability via file-based storage acceptable
- Example: CLI tools, caches, local databases

**Use Redlite Server** when:
- Multiple processes need shared data
- Similar API to Redis desired
- Moderate to high throughput needed (3-5k ops/sec)
- SQLite persistence important
- Example: Microservices, distributed systems

**Use Redis** when:
- Established ecosystem needed
- Existing Redis knowledge in team
- Redis-specific features required
- Proven production track record necessary
- Example: Complex operations, pub/sub, streams

## Advanced Usage

### Generate Statistical Report
```rust
// Add to benchmark:
let mut latencies = vec![];
for _ in 0..1000 {
    let start = Instant::now();
    client.get(&key)?;
    latencies.push(start.elapsed());
}

latencies.sort();
println!("p50: {:?}", latencies[500]);
println!("p95: {:?}", latencies[950]);
println!("p99: {:?}", latencies[990]);
```

### Compare Against Baseline
```bash
# Run once and save results
cargo bench --bench comprehensive_comparison > baseline.txt

# Make optimizations...

# Run again and compare
cargo bench --bench comprehensive_comparison > current.txt
diff baseline.txt current.txt
```

### Profile Memory Usage
```bash
# Build debug binary with profiling
cargo build --bench comprehensive_comparison

# Run with memory profiling
/usr/bin/time -v ./target/debug/deps/comprehensive_comparison
```

## Clean Up

### Stop Services
```bash
# Kill Redlite servers
pkill -f "redlite.*addr"

# Kill Redis (if started by you)
redis-cli shutdown
```

### Clean Temp Files
```bash
rm -f /tmp/redlite_*.db
rm -f /tmp/redlite_server_*.log
```

## More Information

- **Implementation Details**: See `IMPLEMENTATION_NOTES.md`
- **Full Results**: See `BENCHMARK_RESULTS.md`
- **Code**: See `benches/comprehensive_comparison.rs`
