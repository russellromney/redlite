# Comprehensive Benchmark Guide

## Overview

The comprehensive benchmark system compares **all Redlite variants** against **Redis** and **Dragonfly** across multiple dimensions:

### Complete Backend Matrix (10 Total)

**External Services (2):**
- Redis (port 6379)
- Dragonfly (port 6380)

**Redlite Embedded (4):**
- SQLite/Memory (in-process)
- SQLite/File (in-process)
- Turso/Memory (in-process, if feature enabled)
- Turso/File (in-process, if feature enabled)

**Redlite Server (4):**
- SQLite/Memory (port 6381)
- SQLite/File (port 6382)
- Turso/Memory (port 6383, if feature enabled)
- Turso/File (port 6384, if feature enabled)

### Test Matrix Dimensions

**Dataset Sizes:**
- 1,000 keys
- 10,000 keys
- 100,000 keys
- 1,000,000 keys

**Concurrency Levels:**
- 1 connection (sequential baseline)
- 2 connections
- 4 connections
- 8 connections
- 16 connections

**Operations:**
- GET
- SET
- (Extensible to INCR, HSET, LPUSH, etc.)

**Total Test Combinations:** 10 backends × 4 sizes × 5 concurrency levels × 2 operations = **400 benchmark runs**

---

## Quick Start

### 1. Install Dependencies (Choose One)

**Option A: Native Installation (Recommended)**
```bash
# macOS
brew install redis
brew install dragonfly

# Linux (Ubuntu/Debian)
sudo apt install redis-server
# Dragonfly: Download from https://dragonflydb.io
```

**Option B: Docker**
```bash
# Just have Docker installed - the setup script will use it automatically
# if native installations aren't available
```

**Option C: Use Existing Instances**
```bash
# If you already have Redis/Dragonfly running on ports 6379/6380,
# the setup script will detect and use them automatically
```

### 2. Start All Services

```bash
./benches/setup_services.sh
```

The script intelligently:
- Detects existing Redis/Dragonfly on ports 6379/6380
- Falls back to native installations if available
- Falls back to Docker if native isn't available
- Starts 4 Redlite server instances on ports 6381-6384

### 3. Run Benchmarks

```bash
# Full benchmark (WARNING: Takes a LONG time with 1M keys!)
cargo bench --bench comprehensive_comparison

# Quick test (smaller iteration counts for testing)
QUICK_TEST=1 cargo bench --bench comprehensive_comparison
```

### 4. Stop All Services

```bash
./benches/cleanup_services.sh
```

---

## Server Configuration Details

The Redlite server now supports flexible backend/storage configuration:

```bash
# Server CLI options
redlite --help
  --backend <TYPE>   # "sqlite" or "turso"
  --storage <MODE>   # "file" or "memory"
  --addr <ADDR>      # Listen address (default: 127.0.0.1:6379)
  --db <PATH>        # Database file path (ignored if storage=memory)
```

### Server Instances Started by Setup Script

| Port | Backend | Storage | Description |
|------|---------|---------|-------------|
| 6381 | sqlite  | memory  | SQLite in-memory, fastest for benchmarks |
| 6382 | sqlite  | file    | SQLite with persistence |
| 6383 | turso   | memory  | Turso in-memory (if feature enabled) |
| 6384 | turso   | file    | Turso with persistence (if feature enabled) |

---

## Benchmark Output Format

The benchmark produces:

### Real-time Progress

```
═══ Benchmarking: Redis ═══
  Dataset size: 1000
    Populating... ✓
    1 connections: GET 0.45µs SET 0.89µs
    2 connections: GET 0.52µs SET 1.12µs
    ...
```

### Summary Tables

```
=== GET Operation ===

Dataset: 1000 keys
Backend                                  Conns  Latency(µs)  Throughput(ops/s)
--------------------------------------------------------------------------------
Redis                                        1         0.45        2,222,222
Dragonfly                                    1         0.38        2,631,579
Redlite Embedded (Memory/SQLite)             1         3.73          268,097
Redlite Server (Memory/SQLite)               1         5.21          191,939
...
```

---

## Performance Expectations

Based on current single-threaded benchmarks:

### Read Performance (GET)

| Backend | Memory | File | Notes |
|---------|--------|------|-------|
| Redis | ~0.4 µs | N/A | Pure in-memory |
| Dragonfly | ~0.4 µs | N/A | Pure in-memory |
| Redlite Embedded (SQLite) | ~3.7 µs | ~5.6 µs | SQLite overhead |
| Redlite Server (SQLite) | ~5-7 µs | ~8-10 µs | + network overhead |

### Write Performance (SET)

| Backend | Memory | File | Notes |
|---------|--------|------|-------|
| Redis | ~0.9 µs | N/A | Pure in-memory |
| Dragonfly | ~0.8 µs | N/A | Pure in-memory |
| Redlite Embedded (SQLite) | ~18 µs | ~80 µs | WAL writes |
| Redlite Server (SQLite) | ~20-25 µs | ~90-100 µs | + network overhead |

### Concurrency Scaling

- **Redis/Dragonfly**: Linear scaling with connections (multi-threaded)
- **Redlite (SQLite)**: Sub-linear scaling due to write serialization
- **Embedded vs Server**: Embedded avoids network overhead (~2-5 µs gain)

---

## Troubleshooting

### Redis/Dragonfly Not Available

```
⚠️  Redis not found. Install with: brew install redis
```

**Solution:** Install Redis and/or Dragonfly natively:
```bash
# macOS
brew install redis
brew install dragonfly

# Linux
sudo apt install redis-server
# For Dragonfly, download from https://dragonflydb.io
```

Alternatively, install Docker and the script will use containers automatically

### Port Already in Use

```
Error: Bind for 0.0.0.0:6379 failed: port is already allocated
```

**Solution:**
1. Run `./benches/cleanup_services.sh`
2. Check for conflicting processes: `lsof -i :6379`
3. Kill conflicting processes or change ports in setup script

### Turso Feature Not Available

```
Skipping Turso servers (feature not enabled)
```

**Solution:** Rebuild with Turso feature:
```bash
cargo build --release --features turso
```

### Benchmark Takes Too Long

**Problem:** 1M keys × 16 connections × 10 backends = hours of testing

**Solutions:**
1. **Reduce dataset sizes**: Edit `comprehensive_comparison.rs` line 387:
   ```rust
   let sizes = vec![1_000, 10_000]; // Skip 100K and 1M
   ```

2. **Reduce concurrency levels**: Edit line 388:
   ```rust
   let connection_counts = vec![1, 4]; // Test only sequential and 4-way
   ```

3. **Test specific backends**: Comment out backends in `backends` vec (line 370)

4. **Use QUICK_TEST mode**: (if implemented)
   ```bash
   QUICK_TEST=1 cargo bench --bench comprehensive_comparison
   ```

### Out of Memory

**Problem:** Multiple large datasets can consume significant RAM

**Solutions:**
- Close other applications
- Reduce max dataset size to 100K keys
- Test backends sequentially instead of all at once
- Use file-based storage instead of memory

---

## Extending the Benchmark

### Adding New Operations

Edit `benches/comprehensive_comparison.rs`:

```rust
// Add new operation to Client trait
impl Client {
    fn incr(&self, key: &str) -> Result<i64, Box<dyn std::error::Error>> {
        match self {
            Client::Redis(client) | Client::Dragonfly(client) => {
                use redis::Commands;
                let mut conn = client.get_connection()?;
                Ok(conn.incr(key, 1)?)
            }
            Client::RedliteEmbedded(db) => Ok(db.incr(key)?),
            // ... other backends
        }
    }
}

// Add benchmark function
fn bench_incr(client: &Client, size: usize, iterations: usize) -> BenchResult {
    // ...benchmark INCR operation
}
```

### Adding Custom Backends

1. Add variant to `Backend` enum
2. Implement connection in `setup_client()`
3. Add to `backends` vec in `main()`

---

## Results Analysis

### Key Metrics

**Latency (µs/op):**
- Lower is better
- Measures average time per operation
- Critical for low-latency applications

**Throughput (ops/sec):**
- Higher is better
- Total operations per second
- Better indicator of overall capacity

### Comparing Backends

**Redis vs Dragonfly:**
- Industry-standard high-performance comparison
- Both pure in-memory, multi-threaded
- Expected: Similar performance, Dragonfly may edge out on concurrency

**Embedded vs Server:**
- Embedded: No network overhead, lower latency
- Server: Standard Redis protocol, easier to swap
- Expected: Embedded 2-5µs faster per operation

**Memory vs File:**
- Memory: No disk I/O, volatile
- File: Persistent, 2-5x slower (especially writes)
- Expected: File-based 50-400% slower depending on operation

**SQLite vs Turso:**
- Both use SQLite engine underneath
- Performance should be similar
- Turso may have different threading characteristics

### Concurrency Insights

- **1 connection:** Pure sequential performance baseline
- **2-4 connections:** Sweet spot for SQLite (NORMAL locking mode)
- **8-16 connections:** Shows contention behavior under load
- **Throughput plateau:** Indicates bottleneck (lock contention, CPU, I/O)

---

## Files Reference

```
redlite/
├── benches/
│   ├── comprehensive_comparison.rs       # Main benchmark code
│   ├── setup_services.sh                 # Start all services
│   ├── cleanup_services.sh               # Stop all services
│   ├── COMPREHENSIVE_BENCHMARK_GUIDE.md  # This file
│   └── README_COMPREHENSIVE.md           # Quick reference
├── src/
│   ├── main.rs                           # Server with --backend/--storage flags
│   └── ...
└── Cargo.toml                            # Includes comprehensive_comparison bench
```

---

## Next Steps

1. **Start Docker**: Ensure Docker/OrbStack is running
2. **Run Setup**: `./benches/setup_services.sh`
3. **Test Quick**: Run a small benchmark to verify setup
4. **Full Run**: Execute comprehensive benchmark (be patient!)
5. **Analyze**: Review output tables and identify performance characteristics
6. **Cleanup**: `./benches/cleanup_services.sh` when done

---

## FAQ

**Q: Why is Redlite slower than Redis?**

A: Redlite uses SQLite for persistence and ACID guarantees. Redis is pure in-memory with optional async persistence. The tradeoff is durability vs speed.

**Q: When should I use Redlite vs Redis?**

A:
- **Redlite:** Embedded use cases, single-file persistence, SQLite ecosystem integration
- **Redis:** High-throughput caching, sub-millisecond latency requirements, distributed systems

**Q: Why benchmark with 1M keys?**

A: Large datasets reveal:
- Memory pressure and allocation patterns
- Index performance degradation
- Cache effectiveness
- Real-world behavior under scale

**Q: Can I benchmark against real Redis cluster?**

A: Yes! Modify `try_connect_redis()` to point to your cluster endpoint. Same for Dragonfly.

**Q: How do I export results to CSV?**

A: Modify `comprehensive_comparison.rs` to write `BenchResult` vec to CSV file. Example:
```rust
// At end of main()
let mut wtr = csv::Writer::from_path("benchmark_results.csv")?;
for result in results {
    wtr.serialize(result)?;
}
wtr.flush()?;
```

---

**Happy Benchmarking! 🚀**
