# redlite-bench: Comprehensive Redis Protocol Benchmark Suite

> **A spec-first, language-agnostic benchmark suite for anything that speaks the Redis protocol**

## What is this?

**redlite-bench** is a comprehensive benchmarking framework that fills critical gaps in existing Redis benchmark tools:

- ✅ **Complete data type coverage**: Strings, Lists, Hashes, Sets, **Sorted Sets**, and **Streams**
- ✅ **45+ operations** across all 6 Redis data types
- ✅ **Streams benchmarking** (completely missing from redis-benchmark and memtier_benchmark)
- ✅ **Sorted Set operations** (ZADD missing from redis-benchmark defaults)
- ✅ **Concurrent scaling** testing (1, 2, 4, 8, 16+ connections)
- ✅ **Fair comparison** across Redis, Dragonfly, KeyDB, Redlite, and any Redis-compatible system
- ✅ **Spec-driven**: Single YAML specification → multiple language implementations

## Why does this exist?

### Existing Tools Have Gaps

| Tool | Covers All Data Types? | Tests Streams? | Tests Sorted Sets (ZADD)? | Multi-threaded? |
|------|------------------------|----------------|---------------------------|-----------------|
| **redis-benchmark** | ❌ No | ❌ No | ❌ No (missing from defaults) | ❌ Single-threaded |
| **memtier_benchmark** | ⚠️ Partial | ❌ No | ⚠️ Not documented | ✅ Yes |
| **YCSB** | ❌ No (generic K-V) | ❌ No | ❌ No | ✅ Yes |
| **redlite-bench** | ✅ Yes | ✅ Yes | ✅ Yes | ✅ Configurable |

### Real-World Impact

**According to official Redis benchmarking:**
- `redis-benchmark` tests: PING, SET, GET, INCR, LPUSH, RPUSH, LPOP, RPOP, SADD, HSET, SPOP, LRANGE, MSET
- **Missing**: ZADD, ZRANGE, ZSCORE (Sorted Sets), all Stream operations (XADD, XREAD, XRANGE, etc.)
- **Result**: No standard way to benchmark modern Redis features like Streams

**KeyDB's insight:**
- Single-threaded tools can't properly stress multithreaded Redis implementations
- Need concurrent load generation to see real performance characteristics

**Our solution:**
- Comprehensive operations across all data types
- Concurrent testing built-in
- Fair comparison across implementations

## Project Structure

```
redlite-bench/
├── spec/
│   └── benchmark-spec.yaml          # The source of truth (edit this!)
├── implementations/
│   ├── python/                      # Reference implementation (simple, readable)
│   ├── javascript/                  # Node.js implementation
│   ├── go/                          # High-performance Go implementation
│   └── rust/                        # Redlite-integrated Rust implementation
├── results/                         # Benchmark outputs
│   ├── json/                        # Machine-readable results
│   ├── csv/                         # Spreadsheet-compatible
│   └── reports/                     # Markdown/HTML reports
└── docs/                            # Additional documentation
```

## Quick Start

### 1. Read the Spec

```bash
cat spec/benchmark-spec.yaml
```

The YAML spec defines:
- 45+ operations across 6 data types
- Dataset sizes (1K, 10K, 100K)
- Concurrency levels (1-16 connections)
- Metrics to collect (latency, throughput, percentiles)
- Output formats (console, JSON, CSV, markdown, HTML)

### 2. Choose Implementation

**Python** (recommended for first run):
```bash
cd implementations/python
pip install -r requirements.txt
python benchmark.py --backend redis://localhost:6379
```

**JavaScript**:
```bash
cd implementations/javascript
npm install
node benchmark.js --backend redis://localhost:6379
```

**Go**:
```bash
cd implementations/go
go run main.go --backend redis://localhost:6379
```

**Rust** (integrates with Redlite embedded):
```bash
cd implementations/rust
cargo run --release -- --backend redis://localhost:6379
```

### 3. View Results

```bash
# Console output (pretty tables)
python benchmark.py --format console

# JSON for analysis
python benchmark.py --format json > results/benchmark.json

# CSV for Excel
python benchmark.py --format csv > results/benchmark.csv

# Markdown for documentation
python benchmark.py --format markdown > results/RESULTS.md
```

## Benchmark Coverage

### String Operations (7 operations)
- SET, GET, INCR, APPEND, STRLEN, MGET, MSET

### List Operations (7 operations)
- LPUSH, RPUSH, LPOP, RPOP, LLEN, LRANGE, LINDEX

### Hash Operations (7 operations)
- HSET, HGET, HGETALL, HMGET, HLEN, HDEL, HINCRBY

### Set Operations (7 operations)
- SADD, SREM, SMEMBERS, SISMEMBER, SCARD, SPOP, SRANDMEMBER

### Sorted Set Operations (8 operations)  ⭐ **Missing from most tools**
- ZADD, ZREM, ZRANGE, ZRANGEBYSCORE, ZSCORE, ZRANK, ZCARD, ZCOUNT

### Stream Operations (7 operations)  ⭐ **Completely missing from standard tools**
- XADD, XLEN, XRANGE, XREVRANGE, XREAD, XDEL, XTRIM

### Key Management (5 operations)
- DEL, EXISTS, TYPE, EXPIRE, TTL

**Total: 48 operations**

## Workload Scenarios

Pre-defined realistic workloads:

- **read_heavy**: 80% read, 20% write (caching, CDN)
- **write_heavy**: 20% read, 80% write (logging, analytics)
- **balanced**: 50/50 (general purpose)
- **list_queue**: LPUSH/RPOP (message queues)
- **stream_processing**: XADD/XREAD (event streaming)

## Metrics Collected

### Latency
- avg_us, min_us, max_us
- p50_us (median), p95_us, p99_us
- stddev_us

### Throughput
- ops_per_sec
- mb_per_sec (data transfer)

### Resources (if available)
- memory_bytes
- cpu_percent
- network_bytes

## Backends Tested

Out of the box support for:
- **Redis** (official implementation)
- **Dragonfly** (modern multithreaded alternative)
- **KeyDB** (multithreaded Redis fork)
- **Redlite** (SQLite-backed, embedded or server)
- Any Redis-compatible system

## Adding a New Backend

Edit `spec/benchmark-spec.yaml`:

```yaml
backends:
  - name: "YourRedis"
    connection: "redis://127.0.0.1:9999"
    description: "Your custom Redis implementation"
```

## Implementation Status

| Language | Status | Notes |
|----------|--------|-------|
| Python | 🚧 Planned | Reference implementation (next step) |
| JavaScript | 📋 Spec only | Coming soon |
| Go | 📋 Spec only | Coming soon |
| Rust | 🚧 In progress | Integrated with Redlite benchmarks |

## Contributing

We need implementations in:
- ✅ Python (reference, most important)
- JavaScript/TypeScript
- Go
- Java
- C#

**How to contribute:**
1. Pick a language
2. Implement `spec/benchmark-spec.yaml`
3. Output results in specified formats (console, JSON, CSV, markdown)
4. Submit PR

## Roadmap

### v0.1.0 (Current)
- ✅ Complete YAML specification
- 🚧 Python reference implementation
- 📋 Basic documentation

### v0.2.0
- Python implementation complete
- JavaScript implementation
- Comparison reports

### v0.3.0
- Go implementation
- Rust implementation (Redlite integration)
- HTML report generation with charts

### v1.0.0
- Multiple language implementations
- CI/CD integration
- Published package/binary releases
- Community adoption

## Comparison with Existing Tools

### vs redis-benchmark
- **redlite-bench**: 48 operations across 6 data types
- **redis-benchmark**: ~13 operations, missing Streams and Sorted Sets
- **Advantage**: Comprehensive, modern features

### vs memtier_benchmark
- **redlite-bench**: Spec-driven, reproducible
- **memtier_benchmark**: Great multithreaded testing
- **Advantage**: Clear documentation of what's tested

### vs YCSB
- **redlite-bench**: Redis-specific, data structure aware
- **YCSB**: Generic K-V, cross-database
- **Advantage**: Leverages Redis features

## Why Spec-First?

**Single source of truth**:
- Edit YAML → regenerate all implementations
- Consistent across languages
- Easy to add new operations
- Reproducible benchmarks

**Example**:
```yaml
- name: ZADD
  description: "Add member with score"
  command: "ZADD {key} {score} {member}"
  setup: "Empty sorted set"
  complexity: "O(log(N))"
```

This becomes consistent code across Python, JavaScript, Go, Rust, etc.

## FAQ

**Q: Why not just use redis-benchmark?**
A: redis-benchmark is great for basic operations but missing Streams (modern Redis feature) and some Sorted Set operations. We need comprehensive coverage.

**Q: Why not just use memtier_benchmark?**
A: memtier is excellent for load generation but documentation doesn't specify exact operation coverage. We need transparency and completeness.

**Q: Can I use this for non-Redis systems?**
A: Yes! Any system speaking Redis protocol can be benchmarked (Dragonfly, KeyDB, Valkey, custom implementations).

**Q: Which language implementation should I use?**
A: Start with Python (reference implementation). Use Go/Rust for maximum performance if measuring client-side overhead matters.

**Q: How is this different from Redlite's existing benchmarks?**
A: Redlite's benchmarks are Rust-specific and compare embedded vs server modes. redlite-bench is language-agnostic and compares different Redis implementations fairly.

## License

Same as Redlite project (Apache 2.0)

## Credits

Created as part of the Redlite project to provide comprehensive Redis protocol benchmarking.

**Inspired by:**
- [redis-benchmarks-specification](https://github.com/redis/redis-benchmarks-specification) (spec framework)
- [memtier_benchmark](https://github.com/RedisLabs/memtier_benchmark) (multithreaded testing)
- Community feedback on gaps in existing tools

## Getting Help

- Read the spec: `spec/benchmark-spec.yaml`
- Check examples: `implementations/*/examples/`
- Open an issue on GitHub

---

**Status**: ⚠️ v0.1.0 - Specification complete, implementations in progress

Next step: Build Python reference implementation → validate approach → expand to other languages
