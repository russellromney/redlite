# Comprehensive Benchmark Setup

This benchmark compares all variants of Redlite against Redis and Dragonfly.

## Prerequisites

### Required Services

The comprehensive benchmark needs the following services running:

1. **Redis** (port 6379)
2. **Dragonfly** (port 6380)
3. **Redlite Server instances** (port 6381+)

### Quick Start with Docker

```bash
# Start Redis
docker run -d --name redis -p 6379:6379 redis:latest

# Start Dragonfly
docker run -d --name dragonfly -p 6380:6380 docker.dragonflydb.io/dragonflydb/dragonfly

# Start Redlite Server (SQLite Memory)
# Note: You'll need to build and run the redlite server binary
cargo build --release
./target/release/redlite --port 6381 --backend sqlite --storage memory &

# Start Redlite Server (SQLite File)
./target/release/redlite --port 6382 --backend sqlite --storage file &

# Start Redlite Server (Turso Memory) - if turso feature enabled
./target/release/redlite --port 6383 --backend turso --storage memory &

# Start Redlite Server (Turso File) - if turso feature enabled
./target/release/redlite --port 6384 --backend turso --storage file &
```

### Cleanup

```bash
# Stop Docker containers
docker stop redis dragonfly
docker rm redis dragonfly

# Kill Redlite server processes
pkill -f "redlite --port"
```

## Running the Benchmark

### Full Benchmark (All backends, all sizes)

```bash
cargo bench --bench comprehensive_comparison
```

**Warning:** This will take a LONG time with 1M key datasets!

### Quick Test (Check setup)

```bash
# Run with environment variable to skip heavy tests
QUICK_TEST=1 cargo bench --bench comprehensive_comparison
```

### Individual Backend Testing

You can modify the source to comment out backends you don't want to test.

## Benchmark Matrix

The comprehensive benchmark tests:

### Backends (10 total)
- Redis (external)
- Dragonfly (external)
- Redlite Embedded - Memory/SQLite
- Redlite Embedded - File/SQLite
- Redlite Embedded - Memory/Turso (if feature enabled)
- Redlite Embedded - File/Turso (if feature enabled)
- Redlite Server - Memory/SQLite
- Redlite Server - File/SQLite
- Redlite Server - Memory/Turso (if feature enabled)
- Redlite Server - File/Turso (if feature enabled)

### Dataset Sizes
- 1K keys
- 10K keys
- 100K keys
- 1M keys

### Concurrency Levels
- 1 connection (sequential)
- 2 connections
- 4 connections
- 8 connections
- 16 connections

### Operations Tested
- GET
- SET
- (More operations can be added)

## Expected Output

The benchmark will produce:

1. Real-time progress updates
2. Per-backend summaries with latency and throughput
3. Comprehensive comparison tables organized by:
   - Operation type (GET/SET)
   - Dataset size
   - Number of concurrent connections

## Interpreting Results

### Latency (µs/op)
- Lower is better
- Measures average time per operation
- Affected by: storage type (memory/file), backend implementation, concurrency

### Throughput (ops/sec)
- Higher is better
- Total operations completed per second
- Better indicator of overall system performance under load

### Key Comparisons

- **Redis vs Dragonfly**: Industry standard comparison
- **Embedded vs Server**: In-process vs client-server overhead
- **Memory vs File**: Persistence cost
- **SQLite vs Turso**: Backend implementation differences
- **Concurrency scaling**: How well each backend handles multiple connections

## Troubleshooting

### Backend Not Available

If a backend shows "Not available (skipping)", check:
- Is the service running?
- Is it listening on the correct port?
- Can you connect with redis-cli? (e.g., `redis-cli -p 6379 ping`)

### Out of Memory

With 1M keys and multiple backends, you may run out of memory. Consider:
- Running fewer backends at once
- Reducing max dataset size
- Running on a machine with more RAM

### Slow Performance

The full benchmark matrix can take hours. To speed up:
- Reduce dataset sizes
- Reduce concurrent connection counts
- Test fewer backends
- Set `QUICK_TEST=1` environment variable
