# redlite-bench (Rust)

Production-ready benchmark suite for Redlite and Redis-compatible systems.

## Quick Summary

| Command | Purpose |
|---------|---------|
| `redlite` | Benchmark embedded Redlite (no server needed) |
| `redis` | Benchmark a Redis server over the network |
| `scenario` | Run a YAML-defined workload (e.g., 80% read / 20% write) |
| `run-benchmarks` | Compare Redis vs Redlite across multiple scenarios |
| `generate-db` | Create a pre-populated database for consistent benchmarks |
| `scale-test` | Test performance at 10k, 100k, 1M, 10M keys |
| `analyze-access` | Simulate access patterns and get cache sizing recommendations |

**Quick start:**
```bash
cargo build --release
./target/release/redlite-bench redlite --memory -i 10000 -d 1000 -o get,set
```

---

## Overview

**redlite-bench** measures the performance of key-value operations against Redlite (SQLite-backed embedded database) and Redis servers. It provides:

- **Throughput measurement**: Operations per second for GET, SET, and 40+ other Redis commands
- **Latency percentiles**: P50, P95, P99 latency distributions
- **Cache analysis**: Tools to determine optimal SQLite page cache sizing
- **Workload simulation**: Realistic access patterns (Zipfian, temporal) vs stress testing (uniform)

## Installation

```bash
cd implementations/rust
cargo build --release
```

Binary location: `./target/release/redlite-bench`

---

## Commands

### `redlite` - Benchmark Redlite (embedded)

Runs benchmarks directly against Redlite's embedded SQLite-backed database. No server required.

```bash
# In-memory database (fastest, no persistence)
redlite-bench redlite --memory -i 10000 -d 1000 -o get,set

# File-backed database (persistent, measures real I/O)
redlite-bench redlite --path ./bench.db -i 10000 -d 1000 -o get,set,hset,lpush
```

**Options:**

| Option | Description |
|--------|-------------|
| `-i, --iterations` | Number of operations to perform per benchmark. Higher = more accurate but slower. Default: 100,000 |
| `-d, --dataset-size` | Number of keys to pre-populate before benchmarking. Affects cache hit rates and working set. Default: 10,000 |
| `-o, --operations` | Comma-separated list of operations to benchmark (e.g., `get,set,hget`). Default: all supported operations |
| `--memory` | Use in-memory SQLite database. Fastest option, useful for measuring pure CPU overhead |
| `--path` | Path to SQLite database file. Creates if doesn't exist. Measures real disk I/O |
| `-c, --concurrency` | Number of concurrent connections/threads. Default: 1 |
| `--concurrency-mode` | How to handle concurrency: `sequential` (one at a time), `async` (tokio async), `blocking` (OS threads) |
| `--output-format` | Output format: `console` (human-readable) or `json` (machine-parseable) |

---

### `redis` - Benchmark Redis server

Runs the same benchmarks against a running Redis (or Redis-compatible) server over the network.

```bash
redlite-bench redis --url redis://127.0.0.1:6379 -i 10000 -d 1000
```

**Options:** Same as `redlite` command, plus:

| Option | Description |
|--------|-------------|
| `--url` | Redis connection URL. Format: `redis://[user:pass@]host:port[/db]` |

---

### `scenario` - Run YAML-defined workload

Executes a predefined workload scenario from a YAML file. Scenarios define realistic mixes of operations (e.g., 80% reads, 20% writes).

```bash
# Run a specific scenario
redlite-bench scenario -s scenarios/comprehensive.yaml -n read_heavy -i 10000

# With pre-populated database (skip setup phase)
redlite-bench scenario -s scenarios/comprehensive.yaml -n get_only \
  --db-path /tmp/mydb.db -d 1000000 --skip-setup --cache-mb 100
```

**Options:**

| Option | Description |
|--------|-------------|
| `-s, --scenario-file` | Path to YAML file containing scenario definitions |
| `-n, --name` | Name of the scenario to run (must exist in YAML file) |
| `-b, --backend` | Backend to test: `redis` (network) or `redlite` (embedded). Default: redlite |
| `--db-path` | Path to existing database file. Use with `--skip-setup` for pre-generated DBs |
| `--skip-setup` | Skip the data population phase. Assumes database already has data |
| `--cache-mb` | SQLite page cache size in megabytes. Larger = more data in RAM. Default: 64 |
| `-i, --iterations` | Override iteration count from scenario file |
| `-d, --dataset-size` | Override dataset size from scenario file |

---

### `run-benchmarks` - Compare Redis vs Redlite

Runs multiple scenarios against both Redis and Redlite, generating a comparison report.

```bash
redlite-bench run-benchmarks \
  --scenarios "read_heavy,write_heavy,get_only" \
  --iterations 10000 \
  --report-format markdown \
  --report-file report.md
```

**Options:**

| Option | Description |
|--------|-------------|
| `--scenarios` | Comma-separated list of scenario names to run |
| `--iterations` | Operations per scenario |
| `--dataset-size` | Keys to pre-populate |
| `--report-format` | Output format: `markdown` or `json` |
| `--report-file` | File path for the generated report |

---

### `generate-db` - Pre-populate database

Creates a Redlite database file with specified data types and counts. Useful for:
- Testing with large datasets without waiting for setup each run
- Consistent benchmarks across multiple runs
- Measuring performance at specific database sizes

```bash
# Generate 1M string keys (most common benchmark)
redlite-bench generate-db -o /tmp/bench-1m.db \
  --strings 1000000 \
  --lists 0 \
  --hashes 0 \
  --sorted-sets 0

# Large database with mixed data types
redlite-bench generate-db -o /tmp/bench-large.db \
  --strings 5000000 \
  --hashes 100000 \
  --sorted-sets 50000 \
  -v
```

**Options:**

| Option | Description |
|--------|-------------|
| `-o, --output` | Output database file path. Will overwrite if exists |
| `--strings` | Number of string keys (`key_0`, `key_1`, ...) with random 100-byte values. Default: 10,000 |
| `--lists` | Number of lists, each with 10 elements. Default: 1,000 |
| `--hashes` | Number of hashes, each with 5 fields. Default: 1,000 |
| `--sets` | Number of sets, each with 10 members. Default: 1,000 |
| `--sorted-sets` | Number of sorted sets, each with 10 scored members. Default: 1,000 |
| `-v, --verbose` | Show progress during generation |

**Database sizes** (approximate):
- 10,000 strings: ~2 MB
- 100,000 strings: ~19 MB
- 1,000,000 strings: ~195 MB
- 10,000,000 strings: ~1.95 GB

---

### `scale-test` - Test performance at different sizes

Automatically generates databases at multiple sizes and runs benchmarks, showing how performance scales.

```bash
redlite-bench scale-test -s 10k,100k,1m,10m \
  --scenarios get_only,set_only,read_heavy \
  --cache-mb 64 \
  --report-file scale-report.md
```

**Options:**

| Option | Description |
|--------|-------------|
| `-s, --sizes` | Comma-separated dataset sizes. Supports: `10k`, `100k`, `1m`, `10m` (k=thousand, m=million) |
| `--scenarios` | Scenarios to run at each size |
| `--cache-mb` | SQLite page cache size. See [PERFORMANCE.md](PERFORMANCE.md) for sizing guidelines |
| `--keep-dbs` | Don't delete generated databases after test (useful for debugging) |
| `--report-file` | Output file for the scaling report |

---

### `analyze-access` - Analyze access patterns

Simulates workload access patterns and recommends optimal cache sizing. This command:
1. Generates key accesses according to a distribution (Zipfian, uniform, etc.)
2. Tracks which keys are accessed and how often
3. Simulates LRU cache behavior at various sizes
4. Outputs working set analysis and cache recommendations

```bash
# Analyze with Zipfian distribution (realistic hot keys)
redlite-bench analyze-access --db-path /tmp/bench.db -d 1000000 \
  --distribution zipfian --zipf-skew 0.8 \
  --read-pct 70 --write-pct 25 --delete-pct 5

# Analyze temporal locality (session-like access)
redlite-bench analyze-access --db-path /tmp/bench.db -d 1000000 \
  --distribution temporal \
  --read-pct 60 --write-pct 35 --delete-pct 5
```

**Options:**

| Option | Description |
|--------|-------------|
| `--db-path` | Path to database file (used for actual operations) |
| `-d, --dataset-size` | Total number of keys in the database |
| `-i, --iterations` | Number of operations to simulate. Default: 100,000 |
| `--distribution` | Key selection strategy (see Key Distributions below) |
| `--zipf-skew` | Zipfian skew parameter. 0.5 = mild skew, 0.99 = extreme hot keys. Default: 0.99 |
| `--read-pct` | Percentage of operations that are reads (GET). Default: 70 |
| `--write-pct` | Percentage of operations that are writes (SET). Default: 20 |
| `--delete-pct` | Percentage of operations that are deletes (DEL). Default: 10 |
| `--target-hit-rate` | Desired cache hit rate for recommendations. Default: 0.95 (95%) |
| `--entry-size-bytes` | Average size per key-value pair for MB calculations. Default: 200 |

**Output includes:**
- **Working set analysis**: How many keys account for 90%, 95%, 99% of traffic
- **Estimated Zipf skew**: Detected skew in your access pattern
- **Cache size vs hit rate table**: Predicted hit rates at various cache sizes
- **Recommendation**: Minimum cache size to achieve your target hit rate

---

## Key Distributions

The `analyze-access` command supports different key selection strategies:

| Distribution | Mathematical Model | Real-World Example |
|-------------|-------------------|-------------------|
| `uniform` | P(key) = 1/N for all keys | Stress testing, worst-case analysis. Every key equally likely to be accessed. |
| `zipfian` | P(rank k) ∝ 1/k^s | Web traffic, social media, e-commerce. A few "hot" keys get most traffic (80/20 rule). |
| `temporal` | Recent keys weighted higher | Session stores, caches. Recently written keys more likely to be read. |
| `sequential` | Keys accessed in order: 0, 1, 2, ... | Batch processing, data migrations. Predictable, cache-friendly access. |

**Zipfian skew values:**
- `0.5`: Mild skew - top 10% of keys get ~60% of traffic
- `0.7`: Moderate skew - top 1% of keys get ~50% of traffic
- `0.9`: High skew - top 0.1% of keys get ~50% of traffic
- `0.99`: Extreme skew - top 0.01% of keys get ~50% of traffic

---

## Scenarios

32 pre-defined scenarios in `scenarios/comprehensive.yaml`:

### Core Load Patterns

| Scenario | Description | Operation Mix |
|----------|-------------|---------------|
| `read_heavy` | Typical caching workload | 80% GET, 20% SET |
| `write_heavy` | Logging, analytics | 20% GET, 80% SET |
| `truly_balanced` | Equal read/write | 50% GET, 50% SET |
| `read_only` | Pure read benchmark | 100% GET |

### Data Structure Specific

| Scenario | Description | Operations Used |
|----------|-------------|-----------------|
| `cache_pattern` | Application cache | GET, SET, DEL, EXPIRE |
| `session_store` | User sessions | HSET, HGET, EXPIRE |
| `message_queue` | Job queue | LPUSH, RPOP |
| `leaderboard` | Ranking system | ZADD, ZRANGE, ZSCORE |

### Stress Tests

| Scenario | Description | Characteristics |
|----------|-------------|-----------------|
| `hot_keys` | Few keys, many accesses | Tests lock contention |
| `write_storm` | Maximum write throughput | 100% SET operations |
| `read_storm` | Maximum read throughput | 100% GET operations |
| `mixed_storm` | High concurrency mixed | Concurrent R/W |

### Baselines (Single Operation)

| Scenario | Description |
|----------|-------------|
| `get_only` | Pure GET performance baseline |
| `set_only` | Pure SET performance baseline |
| `lpush_only` | List push performance |
| `hset_only` | Hash set performance |
| `zadd_only` | Sorted set add performance |

### Redlite-Specific

| Scenario | Description |
|----------|-------------|
| `history_tracking` | Tests Redlite's key history feature |
| `keyinfo_monitoring` | Tests Redlite's metadata tracking |

---

## Output Formats

| Format | Use Case | Example |
|--------|----------|---------|
| `console` | Interactive use, debugging | Human-readable tables and summaries |
| `json` | CI/CD pipelines, automation | Machine-parseable, includes all metrics |
| `markdown` | Documentation, reports | GitHub-compatible tables |

---

## Pre-generating Test Databases

For consistent, repeatable benchmarks, pre-generate databases at standard sizes. This avoids the overhead of data population during each benchmark run.

### Standard Dataset Sizes

| Size | Keys | DB File | Generation Time | Use Case |
|------|------|---------|-----------------|----------|
| 10k | 10,000 | ~2 MB | <1 sec | Quick tests, CI/CD |
| 100k | 100,000 | ~19 MB | ~2 sec | Development benchmarks |
| 1M | 1,000,000 | ~195 MB | ~20 sec | Production-like testing |
| 10M | 10,000,000 | ~1.95 GB | ~3 min | Large-scale performance analysis |

### Generate Standard Databases

```bash
# Create a benchmarks directory
mkdir -p /tmp/redlite-bench

# Generate each standard size (strings only for GET/SET benchmarks)
redlite-bench generate-db -o /tmp/redlite-bench/10k.db --strings 10000 --lists 0 --hashes 0 --sets 0 --sorted-sets 0
redlite-bench generate-db -o /tmp/redlite-bench/100k.db --strings 100000 --lists 0 --hashes 0 --sets 0 --sorted-sets 0
redlite-bench generate-db -o /tmp/redlite-bench/1m.db --strings 1000000 --lists 0 --hashes 0 --sets 0 --sorted-sets 0
redlite-bench generate-db -o /tmp/redlite-bench/10m.db --strings 10000000 --lists 0 --hashes 0 --sets 0 --sorted-sets 0 -v
```

### Using Pre-generated Databases

Once generated, use `--db-path` and `--skip-setup` to run benchmarks against existing data:

```bash
# Run scenario against 1M key database
redlite-bench scenario -s scenarios/comprehensive.yaml -n get_only \
  --db-path /tmp/redlite-bench/1m.db -d 1000000 --skip-setup --cache-mb 64

# Analyze access patterns on 10M database
redlite-bench analyze-access --db-path /tmp/redlite-bench/10m.db -d 10000000 \
  --distribution zipfian --zipf-skew 0.8
```

### Mixed Data Type Databases

For scenarios that test hashes, lists, and sorted sets:

```bash
# Full mixed-type database for comprehensive testing
redlite-bench generate-db -o /tmp/redlite-bench/mixed-1m.db \
  --strings 500000 \
  --hashes 100000 \
  --lists 50000 \
  --sets 50000 \
  --sorted-sets 50000 \
  -v
```

---

## Examples

### Quick sanity check
```bash
redlite-bench redlite --memory -i 1000 -d 100 -o get,set
```

### Production benchmark workflow
```bash
# 1. Generate a large dataset once
redlite-bench generate-db -o /tmp/prod.db --strings 10000000

# 2. Run scale test to see performance at different sizes
redlite-bench scale-test -s 100k,1m,10m --cache-mb 256

# 3. Analyze your expected workload for cache sizing
redlite-bench analyze-access --db-path /tmp/prod.db -d 10000000 \
  --distribution zipfian --zipf-skew 0.7
```

### CI/CD integration
```bash
redlite-bench run-benchmarks \
  --scenarios "get_only,set_only,read_heavy" \
  --iterations 5000 \
  --output-format json \
  --report-file results.json
```

### Compare cache sizes
```bash
for cache in 32 64 128 256; do
  echo "=== Cache: ${cache}MB ==="
  redlite-bench scenario -s scenarios/comprehensive.yaml -n get_only \
    --db-path /tmp/bench-1m.db -d 1000000 --skip-setup --cache-mb $cache
done
```

---

## SQLite Cache Architecture

Redlite uses SQLite's two-tier caching:

1. **Page cache** (`cache_size` / `--cache-mb`): In-process memory holding database pages. Controlled by redlite-bench.

2. **Memory-mapped I/O** (`mmap_size`): OS-managed file mapping. Automatically set to 4× page cache. Leverages OS page cache.

This means even with a small explicit cache, the OS may cache additional data. "Cold" benchmarks (first run after reboot) differ significantly from "warm" benchmarks.

---

## Planned Features

The following features are planned but not yet implemented:

### Encryption (Planned)

Benchmark impact of at-rest encryption on Redlite performance.

```bash
# Future: Generate encrypted database
redlite-bench generate-db -o /tmp/encrypted.db --strings 1000000 \
  --encryption aes-256-gcm --key-file /path/to/key

# Future: Benchmark with encryption enabled
redlite-bench scenario -s scenarios/comprehensive.yaml -n read_heavy \
  --db-path /tmp/encrypted.db --encryption aes-256-gcm --key-file /path/to/key
```

**Expected impact**: 10-30% throughput reduction depending on CPU AES-NI support.

### Compression (Planned)

Benchmark impact of value compression on storage and throughput.

```bash
# Future: Generate with compression
redlite-bench generate-db -o /tmp/compressed.db --strings 1000000 \
  --compression zstd --compression-level 3

# Future: Benchmark compression trade-offs
redlite-bench scenario -s scenarios/comprehensive.yaml -n read_heavy \
  --db-path /tmp/compressed.db --compression zstd
```

**Expected trade-offs**:
- **zstd**: Best compression ratio, moderate CPU overhead
- **lz4**: Fast compression/decompression, lower ratio
- **snappy**: Balanced speed and ratio

| Compression | Write Overhead | Read Overhead | Space Savings |
|-------------|---------------|---------------|---------------|
| None | 0% | 0% | 0% |
| lz4 | ~5% | ~3% | ~40-60% |
| zstd (level 3) | ~15% | ~5% | ~60-75% |
| zstd (level 9) | ~40% | ~5% | ~70-80% |

### Replication (Planned)

Benchmark primary-replica replication lag and throughput.

```bash
# Future: Benchmark replication scenarios
redlite-bench replication-test \
  --primary /tmp/primary.db \
  --replica /tmp/replica.db \
  --sync-mode async
```

---

## Performance Notes

See [PERFORMANCE.md](PERFORMANCE.md) for detailed analysis including:
- Throughput by database size (10k to 10M keys)
- Cache sizing impact and sweet spots
- Working set estimation methodology
- Realistic workload recommendations
- Cold vs warm cache performance (50-100x difference)

---

## Pre-Generated Benchmark Databases

Large benchmark databases are stored in Tigris (S3-compatible storage) for consistent, reproducible benchmarks without regeneration time.

### Available Databases

| Database | Keys | Size | Tigris URL |
|----------|------|------|------------|
| `redlite-10m.db` | 10,000,000 | ~2.0 GB | `https://fly.storage.tigris.dev/redlite-bench/redlite-10m.db` |
| `redlite-50m.db` | 50,000,000 | ~10 GB | `https://fly.storage.tigris.dev/redlite-bench/redlite-50m.db` |
| `redlite-100m.db` | 100,000,000 | ~20 GB | `https://fly.storage.tigris.dev/redlite-bench/redlite-100m.db` |

### Download Pre-Generated Databases

```bash
# Download to local databases/ directory
cd databases/

# 10M keys (~2GB)
curl -O https://fly.storage.tigris.dev/redlite-bench/redlite-10m.db

# 50M keys (~10GB) - for enterprise testing
curl -O https://fly.storage.tigris.dev/redlite-bench/redlite-50m.db

# 100M keys (~20GB) - for large-scale enterprise testing
curl -O https://fly.storage.tigris.dev/redlite-bench/redlite-100m.db
```

### Local Database Location

Pre-generated databases are stored in `./databases/` (gitignored). After downloading:

```bash
# Run benchmarks with 10M database
./target/release/redlite-bench redlite --path databases/redlite-10m.db -i 100000 -d 10000000 -o get,set

# Run encrypted benchmarks
./target/release/redlite-bench redlite --path databases/redlite-10m-encrypted.db \
  --encryption-key "test-key" -i 100000 -d 10000000 -o get,set
```

### Generating New Databases

For custom sizes or to regenerate:

```bash
# Generate locally (slow for large sizes)
./target/release/redlite-bench generate-db -o databases/redlite-custom.db \
  --strings 10000000 --lists 0 --hashes 0 --sets 0 --sorted-sets 0 -v

# For 50M+ keys, generate on Fly.io (see scripts/generate-large-dbs.sh)
```

### Uploading to Tigris

```bash
# Set Tigris credentials (from Fly.io dashboard)
export AWS_ACCESS_KEY_ID="your-tigris-key"
export AWS_SECRET_ACCESS_KEY="your-tigris-secret"
export AWS_ENDPOINT_URL="https://fly.storage.tigris.dev"

# Upload database
aws s3 cp databases/redlite-10m.db s3://redlite-bench/redlite-10m.db --acl public-read
```
