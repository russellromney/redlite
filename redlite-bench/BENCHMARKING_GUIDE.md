# Redlite-Bench: Comprehensive Benchmarking Guide

This guide explains how to run comprehensive benchmarks comparing Redis and Redlite using the redlite-bench suite.

## Quick Start

### Build
```bash
cd implementations/rust
cargo build --release
```

### Run Core Scenarios
```bash
./target/release/redlite-bench run-benchmarks \
  --iterations 1000 \
  --dataset-size 1000
```

This runs against default scenarios with 1,000 iterations.

### Run Specific Scenarios
```bash
./target/release/redlite-bench run-benchmarks \
  --scenario-file scenarios/comprehensive.yaml \
  --scenarios "get_only,set_only,read_heavy,write_heavy" \
  --iterations 500 \
  --dataset-size 500 \
  --report-format markdown \
  --report-file my_report.md
```

## Available Scenarios

### By Category

**Core Load Patterns** (5 scenarios)
- `read_heavy` - 80% read, 20% write workload
- `write_heavy` - 20% read, 80% write workload
- `truly_balanced` - Equal mix across all data types
- `read_only` - 100% read operations
- `write_only` - 100% write operations

**Data Structure Specific** (6 scenarios)
- `cache_pattern` - KV cache (GET/SET heavy)
- `session_store` - Hash operations
- `message_queue` - List FIFO operations
- `leaderboard` - Sorted set operations
- `event_stream` - Stream operations
- `social_graph` - Set operations

**Specialized Use Cases** (5 scenarios)
- `time_series` - Time-series with sorted sets
- `object_store` - Object storage with hashes
- `tag_system` - Tagging with sets
- `pub_sub_pattern` - Pub/Sub-like behavior
- `counter_pattern` - Counters and rate limiting

**Stress Scenarios** (5 scenarios)
- `hot_keys` - Skewed access pattern
- `write_storm` - Burst write load
- `read_storm` - Burst read load
- `mixed_storm` - Alternating read/write
- `range_operations_heavy` - Expensive range scans

**Redlite-Specific** (2 scenarios)
- `history_tracking` - Redlite history feature
- `keyinfo_monitoring` - Redlite keyinfo operations

**Baselines** (8 scenarios)
- `get_only`, `set_only`, `lpush_only`, `rpop_only`
- `hset_only`, `hget_only`, `incr_only`, `zadd_only`, `zrange_only`

## Configuration Options

### Command Options
```
USAGE:
    redlite-bench run-benchmarks [OPTIONS]

OPTIONS:
    -s, --scenario-file <PATH>
        Path to YAML scenario file
        Default: scenarios/comprehensive.yaml

    --scenarios <NAMES>
        Comma-separated list of scenario names to run
        Example: "get_only,set_only,read_heavy"
        Default: All scenarios

    --redis-url <URL>
        Redis connection URL
        Default: redis://127.0.0.1:6379

    -i, --iterations <NUM>
        Number of iterations per scenario
        Default: 50000
        Recommended for testing: 500-5000
        Recommended for CI: 1000-10000

    -d, --dataset-size <NUM>
        Number of keys to use in dataset
        Default: 10000
        Recommended for testing: 500-1000
        Recommended for CI: 1000-10000

    --report-format <FORMAT>
        Output format: json or markdown
        Default: markdown

    --report-file <PATH>
        Path to save report
        Default: print to console
```

## Recommended Configurations

### Local Testing (Fast)
```bash
./target/release/redlite-bench run-benchmarks \
  --scenarios "get_only,set_only,read_heavy,write_heavy" \
  --iterations 500 \
  --dataset-size 500
```
*Runtime: ~2-5 minutes*

### Development CI (Balanced)
```bash
./target/release/redlite-bench run-benchmarks \
  --scenarios "get_only,set_only,read_heavy,write_heavy,cache_pattern,leaderboard" \
  --iterations 1000 \
  --dataset-size 1000 \
  --report-format markdown \
  --report-file report.md
```
*Runtime: ~5-10 minutes*

### Full Suite (Comprehensive)
```bash
./target/release/redlite-bench run-benchmarks \
  --iterations 10000 \
  --dataset-size 10000 \
  --report-format markdown \
  --report-file full_report.md
```
*Runtime: ~30-60 minutes, all 32 scenarios*

### Performance Regression Testing
```bash
./target/release/redlite-bench run-benchmarks \
  --scenarios "get_only,set_only,read_heavy,write_heavy" \
  --iterations 5000 \
  --dataset-size 5000 \
  --report-format json \
  --report-file regression_test.json
```

## Report Output

### Markdown Format
Human-readable report with:
- Executive summary
- Key findings
- Per-scenario detailed results
- Performance comparisons and verdicts

Example report structure:
```markdown
# redlite-bench: Redis vs Redlite Comprehensive Benchmark Report

## Summary
- Total Scenarios: 4
- Redis Completed: 4
- Redlite Completed: 4
- Redlite Faster: 4 scenarios
- Average Throughput Improvement: 4138.88%

## Key Findings
- ✓ Redlite won 4/4 direct comparisons (100% of scenarios)
- Average throughput: Redlite is 4138.9% faster than Redis

## Detailed Results
### get_only
...
```

### JSON Format
Machine-readable format suitable for:
- Automated analysis
- Dashboard integration
- Historical trend tracking
- CI/CD automation

JSON structure includes:
- Metadata (timestamp, version)
- Summary statistics
- Per-scenario results with full metrics
- Conclusions

## Understanding Results

### Throughput Metrics
```
Throughput: Operations per second
Higher is better

Example: Redis 2,352 ops/sec vs Redlite 208,685 ops/sec
Improvement: (208,685 - 2,352) / 2,352 * 100 = 8,770.97%
```

### Latency Metrics
```
P50: 50th percentile latency (median)
P99: 99th percentile latency (worst-case common)

Measured in microseconds (µs)
Lower is better

Example: Redis P50 406.54 µs vs Redlite P50 3.75 µs
Improvement: (406.54 - 3.75) / 406.54 * 100 = 99.08% faster
```

### Error Handling
- Operations are counted as successful (latency recorded) or failed (error counted)
- Error rate = failed_ops / (successful_ops + failed_ops)
- Benchmarks continue even if some operations fail

## Interpreting Comparisons

### Winners
- **Redlite**: Throughput improvement > +5%
- **Redis**: Throughput improvement < -5%
- **Tie**: Throughput difference ±5%

### Verdict Examples
- "Redlite is 4169.0% faster (104,593 vs 2,450 ops/sec)"
- "Redis is 5.2% faster (9,650 vs 9,170 ops/sec)"
- "Comparable performance (10,000 vs 9,850 ops/sec)"

## Performance Insights from Benchmarks

### Latency Improvements (Typical)
| Operation Mix | Redis P50 | Redlite P50 | Improvement |
|---------------|-----------|-------------|------------|
| Get-heavy     | 380-410µs | 3-5µs       | 98-99%     |
| Set-heavy     | 380-410µs | 15-25µs     | 93-96%     |
| Mixed         | 390-410µs | 10-20µs     | 95-98%     |

### Throughput Improvements (Typical)
| Operation Mix | Redis | Redlite | Factor |
|---------------|-------|---------|--------|
| Get-heavy     | 2.3K  | 200K+   | 90x    |
| Set-heavy     | 2.3K  | 50K     | 20x    |
| Mixed         | 2.3K  | 100K+   | 40x    |

## Troubleshooting

### Redis Connection Failed
```
✗ Failed to connect to Redis: connection refused
```

**Solution**: Ensure Redis is running
```bash
redis-server
# Or via Docker
docker run -d -p 6379:6379 redis:7
```

### Scenario Not Found
```
Error: Scenario 'unknown_scenario' not found
```

**Solution**: Check scenario names in `scenarios/comprehensive.yaml`

### Out of Memory
```
Exit code 137 (killed)
```

**Solution**: Reduce iterations or dataset size
```bash
# Instead of this:
--iterations 50000 --dataset-size 10000

# Try this:
--iterations 1000 --dataset-size 1000
```

### Some Scenarios Fail
- Session store may need Redis-specific setup
- These failures are non-blocking; other scenarios continue
- Check error messages in output

## GitHub Actions Integration

The benchmark suite automatically runs on:
- Pull requests to main/develop
- Pushes to feature branches with "redlite" in path

Reports are:
- Attached as artifacts (30-day retention)
- Posted as PR comments with summary
- Available in Actions runs

## Next Steps

After running benchmarks:

1. **Review Results**
   - Check which scenarios show largest improvements
   - Identify any regressions

2. **Analyze Performance**
   - Store JSON results for trend tracking
   - Compare across versions

3. **Optimize**
   - Profile slow scenarios
   - Implement improvements
   - Re-benchmark to validate

4. **Dashboard** (Planned Phase 7)
   - SQLite storage of historical results
   - Web UI for visualization
   - Trend analysis tools

## See Also

- [PHASE_6_SUMMARY.md](PHASE_6_SUMMARY.md) - Architecture details
- [REVISED_IMPLEMENTATION_PLAN.md](REVISED_IMPLEMENTATION_PLAN.md) - Full roadmap
- [scenarios/comprehensive.yaml](implementations/rust/scenarios/comprehensive.yaml) - Scenario definitions
