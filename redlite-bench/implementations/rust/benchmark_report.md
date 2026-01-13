# redlite-bench: Redis vs Redlite Comprehensive Benchmark Report

**Generated**: 2026-01-13T11:42:01.891507+00:00

## Summary

- **Total Scenarios**: 4
- **Redis Completed**: 4
- **Redlite Completed**: 4

- **Redlite Faster**: 4 scenarios
- **Redis Faster**: 0 scenarios
- **Average Throughput Improvement**: 4138.88%

## Key Findings

- Completed 4 scenarios: 4 on Redis, 4 on Redlite
- ✓ Redlite won 4/4 direct comparisons (100% of scenarios)
- Average throughput: Redlite is 4138.9% faster than Redis

## Detailed Results

### read_heavy
80% read, 20% write - typical caching pattern

**Redis**
- Throughput: 2282 ops/sec
- Latency P50: 406.52 µs
- Latency P99: 990.52 µs
- Duration: 0.219s (500 successful ops)

**Redlite (embedded)**
- Throughput: 90571 ops/sec
- Latency P50: 4.62 µs
- Latency P99: 56.69 µs
- Duration: 0.006s (500 successful ops)

**Comparison**
- Redlite is 3868.9% faster (90571 vs 2282 ops/sec)
- Throughput improvement: 3868.90%
- Latency P50 improvement: 98.86%
- Winner: Redlite

### write_heavy
20% read, 80% write - logging/analytics pattern

**Redis**
- Throughput: 2303 ops/sec
- Latency P50: 406.33 µs
- Latency P99: 938.01 µs
- Duration: 0.217s (500 successful ops)

**Redlite (embedded)**
- Throughput: 49891 ops/sec
- Latency P50: 20.12 µs
- Latency P99: 33.01 µs
- Duration: 0.010s (500 successful ops)

**Comparison**
- Redlite is 2066.3% faster (49891 vs 2303 ops/sec)
- Throughput improvement: 2066.28%
- Latency P50 improvement: 95.05%
- Winner: Redlite

### get_only
Pure GET operations - requires setup

**Redis**
- Throughput: 2352 ops/sec
- Latency P50: 406.54 µs
- Latency P99: 878.05 µs
- Duration: 0.213s (500 successful ops)

**Redlite (embedded)**
- Throughput: 208685 ops/sec
- Latency P50: 3.75 µs
- Latency P99: 23.36 µs
- Duration: 0.002s (500 successful ops)

**Comparison**
- Redlite is 8771.0% faster (208685 vs 2352 ops/sec)
- Throughput improvement: 8770.97%
- Latency P50 improvement: 99.08%
- Winner: Redlite

### set_only
Pure SET operations for baseline

**Redis**
- Throughput: 2320 ops/sec
- Latency P50: 399.17 µs
- Latency P99: 893.64 µs
- Duration: 0.216s (500 successful ops)

**Redlite (embedded)**
- Throughput: 45219 ops/sec
- Latency P50: 19.12 µs
- Latency P99: 76.18 µs
- Duration: 0.011s (500 successful ops)

**Comparison**
- Redlite is 1849.4% faster (45219 vs 2320 ops/sec)
- Throughput improvement: 1849.39%
- Latency P50 improvement: 95.21%
- Winner: Redlite

