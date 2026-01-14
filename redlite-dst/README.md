# redlite-dst

**Deterministic Simulation Testing for Redlite**

[![Rust](https://img.shields.io/badge/rust-1.75+-orange.svg)](https://www.rust-lang.org)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

> Find bugs that would take months to surface in production.
> Every failure is reproducible with a seed.

Inspired by [TigerBeetle VOPR](https://github.com/tigerbeetle/tigerbeetle/blob/main/docs/internals/vopr.md), [sled simulation](https://sled.rs/simulation.html), and [MadSim](https://github.com/madsim-rs/madsim).

## Quick Start

```bash
# Run smoke tests (<1 minute)
cargo run -- smoke

# Property-based tests with 1000 seeds
cargo run -- properties --seeds 1000

# Full test suite
cargo run -- full
```

## Commands

| Command | Description | Example |
|---------|-------------|---------|
| `smoke` | Quick sanity check (<1 min) | `redlite-dst smoke` |
| `properties` | Property-based tests | `redlite-dst properties --seeds 1000` |
| `oracle` | Compare against Redis | `redlite-dst oracle --redis localhost:6379` |
| `simulate` | Deterministic simulation | `redlite-dst simulate --seeds 1000` |
| `chaos` | Fault injection tests | `redlite-dst chaos --faults disk_full,crash_mid_write` |
| `stress` | Scale testing | `redlite-dst stress --connections 100 --keys 100000` |
| `fuzz` | Fuzzing harness | `redlite-dst fuzz --target resp_parser --duration 60` |
| `soak` | Long-running stability | `redlite-dst soak --duration 1h` |
| `cloud` | Parallel on fly.io | `redlite-dst cloud --seeds 100000 --machines 10` |
| `replay` | Reproduce failure | `redlite-dst replay --seed 12345 --test simulate` |
| `full` | Run everything | `redlite-dst full` |

## Seed-Based Reproducibility

Every test failure comes with a seed for exact reproduction:

```bash
# Test fails with seed
✗ FAIL [seed=8675309] simulation (45ms)
    Invariant violated: set_get_roundtrip

# Reproduce the exact failure
redlite-dst replay --seed 8675309 --test simulate
```

### Regression Seed Bank

Track known failure seeds to prevent regressions:

```bash
# Add a failing seed to the bank
redlite-dst seeds add --seed 8675309 --description "SET/GET inconsistency under concurrent writes"

# List all regression seeds
redlite-dst seeds list

# Test all regression seeds
redlite-dst seeds test
```

## Test Categories

### Smoke Tests
Quick sanity checks for basic operations:
- SET/GET roundtrip
- INCR/DECR arithmetic
- List operations
- Hash operations
- Set operations
- Sorted set ordering
- Basic persistence

### Property-Based Tests
Invariants that must always hold:
- `set_get_roundtrip`: SET k v; GET k => v
- `incr_monotonic`: INCR always increases
- `list_order_preserved`: LPUSH/RPUSH order
- `sorted_set_ordering`: ZRANGE returns sorted
- `crash_recovery_consistent`: Data survives restart

### Chaos Tests
Fault injection scenarios:
- `disk_full`: Write fails, no corruption
- `corrupt_read`: Detect via checksum
- `crash_mid_write`: WAL ensures consistency
- `slow_write`: Eventual completion

### Stress Tests
Scale and performance limits:
- High connection count (10, 100, 1000)
- Large key space (1K, 100K, 1M keys)
- Hot key contention
- Large values (1KB, 10KB, 100KB)

### Soak Tests
Long-running stability:
- Memory growth monitoring
- Throughput stability
- Resource leak detection

## Output Formats

```bash
# Console (default)
redlite-dst smoke

# JSON report
redlite-dst smoke --format json --output report.json

# Markdown report
redlite-dst smoke --format markdown --output report.md
```

## CI Integration

```yaml
# .github/workflows/dst.yml
name: DST
on: [push, pull_request]

jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: dtolnay/rust-toolchain@stable

      # Quick tests on every push
      - name: Smoke Tests
        run: cargo run -p redlite-dst -- smoke

      # Full suite on main
      - name: Full DST
        if: github.ref == 'refs/heads/main'
        run: cargo run -p redlite-dst -- full --quick

      # Regression seeds always
      - name: Regression Seeds
        run: cargo run -p redlite-dst -- seeds test
```

## Cloud Parallel Execution

Run massive test suites on fly.io:

```bash
# 100,000 seeds across 10 machines
redlite-dst cloud --seeds 100000 --machines 10
```

This distributes seeds across machines, collects results, and reports any failures with reproducible seeds.

## Spec-Driven Testing

Tests are defined in `spec/dst-spec.yaml`:

```yaml
invariants:
  - name: "set_get_roundtrip"
    description: "SET k v; GET k => v"
    category: "data_integrity"

faults:
  - name: "disk_full"
    description: "Simulate disk full error"
    severity: "high"

scenarios:
  smoke:
    tests:
      - name: "basic_set_get"
        operations: ["SET", "GET"]
        keys: 100
        iterations: 1000
```

## Philosophy

> "If you're not running deterministic simulation tests, you're not testing." — Tyler Neely (sled)

This tool embodies:

1. **Determinism**: Every test runs from a seed, making failures reproducible
2. **Coverage**: Property tests explore edge cases humans wouldn't think of
3. **Chaos**: Real systems fail in unexpected ways; we inject faults deliberately
4. **Scale**: Cloud execution enables testing at production scale
5. **Regression**: Once found, bugs stay caught with the seed bank

## License

MIT
