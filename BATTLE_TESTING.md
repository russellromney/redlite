# Redlite Battle Testing: Deterministic Simulation Testing

> "If you built a plane in windtunnel with zero induced turbulence effects, would you then fly that plane? Because that's how people are building the distributed systems you use today."
> — [sled simulation guide](https://sled.rs/simulation.html)

This document outlines how to make redlite **Jepsen-proof** before releasing to Hacker News.

---

## The Problem

Traditional testing catches ~5% of real bugs. [Jepsen](https://jepsen.io) has found bugs in nearly every distributed system it tested. The reason: **we test under idealized conditions** (no network delays, no crashes mid-operation, no disk corruption, perfect timing).

Real production environments have:
- Packets that arrive out of order or not at all
- Processes that crash at the worst possible moment
- Disks that lie about writes being committed
- Clocks that drift and jump
- Concurrent operations that interleave in unexpected ways

**The goal**: Find bugs in milliseconds on a laptop that would take months to surface in production.

---

## How Others Do It

### FoundationDB / Antithesis
- Deterministic hypervisor that controls all non-determinism
- $105M funding, used by MongoDB, Ethereum, etcd
- [WarpStream found bugs](https://www.warpstream.com/blog/deterministic-simulation-testing-for-our-entire-saas) in 6 hours that 10,000+ hours of CI missed

### [TigerBeetle VOPR](https://github.com/tigerbeetle/tigerbeetle/blob/main/docs/internals/vopr.md)
- 1,000 CPU cores running 24/7/365
- Time accelerated 700x → **2 millennia of simulated runtime per day**
- Seed-based deterministic replay of any failure

### [Sled](https://sled.rs/simulation.html)
- Discrete event simulation with state machine abstraction
- Thousands of tests per second on a laptop
- Simple interface: `receive(msg) -> [(msg, dest)]`, `tick() -> [(msg, dest)]`

### [Polar Signals](https://www.polarsignals.com/blog/posts/2025/07/08/dst-rust)
- State machine architecture, message bus controls scheduling
- Found **data loss and data duplication bugs** before production
- "Dimensionality reduction" - all interactions through message passing

### Common Pattern

All successful DST implementations control **four things**:

| Variable | What | How |
|----------|------|-----|
| **Concurrency** | Thread scheduling | Single-threaded execution, deterministic ordering |
| **Time** | Clocks, timers | Simulated clock, no `Instant::now()` |
| **Randomness** | RNG | Seeded PRNG, same seed = same execution |
| **I/O** | Network, disk | Mocked/simulated with fault injection |

---

## Architecture for Redlite

### Current State

```
┌─────────────────────────────────────────────┐
│                  main.rs                     │
│         (CLI + TCP server startup)           │
└─────────────────┬───────────────────────────┘
                  │
┌─────────────────▼───────────────────────────┐
│              server/mod.rs                   │
│  (TCP listener, RESP protocol, connections)  │
│              Uses: tokio                     │
└─────────────────┬───────────────────────────┘
                  │
┌─────────────────▼───────────────────────────┐
│                 db.rs                        │
│    (Core database logic, 26K+ lines)         │
│    Uses: rusqlite, std::time, etc.           │
└─────────────────┬───────────────────────────┘
                  │
┌─────────────────▼───────────────────────────┐
│               SQLite                         │
│         (Actual disk I/O)                    │
└─────────────────────────────────────────────┘
```

**Problem**: Non-determinism is scattered throughout:
- `tokio` controls async scheduling
- `std::time::Instant` for timeouts/TTL
- `rusqlite` does real disk I/O
- `rand` (if used) for any randomness

### Target Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                     Test Harness (DST)                          │
│  • Seeded RNG for operation generation                          │
│  • Deterministic scheduler (single-threaded)                    │
│  • Simulated clock                                              │
│  • Fault injection (I/O errors, delays, crashes)                │
└─────────────────────────────┬───────────────────────────────────┘
                              │ Commands
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                    RedliteEngine (Pure Logic)                   │
│  • Processes commands, returns responses                        │
│  • No direct I/O, time, or randomness                          │
│  • All external dependencies injected                           │
└─────────────────────────────┬───────────────────────────────────┘
                              │ Storage ops
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                     Storage Backend Trait                       │
├─────────────────────────────┬───────────────────────────────────┤
│   SimulatedStorage          │       SqliteStorage               │
│   • In-memory               │       • Real rusqlite             │
│   • Fault injection         │       • Production use            │
│   • Deterministic           │                                   │
└─────────────────────────────┴───────────────────────────────────┘
```

---

## Implementation Plan

### Phase 1: Property-Based Testing Foundation (1-2 days)

Add property-based testing to catch logic bugs without major refactoring.

**Add to Cargo.toml:**
```toml
[dev-dependencies]
proptest = "1.4"
```

**Create `tests/properties.rs`:**
```rust
use proptest::prelude::*;
use redlite::Redlite;

// Property: SET then GET returns same value
proptest! {
    #[test]
    fn set_get_roundtrip(key in "[a-z]{1,10}", value in "[a-z]{1,100}") {
        let db = Redlite::open(":memory:").unwrap();
        db.set(&key, &value, None).unwrap();
        let got = db.get(&key).unwrap();
        prop_assert_eq!(got, Some(value));
    }
}

// Property: INCR is atomic (concurrent INCRs don't lose updates)
proptest! {
    #[test]
    fn incr_atomic(increments in 1..1000usize) {
        let db = Redlite::open(":memory:").unwrap();
        db.set("counter", "0", None).unwrap();

        for _ in 0..increments {
            db.incr("counter").unwrap();
        }

        let final_val: i64 = db.get("counter").unwrap().unwrap().parse().unwrap();
        prop_assert_eq!(final_val, increments as i64);
    }
}

// Property: List operations maintain LIFO order
proptest! {
    #[test]
    fn list_lifo(values in prop::collection::vec("[a-z]+", 1..100)) {
        let db = Redlite::open(":memory:").unwrap();

        for v in &values {
            db.lpush("list", &[v.clone()]).unwrap();
        }

        let got = db.lrange("list", 0, -1).unwrap();
        let expected: Vec<_> = values.into_iter().rev().collect();
        prop_assert_eq!(got, expected);
    }
}

// Property: Sorted set maintains score ordering
proptest! {
    #[test]
    fn zset_ordering(
        members in prop::collection::vec(("[a-z]+", -1000.0f64..1000.0), 1..100)
    ) {
        let db = Redlite::open(":memory:").unwrap();

        for (member, score) in &members {
            db.zadd("zset", &[(score, member.as_str())]).unwrap();
        }

        let results = db.zrange("zset", 0, -1, true).unwrap();

        // Verify scores are in ascending order
        let scores: Vec<f64> = results.iter()
            .filter_map(|(_, s)| s.map(|x| x))
            .collect();

        for window in scores.windows(2) {
            prop_assert!(window[0] <= window[1]);
        }
    }
}

// Property: WATCH/MULTI/EXEC aborts on concurrent modification
proptest! {
    #[test]
    fn watch_conflict_aborts(
        initial in "[a-z]+",
        modification in "[a-z]+"
    ) {
        let db = Redlite::open(":memory:").unwrap();
        db.set("watched", &initial, None).unwrap();

        // Start watching
        db.watch(&["watched"]).unwrap();
        db.multi().unwrap();
        db.set_queued("watched", "from_transaction", None).unwrap();

        // Simulate concurrent modification (outside transaction)
        // This would need a second connection in real test
        // For now, directly modify to simulate
        db.set("watched", &modification, None).unwrap();

        // EXEC should return nil (transaction aborted)
        let result = db.exec().unwrap();
        prop_assert!(result.is_none(), "Transaction should abort on conflict");
    }
}
```

**Run with:**
```bash
# Quick check (256 cases per property)
cargo test properties

# Thorough check (10K cases, find rarer bugs)
PROPTEST_CASES=10000 cargo test properties

# With specific seed for reproduction
PROPTEST_SEED="0x1234567890abcdef" cargo test properties
```

### Phase 2: MadSim Integration (3-5 days)

Use [MadSim](https://github.com/madsim-rs/madsim) for deterministic async simulation.

**Update Cargo.toml:**
```toml
[features]
default = []
simulation = ["madsim"]

[dependencies]
tokio = "1"

[target.'cfg(madsim)'.dependencies]
tokio = { version = "0.2", package = "madsim-tokio" }
madsim = "0.2"
```

**Create simulation test harness `tests/simulation.rs`:**
```rust
#![cfg(madsim)]

use madsim::runtime::Runtime;
use madsim::time::{sleep, Duration};
use madsim::net::Endpoint;
use std::sync::Arc;

/// Simulated redlite cluster for testing
struct SimCluster {
    seed: u64,
    nodes: Vec<SimNode>,
}

impl SimCluster {
    fn new(seed: u64) -> Self {
        Self { seed, nodes: vec![] }
    }

    async fn run_workload(&self, ops: Vec<Op>) -> Vec<OpResult> {
        // Execute operations with deterministic scheduling
        todo!()
    }
}

#[madsim::test]
async fn test_concurrent_operations() {
    let seed = std::env::var("SIM_SEED")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(12345);

    println!("Running with seed: {}", seed);

    let cluster = SimCluster::new(seed);

    // Generate random workload from seed
    let mut rng = rand::SeedableRng::seed_from_u64(seed);
    let ops = generate_random_ops(&mut rng, 1000);

    let results = cluster.run_workload(ops).await;

    // Verify linearizability
    assert!(is_linearizable(&results));
}

#[madsim::test]
async fn test_crash_recovery() {
    let seed = 42;
    let cluster = SimCluster::new(seed);

    // Write some data
    cluster.set("key1", "value1").await;
    cluster.set("key2", "value2").await;

    // Crash and recover
    cluster.crash_node(0).await;
    cluster.recover_node(0).await;

    // Verify data survived
    assert_eq!(cluster.get("key1").await, Some("value1".into()));
    assert_eq!(cluster.get("key2").await, Some("value2".into()));
}
```

**Run simulation tests:**
```bash
# Single run with specific seed
RUSTFLAGS="--cfg madsim" SIM_SEED=12345 cargo test simulation

# Multiple seeds
for seed in $(seq 1 1000); do
    RUSTFLAGS="--cfg madsim" SIM_SEED=$seed cargo test simulation || echo "FAILED: seed=$seed"
done
```

### Phase 3: Storage Fault Injection (3-5 days)

Create a storage backend trait with fault injection.

**Create `src/storage.rs`:**
```rust
use std::io;

/// Storage backend trait - enables fault injection in tests
pub trait StorageBackend: Send + Sync {
    fn execute(&self, sql: &str, params: &[&dyn rusqlite::ToSql]) -> Result<(), StorageError>;
    fn query<T, F>(&self, sql: &str, params: &[&dyn rusqlite::ToSql], f: F) -> Result<Vec<T>, StorageError>
    where
        F: FnMut(&rusqlite::Row) -> Result<T, rusqlite::Error>;
}

#[derive(Debug)]
pub enum StorageError {
    Sqlite(rusqlite::Error),
    Io(io::Error),
    Injected(InjectedFault),
}

#[derive(Debug, Clone)]
pub enum InjectedFault {
    DiskFull,
    CorruptedRead,
    CorruptedWrite,
    SlowWrite { delay_ms: u64 },
    RandomFailure { probability: f64 },
}

/// Real SQLite storage for production
pub struct SqliteStorage {
    conn: rusqlite::Connection,
}

impl StorageBackend for SqliteStorage {
    // ... real implementation
}

/// Fault-injecting storage for tests
pub struct FaultStorage<S: StorageBackend> {
    inner: S,
    fault_config: FaultConfig,
    rng: std::cell::RefCell<rand::rngs::StdRng>,
}

pub struct FaultConfig {
    pub read_fault_rate: f64,      // 0.0 - 1.0
    pub write_fault_rate: f64,
    pub corruption_rate: f64,
    pub slow_write_ms: Option<u64>,
}

impl<S: StorageBackend> StorageBackend for FaultStorage<S> {
    fn execute(&self, sql: &str, params: &[&dyn rusqlite::ToSql]) -> Result<(), StorageError> {
        let mut rng = self.rng.borrow_mut();

        // Maybe inject write fault
        if rng.gen::<f64>() < self.fault_config.write_fault_rate {
            return Err(StorageError::Injected(InjectedFault::DiskFull));
        }

        // Maybe inject slow write
        if let Some(delay) = self.fault_config.slow_write_ms {
            std::thread::sleep(std::time::Duration::from_millis(delay));
        }

        // Maybe corrupt the write (write succeeds but data is wrong)
        if rng.gen::<f64>() < self.fault_config.corruption_rate {
            // Don't actually execute, pretend success
            return Ok(());
        }

        self.inner.execute(sql, params)
    }

    fn query<T, F>(&self, sql: &str, params: &[&dyn rusqlite::ToSql], f: F) -> Result<Vec<T>, StorageError>
    where
        F: FnMut(&rusqlite::Row) -> Result<T, rusqlite::Error>
    {
        let mut rng = self.rng.borrow_mut();

        // Maybe inject read fault
        if rng.gen::<f64>() < self.fault_config.read_fault_rate {
            return Err(StorageError::Injected(InjectedFault::CorruptedRead));
        }

        self.inner.query(sql, params, f)
    }
}
```

### Phase 4: Redis Compatibility Oracle (2-3 days)

Compare redlite against Redis to catch behavioral differences.

**Create `tests/oracle.rs`:**
```rust
use redis::Commands;

/// Run same operations against Redis and Redlite, compare results
struct OracleTest {
    redis: redis::Client,
    redlite: Redlite,
    divergences: Vec<Divergence>,
}

#[derive(Debug)]
struct Divergence {
    operation: String,
    redis_result: String,
    redlite_result: String,
}

impl OracleTest {
    fn compare<T: PartialEq + std::fmt::Debug>(
        &mut self,
        op: &str,
        redis_result: T,
        redlite_result: T,
    ) {
        if redis_result != redlite_result {
            self.divergences.push(Divergence {
                operation: op.to_string(),
                redis_result: format!("{:?}", redis_result),
                redlite_result: format!("{:?}", redlite_result),
            });
        }
    }
}

#[test]
fn oracle_string_commands() {
    let mut oracle = OracleTest::new();

    // SET/GET
    oracle.redis.set::<_, _, ()>("key", "value").unwrap();
    oracle.redlite.set("key", "value", None).unwrap();
    oracle.compare(
        "GET key",
        oracle.redis.get::<_, Option<String>>("key").unwrap(),
        oracle.redlite.get("key").unwrap(),
    );

    // INCR on non-existent
    oracle.compare(
        "INCR newkey",
        oracle.redis.incr::<_, _, i64>("newkey", 1).unwrap(),
        oracle.redlite.incr("newkey").unwrap(),
    );

    // APPEND
    oracle.compare(
        "APPEND key suffix",
        oracle.redis.append::<_, _, i64>("key", "suffix").unwrap(),
        oracle.redlite.append("key", "suffix").unwrap(),
    );

    assert!(oracle.divergences.is_empty(),
        "Found {} divergences:\n{:#?}",
        oracle.divergences.len(),
        oracle.divergences
    );
}
```

### Phase 5: Continuous Simulation (Fly.io) (2-3 days)

Run thousands of simulation iterations on ephemeral fly.io machines.

**Create `battle-test/` crate:**

```rust
// battle-test/src/main.rs
use clap::Parser;

#[derive(Parser)]
struct Args {
    /// Starting seed
    #[arg(long, default_value = "0")]
    seed_start: u64,

    /// Number of seeds to test
    #[arg(long, default_value = "10000")]
    seed_count: u64,

    /// Operations per seed
    #[arg(long, default_value = "1000")]
    ops_per_seed: usize,

    /// Output file for failures
    #[arg(long, default_value = "failures.json")]
    output: String,
}

fn main() {
    let args = Args::parse();
    let mut failures = vec![];

    for seed in args.seed_start..(args.seed_start + args.seed_count) {
        print!("\rTesting seed {}/{}", seed - args.seed_start + 1, args.seed_count);

        match run_simulation(seed, args.ops_per_seed) {
            Ok(()) => {},
            Err(e) => {
                eprintln!("\nFAILED: seed={} error={:?}", seed, e);
                failures.push(FailedSeed { seed, error: format!("{:?}", e) });
            }
        }
    }

    println!("\n\nCompleted: {} failures out of {} seeds",
        failures.len(), args.seed_count);

    if !failures.is_empty() {
        std::fs::write(&args.output, serde_json::to_string_pretty(&failures).unwrap())
            .unwrap();
        std::process::exit(1);
    }
}

fn run_simulation(seed: u64, ops: usize) -> Result<(), SimError> {
    let mut rng = StdRng::seed_from_u64(seed);
    let db = Redlite::open(":memory:")?;

    // Track expected state for verification
    let mut expected: HashMap<String, Value> = HashMap::new();

    for _ in 0..ops {
        let op = generate_op(&mut rng);

        match &op {
            Op::Set { key, value } => {
                db.set(key, value, None)?;
                expected.insert(key.clone(), Value::String(value.clone()));
            }
            Op::Get { key } => {
                let result = db.get(key)?;
                let expect = expected.get(key).map(|v| v.as_string());
                if result != expect {
                    return Err(SimError::Mismatch {
                        op: format!("{:?}", op),
                        expected: format!("{:?}", expect),
                        got: format!("{:?}", result),
                    });
                }
            }
            // ... more operations
        }
    }

    Ok(())
}
```

**Fly.io deployment:**

```toml
# battle-test/fly.toml
app = "redlite-battle"
primary_region = "ord"

[build]
dockerfile = "Dockerfile"

[env]
RUST_LOG = "warn"
```

```dockerfile
# battle-test/Dockerfile
FROM rust:1.75 as builder
WORKDIR /app
COPY . .
RUN cargo build --release -p battle-test

FROM debian:bookworm-slim
COPY --from=builder /app/target/release/battle-test /usr/local/bin/
ENTRYPOINT ["battle-test"]
```

**Run on fly.io:**
```bash
# Test 100K seeds across 10 machines in parallel
for i in $(seq 0 9); do
    fly machine run redlite-battle \
        --region ord \
        --vm-size shared-cpu-2x \
        --env SEED_START=$((i * 10000)) \
        --env SEED_COUNT=10000 \
        -- --seed-start $((i * 10000)) --seed-count 10000 &
done
wait

# Cost: 10 machines × 30 min × $0.006/hr ≈ $0.03 total
```

---

## Test Matrix

### Properties to Verify

| Property | What | Test |
|----------|------|------|
| **Durability** | Committed data survives crashes | Crash recovery simulation |
| **Atomicity** | MULTI/EXEC all-or-nothing | Transaction property tests |
| **Isolation** | SELECT db isolation | Cross-db leak tests |
| **Linearizability** | Operations appear atomic | Concurrent operation tests |
| **Redis Compatibility** | Same behavior as Redis | Oracle comparison tests |

### Fault Scenarios

| Fault | Injection | Expected Behavior |
|-------|-----------|-------------------|
| Crash mid-write | Kill process during EXEC | Rollback or complete, never partial |
| Disk full | Return ENOSPC on write | Graceful error, no corruption |
| Corrupted read | Return garbage from disk | Detect via checksums, error |
| Slow I/O | 100ms+ write latency | Timeout handling works |
| Connection drop | TCP RST mid-command | Clean connection cleanup |

### Scale Scenarios

| Scenario | Parameters | Success Criteria |
|----------|------------|------------------|
| Key scale | 1M keys | No OOM, queries complete |
| Connection scale | 1000 concurrent | No deadlock, all served |
| Value scale | 100MB strings | Memory bounded |
| History scale | 10K versions/key | Queries performant |

---

## Running Tests

### Quick Validation (Local, < 1 min)
```bash
# Property tests
cargo test properties

# Unit tests
cargo test --lib
```

### Thorough Validation (Local, ~10 min)
```bash
# Extended property tests
PROPTEST_CASES=10000 cargo test properties

# Simulation tests
RUSTFLAGS="--cfg madsim" cargo test simulation

# Oracle tests (requires Redis running)
docker run -d -p 6379:6379 redis:latest
cargo test oracle
```

### Full Battle Test (Fly.io, ~$1)
```bash
cd battle-test
./run-battle.sh --seeds 100000 --parallel 10
```

### Nightly CI
```yaml
# .github/workflows/battle-test.yml
name: Battle Test
on:
  schedule:
    - cron: '0 4 * * *'  # 4 AM UTC daily
  workflow_dispatch:

jobs:
  battle:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4

      - name: Property tests (extended)
        run: PROPTEST_CASES=100000 cargo test properties

      - name: Simulation tests
        run: RUSTFLAGS="--cfg madsim" cargo test simulation

      - name: Fly.io battle test
        env:
          FLY_API_TOKEN: ${{ secrets.FLY_API_TOKEN }}
        run: |
          cd battle-test
          ./run-battle.sh --seeds 1000000
```

---

## When a Test Fails

1. **Note the seed**: Every failure prints a reproducible seed
2. **Reproduce locally**: `PROPTEST_SEED=0xABC cargo test the_failing_test`
3. **Minimize**: proptest automatically shrinks to minimal failing case
4. **Fix**: The seed will fail consistently until bug is fixed
5. **Add regression test**: Keep the seed as a specific test case

Example workflow:
```bash
$ cargo test properties
# ... FAILED seed=0x1a2b3c4d5e6f

$ PROPTEST_SEED=0x1a2b3c4d5e6f cargo test properties
# Reproduces exact failure

# After fix:
$ PROPTEST_SEED=0x1a2b3c4d5e6f cargo test properties
# Now passes

# Add to regression tests
#[test]
fn regression_issue_123() {
    // From seed 0x1a2b3c4d5e6f
    let db = Redlite::open(":memory:").unwrap();
    db.set("specific", "case", None).unwrap();
    // ... exact reproduction
}
```

---

## Success Criteria for HN Launch

Before announcing:

- [ ] **100K+ seeds pass** property tests
- [ ] **Zero divergences** from Redis oracle on common commands
- [ ] **Crash recovery** verified with fault injection
- [ ] **1M key scale** tested without OOM
- [ ] **1000 connection scale** tested without deadlock
- [ ] **CI running nightly** for 2+ weeks with no failures

---

## References

- [sled simulation guide](https://sled.rs/simulation.html) - Foundation of this approach
- [MadSim](https://github.com/madsim-rs/madsim) - Deterministic async runtime
- [TigerBeetle VOPR](https://github.com/tigerbeetle/tigerbeetle/blob/main/docs/internals/vopr.md) - Gold standard
- [Polar Signals DST](https://www.polarsignals.com/blog/posts/2025/07/08/dst-rust) - Practical Rust implementation
- [S2 DST](https://s2.dev/blog/dst) - MadSim + Turmoil integration
- [Jepsen](https://jepsen.io) - Why this matters
- [Awesome DST](https://github.com/ivanyu/awesome-deterministic-simulation-testing) - Comprehensive resource list
