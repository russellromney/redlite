# Benchmark Validation Strategy

**Goal**: Ensure benchmarks are correct and not buggy before adding visualizations

**Priority**: CRITICAL - do this before Phase 7.2 dashboard work

## Why This Matters

The benchmark suite is useless if:
- Setup creates wrong number of keys
- Operation distribution doesn't match scenario weights
- Latency measurements are garbage
- Percentile calculations are wrong
- JSON output is corrupted
- Edge cases crash instead of handling gracefully

## Validation Layers

### Layer 1: Output Correctness (Quick Check)

**Run a single scenario and verify output**:
```bash
./target/release/redlite-bench run-benchmarks \
  --scenarios "read_heavy" \
  --iterations 1000 \
  --dataset-size 100 \
  --report-format json \
  --report-file /tmp/test_report.json
```

**Check JSON structure**:
```bash
jq . /tmp/test_report.json | head -50  # Should be valid JSON
jq '.summary | keys' /tmp/test_report.json  # Should have required fields
jq '.scenarios | length' /tmp/test_report.json  # Should be 1
```

**Verify key fields exist**:
- metadata: timestamp, backend, version
- summary: total_scenarios, redis_scenarios_completed, redlite_scenarios_completed
- scenarios[0]: name, redis, redlite, comparison
- each backend result: throughput_ops_sec, latency_p50_us, latency_p95_us, latency_p99_us

### Layer 2: Setup Validation (Critical)

**Verify setup creates correct number of keys**:

For `read_heavy` scenario:
- Expected: 10,000 strings + 10,000 hash fields = ~11,000 total
- Check by running and examining setup output:

```bash
./target/release/redlite-bench scenario \
  --scenario-file scenarios/comprehensive.yaml \
  --name read_heavy \
  --iterations 100 \
  --backend redlite \
  --memory 2>&1 | grep -i "setup\|keys\|strings\|hashes"
```

**Expected output** (for each scenario):
```
Setup complete: NNNN keys in XXms
  - XXXX strings
  - XXXX hashes
  (etc)
```

**Validation checklist**:
- [ ] String scenarios create correct count
- [ ] Hash scenarios create correct count
- [ ] List scenarios create correct count with right items per list
- [ ] Set scenarios create correct count with right members
- [ ] Sorted set scenarios create correct score distribution
- [ ] Stream scenarios create correct entry count
- [ ] Mixed scenarios sum correctly

### Layer 3: Latency Sanity (Quick)

**Check latency distributions are reasonable**:
```bash
# Extract latency percentiles from JSON
jq '.scenarios[0].redis.latency_p50_us, .latency_p95_us, .latency_p99_us' report.json
jq '.scenarios[0].redlite.latency_p50_us, .latency_p95_us, .latency_p99_us' report.json
```

**Validation rules**:
1. P50 < P95 < P99 (always true for percentiles)
2. All values > 0 (no negative or zero latencies)
3. P50 should be in reasonable range:
   - Redis (network): 300-500 µs typical
   - Redlite (embedded): 1-30 µs typical
4. P99 should be higher but not extreme:
   - Redis: <2000 µs for read ops
   - Redlite: <100 µs for read ops
5. Values should be roughly consistent across runs

**Red flags**:
- P50 > 1 second (way too slow, probably hanging)
- P99 < P50 (math error)
- Negative values (timer bug)
- All zeros (not measuring)
- P99 > 10x P50 (huge tail, might indicate GC/lock contention)

### Layer 4: Operation Distribution (Moderate)

**Verify operations match scenario weights**:

Example: `read_heavy` scenario should have ~50% GET, ~25% SET, ~15% HGET, ~10% HSET

Check by examining dispatcher code and manually running a few ops:
```bash
# Add verbose logging to dispatcher.rs
# Track which operation was executed
# Compare frequency over 1000 ops to expected weights
```

**Manual spot check**:
1. Run `read_heavy` with 1000 iterations
2. Count ops in logs: ~500 GET, ~250 SET, ~150 HGET, ~100 HSET
3. Compare to expected: 50%, 25%, 15%, 10%
4. Should be within ±2%

**How to implement**:
```rust
// Add to dispatcher.rs
eprintln!("[DEBUG] Operation: {} (#{} of {})", op_name, iteration, total);
```

Then:
```bash
./target/release/redlite-bench run-benchmarks --scenarios "read_heavy" 2>&1 | grep "Operation:" | awk '{print $2}' | sort | uniq -c
```

### Layer 5: Throughput Calculation (Moderate)

**Verify throughput is calculated correctly**:

Formula: throughput = successful_ops / duration_secs

Check manually:
```bash
jq '.scenarios[0].redis | "\(.successful_ops) / \(.duration_secs) = \(.throughput_ops_sec)"' report.json
```

Calculate manually and compare:
```bash
python3 -c "print(1000 / 0.4)"  # Should be ~2500 if 1000 ops in 0.4 sec
```

**Validation**:
- Manual calculation matches reported throughput (within 1%)
- Throughput is consistent across runs (within 5%)
- Higher throughput for Redlite vs Redis (expect 10x-100x)

### Layer 6: Edge Cases (Important)

**Test error conditions**:

1. **Redis unavailable**:
   ```bash
   # Kill Redis
   redis-cli shutdown
   # Try to run
   ./target/release/redlite-bench run-benchmarks --scenarios "get_only"
   # Should: log error, skip Redis, continue with Redlite
   # Should NOT: crash
   ```

2. **Empty scenario list**:
   ```bash
   ./target/release/redlite-bench run-benchmarks --scenarios "nonexistent_scenario"
   # Should: report no matching scenarios gracefully
   # Should NOT: crash
   ```

3. **Invalid YAML**:
   ```bash
   ./target/release/redlite-bench run-benchmarks --scenario-file /tmp/broken.yaml --scenarios "anything"
   # Should: report parse error
   # Should NOT: panic
   ```

4. **Large iteration count**:
   ```bash
   ./target/release/redlite-bench run-benchmarks --scenarios "get_only" --iterations 1000000 --dataset-size 100
   # Should: complete within reasonable time (~5 min)
   # May be slower but should not crash
   ```

5. **Very small iteration count**:
   ```bash
   ./target/release/redlite-bench run-benchmarks --scenarios "get_only" --iterations 10
   # Should: complete, report ~10 ops
   # Should NOT crash on small number
   ```

### Layer 7: Consistency Tests (Moderate)

**Run same scenario multiple times, compare**:

```bash
for i in {1..3}; do
  ./target/release/redlite-bench run-benchmarks \
    --scenarios "cache_pattern" \
    --iterations 1000 \
    --dataset-size 100 \
    --report-format json \
    --report-file /tmp/run_$i.json
done

# Compare results
jq '.summary' /tmp/run_*.json
jq '.scenarios[0].redis.throughput_ops_sec' /tmp/run_*.json
```

**Expected**:
- Throughput should be within 10% across runs
- P50 latency should be within 20% across runs
- Winner (Redis vs Redlite) should be consistent

**Red flags**:
- Wildly different throughput (50% variance)
- P50 latency doubles between runs
- Winner changes (unless very close)

## Testing Checklist

**Phase 7.1 Validation Tasks**:
- [ ] Test get_only scenario output structure
- [ ] Test set_only scenario output structure
- [ ] Test read_heavy scenario setup key count
- [ ] Test write_heavy scenario setup key count
- [ ] Verify latency P50 < P95 < P99 for all scenarios
- [ ] Verify all latency values > 0
- [ ] Verify throughput is >100 ops/sec, <1M ops/sec
- [ ] Manual throughput calculation matches reported (within 1%)
- [ ] Run get_only 3x, verify throughput consistent (within 10%)
- [ ] Test with Redis unavailable (should skip gracefully)
- [ ] Test with nonexistent scenario (should error gracefully)
- [ ] Test with small iteration count (10 ops)
- [ ] Test with large iteration count (100k ops)
- [ ] Compare read_heavy P50 latency: Redis 300-500µs, Redlite 1-30µs
- [ ] Verify operation distribution matches weights (spot check)
- [ ] Verify error rates reported correctly
- [ ] Verify JSON is valid on all runs
- [ ] Verify markdown renders without corruption

## How to Run Validation

### Quick Sanity Check (5 minutes)
```bash
cd implementations/rust
cargo build --release
./target/release/redlite-bench run-benchmarks \
  --scenarios "get_only,set_only,read_heavy,write_heavy" \
  --iterations 500 \
  --dataset-size 100 \
  --report-format json > /tmp/validation.json
jq . /tmp/validation.json | head -50  # Check JSON is valid
echo "✓ Basic output validation passed"
```

### Medium Validation (30 minutes)
- Run all core scenarios (5) with 1000 iterations each
- Verify setup counts
- Check latency percentiles
- Manual throughput spot-checks
- Test error conditions

### Full Validation (2 hours)
- Run all 32 scenarios
- Verify setup for each type
- Latency sanity for all
- Consistency tests (3 runs each of 5 scenarios)
- Edge case testing
- Document any anomalies

## Known Issues to Watch For

1. **Session store scenario may fail on Redis** - expected (non-critical)
2. **Very large dataset sizes may run out of memory** - test limits
3. **Streams may have different semantics** between Redis and Redlite
4. **EXPIRE operations on missing keys** - expected behavior varies

## What to Do If You Find a Bug

1. **Document it**: Note the scenario, iterations, dataset size, exact error
2. **Reproduce it**: Can you run it again and get the same error?
3. **Isolate it**: Can you reproduce with fewer iterations?
4. **Fix it**: Update the code
5. **Verify fix**: Run validation again, confirm error is gone
6. **Check regression**: Run other scenarios to ensure no new issues

## Success Criteria

After completing Phase 7.1, we should be able to say:
- "All 32 scenarios run without crashes"
- "Setup creates the right number of keys"
- "Latency measurements are sane (P50 < P95 < P99, reasonable values)"
- "Throughput calculations are correct"
- "Edge cases are handled gracefully"
- "Results are consistent across runs"
- "JSON and markdown outputs are valid"
- "Spot-checked calculations match manual computation"

Only then proceed to Phase 7.2 dashboard work.
