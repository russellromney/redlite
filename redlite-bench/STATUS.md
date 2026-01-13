# redlite-bench Status

## Current Version: 0.1.0

**Status**: ✅ Specification Complete, 🚧 Implementation In Progress

---

## What's Done

### ✅ Complete Specification (spec/benchmark-spec.yaml)
- **48 operations** defined across 6 data types
- **dataset sizes**: 1K, 10K, 100K, 1mm, 10mm, 100mm, 1B
- **concurrency levels**: 1, 2, 4, 8, 16
- **workload scenarios**: read_heavy, write_heavy, balanced, list_queue, stream_processing
- **Multiple output formats**: sqlite (default), console, JSON, CSV, markdown, HTML
- **Comprehensive metrics**: avg/min/max/p50/p95/p99 latency, throughput, resources

### ✅ Documentation
- Complete README.md with motivation, comparison, usage guide
- STATUS.md (this file) tracking progress
- Inline spec documentation with complexity notation

### ✅ Research Completed
- Analyzed existing tools (redis-benchmark, memtier_benchmark, YCSB)
- Identified gaps: Streams (0% coverage), Sorted Sets (partial coverage)
- Confirmed unique value proposition

---

## What's Next

### 🚧 Priority 1: Python Reference Implementation
**Target**: 1-2 days

**Tasks**:
- [ ] Setup Python project structure
- [ ] YAML parser for spec file
- [ ] Backend connection handler
- [ ] Operation implementations (48 operations)
- [ ] Metrics collection (latency, throughput, percentiles)
- [ ] Output formatters (console, JSON, CSV, markdown)
- [ ] CLI interface
- [ ] Test against Redis

**Deliverable**: `python benchmark.py --backend redis://localhost:6379`

### 📋 Priority 2: Validate with Real Data
**Target**: 1 day after Python complete

**Tasks**:
- [ ] Run against Redis (official)
- [ ] Run against Redlite Server
- [ ] Generate comparison report
- [ ] Validate metrics make sense
- [ ] Document findings

**Deliverable**: Proof that spec works in practice

### 📋 Priority 3: JavaScript Implementation
**Target**: 1-2 days

**Tasks**:
- [ ] Node.js project setup
- [ ] Use `ioredis` or `redis` npm package
- [ ] Implement spec parser
- [ ] Operation handlers
- [ ] Output formatters
- [ ] Compare results with Python

**Deliverable**: Cross-language validation

### 📋 Priority 4: Integration with Redlite Rust Benchmarks
**Target**: 2-3 days

**Tasks**:
- [ ] Parse YAML spec in Rust
- [ ] Extend existing comprehensive_comparison.rs
- [ ] Add embedded (Arc<Db>) benchmarks
- [ ] Unified reporting
- [ ] 10K dataset testing

**Deliverable**: Redlite using same spec as other backends

---

## Gaps Filled vs Existing Tools

### redis-benchmark (Built-in Tool)
| Feature | redis-benchmark | redlite-bench |
|---------|-----------------|---------------|
| String operations | SET, GET, INCR | ✅ + APPEND, STRLEN, MGET, MSET |
| List operations | LPUSH, RPUSH, LPOP, RPOP, LRANGE | ✅ + LLEN, LINDEX |
| Hash operations | HSET | ✅ + HGET, HGETALL, HMGET, HLEN, HDEL, HINCRBY |
| Set operations | SADD, SPOP | ✅ + SREM, SMEMBERS, SISMEMBER, SCARD, SRANDMEMBER |
| Sorted Set operations | ❌ **MISSING** | ✅ ZADD, ZREM, ZRANGE, ZSCORE, ZRANK, ZCARD, ZCOUNT, ZRANGEBYSCORE |
| Stream operations | ❌ **COMPLETELY MISSING** | ✅ XADD, XLEN, XRANGE, XREVRANGE, XREAD, XDEL, XTRIM |
| Concurrency testing | Single-threaded | ✅ 1-16 connections |
| Output formats | Console only | ✅ Console, JSON, CSV, Markdown, HTML |

**Net**: redlite-bench adds **35+ operations** missing from redis-benchmark

### memtier_benchmark (Advanced Tool)
| Feature | memtier_benchmark | redlite-bench |
|---------|-------------------|---------------|
| Multithreaded | ✅ Yes | ✅ Configurable |
| Defined operations | ⚠️ Not documented | ✅ 48 operations specified |
| Data type coverage | ⚠️ Unclear | ✅ All 6 types |
| Spec-driven | ❌ No | ✅ YAML specification |
| Cross-language | ❌ C only | ✅ Python, JS, Go, Rust planned |
| Reproducible | ⚠️ Complex config | ✅ Single YAML file |

**Net**: redlite-bench provides **transparency and reproducibility**

---

## Success Metrics

### v0.1.0 Goals ✅
- [x] Complete YAML specification
- [x] Documentation (README, STATUS)
- [x] Research existing tools
- [x] Define scope

### v0.2.0 Goals (In Progress)
- [ ] Python implementation working
- [ ] Benchmark Redis vs Redlite
- [ ] Generate first comparison report
- [ ] Validate spec correctness

### v1.0.0 Goals (Future)
- [ ] 3+ language implementations
- [ ] Published packages
- [ ] Community adoption
- [ ] CI/CD integration

---

## Technical Decisions

### Why YAML for Spec?
- Human-readable
- Easy to edit
- Supported by all languages
- Comments allowed

### Why Multiple Languages?
- **Fair comparison**: Compiled (Go, Rust) vs Interpreted (Python, JS)
- **Client overhead**: Show true network/serialization cost
- **Ecosystem coverage**: Different languages used in different contexts
- **Validation**: Cross-language results validate spec correctness

### Why Not Extend Existing Tools?
- redis-benchmark: C codebase, limited extensibility
- memtier_benchmark: Complex, undocumented operation set
- YCSB: Generic K-V, doesn't leverage Redis features

**Our approach**: Start fresh with spec-driven design

---

## Open Questions

1. **Percentile calculation**: Use HDR Histogram or simple sorting?
   - **Decision needed**: Impact on memory usage for large datasets

2. **Warmup strategy**: How many warmup iterations?
   - **Current spec**: 1000 iterations
   - **Should validate**: Is this enough for stable results?

3. **Error handling**: Fail fast or collect error rates?
   - **Proposal**: Collect error rates as metric, continue on single failures

4. **Memory measurement**: Client or server side?
   - **Complexity**: Server-side requires admin access
   - **Proposal**: Optional, document limitations

---

## Community Impact

**Target audiences**:
1. **Redis users**: Want to benchmark their Redis setup comprehensively
2. **Alternative implementations**: KeyDB, Dragonfly, Valkey, Redlite need fair comparison tool
3. **Researchers**: Need reproducible benchmark methodology
4. **Tool builders**: Can use spec to validate their implementations

**Potential reach**:
- Redis has **millions** of users
- Existing tools have gaps
- First comprehensive, spec-driven benchmark
- Multi-language = broader adoption

---

## Timeline

| Phase | Duration | Deliverable |
|-------|----------|-------------|
| ✅ Spec & Docs | 4 hours | benchmark-spec.yaml, README.md |
| 🚧 Python Impl | 2 days | Working benchmark tool |
| 📋 Validation | 1 day | Redis vs Redlite comparison |
| 📋 JavaScript | 2 days | Second language implementation |
| 📋 Rust Integration | 3 days | Redlite fully integrated |
| 📋 Community | Ongoing | Adoption and feedback |

**Total estimate**: 2-3 weeks to v0.2.0 (usable tool)

---

## How to Contribute

See README.md for:
- Implementation guidelines
- Output format requirements
- Testing procedures

**Most needed right now**:
1. Python implementation (reference)
2. Validation against Redis
3. Feedback on spec completeness

---

**Last Updated**: 2026-01-13
**Maintainer**: Redlite project
**Status**: Active development
