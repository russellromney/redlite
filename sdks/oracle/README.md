# Redlite SDK Oracle Tests

Cross-SDK consistency testing framework. Ensures all Redlite SDKs produce identical results for the same operations, benchmarked against Redis behavior.

## Overview

The oracle testing system uses YAML specification files that define expected behavior for Redis commands. Each SDK has a runner that executes these specs and compares results.

**Current Status**: 137 tests passing for both Python and TypeScript SDKs

```
oracle/
├── spec/           # YAML test specifications
│   ├── strings.yaml    # 29 tests
│   ├── hashes.yaml     # 18 tests
│   ├── keys.yaml       # 26 tests
│   ├── lists.yaml      # 22 tests
│   ├── sets.yaml       # 16 tests
│   └── zsets.yaml      # 26 tests
├── runners/        # SDK-specific test runners
│   ├── python_runner.py
│   └── ts_runner.js
├── Makefile
└── README.md
```

## Quick Start

```bash
# Run all oracle tests
make test

# Run Python SDK tests only
make test-python

# Run with verbose output
make test-verbose

# Run a single spec
make test-spec SPEC=strings.yaml
```

## Test Specification Format

```yaml
name: String Commands
version: "1.0"

tests:
  - name: SET and GET roundtrip
    setup:
      - { cmd: SET, args: ["setup_key", "setup_value"] }
    operations:
      - { cmd: SET, args: ["key", "value"], expect: true }
      - { cmd: GET, args: ["key"], expect: "value" }
```

### Expectation Types

```yaml
# Exact matches
expect: "value"              # String (bytes decoded to UTF-8)
expect: 42                   # Integer
expect: true                 # Boolean
expect: null                 # None/nil

# Collections
expect: ["a", "b", "c"]      # Ordered list
expect: { set: ["a", "b"] }  # Unordered set
expect: { dict: { "k": "v" } }  # Dictionary

# Numeric ranges
expect: { range: [58, 60] }  # Inclusive range (for TTL, etc.)
expect: { approx: 3.14, tol: 0.01 }  # Float with tolerance

# Type checks
expect: { type: "bytes" }    # Just check type, not value
expect: { contains: "error" }  # Substring match

# Binary data
expect: { bytes: [0, 1, 255] }  # Raw bytes
```

## Adding Tests

1. Add test cases to the appropriate spec file in `spec/`
2. Run `make test-verbose` to verify
3. If adding a new command, ensure all SDK runners support it

## Adding SDK Runners

Create a new runner in `runners/` that:
1. Parses YAML spec files
2. Creates a fresh database per test
3. Executes setup and operations
4. Compares results using the expectation types
5. Reports pass/fail with details

See `python_runner.py` for reference implementation.

## Coverage

| Spec File | Tests | Commands Covered |
|-----------|-------|-----------------|
| strings.yaml | 29 | GET, SET, MGET, MSET, INCR, DECR, APPEND, STRLEN, GETRANGE, SETRANGE, SETEX, PSETEX, GETDEL |
| hashes.yaml | 18 | HSET, HGET, HDEL, HGETALL, HMGET, HEXISTS, HLEN, HKEYS, HVALS, HINCRBY |
| keys.yaml | 26 | DEL, EXISTS, TYPE, TTL, PTTL, EXPIRE, PEXPIRE, PERSIST, RENAME, RENAMENX, KEYS, DBSIZE, FLUSHDB |
| lists.yaml | 22 | LPUSH, RPUSH, LPOP, RPOP, LLEN, LRANGE, LINDEX |
| sets.yaml | 16 | SADD, SREM, SMEMBERS, SISMEMBER, SCARD |
| zsets.yaml | 26 | ZADD, ZREM, ZSCORE, ZCARD, ZCOUNT, ZINCRBY, ZRANGE, ZREVRANGE |

## Future Work

- [x] TypeScript runner (`runners/ts_runner.js`)
- [ ] Rust reference runner (baseline)
- [ ] CI integration for PR checks
- [ ] Additional spec files: scan.yaml (SCAN, HSCAN, SSCAN, ZSCAN)
