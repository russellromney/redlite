---
title: Memory
description: Memory introspection and diagnostics commands in Redlite
---

Memory commands for inspecting memory usage, analyzing cache health, and debugging performance issues.

## Commands

| Command | Syntax | Description |
|---------|--------|-------------|
| MEMORY STATS | `MEMORY STATS` | Get memory statistics |
| MEMORY USAGE | `MEMORY USAGE key` | Get memory used by a specific key |
| MEMORY DOCTOR | `MEMORY DOCTOR` | Analyze cache health and get diagnostics |

## Examples

### Memory Statistics

```bash
127.0.0.1:6379> MEMORY STATS
1) "total.allocated"
2) (integer) 1048576
3) "keys.count"
4) (integer) 42
5) "eviction.policy"
6) "allkeys-lru"
```

Returns:
- `total.allocated` - Total memory used by all keys (bytes)
- `keys.count` - Number of keys in current database
- `eviction.policy` - Current eviction policy

### Memory Usage per Key

```bash
# Check memory used by a specific key
127.0.0.1:6379> SET mykey "Hello World"
OK

127.0.0.1:6379> MEMORY USAGE mykey
(integer) 56

# Large hash
127.0.0.1:6379> HSET user:1 name "Alice" email "alice@example.com" bio "..."
(integer) 3

127.0.0.1:6379> MEMORY USAGE user:1
(integer) 248

# Non-existent key returns nil
127.0.0.1:6379> MEMORY USAGE nonexistent
(nil)
```

### Memory Doctor

The MEMORY DOCTOR command analyzes cache health and provides actionable diagnostics:

```bash
127.0.0.1:6379> MEMORY DOCTOR
Sam, I have examined your cache and found:

* Memory: 524288 / 1048576 bytes (50.0% used)
  - OK: Memory usage is healthy
* Eviction policy: allkeys-lru
* Disk: 2097152 / 10485760 bytes (20.0% used)
* Keys: 150 total, 45 with TTL
  - OK: No stale expired keys
* Key types:
  - string: 80
  - hash: 35
  - list: 20
  - set: 10
  - zset: 5

I'm healthy! No issues found.
```

### Doctor Warnings

When issues are detected:

```bash
127.0.0.1:6379> MEMORY DOCTOR
Sam, I have examined your cache and found:

* Memory: 943718 / 1048576 bytes (90.0% used)
  - WARNING: Memory usage is high (>=90%), eviction is actively occurring
* Eviction policy: noeviction
  - WARNING: noeviction policy with maxmemory set - writes may fail when full
* Keys: 1000 total, 200 with TTL
  - WARNING: 50 expired keys awaiting cleanup (run VACUUM)
* Key types:
  - string: 800
  - hash: 150
  - list: 50

2 issue(s) detected. Consider addressing the warnings above.
```

### Finding Large Keys

```bash
# Check memory for each key to find largest
127.0.0.1:6379> KEYS *
1) "user:1"
2) "cache:large"
3) "sessions"

127.0.0.1:6379> MEMORY USAGE user:1
(integer) 256

127.0.0.1:6379> MEMORY USAGE cache:large
(integer) 10485760

127.0.0.1:6379> MEMORY USAGE sessions
(integer) 1024
```

## Diagnostic Scenarios

### High Memory Usage

```bash
127.0.0.1:6379> MEMORY DOCTOR
# Shows: WARNING: Memory usage is high (>=90%)

# Actions:
# 1. Check for large keys
127.0.0.1:6379> KEYS *
# 2. Review eviction policy
127.0.0.1:6379> CONFIG GET maxmemory-policy
# 3. Increase limit or enable eviction
127.0.0.1:6379> CONFIG SET maxmemory-policy allkeys-lru
```

### Expired Keys Not Cleaned

```bash
127.0.0.1:6379> MEMORY DOCTOR
# Shows: WARNING: 50 expired keys awaiting cleanup

# Run vacuum to clean up
127.0.0.1:6379> VACUUM
(integer) 50  # Keys cleaned

127.0.0.1:6379> MEMORY DOCTOR
# Shows: OK: No stale expired keys
```

### No Memory Limit Set

```bash
127.0.0.1:6379> MEMORY DOCTOR
# Shows: INFO: No maxmemory limit set, eviction disabled

# Set a limit to enable eviction
127.0.0.1:6379> CONFIG SET maxmemory 104857600
OK
127.0.0.1:6379> CONFIG SET maxmemory-policy allkeys-lru
OK
```

## Library Mode (Rust)

```rust
use redlite::Db;

let db = Db::open("mydata.db")?;

// Get total memory usage
let total_bytes = db.total_memory_usage()?;
println!("Total memory: {} bytes", total_bytes);

// Get memory for specific key
if let Some(key_id) = db.get_key_id("mykey")? {
    let bytes = db.calculate_key_memory(key_id)?;
    println!("Key memory: {} bytes", bytes);
}

// Run health check
let report = db.memory_doctor()?;
for line in report {
    println!("{}", line);
}

// Configuration
db.set_max_memory(100 * 1024 * 1024);  // 100MB limit
println!("Max memory: {} bytes", db.max_memory());

db.set_eviction_policy(EvictionPolicy::AllKeysLru);
println!("Policy: {}", db.eviction_policy().to_str());
```

## Understanding Memory Usage

### What's Counted

Memory usage includes:
- Key metadata (name, type, TTL, timestamps)
- String values (raw bytes)
- Hash field-value pairs
- List elements
- Set members
- Sorted set members with scores
- Stream entries
- JSON documents

### Memory vs Disk

- `MEMORY USAGE` shows logical data size
- Actual SQLite file may be larger due to:
  - Page overhead
  - Free space from deletions
  - WAL file (if using WAL mode)

Use `VACUUM` to reclaim disk space after deleting many keys.

## Use Cases

### Capacity Planning

```bash
# Monitor memory growth
MEMORY STATS
# ... add data ...
MEMORY STATS

# Set appropriate limits
CONFIG SET maxmemory 1073741824  # 1GB
CONFIG SET maxmemory-policy allkeys-lru
```

### Debugging Memory Issues

```bash
# Regular health checks
MEMORY DOCTOR

# Find memory hogs
SCAN 0 COUNT 100
# For each key:
MEMORY USAGE <key>
```

### Cache Optimization

```bash
# Check if eviction is working
MEMORY DOCTOR
# Look for: "eviction is actively occurring"

# Verify expired key cleanup
MEMORY DOCTOR
# Look for: "expired keys awaiting cleanup"
VACUUM
```
