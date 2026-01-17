---
title: Memory & Disk Eviction
description: How Redlite manages memory and disk limits with automatic eviction
---

Redlite supports automatic eviction of keys when memory or disk limits are exceeded. This allows you to use Redlite as a cache with bounded resource usage.

## Memory vs Disk Eviction

| Feature | Memory (`maxmemory`) | Disk (`maxdisk`) |
|---------|---------------------|------------------|
| Limit | RAM usage estimate | SQLite file size |
| Policy | Configurable (LRU, LFU, random, etc.) | Fixed: oldest-first |
| Scope | Current database only | All databases (0-15) |
| Config | `--eviction-policy` | N/A |

Both use vacuum-first strategy: expired keys are deleted before evicting valid keys.

## Quick Start

```bash
# Limit to 100MB memory with LRU eviction
./redlite --max-memory 104857600 --eviction-policy allkeys-lru

# Limit to 500MB disk
./redlite --max-disk 524288000

# Both limits
./redlite --max-memory 104857600 --max-disk 524288000 --eviction-policy allkeys-lfu
```

## Memory Eviction

Memory eviction removes keys when the estimated memory usage exceeds `maxmemory`. This is useful for caching scenarios where you want to limit RAM usage.

**Key behavior:**
- Uses configurable policies (LRU, LFU, random, TTL-based)
- Only evicts from the **current selected database** (set via `SELECT`)

### Configuration

**Command line:**
```bash
./redlite --max-memory 104857600 --eviction-policy allkeys-lru
```

**Runtime (via CONFIG):**
```bash
CONFIG SET maxmemory 104857600        # 100MB limit
CONFIG SET maxmemory-policy allkeys-lru
CONFIG GET maxmemory
CONFIG GET maxmemory-policy
```

**Embedded API:**
```rust
use redlite::{Db, EvictionPolicy};

let db = Db::open_memory()?;
db.set_max_memory(100 * 1024 * 1024);  // 100MB
db.set_eviction_policy(EvictionPolicy::AllKeysLRU);
```

### Eviction Policies

| Policy | Description |
|--------|-------------|
| `noeviction` | Never evict keys. Returns error when memory limit exceeded. **(Default)** |
| `allkeys-lru` | Evict least recently used keys from all keys |
| `allkeys-lfu` | Evict least frequently used keys from all keys |
| `allkeys-random` | Evict random keys from all keys |
| `volatile-lru` | Evict least recently used keys among keys with TTL |
| `volatile-lfu` | Evict least frequently used keys among keys with TTL |
| `volatile-ttl` | Evict keys with shortest remaining TTL |
| `volatile-random` | Evict random keys among keys with TTL |

### How LRU/LFU Works

Redlite tracks access patterns for LRU and LFU eviction:

- **LRU (Least Recently Used)**: Tracks when each key was last accessed. Keys that haven't been read or written recently are evicted first.
- **LFU (Least Frequently Used)**: Tracks how often each key is accessed. Keys with fewer accesses are evicted first.

Access tracking uses an in-memory HashMap for fast updates, with periodic persistence to SQLite columns for durability.

### Sampling-Based Eviction

Like Redis, Redlite uses **sampling** to find eviction candidates rather than scanning all keys. When eviction is needed:

1. Sample 5 random keys
2. Pick the "worst" key among the samples (oldest access for LRU, lowest count for LFU)
3. Evict that key

This provides O(1) eviction performance even with millions of keys, at the cost of approximate rather than perfect LRU/LFU behavior.

### Volatile Policies

Volatile policies only consider keys that have a TTL set:

```bash
# Only evict keys with expiration
CONFIG SET maxmemory-policy volatile-lru
```

If no keys have TTL and the volatile policy can't find candidates, Redlite will return an error on writes when memory is full.

## Disk Eviction

Disk eviction removes keys when the SQLite database file exceeds `maxdisk`. Unlike memory eviction, disk eviction:

- Uses **oldest-first** ordering (by creation time) - no configurable policy
- Operates across **all databases** (0-15), not just the current one

### Configuration

**Command line:**
```bash
./redlite --max-disk 524288000  # 500MB limit
```

**Runtime:**
```bash
CONFIG SET maxdisk 524288000
CONFIG GET maxdisk
```

**Embedded API:**
```rust
let db = Db::open("data.db")?;
db.set_max_disk(500 * 1024 * 1024);  // 500MB
```

### Behavior

- Eviction checks run on write operations (SET, HSET, LPUSH, etc.)
- Checks are throttled to once per second to minimize overhead
- Oldest keys (by creation time) are evicted first
- Disk size is calculated from SQLite page count

## Vacuum-First Strategy

When eviction is triggered, Redlite first attempts to reclaim space by deleting **expired keys**:

1. **Phase 1: Vacuum** - Delete all expired keys (keys with past TTL)
2. **Phase 2: Evict** - If still over limit, evict valid keys based on policy

This ensures expired keys (which would be deleted on access anyway) are cleaned up before evicting valid user data.

## Access Tracking Configuration

For LRU/LFU policies, you can tune access tracking behavior:

### Persistence

```bash
# Enable/disable access tracking persistence to disk
CONFIG SET persist-access-tracking on   # Persist to SQLite (default for :memory:)
CONFIG SET persist-access-tracking off  # In-memory only (default for file DBs)
CONFIG GET persist-access-tracking
```

When disabled, access tracking still works in-memory but isn't persisted across restarts.

### Flush Interval

```bash
# How often to flush access tracking to disk (milliseconds)
CONFIG SET access-flush-interval 5000   # 5 seconds (default for :memory:)
CONFIG SET access-flush-interval 300000 # 5 minutes (default for file DBs)
CONFIG GET access-flush-interval
```

**Trade-offs:**
- **Shorter intervals**: More accurate LRU/LFU but more disk I/O
- **Longer intervals**: Less disk I/O but LRU/LFU is approximate within the window

### Smart Defaults

| Database Type | persist-access-tracking | access-flush-interval |
|--------------|------------------------|----------------------|
| `:memory:` | `on` | 5 seconds |
| File-based | `off` | 5 minutes |

File-based databases default to `off` to avoid WAL bloat and S3 replication costs when using tools like Litestream.

## Memory Calculation

Redlite estimates memory usage per key:

```
key_memory = key_name_length + value_size + overhead (~150 bytes)
```

Value size varies by type:
- **String**: Length of the value bytes
- **Hash**: Sum of all field names + values
- **List**: Sum of all element sizes
- **Set**: Sum of all member sizes
- **ZSet**: Sum of all member sizes + 8 bytes per score
- **Stream**: Sum of all entry data

Check memory usage:
```bash
MEMORY USAGE mykey          # Memory used by a specific key
MEMORY STATS                # Overall memory statistics
```

## Performance Considerations

### Eviction Overhead

- Eviction checks run on **write operations only**
- Checks are **throttled to once per second**
- Sampling approach is O(1) regardless of key count
- Access tracking adds ~1 microsecond per read operation

### Recommended Settings

**High-performance cache:**
```bash
./redlite --storage memory \
  --max-memory 1073741824 \
  --eviction-policy allkeys-lru \
  --cache 1024
```

**Persistent with bounded disk:**
```bash
./redlite --db /data/cache.db \
  --max-disk 10737418240 \
  --max-memory 1073741824 \
  --eviction-policy allkeys-lfu
```

**Minimal overhead (no LRU/LFU tracking):**
```bash
./redlite --db /data/cache.db \
  --max-disk 5368709120 \
  --eviction-policy allkeys-random
```

### Monitoring

```bash
# Check current limits
CONFIG GET maxmemory
CONFIG GET maxdisk
CONFIG GET maxmemory-policy

# Check usage
MEMORY STATS
DBSIZE
INFO
```

## Comparison with Redis

| Feature | Redlite | Redis |
|---------|---------|-------|
| Memory eviction | Yes | Yes |
| Disk eviction | Yes | No |
| LRU/LFU policies | Yes (approximate) | Yes (approximate) |
| Volatile policies | Yes | Yes |
| Sampling size | 5 keys | 5 keys (configurable) |
| Access tracking | HashMap + SQLite | In-memory only |
| Vacuum-first | Yes | No |
