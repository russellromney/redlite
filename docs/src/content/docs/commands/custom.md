---
title: Custom Commands
description: Redlite-specific commands not in Redis
---

Redlite extends Redis with custom commands that leverage SQLite's capabilities.

## VACUUM

Delete expired keys and run SQLite VACUUM to reclaim disk space.

```bash
127.0.0.1:6379> VACUUM
OK
```

### Why VACUUM?

Redlite uses **lazy expiration** - expired keys are only deleted when accessed. This is efficient but can leave expired data on disk. VACUUM:

1. Scans all databases for expired keys and deletes them
2. Runs SQLite `VACUUM` to reclaim disk space

### Library Mode

```rust
let deleted_count = db.vacuum()?;
println!("Deleted {} expired keys", deleted_count);
```

## AUTOVACUUM

Configure automatic background cleanup of expired keys.

```bash
# Check status
127.0.0.1:6379> AUTOVACUUM
1) "enabled"
2) "true"
3) "interval_ms"
4) "60000"

# Enable (default)
127.0.0.1:6379> AUTOVACUUM ON
OK

# Disable
127.0.0.1:6379> AUTOVACUUM OFF
OK

# Set interval (minimum 1000ms)
127.0.0.1:6379> AUTOVACUUM INTERVAL 30000
OK
```

### How It Works

When enabled, autovacuum runs periodically during read operations:
- Triggered on GET, HGET, SMEMBERS, ZRANGE, LRANGE, EXISTS
- Uses compare-and-exchange to ensure only one connection runs cleanup
- Default interval: 60 seconds

### Library Mode

```rust
// Check if enabled
let enabled = db.autovacuum_enabled();

// Enable/disable
db.set_autovacuum(true);
db.set_autovacuum(false);

// Set interval (milliseconds)
db.set_autovacuum_interval(30_000);  // 30 seconds
```

## KEYINFO

Get detailed metadata about a key.

```bash
127.0.0.1:6379> SET mykey "hello" EX 3600
OK
127.0.0.1:6379> KEYINFO mykey
1) "type"
2) "string"
3) "ttl"
4) (integer) 3599500
5) "created_at"
6) (integer) 1704067200000
7) "updated_at"
8) (integer) 1704067200000
```

### Fields

| Field | Description |
|-------|-------------|
| `type` | Key type: `string`, `hash`, `list`, `set`, `zset`, `stream` |
| `ttl` | Time-to-live in milliseconds (nil if no expiration) |
| `created_at` | Creation timestamp (Unix milliseconds) |
| `updated_at` | Last update timestamp (Unix milliseconds) |

### Library Mode

```rust
if let Some(info) = db.keyinfo("mykey")? {
    println!("Type: {:?}", info.key_type);
    println!("TTL: {:?}ms", info.ttl);
    println!("Created: {}", info.created_at);
    println!("Updated: {}", info.updated_at);
}
```

## HISTORY

Track and query historical data with time-travel queries. See [History Tracking](/reference/history) for full documentation.

### Enable/Disable

```bash
# Enable at different levels
HISTORY ENABLE GLOBAL [RETENTION {TIME ms|COUNT n}]
HISTORY ENABLE DATABASE 0 [RETENTION {TIME ms|COUNT n}]
HISTORY ENABLE KEY mykey [RETENTION {TIME ms|COUNT n}]

# Disable
HISTORY DISABLE GLOBAL
HISTORY DISABLE DATABASE 0
HISTORY DISABLE KEY mykey
```

### Query History

```bash
# Get history entries
HISTORY GET mykey [LIMIT n] [SINCE timestamp] [UNTIL timestamp]

# Time-travel query
HISTORY GETAT mykey 1704067200000
```

### Manage History

```bash
# List keys with history
HISTORY LIST [PATTERN pattern]

# Get statistics
HISTORY STATS [KEY key]

# Clean up
HISTORY CLEAR key [BEFORE timestamp]
HISTORY PRUNE BEFORE timestamp
```

### Example

```bash
# Enable history for a key
127.0.0.1:6379> HISTORY ENABLE KEY user:1 RETENTION COUNT 100
OK

# Make changes
127.0.0.1:6379> SET user:1 "version1"
OK
127.0.0.1:6379> SET user:1 "version2"
OK

# Query history
127.0.0.1:6379> HISTORY GET user:1
1) 1) "version"
   2) (integer) 1
   3) "operation"
   4) "SET"
   5) "timestamp"
   6) (integer) 1704067200000
   7) "type"
   8) "string"
   9) "data"
   10) "version1"
2) ...

# Time-travel
127.0.0.1:6379> HISTORY GETAT user:1 1704067200000
"version1"
```

### Library Mode

```rust
use redlite::{Db, RetentionType};

// Enable history
db.history_enable_key("user:1", Some(RetentionType::Count(100)))?;

// Query history
let entries = db.history_get("user:1", Some(10), None, None)?;

// Time-travel query
let snapshot = db.history_get_at("user:1", 1704067200000)?;

// Statistics
let stats = db.history_stats("user:1")?;

// Cleanup
db.history_clear_key("user:1", Some(1704067200000))?;
db.history_prune(1704067200000)?;
```

## Why Custom Commands?

SQLite provides capabilities that Redis doesn't have natively:

1. **VACUUM** - SQLite manages disk space explicitly; Redis doesn't have this concept
2. **AUTOVACUUM** - Configurable background cleanup leveraging SQLite's efficiency
3. **KEYINFO** - SQLite stores rich metadata; Redis only tracks basic info
4. **HISTORY** - SQLite's ACID properties make versioning reliable and efficient
