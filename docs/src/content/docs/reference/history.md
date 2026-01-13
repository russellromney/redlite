---
title: History Tracking
description: Time-travel queries and historical data tracking
---

Redlite provides built-in history tracking with time-travel queries, allowing you to retrieve the state of any key at a specific point in time.

## Overview

History tracking is a **three-tier opt-in** system:
- **Global** - Track history for all keys in all databases
- **Database** - Track history for all keys in a specific database (0-15)
- **Key** - Track history for a specific key only

Each level can have independent retention policies:
- **Unlimited** - Keep all historical versions
- **Time-based** - Delete versions older than N milliseconds
- **Count-based** - Keep only the last N versions

## Configuration

### Enable History Tracking

Enable at the global level (all databases):
```bash
HISTORY ENABLE GLOBAL [RETENTION {TIME ms|COUNT n}]
```

Enable at the database level (database 0):
```bash
HISTORY ENABLE DATABASE 0 [RETENTION {TIME ms|COUNT n}]
```

Enable for a specific key:
```bash
HISTORY ENABLE KEY mykey [RETENTION {TIME ms|COUNT n}]
```

**Default behavior:** No retention specified = unlimited history.

**Example - 30-day retention:**
```bash
HISTORY ENABLE GLOBAL RETENTION TIME 2592000000
```

**Example - Keep last 100 versions:**
```bash
HISTORY ENABLE KEY user:1000 RETENTION COUNT 100
```

### Disable History Tracking

```bash
HISTORY DISABLE GLOBAL
HISTORY DISABLE DATABASE 0
HISTORY DISABLE KEY mykey
```

## Three-Tier Priority

History configuration follows a **priority cascade**:
1. **Key-level** (highest priority) - If enabled for a specific key, use key-level config
2. **Database-level** (medium priority) - If key-level not configured, use database config
3. **Global-level** (lowest priority) - If neither key nor database config, use global

This allows fine-grained control:
```bash
# Track everything for 30 days globally
HISTORY ENABLE GLOBAL RETENTION TIME 2592000000

# But keep user data for 100 versions instead
HISTORY ENABLE KEY user:1000 RETENTION COUNT 100

# Disable history for sensitive keys
HISTORY DISABLE KEY password:secret
```

## Querying History

### HISTORY GET - Retrieve Historical Entries

Get all historical entries for a key:
```bash
HISTORY GET mykey
```

With optional filters:
```bash
# Limit to 10 most recent entries
HISTORY GET mykey LIMIT 10

# Get entries from specific timestamp onwards
HISTORY GET mykey SINCE 1704067200000

# Get entries up to specific timestamp
HISTORY GET mykey UNTIL 1704153600000

# Get entries in a time range
HISTORY GET mykey SINCE 1704067200000 UNTIL 1704153600000 LIMIT 20
```

**Response format:**
```
[
  [
    "version",
    1,
    "operation",
    "SET",
    "timestamp",
    1704067200000,
    "type",
    "string",
    "data",
    "first_value"
  ],
  ...
]
```

### HISTORY GETAT - Time-Travel Query

Get the value of a key at a specific timestamp:
```bash
HISTORY GETAT mykey 1704067200000
```

**Example:**
```bash
# Write some values
SET mykey "v1"  # timestamp: 1704067200000
SET mykey "v2"  # timestamp: 1704067260000
SET mykey "v3"  # timestamp: 1704067320000

# Query historical state
HISTORY GETAT mykey 1704067200000  # Returns "v1"
HISTORY GETAT mykey 1704067260000  # Returns "v2"
HISTORY GETAT mykey 1704067320000  # Returns "v3"
```

## Managing History

### HISTORY STATS - View Statistics

Get statistics about history for a key:
```bash
HISTORY STATS mykey
```

Returns:
```
[
  "total_entries",
  42,
  "oldest_timestamp",
  1704067200000,
  "newest_timestamp",
  1704239200000,
  "storage_bytes",
  15234
]
```

### HISTORY LIST - Find Keys with History

List all keys that have history tracking enabled:
```bash
HISTORY LIST

# With pattern matching
HISTORY LIST PATTERN "user:*"
```

### HISTORY CLEAR - Manual Cleanup

Delete history entries for a key:
```bash
# Delete all history for mykey
HISTORY CLEAR mykey

# Delete history before a timestamp
HISTORY CLEAR mykey BEFORE 1704067200000
```

### HISTORY PRUNE - Global Cleanup

Delete all history entries before a timestamp across all keys:
```bash
HISTORY PRUNE BEFORE 1704067200000
```

## Storage

Reclaim storage with `VACUUM`:
```bash
VACUUM
```

This deletes expired keys and runs SQLite VACUUM to reclaim disk space.

## Examples

### Audit Trail

Track all changes to a critical key:
```bash
# Enable unlimited history for audit trail
HISTORY ENABLE KEY critical:key

# Application writes
SET critical:key "initial"
SET critical:key "updated"
SET critical:key "final"

# Later, audit changes
HISTORY GET critical:key
# Shows all modifications with timestamps
```

### Debugging State Changes

Investigate what happened to a key:
```bash
# Check current state
GET mykey
# "current_value"

# Travel back in time
HISTORY GETAT mykey 1704067200000
# "previous_value"

# See full timeline
HISTORY GET mykey
# [... all versions ...]
```

## Performance Considerations

- **Recording overhead:** Minimal - async background write
- **Query performance:** Fast for recent entries (indexed by timestamp)
- **Storage impact:** ~2-5KB per version for typical values
- **Retention:** Automatically enforced, no manual intervention needed for time/count policies

## Limitations

- History is per-key, not per-field (HSET changes history for entire hash)
- No history for transactions (MULTI/EXEC) - each command recorded separately
- No history for Pub/Sub messages
- History disabled in library mode by default (enable explicitly)

## Related Commands

- `VACUUM` - Delete expired keys and reclaim disk space
- `KEYINFO` - Get key metadata (type, TTL, created/updated timestamps)
