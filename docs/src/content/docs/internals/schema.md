---
title: Schema
description: Redlite's SQLite database schema
---

Redlite stores data in SQLite using a multi-table schema designed for Redis compatibility while leveraging SQLite's strengths.

## Overview

```
┌─────────────────────────────────────────────────────────┐
│                        keys                              │
│  (db, key) → type, expires_at, created_at, updated_at   │
└─────────────────────────────────────────────────────────┘
         │
         ├──────────────────────────────────────┐
         │                                      │
         ▼                                      ▼
┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐
│    strings      │  │     hashes      │  │     lists       │
│  (db, key)      │  │  (db, key,      │  │  (db, key,      │
│     → value     │  │   field)        │  │   position)     │
│                 │  │     → value     │  │     → value     │
└─────────────────┘  └─────────────────┘  └─────────────────┘

┌─────────────────┐  ┌─────────────────┐
│      sets       │  │     zsets       │
│  (db, key,      │  │  (db, key,      │
│   member)       │  │   member)       │
│                 │  │     → score     │
└─────────────────┘  └─────────────────┘
```

## Tables

### keys

Central metadata table for all keys.

```sql
CREATE TABLE keys (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    db INTEGER NOT NULL DEFAULT 0,
    key TEXT NOT NULL,
    type INTEGER NOT NULL,  -- KeyType enum (0=string, 1=hash, 2=list, 3=set, 4=zset, 5=stream)
    expires_at INTEGER,     -- Unix timestamp (milliseconds)
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,
    version INTEGER NOT NULL DEFAULT 0,  -- For WATCH/UNWATCH optimistic locking
    UNIQUE (db, key)
);
```

| Column | Type | Description |
|--------|------|-------------|
| `id` | INTEGER | Auto-increment primary key |
| `db` | INTEGER | Database number (0-15) |
| `key` | TEXT | Key name |
| `type` | INTEGER | Data type (0=string, 1=hash, 2=list, 3=set, 4=zset, 5=stream) |
| `expires_at` | INTEGER | Expiration timestamp in milliseconds (NULL = no expiry) |
| `created_at` | INTEGER | Creation timestamp in milliseconds |
| `updated_at` | INTEGER | Last update timestamp in milliseconds |
| `version` | INTEGER | Version number for optimistic locking (WATCH) |

### strings

String values (binary data stored as BLOB).

```sql
CREATE TABLE strings (
    db INTEGER NOT NULL DEFAULT 0,
    key TEXT NOT NULL,
    value BLOB NOT NULL,
    PRIMARY KEY (db, key),
    FOREIGN KEY (db, key) REFERENCES keys(db, key) ON DELETE CASCADE
);
```

### hashes

Hash field-value pairs.

```sql
CREATE TABLE hashes (
    db INTEGER NOT NULL DEFAULT 0,
    key TEXT NOT NULL,
    field TEXT NOT NULL,
    value BLOB NOT NULL,
    PRIMARY KEY (db, key, field),
    FOREIGN KEY (db, key) REFERENCES keys(db, key) ON DELETE CASCADE
);
```

### lists

List elements with gap-based positioning for O(1) push operations.

```sql
CREATE TABLE lists (
    db INTEGER NOT NULL DEFAULT 0,
    key TEXT NOT NULL,
    position REAL NOT NULL,  -- Gap-based for efficient insert
    value BLOB NOT NULL,
    PRIMARY KEY (db, key, position),
    FOREIGN KEY (db, key) REFERENCES keys(db, key) ON DELETE CASCADE
);
```

The `position` column uses REAL (floating point) to allow efficient insertions:
- Initial elements: 1.0, 2.0, 3.0, ...
- Insert between 1.0 and 2.0: 1.5
- Insert between 1.0 and 1.5: 1.25

### sets

Set members (unique values per key).

```sql
CREATE TABLE sets (
    db INTEGER NOT NULL DEFAULT 0,
    key TEXT NOT NULL,
    member BLOB NOT NULL,
    PRIMARY KEY (db, key, member),
    FOREIGN KEY (db, key) REFERENCES keys(db, key) ON DELETE CASCADE
);
```

### zsets

Sorted set members with scores.

```sql
CREATE TABLE zsets (
    db INTEGER NOT NULL DEFAULT 0,
    key TEXT NOT NULL,
    member BLOB NOT NULL,
    score REAL NOT NULL,
    PRIMARY KEY (db, key, member),
    FOREIGN KEY (db, key) REFERENCES keys(db, key) ON DELETE CASCADE
);

CREATE INDEX zsets_score ON zsets(db, key, score);
```

## Indexes

```sql
-- Fast expiration lookups
CREATE INDEX keys_expires ON keys(expires_at) WHERE expires_at IS NOT NULL;

-- Fast score-based queries for sorted sets
CREATE INDEX zsets_score ON zsets(db, key, score);
```

## Foreign Keys & Cascading Deletes

All value tables reference the `keys` table with `ON DELETE CASCADE`. When a key is deleted from `keys`, the corresponding data is automatically removed.

```sql
-- Deleting from keys table
DELETE FROM keys WHERE db = 0 AND key = 'mykey';

-- Automatically removes related rows from strings, hashes, lists, sets, or zsets
```

## Type Safety

The `type` column in `keys` ensures a key can only have one type at a time. Attempting to use a key with a different type results in a `WRONGTYPE` error (matching Redis behavior).

## Expiration

Expiration is stored as Unix timestamp in milliseconds in `expires_at`. Keys are lazily expired:

1. On read: Check `expires_at`, delete if expired
2. Background cleanup: Not yet implemented (planned)

## Streams

### streams

Stream entries with timestamp-based IDs.

```sql
CREATE TABLE streams (
    key_id INTEGER NOT NULL REFERENCES keys(id) ON DELETE CASCADE,
    entry_ms INTEGER NOT NULL,   -- Timestamp part of entry ID
    entry_seq INTEGER NOT NULL,  -- Sequence part of entry ID
    data BLOB NOT NULL,          -- MessagePack encoded fields
    created_at INTEGER NOT NULL,
    PRIMARY KEY (key_id, entry_ms, entry_seq)
);
```

### stream_groups

Consumer groups for stream processing.

```sql
CREATE TABLE stream_groups (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    key_id INTEGER NOT NULL REFERENCES keys(id) ON DELETE CASCADE,
    name TEXT NOT NULL,
    last_id_ms INTEGER NOT NULL DEFAULT 0,
    last_id_seq INTEGER NOT NULL DEFAULT 0,
    created_at INTEGER NOT NULL,
    UNIQUE (key_id, name)
);
```

### stream_consumers

Individual consumers within groups.

```sql
CREATE TABLE stream_consumers (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    group_id INTEGER NOT NULL REFERENCES stream_groups(id) ON DELETE CASCADE,
    name TEXT NOT NULL,
    last_seen INTEGER NOT NULL,
    pending_count INTEGER NOT NULL DEFAULT 0,
    UNIQUE (group_id, name)
);
```

### stream_pending

Pending entries awaiting acknowledgment.

```sql
CREATE TABLE stream_pending (
    group_id INTEGER NOT NULL REFERENCES stream_groups(id) ON DELETE CASCADE,
    entry_ms INTEGER NOT NULL,
    entry_seq INTEGER NOT NULL,
    consumer_id INTEGER REFERENCES stream_consumers(id) ON DELETE SET NULL,
    delivery_time INTEGER NOT NULL,
    delivery_count INTEGER NOT NULL DEFAULT 1,
    PRIMARY KEY (group_id, entry_ms, entry_seq)
);
```

## History Tracking

### history_config

Three-tier configuration for history tracking.

```sql
CREATE TABLE history_config (
    id INTEGER PRIMARY KEY,
    level TEXT NOT NULL CHECK(level IN ('global', 'database', 'key')),
    target TEXT NOT NULL,  -- '*' for global, '0-15' for db, 'db:key' for key
    enabled BOOLEAN NOT NULL DEFAULT 1,
    retention_type TEXT CHECK(retention_type IN ('unlimited', 'time', 'count')),
    retention_value INTEGER,
    created_at INTEGER NOT NULL,
    UNIQUE(level, target)
);
```

### key_history

Versioned snapshots for time-travel queries.

```sql
CREATE TABLE key_history (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    key_id INTEGER NOT NULL REFERENCES keys(id) ON DELETE CASCADE,
    db INTEGER NOT NULL,
    key TEXT NOT NULL,
    key_type INTEGER NOT NULL,
    version_num INTEGER NOT NULL,
    operation TEXT NOT NULL,      -- 'SET', 'DEL', 'HSET', etc.
    timestamp_ms INTEGER NOT NULL,
    data_snapshot BLOB,           -- MessagePack encoded state
    expire_at INTEGER,
    UNIQUE(key_id, version_num)
);

CREATE INDEX idx_history_key_time ON key_history(key_id, timestamp_ms DESC);
CREATE INDEX idx_history_db_key_time ON key_history(db, key, timestamp_ms DESC);
```

## WAL Mode

Redlite enables SQLite's Write-Ahead Logging (WAL) mode:

- Concurrent reads while writing
- Better performance for mixed read/write workloads
- Crash recovery
