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
    db INTEGER NOT NULL DEFAULT 0,
    key TEXT NOT NULL,
    type TEXT NOT NULL CHECK (type IN ('string', 'hash', 'list', 'set', 'zset')),
    expires_at INTEGER,  -- Unix timestamp (milliseconds)
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,
    PRIMARY KEY (db, key)
);
```

| Column | Type | Description |
|--------|------|-------------|
| `db` | INTEGER | Database number (0-15) |
| `key` | TEXT | Key name |
| `type` | TEXT | Data type (string, hash, list, set, zset) |
| `expires_at` | INTEGER | Expiration timestamp in milliseconds (NULL = no expiry) |
| `created_at` | INTEGER | Creation timestamp in milliseconds |
| `updated_at` | INTEGER | Last update timestamp in milliseconds |

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

## WAL Mode

Redlite enables SQLite's Write-Ahead Logging (WAL) mode:

- Concurrent reads while writing
- Better performance for mixed read/write workloads
- Crash recovery
