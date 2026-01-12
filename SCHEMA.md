# SQLite Schema

## Tables

```sql
-- Core key metadata (shared across all types)
CREATE TABLE keys (
    id INTEGER PRIMARY KEY,
    db INTEGER NOT NULL DEFAULT 0,
    key TEXT NOT NULL,
    type INTEGER NOT NULL,  -- 1=string, 2=hash, 3=list, 4=set, 5=zset
    expire_at INTEGER,      -- unix ms, NULL = no expiry
    created_at INTEGER NOT NULL DEFAULT (unixepoch('now', 'subsec') * 1000),
    updated_at INTEGER NOT NULL DEFAULT (unixepoch('now', 'subsec') * 1000)
);

CREATE UNIQUE INDEX idx_keys_db_key ON keys(db, key);
CREATE INDEX idx_keys_expire ON keys(expire_at) WHERE expire_at IS NOT NULL;
CREATE INDEX idx_keys_type ON keys(db, type);

-- Strings: simple key-value
CREATE TABLE strings (
    key_id INTEGER PRIMARY KEY REFERENCES keys(id) ON DELETE CASCADE,
    value BLOB NOT NULL
);

-- Hashes: field-value pairs per key
CREATE TABLE hashes (
    key_id INTEGER NOT NULL REFERENCES keys(id) ON DELETE CASCADE,
    field TEXT NOT NULL,
    value BLOB NOT NULL,
    PRIMARY KEY (key_id, field)
);

-- Lists: ordered elements with integer positions (gap-based)
CREATE TABLE lists (
    key_id INTEGER NOT NULL REFERENCES keys(id) ON DELETE CASCADE,
    pos INTEGER NOT NULL,
    value BLOB NOT NULL,
    PRIMARY KEY (key_id, pos)
);

CREATE INDEX idx_lists_key_pos ON lists(key_id, pos);

-- Sets: unique members per key
CREATE TABLE sets (
    key_id INTEGER NOT NULL REFERENCES keys(id) ON DELETE CASCADE,
    member BLOB NOT NULL,
    PRIMARY KEY (key_id, member)
);

-- Sorted sets: members with scores
CREATE TABLE zsets (
    key_id INTEGER NOT NULL REFERENCES keys(id) ON DELETE CASCADE,
    member BLOB NOT NULL,
    score REAL NOT NULL,
    PRIMARY KEY (key_id, member)
);

CREATE INDEX idx_zsets_score ON zsets(key_id, score, member);
```

## Design Notes

### Why a `keys` table?

- Single place for expiration logic
- Type checking before operations
- `DEL` cascades to all data tables automatically
- Easy `KEYS pattern` and `SCAN` implementation
- Supports multiple databases (SELECT 0-15)
- `created_at`/`updated_at` metadata (unique to Redlite)

### Why `INTEGER` for list positions?

Gap-based positioning enables O(1) insertions without reindexing:

```
Initial:        pos = [0]
RPUSH a:        pos = [0, 1000000]
RPUSH b:        pos = [0, 1000000, 2000000]
LPUSH x:        pos = [-1000000, 0, 1000000, 2000000]
LINSERT BEFORE a: pos = [-1000000, -500000, 0, 1000000, 2000000]
```

- Gap of 1,000,000 between positions
- LINSERT uses midpoint: `(prev + next) / 2`
- Rebalance when gap < 2 (after ~20 insertions in same spot)
- Rebalancing reassigns all positions with fresh gaps

### Why `BLOB` for values?

- Redis is binary-safe
- TEXT would mangle binary data
- Strings store/retrieve fine as BLOB

### Why `created_at` / `updated_at`?

Unique to Redlite. Redis doesn't track this. Useful for:
- Debugging ("when was this key last modified?")
- Auditing
- Future history tracking (V2)

Exposed via custom `KEYINFO` command.

### Expiration

**Lazy expiration (V1):** Check on every read, delete if expired.

```sql
-- Lazy: filter in queries
WHERE (expire_at IS NULL OR expire_at > :now)

-- VACUUM command: batch delete expired
DELETE FROM keys WHERE expire_at IS NOT NULL AND expire_at <= ?
```

**Active expiration (V2):** Background daemon cleans up periodically.

### Multiple Databases

The `db` column supports `SELECT 0-15`:

```sql
-- All queries filter by current db
WHERE db = ? AND key = ?

-- FLUSHDB only affects current db
DELETE FROM keys WHERE db = ?
```

## Example Queries

### GET key

```sql
SELECT s.value FROM strings s
JOIN keys k ON k.id = s.key_id
WHERE k.db = ? AND k.key = ?
  AND (k.expire_at IS NULL OR k.expire_at > :now);
```

### SET key value EX 60

```sql
-- Upsert key
INSERT INTO keys (db, key, type, expire_at, updated_at)
VALUES (?, ?, 1, :now + 60000, :now)
ON CONFLICT(db, key) DO UPDATE SET
  type = excluded.type,
  expire_at = excluded.expire_at,
  updated_at = excluded.updated_at;

-- Upsert value
INSERT INTO strings (key_id, value) VALUES (?, ?)
ON CONFLICT(key_id) DO UPDATE SET value = excluded.value;
```

### LPUSH key value

```sql
-- Get minimum position
SELECT MIN(pos) FROM lists WHERE key_id = ?;

-- Insert with position = min - 1000000 (or 0 if empty)
INSERT INTO lists (key_id, pos, value) VALUES (?, ?, ?);
```

### ZADD key score member

```sql
INSERT INTO zsets (key_id, member, score) VALUES (?, ?, ?)
ON CONFLICT(key_id, member) DO UPDATE SET score = excluded.score;
```

### ZRANGEBYSCORE key min max LIMIT offset count

```sql
SELECT member, score FROM zsets
WHERE key_id = ? AND score BETWEEN ? AND ?
ORDER BY score, member
LIMIT ? OFFSET ?;
```

### SCAN cursor MATCH pattern COUNT count

```sql
SELECT key FROM keys
WHERE db = ?
  AND id > :cursor_id  -- cursor is last seen id
  AND (expire_at IS NULL OR expire_at > :now)
  AND key GLOB :pattern
ORDER BY id
LIMIT :count + 1;  -- +1 to check if more exist
```

### KEYINFO key (custom)

```sql
SELECT type, expire_at, created_at, updated_at
FROM keys
WHERE db = ? AND key = ?
  AND (expire_at IS NULL OR expire_at > :now);
```

## Performance Considerations

1. **WAL mode** - Enable with `PRAGMA journal_mode = WAL`
2. **Foreign keys** - Enable with `PRAGMA foreign_keys = ON`
3. **Busy timeout** - Set with `PRAGMA busy_timeout = 5000`
4. **Partial indexes** - `idx_keys_expire` only indexes non-null values
5. **Covering indexes** - `idx_zsets_score` includes member for range queries
6. **Type index** - `idx_keys_type` enables fast SCAN TYPE filtering
