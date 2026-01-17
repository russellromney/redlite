/// Core schema for Redlite - creates all necessary tables and indexes
pub const SCHEMA_CORE: &str = r#"
-- Core key metadata
CREATE TABLE IF NOT EXISTS keys (
    id INTEGER PRIMARY KEY,
    db INTEGER NOT NULL DEFAULT 0,
    key TEXT NOT NULL,
    type INTEGER NOT NULL,
    expire_at INTEGER,
    version INTEGER NOT NULL DEFAULT 0,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_keys_db_key ON keys(db, key);
CREATE INDEX IF NOT EXISTS idx_keys_expire ON keys(expire_at) WHERE expire_at IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_keys_type ON keys(db, type);

-- Strings
CREATE TABLE IF NOT EXISTS strings (
    key_id INTEGER PRIMARY KEY REFERENCES keys(id) ON DELETE CASCADE,
    value BLOB NOT NULL
);

-- Hashes
CREATE TABLE IF NOT EXISTS hashes (
    key_id INTEGER NOT NULL REFERENCES keys(id) ON DELETE CASCADE,
    field TEXT NOT NULL,
    value BLOB NOT NULL,
    PRIMARY KEY (key_id, field)
);

-- Lists (integer positions with gap-based insertion)
CREATE TABLE IF NOT EXISTS lists (
    key_id INTEGER NOT NULL REFERENCES keys(id) ON DELETE CASCADE,
    pos INTEGER NOT NULL,
    value BLOB NOT NULL,
    PRIMARY KEY (key_id, pos)
);

CREATE INDEX IF NOT EXISTS idx_lists_key_pos ON lists(key_id, pos);

-- Sets
CREATE TABLE IF NOT EXISTS sets (
    key_id INTEGER NOT NULL REFERENCES keys(id) ON DELETE CASCADE,
    member BLOB NOT NULL,
    PRIMARY KEY (key_id, member)
);

-- Sorted Sets
CREATE TABLE IF NOT EXISTS zsets (
    key_id INTEGER NOT NULL REFERENCES keys(id) ON DELETE CASCADE,
    member BLOB NOT NULL,
    score REAL NOT NULL,
    PRIMARY KEY (key_id, member)
);

CREATE INDEX IF NOT EXISTS idx_zsets_score ON zsets(key_id, score, member);

-- Streams
CREATE TABLE IF NOT EXISTS streams (
    id INTEGER PRIMARY KEY,
    key_id INTEGER NOT NULL REFERENCES keys(id) ON DELETE CASCADE,
    entry_ms INTEGER NOT NULL,
    entry_seq INTEGER NOT NULL,
    data BLOB NOT NULL,
    created_at INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_streams_key_entry ON streams(key_id, entry_ms, entry_seq);

-- Stream consumer groups
CREATE TABLE IF NOT EXISTS stream_groups (
    id INTEGER PRIMARY KEY,
    key_id INTEGER NOT NULL REFERENCES keys(id) ON DELETE CASCADE,
    name TEXT NOT NULL,
    last_ms INTEGER NOT NULL DEFAULT 0,
    last_seq INTEGER NOT NULL DEFAULT 0,
    UNIQUE(key_id, name)
);

-- Stream pending entries
CREATE TABLE IF NOT EXISTS stream_pending (
    id INTEGER PRIMARY KEY,
    key_id INTEGER NOT NULL REFERENCES keys(id) ON DELETE CASCADE,
    group_id INTEGER NOT NULL REFERENCES stream_groups(id) ON DELETE CASCADE,
    entry_id INTEGER NOT NULL REFERENCES streams(id) ON DELETE CASCADE,
    consumer TEXT NOT NULL,
    delivered_at INTEGER NOT NULL,
    delivery_count INTEGER NOT NULL DEFAULT 1,
    UNIQUE(group_id, entry_id)
);

CREATE INDEX IF NOT EXISTS idx_stream_pending_consumer ON stream_pending(group_id, consumer);

-- Stream consumers
CREATE TABLE IF NOT EXISTS stream_consumers (
    id INTEGER PRIMARY KEY,
    group_id INTEGER NOT NULL REFERENCES stream_groups(id) ON DELETE CASCADE,
    name TEXT NOT NULL,
    seen_time INTEGER NOT NULL DEFAULT 0,
    UNIQUE(group_id, name)
);
"#;
