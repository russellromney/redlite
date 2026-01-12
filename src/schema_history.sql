-- History tracking configuration (three-tier opt-in: global, database, key)
CREATE TABLE IF NOT EXISTS history_config (
    id INTEGER PRIMARY KEY,
    level TEXT NOT NULL CHECK(level IN ('global', 'database', 'key')),
    target TEXT NOT NULL,                -- '*' for global, '0-15' for database, 'db:key' for key
    enabled BOOLEAN NOT NULL DEFAULT 1,
    retention_type TEXT CHECK(retention_type IN ('unlimited', 'time', 'count')),
    retention_value INTEGER,             -- Milliseconds for time, count for count
    created_at INTEGER NOT NULL DEFAULT (unixepoch('now', 'subsec') * 1000),
    UNIQUE(level, target)
);

CREATE INDEX IF NOT EXISTS idx_history_config_level_target ON history_config(level, target);

-- Versioned key history (snapshots with MessagePack encoding)
CREATE TABLE IF NOT EXISTS key_history (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    key_id INTEGER NOT NULL REFERENCES keys(id) ON DELETE CASCADE,
    db INTEGER NOT NULL,
    key TEXT NOT NULL,
    key_type INTEGER NOT NULL,           -- KeyType enum value
    version_num INTEGER NOT NULL,
    operation TEXT NOT NULL,             -- 'SET', 'DEL', 'HSET', 'LPUSH', etc.
    timestamp_ms INTEGER NOT NULL,
    data_snapshot BLOB,                  -- MessagePack encoded current state
    expire_at INTEGER,                   -- Optional TTL at time of operation
    UNIQUE(key_id, version_num)
);

CREATE INDEX IF NOT EXISTS idx_history_key_time ON key_history(key_id, timestamp_ms DESC);
CREATE INDEX IF NOT EXISTS idx_history_db_key_time ON key_history(db, key, timestamp_ms DESC);
