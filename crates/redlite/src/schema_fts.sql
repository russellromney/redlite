-- Full-text search configuration (four-tier opt-in: global, database, pattern, key)
CREATE TABLE IF NOT EXISTS fts_settings (
    id INTEGER PRIMARY KEY,
    level TEXT NOT NULL CHECK(level IN ('global', 'database', 'pattern', 'key')),
    target TEXT NOT NULL,                -- '*' for global, '0-15' for database, 'glob*' for pattern, 'db:key' for key
    enabled BOOLEAN NOT NULL DEFAULT 1,
    created_at INTEGER NOT NULL DEFAULT (unixepoch('now', 'subsec') * 1000),
    UNIQUE(level, target)
);

CREATE INDEX IF NOT EXISTS idx_fts_settings_level_target ON fts_settings(level, target);

-- FTS5 virtual table for full-text search
-- Content is stored in the FTS table to enable highlight() and snippet() functions
CREATE VIRTUAL TABLE IF NOT EXISTS fts USING fts5(
    key_text,           -- The key name (searchable for key-based queries)
    content,            -- The actual content to search
    tokenize='porter unicode61'
);

-- Mapping from FTS rowid to key_id for content retrieval
CREATE TABLE IF NOT EXISTS fts_keys (
    rowid INTEGER PRIMARY KEY,
    key_id INTEGER NOT NULL REFERENCES keys(id) ON DELETE CASCADE,
    db INTEGER NOT NULL,
    key TEXT NOT NULL,
    UNIQUE(db, key)
);

CREATE INDEX IF NOT EXISTS idx_fts_keys_key_id ON fts_keys(key_id);
