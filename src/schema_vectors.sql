-- Vector search configuration (four-tier opt-in: global, database, pattern, key)
CREATE TABLE IF NOT EXISTS vector_settings (
    id INTEGER PRIMARY KEY,
    level TEXT NOT NULL CHECK(level IN ('global', 'database', 'pattern', 'key')),
    target TEXT NOT NULL,                -- '*' for global, '0-15' for database, 'glob*' for pattern, 'db:key' for key
    enabled BOOLEAN NOT NULL DEFAULT 1,
    dimensions INTEGER NOT NULL,         -- Required: vector dimensions for this config
    created_at INTEGER NOT NULL DEFAULT (unixepoch('now', 'subsec') * 1000),
    UNIQUE(level, target)
);

CREATE INDEX IF NOT EXISTS idx_vector_settings_level_target ON vector_settings(level, target);

-- Vector storage
CREATE TABLE IF NOT EXISTS vectors (
    id INTEGER PRIMARY KEY,
    key_id INTEGER NOT NULL REFERENCES keys(id) ON DELETE CASCADE,
    vector_id TEXT NOT NULL,             -- User-provided ID within the key
    embedding BLOB NOT NULL,             -- Raw float32 vector data
    dimensions INTEGER NOT NULL,         -- Vector dimensions
    metadata TEXT,                       -- Optional JSON metadata
    created_at INTEGER NOT NULL DEFAULT (unixepoch('now', 'subsec') * 1000),
    UNIQUE(key_id, vector_id)
);

CREATE INDEX IF NOT EXISTS idx_vectors_key_id ON vectors(key_id);
