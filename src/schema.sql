-- Core key metadata
CREATE TABLE IF NOT EXISTS keys (
    id INTEGER PRIMARY KEY,
    db INTEGER NOT NULL DEFAULT 0,
    key TEXT NOT NULL,
    type INTEGER NOT NULL,
    expire_at INTEGER,
    created_at INTEGER NOT NULL DEFAULT (unixepoch('now', 'subsec') * 1000),
    updated_at INTEGER NOT NULL DEFAULT (unixepoch('now', 'subsec') * 1000)
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
