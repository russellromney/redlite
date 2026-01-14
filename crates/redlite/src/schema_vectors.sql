-- Redis 8 Vector Sets Schema
-- Replaces the old vector_settings/vectors tables with Redis 8 compatible vector_sets table

-- Drop old tables if they exist (migration)
DROP TABLE IF EXISTS vectors;
DROP TABLE IF EXISTS vector_settings;

-- Vector sets storage (Redis 8 compatible)
-- Each key is a "vector set" containing multiple elements with embeddings
CREATE TABLE IF NOT EXISTS vector_sets (
    id INTEGER PRIMARY KEY,
    key_id INTEGER NOT NULL REFERENCES keys(id) ON DELETE CASCADE,
    element TEXT NOT NULL,              -- Element name (unique within key)
    embedding BLOB NOT NULL,            -- Raw float32 vector data (FP32)
    dimensions INTEGER NOT NULL,        -- Vector dimensions (auto-detected from first element)
    quantization TEXT DEFAULT 'NOQUANT', -- Quantization type: NOQUANT, Q8, BF16
    attributes TEXT,                    -- Optional JSON attributes
    created_at INTEGER NOT NULL DEFAULT (unixepoch('now', 'subsec') * 1000),
    UNIQUE(key_id, element)
);

CREATE INDEX IF NOT EXISTS idx_vector_sets_key_id ON vector_sets(key_id);
CREATE INDEX IF NOT EXISTS idx_vector_sets_element ON vector_sets(key_id, element);
