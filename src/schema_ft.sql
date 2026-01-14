-- RediSearch-compatible index schema (Session 23)
-- Provides FT.* command compatibility

-- Index definitions
CREATE TABLE IF NOT EXISTS ft_indexes (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    on_type TEXT NOT NULL CHECK(on_type IN ('HASH', 'JSON')),
    prefixes TEXT,                      -- JSON array of key prefixes to index
    schema TEXT NOT NULL,               -- JSON schema definition [{name, type, sortable, noindex}]
    language TEXT DEFAULT 'english',    -- Default language for stemming
    score_field TEXT,                   -- Optional score field for document ranking
    payload_field TEXT,                 -- Optional payload field
    created_at INTEGER NOT NULL DEFAULT (unixepoch('now', 'subsec') * 1000)
);

CREATE INDEX IF NOT EXISTS idx_ft_indexes_name ON ft_indexes(name);

-- Index aliases (multiple names can point to same index)
CREATE TABLE IF NOT EXISTS ft_aliases (
    alias TEXT PRIMARY KEY,
    index_id INTEGER NOT NULL REFERENCES ft_indexes(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_ft_aliases_index_id ON ft_aliases(index_id);

-- Synonym groups for query expansion
CREATE TABLE IF NOT EXISTS ft_synonyms (
    id INTEGER PRIMARY KEY,
    index_id INTEGER NOT NULL REFERENCES ft_indexes(id) ON DELETE CASCADE,
    group_id TEXT NOT NULL,             -- Synonym group identifier
    term TEXT NOT NULL,                 -- Term in the synonym group
    UNIQUE(index_id, group_id, term)
);

CREATE INDEX IF NOT EXISTS idx_ft_synonyms_index ON ft_synonyms(index_id);
CREATE INDEX IF NOT EXISTS idx_ft_synonyms_group ON ft_synonyms(index_id, group_id);

-- Autocomplete suggestions (separate from full-text search)
CREATE TABLE IF NOT EXISTS ft_suggestions (
    id INTEGER PRIMARY KEY,
    key TEXT NOT NULL,                  -- Suggestion dictionary key
    string TEXT NOT NULL,               -- The suggestion string
    score REAL NOT NULL DEFAULT 1.0,    -- Suggestion score/weight
    payload TEXT,                       -- Optional payload data
    UNIQUE(key, string)
);

CREATE INDEX IF NOT EXISTS idx_ft_suggestions_key ON ft_suggestions(key);
CREATE INDEX IF NOT EXISTS idx_ft_suggestions_key_score ON ft_suggestions(key, score DESC);

-- Indexed documents tracking (maps FT index to actual hash/json keys)
CREATE TABLE IF NOT EXISTS ft_indexed_docs (
    id INTEGER PRIMARY KEY,
    index_id INTEGER NOT NULL REFERENCES ft_indexes(id) ON DELETE CASCADE,
    key_id INTEGER NOT NULL REFERENCES keys(id) ON DELETE CASCADE,
    score REAL DEFAULT 1.0,             -- Document score
    payload TEXT,                       -- Optional payload
    UNIQUE(index_id, key_id)
);

CREATE INDEX IF NOT EXISTS idx_ft_indexed_docs_index ON ft_indexed_docs(index_id);
CREATE INDEX IF NOT EXISTS idx_ft_indexed_docs_key ON ft_indexed_docs(key_id);

-- FTS5 virtual table for each index's text content
-- Note: We'll create separate FTS5 tables per index dynamically
-- Format: fts_content_{index_id}
-- This comment documents the expected structure

-- Numeric field values for range queries (separate from FTS5)
CREATE TABLE IF NOT EXISTS ft_numeric_fields (
    id INTEGER PRIMARY KEY,
    index_id INTEGER NOT NULL REFERENCES ft_indexes(id) ON DELETE CASCADE,
    doc_id INTEGER NOT NULL REFERENCES ft_indexed_docs(id) ON DELETE CASCADE,
    field_name TEXT NOT NULL,
    value REAL NOT NULL,
    UNIQUE(index_id, doc_id, field_name)
);

CREATE INDEX IF NOT EXISTS idx_ft_numeric_fields_index ON ft_numeric_fields(index_id, field_name);
CREATE INDEX IF NOT EXISTS idx_ft_numeric_fields_value ON ft_numeric_fields(index_id, field_name, value);

-- TAG field values for exact match queries (separate from FTS5)
CREATE TABLE IF NOT EXISTS ft_tag_fields (
    id INTEGER PRIMARY KEY,
    index_id INTEGER NOT NULL REFERENCES ft_indexes(id) ON DELETE CASCADE,
    doc_id INTEGER NOT NULL REFERENCES ft_indexed_docs(id) ON DELETE CASCADE,
    field_name TEXT NOT NULL,
    tag TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_ft_tag_fields_index ON ft_tag_fields(index_id, field_name);
CREATE INDEX IF NOT EXISTS idx_ft_tag_fields_tag ON ft_tag_fields(index_id, field_name, tag);
