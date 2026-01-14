# Redlite: RediSearch + Redis 8 Vector Implementation Plan

## Overview

Implement two major features for redlite:
1. **Session 23**: Full RediSearch-compatible FT.* commands (search)
2. **Session 24**: Redis 8-compatible V* commands with sqlite-vec HNSW

**User Choices:**
- Full FT.* implementation (all commands including AGGREGATE, suggestions, synonyms)
- Replace existing redlite-native vector API with Redis 8 V* commands
- Use sqlite-vec for HNSW vector search (not brute-force)

---

## Current State

### Existing FTS (redlite-native)
- `FTS ENABLE/DISABLE GLOBAL|DATABASE|PATTERN|KEY` - four-tier opt-in
- `FTS SEARCH "query" [LIMIT n] [HIGHLIGHT]` - BM25 search
- `FTS REINDEX key`, `FTS INFO`
- Schema: `fts_settings`, `fts` (FTS5 virtual table), `fts_keys`
- Location: [db.rs:6138-6510](src/db.rs#L6138), [server/mod.rs:3166-3417](src/server/mod.rs#L3166)

### Existing Vectors (redlite-native)
- `VECTOR ENABLE/DISABLE`, `VADD`, `VGET`, `VDEL`, `VCOUNT`, `VSEARCH`
- Brute-force KNN search (no HNSW yet)
- Schema: `vector_settings`, `vectors`
- Location: [db.rs:6515-7014](src/db.rs#L6515), [server/mod.rs:3419-3882](src/server/mod.rs#L3419)

---

## Session 23: RediSearch-Compatible API

### New Commands to Implement

#### Priority 1: Core Index & Search
| Command | Description | Complexity |
|---------|-------------|------------|
| `FT.CREATE` | Create index with schema | High |
| `FT.SEARCH` | Search with RediSearch query syntax | High |
| `FT.DROPINDEX` | Drop an index | Low |
| `FT.INFO` | Index metadata | Low |
| `FT._LIST` | List all indexes | Low |

#### Priority 2: Index Management
| Command | Description | Complexity |
|---------|-------------|------------|
| `FT.ALTER` | Add field to schema | Medium |
| `FT.ALIASADD/DEL/UPDATE` | Index aliases | Low |

#### Priority 3: Aggregations
| Command | Description | Complexity |
|---------|-------------|------------|
| `FT.AGGREGATE` | SQL-like aggregations | High |

#### Priority 4: Suggestions & Synonyms
| Command | Description | Complexity |
|---------|-------------|------------|
| `FT.SUGADD/GET/DEL/LEN` | Autocomplete | Medium |
| `FT.SYNUPDATE/DUMP` | Synonyms | Medium |

### Schema Changes

New table: `ft_indexes` for RediSearch-style index definitions:

```sql
CREATE TABLE IF NOT EXISTS ft_indexes (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    on_type TEXT NOT NULL CHECK(on_type IN ('HASH', 'JSON')),
    prefixes TEXT,  -- JSON array of prefixes
    schema TEXT NOT NULL,  -- JSON schema definition
    created_at INTEGER NOT NULL DEFAULT (unixepoch('now', 'subsec') * 1000)
);

CREATE TABLE IF NOT EXISTS ft_aliases (
    alias TEXT PRIMARY KEY,
    index_id INTEGER NOT NULL REFERENCES ft_indexes(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS ft_synonyms (
    id INTEGER PRIMARY KEY,
    index_id INTEGER NOT NULL REFERENCES ft_indexes(id) ON DELETE CASCADE,
    group_id TEXT NOT NULL,
    term TEXT NOT NULL,
    UNIQUE(index_id, group_id, term)
);

CREATE TABLE IF NOT EXISTS ft_suggestions (
    id INTEGER PRIMARY KEY,
    key TEXT NOT NULL,
    string TEXT NOT NULL,
    score REAL NOT NULL DEFAULT 1.0,
    payload TEXT,
    UNIQUE(key, string)
);
```

### Query Syntax Translation

RediSearch query -> FTS5 MATCH:
- `word1 word2` -> `word1 AND word2`
- `word1 | word2` -> `word1 OR word2`
- `-word` -> `NOT word`
- `"exact phrase"` -> `"exact phrase"`
- `prefix*` -> `prefix*`
- `@field:term` -> field-scoped (requires field extraction)
- `@field:[min max]` -> numeric range (handled via WHERE clause)
- `@field:{tag1|tag2}` -> exact match

### Implementation Approach

1. **New module**: `src/search.rs` for query parsing and translation
2. **New schema**: `src/schema_ft.sql` for RediSearch tables
3. **Command handlers**: Add FT.* commands to server/mod.rs
4. **Db methods**: Add ft_* methods to db.rs

---

## Session 24: Redis 8 Vector Sets with sqlite-vec

### Redis 8 V* Commands (Full Implementation)

| Command | Description | Implementation |
|---------|-------------|----------------|
| `VADD key ... element` | Add vector to set | sqlite-vec INSERT |
| `VSIM key ...` | K-NN similarity search | sqlite-vec vec_search |
| `VREM key element` | Remove element | DELETE |
| `VCARD key` | Count elements | COUNT(*) |
| `VDIM key` | Get dimensions | Query metadata |
| `VINFO key` | Index metadata | Stats query |
| `VEMB key element [RAW]` | Get element's vector | SELECT embedding |
| `VGETATTR key element` | Get JSON attributes | SELECT attributes |
| `VSETATTR key element json` | Set JSON attributes | UPDATE |
| `VRANDMEMBER key [count]` | Random sampling | ORDER BY RANDOM() |

### sqlite-vec Integration

sqlite-vec provides:
- SIMD-accelerated distance calculations (L2, cosine, inner product)
- Efficient vector storage as BLOBs
- `vec_search()` function for K-NN queries

**Loading sqlite-vec:**
```rust
// In db.rs open()
conn.load_extension("vec0", None)?;
```

### Schema Changes (Replace Existing)

```sql
-- Drop old redlite-native tables
DROP TABLE IF EXISTS vector_settings;
DROP TABLE IF EXISTS vectors;

-- Redis 8-compatible vector sets
CREATE TABLE IF NOT EXISTS vector_sets (
    id INTEGER PRIMARY KEY,
    key_id INTEGER NOT NULL REFERENCES keys(id) ON DELETE CASCADE,
    element TEXT NOT NULL,
    embedding BLOB NOT NULL,           -- Raw f32 bytes for sqlite-vec
    dimensions INTEGER NOT NULL,
    quantization TEXT DEFAULT 'Q8',    -- Q8, BIN, or NOQUANT
    attributes TEXT,                   -- JSON for FILTER/VGETATTR
    created_at INTEGER NOT NULL DEFAULT (unixepoch('now', 'subsec') * 1000),
    UNIQUE(key_id, element)
);

CREATE INDEX IF NOT EXISTS idx_vector_sets_key ON vector_sets(key_id);
```

### Command Signatures

```
VADD key (FP32 blob | VALUES n v1 v2...) element [REDUCE dim] [NOQUANT|Q8|BIN] [EF n] [SETATTR json] [M n]
VSIM key (ELE element | FP32 blob | VALUES n v1...) [WITHSCORES] [WITHATTRIBS] [COUNT n] [FILTER expr]
VREM key element
VCARD key
VDIM key
VINFO key
VEMB key element [RAW]
VGETATTR key element
VSETATTR key element json
VRANDMEMBER key [count]
```

### Implementation Approach

1. **Remove old API**: Delete VECTOR ENABLE/DISABLE, old VADD/VGET/VDEL/VCOUNT/VSEARCH
2. **New schema**: Replace schema_vectors.sql with Redis 8 schema
3. **sqlite-vec loading**: Add vec0 extension loading on open
4. **New V* commands**: Full Redis 8 API in server/mod.rs
5. **New db methods**: Clean implementation using sqlite-vec functions

---

## Implementation Order

### Phase 1: RediSearch Core (Session 23.1) - COMPLETE
- [x] Create `src/schema_ft.sql` with new tables
- [x] Update migrations in db.rs to include schema_ft.sql
- [x] Add FtIndex, FtField types to types.rs
- [x] Implement FT.CREATE in db.rs (parse schema, store in ft_indexes)
- [x] Implement FT.DROPINDEX, FT._LIST, FT.INFO
- [x] Implement FT.ALTER for adding fields
- [x] Implement FT.ALIASADD/DEL/UPDATE, FT.SYNUPDATE/DUMP, FT.SUGADD/GET/DEL/LEN
- [x] Add command routing in server/mod.rs
- [x] Add comprehensive unit tests (22 tests)

### Phase 2: RediSearch Search (Session 23.2)
- [ ] Create `src/search.rs` for query parser module
- [ ] Implement RediSearch -> FTS5 query translation:
  - AND/OR/NOT operators
  - Phrase matching
  - Prefix search
  - Field-scoped queries (@field:term)
  - Numeric range (@field:[min max])
  - TAG exact match (@field:{tag1|tag2})
- [ ] Implement FT.SEARCH with all options (NOCONTENT, VERBATIM, WITHSCORES, etc.)
- [ ] Add FILTER, RETURN, SUMMARIZE, HIGHLIGHT support
- [ ] Implement FT.EXPLAIN and FT.PROFILE

### Phase 3: RediSearch Aggregations (Session 23.3)
- [ ] Implement FT.AGGREGATE with full syntax:
  - LOAD fields
  - GROUPBY with REDUCE functions (COUNT, SUM, AVG, MIN, MAX, etc.)
  - SORTBY
  - APPLY expressions (parse and convert to SQL)
  - FILTER expressions
  - LIMIT, TIMEOUT

### Phase 4: Redis 8 Vectors with sqlite-vec (Session 24)
- [ ] Remove old redlite-native vector code from db.rs and server/mod.rs
- [ ] Replace schema_vectors.sql with Redis 8-compatible schema
- [ ] Add sqlite-vec extension loading (vec0)
- [ ] Implement all V* commands:
  - VADD (FP32 blob + VALUES input modes, quantization options)
  - VSIM (K-NN using vec_search, FILTER expression support)
  - VREM, VCARD, VDIM
  - VINFO (index metadata)
  - VEMB (with RAW option)
  - VGETATTR, VSETATTR
  - VRANDMEMBER

---

## Files to Modify

| File | Changes |
|------|---------|
| `src/schema_ft.sql` | **NEW**: RediSearch index tables (ft_indexes, ft_aliases, ft_synonyms, ft_suggestions) |
| `src/schema_vectors.sql` | **REPLACE**: Redis 8 vector_sets schema |
| `src/search.rs` | **NEW**: RediSearch query parser module |
| `src/db.rs` | Add ft_* methods, replace v* methods with Redis 8 API |
| `src/server/mod.rs` | Add FT.* commands, replace V* commands |
| `src/lib.rs` | Export search module, update vector types |
| `src/types.rs` | Add FtIndex, FtField types; update Vector types for Redis 8 |
| `Cargo.toml` | Ensure sqlite-vec dependency is correctly configured |

---

## Verification

### Unit Tests (in db.rs / search.rs)
```rust
#[test]
fn test_redisearch_query_parser() {
    // AND/OR/NOT
    assert_eq!(parse_query("hello world"), "hello AND world");
    assert_eq!(parse_query("hello | world"), "hello OR world");
    assert_eq!(parse_query("-hello"), "NOT hello");

    // Field-scoped
    assert_eq!(parse_query("@title:hello"), "title:hello");

    // Phrase
    assert_eq!(parse_query("\"exact phrase\""), "\"exact phrase\"");
}

#[test]
fn test_ft_create_search() {
    let db = Db::open_memory().unwrap();
    db.ft_create("idx", "HASH", &["doc:"], &[("title", "TEXT"), ("body", "TEXT")]).unwrap();
    db.hset("doc:1", &[("title", "Hello"), ("body", "World")]).unwrap();
    let results = db.ft_search("idx", "hello", None).unwrap();
    assert_eq!(results.len(), 1);
}

#[test]
fn test_vadd_vsim() {
    let db = Db::open_memory().unwrap();
    db.vadd("vectors", &[0.1, 0.2, 0.3], "elem1", None).unwrap();
    db.vadd("vectors", &[0.2, 0.3, 0.4], "elem2", None).unwrap();
    let results = db.vsim("vectors", &[0.1, 0.2, 0.3], 2, None).unwrap();
    assert_eq!(results[0].element, "elem1");
}
```

### Integration Tests (redis-cli)
```bash
# Start server
cargo run --features vectors -- --port 6380

# FT.* commands
redis-cli -p 6380 FT.CREATE idx ON HASH PREFIX 1 doc: SCHEMA title TEXT body TEXT
redis-cli -p 6380 HSET doc:1 title "Hello" body "World"
redis-cli -p 6380 FT.SEARCH idx "hello"
redis-cli -p 6380 FT.INFO idx
redis-cli -p 6380 FT._LIST
redis-cli -p 6380 FT.AGGREGATE idx "*" GROUPBY 1 @title REDUCE COUNT 0 AS cnt

# V* commands
redis-cli -p 6380 VADD vectors VALUES 3 0.1 0.2 0.3 elem1
redis-cli -p 6380 VADD vectors VALUES 3 0.2 0.3 0.4 elem2 SETATTR '{"type":"test"}'
redis-cli -p 6380 VSIM vectors VALUES 3 0.1 0.2 0.3 COUNT 5 WITHSCORES
redis-cli -p 6380 VCARD vectors
redis-cli -p 6380 VDIM vectors
redis-cli -p 6380 VEMB vectors elem1
redis-cli -p 6380 VGETATTR vectors elem2
redis-cli -p 6380 VRANDMEMBER vectors 1

# FT.SUGADD/SUGGET
redis-cli -p 6380 FT.SUGADD autocomplete "hello world" 1.0
redis-cli -p 6380 FT.SUGGET autocomplete "hel" FUZZY
```

### Build & Test
```bash
cd redlite
cargo build --features vectors
cargo test --features vectors
cargo clippy --features vectors
```

---

## Summary

This implementation adds full RediSearch and Redis 8 vector compatibility to redlite:

1. **FT.* Commands**: Complete RediSearch API including CREATE, SEARCH, AGGREGATE, suggestions, synonyms
2. **V* Commands**: Full Redis 8 vector set API with sqlite-vec HNSW acceleration
3. **Query Parser**: New module for translating RediSearch query syntax to FTS5
4. **Clean API**: Removes old redlite-native vector commands in favor of standard Redis 8 API

Estimated scope: ~2000-3000 lines of new code across 5-6 files.
