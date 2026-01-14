# Next Session: Search & Vectors Implementation

## Context

Redlite is an SQLite-backed Redis-compatible database. We're implementing:
- **RediSearch-compatible FT.\* commands** (full-text search)
- **Redis 8-compatible V\* commands** (vector similarity search with sqlite-vec)

### Completed
- Phase 1 (Session 23.1): FT.CREATE, FT.DROPINDEX, FT._LIST, FT.INFO, FT.ALTER, aliases, synonyms, suggestions
- Phase 2 (Session 23.2): FT.SEARCH with query parser (AND/OR/NOT, phrases, prefix, field-scoped, numeric range, TAG)
- Benchmark enhancements: file-backed DB size measurement, history entry tracking

### Outstanding from Session 23.2
- [ ] Auto-index documents into FTS5 on HSET (documents created after FT.CREATE should be auto-indexed)
- [ ] HIGHLIGHT, SUMMARIZE in FT.SEARCH (parsed but not applied)
- [ ] FT.EXPLAIN and FT.PROFILE

---

## Option A: Session 23.3 - FT.AGGREGATE

Implement SQL-like aggregation pipeline for RediSearch.

### Command Syntax
```
FT.AGGREGATE index query
  [LOAD count field ...]
  [GROUPBY nargs property ... [REDUCE function nargs arg ... [AS name]] ...]
  [SORTBY nargs [property ASC|DESC] ... [MAX num]]
  [APPLY expression AS alias]
  [FILTER expression]
  [LIMIT offset num]
  [TIMEOUT timeout]
```

### REDUCE Functions to Implement
- COUNT, COUNT_DISTINCT, COUNT_DISTINCTISH
- SUM, AVG, MIN, MAX
- FIRST_VALUE, TOLIST
- STDDEV, QUANTILE
- RANDOM_SAMPLE

### Implementation Approach
1. Parse FT.AGGREGATE into an aggregation pipeline
2. Convert pipeline stages to SQL:
   - LOAD -> SELECT fields
   - GROUPBY -> GROUP BY with aggregates
   - SORTBY -> ORDER BY
   - APPLY -> Computed columns (expression parser)
   - FILTER -> WHERE/HAVING clause
   - LIMIT -> LIMIT OFFSET
3. Execute against FTS5 and return results

### Files to Modify
- `src/db.rs` - Add `ft_aggregate()` method
- `src/server/mod.rs` - Add FT.AGGREGATE command routing
- `src/search.rs` - Add expression parser for APPLY/FILTER

### Test Commands
```bash
redis-cli FT.AGGREGATE idx "*" GROUPBY 1 @category REDUCE COUNT 0 AS cnt SORTBY 2 @cnt DESC
redis-cli FT.AGGREGATE idx "@status:active" LOAD 2 @price @qty APPLY "@price * @qty" AS total
```

---

## Option B: Session 24 - Redis 8 Vector Commands

Replace old redlite-native vector API with Redis 8 V\* commands using sqlite-vec.

### Commands to Implement

| Command | Description |
|---------|-------------|
| `VADD key ... element` | Add vector (FP32 blob or VALUES mode) |
| `VSIM key ...` | K-NN similarity search |
| `VREM key element` | Remove element |
| `VCARD key` | Count elements |
| `VDIM key` | Get dimensions |
| `VINFO key` | Index metadata |
| `VEMB key element [RAW]` | Get embedding |
| `VGETATTR key element` | Get JSON attributes |
| `VSETATTR key element json` | Set JSON attributes |
| `VRANDMEMBER key [count]` | Random sampling |

### Command Signatures
```
VADD key (FP32 blob | VALUES n v1 v2...) element [REDUCE dim] [NOQUANT|Q8|BIN] [EF n] [SETATTR json] [M n]
VSIM key (ELE element | FP32 blob | VALUES n v1...) [WITHSCORES] [WITHATTRIBS] [COUNT n] [FILTER expr]
```

### Implementation Steps
1. **Remove old API**: Delete VECTOR ENABLE/DISABLE, old V* commands from db.rs and server/mod.rs
2. **New schema**: Replace `schema_vectors.sql` with Redis 8-compatible `vector_sets` table
3. **sqlite-vec loading**: Add vec0 extension loading in `Db::open()`
4. **Implement V\* commands**: Full Redis 8 API

### Schema
```sql
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
```

### Files to Modify
- `src/schema_vectors.sql` - Replace with Redis 8 schema
- `src/db.rs` - Replace v* methods with Redis 8 API
- `src/server/mod.rs` - Replace V* command routing
- `Cargo.toml` - Ensure sqlite-vec dependency

### Test Commands
```bash
redis-cli VADD vectors VALUES 3 0.1 0.2 0.3 elem1
redis-cli VADD vectors VALUES 3 0.2 0.3 0.4 elem2 SETATTR '{"type":"test"}'
redis-cli VSIM vectors VALUES 3 0.1 0.2 0.3 COUNT 5 WITHSCORES
redis-cli VCARD vectors
redis-cli VDIM vectors
redis-cli VEMB vectors elem1
```

---

## Key Files Reference

| File | Purpose |
|------|---------|
| `src/db.rs` | Core database methods |
| `src/server/mod.rs` | Command routing |
| `src/search.rs` | Query parser (Session 23.2) |
| `src/schema_ft.sql` | FT.* tables |
| `src/schema_vectors.sql` | Vector tables |
| `src/types.rs` | Type definitions |
| `IMPLEMENTATION_PLAN.md` | Full implementation details |
| `ROADMAP.md` | Project roadmap |

---

## Recommendation

**Start with Option B (Vectors)** if you want:
- Clean slate implementation (replace old code)
- Modern Redis 8 compatibility
- sqlite-vec integration experience

**Start with Option A (FT.AGGREGATE)** if you want:
- Complete the search implementation
- More complex SQL generation logic
- Expression parsing challenge

Both are ~500-1000 lines of new code.

---

## Quick Start Commands

```bash
cd /Users/russellromney/Documents/Github/personal-website/redlite

# Build and test
cargo build
cargo test

# Run server for manual testing
cargo run -- --port 6380

# In another terminal
redis-cli -p 6380 PING
```
