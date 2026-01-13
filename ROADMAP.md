# Roadmap

See [CHANGELOG.md](./CHANGELOG.md) for completed features.

## In Progress

### Sessions 19-21: Language Bindings

**Python (Session 19)** — PyO3 bindings via PyPI
- `open()`, `set()`, `get()`, `delete()` core API
- All data types (hashes, lists, sets, zsets, streams)
- Transactions via context manager

**Node.js/Bun (Session 20)** — NAPI-RS bindings via npm
- Promise-based async API
- TypeScript declarations

**Go (Session 21)** — C FFI + cgo wrapper
- `libredlite` shared library
- Idiomatic Go API

## Planned

### Session 24: Full-Text Search & Vector Search

**FTS5 (built-in)**
```bash
FTS ENABLE KEY article:*
FTS SEARCH "hello world" LIMIT 10
```
- Three-tier opt-in (like History)
- Auto-indexing on SET

**Vector Search (optional `--features vectors`)**

Simplest possible API - 2 commands:
```bash
VIDX ADD myindex doc1 0.1 0.2 0.3 ...    # Add vector (index auto-creates)
VIDX SEARCH myindex 10 0.1 0.2 ...       # K-NN search → [id, distance] pairs
VIDX DEL myindex doc1                     # Delete vector
VIDX COUNT myindex                        # Count vectors
```

Metadata stored separately in hashes (your app joins):
```bash
HSET doc1 title "My Doc" content "The quick brown fox..."
VIDX ADD embeddings doc1 0.1 0.2 0.3 ...
```

Implementation: [sqlite-vector](https://github.com/sqliteai/sqlite-vector)
- Vectors as BLOBs in regular tables (no virtual tables)
- SIMD-accelerated distance calculations
- Quantization for 4-5x speedup
- L2/Cosine/Dot product metrics

### Session 25: Geospatial

R*Tree spatial indexing:
```bash
GEOADD key longitude latitude member
GEOPOS key member
GEODIST key member1 member2 [unit]
GEORADIUS key longitude latitude radius unit
```

### Session 26: Additional Commands

- GETEX, GETDEL, SETEX, PSETEX
- LPUSHX, RPUSHX, LPOS, LMOVE
- BITCOUNT, BITFIELD, BITOP, SETBIT, GETBIT
- RENAME, RENAMENX
- HSCAN, SSCAN, ZSCAN
- ZINTERSTORE, ZUNIONSTORE

### Future

- In-memory mode with periodic snapshots (like Redis RDB)
- HISTORY REPLAY/DIFF for state reconstruction
- Background expiration daemon

## Maybe

- Lua scripting (EVAL/EVALSHA)
- XAUTOCLAIM
- ACL system

## Not Planned

- Cluster mode — Use [walsync](https://github.com/russellromney/walsync) for replication
- Sentinel
- Redis Modules

## Principles

1. **Embedded-first** — Library mode is primary
2. **Disk is cheap** — Don't optimize for memory like Redis
3. **SQLite foundation** — ACID, durability, zero config
4. **Redis-compatible** — Existing clients should work
5. **Extend thoughtfully** — Add features Redis doesn't have
