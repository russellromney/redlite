---
title: Introduction
description: SQLite-backed Redis-compatible key-value store
---

Redlite is a SQLite-backed Redis-compatible key-value store written in Rust. It operates as an embedded library or standalone server.

## Architecture

Redlite implements the Redis protocol on top of SQLite's storage engine:

1. **Storage layer** — SQLite provides ACID transactions and durability
2. **Data model** — Redis data types (strings, hashes, lists, sets, sorted sets, streams) mapped to SQLite tables
3. **Protocol** — RESP (Redis Serialization Protocol) compatibility for network access
4. **Deployment modes** — Embedded library (FFI) or standalone TCP server
5. **Extensions** — Additional commands beyond Redis (KEYINFO, HISTORY, VACUUM)

## Use Cases

Redlite supports these deployment patterns:

- **Embedded storage** — Link directly into applications via FFI bindings
- **Persistent key-value storage** — Disk-backed storage with configurable memory cache
- **Single-file deployment** — SQLite database file contains all data
- **Redis protocol compatibility** — Works with existing Redis clients
- **Cross-process access** — Multiple processes can access the same database file via SQLite's WAL mode

## Technical Trade-offs

**Performance characteristics:**

- **Throughput** — Embedded mode: 53k-232k ops/sec. Server mode: 2k-3k ops/sec over TCP (localhost).
- **Latency** — Higher than in-memory Redis due to SQLite I/O
- **Memory** — Configurable page cache (default: 64MB), not constrained by total RAM
- **Durability** — All writes are durable by default (SQLite WAL mode)

**Limitations:**

- **Replication** — No built-in replication (use external tools like Litestream or walsync)
- **Clustering** — Single-node only
- **Lua scripting** — Not supported

## Feature Status

| Feature | Status |
|---------|--------|
| Strings (GET, SET, INCR, etc.) | ✅ Implemented |
| Key Management (DEL, EXISTS, KEYS, SCAN, TTL) | ✅ Implemented |
| Hashes (HSET, HGET, etc.) | ✅ Implemented |
| Lists (LPUSH, RPOP, LREM, LINSERT, etc.) | ✅ Implemented |
| Sets (SADD, SMEMBERS, SMOVE, etc.) | ✅ Implemented |
| Sorted Sets (ZADD, ZRANGE, etc.) | ✅ Implemented |
| Streams (XADD, XREAD, Consumer Groups) | ✅ Implemented |
| Transactions (MULTI/EXEC/DISCARD/WATCH/UNWATCH) | ✅ Implemented |
| Pub/Sub (Server Mode Only) | ✅ Implemented |
| Blocking Reads (Server Mode Only) | ✅ Implemented |
| History Tracking & Time-Travel | ✅ Implemented |
| Authentication (AUTH, --password) | ✅ Implemented |
| Client Commands (CLIENT LIST, etc.) | ✅ Implemented |
| Disk Eviction (--max-disk, CONFIG SET maxdisk) | ✅ Implemented |
| Full-Text Search (RediSearch) | ✅ Implemented |
| Vector Search (requires `--features vectors`) | ✅ Implemented |
| Geospatial (requires `--features geo`) | ✅ Implemented |
| Language Bindings (Python, TypeScript, Go, Ruby, Dart, etc.) | ✅ Implemented |
