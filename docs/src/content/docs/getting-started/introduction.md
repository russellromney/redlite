---
title: Introduction
description: SQLite-backed Redis-compatible key-value store
---

Redlite is a **SQLite-backed Redis-compatible key-value store** written in Rust. It can be used as an embedded library or as a standalone server.

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

- **Throughput** — Lower than Redis (SQLite I/O vs in-memory). Embedded mode: ~50k-230k ops/sec. Server mode: ~2k-3k ops/sec over TCP.
- **Latency** — Higher than Redis due to disk I/O
- **Memory** — Configurable page cache, not constrained by total RAM
- **Durability** — All writes are durable by default (SQLite WAL mode)

**Limitations:**

- **Replication** — No built-in replication (use external tools like Litestream or walsync)
- **Clustering** — Single-node only
- **Lua scripting** — Not supported

## Feature Status

| Feature | Status |
|---------|--------|
| Strings (GET, SET, INCR, etc.) | ✅ Complete |
| Key Management (DEL, EXISTS, KEYS, SCAN, TTL) | ✅ Complete |
| Hashes (HSET, HGET, etc.) | ✅ Complete |
| Lists (LPUSH, RPOP, LREM, LINSERT, etc.) | ✅ Complete |
| Sets (SADD, SMEMBERS, SMOVE, etc.) | ✅ Complete |
| Sorted Sets (ZADD, ZRANGE, etc.) | ✅ Complete |
| Streams (XADD, XREAD, Consumer Groups) | ✅ Complete |
| Transactions (MULTI/EXEC/DISCARD/WATCH/UNWATCH) | ✅ Complete |
| Pub/Sub (Server Mode Only) | ✅ Complete |
| Blocking Reads (Server Mode Only) | ✅ Complete |
| History Tracking & Time-Travel | ✅ Complete |
| Authentication (AUTH, --password) | ✅ Complete |
| Client Commands (CLIENT LIST, etc.) | ✅ Complete |
| Cache Configuration (--cache) | ✅ Complete |
| Full-Text Search (RediSearch) | ✅ Complete |
| Vector Search (requires `--features vectors`) | ✅ Complete |
| Geospatial (requires `--features geo`) | ✅ Complete |
| Language Bindings (Python, TypeScript, Go, PHP, Elixir, etc.) | ✅ Complete |
