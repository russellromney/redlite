---
title: Introduction
description: What is Redlite and why use it?
---

Redlite is a **SQLite-backed Redis-compatible key-value store** written in Rust. It's designed to be embedded directly in your application, though it can also run as a standalone server.

## Core Principles

1. **Embedded-first** — Library mode is the primary use case
2. **Disk is cheap** — Don't optimize for memory like Redis does
3. **SQLite is the foundation** — Leverage its strengths (ACID, durability, zero config)
4. **Redis-compatible** — Existing clients should just work
5. **Extend thoughtfully** — Add features Redis doesn't have (KEYINFO, history, FTS in the future)

## When to Use Redlite

Use Redlite when you need:

- **Persistent storage** without running a separate Redis server
- **Embedded key-value store** in a Rust application
- **Simple deployment** — it's just a SQLite file
- **Redis protocol compatibility** for existing tools and libraries
- **ACID transactions** and durability guarantees

## When NOT to Use Redlite

Redlite is not suitable for:

- **High-throughput, low-latency workloads** where Redis's in-memory model excels
- **Distributed systems** requiring built-in replication (though this is planned via walsync)
- **Heavy Lua scripting workloads** (Lua scripting not supported)

## Feature Status

**Current Status:** Sessions 1-23 complete

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
| Python/Node.js/Go Bindings | 🔜 Sessions 19-21 |
| Full-Text Search | 🔜 Session 24 |
| Vector Search | 🔜 Session 24 |
| Geospatial | 🔜 Session 25 |
