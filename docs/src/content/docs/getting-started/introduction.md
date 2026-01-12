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

Redlite is a great choice when you need:

- **Persistent storage** without running a separate Redis server
- **Embedded key-value store** in a Rust application
- **Simple deployment** — it's just a SQLite file
- **Redis protocol compatibility** for existing tools and libraries
- **ACID transactions** and durability guarantees

## When NOT to Use Redlite

Redlite may not be the best choice for:

- **High-throughput, low-latency workloads** where Redis's in-memory model excels
- **Distributed systems** requiring replication (though this is planned for the future)
- **Pub/Sub heavy workloads** (not yet implemented)

## Feature Status

| Feature | Status |
|---------|--------|
| GET/SET | ✅ Done |
| DEL, EXISTS, KEYS | 🔜 Planned |
| Expiration (TTL) | ✅ Done |
| Hashes | 🔜 Planned |
| Lists | 🔜 Planned |
| Sets | 🔜 Planned |
| Sorted Sets | 🔜 Planned |
| Server Mode | ✅ Done |
| Embedded Mode | ✅ Done |
