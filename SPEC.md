# Redlite: SQLite-backed Redis-compatible Key-Value Store

A lightweight, embeddable Redis-compatible KV store built on SQLite. Written in Rust.

## Overview

Redlite implements the Redis protocol (RESP) backed by SQLite, providing:

- **Redis wire protocol compatibility** - works with existing Redis clients
- **SQLite storage** - ACID transactions, no separate server process
- **Embeddable Rust API** - use as a library without network overhead
- **Persistent by default** - data survives restarts
- **Multiple databases** - SELECT 0-15 like Redis

## Documentation

- [SPEC.md](./SPEC.md) - This file, overview and design decisions
- [COMMANDS.md](./COMMANDS.md) - Supported Redis commands
- [SCHEMA.md](./SCHEMA.md) - SQLite schema and design notes
- [ROADMAP.md](./ROADMAP.md) - Feature roadmap (V1/V2/V3)
- [RUST_TYPES.md](./RUST_TYPES.md) - Core Rust types and error handling
- [RUST_DB.md](./RUST_DB.md) - Database implementation (strings, keys)
- [RUST_COLLECTIONS.md](./RUST_COLLECTIONS.md) - Hashes, lists, sets, sorted sets
- [RUST_SERVER.md](./RUST_SERVER.md) - RESP protocol and TCP server
- [PROJECT.md](./PROJECT.md) - Project structure and Cargo.toml

## Quick Start

### As a server

```bash
# Start server
redlite --db=data.db --addr=127.0.0.1:6379

# Use any Redis client
redis-cli -p 6379
127.0.0.1:6379> SET foo bar
OK
127.0.0.1:6379> GET foo
"bar"
```

### As a library

```rust
use redlite::{Db, ZMember};
use std::time::Duration;

fn main() -> anyhow::Result<()> {
    let db = Db::open("myapp.db")?;

    // Strings
    db.set("name", b"Alice", None)?;
    db.set("session", b"xyz", Some(Duration::from_secs(3600)))?;

    // Hashes
    db.hset("user:1", &[("name", b"Bob"), ("email", b"bob@example.com")])?;

    // Lists
    db.lpush("queue", &[b"job1", b"job2"])?;

    // Sets
    db.sadd("tags", &[b"rust", b"sqlite", b"redis"])?;

    // Sorted Sets
    db.zadd("leaderboard", &[
        ZMember { score: 100.0, member: b"alice".to_vec() },
        ZMember { score: 200.0, member: b"bob".to_vec() },
    ])?;

    Ok(())
}
```

## Design Goals

1. **Simplicity** - Single binary, single SQLite file
2. **Compatibility** - Works with existing Redis clients
3. **Embeddable** - Use as library or server
4. **Durable** - Disk-first, not memory-first

## Design Decisions

### Why SQLite?

- ACID transactions out of the box
- Single file deployment
- Battle-tested durability
- Disk is 100x cheaper than memory
- Easy replication with walsync (future)

### Expiration Strategy

**Lazy expiration only (V1):**
- Expired keys checked on read, deleted if expired
- Explicit `VACUUM` command cleans up expired keys
- No background daemon needed
- Disk is cheap — expired keys sitting on disk is fine

**Active expiration (V2):**
- Opt-in background daemon for cleanup

### List Positioning

Uses integer positions with large gaps (1,000,000) for O(1) insertions:

```
LPUSH: pos = MIN(pos) - 1,000,000
RPUSH: pos = MAX(pos) + 1,000,000
LINSERT: pos = (prev + next) / 2
```

Rebalance when gap < 2 (rare — requires ~20 insertions in same spot).

### SCAN Cursors

Matches Redis: opaque integers. "0" means start/done.

### Custom Commands

**VACUUM** - Explicitly delete expired keys and run SQLite VACUUM
```
VACUUM
```

**KEYINFO** - Get metadata not available in Redis
```
KEYINFO mykey
> type: string
> ttl: 3600
> created_at: 1704067200000
> updated_at: 1704067200000
```

### What We Don't Support (and Why)

| Feature | Reason |
|---------|--------|
| WATCH | Use SQLite transactions in library mode. See ROADMAP for V2+. |
| Lua scripting | Out of scope |
| Clustering | Not the use case |

### Server-Only Features (Session 15+)

These features work in server mode via RESP protocol but are not available in library mode:

| Feature | Status |
|---------|--------|
| BLPOP/BRPOP/XREAD BLOCK | ✅ Implemented (Session 15) - Blocking list/stream reads with timeout |
| Pub/Sub (SUBSCRIBE/PUBLISH) | ✅ Implemented (Session 15) - Push notifications to subscribers |

## Non-Goals

- Matching Redis performance (good enough for 90% of use cases)
- Clustering/sharding
- Memory-first storage (we're disk-first)

## Upcoming Features

See [ROADMAP.md](./ROADMAP.md) for detailed roadmap including:
- **Session 17**: History tracking & time-travel queries (three-tier opt-in, configurable retention)
- **V3+**: Active expiration, full-text search, replication, history replay

## License

MIT
