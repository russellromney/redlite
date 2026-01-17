---
title: Rust (Core)
description: Redlite core library in Rust
---

Redlite is written in Rust. You can use it directly as a Rust crate.

## Installation

Add to your `Cargo.toml`:

```toml
[dependencies]
redlite = "0.1"
```

## Quick Start

```rust
use redlite::Db;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Open in-memory database
    let db = Db::open_memory()?;

    // Or file-based
    let db = Db::open("/path/to/db.db")?;

    // String operations
    db.set("key", b"value")?;
    let val = db.get("key")?;
    println!("{}", String::from_utf8_lossy(&val.unwrap()));

    // Hash operations
    db.hset("user:1", "name", b"Alice")?;
    db.hset("user:1", "age", b"30")?;
    let user = db.hgetall("user:1")?;

    // List operations
    db.lpush("queue", &[b"job1", b"job2"])?;
    let job = db.rpop("queue")?;

    // Set operations
    db.sadd("tags", &[b"redis", b"sqlite"])?;
    let members = db.smembers("tags")?;

    // Sorted sets
    db.zadd("scores", &[(100.0, b"player1"), (85.0, b"player2")])?;
    let top = db.zrevrange("scores", 0, 9)?;

    Ok(())
}
```

## Features

Enable optional features in `Cargo.toml`:

```toml
[dependencies]
redlite = { version = "0.1", features = ["server", "fts", "vector"] }
```

- `server` - TCP server mode (Redis protocol)
- `fts` - Full-text search
- `vector` - Vector similarity search
- `geo` - Geospatial operations

## API Overview

**Strings**: `set`, `get`, `incr`, `decr`, `append`, `mget`, `mset`

**Keys**: `del`, `exists`, `key_type`, `ttl`, `expire`, `keys`

**Hashes**: `hset`, `hget`, `hdel`, `hgetall`, `hmget`

**Lists**: `lpush`, `rpush`, `lpop`, `rpop`, `llen`, `lrange`

**Sets**: `sadd`, `srem`, `smembers`, `sismember`, `scard`

**Sorted Sets**: `zadd`, `zrem`, `zscore`, `zrange`, `zrevrange`

## Testing

```bash
cd crates/redlite
cargo test
```

## Links

- [Crate Documentation](https://docs.rs/redlite)
- [Source Code](https://github.com/russellromney/redlite/tree/main/crates/redlite)
