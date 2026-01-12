# Project Structure

## Directory Layout

```
redlite/
├── Cargo.toml
├── README.md
├── src/
│   ├── main.rs               # CLI entry point
│   ├── lib.rs                # Public library API
│   ├── db.rs                 # Core Db struct and string/key commands
│   ├── error.rs              # KvError type
│   ├── types.rs              # KeyType, ZMember, SetOptions
│   ├── scan.rs               # ScanCursor, ScanResult
│   ├── commands/
│   │   ├── mod.rs
│   │   ├── strings.rs        # GET, SET, INCR, etc.
│   │   ├── keys.rs           # DEL, EXPIRE, TTL, etc.
│   │   ├── hashes.rs         # HGET, HSET, etc.
│   │   ├── lists.rs          # LPUSH, RPUSH, etc.
│   │   ├── sets.rs           # SADD, SMEMBERS, etc.
│   │   └── zsets.rs          # ZADD, ZRANGE, etc.
│   ├── resp/
│   │   ├── mod.rs
│   │   ├── reader.rs         # RESP parser
│   │   └── value.rs          # RespValue enum
│   └── server/
│       ├── mod.rs            # TCP server
│       ├── router.rs         # Command router
│       └── commands.rs       # Command implementations
└── schema.sql                # SQLite schema
```

## Cargo.toml

```toml
[package]
name = "redlite"
version = "0.1.0"
edition = "2021"
description = "SQLite-backed Redis-compatible KV store"
license = "MIT"
repository = "https://github.com/russellromney/redlite"

[lib]
name = "redlite"
path = "src/lib.rs"

[[bin]]
name = "redlite"
path = "src/main.rs"

[dependencies]
rusqlite = { version = "0.31", features = ["bundled"] }
tokio = { version = "1", features = ["full"] }
bytes = "1"
thiserror = "1"
tracing = "0.1"
tracing-subscriber = "0.3"
clap = { version = "4", features = ["derive"] }
anyhow = "1"

[dev-dependencies]
redis = "0.25"
tempfile = "3"
tokio-test = "0.4"

[profile.release]
lto = true
codegen-units = 1
```

## schema.sql

```sql
-- Core key metadata
CREATE TABLE IF NOT EXISTS keys (
    id INTEGER PRIMARY KEY,
    db INTEGER NOT NULL DEFAULT 0,
    key TEXT NOT NULL,
    type INTEGER NOT NULL,
    expire_at INTEGER,
    created_at INTEGER NOT NULL DEFAULT (unixepoch('now', 'subsec') * 1000),
    updated_at INTEGER NOT NULL DEFAULT (unixepoch('now', 'subsec') * 1000)
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_keys_db_key ON keys(db, key);
CREATE INDEX IF NOT EXISTS idx_keys_expire ON keys(expire_at) WHERE expire_at IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_keys_type ON keys(db, type);

-- Strings
CREATE TABLE IF NOT EXISTS strings (
    key_id INTEGER PRIMARY KEY REFERENCES keys(id) ON DELETE CASCADE,
    value BLOB NOT NULL
);

-- Hashes
CREATE TABLE IF NOT EXISTS hashes (
    key_id INTEGER NOT NULL REFERENCES keys(id) ON DELETE CASCADE,
    field TEXT NOT NULL,
    value BLOB NOT NULL,
    PRIMARY KEY (key_id, field)
);

-- Lists (integer positions with gap-based insertion)
CREATE TABLE IF NOT EXISTS lists (
    key_id INTEGER NOT NULL REFERENCES keys(id) ON DELETE CASCADE,
    pos INTEGER NOT NULL,
    value BLOB NOT NULL,
    PRIMARY KEY (key_id, pos)
);

CREATE INDEX IF NOT EXISTS idx_lists_key_pos ON lists(key_id, pos);

-- Sets
CREATE TABLE IF NOT EXISTS sets (
    key_id INTEGER NOT NULL REFERENCES keys(id) ON DELETE CASCADE,
    member BLOB NOT NULL,
    PRIMARY KEY (key_id, member)
);

-- Sorted Sets
CREATE TABLE IF NOT EXISTS zsets (
    key_id INTEGER NOT NULL REFERENCES keys(id) ON DELETE CASCADE,
    member BLOB NOT NULL,
    score REAL NOT NULL,
    PRIMARY KEY (key_id, member)
);

CREATE INDEX IF NOT EXISTS idx_zsets_score ON zsets(key_id, score, member);
```

## src/lib.rs

```rust
//! Redlite - SQLite-backed Redis-compatible KV store

pub mod db;
pub mod error;
pub mod types;
pub mod scan;

mod commands;
mod resp;
mod server;

pub use db::Db;
pub use error::{KvError, Result};
pub use types::{KeyType, ZMember};
pub use scan::{ScanCursor, ScanResult};
pub use server::Server;
pub use resp::RespValue;
```

## Usage Examples

### As a server

```bash
# Build and run
cargo build --release
./target/release/redlite --db=data.db --addr=127.0.0.1:6379

# Connect with redis-cli
redis-cli -p 6379
127.0.0.1:6379> PING
PONG
127.0.0.1:6379> SET greeting "Hello, World!"
OK
127.0.0.1:6379> GET greeting
"Hello, World!"
```

### As a library

```rust
use redlite::{Db, ZMember};
use std::time::Duration;

fn main() -> anyhow::Result<()> {
    // Open database
    let db = Db::open("myapp.db")?;

    // Strings
    db.set("name", b"Alice", None)?;
    db.set("session", b"token123", Some(Duration::from_secs(3600)))?;

    let name = db.get("name")?;
    println!("Name: {:?}", name);

    // Increment
    db.set("counter", b"0", None)?;
    let count = db.incr("counter")?;
    println!("Counter: {}", count);

    // Hashes
    db.hset("user:1", &[
        ("name", b"Bob".as_slice()),
        ("email", b"bob@example.com".as_slice()),
    ])?;
    let user = db.hgetall("user:1")?;
    println!("User: {:?}", user);

    // Lists
    db.lpush("queue", &[b"job1", b"job2"])?;
    let jobs = db.lrange("queue", 0, -1)?;
    println!("Jobs: {:?}", jobs);

    // Sets
    db.sadd("tags", &[b"rust", b"sqlite", b"redis"])?;
    let has_rust = db.sismember("tags", b"rust")?;
    println!("Has rust tag: {}", has_rust);

    // Sorted Sets
    db.zadd("leaderboard", &[
        ZMember::new(100.0, "alice"),
        ZMember::new(200.0, "bob"),
        ZMember::new(150.0, "charlie"),
    ])?;
    let top = db.zrange("leaderboard", 0, 2, true)?;
    for m in top {
        println!("{}: {}", String::from_utf8_lossy(&m.member), m.score);
    }

    // Key expiration
    db.expire("session", Duration::from_secs(60))?;
    let ttl = db.ttl("session")?;
    println!("Session TTL: {}s", ttl);

    // Multiple databases
    db.select(1)?;
    db.set("in_db_1", b"value", None)?;

    db.select(0)?;
    assert!(db.get("in_db_1")?.is_none()); // Not in db 0

    // Custom commands
    db.vacuum()?;  // Clean up expired keys
    let info = db.keyinfo("name")?;
    println!("Key info: {:?}", info);

    Ok(())
}
```

### Integration test

```rust
use redlite::Db;
use std::time::Duration;

#[test]
fn test_string_operations() {
    let db = Db::open_memory().unwrap();

    // SET/GET
    db.set("key", b"value", None).unwrap();
    assert_eq!(db.get("key").unwrap(), Some(b"value".to_vec()));

    // INCR
    db.set("counter", b"10", None).unwrap();
    assert_eq!(db.incr("counter").unwrap(), 11);

    // Expiration
    db.set("temp", b"data", Some(Duration::from_millis(100))).unwrap();
    assert!(db.get("temp").unwrap().is_some());
    std::thread::sleep(Duration::from_millis(150));
    assert!(db.get("temp").unwrap().is_none());
}

#[test]
fn test_hash_operations() {
    let db = Db::open_memory().unwrap();

    db.hset("hash", &[("f1", b"v1"), ("f2", b"v2")]).unwrap();
    assert_eq!(db.hget("hash", "f1").unwrap(), Some(b"v1".to_vec()));
    assert_eq!(db.hlen("hash").unwrap(), 2);

    let all = db.hgetall("hash").unwrap();
    assert_eq!(all.len(), 2);
}

#[test]
fn test_list_operations() {
    let db = Db::open_memory().unwrap();

    db.rpush("list", &[b"a", b"b", b"c"]).unwrap();
    assert_eq!(db.llen("list").unwrap(), 3);

    let range = db.lrange("list", 0, -1).unwrap();
    assert_eq!(range, vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()]);

    assert_eq!(db.lpop("list").unwrap(), Some(b"a".to_vec()));
    assert_eq!(db.rpop("list").unwrap(), Some(b"c".to_vec()));
}

#[test]
fn test_set_operations() {
    let db = Db::open_memory().unwrap();

    db.sadd("set", &[b"a", b"b", b"c"]).unwrap();
    assert_eq!(db.scard("set").unwrap(), 3);
    assert!(db.sismember("set", b"a").unwrap());
    assert!(!db.sismember("set", b"d").unwrap());
}

#[test]
fn test_sorted_set_operations() {
    use redlite::ZMember;

    let db = Db::open_memory().unwrap();

    db.zadd("zset", &[
        ZMember::new(1.0, "a"),
        ZMember::new(2.0, "b"),
        ZMember::new(3.0, "c"),
    ]).unwrap();

    assert_eq!(db.zcard("zset").unwrap(), 3);
    assert_eq!(db.zscore("zset", b"b").unwrap(), Some(2.0));
    assert_eq!(db.zrank("zset", b"b").unwrap(), Some(1));

    let range = db.zrange("zset", 0, -1, false).unwrap();
    assert_eq!(range.len(), 3);
    assert_eq!(range[0].member, b"a");
}
```

## Docker

```dockerfile
FROM rust:1.75 as builder

WORKDIR /app
COPY . .
RUN cargo build --release

FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*

COPY --from=builder /app/target/release/redlite /usr/local/bin/

EXPOSE 6379
VOLUME /data

ENTRYPOINT ["redlite"]
CMD ["--db=/data/redlite.db", "--addr=0.0.0.0:6379"]
```

```bash
# Build and run
docker build -t redlite .
docker run -p 6379:6379 -v ./data:/data redlite
```

## Performance Notes

1. **WAL mode** - Enabled by default for concurrent reads
2. **Single writer** - Connection protected by mutex; one writer at a time
3. **Batch operations** - MSET/MGET use single transaction
4. **Index optimization** - Schema designed for common access patterns
5. **Lazy expiration** - Expired keys cleaned up on access or via VACUUM

Not trying to beat Redis. Target is "good enough for 90% of use cases, simpler to deploy and operate."

## License

MIT
