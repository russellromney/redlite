---
title: Embedded (Library)
description: Using Redlite as an embedded library in Rust applications
---

Redlite can be embedded directly in applications via the Rust library API. This mode uses FFI for language bindings and eliminates network overhead.

## Opening a Database

```rust
use redlite::Db;

// Persistent storage (creates file if doesn't exist)
let db = Db::open("mydata.db")?;

// In-memory storage
let db = Db::open_memory()?;
```

## String Operations

### SET and GET

```rust
use std::time::Duration;

// Basic set (no expiration)
db.set("user:1:name", b"Alice", None)?;

// Get returns Option<Vec<u8>>
let name = db.get("user:1:name")?;
assert_eq!(name, Some(b"Alice".to_vec()));

// Non-existent keys return None
let missing = db.get("nonexistent")?;
assert_eq!(missing, None);
```

### With TTL (Time-To-Live)

```rust
use std::time::Duration;

// Expires in 60 seconds
db.set("session:abc", b"user_data", Some(Duration::from_secs(60)))?;

// Expires in 100 milliseconds
db.set("temp", b"quick", Some(Duration::from_millis(100)))?;
```

### Conditional SET with Options

```rust
use redlite::SetOptions;

// NX - Only set if key does NOT exist
let opts = SetOptions::new().nx();
db.set_opts("counter", b"0", opts)?;  // Sets value
db.set_opts("counter", b"1", opts)?;  // Does nothing, key exists

// XX - Only set if key DOES exist
let opts = SetOptions::new().xx();
db.set_opts("counter", b"10", opts)?;  // Updates existing key
db.set_opts("new_key", b"0", opts)?;   // Does nothing, key doesn't exist

// Combine with TTL
let opts = SetOptions::new()
    .nx()
    .ex(Duration::from_secs(300));
db.set_opts("lock:resource", b"owner_id", opts)?;
```

## Deleting Keys

```rust
// Delete single key
db.del(&["user:1:name"])?;

// Delete multiple keys at once
db.del(&["key1", "key2", "key3"])?;
```

## Multiple Databases

Like Redis, Redlite supports multiple logical databases (0-15 by default):

```rust
// Default is database 0
db.set("key", b"in db 0", None)?;

// Switch to database 1
db.select(1)?;
db.set("key", b"in db 1", None)?;

// Keys are isolated between databases
db.select(0)?;
assert_eq!(db.get("key")?, Some(b"in db 0".to_vec()));
```

## Error Handling

```rust
use redlite::{Db, KvError};

fn example() -> redlite::Result<()> {
    let db = Db::open("mydata.db")?;

    match db.get("key") {
        Ok(Some(value)) => println!("Found: {:?}", value),
        Ok(None) => println!("Key not found"),
        Err(e) => eprintln!("Error: {}", e),
    }

    Ok(())
}
```

## Thread Safety

The `Db` struct is thread-safe and can be shared across threads using `Arc`:

```rust
use std::sync::Arc;
use std::thread;

let db = Arc::new(Db::open("mydata.db")?);

let handles: Vec<_> = (0..4).map(|i| {
    let db = Arc::clone(&db);
    thread::spawn(move || {
        db.set(&format!("key:{}", i), b"value", None).unwrap();
    })
}).collect();

for handle in handles {
    handle.join().unwrap();
}
```

## SQLite Storage Properties

Redlite's SQLite storage layer provides:

- **ACID transactions** - Atomicity, consistency, isolation, and durability guarantees
- **WAL mode** - Concurrent readers with single writer (SQLite's write-ahead logging)
- **Crash safety** - Transactions are atomic; partial writes are rolled back on crash
- **Single-file format** - Database state contained in one file plus WAL and SHM files
- **Cross-platform** - SQLite file format is portable across architectures
