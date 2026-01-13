---
title: Strings
description: String commands in Redlite
---

String commands for storing and manipulating text and binary data.

## Commands

| Command | Syntax | Description |
|---------|--------|-------------|
| GET | `GET key` | Get value of key |
| SET | `SET key value [EX s] [PX ms] [NX\|XX]` | Set value with optional expiration |
| MGET | `MGET key [key ...]` | Get values of multiple keys |
| MSET | `MSET key value [key value ...]` | Set multiple key-value pairs |
| INCR | `INCR key` | Increment integer value by 1 |
| INCRBY | `INCRBY key increment` | Increment by integer amount |
| INCRBYFLOAT | `INCRBYFLOAT key increment` | Increment by float amount |
| DECR | `DECR key` | Decrement integer value by 1 |
| DECRBY | `DECRBY key decrement` | Decrement by integer amount |
| APPEND | `APPEND key value` | Append to existing value |
| STRLEN | `STRLEN key` | Get string length |
| GETRANGE | `GETRANGE key start end` | Get substring |
| SETRANGE | `SETRANGE key offset value` | Overwrite part of string |
| SETNX | `SETNX key value` | Set only if not exists |
| SETEX | `SETEX key seconds value` | Set with expiration in seconds |
| PSETEX | `PSETEX key milliseconds value` | Set with expiration in milliseconds |

## Examples

### Basic SET/GET

```bash
127.0.0.1:6379> SET name "Alice"
OK
127.0.0.1:6379> GET name
"Alice"
```

### SET with Expiration

```bash
# Expires in 60 seconds
127.0.0.1:6379> SET session:abc "user_data" EX 60
OK

# Expires in 5000 milliseconds
127.0.0.1:6379> SET temp "quick" PX 5000
OK
```

### Conditional SET

```bash
# Only set if key doesn't exist (NX)
127.0.0.1:6379> SET lock:resource "owner_id" NX
OK
127.0.0.1:6379> SET lock:resource "other_owner" NX
(nil)  # Key already exists

# Only set if key exists (XX)
127.0.0.1:6379> SET counter "10" XX
OK  # Updates existing key
```

### Counters

```bash
127.0.0.1:6379> SET views 0
OK
127.0.0.1:6379> INCR views
(integer) 1
127.0.0.1:6379> INCRBY views 10
(integer) 11
127.0.0.1:6379> DECR views
(integer) 10
```

### Multiple Keys

```bash
127.0.0.1:6379> MSET key1 "value1" key2 "value2" key3 "value3"
OK
127.0.0.1:6379> MGET key1 key2 key3
1) "value1"
2) "value2"
3) "value3"
```

## Library Mode (Rust)

```rust
use redlite::{Db, SetOptions};
use std::time::Duration;

let db = Db::open("mydata.db")?;

// Basic SET/GET
db.set("name", b"Alice", None)?;
let name = db.get("name")?;  // Some(b"Alice".to_vec())

// With TTL
db.set("session", b"data", Some(Duration::from_secs(60)))?;

// Conditional SET
db.set_opts("lock", b"owner", SetOptions::new().nx())?;
db.set_opts("counter", b"10", SetOptions::new().xx())?;

// Counters
db.incr("views")?;
db.incrby("views", 10)?;
db.decr("views")?;
```
