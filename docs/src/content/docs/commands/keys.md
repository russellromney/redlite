---
title: Keys
description: Key management commands in Redlite
---

Commands for managing keys, expiration, and key metadata.

## Commands

| Command | Syntax | Description |
|---------|--------|-------------|
| DEL | `DEL key [key ...]` | Delete one or more keys |
| EXISTS | `EXISTS key [key ...]` | Check if keys exist |
| TYPE | `TYPE key` | Get key type |
| EXPIRE | `EXPIRE key seconds` | Set TTL in seconds |
| EXPIREAT | `EXPIREAT key timestamp` | Set expiration as Unix timestamp |
| PEXPIRE | `PEXPIRE key milliseconds` | Set TTL in milliseconds |
| PEXPIREAT | `PEXPIREAT key timestamp` | Set expiration as Unix milliseconds |
| TTL | `TTL key` | Get remaining TTL in seconds |
| PTTL | `PTTL key` | Get remaining TTL in milliseconds |
| PERSIST | `PERSIST key` | Remove expiration |
| KEYS | `KEYS pattern` | Find keys by glob pattern |
| SCAN | `SCAN cursor [MATCH pattern] [COUNT count]` | Iterate keys |
| DBSIZE | `DBSIZE` | Get key count in current database |
| FLUSHDB | `FLUSHDB` | Delete all keys in current database |

## Examples

### Delete Keys

```bash
127.0.0.1:6379> SET key1 "value1"
OK
127.0.0.1:6379> SET key2 "value2"
OK
127.0.0.1:6379> DEL key1 key2
(integer) 2
```

### Check Existence

```bash
127.0.0.1:6379> SET mykey "Hello"
OK
127.0.0.1:6379> EXISTS mykey
(integer) 1
127.0.0.1:6379> EXISTS nosuchkey
(integer) 0
127.0.0.1:6379> EXISTS key1 key2 key3
(integer) 2  # Returns count of existing keys
```

### Expiration

```bash
# Set expiration
127.0.0.1:6379> SET mykey "Hello"
OK
127.0.0.1:6379> EXPIRE mykey 60
(integer) 1

# Check remaining TTL
127.0.0.1:6379> TTL mykey
(integer) 58

# Remove expiration
127.0.0.1:6379> PERSIST mykey
(integer) 1
127.0.0.1:6379> TTL mykey
(integer) -1  # No expiration
```

### Find Keys

```bash
# Find all keys matching pattern
127.0.0.1:6379> KEYS user:*
1) "user:1"
2) "user:2"
3) "user:100"

# Find keys with single character wildcard
127.0.0.1:6379> KEYS user:?
1) "user:1"
2) "user:2"
```

### Scan Keys (Recommended for Large Datasets)

```bash
127.0.0.1:6379> SCAN 0 MATCH user:* COUNT 10
1) "15"  # Next cursor
2) 1) "user:1"
   2) "user:2"
   3) "user:3"

# Continue with returned cursor
127.0.0.1:6379> SCAN 15 MATCH user:* COUNT 10
1) "0"  # Cursor 0 = iteration complete
2) 1) "user:4"
   2) "user:5"
```

### Key Type

```bash
127.0.0.1:6379> SET mystring "hello"
OK
127.0.0.1:6379> LPUSH mylist "world"
(integer) 1
127.0.0.1:6379> TYPE mystring
string
127.0.0.1:6379> TYPE mylist
list
```

## Library Mode (Rust)

```rust
use redlite::Db;
use std::time::Duration;

let db = Db::open("mydata.db")?;

// Delete keys
db.del(&["key1", "key2"])?;

// Check existence
let exists = db.exists(&["mykey"])?;  // Returns count

// Set expiration
db.expire("mykey", Duration::from_secs(60))?;

// Get TTL
let ttl = db.ttl("mykey")?;  // Returns Option<i64>

// Find keys
let keys = db.keys("user:*")?;

// Get key type
let key_type = db.key_type("mykey")?;  // Returns KeyType enum
```
