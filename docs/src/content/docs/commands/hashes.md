---
title: Hashes
description: Hash commands in Redlite
---

Hash commands for storing field-value pairs within a single key.

## Commands

| Command | Syntax | Description |
|---------|--------|-------------|
| HSET | `HSET key field value [field value ...]` | Set field(s) |
| HGET | `HGET key field` | Get field value |
| HMGET | `HMGET key field [field ...]` | Get multiple field values |
| HGETALL | `HGETALL key` | Get all fields and values |
| HDEL | `HDEL key field [field ...]` | Delete field(s) |
| HEXISTS | `HEXISTS key field` | Check if field exists |
| HKEYS | `HKEYS key` | Get all field names |
| HVALS | `HVALS key` | Get all values |
| HLEN | `HLEN key` | Count fields |
| HINCRBY | `HINCRBY key field increment` | Increment field by integer |
| HINCRBYFLOAT | `HINCRBYFLOAT key field increment` | Increment field by float |
| HSETNX | `HSETNX key field value` | Set field only if not exists |

## Examples

### Basic Operations

```bash
# Set single field
127.0.0.1:6379> HSET user:1 name "Alice"
(integer) 1

# Set multiple fields
127.0.0.1:6379> HSET user:1 email "alice@example.com" age "30"
(integer) 2

# Get single field
127.0.0.1:6379> HGET user:1 name
"Alice"

# Get multiple fields
127.0.0.1:6379> HMGET user:1 name email
1) "Alice"
2) "alice@example.com"

# Get all fields and values
127.0.0.1:6379> HGETALL user:1
1) "name"
2) "Alice"
3) "email"
4) "alice@example.com"
5) "age"
6) "30"
```

### Field Management

```bash
# Check if field exists
127.0.0.1:6379> HEXISTS user:1 name
(integer) 1
127.0.0.1:6379> HEXISTS user:1 phone
(integer) 0

# Get all field names
127.0.0.1:6379> HKEYS user:1
1) "name"
2) "email"
3) "age"

# Get all values
127.0.0.1:6379> HVALS user:1
1) "Alice"
2) "alice@example.com"
3) "30"

# Count fields
127.0.0.1:6379> HLEN user:1
(integer) 3

# Delete field
127.0.0.1:6379> HDEL user:1 age
(integer) 1
```

### Counters in Hashes

```bash
# Increment integer field
127.0.0.1:6379> HSET stats:page views 0
(integer) 1
127.0.0.1:6379> HINCRBY stats:page views 1
(integer) 1
127.0.0.1:6379> HINCRBY stats:page views 10
(integer) 11

# Increment float field
127.0.0.1:6379> HSET product:1 price 9.99
(integer) 1
127.0.0.1:6379> HINCRBYFLOAT product:1 price 0.50
"10.49"
```

### Conditional Set

```bash
# Only set if field doesn't exist
127.0.0.1:6379> HSETNX user:1 email "alice@example.com"
(integer) 0  # Field exists, not set
127.0.0.1:6379> HSETNX user:1 phone "555-1234"
(integer) 1  # Field created
```

## Library Mode (Rust)

```rust
use redlite::Db;

let db = Db::open("mydata.db")?;

// Set fields
db.hset("user:1", &[("name", b"Alice"), ("email", b"alice@example.com")])?;

// Get single field
let name = db.hget("user:1", "name")?;  // Some(b"Alice".to_vec())

// Get multiple fields
let values = db.hmget("user:1", &["name", "email"])?;

// Get all fields and values
let all = db.hgetall("user:1")?;  // Vec<(String, Vec<u8>)>

// Check existence
let exists = db.hexists("user:1", "name")?;

// Increment
db.hincrby("stats:page", "views", 1)?;

// Delete field
db.hdel("user:1", &["age"])?;
```

## Use Cases

### User Profiles

```bash
HSET user:1000 username "alice" email "alice@example.com" created_at "2024-01-01"
HGET user:1000 username
HGETALL user:1000
```

### Configuration Storage

```bash
HSET config:app debug "false" timeout "30" max_connections "100"
HGET config:app timeout
```

### Object Storage

```bash
HSET product:123 name "Widget" price "29.99" stock "50"
HINCRBY product:123 stock -1  # Decrement stock on sale
```
