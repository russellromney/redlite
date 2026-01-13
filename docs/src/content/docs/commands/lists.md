---
title: Lists
description: List commands in Redlite
---

List commands for storing ordered sequences of elements.

## Commands

| Command | Syntax | Description |
|---------|--------|-------------|
| LPUSH | `LPUSH key element [element ...]` | Prepend elements |
| RPUSH | `RPUSH key element [element ...]` | Append elements |
| LPOP | `LPOP key [count]` | Pop from front |
| RPOP | `RPOP key [count]` | Pop from back |
| LLEN | `LLEN key` | Get list length |
| LRANGE | `LRANGE key start stop` | Get range of elements |
| LINDEX | `LINDEX key index` | Get element by index |
| LSET | `LSET key index element` | Set element at index |
| LTRIM | `LTRIM key start stop` | Trim list to range |
| LREM | `LREM key count element` | Remove elements by value |
| LINSERT | `LINSERT key BEFORE\|AFTER pivot element` | Insert before/after element |

### Blocking Commands (Server Mode Only)

| Command | Syntax | Description |
|---------|--------|-------------|
| BLPOP | `BLPOP key [key ...] timeout` | Blocking pop from front |
| BRPOP | `BRPOP key [key ...] timeout` | Blocking pop from back |

## Examples

### Basic Operations

```bash
# Push elements
127.0.0.1:6379> RPUSH mylist "one" "two" "three"
(integer) 3
127.0.0.1:6379> LPUSH mylist "zero"
(integer) 4

# Get all elements
127.0.0.1:6379> LRANGE mylist 0 -1
1) "zero"
2) "one"
3) "two"
4) "three"

# Pop elements
127.0.0.1:6379> LPOP mylist
"zero"
127.0.0.1:6379> RPOP mylist
"three"
```

### Index Operations

```bash
# Get by index (0-based)
127.0.0.1:6379> LINDEX mylist 0
"one"
127.0.0.1:6379> LINDEX mylist -1  # Last element
"two"

# Set by index
127.0.0.1:6379> LSET mylist 0 "first"
OK
```

### Range Operations

```bash
# Get subset
127.0.0.1:6379> LRANGE mylist 0 1
1) "first"
2) "two"

# Trim list (keep only elements 0-2)
127.0.0.1:6379> LTRIM mylist 0 2
OK
```

### Remove Elements

```bash
127.0.0.1:6379> RPUSH mylist "a" "b" "a" "c" "a"
(integer) 5

# Remove 2 occurrences of "a" from head
127.0.0.1:6379> LREM mylist 2 "a"
(integer) 2

# Remove all occurrences (count = 0)
127.0.0.1:6379> LREM mylist 0 "a"
(integer) 1
```

### Insert Elements

```bash
127.0.0.1:6379> RPUSH mylist "Hello" "World"
(integer) 2
127.0.0.1:6379> LINSERT mylist BEFORE "World" "Beautiful"
(integer) 3
127.0.0.1:6379> LRANGE mylist 0 -1
1) "Hello"
2) "Beautiful"
3) "World"
```

### Blocking Operations (Server Mode)

```bash
# Wait up to 5 seconds for an element
127.0.0.1:6379> BLPOP myqueue 5
# Returns nil after timeout, or element if pushed by another client

# In another terminal:
127.0.0.1:6379> RPUSH myqueue "message"
```

## Library Mode (Rust)

```rust
use redlite::Db;

let db = Db::open("mydata.db")?;

// Push elements
db.rpush("mylist", &[b"one", b"two", b"three"])?;
db.lpush("mylist", &[b"zero"])?;

// Get range
let elements = db.lrange("mylist", 0, -1)?;  // Vec<Vec<u8>>

// Pop elements
let first = db.lpop("mylist", 1)?;  // Vec<Vec<u8>>
let last = db.rpop("mylist", 1)?;

// Get by index
let elem = db.lindex("mylist", 0)?;  // Option<Vec<u8>>

// List length
let len = db.llen("mylist")?;

// Remove elements
db.lrem("mylist", 2, b"a")?;

// Insert
db.linsert("mylist", "BEFORE", b"World", b"Beautiful")?;
```

## Use Cases

### Message Queues

```bash
# Producer
RPUSH tasks '{"task": "process_image", "id": 123}'

# Consumer (blocking)
BLPOP tasks 0  # Wait indefinitely
```

### Recent Items

```bash
# Add to front, keep last 100
LPUSH recent:user:1 "viewed:product:456"
LTRIM recent:user:1 0 99
```

### Logs

```bash
RPUSH log:app "2024-01-01 12:00:00 INFO: Started"
RPUSH log:app "2024-01-01 12:00:01 DEBUG: Processing"
LRANGE log:app -10 -1  # Last 10 entries
```
