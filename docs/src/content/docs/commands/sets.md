---
title: Sets
description: Set commands in Redlite
---

Set commands for storing unordered collections of unique elements.

## Commands

| Command | Syntax | Description |
|---------|--------|-------------|
| SADD | `SADD key member [member ...]` | Add members |
| SREM | `SREM key member [member ...]` | Remove members |
| SMEMBERS | `SMEMBERS key` | Get all members |
| SISMEMBER | `SISMEMBER key member` | Check membership |
| SCARD | `SCARD key` | Count members |
| SPOP | `SPOP key [count]` | Remove random members |
| SRANDMEMBER | `SRANDMEMBER key [count]` | Get random members |
| SDIFF | `SDIFF key [key ...]` | Difference of sets |
| SINTER | `SINTER key [key ...]` | Intersection of sets |
| SUNION | `SUNION key [key ...]` | Union of sets |
| SMOVE | `SMOVE source destination member` | Move member between sets |
| SDIFFSTORE | `SDIFFSTORE destination key [key ...]` | Store difference |
| SINTERSTORE | `SINTERSTORE destination key [key ...]` | Store intersection |
| SUNIONSTORE | `SUNIONSTORE destination key [key ...]` | Store union |

## Examples

### Basic Operations

```bash
# Add members
127.0.0.1:6379> SADD myset "apple" "banana" "cherry"
(integer) 3

# Check membership
127.0.0.1:6379> SISMEMBER myset "apple"
(integer) 1
127.0.0.1:6379> SISMEMBER myset "grape"
(integer) 0

# Get all members
127.0.0.1:6379> SMEMBERS myset
1) "apple"
2) "banana"
3) "cherry"

# Count members
127.0.0.1:6379> SCARD myset
(integer) 3
```

### Remove Members

```bash
# Remove specific members
127.0.0.1:6379> SREM myset "banana"
(integer) 1

# Pop random member(s)
127.0.0.1:6379> SPOP myset
"cherry"  # Random member removed

127.0.0.1:6379> SPOP myset 2  # Pop 2 random members
1) "apple"
2) "grape"
```

### Random Selection

```bash
127.0.0.1:6379> SADD deck "A" "K" "Q" "J" "10"
(integer) 5

# Get random member (doesn't remove)
127.0.0.1:6379> SRANDMEMBER deck
"K"

# Get multiple random members
127.0.0.1:6379> SRANDMEMBER deck 3
1) "A"
2) "Q"
3) "10"

# Negative count allows repeats
127.0.0.1:6379> SRANDMEMBER deck -5
1) "K"
2) "K"
3) "A"
4) "J"
5) "Q"
```

### Set Operations

```bash
127.0.0.1:6379> SADD set1 "a" "b" "c"
(integer) 3
127.0.0.1:6379> SADD set2 "b" "c" "d"
(integer) 3

# Difference (in set1 but not set2)
127.0.0.1:6379> SDIFF set1 set2
1) "a"

# Intersection (in both sets)
127.0.0.1:6379> SINTER set1 set2
1) "b"
2) "c"

# Union (in either set)
127.0.0.1:6379> SUNION set1 set2
1) "a"
2) "b"
3) "c"
4) "d"
```

### Store Operations

```bash
# Store results in new set
127.0.0.1:6379> SDIFFSTORE diff_result set1 set2
(integer) 1  # Number of elements in result

127.0.0.1:6379> SINTERSTORE inter_result set1 set2
(integer) 2

127.0.0.1:6379> SUNIONSTORE union_result set1 set2
(integer) 4
```

### Move Between Sets

```bash
127.0.0.1:6379> SADD active "user:1" "user:2"
(integer) 2
127.0.0.1:6379> SADD inactive
(integer) 0

# Move user to inactive
127.0.0.1:6379> SMOVE active inactive "user:1"
(integer) 1
```

## Library Mode (Rust)

```rust
use redlite::Db;

let db = Db::open("mydata.db")?;

// Add members
db.sadd("myset", &[b"apple", b"banana", b"cherry"])?;

// Check membership
let is_member = db.sismember("myset", b"apple")?;  // bool

// Get all members
let members = db.smembers("myset")?;  // Vec<Vec<u8>>

// Count
let count = db.scard("myset")?;

// Set operations
let diff = db.sdiff(&["set1", "set2"])?;
let inter = db.sinter(&["set1", "set2"])?;
let union = db.sunion(&["set1", "set2"])?;

// Store operations
db.sdiffstore("result", &["set1", "set2"])?;
db.sinterstore("result", &["set1", "set2"])?;
db.sunionstore("result", &["set1", "set2"])?;

// Move member
db.smove("source", "destination", b"member")?;
```

## Use Cases

### Tags

```bash
SADD article:123:tags "rust" "database" "redis"
SMEMBERS article:123:tags
SISMEMBER article:123:tags "rust"
```

### Unique Visitors

```bash
SADD visitors:2024-01-15 "user:100" "user:200" "user:100"  # Duplicates ignored
SCARD visitors:2024-01-15  # Unique visitor count
```

### Friends/Followers

```bash
SADD user:1:friends "user:2" "user:3" "user:4"
SADD user:2:friends "user:1" "user:3" "user:5"

# Mutual friends
SINTER user:1:friends user:2:friends
```

### Online Users

```bash
SADD online "user:1" "user:2"
SMEMBERS online
SREM online "user:1"  # User goes offline
```
