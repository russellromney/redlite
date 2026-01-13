---
title: Sorted Sets
description: Sorted set commands in Redlite
---

Sorted set commands for storing unique elements with scores, ordered by score.

## Commands

| Command | Syntax | Description |
|---------|--------|-------------|
| ZADD | `ZADD key score member [score member ...]` | Add members with scores |
| ZREM | `ZREM key member [member ...]` | Remove members |
| ZSCORE | `ZSCORE key member` | Get member's score |
| ZRANK | `ZRANK key member` | Get rank (ascending) |
| ZREVRANK | `ZREVRANK key member` | Get rank (descending) |
| ZCARD | `ZCARD key` | Count members |
| ZCOUNT | `ZCOUNT key min max` | Count by score range |
| ZRANGE | `ZRANGE key start stop [WITHSCORES]` | Get by rank range |
| ZREVRANGE | `ZREVRANGE key start stop [WITHSCORES]` | Get by rank (descending) |
| ZRANGEBYSCORE | `ZRANGEBYSCORE key min max [WITHSCORES] [LIMIT offset count]` | Get by score range |
| ZINCRBY | `ZINCRBY key increment member` | Increment member's score |
| ZREMRANGEBYRANK | `ZREMRANGEBYRANK key start stop` | Remove by rank range |
| ZREMRANGEBYSCORE | `ZREMRANGEBYSCORE key min max` | Remove by score range |

## Examples

### Basic Operations

```bash
# Add members with scores
127.0.0.1:6379> ZADD leaderboard 100 "alice" 85 "bob" 92 "charlie"
(integer) 3

# Get score
127.0.0.1:6379> ZSCORE leaderboard "alice"
"100"

# Get rank (0-indexed, ascending by score)
127.0.0.1:6379> ZRANK leaderboard "bob"
(integer) 0  # Lowest score

127.0.0.1:6379> ZREVRANK leaderboard "alice"
(integer) 0  # Highest score
```

### Range Queries

```bash
# Get all by rank (ascending)
127.0.0.1:6379> ZRANGE leaderboard 0 -1
1) "bob"
2) "charlie"
3) "alice"

# With scores
127.0.0.1:6379> ZRANGE leaderboard 0 -1 WITHSCORES
1) "bob"
2) "85"
3) "charlie"
4) "92"
5) "alice"
6) "100"

# Top 3 (descending)
127.0.0.1:6379> ZREVRANGE leaderboard 0 2 WITHSCORES
1) "alice"
2) "100"
3) "charlie"
4) "92"
5) "bob"
6) "85"
```

### Score Range Queries

```bash
# Get by score range
127.0.0.1:6379> ZRANGEBYSCORE leaderboard 85 95
1) "bob"
2) "charlie"

# With limit (pagination)
127.0.0.1:6379> ZRANGEBYSCORE leaderboard -inf +inf LIMIT 0 2
1) "bob"
2) "charlie"

# Count in range
127.0.0.1:6379> ZCOUNT leaderboard 90 100
(integer) 2  # charlie and alice
```

### Update Scores

```bash
# Increment score
127.0.0.1:6379> ZINCRBY leaderboard 5 "bob"
"90"  # New score

# Use negative increment to decrease
127.0.0.1:6379> ZINCRBY leaderboard -10 "alice"
"90"
```

### Remove Members

```bash
# Remove specific members
127.0.0.1:6379> ZREM leaderboard "bob"
(integer) 1

# Remove by rank range (lowest 2)
127.0.0.1:6379> ZREMRANGEBYRANK leaderboard 0 1
(integer) 2

# Remove by score range
127.0.0.1:6379> ZREMRANGEBYSCORE leaderboard 0 50
(integer) 1
```

## Library Mode (Rust)

```rust
use redlite::Db;

let db = Db::open("mydata.db")?;

// Add members
db.zadd("leaderboard", &[(100.0, b"alice"), (85.0, b"bob")])?;

// Get score
let score = db.zscore("leaderboard", b"alice")?;  // Option<f64>

// Get rank
let rank = db.zrank("leaderboard", b"alice")?;  // Option<i64>

// Range queries
let top10 = db.zrevrange("leaderboard", 0, 9, true)?;  // With scores

// Score range
let in_range = db.zrangebyscore("leaderboard", 80.0, 100.0)?;

// Increment
db.zincrby("leaderboard", 5.0, b"bob")?;

// Count in range
let count = db.zcount("leaderboard", 80.0, 100.0)?;
```

## Use Cases

### Leaderboards

```bash
ZADD game:leaderboard 1500 "player:1" 1200 "player:2" 1800 "player:3"

# Top 10 players
ZREVRANGE game:leaderboard 0 9 WITHSCORES

# Player's rank
ZREVRANK game:leaderboard "player:1"

# Update score after game
ZINCRBY game:leaderboard 50 "player:1"
```

### Time-Based Feeds

```bash
# Score = Unix timestamp
ZADD feed:user:1 1704067200 "post:100"
ZADD feed:user:1 1704153600 "post:101"

# Get recent posts
ZREVRANGE feed:user:1 0 9

# Get posts from last hour
ZRANGEBYSCORE feed:user:1 (now-3600) (now)
```

### Priority Queues

```bash
# Score = priority (lower = higher priority)
ZADD tasks 1 "urgent:task1"
ZADD tasks 5 "normal:task2"
ZADD tasks 10 "low:task3"

# Get highest priority task
ZRANGE tasks 0 0
ZREM tasks "urgent:task1"  # Process and remove
```

### Rate Limiting

```bash
# Score = timestamp of request
ZADD ratelimit:user:1 1704067200000 "req:1"
ZADD ratelimit:user:1 1704067201000 "req:2"

# Remove old entries (older than 1 minute)
ZREMRANGEBYSCORE ratelimit:user:1 0 (now-60000)

# Check request count
ZCARD ratelimit:user:1
```
