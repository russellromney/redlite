---
title: Lua SDK
description: Redlite SDK for Lua
---

Lua SDK with FFI bindings for LuaJIT.

## Installation

```bash
luarocks install redlite
```

Requires LuaJIT (for FFI support).

## Quick Start

```lua
local redlite = require("redlite")

-- Open in-memory database
local db = redlite.open(":memory:")

-- Or file-based
local db = redlite.open("/path/to/db.db")

-- String operations
db:set("key", "value")
local val = db:get("key")
print(val)  -- "value"

-- Increment
db:incr("counter")
db:incrby("counter", 5)

-- Hash operations
db:hset("user:1", "name", "Alice")
db:hset("user:1", "age", "30")
local user = db:hgetall("user:1")

-- List operations
db:lpush("queue", "job1", "job2")
local job = db:rpop("queue")

-- Set operations
db:sadd("tags", "redis", "sqlite")
local members = db:smembers("tags")

-- Sorted sets
db:zadd("scores", 100, "player1")
db:zadd("scores", 85, "player2")
local top = db:zrevrange("scores", 0, 9)

db:close()
```

## API Overview

**Strings**: `set`, `get`, `incr`, `decr`, `append`, `mget`, `mset`

**Keys**: `del`, `exists`, `type`, `ttl`, `expire`, `keys`

**Hashes**: `hset`, `hget`, `hdel`, `hgetall`, `hmget`

**Lists**: `lpush`, `rpush`, `lpop`, `rpop`, `llen`, `lrange`

**Sets**: `sadd`, `srem`, `smembers`, `sismember`, `scard`

**Sorted Sets**: `zadd`, `zrem`, `zscore`, `zrange`, `zrevrange`

## Testing

```bash
cd sdks/redlite-lua
make test  # Runs busted tests
```

## Links

- [Full README](https://github.com/russellromney/redlite/tree/main/sdks/redlite-lua)
- [Source Code](https://github.com/russellromney/redlite/tree/main/sdks/redlite-lua)
