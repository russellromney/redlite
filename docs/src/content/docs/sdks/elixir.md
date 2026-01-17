---
title: Elixir SDK
description: Redlite SDK for Elixir
---

Elixir SDK with NIF (Native Implemented Functions) bindings.

## Installation

Add to your `mix.exs`:

```elixir
def deps do
  [
    {:redlite, "~> 0.1.0"}
  ]
end
```

## Quick Start

```elixir
# Open in-memory database
{:ok, db} = Redlite.open(":memory:")

# Or file-based
{:ok, db} = Redlite.open("/path/to/db.db")

# String operations
:ok = Redlite.set(db, "key", "value")
{:ok, value} = Redlite.get(db, "key")

# Hash operations
Redlite.hset(db, "user:1", "name", "Alice")
Redlite.hset(db, "user:1", "age", "30")
{:ok, user} = Redlite.hgetall(db, "user:1")

# List operations
Redlite.lpush(db, "queue", ["job1", "job2"])
{:ok, job} = Redlite.rpop(db, "queue")

# Set operations
Redlite.sadd(db, "tags", ["redis", "sqlite"])
{:ok, members} = Redlite.smembers(db, "tags")

# Sorted sets
Redlite.zadd(db, "scores", [{100.0, "player1"}, {85.0, "player2"}])
{:ok, top} = Redlite.zrevrange(db, "scores", 0, 9)

Redlite.close(db)
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
cd sdks/redlite-elixir
mix test
```

## Links

- [Full README](https://github.com/russellromney/redlite/tree/main/sdks/redlite-elixir)
- [Source Code](https://github.com/russellromney/redlite/tree/main/sdks/redlite-elixir)
