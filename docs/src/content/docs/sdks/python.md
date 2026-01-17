---
title: Python SDK
description: Redlite SDK for Python
---

Python SDK with native PyO3 bindings for embedded database operations.

## Installation

```bash
pip install redlite

# With server mode support (wraps redis-py)
pip install redlite[redis]
```

## Quick Start

```python
from redlite import Redlite

# Auto-detects mode from URL
db = Redlite(":memory:")              # Embedded (native)
db = Redlite("/path/to/db.db")        # Embedded file
db = Redlite("redis://localhost:6379") # Server mode

# Same API either way
db.set("key", "value")
db.get("key")  # b"value"

# With context manager
with Redlite("/data/cache.db") as db:
    db.hset("user:1", {"name": "Alice"})
    db.lpush("queue", "job1", "job2")
```

## API Overview

**Strings**: `get`, `set`, `setex`, `incr`, `decr`, `incrby`, `append`, `mget`, `mset`

**Keys**: `delete`, `exists`, `type`, `ttl`, `expire`, `keys`, `dbsize`, `flushdb`

**Hashes**: `hset`, `hget`, `hdel`, `hgetall`, `hmget`, `hincrby`

**Lists**: `lpush`, `rpush`, `lpop`, `rpop`, `llen`, `lrange`

**Sets**: `sadd`, `srem`, `smembers`, `sismember`, `scard`

**Sorted Sets**: `zadd`, `zrem`, `zscore`, `zrange`, `zrevrange`

**Extensions**: `fts` (full-text search), `vector` (similarity), `geo` (geospatial)

## Testing

```bash
cd sdks/redlite-python
uv run pytest tests/ -v
```

## Links

- [Full README](https://github.com/russellromney/redlite/tree/main/sdks/redlite-python)
- [Source Code](https://github.com/russellromney/redlite/tree/main/sdks/redlite-python)
