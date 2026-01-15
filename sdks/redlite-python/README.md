# Redlite Python SDK

Python SDK for Redlite - Redis API + SQLite durability.

## Install

```bash
pip install redlite

# With server mode support
pip install redlite[redis]
```

## Usage

```python
from redlite import Redlite

# Auto-detects mode from URL
db = Redlite(":memory:")              # Embedded (PyO3 native)
db = Redlite("/path/to/db.db")        # Embedded file
db = Redlite("redis://localhost:6379") # Server mode (wraps redis-py)

# Same API either way
db.set("key", "value")
db.get("key")  # b"value"

# Check mode
db.mode  # "embedded" or "server"

db.close()
```

### With Context Manager

```python
with Redlite("/data/cache.db", cache_mb=1000) as db:
    db.set("hello", "world")
    db.hset("user:1", {"name": "Alice"})
    db.lpush("queue", "job1", "job2")
```

## Supported Commands

### Strings
`get`, `set`, `setex`, `psetex`, `getdel`, `append`, `strlen`, `getrange`, `setrange`, `incr`, `decr`, `incrby`, `decrby`, `incrbyfloat`, `mget`, `mset`

### Keys
`delete`, `exists`, `type`, `ttl`, `pttl`, `expire`, `pexpire`, `expireat`, `pexpireat`, `persist`, `rename`, `renamenx`, `keys`, `dbsize`, `flushdb`, `select`

### Hashes
`hset`, `hget`, `hdel`, `hexists`, `hlen`, `hkeys`, `hvals`, `hincrby`, `hgetall`, `hmget`

### Lists
`lpush`, `rpush`, `lpop`, `rpop`, `llen`, `lrange`, `lindex`

### Sets
`sadd`, `srem`, `smembers`, `sismember`, `scard`

### Sorted Sets
`zadd`, `zrem`, `zscore`, `zcard`, `zcount`, `zincrby`, `zrange`, `zrevrange`

### Scan Commands
`scan`, `hscan`, `sscan`, `zscan`

### Redlite-Specific Features

```python
# Full-text search
db.fts.create("idx", {"title": "TEXT"}, prefix="doc:")
db.fts.search("idx", "hello world")

# Vector search
db.vector.add("embeddings", "doc1", [0.1, 0.2, 0.3])
db.vector.sim("embeddings", [0.1, 0.2, 0.3], count=5)

# Geospatial
db.geo.add("locations", (-122.4, 37.8, "sf"))
db.geo.search("locations", -122.4, 37.8, 100, unit="km")
```

## Build

Requires maturin for embedded mode:

```bash
cd sdks/redlite-python
pip install maturin
maturin develop          # Development build
maturin build --release  # Release wheel
```

## Test

```bash
uv run pytest tests/ -v
```

## Architecture

The SDK uses PyO3 for direct Rust bindings (1 layer):

```
Python -> PyO3 native module -> Rust core
```

This provides microsecond latency for embedded mode operations.
