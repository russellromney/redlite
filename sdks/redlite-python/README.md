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
db = Redlite(":memory:")              # Embedded (FFI)
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

Requires the native library for embedded mode:

```bash
# Build FFI library
cd crates/redlite-ffi && cargo build --release

# Set library path
export REDLITE_LIB_PATH=/path/to/target/release/libredlite_ffi.dylib
```

## Test

```bash
uv run pytest tests/ -v
```
