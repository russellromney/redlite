"""
Redlite - Redis API + SQLite durability.

Embedded Redis-compatible database with optional server mode.

**Embedded mode** (FFI, no network, microsecond latency):
    >>> db = Redlite(":memory:")
    >>> db = Redlite("/path/to/db.db")

**Server mode** (wraps redis-py):
    >>> db = Redlite("redis://localhost:6379")

Both modes expose the same API:
    >>> db.set("key", "value")
    >>> db.get("key")
    b'value'
    >>> db.fts.search("idx", "hello world")
    >>> db.close()

With context manager:
    >>> with Redlite("cache.db", cache_mb=1000) as db:
    ...     db.set("hello", "world")
    ...     db.lpush("queue", "job1", "job2")
    ...     db.hset("user:1", name="Alice")
"""

from .client import Redlite, FTSNamespace, VectorNamespace, GeoNamespace
from ._ffi import RedliteError

__all__ = ["Redlite", "RedliteError", "FTSNamespace", "VectorNamespace", "GeoNamespace"]
__version__ = "0.1.0"
