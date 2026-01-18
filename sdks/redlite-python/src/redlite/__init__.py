"""
Redlite - Redis API + SQLite durability.

Embedded Redis-compatible database with optional server mode.

**Embedded mode** (PyO3 native bindings, no network, microsecond latency):
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


class RedliteError(Exception):
    """Error from redlite library."""

    pass


# Export native module types for advanced usage
try:
    from ._native import EmbeddedDb, SetOptions, ZMember, StreamId
except ImportError:
    # Native module not available (maturin build needed)
    EmbeddedDb = None
    SetOptions = None
    ZMember = None
    StreamId = None

__all__ = [
    "Redlite",
    "RedliteError",
    "FTSNamespace",
    "VectorNamespace",
    "GeoNamespace",
    "EmbeddedDb",
    "SetOptions",
    "ZMember",
    "StreamId",
]
__version__ = "0.1.0"
