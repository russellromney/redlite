"""
Redlite client - unified API for embedded and server modes.

Embedded mode: Direct PyO3 native bindings (no network, microsecond latency)
Server mode: Wraps redis-py (connect to redlite or Redis server)
"""

from __future__ import annotations

import math
from typing import Any, Dict, List, Optional, Set, Union


class FTSNamespace:
    """Full-text search namespace for Redlite-specific FTS commands."""

    def __init__(self, client: "Redlite"):
        self._client = client

    def search(
        self,
        index: str,
        query: str,
        nocontent: bool = False,
        limit: int = 10,
        offset: int = 0,
        withscores: bool = False,
    ) -> List[Any]:
        """
        Search an FTS index.

        Args:
            index: Index name
            query: Search query
            nocontent: Return only document IDs
            limit: Maximum results
            offset: Result offset
            withscores: Include BM25 scores

        Returns:
            Search results
        """
        args = ["FT.SEARCH", index, query]
        if nocontent:
            args.append("NOCONTENT")
        if withscores:
            args.append("WITHSCORES")
        args.extend(["LIMIT", str(offset), str(limit)])
        return self._client._execute(*args)

    def create(
        self,
        index: str,
        schema: Dict[str, str],
        prefix: Optional[str] = None,
        on: str = "HASH",
    ) -> bool:
        """
        Create an FTS index.

        Args:
            index: Index name
            schema: Field definitions {field_name: field_type}
            prefix: Key prefix to index
            on: Data type (HASH or JSON)
        """
        args = ["FT.CREATE", index, "ON", on]
        if prefix:
            args.extend(["PREFIX", "1", prefix])
        args.append("SCHEMA")
        for field, ftype in schema.items():
            args.extend([field, ftype])
        self._client._execute(*args)
        return True

    def dropindex(self, index: str, delete_docs: bool = False) -> bool:
        """Drop an FTS index."""
        args = ["FT.DROPINDEX", index]
        if delete_docs:
            args.append("DD")
        self._client._execute(*args)
        return True

    def info(self, index: str) -> Dict[str, Any]:
        """Get index information."""
        result = self._client._execute("FT.INFO", index)
        # Parse key-value pairs
        if isinstance(result, list):
            return dict(zip(result[::2], result[1::2]))
        return result


class VectorNamespace:
    """Vector search namespace for Redlite V* commands."""

    def __init__(self, client: "Redlite"):
        self._client = client

    def add(
        self,
        key: str,
        element: str,
        vector: List[float],
        attributes: Optional[Dict[str, Any]] = None,
    ) -> bool:
        """
        Add a vector to a vector set.

        Args:
            key: Vector set key
            element: Element identifier
            vector: Vector values
            attributes: Optional JSON attributes
        """
        args = ["VADD", key, "VALUES", str(len(vector))]
        args.extend(str(v) for v in vector)
        args.append(element)
        if attributes:
            import json

            args.extend(["SETATTR", json.dumps(attributes)])
        self._client._execute(*args)
        return True

    def sim(
        self,
        key: str,
        vector: List[float],
        count: int = 10,
        withscores: bool = False,
    ) -> List[Any]:
        """
        Find similar vectors.

        Args:
            key: Vector set key
            vector: Query vector
            count: Number of results
            withscores: Include distance scores
        """
        args = ["VSIM", key, "VALUES", str(len(vector))]
        args.extend(str(v) for v in vector)
        args.extend(["COUNT", str(count)])
        if withscores:
            args.append("WITHSCORES")
        return self._client._execute(*args)

    def rem(self, key: str, element: str) -> int:
        """Remove element from vector set."""
        return self._client._execute("VREM", key, element)

    def card(self, key: str) -> int:
        """Get number of elements in vector set."""
        return self._client._execute("VCARD", key)


class GeoNamespace:
    """Geospatial namespace for GEO* commands."""

    def __init__(self, client: "Redlite"):
        self._client = client

    def add(self, key: str, *members: tuple) -> int:
        """
        Add geospatial items.

        Args:
            key: Geo key
            members: Tuples of (longitude, latitude, member)
        """
        args = ["GEOADD", key]
        for lon, lat, member in members:
            args.extend([str(lon), str(lat), member])
        return self._client._execute(*args)

    def search(
        self,
        key: str,
        longitude: float,
        latitude: float,
        radius: float,
        unit: str = "km",
        count: Optional[int] = None,
        withdist: bool = False,
        withcoord: bool = False,
    ) -> List[Any]:
        """
        Search for members within radius.

        Args:
            key: Geo key
            longitude: Center longitude
            latitude: Center latitude
            radius: Search radius
            unit: Distance unit (m, km, mi, ft)
            count: Limit results
            withdist: Include distances
            withcoord: Include coordinates
        """
        args = [
            "GEOSEARCH",
            key,
            "FROMLONLAT",
            str(longitude),
            str(latitude),
            "BYRADIUS",
            str(radius),
            unit.upper(),
        ]
        if count:
            args.extend(["COUNT", str(count)])
        if withdist:
            args.append("WITHDIST")
        if withcoord:
            args.append("WITHCOORD")
        return self._client._execute(*args)


class Redlite:
    """
    Unified Redlite client supporting both embedded and server modes.

    **Embedded mode** (PyO3 native bindings, no network):
        db = Redlite(":memory:")
        db = Redlite("/path/to/db.db")

    **Server mode** (wraps redis-py):
        db = Redlite("redis://localhost:6379")

    Both modes expose the same API. Embedded mode is faster (microsecond latency),
    server mode connects to a running redlite or Redis server.
    """

    def __init__(
        self,
        url: str = ":memory:",
        cache_mb: int = 64,
    ):
        """
        Open a Redlite database.

        Args:
            url: Connection URL or file path
                - ":memory:" for in-memory embedded database
                - "/path/to/db.db" for file-based embedded database
                - "redis://host:port" for server mode
                - "rediss://host:port" for TLS server mode
            cache_mb: SQLite cache size in MB (embedded mode only)
        """
        self._url = url
        self._mode: str
        self._native = None
        self._redis = None

        if url.startswith(("redis://", "rediss://")):
            # Server mode - use redis-py
            self._mode = "server"
            try:
                import redis
            except ImportError:
                raise ImportError(
                    "Server mode requires redis-py. Install with: pip install redlite[redis]"
                )
            self._redis = redis.from_url(url)
        else:
            # Embedded mode - use PyO3 native module
            self._mode = "embedded"
            try:
                from ._native import EmbeddedDb
            except ImportError:
                raise ImportError(
                    "Native module not found. Run 'maturin develop' to build it."
                )

            if url == ":memory:":
                self._native = EmbeddedDb.open_memory()
            else:
                self._native = EmbeddedDb.open_with_cache(url, cache_mb)

        # Namespaces for Redlite-specific commands
        self.fts = FTSNamespace(self)
        self.vector = VectorNamespace(self)
        self.geo = GeoNamespace(self)

    @classmethod
    def open(cls, path: str = ":memory:", cache_mb: int = 64) -> "Redlite":
        """
        Open an embedded database (legacy API).

        Use Redlite(url) constructor instead for unified API.
        """
        return cls(path, cache_mb=cache_mb)

    @classmethod
    def connect(cls, url: str) -> "Redlite":
        """
        Connect to a server (legacy API).

        Use Redlite(url) constructor instead for unified API.
        """
        if not url.startswith(("redis://", "rediss://")):
            url = f"redis://{url}"
        return cls(url)

    @property
    def mode(self) -> str:
        """Return the connection mode: 'embedded' or 'server'."""
        return self._mode

    def close(self) -> None:
        """Close the database connection."""
        if self._mode == "embedded" and self._native is not None:
            # EmbeddedDb doesn't have a close method - it's handled by Python GC
            self._native = None
        elif self._mode == "server" and self._redis is not None:
            self._redis.close()
            self._redis = None

    def __enter__(self) -> "Redlite":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()

    def __del__(self) -> None:
        self.close()

    def _check_open(self) -> None:
        from . import RedliteError

        if self._mode == "embedded" and self._native is None:
            raise RedliteError("Database is closed")
        if self._mode == "server" and self._redis is None:
            raise RedliteError("Connection is closed")

    def _execute(self, *args) -> Any:
        """Execute a raw command (for Redlite-specific commands)."""
        self._check_open()
        if self._mode == "server":
            return self._redis.execute_command(*args)
        else:
            # For embedded mode, we'd need to implement a command dispatcher
            # For now, raise an error for unimplemented commands
            raise NotImplementedError(
                f"Command {args[0]} not yet implemented for embedded mode"
            )

    def _encode_value(self, value: Union[str, bytes, int, float]) -> bytes:
        """Encode a value to bytes."""
        if isinstance(value, bytes):
            return value
        elif isinstance(value, str):
            return value.encode("utf-8")
        else:
            return str(value).encode("utf-8")

    # =========================================================================
    # String Commands
    # =========================================================================

    def get(self, key: str) -> Optional[bytes]:
        """Get the value of a key."""
        self._check_open()
        if self._mode == "server":
            return self._redis.get(key)
        return self._native.get(key)

    def set(
        self,
        key: str,
        value: Union[str, bytes, int, float],
        ex: Optional[int] = None,
        px: Optional[int] = None,
        nx: bool = False,
        xx: bool = False,
    ) -> bool:
        """Set the value of a key."""
        self._check_open()
        if self._mode == "server":
            return self._redis.set(key, value, ex=ex, px=px, nx=nx, xx=xx)

        value_bytes = self._encode_value(value)

        if nx or xx or px:
            # Use set_opts for advanced options
            from ._native import SetOptions

            opts = SetOptions(ex=ex, px=px, nx=nx, xx=xx)
            return self._native.set_opts(key, value_bytes, opts)
        else:
            # Simple set with optional TTL
            return self._native.set(key, value_bytes, ex)

    def setex(self, key: str, seconds: int, value: Union[str, bytes]) -> bool:
        """Set key with expiration in seconds."""
        self._check_open()
        if self._mode == "server":
            return self._redis.setex(key, seconds, value)
        value_bytes = self._encode_value(value)
        return self._native.setex(key, seconds, value_bytes)

    def psetex(self, key: str, milliseconds: int, value: Union[str, bytes]) -> bool:
        """Set key with expiration in milliseconds."""
        self._check_open()
        if self._mode == "server":
            return self._redis.psetex(key, milliseconds, value)
        value_bytes = self._encode_value(value)
        return self._native.psetex(key, milliseconds, value_bytes)

    def getdel(self, key: str) -> Optional[bytes]:
        """Get and delete a key."""
        self._check_open()
        if self._mode == "server":
            return self._redis.getdel(key)
        return self._native.getdel(key)

    def append(self, key: str, value: Union[str, bytes]) -> int:
        """Append value to key, return new length."""
        self._check_open()
        if self._mode == "server":
            return self._redis.append(key, value)
        value_bytes = self._encode_value(value)
        return self._native.append(key, value_bytes)

    def strlen(self, key: str) -> int:
        """Get the length of the value stored at key."""
        self._check_open()
        if self._mode == "server":
            return self._redis.strlen(key)
        return self._native.strlen(key)

    def getrange(self, key: str, start: int, end: int) -> bytes:
        """Get a substring of the value stored at key."""
        self._check_open()
        if self._mode == "server":
            return self._redis.getrange(key, start, end)
        result = self._native.getrange(key, start, end)
        return result if result is not None else b""

    def setrange(self, key: str, offset: int, value: Union[str, bytes]) -> int:
        """Overwrite part of a string at key starting at offset."""
        self._check_open()
        if self._mode == "server":
            return self._redis.setrange(key, offset, value)
        value_bytes = self._encode_value(value)
        return self._native.setrange(key, offset, value_bytes)

    def incr(self, key: str) -> int:
        """Increment the integer value of a key by one."""
        self._check_open()
        if self._mode == "server":
            return self._redis.incr(key)
        return self._native.incr(key)

    def decr(self, key: str) -> int:
        """Decrement the integer value of a key by one."""
        self._check_open()
        if self._mode == "server":
            return self._redis.decr(key)
        return self._native.decr(key)

    def incrby(self, key: str, amount: int) -> int:
        """Increment the integer value of a key by amount."""
        self._check_open()
        if self._mode == "server":
            return self._redis.incrby(key, amount)
        return self._native.incrby(key, amount)

    def decrby(self, key: str, amount: int) -> int:
        """Decrement the integer value of a key by amount."""
        self._check_open()
        if self._mode == "server":
            return self._redis.decrby(key, amount)
        return self._native.decrby(key, amount)

    def incrbyfloat(self, key: str, amount: float) -> float:
        """Increment the float value of a key by amount."""
        self._check_open()
        if self._mode == "server":
            return float(self._redis.incrbyfloat(key, amount))
        return self._native.incrbyfloat(key, amount)

    # =========================================================================
    # Key Commands
    # =========================================================================

    def delete(self, *keys: str) -> int:
        """Delete one or more keys."""
        self._check_open()
        if not keys:
            return 0
        if self._mode == "server":
            return self._redis.delete(*keys)
        return self._native.delete(list(keys))

    def exists(self, *keys: str) -> int:
        """Check if keys exist, return count of existing keys."""
        self._check_open()
        if not keys:
            return 0
        if self._mode == "server":
            return self._redis.exists(*keys)
        return self._native.exists(list(keys))

    def type(self, key: str) -> str:
        """Get the type of a key."""
        self._check_open()
        if self._mode == "server":
            return self._redis.type(key)
        return self._native.key_type(key)

    def ttl(self, key: str) -> int:
        """Get the TTL of a key in seconds."""
        self._check_open()
        if self._mode == "server":
            return self._redis.ttl(key)
        return self._native.ttl(key)

    def pttl(self, key: str) -> int:
        """Get the TTL of a key in milliseconds."""
        self._check_open()
        if self._mode == "server":
            return self._redis.pttl(key)
        return self._native.pttl(key)

    def expire(self, key: str, seconds: int) -> bool:
        """Set a timeout on key in seconds."""
        self._check_open()
        if self._mode == "server":
            return self._redis.expire(key, seconds)
        return self._native.expire(key, seconds)

    def pexpire(self, key: str, milliseconds: int) -> bool:
        """Set a timeout on key in milliseconds."""
        self._check_open()
        if self._mode == "server":
            return self._redis.pexpire(key, milliseconds)
        return self._native.pexpire(key, milliseconds)

    def expireat(self, key: str, unix_time: int) -> bool:
        """Set an expiration time as Unix timestamp (seconds)."""
        self._check_open()
        if self._mode == "server":
            return self._redis.expireat(key, unix_time)
        return self._native.expireat(key, unix_time)

    def pexpireat(self, key: str, unix_time_ms: int) -> bool:
        """Set an expiration time as Unix timestamp (milliseconds)."""
        self._check_open()
        if self._mode == "server":
            return self._redis.pexpireat(key, unix_time_ms)
        return self._native.pexpireat(key, unix_time_ms)

    def persist(self, key: str) -> bool:
        """Remove the timeout on key."""
        self._check_open()
        if self._mode == "server":
            return self._redis.persist(key)
        return self._native.persist(key)

    def rename(self, src: str, dst: str) -> bool:
        """Rename a key."""
        self._check_open()
        if self._mode == "server":
            return self._redis.rename(src, dst)
        return self._native.rename(src, dst)

    def renamenx(self, src: str, dst: str) -> bool:
        """Rename a key only if the new key doesn't exist."""
        self._check_open()
        if self._mode == "server":
            return self._redis.renamenx(src, dst)
        return self._native.renamenx(src, dst)

    def keys(self, pattern: str = "*") -> List[str]:
        """Find all keys matching a pattern."""
        self._check_open()
        if self._mode == "server":
            return [
                k.decode() if isinstance(k, bytes) else k
                for k in self._redis.keys(pattern)
            ]
        return self._native.keys(pattern)

    def dbsize(self) -> int:
        """Return the number of keys in the database."""
        self._check_open()
        if self._mode == "server":
            return self._redis.dbsize()
        return self._native.dbsize()

    def flushdb(self) -> bool:
        """Delete all keys in the current database."""
        self._check_open()
        if self._mode == "server":
            return self._redis.flushdb()
        return self._native.flushdb()

    def select(self, db: int) -> bool:
        """Select the database to use."""
        self._check_open()
        if self._mode == "server":
            return self._redis.select(db)
        return self._native.select(db)

    # =========================================================================
    # Hash Commands
    # =========================================================================

    def hset(
        self, key: str, mapping: Dict[str, Union[str, bytes]] = None, **kwargs
    ) -> int:
        """Set hash field(s)."""
        self._check_open()
        items = {}
        if mapping:
            items.update(mapping)
        items.update(kwargs)
        if not items:
            return 0

        if self._mode == "server":
            return self._redis.hset(key, mapping=items)

        # For embedded mode, set each field
        count = 0
        for field, value in items.items():
            value_bytes = self._encode_value(value)
            count += self._native.hset(key, field, value_bytes)
        return count

    def hget(self, key: str, field: str) -> Optional[bytes]:
        """Get the value of a hash field."""
        self._check_open()
        if self._mode == "server":
            return self._redis.hget(key, field)
        return self._native.hget(key, field)

    def hdel(self, key: str, *fields: str) -> int:
        """Delete hash field(s)."""
        self._check_open()
        if not fields:
            return 0
        if self._mode == "server":
            return self._redis.hdel(key, *fields)
        return self._native.hdel(key, list(fields))

    def hexists(self, key: str, field: str) -> bool:
        """Check if a hash field exists."""
        self._check_open()
        if self._mode == "server":
            return self._redis.hexists(key, field)
        return self._native.hexists(key, field)

    def hlen(self, key: str) -> int:
        """Get the number of fields in a hash."""
        self._check_open()
        if self._mode == "server":
            return self._redis.hlen(key)
        return self._native.hlen(key)

    def hkeys(self, key: str) -> List[str]:
        """Get all field names in a hash."""
        self._check_open()
        if self._mode == "server":
            return [
                k.decode() if isinstance(k, bytes) else k
                for k in self._redis.hkeys(key)
            ]
        return self._native.hkeys(key)

    def hvals(self, key: str) -> List[bytes]:
        """Get all values in a hash."""
        self._check_open()
        if self._mode == "server":
            return list(self._redis.hvals(key))
        return list(self._native.hvals(key))

    def hincrby(self, key: str, field: str, amount: int) -> int:
        """Increment a hash field by amount."""
        self._check_open()
        if self._mode == "server":
            return self._redis.hincrby(key, field, amount)
        return self._native.hincrby(key, field, amount)

    def hgetall(self, key: str) -> Dict[str, bytes]:
        """Get all fields and values in a hash."""
        self._check_open()
        if self._mode == "server":
            return self._redis.hgetall(key)
        result = self._native.hgetall(key)
        return {field: value for field, value in result}

    def hmget(self, key: str, *fields: str) -> List[Optional[bytes]]:
        """Get values of multiple hash fields."""
        self._check_open()
        if not fields:
            return []
        if self._mode == "server":
            return list(self._redis.hmget(key, *fields))
        return list(self._native.hmget(key, list(fields)))

    # =========================================================================
    # List Commands
    # =========================================================================

    def lpush(self, key: str, *values: Union[str, bytes]) -> int:
        """Push values to the head of a list."""
        self._check_open()
        if not values:
            return 0
        if self._mode == "server":
            return self._redis.lpush(key, *values)
        encoded = [self._encode_value(v) for v in values]
        return self._native.lpush(key, encoded)

    def rpush(self, key: str, *values: Union[str, bytes]) -> int:
        """Push values to the tail of a list."""
        self._check_open()
        if not values:
            return 0
        if self._mode == "server":
            return self._redis.rpush(key, *values)
        encoded = [self._encode_value(v) for v in values]
        return self._native.rpush(key, encoded)

    def lpop(self, key: str, count: int = 1) -> Union[Optional[bytes], List[bytes]]:
        """Pop values from the head of a list."""
        self._check_open()
        if self._mode == "server":
            if count == 1:
                return self._redis.lpop(key)
            return self._redis.lpop(key, count)
        items = self._native.lpop(key, count)
        if count == 1:
            return items[0] if items else None
        return list(items)

    def rpop(self, key: str, count: int = 1) -> Union[Optional[bytes], List[bytes]]:
        """Pop values from the tail of a list."""
        self._check_open()
        if self._mode == "server":
            if count == 1:
                return self._redis.rpop(key)
            return self._redis.rpop(key, count)
        items = self._native.rpop(key, count)
        if count == 1:
            return items[0] if items else None
        return list(items)

    def llen(self, key: str) -> int:
        """Get the length of a list."""
        self._check_open()
        if self._mode == "server":
            return self._redis.llen(key)
        return self._native.llen(key)

    def lrange(self, key: str, start: int, stop: int) -> List[bytes]:
        """Get a range of elements from a list."""
        self._check_open()
        if self._mode == "server":
            return list(self._redis.lrange(key, start, stop))
        return list(self._native.lrange(key, start, stop))

    def lindex(self, key: str, index: int) -> Optional[bytes]:
        """Get an element from a list by index."""
        self._check_open()
        if self._mode == "server":
            return self._redis.lindex(key, index)
        return self._native.lindex(key, index)

    # =========================================================================
    # Set Commands
    # =========================================================================

    def sadd(self, key: str, *members: Union[str, bytes]) -> int:
        """Add members to a set."""
        self._check_open()
        if not members:
            return 0
        if self._mode == "server":
            return self._redis.sadd(key, *members)
        encoded = [self._encode_value(m) for m in members]
        return self._native.sadd(key, encoded)

    def srem(self, key: str, *members: Union[str, bytes]) -> int:
        """Remove members from a set."""
        self._check_open()
        if not members:
            return 0
        if self._mode == "server":
            return self._redis.srem(key, *members)
        encoded = [self._encode_value(m) for m in members]
        return self._native.srem(key, encoded)

    def smembers(self, key: str) -> Set[bytes]:
        """Get all members of a set."""
        self._check_open()
        if self._mode == "server":
            return self._redis.smembers(key)
        return set(self._native.smembers(key))

    def sismember(self, key: str, member: Union[str, bytes]) -> bool:
        """Check if a value is a member of a set."""
        self._check_open()
        if self._mode == "server":
            return self._redis.sismember(key, member)
        member_bytes = self._encode_value(member)
        return self._native.sismember(key, member_bytes)

    def scard(self, key: str) -> int:
        """Get the number of members in a set."""
        self._check_open()
        if self._mode == "server":
            return self._redis.scard(key)
        return self._native.scard(key)

    # =========================================================================
    # Sorted Set Commands
    # =========================================================================

    def zadd(
        self, key: str, mapping: Dict[Union[str, bytes], float] = None, **kwargs
    ) -> int:
        """Add members to a sorted set."""
        self._check_open()
        items = {}
        if mapping:
            items.update(mapping)
        items.update(kwargs)
        if not items:
            return 0

        if self._mode == "server":
            return self._redis.zadd(key, items)

        # Convert to list of (score, member) tuples
        members = []
        for member, score in items.items():
            member_bytes = self._encode_value(member)
            members.append((float(score), member_bytes))
        return self._native.zadd(key, members)

    def zrem(self, key: str, *members: Union[str, bytes]) -> int:
        """Remove members from a sorted set."""
        self._check_open()
        if not members:
            return 0
        if self._mode == "server":
            return self._redis.zrem(key, *members)
        encoded = [self._encode_value(m) for m in members]
        return self._native.zrem(key, encoded)

    def zscore(self, key: str, member: Union[str, bytes]) -> Optional[float]:
        """Get the score of a member in a sorted set."""
        self._check_open()
        if self._mode == "server":
            return self._redis.zscore(key, member)
        member_bytes = self._encode_value(member)
        result = self._native.zscore(key, member_bytes)
        if result is not None and math.isnan(result):
            return None
        return result

    def zcard(self, key: str) -> int:
        """Get the number of members in a sorted set."""
        self._check_open()
        if self._mode == "server":
            return self._redis.zcard(key)
        return self._native.zcard(key)

    def zcount(self, key: str, min_score: float, max_score: float) -> int:
        """Count members with scores in the given range."""
        self._check_open()
        if self._mode == "server":
            return self._redis.zcount(key, min_score, max_score)
        return self._native.zcount(key, min_score, max_score)

    def zincrby(self, key: str, amount: float, member: Union[str, bytes]) -> float:
        """Increment the score of a member in a sorted set."""
        self._check_open()
        if self._mode == "server":
            return self._redis.zincrby(key, amount, member)
        member_bytes = self._encode_value(member)
        return self._native.zincrby(key, amount, member_bytes)

    def zrange(
        self, key: str, start: int, stop: int, withscores: bool = False
    ) -> Union[List[bytes], List[tuple]]:
        """Get members by rank range (ascending order)."""
        self._check_open()
        if self._mode == "server":
            return self._redis.zrange(key, start, stop, withscores=withscores)
        result = self._native.zrange(key, start, stop, withscores)
        if withscores:
            return [(member, score) for member, score in result]
        return [member for member, _ in result]

    def zrevrange(
        self, key: str, start: int, stop: int, withscores: bool = False
    ) -> Union[List[bytes], List[tuple]]:
        """Get members by rank range (descending order)."""
        self._check_open()
        if self._mode == "server":
            return self._redis.zrevrange(key, start, stop, withscores=withscores)
        result = self._native.zrevrange(key, start, stop, withscores)
        if withscores:
            return [(member, score) for member, score in result]
        return [member for member, _ in result]

    # =========================================================================
    # Multi-key Commands
    # =========================================================================

    def mget(self, *keys: str) -> List[Optional[bytes]]:
        """Get values of multiple keys."""
        self._check_open()
        if not keys:
            return []
        if self._mode == "server":
            return list(self._redis.mget(*keys))
        return list(self._native.mget(list(keys)))

    def mset(self, mapping: Dict[str, Union[str, bytes]] = None, **kwargs) -> bool:
        """Set multiple key-value pairs atomically."""
        self._check_open()
        items = {}
        if mapping:
            items.update(mapping)
        items.update(kwargs)
        if not items:
            return True

        if self._mode == "server":
            return self._redis.mset(items)

        pairs = [(k, self._encode_value(v)) for k, v in items.items()]
        return self._native.mset(pairs)

    # =========================================================================
    # Scan Commands
    # =========================================================================

    def scan(
        self, cursor: str = "0", match: Optional[str] = None, count: int = 10
    ) -> tuple:
        """Incrementally iterate keys matching a pattern."""
        self._check_open()
        if self._mode == "server":
            return self._redis.scan(cursor=int(cursor), match=match, count=count)
        return self._native.scan(cursor, match, count)

    def hscan(
        self, key: str, cursor: str = "0", match: Optional[str] = None, count: int = 10
    ) -> tuple:
        """Incrementally iterate hash fields."""
        self._check_open()
        if self._mode == "server":
            return self._redis.hscan(key, cursor=int(cursor), match=match, count=count)
        next_cursor, items = self._native.hscan(key, cursor, match, count)
        return (next_cursor, {field: value for field, value in items})

    def sscan(
        self, key: str, cursor: str = "0", match: Optional[str] = None, count: int = 10
    ) -> tuple:
        """Incrementally iterate set members."""
        self._check_open()
        if self._mode == "server":
            return self._redis.sscan(key, cursor=int(cursor), match=match, count=count)
        next_cursor, members = self._native.sscan(key, cursor, match, count)
        return (next_cursor, list(members))

    def zscan(
        self, key: str, cursor: str = "0", match: Optional[str] = None, count: int = 10
    ) -> tuple:
        """Incrementally iterate sorted set members with scores."""
        self._check_open()
        if self._mode == "server":
            return self._redis.zscan(key, cursor=int(cursor), match=match, count=count)
        next_cursor, members = self._native.zscan(key, cursor, match, count)
        return (next_cursor, [(member, score) for member, score in members])

    # =========================================================================
    # Server Commands
    # =========================================================================

    def vacuum(self) -> int:
        """Compact the database, return bytes freed (embedded mode only)."""
        self._check_open()
        if self._mode == "server":
            return self._execute("VACUUM")
        return self._native.vacuum()

    @staticmethod
    def version() -> str:
        """Get the redlite library version."""
        return "0.1.0"  # TODO: Get from native module
