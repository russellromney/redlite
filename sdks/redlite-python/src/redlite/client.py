"""
Redlite client - unified API for embedded and server modes.

Embedded mode: Direct FFI to libredlite_ffi (no network, microsecond latency)
Server mode: Wraps redis-py (connect to redlite or Redis server)
"""

from __future__ import annotations

import math
from typing import Any, Dict, List, Optional, Set, Union

from . import _ffi


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

    def add(
        self, key: str, *members: tuple
    ) -> int:
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
            "GEOSEARCH", key,
            "FROMLONLAT", str(longitude), str(latitude),
            "BYRADIUS", str(radius), unit.upper(),
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

    **Embedded mode** (FFI, no network):
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
        self._handle = None
        self._redis = None
        self._lib = None
        self._ffi_obj = None

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
            # Embedded mode - use FFI
            self._mode = "embedded"
            self._lib = _ffi.get_lib()
            self._ffi_obj = _ffi.get_ffi()

            if url == ":memory:":
                self._handle = self._lib.redlite_open_memory()
            else:
                self._handle = self._lib.redlite_open_with_cache(
                    url.encode("utf-8"), cache_mb
                )

            if self._handle == self._ffi_obj.NULL:
                _ffi.check_error()
                raise _ffi.RedliteError("Failed to open database")

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
        if self._mode == "embedded" and self._handle is not None:
            self._lib.redlite_close(self._handle)
            self._handle = None
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
        if self._mode == "embedded" and self._handle is None:
            raise _ffi.RedliteError("Database is closed")
        if self._mode == "server" and self._redis is None:
            raise _ffi.RedliteError("Connection is closed")

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
        result = self._lib.redlite_get(self._handle, key.encode("utf-8"))
        return _ffi.bytes_to_python(result)

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
        ttl = ex if ex is not None else (px // 1000 if px is not None else 0)
        result = self._lib.redlite_set(
            self._handle,
            key.encode("utf-8"),
            value_bytes,
            len(value_bytes),
            ttl,
        )
        if result < 0:
            _ffi.check_error()
        return result == 0

    def setex(self, key: str, seconds: int, value: Union[str, bytes]) -> bool:
        """Set key with expiration in seconds."""
        self._check_open()
        if self._mode == "server":
            return self._redis.setex(key, seconds, value)
        value_bytes = self._encode_value(value)
        result = self._lib.redlite_setex(
            self._handle, key.encode("utf-8"), seconds, value_bytes, len(value_bytes)
        )
        if result < 0:
            _ffi.check_error()
        return result == 0

    def psetex(self, key: str, milliseconds: int, value: Union[str, bytes]) -> bool:
        """Set key with expiration in milliseconds."""
        self._check_open()
        if self._mode == "server":
            return self._redis.psetex(key, milliseconds, value)
        value_bytes = self._encode_value(value)
        result = self._lib.redlite_psetex(
            self._handle, key.encode("utf-8"), milliseconds, value_bytes, len(value_bytes)
        )
        if result < 0:
            _ffi.check_error()
        return result == 0

    def getdel(self, key: str) -> Optional[bytes]:
        """Get and delete a key."""
        self._check_open()
        if self._mode == "server":
            return self._redis.getdel(key)
        result = self._lib.redlite_getdel(self._handle, key.encode("utf-8"))
        return _ffi.bytes_to_python(result)

    def append(self, key: str, value: Union[str, bytes]) -> int:
        """Append value to key, return new length."""
        self._check_open()
        if self._mode == "server":
            return self._redis.append(key, value)
        value_bytes = self._encode_value(value)
        result = self._lib.redlite_append(
            self._handle, key.encode("utf-8"), value_bytes, len(value_bytes)
        )
        if result < 0:
            _ffi.check_error()
        return result

    def strlen(self, key: str) -> int:
        """Get the length of the value stored at key."""
        self._check_open()
        if self._mode == "server":
            return self._redis.strlen(key)
        result = self._lib.redlite_strlen(self._handle, key.encode("utf-8"))
        if result < 0:
            _ffi.check_error()
        return result

    def getrange(self, key: str, start: int, end: int) -> bytes:
        """Get a substring of the value stored at key."""
        self._check_open()
        if self._mode == "server":
            return self._redis.getrange(key, start, end)
        result = self._lib.redlite_getrange(self._handle, key.encode("utf-8"), start, end)
        data = _ffi.bytes_to_python(result)
        return data if data is not None else b""

    def setrange(self, key: str, offset: int, value: Union[str, bytes]) -> int:
        """Overwrite part of a string at key starting at offset."""
        self._check_open()
        if self._mode == "server":
            return self._redis.setrange(key, offset, value)
        value_bytes = self._encode_value(value)
        result = self._lib.redlite_setrange(
            self._handle, key.encode("utf-8"), offset, value_bytes, len(value_bytes)
        )
        if result < 0:
            _ffi.check_error()
        return result

    def incr(self, key: str) -> int:
        """Increment the integer value of a key by one."""
        self._check_open()
        if self._mode == "server":
            return self._redis.incr(key)
        result = self._lib.redlite_incr(self._handle, key.encode("utf-8"))
        if result == -9223372036854775808:
            _ffi.check_error()
        return result

    def decr(self, key: str) -> int:
        """Decrement the integer value of a key by one."""
        self._check_open()
        if self._mode == "server":
            return self._redis.decr(key)
        result = self._lib.redlite_decr(self._handle, key.encode("utf-8"))
        if result == -9223372036854775808:
            _ffi.check_error()
        return result

    def incrby(self, key: str, amount: int) -> int:
        """Increment the integer value of a key by amount."""
        self._check_open()
        if self._mode == "server":
            return self._redis.incrby(key, amount)
        result = self._lib.redlite_incrby(self._handle, key.encode("utf-8"), amount)
        if result == -9223372036854775808:
            _ffi.check_error()
        return result

    def decrby(self, key: str, amount: int) -> int:
        """Decrement the integer value of a key by amount."""
        self._check_open()
        if self._mode == "server":
            return self._redis.decrby(key, amount)
        result = self._lib.redlite_decrby(self._handle, key.encode("utf-8"), amount)
        if result == -9223372036854775808:
            _ffi.check_error()
        return result

    def incrbyfloat(self, key: str, amount: float) -> float:
        """Increment the float value of a key by amount."""
        self._check_open()
        if self._mode == "server":
            return float(self._redis.incrbyfloat(key, amount))
        result = self._lib.redlite_incrbyfloat(self._handle, key.encode("utf-8"), amount)
        if result == self._ffi_obj.NULL:
            _ffi.check_error()
            raise _ffi.RedliteError("INCRBYFLOAT failed")
        value = self._ffi_obj.string(result).decode("utf-8")
        self._lib.redlite_free_string(result)
        return float(value)

    # =========================================================================
    # Key Commands
    # =========================================================================

    def _make_string_array(self, strings):
        """Create a char*[] array from Python strings."""
        c_strings = [self._ffi_obj.new("char[]", s.encode("utf-8")) for s in strings]
        array = self._ffi_obj.new("char*[]", c_strings)
        return array, c_strings

    def _make_bytes_array(self, byte_values):
        """Create a RedliteBytes[] array from Python bytes."""
        buffers = []
        items = []
        for b in byte_values:
            if b:
                buf = self._ffi_obj.new("uint8_t[]", b)
                buffers.append(buf)
                items.append({"data": buf, "len": len(b)})
            else:
                items.append({"data": self._ffi_obj.NULL, "len": 0})
        array = self._ffi_obj.new("RedliteBytes[]", items)
        return array, buffers

    def delete(self, *keys: str) -> int:
        """Delete one or more keys."""
        self._check_open()
        if not keys:
            return 0
        if self._mode == "server":
            return self._redis.delete(*keys)
        keys_array, _refs = self._make_string_array(keys)
        result = self._lib.redlite_del(self._handle, keys_array, len(keys))
        if result < 0:
            _ffi.check_error()
        return result

    def exists(self, *keys: str) -> int:
        """Check if keys exist, return count of existing keys."""
        self._check_open()
        if not keys:
            return 0
        if self._mode == "server":
            return self._redis.exists(*keys)
        keys_array, _refs = self._make_string_array(keys)
        result = self._lib.redlite_exists(self._handle, keys_array, len(keys))
        if result < 0:
            _ffi.check_error()
        return result

    def type(self, key: str) -> str:
        """Get the type of a key."""
        self._check_open()
        if self._mode == "server":
            return self._redis.type(key)
        result = self._lib.redlite_type(self._handle, key.encode("utf-8"))
        if result == self._ffi_obj.NULL:
            _ffi.check_error()
            return "none"
        value = self._ffi_obj.string(result).decode("utf-8")
        self._lib.redlite_free_string(result)
        return value

    def ttl(self, key: str) -> int:
        """Get the TTL of a key in seconds."""
        self._check_open()
        if self._mode == "server":
            return self._redis.ttl(key)
        result = self._lib.redlite_ttl(self._handle, key.encode("utf-8"))
        if result == -3:
            _ffi.check_error()
        return result

    def pttl(self, key: str) -> int:
        """Get the TTL of a key in milliseconds."""
        self._check_open()
        if self._mode == "server":
            return self._redis.pttl(key)
        result = self._lib.redlite_pttl(self._handle, key.encode("utf-8"))
        if result == -3:
            _ffi.check_error()
        return result

    def expire(self, key: str, seconds: int) -> bool:
        """Set a timeout on key in seconds."""
        self._check_open()
        if self._mode == "server":
            return self._redis.expire(key, seconds)
        result = self._lib.redlite_expire(self._handle, key.encode("utf-8"), seconds)
        if result < 0:
            _ffi.check_error()
        return result == 1

    def pexpire(self, key: str, milliseconds: int) -> bool:
        """Set a timeout on key in milliseconds."""
        self._check_open()
        if self._mode == "server":
            return self._redis.pexpire(key, milliseconds)
        result = self._lib.redlite_pexpire(self._handle, key.encode("utf-8"), milliseconds)
        if result < 0:
            _ffi.check_error()
        return result == 1

    def expireat(self, key: str, unix_time: int) -> bool:
        """Set an expiration time as Unix timestamp (seconds)."""
        self._check_open()
        if self._mode == "server":
            return self._redis.expireat(key, unix_time)
        result = self._lib.redlite_expireat(self._handle, key.encode("utf-8"), unix_time)
        if result < 0:
            _ffi.check_error()
        return result == 1

    def pexpireat(self, key: str, unix_time_ms: int) -> bool:
        """Set an expiration time as Unix timestamp (milliseconds)."""
        self._check_open()
        if self._mode == "server":
            return self._redis.pexpireat(key, unix_time_ms)
        result = self._lib.redlite_pexpireat(self._handle, key.encode("utf-8"), unix_time_ms)
        if result < 0:
            _ffi.check_error()
        return result == 1

    def persist(self, key: str) -> bool:
        """Remove the timeout on key."""
        self._check_open()
        if self._mode == "server":
            return self._redis.persist(key)
        result = self._lib.redlite_persist(self._handle, key.encode("utf-8"))
        if result < 0:
            _ffi.check_error()
        return result == 1

    def rename(self, src: str, dst: str) -> bool:
        """Rename a key."""
        self._check_open()
        if self._mode == "server":
            return self._redis.rename(src, dst)
        result = self._lib.redlite_rename(
            self._handle, src.encode("utf-8"), dst.encode("utf-8")
        )
        if result < 0:
            _ffi.check_error()
        return result == 0

    def renamenx(self, src: str, dst: str) -> bool:
        """Rename a key only if the new key doesn't exist."""
        self._check_open()
        if self._mode == "server":
            return self._redis.renamenx(src, dst)
        result = self._lib.redlite_renamenx(
            self._handle, src.encode("utf-8"), dst.encode("utf-8")
        )
        if result < 0:
            _ffi.check_error()
        return result == 1

    def keys(self, pattern: str = "*") -> List[str]:
        """Find all keys matching a pattern."""
        self._check_open()
        if self._mode == "server":
            return [k.decode() if isinstance(k, bytes) else k for k in self._redis.keys(pattern)]
        result = self._lib.redlite_keys(self._handle, pattern.encode("utf-8"))
        return _ffi.string_array_to_python(result)

    def dbsize(self) -> int:
        """Return the number of keys in the database."""
        self._check_open()
        if self._mode == "server":
            return self._redis.dbsize()
        result = self._lib.redlite_dbsize(self._handle)
        if result < 0:
            _ffi.check_error()
        return result

    def flushdb(self) -> bool:
        """Delete all keys in the current database."""
        self._check_open()
        if self._mode == "server":
            return self._redis.flushdb()
        result = self._lib.redlite_flushdb(self._handle)
        if result < 0:
            _ffi.check_error()
        return result == 0

    def select(self, db: int) -> bool:
        """Select the database to use."""
        self._check_open()
        if self._mode == "server":
            return self._redis.select(db)
        result = self._lib.redlite_select(self._handle, db)
        if result < 0:
            _ffi.check_error()
        return result == 0

    # =========================================================================
    # Hash Commands
    # =========================================================================

    def hset(self, key: str, mapping: Dict[str, Union[str, bytes]] = None, **kwargs) -> int:
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

        fields = list(items.keys())
        values = [self._encode_value(v) for v in items.values()]
        fields_array, _field_refs = self._make_string_array(fields)
        values_array, _value_refs = self._make_bytes_array(values)

        result = self._lib.redlite_hset(
            self._handle, key.encode("utf-8"), fields_array, values_array, len(items)
        )
        if result < 0:
            _ffi.check_error()
        return result

    def hget(self, key: str, field: str) -> Optional[bytes]:
        """Get the value of a hash field."""
        self._check_open()
        if self._mode == "server":
            return self._redis.hget(key, field)
        result = self._lib.redlite_hget(
            self._handle, key.encode("utf-8"), field.encode("utf-8")
        )
        return _ffi.bytes_to_python(result)

    def hdel(self, key: str, *fields: str) -> int:
        """Delete hash field(s)."""
        self._check_open()
        if not fields:
            return 0
        if self._mode == "server":
            return self._redis.hdel(key, *fields)
        fields_array, _refs = self._make_string_array(fields)
        result = self._lib.redlite_hdel(
            self._handle, key.encode("utf-8"), fields_array, len(fields)
        )
        if result < 0:
            _ffi.check_error()
        return result

    def hexists(self, key: str, field: str) -> bool:
        """Check if a hash field exists."""
        self._check_open()
        if self._mode == "server":
            return self._redis.hexists(key, field)
        result = self._lib.redlite_hexists(
            self._handle, key.encode("utf-8"), field.encode("utf-8")
        )
        if result < 0:
            _ffi.check_error()
        return result == 1

    def hlen(self, key: str) -> int:
        """Get the number of fields in a hash."""
        self._check_open()
        if self._mode == "server":
            return self._redis.hlen(key)
        result = self._lib.redlite_hlen(self._handle, key.encode("utf-8"))
        if result < 0:
            _ffi.check_error()
        return result

    def hkeys(self, key: str) -> List[str]:
        """Get all field names in a hash."""
        self._check_open()
        if self._mode == "server":
            return [k.decode() if isinstance(k, bytes) else k for k in self._redis.hkeys(key)]
        result = self._lib.redlite_hkeys(self._handle, key.encode("utf-8"))
        return _ffi.string_array_to_python(result)

    def hvals(self, key: str) -> List[bytes]:
        """Get all values in a hash."""
        self._check_open()
        if self._mode == "server":
            return list(self._redis.hvals(key))
        result = self._lib.redlite_hvals(self._handle, key.encode("utf-8"))
        return _ffi.bytes_array_to_python(result)

    def hincrby(self, key: str, field: str, amount: int) -> int:
        """Increment a hash field by amount."""
        self._check_open()
        if self._mode == "server":
            return self._redis.hincrby(key, field, amount)
        result = self._lib.redlite_hincrby(
            self._handle, key.encode("utf-8"), field.encode("utf-8"), amount
        )
        if result == -9223372036854775808:
            _ffi.check_error()
        return result

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
        values_array, _refs = self._make_bytes_array(encoded)
        result = self._lib.redlite_lpush(
            self._handle, key.encode("utf-8"), values_array, len(values)
        )
        if result < 0:
            _ffi.check_error()
        return result

    def rpush(self, key: str, *values: Union[str, bytes]) -> int:
        """Push values to the tail of a list."""
        self._check_open()
        if not values:
            return 0
        if self._mode == "server":
            return self._redis.rpush(key, *values)
        encoded = [self._encode_value(v) for v in values]
        values_array, _refs = self._make_bytes_array(encoded)
        result = self._lib.redlite_rpush(
            self._handle, key.encode("utf-8"), values_array, len(values)
        )
        if result < 0:
            _ffi.check_error()
        return result

    def lpop(self, key: str, count: int = 1) -> Union[Optional[bytes], List[bytes]]:
        """Pop values from the head of a list."""
        self._check_open()
        if self._mode == "server":
            if count == 1:
                return self._redis.lpop(key)
            return self._redis.lpop(key, count)
        result = self._lib.redlite_lpop(self._handle, key.encode("utf-8"), count)
        items = _ffi.bytes_array_to_python(result)
        if count == 1:
            return items[0] if items else None
        return items

    def rpop(self, key: str, count: int = 1) -> Union[Optional[bytes], List[bytes]]:
        """Pop values from the tail of a list."""
        self._check_open()
        if self._mode == "server":
            if count == 1:
                return self._redis.rpop(key)
            return self._redis.rpop(key, count)
        result = self._lib.redlite_rpop(self._handle, key.encode("utf-8"), count)
        items = _ffi.bytes_array_to_python(result)
        if count == 1:
            return items[0] if items else None
        return items

    def llen(self, key: str) -> int:
        """Get the length of a list."""
        self._check_open()
        if self._mode == "server":
            return self._redis.llen(key)
        result = self._lib.redlite_llen(self._handle, key.encode("utf-8"))
        if result < 0:
            _ffi.check_error()
        return result

    def lrange(self, key: str, start: int, stop: int) -> List[bytes]:
        """Get a range of elements from a list."""
        self._check_open()
        if self._mode == "server":
            return list(self._redis.lrange(key, start, stop))
        result = self._lib.redlite_lrange(self._handle, key.encode("utf-8"), start, stop)
        return _ffi.bytes_array_to_python(result)

    def lindex(self, key: str, index: int) -> Optional[bytes]:
        """Get an element from a list by index."""
        self._check_open()
        if self._mode == "server":
            return self._redis.lindex(key, index)
        result = self._lib.redlite_lindex(self._handle, key.encode("utf-8"), index)
        return _ffi.bytes_to_python(result)

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
        members_array, _refs = self._make_bytes_array(encoded)
        result = self._lib.redlite_sadd(
            self._handle, key.encode("utf-8"), members_array, len(members)
        )
        if result < 0:
            _ffi.check_error()
        return result

    def srem(self, key: str, *members: Union[str, bytes]) -> int:
        """Remove members from a set."""
        self._check_open()
        if not members:
            return 0
        if self._mode == "server":
            return self._redis.srem(key, *members)
        encoded = [self._encode_value(m) for m in members]
        members_array, _refs = self._make_bytes_array(encoded)
        result = self._lib.redlite_srem(
            self._handle, key.encode("utf-8"), members_array, len(members)
        )
        if result < 0:
            _ffi.check_error()
        return result

    def smembers(self, key: str) -> Set[bytes]:
        """Get all members of a set."""
        self._check_open()
        if self._mode == "server":
            return self._redis.smembers(key)
        result = self._lib.redlite_smembers(self._handle, key.encode("utf-8"))
        return set(_ffi.bytes_array_to_python(result))

    def sismember(self, key: str, member: Union[str, bytes]) -> bool:
        """Check if a value is a member of a set."""
        self._check_open()
        if self._mode == "server":
            return self._redis.sismember(key, member)
        member_bytes = self._encode_value(member)
        result = self._lib.redlite_sismember(
            self._handle, key.encode("utf-8"), member_bytes, len(member_bytes)
        )
        if result < 0:
            _ffi.check_error()
        return result == 1

    def scard(self, key: str) -> int:
        """Get the number of members in a set."""
        self._check_open()
        if self._mode == "server":
            return self._redis.scard(key)
        result = self._lib.redlite_scard(self._handle, key.encode("utf-8"))
        if result < 0:
            _ffi.check_error()
        return result

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

        member_bufs = []
        members_array = self._ffi_obj.new("RedliteZMember[]", len(items))
        for i, (member, score) in enumerate(items.items()):
            member_bytes = self._encode_value(member)
            member_buf = self._ffi_obj.new("uint8_t[]", member_bytes)
            member_bufs.append(member_buf)
            members_array[i].score = float(score)
            members_array[i].member = member_buf
            members_array[i].member_len = len(member_bytes)

        result = self._lib.redlite_zadd(
            self._handle, key.encode("utf-8"), members_array, len(items)
        )
        if result < 0:
            _ffi.check_error()
        return result

    def zrem(self, key: str, *members: Union[str, bytes]) -> int:
        """Remove members from a sorted set."""
        self._check_open()
        if not members:
            return 0
        if self._mode == "server":
            return self._redis.zrem(key, *members)
        encoded = [self._encode_value(m) for m in members]
        members_array, _refs = self._make_bytes_array(encoded)
        result = self._lib.redlite_zrem(
            self._handle, key.encode("utf-8"), members_array, len(members)
        )
        if result < 0:
            _ffi.check_error()
        return result

    def zscore(self, key: str, member: Union[str, bytes]) -> Optional[float]:
        """Get the score of a member in a sorted set."""
        self._check_open()
        if self._mode == "server":
            return self._redis.zscore(key, member)
        member_bytes = self._encode_value(member)
        result = self._lib.redlite_zscore(
            self._handle, key.encode("utf-8"), member_bytes, len(member_bytes)
        )
        if math.isnan(result):
            return None
        return result

    def zcard(self, key: str) -> int:
        """Get the number of members in a sorted set."""
        self._check_open()
        if self._mode == "server":
            return self._redis.zcard(key)
        result = self._lib.redlite_zcard(self._handle, key.encode("utf-8"))
        if result < 0:
            _ffi.check_error()
        return result

    def zcount(self, key: str, min_score: float, max_score: float) -> int:
        """Count members with scores in the given range."""
        self._check_open()
        if self._mode == "server":
            return self._redis.zcount(key, min_score, max_score)
        result = self._lib.redlite_zcount(
            self._handle, key.encode("utf-8"), min_score, max_score
        )
        if result < 0:
            _ffi.check_error()
        return result

    def zincrby(self, key: str, amount: float, member: Union[str, bytes]) -> float:
        """Increment the score of a member in a sorted set."""
        self._check_open()
        if self._mode == "server":
            return self._redis.zincrby(key, amount, member)
        member_bytes = self._encode_value(member)
        result = self._lib.redlite_zincrby(
            self._handle, key.encode("utf-8"), amount, member_bytes, len(member_bytes)
        )
        if math.isnan(result):
            _ffi.check_error()
        return result

    # =========================================================================
    # Server Commands
    # =========================================================================

    def vacuum(self) -> int:
        """Compact the database, return bytes freed (embedded mode only)."""
        self._check_open()
        if self._mode == "server":
            return self._execute("VACUUM")
        result = self._lib.redlite_vacuum(self._handle)
        if result < 0:
            _ffi.check_error()
        return result

    @staticmethod
    def version() -> str:
        """Get the redlite library version."""
        lib = _ffi.get_lib()
        ffi = _ffi.get_ffi()
        result = lib.redlite_version()
        version = ffi.string(result).decode("utf-8")
        lib.redlite_free_string(result)
        return version
