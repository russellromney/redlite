"""Comprehensive configuration tests for redlite Python SDK."""

import os
import tempfile
import threading
import time
import pytest
from redlite import Redlite, RedliteError


class TestOpenConfigurations:
    """Test all database opening configurations."""

    def test_open_memory_default(self):
        """Test default memory open."""
        db = Redlite.open(":memory:")
        assert db is not None
        db.set("key", "value")
        assert db.get("key") == b"value"
        db.close()

    def test_open_memory_explicit(self):
        """Test explicit memory path."""
        db = Redlite.open(":memory:", cache_mb=64)
        assert db is not None
        db.close()

    def test_open_memory_small_cache(self):
        """Test with small cache (1MB)."""
        db = Redlite.open(":memory:", cache_mb=1)
        db.set("key", "value")
        assert db.get("key") == b"value"
        db.close()

    def test_open_memory_large_cache(self):
        """Test with large cache (512MB)."""
        db = Redlite.open(":memory:", cache_mb=512)
        db.set("key", "value")
        assert db.get("key") == b"value"
        db.close()

    def test_open_file_database(self):
        """Test file-based database."""
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            path = f.name

        try:
            db = Redlite.open(path)
            db.set("persistent", "data")
            db.close()

            # Reopen and verify data persisted
            db2 = Redlite.open(path)
            assert db2.get("persistent") == b"data"
            db2.close()
        finally:
            os.unlink(path)

    def test_open_file_with_cache(self):
        """Test file database with custom cache."""
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            path = f.name

        try:
            db = Redlite.open(path, cache_mb=128)
            db.set("key", "value")
            assert db.get("key") == b"value"
            db.close()
        finally:
            os.unlink(path)


class TestDatabaseIsolation:
    """Test database isolation."""

    def test_multiple_memory_databases(self):
        """Multiple in-memory databases are isolated."""
        db1 = Redlite.open(":memory:")
        db2 = Redlite.open(":memory:")

        db1.set("key", "value1")
        db2.set("key", "value2")

        assert db1.get("key") == b"value1"
        assert db2.get("key") == b"value2"

        db1.close()
        db2.close()

    def test_select_database_isolation(self):
        """SELECT isolates data between databases."""
        db = Redlite.open(":memory:")

        db.select(0)
        db.set("key", "in_db_0")

        db.select(1)
        assert db.get("key") is None
        db.set("key", "in_db_1")

        db.select(0)
        assert db.get("key") == b"in_db_0"

        db.select(1)
        assert db.get("key") == b"in_db_1"

        db.close()

    def test_flushdb_only_affects_current_db(self):
        """FLUSHDB only clears current database."""
        db = Redlite.open(":memory:")

        db.select(0)
        db.set("key0", "value0")

        db.select(1)
        db.set("key1", "value1")

        db.flushdb()  # Only flushes db 1

        assert db.get("key1") is None
        db.select(0)
        assert db.get("key0") == b"value0"

        db.close()


class TestDatabaseLifecycle:
    """Test database lifecycle operations."""

    def test_context_manager_closes(self):
        """Context manager properly closes database."""
        with Redlite.open(":memory:") as db:
            db.set("key", "value")

        # After context, database should be closed
        with pytest.raises(RedliteError):
            db.get("key")

    def test_close_is_idempotent(self):
        """Closing multiple times is safe."""
        db = Redlite.open(":memory:")
        db.close()
        db.close()
        db.close()

    def test_operations_after_close_raise_error(self):
        """All operations on closed database raise error."""
        db = Redlite.open(":memory:")
        db.close()

        with pytest.raises(RedliteError):
            db.get("key")
        with pytest.raises(RedliteError):
            db.set("key", "value")
        with pytest.raises(RedliteError):
            db.delete("key")
        with pytest.raises(RedliteError):
            db.lpush("list", "item")
        with pytest.raises(RedliteError):
            db.hset("hash", {"field": "value"})

    def test_destructor_closes(self):
        """Destructor closes database."""
        db = Redlite.open(":memory:")
        db.set("key", "value")
        del db  # Should trigger __del__ and close


class TestConcurrency:
    """Test concurrent access."""

    def test_concurrent_reads(self):
        """Concurrent reads from same database."""
        db = Redlite.open(":memory:")
        db.set("shared", "value")

        results = []

        def read_key():
            for _ in range(100):
                val = db.get("shared")
                results.append(val)

        threads = [threading.Thread(target=read_key) for _ in range(5)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert len(results) == 500
        assert all(r == b"value" for r in results)

        db.close()

    def test_concurrent_writes(self):
        """Concurrent writes to same database."""
        db = Redlite.open(":memory:")

        def write_keys(prefix):
            for i in range(100):
                db.set(f"{prefix}:{i}", f"value_{i}")

        threads = [threading.Thread(target=write_keys, args=(f"thread_{i}",)) for i in range(5)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert db.dbsize() == 500

        db.close()

    def test_concurrent_incr(self):
        """Concurrent INCR operations."""
        db = Redlite.open(":memory:")
        db.set("counter", "0")

        def increment():
            for _ in range(100):
                db.incr("counter")

        threads = [threading.Thread(target=increment) for _ in range(10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert db.get("counter") == b"1000"

        db.close()


class TestVersion:
    """Test version API."""

    def test_version_format(self):
        """Version has expected format."""
        version = Redlite.version()
        assert isinstance(version, str)
        assert "." in version
        parts = version.split(".")
        assert len(parts) >= 2
        assert all(p.isdigit() for p in parts[:2])

    def test_version_is_static(self):
        """Version can be called without instance."""
        v1 = Redlite.version()
        v2 = Redlite.version()
        assert v1 == v2
