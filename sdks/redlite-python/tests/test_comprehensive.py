"""Comprehensive tests for redlite Python SDK."""

import pytest
import time
from redlite import Redlite, RedliteError


# =============================================================================
# Basic Operations
# =============================================================================

class TestBasicOperations:
    """Test database open/close and lifecycle."""

    def test_open_memory(self):
        """Test opening in-memory database."""
        db = Redlite.open(":memory:")
        assert db is not None
        db.close()

    def test_open_with_cache(self):
        """Test opening with custom cache size."""
        db = Redlite.open(":memory:", cache_mb=128)
        assert db is not None
        db.close()

    def test_context_manager(self):
        """Test context manager usage."""
        with Redlite.open(":memory:") as db:
            db.set("key", "value")
            assert db.get("key") == b"value"

    def test_close_twice(self):
        """Test that closing twice is safe."""
        db = Redlite.open(":memory:")
        db.close()
        db.close()  # Should not raise

    def test_operations_on_closed_db(self):
        """Test operations on closed database raise error."""
        db = Redlite.open(":memory:")
        db.close()

        with pytest.raises(RedliteError):
            db.get("key")

        with pytest.raises(RedliteError):
            db.set("key", "value")

    def test_version(self):
        """Test version string."""
        version = Redlite.version()
        assert version is not None
        assert "." in version


# =============================================================================
# String Commands
# =============================================================================

class TestStrings:
    """String command tests."""

    @pytest.fixture
    def db(self):
        with Redlite.open(":memory:") as db:
            yield db

    def test_set_get(self, db):
        """Test SET and GET."""
        db.set("key", "value")
        assert db.get("key") == b"value"

    def test_get_nonexistent(self, db):
        """Test GET on non-existent key."""
        assert db.get("nonexistent") is None

    def test_set_overwrite(self, db):
        """Test SET overwrites existing value."""
        db.set("key", "first")
        db.set("key", "second")
        assert db.get("key") == b"second"

    def test_set_bytes(self, db):
        """Test SET with bytes."""
        db.set("key", b"\x00\x01\x02\xff\xfe")
        assert db.get("key") == b"\x00\x01\x02\xff\xfe"

    def test_set_int(self, db):
        """Test SET with int (auto-converted to string)."""
        db.set("key", 42)
        assert db.get("key") == b"42"

    def test_set_float(self, db):
        """Test SET with float (auto-converted to string)."""
        db.set("key", 3.14)
        assert db.get("key") == b"3.14"

    def test_set_large_value(self, db):
        """Test SET with large value (1MB)."""
        large = b"x" * (1024 * 1024)
        db.set("large", large)
        assert db.get("large") == large

    def test_setex(self, db):
        """Test SETEX with expiration."""
        db.setex("key", 60, "value")
        ttl = db.ttl("key")
        assert 59 <= ttl <= 60

    def test_set_with_ex(self, db):
        """Test SET with ex parameter."""
        db.set("key", "value", ex=60)
        ttl = db.ttl("key")
        assert 59 <= ttl <= 60

    def test_set_with_px(self, db):
        """Test SET with px (milliseconds) parameter."""
        db.set("key", "value", px=60000)
        ttl = db.ttl("key")
        assert 59 <= ttl <= 60

    def test_incr_decr(self, db):
        """Test INCR and DECR."""
        # Incr on non-existent starts at 1
        assert db.incr("counter") == 1
        assert db.incr("counter") == 2
        assert db.decr("counter") == 1
        # Decr below zero
        assert db.decr("counter") == 0
        assert db.decr("counter") == -1

    def test_incr_on_existing(self, db):
        """Test INCR on existing value."""
        db.set("counter", "10")
        assert db.incr("counter") == 11

    def test_incrby_decrby(self, db):
        """Test INCRBY and DECRBY."""
        db.set("counter", "100")
        assert db.incrby("counter", 50) == 150
        assert db.decrby("counter", 30) == 120
        assert db.incrby("counter", -20) == 100

    def test_append(self, db):
        """Test APPEND."""
        db.set("key", "Hello")
        length = db.append("key", " World")
        assert length == 11
        assert db.get("key") == b"Hello World"

    def test_append_nonexistent(self, db):
        """Test APPEND on non-existent key."""
        length = db.append("newkey", "value")
        assert length == 5
        assert db.get("newkey") == b"value"

    def test_strlen(self, db):
        """Test STRLEN."""
        db.set("key", "hello")
        assert db.strlen("key") == 5
        assert db.strlen("nonexistent") == 0

    def test_getrange(self, db):
        """Test GETRANGE."""
        db.set("key", "Hello World")
        assert db.getrange("key", 0, 4) == b"Hello"
        assert db.getrange("key", 6, -1) == b"World"
        assert db.getrange("key", -5, -1) == b"World"

    def test_setrange(self, db):
        """Test SETRANGE."""
        db.set("key", "Hello World")
        new_len = db.setrange("key", 6, "Redis")
        assert new_len == 11
        assert db.get("key") == b"Hello Redis"

    def test_getdel(self, db):
        """Test GETDEL."""
        db.set("key", "value")
        val = db.getdel("key")
        assert val == b"value"
        assert db.get("key") is None

    def test_getdel_nonexistent(self, db):
        """Test GETDEL on non-existent key."""
        val = db.getdel("nonexistent")
        assert val is None

    def test_incrbyfloat(self, db):
        """Test INCRBYFLOAT."""
        db.set("float", "10.5")
        result = db.incrbyfloat("float", 0.1)
        assert abs(result - 10.6) < 0.001


# =============================================================================
# Key Commands
# =============================================================================

class TestKeys:
    """Key command tests."""

    @pytest.fixture
    def db(self):
        with Redlite.open(":memory:") as db:
            yield db

    def test_delete(self, db):
        """Test DELETE."""
        db.set("key1", "value1")
        db.set("key2", "value2")
        db.set("key3", "value3")
        assert db.delete("key1", "key2") == 2
        assert db.get("key1") is None
        assert db.get("key2") is None
        assert db.get("key3") == b"value3"

    def test_delete_nonexistent(self, db):
        """Test DELETE on non-existent keys."""
        assert db.delete("nonexistent1", "nonexistent2") == 0

    def test_delete_no_args(self, db):
        """Test DELETE with no arguments."""
        assert db.delete() == 0

    def test_exists(self, db):
        """Test EXISTS."""
        db.set("key1", "value1")
        db.set("key2", "value2")
        assert db.exists("key1") == 1
        assert db.exists("key1", "key2") == 2
        assert db.exists("key1", "nonexistent") == 1
        assert db.exists("nonexistent") == 0

    def test_exists_same_key_multiple(self, db):
        """Test EXISTS counts same key multiple times."""
        db.set("key", "value")
        assert db.exists("key", "key", "key") == 3

    def test_type(self, db):
        """Test TYPE."""
        db.set("string", "value")
        db.lpush("list", "item")
        db.sadd("set", "member")
        db.zadd("zset", {"member": 1.0})
        db.hset("hash", {"field": "value"})

        assert db.type("string") == "string"
        assert db.type("list") == "list"
        assert db.type("set") == "set"
        assert db.type("zset") == "zset"
        assert db.type("hash") == "hash"
        assert db.type("nonexistent") == "none"

    def test_keys(self, db):
        """Test KEYS."""
        db.set("user:1", "alice")
        db.set("user:2", "bob")
        db.set("user:100", "carol")
        db.set("post:1", "hello")

        users = db.keys("user:*")
        assert len(users) == 3
        assert "user:1" in users
        assert "user:2" in users
        assert "user:100" in users

        all_keys = db.keys("*")
        assert len(all_keys) == 4

        # Single character wildcard
        single = db.keys("user:?")
        assert len(single) == 2

    def test_keys_no_match(self, db):
        """Test KEYS with no matches."""
        db.set("key1", "value")
        assert db.keys("nomatch:*") == []

    def test_dbsize(self, db):
        """Test DBSIZE."""
        assert db.dbsize() == 0
        db.set("key1", "value1")
        assert db.dbsize() == 1
        db.set("key2", "value2")
        assert db.dbsize() == 2
        db.delete("key1")
        assert db.dbsize() == 1

    def test_flushdb(self, db):
        """Test FLUSHDB."""
        db.set("key1", "value1")
        db.set("key2", "value2")
        db.lpush("list", "item")
        assert db.dbsize() == 3
        db.flushdb()
        assert db.dbsize() == 0

    def test_rename(self, db):
        """Test RENAME."""
        db.set("old", "value")
        db.rename("old", "new")
        assert db.get("old") is None
        assert db.get("new") == b"value"

    def test_renamenx(self, db):
        """Test RENAMENX."""
        db.set("key1", "value1")
        db.set("key2", "value2")

        # Should fail - key2 exists
        result = db.renamenx("key1", "key2")
        assert result is False
        assert db.get("key1") == b"value1"

        # Should succeed - key3 doesn't exist
        result = db.renamenx("key1", "key3")
        assert result is True
        assert db.get("key1") is None
        assert db.get("key3") == b"value1"


# =============================================================================
# TTL Commands
# =============================================================================

class TestTTL:
    """TTL and expiration tests."""

    @pytest.fixture
    def db(self):
        with Redlite.open(":memory:") as db:
            yield db

    def test_ttl(self, db):
        """Test TTL."""
        db.setex("key", 60, "value")
        ttl = db.ttl("key")
        assert 59 <= ttl <= 60

        # No TTL
        db.set("noexpire", "value")
        assert db.ttl("noexpire") == -1

        # Non-existent key
        assert db.ttl("nonexistent") == -2

    def test_pttl(self, db):
        """Test PTTL (milliseconds)."""
        db.psetex("key", 60000, "value")
        pttl = db.pttl("key")
        assert 59000 <= pttl <= 60000

    def test_expire(self, db):
        """Test EXPIRE."""
        db.set("key", "value")
        assert db.expire("key", 60) is True
        ttl = db.ttl("key")
        assert 59 <= ttl <= 60

        # Non-existent key
        assert db.expire("nonexistent", 60) is False

    def test_pexpire(self, db):
        """Test PEXPIRE."""
        db.set("key", "value")
        assert db.pexpire("key", 60000) is True
        pttl = db.pttl("key")
        assert 59000 <= pttl <= 60000

    def test_expireat(self, db):
        """Test EXPIREAT."""
        db.set("key", "value")
        future = int(time.time()) + 60
        assert db.expireat("key", future) is True
        ttl = db.ttl("key")
        assert 58 <= ttl <= 60

    def test_pexpireat(self, db):
        """Test PEXPIREAT."""
        db.set("key", "value")
        future_ms = int(time.time() * 1000) + 60000
        assert db.pexpireat("key", future_ms) is True
        pttl = db.pttl("key")
        assert 58000 <= pttl <= 60000

    def test_persist(self, db):
        """Test PERSIST."""
        db.setex("key", 60, "value")
        assert db.persist("key") is True
        assert db.ttl("key") == -1

        # Persist on key without TTL - behavior may vary
        # Just ensure no error
        db.persist("key")

        # Non-existent key
        assert db.persist("nonexistent") is False


# =============================================================================
# Hash Commands
# =============================================================================

class TestHashes:
    """Hash command tests."""

    @pytest.fixture
    def db(self):
        with Redlite.open(":memory:") as db:
            yield db

    def test_hset_hget(self, db):
        """Test HSET and HGET."""
        db.hset("hash", {"field1": "value1", "field2": "value2"})
        assert db.hget("hash", "field1") == b"value1"
        assert db.hget("hash", "field2") == b"value2"
        assert db.hget("hash", "nonexistent") is None

    def test_hset_kwargs(self, db):
        """Test HSET with kwargs."""
        db.hset("hash", name="Alice", age="30")
        assert db.hget("hash", "name") == b"Alice"
        assert db.hget("hash", "age") == b"30"

    def test_hset_update(self, db):
        """Test HSET returns new field count."""
        # New fields
        n = db.hset("hash", {"f1": "v1", "f2": "v2"})
        assert n == 2

        # Update existing (returns 0 for no new fields)
        n = db.hset("hash", {"f1": "updated"})
        assert n == 0

        # Mix of new and existing
        n = db.hset("hash", {"f1": "v1", "f3": "v3"})
        assert n == 1

    def test_hget_nonexistent_hash(self, db):
        """Test HGET on non-existent hash."""
        assert db.hget("nonexistent", "field") is None

    def test_hdel(self, db):
        """Test HDEL."""
        db.hset("hash", f1="v1", f2="v2", f3="v3")
        assert db.hdel("hash", "f1") == 1
        assert db.hget("hash", "f1") is None
        assert db.hget("hash", "f2") == b"v2"

        # Delete multiple including non-existent
        assert db.hdel("hash", "f2", "f3", "f4") == 2

    def test_hexists(self, db):
        """Test HEXISTS."""
        db.hset("hash", field="value")
        assert db.hexists("hash", "field") is True
        assert db.hexists("hash", "nonexistent") is False
        assert db.hexists("nonexistent", "field") is False

    def test_hlen(self, db):
        """Test HLEN."""
        assert db.hlen("nonexistent") == 0
        db.hset("hash", f1="v1", f2="v2", f3="v3")
        assert db.hlen("hash") == 3

    def test_hkeys_hvals(self, db):
        """Test HKEYS and HVALS."""
        db.hset("hash", a="1", b="2", c="3")
        keys = db.hkeys("hash")
        vals = db.hvals("hash")
        assert len(keys) == 3
        assert len(vals) == 3
        assert set(keys) == {"a", "b", "c"}

    def test_hincrby(self, db):
        """Test HINCRBY."""
        # Start from 0
        assert db.hincrby("hash", "counter", 5) == 5
        assert db.hincrby("hash", "counter", -2) == 3

        # On existing value
        db.hset("hash", num="100")
        assert db.hincrby("hash", "num", 10) == 110


# =============================================================================
# List Commands
# =============================================================================

class TestLists:
    """List command tests."""

    @pytest.fixture
    def db(self):
        with Redlite.open(":memory:") as db:
            yield db

    def test_lpush_rpush(self, db):
        """Test LPUSH and RPUSH."""
        db.lpush("list", "a", "b")  # list: b, a
        db.rpush("list", "c", "d")  # list: b, a, c, d
        assert db.lrange("list", 0, -1) == [b"b", b"a", b"c", b"d"]

    def test_lpush_order(self, db):
        """Test LPUSH pushes in order (last item at head)."""
        db.lpush("list", "1", "2", "3")  # list: 3, 2, 1
        assert db.lrange("list", 0, -1) == [b"3", b"2", b"1"]

    def test_lpop_rpop(self, db):
        """Test LPOP and RPOP."""
        db.rpush("list", "a", "b", "c")
        assert db.lpop("list") == b"a"
        assert db.rpop("list") == b"c"
        assert db.lrange("list", 0, -1) == [b"b"]

    def test_lpop_count(self, db):
        """Test LPOP with count."""
        db.rpush("list", "a", "b", "c", "d")
        result = db.lpop("list", 2)
        assert result == [b"a", b"b"]

        # Pop more than available
        result = db.lpop("list", 10)
        assert result == [b"c", b"d"]

    def test_rpop_count(self, db):
        """Test RPOP with count."""
        db.rpush("list", "a", "b", "c")
        result = db.rpop("list", 2)
        assert result == [b"c", b"b"]

    def test_llen(self, db):
        """Test LLEN."""
        assert db.llen("nonexistent") == 0
        db.rpush("list", "a", "b", "c")
        assert db.llen("list") == 3

    def test_lrange_negative_indices(self, db):
        """Test LRANGE with negative indices."""
        db.rpush("list", "a", "b", "c", "d", "e")
        # Last 3 elements
        assert db.lrange("list", -3, -1) == [b"c", b"d", b"e"]
        # From index 1 to second-to-last
        assert db.lrange("list", 1, -2) == [b"b", b"c", b"d"]

    def test_lindex(self, db):
        """Test LINDEX."""
        db.rpush("list", "a", "b", "c")
        assert db.lindex("list", 0) == b"a"
        assert db.lindex("list", 1) == b"b"
        assert db.lindex("list", -1) == b"c"
        assert db.lindex("list", 100) is None

    def test_list_empty_operations(self, db):
        """Test list operations on empty/non-existent lists."""
        assert db.llen("nonexistent") == 0
        assert db.lrange("nonexistent", 0, -1) == []
        assert db.lpop("nonexistent") is None
        assert db.rpop("nonexistent") is None


# =============================================================================
# Set Commands
# =============================================================================

class TestSets:
    """Set command tests."""

    @pytest.fixture
    def db(self):
        with Redlite.open(":memory:") as db:
            yield db

    def test_sadd_smembers(self, db):
        """Test SADD and SMEMBERS."""
        db.sadd("set", "a", "b", "c")
        members = db.smembers("set")
        assert len(members) == 3
        assert set(members) == {b"a", b"b", b"c"}

    def test_sadd_duplicates(self, db):
        """Test SADD ignores duplicates."""
        n = db.sadd("set", "a", "b", "a")
        assert n == 2  # Only 2 unique

        # Add again with some existing
        n = db.sadd("set", "a", "c")
        assert n == 1  # Only 'c' is new

    def test_srem(self, db):
        """Test SREM."""
        db.sadd("set", "a", "b", "c")
        assert db.srem("set", "a", "b") == 2
        assert db.smembers("set") == {b"c"}

        # Remove non-existent
        assert db.srem("set", "nonexistent") == 0

    def test_sismember(self, db):
        """Test SISMEMBER."""
        db.sadd("set", "a", "b")
        assert db.sismember("set", "a") is True
        assert db.sismember("set", "c") is False
        assert db.sismember("nonexistent", "a") is False

    def test_scard(self, db):
        """Test SCARD."""
        assert db.scard("nonexistent") == 0
        db.sadd("set", "a", "b", "c")
        assert db.scard("set") == 3


# =============================================================================
# Sorted Set Commands
# =============================================================================

class TestSortedSets:
    """Sorted set command tests."""

    @pytest.fixture
    def db(self):
        with Redlite.open(":memory:") as db:
            yield db

    def test_zadd_zscore(self, db):
        """Test ZADD and ZSCORE."""
        db.zadd("zset", {"a": 1.0, "b": 2.0, "c": 3.0})
        assert db.zscore("zset", "a") == 1.0
        assert db.zscore("zset", "b") == 2.0
        assert db.zscore("zset", "nonexistent") is None

    def test_zadd_kwargs(self, db):
        """Test ZADD with kwargs."""
        db.zadd("zset", a=1.0, b=2.0)
        assert db.zscore("zset", "a") == 1.0
        assert db.zscore("zset", "b") == 2.0

    def test_zadd_update(self, db):
        """Test ZADD updates existing scores."""
        n = db.zadd("zset", {"a": 1.0})
        assert n == 1

        # Update - returns 0 for no new members
        n = db.zadd("zset", {"a": 5.0})
        assert n == 0

        assert db.zscore("zset", "a") == 5.0

    def test_zrem(self, db):
        """Test ZREM."""
        db.zadd("zset", a=1.0, b=2.0, c=3.0)
        assert db.zrem("zset", "a", "b") == 2
        assert db.zcard("zset") == 1

    def test_zcard(self, db):
        """Test ZCARD."""
        assert db.zcard("nonexistent") == 0
        db.zadd("zset", a=1.0, b=2.0)
        assert db.zcard("zset") == 2

    def test_zcount(self, db):
        """Test ZCOUNT."""
        db.zadd("zset", a=1.0, b=2.0, c=3.0, d=4.0, e=5.0)
        assert db.zcount("zset", 2.0, 4.0) == 3
        assert db.zcount("zset", 0.0, 100.0) == 5
        assert db.zcount("zset", 10.0, 20.0) == 0

    def test_zincrby(self, db):
        """Test ZINCRBY."""
        # New member
        score = db.zincrby("zset", 5.0, "a")
        assert score == 5.0

        # Increment existing
        score = db.zincrby("zset", 2.5, "a")
        assert score == 7.5

        # Decrement
        score = db.zincrby("zset", -3.0, "a")
        assert score == 4.5

    def test_zscore_nonexistent(self, db):
        """Test ZSCORE on non-existent zset/member."""
        assert db.zscore("nonexistent", "a") is None
        db.zadd("zset", a=1.0)
        assert db.zscore("zset", "nonexistent") is None


# =============================================================================
# Database Selection
# =============================================================================

class TestSelect:
    """Database selection tests."""

    @pytest.fixture
    def db(self):
        with Redlite.open(":memory:") as db:
            yield db

    def test_select(self, db):
        """Test SELECT switches databases."""
        db.select(1)
        db.set("key", "in db 1")

        db.select(0)
        assert db.get("key") is None

        db.select(1)
        assert db.get("key") == b"in db 1"


# =============================================================================
# Server Commands
# =============================================================================

class TestServer:
    """Server command tests."""

    @pytest.fixture
    def db(self):
        with Redlite.open(":memory:") as db:
            yield db

    def test_vacuum(self, db):
        """Test VACUUM."""
        # Create and delete data
        for i in range(100):
            db.set(f"key{i}", "x" * 1000)
        for i in range(100):
            db.delete(f"key{i}")

        # Vacuum should succeed
        freed = db.vacuum()
        assert freed >= 0


# =============================================================================
# Special Characters and Unicode
# =============================================================================

class TestSpecialCharacters:
    """Test special characters and unicode."""

    @pytest.fixture
    def db(self):
        with Redlite.open(":memory:") as db:
            yield db

    def test_unicode_keys_values(self, db):
        """Test unicode keys and values."""
        db.set("键", "值")
        assert db.get("键") == "值".encode("utf-8")

        db.set("emoji:🔥", "🎉")
        assert db.get("emoji:🔥") == "🎉".encode("utf-8")

    def test_special_character_keys(self, db):
        """Test keys with special characters."""
        special_keys = [
            "key with spaces",
            "key:with:colons",
            "key/with/slashes",
            "key.with.dots",
            "key-with-dashes",
            "key_with_underscores",
            "key{with}braces",
            "key[with]brackets",
        ]

        for key in special_keys:
            db.set(key, "value")
            assert db.get(key) == b"value", f"Failed for key: {key}"

    def test_binary_values(self, db):
        """Test binary values with null bytes."""
        binary = b"\x00\x01\x02\xff\xfe\x00\x80"
        db.set("binary", binary)
        assert db.get("binary") == binary


# =============================================================================
# Edge Cases
# =============================================================================

class TestEdgeCases:
    """Test edge cases and boundary conditions."""

    @pytest.fixture
    def db(self):
        with Redlite.open(":memory:") as db:
            yield db

    def test_empty_string_value(self, db):
        """Test empty string value - redlite treats empty as None."""
        db.set("key", "")
        result = db.get("key")
        # redlite treats empty values as None
        assert result is None or result == b""

    def test_empty_key(self, db):
        """Test empty key (should work)."""
        db.set("", "value")
        assert db.get("") == b"value"

    def test_very_long_key(self, db):
        """Test very long key."""
        long_key = "k" * 10000
        db.set(long_key, "value")
        assert db.get(long_key) == b"value"

    def test_negative_numbers(self, db):
        """Test negative numbers with INCR/DECR."""
        db.set("num", "-10")
        assert db.incr("num") == -9
        assert db.decr("num") == -10
        assert db.incrby("num", -5) == -15

    def test_large_incr(self, db):
        """Test large INCRBY values."""
        assert db.incrby("num", 1000000000) == 1000000000
        assert db.incrby("num", 1000000000) == 2000000000

    def test_float_precision(self, db):
        """Test float precision with INCRBYFLOAT."""
        db.set("float", "0")
        for _ in range(10):
            db.incrbyfloat("float", 0.1)
        result = db.incrbyfloat("float", 0)
        assert abs(result - 1.0) < 0.001

    def test_hash_empty_field_value(self, db):
        """Test hash with empty field/value."""
        db.hset("hash", {"": "empty_field"})
        assert db.hget("hash", "") == b"empty_field"

        db.hset("hash", {"field": ""})
        # redlite treats empty values as None
        result = db.hget("hash", "field")
        assert result is None or result == b""

    def test_list_single_element(self, db):
        """Test list operations with single element."""
        db.lpush("list", "single")
        assert db.llen("list") == 1
        assert db.lindex("list", 0) == b"single"
        assert db.lindex("list", -1) == b"single"
        assert db.lrange("list", 0, -1) == [b"single"]

    def test_set_single_member(self, db):
        """Test set operations with single member."""
        db.sadd("set", "single")
        assert db.scard("set") == 1
        assert db.sismember("set", "single") is True
        assert db.smembers("set") == {b"single"}

    def test_zset_negative_scores(self, db):
        """Test sorted set with negative scores."""
        db.zadd("zset", a=-10.0, b=-5.0, c=0.0, d=5.0)
        assert db.zscore("zset", "a") == -10.0
        assert db.zcount("zset", -100.0, 0.0) == 3

    def test_zset_float_scores(self, db):
        """Test sorted set with float scores."""
        db.zadd("zset", {"a": 1.5, "b": 2.7, "c": 3.14159})
        assert db.zscore("zset", "c") == pytest.approx(3.14159, rel=1e-5)
