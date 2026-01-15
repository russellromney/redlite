"""Basic tests for redlite Python SDK."""

import pytest
from redlite import Redlite, RedliteError


class TestBasic:
    """Basic functionality tests."""

    def test_open_memory(self):
        """Test opening in-memory database."""
        db = Redlite.open(":memory:")
        assert db is not None
        db.close()

    def test_context_manager(self):
        """Test context manager usage."""
        with Redlite.open(":memory:") as db:
            db.set("key", "value")
            assert db.get("key") == b"value"

    def test_version(self):
        """Test version string."""
        version = Redlite.version()
        assert version is not None
        assert "." in version


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

    def test_set_bytes(self, db):
        """Test SET with bytes."""
        db.set("key", b"\x00\x01\x02")
        assert db.get("key") == b"\x00\x01\x02"

    def test_incr_decr(self, db):
        """Test INCR and DECR."""
        db.set("counter", "10")
        assert db.incr("counter") == 11
        assert db.decr("counter") == 10
        assert db.incrby("counter", 5) == 15
        assert db.decrby("counter", 3) == 12

    def test_append(self, db):
        """Test APPEND."""
        db.set("key", "hello")
        length = db.append("key", " world")
        assert length == 11
        assert db.get("key") == b"hello world"

    def test_strlen(self, db):
        """Test STRLEN."""
        db.set("key", "hello")
        assert db.strlen("key") == 5
        assert db.strlen("nonexistent") == 0


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
        assert db.delete("key1", "key2") == 2
        assert db.get("key1") is None
        assert db.get("key2") is None

    def test_exists(self, db):
        """Test EXISTS."""
        db.set("key1", "value1")
        db.set("key2", "value2")
        assert db.exists("key1") == 1
        assert db.exists("key1", "key2") == 2
        assert db.exists("key1", "nonexistent") == 1

    def test_type(self, db):
        """Test TYPE."""
        db.set("string", "value")
        db.lpush("list", "item")
        db.sadd("set", "member")

        assert db.type("string") == "string"
        assert db.type("list") == "list"
        assert db.type("set") == "set"
        assert db.type("nonexistent") == "none"

    def test_keys(self, db):
        """Test KEYS."""
        db.set("user:1", "alice")
        db.set("user:2", "bob")
        db.set("post:1", "hello")

        users = db.keys("user:*")
        assert len(users) == 2
        assert "user:1" in users
        assert "user:2" in users

    def test_dbsize(self, db):
        """Test DBSIZE."""
        assert db.dbsize() == 0
        db.set("key1", "value1")
        assert db.dbsize() == 1
        db.set("key2", "value2")
        assert db.dbsize() == 2

    def test_flushdb(self, db):
        """Test FLUSHDB."""
        db.set("key1", "value1")
        db.set("key2", "value2")
        assert db.dbsize() == 2
        db.flushdb()
        assert db.dbsize() == 0


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

    def test_hdel(self, db):
        """Test HDEL."""
        db.hset("hash", field1="value1", field2="value2")
        assert db.hdel("hash", "field1") == 1
        assert db.hget("hash", "field1") is None
        assert db.hget("hash", "field2") == b"value2"

    def test_hexists(self, db):
        """Test HEXISTS."""
        db.hset("hash", field="value")
        assert db.hexists("hash", "field") is True
        assert db.hexists("hash", "nonexistent") is False

    def test_hlen(self, db):
        """Test HLEN."""
        db.hset("hash", field1="v1", field2="v2", field3="v3")
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
        db.hset("hash", counter="10")
        assert db.hincrby("hash", "counter", 5) == 15
        assert db.hincrby("hash", "counter", -3) == 12


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

    def test_lpop_rpop(self, db):
        """Test LPOP and RPOP."""
        db.rpush("list", "a", "b", "c")
        assert db.lpop("list") == b"a"
        assert db.rpop("list") == b"c"
        assert db.lrange("list", 0, -1) == [b"b"]

    def test_llen(self, db):
        """Test LLEN."""
        assert db.llen("list") == 0
        db.rpush("list", "a", "b", "c")
        assert db.llen("list") == 3

    def test_lindex(self, db):
        """Test LINDEX."""
        db.rpush("list", "a", "b", "c")
        assert db.lindex("list", 0) == b"a"
        assert db.lindex("list", 1) == b"b"
        assert db.lindex("list", -1) == b"c"
        assert db.lindex("list", 100) is None


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

    def test_srem(self, db):
        """Test SREM."""
        db.sadd("set", "a", "b", "c")
        assert db.srem("set", "a", "b") == 2
        assert db.smembers("set") == {b"c"}

    def test_sismember(self, db):
        """Test SISMEMBER."""
        db.sadd("set", "a", "b")
        assert db.sismember("set", "a") is True
        assert db.sismember("set", "c") is False

    def test_scard(self, db):
        """Test SCARD."""
        assert db.scard("set") == 0
        db.sadd("set", "a", "b", "c")
        assert db.scard("set") == 3


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

    def test_zrem(self, db):
        """Test ZREM."""
        db.zadd("zset", a=1.0, b=2.0, c=3.0)
        assert db.zrem("zset", "a", "b") == 2
        assert db.zcard("zset") == 1

    def test_zcard(self, db):
        """Test ZCARD."""
        assert db.zcard("zset") == 0
        db.zadd("zset", a=1.0, b=2.0)
        assert db.zcard("zset") == 2

    def test_zcount(self, db):
        """Test ZCOUNT."""
        db.zadd("zset", a=1.0, b=2.0, c=3.0, d=4.0)
        assert db.zcount("zset", 2.0, 3.0) == 2

    def test_zincrby(self, db):
        """Test ZINCRBY."""
        db.zadd("zset", member=10.0)
        assert db.zincrby("zset", 5.0, "member") == 15.0
        assert db.zscore("zset", "member") == 15.0
