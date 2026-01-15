"""Comprehensive collection command tests for redlite Python SDK."""

import pytest
from redlite import Redlite, RedliteError


# =============================================================================
# Hash Commands
# =============================================================================

class TestHset:
    """HSET command tests."""

    @pytest.fixture
    def db(self):
        with Redlite.open(":memory:") as db:
            yield db

    def test_hset_dict(self, db):
        count = db.hset("hash", {"field1": "value1", "field2": "value2"})
        assert count == 2

    def test_hset_kwargs(self, db):
        count = db.hset("hash", name="Alice", age="30")
        assert count == 2

    def test_hset_mixed(self, db):
        count = db.hset("hash", {"field1": "value1"}, field2="value2")
        assert count == 2

    def test_hset_update_returns_zero(self, db):
        db.hset("hash", {"field": "value1"})
        count = db.hset("hash", {"field": "value2"})
        assert count == 0  # No new fields

    def test_hset_mixed_new_and_update(self, db):
        db.hset("hash", {"field1": "value1"})
        count = db.hset("hash", {"field1": "updated", "field2": "new"})
        assert count == 1  # Only field2 is new

    def test_hset_bytes_value(self, db):
        db.hset("hash", {"field": b"\x00\x01\x02"})
        assert db.hget("hash", "field") == b"\x00\x01\x02"


class TestHget:
    """HGET command tests."""

    @pytest.fixture
    def db(self):
        with Redlite.open(":memory:") as db:
            yield db

    def test_hget_existing(self, db):
        db.hset("hash", {"field": "value"})
        assert db.hget("hash", "field") == b"value"

    def test_hget_nonexistent_field(self, db):
        db.hset("hash", {"field": "value"})
        assert db.hget("hash", "nonexistent") is None

    def test_hget_nonexistent_hash(self, db):
        assert db.hget("nonexistent", "field") is None


class TestHdel:
    """HDEL command tests."""

    @pytest.fixture
    def db(self):
        with Redlite.open(":memory:") as db:
            yield db

    def test_hdel_single(self, db):
        db.hset("hash", {"f1": "v1", "f2": "v2"})
        count = db.hdel("hash", "f1")
        assert count == 1
        assert db.hget("hash", "f1") is None
        assert db.hget("hash", "f2") == b"v2"

    def test_hdel_multiple(self, db):
        db.hset("hash", {"f1": "v1", "f2": "v2", "f3": "v3"})
        count = db.hdel("hash", "f1", "f2")
        assert count == 2

    def test_hdel_nonexistent(self, db):
        db.hset("hash", {"field": "value"})
        count = db.hdel("hash", "nonexistent")
        assert count == 0


class TestHexists:
    """HEXISTS command tests."""

    @pytest.fixture
    def db(self):
        with Redlite.open(":memory:") as db:
            yield db

    def test_hexists_true(self, db):
        db.hset("hash", {"field": "value"})
        assert db.hexists("hash", "field") is True

    def test_hexists_false(self, db):
        db.hset("hash", {"field": "value"})
        assert db.hexists("hash", "other") is False

    def test_hexists_nonexistent_hash(self, db):
        assert db.hexists("nonexistent", "field") is False


class TestHlen:
    """HLEN command tests."""

    @pytest.fixture
    def db(self):
        with Redlite.open(":memory:") as db:
            yield db

    def test_hlen(self, db):
        db.hset("hash", {"f1": "v1", "f2": "v2", "f3": "v3"})
        assert db.hlen("hash") == 3

    def test_hlen_empty(self, db):
        assert db.hlen("nonexistent") == 0


class TestHkeysHvals:
    """HKEYS and HVALS command tests."""

    @pytest.fixture
    def db(self):
        with Redlite.open(":memory:") as db:
            yield db

    def test_hkeys(self, db):
        db.hset("hash", {"a": "1", "b": "2", "c": "3"})
        keys = db.hkeys("hash")
        assert len(keys) == 3
        assert set(keys) == {"a", "b", "c"}

    def test_hvals(self, db):
        db.hset("hash", {"a": "1", "b": "2", "c": "3"})
        vals = db.hvals("hash")
        assert len(vals) == 3

    def test_hkeys_empty(self, db):
        assert db.hkeys("nonexistent") == []

    def test_hvals_empty(self, db):
        assert db.hvals("nonexistent") == []


class TestHincrby:
    """HINCRBY command tests."""

    @pytest.fixture
    def db(self):
        with Redlite.open(":memory:") as db:
            yield db

    def test_hincrby_existing(self, db):
        db.hset("hash", {"counter": "10"})
        result = db.hincrby("hash", "counter", 5)
        assert result == 15

    def test_hincrby_nonexistent_field(self, db):
        result = db.hincrby("hash", "counter", 5)
        assert result == 5

    def test_hincrby_negative(self, db):
        db.hset("hash", {"counter": "10"})
        result = db.hincrby("hash", "counter", -3)
        assert result == 7


# =============================================================================
# List Commands
# =============================================================================

class TestLpush:
    """LPUSH command tests."""

    @pytest.fixture
    def db(self):
        with Redlite.open(":memory:") as db:
            yield db

    def test_lpush_single(self, db):
        length = db.lpush("list", "a")
        assert length == 1

    def test_lpush_multiple(self, db):
        length = db.lpush("list", "a", "b", "c")
        assert length == 3

    def test_lpush_order(self, db):
        """LPUSH adds at head, last arg is first."""
        db.lpush("list", "1", "2", "3")
        items = db.lrange("list", 0, -1)
        assert items == [b"3", b"2", b"1"]


class TestRpush:
    """RPUSH command tests."""

    @pytest.fixture
    def db(self):
        with Redlite.open(":memory:") as db:
            yield db

    def test_rpush_single(self, db):
        length = db.rpush("list", "a")
        assert length == 1

    def test_rpush_multiple(self, db):
        length = db.rpush("list", "a", "b", "c")
        assert length == 3

    def test_rpush_order(self, db):
        """RPUSH adds at tail."""
        db.rpush("list", "1", "2", "3")
        items = db.lrange("list", 0, -1)
        assert items == [b"1", b"2", b"3"]


class TestLpop:
    """LPOP command tests."""

    @pytest.fixture
    def db(self):
        with Redlite.open(":memory:") as db:
            yield db

    def test_lpop_single(self, db):
        db.rpush("list", "a", "b", "c")
        result = db.lpop("list")
        assert result == b"a"

    def test_lpop_count(self, db):
        db.rpush("list", "a", "b", "c")
        result = db.lpop("list", 2)
        assert result == [b"a", b"b"]

    def test_lpop_more_than_exists(self, db):
        db.rpush("list", "a", "b")
        result = db.lpop("list", 10)
        assert result == [b"a", b"b"]

    def test_lpop_empty(self, db):
        result = db.lpop("nonexistent")
        assert result is None


class TestRpop:
    """RPOP command tests."""

    @pytest.fixture
    def db(self):
        with Redlite.open(":memory:") as db:
            yield db

    def test_rpop_single(self, db):
        db.rpush("list", "a", "b", "c")
        result = db.rpop("list")
        assert result == b"c"

    def test_rpop_count(self, db):
        db.rpush("list", "a", "b", "c")
        result = db.rpop("list", 2)
        assert result == [b"c", b"b"]


class TestLlen:
    """LLEN command tests."""

    @pytest.fixture
    def db(self):
        with Redlite.open(":memory:") as db:
            yield db

    def test_llen(self, db):
        db.rpush("list", "a", "b", "c")
        assert db.llen("list") == 3

    def test_llen_empty(self, db):
        assert db.llen("nonexistent") == 0


class TestLrange:
    """LRANGE command tests."""

    @pytest.fixture
    def db(self):
        with Redlite.open(":memory:") as db:
            yield db

    def test_lrange_all(self, db):
        db.rpush("list", "a", "b", "c", "d", "e")
        result = db.lrange("list", 0, -1)
        assert result == [b"a", b"b", b"c", b"d", b"e"]

    def test_lrange_subset(self, db):
        db.rpush("list", "a", "b", "c", "d", "e")
        result = db.lrange("list", 1, 3)
        assert result == [b"b", b"c", b"d"]

    def test_lrange_negative(self, db):
        db.rpush("list", "a", "b", "c", "d", "e")
        result = db.lrange("list", -3, -1)
        assert result == [b"c", b"d", b"e"]

    def test_lrange_beyond(self, db):
        db.rpush("list", "a", "b")
        result = db.lrange("list", 0, 100)
        assert result == [b"a", b"b"]


class TestLindex:
    """LINDEX command tests."""

    @pytest.fixture
    def db(self):
        with Redlite.open(":memory:") as db:
            yield db

    def test_lindex_positive(self, db):
        db.rpush("list", "a", "b", "c")
        assert db.lindex("list", 0) == b"a"
        assert db.lindex("list", 1) == b"b"
        assert db.lindex("list", 2) == b"c"

    def test_lindex_negative(self, db):
        db.rpush("list", "a", "b", "c")
        assert db.lindex("list", -1) == b"c"
        assert db.lindex("list", -2) == b"b"

    def test_lindex_out_of_range(self, db):
        db.rpush("list", "a", "b")
        assert db.lindex("list", 100) is None


# =============================================================================
# Set Commands
# =============================================================================

class TestSadd:
    """SADD command tests."""

    @pytest.fixture
    def db(self):
        with Redlite.open(":memory:") as db:
            yield db

    def test_sadd_single(self, db):
        count = db.sadd("set", "a")
        assert count == 1

    def test_sadd_multiple(self, db):
        count = db.sadd("set", "a", "b", "c")
        assert count == 3

    def test_sadd_duplicates(self, db):
        count = db.sadd("set", "a", "a", "b")
        assert count == 2  # Only 2 unique

    def test_sadd_existing_member(self, db):
        db.sadd("set", "a")
        count = db.sadd("set", "a", "b")
        assert count == 1  # Only b is new


class TestSrem:
    """SREM command tests."""

    @pytest.fixture
    def db(self):
        with Redlite.open(":memory:") as db:
            yield db

    def test_srem_single(self, db):
        db.sadd("set", "a", "b", "c")
        count = db.srem("set", "a")
        assert count == 1

    def test_srem_multiple(self, db):
        db.sadd("set", "a", "b", "c")
        count = db.srem("set", "a", "b")
        assert count == 2

    def test_srem_nonexistent(self, db):
        db.sadd("set", "a")
        count = db.srem("set", "nonexistent")
        assert count == 0


class TestSmembers:
    """SMEMBERS command tests."""

    @pytest.fixture
    def db(self):
        with Redlite.open(":memory:") as db:
            yield db

    def test_smembers(self, db):
        db.sadd("set", "a", "b", "c")
        members = db.smembers("set")
        assert len(members) == 3
        assert set(members) == {b"a", b"b", b"c"}

    def test_smembers_empty(self, db):
        assert db.smembers("nonexistent") == set()


class TestSismember:
    """SISMEMBER command tests."""

    @pytest.fixture
    def db(self):
        with Redlite.open(":memory:") as db:
            yield db

    def test_sismember_true(self, db):
        db.sadd("set", "a", "b")
        assert db.sismember("set", "a") is True

    def test_sismember_false(self, db):
        db.sadd("set", "a", "b")
        assert db.sismember("set", "c") is False

    def test_sismember_nonexistent_set(self, db):
        assert db.sismember("nonexistent", "a") is False


class TestScard:
    """SCARD command tests."""

    @pytest.fixture
    def db(self):
        with Redlite.open(":memory:") as db:
            yield db

    def test_scard(self, db):
        db.sadd("set", "a", "b", "c")
        assert db.scard("set") == 3

    def test_scard_empty(self, db):
        assert db.scard("nonexistent") == 0


# =============================================================================
# Sorted Set Commands
# =============================================================================

class TestZadd:
    """ZADD command tests."""

    @pytest.fixture
    def db(self):
        with Redlite.open(":memory:") as db:
            yield db

    def test_zadd_dict(self, db):
        count = db.zadd("zset", {"a": 1.0, "b": 2.0, "c": 3.0})
        assert count == 3

    def test_zadd_kwargs(self, db):
        count = db.zadd("zset", a=1.0, b=2.0)
        assert count == 2

    def test_zadd_update_returns_zero(self, db):
        db.zadd("zset", {"a": 1.0})
        count = db.zadd("zset", {"a": 5.0})
        assert count == 0  # No new members

    def test_zadd_updates_score(self, db):
        db.zadd("zset", {"a": 1.0})
        db.zadd("zset", {"a": 5.0})
        assert db.zscore("zset", "a") == 5.0


class TestZrem:
    """ZREM command tests."""

    @pytest.fixture
    def db(self):
        with Redlite.open(":memory:") as db:
            yield db

    def test_zrem_single(self, db):
        db.zadd("zset", {"a": 1.0, "b": 2.0})
        count = db.zrem("zset", "a")
        assert count == 1

    def test_zrem_multiple(self, db):
        db.zadd("zset", {"a": 1.0, "b": 2.0, "c": 3.0})
        count = db.zrem("zset", "a", "b")
        assert count == 2


class TestZscore:
    """ZSCORE command tests."""

    @pytest.fixture
    def db(self):
        with Redlite.open(":memory:") as db:
            yield db

    def test_zscore_existing(self, db):
        db.zadd("zset", {"a": 1.5})
        assert db.zscore("zset", "a") == 1.5

    def test_zscore_nonexistent_member(self, db):
        db.zadd("zset", {"a": 1.0})
        assert db.zscore("zset", "nonexistent") is None

    def test_zscore_nonexistent_zset(self, db):
        assert db.zscore("nonexistent", "a") is None


class TestZcard:
    """ZCARD command tests."""

    @pytest.fixture
    def db(self):
        with Redlite.open(":memory:") as db:
            yield db

    def test_zcard(self, db):
        db.zadd("zset", {"a": 1.0, "b": 2.0})
        assert db.zcard("zset") == 2

    def test_zcard_empty(self, db):
        assert db.zcard("nonexistent") == 0


class TestZcount:
    """ZCOUNT command tests."""

    @pytest.fixture
    def db(self):
        with Redlite.open(":memory:") as db:
            yield db

    def test_zcount_all(self, db):
        db.zadd("zset", {"a": 1.0, "b": 2.0, "c": 3.0, "d": 4.0})
        assert db.zcount("zset", 0, 100) == 4

    def test_zcount_range(self, db):
        db.zadd("zset", {"a": 1.0, "b": 2.0, "c": 3.0, "d": 4.0})
        assert db.zcount("zset", 2.0, 3.0) == 2

    def test_zcount_none(self, db):
        db.zadd("zset", {"a": 1.0})
        assert db.zcount("zset", 10.0, 20.0) == 0


class TestZincrby:
    """ZINCRBY command tests."""

    @pytest.fixture
    def db(self):
        with Redlite.open(":memory:") as db:
            yield db

    def test_zincrby_existing(self, db):
        db.zadd("zset", {"a": 10.0})
        result = db.zincrby("zset", 5.0, "a")
        assert result == 15.0

    def test_zincrby_new_member(self, db):
        result = db.zincrby("zset", 5.0, "a")
        assert result == 5.0

    def test_zincrby_negative(self, db):
        db.zadd("zset", {"a": 10.0})
        result = db.zincrby("zset", -3.0, "a")
        assert result == 7.0

    def test_zincrby_float(self, db):
        db.zadd("zset", {"a": 1.0})
        result = db.zincrby("zset", 0.5, "a")
        assert abs(result - 1.5) < 0.001
