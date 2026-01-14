"""Comprehensive key command tests for redlite Python SDK."""

import time
import pytest
from redlite import Redlite, RedliteError


class TestDelete:
    """DELETE command tests."""

    @pytest.fixture
    def db(self):
        with Redlite.open(":memory:") as db:
            yield db

    def test_delete_single(self, db):
        db.set("key", "value")
        count = db.delete("key")
        assert count == 1
        assert db.get("key") is None

    def test_delete_multiple(self, db):
        db.set("key1", "value1")
        db.set("key2", "value2")
        db.set("key3", "value3")
        count = db.delete("key1", "key2", "key3")
        assert count == 3

    def test_delete_nonexistent(self, db):
        count = db.delete("nonexistent")
        assert count == 0

    def test_delete_mixed(self, db):
        db.set("key1", "value1")
        count = db.delete("key1", "nonexistent")
        assert count == 1

    def test_delete_no_args(self, db):
        count = db.delete()
        assert count == 0

    def test_delete_all_types(self, db):
        """DELETE works on all data types."""
        db.set("string", "value")
        db.lpush("list", "item")
        db.sadd("set", "member")
        db.hset("hash", {"field": "value"})
        db.zadd("zset", {"member": 1.0})

        count = db.delete("string", "list", "set", "hash", "zset")
        assert count == 5


class TestExists:
    """EXISTS command tests."""

    @pytest.fixture
    def db(self):
        with Redlite.open(":memory:") as db:
            yield db

    def test_exists_single_exists(self, db):
        db.set("key", "value")
        assert db.exists("key") == 1

    def test_exists_single_not_exists(self, db):
        assert db.exists("nonexistent") == 0

    def test_exists_multiple(self, db):
        db.set("key1", "value1")
        db.set("key2", "value2")
        assert db.exists("key1", "key2") == 2

    def test_exists_mixed(self, db):
        db.set("key1", "value1")
        assert db.exists("key1", "nonexistent") == 1

    def test_exists_same_key_counted(self, db):
        """Same key counted multiple times."""
        db.set("key", "value")
        assert db.exists("key", "key", "key") == 3

    def test_exists_all_types(self, db):
        """EXISTS works on all data types."""
        db.set("string", "value")
        db.lpush("list", "item")
        db.sadd("set", "member")
        db.hset("hash", {"field": "value"})
        db.zadd("zset", {"member": 1.0})

        assert db.exists("string", "list", "set", "hash", "zset") == 5


class TestType:
    """TYPE command tests."""

    @pytest.fixture
    def db(self):
        with Redlite.open(":memory:") as db:
            yield db

    def test_type_string(self, db):
        db.set("key", "value")
        assert db.type("key") == "string"

    def test_type_list(self, db):
        db.lpush("key", "item")
        assert db.type("key") == "list"

    def test_type_set(self, db):
        db.sadd("key", "member")
        assert db.type("key") == "set"

    def test_type_zset(self, db):
        db.zadd("key", {"member": 1.0})
        assert db.type("key") == "zset"

    def test_type_hash(self, db):
        db.hset("key", {"field": "value"})
        assert db.type("key") == "hash"

    def test_type_nonexistent(self, db):
        assert db.type("nonexistent") == "none"


class TestTTL:
    """TTL command tests."""

    @pytest.fixture
    def db(self):
        with Redlite.open(":memory:") as db:
            yield db

    def test_ttl_with_expiry(self, db):
        db.setex("key", 60, "value")
        ttl = db.ttl("key")
        assert 59 <= ttl <= 60

    def test_ttl_no_expiry(self, db):
        db.set("key", "value")
        assert db.ttl("key") == -1

    def test_ttl_nonexistent(self, db):
        assert db.ttl("nonexistent") == -2


class TestPTTL:
    """PTTL command tests."""

    @pytest.fixture
    def db(self):
        with Redlite.open(":memory:") as db:
            yield db

    def test_pttl_with_expiry(self, db):
        db.psetex("key", 60000, "value")
        pttl = db.pttl("key")
        assert 59000 <= pttl <= 60000

    def test_pttl_no_expiry(self, db):
        db.set("key", "value")
        assert db.pttl("key") == -1

    def test_pttl_nonexistent(self, db):
        assert db.pttl("nonexistent") == -2


class TestExpire:
    """EXPIRE command tests."""

    @pytest.fixture
    def db(self):
        with Redlite.open(":memory:") as db:
            yield db

    def test_expire_existing(self, db):
        db.set("key", "value")
        result = db.expire("key", 60)
        assert result is True
        ttl = db.ttl("key")
        assert 59 <= ttl <= 60

    def test_expire_nonexistent(self, db):
        result = db.expire("nonexistent", 60)
        assert result is False

    def test_expire_updates_existing(self, db):
        db.setex("key", 300, "value")
        db.expire("key", 60)
        ttl = db.ttl("key")
        assert 59 <= ttl <= 60


class TestPExpire:
    """PEXPIRE command tests."""

    @pytest.fixture
    def db(self):
        with Redlite.open(":memory:") as db:
            yield db

    def test_pexpire_existing(self, db):
        db.set("key", "value")
        result = db.pexpire("key", 60000)
        assert result is True
        pttl = db.pttl("key")
        assert 59000 <= pttl <= 60000

    def test_pexpire_nonexistent(self, db):
        result = db.pexpire("nonexistent", 60000)
        assert result is False


class TestExpireAt:
    """EXPIREAT command tests."""

    @pytest.fixture
    def db(self):
        with Redlite.open(":memory:") as db:
            yield db

    def test_expireat_future(self, db):
        db.set("key", "value")
        future = int(time.time()) + 60
        result = db.expireat("key", future)
        assert result is True
        ttl = db.ttl("key")
        assert 58 <= ttl <= 60

    def test_expireat_nonexistent(self, db):
        future = int(time.time()) + 60
        result = db.expireat("nonexistent", future)
        assert result is False


class TestPExpireAt:
    """PEXPIREAT command tests."""

    @pytest.fixture
    def db(self):
        with Redlite.open(":memory:") as db:
            yield db

    def test_pexpireat_future(self, db):
        db.set("key", "value")
        future_ms = int(time.time() * 1000) + 60000
        result = db.pexpireat("key", future_ms)
        assert result is True
        pttl = db.pttl("key")
        assert 58000 <= pttl <= 60000


class TestPersist:
    """PERSIST command tests."""

    @pytest.fixture
    def db(self):
        with Redlite.open(":memory:") as db:
            yield db

    def test_persist_removes_ttl(self, db):
        db.setex("key", 60, "value")
        result = db.persist("key")
        assert result is True
        assert db.ttl("key") == -1

    def test_persist_nonexistent(self, db):
        result = db.persist("nonexistent")
        assert result is False


class TestRename:
    """RENAME command tests."""

    @pytest.fixture
    def db(self):
        with Redlite.open(":memory:") as db:
            yield db

    def test_rename_basic(self, db):
        db.set("old", "value")
        db.rename("old", "new")
        assert db.get("old") is None
        assert db.get("new") == b"value"

    def test_rename_overwrites(self, db):
        db.set("old", "old_value")
        db.set("new", "new_value")
        db.rename("old", "new")
        assert db.get("new") == b"old_value"

    def test_rename_preserves_ttl(self, db):
        db.setex("old", 60, "value")
        db.rename("old", "new")
        ttl = db.ttl("new")
        assert 58 <= ttl <= 60


class TestRenamenx:
    """RENAMENX command tests."""

    @pytest.fixture
    def db(self):
        with Redlite.open(":memory:") as db:
            yield db

    def test_renamenx_success(self, db):
        db.set("old", "value")
        result = db.renamenx("old", "new")
        assert result is True
        assert db.get("new") == b"value"

    def test_renamenx_target_exists(self, db):
        db.set("old", "old_value")
        db.set("new", "new_value")
        result = db.renamenx("old", "new")
        assert result is False
        assert db.get("old") == b"old_value"
        assert db.get("new") == b"new_value"


class TestKeys:
    """KEYS command tests."""

    @pytest.fixture
    def db(self):
        with Redlite.open(":memory:") as db:
            yield db

    def test_keys_all(self, db):
        db.set("key1", "value")
        db.set("key2", "value")
        db.set("key3", "value")
        keys = db.keys("*")
        assert len(keys) == 3

    def test_keys_pattern(self, db):
        db.set("user:1", "alice")
        db.set("user:2", "bob")
        db.set("post:1", "hello")
        keys = db.keys("user:*")
        assert len(keys) == 2
        assert all(k.startswith("user:") for k in keys)

    def test_keys_single_char(self, db):
        db.set("a1", "value")
        db.set("a2", "value")
        db.set("a10", "value")
        keys = db.keys("a?")
        assert len(keys) == 2

    def test_keys_no_match(self, db):
        db.set("key", "value")
        keys = db.keys("nomatch:*")
        assert len(keys) == 0

    def test_keys_empty_db(self, db):
        keys = db.keys("*")
        assert len(keys) == 0


class TestDbsize:
    """DBSIZE command tests."""

    @pytest.fixture
    def db(self):
        with Redlite.open(":memory:") as db:
            yield db

    def test_dbsize_empty(self, db):
        assert db.dbsize() == 0

    def test_dbsize_with_keys(self, db):
        db.set("key1", "value")
        assert db.dbsize() == 1
        db.set("key2", "value")
        assert db.dbsize() == 2

    def test_dbsize_after_delete(self, db):
        db.set("key1", "value")
        db.set("key2", "value")
        db.delete("key1")
        assert db.dbsize() == 1

    def test_dbsize_counts_all_types(self, db):
        db.set("string", "value")
        db.lpush("list", "item")
        db.sadd("set", "member")
        assert db.dbsize() == 3


class TestFlushdb:
    """FLUSHDB command tests."""

    @pytest.fixture
    def db(self):
        with Redlite.open(":memory:") as db:
            yield db

    def test_flushdb_clears_all(self, db):
        db.set("key1", "value")
        db.set("key2", "value")
        db.lpush("list", "item")
        db.sadd("set", "member")

        db.flushdb()
        assert db.dbsize() == 0

    def test_flushdb_empty(self, db):
        # Should not error on empty db
        db.flushdb()
        assert db.dbsize() == 0


class TestSelect:
    """SELECT command tests."""

    @pytest.fixture
    def db(self):
        with Redlite.open(":memory:") as db:
            yield db

    def test_select_isolates_data(self, db):
        db.select(0)
        db.set("key", "db0")

        db.select(1)
        assert db.get("key") is None
        db.set("key", "db1")

        db.select(0)
        assert db.get("key") == b"db0"

    def test_select_multiple_databases(self, db):
        for i in range(5):
            db.select(i)
            db.set("db_num", str(i))

        for i in range(5):
            db.select(i)
            assert db.get("db_num") == str(i).encode()
