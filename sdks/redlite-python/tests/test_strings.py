"""Comprehensive string command tests for redlite Python SDK."""

import pytest
from redlite import Redlite, RedliteError


class TestGet:
    """GET command tests."""

    @pytest.fixture
    def db(self):
        with Redlite.open(":memory:") as db:
            yield db

    def test_get_existing(self, db):
        db.set("key", "value")
        assert db.get("key") == b"value"

    def test_get_nonexistent(self, db):
        assert db.get("nonexistent") is None

    def test_get_after_delete(self, db):
        db.set("key", "value")
        db.delete("key")
        assert db.get("key") is None

    def test_get_binary(self, db):
        binary = b"\x00\x01\x02\xff\xfe"
        db.set("binary", binary)
        assert db.get("binary") == binary

    def test_get_unicode(self, db):
        db.set("unicode", "日本語テスト🎉")
        assert db.get("unicode") == "日本語テスト🎉".encode("utf-8")

    def test_get_empty_key(self, db):
        db.set("", "empty_key_value")
        assert db.get("") == b"empty_key_value"


class TestSet:
    """SET command tests with all configurations."""

    @pytest.fixture
    def db(self):
        with Redlite.open(":memory:") as db:
            yield db

    # Basic SET
    def test_set_string(self, db):
        assert db.set("key", "value") is True
        assert db.get("key") == b"value"

    def test_set_bytes(self, db):
        db.set("key", b"bytes_value")
        assert db.get("key") == b"bytes_value"

    def test_set_int(self, db):
        db.set("key", 12345)
        assert db.get("key") == b"12345"

    def test_set_float(self, db):
        db.set("key", 3.14159)
        assert db.get("key") == b"3.14159"

    def test_set_overwrite(self, db):
        db.set("key", "first")
        db.set("key", "second")
        assert db.get("key") == b"second"

    # SET with EX (seconds)
    def test_set_with_ex(self, db):
        db.set("key", "value", ex=60)
        ttl = db.ttl("key")
        assert 59 <= ttl <= 60

    def test_set_with_ex_zero(self, db):
        db.set("key", "value", ex=0)
        # ex=0 means immediate expiration, key should not exist
        # TTL returns -2 for non-existent keys
        assert db.ttl("key") == -2

    # SET with PX (milliseconds)
    def test_set_with_px(self, db):
        db.set("key", "value", px=60000)
        ttl = db.ttl("key")
        assert 59 <= ttl <= 60

    # Edge cases
    def test_set_binary_with_nulls(self, db):
        binary = b"\x00" * 100 + b"data" + b"\x00" * 100
        db.set("binary", binary)
        assert db.get("binary") == binary

    def test_set_large_value(self, db):
        large = b"x" * (1024 * 1024)  # 1MB
        db.set("large", large)
        assert db.get("large") == large

    def test_set_unicode_key_and_value(self, db):
        db.set("键", "值")
        assert db.get("键") == "值".encode("utf-8")

    def test_set_special_chars_key(self, db):
        special = "key:with/special\\chars{and}[brackets]"
        db.set(special, "value")
        assert db.get(special) == b"value"


class TestSetex:
    """SETEX command tests."""

    @pytest.fixture
    def db(self):
        with Redlite.open(":memory:") as db:
            yield db

    def test_setex_basic(self, db):
        db.setex("key", 60, "value")
        assert db.get("key") == b"value"
        ttl = db.ttl("key")
        assert 59 <= ttl <= 60

    def test_setex_short_ttl(self, db):
        db.setex("key", 1, "value")
        assert db.get("key") == b"value"
        ttl = db.ttl("key")
        assert 0 <= ttl <= 1

    def test_setex_overwrites(self, db):
        db.set("key", "first", ex=300)
        db.setex("key", 60, "second")
        assert db.get("key") == b"second"
        ttl = db.ttl("key")
        assert 59 <= ttl <= 60


class TestPsetex:
    """PSETEX command tests."""

    @pytest.fixture
    def db(self):
        with Redlite.open(":memory:") as db:
            yield db

    def test_psetex_basic(self, db):
        db.psetex("key", 60000, "value")
        pttl = db.pttl("key")
        assert 59000 <= pttl <= 60000

    def test_psetex_short_ms(self, db):
        db.psetex("key", 500, "value")
        pttl = db.pttl("key")
        assert 0 <= pttl <= 500


class TestGetdel:
    """GETDEL command tests."""

    @pytest.fixture
    def db(self):
        with Redlite.open(":memory:") as db:
            yield db

    def test_getdel_existing(self, db):
        db.set("key", "value")
        result = db.getdel("key")
        assert result == b"value"
        assert db.get("key") is None

    def test_getdel_nonexistent(self, db):
        result = db.getdel("nonexistent")
        assert result is None

    def test_getdel_removes_key(self, db):
        db.set("key", "value")
        db.getdel("key")
        assert db.exists("key") == 0


class TestAppend:
    """APPEND command tests."""

    @pytest.fixture
    def db(self):
        with Redlite.open(":memory:") as db:
            yield db

    def test_append_existing(self, db):
        db.set("key", "Hello")
        length = db.append("key", " World")
        assert length == 11
        assert db.get("key") == b"Hello World"

    def test_append_nonexistent(self, db):
        length = db.append("key", "value")
        assert length == 5
        assert db.get("key") == b"value"

    def test_append_empty(self, db):
        db.set("key", "value")
        length = db.append("key", "")
        assert length == 5
        assert db.get("key") == b"value"

    def test_append_binary(self, db):
        db.set("key", b"\x00\x01")
        length = db.append("key", b"\x02\x03")
        assert length == 4
        assert db.get("key") == b"\x00\x01\x02\x03"

    def test_append_multiple(self, db):
        db.append("key", "a")
        db.append("key", "b")
        db.append("key", "c")
        assert db.get("key") == b"abc"


class TestStrlen:
    """STRLEN command tests."""

    @pytest.fixture
    def db(self):
        with Redlite.open(":memory:") as db:
            yield db

    def test_strlen_existing(self, db):
        db.set("key", "hello")
        assert db.strlen("key") == 5

    def test_strlen_nonexistent(self, db):
        assert db.strlen("nonexistent") == 0

    def test_strlen_empty(self, db):
        db.set("key", "")
        assert db.strlen("key") == 0

    def test_strlen_binary(self, db):
        db.set("key", b"\x00\x01\x02\x03\x04")
        assert db.strlen("key") == 5

    def test_strlen_unicode(self, db):
        # UTF-8 encoding: "日" is 3 bytes
        db.set("key", "日本語")
        assert db.strlen("key") == 9  # 3 chars * 3 bytes


class TestGetrange:
    """GETRANGE command tests."""

    @pytest.fixture
    def db(self):
        with Redlite.open(":memory:") as db:
            yield db

    def test_getrange_basic(self, db):
        db.set("key", "Hello World")
        assert db.getrange("key", 0, 4) == b"Hello"

    def test_getrange_negative_end(self, db):
        db.set("key", "Hello World")
        assert db.getrange("key", 6, -1) == b"World"

    def test_getrange_negative_start(self, db):
        db.set("key", "Hello World")
        assert db.getrange("key", -5, -1) == b"World"

    def test_getrange_beyond_length(self, db):
        db.set("key", "Hello")
        assert db.getrange("key", 0, 100) == b"Hello"

    def test_getrange_nonexistent(self, db):
        assert db.getrange("nonexistent", 0, 10) == b""

    def test_getrange_entire_string(self, db):
        db.set("key", "value")
        assert db.getrange("key", 0, -1) == b"value"


class TestSetrange:
    """SETRANGE command tests."""

    @pytest.fixture
    def db(self):
        with Redlite.open(":memory:") as db:
            yield db

    def test_setrange_basic(self, db):
        db.set("key", "Hello World")
        length = db.setrange("key", 6, "Redis")
        assert length == 11
        assert db.get("key") == b"Hello Redis"

    def test_setrange_extend(self, db):
        db.set("key", "Hello")
        length = db.setrange("key", 10, "World")
        assert length == 15
        # Gap should be null bytes
        result = db.get("key")
        assert result[:5] == b"Hello"
        assert result[10:] == b"World"

    def test_setrange_nonexistent(self, db):
        length = db.setrange("key", 5, "value")
        assert length == 10
        result = db.get("key")
        assert result[5:] == b"value"


class TestIncr:
    """INCR command tests."""

    @pytest.fixture
    def db(self):
        with Redlite.open(":memory:") as db:
            yield db

    def test_incr_nonexistent(self, db):
        result = db.incr("counter")
        assert result == 1

    def test_incr_existing(self, db):
        db.set("counter", "10")
        result = db.incr("counter")
        assert result == 11

    def test_incr_multiple(self, db):
        for i in range(1, 11):
            result = db.incr("counter")
            assert result == i

    def test_incr_negative(self, db):
        db.set("counter", "-5")
        assert db.incr("counter") == -4

    def test_incr_zero(self, db):
        db.set("counter", "0")
        assert db.incr("counter") == 1


class TestDecr:
    """DECR command tests."""

    @pytest.fixture
    def db(self):
        with Redlite.open(":memory:") as db:
            yield db

    def test_decr_nonexistent(self, db):
        result = db.decr("counter")
        assert result == -1

    def test_decr_existing(self, db):
        db.set("counter", "10")
        result = db.decr("counter")
        assert result == 9

    def test_decr_to_negative(self, db):
        db.set("counter", "1")
        assert db.decr("counter") == 0
        assert db.decr("counter") == -1
        assert db.decr("counter") == -2


class TestIncrby:
    """INCRBY command tests."""

    @pytest.fixture
    def db(self):
        with Redlite.open(":memory:") as db:
            yield db

    def test_incrby_positive(self, db):
        db.set("counter", "10")
        assert db.incrby("counter", 5) == 15

    def test_incrby_negative(self, db):
        db.set("counter", "10")
        assert db.incrby("counter", -3) == 7

    def test_incrby_nonexistent(self, db):
        assert db.incrby("counter", 100) == 100

    def test_incrby_large(self, db):
        assert db.incrby("counter", 1000000000) == 1000000000
        assert db.incrby("counter", 1000000000) == 2000000000


class TestDecrby:
    """DECRBY command tests."""

    @pytest.fixture
    def db(self):
        with Redlite.open(":memory:") as db:
            yield db

    def test_decrby_positive(self, db):
        db.set("counter", "100")
        assert db.decrby("counter", 30) == 70

    def test_decrby_nonexistent(self, db):
        assert db.decrby("counter", 10) == -10


class TestIncrbyfloat:
    """INCRBYFLOAT command tests."""

    @pytest.fixture
    def db(self):
        with Redlite.open(":memory:") as db:
            yield db

    def test_incrbyfloat_basic(self, db):
        db.set("float", "10.5")
        result = db.incrbyfloat("float", 0.1)
        assert abs(result - 10.6) < 0.001

    def test_incrbyfloat_negative(self, db):
        db.set("float", "10.0")
        result = db.incrbyfloat("float", -2.5)
        assert abs(result - 7.5) < 0.001

    def test_incrbyfloat_nonexistent(self, db):
        result = db.incrbyfloat("float", 1.5)
        assert abs(result - 1.5) < 0.001

    def test_incrbyfloat_precision(self, db):
        db.set("float", "0")
        for _ in range(10):
            db.incrbyfloat("float", 0.1)
        result = db.incrbyfloat("float", 0)
        assert abs(result - 1.0) < 0.001

    def test_incrbyfloat_from_int(self, db):
        db.set("num", "10")
        result = db.incrbyfloat("num", 0.5)
        assert abs(result - 10.5) < 0.001
