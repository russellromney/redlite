defmodule RedliteTest do
  use ExUnit.Case, async: true
  doctest Redlite

  setup do
    {:ok, db} = Redlite.open(":memory:")
    {:ok, db: db}
  end

  # ==========================================================================
  # String Commands
  # ==========================================================================

  describe "GET/SET" do
    test "SET and GET roundtrip", %{db: db} do
      :ok = Redlite.set(db, "key", "value")
      assert {:ok, "value"} = Redlite.get(db, "key")
    end

    test "GET nonexistent key returns nil", %{db: db} do
      assert {:ok, nil} = Redlite.get(db, "nonexistent")
    end

    test "SET overwrites existing value", %{db: db} do
      :ok = Redlite.set(db, "key", "first")
      :ok = Redlite.set(db, "key", "second")
      assert {:ok, "second"} = Redlite.get(db, "key")
    end

    test "SET empty string", %{db: db} do
      :ok = Redlite.set(db, "key", "")
      assert {:ok, ""} = Redlite.get(db, "key")
    end

    test "SET binary data", %{db: db} do
      data = <<0, 1, 255, 128>>
      :ok = Redlite.set(db, "key", data)
      assert {:ok, ^data} = Redlite.get(db, "key")
    end

    test "SET with TTL", %{db: db} do
      :ok = Redlite.set(db, "key", "value", ttl: 3600)
      assert {:ok, "value"} = Redlite.get(db, "key")
      assert {:ok, ttl} = Redlite.ttl(db, "key")
      assert ttl > 0 and ttl <= 3600
    end
  end

  describe "MGET/MSET" do
    test "MSET multiple keys", %{db: db} do
      :ok = Redlite.mset(db, [{"k1", "v1"}, {"k2", "v2"}, {"k3", "v3"}])
      assert {:ok, "v1"} = Redlite.get(db, "k1")
      assert {:ok, "v2"} = Redlite.get(db, "k2")
      assert {:ok, "v3"} = Redlite.get(db, "k3")
    end

    test "MSET with map", %{db: db} do
      :ok = Redlite.mset(db, %{"k1" => "v1", "k2" => "v2"})
      assert {:ok, "v1"} = Redlite.get(db, "k1")
      assert {:ok, "v2"} = Redlite.get(db, "k2")
    end

    test "MGET multiple keys", %{db: db} do
      :ok = Redlite.set(db, "k1", "v1")
      :ok = Redlite.set(db, "k2", "v2")
      assert {:ok, ["v1", "v2", nil]} = Redlite.mget(db, ["k1", "k2", "k3"])
    end

    test "MGET all nonexistent", %{db: db} do
      assert {:ok, [nil, nil, nil]} = Redlite.mget(db, ["a", "b", "c"])
    end
  end

  describe "INCR/DECR" do
    test "INCR creates key with value 1", %{db: db} do
      assert {:ok, 1} = Redlite.incr(db, "counter")
      assert {:ok, "1"} = Redlite.get(db, "counter")
    end

    test "INCR increments existing value", %{db: db} do
      :ok = Redlite.set(db, "counter", "10")
      assert {:ok, 11} = Redlite.incr(db, "counter")
      assert {:ok, 12} = Redlite.incr(db, "counter")
    end

    test "DECR creates key with value -1", %{db: db} do
      assert {:ok, -1} = Redlite.decr(db, "counter")
      assert {:ok, "-1"} = Redlite.get(db, "counter")
    end

    test "INCRBY with positive increment", %{db: db} do
      assert {:ok, 5} = Redlite.incrby(db, "counter", 5)
      assert {:ok, 15} = Redlite.incrby(db, "counter", 10)
    end

    test "INCRBY with negative increment", %{db: db} do
      :ok = Redlite.set(db, "counter", "100")
      assert {:ok, 70} = Redlite.incrby(db, "counter", -30)
    end

    test "DECRBY", %{db: db} do
      :ok = Redlite.set(db, "counter", "100")
      assert {:ok, 90} = Redlite.decrby(db, "counter", 10)
    end

    test "INCRBYFLOAT", %{db: db} do
      assert {:ok, result} = Redlite.incrbyfloat(db, "float", 3.14)
      assert_in_delta result, 3.14, 0.001
    end
  end

  describe "String manipulation" do
    test "APPEND", %{db: db} do
      assert {:ok, 5} = Redlite.append(db, "key", "Hello")
      assert {:ok, 11} = Redlite.append(db, "key", " World")
      assert {:ok, "Hello World"} = Redlite.get(db, "key")
    end

    test "STRLEN", %{db: db} do
      :ok = Redlite.set(db, "key", "Hello")
      assert {:ok, 5} = Redlite.strlen(db, "key")
    end

    test "GETRANGE", %{db: db} do
      :ok = Redlite.set(db, "key", "Hello World")
      assert {:ok, "World"} = Redlite.getrange(db, "key", 6, 10)
    end

    test "SETRANGE", %{db: db} do
      :ok = Redlite.set(db, "key", "Hello World")
      assert {:ok, 11} = Redlite.setrange(db, "key", 6, "Redis")
      assert {:ok, "Hello Redis"} = Redlite.get(db, "key")
    end
  end

  describe "SETNX" do
    test "SETNX sets value when key doesn't exist", %{db: db} do
      assert {:ok, true} = Redlite.setnx(db, "key", "value")
      assert {:ok, "value"} = Redlite.get(db, "key")
    end

    test "SETNX returns false when key exists", %{db: db} do
      :ok = Redlite.set(db, "key", "first")
      assert {:ok, false} = Redlite.setnx(db, "key", "second")
      assert {:ok, "first"} = Redlite.get(db, "key")
    end
  end

  describe "SETEX/PSETEX" do
    test "SETEX sets key with TTL in seconds", %{db: db} do
      :ok = Redlite.setex(db, "key", 3600, "value")
      assert {:ok, "value"} = Redlite.get(db, "key")
      assert {:ok, ttl} = Redlite.ttl(db, "key")
      assert ttl > 0 and ttl <= 3600
    end

    test "PSETEX sets key with TTL in milliseconds", %{db: db} do
      :ok = Redlite.psetex(db, "key", 360000, "value")
      assert {:ok, "value"} = Redlite.get(db, "key")
      assert {:ok, pttl} = Redlite.pttl(db, "key")
      assert pttl > 0 and pttl <= 360000
    end
  end

  describe "GETDEL" do
    test "GETDEL returns value and deletes key", %{db: db} do
      :ok = Redlite.set(db, "key", "value")
      assert {:ok, "value"} = Redlite.getdel(db, "key")
      assert {:ok, nil} = Redlite.get(db, "key")
    end

    test "GETDEL returns nil for nonexistent key", %{db: db} do
      assert {:ok, nil} = Redlite.getdel(db, "nonexistent")
    end
  end

  describe "GETEX" do
    test "GETEX returns value without modifying TTL", %{db: db} do
      :ok = Redlite.set(db, "key", "value")
      assert {:ok, "value"} = Redlite.getex(db, "key")
      assert {:ok, -1} = Redlite.ttl(db, "key")
    end

    test "GETEX with ex sets TTL in seconds", %{db: db} do
      :ok = Redlite.set(db, "key", "value")
      assert {:ok, "value"} = Redlite.getex(db, "key", ex: 3600)
      assert {:ok, ttl} = Redlite.ttl(db, "key")
      assert ttl > 0 and ttl <= 3600
    end

    test "GETEX with persist removes TTL", %{db: db} do
      :ok = Redlite.set(db, "key", "value", ttl: 3600)
      assert {:ok, "value"} = Redlite.getex(db, "key", persist: true)
      assert {:ok, -1} = Redlite.ttl(db, "key")
    end

    test "GETEX returns nil for nonexistent key", %{db: db} do
      assert {:ok, nil} = Redlite.getex(db, "nonexistent")
    end
  end

  describe "Bit operations" do
    test "SETBIT and GETBIT", %{db: db} do
      assert {:ok, 0} = Redlite.setbit(db, "bits", 7, true)
      assert {:ok, 1} = Redlite.getbit(db, "bits", 7)
      assert {:ok, 0} = Redlite.getbit(db, "bits", 6)
    end

    test "SETBIT returns previous value", %{db: db} do
      assert {:ok, 0} = Redlite.setbit(db, "bits", 7, true)
      assert {:ok, 1} = Redlite.setbit(db, "bits", 7, false)
      assert {:ok, 0} = Redlite.getbit(db, "bits", 7)
    end

    test "BITCOUNT counts set bits", %{db: db} do
      :ok = Redlite.set(db, "key", "foobar")
      assert {:ok, count} = Redlite.bitcount(db, "key")
      assert count > 0
    end

    test "BITCOUNT with range", %{db: db} do
      :ok = Redlite.set(db, "key", "foobar")
      assert {:ok, count} = Redlite.bitcount(db, "key", start: 0, end: 0)
      assert count >= 0
    end

    test "BITOP AND", %{db: db} do
      :ok = Redlite.set(db, "k1", "foof")
      :ok = Redlite.set(db, "k2", "foof")
      assert {:ok, len} = Redlite.bitop(db, "AND", "dest", ["k1", "k2"])
      assert len > 0
    end

    test "BITOP OR", %{db: db} do
      :ok = Redlite.set(db, "k1", <<0x00, 0xFF>>)
      :ok = Redlite.set(db, "k2", <<0xFF, 0x00>>)
      assert {:ok, 2} = Redlite.bitop(db, "OR", "dest", ["k1", "k2"])
    end
  end

  # ==========================================================================
  # Key Commands
  # ==========================================================================

  describe "Key operations" do
    test "DEL single key", %{db: db} do
      :ok = Redlite.set(db, "key", "value")
      assert {:ok, 1} = Redlite.del(db, "key")
      assert {:ok, nil} = Redlite.get(db, "key")
    end

    test "DEL multiple keys", %{db: db} do
      :ok = Redlite.set(db, "k1", "v1")
      :ok = Redlite.set(db, "k2", "v2")
      assert {:ok, 2} = Redlite.del(db, ["k1", "k2", "k3"])
    end

    test "EXISTS", %{db: db} do
      :ok = Redlite.set(db, "k1", "v1")
      assert {:ok, 1} = Redlite.exists(db, "k1")
      assert {:ok, 0} = Redlite.exists(db, "nonexistent")
      assert {:ok, 1} = Redlite.exists(db, ["k1", "k2"])
    end

    test "TYPE", %{db: db} do
      :ok = Redlite.set(db, "string", "value")
      assert {:ok, :string} = Redlite.type(db, "string")
      assert {:ok, :none} = Redlite.type(db, "nonexistent")
    end

    test "KEYS pattern", %{db: db} do
      :ok = Redlite.set(db, "user:1", "a")
      :ok = Redlite.set(db, "user:2", "b")
      :ok = Redlite.set(db, "other", "c")
      assert {:ok, keys} = Redlite.keys(db, "user:*")
      assert length(keys) == 2
      assert "user:1" in keys
      assert "user:2" in keys
    end

    test "RENAME", %{db: db} do
      :ok = Redlite.set(db, "old", "value")
      :ok = Redlite.rename(db, "old", "new")
      assert {:ok, nil} = Redlite.get(db, "old")
      assert {:ok, "value"} = Redlite.get(db, "new")
    end

    test "RENAMENX", %{db: db} do
      :ok = Redlite.set(db, "k1", "v1")
      :ok = Redlite.set(db, "k2", "v2")
      assert {:ok, false} = Redlite.renamenx(db, "k1", "k2")
      assert {:ok, true} = Redlite.renamenx(db, "k1", "k3")
    end

    test "DBSIZE", %{db: db} do
      assert {:ok, 0} = Redlite.dbsize(db)
      :ok = Redlite.set(db, "k1", "v1")
      :ok = Redlite.set(db, "k2", "v2")
      assert {:ok, 2} = Redlite.dbsize(db)
    end

    test "FLUSHDB", %{db: db} do
      :ok = Redlite.set(db, "k1", "v1")
      :ok = Redlite.set(db, "k2", "v2")
      :ok = Redlite.flushdb(db)
      assert {:ok, 0} = Redlite.dbsize(db)
    end
  end

  describe "TTL operations" do
    test "TTL on nonexistent key returns -2", %{db: db} do
      assert {:ok, -2} = Redlite.ttl(db, "nonexistent")
    end

    test "TTL on key without expiration returns -1", %{db: db} do
      :ok = Redlite.set(db, "key", "value")
      assert {:ok, -1} = Redlite.ttl(db, "key")
    end

    test "EXPIRE and TTL", %{db: db} do
      :ok = Redlite.set(db, "key", "value")
      assert {:ok, true} = Redlite.expire(db, "key", 3600)
      assert {:ok, ttl} = Redlite.ttl(db, "key")
      assert ttl > 0 and ttl <= 3600
    end

    test "PERSIST removes TTL", %{db: db} do
      :ok = Redlite.set(db, "key", "value", ttl: 3600)
      assert {:ok, true} = Redlite.persist(db, "key")
      assert {:ok, -1} = Redlite.ttl(db, "key")
    end

    test "PTTL returns milliseconds", %{db: db} do
      :ok = Redlite.set(db, "key", "value", ttl: 3600)
      assert {:ok, pttl} = Redlite.pttl(db, "key")
      assert pttl > 0 and pttl <= 3600000
    end

    test "PEXPIRE sets expiration in milliseconds", %{db: db} do
      :ok = Redlite.set(db, "key", "value")
      assert {:ok, true} = Redlite.pexpire(db, "key", 360000)
      assert {:ok, pttl} = Redlite.pttl(db, "key")
      assert pttl > 0 and pttl <= 360000
    end

    test "EXPIREAT sets expiration at Unix timestamp", %{db: db} do
      :ok = Redlite.set(db, "key", "value")
      future = System.system_time(:second) + 3600
      assert {:ok, true} = Redlite.expireat(db, "key", future)
      assert {:ok, ttl} = Redlite.ttl(db, "key")
      assert ttl > 0 and ttl <= 3600
    end

    test "PEXPIREAT sets expiration at Unix timestamp in milliseconds", %{db: db} do
      :ok = Redlite.set(db, "key", "value")
      future_ms = System.system_time(:millisecond) + 360000
      assert {:ok, true} = Redlite.pexpireat(db, "key", future_ms)
      assert {:ok, pttl} = Redlite.pttl(db, "key")
      assert pttl > 0 and pttl <= 360000
    end
  end

  describe "SELECT" do
    test "SELECT switches database", %{db: db} do
      :ok = Redlite.set(db, "key", "value")
      :ok = Redlite.select(db, 1)
      assert {:ok, nil} = Redlite.get(db, "key")
      :ok = Redlite.select(db, 0)
      assert {:ok, "value"} = Redlite.get(db, "key")
    end
  end

  # ==========================================================================
  # Hash Commands
  # ==========================================================================

  describe "Hash operations" do
    test "HSET and HGET", %{db: db} do
      assert {:ok, 1} = Redlite.hset(db, "hash", "field", "value")
      assert {:ok, "value"} = Redlite.hget(db, "hash", "field")
    end

    test "HSET multiple fields", %{db: db} do
      assert {:ok, 2} = Redlite.hset(db, "hash", %{"f1" => "v1", "f2" => "v2"})
      assert {:ok, "v1"} = Redlite.hget(db, "hash", "f1")
      assert {:ok, "v2"} = Redlite.hget(db, "hash", "f2")
    end

    test "HGET nonexistent field", %{db: db} do
      assert {:ok, nil} = Redlite.hget(db, "hash", "field")
    end

    test "HDEL", %{db: db} do
      Redlite.hset(db, "hash", "f1", "v1")
      Redlite.hset(db, "hash", "f2", "v2")
      assert {:ok, 1} = Redlite.hdel(db, "hash", "f1")
      assert {:ok, nil} = Redlite.hget(db, "hash", "f1")
    end

    test "HEXISTS", %{db: db} do
      Redlite.hset(db, "hash", "field", "value")
      assert {:ok, true} = Redlite.hexists(db, "hash", "field")
      assert {:ok, false} = Redlite.hexists(db, "hash", "other")
    end

    test "HLEN", %{db: db} do
      Redlite.hset(db, "hash", %{"f1" => "v1", "f2" => "v2"})
      assert {:ok, 2} = Redlite.hlen(db, "hash")
    end

    test "HKEYS and HVALS", %{db: db} do
      Redlite.hset(db, "hash", %{"f1" => "v1", "f2" => "v2"})
      assert {:ok, keys} = Redlite.hkeys(db, "hash")
      assert Enum.sort(keys) == ["f1", "f2"]
      assert {:ok, vals} = Redlite.hvals(db, "hash")
      assert Enum.sort(vals) == ["v1", "v2"]
    end

    test "HGETALL", %{db: db} do
      Redlite.hset(db, "hash", %{"f1" => "v1", "f2" => "v2"})
      assert {:ok, pairs} = Redlite.hgetall(db, "hash")
      pairs_map = Map.new(pairs)
      assert pairs_map["f1"] == "v1"
      assert pairs_map["f2"] == "v2"
    end

    test "HMGET", %{db: db} do
      Redlite.hset(db, "hash", %{"f1" => "v1", "f2" => "v2"})
      assert {:ok, ["v1", "v2", nil]} = Redlite.hmget(db, "hash", ["f1", "f2", "f3"])
    end

    test "HINCRBY", %{db: db} do
      Redlite.hset(db, "hash", "counter", "10")
      assert {:ok, 15} = Redlite.hincrby(db, "hash", "counter", 5)
    end

    test "HSETNX sets field when it doesn't exist", %{db: db} do
      assert {:ok, true} = Redlite.hsetnx(db, "hash", "field", "value")
      assert {:ok, "value"} = Redlite.hget(db, "hash", "field")
    end

    test "HSETNX returns false when field exists", %{db: db} do
      Redlite.hset(db, "hash", "field", "first")
      assert {:ok, false} = Redlite.hsetnx(db, "hash", "field", "second")
      assert {:ok, "first"} = Redlite.hget(db, "hash", "field")
    end

    test "HINCRBYFLOAT increments by float", %{db: db} do
      Redlite.hset(db, "hash", "counter", "10.5")
      assert {:ok, result} = Redlite.hincrbyfloat(db, "hash", "counter", 2.5)
      assert_in_delta result, 13.0, 0.001
    end

    test "HINCRBYFLOAT creates field if not exists", %{db: db} do
      assert {:ok, result} = Redlite.hincrbyfloat(db, "hash", "newfield", 3.14)
      assert_in_delta result, 3.14, 0.001
    end
  end

  # ==========================================================================
  # List Commands
  # ==========================================================================

  describe "List operations" do
    test "LPUSH and RPUSH", %{db: db} do
      assert {:ok, 1} = Redlite.rpush(db, "list", "a")
      assert {:ok, 2} = Redlite.rpush(db, "list", "b")
      assert {:ok, 3} = Redlite.lpush(db, "list", "c")
      assert {:ok, ["c", "a", "b"]} = Redlite.lrange(db, "list", 0, -1)
    end

    test "LPOP and RPOP", %{db: db} do
      Redlite.rpush(db, "list", ["a", "b", "c"])
      assert {:ok, "a"} = Redlite.lpop(db, "list")
      assert {:ok, "c"} = Redlite.rpop(db, "list")
      assert {:ok, ["b"]} = Redlite.lrange(db, "list", 0, -1)
    end

    test "LPOP from empty list", %{db: db} do
      assert {:ok, nil} = Redlite.lpop(db, "nonexistent")
    end

    test "LLEN", %{db: db} do
      Redlite.rpush(db, "list", ["a", "b", "c"])
      assert {:ok, 3} = Redlite.llen(db, "list")
    end

    test "LRANGE", %{db: db} do
      Redlite.rpush(db, "list", ["a", "b", "c", "d"])
      assert {:ok, ["b", "c"]} = Redlite.lrange(db, "list", 1, 2)
      assert {:ok, ["c", "d"]} = Redlite.lrange(db, "list", -2, -1)
    end

    test "LINDEX", %{db: db} do
      Redlite.rpush(db, "list", ["a", "b", "c"])
      assert {:ok, "a"} = Redlite.lindex(db, "list", 0)
      assert {:ok, "c"} = Redlite.lindex(db, "list", -1)
      assert {:ok, nil} = Redlite.lindex(db, "list", 10)
    end

    test "LSET modifies element at index", %{db: db} do
      Redlite.rpush(db, "list", ["a", "b", "c"])
      :ok = Redlite.lset(db, "list", 1, "B")
      assert {:ok, ["a", "B", "c"]} = Redlite.lrange(db, "list", 0, -1)
    end

    test "LTRIM trims list to range", %{db: db} do
      Redlite.rpush(db, "list", ["a", "b", "c", "d", "e"])
      :ok = Redlite.ltrim(db, "list", 1, 3)
      assert {:ok, ["b", "c", "d"]} = Redlite.lrange(db, "list", 0, -1)
    end

    test "LREM removes elements", %{db: db} do
      Redlite.rpush(db, "list", ["a", "b", "a", "c", "a"])
      assert {:ok, 2} = Redlite.lrem(db, "list", 2, "a")
      assert {:ok, ["b", "c", "a"]} = Redlite.lrange(db, "list", 0, -1)
    end

    test "LREM with negative count removes from tail", %{db: db} do
      Redlite.rpush(db, "list", ["a", "b", "a", "c", "a"])
      assert {:ok, 2} = Redlite.lrem(db, "list", -2, "a")
      assert {:ok, ["a", "b", "c"]} = Redlite.lrange(db, "list", 0, -1)
    end

    test "LINSERT before pivot", %{db: db} do
      Redlite.rpush(db, "list", ["a", "b", "c"])
      assert {:ok, 4} = Redlite.linsert(db, "list", :before, "b", "x")
      assert {:ok, ["a", "x", "b", "c"]} = Redlite.lrange(db, "list", 0, -1)
    end

    test "LINSERT after pivot", %{db: db} do
      Redlite.rpush(db, "list", ["a", "b", "c"])
      assert {:ok, 4} = Redlite.linsert(db, "list", :after, "b", "x")
      assert {:ok, ["a", "b", "x", "c"]} = Redlite.lrange(db, "list", 0, -1)
    end

    test "LINSERT returns -1 when pivot not found", %{db: db} do
      Redlite.rpush(db, "list", ["a", "b", "c"])
      assert {:ok, -1} = Redlite.linsert(db, "list", :before, "z", "x")
    end

    test "LPUSHX only pushes if list exists", %{db: db} do
      assert {:ok, 0} = Redlite.lpushx(db, "nonexistent", "value")
      Redlite.rpush(db, "list", ["a"])
      assert {:ok, 2} = Redlite.lpushx(db, "list", "b")
      assert {:ok, ["b", "a"]} = Redlite.lrange(db, "list", 0, -1)
    end

    test "RPUSHX only pushes if list exists", %{db: db} do
      assert {:ok, 0} = Redlite.rpushx(db, "nonexistent", "value")
      Redlite.rpush(db, "list", ["a"])
      assert {:ok, 2} = Redlite.rpushx(db, "list", "b")
      assert {:ok, ["a", "b"]} = Redlite.lrange(db, "list", 0, -1)
    end

    test "LMOVE moves element between lists", %{db: db} do
      Redlite.rpush(db, "src", ["a", "b", "c"])
      Redlite.rpush(db, "dst", ["x"])
      assert {:ok, "c"} = Redlite.lmove(db, "src", "dst", :right, :left)
      assert {:ok, ["a", "b"]} = Redlite.lrange(db, "src", 0, -1)
      assert {:ok, ["c", "x"]} = Redlite.lrange(db, "dst", 0, -1)
    end

    test "LPOS finds element position", %{db: db} do
      Redlite.rpush(db, "list", ["a", "b", "c", "b", "d"])
      assert {:ok, 1} = Redlite.lpos(db, "list", "b")
    end

    test "LPOS with count returns multiple positions", %{db: db} do
      Redlite.rpush(db, "list", ["a", "b", "c", "b", "d"])
      assert {:ok, [1, 3]} = Redlite.lpos(db, "list", "b", count: 2)
    end

    test "LPOS returns nil when not found", %{db: db} do
      Redlite.rpush(db, "list", ["a", "b", "c"])
      assert {:ok, nil} = Redlite.lpos(db, "list", "z")
    end
  end

  # ==========================================================================
  # Set Commands
  # ==========================================================================

  describe "Set operations" do
    test "SADD and SMEMBERS", %{db: db} do
      assert {:ok, 3} = Redlite.sadd(db, "set", ["a", "b", "c"])
      assert {:ok, members} = Redlite.smembers(db, "set")
      assert Enum.sort(members) == ["a", "b", "c"]
    end

    test "SADD duplicates", %{db: db} do
      assert {:ok, 2} = Redlite.sadd(db, "set", ["a", "b"])
      assert {:ok, 1} = Redlite.sadd(db, "set", ["b", "c"])
    end

    test "SREM", %{db: db} do
      Redlite.sadd(db, "set", ["a", "b", "c"])
      assert {:ok, 2} = Redlite.srem(db, "set", ["a", "b"])
      assert {:ok, members} = Redlite.smembers(db, "set")
      assert members == ["c"]
    end

    test "SISMEMBER", %{db: db} do
      Redlite.sadd(db, "set", ["a", "b"])
      assert {:ok, true} = Redlite.sismember(db, "set", "a")
      assert {:ok, false} = Redlite.sismember(db, "set", "c")
    end

    test "SCARD", %{db: db} do
      Redlite.sadd(db, "set", ["a", "b", "c"])
      assert {:ok, 3} = Redlite.scard(db, "set")
    end

    test "SPOP removes and returns random member", %{db: db} do
      Redlite.sadd(db, "set", ["a", "b", "c"])
      assert {:ok, member} = Redlite.spop(db, "set")
      assert member in ["a", "b", "c"]
      assert {:ok, 2} = Redlite.scard(db, "set")
    end

    test "SPOP with count removes multiple members", %{db: db} do
      Redlite.sadd(db, "set", ["a", "b", "c", "d"])
      assert {:ok, members} = Redlite.spop(db, "set", 2)
      assert length(members) == 2
      assert {:ok, 2} = Redlite.scard(db, "set")
    end

    test "SRANDMEMBER returns random member without removing", %{db: db} do
      Redlite.sadd(db, "set", ["a", "b", "c"])
      assert {:ok, member} = Redlite.srandmember(db, "set")
      assert member in ["a", "b", "c"]
      assert {:ok, 3} = Redlite.scard(db, "set")
    end

    test "SRANDMEMBER with count returns multiple members", %{db: db} do
      Redlite.sadd(db, "set", ["a", "b", "c", "d"])
      assert {:ok, members} = Redlite.srandmember(db, "set", 2)
      assert length(members) == 2
      assert {:ok, 4} = Redlite.scard(db, "set")
    end

    test "SDIFF returns difference of sets", %{db: db} do
      Redlite.sadd(db, "s1", ["a", "b", "c"])
      Redlite.sadd(db, "s2", ["b", "c", "d"])
      assert {:ok, diff} = Redlite.sdiff(db, ["s1", "s2"])
      assert Enum.sort(diff) == ["a"]
    end

    test "SINTER returns intersection of sets", %{db: db} do
      Redlite.sadd(db, "s1", ["a", "b", "c"])
      Redlite.sadd(db, "s2", ["b", "c", "d"])
      assert {:ok, inter} = Redlite.sinter(db, ["s1", "s2"])
      assert Enum.sort(inter) == ["b", "c"]
    end

    test "SUNION returns union of sets", %{db: db} do
      Redlite.sadd(db, "s1", ["a", "b"])
      Redlite.sadd(db, "s2", ["c", "d"])
      assert {:ok, union} = Redlite.sunion(db, ["s1", "s2"])
      assert Enum.sort(union) == ["a", "b", "c", "d"]
    end

    test "SDIFFSTORE stores difference", %{db: db} do
      Redlite.sadd(db, "s1", ["a", "b", "c"])
      Redlite.sadd(db, "s2", ["b", "c", "d"])
      assert {:ok, 1} = Redlite.sdiffstore(db, "dest", ["s1", "s2"])
      assert {:ok, members} = Redlite.smembers(db, "dest")
      assert members == ["a"]
    end

    test "SINTERSTORE stores intersection", %{db: db} do
      Redlite.sadd(db, "s1", ["a", "b", "c"])
      Redlite.sadd(db, "s2", ["b", "c", "d"])
      assert {:ok, 2} = Redlite.sinterstore(db, "dest", ["s1", "s2"])
      assert {:ok, members} = Redlite.smembers(db, "dest")
      assert Enum.sort(members) == ["b", "c"]
    end

    test "SUNIONSTORE stores union", %{db: db} do
      Redlite.sadd(db, "s1", ["a", "b"])
      Redlite.sadd(db, "s2", ["c", "d"])
      assert {:ok, 4} = Redlite.sunionstore(db, "dest", ["s1", "s2"])
      assert {:ok, members} = Redlite.smembers(db, "dest")
      assert Enum.sort(members) == ["a", "b", "c", "d"]
    end

    test "SMOVE moves member between sets", %{db: db} do
      Redlite.sadd(db, "src", ["a", "b", "c"])
      Redlite.sadd(db, "dst", ["x", "y"])
      assert {:ok, true} = Redlite.smove(db, "src", "dst", "b")
      assert {:ok, src_members} = Redlite.smembers(db, "src")
      assert {:ok, dst_members} = Redlite.smembers(db, "dst")
      assert Enum.sort(src_members) == ["a", "c"]
      assert Enum.sort(dst_members) == ["b", "x", "y"]
    end

    test "SMOVE returns false when member not in source", %{db: db} do
      Redlite.sadd(db, "src", ["a", "b"])
      Redlite.sadd(db, "dst", ["x"])
      assert {:ok, false} = Redlite.smove(db, "src", "dst", "z")
    end
  end

  # ==========================================================================
  # Sorted Set Commands
  # ==========================================================================

  describe "Sorted set operations" do
    test "ZADD and ZRANGE", %{db: db} do
      assert {:ok, 3} = Redlite.zadd(db, "zset", [{1.0, "a"}, {2.0, "b"}, {3.0, "c"}])
      assert {:ok, members} = Redlite.zrange(db, "zset", 0, -1)
      assert members == ["a", "b", "c"]
    end

    test "ZADD single member", %{db: db} do
      assert {:ok, 1} = Redlite.zadd(db, "zset", 1.0, "member")
      assert {:ok, score} = Redlite.zscore(db, "zset", "member")
      assert score == 1.0
    end

    test "ZRANGE with scores", %{db: db} do
      Redlite.zadd(db, "zset", [{1.0, "a"}, {2.0, "b"}])
      assert {:ok, members} = Redlite.zrange(db, "zset", 0, -1, with_scores: true)
      assert members == [{"a", 1.0}, {"b", 2.0}]
    end

    test "ZREVRANGE", %{db: db} do
      Redlite.zadd(db, "zset", [{1.0, "a"}, {2.0, "b"}, {3.0, "c"}])
      assert {:ok, members} = Redlite.zrevrange(db, "zset", 0, -1)
      assert members == ["c", "b", "a"]
    end

    test "ZREM", %{db: db} do
      Redlite.zadd(db, "zset", [{1.0, "a"}, {2.0, "b"}])
      assert {:ok, 1} = Redlite.zrem(db, "zset", "a")
      assert {:ok, nil} = Redlite.zscore(db, "zset", "a")
    end

    test "ZSCORE", %{db: db} do
      Redlite.zadd(db, "zset", 3.14, "pi")
      assert {:ok, score} = Redlite.zscore(db, "zset", "pi")
      assert_in_delta score, 3.14, 0.001
      assert {:ok, nil} = Redlite.zscore(db, "zset", "nonexistent")
    end

    test "ZCARD", %{db: db} do
      Redlite.zadd(db, "zset", [{1.0, "a"}, {2.0, "b"}])
      assert {:ok, 2} = Redlite.zcard(db, "zset")
    end

    test "ZCOUNT", %{db: db} do
      Redlite.zadd(db, "zset", [{1.0, "a"}, {2.0, "b"}, {3.0, "c"}])
      assert {:ok, 2} = Redlite.zcount(db, "zset", 1.0, 2.0)
    end

    test "ZINCRBY", %{db: db} do
      Redlite.zadd(db, "zset", 1.0, "member")
      assert {:ok, score} = Redlite.zincrby(db, "zset", 2.5, "member")
      assert_in_delta score, 3.5, 0.001
    end

    test "ZRANK returns rank in ascending order", %{db: db} do
      Redlite.zadd(db, "zset", [{1.0, "a"}, {2.0, "b"}, {3.0, "c"}])
      assert {:ok, 0} = Redlite.zrank(db, "zset", "a")
      assert {:ok, 1} = Redlite.zrank(db, "zset", "b")
      assert {:ok, 2} = Redlite.zrank(db, "zset", "c")
    end

    test "ZRANK returns nil for nonexistent member", %{db: db} do
      Redlite.zadd(db, "zset", [{1.0, "a"}])
      assert {:ok, nil} = Redlite.zrank(db, "zset", "z")
    end

    test "ZREVRANK returns rank in descending order", %{db: db} do
      Redlite.zadd(db, "zset", [{1.0, "a"}, {2.0, "b"}, {3.0, "c"}])
      assert {:ok, 2} = Redlite.zrevrank(db, "zset", "a")
      assert {:ok, 1} = Redlite.zrevrank(db, "zset", "b")
      assert {:ok, 0} = Redlite.zrevrank(db, "zset", "c")
    end

    test "ZRANGEBYSCORE returns members in score range", %{db: db} do
      Redlite.zadd(db, "zset", [{1.0, "a"}, {2.0, "b"}, {3.0, "c"}, {4.0, "d"}])
      assert {:ok, members} = Redlite.zrangebyscore(db, "zset", 2.0, 3.0)
      assert members == ["b", "c"]
    end

    test "ZRANGEBYSCORE with offset and count", %{db: db} do
      Redlite.zadd(db, "zset", [{1.0, "a"}, {2.0, "b"}, {3.0, "c"}, {4.0, "d"}])
      assert {:ok, members} = Redlite.zrangebyscore(db, "zset", 1.0, 4.0, offset: 1, count: 2)
      assert members == ["b", "c"]
    end

    test "ZREMRANGEBYRANK removes by rank range", %{db: db} do
      Redlite.zadd(db, "zset", [{1.0, "a"}, {2.0, "b"}, {3.0, "c"}, {4.0, "d"}])
      assert {:ok, 2} = Redlite.zremrangebyrank(db, "zset", 1, 2)
      assert {:ok, members} = Redlite.zrange(db, "zset", 0, -1)
      assert members == ["a", "d"]
    end

    test "ZREMRANGEBYSCORE removes by score range", %{db: db} do
      Redlite.zadd(db, "zset", [{1.0, "a"}, {2.0, "b"}, {3.0, "c"}, {4.0, "d"}])
      assert {:ok, 2} = Redlite.zremrangebyscore(db, "zset", 2.0, 3.0)
      assert {:ok, members} = Redlite.zrange(db, "zset", 0, -1)
      assert members == ["a", "d"]
    end

    test "ZINTERSTORE computes intersection", %{db: db} do
      Redlite.zadd(db, "z1", [{1.0, "a"}, {2.0, "b"}, {3.0, "c"}])
      Redlite.zadd(db, "z2", [{10.0, "b"}, {20.0, "c"}, {30.0, "d"}])
      assert {:ok, 2} = Redlite.zinterstore(db, "dest", ["z1", "z2"])
      assert {:ok, members} = Redlite.zrange(db, "dest", 0, -1, with_scores: true)
      # Default aggregate is SUM: b=2+10=12, c=3+20=23
      assert [{"b", b_score}, {"c", c_score}] = members
      assert_in_delta b_score, 12.0, 0.001
      assert_in_delta c_score, 23.0, 0.001
    end

    test "ZUNIONSTORE computes union", %{db: db} do
      Redlite.zadd(db, "z1", [{1.0, "a"}, {2.0, "b"}])
      Redlite.zadd(db, "z2", [{3.0, "c"}, {4.0, "d"}])
      assert {:ok, 4} = Redlite.zunionstore(db, "dest", ["z1", "z2"])
      assert {:ok, members} = Redlite.zrange(db, "dest", 0, -1)
      assert members == ["a", "b", "c", "d"]
    end

    test "ZINTERSTORE with weights", %{db: db} do
      Redlite.zadd(db, "z1", [{1.0, "a"}, {2.0, "b"}])
      Redlite.zadd(db, "z2", [{1.0, "a"}, {2.0, "b"}])
      assert {:ok, 2} = Redlite.zinterstore(db, "dest", ["z1", "z2"], weights: [2.0, 3.0])
      assert {:ok, members} = Redlite.zrange(db, "dest", 0, -1, with_scores: true)
      # a = 1*2 + 1*3 = 5, b = 2*2 + 2*3 = 10
      assert [{"a", a_score}, {"b", b_score}] = members
      assert_in_delta a_score, 5.0, 0.001
      assert_in_delta b_score, 10.0, 0.001
    end

    test "ZREVRANGE with scores", %{db: db} do
      Redlite.zadd(db, "zset", [{1.0, "a"}, {2.0, "b"}, {3.0, "c"}])
      assert {:ok, members} = Redlite.zrevrange(db, "zset", 0, -1, with_scores: true)
      assert [{"c", 3.0}, {"b", 2.0}, {"a", 1.0}] = members
    end
  end

  # ==========================================================================
  # Scan Commands
  # ==========================================================================

  describe "Scan operations" do
    test "SCAN", %{db: db} do
      for i <- 1..100 do
        Redlite.set(db, "key:#{i}", "value")
      end

      assert {:ok, {cursor, keys}} = Redlite.scan(db, "0", count: 10)
      assert is_binary(cursor)
      assert is_list(keys)
    end

    test "HSCAN", %{db: db} do
      for i <- 1..50 do
        Redlite.hset(db, "hash", "field:#{i}", "value")
      end

      assert {:ok, {cursor, pairs}} = Redlite.hscan(db, "hash", "0", count: 10)
      assert is_binary(cursor)
      assert is_list(pairs)
    end

    test "SSCAN iterates set members", %{db: db} do
      for i <- 1..50 do
        Redlite.sadd(db, "set", "member:#{i}")
      end

      assert {:ok, {cursor, members}} = Redlite.sscan(db, "set", "0", count: 10)
      assert is_binary(cursor)
      assert is_list(members)
    end

    test "ZSCAN iterates sorted set members with scores", %{db: db} do
      for i <- 1..50 do
        Redlite.zadd(db, "zset", i * 1.0, "member:#{i}")
      end

      assert {:ok, {cursor, members}} = Redlite.zscan(db, "zset", "0", count: 10)
      assert is_binary(cursor)
      assert is_list(members)
    end
  end

  describe "Server commands" do
    test "VACUUM reclaims space", %{db: db} do
      # Add some data
      for i <- 1..100 do
        Redlite.set(db, "key:#{i}", "value#{i}")
      end

      # Delete it
      for i <- 1..100 do
        Redlite.del(db, "key:#{i}")
      end

      # Vacuum should succeed
      assert {:ok, _} = Redlite.vacuum(db)
    end
  end

  describe "SET with options" do
    test "SET_OPTS with NX flag", %{db: db} do
      opts = %Redlite.SetOptions{nx: true}
      assert {:ok, true} = Redlite.set_opts(db, "key", "value", opts)
      assert {:ok, false} = Redlite.set_opts(db, "key", "value2", opts)
      assert {:ok, "value"} = Redlite.get(db, "key")
    end

    test "SET_OPTS with XX flag", %{db: db} do
      opts = %Redlite.SetOptions{xx: true}
      assert {:ok, false} = Redlite.set_opts(db, "key", "value", opts)
      :ok = Redlite.set(db, "key", "first")
      assert {:ok, true} = Redlite.set_opts(db, "key", "second", opts)
      assert {:ok, "second"} = Redlite.get(db, "key")
    end

    test "SET_OPTS with EX (seconds TTL)", %{db: db} do
      opts = %Redlite.SetOptions{ex: 3600}
      assert {:ok, true} = Redlite.set_opts(db, "key", "value", opts)
      assert {:ok, ttl} = Redlite.ttl(db, "key")
      assert ttl > 0 and ttl <= 3600
    end

    test "SET_OPTS with PX (milliseconds TTL)", %{db: db} do
      opts = %Redlite.SetOptions{px: 360000}
      assert {:ok, true} = Redlite.set_opts(db, "key", "value", opts)
      assert {:ok, pttl} = Redlite.pttl(db, "key")
      assert pttl > 0 and pttl <= 360000
    end
  end

  # ==========================================================================
  # GenServer API
  # ==========================================================================

  describe "GenServer wrapper" do
    test "start_link and basic operations" do
      {:ok, pid} = Redlite.start_link(path: ":memory:")
      :ok = Redlite.set(pid, "key", "value")
      assert {:ok, "value"} = Redlite.get(pid, "key")
    end

    test "start_link with name" do
      {:ok, _pid} = Redlite.start_link(path: ":memory:", name: TestRedlite)
      :ok = Redlite.set(TestRedlite, "key", "value")
      assert {:ok, "value"} = Redlite.get(TestRedlite, "key")
    end
  end
end
