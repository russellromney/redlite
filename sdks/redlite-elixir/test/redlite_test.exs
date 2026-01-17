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
