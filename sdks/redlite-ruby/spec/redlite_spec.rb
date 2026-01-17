# frozen_string_literal: true

require "spec_helper"

RSpec.describe Redlite do
  describe ".version" do
    it "returns a version string" do
      expect(Redlite::VERSION).to match(/\d+\.\d+\.\d+/)
    end
  end

  describe ".open" do
    it "opens an in-memory database" do
      db = Redlite.open
      expect(db).to be_a(Redlite::Database)
      expect(db.closed?).to be false
      db.close
      expect(db.closed?).to be true
    end

    it "supports block syntax" do
      result = Redlite.open do |db|
        db.set("key", "value")
        db.get("key")
      end
      expect(result).to eq("value")
    end
  end

  describe Redlite::Database do
    describe "string commands" do
      it "SET and GET roundtrip" do
        with_db do |db|
          expect(db.set("key", "value")).to be true
          expect(db.get("key")).to eq("value")
        end
      end

      it "returns nil for missing key" do
        with_db do |db|
          expect(db.get("nonexistent")).to be_nil
        end
      end

      it "SET with expiration" do
        with_db do |db|
          db.set("key", "value", ex: 100)
          expect(db.ttl("key")).to be > 0
        end
      end

      it "SETEX" do
        with_db do |db|
          db.setex("key", 100, "value")
          expect(db.get("key")).to eq("value")
          expect(db.ttl("key")).to be > 0
        end
      end

      it "GETDEL" do
        with_db do |db|
          db.set("key", "value")
          expect(db.getdel("key")).to eq("value")
          expect(db.get("key")).to be_nil
        end
      end

      it "APPEND" do
        with_db do |db|
          db.set("key", "Hello")
          length = db.append("key", " World")
          expect(length).to eq(11)
          expect(db.get("key")).to eq("Hello World")
        end
      end

      it "STRLEN" do
        with_db do |db|
          db.set("key", "Hello")
          expect(db.strlen("key")).to eq(5)
          expect(db.strlen("nonexistent")).to eq(0)
        end
      end

      it "GETRANGE" do
        with_db do |db|
          db.set("key", "Hello World")
          expect(db.getrange("key", 0, 4)).to eq("Hello")
          expect(db.getrange("key", -5, -1)).to eq("World")
        end
      end

      it "SETRANGE" do
        with_db do |db|
          db.set("key", "Hello World")
          db.setrange("key", 6, "Redis")
          expect(db.get("key")).to eq("Hello Redis")
        end
      end

      it "INCR/DECR" do
        with_db do |db|
          db.set("counter", "10")
          expect(db.incr("counter")).to eq(11)
          expect(db.decr("counter")).to eq(10)
          expect(db.incrby("counter", 5)).to eq(15)
          expect(db.decrby("counter", 3)).to eq(12)
        end
      end

      it "INCRBYFLOAT" do
        with_db do |db|
          db.set("float", "10.5")
          result = db.incrbyfloat("float", 0.1)
          expect(result).to be_within(0.001).of(10.6)
        end
      end

      it "MGET/MSET" do
        with_db do |db|
          db.mset("k1" => "v1", "k2" => "v2", "k3" => "v3")
          expect(db.mget("k1", "k2", "k3")).to eq(["v1", "v2", "v3"])
          expect(db.mget("k1", "nonexistent", "k3")).to eq(["v1", nil, "v3"])
        end
      end
    end

    describe "key commands" do
      it "DEL" do
        with_db do |db|
          db.set("k1", "v1")
          db.set("k2", "v2")
          expect(db.del("k1", "k2", "k3")).to eq(2)
          expect(db.get("k1")).to be_nil
        end
      end

      it "EXISTS" do
        with_db do |db|
          db.set("k1", "v1")
          db.set("k2", "v2")
          expect(db.exists("k1", "k2", "k3")).to eq(2)
        end
      end

      it "TYPE" do
        with_db do |db|
          db.set("string", "value")
          db.lpush("list", "item")
          db.sadd("set", "member")
          db.zadd("zset", { "member" => 1.0 })
          db.hset("hash", { "field" => "value" })

          expect(db.type("string")).to eq("string")
          expect(db.type("list")).to eq("list")
          expect(db.type("set")).to eq("set")
          expect(db.type("zset")).to eq("zset")
          expect(db.type("hash")).to eq("hash")
          expect(db.type("nonexistent")).to eq("none")
        end
      end

      it "TTL/PTTL" do
        with_db do |db|
          db.set("key", "value")
          expect(db.ttl("key")).to eq(-1)
          expect(db.ttl("nonexistent")).to eq(-2)

          db.expire("key", 100)
          expect(db.ttl("key")).to be > 0
          expect(db.pttl("key")).to be > 0
        end
      end

      it "EXPIRE/PEXPIRE" do
        with_db do |db|
          db.set("key", "value")
          expect(db.expire("key", 100)).to be true
          expect(db.expire("nonexistent", 100)).to be false
        end
      end

      it "PERSIST" do
        with_db do |db|
          db.setex("key", 100, "value")
          expect(db.persist("key")).to be true
          expect(db.ttl("key")).to eq(-1)
        end
      end

      it "RENAME" do
        with_db do |db|
          db.set("old", "value")
          db.rename("old", "new")
          expect(db.get("old")).to be_nil
          expect(db.get("new")).to eq("value")
        end
      end

      it "RENAMENX" do
        with_db do |db|
          db.set("old", "value")
          db.set("existing", "other")
          expect(db.renamenx("old", "new")).to be true
          expect(db.renamenx("new", "existing")).to be false
        end
      end

      it "KEYS" do
        with_db do |db|
          db.set("key1", "v1")
          db.set("key2", "v2")
          db.set("other", "v3")

          keys = db.keys("key*")
          expect(keys.sort).to eq(["key1", "key2"])
        end
      end

      it "DBSIZE" do
        with_db do |db|
          expect(db.dbsize).to eq(0)
          db.set("k1", "v1")
          db.set("k2", "v2")
          expect(db.dbsize).to eq(2)
        end
      end

      it "FLUSHDB" do
        with_db do |db|
          db.set("k1", "v1")
          db.set("k2", "v2")
          db.flushdb
          expect(db.dbsize).to eq(0)
        end
      end
    end

    describe "hash commands" do
      it "HSET/HGET" do
        with_db do |db|
          db.hset("hash", { "field" => "value" })
          expect(db.hget("hash", "field")).to eq("value")
          expect(db.hget("hash", "nonexistent")).to be_nil
        end
      end

      it "HDEL" do
        with_db do |db|
          db.hset("hash", { "f1" => "v1", "f2" => "v2" })
          expect(db.hdel("hash", "f1")).to eq(1)
          expect(db.hget("hash", "f1")).to be_nil
        end
      end

      it "HEXISTS" do
        with_db do |db|
          db.hset("hash", { "field" => "value" })
          expect(db.hexists("hash", "field")).to be true
          expect(db.hexists("hash", "nonexistent")).to be false
        end
      end

      it "HLEN" do
        with_db do |db|
          db.hset("hash", { "f1" => "v1", "f2" => "v2" })
          expect(db.hlen("hash")).to eq(2)
        end
      end

      it "HKEYS/HVALS" do
        with_db do |db|
          db.hset("hash", { "f1" => "v1", "f2" => "v2" })
          expect(db.hkeys("hash").sort).to eq(["f1", "f2"])
          expect(db.hvals("hash").sort).to eq(["v1", "v2"])
        end
      end

      it "HGETALL" do
        with_db do |db|
          db.hset("hash", { "f1" => "v1", "f2" => "v2" })
          expect(db.hgetall("hash")).to eq({ "f1" => "v1", "f2" => "v2" })
        end
      end

      it "HMGET" do
        with_db do |db|
          db.hset("hash", { "f1" => "v1", "f2" => "v2" })
          expect(db.hmget("hash", "f1", "f2", "f3")).to eq(["v1", "v2", nil])
        end
      end

      it "HINCRBY" do
        with_db do |db|
          db.hset("hash", { "counter" => "10" })
          expect(db.hincrby("hash", "counter", 5)).to eq(15)
        end
      end
    end

    describe "list commands" do
      it "LPUSH/RPUSH" do
        with_db do |db|
          expect(db.lpush("list", "a", "b")).to eq(2)
          expect(db.rpush("list", "c", "d")).to eq(4)
          expect(db.lrange("list", 0, -1)).to eq(["b", "a", "c", "d"])
        end
      end

      it "LPOP/RPOP" do
        with_db do |db|
          db.rpush("list", "a", "b", "c")
          expect(db.lpop("list")).to eq("a")
          expect(db.rpop("list")).to eq("c")
          expect(db.lrange("list", 0, -1)).to eq(["b"])
        end
      end

      it "LPOP/RPOP with count" do
        with_db do |db|
          db.rpush("list", "a", "b", "c", "d")
          expect(db.lpop("list", 2)).to eq(["a", "b"])
          expect(db.rpop("list", 2)).to eq(["d", "c"])
        end
      end

      it "LLEN" do
        with_db do |db|
          db.rpush("list", "a", "b", "c")
          expect(db.llen("list")).to eq(3)
        end
      end

      it "LRANGE" do
        with_db do |db|
          db.rpush("list", "a", "b", "c", "d", "e")
          expect(db.lrange("list", 0, 2)).to eq(["a", "b", "c"])
          expect(db.lrange("list", -2, -1)).to eq(["d", "e"])
        end
      end

      it "LINDEX" do
        with_db do |db|
          db.rpush("list", "a", "b", "c")
          expect(db.lindex("list", 0)).to eq("a")
          expect(db.lindex("list", -1)).to eq("c")
          expect(db.lindex("list", 100)).to be_nil
        end
      end
    end

    describe "set commands" do
      it "SADD/SMEMBERS" do
        with_db do |db|
          expect(db.sadd("set", "a", "b", "c")).to eq(3)
          expect(db.smembers("set").sort).to eq(["a", "b", "c"])
        end
      end

      it "SREM" do
        with_db do |db|
          db.sadd("set", "a", "b", "c")
          expect(db.srem("set", "a", "b")).to eq(2)
          expect(db.smembers("set")).to eq(["c"])
        end
      end

      it "SISMEMBER" do
        with_db do |db|
          db.sadd("set", "a", "b")
          expect(db.sismember("set", "a")).to be true
          expect(db.sismember("set", "z")).to be false
        end
      end

      it "SCARD" do
        with_db do |db|
          db.sadd("set", "a", "b", "c")
          expect(db.scard("set")).to eq(3)
        end
      end
    end

    describe "sorted set commands" do
      it "ZADD/ZRANGE" do
        with_db do |db|
          expect(db.zadd("zset", { "a" => 1.0, "b" => 2.0, "c" => 3.0 })).to eq(3)
          expect(db.zrange("zset", 0, -1)).to eq(["a", "b", "c"])
        end
      end

      it "ZRANGE with scores" do
        with_db do |db|
          db.zadd("zset", { "a" => 1.0, "b" => 2.0 })
          result = db.zrange("zset", 0, -1, with_scores: true)
          expect(result).to eq([["a", 1.0], ["b", 2.0]])
        end
      end

      it "ZREM" do
        with_db do |db|
          db.zadd("zset", { "a" => 1.0, "b" => 2.0, "c" => 3.0 })
          expect(db.zrem("zset", "a", "b")).to eq(2)
          expect(db.zrange("zset", 0, -1)).to eq(["c"])
        end
      end

      it "ZSCORE" do
        with_db do |db|
          db.zadd("zset", { "member" => 1.5 })
          expect(db.zscore("zset", "member")).to eq(1.5)
          expect(db.zscore("zset", "nonexistent")).to be_nil
        end
      end

      it "ZCARD" do
        with_db do |db|
          db.zadd("zset", { "a" => 1.0, "b" => 2.0 })
          expect(db.zcard("zset")).to eq(2)
        end
      end

      it "ZCOUNT" do
        with_db do |db|
          db.zadd("zset", { "a" => 1.0, "b" => 2.0, "c" => 3.0 })
          expect(db.zcount("zset", 1.0, 2.0)).to eq(2)
        end
      end

      it "ZINCRBY" do
        with_db do |db|
          db.zadd("zset", { "member" => 1.0 })
          expect(db.zincrby("zset", 2.5, "member")).to eq(3.5)
        end
      end

      it "ZREVRANGE" do
        with_db do |db|
          db.zadd("zset", { "a" => 1.0, "b" => 2.0, "c" => 3.0 })
          expect(db.zrevrange("zset", 0, -1)).to eq(["c", "b", "a"])
        end
      end
    end

    describe "error handling" do
      it "raises error when database is closed" do
        db = Redlite::Database.new
        db.close
        expect { db.get("key") }.to raise_error(Redlite::ConnectionClosedError)
      end
    end
  end
end
