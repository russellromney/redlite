--[[
Redlite Lua SDK Tests

Run with: busted spec/

Prerequisites:
  - LuaJIT (with FFI support)
  - busted test framework: luarocks install busted
  - REDLITE_LIB_PATH environment variable set, or library in standard location
]]

-- Add the parent directory to path for require
package.path = "../?.lua;../redlite/?.lua;" .. package.path

local redlite = require("redlite")

describe("Redlite Lua SDK", function()
    local db

    before_each(function()
        db = redlite.open_memory()
    end)

    after_each(function()
        if db then
            db:close()
        end
    end)

    -- ==========================================================================
    -- Lifecycle Tests
    -- ==========================================================================

    describe("lifecycle", function()
        it("should open an in-memory database", function()
            assert.is_not_nil(db)
        end)

        it("should get version", function()
            local version = redlite.version()
            assert.is_string(version)
            assert.is_true(#version > 0)
        end)

        it("should close database", function()
            db:close()
            assert.has_error(function() db:get("key") end, "database is closed")
        end)
    end)

    -- ==========================================================================
    -- String Commands Tests
    -- ==========================================================================

    describe("string commands", function()
        it("should SET and GET a value", function()
            db:set("hello", "world")
            assert.equals("world", db:get("hello"))
        end)

        it("should return nil for missing key", function()
            assert.is_nil(db:get("nonexistent"))
        end)

        it("should SET with TTL", function()
            db:set("key", "value", 3600)
            assert.equals("value", db:get("key"))
            assert.is_true(db:ttl("key") > 0)
        end)

        it("should SETEX", function()
            db:setex("key", 60, "value")
            assert.equals("value", db:get("key"))
            assert.is_true(db:ttl("key") > 0)
        end)

        it("should PSETEX", function()
            db:psetex("key", 60000, "value")
            assert.equals("value", db:get("key"))
            assert.is_true(db:pttl("key") > 0)
        end)

        it("should GETDEL", function()
            db:set("key", "value")
            assert.equals("value", db:getdel("key"))
            assert.is_nil(db:get("key"))
        end)

        it("should APPEND", function()
            db:set("key", "hello")
            local len = db:append("key", " world")
            assert.equals(11, len)
            assert.equals("hello world", db:get("key"))
        end)

        it("should STRLEN", function()
            db:set("key", "hello")
            assert.equals(5, db:strlen("key"))
        end)

        it("should GETRANGE", function()
            db:set("key", "hello world")
            assert.equals("world", db:getrange("key", 6, 10))
        end)

        it("should SETRANGE", function()
            db:set("key", "hello world")
            local len = db:setrange("key", 6, "WORLD")
            assert.equals(11, len)
            assert.equals("hello WORLD", db:get("key"))
        end)

        it("should INCR", function()
            db:set("counter", "10")
            assert.equals(11, db:incr("counter"))
            assert.equals(12, db:incr("counter"))
        end)

        it("should DECR", function()
            db:set("counter", "10")
            assert.equals(9, db:decr("counter"))
        end)

        it("should INCRBY", function()
            db:set("counter", "10")
            assert.equals(15, db:incrby("counter", 5))
        end)

        it("should DECRBY", function()
            db:set("counter", "10")
            assert.equals(5, db:decrby("counter", 5))
        end)

        it("should INCRBYFLOAT", function()
            db:set("counter", "10.5")
            local result = db:incrbyfloat("counter", 0.5)
            assert.is_true(math.abs(result - 11.0) < 0.001)
        end)

        it("should MGET", function()
            db:set("k1", "v1")
            db:set("k2", "v2")
            local values = db:mget("k1", "k2", "k3")
            assert.equals("v1", values[1])
            assert.equals("v2", values[2])
            assert.is_nil(values[3])
        end)

        it("should MSET", function()
            db:mset({k1 = "v1", k2 = "v2"})
            assert.equals("v1", db:get("k1"))
            assert.equals("v2", db:get("k2"))
        end)
    end)

    -- ==========================================================================
    -- Key Commands Tests
    -- ==========================================================================

    describe("key commands", function()
        it("should DEL keys", function()
            db:set("k1", "v1")
            db:set("k2", "v2")
            assert.equals(2, db:del("k1", "k2", "k3"))
            assert.is_nil(db:get("k1"))
        end)

        it("should EXISTS", function()
            db:set("k1", "v1")
            assert.equals(1, db:exists("k1"))
            assert.equals(0, db:exists("nonexistent"))
            assert.equals(1, db:exists("k1", "nonexistent"))
        end)

        it("should TYPE", function()
            db:set("string", "value")
            assert.equals("string", db:type("string"))
            assert.equals("none", db:type("nonexistent"))
        end)

        it("should TTL and PTTL", function()
            db:set("key", "value")
            assert.equals(-1, db:ttl("key"))  -- No TTL
            assert.equals(-2, db:ttl("nonexistent"))  -- Doesn't exist
        end)

        it("should EXPIRE and PERSIST", function()
            db:set("key", "value")
            assert.is_true(db:expire("key", 3600))
            assert.is_true(db:ttl("key") > 0)
            assert.is_true(db:persist("key"))
            assert.equals(-1, db:ttl("key"))
        end)

        it("should PEXPIRE", function()
            db:set("key", "value")
            assert.is_true(db:pexpire("key", 60000))
            assert.is_true(db:pttl("key") > 0)
        end)

        it("should RENAME", function()
            db:set("old", "value")
            db:rename("old", "new")
            assert.is_nil(db:get("old"))
            assert.equals("value", db:get("new"))
        end)

        it("should RENAMENX", function()
            db:set("k1", "v1")
            db:set("k2", "v2")
            assert.is_false(db:renamenx("k1", "k2"))  -- k2 exists
            assert.is_true(db:renamenx("k1", "k3"))   -- k3 doesn't exist
        end)

        it("should KEYS", function()
            db:set("foo:1", "v1")
            db:set("foo:2", "v2")
            db:set("bar:1", "v3")
            local keys = db:keys("foo:*")
            assert.equals(2, #keys)
        end)

        it("should DBSIZE", function()
            assert.equals(0, db:dbsize())
            db:set("k1", "v1")
            db:set("k2", "v2")
            assert.equals(2, db:dbsize())
        end)

        it("should FLUSHDB", function()
            db:set("k1", "v1")
            db:set("k2", "v2")
            db:flushdb()
            assert.equals(0, db:dbsize())
        end)
    end)

    -- ==========================================================================
    -- Hash Commands Tests
    -- ==========================================================================

    describe("hash commands", function()
        it("should HSET and HGET", function()
            db:hset("hash", "field", "value")
            assert.equals("value", db:hget("hash", "field"))
        end)

        it("should HSET with mapping", function()
            db:hset("hash", {f1 = "v1", f2 = "v2"})
            assert.equals("v1", db:hget("hash", "f1"))
            assert.equals("v2", db:hget("hash", "f2"))
        end)

        it("should HDEL", function()
            db:hset("hash", {f1 = "v1", f2 = "v2"})
            assert.equals(1, db:hdel("hash", "f1"))
            assert.is_nil(db:hget("hash", "f1"))
        end)

        it("should HEXISTS", function()
            db:hset("hash", "field", "value")
            assert.is_true(db:hexists("hash", "field"))
            assert.is_false(db:hexists("hash", "nonexistent"))
        end)

        it("should HLEN", function()
            db:hset("hash", {f1 = "v1", f2 = "v2", f3 = "v3"})
            assert.equals(3, db:hlen("hash"))
        end)

        it("should HKEYS", function()
            db:hset("hash", {f1 = "v1", f2 = "v2"})
            local keys = db:hkeys("hash")
            assert.equals(2, #keys)
        end)

        it("should HVALS", function()
            db:hset("hash", {f1 = "v1", f2 = "v2"})
            local vals = db:hvals("hash")
            assert.equals(2, #vals)
        end)

        it("should HINCRBY", function()
            db:hset("hash", "counter", "10")
            assert.equals(15, db:hincrby("hash", "counter", 5))
        end)

        it("should HGETALL", function()
            db:hset("hash", {f1 = "v1", f2 = "v2"})
            local all = db:hgetall("hash")
            assert.equals("v1", all.f1)
            assert.equals("v2", all.f2)
        end)

        it("should HMGET", function()
            db:hset("hash", {f1 = "v1", f2 = "v2"})
            local vals = db:hmget("hash", "f1", "f2", "f3")
            assert.equals("v1", vals[1])
            assert.equals("v2", vals[2])
            assert.is_nil(vals[3])
        end)
    end)

    -- ==========================================================================
    -- List Commands Tests
    -- ==========================================================================

    describe("list commands", function()
        it("should LPUSH and RPUSH", function()
            db:lpush("list", "a", "b")
            db:rpush("list", "c", "d")
            assert.equals(4, db:llen("list"))
        end)

        it("should LPOP and RPOP", function()
            db:rpush("list", "a", "b", "c")
            assert.equals("a", db:lpop("list"))
            assert.equals("c", db:rpop("list"))
        end)

        it("should LLEN", function()
            db:rpush("list", "a", "b", "c")
            assert.equals(3, db:llen("list"))
        end)

        it("should LRANGE", function()
            db:rpush("list", "a", "b", "c", "d")
            local range = db:lrange("list", 1, 2)
            assert.equals(2, #range)
            assert.equals("b", range[1])
            assert.equals("c", range[2])
        end)

        it("should LINDEX", function()
            db:rpush("list", "a", "b", "c")
            assert.equals("b", db:lindex("list", 1))
            assert.is_nil(db:lindex("list", 100))
        end)

        it("should LPOP multiple", function()
            db:rpush("list", "a", "b", "c", "d")
            local popped = db:lpop("list", 2)
            assert.equals(2, #popped)
            assert.equals("a", popped[1])
            assert.equals("b", popped[2])
        end)
    end)

    -- ==========================================================================
    -- Set Commands Tests
    -- ==========================================================================

    describe("set commands", function()
        it("should SADD and SMEMBERS", function()
            assert.equals(3, db:sadd("set", "a", "b", "c"))
            local members = db:smembers("set")
            assert.equals(3, #members)
        end)

        it("should SREM", function()
            db:sadd("set", "a", "b", "c")
            assert.equals(1, db:srem("set", "a"))
            assert.equals(2, db:scard("set"))
        end)

        it("should SISMEMBER", function()
            db:sadd("set", "a", "b")
            assert.is_true(db:sismember("set", "a"))
            assert.is_false(db:sismember("set", "c"))
        end)

        it("should SCARD", function()
            db:sadd("set", "a", "b", "c")
            assert.equals(3, db:scard("set"))
        end)
    end)

    -- ==========================================================================
    -- Sorted Set Commands Tests
    -- ==========================================================================

    describe("sorted set commands", function()
        it("should ZADD with score and member", function()
            assert.equals(1, db:zadd("zset", 1.0, "a"))
            assert.equals(1, db:zadd("zset", 2.0, "b"))
        end)

        it("should ZADD with mapping", function()
            db:zadd("zset", {a = 1.0, b = 2.0, c = 3.0})
            assert.equals(3, db:zcard("zset"))
        end)

        it("should ZADD with array of pairs", function()
            db:zadd("zset", {{1.0, "a"}, {2.0, "b"}})
            assert.equals(2, db:zcard("zset"))
        end)

        it("should ZSCORE", function()
            db:zadd("zset", 1.5, "member")
            local score = db:zscore("zset", "member")
            assert.is_true(math.abs(score - 1.5) < 0.001)
        end)

        it("should return nil for missing ZSCORE", function()
            db:zadd("zset", 1.0, "a")
            assert.is_nil(db:zscore("zset", "nonexistent"))
        end)

        it("should ZCARD", function()
            db:zadd("zset", {a = 1.0, b = 2.0})
            assert.equals(2, db:zcard("zset"))
        end)

        it("should ZCOUNT", function()
            db:zadd("zset", {a = 1.0, b = 2.0, c = 3.0})
            assert.equals(2, db:zcount("zset", 1.5, 3.5))
        end)

        it("should ZINCRBY", function()
            db:zadd("zset", 1.0, "member")
            local newScore = db:zincrby("zset", 0.5, "member")
            assert.is_true(math.abs(newScore - 1.5) < 0.001)
        end)

        it("should ZREM", function()
            db:zadd("zset", {a = 1.0, b = 2.0})
            assert.equals(1, db:zrem("zset", "a"))
            assert.equals(1, db:zcard("zset"))
        end)

        it("should ZRANGE", function()
            db:zadd("zset", {a = 1.0, b = 2.0, c = 3.0})
            local range = db:zrange("zset", 0, 1)
            assert.equals(2, #range)
            assert.equals("a", range[1])
            assert.equals("b", range[2])
        end)

        it("should ZRANGE with scores", function()
            db:zadd("zset", {a = 1.0, b = 2.0})
            local range = db:zrange("zset", 0, -1, true)
            assert.equals(2, #range)
            assert.equals("a", range[1][1])
            assert.is_true(math.abs(range[1][2] - 1.0) < 0.001)
        end)

        it("should ZREVRANGE", function()
            db:zadd("zset", {a = 1.0, b = 2.0, c = 3.0})
            local range = db:zrevrange("zset", 0, 1)
            assert.equals(2, #range)
            assert.equals("c", range[1])
            assert.equals("b", range[2])
        end)
    end)

    -- ==========================================================================
    -- Server Commands Tests
    -- ==========================================================================

    describe("server commands", function()
        it("should VACUUM", function()
            db:set("key", "value")
            local bytes = db:vacuum()
            assert.is_number(bytes)
        end)

        it("should SELECT database", function()
            db:set("key", "value")
            db:select(1)
            assert.is_nil(db:get("key"))  -- Different database
            db:select(0)
            assert.equals("value", db:get("key"))
        end)
    end)
end)
