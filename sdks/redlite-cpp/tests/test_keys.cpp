#include <catch2/catch_test_macros.hpp>
#include <redlite/redlite.hpp>
#include <algorithm>

using namespace redlite;

TEST_CASE("Key commands", "[keys]") {
    auto db = Database::open_memory();

    SECTION("DEL single key") {
        db.set("delkey", "value");
        REQUIRE(db.del("delkey") == 1);
        REQUIRE_FALSE(db.exists("delkey"));
    }

    SECTION("DEL multiple keys") {
        db.set("key1", "v1");
        db.set("key2", "v2");
        db.set("key3", "v3");

        std::vector<std::string> keys = {"key1", "key2", "nonexistent"};
        REQUIRE(db.del(keys) == 2);
    }

    SECTION("EXISTS returns count of existing keys") {
        db.set("exists1", "v1");
        db.set("exists2", "v2");

        REQUIRE(db.exists("exists1"));
        REQUIRE_FALSE(db.exists("nonexistent"));

        std::vector<std::string> keys = {"exists1", "exists2", "nonexistent"};
        REQUIRE(db.exists(keys) == 2);
    }

    SECTION("TYPE returns correct type") {
        db.set("strkey", "value");
        REQUIRE(db.type("strkey").value() == "string");

        db.lpush("listkey", "value");
        REQUIRE(db.type("listkey").value() == "list");

        db.sadd("setkey", "member");
        REQUIRE(db.type("setkey").value() == "set");

        db.hset("hashkey", "field", "value");
        REQUIRE(db.type("hashkey").value() == "hash");

        db.zadd("zsetkey", 1.0, "member");
        REQUIRE(db.type("zsetkey").value() == "zset");

        REQUIRE_FALSE(db.type("nonexistent").has_value());
    }

    SECTION("TTL and PTTL") {
        db.set("noexpire", "value");
        REQUIRE(db.ttl("noexpire") == -1);
        REQUIRE(db.pttl("noexpire") == -1);

        REQUIRE(db.ttl("nonexistent") == -2);
        REQUIRE(db.pttl("nonexistent") == -2);

        db.setex("withexpire", 60, "value");
        REQUIRE(db.ttl("withexpire") > 0);
        REQUIRE(db.ttl("withexpire") <= 60);
    }

    SECTION("EXPIRE and PEXPIRE") {
        db.set("expkey", "value");
        REQUIRE(db.expire("expkey", 60));
        REQUIRE(db.ttl("expkey") > 0);

        db.set("pexpkey", "value");
        REQUIRE(db.pexpire("pexpkey", 60000));
        REQUIRE(db.pttl("pexpkey") > 0);

        REQUIRE_FALSE(db.expire("nonexistent", 60));
    }

    SECTION("PERSIST removes TTL") {
        db.setex("persistkey", 60, "value");
        REQUIRE(db.ttl("persistkey") > 0);

        REQUIRE(db.persist("persistkey"));
        REQUIRE(db.ttl("persistkey") == -1);

        REQUIRE_FALSE(db.persist("nonexistent"));
    }

    SECTION("RENAME") {
        db.set("oldkey", "value");
        REQUIRE(db.rename("oldkey", "newkey"));
        REQUIRE_FALSE(db.exists("oldkey"));
        REQUIRE(db.get("newkey").value() == "value");
    }

    SECTION("RENAMENX only renames if target doesn't exist") {
        db.set("rnxkey", "value");
        db.set("existingkey", "existing");

        REQUIRE_FALSE(db.renamenx("rnxkey", "existingkey"));
        REQUIRE(db.get("rnxkey").has_value());

        REQUIRE(db.renamenx("rnxkey", "brandnewkey"));
        REQUIRE_FALSE(db.exists("rnxkey"));
        REQUIRE(db.get("brandnewkey").value() == "value");
    }

    SECTION("KEYS with pattern") {
        db.set("user:1", "alice");
        db.set("user:2", "bob");
        db.set("session:1", "data");

        auto all_keys = db.keys("*");
        REQUIRE(all_keys.size() == 3);

        auto user_keys = db.keys("user:*");
        REQUIRE(user_keys.size() == 2);
        REQUIRE(std::find(user_keys.begin(), user_keys.end(), "user:1") != user_keys.end());
        REQUIRE(std::find(user_keys.begin(), user_keys.end(), "user:2") != user_keys.end());
    }

    SECTION("DBSIZE returns key count") {
        REQUIRE(db.dbsize() == 0);
        db.set("k1", "v1");
        REQUIRE(db.dbsize() == 1);
        db.set("k2", "v2");
        REQUIRE(db.dbsize() == 2);
    }

    SECTION("FLUSHDB clears all keys") {
        db.set("k1", "v1");
        db.set("k2", "v2");
        REQUIRE(db.dbsize() == 2);

        REQUIRE(db.flushdb());
        REQUIRE(db.dbsize() == 0);
    }

    SECTION("SELECT switches database") {
        db.set("key", "db0");
        REQUIRE(db.select(1));
        REQUIRE_FALSE(db.exists("key"));

        db.set("key", "db1");
        REQUIRE(db.select(0));
        REQUIRE(db.get("key").value() == "db0");
    }
}
