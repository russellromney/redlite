#include <catch2/catch_test_macros.hpp>
#include <catch2/catch_approx.hpp>
#include <redlite/redlite.hpp>

using namespace redlite;
using Catch::Approx;

TEST_CASE("String commands", "[strings]") {
    auto db = Database::open_memory();

    SECTION("SET and GET basic operations") {
        REQUIRE(db.set("key1", "value1"));
        auto result = db.get("key1");
        REQUIRE(result.has_value());
        REQUIRE(result.value() == "value1");
    }

    SECTION("GET returns nullopt for non-existent key") {
        auto result = db.get("nonexistent");
        REQUIRE_FALSE(result.has_value());
    }

    SECTION("SET with TTL") {
        REQUIRE(db.set("expkey", "value", 10));
        auto ttl = db.ttl("expkey");
        REQUIRE(ttl > 0);
        REQUIRE(ttl <= 10);
    }

    SECTION("SETEX sets with expiration") {
        REQUIRE(db.setex("setexkey", 60, "value"));
        auto ttl = db.ttl("setexkey");
        REQUIRE(ttl > 0);
        REQUIRE(ttl <= 60);
    }

    SECTION("PSETEX sets with millisecond expiration") {
        REQUIRE(db.psetex("psetexkey", 60000, "value"));
        auto pttl = db.pttl("psetexkey");
        REQUIRE(pttl > 0);
        REQUIRE(pttl <= 60000);
    }

    SECTION("GETDEL retrieves and deletes") {
        db.set("delkey", "value");
        auto result = db.getdel("delkey");
        REQUIRE(result.has_value());
        REQUIRE(result.value() == "value");
        REQUIRE_FALSE(db.exists("delkey"));
    }

    SECTION("APPEND to existing key") {
        db.set("appendkey", "Hello");
        auto len = db.append("appendkey", " World");
        REQUIRE(len == 11);
        REQUIRE(db.get("appendkey").value() == "Hello World");
    }

    SECTION("APPEND to non-existent key creates it") {
        auto len = db.append("newappend", "value");
        REQUIRE(len == 5);
        REQUIRE(db.get("newappend").value() == "value");
    }

    SECTION("STRLEN returns correct length") {
        db.set("strlenkey", "Hello World");
        REQUIRE(db.strlen("strlenkey") == 11);
        REQUIRE(db.strlen("nonexistent") == 0);
    }

    SECTION("GETRANGE returns substring") {
        db.set("rangekey", "Hello World");
        REQUIRE(db.getrange("rangekey", 0, 4) == "Hello");
        REQUIRE(db.getrange("rangekey", 6, 10) == "World");
        REQUIRE(db.getrange("rangekey", -5, -1) == "World");
    }

    SECTION("SETRANGE modifies part of string") {
        db.set("setrangekey", "Hello World");
        auto len = db.setrange("setrangekey", 6, "Redis");
        REQUIRE(len == 11);
        REQUIRE(db.get("setrangekey").value() == "Hello Redis");
    }

    SECTION("INCR and DECR operations") {
        db.set("counter", "10");
        REQUIRE(db.incr("counter") == 11);
        REQUIRE(db.incr("counter") == 12);
        REQUIRE(db.decr("counter") == 11);
        REQUIRE(db.decr("counter") == 10);
    }

    SECTION("INCR on non-existent key starts from 0") {
        REQUIRE(db.incr("newcounter") == 1);
        REQUIRE(db.incr("newcounter") == 2);
    }

    SECTION("INCRBY and DECRBY operations") {
        db.set("counter", "100");
        REQUIRE(db.incrby("counter", 10) == 110);
        REQUIRE(db.decrby("counter", 25) == 85);
    }

    SECTION("INCRBYFLOAT operation") {
        db.set("floatkey", "10.5");
        auto result = db.incrbyfloat("floatkey", 2.5);
        REQUIRE(result == Approx(13.0).epsilon(0.001));
    }

    SECTION("MSET and MGET multiple keys") {
        std::unordered_map<std::string, std::string> pairs = {
            {"mkey1", "mval1"},
            {"mkey2", "mval2"},
            {"mkey3", "mval3"}
        };
        REQUIRE(db.mset(pairs));

        auto values = db.mget({"mkey1", "mkey2", "mkey3", "nonexistent"});
        REQUIRE(values.size() == 4);
        REQUIRE(values[0].has_value());
        REQUIRE(values[0].value() == "mval1");
        REQUIRE(values[1].has_value());
        REQUIRE(values[1].value() == "mval2");
        REQUIRE(values[2].has_value());
        REQUIRE(values[2].value() == "mval3");
        REQUIRE_FALSE(values[3].has_value());
    }
}
