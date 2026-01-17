#include <catch2/catch_test_macros.hpp>
#include <redlite/redlite.hpp>
#include <algorithm>

using namespace redlite;

TEST_CASE("Hash commands", "[hashes]") {
    auto db = Database::open_memory();

    SECTION("HSET and HGET single field") {
        auto count = db.hset("myhash", "name", "Alice");
        REQUIRE(count == 1);

        auto result = db.hget("myhash", "name");
        REQUIRE(result.has_value());
        REQUIRE(result.value() == "Alice");
    }

    SECTION("HGET returns nullopt for non-existent field") {
        db.hset("myhash", "name", "Alice");
        REQUIRE_FALSE(db.hget("myhash", "age").has_value());
        REQUIRE_FALSE(db.hget("nonexistent", "field").has_value());
    }

    SECTION("HSET multiple fields") {
        std::unordered_map<std::string, std::string> fields = {
            {"name", "Alice"},
            {"age", "30"},
            {"city", "NYC"}
        };
        auto count = db.hset("myhash", fields);
        REQUIRE(count == 3);

        REQUIRE(db.hget("myhash", "name").value() == "Alice");
        REQUIRE(db.hget("myhash", "age").value() == "30");
        REQUIRE(db.hget("myhash", "city").value() == "NYC");
    }

    SECTION("HSET updates existing field") {
        db.hset("myhash", "name", "Alice");
        auto count = db.hset("myhash", "name", "Bob");
        REQUIRE(count == 0); // Field existed
        REQUIRE(db.hget("myhash", "name").value() == "Bob");
    }

    SECTION("HDEL removes fields") {
        db.hset("myhash", {{"a", "1"}, {"b", "2"}, {"c", "3"}});

        std::vector<std::string> fields_to_del = {"a", "b", "nonexistent"};
        auto count = db.hdel("myhash", fields_to_del);
        REQUIRE(count == 2);

        REQUIRE_FALSE(db.hget("myhash", "a").has_value());
        REQUIRE_FALSE(db.hget("myhash", "b").has_value());
        REQUIRE(db.hget("myhash", "c").has_value());
    }

    SECTION("HEXISTS checks field existence") {
        db.hset("myhash", "name", "Alice");

        REQUIRE(db.hexists("myhash", "name"));
        REQUIRE_FALSE(db.hexists("myhash", "age"));
        REQUIRE_FALSE(db.hexists("nonexistent", "field"));
    }

    SECTION("HLEN returns field count") {
        REQUIRE(db.hlen("myhash") == 0);

        db.hset("myhash", "a", "1");
        REQUIRE(db.hlen("myhash") == 1);

        db.hset("myhash", {{"b", "2"}, {"c", "3"}});
        REQUIRE(db.hlen("myhash") == 3);
    }

    SECTION("HKEYS returns all field names") {
        db.hset("myhash", {{"name", "Alice"}, {"age", "30"}, {"city", "NYC"}});

        auto keys = db.hkeys("myhash");
        REQUIRE(keys.size() == 3);
        REQUIRE(std::find(keys.begin(), keys.end(), "name") != keys.end());
        REQUIRE(std::find(keys.begin(), keys.end(), "age") != keys.end());
        REQUIRE(std::find(keys.begin(), keys.end(), "city") != keys.end());
    }

    SECTION("HVALS returns all values") {
        db.hset("myhash", {{"name", "Alice"}, {"age", "30"}});

        auto vals = db.hvals("myhash");
        REQUIRE(vals.size() == 2);
        REQUIRE(std::find(vals.begin(), vals.end(), "Alice") != vals.end());
        REQUIRE(std::find(vals.begin(), vals.end(), "30") != vals.end());
    }

    SECTION("HINCRBY increments integer field") {
        db.hset("myhash", "counter", "10");

        REQUIRE(db.hincrby("myhash", "counter", 5) == 15);
        REQUIRE(db.hincrby("myhash", "counter", -3) == 12);
    }

    SECTION("HINCRBY creates field if not exists") {
        REQUIRE(db.hincrby("myhash", "newcounter", 10) == 10);
    }

    SECTION("HGETALL returns all fields and values") {
        db.hset("myhash", {{"name", "Alice"}, {"age", "30"}});

        auto all = db.hgetall("myhash");
        REQUIRE(all.size() == 2);
        REQUIRE(all["name"] == "Alice");
        REQUIRE(all["age"] == "30");
    }

    SECTION("HMGET returns multiple fields") {
        db.hset("myhash", {{"a", "1"}, {"b", "2"}, {"c", "3"}});

        auto vals = db.hmget("myhash", {"a", "c", "nonexistent"});
        REQUIRE(vals.size() == 3);
        REQUIRE(vals[0].value() == "1");
        REQUIRE(vals[1].value() == "3");
        REQUIRE_FALSE(vals[2].has_value());
    }
}
