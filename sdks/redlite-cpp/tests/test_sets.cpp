#include <catch2/catch_test_macros.hpp>
#include <redlite/redlite.hpp>
#include <algorithm>

using namespace redlite;

TEST_CASE("Set commands", "[sets]") {
    auto db = Database::open_memory();

    SECTION("SADD single member") {
        REQUIRE(db.sadd("myset", "a") == 1);
        REQUIRE(db.sadd("myset", "a") == 0); // Already exists
        REQUIRE(db.sadd("myset", "b") == 1);
    }

    SECTION("SADD multiple members") {
        std::vector<std::string> members = {"a", "b", "c"};
        REQUIRE(db.sadd("myset", members) == 3);
        REQUIRE(db.scard("myset") == 3);

        // Adding mix of new and existing
        members = {"c", "d", "e"};
        REQUIRE(db.sadd("myset", members) == 2); // c already exists
    }

    SECTION("SREM removes members") {
        db.sadd("myset", {"a", "b", "c", "d"});

        std::vector<std::string> to_remove = {"a", "b", "nonexistent"};
        REQUIRE(db.srem("myset", to_remove) == 2);
        REQUIRE(db.scard("myset") == 2);

        REQUIRE_FALSE(db.sismember("myset", "a"));
        REQUIRE_FALSE(db.sismember("myset", "b"));
        REQUIRE(db.sismember("myset", "c"));
    }

    SECTION("SMEMBERS returns all members") {
        db.sadd("myset", {"a", "b", "c"});

        auto members = db.smembers("myset");
        REQUIRE(members.size() == 3);
        REQUIRE(std::find(members.begin(), members.end(), "a") != members.end());
        REQUIRE(std::find(members.begin(), members.end(), "b") != members.end());
        REQUIRE(std::find(members.begin(), members.end(), "c") != members.end());
    }

    SECTION("SMEMBERS on empty/non-existent set") {
        auto members = db.smembers("nonexistent");
        REQUIRE(members.empty());
    }

    SECTION("SISMEMBER checks membership") {
        db.sadd("myset", {"a", "b", "c"});

        REQUIRE(db.sismember("myset", "a"));
        REQUIRE(db.sismember("myset", "b"));
        REQUIRE_FALSE(db.sismember("myset", "x"));
        REQUIRE_FALSE(db.sismember("nonexistent", "a"));
    }

    SECTION("SCARD returns set cardinality") {
        REQUIRE(db.scard("myset") == 0);

        db.sadd("myset", "a");
        REQUIRE(db.scard("myset") == 1);

        db.sadd("myset", {"b", "c", "d"});
        REQUIRE(db.scard("myset") == 4);

        db.srem("myset", {"a"});
        REQUIRE(db.scard("myset") == 3);
    }

    SECTION("Set maintains uniqueness") {
        db.sadd("myset", "a");
        db.sadd("myset", "a");
        db.sadd("myset", "a");
        REQUIRE(db.scard("myset") == 1);

        db.sadd("myset", {"a", "a", "b", "b", "c"});
        REQUIRE(db.scard("myset") == 3);
    }

    SECTION("Set with binary data") {
        std::string binary_member = "hello\x00world";
        db.sadd("binset", binary_member);
        REQUIRE(db.sismember("binset", binary_member));
    }
}
