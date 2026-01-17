#include <catch2/catch_test_macros.hpp>
#include <redlite/redlite.hpp>

using namespace redlite;

TEST_CASE("List commands", "[lists]") {
    auto db = Database::open_memory();

    SECTION("LPUSH and RPUSH single value") {
        REQUIRE(db.lpush("mylist", "a") == 1);
        REQUIRE(db.rpush("mylist", "b") == 2);

        auto range = db.lrange("mylist", 0, -1);
        REQUIRE(range.size() == 2);
        REQUIRE(range[0] == "a");
        REQUIRE(range[1] == "b");
    }

    SECTION("LPUSH multiple values") {
        std::vector<std::string> vals = {"c", "b", "a"};
        REQUIRE(db.lpush("mylist", vals) == 3);

        auto range = db.lrange("mylist", 0, -1);
        REQUIRE(range.size() == 3);
        REQUIRE(range[0] == "a");
        REQUIRE(range[1] == "b");
        REQUIRE(range[2] == "c");
    }

    SECTION("RPUSH multiple values") {
        std::vector<std::string> vals = {"a", "b", "c"};
        REQUIRE(db.rpush("mylist", vals) == 3);

        auto range = db.lrange("mylist", 0, -1);
        REQUIRE(range.size() == 3);
        REQUIRE(range[0] == "a");
        REQUIRE(range[1] == "b");
        REQUIRE(range[2] == "c");
    }

    SECTION("LPOP returns and removes from left") {
        db.rpush("mylist", {"a", "b", "c"});

        auto result = db.lpop("mylist");
        REQUIRE(result.size() == 1);
        REQUIRE(result[0] == "a");

        REQUIRE(db.llen("mylist") == 2);
    }

    SECTION("RPOP returns and removes from right") {
        db.rpush("mylist", {"a", "b", "c"});

        auto result = db.rpop("mylist");
        REQUIRE(result.size() == 1);
        REQUIRE(result[0] == "c");

        REQUIRE(db.llen("mylist") == 2);
    }

    SECTION("LPOP with count") {
        db.rpush("mylist", {"a", "b", "c", "d", "e"});

        auto result = db.lpop("mylist", 3);
        REQUIRE(result.size() == 3);
        REQUIRE(result[0] == "a");
        REQUIRE(result[1] == "b");
        REQUIRE(result[2] == "c");

        REQUIRE(db.llen("mylist") == 2);
    }

    SECTION("RPOP with count") {
        db.rpush("mylist", {"a", "b", "c", "d", "e"});

        auto result = db.rpop("mylist", 3);
        REQUIRE(result.size() == 3);
        REQUIRE(result[0] == "e");
        REQUIRE(result[1] == "d");
        REQUIRE(result[2] == "c");

        REQUIRE(db.llen("mylist") == 2);
    }

    SECTION("LPOP and RPOP on empty list") {
        auto lpop_result = db.lpop("nonexistent");
        REQUIRE(lpop_result.empty());

        auto rpop_result = db.rpop("nonexistent");
        REQUIRE(rpop_result.empty());
    }

    SECTION("LLEN returns list length") {
        REQUIRE(db.llen("mylist") == 0);

        db.rpush("mylist", "a");
        REQUIRE(db.llen("mylist") == 1);

        db.rpush("mylist", {"b", "c"});
        REQUIRE(db.llen("mylist") == 3);
    }

    SECTION("LRANGE with positive indices") {
        db.rpush("mylist", {"a", "b", "c", "d", "e"});

        auto range = db.lrange("mylist", 0, 2);
        REQUIRE(range.size() == 3);
        REQUIRE(range[0] == "a");
        REQUIRE(range[1] == "b");
        REQUIRE(range[2] == "c");

        range = db.lrange("mylist", 1, 3);
        REQUIRE(range.size() == 3);
        REQUIRE(range[0] == "b");
        REQUIRE(range[1] == "c");
        REQUIRE(range[2] == "d");
    }

    SECTION("LRANGE with negative indices") {
        db.rpush("mylist", {"a", "b", "c", "d", "e"});

        auto range = db.lrange("mylist", -3, -1);
        REQUIRE(range.size() == 3);
        REQUIRE(range[0] == "c");
        REQUIRE(range[1] == "d");
        REQUIRE(range[2] == "e");

        range = db.lrange("mylist", 0, -1);
        REQUIRE(range.size() == 5);
    }

    SECTION("LINDEX returns element at index") {
        db.rpush("mylist", {"a", "b", "c", "d", "e"});

        REQUIRE(db.lindex("mylist", 0).value() == "a");
        REQUIRE(db.lindex("mylist", 2).value() == "c");
        REQUIRE(db.lindex("mylist", -1).value() == "e");
        REQUIRE(db.lindex("mylist", -2).value() == "d");
    }

    SECTION("LINDEX out of range returns nullopt") {
        db.rpush("mylist", {"a", "b", "c"});

        REQUIRE_FALSE(db.lindex("mylist", 10).has_value());
        REQUIRE_FALSE(db.lindex("mylist", -10).has_value());
    }
}
