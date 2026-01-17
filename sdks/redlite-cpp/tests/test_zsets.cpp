#include <catch2/catch_test_macros.hpp>
#include <catch2/catch_approx.hpp>
#include <redlite/redlite.hpp>

using namespace redlite;
using Catch::Approx;

TEST_CASE("Sorted set commands", "[zsets]") {
    auto db = Database::open_memory();

    SECTION("ZADD single member") {
        REQUIRE(db.zadd("myzset", 1.0, "a") == 1);
        REQUIRE(db.zadd("myzset", 2.0, "b") == 1);
        REQUIRE(db.zadd("myzset", 1.5, "a") == 0); // Update existing
    }

    SECTION("ZADD multiple members") {
        std::vector<ZMember> members = {
            {1.0, "a"},
            {2.0, "b"},
            {3.0, "c"}
        };
        REQUIRE(db.zadd("myzset", members) == 3);
        REQUIRE(db.zcard("myzset") == 3);
    }

    SECTION("ZREM removes members") {
        db.zadd("myzset", {{1.0, "a"}, {2.0, "b"}, {3.0, "c"}});

        std::vector<std::string> to_remove = {"a", "b", "nonexistent"};
        REQUIRE(db.zrem("myzset", to_remove) == 2);
        REQUIRE(db.zcard("myzset") == 1);
    }

    SECTION("ZSCORE returns member score") {
        db.zadd("myzset", {{1.5, "a"}, {2.5, "b"}});

        auto score = db.zscore("myzset", "a");
        REQUIRE(score.has_value());
        REQUIRE(score.value() == Approx(1.5));

        score = db.zscore("myzset", "b");
        REQUIRE(score.value() == Approx(2.5));

        REQUIRE_FALSE(db.zscore("myzset", "nonexistent").has_value());
        REQUIRE_FALSE(db.zscore("nonexistent", "a").has_value());
    }

    SECTION("ZCARD returns cardinality") {
        REQUIRE(db.zcard("myzset") == 0);

        db.zadd("myzset", 1.0, "a");
        REQUIRE(db.zcard("myzset") == 1);

        db.zadd("myzset", {{2.0, "b"}, {3.0, "c"}});
        REQUIRE(db.zcard("myzset") == 3);
    }

    SECTION("ZCOUNT counts members in score range") {
        db.zadd("myzset", {{1.0, "a"}, {2.0, "b"}, {3.0, "c"}, {4.0, "d"}, {5.0, "e"}});

        REQUIRE(db.zcount("myzset", 2.0, 4.0) == 3);
        REQUIRE(db.zcount("myzset", 1.0, 5.0) == 5);
        REQUIRE(db.zcount("myzset", 10.0, 20.0) == 0);
    }

    SECTION("ZINCRBY increments score") {
        db.zadd("myzset", 10.0, "a");

        auto new_score = db.zincrby("myzset", 5.0, "a");
        REQUIRE(new_score == Approx(15.0));

        new_score = db.zincrby("myzset", -3.0, "a");
        REQUIRE(new_score == Approx(12.0));
    }

    SECTION("ZINCRBY creates member if not exists") {
        auto score = db.zincrby("myzset", 10.0, "newmember");
        REQUIRE(score == Approx(10.0));
        REQUIRE(db.zcard("myzset") == 1);
    }

    SECTION("ZRANGE returns members by index") {
        db.zadd("myzset", {{1.0, "a"}, {2.0, "b"}, {3.0, "c"}, {4.0, "d"}});

        auto range = db.zrange("myzset", 0, -1);
        REQUIRE(range.size() == 4);
        REQUIRE(range[0] == "a");
        REQUIRE(range[1] == "b");
        REQUIRE(range[2] == "c");
        REQUIRE(range[3] == "d");

        range = db.zrange("myzset", 1, 2);
        REQUIRE(range.size() == 2);
        REQUIRE(range[0] == "b");
        REQUIRE(range[1] == "c");
    }

    SECTION("ZRANGE with WITHSCORES") {
        db.zadd("myzset", {{1.0, "a"}, {2.0, "b"}, {3.0, "c"}});

        auto range = db.zrange_with_scores("myzset", 0, -1);
        REQUIRE(range.size() == 3);
        REQUIRE(range[0].member == "a");
        REQUIRE(range[0].score == Approx(1.0));
        REQUIRE(range[1].member == "b");
        REQUIRE(range[1].score == Approx(2.0));
        REQUIRE(range[2].member == "c");
        REQUIRE(range[2].score == Approx(3.0));
    }

    SECTION("ZREVRANGE returns members in reverse order") {
        db.zadd("myzset", {{1.0, "a"}, {2.0, "b"}, {3.0, "c"}, {4.0, "d"}});

        auto range = db.zrevrange("myzset", 0, -1);
        REQUIRE(range.size() == 4);
        REQUIRE(range[0] == "d");
        REQUIRE(range[1] == "c");
        REQUIRE(range[2] == "b");
        REQUIRE(range[3] == "a");

        range = db.zrevrange("myzset", 0, 1);
        REQUIRE(range.size() == 2);
        REQUIRE(range[0] == "d");
        REQUIRE(range[1] == "c");
    }

    SECTION("Sorted set ordering by score") {
        // Add in non-score order
        db.zadd("myzset", 3.0, "c");
        db.zadd("myzset", 1.0, "a");
        db.zadd("myzset", 2.0, "b");

        auto range = db.zrange("myzset", 0, -1);
        REQUIRE(range[0] == "a");
        REQUIRE(range[1] == "b");
        REQUIRE(range[2] == "c");
    }

    SECTION("Score update changes position") {
        db.zadd("myzset", {{1.0, "a"}, {2.0, "b"}, {3.0, "c"}});

        // Update 'a' to have highest score
        db.zadd("myzset", 10.0, "a");

        auto range = db.zrange("myzset", 0, -1);
        REQUIRE(range[0] == "b");
        REQUIRE(range[1] == "c");
        REQUIRE(range[2] == "a");
    }
}
