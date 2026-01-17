/**
 * Simple test runner for C++ SDK (no external dependencies)
 */

#include <iostream>
#include <string>
#include <vector>
#include <cassert>
#include <cmath>
#include <algorithm>
#include "../include/redlite/redlite.hpp"

using namespace redlite;

static int tests_passed = 0;
static int tests_failed = 0;

#define TEST(name) \
    std::cout << "  Testing: " << #name << "... "; \
    try {

#define END_TEST \
        std::cout << "OK\n"; \
        tests_passed++; \
    } catch (const std::exception& e) { \
        std::cout << "FAILED: " << e.what() << "\n"; \
        tests_failed++; \
    }

#define ASSERT(cond) if (!(cond)) throw std::runtime_error("Assertion failed: " #cond)
#define ASSERT_EQ(a, b) if ((a) != (b)) throw std::runtime_error("Assertion failed: " #a " == " #b)
#define ASSERT_NEAR(a, b, tol) if (std::abs((a) - (b)) > (tol)) throw std::runtime_error("Assertion failed: " #a " ~= " #b)

void test_strings() {
    std::cout << "\n=== String Commands ===\n";

    TEST(SET_and_GET) {
        auto db = Database::open_memory();
        ASSERT(db.set("key1", "value1"));
        auto result = db.get("key1");
        ASSERT(result.has_value());
        ASSERT_EQ(result.value(), "value1");
    } END_TEST

    TEST(GET_nonexistent) {
        auto db = Database::open_memory();
        auto result = db.get("nonexistent");
        ASSERT(!result.has_value());
    } END_TEST

    TEST(SET_with_TTL) {
        auto db = Database::open_memory();
        ASSERT(db.set("expkey", "value", 10));
        auto ttl = db.ttl("expkey");
        ASSERT(ttl > 0 && ttl <= 10);
    } END_TEST

    TEST(INCR_DECR) {
        auto db = Database::open_memory();
        db.set("counter", "10");
        ASSERT_EQ(db.incr("counter"), 11);
        ASSERT_EQ(db.incr("counter"), 12);
        ASSERT_EQ(db.decr("counter"), 11);
    } END_TEST

    TEST(INCRBYFLOAT) {
        auto db = Database::open_memory();
        db.set("floatkey", "10.5");
        auto result = db.incrbyfloat("floatkey", 2.5);
        ASSERT_NEAR(result, 13.0, 0.001);
    } END_TEST

    TEST(APPEND) {
        auto db = Database::open_memory();
        db.set("appendkey", "Hello");
        auto len = db.append("appendkey", " World");
        ASSERT_EQ(len, 11);
        ASSERT_EQ(db.get("appendkey").value(), "Hello World");
    } END_TEST

    TEST(GETRANGE) {
        auto db = Database::open_memory();
        db.set("rangekey", "Hello World");
        ASSERT_EQ(db.getrange("rangekey", 0, 4), "Hello");
        ASSERT_EQ(db.getrange("rangekey", 6, 10), "World");
    } END_TEST

    TEST(MSET_MGET) {
        auto db = Database::open_memory();
        std::unordered_map<std::string, std::string> pairs = {
            {"k1", "v1"}, {"k2", "v2"}, {"k3", "v3"}
        };
        ASSERT(db.mset(pairs));
        auto values = db.mget({"k1", "k2", "k3", "nonexistent"});
        ASSERT_EQ(values.size(), 4u);
        ASSERT(values[0].has_value());
        ASSERT(values[1].has_value());
        ASSERT(values[2].has_value());
        ASSERT(!values[3].has_value());
    } END_TEST
}

void test_keys() {
    std::cout << "\n=== Key Commands ===\n";

    TEST(DEL) {
        auto db = Database::open_memory();
        db.set("delkey", "value");
        ASSERT_EQ(db.del("delkey"), 1);
        ASSERT(!db.exists("delkey"));
    } END_TEST

    TEST(EXISTS) {
        auto db = Database::open_memory();
        db.set("exists1", "v1");
        ASSERT(db.exists("exists1"));
        ASSERT(!db.exists("nonexistent"));
    } END_TEST

    TEST(TYPE) {
        auto db = Database::open_memory();
        db.set("strkey", "value");
        ASSERT_EQ(db.type("strkey").value(), "string");
        db.lpush("listkey", "value");
        ASSERT_EQ(db.type("listkey").value(), "list");
    } END_TEST

    TEST(EXPIRE_TTL) {
        auto db = Database::open_memory();
        db.set("noexpire", "value");
        ASSERT_EQ(db.ttl("noexpire"), -1);
        ASSERT(db.expire("noexpire", 60));
        ASSERT(db.ttl("noexpire") > 0);
    } END_TEST

    TEST(KEYS) {
        auto db = Database::open_memory();
        db.set("user:1", "alice");
        db.set("user:2", "bob");
        db.set("session:1", "data");
        auto user_keys = db.keys("user:*");
        ASSERT_EQ(user_keys.size(), 2u);
    } END_TEST

    TEST(DBSIZE_FLUSHDB) {
        auto db = Database::open_memory();
        db.set("k1", "v1");
        db.set("k2", "v2");
        ASSERT_EQ(db.dbsize(), 2);
        ASSERT(db.flushdb());
        ASSERT_EQ(db.dbsize(), 0);
    } END_TEST
}

void test_hashes() {
    std::cout << "\n=== Hash Commands ===\n";

    TEST(HSET_HGET) {
        auto db = Database::open_memory();
        ASSERT_EQ(db.hset("myhash", "name", "Alice"), 1);
        auto result = db.hget("myhash", "name");
        ASSERT(result.has_value());
        ASSERT_EQ(result.value(), "Alice");
    } END_TEST

    TEST(HSET_multiple) {
        auto db = Database::open_memory();
        std::unordered_map<std::string, std::string> fields = {
            {"name", "Alice"}, {"age", "30"}
        };
        ASSERT_EQ(db.hset("myhash", fields), 2);
        ASSERT_EQ(db.hget("myhash", "name").value(), "Alice");
        ASSERT_EQ(db.hget("myhash", "age").value(), "30");
    } END_TEST

    TEST(HLEN_HKEYS_HVALS) {
        auto db = Database::open_memory();
        db.hset("myhash", {{"a", "1"}, {"b", "2"}, {"c", "3"}});
        ASSERT_EQ(db.hlen("myhash"), 3);
        ASSERT_EQ(db.hkeys("myhash").size(), 3u);
        ASSERT_EQ(db.hvals("myhash").size(), 3u);
    } END_TEST

    TEST(HGETALL) {
        auto db = Database::open_memory();
        db.hset("myhash", {{"name", "Alice"}, {"age", "30"}});
        auto all = db.hgetall("myhash");
        ASSERT_EQ(all.size(), 2u);
        ASSERT_EQ(all["name"], "Alice");
        ASSERT_EQ(all["age"], "30");
    } END_TEST
}

void test_lists() {
    std::cout << "\n=== List Commands ===\n";

    TEST(LPUSH_RPUSH) {
        auto db = Database::open_memory();
        ASSERT_EQ(db.lpush("mylist", "a"), 1);
        ASSERT_EQ(db.rpush("mylist", "b"), 2);
        auto range = db.lrange("mylist", 0, -1);
        ASSERT_EQ(range.size(), 2u);
    } END_TEST

    TEST(LPOP_RPOP) {
        auto db = Database::open_memory();
        db.rpush("mylist", {"a", "b", "c"});
        auto lpop = db.lpop("mylist");
        ASSERT_EQ(lpop.size(), 1u);
        ASSERT_EQ(lpop[0], "a");
        auto rpop = db.rpop("mylist");
        ASSERT_EQ(rpop[0], "c");
    } END_TEST

    TEST(LLEN_LRANGE) {
        auto db = Database::open_memory();
        db.rpush("mylist", {"a", "b", "c", "d", "e"});
        ASSERT_EQ(db.llen("mylist"), 5);
        auto range = db.lrange("mylist", 0, 2);
        ASSERT_EQ(range.size(), 3u);
        ASSERT_EQ(range[0], "a");
    } END_TEST

    TEST(LINDEX) {
        auto db = Database::open_memory();
        db.rpush("mylist", {"a", "b", "c"});
        ASSERT_EQ(db.lindex("mylist", 0).value(), "a");
        ASSERT_EQ(db.lindex("mylist", -1).value(), "c");
        ASSERT(!db.lindex("mylist", 10).has_value());
    } END_TEST
}

void test_sets() {
    std::cout << "\n=== Set Commands ===\n";

    TEST(SADD_SCARD) {
        auto db = Database::open_memory();
        ASSERT_EQ(db.sadd("myset", "a"), 1);
        ASSERT_EQ(db.sadd("myset", "a"), 0);  // Already exists
        ASSERT_EQ(db.sadd("myset", "b"), 1);
        ASSERT_EQ(db.scard("myset"), 2);
    } END_TEST

    TEST(SISMEMBER) {
        auto db = Database::open_memory();
        db.sadd("myset", {"a", "b", "c"});
        ASSERT(db.sismember("myset", "a"));
        ASSERT(!db.sismember("myset", "x"));
    } END_TEST

    TEST(SMEMBERS) {
        auto db = Database::open_memory();
        db.sadd("myset", {"a", "b", "c"});
        auto members = db.smembers("myset");
        ASSERT_EQ(members.size(), 3u);
    } END_TEST

    TEST(SREM) {
        auto db = Database::open_memory();
        db.sadd("myset", {"a", "b", "c"});
        ASSERT_EQ(db.srem("myset", {"a", "b", "nonexistent"}), 2);
        ASSERT_EQ(db.scard("myset"), 1);
    } END_TEST
}

void test_zsets() {
    std::cout << "\n=== Sorted Set Commands ===\n";

    TEST(ZADD_ZCARD) {
        auto db = Database::open_memory();
        ASSERT_EQ(db.zadd("myzset", 1.0, "a"), 1);
        ASSERT_EQ(db.zadd("myzset", 2.0, "b"), 1);
        ASSERT_EQ(db.zcard("myzset"), 2);
    } END_TEST

    TEST(ZSCORE) {
        auto db = Database::open_memory();
        db.zadd("myzset", {{1.5, "a"}, {2.5, "b"}});
        auto score = db.zscore("myzset", "a");
        ASSERT(score.has_value());
        ASSERT_NEAR(score.value(), 1.5, 0.001);
        ASSERT(!db.zscore("myzset", "nonexistent").has_value());
    } END_TEST

    TEST(ZRANGE) {
        auto db = Database::open_memory();
        db.zadd("myzset", {{1.0, "a"}, {2.0, "b"}, {3.0, "c"}});
        auto range = db.zrange("myzset", 0, -1);
        ASSERT_EQ(range.size(), 3u);
        ASSERT_EQ(range[0], "a");
        ASSERT_EQ(range[1], "b");
        ASSERT_EQ(range[2], "c");
    } END_TEST

    TEST(ZREVRANGE) {
        auto db = Database::open_memory();
        db.zadd("myzset", {{1.0, "a"}, {2.0, "b"}, {3.0, "c"}});
        auto range = db.zrevrange("myzset", 0, -1);
        ASSERT_EQ(range[0], "c");
        ASSERT_EQ(range[1], "b");
        ASSERT_EQ(range[2], "a");
    } END_TEST

    TEST(ZINCRBY) {
        auto db = Database::open_memory();
        db.zadd("myzset", 10.0, "a");
        auto new_score = db.zincrby("myzset", 5.0, "a");
        ASSERT_NEAR(new_score, 15.0, 0.001);
    } END_TEST

    TEST(ZCOUNT) {
        auto db = Database::open_memory();
        db.zadd("myzset", {{1.0, "a"}, {2.0, "b"}, {3.0, "c"}, {4.0, "d"}});
        ASSERT_EQ(db.zcount("myzset", 2.0, 4.0), 3);
    } END_TEST
}

int main() {
    std::cout << "Redlite C++ SDK Tests\n";
    std::cout << "=====================\n";

    test_strings();
    test_keys();
    test_hashes();
    test_lists();
    test_sets();
    test_zsets();

    std::cout << "\n=== Summary ===\n";
    std::cout << "Passed: " << tests_passed << "\n";
    std::cout << "Failed: " << tests_failed << "\n";

    return tests_failed > 0 ? 1 : 0;
}
