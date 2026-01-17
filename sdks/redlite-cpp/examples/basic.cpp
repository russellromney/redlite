#include <iostream>
#include <redlite/redlite.hpp>

using namespace redlite;

int main() {
    try {
        // Open in-memory database
        auto db = Database::open_memory();

        std::cout << "Redlite C++ SDK Example\n";
        std::cout << "=======================\n\n";

        // String operations
        std::cout << "String operations:\n";
        db.set("greeting", "Hello, World!");
        auto greeting = db.get("greeting");
        std::cout << "  GET greeting: " << greeting.value_or("(nil)") << "\n";

        db.set("counter", "0");
        std::cout << "  INCR counter: " << db.incr("counter") << "\n";
        std::cout << "  INCR counter: " << db.incr("counter") << "\n";
        std::cout << "  INCRBY counter 10: " << db.incrby("counter", 10) << "\n";

        // Hash operations
        std::cout << "\nHash operations:\n";
        db.hset("user:1", {
            {"name", "Alice"},
            {"email", "alice@example.com"},
            {"age", "30"}
        });
        std::cout << "  HGET user:1 name: " << db.hget("user:1", "name").value_or("(nil)") << "\n";
        std::cout << "  HGET user:1 email: " << db.hget("user:1", "email").value_or("(nil)") << "\n";

        auto user = db.hgetall("user:1");
        std::cout << "  HGETALL user:1:\n";
        for (const auto& [field, value] : user) {
            std::cout << "    " << field << ": " << value << "\n";
        }

        // List operations
        std::cout << "\nList operations:\n";
        db.rpush("tasks", {"task1", "task2", "task3"});
        std::cout << "  LLEN tasks: " << db.llen("tasks") << "\n";

        auto tasks = db.lrange("tasks", 0, -1);
        std::cout << "  LRANGE tasks 0 -1: ";
        for (const auto& task : tasks) {
            std::cout << task << " ";
        }
        std::cout << "\n";

        auto popped = db.lpop("tasks");
        if (!popped.empty()) {
            std::cout << "  LPOP tasks: " << popped[0] << "\n";
        }

        // Set operations
        std::cout << "\nSet operations:\n";
        db.sadd("tags", {"redis", "database", "nosql", "embedded"});
        std::cout << "  SCARD tags: " << db.scard("tags") << "\n";
        std::cout << "  SISMEMBER tags redis: " << (db.sismember("tags", "redis") ? "true" : "false") << "\n";
        std::cout << "  SISMEMBER tags mysql: " << (db.sismember("tags", "mysql") ? "true" : "false") << "\n";

        auto members = db.smembers("tags");
        std::cout << "  SMEMBERS tags: ";
        for (const auto& m : members) {
            std::cout << m << " ";
        }
        std::cout << "\n";

        // Sorted set operations
        std::cout << "\nSorted set operations:\n";
        db.zadd("leaderboard", {
            {100, "alice"},
            {150, "bob"},
            {75, "charlie"},
            {200, "diana"}
        });

        std::cout << "  ZCARD leaderboard: " << db.zcard("leaderboard") << "\n";

        auto top_players = db.zrevrange("leaderboard", 0, 2);
        std::cout << "  ZREVRANGE leaderboard 0 2 (top 3):\n";
        for (const auto& player : top_players) {
            auto score = db.zscore("leaderboard", player);
            std::cout << "    " << player << ": " << score.value_or(0) << "\n";
        }

        // Key operations
        std::cout << "\nKey operations:\n";
        std::cout << "  DBSIZE: " << db.dbsize() << "\n";

        auto all_keys = db.keys("*");
        std::cout << "  KEYS *: ";
        for (const auto& key : all_keys) {
            std::cout << key << " ";
        }
        std::cout << "\n";

        // Expiration
        std::cout << "\nExpiration:\n";
        db.setex("temp_key", 60, "temporary value");
        std::cout << "  SETEX temp_key 60: ok\n";
        std::cout << "  TTL temp_key: " << db.ttl("temp_key") << " seconds\n";

        std::cout << "\nDone!\n";

    } catch (const Error& e) {
        std::cerr << "Redlite error: " << e.what() << "\n";
        return 1;
    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << "\n";
        return 1;
    }

    return 0;
}
