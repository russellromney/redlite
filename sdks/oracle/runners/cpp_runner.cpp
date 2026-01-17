/**
 * C++ Oracle Test Runner
 * Validates C++ SDK against oracle test specifications
 *
 * Build: g++ -std=c++17 -o cpp_runner cpp_runner.cpp -lyaml-cpp -L/path/to/lib -lredlite_ffi
 * Usage: ./cpp_runner [-v] ../spec/strings.yaml
 */

#include <iostream>
#include <fstream>
#include <sstream>
#include <string>
#include <vector>
#include <unordered_map>
#include <unordered_set>
#include <optional>
#include <variant>
#include <cmath>
#include <filesystem>
#include <yaml-cpp/yaml.h>

// Include the Redlite C++ SDK
#include "../../redlite-cpp/include/redlite/redlite.hpp"

namespace fs = std::filesystem;
using namespace redlite;

// Value type to hold any result
using Value = std::variant<
    std::nullptr_t,
    bool,
    int64_t,
    double,
    std::string,
    std::vector<std::string>,
    std::vector<std::optional<std::string>>,
    std::unordered_map<std::string, std::string>
>;

struct TestResult {
    bool passed;
    std::string test_name;
    std::string error;
};

class OracleRunner {
public:
    OracleRunner(bool verbose = false) : verbose_(verbose) {}

    void runSpecFile(const std::string& path) {
        YAML::Node spec = YAML::LoadFile(path);
        std::string spec_name = spec["name"].as<std::string>();

        if (verbose_) {
            std::cout << "Running spec: " << spec_name << "\n";
        }

        for (const auto& test : spec["tests"]) {
            runTest(test, spec_name);
        }
    }

    void printSummary() {
        std::cout << "\n=== Results ===\n";
        std::cout << "Passed: " << passed_ << "\n";
        std::cout << "Failed: " << failed_ << "\n";

        if (!errors_.empty()) {
            std::cout << "\nErrors:\n";
            for (const auto& err : errors_) {
                std::cout << "  - " << err.test_name << ": " << err.error << "\n";
            }
        }
    }

    int getExitCode() const {
        return failed_ > 0 ? 1 : 0;
    }

private:
    bool verbose_;
    int passed_ = 0;
    int failed_ = 0;
    std::vector<TestResult> errors_;

    void runTest(const YAML::Node& test, const std::string& spec_name) {
        std::string test_name = test["name"].as<std::string>();
        std::string full_name = spec_name + " :: " + test_name;

        try {
            auto db = Database::open_memory();

            // Run setup operations
            if (test["setup"]) {
                for (const auto& op : test["setup"]) {
                    executeCmd(db, op);
                }
            }

            // Run test operations
            for (const auto& op : test["operations"]) {
                Value actual = executeCmd(db, op);

                if (op["expect"]) {
                    if (!compare(actual, op["expect"])) {
                        throw std::runtime_error(
                            "Expected: " + yamlToString(op["expect"]) +
                            ", Got: " + valueToString(actual));
                    }
                }
            }

            passed_++;
            if (verbose_) {
                std::cout << "  ✓ " << test_name << "\n";
            }

        } catch (const std::exception& e) {
            failed_++;
            errors_.push_back({false, full_name, e.what()});
            if (verbose_) {
                std::cout << "  ✗ " << test_name << ": " << e.what() << "\n";
            }
        }
    }

    Value executeCmd(Database& db, const YAML::Node& op) {
        std::string cmd = op["cmd"].as<std::string>();
        auto args = op["args"];

        // String commands
        if (cmd == "GET") {
            auto result = db.get(args[0].as<std::string>());
            return result ? Value(*result) : Value(nullptr);
        }
        if (cmd == "SET") {
            std::string key = args[0].as<std::string>();
            std::string value = args[1].as<std::string>();
            int64_t ttl = 0;
            if (op["kwargs"] && op["kwargs"]["ex"]) {
                ttl = op["kwargs"]["ex"].as<int64_t>();
            }
            return Value(db.set(key, value, ttl));
        }
        if (cmd == "SETEX") {
            return Value(db.setex(args[0].as<std::string>(),
                                  args[1].as<int64_t>(),
                                  args[2].as<std::string>()));
        }
        if (cmd == "PSETEX") {
            return Value(db.psetex(args[0].as<std::string>(),
                                   args[1].as<int64_t>(),
                                   args[2].as<std::string>()));
        }
        if (cmd == "GETDEL") {
            auto result = db.getdel(args[0].as<std::string>());
            return result ? Value(*result) : Value(nullptr);
        }
        if (cmd == "APPEND") {
            return Value(db.append(args[0].as<std::string>(),
                                   args[1].as<std::string>()));
        }
        if (cmd == "STRLEN") {
            return Value(db.strlen(args[0].as<std::string>()));
        }
        if (cmd == "GETRANGE") {
            return Value(db.getrange(args[0].as<std::string>(),
                                     args[1].as<int64_t>(),
                                     args[2].as<int64_t>()));
        }
        if (cmd == "SETRANGE") {
            return Value(db.setrange(args[0].as<std::string>(),
                                     args[1].as<int64_t>(),
                                     args[2].as<std::string>()));
        }
        if (cmd == "INCR") {
            return Value(db.incr(args[0].as<std::string>()));
        }
        if (cmd == "DECR") {
            return Value(db.decr(args[0].as<std::string>()));
        }
        if (cmd == "INCRBY") {
            return Value(db.incrby(args[0].as<std::string>(),
                                   args[1].as<int64_t>()));
        }
        if (cmd == "DECRBY") {
            return Value(db.decrby(args[0].as<std::string>(),
                                   args[1].as<int64_t>()));
        }
        if (cmd == "INCRBYFLOAT") {
            return Value(db.incrbyfloat(args[0].as<std::string>(),
                                        args[1].as<double>()));
        }
        if (cmd == "MGET") {
            std::vector<std::string> keys;
            for (const auto& k : args) {
                keys.push_back(k.as<std::string>());
            }
            return Value(db.mget(keys));
        }
        if (cmd == "MSET") {
            std::unordered_map<std::string, std::string> pairs;
            for (size_t i = 0; i + 1 < args.size(); i += 2) {
                pairs[args[i].as<std::string>()] = args[i + 1].as<std::string>();
            }
            return Value(db.mset(pairs));
        }

        // Key commands
        if (cmd == "DEL") {
            if (args.size() == 1 && args[0].IsScalar()) {
                return Value(db.del(args[0].as<std::string>()));
            }
            std::vector<std::string> keys;
            for (const auto& k : args) {
                keys.push_back(k.as<std::string>());
            }
            return Value(db.del(keys));
        }
        if (cmd == "EXISTS") {
            if (args.size() == 1) {
                return Value(db.exists(args[0].as<std::string>()) ? (int64_t)1 : (int64_t)0);
            }
            std::vector<std::string> keys;
            for (const auto& k : args) {
                keys.push_back(k.as<std::string>());
            }
            return Value(db.exists(keys));
        }
        if (cmd == "TYPE") {
            auto result = db.type(args[0].as<std::string>());
            return result ? Value(*result) : Value(std::string("none"));
        }
        if (cmd == "TTL") {
            return Value(db.ttl(args[0].as<std::string>()));
        }
        if (cmd == "PTTL") {
            return Value(db.pttl(args[0].as<std::string>()));
        }
        if (cmd == "EXPIRE") {
            return Value(db.expire(args[0].as<std::string>(),
                                   args[1].as<int64_t>()) ? (int64_t)1 : (int64_t)0);
        }
        if (cmd == "PEXPIRE") {
            return Value(db.pexpire(args[0].as<std::string>(),
                                    args[1].as<int64_t>()) ? (int64_t)1 : (int64_t)0);
        }
        if (cmd == "PERSIST") {
            return Value(db.persist(args[0].as<std::string>()) ? (int64_t)1 : (int64_t)0);
        }
        if (cmd == "RENAME") {
            return Value(db.rename(args[0].as<std::string>(),
                                   args[1].as<std::string>()));
        }
        if (cmd == "RENAMENX") {
            return Value(db.renamenx(args[0].as<std::string>(),
                                     args[1].as<std::string>()) ? (int64_t)1 : (int64_t)0);
        }
        if (cmd == "KEYS") {
            std::string pattern = args.size() > 0 ? args[0].as<std::string>() : "*";
            return Value(db.keys(pattern));
        }
        if (cmd == "DBSIZE") {
            return Value(db.dbsize());
        }
        if (cmd == "FLUSHDB") {
            return Value(db.flushdb());
        }

        // Hash commands
        if (cmd == "HSET") {
            std::string key = args[0].as<std::string>();
            if (args.size() == 3) {
                return Value(db.hset(key, args[1].as<std::string>(),
                                     args[2].as<std::string>()));
            }
            std::unordered_map<std::string, std::string> fields;
            for (size_t i = 1; i + 1 < args.size(); i += 2) {
                fields[args[i].as<std::string>()] = args[i + 1].as<std::string>();
            }
            return Value(db.hset(key, fields));
        }
        if (cmd == "HGET") {
            auto result = db.hget(args[0].as<std::string>(),
                                  args[1].as<std::string>());
            return result ? Value(*result) : Value(nullptr);
        }
        if (cmd == "HDEL") {
            std::vector<std::string> fields;
            for (size_t i = 1; i < args.size(); ++i) {
                fields.push_back(args[i].as<std::string>());
            }
            return Value(db.hdel(args[0].as<std::string>(), fields));
        }
        if (cmd == "HEXISTS") {
            return Value(db.hexists(args[0].as<std::string>(),
                                    args[1].as<std::string>()) ? (int64_t)1 : (int64_t)0);
        }
        if (cmd == "HLEN") {
            return Value(db.hlen(args[0].as<std::string>()));
        }
        if (cmd == "HKEYS") {
            return Value(db.hkeys(args[0].as<std::string>()));
        }
        if (cmd == "HVALS") {
            return Value(db.hvals(args[0].as<std::string>()));
        }
        if (cmd == "HINCRBY") {
            return Value(db.hincrby(args[0].as<std::string>(),
                                    args[1].as<std::string>(),
                                    args[2].as<int64_t>()));
        }
        if (cmd == "HGETALL") {
            return Value(db.hgetall(args[0].as<std::string>()));
        }
        if (cmd == "HMGET") {
            std::vector<std::string> fields;
            for (size_t i = 1; i < args.size(); ++i) {
                fields.push_back(args[i].as<std::string>());
            }
            return Value(db.hmget(args[0].as<std::string>(), fields));
        }

        // List commands
        if (cmd == "LPUSH") {
            std::vector<std::string> values;
            for (size_t i = 1; i < args.size(); ++i) {
                values.push_back(args[i].as<std::string>());
            }
            return Value(db.lpush(args[0].as<std::string>(), values));
        }
        if (cmd == "RPUSH") {
            std::vector<std::string> values;
            for (size_t i = 1; i < args.size(); ++i) {
                values.push_back(args[i].as<std::string>());
            }
            return Value(db.rpush(args[0].as<std::string>(), values));
        }
        if (cmd == "LPOP") {
            auto result = db.lpop(args[0].as<std::string>());
            if (result.empty()) return Value(nullptr);
            return Value(result[0]);
        }
        if (cmd == "RPOP") {
            auto result = db.rpop(args[0].as<std::string>());
            if (result.empty()) return Value(nullptr);
            return Value(result[0]);
        }
        if (cmd == "LLEN") {
            return Value(db.llen(args[0].as<std::string>()));
        }
        if (cmd == "LRANGE") {
            return Value(db.lrange(args[0].as<std::string>(),
                                   args[1].as<int64_t>(),
                                   args[2].as<int64_t>()));
        }
        if (cmd == "LINDEX") {
            auto result = db.lindex(args[0].as<std::string>(),
                                    args[1].as<int64_t>());
            return result ? Value(*result) : Value(nullptr);
        }

        // Set commands
        if (cmd == "SADD") {
            std::vector<std::string> members;
            for (size_t i = 1; i < args.size(); ++i) {
                members.push_back(args[i].as<std::string>());
            }
            return Value(db.sadd(args[0].as<std::string>(), members));
        }
        if (cmd == "SREM") {
            std::vector<std::string> members;
            for (size_t i = 1; i < args.size(); ++i) {
                members.push_back(args[i].as<std::string>());
            }
            return Value(db.srem(args[0].as<std::string>(), members));
        }
        if (cmd == "SMEMBERS") {
            return Value(db.smembers(args[0].as<std::string>()));
        }
        if (cmd == "SISMEMBER") {
            return Value(db.sismember(args[0].as<std::string>(),
                                      args[1].as<std::string>()) ? (int64_t)1 : (int64_t)0);
        }
        if (cmd == "SCARD") {
            return Value(db.scard(args[0].as<std::string>()));
        }

        // Sorted set commands
        if (cmd == "ZADD") {
            std::string key = args[0].as<std::string>();
            std::vector<ZMember> members;
            for (size_t i = 1; i + 1 < args.size(); i += 2) {
                members.emplace_back(args[i].as<double>(), args[i + 1].as<std::string>());
            }
            return Value(db.zadd(key, members));
        }
        if (cmd == "ZREM") {
            std::vector<std::string> members;
            for (size_t i = 1; i < args.size(); ++i) {
                members.push_back(args[i].as<std::string>());
            }
            return Value(db.zrem(args[0].as<std::string>(), members));
        }
        if (cmd == "ZSCORE") {
            auto result = db.zscore(args[0].as<std::string>(),
                                    args[1].as<std::string>());
            return result ? Value(*result) : Value(nullptr);
        }
        if (cmd == "ZCARD") {
            return Value(db.zcard(args[0].as<std::string>()));
        }
        if (cmd == "ZCOUNT") {
            return Value(db.zcount(args[0].as<std::string>(),
                                   args[1].as<double>(),
                                   args[2].as<double>()));
        }
        if (cmd == "ZINCRBY") {
            return Value(db.zincrby(args[0].as<std::string>(),
                                    args[1].as<double>(),
                                    args[2].as<std::string>()));
        }
        if (cmd == "ZRANGE") {
            return Value(db.zrange(args[0].as<std::string>(),
                                   args[1].as<int64_t>(),
                                   args[2].as<int64_t>()));
        }
        if (cmd == "ZREVRANGE") {
            return Value(db.zrevrange(args[0].as<std::string>(),
                                      args[1].as<int64_t>(),
                                      args[2].as<int64_t>()));
        }

        throw std::runtime_error("Unknown command: " + cmd);
    }

    bool compare(const Value& actual, const YAML::Node& expected) {
        // Handle special expectations like {range: [a, b]}, {set: [...]}, etc.
        if (expected.IsMap()) {
            return compareSpecial(actual, expected);
        }

        // Null
        if (expected.IsNull()) {
            return std::holds_alternative<std::nullptr_t>(actual);
        }

        // Boolean
        if (expected.IsScalar()) {
            std::string s = expected.as<std::string>();
            if (s == "true") {
                if (std::holds_alternative<bool>(actual)) {
                    return std::get<bool>(actual) == true;
                }
                if (std::holds_alternative<int64_t>(actual)) {
                    return std::get<int64_t>(actual) != 0;
                }
            }
            if (s == "false") {
                if (std::holds_alternative<bool>(actual)) {
                    return std::get<bool>(actual) == false;
                }
                if (std::holds_alternative<int64_t>(actual)) {
                    return std::get<int64_t>(actual) == 0;
                }
            }

            // Try integer
            try {
                int64_t expected_int = expected.as<int64_t>();
                if (std::holds_alternative<int64_t>(actual)) {
                    return std::get<int64_t>(actual) == expected_int;
                }
            } catch (...) {}

            // Try float
            try {
                double expected_double = expected.as<double>();
                if (std::holds_alternative<double>(actual)) {
                    return std::abs(std::get<double>(actual) - expected_double) < 0.001;
                }
            } catch (...) {}

            // String
            if (std::holds_alternative<std::string>(actual)) {
                return std::get<std::string>(actual) == expected.as<std::string>();
            }
        }

        // Sequence (list)
        if (expected.IsSequence()) {
            if (std::holds_alternative<std::vector<std::string>>(actual)) {
                const auto& actual_vec = std::get<std::vector<std::string>>(actual);
                if (actual_vec.size() != expected.size()) return false;
                for (size_t i = 0; i < expected.size(); ++i) {
                    if (expected[i].IsNull()) continue;
                    if (actual_vec[i] != expected[i].as<std::string>()) return false;
                }
                return true;
            }
            if (std::holds_alternative<std::vector<std::optional<std::string>>>(actual)) {
                const auto& actual_vec = std::get<std::vector<std::optional<std::string>>>(actual);
                if (actual_vec.size() != expected.size()) return false;
                for (size_t i = 0; i < expected.size(); ++i) {
                    if (expected[i].IsNull()) {
                        if (actual_vec[i].has_value()) return false;
                    } else {
                        if (!actual_vec[i].has_value()) return false;
                        if (*actual_vec[i] != expected[i].as<std::string>()) return false;
                    }
                }
                return true;
            }
        }

        return false;
    }

    bool compareSpecial(const Value& actual, const YAML::Node& expected) {
        // {set: ["a", "b"]} - unordered set comparison
        if (expected["set"]) {
            if (!std::holds_alternative<std::vector<std::string>>(actual)) return false;
            const auto& actual_vec = std::get<std::vector<std::string>>(actual);
            std::unordered_set<std::string> actual_set(actual_vec.begin(), actual_vec.end());
            std::unordered_set<std::string> expected_set;
            for (const auto& item : expected["set"]) {
                expected_set.insert(item.as<std::string>());
            }
            return actual_set == expected_set;
        }

        // {dict: {"k": "v"}} - dictionary comparison
        if (expected["dict"]) {
            if (!std::holds_alternative<std::unordered_map<std::string, std::string>>(actual)) return false;
            const auto& actual_map = std::get<std::unordered_map<std::string, std::string>>(actual);
            for (const auto& pair : expected["dict"]) {
                auto it = actual_map.find(pair.first.as<std::string>());
                if (it == actual_map.end()) return false;
                if (it->second != pair.second.as<std::string>()) return false;
            }
            return actual_map.size() == expected["dict"].size();
        }

        // {range: [min, max]} - numeric range
        if (expected["range"]) {
            int64_t min_val = expected["range"][0].as<int64_t>();
            int64_t max_val = expected["range"][1].as<int64_t>();
            if (std::holds_alternative<int64_t>(actual)) {
                int64_t val = std::get<int64_t>(actual);
                return val >= min_val && val <= max_val;
            }
        }

        // {approx: val, tol: tolerance} - float with tolerance
        if (expected["approx"]) {
            double expected_val = expected["approx"].as<double>();
            double tol = expected["tol"] ? expected["tol"].as<double>() : 0.001;
            if (std::holds_alternative<double>(actual)) {
                return std::abs(std::get<double>(actual) - expected_val) <= tol;
            }
        }

        // {type: "typename"} - type check only
        if (expected["type"]) {
            std::string type = expected["type"].as<std::string>();
            if (type == "string") return std::holds_alternative<std::string>(actual);
            if (type == "int") return std::holds_alternative<int64_t>(actual);
            if (type == "float") return std::holds_alternative<double>(actual);
            if (type == "list") return std::holds_alternative<std::vector<std::string>>(actual);
            if (type == "map") return std::holds_alternative<std::unordered_map<std::string, std::string>>(actual);
        }

        return false;
    }

    std::string yamlToString(const YAML::Node& node) {
        if (node.IsNull()) return "null";
        if (node.IsScalar()) return node.as<std::string>();
        if (node.IsSequence()) {
            std::ostringstream oss;
            oss << "[";
            for (size_t i = 0; i < node.size(); ++i) {
                if (i > 0) oss << ", ";
                oss << yamlToString(node[i]);
            }
            oss << "]";
            return oss.str();
        }
        if (node.IsMap()) {
            std::ostringstream oss;
            oss << "{...}";
            return oss.str();
        }
        return "???";
    }

    std::string valueToString(const Value& v) {
        return std::visit([](auto&& arg) -> std::string {
            using T = std::decay_t<decltype(arg)>;
            if constexpr (std::is_same_v<T, std::nullptr_t>) {
                return "null";
            } else if constexpr (std::is_same_v<T, bool>) {
                return arg ? "true" : "false";
            } else if constexpr (std::is_same_v<T, int64_t>) {
                return std::to_string(arg);
            } else if constexpr (std::is_same_v<T, double>) {
                return std::to_string(arg);
            } else if constexpr (std::is_same_v<T, std::string>) {
                return "\"" + arg + "\"";
            } else if constexpr (std::is_same_v<T, std::vector<std::string>>) {
                std::ostringstream oss;
                oss << "[";
                for (size_t i = 0; i < arg.size(); ++i) {
                    if (i > 0) oss << ", ";
                    oss << "\"" << arg[i] << "\"";
                }
                oss << "]";
                return oss.str();
            } else if constexpr (std::is_same_v<T, std::vector<std::optional<std::string>>>) {
                std::ostringstream oss;
                oss << "[";
                for (size_t i = 0; i < arg.size(); ++i) {
                    if (i > 0) oss << ", ";
                    if (arg[i]) oss << "\"" << *arg[i] << "\"";
                    else oss << "null";
                }
                oss << "]";
                return oss.str();
            } else {
                return "(complex)";
            }
        }, v);
    }
};

int main(int argc, char* argv[]) {
    bool verbose = false;
    std::vector<std::string> spec_files;

    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        if (arg == "-v" || arg == "--verbose") {
            verbose = true;
        } else {
            spec_files.push_back(arg);
        }
    }

    if (spec_files.empty()) {
        std::cerr << "Usage: " << argv[0] << " [-v] <spec.yaml> [spec2.yaml ...]\n";
        std::cerr << "       " << argv[0] << " [-v] ../spec/    (run all specs in directory)\n";
        return 1;
    }

    OracleRunner runner(verbose);

    for (const auto& path : spec_files) {
        if (fs::is_directory(path)) {
            for (const auto& entry : fs::directory_iterator(path)) {
                if (entry.path().extension() == ".yaml") {
                    runner.runSpecFile(entry.path().string());
                }
            }
        } else {
            runner.runSpecFile(path);
        }
    }

    runner.printSummary();
    return runner.getExitCode();
}
