/**
 * Redlite C++ SDK
 * Modern C++17 wrapper around the Redlite C FFI
 *
 * RAII-based resource management with idiomatic C++ API.
 */

#ifndef REDLITE_HPP
#define REDLITE_HPP

#include <cstdint>
#include <cstddef>
#include <string>
#include <string_view>
#include <vector>
#include <optional>
#include <stdexcept>
#include <memory>
#include <unordered_map>
#include <cmath>

// Forward declare the C types
extern "C" {
    struct RedliteDb;
    struct RedliteBytes {
        uint8_t* data;
        size_t len;
    };
    struct RedliteStringArray {
        char** strings;
        size_t len;
    };
    struct RedliteBytesArray {
        RedliteBytes* items;
        size_t len;
    };
    struct RedliteKV {
        const char* key;
        const uint8_t* value;
        size_t value_len;
    };
    struct RedliteZMember {
        double score;
        const uint8_t* member;
        size_t member_len;
    };

    // FFI function declarations
    RedliteDb* redlite_open(const char* path);
    RedliteDb* redlite_open_memory();
    RedliteDb* redlite_open_with_cache(const char* path, int64_t cache_mb);
    void redlite_close(RedliteDb* db);
    char* redlite_last_error();
    void redlite_free_string(char* s);
    void redlite_free_bytes(RedliteBytes bytes);
    void redlite_free_string_array(RedliteStringArray arr);
    void redlite_free_bytes_array(RedliteBytesArray arr);

    // String commands
    RedliteBytes redlite_get(RedliteDb* db, const char* key);
    int redlite_set(RedliteDb* db, const char* key, const uint8_t* value, size_t value_len, int64_t ttl_seconds);
    int redlite_setex(RedliteDb* db, const char* key, int64_t seconds, const uint8_t* value, size_t value_len);
    int redlite_psetex(RedliteDb* db, const char* key, int64_t milliseconds, const uint8_t* value, size_t value_len);
    RedliteBytes redlite_getdel(RedliteDb* db, const char* key);
    int64_t redlite_append(RedliteDb* db, const char* key, const uint8_t* value, size_t value_len);
    int64_t redlite_strlen(RedliteDb* db, const char* key);
    RedliteBytes redlite_getrange(RedliteDb* db, const char* key, int64_t start, int64_t end);
    int64_t redlite_setrange(RedliteDb* db, const char* key, int64_t offset, const uint8_t* value, size_t value_len);
    int64_t redlite_incr(RedliteDb* db, const char* key);
    int64_t redlite_decr(RedliteDb* db, const char* key);
    int64_t redlite_incrby(RedliteDb* db, const char* key, int64_t increment);
    int64_t redlite_decrby(RedliteDb* db, const char* key, int64_t decrement);
    char* redlite_incrbyfloat(RedliteDb* db, const char* key, double increment);
    RedliteBytesArray redlite_mget(RedliteDb* db, const char* const* keys, size_t keys_len);
    int redlite_mset(RedliteDb* db, const RedliteKV* pairs, size_t pairs_len);

    // Key commands
    int64_t redlite_del(RedliteDb* db, const char* const* keys, size_t keys_len);
    int64_t redlite_exists(RedliteDb* db, const char* const* keys, size_t keys_len);
    char* redlite_type(RedliteDb* db, const char* key);
    int64_t redlite_ttl(RedliteDb* db, const char* key);
    int64_t redlite_pttl(RedliteDb* db, const char* key);
    int redlite_expire(RedliteDb* db, const char* key, int64_t seconds);
    int redlite_pexpire(RedliteDb* db, const char* key, int64_t milliseconds);
    int redlite_expireat(RedliteDb* db, const char* key, int64_t unix_seconds);
    int redlite_pexpireat(RedliteDb* db, const char* key, int64_t unix_ms);
    int redlite_persist(RedliteDb* db, const char* key);
    int redlite_rename(RedliteDb* db, const char* key, const char* newkey);
    int redlite_renamenx(RedliteDb* db, const char* key, const char* newkey);
    RedliteStringArray redlite_keys(RedliteDb* db, const char* pattern);
    int64_t redlite_dbsize(RedliteDb* db);
    int redlite_flushdb(RedliteDb* db);
    int redlite_select(RedliteDb* db, int db_num);

    // Hash commands
    int64_t redlite_hset(RedliteDb* db, const char* key, const char* const* fields, const RedliteBytes* values, size_t count);
    RedliteBytes redlite_hget(RedliteDb* db, const char* key, const char* field);
    int64_t redlite_hdel(RedliteDb* db, const char* key, const char* const* fields, size_t fields_len);
    int redlite_hexists(RedliteDb* db, const char* key, const char* field);
    int64_t redlite_hlen(RedliteDb* db, const char* key);
    RedliteStringArray redlite_hkeys(RedliteDb* db, const char* key);
    RedliteBytesArray redlite_hvals(RedliteDb* db, const char* key);
    int64_t redlite_hincrby(RedliteDb* db, const char* key, const char* field, int64_t increment);
    RedliteBytesArray redlite_hgetall(RedliteDb* db, const char* key);
    RedliteBytesArray redlite_hmget(RedliteDb* db, const char* key, const char* const* fields, size_t fields_len);

    // List commands
    int64_t redlite_lpush(RedliteDb* db, const char* key, const RedliteBytes* values, size_t values_len);
    int64_t redlite_rpush(RedliteDb* db, const char* key, const RedliteBytes* values, size_t values_len);
    RedliteBytesArray redlite_lpop(RedliteDb* db, const char* key, size_t count);
    RedliteBytesArray redlite_rpop(RedliteDb* db, const char* key, size_t count);
    int64_t redlite_llen(RedliteDb* db, const char* key);
    RedliteBytesArray redlite_lrange(RedliteDb* db, const char* key, int64_t start, int64_t stop);
    RedliteBytes redlite_lindex(RedliteDb* db, const char* key, int64_t index);

    // Set commands
    int64_t redlite_sadd(RedliteDb* db, const char* key, const RedliteBytes* members, size_t members_len);
    int64_t redlite_srem(RedliteDb* db, const char* key, const RedliteBytes* members, size_t members_len);
    RedliteBytesArray redlite_smembers(RedliteDb* db, const char* key);
    int redlite_sismember(RedliteDb* db, const char* key, const uint8_t* member, size_t member_len);
    int64_t redlite_scard(RedliteDb* db, const char* key);

    // Sorted set commands
    int64_t redlite_zadd(RedliteDb* db, const char* key, const RedliteZMember* members, size_t members_len);
    int64_t redlite_zrem(RedliteDb* db, const char* key, const RedliteBytes* members, size_t members_len);
    double redlite_zscore(RedliteDb* db, const char* key, const uint8_t* member, size_t member_len);
    int64_t redlite_zcard(RedliteDb* db, const char* key);
    int64_t redlite_zcount(RedliteDb* db, const char* key, double min, double max);
    double redlite_zincrby(RedliteDb* db, const char* key, double increment, const uint8_t* member, size_t member_len);
    RedliteBytesArray redlite_zrange(RedliteDb* db, const char* key, int64_t start, int64_t stop, int with_scores);
    RedliteBytesArray redlite_zrevrange(RedliteDb* db, const char* key, int64_t start, int64_t stop, int with_scores);

    // Server commands
    int64_t redlite_vacuum(RedliteDb* db);
    char* redlite_version();

    // JSON commands (ReJSON-compatible)
    int redlite_json_set(RedliteDb* db, const char* key, const char* path, const char* value, int nx, int xx);
    char* redlite_json_get(RedliteDb* db, const char* key, const char** paths, size_t paths_len);
    int64_t redlite_json_del(RedliteDb* db, const char* key, const char* path);
    char* redlite_json_type(RedliteDb* db, const char* key, const char* path);
    char* redlite_json_numincrby(RedliteDb* db, const char* key, const char* path, double increment);
    int64_t redlite_json_strappend(RedliteDb* db, const char* key, const char* path, const char* value);
    int64_t redlite_json_strlen(RedliteDb* db, const char* key, const char* path);
    int64_t redlite_json_arrappend(RedliteDb* db, const char* key, const char* path, const char** values, size_t values_len);
    int64_t redlite_json_arrlen(RedliteDb* db, const char* key, const char* path);
    char* redlite_json_arrpop(RedliteDb* db, const char* key, const char* path, int64_t index);
    int64_t redlite_json_clear(RedliteDb* db, const char* key, const char* path);

    // History enable/disable commands
    int redlite_history_enable_global(RedliteDb* db, const char* retention_type, int64_t retention_value);
    int redlite_history_enable_database(RedliteDb* db, int db_num, const char* retention_type, int64_t retention_value);
    int redlite_history_enable_key(RedliteDb* db, const char* key, const char* retention_type, int64_t retention_value);
    int redlite_history_disable_global(RedliteDb* db);
    int redlite_history_disable_database(RedliteDb* db, int db_num);
    int redlite_history_disable_key(RedliteDb* db, const char* key);
    int redlite_is_history_enabled(RedliteDb* db, const char* key);

    // FTS enable/disable commands
    int redlite_fts_enable_global(RedliteDb* db);
    int redlite_fts_enable_database(RedliteDb* db, int db_num);
    int redlite_fts_enable_pattern(RedliteDb* db, const char* pattern);
    int redlite_fts_enable_key(RedliteDb* db, const char* key);
    int redlite_fts_disable_global(RedliteDb* db);
    int redlite_fts_disable_database(RedliteDb* db, int db_num);
    int redlite_fts_disable_pattern(RedliteDb* db, const char* pattern);
    int redlite_fts_disable_key(RedliteDb* db, const char* key);
    int redlite_is_fts_enabled(RedliteDb* db, const char* key);

    // KeyInfo command
    struct RedliteKeyInfo {
        char* key_type;
        int64_t ttl;
        int64_t created_at;
        int64_t updated_at;
        int valid;
    };
    RedliteKeyInfo redlite_keyinfo(RedliteDb* db, const char* key);
    void redlite_free_keyinfo(RedliteKeyInfo info);
}

namespace redlite {

/**
 * Exception thrown for Redlite errors
 */
class Error : public std::runtime_error {
public:
    explicit Error(const std::string& msg) : std::runtime_error(msg) {}

    static Error from_last_error() {
        char* err = redlite_last_error();
        if (err) {
            std::string msg(err);
            redlite_free_string(err);
            return Error(msg);
        }
        return Error("Unknown error");
    }
};

/**
 * SET command options
 */
struct SetOptions {
    std::optional<int64_t> ex;   // Expire in seconds
    std::optional<int64_t> px;   // Expire in milliseconds
    bool nx = false;              // Only set if not exists
    bool xx = false;              // Only set if exists
};

/**
 * Builder for SET command options
 */
class SetOptionsBuilder {
public:
    SetOptionsBuilder& ex(int64_t seconds) { opts_.ex = seconds; return *this; }
    SetOptionsBuilder& px(int64_t milliseconds) { opts_.px = milliseconds; return *this; }
    SetOptionsBuilder& nx() { opts_.nx = true; return *this; }
    SetOptionsBuilder& xx() { opts_.xx = true; return *this; }
    SetOptions build() const { return opts_; }
private:
    SetOptions opts_;
};

/**
 * Sorted set member
 */
struct ZMember {
    double score;
    std::string member;

    ZMember(double s, std::string_view m) : score(s), member(m) {}
};

/**
 * Key information returned by keyinfo()
 */
struct KeyInfo {
    std::string type;
    int64_t ttl;
    int64_t created_at;
    int64_t updated_at;
};

/**
 * JSON SET options
 */
struct JsonSetOptions {
    bool nx = false;  // Only set if not exists
    bool xx = false;  // Only set if exists
};

/**
 * RAII wrapper for bytes result - auto-frees on destruction
 */
class Bytes {
public:
    Bytes() : data_{nullptr, 0} {}
    explicit Bytes(RedliteBytes b) : data_(b) {}
    ~Bytes() { if (data_.data) redlite_free_bytes(data_); }

    // Move only
    Bytes(Bytes&& other) noexcept : data_(other.data_) { other.data_ = {nullptr, 0}; }
    Bytes& operator=(Bytes&& other) noexcept {
        if (this != &other) {
            if (data_.data) redlite_free_bytes(data_);
            data_ = other.data_;
            other.data_ = {nullptr, 0};
        }
        return *this;
    }
    Bytes(const Bytes&) = delete;
    Bytes& operator=(const Bytes&) = delete;

    bool empty() const { return data_.data == nullptr || data_.len == 0; }
    explicit operator bool() const { return !empty(); }

    const uint8_t* data() const { return data_.data; }
    size_t size() const { return data_.len; }

    std::string to_string() const {
        if (empty()) return {};
        return std::string(reinterpret_cast<const char*>(data_.data), data_.len);
    }

    std::vector<uint8_t> to_vector() const {
        if (empty()) return {};
        return std::vector<uint8_t>(data_.data, data_.data + data_.len);
    }

private:
    RedliteBytes data_;
};

/**
 * Main database class - RAII managed
 */
class Database {
public:
    /**
     * Open a database at the given path
     * @throws Error if open fails
     */
    explicit Database(const std::string& path) {
        db_ = redlite_open(path.c_str());
        if (!db_) throw Error::from_last_error();
    }

    /**
     * Open an in-memory database
     */
    static Database open_memory() {
        RedliteDb* db = redlite_open_memory();
        if (!db) throw Error::from_last_error();
        return Database(db);
    }

    /**
     * Open a database with custom cache size
     */
    static Database open_with_cache(const std::string& path, int64_t cache_mb) {
        RedliteDb* db = redlite_open_with_cache(path.c_str(), cache_mb);
        if (!db) throw Error::from_last_error();
        return Database(db);
    }

    ~Database() {
        if (db_) redlite_close(db_);
    }

    // Move only
    Database(Database&& other) noexcept : db_(other.db_) { other.db_ = nullptr; }
    Database& operator=(Database&& other) noexcept {
        if (this != &other) {
            if (db_) redlite_close(db_);
            db_ = other.db_;
            other.db_ = nullptr;
        }
        return *this;
    }
    Database(const Database&) = delete;
    Database& operator=(const Database&) = delete;

    // ==================== String Commands ====================

    /**
     * GET key
     * @return Value or empty optional if key doesn't exist
     */
    std::optional<std::string> get(std::string_view key) {
        RedliteBytes result = redlite_get(db_, std::string(key).c_str());
        if (!result.data) return std::nullopt;
        Bytes b(result);
        return b.to_string();
    }

    /**
     * GET key (raw bytes)
     */
    std::optional<std::vector<uint8_t>> get_bytes(std::string_view key) {
        RedliteBytes result = redlite_get(db_, std::string(key).c_str());
        if (!result.data) return std::nullopt;
        Bytes b(result);
        return b.to_vector();
    }

    /**
     * SET key value [TTL seconds]
     * @return true on success
     */
    bool set(std::string_view key, std::string_view value, int64_t ttl_seconds = 0) {
        return redlite_set(db_, std::string(key).c_str(),
                          reinterpret_cast<const uint8_t*>(value.data()),
                          value.size(), ttl_seconds) == 0;
    }

    /**
     * SET key value with options
     */
    bool set(std::string_view key, std::string_view value, const SetOptions& opts) {
        // Handle NX/XX options via the simpler set
        // Note: Full NX/XX support would need FFI extension
        int64_t ttl = 0;
        if (opts.ex) ttl = *opts.ex;
        else if (opts.px) ttl = *opts.px / 1000; // Convert ms to seconds
        return set(key, value, ttl);
    }

    /**
     * SETEX key seconds value
     */
    bool setex(std::string_view key, int64_t seconds, std::string_view value) {
        return redlite_setex(db_, std::string(key).c_str(), seconds,
                            reinterpret_cast<const uint8_t*>(value.data()),
                            value.size()) == 0;
    }

    /**
     * PSETEX key milliseconds value
     */
    bool psetex(std::string_view key, int64_t milliseconds, std::string_view value) {
        return redlite_psetex(db_, std::string(key).c_str(), milliseconds,
                             reinterpret_cast<const uint8_t*>(value.data()),
                             value.size()) == 0;
    }

    /**
     * GETDEL key - Get and delete
     */
    std::optional<std::string> getdel(std::string_view key) {
        RedliteBytes result = redlite_getdel(db_, std::string(key).c_str());
        if (!result.data) return std::nullopt;
        Bytes b(result);
        return b.to_string();
    }

    /**
     * APPEND key value
     * @return New length of string
     */
    int64_t append(std::string_view key, std::string_view value) {
        return redlite_append(db_, std::string(key).c_str(),
                             reinterpret_cast<const uint8_t*>(value.data()),
                             value.size());
    }

    /**
     * STRLEN key
     */
    int64_t strlen(std::string_view key) {
        return redlite_strlen(db_, std::string(key).c_str());
    }

    /**
     * GETRANGE key start end
     */
    std::string getrange(std::string_view key, int64_t start, int64_t end) {
        RedliteBytes result = redlite_getrange(db_, std::string(key).c_str(), start, end);
        if (!result.data) return {};
        Bytes b(result);
        return b.to_string();
    }

    /**
     * SETRANGE key offset value
     * @return New length of string
     */
    int64_t setrange(std::string_view key, int64_t offset, std::string_view value) {
        return redlite_setrange(db_, std::string(key).c_str(), offset,
                               reinterpret_cast<const uint8_t*>(value.data()),
                               value.size());
    }

    /**
     * INCR key
     */
    int64_t incr(std::string_view key) {
        return redlite_incr(db_, std::string(key).c_str());
    }

    /**
     * DECR key
     */
    int64_t decr(std::string_view key) {
        return redlite_decr(db_, std::string(key).c_str());
    }

    /**
     * INCRBY key increment
     */
    int64_t incrby(std::string_view key, int64_t increment) {
        return redlite_incrby(db_, std::string(key).c_str(), increment);
    }

    /**
     * DECRBY key decrement
     */
    int64_t decrby(std::string_view key, int64_t decrement) {
        return redlite_decrby(db_, std::string(key).c_str(), decrement);
    }

    /**
     * INCRBYFLOAT key increment
     */
    double incrbyfloat(std::string_view key, double increment) {
        char* result = redlite_incrbyfloat(db_, std::string(key).c_str(), increment);
        if (!result) throw Error::from_last_error();
        double val = std::stod(result);
        redlite_free_string(result);
        return val;
    }

    /**
     * MGET key [key ...]
     */
    std::vector<std::optional<std::string>> mget(const std::vector<std::string>& keys) {
        std::vector<const char*> key_ptrs;
        key_ptrs.reserve(keys.size());
        for (const auto& k : keys) key_ptrs.push_back(k.c_str());

        RedliteBytesArray arr = redlite_mget(db_, key_ptrs.data(), key_ptrs.size());
        std::vector<std::optional<std::string>> result;
        result.reserve(arr.len);

        for (size_t i = 0; i < arr.len; ++i) {
            if (arr.items[i].data) {
                result.emplace_back(std::string(
                    reinterpret_cast<const char*>(arr.items[i].data),
                    arr.items[i].len));
            } else {
                result.emplace_back(std::nullopt);
            }
        }

        redlite_free_bytes_array(arr);
        return result;
    }

    /**
     * MSET key value [key value ...]
     */
    bool mset(const std::unordered_map<std::string, std::string>& pairs) {
        std::vector<RedliteKV> kvs;
        kvs.reserve(pairs.size());
        for (const auto& [k, v] : pairs) {
            kvs.push_back({k.c_str(),
                          reinterpret_cast<const uint8_t*>(v.data()),
                          v.size()});
        }
        return redlite_mset(db_, kvs.data(), kvs.size()) == 0;
    }

    // ==================== Key Commands ====================

    /**
     * DEL key [key ...]
     * @return Number of keys deleted
     */
    int64_t del(const std::vector<std::string>& keys) {
        std::vector<const char*> key_ptrs;
        key_ptrs.reserve(keys.size());
        for (const auto& k : keys) key_ptrs.push_back(k.c_str());
        return redlite_del(db_, key_ptrs.data(), key_ptrs.size());
    }

    int64_t del(std::string_view key) {
        std::string key_str(key);
        const char* k = key_str.c_str();
        return redlite_del(db_, &k, 1);
    }

    /**
     * EXISTS key [key ...]
     * @return Number of keys that exist
     */
    int64_t exists(const std::vector<std::string>& keys) {
        std::vector<const char*> key_ptrs;
        key_ptrs.reserve(keys.size());
        for (const auto& k : keys) key_ptrs.push_back(k.c_str());
        return redlite_exists(db_, key_ptrs.data(), key_ptrs.size());
    }

    bool exists(std::string_view key) {
        std::string k(key);
        const char* kp = k.c_str();
        return redlite_exists(db_, &kp, 1) > 0;
    }

    /**
     * TYPE key
     */
    std::optional<std::string> type(std::string_view key) {
        char* result = redlite_type(db_, std::string(key).c_str());
        if (!result) return std::nullopt;
        std::string t(result);
        redlite_free_string(result);
        return t;
    }

    /**
     * TTL key
     * @return -2 if key doesn't exist, -1 if no TTL, else seconds
     */
    int64_t ttl(std::string_view key) {
        return redlite_ttl(db_, std::string(key).c_str());
    }

    /**
     * PTTL key (milliseconds)
     */
    int64_t pttl(std::string_view key) {
        return redlite_pttl(db_, std::string(key).c_str());
    }

    /**
     * EXPIRE key seconds
     */
    bool expire(std::string_view key, int64_t seconds) {
        return redlite_expire(db_, std::string(key).c_str(), seconds) == 1;
    }

    /**
     * PEXPIRE key milliseconds
     */
    bool pexpire(std::string_view key, int64_t milliseconds) {
        return redlite_pexpire(db_, std::string(key).c_str(), milliseconds) == 1;
    }

    /**
     * EXPIREAT key unix_timestamp
     */
    bool expireat(std::string_view key, int64_t unix_seconds) {
        return redlite_expireat(db_, std::string(key).c_str(), unix_seconds) == 1;
    }

    /**
     * PEXPIREAT key unix_timestamp_ms
     */
    bool pexpireat(std::string_view key, int64_t unix_ms) {
        return redlite_pexpireat(db_, std::string(key).c_str(), unix_ms) == 1;
    }

    /**
     * PERSIST key - Remove TTL
     */
    bool persist(std::string_view key) {
        return redlite_persist(db_, std::string(key).c_str()) == 1;
    }

    /**
     * RENAME key newkey
     */
    bool rename(std::string_view key, std::string_view newkey) {
        return redlite_rename(db_, std::string(key).c_str(),
                             std::string(newkey).c_str()) == 0;
    }

    /**
     * RENAMENX key newkey
     */
    bool renamenx(std::string_view key, std::string_view newkey) {
        return redlite_renamenx(db_, std::string(key).c_str(),
                               std::string(newkey).c_str()) == 1;
    }

    /**
     * KEYS pattern
     */
    std::vector<std::string> keys(std::string_view pattern = "*") {
        RedliteStringArray arr = redlite_keys(db_, std::string(pattern).c_str());
        std::vector<std::string> result;
        result.reserve(arr.len);
        for (size_t i = 0; i < arr.len; ++i) {
            result.emplace_back(arr.strings[i]);
        }
        redlite_free_string_array(arr);
        return result;
    }

    /**
     * DBSIZE
     */
    int64_t dbsize() {
        return redlite_dbsize(db_);
    }

    /**
     * FLUSHDB
     */
    bool flushdb() {
        return redlite_flushdb(db_) == 0;
    }

    /**
     * SELECT db
     */
    bool select(int db_num) {
        return redlite_select(db_, db_num) == 0;
    }

    // ==================== Hash Commands ====================

    /**
     * HSET key field value
     */
    int64_t hset(std::string_view key, std::string_view field, std::string_view value) {
        std::string key_str(key);
        std::string field_str(field);
        const char* f = field_str.c_str();
        RedliteBytes v = {reinterpret_cast<uint8_t*>(const_cast<char*>(value.data())), value.size()};
        return redlite_hset(db_, key_str.c_str(), &f, &v, 1);
    }

    /**
     * HSET key field value [field value ...]
     */
    int64_t hset(std::string_view key, const std::unordered_map<std::string, std::string>& fields) {
        std::vector<const char*> field_ptrs;
        std::vector<RedliteBytes> values;
        field_ptrs.reserve(fields.size());
        values.reserve(fields.size());

        for (const auto& [f, v] : fields) {
            field_ptrs.push_back(f.c_str());
            values.push_back({reinterpret_cast<uint8_t*>(const_cast<char*>(v.data())), v.size()});
        }

        return redlite_hset(db_, std::string(key).c_str(),
                           field_ptrs.data(), values.data(), fields.size());
    }

    /**
     * HGET key field
     */
    std::optional<std::string> hget(std::string_view key, std::string_view field) {
        RedliteBytes result = redlite_hget(db_, std::string(key).c_str(),
                                           std::string(field).c_str());
        if (!result.data) return std::nullopt;
        Bytes b(result);
        return b.to_string();
    }

    /**
     * HDEL key field [field ...]
     */
    int64_t hdel(std::string_view key, const std::vector<std::string>& fields) {
        std::vector<const char*> field_ptrs;
        field_ptrs.reserve(fields.size());
        for (const auto& f : fields) field_ptrs.push_back(f.c_str());
        return redlite_hdel(db_, std::string(key).c_str(),
                           field_ptrs.data(), field_ptrs.size());
    }

    /**
     * HEXISTS key field
     */
    bool hexists(std::string_view key, std::string_view field) {
        return redlite_hexists(db_, std::string(key).c_str(),
                              std::string(field).c_str()) == 1;
    }

    /**
     * HLEN key
     */
    int64_t hlen(std::string_view key) {
        return redlite_hlen(db_, std::string(key).c_str());
    }

    /**
     * HKEYS key
     */
    std::vector<std::string> hkeys(std::string_view key) {
        RedliteStringArray arr = redlite_hkeys(db_, std::string(key).c_str());
        std::vector<std::string> result;
        result.reserve(arr.len);
        for (size_t i = 0; i < arr.len; ++i) {
            result.emplace_back(arr.strings[i]);
        }
        redlite_free_string_array(arr);
        return result;
    }

    /**
     * HVALS key
     */
    std::vector<std::string> hvals(std::string_view key) {
        RedliteBytesArray arr = redlite_hvals(db_, std::string(key).c_str());
        std::vector<std::string> result;
        result.reserve(arr.len);
        for (size_t i = 0; i < arr.len; ++i) {
            result.emplace_back(
                reinterpret_cast<const char*>(arr.items[i].data),
                arr.items[i].len);
        }
        redlite_free_bytes_array(arr);
        return result;
    }

    /**
     * HINCRBY key field increment
     */
    int64_t hincrby(std::string_view key, std::string_view field, int64_t increment) {
        return redlite_hincrby(db_, std::string(key).c_str(),
                              std::string(field).c_str(), increment);
    }

    /**
     * HGETALL key
     */
    std::unordered_map<std::string, std::string> hgetall(std::string_view key) {
        RedliteBytesArray arr = redlite_hgetall(db_, std::string(key).c_str());
        std::unordered_map<std::string, std::string> result;
        for (size_t i = 0; i + 1 < arr.len; i += 2) {
            std::string field(reinterpret_cast<const char*>(arr.items[i].data), arr.items[i].len);
            std::string value(reinterpret_cast<const char*>(arr.items[i+1].data), arr.items[i+1].len);
            result[field] = value;
        }
        redlite_free_bytes_array(arr);
        return result;
    }

    /**
     * HMGET key field [field ...]
     */
    std::vector<std::optional<std::string>> hmget(std::string_view key,
                                                   const std::vector<std::string>& fields) {
        std::vector<const char*> field_ptrs;
        field_ptrs.reserve(fields.size());
        for (const auto& f : fields) field_ptrs.push_back(f.c_str());

        RedliteBytesArray arr = redlite_hmget(db_, std::string(key).c_str(),
                                              field_ptrs.data(), field_ptrs.size());
        std::vector<std::optional<std::string>> result;
        result.reserve(arr.len);

        for (size_t i = 0; i < arr.len; ++i) {
            if (arr.items[i].data) {
                result.emplace_back(std::string(
                    reinterpret_cast<const char*>(arr.items[i].data),
                    arr.items[i].len));
            } else {
                result.emplace_back(std::nullopt);
            }
        }

        redlite_free_bytes_array(arr);
        return result;
    }

    // ==================== List Commands ====================

    /**
     * LPUSH key value [value ...]
     */
    int64_t lpush(std::string_view key, const std::vector<std::string>& values) {
        std::vector<RedliteBytes> bytes;
        bytes.reserve(values.size());
        for (const auto& v : values) {
            bytes.push_back({reinterpret_cast<uint8_t*>(const_cast<char*>(v.data())), v.size()});
        }
        return redlite_lpush(db_, std::string(key).c_str(), bytes.data(), bytes.size());
    }

    int64_t lpush(std::string_view key, std::string_view value) {
        RedliteBytes v = {reinterpret_cast<uint8_t*>(const_cast<char*>(value.data())), value.size()};
        return redlite_lpush(db_, std::string(key).c_str(), &v, 1);
    }

    /**
     * RPUSH key value [value ...]
     */
    int64_t rpush(std::string_view key, const std::vector<std::string>& values) {
        std::vector<RedliteBytes> bytes;
        bytes.reserve(values.size());
        for (const auto& v : values) {
            bytes.push_back({reinterpret_cast<uint8_t*>(const_cast<char*>(v.data())), v.size()});
        }
        return redlite_rpush(db_, std::string(key).c_str(), bytes.data(), bytes.size());
    }

    int64_t rpush(std::string_view key, std::string_view value) {
        RedliteBytes v = {reinterpret_cast<uint8_t*>(const_cast<char*>(value.data())), value.size()};
        return redlite_rpush(db_, std::string(key).c_str(), &v, 1);
    }

    /**
     * LPOP key [count]
     */
    std::vector<std::string> lpop(std::string_view key, size_t count = 1) {
        RedliteBytesArray arr = redlite_lpop(db_, std::string(key).c_str(), count);
        std::vector<std::string> result;
        result.reserve(arr.len);
        for (size_t i = 0; i < arr.len; ++i) {
            result.emplace_back(
                reinterpret_cast<const char*>(arr.items[i].data),
                arr.items[i].len);
        }
        redlite_free_bytes_array(arr);
        return result;
    }

    /**
     * RPOP key [count]
     */
    std::vector<std::string> rpop(std::string_view key, size_t count = 1) {
        RedliteBytesArray arr = redlite_rpop(db_, std::string(key).c_str(), count);
        std::vector<std::string> result;
        result.reserve(arr.len);
        for (size_t i = 0; i < arr.len; ++i) {
            result.emplace_back(
                reinterpret_cast<const char*>(arr.items[i].data),
                arr.items[i].len);
        }
        redlite_free_bytes_array(arr);
        return result;
    }

    /**
     * LLEN key
     */
    int64_t llen(std::string_view key) {
        return redlite_llen(db_, std::string(key).c_str());
    }

    /**
     * LRANGE key start stop
     */
    std::vector<std::string> lrange(std::string_view key, int64_t start, int64_t stop) {
        RedliteBytesArray arr = redlite_lrange(db_, std::string(key).c_str(), start, stop);
        std::vector<std::string> result;
        result.reserve(arr.len);
        for (size_t i = 0; i < arr.len; ++i) {
            result.emplace_back(
                reinterpret_cast<const char*>(arr.items[i].data),
                arr.items[i].len);
        }
        redlite_free_bytes_array(arr);
        return result;
    }

    /**
     * LINDEX key index
     */
    std::optional<std::string> lindex(std::string_view key, int64_t index) {
        RedliteBytes result = redlite_lindex(db_, std::string(key).c_str(), index);
        if (!result.data) return std::nullopt;
        Bytes b(result);
        return b.to_string();
    }

    // ==================== Set Commands ====================

    /**
     * SADD key member [member ...]
     */
    int64_t sadd(std::string_view key, const std::vector<std::string>& members) {
        std::vector<RedliteBytes> bytes;
        bytes.reserve(members.size());
        for (const auto& m : members) {
            bytes.push_back({reinterpret_cast<uint8_t*>(const_cast<char*>(m.data())), m.size()});
        }
        return redlite_sadd(db_, std::string(key).c_str(), bytes.data(), bytes.size());
    }

    int64_t sadd(std::string_view key, std::string_view member) {
        RedliteBytes m = {reinterpret_cast<uint8_t*>(const_cast<char*>(member.data())), member.size()};
        return redlite_sadd(db_, std::string(key).c_str(), &m, 1);
    }

    /**
     * SREM key member [member ...]
     */
    int64_t srem(std::string_view key, const std::vector<std::string>& members) {
        std::vector<RedliteBytes> bytes;
        bytes.reserve(members.size());
        for (const auto& m : members) {
            bytes.push_back({reinterpret_cast<uint8_t*>(const_cast<char*>(m.data())), m.size()});
        }
        return redlite_srem(db_, std::string(key).c_str(), bytes.data(), bytes.size());
    }

    /**
     * SMEMBERS key
     */
    std::vector<std::string> smembers(std::string_view key) {
        RedliteBytesArray arr = redlite_smembers(db_, std::string(key).c_str());
        std::vector<std::string> result;
        result.reserve(arr.len);
        for (size_t i = 0; i < arr.len; ++i) {
            result.emplace_back(
                reinterpret_cast<const char*>(arr.items[i].data),
                arr.items[i].len);
        }
        redlite_free_bytes_array(arr);
        return result;
    }

    /**
     * SISMEMBER key member
     */
    bool sismember(std::string_view key, std::string_view member) {
        return redlite_sismember(db_, std::string(key).c_str(),
                                reinterpret_cast<const uint8_t*>(member.data()),
                                member.size()) == 1;
    }

    /**
     * SCARD key
     */
    int64_t scard(std::string_view key) {
        return redlite_scard(db_, std::string(key).c_str());
    }

    // ==================== Sorted Set Commands ====================

    /**
     * ZADD key score member [score member ...]
     */
    int64_t zadd(std::string_view key, const std::vector<ZMember>& members) {
        std::vector<RedliteZMember> zm;
        zm.reserve(members.size());
        for (const auto& m : members) {
            zm.push_back({m.score,
                         reinterpret_cast<const uint8_t*>(m.member.data()),
                         m.member.size()});
        }
        return redlite_zadd(db_, std::string(key).c_str(), zm.data(), zm.size());
    }

    int64_t zadd(std::string_view key, double score, std::string_view member) {
        RedliteZMember zm = {score,
                            reinterpret_cast<const uint8_t*>(member.data()),
                            member.size()};
        return redlite_zadd(db_, std::string(key).c_str(), &zm, 1);
    }

    /**
     * ZREM key member [member ...]
     */
    int64_t zrem(std::string_view key, const std::vector<std::string>& members) {
        std::vector<RedliteBytes> bytes;
        bytes.reserve(members.size());
        for (const auto& m : members) {
            bytes.push_back({reinterpret_cast<uint8_t*>(const_cast<char*>(m.data())), m.size()});
        }
        return redlite_zrem(db_, std::string(key).c_str(), bytes.data(), bytes.size());
    }

    /**
     * ZSCORE key member
     */
    std::optional<double> zscore(std::string_view key, std::string_view member) {
        double score = redlite_zscore(db_, std::string(key).c_str(),
                                      reinterpret_cast<const uint8_t*>(member.data()),
                                      member.size());
        if (std::isnan(score)) return std::nullopt;
        return score;
    }

    /**
     * ZCARD key
     */
    int64_t zcard(std::string_view key) {
        return redlite_zcard(db_, std::string(key).c_str());
    }

    /**
     * ZCOUNT key min max
     */
    int64_t zcount(std::string_view key, double min, double max) {
        return redlite_zcount(db_, std::string(key).c_str(), min, max);
    }

    /**
     * ZINCRBY key increment member
     */
    double zincrby(std::string_view key, double increment, std::string_view member) {
        return redlite_zincrby(db_, std::string(key).c_str(), increment,
                              reinterpret_cast<const uint8_t*>(member.data()),
                              member.size());
    }

    /**
     * ZRANGE key start stop [WITHSCORES]
     */
    std::vector<std::string> zrange(std::string_view key, int64_t start, int64_t stop) {
        RedliteBytesArray arr = redlite_zrange(db_, std::string(key).c_str(),
                                               start, stop, 0);
        std::vector<std::string> result;
        result.reserve(arr.len);
        for (size_t i = 0; i < arr.len; ++i) {
            result.emplace_back(
                reinterpret_cast<const char*>(arr.items[i].data),
                arr.items[i].len);
        }
        redlite_free_bytes_array(arr);
        return result;
    }

    std::vector<ZMember> zrange_with_scores(std::string_view key, int64_t start, int64_t stop) {
        RedliteBytesArray arr = redlite_zrange(db_, std::string(key).c_str(),
                                               start, stop, 1);
        std::vector<ZMember> result;
        for (size_t i = 0; i + 1 < arr.len; i += 2) {
            std::string member(reinterpret_cast<const char*>(arr.items[i].data), arr.items[i].len);
            std::string score_str(reinterpret_cast<const char*>(arr.items[i+1].data), arr.items[i+1].len);
            result.emplace_back(std::stod(score_str), member);
        }
        redlite_free_bytes_array(arr);
        return result;
    }

    /**
     * ZREVRANGE key start stop [WITHSCORES]
     */
    std::vector<std::string> zrevrange(std::string_view key, int64_t start, int64_t stop) {
        RedliteBytesArray arr = redlite_zrevrange(db_, std::string(key).c_str(),
                                                  start, stop, 0);
        std::vector<std::string> result;
        result.reserve(arr.len);
        for (size_t i = 0; i < arr.len; ++i) {
            result.emplace_back(
                reinterpret_cast<const char*>(arr.items[i].data),
                arr.items[i].len);
        }
        redlite_free_bytes_array(arr);
        return result;
    }

    // ==================== Server Commands ====================

    /**
     * VACUUM - Compact the database
     */
    int64_t vacuum() {
        return redlite_vacuum(db_);
    }

    /**
     * Get library version
     */
    static std::string version() {
        char* v = redlite_version();
        if (!v) return "";
        std::string result(v);
        redlite_free_string(v);
        return result;
    }

    // ==================== JSON Commands ====================

    /**
     * JSON.SET key path value [NX|XX]
     * @return true if set, false if NX/XX condition not met
     */
    bool json_set(std::string_view key, std::string_view path, std::string_view value,
                  const JsonSetOptions& opts = {}) {
        int result = redlite_json_set(db_, std::string(key).c_str(),
                                      std::string(path).c_str(),
                                      std::string(value).c_str(),
                                      opts.nx ? 1 : 0, opts.xx ? 1 : 0);
        if (result < 0) throw Error::from_last_error();
        return result == 1;
    }

    /**
     * JSON.GET key [path ...]
     * @return JSON-encoded result or empty if not found
     */
    std::optional<std::string> json_get(std::string_view key,
                                        const std::vector<std::string>& paths = {"$"}) {
        std::vector<const char*> path_ptrs;
        path_ptrs.reserve(paths.size());
        for (const auto& p : paths) path_ptrs.push_back(p.c_str());

        char* result = redlite_json_get(db_, std::string(key).c_str(),
                                        path_ptrs.data(), path_ptrs.size());
        if (!result) return std::nullopt;
        std::string str(result);
        redlite_free_string(result);
        return str;
    }

    /**
     * JSON.DEL key [path]
     * @return Number of paths deleted
     */
    int64_t json_del(std::string_view key, std::string_view path = "$") {
        return redlite_json_del(db_, std::string(key).c_str(),
                               std::string(path).c_str());
    }

    /**
     * JSON.TYPE key [path]
     * @return Type name or empty if not found
     */
    std::optional<std::string> json_type(std::string_view key, std::string_view path = "$") {
        char* result = redlite_json_type(db_, std::string(key).c_str(),
                                         std::string(path).c_str());
        if (!result) return std::nullopt;
        std::string str(result);
        redlite_free_string(result);
        return str;
    }

    /**
     * JSON.NUMINCRBY key path increment
     * @return New value as JSON string
     */
    std::optional<std::string> json_numincrby(std::string_view key, std::string_view path,
                                               double increment) {
        char* result = redlite_json_numincrby(db_, std::string(key).c_str(),
                                              std::string(path).c_str(), increment);
        if (!result) return std::nullopt;
        std::string str(result);
        redlite_free_string(result);
        return str;
    }

    /**
     * JSON.STRAPPEND key path value
     * @return New length of string
     */
    int64_t json_strappend(std::string_view key, std::string_view path, std::string_view value) {
        int64_t result = redlite_json_strappend(db_, std::string(key).c_str(),
                                                std::string(path).c_str(),
                                                std::string(value).c_str());
        if (result < 0) throw Error::from_last_error();
        return result;
    }

    /**
     * JSON.STRLEN key [path]
     * @return Length of string
     */
    int64_t json_strlen(std::string_view key, std::string_view path = "$") {
        return redlite_json_strlen(db_, std::string(key).c_str(),
                                   std::string(path).c_str());
    }

    /**
     * JSON.ARRAPPEND key path value [value ...]
     * @return New length of array
     */
    int64_t json_arrappend(std::string_view key, std::string_view path,
                           const std::vector<std::string>& values) {
        std::vector<const char*> value_ptrs;
        value_ptrs.reserve(values.size());
        for (const auto& v : values) value_ptrs.push_back(v.c_str());

        int64_t result = redlite_json_arrappend(db_, std::string(key).c_str(),
                                                std::string(path).c_str(),
                                                value_ptrs.data(), value_ptrs.size());
        if (result < 0) throw Error::from_last_error();
        return result;
    }

    /**
     * JSON.ARRLEN key [path]
     * @return Length of array
     */
    int64_t json_arrlen(std::string_view key, std::string_view path = "$") {
        return redlite_json_arrlen(db_, std::string(key).c_str(),
                                   std::string(path).c_str());
    }

    /**
     * JSON.ARRPOP key [path [index]]
     * @return Popped value as JSON string
     */
    std::optional<std::string> json_arrpop(std::string_view key, std::string_view path = "$",
                                            int64_t index = -1) {
        char* result = redlite_json_arrpop(db_, std::string(key).c_str(),
                                           std::string(path).c_str(), index);
        if (!result) return std::nullopt;
        std::string str(result);
        redlite_free_string(result);
        return str;
    }

    /**
     * JSON.CLEAR key [path]
     * @return Number of values cleared
     */
    int64_t json_clear(std::string_view key, std::string_view path = "$") {
        return redlite_json_clear(db_, std::string(key).c_str(),
                                  std::string(path).c_str());
    }

    // ==================== History Commands ====================

    /**
     * Enable history tracking globally
     * @param retention_type "unlimited", "time", or "count"
     * @param retention_value Value for time (ms) or count retention
     */
    void history_enable_global(std::string_view retention_type = "unlimited",
                               int64_t retention_value = 0) {
        int result = redlite_history_enable_global(db_, std::string(retention_type).c_str(),
                                                   retention_value);
        if (result < 0) throw Error::from_last_error();
    }

    /**
     * Enable history tracking for a specific database
     */
    void history_enable_database(int db_num, std::string_view retention_type = "unlimited",
                                 int64_t retention_value = 0) {
        int result = redlite_history_enable_database(db_, db_num,
                                                     std::string(retention_type).c_str(),
                                                     retention_value);
        if (result < 0) throw Error::from_last_error();
    }

    /**
     * Enable history tracking for a specific key
     */
    void history_enable_key(std::string_view key, std::string_view retention_type = "unlimited",
                            int64_t retention_value = 0) {
        int result = redlite_history_enable_key(db_, std::string(key).c_str(),
                                                std::string(retention_type).c_str(),
                                                retention_value);
        if (result < 0) throw Error::from_last_error();
    }

    /**
     * Disable history tracking globally
     */
    void history_disable_global() {
        int result = redlite_history_disable_global(db_);
        if (result < 0) throw Error::from_last_error();
    }

    /**
     * Disable history tracking for a specific database
     */
    void history_disable_database(int db_num) {
        int result = redlite_history_disable_database(db_, db_num);
        if (result < 0) throw Error::from_last_error();
    }

    /**
     * Disable history tracking for a specific key
     */
    void history_disable_key(std::string_view key) {
        int result = redlite_history_disable_key(db_, std::string(key).c_str());
        if (result < 0) throw Error::from_last_error();
    }

    /**
     * Check if history tracking is enabled for a key
     */
    bool is_history_enabled(std::string_view key) {
        return redlite_is_history_enabled(db_, std::string(key).c_str()) == 1;
    }

    // ==================== FTS Commands ====================

    /**
     * Enable FTS indexing globally
     */
    void fts_enable_global() {
        int result = redlite_fts_enable_global(db_);
        if (result < 0) throw Error::from_last_error();
    }

    /**
     * Enable FTS indexing for a specific database
     */
    void fts_enable_database(int db_num) {
        int result = redlite_fts_enable_database(db_, db_num);
        if (result < 0) throw Error::from_last_error();
    }

    /**
     * Enable FTS indexing for keys matching a pattern
     */
    void fts_enable_pattern(std::string_view pattern) {
        int result = redlite_fts_enable_pattern(db_, std::string(pattern).c_str());
        if (result < 0) throw Error::from_last_error();
    }

    /**
     * Enable FTS indexing for a specific key
     */
    void fts_enable_key(std::string_view key) {
        int result = redlite_fts_enable_key(db_, std::string(key).c_str());
        if (result < 0) throw Error::from_last_error();
    }

    /**
     * Disable FTS indexing globally
     */
    void fts_disable_global() {
        int result = redlite_fts_disable_global(db_);
        if (result < 0) throw Error::from_last_error();
    }

    /**
     * Disable FTS indexing for a specific database
     */
    void fts_disable_database(int db_num) {
        int result = redlite_fts_disable_database(db_, db_num);
        if (result < 0) throw Error::from_last_error();
    }

    /**
     * Disable FTS indexing for keys matching a pattern
     */
    void fts_disable_pattern(std::string_view pattern) {
        int result = redlite_fts_disable_pattern(db_, std::string(pattern).c_str());
        if (result < 0) throw Error::from_last_error();
    }

    /**
     * Disable FTS indexing for a specific key
     */
    void fts_disable_key(std::string_view key) {
        int result = redlite_fts_disable_key(db_, std::string(key).c_str());
        if (result < 0) throw Error::from_last_error();
    }

    /**
     * Check if FTS indexing is enabled for a key
     */
    bool is_fts_enabled(std::string_view key) {
        return redlite_is_fts_enabled(db_, std::string(key).c_str()) == 1;
    }

    // ==================== KeyInfo Command ====================

    /**
     * KEYINFO - Get detailed information about a key
     * @return KeyInfo or empty if key doesn't exist
     */
    std::optional<KeyInfo> keyinfo(std::string_view key) {
        RedliteKeyInfo info = redlite_keyinfo(db_, std::string(key).c_str());

        if (info.valid == 0) {
            redlite_free_keyinfo(info);
            return std::nullopt;
        }

        KeyInfo result;
        result.type = info.key_type ? std::string(info.key_type) : "none";
        result.ttl = info.ttl;
        result.created_at = info.created_at;
        result.updated_at = info.updated_at;

        redlite_free_keyinfo(info);
        return result;
    }

private:
    explicit Database(RedliteDb* db) : db_(db) {}
    RedliteDb* db_;
};

} // namespace redlite

#endif // REDLITE_HPP
