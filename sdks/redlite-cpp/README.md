# Redlite C++ SDK

Modern C++17 SDK for Redlite - Redis API with SQLite durability.

## Features

- **Header-only library** - just include and use
- **RAII resource management** - automatic cleanup
- **Modern C++17 API** - `std::optional`, `std::string_view`, move semantics
- **Full Redis command coverage** - strings, hashes, lists, sets, sorted sets
- **Type-safe** - proper C++ types throughout

## Requirements

- C++17 compatible compiler (GCC 7+, Clang 5+, MSVC 2017+)
- CMake 3.16+
- libredlite_ffi (built from the Rust core)

## Building the FFI Library

Before using the C++ SDK, build the Redlite FFI library:

```bash
cd ../../crates/redlite-ffi
cargo build --release
```

This creates `libredlite_ffi.dylib` (macOS), `libredlite_ffi.so` (Linux), or `redlite_ffi.dll` (Windows).

## Installation

### Using CMake

```bash
mkdir build && cd build
cmake ..
cmake --build .
```

### Custom FFI Path

If the FFI library is in a non-standard location:

```bash
cmake -DREDLITE_LIB_PATH=/path/to/lib ..
```

### As a Subdirectory

```cmake
add_subdirectory(redlite-cpp)
target_link_libraries(your_app PRIVATE redlite)
```

## Quick Start

```cpp
#include <redlite/redlite.hpp>
#include <iostream>

int main() {
    using namespace redlite;

    // Open in-memory database
    auto db = Database::open_memory();

    // String operations
    db.set("key", "value");
    auto val = db.get("key");
    if (val) {
        std::cout << "Got: " << *val << "\n";
    }

    // Counter
    db.incr("counter");
    db.incrby("counter", 10);

    // Hash
    db.hset("user:1", {
        {"name", "Alice"},
        {"email", "alice@example.com"}
    });
    auto name = db.hget("user:1", "name");

    // List
    db.rpush("queue", {"task1", "task2", "task3"});
    auto tasks = db.lrange("queue", 0, -1);

    // Set
    db.sadd("tags", {"redis", "cpp", "database"});
    bool has_tag = db.sismember("tags", "redis");

    // Sorted set
    db.zadd("scores", {{100, "alice"}, {150, "bob"}});
    auto top = db.zrevrange("scores", 0, 2);

    return 0;
}
```

## API Reference

### Database

```cpp
// Open database
Database db("/path/to/db.sqlite");
auto db = Database::open_memory();
auto db = Database::open_with_cache("/path/to/db.sqlite", 128);

// Get version
std::string version = Database::version();
```

### String Commands

```cpp
bool set(key, value, ttl_seconds = 0);
std::optional<std::string> get(key);
bool setex(key, seconds, value);
bool psetex(key, milliseconds, value);
std::optional<std::string> getdel(key);
int64_t append(key, value);
int64_t strlen(key);
std::string getrange(key, start, end);
int64_t setrange(key, offset, value);
int64_t incr(key);
int64_t decr(key);
int64_t incrby(key, increment);
int64_t decrby(key, decrement);
double incrbyfloat(key, increment);
bool mset(pairs);
std::vector<std::optional<std::string>> mget(keys);
```

### Key Commands

```cpp
int64_t del(key);
int64_t del(keys);
bool exists(key);
int64_t exists(keys);
std::optional<std::string> type(key);
int64_t ttl(key);
int64_t pttl(key);
bool expire(key, seconds);
bool pexpire(key, milliseconds);
bool expireat(key, unix_timestamp);
bool pexpireat(key, unix_timestamp_ms);
bool persist(key);
bool rename(key, newkey);
bool renamenx(key, newkey);
std::vector<std::string> keys(pattern = "*");
int64_t dbsize();
bool flushdb();
bool select(db_num);
```

### Hash Commands

```cpp
int64_t hset(key, field, value);
int64_t hset(key, fields_map);
std::optional<std::string> hget(key, field);
int64_t hdel(key, fields);
bool hexists(key, field);
int64_t hlen(key);
std::vector<std::string> hkeys(key);
std::vector<std::string> hvals(key);
int64_t hincrby(key, field, increment);
std::unordered_map<std::string, std::string> hgetall(key);
std::vector<std::optional<std::string>> hmget(key, fields);
```

### List Commands

```cpp
int64_t lpush(key, value);
int64_t lpush(key, values);
int64_t rpush(key, value);
int64_t rpush(key, values);
std::vector<std::string> lpop(key, count = 1);
std::vector<std::string> rpop(key, count = 1);
int64_t llen(key);
std::vector<std::string> lrange(key, start, stop);
std::optional<std::string> lindex(key, index);
```

### Set Commands

```cpp
int64_t sadd(key, member);
int64_t sadd(key, members);
int64_t srem(key, members);
std::vector<std::string> smembers(key);
bool sismember(key, member);
int64_t scard(key);
```

### Sorted Set Commands

```cpp
int64_t zadd(key, score, member);
int64_t zadd(key, members);  // vector<ZMember>
int64_t zrem(key, members);
std::optional<double> zscore(key, member);
int64_t zcard(key);
int64_t zcount(key, min, max);
double zincrby(key, increment, member);
std::vector<std::string> zrange(key, start, stop);
std::vector<ZMember> zrange_with_scores(key, start, stop);
std::vector<std::string> zrevrange(key, start, stop);
```

### Server Commands

```cpp
int64_t vacuum();  // Compact database
```

## Error Handling

All errors throw `redlite::Error`:

```cpp
try {
    auto db = Database("/invalid/path");
} catch (const redlite::Error& e) {
    std::cerr << "Error: " << e.what() << "\n";
}
```

## Running Tests

```bash
cd build
cmake -DREDLITE_BUILD_TESTS=ON ..
cmake --build .
ctest --output-on-failure
```

## License

MIT
