---
title: C++ SDK
description: Redlite SDK for C++
---

C++ SDK with FFI bindings via CMake.

## Installation

Add to your CMakeLists.txt:

```cmake
find_package(Redlite REQUIRED)
target_link_libraries(your_target redlite)
```

Or build from source:

```bash
cd sdks/redlite-cpp
mkdir build && cd build
cmake ..
make
sudo make install
```

## Quick Start

```cpp
#include <redlite/redlite.h>
#include <iostream>

int main() {
    // Open in-memory database
    Redlite db(":memory:");

    // String operations
    db.set("key", "value");
    auto val = db.get("key");
    std::cout << val.value() << std::endl;  // "value"

    // Increment
    db.incr("counter");
    db.incrby("counter", 5);

    // Hash operations
    db.hset("user:1", "name", "Alice");
    db.hset("user:1", "age", "30");
    auto user = db.hgetall("user:1");

    // List operations
    db.lpush("queue", {"job1", "job2"});
    auto job = db.rpop("queue");

    // Set operations
    db.sadd("tags", {"redis", "sqlite"});
    bool isMember = db.sismember("tags", "redis");

    // Sorted set operations
    db.zadd("scores", {{100.0, "player1"}, {85.0, "player2"}});
    auto top = db.zrevrange("scores", 0, 9);

    return 0;
}
```

## API Overview

**Strings**: `set`, `get`, `incr`, `decr`, `append`, `mget`, `mset`

**Keys**: `del`, `exists`, `type`, `ttl`, `expire`, `keys`

**Hashes**: `hset`, `hget`, `hdel`, `hgetall`, `hmget`

**Lists**: `lpush`, `rpush`, `lpop`, `rpop`, `llen`, `lrange`

**Sets**: `sadd`, `srem`, `smembers`, `sismember`, `scard`

**Sorted Sets**: `zadd`, `zrem`, `zscore`, `zrange`, `zrevrange`

## Testing

```bash
cd sdks/redlite-cpp
make test
```

## Links

- [Full README](https://github.com/russellromney/redlite/tree/main/sdks/redlite-cpp)
- [Source Code](https://github.com/russellromney/redlite/tree/main/sdks/redlite-cpp)
