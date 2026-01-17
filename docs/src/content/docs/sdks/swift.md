---
title: Swift SDK
description: Redlite SDK for Swift/iOS/macOS
---

Swift SDK with FFI bindings for Apple platforms.

## Installation

Add to your `Package.swift`:

```swift
dependencies: [
    .package(url: "https://github.com/russellromney/redlite", from: "0.1.0")
]
```

## Quick Start

```swift
import Redlite

// Open in-memory database
let db = try Redlite(path: ":memory:")

// Or file-based
let db = try Redlite(path: "/path/to/db.db")

// String operations
try db.set("key", value: "value")
if let val = try db.get("key") {
    print(String(data: val, encoding: .utf8)!)  // "value"
}

// Hash operations
try db.hset("user:1", field: "name", value: "Alice")
try db.hset("user:1", field: "age", value: "30")
let user = try db.hgetall("user:1")

// List operations
try db.lpush("queue", values: ["job1", "job2"])
let job = try db.rpop("queue")

// Set operations
try db.sadd("tags", members: ["redis", "sqlite"])
let members = try db.smembers("tags")

// Sorted sets
try db.zadd("scores", members: [(100.0, "player1"), (85.0, "player2")])
let top = try db.zrevrange("scores", start: 0, stop: 9)

db.close()
```

## Platform Support

- iOS (arm64, simulator)
- macOS (arm64, x86_64)
- tvOS, watchOS

## API Overview

**Strings**: `set`, `get`, `incr`, `decr`, `append`, `mget`, `mset`

**Keys**: `del`, `exists`, `type`, `ttl`, `expire`, `keys`

**Hashes**: `hset`, `hget`, `hdel`, `hgetall`, `hmget`

**Lists**: `lpush`, `rpush`, `lpop`, `rpop`, `llen`, `lrange`

**Sets**: `sadd`, `srem`, `smembers`, `sismember`, `scard`

**Sorted Sets**: `zadd`, `zrem`, `zscore`, `zrange`, `zrevrange`

## Testing

```bash
cd sdks/redlite-swift
make test
```

## Links

- [Full README](https://github.com/russellromney/redlite/tree/main/sdks/redlite-swift)
- [Source Code](https://github.com/russellromney/redlite/tree/main/sdks/redlite-swift)
