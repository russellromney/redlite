# Redlite Swift SDK

A Swift SDK for [Redlite](https://github.com/example/redlite), a Redis-compatible embedded database with SQLite durability.

## Features

- Full Redis command compatibility (strings, hashes, lists, sets, sorted sets)
- Thread-safe database access
- Automatic memory management
- Support for iOS, macOS, tvOS, and watchOS
- Swift Package Manager support

## Requirements

- Swift 5.9+
- iOS 13.0+ / macOS 10.15+ / tvOS 13.0+ / watchOS 6.0+

## Installation

### Swift Package Manager

Add to your `Package.swift`:

```swift
dependencies: [
    .package(url: "https://github.com/example/redlite-swift", from: "0.1.0")
]
```

Or in Xcode: File > Add Package Dependencies... and enter the repository URL.

## Quick Start

```swift
import Redlite

// Open an in-memory database
let db = try Database.openMemory()

// String operations
try db.set("name", value: "Alice")
let name = try db.getString("name")  // "Alice"

// Increment counters
try db.incr("counter")  // 1
try db.incrby("counter", increment: 5)  // 6

// Hash operations
try db.hset("user:1", ["name": "Bob", "age": "30"])
let user = try db.hgetallStrings("user:1")  // ["name": "Bob", "age": "30"]

// List operations
try db.rpush("queue", "task1", "task2", "task3")
let task = try db.lpopOne("queue")  // "task1"

// Set operations
try db.sadd("tags", "swift", "redis", "database")
let isMember = try db.sismember("tags", member: "swift")  // true

// Sorted set operations
try db.zadd("scores", (100.0, "alice"), (85.0, "bob"), (92.0, "carol"))
let top = try db.zrevrangeStrings("scores", start: 0, stop: 2)  // ["alice", "carol", "bob"]
```

## API Reference

### Database

```swift
// Open database
let db = try Database(path: "/path/to/db.sqlite")
let db = try Database.openMemory()
let db = try Database.open(path: "/path/to/db.sqlite", cacheMB: 128)

// Get version
let version = Database.version
```

### String Commands

| Method | Description |
|--------|-------------|
| `get(_:)` | Get value by key |
| `set(_:value:ttl:)` | Set key-value with optional TTL |
| `setex(_:seconds:value:)` | Set with expiration in seconds |
| `psetex(_:milliseconds:value:)` | Set with expiration in milliseconds |
| `getdel(_:)` | Get and delete atomically |
| `incr(_:)` | Increment by 1 |
| `decr(_:)` | Decrement by 1 |
| `incrby(_:increment:)` | Increment by amount |
| `decrby(_:decrement:)` | Decrement by amount |
| `incrbyfloat(_:increment:)` | Increment by float |
| `append(_:value:)` | Append to value |
| `strlen(_:)` | Get string length |
| `getrange(_:start:end:)` | Get substring |
| `setrange(_:offset:value:)` | Overwrite substring |
| `mget(_:)` | Get multiple keys |
| `mset(_:)` | Set multiple key-values |

### Key Commands

| Method | Description |
|--------|-------------|
| `del(_:)` | Delete keys |
| `exists(_:)` | Check if keys exist |
| `type(_:)` | Get key type |
| `ttl(_:)` | Get TTL in seconds |
| `pttl(_:)` | Get TTL in milliseconds |
| `expire(_:seconds:)` | Set TTL in seconds |
| `pexpire(_:milliseconds:)` | Set TTL in milliseconds |
| `persist(_:)` | Remove TTL |
| `rename(_:to:)` | Rename key |
| `renamenx(_:to:)` | Rename if new doesn't exist |
| `keys(_:)` | Find keys by pattern |
| `dbsize()` | Get key count |
| `flushdb()` | Delete all keys |

### Hash Commands

| Method | Description |
|--------|-------------|
| `hset(_:field:value:)` | Set hash field |
| `hget(_:field:)` | Get hash field |
| `hdel(_:fields:)` | Delete hash fields |
| `hexists(_:field:)` | Check if field exists |
| `hlen(_:)` | Get number of fields |
| `hkeys(_:)` | Get all field names |
| `hvals(_:)` | Get all values |
| `hgetall(_:)` | Get all fields and values |
| `hmget(_:fields:)` | Get multiple fields |
| `hincrby(_:field:increment:)` | Increment field value |

### List Commands

| Method | Description |
|--------|-------------|
| `lpush(_:values:)` | Push to head |
| `rpush(_:values:)` | Push to tail |
| `lpop(_:count:)` | Pop from head |
| `rpop(_:count:)` | Pop from tail |
| `llen(_:)` | Get list length |
| `lrange(_:start:stop:)` | Get range of elements |
| `lindex(_:index:)` | Get element by index |

### Set Commands

| Method | Description |
|--------|-------------|
| `sadd(_:members:)` | Add members |
| `srem(_:members:)` | Remove members |
| `smembers(_:)` | Get all members |
| `sismember(_:member:)` | Check membership |
| `scard(_:)` | Get set size |

### Sorted Set Commands

| Method | Description |
|--------|-------------|
| `zadd(_:members:)` | Add members with scores |
| `zrem(_:members:)` | Remove members |
| `zscore(_:member:)` | Get member score |
| `zcard(_:)` | Get set size |
| `zcount(_:min:max:)` | Count members in score range |
| `zincrby(_:member:increment:)` | Increment score |
| `zrange(_:start:stop:withScores:)` | Get by rank (low to high) |
| `zrevrange(_:start:stop:withScores:)` | Get by rank (high to low) |

## Building from Source

```bash
# Build the Rust FFI library first
cd ../../crates/redlite-ffi
cargo build --release

# Build Swift package
cd ../../sdks/redlite-swift
make build

# Run tests
make test

# Build XCFramework for distribution
make build-xcframework
```

## Thread Safety

The `Database` class is thread-safe and can be used from multiple threads. Internally, it uses an `NSLock` to synchronize access to the underlying database handle.

```swift
let db = try Database.openMemory()

DispatchQueue.concurrentPerform(iterations: 100) { i in
    try? db.set("key-\(i)", value: "value-\(i)")
}
```

## Error Handling

All database operations throw `RedliteError` on failure:

```swift
do {
    try db.set("key", value: "value")
} catch RedliteError.operationFailed(let message) {
    print("Operation failed: \(message)")
} catch {
    print("Unexpected error: \(error)")
}
```

## License

MIT License - see LICENSE file for details.
