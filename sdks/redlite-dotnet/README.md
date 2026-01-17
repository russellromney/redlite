# Redlite .NET SDK

Redis-compatible embedded database with SQLite durability for .NET applications.

## Installation

```bash
dotnet add package Redlite
```

## Quick Start

```csharp
using Redlite;

// Open an in-memory database
using var db = RedliteDb.OpenMemory();

// Or open a persistent database
// using var db = RedliteDb.Open("/path/to/database.db");

// String operations
db.Set("key", "value");
var value = db.GetString("key"); // "value"

// With TTL
db.Set("temp", "expires", ttlSeconds: 60);

// Increment/decrement
db.Incr("counter");           // 1
db.IncrBy("counter", 5);      // 6

// Hash operations
db.HSet("user:1", "name", "Alice");
db.HSet("user:1", new Dictionary<string, string>
{
    { "email", "alice@example.com" },
    { "age", "30" }
});
var name = db.HGet("user:1", "name"); // "Alice"
var all = db.HGetAll("user:1");

// List operations
db.RPush("queue", "task1", "task2", "task3");
var task = db.LPop("queue"); // "task1"
var range = db.LRange("queue", 0, -1);

// Set operations
db.SAdd("tags", "redis", "database", "embedded");
var isMember = db.SIsMember("tags", "redis"); // true
var members = db.SMembers("tags");

// Sorted set operations
db.ZAdd("leaderboard", 100, "player1");
db.ZAdd("leaderboard",
    new ZMember(200, "player2"),
    new ZMember(150, "player3"));
var top3 = db.ZRevRange("leaderboard", 0, 2);
var score = db.ZScore("leaderboard", "player1"); // 100.0

// Key operations
var exists = db.Exists("key"); // 1
var type = db.Type("key");     // "string"
var ttl = db.Ttl("temp");      // seconds until expiry
db.Del("key");
```

## API Reference

### Database Operations

```csharp
// Open database
RedliteDb.Open(string path)           // Persistent database
RedliteDb.OpenMemory()                // In-memory database
RedliteDb.OpenWithCache(path, mb)     // With custom cache size

// Maintenance
db.Vacuum()                           // Compact database
db.FlushDb()                          // Clear all keys
RedliteDb.Version()                   // Library version
```

### String Commands

```csharp
db.Get(key)                           // Get raw bytes
db.GetString(key)                     // Get as string
db.Set(key, value, ttlSeconds?)       // Set with optional TTL
db.SetEx(key, seconds, value)         // Set with expiration
db.PSetEx(key, milliseconds, value)   // Set with ms expiration
db.GetDel(key)                        // Get and delete
db.Append(key, value)                 // Append to string
db.StrLen(key)                        // String length
db.GetRange(key, start, end)          // Substring
db.SetRange(key, offset, value)       // Overwrite substring
db.Incr(key)                          // Increment by 1
db.Decr(key)                          // Decrement by 1
db.IncrBy(key, increment)             // Increment by N
db.DecrBy(key, decrement)             // Decrement by N
db.IncrByFloat(key, increment)        // Increment float
db.MGet(keys...)                      // Get multiple keys
db.MSet((key, value)...)              // Set multiple keys
```

### Key Commands

```csharp
db.Del(keys...)                       // Delete keys
db.Exists(keys...)                    // Count existing keys
db.Type(key)                          // Get key type
db.Ttl(key)                           // TTL in seconds
db.PTtl(key)                          // TTL in milliseconds
db.Expire(key, seconds)               // Set TTL
db.PExpire(key, milliseconds)         // Set TTL in ms
db.ExpireAt(key, unixSeconds)         // Set absolute expiry
db.PExpireAt(key, unixMs)             // Set absolute expiry in ms
db.Persist(key)                       // Remove TTL
db.Rename(key, newKey)                // Rename key
db.RenameNx(key, newKey)              // Rename if new doesn't exist
db.Keys(pattern)                      // Find keys by pattern
db.DbSize()                           // Number of keys
```

### Hash Commands

```csharp
db.HSet(key, field, value)            // Set single field
db.HSet(key, dict)                    // Set multiple fields
db.HGet(key, field)                   // Get field value
db.HDel(key, fields...)               // Delete fields
db.HExists(key, field)                // Check field exists
db.HLen(key)                          // Number of fields
db.HKeys(key)                         // Get all field names
db.HVals(key)                         // Get all values
db.HIncrBy(key, field, increment)     // Increment field
db.HGetAll(key)                       // Get all field-value pairs
db.HMGet(key, fields...)              // Get multiple fields
```

### List Commands

```csharp
db.LPush(key, values...)              // Push to head
db.RPush(key, values...)              // Push to tail
db.LPop(key)                          // Pop from head
db.LPop(key, count)                   // Pop multiple from head
db.RPop(key)                          // Pop from tail
db.RPop(key, count)                   // Pop multiple from tail
db.LLen(key)                          // List length
db.LRange(key, start, stop)           // Get range
db.LIndex(key, index)                 // Get by index
```

### Set Commands

```csharp
db.SAdd(key, members...)              // Add members
db.SRem(key, members...)              // Remove members
db.SMembers(key)                      // Get all members
db.SIsMember(key, member)             // Check membership
db.SCard(key)                         // Set cardinality
```

### Sorted Set Commands

```csharp
db.ZAdd(key, ZMember...)              // Add with scores
db.ZAdd(key, score, member)           // Add single member
db.ZRem(key, members...)              // Remove members
db.ZScore(key, member)                // Get score
db.ZCard(key)                         // Cardinality
db.ZCount(key, min, max)              // Count in score range
db.ZIncrBy(key, increment, member)    // Increment score
db.ZRange(key, start, stop)           // Get by index
db.ZRangeWithScores(key, start, stop) // Get with scores
db.ZRevRange(key, start, stop)        // Get in reverse order
```

## Building from Source

```bash
cd sdks/redlite-dotnet
dotnet build
dotnet test
```

## Requirements

- .NET 6.0 or later
- Native Redlite library (`libredlite_ffi.dylib` / `redlite_ffi.dll` / `libredlite_ffi.so`)

## License

MIT
