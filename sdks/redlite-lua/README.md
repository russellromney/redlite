# Redlite Lua SDK

LuaJIT FFI bindings for Redlite - a Redis-compatible embedded database with SQLite durability.

## Requirements

- **LuaJIT** (required for FFI support)
- Redlite FFI library (`libredlite_ffi.dylib` / `.so`)

## Installation

### From LuaRocks

```bash
luarocks install redlite
```

### From Source

```bash
# Build the FFI library first
cd crates/redlite-ffi
cargo build --release

# Install the Lua module
cd sdks/redlite-lua
luarocks make
```

## Quick Start

```lua
local redlite = require("redlite")

-- Open in-memory database
local db = redlite.open_memory()

-- Or open a file-backed database
-- local db = redlite.open("/path/to/database.db")

-- String operations
db:set("greeting", "Hello, World!")
print(db:get("greeting"))  -- "Hello, World!"

-- Hash operations
db:hset("user:1", {name = "Alice", age = "30"})
print(db:hget("user:1", "name"))  -- "Alice"

-- List operations
db:rpush("queue", "task1", "task2", "task3")
print(db:lpop("queue"))  -- "task1"

-- Set operations
db:sadd("tags", "lua", "redis", "database")
print(db:sismember("tags", "lua"))  -- true

-- Sorted set operations
db:zadd("scores", {alice = 100, bob = 95, carol = 98})
local top = db:zrevrange("scores", 0, 2)
-- {"alice", "carol", "bob"}

-- Clean up
db:close()
```

## Configuration

Set `REDLITE_LIB_PATH` environment variable to specify the library location:

```bash
export REDLITE_LIB_PATH=/usr/local/lib/libredlite_ffi.dylib
```

## API Reference

### Lifecycle

| Function | Description |
|----------|-------------|
| `redlite.open(path, cache_mb?)` | Open database at path |
| `redlite.open_memory()` | Open in-memory database |
| `redlite.version()` | Get library version |
| `db:close()` | Close database |

### String Commands

| Command | Description |
|---------|-------------|
| `db:get(key)` | Get value |
| `db:set(key, value, ttl?)` | Set value with optional TTL |
| `db:setex(key, seconds, value)` | Set with expiration |
| `db:psetex(key, ms, value)` | Set with ms expiration |
| `db:getdel(key)` | Get and delete |
| `db:append(key, value)` | Append to string |
| `db:strlen(key)` | Get string length |
| `db:getrange(key, start, end)` | Get substring |
| `db:setrange(key, offset, value)` | Overwrite substring |
| `db:incr(key)` | Increment by 1 |
| `db:decr(key)` | Decrement by 1 |
| `db:incrby(key, n)` | Increment by n |
| `db:decrby(key, n)` | Decrement by n |
| `db:incrbyfloat(key, f)` | Increment by float |
| `db:mget(key, ...)` | Get multiple keys |
| `db:mset({k1=v1, ...})` | Set multiple keys |

### Key Commands

| Command | Description |
|---------|-------------|
| `db:del(key, ...)` | Delete keys |
| `db:exists(key, ...)` | Check if keys exist |
| `db:type(key)` | Get key type |
| `db:ttl(key)` | Get TTL in seconds |
| `db:pttl(key)` | Get TTL in milliseconds |
| `db:expire(key, seconds)` | Set expiration |
| `db:pexpire(key, ms)` | Set ms expiration |
| `db:expireat(key, timestamp)` | Set expiration at time |
| `db:pexpireat(key, ms_timestamp)` | Set ms expiration at time |
| `db:persist(key)` | Remove expiration |
| `db:rename(key, newkey)` | Rename key |
| `db:renamenx(key, newkey)` | Rename if newkey doesn't exist |
| `db:keys(pattern)` | Find keys matching pattern |
| `db:dbsize()` | Get number of keys |
| `db:flushdb()` | Delete all keys |
| `db:select(db_num)` | Select database |

### Hash Commands

| Command | Description |
|---------|-------------|
| `db:hset(key, field, value)` | Set field |
| `db:hset(key, {f1=v1, ...})` | Set multiple fields |
| `db:hget(key, field)` | Get field |
| `db:hdel(key, field, ...)` | Delete fields |
| `db:hexists(key, field)` | Check field exists |
| `db:hlen(key)` | Get number of fields |
| `db:hkeys(key)` | Get all field names |
| `db:hvals(key)` | Get all values |
| `db:hincrby(key, field, n)` | Increment field |
| `db:hgetall(key)` | Get all fields as table |
| `db:hmget(key, field, ...)` | Get multiple fields |

### List Commands

| Command | Description |
|---------|-------------|
| `db:lpush(key, value, ...)` | Push to head |
| `db:rpush(key, value, ...)` | Push to tail |
| `db:lpop(key, count?)` | Pop from head |
| `db:rpop(key, count?)` | Pop from tail |
| `db:llen(key)` | Get list length |
| `db:lrange(key, start, stop)` | Get range |
| `db:lindex(key, index)` | Get element by index |

### Set Commands

| Command | Description |
|---------|-------------|
| `db:sadd(key, member, ...)` | Add members |
| `db:srem(key, member, ...)` | Remove members |
| `db:smembers(key)` | Get all members |
| `db:sismember(key, member)` | Check membership |
| `db:scard(key)` | Get set size |

### Sorted Set Commands

| Command | Description |
|---------|-------------|
| `db:zadd(key, score, member)` | Add member with score |
| `db:zadd(key, {m1=s1, ...})` | Add multiple members |
| `db:zrem(key, member, ...)` | Remove members |
| `db:zscore(key, member)` | Get score |
| `db:zcard(key)` | Get set size |
| `db:zcount(key, min, max)` | Count in score range |
| `db:zincrby(key, incr, member)` | Increment score |
| `db:zrange(key, start, stop, withscores?)` | Get range by rank |
| `db:zrevrange(key, start, stop, withscores?)` | Get reverse range |

### Server Commands

| Command | Description |
|---------|-------------|
| `db:vacuum()` | Compact database |

## Testing

```bash
# Run unit tests
make test

# Run oracle compatibility tests
make oracle
```

## License

MIT License
