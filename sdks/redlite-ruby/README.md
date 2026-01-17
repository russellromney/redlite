# Redlite Ruby SDK

Redis-compatible embedded database with SQLite durability - Ruby bindings via FFI.

## Installation

Add to your Gemfile:

```ruby
gem 'redlite'
```

Or install directly:

```bash
gem install redlite
```

### Prerequisites

The gem requires the Redlite FFI shared library. Build it from the Rust source:

```bash
cd crates/redlite-ffi
cargo build --release
```

Set `REDLITE_LIB_PATH` to point to the library, or ensure it's in a standard search path.

## Quick Start

```ruby
require 'redlite'

# Block syntax with automatic cleanup
Redlite.open do |db|
  # Strings
  db.set("key", "value")
  db.get("key")              # => "value"
  db.set("counter", "0")
  db.incr("counter")         # => 1

  # With expiration (60 seconds)
  db.set("temp", "data", ex: 60)

  # Hashes
  db.hset("user:1", { name: "Alice", age: "30" })
  db.hget("user:1", "name")  # => "Alice"
  db.hgetall("user:1")       # => {"name"=>"Alice", "age"=>"30"}

  # Lists
  db.rpush("queue", "job1", "job2", "job3")
  db.lpop("queue")           # => "job1"
  db.lrange("queue", 0, -1)  # => ["job2", "job3"]

  # Sets
  db.sadd("tags", "ruby", "redis", "database")
  db.smembers("tags")        # => ["ruby", "redis", "database"]
  db.sismember("tags", "ruby") # => true

  # Sorted Sets
  db.zadd("scores", { "alice" => 100, "bob" => 85, "carol" => 92 })
  db.zrange("scores", 0, -1) # => ["bob", "carol", "alice"]
  db.zrange("scores", 0, -1, with_scores: true)
  # => [["bob", 85.0], ["carol", 92.0], ["alice", 100.0]]
end
```

## Manual Connection Management

```ruby
db = Redlite::Database.new  # In-memory database
# Or with file persistence:
db = Redlite::Database.new("/path/to/database.db")

db.set("key", "value")
db.close  # Don't forget to close!
```

## API Reference

### String Commands

| Method | Description |
|--------|-------------|
| `get(key)` | Get value (returns nil if not found) |
| `set(key, value, ex: nil)` | Set value with optional expiration (seconds) |
| `setex(key, seconds, value)` | Set with expiration |
| `psetex(key, milliseconds, value)` | Set with millisecond expiration |
| `getdel(key)` | Get and delete |
| `append(key, value)` | Append to string, returns new length |
| `strlen(key)` | Get string length |
| `getrange(key, start, stop)` | Get substring |
| `setrange(key, offset, value)` | Overwrite at offset |
| `incr(key)` | Increment by 1 |
| `decr(key)` | Decrement by 1 |
| `incrby(key, amount)` | Increment by amount |
| `decrby(key, amount)` | Decrement by amount |
| `incrbyfloat(key, amount)` | Increment by float |
| `mget(*keys)` | Get multiple keys |
| `mset(mapping)` | Set multiple key-value pairs |

### Key Commands

| Method | Description |
|--------|-------------|
| `del(*keys)` / `delete(*keys)` | Delete keys, returns count |
| `exists(*keys)` | Count existing keys |
| `type(key)` | Get type (string, list, set, zset, hash, none) |
| `ttl(key)` | Get TTL in seconds (-1 no TTL, -2 not found) |
| `pttl(key)` | Get TTL in milliseconds |
| `expire(key, seconds)` | Set expiration |
| `pexpire(key, milliseconds)` | Set millisecond expiration |
| `expireat(key, unix_time)` | Set expiration as timestamp |
| `pexpireat(key, unix_time_ms)` | Set millisecond timestamp expiration |
| `persist(key)` | Remove expiration |
| `rename(key, newkey)` | Rename key |
| `renamenx(key, newkey)` | Rename if new key doesn't exist |
| `keys(pattern)` | Find keys matching pattern |
| `dbsize` | Get total key count |
| `flushdb` | Delete all keys |

### Hash Commands

| Method | Description |
|--------|-------------|
| `hset(key, mapping)` | Set fields (Hash argument) |
| `hget(key, field)` | Get field value |
| `hdel(key, *fields)` | Delete fields |
| `hexists(key, field)` | Check field exists |
| `hlen(key)` | Get field count |
| `hkeys(key)` | Get all field names |
| `hvals(key)` | Get all values |
| `hgetall(key)` | Get all fields and values as Hash |
| `hmget(key, *fields)` | Get multiple fields |
| `hincrby(key, field, amount)` | Increment field by integer |

### List Commands

| Method | Description |
|--------|-------------|
| `lpush(key, *values)` | Prepend values |
| `rpush(key, *values)` | Append values |
| `lpop(key, count=nil)` | Pop from head |
| `rpop(key, count=nil)` | Pop from tail |
| `llen(key)` | Get length |
| `lrange(key, start, stop)` | Get range |
| `lindex(key, index)` | Get element at index |

### Set Commands

| Method | Description |
|--------|-------------|
| `sadd(key, *members)` | Add members |
| `srem(key, *members)` | Remove members |
| `smembers(key)` | Get all members |
| `sismember(key, member)` | Check membership |
| `scard(key)` | Get cardinality |

### Sorted Set Commands

| Method | Description |
|--------|-------------|
| `zadd(key, mapping)` | Add members with scores (Hash: member => score) |
| `zrem(key, *members)` | Remove members |
| `zscore(key, member)` | Get member's score |
| `zcard(key)` | Get cardinality |
| `zcount(key, min, max)` | Count members in score range |
| `zincrby(key, increment, member)` | Increment member's score |
| `zrange(key, start, stop, with_scores: false)` | Get range by index |
| `zrevrange(key, start, stop, with_scores: false)` | Get range in reverse |

### Server Commands

| Method | Description |
|--------|-------------|
| `vacuum` | Compact database, returns bytes freed |
| `version` | Get library version |

## Error Handling

```ruby
begin
  db = Redlite::Database.new
  db.close
  db.get("key")  # Raises after close
rescue Redlite::ConnectionClosedError => e
  puts "Database is closed"
rescue Redlite::Error => e
  puts "Database error: #{e.message}"
end
```

## Development

```bash
# Install dependencies
bundle install

# Run tests (requires FFI library built)
bundle exec rspec

# Run oracle tests
cd ../oracle && ruby runners/ruby_runner.rb -v
```

## License

MIT License - see [LICENSE](../../LICENSE)
