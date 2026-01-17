# Redlite

Redis-compatible embedded database with SQLite durability for Elixir.

## Installation

Add `redlite` to your list of dependencies in `mix.exs`:

```elixir
def deps do
  [
    {:redlite, "~> 0.1.0"}
  ]
end
```

## Usage

### Direct API

```elixir
# Open an in-memory database
{:ok, db} = Redlite.open(":memory:")

# Or open a file-based database
{:ok, db} = Redlite.open("/path/to/database.db")

# Basic operations
:ok = Redlite.set(db, "key", "value")
{:ok, "value"} = Redlite.get(db, "key")

# With TTL (in seconds)
:ok = Redlite.set(db, "temp", "data", ttl: 3600)

# Numeric operations
{:ok, 1} = Redlite.incr(db, "counter")
{:ok, 6} = Redlite.incrby(db, "counter", 5)

# Multi-key operations
:ok = Redlite.mset(db, %{"k1" => "v1", "k2" => "v2"})
{:ok, ["v1", "v2"]} = Redlite.mget(db, ["k1", "k2"])
```

### GenServer Wrapper

For process isolation and named access:

```elixir
# Start as a named process
{:ok, _pid} = Redlite.start_link(path: ":memory:", name: MyCache)

# Use the name for operations
:ok = Redlite.set(MyCache, "key", "value")
{:ok, "value"} = Redlite.get(MyCache, "key")

# Add to supervision tree
children = [
  {Redlite, path: "/var/data/cache.db", name: MyApp.Cache}
]
```

## Supported Commands

### Strings
- `GET`, `SET`, `MGET`, `MSET`
- `INCR`, `DECR`, `INCRBY`, `DECRBY`, `INCRBYFLOAT`
- `APPEND`, `STRLEN`, `GETRANGE`, `SETRANGE`
- `SETEX`, `PSETEX`, `GETDEL`

### Keys
- `DEL`, `EXISTS`, `TYPE`
- `EXPIRE`, `PEXPIRE`, `EXPIREAT`, `PEXPIREAT`
- `TTL`, `PTTL`, `PERSIST`
- `RENAME`, `RENAMENX`
- `KEYS`, `SCAN`
- `DBSIZE`, `FLUSHDB`, `SELECT`

### Hashes
- `HSET`, `HGET`, `HDEL`, `HEXISTS`
- `HLEN`, `HKEYS`, `HVALS`
- `HGETALL`, `HMGET`
- `HINCRBY`, `HSCAN`

### Lists
- `LPUSH`, `RPUSH`, `LPOP`, `RPOP`
- `LLEN`, `LRANGE`, `LINDEX`

### Sets
- `SADD`, `SREM`, `SMEMBERS`
- `SISMEMBER`, `SCARD`, `SSCAN`

### Sorted Sets
- `ZADD`, `ZREM`, `ZSCORE`
- `ZRANGE`, `ZREVRANGE`
- `ZCARD`, `ZCOUNT`, `ZINCRBY`
- `ZSCAN`

### Server
- `VACUUM` - Compact the SQLite database

## Building

```bash
# Install dependencies and compile
mix deps.get
mix compile

# Run tests
mix test
```

## License

Apache-2.0
