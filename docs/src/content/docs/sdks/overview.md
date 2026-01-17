---
title: SDK Overview
description: Redlite SDKs for multiple programming languages
---

Redlite provides SDKs for 11+ programming languages, all providing the same Redis-compatible API backed by SQLite durability.

## Production SDKs

| Language | Install | Binding Type |
|----------|---------|--------------|
| [Python](/sdks/python) | `pip install redlite` | PyO3 (native) |
| [TypeScript](/sdks/typescript) | `npm install redlite` | NAPI (native) |
| [Go](/sdks/go) | `go get github.com/russellromney/redlite/sdks/redlite-go` | FFI |
| [Ruby](/sdks/ruby) | `gem install redlite` | FFI |
| [Dart/Flutter](/sdks/dart) | See docs | flutter_rust_bridge |
| [C++](/sdks/cpp) | CMake | FFI |
| [Elixir](/sdks/elixir) | `{:redlite, ...}` | NIF |
| [Lua](/sdks/lua) | LuaRocks | FFI |
| [PHP](/sdks/php) | Composer | FFI |
| [Swift](/sdks/swift) | SPM | FFI |
| [.NET](/sdks/dotnet) | NuGet | P/Invoke |

## Experimental SDKs

| Language | Status |
|----------|--------|
| [WASM](/sdks/wasm) | Browser support |
| [Zig](/sdks/zig) | Experimental |
| [Java/Kotlin](/sdks/java) | Stub |

## Esoteric Languages

For the adventurous: [Brainf*ck, Chef, COW, LOLCODE, Piet, SPL, Whitespace](/sdks/esoteric)

## API Consistency

All SDKs implement the same core API:

```
# Strings
SET, GET, INCR, DECR, APPEND, STRLEN, MGET, MSET...

# Keys
DEL, EXISTS, TYPE, TTL, EXPIRE, KEYS, SCAN...

# Hashes
HSET, HGET, HDEL, HGETALL, HMGET, HMSET, HINCRBY...

# Lists
LPUSH, RPUSH, LPOP, RPOP, LLEN, LRANGE, LINDEX...

# Sets
SADD, SREM, SMEMBERS, SISMEMBER, SCARD, SINTER...

# Sorted Sets
ZADD, ZREM, ZSCORE, ZRANK, ZRANGE, ZREVRANGE...

# Redlite Extensions
FTS (full-text search), VECTOR (similarity search), GEO (geospatial)
```

## Oracle Tests

All production SDKs pass the [oracle test suite](https://github.com/russellromney/redlite/tree/main/sdks/oracle) - 137 cross-language compatibility tests ensuring consistent behavior.
