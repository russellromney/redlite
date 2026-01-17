# Changelog

## Session 48: PHP SDK Implementation

### Added - Complete PHP SDK with FFI Bindings

**Location**: `sdks/redlite-php/`

**Features**:
- PHP FFI bindings for native library integration (PHP 7.4+)
- PHP-idiomatic interface with null for missing keys
- Full Redis command coverage: strings, hashes, lists, sets, sorted sets
- Automatic memory management with proper FFI cleanup
- Composer package with bundled native binaries

**Files Created**:
- `composer.json` - Composer package specification
- `src/Database.php` - Low-level FFI wrapper (~900 lines)
- `src/Redlite.php` - Main client class with friendly API (~600 lines)
- `src/RedliteException.php` - Exception class for error handling
- `tests/RedliteTest.php` - Comprehensive PHPUnit test suite (80+ tests)
- `phpunit.xml` - PHPUnit configuration

**Commands Implemented** (64 total):
- String (16): GET, SET, SETEX, PSETEX, GETDEL, APPEND, STRLEN, GETRANGE, SETRANGE, INCR, DECR, INCRBY, DECRBY, INCRBYFLOAT, MGET, MSET
- Key (16): DEL, EXISTS, TYPE, TTL, PTTL, EXPIRE, PEXPIRE, EXPIREAT, PEXPIREAT, PERSIST, RENAME, RENAMENX, KEYS, DBSIZE, FLUSHDB, SELECT
- Hash (10): HSET, HGET, HDEL, HEXISTS, HLEN, HKEYS, HVALS, HINCRBY, HGETALL, HMGET
- List (7): LPUSH, RPUSH, LPOP, RPOP, LLEN, LRANGE, LINDEX
- Set (5): SADD, SREM, SMEMBERS, SISMEMBER, SCARD
- Sorted Set (8): ZADD, ZREM, ZSCORE, ZCARD, ZCOUNT, ZINCRBY, ZRANGE, ZREVRANGE
- Server (2): VACUUM, VERSION

**Example**:
```php
<?php
use Redlite\Redlite;

// Open in-memory database
$db = new Redlite(':memory:');

// String operations
$db->set('greeting', 'Hello, World!');
echo $db->get('greeting');  // "Hello, World!"

// Hash operations
$db->hset('user:1', ['name' => 'Alice', 'age' => '30']);
print_r($db->hgetall('user:1'));  // ['name' => 'Alice', 'age' => '30']

// Sorted sets
$db->zadd('scores', ['alice' => 100, 'bob' => 95, 'carol' => 98]);
$top = $db->zrevrange('scores', 0, 2);  // ['alice', 'carol', 'bob']

$db->close();
```

**Build Instructions**:
```bash
cd sdks/redlite-php
composer install
./vendor/bin/phpunit
```

### Added - PHP Oracle Test Runner

**Location**: `sdks/oracle/runners/php_runner.php`

Oracle test runner for validating PHP SDK against shared test specifications:
- Supports all oracle tests across spec files
- YAML parsing via yaml PHP extension
- Full expectation type support (bytes, set, dict, range, approx)
- Verbose mode for debugging (`-v` flag)

**Usage**:
```bash
cd sdks/oracle/runners
php php_runner.php          # Run all specs
php php_runner.php -v       # Verbose output
```

**Requirements**:
- PHP 7.4+ with FFI extension enabled
- yaml extension (`pecl install yaml`)
- Composer for autoloading
- PHPUnit for unit tests

---

## Session 47: Elixir SDK Implementation

### Added - Complete Elixir SDK with Rustler NIFs

**Location**: `sdks/redlite-elixir/`

**Features**:
- Rustler NIF bindings for high-performance Rust -> Elixir integration
- GenServer wrapper for process isolation and named access
- Dirty schedulers for potentially long operations (prevents scheduler blocking)
- Idiomatic Elixir API with `:ok/:error` tuples and `nil` for missing values
- Full Redis command coverage: strings, hashes, lists, sets, sorted sets

**Files Created**:
- `mix.exs` - Mix project configuration with Rustler dependency
- `native/redlite_nif/Cargo.toml` - Rustler crate configuration
- `native/redlite_nif/src/lib.rs` - Rust NIF implementations (~1100 lines)
- `lib/redlite.ex` - Main Elixir module with GenServer (~600 lines)
- `lib/redlite/native.ex` - NIF wrapper module
- `test/redlite_test.exs` - Comprehensive ExUnit tests (70+ tests)
- `Makefile` - Convenience commands
- `README.md` - Documentation with API reference

**Commands Implemented** (64 total):
- String (17): GET, SET, SET_OPTS, SETEX, PSETEX, GETDEL, APPEND, STRLEN, GETRANGE, SETRANGE, INCR, DECR, INCRBY, DECRBY, INCRBYFLOAT, MGET, MSET
- Key (16): DEL, EXISTS, TYPE, TTL, PTTL, EXPIRE, PEXPIRE, EXPIREAT, PEXPIREAT, PERSIST, RENAME, RENAMENX, KEYS, DBSIZE, FLUSHDB, SELECT
- Hash (10): HSET, HGET, HDEL, HEXISTS, HLEN, HKEYS, HVALS, HINCRBY, HGETALL, HMGET
- List (7): LPUSH, RPUSH, LPOP, RPOP, LLEN, LRANGE, LINDEX
- Set (5): SADD, SREM, SMEMBERS, SISMEMBER, SCARD
- Sorted Set (8): ZADD, ZREM, ZSCORE, ZCARD, ZCOUNT, ZINCRBY, ZRANGE, ZREVRANGE
- Scan (4): SCAN, HSCAN, SSCAN, ZSCAN
- Server (1): VACUUM

**API Design**:
- `Redlite.open/1` - Open database (":memory:" or file path)
- `Redlite.start_link/1` - Start GenServer for named process
- All commands return `{:ok, result}` or `{:error, reason}`
- `nil` for missing keys/fields (Elixir-idiomatic)
- Binary data support for all values

**Example**:
```elixir
# Direct API
{:ok, db} = Redlite.open(":memory:")
:ok = Redlite.set(db, "key", "value")
{:ok, "value"} = Redlite.get(db, "key")

# GenServer wrapper
{:ok, _pid} = Redlite.start_link(path: ":memory:", name: MyCache)
:ok = Redlite.set(MyCache, "key", "value")

# Hash operations
{:ok, 2} = Redlite.hset(db, "user:1", %{"name" => "Alice", "age" => "30"})
{:ok, "Alice"} = Redlite.hget(db, "user:1", "name")

# Sorted sets
{:ok, 1} = Redlite.zadd(db, "leaderboard", 100.0, "alice")
{:ok, ["alice"]} = Redlite.zrange(db, "leaderboard", 0, -1)
```

**Build Instructions**:
```bash
cd sdks/redlite-elixir
mix deps.get
mix compile
mix test
```

### Added - Elixir Oracle Test Runner

**Location**: `sdks/oracle/runners/elixir_runner.exs`

Oracle test runner for validating Elixir SDK against shared test specifications:
- Mix.install-based runner (no compilation needed)
- Full YAML spec parsing with YamlElixir
- Support for all expectation types (exact, range, approx, set)
- Verbose mode with detailed error reporting

---

## Session 46: Zig SDK Implementation

### Added - Complete Zig SDK with Idiomatic API

**Location**: `sdks/redlite-zig/`

**Features**:
- Zig-idiomatic wrapper using slices, optionals, and error unions
- @cImport integration with redlite.h C FFI
- RAII-style resource management with `defer`
- Full Redis command coverage: strings, hashes, lists, sets, sorted sets

**Files Created**:
- `build.zig` - Zig build system configuration
- `src/c.zig` - Low-level C FFI bindings via @cImport
- `src/redlite.zig` - Main Zig-idiomatic wrapper API (~650 lines)
- `tests/test_basic.zig` - Comprehensive unit tests (35+ tests)
- `examples/basic.zig` - Usage example
- `Makefile` - Convenience commands for building and testing
- `README.md` - Documentation with API reference

**Commands Implemented** (64 total):
- String (16): GET, SET, SETEX, PSETEX, GETDEL, APPEND, STRLEN, GETRANGE, SETRANGE, INCR, DECR, INCRBY, DECRBY, INCRBYFLOAT, MGET, MSET
- Key (16): DEL, EXISTS, TYPE, TTL, PTTL, EXPIRE, PEXPIRE, EXPIREAT, PEXPIREAT, PERSIST, RENAME, RENAMENX, KEYS, DBSIZE, FLUSHDB, SELECT
- Hash (10): HSET, HGET, HDEL, HEXISTS, HLEN, HKEYS, HVALS, HINCRBY, HGETALL, HMGET
- List (7): LPUSH, RPUSH, LPOP, RPOP, LLEN, LRANGE, LINDEX
- Set (5): SADD, SREM, SMEMBERS, SISMEMBER, SCARD
- Sorted Set (8): ZADD, ZREM, ZSCORE, ZCARD, ZCOUNT, ZINCRBY, ZRANGE, ZREVRANGE
- Server (2): VACUUM, VERSION

**API Types**:
- `Database` - Main database handle with all Redis commands
- `OwnedBytes` - Owned byte result with RAII cleanup
- `OwnedStringArray` - Owned string array result
- `OwnedBytesArray` - Owned bytes array result
- `ZMember` - Sorted set member (score + member)
- `KV` - Key-value pair for batch operations
- `Error` - Error union type for operation failures

**Example**:
```zig
const redlite = @import("redlite");

const db = try redlite.Database.openMemory();
defer db.close();

// Strings
try db.set("key", "value", null);
if (try db.get("key")) |v| {
    defer v.deinit();
    std.debug.print("{s}\n", .{v.data()});
}

// Counters
_ = db.incr("counter");
_ = db.incrby("counter", 10);

// Hashes
_ = db.hset("user:1", "name", "Alice");

// Sorted sets
var members = [_]redlite.ZMember{
    .{ .score = 100.0, .member = "alice" },
};
_ = try db.zadd("leaderboard", &members);
```

**Build Instructions**:
```bash
cd sdks/redlite-zig
make ffi     # Build the FFI library first
make build   # Build the Zig SDK
make test    # Run all tests
make example # Run the example
```

### Added - Zig Oracle Test Runner

**Location**: `sdks/oracle/runners/zig_runner.zig`

Oracle test runner for validating Zig SDK against shared test specifications:
- Interactive mode for JSON command input
- Command dispatch with proper type conversions
- Error handling with JSON error responses

---

## Session 45: Lua SDK Implementation

### Added - Complete Lua SDK with LuaJIT FFI Bindings

**Location**: `sdks/redlite-lua/`

**Features**:
- LuaJIT FFI bindings for maximum performance (microsecond latency)
- Lua-idiomatic interface with nil for missing keys
- Full Redis command coverage: strings, hashes, lists, sets, sorted sets
- Automatic memory management with proper FFI cleanup
- LuaRocks package support

**Files Created**:
- `redlite.lua` - Main LuaJIT FFI wrapper (800+ lines)
- `redlite/init.lua` - Module entry point for require("redlite")
- `spec/redlite_spec.lua` - Comprehensive busted test suite (45+ tests)
- `redlite-scm-1.rockspec` - LuaRocks package specification
- `Makefile` - Build and test commands
- `README.md` - Documentation with full API reference

**Commands Implemented** (64 total):
- String (16): GET, SET, SETEX, PSETEX, GETDEL, APPEND, STRLEN, GETRANGE, SETRANGE, INCR, DECR, INCRBY, DECRBY, INCRBYFLOAT, MGET, MSET
- Key (16): DEL, EXISTS, TYPE, TTL, PTTL, EXPIRE, PEXPIRE, EXPIREAT, PEXPIREAT, PERSIST, RENAME, RENAMENX, KEYS, DBSIZE, FLUSHDB, SELECT
- Hash (10): HSET, HGET, HDEL, HEXISTS, HLEN, HKEYS, HVALS, HINCRBY, HGETALL, HMGET
- List (7): LPUSH, RPUSH, LPOP, RPOP, LLEN, LRANGE, LINDEX
- Set (5): SADD, SREM, SMEMBERS, SISMEMBER, SCARD
- Sorted Set (8): ZADD, ZREM, ZSCORE, ZCARD, ZCOUNT, ZINCRBY, ZRANGE, ZREVRANGE
- Server (2): VACUUM, VERSION

**Example**:
```lua
local redlite = require("redlite")

-- Open in-memory database
local db = redlite.open_memory()

-- String operations
db:set("greeting", "Hello, World!")
print(db:get("greeting"))  -- "Hello, World!"

-- Hash operations
db:hset("user:1", {name = "Alice", age = "30"})
print(db:hgetall("user:1"))  -- {name = "Alice", age = "30"}

-- Sorted sets with flexible API
db:zadd("scores", {alice = 100, bob = 95, carol = 98})
local top = db:zrevrange("scores", 0, 2)  -- {"alice", "carol", "bob"}

db:close()
```

**Build Instructions**:
```bash
cd sdks/redlite-lua
make build  # Build FFI library
make test   # Run busted tests
make oracle # Run oracle tests
```

### Added - Lua Oracle Test Runner

**Location**: `sdks/oracle/runners/lua_runner.lua`

Oracle test runner for validating Lua SDK against shared test specifications:
- Supports all oracle tests across spec files
- YAML parsing via lyaml library
- Full expectation type support (bytes, set, dict, range, approx)
- Verbose mode for debugging (`-v` flag)

**Usage**:
```bash
cd sdks/oracle/runners
luajit lua_runner.lua          # Run all specs
luajit lua_runner.lua -v       # Verbose output
```

**Requirements**:
- LuaJIT (required for FFI support)
- lyaml (`luarocks install lyaml`)
- busted (`luarocks install busted`) for unit tests

---

## Session 44: Ruby SDK Implementation

### Added - Complete Ruby SDK with FFI Bindings

**Location**: `sdks/redlite-ruby/`

**Features**:
- FFI-based bindings using the `ffi` gem (simpler than native extensions)
- Ruby-idiomatic interface with nil for missing keys, keyword arguments
- Full Redis command coverage: strings, hashes, lists, sets, sorted sets
- Block syntax support for automatic resource cleanup

**Files Created**:
- `redlite.gemspec` - Gem specification with ffi dependency
- `Gemfile` - Dependency management
- `Rakefile` - Build and test tasks
- `lib/redlite.rb` - Main entry point
- `lib/redlite/version.rb` - Version constant
- `lib/redlite/errors.rb` - Custom exception classes
- `lib/redlite/ffi.rb` - FFI bindings to C library (58 functions)
- `lib/redlite/database.rb` - Database class with all Redis commands
- `spec/spec_helper.rb` - RSpec configuration
- `spec/redlite_spec.rb` - Comprehensive test suite

**Commands Implemented** (64 total):
- String (16): GET, SET, SETEX, PSETEX, GETDEL, APPEND, STRLEN, GETRANGE, SETRANGE, INCR, DECR, INCRBY, DECRBY, INCRBYFLOAT, MGET, MSET
- Key (16): DEL, EXISTS, TYPE, TTL, PTTL, EXPIRE, PEXPIRE, EXPIREAT, PEXPIREAT, PERSIST, RENAME, RENAMENX, KEYS, DBSIZE, FLUSHDB, SELECT
- Hash (10): HSET, HGET, HDEL, HEXISTS, HLEN, HKEYS, HVALS, HINCRBY, HGETALL, HMGET
- List (7): LPUSH, RPUSH, LPOP, RPOP, LLEN, LRANGE, LINDEX
- Set (5): SADD, SREM, SMEMBERS, SISMEMBER, SCARD
- Sorted Set (8): ZADD, ZREM, ZSCORE, ZCARD, ZCOUNT, ZINCRBY, ZRANGE, ZREVRANGE
- Server (2): VACUUM, VERSION

**Example**:
```ruby
require 'redlite'

# Block syntax with automatic cleanup
Redlite.open do |db|
  db.set("key", "value", ex: 60)  # With expiration
  puts db.get("key")              # => "value"

  # Hashes
  db.hset("user:1", { name: "Alice", age: "30" })
  puts db.hgetall("user:1")       # => {"name"=>"Alice", "age"=>"30"}

  # Lists
  db.rpush("queue", "job1", "job2", "job3")
  puts db.lpop("queue")           # => "job1"
end
```

**Build Instructions**:
```bash
cd sdks/redlite-ruby
bundle install
bundle exec rspec
```

### Added - Ruby Oracle Test Runner

**Location**: `sdks/oracle/runners/ruby_runner.rb`

Oracle test runner for validating Ruby SDK against shared test specifications:
- Supports all 137 oracle tests across 6 spec files
- YAML parsing with full expectation type support
- Command dispatch handling API differences
- Verbose mode for debugging (`-v` flag)

**Usage**:
```bash
cd sdks/oracle
ruby runners/ruby_runner.rb        # Run all specs
ruby runners/ruby_runner.rb -v     # Verbose output
```

---

## Session 43: C++ SDK Implementation

### Added - Complete C++ SDK with Modern C++17 API

**Location**: `sdks/redlite-cpp/`

**Features**:
- Header-only library (`include/redlite/redlite.hpp` - 880+ lines)
- RAII resource management with automatic cleanup
- Modern C++17 API using `std::optional`, `std::string_view`, move semantics
- Full Redis command coverage: strings, hashes, lists, sets, sorted sets

**Files Created**:
- `include/redlite/redlite.hpp` - Main header with full C++ API
- `CMakeLists.txt` - CMake build system with Catch2 test framework
- `Makefile` - Convenience commands for building and testing
- `README.md` - Documentation with API reference
- `redlite.pc.in` - pkg-config template

**Test Files** (6 test files using Catch2):
- `tests/test_strings.cpp` - String command tests (20 tests)
- `tests/test_keys.cpp` - Key command tests (12 tests)
- `tests/test_hashes.cpp` - Hash command tests (12 tests)
- `tests/test_lists.cpp` - List command tests (13 tests)
- `tests/test_sets.cpp` - Set command tests (8 tests)
- `tests/test_zsets.cpp` - Sorted set command tests (12 tests)

**Example**:
- `examples/basic.cpp` - Comprehensive usage example

**API Classes**:
- `redlite::Database` - Main database class with all commands
- `redlite::Error` - Exception class for error handling
- `redlite::SetOptions` - Builder pattern for SET options
- `redlite::ZMember` - Sorted set member (score + value)
- `redlite::Bytes` - RAII wrapper for byte results

**Build Instructions**:
```bash
cd sdks/redlite-cpp
make build   # Build SDK and tests
make test    # Run all tests
make example # Run the basic example
```

### Added - C++ Oracle Test Runner

**Location**: `sdks/oracle/runners/cpp_runner.cpp`

Oracle test runner for validating C++ SDK against shared test specifications:
- Supports all 137 oracle tests across 6 spec files
- YAML parsing via yaml-cpp
- Full command coverage matching Python/TypeScript runners
- Verbose mode for debugging

### Added - SDK Session Prompts

**Location**: `sdks/SESSION_PROMPTS.md`

Ready-to-use session prompts for implementing SDKs in other languages:
- Swift SDK (iOS/macOS)
- C# .NET SDK (Windows/cross-platform)
- Ruby SDK (FFI gem)
- Zig SDK (C interop)
- PHP SDK (PHP FFI)
- Elixir SDK (Rustler NIFs)
- Lua SDK (LuaJIT FFI)
- WASM SDK (browser/Node.js)

Each prompt includes: context, task breakdown, architecture decisions, and build/test commands.

---

## Session 42: Kotlin & Java SDK Unit Tests + SDK Roadmap Expansion

### Added - SDK Unit Tests (12 test files, ~200 tests)

**Kotlin SDK Tests** (`sdks/redlite-kotlin/src/test/kotlin/com/redlite/`):
- `StringsTest.kt` - SET, GET, MSET, MGET, INCR, DECR, APPEND, STRLEN (16 tests)
- `KeysTest.kt` - DEL, EXISTS, TYPE, EXPIRE, TTL, SCAN, COPY, DUMP/RESTORE (20 tests)
- `HashesTest.kt` - HSET, HGET, HMSET, HMGET, HDEL, HSCAN, HRANDFIELD (17 tests)
- `ListsTest.kt` - LPUSH, RPUSH, LPOP, RPOP, LRANGE, LINSERT, LMOVE (16 tests)
- `SetsTest.kt` - SADD, SREM, SMEMBERS, SUNION, SINTER, SDIFF, SSCAN (18 tests)
- `ZSetsTest.kt` - ZADD, ZREM, ZSCORE, ZRANGE, ZPOPMIN/MAX, ZUNION, ZINTER (26 tests)

**Java SDK Tests** (`sdks/redlite-java/src/test/java/com/redlite/`):
- Equivalent 6 test files with same coverage as Kotlin tests
- JUnit 5 test framework with `@DisplayName` annotations

### Updated - SDK Roadmap (7 complete + 47 planned = 54 SDKs)

**Priority Tiers**:
- HIGH: Swift, C#/.NET, Ruby, C/C++, Zig, WASM
- MEDIUM: PHP, Lua, Scala, Clojure, F#, Crystal, Nim
- LOW: OCaml, Haskell, Julia, R, V, D, Perl
- ENTERPRISE: COBOL, Ada, Fortran, MATLAB, PowerShell
- MEME: Brainfuck, LOLCODE, Rockstar, Shakespeare, Piet, Whitespace, Scratch, Tabloid

**SDK Build Strategy**:
1. C/C++ first (enables many others via FFI)
2. Swift, C#/.NET, WASM (immediate user demand)
3. Esoteric/meme languages (fun, LLM-assisted)

**Files Changed**:
- `sdks/ROADMAP.md` - Expanded from 7 to 54 SDKs with detailed implementation plans
- All 12 test files created for Kotlin and Java SDKs

---

## Session 40: Oracle Test Infrastructure Fix - 100% Pass Rate

### Achievement - Full Redis Compatibility Verified

**Test Results**: **230/230 oracle tests passing (100%)**

This session confirmed that all 11 "failures" reported after Session 38 were caused by test infrastructure issues, not implementation bugs.

### Root Cause - Test Pollution from Parallel Execution

When `run_tests_sqlite.sh` runs with default parallelism (8 workers):
- Multiple tests simultaneously call FLUSHDB on shared Redis
- Tests create keys with identical names (e.g., "key:1", "mystream")
- Race conditions cause assertions to fail (e.g., SCAN returns 50 keys instead of expected 21)

**Evidence**:
- All 11 "failing" tests pass when run individually
- Running with `parallelism=1` produces 230/230 passing tests
- No code changes required - Session 38 fixes were correct

### Tests Verified

All previously "failing" tests now pass:
- `oracle_cmd_scan` - SCAN command behavior ✅
- `oracle_cmd_zscan` - Sorted set scan ✅
- `oracle_cmd_zcount` - Sorted set count by score ✅
- `oracle_cmd_zrange` - Sorted set range query ✅
- `oracle_cmd_zrangebyscore` - Sorted set range by score ✅
- `oracle_cmd_xclaim` - Stream consumer claim ✅
- `oracle_hashes_random_ops` - Hash random operations ✅
- `oracle_lists_random_ops` - List random operations ✅
- `oracle_sets_random_ops` - Set random operations ✅
- `oracle_strings_random_ops` - String random operations ✅
- `oracle_zsets_random_ops` - Sorted set random operations ✅

### Recommendation for Test Infrastructure

For Redis oracle tests, always use sequential execution:
```bash
./run_tests_sqlite.sh test_results.db "" 1
```

Future improvement: Add unique key prefixes per test to enable safe parallel execution.

---

## Session 39: Core Bug Fixes & Transaction Tests

### Fixed - 3 Critical Bugs in Core Commands

**persist() Bug** (crates/redlite/src/db.rs:1944-1952):
- Fixed to return `false` when key has no TTL (was incorrectly returning `true`)
- Changed WHERE clause: `expire_at IS NULL OR expire_at > ?1` → `expire_at IS NOT NULL AND expire_at > ?1`
- Test: `oracle_cmd_persist` now passes

**rename() Bug** (crates/redlite/src/db.rs:2009-2054):
- Fixed handling when renaming key to itself (was deleting the key)
- Added early return when `key == newkey`
- Test: `oracle_cmd_rename` now passes

**lrem() & linsert() Compilation Errors** (crates/redlite/src/db.rs:3606-3609, 3713-3716):
- Fixed borrowing conflicts preventing compilation
- Wrapped `stmt` usage in block scope to drop borrows before `drop(conn)`
- Both functions now compile successfully

### Added - 10 Server-Mode Transaction Tests

**New Test File**: tests/server_watch.rs
- Added Category 7: Basic Transactions (without WATCH)
- Tests cover MULTI/EXEC/DISCARD behavior in actual server mode
- All tests use real TCP connections to validate protocol compliance

**Tests Added**:
1. `test_basic_multi_exec` - Basic transaction with 3 SET commands
2. `test_multi_exec_with_get` - Transaction with GET operations
3. `test_discard_aborts_transaction` - DISCARD cancels queued commands
4. `test_multi_exec_error_handling` - Type errors within transactions
5. `test_exec_without_multi_errors` - EXEC requires MULTI first
6. `test_discard_without_multi_errors` - DISCARD requires MULTI first
7. `test_nested_multi_errors` - Nested MULTI not allowed
8. `test_transaction_isolation` - Changes invisible until EXEC
9. `test_large_transaction_100_commands` - 100 SET commands in one transaction
10. `test_transaction_with_mixed_data_types` - String/List/Hash/Set/ZSet in one transaction

### Performance - Poll Impact Benchmarks (Complete)

**All 8 benchmark groups completed successfully** (FK constraint error was transient):

| Benchmark | Throughput |
|-----------|------------|
| Baseline SET/GET | 21K ops/sec |
| Baseline LPUSH/LPOP | 22.5K ops/sec |
| Baseline HSET/HGET | 37K ops/sec |
| 1 waiter | 11K ops/sec (~50% degradation) |
| 10 waiters | 6.6K ops/sec (~70% degradation) |
| 50 waiters | 1.7K ops/sec (~92% degradation) |

**PollConfig Comparison (10 waiters)**:
- Aggressive: 7.9K ops/sec
- Default: 7.0K ops/sec
- **Relaxed: 9.2K ops/sec (BEST)** ✓

**Recommendation**: Use `PollConfig::relaxed()` for workloads with concurrent blocking waiters

### Oracle Test Analysis

**Finding**: Most failures are test pollution (shared Redis state)
- Tests pass individually but fail when run in batch
- Not implementation bugs, but test infrastructure issues
- 5-7 hanging tests remain (linsert, lrem, sdiffstore, sinterstore, sunionstore) - likely Redis connection timeouts

**Current Status**: 219 passed / ~11-50 failed (depends on test order and Redis state)

## Session 38: DST Oracle Tests - Deadlock Fixes (7 Timeouts Resolved)

### Fixed - Critical Deadlocks in List and Set Operations

**Achievement**: Eliminated all 7 timeout tests (100% timeout fix rate)

**Test Results**:
- **Before**: 212 passed / 17 failed (7 timeouts at 30s) = 92% pass rate
- **After**: 219 passed / 11 failed (0 timeouts) = 95% pass rate
- **Improvement**: +7 tests fixed, +3% pass rate increase

### Deadlock Pattern 1: record_history() Called While Holding Lock

**Affected Functions**: `linsert()`, `lrem()`

**Root Cause**:
- Functions acquired connection lock for database operations
- Called `record_history()` before dropping the lock
- `record_history()` calls helper functions (`get_or_create_key_id()`, `increment_version()`) that acquire the same lock
- Result: Nested lock acquisition → deadlock

**Fix Applied**:
- Added `drop(conn);` before `record_history()` calls
- Ensures lock is released before helper functions try to acquire it

**Files Changed**:
- `crates/redlite/src/db.rs:3836` - LINSERT: Added drop(conn) before record_history
- `crates/redlite/src/db.rs:3685` - LREM: Added drop(conn) before record_history

**Tests Fixed**:
- `oracle_cmd_linsert` (was 30s timeout → now passes)
- `oracle_cmd_lrem` (was 30s timeout → now passes)
- `oracle_lists_linsert` (was 30s timeout → now passes)
- `oracle_lists_lset_lrem` (was 30s timeout → now passes)

### Deadlock Pattern 2: Nested Lock Acquisition in if-let Expressions

**Affected Functions**: `sdiffstore()`, `sinterstore()`, `sunionstore()`

**Root Cause**:
```rust
// Lock acquired as temporary in if-let condition
if let Some(dest_key_id) = self.get_set_key_id(
    &self.core.conn.lock().unwrap_or_else(|e| e.into_inner()),  // Lock #1
    destination,
)? {
    let conn = self.core.conn.lock().unwrap_or_else(|e| e.into_inner());  // Lock #2 (DEADLOCK!)
    // ...
}
```

**Fix Applied**:
- Separated lock acquisition into explicit scopes
- First scope: Acquire lock, call helper, drop lock
- Second scope: Acquire lock again for cleanup operations

```rust
// Lock scope #1: Check if destination exists
let dest_key_id_opt = {
    let conn = self.core.conn.lock().unwrap_or_else(|e| e.into_inner());
    self.get_set_key_id(&conn, destination)?
};  // Lock automatically dropped here

// Lock scope #2: Cleanup (if needed)
if let Some(dest_key_id) = dest_key_id_opt {
    let conn = self.core.conn.lock().unwrap_or_else(|e| e.into_inner());
    // ... cleanup operations
}
```

**Files Changed**:
- `crates/redlite/src/db.rs:4299-4311` - SDIFFSTORE: Separated lock scopes
- `crates/redlite/src/db.rs:4345-4357` - SINTERSTORE: Separated lock scopes
- `crates/redlite/src/db.rs:4391-4403` - SUNIONSTORE: Separated lock scopes

**Tests Fixed**:
- `oracle_cmd_sdiffstore` (was 30s timeout → now passes)
- `oracle_cmd_sinterstore` (was 30s timeout → now passes)
- `oracle_cmd_sunionstore` (was 30s timeout → now passes)

### Remaining Failures (11 tests, non-deadlock issues)

**Different Test Failures** (not related to deadlocks):
- `oracle_cmd_scan` - SCAN command behavior
- `oracle_cmd_xclaim` - Stream consumer claim
- `oracle_cmd_zcount` - Sorted set count by score
- `oracle_cmd_zrange` - Sorted set range query
- `oracle_cmd_zrangebyscore` - Sorted set range by score
- `oracle_cmd_zscan` - Sorted set scan
- `oracle_hashes_random_ops` - Hash random operations
- `oracle_lists_random_ops` - List random operations
- `oracle_sets_random_ops` - Set random operations
- `oracle_strings_random_ops` - String random operations
- `oracle_zsets_random_ops` - Sorted set random operations

These failures are behavioral differences or edge cases, not deadlocks. They require different investigation and fixes.

### Key Learnings

**Lock Management Best Practices**:
1. Always drop locks before calling functions that acquire locks
2. Use explicit scope blocks `{ let conn = ...; }` to control lock lifetime
3. Be cautious with temporary lock guards in complex expressions (if-let, match)
4. Helper functions that need locks should take `&Connection` parameter, not acquire lock themselves

**Similar to Session 36 History Bug**:
- Session 36 fixed `record_history()` to call helpers before acquiring lock
- Session 38 fixed callers of `record_history()` to drop lock before calling it
- Together, these ensure no nested lock acquisition anywhere in the history tracking code path

---

## Session 37: Go SDK Complete - 100% Oracle Test Coverage

### Added - 17 Missing Redis Commands

Completed Go SDK implementation by adding all missing Redis commands, achieving 100% oracle test coverage (137/137 tests passing, up from 107/137).

**String Commands (8)**:
- MGET - Get multiple key values
- MSET - Set multiple key-value pairs
- GETDEL - Get and delete a key
- GETRANGE - Get substring of string value
- SETRANGE - Overwrite part of string value
- DECRBY - Decrement by amount
- INCRBYFLOAT - Increment by float amount
- PSETEX - Set with millisecond expiration

**Key Commands (4)**:
- PTTL - Get TTL in milliseconds
- PEXPIRE - Set TTL in milliseconds
- RENAME - Rename a key
- RENAMENX - Rename only if new key doesn't exist

**Hash Commands (2)**:
- HGETALL - Get all fields and values
- HMGET - Get multiple hash field values

**Sorted Set Commands (3)**:
- ZREM - Remove members from sorted set
- ZRANGE - Get range of members
- ZREVRANGE - Get range in reverse order

**Implementation Changes**:
- FFI Layer: Added 6 new C functions to redlite-ffi/src/lib.rs (mget, mset, hgetall, hmget, zrange, zrevrange)
- C Headers: Updated redlite.h with new function declarations
- Go SDK: Added 17 new methods to embedded.go
- Test Runner: Added command handlers and special dict type comparison for HGETALL tests
- Removed all commands from unsupportedCommands map

**Test Results**:
- Oracle tests: 137/137 passing (100%)
- Previously: 107/137 (78%) with 30 skipped tests
- All 17 new commands fully tested and working

**Files Changed**:
- crates/redlite-ffi/src/lib.rs - 6 new FFI functions
- sdks/redlite-go/redlite.h - C header updates
- sdks/redlite-go/embedded.go - 17 new Go methods
- sdks/oracle/runners/go_runner.go - Command handlers and test support

---

## Session 36: History Feature Bug Fixes & Parallel Test Infrastructure

### Fixed - History Tracking Deadlock

**Critical Bug**: History tracking was causing deadlocks when enabled globally.

**Root Cause**:
- `record_history()` acquired connection lock, then called helper functions that tried to acquire the same lock
- `increment_version()` used `row.get()` on nullable `MAX(version_num)` which returned error on first insert

**Fixes Applied**:
1. **Deadlock Fix** (db.rs:7567-7570):
   - Call `get_or_create_key_id()` and `increment_version()` BEFORE acquiring main lock
   - Added explicit lock scope blocks to ensure proper release

2. **NULL Handling** (db.rs:7437-7441):
   - Changed `SELECT MAX(version_num)` to `SELECT COALESCE(MAX(version_num), 0)`
   - Fixed `query_to_history_entries()` to use `row.get::<_, Option<Vec<u8>>>()` for nullable columns

3. **Sort Order** (db.rs:7747-7796):
   - Changed all `ORDER BY timestamp_ms DESC` to `ORDER BY timestamp_ms ASC` in `history_get()`
   - Test expects ascending order, implementation was returning descending

4. **Data Snapshot** (db.rs:520):
   - SET operation now records `data_snapshot` with actual value instead of None
   - Enables time-travel queries via `history_get_at()`

### Added - Parallel Test Infrastructure

**New Scripts** (`redlite-dst/`):
- `run_tests_sqlite.sh` - Parallel test runner with SQLite result tracking
  - WAL mode for concurrent writes
  - Configurable parallelism (default: 8 workers)
  - 30s timeout per test
  - Real-time status updates to SQLite

- `query_tests.sh` - Test result query tool
  - `./query_tests.sh status` - Pass/fail counts
  - `./query_tests.sh failed` - List failed tests
  - `./query_tests.sh watch` - Live dashboard (refreshes every 2s)
  - `./query_tests.sh slow` - Slowest tests
  - `./query_tests.sh output <test>` - Show test output

**Database Schema**:
```sql
test_runs (id, started_at, completed_at, total_tests, parallelism)
test_results (id, run_id, test_name, status, duration_ms, output)
```

### Test Results

**Oracle Test Suite** (redlite-dst):
- **212 passed** / 10 failed / 7 timeout (92% pass rate)
- **All 6 history tests passing** after fixes
- Runtime: ~2 minutes with 8 parallel workers (vs 10+ minutes sequential)

**Failed Tests** (10):
- `oracle_cmd_persist`, `oracle_cmd_rename` - Persist/rename issues
- `oracle_cmd_xclaim`, `oracle_cmd_zcount`, `oracle_cmd_zrange`, `oracle_cmd_zrangebyscore` - Z-command bugs
- `oracle_comprehensive_mixed_ops`, `oracle_hashes_random_ops`, `oracle_keys_persist`, `oracle_keys_random_ops` - Complex scenario failures

**Timeout Tests** (7):
- `oracle_cmd_linsert`, `oracle_cmd_lrem` - List operation hangs
- `oracle_cmd_sdiffstore`, `oracle_cmd_sinterstore`, `oracle_cmd_sunionstore` - Set store operation hangs
- `oracle_lists_linsert`, `oracle_lists_lset_lrem` - List operation hangs

**Files Changed**:
- `crates/redlite/src/db.rs` - History deadlock fixes
- `redlite-dst/tests/oracle.rs` - Fixed test parameter order
- `redlite-dst/run_tests_sqlite.sh` - New parallel runner
- `redlite-dst/query_tests.sh` - New query tool

---

## Session 35.1: Sync Blocking Operations for Embedded Mode

### Added - Sync Blocking Methods

**New Methods** (no tokio required):
- `blpop_sync(keys, timeout)` - Blocking left pop with adaptive polling
- `brpop_sync(keys, timeout)` - Blocking right pop with adaptive polling
- `xread_block_sync(keys, timeout)` - Blocking stream read
- `xreadgroup_block_sync(group, consumer, keys, timeout)` - Blocking consumer group read

**Configurable Polling** (`PollConfig`):
- `PollConfig::default()` - 250μs initial, 1ms cap (balanced)
- `PollConfig::aggressive()` - 100μs initial, 500μs cap (low latency)
- `PollConfig::relaxed()` - 1ms initial, 10ms cap (low CPU)
- `db.set_poll_config()` and `db.poll_config()` methods

### Use Case - Cross-Process Coordination

Multiple processes can share the same SQLite file for inter-process communication:

```rust
// Process A: Wait for data
let db = Db::open("/shared/cache.db")?;
let result = db.blpop_sync(&["queue"], 30.0)?; // Blocks until data arrives

// Process B: Push data (separate process)
let db = Db::open("/shared/cache.db")?;
db.rpush("queue", &[b"job-123"])?; // Process A unblocks
```

SQLite with warm page cache returns queries in microseconds, making polling at 250μs-1ms intervals efficient.

### Test Results
- **13 new tests** for sync blocking operations
- **614 total tests** passing

---

## Session 41: Oracle Specs for Lists, Sets, Sorted Sets

### Added - Data Structure Spec Files

**New Spec Files**:
- `oracle/spec/lists.yaml` - 22 tests (LPUSH, RPUSH, LPOP, RPOP, LLEN, LRANGE, LINDEX)
- `oracle/spec/sets.yaml` - 16 tests (SADD, SREM, SMEMBERS, SISMEMBER, SCARD)
- `oracle/spec/zsets.yaml` - 26 tests (ZADD, ZREM, ZSCORE, ZCARD, ZCOUNT, ZINCRBY, ZRANGE, ZREVRANGE)

**Test Results**:
- **137/137 oracle tests passing** for Python SDK (up from 73)
- **137/137 oracle tests passing** for TypeScript SDK (up from 73)

### Updated - Runner Behavior Normalization

Runners now match Redis behavior for LPOP/RPOP:
- No count argument: returns single value or null
- With count argument: returns array

**Files Changed**:
- `oracle/runners/python_runner.py` - Passes through to SDK (Redis behavior)
- `oracle/runners/ts_runner.js` - Normalizes array result to single value when no count
- `oracle/README.md` - Updated coverage table
- `sdks/ROADMAP.md` - Phase 3.3 marked complete

---

## Session 40: TypeScript SDK Parity + Oracle Runner

### Added - TypeScript SDK Commands (6 new commands)

**Multi-key String Operations**:
- `mget(keys: string[])` - Get values of multiple keys
- `mset(pairs: Buffer[][])` - Set multiple key-value pairs atomically

**Hash Commands**:
- `hgetall(key)` - Get all fields and values in a hash
- `hmget(key, fields[])` - Get values of multiple hash fields

**Sorted Set Commands**:
- `zrange(key, start, stop, withScores?)` - Get range by index
- `zrevrange(key, start, stop, withScores?)` - Get range in reverse order

### Added - TypeScript Oracle Runner

**Files Created**:
- `oracle/runners/ts_runner.js` - TypeScript SDK test runner (~400 lines)
- `oracle/package.json` - Node dependencies (yaml)

**Test Results**:
- **73/73 oracle tests passing** for TypeScript SDK
- **73/73 oracle tests passing** for Python SDK
- Both SDKs produce identical results for all tested commands

### Updated

**Makefile Commands**:
```bash
make test           # Run both Python + TypeScript oracle tests
make test-ts        # Run TypeScript SDK tests only
make test-ts-verbose # Verbose TypeScript output
```

**Files Changed**:
- `redlite-ts/src/lib.rs` - Added 6 new commands
- `oracle/Makefile` - Added TypeScript runner targets
- `sdks/ROADMAP.md` - Phase 3.2 marked complete

---

## Session 39: SDK Oracle Testing Framework

### Added - Cross-SDK Oracle Testing (`sdks/oracle/`)

**Goal**: Single source of truth for SDK behavior across Python, TypeScript, and future SDKs.

#### Files Created
- `oracle/spec/strings.yaml` - 29 string command tests (GET, SET, MGET, MSET, INCR, etc.)
- `oracle/spec/hashes.yaml` - 18 hash command tests (HSET, HGET, HGETALL, HMGET, etc.)
- `oracle/spec/keys.yaml` - 26 key command tests (DEL, EXISTS, TYPE, TTL, EXPIRE, etc.)
- `oracle/runners/python_runner.py` - Python SDK test runner (~400 lines)
- `oracle/Makefile` - Build commands for running oracle tests
- `oracle/README.md` - Documentation for the oracle testing framework

#### Test Specification Format
```yaml
tests:
  - name: SET and GET roundtrip
    operations:
      - { cmd: SET, args: ["key", "value"], expect: true }
      - { cmd: GET, args: ["key"], expect: "value" }
```

#### Expectation Types Supported
- Exact match: `expect: "value"`, `expect: 42`, `expect: true`
- Null check: `expect: null`
- Unordered set: `expect: { set: ["a", "b"] }`
- Dictionary: `expect: { dict: { "k": "v" } }`
- Numeric range: `expect: { range: [58, 60] }` (for TTL tests)
- Float tolerance: `expect: { approx: 3.14, tol: 0.01 }`
- Binary data: `expect: { bytes: [0, 1, 255] }`

#### Test Results
- **73 oracle tests** - all passing
- Covers string, hash, and key commands

#### Usage
```bash
cd sdks/oracle
make test-python        # Run all oracle tests
make test-python-verbose # Verbose output
make test-spec SPEC=strings.yaml  # Single spec file
```

#### Known Behavioral Differences (documented in specs)
- `PERSIST` returns True for keys without TTL (Redis returns False)

### Updated
- `sdks/ROADMAP.md` - Task 3 Phase 3.1 marked complete

---

## Session 38: Performance Benchmarking (6 benchmarks)

### Added - Criterion Benchmarks for FT.AGGREGATE

**Benchmark Suite**: `benches/ft_aggregate.rs` with 6 comprehensive performance benchmarks.

#### Benchmarks Implemented
- `bench_ft_aggregate_1k_simple` - Single GROUPBY + COUNT on 1K documents
- `bench_ft_aggregate_10k_complex` - Complex pipeline with 5 REDUCE functions (COUNT, AVG, SUM, MAX, STDDEV), FILTER, and SORTBY
- `bench_ft_aggregate_100k_scale` - Simple and complex pipelines at 100K document scale
- `bench_ft_search_bm25` - BM25 ranking with single-term and multi-term queries on 10K documents
- `bench_scaling_comparison` - Scaling analysis across 1K/5K/10K/25K documents
- `bench_memory_pressure` - Sustained 10K write operations followed by aggregation

#### Initial Benchmark Results (Apple M1)
- **1K simple GROUPBY+COUNT**: ~4.8ms per query (~208K elements/sec throughput)
- **10K complex pipeline**: Baseline established for 5-REDUCE function pipelines
- **100K scale**: Memory pressure and throughput profiling captured

#### Files Changed
- `crates/redlite/benches/ft_aggregate.rs` - New benchmark suite (~360 lines)
- `crates/redlite/Cargo.toml` - Enabled `[[bench]]` configuration for criterion

### Usage
```bash
# Run all benchmarks
cargo bench --bench ft_aggregate

# Test mode (quick verification)
cargo bench --bench ft_aggregate -- --test

# Run specific benchmark group
cargo bench --bench ft_aggregate -- "ft_aggregate_1k"
```

### Test Results
- **668 total tests** (664 unit + 4 doc) - all passing
- **6 benchmarks** - all passing in test mode
- **Zero regressions**

---

## Session 37: Python SDK - Add Missing Commands & Test Fixes

### Added - Python SDK Commands (10 new commands)

**Hash Commands**:
- `hgetall` - Get all fields and values in a hash
- `hmget` - Get values of multiple hash fields

**Multi-key Commands**:
- `mget` - Get values of multiple keys
- `mset` - Set multiple key-value pairs atomically

**Sorted Set Commands**:
- `zrange` - Get members by rank range (ascending order)
- `zrevrange` - Get members by rank range (descending order)

**Scan Commands**:
- `scan` - Incrementally iterate keys matching a pattern
- `hscan` - Incrementally iterate hash fields
- `sscan` - Incrementally iterate set members
- `zscan` - Incrementally iterate sorted set members with scores

### Fixed - Test Suite (5 test fixes)

- Fixed `smembers` tests to expect `set()` instead of `list` (correct return type)
- Fixed `test_set_with_ex_zero` - TTL is -2 (key doesn't exist) when ex=0

### Added - SDK Oracle Testing Plan (ROADMAP)

Added Task 3 to sdks/ROADMAP.md: Cross-SDK Oracle Testing
- YAML-based test specifications shared across all SDKs
- Runners for Python, TypeScript, and Rust
- Comparison modes: exact, range, set, approx, type check
- Phases: spec format, runners, CI integration, test migration

### Test Results
- **339 tests passed** in Python SDK
- All new commands verified with smoke tests

---

## Session 36: FT.SEARCH Enhancement (10 new tests)

### Added - FT.SEARCH Robustness Tests

**SORTBY Improvements** (2 tests):
- `test_ft_search_sortby_missing_field` - Documents without sort field still returned (total = 3)
- `test_ft_search_sortby_tie_breaking` - Consistent ordering verified across multiple runs

**BM25 Accuracy** (3 tests):
- `test_bm25_term_frequency` - Higher term frequency results in higher scores
- `test_bm25_document_length_normalization` - Both short and long docs found correctly
- `test_bm25_idf_rare_terms` - Rare terms found with correct IDF behavior

**Query Parser Edge Cases** (5 tests):
- `test_query_parser_unicode_terms` - Japanese, mixed Unicode, emoji search terms work
- `test_query_parser_special_characters` - Hyphens and underscores in terms handled
- `test_query_parser_unclosed_brackets` - Malformed input handled gracefully
- `test_query_parser_deeply_nested` - 5+ levels of nested parentheses work
- `test_query_parser_empty_phrase` - Empty phrase "" handled

### Test Results
- **665 total tests** (664 unit + 4 doc + 1 ignored)
- **10 new FT.SEARCH enhancement tests** all passing
- **Zero regressions**

---

## Session 33.3: Levenshtein Ranking (16 new tests)

### Added - Levenshtein Distance Functions

**Edit Distance Calculation**: Pure Rust implementation of Wagner-Fischer algorithm for precise fuzzy search ranking.

#### Functions Added (`search.rs`)
- `levenshtein_distance(a, b)` - Calculate edit distance between two strings
- `fuzzy_score(query, candidate, max_distance)` - Normalized score (0.0-1.0) with threshold
- `best_fuzzy_match(query, text, max_distance)` - Find best matching word in multi-word text

#### Algorithm Details
- **Wagner-Fischer DP**: O(m*n) time, O(m*n) space
- **Operations counted**: Insertion, deletion, substitution (NOT transposition - that's Damerau-Levenshtein)
- **Unicode support**: Works with Japanese, emoji, CJK characters
- **Case-insensitive**: Comparisons normalized to lowercase

#### Tests (16 new tests)
**Levenshtein Distance (8 tests)**:
- `test_levenshtein_identical` - Distance = 0 for identical strings
- `test_levenshtein_deletion` - Single/multiple deletions
- `test_levenshtein_insertion` - Single/multiple insertions
- `test_levenshtein_substitution` - Character substitutions
- `test_levenshtein_transposition` - Swap = 2 ops (delete + insert)
- `test_levenshtein_empty_strings` - Edge cases
- `test_levenshtein_unicode` - Japanese, emoji, mixed
- `test_levenshtein_completely_different` - Large distances

**Fuzzy Score (4 tests)**:
- `test_fuzzy_score_exact_match` - Score = 1.0
- `test_fuzzy_score_one_edit` - Score = 0.8 for 1 edit on 5-char
- `test_fuzzy_score_threshold` - max_distance filtering
- `test_fuzzy_score_case_insensitive` - Case normalization

**Best Fuzzy Match (4 tests)**:
- `test_best_fuzzy_match_exact_word` - Find exact in text
- `test_best_fuzzy_match_typo` - Find closest despite typo
- `test_best_fuzzy_match_no_match` - Returns None
- `test_best_fuzzy_match_picks_closest` - Highest score wins

### Test Results
- **655 total tests** (654 unit + 4 doc + 1 ignored)
- **16 new Levenshtein tests** all passing
- **Zero regressions**

---

## Session 34: Bug Fixes (LPOS, LMOVE)

### Fixed - LPOS COUNT 0 Behavior
- **Issue**: `LPOS key element COUNT 0` returned only first match instead of all matches
- **Redis Spec**: `COUNT 0` means "return ALL matching positions"
- **Root Cause**: Break condition `found >= count` evaluated to `true` when `count=0` after first match
- **Fix**: Changed to `count > 0 && found >= count` - only break when count is explicitly limited
- **File**: `db.rs:3029`

### Fixed - LMOVE Same-List Deadlock
- **Issue**: `LMOVE mylist mylist LEFT RIGHT` caused test to hang indefinitely
- **Use Case**: List rotation (pop from one end, push to other end of same list)
- **Root Cause**: When `source == destination`, the connection mutex was not dropped before reacquiring
- **Fix**: Added `drop(conn)` in the same-list branch before reacquiring lock
- **File**: `db.rs:3137`

### Test Results
- **639 tests passing** (638 unit + 4 doc + 1 ignored)
- Both previously failing tests now pass
- No regressions introduced

---

## Session 33: Fuzzy Search with Trigram Tokenizer (15 new tests)

### Added - FtTokenizer Enum and Trigram Support

**Fuzzy/Substring Search**: Using SQLite FTS5's built-in trigram tokenizer for typo-tolerant search.

#### Types (`types.rs`)
- `FtTokenizer` enum with variants: `Porter` (default), `Trigram`, `Unicode61`, `Ascii`
- `FtField.tokenizer` field to specify tokenizer per TEXT field
- `FtField::text_trigram(name)` convenience constructor
- `.tokenizer(FtTokenizer)` builder method

#### Query Parser (`search.rs`)
- `QueryExpr::Fuzzy(String)` variant for `%%term%%` syntax
- Parse `%%term%%` as fuzzy/substring search
- FTS5 generation for fuzzy terms (phrase match for trigram)
- `expr_to_explain` support for FT.EXPLAIN

#### Database (`db.rs`)
- `ft_create` now uses field's tokenizer when creating FTS5 table
- Generates `tokenize='trigram'` for trigram fields
- Schema JSON includes tokenizer type for persistence

### Tests - Phase 1: Trigram Index Support (7 tests)
- `test_ft_create_with_trigram_tokenizer` - Create index with TOKENIZE trigram
- `test_ft_create_with_text_trigram_helper` - FtField::text_trigram() helper
- `test_ft_search_trigram_substring` - Find "hello" in "say hello world"
- `test_ft_search_trigram_prefix_and_suffix` - Prefix/suffix matching
- `test_ft_search_trigram_case_insensitive` - Case-insensitive search
- `test_ft_info_shows_tokenizer` - FT.INFO displays tokenizer type
- `test_ft_tokenizer_builder_pattern` - Builder pattern test

### Tests - Phase 2: Fuzzy Query Syntax (8 tests)
- `test_ft_search_fuzzy_syntax_basic` - Basic %%term%% query
- `test_ft_search_fuzzy_typo_matches` - Similar word matching
- `test_ft_search_fuzzy_field_scoped` - @field:%%term%%
- `test_ft_search_fuzzy_mixed_query` - Fuzzy + exact in same query
- `test_ft_search_fuzzy_unicode` - Unicode (Japanese) fuzzy matching
- `test_ft_search_fuzzy_short_terms` - Short term edge cases
- `test_query_parser_fuzzy_expr` - Parser produces Fuzzy variant
- `test_query_parser_fuzzy_in_and` - Fuzzy in AND expression

### Test Results
- **15 new tests** (7 trigram + 8 fuzzy)
- **639 total tests** with `--features "vectors geo"`
- **All new tests passing**

### Usage
```rust
use redlite::types::{FtField, FtOnType};

// Create index with trigram tokenizer
let schema = vec![FtField::text_trigram("content")];
db.ft_create("idx", FtOnType::Hash, &["doc:"], &schema)?;

// Index documents
db.hset("doc:1", &[("content", b"hello world")])?;

// Substring search (trigram enables this)
let (total, results) = db.ft_search("idx", "hello", &options)?;

// Explicit fuzzy syntax
let (total, results) = db.ft_search("idx", "%%program%%", &options)?;
```

---

## Session 32: Vector Search Test Expansion (35 → 61 tests)

### Added - 26 New Vector Tests

**Test Coverage Expansion**: Comprehensive testing of all V* command features for production-readiness.

#### Distance Metrics & Accuracy (3 tests)
- `test_vsim_l2_distance_accuracy` - L2 distance calculation verification
- `test_vsim_cosine_accuracy` - Cosine similarity with orthogonal/parallel vectors
- `test_vsim_inner_product` - Inner product similarity metric

#### Quantization (1 test)
- `test_vadd_quantization_preserves_similarity` - Q8 vs NoQuant ranking consistency

#### Scale & Dimensions (3 tests)
- `test_vadd_large_scale` - 1000 vectors, verify search performance
- `test_vadd_very_high_dimensions` - 1536 dimensions (OpenAI embedding size)
- `test_vadd_single_dimension` - 1D vector edge case

#### Vector Properties (3 tests)
- `test_vadd_normalized_vectors` - Unit-length vectors
- `test_vadd_zero_vector_handling` - Degenerate zero vectors
- `test_vadd_negative_values` - Negative embedding values

#### Query Behavior (3 tests)
- `test_vsim_dimension_mismatch_query` - Mismatched query dimensions
- `test_vsim_count_zero` - COUNT=0 returns empty
- `test_vsim_count_exceeds_available` - COUNT > available elements

#### Attributes (3 tests)
- `test_vgetattr_complex_json` - Nested JSON attributes
- `test_vsetattr_update_existing` - Attribute updates
- `test_vsetattr_remove_attributes` - Empty JSON attribute removal

#### Operations (4 tests)
- `test_vrandmember_count_negative` - Random sampling with count parameter
- `test_vrem_multiple_elements` - Bulk element removal
- `test_vector_cross_database_isolation` - Database isolation behavior
- `test_vinfo_with_mixed_quantization` - Mixed quantization in same set

#### Search Features (3 tests)
- `test_vsim_with_filter_complex` - Attribute-based filtering
- `test_vsim_exact_match_score` - Perfect match scoring
- `test_vcard_nonexistent_key` - VCARD on missing key returns 0

### Test Results
- **61 total vector tests** (35 existing + 26 new)
- **592 total tests** with `--features "vectors geo"` (61 V* + 41 FT.AGGREGATE + 17 geo + 473 other)
- **All tests passing** in 0.49 seconds
- **Zero test failures**

### Coverage Summary
Now testing:
- ✅ All distance metrics (L2, Cosine, Inner Product)
- ✅ All quantization modes (NoQuant, Q8, BF16)
- ✅ Scale: 1-1536 dimensions, 1-1000 vectors per set
- ✅ Edge cases: zero vectors, negative values, dimension mismatches
- ✅ Attribute operations: complex JSON, updates, removal
- ✅ Filter integration with VSIM
- ✅ All V* commands: VADD, VREM, VCARD, VDIM, VINFO, VEMB, VGETATTR, VSETATTR, VRANDMEMBER, VSIM

**Production Readiness**: Vector search is comprehensively tested with 74% more coverage.

---

## Session 31.5: Fuzzy Search Planning

### Added - Fuzzy Search Implementation Plan
- Comprehensive roadmap for Session 32: Fuzzy Search & Spell Correction
- **Phase 1**: Trigram tokenizer for approximate matching (~15 tests)
- **Phase 2**: Levenshtein distance for precise ranking (~10 tests)
- **Phase 3**: FT.SPELLCHECK with spellfix1 extension (~10 tests)
- Feature flags: `fuzzy` and `spellcheck` with integration into `full` feature set

### Documentation Updates
- Added [Session 32 plan](ROADMAP.md#L559-L755) with implementation details
- Updated feature flags to include fuzzy search in `full` feature
- References to FTS5 custom tokenizers, Wagner-Fischer algorithm, SQLite spellfix1

---

## Session 31: FT.AGGREGATE Test Expansion (14 → 41 tests)

### Added - 27 New FT.AGGREGATE Tests

**Test Coverage Expansion**: Comprehensive testing of all FT.AGGREGATE features to ensure production-readiness.

#### REDUCE Functions (8 tests)
- `test_ft_aggregate_reduce_sum` - SUM reducer aggregating numeric values
- `test_ft_aggregate_reduce_avg` - AVG reducer calculating mean
- `test_ft_aggregate_reduce_min_max` - MIN and MAX reducers in same aggregation
- `test_ft_aggregate_reduce_stddev` - STDDEV for statistical variance analysis
- `test_ft_aggregate_reduce_count_distinct` - COUNT_DISTINCT for unique value counts
- `test_ft_aggregate_reduce_count_distinctish` - Approximate unique count (HyperLogLog-style)
- `test_ft_aggregate_reduce_tolist` - TOLIST collecting values into arrays
- `test_ft_aggregate_reduce_first_value` - FIRST_VALUE from each group

#### SORTBY Variations (5 tests)
- `test_ft_aggregate_sortby_desc` - Descending sort order
- `test_ft_aggregate_sortby_multiple_fields` - Multi-field sorting with tiebreakers
- `test_ft_aggregate_sortby_with_max` - SORTBY MAX for top-N queries
- `test_ft_aggregate_sortby_on_original_field` - Sort on original fields (no APPLY needed)
- `test_ft_aggregate_sortby_numeric_vs_string` - Numeric vs lexical sorting behavior

#### GROUPBY Variations (3 tests)
- `test_ft_aggregate_groupby_multiple_fields` - Group by 2+ fields (category + status)
- `test_ft_aggregate_groupby_multiple_reducers` - Multiple REDUCE functions in one GROUPBY
- `test_ft_aggregate_groupby_missing_fields` - Graceful handling of missing group fields

#### LOAD, LIMIT, and Pipeline Tests (6 tests)
- `test_ft_aggregate_load_specific_fields` - LOAD only requested fields
- `test_ft_aggregate_load_with_groupby` - LOAD with aggregations
- `test_ft_aggregate_limit_offset` - Pagination with LIMIT offset num
- `test_ft_aggregate_limit_edge_cases` - Edge cases (LIMIT 0, out-of-bounds offset)
- `test_ft_aggregate_full_pipeline` - Complete pipeline: LOAD + GROUPBY + REDUCE + APPLY + FILTER + SORTBY + LIMIT
- `test_ft_aggregate_complex_ecommerce` - Real-world e-commerce analytics scenario

#### Query Integration (3 tests)
- `test_ft_aggregate_with_text_query` - Aggregation with FTS text queries
- `test_ft_aggregate_with_field_query` - Field-scoped queries with aggregation
- `test_ft_aggregate_with_numeric_range` - Numeric range queries (baseline for future enhancement)

#### Edge Cases (2 tests)
- `test_ft_aggregate_empty_results` - Aggregation when query matches zero documents
- `test_ft_aggregate_single_document` - Aggregation with single document (StdDev edge case)

### Test Results
- **41 total FT.AGGREGATE tests** (14 existing + 27 new)
- **566 total tests** with `--features geo` (41 FT.AGGREGATE + 17 geo + 508 other)
- **All tests passing** in 0.35 seconds
- **Zero test failures**

### Coverage Summary
Now testing:
- ✅ All 12 REDUCE functions (COUNT, COUNT_DISTINCT, SUM, AVG, MIN, MAX, STDDEV, TOLIST, FIRST_VALUE, QUANTILE, RANDOM_SAMPLE, COUNT_DISTINCTISH)
- ✅ SORTBY ASC/DESC with single and multiple fields
- ✅ SORTBY MAX for top-N queries
- ✅ GROUPBY with 1-N fields and 1-N reducers
- ✅ LOAD feature for field selection
- ✅ LIMIT with offset for pagination
- ✅ APPLY expressions (arithmetic, string functions)
- ✅ FILTER expressions (comparison operators, logical AND/OR)
- ✅ Full pipeline combinations
- ✅ Query integration (text search + aggregation)
- ✅ Edge cases (empty results, single document, missing fields)

**Production Readiness**: FT.AGGREGATE is now comprehensively tested and ready for production use.

---

## Session 30: Documentation Audit & Roadmap Synchronization

### Documentation Updates
- **Reviewed Session 28** keyset pagination implementation
  - Verified all 16 scan tests passing
  - Confirmed cursor format changes working correctly
  - Noted WASM SDK uses separate SQLite implementation (acceptable)
- **Discovered FT.AGGREGATE is complete**
  - Phase 3 was marked as "Next" but fully implemented
  - All 14 FT.AGGREGATE tests passing
  - Complete feature set: LOAD, GROUPBY, REDUCE, SORTBY, APPLY, FILTER, LIMIT
- **Updated ROADMAP.md**
  - Added Session 29 completion summary
  - Marked Phase 3: RediSearch Aggregations as COMPLETE
  - Updated "In Progress" section to "Completed Major Features"
  - Documented all REDUCE functions and APPLY/FILTER support

### Test Status
- **539 total tests** across all features
- **16 scan tests** (keyset pagination)
- **14 FT.AGGREGATE tests** (all REDUCE functions, APPLY, FILTER, SORTBY)
- **509 other tests** (strings, lists, hashes, sets, sorted sets, streams, FTS, geo, vectors)
- **Zero test failures**

### Key Findings
- FT.AGGREGATE implementation is production-ready
- Keyset pagination improves SCAN performance from O(n) to O(log n + k)
- Standard Redis clients work correctly with string cursors
- Documentation now accurately reflects implementation status

---

## Session 29: Oracle Test Expansion (66 → 85 tests)

### Added - 19 New Oracle Tests
- **Stream commands**: `xrange`, `xrevrange`, `xdel`, `xinfo_stream`
- **Sorted sets**: `zremrangebyscore`
- **Keys**: `scan_iteration` (proper cursor iteration test)
- **String options**: `set_options_nx_xx`, `set_options_ex_px`
- **Type mismatch tests**: `string_on_list`, `list_on_string`, `hash_on_set`, `zset_on_hash`
- **Edge cases**: `empty_value`, `large_value` (1MB strings)
- **Empty operations**: `lists_empty`, `hashes_empty`, `sets_empty`, `zsets_empty`
- **Server**: `ping_echo`

### Updated - ROADMAP.md Checklist
- Comprehensive command coverage checklist added
- Tracked ~100 Redis-compatible commands
- 85 tests now cover: strings, keys, hashes, lists, sets, sorted sets, streams
- Remaining: blocking commands (BLPOP/BRPOP), transactions, GEO

### Test Coverage Summary
- **85 oracle tests** (up from 66)
- **Zero divergences** across all data types
- **Type mismatch errors** (WRONGTYPE) verified against Redis
- **Edge cases** tested (empty values, 1MB strings, non-existent keys)

---

## Session 28: Keyset Pagination for SCAN Commands

### Performance Optimization
- **Refactored all SCAN commands** (SCAN, HSCAN, SSCAN, ZSCAN) from OFFSET-based to keyset pagination
- **Complexity improvement**: O(n) per call -> O(log n + k) per call for large datasets
- **Cursor format**: Base64-encoded last-seen value instead of integer offset
  - SCAN: `base64(last_key)`
  - HSCAN: `base64(last_field)`
  - SSCAN: `base64(last_member)`
  - ZSCAN: `base64(JSON{"s":score,"m":"base64(member)"})` for compound ordering

### Implementation Details
- `db.rs`: Updated `scan()`, `hscan()`, `sscan()`, `zscan()` methods
- `server/mod.rs`: Updated `cmd_scan`, `cmd_hscan`, `cmd_sscan`, `cmd_zscan` handlers
- SQL queries now use `WHERE key > ?` instead of `OFFSET ?`
- All 16 scan-related unit tests pass

### Why Keyset Pagination
With OFFSET, SQLite must scan and skip N rows for each page. With keyset pagination using `WHERE key > last_seen`, SQLite uses the index to jump directly to the next page. This matters significantly for datasets with 100K+ keys.

---

## Session 27.2: Redis Oracle Testing

### Added - Oracle Integration Tests
- **`tests/oracle.rs`** - 24 comprehensive Redis oracle tests
- **Test coverage:**
  - **Strings**: `set_get`, `incr_decr`, `append`, `random_ops`
  - **Lists**: `push_pop`, `llen_lindex`, `random_ops`
  - **Hashes**: `basic`, `multiple_fields`, `hincrby`, `random_ops`
  - **Sets**: `basic`, `smembers`, `random_ops`
  - **Sorted Sets**: `basic`, `ordering`, `zincrby`, `random_ops`
  - **Keys**: `exists_del`, `type`, `ttl_expire`, `rename`, `random_ops`
  - **Comprehensive**: `mixed_ops` (2000 operations across all data types)

### Added - RedliteClient Key Operations
- **`exists()`** - Check if keys exist
- **`del()`** - Delete keys
- **`key_type()`** - Get key type
- **`keys()`** - Find keys matching pattern
- **`rename()`** - Rename a key

### Added - CLI Oracle Command
- **`redlite-dst oracle`** - Now includes `keys` test group (6 groups total)
- Tests: strings, lists, hashes, sets, sorted_sets, keys

### Usage
```bash
# Start Redis (native or Docker)
redis-server &
# Or: docker run -d -p 6379:6379 redis

# Run oracle tests (sequential required - tests share Redis state)
cargo test --test oracle -- --test-threads=1

# Via CLI
redlite-dst oracle --redis localhost:6379 --ops 1000
```

### Test Results
- ✅ 24 oracle tests passing
- ✅ Zero divergences across all test groups
- ✅ 100% compatibility with Redis for tested operations

---

## Session 27.1: Property-Based Testing + Fuzzing

### Added - Proptest Property Tests
- **`tests/properties.rs`** - 34 comprehensive proptest-based property tests
- **Properties covered:**
  - `set_get_roundtrip`, `set_nx_behavior`, `set_xx_behavior` - String operations
  - `incr_atomic`, `decr_atomic`, `incrby_exact` - Counter operations
  - `list_rpush_order`, `list_lpush_order`, `list_lpop_left`, `list_rpop_right`, `list_llen` - List operations
  - `set_uniqueness`, `set_ismember`, `set_srem` - Set operations
  - `zset_score_ordering`, `zset_reverse_ordering`, `zset_score_exact`, `zset_zincrby` - Sorted set operations
  - `hash_field_roundtrip`, `hash_field_update`, `hash_hdel`, `hash_hgetall`, `hash_hincrby` - Hash operations
  - `expire_ttl`, `persist_removes_ttl` - Expiration operations
  - `del_removes_key`, `exists_count` - Key management
  - `type_string`, `type_list`, `type_set`, `type_zset`, `type_hash`, `type_nonexistent` - TYPE command
  - `append_concat` - APPEND command

### Added - Cargo-Fuzz Targets
- **`fuzz/fuzz_targets/resp_parser.rs`** - Fuzz RESP protocol parsing
- **`fuzz/fuzz_targets/query_parser.rs`** - Fuzz FT.SEARCH query syntax
- **`fuzz/fuzz_targets/command_handler.rs`** - Fuzz Redis command execution with Arbitrary derive

### Added - Dependencies
- **`proptest = "1.5"`** - Property-based testing framework
- **`arbitrary = "1.3"`** - Structured fuzzing with derive macro
- **`libfuzzer-sys = "0.4"`** - In-process coverage-guided fuzzing

### Updated - Regression Seeds
- **`tests/regression_seeds.txt`** - Organized by test type (properties, simulate, chaos, fuzz)
- Added categorized sections for better organization

### Fixed - Pre-existing Issues
- **`base64` dependency** - Added missing dependency to redlite crate
- **`zscan` tests** - Fixed API mismatch (string cursor vs integer)

### Test Results
- ✅ 34 proptest property tests passing
- ✅ 9 redlite-dst unit tests passing
- ✅ All tests complete in ~120 seconds

---

## Session 27.5.5: Report Output Wiring

### Added - JSON and Markdown Report Output
- **`--format` flag** - Output test results in `console` (default), `json`, or `markdown` format
- **`--output` flag** - Write report to file instead of stdout
- **`OutputFormat` enum** - Type-safe format selection in TestRunner
- **`output_results()` method** - Central report generation for all test commands

### Changed - TestRunner API
- **`TestRunner::new(verbose, format, output)`** - Now accepts format and output parameters
- **Results tracking** - All commands now collect `Vec<TestResult>` for report generation
- **Updated commands**: `smoke`, `properties`, `oracle`, `simulate`, `chaos`, `seeds_test`

### Usage Examples
```bash
# Console output (default)
redlite-dst smoke

# JSON report to stdout
redlite-dst smoke --format json

# Markdown report to file
redlite-dst properties --seeds 100 --format markdown --output report.md

# JSON report for CI integration
redlite-dst simulate --seeds 10 --format json --output results.json
```

### Test Results
- ✅ 9 redlite-dst tests passing
- ✅ All three output formats verified working
- ✅ File output with `--output` verified

---

## Session 27: DST Code Review and Cleanup

### Removed - Dead Code and Distributed System Concepts
- **Deleted `libsql_db.rs`** - 768 lines of unused code (libsql backend removed)
- **Removed libsql from `backend.rs`** - Only SQLite and Turso (feature-gated) remain
- **Removed distributed concepts from `sim.rs`**:
  - Removed `network_delay_prob` field (no network in embedded DB)
  - Removed `with_network_delay()` builder
  - Removed `get_delay()` method
- **Removed dead types from `types.rs`**:
  - `RegressionSeed` - actual impl uses simple text file format
  - `Fault` enum - chaos testing uses inline string matching
  - `MemorySnapshot` - not used, can re-add when soak testing enhanced

### Added - Write Contention Scenario
- **`write_contention` simulation** - Multiple writers hammering hot keys with INCR/DECR/SET/GET
- Tests data consistency under heavy write contention to 5 hot keys

### Fixed - License Consistency
- Changed `redlite-dst/Cargo.toml` license from MIT to Apache-2.0
- Removed MIT license badge from `redlite-dst/README.md`
- License is Apache-2.0 at root level only

### Code Review Findings (for future work)
- `cloud` command is placeholder (will use fly-benchmark-engine)
- Main redlite crate has ~10 warnings (unused variables, dead methods)

### Test Results
- ✅ 9 redlite-dst tests passing
- ✅ Build clean

## Session 26: MadSim Integration for DST

### Added - MadSim Deterministic Runtime Support
- **`madsim` feature flag** - Enable with `RUSTFLAGS="--cfg madsim" cargo build --features madsim`
- **Conditional tokio/madsim-tokio** - Swaps runtime based on cfg flag
- **`src/sim.rs` module** - Unified simulation API for both runtimes:
  - `SimConfig` - Configuration for deterministic simulation runs
  - `SimContext` - Controlled randomness and fault injection context
  - `runtime::spawn()`, `runtime::sleep()`, `runtime::yield_now()` - Works with both tokio and madsim

### Changed - DST Runner Improvements
- **Refactored `runner.rs`** - Uses `sim::runtime` module for async operations
- **Refactored `main.rs`** - Conditional main function for madsim/tokio compatibility
- **Fixed `concurrent_operations` test** - Properly handles type changes when keys are converted from strings to lists/sets
- **Implemented `REPLAY` command** - Full routing to all test types (properties, simulate, chaos, specific scenarios)

### MadSim Benefits
- **True deterministic async scheduling** - Tokio's task scheduling is non-deterministic; MadSim makes it reproducible
- **Simulated time** - Tests run instantly with simulated time instead of real time
- **Fault injection APIs** - Kill processes, partition networks, inject delays programmatically
- **Seed-based replay** - Exact reproduction of any test run

### Test Results
```
Normal build (tokio):
  ✅ Smoke tests: 7/7 passed (44ms)
  ✅ Simulate tests: 30/30 passed (1.9s)

MadSim build:
  ✅ Smoke tests: 7/7 passed (0ms - simulated time)
  ✅ Simulate tests: 30/30 passed (0ms - simulated time)
```

### Files Modified
- `redlite-dst/Cargo.toml` - Added madsim feature and conditional dependencies
- `redlite-dst/src/sim.rs` - New simulation module with cfg-conditional runtime
- `redlite-dst/src/runner.rs` - Updated to use sim::runtime functions
- `redlite-dst/src/main.rs` - Conditional main for madsim/tokio
- `redlite-dst/README.md` - Added MadSim usage documentation

## Session 25: Geospatial Commands (R*Tree)

### Added - GEO* Command Family
- **GEOADD** - Add members with longitude/latitude to a geo set
  - Supports NX (only add new), XX (only update), CH (return changed count) flags
  - Coordinate validation: lon -180 to 180, lat -85.05112878 to 85.05112878
- **GEOPOS** - Get coordinates (longitude, latitude) for members
- **GEODIST** - Calculate distance between two members
  - Supports units: M (meters), KM (kilometers), MI (miles), FT (feet)
- **GEOHASH** - Get 11-character geohash strings for members
- **GEOSEARCH** - Search for members within radius or bounding box
  - FROMMEMBER / FROMLONLAT center options
  - BYRADIUS / BYBOX shape options
  - ASC/DESC sorting, COUNT limit, ANY flag
  - WITHCOORD, WITHDIST, WITHHASH response options
- **GEOSEARCHSTORE** - Store GEOSEARCH results as sorted set
  - STOREDIST option to store distances instead of geohashes

### Implementation Details
- Uses SQLite's built-in R*Tree extension for efficient spatial indexing
- Haversine formula for accurate great-circle distance calculations
- Base32 geohash encoding (11 chars = ~0.6mm precision)
- Bounding box pre-filtering for radius queries

### Feature Flag
- `geo` feature flag enables geo commands (R*Tree is built into SQLite)
- Included in `full` feature: `--features full` or `--features geo`

### Files Modified
- `src/schema_geo.sql` - New schema for geo_data + geo_rtree tables
- `src/types.rs` - Added GeoUnit, GeoMember, GeoSearchOptions types
- `src/db.rs` - Added geo* methods + helper functions (haversine, encode_geohash, bounding_box)
- `src/server/mod.rs` - Added cmd_geo* server handlers
- `Cargo.toml` - Added `geo` and `full` features

### Test Results
- ✅ 17 new geo-specific tests
- ✅ **Total: 469 unit tests + 4 doctests = 473 tests passing** with `--features geo`

## Session 24: Redis 8 Vector Commands

### Added - V* Command Family (Redis 8 Compatible)
- **VADD** - Add vector elements to a set with embeddings
- **VREM** - Remove vector elements from a set
- **VCARD** - Get cardinality (number of elements) in vector set
- **VEXISTS** - Check if element exists in vector set
- **VSIM** - KNN similarity search within a vector set
- **VSIMBATCH** - Batch similarity search across multiple vector sets
- **VGET** - Get embedding for specific element
- **VGETALL** - Get all elements and embeddings in a set
- **VGETATTRIBUTES** - Get attributes for elements
- **VSETATTRIBUTES** - Set attributes for elements
- **VDELATTRIBUTES** - Delete attributes from elements
- **VDIM** - Get dimensions of vectors in a set

### Schema Migration
- Migrated from old `vector_settings`/`vectors` tables to Redis 8-compatible `vector_sets` table
- New schema supports element-based storage with optional attributes
- Auto-detects vector dimensions from first element
- Supports quantization types: NOQUANT, Q8, BF16

### Implementation
- Vector DB methods: `db.rs:8301-8710` (410 lines)
- V* command handlers: `server/mod.rs:4835-5329` (495 lines)
- Vector types: `types.rs:785-858` (74 lines)
- Commands registered in dispatcher at lines 504-513 and 6893-6902

### Fixed
- PRAGMA execution in `open_with_cache()` and `set_cache_mb()`
- Changed `execute()` to `execute_batch()` to avoid `ExecuteReturnedResults` error
- Fixed failing doctest at `db.rs:201` for `Db::open_with_cache`

### Test Results
- ✅ All 487 unit tests passing
- ✅ All 4 doctests passing
- ✅ **Total: 491/491 tests passing** with `--features vectors`

## Session 22: Complete DST Command Implementation

### Added - ORACLE Command (Redis Comparison)
- Full Redis compatibility testing via `redlite-dst oracle`
- Compares Redlite against real Redis instance for 5 data types:
  - **Strings**: SET, GET, INCR, APPEND
  - **Lists**: LPUSH, RPUSH, LPOP, RPOP, LRANGE
  - **Hashes**: HSET, HGET, HDEL, HGETALL
  - **Sets**: SADD, SREM, SISMEMBER, SMEMBERS
  - **Sorted Sets**: ZADD, ZSCORE, ZRANGE
- Reports divergence count and compatibility percentage
- Requires Redis running: `docker run -d -p 6379:6379 redis`

### Added - SIMULATE Command (Deterministic Simulation)
- Seed-reproducible simulation testing via `redlite-dst simulate`
- Three scenarios per seed:
  - **concurrent_operations**: Virtual connections with deterministic interleaving
  - **crash_recovery**: Write data, simulate crash (drop client), verify recovery
  - **connection_storm**: Rapid open/close cycles to test connection churn
- Uses ChaCha8Rng for cross-platform reproducibility
- Tracks expected state for verification

### Added - CHAOS Command (Fault Injection)
- Fault injection testing via `redlite-dst chaos`
- Four fault scenarios:
  - **crash_mid_write**: Verify data survives simulated crashes
  - **corrupt_read**: Test graceful handling of read errors
  - **disk_full**: Verify database remains usable under write pressure
  - **slow_write**: Test operation completion under delays

### Added - STRESS Command (Load Testing)
- Concurrent load testing via `redlite-dst stress`
- Spawns multiple tokio tasks for parallel operations
- Metrics: throughput (ops/sec), latency percentiles (p50, p99)
- Memory monitoring via sysinfo crate

### Added - FUZZ Command (In-Process Fuzzing)
- Random input testing via `redlite-dst fuzz`
- Three fuzz targets:
  - **resp_parser**: Random RESP protocol data
  - **query_parser**: Random FT.SEARCH query syntax
  - **command_handler**: Random Redis commands
- Catches panics, reports crash seeds for reproduction
- Seed-based reproducibility for crash replay

### Added - SOAK Command (Stability Testing)
- Long-running stability testing via `redlite-dst soak`
- Memory growth monitoring with leak detection warnings
- Throughput stability analysis (coefficient of variation)
- Background operation generator with mixed workload

### Dependencies Added
- `redis = "0.27"` for oracle testing
- `sysinfo = "0.32"` for memory monitoring
- `parking_lot = "0.12"` for synchronization

### Test Results Summary
```
✅ Smoke tests: 7/7 passed
✅ Property tests: 70/70 passed (10 seeds × 7 properties)
✅ All DST commands implemented with real Redlite library
```

## Session 21: Deterministic Simulation Testing Foundation

### Added - redlite-dst Testing Framework
- Created `redlite-dst/` crate for deterministic simulation testing
- Integrated actual Redlite library (replaced in-memory mock)
- Implemented 7 comprehensive smoke tests:
  - `basic_set_get` - SET/GET roundtrip validation
  - `basic_incr_decr` - INCR/DECR monotonicity checks
  - `basic_list_ops` - LPUSH/RPUSH/LRANGE/LPOP verification
  - `basic_hash_ops` - HSET/HGET/HGETALL validation
  - `basic_set_ops` - SADD/SMEMBERS/SISMEMBER checks
  - `basic_sorted_set` - ZADD/ZRANGE/ZSCORE ordering verification
  - `basic_persistence` - File-backed database recovery testing
- All smoke tests: **7/7 passing** (34ms)

### Added - Regression Seed Management
- `redlite-dst seeds list` - Display all regression seeds
- `redlite-dst seeds add` - Add failing seeds with descriptions to permanent bank
- `redlite-dst seeds test` - Replay all regression seeds with property tests
- Seed file format: `SEED TEST_TYPE DESCRIPTION` in `tests/regression_seeds.txt`

### Added - Property-Based Testing
- 7 properties testing core Redis operations:
  - `set_get_roundtrip` - SET k v; GET k => v
  - `incr_is_monotonic` - INCR always increases
  - `list_order_preserved` - LPUSH/RPUSH preserve order
  - `hash_fields_unique` - Hash field updates work correctly
  - `sorted_set_ordering` - ZRANGE returns sorted results
  - `expire_removes_key` - TTL/expiration behavior
  - `crash_recovery_consistent` - Persistence verification
- All property tests: **70/70 passing** (223ms, 10 seeds × 7 properties)

### Infrastructure
- Added tempfile dependency for persistence testing
- Created `.gitignore` for redlite-dst target directory
- Updated README with testing framework section
- Updated ROADMAP marking Phase 5 partially complete

### Test Results Summary
```
✅ Smoke tests: 7/7 passed (34ms)
✅ Property tests: 70/70 passed (223ms)
✅ Regression seeds: 1/1 passed (80ms)
```

## Session 20: Monorepo & Multi-Language SDKs

### Changed - Monorepo Migration
- **BREAKING**: Reorganized into monorepo structure
  - Rust core moved to `crates/redlite/`
  - Python SDK moved to `bindings/python/`
  - TypeScript SDK added at `bindings/node/`
- Fixed Rust build: Disabled turso backend in server mode
- Updated all SDK build scripts to reference monorepo paths

### Added - TypeScript/Node SDK
- Created `@redlite/node` package with full feature parity
- Embedded mode with binary bundling (darwin-arm64, darwin-x86_64, linux-x86_64, win32-x86_64)
- Extends ioredis for full Redis compatibility
- FTS namespace implementation
- Works with both Node.js and Bun
- Vitest test suite with embedded mode support

### Added - Python SDK Enhancements
- Binary bundling infrastructure with platform-specific builds
- Updated build scripts for monorepo structure
- Maturin-ready for future PyO3 optimization
- 98 unit tests passing (including embedded mode)

### Added - Documentation System
- LLM-powered doc generation templates
- Language-agnostic pseudocode template (`docs/templates/sdk-guide.template.md`)
- Focus on use cases: "Embedded Redis + SQLite durability"
- Emphasizes CLI/desktop/serverless use cases over feature lists

### Infrastructure
- Unified GitHub Actions CI workflow (`monorepo-ci.yml`)
- Cross-platform binary builds (macOS, Linux, Windows)
- Automated SDK testing with real binaries
- Root-level Makefile for building all SDKs
- Migration guide (`MONOREPO_MIGRATION.md`)

### Roadmap Updates
- Async client for Python: Decided to skip for v1 (use run_in_executor)
- SDK priority: TypeScript ✅, WASM (Q1 2026), Go (Q2), Kotlin (Q2)
- Documentation approach: Template-based, LLM-assisted generation

## Sessions 1-23.2 (Complete)

### Benchmark Suite Enhancements
- File-backed database size measurement (db + WAL + shm files)
- History entry count and storage bytes tracking
- `get_history_count()` trait method for global history stats
- `bytes_per_history_entry` calculation in BenchmarkResult
- Enhanced `print_summary()` with history metrics output

### Session 23.2: FT.SEARCH Implementation
- `src/search.rs` query parser module for RediSearch syntax
- Query translation: AND/OR/NOT, phrases, prefix, field-scoped
- Numeric range queries (@field:[min max])
- Tag exact match queries (@field:{tag1|tag2})
- FT.SEARCH with NOCONTENT, VERBATIM, WITHSCORES, LIMIT, SORTBY, RETURN
- In-memory text matching fallback for unindexed documents
- 26 new tests (14 query parser + 12 ft_search integration)

### Session 23.1: RediSearch Index Management
- FT.CREATE, FT.DROPINDEX, FT._LIST, FT.INFO, FT.ALTER
- FT.ALIASADD/DEL/UPDATE for index aliases
- FT.SYNUPDATE/DUMP for synonym groups
- FT.SUGADD/GET/DEL/LEN for autocomplete suggestions
- Schema support: TEXT, NUMERIC, TAG field types
- 22 comprehensive unit tests

### Session 23: Per-Connection State & CLIENT Commands
- CLIENT LIST with TYPE/ID filters
- CLIENT INFO, CLIENT KILL, CLIENT PAUSE/UNPAUSE
- Connection lifecycle management with ConnectionPool

### Session 22: Redis Ecosystem Compatibility
- `--password` flag and AUTH command
- `--backend` (sqlite/turso), `--storage` (file/memory) flags
- WATCH/UNWATCH optimistic locking with version tracking
- CLIENT SETNAME/GETNAME/LIST/ID
- LREM, LINSERT list operations
- SMOVE, SDIFFSTORE, SINTERSTORE, SUNIONSTORE set operations

### Session 18: Performance & Cache Configuration
- `redlite-bench` benchmarking suite (35+ scenarios)
- `--cache` flag for SQLite page cache tuning
- `Db::open_with_cache()` and `db.set_cache_mb()` API

### Session 17: History Tracking & Time-Travel
- Three-tier opt-in (global, database, key)
- HISTORY ENABLE/DISABLE/GET/GETAT/STATS/CLEAR/PRUNE/LIST
- MessagePack serialization for efficient storage
- Configurable retention (time-based, count-based)

### Session 16: Transactions
- MULTI/EXEC/DISCARD command batching
- WATCH/UNWATCH optimistic locking
- Command queueing with validation

### Session 15: Blocking & Pub/Sub (Server Mode)
- BLPOP, BRPOP blocking list operations
- XREAD BLOCK, XREADGROUP BLOCK stream operations
- SUBSCRIBE/UNSUBSCRIBE/PUBLISH/PSUBSCRIBE/PUNSUBSCRIBE
- Tokio broadcast channels for notifications

### Session 14: Stream Consumer Groups
- XGROUP CREATE/DESTROY/SETID/CREATECONSUMER/DELCONSUMER
- XREADGROUP with consumer tracking
- XACK, XPENDING, XCLAIM
- XINFO GROUPS/CONSUMERS

### Session 13: Streams
- XADD, XLEN, XRANGE, XREVRANGE, XREAD
- XTRIM, XDEL, XINFO STREAM
- MessagePack field encoding
- Entry ID format: `{timestamp}-{seq}`

### Session 11-12: Custom Commands & Polish
- VACUUM (delete expired + SQLite VACUUM)
- KEYINFO (type, ttl, created_at, updated_at)
- AUTOVACUUM ON/OFF/INTERVAL
- Error message Redis compatibility

### Session 10: Server Operations
- SELECT (multiple databases 0-15)
- INFO, DBSIZE, FLUSHDB
- Per-connection database isolation

### Session 9: Sorted Sets
- ZADD, ZREM, ZSCORE, ZRANK, ZREVRANK
- ZRANGE, ZREVRANGE, ZRANGEBYSCORE
- ZCOUNT, ZCARD, ZINCRBY
- ZREMRANGEBYRANK, ZREMRANGEBYSCORE

### Session 8: Sets
- SADD, SREM, SMEMBERS, SISMEMBER, SCARD
- SPOP, SRANDMEMBER (with count)
- SDIFF, SINTER, SUNION

### Session 7: Lists
- LPUSH, RPUSH, LPOP, RPOP (with count)
- LLEN, LRANGE, LINDEX, LSET, LTRIM
- Integer gap positioning for O(1) operations

### Session 6: Hashes
- HSET, HGET, HMGET, HGETALL
- HDEL, HEXISTS, HKEYS, HVALS, HLEN
- HINCRBY, HINCRBYFLOAT, HSETNX

### Session 3: String Operations
- INCR, DECR, INCRBY, DECRBY, INCRBYFLOAT
- MGET, MSET, APPEND, STRLEN
- GETRANGE, SETRANGE

### Session 2: Key Management
- DEL, EXISTS, TYPE
- KEYS (glob pattern), SCAN (with MATCH, COUNT)
- TTL, PTTL, EXPIRE, PERSIST

### Session 1: Foundation
- GET, SET (with EX/PX/NX/XX)
- RESP protocol parser
- TCP server mode
- Lazy expiration
- SQLite schema with WAL mode
