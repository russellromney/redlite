# Redlite FFI Bindings

C FFI bindings for embedding Redlite in other languages like Python (via CFFI) and Go (via CGO).

## Overview

This crate produces a C-compatible shared library (`libredlite_ffi`) that exposes Redlite's functionality through a C ABI. This allows any language with C FFI support to use Redlite as an embedded database.

## Building

```bash
cd crates/redlite-ffi
cargo build --release
```

The output will be:
- **macOS**: `target/release/libredlite_ffi.dylib`
- **Linux**: `target/release/libredlite_ffi.so`
- **Windows**: `target/release/redlite_ffi.dll`

The C header file `redlite.h` is auto-generated via cbindgen during the build.

## Memory Management

All memory allocated by the library must be freed by the caller:

- **Strings**: Free with `redlite_free_string(ptr)`
- **Byte arrays**: Free with `redlite_free_bytes(ptr)`
- **String arrays**: Free with `redlite_free_string_array(ptr)`
- **Bytes arrays**: Free with `redlite_free_bytes_array(ptr)`
- **Database handle**: Free with `redlite_close(ptr)`

## Error Handling

Functions that can fail return a status code:
- `0` = success
- Negative values = error (call `redlite_last_error()` for message)

Functions returning data use out-parameters; NULL indicates no value (e.g., key not found).

## C API Reference

### Lifecycle

```c
// Open a database (":memory:" for in-memory, or file path)
RedliteDb* redlite_open(const char* path);

// Open with custom cache size (MB)
RedliteDb* redlite_open_with_cache(const char* path, int cache_mb);

// Close database and free resources
void redlite_close(RedliteDb* db);

// Get library version
char* redlite_version(void);

// Get last error message (thread-local)
char* redlite_last_error(void);
```

### String Commands

```c
// GET - Returns NULL if key doesn't exist
RedliteBytes redlite_get(RedliteDb* db, const char* key);

// SET - ttl_ms=0 for no expiration
int redlite_set(RedliteDb* db, const char* key,
                const uint8_t* value, size_t value_len,
                int64_t ttl_ms);

// SETEX - Set with TTL in seconds
int redlite_setex(RedliteDb* db, const char* key,
                  int64_t seconds,
                  const uint8_t* value, size_t value_len);

// PSETEX - Set with TTL in milliseconds
int redlite_psetex(RedliteDb* db, const char* key,
                   int64_t milliseconds,
                   const uint8_t* value, size_t value_len);

// GETDEL - Get and delete
RedliteBytes redlite_getdel(RedliteDb* db, const char* key);

// APPEND - Returns new length
int64_t redlite_append(RedliteDb* db, const char* key,
                       const uint8_t* value, size_t value_len);

// STRLEN
int64_t redlite_strlen(RedliteDb* db, const char* key);

// GETRANGE
RedliteBytes redlite_getrange(RedliteDb* db, const char* key,
                              int64_t start, int64_t end);

// SETRANGE - Returns new length
int64_t redlite_setrange(RedliteDb* db, const char* key,
                         int64_t offset,
                         const uint8_t* value, size_t value_len);

// INCR/DECR
int64_t redlite_incr(RedliteDb* db, const char* key);
int64_t redlite_decr(RedliteDb* db, const char* key);
int64_t redlite_incrby(RedliteDb* db, const char* key, int64_t amount);
int64_t redlite_decrby(RedliteDb* db, const char* key, int64_t amount);
double redlite_incrbyfloat(RedliteDb* db, const char* key, double amount);
```

### Key Commands

```c
// DEL - Returns count of deleted keys
int64_t redlite_del(RedliteDb* db, const char** keys, size_t count);

// EXISTS - Returns count of existing keys
int64_t redlite_exists(RedliteDb* db, const char** keys, size_t count);

// TYPE - Returns type string ("string", "list", "set", "zset", "hash", "none")
char* redlite_type(RedliteDb* db, const char* key);

// KEYS - Pattern matching
RedliteStringArray redlite_keys(RedliteDb* db, const char* pattern);

// DBSIZE
int64_t redlite_dbsize(RedliteDb* db);

// FLUSHDB
int redlite_flushdb(RedliteDb* db);

// RENAME
int redlite_rename(RedliteDb* db, const char* key, const char* newkey);

// RENAMENX - Returns 1 if renamed, 0 if newkey exists
int redlite_renamenx(RedliteDb* db, const char* key, const char* newkey);
```

### TTL Commands

```c
// TTL - Returns seconds, -1 if no TTL, -2 if key doesn't exist
int64_t redlite_ttl(RedliteDb* db, const char* key);

// PTTL - Returns milliseconds
int64_t redlite_pttl(RedliteDb* db, const char* key);

// EXPIRE - Returns 1 if set, 0 if key doesn't exist
int redlite_expire(RedliteDb* db, const char* key, int64_t seconds);

// PEXPIRE
int redlite_pexpire(RedliteDb* db, const char* key, int64_t milliseconds);

// EXPIREAT - Unix timestamp in seconds
int redlite_expireat(RedliteDb* db, const char* key, int64_t timestamp);

// PEXPIREAT - Unix timestamp in milliseconds
int redlite_pexpireat(RedliteDb* db, const char* key, int64_t timestamp_ms);

// PERSIST - Returns 1 if TTL removed, 0 if no TTL
int redlite_persist(RedliteDb* db, const char* key);
```

### Hash Commands

```c
// HSET - Returns count of new fields
int64_t redlite_hset(RedliteDb* db, const char* key,
                     const RedliteKV* fields, size_t count);

// HGET
RedliteBytes redlite_hget(RedliteDb* db, const char* key, const char* field);

// HDEL
int64_t redlite_hdel(RedliteDb* db, const char* key,
                     const char** fields, size_t count);

// HEXISTS
int redlite_hexists(RedliteDb* db, const char* key, const char* field);

// HLEN
int64_t redlite_hlen(RedliteDb* db, const char* key);

// HKEYS
RedliteStringArray redlite_hkeys(RedliteDb* db, const char* key);

// HVALS
RedliteBytesArray redlite_hvals(RedliteDb* db, const char* key);

// HINCRBY
int64_t redlite_hincrby(RedliteDb* db, const char* key,
                        const char* field, int64_t amount);
```

### List Commands

```c
// LPUSH/RPUSH - Returns new length
int64_t redlite_lpush(RedliteDb* db, const char* key,
                      const RedliteBytes* values, size_t count);
int64_t redlite_rpush(RedliteDb* db, const char* key,
                      const RedliteBytes* values, size_t count);

// LPOP/RPOP
RedliteBytesArray redlite_lpop(RedliteDb* db, const char* key, int64_t count);
RedliteBytesArray redlite_rpop(RedliteDb* db, const char* key, int64_t count);

// LLEN
int64_t redlite_llen(RedliteDb* db, const char* key);

// LRANGE
RedliteBytesArray redlite_lrange(RedliteDb* db, const char* key,
                                 int64_t start, int64_t stop);

// LINDEX
RedliteBytes redlite_lindex(RedliteDb* db, const char* key, int64_t index);
```

### Set Commands

```c
// SADD - Returns count of new members
int64_t redlite_sadd(RedliteDb* db, const char* key,
                     const RedliteBytes* members, size_t count);

// SREM
int64_t redlite_srem(RedliteDb* db, const char* key,
                     const RedliteBytes* members, size_t count);

// SMEMBERS
RedliteBytesArray redlite_smembers(RedliteDb* db, const char* key);

// SISMEMBER
int redlite_sismember(RedliteDb* db, const char* key,
                      const uint8_t* member, size_t member_len);

// SCARD
int64_t redlite_scard(RedliteDb* db, const char* key);
```

### Sorted Set Commands

```c
// ZADD - Returns count of new members
int64_t redlite_zadd(RedliteDb* db, const char* key,
                     const RedliteZMember* members, size_t count);

// ZREM
int64_t redlite_zrem(RedliteDb* db, const char* key,
                     const RedliteBytes* members, size_t count);

// ZSCORE - out_exists set to 1 if member exists
double redlite_zscore(RedliteDb* db, const char* key,
                      const uint8_t* member, size_t member_len,
                      int* out_exists);

// ZCARD
int64_t redlite_zcard(RedliteDb* db, const char* key);

// ZCOUNT
int64_t redlite_zcount(RedliteDb* db, const char* key,
                       double min, double max);

// ZINCRBY
double redlite_zincrby(RedliteDb* db, const char* key,
                       double increment,
                       const uint8_t* member, size_t member_len);
```

### Server Commands

```c
// SELECT database (0-15)
int redlite_select(RedliteDb* db, int db_num);

// VACUUM - Returns bytes freed
int64_t redlite_vacuum(RedliteDb* db);
```

### Memory Freeing

```c
void redlite_free_string(char* ptr);
void redlite_free_bytes(RedliteBytes bytes);
void redlite_free_string_array(RedliteStringArray arr);
void redlite_free_bytes_array(RedliteBytesArray arr);
```

## Example Usage (C)

```c
#include "redlite.h"
#include <stdio.h>

int main() {
    // Open database
    RedliteDb* db = redlite_open(":memory:");
    if (!db) {
        char* err = redlite_last_error();
        printf("Error: %s\n", err);
        redlite_free_string(err);
        return 1;
    }

    // Set a value
    const char* value = "Hello, World!";
    redlite_set(db, "greeting", (const uint8_t*)value, strlen(value), 0);

    // Get the value
    RedliteBytes result = redlite_get(db, "greeting");
    if (result.data) {
        printf("Value: %.*s\n", (int)result.len, result.data);
        redlite_free_bytes(result);
    }

    // Increment a counter
    int64_t count = redlite_incr(db, "counter");
    printf("Counter: %lld\n", count);

    // Close database
    redlite_close(db);
    return 0;
}
```

## Thread Safety

The database handle is protected by a mutex, making it safe for concurrent access from multiple threads. Each call acquires the lock, performs the operation, and releases the lock.

## Differences from Core Redlite

- **Synchronous only**: All operations are blocking
- **No transactions**: Each operation is atomic but there's no multi-command transaction support
- **No pub/sub**: Publish/subscribe not available via FFI
- **No scripting**: Lua scripting not supported

## Language Bindings

This FFI library is used by:
- **Python SDK**: Uses CFFI to load the shared library
- **Go SDK**: Uses CGO to link against the library
