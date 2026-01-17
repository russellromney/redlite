# Redlite Dart/Flutter SDK

Redis API with SQLite durability for Flutter/Dart applications.

## Features

- **Redis-compatible API**: Familiar commands like GET, SET, HSET, LPUSH, ZADD
- **SQLite durability**: ACID transactions with WAL mode
- **Embedded database**: No network overhead, single-file storage
- **Cross-platform**: iOS, Android, macOS, Linux, Windows, Web (WASM)
- **Type-safe**: Full Dart type safety with null-safety support

## Installation

Add to your `pubspec.yaml`:

```yaml
dependencies:
  redlite: ^0.1.0
```

## Quick Start

```dart
import 'package:redlite/redlite.dart';
import 'dart:typed_data';
import 'dart:convert';

void main() async {
  // Initialize the Rust library
  await RustLib.init();

  // Open a database (file-based or in-memory)
  final db = RedliteDb.openMemory();
  // Or: final db = RedliteDb.open('/path/to/database.db');

  // String operations
  await db.set('user:1:name', Uint8List.fromList('Alice'.codeUnits), null);
  final name = await db.get('user:1:name');
  print('Name: ${utf8.decode(name!)}');

  // Hash operations
  await db.hset('user:1', 'email', Uint8List.fromList('alice@example.com'.codeUnits));
  await db.hset('user:1', 'age', Uint8List.fromList('30'.codeUnits));
  final fields = await db.hgetall('user:1');

  // List operations
  await db.lpush('queue', [Uint8List.fromList('task1'.codeUnits)]);
  await db.rpush('queue', [Uint8List.fromList('task2'.codeUnits)]);
  final items = await db.lrange('queue', 0, -1);

  // Set operations
  await db.sadd('tags', [
    Uint8List.fromList('flutter'.codeUnits),
    Uint8List.fromList('dart'.codeUnits),
  ]);
  final members = await db.smembers('tags');

  // Sorted set operations
  await db.zadd('leaderboard', [
    ZMember(score: 100, member: Uint8List.fromList('player1'.codeUnits)),
    ZMember(score: 200, member: Uint8List.fromList('player2'.codeUnits)),
  ]);
  final leaders = await db.zrevrange('leaderboard', 0, 10, true);

  // TTL support
  await db.setex('session', 3600, Uint8List.fromList('token123'.codeUnits));
  final ttl = await db.ttl('session');
  print('Session TTL: $ttl seconds');
}
```

## API Reference

### Database Operations

- `RedliteDb.open(path)` - Open file-based database
- `RedliteDb.openMemory()` - Open in-memory database
- `RedliteDb.openWithCache(path, cacheMb)` - Open with custom cache size

### String Commands

- `get(key)` / `set(key, value, ttlSeconds)`
- `setex(key, seconds, value)` / `psetex(key, ms, value)`
- `getdel(key)` / `append(key, value)`
- `incr(key)` / `decr(key)` / `incrby(key, n)` / `decrby(key, n)`
- `strlen(key)` / `getrange(key, start, end)` / `setrange(key, offset, value)`

### Key Commands

- `del(keys)` / `exists(keys)` / `keyType(key)`
- `ttl(key)` / `pttl(key)` / `expire(key, seconds)` / `persist(key)`
- `rename(key, newkey)` / `renamenx(key, newkey)`
- `keys(pattern)` / `scan(cursor, pattern, count)` / `dbsize()` / `flushdb()`

### Hash Commands

- `hset(key, field, value)` / `hmset(key, mapping)` / `hget(key, field)`
- `hdel(key, fields)` / `hexists(key, field)` / `hlen(key)`
- `hkeys(key)` / `hvals(key)` / `hgetall(key)` / `hmget(key, fields)`
- `hincrby(key, field, increment)` / `hscan(key, cursor, pattern, count)`

### List Commands

- `lpush(key, values)` / `rpush(key, values)`
- `lpop(key, count)` / `rpop(key, count)`
- `llen(key)` / `lrange(key, start, stop)` / `lindex(key, index)`
- `ltrim(key, start, stop)` / `lset(key, index, value)`

### Set Commands

- `sadd(key, members)` / `srem(key, members)` / `smembers(key)`
- `sismember(key, member)` / `scard(key)`
- `sdiff(keys)` / `sinter(keys)` / `sunion(keys)`
- `sscan(key, cursor, pattern, count)`

### Sorted Set Commands

- `zadd(key, members)` / `zrem(key, members)`
- `zscore(key, member)` / `zcard(key)` / `zcount(key, min, max)`
- `zincrby(key, increment, member)`
- `zrange(key, start, stop, withScores)` / `zrevrange(key, start, stop, withScores)`
- `zrank(key, member)` / `zrevrank(key, member)`
- `zscan(key, cursor, pattern, count)`

### Multi-key Commands

- `mget(keys)` / `mset(pairs)`

### Server Commands

- `vacuum()` - Reclaim disk space

## Building from Source

### Prerequisites

- Flutter SDK 3.10+
- Rust 1.70+
- flutter_rust_bridge_codegen

### Build Steps

```bash
# Install flutter_rust_bridge_codegen
cargo install flutter_rust_bridge_codegen

# Generate bindings
flutter_rust_bridge_codegen generate

# Build for current platform
flutter build <platform>
```

### Platform-specific builds

```bash
# iOS/macOS (requires Xcode)
./scripts/build_apple.sh

# Android (requires NDK)
./scripts/build_android.sh

# Linux
cargo build --release --target x86_64-unknown-linux-gnu

# Windows
cargo build --release --target x86_64-pc-windows-msvc
```

## License

MIT License
