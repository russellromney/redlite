---
title: Dart/Flutter SDK
description: Redlite SDK for Dart and Flutter applications
---

The Dart SDK provides native Redlite bindings for Flutter applications using [flutter_rust_bridge](https://cjycode.com/flutter_rust_bridge/).

## Installation

Add to your `pubspec.yaml`:

```yaml
dependencies:
  redlite:
    git:
      url: https://github.com/russellromney/redlite
      path: sdks/redlite-dart
```

## Quick Start

```dart
import 'package:redlite/redlite.dart';

void main() async {
  // Initialize the library
  await RustLib.init();

  // Open an in-memory database
  final db = Db.openMemory();

  // Or open a persistent database
  // final db = Db.open(path: '/path/to/db.sqlite');

  // String operations
  await db.set_(key: 'name', value: 'Alice'.codeUnits);
  final name = await db.get_(key: 'name');
  print(utf8.decode(name!)); // Alice

  // Increment/Decrement
  await db.incr(key: 'counter');
  await db.incrby(key: 'counter', increment: 5);

  // Hash operations
  await db.hset(key: 'user:1', field: 'name', value: 'Bob'.codeUnits);
  await db.hset(key: 'user:1', field: 'age', value: '30'.codeUnits);
  final user = await db.hgetall(key: 'user:1');

  // List operations
  await db.lpush(key: 'queue', values: ['task1', 'task2'].map((s) => Uint8List.fromList(s.codeUnits)).toList());
  final task = await db.rpop(key: 'queue');

  // Set operations
  await db.sadd(key: 'tags', members: ['redis', 'sqlite'].map((s) => Uint8List.fromList(s.codeUnits)).toList());
  final isMember = await db.sismember(key: 'tags', member: 'redis'.codeUnits);

  // Sorted set operations
  await db.zadd(key: 'scores', members: [
    ZMember(score: 100.0, member: Uint8List.fromList('player1'.codeUnits)),
    ZMember(score: 85.0, member: Uint8List.fromList('player2'.codeUnits)),
  ]);
  final topPlayers = await db.zrevrange(key: 'scores', start: 0, stop: 9, withScores: true);
}
```

## API Reference

### Database Operations

```dart
// Open database
Db.open(path: String)           // File-based database
Db.openMemory()                 // In-memory database
Db.openWithCache(path: String, cacheMb: int)  // With custom cache size

// Database management
db.dbsize()                     // Get key count
db.flushdb()                    // Delete all keys
db.vacuum()                     // Reclaim space
```

### String Commands

```dart
db.set_(key: String, value: List<int>, ttlSeconds: int?)
db.get_(key: String) -> Uint8List?
db.getdel(key: String) -> Uint8List?
db.mset(pairs: List<(String, Uint8List)>)
db.mget(keys: List<String>) -> List<Uint8List?>
db.incr(key: String) -> int
db.incrby(key: String, increment: int) -> int
db.incrbyfloat(key: String, increment: double) -> double
db.decr(key: String) -> int
db.decrby(key: String, decrement: int) -> int
db.append(key: String, value: List<int>) -> int
db.strlen(key: String) -> int
db.getrange(key: String, start: int, end: int) -> Uint8List
db.setrange(key: String, offset: int, value: List<int>) -> int
db.setex(key: String, seconds: int, value: List<int>)
db.psetex(key: String, milliseconds: int, value: List<int>)
```

### Key Commands

```dart
db.del(keys: List<String>) -> int
db.exists(keys: List<String>) -> int
db.keyType(key: String) -> KeyType
db.keys(pattern: String) -> List<String>
db.rename(key: String, newkey: String)
db.renamenx(key: String, newkey: String) -> bool
db.expire(key: String, seconds: int) -> bool
db.pexpire(key: String, milliseconds: int) -> bool
db.expireat(key: String, unixTime: int) -> bool
db.persist(key: String) -> bool
db.ttl(key: String) -> int
db.pttl(key: String) -> int
db.scan(cursor: String, pattern: String?, count: int) -> (String, List<String>)
```

### Hash Commands

```dart
db.hset(key: String, field: String, value: List<int>) -> int
db.hget(key: String, field: String) -> Uint8List?
db.hmset(key: String, mapping: List<(String, Uint8List)>) -> int
db.hmget(key: String, fields: List<String>) -> List<Uint8List?>
db.hgetall(key: String) -> List<(String, Uint8List)>
db.hdel(key: String, fields: List<String>) -> int
db.hexists(key: String, field: String) -> bool
db.hlen(key: String) -> int
db.hkeys(key: String) -> List<String>
db.hvals(key: String) -> List<Uint8List>
db.hincrby(key: String, field: String, increment: int) -> int
db.hscan(key: String, cursor: String, pattern: String?, count: int) -> (String, List<(String, Uint8List)>)
```

### List Commands

```dart
db.lpush(key: String, values: List<Uint8List>) -> int
db.rpush(key: String, values: List<Uint8List>) -> int
db.lpop(key: String, count: int?) -> List<Uint8List>
db.rpop(key: String, count: int?) -> List<Uint8List>
db.llen(key: String) -> int
db.lrange(key: String, start: int, stop: int) -> List<Uint8List>
db.lindex(key: String, index: int) -> Uint8List?
db.lset(key: String, index: int, value: List<int>)
db.ltrim(key: String, start: int, stop: int)
```

### Set Commands

```dart
db.sadd(key: String, members: List<Uint8List>) -> int
db.srem(key: String, members: List<Uint8List>) -> int
db.smembers(key: String) -> List<Uint8List>
db.sismember(key: String, member: List<int>) -> bool
db.scard(key: String) -> int
db.sinter(keys: List<String>) -> List<Uint8List>
db.sunion(keys: List<String>) -> List<Uint8List>
db.sdiff(keys: List<String>) -> List<Uint8List>
db.sscan(key: String, cursor: String, pattern: String?, count: int) -> (String, List<Uint8List>)
```

### Sorted Set Commands

```dart
db.zadd(key: String, members: List<ZMember>) -> int
db.zrem(key: String, members: List<Uint8List>) -> int
db.zscore(key: String, member: List<int>) -> double?
db.zcard(key: String) -> int
db.zcount(key: String, minScore: double, maxScore: double) -> int
db.zincrby(key: String, increment: double, member: List<int>) -> double
db.zrange(key: String, start: int, stop: int, withScores: bool) -> List<ZMember>
db.zrevrange(key: String, start: int, stop: int, withScores: bool) -> List<ZMember>
db.zrank(key: String, member: List<int>) -> int?
db.zrevrank(key: String, member: List<int>) -> int?
db.zscan(key: String, cursor: String, pattern: String?, count: int) -> (String, List<(Uint8List, double)>)
```

## Platform Support

The SDK supports all Flutter platforms via FFI:
- Android (arm64, arm32, x86_64, x86)
- iOS (arm64, simulator)
- macOS (arm64, x86_64)
- Linux (x86_64)
- Windows (x86_64)

## Building from Source

```bash
cd sdks/redlite-dart

# Install dependencies
make setup

# Generate Rust-Dart bindings
make codegen

# Build for current platform
make build

# Run tests
make test
```

## Example Flutter App

```dart
import 'package:flutter/material.dart';
import 'package:redlite/redlite.dart';
import 'dart:convert';

void main() async {
  WidgetsFlutterBinding.ensureInitialized();
  await RustLib.init();
  runApp(MyApp());
}

class MyApp extends StatefulWidget {
  @override
  _MyAppState createState() => _MyAppState();
}

class _MyAppState extends State<MyApp> {
  late Db db;
  int counter = 0;

  @override
  void initState() {
    super.initState();
    db = Db.openMemory();
    _loadCounter();
  }

  Future<void> _loadCounter() async {
    final value = await db.get_(key: 'counter');
    if (value != null) {
      setState(() {
        counter = int.parse(utf8.decode(value));
      });
    }
  }

  Future<void> _incrementCounter() async {
    final newValue = await db.incr(key: 'counter');
    setState(() {
      counter = newValue;
    });
  }

  @override
  Widget build(BuildContext context) {
    return MaterialApp(
      home: Scaffold(
        appBar: AppBar(title: Text('Redlite Counter')),
        body: Center(
          child: Text('Counter: $counter', style: TextStyle(fontSize: 24)),
        ),
        floatingActionButton: FloatingActionButton(
          onPressed: _incrementCounter,
          child: Icon(Icons.add),
        ),
      ),
    );
  }
}
```

## Notes

- All values are stored as bytes (`Uint8List`). Use `utf8.encode()` and `utf8.decode()` for strings.
- The SDK uses flutter_rust_bridge v2.11.1 for seamless Rust integration.
- Database operations are async and thread-safe.
