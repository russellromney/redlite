// Oracle Test Runner for Dart SDK.
//
// Executes YAML test specifications against the Redlite Dart SDK
// and reports pass/fail results.
//
// Run with: flutter test test/oracle_test.dart

import 'dart:convert';
import 'dart:io';
import 'dart:typed_data';
import 'package:flutter_test/flutter_test.dart';
import 'package:yaml/yaml.dart';
import 'package:redlite/src/rust/api.dart';
import 'package:redlite/src/rust/frb_generated.dart';

void main() {
  setUpAll(() async {
    await RustLib.init();
  });

  // Find spec files
  final specDir = Directory('${Directory.current.path}/../oracle/spec');
  if (!specDir.existsSync()) {
    test('Oracle specs not found', () {
      fail('Could not find oracle spec directory at ${specDir.path}');
    });
    return;
  }

  final specFiles = specDir
      .listSync()
      .whereType<File>()
      .where((f) => f.path.endsWith('.yaml'))
      .toList()
    ..sort((a, b) => a.path.compareTo(b.path));

  for (final specFile in specFiles) {
    final content = specFile.readAsStringSync();
    final spec = loadYaml(content);
    final specName = spec['name'] ?? specFile.path.split('/').last;
    final tests = spec['tests'] as YamlList? ?? [];

    group(specName, () {
      for (final testCase in tests) {
        final testName = testCase['name'] ?? 'unnamed';

        test(testName, () async {
          final db = Db.openMemory();

          try {
            // Run setup operations
            final setup = testCase['setup'] as YamlList? ?? [];
            for (final op in setup) {
              await _executeCmd(db, op);
            }

            // Run test operations and check expectations
            final operations = testCase['operations'] as YamlList;
            for (final op in operations) {
              final actual = await _executeCmd(db, op);
              final expected = op['expect'];

              expect(
                _compare(actual, expected),
                isTrue,
                reason:
                    'Command ${op['cmd']} with args ${op['args']}: expected $expected but got ${_serialize(actual)}',
              );
            }
          } finally {
            // DB cleanup handled by garbage collection
          }
        });
      }
    });
  }
}

Future<dynamic> _executeCmd(Db db, dynamic op) async {
  final cmd = (op['cmd'] as String).toLowerCase();
  final args = (op['args'] as YamlList?)?.toList() ?? [];

  // Process args to handle special types
  final processedArgs = args.map(_processArg).toList();

  switch (cmd) {
    // String commands
    case 'get':
      final result = await db.get_(key: processedArgs[0] as String);
      if (result == null) return null;
      // Try to decode as UTF-8, but return raw bytes if that fails
      try {
        return utf8.decode(result);
      } catch (_) {
        return result;
      }

    case 'set':
      await db.set_(
        key: processedArgs[0] as String,
        value: _toBytes(processedArgs[1]),
      );
      return true;

    case 'setex':
      await db.setex(
        key: processedArgs[0] as String,
        seconds: (processedArgs[1] as num).toInt(),
        value: _toBytes(processedArgs[2]),
      );
      return true;

    case 'psetex':
      await db.psetex(
        key: processedArgs[0] as String,
        milliseconds: (processedArgs[1] as num).toInt(),
        value: _toBytes(processedArgs[2]),
      );
      return true;

    case 'setnx':
      final result = await db.setOpts(
        key: processedArgs[0] as String,
        value: _toBytes(processedArgs[1]),
        options: SetOptions(nx: true, xx: false),
      );
      return result ? 1 : 0;

    case 'setxx':
      final result = await db.setOpts(
        key: processedArgs[0] as String,
        value: _toBytes(processedArgs[1]),
        options: SetOptions(nx: false, xx: true),
      );
      return result ? 1 : 0;

    case 'getdel':
      final result = await db.getdel(key: processedArgs[0] as String);
      return result != null ? utf8.decode(result) : null;

    case 'append':
      return await db.append(
        key: processedArgs[0] as String,
        value: _toBytes(processedArgs[1]),
      );

    case 'strlen':
      return await db.strlen(key: processedArgs[0] as String);

    case 'getrange':
      final result = await db.getrange(
        key: processedArgs[0] as String,
        start: (processedArgs[1] as num).toInt(),
        end: (processedArgs[2] as num).toInt(),
      );
      return utf8.decode(result);

    case 'setrange':
      return await db.setrange(
        key: processedArgs[0] as String,
        offset: (processedArgs[1] as num).toInt(),
        value: _toBytes(processedArgs[2]),
      );

    case 'incr':
      return await db.incr(key: processedArgs[0] as String);

    case 'decr':
      return await db.decr(key: processedArgs[0] as String);

    case 'incrby':
      return await db.incrby(
        key: processedArgs[0] as String,
        increment: (processedArgs[1] as num).toInt(),
      );

    case 'decrby':
      return await db.decrby(
        key: processedArgs[0] as String,
        decrement: (processedArgs[1] as num).toInt(),
      );

    case 'incrbyfloat':
      return await db.incrbyfloat(
        key: processedArgs[0] as String,
        increment: (processedArgs[1] as num).toDouble(),
      );

    case 'mget':
      final keys = (processedArgs[0] as List).cast<String>();
      final results = await db.mget(keys: keys);
      return results.map((r) => r != null ? utf8.decode(r) : null).toList();

    case 'mset':
      final pairs = <(String, Uint8List)>[];
      for (final pair in processedArgs) {
        final p = pair as List;
        pairs.add((p[0] as String, _toBytes(p[1])));
      }
      await db.mset(pairs: pairs);
      return true;

    // Key commands
    case 'del':
      final keys = (processedArgs[0] as List).cast<String>();
      return await db.del(keys: keys);

    case 'exists':
      final keys = (processedArgs[0] as List).cast<String>();
      return await db.exists(keys: keys);

    case 'type':
      final kt = await db.keyType(key: processedArgs[0] as String);
      // Map Dart enum names to Redis standard names
      switch (kt) {
        case KeyType.string:
          return 'string';
        case KeyType.list:
          return 'list';
        case KeyType.set_:
          return 'set';
        case KeyType.hash:
          return 'hash';
        case KeyType.zSet:
          return 'zset';
        case KeyType.stream:
          return 'stream';
        case KeyType.none:
          return 'none';
      }

    case 'ttl':
      return await db.ttl(key: processedArgs[0] as String);

    case 'pttl':
      return await db.pttl(key: processedArgs[0] as String);

    case 'expire':
      return await db.expire(
        key: processedArgs[0] as String,
        seconds: (processedArgs[1] as num).toInt(),
      );

    case 'pexpire':
      return await db.pexpire(
        key: processedArgs[0] as String,
        milliseconds: (processedArgs[1] as num).toInt(),
      );

    case 'expireat':
      return await db.expireat(
        key: processedArgs[0] as String,
        unixTime: (processedArgs[1] as num).toInt(),
      );

    case 'persist':
      return await db.persist(key: processedArgs[0] as String);

    case 'rename':
      await db.rename(
        key: processedArgs[0] as String,
        newkey: processedArgs[1] as String,
      );
      return true;

    case 'renamenx':
      return await db.renamenx(
        key: processedArgs[0] as String,
        newkey: processedArgs[1] as String,
      );

    case 'keys':
      return await db.keys(pattern: processedArgs[0] as String);

    case 'dbsize':
      return await db.dbsize();

    case 'flushdb':
      await db.flushdb();
      return true;

    // Hash commands
    case 'hset':
      return await db.hset(
        key: processedArgs[0] as String,
        field: processedArgs[1] as String,
        value: _toBytes(processedArgs[2]),
      );

    case 'hget':
      final result = await db.hget(
        key: processedArgs[0] as String,
        field: processedArgs[1] as String,
      );
      return result != null ? utf8.decode(result) : null;

    case 'hmset':
      final key = processedArgs[0] as String;
      final mapping = <(String, Uint8List)>[];
      for (var i = 1; i < processedArgs.length; i += 2) {
        mapping.add((
          processedArgs[i] as String,
          _toBytes(processedArgs[i + 1]),
        ));
      }
      await db.hmset(key: key, mapping: mapping);
      return true;

    case 'hmget':
      final key = processedArgs[0] as String;
      final fields = (processedArgs[1] as List).cast<String>();
      final results = await db.hmget(key: key, fields: fields);
      return results.map((r) => r != null ? utf8.decode(r) : null).toList();

    case 'hdel':
      final key = processedArgs[0] as String;
      final fields = (processedArgs[1] as List).cast<String>();
      return await db.hdel(key: key, fields: fields);

    case 'hexists':
      final result = await db.hexists(
        key: processedArgs[0] as String,
        field: processedArgs[1] as String,
      );
      return result ? 1 : 0;

    case 'hlen':
      return await db.hlen(key: processedArgs[0] as String);

    case 'hkeys':
      return await db.hkeys(key: processedArgs[0] as String);

    case 'hvals':
      final results = await db.hvals(key: processedArgs[0] as String);
      return results.map((r) => utf8.decode(r)).toList();

    case 'hgetall':
      final results = await db.hgetall(key: processedArgs[0] as String);
      final map = <String, String>{};
      for (final (field, value) in results) {
        map[field] = utf8.decode(value);
      }
      return map;

    case 'hincrby':
      return await db.hincrby(
        key: processedArgs[0] as String,
        field: processedArgs[1] as String,
        increment: (processedArgs[2] as num).toInt(),
      );

    // List commands
    case 'lpush':
      final key = processedArgs[0] as String;
      // Oracle spec uses nested format: ["key", ["a", "b", "c"]]
      final valueList = processedArgs[1] as List;
      final values = valueList.map((v) => _toBytes(v)).toList();
      return await db.lpush(key: key, values: values);

    case 'rpush':
      final key = processedArgs[0] as String;
      // Oracle spec uses nested format: ["key", ["a", "b", "c"]]
      final valueList = processedArgs[1] as List;
      final values = valueList.map((v) => _toBytes(v)).toList();
      return await db.rpush(key: key, values: values);

    case 'lpop':
      final count = processedArgs.length > 1 ? (processedArgs[1] as num).toInt() : null;
      final results = await db.lpop(key: processedArgs[0] as String, count: count);
      if (results.isEmpty) return null;
      if (count != null) {
        return results.map((r) => utf8.decode(r)).toList();
      }
      return utf8.decode(results[0]);

    case 'rpop':
      final count = processedArgs.length > 1 ? (processedArgs[1] as num).toInt() : null;
      final results = await db.rpop(key: processedArgs[0] as String, count: count);
      if (results.isEmpty) return null;
      if (count != null) {
        return results.map((r) => utf8.decode(r)).toList();
      }
      return utf8.decode(results[0]);

    case 'llen':
      return await db.llen(key: processedArgs[0] as String);

    case 'lrange':
      final results = await db.lrange(
        key: processedArgs[0] as String,
        start: (processedArgs[1] as num).toInt(),
        stop: (processedArgs[2] as num).toInt(),
      );
      return results.map((r) => utf8.decode(r)).toList();

    case 'lindex':
      final result = await db.lindex(
        key: processedArgs[0] as String,
        index: (processedArgs[1] as num).toInt(),
      );
      return result != null ? utf8.decode(result) : null;

    case 'ltrim':
      await db.ltrim(
        key: processedArgs[0] as String,
        start: (processedArgs[1] as num).toInt(),
        stop: (processedArgs[2] as num).toInt(),
      );
      return true;

    case 'lset':
      await db.lset(
        key: processedArgs[0] as String,
        index: (processedArgs[1] as num).toInt(),
        value: _toBytes(processedArgs[2]),
      );
      return true;

    // Set commands
    case 'sadd':
      final key = processedArgs[0] as String;
      // Oracle spec uses nested format: ["key", ["a", "b", "c"]]
      final memberList = processedArgs[1] as List;
      final members = memberList.map((v) => _toBytes(v)).toList();
      return await db.sadd(key: key, members: members);

    case 'srem':
      final key = processedArgs[0] as String;
      // Oracle spec uses nested format: ["key", ["a", "b", "c"]]
      final memberList = processedArgs[1] as List;
      final members = memberList.map((v) => _toBytes(v)).toList();
      return await db.srem(key: key, members: members);

    case 'smembers':
      final results = await db.smembers(key: processedArgs[0] as String);
      return results.map((r) => utf8.decode(r)).toSet();

    case 'sismember':
      final result = await db.sismember(
        key: processedArgs[0] as String,
        member: _toBytes(processedArgs[1]),
      );
      return result ? 1 : 0;

    case 'scard':
      return await db.scard(key: processedArgs[0] as String);

    case 'sdiff':
      final keys = processedArgs.cast<String>();
      final results = await db.sdiff(keys: keys);
      return results.map((r) => utf8.decode(r)).toSet();

    case 'sinter':
      final keys = processedArgs.cast<String>();
      final results = await db.sinter(keys: keys);
      return results.map((r) => utf8.decode(r)).toSet();

    case 'sunion':
      final keys = processedArgs.cast<String>();
      final results = await db.sunion(keys: keys);
      return results.map((r) => utf8.decode(r)).toSet();

    // Sorted set commands
    case 'zadd':
      final key = processedArgs[0] as String;
      final members = <ZMember>[];
      // Oracle spec uses nested format: [[score, member], [score, member], ...]
      final memberPairs = processedArgs[1] as List;
      for (final pair in memberPairs) {
        final p = pair as List;
        members.add(ZMember(
          score: (p[0] as num).toDouble(),
          member: _toBytes(p[1]),
        ));
      }
      return await db.zadd(key: key, members: members);

    case 'zrem':
      final key = processedArgs[0] as String;
      // Oracle spec uses: ["key", ["member1", "member2"]]
      final memberList = processedArgs[1] as List;
      final members = memberList.map((v) => _toBytes(v)).toList();
      return await db.zrem(key: key, members: members);

    case 'zscore':
      return await db.zscore(
        key: processedArgs[0] as String,
        member: _toBytes(processedArgs[1]),
      );

    case 'zcard':
      return await db.zcard(key: processedArgs[0] as String);

    case 'zcount':
      return await db.zcount(
        key: processedArgs[0] as String,
        minScore: (processedArgs[1] as num).toDouble(),
        maxScore: (processedArgs[2] as num).toDouble(),
      );

    case 'zincrby':
      return await db.zincrby(
        key: processedArgs[0] as String,
        increment: (processedArgs[1] as num).toDouble(),
        member: _toBytes(processedArgs[2]),
      );

    case 'zrange':
      final withScores = processedArgs.length > 3 &&
          processedArgs[3].toString().toUpperCase() == 'WITHSCORES';
      final results = await db.zrange(
        key: processedArgs[0] as String,
        start: (processedArgs[1] as num).toInt(),
        stop: (processedArgs[2] as num).toInt(),
        withScores: withScores,
      );
      if (withScores) {
        final list = <dynamic>[];
        for (final m in results) {
          list.add(utf8.decode(m.member));
          list.add(m.score);
        }
        return list;
      }
      return results.map((m) => utf8.decode(m.member)).toList();

    case 'zrevrange':
      final withScores = processedArgs.length > 3 &&
          processedArgs[3].toString().toUpperCase() == 'WITHSCORES';
      final results = await db.zrevrange(
        key: processedArgs[0] as String,
        start: (processedArgs[1] as num).toInt(),
        stop: (processedArgs[2] as num).toInt(),
        withScores: withScores,
      );
      if (withScores) {
        final list = <dynamic>[];
        for (final m in results) {
          list.add(utf8.decode(m.member));
          list.add(m.score);
        }
        return list;
      }
      return results.map((m) => utf8.decode(m.member)).toList();

    case 'zrank':
      return await db.zrank(
        key: processedArgs[0] as String,
        member: _toBytes(processedArgs[1]),
      );

    case 'zrevrank':
      return await db.zrevrank(
        key: processedArgs[0] as String,
        member: _toBytes(processedArgs[1]),
      );

    default:
      throw UnimplementedError('Command not implemented: $cmd');
  }
}

dynamic _processArg(dynamic arg) {
  if (arg is YamlList) {
    return arg.map(_processArg).toList();
  }
  if (arg is YamlMap) {
    if (arg.containsKey('bytes')) {
      return Uint8List.fromList((arg['bytes'] as List).cast<int>());
    }
    return Map.fromEntries(
      arg.entries.map((e) => MapEntry(_processArg(e.key), _processArg(e.value))),
    );
  }
  return arg;
}

Uint8List _toBytes(dynamic value) {
  if (value is Uint8List) return value;
  if (value is List<int>) return Uint8List.fromList(value);
  if (value is String) return Uint8List.fromList(utf8.encode(value));
  return Uint8List.fromList(utf8.encode(value.toString()));
}

bool _compare(dynamic actual, dynamic expected) {
  if (expected == null) {
    return actual == null;
  }

  if (expected is Map) {
    return _compareSpecial(actual, expected);
  }

  if (expected is bool) {
    if (actual is bool) return actual == expected;
    if (actual is int) return (actual != 0) == expected;
    return false;
  }

  if (expected is int) {
    if (actual is int) return actual == expected;
    if (actual is double) return actual.toInt() == expected;
    return false;
  }

  if (expected is double) {
    if (actual is num) return (actual - expected).abs() < 0.001;
    return false;
  }

  if (expected is String) {
    if (actual is String) return actual == expected;
    if (actual is Uint8List) return utf8.decode(actual) == expected;
    return actual.toString() == expected;
  }

  if (expected is List) {
    if (actual is! List) return false;
    if (actual.length != expected.length) return false;
    for (var i = 0; i < expected.length; i++) {
      if (!_compare(actual[i], expected[i])) return false;
    }
    return true;
  }

  return actual == expected;
}

bool _compareSpecial(dynamic actual, Map expected) {
  if (expected.containsKey('bytes')) {
    final expBytes = Uint8List.fromList((expected['bytes'] as List).cast<int>());
    if (actual is Uint8List) return _bytesEqual(actual, expBytes);
    return false;
  }

  if (expected.containsKey('set')) {
    final expSet = (expected['set'] as List).toSet();
    if (actual is Set) {
      final actualSet = actual.map((v) {
        if (v is Uint8List) return utf8.decode(v);
        return v;
      }).toSet();
      return actualSet.length == expSet.length &&
          actualSet.every((e) => expSet.contains(e));
    }
    if (actual is List) {
      final actualSet = actual.map((v) {
        if (v is Uint8List) return utf8.decode(v);
        return v;
      }).toSet();
      return actualSet.length == expSet.length &&
          actualSet.every((e) => expSet.contains(e));
    }
    return false;
  }

  if (expected.containsKey('dict')) {
    final expDict = expected['dict'] as Map;
    if (actual is! Map) return false;
    if (actual.length != expDict.length) return false;
    for (final entry in expDict.entries) {
      final actualVal = actual[entry.key];
      if (actualVal is Uint8List) {
        if (utf8.decode(actualVal) != entry.value) return false;
      } else if (actualVal != entry.value) {
        return false;
      }
    }
    return true;
  }

  if (expected.containsKey('range')) {
    final range = expected['range'] as List;
    final low = (range[0] as num).toDouble();
    final high = (range[1] as num).toDouble();
    if (actual is num) return actual >= low && actual <= high;
    return false;
  }

  if (expected.containsKey('approx')) {
    final target = (expected['approx'] as num).toDouble();
    final tol = ((expected['tol'] ?? 0.001) as num).toDouble();
    if (actual is num) return (actual - target).abs() <= tol;
    return false;
  }

  if (expected.containsKey('type')) {
    final expType = expected['type'] as String;
    switch (expType) {
      case 'bytes':
        return actual is Uint8List;
      case 'str':
        return actual is String;
      case 'int':
        return actual is int;
      case 'float':
        return actual is double;
      case 'list':
        return actual is List;
      case 'dict':
        return actual is Map;
      case 'set':
        return actual is Set;
      default:
        return false;
    }
  }

  if (expected.containsKey('contains')) {
    final substring = expected['contains'] as String;
    return actual.toString().contains(substring);
  }

  return false;
}

bool _bytesEqual(Uint8List a, Uint8List b) {
  if (a.length != b.length) return false;
  for (var i = 0; i < a.length; i++) {
    if (a[i] != b[i]) return false;
  }
  return true;
}

dynamic _serialize(dynamic value) {
  if (value is Uint8List) {
    try {
      return utf8.decode(value);
    } catch (_) {
      return '<bytes: ${value.toList()}>';
    }
  }
  if (value is List) {
    return value.map(_serialize).toList();
  }
  if (value is Map) {
    return Map.fromEntries(
      value.entries.map((e) => MapEntry(_serialize(e.key), _serialize(e.value))),
    );
  }
  if (value is Set) {
    return value.map(_serialize).toSet();
  }
  return value;
}
