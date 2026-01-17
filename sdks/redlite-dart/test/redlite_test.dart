import 'dart:convert';
import 'dart:typed_data';
import 'package:flutter_test/flutter_test.dart';
import 'package:redlite/src/rust/api.dart';
import 'package:redlite/src/rust/frb_generated.dart';

void main() {
  setUpAll(() async {
    await RustLib.init();
  });

  group('String Commands', () {
    test('set and get', () async {
      final db = Db.openMemory();

      await db.set_(key: 'test_key', value: utf8.encode('hello'));
      final result = await db.get_(key: 'test_key');

      expect(result, isNotNull);
      expect(utf8.decode(result!), equals('hello'));
    });

    test('set with TTL', () async {
      final db = Db.openMemory();

      await db.setex(key: 'ttl_key', seconds: 3600, value: utf8.encode('value'));
      final ttl = await db.ttl(key: 'ttl_key');

      expect(ttl, greaterThan(0));
      expect(ttl, lessThanOrEqualTo(3600));
    });

    test('incr and decr', () async {
      final db = Db.openMemory();

      await db.set_(key: 'counter', value: utf8.encode('10'));

      final after_incr = await db.incr(key: 'counter');
      expect(after_incr, equals(11));

      final after_decr = await db.decr(key: 'counter');
      expect(after_decr, equals(10));
    });

    test('append', () async {
      final db = Db.openMemory();

      await db.set_(key: 'str', value: utf8.encode('Hello'));
      final len = await db.append(key: 'str', value: utf8.encode(' World'));

      expect(len, equals(11));

      final result = await db.get_(key: 'str');
      expect(utf8.decode(result!), equals('Hello World'));
    });
  });

  group('Key Commands', () {
    test('exists and del', () async {
      final db = Db.openMemory();

      await db.set_(key: 'key1', value: utf8.encode('v1'));
      await db.set_(key: 'key2', value: utf8.encode('v2'));

      final exists = await db.exists(keys: ['key1', 'key2', 'key3']);
      expect(exists, equals(2));

      final deleted = await db.del(keys: ['key1']);
      expect(deleted, equals(1));

      final existsAfter = await db.exists(keys: ['key1', 'key2']);
      expect(existsAfter, equals(1));
    });

    test('keys pattern', () async {
      final db = Db.openMemory();

      await db.set_(key: 'user:1', value: utf8.encode('a'));
      await db.set_(key: 'user:2', value: utf8.encode('b'));
      await db.set_(key: 'other:1', value: utf8.encode('c'));

      final userKeys = await db.keys(pattern: 'user:*');
      expect(userKeys.length, equals(2));
      expect(userKeys, containsAll(['user:1', 'user:2']));
    });
  });

  group('Hash Commands', () {
    test('hset and hget', () async {
      final db = Db.openMemory();

      await db.hset(key: 'user', field: 'name', value: utf8.encode('Alice'));
      await db.hset(key: 'user', field: 'age', value: utf8.encode('30'));

      final name = await db.hget(key: 'user', field: 'name');
      final age = await db.hget(key: 'user', field: 'age');

      expect(utf8.decode(name!), equals('Alice'));
      expect(utf8.decode(age!), equals('30'));
    });

    test('hgetall', () async {
      final db = Db.openMemory();

      await db.hset(key: 'hash', field: 'f1', value: utf8.encode('v1'));
      await db.hset(key: 'hash', field: 'f2', value: utf8.encode('v2'));

      final all = await db.hgetall(key: 'hash');
      expect(all.length, equals(2));
    });

    test('hincrby', () async {
      final db = Db.openMemory();

      await db.hset(key: 'counters', field: 'visits', value: utf8.encode('100'));
      final result = await db.hincrby(key: 'counters', field: 'visits', increment: 5);

      expect(result, equals(105));
    });
  });

  group('List Commands', () {
    test('lpush, rpush, lrange', () async {
      final db = Db.openMemory();

      await db.rpush(key: 'list', values: [
        Uint8List.fromList(utf8.encode('a')),
        Uint8List.fromList(utf8.encode('b')),
      ]);
      await db.lpush(key: 'list', values: [
        Uint8List.fromList(utf8.encode('c')),
      ]);

      final items = await db.lrange(key: 'list', start: 0, stop: -1);
      expect(items.length, equals(3));
      expect(utf8.decode(items[0]), equals('c'));
      expect(utf8.decode(items[1]), equals('a'));
      expect(utf8.decode(items[2]), equals('b'));
    });

    test('lpop and rpop', () async {
      final db = Db.openMemory();

      await db.rpush(key: 'q', values: [
        Uint8List.fromList(utf8.encode('1')),
        Uint8List.fromList(utf8.encode('2')),
        Uint8List.fromList(utf8.encode('3')),
      ]);

      final left = await db.lpop(key: 'q');
      expect(utf8.decode(left[0]), equals('1'));

      final right = await db.rpop(key: 'q');
      expect(utf8.decode(right[0]), equals('3'));
    });
  });

  group('Set Commands', () {
    test('sadd, smembers, scard', () async {
      final db = Db.openMemory();

      await db.sadd(key: 'tags', members: [
        Uint8List.fromList(utf8.encode('rust')),
        Uint8List.fromList(utf8.encode('dart')),
        Uint8List.fromList(utf8.encode('flutter')),
      ]);

      final card = await db.scard(key: 'tags');
      expect(card, equals(3));

      final members = await db.smembers(key: 'tags');
      expect(members.length, equals(3));
    });

    test('sismember', () async {
      final db = Db.openMemory();

      await db.sadd(key: 'set', members: [
        Uint8List.fromList(utf8.encode('a')),
      ]);

      final isMember = await db.sismember(key: 'set', member: utf8.encode('a'));
      expect(isMember, isTrue);

      final notMember = await db.sismember(key: 'set', member: utf8.encode('b'));
      expect(notMember, isFalse);
    });
  });

  group('Sorted Set Commands', () {
    test('zadd and zrange', () async {
      final db = Db.openMemory();

      await db.zadd(key: 'leaderboard', members: [
        ZMember(score: 100, member: Uint8List.fromList(utf8.encode('player1'))),
        ZMember(score: 200, member: Uint8List.fromList(utf8.encode('player2'))),
        ZMember(score: 150, member: Uint8List.fromList(utf8.encode('player3'))),
      ]);

      final card = await db.zcard(key: 'leaderboard');
      expect(card, equals(3));

      final top = await db.zrevrange(key: 'leaderboard', start: 0, stop: 1, withScores: true);
      expect(top.length, equals(2));
      expect(utf8.decode(top[0].member), equals('player2'));
      expect(top[0].score, equals(200));
    });

    test('zincrby', () async {
      final db = Db.openMemory();

      await db.zadd(key: 'scores', members: [
        ZMember(score: 10, member: Uint8List.fromList(utf8.encode('p1'))),
      ]);

      final newScore = await db.zincrby(key: 'scores', increment: 5, member: utf8.encode('p1'));
      expect(newScore, equals(15));
    });
  });

  group('Multi-key Commands', () {
    test('mset and mget', () async {
      final db = Db.openMemory();

      await db.mset(pairs: [
        ('k1', Uint8List.fromList(utf8.encode('v1'))),
        ('k2', Uint8List.fromList(utf8.encode('v2'))),
      ]);

      final results = await db.mget(keys: ['k1', 'k2', 'k3']);
      expect(results.length, equals(3));
      expect(results[0], isNotNull);
      expect(results[1], isNotNull);
      expect(results[2], isNull);
    });
  });

  group('Database Commands', () {
    test('dbsize and flushdb', () async {
      final db = Db.openMemory();

      await db.set_(key: 'k1', value: utf8.encode('v1'));
      await db.set_(key: 'k2', value: utf8.encode('v2'));

      final size = await db.dbsize();
      expect(size, equals(2));

      await db.flushdb();

      final sizeAfter = await db.dbsize();
      expect(sizeAfter, equals(0));
    });
  });
}
