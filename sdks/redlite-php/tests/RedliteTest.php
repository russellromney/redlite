<?php

declare(strict_types=1);

namespace Redlite\Tests;

use PHPUnit\Framework\TestCase;
use Redlite\Redlite;

/**
 * @requires extension ffi
 */
class RedliteTest extends TestCase
{
    private Redlite $db;

    protected function setUp(): void
    {
        $this->db = new Redlite(':memory:');
    }

    protected function tearDown(): void
    {
        $this->db->close();
    }

    // -------------------------------------------------------------------------
    // String commands
    // -------------------------------------------------------------------------

    public function testGetSetBasic(): void
    {
        $this->assertTrue($this->db->set('key', 'value'));
        $this->assertEquals('value', $this->db->get('key'));
    }

    public function testGetNonExistent(): void
    {
        $this->assertNull($this->db->get('nonexistent'));
    }

    public function testSetWithExpiry(): void
    {
        $this->assertTrue($this->db->set('key', 'value', 10));
        $this->assertEquals('value', $this->db->get('key'));
        $ttl = $this->db->ttl('key');
        $this->assertGreaterThan(0, $ttl);
        $this->assertLessThanOrEqual(10, $ttl);
    }

    public function testSetex(): void
    {
        $this->assertTrue($this->db->setex('key', 10, 'value'));
        $this->assertEquals('value', $this->db->get('key'));
        $ttl = $this->db->ttl('key');
        $this->assertGreaterThan(0, $ttl);
        $this->assertLessThanOrEqual(10, $ttl);
    }

    public function testGetdel(): void
    {
        $this->db->set('key', 'value');
        $this->assertEquals('value', $this->db->getdel('key'));
        $this->assertNull($this->db->get('key'));
    }

    public function testAppend(): void
    {
        $this->db->set('key', 'Hello');
        $len = $this->db->append('key', ' World');
        $this->assertEquals(11, $len);
        $this->assertEquals('Hello World', $this->db->get('key'));
    }

    public function testStrlen(): void
    {
        $this->db->set('key', 'Hello World');
        $this->assertEquals(11, $this->db->strlen('key'));
        $this->assertEquals(0, $this->db->strlen('nonexistent'));
    }

    public function testGetrange(): void
    {
        $this->db->set('key', 'Hello World');
        $this->assertEquals('World', $this->db->getrange('key', 6, 10));
        $this->assertEquals('Hello', $this->db->getrange('key', 0, 4));
    }

    public function testSetrange(): void
    {
        $this->db->set('key', 'Hello World');
        $len = $this->db->setrange('key', 6, 'PHP');
        $this->assertEquals(11, $len);
        $this->assertEquals('Hello PHPld', $this->db->get('key'));
    }

    public function testIncr(): void
    {
        $this->db->set('counter', '10');
        $this->assertEquals(11, $this->db->incr('counter'));
        $this->assertEquals(12, $this->db->incr('counter'));
    }

    public function testDecr(): void
    {
        $this->db->set('counter', '10');
        $this->assertEquals(9, $this->db->decr('counter'));
        $this->assertEquals(8, $this->db->decr('counter'));
    }

    public function testIncrby(): void
    {
        $this->db->set('counter', '10');
        $this->assertEquals(15, $this->db->incrby('counter', 5));
    }

    public function testDecrby(): void
    {
        $this->db->set('counter', '10');
        $this->assertEquals(7, $this->db->decrby('counter', 3));
    }

    public function testIncrbyfloat(): void
    {
        $this->db->set('counter', '10.5');
        $result = $this->db->incrbyfloat('counter', 0.1);
        $this->assertEqualsWithDelta(10.6, $result, 0.001);
    }

    // -------------------------------------------------------------------------
    // Multi-key commands
    // -------------------------------------------------------------------------

    public function testMget(): void
    {
        $this->db->set('key1', 'value1');
        $this->db->set('key2', 'value2');

        $values = $this->db->mget('key1', 'key2', 'key3');
        $this->assertEquals(['value1', 'value2', null], $values);
    }

    public function testMset(): void
    {
        $this->assertTrue($this->db->mset([
            'key1' => 'value1',
            'key2' => 'value2',
        ]));

        $this->assertEquals('value1', $this->db->get('key1'));
        $this->assertEquals('value2', $this->db->get('key2'));
    }

    // -------------------------------------------------------------------------
    // Key commands
    // -------------------------------------------------------------------------

    public function testDelete(): void
    {
        $this->db->set('key1', 'value1');
        $this->db->set('key2', 'value2');

        $this->assertEquals(2, $this->db->delete('key1', 'key2'));
        $this->assertNull($this->db->get('key1'));
        $this->assertNull($this->db->get('key2'));
    }

    public function testExists(): void
    {
        $this->db->set('key1', 'value1');
        $this->db->set('key2', 'value2');

        $this->assertEquals(2, $this->db->exists('key1', 'key2', 'key3'));
        $this->assertEquals(0, $this->db->exists('nonexistent'));
    }

    public function testType(): void
    {
        $this->db->set('string', 'value');
        $this->db->lpush('list', 'item');
        $this->db->sadd('set', 'member');
        $this->db->hset('hash', ['field' => 'value']);
        $this->db->zadd('zset', ['member' => 1.0]);

        $this->assertEquals('string', $this->db->type('string'));
        $this->assertEquals('list', $this->db->type('list'));
        $this->assertEquals('set', $this->db->type('set'));
        $this->assertEquals('hash', $this->db->type('hash'));
        $this->assertEquals('zset', $this->db->type('zset'));
        $this->assertNull($this->db->type('nonexistent'));
    }

    public function testTtlPttl(): void
    {
        $this->db->set('key', 'value');
        $this->assertEquals(-1, $this->db->ttl('key')); // No TTL set
        $this->assertEquals(-2, $this->db->ttl('nonexistent'));

        $this->db->expire('key', 100);
        $ttl = $this->db->ttl('key');
        $this->assertGreaterThan(0, $ttl);
        $this->assertLessThanOrEqual(100, $ttl);
    }

    public function testExpirePersist(): void
    {
        $this->db->set('key', 'value');
        $this->assertTrue($this->db->expire('key', 100));
        $this->assertGreaterThan(0, $this->db->ttl('key'));

        $this->assertTrue($this->db->persist('key'));
        $this->assertEquals(-1, $this->db->ttl('key'));
    }

    public function testRename(): void
    {
        $this->db->set('key', 'value');
        $this->assertTrue($this->db->rename('key', 'newkey'));
        $this->assertNull($this->db->get('key'));
        $this->assertEquals('value', $this->db->get('newkey'));
    }

    public function testRenamenx(): void
    {
        $this->db->set('key', 'value');
        $this->db->set('existing', 'other');

        $this->assertTrue($this->db->renamenx('key', 'newkey'));
        $this->assertNull($this->db->get('key'));

        $this->db->set('key2', 'value2');
        $this->assertFalse($this->db->renamenx('key2', 'existing'));
    }

    public function testKeys(): void
    {
        $this->db->set('user:1', 'alice');
        $this->db->set('user:2', 'bob');
        $this->db->set('session:1', 'data');

        $keys = $this->db->keys('user:*');
        $this->assertCount(2, $keys);
        $this->assertContains('user:1', $keys);
        $this->assertContains('user:2', $keys);

        $this->assertCount(3, $this->db->keys('*'));
    }

    public function testDbsize(): void
    {
        $this->assertEquals(0, $this->db->dbsize());

        $this->db->set('key1', 'value1');
        $this->db->set('key2', 'value2');

        $this->assertEquals(2, $this->db->dbsize());
    }

    public function testFlushdb(): void
    {
        $this->db->set('key1', 'value1');
        $this->db->set('key2', 'value2');

        $this->assertTrue($this->db->flushdb());
        $this->assertEquals(0, $this->db->dbsize());
    }

    // -------------------------------------------------------------------------
    // Hash commands
    // -------------------------------------------------------------------------

    public function testHsetHget(): void
    {
        $this->db->hset('hash', ['field1' => 'value1', 'field2' => 'value2']);
        $this->assertEquals('value1', $this->db->hget('hash', 'field1'));
        $this->assertEquals('value2', $this->db->hget('hash', 'field2'));
        $this->assertNull($this->db->hget('hash', 'nonexistent'));
    }

    public function testHdel(): void
    {
        $this->db->hset('hash', ['f1' => 'v1', 'f2' => 'v2', 'f3' => 'v3']);
        $this->assertEquals(2, $this->db->hdel('hash', 'f1', 'f2'));
        $this->assertNull($this->db->hget('hash', 'f1'));
        $this->assertEquals('v3', $this->db->hget('hash', 'f3'));
    }

    public function testHexists(): void
    {
        $this->db->hset('hash', ['field' => 'value']);
        $this->assertTrue($this->db->hexists('hash', 'field'));
        $this->assertFalse($this->db->hexists('hash', 'nonexistent'));
    }

    public function testHlen(): void
    {
        $this->db->hset('hash', ['f1' => 'v1', 'f2' => 'v2']);
        $this->assertEquals(2, $this->db->hlen('hash'));
        $this->assertEquals(0, $this->db->hlen('nonexistent'));
    }

    public function testHkeysHvals(): void
    {
        $this->db->hset('hash', ['f1' => 'v1', 'f2' => 'v2']);

        $keys = $this->db->hkeys('hash');
        $this->assertCount(2, $keys);
        $this->assertContains('f1', $keys);
        $this->assertContains('f2', $keys);

        $vals = $this->db->hvals('hash');
        $this->assertCount(2, $vals);
        $this->assertContains('v1', $vals);
        $this->assertContains('v2', $vals);
    }

    public function testHincrby(): void
    {
        $this->db->hset('hash', ['counter' => '10']);
        $this->assertEquals(15, $this->db->hincrby('hash', 'counter', 5));
        $this->assertEquals('15', $this->db->hget('hash', 'counter'));
    }

    public function testHgetall(): void
    {
        $this->db->hset('hash', ['f1' => 'v1', 'f2' => 'v2']);
        $all = $this->db->hgetall('hash');

        $this->assertCount(2, $all);
        $this->assertEquals('v1', $all['f1']);
        $this->assertEquals('v2', $all['f2']);
    }

    public function testHmget(): void
    {
        $this->db->hset('hash', ['f1' => 'v1', 'f2' => 'v2']);
        $values = $this->db->hmget('hash', 'f1', 'f3', 'f2');

        $this->assertEquals(['v1', null, 'v2'], $values);
    }

    // -------------------------------------------------------------------------
    // List commands
    // -------------------------------------------------------------------------

    public function testLpushRpush(): void
    {
        $this->assertEquals(2, $this->db->lpush('list', 'b', 'a'));
        $this->assertEquals(4, $this->db->rpush('list', 'c', 'd'));

        $this->assertEquals(['a', 'b', 'c', 'd'], $this->db->lrange('list', 0, -1));
    }

    public function testLpopRpop(): void
    {
        $this->db->rpush('list', 'a', 'b', 'c', 'd');

        $this->assertEquals(['a'], $this->db->lpop('list'));
        $this->assertEquals(['d'], $this->db->rpop('list'));
        $this->assertEquals(['b', 'c'], $this->db->lrange('list', 0, -1));
    }

    public function testLpopRpopCount(): void
    {
        $this->db->rpush('list', 'a', 'b', 'c', 'd');

        $this->assertEquals(['a', 'b'], $this->db->lpop('list', 2));
        $this->assertEquals(['c', 'd'], $this->db->lrange('list', 0, -1));
    }

    public function testLlen(): void
    {
        $this->db->rpush('list', 'a', 'b', 'c');
        $this->assertEquals(3, $this->db->llen('list'));
        $this->assertEquals(0, $this->db->llen('nonexistent'));
    }

    public function testLrange(): void
    {
        $this->db->rpush('list', 'a', 'b', 'c', 'd', 'e');

        $this->assertEquals(['b', 'c', 'd'], $this->db->lrange('list', 1, 3));
        $this->assertEquals(['d', 'e'], $this->db->lrange('list', -2, -1));
    }

    public function testLindex(): void
    {
        $this->db->rpush('list', 'a', 'b', 'c');

        $this->assertEquals('a', $this->db->lindex('list', 0));
        $this->assertEquals('c', $this->db->lindex('list', 2));
        $this->assertEquals('c', $this->db->lindex('list', -1));
        $this->assertNull($this->db->lindex('list', 10));
    }

    // -------------------------------------------------------------------------
    // Set commands
    // -------------------------------------------------------------------------

    public function testSaddSmembers(): void
    {
        $this->assertEquals(3, $this->db->sadd('set', 'a', 'b', 'c'));
        $this->assertEquals(1, $this->db->sadd('set', 'c', 'd')); // c is duplicate

        $members = $this->db->smembers('set');
        $this->assertCount(4, $members);
        $this->assertContains('a', $members);
        $this->assertContains('b', $members);
        $this->assertContains('c', $members);
        $this->assertContains('d', $members);
    }

    public function testSrem(): void
    {
        $this->db->sadd('set', 'a', 'b', 'c');
        $this->assertEquals(2, $this->db->srem('set', 'a', 'b', 'nonexistent'));

        $members = $this->db->smembers('set');
        $this->assertEquals(['c'], $members);
    }

    public function testSismember(): void
    {
        $this->db->sadd('set', 'a', 'b');
        $this->assertTrue($this->db->sismember('set', 'a'));
        $this->assertFalse($this->db->sismember('set', 'c'));
    }

    public function testScard(): void
    {
        $this->db->sadd('set', 'a', 'b', 'c');
        $this->assertEquals(3, $this->db->scard('set'));
        $this->assertEquals(0, $this->db->scard('nonexistent'));
    }

    // -------------------------------------------------------------------------
    // Sorted set commands
    // -------------------------------------------------------------------------

    public function testZaddZrange(): void
    {
        $this->assertEquals(3, $this->db->zadd('zset', [
            'a' => 1.0,
            'b' => 2.0,
            'c' => 3.0,
        ]));

        $this->assertEquals(['a', 'b', 'c'], $this->db->zrange('zset', 0, -1));
    }

    public function testZrangeWithScores(): void
    {
        $this->db->zadd('zset', ['a' => 1.0, 'b' => 2.0]);

        $result = $this->db->zrange('zset', 0, -1, true);
        $this->assertEquals(['a' => 1.0, 'b' => 2.0], $result);
    }

    public function testZrem(): void
    {
        $this->db->zadd('zset', ['a' => 1.0, 'b' => 2.0, 'c' => 3.0]);
        $this->assertEquals(2, $this->db->zrem('zset', 'a', 'c'));

        $this->assertEquals(['b'], $this->db->zrange('zset', 0, -1));
    }

    public function testZscore(): void
    {
        $this->db->zadd('zset', ['a' => 1.5, 'b' => 2.5]);

        $this->assertEquals(1.5, $this->db->zscore('zset', 'a'));
        $this->assertNull($this->db->zscore('zset', 'nonexistent'));
    }

    public function testZcard(): void
    {
        $this->db->zadd('zset', ['a' => 1.0, 'b' => 2.0]);
        $this->assertEquals(2, $this->db->zcard('zset'));
        $this->assertEquals(0, $this->db->zcard('nonexistent'));
    }

    public function testZcount(): void
    {
        $this->db->zadd('zset', ['a' => 1.0, 'b' => 2.0, 'c' => 3.0, 'd' => 4.0]);
        $this->assertEquals(2, $this->db->zcount('zset', 2.0, 3.0));
    }

    public function testZincrby(): void
    {
        $this->db->zadd('zset', ['a' => 1.0]);
        $result = $this->db->zincrby('zset', 2.5, 'a');
        $this->assertEqualsWithDelta(3.5, $result, 0.001);
    }

    public function testZrevrange(): void
    {
        $this->db->zadd('zset', ['a' => 1.0, 'b' => 2.0, 'c' => 3.0]);
        $this->assertEquals(['c', 'b', 'a'], $this->db->zrevrange('zset', 0, -1));
    }
}
