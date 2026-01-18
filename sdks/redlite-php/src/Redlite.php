<?php

declare(strict_types=1);

namespace Redlite;

/**
 * Redlite - Redis API + SQLite durability.
 *
 * Embedded Redis-compatible database with microsecond latency.
 *
 * Usage:
 *     $db = new Redlite(':memory:');           // In-memory database
 *     $db = new Redlite('/path/to/db.db');     // File-based database
 *     $db = new Redlite('/path/to/db.db', 100); // With 100MB cache
 *
 *     $db->set('key', 'value');
 *     $db->get('key');  // Returns 'value'
 *
 *     $db->close();
 *
 * @method string|null get(string $key)
 * @method bool set(string $key, string $value, int $ex = 0, int $px = 0, bool $nx = false, bool $xx = false)
 * @method bool setex(string $key, int $seconds, string $value)
 * @method bool psetex(string $key, int $milliseconds, string $value)
 * @method string|null getdel(string $key)
 * @method int append(string $key, string $value)
 * @method int strlen(string $key)
 * @method string|null getrange(string $key, int $start, int $end)
 * @method int setrange(string $key, int $offset, string $value)
 * @method int incr(string $key)
 * @method int decr(string $key)
 * @method int incrby(string $key, int $increment)
 * @method int decrby(string $key, int $decrement)
 * @method float|null incrbyfloat(string $key, float $increment)
 * @method array mget(string ...$keys)
 * @method bool mset(array $pairs)
 * @method int delete(string ...$keys)
 * @method int exists(string ...$keys)
 * @method string|null type(string $key)
 * @method int ttl(string $key)
 * @method int pttl(string $key)
 * @method bool expire(string $key, int $seconds)
 * @method bool pexpire(string $key, int $milliseconds)
 * @method bool expireat(string $key, int $unixSeconds)
 * @method bool pexpireat(string $key, int $unixMs)
 * @method bool persist(string $key)
 * @method bool rename(string $key, string $newkey)
 * @method bool renamenx(string $key, string $newkey)
 * @method array keys(string $pattern)
 * @method int dbsize()
 * @method bool flushdb()
 * @method bool select(int $db)
 * @method int hset(string $key, array $fieldValues)
 * @method string|null hget(string $key, string $field)
 * @method int hdel(string $key, string ...$fields)
 * @method bool hexists(string $key, string $field)
 * @method int hlen(string $key)
 * @method array hkeys(string $key)
 * @method array hvals(string $key)
 * @method int hincrby(string $key, string $field, int $increment)
 * @method array hgetall(string $key)
 * @method array hmget(string $key, string ...$fields)
 * @method int lpush(string $key, string ...$values)
 * @method int rpush(string $key, string ...$values)
 * @method array lpop(string $key, int $count = 1)
 * @method array rpop(string $key, int $count = 1)
 * @method int llen(string $key)
 * @method array lrange(string $key, int $start, int $stop)
 * @method string|null lindex(string $key, int $index)
 * @method int sadd(string $key, string ...$members)
 * @method int srem(string $key, string ...$members)
 * @method array smembers(string $key)
 * @method bool sismember(string $key, string $member)
 * @method int scard(string $key)
 * @method int zadd(string $key, array $members)
 * @method int zrem(string $key, string ...$members)
 * @method float|null zscore(string $key, string $member)
 * @method int zcard(string $key)
 * @method int zcount(string $key, float $min, float $max)
 * @method float zincrby(string $key, float $increment, string $member)
 * @method array zrange(string $key, int $start, int $stop, bool $withscores = false)
 * @method array zrevrange(string $key, int $start, int $stop, bool $withscores = false)
 * @method int vacuum()
 */
class Redlite
{
    private Database $db;
    private bool $closed = false;

    /**
     * Create a new Redlite instance.
     *
     * @param string $url Database path. Use ':memory:' for in-memory database.
     * @param int $cacheMb Cache size in megabytes (only for file-based databases).
     */
    public function __construct(string $url = ':memory:', int $cacheMb = 64)
    {
        if ($url === ':memory:') {
            $this->db = Database::openMemory();
        } elseif ($cacheMb !== 64) {
            $this->db = Database::openWithCache($url, $cacheMb);
        } else {
            $this->db = Database::open($url);
        }
    }

    /**
     * Get the library version.
     */
    public static function version(): string
    {
        return Database::version();
    }

    /**
     * Set the path to the native library.
     * Call this before creating any Redlite instances.
     */
    public static function setLibraryPath(string $path): void
    {
        Database::setLibraryPath($path);
    }

    /**
     * Close the database connection.
     */
    public function close(): void
    {
        if (!$this->closed) {
            $this->db->close();
            $this->closed = true;
        }
    }

    public function __destruct()
    {
        $this->close();
    }

    // -------------------------------------------------------------------------
    // String commands
    // -------------------------------------------------------------------------

    /**
     * Get the value of a key.
     */
    public function get(string $key): ?string
    {
        return $this->db->get($key);
    }

    /**
     * Set the value of a key.
     *
     * @param int $ex Expire time in seconds
     * @param int $px Expire time in milliseconds
     * @param bool $nx Only set if key doesn't exist
     * @param bool $xx Only set if key exists
     */
    public function set(string $key, string $value, int $ex = 0, int $px = 0, bool $nx = false, bool $xx = false): bool
    {
        // Handle NX/XX options
        if ($nx) {
            if ($this->db->exists([$key]) > 0) {
                return false;
            }
        }
        if ($xx) {
            if ($this->db->exists([$key]) === 0) {
                return false;
            }
        }

        // Use millisecond precision if specified
        if ($px > 0) {
            return $this->db->psetex($key, $px, $value);
        }

        return $this->db->set($key, $value, $ex);
    }

    /**
     * Set a key with an expiration time in seconds.
     */
    public function setex(string $key, int $seconds, string $value): bool
    {
        return $this->db->setex($key, $seconds, $value);
    }

    /**
     * Set a key with an expiration time in milliseconds.
     */
    public function psetex(string $key, int $milliseconds, string $value): bool
    {
        return $this->db->psetex($key, $milliseconds, $value);
    }

    /**
     * Get the value of a key and delete it.
     */
    public function getdel(string $key): ?string
    {
        return $this->db->getdel($key);
    }

    /**
     * Append a value to a key.
     */
    public function append(string $key, string $value): int
    {
        return $this->db->append($key, $value);
    }

    /**
     * Get the length of the value stored at a key.
     */
    public function strlen(string $key): int
    {
        return $this->db->strlen($key);
    }

    /**
     * Get a substring of the string stored at a key.
     */
    public function getrange(string $key, int $start, int $end): ?string
    {
        return $this->db->getrange($key, $start, $end);
    }

    /**
     * Overwrite part of a string at key starting at the specified offset.
     */
    public function setrange(string $key, int $offset, string $value): int
    {
        return $this->db->setrange($key, $offset, $value);
    }

    /**
     * Increment the integer value of a key by one.
     */
    public function incr(string $key): int
    {
        return $this->db->incr($key);
    }

    /**
     * Decrement the integer value of a key by one.
     */
    public function decr(string $key): int
    {
        return $this->db->decr($key);
    }

    /**
     * Increment the integer value of a key by the given amount.
     */
    public function incrby(string $key, int $increment): int
    {
        return $this->db->incrby($key, $increment);
    }

    /**
     * Decrement the integer value of a key by the given amount.
     */
    public function decrby(string $key, int $decrement): int
    {
        return $this->db->decrby($key, $decrement);
    }

    /**
     * Increment the float value of a key by the given amount.
     */
    public function incrbyfloat(string $key, float $increment): ?float
    {
        return $this->db->incrbyfloat($key, $increment);
    }

    // -------------------------------------------------------------------------
    // Multi-key commands
    // -------------------------------------------------------------------------

    /**
     * Get the values of all the given keys.
     *
     * @param string ...$keys
     * @return array<string|null>
     */
    public function mget(string ...$keys): array
    {
        return $this->db->mget($keys);
    }

    /**
     * Set multiple keys to multiple values.
     *
     * @param array<string, string> $pairs
     */
    public function mset(array $pairs): bool
    {
        return $this->db->mset($pairs);
    }

    // -------------------------------------------------------------------------
    // Key commands
    // -------------------------------------------------------------------------

    /**
     * Delete one or more keys.
     *
     * @param string ...$keys
     */
    public function delete(string ...$keys): int
    {
        return $this->db->del($keys);
    }

    /**
     * Check if one or more keys exist.
     *
     * @param string ...$keys
     */
    public function exists(string ...$keys): int
    {
        return $this->db->exists($keys);
    }

    /**
     * Get the type of a key.
     */
    public function type(string $key): ?string
    {
        return $this->db->type($key);
    }

    /**
     * Get the time to live for a key in seconds.
     * Returns -2 if the key does not exist.
     * Returns -1 if the key exists but has no associated expire.
     */
    public function ttl(string $key): int
    {
        return $this->db->ttl($key);
    }

    /**
     * Get the time to live for a key in milliseconds.
     */
    public function pttl(string $key): int
    {
        return $this->db->pttl($key);
    }

    /**
     * Set a timeout on a key in seconds.
     */
    public function expire(string $key, int $seconds): bool
    {
        return $this->db->expire($key, $seconds);
    }

    /**
     * Set a timeout on a key in milliseconds.
     */
    public function pexpire(string $key, int $milliseconds): bool
    {
        return $this->db->pexpire($key, $milliseconds);
    }

    /**
     * Set the expiration for a key as a UNIX timestamp in seconds.
     */
    public function expireat(string $key, int $unixSeconds): bool
    {
        return $this->db->expireat($key, $unixSeconds);
    }

    /**
     * Set the expiration for a key as a UNIX timestamp in milliseconds.
     */
    public function pexpireat(string $key, int $unixMs): bool
    {
        return $this->db->pexpireat($key, $unixMs);
    }

    /**
     * Remove the expiration from a key.
     */
    public function persist(string $key): bool
    {
        return $this->db->persist($key);
    }

    /**
     * Rename a key.
     */
    public function rename(string $key, string $newkey): bool
    {
        return $this->db->rename($key, $newkey);
    }

    /**
     * Rename a key, only if the new key does not exist.
     */
    public function renamenx(string $key, string $newkey): bool
    {
        return $this->db->renamenx($key, $newkey);
    }

    /**
     * Find all keys matching the given pattern.
     *
     * @return string[]
     */
    public function keys(string $pattern): array
    {
        return $this->db->keys($pattern);
    }

    /**
     * Return the number of keys in the currently-selected database.
     */
    public function dbsize(): int
    {
        return $this->db->dbsize();
    }

    /**
     * Remove all keys from the current database.
     */
    public function flushdb(): bool
    {
        return $this->db->flushdb();
    }

    /**
     * Select the database to use.
     */
    public function select(int $db): bool
    {
        return $this->db->select($db);
    }

    // -------------------------------------------------------------------------
    // Hash commands
    // -------------------------------------------------------------------------

    /**
     * Set field in the hash stored at key to value.
     *
     * @param array<string, string> $fieldValues
     */
    public function hset(string $key, array $fieldValues): int
    {
        return $this->db->hset($key, $fieldValues);
    }

    /**
     * Get the value of a hash field.
     */
    public function hget(string $key, string $field): ?string
    {
        return $this->db->hget($key, $field);
    }

    /**
     * Delete one or more hash fields.
     *
     * @param string ...$fields
     */
    public function hdel(string $key, string ...$fields): int
    {
        return $this->db->hdel($key, $fields);
    }

    /**
     * Determine if a hash field exists.
     */
    public function hexists(string $key, string $field): bool
    {
        return $this->db->hexists($key, $field);
    }

    /**
     * Get the number of fields in a hash.
     */
    public function hlen(string $key): int
    {
        return $this->db->hlen($key);
    }

    /**
     * Get all the fields in a hash.
     *
     * @return string[]
     */
    public function hkeys(string $key): array
    {
        return $this->db->hkeys($key);
    }

    /**
     * Get all the values in a hash.
     *
     * @return string[]
     */
    public function hvals(string $key): array
    {
        return $this->db->hvals($key);
    }

    /**
     * Increment the integer value of a hash field.
     */
    public function hincrby(string $key, string $field, int $increment): int
    {
        return $this->db->hincrby($key, $field, $increment);
    }

    /**
     * Get all the fields and values in a hash.
     *
     * @return array<string, string>
     */
    public function hgetall(string $key): array
    {
        return $this->db->hgetall($key);
    }

    /**
     * Get the values of all the given hash fields.
     *
     * @param string ...$fields
     * @return array<string|null>
     */
    public function hmget(string $key, string ...$fields): array
    {
        return $this->db->hmget($key, $fields);
    }

    // -------------------------------------------------------------------------
    // List commands
    // -------------------------------------------------------------------------

    /**
     * Prepend one or more values to a list.
     *
     * @param string ...$values
     */
    public function lpush(string $key, string ...$values): int
    {
        return $this->db->lpush($key, $values);
    }

    /**
     * Append one or more values to a list.
     *
     * @param string ...$values
     */
    public function rpush(string $key, string ...$values): int
    {
        return $this->db->rpush($key, $values);
    }

    /**
     * Remove and get the first element(s) in a list.
     *
     * @return string[]
     */
    public function lpop(string $key, int $count = 1): array
    {
        return $this->db->lpop($key, $count);
    }

    /**
     * Remove and get the last element(s) in a list.
     *
     * @return string[]
     */
    public function rpop(string $key, int $count = 1): array
    {
        return $this->db->rpop($key, $count);
    }

    /**
     * Get the length of a list.
     */
    public function llen(string $key): int
    {
        return $this->db->llen($key);
    }

    /**
     * Get a range of elements from a list.
     *
     * @return string[]
     */
    public function lrange(string $key, int $start, int $stop): array
    {
        return $this->db->lrange($key, $start, $stop);
    }

    /**
     * Get an element from a list by its index.
     */
    public function lindex(string $key, int $index): ?string
    {
        return $this->db->lindex($key, $index);
    }

    /**
     * Prepend values to a list only if the key exists.
     *
     * @param string ...$values
     */
    public function lpushx(string $key, string ...$values): int
    {
        return $this->db->lpushx($key, $values);
    }

    /**
     * Append values to a list only if the key exists.
     *
     * @param string ...$values
     */
    public function rpushx(string $key, string ...$values): int
    {
        return $this->db->rpushx($key, $values);
    }

    /**
     * Atomically move an element from source list to destination list.
     *
     * @param string $wherefrom "LEFT" or "RIGHT"
     * @param string $whereto "LEFT" or "RIGHT"
     */
    public function lmove(string $source, string $destination, string $wherefrom, string $whereto): ?string
    {
        return $this->db->lmove($source, $destination, $wherefrom, $whereto);
    }

    /**
     * Find the position(s) of an element in a list.
     *
     * @return int[]
     */
    public function lpos(string $key, string $element, int $rank = 0, int $count = 0, int $maxlen = 0): array
    {
        return $this->db->lpos($key, $element, $rank, $count, $maxlen);
    }

    // -------------------------------------------------------------------------
    // Set commands
    // -------------------------------------------------------------------------

    /**
     * Add one or more members to a set.
     *
     * @param string ...$members
     */
    public function sadd(string $key, string ...$members): int
    {
        return $this->db->sadd($key, $members);
    }

    /**
     * Remove one or more members from a set.
     *
     * @param string ...$members
     */
    public function srem(string $key, string ...$members): int
    {
        return $this->db->srem($key, $members);
    }

    /**
     * Get all the members in a set.
     *
     * @return string[]
     */
    public function smembers(string $key): array
    {
        return $this->db->smembers($key);
    }

    /**
     * Determine if a given value is a member of a set.
     */
    public function sismember(string $key, string $member): bool
    {
        return $this->db->sismember($key, $member);
    }

    /**
     * Get the number of members in a set.
     */
    public function scard(string $key): int
    {
        return $this->db->scard($key);
    }

    // -------------------------------------------------------------------------
    // Sorted set commands
    // -------------------------------------------------------------------------

    /**
     * Add one or more members to a sorted set.
     *
     * @param array<string, float> $members member => score
     */
    public function zadd(string $key, array $members): int
    {
        return $this->db->zadd($key, $members);
    }

    /**
     * Remove one or more members from a sorted set.
     *
     * @param string ...$members
     */
    public function zrem(string $key, string ...$members): int
    {
        return $this->db->zrem($key, $members);
    }

    /**
     * Get the score of a member in a sorted set.
     */
    public function zscore(string $key, string $member): ?float
    {
        return $this->db->zscore($key, $member);
    }

    /**
     * Get the number of members in a sorted set.
     */
    public function zcard(string $key): int
    {
        return $this->db->zcard($key);
    }

    /**
     * Count the members in a sorted set with scores within the given values.
     */
    public function zcount(string $key, float $min, float $max): int
    {
        return $this->db->zcount($key, $min, $max);
    }

    /**
     * Increment the score of a member in a sorted set.
     */
    public function zincrby(string $key, float $increment, string $member): float
    {
        return $this->db->zincrby($key, $increment, $member);
    }

    /**
     * Return a range of members in a sorted set, by index.
     *
     * @return string[]|array<string, float>
     */
    public function zrange(string $key, int $start, int $stop, bool $withscores = false): array
    {
        return $this->db->zrange($key, $start, $stop, $withscores);
    }

    /**
     * Return a range of members in a sorted set, by index, with scores ordered from high to low.
     *
     * @return string[]|array<string, float>
     */
    public function zrevrange(string $key, int $start, int $stop, bool $withscores = false): array
    {
        return $this->db->zrevrange($key, $start, $stop, $withscores);
    }

    /**
     * Intersect sorted sets and store the result in a new key.
     *
     * @param string[] $keys Source keys
     * @param float[]|null $weights Optional weights for each key
     * @param string|null $aggregate "SUM", "MIN", or "MAX"
     */
    public function zinterstore(string $destination, array $keys, ?array $weights = null, ?string $aggregate = null): int
    {
        return $this->db->zinterstore($destination, $keys, $weights, $aggregate);
    }

    /**
     * Union sorted sets and store the result in a new key.
     *
     * @param string[] $keys Source keys
     * @param float[]|null $weights Optional weights for each key
     * @param string|null $aggregate "SUM", "MIN", or "MAX"
     */
    public function zunionstore(string $destination, array $keys, ?array $weights = null, ?string $aggregate = null): int
    {
        return $this->db->zunionstore($destination, $keys, $weights, $aggregate);
    }

    // -------------------------------------------------------------------------
    // Utility commands
    // -------------------------------------------------------------------------

    /**
     * Compact the database.
     */
    public function vacuum(): int
    {
        return $this->db->vacuum();
    }

    // -------------------------------------------------------------------------
    // JSON commands (ReJSON-compatible)
    // -------------------------------------------------------------------------

    /**
     * Set a JSON value at a path.
     *
     * @param bool $nx Only set if key does not exist
     * @param bool $xx Only set if key already exists
     */
    public function jsonSet(string $key, string $path, string $value, bool $nx = false, bool $xx = false): bool
    {
        return $this->db->jsonSet($key, $path, $value, $nx, $xx);
    }

    /**
     * Get JSON value(s) at path(s).
     *
     * @param string ...$paths Paths to get (defaults to "$" if empty)
     */
    public function jsonGet(string $key, string ...$paths): ?string
    {
        return $this->db->jsonGet($key, $paths);
    }

    /**
     * Delete JSON value at path.
     *
     * @return int Number of paths deleted
     */
    public function jsonDel(string $key, ?string $path = null): int
    {
        return $this->db->jsonDel($key, $path);
    }

    /**
     * Get the type of JSON value at path.
     */
    public function jsonType(string $key, ?string $path = null): ?string
    {
        return $this->db->jsonType($key, $path);
    }

    /**
     * Increment numeric value at path.
     *
     * @return string|null New value as string
     */
    public function jsonNumIncrBy(string $key, string $path, float $increment): ?string
    {
        return $this->db->jsonNumIncrBy($key, $path, $increment);
    }

    /**
     * Append to JSON string at path.
     *
     * @return int New string length
     */
    public function jsonStrAppend(string $key, string $path, string $value): int
    {
        return $this->db->jsonStrAppend($key, $path, $value);
    }

    /**
     * Get length of JSON string at path.
     */
    public function jsonStrLen(string $key, ?string $path = null): int
    {
        return $this->db->jsonStrLen($key, $path);
    }

    /**
     * Append values to JSON array.
     *
     * @param string[] $values JSON-encoded values to append
     * @return int New array length
     */
    public function jsonArrAppend(string $key, string $path, array $values): int
    {
        return $this->db->jsonArrAppend($key, $path, $values);
    }

    /**
     * Get length of JSON array at path.
     */
    public function jsonArrLen(string $key, ?string $path = null): int
    {
        return $this->db->jsonArrLen($key, $path);
    }

    /**
     * Pop element from JSON array.
     *
     * @param int $index Index to pop from (-1 = last element)
     * @return string|null Popped element as JSON string
     */
    public function jsonArrPop(string $key, ?string $path = null, int $index = -1): ?string
    {
        return $this->db->jsonArrPop($key, $path, $index);
    }

    /**
     * Clear container values (arrays/objects).
     *
     * @return int Number of containers cleared
     */
    public function jsonClear(string $key, ?string $path = null): int
    {
        return $this->db->jsonClear($key, $path);
    }

    // -------------------------------------------------------------------------
    // History commands
    // -------------------------------------------------------------------------

    /**
     * Enable history tracking globally.
     *
     * @param string $retentionType "unlimited", "time", or "count"
     * @param int $retentionValue Value for time (ms) or count retention
     */
    public function historyEnableGlobal(string $retentionType = 'unlimited', int $retentionValue = 0): bool
    {
        return $this->db->historyEnableGlobal($retentionType, $retentionValue);
    }

    /**
     * Enable history tracking for a specific database.
     */
    public function historyEnableDatabase(int $dbNum, string $retentionType = 'unlimited', int $retentionValue = 0): bool
    {
        return $this->db->historyEnableDatabase($dbNum, $retentionType, $retentionValue);
    }

    /**
     * Enable history tracking for a specific key.
     */
    public function historyEnableKey(string $key, string $retentionType = 'unlimited', int $retentionValue = 0): bool
    {
        return $this->db->historyEnableKey($key, $retentionType, $retentionValue);
    }

    /**
     * Disable history tracking globally.
     */
    public function historyDisableGlobal(): bool
    {
        return $this->db->historyDisableGlobal();
    }

    /**
     * Disable history tracking for a specific database.
     */
    public function historyDisableDatabase(int $dbNum): bool
    {
        return $this->db->historyDisableDatabase($dbNum);
    }

    /**
     * Disable history tracking for a specific key.
     */
    public function historyDisableKey(string $key): bool
    {
        return $this->db->historyDisableKey($key);
    }

    /**
     * Check if history tracking is enabled for a key.
     */
    public function isHistoryEnabled(string $key): bool
    {
        return $this->db->isHistoryEnabled($key);
    }

    // -------------------------------------------------------------------------
    // FTS (Full-Text Search) commands
    // -------------------------------------------------------------------------

    /**
     * Enable full-text search globally.
     */
    public function ftsEnableGlobal(): bool
    {
        return $this->db->ftsEnableGlobal();
    }

    /**
     * Enable full-text search for a specific database.
     */
    public function ftsEnableDatabase(int $dbNum): bool
    {
        return $this->db->ftsEnableDatabase($dbNum);
    }

    /**
     * Enable full-text search for keys matching a pattern.
     */
    public function ftsEnablePattern(string $pattern): bool
    {
        return $this->db->ftsEnablePattern($pattern);
    }

    /**
     * Enable full-text search for a specific key.
     */
    public function ftsEnableKey(string $key): bool
    {
        return $this->db->ftsEnableKey($key);
    }

    /**
     * Disable full-text search globally.
     */
    public function ftsDisableGlobal(): bool
    {
        return $this->db->ftsDisableGlobal();
    }

    /**
     * Disable full-text search for a specific database.
     */
    public function ftsDisableDatabase(int $dbNum): bool
    {
        return $this->db->ftsDisableDatabase($dbNum);
    }

    /**
     * Disable full-text search for keys matching a pattern.
     */
    public function ftsDisablePattern(string $pattern): bool
    {
        return $this->db->ftsDisablePattern($pattern);
    }

    /**
     * Disable full-text search for a specific key.
     */
    public function ftsDisableKey(string $key): bool
    {
        return $this->db->ftsDisableKey($key);
    }

    /**
     * Check if full-text search is enabled for a key.
     */
    public function isFtsEnabled(string $key): bool
    {
        return $this->db->isFtsEnabled($key);
    }

    // -------------------------------------------------------------------------
    // KeyInfo command
    // -------------------------------------------------------------------------

    /**
     * Get detailed information about a key.
     *
     * @return array{type: string, ttl: int, created_at: int, updated_at: int}|null
     */
    public function keyinfo(string $key): ?array
    {
        return $this->db->keyinfo($key);
    }
}
