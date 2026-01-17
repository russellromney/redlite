package com.redlite

import com.redlite.namespaces.FTSNamespace
import com.redlite.namespaces.GeoNamespace
import com.redlite.namespaces.HistoryNamespace
import com.redlite.namespaces.VectorNamespace
import redis.clients.jedis.Jedis
import redis.clients.jedis.JedisPool
import redis.clients.jedis.params.SetParams
import java.io.Closeable

/**
 * Unified Redlite client supporting both embedded and server modes.
 *
 * **Embedded mode** (JNI native bindings, no network, microsecond latency):
 * ```kotlin
 * val db = Redlite(":memory:")
 * val db = Redlite("/path/to/db.db")
 * ```
 *
 * **Server mode** (wraps Jedis Redis client):
 * ```kotlin
 * val db = Redlite("redis://localhost:6379")
 * ```
 *
 * Both modes expose the same API. Embedded mode is faster (microsecond latency),
 * server mode connects to a running redlite or Redis server.
 */
class Redlite(
    url: String = ":memory:",
    cacheMb: Int = 64
) : Closeable {

    /**
     * Connection mode: "embedded" or "server"
     */
    val mode: String

    private var native: EmbeddedDb? = null
    private var jedisPool: JedisPool? = null

    /**
     * Full-text search namespace
     */
    val fts: FTSNamespace

    /**
     * Vector search namespace
     */
    val vector: VectorNamespace

    /**
     * Geospatial namespace
     */
    val geo: GeoNamespace

    /**
     * Version history namespace
     */
    val history: HistoryNamespace

    init {
        mode = when {
            url.startsWith("redis://") || url.startsWith("rediss://") -> {
                jedisPool = JedisPool(url)
                "server"
            }
            else -> {
                native = if (url == ":memory:") {
                    EmbeddedDb.openMemory()
                } else {
                    EmbeddedDb.openWithCache(url, cacheMb)
                }
                "embedded"
            }
        }

        fts = FTSNamespace(this)
        vector = VectorNamespace(this)
        geo = GeoNamespace(this)
        history = HistoryNamespace(this)
    }

    override fun close() {
        native?.close()
        native = null
        jedisPool?.close()
        jedisPool = null
    }

    private fun checkOpen() {
        if (mode == "embedded" && native == null) {
            throw RedliteException("Database is closed")
        }
        if (mode == "server" && jedisPool == null) {
            throw RedliteException("Connection is closed")
        }
    }

    private inline fun <T> withJedis(block: (Jedis) -> T): T {
        val pool = jedisPool ?: throw RedliteException("Connection is closed")
        return pool.resource.use { jedis -> block(jedis) }
    }

    private fun encodeValue(value: Any): ByteArray = when (value) {
        is ByteArray -> value
        is String -> value.toByteArray()
        else -> value.toString().toByteArray()
    }

    // =========================================================================
    // String Commands
    // =========================================================================

    /**
     * Get the value of a key.
     */
    fun get(key: String): ByteArray? {
        checkOpen()
        return if (mode == "server") {
            withJedis { it.get(key.toByteArray()) }
        } else {
            native!!.get(key)
        }
    }

    /**
     * Set the value of a key.
     */
    fun set(key: String, value: ByteArray, options: SetOptions? = null): Boolean {
        checkOpen()
        return if (mode == "server") {
            withJedis { jedis ->
                if (options != null) {
                    val params = SetParams()
                    options.ex?.let { params.ex(it) }
                    options.px?.let { params.px(it) }
                    if (options.nx) params.nx()
                    if (options.xx) params.xx()
                    jedis.set(key.toByteArray(), value, params) != null
                } else {
                    jedis.set(key.toByteArray(), value) == "OK"
                }
            }
        } else {
            if (options != null) {
                native!!.setOpts(key, value, options)
            } else {
                native!!.set(key, value)
            }
        }
    }

    /**
     * Set the value of a key from a string.
     */
    fun set(key: String, value: String, options: SetOptions? = null): Boolean =
        set(key, value.toByteArray(), options)

    /**
     * Set key with expiration in seconds.
     */
    fun setex(key: String, seconds: Long, value: ByteArray): Boolean {
        checkOpen()
        return if (mode == "server") {
            withJedis { it.setex(key.toByteArray(), seconds, value) == "OK" }
        } else {
            native!!.setex(key, seconds, value)
        }
    }

    /**
     * Set key with expiration in milliseconds.
     */
    fun psetex(key: String, milliseconds: Long, value: ByteArray): Boolean {
        checkOpen()
        return if (mode == "server") {
            withJedis { it.psetex(key.toByteArray(), milliseconds, value) == "OK" }
        } else {
            native!!.psetex(key, milliseconds, value)
        }
    }

    /**
     * Get and delete a key.
     */
    fun getdel(key: String): ByteArray? {
        checkOpen()
        return if (mode == "server") {
            withJedis { it.getDel(key.toByteArray()) }
        } else {
            native!!.getdel(key)
        }
    }

    /**
     * Append value to key, return new length.
     */
    fun append(key: String, value: ByteArray): Long {
        checkOpen()
        return if (mode == "server") {
            withJedis { it.append(key.toByteArray(), value) }
        } else {
            native!!.append(key, value)
        }
    }

    /**
     * Get the length of the value stored at key.
     */
    fun strlen(key: String): Long {
        checkOpen()
        return if (mode == "server") {
            withJedis { it.strlen(key.toByteArray()) }
        } else {
            native!!.strlen(key)
        }
    }

    /**
     * Get a substring of the value stored at key.
     */
    fun getrange(key: String, start: Long, end: Long): ByteArray {
        checkOpen()
        return if (mode == "server") {
            withJedis { it.getrange(key.toByteArray(), start, end) ?: ByteArray(0) }
        } else {
            native!!.getrange(key, start, end)
        }
    }

    /**
     * Overwrite part of a string at key starting at offset.
     */
    fun setrange(key: String, offset: Long, value: ByteArray): Long {
        checkOpen()
        return if (mode == "server") {
            withJedis { it.setrange(key.toByteArray(), offset, value) }
        } else {
            native!!.setrange(key, offset, value)
        }
    }

    /**
     * Increment the integer value of a key by one.
     */
    fun incr(key: String): Long {
        checkOpen()
        return if (mode == "server") {
            withJedis { it.incr(key) }
        } else {
            native!!.incr(key)
        }
    }

    /**
     * Decrement the integer value of a key by one.
     */
    fun decr(key: String): Long {
        checkOpen()
        return if (mode == "server") {
            withJedis { it.decr(key) }
        } else {
            native!!.decr(key)
        }
    }

    /**
     * Increment the integer value of a key by amount.
     */
    fun incrby(key: String, amount: Long): Long {
        checkOpen()
        return if (mode == "server") {
            withJedis { it.incrBy(key, amount) }
        } else {
            native!!.incrby(key, amount)
        }
    }

    /**
     * Decrement the integer value of a key by amount.
     */
    fun decrby(key: String, amount: Long): Long {
        checkOpen()
        return if (mode == "server") {
            withJedis { it.decrBy(key, amount) }
        } else {
            native!!.decrby(key, amount)
        }
    }

    /**
     * Increment the float value of a key by amount.
     */
    fun incrbyfloat(key: String, amount: Double): Double {
        checkOpen()
        return if (mode == "server") {
            withJedis { it.incrByFloat(key, amount) }
        } else {
            native!!.incrbyfloat(key, amount)
        }
    }

    /**
     * Get values of multiple keys.
     */
    fun mget(vararg keys: String): List<ByteArray?> {
        checkOpen()
        if (keys.isEmpty()) return emptyList()
        return if (mode == "server") {
            withJedis { jedis ->
                jedis.mget(*keys.map { it.toByteArray() }.toTypedArray()).toList()
            }
        } else {
            native!!.mget(keys.toList())
        }
    }

    /**
     * Set multiple key-value pairs atomically.
     */
    fun mset(pairs: Map<String, ByteArray>): Boolean {
        checkOpen()
        if (pairs.isEmpty()) return true
        return if (mode == "server") {
            withJedis { jedis ->
                val args = pairs.flatMap { (k, v) -> listOf(k.toByteArray(), v) }.toTypedArray()
                jedis.mset(*args) == "OK"
            }
        } else {
            native!!.mset(pairs.map { it.key to it.value })
        }
    }

    // =========================================================================
    // Key Commands
    // =========================================================================

    /**
     * Delete one or more keys, return count deleted.
     */
    fun delete(vararg keys: String): Long {
        checkOpen()
        if (keys.isEmpty()) return 0
        return if (mode == "server") {
            withJedis { it.del(*keys) }
        } else {
            native!!.delete(keys.toList())
        }
    }

    /**
     * Check if keys exist, return count of existing keys.
     */
    fun exists(vararg keys: String): Long {
        checkOpen()
        if (keys.isEmpty()) return 0
        return if (mode == "server") {
            withJedis { it.exists(*keys) }
        } else {
            native!!.exists(keys.toList())
        }
    }

    /**
     * Get the type of a key.
     */
    fun type(key: String): String {
        checkOpen()
        return if (mode == "server") {
            withJedis { it.type(key) }
        } else {
            native!!.type(key)
        }
    }

    /**
     * Get the TTL of a key in seconds.
     */
    fun ttl(key: String): Long {
        checkOpen()
        return if (mode == "server") {
            withJedis { it.ttl(key) }
        } else {
            native!!.ttl(key)
        }
    }

    /**
     * Get the TTL of a key in milliseconds.
     */
    fun pttl(key: String): Long {
        checkOpen()
        return if (mode == "server") {
            withJedis { it.pttl(key) }
        } else {
            native!!.pttl(key)
        }
    }

    /**
     * Set a timeout on key in seconds.
     */
    fun expire(key: String, seconds: Long): Boolean {
        checkOpen()
        return if (mode == "server") {
            withJedis { it.expire(key, seconds) == 1L }
        } else {
            native!!.expire(key, seconds)
        }
    }

    /**
     * Set a timeout on key in milliseconds.
     */
    fun pexpire(key: String, milliseconds: Long): Boolean {
        checkOpen()
        return if (mode == "server") {
            withJedis { it.pexpire(key, milliseconds) == 1L }
        } else {
            native!!.pexpire(key, milliseconds)
        }
    }

    /**
     * Set an expiration time as Unix timestamp (seconds).
     */
    fun expireat(key: String, unixTime: Long): Boolean {
        checkOpen()
        return if (mode == "server") {
            withJedis { it.expireAt(key, unixTime) == 1L }
        } else {
            native!!.expireat(key, unixTime)
        }
    }

    /**
     * Set an expiration time as Unix timestamp (milliseconds).
     */
    fun pexpireat(key: String, unixTimeMs: Long): Boolean {
        checkOpen()
        return if (mode == "server") {
            withJedis { it.pexpireAt(key, unixTimeMs) == 1L }
        } else {
            native!!.pexpireat(key, unixTimeMs)
        }
    }

    /**
     * Remove the timeout on key.
     */
    fun persist(key: String): Boolean {
        checkOpen()
        return if (mode == "server") {
            withJedis { it.persist(key) == 1L }
        } else {
            native!!.persist(key)
        }
    }

    /**
     * Rename a key.
     */
    fun rename(src: String, dst: String): Boolean {
        checkOpen()
        return if (mode == "server") {
            withJedis { it.rename(src, dst) == "OK" }
        } else {
            native!!.rename(src, dst)
        }
    }

    /**
     * Rename a key only if the new key doesn't exist.
     */
    fun renamenx(src: String, dst: String): Boolean {
        checkOpen()
        return if (mode == "server") {
            withJedis { it.renamenx(src, dst) == 1L }
        } else {
            native!!.renamenx(src, dst)
        }
    }

    /**
     * Find all keys matching a pattern.
     */
    fun keys(pattern: String = "*"): List<String> {
        checkOpen()
        return if (mode == "server") {
            withJedis { it.keys(pattern).toList() }
        } else {
            native!!.keys(pattern)
        }
    }

    /**
     * Return the number of keys in the database.
     */
    fun dbsize(): Long {
        checkOpen()
        return if (mode == "server") {
            withJedis { it.dbSize() }
        } else {
            native!!.dbsize()
        }
    }

    /**
     * Delete all keys in the current database.
     */
    fun flushdb(): Boolean {
        checkOpen()
        return if (mode == "server") {
            withJedis { it.flushDB() == "OK" }
        } else {
            native!!.flushdb()
        }
    }

    /**
     * Select the database to use.
     */
    fun select(db: Int): Boolean {
        checkOpen()
        return if (mode == "server") {
            withJedis { it.select(db) == "OK" }
        } else {
            native!!.select(db)
        }
    }

    // =========================================================================
    // Hash Commands
    // =========================================================================

    /**
     * Set a single hash field.
     */
    fun hset(key: String, field: String, value: ByteArray): Long {
        checkOpen()
        return if (mode == "server") {
            withJedis { it.hset(key.toByteArray(), field.toByteArray(), value) }
        } else {
            native!!.hset(key, field, value)
        }
    }

    /**
     * Set multiple hash fields.
     */
    fun hset(key: String, mapping: Map<String, ByteArray>): Long {
        checkOpen()
        if (mapping.isEmpty()) return 0
        return if (mode == "server") {
            withJedis { jedis ->
                val byteMap = mapping.mapKeys { it.key.toByteArray() }
                jedis.hset(key.toByteArray(), byteMap)
            }
        } else {
            native!!.hsetMultiple(key, mapping)
        }
    }

    /**
     * Get a hash field value.
     */
    fun hget(key: String, field: String): ByteArray? {
        checkOpen()
        return if (mode == "server") {
            withJedis { it.hget(key.toByteArray(), field.toByteArray()) }
        } else {
            native!!.hget(key, field)
        }
    }

    /**
     * Delete hash fields.
     */
    fun hdel(key: String, vararg fields: String): Long {
        checkOpen()
        if (fields.isEmpty()) return 0
        return if (mode == "server") {
            withJedis { it.hdel(key.toByteArray(), *fields.map { it.toByteArray() }.toTypedArray()) }
        } else {
            native!!.hdel(key, fields.toList())
        }
    }

    /**
     * Check if a hash field exists.
     */
    fun hexists(key: String, field: String): Boolean {
        checkOpen()
        return if (mode == "server") {
            withJedis { it.hexists(key.toByteArray(), field.toByteArray()) }
        } else {
            native!!.hexists(key, field)
        }
    }

    /**
     * Get the number of fields in a hash.
     */
    fun hlen(key: String): Long {
        checkOpen()
        return if (mode == "server") {
            withJedis { it.hlen(key.toByteArray()) }
        } else {
            native!!.hlen(key)
        }
    }

    /**
     * Get all field names in a hash.
     */
    fun hkeys(key: String): List<String> {
        checkOpen()
        return if (mode == "server") {
            withJedis { jedis ->
                jedis.hkeys(key.toByteArray()).map { String(it) }
            }
        } else {
            native!!.hkeys(key)
        }
    }

    /**
     * Get all values in a hash.
     */
    fun hvals(key: String): List<ByteArray> {
        checkOpen()
        return if (mode == "server") {
            withJedis { it.hvals(key.toByteArray()).toList() }
        } else {
            native!!.hvals(key)
        }
    }

    /**
     * Increment a hash field by amount.
     */
    fun hincrby(key: String, field: String, amount: Long): Long {
        checkOpen()
        return if (mode == "server") {
            withJedis { it.hincrBy(key.toByteArray(), field.toByteArray(), amount) }
        } else {
            native!!.hincrby(key, field, amount)
        }
    }

    /**
     * Increment a hash field by float amount.
     */
    fun hincrbyfloat(key: String, field: String, amount: Double): Double {
        checkOpen()
        return if (mode == "server") {
            withJedis { it.hincrByFloat(key.toByteArray(), field.toByteArray(), amount) }
        } else {
            native!!.hincrbyfloat(key, field, amount)
        }
    }

    /**
     * Get all fields and values in a hash.
     */
    fun hgetall(key: String): Map<String, ByteArray> {
        checkOpen()
        return if (mode == "server") {
            withJedis { jedis ->
                jedis.hgetAll(key.toByteArray()).mapKeys { String(it.key) }
            }
        } else {
            native!!.hgetall(key)
        }
    }

    /**
     * Get values of multiple hash fields.
     */
    fun hmget(key: String, vararg fields: String): List<ByteArray?> {
        checkOpen()
        if (fields.isEmpty()) return emptyList()
        return if (mode == "server") {
            withJedis { jedis ->
                jedis.hmget(key.toByteArray(), *fields.map { it.toByteArray() }.toTypedArray()).toList()
            }
        } else {
            native!!.hmget(key, fields.toList())
        }
    }

    /**
     * Set a hash field only if it doesn't exist.
     */
    fun hsetnx(key: String, field: String, value: ByteArray): Boolean {
        checkOpen()
        return if (mode == "server") {
            withJedis { it.hsetnx(key.toByteArray(), field.toByteArray(), value) == 1L }
        } else {
            native!!.hsetnx(key, field, value)
        }
    }

    // =========================================================================
    // List Commands
    // =========================================================================

    /**
     * Push values to the head of a list.
     */
    fun lpush(key: String, vararg values: ByteArray): Long {
        checkOpen()
        if (values.isEmpty()) return 0
        return if (mode == "server") {
            withJedis { it.lpush(key.toByteArray(), *values) }
        } else {
            native!!.lpush(key, values.toList())
        }
    }

    /**
     * Push values to the tail of a list.
     */
    fun rpush(key: String, vararg values: ByteArray): Long {
        checkOpen()
        if (values.isEmpty()) return 0
        return if (mode == "server") {
            withJedis { it.rpush(key.toByteArray(), *values) }
        } else {
            native!!.rpush(key, values.toList())
        }
    }

    /**
     * Pop values from the head of a list.
     */
    fun lpop(key: String, count: Int = 1): List<ByteArray> {
        checkOpen()
        return if (mode == "server") {
            withJedis { jedis ->
                if (count == 1) {
                    listOfNotNull(jedis.lpop(key.toByteArray()))
                } else {
                    jedis.lpop(key.toByteArray(), count) ?: emptyList()
                }
            }
        } else {
            native!!.lpop(key, count)
        }
    }

    /**
     * Pop values from the tail of a list.
     */
    fun rpop(key: String, count: Int = 1): List<ByteArray> {
        checkOpen()
        return if (mode == "server") {
            withJedis { jedis ->
                if (count == 1) {
                    listOfNotNull(jedis.rpop(key.toByteArray()))
                } else {
                    jedis.rpop(key.toByteArray(), count) ?: emptyList()
                }
            }
        } else {
            native!!.rpop(key, count)
        }
    }

    /**
     * Get the length of a list.
     */
    fun llen(key: String): Long {
        checkOpen()
        return if (mode == "server") {
            withJedis { it.llen(key.toByteArray()) }
        } else {
            native!!.llen(key)
        }
    }

    /**
     * Get a range of elements from a list.
     */
    fun lrange(key: String, start: Long, stop: Long): List<ByteArray> {
        checkOpen()
        return if (mode == "server") {
            withJedis { it.lrange(key.toByteArray(), start, stop) }
        } else {
            native!!.lrange(key, start, stop)
        }
    }

    /**
     * Get an element from a list by index.
     */
    fun lindex(key: String, index: Long): ByteArray? {
        checkOpen()
        return if (mode == "server") {
            withJedis { it.lindex(key.toByteArray(), index) }
        } else {
            native!!.lindex(key, index)
        }
    }

    /**
     * Set an element in a list by index.
     */
    fun lset(key: String, index: Long, value: ByteArray): Boolean {
        checkOpen()
        return if (mode == "server") {
            withJedis { it.lset(key.toByteArray(), index, value) == "OK" }
        } else {
            native!!.lset(key, index, value)
        }
    }

    /**
     * Insert an element before or after another element.
     */
    fun linsert(key: String, before: Boolean, pivot: ByteArray, value: ByteArray): Long {
        checkOpen()
        return if (mode == "server") {
            withJedis { jedis ->
                val pos = if (before) redis.clients.jedis.args.ListPosition.BEFORE
                else redis.clients.jedis.args.ListPosition.AFTER
                jedis.linsert(key.toByteArray(), pos, pivot, value)
            }
        } else {
            native!!.linsert(key, before, pivot, value)
        }
    }

    /**
     * Remove elements from a list.
     */
    fun lrem(key: String, count: Long, value: ByteArray): Long {
        checkOpen()
        return if (mode == "server") {
            withJedis { it.lrem(key.toByteArray(), count, value) }
        } else {
            native!!.lrem(key, count, value)
        }
    }

    /**
     * Trim a list to the specified range.
     */
    fun ltrim(key: String, start: Long, stop: Long): Boolean {
        checkOpen()
        return if (mode == "server") {
            withJedis { it.ltrim(key.toByteArray(), start, stop) == "OK" }
        } else {
            native!!.ltrim(key, start, stop)
        }
    }

    /**
     * Push values to head only if key exists.
     */
    fun lpushx(key: String, vararg values: ByteArray): Long {
        checkOpen()
        if (values.isEmpty()) return 0
        return if (mode == "server") {
            withJedis { it.lpushx(key.toByteArray(), *values) }
        } else {
            native!!.lpushx(key, values.toList())
        }
    }

    /**
     * Push values to tail only if key exists.
     */
    fun rpushx(key: String, vararg values: ByteArray): Long {
        checkOpen()
        if (values.isEmpty()) return 0
        return if (mode == "server") {
            withJedis { it.rpushx(key.toByteArray(), *values) }
        } else {
            native!!.rpushx(key, values.toList())
        }
    }

    // =========================================================================
    // Set Commands
    // =========================================================================

    /**
     * Add members to a set.
     */
    fun sadd(key: String, vararg members: ByteArray): Long {
        checkOpen()
        if (members.isEmpty()) return 0
        return if (mode == "server") {
            withJedis { it.sadd(key.toByteArray(), *members) }
        } else {
            native!!.sadd(key, members.toList())
        }
    }

    /**
     * Remove members from a set.
     */
    fun srem(key: String, vararg members: ByteArray): Long {
        checkOpen()
        if (members.isEmpty()) return 0
        return if (mode == "server") {
            withJedis { it.srem(key.toByteArray(), *members) }
        } else {
            native!!.srem(key, members.toList())
        }
    }

    /**
     * Get all members of a set.
     */
    fun smembers(key: String): Set<ByteArray> {
        checkOpen()
        return if (mode == "server") {
            withJedis { it.smembers(key.toByteArray()).toSet() }
        } else {
            native!!.smembers(key)
        }
    }

    /**
     * Check if a value is a member of a set.
     */
    fun sismember(key: String, member: ByteArray): Boolean {
        checkOpen()
        return if (mode == "server") {
            withJedis { it.sismember(key.toByteArray(), member) }
        } else {
            native!!.sismember(key, member)
        }
    }

    /**
     * Get the number of members in a set.
     */
    fun scard(key: String): Long {
        checkOpen()
        return if (mode == "server") {
            withJedis { it.scard(key.toByteArray()) }
        } else {
            native!!.scard(key)
        }
    }

    /**
     * Get the difference between sets.
     */
    fun sdiff(vararg keys: String): Set<ByteArray> {
        checkOpen()
        if (keys.isEmpty()) return emptySet()
        return if (mode == "server") {
            withJedis { it.sdiff(*keys.map { it.toByteArray() }.toTypedArray()).toSet() }
        } else {
            native!!.sdiff(keys.toList())
        }
    }

    /**
     * Store the difference between sets.
     */
    fun sdiffstore(destination: String, vararg keys: String): Long {
        checkOpen()
        if (keys.isEmpty()) return 0
        return if (mode == "server") {
            withJedis { it.sdiffstore(destination.toByteArray(), *keys.map { it.toByteArray() }.toTypedArray()) }
        } else {
            native!!.sdiffstore(destination, keys.toList())
        }
    }

    /**
     * Get the intersection of sets.
     */
    fun sinter(vararg keys: String): Set<ByteArray> {
        checkOpen()
        if (keys.isEmpty()) return emptySet()
        return if (mode == "server") {
            withJedis { it.sinter(*keys.map { it.toByteArray() }.toTypedArray()).toSet() }
        } else {
            native!!.sinter(keys.toList())
        }
    }

    /**
     * Store the intersection of sets.
     */
    fun sinterstore(destination: String, vararg keys: String): Long {
        checkOpen()
        if (keys.isEmpty()) return 0
        return if (mode == "server") {
            withJedis { it.sinterstore(destination.toByteArray(), *keys.map { it.toByteArray() }.toTypedArray()) }
        } else {
            native!!.sinterstore(destination, keys.toList())
        }
    }

    /**
     * Get the union of sets.
     */
    fun sunion(vararg keys: String): Set<ByteArray> {
        checkOpen()
        if (keys.isEmpty()) return emptySet()
        return if (mode == "server") {
            withJedis { it.sunion(*keys.map { it.toByteArray() }.toTypedArray()).toSet() }
        } else {
            native!!.sunion(keys.toList())
        }
    }

    /**
     * Store the union of sets.
     */
    fun sunionstore(destination: String, vararg keys: String): Long {
        checkOpen()
        if (keys.isEmpty()) return 0
        return if (mode == "server") {
            withJedis { it.sunionstore(destination.toByteArray(), *keys.map { it.toByteArray() }.toTypedArray()) }
        } else {
            native!!.sunionstore(destination, keys.toList())
        }
    }

    /**
     * Move a member from one set to another.
     */
    fun smove(src: String, dst: String, member: ByteArray): Boolean {
        checkOpen()
        return if (mode == "server") {
            withJedis { it.smove(src.toByteArray(), dst.toByteArray(), member) == 1L }
        } else {
            native!!.smove(src, dst, member)
        }
    }

    /**
     * Remove and return random members from a set.
     */
    fun spop(key: String, count: Int = 1): List<ByteArray> {
        checkOpen()
        return if (mode == "server") {
            withJedis { jedis ->
                if (count == 1) {
                    listOfNotNull(jedis.spop(key.toByteArray()))
                } else {
                    jedis.spop(key.toByteArray(), count.toLong()).toList()
                }
            }
        } else {
            native!!.spop(key, count)
        }
    }

    /**
     * Get random members from a set.
     */
    fun srandmember(key: String, count: Int = 1): List<ByteArray> {
        checkOpen()
        return if (mode == "server") {
            withJedis { jedis ->
                if (count == 1) {
                    listOfNotNull(jedis.srandmember(key.toByteArray()))
                } else {
                    jedis.srandmember(key.toByteArray(), count).toList()
                }
            }
        } else {
            native!!.srandmember(key, count)
        }
    }

    // =========================================================================
    // Sorted Set Commands
    // =========================================================================

    /**
     * Add members to a sorted set.
     */
    fun zadd(key: String, vararg members: ZMember): Long {
        checkOpen()
        if (members.isEmpty()) return 0
        return if (mode == "server") {
            withJedis { jedis ->
                val scoreMembers = members.associate { it.member to it.score }
                jedis.zadd(key.toByteArray(), scoreMembers)
            }
        } else {
            native!!.zadd(key, members.toList())
        }
    }

    /**
     * Add members to a sorted set from a map.
     */
    fun zadd(key: String, mapping: Map<ByteArray, Double>): Long {
        checkOpen()
        if (mapping.isEmpty()) return 0
        return if (mode == "server") {
            withJedis { jedis ->
                jedis.zadd(key.toByteArray(), mapping)
            }
        } else {
            native!!.zadd(key, mapping.map { ZMember(it.value, it.key) })
        }
    }

    /**
     * Remove members from a sorted set.
     */
    fun zrem(key: String, vararg members: ByteArray): Long {
        checkOpen()
        if (members.isEmpty()) return 0
        return if (mode == "server") {
            withJedis { it.zrem(key.toByteArray(), *members) }
        } else {
            native!!.zrem(key, members.toList())
        }
    }

    /**
     * Get the score of a member in a sorted set.
     */
    fun zscore(key: String, member: ByteArray): Double? {
        checkOpen()
        return if (mode == "server") {
            withJedis { it.zscore(key.toByteArray(), member) }
        } else {
            native!!.zscore(key, member)
        }
    }

    /**
     * Get the number of members in a sorted set.
     */
    fun zcard(key: String): Long {
        checkOpen()
        return if (mode == "server") {
            withJedis { it.zcard(key.toByteArray()) }
        } else {
            native!!.zcard(key)
        }
    }

    /**
     * Count members with scores in the given range.
     */
    fun zcount(key: String, min: Double, max: Double): Long {
        checkOpen()
        return if (mode == "server") {
            withJedis { it.zcount(key.toByteArray(), min, max) }
        } else {
            native!!.zcount(key, min, max)
        }
    }

    /**
     * Increment the score of a member in a sorted set.
     */
    fun zincrby(key: String, amount: Double, member: ByteArray): Double {
        checkOpen()
        return if (mode == "server") {
            withJedis { it.zincrby(key.toByteArray(), amount, member) }
        } else {
            native!!.zincrby(key, amount, member)
        }
    }

    /**
     * Get members by rank range (ascending order).
     */
    fun zrange(key: String, start: Long, stop: Long, withScores: Boolean = false): List<Any> {
        checkOpen()
        return if (mode == "server") {
            withJedis { jedis ->
                if (withScores) {
                    jedis.zrangeWithScores(key.toByteArray(), start, stop)
                        .flatMap { listOf(it.element, it.score) }
                } else {
                    jedis.zrange(key.toByteArray(), start, stop).toList()
                }
            }
        } else {
            val members = native!!.zrange(key, start, stop, withScores)
            if (withScores) {
                members.flatMap { listOf(it.member, it.score) }
            } else {
                members.map { it.member }
            }
        }
    }

    /**
     * Get members by rank range (descending order).
     */
    fun zrevrange(key: String, start: Long, stop: Long, withScores: Boolean = false): List<Any> {
        checkOpen()
        return if (mode == "server") {
            withJedis { jedis ->
                if (withScores) {
                    jedis.zrevrangeWithScores(key.toByteArray(), start, stop)
                        .flatMap { listOf(it.element, it.score) }
                } else {
                    jedis.zrevrange(key.toByteArray(), start, stop).toList()
                }
            }
        } else {
            val members = native!!.zrevrange(key, start, stop, withScores)
            if (withScores) {
                members.flatMap { listOf(it.member, it.score) }
            } else {
                members.map { it.member }
            }
        }
    }

    /**
     * Get members by score range.
     */
    fun zrangebyscore(key: String, min: Double, max: Double, withScores: Boolean = false): List<Any> {
        checkOpen()
        return if (mode == "server") {
            withJedis { jedis ->
                if (withScores) {
                    jedis.zrangeByScoreWithScores(key.toByteArray(), min, max)
                        .flatMap { listOf(it.element, it.score) }
                } else {
                    jedis.zrangeByScore(key.toByteArray(), min, max).toList()
                }
            }
        } else {
            val members = native!!.zrangebyscore(key, min, max, withScores)
            if (withScores) {
                members.flatMap { listOf(it.member, it.score) }
            } else {
                members.map { it.member }
            }
        }
    }

    /**
     * Get the rank of a member in a sorted set (ascending).
     */
    fun zrank(key: String, member: ByteArray): Long? {
        checkOpen()
        return if (mode == "server") {
            withJedis { it.zrank(key.toByteArray(), member) }
        } else {
            native!!.zrank(key, member)
        }
    }

    /**
     * Get the rank of a member in a sorted set (descending).
     */
    fun zrevrank(key: String, member: ByteArray): Long? {
        checkOpen()
        return if (mode == "server") {
            withJedis { it.zrevrank(key.toByteArray(), member) }
        } else {
            native!!.zrevrank(key, member)
        }
    }

    /**
     * Remove members by rank range.
     */
    fun zremrangebyrank(key: String, start: Long, stop: Long): Long {
        checkOpen()
        return if (mode == "server") {
            withJedis { it.zremrangeByRank(key.toByteArray(), start, stop) }
        } else {
            native!!.zremrangebyrank(key, start, stop)
        }
    }

    /**
     * Remove members by score range.
     */
    fun zremrangebyscore(key: String, min: Double, max: Double): Long {
        checkOpen()
        return if (mode == "server") {
            withJedis { it.zremrangeByScore(key.toByteArray(), min, max) }
        } else {
            native!!.zremrangebyscore(key, min, max)
        }
    }

    /**
     * Store the intersection of sorted sets.
     */
    fun zinterstore(destination: String, vararg keys: String): Long {
        checkOpen()
        if (keys.isEmpty()) return 0
        return if (mode == "server") {
            withJedis { it.zinterstore(destination.toByteArray(), *keys.map { k -> k.toByteArray() }.toTypedArray()) }
        } else {
            native!!.zinterstore(destination, keys.toList())
        }
    }

    /**
     * Store the union of sorted sets.
     */
    fun zunionstore(destination: String, vararg keys: String): Long {
        checkOpen()
        if (keys.isEmpty()) return 0
        return if (mode == "server") {
            withJedis { it.zunionstore(destination.toByteArray(), *keys.map { k -> k.toByteArray() }.toTypedArray()) }
        } else {
            native!!.zunionstore(destination, keys.toList())
        }
    }

    // =========================================================================
    // Scan Commands
    // =========================================================================

    /**
     * Incrementally iterate keys matching a pattern.
     */
    fun scan(cursor: String = "0", match: String? = null, count: Int = 10): Pair<String, List<String>> {
        checkOpen()
        return if (mode == "server") {
            withJedis { jedis ->
                val params = redis.clients.jedis.params.ScanParams()
                match?.let { params.match(it) }
                params.count(count)
                val result = jedis.scan(cursor, params)
                Pair(result.cursor, result.result)
            }
        } else {
            native!!.scan(cursor, match, count)
        }
    }

    /**
     * Incrementally iterate hash fields.
     */
    fun hscan(key: String, cursor: String = "0", match: String? = null, count: Int = 10): Pair<String, Map<String, ByteArray>> {
        checkOpen()
        return if (mode == "server") {
            withJedis { jedis ->
                val params = redis.clients.jedis.params.ScanParams()
                match?.let { params.match(it) }
                params.count(count)
                val result = jedis.hscan(key.toByteArray(), cursor.toByteArray(), params)
                val map = result.result.associate { String(it.key) to it.value }
                Pair(String(result.cursor), map)
            }
        } else {
            native!!.hscan(key, cursor, match, count)
        }
    }

    /**
     * Incrementally iterate set members.
     */
    fun sscan(key: String, cursor: String = "0", match: String? = null, count: Int = 10): Pair<String, List<ByteArray>> {
        checkOpen()
        return if (mode == "server") {
            withJedis { jedis ->
                val params = redis.clients.jedis.params.ScanParams()
                match?.let { params.match(it) }
                params.count(count)
                val result = jedis.sscan(key.toByteArray(), cursor.toByteArray(), params)
                Pair(String(result.cursor), result.result.toList())
            }
        } else {
            native!!.sscan(key, cursor, match, count)
        }
    }

    /**
     * Incrementally iterate sorted set members with scores.
     */
    fun zscan(key: String, cursor: String = "0", match: String? = null, count: Int = 10): Pair<String, List<ZMember>> {
        checkOpen()
        return if (mode == "server") {
            withJedis { jedis ->
                val params = redis.clients.jedis.params.ScanParams()
                match?.let { params.match(it) }
                params.count(count)
                val result = jedis.zscan(key.toByteArray(), cursor.toByteArray(), params)
                val members = result.result.map { ZMember(it.score, it.element) }
                Pair(String(result.cursor), members)
            }
        } else {
            native!!.zscan(key, cursor, match, count)
        }
    }

    // =========================================================================
    // Server Commands
    // =========================================================================

    /**
     * Compact the database, return bytes freed (embedded mode only).
     */
    fun vacuum(): Long {
        checkOpen()
        return if (mode == "server") {
            throw RedliteException("VACUUM is only available in embedded mode")
        } else {
            native!!.vacuum()
        }
    }

    /**
     * Ping the server.
     */
    fun ping(): String {
        checkOpen()
        return if (mode == "server") {
            withJedis { it.ping() }
        } else {
            native!!.ping()
        }
    }

    /**
     * Echo a message.
     */
    fun echo(message: String): String {
        checkOpen()
        return if (mode == "server") {
            withJedis { it.echo(message) }
        } else {
            native!!.echo(message)
        }
    }

    /**
     * Execute a raw command (for Redlite-specific commands).
     */
    internal fun execute(vararg args: Any): Any? {
        checkOpen()
        return if (mode == "server") {
            withJedis { jedis ->
                val strArgs = args.map {
                    when (it) {
                        is ByteArray -> String(it)
                        else -> it.toString()
                    }
                }.toTypedArray()
                jedis.sendCommand { strArgs[0].toByteArray() }
            }
        } else {
            throw RedliteException("Raw command execution not supported in embedded mode")
        }
    }

    companion object {
        /**
         * Get the redlite library version.
         */
        @JvmStatic
        fun version(): String = "0.1.0"
    }
}
