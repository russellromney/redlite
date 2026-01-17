package com.redlite

import java.io.File
import java.nio.file.Files

/**
 * JNI wrapper for the native Redlite database.
 *
 * This class provides direct access to the embedded Redlite database
 * through JNI bindings. For most use cases, use the [Redlite] class instead,
 * which provides a unified API for both embedded and server modes.
 */
class EmbeddedDb private constructor(private var ptr: Long) : AutoCloseable {

    companion object {
        private var libraryLoaded = false

        init {
            loadNativeLibrary()
        }

        private fun loadNativeLibrary() {
            if (libraryLoaded) return

            val osName = System.getProperty("os.name").lowercase()
            val osArch = System.getProperty("os.arch").lowercase()

            val libName = when {
                osName.contains("mac") || osName.contains("darwin") -> "libredlite_jni.dylib"
                osName.contains("linux") -> "libredlite_jni.so"
                osName.contains("windows") -> "redlite_jni.dll"
                else -> throw RedliteException("Unsupported OS: $osName")
            }

            // Try loading from different locations
            val loaded = tryLoadFromResources(libName)
                    || tryLoadFromPath(libName)
                    || tryLoadFromEnv(libName)
                    || tryLoadSystem()

            if (!loaded) {
                throw RedliteException(
                    "Failed to load native library. " +
                    "Ensure the redlite JNI library is available in the classpath or system path."
                )
            }

            libraryLoaded = true
        }

        private fun tryLoadFromResources(libName: String): Boolean {
            return try {
                val resourcePath = "/native/$libName"
                val inputStream = EmbeddedDb::class.java.getResourceAsStream(resourcePath)
                    ?: return false

                val tempFile = Files.createTempFile("redlite_jni", libName.substringAfterLast('.'))
                    .toFile()
                tempFile.deleteOnExit()

                inputStream.use { input ->
                    tempFile.outputStream().use { output ->
                        input.copyTo(output)
                    }
                }

                System.load(tempFile.absolutePath)
                true
            } catch (e: Exception) {
                false
            }
        }

        private fun tryLoadFromPath(libName: String): Boolean {
            return try {
                val paths = listOf(
                    "native/target/release/$libName",
                    "../native/target/release/$libName",
                    "target/release/$libName"
                )
                for (path in paths) {
                    val file = File(path)
                    if (file.exists()) {
                        System.load(file.absolutePath)
                        return true
                    }
                }
                false
            } catch (e: Exception) {
                false
            }
        }

        private fun tryLoadFromEnv(libName: String): Boolean {
            return try {
                val envPath = System.getenv("REDLITE_NATIVE_PATH") ?: return false
                val file = File(envPath, libName)
                if (file.exists()) {
                    System.load(file.absolutePath)
                    true
                } else {
                    false
                }
            } catch (e: Exception) {
                false
            }
        }

        private fun tryLoadSystem(): Boolean {
            return try {
                System.loadLibrary("redlite_jni")
                true
            } catch (e: UnsatisfiedLinkError) {
                false
            }
        }

        /**
         * Open an in-memory database.
         */
        @JvmStatic
        fun openMemory(): EmbeddedDb {
            val ptr = nativeOpenMemory()
            if (ptr == 0L) {
                throw RedliteException("Failed to open in-memory database")
            }
            return EmbeddedDb(ptr)
        }

        /**
         * Open a database at the given path with custom cache size.
         *
         * @param path The path to the database file
         * @param cacheMb The cache size in megabytes
         */
        @JvmStatic
        fun openWithCache(path: String, cacheMb: Int): EmbeddedDb {
            val ptr = nativeOpenWithCache(path, cacheMb)
            if (ptr == 0L) {
                throw RedliteException("Failed to open database at $path")
            }
            return EmbeddedDb(ptr)
        }

        // Native method declarations
        @JvmStatic private external fun nativeOpenMemory(): Long
        @JvmStatic private external fun nativeOpenWithCache(path: String, cacheMb: Int): Long
    }

    private fun checkOpen() {
        if (ptr == 0L) {
            throw RedliteException("Database is closed")
        }
    }

    override fun close() {
        if (ptr != 0L) {
            nativeClose(ptr)
            ptr = 0L
        }
    }

    // String Commands
    fun get(key: String): ByteArray? {
        checkOpen()
        return nativeGet(ptr, key)
    }

    fun set(key: String, value: ByteArray, ttlSeconds: Long? = null): Boolean {
        checkOpen()
        return nativeSet(ptr, key, value, ttlSeconds ?: -1)
    }

    fun setOpts(key: String, value: ByteArray, options: SetOptions): Boolean {
        checkOpen()
        return nativeSetOpts(ptr, key, value, options.ex ?: -1, options.px ?: -1, options.nx, options.xx)
    }

    fun setex(key: String, seconds: Long, value: ByteArray): Boolean {
        checkOpen()
        return nativeSetex(ptr, key, seconds, value)
    }

    fun psetex(key: String, milliseconds: Long, value: ByteArray): Boolean {
        checkOpen()
        return nativePsetex(ptr, key, milliseconds, value)
    }

    fun getdel(key: String): ByteArray? {
        checkOpen()
        return nativeGetdel(ptr, key)
    }

    fun append(key: String, value: ByteArray): Long {
        checkOpen()
        return nativeAppend(ptr, key, value)
    }

    fun strlen(key: String): Long {
        checkOpen()
        return nativeStrlen(ptr, key)
    }

    fun getrange(key: String, start: Long, end: Long): ByteArray {
        checkOpen()
        return nativeGetrange(ptr, key, start, end) ?: ByteArray(0)
    }

    fun setrange(key: String, offset: Long, value: ByteArray): Long {
        checkOpen()
        return nativeSetrange(ptr, key, offset, value)
    }

    fun incr(key: String): Long {
        checkOpen()
        return nativeIncr(ptr, key)
    }

    fun decr(key: String): Long {
        checkOpen()
        return nativeDecr(ptr, key)
    }

    fun incrby(key: String, amount: Long): Long {
        checkOpen()
        return nativeIncrby(ptr, key, amount)
    }

    fun decrby(key: String, amount: Long): Long {
        checkOpen()
        return nativeDecrby(ptr, key, amount)
    }

    fun incrbyfloat(key: String, amount: Double): Double {
        checkOpen()
        return nativeIncrbyfloat(ptr, key, amount)
    }

    // Key Commands
    fun delete(keys: List<String>): Long {
        checkOpen()
        return nativeDelete(ptr, keys.toTypedArray())
    }

    fun exists(keys: List<String>): Long {
        checkOpen()
        return nativeExists(ptr, keys.toTypedArray())
    }

    fun type(key: String): String {
        checkOpen()
        return nativeType(ptr, key)
    }

    fun ttl(key: String): Long {
        checkOpen()
        return nativeTtl(ptr, key)
    }

    fun pttl(key: String): Long {
        checkOpen()
        return nativePttl(ptr, key)
    }

    fun expire(key: String, seconds: Long): Boolean {
        checkOpen()
        return nativeExpire(ptr, key, seconds)
    }

    fun pexpire(key: String, milliseconds: Long): Boolean {
        checkOpen()
        return nativePexpire(ptr, key, milliseconds)
    }

    fun expireat(key: String, unixTime: Long): Boolean {
        checkOpen()
        return nativeExpireat(ptr, key, unixTime)
    }

    fun pexpireat(key: String, unixTimeMs: Long): Boolean {
        checkOpen()
        return nativePexpireat(ptr, key, unixTimeMs)
    }

    fun persist(key: String): Boolean {
        checkOpen()
        return nativePersist(ptr, key)
    }

    fun rename(src: String, dst: String): Boolean {
        checkOpen()
        return nativeRename(ptr, src, dst)
    }

    fun renamenx(src: String, dst: String): Boolean {
        checkOpen()
        return nativeRenamenx(ptr, src, dst)
    }

    fun keys(pattern: String): List<String> {
        checkOpen()
        return nativeKeys(ptr, pattern).toList()
    }

    fun dbsize(): Long {
        checkOpen()
        return nativeDbsize(ptr)
    }

    fun flushdb(): Boolean {
        checkOpen()
        return nativeFlushdb(ptr)
    }

    fun select(db: Int): Boolean {
        checkOpen()
        return nativeSelect(ptr, db)
    }

    // Hash Commands
    fun hset(key: String, field: String, value: ByteArray): Long {
        checkOpen()
        return nativeHset(ptr, key, field, value)
    }

    fun hsetMultiple(key: String, mapping: Map<String, ByteArray>): Long {
        checkOpen()
        val fields = mapping.keys.toTypedArray()
        val values = mapping.values.toTypedArray()
        return nativeHsetMultiple(ptr, key, fields, values)
    }

    fun hget(key: String, field: String): ByteArray? {
        checkOpen()
        return nativeHget(ptr, key, field)
    }

    fun hdel(key: String, fields: List<String>): Long {
        checkOpen()
        return nativeHdel(ptr, key, fields.toTypedArray())
    }

    fun hexists(key: String, field: String): Boolean {
        checkOpen()
        return nativeHexists(ptr, key, field)
    }

    fun hlen(key: String): Long {
        checkOpen()
        return nativeHlen(ptr, key)
    }

    fun hkeys(key: String): List<String> {
        checkOpen()
        return nativeHkeys(ptr, key).toList()
    }

    fun hvals(key: String): List<ByteArray> {
        checkOpen()
        return nativeHvals(ptr, key).toList()
    }

    fun hincrby(key: String, field: String, amount: Long): Long {
        checkOpen()
        return nativeHincrby(ptr, key, field, amount)
    }

    fun hincrbyfloat(key: String, field: String, amount: Double): Double {
        checkOpen()
        return nativeHincrbyfloat(ptr, key, field, amount)
    }

    fun hgetall(key: String): Map<String, ByteArray> {
        checkOpen()
        val result = nativeHgetall(ptr, key)
        val map = mutableMapOf<String, ByteArray>()
        for (i in result.indices step 2) {
            if (i + 1 < result.size) {
                map[String(result[i])] = result[i + 1]
            }
        }
        return map
    }

    fun hmget(key: String, fields: List<String>): List<ByteArray?> {
        checkOpen()
        return nativeHmget(ptr, key, fields.toTypedArray()).toList()
    }

    fun hsetnx(key: String, field: String, value: ByteArray): Boolean {
        checkOpen()
        return nativeHsetnx(ptr, key, field, value)
    }

    // List Commands
    fun lpush(key: String, values: List<ByteArray>): Long {
        checkOpen()
        return nativeLpush(ptr, key, values.toTypedArray())
    }

    fun rpush(key: String, values: List<ByteArray>): Long {
        checkOpen()
        return nativeRpush(ptr, key, values.toTypedArray())
    }

    fun lpop(key: String, count: Int): List<ByteArray> {
        checkOpen()
        return nativeLpop(ptr, key, count).toList()
    }

    fun rpop(key: String, count: Int): List<ByteArray> {
        checkOpen()
        return nativeRpop(ptr, key, count).toList()
    }

    fun llen(key: String): Long {
        checkOpen()
        return nativeLlen(ptr, key)
    }

    fun lrange(key: String, start: Long, stop: Long): List<ByteArray> {
        checkOpen()
        return nativeLrange(ptr, key, start, stop).toList()
    }

    fun lindex(key: String, index: Long): ByteArray? {
        checkOpen()
        return nativeLindex(ptr, key, index)
    }

    fun lset(key: String, index: Long, value: ByteArray): Boolean {
        checkOpen()
        return nativeLset(ptr, key, index, value)
    }

    fun linsert(key: String, before: Boolean, pivot: ByteArray, value: ByteArray): Long {
        checkOpen()
        return nativeLinsert(ptr, key, before, pivot, value)
    }

    fun lrem(key: String, count: Long, value: ByteArray): Long {
        checkOpen()
        return nativeLrem(ptr, key, count, value)
    }

    fun ltrim(key: String, start: Long, stop: Long): Boolean {
        checkOpen()
        return nativeLtrim(ptr, key, start, stop)
    }

    fun lpos(key: String, value: ByteArray): Long? {
        checkOpen()
        val result = nativeLpos(ptr, key, value)
        return if (result < 0) null else result
    }

    fun lpushx(key: String, values: List<ByteArray>): Long {
        checkOpen()
        return nativeLpushx(ptr, key, values.toTypedArray())
    }

    fun rpushx(key: String, values: List<ByteArray>): Long {
        checkOpen()
        return nativeRpushx(ptr, key, values.toTypedArray())
    }

    // Set Commands
    fun sadd(key: String, members: List<ByteArray>): Long {
        checkOpen()
        return nativeSadd(ptr, key, members.toTypedArray())
    }

    fun srem(key: String, members: List<ByteArray>): Long {
        checkOpen()
        return nativeSrem(ptr, key, members.toTypedArray())
    }

    fun smembers(key: String): Set<ByteArray> {
        checkOpen()
        return nativeSmembers(ptr, key).toSet()
    }

    fun sismember(key: String, member: ByteArray): Boolean {
        checkOpen()
        return nativeSismember(ptr, key, member)
    }

    fun scard(key: String): Long {
        checkOpen()
        return nativeScard(ptr, key)
    }

    fun sdiff(keys: List<String>): Set<ByteArray> {
        checkOpen()
        return nativeSdiff(ptr, keys.toTypedArray()).toSet()
    }

    fun sdiffstore(destination: String, keys: List<String>): Long {
        checkOpen()
        return nativeSdiffstore(ptr, destination, keys.toTypedArray())
    }

    fun sinter(keys: List<String>): Set<ByteArray> {
        checkOpen()
        return nativeSinter(ptr, keys.toTypedArray()).toSet()
    }

    fun sinterstore(destination: String, keys: List<String>): Long {
        checkOpen()
        return nativeSinterstore(ptr, destination, keys.toTypedArray())
    }

    fun sunion(keys: List<String>): Set<ByteArray> {
        checkOpen()
        return nativeSunion(ptr, keys.toTypedArray()).toSet()
    }

    fun sunionstore(destination: String, keys: List<String>): Long {
        checkOpen()
        return nativeSunionstore(ptr, destination, keys.toTypedArray())
    }

    fun smove(src: String, dst: String, member: ByteArray): Boolean {
        checkOpen()
        return nativeSmove(ptr, src, dst, member)
    }

    fun spop(key: String, count: Int): List<ByteArray> {
        checkOpen()
        return nativeSpop(ptr, key, count).toList()
    }

    fun srandmember(key: String, count: Int): List<ByteArray> {
        checkOpen()
        return nativeSrandmember(ptr, key, count).toList()
    }

    // Sorted Set Commands
    fun zadd(key: String, members: List<ZMember>): Long {
        checkOpen()
        val scores = members.map { it.score }.toDoubleArray()
        val memberBytes = members.map { it.member }.toTypedArray()
        return nativeZadd(ptr, key, scores, memberBytes)
    }

    fun zrem(key: String, members: List<ByteArray>): Long {
        checkOpen()
        return nativeZrem(ptr, key, members.toTypedArray())
    }

    fun zscore(key: String, member: ByteArray): Double? {
        checkOpen()
        val result = nativeZscore(ptr, key, member)
        return if (result.isNaN()) null else result
    }

    fun zcard(key: String): Long {
        checkOpen()
        return nativeZcard(ptr, key)
    }

    fun zcount(key: String, min: Double, max: Double): Long {
        checkOpen()
        return nativeZcount(ptr, key, min, max)
    }

    fun zincrby(key: String, amount: Double, member: ByteArray): Double {
        checkOpen()
        return nativeZincrby(ptr, key, amount, member)
    }

    fun zrange(key: String, start: Long, stop: Long, withScores: Boolean): List<ZMember> {
        checkOpen()
        val result = nativeZrange(ptr, key, start, stop, withScores)
        return parseZMembers(result)
    }

    fun zrevrange(key: String, start: Long, stop: Long, withScores: Boolean): List<ZMember> {
        checkOpen()
        val result = nativeZrevrange(ptr, key, start, stop, withScores)
        return parseZMembers(result)
    }

    fun zrangebyscore(key: String, min: Double, max: Double, withScores: Boolean): List<ZMember> {
        checkOpen()
        val result = nativeZrangebyscore(ptr, key, min, max, withScores)
        return parseZMembers(result)
    }

    fun zrank(key: String, member: ByteArray): Long? {
        checkOpen()
        val result = nativeZrank(ptr, key, member)
        return if (result < 0) null else result
    }

    fun zrevrank(key: String, member: ByteArray): Long? {
        checkOpen()
        val result = nativeZrevrank(ptr, key, member)
        return if (result < 0) null else result
    }

    fun zremrangebyrank(key: String, start: Long, stop: Long): Long {
        checkOpen()
        return nativeZremrangebyrank(ptr, key, start, stop)
    }

    fun zremrangebyscore(key: String, min: Double, max: Double): Long {
        checkOpen()
        return nativeZremrangebyscore(ptr, key, min, max)
    }

    fun zinterstore(destination: String, keys: List<String>): Long {
        checkOpen()
        return nativeZinterstore(ptr, destination, keys.toTypedArray())
    }

    fun zunionstore(destination: String, keys: List<String>): Long {
        checkOpen()
        return nativeZunionstore(ptr, destination, keys.toTypedArray())
    }

    // Multi-key Commands
    fun mget(keys: List<String>): List<ByteArray?> {
        checkOpen()
        return nativeMget(ptr, keys.toTypedArray()).toList()
    }

    fun mset(pairs: List<Pair<String, ByteArray>>): Boolean {
        checkOpen()
        val keys = pairs.map { it.first }.toTypedArray()
        val values = pairs.map { it.second }.toTypedArray()
        return nativeMset(ptr, keys, values)
    }

    // Scan Commands
    fun scan(cursor: String, match: String?, count: Int): Pair<String, List<String>> {
        checkOpen()
        val result = nativeScan(ptr, cursor, match ?: "*", count)
        return Pair(result[0] as String, (result[1] as Array<*>).map { it as String })
    }

    fun hscan(key: String, cursor: String, match: String?, count: Int): Pair<String, Map<String, ByteArray>> {
        checkOpen()
        val result = nativeHscan(ptr, key, cursor, match ?: "*", count)
        val nextCursor = result[0] as String
        val items = result[1] as Array<*>
        val map = mutableMapOf<String, ByteArray>()
        for (i in items.indices step 2) {
            if (i + 1 < items.size) {
                map[items[i] as String] = items[i + 1] as ByteArray
            }
        }
        return Pair(nextCursor, map)
    }

    fun sscan(key: String, cursor: String, match: String?, count: Int): Pair<String, List<ByteArray>> {
        checkOpen()
        val result = nativeSscan(ptr, key, cursor, match ?: "*", count)
        val nextCursor = result[0] as String
        val members = (result[1] as Array<*>).map { it as ByteArray }
        return Pair(nextCursor, members)
    }

    fun zscan(key: String, cursor: String, match: String?, count: Int): Pair<String, List<ZMember>> {
        checkOpen()
        val result = nativeZscan(ptr, key, cursor, match ?: "*", count)
        val nextCursor = result[0] as String
        val items = result[1] as Array<*>
        return Pair(nextCursor, parseZMembersFromArray(items))
    }

    // Server Commands
    fun vacuum(): Long {
        checkOpen()
        return nativeVacuum(ptr)
    }

    fun ping(): String {
        checkOpen()
        return nativePing(ptr)
    }

    fun echo(message: String): String {
        checkOpen()
        return nativeEcho(ptr, message)
    }

    private fun parseZMembers(result: Array<Any>): List<ZMember> {
        val members = mutableListOf<ZMember>()
        var i = 0
        while (i < result.size) {
            val member = result[i] as ByteArray
            val score = if (i + 1 < result.size) (result[i + 1] as Number).toDouble() else 0.0
            members.add(ZMember(score, member))
            i += 2
        }
        return members
    }

    private fun parseZMembersFromArray(items: Array<*>): List<ZMember> {
        val members = mutableListOf<ZMember>()
        var i = 0
        while (i < items.size) {
            val member = items[i] as ByteArray
            val score = if (i + 1 < items.size) (items[i + 1] as Number).toDouble() else 0.0
            members.add(ZMember(score, member))
            i += 2
        }
        return members
    }

    // Native method declarations
    private external fun nativeClose(ptr: Long)

    // String commands
    private external fun nativeGet(ptr: Long, key: String): ByteArray?
    private external fun nativeSet(ptr: Long, key: String, value: ByteArray, ttlSeconds: Long): Boolean
    private external fun nativeSetOpts(ptr: Long, key: String, value: ByteArray, ex: Long, px: Long, nx: Boolean, xx: Boolean): Boolean
    private external fun nativeSetex(ptr: Long, key: String, seconds: Long, value: ByteArray): Boolean
    private external fun nativePsetex(ptr: Long, key: String, milliseconds: Long, value: ByteArray): Boolean
    private external fun nativeGetdel(ptr: Long, key: String): ByteArray?
    private external fun nativeAppend(ptr: Long, key: String, value: ByteArray): Long
    private external fun nativeStrlen(ptr: Long, key: String): Long
    private external fun nativeGetrange(ptr: Long, key: String, start: Long, end: Long): ByteArray?
    private external fun nativeSetrange(ptr: Long, key: String, offset: Long, value: ByteArray): Long
    private external fun nativeIncr(ptr: Long, key: String): Long
    private external fun nativeDecr(ptr: Long, key: String): Long
    private external fun nativeIncrby(ptr: Long, key: String, amount: Long): Long
    private external fun nativeDecrby(ptr: Long, key: String, amount: Long): Long
    private external fun nativeIncrbyfloat(ptr: Long, key: String, amount: Double): Double

    // Key commands
    private external fun nativeDelete(ptr: Long, keys: Array<String>): Long
    private external fun nativeExists(ptr: Long, keys: Array<String>): Long
    private external fun nativeType(ptr: Long, key: String): String
    private external fun nativeTtl(ptr: Long, key: String): Long
    private external fun nativePttl(ptr: Long, key: String): Long
    private external fun nativeExpire(ptr: Long, key: String, seconds: Long): Boolean
    private external fun nativePexpire(ptr: Long, key: String, milliseconds: Long): Boolean
    private external fun nativeExpireat(ptr: Long, key: String, unixTime: Long): Boolean
    private external fun nativePexpireat(ptr: Long, key: String, unixTimeMs: Long): Boolean
    private external fun nativePersist(ptr: Long, key: String): Boolean
    private external fun nativeRename(ptr: Long, src: String, dst: String): Boolean
    private external fun nativeRenamenx(ptr: Long, src: String, dst: String): Boolean
    private external fun nativeKeys(ptr: Long, pattern: String): Array<String>
    private external fun nativeDbsize(ptr: Long): Long
    private external fun nativeFlushdb(ptr: Long): Boolean
    private external fun nativeSelect(ptr: Long, db: Int): Boolean

    // Hash commands
    private external fun nativeHset(ptr: Long, key: String, field: String, value: ByteArray): Long
    private external fun nativeHsetMultiple(ptr: Long, key: String, fields: Array<String>, values: Array<ByteArray>): Long
    private external fun nativeHget(ptr: Long, key: String, field: String): ByteArray?
    private external fun nativeHdel(ptr: Long, key: String, fields: Array<String>): Long
    private external fun nativeHexists(ptr: Long, key: String, field: String): Boolean
    private external fun nativeHlen(ptr: Long, key: String): Long
    private external fun nativeHkeys(ptr: Long, key: String): Array<String>
    private external fun nativeHvals(ptr: Long, key: String): Array<ByteArray>
    private external fun nativeHincrby(ptr: Long, key: String, field: String, amount: Long): Long
    private external fun nativeHincrbyfloat(ptr: Long, key: String, field: String, amount: Double): Double
    private external fun nativeHgetall(ptr: Long, key: String): Array<ByteArray>
    private external fun nativeHmget(ptr: Long, key: String, fields: Array<String>): Array<ByteArray?>
    private external fun nativeHsetnx(ptr: Long, key: String, field: String, value: ByteArray): Boolean

    // List commands
    private external fun nativeLpush(ptr: Long, key: String, values: Array<ByteArray>): Long
    private external fun nativeRpush(ptr: Long, key: String, values: Array<ByteArray>): Long
    private external fun nativeLpop(ptr: Long, key: String, count: Int): Array<ByteArray>
    private external fun nativeRpop(ptr: Long, key: String, count: Int): Array<ByteArray>
    private external fun nativeLlen(ptr: Long, key: String): Long
    private external fun nativeLrange(ptr: Long, key: String, start: Long, stop: Long): Array<ByteArray>
    private external fun nativeLindex(ptr: Long, key: String, index: Long): ByteArray?
    private external fun nativeLset(ptr: Long, key: String, index: Long, value: ByteArray): Boolean
    private external fun nativeLinsert(ptr: Long, key: String, before: Boolean, pivot: ByteArray, value: ByteArray): Long
    private external fun nativeLrem(ptr: Long, key: String, count: Long, value: ByteArray): Long
    private external fun nativeLtrim(ptr: Long, key: String, start: Long, stop: Long): Boolean
    private external fun nativeLpos(ptr: Long, key: String, value: ByteArray): Long
    private external fun nativeLpushx(ptr: Long, key: String, values: Array<ByteArray>): Long
    private external fun nativeRpushx(ptr: Long, key: String, values: Array<ByteArray>): Long

    // Set commands
    private external fun nativeSadd(ptr: Long, key: String, members: Array<ByteArray>): Long
    private external fun nativeSrem(ptr: Long, key: String, members: Array<ByteArray>): Long
    private external fun nativeSmembers(ptr: Long, key: String): Array<ByteArray>
    private external fun nativeSismember(ptr: Long, key: String, member: ByteArray): Boolean
    private external fun nativeScard(ptr: Long, key: String): Long
    private external fun nativeSdiff(ptr: Long, keys: Array<String>): Array<ByteArray>
    private external fun nativeSdiffstore(ptr: Long, destination: String, keys: Array<String>): Long
    private external fun nativeSinter(ptr: Long, keys: Array<String>): Array<ByteArray>
    private external fun nativeSinterstore(ptr: Long, destination: String, keys: Array<String>): Long
    private external fun nativeSunion(ptr: Long, keys: Array<String>): Array<ByteArray>
    private external fun nativeSunionstore(ptr: Long, destination: String, keys: Array<String>): Long
    private external fun nativeSmove(ptr: Long, src: String, dst: String, member: ByteArray): Boolean
    private external fun nativeSpop(ptr: Long, key: String, count: Int): Array<ByteArray>
    private external fun nativeSrandmember(ptr: Long, key: String, count: Int): Array<ByteArray>

    // Sorted set commands
    private external fun nativeZadd(ptr: Long, key: String, scores: DoubleArray, members: Array<ByteArray>): Long
    private external fun nativeZrem(ptr: Long, key: String, members: Array<ByteArray>): Long
    private external fun nativeZscore(ptr: Long, key: String, member: ByteArray): Double
    private external fun nativeZcard(ptr: Long, key: String): Long
    private external fun nativeZcount(ptr: Long, key: String, min: Double, max: Double): Long
    private external fun nativeZincrby(ptr: Long, key: String, amount: Double, member: ByteArray): Double
    private external fun nativeZrange(ptr: Long, key: String, start: Long, stop: Long, withScores: Boolean): Array<Any>
    private external fun nativeZrevrange(ptr: Long, key: String, start: Long, stop: Long, withScores: Boolean): Array<Any>
    private external fun nativeZrangebyscore(ptr: Long, key: String, min: Double, max: Double, withScores: Boolean): Array<Any>
    private external fun nativeZrank(ptr: Long, key: String, member: ByteArray): Long
    private external fun nativeZrevrank(ptr: Long, key: String, member: ByteArray): Long
    private external fun nativeZremrangebyrank(ptr: Long, key: String, start: Long, stop: Long): Long
    private external fun nativeZremrangebyscore(ptr: Long, key: String, min: Double, max: Double): Long
    private external fun nativeZinterstore(ptr: Long, destination: String, keys: Array<String>): Long
    private external fun nativeZunionstore(ptr: Long, destination: String, keys: Array<String>): Long

    // Multi-key commands
    private external fun nativeMget(ptr: Long, keys: Array<String>): Array<ByteArray?>
    private external fun nativeMset(ptr: Long, keys: Array<String>, values: Array<ByteArray>): Boolean

    // Scan commands
    private external fun nativeScan(ptr: Long, cursor: String, match: String, count: Int): Array<Any>
    private external fun nativeHscan(ptr: Long, key: String, cursor: String, match: String, count: Int): Array<Any>
    private external fun nativeSscan(ptr: Long, key: String, cursor: String, match: String, count: Int): Array<Any>
    private external fun nativeZscan(ptr: Long, key: String, cursor: String, match: String, count: Int): Array<Any>

    // Server commands
    private external fun nativeVacuum(ptr: Long): Long
    private external fun nativePing(ptr: Long): String
    private external fun nativeEcho(ptr: Long, message: String): String
}
