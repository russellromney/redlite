/**
 * Oracle Test Runner for Kotlin SDK.
 *
 * Executes YAML test specifications against the Redlite Kotlin SDK
 * and reports pass/fail results with detailed error messages.
 *
 * Usage:
 *     kotlin kotlin_runner.kt                    # Run all specs
 *     kotlin kotlin_runner.kt spec/strings.yaml  # Run single spec
 *     kotlin kotlin_runner.kt -v                 # Verbose output
 */
package com.redlite.oracle

import com.redlite.Redlite
import org.yaml.snakeyaml.Yaml
import java.io.File
import kotlin.math.abs

data class TestError(
    val spec: String,
    val test: String,
    val cmd: String,
    val expected: Any?,
    val actual: Any?
)

class OracleRunner(private val verbose: Boolean = false) {
    private var passed = 0
    private var failed = 0
    private var skipped = 0
    private val errors = mutableListOf<TestError>()

    fun runSpecFile(specPath: String): Boolean {
        val yaml = Yaml()
        val spec = yaml.load<Map<String, Any>>(File(specPath).readText())
        val specName = spec["name"] as? String ?: File(specPath).name
        @Suppress("UNCHECKED_CAST")
        val tests = spec["tests"] as? List<Map<String, Any>> ?: emptyList()

        if (verbose) {
            println("=".repeat(60))
            println("Running: $specName (${tests.size} tests)")
            println("=".repeat(60))
        }

        for (test in tests) {
            runTest(test, specName)
        }

        return errors.isEmpty()
    }

    @Suppress("UNCHECKED_CAST")
    private fun runTest(test: Map<String, Any>, specName: String) {
        val testName = test["name"] as? String ?: "unnamed"

        if (verbose) {
            print("  $testName... ")
        }

        // Create fresh in-memory database for each test
        Redlite(":memory:").use { db ->
            try {
                // Run setup operations
                val setup = test["setup"] as? List<Map<String, Any>>
                setup?.forEach { op ->
                    executeCmd(db, op)
                }

                // Run test operations and check expectations
                val operations = test["operations"] as? List<Map<String, Any>> ?: emptyList()
                for (op in operations) {
                    val actual = executeCmd(db, op)
                    val expected = op["expect"]

                    if (!compare(actual, expected)) {
                        failed++
                        errors.add(TestError(
                            spec = specName,
                            test = testName,
                            cmd = op["cmd"] as? String ?: "unknown",
                            expected = expected,
                            actual = serialize(actual)
                        ))
                        if (verbose) {
                            println("FAILED")
                            println("      Expected: $expected")
                            println("      Actual:   ${serialize(actual)}")
                        }
                        return
                    }
                }

                passed++
                if (verbose) {
                    println("PASSED")
                }
            } catch (e: Exception) {
                failed++
                errors.add(TestError(
                    spec = specName,
                    test = testName,
                    cmd = "unknown",
                    expected = null,
                    actual = "ERROR: ${e.message}"
                ))
                if (verbose) {
                    println("ERROR: ${e.message}")
                }
            }
        }
    }

    @Suppress("UNCHECKED_CAST")
    private fun executeCmd(db: Redlite, op: Map<String, Any>): Any? {
        val cmd = (op["cmd"] as? String)?.lowercase() ?: return null
        val args = op["args"] as? List<Any> ?: emptyList()
        val processedArgs = args.map { processArg(it) }

        return when (cmd) {
            // String commands
            "get" -> db.get(processedArgs[0] as String)?.let { String(it) }
            "set" -> {
                val key = processedArgs[0] as String
                val value = when (val v = processedArgs[1]) {
                    is ByteArray -> v
                    is String -> v.toByteArray()
                    else -> v.toString().toByteArray()
                }
                db.set(key, value)
            }
            "setex" -> {
                val key = processedArgs[0] as String
                val seconds = (processedArgs[1] as Number).toLong()
                val value = when (val v = processedArgs[2]) {
                    is ByteArray -> v
                    is String -> v.toByteArray()
                    else -> v.toString().toByteArray()
                }
                db.setex(key, seconds, value)
            }
            "incr" -> db.incr(processedArgs[0] as String)
            "decr" -> db.decr(processedArgs[0] as String)
            "incrby" -> db.incrby(processedArgs[0] as String, (processedArgs[1] as Number).toLong())
            "decrby" -> db.decrby(processedArgs[0] as String, (processedArgs[1] as Number).toLong())
            "incrbyfloat" -> db.incrbyfloat(processedArgs[0] as String, (processedArgs[1] as Number).toDouble())
            "append" -> {
                val value = when (val v = processedArgs[1]) {
                    is ByteArray -> v
                    is String -> v.toByteArray()
                    else -> v.toString().toByteArray()
                }
                db.append(processedArgs[0] as String, value)
            }
            "strlen" -> db.strlen(processedArgs[0] as String)
            "getrange" -> {
                val result = db.getrange(
                    processedArgs[0] as String,
                    (processedArgs[1] as Number).toLong(),
                    (processedArgs[2] as Number).toLong()
                )
                String(result)
            }

            // Key commands
            "del", "delete" -> {
                val keys = if (processedArgs[0] is List<*>) {
                    (processedArgs[0] as List<*>).map { it as String }.toTypedArray()
                } else {
                    processedArgs.map { it as String }.toTypedArray()
                }
                db.delete(*keys)
            }
            "exists" -> {
                val keys = if (processedArgs[0] is List<*>) {
                    (processedArgs[0] as List<*>).map { it as String }.toTypedArray()
                } else {
                    processedArgs.map { it as String }.toTypedArray()
                }
                db.exists(*keys)
            }
            "type" -> db.type(processedArgs[0] as String)
            "ttl" -> db.ttl(processedArgs[0] as String)
            "pttl" -> db.pttl(processedArgs[0] as String)
            "expire" -> db.expire(processedArgs[0] as String, (processedArgs[1] as Number).toLong())
            "persist" -> db.persist(processedArgs[0] as String)
            "rename" -> db.rename(processedArgs[0] as String, processedArgs[1] as String)
            "keys" -> db.keys(processedArgs.getOrNull(0) as? String ?: "*")
            "dbsize" -> db.dbsize()
            "flushdb" -> db.flushdb()

            // Hash commands
            "hset" -> {
                val key = processedArgs[0] as String
                val field = processedArgs[1] as String
                val value = when (val v = processedArgs[2]) {
                    is ByteArray -> v
                    is String -> v.toByteArray()
                    else -> v.toString().toByteArray()
                }
                db.hset(key, field, value)
            }
            "hget" -> db.hget(processedArgs[0] as String, processedArgs[1] as String)?.let { String(it) }
            "hdel" -> {
                val key = processedArgs[0] as String
                val fields = if (processedArgs[1] is List<*>) {
                    (processedArgs[1] as List<*>).map { it as String }.toTypedArray()
                } else {
                    arrayOf(processedArgs[1] as String)
                }
                db.hdel(key, *fields)
            }
            "hexists" -> db.hexists(processedArgs[0] as String, processedArgs[1] as String)
            "hlen" -> db.hlen(processedArgs[0] as String)
            "hkeys" -> db.hkeys(processedArgs[0] as String)
            "hincrby" -> db.hincrby(
                processedArgs[0] as String,
                processedArgs[1] as String,
                (processedArgs[2] as Number).toLong()
            )
            "hgetall" -> {
                val result = db.hgetall(processedArgs[0] as String)
                result.mapValues { String(it.value) }
            }

            // List commands
            "lpush" -> {
                val key = processedArgs[0] as String
                val values = if (processedArgs[1] is List<*>) {
                    (processedArgs[1] as List<*>).map {
                        when (it) {
                            is ByteArray -> it
                            is String -> it.toByteArray()
                            else -> it.toString().toByteArray()
                        }
                    }.toTypedArray()
                } else {
                    arrayOf(processedArgs[1].toString().toByteArray())
                }
                db.lpush(key, *values)
            }
            "rpush" -> {
                val key = processedArgs[0] as String
                val values = if (processedArgs[1] is List<*>) {
                    (processedArgs[1] as List<*>).map {
                        when (it) {
                            is ByteArray -> it
                            is String -> it.toByteArray()
                            else -> it.toString().toByteArray()
                        }
                    }.toTypedArray()
                } else {
                    arrayOf(processedArgs[1].toString().toByteArray())
                }
                db.rpush(key, *values)
            }
            "lpop" -> {
                val count = processedArgs.getOrNull(1) as? Int ?: 1
                val result = db.lpop(processedArgs[0] as String, count)
                if (count == 1) result.firstOrNull()?.let { String(it) }
                else result.map { String(it) }
            }
            "rpop" -> {
                val count = processedArgs.getOrNull(1) as? Int ?: 1
                val result = db.rpop(processedArgs[0] as String, count)
                if (count == 1) result.firstOrNull()?.let { String(it) }
                else result.map { String(it) }
            }
            "llen" -> db.llen(processedArgs[0] as String)
            "lrange" -> {
                val result = db.lrange(
                    processedArgs[0] as String,
                    (processedArgs[1] as Number).toLong(),
                    (processedArgs[2] as Number).toLong()
                )
                result.map { String(it) }
            }
            "lindex" -> db.lindex(processedArgs[0] as String, (processedArgs[1] as Number).toLong())?.let { String(it) }

            // Set commands
            "sadd" -> {
                val key = processedArgs[0] as String
                val members = if (processedArgs[1] is List<*>) {
                    (processedArgs[1] as List<*>).map {
                        when (it) {
                            is ByteArray -> it
                            is String -> it.toByteArray()
                            else -> it.toString().toByteArray()
                        }
                    }.toTypedArray()
                } else {
                    arrayOf(processedArgs[1].toString().toByteArray())
                }
                db.sadd(key, *members)
            }
            "srem" -> {
                val key = processedArgs[0] as String
                val members = if (processedArgs[1] is List<*>) {
                    (processedArgs[1] as List<*>).map {
                        when (it) {
                            is ByteArray -> it
                            is String -> it.toByteArray()
                            else -> it.toString().toByteArray()
                        }
                    }.toTypedArray()
                } else {
                    arrayOf(processedArgs[1].toString().toByteArray())
                }
                db.srem(key, *members)
            }
            "smembers" -> db.smembers(processedArgs[0] as String).map { String(it) }.toSet()
            "sismember" -> db.sismember(processedArgs[0] as String, processedArgs[1].toString().toByteArray())
            "scard" -> db.scard(processedArgs[0] as String)

            // Sorted set commands
            "zadd" -> {
                val key = processedArgs[0] as String
                val members = processedArgs[1] as List<*>
                val zMembers = members.map { item ->
                    when (item) {
                        is List<*> -> {
                            val score = (item[0] as Number).toDouble()
                            val member = when (val m = item[1]) {
                                is ByteArray -> m
                                is String -> m.toByteArray()
                                else -> m.toString().toByteArray()
                            }
                            com.redlite.ZMember(score, member)
                        }
                        else -> throw IllegalArgumentException("Invalid zadd member format")
                    }
                }.toTypedArray()
                db.zadd(key, *zMembers)
            }
            "zscore" -> db.zscore(processedArgs[0] as String, processedArgs[1].toString().toByteArray())
            "zcard" -> db.zcard(processedArgs[0] as String)
            "zcount" -> db.zcount(
                processedArgs[0] as String,
                (processedArgs[1] as Number).toDouble(),
                (processedArgs[2] as Number).toDouble()
            )
            "zrange" -> {
                val result = db.zrange(
                    processedArgs[0] as String,
                    (processedArgs[1] as Number).toLong(),
                    (processedArgs[2] as Number).toLong()
                )
                result.map { if (it is ByteArray) String(it) else it }
            }

            // Multi-key commands
            "mget" -> {
                val keys = if (processedArgs[0] is List<*>) {
                    (processedArgs[0] as List<*>).map { it as String }.toTypedArray()
                } else {
                    processedArgs.map { it as String }.toTypedArray()
                }
                db.mget(*keys).map { it?.let { String(it) } }
            }
            "mset" -> {
                val pairs = (processedArgs as List<*>).associate { pair ->
                    val p = pair as List<*>
                    p[0] as String to (p[1] as String).toByteArray()
                }
                db.mset(pairs)
            }

            else -> throw IllegalArgumentException("Unknown command: $cmd")
        }
    }

    @Suppress("UNCHECKED_CAST")
    private fun processArg(arg: Any): Any {
        return when (arg) {
            is Map<*, *> -> {
                if (arg.containsKey("bytes")) {
                    (arg["bytes"] as List<Number>).map { it.toByte() }.toByteArray()
                } else {
                    arg
                }
            }
            else -> arg
        }
    }

    @Suppress("UNCHECKED_CAST")
    private fun compare(actual: Any?, expected: Any?): Boolean {
        if (expected == null) {
            return actual == null || (actual is ByteArray && actual.isEmpty())
        }

        return when (expected) {
            is Map<*, *> -> compareSpecial(actual, expected)
            is Boolean -> actual == expected
            is Number -> compareNumber(actual, expected)
            is String -> compareString(actual, expected)
            is List<*> -> compareList(actual, expected)
            else -> actual == expected
        }
    }

    @Suppress("UNCHECKED_CAST")
    private fun compareSpecial(actual: Any?, expected: Map<*, *>): Boolean {
        when {
            "range" in expected -> {
                val bounds = expected["range"] as List<Number>
                val value = (actual as? Number)?.toLong() ?: return false
                return value >= bounds[0].toLong() && value <= bounds[1].toLong()
            }
            "approx" in expected -> {
                val target = (expected["approx"] as Number).toDouble()
                val tol = (expected["tol"] as? Number)?.toDouble() ?: 0.001
                val value = (actual as? Number)?.toDouble() ?: return false
                return abs(value - target) <= tol
            }
            "set" in expected -> {
                val expSet = (expected["set"] as List<*>).map { it.toString() }.toSet()
                val actSet = when (actual) {
                    is Set<*> -> actual.map {
                        if (it is ByteArray) String(it) else it.toString()
                    }.toSet()
                    is List<*> -> actual.map {
                        if (it is ByteArray) String(it) else it.toString()
                    }.toSet()
                    else -> return false
                }
                return actSet == expSet
            }
            "dict" in expected -> {
                val expDict = expected["dict"] as Map<*, *>
                val actDict = actual as? Map<*, *> ?: return false
                return expDict.all { (k, v) ->
                    val actVal = actDict[k]
                    when (actVal) {
                        is ByteArray -> String(actVal) == v.toString()
                        else -> actVal?.toString() == v.toString()
                    }
                }
            }
            "bytes" in expected -> {
                val expBytes = (expected["bytes"] as List<Number>).map { it.toByte() }.toByteArray()
                return (actual as? ByteArray)?.contentEquals(expBytes) == true
            }
            else -> return false
        }
    }

    private fun compareString(actual: Any?, expected: String): Boolean {
        return when (actual) {
            is ByteArray -> String(actual) == expected
            is String -> actual == expected
            else -> false
        }
    }

    private fun compareNumber(actual: Any?, expected: Number): Boolean {
        return when (actual) {
            is Number -> actual.toLong() == expected.toLong()
            else -> false
        }
    }

    private fun compareList(actual: Any?, expected: List<*>): Boolean {
        val actList = when (actual) {
            is List<*> -> actual
            else -> return false
        }
        if (actList.size != expected.size) return false
        return actList.zip(expected).all { (a, e) -> compare(a, e) }
    }

    private fun serialize(value: Any?): Any? {
        return when (value) {
            is ByteArray -> String(value)
            is List<*> -> value.map { serialize(it) }
            is Set<*> -> value.map { serialize(it) }.toSet()
            is Map<*, *> -> value.mapValues { serialize(it.value) }
            else -> value
        }
    }

    fun summary(): String {
        val total = passed + failed
        return if (skipped > 0) {
            "$passed/$total passed, $failed failed, $skipped skipped"
        } else {
            "$passed/$total passed, $failed failed"
        }
    }

    fun printErrors() {
        if (errors.isNotEmpty()) {
            println("\nFailed tests:")
            for (error in errors) {
                println("  ${error.spec} / ${error.test}")
                println("    Command: ${error.cmd}")
                println("    Expected: ${error.expected}")
                println("    Actual: ${error.actual}")
            }
        }
    }
}

fun main(args: Array<String>) {
    val verbose = "-v" in args || "--verbose" in args
    val specFiles = args.filter { !it.startsWith("-") }.ifEmpty {
        File("../spec").listFiles()
            ?.filter { it.extension == "yaml" }
            ?.map { it.path }
            ?: emptyList()
    }

    val runner = OracleRunner(verbose)
    specFiles.forEach { runner.runSpecFile(it) }

    println("=".repeat(60))
    println("Oracle Test Results: ${runner.summary()}")
    println("=".repeat(60))
    runner.printErrors()

    System.exit(if (runner.summary().contains("0 failed")) 0 else 1)
}
