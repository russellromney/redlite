package com.redlite

import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.DisplayName

/**
 * Unit tests for string commands.
 */
@DisplayName("Strings Commands")
class StringsTest {
    private lateinit var client: Redlite

    @BeforeEach
    fun setUp() {
        client = Redlite.open(":memory:")
    }

    @AfterEach
    fun tearDown() {
        client.close()
    }

    @Test
    @DisplayName("SET and GET basic operations")
    fun testSetAndGet() {
        val result = client.set("key1", "value1".toByteArray())
        assertTrue(result)

        val value = client.get("key1")
        assertNotNull(value)
        assertEquals("value1", String(value!!))
    }

    @Test
    @DisplayName("GET returns null for non-existent key")
    fun testGetNonExistent() {
        val value = client.get("nonexistent")
        assertNull(value)
    }

    @Test
    @DisplayName("SET with NX option - only set if not exists")
    fun testSetNx() {
        val opts = SetOptions.Builder().nx().build()

        // First SET should succeed
        assertTrue(client.set("nxkey", "first".toByteArray(), opts))
        assertEquals("first", String(client.get("nxkey")!!))

        // Second SET with NX should fail (key exists)
        assertFalse(client.set("nxkey", "second".toByteArray(), opts))
        assertEquals("first", String(client.get("nxkey")!!))
    }

    @Test
    @DisplayName("SET with XX option - only set if exists")
    fun testSetXx() {
        val opts = SetOptions.Builder().xx().build()

        // SET with XX on non-existent key should fail
        assertFalse(client.set("xxkey", "value".toByteArray(), opts))
        assertNull(client.get("xxkey"))

        // Create the key first
        client.set("xxkey", "initial".toByteArray())

        // Now SET with XX should succeed
        assertTrue(client.set("xxkey", "updated".toByteArray(), opts))
        assertEquals("updated", String(client.get("xxkey")!!))
    }

    @Test
    @DisplayName("SET with EX option - expiration in seconds")
    fun testSetWithExpiry() {
        val opts = SetOptions.Builder().ex(10).build()
        assertTrue(client.set("expkey", "value".toByteArray(), opts))

        val ttl = client.ttl("expkey")
        assertTrue(ttl > 0 && ttl <= 10)
    }

    @Test
    @DisplayName("MSET and MGET multiple keys")
    fun testMsetAndMget() {
        val keyValues = mapOf(
            "mkey1" to "mval1".toByteArray(),
            "mkey2" to "mval2".toByteArray(),
            "mkey3" to "mval3".toByteArray()
        )

        val result = client.mset(keyValues)
        assertTrue(result)

        val values = client.mget("mkey1", "mkey2", "mkey3", "nonexistent")
        assertEquals(4, values.size)
        assertEquals("mval1", String(values[0]!!))
        assertEquals("mval2", String(values[1]!!))
        assertEquals("mval3", String(values[2]!!))
        assertNull(values[3])
    }

    @Test
    @DisplayName("INCR and DECR operations")
    fun testIncrDecr() {
        client.set("counter", "10".toByteArray())

        assertEquals(11, client.incr("counter"))
        assertEquals(12, client.incr("counter"))
        assertEquals(11, client.decr("counter"))
        assertEquals(10, client.decr("counter"))
    }

    @Test
    @DisplayName("INCR on non-existent key starts from 0")
    fun testIncrNonExistent() {
        assertEquals(1, client.incr("newcounter"))
        assertEquals(2, client.incr("newcounter"))
    }

    @Test
    @DisplayName("INCRBY and DECRBY operations")
    fun testIncrByDecrBy() {
        client.set("counter", "100".toByteArray())

        assertEquals(110, client.incrBy("counter", 10))
        assertEquals(85, client.decrBy("counter", 25))
    }

    @Test
    @DisplayName("INCRBYFLOAT operation")
    fun testIncrByFloat() {
        client.set("floatkey", "10.5".toByteArray())

        val result = client.incrByFloat("floatkey", 2.5)
        assertEquals(13.0, result, 0.001)
    }

    @Test
    @DisplayName("APPEND to existing key")
    fun testAppend() {
        client.set("appendkey", "Hello".toByteArray())

        val newLen = client.append("appendkey", " World".toByteArray())
        assertEquals(11, newLen)
        assertEquals("Hello World", String(client.get("appendkey")!!))
    }

    @Test
    @DisplayName("APPEND to non-existent key creates it")
    fun testAppendNonExistent() {
        val len = client.append("newappend", "value".toByteArray())
        assertEquals(5, len)
        assertEquals("value", String(client.get("newappend")!!))
    }

    @Test
    @DisplayName("STRLEN returns correct length")
    fun testStrlen() {
        client.set("strlenkey", "Hello World".toByteArray())
        assertEquals(11, client.strlen("strlenkey"))

        // Non-existent key returns 0
        assertEquals(0, client.strlen("nonexistent"))
    }

    @Test
    @DisplayName("GETRANGE returns substring")
    fun testGetRange() {
        client.set("rangekey", "Hello World".toByteArray())

        assertEquals("Hello", String(client.getRange("rangekey", 0, 4)))
        assertEquals("World", String(client.getRange("rangekey", 6, 10)))
        assertEquals("World", String(client.getRange("rangekey", -5, -1)))
    }

    @Test
    @DisplayName("SETRANGE modifies part of string")
    fun testSetRange() {
        client.set("setrangekey", "Hello World".toByteArray())

        val newLen = client.setRange("setrangekey", 6, "Redis".toByteArray())
        assertEquals(11, newLen)
        assertEquals("Hello Redis", String(client.get("setrangekey")!!))
    }

    @Test
    @DisplayName("SETNX sets only if not exists")
    fun testSetNxCommand() {
        assertTrue(client.setNx("setnxkey", "first".toByteArray()))
        assertFalse(client.setNx("setnxkey", "second".toByteArray()))
        assertEquals("first", String(client.get("setnxkey")!!))
    }

    @Test
    @DisplayName("GETSET returns old value and sets new")
    fun testGetSet() {
        client.set("getsetkey", "old".toByteArray())

        val oldValue = client.getSet("getsetkey", "new".toByteArray())
        assertEquals("old", String(oldValue!!))
        assertEquals("new", String(client.get("getsetkey")!!))
    }

    @Test
    @DisplayName("GETSET on non-existent key returns null")
    fun testGetSetNonExistent() {
        val oldValue = client.getSet("newgetset", "value".toByteArray())
        assertNull(oldValue)
        assertEquals("value", String(client.get("newgetset")!!))
    }
}
