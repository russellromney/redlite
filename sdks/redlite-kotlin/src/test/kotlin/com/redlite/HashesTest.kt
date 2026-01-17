package com.redlite

import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.DisplayName

/**
 * Unit tests for hash commands.
 */
@DisplayName("Hash Commands")
class HashesTest {
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
    @DisplayName("HSET and HGET basic operations")
    fun testHsetAndHget() {
        assertEquals(1, client.hset("hash", "field1", "value1".toByteArray()))

        val value = client.hget("hash", "field1")
        assertNotNull(value)
        assertEquals("value1", String(value!!))
    }

    @Test
    @DisplayName("HGET returns null for non-existent field")
    fun testHgetNonExistent() {
        assertNull(client.hget("hash", "nonexistent"))

        client.hset("hash", "field", "value".toByteArray())
        assertNull(client.hget("hash", "otherfield"))
    }

    @Test
    @DisplayName("HSET updates existing field")
    fun testHsetUpdate() {
        client.hset("hash", "field", "old".toByteArray())

        // Updating returns 0 (field already exists)
        assertEquals(0, client.hset("hash", "field", "new".toByteArray()))
        assertEquals("new", String(client.hget("hash", "field")!!))
    }

    @Test
    @DisplayName("HMSET sets multiple fields")
    fun testHmset() {
        val fields = mapOf(
            "f1" to "v1".toByteArray(),
            "f2" to "v2".toByteArray(),
            "f3" to "v3".toByteArray()
        )

        assertTrue(client.hmset("hash", fields))

        assertEquals("v1", String(client.hget("hash", "f1")!!))
        assertEquals("v2", String(client.hget("hash", "f2")!!))
        assertEquals("v3", String(client.hget("hash", "f3")!!))
    }

    @Test
    @DisplayName("HMGET gets multiple fields")
    fun testHmget() {
        client.hset("hash", "f1", "v1".toByteArray())
        client.hset("hash", "f2", "v2".toByteArray())

        val values = client.hmget("hash", "f1", "f2", "nonexistent")
        assertEquals(3, values.size)
        assertEquals("v1", String(values[0]!!))
        assertEquals("v2", String(values[1]!!))
        assertNull(values[2])
    }

    @Test
    @DisplayName("HDEL removes fields")
    fun testHdel() {
        client.hset("hash", "f1", "v1".toByteArray())
        client.hset("hash", "f2", "v2".toByteArray())
        client.hset("hash", "f3", "v3".toByteArray())

        assertEquals(2, client.hdel("hash", "f1", "f2", "nonexistent"))
        assertNull(client.hget("hash", "f1"))
        assertNull(client.hget("hash", "f2"))
        assertNotNull(client.hget("hash", "f3"))
    }

    @Test
    @DisplayName("HEXISTS checks field existence")
    fun testHexists() {
        client.hset("hash", "field", "value".toByteArray())

        assertTrue(client.hexists("hash", "field"))
        assertFalse(client.hexists("hash", "nonexistent"))
        assertFalse(client.hexists("nonexistent", "field"))
    }

    @Test
    @DisplayName("HLEN returns number of fields")
    fun testHlen() {
        assertEquals(0, client.hlen("nonexistent"))

        client.hset("hash", "f1", "v1".toByteArray())
        assertEquals(1, client.hlen("hash"))

        client.hset("hash", "f2", "v2".toByteArray())
        assertEquals(2, client.hlen("hash"))
    }

    @Test
    @DisplayName("HKEYS returns all field names")
    fun testHkeys() {
        client.hset("hash", "f1", "v1".toByteArray())
        client.hset("hash", "f2", "v2".toByteArray())
        client.hset("hash", "f3", "v3".toByteArray())

        val keys = client.hkeys("hash")
        assertEquals(3, keys.size)
        assertTrue(keys.containsAll(listOf("f1", "f2", "f3")))
    }

    @Test
    @DisplayName("HVALS returns all values")
    fun testHvals() {
        client.hset("hash", "f1", "v1".toByteArray())
        client.hset("hash", "f2", "v2".toByteArray())

        val values = client.hvals("hash")
        assertEquals(2, values.size)
        val stringValues = values.map { String(it) }
        assertTrue(stringValues.containsAll(listOf("v1", "v2")))
    }

    @Test
    @DisplayName("HGETALL returns all field-value pairs")
    fun testHgetall() {
        client.hset("hash", "f1", "v1".toByteArray())
        client.hset("hash", "f2", "v2".toByteArray())

        val all = client.hgetall("hash")
        assertEquals(2, all.size)
        assertEquals("v1", String(all["f1"]!!))
        assertEquals("v2", String(all["f2"]!!))
    }

    @Test
    @DisplayName("HINCRBY increments integer field")
    fun testHincrBy() {
        client.hset("hash", "counter", "10".toByteArray())

        assertEquals(15, client.hincrBy("hash", "counter", 5))
        assertEquals(10, client.hincrBy("hash", "counter", -5))
    }

    @Test
    @DisplayName("HINCRBY on non-existent field starts from 0")
    fun testHincrByNonExistent() {
        assertEquals(5, client.hincrBy("hash", "newcounter", 5))
    }

    @Test
    @DisplayName("HINCRBYFLOAT increments float field")
    fun testHincrByFloat() {
        client.hset("hash", "price", "10.5".toByteArray())

        val result = client.hincrByFloat("hash", "price", 2.5)
        assertEquals(13.0, result, 0.001)
    }

    @Test
    @DisplayName("HSETNX sets field only if not exists")
    fun testHsetNx() {
        assertTrue(client.hsetnx("hash", "field", "first".toByteArray()))
        assertFalse(client.hsetnx("hash", "field", "second".toByteArray()))
        assertEquals("first", String(client.hget("hash", "field")!!))
    }

    @Test
    @DisplayName("HSTRLEN returns field value length")
    fun testHstrlen() {
        client.hset("hash", "field", "Hello World".toByteArray())

        assertEquals(11, client.hstrlen("hash", "field"))
        assertEquals(0, client.hstrlen("hash", "nonexistent"))
    }

    @Test
    @DisplayName("HSCAN iterates through fields")
    fun testHscan() {
        for (i in 1..10) {
            client.hset("hash", "field$i", "value$i".toByteArray())
        }

        val allFields = mutableMapOf<String, ByteArray>()
        var cursor = "0"

        do {
            val result = client.hscan("hash", cursor)
            cursor = result.cursor
            allFields.putAll(result.entries)
        } while (cursor != "0")

        assertEquals(10, allFields.size)
        for (i in 1..10) {
            assertTrue("field$i" in allFields.keys)
            assertEquals("value$i", String(allFields["field$i"]!!))
        }
    }

    @Test
    @DisplayName("HSCAN with MATCH pattern")
    fun testHscanWithMatch() {
        for (i in 1..5) {
            client.hset("hash", "user:$i", "u$i".toByteArray())
            client.hset("hash", "order:$i", "o$i".toByteArray())
        }

        val userFields = mutableMapOf<String, ByteArray>()
        var cursor = "0"

        do {
            val result = client.hscan("hash", cursor, match = "user:*")
            cursor = result.cursor
            userFields.putAll(result.entries)
        } while (cursor != "0")

        assertEquals(5, userFields.size)
        userFields.keys.forEach { assertTrue(it.startsWith("user:")) }
    }

    @Test
    @DisplayName("HRANDFIELD returns random fields")
    fun testHrandfield() {
        for (i in 1..5) {
            client.hset("hash", "f$i", "v$i".toByteArray())
        }

        val field = client.hrandfield("hash")
        assertNotNull(field)
        assertTrue(field!!.startsWith("f"))

        val fields = client.hrandfield("hash", 3)
        assertEquals(3, fields.size)
    }
}
