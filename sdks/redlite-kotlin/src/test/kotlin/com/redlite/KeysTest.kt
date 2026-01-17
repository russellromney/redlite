package com.redlite

import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.DisplayName

/**
 * Unit tests for key commands.
 */
@DisplayName("Keys Commands")
class KeysTest {
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
    @DisplayName("DEL removes existing keys")
    fun testDel() {
        client.set("del1", "v1".toByteArray())
        client.set("del2", "v2".toByteArray())
        client.set("del3", "v3".toByteArray())

        assertEquals(2, client.del("del1", "del2", "nonexistent"))
        assertNull(client.get("del1"))
        assertNull(client.get("del2"))
        assertNotNull(client.get("del3"))
    }

    @Test
    @DisplayName("EXISTS checks key existence")
    fun testExists() {
        client.set("exists1", "v1".toByteArray())
        client.set("exists2", "v2".toByteArray())

        assertEquals(2, client.exists("exists1", "exists2", "nonexistent"))
        assertEquals(0, client.exists("none1", "none2"))
    }

    @Test
    @DisplayName("TYPE returns correct key type")
    fun testType() {
        client.set("strkey", "value".toByteArray())
        assertEquals("string", client.type("strkey"))

        client.lpush("listkey", "item".toByteArray())
        assertEquals("list", client.type("listkey"))

        client.hset("hashkey", "field", "value".toByteArray())
        assertEquals("hash", client.type("hashkey"))

        client.sadd("setkey", "member".toByteArray())
        assertEquals("set", client.type("setkey"))

        client.zadd("zsetkey", ZMember("member", 1.0))
        assertEquals("zset", client.type("zsetkey"))

        assertEquals("none", client.type("nonexistent"))
    }

    @Test
    @DisplayName("RENAME renames a key")
    fun testRename() {
        client.set("oldname", "value".toByteArray())

        assertTrue(client.rename("oldname", "newname"))
        assertNull(client.get("oldname"))
        assertEquals("value", String(client.get("newname")!!))
    }

    @Test
    @DisplayName("RENAMENX renames only if new key doesn't exist")
    fun testRenameNx() {
        client.set("src", "value".toByteArray())
        client.set("existing", "other".toByteArray())

        assertFalse(client.renameNx("src", "existing"))
        assertEquals("value", String(client.get("src")!!))

        assertTrue(client.renameNx("src", "newdest"))
        assertNull(client.get("src"))
        assertEquals("value", String(client.get("newdest")!!))
    }

    @Test
    @DisplayName("KEYS returns matching keys")
    fun testKeys() {
        client.set("user:1", "a".toByteArray())
        client.set("user:2", "b".toByteArray())
        client.set("user:3", "c".toByteArray())
        client.set("other:1", "d".toByteArray())

        val userKeys = client.keys("user:*")
        assertEquals(3, userKeys.size)
        assertTrue(userKeys.containsAll(listOf("user:1", "user:2", "user:3")))

        val allKeys = client.keys("*")
        assertEquals(4, allKeys.size)
    }

    @Test
    @DisplayName("EXPIRE and TTL operations")
    fun testExpireAndTtl() {
        client.set("expirekey", "value".toByteArray())

        // Key without expiry
        assertEquals(-1, client.ttl("expirekey"))

        // Set expiry
        assertTrue(client.expire("expirekey", 100))
        val ttl = client.ttl("expirekey")
        assertTrue(ttl > 0 && ttl <= 100)

        // Non-existent key
        assertEquals(-2, client.ttl("nonexistent"))
    }

    @Test
    @DisplayName("EXPIREAT sets expiration timestamp")
    fun testExpireAt() {
        client.set("expireatkey", "value".toByteArray())

        val futureTime = System.currentTimeMillis() / 1000 + 3600 // 1 hour from now
        assertTrue(client.expireAt("expireatkey", futureTime))

        val ttl = client.ttl("expireatkey")
        assertTrue(ttl > 3500 && ttl <= 3600)
    }

    @Test
    @DisplayName("PEXPIRE and PTTL millisecond operations")
    fun testPexpireAndPttl() {
        client.set("pexpirekey", "value".toByteArray())

        assertTrue(client.pexpire("pexpirekey", 10000)) // 10 seconds
        val pttl = client.pttl("pexpirekey")
        assertTrue(pttl > 0 && pttl <= 10000)
    }

    @Test
    @DisplayName("PERSIST removes expiration")
    fun testPersist() {
        client.set("persistkey", "value".toByteArray())
        client.expire("persistkey", 100)

        assertTrue(client.ttl("persistkey") > 0)

        assertTrue(client.persist("persistkey"))
        assertEquals(-1, client.ttl("persistkey"))
    }

    @Test
    @DisplayName("COPY duplicates a key")
    fun testCopy() {
        client.set("src", "value".toByteArray())

        assertTrue(client.copy("src", "dst"))
        assertEquals("value", String(client.get("dst")!!))
        assertEquals("value", String(client.get("src")!!)) // Original still exists
    }

    @Test
    @DisplayName("COPY with REPLACE option")
    fun testCopyReplace() {
        client.set("src", "newvalue".toByteArray())
        client.set("dst", "oldvalue".toByteArray())

        assertTrue(client.copy("src", "dst", replace = true))
        assertEquals("newvalue", String(client.get("dst")!!))
    }

    @Test
    @DisplayName("DUMP and RESTORE key serialization")
    fun testDumpRestore() {
        client.set("dumpkey", "dumpvalue".toByteArray())

        val serialized = client.dump("dumpkey")
        assertNotNull(serialized)
        assertTrue(serialized!!.isNotEmpty())

        client.del("dumpkey")
        assertNull(client.get("dumpkey"))

        assertTrue(client.restore("dumpkey", 0, serialized))
        assertEquals("dumpvalue", String(client.get("dumpkey")!!))
    }

    @Test
    @DisplayName("OBJECT ENCODING returns encoding type")
    fun testObjectEncoding() {
        client.set("intkey", "12345".toByteArray())
        // Could be "int" or "embstr" depending on implementation
        val encoding = client.objectEncoding("intkey")
        assertNotNull(encoding)

        client.set("strkey", "this is a longer string value".toByteArray())
        val strEncoding = client.objectEncoding("strkey")
        assertNotNull(strEncoding)
    }

    @Test
    @DisplayName("RANDOMKEY returns a random key")
    fun testRandomKey() {
        // Empty database
        assertNull(client.randomKey())

        client.set("rkey1", "v1".toByteArray())
        client.set("rkey2", "v2".toByteArray())

        val randomKey = client.randomKey()
        assertNotNull(randomKey)
        assertTrue(randomKey in listOf("rkey1", "rkey2"))
    }

    @Test
    @DisplayName("TOUCH updates access time")
    fun testTouch() {
        client.set("touch1", "v1".toByteArray())
        client.set("touch2", "v2".toByteArray())

        assertEquals(2, client.touch("touch1", "touch2", "nonexistent"))
    }

    @Test
    @DisplayName("UNLINK removes keys asynchronously")
    fun testUnlink() {
        client.set("unlink1", "v1".toByteArray())
        client.set("unlink2", "v2".toByteArray())

        assertEquals(2, client.unlink("unlink1", "unlink2"))
        assertNull(client.get("unlink1"))
        assertNull(client.get("unlink2"))
    }

    @Test
    @DisplayName("SCAN iterates through keys")
    fun testScan() {
        // Add some keys
        for (i in 1..10) {
            client.set("scan:$i", "value$i".toByteArray())
        }

        val allKeys = mutableSetOf<String>()
        var cursor = "0"

        do {
            val result = client.scan(cursor)
            cursor = result.cursor
            allKeys.addAll(result.keys)
        } while (cursor != "0")

        assertEquals(10, allKeys.size)
        for (i in 1..10) {
            assertTrue("scan:$i" in allKeys)
        }
    }

    @Test
    @DisplayName("SCAN with MATCH pattern")
    fun testScanWithMatch() {
        for (i in 1..5) {
            client.set("user:$i", "u$i".toByteArray())
            client.set("order:$i", "o$i".toByteArray())
        }

        val userKeys = mutableSetOf<String>()
        var cursor = "0"

        do {
            val result = client.scan(cursor, match = "user:*")
            cursor = result.cursor
            userKeys.addAll(result.keys)
        } while (cursor != "0")

        assertEquals(5, userKeys.size)
        userKeys.forEach { assertTrue(it.startsWith("user:")) }
    }

    @Test
    @DisplayName("SCAN with COUNT hint")
    fun testScanWithCount() {
        for (i in 1..100) {
            client.set("count:$i", "v$i".toByteArray())
        }

        val result = client.scan("0", count = 10)
        // COUNT is a hint, not a guarantee, but should return some results
        assertTrue(result.keys.isNotEmpty())
    }
}
