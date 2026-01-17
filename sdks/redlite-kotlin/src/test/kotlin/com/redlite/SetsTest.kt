package com.redlite

import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.DisplayName

/**
 * Unit tests for set commands.
 */
@DisplayName("Set Commands")
class SetsTest {
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
    @DisplayName("SADD adds members to set")
    fun testSadd() {
        assertEquals(3, client.sadd("set", "a".toByteArray(), "b".toByteArray(), "c".toByteArray()))

        // Adding existing members returns 0
        assertEquals(0, client.sadd("set", "a".toByteArray()))

        // Mixed new and existing
        assertEquals(1, client.sadd("set", "a".toByteArray(), "d".toByteArray()))
    }

    @Test
    @DisplayName("SMEMBERS returns all members")
    fun testSmembers() {
        client.sadd("set", "a".toByteArray(), "b".toByteArray(), "c".toByteArray())

        val members = client.smembers("set")
        assertEquals(3, members.size)
        assertTrue(members.map { String(it) }.containsAll(listOf("a", "b", "c")))
    }

    @Test
    @DisplayName("SISMEMBER checks membership")
    fun testSismember() {
        client.sadd("set", "a".toByteArray(), "b".toByteArray())

        assertTrue(client.sismember("set", "a".toByteArray()))
        assertFalse(client.sismember("set", "z".toByteArray()))
    }

    @Test
    @DisplayName("SMISMEMBER checks multiple memberships")
    fun testSmismember() {
        client.sadd("set", "a".toByteArray(), "b".toByteArray(), "c".toByteArray())

        val results = client.smismember("set", "a".toByteArray(), "z".toByteArray(), "c".toByteArray())
        assertEquals(listOf(true, false, true), results)
    }

    @Test
    @DisplayName("SCARD returns cardinality")
    fun testScard() {
        assertEquals(0, client.scard("nonexistent"))

        client.sadd("set", "a".toByteArray(), "b".toByteArray(), "c".toByteArray())
        assertEquals(3, client.scard("set"))
    }

    @Test
    @DisplayName("SREM removes members")
    fun testSrem() {
        client.sadd("set", "a".toByteArray(), "b".toByteArray(), "c".toByteArray())

        assertEquals(2, client.srem("set", "a".toByteArray(), "c".toByteArray(), "z".toByteArray()))
        assertEquals(1, client.scard("set"))
        assertTrue(client.sismember("set", "b".toByteArray()))
    }

    @Test
    @DisplayName("SPOP removes and returns random member")
    fun testSpop() {
        client.sadd("set", "a".toByteArray(), "b".toByteArray(), "c".toByteArray())

        val popped = client.spop("set")
        assertNotNull(popped)
        assertEquals(2, client.scard("set"))
    }

    @Test
    @DisplayName("SPOP with count removes multiple")
    fun testSpopCount() {
        client.sadd("set", "a".toByteArray(), "b".toByteArray(), "c".toByteArray(), "d".toByteArray(), "e".toByteArray())

        val popped = client.spop("set", 3)
        assertEquals(3, popped.size)
        assertEquals(2, client.scard("set"))
    }

    @Test
    @DisplayName("SRANDMEMBER returns random member without removal")
    fun testSrandmember() {
        client.sadd("set", "a".toByteArray(), "b".toByteArray(), "c".toByteArray())

        val member = client.srandmember("set")
        assertNotNull(member)
        assertEquals(3, client.scard("set")) // Not removed
    }

    @Test
    @DisplayName("SRANDMEMBER with count")
    fun testSrandmemberCount() {
        client.sadd("set", "a".toByteArray(), "b".toByteArray(), "c".toByteArray())

        val members = client.srandmember("set", 2)
        assertEquals(2, members.size)
        assertEquals(3, client.scard("set")) // Not removed
    }

    @Test
    @DisplayName("SMOVE moves member between sets")
    fun testSmove() {
        client.sadd("src", "a".toByteArray(), "b".toByteArray())
        client.sadd("dst", "x".toByteArray())

        assertTrue(client.smove("src", "dst", "a".toByteArray()))

        assertFalse(client.sismember("src", "a".toByteArray()))
        assertTrue(client.sismember("dst", "a".toByteArray()))
    }

    @Test
    @DisplayName("SUNION returns union of sets")
    fun testSunion() {
        client.sadd("set1", "a".toByteArray(), "b".toByteArray())
        client.sadd("set2", "b".toByteArray(), "c".toByteArray())
        client.sadd("set3", "c".toByteArray(), "d".toByteArray())

        val union = client.sunion("set1", "set2", "set3")
        assertEquals(4, union.size)
        assertTrue(union.map { String(it) }.containsAll(listOf("a", "b", "c", "d")))
    }

    @Test
    @DisplayName("SUNIONSTORE stores union result")
    fun testSunionStore() {
        client.sadd("set1", "a".toByteArray(), "b".toByteArray())
        client.sadd("set2", "c".toByteArray(), "d".toByteArray())

        assertEquals(4, client.sunionstore("dest", "set1", "set2"))
        assertEquals(4, client.scard("dest"))
    }

    @Test
    @DisplayName("SINTER returns intersection of sets")
    fun testSinter() {
        client.sadd("set1", "a".toByteArray(), "b".toByteArray(), "c".toByteArray())
        client.sadd("set2", "b".toByteArray(), "c".toByteArray(), "d".toByteArray())
        client.sadd("set3", "c".toByteArray(), "d".toByteArray(), "e".toByteArray())

        val inter = client.sinter("set1", "set2", "set3")
        assertEquals(1, inter.size)
        assertEquals("c", String(inter.first()))
    }

    @Test
    @DisplayName("SINTERSTORE stores intersection result")
    fun testSinterStore() {
        client.sadd("set1", "a".toByteArray(), "b".toByteArray())
        client.sadd("set2", "b".toByteArray(), "c".toByteArray())

        assertEquals(1, client.sinterstore("dest", "set1", "set2"))
        assertTrue(client.sismember("dest", "b".toByteArray()))
    }

    @Test
    @DisplayName("SINTERCARD returns intersection cardinality")
    fun testSinterCard() {
        client.sadd("set1", "a".toByteArray(), "b".toByteArray(), "c".toByteArray())
        client.sadd("set2", "b".toByteArray(), "c".toByteArray(), "d".toByteArray())

        assertEquals(2, client.sintercard("set1", "set2"))
    }

    @Test
    @DisplayName("SINTERCARD with limit")
    fun testSinterCardLimit() {
        client.sadd("set1", "a".toByteArray(), "b".toByteArray(), "c".toByteArray())
        client.sadd("set2", "a".toByteArray(), "b".toByteArray(), "c".toByteArray())

        assertEquals(2, client.sintercard(listOf("set1", "set2"), limit = 2))
    }

    @Test
    @DisplayName("SDIFF returns difference of sets")
    fun testSdiff() {
        client.sadd("set1", "a".toByteArray(), "b".toByteArray(), "c".toByteArray())
        client.sadd("set2", "b".toByteArray(), "d".toByteArray())

        val diff = client.sdiff("set1", "set2")
        assertEquals(2, diff.size)
        assertTrue(diff.map { String(it) }.containsAll(listOf("a", "c")))
    }

    @Test
    @DisplayName("SDIFFSTORE stores difference result")
    fun testSdiffStore() {
        client.sadd("set1", "a".toByteArray(), "b".toByteArray(), "c".toByteArray())
        client.sadd("set2", "b".toByteArray())

        assertEquals(2, client.sdiffstore("dest", "set1", "set2"))
    }

    @Test
    @DisplayName("SSCAN iterates through members")
    fun testSscan() {
        for (i in 1..10) {
            client.sadd("set", "member$i".toByteArray())
        }

        val allMembers = mutableSetOf<String>()
        var cursor = "0"

        do {
            val result = client.sscan("set", cursor)
            cursor = result.cursor
            allMembers.addAll(result.members.map { String(it) })
        } while (cursor != "0")

        assertEquals(10, allMembers.size)
    }

    @Test
    @DisplayName("SSCAN with MATCH pattern")
    fun testSscanWithMatch() {
        for (i in 1..5) {
            client.sadd("set", "user:$i".toByteArray())
            client.sadd("set", "order:$i".toByteArray())
        }

        val userMembers = mutableSetOf<String>()
        var cursor = "0"

        do {
            val result = client.sscan("set", cursor, match = "user:*")
            cursor = result.cursor
            userMembers.addAll(result.members.map { String(it) })
        } while (cursor != "0")

        assertEquals(5, userMembers.size)
        userMembers.forEach { assertTrue(it.startsWith("user:")) }
    }
}
