package com.redlite

import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.DisplayName

/**
 * Unit tests for list commands.
 */
@DisplayName("List Commands")
class ListsTest {
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
    @DisplayName("LPUSH and RPUSH add elements")
    fun testLpushRpush() {
        assertEquals(1, client.lpush("list", "first".toByteArray()))
        assertEquals(2, client.lpush("list", "second".toByteArray()))

        // List is now: [second, first]
        assertEquals(3, client.rpush("list", "third".toByteArray()))
        // List is now: [second, first, third]

        val elements = client.lrange("list", 0, -1)
        assertEquals(3, elements.size)
        assertEquals("second", String(elements[0]))
        assertEquals("first", String(elements[1]))
        assertEquals("third", String(elements[2]))
    }

    @Test
    @DisplayName("LPUSH with multiple values")
    fun testLpushMultiple() {
        val count = client.lpush("list", "a".toByteArray(), "b".toByteArray(), "c".toByteArray())
        assertEquals(3, count)

        // Order: c, b, a (last pushed is at head)
        val elements = client.lrange("list", 0, -1)
        assertEquals("c", String(elements[0]))
        assertEquals("b", String(elements[1]))
        assertEquals("a", String(elements[2]))
    }

    @Test
    @DisplayName("LPOP and RPOP remove elements")
    fun testLpopRpop() {
        client.rpush("list", "a".toByteArray(), "b".toByteArray(), "c".toByteArray())

        assertEquals("a", String(client.lpop("list")!!))
        assertEquals("c", String(client.rpop("list")!!))
        assertEquals("b", String(client.lpop("list")!!))
        assertNull(client.lpop("list"))
    }

    @Test
    @DisplayName("LPOP and RPOP with count")
    fun testLpopRpopCount() {
        client.rpush("list", "1".toByteArray(), "2".toByteArray(), "3".toByteArray(), "4".toByteArray(), "5".toByteArray())

        val left = client.lpop("list", 2)
        assertEquals(2, left.size)
        assertEquals("1", String(left[0]))
        assertEquals("2", String(left[1]))

        val right = client.rpop("list", 2)
        assertEquals(2, right.size)
        assertEquals("5", String(right[0]))
        assertEquals("4", String(right[1]))
    }

    @Test
    @DisplayName("LINDEX gets element by index")
    fun testLindex() {
        client.rpush("list", "a".toByteArray(), "b".toByteArray(), "c".toByteArray())

        assertEquals("a", String(client.lindex("list", 0)!!))
        assertEquals("b", String(client.lindex("list", 1)!!))
        assertEquals("c", String(client.lindex("list", 2)!!))
        assertEquals("c", String(client.lindex("list", -1)!!))
        assertNull(client.lindex("list", 10))
    }

    @Test
    @DisplayName("LLEN returns list length")
    fun testLlen() {
        assertEquals(0, client.llen("nonexistent"))

        client.rpush("list", "a".toByteArray(), "b".toByteArray(), "c".toByteArray())
        assertEquals(3, client.llen("list"))
    }

    @Test
    @DisplayName("LRANGE returns range of elements")
    fun testLrange() {
        client.rpush("list", "a".toByteArray(), "b".toByteArray(), "c".toByteArray(), "d".toByteArray(), "e".toByteArray())

        val range1 = client.lrange("list", 0, 2)
        assertEquals(3, range1.size)
        assertEquals("a", String(range1[0]))
        assertEquals("b", String(range1[1]))
        assertEquals("c", String(range1[2]))

        val range2 = client.lrange("list", -2, -1)
        assertEquals(2, range2.size)
        assertEquals("d", String(range2[0]))
        assertEquals("e", String(range2[1]))
    }

    @Test
    @DisplayName("LSET updates element at index")
    fun testLset() {
        client.rpush("list", "a".toByteArray(), "b".toByteArray(), "c".toByteArray())

        assertTrue(client.lset("list", 1, "B".toByteArray()))
        assertEquals("B", String(client.lindex("list", 1)!!))
    }

    @Test
    @DisplayName("LINSERT inserts before or after pivot")
    fun testLinsert() {
        client.rpush("list", "a".toByteArray(), "c".toByteArray())

        // Insert before "c"
        assertEquals(3, client.linsert("list", "BEFORE", "c".toByteArray(), "b".toByteArray()))

        val elements = client.lrange("list", 0, -1)
        assertEquals(listOf("a", "b", "c"), elements.map { String(it) })

        // Insert after "c"
        assertEquals(4, client.linsert("list", "AFTER", "c".toByteArray(), "d".toByteArray()))

        val elements2 = client.lrange("list", 0, -1)
        assertEquals(listOf("a", "b", "c", "d"), elements2.map { String(it) })
    }

    @Test
    @DisplayName("LTRIM trims list to specified range")
    fun testLtrim() {
        client.rpush("list", "1".toByteArray(), "2".toByteArray(), "3".toByteArray(), "4".toByteArray(), "5".toByteArray())

        assertTrue(client.ltrim("list", 1, 3))

        val elements = client.lrange("list", 0, -1)
        assertEquals(listOf("2", "3", "4"), elements.map { String(it) })
    }

    @Test
    @DisplayName("LREM removes matching elements")
    fun testLrem() {
        client.rpush("list", "a".toByteArray(), "b".toByteArray(), "a".toByteArray(), "c".toByteArray(), "a".toByteArray())

        // Remove 2 occurrences of "a" from head
        assertEquals(2, client.lrem("list", 2, "a".toByteArray()))

        val elements = client.lrange("list", 0, -1)
        assertEquals(listOf("b", "c", "a"), elements.map { String(it) })
    }

    @Test
    @DisplayName("LREM with negative count removes from tail")
    fun testLremFromTail() {
        client.rpush("list", "a".toByteArray(), "b".toByteArray(), "a".toByteArray(), "c".toByteArray(), "a".toByteArray())

        // Remove 2 occurrences of "a" from tail
        assertEquals(2, client.lrem("list", -2, "a".toByteArray()))

        val elements = client.lrange("list", 0, -1)
        assertEquals(listOf("a", "b", "c"), elements.map { String(it) })
    }

    @Test
    @DisplayName("LPOS finds position of element")
    fun testLpos() {
        client.rpush("list", "a".toByteArray(), "b".toByteArray(), "c".toByteArray(), "b".toByteArray(), "d".toByteArray())

        assertEquals(1, client.lpos("list", "b".toByteArray()))
        assertEquals(0, client.lpos("list", "a".toByteArray()))
        assertNull(client.lpos("list", "z".toByteArray()))
    }

    @Test
    @DisplayName("LMOVE moves element between lists")
    fun testLmove() {
        client.rpush("src", "1".toByteArray(), "2".toByteArray(), "3".toByteArray())
        client.rpush("dst", "a".toByteArray())

        val moved = client.lmove("src", "dst", "LEFT", "RIGHT")
        assertEquals("1", String(moved!!))

        assertEquals(listOf("2", "3"), client.lrange("src", 0, -1).map { String(it) })
        assertEquals(listOf("a", "1"), client.lrange("dst", 0, -1).map { String(it) })
    }

    @Test
    @DisplayName("LPUSHX only pushes to existing list")
    fun testLpushX() {
        // Non-existent list
        assertEquals(0, client.lpushx("newlist", "value".toByteArray()))
        assertEquals(0, client.llen("newlist"))

        // Existing list
        client.lpush("list", "first".toByteArray())
        assertEquals(2, client.lpushx("list", "second".toByteArray()))
    }

    @Test
    @DisplayName("RPUSHX only pushes to existing list")
    fun testRpushX() {
        assertEquals(0, client.rpushx("newlist", "value".toByteArray()))

        client.rpush("list", "first".toByteArray())
        assertEquals(2, client.rpushx("list", "second".toByteArray()))
    }
}
