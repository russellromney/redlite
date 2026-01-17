package com.redlite

import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.DisplayName

/**
 * Unit tests for sorted set commands.
 */
@DisplayName("Sorted Set Commands")
class ZSetsTest {
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
    @DisplayName("ZADD adds members with scores")
    fun testZadd() {
        val count = client.zadd("zset",
            ZMember("a", 1.0),
            ZMember("b", 2.0),
            ZMember("c", 3.0)
        )
        assertEquals(3, count)
    }

    @Test
    @DisplayName("ZADD updates existing member score")
    fun testZaddUpdate() {
        client.zadd("zset", ZMember("a", 1.0))

        // Update score - returns 0 (not new member)
        assertEquals(0, client.zadd("zset", ZMember("a", 5.0)))

        val score = client.zscore("zset", "a")
        assertEquals(5.0, score!!, 0.001)
    }

    @Test
    @DisplayName("ZADD with NX option")
    fun testZaddNx() {
        client.zadd("zset", ZMember("a", 1.0))

        // NX - only add if not exists
        assertEquals(0, client.zadd("zset", listOf(ZMember("a", 5.0)), nx = true))
        assertEquals(1.0, client.zscore("zset", "a")!!, 0.001)

        assertEquals(1, client.zadd("zset", listOf(ZMember("b", 2.0)), nx = true))
    }

    @Test
    @DisplayName("ZADD with XX option")
    fun testZaddXx() {
        client.zadd("zset", ZMember("a", 1.0))

        // XX - only update if exists
        assertEquals(0, client.zadd("zset", listOf(ZMember("b", 2.0)), xx = true))
        assertNull(client.zscore("zset", "b"))

        assertEquals(0, client.zadd("zset", listOf(ZMember("a", 5.0)), xx = true))
        assertEquals(5.0, client.zscore("zset", "a")!!, 0.001)
    }

    @Test
    @DisplayName("ZSCORE returns member score")
    fun testZscore() {
        client.zadd("zset", ZMember("a", 1.5))

        assertEquals(1.5, client.zscore("zset", "a")!!, 0.001)
        assertNull(client.zscore("zset", "nonexistent"))
    }

    @Test
    @DisplayName("ZMSCORE returns multiple scores")
    fun testZmscore() {
        client.zadd("zset",
            ZMember("a", 1.0),
            ZMember("b", 2.0),
            ZMember("c", 3.0)
        )

        val scores = client.zmscore("zset", "a", "nonexistent", "c")
        assertEquals(3, scores.size)
        assertEquals(1.0, scores[0]!!, 0.001)
        assertNull(scores[1])
        assertEquals(3.0, scores[2]!!, 0.001)
    }

    @Test
    @DisplayName("ZCARD returns cardinality")
    fun testZcard() {
        assertEquals(0, client.zcard("nonexistent"))

        client.zadd("zset", ZMember("a", 1.0), ZMember("b", 2.0))
        assertEquals(2, client.zcard("zset"))
    }

    @Test
    @DisplayName("ZRANK returns rank by score ascending")
    fun testZrank() {
        client.zadd("zset",
            ZMember("a", 1.0),
            ZMember("b", 2.0),
            ZMember("c", 3.0)
        )

        assertEquals(0, client.zrank("zset", "a"))
        assertEquals(1, client.zrank("zset", "b"))
        assertEquals(2, client.zrank("zset", "c"))
        assertNull(client.zrank("zset", "nonexistent"))
    }

    @Test
    @DisplayName("ZREVRANK returns rank by score descending")
    fun testZrevrank() {
        client.zadd("zset",
            ZMember("a", 1.0),
            ZMember("b", 2.0),
            ZMember("c", 3.0)
        )

        assertEquals(2, client.zrevrank("zset", "a"))
        assertEquals(1, client.zrevrank("zset", "b"))
        assertEquals(0, client.zrevrank("zset", "c"))
    }

    @Test
    @DisplayName("ZINCRBY increments score")
    fun testZincrBy() {
        client.zadd("zset", ZMember("a", 10.0))

        val newScore = client.zincrby("zset", 5.0, "a")
        assertEquals(15.0, newScore, 0.001)

        val decrementScore = client.zincrby("zset", -3.0, "a")
        assertEquals(12.0, decrementScore, 0.001)
    }

    @Test
    @DisplayName("ZINCRBY creates member if not exists")
    fun testZincrByNewMember() {
        val score = client.zincrby("zset", 5.0, "newmember")
        assertEquals(5.0, score, 0.001)
    }

    @Test
    @DisplayName("ZREM removes members")
    fun testZrem() {
        client.zadd("zset",
            ZMember("a", 1.0),
            ZMember("b", 2.0),
            ZMember("c", 3.0)
        )

        assertEquals(2, client.zrem("zset", "a", "c", "nonexistent"))
        assertEquals(1, client.zcard("zset"))
        assertNotNull(client.zscore("zset", "b"))
    }

    @Test
    @DisplayName("ZRANGE returns members by rank")
    fun testZrange() {
        client.zadd("zset",
            ZMember("a", 1.0),
            ZMember("b", 2.0),
            ZMember("c", 3.0),
            ZMember("d", 4.0)
        )

        val range = client.zrange("zset", 0, 2)
        assertEquals(listOf("a", "b", "c"), range)

        val lastTwo = client.zrange("zset", -2, -1)
        assertEquals(listOf("c", "d"), lastTwo)
    }

    @Test
    @DisplayName("ZRANGE with WITHSCORES")
    fun testZrangeWithScores() {
        client.zadd("zset",
            ZMember("a", 1.0),
            ZMember("b", 2.0)
        )

        val range = client.zrangeWithScores("zset", 0, -1)
        assertEquals(2, range.size)
        assertEquals("a", range[0].member)
        assertEquals(1.0, range[0].score, 0.001)
        assertEquals("b", range[1].member)
        assertEquals(2.0, range[1].score, 0.001)
    }

    @Test
    @DisplayName("ZREVRANGE returns members in reverse order")
    fun testZrevrange() {
        client.zadd("zset",
            ZMember("a", 1.0),
            ZMember("b", 2.0),
            ZMember("c", 3.0)
        )

        val range = client.zrevrange("zset", 0, 1)
        assertEquals(listOf("c", "b"), range)
    }

    @Test
    @DisplayName("ZRANGEBYSCORE returns members by score range")
    fun testZrangeByScore() {
        client.zadd("zset",
            ZMember("a", 1.0),
            ZMember("b", 2.0),
            ZMember("c", 3.0),
            ZMember("d", 4.0)
        )

        val range = client.zrangeByScore("zset", 2.0, 3.0)
        assertEquals(listOf("b", "c"), range)

        // Exclusive range
        val exclusive = client.zrangeByScore("zset", "(1", "4")
        assertEquals(listOf("b", "c", "d"), exclusive)
    }

    @Test
    @DisplayName("ZCOUNT counts members in score range")
    fun testZcount() {
        client.zadd("zset",
            ZMember("a", 1.0),
            ZMember("b", 2.0),
            ZMember("c", 3.0),
            ZMember("d", 4.0)
        )

        assertEquals(2, client.zcount("zset", 2.0, 3.0))
        assertEquals(4, client.zcount("zset", "-inf", "+inf"))
    }

    @Test
    @DisplayName("ZLEXCOUNT counts members in lex range")
    fun testZlexCount() {
        // All same score for lex ordering
        client.zadd("zset",
            ZMember("a", 0.0),
            ZMember("b", 0.0),
            ZMember("c", 0.0),
            ZMember("d", 0.0)
        )

        assertEquals(2, client.zlexcount("zset", "[b", "[c"))
        assertEquals(4, client.zlexcount("zset", "-", "+"))
    }

    @Test
    @DisplayName("ZPOPMIN removes member with lowest score")
    fun testZpopMin() {
        client.zadd("zset",
            ZMember("a", 1.0),
            ZMember("b", 2.0),
            ZMember("c", 3.0)
        )

        val popped = client.zpopmin("zset")
        assertNotNull(popped)
        assertEquals("a", popped!!.member)
        assertEquals(1.0, popped.score, 0.001)

        assertEquals(2, client.zcard("zset"))
    }

    @Test
    @DisplayName("ZPOPMAX removes member with highest score")
    fun testZpopMax() {
        client.zadd("zset",
            ZMember("a", 1.0),
            ZMember("b", 2.0),
            ZMember("c", 3.0)
        )

        val popped = client.zpopmax("zset")
        assertNotNull(popped)
        assertEquals("c", popped!!.member)
        assertEquals(3.0, popped.score, 0.001)
    }

    @Test
    @DisplayName("ZPOPMIN/MAX with count")
    fun testZpopMinMaxCount() {
        client.zadd("zset",
            ZMember("a", 1.0),
            ZMember("b", 2.0),
            ZMember("c", 3.0),
            ZMember("d", 4.0)
        )

        val minTwo = client.zpopmin("zset", 2)
        assertEquals(2, minTwo.size)
        assertEquals("a", minTwo[0].member)
        assertEquals("b", minTwo[1].member)

        val maxOne = client.zpopmax("zset", 1)
        assertEquals(1, maxOne.size)
        assertEquals("d", maxOne[0].member)
    }

    @Test
    @DisplayName("ZREMRANGEBYRANK removes by rank range")
    fun testZremRangeByRank() {
        client.zadd("zset",
            ZMember("a", 1.0),
            ZMember("b", 2.0),
            ZMember("c", 3.0),
            ZMember("d", 4.0)
        )

        assertEquals(2, client.zremrangebyrank("zset", 0, 1))
        assertEquals(2, client.zcard("zset"))
    }

    @Test
    @DisplayName("ZREMRANGEBYSCORE removes by score range")
    fun testZremRangeByScore() {
        client.zadd("zset",
            ZMember("a", 1.0),
            ZMember("b", 2.0),
            ZMember("c", 3.0),
            ZMember("d", 4.0)
        )

        assertEquals(2, client.zremrangebyscore("zset", 2.0, 3.0))
        assertEquals(2, client.zcard("zset"))
    }

    @Test
    @DisplayName("ZUNION returns union of sorted sets")
    fun testZunion() {
        client.zadd("zset1", ZMember("a", 1.0), ZMember("b", 2.0))
        client.zadd("zset2", ZMember("b", 3.0), ZMember("c", 4.0))

        val union = client.zunion("zset1", "zset2")
        assertEquals(3, union.size)
        assertTrue(union.contains("a"))
        assertTrue(union.contains("b"))
        assertTrue(union.contains("c"))
    }

    @Test
    @DisplayName("ZUNIONSTORE stores union result")
    fun testZunionStore() {
        client.zadd("zset1", ZMember("a", 1.0), ZMember("b", 2.0))
        client.zadd("zset2", ZMember("b", 3.0), ZMember("c", 4.0))

        assertEquals(3, client.zunionstore("dest", "zset1", "zset2"))
        // b should have combined score (default: SUM)
        assertEquals(5.0, client.zscore("dest", "b")!!, 0.001)
    }

    @Test
    @DisplayName("ZINTER returns intersection")
    fun testZinter() {
        client.zadd("zset1", ZMember("a", 1.0), ZMember("b", 2.0))
        client.zadd("zset2", ZMember("b", 3.0), ZMember("c", 4.0))

        val inter = client.zinter("zset1", "zset2")
        assertEquals(1, inter.size)
        assertEquals("b", inter[0])
    }

    @Test
    @DisplayName("ZINTERSTORE stores intersection result")
    fun testZinterStore() {
        client.zadd("zset1", ZMember("a", 1.0), ZMember("b", 2.0))
        client.zadd("zset2", ZMember("b", 3.0), ZMember("c", 4.0))

        assertEquals(1, client.zinterstore("dest", "zset1", "zset2"))
        assertEquals(5.0, client.zscore("dest", "b")!!, 0.001)
    }

    @Test
    @DisplayName("ZSCAN iterates through members")
    fun testZscan() {
        for (i in 1..10) {
            client.zadd("zset", ZMember("member$i", i.toDouble()))
        }

        val allMembers = mutableListOf<ZMember>()
        var cursor = "0"

        do {
            val result = client.zscan("zset", cursor)
            cursor = result.cursor
            allMembers.addAll(result.members)
        } while (cursor != "0")

        assertEquals(10, allMembers.size)
    }

    @Test
    @DisplayName("ZRANDMEMBER returns random members")
    fun testZrandmember() {
        client.zadd("zset",
            ZMember("a", 1.0),
            ZMember("b", 2.0),
            ZMember("c", 3.0)
        )

        val member = client.zrandmember("zset")
        assertNotNull(member)
        assertTrue(member in listOf("a", "b", "c"))

        val members = client.zrandmember("zset", 2)
        assertEquals(2, members.size)
    }
}
