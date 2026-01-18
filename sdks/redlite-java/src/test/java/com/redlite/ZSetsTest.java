package com.redlite;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;

import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for sorted set commands.
 */
@DisplayName("Sorted Set Commands")
class ZSetsTest {
    private Redlite client;

    @BeforeEach
    void setUp() {
        client = new Redlite(":memory:");
    }

    @AfterEach
    void tearDown() {
        client.close();
    }

    @Test
    @DisplayName("ZADD adds members with scores")
    void testZadd() {
        long count = client.zadd("zset",
            ZMember.of(1.0, "a"),
            ZMember.of(2.0, "b"),
            ZMember.of(3.0, "c")
        );
        assertEquals(3, count);
    }

    @Test
    @DisplayName("ZADD updates existing member score")
    void testZaddUpdate() {
        client.zadd("zset", ZMember.of(1.0, "a"));

        // Update score - returns 0 (not new member)
        assertEquals(0, client.zadd("zset", ZMember.of(5.0, "a")));

        Double score = client.zscore("zset", "a".getBytes(StandardCharsets.UTF_8));
        assertEquals(5.0, score, 0.001);
    }

    @Test
    @DisplayName("ZSCORE returns member score")
    void testZscore() {
        client.zadd("zset", ZMember.of(1.5, "a"));

        assertEquals(1.5, client.zscore("zset", "a".getBytes(StandardCharsets.UTF_8)), 0.001);
        assertNull(client.zscore("zset", "nonexistent".getBytes(StandardCharsets.UTF_8)));
    }

    @Test
    @DisplayName("ZCARD returns cardinality")
    void testZcard() {
        assertEquals(0, client.zcard("nonexistent"));

        client.zadd("zset", ZMember.of(1.0, "a"), ZMember.of(2.0, "b"));
        assertEquals(2, client.zcard("zset"));
    }

    @Test
    @DisplayName("ZINCRBY increments score")
    void testZincrBy() {
        client.zadd("zset", ZMember.of(10.0, "a"));

        double newScore = client.zincrby("zset", 5.0, "a".getBytes(StandardCharsets.UTF_8));
        assertEquals(15.0, newScore, 0.001);

        double decrementScore = client.zincrby("zset", -3.0, "a".getBytes(StandardCharsets.UTF_8));
        assertEquals(12.0, decrementScore, 0.001);
    }

    @Test
    @DisplayName("ZINCRBY creates member if not exists")
    void testZincrByNewMember() {
        double score = client.zincrby("zset", 5.0, "newmember".getBytes(StandardCharsets.UTF_8));
        assertEquals(5.0, score, 0.001);
    }

    @Test
    @DisplayName("ZREM removes members")
    void testZrem() {
        client.zadd("zset",
            ZMember.of(1.0, "a"),
            ZMember.of(2.0, "b"),
            ZMember.of(3.0, "c")
        );

        assertEquals(2, client.zrem("zset",
            "a".getBytes(StandardCharsets.UTF_8),
            "c".getBytes(StandardCharsets.UTF_8),
            "nonexistent".getBytes(StandardCharsets.UTF_8)
        ));
        assertEquals(1, client.zcard("zset"));
        assertNotNull(client.zscore("zset", "b".getBytes(StandardCharsets.UTF_8)));
    }

    @Test
    @DisplayName("ZRANGE returns members by rank")
    void testZrange() {
        client.zadd("zset",
            ZMember.of(1.0, "a"),
            ZMember.of(2.0, "b"),
            ZMember.of(3.0, "c"),
            ZMember.of(4.0, "d")
        );

        List<byte[]> range = client.zrange("zset", 0, 2);
        assertEquals(3, range.size());
        assertEquals("a", new String(range.get(0), StandardCharsets.UTF_8));
        assertEquals("b", new String(range.get(1), StandardCharsets.UTF_8));
        assertEquals("c", new String(range.get(2), StandardCharsets.UTF_8));
    }

    @Test
    @DisplayName("ZRANGE with negative indices")
    void testZrangeNegative() {
        client.zadd("zset",
            ZMember.of(1.0, "a"),
            ZMember.of(2.0, "b"),
            ZMember.of(3.0, "c"),
            ZMember.of(4.0, "d")
        );

        List<byte[]> lastTwo = client.zrange("zset", -2, -1);
        assertEquals(2, lastTwo.size());
        assertEquals("c", new String(lastTwo.get(0), StandardCharsets.UTF_8));
        assertEquals("d", new String(lastTwo.get(1), StandardCharsets.UTF_8));
    }

    @Test
    @DisplayName("ZRANGE with WITHSCORES")
    void testZrangeWithScores() {
        client.zadd("zset",
            ZMember.of(1.0, "a"),
            ZMember.of(2.0, "b")
        );

        List<ZMember> range = client.zrangeWithScores("zset", 0, -1);
        assertEquals(2, range.size());
        assertEquals("a", range.get(0).memberAsString());
        assertEquals(1.0, range.get(0).score(), 0.001);
        assertEquals("b", range.get(1).memberAsString());
        assertEquals(2.0, range.get(1).score(), 0.001);
    }

    @Test
    @DisplayName("ZREVRANGE returns members in reverse order")
    void testZrevrange() {
        client.zadd("zset",
            ZMember.of(1.0, "a"),
            ZMember.of(2.0, "b"),
            ZMember.of(3.0, "c")
        );

        List<byte[]> range = client.zrevrange("zset", 0, 1);
        assertEquals(2, range.size());
        assertEquals("c", new String(range.get(0), StandardCharsets.UTF_8));
        assertEquals("b", new String(range.get(1), StandardCharsets.UTF_8));
    }

    @Test
    @DisplayName("ZREVRANGE with WITHSCORES")
    void testZrevrangeWithScores() {
        client.zadd("zset",
            ZMember.of(1.0, "a"),
            ZMember.of(2.0, "b"),
            ZMember.of(3.0, "c")
        );

        List<ZMember> range = client.zrevrangeWithScores("zset", 0, 1);
        assertEquals(2, range.size());
        assertEquals("c", range.get(0).memberAsString());
        assertEquals(3.0, range.get(0).score(), 0.001);
        assertEquals("b", range.get(1).memberAsString());
        assertEquals(2.0, range.get(1).score(), 0.001);
    }

    @Test
    @DisplayName("ZCOUNT counts members in score range")
    void testZcount() {
        client.zadd("zset",
            ZMember.of(1.0, "a"),
            ZMember.of(2.0, "b"),
            ZMember.of(3.0, "c"),
            ZMember.of(4.0, "d")
        );

        assertEquals(2, client.zcount("zset", 2.0, 3.0));
        assertEquals(4, client.zcount("zset", Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY));
    }

    @Test
    @DisplayName("ZCARD on non-existent key returns 0")
    void testZcardNonExistent() {
        assertEquals(0, client.zcard("nonexistent"));
    }

    @Test
    @DisplayName("ZRANGE on non-existent key returns empty")
    void testZrangeNonExistent() {
        List<byte[]> range = client.zrange("nonexistent", 0, -1);
        assertTrue(range.isEmpty());
    }

    @Test
    @DisplayName("ZADD with map of score-member pairs")
    void testZaddMap() {
        java.util.Map<byte[], Double> members = new java.util.LinkedHashMap<>();
        members.put("a".getBytes(StandardCharsets.UTF_8), 1.0);
        members.put("b".getBytes(StandardCharsets.UTF_8), 2.0);
        members.put("c".getBytes(StandardCharsets.UTF_8), 3.0);

        assertEquals(3, client.zadd("zset", members));
        assertEquals(3, client.zcard("zset"));
    }

    @Test
    @DisplayName("ZMember record works correctly")
    void testZMemberRecord() {
        ZMember m1 = ZMember.of(1.5, "test");
        assertEquals(1.5, m1.score(), 0.001);
        assertEquals("test", m1.memberAsString());

        ZMember m2 = new ZMember(2.5, "hello".getBytes(StandardCharsets.UTF_8));
        assertEquals(2.5, m2.score(), 0.001);
        assertEquals("hello", m2.memberAsString());
    }
}
