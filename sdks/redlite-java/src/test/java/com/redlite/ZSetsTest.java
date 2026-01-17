package com.redlite;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;

import java.util.ArrayList;
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
        client = Redlite.open(":memory:");
    }

    @AfterEach
    void tearDown() {
        client.close();
    }

    @Test
    @DisplayName("ZADD adds members with scores")
    void testZadd() {
        long count = client.zadd("zset",
            new ZMember("a", 1.0),
            new ZMember("b", 2.0),
            new ZMember("c", 3.0)
        );
        assertEquals(3, count);
    }

    @Test
    @DisplayName("ZADD updates existing member score")
    void testZaddUpdate() {
        client.zadd("zset", new ZMember("a", 1.0));

        // Update score - returns 0 (not new member)
        assertEquals(0, client.zadd("zset", new ZMember("a", 5.0)));

        Double score = client.zscore("zset", "a");
        assertEquals(5.0, score, 0.001);
    }

    @Test
    @DisplayName("ZADD with NX option")
    void testZaddNx() {
        client.zadd("zset", new ZMember("a", 1.0));

        // NX - only add if not exists
        assertEquals(0, client.zaddNx("zset", new ZMember("a", 5.0)));
        assertEquals(1.0, client.zscore("zset", "a"), 0.001);

        assertEquals(1, client.zaddNx("zset", new ZMember("b", 2.0)));
    }

    @Test
    @DisplayName("ZADD with XX option")
    void testZaddXx() {
        client.zadd("zset", new ZMember("a", 1.0));

        // XX - only update if exists
        assertEquals(0, client.zaddXx("zset", new ZMember("b", 2.0)));
        assertNull(client.zscore("zset", "b"));

        assertEquals(0, client.zaddXx("zset", new ZMember("a", 5.0)));
        assertEquals(5.0, client.zscore("zset", "a"), 0.001);
    }

    @Test
    @DisplayName("ZSCORE returns member score")
    void testZscore() {
        client.zadd("zset", new ZMember("a", 1.5));

        assertEquals(1.5, client.zscore("zset", "a"), 0.001);
        assertNull(client.zscore("zset", "nonexistent"));
    }

    @Test
    @DisplayName("ZMSCORE returns multiple scores")
    void testZmscore() {
        client.zadd("zset",
            new ZMember("a", 1.0),
            new ZMember("b", 2.0),
            new ZMember("c", 3.0)
        );

        List<Double> scores = client.zmscore("zset", "a", "nonexistent", "c");
        assertEquals(3, scores.size());
        assertEquals(1.0, scores.get(0), 0.001);
        assertNull(scores.get(1));
        assertEquals(3.0, scores.get(2), 0.001);
    }

    @Test
    @DisplayName("ZCARD returns cardinality")
    void testZcard() {
        assertEquals(0, client.zcard("nonexistent"));

        client.zadd("zset", new ZMember("a", 1.0), new ZMember("b", 2.0));
        assertEquals(2, client.zcard("zset"));
    }

    @Test
    @DisplayName("ZRANK returns rank by score ascending")
    void testZrank() {
        client.zadd("zset",
            new ZMember("a", 1.0),
            new ZMember("b", 2.0),
            new ZMember("c", 3.0)
        );

        assertEquals(0, client.zrank("zset", "a"));
        assertEquals(1, client.zrank("zset", "b"));
        assertEquals(2, client.zrank("zset", "c"));
        assertNull(client.zrank("zset", "nonexistent"));
    }

    @Test
    @DisplayName("ZREVRANK returns rank by score descending")
    void testZrevrank() {
        client.zadd("zset",
            new ZMember("a", 1.0),
            new ZMember("b", 2.0),
            new ZMember("c", 3.0)
        );

        assertEquals(2, client.zrevrank("zset", "a"));
        assertEquals(1, client.zrevrank("zset", "b"));
        assertEquals(0, client.zrevrank("zset", "c"));
    }

    @Test
    @DisplayName("ZINCRBY increments score")
    void testZincrBy() {
        client.zadd("zset", new ZMember("a", 10.0));

        double newScore = client.zincrby("zset", 5.0, "a");
        assertEquals(15.0, newScore, 0.001);

        double decrementScore = client.zincrby("zset", -3.0, "a");
        assertEquals(12.0, decrementScore, 0.001);
    }

    @Test
    @DisplayName("ZINCRBY creates member if not exists")
    void testZincrByNewMember() {
        double score = client.zincrby("zset", 5.0, "newmember");
        assertEquals(5.0, score, 0.001);
    }

    @Test
    @DisplayName("ZREM removes members")
    void testZrem() {
        client.zadd("zset",
            new ZMember("a", 1.0),
            new ZMember("b", 2.0),
            new ZMember("c", 3.0)
        );

        assertEquals(2, client.zrem("zset", "a", "c", "nonexistent"));
        assertEquals(1, client.zcard("zset"));
        assertNotNull(client.zscore("zset", "b"));
    }

    @Test
    @DisplayName("ZRANGE returns members by rank")
    void testZrange() {
        client.zadd("zset",
            new ZMember("a", 1.0),
            new ZMember("b", 2.0),
            new ZMember("c", 3.0),
            new ZMember("d", 4.0)
        );

        List<String> range = client.zrange("zset", 0, 2);
        assertEquals(List.of("a", "b", "c"), range);

        List<String> lastTwo = client.zrange("zset", -2, -1);
        assertEquals(List.of("c", "d"), lastTwo);
    }

    @Test
    @DisplayName("ZRANGE with WITHSCORES")
    void testZrangeWithScores() {
        client.zadd("zset",
            new ZMember("a", 1.0),
            new ZMember("b", 2.0)
        );

        List<ZMember> range = client.zrangeWithScores("zset", 0, -1);
        assertEquals(2, range.size());
        assertEquals("a", range.get(0).member());
        assertEquals(1.0, range.get(0).score(), 0.001);
        assertEquals("b", range.get(1).member());
        assertEquals(2.0, range.get(1).score(), 0.001);
    }

    @Test
    @DisplayName("ZREVRANGE returns members in reverse order")
    void testZrevrange() {
        client.zadd("zset",
            new ZMember("a", 1.0),
            new ZMember("b", 2.0),
            new ZMember("c", 3.0)
        );

        List<String> range = client.zrevrange("zset", 0, 1);
        assertEquals(List.of("c", "b"), range);
    }

    @Test
    @DisplayName("ZRANGEBYSCORE returns members by score range")
    void testZrangeByScore() {
        client.zadd("zset",
            new ZMember("a", 1.0),
            new ZMember("b", 2.0),
            new ZMember("c", 3.0),
            new ZMember("d", 4.0)
        );

        List<String> range = client.zrangeByScore("zset", 2.0, 3.0);
        assertEquals(List.of("b", "c"), range);

        // Exclusive range
        List<String> exclusive = client.zrangeByScore("zset", "(1", "4");
        assertEquals(List.of("b", "c", "d"), exclusive);
    }

    @Test
    @DisplayName("ZCOUNT counts members in score range")
    void testZcount() {
        client.zadd("zset",
            new ZMember("a", 1.0),
            new ZMember("b", 2.0),
            new ZMember("c", 3.0),
            new ZMember("d", 4.0)
        );

        assertEquals(2, client.zcount("zset", 2.0, 3.0));
        assertEquals(4, client.zcount("zset", "-inf", "+inf"));
    }

    @Test
    @DisplayName("ZLEXCOUNT counts members in lex range")
    void testZlexCount() {
        // All same score for lex ordering
        client.zadd("zset",
            new ZMember("a", 0.0),
            new ZMember("b", 0.0),
            new ZMember("c", 0.0),
            new ZMember("d", 0.0)
        );

        assertEquals(2, client.zlexcount("zset", "[b", "[c"));
        assertEquals(4, client.zlexcount("zset", "-", "+"));
    }

    @Test
    @DisplayName("ZPOPMIN removes member with lowest score")
    void testZpopMin() {
        client.zadd("zset",
            new ZMember("a", 1.0),
            new ZMember("b", 2.0),
            new ZMember("c", 3.0)
        );

        ZMember popped = client.zpopmin("zset");
        assertNotNull(popped);
        assertEquals("a", popped.member());
        assertEquals(1.0, popped.score(), 0.001);

        assertEquals(2, client.zcard("zset"));
    }

    @Test
    @DisplayName("ZPOPMAX removes member with highest score")
    void testZpopMax() {
        client.zadd("zset",
            new ZMember("a", 1.0),
            new ZMember("b", 2.0),
            new ZMember("c", 3.0)
        );

        ZMember popped = client.zpopmax("zset");
        assertNotNull(popped);
        assertEquals("c", popped.member());
        assertEquals(3.0, popped.score(), 0.001);
    }

    @Test
    @DisplayName("ZPOPMIN/MAX with count")
    void testZpopMinMaxCount() {
        client.zadd("zset",
            new ZMember("a", 1.0),
            new ZMember("b", 2.0),
            new ZMember("c", 3.0),
            new ZMember("d", 4.0)
        );

        List<ZMember> minTwo = client.zpopmin("zset", 2);
        assertEquals(2, minTwo.size());
        assertEquals("a", minTwo.get(0).member());
        assertEquals("b", minTwo.get(1).member());

        List<ZMember> maxOne = client.zpopmax("zset", 1);
        assertEquals(1, maxOne.size());
        assertEquals("d", maxOne.get(0).member());
    }

    @Test
    @DisplayName("ZREMRANGEBYRANK removes by rank range")
    void testZremRangeByRank() {
        client.zadd("zset",
            new ZMember("a", 1.0),
            new ZMember("b", 2.0),
            new ZMember("c", 3.0),
            new ZMember("d", 4.0)
        );

        assertEquals(2, client.zremrangebyrank("zset", 0, 1));
        assertEquals(2, client.zcard("zset"));
    }

    @Test
    @DisplayName("ZREMRANGEBYSCORE removes by score range")
    void testZremRangeByScore() {
        client.zadd("zset",
            new ZMember("a", 1.0),
            new ZMember("b", 2.0),
            new ZMember("c", 3.0),
            new ZMember("d", 4.0)
        );

        assertEquals(2, client.zremrangebyscore("zset", 2.0, 3.0));
        assertEquals(2, client.zcard("zset"));
    }

    @Test
    @DisplayName("ZUNION returns union of sorted sets")
    void testZunion() {
        client.zadd("zset1", new ZMember("a", 1.0), new ZMember("b", 2.0));
        client.zadd("zset2", new ZMember("b", 3.0), new ZMember("c", 4.0));

        List<String> union = client.zunion("zset1", "zset2");
        assertEquals(3, union.size());
        assertTrue(union.contains("a"));
        assertTrue(union.contains("b"));
        assertTrue(union.contains("c"));
    }

    @Test
    @DisplayName("ZUNIONSTORE stores union result")
    void testZunionStore() {
        client.zadd("zset1", new ZMember("a", 1.0), new ZMember("b", 2.0));
        client.zadd("zset2", new ZMember("b", 3.0), new ZMember("c", 4.0));

        assertEquals(3, client.zunionstore("dest", "zset1", "zset2"));
        // b should have combined score (default: SUM)
        assertEquals(5.0, client.zscore("dest", "b"), 0.001);
    }

    @Test
    @DisplayName("ZINTER returns intersection")
    void testZinter() {
        client.zadd("zset1", new ZMember("a", 1.0), new ZMember("b", 2.0));
        client.zadd("zset2", new ZMember("b", 3.0), new ZMember("c", 4.0));

        List<String> inter = client.zinter("zset1", "zset2");
        assertEquals(1, inter.size());
        assertEquals("b", inter.get(0));
    }

    @Test
    @DisplayName("ZINTERSTORE stores intersection result")
    void testZinterStore() {
        client.zadd("zset1", new ZMember("a", 1.0), new ZMember("b", 2.0));
        client.zadd("zset2", new ZMember("b", 3.0), new ZMember("c", 4.0));

        assertEquals(1, client.zinterstore("dest", "zset1", "zset2"));
        assertEquals(5.0, client.zscore("dest", "b"), 0.001);
    }

    @Test
    @DisplayName("ZSCAN iterates through members")
    void testZscan() {
        for (int i = 1; i <= 10; i++) {
            client.zadd("zset", new ZMember("member" + i, (double) i));
        }

        List<ZMember> allMembers = new ArrayList<>();
        String cursor = "0";

        do {
            ScanResult<ZMember> result = client.zscan("zset", cursor);
            cursor = result.getCursor();
            allMembers.addAll(result.getResult());
        } while (!"0".equals(cursor));

        assertEquals(10, allMembers.size());
    }

    @Test
    @DisplayName("ZRANDMEMBER returns random members")
    void testZrandmember() {
        client.zadd("zset",
            new ZMember("a", 1.0),
            new ZMember("b", 2.0),
            new ZMember("c", 3.0)
        );

        String member = client.zrandmember("zset");
        assertNotNull(member);
        assertTrue(List.of("a", "b", "c").contains(member));

        List<String> members = client.zrandmember("zset", 2);
        assertEquals(2, members.size());
    }
}
