package com.redlite;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;

import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for set commands.
 */
@DisplayName("Set Commands")
class SetsTest {
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
    @DisplayName("SADD adds members to set")
    void testSadd() {
        assertEquals(3, client.sadd("set",
            "a".getBytes(StandardCharsets.UTF_8),
            "b".getBytes(StandardCharsets.UTF_8),
            "c".getBytes(StandardCharsets.UTF_8)
        ));

        // Adding existing members returns 0
        assertEquals(0, client.sadd("set", "a".getBytes(StandardCharsets.UTF_8)));

        // Mixed new and existing
        assertEquals(1, client.sadd("set",
            "a".getBytes(StandardCharsets.UTF_8),
            "d".getBytes(StandardCharsets.UTF_8)
        ));
    }

    @Test
    @DisplayName("SMEMBERS returns all members")
    void testSmembers() {
        client.sadd("set",
            "a".getBytes(StandardCharsets.UTF_8),
            "b".getBytes(StandardCharsets.UTF_8),
            "c".getBytes(StandardCharsets.UTF_8)
        );

        Set<byte[]> members = client.smembers("set");
        assertEquals(3, members.size());
        Set<String> stringMembers = new HashSet<>();
        members.forEach(m -> stringMembers.add(new String(m, StandardCharsets.UTF_8)));
        assertTrue(stringMembers.containsAll(Set.of("a", "b", "c")));
    }

    @Test
    @DisplayName("SISMEMBER checks membership")
    void testSismember() {
        client.sadd("set",
            "a".getBytes(StandardCharsets.UTF_8),
            "b".getBytes(StandardCharsets.UTF_8)
        );

        assertTrue(client.sismember("set", "a".getBytes(StandardCharsets.UTF_8)));
        assertFalse(client.sismember("set", "z".getBytes(StandardCharsets.UTF_8)));
    }

    @Test
    @DisplayName("SMISMEMBER checks multiple memberships")
    void testSmismember() {
        client.sadd("set",
            "a".getBytes(StandardCharsets.UTF_8),
            "b".getBytes(StandardCharsets.UTF_8),
            "c".getBytes(StandardCharsets.UTF_8)
        );

        List<Boolean> results = client.smismember("set",
            "a".getBytes(StandardCharsets.UTF_8),
            "z".getBytes(StandardCharsets.UTF_8),
            "c".getBytes(StandardCharsets.UTF_8)
        );
        assertEquals(List.of(true, false, true), results);
    }

    @Test
    @DisplayName("SCARD returns cardinality")
    void testScard() {
        assertEquals(0, client.scard("nonexistent"));

        client.sadd("set",
            "a".getBytes(StandardCharsets.UTF_8),
            "b".getBytes(StandardCharsets.UTF_8),
            "c".getBytes(StandardCharsets.UTF_8)
        );
        assertEquals(3, client.scard("set"));
    }

    @Test
    @DisplayName("SREM removes members")
    void testSrem() {
        client.sadd("set",
            "a".getBytes(StandardCharsets.UTF_8),
            "b".getBytes(StandardCharsets.UTF_8),
            "c".getBytes(StandardCharsets.UTF_8)
        );

        assertEquals(2, client.srem("set",
            "a".getBytes(StandardCharsets.UTF_8),
            "c".getBytes(StandardCharsets.UTF_8),
            "z".getBytes(StandardCharsets.UTF_8)
        ));
        assertEquals(1, client.scard("set"));
        assertTrue(client.sismember("set", "b".getBytes(StandardCharsets.UTF_8)));
    }

    @Test
    @DisplayName("SPOP removes and returns random member")
    void testSpop() {
        client.sadd("set",
            "a".getBytes(StandardCharsets.UTF_8),
            "b".getBytes(StandardCharsets.UTF_8),
            "c".getBytes(StandardCharsets.UTF_8)
        );

        byte[] popped = client.spop("set");
        assertNotNull(popped);
        assertEquals(2, client.scard("set"));
    }

    @Test
    @DisplayName("SPOP with count removes multiple")
    void testSpopCount() {
        client.sadd("set",
            "a".getBytes(StandardCharsets.UTF_8),
            "b".getBytes(StandardCharsets.UTF_8),
            "c".getBytes(StandardCharsets.UTF_8),
            "d".getBytes(StandardCharsets.UTF_8),
            "e".getBytes(StandardCharsets.UTF_8)
        );

        Set<byte[]> popped = client.spop("set", 3);
        assertEquals(3, popped.size());
        assertEquals(2, client.scard("set"));
    }

    @Test
    @DisplayName("SRANDMEMBER returns random member without removal")
    void testSrandmember() {
        client.sadd("set",
            "a".getBytes(StandardCharsets.UTF_8),
            "b".getBytes(StandardCharsets.UTF_8),
            "c".getBytes(StandardCharsets.UTF_8)
        );

        byte[] member = client.srandmember("set");
        assertNotNull(member);
        assertEquals(3, client.scard("set")); // Not removed
    }

    @Test
    @DisplayName("SRANDMEMBER with count")
    void testSrandmemberCount() {
        client.sadd("set",
            "a".getBytes(StandardCharsets.UTF_8),
            "b".getBytes(StandardCharsets.UTF_8),
            "c".getBytes(StandardCharsets.UTF_8)
        );

        List<byte[]> members = client.srandmember("set", 2);
        assertEquals(2, members.size());
        assertEquals(3, client.scard("set")); // Not removed
    }

    @Test
    @DisplayName("SMOVE moves member between sets")
    void testSmove() {
        client.sadd("src",
            "a".getBytes(StandardCharsets.UTF_8),
            "b".getBytes(StandardCharsets.UTF_8)
        );
        client.sadd("dst", "x".getBytes(StandardCharsets.UTF_8));

        assertTrue(client.smove("src", "dst", "a".getBytes(StandardCharsets.UTF_8)));

        assertFalse(client.sismember("src", "a".getBytes(StandardCharsets.UTF_8)));
        assertTrue(client.sismember("dst", "a".getBytes(StandardCharsets.UTF_8)));
    }

    @Test
    @DisplayName("SUNION returns union of sets")
    void testSunion() {
        client.sadd("set1", "a".getBytes(StandardCharsets.UTF_8), "b".getBytes(StandardCharsets.UTF_8));
        client.sadd("set2", "b".getBytes(StandardCharsets.UTF_8), "c".getBytes(StandardCharsets.UTF_8));
        client.sadd("set3", "c".getBytes(StandardCharsets.UTF_8), "d".getBytes(StandardCharsets.UTF_8));

        Set<byte[]> union = client.sunion("set1", "set2", "set3");
        assertEquals(4, union.size());
        Set<String> stringMembers = new HashSet<>();
        union.forEach(m -> stringMembers.add(new String(m, StandardCharsets.UTF_8)));
        assertTrue(stringMembers.containsAll(Set.of("a", "b", "c", "d")));
    }

    @Test
    @DisplayName("SUNIONSTORE stores union result")
    void testSunionStore() {
        client.sadd("set1", "a".getBytes(StandardCharsets.UTF_8), "b".getBytes(StandardCharsets.UTF_8));
        client.sadd("set2", "c".getBytes(StandardCharsets.UTF_8), "d".getBytes(StandardCharsets.UTF_8));

        assertEquals(4, client.sunionstore("dest", "set1", "set2"));
        assertEquals(4, client.scard("dest"));
    }

    @Test
    @DisplayName("SINTER returns intersection of sets")
    void testSinter() {
        client.sadd("set1",
            "a".getBytes(StandardCharsets.UTF_8),
            "b".getBytes(StandardCharsets.UTF_8),
            "c".getBytes(StandardCharsets.UTF_8)
        );
        client.sadd("set2",
            "b".getBytes(StandardCharsets.UTF_8),
            "c".getBytes(StandardCharsets.UTF_8),
            "d".getBytes(StandardCharsets.UTF_8)
        );
        client.sadd("set3",
            "c".getBytes(StandardCharsets.UTF_8),
            "d".getBytes(StandardCharsets.UTF_8),
            "e".getBytes(StandardCharsets.UTF_8)
        );

        Set<byte[]> inter = client.sinter("set1", "set2", "set3");
        assertEquals(1, inter.size());
        assertEquals("c", new String(inter.iterator().next(), StandardCharsets.UTF_8));
    }

    @Test
    @DisplayName("SINTERSTORE stores intersection result")
    void testSinterStore() {
        client.sadd("set1", "a".getBytes(StandardCharsets.UTF_8), "b".getBytes(StandardCharsets.UTF_8));
        client.sadd("set2", "b".getBytes(StandardCharsets.UTF_8), "c".getBytes(StandardCharsets.UTF_8));

        assertEquals(1, client.sinterstore("dest", "set1", "set2"));
        assertTrue(client.sismember("dest", "b".getBytes(StandardCharsets.UTF_8)));
    }

    @Test
    @DisplayName("SINTERCARD returns intersection cardinality")
    void testSinterCard() {
        client.sadd("set1",
            "a".getBytes(StandardCharsets.UTF_8),
            "b".getBytes(StandardCharsets.UTF_8),
            "c".getBytes(StandardCharsets.UTF_8)
        );
        client.sadd("set2",
            "b".getBytes(StandardCharsets.UTF_8),
            "c".getBytes(StandardCharsets.UTF_8),
            "d".getBytes(StandardCharsets.UTF_8)
        );

        assertEquals(2, client.sintercard("set1", "set2"));
    }

    @Test
    @DisplayName("SINTERCARD with limit")
    void testSinterCardLimit() {
        client.sadd("set1",
            "a".getBytes(StandardCharsets.UTF_8),
            "b".getBytes(StandardCharsets.UTF_8),
            "c".getBytes(StandardCharsets.UTF_8)
        );
        client.sadd("set2",
            "a".getBytes(StandardCharsets.UTF_8),
            "b".getBytes(StandardCharsets.UTF_8),
            "c".getBytes(StandardCharsets.UTF_8)
        );

        assertEquals(2, client.sintercard(List.of("set1", "set2"), 2));
    }

    @Test
    @DisplayName("SDIFF returns difference of sets")
    void testSdiff() {
        client.sadd("set1",
            "a".getBytes(StandardCharsets.UTF_8),
            "b".getBytes(StandardCharsets.UTF_8),
            "c".getBytes(StandardCharsets.UTF_8)
        );
        client.sadd("set2",
            "b".getBytes(StandardCharsets.UTF_8),
            "d".getBytes(StandardCharsets.UTF_8)
        );

        Set<byte[]> diff = client.sdiff("set1", "set2");
        assertEquals(2, diff.size());
        Set<String> stringMembers = new HashSet<>();
        diff.forEach(m -> stringMembers.add(new String(m, StandardCharsets.UTF_8)));
        assertTrue(stringMembers.containsAll(Set.of("a", "c")));
    }

    @Test
    @DisplayName("SDIFFSTORE stores difference result")
    void testSdiffStore() {
        client.sadd("set1",
            "a".getBytes(StandardCharsets.UTF_8),
            "b".getBytes(StandardCharsets.UTF_8),
            "c".getBytes(StandardCharsets.UTF_8)
        );
        client.sadd("set2", "b".getBytes(StandardCharsets.UTF_8));

        assertEquals(2, client.sdiffstore("dest", "set1", "set2"));
    }

    @Test
    @DisplayName("SSCAN iterates through members")
    void testSscan() {
        for (int i = 1; i <= 10; i++) {
            client.sadd("set", ("member" + i).getBytes(StandardCharsets.UTF_8));
        }

        Set<String> allMembers = new HashSet<>();
        String cursor = "0";

        do {
            ScanResult<byte[]> result = client.sscan("set", cursor);
            cursor = result.getCursor();
            result.getResult().forEach(m -> allMembers.add(new String(m, StandardCharsets.UTF_8)));
        } while (!"0".equals(cursor));

        assertEquals(10, allMembers.size());
    }

    @Test
    @DisplayName("SSCAN with MATCH pattern")
    void testSscanWithMatch() {
        for (int i = 1; i <= 5; i++) {
            client.sadd("set", ("user:" + i).getBytes(StandardCharsets.UTF_8));
            client.sadd("set", ("order:" + i).getBytes(StandardCharsets.UTF_8));
        }

        Set<String> userMembers = new HashSet<>();
        String cursor = "0";

        do {
            ScanResult<byte[]> result = client.sscan("set", cursor, new ScanOptions().match("user:*"));
            cursor = result.getCursor();
            result.getResult().forEach(m -> userMembers.add(new String(m, StandardCharsets.UTF_8)));
        } while (!"0".equals(cursor));

        assertEquals(5, userMembers.size());
        userMembers.forEach(m -> assertTrue(m.startsWith("user:")));
    }
}
