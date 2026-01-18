package com.redlite;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;

import java.nio.charset.StandardCharsets;
import java.util.HashSet;
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
        client = new Redlite(":memory:");
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
    @DisplayName("SMEMBERS on non-existent set returns empty")
    void testSmembersEmpty() {
        Set<byte[]> members = client.smembers("nonexistent");
        assertTrue(members.isEmpty());
    }

    @Test
    @DisplayName("SISMEMBER on non-existent set returns false")
    void testSismemberNonExistent() {
        assertFalse(client.sismember("nonexistent", "a".getBytes(StandardCharsets.UTF_8)));
    }

    @Test
    @DisplayName("SREM on non-existent set returns 0")
    void testSremNonExistent() {
        assertEquals(0, client.srem("nonexistent", "a".getBytes(StandardCharsets.UTF_8)));
    }

    @Test
    @DisplayName("SADD and SREM with multiple members")
    void testSaddSremMultiple() {
        // Add multiple at once
        assertEquals(5, client.sadd("set",
            "a".getBytes(StandardCharsets.UTF_8),
            "b".getBytes(StandardCharsets.UTF_8),
            "c".getBytes(StandardCharsets.UTF_8),
            "d".getBytes(StandardCharsets.UTF_8),
            "e".getBytes(StandardCharsets.UTF_8)
        ));

        assertEquals(5, client.scard("set"));

        // Remove multiple at once
        assertEquals(3, client.srem("set",
            "a".getBytes(StandardCharsets.UTF_8),
            "c".getBytes(StandardCharsets.UTF_8),
            "e".getBytes(StandardCharsets.UTF_8)
        ));

        assertEquals(2, client.scard("set"));
        assertTrue(client.sismember("set", "b".getBytes(StandardCharsets.UTF_8)));
        assertTrue(client.sismember("set", "d".getBytes(StandardCharsets.UTF_8)));
    }
}
