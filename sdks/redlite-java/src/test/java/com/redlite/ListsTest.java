package com.redlite;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;

import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for list commands.
 */
@DisplayName("List Commands")
class ListsTest {
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
    @DisplayName("LPUSH and RPUSH add elements")
    void testLpushRpush() {
        assertEquals(1, client.lpush("list", "first".getBytes(StandardCharsets.UTF_8)));
        assertEquals(2, client.lpush("list", "second".getBytes(StandardCharsets.UTF_8)));

        // List is now: [second, first]
        assertEquals(3, client.rpush("list", "third".getBytes(StandardCharsets.UTF_8)));
        // List is now: [second, first, third]

        List<byte[]> elements = client.lrange("list", 0, -1);
        assertEquals(3, elements.size());
        assertEquals("second", new String(elements.get(0), StandardCharsets.UTF_8));
        assertEquals("first", new String(elements.get(1), StandardCharsets.UTF_8));
        assertEquals("third", new String(elements.get(2), StandardCharsets.UTF_8));
    }

    @Test
    @DisplayName("LPUSH with multiple values")
    void testLpushMultiple() {
        long count = client.lpush("list",
            "a".getBytes(StandardCharsets.UTF_8),
            "b".getBytes(StandardCharsets.UTF_8),
            "c".getBytes(StandardCharsets.UTF_8)
        );
        assertEquals(3, count);

        // Order: c, b, a (last pushed is at head)
        List<byte[]> elements = client.lrange("list", 0, -1);
        assertEquals("c", new String(elements.get(0), StandardCharsets.UTF_8));
        assertEquals("b", new String(elements.get(1), StandardCharsets.UTF_8));
        assertEquals("a", new String(elements.get(2), StandardCharsets.UTF_8));
    }

    @Test
    @DisplayName("LPOP and RPOP remove elements")
    void testLpopRpop() {
        client.rpush("list",
            "a".getBytes(StandardCharsets.UTF_8),
            "b".getBytes(StandardCharsets.UTF_8),
            "c".getBytes(StandardCharsets.UTF_8)
        );

        assertEquals("a", new String(client.lpop("list"), StandardCharsets.UTF_8));
        assertEquals("c", new String(client.rpop("list"), StandardCharsets.UTF_8));
        assertEquals("b", new String(client.lpop("list"), StandardCharsets.UTF_8));
        assertNull(client.lpop("list"));
    }

    @Test
    @DisplayName("LPOP and RPOP with count")
    void testLpopRpopCount() {
        client.rpush("list",
            "1".getBytes(StandardCharsets.UTF_8),
            "2".getBytes(StandardCharsets.UTF_8),
            "3".getBytes(StandardCharsets.UTF_8),
            "4".getBytes(StandardCharsets.UTF_8),
            "5".getBytes(StandardCharsets.UTF_8)
        );

        List<byte[]> left = client.lpop("list", 2);
        assertEquals(2, left.size());
        assertEquals("1", new String(left.get(0), StandardCharsets.UTF_8));
        assertEquals("2", new String(left.get(1), StandardCharsets.UTF_8));

        List<byte[]> right = client.rpop("list", 2);
        assertEquals(2, right.size());
        assertEquals("5", new String(right.get(0), StandardCharsets.UTF_8));
        assertEquals("4", new String(right.get(1), StandardCharsets.UTF_8));
    }

    @Test
    @DisplayName("LINDEX gets element by index")
    void testLindex() {
        client.rpush("list",
            "a".getBytes(StandardCharsets.UTF_8),
            "b".getBytes(StandardCharsets.UTF_8),
            "c".getBytes(StandardCharsets.UTF_8)
        );

        assertEquals("a", new String(client.lindex("list", 0), StandardCharsets.UTF_8));
        assertEquals("b", new String(client.lindex("list", 1), StandardCharsets.UTF_8));
        assertEquals("c", new String(client.lindex("list", 2), StandardCharsets.UTF_8));
        assertEquals("c", new String(client.lindex("list", -1), StandardCharsets.UTF_8));
        assertNull(client.lindex("list", 10));
    }

    @Test
    @DisplayName("LLEN returns list length")
    void testLlen() {
        assertEquals(0, client.llen("nonexistent"));

        client.rpush("list",
            "a".getBytes(StandardCharsets.UTF_8),
            "b".getBytes(StandardCharsets.UTF_8),
            "c".getBytes(StandardCharsets.UTF_8)
        );
        assertEquals(3, client.llen("list"));
    }

    @Test
    @DisplayName("LRANGE returns range of elements")
    void testLrange() {
        client.rpush("list",
            "a".getBytes(StandardCharsets.UTF_8),
            "b".getBytes(StandardCharsets.UTF_8),
            "c".getBytes(StandardCharsets.UTF_8),
            "d".getBytes(StandardCharsets.UTF_8),
            "e".getBytes(StandardCharsets.UTF_8)
        );

        List<byte[]> range1 = client.lrange("list", 0, 2);
        assertEquals(3, range1.size());
        assertEquals("a", new String(range1.get(0), StandardCharsets.UTF_8));
        assertEquals("b", new String(range1.get(1), StandardCharsets.UTF_8));
        assertEquals("c", new String(range1.get(2), StandardCharsets.UTF_8));

        List<byte[]> range2 = client.lrange("list", -2, -1);
        assertEquals(2, range2.size());
        assertEquals("d", new String(range2.get(0), StandardCharsets.UTF_8));
        assertEquals("e", new String(range2.get(1), StandardCharsets.UTF_8));
    }

    @Test
    @DisplayName("LSET updates element at index")
    void testLset() {
        client.rpush("list",
            "a".getBytes(StandardCharsets.UTF_8),
            "b".getBytes(StandardCharsets.UTF_8),
            "c".getBytes(StandardCharsets.UTF_8)
        );

        assertTrue(client.lset("list", 1, "B".getBytes(StandardCharsets.UTF_8)));
        assertEquals("B", new String(client.lindex("list", 1), StandardCharsets.UTF_8));
    }

    @Test
    @DisplayName("LINSERT inserts before or after pivot")
    void testLinsert() {
        client.rpush("list",
            "a".getBytes(StandardCharsets.UTF_8),
            "c".getBytes(StandardCharsets.UTF_8)
        );

        // Insert before "c"
        assertEquals(3, client.linsert("list", "BEFORE",
            "c".getBytes(StandardCharsets.UTF_8),
            "b".getBytes(StandardCharsets.UTF_8)
        ));

        List<byte[]> elements = client.lrange("list", 0, -1);
        assertEquals(List.of("a", "b", "c"),
            elements.stream().map(b -> new String(b, StandardCharsets.UTF_8)).toList()
        );

        // Insert after "c"
        assertEquals(4, client.linsert("list", "AFTER",
            "c".getBytes(StandardCharsets.UTF_8),
            "d".getBytes(StandardCharsets.UTF_8)
        ));

        List<byte[]> elements2 = client.lrange("list", 0, -1);
        assertEquals(List.of("a", "b", "c", "d"),
            elements2.stream().map(b -> new String(b, StandardCharsets.UTF_8)).toList()
        );
    }

    @Test
    @DisplayName("LTRIM trims list to specified range")
    void testLtrim() {
        client.rpush("list",
            "1".getBytes(StandardCharsets.UTF_8),
            "2".getBytes(StandardCharsets.UTF_8),
            "3".getBytes(StandardCharsets.UTF_8),
            "4".getBytes(StandardCharsets.UTF_8),
            "5".getBytes(StandardCharsets.UTF_8)
        );

        assertTrue(client.ltrim("list", 1, 3));

        List<byte[]> elements = client.lrange("list", 0, -1);
        assertEquals(List.of("2", "3", "4"),
            elements.stream().map(b -> new String(b, StandardCharsets.UTF_8)).toList()
        );
    }

    @Test
    @DisplayName("LREM removes matching elements")
    void testLrem() {
        client.rpush("list",
            "a".getBytes(StandardCharsets.UTF_8),
            "b".getBytes(StandardCharsets.UTF_8),
            "a".getBytes(StandardCharsets.UTF_8),
            "c".getBytes(StandardCharsets.UTF_8),
            "a".getBytes(StandardCharsets.UTF_8)
        );

        // Remove 2 occurrences of "a" from head
        assertEquals(2, client.lrem("list", 2, "a".getBytes(StandardCharsets.UTF_8)));

        List<byte[]> elements = client.lrange("list", 0, -1);
        assertEquals(List.of("b", "c", "a"),
            elements.stream().map(b -> new String(b, StandardCharsets.UTF_8)).toList()
        );
    }

    @Test
    @DisplayName("LREM with negative count removes from tail")
    void testLremFromTail() {
        client.rpush("list",
            "a".getBytes(StandardCharsets.UTF_8),
            "b".getBytes(StandardCharsets.UTF_8),
            "a".getBytes(StandardCharsets.UTF_8),
            "c".getBytes(StandardCharsets.UTF_8),
            "a".getBytes(StandardCharsets.UTF_8)
        );

        // Remove 2 occurrences of "a" from tail
        assertEquals(2, client.lrem("list", -2, "a".getBytes(StandardCharsets.UTF_8)));

        List<byte[]> elements = client.lrange("list", 0, -1);
        assertEquals(List.of("a", "b", "c"),
            elements.stream().map(b -> new String(b, StandardCharsets.UTF_8)).toList()
        );
    }

    @Test
    @DisplayName("LPOS finds position of element")
    void testLpos() {
        client.rpush("list",
            "a".getBytes(StandardCharsets.UTF_8),
            "b".getBytes(StandardCharsets.UTF_8),
            "c".getBytes(StandardCharsets.UTF_8),
            "b".getBytes(StandardCharsets.UTF_8),
            "d".getBytes(StandardCharsets.UTF_8)
        );

        assertEquals(1, client.lpos("list", "b".getBytes(StandardCharsets.UTF_8)));
        assertEquals(0, client.lpos("list", "a".getBytes(StandardCharsets.UTF_8)));
        assertNull(client.lpos("list", "z".getBytes(StandardCharsets.UTF_8)));
    }

    @Test
    @DisplayName("LMOVE moves element between lists")
    void testLmove() {
        client.rpush("src",
            "1".getBytes(StandardCharsets.UTF_8),
            "2".getBytes(StandardCharsets.UTF_8),
            "3".getBytes(StandardCharsets.UTF_8)
        );
        client.rpush("dst", "a".getBytes(StandardCharsets.UTF_8));

        byte[] moved = client.lmove("src", "dst", "LEFT", "RIGHT");
        assertEquals("1", new String(moved, StandardCharsets.UTF_8));

        assertEquals(List.of("2", "3"),
            client.lrange("src", 0, -1).stream()
                .map(b -> new String(b, StandardCharsets.UTF_8)).toList()
        );
        assertEquals(List.of("a", "1"),
            client.lrange("dst", 0, -1).stream()
                .map(b -> new String(b, StandardCharsets.UTF_8)).toList()
        );
    }

    @Test
    @DisplayName("LPUSHX only pushes to existing list")
    void testLpushX() {
        // Non-existent list
        assertEquals(0, client.lpushx("newlist", "value".getBytes(StandardCharsets.UTF_8)));
        assertEquals(0, client.llen("newlist"));

        // Existing list
        client.lpush("list", "first".getBytes(StandardCharsets.UTF_8));
        assertEquals(2, client.lpushx("list", "second".getBytes(StandardCharsets.UTF_8)));
    }

    @Test
    @DisplayName("RPUSHX only pushes to existing list")
    void testRpushX() {
        assertEquals(0, client.rpushx("newlist", "value".getBytes(StandardCharsets.UTF_8)));

        client.rpush("list", "first".getBytes(StandardCharsets.UTF_8));
        assertEquals(2, client.rpushx("list", "second".getBytes(StandardCharsets.UTF_8)));
    }
}
