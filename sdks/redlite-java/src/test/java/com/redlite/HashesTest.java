package com.redlite;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for hash commands.
 */
@DisplayName("Hash Commands")
class HashesTest {
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
    @DisplayName("HSET and HGET basic operations")
    void testHsetAndHget() {
        assertEquals(1, client.hset("hash", "field1", "value1".getBytes(StandardCharsets.UTF_8)));

        byte[] value = client.hget("hash", "field1");
        assertNotNull(value);
        assertEquals("value1", new String(value, StandardCharsets.UTF_8));
    }

    @Test
    @DisplayName("HGET returns null for non-existent field")
    void testHgetNonExistent() {
        assertNull(client.hget("hash", "nonexistent"));

        client.hset("hash", "field", "value".getBytes(StandardCharsets.UTF_8));
        assertNull(client.hget("hash", "otherfield"));
    }

    @Test
    @DisplayName("HSET updates existing field")
    void testHsetUpdate() {
        client.hset("hash", "field", "old".getBytes(StandardCharsets.UTF_8));

        // Updating returns 0 (field already exists)
        assertEquals(0, client.hset("hash", "field", "new".getBytes(StandardCharsets.UTF_8)));
        assertEquals("new", new String(client.hget("hash", "field"), StandardCharsets.UTF_8));
    }

    @Test
    @DisplayName("HSET with map sets multiple fields")
    void testHsetMultiple() {
        Map<String, byte[]> fields = new HashMap<>();
        fields.put("f1", "v1".getBytes(StandardCharsets.UTF_8));
        fields.put("f2", "v2".getBytes(StandardCharsets.UTF_8));
        fields.put("f3", "v3".getBytes(StandardCharsets.UTF_8));

        assertEquals(3, client.hset("hash", fields));

        assertEquals("v1", new String(client.hget("hash", "f1"), StandardCharsets.UTF_8));
        assertEquals("v2", new String(client.hget("hash", "f2"), StandardCharsets.UTF_8));
        assertEquals("v3", new String(client.hget("hash", "f3"), StandardCharsets.UTF_8));
    }

    @Test
    @DisplayName("HMGET gets multiple fields")
    void testHmget() {
        client.hset("hash", "f1", "v1".getBytes(StandardCharsets.UTF_8));
        client.hset("hash", "f2", "v2".getBytes(StandardCharsets.UTF_8));

        List<byte[]> values = client.hmget("hash", "f1", "f2", "nonexistent");
        assertEquals(3, values.size());
        assertEquals("v1", new String(values.get(0), StandardCharsets.UTF_8));
        assertEquals("v2", new String(values.get(1), StandardCharsets.UTF_8));
        assertNull(values.get(2));
    }

    @Test
    @DisplayName("HDEL removes fields")
    void testHdel() {
        client.hset("hash", "f1", "v1".getBytes(StandardCharsets.UTF_8));
        client.hset("hash", "f2", "v2".getBytes(StandardCharsets.UTF_8));
        client.hset("hash", "f3", "v3".getBytes(StandardCharsets.UTF_8));

        assertEquals(2, client.hdel("hash", "f1", "f2", "nonexistent"));
        assertNull(client.hget("hash", "f1"));
        assertNull(client.hget("hash", "f2"));
        assertNotNull(client.hget("hash", "f3"));
    }

    @Test
    @DisplayName("HEXISTS checks field existence")
    void testHexists() {
        client.hset("hash", "field", "value".getBytes(StandardCharsets.UTF_8));

        assertTrue(client.hexists("hash", "field"));
        assertFalse(client.hexists("hash", "nonexistent"));
        assertFalse(client.hexists("nonexistent", "field"));
    }

    @Test
    @DisplayName("HLEN returns number of fields")
    void testHlen() {
        assertEquals(0, client.hlen("nonexistent"));

        client.hset("hash", "f1", "v1".getBytes(StandardCharsets.UTF_8));
        assertEquals(1, client.hlen("hash"));

        client.hset("hash", "f2", "v2".getBytes(StandardCharsets.UTF_8));
        assertEquals(2, client.hlen("hash"));
    }

    @Test
    @DisplayName("HKEYS returns all field names")
    void testHkeys() {
        client.hset("hash", "f1", "v1".getBytes(StandardCharsets.UTF_8));
        client.hset("hash", "f2", "v2".getBytes(StandardCharsets.UTF_8));
        client.hset("hash", "f3", "v3".getBytes(StandardCharsets.UTF_8));

        List<String> keys = client.hkeys("hash");
        assertEquals(3, keys.size());
        assertTrue(keys.containsAll(List.of("f1", "f2", "f3")));
    }

    @Test
    @DisplayName("HVALS returns all values")
    void testHvals() {
        client.hset("hash", "f1", "v1".getBytes(StandardCharsets.UTF_8));
        client.hset("hash", "f2", "v2".getBytes(StandardCharsets.UTF_8));

        List<byte[]> values = client.hvals("hash");
        assertEquals(2, values.size());
        List<String> stringValues = values.stream()
            .map(v -> new String(v, StandardCharsets.UTF_8))
            .toList();
        assertTrue(stringValues.containsAll(List.of("v1", "v2")));
    }

    @Test
    @DisplayName("HGETALL returns all field-value pairs")
    void testHgetall() {
        client.hset("hash", "f1", "v1".getBytes(StandardCharsets.UTF_8));
        client.hset("hash", "f2", "v2".getBytes(StandardCharsets.UTF_8));

        Map<String, byte[]> all = client.hgetall("hash");
        assertEquals(2, all.size());
        assertEquals("v1", new String(all.get("f1"), StandardCharsets.UTF_8));
        assertEquals("v2", new String(all.get("f2"), StandardCharsets.UTF_8));
    }

    @Test
    @DisplayName("HINCRBY increments integer field")
    void testHincrBy() {
        client.hset("hash", "counter", "10".getBytes(StandardCharsets.UTF_8));

        assertEquals(15, client.hincrby("hash", "counter", 5));
        assertEquals(10, client.hincrby("hash", "counter", -5));
    }

    @Test
    @DisplayName("HINCRBY on non-existent field starts from 0")
    void testHincrByNonExistent() {
        assertEquals(5, client.hincrby("hash", "newcounter", 5));
    }

    @Test
    @DisplayName("HINCRBYFLOAT increments float field")
    void testHincrByFloat() {
        client.hset("hash", "price", "10.5".getBytes(StandardCharsets.UTF_8));

        double result = client.hincrbyfloat("hash", "price", 2.5);
        assertEquals(13.0, result, 0.001);
    }

    @Test
    @DisplayName("HSETNX sets field only if not exists")
    void testHsetNx() {
        assertTrue(client.hsetnx("hash", "field", "first".getBytes(StandardCharsets.UTF_8)));
        assertFalse(client.hsetnx("hash", "field", "second".getBytes(StandardCharsets.UTF_8)));
        assertEquals("first", new String(client.hget("hash", "field"), StandardCharsets.UTF_8));
    }

    @Test
    @DisplayName("HGETALL on empty hash returns empty map")
    void testHgetallEmpty() {
        Map<String, byte[]> all = client.hgetall("nonexistent");
        assertTrue(all.isEmpty());
    }

    @Test
    @DisplayName("HKEYS on empty hash returns empty list")
    void testHkeysEmpty() {
        List<String> keys = client.hkeys("nonexistent");
        assertTrue(keys.isEmpty());
    }

    @Test
    @DisplayName("HVALS on empty hash returns empty list")
    void testHvalsEmpty() {
        List<byte[]> values = client.hvals("nonexistent");
        assertTrue(values.isEmpty());
    }
}
