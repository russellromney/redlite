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
        client = Redlite.open(":memory:");
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
    @DisplayName("HMSET sets multiple fields")
    void testHmset() {
        Map<String, byte[]> fields = new HashMap<>();
        fields.put("f1", "v1".getBytes(StandardCharsets.UTF_8));
        fields.put("f2", "v2".getBytes(StandardCharsets.UTF_8));
        fields.put("f3", "v3".getBytes(StandardCharsets.UTF_8));

        assertTrue(client.hmset("hash", fields));

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

        assertEquals(15, client.hincrBy("hash", "counter", 5));
        assertEquals(10, client.hincrBy("hash", "counter", -5));
    }

    @Test
    @DisplayName("HINCRBY on non-existent field starts from 0")
    void testHincrByNonExistent() {
        assertEquals(5, client.hincrBy("hash", "newcounter", 5));
    }

    @Test
    @DisplayName("HINCRBYFLOAT increments float field")
    void testHincrByFloat() {
        client.hset("hash", "price", "10.5".getBytes(StandardCharsets.UTF_8));

        double result = client.hincrByFloat("hash", "price", 2.5);
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
    @DisplayName("HSTRLEN returns field value length")
    void testHstrlen() {
        client.hset("hash", "field", "Hello World".getBytes(StandardCharsets.UTF_8));

        assertEquals(11, client.hstrlen("hash", "field"));
        assertEquals(0, client.hstrlen("hash", "nonexistent"));
    }

    @Test
    @DisplayName("HSCAN iterates through fields")
    void testHscan() {
        for (int i = 1; i <= 10; i++) {
            client.hset("hash", "field" + i, ("value" + i).getBytes(StandardCharsets.UTF_8));
        }

        Map<String, byte[]> allFields = new HashMap<>();
        String cursor = "0";

        do {
            ScanResult<Map.Entry<String, byte[]>> result = client.hscan("hash", cursor);
            cursor = result.getCursor();
            for (Map.Entry<String, byte[]> entry : result.getResult()) {
                allFields.put(entry.getKey(), entry.getValue());
            }
        } while (!"0".equals(cursor));

        assertEquals(10, allFields.size());
        for (int i = 1; i <= 10; i++) {
            assertTrue(allFields.containsKey("field" + i));
            assertEquals("value" + i, new String(allFields.get("field" + i), StandardCharsets.UTF_8));
        }
    }

    @Test
    @DisplayName("HSCAN with MATCH pattern")
    void testHscanWithMatch() {
        for (int i = 1; i <= 5; i++) {
            client.hset("hash", "user:" + i, ("u" + i).getBytes(StandardCharsets.UTF_8));
            client.hset("hash", "order:" + i, ("o" + i).getBytes(StandardCharsets.UTF_8));
        }

        Map<String, byte[]> userFields = new HashMap<>();
        String cursor = "0";

        do {
            ScanResult<Map.Entry<String, byte[]>> result = client.hscan("hash", cursor, new ScanOptions().match("user:*"));
            cursor = result.getCursor();
            for (Map.Entry<String, byte[]> entry : result.getResult()) {
                userFields.put(entry.getKey(), entry.getValue());
            }
        } while (!"0".equals(cursor));

        assertEquals(5, userFields.size());
        userFields.keySet().forEach(k -> assertTrue(k.startsWith("user:")));
    }

    @Test
    @DisplayName("HRANDFIELD returns random fields")
    void testHrandfield() {
        for (int i = 1; i <= 5; i++) {
            client.hset("hash", "f" + i, ("v" + i).getBytes(StandardCharsets.UTF_8));
        }

        String field = client.hrandfield("hash");
        assertNotNull(field);
        assertTrue(field.startsWith("f"));

        List<String> fields = client.hrandfield("hash", 3);
        assertEquals(3, fields.size());
    }
}
