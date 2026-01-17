package com.redlite;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for string commands.
 */
@DisplayName("Strings Commands")
class StringsTest {
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
    @DisplayName("SET and GET basic operations")
    void testSetAndGet() {
        boolean result = client.set("key1", "value1".getBytes(StandardCharsets.UTF_8));
        assertTrue(result);

        byte[] value = client.get("key1");
        assertNotNull(value);
        assertEquals("value1", new String(value, StandardCharsets.UTF_8));
    }

    @Test
    @DisplayName("GET returns null for non-existent key")
    void testGetNonExistent() {
        byte[] value = client.get("nonexistent");
        assertNull(value);
    }

    @Test
    @DisplayName("SET with NX option - only set if not exists")
    void testSetNx() {
        SetOptions opts = new SetOptions.Builder().nx().build();

        // First SET should succeed
        assertTrue(client.set("nxkey", "first".getBytes(StandardCharsets.UTF_8), opts));
        assertEquals("first", new String(client.get("nxkey"), StandardCharsets.UTF_8));

        // Second SET with NX should fail (key exists)
        assertFalse(client.set("nxkey", "second".getBytes(StandardCharsets.UTF_8), opts));
        assertEquals("first", new String(client.get("nxkey"), StandardCharsets.UTF_8));
    }

    @Test
    @DisplayName("SET with XX option - only set if exists")
    void testSetXx() {
        SetOptions opts = new SetOptions.Builder().xx().build();

        // SET with XX on non-existent key should fail
        assertFalse(client.set("xxkey", "value".getBytes(StandardCharsets.UTF_8), opts));
        assertNull(client.get("xxkey"));

        // Create the key first
        client.set("xxkey", "initial".getBytes(StandardCharsets.UTF_8));

        // Now SET with XX should succeed
        assertTrue(client.set("xxkey", "updated".getBytes(StandardCharsets.UTF_8), opts));
        assertEquals("updated", new String(client.get("xxkey"), StandardCharsets.UTF_8));
    }

    @Test
    @DisplayName("SET with EX option - expiration in seconds")
    void testSetWithExpiry() {
        SetOptions opts = new SetOptions.Builder().ex(10).build();
        assertTrue(client.set("expkey", "value".getBytes(StandardCharsets.UTF_8), opts));

        long ttl = client.ttl("expkey");
        assertTrue(ttl > 0 && ttl <= 10);
    }

    @Test
    @DisplayName("MSET and MGET multiple keys")
    void testMsetAndMget() {
        Map<String, byte[]> keyValues = Map.of(
            "mkey1", "mval1".getBytes(StandardCharsets.UTF_8),
            "mkey2", "mval2".getBytes(StandardCharsets.UTF_8),
            "mkey3", "mval3".getBytes(StandardCharsets.UTF_8)
        );

        boolean result = client.mset(keyValues);
        assertTrue(result);

        List<byte[]> values = client.mget("mkey1", "mkey2", "mkey3", "nonexistent");
        assertEquals(4, values.size());
        assertEquals("mval1", new String(values.get(0), StandardCharsets.UTF_8));
        assertEquals("mval2", new String(values.get(1), StandardCharsets.UTF_8));
        assertEquals("mval3", new String(values.get(2), StandardCharsets.UTF_8));
        assertNull(values.get(3));
    }

    @Test
    @DisplayName("INCR and DECR operations")
    void testIncrDecr() {
        client.set("counter", "10".getBytes(StandardCharsets.UTF_8));

        assertEquals(11, client.incr("counter"));
        assertEquals(12, client.incr("counter"));
        assertEquals(11, client.decr("counter"));
        assertEquals(10, client.decr("counter"));
    }

    @Test
    @DisplayName("INCR on non-existent key starts from 0")
    void testIncrNonExistent() {
        assertEquals(1, client.incr("newcounter"));
        assertEquals(2, client.incr("newcounter"));
    }

    @Test
    @DisplayName("INCRBY and DECRBY operations")
    void testIncrByDecrBy() {
        client.set("counter", "100".getBytes(StandardCharsets.UTF_8));

        assertEquals(110, client.incrBy("counter", 10));
        assertEquals(85, client.decrBy("counter", 25));
    }

    @Test
    @DisplayName("INCRBYFLOAT operation")
    void testIncrByFloat() {
        client.set("floatkey", "10.5".getBytes(StandardCharsets.UTF_8));

        double result = client.incrByFloat("floatkey", 2.5);
        assertEquals(13.0, result, 0.001);
    }

    @Test
    @DisplayName("APPEND to existing key")
    void testAppend() {
        client.set("appendkey", "Hello".getBytes(StandardCharsets.UTF_8));

        long newLen = client.append("appendkey", " World".getBytes(StandardCharsets.UTF_8));
        assertEquals(11, newLen);
        assertEquals("Hello World", new String(client.get("appendkey"), StandardCharsets.UTF_8));
    }

    @Test
    @DisplayName("APPEND to non-existent key creates it")
    void testAppendNonExistent() {
        long len = client.append("newappend", "value".getBytes(StandardCharsets.UTF_8));
        assertEquals(5, len);
        assertEquals("value", new String(client.get("newappend"), StandardCharsets.UTF_8));
    }

    @Test
    @DisplayName("STRLEN returns correct length")
    void testStrlen() {
        client.set("strlenkey", "Hello World".getBytes(StandardCharsets.UTF_8));
        assertEquals(11, client.strlen("strlenkey"));

        // Non-existent key returns 0
        assertEquals(0, client.strlen("nonexistent"));
    }

    @Test
    @DisplayName("GETRANGE returns substring")
    void testGetRange() {
        client.set("rangekey", "Hello World".getBytes(StandardCharsets.UTF_8));

        assertEquals("Hello", new String(client.getRange("rangekey", 0, 4), StandardCharsets.UTF_8));
        assertEquals("World", new String(client.getRange("rangekey", 6, 10), StandardCharsets.UTF_8));
        assertEquals("World", new String(client.getRange("rangekey", -5, -1), StandardCharsets.UTF_8));
    }

    @Test
    @DisplayName("SETRANGE modifies part of string")
    void testSetRange() {
        client.set("setrangekey", "Hello World".getBytes(StandardCharsets.UTF_8));

        long newLen = client.setRange("setrangekey", 6, "Redis".getBytes(StandardCharsets.UTF_8));
        assertEquals(11, newLen);
        assertEquals("Hello Redis", new String(client.get("setrangekey"), StandardCharsets.UTF_8));
    }

    @Test
    @DisplayName("SETNX sets only if not exists")
    void testSetNxCommand() {
        assertTrue(client.setNx("setnxkey", "first".getBytes(StandardCharsets.UTF_8)));
        assertFalse(client.setNx("setnxkey", "second".getBytes(StandardCharsets.UTF_8)));
        assertEquals("first", new String(client.get("setnxkey"), StandardCharsets.UTF_8));
    }

    @Test
    @DisplayName("GETSET returns old value and sets new")
    void testGetSet() {
        client.set("getsetkey", "old".getBytes(StandardCharsets.UTF_8));

        byte[] oldValue = client.getSet("getsetkey", "new".getBytes(StandardCharsets.UTF_8));
        assertEquals("old", new String(oldValue, StandardCharsets.UTF_8));
        assertEquals("new", new String(client.get("getsetkey"), StandardCharsets.UTF_8));
    }

    @Test
    @DisplayName("GETSET on non-existent key returns null")
    void testGetSetNonExistent() {
        byte[] oldValue = client.getSet("newgetset", "value".getBytes(StandardCharsets.UTF_8));
        assertNull(oldValue);
        assertEquals("value", new String(client.get("newgetset"), StandardCharsets.UTF_8));
    }
}
