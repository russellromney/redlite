package com.redlite;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;

import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for key commands.
 */
@DisplayName("Keys Commands")
class KeysTest {
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
    @DisplayName("DELETE removes existing keys")
    void testDelete() {
        client.set("del1", "v1".getBytes(StandardCharsets.UTF_8));
        client.set("del2", "v2".getBytes(StandardCharsets.UTF_8));
        client.set("del3", "v3".getBytes(StandardCharsets.UTF_8));

        assertEquals(2, client.delete("del1", "del2", "nonexistent"));
        assertNull(client.get("del1"));
        assertNull(client.get("del2"));
        assertNotNull(client.get("del3"));
    }

    @Test
    @DisplayName("EXISTS checks key existence")
    void testExists() {
        client.set("exists1", "v1".getBytes(StandardCharsets.UTF_8));
        client.set("exists2", "v2".getBytes(StandardCharsets.UTF_8));

        assertEquals(2, client.exists("exists1", "exists2", "nonexistent"));
        assertEquals(0, client.exists("none1", "none2"));
    }

    @Test
    @DisplayName("TYPE returns correct key type")
    void testType() {
        client.set("strkey", "value".getBytes(StandardCharsets.UTF_8));
        assertEquals("string", client.type("strkey"));

        client.lpush("listkey", "item".getBytes(StandardCharsets.UTF_8));
        assertEquals("list", client.type("listkey"));

        client.hset("hashkey", "field", "value".getBytes(StandardCharsets.UTF_8));
        assertEquals("hash", client.type("hashkey"));

        client.sadd("setkey", "member".getBytes(StandardCharsets.UTF_8));
        assertEquals("set", client.type("setkey"));

        client.zadd("zsetkey", ZMember.of(1.0, "member"));
        assertEquals("zset", client.type("zsetkey"));

        assertEquals("none", client.type("nonexistent"));
    }

    @Test
    @DisplayName("RENAME renames a key")
    void testRename() {
        client.set("oldname", "value".getBytes(StandardCharsets.UTF_8));

        assertTrue(client.rename("oldname", "newname"));
        assertNull(client.get("oldname"));
        assertEquals("value", new String(client.get("newname"), StandardCharsets.UTF_8));
    }

    @Test
    @DisplayName("RENAMENX renames only if new key doesn't exist")
    void testRenameNx() {
        client.set("src", "value".getBytes(StandardCharsets.UTF_8));
        client.set("existing", "other".getBytes(StandardCharsets.UTF_8));

        assertFalse(client.renamenx("src", "existing"));
        assertEquals("value", new String(client.get("src"), StandardCharsets.UTF_8));

        assertTrue(client.renamenx("src", "newdest"));
        assertNull(client.get("src"));
        assertEquals("value", new String(client.get("newdest"), StandardCharsets.UTF_8));
    }

    @Test
    @DisplayName("KEYS returns matching keys")
    void testKeys() {
        client.set("user:1", "a".getBytes(StandardCharsets.UTF_8));
        client.set("user:2", "b".getBytes(StandardCharsets.UTF_8));
        client.set("user:3", "c".getBytes(StandardCharsets.UTF_8));
        client.set("other:1", "d".getBytes(StandardCharsets.UTF_8));

        List<String> userKeys = client.keys("user:*");
        assertEquals(3, userKeys.size());
        assertTrue(userKeys.containsAll(List.of("user:1", "user:2", "user:3")));

        List<String> allKeys = client.keys("*");
        assertEquals(4, allKeys.size());
    }

    @Test
    @DisplayName("EXPIRE and TTL operations")
    void testExpireAndTtl() {
        client.set("expirekey", "value".getBytes(StandardCharsets.UTF_8));

        // Key without expiry
        assertEquals(-1, client.ttl("expirekey"));

        // Set expiry
        assertTrue(client.expire("expirekey", 100));
        long ttl = client.ttl("expirekey");
        assertTrue(ttl > 0 && ttl <= 100);

        // Non-existent key
        assertEquals(-2, client.ttl("nonexistent"));
    }

    @Test
    @DisplayName("EXPIREAT sets expiration timestamp")
    void testExpireAt() {
        client.set("expireatkey", "value".getBytes(StandardCharsets.UTF_8));

        long futureTime = System.currentTimeMillis() / 1000 + 3600; // 1 hour from now
        assertTrue(client.expireat("expireatkey", futureTime));

        long ttl = client.ttl("expireatkey");
        assertTrue(ttl > 3500 && ttl <= 3600);
    }

    @Test
    @DisplayName("PEXPIRE and PTTL millisecond operations")
    void testPexpireAndPttl() {
        client.set("pexpirekey", "value".getBytes(StandardCharsets.UTF_8));

        assertTrue(client.pexpire("pexpirekey", 10000)); // 10 seconds
        long pttl = client.pttl("pexpirekey");
        assertTrue(pttl > 0 && pttl <= 10000);
    }

    @Test
    @DisplayName("PERSIST removes expiration")
    void testPersist() {
        client.set("persistkey", "value".getBytes(StandardCharsets.UTF_8));
        client.expire("persistkey", 100);

        assertTrue(client.ttl("persistkey") > 0);

        assertTrue(client.persist("persistkey"));
        assertEquals(-1, client.ttl("persistkey"));
    }

    @Test
    @DisplayName("DBSIZE returns number of keys")
    void testDbSize() {
        assertEquals(0, client.dbsize());

        client.set("key1", "v1".getBytes(StandardCharsets.UTF_8));
        client.set("key2", "v2".getBytes(StandardCharsets.UTF_8));

        assertEquals(2, client.dbsize());
    }

    @Test
    @DisplayName("FLUSHDB clears the database")
    void testFlushDb() {
        client.set("key1", "v1".getBytes(StandardCharsets.UTF_8));
        client.set("key2", "v2".getBytes(StandardCharsets.UTF_8));

        assertTrue(client.flushdb());
        assertEquals(0, client.dbsize());
    }

    @Test
    @DisplayName("PEXPIREAT sets expiration timestamp in milliseconds")
    void testPexpireAt() {
        client.set("pexpireatkey", "value".getBytes(StandardCharsets.UTF_8));

        long futureTimeMs = System.currentTimeMillis() + 3600000; // 1 hour from now
        assertTrue(client.pexpireat("pexpireatkey", futureTimeMs));

        long pttl = client.pttl("pexpireatkey");
        assertTrue(pttl > 3500000 && pttl <= 3600000);
    }

    @Test
    @DisplayName("SELECT switches database")
    void testSelect() {
        client.set("key1", "value1".getBytes(StandardCharsets.UTF_8));

        // Select database 1
        assertTrue(client.select(1));
        assertNull(client.get("key1")); // Key doesn't exist in db 1

        // Switch back to db 0
        assertTrue(client.select(0));
        assertNotNull(client.get("key1")); // Key exists in db 0
    }
}
