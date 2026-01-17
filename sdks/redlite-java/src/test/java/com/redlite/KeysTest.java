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
 * Unit tests for key commands.
 */
@DisplayName("Keys Commands")
class KeysTest {
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
    @DisplayName("DEL removes existing keys")
    void testDel() {
        client.set("del1", "v1".getBytes(StandardCharsets.UTF_8));
        client.set("del2", "v2".getBytes(StandardCharsets.UTF_8));
        client.set("del3", "v3".getBytes(StandardCharsets.UTF_8));

        assertEquals(2, client.del("del1", "del2", "nonexistent"));
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

        client.zadd("zsetkey", new ZMember("member", 1.0));
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

        assertFalse(client.renameNx("src", "existing"));
        assertEquals("value", new String(client.get("src"), StandardCharsets.UTF_8));

        assertTrue(client.renameNx("src", "newdest"));
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
        assertTrue(client.expireAt("expireatkey", futureTime));

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
    @DisplayName("COPY duplicates a key")
    void testCopy() {
        client.set("src", "value".getBytes(StandardCharsets.UTF_8));

        assertTrue(client.copy("src", "dst"));
        assertEquals("value", new String(client.get("dst"), StandardCharsets.UTF_8));
        assertEquals("value", new String(client.get("src"), StandardCharsets.UTF_8)); // Original still exists
    }

    @Test
    @DisplayName("COPY with REPLACE option")
    void testCopyReplace() {
        client.set("src", "newvalue".getBytes(StandardCharsets.UTF_8));
        client.set("dst", "oldvalue".getBytes(StandardCharsets.UTF_8));

        assertTrue(client.copy("src", "dst", true));
        assertEquals("newvalue", new String(client.get("dst"), StandardCharsets.UTF_8));
    }

    @Test
    @DisplayName("DUMP and RESTORE key serialization")
    void testDumpRestore() {
        client.set("dumpkey", "dumpvalue".getBytes(StandardCharsets.UTF_8));

        byte[] serialized = client.dump("dumpkey");
        assertNotNull(serialized);
        assertTrue(serialized.length > 0);

        client.del("dumpkey");
        assertNull(client.get("dumpkey"));

        assertTrue(client.restore("dumpkey", 0, serialized));
        assertEquals("dumpvalue", new String(client.get("dumpkey"), StandardCharsets.UTF_8));
    }

    @Test
    @DisplayName("OBJECT ENCODING returns encoding type")
    void testObjectEncoding() {
        client.set("intkey", "12345".getBytes(StandardCharsets.UTF_8));
        // Could be "int" or "embstr" depending on implementation
        String encoding = client.objectEncoding("intkey");
        assertNotNull(encoding);

        client.set("strkey", "this is a longer string value".getBytes(StandardCharsets.UTF_8));
        String strEncoding = client.objectEncoding("strkey");
        assertNotNull(strEncoding);
    }

    @Test
    @DisplayName("RANDOMKEY returns a random key")
    void testRandomKey() {
        // Empty database
        assertNull(client.randomKey());

        client.set("rkey1", "v1".getBytes(StandardCharsets.UTF_8));
        client.set("rkey2", "v2".getBytes(StandardCharsets.UTF_8));

        String randomKey = client.randomKey();
        assertNotNull(randomKey);
        assertTrue(List.of("rkey1", "rkey2").contains(randomKey));
    }

    @Test
    @DisplayName("TOUCH updates access time")
    void testTouch() {
        client.set("touch1", "v1".getBytes(StandardCharsets.UTF_8));
        client.set("touch2", "v2".getBytes(StandardCharsets.UTF_8));

        assertEquals(2, client.touch("touch1", "touch2", "nonexistent"));
    }

    @Test
    @DisplayName("UNLINK removes keys asynchronously")
    void testUnlink() {
        client.set("unlink1", "v1".getBytes(StandardCharsets.UTF_8));
        client.set("unlink2", "v2".getBytes(StandardCharsets.UTF_8));

        assertEquals(2, client.unlink("unlink1", "unlink2"));
        assertNull(client.get("unlink1"));
        assertNull(client.get("unlink2"));
    }

    @Test
    @DisplayName("SCAN iterates through keys")
    void testScan() {
        // Add some keys
        for (int i = 1; i <= 10; i++) {
            client.set("scan:" + i, ("value" + i).getBytes(StandardCharsets.UTF_8));
        }

        Set<String> allKeys = new HashSet<>();
        String cursor = "0";

        do {
            ScanResult<String> result = client.scan(cursor);
            cursor = result.getCursor();
            allKeys.addAll(result.getResult());
        } while (!"0".equals(cursor));

        assertEquals(10, allKeys.size());
        for (int i = 1; i <= 10; i++) {
            assertTrue(allKeys.contains("scan:" + i));
        }
    }

    @Test
    @DisplayName("SCAN with MATCH pattern")
    void testScanWithMatch() {
        for (int i = 1; i <= 5; i++) {
            client.set("user:" + i, ("u" + i).getBytes(StandardCharsets.UTF_8));
            client.set("order:" + i, ("o" + i).getBytes(StandardCharsets.UTF_8));
        }

        Set<String> userKeys = new HashSet<>();
        String cursor = "0";

        do {
            ScanResult<String> result = client.scan(cursor, new ScanOptions().match("user:*"));
            cursor = result.getCursor();
            userKeys.addAll(result.getResult());
        } while (!"0".equals(cursor));

        assertEquals(5, userKeys.size());
        userKeys.forEach(k -> assertTrue(k.startsWith("user:")));
    }

    @Test
    @DisplayName("SCAN with COUNT hint")
    void testScanWithCount() {
        for (int i = 1; i <= 100; i++) {
            client.set("count:" + i, ("v" + i).getBytes(StandardCharsets.UTF_8));
        }

        ScanResult<String> result = client.scan("0", new ScanOptions().count(10));
        // COUNT is a hint, not a guarantee, but should return some results
        assertFalse(result.getResult().isEmpty());
    }
}
