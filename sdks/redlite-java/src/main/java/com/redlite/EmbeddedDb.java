package com.redlite;

import org.jetbrains.annotations.Nullable;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

/**
 * JNI wrapper for the native Redlite database.
 * <p>
 * This class provides direct access to the embedded Redlite database
 * through JNI bindings. For most use cases, use the {@link Redlite} class instead,
 * which provides a unified API for both embedded and server modes.
 */
public class EmbeddedDb implements AutoCloseable {
    private static boolean libraryLoaded = false;
    private long ptr;

    static {
        loadNativeLibrary();
    }

    private static void loadNativeLibrary() {
        if (libraryLoaded) return;

        String osName = System.getProperty("os.name").toLowerCase();
        String libName;

        if (osName.contains("mac") || osName.contains("darwin")) {
            libName = "libredlite_jni.dylib";
        } else if (osName.contains("linux")) {
            libName = "libredlite_jni.so";
        } else if (osName.contains("windows")) {
            libName = "redlite_jni.dll";
        } else {
            throw new RedliteException("Unsupported OS: " + osName);
        }

        boolean loaded = tryLoadFromResources(libName)
                || tryLoadFromPath(libName)
                || tryLoadFromEnv(libName)
                || tryLoadSystem();

        if (!loaded) {
            throw new RedliteException(
                    "Failed to load native library. " +
                            "Ensure the redlite JNI library is available in the classpath or system path."
            );
        }

        libraryLoaded = true;
    }

    private static boolean tryLoadFromResources(String libName) {
        try {
            String resourcePath = "/native/" + libName;
            InputStream inputStream = EmbeddedDb.class.getResourceAsStream(resourcePath);
            if (inputStream == null) return false;

            Path tempFile = Files.createTempFile("redlite_jni", libName.substring(libName.lastIndexOf('.')));
            tempFile.toFile().deleteOnExit();

            try (OutputStream out = Files.newOutputStream(tempFile)) {
                inputStream.transferTo(out);
            }
            inputStream.close();

            System.load(tempFile.toAbsolutePath().toString());
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    private static boolean tryLoadFromPath(String libName) {
        String[] paths = {
                "native/target/release/" + libName,
                "../native/target/release/" + libName,
                "target/release/" + libName
        };

        for (String path : paths) {
            File file = new File(path);
            if (file.exists()) {
                try {
                    System.load(file.getAbsolutePath());
                    return true;
                } catch (UnsatisfiedLinkError e) {
                    // Try next path
                }
            }
        }
        return false;
    }

    private static boolean tryLoadFromEnv(String libName) {
        String envPath = System.getenv("REDLITE_NATIVE_PATH");
        if (envPath == null) return false;

        File file = new File(envPath, libName);
        if (file.exists()) {
            try {
                System.load(file.getAbsolutePath());
                return true;
            } catch (UnsatisfiedLinkError e) {
                return false;
            }
        }
        return false;
    }

    private static boolean tryLoadSystem() {
        try {
            System.loadLibrary("redlite_jni");
            return true;
        } catch (UnsatisfiedLinkError e) {
            return false;
        }
    }

    private EmbeddedDb(long ptr) {
        this.ptr = ptr;
    }

    /**
     * Open an in-memory database.
     */
    public static EmbeddedDb openMemory() {
        long ptr = nativeOpenMemory();
        if (ptr == 0) {
            throw new RedliteException("Failed to open in-memory database");
        }
        return new EmbeddedDb(ptr);
    }

    /**
     * Open a database at the given path with custom cache size.
     *
     * @param path    The path to the database file
     * @param cacheMb The cache size in megabytes
     */
    public static EmbeddedDb openWithCache(String path, int cacheMb) {
        long ptr = nativeOpenWithCache(path, cacheMb);
        if (ptr == 0) {
            throw new RedliteException("Failed to open database at " + path);
        }
        return new EmbeddedDb(ptr);
    }

    private void checkOpen() {
        if (ptr == 0) {
            throw new RedliteException("Database is closed");
        }
    }

    @Override
    public void close() {
        if (ptr != 0) {
            nativeClose(ptr);
            ptr = 0;
        }
    }

    // String Commands
    public @Nullable byte[] get(String key) {
        checkOpen();
        return nativeGet(ptr, key);
    }

    public boolean set(String key, byte[] value, @Nullable Long ttlSeconds) {
        checkOpen();
        return nativeSet(ptr, key, value, ttlSeconds != null ? ttlSeconds : -1);
    }

    public boolean set(String key, byte[] value) {
        return set(key, value, null);
    }

    public boolean setOpts(String key, byte[] value, SetOptions options) {
        checkOpen();
        return nativeSetOpts(ptr, key, value,
                options.getEx() != null ? options.getEx() : -1,
                options.getPx() != null ? options.getPx() : -1,
                options.isNx(), options.isXx());
    }

    public boolean setex(String key, long seconds, byte[] value) {
        checkOpen();
        return nativeSetex(ptr, key, seconds, value);
    }

    public boolean psetex(String key, long milliseconds, byte[] value) {
        checkOpen();
        return nativePsetex(ptr, key, milliseconds, value);
    }

    public @Nullable byte[] getdel(String key) {
        checkOpen();
        return nativeGetdel(ptr, key);
    }

    public long append(String key, byte[] value) {
        checkOpen();
        return nativeAppend(ptr, key, value);
    }

    public long strlen(String key) {
        checkOpen();
        return nativeStrlen(ptr, key);
    }

    public byte[] getrange(String key, long start, long end) {
        checkOpen();
        byte[] result = nativeGetrange(ptr, key, start, end);
        return result != null ? result : new byte[0];
    }

    public long setrange(String key, long offset, byte[] value) {
        checkOpen();
        return nativeSetrange(ptr, key, offset, value);
    }

    public long incr(String key) {
        checkOpen();
        return nativeIncr(ptr, key);
    }

    public long decr(String key) {
        checkOpen();
        return nativeDecr(ptr, key);
    }

    public long incrby(String key, long amount) {
        checkOpen();
        return nativeIncrby(ptr, key, amount);
    }

    public long decrby(String key, long amount) {
        checkOpen();
        return nativeDecrby(ptr, key, amount);
    }

    public double incrbyfloat(String key, double amount) {
        checkOpen();
        return nativeIncrbyfloat(ptr, key, amount);
    }

    // Key Commands
    public long delete(String... keys) {
        checkOpen();
        return nativeDelete(ptr, keys);
    }

    public long exists(String... keys) {
        checkOpen();
        return nativeExists(ptr, keys);
    }

    public String type(String key) {
        checkOpen();
        return nativeType(ptr, key);
    }

    public long ttl(String key) {
        checkOpen();
        return nativeTtl(ptr, key);
    }

    public long pttl(String key) {
        checkOpen();
        return nativePttl(ptr, key);
    }

    public boolean expire(String key, long seconds) {
        checkOpen();
        return nativeExpire(ptr, key, seconds);
    }

    public boolean pexpire(String key, long milliseconds) {
        checkOpen();
        return nativePexpire(ptr, key, milliseconds);
    }

    public boolean expireat(String key, long unixTime) {
        checkOpen();
        return nativeExpireat(ptr, key, unixTime);
    }

    public boolean pexpireat(String key, long unixTimeMs) {
        checkOpen();
        return nativePexpireat(ptr, key, unixTimeMs);
    }

    public boolean persist(String key) {
        checkOpen();
        return nativePersist(ptr, key);
    }

    public boolean rename(String src, String dst) {
        checkOpen();
        return nativeRename(ptr, src, dst);
    }

    public boolean renamenx(String src, String dst) {
        checkOpen();
        return nativeRenamenx(ptr, src, dst);
    }

    public List<String> keys(String pattern) {
        checkOpen();
        return Arrays.asList(nativeKeys(ptr, pattern));
    }

    public long dbsize() {
        checkOpen();
        return nativeDbsize(ptr);
    }

    public boolean flushdb() {
        checkOpen();
        return nativeFlushdb(ptr);
    }

    public boolean select(int db) {
        checkOpen();
        return nativeSelect(ptr, db);
    }

    // Hash Commands
    public long hset(String key, String field, byte[] value) {
        checkOpen();
        return nativeHset(ptr, key, field, value);
    }

    public long hset(String key, Map<String, byte[]> mapping) {
        checkOpen();
        String[] fields = mapping.keySet().toArray(new String[0]);
        byte[][] values = mapping.values().toArray(new byte[0][]);
        return nativeHsetMultiple(ptr, key, fields, values);
    }

    public @Nullable byte[] hget(String key, String field) {
        checkOpen();
        return nativeHget(ptr, key, field);
    }

    public long hdel(String key, String... fields) {
        checkOpen();
        return nativeHdel(ptr, key, fields);
    }

    public boolean hexists(String key, String field) {
        checkOpen();
        return nativeHexists(ptr, key, field);
    }

    public long hlen(String key) {
        checkOpen();
        return nativeHlen(ptr, key);
    }

    public List<String> hkeys(String key) {
        checkOpen();
        return Arrays.asList(nativeHkeys(ptr, key));
    }

    public List<byte[]> hvals(String key) {
        checkOpen();
        return Arrays.asList(nativeHvals(ptr, key));
    }

    public long hincrby(String key, String field, long amount) {
        checkOpen();
        return nativeHincrby(ptr, key, field, amount);
    }

    public double hincrbyfloat(String key, String field, double amount) {
        checkOpen();
        return nativeHincrbyfloat(ptr, key, field, amount);
    }

    public Map<String, byte[]> hgetall(String key) {
        checkOpen();
        byte[][] result = nativeHgetall(ptr, key);
        Map<String, byte[]> map = new LinkedHashMap<>();
        for (int i = 0; i < result.length - 1; i += 2) {
            map.put(new String(result[i]), result[i + 1]);
        }
        return map;
    }

    public List<byte[]> hmget(String key, String... fields) {
        checkOpen();
        return Arrays.asList(nativeHmget(ptr, key, fields));
    }

    public boolean hsetnx(String key, String field, byte[] value) {
        checkOpen();
        return nativeHsetnx(ptr, key, field, value);
    }

    // List Commands
    public long lpush(String key, byte[]... values) {
        checkOpen();
        return nativeLpush(ptr, key, values);
    }

    public long rpush(String key, byte[]... values) {
        checkOpen();
        return nativeRpush(ptr, key, values);
    }

    public List<byte[]> lpop(String key, int count) {
        checkOpen();
        return Arrays.asList(nativeLpop(ptr, key, count));
    }

    public @Nullable byte[] lpop(String key) {
        List<byte[]> result = lpop(key, 1);
        return result.isEmpty() ? null : result.get(0);
    }

    public List<byte[]> rpop(String key, int count) {
        checkOpen();
        return Arrays.asList(nativeRpop(ptr, key, count));
    }

    public @Nullable byte[] rpop(String key) {
        List<byte[]> result = rpop(key, 1);
        return result.isEmpty() ? null : result.get(0);
    }

    public long llen(String key) {
        checkOpen();
        return nativeLlen(ptr, key);
    }

    public List<byte[]> lrange(String key, long start, long stop) {
        checkOpen();
        return Arrays.asList(nativeLrange(ptr, key, start, stop));
    }

    public @Nullable byte[] lindex(String key, long index) {
        checkOpen();
        return nativeLindex(ptr, key, index);
    }

    public boolean lset(String key, long index, byte[] value) {
        checkOpen();
        return nativeLset(ptr, key, index, value);
    }

    public long lrem(String key, long count, byte[] value) {
        checkOpen();
        return nativeLrem(ptr, key, count, value);
    }

    public boolean ltrim(String key, long start, long stop) {
        checkOpen();
        return nativeLtrim(ptr, key, start, stop);
    }

    // Set Commands
    public long sadd(String key, byte[]... members) {
        checkOpen();
        return nativeSadd(ptr, key, members);
    }

    public long srem(String key, byte[]... members) {
        checkOpen();
        return nativeSrem(ptr, key, members);
    }

    public Set<byte[]> smembers(String key) {
        checkOpen();
        return new HashSet<>(Arrays.asList(nativeSmembers(ptr, key)));
    }

    public boolean sismember(String key, byte[] member) {
        checkOpen();
        return nativeSismember(ptr, key, member);
    }

    public long scard(String key) {
        checkOpen();
        return nativeScard(ptr, key);
    }

    // Sorted Set Commands
    public long zadd(String key, ZMember... members) {
        checkOpen();
        double[] scores = new double[members.length];
        byte[][] memberBytes = new byte[members.length][];
        for (int i = 0; i < members.length; i++) {
            scores[i] = members[i].score();
            memberBytes[i] = members[i].member();
        }
        return nativeZadd(ptr, key, scores, memberBytes);
    }

    public long zadd(String key, Map<byte[], Double> mapping) {
        checkOpen();
        double[] scores = new double[mapping.size()];
        byte[][] members = new byte[mapping.size()][];
        int i = 0;
        for (var entry : mapping.entrySet()) {
            scores[i] = entry.getValue();
            members[i] = entry.getKey();
            i++;
        }
        return nativeZadd(ptr, key, scores, members);
    }

    public long zrem(String key, byte[]... members) {
        checkOpen();
        return nativeZrem(ptr, key, members);
    }

    public @Nullable Double zscore(String key, byte[] member) {
        checkOpen();
        double result = nativeZscore(ptr, key, member);
        return Double.isNaN(result) ? null : result;
    }

    public long zcard(String key) {
        checkOpen();
        return nativeZcard(ptr, key);
    }

    public long zcount(String key, double min, double max) {
        checkOpen();
        return nativeZcount(ptr, key, min, max);
    }

    public double zincrby(String key, double amount, byte[] member) {
        checkOpen();
        return nativeZincrby(ptr, key, amount, member);
    }

    public List<byte[]> zrange(String key, long start, long stop) {
        checkOpen();
        Object[] result = nativeZrange(ptr, key, start, stop, false);
        List<byte[]> members = new ArrayList<>();
        for (int i = 0; i < result.length; i += 2) {
            members.add((byte[]) result[i]);
        }
        return members;
    }

    public List<ZMember> zrangeWithScores(String key, long start, long stop) {
        checkOpen();
        Object[] result = nativeZrange(ptr, key, start, stop, true);
        List<ZMember> members = new ArrayList<>();
        for (int i = 0; i < result.length - 1; i += 2) {
            byte[] member = (byte[]) result[i];
            double score = ((Number) result[i + 1]).doubleValue();
            members.add(new ZMember(score, member));
        }
        return members;
    }

    public List<byte[]> zrevrange(String key, long start, long stop) {
        checkOpen();
        Object[] result = nativeZrevrange(ptr, key, start, stop, false);
        List<byte[]> members = new ArrayList<>();
        for (int i = 0; i < result.length; i += 2) {
            members.add((byte[]) result[i]);
        }
        return members;
    }

    public List<ZMember> zrevrangeWithScores(String key, long start, long stop) {
        checkOpen();
        Object[] result = nativeZrevrange(ptr, key, start, stop, true);
        List<ZMember> members = new ArrayList<>();
        for (int i = 0; i < result.length - 1; i += 2) {
            byte[] member = (byte[]) result[i];
            double score = ((Number) result[i + 1]).doubleValue();
            members.add(new ZMember(score, member));
        }
        return members;
    }

    public @Nullable Long zrank(String key, byte[] member) {
        checkOpen();
        long result = nativeZrank(ptr, key, member);
        return result < 0 ? null : result;
    }

    public @Nullable Long zrevrank(String key, byte[] member) {
        checkOpen();
        long result = nativeZrevrank(ptr, key, member);
        return result < 0 ? null : result;
    }

    // Multi-key Commands
    public List<byte[]> mget(String... keys) {
        checkOpen();
        return Arrays.asList(nativeMget(ptr, keys));
    }

    public boolean mset(Map<String, byte[]> pairs) {
        checkOpen();
        String[] keys = pairs.keySet().toArray(new String[0]);
        byte[][] values = pairs.values().toArray(new byte[0][]);
        return nativeMset(ptr, keys, values);
    }

    // Server Commands
    public long vacuum() {
        checkOpen();
        return nativeVacuum(ptr);
    }

    public String ping() {
        checkOpen();
        return nativePing(ptr);
    }

    // Native method declarations
    private static native long nativeOpenMemory();
    private static native long nativeOpenWithCache(String path, int cacheMb);
    private static native void nativeClose(long ptr);

    // String commands
    private static native byte[] nativeGet(long ptr, String key);
    private static native boolean nativeSet(long ptr, String key, byte[] value, long ttlSeconds);
    private static native boolean nativeSetOpts(long ptr, String key, byte[] value, long ex, long px, boolean nx, boolean xx);
    private static native boolean nativeSetex(long ptr, String key, long seconds, byte[] value);
    private static native boolean nativePsetex(long ptr, String key, long milliseconds, byte[] value);
    private static native byte[] nativeGetdel(long ptr, String key);
    private static native long nativeAppend(long ptr, String key, byte[] value);
    private static native long nativeStrlen(long ptr, String key);
    private static native byte[] nativeGetrange(long ptr, String key, long start, long end);
    private static native long nativeSetrange(long ptr, String key, long offset, byte[] value);
    private static native long nativeIncr(long ptr, String key);
    private static native long nativeDecr(long ptr, String key);
    private static native long nativeIncrby(long ptr, String key, long amount);
    private static native long nativeDecrby(long ptr, String key, long amount);
    private static native double nativeIncrbyfloat(long ptr, String key, double amount);

    // Key commands
    private static native long nativeDelete(long ptr, String[] keys);
    private static native long nativeExists(long ptr, String[] keys);
    private static native String nativeType(long ptr, String key);
    private static native long nativeTtl(long ptr, String key);
    private static native long nativePttl(long ptr, String key);
    private static native boolean nativeExpire(long ptr, String key, long seconds);
    private static native boolean nativePexpire(long ptr, String key, long milliseconds);
    private static native boolean nativeExpireat(long ptr, String key, long unixTime);
    private static native boolean nativePexpireat(long ptr, String key, long unixTimeMs);
    private static native boolean nativePersist(long ptr, String key);
    private static native boolean nativeRename(long ptr, String src, String dst);
    private static native boolean nativeRenamenx(long ptr, String src, String dst);
    private static native String[] nativeKeys(long ptr, String pattern);
    private static native long nativeDbsize(long ptr);
    private static native boolean nativeFlushdb(long ptr);
    private static native boolean nativeSelect(long ptr, int db);

    // Hash commands
    private static native long nativeHset(long ptr, String key, String field, byte[] value);
    private static native long nativeHsetMultiple(long ptr, String key, String[] fields, byte[][] values);
    private static native byte[] nativeHget(long ptr, String key, String field);
    private static native long nativeHdel(long ptr, String key, String[] fields);
    private static native boolean nativeHexists(long ptr, String key, String field);
    private static native long nativeHlen(long ptr, String key);
    private static native String[] nativeHkeys(long ptr, String key);
    private static native byte[][] nativeHvals(long ptr, String key);
    private static native long nativeHincrby(long ptr, String key, String field, long amount);
    private static native double nativeHincrbyfloat(long ptr, String key, String field, double amount);
    private static native byte[][] nativeHgetall(long ptr, String key);
    private static native byte[][] nativeHmget(long ptr, String key, String[] fields);
    private static native boolean nativeHsetnx(long ptr, String key, String field, byte[] value);

    // List commands
    private static native long nativeLpush(long ptr, String key, byte[][] values);
    private static native long nativeRpush(long ptr, String key, byte[][] values);
    private static native byte[][] nativeLpop(long ptr, String key, int count);
    private static native byte[][] nativeRpop(long ptr, String key, int count);
    private static native long nativeLlen(long ptr, String key);
    private static native byte[][] nativeLrange(long ptr, String key, long start, long stop);
    private static native byte[] nativeLindex(long ptr, String key, long index);
    private static native boolean nativeLset(long ptr, String key, long index, byte[] value);
    private static native long nativeLrem(long ptr, String key, long count, byte[] value);
    private static native boolean nativeLtrim(long ptr, String key, long start, long stop);

    // Set commands
    private static native long nativeSadd(long ptr, String key, byte[][] members);
    private static native long nativeSrem(long ptr, String key, byte[][] members);
    private static native byte[][] nativeSmembers(long ptr, String key);
    private static native boolean nativeSismember(long ptr, String key, byte[] member);
    private static native long nativeScard(long ptr, String key);

    // Sorted set commands
    private static native long nativeZadd(long ptr, String key, double[] scores, byte[][] members);
    private static native long nativeZrem(long ptr, String key, byte[][] members);
    private static native double nativeZscore(long ptr, String key, byte[] member);
    private static native long nativeZcard(long ptr, String key);
    private static native long nativeZcount(long ptr, String key, double min, double max);
    private static native double nativeZincrby(long ptr, String key, double amount, byte[] member);
    private static native Object[] nativeZrange(long ptr, String key, long start, long stop, boolean withScores);
    private static native Object[] nativeZrevrange(long ptr, String key, long start, long stop, boolean withScores);
    private static native long nativeZrank(long ptr, String key, byte[] member);
    private static native long nativeZrevrank(long ptr, String key, byte[] member);

    // Multi-key commands
    private static native byte[][] nativeMget(long ptr, String[] keys);
    private static native boolean nativeMset(long ptr, String[] keys, byte[][] values);

    // Server commands
    private static native long nativeVacuum(long ptr);
    private static native String nativePing(long ptr);
}
