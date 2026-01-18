package com.redlite;

import com.redlite.namespaces.FTSNamespace;
import com.redlite.namespaces.GeoNamespace;
import com.redlite.namespaces.HistoryNamespace;
import com.redlite.namespaces.JSONNamespace;
import com.redlite.namespaces.VectorNamespace;
import org.jetbrains.annotations.Nullable;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.params.SetParams;

import java.io.Closeable;
import java.nio.charset.StandardCharsets;
import java.util.*;

/**
 * Unified Redlite client supporting both embedded and server modes.
 * <p>
 * <b>Embedded mode</b> (JNI native bindings, no network, microsecond latency):
 * <pre>{@code
 * var db = new Redlite(":memory:");
 * var db = new Redlite("/path/to/db.db");
 * }</pre>
 * <p>
 * <b>Server mode</b> (wraps Jedis Redis client):
 * <pre>{@code
 * var db = new Redlite("redis://localhost:6379");
 * }</pre>
 * <p>
 * Both modes expose the same API. Embedded mode is faster (microsecond latency),
 * server mode connects to a running redlite or Redis server.
 */
public class Redlite implements Closeable, AutoCloseable {

    /**
     * Connection mode enumeration.
     */
    public enum Mode {
        EMBEDDED, SERVER
    }

    private final Mode mode;
    private EmbeddedDb nativeDb;
    private JedisPool jedisPool;

    private final FTSNamespace fts;
    private final VectorNamespace vector;
    private final GeoNamespace geo;
    private final HistoryNamespace history;
    private final JSONNamespace json;

    /**
     * Create a new Redlite client with an in-memory database.
     */
    public Redlite() {
        this(":memory:");
    }

    /**
     * Create a new Redlite client.
     *
     * @param url Connection URL or file path:
     *            - ":memory:" for in-memory embedded database
     *            - "/path/to/db.db" for file-based embedded database
     *            - "redis://host:port" for server mode
     *            - "rediss://host:port" for TLS server mode
     */
    public Redlite(String url) {
        this(url, 64);
    }

    /**
     * Create a new Redlite client with custom cache size.
     *
     * @param url     Connection URL or file path
     * @param cacheMb SQLite cache size in MB (embedded mode only)
     */
    public Redlite(String url, int cacheMb) {
        if (url.startsWith("redis://") || url.startsWith("rediss://")) {
            this.mode = Mode.SERVER;
            this.jedisPool = new JedisPool(url);
            this.nativeDb = null;
        } else {
            this.mode = Mode.EMBEDDED;
            this.jedisPool = null;
            if (":memory:".equals(url)) {
                this.nativeDb = EmbeddedDb.openMemory();
            } else {
                this.nativeDb = EmbeddedDb.openWithCache(url, cacheMb);
            }
        }

        this.fts = new FTSNamespace(this);
        this.vector = new VectorNamespace(this);
        this.geo = new GeoNamespace(this);
        this.history = new HistoryNamespace(this);
        this.json = new JSONNamespace(this);
    }

    /**
     * Get the connection mode.
     */
    public Mode getMode() {
        return mode;
    }

    /**
     * Get the FTS namespace.
     */
    public FTSNamespace fts() {
        return fts;
    }

    /**
     * Get the Vector namespace.
     */
    public VectorNamespace vector() {
        return vector;
    }

    /**
     * Get the Geo namespace.
     */
    public GeoNamespace geo() {
        return geo;
    }

    /**
     * Get the History namespace.
     */
    public HistoryNamespace history() {
        return history;
    }

    /**
     * Get the JSON namespace.
     */
    public JSONNamespace json() {
        return json;
    }

    @Override
    public void close() {
        if (nativeDb != null) {
            nativeDb.close();
            nativeDb = null;
        }
        if (jedisPool != null) {
            jedisPool.close();
            jedisPool = null;
        }
    }

    private void checkOpen() {
        if (mode == Mode.EMBEDDED && nativeDb == null) {
            throw new RedliteException("Database is closed");
        }
        if (mode == Mode.SERVER && jedisPool == null) {
            throw new RedliteException("Connection is closed");
        }
    }

    private <T> T withJedis(java.util.function.Function<Jedis, T> fn) {
        if (jedisPool == null) {
            throw new RedliteException("Connection is closed");
        }
        try (Jedis jedis = jedisPool.getResource()) {
            return fn.apply(jedis);
        }
    }

    // =========================================================================
    // String Commands
    // =========================================================================

    /**
     * Get the value of a key.
     */
    public @Nullable byte[] get(String key) {
        checkOpen();
        if (mode == Mode.SERVER) {
            return withJedis(j -> j.get(key.getBytes(StandardCharsets.UTF_8)));
        }
        return nativeDb.get(key);
    }

    /**
     * Set the value of a key.
     */
    public boolean set(String key, byte[] value) {
        return set(key, value, null);
    }

    /**
     * Set the value of a key with options.
     */
    public boolean set(String key, byte[] value, @Nullable SetOptions options) {
        checkOpen();
        if (mode == Mode.SERVER) {
            return withJedis(j -> {
                if (options != null) {
                    SetParams params = new SetParams();
                    if (options.getEx() != null) params.ex(options.getEx());
                    if (options.getPx() != null) params.px(options.getPx());
                    if (options.isNx()) params.nx();
                    if (options.isXx()) params.xx();
                    return j.set(key.getBytes(StandardCharsets.UTF_8), value, params) != null;
                }
                return "OK".equals(j.set(key.getBytes(StandardCharsets.UTF_8), value));
            });
        }
        if (options != null) {
            return nativeDb.setOpts(key, value, options);
        }
        return nativeDb.set(key, value);
    }

    /**
     * Set key with expiration in seconds.
     */
    public boolean setex(String key, long seconds, byte[] value) {
        checkOpen();
        if (mode == Mode.SERVER) {
            return withJedis(j ->
                    "OK".equals(j.setex(key.getBytes(StandardCharsets.UTF_8), seconds, value)));
        }
        return nativeDb.setex(key, seconds, value);
    }

    /**
     * Set key with expiration in milliseconds.
     */
    public boolean psetex(String key, long milliseconds, byte[] value) {
        checkOpen();
        if (mode == Mode.SERVER) {
            return withJedis(j ->
                    "OK".equals(j.psetex(key.getBytes(StandardCharsets.UTF_8), milliseconds, value)));
        }
        return nativeDb.psetex(key, milliseconds, value);
    }

    /**
     * Get and delete a key.
     */
    public @Nullable byte[] getdel(String key) {
        checkOpen();
        if (mode == Mode.SERVER) {
            return withJedis(j -> j.getDel(key.getBytes(StandardCharsets.UTF_8)));
        }
        return nativeDb.getdel(key);
    }

    /**
     * Append value to key, return new length.
     */
    public long append(String key, byte[] value) {
        checkOpen();
        if (mode == Mode.SERVER) {
            return withJedis(j -> j.append(key.getBytes(StandardCharsets.UTF_8), value));
        }
        return nativeDb.append(key, value);
    }

    /**
     * Get the length of the value stored at key.
     */
    public long strlen(String key) {
        checkOpen();
        if (mode == Mode.SERVER) {
            return withJedis(j -> j.strlen(key.getBytes(StandardCharsets.UTF_8)));
        }
        return nativeDb.strlen(key);
    }

    /**
     * Get a substring of the value stored at key.
     */
    public byte[] getrange(String key, long start, long end) {
        checkOpen();
        if (mode == Mode.SERVER) {
            byte[] result = withJedis(j ->
                    j.getrange(key.getBytes(StandardCharsets.UTF_8), start, end));
            return result != null ? result : new byte[0];
        }
        return nativeDb.getrange(key, start, end);
    }

    /**
     * Overwrite part of a string at key starting at offset.
     */
    public long setrange(String key, long offset, byte[] value) {
        checkOpen();
        if (mode == Mode.SERVER) {
            return withJedis(j -> j.setrange(key.getBytes(StandardCharsets.UTF_8), offset, value));
        }
        return nativeDb.setrange(key, offset, value);
    }

    /**
     * Increment the integer value of a key by one.
     */
    public long incr(String key) {
        checkOpen();
        if (mode == Mode.SERVER) {
            return withJedis(j -> j.incr(key));
        }
        return nativeDb.incr(key);
    }

    /**
     * Decrement the integer value of a key by one.
     */
    public long decr(String key) {
        checkOpen();
        if (mode == Mode.SERVER) {
            return withJedis(j -> j.decr(key));
        }
        return nativeDb.decr(key);
    }

    /**
     * Increment the integer value of a key by amount.
     */
    public long incrby(String key, long amount) {
        checkOpen();
        if (mode == Mode.SERVER) {
            return withJedis(j -> j.incrBy(key, amount));
        }
        return nativeDb.incrby(key, amount);
    }

    /**
     * Decrement the integer value of a key by amount.
     */
    public long decrby(String key, long amount) {
        checkOpen();
        if (mode == Mode.SERVER) {
            return withJedis(j -> j.decrBy(key, amount));
        }
        return nativeDb.decrby(key, amount);
    }

    /**
     * Increment the float value of a key by amount.
     */
    public double incrbyfloat(String key, double amount) {
        checkOpen();
        if (mode == Mode.SERVER) {
            return withJedis(j -> j.incrByFloat(key, amount));
        }
        return nativeDb.incrbyfloat(key, amount);
    }

    /**
     * Get values of multiple keys.
     */
    public List<byte[]> mget(String... keys) {
        checkOpen();
        if (keys.length == 0) return Collections.emptyList();
        if (mode == Mode.SERVER) {
            return withJedis(j -> {
                byte[][] byteKeys = Arrays.stream(keys)
                        .map(k -> k.getBytes(StandardCharsets.UTF_8))
                        .toArray(byte[][]::new);
                return j.mget(byteKeys);
            });
        }
        return nativeDb.mget(keys);
    }

    /**
     * Set multiple key-value pairs atomically.
     */
    public boolean mset(Map<String, byte[]> pairs) {
        checkOpen();
        if (pairs.isEmpty()) return true;
        if (mode == Mode.SERVER) {
            return withJedis(j -> {
                byte[][] args = new byte[pairs.size() * 2][];
                int i = 0;
                for (var entry : pairs.entrySet()) {
                    args[i++] = entry.getKey().getBytes(StandardCharsets.UTF_8);
                    args[i++] = entry.getValue();
                }
                return "OK".equals(j.mset(args));
            });
        }
        return nativeDb.mset(pairs);
    }

    // =========================================================================
    // Key Commands
    // =========================================================================

    /**
     * Delete one or more keys.
     */
    public long delete(String... keys) {
        checkOpen();
        if (keys.length == 0) return 0;
        if (mode == Mode.SERVER) {
            return withJedis(j -> j.del(keys));
        }
        return nativeDb.delete(keys);
    }

    /**
     * Check if keys exist.
     */
    public long exists(String... keys) {
        checkOpen();
        if (keys.length == 0) return 0;
        if (mode == Mode.SERVER) {
            return withJedis(j -> j.exists(keys));
        }
        return nativeDb.exists(keys);
    }

    /**
     * Get the type of a key.
     */
    public String type(String key) {
        checkOpen();
        if (mode == Mode.SERVER) {
            return withJedis(j -> j.type(key));
        }
        return nativeDb.type(key);
    }

    /**
     * Get the TTL of a key in seconds.
     */
    public long ttl(String key) {
        checkOpen();
        if (mode == Mode.SERVER) {
            return withJedis(j -> j.ttl(key));
        }
        return nativeDb.ttl(key);
    }

    /**
     * Get the TTL of a key in milliseconds.
     */
    public long pttl(String key) {
        checkOpen();
        if (mode == Mode.SERVER) {
            return withJedis(j -> j.pttl(key));
        }
        return nativeDb.pttl(key);
    }

    /**
     * Set a timeout on key in seconds.
     */
    public boolean expire(String key, long seconds) {
        checkOpen();
        if (mode == Mode.SERVER) {
            return withJedis(j -> j.expire(key, seconds) == 1L);
        }
        return nativeDb.expire(key, seconds);
    }

    /**
     * Set a timeout on key in milliseconds.
     */
    public boolean pexpire(String key, long milliseconds) {
        checkOpen();
        if (mode == Mode.SERVER) {
            return withJedis(j -> j.pexpire(key, milliseconds) == 1L);
        }
        return nativeDb.pexpire(key, milliseconds);
    }

    /**
     * Set an expiration time as Unix timestamp (seconds).
     */
    public boolean expireat(String key, long unixTime) {
        checkOpen();
        if (mode == Mode.SERVER) {
            return withJedis(j -> j.expireAt(key, unixTime) == 1L);
        }
        return nativeDb.expireat(key, unixTime);
    }

    /**
     * Set an expiration time as Unix timestamp (milliseconds).
     */
    public boolean pexpireat(String key, long unixTimeMs) {
        checkOpen();
        if (mode == Mode.SERVER) {
            return withJedis(j -> j.pexpireAt(key, unixTimeMs) == 1L);
        }
        return nativeDb.pexpireat(key, unixTimeMs);
    }

    /**
     * Remove the timeout on key.
     */
    public boolean persist(String key) {
        checkOpen();
        if (mode == Mode.SERVER) {
            return withJedis(j -> j.persist(key) == 1L);
        }
        return nativeDb.persist(key);
    }

    /**
     * Rename a key.
     */
    public boolean rename(String src, String dst) {
        checkOpen();
        if (mode == Mode.SERVER) {
            return withJedis(j -> "OK".equals(j.rename(src, dst)));
        }
        return nativeDb.rename(src, dst);
    }

    /**
     * Rename a key only if the new key doesn't exist.
     */
    public boolean renamenx(String src, String dst) {
        checkOpen();
        if (mode == Mode.SERVER) {
            return withJedis(j -> j.renamenx(src, dst) == 1L);
        }
        return nativeDb.renamenx(src, dst);
    }

    /**
     * Find all keys matching a pattern.
     */
    public List<String> keys(String pattern) {
        checkOpen();
        if (mode == Mode.SERVER) {
            return withJedis(j -> new ArrayList<>(j.keys(pattern)));
        }
        return nativeDb.keys(pattern);
    }

    /**
     * Return the number of keys in the database.
     */
    public long dbsize() {
        checkOpen();
        if (mode == Mode.SERVER) {
            return withJedis(Jedis::dbSize);
        }
        return nativeDb.dbsize();
    }

    /**
     * Delete all keys in the current database.
     */
    public boolean flushdb() {
        checkOpen();
        if (mode == Mode.SERVER) {
            return withJedis(j -> "OK".equals(j.flushDB()));
        }
        return nativeDb.flushdb();
    }

    /**
     * Select the database to use.
     */
    public boolean select(int db) {
        checkOpen();
        if (mode == Mode.SERVER) {
            return withJedis(j -> "OK".equals(j.select(db)));
        }
        return nativeDb.select(db);
    }

    // =========================================================================
    // Hash Commands
    // =========================================================================

    /**
     * Set a single hash field.
     */
    public long hset(String key, String field, byte[] value) {
        checkOpen();
        if (mode == Mode.SERVER) {
            return withJedis(j ->
                    j.hset(key.getBytes(StandardCharsets.UTF_8),
                            field.getBytes(StandardCharsets.UTF_8), value));
        }
        return nativeDb.hset(key, field, value);
    }

    /**
     * Set multiple hash fields.
     */
    public long hset(String key, Map<String, byte[]> mapping) {
        checkOpen();
        if (mapping.isEmpty()) return 0;
        if (mode == Mode.SERVER) {
            return withJedis(j -> {
                Map<byte[], byte[]> byteMap = new LinkedHashMap<>();
                for (var entry : mapping.entrySet()) {
                    byteMap.put(entry.getKey().getBytes(StandardCharsets.UTF_8), entry.getValue());
                }
                return j.hset(key.getBytes(StandardCharsets.UTF_8), byteMap);
            });
        }
        return nativeDb.hset(key, mapping);
    }

    /**
     * Get a hash field value.
     */
    public @Nullable byte[] hget(String key, String field) {
        checkOpen();
        if (mode == Mode.SERVER) {
            return withJedis(j ->
                    j.hget(key.getBytes(StandardCharsets.UTF_8),
                            field.getBytes(StandardCharsets.UTF_8)));
        }
        return nativeDb.hget(key, field);
    }

    /**
     * Delete hash fields.
     */
    public long hdel(String key, String... fields) {
        checkOpen();
        if (fields.length == 0) return 0;
        if (mode == Mode.SERVER) {
            return withJedis(j -> {
                byte[][] byteFields = Arrays.stream(fields)
                        .map(f -> f.getBytes(StandardCharsets.UTF_8))
                        .toArray(byte[][]::new);
                return j.hdel(key.getBytes(StandardCharsets.UTF_8), byteFields);
            });
        }
        return nativeDb.hdel(key, fields);
    }

    /**
     * Check if a hash field exists.
     */
    public boolean hexists(String key, String field) {
        checkOpen();
        if (mode == Mode.SERVER) {
            return withJedis(j ->
                    j.hexists(key.getBytes(StandardCharsets.UTF_8),
                            field.getBytes(StandardCharsets.UTF_8)));
        }
        return nativeDb.hexists(key, field);
    }

    /**
     * Get the number of fields in a hash.
     */
    public long hlen(String key) {
        checkOpen();
        if (mode == Mode.SERVER) {
            return withJedis(j -> j.hlen(key.getBytes(StandardCharsets.UTF_8)));
        }
        return nativeDb.hlen(key);
    }

    /**
     * Get all field names in a hash.
     */
    public List<String> hkeys(String key) {
        checkOpen();
        if (mode == Mode.SERVER) {
            return withJedis(j -> {
                Set<byte[]> keys = j.hkeys(key.getBytes(StandardCharsets.UTF_8));
                List<String> result = new ArrayList<>();
                for (byte[] k : keys) {
                    result.add(new String(k, StandardCharsets.UTF_8));
                }
                return result;
            });
        }
        return nativeDb.hkeys(key);
    }

    /**
     * Get all values in a hash.
     */
    public List<byte[]> hvals(String key) {
        checkOpen();
        if (mode == Mode.SERVER) {
            return withJedis(j -> j.hvals(key.getBytes(StandardCharsets.UTF_8)));
        }
        return nativeDb.hvals(key);
    }

    /**
     * Increment a hash field by amount.
     */
    public long hincrby(String key, String field, long amount) {
        checkOpen();
        if (mode == Mode.SERVER) {
            return withJedis(j ->
                    j.hincrBy(key.getBytes(StandardCharsets.UTF_8),
                            field.getBytes(StandardCharsets.UTF_8), amount));
        }
        return nativeDb.hincrby(key, field, amount);
    }

    /**
     * Increment a hash field by float amount.
     */
    public double hincrbyfloat(String key, String field, double amount) {
        checkOpen();
        if (mode == Mode.SERVER) {
            return withJedis(j ->
                    j.hincrByFloat(key.getBytes(StandardCharsets.UTF_8),
                            field.getBytes(StandardCharsets.UTF_8), amount));
        }
        return nativeDb.hincrbyfloat(key, field, amount);
    }

    /**
     * Get all fields and values in a hash.
     */
    public Map<String, byte[]> hgetall(String key) {
        checkOpen();
        if (mode == Mode.SERVER) {
            return withJedis(j -> {
                Map<byte[], byte[]> byteMap = j.hgetAll(key.getBytes(StandardCharsets.UTF_8));
                Map<String, byte[]> result = new LinkedHashMap<>();
                for (var entry : byteMap.entrySet()) {
                    result.put(new String(entry.getKey(), StandardCharsets.UTF_8), entry.getValue());
                }
                return result;
            });
        }
        return nativeDb.hgetall(key);
    }

    /**
     * Get values of multiple hash fields.
     */
    public List<byte[]> hmget(String key, String... fields) {
        checkOpen();
        if (fields.length == 0) return Collections.emptyList();
        if (mode == Mode.SERVER) {
            return withJedis(j -> {
                byte[][] byteFields = Arrays.stream(fields)
                        .map(f -> f.getBytes(StandardCharsets.UTF_8))
                        .toArray(byte[][]::new);
                return j.hmget(key.getBytes(StandardCharsets.UTF_8), byteFields);
            });
        }
        return nativeDb.hmget(key, fields);
    }

    /**
     * Set a hash field only if it doesn't exist.
     */
    public boolean hsetnx(String key, String field, byte[] value) {
        checkOpen();
        if (mode == Mode.SERVER) {
            return withJedis(j ->
                    j.hsetnx(key.getBytes(StandardCharsets.UTF_8),
                            field.getBytes(StandardCharsets.UTF_8), value) == 1L);
        }
        return nativeDb.hsetnx(key, field, value);
    }

    // =========================================================================
    // List Commands
    // =========================================================================

    /**
     * Push values to the head of a list.
     */
    public long lpush(String key, byte[]... values) {
        checkOpen();
        if (values.length == 0) return 0;
        if (mode == Mode.SERVER) {
            return withJedis(j -> j.lpush(key.getBytes(StandardCharsets.UTF_8), values));
        }
        return nativeDb.lpush(key, values);
    }

    /**
     * Push values to the tail of a list.
     */
    public long rpush(String key, byte[]... values) {
        checkOpen();
        if (values.length == 0) return 0;
        if (mode == Mode.SERVER) {
            return withJedis(j -> j.rpush(key.getBytes(StandardCharsets.UTF_8), values));
        }
        return nativeDb.rpush(key, values);
    }

    /**
     * Pop values from the head of a list.
     */
    public List<byte[]> lpop(String key, int count) {
        checkOpen();
        if (mode == Mode.SERVER) {
            return withJedis(j -> {
                if (count == 1) {
                    byte[] result = j.lpop(key.getBytes(StandardCharsets.UTF_8));
                    return result != null ? List.of(result) : Collections.emptyList();
                }
                List<byte[]> result = j.lpop(key.getBytes(StandardCharsets.UTF_8), count);
                return result != null ? result : Collections.emptyList();
            });
        }
        return nativeDb.lpop(key, count);
    }

    /**
     * Pop a single value from the head of a list.
     */
    public @Nullable byte[] lpop(String key) {
        List<byte[]> result = lpop(key, 1);
        return result.isEmpty() ? null : result.get(0);
    }

    /**
     * Pop values from the tail of a list.
     */
    public List<byte[]> rpop(String key, int count) {
        checkOpen();
        if (mode == Mode.SERVER) {
            return withJedis(j -> {
                if (count == 1) {
                    byte[] result = j.rpop(key.getBytes(StandardCharsets.UTF_8));
                    return result != null ? List.of(result) : Collections.emptyList();
                }
                List<byte[]> result = j.rpop(key.getBytes(StandardCharsets.UTF_8), count);
                return result != null ? result : Collections.emptyList();
            });
        }
        return nativeDb.rpop(key, count);
    }

    /**
     * Pop a single value from the tail of a list.
     */
    public @Nullable byte[] rpop(String key) {
        List<byte[]> result = rpop(key, 1);
        return result.isEmpty() ? null : result.get(0);
    }

    /**
     * Get the length of a list.
     */
    public long llen(String key) {
        checkOpen();
        if (mode == Mode.SERVER) {
            return withJedis(j -> j.llen(key.getBytes(StandardCharsets.UTF_8)));
        }
        return nativeDb.llen(key);
    }

    /**
     * Get a range of elements from a list.
     */
    public List<byte[]> lrange(String key, long start, long stop) {
        checkOpen();
        if (mode == Mode.SERVER) {
            return withJedis(j -> j.lrange(key.getBytes(StandardCharsets.UTF_8), start, stop));
        }
        return nativeDb.lrange(key, start, stop);
    }

    /**
     * Get an element from a list by index.
     */
    public @Nullable byte[] lindex(String key, long index) {
        checkOpen();
        if (mode == Mode.SERVER) {
            return withJedis(j -> j.lindex(key.getBytes(StandardCharsets.UTF_8), index));
        }
        return nativeDb.lindex(key, index);
    }

    // =========================================================================
    // Set Commands
    // =========================================================================

    /**
     * Add members to a set.
     */
    public long sadd(String key, byte[]... members) {
        checkOpen();
        if (members.length == 0) return 0;
        if (mode == Mode.SERVER) {
            return withJedis(j -> j.sadd(key.getBytes(StandardCharsets.UTF_8), members));
        }
        return nativeDb.sadd(key, members);
    }

    /**
     * Remove members from a set.
     */
    public long srem(String key, byte[]... members) {
        checkOpen();
        if (members.length == 0) return 0;
        if (mode == Mode.SERVER) {
            return withJedis(j -> j.srem(key.getBytes(StandardCharsets.UTF_8), members));
        }
        return nativeDb.srem(key, members);
    }

    /**
     * Get all members of a set.
     */
    public Set<byte[]> smembers(String key) {
        checkOpen();
        if (mode == Mode.SERVER) {
            return withJedis(j -> j.smembers(key.getBytes(StandardCharsets.UTF_8)));
        }
        return nativeDb.smembers(key);
    }

    /**
     * Check if a value is a member of a set.
     */
    public boolean sismember(String key, byte[] member) {
        checkOpen();
        if (mode == Mode.SERVER) {
            return withJedis(j -> j.sismember(key.getBytes(StandardCharsets.UTF_8), member));
        }
        return nativeDb.sismember(key, member);
    }

    /**
     * Get the number of members in a set.
     */
    public long scard(String key) {
        checkOpen();
        if (mode == Mode.SERVER) {
            return withJedis(j -> j.scard(key.getBytes(StandardCharsets.UTF_8)));
        }
        return nativeDb.scard(key);
    }

    // =========================================================================
    // Sorted Set Commands
    // =========================================================================

    /**
     * Add members to a sorted set.
     */
    public long zadd(String key, ZMember... members) {
        checkOpen();
        if (members.length == 0) return 0;
        if (mode == Mode.SERVER) {
            return withJedis(j -> {
                Map<byte[], Double> scoreMembers = new LinkedHashMap<>();
                for (ZMember m : members) {
                    scoreMembers.put(m.member(), m.score());
                }
                return j.zadd(key.getBytes(StandardCharsets.UTF_8), scoreMembers);
            });
        }
        return nativeDb.zadd(key, members);
    }

    /**
     * Add members to a sorted set from a map.
     */
    public long zadd(String key, Map<byte[], Double> mapping) {
        checkOpen();
        if (mapping.isEmpty()) return 0;
        if (mode == Mode.SERVER) {
            return withJedis(j -> j.zadd(key.getBytes(StandardCharsets.UTF_8), mapping));
        }
        return nativeDb.zadd(key, mapping);
    }

    /**
     * Remove members from a sorted set.
     */
    public long zrem(String key, byte[]... members) {
        checkOpen();
        if (members.length == 0) return 0;
        if (mode == Mode.SERVER) {
            return withJedis(j -> j.zrem(key.getBytes(StandardCharsets.UTF_8), members));
        }
        return nativeDb.zrem(key, members);
    }

    /**
     * Get the score of a member in a sorted set.
     */
    public @Nullable Double zscore(String key, byte[] member) {
        checkOpen();
        if (mode == Mode.SERVER) {
            return withJedis(j -> j.zscore(key.getBytes(StandardCharsets.UTF_8), member));
        }
        return nativeDb.zscore(key, member);
    }

    /**
     * Get the number of members in a sorted set.
     */
    public long zcard(String key) {
        checkOpen();
        if (mode == Mode.SERVER) {
            return withJedis(j -> j.zcard(key.getBytes(StandardCharsets.UTF_8)));
        }
        return nativeDb.zcard(key);
    }

    /**
     * Count members with scores in the given range.
     */
    public long zcount(String key, double min, double max) {
        checkOpen();
        if (mode == Mode.SERVER) {
            return withJedis(j -> j.zcount(key.getBytes(StandardCharsets.UTF_8), min, max));
        }
        return nativeDb.zcount(key, min, max);
    }

    /**
     * Increment the score of a member in a sorted set.
     */
    public double zincrby(String key, double amount, byte[] member) {
        checkOpen();
        if (mode == Mode.SERVER) {
            return withJedis(j -> j.zincrby(key.getBytes(StandardCharsets.UTF_8), amount, member));
        }
        return nativeDb.zincrby(key, amount, member);
    }

    /**
     * Get members by rank range (ascending order).
     */
    public List<byte[]> zrange(String key, long start, long stop) {
        checkOpen();
        if (mode == Mode.SERVER) {
            return withJedis(j -> j.zrange(key.getBytes(StandardCharsets.UTF_8), start, stop));
        }
        return nativeDb.zrange(key, start, stop);
    }

    /**
     * Get members by rank range with scores (ascending order).
     */
    public List<ZMember> zrangeWithScores(String key, long start, long stop) {
        checkOpen();
        if (mode == Mode.SERVER) {
            return withJedis(j -> {
                List<redis.clients.jedis.resps.Tuple> tuples =
                        j.zrangeWithScores(key.getBytes(StandardCharsets.UTF_8), start, stop);
                List<ZMember> result = new ArrayList<>();
                for (var t : tuples) {
                    result.add(new ZMember(t.getScore(), t.getBinaryElement()));
                }
                return result;
            });
        }
        return nativeDb.zrangeWithScores(key, start, stop);
    }

    /**
     * Get members by rank range (descending order).
     */
    public List<byte[]> zrevrange(String key, long start, long stop) {
        checkOpen();
        if (mode == Mode.SERVER) {
            return withJedis(j -> j.zrevrange(key.getBytes(StandardCharsets.UTF_8), start, stop));
        }
        return nativeDb.zrevrange(key, start, stop);
    }

    /**
     * Get members by rank range with scores (descending order).
     */
    public List<ZMember> zrevrangeWithScores(String key, long start, long stop) {
        checkOpen();
        if (mode == Mode.SERVER) {
            return withJedis(j -> {
                List<redis.clients.jedis.resps.Tuple> tuples =
                        j.zrevrangeWithScores(key.getBytes(StandardCharsets.UTF_8), start, stop);
                List<ZMember> result = new ArrayList<>();
                for (var t : tuples) {
                    result.add(new ZMember(t.getScore(), t.getBinaryElement()));
                }
                return result;
            });
        }
        return nativeDb.zrevrangeWithScores(key, start, stop);
    }

    // =========================================================================
    // Server Commands
    // =========================================================================

    /**
     * Compact the database, return bytes freed (embedded mode only).
     */
    public long vacuum() {
        checkOpen();
        if (mode == Mode.SERVER) {
            throw new RedliteException("VACUUM is only available in embedded mode");
        }
        return nativeDb.vacuum();
    }

    /**
     * Ping the server.
     */
    public String ping() {
        checkOpen();
        if (mode == Mode.SERVER) {
            return withJedis(Jedis::ping);
        }
        return nativeDb.ping();
    }

    /**
     * Get the redlite library version.
     */
    public static String version() {
        return "0.1.0";
    }

    // =========================================================================
    // KeyInfo Command
    // =========================================================================

    /**
     * KEYINFO - Get detailed information about a key.
     *
     * @param key Key to get info for
     * @return KeyInfo or null if key doesn't exist
     */
    public @Nullable KeyInfo keyinfo(String key) {
        checkOpen();
        // Both modes would need to execute KEYINFO command
        // For now, this is a placeholder that throws
        throw new RedliteException("KEYINFO not yet implemented");
    }
}
