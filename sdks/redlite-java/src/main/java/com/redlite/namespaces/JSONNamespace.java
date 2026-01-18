package com.redlite.namespaces;

import com.redlite.JsonSetOptions;
import com.redlite.Redlite;
import com.redlite.RedliteException;
import org.jetbrains.annotations.Nullable;

/**
 * JSON namespace for ReJSON-compatible JSON commands.
 */
public class JSONNamespace {
    private final Redlite client;

    public JSONNamespace(Redlite client) {
        this.client = client;
    }

    /**
     * JSON.SET key path value - Set a JSON value at the specified path.
     *
     * @param key   The key
     * @param path  JSON path (use "$" for root)
     * @param value JSON-encoded value
     * @return true if set successfully
     */
    public boolean set(String key, String path, String value) {
        return set(key, path, value, null);
    }

    /**
     * JSON.SET key path value [NX|XX] - Set a JSON value with options.
     *
     * @param key     The key
     * @param path    JSON path (use "$" for root)
     * @param value   JSON-encoded value
     * @param options NX/XX options
     * @return true if set, false if NX/XX condition not met
     */
    public boolean set(String key, String path, String value, @Nullable JsonSetOptions options) {
        // Server mode: would use JSON.SET command via Jedis custom command
        // Embedded mode: would use JNI
        throw new RedliteException("JSON commands not yet implemented");
    }

    /**
     * JSON.GET key [path ...] - Get JSON values at the specified paths.
     *
     * @param key   The key
     * @param paths JSON paths to get (defaults to "$")
     * @return JSON-encoded result or null if not found
     */
    public @Nullable String get(String key, String... paths) {
        throw new RedliteException("JSON commands not yet implemented");
    }

    /**
     * JSON.DEL key [path] - Delete JSON values at the specified path.
     *
     * @param key  The key
     * @param path JSON path (defaults to "$")
     * @return Number of paths deleted
     */
    public long del(String key, String path) {
        throw new RedliteException("JSON commands not yet implemented");
    }

    /**
     * JSON.DEL key - Delete the entire JSON document.
     */
    public long del(String key) {
        return del(key, "$");
    }

    /**
     * JSON.TYPE key [path] - Get the type of JSON value at the specified path.
     *
     * @param key  The key
     * @param path JSON path (defaults to "$")
     * @return Type name or null if not found
     */
    public @Nullable String type(String key, String path) {
        throw new RedliteException("JSON commands not yet implemented");
    }

    /**
     * JSON.TYPE key - Get the type at root.
     */
    public @Nullable String type(String key) {
        return type(key, "$");
    }

    /**
     * JSON.NUMINCRBY key path increment - Increment a JSON number.
     *
     * @param key       The key
     * @param path      JSON path
     * @param increment Amount to increment
     * @return New value as JSON string
     */
    public @Nullable String numIncrBy(String key, String path, double increment) {
        throw new RedliteException("JSON commands not yet implemented");
    }

    /**
     * JSON.STRAPPEND key path value - Append to a JSON string.
     *
     * @param key   The key
     * @param path  JSON path
     * @param value String to append (JSON-encoded)
     * @return New length of string
     */
    public long strAppend(String key, String path, String value) {
        throw new RedliteException("JSON commands not yet implemented");
    }

    /**
     * JSON.STRLEN key [path] - Get the length of a JSON string.
     *
     * @param key  The key
     * @param path JSON path (defaults to "$")
     * @return Length of string
     */
    public long strLen(String key, String path) {
        throw new RedliteException("JSON commands not yet implemented");
    }

    /**
     * JSON.STRLEN key - Get the length of the string at root.
     */
    public long strLen(String key) {
        return strLen(key, "$");
    }

    /**
     * JSON.ARRAPPEND key path value [value ...] - Append to a JSON array.
     *
     * @param key    The key
     * @param path   JSON path
     * @param values JSON-encoded values to append
     * @return New length of array
     */
    public long arrAppend(String key, String path, String... values) {
        throw new RedliteException("JSON commands not yet implemented");
    }

    /**
     * JSON.ARRLEN key [path] - Get the length of a JSON array.
     *
     * @param key  The key
     * @param path JSON path (defaults to "$")
     * @return Length of array
     */
    public long arrLen(String key, String path) {
        throw new RedliteException("JSON commands not yet implemented");
    }

    /**
     * JSON.ARRLEN key - Get the length of array at root.
     */
    public long arrLen(String key) {
        return arrLen(key, "$");
    }

    /**
     * JSON.ARRPOP key [path [index]] - Pop an element from a JSON array.
     *
     * @param key   The key
     * @param path  JSON path (defaults to "$")
     * @param index Index to pop from (defaults to -1, last element)
     * @return Popped value as JSON string
     */
    public @Nullable String arrPop(String key, String path, long index) {
        throw new RedliteException("JSON commands not yet implemented");
    }

    /**
     * JSON.ARRPOP key [path] - Pop the last element from a JSON array.
     */
    public @Nullable String arrPop(String key, String path) {
        return arrPop(key, path, -1);
    }

    /**
     * JSON.ARRPOP key - Pop the last element from array at root.
     */
    public @Nullable String arrPop(String key) {
        return arrPop(key, "$", -1);
    }

    /**
     * JSON.CLEAR key [path] - Clear JSON arrays or objects.
     *
     * @param key  The key
     * @param path JSON path (defaults to "$")
     * @return Number of values cleared
     */
    public long clear(String key, String path) {
        throw new RedliteException("JSON commands not yet implemented");
    }

    /**
     * JSON.CLEAR key - Clear the document at root.
     */
    public long clear(String key) {
        return clear(key, "$");
    }
}
