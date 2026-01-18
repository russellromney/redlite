using System.Runtime.InteropServices;
using System.Text;
using Redlite.Native;

namespace Redlite;

/// <summary>
/// Redlite database client providing Redis-compatible commands with SQLite durability.
/// Implements IDisposable for proper resource cleanup.
/// </summary>
public sealed class RedliteDb : IDisposable
{
    private IntPtr _db;
    private bool _disposed;

    private RedliteDb(IntPtr db)
    {
        _db = db;
    }

    /// <summary>
    /// Open a database at the given path.
    /// </summary>
    /// <param name="path">Path to the database file</param>
    /// <returns>A new RedliteDb instance</returns>
    /// <exception cref="RedliteException">If the database cannot be opened</exception>
    public static RedliteDb Open(string path)
    {
        var db = NativeMethods.redlite_open(path);
        if (db == IntPtr.Zero)
        {
            throw new RedliteException(GetLastError() ?? "Failed to open database");
        }
        return new RedliteDb(db);
    }

    /// <summary>
    /// Open an in-memory database.
    /// </summary>
    /// <returns>A new RedliteDb instance</returns>
    /// <exception cref="RedliteException">If the database cannot be opened</exception>
    public static RedliteDb OpenMemory()
    {
        var db = NativeMethods.redlite_open_memory();
        if (db == IntPtr.Zero)
        {
            throw new RedliteException(GetLastError() ?? "Failed to open in-memory database");
        }
        return new RedliteDb(db);
    }

    /// <summary>
    /// Open a database with custom cache size.
    /// </summary>
    /// <param name="path">Path to the database file</param>
    /// <param name="cacheMb">Cache size in megabytes</param>
    /// <returns>A new RedliteDb instance</returns>
    /// <exception cref="RedliteException">If the database cannot be opened</exception>
    public static RedliteDb OpenWithCache(string path, long cacheMb)
    {
        var db = NativeMethods.redlite_open_with_cache(path, cacheMb);
        if (db == IntPtr.Zero)
        {
            throw new RedliteException(GetLastError() ?? "Failed to open database with cache");
        }
        return new RedliteDb(db);
    }

    private static string? GetLastError()
    {
        var ptr = NativeMethods.redlite_last_error();
        if (ptr == IntPtr.Zero) return null;
        var error = Marshal.PtrToStringUTF8(ptr);
        NativeMethods.redlite_free_string(ptr);
        return error;
    }

    private void ThrowIfDisposed()
    {
        if (_disposed)
            throw new ObjectDisposedException(nameof(RedliteDb));
    }

    // ==================== String Commands ====================

    /// <summary>
    /// GET key - Get the value of a key.
    /// </summary>
    public byte[]? Get(string key)
    {
        ThrowIfDisposed();
        var result = NativeMethods.redlite_get(_db, key);
        if (result.IsNull) return null;
        var bytes = result.ToArray();
        NativeMethods.redlite_free_bytes(result);
        return bytes;
    }

    /// <summary>
    /// GET key - Get the value of a key as a string.
    /// </summary>
    public string? GetString(string key)
    {
        var bytes = Get(key);
        return bytes == null ? null : Encoding.UTF8.GetString(bytes);
    }

    /// <summary>
    /// SET key value [TTL] - Set the string value of a key.
    /// </summary>
    /// <param name="key">The key</param>
    /// <param name="value">The value as bytes</param>
    /// <param name="ttlSeconds">Optional TTL in seconds (0 = no expiry)</param>
    /// <returns>True on success</returns>
    public bool Set(string key, byte[] value, long ttlSeconds = 0)
    {
        ThrowIfDisposed();
        unsafe
        {
            fixed (byte* ptr = value)
            {
                return NativeMethods.redlite_set(_db, key, (IntPtr)ptr, (nuint)value.Length, ttlSeconds) == 0;
            }
        }
    }

    /// <summary>
    /// SET key value [TTL] - Set the string value of a key.
    /// </summary>
    public bool Set(string key, string value, long ttlSeconds = 0)
    {
        return Set(key, Encoding.UTF8.GetBytes(value), ttlSeconds);
    }

    /// <summary>
    /// SET key value with options.
    /// </summary>
    public bool Set(string key, byte[] value, SetOptions options)
    {
        long ttl = options.Ex ?? (options.Px.HasValue ? options.Px.Value / 1000 : 0);
        return Set(key, value, ttl);
    }

    /// <summary>
    /// SETEX key seconds value - Set key with expiration in seconds.
    /// </summary>
    public bool SetEx(string key, long seconds, byte[] value)
    {
        ThrowIfDisposed();
        unsafe
        {
            fixed (byte* ptr = value)
            {
                return NativeMethods.redlite_setex(_db, key, seconds, (IntPtr)ptr, (nuint)value.Length) == 0;
            }
        }
    }

    /// <summary>
    /// SETEX key seconds value - Set key with expiration in seconds.
    /// </summary>
    public bool SetEx(string key, long seconds, string value) =>
        SetEx(key, seconds, Encoding.UTF8.GetBytes(value));

    /// <summary>
    /// PSETEX key milliseconds value - Set key with expiration in milliseconds.
    /// </summary>
    public bool PSetEx(string key, long milliseconds, byte[] value)
    {
        ThrowIfDisposed();
        unsafe
        {
            fixed (byte* ptr = value)
            {
                return NativeMethods.redlite_psetex(_db, key, milliseconds, (IntPtr)ptr, (nuint)value.Length) == 0;
            }
        }
    }

    /// <summary>
    /// PSETEX key milliseconds value - Set key with expiration in milliseconds.
    /// </summary>
    public bool PSetEx(string key, long milliseconds, string value) =>
        PSetEx(key, milliseconds, Encoding.UTF8.GetBytes(value));

    /// <summary>
    /// GETDEL key - Get the value and delete the key.
    /// </summary>
    public byte[]? GetDel(string key)
    {
        ThrowIfDisposed();
        var result = NativeMethods.redlite_getdel(_db, key);
        if (result.IsNull) return null;
        var bytes = result.ToArray();
        NativeMethods.redlite_free_bytes(result);
        return bytes;
    }

    /// <summary>
    /// GETDEL key - Get the value as string and delete the key.
    /// </summary>
    public string? GetDelString(string key)
    {
        var bytes = GetDel(key);
        return bytes == null ? null : Encoding.UTF8.GetString(bytes);
    }

    /// <summary>
    /// APPEND key value - Append a value to a key.
    /// </summary>
    /// <returns>The length of the string after the append operation</returns>
    public long Append(string key, byte[] value)
    {
        ThrowIfDisposed();
        unsafe
        {
            fixed (byte* ptr = value)
            {
                return NativeMethods.redlite_append(_db, key, (IntPtr)ptr, (nuint)value.Length);
            }
        }
    }

    /// <summary>
    /// APPEND key value - Append a string value to a key.
    /// </summary>
    public long Append(string key, string value) =>
        Append(key, Encoding.UTF8.GetBytes(value));

    /// <summary>
    /// STRLEN key - Get the length of the value stored at a key.
    /// </summary>
    public long StrLen(string key)
    {
        ThrowIfDisposed();
        return NativeMethods.redlite_strlen(_db, key);
    }

    /// <summary>
    /// GETRANGE key start end - Get a substring of the string stored at a key.
    /// </summary>
    public string GetRange(string key, long start, long end)
    {
        ThrowIfDisposed();
        var result = NativeMethods.redlite_getrange(_db, key, start, end);
        var str = result.ToStringUtf8() ?? "";
        NativeMethods.redlite_free_bytes(result);
        return str;
    }

    /// <summary>
    /// SETRANGE key offset value - Overwrite part of a string at key starting at the specified offset.
    /// </summary>
    /// <returns>The length of the string after it was modified</returns>
    public long SetRange(string key, long offset, string value)
    {
        ThrowIfDisposed();
        var bytes = Encoding.UTF8.GetBytes(value);
        unsafe
        {
            fixed (byte* ptr = bytes)
            {
                return NativeMethods.redlite_setrange(_db, key, offset, (IntPtr)ptr, (nuint)bytes.Length);
            }
        }
    }

    /// <summary>
    /// INCR key - Increment the integer value of a key by one.
    /// </summary>
    public long Incr(string key)
    {
        ThrowIfDisposed();
        return NativeMethods.redlite_incr(_db, key);
    }

    /// <summary>
    /// DECR key - Decrement the integer value of a key by one.
    /// </summary>
    public long Decr(string key)
    {
        ThrowIfDisposed();
        return NativeMethods.redlite_decr(_db, key);
    }

    /// <summary>
    /// INCRBY key increment - Increment the integer value of a key by the given amount.
    /// </summary>
    public long IncrBy(string key, long increment)
    {
        ThrowIfDisposed();
        return NativeMethods.redlite_incrby(_db, key, increment);
    }

    /// <summary>
    /// DECRBY key decrement - Decrement the integer value of a key by the given amount.
    /// </summary>
    public long DecrBy(string key, long decrement)
    {
        ThrowIfDisposed();
        return NativeMethods.redlite_decrby(_db, key, decrement);
    }

    /// <summary>
    /// INCRBYFLOAT key increment - Increment the float value of a key by the given amount.
    /// </summary>
    public double IncrByFloat(string key, double increment)
    {
        ThrowIfDisposed();
        var ptr = NativeMethods.redlite_incrbyfloat(_db, key, increment);
        if (ptr == IntPtr.Zero)
        {
            throw new RedliteException(GetLastError() ?? "INCRBYFLOAT failed");
        }
        var result = Marshal.PtrToStringUTF8(ptr)!;
        NativeMethods.redlite_free_string(ptr);
        return double.Parse(result);
    }

    /// <summary>
    /// MGET key [key ...] - Get the values of all specified keys.
    /// </summary>
    public string?[] MGet(params string[] keys)
    {
        ThrowIfDisposed();
        var keyPtrs = AllocateStringArray(keys);
        try
        {
            var result = NativeMethods.redlite_mget(_db, keyPtrs, (nuint)keys.Length);
            var values = result.ToStringArray();
            NativeMethods.redlite_free_bytes_array(result);
            return values;
        }
        finally
        {
            FreeStringArray(keyPtrs, keys.Length);
        }
    }

    /// <summary>
    /// MSET key value [key value ...] - Set multiple keys to multiple values.
    /// </summary>
    public bool MSet(params (string Key, string Value)[] pairs)
    {
        ThrowIfDisposed();
        var kvs = new RedliteKV[pairs.Length];
        var handles = new List<GCHandle>();

        try
        {
            for (int i = 0; i < pairs.Length; i++)
            {
                var keyBytes = Encoding.UTF8.GetBytes(pairs[i].Key + '\0');
                var valBytes = Encoding.UTF8.GetBytes(pairs[i].Value);
                var keyHandle = GCHandle.Alloc(keyBytes, GCHandleType.Pinned);
                var valHandle = GCHandle.Alloc(valBytes, GCHandleType.Pinned);
                handles.Add(keyHandle);
                handles.Add(valHandle);

                kvs[i] = new RedliteKV
                {
                    Key = keyHandle.AddrOfPinnedObject(),
                    Value = valHandle.AddrOfPinnedObject(),
                    ValueLen = (nuint)valBytes.Length
                };
            }

            unsafe
            {
                fixed (RedliteKV* ptr = kvs)
                {
                    return NativeMethods.redlite_mset(_db, (IntPtr)ptr, (nuint)pairs.Length) == 0;
                }
            }
        }
        finally
        {
            foreach (var handle in handles)
            {
                handle.Free();
            }
        }
    }

    /// <summary>
    /// MSET key value [key value ...] - Set multiple keys to multiple values using a dictionary.
    /// </summary>
    public bool MSet(Dictionary<string, string> pairs)
    {
        var arr = pairs.Select(p => (p.Key, p.Value)).ToArray();
        return MSet(arr);
    }

    // ==================== Key Commands ====================

    /// <summary>
    /// DEL key [key ...] - Delete one or more keys.
    /// </summary>
    /// <returns>The number of keys removed</returns>
    public long Del(params string[] keys)
    {
        ThrowIfDisposed();
        var keyPtrs = AllocateStringArray(keys);
        try
        {
            return NativeMethods.redlite_del(_db, keyPtrs, (nuint)keys.Length);
        }
        finally
        {
            FreeStringArray(keyPtrs, keys.Length);
        }
    }

    /// <summary>
    /// EXISTS key [key ...] - Determine if keys exist.
    /// </summary>
    /// <returns>The number of keys that exist</returns>
    public long Exists(params string[] keys)
    {
        ThrowIfDisposed();
        var keyPtrs = AllocateStringArray(keys);
        try
        {
            return NativeMethods.redlite_exists(_db, keyPtrs, (nuint)keys.Length);
        }
        finally
        {
            FreeStringArray(keyPtrs, keys.Length);
        }
    }

    /// <summary>
    /// TYPE key - Get the type of a key.
    /// </summary>
    /// <returns>The type ("string", "hash", "list", "set", "zset") or "none" if key doesn't exist</returns>
    public string Type(string key)
    {
        ThrowIfDisposed();
        var ptr = NativeMethods.redlite_type(_db, key);
        if (ptr == IntPtr.Zero) return "none";
        var result = Marshal.PtrToStringUTF8(ptr) ?? "none";
        NativeMethods.redlite_free_string(ptr);
        return result;
    }

    /// <summary>
    /// TTL key - Get the time to live for a key in seconds.
    /// </summary>
    /// <returns>-2 if key doesn't exist, -1 if no TTL, otherwise seconds until expiry</returns>
    public long Ttl(string key)
    {
        ThrowIfDisposed();
        return NativeMethods.redlite_ttl(_db, key);
    }

    /// <summary>
    /// PTTL key - Get the time to live for a key in milliseconds.
    /// </summary>
    public long PTtl(string key)
    {
        ThrowIfDisposed();
        return NativeMethods.redlite_pttl(_db, key);
    }

    /// <summary>
    /// EXPIRE key seconds - Set a timeout on a key in seconds.
    /// </summary>
    /// <returns>True if the timeout was set, false if key doesn't exist</returns>
    public bool Expire(string key, long seconds)
    {
        ThrowIfDisposed();
        return NativeMethods.redlite_expire(_db, key, seconds) == 1;
    }

    /// <summary>
    /// PEXPIRE key milliseconds - Set a timeout on a key in milliseconds.
    /// </summary>
    public bool PExpire(string key, long milliseconds)
    {
        ThrowIfDisposed();
        return NativeMethods.redlite_pexpire(_db, key, milliseconds) == 1;
    }

    /// <summary>
    /// EXPIREAT key timestamp - Set an absolute expiration time (Unix timestamp).
    /// </summary>
    public bool ExpireAt(string key, long unixSeconds)
    {
        ThrowIfDisposed();
        return NativeMethods.redlite_expireat(_db, key, unixSeconds) == 1;
    }

    /// <summary>
    /// PEXPIREAT key timestamp - Set an absolute expiration time in milliseconds.
    /// </summary>
    public bool PExpireAt(string key, long unixMs)
    {
        ThrowIfDisposed();
        return NativeMethods.redlite_pexpireat(_db, key, unixMs) == 1;
    }

    /// <summary>
    /// PERSIST key - Remove the expiration from a key.
    /// </summary>
    public bool Persist(string key)
    {
        ThrowIfDisposed();
        return NativeMethods.redlite_persist(_db, key) == 1;
    }

    /// <summary>
    /// RENAME key newkey - Rename a key.
    /// </summary>
    public bool Rename(string key, string newKey)
    {
        ThrowIfDisposed();
        return NativeMethods.redlite_rename(_db, key, newKey) == 0;
    }

    /// <summary>
    /// RENAMENX key newkey - Rename a key only if the new key does not exist.
    /// </summary>
    /// <returns>True if renamed, false if newkey already exists</returns>
    public bool RenameNx(string key, string newKey)
    {
        ThrowIfDisposed();
        return NativeMethods.redlite_renamenx(_db, key, newKey) == 1;
    }

    /// <summary>
    /// KEYS pattern - Find all keys matching the given pattern.
    /// </summary>
    public string[] Keys(string pattern = "*")
    {
        ThrowIfDisposed();
        var result = NativeMethods.redlite_keys(_db, pattern);
        var keys = result.ToArray();
        NativeMethods.redlite_free_string_array(result);
        return keys;
    }

    /// <summary>
    /// DBSIZE - Return the number of keys in the database.
    /// </summary>
    public long DbSize()
    {
        ThrowIfDisposed();
        return NativeMethods.redlite_dbsize(_db);
    }

    /// <summary>
    /// FLUSHDB - Remove all keys from the current database.
    /// </summary>
    public bool FlushDb()
    {
        ThrowIfDisposed();
        return NativeMethods.redlite_flushdb(_db) == 0;
    }

    /// <summary>
    /// SELECT db - Select the logical database.
    /// </summary>
    public bool Select(int dbNum)
    {
        ThrowIfDisposed();
        return NativeMethods.redlite_select(_db, dbNum) == 0;
    }

    // ==================== Hash Commands ====================

    /// <summary>
    /// HSET key field value - Set the string value of a hash field.
    /// </summary>
    /// <returns>Number of fields added (1 if new, 0 if updated)</returns>
    public long HSet(string key, string field, string value)
    {
        ThrowIfDisposed();
        return HSet(key, new Dictionary<string, string> { { field, value } });
    }

    /// <summary>
    /// HSET key field value [field value ...] - Set multiple hash fields.
    /// </summary>
    public long HSet(string key, Dictionary<string, string> fields)
    {
        ThrowIfDisposed();
        var fieldPtrs = AllocateStringArray(fields.Keys.ToArray());
        var values = new RedliteBytes[fields.Count];
        var handles = new List<GCHandle>();

        try
        {
            int i = 0;
            foreach (var val in fields.Values)
            {
                var bytes = Encoding.UTF8.GetBytes(val);
                var handle = GCHandle.Alloc(bytes, GCHandleType.Pinned);
                handles.Add(handle);
                values[i] = new RedliteBytes
                {
                    Data = handle.AddrOfPinnedObject(),
                    Len = (nuint)bytes.Length
                };
                i++;
            }

            unsafe
            {
                fixed (RedliteBytes* valPtr = values)
                {
                    return NativeMethods.redlite_hset(_db, key, fieldPtrs, (IntPtr)valPtr, (nuint)fields.Count);
                }
            }
        }
        finally
        {
            FreeStringArray(fieldPtrs, fields.Count);
            foreach (var handle in handles)
            {
                handle.Free();
            }
        }
    }

    /// <summary>
    /// HGET key field - Get the value of a hash field.
    /// </summary>
    public string? HGet(string key, string field)
    {
        ThrowIfDisposed();
        var result = NativeMethods.redlite_hget(_db, key, field);
        if (result.IsNull) return null;
        var str = result.ToStringUtf8();
        NativeMethods.redlite_free_bytes(result);
        return str;
    }

    /// <summary>
    /// HDEL key field [field ...] - Delete one or more hash fields.
    /// </summary>
    public long HDel(string key, params string[] fields)
    {
        ThrowIfDisposed();
        var fieldPtrs = AllocateStringArray(fields);
        try
        {
            return NativeMethods.redlite_hdel(_db, key, fieldPtrs, (nuint)fields.Length);
        }
        finally
        {
            FreeStringArray(fieldPtrs, fields.Length);
        }
    }

    /// <summary>
    /// HEXISTS key field - Determine if a hash field exists.
    /// </summary>
    public bool HExists(string key, string field)
    {
        ThrowIfDisposed();
        return NativeMethods.redlite_hexists(_db, key, field) == 1;
    }

    /// <summary>
    /// HLEN key - Get the number of fields in a hash.
    /// </summary>
    public long HLen(string key)
    {
        ThrowIfDisposed();
        return NativeMethods.redlite_hlen(_db, key);
    }

    /// <summary>
    /// HKEYS key - Get all field names in a hash.
    /// </summary>
    public string[] HKeys(string key)
    {
        ThrowIfDisposed();
        var result = NativeMethods.redlite_hkeys(_db, key);
        var keys = result.ToArray();
        NativeMethods.redlite_free_string_array(result);
        return keys;
    }

    /// <summary>
    /// HVALS key - Get all values in a hash.
    /// </summary>
    public string[] HVals(string key)
    {
        ThrowIfDisposed();
        var result = NativeMethods.redlite_hvals(_db, key);
        var vals = result.ToStringArray().Select(v => v ?? "").ToArray();
        NativeMethods.redlite_free_bytes_array(result);
        return vals;
    }

    /// <summary>
    /// HINCRBY key field increment - Increment the integer value of a hash field.
    /// </summary>
    public long HIncrBy(string key, string field, long increment)
    {
        ThrowIfDisposed();
        return NativeMethods.redlite_hincrby(_db, key, field, increment);
    }

    /// <summary>
    /// HGETALL key - Get all fields and values in a hash.
    /// </summary>
    public Dictionary<string, string> HGetAll(string key)
    {
        ThrowIfDisposed();
        var result = NativeMethods.redlite_hgetall(_db, key);
        var arr = result.ToStringArray();
        NativeMethods.redlite_free_bytes_array(result);

        var dict = new Dictionary<string, string>();
        for (int i = 0; i + 1 < arr.Length; i += 2)
        {
            dict[arr[i] ?? ""] = arr[i + 1] ?? "";
        }
        return dict;
    }

    /// <summary>
    /// HMGET key field [field ...] - Get the values of multiple hash fields.
    /// </summary>
    public string?[] HMGet(string key, params string[] fields)
    {
        ThrowIfDisposed();
        var fieldPtrs = AllocateStringArray(fields);
        try
        {
            var result = NativeMethods.redlite_hmget(_db, key, fieldPtrs, (nuint)fields.Length);
            var values = result.ToStringArray();
            NativeMethods.redlite_free_bytes_array(result);
            return values;
        }
        finally
        {
            FreeStringArray(fieldPtrs, fields.Length);
        }
    }

    // ==================== List Commands ====================

    /// <summary>
    /// LPUSH key value [value ...] - Insert values at the head of the list.
    /// </summary>
    /// <returns>The length of the list after the push operation</returns>
    public long LPush(string key, params string[] values)
    {
        ThrowIfDisposed();
        return PushInternal(key, values, NativeMethods.redlite_lpush);
    }

    /// <summary>
    /// RPUSH key value [value ...] - Insert values at the tail of the list.
    /// </summary>
    public long RPush(string key, params string[] values)
    {
        ThrowIfDisposed();
        return PushInternal(key, values, NativeMethods.redlite_rpush);
    }

    private long PushInternal(string key, string[] values, Func<IntPtr, string, IntPtr, nuint, long> pushFunc)
    {
        var bytesArr = new RedliteBytes[values.Length];
        var handles = new List<GCHandle>();

        try
        {
            for (int i = 0; i < values.Length; i++)
            {
                var bytes = Encoding.UTF8.GetBytes(values[i]);
                var handle = GCHandle.Alloc(bytes, GCHandleType.Pinned);
                handles.Add(handle);
                bytesArr[i] = new RedliteBytes
                {
                    Data = handle.AddrOfPinnedObject(),
                    Len = (nuint)bytes.Length
                };
            }

            unsafe
            {
                fixed (RedliteBytes* ptr = bytesArr)
                {
                    return pushFunc(_db, key, (IntPtr)ptr, (nuint)values.Length);
                }
            }
        }
        finally
        {
            foreach (var handle in handles)
            {
                handle.Free();
            }
        }
    }

    /// <summary>
    /// LPOP key - Remove and return the first element of the list.
    /// </summary>
    public string? LPop(string key)
    {
        ThrowIfDisposed();
        var result = NativeMethods.redlite_lpop(_db, key, 1);
        var arr = result.ToStringArray();
        NativeMethods.redlite_free_bytes_array(result);
        return arr.Length > 0 ? arr[0] : null;
    }

    /// <summary>
    /// LPOP key count - Remove and return multiple elements from the head.
    /// </summary>
    public string[] LPop(string key, int count)
    {
        ThrowIfDisposed();
        var result = NativeMethods.redlite_lpop(_db, key, (nuint)count);
        var arr = result.ToStringArray().Select(s => s ?? "").ToArray();
        NativeMethods.redlite_free_bytes_array(result);
        return arr;
    }

    /// <summary>
    /// RPOP key - Remove and return the last element of the list.
    /// </summary>
    public string? RPop(string key)
    {
        ThrowIfDisposed();
        var result = NativeMethods.redlite_rpop(_db, key, 1);
        var arr = result.ToStringArray();
        NativeMethods.redlite_free_bytes_array(result);
        return arr.Length > 0 ? arr[0] : null;
    }

    /// <summary>
    /// RPOP key count - Remove and return multiple elements from the tail.
    /// </summary>
    public string[] RPop(string key, int count)
    {
        ThrowIfDisposed();
        var result = NativeMethods.redlite_rpop(_db, key, (nuint)count);
        var arr = result.ToStringArray().Select(s => s ?? "").ToArray();
        NativeMethods.redlite_free_bytes_array(result);
        return arr;
    }

    /// <summary>
    /// LLEN key - Get the length of a list.
    /// </summary>
    public long LLen(string key)
    {
        ThrowIfDisposed();
        return NativeMethods.redlite_llen(_db, key);
    }

    /// <summary>
    /// LRANGE key start stop - Get a range of elements from a list.
    /// </summary>
    public string[] LRange(string key, long start, long stop)
    {
        ThrowIfDisposed();
        var result = NativeMethods.redlite_lrange(_db, key, start, stop);
        var arr = result.ToStringArray().Select(s => s ?? "").ToArray();
        NativeMethods.redlite_free_bytes_array(result);
        return arr;
    }

    /// <summary>
    /// LINDEX key index - Get an element from a list by its index.
    /// </summary>
    public string? LIndex(string key, long index)
    {
        ThrowIfDisposed();
        var result = NativeMethods.redlite_lindex(_db, key, index);
        if (result.IsNull) return null;
        var str = result.ToStringUtf8();
        NativeMethods.redlite_free_bytes(result);
        return str;
    }

    // ==================== Set Commands ====================

    /// <summary>
    /// SADD key member [member ...] - Add members to a set.
    /// </summary>
    /// <returns>Number of elements added (not including already existing members)</returns>
    public long SAdd(string key, params string[] members)
    {
        ThrowIfDisposed();
        var bytesArr = new RedliteBytes[members.Length];
        var handles = new List<GCHandle>();

        try
        {
            for (int i = 0; i < members.Length; i++)
            {
                var bytes = Encoding.UTF8.GetBytes(members[i]);
                var handle = GCHandle.Alloc(bytes, GCHandleType.Pinned);
                handles.Add(handle);
                bytesArr[i] = new RedliteBytes
                {
                    Data = handle.AddrOfPinnedObject(),
                    Len = (nuint)bytes.Length
                };
            }

            unsafe
            {
                fixed (RedliteBytes* ptr = bytesArr)
                {
                    return NativeMethods.redlite_sadd(_db, key, (IntPtr)ptr, (nuint)members.Length);
                }
            }
        }
        finally
        {
            foreach (var handle in handles)
            {
                handle.Free();
            }
        }
    }

    /// <summary>
    /// SREM key member [member ...] - Remove members from a set.
    /// </summary>
    public long SRem(string key, params string[] members)
    {
        ThrowIfDisposed();
        var bytesArr = new RedliteBytes[members.Length];
        var handles = new List<GCHandle>();

        try
        {
            for (int i = 0; i < members.Length; i++)
            {
                var bytes = Encoding.UTF8.GetBytes(members[i]);
                var handle = GCHandle.Alloc(bytes, GCHandleType.Pinned);
                handles.Add(handle);
                bytesArr[i] = new RedliteBytes
                {
                    Data = handle.AddrOfPinnedObject(),
                    Len = (nuint)bytes.Length
                };
            }

            unsafe
            {
                fixed (RedliteBytes* ptr = bytesArr)
                {
                    return NativeMethods.redlite_srem(_db, key, (IntPtr)ptr, (nuint)members.Length);
                }
            }
        }
        finally
        {
            foreach (var handle in handles)
            {
                handle.Free();
            }
        }
    }

    /// <summary>
    /// SMEMBERS key - Get all members of a set.
    /// </summary>
    public string[] SMembers(string key)
    {
        ThrowIfDisposed();
        var result = NativeMethods.redlite_smembers(_db, key);
        var arr = result.ToStringArray().Select(s => s ?? "").ToArray();
        NativeMethods.redlite_free_bytes_array(result);
        return arr;
    }

    /// <summary>
    /// SISMEMBER key member - Determine if a value is a member of a set.
    /// </summary>
    public bool SIsMember(string key, string member)
    {
        ThrowIfDisposed();
        var bytes = Encoding.UTF8.GetBytes(member);
        unsafe
        {
            fixed (byte* ptr = bytes)
            {
                return NativeMethods.redlite_sismember(_db, key, (IntPtr)ptr, (nuint)bytes.Length) == 1;
            }
        }
    }

    /// <summary>
    /// SCARD key - Get the number of members in a set.
    /// </summary>
    public long SCard(string key)
    {
        ThrowIfDisposed();
        return NativeMethods.redlite_scard(_db, key);
    }

    // ==================== Sorted Set Commands ====================

    /// <summary>
    /// ZADD key score member [score member ...] - Add members to a sorted set.
    /// </summary>
    /// <returns>Number of elements added</returns>
    public long ZAdd(string key, params ZMember[] members)
    {
        ThrowIfDisposed();
        var nativeMembers = new RedliteZMemberNative[members.Length];
        var handles = new List<GCHandle>();

        try
        {
            for (int i = 0; i < members.Length; i++)
            {
                var bytes = Encoding.UTF8.GetBytes(members[i].Member);
                var handle = GCHandle.Alloc(bytes, GCHandleType.Pinned);
                handles.Add(handle);
                nativeMembers[i] = new RedliteZMemberNative
                {
                    Score = members[i].Score,
                    Member = handle.AddrOfPinnedObject(),
                    MemberLen = (nuint)bytes.Length
                };
            }

            unsafe
            {
                fixed (RedliteZMemberNative* ptr = nativeMembers)
                {
                    return NativeMethods.redlite_zadd(_db, key, (IntPtr)ptr, (nuint)members.Length);
                }
            }
        }
        finally
        {
            foreach (var handle in handles)
            {
                handle.Free();
            }
        }
    }

    /// <summary>
    /// ZADD key score member - Add a single member to a sorted set.
    /// </summary>
    public long ZAdd(string key, double score, string member)
    {
        return ZAdd(key, new ZMember(score, member));
    }

    /// <summary>
    /// ZREM key member [member ...] - Remove members from a sorted set.
    /// </summary>
    public long ZRem(string key, params string[] members)
    {
        ThrowIfDisposed();
        var bytesArr = new RedliteBytes[members.Length];
        var handles = new List<GCHandle>();

        try
        {
            for (int i = 0; i < members.Length; i++)
            {
                var bytes = Encoding.UTF8.GetBytes(members[i]);
                var handle = GCHandle.Alloc(bytes, GCHandleType.Pinned);
                handles.Add(handle);
                bytesArr[i] = new RedliteBytes
                {
                    Data = handle.AddrOfPinnedObject(),
                    Len = (nuint)bytes.Length
                };
            }

            unsafe
            {
                fixed (RedliteBytes* ptr = bytesArr)
                {
                    return NativeMethods.redlite_zrem(_db, key, (IntPtr)ptr, (nuint)members.Length);
                }
            }
        }
        finally
        {
            foreach (var handle in handles)
            {
                handle.Free();
            }
        }
    }

    /// <summary>
    /// ZSCORE key member - Get the score of a member in a sorted set.
    /// </summary>
    /// <returns>The score, or null if member doesn't exist</returns>
    public double? ZScore(string key, string member)
    {
        ThrowIfDisposed();
        var bytes = Encoding.UTF8.GetBytes(member);
        double score;
        unsafe
        {
            fixed (byte* ptr = bytes)
            {
                score = NativeMethods.redlite_zscore(_db, key, (IntPtr)ptr, (nuint)bytes.Length);
            }
        }
        return double.IsNaN(score) ? null : score;
    }

    /// <summary>
    /// ZCARD key - Get the number of members in a sorted set.
    /// </summary>
    public long ZCard(string key)
    {
        ThrowIfDisposed();
        return NativeMethods.redlite_zcard(_db, key);
    }

    /// <summary>
    /// ZCOUNT key min max - Count members with scores within the given range.
    /// </summary>
    public long ZCount(string key, double min, double max)
    {
        ThrowIfDisposed();
        return NativeMethods.redlite_zcount(_db, key, min, max);
    }

    /// <summary>
    /// ZINCRBY key increment member - Increment the score of a member.
    /// </summary>
    public double ZIncrBy(string key, double increment, string member)
    {
        ThrowIfDisposed();
        var bytes = Encoding.UTF8.GetBytes(member);
        unsafe
        {
            fixed (byte* ptr = bytes)
            {
                return NativeMethods.redlite_zincrby(_db, key, increment, (IntPtr)ptr, (nuint)bytes.Length);
            }
        }
    }

    /// <summary>
    /// ZRANGE key start stop - Return a range of members by index.
    /// </summary>
    public string[] ZRange(string key, long start, long stop)
    {
        ThrowIfDisposed();
        var result = NativeMethods.redlite_zrange(_db, key, start, stop, 0);
        var arr = result.ToStringArray().Select(s => s ?? "").ToArray();
        NativeMethods.redlite_free_bytes_array(result);
        return arr;
    }

    /// <summary>
    /// ZRANGE key start stop WITHSCORES - Return a range of members with their scores.
    /// </summary>
    public ZMember[] ZRangeWithScores(string key, long start, long stop)
    {
        ThrowIfDisposed();
        var result = NativeMethods.redlite_zrange(_db, key, start, stop, 1);
        var arr = result.ToStringArray();
        NativeMethods.redlite_free_bytes_array(result);

        var members = new List<ZMember>();
        for (int i = 0; i + 1 < arr.Length; i += 2)
        {
            var member = arr[i] ?? "";
            var score = double.Parse(arr[i + 1] ?? "0");
            members.Add(new ZMember(score, member));
        }
        return members.ToArray();
    }

    /// <summary>
    /// ZREVRANGE key start stop - Return a range of members by index, in reverse order.
    /// </summary>
    public string[] ZRevRange(string key, long start, long stop)
    {
        ThrowIfDisposed();
        var result = NativeMethods.redlite_zrevrange(_db, key, start, stop, 0);
        var arr = result.ToStringArray().Select(s => s ?? "").ToArray();
        NativeMethods.redlite_free_bytes_array(result);
        return arr;
    }

    // ==================== Server Commands ====================

    /// <summary>
    /// VACUUM - Compact the database.
    /// </summary>
    public long Vacuum()
    {
        ThrowIfDisposed();
        return NativeMethods.redlite_vacuum(_db);
    }

    /// <summary>
    /// Get the Redlite library version.
    /// </summary>
    public static string Version()
    {
        var ptr = NativeMethods.redlite_version();
        if (ptr == IntPtr.Zero) return "";
        var version = Marshal.PtrToStringUTF8(ptr) ?? "";
        NativeMethods.redlite_free_string(ptr);
        return version;
    }

    // ==================== JSON Commands (ReJSON-compatible) ====================

    /// <summary>
    /// JSON.SET key path value - Set a JSON value at the specified path.
    /// </summary>
    /// <param name="key">The key</param>
    /// <param name="path">JSON path (use "$" for root)</param>
    /// <param name="value">JSON-encoded value</param>
    /// <param name="options">NX/XX options</param>
    /// <returns>True if set, false if NX/XX condition not met</returns>
    public bool JsonSet(string key, string path, string value, JsonSetOptions? options = null)
    {
        ThrowIfDisposed();
        var result = NativeMethods.redlite_json_set(
            _db, key, path, value,
            options?.Nx == true ? 1 : 0,
            options?.Xx == true ? 1 : 0);
        if (result < 0)
        {
            throw new RedliteException(GetLastError() ?? "JSON.SET failed");
        }
        return result == 1;
    }

    /// <summary>
    /// JSON.GET key [path ...] - Get JSON values at the specified paths.
    /// </summary>
    /// <param name="key">The key</param>
    /// <param name="paths">JSON paths to get (defaults to "$")</param>
    /// <returns>JSON-encoded result or null if not found</returns>
    public string? JsonGet(string key, params string[] paths)
    {
        ThrowIfDisposed();
        var pathsToUse = paths.Length == 0 ? new[] { "$" } : paths;
        var pathPtrs = AllocateStringArray(pathsToUse);
        try
        {
            var result = NativeMethods.redlite_json_get(_db, key, pathPtrs, (nuint)pathsToUse.Length);
            if (result == IntPtr.Zero) return null;
            var str = Marshal.PtrToStringUTF8(result);
            NativeMethods.redlite_free_string(result);
            return str;
        }
        finally
        {
            FreeStringArray(pathPtrs, pathsToUse.Length);
        }
    }

    /// <summary>
    /// JSON.DEL key [path] - Delete JSON values at the specified path.
    /// </summary>
    /// <param name="key">The key</param>
    /// <param name="path">JSON path (defaults to "$")</param>
    /// <returns>Number of paths deleted</returns>
    public long JsonDel(string key, string path = "$")
    {
        ThrowIfDisposed();
        return NativeMethods.redlite_json_del(_db, key, path);
    }

    /// <summary>
    /// JSON.TYPE key [path] - Get the type of JSON value at the specified path.
    /// </summary>
    /// <param name="key">The key</param>
    /// <param name="path">JSON path (defaults to "$")</param>
    /// <returns>Type name or null if not found</returns>
    public string? JsonType(string key, string path = "$")
    {
        ThrowIfDisposed();
        var result = NativeMethods.redlite_json_type(_db, key, path);
        if (result == IntPtr.Zero) return null;
        var str = Marshal.PtrToStringUTF8(result);
        NativeMethods.redlite_free_string(result);
        return str;
    }

    /// <summary>
    /// JSON.NUMINCRBY key path increment - Increment a JSON number.
    /// </summary>
    /// <param name="key">The key</param>
    /// <param name="path">JSON path</param>
    /// <param name="increment">Amount to increment</param>
    /// <returns>New value as JSON string</returns>
    public string? JsonNumIncrBy(string key, string path, double increment)
    {
        ThrowIfDisposed();
        var result = NativeMethods.redlite_json_numincrby(_db, key, path, increment);
        if (result == IntPtr.Zero) return null;
        var str = Marshal.PtrToStringUTF8(result);
        NativeMethods.redlite_free_string(result);
        return str;
    }

    /// <summary>
    /// JSON.STRAPPEND key path value - Append to a JSON string.
    /// </summary>
    /// <param name="key">The key</param>
    /// <param name="path">JSON path</param>
    /// <param name="value">String to append (JSON-encoded)</param>
    /// <returns>New length of string</returns>
    public long JsonStrAppend(string key, string path, string value)
    {
        ThrowIfDisposed();
        var result = NativeMethods.redlite_json_strappend(_db, key, path, value);
        if (result < 0)
        {
            throw new RedliteException(GetLastError() ?? "JSON.STRAPPEND failed");
        }
        return result;
    }

    /// <summary>
    /// JSON.STRLEN key [path] - Get the length of a JSON string.
    /// </summary>
    /// <param name="key">The key</param>
    /// <param name="path">JSON path (defaults to "$")</param>
    /// <returns>Length of string</returns>
    public long JsonStrLen(string key, string path = "$")
    {
        ThrowIfDisposed();
        return NativeMethods.redlite_json_strlen(_db, key, path);
    }

    /// <summary>
    /// JSON.ARRAPPEND key path value [value ...] - Append to a JSON array.
    /// </summary>
    /// <param name="key">The key</param>
    /// <param name="path">JSON path</param>
    /// <param name="values">JSON-encoded values to append</param>
    /// <returns>New length of array</returns>
    public long JsonArrAppend(string key, string path, params string[] values)
    {
        ThrowIfDisposed();
        var valuePtrs = AllocateStringArray(values);
        try
        {
            var result = NativeMethods.redlite_json_arrappend(_db, key, path, valuePtrs, (nuint)values.Length);
            if (result < 0)
            {
                throw new RedliteException(GetLastError() ?? "JSON.ARRAPPEND failed");
            }
            return result;
        }
        finally
        {
            FreeStringArray(valuePtrs, values.Length);
        }
    }

    /// <summary>
    /// JSON.ARRLEN key [path] - Get the length of a JSON array.
    /// </summary>
    /// <param name="key">The key</param>
    /// <param name="path">JSON path (defaults to "$")</param>
    /// <returns>Length of array</returns>
    public long JsonArrLen(string key, string path = "$")
    {
        ThrowIfDisposed();
        return NativeMethods.redlite_json_arrlen(_db, key, path);
    }

    /// <summary>
    /// JSON.ARRPOP key [path [index]] - Pop an element from a JSON array.
    /// </summary>
    /// <param name="key">The key</param>
    /// <param name="path">JSON path (defaults to "$")</param>
    /// <param name="index">Index to pop from (defaults to -1, last element)</param>
    /// <returns>Popped value as JSON string</returns>
    public string? JsonArrPop(string key, string path = "$", long index = -1)
    {
        ThrowIfDisposed();
        var result = NativeMethods.redlite_json_arrpop(_db, key, path, index);
        if (result == IntPtr.Zero) return null;
        var str = Marshal.PtrToStringUTF8(result);
        NativeMethods.redlite_free_string(result);
        return str;
    }

    /// <summary>
    /// JSON.CLEAR key [path] - Clear JSON arrays or objects.
    /// </summary>
    /// <param name="key">The key</param>
    /// <param name="path">JSON path (defaults to "$")</param>
    /// <returns>Number of values cleared</returns>
    public long JsonClear(string key, string path = "$")
    {
        ThrowIfDisposed();
        return NativeMethods.redlite_json_clear(_db, key, path);
    }

    // ==================== History Commands ====================

    /// <summary>
    /// Enable history tracking globally.
    /// </summary>
    /// <param name="retentionType">"unlimited", "time", or "count"</param>
    /// <param name="retentionValue">Value for time (ms) or count retention</param>
    public void HistoryEnableGlobal(string retentionType = "unlimited", long retentionValue = 0)
    {
        ThrowIfDisposed();
        var result = NativeMethods.redlite_history_enable_global(_db, retentionType, retentionValue);
        if (result < 0)
        {
            throw new RedliteException(GetLastError() ?? "HISTORY.ENABLE GLOBAL failed");
        }
    }

    /// <summary>
    /// Enable history tracking for a specific database.
    /// </summary>
    /// <param name="dbNum">Database number</param>
    /// <param name="retentionType">"unlimited", "time", or "count"</param>
    /// <param name="retentionValue">Value for time (ms) or count retention</param>
    public void HistoryEnableDatabase(int dbNum, string retentionType = "unlimited", long retentionValue = 0)
    {
        ThrowIfDisposed();
        var result = NativeMethods.redlite_history_enable_database(_db, dbNum, retentionType, retentionValue);
        if (result < 0)
        {
            throw new RedliteException(GetLastError() ?? "HISTORY.ENABLE DATABASE failed");
        }
    }

    /// <summary>
    /// Enable history tracking for a specific key.
    /// </summary>
    /// <param name="key">Key to enable history for</param>
    /// <param name="retentionType">"unlimited", "time", or "count"</param>
    /// <param name="retentionValue">Value for time (ms) or count retention</param>
    public void HistoryEnableKey(string key, string retentionType = "unlimited", long retentionValue = 0)
    {
        ThrowIfDisposed();
        var result = NativeMethods.redlite_history_enable_key(_db, key, retentionType, retentionValue);
        if (result < 0)
        {
            throw new RedliteException(GetLastError() ?? "HISTORY.ENABLE KEY failed");
        }
    }

    /// <summary>
    /// Disable history tracking globally.
    /// </summary>
    public void HistoryDisableGlobal()
    {
        ThrowIfDisposed();
        var result = NativeMethods.redlite_history_disable_global(_db);
        if (result < 0)
        {
            throw new RedliteException(GetLastError() ?? "HISTORY.DISABLE GLOBAL failed");
        }
    }

    /// <summary>
    /// Disable history tracking for a specific database.
    /// </summary>
    /// <param name="dbNum">Database number</param>
    public void HistoryDisableDatabase(int dbNum)
    {
        ThrowIfDisposed();
        var result = NativeMethods.redlite_history_disable_database(_db, dbNum);
        if (result < 0)
        {
            throw new RedliteException(GetLastError() ?? "HISTORY.DISABLE DATABASE failed");
        }
    }

    /// <summary>
    /// Disable history tracking for a specific key.
    /// </summary>
    /// <param name="key">Key to disable history for</param>
    public void HistoryDisableKey(string key)
    {
        ThrowIfDisposed();
        var result = NativeMethods.redlite_history_disable_key(_db, key);
        if (result < 0)
        {
            throw new RedliteException(GetLastError() ?? "HISTORY.DISABLE KEY failed");
        }
    }

    /// <summary>
    /// Check if history tracking is enabled for a key.
    /// </summary>
    /// <param name="key">Key to check</param>
    /// <returns>True if history is enabled</returns>
    public bool IsHistoryEnabled(string key)
    {
        ThrowIfDisposed();
        return NativeMethods.redlite_is_history_enabled(_db, key) == 1;
    }

    // ==================== FTS (Full-Text Search) Commands ====================

    /// <summary>
    /// Enable FTS indexing globally.
    /// </summary>
    public void FtsEnableGlobal()
    {
        ThrowIfDisposed();
        var result = NativeMethods.redlite_fts_enable_global(_db);
        if (result < 0)
        {
            throw new RedliteException(GetLastError() ?? "FTS.ENABLE GLOBAL failed");
        }
    }

    /// <summary>
    /// Enable FTS indexing for a specific database.
    /// </summary>
    /// <param name="dbNum">Database number</param>
    public void FtsEnableDatabase(int dbNum)
    {
        ThrowIfDisposed();
        var result = NativeMethods.redlite_fts_enable_database(_db, dbNum);
        if (result < 0)
        {
            throw new RedliteException(GetLastError() ?? "FTS.ENABLE DATABASE failed");
        }
    }

    /// <summary>
    /// Enable FTS indexing for keys matching a pattern.
    /// </summary>
    /// <param name="pattern">Glob pattern</param>
    public void FtsEnablePattern(string pattern)
    {
        ThrowIfDisposed();
        var result = NativeMethods.redlite_fts_enable_pattern(_db, pattern);
        if (result < 0)
        {
            throw new RedliteException(GetLastError() ?? "FTS.ENABLE PATTERN failed");
        }
    }

    /// <summary>
    /// Enable FTS indexing for a specific key.
    /// </summary>
    /// <param name="key">Key to enable FTS for</param>
    public void FtsEnableKey(string key)
    {
        ThrowIfDisposed();
        var result = NativeMethods.redlite_fts_enable_key(_db, key);
        if (result < 0)
        {
            throw new RedliteException(GetLastError() ?? "FTS.ENABLE KEY failed");
        }
    }

    /// <summary>
    /// Disable FTS indexing globally.
    /// </summary>
    public void FtsDisableGlobal()
    {
        ThrowIfDisposed();
        var result = NativeMethods.redlite_fts_disable_global(_db);
        if (result < 0)
        {
            throw new RedliteException(GetLastError() ?? "FTS.DISABLE GLOBAL failed");
        }
    }

    /// <summary>
    /// Disable FTS indexing for a specific database.
    /// </summary>
    /// <param name="dbNum">Database number</param>
    public void FtsDisableDatabase(int dbNum)
    {
        ThrowIfDisposed();
        var result = NativeMethods.redlite_fts_disable_database(_db, dbNum);
        if (result < 0)
        {
            throw new RedliteException(GetLastError() ?? "FTS.DISABLE DATABASE failed");
        }
    }

    /// <summary>
    /// Disable FTS indexing for keys matching a pattern.
    /// </summary>
    /// <param name="pattern">Glob pattern</param>
    public void FtsDisablePattern(string pattern)
    {
        ThrowIfDisposed();
        var result = NativeMethods.redlite_fts_disable_pattern(_db, pattern);
        if (result < 0)
        {
            throw new RedliteException(GetLastError() ?? "FTS.DISABLE PATTERN failed");
        }
    }

    /// <summary>
    /// Disable FTS indexing for a specific key.
    /// </summary>
    /// <param name="key">Key to disable FTS for</param>
    public void FtsDisableKey(string key)
    {
        ThrowIfDisposed();
        var result = NativeMethods.redlite_fts_disable_key(_db, key);
        if (result < 0)
        {
            throw new RedliteException(GetLastError() ?? "FTS.DISABLE KEY failed");
        }
    }

    /// <summary>
    /// Check if FTS indexing is enabled for a key.
    /// </summary>
    /// <param name="key">Key to check</param>
    /// <returns>True if FTS is enabled</returns>
    public bool IsFtsEnabled(string key)
    {
        ThrowIfDisposed();
        return NativeMethods.redlite_is_fts_enabled(_db, key) == 1;
    }

    // ==================== KeyInfo Command ====================

    /// <summary>
    /// KEYINFO - Get detailed information about a key.
    /// </summary>
    /// <param name="key">Key to get info for</param>
    /// <returns>KeyInfo or null if key doesn't exist</returns>
    public KeyInfo? KeyInfo(string key)
    {
        ThrowIfDisposed();
        var info = NativeMethods.redlite_keyinfo(_db, key);
        if (info.Valid == 0)
        {
            NativeMethods.redlite_free_keyinfo(info);
            return null;
        }

        var keyType = info.KeyType != IntPtr.Zero ? Marshal.PtrToStringUTF8(info.KeyType) ?? "none" : "none";
        var result = new KeyInfo(keyType, info.Ttl, info.CreatedAt, info.UpdatedAt);
        NativeMethods.redlite_free_keyinfo(info);
        return result;
    }

    // ==================== Helper Methods ====================

    private static IntPtr AllocateStringArray(string[] strings)
    {
        var ptrs = new IntPtr[strings.Length];
        for (int i = 0; i < strings.Length; i++)
        {
            ptrs[i] = Marshal.StringToCoTaskMemUTF8(strings[i]);
        }

        var arrayPtr = Marshal.AllocHGlobal(IntPtr.Size * strings.Length);
        Marshal.Copy(ptrs, 0, arrayPtr, strings.Length);
        return arrayPtr;
    }

    private static void FreeStringArray(IntPtr arrayPtr, int count)
    {
        var ptrs = new IntPtr[count];
        Marshal.Copy(arrayPtr, ptrs, 0, count);

        foreach (var ptr in ptrs)
        {
            Marshal.FreeCoTaskMem(ptr);
        }
        Marshal.FreeHGlobal(arrayPtr);
    }

    // ==================== IDisposable ====================

    public void Dispose()
    {
        if (!_disposed)
        {
            if (_db != IntPtr.Zero)
            {
                NativeMethods.redlite_close(_db);
                _db = IntPtr.Zero;
            }
            _disposed = true;
        }
    }
}
