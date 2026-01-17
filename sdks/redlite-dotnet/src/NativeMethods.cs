using System.Runtime.InteropServices;

namespace Redlite.Native;

/// <summary>
/// Native type representing bytes returned from Redlite
/// </summary>
[StructLayout(LayoutKind.Sequential)]
internal struct RedliteBytes
{
    public IntPtr Data;
    public nuint Len;

    public readonly bool IsNull => Data == IntPtr.Zero;

    public readonly byte[]? ToArray()
    {
        if (Data == IntPtr.Zero) return null;
        var bytes = new byte[Len];
        Marshal.Copy(Data, bytes, 0, (int)Len);
        return bytes;
    }

    public readonly string? ToStringUtf8()
    {
        if (Data == IntPtr.Zero) return null;
        return Marshal.PtrToStringUTF8(Data, (int)Len);
    }
}

/// <summary>
/// Native type representing a string array
/// </summary>
[StructLayout(LayoutKind.Sequential)]
internal struct RedliteStringArray
{
    public IntPtr Strings;
    public nuint Len;

    public readonly string[] ToArray()
    {
        if (Strings == IntPtr.Zero || Len == 0) return [];

        var result = new string[Len];
        for (nuint i = 0; i < Len; i++)
        {
            var ptr = Marshal.ReadIntPtr(Strings, (int)i * IntPtr.Size);
            result[i] = Marshal.PtrToStringUTF8(ptr) ?? "";
        }
        return result;
    }
}

/// <summary>
/// Native type representing a bytes array
/// </summary>
[StructLayout(LayoutKind.Sequential)]
internal struct RedliteBytesArray
{
    public IntPtr Items;
    public nuint Len;

    public readonly byte[]?[] ToArray()
    {
        if (Items == IntPtr.Zero || Len == 0) return [];

        var result = new byte[]?[Len];
        var itemSize = Marshal.SizeOf<RedliteBytes>();

        for (nuint i = 0; i < Len; i++)
        {
            var itemPtr = Items + (int)i * itemSize;
            var item = Marshal.PtrToStructure<RedliteBytes>(itemPtr);
            result[i] = item.ToArray();
        }
        return result;
    }

    public readonly string?[] ToStringArray()
    {
        if (Items == IntPtr.Zero || Len == 0) return [];

        var result = new string?[Len];
        var itemSize = Marshal.SizeOf<RedliteBytes>();

        for (nuint i = 0; i < Len; i++)
        {
            var itemPtr = Items + (int)i * itemSize;
            var item = Marshal.PtrToStructure<RedliteBytes>(itemPtr);
            result[i] = item.ToStringUtf8();
        }
        return result;
    }
}

/// <summary>
/// Native type for key-value pairs (hash operations)
/// </summary>
[StructLayout(LayoutKind.Sequential)]
internal struct RedliteKV
{
    public IntPtr Key;
    public IntPtr Value;
    public nuint ValueLen;
}

/// <summary>
/// Native type for sorted set members
/// </summary>
[StructLayout(LayoutKind.Sequential)]
internal struct RedliteZMemberNative
{
    public double Score;
    public IntPtr Member;
    public nuint MemberLen;
}

/// <summary>
/// P/Invoke declarations for Redlite C FFI
/// </summary>
internal static partial class NativeMethods
{
    private const string LibraryName = "redlite_ffi";

    // Database lifecycle
    [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl)]
    public static extern IntPtr redlite_open([MarshalAs(UnmanagedType.LPUTF8Str)] string path);

    [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl)]
    public static extern IntPtr redlite_open_memory();

    [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl)]
    public static extern IntPtr redlite_open_with_cache(
        [MarshalAs(UnmanagedType.LPUTF8Str)] string path,
        long cacheMb);

    [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl)]
    public static extern void redlite_close(IntPtr db);

    [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl)]
    public static extern IntPtr redlite_last_error();

    [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl)]
    public static extern void redlite_free_string(IntPtr s);

    [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl)]
    public static extern void redlite_free_bytes(RedliteBytes bytes);

    [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl)]
    public static extern void redlite_free_string_array(RedliteStringArray arr);

    [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl)]
    public static extern void redlite_free_bytes_array(RedliteBytesArray arr);

    // String commands
    [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl)]
    public static extern RedliteBytes redlite_get(
        IntPtr db,
        [MarshalAs(UnmanagedType.LPUTF8Str)] string key);

    [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl)]
    public static extern int redlite_set(
        IntPtr db,
        [MarshalAs(UnmanagedType.LPUTF8Str)] string key,
        IntPtr value,
        nuint valueLen,
        long ttlSeconds);

    [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl)]
    public static extern int redlite_setex(
        IntPtr db,
        [MarshalAs(UnmanagedType.LPUTF8Str)] string key,
        long seconds,
        IntPtr value,
        nuint valueLen);

    [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl)]
    public static extern int redlite_psetex(
        IntPtr db,
        [MarshalAs(UnmanagedType.LPUTF8Str)] string key,
        long milliseconds,
        IntPtr value,
        nuint valueLen);

    [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl)]
    public static extern RedliteBytes redlite_getdel(
        IntPtr db,
        [MarshalAs(UnmanagedType.LPUTF8Str)] string key);

    [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl)]
    public static extern long redlite_append(
        IntPtr db,
        [MarshalAs(UnmanagedType.LPUTF8Str)] string key,
        IntPtr value,
        nuint valueLen);

    [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl)]
    public static extern long redlite_strlen(
        IntPtr db,
        [MarshalAs(UnmanagedType.LPUTF8Str)] string key);

    [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl)]
    public static extern RedliteBytes redlite_getrange(
        IntPtr db,
        [MarshalAs(UnmanagedType.LPUTF8Str)] string key,
        long start,
        long end);

    [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl)]
    public static extern long redlite_setrange(
        IntPtr db,
        [MarshalAs(UnmanagedType.LPUTF8Str)] string key,
        long offset,
        IntPtr value,
        nuint valueLen);

    [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl)]
    public static extern long redlite_incr(
        IntPtr db,
        [MarshalAs(UnmanagedType.LPUTF8Str)] string key);

    [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl)]
    public static extern long redlite_decr(
        IntPtr db,
        [MarshalAs(UnmanagedType.LPUTF8Str)] string key);

    [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl)]
    public static extern long redlite_incrby(
        IntPtr db,
        [MarshalAs(UnmanagedType.LPUTF8Str)] string key,
        long increment);

    [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl)]
    public static extern long redlite_decrby(
        IntPtr db,
        [MarshalAs(UnmanagedType.LPUTF8Str)] string key,
        long decrement);

    [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl)]
    public static extern IntPtr redlite_incrbyfloat(
        IntPtr db,
        [MarshalAs(UnmanagedType.LPUTF8Str)] string key,
        double increment);

    [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl)]
    public static extern RedliteBytesArray redlite_mget(
        IntPtr db,
        IntPtr keys,
        nuint keysLen);

    [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl)]
    public static extern int redlite_mset(
        IntPtr db,
        IntPtr pairs,
        nuint pairsLen);

    // Key commands
    [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl)]
    public static extern long redlite_del(
        IntPtr db,
        IntPtr keys,
        nuint keysLen);

    [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl)]
    public static extern long redlite_exists(
        IntPtr db,
        IntPtr keys,
        nuint keysLen);

    [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl)]
    public static extern IntPtr redlite_type(
        IntPtr db,
        [MarshalAs(UnmanagedType.LPUTF8Str)] string key);

    [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl)]
    public static extern long redlite_ttl(
        IntPtr db,
        [MarshalAs(UnmanagedType.LPUTF8Str)] string key);

    [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl)]
    public static extern long redlite_pttl(
        IntPtr db,
        [MarshalAs(UnmanagedType.LPUTF8Str)] string key);

    [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl)]
    public static extern int redlite_expire(
        IntPtr db,
        [MarshalAs(UnmanagedType.LPUTF8Str)] string key,
        long seconds);

    [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl)]
    public static extern int redlite_pexpire(
        IntPtr db,
        [MarshalAs(UnmanagedType.LPUTF8Str)] string key,
        long milliseconds);

    [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl)]
    public static extern int redlite_expireat(
        IntPtr db,
        [MarshalAs(UnmanagedType.LPUTF8Str)] string key,
        long unixSeconds);

    [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl)]
    public static extern int redlite_pexpireat(
        IntPtr db,
        [MarshalAs(UnmanagedType.LPUTF8Str)] string key,
        long unixMs);

    [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl)]
    public static extern int redlite_persist(
        IntPtr db,
        [MarshalAs(UnmanagedType.LPUTF8Str)] string key);

    [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl)]
    public static extern int redlite_rename(
        IntPtr db,
        [MarshalAs(UnmanagedType.LPUTF8Str)] string key,
        [MarshalAs(UnmanagedType.LPUTF8Str)] string newkey);

    [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl)]
    public static extern int redlite_renamenx(
        IntPtr db,
        [MarshalAs(UnmanagedType.LPUTF8Str)] string key,
        [MarshalAs(UnmanagedType.LPUTF8Str)] string newkey);

    [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl)]
    public static extern RedliteStringArray redlite_keys(
        IntPtr db,
        [MarshalAs(UnmanagedType.LPUTF8Str)] string pattern);

    [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl)]
    public static extern long redlite_dbsize(IntPtr db);

    [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl)]
    public static extern int redlite_flushdb(IntPtr db);

    [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl)]
    public static extern int redlite_select(IntPtr db, int dbNum);

    // Hash commands
    [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl)]
    public static extern long redlite_hset(
        IntPtr db,
        [MarshalAs(UnmanagedType.LPUTF8Str)] string key,
        IntPtr fields,
        IntPtr values,
        nuint count);

    [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl)]
    public static extern RedliteBytes redlite_hget(
        IntPtr db,
        [MarshalAs(UnmanagedType.LPUTF8Str)] string key,
        [MarshalAs(UnmanagedType.LPUTF8Str)] string field);

    [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl)]
    public static extern long redlite_hdel(
        IntPtr db,
        [MarshalAs(UnmanagedType.LPUTF8Str)] string key,
        IntPtr fields,
        nuint fieldsLen);

    [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl)]
    public static extern int redlite_hexists(
        IntPtr db,
        [MarshalAs(UnmanagedType.LPUTF8Str)] string key,
        [MarshalAs(UnmanagedType.LPUTF8Str)] string field);

    [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl)]
    public static extern long redlite_hlen(
        IntPtr db,
        [MarshalAs(UnmanagedType.LPUTF8Str)] string key);

    [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl)]
    public static extern RedliteStringArray redlite_hkeys(
        IntPtr db,
        [MarshalAs(UnmanagedType.LPUTF8Str)] string key);

    [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl)]
    public static extern RedliteBytesArray redlite_hvals(
        IntPtr db,
        [MarshalAs(UnmanagedType.LPUTF8Str)] string key);

    [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl)]
    public static extern long redlite_hincrby(
        IntPtr db,
        [MarshalAs(UnmanagedType.LPUTF8Str)] string key,
        [MarshalAs(UnmanagedType.LPUTF8Str)] string field,
        long increment);

    [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl)]
    public static extern RedliteBytesArray redlite_hgetall(
        IntPtr db,
        [MarshalAs(UnmanagedType.LPUTF8Str)] string key);

    [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl)]
    public static extern RedliteBytesArray redlite_hmget(
        IntPtr db,
        [MarshalAs(UnmanagedType.LPUTF8Str)] string key,
        IntPtr fields,
        nuint fieldsLen);

    // List commands
    [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl)]
    public static extern long redlite_lpush(
        IntPtr db,
        [MarshalAs(UnmanagedType.LPUTF8Str)] string key,
        IntPtr values,
        nuint valuesLen);

    [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl)]
    public static extern long redlite_rpush(
        IntPtr db,
        [MarshalAs(UnmanagedType.LPUTF8Str)] string key,
        IntPtr values,
        nuint valuesLen);

    [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl)]
    public static extern RedliteBytesArray redlite_lpop(
        IntPtr db,
        [MarshalAs(UnmanagedType.LPUTF8Str)] string key,
        nuint count);

    [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl)]
    public static extern RedliteBytesArray redlite_rpop(
        IntPtr db,
        [MarshalAs(UnmanagedType.LPUTF8Str)] string key,
        nuint count);

    [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl)]
    public static extern long redlite_llen(
        IntPtr db,
        [MarshalAs(UnmanagedType.LPUTF8Str)] string key);

    [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl)]
    public static extern RedliteBytesArray redlite_lrange(
        IntPtr db,
        [MarshalAs(UnmanagedType.LPUTF8Str)] string key,
        long start,
        long stop);

    [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl)]
    public static extern RedliteBytes redlite_lindex(
        IntPtr db,
        [MarshalAs(UnmanagedType.LPUTF8Str)] string key,
        long index);

    // Set commands
    [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl)]
    public static extern long redlite_sadd(
        IntPtr db,
        [MarshalAs(UnmanagedType.LPUTF8Str)] string key,
        IntPtr members,
        nuint membersLen);

    [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl)]
    public static extern long redlite_srem(
        IntPtr db,
        [MarshalAs(UnmanagedType.LPUTF8Str)] string key,
        IntPtr members,
        nuint membersLen);

    [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl)]
    public static extern RedliteBytesArray redlite_smembers(
        IntPtr db,
        [MarshalAs(UnmanagedType.LPUTF8Str)] string key);

    [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl)]
    public static extern int redlite_sismember(
        IntPtr db,
        [MarshalAs(UnmanagedType.LPUTF8Str)] string key,
        IntPtr member,
        nuint memberLen);

    [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl)]
    public static extern long redlite_scard(
        IntPtr db,
        [MarshalAs(UnmanagedType.LPUTF8Str)] string key);

    // Sorted set commands
    [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl)]
    public static extern long redlite_zadd(
        IntPtr db,
        [MarshalAs(UnmanagedType.LPUTF8Str)] string key,
        IntPtr members,
        nuint membersLen);

    [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl)]
    public static extern long redlite_zrem(
        IntPtr db,
        [MarshalAs(UnmanagedType.LPUTF8Str)] string key,
        IntPtr members,
        nuint membersLen);

    [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl)]
    public static extern double redlite_zscore(
        IntPtr db,
        [MarshalAs(UnmanagedType.LPUTF8Str)] string key,
        IntPtr member,
        nuint memberLen);

    [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl)]
    public static extern long redlite_zcard(
        IntPtr db,
        [MarshalAs(UnmanagedType.LPUTF8Str)] string key);

    [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl)]
    public static extern long redlite_zcount(
        IntPtr db,
        [MarshalAs(UnmanagedType.LPUTF8Str)] string key,
        double min,
        double max);

    [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl)]
    public static extern double redlite_zincrby(
        IntPtr db,
        [MarshalAs(UnmanagedType.LPUTF8Str)] string key,
        double increment,
        IntPtr member,
        nuint memberLen);

    [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl)]
    public static extern RedliteBytesArray redlite_zrange(
        IntPtr db,
        [MarshalAs(UnmanagedType.LPUTF8Str)] string key,
        long start,
        long stop,
        int withScores);

    [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl)]
    public static extern RedliteBytesArray redlite_zrevrange(
        IntPtr db,
        [MarshalAs(UnmanagedType.LPUTF8Str)] string key,
        long start,
        long stop,
        int withScores);

    // Server commands
    [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl)]
    public static extern long redlite_vacuum(IntPtr db);

    [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl)]
    public static extern IntPtr redlite_version();
}
