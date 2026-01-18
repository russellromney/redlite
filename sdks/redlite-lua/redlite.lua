--[[
Redlite Lua SDK - LuaJIT FFI Bindings

Redis-compatible embedded database with SQLite durability.
Uses LuaJIT FFI for maximum performance.

Usage:
    local redlite = require("redlite")
    local db = redlite.open_memory()  -- or redlite.open("/path/to/db.sqlite")

    db:set("key", "value")
    local value = db:get("key")  -- returns "value"

    db:hset("hash", "field", "value")
    local fields = db:hgetall("hash")

    db:close()

Copyright (c) 2024 Redlite
MIT License
]]

local ffi = require("ffi")

-- FFI declarations matching redlite.h
ffi.cdef[[
    // Opaque handle to a redlite database
    typedef struct RedliteDb RedliteDb;

    // Result of operations that return bytes
    typedef struct RedliteBytes {
        uint8_t *data;
        size_t len;
    } RedliteBytes;

    // Result of operations that return a string array
    typedef struct RedliteStringArray {
        char **strings;
        size_t len;
    } RedliteStringArray;

    // Result of operations that return bytes array
    typedef struct RedliteBytesArray {
        RedliteBytes *items;
        size_t len;
    } RedliteBytesArray;

    // Key-value pair for hash operations
    typedef struct RedliteKV {
        const char *key;
        const uint8_t *value;
        size_t value_len;
    } RedliteKV;

    // Sorted set member
    typedef struct RedliteZMember {
        double score;
        const uint8_t *member;
        size_t member_len;
    } RedliteZMember;

    // Lifecycle functions
    RedliteDb *redlite_open(const char *path);
    RedliteDb *redlite_open_memory(void);
    RedliteDb *redlite_open_with_cache(const char *path, int64_t cache_mb);
    void redlite_close(RedliteDb *db);
    char *redlite_last_error(void);
    char *redlite_version(void);

    // Memory management
    void redlite_free_string(char *s);
    void redlite_free_bytes(RedliteBytes bytes);
    void redlite_free_string_array(RedliteStringArray arr);
    void redlite_free_bytes_array(RedliteBytesArray arr);

    // String commands
    RedliteBytes redlite_get(RedliteDb *db, const char *key);
    int redlite_set(RedliteDb *db, const char *key, const uint8_t *value, size_t value_len, int64_t ttl_seconds);
    int redlite_setex(RedliteDb *db, const char *key, int64_t seconds, const uint8_t *value, size_t value_len);
    int redlite_psetex(RedliteDb *db, const char *key, int64_t milliseconds, const uint8_t *value, size_t value_len);
    RedliteBytes redlite_getdel(RedliteDb *db, const char *key);
    int64_t redlite_append(RedliteDb *db, const char *key, const uint8_t *value, size_t value_len);
    int64_t redlite_strlen(RedliteDb *db, const char *key);
    RedliteBytes redlite_getrange(RedliteDb *db, const char *key, int64_t start, int64_t end);
    int64_t redlite_setrange(RedliteDb *db, const char *key, int64_t offset, const uint8_t *value, size_t value_len);
    int64_t redlite_incr(RedliteDb *db, const char *key);
    int64_t redlite_decr(RedliteDb *db, const char *key);
    int64_t redlite_incrby(RedliteDb *db, const char *key, int64_t increment);
    int64_t redlite_decrby(RedliteDb *db, const char *key, int64_t decrement);
    char *redlite_incrbyfloat(RedliteDb *db, const char *key, double increment);
    RedliteBytesArray redlite_mget(RedliteDb *db, const char * const *keys, size_t keys_len);
    int redlite_mset(RedliteDb *db, const RedliteKV *pairs, size_t pairs_len);

    // Key commands
    int64_t redlite_del(RedliteDb *db, const char * const *keys, size_t keys_len);
    int64_t redlite_exists(RedliteDb *db, const char * const *keys, size_t keys_len);
    char *redlite_type(RedliteDb *db, const char *key);
    int64_t redlite_ttl(RedliteDb *db, const char *key);
    int64_t redlite_pttl(RedliteDb *db, const char *key);
    int redlite_expire(RedliteDb *db, const char *key, int64_t seconds);
    int redlite_pexpire(RedliteDb *db, const char *key, int64_t milliseconds);
    int redlite_expireat(RedliteDb *db, const char *key, int64_t unix_seconds);
    int redlite_pexpireat(RedliteDb *db, const char *key, int64_t unix_ms);
    int redlite_persist(RedliteDb *db, const char *key);
    int redlite_rename(RedliteDb *db, const char *key, const char *newkey);
    int redlite_renamenx(RedliteDb *db, const char *key, const char *newkey);
    RedliteStringArray redlite_keys(RedliteDb *db, const char *pattern);
    int64_t redlite_dbsize(RedliteDb *db);
    int redlite_flushdb(RedliteDb *db);
    int redlite_select(RedliteDb *db, int db_num);

    // Hash commands
    int64_t redlite_hset(RedliteDb *db, const char *key, const char * const *fields, const RedliteBytes *values, size_t count);
    RedliteBytes redlite_hget(RedliteDb *db, const char *key, const char *field);
    int64_t redlite_hdel(RedliteDb *db, const char *key, const char * const *fields, size_t fields_len);
    int redlite_hexists(RedliteDb *db, const char *key, const char *field);
    int64_t redlite_hlen(RedliteDb *db, const char *key);
    RedliteStringArray redlite_hkeys(RedliteDb *db, const char *key);
    RedliteBytesArray redlite_hvals(RedliteDb *db, const char *key);
    int64_t redlite_hincrby(RedliteDb *db, const char *key, const char *field, int64_t increment);
    RedliteBytesArray redlite_hgetall(RedliteDb *db, const char *key);
    RedliteBytesArray redlite_hmget(RedliteDb *db, const char *key, const char * const *fields, size_t fields_len);

    // List commands
    int64_t redlite_lpush(RedliteDb *db, const char *key, const RedliteBytes *values, size_t values_len);
    int64_t redlite_rpush(RedliteDb *db, const char *key, const RedliteBytes *values, size_t values_len);
    int64_t redlite_lpushx(RedliteDb *db, const char *key, const RedliteBytes *values, size_t values_len);
    int64_t redlite_rpushx(RedliteDb *db, const char *key, const RedliteBytes *values, size_t values_len);
    RedliteBytesArray redlite_lpop(RedliteDb *db, const char *key, size_t count);
    RedliteBytesArray redlite_rpop(RedliteDb *db, const char *key, size_t count);
    int64_t redlite_llen(RedliteDb *db, const char *key);
    RedliteBytesArray redlite_lrange(RedliteDb *db, const char *key, int64_t start, int64_t stop);
    RedliteBytes redlite_lindex(RedliteDb *db, const char *key, int64_t index);
    RedliteBytes redlite_lmove(RedliteDb *db, const char *source, const char *destination, int wherefrom, int whereto);
    RedliteBytesArray redlite_lpos(RedliteDb *db, const char *key, const uint8_t *element, size_t element_len, int64_t rank, size_t count, size_t maxlen);

    // Set commands
    int64_t redlite_sadd(RedliteDb *db, const char *key, const RedliteBytes *members, size_t members_len);
    int64_t redlite_srem(RedliteDb *db, const char *key, const RedliteBytes *members, size_t members_len);
    RedliteBytesArray redlite_smembers(RedliteDb *db, const char *key);
    int redlite_sismember(RedliteDb *db, const char *key, const uint8_t *member, size_t member_len);
    int64_t redlite_scard(RedliteDb *db, const char *key);

    // Sorted set commands
    int64_t redlite_zadd(RedliteDb *db, const char *key, const RedliteZMember *members, size_t members_len);
    int64_t redlite_zrem(RedliteDb *db, const char *key, const RedliteBytes *members, size_t members_len);
    double redlite_zscore(RedliteDb *db, const char *key, const uint8_t *member, size_t member_len);
    int64_t redlite_zcard(RedliteDb *db, const char *key);
    int64_t redlite_zcount(RedliteDb *db, const char *key, double min, double max);
    double redlite_zincrby(RedliteDb *db, const char *key, double increment, const uint8_t *member, size_t member_len);
    RedliteBytesArray redlite_zrange(RedliteDb *db, const char *key, int64_t start, int64_t stop, int with_scores);
    RedliteBytesArray redlite_zrevrange(RedliteDb *db, const char *key, int64_t start, int64_t stop, int with_scores);
    int64_t redlite_zinterstore(RedliteDb *db, const char *destination, const char * const *keys, size_t keys_len, const double *weights, size_t weights_len, const char *aggregate);
    int64_t redlite_zunionstore(RedliteDb *db, const char *destination, const char * const *keys, size_t keys_len, const double *weights, size_t weights_len, const char *aggregate);

    // Utility
    int64_t redlite_vacuum(RedliteDb *db);

    // JSON commands (ReJSON-compatible)
    int redlite_json_set(RedliteDb *db, const char *key, const char *path, const char *value, int nx, int xx);
    char *redlite_json_get(RedliteDb *db, const char *key, const char **paths, size_t paths_len);
    int64_t redlite_json_del(RedliteDb *db, const char *key, const char *path);
    char *redlite_json_type(RedliteDb *db, const char *key, const char *path);
    char *redlite_json_numincrby(RedliteDb *db, const char *key, const char *path, double increment);
    int64_t redlite_json_strappend(RedliteDb *db, const char *key, const char *path, const char *value);
    int64_t redlite_json_strlen(RedliteDb *db, const char *key, const char *path);
    int64_t redlite_json_arrappend(RedliteDb *db, const char *key, const char *path, const char **values, size_t values_len);
    int64_t redlite_json_arrlen(RedliteDb *db, const char *key, const char *path);
    char *redlite_json_arrpop(RedliteDb *db, const char *key, const char *path, int64_t index);
    int64_t redlite_json_clear(RedliteDb *db, const char *key, const char *path);

    // History enable/disable commands
    int redlite_history_enable_global(RedliteDb *db, const char *retention_type, int64_t retention_value);
    int redlite_history_enable_database(RedliteDb *db, int db_num, const char *retention_type, int64_t retention_value);
    int redlite_history_enable_key(RedliteDb *db, const char *key, const char *retention_type, int64_t retention_value);
    int redlite_history_disable_global(RedliteDb *db);
    int redlite_history_disable_database(RedliteDb *db, int db_num);
    int redlite_history_disable_key(RedliteDb *db, const char *key);
    int redlite_is_history_enabled(RedliteDb *db, const char *key);

    // FTS enable/disable commands
    int redlite_fts_enable_global(RedliteDb *db);
    int redlite_fts_enable_database(RedliteDb *db, int db_num);
    int redlite_fts_enable_pattern(RedliteDb *db, const char *pattern);
    int redlite_fts_enable_key(RedliteDb *db, const char *key);
    int redlite_fts_disable_global(RedliteDb *db);
    int redlite_fts_disable_database(RedliteDb *db, int db_num);
    int redlite_fts_disable_pattern(RedliteDb *db, const char *pattern);
    int redlite_fts_disable_key(RedliteDb *db, const char *key);
    int redlite_is_fts_enabled(RedliteDb *db, const char *key);

    // KeyInfo command
    typedef struct RedliteKeyInfo {
        char *key_type;
        int64_t ttl;
        int64_t created_at;
        int64_t updated_at;
        int valid;
    } RedliteKeyInfo;
    RedliteKeyInfo redlite_keyinfo(RedliteDb *db, const char *key);
    void redlite_free_keyinfo(RedliteKeyInfo info);
]]

-- Load the shared library
local lib_path = os.getenv("REDLITE_LIB_PATH")
local lib

if lib_path then
    lib = ffi.load(lib_path)
else
    -- Try common locations
    local paths = {
        "libredlite_ffi",  -- Standard library name (searches LD_LIBRARY_PATH, etc.)
        "./libredlite_ffi.dylib",
        "./libredlite_ffi.so",
        "../../../crates/redlite-ffi/target/release/libredlite_ffi.dylib",
        "../../../crates/redlite-ffi/target/release/libredlite_ffi.so",
    }

    for _, path in ipairs(paths) do
        local ok, l = pcall(ffi.load, path)
        if ok then
            lib = l
            break
        end
    end

    if not lib then
        error("Could not load redlite library. Set REDLITE_LIB_PATH environment variable.")
    end
end

-- ============================================================================
-- Helper functions for memory management and type conversion
-- ============================================================================

local function get_last_error()
    local err = lib.redlite_last_error()
    if err == nil then
        return "unknown error"
    end
    local msg = ffi.string(err)
    lib.redlite_free_string(err)
    return msg
end

local function bytes_to_string(rb)
    if rb.data == nil or rb.len == 0 then
        return nil
    end
    local str = ffi.string(rb.data, rb.len)
    lib.redlite_free_bytes(rb)
    return str
end

local function string_array_to_table(arr)
    if arr.strings == nil or arr.len == 0 then
        return {}
    end
    local result = {}
    for i = 0, tonumber(arr.len) - 1 do
        result[i + 1] = ffi.string(arr.strings[i])
    end
    lib.redlite_free_string_array(arr)
    return result
end

local function bytes_array_to_table(arr)
    if arr.items == nil or arr.len == 0 then
        return {}
    end
    local result = {}
    for i = 0, tonumber(arr.len) - 1 do
        local item = arr.items[i]
        if item.data ~= nil and item.len > 0 then
            result[i + 1] = ffi.string(item.data, item.len)
        else
            result[i + 1] = nil
        end
    end
    lib.redlite_free_bytes_array(arr)
    return result
end

-- Convert Lua value to bytes (string or already bytes)
local function to_bytes(val)
    if val == nil then
        return nil, 0
    end
    local s = tostring(val)
    return s, #s
end

-- ============================================================================
-- Redlite Database Class
-- ============================================================================

local Redlite = {}
Redlite.__index = Redlite

-- Check if database is open
local function check_open(self)
    if self._handle == nil then
        error("database is closed")
    end
end

-- ============================================================================
-- Lifecycle Functions
-- ============================================================================

--- Open a database at the given path
--- @param path string Path to database file
--- @param cache_mb number|nil Optional cache size in MB (default: 64)
--- @return Redlite
function Redlite.open(path, cache_mb)
    local handle
    if cache_mb then
        handle = lib.redlite_open_with_cache(path, cache_mb)
    else
        handle = lib.redlite_open(path)
    end

    if handle == nil then
        error("failed to open database: " .. get_last_error())
    end

    local self = setmetatable({}, Redlite)
    self._handle = handle
    return self
end

--- Open an in-memory database
--- @return Redlite
function Redlite.open_memory()
    local handle = lib.redlite_open_memory()
    if handle == nil then
        error("failed to open memory database: " .. get_last_error())
    end

    local self = setmetatable({}, Redlite)
    self._handle = handle
    return self
end

--- Close the database
function Redlite:close()
    if self._handle ~= nil then
        lib.redlite_close(self._handle)
        self._handle = nil
    end
end

--- Get library version
--- @return string
function Redlite.version()
    local v = lib.redlite_version()
    local result = ffi.string(v)
    lib.redlite_free_string(v)
    return result
end

-- ============================================================================
-- String Commands
-- ============================================================================

--- GET key
--- @param key string
--- @return string|nil
function Redlite:get(key)
    check_open(self)
    local result = lib.redlite_get(self._handle, key)
    return bytes_to_string(result)
end

--- SET key value [ttl_seconds]
--- @param key string
--- @param value string
--- @param ttl number|nil Optional TTL in seconds
function Redlite:set(key, value, ttl)
    check_open(self)
    local data, len = to_bytes(value)
    local result = lib.redlite_set(self._handle, key, data, len, ttl or 0)
    if result < 0 then
        error("SET failed: " .. get_last_error())
    end
end

--- SETEX key seconds value
--- @param key string
--- @param seconds number
--- @param value string
function Redlite:setex(key, seconds, value)
    check_open(self)
    local data, len = to_bytes(value)
    local result = lib.redlite_setex(self._handle, key, seconds, data, len)
    if result < 0 then
        error("SETEX failed: " .. get_last_error())
    end
end

--- PSETEX key milliseconds value
--- @param key string
--- @param milliseconds number
--- @param value string
function Redlite:psetex(key, milliseconds, value)
    check_open(self)
    local data, len = to_bytes(value)
    local result = lib.redlite_psetex(self._handle, key, milliseconds, data, len)
    if result < 0 then
        error("PSETEX failed: " .. get_last_error())
    end
end

--- GETDEL key
--- @param key string
--- @return string|nil
function Redlite:getdel(key)
    check_open(self)
    local result = lib.redlite_getdel(self._handle, key)
    return bytes_to_string(result)
end

--- APPEND key value
--- @param key string
--- @param value string
--- @return number New length of the string
function Redlite:append(key, value)
    check_open(self)
    local data, len = to_bytes(value)
    local result = lib.redlite_append(self._handle, key, data, len)
    if result < 0 then
        error("APPEND failed: " .. get_last_error())
    end
    return tonumber(result)
end

--- STRLEN key
--- @param key string
--- @return number
function Redlite:strlen(key)
    check_open(self)
    local result = lib.redlite_strlen(self._handle, key)
    return tonumber(result)
end

--- GETRANGE key start end
--- @param key string
--- @param start number
--- @param stop number
--- @return string
function Redlite:getrange(key, start, stop)
    check_open(self)
    local result = lib.redlite_getrange(self._handle, key, start, stop)
    return bytes_to_string(result) or ""
end

--- SETRANGE key offset value
--- @param key string
--- @param offset number
--- @param value string
--- @return number New length of the string
function Redlite:setrange(key, offset, value)
    check_open(self)
    local data, len = to_bytes(value)
    local result = lib.redlite_setrange(self._handle, key, offset, data, len)
    if result < 0 then
        error("SETRANGE failed: " .. get_last_error())
    end
    return tonumber(result)
end

--- INCR key
--- @param key string
--- @return number
function Redlite:incr(key)
    check_open(self)
    local result = lib.redlite_incr(self._handle, key)
    return tonumber(result)
end

--- DECR key
--- @param key string
--- @return number
function Redlite:decr(key)
    check_open(self)
    local result = lib.redlite_decr(self._handle, key)
    return tonumber(result)
end

--- INCRBY key increment
--- @param key string
--- @param increment number
--- @return number
function Redlite:incrby(key, increment)
    check_open(self)
    local result = lib.redlite_incrby(self._handle, key, increment)
    return tonumber(result)
end

--- DECRBY key decrement
--- @param key string
--- @param decrement number
--- @return number
function Redlite:decrby(key, decrement)
    check_open(self)
    local result = lib.redlite_decrby(self._handle, key, decrement)
    return tonumber(result)
end

--- INCRBYFLOAT key increment
--- @param key string
--- @param increment number
--- @return number
function Redlite:incrbyfloat(key, increment)
    check_open(self)
    local result = lib.redlite_incrbyfloat(self._handle, key, increment)
    if result == nil then
        error("INCRBYFLOAT failed: " .. get_last_error())
    end
    local str = ffi.string(result)
    lib.redlite_free_string(result)
    return tonumber(str)
end

--- MGET key [key ...]
--- @param ... string Keys to get
--- @return table Array of values (nil for missing keys)
function Redlite:mget(...)
    check_open(self)
    local keys = {...}
    if #keys == 0 then
        return {}
    end

    -- Build C string array
    local c_keys = ffi.new("const char*[?]", #keys)
    for i, key in ipairs(keys) do
        c_keys[i - 1] = key
    end

    local result = lib.redlite_mget(self._handle, c_keys, #keys)
    return bytes_array_to_table(result)
end

--- MSET key value [key value ...]
--- @param mapping table Key-value pairs
function Redlite:mset(mapping)
    check_open(self)
    local count = 0
    for _ in pairs(mapping) do count = count + 1 end
    if count == 0 then return end

    local pairs_arr = ffi.new("RedliteKV[?]", count)
    local i = 0
    for key, value in pairs(mapping) do
        local data, len = to_bytes(value)
        pairs_arr[i].key = key
        pairs_arr[i].value = ffi.cast("const uint8_t*", data)
        pairs_arr[i].value_len = len
        i = i + 1
    end

    local result = lib.redlite_mset(self._handle, pairs_arr, count)
    if result < 0 then
        error("MSET failed: " .. get_last_error())
    end
end

-- ============================================================================
-- Key Commands
-- ============================================================================

--- DEL key [key ...]
--- @param ... string Keys to delete
--- @return number Number of keys deleted
function Redlite:del(...)
    check_open(self)
    local keys = {...}
    if #keys == 0 then
        return 0
    end

    local c_keys = ffi.new("const char*[?]", #keys)
    for i, key in ipairs(keys) do
        c_keys[i - 1] = key
    end

    local result = lib.redlite_del(self._handle, c_keys, #keys)
    return tonumber(result)
end

--- EXISTS key [key ...]
--- @param ... string Keys to check
--- @return number Number of keys that exist
function Redlite:exists(...)
    check_open(self)
    local keys = {...}
    if #keys == 0 then
        return 0
    end

    local c_keys = ffi.new("const char*[?]", #keys)
    for i, key in ipairs(keys) do
        c_keys[i - 1] = key
    end

    local result = lib.redlite_exists(self._handle, c_keys, #keys)
    return tonumber(result)
end

--- TYPE key
--- @param key string
--- @return string Type of the key ("none", "string", "list", "set", "zset", "hash")
function Redlite:type(key)
    check_open(self)
    local result = lib.redlite_type(self._handle, key)
    if result == nil then
        return "none"
    end
    local t = ffi.string(result)
    lib.redlite_free_string(result)
    return t
end

--- TTL key
--- @param key string
--- @return number TTL in seconds (-2 if key doesn't exist, -1 if no TTL)
function Redlite:ttl(key)
    check_open(self)
    return tonumber(lib.redlite_ttl(self._handle, key))
end

--- PTTL key
--- @param key string
--- @return number TTL in milliseconds
function Redlite:pttl(key)
    check_open(self)
    return tonumber(lib.redlite_pttl(self._handle, key))
end

--- EXPIRE key seconds
--- @param key string
--- @param seconds number
--- @return boolean True if timeout was set
function Redlite:expire(key, seconds)
    check_open(self)
    local result = lib.redlite_expire(self._handle, key, seconds)
    return result == 1
end

--- PEXPIRE key milliseconds
--- @param key string
--- @param milliseconds number
--- @return boolean True if timeout was set
function Redlite:pexpire(key, milliseconds)
    check_open(self)
    local result = lib.redlite_pexpire(self._handle, key, milliseconds)
    return result == 1
end

--- EXPIREAT key unix_timestamp
--- @param key string
--- @param unix_seconds number
--- @return boolean True if timeout was set
function Redlite:expireat(key, unix_seconds)
    check_open(self)
    local result = lib.redlite_expireat(self._handle, key, unix_seconds)
    return result == 1
end

--- PEXPIREAT key unix_timestamp_ms
--- @param key string
--- @param unix_ms number
--- @return boolean True if timeout was set
function Redlite:pexpireat(key, unix_ms)
    check_open(self)
    local result = lib.redlite_pexpireat(self._handle, key, unix_ms)
    return result == 1
end

--- PERSIST key
--- @param key string
--- @return boolean True if timeout was removed
function Redlite:persist(key)
    check_open(self)
    local result = lib.redlite_persist(self._handle, key)
    return result == 1
end

--- RENAME key newkey
--- @param key string
--- @param newkey string
function Redlite:rename(key, newkey)
    check_open(self)
    local result = lib.redlite_rename(self._handle, key, newkey)
    if result < 0 then
        error("RENAME failed: " .. get_last_error())
    end
end

--- RENAMENX key newkey
--- @param key string
--- @param newkey string
--- @return boolean True if renamed, false if newkey already exists
function Redlite:renamenx(key, newkey)
    check_open(self)
    local result = lib.redlite_renamenx(self._handle, key, newkey)
    if result < 0 then
        error("RENAMENX failed: " .. get_last_error())
    end
    return result == 1
end

--- KEYS pattern
--- @param pattern string Glob-style pattern
--- @return table Array of matching keys
function Redlite:keys(pattern)
    check_open(self)
    local result = lib.redlite_keys(self._handle, pattern)
    return string_array_to_table(result)
end

--- DBSIZE
--- @return number Number of keys in the database
function Redlite:dbsize()
    check_open(self)
    return tonumber(lib.redlite_dbsize(self._handle))
end

--- FLUSHDB
function Redlite:flushdb()
    check_open(self)
    local result = lib.redlite_flushdb(self._handle)
    if result < 0 then
        error("FLUSHDB failed: " .. get_last_error())
    end
end

--- SELECT db
--- @param db_num number Database number (0-15)
function Redlite:select(db_num)
    check_open(self)
    local result = lib.redlite_select(self._handle, db_num)
    if result < 0 then
        error("SELECT failed: " .. get_last_error())
    end
end

-- ============================================================================
-- Hash Commands
-- ============================================================================

--- HSET key field value [field value ...]
--- Can be called as:
---   hset(key, field, value)
---   hset(key, {field1 = value1, field2 = value2})
--- @return number Number of fields added (not updated)
function Redlite:hset(key, field_or_mapping, value)
    check_open(self)

    local mapping
    if type(field_or_mapping) == "table" then
        mapping = field_or_mapping
    else
        mapping = {[field_or_mapping] = value}
    end

    local count = 0
    for _ in pairs(mapping) do count = count + 1 end
    if count == 0 then return 0 end

    local fields = ffi.new("const char*[?]", count)
    local values = ffi.new("RedliteBytes[?]", count)

    local i = 0
    for f, v in pairs(mapping) do
        local data, len = to_bytes(v)
        fields[i] = f
        values[i].data = ffi.cast("uint8_t*", data)
        values[i].len = len
        i = i + 1
    end

    local result = lib.redlite_hset(self._handle, key, fields, values, count)
    if result < 0 then
        error("HSET failed: " .. get_last_error())
    end
    return tonumber(result)
end

--- HGET key field
--- @param key string
--- @param field string
--- @return string|nil
function Redlite:hget(key, field)
    check_open(self)
    local result = lib.redlite_hget(self._handle, key, field)
    return bytes_to_string(result)
end

--- HDEL key field [field ...]
--- @param key string
--- @param ... string Fields to delete
--- @return number Number of fields deleted
function Redlite:hdel(key, ...)
    check_open(self)
    local fields = {...}
    if #fields == 0 then return 0 end

    local c_fields = ffi.new("const char*[?]", #fields)
    for i, f in ipairs(fields) do
        c_fields[i - 1] = f
    end

    local result = lib.redlite_hdel(self._handle, key, c_fields, #fields)
    return tonumber(result)
end

--- HEXISTS key field
--- @param key string
--- @param field string
--- @return boolean
function Redlite:hexists(key, field)
    check_open(self)
    local result = lib.redlite_hexists(self._handle, key, field)
    return result == 1
end

--- HLEN key
--- @param key string
--- @return number
function Redlite:hlen(key)
    check_open(self)
    return tonumber(lib.redlite_hlen(self._handle, key))
end

--- HKEYS key
--- @param key string
--- @return table Array of field names
function Redlite:hkeys(key)
    check_open(self)
    local result = lib.redlite_hkeys(self._handle, key)
    return string_array_to_table(result)
end

--- HVALS key
--- @param key string
--- @return table Array of values
function Redlite:hvals(key)
    check_open(self)
    local result = lib.redlite_hvals(self._handle, key)
    return bytes_array_to_table(result)
end

--- HINCRBY key field increment
--- @param key string
--- @param field string
--- @param increment number
--- @return number
function Redlite:hincrby(key, field, increment)
    check_open(self)
    local result = lib.redlite_hincrby(self._handle, key, field, increment)
    return tonumber(result)
end

--- HGETALL key
--- @param key string
--- @return table Dictionary of field-value pairs
function Redlite:hgetall(key)
    check_open(self)
    local result = lib.redlite_hgetall(self._handle, key)
    local flat = bytes_array_to_table(result)

    -- Convert flat array to dictionary
    local hash = {}
    for i = 1, #flat, 2 do
        if flat[i] and flat[i + 1] then
            hash[flat[i]] = flat[i + 1]
        end
    end
    return hash
end

--- HMGET key field [field ...]
--- @param key string
--- @param ... string Fields to get
--- @return table Array of values (nil for missing fields)
function Redlite:hmget(key, ...)
    check_open(self)
    local fields = {...}
    if #fields == 0 then return {} end

    local c_fields = ffi.new("const char*[?]", #fields)
    for i, f in ipairs(fields) do
        c_fields[i - 1] = f
    end

    local result = lib.redlite_hmget(self._handle, key, c_fields, #fields)
    return bytes_array_to_table(result)
end

-- ============================================================================
-- List Commands
-- ============================================================================

--- LPUSH key value [value ...]
--- @param key string
--- @param ... string Values to push
--- @return number New length of list
function Redlite:lpush(key, ...)
    check_open(self)
    local values = {...}
    if #values == 0 then return 0 end

    local c_values = ffi.new("RedliteBytes[?]", #values)
    for i, v in ipairs(values) do
        local data, len = to_bytes(v)
        c_values[i - 1].data = ffi.cast("uint8_t*", data)
        c_values[i - 1].len = len
    end

    local result = lib.redlite_lpush(self._handle, key, c_values, #values)
    return tonumber(result)
end

--- RPUSH key value [value ...]
--- @param key string
--- @param ... string Values to push
--- @return number New length of list
function Redlite:rpush(key, ...)
    check_open(self)
    local values = {...}
    if #values == 0 then return 0 end

    local c_values = ffi.new("RedliteBytes[?]", #values)
    for i, v in ipairs(values) do
        local data, len = to_bytes(v)
        c_values[i - 1].data = ffi.cast("uint8_t*", data)
        c_values[i - 1].len = len
    end

    local result = lib.redlite_rpush(self._handle, key, c_values, #values)
    return tonumber(result)
end

--- LPOP key [count]
--- @param key string
--- @param count number|nil Number of elements to pop (default: 1)
--- @return string|table|nil Single value if count=1, array otherwise
function Redlite:lpop(key, count)
    check_open(self)
    count = count or 1
    local result = lib.redlite_lpop(self._handle, key, count)
    local values = bytes_array_to_table(result)
    if count == 1 then
        return values[1]
    end
    return values
end

--- RPOP key [count]
--- @param key string
--- @param count number|nil Number of elements to pop (default: 1)
--- @return string|table|nil Single value if count=1, array otherwise
function Redlite:rpop(key, count)
    check_open(self)
    count = count or 1
    local result = lib.redlite_rpop(self._handle, key, count)
    local values = bytes_array_to_table(result)
    if count == 1 then
        return values[1]
    end
    return values
end

--- LLEN key
--- @param key string
--- @return number
function Redlite:llen(key)
    check_open(self)
    return tonumber(lib.redlite_llen(self._handle, key))
end

--- LRANGE key start stop
--- @param key string
--- @param start number
--- @param stop number
--- @return table Array of values
function Redlite:lrange(key, start, stop)
    check_open(self)
    local result = lib.redlite_lrange(self._handle, key, start, stop)
    return bytes_array_to_table(result)
end

--- LINDEX key index
--- @param key string
--- @param index number
--- @return string|nil
function Redlite:lindex(key, index)
    check_open(self)
    local result = lib.redlite_lindex(self._handle, key, index)
    return bytes_to_string(result)
end

--- LPUSHX key value [value ...]
--- Pushes values to list only if key exists
--- @param key string
--- @param ... string Values to push
--- @return number New length of list (0 if key doesn't exist)
function Redlite:lpushx(key, ...)
    check_open(self)
    local values = {...}
    if #values == 0 then return 0 end

    local c_values = ffi.new("RedliteBytes[?]", #values)
    for i, v in ipairs(values) do
        local data, len = to_bytes(v)
        c_values[i - 1].data = ffi.cast("uint8_t*", data)
        c_values[i - 1].len = len
    end

    local result = lib.redlite_lpushx(self._handle, key, c_values, #values)
    return tonumber(result)
end

--- RPUSHX key value [value ...]
--- Pushes values to list only if key exists
--- @param key string
--- @param ... string Values to push
--- @return number New length of list (0 if key doesn't exist)
function Redlite:rpushx(key, ...)
    check_open(self)
    local values = {...}
    if #values == 0 then return 0 end

    local c_values = ffi.new("RedliteBytes[?]", #values)
    for i, v in ipairs(values) do
        local data, len = to_bytes(v)
        c_values[i - 1].data = ffi.cast("uint8_t*", data)
        c_values[i - 1].len = len
    end

    local result = lib.redlite_rpushx(self._handle, key, c_values, #values)
    return tonumber(result)
end

--- LMOVE source destination LEFT|RIGHT LEFT|RIGHT
--- Atomically move element from source to destination
--- @param source string Source list key
--- @param destination string Destination list key
--- @param wherefrom string "LEFT" or "RIGHT"
--- @param whereto string "LEFT" or "RIGHT"
--- @return string|nil Moved element, or nil if source is empty
function Redlite:lmove(source, destination, wherefrom, whereto)
    check_open(self)
    local from_left = (wherefrom:upper() == "LEFT") and 0 or 1
    local to_left = (whereto:upper() == "LEFT") and 0 or 1
    local result = lib.redlite_lmove(self._handle, source, destination, from_left, to_left)
    return bytes_to_string(result)
end

--- LPOS key element [RANK rank] [COUNT count] [MAXLEN maxlen]
--- Find position(s) of element in list
--- @param key string
--- @param element string Element to find
--- @param opts table|nil Options: {rank=number, count=number, maxlen=number}
--- @return table Array of positions
function Redlite:lpos(key, element, opts)
    check_open(self)
    opts = opts or {}
    local data, len = to_bytes(element)
    local rank = opts.rank or 0
    local count = opts.count or 0
    local maxlen = opts.maxlen or 0

    local result = lib.redlite_lpos(self._handle, key, data, len, rank, count, maxlen)
    local positions = {}
    if result.items ~= nil and result.len > 0 then
        for i = 0, tonumber(result.len) - 1 do
            local item = result.items[i]
            if item.data ~= nil and item.len > 0 then
                local pos_str = ffi.string(item.data, item.len)
                local pos = tonumber(pos_str)
                if pos then
                    table.insert(positions, pos)
                end
            end
        end
    end
    lib.redlite_free_bytes_array(result)
    return positions
end

-- ============================================================================
-- Set Commands
-- ============================================================================

--- SADD key member [member ...]
--- @param key string
--- @param ... string Members to add
--- @return number Number of members added
function Redlite:sadd(key, ...)
    check_open(self)
    local members = {...}
    if #members == 0 then return 0 end

    local c_members = ffi.new("RedliteBytes[?]", #members)
    for i, m in ipairs(members) do
        local data, len = to_bytes(m)
        c_members[i - 1].data = ffi.cast("uint8_t*", data)
        c_members[i - 1].len = len
    end

    local result = lib.redlite_sadd(self._handle, key, c_members, #members)
    return tonumber(result)
end

--- SREM key member [member ...]
--- @param key string
--- @param ... string Members to remove
--- @return number Number of members removed
function Redlite:srem(key, ...)
    check_open(self)
    local members = {...}
    if #members == 0 then return 0 end

    local c_members = ffi.new("RedliteBytes[?]", #members)
    for i, m in ipairs(members) do
        local data, len = to_bytes(m)
        c_members[i - 1].data = ffi.cast("uint8_t*", data)
        c_members[i - 1].len = len
    end

    local result = lib.redlite_srem(self._handle, key, c_members, #members)
    return tonumber(result)
end

--- SMEMBERS key
--- @param key string
--- @return table Array of members
function Redlite:smembers(key)
    check_open(self)
    local result = lib.redlite_smembers(self._handle, key)
    return bytes_array_to_table(result)
end

--- SISMEMBER key member
--- @param key string
--- @param member string
--- @return boolean
function Redlite:sismember(key, member)
    check_open(self)
    local data, len = to_bytes(member)
    local result = lib.redlite_sismember(self._handle, key, data, len)
    return result == 1
end

--- SCARD key
--- @param key string
--- @return number
function Redlite:scard(key)
    check_open(self)
    return tonumber(lib.redlite_scard(self._handle, key))
end

-- ============================================================================
-- Sorted Set Commands
-- ============================================================================

--- ZADD key score member [score member ...]
--- Can be called as:
---   zadd(key, score, member)
---   zadd(key, {member1 = score1, member2 = score2})
---   zadd(key, {{score1, member1}, {score2, member2}})
--- @return number Number of members added
function Redlite:zadd(key, score_or_mapping, member)
    check_open(self)

    local members_arr = {}

    if type(score_or_mapping) == "number" then
        -- zadd(key, score, member)
        table.insert(members_arr, {score = score_or_mapping, member = member})
    elseif type(score_or_mapping) == "table" then
        if #score_or_mapping > 0 and type(score_or_mapping[1]) == "table" then
            -- zadd(key, {{score1, member1}, {score2, member2}})
            for _, pair in ipairs(score_or_mapping) do
                table.insert(members_arr, {score = pair[1], member = pair[2]})
            end
        else
            -- zadd(key, {member1 = score1, member2 = score2})
            for m, s in pairs(score_or_mapping) do
                table.insert(members_arr, {score = s, member = m})
            end
        end
    end

    if #members_arr == 0 then return 0 end

    local c_members = ffi.new("RedliteZMember[?]", #members_arr)
    for i, m in ipairs(members_arr) do
        local data, len = to_bytes(m.member)
        c_members[i - 1].score = m.score
        c_members[i - 1].member = ffi.cast("const uint8_t*", data)
        c_members[i - 1].member_len = len
    end

    local result = lib.redlite_zadd(self._handle, key, c_members, #members_arr)
    return tonumber(result)
end

--- ZREM key member [member ...]
--- @param key string
--- @param ... string Members to remove
--- @return number Number of members removed
function Redlite:zrem(key, ...)
    check_open(self)
    local members = {...}
    if #members == 0 then return 0 end

    local c_members = ffi.new("RedliteBytes[?]", #members)
    for i, m in ipairs(members) do
        local data, len = to_bytes(m)
        c_members[i - 1].data = ffi.cast("uint8_t*", data)
        c_members[i - 1].len = len
    end

    local result = lib.redlite_zrem(self._handle, key, c_members, #members)
    return tonumber(result)
end

--- ZSCORE key member
--- @param key string
--- @param member string
--- @return number|nil Score, or nil if member doesn't exist
function Redlite:zscore(key, member)
    check_open(self)
    local data, len = to_bytes(member)
    local result = lib.redlite_zscore(self._handle, key, data, len)
    -- NaN indicates not found
    if result ~= result then  -- NaN check
        return nil
    end
    return result
end

--- ZCARD key
--- @param key string
--- @return number
function Redlite:zcard(key)
    check_open(self)
    return tonumber(lib.redlite_zcard(self._handle, key))
end

--- ZCOUNT key min max
--- @param key string
--- @param min number
--- @param max number
--- @return number
function Redlite:zcount(key, min, max)
    check_open(self)
    return tonumber(lib.redlite_zcount(self._handle, key, min, max))
end

--- ZINCRBY key increment member
--- @param key string
--- @param increment number
--- @param member string
--- @return number New score
function Redlite:zincrby(key, increment, member)
    check_open(self)
    local data, len = to_bytes(member)
    local result = lib.redlite_zincrby(self._handle, key, increment, data, len)
    return result
end

--- ZRANGE key start stop [WITHSCORES]
--- @param key string
--- @param start number
--- @param stop number
--- @param with_scores boolean|nil Include scores in result
--- @return table Array of members, or array of {member, score} pairs if with_scores
function Redlite:zrange(key, start, stop, with_scores)
    check_open(self)
    local ws = with_scores and 1 or 0
    local result = lib.redlite_zrange(self._handle, key, start, stop, ws)
    local flat = bytes_array_to_table(result)

    if with_scores then
        -- Convert flat array to pairs
        local pairs = {}
        for i = 1, #flat, 2 do
            if flat[i] then
                table.insert(pairs, {flat[i], tonumber(flat[i + 1])})
            end
        end
        return pairs
    end

    return flat
end

--- ZREVRANGE key start stop [WITHSCORES]
--- @param key string
--- @param start number
--- @param stop number
--- @param with_scores boolean|nil Include scores in result
--- @return table Array of members, or array of {member, score} pairs if with_scores
function Redlite:zrevrange(key, start, stop, with_scores)
    check_open(self)
    local ws = with_scores and 1 or 0
    local result = lib.redlite_zrevrange(self._handle, key, start, stop, ws)
    local flat = bytes_array_to_table(result)

    if with_scores then
        -- Convert flat array to pairs
        local pairs = {}
        for i = 1, #flat, 2 do
            if flat[i] then
                table.insert(pairs, {flat[i], tonumber(flat[i + 1])})
            end
        end
        return pairs
    end

    return flat
end

--- ZINTERSTORE destination numkeys key [key ...] [WEIGHTS weight ...] [AGGREGATE SUM|MIN|MAX]
--- Intersect sorted sets and store result in destination
--- @param destination string Destination key
--- @param keys table Array of source keys
--- @param opts table|nil Options: {weights=table, aggregate=string}
--- @return number Number of elements in the resulting sorted set
function Redlite:zinterstore(destination, keys, opts)
    check_open(self)
    if #keys == 0 then return 0 end

    opts = opts or {}

    -- Build C string array for keys
    local c_keys = ffi.new("const char*[?]", #keys)
    for i, key in ipairs(keys) do
        c_keys[i - 1] = key
    end

    -- Build weights array if provided
    local c_weights = nil
    local weights_len = 0
    if opts.weights and #opts.weights > 0 then
        weights_len = #opts.weights
        c_weights = ffi.new("double[?]", weights_len)
        for i, w in ipairs(opts.weights) do
            c_weights[i - 1] = w
        end
    end

    -- Aggregate function (NULL for default SUM)
    local aggregate = opts.aggregate

    local result = lib.redlite_zinterstore(
        self._handle,
        destination,
        c_keys,
        #keys,
        c_weights,
        weights_len,
        aggregate
    )

    if result < 0 then
        error("ZINTERSTORE failed: " .. get_last_error())
    end
    return tonumber(result)
end

--- ZUNIONSTORE destination numkeys key [key ...] [WEIGHTS weight ...] [AGGREGATE SUM|MIN|MAX]
--- Union sorted sets and store result in destination
--- @param destination string Destination key
--- @param keys table Array of source keys
--- @param opts table|nil Options: {weights=table, aggregate=string}
--- @return number Number of elements in the resulting sorted set
function Redlite:zunionstore(destination, keys, opts)
    check_open(self)
    if #keys == 0 then return 0 end

    opts = opts or {}

    -- Build C string array for keys
    local c_keys = ffi.new("const char*[?]", #keys)
    for i, key in ipairs(keys) do
        c_keys[i - 1] = key
    end

    -- Build weights array if provided
    local c_weights = nil
    local weights_len = 0
    if opts.weights and #opts.weights > 0 then
        weights_len = #opts.weights
        c_weights = ffi.new("double[?]", weights_len)
        for i, w in ipairs(opts.weights) do
            c_weights[i - 1] = w
        end
    end

    -- Aggregate function (NULL for default SUM)
    local aggregate = opts.aggregate

    local result = lib.redlite_zunionstore(
        self._handle,
        destination,
        c_keys,
        #keys,
        c_weights,
        weights_len,
        aggregate
    )

    if result < 0 then
        error("ZUNIONSTORE failed: " .. get_last_error())
    end
    return tonumber(result)
end

-- ============================================================================
-- Server Commands
-- ============================================================================

--- VACUUM - compact the database
--- @return number Bytes reclaimed
function Redlite:vacuum()
    check_open(self)
    return tonumber(lib.redlite_vacuum(self._handle))
end

-- ============================================================================
-- JSON Commands (ReJSON-compatible)
-- ============================================================================

--- JSON.SET key path value [NX|XX]
--- Set a JSON value at the given path
--- @param key string
--- @param path string JSON path ($ for root)
--- @param value string JSON-encoded value
--- @param opts table|nil Options: {nx=boolean, xx=boolean}
--- @return boolean True if set, false if NX/XX condition not met
function Redlite:json_set(key, path, value, opts)
    check_open(self)
    opts = opts or {}
    local nx = opts.nx and 1 or 0
    local xx = opts.xx and 1 or 0
    local result = lib.redlite_json_set(self._handle, key, path, value, nx, xx)
    if result < 0 then
        error("JSON.SET failed: " .. get_last_error())
    end
    return result == 1
end

--- JSON.GET key [path ...]
--- Get JSON values at the given paths
--- @param key string
--- @param ... string Paths to get (defaults to $ if none specified)
--- @return string|nil JSON-encoded result
function Redlite:json_get(key, ...)
    check_open(self)
    local paths = {...}
    if #paths == 0 then
        paths = {"$"}
    end

    local c_paths = ffi.new("const char*[?]", #paths)
    for i, p in ipairs(paths) do
        c_paths[i - 1] = p
    end

    local result = lib.redlite_json_get(self._handle, key, c_paths, #paths)
    if result == nil then
        return nil
    end
    local str = ffi.string(result)
    lib.redlite_free_string(result)
    return str
end

--- JSON.DEL key [path]
--- Delete JSON value at the given path
--- @param key string
--- @param path string|nil JSON path (defaults to $)
--- @return number Number of paths deleted
function Redlite:json_del(key, path)
    check_open(self)
    path = path or "$"
    local result = lib.redlite_json_del(self._handle, key, path)
    return tonumber(result)
end

--- JSON.TYPE key [path]
--- Get the type of JSON value at the given path
--- @param key string
--- @param path string|nil JSON path (defaults to $)
--- @return string|nil Type name or nil if not found
function Redlite:json_type(key, path)
    check_open(self)
    path = path or "$"
    local result = lib.redlite_json_type(self._handle, key, path)
    if result == nil then
        return nil
    end
    local str = ffi.string(result)
    lib.redlite_free_string(result)
    return str
end

--- JSON.NUMINCRBY key path increment
--- Increment a numeric JSON value
--- @param key string
--- @param path string JSON path
--- @param increment number Amount to increment
--- @return string|nil New value as JSON string, or nil on error
function Redlite:json_numincrby(key, path, increment)
    check_open(self)
    local result = lib.redlite_json_numincrby(self._handle, key, path, increment)
    if result == nil then
        return nil
    end
    local str = ffi.string(result)
    lib.redlite_free_string(result)
    return str
end

--- JSON.STRAPPEND key path value
--- Append to a string JSON value
--- @param key string
--- @param path string JSON path
--- @param value string String to append (JSON-encoded)
--- @return number New length of string
function Redlite:json_strappend(key, path, value)
    check_open(self)
    local result = lib.redlite_json_strappend(self._handle, key, path, value)
    if result < 0 then
        error("JSON.STRAPPEND failed: " .. get_last_error())
    end
    return tonumber(result)
end

--- JSON.STRLEN key [path]
--- Get length of a string JSON value
--- @param key string
--- @param path string|nil JSON path (defaults to $)
--- @return number Length of string
function Redlite:json_strlen(key, path)
    check_open(self)
    path = path or "$"
    local result = lib.redlite_json_strlen(self._handle, key, path)
    return tonumber(result)
end

--- JSON.ARRAPPEND key path value [value ...]
--- Append values to a JSON array
--- @param key string
--- @param path string JSON path
--- @param ... string JSON-encoded values to append
--- @return number New length of array
function Redlite:json_arrappend(key, path, ...)
    check_open(self)
    local values = {...}
    if #values == 0 then
        return 0
    end

    local c_values = ffi.new("const char*[?]", #values)
    for i, v in ipairs(values) do
        c_values[i - 1] = v
    end

    local result = lib.redlite_json_arrappend(self._handle, key, path, c_values, #values)
    if result < 0 then
        error("JSON.ARRAPPEND failed: " .. get_last_error())
    end
    return tonumber(result)
end

--- JSON.ARRLEN key [path]
--- Get length of a JSON array
--- @param key string
--- @param path string|nil JSON path (defaults to $)
--- @return number Length of array
function Redlite:json_arrlen(key, path)
    check_open(self)
    path = path or "$"
    local result = lib.redlite_json_arrlen(self._handle, key, path)
    return tonumber(result)
end

--- JSON.ARRPOP key [path [index]]
--- Pop an element from a JSON array
--- @param key string
--- @param path string|nil JSON path (defaults to $)
--- @param index number|nil Index to pop from (defaults to -1, last element)
--- @return string|nil Popped value as JSON string
function Redlite:json_arrpop(key, path, index)
    check_open(self)
    path = path or "$"
    index = index or -1
    local result = lib.redlite_json_arrpop(self._handle, key, path, index)
    if result == nil then
        return nil
    end
    local str = ffi.string(result)
    lib.redlite_free_string(result)
    return str
end

--- JSON.CLEAR key [path]
--- Clear container JSON values (arrays/objects become empty)
--- @param key string
--- @param path string|nil JSON path (defaults to $)
--- @return number Number of values cleared
function Redlite:json_clear(key, path)
    check_open(self)
    path = path or "$"
    local result = lib.redlite_json_clear(self._handle, key, path)
    return tonumber(result)
end

-- ============================================================================
-- History Commands
-- ============================================================================

--- Enable history tracking globally
--- @param retention_type string "unlimited", "time", or "count"
--- @param retention_value number|nil Value for time (ms) or count retention
function Redlite:history_enable_global(retention_type, retention_value)
    check_open(self)
    retention_type = retention_type or "unlimited"
    retention_value = retention_value or 0
    local result = lib.redlite_history_enable_global(self._handle, retention_type, retention_value)
    if result < 0 then
        error("HISTORY.ENABLE GLOBAL failed: " .. get_last_error())
    end
end

--- Enable history tracking for a specific database
--- @param db_num number Database number
--- @param retention_type string "unlimited", "time", or "count"
--- @param retention_value number|nil Value for time (ms) or count retention
function Redlite:history_enable_database(db_num, retention_type, retention_value)
    check_open(self)
    retention_type = retention_type or "unlimited"
    retention_value = retention_value or 0
    local result = lib.redlite_history_enable_database(self._handle, db_num, retention_type, retention_value)
    if result < 0 then
        error("HISTORY.ENABLE DATABASE failed: " .. get_last_error())
    end
end

--- Enable history tracking for a specific key
--- @param key string Key to enable history for
--- @param retention_type string "unlimited", "time", or "count"
--- @param retention_value number|nil Value for time (ms) or count retention
function Redlite:history_enable_key(key, retention_type, retention_value)
    check_open(self)
    retention_type = retention_type or "unlimited"
    retention_value = retention_value or 0
    local result = lib.redlite_history_enable_key(self._handle, key, retention_type, retention_value)
    if result < 0 then
        error("HISTORY.ENABLE KEY failed: " .. get_last_error())
    end
end

--- Disable history tracking globally
function Redlite:history_disable_global()
    check_open(self)
    local result = lib.redlite_history_disable_global(self._handle)
    if result < 0 then
        error("HISTORY.DISABLE GLOBAL failed: " .. get_last_error())
    end
end

--- Disable history tracking for a specific database
--- @param db_num number Database number
function Redlite:history_disable_database(db_num)
    check_open(self)
    local result = lib.redlite_history_disable_database(self._handle, db_num)
    if result < 0 then
        error("HISTORY.DISABLE DATABASE failed: " .. get_last_error())
    end
end

--- Disable history tracking for a specific key
--- @param key string Key to disable history for
function Redlite:history_disable_key(key)
    check_open(self)
    local result = lib.redlite_history_disable_key(self._handle, key)
    if result < 0 then
        error("HISTORY.DISABLE KEY failed: " .. get_last_error())
    end
end

--- Check if history tracking is enabled for a key
--- @param key string Key to check
--- @return boolean True if history is enabled
function Redlite:is_history_enabled(key)
    check_open(self)
    local result = lib.redlite_is_history_enabled(self._handle, key)
    return result == 1
end

-- ============================================================================
-- FTS (Full-Text Search) Commands
-- ============================================================================

--- Enable FTS indexing globally
function Redlite:fts_enable_global()
    check_open(self)
    local result = lib.redlite_fts_enable_global(self._handle)
    if result < 0 then
        error("FTS.ENABLE GLOBAL failed: " .. get_last_error())
    end
end

--- Enable FTS indexing for a specific database
--- @param db_num number Database number
function Redlite:fts_enable_database(db_num)
    check_open(self)
    local result = lib.redlite_fts_enable_database(self._handle, db_num)
    if result < 0 then
        error("FTS.ENABLE DATABASE failed: " .. get_last_error())
    end
end

--- Enable FTS indexing for keys matching a pattern
--- @param pattern string Glob pattern
function Redlite:fts_enable_pattern(pattern)
    check_open(self)
    local result = lib.redlite_fts_enable_pattern(self._handle, pattern)
    if result < 0 then
        error("FTS.ENABLE PATTERN failed: " .. get_last_error())
    end
end

--- Enable FTS indexing for a specific key
--- @param key string Key to enable FTS for
function Redlite:fts_enable_key(key)
    check_open(self)
    local result = lib.redlite_fts_enable_key(self._handle, key)
    if result < 0 then
        error("FTS.ENABLE KEY failed: " .. get_last_error())
    end
end

--- Disable FTS indexing globally
function Redlite:fts_disable_global()
    check_open(self)
    local result = lib.redlite_fts_disable_global(self._handle)
    if result < 0 then
        error("FTS.DISABLE GLOBAL failed: " .. get_last_error())
    end
end

--- Disable FTS indexing for a specific database
--- @param db_num number Database number
function Redlite:fts_disable_database(db_num)
    check_open(self)
    local result = lib.redlite_fts_disable_database(self._handle, db_num)
    if result < 0 then
        error("FTS.DISABLE DATABASE failed: " .. get_last_error())
    end
end

--- Disable FTS indexing for keys matching a pattern
--- @param pattern string Glob pattern
function Redlite:fts_disable_pattern(pattern)
    check_open(self)
    local result = lib.redlite_fts_disable_pattern(self._handle, pattern)
    if result < 0 then
        error("FTS.DISABLE PATTERN failed: " .. get_last_error())
    end
end

--- Disable FTS indexing for a specific key
--- @param key string Key to disable FTS for
function Redlite:fts_disable_key(key)
    check_open(self)
    local result = lib.redlite_fts_disable_key(self._handle, key)
    if result < 0 then
        error("FTS.DISABLE KEY failed: " .. get_last_error())
    end
end

--- Check if FTS indexing is enabled for a key
--- @param key string Key to check
--- @return boolean True if FTS is enabled
function Redlite:is_fts_enabled(key)
    check_open(self)
    local result = lib.redlite_is_fts_enabled(self._handle, key)
    return result == 1
end

-- ============================================================================
-- KeyInfo Command
-- ============================================================================

--- KEYINFO - Get detailed information about a key
--- @param key string Key to get info for
--- @return table|nil Key info {type, ttl, created_at, updated_at} or nil if key doesn't exist
function Redlite:keyinfo(key)
    check_open(self)
    local info = lib.redlite_keyinfo(self._handle, key)

    if info.valid == 0 then
        lib.redlite_free_keyinfo(info)
        return nil
    end

    local result = {
        type = info.key_type ~= nil and ffi.string(info.key_type) or "none",
        ttl = tonumber(info.ttl),
        created_at = tonumber(info.created_at),
        updated_at = tonumber(info.updated_at)
    }

    lib.redlite_free_keyinfo(info)
    return result
end

-- ============================================================================
-- Module Export
-- ============================================================================

return Redlite
