/// Low-level C FFI bindings for Redlite.
///
/// This module provides direct access to the C API via @cImport.
/// For a Zig-idiomatic API, use the main `redlite` module instead.

pub const c = @cImport({
    @cInclude("redlite.h");
});

// Re-export C types for convenience
pub const RedliteDb = c.RedliteDb;
pub const RedliteBytes = c.RedliteBytes;
pub const RedliteStringArray = c.RedliteStringArray;
pub const RedliteBytesArray = c.RedliteBytesArray;
pub const RedliteKV = c.RedliteKV;
pub const RedliteZMember = c.RedliteZMember;

// Re-export C functions
pub const open = c.redlite_open;
pub const open_memory = c.redlite_open_memory;
pub const open_with_cache = c.redlite_open_with_cache;
pub const close = c.redlite_close;
pub const last_error = c.redlite_last_error;
pub const free_string = c.redlite_free_string;
pub const free_bytes = c.redlite_free_bytes;
pub const free_string_array = c.redlite_free_string_array;
pub const free_bytes_array = c.redlite_free_bytes_array;

// String commands
pub const get = c.redlite_get;
pub const set = c.redlite_set;
pub const setex = c.redlite_setex;
pub const psetex = c.redlite_psetex;
pub const getdel = c.redlite_getdel;
pub const append = c.redlite_append;
pub const strlen = c.redlite_strlen;
pub const getrange = c.redlite_getrange;
pub const setrange = c.redlite_setrange;
pub const incr = c.redlite_incr;
pub const decr = c.redlite_decr;
pub const incrby = c.redlite_incrby;
pub const decrby = c.redlite_decrby;
pub const incrbyfloat = c.redlite_incrbyfloat;
pub const mget = c.redlite_mget;
pub const mset = c.redlite_mset;

// Key commands
pub const del = c.redlite_del;
pub const exists = c.redlite_exists;
pub const @"type" = c.redlite_type;
pub const ttl = c.redlite_ttl;
pub const pttl = c.redlite_pttl;
pub const expire = c.redlite_expire;
pub const pexpire = c.redlite_pexpire;
pub const expireat = c.redlite_expireat;
pub const pexpireat = c.redlite_pexpireat;
pub const persist = c.redlite_persist;
pub const rename = c.redlite_rename;
pub const renamenx = c.redlite_renamenx;
pub const keys = c.redlite_keys;
pub const dbsize = c.redlite_dbsize;
pub const flushdb = c.redlite_flushdb;
pub const select = c.redlite_select;

// Hash commands
pub const hset = c.redlite_hset;
pub const hget = c.redlite_hget;
pub const hdel = c.redlite_hdel;
pub const hexists = c.redlite_hexists;
pub const hlen = c.redlite_hlen;
pub const hkeys = c.redlite_hkeys;
pub const hvals = c.redlite_hvals;
pub const hincrby = c.redlite_hincrby;
pub const hgetall = c.redlite_hgetall;
pub const hmget = c.redlite_hmget;

// List commands
pub const lpush = c.redlite_lpush;
pub const rpush = c.redlite_rpush;
pub const lpushx = c.redlite_lpushx;
pub const rpushx = c.redlite_rpushx;
pub const lpop = c.redlite_lpop;
pub const rpop = c.redlite_rpop;
pub const llen = c.redlite_llen;
pub const lrange = c.redlite_lrange;
pub const lindex = c.redlite_lindex;
pub const lmove = c.redlite_lmove;
pub const lpos = c.redlite_lpos;

// Set commands
pub const sadd = c.redlite_sadd;
pub const srem = c.redlite_srem;
pub const smembers = c.redlite_smembers;
pub const sismember = c.redlite_sismember;
pub const scard = c.redlite_scard;

// Sorted set commands
pub const zadd = c.redlite_zadd;
pub const zrem = c.redlite_zrem;
pub const zscore = c.redlite_zscore;
pub const zcard = c.redlite_zcard;
pub const zcount = c.redlite_zcount;
pub const zincrby = c.redlite_zincrby;
pub const zrange = c.redlite_zrange;
pub const zrevrange = c.redlite_zrevrange;
pub const zinterstore = c.redlite_zinterstore;
pub const zunionstore = c.redlite_zunionstore;

// Server commands
pub const vacuum = c.redlite_vacuum;
pub const version = c.redlite_version;

// KeyInfo command
pub const RedliteKeyInfo = c.RedliteKeyInfo;
pub const keyinfo = c.redlite_keyinfo;

// JSON commands
pub const json_set = c.redlite_json_set;
pub const json_get = c.redlite_json_get;
pub const json_del = c.redlite_json_del;
pub const json_type = c.redlite_json_type;
pub const json_numincrby = c.redlite_json_numincrby;
pub const json_strappend = c.redlite_json_strappend;
pub const json_strlen = c.redlite_json_strlen;
pub const json_arrappend = c.redlite_json_arrappend;
pub const json_arrlen = c.redlite_json_arrlen;
pub const json_arrpop = c.redlite_json_arrpop;
pub const json_clear = c.redlite_json_clear;

// History commands
pub const history_enable_global = c.redlite_history_enable_global;
pub const history_enable_db = c.redlite_history_enable_db;
pub const history_enable_key = c.redlite_history_enable_key;
pub const history_disable_global = c.redlite_history_disable_global;
pub const history_disable_db = c.redlite_history_disable_db;
pub const history_disable_key = c.redlite_history_disable_key;
pub const history_is_enabled = c.redlite_history_is_enabled;

// FTS commands
pub const fts_enable_global = c.redlite_fts_enable_global;
pub const fts_enable_db = c.redlite_fts_enable_db;
pub const fts_enable_pattern = c.redlite_fts_enable_pattern;
pub const fts_enable_key = c.redlite_fts_enable_key;
pub const fts_disable_global = c.redlite_fts_disable_global;
pub const fts_disable_db = c.redlite_fts_disable_db;
pub const fts_disable_pattern = c.redlite_fts_disable_pattern;
pub const fts_disable_key = c.redlite_fts_disable_key;
pub const fts_is_enabled = c.redlite_fts_is_enabled;
