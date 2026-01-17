//! Rustler NIF bindings for Redlite - Elixir SDK
//!
//! This module provides direct Rust -> Elixir bindings via Rustler NIFs,
//! enabling high-performance Redis-compatible operations in Elixir.

use redlite::Db as RedliteDb;
use rustler::{Atom, Binary, Encoder, Env, NifResult, NifStruct, OwnedBinary, ResourceArc, Term};
use std::sync::Mutex;
use std::time::Duration;

mod atoms {
    rustler::atoms! {
        ok,
        error,
        nil,
        // Key types
        string,
        list,
        set,
        zset,
        hash,
        stream,
        none,
        // Errors
        not_found,
        wrong_type,
        not_integer,
        not_float,
        syntax_error,
        invalid_argument,
    }
}

/// Resource wrapper for the database handle
pub struct DbResource {
    db: Mutex<RedliteDb>,
}

impl DbResource {
    fn new(db: RedliteDb) -> Self {
        Self { db: Mutex::new(db) }
    }
}

/// Sorted set member for ZADD
#[derive(NifStruct)]
#[module = "Redlite.ZMember"]
pub struct ZMember {
    score: f64,
    member: Binary<'static>,
}

/// SET options
#[derive(NifStruct)]
#[module = "Redlite.SetOptions"]
pub struct SetOptions {
    ex: Option<i64>,
    px: Option<i64>,
    nx: bool,
    xx: bool,
}

// Resource type initialization
fn load(env: Env, _info: Term) -> bool {
    rustler::resource!(DbResource, env);
    true
}

// Helper to convert Rust errors to Elixir errors
fn to_error<'a>(env: Env<'a>, e: impl std::fmt::Display) -> Term<'a> {
    (atoms::error(), e.to_string()).encode(env)
}

// Helper to create ok tuples
fn ok<'a, T: Encoder>(env: Env<'a>, value: T) -> Term<'a> {
    (atoms::ok(), value).encode(env)
}

// Helper to create binary from Vec<u8>
fn vec_to_binary<'a>(env: Env<'a>, data: Vec<u8>) -> Binary<'a> {
    let mut binary = OwnedBinary::new(data.len()).unwrap();
    binary.as_mut_slice().copy_from_slice(&data);
    binary.release(env)
}

// =============================================================================
// Lifecycle
// =============================================================================

/// Open a database at the given path
#[rustler::nif]
fn open(path: &str) -> NifResult<(Atom, ResourceArc<DbResource>)> {
    match RedliteDb::open(path) {
        Ok(db) => Ok((atoms::ok(), ResourceArc::new(DbResource::new(db)))),
        Err(e) => Err(rustler::Error::Term(Box::new(e.to_string()))),
    }
}

/// Open an in-memory database
#[rustler::nif]
fn open_memory() -> NifResult<(Atom, ResourceArc<DbResource>)> {
    match RedliteDb::open_memory() {
        Ok(db) => Ok((atoms::ok(), ResourceArc::new(DbResource::new(db)))),
        Err(e) => Err(rustler::Error::Term(Box::new(e.to_string()))),
    }
}

/// Open a database with custom cache size
#[rustler::nif]
fn open_with_cache(path: &str, cache_mb: i64) -> NifResult<(Atom, ResourceArc<DbResource>)> {
    match RedliteDb::open_with_cache(path, cache_mb) {
        Ok(db) => Ok((atoms::ok(), ResourceArc::new(DbResource::new(db)))),
        Err(e) => Err(rustler::Error::Term(Box::new(e.to_string()))),
    }
}

// =============================================================================
// String Commands
// =============================================================================

/// GET key
#[rustler::nif(schedule = "DirtyCpu")]
fn get<'a>(env: Env<'a>, db: ResourceArc<DbResource>, key: &str) -> NifResult<Term<'a>> {
    let guard = db.db.lock().map_err(|_| rustler::Error::Atom("lock_error"))?;
    match guard.get(key) {
        Ok(Some(v)) => Ok(ok(env, vec_to_binary(env, v))),
        Ok(None) => Ok(ok(env, atoms::nil())),
        Err(e) => Ok(to_error(env, e)),
    }
}

/// SET key value [ttl_seconds]
#[rustler::nif(schedule = "DirtyCpu")]
fn set<'a>(
    env: Env<'a>,
    db: ResourceArc<DbResource>,
    key: &str,
    value: Binary,
    ttl_seconds: Option<i64>,
) -> NifResult<Term<'a>> {
    let guard = db.db.lock().map_err(|_| rustler::Error::Atom("lock_error"))?;
    let ttl = ttl_seconds.map(|s| Duration::from_secs(s as u64));
    match guard.set(key, value.as_slice(), ttl) {
        Ok(()) => Ok(ok(env, true)),
        Err(e) => Ok(to_error(env, e)),
    }
}

/// SET with options (NX, XX, EX, PX)
#[rustler::nif(schedule = "DirtyCpu")]
fn set_opts<'a>(
    env: Env<'a>,
    db: ResourceArc<DbResource>,
    key: &str,
    value: Binary,
    opts: SetOptions,
) -> NifResult<Term<'a>> {
    let guard = db.db.lock().map_err(|_| rustler::Error::Atom("lock_error"))?;
    let ttl = if let Some(ex) = opts.ex {
        Some(Duration::from_secs(ex as u64))
    } else if let Some(px) = opts.px {
        Some(Duration::from_millis(px as u64))
    } else {
        None
    };
    let options = redlite::SetOptions {
        ttl,
        nx: opts.nx,
        xx: opts.xx,
    };
    match guard.set_opts(key, value.as_slice(), options) {
        Ok(true) => Ok(ok(env, true)),
        Ok(false) => Ok(ok(env, false)),
        Err(e) => Ok(to_error(env, e)),
    }
}

/// SETEX key seconds value
#[rustler::nif(schedule = "DirtyCpu")]
fn setex<'a>(
    env: Env<'a>,
    db: ResourceArc<DbResource>,
    key: &str,
    seconds: i64,
    value: Binary,
) -> NifResult<Term<'a>> {
    let guard = db.db.lock().map_err(|_| rustler::Error::Atom("lock_error"))?;
    match guard.setex(key, seconds, value.as_slice()) {
        Ok(()) => Ok(ok(env, true)),
        Err(e) => Ok(to_error(env, e)),
    }
}

/// PSETEX key milliseconds value
#[rustler::nif(schedule = "DirtyCpu")]
fn psetex<'a>(
    env: Env<'a>,
    db: ResourceArc<DbResource>,
    key: &str,
    milliseconds: i64,
    value: Binary,
) -> NifResult<Term<'a>> {
    let guard = db.db.lock().map_err(|_| rustler::Error::Atom("lock_error"))?;
    match guard.psetex(key, milliseconds, value.as_slice()) {
        Ok(()) => Ok(ok(env, true)),
        Err(e) => Ok(to_error(env, e)),
    }
}

/// GETDEL key
#[rustler::nif(schedule = "DirtyCpu")]
fn getdel<'a>(env: Env<'a>, db: ResourceArc<DbResource>, key: &str) -> NifResult<Term<'a>> {
    let guard = db.db.lock().map_err(|_| rustler::Error::Atom("lock_error"))?;
    match guard.getdel(key) {
        Ok(Some(v)) => Ok(ok(env, vec_to_binary(env, v))),
        Ok(None) => Ok(ok(env, atoms::nil())),
        Err(e) => Ok(to_error(env, e)),
    }
}

/// APPEND key value
#[rustler::nif(schedule = "DirtyCpu")]
fn append<'a>(
    env: Env<'a>,
    db: ResourceArc<DbResource>,
    key: &str,
    value: Binary,
) -> NifResult<Term<'a>> {
    let guard = db.db.lock().map_err(|_| rustler::Error::Atom("lock_error"))?;
    match guard.append(key, value.as_slice()) {
        Ok(len) => Ok(ok(env, len)),
        Err(e) => Ok(to_error(env, e)),
    }
}

/// STRLEN key
#[rustler::nif(schedule = "DirtyCpu")]
fn strlen<'a>(env: Env<'a>, db: ResourceArc<DbResource>, key: &str) -> NifResult<Term<'a>> {
    let guard = db.db.lock().map_err(|_| rustler::Error::Atom("lock_error"))?;
    match guard.strlen(key) {
        Ok(len) => Ok(ok(env, len)),
        Err(e) => Ok(to_error(env, e)),
    }
}

/// GETRANGE key start end
#[rustler::nif(schedule = "DirtyCpu")]
fn getrange<'a>(
    env: Env<'a>,
    db: ResourceArc<DbResource>,
    key: &str,
    start: i64,
    stop: i64,
) -> NifResult<Term<'a>> {
    let guard = db.db.lock().map_err(|_| rustler::Error::Atom("lock_error"))?;
    match guard.getrange(key, start, stop) {
        Ok(v) => Ok(ok(env, vec_to_binary(env, v))),
        Err(e) => Ok(to_error(env, e)),
    }
}

/// SETRANGE key offset value
#[rustler::nif(schedule = "DirtyCpu")]
fn setrange<'a>(
    env: Env<'a>,
    db: ResourceArc<DbResource>,
    key: &str,
    offset: i64,
    value: Binary,
) -> NifResult<Term<'a>> {
    let guard = db.db.lock().map_err(|_| rustler::Error::Atom("lock_error"))?;
    match guard.setrange(key, offset, value.as_slice()) {
        Ok(len) => Ok(ok(env, len)),
        Err(e) => Ok(to_error(env, e)),
    }
}

/// INCR key
#[rustler::nif(schedule = "DirtyCpu")]
fn incr<'a>(env: Env<'a>, db: ResourceArc<DbResource>, key: &str) -> NifResult<Term<'a>> {
    let guard = db.db.lock().map_err(|_| rustler::Error::Atom("lock_error"))?;
    match guard.incr(key) {
        Ok(v) => Ok(ok(env, v)),
        Err(e) => Ok(to_error(env, e)),
    }
}

/// DECR key
#[rustler::nif(schedule = "DirtyCpu")]
fn decr<'a>(env: Env<'a>, db: ResourceArc<DbResource>, key: &str) -> NifResult<Term<'a>> {
    let guard = db.db.lock().map_err(|_| rustler::Error::Atom("lock_error"))?;
    match guard.decr(key) {
        Ok(v) => Ok(ok(env, v)),
        Err(e) => Ok(to_error(env, e)),
    }
}

/// INCRBY key increment
#[rustler::nif(schedule = "DirtyCpu")]
fn incrby<'a>(
    env: Env<'a>,
    db: ResourceArc<DbResource>,
    key: &str,
    increment: i64,
) -> NifResult<Term<'a>> {
    let guard = db.db.lock().map_err(|_| rustler::Error::Atom("lock_error"))?;
    match guard.incrby(key, increment) {
        Ok(v) => Ok(ok(env, v)),
        Err(e) => Ok(to_error(env, e)),
    }
}

/// DECRBY key decrement
#[rustler::nif(schedule = "DirtyCpu")]
fn decrby<'a>(
    env: Env<'a>,
    db: ResourceArc<DbResource>,
    key: &str,
    decrement: i64,
) -> NifResult<Term<'a>> {
    let guard = db.db.lock().map_err(|_| rustler::Error::Atom("lock_error"))?;
    match guard.decrby(key, decrement) {
        Ok(v) => Ok(ok(env, v)),
        Err(e) => Ok(to_error(env, e)),
    }
}

/// INCRBYFLOAT key increment
#[rustler::nif(schedule = "DirtyCpu")]
fn incrbyfloat<'a>(
    env: Env<'a>,
    db: ResourceArc<DbResource>,
    key: &str,
    increment: f64,
) -> NifResult<Term<'a>> {
    let guard = db.db.lock().map_err(|_| rustler::Error::Atom("lock_error"))?;
    match guard.incrbyfloat(key, increment) {
        Ok(v) => {
            let f: f64 = v.parse().unwrap_or(0.0);
            Ok(ok(env, f))
        }
        Err(e) => Ok(to_error(env, e)),
    }
}

/// MGET keys
#[rustler::nif(schedule = "DirtyCpu")]
fn mget<'a>(env: Env<'a>, db: ResourceArc<DbResource>, keys: Vec<&str>) -> NifResult<Term<'a>> {
    let guard = db.db.lock().map_err(|_| rustler::Error::Atom("lock_error"))?;
    let results = guard.mget(&keys);
    let values: Vec<Term> = results
        .into_iter()
        .map(|opt| match opt {
            Some(v) => vec_to_binary(env, v).encode(env),
            None => atoms::nil().encode(env),
        })
        .collect();
    Ok(ok(env, values))
}

/// MSET pairs
#[rustler::nif(schedule = "DirtyCpu")]
fn mset<'a>(
    env: Env<'a>,
    db: ResourceArc<DbResource>,
    pairs: Vec<(&str, Binary)>,
) -> NifResult<Term<'a>> {
    let guard = db.db.lock().map_err(|_| rustler::Error::Atom("lock_error"))?;
    let kv_pairs: Vec<(&str, &[u8])> = pairs
        .iter()
        .map(|(k, v)| (*k, v.as_slice()))
        .collect();
    match guard.mset(&kv_pairs) {
        Ok(()) => Ok(ok(env, true)),
        Err(e) => Ok(to_error(env, e)),
    }
}

// =============================================================================
// Key Commands
// =============================================================================

/// DEL keys
#[rustler::nif(schedule = "DirtyCpu")]
fn del<'a>(env: Env<'a>, db: ResourceArc<DbResource>, keys: Vec<&str>) -> NifResult<Term<'a>> {
    let guard = db.db.lock().map_err(|_| rustler::Error::Atom("lock_error"))?;
    match guard.del(&keys) {
        Ok(n) => Ok(ok(env, n)),
        Err(e) => Ok(to_error(env, e)),
    }
}

/// EXISTS keys
#[rustler::nif(schedule = "DirtyCpu")]
fn exists<'a>(env: Env<'a>, db: ResourceArc<DbResource>, keys: Vec<&str>) -> NifResult<Term<'a>> {
    let guard = db.db.lock().map_err(|_| rustler::Error::Atom("lock_error"))?;
    match guard.exists(&keys) {
        Ok(n) => Ok(ok(env, n)),
        Err(e) => Ok(to_error(env, e)),
    }
}

/// TYPE key
#[rustler::nif(schedule = "DirtyCpu")]
fn key_type<'a>(env: Env<'a>, db: ResourceArc<DbResource>, key: &str) -> NifResult<Term<'a>> {
    let guard = db.db.lock().map_err(|_| rustler::Error::Atom("lock_error"))?;
    match guard.key_type(key) {
        Ok(Some(t)) => {
            let type_atom = match t {
                redlite::KeyType::String => atoms::string(),
                redlite::KeyType::List => atoms::list(),
                redlite::KeyType::Set => atoms::set(),
                redlite::KeyType::ZSet => atoms::zset(),
                redlite::KeyType::Hash => atoms::hash(),
                redlite::KeyType::Stream => atoms::stream(),
            };
            Ok(ok(env, type_atom))
        }
        Ok(None) => Ok(ok(env, atoms::none())),
        Err(e) => Ok(to_error(env, e)),
    }
}

/// TTL key
#[rustler::nif(schedule = "DirtyCpu")]
fn ttl<'a>(env: Env<'a>, db: ResourceArc<DbResource>, key: &str) -> NifResult<Term<'a>> {
    let guard = db.db.lock().map_err(|_| rustler::Error::Atom("lock_error"))?;
    match guard.ttl(key) {
        Ok(v) => Ok(ok(env, v)),
        Err(e) => Ok(to_error(env, e)),
    }
}

/// PTTL key
#[rustler::nif(schedule = "DirtyCpu")]
fn pttl<'a>(env: Env<'a>, db: ResourceArc<DbResource>, key: &str) -> NifResult<Term<'a>> {
    let guard = db.db.lock().map_err(|_| rustler::Error::Atom("lock_error"))?;
    match guard.pttl(key) {
        Ok(v) => Ok(ok(env, v)),
        Err(e) => Ok(to_error(env, e)),
    }
}

/// EXPIRE key seconds
#[rustler::nif(schedule = "DirtyCpu")]
fn expire<'a>(
    env: Env<'a>,
    db: ResourceArc<DbResource>,
    key: &str,
    seconds: i64,
) -> NifResult<Term<'a>> {
    let guard = db.db.lock().map_err(|_| rustler::Error::Atom("lock_error"))?;
    match guard.expire(key, seconds) {
        Ok(v) => Ok(ok(env, v)),
        Err(e) => Ok(to_error(env, e)),
    }
}

/// PEXPIRE key milliseconds
#[rustler::nif(schedule = "DirtyCpu")]
fn pexpire<'a>(
    env: Env<'a>,
    db: ResourceArc<DbResource>,
    key: &str,
    milliseconds: i64,
) -> NifResult<Term<'a>> {
    let guard = db.db.lock().map_err(|_| rustler::Error::Atom("lock_error"))?;
    match guard.pexpire(key, milliseconds) {
        Ok(v) => Ok(ok(env, v)),
        Err(e) => Ok(to_error(env, e)),
    }
}

/// EXPIREAT key unix_timestamp
#[rustler::nif(schedule = "DirtyCpu")]
fn expireat<'a>(
    env: Env<'a>,
    db: ResourceArc<DbResource>,
    key: &str,
    unix_seconds: i64,
) -> NifResult<Term<'a>> {
    let guard = db.db.lock().map_err(|_| rustler::Error::Atom("lock_error"))?;
    match guard.expireat(key, unix_seconds) {
        Ok(v) => Ok(ok(env, v)),
        Err(e) => Ok(to_error(env, e)),
    }
}

/// PEXPIREAT key unix_timestamp_ms
#[rustler::nif(schedule = "DirtyCpu")]
fn pexpireat<'a>(
    env: Env<'a>,
    db: ResourceArc<DbResource>,
    key: &str,
    unix_ms: i64,
) -> NifResult<Term<'a>> {
    let guard = db.db.lock().map_err(|_| rustler::Error::Atom("lock_error"))?;
    match guard.pexpireat(key, unix_ms) {
        Ok(v) => Ok(ok(env, v)),
        Err(e) => Ok(to_error(env, e)),
    }
}

/// PERSIST key
#[rustler::nif(schedule = "DirtyCpu")]
fn persist<'a>(env: Env<'a>, db: ResourceArc<DbResource>, key: &str) -> NifResult<Term<'a>> {
    let guard = db.db.lock().map_err(|_| rustler::Error::Atom("lock_error"))?;
    match guard.persist(key) {
        Ok(v) => Ok(ok(env, v)),
        Err(e) => Ok(to_error(env, e)),
    }
}

/// RENAME key newkey
#[rustler::nif(schedule = "DirtyCpu")]
fn rename<'a>(
    env: Env<'a>,
    db: ResourceArc<DbResource>,
    key: &str,
    newkey: &str,
) -> NifResult<Term<'a>> {
    let guard = db.db.lock().map_err(|_| rustler::Error::Atom("lock_error"))?;
    match guard.rename(key, newkey) {
        Ok(()) => Ok(ok(env, true)),
        Err(e) => Ok(to_error(env, e)),
    }
}

/// RENAMENX key newkey
#[rustler::nif(schedule = "DirtyCpu")]
fn renamenx<'a>(
    env: Env<'a>,
    db: ResourceArc<DbResource>,
    key: &str,
    newkey: &str,
) -> NifResult<Term<'a>> {
    let guard = db.db.lock().map_err(|_| rustler::Error::Atom("lock_error"))?;
    match guard.renamenx(key, newkey) {
        Ok(v) => Ok(ok(env, v)),
        Err(e) => Ok(to_error(env, e)),
    }
}

/// KEYS pattern
#[rustler::nif(schedule = "DirtyCpu")]
fn keys<'a>(env: Env<'a>, db: ResourceArc<DbResource>, pattern: &str) -> NifResult<Term<'a>> {
    let guard = db.db.lock().map_err(|_| rustler::Error::Atom("lock_error"))?;
    match guard.keys(pattern) {
        Ok(keys) => Ok(ok(env, keys)),
        Err(e) => Ok(to_error(env, e)),
    }
}

/// DBSIZE
#[rustler::nif(schedule = "DirtyCpu")]
fn dbsize<'a>(env: Env<'a>, db: ResourceArc<DbResource>) -> NifResult<Term<'a>> {
    let guard = db.db.lock().map_err(|_| rustler::Error::Atom("lock_error"))?;
    match guard.dbsize() {
        Ok(n) => Ok(ok(env, n)),
        Err(e) => Ok(to_error(env, e)),
    }
}

/// FLUSHDB
#[rustler::nif(schedule = "DirtyCpu")]
fn flushdb<'a>(env: Env<'a>, db: ResourceArc<DbResource>) -> NifResult<Term<'a>> {
    let guard = db.db.lock().map_err(|_| rustler::Error::Atom("lock_error"))?;
    match guard.flushdb() {
        Ok(()) => Ok(ok(env, true)),
        Err(e) => Ok(to_error(env, e)),
    }
}

/// SELECT db
#[rustler::nif(schedule = "DirtyCpu")]
fn select<'a>(env: Env<'a>, db: ResourceArc<DbResource>, db_num: i32) -> NifResult<Term<'a>> {
    let mut guard = db.db.lock().map_err(|_| rustler::Error::Atom("lock_error"))?;
    match guard.select(db_num) {
        Ok(()) => Ok(ok(env, true)),
        Err(e) => Ok(to_error(env, e)),
    }
}

// =============================================================================
// Hash Commands
// =============================================================================

/// HSET key field value
#[rustler::nif(schedule = "DirtyCpu")]
fn hset<'a>(
    env: Env<'a>,
    db: ResourceArc<DbResource>,
    key: &str,
    pairs: Vec<(&str, Binary)>,
) -> NifResult<Term<'a>> {
    let guard = db.db.lock().map_err(|_| rustler::Error::Atom("lock_error"))?;
    let field_values: Vec<(&str, &[u8])> = pairs
        .iter()
        .map(|(f, v)| (*f, v.as_slice()))
        .collect();
    match guard.hset(key, &field_values) {
        Ok(n) => Ok(ok(env, n)),
        Err(e) => Ok(to_error(env, e)),
    }
}

/// HGET key field
#[rustler::nif(schedule = "DirtyCpu")]
fn hget<'a>(
    env: Env<'a>,
    db: ResourceArc<DbResource>,
    key: &str,
    field: &str,
) -> NifResult<Term<'a>> {
    let guard = db.db.lock().map_err(|_| rustler::Error::Atom("lock_error"))?;
    match guard.hget(key, field) {
        Ok(Some(v)) => Ok(ok(env, vec_to_binary(env, v))),
        Ok(None) => Ok(ok(env, atoms::nil())),
        Err(e) => Ok(to_error(env, e)),
    }
}

/// HDEL key fields
#[rustler::nif(schedule = "DirtyCpu")]
fn hdel<'a>(
    env: Env<'a>,
    db: ResourceArc<DbResource>,
    key: &str,
    fields: Vec<&str>,
) -> NifResult<Term<'a>> {
    let guard = db.db.lock().map_err(|_| rustler::Error::Atom("lock_error"))?;
    match guard.hdel(key, &fields) {
        Ok(n) => Ok(ok(env, n)),
        Err(e) => Ok(to_error(env, e)),
    }
}

/// HEXISTS key field
#[rustler::nif(schedule = "DirtyCpu")]
fn hexists<'a>(
    env: Env<'a>,
    db: ResourceArc<DbResource>,
    key: &str,
    field: &str,
) -> NifResult<Term<'a>> {
    let guard = db.db.lock().map_err(|_| rustler::Error::Atom("lock_error"))?;
    match guard.hexists(key, field) {
        Ok(v) => Ok(ok(env, v)),
        Err(e) => Ok(to_error(env, e)),
    }
}

/// HLEN key
#[rustler::nif(schedule = "DirtyCpu")]
fn hlen<'a>(env: Env<'a>, db: ResourceArc<DbResource>, key: &str) -> NifResult<Term<'a>> {
    let guard = db.db.lock().map_err(|_| rustler::Error::Atom("lock_error"))?;
    match guard.hlen(key) {
        Ok(n) => Ok(ok(env, n)),
        Err(e) => Ok(to_error(env, e)),
    }
}

/// HKEYS key
#[rustler::nif(schedule = "DirtyCpu")]
fn hkeys<'a>(env: Env<'a>, db: ResourceArc<DbResource>, key: &str) -> NifResult<Term<'a>> {
    let guard = db.db.lock().map_err(|_| rustler::Error::Atom("lock_error"))?;
    match guard.hkeys(key) {
        Ok(keys) => Ok(ok(env, keys)),
        Err(e) => Ok(to_error(env, e)),
    }
}

/// HVALS key
#[rustler::nif(schedule = "DirtyCpu")]
fn hvals<'a>(env: Env<'a>, db: ResourceArc<DbResource>, key: &str) -> NifResult<Term<'a>> {
    let guard = db.db.lock().map_err(|_| rustler::Error::Atom("lock_error"))?;
    match guard.hvals(key) {
        Ok(vals) => {
            let binaries: Vec<Term> = vals
                .into_iter()
                .map(|v| vec_to_binary(env, v).encode(env))
                .collect();
            Ok(ok(env, binaries))
        }
        Err(e) => Ok(to_error(env, e)),
    }
}

/// HINCRBY key field increment
#[rustler::nif(schedule = "DirtyCpu")]
fn hincrby<'a>(
    env: Env<'a>,
    db: ResourceArc<DbResource>,
    key: &str,
    field: &str,
    increment: i64,
) -> NifResult<Term<'a>> {
    let guard = db.db.lock().map_err(|_| rustler::Error::Atom("lock_error"))?;
    match guard.hincrby(key, field, increment) {
        Ok(v) => Ok(ok(env, v)),
        Err(e) => Ok(to_error(env, e)),
    }
}

/// HGETALL key
#[rustler::nif(schedule = "DirtyCpu")]
fn hgetall<'a>(env: Env<'a>, db: ResourceArc<DbResource>, key: &str) -> NifResult<Term<'a>> {
    let guard = db.db.lock().map_err(|_| rustler::Error::Atom("lock_error"))?;
    match guard.hgetall(key) {
        Ok(pairs) => {
            let result: Vec<(String, Term)> = pairs
                .into_iter()
                .map(|(field, value)| (field, vec_to_binary(env, value).encode(env)))
                .collect();
            Ok(ok(env, result))
        }
        Err(e) => Ok(to_error(env, e)),
    }
}

/// HMGET key fields
#[rustler::nif(schedule = "DirtyCpu")]
fn hmget<'a>(
    env: Env<'a>,
    db: ResourceArc<DbResource>,
    key: &str,
    fields: Vec<&str>,
) -> NifResult<Term<'a>> {
    let guard = db.db.lock().map_err(|_| rustler::Error::Atom("lock_error"))?;
    match guard.hmget(key, &fields) {
        Ok(results) => {
            let values: Vec<Term> = results
                .into_iter()
                .map(|opt| match opt {
                    Some(v) => vec_to_binary(env, v).encode(env),
                    None => atoms::nil().encode(env),
                })
                .collect();
            Ok(ok(env, values))
        }
        Err(e) => Ok(to_error(env, e)),
    }
}

// =============================================================================
// List Commands
// =============================================================================

/// LPUSH key values
#[rustler::nif(schedule = "DirtyCpu")]
fn lpush<'a>(
    env: Env<'a>,
    db: ResourceArc<DbResource>,
    key: &str,
    values: Vec<Binary>,
) -> NifResult<Term<'a>> {
    let guard = db.db.lock().map_err(|_| rustler::Error::Atom("lock_error"))?;
    let value_refs: Vec<&[u8]> = values.iter().map(|v| v.as_slice()).collect();
    match guard.lpush(key, &value_refs) {
        Ok(n) => Ok(ok(env, n)),
        Err(e) => Ok(to_error(env, e)),
    }
}

/// RPUSH key values
#[rustler::nif(schedule = "DirtyCpu")]
fn rpush<'a>(
    env: Env<'a>,
    db: ResourceArc<DbResource>,
    key: &str,
    values: Vec<Binary>,
) -> NifResult<Term<'a>> {
    let guard = db.db.lock().map_err(|_| rustler::Error::Atom("lock_error"))?;
    let value_refs: Vec<&[u8]> = values.iter().map(|v| v.as_slice()).collect();
    match guard.rpush(key, &value_refs) {
        Ok(n) => Ok(ok(env, n)),
        Err(e) => Ok(to_error(env, e)),
    }
}

/// LPOP key count
#[rustler::nif(schedule = "DirtyCpu")]
fn lpop<'a>(
    env: Env<'a>,
    db: ResourceArc<DbResource>,
    key: &str,
    count: Option<usize>,
) -> NifResult<Term<'a>> {
    let guard = db.db.lock().map_err(|_| rustler::Error::Atom("lock_error"))?;
    match guard.lpop(key, count) {
        Ok(vals) => {
            if vals.is_empty() {
                Ok(ok(env, atoms::nil()))
            } else if count.is_none() || count == Some(1) {
                // Single value case - return just the value
                Ok(ok(env, vec_to_binary(env, vals.into_iter().next().unwrap())))
            } else {
                let binaries: Vec<Term> = vals
                    .into_iter()
                    .map(|v| vec_to_binary(env, v).encode(env))
                    .collect();
                Ok(ok(env, binaries))
            }
        }
        Err(e) => Ok(to_error(env, e)),
    }
}

/// RPOP key count
#[rustler::nif(schedule = "DirtyCpu")]
fn rpop<'a>(
    env: Env<'a>,
    db: ResourceArc<DbResource>,
    key: &str,
    count: Option<usize>,
) -> NifResult<Term<'a>> {
    let guard = db.db.lock().map_err(|_| rustler::Error::Atom("lock_error"))?;
    match guard.rpop(key, count) {
        Ok(vals) => {
            if vals.is_empty() {
                Ok(ok(env, atoms::nil()))
            } else if count.is_none() || count == Some(1) {
                // Single value case - return just the value
                Ok(ok(env, vec_to_binary(env, vals.into_iter().next().unwrap())))
            } else {
                let binaries: Vec<Term> = vals
                    .into_iter()
                    .map(|v| vec_to_binary(env, v).encode(env))
                    .collect();
                Ok(ok(env, binaries))
            }
        }
        Err(e) => Ok(to_error(env, e)),
    }
}

/// LLEN key
#[rustler::nif(schedule = "DirtyCpu")]
fn llen<'a>(env: Env<'a>, db: ResourceArc<DbResource>, key: &str) -> NifResult<Term<'a>> {
    let guard = db.db.lock().map_err(|_| rustler::Error::Atom("lock_error"))?;
    match guard.llen(key) {
        Ok(n) => Ok(ok(env, n)),
        Err(e) => Ok(to_error(env, e)),
    }
}

/// LRANGE key start stop
#[rustler::nif(schedule = "DirtyCpu")]
fn lrange<'a>(
    env: Env<'a>,
    db: ResourceArc<DbResource>,
    key: &str,
    start: i64,
    stop: i64,
) -> NifResult<Term<'a>> {
    let guard = db.db.lock().map_err(|_| rustler::Error::Atom("lock_error"))?;
    match guard.lrange(key, start, stop) {
        Ok(vals) => {
            let binaries: Vec<Term> = vals
                .into_iter()
                .map(|v| vec_to_binary(env, v).encode(env))
                .collect();
            Ok(ok(env, binaries))
        }
        Err(e) => Ok(to_error(env, e)),
    }
}

/// LINDEX key index
#[rustler::nif(schedule = "DirtyCpu")]
fn lindex<'a>(
    env: Env<'a>,
    db: ResourceArc<DbResource>,
    key: &str,
    index: i64,
) -> NifResult<Term<'a>> {
    let guard = db.db.lock().map_err(|_| rustler::Error::Atom("lock_error"))?;
    match guard.lindex(key, index) {
        Ok(Some(v)) => Ok(ok(env, vec_to_binary(env, v))),
        Ok(None) => Ok(ok(env, atoms::nil())),
        Err(e) => Ok(to_error(env, e)),
    }
}

// =============================================================================
// Set Commands
// =============================================================================

/// SADD key members
#[rustler::nif(schedule = "DirtyCpu")]
fn sadd<'a>(
    env: Env<'a>,
    db: ResourceArc<DbResource>,
    key: &str,
    members: Vec<Binary>,
) -> NifResult<Term<'a>> {
    let guard = db.db.lock().map_err(|_| rustler::Error::Atom("lock_error"))?;
    let member_refs: Vec<&[u8]> = members.iter().map(|m| m.as_slice()).collect();
    match guard.sadd(key, &member_refs) {
        Ok(n) => Ok(ok(env, n)),
        Err(e) => Ok(to_error(env, e)),
    }
}

/// SREM key members
#[rustler::nif(schedule = "DirtyCpu")]
fn srem<'a>(
    env: Env<'a>,
    db: ResourceArc<DbResource>,
    key: &str,
    members: Vec<Binary>,
) -> NifResult<Term<'a>> {
    let guard = db.db.lock().map_err(|_| rustler::Error::Atom("lock_error"))?;
    let member_refs: Vec<&[u8]> = members.iter().map(|m| m.as_slice()).collect();
    match guard.srem(key, &member_refs) {
        Ok(n) => Ok(ok(env, n)),
        Err(e) => Ok(to_error(env, e)),
    }
}

/// SMEMBERS key
#[rustler::nif(schedule = "DirtyCpu")]
fn smembers<'a>(env: Env<'a>, db: ResourceArc<DbResource>, key: &str) -> NifResult<Term<'a>> {
    let guard = db.db.lock().map_err(|_| rustler::Error::Atom("lock_error"))?;
    match guard.smembers(key) {
        Ok(members) => {
            let binaries: Vec<Term> = members
                .into_iter()
                .map(|m| vec_to_binary(env, m).encode(env))
                .collect();
            Ok(ok(env, binaries))
        }
        Err(e) => Ok(to_error(env, e)),
    }
}

/// SISMEMBER key member
#[rustler::nif(schedule = "DirtyCpu")]
fn sismember<'a>(
    env: Env<'a>,
    db: ResourceArc<DbResource>,
    key: &str,
    member: Binary,
) -> NifResult<Term<'a>> {
    let guard = db.db.lock().map_err(|_| rustler::Error::Atom("lock_error"))?;
    match guard.sismember(key, member.as_slice()) {
        Ok(v) => Ok(ok(env, v)),
        Err(e) => Ok(to_error(env, e)),
    }
}

/// SCARD key
#[rustler::nif(schedule = "DirtyCpu")]
fn scard<'a>(env: Env<'a>, db: ResourceArc<DbResource>, key: &str) -> NifResult<Term<'a>> {
    let guard = db.db.lock().map_err(|_| rustler::Error::Atom("lock_error"))?;
    match guard.scard(key) {
        Ok(n) => Ok(ok(env, n)),
        Err(e) => Ok(to_error(env, e)),
    }
}

// =============================================================================
// Sorted Set Commands
// =============================================================================

/// ZADD key members (list of {score, member} tuples)
#[rustler::nif(schedule = "DirtyCpu")]
fn zadd<'a>(
    env: Env<'a>,
    db: ResourceArc<DbResource>,
    key: &str,
    members: Vec<(f64, Binary)>,
) -> NifResult<Term<'a>> {
    let guard = db.db.lock().map_err(|_| rustler::Error::Atom("lock_error"))?;
    let zmembers: Vec<redlite::ZMember> = members
        .iter()
        .map(|(score, member)| redlite::ZMember {
            score: *score,
            member: member.as_slice().to_vec(),
        })
        .collect();
    match guard.zadd(key, &zmembers) {
        Ok(n) => Ok(ok(env, n)),
        Err(e) => Ok(to_error(env, e)),
    }
}

/// ZREM key members
#[rustler::nif(schedule = "DirtyCpu")]
fn zrem<'a>(
    env: Env<'a>,
    db: ResourceArc<DbResource>,
    key: &str,
    members: Vec<Binary>,
) -> NifResult<Term<'a>> {
    let guard = db.db.lock().map_err(|_| rustler::Error::Atom("lock_error"))?;
    let member_refs: Vec<&[u8]> = members.iter().map(|m| m.as_slice()).collect();
    match guard.zrem(key, &member_refs) {
        Ok(n) => Ok(ok(env, n)),
        Err(e) => Ok(to_error(env, e)),
    }
}

/// ZSCORE key member
#[rustler::nif(schedule = "DirtyCpu")]
fn zscore<'a>(
    env: Env<'a>,
    db: ResourceArc<DbResource>,
    key: &str,
    member: Binary,
) -> NifResult<Term<'a>> {
    let guard = db.db.lock().map_err(|_| rustler::Error::Atom("lock_error"))?;
    match guard.zscore(key, member.as_slice()) {
        Ok(Some(score)) => Ok(ok(env, score)),
        Ok(None) => Ok(ok(env, atoms::nil())),
        Err(e) => Ok(to_error(env, e)),
    }
}

/// ZCARD key
#[rustler::nif(schedule = "DirtyCpu")]
fn zcard<'a>(env: Env<'a>, db: ResourceArc<DbResource>, key: &str) -> NifResult<Term<'a>> {
    let guard = db.db.lock().map_err(|_| rustler::Error::Atom("lock_error"))?;
    match guard.zcard(key) {
        Ok(n) => Ok(ok(env, n)),
        Err(e) => Ok(to_error(env, e)),
    }
}

/// ZCOUNT key min max
#[rustler::nif(schedule = "DirtyCpu")]
fn zcount<'a>(
    env: Env<'a>,
    db: ResourceArc<DbResource>,
    key: &str,
    min: f64,
    max: f64,
) -> NifResult<Term<'a>> {
    let guard = db.db.lock().map_err(|_| rustler::Error::Atom("lock_error"))?;
    match guard.zcount(key, min, max) {
        Ok(n) => Ok(ok(env, n)),
        Err(e) => Ok(to_error(env, e)),
    }
}

/// ZINCRBY key increment member
#[rustler::nif(schedule = "DirtyCpu")]
fn zincrby<'a>(
    env: Env<'a>,
    db: ResourceArc<DbResource>,
    key: &str,
    increment: f64,
    member: Binary,
) -> NifResult<Term<'a>> {
    let guard = db.db.lock().map_err(|_| rustler::Error::Atom("lock_error"))?;
    match guard.zincrby(key, increment, member.as_slice()) {
        Ok(score) => Ok(ok(env, score)),
        Err(e) => Ok(to_error(env, e)),
    }
}

/// ZRANGE key start stop [WITHSCORES]
#[rustler::nif(schedule = "DirtyCpu")]
fn zrange<'a>(
    env: Env<'a>,
    db: ResourceArc<DbResource>,
    key: &str,
    start: i64,
    stop: i64,
    with_scores: bool,
) -> NifResult<Term<'a>> {
    let guard = db.db.lock().map_err(|_| rustler::Error::Atom("lock_error"))?;
    match guard.zrange(key, start, stop, with_scores) {
        Ok(members) => {
            if with_scores {
                let result: Vec<(Term, f64)> = members
                    .into_iter()
                    .map(|m| (vec_to_binary(env, m.member).encode(env), m.score))
                    .collect();
                Ok(ok(env, result))
            } else {
                let result: Vec<Term> = members
                    .into_iter()
                    .map(|m| vec_to_binary(env, m.member).encode(env))
                    .collect();
                Ok(ok(env, result))
            }
        }
        Err(e) => Ok(to_error(env, e)),
    }
}

/// ZREVRANGE key start stop [WITHSCORES]
#[rustler::nif(schedule = "DirtyCpu")]
fn zrevrange<'a>(
    env: Env<'a>,
    db: ResourceArc<DbResource>,
    key: &str,
    start: i64,
    stop: i64,
    with_scores: bool,
) -> NifResult<Term<'a>> {
    let guard = db.db.lock().map_err(|_| rustler::Error::Atom("lock_error"))?;
    match guard.zrevrange(key, start, stop, with_scores) {
        Ok(members) => {
            if with_scores {
                let result: Vec<(Term, f64)> = members
                    .into_iter()
                    .map(|m| (vec_to_binary(env, m.member).encode(env), m.score))
                    .collect();
                Ok(ok(env, result))
            } else {
                let result: Vec<Term> = members
                    .into_iter()
                    .map(|m| vec_to_binary(env, m.member).encode(env))
                    .collect();
                Ok(ok(env, result))
            }
        }
        Err(e) => Ok(to_error(env, e)),
    }
}

// =============================================================================
// Scan Commands
// =============================================================================

/// SCAN cursor [MATCH pattern] [COUNT count]
#[rustler::nif(schedule = "DirtyCpu")]
fn scan<'a>(
    env: Env<'a>,
    db: ResourceArc<DbResource>,
    cursor: &str,
    pattern: Option<&str>,
    count: usize,
) -> NifResult<Term<'a>> {
    let guard = db.db.lock().map_err(|_| rustler::Error::Atom("lock_error"))?;
    match guard.scan(cursor, pattern, count) {
        Ok((next_cursor, keys)) => Ok(ok(env, (next_cursor, keys))),
        Err(e) => Ok(to_error(env, e)),
    }
}

/// HSCAN key cursor [MATCH pattern] [COUNT count]
#[rustler::nif(schedule = "DirtyCpu")]
fn hscan<'a>(
    env: Env<'a>,
    db: ResourceArc<DbResource>,
    key: &str,
    cursor: &str,
    pattern: Option<&str>,
    count: usize,
) -> NifResult<Term<'a>> {
    let guard = db.db.lock().map_err(|_| rustler::Error::Atom("lock_error"))?;
    match guard.hscan(key, cursor, pattern, count) {
        Ok((next_cursor, pairs)) => {
            let result: Vec<(String, Term)> = pairs
                .into_iter()
                .map(|(field, value)| (field, vec_to_binary(env, value).encode(env)))
                .collect();
            Ok(ok(env, (next_cursor, result)))
        }
        Err(e) => Ok(to_error(env, e)),
    }
}

/// SSCAN key cursor [MATCH pattern] [COUNT count]
#[rustler::nif(schedule = "DirtyCpu")]
fn sscan<'a>(
    env: Env<'a>,
    db: ResourceArc<DbResource>,
    key: &str,
    cursor: &str,
    pattern: Option<&str>,
    count: usize,
) -> NifResult<Term<'a>> {
    let guard = db.db.lock().map_err(|_| rustler::Error::Atom("lock_error"))?;
    match guard.sscan(key, cursor, pattern, count) {
        Ok((next_cursor, members)) => {
            let result: Vec<Term> = members
                .into_iter()
                .map(|m| vec_to_binary(env, m).encode(env))
                .collect();
            Ok(ok(env, (next_cursor, result)))
        }
        Err(e) => Ok(to_error(env, e)),
    }
}

/// ZSCAN key cursor [MATCH pattern] [COUNT count]
#[rustler::nif(schedule = "DirtyCpu")]
fn zscan<'a>(
    env: Env<'a>,
    db: ResourceArc<DbResource>,
    key: &str,
    cursor: &str,
    pattern: Option<&str>,
    count: usize,
) -> NifResult<Term<'a>> {
    let guard = db.db.lock().map_err(|_| rustler::Error::Atom("lock_error"))?;
    match guard.zscan(key, cursor, pattern, count) {
        Ok((next_cursor, members)) => {
            let result: Vec<(Term, f64)> = members
                .into_iter()
                .map(|(m, score)| (vec_to_binary(env, m).encode(env), score))
                .collect();
            Ok(ok(env, (next_cursor, result)))
        }
        Err(e) => Ok(to_error(env, e)),
    }
}

// =============================================================================
// Server Commands
// =============================================================================

/// VACUUM - compact the database
#[rustler::nif(schedule = "DirtyCpu")]
fn vacuum<'a>(env: Env<'a>, db: ResourceArc<DbResource>) -> NifResult<Term<'a>> {
    let guard = db.db.lock().map_err(|_| rustler::Error::Atom("lock_error"))?;
    match guard.vacuum() {
        Ok(n) => Ok(ok(env, n)),
        Err(e) => Ok(to_error(env, e)),
    }
}

rustler::init!(
    "Elixir.Redlite.Native",
    [
        // Lifecycle
        open,
        open_memory,
        open_with_cache,
        // String commands
        get,
        set,
        set_opts,
        setex,
        psetex,
        getdel,
        append,
        strlen,
        getrange,
        setrange,
        incr,
        decr,
        incrby,
        decrby,
        incrbyfloat,
        mget,
        mset,
        // Key commands
        del,
        exists,
        key_type,
        ttl,
        pttl,
        expire,
        pexpire,
        expireat,
        pexpireat,
        persist,
        rename,
        renamenx,
        keys,
        dbsize,
        flushdb,
        select,
        // Hash commands
        hset,
        hget,
        hdel,
        hexists,
        hlen,
        hkeys,
        hvals,
        hincrby,
        hgetall,
        hmget,
        // List commands
        lpush,
        rpush,
        lpop,
        rpop,
        llen,
        lrange,
        lindex,
        // Set commands
        sadd,
        srem,
        smembers,
        sismember,
        scard,
        // Sorted set commands
        zadd,
        zrem,
        zscore,
        zcard,
        zcount,
        zincrby,
        zrange,
        zrevrange,
        // Scan commands
        scan,
        hscan,
        sscan,
        zscan,
        // Server commands
        vacuum,
    ],
    load = load
);
