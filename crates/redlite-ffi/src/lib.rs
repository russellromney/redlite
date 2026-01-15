//! C FFI bindings for redlite
//!
//! This crate provides C-compatible functions for embedding redlite in other languages
//! like Python (via CFFI) and Go (via CGO).
//!
//! # Memory Management
//!
//! - Strings returned by functions must be freed with `redlite_free_string`
//! - Byte arrays returned must be freed with `redlite_free_bytes`
//! - The `RedliteDb` handle must be freed with `redlite_close`
//!
//! # Error Handling
//!
//! Functions that can fail return a status code:
//! - 0 = success
//! - negative = error (call `redlite_last_error` for message)
//!
//! Functions returning data use out-parameters with NULL indicating no value.

use libc::{c_char, c_int, c_void, size_t};
use redlite::Db;
use std::ffi::{CStr, CString};
use std::ptr;
use std::slice;
use std::sync::Mutex;
use std::time::Duration;

// Thread-local error storage
thread_local! {
    static LAST_ERROR: std::cell::RefCell<Option<String>> = std::cell::RefCell::new(None);
}

fn set_error(msg: String) {
    LAST_ERROR.with(|e| *e.borrow_mut() = Some(msg));
}

fn clear_error() {
    LAST_ERROR.with(|e| *e.borrow_mut() = None);
}

/// Opaque handle to a redlite database
pub struct RedliteDb {
    db: Mutex<Db>,
}

/// Result of operations that return bytes
#[repr(C)]
pub struct RedliteBytes {
    pub data: *mut u8,
    pub len: size_t,
}

/// Result of operations that return a string array
#[repr(C)]
pub struct RedliteStringArray {
    pub strings: *mut *mut c_char,
    pub len: size_t,
}

/// Result of operations that return bytes array
#[repr(C)]
pub struct RedliteBytesArray {
    pub items: *mut RedliteBytes,
    pub len: size_t,
}

/// Key-value pair for hash operations
#[repr(C)]
pub struct RedliteKV {
    pub key: *const c_char,
    pub value: *const u8,
    pub value_len: size_t,
}

/// Sorted set member
#[repr(C)]
pub struct RedliteZMember {
    pub score: f64,
    pub member: *const u8,
    pub member_len: size_t,
}

// =============================================================================
// Lifecycle
// =============================================================================

/// Open a database at the given path
///
/// Returns NULL on error. Call `redlite_last_error` for details.
#[no_mangle]
pub extern "C" fn redlite_open(path: *const c_char) -> *mut RedliteDb {
    clear_error();

    let path = match unsafe { CStr::from_ptr(path) }.to_str() {
        Ok(s) => s,
        Err(e) => {
            set_error(format!("Invalid path: {}", e));
            return ptr::null_mut();
        }
    };

    match Db::open(path) {
        Ok(db) => Box::into_raw(Box::new(RedliteDb { db: Mutex::new(db) })),
        Err(e) => {
            set_error(format!("Failed to open database: {}", e));
            ptr::null_mut()
        }
    }
}

/// Open an in-memory database
#[no_mangle]
pub extern "C" fn redlite_open_memory() -> *mut RedliteDb {
    clear_error();

    match Db::open_memory() {
        Ok(db) => Box::into_raw(Box::new(RedliteDb { db: Mutex::new(db) })),
        Err(e) => {
            set_error(format!("Failed to open memory database: {}", e));
            ptr::null_mut()
        }
    }
}

/// Open a database with custom cache size
#[no_mangle]
pub extern "C" fn redlite_open_with_cache(path: *const c_char, cache_mb: i64) -> *mut RedliteDb {
    clear_error();

    let path = match unsafe { CStr::from_ptr(path) }.to_str() {
        Ok(s) => s,
        Err(e) => {
            set_error(format!("Invalid path: {}", e));
            return ptr::null_mut();
        }
    };

    match Db::open_with_cache(path, cache_mb) {
        Ok(db) => Box::into_raw(Box::new(RedliteDb { db: Mutex::new(db) })),
        Err(e) => {
            set_error(format!("Failed to open database: {}", e));
            ptr::null_mut()
        }
    }
}

/// Close a database and free resources
#[no_mangle]
pub extern "C" fn redlite_close(db: *mut RedliteDb) {
    if !db.is_null() {
        unsafe { drop(Box::from_raw(db)) };
    }
}

/// Get the last error message (NULL if no error)
#[no_mangle]
pub extern "C" fn redlite_last_error() -> *mut c_char {
    LAST_ERROR.with(|e| {
        match &*e.borrow() {
            Some(msg) => CString::new(msg.as_str()).unwrap().into_raw(),
            None => ptr::null_mut(),
        }
    })
}

// =============================================================================
// Memory Management
// =============================================================================

/// Free a string returned by redlite functions
#[no_mangle]
pub extern "C" fn redlite_free_string(s: *mut c_char) {
    if !s.is_null() {
        unsafe { drop(CString::from_raw(s)) };
    }
}

/// Free bytes returned by redlite functions
#[no_mangle]
pub extern "C" fn redlite_free_bytes(bytes: RedliteBytes) {
    if !bytes.data.is_null() && bytes.len > 0 {
        unsafe {
            drop(Vec::from_raw_parts(bytes.data, bytes.len, bytes.len));
        }
    }
}

/// Free a string array returned by redlite functions
#[no_mangle]
pub extern "C" fn redlite_free_string_array(arr: RedliteStringArray) {
    if !arr.strings.is_null() && arr.len > 0 {
        unsafe {
            let strings = Vec::from_raw_parts(arr.strings, arr.len, arr.len);
            for s in strings {
                if !s.is_null() {
                    drop(CString::from_raw(s));
                }
            }
        }
    }
}

/// Free a bytes array returned by redlite functions
#[no_mangle]
pub extern "C" fn redlite_free_bytes_array(arr: RedliteBytesArray) {
    if !arr.items.is_null() && arr.len > 0 {
        unsafe {
            let items = Vec::from_raw_parts(arr.items, arr.len, arr.len);
            for item in items {
                redlite_free_bytes(item);
            }
        }
    }
}

// =============================================================================
// Helper macros and functions
// =============================================================================

macro_rules! get_db {
    ($db:expr) => {{
        if $db.is_null() {
            set_error("NULL database handle".to_string());
            return -1;
        }
        unsafe { &*$db }
    }};
}

macro_rules! get_db_ret {
    ($db:expr, $ret:expr) => {{
        if $db.is_null() {
            set_error("NULL database handle".to_string());
            return $ret;
        }
        unsafe { &*$db }
    }};
}

fn cstr_to_str(s: *const c_char) -> Result<&'static str, String> {
    if s.is_null() {
        return Err("NULL string".to_string());
    }
    unsafe { CStr::from_ptr(s) }
        .to_str()
        .map_err(|e| format!("Invalid UTF-8: {}", e))
}

fn bytes_to_vec(data: *const u8, len: size_t) -> Vec<u8> {
    if data.is_null() || len == 0 {
        Vec::new()
    } else {
        unsafe { slice::from_raw_parts(data, len) }.to_vec()
    }
}

fn vec_to_bytes(v: Vec<u8>) -> RedliteBytes {
    let len = v.len();
    let data = if len > 0 {
        let mut v = v.into_boxed_slice();
        let ptr = v.as_mut_ptr();
        std::mem::forget(v);
        ptr
    } else {
        ptr::null_mut()
    };
    RedliteBytes { data, len }
}

fn opt_vec_to_bytes(v: Option<Vec<u8>>) -> RedliteBytes {
    match v {
        Some(v) => vec_to_bytes(v),
        None => RedliteBytes { data: ptr::null_mut(), len: 0 },
    }
}

fn strings_to_array(strings: Vec<String>) -> RedliteStringArray {
    let len = strings.len();
    if len == 0 {
        return RedliteStringArray { strings: ptr::null_mut(), len: 0 };
    }

    let mut ptrs: Vec<*mut c_char> = strings
        .into_iter()
        .map(|s| CString::new(s).unwrap().into_raw())
        .collect();

    let ptr = ptrs.as_mut_ptr();
    std::mem::forget(ptrs);

    RedliteStringArray { strings: ptr, len }
}

fn vecs_to_bytes_array(vecs: Vec<Vec<u8>>) -> RedliteBytesArray {
    let len = vecs.len();
    if len == 0 {
        return RedliteBytesArray { items: ptr::null_mut(), len: 0 };
    }

    let mut items: Vec<RedliteBytes> = vecs.into_iter().map(vec_to_bytes).collect();
    let ptr = items.as_mut_ptr();
    std::mem::forget(items);

    RedliteBytesArray { items: ptr, len }
}

// =============================================================================
// String Commands
// =============================================================================

/// GET key
#[no_mangle]
pub extern "C" fn redlite_get(db: *mut RedliteDb, key: *const c_char) -> RedliteBytes {
    clear_error();
    let handle = get_db_ret!(db, RedliteBytes { data: ptr::null_mut(), len: 0 });

    let key = match cstr_to_str(key) {
        Ok(k) => k,
        Err(e) => {
            set_error(e);
            return RedliteBytes { data: ptr::null_mut(), len: 0 };
        }
    };

    let guard = handle.db.lock().unwrap();
    match guard.get(key) {
        Ok(v) => opt_vec_to_bytes(v),
        Err(e) => {
            set_error(format!("GET failed: {}", e));
            RedliteBytes { data: ptr::null_mut(), len: 0 }
        }
    }
}

/// SET key value [ttl_seconds]
/// Returns 0 on success, -1 on error
#[no_mangle]
pub extern "C" fn redlite_set(
    db: *mut RedliteDb,
    key: *const c_char,
    value: *const u8,
    value_len: size_t,
    ttl_seconds: i64,
) -> c_int {
    clear_error();
    let handle = get_db!(db);

    let key = match cstr_to_str(key) {
        Ok(k) => k,
        Err(e) => {
            set_error(e);
            return -1;
        }
    };

    let value = bytes_to_vec(value, value_len);
    let ttl = if ttl_seconds > 0 {
        Some(Duration::from_secs(ttl_seconds as u64))
    } else {
        None
    };

    let guard = handle.db.lock().unwrap();
    match guard.set(key, &value, ttl) {
        Ok(()) => 0,
        Err(e) => {
            set_error(format!("SET failed: {}", e));
            -1
        }
    }
}

/// SETEX key seconds value
#[no_mangle]
pub extern "C" fn redlite_setex(
    db: *mut RedliteDb,
    key: *const c_char,
    seconds: i64,
    value: *const u8,
    value_len: size_t,
) -> c_int {
    clear_error();
    let handle = get_db!(db);

    let key = match cstr_to_str(key) {
        Ok(k) => k,
        Err(e) => {
            set_error(e);
            return -1;
        }
    };

    let value = bytes_to_vec(value, value_len);
    let guard = handle.db.lock().unwrap();

    match guard.setex(key, seconds, &value) {
        Ok(()) => 0,
        Err(e) => {
            set_error(format!("SETEX failed: {}", e));
            -1
        }
    }
}

/// PSETEX key milliseconds value
#[no_mangle]
pub extern "C" fn redlite_psetex(
    db: *mut RedliteDb,
    key: *const c_char,
    milliseconds: i64,
    value: *const u8,
    value_len: size_t,
) -> c_int {
    clear_error();
    let handle = get_db!(db);

    let key = match cstr_to_str(key) {
        Ok(k) => k,
        Err(e) => {
            set_error(e);
            return -1;
        }
    };

    let value = bytes_to_vec(value, value_len);
    let guard = handle.db.lock().unwrap();

    match guard.psetex(key, milliseconds, &value) {
        Ok(()) => 0,
        Err(e) => {
            set_error(format!("PSETEX failed: {}", e));
            -1
        }
    }
}

/// GETDEL key
#[no_mangle]
pub extern "C" fn redlite_getdel(db: *mut RedliteDb, key: *const c_char) -> RedliteBytes {
    clear_error();
    let handle = get_db_ret!(db, RedliteBytes { data: ptr::null_mut(), len: 0 });

    let key = match cstr_to_str(key) {
        Ok(k) => k,
        Err(e) => {
            set_error(e);
            return RedliteBytes { data: ptr::null_mut(), len: 0 };
        }
    };

    let guard = handle.db.lock().unwrap();
    match guard.getdel(key) {
        Ok(v) => opt_vec_to_bytes(v),
        Err(e) => {
            set_error(format!("GETDEL failed: {}", e));
            RedliteBytes { data: ptr::null_mut(), len: 0 }
        }
    }
}

/// APPEND key value
/// Returns new length, or -1 on error
#[no_mangle]
pub extern "C" fn redlite_append(
    db: *mut RedliteDb,
    key: *const c_char,
    value: *const u8,
    value_len: size_t,
) -> i64 {
    clear_error();
    let handle = get_db_ret!(db, -1);

    let key = match cstr_to_str(key) {
        Ok(k) => k,
        Err(e) => {
            set_error(e);
            return -1;
        }
    };

    let value = bytes_to_vec(value, value_len);
    let guard = handle.db.lock().unwrap();

    match guard.append(key, &value) {
        Ok(len) => len,
        Err(e) => {
            set_error(format!("APPEND failed: {}", e));
            -1
        }
    }
}

/// STRLEN key
#[no_mangle]
pub extern "C" fn redlite_strlen(db: *mut RedliteDb, key: *const c_char) -> i64 {
    clear_error();
    let handle = get_db_ret!(db, -1);

    let key = match cstr_to_str(key) {
        Ok(k) => k,
        Err(e) => {
            set_error(e);
            return -1;
        }
    };

    let guard = handle.db.lock().unwrap();
    match guard.strlen(key) {
        Ok(len) => len,
        Err(e) => {
            set_error(format!("STRLEN failed: {}", e));
            -1
        }
    }
}

/// GETRANGE key start end
#[no_mangle]
pub extern "C" fn redlite_getrange(
    db: *mut RedliteDb,
    key: *const c_char,
    start: i64,
    end: i64,
) -> RedliteBytes {
    clear_error();
    let handle = get_db_ret!(db, RedliteBytes { data: ptr::null_mut(), len: 0 });

    let key = match cstr_to_str(key) {
        Ok(k) => k,
        Err(e) => {
            set_error(e);
            return RedliteBytes { data: ptr::null_mut(), len: 0 };
        }
    };

    let guard = handle.db.lock().unwrap();
    match guard.getrange(key, start, end) {
        Ok(v) => vec_to_bytes(v),
        Err(e) => {
            set_error(format!("GETRANGE failed: {}", e));
            RedliteBytes { data: ptr::null_mut(), len: 0 }
        }
    }
}

/// SETRANGE key offset value
#[no_mangle]
pub extern "C" fn redlite_setrange(
    db: *mut RedliteDb,
    key: *const c_char,
    offset: i64,
    value: *const u8,
    value_len: size_t,
) -> i64 {
    clear_error();
    let handle = get_db_ret!(db, -1);

    let key = match cstr_to_str(key) {
        Ok(k) => k,
        Err(e) => {
            set_error(e);
            return -1;
        }
    };

    let value = bytes_to_vec(value, value_len);
    let guard = handle.db.lock().unwrap();

    match guard.setrange(key, offset, &value) {
        Ok(len) => len,
        Err(e) => {
            set_error(format!("SETRANGE failed: {}", e));
            -1
        }
    }
}

/// INCR key
#[no_mangle]
pub extern "C" fn redlite_incr(db: *mut RedliteDb, key: *const c_char) -> i64 {
    clear_error();
    let handle = get_db_ret!(db, i64::MIN);

    let key = match cstr_to_str(key) {
        Ok(k) => k,
        Err(e) => {
            set_error(e);
            return i64::MIN;
        }
    };

    let guard = handle.db.lock().unwrap();
    match guard.incr(key) {
        Ok(v) => v,
        Err(e) => {
            set_error(format!("INCR failed: {}", e));
            i64::MIN
        }
    }
}

/// DECR key
#[no_mangle]
pub extern "C" fn redlite_decr(db: *mut RedliteDb, key: *const c_char) -> i64 {
    clear_error();
    let handle = get_db_ret!(db, i64::MIN);

    let key = match cstr_to_str(key) {
        Ok(k) => k,
        Err(e) => {
            set_error(e);
            return i64::MIN;
        }
    };

    let guard = handle.db.lock().unwrap();
    match guard.decr(key) {
        Ok(v) => v,
        Err(e) => {
            set_error(format!("DECR failed: {}", e));
            i64::MIN
        }
    }
}

/// INCRBY key increment
#[no_mangle]
pub extern "C" fn redlite_incrby(db: *mut RedliteDb, key: *const c_char, increment: i64) -> i64 {
    clear_error();
    let handle = get_db_ret!(db, i64::MIN);

    let key = match cstr_to_str(key) {
        Ok(k) => k,
        Err(e) => {
            set_error(e);
            return i64::MIN;
        }
    };

    let guard = handle.db.lock().unwrap();
    match guard.incrby(key, increment) {
        Ok(v) => v,
        Err(e) => {
            set_error(format!("INCRBY failed: {}", e));
            i64::MIN
        }
    }
}

/// DECRBY key decrement
#[no_mangle]
pub extern "C" fn redlite_decrby(db: *mut RedliteDb, key: *const c_char, decrement: i64) -> i64 {
    clear_error();
    let handle = get_db_ret!(db, i64::MIN);

    let key = match cstr_to_str(key) {
        Ok(k) => k,
        Err(e) => {
            set_error(e);
            return i64::MIN;
        }
    };

    let guard = handle.db.lock().unwrap();
    match guard.decrby(key, decrement) {
        Ok(v) => v,
        Err(e) => {
            set_error(format!("DECRBY failed: {}", e));
            i64::MIN
        }
    }
}

/// INCRBYFLOAT key increment
/// Returns result as string (caller must free), NULL on error
#[no_mangle]
pub extern "C" fn redlite_incrbyfloat(
    db: *mut RedliteDb,
    key: *const c_char,
    increment: f64,
) -> *mut c_char {
    clear_error();
    let handle = get_db_ret!(db, ptr::null_mut());

    let key = match cstr_to_str(key) {
        Ok(k) => k,
        Err(e) => {
            set_error(e);
            return ptr::null_mut();
        }
    };

    let guard = handle.db.lock().unwrap();
    match guard.incrbyfloat(key, increment) {
        Ok(v) => CString::new(v).unwrap().into_raw(),
        Err(e) => {
            set_error(format!("INCRBYFLOAT failed: {}", e));
            ptr::null_mut()
        }
    }
}

/// MGET key [key ...]
#[no_mangle]
pub extern "C" fn redlite_mget(
    db: *mut RedliteDb,
    keys: *const *const c_char,
    keys_len: size_t,
) -> RedliteBytesArray {
    clear_error();
    let handle = get_db_ret!(db, RedliteBytesArray { items: ptr::null_mut(), len: 0 });

    if keys.is_null() || keys_len == 0 {
        return RedliteBytesArray { items: ptr::null_mut(), len: 0 };
    }

    let keys_slice = unsafe { slice::from_raw_parts(keys, keys_len) };
    let keys_vec: Result<Vec<&str>, _> = keys_slice
        .iter()
        .map(|&k| cstr_to_str(k))
        .collect();

    let keys_vec = match keys_vec {
        Ok(v) => v,
        Err(e) => {
            set_error(e);
            return RedliteBytesArray { items: ptr::null_mut(), len: 0 };
        }
    };

    let guard = handle.db.lock().unwrap();
    let results = guard.mget(&keys_vec);

    // Convert Vec<Option<Vec<u8>>> to RedliteBytesArray
    let items: Vec<RedliteBytes> = results.into_iter().map(opt_vec_to_bytes).collect();
    vecs_to_bytes_array(items.into_iter().map(|rb| {
        if rb.data.is_null() {
            Vec::new()
        } else {
            unsafe { slice::from_raw_parts(rb.data, rb.len) }.to_vec()
        }
    }).collect())
}

/// MSET key value [key value ...]
#[no_mangle]
pub extern "C" fn redlite_mset(
    db: *mut RedliteDb,
    pairs: *const RedliteKV,
    pairs_len: size_t,
) -> c_int {
    clear_error();
    let handle = get_db!(db);

    if pairs.is_null() || pairs_len == 0 {
        return 0;
    }

    let pairs_slice = unsafe { slice::from_raw_parts(pairs, pairs_len) };
    let mut kv_pairs: Vec<(&str, Vec<u8>)> = Vec::with_capacity(pairs_len);

    for pair in pairs_slice {
        let key = match cstr_to_str(pair.key) {
            Ok(k) => k,
            Err(e) => {
                set_error(e);
                return -1;
            }
        };
        let value = bytes_to_vec(pair.value, pair.value_len);
        kv_pairs.push((key, value));
    }

    let kv_refs: Vec<(&str, &[u8])> = kv_pairs.iter().map(|(k, v)| (*k, v.as_slice())).collect();

    let guard = handle.db.lock().unwrap();
    match guard.mset(&kv_refs) {
        Ok(()) => 0,
        Err(e) => {
            set_error(format!("MSET failed: {}", e));
            -1
        }
    }
}

// =============================================================================
// Key Commands
// =============================================================================

/// DEL key [key ...]
/// Returns number of keys deleted
#[no_mangle]
pub extern "C" fn redlite_del(
    db: *mut RedliteDb,
    keys: *const *const c_char,
    keys_len: size_t,
) -> i64 {
    clear_error();
    let handle = get_db_ret!(db, -1);

    if keys.is_null() || keys_len == 0 {
        return 0;
    }

    let keys_slice = unsafe { slice::from_raw_parts(keys, keys_len) };
    let keys_vec: Result<Vec<&str>, _> = keys_slice
        .iter()
        .map(|&k| cstr_to_str(k))
        .collect();

    let keys_vec = match keys_vec {
        Ok(v) => v,
        Err(e) => {
            set_error(e);
            return -1;
        }
    };

    let guard = handle.db.lock().unwrap();
    match guard.del(&keys_vec) {
        Ok(n) => n,
        Err(e) => {
            set_error(format!("DEL failed: {}", e));
            -1
        }
    }
}

/// EXISTS key [key ...]
#[no_mangle]
pub extern "C" fn redlite_exists(
    db: *mut RedliteDb,
    keys: *const *const c_char,
    keys_len: size_t,
) -> i64 {
    clear_error();
    let handle = get_db_ret!(db, -1);

    if keys.is_null() || keys_len == 0 {
        return 0;
    }

    let keys_slice = unsafe { slice::from_raw_parts(keys, keys_len) };
    let keys_vec: Result<Vec<&str>, _> = keys_slice
        .iter()
        .map(|&k| cstr_to_str(k))
        .collect();

    let keys_vec = match keys_vec {
        Ok(v) => v,
        Err(e) => {
            set_error(e);
            return -1;
        }
    };

    let guard = handle.db.lock().unwrap();
    match guard.exists(&keys_vec) {
        Ok(n) => n,
        Err(e) => {
            set_error(format!("EXISTS failed: {}", e));
            -1
        }
    }
}

/// TYPE key
/// Returns type string (caller must free), NULL if key doesn't exist
#[no_mangle]
pub extern "C" fn redlite_type(db: *mut RedliteDb, key: *const c_char) -> *mut c_char {
    clear_error();
    let handle = get_db_ret!(db, ptr::null_mut());

    let key = match cstr_to_str(key) {
        Ok(k) => k,
        Err(e) => {
            set_error(e);
            return ptr::null_mut();
        }
    };

    let guard = handle.db.lock().unwrap();
    match guard.key_type(key) {
        Ok(Some(t)) => {
            let type_str = match t {
                redlite::KeyType::String => "string",
                redlite::KeyType::List => "list",
                redlite::KeyType::Set => "set",
                redlite::KeyType::ZSet => "zset",
                redlite::KeyType::Hash => "hash",
                redlite::KeyType::Stream => "stream",
            };
            CString::new(type_str).unwrap().into_raw()
        }
        Ok(None) => CString::new("none").unwrap().into_raw(),
        Err(e) => {
            set_error(format!("TYPE failed: {}", e));
            ptr::null_mut()
        }
    }
}

/// TTL key
/// Returns -2 if key doesn't exist, -1 if no TTL, otherwise seconds
#[no_mangle]
pub extern "C" fn redlite_ttl(db: *mut RedliteDb, key: *const c_char) -> i64 {
    clear_error();
    let handle = get_db_ret!(db, -3);

    let key = match cstr_to_str(key) {
        Ok(k) => k,
        Err(e) => {
            set_error(e);
            return -3;
        }
    };

    let guard = handle.db.lock().unwrap();
    match guard.ttl(key) {
        Ok(v) => v,
        Err(e) => {
            set_error(format!("TTL failed: {}", e));
            -3
        }
    }
}

/// PTTL key
#[no_mangle]
pub extern "C" fn redlite_pttl(db: *mut RedliteDb, key: *const c_char) -> i64 {
    clear_error();
    let handle = get_db_ret!(db, -3);

    let key = match cstr_to_str(key) {
        Ok(k) => k,
        Err(e) => {
            set_error(e);
            return -3;
        }
    };

    let guard = handle.db.lock().unwrap();
    match guard.pttl(key) {
        Ok(v) => v,
        Err(e) => {
            set_error(format!("PTTL failed: {}", e));
            -3
        }
    }
}

/// EXPIRE key seconds
#[no_mangle]
pub extern "C" fn redlite_expire(db: *mut RedliteDb, key: *const c_char, seconds: i64) -> c_int {
    clear_error();
    let handle = get_db!(db);

    let key = match cstr_to_str(key) {
        Ok(k) => k,
        Err(e) => {
            set_error(e);
            return -1;
        }
    };

    let guard = handle.db.lock().unwrap();
    match guard.expire(key, seconds) {
        Ok(true) => 1,
        Ok(false) => 0,
        Err(e) => {
            set_error(format!("EXPIRE failed: {}", e));
            -1
        }
    }
}

/// PEXPIRE key milliseconds
#[no_mangle]
pub extern "C" fn redlite_pexpire(db: *mut RedliteDb, key: *const c_char, milliseconds: i64) -> c_int {
    clear_error();
    let handle = get_db!(db);

    let key = match cstr_to_str(key) {
        Ok(k) => k,
        Err(e) => {
            set_error(e);
            return -1;
        }
    };

    let guard = handle.db.lock().unwrap();
    match guard.pexpire(key, milliseconds) {
        Ok(true) => 1,
        Ok(false) => 0,
        Err(e) => {
            set_error(format!("PEXPIRE failed: {}", e));
            -1
        }
    }
}

/// EXPIREAT key unix_timestamp
#[no_mangle]
pub extern "C" fn redlite_expireat(db: *mut RedliteDb, key: *const c_char, unix_seconds: i64) -> c_int {
    clear_error();
    let handle = get_db!(db);

    let key = match cstr_to_str(key) {
        Ok(k) => k,
        Err(e) => {
            set_error(e);
            return -1;
        }
    };

    let guard = handle.db.lock().unwrap();
    match guard.expireat(key, unix_seconds) {
        Ok(true) => 1,
        Ok(false) => 0,
        Err(e) => {
            set_error(format!("EXPIREAT failed: {}", e));
            -1
        }
    }
}

/// PEXPIREAT key unix_timestamp_ms
#[no_mangle]
pub extern "C" fn redlite_pexpireat(db: *mut RedliteDb, key: *const c_char, unix_ms: i64) -> c_int {
    clear_error();
    let handle = get_db!(db);

    let key = match cstr_to_str(key) {
        Ok(k) => k,
        Err(e) => {
            set_error(e);
            return -1;
        }
    };

    let guard = handle.db.lock().unwrap();
    match guard.pexpireat(key, unix_ms) {
        Ok(true) => 1,
        Ok(false) => 0,
        Err(e) => {
            set_error(format!("PEXPIREAT failed: {}", e));
            -1
        }
    }
}

/// PERSIST key
#[no_mangle]
pub extern "C" fn redlite_persist(db: *mut RedliteDb, key: *const c_char) -> c_int {
    clear_error();
    let handle = get_db!(db);

    let key = match cstr_to_str(key) {
        Ok(k) => k,
        Err(e) => {
            set_error(e);
            return -1;
        }
    };

    let guard = handle.db.lock().unwrap();
    match guard.persist(key) {
        Ok(true) => 1,
        Ok(false) => 0,
        Err(e) => {
            set_error(format!("PERSIST failed: {}", e));
            -1
        }
    }
}

/// RENAME key newkey
#[no_mangle]
pub extern "C" fn redlite_rename(
    db: *mut RedliteDb,
    key: *const c_char,
    newkey: *const c_char,
) -> c_int {
    clear_error();
    let handle = get_db!(db);

    let key = match cstr_to_str(key) {
        Ok(k) => k,
        Err(e) => {
            set_error(e);
            return -1;
        }
    };

    let newkey = match cstr_to_str(newkey) {
        Ok(k) => k,
        Err(e) => {
            set_error(e);
            return -1;
        }
    };

    let guard = handle.db.lock().unwrap();
    match guard.rename(key, newkey) {
        Ok(()) => 0,
        Err(e) => {
            set_error(format!("RENAME failed: {}", e));
            -1
        }
    }
}

/// RENAMENX key newkey
#[no_mangle]
pub extern "C" fn redlite_renamenx(
    db: *mut RedliteDb,
    key: *const c_char,
    newkey: *const c_char,
) -> c_int {
    clear_error();
    let handle = get_db!(db);

    let key = match cstr_to_str(key) {
        Ok(k) => k,
        Err(e) => {
            set_error(e);
            return -1;
        }
    };

    let newkey = match cstr_to_str(newkey) {
        Ok(k) => k,
        Err(e) => {
            set_error(e);
            return -1;
        }
    };

    let guard = handle.db.lock().unwrap();
    match guard.renamenx(key, newkey) {
        Ok(true) => 1,
        Ok(false) => 0,
        Err(e) => {
            set_error(format!("RENAMENX failed: {}", e));
            -1
        }
    }
}

/// KEYS pattern
#[no_mangle]
pub extern "C" fn redlite_keys(db: *mut RedliteDb, pattern: *const c_char) -> RedliteStringArray {
    clear_error();
    let handle = get_db_ret!(db, RedliteStringArray { strings: ptr::null_mut(), len: 0 });

    let pattern = match cstr_to_str(pattern) {
        Ok(p) => p,
        Err(e) => {
            set_error(e);
            return RedliteStringArray { strings: ptr::null_mut(), len: 0 };
        }
    };

    let guard = handle.db.lock().unwrap();
    match guard.keys(pattern) {
        Ok(keys) => strings_to_array(keys),
        Err(e) => {
            set_error(format!("KEYS failed: {}", e));
            RedliteStringArray { strings: ptr::null_mut(), len: 0 }
        }
    }
}

/// DBSIZE
#[no_mangle]
pub extern "C" fn redlite_dbsize(db: *mut RedliteDb) -> i64 {
    clear_error();
    let handle = get_db_ret!(db, -1);

    let guard = handle.db.lock().unwrap();
    match guard.dbsize() {
        Ok(n) => n,
        Err(e) => {
            set_error(format!("DBSIZE failed: {}", e));
            -1
        }
    }
}

/// FLUSHDB
#[no_mangle]
pub extern "C" fn redlite_flushdb(db: *mut RedliteDb) -> c_int {
    clear_error();
    let handle = get_db!(db);

    let guard = handle.db.lock().unwrap();
    match guard.flushdb() {
        Ok(()) => 0,
        Err(e) => {
            set_error(format!("FLUSHDB failed: {}", e));
            -1
        }
    }
}

/// SELECT db
#[no_mangle]
pub extern "C" fn redlite_select(db: *mut RedliteDb, db_num: c_int) -> c_int {
    clear_error();
    let handle = get_db!(db);

    let mut guard = handle.db.lock().unwrap();
    match guard.select(db_num) {
        Ok(()) => 0,
        Err(e) => {
            set_error(format!("SELECT failed: {}", e));
            -1
        }
    }
}

// =============================================================================
// Hash Commands
// =============================================================================

/// HSET key field value [field value ...]
#[no_mangle]
pub extern "C" fn redlite_hset(
    db: *mut RedliteDb,
    key: *const c_char,
    fields: *const *const c_char,
    values: *const RedliteBytes,
    count: size_t,
) -> i64 {
    clear_error();
    let handle = get_db_ret!(db, -1);

    let key = match cstr_to_str(key) {
        Ok(k) => k,
        Err(e) => {
            set_error(e);
            return -1;
        }
    };

    if fields.is_null() || values.is_null() || count == 0 {
        return 0;
    }

    let fields_slice = unsafe { slice::from_raw_parts(fields, count) };
    let values_slice = unsafe { slice::from_raw_parts(values, count) };

    let mut pairs: Vec<(&str, Vec<u8>)> = Vec::with_capacity(count);
    for i in 0..count {
        let field = match cstr_to_str(fields_slice[i]) {
            Ok(f) => f,
            Err(e) => {
                set_error(e);
                return -1;
            }
        };
        let value = if values_slice[i].data.is_null() {
            Vec::new()
        } else {
            unsafe { slice::from_raw_parts(values_slice[i].data, values_slice[i].len) }.to_vec()
        };
        pairs.push((field, value));
    }

    let pairs_refs: Vec<(&str, &[u8])> = pairs.iter().map(|(k, v)| (*k, v.as_slice())).collect();

    let guard = handle.db.lock().unwrap();
    match guard.hset(key, &pairs_refs) {
        Ok(n) => n,
        Err(e) => {
            set_error(format!("HSET failed: {}", e));
            -1
        }
    }
}

/// HGET key field
#[no_mangle]
pub extern "C" fn redlite_hget(
    db: *mut RedliteDb,
    key: *const c_char,
    field: *const c_char,
) -> RedliteBytes {
    clear_error();
    let handle = get_db_ret!(db, RedliteBytes { data: ptr::null_mut(), len: 0 });

    let key = match cstr_to_str(key) {
        Ok(k) => k,
        Err(e) => {
            set_error(e);
            return RedliteBytes { data: ptr::null_mut(), len: 0 };
        }
    };

    let field = match cstr_to_str(field) {
        Ok(f) => f,
        Err(e) => {
            set_error(e);
            return RedliteBytes { data: ptr::null_mut(), len: 0 };
        }
    };

    let guard = handle.db.lock().unwrap();
    match guard.hget(key, field) {
        Ok(v) => opt_vec_to_bytes(v),
        Err(e) => {
            set_error(format!("HGET failed: {}", e));
            RedliteBytes { data: ptr::null_mut(), len: 0 }
        }
    }
}

/// HDEL key field [field ...]
#[no_mangle]
pub extern "C" fn redlite_hdel(
    db: *mut RedliteDb,
    key: *const c_char,
    fields: *const *const c_char,
    fields_len: size_t,
) -> i64 {
    clear_error();
    let handle = get_db_ret!(db, -1);

    let key = match cstr_to_str(key) {
        Ok(k) => k,
        Err(e) => {
            set_error(e);
            return -1;
        }
    };

    if fields.is_null() || fields_len == 0 {
        return 0;
    }

    let fields_slice = unsafe { slice::from_raw_parts(fields, fields_len) };
    let fields_vec: Result<Vec<&str>, _> = fields_slice
        .iter()
        .map(|&f| cstr_to_str(f))
        .collect();

    let fields_vec = match fields_vec {
        Ok(v) => v,
        Err(e) => {
            set_error(e);
            return -1;
        }
    };

    let guard = handle.db.lock().unwrap();
    match guard.hdel(key, &fields_vec) {
        Ok(n) => n,
        Err(e) => {
            set_error(format!("HDEL failed: {}", e));
            -1
        }
    }
}

/// HEXISTS key field
#[no_mangle]
pub extern "C" fn redlite_hexists(
    db: *mut RedliteDb,
    key: *const c_char,
    field: *const c_char,
) -> c_int {
    clear_error();
    let handle = get_db!(db);

    let key = match cstr_to_str(key) {
        Ok(k) => k,
        Err(e) => {
            set_error(e);
            return -1;
        }
    };

    let field = match cstr_to_str(field) {
        Ok(f) => f,
        Err(e) => {
            set_error(e);
            return -1;
        }
    };

    let guard = handle.db.lock().unwrap();
    match guard.hexists(key, field) {
        Ok(true) => 1,
        Ok(false) => 0,
        Err(e) => {
            set_error(format!("HEXISTS failed: {}", e));
            -1
        }
    }
}

/// HLEN key
#[no_mangle]
pub extern "C" fn redlite_hlen(db: *mut RedliteDb, key: *const c_char) -> i64 {
    clear_error();
    let handle = get_db_ret!(db, -1);

    let key = match cstr_to_str(key) {
        Ok(k) => k,
        Err(e) => {
            set_error(e);
            return -1;
        }
    };

    let guard = handle.db.lock().unwrap();
    match guard.hlen(key) {
        Ok(n) => n,
        Err(e) => {
            set_error(format!("HLEN failed: {}", e));
            -1
        }
    }
}

/// HKEYS key
#[no_mangle]
pub extern "C" fn redlite_hkeys(db: *mut RedliteDb, key: *const c_char) -> RedliteStringArray {
    clear_error();
    let handle = get_db_ret!(db, RedliteStringArray { strings: ptr::null_mut(), len: 0 });

    let key = match cstr_to_str(key) {
        Ok(k) => k,
        Err(e) => {
            set_error(e);
            return RedliteStringArray { strings: ptr::null_mut(), len: 0 };
        }
    };

    let guard = handle.db.lock().unwrap();
    match guard.hkeys(key) {
        Ok(keys) => strings_to_array(keys),
        Err(e) => {
            set_error(format!("HKEYS failed: {}", e));
            RedliteStringArray { strings: ptr::null_mut(), len: 0 }
        }
    }
}

/// HVALS key
#[no_mangle]
pub extern "C" fn redlite_hvals(db: *mut RedliteDb, key: *const c_char) -> RedliteBytesArray {
    clear_error();
    let handle = get_db_ret!(db, RedliteBytesArray { items: ptr::null_mut(), len: 0 });

    let key = match cstr_to_str(key) {
        Ok(k) => k,
        Err(e) => {
            set_error(e);
            return RedliteBytesArray { items: ptr::null_mut(), len: 0 };
        }
    };

    let guard = handle.db.lock().unwrap();
    match guard.hvals(key) {
        Ok(vals) => vecs_to_bytes_array(vals),
        Err(e) => {
            set_error(format!("HVALS failed: {}", e));
            RedliteBytesArray { items: ptr::null_mut(), len: 0 }
        }
    }
}

/// HINCRBY key field increment
#[no_mangle]
pub extern "C" fn redlite_hincrby(
    db: *mut RedliteDb,
    key: *const c_char,
    field: *const c_char,
    increment: i64,
) -> i64 {
    clear_error();
    let handle = get_db_ret!(db, i64::MIN);

    let key = match cstr_to_str(key) {
        Ok(k) => k,
        Err(e) => {
            set_error(e);
            return i64::MIN;
        }
    };

    let field = match cstr_to_str(field) {
        Ok(f) => f,
        Err(e) => {
            set_error(e);
            return i64::MIN;
        }
    };

    let guard = handle.db.lock().unwrap();
    match guard.hincrby(key, field, increment) {
        Ok(v) => v,
        Err(e) => {
            set_error(format!("HINCRBY failed: {}", e));
            i64::MIN
        }
    }
}

/// HGETALL key
/// Returns alternating field-value pairs
#[no_mangle]
pub extern "C" fn redlite_hgetall(db: *mut RedliteDb, key: *const c_char) -> RedliteBytesArray {
    clear_error();
    let handle = get_db_ret!(db, RedliteBytesArray { items: ptr::null_mut(), len: 0 });

    let key = match cstr_to_str(key) {
        Ok(k) => k,
        Err(e) => {
            set_error(e);
            return RedliteBytesArray { items: ptr::null_mut(), len: 0 };
        }
    };

    let guard = handle.db.lock().unwrap();
    match guard.hgetall(key) {
        Ok(pairs) => {
            // Convert Vec<(String, Vec<u8>)> to flat array of field-value pairs
            let mut flat: Vec<Vec<u8>> = Vec::with_capacity(pairs.len() * 2);
            for (field, value) in pairs {
                flat.push(field.into_bytes());
                flat.push(value);
            }
            vecs_to_bytes_array(flat)
        }
        Err(e) => {
            set_error(format!("HGETALL failed: {}", e));
            RedliteBytesArray { items: ptr::null_mut(), len: 0 }
        }
    }
}

/// HMGET key field [field ...]
#[no_mangle]
pub extern "C" fn redlite_hmget(
    db: *mut RedliteDb,
    key: *const c_char,
    fields: *const *const c_char,
    fields_len: size_t,
) -> RedliteBytesArray {
    clear_error();
    let handle = get_db_ret!(db, RedliteBytesArray { items: ptr::null_mut(), len: 0 });

    let key = match cstr_to_str(key) {
        Ok(k) => k,
        Err(e) => {
            set_error(e);
            return RedliteBytesArray { items: ptr::null_mut(), len: 0 };
        }
    };

    if fields.is_null() || fields_len == 0 {
        return RedliteBytesArray { items: ptr::null_mut(), len: 0 };
    }

    let fields_slice = unsafe { slice::from_raw_parts(fields, fields_len) };
    let fields_vec: Result<Vec<&str>, _> = fields_slice
        .iter()
        .map(|&f| cstr_to_str(f))
        .collect();

    let fields_vec = match fields_vec {
        Ok(v) => v,
        Err(e) => {
            set_error(e);
            return RedliteBytesArray { items: ptr::null_mut(), len: 0 };
        }
    };

    let guard = handle.db.lock().unwrap();
    match guard.hmget(key, &fields_vec) {
        Ok(results) => {
            let vecs: Vec<Vec<u8>> = results.into_iter().map(|opt| opt.unwrap_or_default()).collect();
            vecs_to_bytes_array(vecs)
        }
        Err(e) => {
            set_error(format!("HMGET failed: {}", e));
            RedliteBytesArray { items: ptr::null_mut(), len: 0 }
        }
    }
}

// =============================================================================
// List Commands
// =============================================================================

/// LPUSH key value [value ...]
#[no_mangle]
pub extern "C" fn redlite_lpush(
    db: *mut RedliteDb,
    key: *const c_char,
    values: *const RedliteBytes,
    values_len: size_t,
) -> i64 {
    clear_error();
    let handle = get_db_ret!(db, -1);

    let key = match cstr_to_str(key) {
        Ok(k) => k,
        Err(e) => {
            set_error(e);
            return -1;
        }
    };

    if values.is_null() || values_len == 0 {
        set_error("No values provided".to_string());
        return -1;
    }

    let values_slice = unsafe { slice::from_raw_parts(values, values_len) };
    let values_vecs: Vec<Vec<u8>> = values_slice
        .iter()
        .map(|b| {
            if b.data.is_null() {
                Vec::new()
            } else {
                unsafe { slice::from_raw_parts(b.data, b.len) }.to_vec()
            }
        })
        .collect();

    let values_refs: Vec<&[u8]> = values_vecs.iter().map(|v| v.as_slice()).collect();

    let guard = handle.db.lock().unwrap();
    match guard.lpush(key, &values_refs) {
        Ok(n) => n,
        Err(e) => {
            set_error(format!("LPUSH failed: {}", e));
            -1
        }
    }
}

/// RPUSH key value [value ...]
#[no_mangle]
pub extern "C" fn redlite_rpush(
    db: *mut RedliteDb,
    key: *const c_char,
    values: *const RedliteBytes,
    values_len: size_t,
) -> i64 {
    clear_error();
    let handle = get_db_ret!(db, -1);

    let key = match cstr_to_str(key) {
        Ok(k) => k,
        Err(e) => {
            set_error(e);
            return -1;
        }
    };

    if values.is_null() || values_len == 0 {
        set_error("No values provided".to_string());
        return -1;
    }

    let values_slice = unsafe { slice::from_raw_parts(values, values_len) };
    let values_vecs: Vec<Vec<u8>> = values_slice
        .iter()
        .map(|b| {
            if b.data.is_null() {
                Vec::new()
            } else {
                unsafe { slice::from_raw_parts(b.data, b.len) }.to_vec()
            }
        })
        .collect();

    let values_refs: Vec<&[u8]> = values_vecs.iter().map(|v| v.as_slice()).collect();

    let guard = handle.db.lock().unwrap();
    match guard.rpush(key, &values_refs) {
        Ok(n) => n,
        Err(e) => {
            set_error(format!("RPUSH failed: {}", e));
            -1
        }
    }
}

/// LPOP key [count]
#[no_mangle]
pub extern "C" fn redlite_lpop(
    db: *mut RedliteDb,
    key: *const c_char,
    count: size_t,
) -> RedliteBytesArray {
    clear_error();
    let handle = get_db_ret!(db, RedliteBytesArray { items: ptr::null_mut(), len: 0 });

    let key = match cstr_to_str(key) {
        Ok(k) => k,
        Err(e) => {
            set_error(e);
            return RedliteBytesArray { items: ptr::null_mut(), len: 0 };
        }
    };

    let count_opt = if count > 0 { Some(count) } else { Some(1) };

    let guard = handle.db.lock().unwrap();
    match guard.lpop(key, count_opt) {
        Ok(vals) => vecs_to_bytes_array(vals),
        Err(e) => {
            set_error(format!("LPOP failed: {}", e));
            RedliteBytesArray { items: ptr::null_mut(), len: 0 }
        }
    }
}

/// RPOP key [count]
#[no_mangle]
pub extern "C" fn redlite_rpop(
    db: *mut RedliteDb,
    key: *const c_char,
    count: size_t,
) -> RedliteBytesArray {
    clear_error();
    let handle = get_db_ret!(db, RedliteBytesArray { items: ptr::null_mut(), len: 0 });

    let key = match cstr_to_str(key) {
        Ok(k) => k,
        Err(e) => {
            set_error(e);
            return RedliteBytesArray { items: ptr::null_mut(), len: 0 };
        }
    };

    let count_opt = if count > 0 { Some(count) } else { Some(1) };

    let guard = handle.db.lock().unwrap();
    match guard.rpop(key, count_opt) {
        Ok(vals) => vecs_to_bytes_array(vals),
        Err(e) => {
            set_error(format!("RPOP failed: {}", e));
            RedliteBytesArray { items: ptr::null_mut(), len: 0 }
        }
    }
}

/// LLEN key
#[no_mangle]
pub extern "C" fn redlite_llen(db: *mut RedliteDb, key: *const c_char) -> i64 {
    clear_error();
    let handle = get_db_ret!(db, -1);

    let key = match cstr_to_str(key) {
        Ok(k) => k,
        Err(e) => {
            set_error(e);
            return -1;
        }
    };

    let guard = handle.db.lock().unwrap();
    match guard.llen(key) {
        Ok(n) => n,
        Err(e) => {
            set_error(format!("LLEN failed: {}", e));
            -1
        }
    }
}

/// LRANGE key start stop
#[no_mangle]
pub extern "C" fn redlite_lrange(
    db: *mut RedliteDb,
    key: *const c_char,
    start: i64,
    stop: i64,
) -> RedliteBytesArray {
    clear_error();
    let handle = get_db_ret!(db, RedliteBytesArray { items: ptr::null_mut(), len: 0 });

    let key = match cstr_to_str(key) {
        Ok(k) => k,
        Err(e) => {
            set_error(e);
            return RedliteBytesArray { items: ptr::null_mut(), len: 0 };
        }
    };

    let guard = handle.db.lock().unwrap();
    match guard.lrange(key, start, stop) {
        Ok(vals) => vecs_to_bytes_array(vals),
        Err(e) => {
            set_error(format!("LRANGE failed: {}", e));
            RedliteBytesArray { items: ptr::null_mut(), len: 0 }
        }
    }
}

/// LINDEX key index
#[no_mangle]
pub extern "C" fn redlite_lindex(
    db: *mut RedliteDb,
    key: *const c_char,
    index: i64,
) -> RedliteBytes {
    clear_error();
    let handle = get_db_ret!(db, RedliteBytes { data: ptr::null_mut(), len: 0 });

    let key = match cstr_to_str(key) {
        Ok(k) => k,
        Err(e) => {
            set_error(e);
            return RedliteBytes { data: ptr::null_mut(), len: 0 };
        }
    };

    let guard = handle.db.lock().unwrap();
    match guard.lindex(key, index) {
        Ok(v) => opt_vec_to_bytes(v),
        Err(e) => {
            set_error(format!("LINDEX failed: {}", e));
            RedliteBytes { data: ptr::null_mut(), len: 0 }
        }
    }
}

// =============================================================================
// Set Commands
// =============================================================================

/// SADD key member [member ...]
#[no_mangle]
pub extern "C" fn redlite_sadd(
    db: *mut RedliteDb,
    key: *const c_char,
    members: *const RedliteBytes,
    members_len: size_t,
) -> i64 {
    clear_error();
    let handle = get_db_ret!(db, -1);

    let key = match cstr_to_str(key) {
        Ok(k) => k,
        Err(e) => {
            set_error(e);
            return -1;
        }
    };

    if members.is_null() || members_len == 0 {
        return 0;
    }

    let members_slice = unsafe { slice::from_raw_parts(members, members_len) };
    let members_vecs: Vec<Vec<u8>> = members_slice
        .iter()
        .map(|b| {
            if b.data.is_null() {
                Vec::new()
            } else {
                unsafe { slice::from_raw_parts(b.data, b.len) }.to_vec()
            }
        })
        .collect();

    let members_refs: Vec<&[u8]> = members_vecs.iter().map(|v| v.as_slice()).collect();

    let guard = handle.db.lock().unwrap();
    match guard.sadd(key, &members_refs) {
        Ok(n) => n,
        Err(e) => {
            set_error(format!("SADD failed: {}", e));
            -1
        }
    }
}

/// SREM key member [member ...]
#[no_mangle]
pub extern "C" fn redlite_srem(
    db: *mut RedliteDb,
    key: *const c_char,
    members: *const RedliteBytes,
    members_len: size_t,
) -> i64 {
    clear_error();
    let handle = get_db_ret!(db, -1);

    let key = match cstr_to_str(key) {
        Ok(k) => k,
        Err(e) => {
            set_error(e);
            return -1;
        }
    };

    if members.is_null() || members_len == 0 {
        return 0;
    }

    let members_slice = unsafe { slice::from_raw_parts(members, members_len) };
    let members_vecs: Vec<Vec<u8>> = members_slice
        .iter()
        .map(|b| {
            if b.data.is_null() {
                Vec::new()
            } else {
                unsafe { slice::from_raw_parts(b.data, b.len) }.to_vec()
            }
        })
        .collect();

    let members_refs: Vec<&[u8]> = members_vecs.iter().map(|v| v.as_slice()).collect();

    let guard = handle.db.lock().unwrap();
    match guard.srem(key, &members_refs) {
        Ok(n) => n,
        Err(e) => {
            set_error(format!("SREM failed: {}", e));
            -1
        }
    }
}

/// SMEMBERS key
#[no_mangle]
pub extern "C" fn redlite_smembers(db: *mut RedliteDb, key: *const c_char) -> RedliteBytesArray {
    clear_error();
    let handle = get_db_ret!(db, RedliteBytesArray { items: ptr::null_mut(), len: 0 });

    let key = match cstr_to_str(key) {
        Ok(k) => k,
        Err(e) => {
            set_error(e);
            return RedliteBytesArray { items: ptr::null_mut(), len: 0 };
        }
    };

    let guard = handle.db.lock().unwrap();
    match guard.smembers(key) {
        Ok(members) => vecs_to_bytes_array(members),
        Err(e) => {
            set_error(format!("SMEMBERS failed: {}", e));
            RedliteBytesArray { items: ptr::null_mut(), len: 0 }
        }
    }
}

/// SISMEMBER key member
#[no_mangle]
pub extern "C" fn redlite_sismember(
    db: *mut RedliteDb,
    key: *const c_char,
    member: *const u8,
    member_len: size_t,
) -> c_int {
    clear_error();
    let handle = get_db!(db);

    let key = match cstr_to_str(key) {
        Ok(k) => k,
        Err(e) => {
            set_error(e);
            return -1;
        }
    };

    let member = bytes_to_vec(member, member_len);

    let guard = handle.db.lock().unwrap();
    match guard.sismember(key, &member) {
        Ok(true) => 1,
        Ok(false) => 0,
        Err(e) => {
            set_error(format!("SISMEMBER failed: {}", e));
            -1
        }
    }
}

/// SCARD key
#[no_mangle]
pub extern "C" fn redlite_scard(db: *mut RedliteDb, key: *const c_char) -> i64 {
    clear_error();
    let handle = get_db_ret!(db, -1);

    let key = match cstr_to_str(key) {
        Ok(k) => k,
        Err(e) => {
            set_error(e);
            return -1;
        }
    };

    let guard = handle.db.lock().unwrap();
    match guard.scard(key) {
        Ok(n) => n,
        Err(e) => {
            set_error(format!("SCARD failed: {}", e));
            -1
        }
    }
}

// =============================================================================
// Sorted Set Commands
// =============================================================================

/// ZADD key score member [score member ...]
#[no_mangle]
pub extern "C" fn redlite_zadd(
    db: *mut RedliteDb,
    key: *const c_char,
    members: *const RedliteZMember,
    members_len: size_t,
) -> i64 {
    clear_error();
    let handle = get_db_ret!(db, -1);

    let key = match cstr_to_str(key) {
        Ok(k) => k,
        Err(e) => {
            set_error(e);
            return -1;
        }
    };

    if members.is_null() || members_len == 0 {
        return 0;
    }

    let members_slice = unsafe { slice::from_raw_parts(members, members_len) };
    let zmembers: Vec<redlite::ZMember> = members_slice
        .iter()
        .map(|m| {
            let member = if m.member.is_null() {
                Vec::new()
            } else {
                unsafe { slice::from_raw_parts(m.member, m.member_len) }.to_vec()
            };
            redlite::ZMember {
                score: m.score,
                member,
            }
        })
        .collect();

    let guard = handle.db.lock().unwrap();
    match guard.zadd(key, &zmembers) {
        Ok(n) => n,
        Err(e) => {
            set_error(format!("ZADD failed: {}", e));
            -1
        }
    }
}

/// ZREM key member [member ...]
#[no_mangle]
pub extern "C" fn redlite_zrem(
    db: *mut RedliteDb,
    key: *const c_char,
    members: *const RedliteBytes,
    members_len: size_t,
) -> i64 {
    clear_error();
    let handle = get_db_ret!(db, -1);

    let key = match cstr_to_str(key) {
        Ok(k) => k,
        Err(e) => {
            set_error(e);
            return -1;
        }
    };

    if members.is_null() || members_len == 0 {
        return 0;
    }

    let members_slice = unsafe { slice::from_raw_parts(members, members_len) };
    let members_vecs: Vec<Vec<u8>> = members_slice
        .iter()
        .map(|b| {
            if b.data.is_null() {
                Vec::new()
            } else {
                unsafe { slice::from_raw_parts(b.data, b.len) }.to_vec()
            }
        })
        .collect();

    let members_refs: Vec<&[u8]> = members_vecs.iter().map(|v| v.as_slice()).collect();

    let guard = handle.db.lock().unwrap();
    match guard.zrem(key, &members_refs) {
        Ok(n) => n,
        Err(e) => {
            set_error(format!("ZREM failed: {}", e));
            -1
        }
    }
}

/// ZSCORE key member
/// Returns NaN if not found or on error
#[no_mangle]
pub extern "C" fn redlite_zscore(
    db: *mut RedliteDb,
    key: *const c_char,
    member: *const u8,
    member_len: size_t,
) -> f64 {
    clear_error();
    let handle = get_db_ret!(db, f64::NAN);

    let key = match cstr_to_str(key) {
        Ok(k) => k,
        Err(e) => {
            set_error(e);
            return f64::NAN;
        }
    };

    let member = bytes_to_vec(member, member_len);

    let guard = handle.db.lock().unwrap();
    match guard.zscore(key, &member) {
        Ok(Some(score)) => score,
        Ok(None) => f64::NAN,
        Err(e) => {
            set_error(format!("ZSCORE failed: {}", e));
            f64::NAN
        }
    }
}

/// ZCARD key
#[no_mangle]
pub extern "C" fn redlite_zcard(db: *mut RedliteDb, key: *const c_char) -> i64 {
    clear_error();
    let handle = get_db_ret!(db, -1);

    let key = match cstr_to_str(key) {
        Ok(k) => k,
        Err(e) => {
            set_error(e);
            return -1;
        }
    };

    let guard = handle.db.lock().unwrap();
    match guard.zcard(key) {
        Ok(n) => n,
        Err(e) => {
            set_error(format!("ZCARD failed: {}", e));
            -1
        }
    }
}

/// ZCOUNT key min max
#[no_mangle]
pub extern "C" fn redlite_zcount(
    db: *mut RedliteDb,
    key: *const c_char,
    min: f64,
    max: f64,
) -> i64 {
    clear_error();
    let handle = get_db_ret!(db, -1);

    let key = match cstr_to_str(key) {
        Ok(k) => k,
        Err(e) => {
            set_error(e);
            return -1;
        }
    };

    let guard = handle.db.lock().unwrap();
    match guard.zcount(key, min, max) {
        Ok(n) => n,
        Err(e) => {
            set_error(format!("ZCOUNT failed: {}", e));
            -1
        }
    }
}

/// ZINCRBY key increment member
#[no_mangle]
pub extern "C" fn redlite_zincrby(
    db: *mut RedliteDb,
    key: *const c_char,
    increment: f64,
    member: *const u8,
    member_len: size_t,
) -> f64 {
    clear_error();
    let handle = get_db_ret!(db, f64::NAN);

    let key = match cstr_to_str(key) {
        Ok(k) => k,
        Err(e) => {
            set_error(e);
            return f64::NAN;
        }
    };

    let member = bytes_to_vec(member, member_len);

    let guard = handle.db.lock().unwrap();
    match guard.zincrby(key, increment, &member) {
        Ok(score) => score,
        Err(e) => {
            set_error(format!("ZINCRBY failed: {}", e));
            f64::NAN
        }
    }
}

/// ZRANGE key start stop [withscores]
/// If withscores is true, returns alternating member-score pairs
#[no_mangle]
pub extern "C" fn redlite_zrange(
    db: *mut RedliteDb,
    key: *const c_char,
    start: i64,
    stop: i64,
    with_scores: c_int,
) -> RedliteBytesArray {
    clear_error();
    let handle = get_db_ret!(db, RedliteBytesArray { items: ptr::null_mut(), len: 0 });

    let key = match cstr_to_str(key) {
        Ok(k) => k,
        Err(e) => {
            set_error(e);
            return RedliteBytesArray { items: ptr::null_mut(), len: 0 };
        }
    };

    let guard = handle.db.lock().unwrap();
    match guard.zrange(key, start, stop, with_scores != 0) {
        Ok(members) => {
            if with_scores != 0 {
                // Return alternating member-score pairs
                let mut flat: Vec<Vec<u8>> = Vec::with_capacity(members.len() * 2);
                for zm in members {
                    flat.push(zm.member);
                    flat.push(zm.score.to_string().into_bytes());
                }
                vecs_to_bytes_array(flat)
            } else {
                // Return just members
                let members: Vec<Vec<u8>> = members.into_iter().map(|zm| zm.member).collect();
                vecs_to_bytes_array(members)
            }
        }
        Err(e) => {
            set_error(format!("ZRANGE failed: {}", e));
            RedliteBytesArray { items: ptr::null_mut(), len: 0 }
        }
    }
}

/// ZREVRANGE key start stop [withscores]
/// If withscores is true, returns alternating member-score pairs
#[no_mangle]
pub extern "C" fn redlite_zrevrange(
    db: *mut RedliteDb,
    key: *const c_char,
    start: i64,
    stop: i64,
    with_scores: c_int,
) -> RedliteBytesArray {
    clear_error();
    let handle = get_db_ret!(db, RedliteBytesArray { items: ptr::null_mut(), len: 0 });

    let key = match cstr_to_str(key) {
        Ok(k) => k,
        Err(e) => {
            set_error(e);
            return RedliteBytesArray { items: ptr::null_mut(), len: 0 };
        }
    };

    let guard = handle.db.lock().unwrap();
    match guard.zrevrange(key, start, stop, with_scores != 0) {
        Ok(members) => {
            if with_scores != 0 {
                // Return alternating member-score pairs
                let mut flat: Vec<Vec<u8>> = Vec::with_capacity(members.len() * 2);
                for zm in members {
                    flat.push(zm.member);
                    flat.push(zm.score.to_string().into_bytes());
                }
                vecs_to_bytes_array(flat)
            } else {
                // Return just members
                let members: Vec<Vec<u8>> = members.into_iter().map(|zm| zm.member).collect();
                vecs_to_bytes_array(members)
            }
        }
        Err(e) => {
            set_error(format!("ZREVRANGE failed: {}", e));
            RedliteBytesArray { items: ptr::null_mut(), len: 0 }
        }
    }
}

// =============================================================================
// Server Commands
// =============================================================================

/// VACUUM - compact the database
#[no_mangle]
pub extern "C" fn redlite_vacuum(db: *mut RedliteDb) -> i64 {
    clear_error();
    let handle = get_db_ret!(db, -1);

    let guard = handle.db.lock().unwrap();
    match guard.vacuum() {
        Ok(n) => n,
        Err(e) => {
            set_error(format!("VACUUM failed: {}", e));
            -1
        }
    }
}

/// Get library version
#[no_mangle]
pub extern "C" fn redlite_version() -> *mut c_char {
    CString::new(env!("CARGO_PKG_VERSION")).unwrap().into_raw()
}
