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

use libc::{c_char, c_int, size_t};
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

/// Stream ID (ms-seq)
#[repr(C)]
pub struct RedliteStreamId {
    pub ms: i64,
    pub seq: i64,
}

/// Stream entry field
#[repr(C)]
pub struct RedliteStreamField {
    pub key: *const u8,
    pub key_len: size_t,
    pub value: *const u8,
    pub value_len: size_t,
}

/// Stream entry
#[repr(C)]
pub struct RedliteStreamEntry {
    pub id: RedliteStreamId,
    pub fields: *mut RedliteStreamField,
    pub fields_len: size_t,
}

/// Stream entry array
#[repr(C)]
pub struct RedliteStreamEntryArray {
    pub entries: *mut RedliteStreamEntry,
    pub len: size_t,
}

/// SCAN result
#[repr(C)]
pub struct RedliteScanResult {
    pub cursor: *mut c_char,
    pub keys: RedliteStringArray,
}

/// HSCAN result (field-value pairs)
#[repr(C)]
pub struct RedliteHScanResult {
    pub cursor: *mut c_char,
    pub pairs: RedliteBytesArray, // Flattened: [field1, value1, field2, value2, ...]
}

/// SSCAN result
#[repr(C)]
pub struct RedliteSScanResult {
    pub cursor: *mut c_char,
    pub members: RedliteBytesArray,
}

/// ZSCAN member with score
#[repr(C)]
pub struct RedliteZScanMember {
    pub member: RedliteBytes,
    pub score: f64,
}

/// ZSCAN result
#[repr(C)]
pub struct RedliteZScanResult {
    pub cursor: *mut c_char,
    pub members: *mut RedliteZScanMember,
    pub len: size_t,
}

/// Consumer group information
#[repr(C)]
pub struct RedliteConsumerGroupInfo {
    pub name: *mut c_char,
    pub consumers: i64,
    pub pending: i64,
    pub last_delivered_id: RedliteStreamId,
}

/// Consumer group info array
#[repr(C)]
pub struct RedliteConsumerGroupInfoArray {
    pub groups: *mut RedliteConsumerGroupInfo,
    pub len: size_t,
}

/// Consumer information
#[repr(C)]
pub struct RedliteConsumerInfo {
    pub name: *mut c_char,
    pub pending: i64,
    pub idle: i64,
}

/// Consumer info array
#[repr(C)]
pub struct RedliteConsumerInfoArray {
    pub consumers: *mut RedliteConsumerInfo,
    pub len: size_t,
}

/// Stream information
#[repr(C)]
pub struct RedliteStreamInfo {
    pub length: i64,
    pub radix_tree_keys: i64,
    pub radix_tree_nodes: i64,
    pub last_generated_id: RedliteStreamId,
    pub first_entry: *mut RedliteStreamEntry,  // NULL if stream is empty
    pub last_entry: *mut RedliteStreamEntry,   // NULL if stream is empty
}

/// History entry
#[repr(C)]
pub struct RedliteHistoryEntry {
    pub timestamp: i64,
    pub value: RedliteBytes,
}

/// History entry array
#[repr(C)]
pub struct RedliteHistoryEntryArray {
    pub entries: *mut RedliteHistoryEntry,
    pub len: size_t,
}

/// Geospatial member with coordinates
#[repr(C)]
pub struct RedliteGeoMember {
    pub member: *mut c_char,
    pub longitude: f64,
    pub latitude: f64,
    pub dist: f64,  // distance if applicable, 0.0 otherwise
}

/// Geo member array
#[repr(C)]
pub struct RedliteGeoMemberArray {
    pub members: *mut RedliteGeoMember,
    pub len: size_t,
}

/// Geo position (lon, lat)
#[repr(C)]
pub struct RedliteGeoPos {
    pub longitude: f64,
    pub latitude: f64,
    pub exists: c_int,  // 1 if position exists, 0 if NULL
}

/// Geo position array
#[repr(C)]
pub struct RedliteGeoPosArray {
    pub positions: *mut RedliteGeoPos,
    pub len: size_t,
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

/// Free a SCAN result
#[no_mangle]
pub extern "C" fn redlite_free_scan_result(result: RedliteScanResult) {
    redlite_free_string(result.cursor);
    redlite_free_string_array(result.keys);
}

/// Free an HSCAN result
#[no_mangle]
pub extern "C" fn redlite_free_hscan_result(result: RedliteHScanResult) {
    redlite_free_string(result.cursor);
    redlite_free_bytes_array(result.pairs);
}

/// Free an SSCAN result
#[no_mangle]
pub extern "C" fn redlite_free_sscan_result(result: RedliteSScanResult) {
    redlite_free_string(result.cursor);
    redlite_free_bytes_array(result.members);
}

/// Free a ZSCAN result
#[no_mangle]
pub extern "C" fn redlite_free_zscan_result(result: RedliteZScanResult) {
    redlite_free_string(result.cursor);
    if !result.members.is_null() && result.len > 0 {
        unsafe {
            let members = Vec::from_raw_parts(result.members, result.len, result.len);
            for member in members {
                redlite_free_bytes(member.member);
            }
        }
    }
}

/// Free a stream entry
#[no_mangle]
pub extern "C" fn redlite_free_stream_entry(entry: RedliteStreamEntry) {
    if !entry.fields.is_null() && entry.fields_len > 0 {
        unsafe {
            drop(Vec::from_raw_parts(entry.fields, entry.fields_len, entry.fields_len));
        }
    }
}

/// Free a stream entry array
#[no_mangle]
pub extern "C" fn redlite_free_stream_entry_array(arr: RedliteStreamEntryArray) {
    if !arr.entries.is_null() && arr.len > 0 {
        unsafe {
            let entries = Vec::from_raw_parts(arr.entries, arr.len, arr.len);
            for entry in entries {
                redlite_free_stream_entry(entry);
            }
        }
    }
}

/// Free consumer group info array
#[no_mangle]
pub extern "C" fn redlite_free_consumer_group_info_array(arr: RedliteConsumerGroupInfoArray) {
    if !arr.groups.is_null() && arr.len > 0 {
        unsafe {
            let groups = Vec::from_raw_parts(arr.groups, arr.len, arr.len);
            for group in groups {
                if !group.name.is_null() {
                    let _ = CString::from_raw(group.name);
                }
            }
        }
    }
}

/// Free consumer info array
#[no_mangle]
pub extern "C" fn redlite_free_consumer_info_array(arr: RedliteConsumerInfoArray) {
    if !arr.consumers.is_null() && arr.len > 0 {
        unsafe {
            let consumers = Vec::from_raw_parts(arr.consumers, arr.len, arr.len);
            for consumer in consumers {
                if !consumer.name.is_null() {
                    let _ = CString::from_raw(consumer.name);
                }
            }
        }
    }
}

/// Free stream info
#[no_mangle]
pub extern "C" fn redlite_free_stream_info(info: RedliteStreamInfo) {
    if !info.first_entry.is_null() {
        unsafe {
            let entry = Box::from_raw(info.first_entry);
            redlite_free_stream_entry(*entry);
        }
    }
    if !info.last_entry.is_null() {
        unsafe {
            let entry = Box::from_raw(info.last_entry);
            redlite_free_stream_entry(*entry);
        }
    }
}

/// Free history entry array
#[no_mangle]
pub extern "C" fn redlite_free_history_entry_array(arr: RedliteHistoryEntryArray) {
    if !arr.entries.is_null() && arr.len > 0 {
        unsafe {
            let entries = Vec::from_raw_parts(arr.entries, arr.len, arr.len);
            for entry in entries {
                redlite_free_bytes(entry.value);
            }
        }
    }
}

/// Free geo member array
#[no_mangle]
pub extern "C" fn redlite_free_geo_member_array(arr: RedliteGeoMemberArray) {
    if !arr.members.is_null() && arr.len > 0 {
        unsafe {
            let members = Vec::from_raw_parts(arr.members, arr.len, arr.len);
            for member in members {
                if !member.member.is_null() {
                    let _ = CString::from_raw(member.member);
                }
            }
        }
    }
}

/// Free geo position array
#[no_mangle]
pub extern "C" fn redlite_free_geo_pos_array(arr: RedliteGeoPosArray) {
    if !arr.positions.is_null() && arr.len > 0 {
        unsafe {
            let _ = Vec::from_raw_parts(arr.positions, arr.len, arr.len);
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

/// SETNX key value
/// Returns 1 if key was set, 0 if key already exists
#[no_mangle]
pub extern "C" fn redlite_setnx(
    db: *mut RedliteDb,
    key: *const c_char,
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
    match guard.set_opts(key, &value, redlite::SetOptions::new().nx()) {
        Ok(true) => 1,
        Ok(false) => 0,
        Err(e) => {
            set_error(format!("SETNX failed: {}", e));
            -1
        }
    }
}

/// GETEX key [EX seconds | PX milliseconds | EXAT unix-time-seconds | PXAT unix-time-milliseconds | PERSIST]
/// ex_seconds: >0 to set EX, 0 to ignore
/// px_milliseconds: >0 to set PX, 0 to ignore
/// exat_timestamp: >0 to set EXAT, 0 to ignore
/// pxat_timestamp: >0 to set PXAT, 0 to ignore
/// persist: 1 to set PERSIST, 0 to ignore
/// Only one option should be non-zero/non-false
#[no_mangle]
pub extern "C" fn redlite_getex(
    db: *mut RedliteDb,
    key: *const c_char,
    ex_seconds: i64,
    px_milliseconds: i64,
    exat_timestamp: i64,
    pxat_timestamp: i64,
    persist: c_int,
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

    let option = if ex_seconds > 0 {
        Some(redlite::GetExOption::Ex(ex_seconds))
    } else if px_milliseconds > 0 {
        Some(redlite::GetExOption::Px(px_milliseconds))
    } else if exat_timestamp > 0 {
        Some(redlite::GetExOption::ExAt(exat_timestamp))
    } else if pxat_timestamp > 0 {
        Some(redlite::GetExOption::PxAt(pxat_timestamp))
    } else if persist != 0 {
        Some(redlite::GetExOption::Persist)
    } else {
        None
    };

    let guard = handle.db.lock().unwrap();
    match guard.getex(key, option) {
        Ok(v) => opt_vec_to_bytes(v),
        Err(e) => {
            set_error(format!("GETEX failed: {}", e));
            RedliteBytes { data: ptr::null_mut(), len: 0 }
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
                redlite::KeyType::Json => "ReJSON-RL",
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

/// HSETNX key field value
/// Returns 1 if field was set, 0 if field already exists
#[no_mangle]
pub extern "C" fn redlite_hsetnx(
    db: *mut RedliteDb,
    key: *const c_char,
    field: *const c_char,
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

    let field = match cstr_to_str(field) {
        Ok(f) => f,
        Err(e) => {
            set_error(e);
            return -1;
        }
    };

    let value = bytes_to_vec(value, value_len);

    let guard = handle.db.lock().unwrap();
    match guard.hsetnx(key, field, &value) {
        Ok(true) => 1,
        Ok(false) => 0,
        Err(e) => {
            set_error(format!("HSETNX failed: {}", e));
            -1
        }
    }
}

/// HINCRBYFLOAT key field increment
/// Returns result as string (caller must free), NULL on error
#[no_mangle]
pub extern "C" fn redlite_hincrbyfloat(
    db: *mut RedliteDb,
    key: *const c_char,
    field: *const c_char,
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

    let field = match cstr_to_str(field) {
        Ok(f) => f,
        Err(e) => {
            set_error(e);
            return ptr::null_mut();
        }
    };

    let guard = handle.db.lock().unwrap();
    match guard.hincrbyfloat(key, field, increment) {
        Ok(v) => CString::new(v).unwrap().into_raw(),
        Err(e) => {
            set_error(format!("HINCRBYFLOAT failed: {}", e));
            ptr::null_mut()
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

/// LSET key index value
#[no_mangle]
pub extern "C" fn redlite_lset(
    db: *mut RedliteDb,
    key: *const c_char,
    index: i64,
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
    match guard.lset(key, index, &value) {
        Ok(()) => 0,
        Err(e) => {
            set_error(format!("LSET failed: {}", e));
            -1
        }
    }
}

/// LTRIM key start stop
#[no_mangle]
pub extern "C" fn redlite_ltrim(
    db: *mut RedliteDb,
    key: *const c_char,
    start: i64,
    stop: i64,
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

    let guard = handle.db.lock().unwrap();
    match guard.ltrim(key, start, stop) {
        Ok(()) => 0,
        Err(e) => {
            set_error(format!("LTRIM failed: {}", e));
            -1
        }
    }
}

/// LREM key count element
/// Returns number of elements removed
#[no_mangle]
pub extern "C" fn redlite_lrem(
    db: *mut RedliteDb,
    key: *const c_char,
    count: i64,
    element: *const u8,
    element_len: size_t,
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

    let element = bytes_to_vec(element, element_len);

    let guard = handle.db.lock().unwrap();
    match guard.lrem(key, count, &element) {
        Ok(n) => n,
        Err(e) => {
            set_error(format!("LREM failed: {}", e));
            -1
        }
    }
}

/// LINSERT key BEFORE|AFTER pivot element
/// before: 1 for BEFORE, 0 for AFTER
/// Returns new length, -1 if pivot not found or on error
#[no_mangle]
pub extern "C" fn redlite_linsert(
    db: *mut RedliteDb,
    key: *const c_char,
    before: c_int,
    pivot: *const u8,
    pivot_len: size_t,
    element: *const u8,
    element_len: size_t,
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

    let pivot = bytes_to_vec(pivot, pivot_len);
    let element = bytes_to_vec(element, element_len);

    let guard = handle.db.lock().unwrap();
    match guard.linsert(key, before != 0, &pivot, &element) {
        Ok(n) => n,
        Err(e) => {
            set_error(format!("LINSERT failed: {}", e));
            -1
        }
    }
}

/// LPUSHX key element [element ...]
/// Returns new length or -1 on error
#[no_mangle]
pub extern "C" fn redlite_lpushx(
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
    match guard.lpushx(key, &values_refs) {
        Ok(n) => n,
        Err(e) => {
            set_error(format!("LPUSHX failed: {}", e));
            -1
        }
    }
}

/// RPUSHX key element [element ...]
/// Returns new length or -1 on error
#[no_mangle]
pub extern "C" fn redlite_rpushx(
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
    match guard.rpushx(key, &values_refs) {
        Ok(n) => n,
        Err(e) => {
            set_error(format!("RPUSHX failed: {}", e));
            -1
        }
    }
}

/// LMOVE source destination LEFT|RIGHT LEFT|RIGHT
/// wherefrom: 0 for LEFT, 1 for RIGHT
/// whereto: 0 for LEFT, 1 for RIGHT
/// Returns moved element or NULL on error/empty
#[no_mangle]
pub extern "C" fn redlite_lmove(
    db: *mut RedliteDb,
    source: *const c_char,
    destination: *const c_char,
    wherefrom: c_int,
    whereto: c_int,
) -> RedliteBytes {
    clear_error();
    let handle = get_db_ret!(db, RedliteBytes { data: ptr::null_mut(), len: 0 });

    let source_str = match cstr_to_str(source) {
        Ok(s) => s,
        Err(e) => {
            set_error(e);
            return RedliteBytes { data: ptr::null_mut(), len: 0 };
        }
    };

    let dest_str = match cstr_to_str(destination) {
        Ok(s) => s,
        Err(e) => {
            set_error(e);
            return RedliteBytes { data: ptr::null_mut(), len: 0 };
        }
    };

    use redlite::ListDirection;
    let from_dir = if wherefrom == 0 { ListDirection::Left } else { ListDirection::Right };
    let to_dir = if whereto == 0 { ListDirection::Left } else { ListDirection::Right };

    let guard = handle.db.lock().unwrap();
    match guard.lmove(source_str, dest_str, from_dir, to_dir) {
        Ok(Some(v)) => vec_to_bytes(v),
        Ok(None) => RedliteBytes { data: ptr::null_mut(), len: 0 },
        Err(e) => {
            set_error(format!("LMOVE failed: {}", e));
            RedliteBytes { data: ptr::null_mut(), len: 0 }
        }
    }
}

/// LPOS key element [RANK rank] [COUNT count] [MAXLEN maxlen]
/// Returns array of positions (use -1 for not found)
/// rank: optional rank parameter (0 for None)
/// count: number of matches to return (0 for all matches)
/// maxlen: max elements to scan (0 for no limit)
#[no_mangle]
pub extern "C" fn redlite_lpos(
    db: *mut RedliteDb,
    key: *const c_char,
    element: *const u8,
    element_len: size_t,
    rank: i64,
    count: size_t,
    maxlen: size_t,
) -> RedliteBytesArray {
    clear_error();
    let handle = get_db_ret!(db, RedliteBytesArray { items: ptr::null_mut(), len: 0 });

    let key_str = match cstr_to_str(key) {
        Ok(s) => s,
        Err(e) => {
            set_error(e);
            return RedliteBytesArray { items: ptr::null_mut(), len: 0 };
        }
    };

    let element_bytes = bytes_to_vec(element, element_len);
    let rank_opt = if rank == 0 { None } else { Some(rank) };
    let count_opt = if count == 0 { None } else { Some(count) };
    let maxlen_opt = if maxlen == 0 { None } else { Some(maxlen) };

    let guard = handle.db.lock().unwrap();
    match guard.lpos(key_str, &element_bytes, rank_opt, count_opt, maxlen_opt) {
        Ok(positions) => {
            // Convert Vec<i64> to bytes array where each position is encoded as 8 bytes
            let mut result_items = Vec::with_capacity(positions.len());
            for pos in positions {
                let bytes = pos.to_le_bytes().to_vec();
                result_items.push(vec_to_bytes(bytes));
            }

            let len = result_items.len();
            let ptr = result_items.as_mut_ptr();
            std::mem::forget(result_items);

            RedliteBytesArray { items: ptr, len }
        }
        Err(e) => {
            set_error(format!("LPOS failed: {}", e));
            RedliteBytesArray { items: ptr::null_mut(), len: 0 }
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

/// SPOP key [count]
#[no_mangle]
pub extern "C" fn redlite_spop(
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

    let count_opt = if count > 0 { Some(count) } else { None };

    let guard = handle.db.lock().unwrap();
    match guard.spop(key, count_opt) {
        Ok(members) => vecs_to_bytes_array(members),
        Err(e) => {
            set_error(format!("SPOP failed: {}", e));
            RedliteBytesArray { items: ptr::null_mut(), len: 0 }
        }
    }
}

/// SRANDMEMBER key [count]
/// If count is positive, returns up to count distinct elements
/// If count is negative, may return duplicates
#[no_mangle]
pub extern "C" fn redlite_srandmember(
    db: *mut RedliteDb,
    key: *const c_char,
    count: i64,
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

    let count_opt = if count != 0 { Some(count) } else { None };

    let guard = handle.db.lock().unwrap();
    match guard.srandmember(key, count_opt) {
        Ok(members) => vecs_to_bytes_array(members),
        Err(e) => {
            set_error(format!("SRANDMEMBER failed: {}", e));
            RedliteBytesArray { items: ptr::null_mut(), len: 0 }
        }
    }
}

/// SDIFF key [key ...]
#[no_mangle]
pub extern "C" fn redlite_sdiff(
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
    match guard.sdiff(&keys_vec) {
        Ok(members) => vecs_to_bytes_array(members),
        Err(e) => {
            set_error(format!("SDIFF failed: {}", e));
            RedliteBytesArray { items: ptr::null_mut(), len: 0 }
        }
    }
}

/// SINTER key [key ...]
#[no_mangle]
pub extern "C" fn redlite_sinter(
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
    match guard.sinter(&keys_vec) {
        Ok(members) => vecs_to_bytes_array(members),
        Err(e) => {
            set_error(format!("SINTER failed: {}", e));
            RedliteBytesArray { items: ptr::null_mut(), len: 0 }
        }
    }
}

/// SUNION key [key ...]
#[no_mangle]
pub extern "C" fn redlite_sunion(
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
    match guard.sunion(&keys_vec) {
        Ok(members) => vecs_to_bytes_array(members),
        Err(e) => {
            set_error(format!("SUNION failed: {}", e));
            RedliteBytesArray { items: ptr::null_mut(), len: 0 }
        }
    }
}

/// SMOVE source destination member
/// Returns 1 if moved, 0 if member not in source
#[no_mangle]
pub extern "C" fn redlite_smove(
    db: *mut RedliteDb,
    source: *const c_char,
    destination: *const c_char,
    member: *const u8,
    member_len: size_t,
) -> c_int {
    clear_error();
    let handle = get_db!(db);

    let source = match cstr_to_str(source) {
        Ok(k) => k,
        Err(e) => {
            set_error(e);
            return -1;
        }
    };

    let destination = match cstr_to_str(destination) {
        Ok(k) => k,
        Err(e) => {
            set_error(e);
            return -1;
        }
    };

    let member = bytes_to_vec(member, member_len);

    let guard = handle.db.lock().unwrap();
    match guard.smove(source, destination, &member) {
        Ok(n) => n as c_int,
        Err(e) => {
            set_error(format!("SMOVE failed: {}", e));
            -1
        }
    }
}

/// SDIFFSTORE destination key [key ...]
/// Returns size of resulting set
#[no_mangle]
pub extern "C" fn redlite_sdiffstore(
    db: *mut RedliteDb,
    destination: *const c_char,
    keys: *const *const c_char,
    keys_len: size_t,
) -> i64 {
    clear_error();
    let handle = get_db_ret!(db, -1);

    let destination = match cstr_to_str(destination) {
        Ok(k) => k,
        Err(e) => {
            set_error(e);
            return -1;
        }
    };

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
    match guard.sdiffstore(destination, &keys_vec) {
        Ok(n) => n,
        Err(e) => {
            set_error(format!("SDIFFSTORE failed: {}", e));
            -1
        }
    }
}

/// SINTERSTORE destination key [key ...]
/// Returns size of resulting set
#[no_mangle]
pub extern "C" fn redlite_sinterstore(
    db: *mut RedliteDb,
    destination: *const c_char,
    keys: *const *const c_char,
    keys_len: size_t,
) -> i64 {
    clear_error();
    let handle = get_db_ret!(db, -1);

    let destination = match cstr_to_str(destination) {
        Ok(k) => k,
        Err(e) => {
            set_error(e);
            return -1;
        }
    };

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
    match guard.sinterstore(destination, &keys_vec) {
        Ok(n) => n,
        Err(e) => {
            set_error(format!("SINTERSTORE failed: {}", e));
            -1
        }
    }
}

/// SUNIONSTORE destination key [key ...]
/// Returns size of resulting set
#[no_mangle]
pub extern "C" fn redlite_sunionstore(
    db: *mut RedliteDb,
    destination: *const c_char,
    keys: *const *const c_char,
    keys_len: size_t,
) -> i64 {
    clear_error();
    let handle = get_db_ret!(db, -1);

    let destination = match cstr_to_str(destination) {
        Ok(k) => k,
        Err(e) => {
            set_error(e);
            return -1;
        }
    };

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
    match guard.sunionstore(destination, &keys_vec) {
        Ok(n) => n,
        Err(e) => {
            set_error(format!("SUNIONSTORE failed: {}", e));
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

/// ZRANK key member
/// Returns rank (0-based), -1 if not found, -2 on error
#[no_mangle]
pub extern "C" fn redlite_zrank(
    db: *mut RedliteDb,
    key: *const c_char,
    member: *const u8,
    member_len: size_t,
) -> i64 {
    clear_error();
    let handle = get_db_ret!(db, -2);

    let key = match cstr_to_str(key) {
        Ok(k) => k,
        Err(e) => {
            set_error(e);
            return -2;
        }
    };

    let member = bytes_to_vec(member, member_len);

    let guard = handle.db.lock().unwrap();
    match guard.zrank(key, &member) {
        Ok(Some(rank)) => rank,
        Ok(None) => -1,
        Err(e) => {
            set_error(format!("ZRANK failed: {}", e));
            -2
        }
    }
}

/// ZREVRANK key member
/// Returns reverse rank (0-based), -1 if not found, -2 on error
#[no_mangle]
pub extern "C" fn redlite_zrevrank(
    db: *mut RedliteDb,
    key: *const c_char,
    member: *const u8,
    member_len: size_t,
) -> i64 {
    clear_error();
    let handle = get_db_ret!(db, -2);

    let key = match cstr_to_str(key) {
        Ok(k) => k,
        Err(e) => {
            set_error(e);
            return -2;
        }
    };

    let member = bytes_to_vec(member, member_len);

    let guard = handle.db.lock().unwrap();
    match guard.zrevrank(key, &member) {
        Ok(Some(rank)) => rank,
        Ok(None) => -1,
        Err(e) => {
            set_error(format!("ZREVRANK failed: {}", e));
            -2
        }
    }
}

/// ZRANGEBYSCORE key min max [offset count]
/// offset: number of elements to skip (use -1 for no offset)
/// count: max number of elements to return (use -1 for no limit)
#[no_mangle]
pub extern "C" fn redlite_zrangebyscore(
    db: *mut RedliteDb,
    key: *const c_char,
    min: f64,
    max: f64,
    offset: i64,
    count: i64,
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

    let offset_opt = if offset >= 0 { Some(offset) } else { None };
    let count_opt = if count >= 0 { Some(count) } else { None };

    let guard = handle.db.lock().unwrap();
    match guard.zrangebyscore(key, min, max, offset_opt, count_opt) {
        Ok(members) => {
            let members_only: Vec<Vec<u8>> = members.into_iter().map(|zm| zm.member).collect();
            vecs_to_bytes_array(members_only)
        }
        Err(e) => {
            set_error(format!("ZRANGEBYSCORE failed: {}", e));
            RedliteBytesArray { items: ptr::null_mut(), len: 0 }
        }
    }
}

/// ZREMRANGEBYRANK key start stop
/// Returns number of elements removed
#[no_mangle]
pub extern "C" fn redlite_zremrangebyrank(
    db: *mut RedliteDb,
    key: *const c_char,
    start: i64,
    stop: i64,
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
    match guard.zremrangebyrank(key, start, stop) {
        Ok(n) => n,
        Err(e) => {
            set_error(format!("ZREMRANGEBYRANK failed: {}", e));
            -1
        }
    }
}

/// ZREMRANGEBYSCORE key min max
/// Returns number of elements removed
#[no_mangle]
pub extern "C" fn redlite_zremrangebyscore(
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
    match guard.zremrangebyscore(key, min, max) {
        Ok(n) => n,
        Err(e) => {
            set_error(format!("ZREMRANGEBYSCORE failed: {}", e));
            -1
        }
    }
}

// =============================================================================
// Bit Operations (Phase 1)
// =============================================================================

/// GETBIT key offset
/// Returns bit value (0 or 1), or -1 on error
#[no_mangle]
pub extern "C" fn redlite_getbit(
    db: *mut RedliteDb,
    key: *const c_char,
    offset: u64,
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
    match guard.getbit(key, offset) {
        Ok(bit) => bit,
        Err(e) => {
            set_error(format!("GETBIT failed: {}", e));
            -1
        }
    }
}

/// SETBIT key offset value
/// Returns previous bit value (0 or 1), or -1 on error
#[no_mangle]
pub extern "C" fn redlite_setbit(
    db: *mut RedliteDb,
    key: *const c_char,
    offset: u64,
    value: c_int,
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
    match guard.setbit(key, offset, value != 0) {
        Ok(prev) => prev,
        Err(e) => {
            set_error(format!("SETBIT failed: {}", e));
            -1
        }
    }
}

/// BITCOUNT key [start end]
/// Returns number of set bits, or -1 on error
#[no_mangle]
pub extern "C" fn redlite_bitcount(
    db: *mut RedliteDb,
    key: *const c_char,
    start: i64,
    end: i64,
    use_range: c_int,
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

    let start_opt = if use_range != 0 { Some(start) } else { None };
    let end_opt = if use_range != 0 { Some(end) } else { None };

    let guard = handle.db.lock().unwrap();
    match guard.bitcount(key, start_opt, end_opt) {
        Ok(count) => count,
        Err(e) => {
            set_error(format!("BITCOUNT failed: {}", e));
            -1
        }
    }
}

/// BITOP operation destkey key [key ...]
/// Returns length of result string, or -1 on error
/// operation: "AND", "OR", "XOR", "NOT"
#[no_mangle]
pub extern "C" fn redlite_bitop(
    db: *mut RedliteDb,
    operation: *const c_char,
    destkey: *const c_char,
    keys: *const *const c_char,
    keys_len: size_t,
) -> i64 {
    clear_error();
    let handle = get_db_ret!(db, -1);

    let op = match cstr_to_str(operation) {
        Ok(s) => s,
        Err(e) => {
            set_error(e);
            return -1;
        }
    };

    let dest = match cstr_to_str(destkey) {
        Ok(s) => s,
        Err(e) => {
            set_error(e);
            return -1;
        }
    };

    if keys.is_null() || keys_len == 0 {
        set_error("BITOP requires at least one source key".to_string());
        return -1;
    }

    let keys_slice = unsafe { slice::from_raw_parts(keys, keys_len) };
    let mut key_strs = Vec::new();
    for &key_ptr in keys_slice {
        match cstr_to_str(key_ptr) {
            Ok(s) => key_strs.push(s),
            Err(e) => {
                set_error(e);
                return -1;
            }
        }
    }

    let guard = handle.db.lock().unwrap();
    match guard.bitop(op, dest, &key_strs) {
        Ok(len) => len,
        Err(e) => {
            set_error(format!("BITOP failed: {}", e));
            -1
        }
    }
}

// =============================================================================
// Scan Operations (Phase 1)
// =============================================================================

/// SCAN cursor [MATCH pattern] [COUNT count]
/// Returns scan result with next cursor and keys
#[no_mangle]
pub extern "C" fn redlite_scan(
    db: *mut RedliteDb,
    cursor: *const c_char,
    pattern: *const c_char,
    count: size_t,
) -> RedliteScanResult {
    clear_error();
    let handle = get_db_ret!(db, RedliteScanResult {
        cursor: ptr::null_mut(),
        keys: RedliteStringArray { strings: ptr::null_mut(), len: 0 },
    });

    let cursor_str = match cstr_to_str(cursor) {
        Ok(s) => s,
        Err(e) => {
            set_error(e);
            return RedliteScanResult {
                cursor: ptr::null_mut(),
                keys: RedliteStringArray { strings: ptr::null_mut(), len: 0 },
            };
        }
    };

    let pattern_opt = if pattern.is_null() {
        None
    } else {
        match cstr_to_str(pattern) {
            Ok(s) => Some(s),
            Err(e) => {
                set_error(e);
                return RedliteScanResult {
                    cursor: ptr::null_mut(),
                    keys: RedliteStringArray { strings: ptr::null_mut(), len: 0 },
                };
            }
        }
    };

    let guard = handle.db.lock().unwrap();
    match guard.scan(cursor_str, pattern_opt, count) {
        Ok((next_cursor, keys)) => {
            let cursor_cstr = CString::new(next_cursor).unwrap().into_raw();
            let keys_array = strings_to_array(keys);
            RedliteScanResult {
                cursor: cursor_cstr,
                keys: keys_array,
            }
        }
        Err(e) => {
            set_error(format!("SCAN failed: {}", e));
            RedliteScanResult {
                cursor: ptr::null_mut(),
                keys: RedliteStringArray { strings: ptr::null_mut(), len: 0 },
            }
        }
    }
}

/// HSCAN key cursor [MATCH pattern] [COUNT count]
/// Returns scan result with next cursor and field-value pairs
#[no_mangle]
pub extern "C" fn redlite_hscan(
    db: *mut RedliteDb,
    key: *const c_char,
    cursor: *const c_char,
    pattern: *const c_char,
    count: size_t,
) -> RedliteHScanResult {
    clear_error();
    let handle = get_db_ret!(db, RedliteHScanResult {
        cursor: ptr::null_mut(),
        pairs: RedliteBytesArray { items: ptr::null_mut(), len: 0 },
    });

    let key_str = match cstr_to_str(key) {
        Ok(s) => s,
        Err(e) => {
            set_error(e);
            return RedliteHScanResult {
                cursor: ptr::null_mut(),
                pairs: RedliteBytesArray { items: ptr::null_mut(), len: 0 },
            };
        }
    };

    let cursor_str = match cstr_to_str(cursor) {
        Ok(s) => s,
        Err(e) => {
            set_error(e);
            return RedliteHScanResult {
                cursor: ptr::null_mut(),
                pairs: RedliteBytesArray { items: ptr::null_mut(), len: 0 },
            };
        }
    };

    let pattern_opt = if pattern.is_null() {
        None
    } else {
        match cstr_to_str(pattern) {
            Ok(s) => Some(s),
            Err(e) => {
                set_error(e);
                return RedliteHScanResult {
                    cursor: ptr::null_mut(),
                    pairs: RedliteBytesArray { items: ptr::null_mut(), len: 0 },
                };
            }
        }
    };

    let guard = handle.db.lock().unwrap();
    match guard.hscan(key_str, cursor_str, pattern_opt, count) {
        Ok((next_cursor, pairs)) => {
            let cursor_cstr = CString::new(next_cursor).unwrap().into_raw();
            // Flatten pairs into [field1, value1, field2, value2, ...]
            let mut flat_pairs = Vec::new();
            for (field, value) in pairs {
                flat_pairs.push(field.into_bytes());
                flat_pairs.push(value);
            }
            let pairs_array = vecs_to_bytes_array(flat_pairs);
            RedliteHScanResult {
                cursor: cursor_cstr,
                pairs: pairs_array,
            }
        }
        Err(e) => {
            set_error(format!("HSCAN failed: {}", e));
            RedliteHScanResult {
                cursor: ptr::null_mut(),
                pairs: RedliteBytesArray { items: ptr::null_mut(), len: 0 },
            }
        }
    }
}

/// SSCAN key cursor [MATCH pattern] [COUNT count]
/// Returns scan result with next cursor and members
#[no_mangle]
pub extern "C" fn redlite_sscan(
    db: *mut RedliteDb,
    key: *const c_char,
    cursor: *const c_char,
    pattern: *const c_char,
    count: size_t,
) -> RedliteSScanResult {
    clear_error();
    let handle = get_db_ret!(db, RedliteSScanResult {
        cursor: ptr::null_mut(),
        members: RedliteBytesArray { items: ptr::null_mut(), len: 0 },
    });

    let key_str = match cstr_to_str(key) {
        Ok(s) => s,
        Err(e) => {
            set_error(e);
            return RedliteSScanResult {
                cursor: ptr::null_mut(),
                members: RedliteBytesArray { items: ptr::null_mut(), len: 0 },
            };
        }
    };

    let cursor_str = match cstr_to_str(cursor) {
        Ok(s) => s,
        Err(e) => {
            set_error(e);
            return RedliteSScanResult {
                cursor: ptr::null_mut(),
                members: RedliteBytesArray { items: ptr::null_mut(), len: 0 },
            };
        }
    };

    let pattern_opt = if pattern.is_null() {
        None
    } else {
        match cstr_to_str(pattern) {
            Ok(s) => Some(s),
            Err(e) => {
                set_error(e);
                return RedliteSScanResult {
                    cursor: ptr::null_mut(),
                    members: RedliteBytesArray { items: ptr::null_mut(), len: 0 },
                };
            }
        }
    };

    let guard = handle.db.lock().unwrap();
    match guard.sscan(key_str, cursor_str, pattern_opt, count) {
        Ok((next_cursor, members)) => {
            let cursor_cstr = CString::new(next_cursor).unwrap().into_raw();
            let members_array = vecs_to_bytes_array(members);
            RedliteSScanResult {
                cursor: cursor_cstr,
                members: members_array,
            }
        }
        Err(e) => {
            set_error(format!("SSCAN failed: {}", e));
            RedliteSScanResult {
                cursor: ptr::null_mut(),
                members: RedliteBytesArray { items: ptr::null_mut(), len: 0 },
            }
        }
    }
}

/// ZSCAN key cursor [MATCH pattern] [COUNT count]
/// Returns scan result with next cursor and member-score pairs
#[no_mangle]
pub extern "C" fn redlite_zscan(
    db: *mut RedliteDb,
    key: *const c_char,
    cursor: *const c_char,
    pattern: *const c_char,
    count: size_t,
) -> RedliteZScanResult {
    clear_error();
    let handle = get_db_ret!(db, RedliteZScanResult {
        cursor: ptr::null_mut(),
        members: ptr::null_mut(),
        len: 0,
    });

    let key_str = match cstr_to_str(key) {
        Ok(s) => s,
        Err(e) => {
            set_error(e);
            return RedliteZScanResult {
                cursor: ptr::null_mut(),
                members: ptr::null_mut(),
                len: 0,
            };
        }
    };

    let cursor_str = match cstr_to_str(cursor) {
        Ok(s) => s,
        Err(e) => {
            set_error(e);
            return RedliteZScanResult {
                cursor: ptr::null_mut(),
                members: ptr::null_mut(),
                len: 0,
            };
        }
    };

    let pattern_opt = if pattern.is_null() {
        None
    } else {
        match cstr_to_str(pattern) {
            Ok(s) => Some(s),
            Err(e) => {
                set_error(e);
                return RedliteZScanResult {
                    cursor: ptr::null_mut(),
                    members: ptr::null_mut(),
                    len: 0,
                };
            }
        }
    };

    let guard = handle.db.lock().unwrap();
    match guard.zscan(key_str, cursor_str, pattern_opt, count) {
        Ok((next_cursor, pairs)) => {
            let cursor_cstr = CString::new(next_cursor).unwrap().into_raw();
            let len = pairs.len();
            if len == 0 {
                return RedliteZScanResult {
                    cursor: cursor_cstr,
                    members: ptr::null_mut(),
                    len: 0,
                };
            }

            let mut members: Vec<RedliteZScanMember> = pairs
                .into_iter()
                .map(|(member, score)| RedliteZScanMember {
                    member: vec_to_bytes(member),
                    score,
                })
                .collect();

            let ptr = members.as_mut_ptr();
            std::mem::forget(members);

            RedliteZScanResult {
                cursor: cursor_cstr,
                members: ptr,
                len,
            }
        }
        Err(e) => {
            set_error(format!("ZSCAN failed: {}", e));
            RedliteZScanResult {
                cursor: ptr::null_mut(),
                members: ptr::null_mut(),
                len: 0,
            }
        }
    }
}

/// ZINTERSTORE destination numkeys key [key ...] [WEIGHTS weight [weight ...]] [AGGREGATE SUM|MIN|MAX]
/// Returns number of elements in result, or -1 on error
/// weights: optional array (NULL for default 1.0), must match keys_len if provided
/// aggregate: optional string "SUM", "MIN", or "MAX" (NULL for default SUM)
#[no_mangle]
pub extern "C" fn redlite_zinterstore(
    db: *mut RedliteDb,
    destination: *const c_char,
    keys: *const *const c_char,
    keys_len: size_t,
    weights: *const f64,
    weights_len: size_t,
    aggregate: *const c_char,
) -> i64 {
    clear_error();
    let handle = get_db_ret!(db, -1);

    let dest_str = match cstr_to_str(destination) {
        Ok(s) => s,
        Err(e) => {
            set_error(e);
            return -1;
        }
    };

    if keys.is_null() || keys_len == 0 {
        set_error("ZINTERSTORE requires at least one key".to_string());
        return -1;
    }

    let keys_slice = unsafe { slice::from_raw_parts(keys, keys_len) };
    let mut key_strs = Vec::new();
    for &key_ptr in keys_slice {
        match cstr_to_str(key_ptr) {
            Ok(s) => key_strs.push(s),
            Err(e) => {
                set_error(e);
                return -1;
            }
        }
    }
    let key_refs: Vec<&str> = key_strs.iter().map(|s| s.as_ref()).collect();

    let weights_opt = if weights.is_null() || weights_len == 0 {
        None
    } else {
        let w_slice = unsafe { slice::from_raw_parts(weights, weights_len) };
        Some(w_slice)
    };

    let agg_opt = if aggregate.is_null() {
        None
    } else {
        match cstr_to_str(aggregate) {
            Ok(s) => Some(s),
            Err(e) => {
                set_error(e);
                return -1;
            }
        }
    };

    let guard = handle.db.lock().unwrap();
    match guard.zinterstore(dest_str, &key_refs, weights_opt, agg_opt) {
        Ok(n) => n,
        Err(e) => {
            set_error(format!("ZINTERSTORE failed: {}", e));
            -1
        }
    }
}

/// ZUNIONSTORE destination numkeys key [key ...] [WEIGHTS weight [weight ...]] [AGGREGATE SUM|MIN|MAX]
/// Returns number of elements in result, or -1 on error
/// weights: optional array (NULL for default 1.0), must match keys_len if provided
/// aggregate: optional string "SUM", "MIN", or "MAX" (NULL for default SUM)
#[no_mangle]
pub extern "C" fn redlite_zunionstore(
    db: *mut RedliteDb,
    destination: *const c_char,
    keys: *const *const c_char,
    keys_len: size_t,
    weights: *const f64,
    weights_len: size_t,
    aggregate: *const c_char,
) -> i64 {
    clear_error();
    let handle = get_db_ret!(db, -1);

    let dest_str = match cstr_to_str(destination) {
        Ok(s) => s,
        Err(e) => {
            set_error(e);
            return -1;
        }
    };

    if keys.is_null() || keys_len == 0 {
        set_error("ZUNIONSTORE requires at least one key".to_string());
        return -1;
    }

    let keys_slice = unsafe { slice::from_raw_parts(keys, keys_len) };
    let mut key_strs = Vec::new();
    for &key_ptr in keys_slice {
        match cstr_to_str(key_ptr) {
            Ok(s) => key_strs.push(s),
            Err(e) => {
                set_error(e);
                return -1;
            }
        }
    }
    let key_refs: Vec<&str> = key_strs.iter().map(|s| s.as_ref()).collect();

    let weights_opt = if weights.is_null() || weights_len == 0 {
        None
    } else {
        let w_slice = unsafe { slice::from_raw_parts(weights, weights_len) };
        Some(w_slice)
    };

    let agg_opt = if aggregate.is_null() {
        None
    } else {
        match cstr_to_str(aggregate) {
            Ok(s) => Some(s),
            Err(e) => {
                set_error(e);
                return -1;
            }
        }
    };

    let guard = handle.db.lock().unwrap();
    match guard.zunionstore(dest_str, &key_refs, weights_opt, agg_opt) {
        Ok(n) => n,
        Err(e) => {
            set_error(format!("ZUNIONSTORE failed: {}", e));
            -1
        }
    }
}

// =============================================================================
// Stream Operations (Phase 1)
// =============================================================================

/// XADD key [NOMKSTREAM] [MAXLEN|MINID [=|~] threshold] *|ID field value [field value ...]
/// Returns generated stream ID, or NULL on error
/// If id_ms and id_seq are both 0, auto-generates ID (*)
#[no_mangle]
pub extern "C" fn redlite_xadd(
    db: *mut RedliteDb,
    key: *const c_char,
    id_ms: i64,
    id_seq: i64,
    fields: *const RedliteStreamField,
    fields_len: size_t,
    nomkstream: c_int,
    maxlen: i64,
    use_maxlen: c_int,
) -> RedliteStreamId {
    clear_error();
    let handle = get_db_ret!(db, RedliteStreamId { ms: -1, seq: -1 });

    let key_str = match cstr_to_str(key) {
        Ok(s) => s,
        Err(e) => {
            set_error(e);
            return RedliteStreamId { ms: -1, seq: -1 };
        }
    };

    if fields.is_null() || fields_len == 0 {
        set_error("XADD requires at least one field-value pair".to_string());
        return RedliteStreamId { ms: -1, seq: -1 };
    }

    let fields_slice = unsafe { slice::from_raw_parts(fields, fields_len) };
    let mut field_pairs = Vec::new();
    for field in fields_slice {
        let key_bytes = bytes_to_vec(field.key, field.key_len);
        let value_bytes = bytes_to_vec(field.value, field.value_len);
        field_pairs.push((key_bytes, value_bytes));
    }

    let field_refs: Vec<(&[u8], &[u8])> = field_pairs
        .iter()
        .map(|(k, v)| (k.as_slice(), v.as_slice()))
        .collect();

    let stream_id = if id_ms == 0 && id_seq == 0 {
        None
    } else {
        Some(redlite::StreamId { ms: id_ms, seq: id_seq })
    };

    let maxlen_opt = if use_maxlen != 0 { Some(maxlen) } else { None };

    let guard = handle.db.lock().unwrap();
    match guard.xadd(
        key_str,
        stream_id,
        &field_refs,
        nomkstream != 0,
        maxlen_opt,
        None,
        false,
    ) {
        Ok(Some(id)) => RedliteStreamId { ms: id.ms, seq: id.seq },
        Ok(None) => {
            set_error("XADD returned no ID (stream not created)".to_string());
            RedliteStreamId { ms: -1, seq: -1 }
        }
        Err(e) => {
            set_error(format!("XADD failed: {}", e));
            RedliteStreamId { ms: -1, seq: -1 }
        }
    }
}

/// XLEN key
/// Returns stream length, or -1 on error
#[no_mangle]
pub extern "C" fn redlite_xlen(db: *mut RedliteDb, key: *const c_char) -> i64 {
    clear_error();
    let handle = get_db_ret!(db, -1);

    let key_str = match cstr_to_str(key) {
        Ok(s) => s,
        Err(e) => {
            set_error(e);
            return -1;
        }
    };

    let guard = handle.db.lock().unwrap();
    match guard.xlen(key_str) {
        Ok(len) => len,
        Err(e) => {
            set_error(format!("XLEN failed: {}", e));
            -1
        }
    }
}

/// XRANGE key start end [COUNT count]
/// Returns stream entries in range, or empty array on error
#[no_mangle]
pub extern "C" fn redlite_xrange(
    db: *mut RedliteDb,
    key: *const c_char,
    start_ms: i64,
    start_seq: i64,
    end_ms: i64,
    end_seq: i64,
    count: i64,
    use_count: c_int,
) -> RedliteStreamEntryArray {
    clear_error();
    let handle = get_db_ret!(db, RedliteStreamEntryArray {
        entries: ptr::null_mut(),
        len: 0,
    });

    let key_str = match cstr_to_str(key) {
        Ok(s) => s,
        Err(e) => {
            set_error(e);
            return RedliteStreamEntryArray {
                entries: ptr::null_mut(),
                len: 0,
            };
        }
    };

    let start_id = redlite::StreamId { ms: start_ms, seq: start_seq };
    let end_id = redlite::StreamId { ms: end_ms, seq: end_seq };
    let count_opt = if use_count != 0 { Some(count) } else { None };

    let guard = handle.db.lock().unwrap();
    match guard.xrange(key_str, start_id, end_id, count_opt) {
        Ok(entries) => {
            let len = entries.len();
            if len == 0 {
                return RedliteStreamEntryArray {
                    entries: ptr::null_mut(),
                    len: 0,
                };
            }

            let mut c_entries: Vec<RedliteStreamEntry> = entries
                .into_iter()
                .map(|entry| {
                    let fields_len = entry.fields.len();
                    let mut fields: Vec<RedliteStreamField> = entry
                        .fields
                        .into_iter()
                        .map(|(k, v)| {
                            let k_bytes = vec_to_bytes(k);
                            let v_bytes = vec_to_bytes(v);
                            RedliteStreamField {
                                key: k_bytes.data,
                                key_len: k_bytes.len,
                                value: v_bytes.data,
                                value_len: v_bytes.len,
                            }
                        })
                        .collect();

                    let fields_ptr = fields.as_mut_ptr();
                    std::mem::forget(fields);

                    RedliteStreamEntry {
                        id: RedliteStreamId {
                            ms: entry.id.ms,
                            seq: entry.id.seq,
                        },
                        fields: fields_ptr,
                        fields_len,
                    }
                })
                .collect();

            let ptr = c_entries.as_mut_ptr();
            std::mem::forget(c_entries);

            RedliteStreamEntryArray { entries: ptr, len }
        }
        Err(e) => {
            set_error(format!("XRANGE failed: {}", e));
            RedliteStreamEntryArray {
                entries: ptr::null_mut(),
                len: 0,
            }
        }
    }
}

/// XREVRANGE key end start [COUNT count]
/// Returns stream entries in reverse range
#[no_mangle]
pub extern "C" fn redlite_xrevrange(
    db: *mut RedliteDb,
    key: *const c_char,
    end_ms: i64,
    end_seq: i64,
    start_ms: i64,
    start_seq: i64,
    count: i64,
    use_count: c_int,
) -> RedliteStreamEntryArray {
    clear_error();
    let handle = get_db_ret!(db, RedliteStreamEntryArray {
        entries: ptr::null_mut(),
        len: 0,
    });

    let key_str = match cstr_to_str(key) {
        Ok(s) => s,
        Err(e) => {
            set_error(e);
            return RedliteStreamEntryArray {
                entries: ptr::null_mut(),
                len: 0,
            };
        }
    };

    let end_id = redlite::StreamId { ms: end_ms, seq: end_seq };
    let start_id = redlite::StreamId { ms: start_ms, seq: start_seq };
    let count_opt = if use_count != 0 { Some(count) } else { None };

    let guard = handle.db.lock().unwrap();
    match guard.xrevrange(key_str, end_id, start_id, count_opt) {
        Ok(entries) => {
            let len = entries.len();
            if len == 0 {
                return RedliteStreamEntryArray {
                    entries: ptr::null_mut(),
                    len: 0,
                };
            }

            let mut c_entries: Vec<RedliteStreamEntry> = entries
                .into_iter()
                .map(|entry| {
                    let fields_len = entry.fields.len();
                    let mut fields: Vec<RedliteStreamField> = entry
                        .fields
                        .into_iter()
                        .map(|(k, v)| {
                            let k_bytes = vec_to_bytes(k);
                            let v_bytes = vec_to_bytes(v);
                            RedliteStreamField {
                                key: k_bytes.data,
                                key_len: k_bytes.len,
                                value: v_bytes.data,
                                value_len: v_bytes.len,
                            }
                        })
                        .collect();

                    let fields_ptr = fields.as_mut_ptr();
                    std::mem::forget(fields);

                    RedliteStreamEntry {
                        id: RedliteStreamId {
                            ms: entry.id.ms,
                            seq: entry.id.seq,
                        },
                        fields: fields_ptr,
                        fields_len,
                    }
                })
                .collect();

            let ptr = c_entries.as_mut_ptr();
            std::mem::forget(c_entries);

            RedliteStreamEntryArray { entries: ptr, len }
        }
        Err(e) => {
            set_error(format!("XREVRANGE failed: {}", e));
            RedliteStreamEntryArray {
                entries: ptr::null_mut(),
                len: 0,
            }
        }
    }
}

/// XREAD [COUNT count] STREAMS key [key ...] id [id ...]
/// Note: This is a simplified version. Full XREAD with BLOCK is async-only.
/// For single-key reads with count. Use XRANGE for more control.
#[no_mangle]
pub extern "C" fn redlite_xread(
    db: *mut RedliteDb,
    key: *const c_char,
    id_ms: i64,
    id_seq: i64,
    count: i64,
    use_count: c_int,
) -> RedliteStreamEntryArray {
    clear_error();
    let handle = get_db_ret!(db, RedliteStreamEntryArray {
        entries: ptr::null_mut(),
        len: 0,
    });

    let key_str = match cstr_to_str(key) {
        Ok(s) => s,
        Err(e) => {
            set_error(e);
            return RedliteStreamEntryArray {
                entries: ptr::null_mut(),
                len: 0,
            };
        }
    };

    let start_id = redlite::StreamId { ms: id_ms, seq: id_seq };
    let count_opt = if use_count != 0 { Some(count) } else { None };

    let guard = handle.db.lock().unwrap();
    match guard.xread(&[key_str], &[start_id], count_opt) {
        Ok(results) => {
            // Results is Vec<(String, Vec<StreamEntry>)>
            // For simplicity, flatten all entries from all keys
            let mut all_entries = Vec::new();
            for (_key, entries) in results {
                all_entries.extend(entries);
            }

            let len = all_entries.len();
            if len == 0 {
                return RedliteStreamEntryArray {
                    entries: ptr::null_mut(),
                    len: 0,
                };
            }

            let mut c_entries: Vec<RedliteStreamEntry> = all_entries
                .into_iter()
                .map(|entry| {
                    let fields_len = entry.fields.len();
                    let mut fields: Vec<RedliteStreamField> = entry
                        .fields
                        .into_iter()
                        .map(|(k, v)| {
                            let k_bytes = vec_to_bytes(k);
                            let v_bytes = vec_to_bytes(v);
                            RedliteStreamField {
                                key: k_bytes.data,
                                key_len: k_bytes.len,
                                value: v_bytes.data,
                                value_len: v_bytes.len,
                            }
                        })
                        .collect();

                    let fields_ptr = fields.as_mut_ptr();
                    std::mem::forget(fields);

                    RedliteStreamEntry {
                        id: RedliteStreamId {
                            ms: entry.id.ms,
                            seq: entry.id.seq,
                        },
                        fields: fields_ptr,
                        fields_len,
                    }
                })
                .collect();

            let ptr = c_entries.as_mut_ptr();
            std::mem::forget(c_entries);

            RedliteStreamEntryArray { entries: ptr, len }
        }
        Err(e) => {
            set_error(format!("XREAD failed: {}", e));
            RedliteStreamEntryArray {
                entries: ptr::null_mut(),
                len: 0,
            }
        }
    }
}

/// XTRIM key MAXLEN [~] count
/// Returns number of entries deleted, or -1 on error
#[no_mangle]
pub extern "C" fn redlite_xtrim(
    db: *mut RedliteDb,
    key: *const c_char,
    maxlen: i64,
) -> i64 {
    clear_error();
    let handle = get_db_ret!(db, -1);

    let key_str = match cstr_to_str(key) {
        Ok(s) => s,
        Err(e) => {
            set_error(e);
            return -1;
        }
    };

    let guard = handle.db.lock().unwrap();
    match guard.xtrim(key_str, Some(maxlen), None, false) {
        Ok(count) => count,
        Err(e) => {
            set_error(format!("XTRIM failed: {}", e));
            -1
        }
    }
}

/// XDEL key id [id ...]
/// Returns number of entries deleted, or -1 on error
#[no_mangle]
pub extern "C" fn redlite_xdel(
    db: *mut RedliteDb,
    key: *const c_char,
    ids: *const RedliteStreamId,
    ids_len: size_t,
) -> i64 {
    clear_error();
    let handle = get_db_ret!(db, -1);

    let key_str = match cstr_to_str(key) {
        Ok(s) => s,
        Err(e) => {
            set_error(e);
            return -1;
        }
    };

    if ids.is_null() || ids_len == 0 {
        return 0;
    }

    let ids_slice = unsafe { slice::from_raw_parts(ids, ids_len) };
    let stream_ids: Vec<redlite::StreamId> = ids_slice
        .iter()
        .map(|id| redlite::StreamId { ms: id.ms, seq: id.seq })
        .collect();

    let guard = handle.db.lock().unwrap();
    match guard.xdel(key_str, &stream_ids) {
        Ok(count) => count,
        Err(e) => {
            set_error(format!("XDEL failed: {}", e));
            -1
        }
    }
}

// =============================================================================
// Stream Consumer Groups (Phase 1)
// =============================================================================

/// XGROUP CREATE key group id [MKSTREAM]
/// Returns 1 on success, 0 on error
#[no_mangle]
pub extern "C" fn redlite_xgroup_create(
    db: *mut RedliteDb,
    key: *const c_char,
    group: *const c_char,
    id_ms: i64,
    id_seq: i64,
    mkstream: c_int,
) -> c_int {
    clear_error();
    let handle = get_db_ret!(db, 0);

    let key_str = match cstr_to_str(key) {
        Ok(s) => s,
        Err(e) => {
            set_error(e);
            return 0;
        }
    };

    let group_str = match cstr_to_str(group) {
        Ok(s) => s,
        Err(e) => {
            set_error(e);
            return 0;
        }
    };

    let stream_id = redlite::StreamId { ms: id_ms, seq: id_seq };

    let guard = handle.db.lock().unwrap();
    match guard.xgroup_create(key_str, group_str, stream_id, mkstream != 0) {
        Ok(true) => 1,
        Ok(false) => 0,
        Err(e) => {
            set_error(format!("XGROUP CREATE failed: {}", e));
            0
        }
    }
}

/// XGROUP DESTROY key group
/// Returns 1 if group was destroyed, 0 otherwise
#[no_mangle]
pub extern "C" fn redlite_xgroup_destroy(
    db: *mut RedliteDb,
    key: *const c_char,
    group: *const c_char,
) -> c_int {
    clear_error();
    let handle = get_db_ret!(db, 0);

    let key_str = match cstr_to_str(key) {
        Ok(s) => s,
        Err(e) => {
            set_error(e);
            return 0;
        }
    };

    let group_str = match cstr_to_str(group) {
        Ok(s) => s,
        Err(e) => {
            set_error(e);
            return 0;
        }
    };

    let guard = handle.db.lock().unwrap();
    match guard.xgroup_destroy(key_str, group_str) {
        Ok(true) => 1,
        Ok(false) => 0,
        Err(e) => {
            set_error(format!("XGROUP DESTROY failed: {}", e));
            0
        }
    }
}

/// XREADGROUP GROUP group consumer [COUNT count] STREAMS key id
/// Note: Simplified single-key version. Use ">" for id to get new messages.
/// Returns stream entries
#[no_mangle]
pub extern "C" fn redlite_xreadgroup(
    db: *mut RedliteDb,
    group: *const c_char,
    consumer: *const c_char,
    key: *const c_char,
    id: *const c_char,
    count: i64,
    use_count: c_int,
    noack: c_int,
) -> RedliteStreamEntryArray {
    clear_error();
    let handle = get_db_ret!(db, RedliteStreamEntryArray {
        entries: ptr::null_mut(),
        len: 0,
    });

    let group_str = match cstr_to_str(group) {
        Ok(s) => s,
        Err(e) => {
            set_error(e);
            return RedliteStreamEntryArray {
                entries: ptr::null_mut(),
                len: 0,
            };
        }
    };

    let consumer_str = match cstr_to_str(consumer) {
        Ok(s) => s,
        Err(e) => {
            set_error(e);
            return RedliteStreamEntryArray {
                entries: ptr::null_mut(),
                len: 0,
            };
        }
    };

    let key_str = match cstr_to_str(key) {
        Ok(s) => s,
        Err(e) => {
            set_error(e);
            return RedliteStreamEntryArray {
                entries: ptr::null_mut(),
                len: 0,
            };
        }
    };

    let id_str = match cstr_to_str(id) {
        Ok(s) => s,
        Err(e) => {
            set_error(e);
            return RedliteStreamEntryArray {
                entries: ptr::null_mut(),
                len: 0,
            };
        }
    };

    let count_opt = if use_count != 0 { Some(count) } else { None };

    let guard = handle.db.lock().unwrap();
    match guard.xreadgroup(
        group_str,
        consumer_str,
        &[key_str],
        &[id_str],
        count_opt,
        noack != 0,
    ) {
        Ok(results) => {
            let mut all_entries = Vec::new();
            for (_key, entries) in results {
                all_entries.extend(entries);
            }

            let len = all_entries.len();
            if len == 0 {
                return RedliteStreamEntryArray {
                    entries: ptr::null_mut(),
                    len: 0,
                };
            }

            let mut c_entries: Vec<RedliteStreamEntry> = all_entries
                .into_iter()
                .map(|entry| {
                    let fields_len = entry.fields.len();
                    let mut fields: Vec<RedliteStreamField> = entry
                        .fields
                        .into_iter()
                        .map(|(k, v)| {
                            let k_bytes = vec_to_bytes(k);
                            let v_bytes = vec_to_bytes(v);
                            RedliteStreamField {
                                key: k_bytes.data,
                                key_len: k_bytes.len,
                                value: v_bytes.data,
                                value_len: v_bytes.len,
                            }
                        })
                        .collect();

                    let fields_ptr = fields.as_mut_ptr();
                    std::mem::forget(fields);

                    RedliteStreamEntry {
                        id: RedliteStreamId {
                            ms: entry.id.ms,
                            seq: entry.id.seq,
                        },
                        fields: fields_ptr,
                        fields_len,
                    }
                })
                .collect();

            let ptr = c_entries.as_mut_ptr();
            std::mem::forget(c_entries);

            RedliteStreamEntryArray { entries: ptr, len }
        }
        Err(e) => {
            set_error(format!("XREADGROUP failed: {}", e));
            RedliteStreamEntryArray {
                entries: ptr::null_mut(),
                len: 0,
            }
        }
    }
}

/// XACK key group id [id ...]
/// Returns number of messages acknowledged, or -1 on error
#[no_mangle]
pub extern "C" fn redlite_xack(
    db: *mut RedliteDb,
    key: *const c_char,
    group: *const c_char,
    ids: *const RedliteStreamId,
    ids_len: size_t,
) -> i64 {
    clear_error();
    let handle = get_db_ret!(db, -1);

    let key_str = match cstr_to_str(key) {
        Ok(s) => s,
        Err(e) => {
            set_error(e);
            return -1;
        }
    };

    let group_str = match cstr_to_str(group) {
        Ok(s) => s,
        Err(e) => {
            set_error(e);
            return -1;
        }
    };

    if ids.is_null() || ids_len == 0 {
        return 0;
    }

    let ids_slice = unsafe { slice::from_raw_parts(ids, ids_len) };
    let stream_ids: Vec<redlite::StreamId> = ids_slice
        .iter()
        .map(|id| redlite::StreamId { ms: id.ms, seq: id.seq })
        .collect();

    let guard = handle.db.lock().unwrap();
    match guard.xack(key_str, group_str, &stream_ids) {
        Ok(count) => count,
        Err(e) => {
            set_error(format!("XACK failed: {}", e));
            -1
        }
    }
}

/// XGROUP SETID key group id
/// Returns 1 on success, 0 if group doesn't exist, -1 on error
#[no_mangle]
pub extern "C" fn redlite_xgroup_setid(
    db: *mut RedliteDb,
    key: *const c_char,
    group: *const c_char,
    id_ms: i64,
    id_seq: i64,
) -> i64 {
    clear_error();
    let handle = get_db_ret!(db, -1);

    let key_str = match cstr_to_str(key) {
        Ok(s) => s,
        Err(e) => {
            set_error(e);
            return -1;
        }
    };

    let group_str = match cstr_to_str(group) {
        Ok(s) => s,
        Err(e) => {
            set_error(e);
            return -1;
        }
    };

    let stream_id = redlite::StreamId { ms: id_ms, seq: id_seq };

    let guard = handle.db.lock().unwrap();
    match guard.xgroup_setid(key_str, group_str, stream_id) {
        Ok(true) => 1,
        Ok(false) => 0,
        Err(e) => {
            set_error(format!("XGROUP SETID failed: {}", e));
            -1
        }
    }
}

/// XGROUP CREATECONSUMER key group consumer
/// Returns 1 if consumer was created, 0 if it already existed, -1 on error
#[no_mangle]
pub extern "C" fn redlite_xgroup_createconsumer(
    db: *mut RedliteDb,
    key: *const c_char,
    group: *const c_char,
    consumer: *const c_char,
) -> i64 {
    clear_error();
    let handle = get_db_ret!(db, -1);

    let key_str = match cstr_to_str(key) {
        Ok(s) => s,
        Err(e) => {
            set_error(e);
            return -1;
        }
    };

    let group_str = match cstr_to_str(group) {
        Ok(s) => s,
        Err(e) => {
            set_error(e);
            return -1;
        }
    };

    let consumer_str = match cstr_to_str(consumer) {
        Ok(s) => s,
        Err(e) => {
            set_error(e);
            return -1;
        }
    };

    let guard = handle.db.lock().unwrap();
    match guard.xgroup_createconsumer(key_str, group_str, consumer_str) {
        Ok(true) => 1,
        Ok(false) => 0,
        Err(e) => {
            set_error(format!("XGROUP CREATECONSUMER failed: {}", e));
            -1
        }
    }
}

/// XGROUP DELCONSUMER key group consumer
/// Returns number of pending messages consumer had, or -1 on error
#[no_mangle]
pub extern "C" fn redlite_xgroup_delconsumer(
    db: *mut RedliteDb,
    key: *const c_char,
    group: *const c_char,
    consumer: *const c_char,
) -> i64 {
    clear_error();
    let handle = get_db_ret!(db, -1);

    let key_str = match cstr_to_str(key) {
        Ok(s) => s,
        Err(e) => {
            set_error(e);
            return -1;
        }
    };

    let group_str = match cstr_to_str(group) {
        Ok(s) => s,
        Err(e) => {
            set_error(e);
            return -1;
        }
    };

    let consumer_str = match cstr_to_str(consumer) {
        Ok(s) => s,
        Err(e) => {
            set_error(e);
            return -1;
        }
    };

    let guard = handle.db.lock().unwrap();
    match guard.xgroup_delconsumer(key_str, group_str, consumer_str) {
        Ok(count) => count,
        Err(e) => {
            set_error(format!("XGROUP DELCONSUMER failed: {}", e));
            -1
        }
    }
}

/// XCLAIM key group consumer min-idle-time ID [ID ...] [IDLE ms] [TIME ms-unix-time] [RETRYCOUNT count] [FORCE] [JUSTID]
/// Returns claimed stream entries
#[no_mangle]
pub extern "C" fn redlite_xclaim(
    db: *mut RedliteDb,
    key: *const c_char,
    group: *const c_char,
    consumer: *const c_char,
    min_idle_time: i64,
    ids: *const RedliteStreamId,
    ids_len: size_t,
    idle_ms: i64,
    time_ms: i64,
    retry_count: i64,
    force: c_int,
    justid: c_int,
) -> RedliteStreamEntryArray {
    clear_error();
    let handle = get_db_ret!(db, RedliteStreamEntryArray {
        entries: ptr::null_mut(),
        len: 0,
    });

    let key_str = match cstr_to_str(key) {
        Ok(s) => s,
        Err(e) => {
            set_error(e);
            return RedliteStreamEntryArray {
                entries: ptr::null_mut(),
                len: 0,
            };
        }
    };

    let group_str = match cstr_to_str(group) {
        Ok(s) => s,
        Err(e) => {
            set_error(e);
            return RedliteStreamEntryArray {
                entries: ptr::null_mut(),
                len: 0,
            };
        }
    };

    let consumer_str = match cstr_to_str(consumer) {
        Ok(s) => s,
        Err(e) => {
            set_error(e);
            return RedliteStreamEntryArray {
                entries: ptr::null_mut(),
                len: 0,
            };
        }
    };

    if ids.is_null() || ids_len == 0 {
        set_error("XCLAIM requires at least one ID".to_string());
        return RedliteStreamEntryArray {
            entries: ptr::null_mut(),
            len: 0,
        };
    }

    let ids_slice = unsafe { slice::from_raw_parts(ids, ids_len) };
    let stream_ids: Vec<redlite::StreamId> = ids_slice
        .iter()
        .map(|id| redlite::StreamId { ms: id.ms, seq: id.seq })
        .collect();

    let idle_opt = if idle_ms > 0 { Some(idle_ms) } else { None };
    let time_opt = if time_ms > 0 { Some(time_ms) } else { None };
    let retry_opt = if retry_count >= 0 { Some(retry_count) } else { None };

    let guard = handle.db.lock().unwrap();
    match guard.xclaim(
        key_str,
        group_str,
        consumer_str,
        min_idle_time,
        &stream_ids,
        idle_opt,
        time_opt,
        retry_opt,
        force != 0,
        justid != 0,
    ) {
        Ok(entries) => {
            let len = entries.len();
            if len == 0 {
                return RedliteStreamEntryArray {
                    entries: ptr::null_mut(),
                    len: 0,
                };
            }

            let mut c_entries: Vec<RedliteStreamEntry> = entries
                .into_iter()
                .map(|entry| {
                    let fields_len = entry.fields.len();
                    let mut fields: Vec<RedliteStreamField> = entry
                        .fields
                        .into_iter()
                        .map(|(k, v)| {
                            let k_bytes = vec_to_bytes(k);
                            let v_bytes = vec_to_bytes(v);
                            RedliteStreamField {
                                key: k_bytes.data,
                                key_len: k_bytes.len,
                                value: v_bytes.data,
                                value_len: v_bytes.len,
                            }
                        })
                        .collect();

                    let fields_ptr = fields.as_mut_ptr();
                    std::mem::forget(fields);

                    RedliteStreamEntry {
                        id: RedliteStreamId {
                            ms: entry.id.ms,
                            seq: entry.id.seq,
                        },
                        fields: fields_ptr,
                        fields_len,
                    }
                })
                .collect();

            let ptr = c_entries.as_mut_ptr();
            std::mem::forget(c_entries);

            RedliteStreamEntryArray { entries: ptr, len }
        }
        Err(e) => {
            set_error(format!("XCLAIM failed: {}", e));
            RedliteStreamEntryArray {
                entries: ptr::null_mut(),
                len: 0,
            }
        }
    }
}

/// XINFO STREAM key
/// Returns stream information or NULL on error
#[no_mangle]
pub extern "C" fn redlite_xinfo_stream(
    db: *mut RedliteDb,
    key: *const c_char,
) -> *mut RedliteStreamInfo {
    clear_error();
    let handle = get_db_ret!(db, ptr::null_mut());

    let key_str = match cstr_to_str(key) {
        Ok(s) => s,
        Err(e) => {
            set_error(e);
            return ptr::null_mut();
        }
    };

    let guard = handle.db.lock().unwrap();
    match guard.xinfo_stream(key_str) {
        Ok(Some(info)) => {
            let first_entry_ptr = if let Some(entry) = info.first_entry {
                let fields_len = entry.fields.len();
                let mut fields: Vec<RedliteStreamField> = entry
                    .fields
                    .into_iter()
                    .map(|(k, v)| {
                        let k_bytes = vec_to_bytes(k);
                        let v_bytes = vec_to_bytes(v);
                        RedliteStreamField {
                            key: k_bytes.data,
                            key_len: k_bytes.len,
                            value: v_bytes.data,
                            value_len: v_bytes.len,
                        }
                    })
                    .collect();

                let fields_ptr = fields.as_mut_ptr();
                std::mem::forget(fields);

                Box::into_raw(Box::new(RedliteStreamEntry {
                    id: RedliteStreamId {
                        ms: entry.id.ms,
                        seq: entry.id.seq,
                    },
                    fields: fields_ptr,
                    fields_len,
                }))
            } else {
                ptr::null_mut()
            };

            let last_entry_ptr = if let Some(entry) = info.last_entry {
                let fields_len = entry.fields.len();
                let mut fields: Vec<RedliteStreamField> = entry
                    .fields
                    .into_iter()
                    .map(|(k, v)| {
                        let k_bytes = vec_to_bytes(k);
                        let v_bytes = vec_to_bytes(v);
                        RedliteStreamField {
                            key: k_bytes.data,
                            key_len: k_bytes.len,
                            value: v_bytes.data,
                            value_len: v_bytes.len,
                        }
                    })
                    .collect();

                let fields_ptr = fields.as_mut_ptr();
                std::mem::forget(fields);

                Box::into_raw(Box::new(RedliteStreamEntry {
                    id: RedliteStreamId {
                        ms: entry.id.ms,
                        seq: entry.id.seq,
                    },
                    fields: fields_ptr,
                    fields_len,
                }))
            } else {
                ptr::null_mut()
            };

            Box::into_raw(Box::new(RedliteStreamInfo {
                length: info.length,
                radix_tree_keys: info.radix_tree_keys,
                radix_tree_nodes: info.radix_tree_nodes,
                last_generated_id: RedliteStreamId {
                    ms: info.last_generated_id.ms,
                    seq: info.last_generated_id.seq,
                },
                first_entry: first_entry_ptr,
                last_entry: last_entry_ptr,
            }))
        }
        Ok(None) => ptr::null_mut(),
        Err(e) => {
            set_error(format!("XINFO STREAM failed: {}", e));
            ptr::null_mut()
        }
    }
}

/// XINFO GROUPS key
/// Returns array of consumer group info
#[no_mangle]
pub extern "C" fn redlite_xinfo_groups(
    db: *mut RedliteDb,
    key: *const c_char,
) -> RedliteConsumerGroupInfoArray {
    clear_error();
    let handle = get_db_ret!(db, RedliteConsumerGroupInfoArray {
        groups: ptr::null_mut(),
        len: 0,
    });

    let key_str = match cstr_to_str(key) {
        Ok(s) => s,
        Err(e) => {
            set_error(e);
            return RedliteConsumerGroupInfoArray {
                groups: ptr::null_mut(),
                len: 0,
            };
        }
    };

    let guard = handle.db.lock().unwrap();
    match guard.xinfo_groups(key_str) {
        Ok(groups_vec) => {
            let len = groups_vec.len();
            if len == 0 {
                return RedliteConsumerGroupInfoArray {
                    groups: ptr::null_mut(),
                    len: 0,
                };
            }

            let mut groups: Vec<RedliteConsumerGroupInfo> = groups_vec
                .into_iter()
                .map(|g| RedliteConsumerGroupInfo {
                    name: CString::new(g.name).unwrap().into_raw(),
                    consumers: g.consumers,
                    pending: g.pending,
                    last_delivered_id: RedliteStreamId {
                        ms: g.last_delivered_id.ms,
                        seq: g.last_delivered_id.seq,
                    },
                })
                .collect();

            let ptr = groups.as_mut_ptr();
            std::mem::forget(groups);

            RedliteConsumerGroupInfoArray { groups: ptr, len }
        }
        Err(e) => {
            set_error(format!("XINFO GROUPS failed: {}", e));
            RedliteConsumerGroupInfoArray {
                groups: ptr::null_mut(),
                len: 0,
            }
        }
    }
}

/// XINFO CONSUMERS key group
/// Returns array of consumer info
#[no_mangle]
pub extern "C" fn redlite_xinfo_consumers(
    db: *mut RedliteDb,
    key: *const c_char,
    group: *const c_char,
) -> RedliteConsumerInfoArray {
    clear_error();
    let handle = get_db_ret!(db, RedliteConsumerInfoArray {
        consumers: ptr::null_mut(),
        len: 0,
    });

    let key_str = match cstr_to_str(key) {
        Ok(s) => s,
        Err(e) => {
            set_error(e);
            return RedliteConsumerInfoArray {
                consumers: ptr::null_mut(),
                len: 0,
            };
        }
    };

    let group_str = match cstr_to_str(group) {
        Ok(s) => s,
        Err(e) => {
            set_error(e);
            return RedliteConsumerInfoArray {
                consumers: ptr::null_mut(),
                len: 0,
            };
        }
    };

    let guard = handle.db.lock().unwrap();
    match guard.xinfo_consumers(key_str, group_str) {
        Ok(consumers_vec) => {
            let len = consumers_vec.len();
            if len == 0 {
                return RedliteConsumerInfoArray {
                    consumers: ptr::null_mut(),
                    len: 0,
                };
            }

            let mut consumers: Vec<RedliteConsumerInfo> = consumers_vec
                .into_iter()
                .map(|c| RedliteConsumerInfo {
                    name: CString::new(c.name).unwrap().into_raw(),
                    pending: c.pending,
                    idle: c.idle,
                })
                .collect();

            let ptr = consumers.as_mut_ptr();
            std::mem::forget(consumers);

            RedliteConsumerInfoArray { consumers: ptr, len }
        }
        Err(e) => {
            set_error(format!("XINFO CONSUMERS failed: {}", e));
            RedliteConsumerInfoArray {
                consumers: ptr::null_mut(),
                len: 0,
            }
        }
    }
}

// =============================================================================
// Geospatial Commands
// =============================================================================

/// GEOADD key [NX|XX] [CH] longitude latitude member [longitude latitude member ...]
/// Returns number of elements added
#[no_mangle]
pub extern "C" fn redlite_geoadd(
    db: *mut RedliteDb,
    key: *const c_char,
    members: *const RedliteGeoMember,
    members_len: size_t,
    nx: c_int,
    xx: c_int,
    ch: c_int,
) -> i64 {
    clear_error();
    let handle = get_db_ret!(db, -1);

    let key_str = match cstr_to_str(key) {
        Ok(s) => s,
        Err(e) => {
            set_error(e);
            return -1;
        }
    };

    if members.is_null() || members_len == 0 {
        return 0;
    }

    let members_slice = unsafe { slice::from_raw_parts(members, members_len) };
    let mut geo_members = Vec::new();
    for m in members_slice {
        let member_str = match cstr_to_str(m.member) {
            Ok(s) => s,
            Err(e) => {
                set_error(e);
                return -1;
            }
        };
        geo_members.push((m.longitude, m.latitude, member_str));
    }

    let geo_refs: Vec<(f64, f64, &str)> = geo_members.iter().map(|(lon, lat, m)| (*lon, *lat, m.as_ref())).collect();

    let guard = handle.db.lock().unwrap();
    match guard.geoadd(key_str, &geo_refs, nx != 0, xx != 0, ch != 0) {
        Ok(count) => count,
        Err(e) => {
            set_error(format!("GEOADD failed: {}", e));
            -1
        }
    }
}

/// GEOPOS key member [member ...]
/// Returns array of positions
#[no_mangle]
pub extern "C" fn redlite_geopos(
    db: *mut RedliteDb,
    key: *const c_char,
    members: *const *const c_char,
    members_len: size_t,
) -> RedliteGeoPosArray {
    clear_error();
    let handle = get_db_ret!(db, RedliteGeoPosArray {
        positions: ptr::null_mut(),
        len: 0,
    });

    let key_str = match cstr_to_str(key) {
        Ok(s) => s,
        Err(e) => {
            set_error(e);
            return RedliteGeoPosArray {
                positions: ptr::null_mut(),
                len: 0,
            };
        }
    };

    if members.is_null() || members_len == 0 {
        return RedliteGeoPosArray {
            positions: ptr::null_mut(),
            len: 0,
        };
    }

    let members_slice = unsafe { slice::from_raw_parts(members, members_len) };
    let mut member_strs = Vec::new();
    for &m_ptr in members_slice {
        match cstr_to_str(m_ptr) {
            Ok(s) => member_strs.push(s),
            Err(e) => {
                set_error(e);
                return RedliteGeoPosArray {
                    positions: ptr::null_mut(),
                    len: 0,
                };
            }
        }
    }

    let member_refs: Vec<&str> = member_strs.iter().map(|s| s.as_ref()).collect();

    let guard = handle.db.lock().unwrap();
    match guard.geopos(key_str, &member_refs) {
        Ok(positions) => {
            let len = positions.len();
            let mut c_positions: Vec<RedliteGeoPos> = positions
                .into_iter()
                .map(|opt_pos| {
                    if let Some((lon, lat)) = opt_pos {
                        RedliteGeoPos {
                            longitude: lon,
                            latitude: lat,
                            exists: 1,
                        }
                    } else {
                        RedliteGeoPos {
                            longitude: 0.0,
                            latitude: 0.0,
                            exists: 0,
                        }
                    }
                })
                .collect();

            let ptr = c_positions.as_mut_ptr();
            std::mem::forget(c_positions);

            RedliteGeoPosArray { positions: ptr, len }
        }
        Err(e) => {
            set_error(format!("GEOPOS failed: {}", e));
            RedliteGeoPosArray {
                positions: ptr::null_mut(),
                len: 0,
            }
        }
    }
}

/// GEODIST key member1 member2 [M|KM|FT|MI]
/// unit: 0=M, 1=KM, 2=FT, 3=MI
/// Returns distance or -1.0 on error/not found
#[no_mangle]
pub extern "C" fn redlite_geodist(
    db: *mut RedliteDb,
    key: *const c_char,
    member1: *const c_char,
    member2: *const c_char,
    unit: c_int,
) -> f64 {
    clear_error();
    let handle = get_db_ret!(db, -1.0);

    let key_str = match cstr_to_str(key) {
        Ok(s) => s,
        Err(e) => {
            set_error(e);
            return -1.0;
        }
    };

    let member1_str = match cstr_to_str(member1) {
        Ok(s) => s,
        Err(e) => {
            set_error(e);
            return -1.0;
        }
    };

    let member2_str = match cstr_to_str(member2) {
        Ok(s) => s,
        Err(e) => {
            set_error(e);
            return -1.0;
        }
    };

    use redlite::types::GeoUnit;
    let geo_unit = match unit {
        1 => GeoUnit::Kilometers,
        2 => GeoUnit::Feet,
        3 => GeoUnit::Miles,
        _ => GeoUnit::Meters,
    };

    let guard = handle.db.lock().unwrap();
    match guard.geodist(key_str, member1_str, member2_str, geo_unit) {
        Ok(Some(dist)) => dist,
        Ok(None) => -1.0,
        Err(e) => {
            set_error(format!("GEODIST failed: {}", e));
            -1.0
        }
    }
}

/// GEOHASH key member [member ...]
/// Returns array of geohash strings
#[no_mangle]
pub extern "C" fn redlite_geohash(
    db: *mut RedliteDb,
    key: *const c_char,
    members: *const *const c_char,
    members_len: size_t,
) -> RedliteStringArray {
    clear_error();
    let handle = get_db_ret!(db, RedliteStringArray {
        strings: ptr::null_mut(),
        len: 0,
    });

    let key_str = match cstr_to_str(key) {
        Ok(s) => s,
        Err(e) => {
            set_error(e);
            return RedliteStringArray {
                strings: ptr::null_mut(),
                len: 0,
            };
        }
    };

    if members.is_null() || members_len == 0 {
        return RedliteStringArray {
            strings: ptr::null_mut(),
            len: 0,
        };
    }

    let members_slice = unsafe { slice::from_raw_parts(members, members_len) };
    let mut member_strs = Vec::new();
    for &m_ptr in members_slice {
        match cstr_to_str(m_ptr) {
            Ok(s) => member_strs.push(s),
            Err(e) => {
                set_error(e);
                return RedliteStringArray {
                    strings: ptr::null_mut(),
                    len: 0,
                };
            }
        }
    }

    let member_refs: Vec<&str> = member_strs.iter().map(|s| s.as_ref()).collect();

    let guard = handle.db.lock().unwrap();
    match guard.geohash(key_str, &member_refs) {
        Ok(hashes) => {
            let len = hashes.len();
            let c_strings: Vec<*mut c_char> = hashes
                .into_iter()
                .map(|opt_hash| {
                    if let Some(hash) = opt_hash {
                        CString::new(hash).unwrap().into_raw()
                    } else {
                        ptr::null_mut()
                    }
                })
                .collect();

            let mut c_strs = c_strings;
            let ptr = c_strs.as_mut_ptr();
            std::mem::forget(c_strs);

            RedliteStringArray { strings: ptr, len }
        }
        Err(e) => {
            set_error(format!("GEOHASH failed: {}", e));
            RedliteStringArray {
                strings: ptr::null_mut(),
                len: 0,
            }
        }
    }
}

/// GEOSEARCH key FROMMEMBER member|FROMLONLAT lon lat BYRADIUS radius|BYBOX width height M|KM|FT|MI [ASC|DESC] [COUNT count] [WITHDIST]
/// Simplified: searches by radius from a point
/// from_member: if not NULL, search from this member; else use from_lon/from_lat
/// radius: search radius
/// unit: 0=M, 1=KM, 2=FT, 3=MI
/// count: limit results (0 for no limit)
/// withdist: 1 to include distance in results
#[no_mangle]
pub extern "C" fn redlite_geosearch(
    db: *mut RedliteDb,
    key: *const c_char,
    from_member: *const c_char,
    from_lon: f64,
    from_lat: f64,
    radius: f64,
    unit: c_int,
    count: i64,
    withdist: c_int,
) -> RedliteGeoMemberArray {
    clear_error();
    let handle = get_db_ret!(db, RedliteGeoMemberArray {
        members: ptr::null_mut(),
        len: 0,
    });

    let key_str = match cstr_to_str(key) {
        Ok(s) => s,
        Err(e) => {
            set_error(e);
            return RedliteGeoMemberArray {
                members: ptr::null_mut(),
                len: 0,
            };
        }
    };

    use redlite::types::{GeoUnit, GeoSearchOptions};
    let geo_unit = match unit {
        1 => GeoUnit::Kilometers,
        2 => GeoUnit::Feet,
        3 => GeoUnit::Miles,
        _ => GeoUnit::Meters,
    };

    let (from_member_opt, from_lonlat_opt) = if from_member.is_null() {
        (None, Some((from_lon, from_lat)))
    } else {
        match cstr_to_str(from_member) {
            Ok(s) => (Some(s.to_string()), None),
            Err(e) => {
                set_error(e);
                return RedliteGeoMemberArray {
                    members: ptr::null_mut(),
                    len: 0,
                };
            }
        }
    };

    let options = GeoSearchOptions {
        from_member: from_member_opt,
        from_lonlat: from_lonlat_opt,
        by_radius: Some((radius, geo_unit)),
        by_box: None,
        ascending: false,
        count: if count > 0 { Some(count as usize) } else { None },
        any: false,
        with_coord: false,
        with_dist: withdist != 0,
        with_hash: false,
    };

    let guard = handle.db.lock().unwrap();
    match guard.geosearch(key_str, &options) {
        Ok(members) => {
            let len = members.len();
            if len == 0 {
                return RedliteGeoMemberArray {
                    members: ptr::null_mut(),
                    len: 0,
                };
            }

            let mut c_members: Vec<RedliteGeoMember> = members
                .into_iter()
                .map(|m| RedliteGeoMember {
                    member: CString::new(m.member).unwrap().into_raw(),
                    longitude: m.longitude,
                    latitude: m.latitude,
                    dist: m.distance.unwrap_or(0.0),
                })
                .collect();

            let ptr = c_members.as_mut_ptr();
            std::mem::forget(c_members);

            RedliteGeoMemberArray { members: ptr, len }
        }
        Err(e) => {
            set_error(format!("GEOSEARCH failed: {}", e));
            RedliteGeoMemberArray {
                members: ptr::null_mut(),
                len: 0,
            }
        }
    }
}

/// GEOSEARCHSTORE dest source <GEOSEARCH args>
/// Returns number of elements stored
#[no_mangle]
pub extern "C" fn redlite_geosearchstore(
    db: *mut RedliteDb,
    dest: *const c_char,
    src: *const c_char,
    from_member: *const c_char,
    from_lon: f64,
    from_lat: f64,
    radius: f64,
    unit: c_int,
    count: i64,
    store_dist: c_int,
) -> i64 {
    clear_error();
    let handle = get_db_ret!(db, -1);

    let dest_str = match cstr_to_str(dest) {
        Ok(s) => s,
        Err(e) => {
            set_error(e);
            return -1;
        }
    };

    let src_str = match cstr_to_str(src) {
        Ok(s) => s,
        Err(e) => {
            set_error(e);
            return -1;
        }
    };

    use redlite::types::{GeoUnit, GeoSearchOptions};
    let geo_unit = match unit {
        1 => GeoUnit::Kilometers,
        2 => GeoUnit::Feet,
        3 => GeoUnit::Miles,
        _ => GeoUnit::Meters,
    };

    let (from_member_opt, from_lonlat_opt) = if from_member.is_null() {
        (None, Some((from_lon, from_lat)))
    } else {
        match cstr_to_str(from_member) {
            Ok(s) => (Some(s.to_string()), None),
            Err(e) => {
                set_error(e);
                return -1;
            }
        }
    };

    let options = GeoSearchOptions {
        from_member: from_member_opt,
        from_lonlat: from_lonlat_opt,
        by_radius: Some((radius, geo_unit)),
        by_box: None,
        ascending: false,
        count: if count > 0 { Some(count as usize) } else { None },
        any: false,
        with_coord: false,
        with_dist: store_dist != 0,
        with_hash: false,
    };

    let guard = handle.db.lock().unwrap();
    match guard.geosearchstore(dest_str, src_str, &options, store_dist != 0) {
        Ok(n) => n,
        Err(e) => {
            set_error(format!("GEOSEARCHSTORE failed: {}", e));
            -1
        }
    }
}

/// FT.CREATE - Simplified: create search index
/// For simplicity, this is a stub that returns success (full implementation complex)
#[no_mangle]
pub extern "C" fn redlite_ft_create(
    db: *mut RedliteDb,
    index_name: *const c_char,
) -> i64 {
    clear_error();
    let handle = get_db_ret!(db, -1);

    let _name = match cstr_to_str(index_name) {
        Ok(s) => s,
        Err(e) => {
            set_error(e);
            return -1;
        }
    };

    // Simplified stub - full implementation would need schema definition
    let _guard = handle.db.lock().unwrap();
    set_error("FT.CREATE not fully implemented in FFI (complex schema types)".to_string());
    -1
}

/// FT.DROPINDEX - Drop search index
#[no_mangle]
pub extern "C" fn redlite_ft_dropindex(
    db: *mut RedliteDb,
    index_name: *const c_char,
    delete_docs: c_int,
) -> i64 {
    clear_error();
    let handle = get_db_ret!(db, -1);

    let name = match cstr_to_str(index_name) {
        Ok(s) => s,
        Err(e) => {
            set_error(e);
            return -1;
        }
    };

    let guard = handle.db.lock().unwrap();
    match guard.ft_dropindex(name, delete_docs != 0) {
        Ok(true) => 1,
        Ok(false) => 0,
        Err(e) => {
            set_error(format!("FT.DROPINDEX failed: {}", e));
            -1
        }
    }
}

/// FT._LIST - List all search indexes
#[no_mangle]
pub extern "C" fn redlite_ft_list(db: *mut RedliteDb) -> RedliteStringArray {
    clear_error();
    let handle = get_db_ret!(db, RedliteStringArray {
        strings: ptr::null_mut(),
        len: 0,
    });

    let guard = handle.db.lock().unwrap();
    match guard.ft_list() {
        Ok(indexes) => {
            let len = indexes.len();
            if len == 0 {
                return RedliteStringArray {
                    strings: ptr::null_mut(),
                    len: 0,
                };
            }

            let c_strings: Vec<*mut c_char> = indexes
                .into_iter()
                .map(|idx| CString::new(idx).unwrap().into_raw())
                .collect();

            let mut c_strs = c_strings;
            let ptr = c_strs.as_mut_ptr();
            std::mem::forget(c_strs);

            RedliteStringArray { strings: ptr, len }
        }
        Err(e) => {
            set_error(format!("FT._LIST failed: {}", e));
            RedliteStringArray {
                strings: ptr::null_mut(),
                len: 0,
            }
        }
    }
}

// Simplified stubs for remaining FT commands (complex types)
/// FT.INFO - stub
#[no_mangle]
pub extern "C" fn redlite_ft_info(db: *mut RedliteDb, _index: *const c_char) -> i64 {
    clear_error();
    let _handle = get_db_ret!(db, -1);
    set_error("FT.INFO not fully implemented in FFI (complex return types)".to_string());
    -1
}

/// FT.ALTER - stub
#[no_mangle]
pub extern "C" fn redlite_ft_alter(db: *mut RedliteDb, _index: *const c_char) -> i64 {
    clear_error();
    let _handle = get_db_ret!(db, -1);
    set_error("FT.ALTER not fully implemented in FFI (complex schema types)".to_string());
    -1
}

/// FT.SEARCH - stub
#[no_mangle]
pub extern "C" fn redlite_ft_search(db: *mut RedliteDb, _index: *const c_char, _query: *const c_char) -> i64 {
    clear_error();
    let _handle = get_db_ret!(db, -1);
    set_error("FT.SEARCH not fully implemented in FFI (complex options/results)".to_string());
    -1
}

/// FT.ALIASADD - stub
#[no_mangle]
pub extern "C" fn redlite_ft_aliasadd(db: *mut RedliteDb, _alias: *const c_char, _index: *const c_char) -> i64 {
    clear_error();
    let _handle = get_db_ret!(db, -1);
    set_error("FT.ALIASADD not fully implemented in FFI".to_string());
    -1
}

/// FT.ALIASDEL - stub
#[no_mangle]
pub extern "C" fn redlite_ft_aliasdel(db: *mut RedliteDb, _alias: *const c_char) -> i64 {
    clear_error();
    let _handle = get_db_ret!(db, -1);
    set_error("FT.ALIASDEL not fully implemented in FFI".to_string());
    -1
}

/// FT.ALIASUPDATE - stub
#[no_mangle]
pub extern "C" fn redlite_ft_aliasupdate(db: *mut RedliteDb, _alias: *const c_char, _index: *const c_char) -> i64 {
    clear_error();
    let _handle = get_db_ret!(db, -1);
    set_error("FT.ALIASUPDATE not fully implemented in FFI".to_string());
    -1
}

/// FT.SYNUPDATE - stub
#[no_mangle]
pub extern "C" fn redlite_ft_synupdate(db: *mut RedliteDb, _index: *const c_char) -> i64 {
    clear_error();
    let _handle = get_db_ret!(db, -1);
    set_error("FT.SYNUPDATE not fully implemented in FFI".to_string());
    -1
}

/// FT.SYNDUMP - stub
#[no_mangle]
pub extern "C" fn redlite_ft_syndump(db: *mut RedliteDb, _index: *const c_char) -> i64 {
    clear_error();
    let _handle = get_db_ret!(db, -1);
    set_error("FT.SYNDUMP not fully implemented in FFI".to_string());
    -1
}

/// FT.SUGADD - stub
#[no_mangle]
pub extern "C" fn redlite_ft_sugadd(db: *mut RedliteDb, _key: *const c_char, _string: *const c_char, _score: f64) -> i64 {
    clear_error();
    let _handle = get_db_ret!(db, -1);
    set_error("FT.SUGADD not fully implemented in FFI".to_string());
    -1
}

/// FT.SUGGET - stub
#[no_mangle]
pub extern "C" fn redlite_ft_sugget(db: *mut RedliteDb, _key: *const c_char, _prefix: *const c_char) -> i64 {
    clear_error();
    let _handle = get_db_ret!(db, -1);
    set_error("FT.SUGGET not fully implemented in FFI".to_string());
    -1
}

/// FT.SUGDEL - stub
#[no_mangle]
pub extern "C" fn redlite_ft_sugdel(db: *mut RedliteDb, _key: *const c_char, _string: *const c_char) -> i64 {
    clear_error();
    let _handle = get_db_ret!(db, -1);
    set_error("FT.SUGDEL not fully implemented in FFI".to_string());
    -1
}

/// FT.SUGLEN - stub
#[no_mangle]
pub extern "C" fn redlite_ft_suglen(db: *mut RedliteDb, _key: *const c_char) -> i64 {
    clear_error();
    let _handle = get_db_ret!(db, -1);
    set_error("FT.SUGLEN not fully implemented in FFI".to_string());
    -1
}

// =============================================================================
// History Commands
// =============================================================================

/// HISTORY GET key [LIMIT limit] [SINCE timestamp] [UNTIL timestamp]
/// Returns array of history entries for the key
#[no_mangle]
pub extern "C" fn redlite_history_get(
    db: *mut RedliteDb,
    key: *const c_char,
    limit: i64,
    since: i64,
    until: i64,
) -> RedliteHistoryEntryArray {
    clear_error();
    let handle = get_db_ret!(db, RedliteHistoryEntryArray {
        entries: ptr::null_mut(),
        len: 0,
    });

    let key_str = match cstr_to_str(key) {
        Ok(s) => s,
        Err(e) => {
            set_error(e);
            return RedliteHistoryEntryArray {
                entries: ptr::null_mut(),
                len: 0,
            };
        }
    };

    let limit_opt = if limit > 0 { Some(limit) } else { None };
    let since_opt = if since > 0 { Some(since) } else { None };
    let until_opt = if until > 0 { Some(until) } else { None };

    let guard = handle.db.lock().unwrap();
    match guard.history_get(key_str, limit_opt, since_opt, until_opt) {
        Ok(entries) => {
            let len = entries.len();
            if len == 0 {
                return RedliteHistoryEntryArray {
                    entries: ptr::null_mut(),
                    len: 0,
                };
            }

            let mut c_entries: Vec<RedliteHistoryEntry> = entries
                .into_iter()
                .map(|entry| RedliteHistoryEntry {
                    timestamp: entry.timestamp_ms,
                    value: vec_to_bytes(entry.data_snapshot.unwrap_or_default()),
                })
                .collect();

            let ptr = c_entries.as_mut_ptr();
            std::mem::forget(c_entries);

            RedliteHistoryEntryArray { entries: ptr, len }
        }
        Err(e) => {
            set_error(format!("HISTORY GET failed: {}", e));
            RedliteHistoryEntryArray {
                entries: ptr::null_mut(),
                len: 0,
            }
        }
    }
}

/// HISTORY GETAT key timestamp
/// Returns value of key at a specific timestamp (time-travel query)
#[no_mangle]
pub extern "C" fn redlite_history_getat(
    db: *mut RedliteDb,
    key: *const c_char,
    timestamp: i64,
) -> RedliteBytes {
    clear_error();
    let handle = get_db_ret!(db, RedliteBytes { data: ptr::null_mut(), len: 0 });

    let key_str = match cstr_to_str(key) {
        Ok(s) => s,
        Err(e) => {
            set_error(e);
            return RedliteBytes { data: ptr::null_mut(), len: 0 };
        }
    };

    let guard = handle.db.lock().unwrap();
    match guard.history_get_at(key_str, timestamp) {
        Ok(Some(v)) => vec_to_bytes(v),
        Ok(None) => RedliteBytes { data: ptr::null_mut(), len: 0 },
        Err(e) => {
            set_error(format!("HISTORY GETAT failed: {}", e));
            RedliteBytes { data: ptr::null_mut(), len: 0 }
        }
    }
}

/// HISTORY LIST [PATTERN pattern]
/// Returns list of all keys that have history tracking enabled
#[no_mangle]
pub extern "C" fn redlite_history_list(
    db: *mut RedliteDb,
    pattern: *const c_char,
) -> RedliteStringArray {
    clear_error();
    let handle = get_db_ret!(db, RedliteStringArray {
        strings: ptr::null_mut(),
        len: 0,
    });

    let pattern_opt = if pattern.is_null() {
        None
    } else {
        match cstr_to_str(pattern) {
            Ok(s) => Some(s),
            Err(e) => {
                set_error(e);
                return RedliteStringArray {
                    strings: ptr::null_mut(),
                    len: 0,
                };
            }
        }
    };

    let guard = handle.db.lock().unwrap();
    match guard.history_list_keys(pattern_opt) {
        Ok(keys) => {
            let len = keys.len();
            if len == 0 {
                return RedliteStringArray {
                    strings: ptr::null_mut(),
                    len: 0,
                };
            }

            let c_strings: Vec<*mut c_char> = keys
                .into_iter()
                .map(|k| CString::new(k).unwrap().into_raw())
                .collect();

            let mut c_strs = c_strings;
            let ptr = c_strs.as_mut_ptr();
            std::mem::forget(c_strs);

            RedliteStringArray { strings: ptr, len }
        }
        Err(e) => {
            set_error(format!("HISTORY LIST failed: {}", e));
            RedliteStringArray {
                strings: ptr::null_mut(),
                len: 0,
            }
        }
    }
}

/// HISTORY STATS [key]
/// Returns statistics about history tracking
/// For now returns a simplified count as i64 (enhance later if needed)
#[no_mangle]
pub extern "C" fn redlite_history_stats(
    db: *mut RedliteDb,
    key: *const c_char,
) -> i64 {
    clear_error();
    let handle = get_db_ret!(db, -1);

    let key_opt = if key.is_null() {
        None
    } else {
        match cstr_to_str(key) {
            Ok(s) => Some(s),
            Err(e) => {
                set_error(e);
                return -1;
            }
        }
    };

    let guard = handle.db.lock().unwrap();
    match guard.history_stats(key_opt) {
        Ok(_stats) => {
            // For simplicity, return 0 for success (would need complex struct for full stats)
            0
        }
        Err(e) => {
            set_error(format!("HISTORY STATS failed: {}", e));
            -1
        }
    }
}

/// HISTORY CLEAR key [BEFORE timestamp]
/// Clear history entries for a key, returns number of entries deleted
#[no_mangle]
pub extern "C" fn redlite_history_clear(
    db: *mut RedliteDb,
    key: *const c_char,
    before: i64,
) -> i64 {
    clear_error();
    let handle = get_db_ret!(db, -1);

    let key_str = match cstr_to_str(key) {
        Ok(s) => s,
        Err(e) => {
            set_error(e);
            return -1;
        }
    };

    let before_opt = if before > 0 { Some(before) } else { None };

    let guard = handle.db.lock().unwrap();
    match guard.history_clear_key(key_str, before_opt) {
        Ok(count) => count,
        Err(e) => {
            set_error(format!("HISTORY CLEAR failed: {}", e));
            -1
        }
    }
}

/// HISTORY PRUNE before_timestamp
/// Prune old history entries across all keys, returns number deleted
#[no_mangle]
pub extern "C" fn redlite_history_prune(
    db: *mut RedliteDb,
    before_timestamp: i64,
) -> i64 {
    clear_error();
    let handle = get_db_ret!(db, -1);

    let guard = handle.db.lock().unwrap();
    match guard.history_prune(before_timestamp) {
        Ok(count) => count,
        Err(e) => {
            set_error(format!("HISTORY PRUNE failed: {}", e));
            -1
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

// =============================================================================
// JSON Commands
// =============================================================================

/// JSON.SET key path value [NX|XX]
/// Set JSON value at path. Returns true on success.
#[no_mangle]
pub extern "C" fn redlite_json_set(
    db: *mut RedliteDb,
    key: *const c_char,
    path: *const c_char,
    value: *const c_char,
    nx: c_int,
    xx: c_int,
) -> c_int {
    clear_error();
    let handle = get_db_ret!(db, -1);

    let key_str = match cstr_to_str(key) {
        Ok(s) => s,
        Err(e) => {
            set_error(e);
            return -1;
        }
    };
    let path_str = match cstr_to_str(path) {
        Ok(s) => s,
        Err(e) => {
            set_error(e);
            return -1;
        }
    };
    let value_str = match cstr_to_str(value) {
        Ok(s) => s,
        Err(e) => {
            set_error(e);
            return -1;
        }
    };

    let guard = handle.db.lock().unwrap();
    match guard.json_set(key_str, path_str, value_str, nx != 0, xx != 0) {
        Ok(true) => 1,
        Ok(false) => 0,
        Err(e) => {
            set_error(format!("JSON.SET failed: {}", e));
            -1
        }
    }
}

/// JSON.GET key [path...]
/// Get JSON value at path(s). Returns JSON string or null.
#[no_mangle]
pub extern "C" fn redlite_json_get(
    db: *mut RedliteDb,
    key: *const c_char,
    paths: *const *const c_char,
    paths_len: usize,
) -> *mut c_char {
    clear_error();
    let handle = get_db_ret!(db, ptr::null_mut());

    let key_str = match cstr_to_str(key) {
        Ok(s) => s,
        Err(e) => {
            set_error(e);
            return ptr::null_mut();
        }
    };

    let path_vec: Vec<&str> = if paths.is_null() || paths_len == 0 {
        vec!["$"]
    } else {
        let slice = unsafe { std::slice::from_raw_parts(paths, paths_len) };
        let mut v = Vec::with_capacity(paths_len);
        for &p in slice {
            match cstr_to_str(p) {
                Ok(s) => v.push(s),
                Err(e) => {
                    set_error(e);
                    return ptr::null_mut();
                }
            }
        }
        v
    };

    let guard = handle.db.lock().unwrap();
    match guard.json_get(key_str, &path_vec) {
        Ok(Some(s)) => CString::new(s).unwrap().into_raw(),
        Ok(None) => ptr::null_mut(),
        Err(e) => {
            set_error(format!("JSON.GET failed: {}", e));
            ptr::null_mut()
        }
    }
}

/// JSON.DEL key [path]
/// Delete JSON value at path. Returns number of values deleted.
#[no_mangle]
pub extern "C" fn redlite_json_del(
    db: *mut RedliteDb,
    key: *const c_char,
    path: *const c_char,
) -> i64 {
    clear_error();
    let handle = get_db_ret!(db, -1);

    let key_str = match cstr_to_str(key) {
        Ok(s) => s,
        Err(e) => {
            set_error(e);
            return -1;
        }
    };
    let path_opt = if path.is_null() {
        None
    } else {
        match cstr_to_str(path) {
            Ok(s) => Some(s),
            Err(e) => {
                set_error(e);
                return -1;
            }
        }
    };

    let guard = handle.db.lock().unwrap();
    match guard.json_del(key_str, path_opt) {
        Ok(n) => n,
        Err(e) => {
            set_error(format!("JSON.DEL failed: {}", e));
            -1
        }
    }
}

/// JSON.TYPE key [path]
/// Get the type of JSON value at path.
#[no_mangle]
pub extern "C" fn redlite_json_type(
    db: *mut RedliteDb,
    key: *const c_char,
    path: *const c_char,
) -> *mut c_char {
    clear_error();
    let handle = get_db_ret!(db, ptr::null_mut());

    let key_str = match cstr_to_str(key) {
        Ok(s) => s,
        Err(e) => {
            set_error(e);
            return ptr::null_mut();
        }
    };
    let path_opt = if path.is_null() {
        None
    } else {
        match cstr_to_str(path) {
            Ok(s) => Some(s),
            Err(e) => {
                set_error(e);
                return ptr::null_mut();
            }
        }
    };

    let guard = handle.db.lock().unwrap();
    match guard.json_type(key_str, path_opt) {
        Ok(Some(s)) => CString::new(s).unwrap().into_raw(),
        Ok(None) => ptr::null_mut(),
        Err(e) => {
            set_error(format!("JSON.TYPE failed: {}", e));
            ptr::null_mut()
        }
    }
}

/// JSON.NUMINCRBY key path increment
/// Increment numeric value at path. Returns the new value as string.
#[no_mangle]
pub extern "C" fn redlite_json_numincrby(
    db: *mut RedliteDb,
    key: *const c_char,
    path: *const c_char,
    increment: f64,
) -> *mut c_char {
    clear_error();
    let handle = get_db_ret!(db, ptr::null_mut());

    let key_str = match cstr_to_str(key) {
        Ok(s) => s,
        Err(e) => {
            set_error(e);
            return ptr::null_mut();
        }
    };
    let path_str = match cstr_to_str(path) {
        Ok(s) => s,
        Err(e) => {
            set_error(e);
            return ptr::null_mut();
        }
    };

    let guard = handle.db.lock().unwrap();
    match guard.json_numincrby(key_str, path_str, increment) {
        Ok(s) => CString::new(s).unwrap().into_raw(),
        Err(e) => {
            set_error(format!("JSON.NUMINCRBY failed: {}", e));
            ptr::null_mut()
        }
    }
}

/// JSON.STRAPPEND key [path] value
/// Append string to JSON string at path. Returns new length.
#[no_mangle]
pub extern "C" fn redlite_json_strappend(
    db: *mut RedliteDb,
    key: *const c_char,
    path: *const c_char,
    value: *const c_char,
) -> i64 {
    clear_error();
    let handle = get_db_ret!(db, -1);

    let key_str = match cstr_to_str(key) {
        Ok(s) => s,
        Err(e) => {
            set_error(e);
            return -1;
        }
    };
    let path_opt = if path.is_null() {
        None
    } else {
        match cstr_to_str(path) {
            Ok(s) => Some(s),
            Err(e) => {
                set_error(e);
                return -1;
            }
        }
    };
    let value_str = match cstr_to_str(value) {
        Ok(s) => s,
        Err(e) => {
            set_error(e);
            return -1;
        }
    };

    let guard = handle.db.lock().unwrap();
    match guard.json_strappend(key_str, path_opt, value_str) {
        Ok(n) => n,
        Err(e) => {
            set_error(format!("JSON.STRAPPEND failed: {}", e));
            -1
        }
    }
}

/// JSON.STRLEN key [path]
/// Get length of JSON string at path.
#[no_mangle]
pub extern "C" fn redlite_json_strlen(
    db: *mut RedliteDb,
    key: *const c_char,
    path: *const c_char,
) -> i64 {
    clear_error();
    let handle = get_db_ret!(db, -1);

    let key_str = match cstr_to_str(key) {
        Ok(s) => s,
        Err(e) => {
            set_error(e);
            return -1;
        }
    };
    let path_opt = if path.is_null() {
        None
    } else {
        match cstr_to_str(path) {
            Ok(s) => Some(s),
            Err(e) => {
                set_error(e);
                return -1;
            }
        }
    };

    let guard = handle.db.lock().unwrap();
    match guard.json_strlen(key_str, path_opt) {
        Ok(Some(n)) => n,
        Ok(None) => -2, // Indicates null/not found (different from error)
        Err(e) => {
            set_error(format!("JSON.STRLEN failed: {}", e));
            -1
        }
    }
}

/// JSON.ARRAPPEND key path value [value...]
/// Append values to JSON array. Returns new array length.
#[no_mangle]
pub extern "C" fn redlite_json_arrappend(
    db: *mut RedliteDb,
    key: *const c_char,
    path: *const c_char,
    values: *const *const c_char,
    values_len: usize,
) -> i64 {
    clear_error();
    let handle = get_db_ret!(db, -1);

    let key_str = match cstr_to_str(key) {
        Ok(s) => s,
        Err(e) => {
            set_error(e);
            return -1;
        }
    };
    let path_str = match cstr_to_str(path) {
        Ok(s) => s,
        Err(e) => {
            set_error(e);
            return -1;
        }
    };

    if values.is_null() || values_len == 0 {
        set_error("JSON.ARRAPPEND requires at least one value".to_string());
        return -1;
    }

    let slice = unsafe { std::slice::from_raw_parts(values, values_len) };
    let mut value_vec = Vec::with_capacity(values_len);
    for &v in slice {
        match cstr_to_str(v) {
            Ok(s) => value_vec.push(s),
            Err(e) => {
                set_error(e);
                return -1;
            }
        }
    }

    let guard = handle.db.lock().unwrap();
    match guard.json_arrappend(key_str, path_str, &value_vec) {
        Ok(n) => n,
        Err(e) => {
            set_error(format!("JSON.ARRAPPEND failed: {}", e));
            -1
        }
    }
}

/// JSON.ARRLEN key [path]
/// Get length of JSON array at path.
#[no_mangle]
pub extern "C" fn redlite_json_arrlen(
    db: *mut RedliteDb,
    key: *const c_char,
    path: *const c_char,
) -> i64 {
    clear_error();
    let handle = get_db_ret!(db, -1);

    let key_str = match cstr_to_str(key) {
        Ok(s) => s,
        Err(e) => {
            set_error(e);
            return -1;
        }
    };
    let path_opt = if path.is_null() {
        None
    } else {
        match cstr_to_str(path) {
            Ok(s) => Some(s),
            Err(e) => {
                set_error(e);
                return -1;
            }
        }
    };

    let guard = handle.db.lock().unwrap();
    match guard.json_arrlen(key_str, path_opt) {
        Ok(Some(n)) => n,
        Ok(None) => -2,
        Err(e) => {
            set_error(format!("JSON.ARRLEN failed: {}", e));
            -1
        }
    }
}

/// JSON.ARRPOP key [path [index]]
/// Pop element from JSON array. Returns the popped element as JSON string.
#[no_mangle]
pub extern "C" fn redlite_json_arrpop(
    db: *mut RedliteDb,
    key: *const c_char,
    path: *const c_char,
    index: i64,
    use_index: c_int,
) -> *mut c_char {
    clear_error();
    let handle = get_db_ret!(db, ptr::null_mut());

    let key_str = match cstr_to_str(key) {
        Ok(s) => s,
        Err(e) => {
            set_error(e);
            return ptr::null_mut();
        }
    };
    let path_opt = if path.is_null() {
        None
    } else {
        match cstr_to_str(path) {
            Ok(s) => Some(s),
            Err(e) => {
                set_error(e);
                return ptr::null_mut();
            }
        }
    };
    let index_opt = if use_index != 0 { Some(index) } else { None };

    let guard = handle.db.lock().unwrap();
    match guard.json_arrpop(key_str, path_opt, index_opt) {
        Ok(Some(s)) => CString::new(s).unwrap().into_raw(),
        Ok(None) => ptr::null_mut(),
        Err(e) => {
            set_error(format!("JSON.ARRPOP failed: {}", e));
            ptr::null_mut()
        }
    }
}

/// JSON.CLEAR key [path]
/// Clear container values (arrays/objects). Returns count of cleared values.
#[no_mangle]
pub extern "C" fn redlite_json_clear(
    db: *mut RedliteDb,
    key: *const c_char,
    path: *const c_char,
) -> i64 {
    clear_error();
    let handle = get_db_ret!(db, -1);

    let key_str = match cstr_to_str(key) {
        Ok(s) => s,
        Err(e) => {
            set_error(e);
            return -1;
        }
    };
    let path_opt = if path.is_null() {
        None
    } else {
        match cstr_to_str(path) {
            Ok(s) => Some(s),
            Err(e) => {
                set_error(e);
                return -1;
            }
        }
    };

    let guard = handle.db.lock().unwrap();
    match guard.json_clear(key_str, path_opt) {
        Ok(n) => n,
        Err(e) => {
            set_error(format!("JSON.CLEAR failed: {}", e));
            -1
        }
    }
}

// =============================================================================
// History Enable/Disable Commands
// =============================================================================

/// HISTORY ENABLE GLOBAL [retention_type] [retention_value]
/// Enable history tracking globally.
/// retention_type: 0=Unlimited, 1=Time(ms), 2=Count
#[no_mangle]
pub extern "C" fn redlite_history_enable_global(
    db: *mut RedliteDb,
    retention_type: c_int,
    retention_value: i64,
) -> c_int {
    clear_error();
    let handle = get_db_ret!(db, -1);

    let retention = match retention_type {
        0 => redlite::RetentionType::Unlimited,
        1 => redlite::RetentionType::Time(retention_value),
        2 => redlite::RetentionType::Count(retention_value),
        _ => {
            set_error("Invalid retention type (0=Unlimited, 1=Time, 2=Count)".to_string());
            return -1;
        }
    };

    let guard = handle.db.lock().unwrap();
    match guard.history_enable_global(retention) {
        Ok(()) => 0,
        Err(e) => {
            set_error(format!("HISTORY ENABLE GLOBAL failed: {}", e));
            -1
        }
    }
}

/// HISTORY ENABLE DATABASE db_num [retention_type] [retention_value]
/// Enable history tracking for a specific database.
#[no_mangle]
pub extern "C" fn redlite_history_enable_database(
    db: *mut RedliteDb,
    db_num: c_int,
    retention_type: c_int,
    retention_value: i64,
) -> c_int {
    clear_error();
    let handle = get_db_ret!(db, -1);

    let retention = match retention_type {
        0 => redlite::RetentionType::Unlimited,
        1 => redlite::RetentionType::Time(retention_value),
        2 => redlite::RetentionType::Count(retention_value),
        _ => {
            set_error("Invalid retention type".to_string());
            return -1;
        }
    };

    let guard = handle.db.lock().unwrap();
    match guard.history_enable_database(db_num, retention) {
        Ok(()) => 0,
        Err(e) => {
            set_error(format!("HISTORY ENABLE DATABASE failed: {}", e));
            -1
        }
    }
}

/// HISTORY ENABLE KEY key [retention_type] [retention_value]
/// Enable history tracking for a specific key.
#[no_mangle]
pub extern "C" fn redlite_history_enable_key(
    db: *mut RedliteDb,
    key: *const c_char,
    retention_type: c_int,
    retention_value: i64,
) -> c_int {
    clear_error();
    let handle = get_db_ret!(db, -1);

    let key_str = match cstr_to_str(key) {
        Ok(s) => s,
        Err(e) => {
            set_error(e);
            return -1;
        }
    };

    let retention = match retention_type {
        0 => redlite::RetentionType::Unlimited,
        1 => redlite::RetentionType::Time(retention_value),
        2 => redlite::RetentionType::Count(retention_value),
        _ => {
            set_error("Invalid retention type".to_string());
            return -1;
        }
    };

    let guard = handle.db.lock().unwrap();
    match guard.history_enable_key(key_str, retention) {
        Ok(()) => 0,
        Err(e) => {
            set_error(format!("HISTORY ENABLE KEY failed: {}", e));
            -1
        }
    }
}

/// HISTORY DISABLE GLOBAL
/// Disable history tracking globally.
#[no_mangle]
pub extern "C" fn redlite_history_disable_global(db: *mut RedliteDb) -> c_int {
    clear_error();
    let handle = get_db_ret!(db, -1);

    let guard = handle.db.lock().unwrap();
    match guard.history_disable_global() {
        Ok(()) => 0,
        Err(e) => {
            set_error(format!("HISTORY DISABLE GLOBAL failed: {}", e));
            -1
        }
    }
}

/// HISTORY DISABLE DATABASE db_num
/// Disable history tracking for a specific database.
#[no_mangle]
pub extern "C" fn redlite_history_disable_database(db: *mut RedliteDb, db_num: c_int) -> c_int {
    clear_error();
    let handle = get_db_ret!(db, -1);

    let guard = handle.db.lock().unwrap();
    match guard.history_disable_database(db_num) {
        Ok(()) => 0,
        Err(e) => {
            set_error(format!("HISTORY DISABLE DATABASE failed: {}", e));
            -1
        }
    }
}

/// HISTORY DISABLE KEY key
/// Disable history tracking for a specific key.
#[no_mangle]
pub extern "C" fn redlite_history_disable_key(db: *mut RedliteDb, key: *const c_char) -> c_int {
    clear_error();
    let handle = get_db_ret!(db, -1);

    let key_str = match cstr_to_str(key) {
        Ok(s) => s,
        Err(e) => {
            set_error(e);
            return -1;
        }
    };

    let guard = handle.db.lock().unwrap();
    match guard.history_disable_key(key_str) {
        Ok(()) => 0,
        Err(e) => {
            set_error(format!("HISTORY DISABLE KEY failed: {}", e));
            -1
        }
    }
}

/// Check if history is enabled for a key
/// Returns: 1 if enabled, 0 if disabled, -1 on error
#[no_mangle]
pub extern "C" fn redlite_is_history_enabled(db: *mut RedliteDb, key: *const c_char) -> c_int {
    clear_error();
    let handle = get_db_ret!(db, -1);

    let key_str = match cstr_to_str(key) {
        Ok(s) => s,
        Err(e) => {
            set_error(e);
            return -1;
        }
    };

    let guard = handle.db.lock().unwrap();
    match guard.is_history_enabled(key_str) {
        Ok(true) => 1,
        Ok(false) => 0,
        Err(e) => {
            set_error(format!("IS_HISTORY_ENABLED failed: {}", e));
            -1
        }
    }
}

// =============================================================================
// FTS Enable/Disable Commands
// =============================================================================

/// FTS ENABLE GLOBAL
/// Enable full-text search indexing globally.
#[no_mangle]
pub extern "C" fn redlite_fts_enable_global(db: *mut RedliteDb) -> c_int {
    clear_error();
    let handle = get_db_ret!(db, -1);

    let guard = handle.db.lock().unwrap();
    match guard.fts_enable_global() {
        Ok(()) => 0,
        Err(e) => {
            set_error(format!("FTS ENABLE GLOBAL failed: {}", e));
            -1
        }
    }
}

/// FTS ENABLE DATABASE db_num
/// Enable full-text search indexing for a specific database.
#[no_mangle]
pub extern "C" fn redlite_fts_enable_database(db: *mut RedliteDb, db_num: c_int) -> c_int {
    clear_error();
    let handle = get_db_ret!(db, -1);

    let guard = handle.db.lock().unwrap();
    match guard.fts_enable_database(db_num) {
        Ok(()) => 0,
        Err(e) => {
            set_error(format!("FTS ENABLE DATABASE failed: {}", e));
            -1
        }
    }
}

/// FTS ENABLE PATTERN pattern
/// Enable full-text search indexing for keys matching a pattern.
#[no_mangle]
pub extern "C" fn redlite_fts_enable_pattern(db: *mut RedliteDb, pattern: *const c_char) -> c_int {
    clear_error();
    let handle = get_db_ret!(db, -1);

    let pattern_str = match cstr_to_str(pattern) {
        Ok(s) => s,
        Err(e) => {
            set_error(e);
            return -1;
        }
    };

    let guard = handle.db.lock().unwrap();
    match guard.fts_enable_pattern(pattern_str) {
        Ok(()) => 0,
        Err(e) => {
            set_error(format!("FTS ENABLE PATTERN failed: {}", e));
            -1
        }
    }
}

/// FTS ENABLE KEY key
/// Enable full-text search indexing for a specific key.
#[no_mangle]
pub extern "C" fn redlite_fts_enable_key(db: *mut RedliteDb, key: *const c_char) -> c_int {
    clear_error();
    let handle = get_db_ret!(db, -1);

    let key_str = match cstr_to_str(key) {
        Ok(s) => s,
        Err(e) => {
            set_error(e);
            return -1;
        }
    };

    let guard = handle.db.lock().unwrap();
    match guard.fts_enable_key(key_str) {
        Ok(()) => 0,
        Err(e) => {
            set_error(format!("FTS ENABLE KEY failed: {}", e));
            -1
        }
    }
}

/// FTS DISABLE GLOBAL
/// Disable full-text search indexing globally.
#[no_mangle]
pub extern "C" fn redlite_fts_disable_global(db: *mut RedliteDb) -> c_int {
    clear_error();
    let handle = get_db_ret!(db, -1);

    let guard = handle.db.lock().unwrap();
    match guard.fts_disable_global() {
        Ok(()) => 0,
        Err(e) => {
            set_error(format!("FTS DISABLE GLOBAL failed: {}", e));
            -1
        }
    }
}

/// FTS DISABLE DATABASE db_num
/// Disable full-text search indexing for a specific database.
#[no_mangle]
pub extern "C" fn redlite_fts_disable_database(db: *mut RedliteDb, db_num: c_int) -> c_int {
    clear_error();
    let handle = get_db_ret!(db, -1);

    let guard = handle.db.lock().unwrap();
    match guard.fts_disable_database(db_num) {
        Ok(()) => 0,
        Err(e) => {
            set_error(format!("FTS DISABLE DATABASE failed: {}", e));
            -1
        }
    }
}

/// FTS DISABLE PATTERN pattern
/// Disable full-text search indexing for keys matching a pattern.
#[no_mangle]
pub extern "C" fn redlite_fts_disable_pattern(db: *mut RedliteDb, pattern: *const c_char) -> c_int {
    clear_error();
    let handle = get_db_ret!(db, -1);

    let pattern_str = match cstr_to_str(pattern) {
        Ok(s) => s,
        Err(e) => {
            set_error(e);
            return -1;
        }
    };

    let guard = handle.db.lock().unwrap();
    match guard.fts_disable_pattern(pattern_str) {
        Ok(()) => 0,
        Err(e) => {
            set_error(format!("FTS DISABLE PATTERN failed: {}", e));
            -1
        }
    }
}

/// FTS DISABLE KEY key
/// Disable full-text search indexing for a specific key.
#[no_mangle]
pub extern "C" fn redlite_fts_disable_key(db: *mut RedliteDb, key: *const c_char) -> c_int {
    clear_error();
    let handle = get_db_ret!(db, -1);

    let key_str = match cstr_to_str(key) {
        Ok(s) => s,
        Err(e) => {
            set_error(e);
            return -1;
        }
    };

    let guard = handle.db.lock().unwrap();
    match guard.fts_disable_key(key_str) {
        Ok(()) => 0,
        Err(e) => {
            set_error(format!("FTS DISABLE KEY failed: {}", e));
            -1
        }
    }
}

/// Check if FTS is enabled for a key
/// Returns: 1 if enabled, 0 if disabled, -1 on error
#[no_mangle]
pub extern "C" fn redlite_is_fts_enabled(db: *mut RedliteDb, key: *const c_char) -> c_int {
    clear_error();
    let handle = get_db_ret!(db, -1);

    let key_str = match cstr_to_str(key) {
        Ok(s) => s,
        Err(e) => {
            set_error(e);
            return -1;
        }
    };

    let guard = handle.db.lock().unwrap();
    match guard.is_fts_enabled(key_str) {
        Ok(true) => 1,
        Ok(false) => 0,
        Err(e) => {
            set_error(format!("IS_FTS_ENABLED failed: {}", e));
            -1
        }
    }
}

// =============================================================================
// KeyInfo Command
// =============================================================================

/// KeyInfo result struct
#[repr(C)]
pub struct RedliteKeyInfo {
    /// Key type as string (string, list, set, hash, zset, stream, none)
    pub key_type: *mut c_char,
    /// TTL in seconds (-1 if no expiry, -2 if key doesn't exist)
    pub ttl: i64,
    /// Created at timestamp in milliseconds
    pub created_at: i64,
    /// Updated at timestamp in milliseconds
    pub updated_at: i64,
    /// Whether the struct is valid (1) or null (0)
    pub valid: c_int,
}

/// Free a KeyInfo result
#[no_mangle]
pub extern "C" fn redlite_free_keyinfo(info: RedliteKeyInfo) {
    if !info.key_type.is_null() {
        unsafe { drop(CString::from_raw(info.key_type)); }
    }
}

/// KEYINFO key
/// Get detailed information about a key.
#[no_mangle]
pub extern "C" fn redlite_keyinfo(db: *mut RedliteDb, key: *const c_char) -> RedliteKeyInfo {
    clear_error();

    let null_result = RedliteKeyInfo {
        key_type: ptr::null_mut(),
        ttl: -2,
        created_at: 0,
        updated_at: 0,
        valid: 0,
    };

    let handle = match unsafe { db.as_ref() } {
        Some(h) => h,
        None => {
            set_error("Invalid database handle".to_string());
            return null_result;
        }
    };

    let key_str = match cstr_to_str(key) {
        Ok(s) => s,
        Err(e) => {
            set_error(e);
            return null_result;
        }
    };

    let guard = handle.db.lock().unwrap();
    match guard.keyinfo(key_str) {
        Ok(Some(info)) => {
            let type_str = match info.key_type {
                redlite::KeyType::String => "string",
                redlite::KeyType::List => "list",
                redlite::KeyType::Set => "set",
                redlite::KeyType::Hash => "hash",
                redlite::KeyType::ZSet => "zset",
                redlite::KeyType::Stream => "stream",
                redlite::KeyType::Json => "ReJSON-RL",
            };
            RedliteKeyInfo {
                key_type: CString::new(type_str).unwrap().into_raw(),
                ttl: info.ttl,
                created_at: info.created_at,
                updated_at: info.updated_at,
                valid: 1,
            }
        }
        Ok(None) => null_result,
        Err(e) => {
            set_error(format!("KEYINFO failed: {}", e));
            null_result
        }
    }
}
