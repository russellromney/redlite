//! JNI bindings for Redlite embedded database.
//!
//! This crate provides JNI bindings for use by both Kotlin and Java SDKs.

use jni::objects::{JByteArray, JClass, JObject, JObjectArray, JString, JValue};
use jni::sys::{jboolean, jdouble, jint, jlong, JNI_TRUE, JNI_FALSE};
use jni::JNIEnv;
use redlite::Db as RedliteDb;
use std::time::Duration;

// Helper to convert Rust errors to JNI exceptions
fn throw_exception(env: &mut JNIEnv, message: &str) {
    let _ = env.throw_new("com/redlite/RedliteException", message);
}

// Helper to get string from JString
fn get_string(env: &mut JNIEnv, s: &JString) -> Result<String, String> {
    env.get_string(s)
        .map(|s| s.into())
        .map_err(|e| e.to_string())
}

// Helper to convert byte array to Vec<u8>
fn get_bytes(env: &mut JNIEnv, arr: &JByteArray) -> Result<Vec<u8>, String> {
    let len = env.get_array_length(arr).map_err(|e| e.to_string())? as usize;
    let mut buf = vec![0i8; len];
    env.get_byte_array_region(arr, 0, &mut buf).map_err(|e| e.to_string())?;
    Ok(buf.into_iter().map(|b| b as u8).collect())
}

// Helper to create byte array from Vec<u8>
fn create_byte_array<'a>(env: &mut JNIEnv<'a>, data: &[u8]) -> Result<JByteArray<'a>, String> {
    let arr = env.new_byte_array(data.len() as i32).map_err(|e| e.to_string())?;
    let signed: Vec<i8> = data.iter().map(|&b| b as i8).collect();
    env.set_byte_array_region(&arr, 0, &signed).map_err(|e| e.to_string())?;
    Ok(arr)
}

// ============================================================================
// Database Lifecycle
// ============================================================================

#[no_mangle]
pub extern "system" fn Java_com_redlite_EmbeddedDb_nativeOpenMemory(
    _env: JNIEnv,
    _class: JClass,
) -> jlong {
    match RedliteDb::open_memory() {
        Ok(db) => Box::into_raw(Box::new(db)) as jlong,
        Err(_) => 0,
    }
}

#[no_mangle]
pub extern "system" fn Java_com_redlite_EmbeddedDb_nativeOpenWithCache(
    mut env: JNIEnv,
    _class: JClass,
    path: JString,
    cache_mb: jint,
) -> jlong {
    let path_str = match get_string(&mut env, &path) {
        Ok(s) => s,
        Err(_) => return 0,
    };

    match RedliteDb::open_with_cache(&path_str, cache_mb as i64) {
        Ok(db) => Box::into_raw(Box::new(db)) as jlong,
        Err(_) => 0,
    }
}

#[no_mangle]
pub extern "system" fn Java_com_redlite_EmbeddedDb_nativeClose(
    _env: JNIEnv,
    _class: JClass,
    ptr: jlong,
) {
    if ptr != 0 {
        unsafe {
            let _ = Box::from_raw(ptr as *mut RedliteDb);
        }
    }
}

// ============================================================================
// String Commands
// ============================================================================

#[no_mangle]
pub extern "system" fn Java_com_redlite_EmbeddedDb_nativeGet<'a>(
    mut env: JNIEnv<'a>,
    _class: JClass,
    ptr: jlong,
    key: JString,
) -> JByteArray<'a> {
    let db = unsafe { &*(ptr as *const RedliteDb) };
    let key_str = match get_string(&mut env, &key) {
        Ok(s) => s,
        Err(e) => {
            throw_exception(&mut env, &e);
            return JByteArray::default();
        }
    };

    match db.get(&key_str) {
        Ok(Some(value)) => {
            match create_byte_array(&mut env, &value) {
                Ok(arr) => arr,
                Err(e) => {
                    throw_exception(&mut env, &e);
                    JByteArray::default()
                }
            }
        }
        Ok(None) => JByteArray::default(),
        Err(e) => {
            throw_exception(&mut env, &e.to_string());
            JByteArray::default()
        }
    }
}

#[no_mangle]
pub extern "system" fn Java_com_redlite_EmbeddedDb_nativeSet(
    mut env: JNIEnv,
    _class: JClass,
    ptr: jlong,
    key: JString,
    value: JByteArray,
    ttl_seconds: jlong,
) -> jboolean {
    let db = unsafe { &*(ptr as *const RedliteDb) };

    let key_str = match get_string(&mut env, &key) {
        Ok(s) => s,
        Err(e) => {
            throw_exception(&mut env, &e);
            return JNI_FALSE;
        }
    };

    let value_bytes = match get_bytes(&mut env, &value) {
        Ok(b) => b,
        Err(e) => {
            throw_exception(&mut env, &e);
            return JNI_FALSE;
        }
    };

    let ttl = if ttl_seconds > 0 {
        Some(Duration::from_secs(ttl_seconds as u64))
    } else {
        None
    };

    match db.set(&key_str, &value_bytes, ttl) {
        Ok(_) => JNI_TRUE,
        Err(e) => {
            throw_exception(&mut env, &e.to_string());
            JNI_FALSE
        }
    }
}

#[no_mangle]
pub extern "system" fn Java_com_redlite_EmbeddedDb_nativeSetOpts(
    mut env: JNIEnv,
    _class: JClass,
    ptr: jlong,
    key: JString,
    value: JByteArray,
    ex: jlong,
    px: jlong,
    nx: jboolean,
    xx: jboolean,
) -> jboolean {
    let db = unsafe { &*(ptr as *const RedliteDb) };

    let key_str = match get_string(&mut env, &key) {
        Ok(s) => s,
        Err(e) => {
            throw_exception(&mut env, &e);
            return JNI_FALSE;
        }
    };

    let value_bytes = match get_bytes(&mut env, &value) {
        Ok(b) => b,
        Err(e) => {
            throw_exception(&mut env, &e);
            return JNI_FALSE;
        }
    };

    let ttl = if ex > 0 {
        Some(Duration::from_secs(ex as u64))
    } else if px > 0 {
        Some(Duration::from_millis(px as u64))
    } else {
        None
    };

    let opts = redlite::SetOptions {
        ttl,
        nx: nx == JNI_TRUE,
        xx: xx == JNI_TRUE,
    };

    match db.set_opts(&key_str, &value_bytes, opts) {
        Ok(true) => JNI_TRUE,
        Ok(false) => JNI_FALSE,
        Err(e) => {
            throw_exception(&mut env, &e.to_string());
            JNI_FALSE
        }
    }
}

#[no_mangle]
pub extern "system" fn Java_com_redlite_EmbeddedDb_nativeSetex(
    mut env: JNIEnv,
    _class: JClass,
    ptr: jlong,
    key: JString,
    seconds: jlong,
    value: JByteArray,
) -> jboolean {
    let db = unsafe { &*(ptr as *const RedliteDb) };

    let key_str = match get_string(&mut env, &key) {
        Ok(s) => s,
        Err(e) => {
            throw_exception(&mut env, &e);
            return JNI_FALSE;
        }
    };

    let value_bytes = match get_bytes(&mut env, &value) {
        Ok(b) => b,
        Err(e) => {
            throw_exception(&mut env, &e);
            return JNI_FALSE;
        }
    };

    match db.setex(&key_str, seconds, &value_bytes) {
        Ok(_) => JNI_TRUE,
        Err(e) => {
            throw_exception(&mut env, &e.to_string());
            JNI_FALSE
        }
    }
}

#[no_mangle]
pub extern "system" fn Java_com_redlite_EmbeddedDb_nativeIncr(
    mut env: JNIEnv,
    _class: JClass,
    ptr: jlong,
    key: JString,
) -> jlong {
    let db = unsafe { &*(ptr as *const RedliteDb) };

    let key_str = match get_string(&mut env, &key) {
        Ok(s) => s,
        Err(e) => {
            throw_exception(&mut env, &e);
            return 0;
        }
    };

    match db.incr(&key_str) {
        Ok(v) => v,
        Err(e) => {
            throw_exception(&mut env, &e.to_string());
            0
        }
    }
}

#[no_mangle]
pub extern "system" fn Java_com_redlite_EmbeddedDb_nativeDecr(
    mut env: JNIEnv,
    _class: JClass,
    ptr: jlong,
    key: JString,
) -> jlong {
    let db = unsafe { &*(ptr as *const RedliteDb) };

    let key_str = match get_string(&mut env, &key) {
        Ok(s) => s,
        Err(e) => {
            throw_exception(&mut env, &e);
            return 0;
        }
    };

    match db.decr(&key_str) {
        Ok(v) => v,
        Err(e) => {
            throw_exception(&mut env, &e.to_string());
            0
        }
    }
}

#[no_mangle]
pub extern "system" fn Java_com_redlite_EmbeddedDb_nativeIncrby(
    mut env: JNIEnv,
    _class: JClass,
    ptr: jlong,
    key: JString,
    amount: jlong,
) -> jlong {
    let db = unsafe { &*(ptr as *const RedliteDb) };

    let key_str = match get_string(&mut env, &key) {
        Ok(s) => s,
        Err(e) => {
            throw_exception(&mut env, &e);
            return 0;
        }
    };

    match db.incrby(&key_str, amount) {
        Ok(v) => v,
        Err(e) => {
            throw_exception(&mut env, &e.to_string());
            0
        }
    }
}

// ============================================================================
// Key Commands
// ============================================================================

#[no_mangle]
pub extern "system" fn Java_com_redlite_EmbeddedDb_nativeDelete(
    mut env: JNIEnv,
    _class: JClass,
    ptr: jlong,
    keys: JObjectArray,
) -> jlong {
    let db = unsafe { &*(ptr as *const RedliteDb) };

    let len = match env.get_array_length(&keys) {
        Ok(l) => l as usize,
        Err(e) => {
            throw_exception(&mut env, &e.to_string());
            return 0;
        }
    };

    let mut key_strings = Vec::with_capacity(len);
    for i in 0..len {
        let obj = match env.get_object_array_element(&keys, i as i32) {
            Ok(o) => o,
            Err(e) => {
                throw_exception(&mut env, &e.to_string());
                return 0;
            }
        };
        let jstr: JString = obj.into();
        match get_string(&mut env, &jstr) {
            Ok(s) => key_strings.push(s),
            Err(e) => {
                throw_exception(&mut env, &e);
                return 0;
            }
        }
    }

    let key_refs: Vec<&str> = key_strings.iter().map(|s| s.as_str()).collect();
    match db.del(&key_refs) {
        Ok(n) => n as jlong,
        Err(e) => {
            throw_exception(&mut env, &e.to_string());
            0
        }
    }
}

#[no_mangle]
pub extern "system" fn Java_com_redlite_EmbeddedDb_nativeExists(
    mut env: JNIEnv,
    _class: JClass,
    ptr: jlong,
    keys: JObjectArray,
) -> jlong {
    let db = unsafe { &*(ptr as *const RedliteDb) };

    let len = match env.get_array_length(&keys) {
        Ok(l) => l as usize,
        Err(e) => {
            throw_exception(&mut env, &e.to_string());
            return 0;
        }
    };

    let mut key_strings = Vec::with_capacity(len);
    for i in 0..len {
        let obj = match env.get_object_array_element(&keys, i as i32) {
            Ok(o) => o,
            Err(e) => {
                throw_exception(&mut env, &e.to_string());
                return 0;
            }
        };
        let jstr: JString = obj.into();
        match get_string(&mut env, &jstr) {
            Ok(s) => key_strings.push(s),
            Err(e) => {
                throw_exception(&mut env, &e);
                return 0;
            }
        }
    }

    let key_refs: Vec<&str> = key_strings.iter().map(|s| s.as_str()).collect();
    match db.exists(&key_refs) {
        Ok(n) => n as jlong,
        Err(e) => {
            throw_exception(&mut env, &e.to_string());
            0
        }
    }
}

#[no_mangle]
pub extern "system" fn Java_com_redlite_EmbeddedDb_nativeTtl(
    mut env: JNIEnv,
    _class: JClass,
    ptr: jlong,
    key: JString,
) -> jlong {
    let db = unsafe { &*(ptr as *const RedliteDb) };

    let key_str = match get_string(&mut env, &key) {
        Ok(s) => s,
        Err(e) => {
            throw_exception(&mut env, &e);
            return -2;
        }
    };

    match db.ttl(&key_str) {
        Ok(v) => v,
        Err(e) => {
            throw_exception(&mut env, &e.to_string());
            -2
        }
    }
}

#[no_mangle]
pub extern "system" fn Java_com_redlite_EmbeddedDb_nativeExpire(
    mut env: JNIEnv,
    _class: JClass,
    ptr: jlong,
    key: JString,
    seconds: jlong,
) -> jboolean {
    let db = unsafe { &*(ptr as *const RedliteDb) };

    let key_str = match get_string(&mut env, &key) {
        Ok(s) => s,
        Err(e) => {
            throw_exception(&mut env, &e);
            return JNI_FALSE;
        }
    };

    match db.expire(&key_str, seconds) {
        Ok(true) => JNI_TRUE,
        Ok(false) => JNI_FALSE,
        Err(e) => {
            throw_exception(&mut env, &e.to_string());
            JNI_FALSE
        }
    }
}

#[no_mangle]
pub extern "system" fn Java_com_redlite_EmbeddedDb_nativeFlushdb(
    mut env: JNIEnv,
    _class: JClass,
    ptr: jlong,
) -> jboolean {
    let db = unsafe { &*(ptr as *const RedliteDb) };

    match db.flushdb() {
        Ok(_) => JNI_TRUE,
        Err(e) => {
            throw_exception(&mut env, &e.to_string());
            JNI_FALSE
        }
    }
}

#[no_mangle]
pub extern "system" fn Java_com_redlite_EmbeddedDb_nativeDbsize(
    mut env: JNIEnv,
    _class: JClass,
    ptr: jlong,
) -> jlong {
    let db = unsafe { &*(ptr as *const RedliteDb) };

    match db.dbsize() {
        Ok(n) => n as jlong,
        Err(e) => {
            throw_exception(&mut env, &e.to_string());
            0
        }
    }
}

// ============================================================================
// Server Commands
// ============================================================================

#[no_mangle]
pub extern "system" fn Java_com_redlite_EmbeddedDb_nativeVacuum(
    mut env: JNIEnv,
    _class: JClass,
    ptr: jlong,
) -> jlong {
    let db = unsafe { &*(ptr as *const RedliteDb) };

    match db.vacuum() {
        Ok(n) => n as jlong,
        Err(e) => {
            throw_exception(&mut env, &e.to_string());
            0
        }
    }
}

#[no_mangle]
pub extern "system" fn Java_com_redlite_EmbeddedDb_nativePing<'a>(
    mut env: JNIEnv<'a>,
    _class: JClass,
    _ptr: jlong,
) -> JString<'a> {
    match env.new_string("PONG") {
        Ok(s) => s,
        Err(_) => JString::default(),
    }
}

// Note: Additional methods for Hash, List, Set, Sorted Set, etc. would follow
// the same pattern. This is a representative subset demonstrating the JNI
// binding approach.
