//! WASM tests for Redlite
//!
//! Run with: wasm-pack test --headless --chrome

#![cfg(target_arch = "wasm32")]

use wasm_bindgen_test::*;

wasm_bindgen_test_configure!(run_in_browser);

use redlite_wasm::RedliteWasm;

#[wasm_bindgen_test]
fn test_create_database() {
    let db = RedliteWasm::new().expect("Failed to create database");
    assert_eq!(db.dbsize().unwrap(), 0);
}

#[wasm_bindgen_test]
fn test_string_set_get() {
    let mut db = RedliteWasm::new().unwrap();

    // Set a value
    let result = db.set("foo", b"bar", None).unwrap();
    assert!(result);

    // Get the value
    let value = db.get("foo").unwrap();
    assert_eq!(value, Some(b"bar".to_vec()));
}

#[wasm_bindgen_test]
fn test_string_incr() {
    let mut db = RedliteWasm::new().unwrap();

    // INCR on non-existent key starts at 0
    let val = db.incr("counter").unwrap();
    assert_eq!(val, 1);

    // INCR again
    let val = db.incr("counter").unwrap();
    assert_eq!(val, 2);

    // INCRBY
    let val = db.incrby("counter", 10).unwrap();
    assert_eq!(val, 12);
}

#[wasm_bindgen_test]
fn test_key_exists() {
    let mut db = RedliteWasm::new().unwrap();

    db.set("key1", b"value1", None).unwrap();
    db.set("key2", b"value2", None).unwrap();

    let count = db.exists(vec!["key1".to_string(), "key2".to_string(), "key3".to_string()]).unwrap();
    assert_eq!(count, 2);
}

#[wasm_bindgen_test]
fn test_key_del() {
    let mut db = RedliteWasm::new().unwrap();

    db.set("key1", b"value1", None).unwrap();
    db.set("key2", b"value2", None).unwrap();

    let deleted = db.del(vec!["key1".to_string(), "key3".to_string()]).unwrap();
    assert_eq!(deleted, 1);

    assert!(db.get("key1").unwrap().is_none());
    assert!(db.get("key2").unwrap().is_some());
}

#[wasm_bindgen_test]
fn test_hash_operations() {
    let mut db = RedliteWasm::new().unwrap();

    // HSET
    db.hset("hash", "field1", b"value1").unwrap();
    db.hset("hash", "field2", b"value2").unwrap();

    // HGET
    let val = db.hget("hash", "field1").unwrap();
    assert_eq!(val, Some(b"value1".to_vec()));

    // HLEN
    assert_eq!(db.hlen("hash").unwrap(), 2);

    // HEXISTS
    assert!(db.hexists("hash", "field1").unwrap());
    assert!(!db.hexists("hash", "field3").unwrap());

    // HKEYS
    let keys = db.hkeys("hash").unwrap();
    assert_eq!(keys.len(), 2);
}

#[wasm_bindgen_test]
fn test_list_operations() {
    let mut db = RedliteWasm::new().unwrap();

    // RPUSH
    db.rpush("list", vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()]).unwrap();

    // LLEN
    assert_eq!(db.llen("list").unwrap(), 3);

    // LRANGE
    let range = db.lrange("list", 0, -1).unwrap();
    assert_eq!(range.len(), 3);
    assert_eq!(range[0], b"a".to_vec());
    assert_eq!(range[1], b"b".to_vec());
    assert_eq!(range[2], b"c".to_vec());

    // LPOP
    let popped = db.lpop("list", Some(1)).unwrap();
    assert_eq!(popped.len(), 1);
    assert_eq!(popped[0], b"a".to_vec());
    assert_eq!(db.llen("list").unwrap(), 2);
}

#[wasm_bindgen_test]
fn test_set_operations() {
    let mut db = RedliteWasm::new().unwrap();

    // SADD
    let added = db.sadd("set", vec![b"a".to_vec(), b"b".to_vec(), b"a".to_vec()]).unwrap();
    assert_eq!(added, 2); // 'a' is duplicate

    // SCARD
    assert_eq!(db.scard("set").unwrap(), 2);

    // SISMEMBER
    assert!(db.sismember("set", b"a").unwrap());
    assert!(!db.sismember("set", b"c").unwrap());

    // SREM
    let removed = db.srem("set", vec![b"a".to_vec()]).unwrap();
    assert_eq!(removed, 1);
    assert_eq!(db.scard("set").unwrap(), 1);
}

#[wasm_bindgen_test]
fn test_database_selection() {
    let mut db = RedliteWasm::new().unwrap();

    // Set in db 0
    db.set("key", b"db0", None).unwrap();

    // Switch to db 1
    db.select(1).unwrap();
    assert!(db.get("key").unwrap().is_none());

    db.set("key", b"db1", None).unwrap();

    // Switch back to db 0
    db.select(0).unwrap();
    let val = db.get("key").unwrap();
    assert_eq!(val, Some(b"db0".to_vec()));
}
