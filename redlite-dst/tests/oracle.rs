//! Oracle tests for Redlite - Compare against real Redis
//!
//! These tests verify that Redlite behaves identically to Redis for ALL supported commands.
//! Requires a Redis instance running at localhost:6379.
//!
//! Run with:
//!   # Start Redis (native or via Docker)
//!   redis-server &
//!   # Or: docker run -d -p 6379:6379 redis
//!
//!   # Run tests sequentially (required - tests share Redis state)
//!   cargo test --test oracle -- --test-threads=1
//!
//! Set REDIS_URL environment variable to use a different Redis instance:
//!   REDIS_URL=redis://myhost:6380 cargo test --test oracle -- --test-threads=1
//!
//! Note: Tests skip gracefully if Redis is not available.

use rand::Rng;
use rand_chacha::ChaCha8Rng;
use rand::SeedableRng;
use redis::Commands;
use redlite::{Db, ZMember, ListDirection, SetOptions, GetExOption};
use std::collections::HashMap;

/// Check if Redis is available, skip tests if not
fn get_redis_connection() -> Option<redis::Connection> {
    let redis_url = std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://localhost:6379".to_string());
    let client = redis::Client::open(redis_url).ok()?;
    client.get_connection().ok()
}

/// Helper macro to skip test if Redis is not available
macro_rules! require_redis {
    () => {
        match get_redis_connection() {
            Some(conn) => conn,
            None => {
                eprintln!("Skipping test: Redis not available at localhost:6379");
                eprintln!("Start Redis: docker run -d -p 6379:6379 redis");
                return;
            }
        }
    };
}

// ============================================================================
// STRING ORACLE TESTS - Basic
// ============================================================================

#[test]
fn oracle_strings_set_get() {
    let mut redis = require_redis!();
    let redlite = Db::open_memory().unwrap();
    let _: () = redis::cmd("FLUSHDB").query(&mut redis).unwrap();

    let test_cases = vec![
        ("simple", b"hello".to_vec()),
        ("empty", vec![]),
        ("binary", vec![0, 1, 2, 255, 128]),
        ("unicode", "hello\u{1F600}world".as_bytes().to_vec()),
        ("spaces", b"hello world".to_vec()),
        ("newlines", b"line1\nline2\r\nline3".to_vec()),
    ];

    for (key, value) in test_cases {
        let key = format!("str:{}", key);
        redlite.set(&key, &value, None).unwrap();
        let _: () = redis.set(&key, &value).unwrap();

        let redlite_val = redlite.get(&key).unwrap();
        let redis_val: Option<Vec<u8>> = redis.get(&key).unwrap();
        assert_eq!(redlite_val, redis_val, "Mismatch for key: {}", key);
    }
}

#[test]
fn oracle_strings_incr_decr() {
    let mut redis = require_redis!();
    let redlite = Db::open_memory().unwrap();
    let _: () = redis::cmd("FLUSHDB").query(&mut redis).unwrap();

    // INCR on new key
    let r1 = redlite.incr("counter").unwrap();
    let r2: i64 = redis.incr("counter", 1i64).unwrap();
    assert_eq!(r1, r2);

    // Multiple INCRs
    for _ in 0..10 {
        let r1 = redlite.incr("counter").unwrap();
        let r2: i64 = redis.incr("counter", 1i64).unwrap();
        assert_eq!(r1, r2);
    }

    // DECR
    let r1 = redlite.decr("counter").unwrap();
    let r2: i64 = redis.decr("counter", 1i64).unwrap();
    assert_eq!(r1, r2);

    // INCRBY
    let r1 = redlite.incrby("counter", 5).unwrap();
    let r2: i64 = redis.incr("counter", 5).unwrap();
    assert_eq!(r1, r2);

    // DECRBY
    let r1 = redlite.decrby("counter", 3).unwrap();
    let r2: i64 = redis.decr("counter", 3).unwrap();
    assert_eq!(r1, r2);
}

#[test]
fn oracle_strings_incrbyfloat() {
    let mut redis = require_redis!();
    let redlite = Db::open_memory().unwrap();
    let _: () = redis::cmd("FLUSHDB").query(&mut redis).unwrap();

    // INCRBYFLOAT on new key
    let r1: f64 = redlite.incrbyfloat("float_counter", 1.5).unwrap().parse().unwrap();
    let r2: f64 = redis::cmd("INCRBYFLOAT").arg("float_counter").arg(1.5).query(&mut redis).unwrap();
    assert!((r1 - r2).abs() < 1e-10);

    // INCRBYFLOAT again
    let r1: f64 = redlite.incrbyfloat("float_counter", 2.5).unwrap().parse().unwrap();
    let r2: f64 = redis::cmd("INCRBYFLOAT").arg("float_counter").arg(2.5).query(&mut redis).unwrap();
    assert!((r1 - r2).abs() < 1e-10);

    // INCRBYFLOAT negative
    let r1: f64 = redlite.incrbyfloat("float_counter", -1.0).unwrap().parse().unwrap();
    let r2: f64 = redis::cmd("INCRBYFLOAT").arg("float_counter").arg(-1.0).query(&mut redis).unwrap();
    assert!((r1 - r2).abs() < 1e-10);
}

#[test]
fn oracle_strings_append() {
    let mut redis = require_redis!();
    let redlite = Db::open_memory().unwrap();
    let _: () = redis::cmd("FLUSHDB").query(&mut redis).unwrap();

    // APPEND to non-existent key
    let r1 = redlite.append("app", b"hello").unwrap();
    let r2: usize = redis.append("app", "hello").unwrap();
    assert_eq!(r1 as usize, r2);

    // APPEND to existing key
    let r1 = redlite.append("app", b" world").unwrap();
    let r2: usize = redis.append("app", " world").unwrap();
    assert_eq!(r1 as usize, r2);

    // Verify final value
    let v1 = redlite.get("app").unwrap();
    let v2: Option<Vec<u8>> = redis.get("app").unwrap();
    assert_eq!(v1, v2);
}

#[test]
fn oracle_strings_strlen() {
    let mut redis = require_redis!();
    let redlite = Db::open_memory().unwrap();
    let _: () = redis::cmd("FLUSHDB").query(&mut redis).unwrap();

    // STRLEN on non-existent key
    let r1 = redlite.strlen("key").unwrap();
    let r2: i64 = redis.strlen("key").unwrap();
    assert_eq!(r1, r2);

    // Create key and check STRLEN
    redlite.set("key", b"hello world", None).unwrap();
    let _: () = redis.set("key", "hello world").unwrap();

    let r1 = redlite.strlen("key").unwrap();
    let r2: i64 = redis.strlen("key").unwrap();
    assert_eq!(r1, r2);
}

#[test]
fn oracle_strings_getrange() {
    let mut redis = require_redis!();
    let redlite = Db::open_memory().unwrap();
    let _: () = redis::cmd("FLUSHDB").query(&mut redis).unwrap();

    redlite.set("key", b"Hello World", None).unwrap();
    let _: () = redis.set("key", "Hello World").unwrap();

    // Basic range
    let r1 = redlite.getrange("key", 0, 4).unwrap();
    let r2: Vec<u8> = redis.getrange("key", 0, 4).unwrap();
    assert_eq!(r1, r2);

    // Negative indices
    let r1 = redlite.getrange("key", -5, -1).unwrap();
    let r2: Vec<u8> = redis.getrange("key", -5, -1).unwrap();
    assert_eq!(r1, r2);

    // Mixed indices
    let r1 = redlite.getrange("key", 6, -1).unwrap();
    let r2: Vec<u8> = redis.getrange("key", 6, -1).unwrap();
    assert_eq!(r1, r2);
}

#[test]
fn oracle_strings_setrange() {
    let mut redis = require_redis!();
    let redlite = Db::open_memory().unwrap();
    let _: () = redis::cmd("FLUSHDB").query(&mut redis).unwrap();

    redlite.set("key", b"Hello World", None).unwrap();
    let _: () = redis.set("key", "Hello World").unwrap();

    // SETRANGE
    let r1 = redlite.setrange("key", 6, b"Redis").unwrap();
    let r2: i64 = redis::cmd("SETRANGE").arg("key").arg(6).arg("Redis").query(&mut redis).unwrap();
    assert_eq!(r1, r2);

    // Verify final value
    let v1 = redlite.get("key").unwrap();
    let v2: Option<Vec<u8>> = redis.get("key").unwrap();
    assert_eq!(v1, v2);
}

#[test]
fn oracle_strings_mget_mset() {
    let mut redis = require_redis!();
    let redlite = Db::open_memory().unwrap();
    let _: () = redis::cmd("FLUSHDB").query(&mut redis).unwrap();

    // MSET
    let pairs = vec![
        ("key1", b"value1".to_vec()),
        ("key2", b"value2".to_vec()),
        ("key3", b"value3".to_vec()),
    ];

    let kvs: Vec<(&str, &[u8])> = pairs.iter().map(|(k, v)| (*k, v.as_slice())).collect();
    redlite.mset(&kvs).unwrap();
    let _: () = redis::cmd("MSET")
        .arg("key1").arg("value1")
        .arg("key2").arg("value2")
        .arg("key3").arg("value3")
        .query(&mut redis).unwrap();

    // MGET
    let r1 = redlite.mget(&["key1", "key2", "key3", "nonexistent"]);
    let r2: Vec<Option<Vec<u8>>> = redis::cmd("MGET").arg("key1").arg("key2").arg("key3").arg("nonexistent").query(&mut redis).unwrap();
    assert_eq!(r1, r2);
}

#[test]
fn oracle_strings_setnx() {
    let mut redis = require_redis!();
    let redlite = Db::open_memory().unwrap();
    let _: () = redis::cmd("FLUSHDB").query(&mut redis).unwrap();

    // SETNX on new key
    let r1 = redlite.set_opts("key", b"value", SetOptions::new().nx()).unwrap();
    let r2: bool = redis.set_nx("key", "value").unwrap();
    assert_eq!(r1, r2);

    // SETNX on existing key
    let r1 = redlite.set_opts("key", b"newvalue", SetOptions::new().nx()).unwrap();
    let r2: bool = redis.set_nx("key", "newvalue").unwrap();
    assert_eq!(r1, r2);

    // Value should still be original
    let v1 = redlite.get("key").unwrap();
    let v2: Option<Vec<u8>> = redis.get("key").unwrap();
    assert_eq!(v1, v2);
}

#[test]
fn oracle_strings_setex_psetex() {
    let mut redis = require_redis!();
    let redlite = Db::open_memory().unwrap();
    let _: () = redis::cmd("FLUSHDB").query(&mut redis).unwrap();

    // SETEX
    redlite.setex("key1", 3600, b"value1").unwrap();
    let _: () = redis::cmd("SETEX").arg("key1").arg(3600).arg("value1").query(&mut redis).unwrap();

    let v1 = redlite.get("key1").unwrap();
    let v2: Option<Vec<u8>> = redis.get("key1").unwrap();
    assert_eq!(v1, v2);

    let ttl1 = redlite.ttl("key1").unwrap();
    let ttl2: i64 = redis.ttl("key1").unwrap();
    assert!((ttl1 - ttl2).abs() <= 1);

    // PSETEX
    redlite.psetex("key2", 3600000, b"value2").unwrap();
    let _: () = redis::cmd("PSETEX").arg("key2").arg(3600000).arg("value2").query(&mut redis).unwrap();

    let v1 = redlite.get("key2").unwrap();
    let v2: Option<Vec<u8>> = redis.get("key2").unwrap();
    assert_eq!(v1, v2);
}

#[test]
fn oracle_strings_getex_getdel() {
    let mut redis = require_redis!();
    let redlite = Db::open_memory().unwrap();
    let _: () = redis::cmd("FLUSHDB").query(&mut redis).unwrap();

    // Setup
    redlite.set("key1", b"value1", None).unwrap();
    let _: () = redis.set("key1", "value1").unwrap();
    redlite.set("key2", b"value2", None).unwrap();
    let _: () = redis.set("key2", "value2").unwrap();

    // GETDEL
    let r1 = redlite.getdel("key1").unwrap();
    let r2: Option<Vec<u8>> = redis::cmd("GETDEL").arg("key1").query(&mut redis).unwrap();
    assert_eq!(r1, r2);

    // Key should be deleted
    let r1 = redlite.exists(&["key1"]).unwrap();
    let r2: i64 = redis.exists("key1").unwrap();
    assert_eq!(r1, r2);

    // GETEX with EX
    let r1 = redlite.getex("key2", Some(GetExOption::Ex(3600))).unwrap();
    let r2: Option<Vec<u8>> = redis::cmd("GETEX").arg("key2").arg("EX").arg(3600).query(&mut redis).unwrap();
    assert_eq!(r1, r2);

    // Check TTL was set
    let ttl1 = redlite.ttl("key2").unwrap();
    let ttl2: i64 = redis.ttl("key2").unwrap();
    assert!(ttl1 > 0 && ttl2 > 0);
}

// ============================================================================
// STRING ORACLE TESTS - Bit Operations
// ============================================================================

#[test]
fn oracle_strings_bit_operations() {
    let mut redis = require_redis!();
    let redlite = Db::open_memory().unwrap();
    let _: () = redis::cmd("FLUSHDB").query(&mut redis).unwrap();

    // SETBIT
    let r1 = redlite.setbit("bits", 7, true).unwrap();
    let r2: i64 = redis::cmd("SETBIT").arg("bits").arg(7).arg(1).query(&mut redis).unwrap();
    assert_eq!(r1, r2);

    let r1 = redlite.setbit("bits", 0, true).unwrap();
    let r2: i64 = redis::cmd("SETBIT").arg("bits").arg(0).arg(1).query(&mut redis).unwrap();
    assert_eq!(r1, r2);

    // GETBIT
    let r1 = redlite.getbit("bits", 0).unwrap();
    let r2: i64 = redis::cmd("GETBIT").arg("bits").arg(0).query(&mut redis).unwrap();
    assert_eq!(r1, r2);

    let r1 = redlite.getbit("bits", 7).unwrap();
    let r2: i64 = redis::cmd("GETBIT").arg("bits").arg(7).query(&mut redis).unwrap();
    assert_eq!(r1, r2);

    let r1 = redlite.getbit("bits", 100).unwrap();
    let r2: i64 = redis::cmd("GETBIT").arg("bits").arg(100).query(&mut redis).unwrap();
    assert_eq!(r1, r2);

    // BITCOUNT
    let r1 = redlite.bitcount("bits", None, None).unwrap();
    let r2: i64 = redis::cmd("BITCOUNT").arg("bits").query(&mut redis).unwrap();
    assert_eq!(r1, r2);
}

#[test]
fn oracle_strings_random_ops() {
    let mut redis = require_redis!();
    let redlite = Db::open_memory().unwrap();
    let _: () = redis::cmd("FLUSHDB").query(&mut redis).unwrap();

    let mut rng = ChaCha8Rng::seed_from_u64(12345);
    let mut divergences = 0;

    for _ in 0..1000 {
        let key = format!("str_{}", rng.gen_range(0..10));
        let op = rng.gen_range(0..6);

        match op {
            0 => {
                // SET
                let value: Vec<u8> = (0..rng.gen_range(1..20)).map(|_| rng.gen()).collect();
                redlite.set(&key, &value, None).unwrap();
                let _: () = redis.set(&key, &value).unwrap();
            }
            1 => {
                // GET
                let r1 = redlite.get(&key).unwrap();
                let r2: Option<Vec<u8>> = redis.get(&key).unwrap();
                if r1 != r2 { divergences += 1; }
            }
            2 => {
                // STRLEN
                let r1 = redlite.strlen(&key).unwrap();
                let r2: i64 = redis.strlen(&key).unwrap();
                if r1 != r2 { divergences += 1; }
            }
            3 => {
                // APPEND
                let value: Vec<u8> = (0..rng.gen_range(1..10)).map(|_| rng.gen()).collect();
                let r1 = redlite.append(&key, &value).ok();
                let r2: Option<usize> = redis.append(&key, &value).ok();
                if r1.map(|v| v as usize) != r2 { divergences += 1; }
            }
            4 => {
                // SETBIT
                let offset = rng.gen_range(0..100);
                let bit = rng.gen_bool(0.5);
                let r1 = redlite.setbit(&key, offset, bit).ok();
                let r2: Option<i64> = redis::cmd("SETBIT").arg(&key).arg(offset).arg(if bit { 1 } else { 0 }).query(&mut redis).ok();
                if r1 != r2 { divergences += 1; }
            }
            _ => {
                // GETBIT
                let offset = rng.gen_range(0..100);
                let r1 = redlite.getbit(&key, offset).ok();
                let r2: Option<i64> = redis::cmd("GETBIT").arg(&key).arg(offset).query(&mut redis).ok();
                if r1 != r2 { divergences += 1; }
            }
        }
    }

    assert_eq!(divergences, 0, "Found {} divergences in string operations", divergences);
}

// ============================================================================
// LIST ORACLE TESTS - Basic
// ============================================================================

#[test]
fn oracle_lists_push_pop() {
    let mut redis = require_redis!();
    let redlite = Db::open_memory().unwrap();
    let _: () = redis::cmd("FLUSHDB").query(&mut redis).unwrap();

    // LPUSH
    let r1 = redlite.lpush("list", &[b"a"]).unwrap();
    let r2: i64 = redis.lpush("list", "a").unwrap();
    assert_eq!(r1, r2);

    // RPUSH
    let r1 = redlite.rpush("list", &[b"b"]).unwrap();
    let r2: i64 = redis.rpush("list", "b").unwrap();
    assert_eq!(r1, r2);

    // LPUSH multiple
    let r1 = redlite.lpush("list", &[b"c", b"d"]).unwrap();
    let r2: i64 = redis.lpush("list", &["c", "d"]).unwrap();
    assert_eq!(r1, r2);

    // LRANGE
    let r1 = redlite.lrange("list", 0, -1).unwrap();
    let r2: Vec<Vec<u8>> = redis.lrange("list", 0, -1).unwrap();
    assert_eq!(r1, r2);

    // LPOP
    let r1 = redlite.lpop("list", None).unwrap();
    let r2: Option<Vec<u8>> = redis.lpop("list", None).unwrap();
    assert_eq!(r1.into_iter().next(), r2);

    // RPOP
    let r1 = redlite.rpop("list", None).unwrap();
    let r2: Option<Vec<u8>> = redis.rpop("list", None).unwrap();
    assert_eq!(r1.into_iter().next(), r2);
}

#[test]
fn oracle_lists_llen_lindex() {
    let mut redis = require_redis!();
    let redlite = Db::open_memory().unwrap();
    let _: () = redis::cmd("FLUSHDB").query(&mut redis).unwrap();

    for i in 0..5 {
        redlite.rpush("list", &[format!("item{}", i).as_bytes()]).unwrap();
        let _: i64 = redis.rpush("list", format!("item{}", i)).unwrap();
    }

    // LLEN
    let r1 = redlite.llen("list").unwrap();
    let r2: i64 = redis.llen("list").unwrap();
    assert_eq!(r1, r2);

    // LINDEX
    for i in 0..5 {
        let r1 = redlite.lindex("list", i).unwrap();
        let r2: Option<Vec<u8>> = redis.lindex("list", i as isize).unwrap();
        assert_eq!(r1, r2, "LINDEX mismatch at index {}", i);
    }

    // Negative indices
    let r1 = redlite.lindex("list", -1).unwrap();
    let r2: Option<Vec<u8>> = redis.lindex("list", -1).unwrap();
    assert_eq!(r1, r2);
}

#[test]
fn oracle_lists_lset_lrem() {
    let mut redis = require_redis!();
    let redlite = Db::open_memory().unwrap();
    let _: () = redis::cmd("FLUSHDB").query(&mut redis).unwrap();

    // Build list
    for i in 0..5 {
        redlite.rpush("list", &[format!("item{}", i).as_bytes()]).unwrap();
        let _: i64 = redis.rpush("list", format!("item{}", i)).unwrap();
    }

    // LSET
    redlite.lset("list", 2, b"modified").unwrap();
    let _: () = redis.lset("list", 2, "modified").unwrap();

    let r1 = redlite.lindex("list", 2).unwrap();
    let r2: Option<Vec<u8>> = redis.lindex("list", 2).unwrap();
    assert_eq!(r1, r2);

    // Add duplicates for LREM test
    redlite.rpush("list", &[b"dup", b"dup", b"dup"]).unwrap();
    let _: i64 = redis.rpush("list", &["dup", "dup", "dup"]).unwrap();

    // LREM
    let r1 = redlite.lrem("list", 2, b"dup").unwrap();
    let r2: i64 = redis.lrem("list", 2, "dup").unwrap();
    assert_eq!(r1, r2);

    // Verify list state
    let r1 = redlite.lrange("list", 0, -1).unwrap();
    let r2: Vec<Vec<u8>> = redis.lrange("list", 0, -1).unwrap();
    assert_eq!(r1, r2);
}

#[test]
fn oracle_lists_ltrim() {
    let mut redis = require_redis!();
    let redlite = Db::open_memory().unwrap();
    let _: () = redis::cmd("FLUSHDB").query(&mut redis).unwrap();

    for i in 0..10 {
        redlite.rpush("list", &[format!("item{}", i).as_bytes()]).unwrap();
        let _: i64 = redis.rpush("list", format!("item{}", i)).unwrap();
    }

    // LTRIM
    redlite.ltrim("list", 2, 7).unwrap();
    let _: () = redis.ltrim("list", 2, 7).unwrap();

    let r1 = redlite.lrange("list", 0, -1).unwrap();
    let r2: Vec<Vec<u8>> = redis.lrange("list", 0, -1).unwrap();
    assert_eq!(r1, r2);

    let r1 = redlite.llen("list").unwrap();
    let r2: i64 = redis.llen("list").unwrap();
    assert_eq!(r1, r2);
}

#[test]
fn oracle_lists_linsert() {
    let mut redis = require_redis!();
    let redlite = Db::open_memory().unwrap();
    let _: () = redis::cmd("FLUSHDB").query(&mut redis).unwrap();

    redlite.rpush("list", &[b"a", b"c"]).unwrap();
    let _: i64 = redis.rpush("list", &["a", "c"]).unwrap();

    // LINSERT BEFORE
    let r1 = redlite.linsert("list", true, b"c", b"b").unwrap();
    let r2: i64 = redis::cmd("LINSERT").arg("list").arg("BEFORE").arg("c").arg("b").query(&mut redis).unwrap();
    assert_eq!(r1, r2);

    // LINSERT AFTER
    let r1 = redlite.linsert("list", false, b"c", b"d").unwrap();
    let r2: i64 = redis::cmd("LINSERT").arg("list").arg("AFTER").arg("c").arg("d").query(&mut redis).unwrap();
    assert_eq!(r1, r2);

    let r1 = redlite.lrange("list", 0, -1).unwrap();
    let r2: Vec<Vec<u8>> = redis.lrange("list", 0, -1).unwrap();
    assert_eq!(r1, r2);
}

#[test]
fn oracle_lists_lpushx_rpushx() {
    let mut redis = require_redis!();
    let redlite = Db::open_memory().unwrap();
    let _: () = redis::cmd("FLUSHDB").query(&mut redis).unwrap();

    // LPUSHX on non-existent key
    let r1 = redlite.lpushx("list", &[b"a"]).unwrap();
    let r2: i64 = redis::cmd("LPUSHX").arg("list").arg("a").query(&mut redis).unwrap();
    assert_eq!(r1, r2);
    assert_eq!(r1, 0);

    // Create list
    redlite.rpush("list", &[b"x"]).unwrap();
    let _: i64 = redis.rpush("list", "x").unwrap();

    // LPUSHX on existing key
    let r1 = redlite.lpushx("list", &[b"a"]).unwrap();
    let r2: i64 = redis::cmd("LPUSHX").arg("list").arg("a").query(&mut redis).unwrap();
    assert_eq!(r1, r2);

    // RPUSHX on existing key
    let r1 = redlite.rpushx("list", &[b"z"]).unwrap();
    let r2: i64 = redis::cmd("RPUSHX").arg("list").arg("z").query(&mut redis).unwrap();
    assert_eq!(r1, r2);

    let r1 = redlite.lrange("list", 0, -1).unwrap();
    let r2: Vec<Vec<u8>> = redis.lrange("list", 0, -1).unwrap();
    assert_eq!(r1, r2);
}

#[test]
fn oracle_lists_random_ops() {
    let mut redis = require_redis!();
    let redlite = Db::open_memory().unwrap();
    let _: () = redis::cmd("FLUSHDB").query(&mut redis).unwrap();

    let mut rng = ChaCha8Rng::seed_from_u64(54321);
    let mut divergences = 0;

    for _ in 0..1000 {
        let key = format!("list_{}", rng.gen_range(0..5));
        let op = rng.gen_range(0..7);

        match op {
            0 => {
                // LPUSH
                let value = format!("item_{}", rng.gen::<u32>());
                let r1 = redlite.lpush(&key, &[value.as_bytes()]).unwrap();
                let r2: i64 = redis.lpush(&key, &value).unwrap();
                if r1 != r2 { divergences += 1; }
            }
            1 => {
                // RPUSH
                let value = format!("item_{}", rng.gen::<u32>());
                let r1 = redlite.rpush(&key, &[value.as_bytes()]).unwrap();
                let r2: i64 = redis.rpush(&key, &value).unwrap();
                if r1 != r2 { divergences += 1; }
            }
            2 => {
                // LPOP
                let r1 = redlite.lpop(&key, None).unwrap().into_iter().next();
                let r2: Option<Vec<u8>> = redis.lpop(&key, None).unwrap();
                if r1 != r2 { divergences += 1; }
            }
            3 => {
                // RPOP
                let r1 = redlite.rpop(&key, None).unwrap().into_iter().next();
                let r2: Option<Vec<u8>> = redis.rpop(&key, None).unwrap();
                if r1 != r2 { divergences += 1; }
            }
            4 => {
                // LLEN
                let r1 = redlite.llen(&key).unwrap();
                let r2: i64 = redis.llen(&key).unwrap();
                if r1 != r2 { divergences += 1; }
            }
            5 => {
                // LINDEX
                let idx = rng.gen_range(-5..10);
                let r1 = redlite.lindex(&key, idx).unwrap();
                let r2: Option<Vec<u8>> = redis.lindex(&key, idx as isize).unwrap();
                if r1 != r2 { divergences += 1; }
            }
            _ => {
                // LRANGE
                let r1 = redlite.lrange(&key, 0, -1).unwrap();
                let r2: Vec<Vec<u8>> = redis.lrange(&key, 0, -1).unwrap();
                if r1 != r2 { divergences += 1; }
            }
        }
    }

    assert_eq!(divergences, 0, "Found {} divergences in list operations", divergences);
}

// ============================================================================
// HASH ORACLE TESTS
// ============================================================================

#[test]
fn oracle_hashes_basic() {
    let mut redis = require_redis!();
    let redlite = Db::open_memory().unwrap();
    let _: () = redis::cmd("FLUSHDB").query(&mut redis).unwrap();

    // HSET single field
    let r1 = redlite.hset("hash", &[("field1", b"value1".as_slice())]).unwrap();
    let r2: usize = redis.hset("hash", "field1", "value1").unwrap();
    assert_eq!(r1 as usize, r2);

    // HGET
    let r1 = redlite.hget("hash", "field1").unwrap();
    let r2: Option<Vec<u8>> = redis.hget("hash", "field1").unwrap();
    assert_eq!(r1, r2);

    // HSET update
    let r1 = redlite.hset("hash", &[("field1", b"updated".as_slice())]).unwrap();
    let r2: usize = redis.hset("hash", "field1", "updated").unwrap();
    assert_eq!(r1 as usize, r2);

    // HDEL
    let r1 = redlite.hdel("hash", &["field1"]).unwrap();
    let r2: usize = redis.hdel("hash", "field1").unwrap();
    assert_eq!(r1 as usize, r2);

    // HGET deleted field
    let r1 = redlite.hget("hash", "field1").unwrap();
    let r2: Option<Vec<u8>> = redis.hget("hash", "field1").unwrap();
    assert_eq!(r1, r2);
}

#[test]
fn oracle_hashes_multiple_fields() {
    let mut redis = require_redis!();
    let redlite = Db::open_memory().unwrap();
    let _: () = redis::cmd("FLUSHDB").query(&mut redis).unwrap();

    for i in 0..5 {
        let field = format!("field{}", i);
        let value = format!("value{}", i);
        redlite.hset("hash", &[(&field, value.as_bytes())]).unwrap();
        let _: usize = redis.hset("hash", &field, &value).unwrap();
    }

    // HLEN
    let r1 = redlite.hlen("hash").unwrap();
    let r2: usize = redis.hlen("hash").unwrap();
    assert_eq!(r1 as usize, r2);

    // HGETALL
    let r1: HashMap<String, Vec<u8>> = redlite.hgetall("hash").unwrap().into_iter().collect();
    let r2: HashMap<String, Vec<u8>> = redis.hgetall("hash").unwrap();
    assert_eq!(r1, r2);

    // HEXISTS
    let r1 = redlite.hexists("hash", "field0").unwrap();
    let r2: bool = redis.hexists("hash", "field0").unwrap();
    assert_eq!(r1, r2);

    // HKEYS
    let mut r1 = redlite.hkeys("hash").unwrap();
    let mut r2: Vec<String> = redis.hkeys("hash").unwrap();
    r1.sort();
    r2.sort();
    assert_eq!(r1, r2);

    // HVALS
    let mut r1 = redlite.hvals("hash").unwrap();
    let mut r2: Vec<Vec<u8>> = redis.hvals("hash").unwrap();
    r1.sort();
    r2.sort();
    assert_eq!(r1, r2);
}

#[test]
fn oracle_hashes_hmget() {
    let mut redis = require_redis!();
    let redlite = Db::open_memory().unwrap();
    let _: () = redis::cmd("FLUSHDB").query(&mut redis).unwrap();

    for i in 0..5 {
        let field = format!("field{}", i);
        let value = format!("value{}", i);
        redlite.hset("hash", &[(&field, value.as_bytes())]).unwrap();
        let _: usize = redis.hset("hash", &field, &value).unwrap();
    }

    // HMGET
    let r1 = redlite.hmget("hash", &["field0", "field2", "field4", "nonexistent"]).unwrap();
    let r2: Vec<Option<Vec<u8>>> = redis.hget("hash", &["field0", "field2", "field4", "nonexistent"]).unwrap();
    assert_eq!(r1, r2);
}

#[test]
fn oracle_hashes_hincrby() {
    let mut redis = require_redis!();
    let redlite = Db::open_memory().unwrap();
    let _: () = redis::cmd("FLUSHDB").query(&mut redis).unwrap();

    // HINCRBY on new field
    let r1 = redlite.hincrby("hash", "counter", 5).unwrap();
    let r2: i64 = redis.hincr("hash", "counter", 5).unwrap();
    assert_eq!(r1, r2);

    // HINCRBY again
    let r1 = redlite.hincrby("hash", "counter", 3).unwrap();
    let r2: i64 = redis.hincr("hash", "counter", 3).unwrap();
    assert_eq!(r1, r2);

    // HINCRBY negative
    let r1 = redlite.hincrby("hash", "counter", -2).unwrap();
    let r2: i64 = redis.hincr("hash", "counter", -2).unwrap();
    assert_eq!(r1, r2);

    // HINCRBYFLOAT
    let r1: f64 = redlite.hincrbyfloat("hash", "float_counter", 1.5).unwrap().parse().unwrap();
    let r2: f64 = redis::cmd("HINCRBYFLOAT").arg("hash").arg("float_counter").arg(1.5).query(&mut redis).unwrap();
    assert!((r1 - r2).abs() < 1e-10);
}

#[test]
fn oracle_hashes_hsetnx() {
    let mut redis = require_redis!();
    let redlite = Db::open_memory().unwrap();
    let _: () = redis::cmd("FLUSHDB").query(&mut redis).unwrap();

    // HSETNX on new field
    let r1 = redlite.hsetnx("hash", "field", b"value").unwrap();
    let r2: bool = redis.hset_nx("hash", "field", "value").unwrap();
    assert_eq!(r1, r2);

    // HSETNX on existing field
    let r1 = redlite.hsetnx("hash", "field", b"newvalue").unwrap();
    let r2: bool = redis.hset_nx("hash", "field", "newvalue").unwrap();
    assert_eq!(r1, r2);

    // Value should still be original
    let v1 = redlite.hget("hash", "field").unwrap();
    let v2: Option<Vec<u8>> = redis.hget("hash", "field").unwrap();
    assert_eq!(v1, v2);
}

#[test]
fn oracle_hashes_random_ops() {
    let mut redis = require_redis!();
    let redlite = Db::open_memory().unwrap();
    let _: () = redis::cmd("FLUSHDB").query(&mut redis).unwrap();

    let mut rng = ChaCha8Rng::seed_from_u64(11111);
    let mut divergences = 0;

    for _ in 0..1000 {
        let key = format!("hash_{}", rng.gen_range(0..5));
        let field = format!("field_{}", rng.gen_range(0..10));
        let op = rng.gen_range(0..6);

        match op {
            0 => {
                // HSET
                let value = format!("value_{}", rng.gen::<u32>());
                let r1 = redlite.hset(&key, &[(&field, value.as_bytes())]).unwrap() as usize;
                let r2: usize = redis.hset(&key, &field, &value).unwrap();
                if r1 != r2 { divergences += 1; }
            }
            1 => {
                // HGET
                let r1 = redlite.hget(&key, &field).unwrap();
                let r2: Option<Vec<u8>> = redis.hget(&key, &field).unwrap();
                if r1 != r2 { divergences += 1; }
            }
            2 => {
                // HDEL
                let r1 = redlite.hdel(&key, &[&field]).unwrap() as usize;
                let r2: usize = redis.hdel(&key, &field).unwrap();
                if r1 != r2 { divergences += 1; }
            }
            3 => {
                // HEXISTS
                let r1 = redlite.hexists(&key, &field).unwrap();
                let r2: bool = redis.hexists(&key, &field).unwrap();
                if r1 != r2 { divergences += 1; }
            }
            4 => {
                // HLEN
                let r1 = redlite.hlen(&key).unwrap() as usize;
                let r2: usize = redis.hlen(&key).unwrap();
                if r1 != r2 { divergences += 1; }
            }
            _ => {
                // HGETALL
                let r1: HashMap<String, Vec<u8>> = redlite.hgetall(&key).unwrap().into_iter().collect();
                let r2: HashMap<String, Vec<u8>> = redis.hgetall(&key).unwrap();
                if r1 != r2 { divergences += 1; }
            }
        }
    }

    assert_eq!(divergences, 0, "Found {} divergences in hash operations", divergences);
}

// ============================================================================
// SET ORACLE TESTS
// ============================================================================

#[test]
fn oracle_sets_basic() {
    let mut redis = require_redis!();
    let redlite = Db::open_memory().unwrap();
    let _: () = redis::cmd("FLUSHDB").query(&mut redis).unwrap();

    // SADD single
    let r1 = redlite.sadd("set", &[b"a"]).unwrap();
    let r2: usize = redis.sadd("set", "a").unwrap();
    assert_eq!(r1 as usize, r2);

    // SADD duplicate
    let r1 = redlite.sadd("set", &[b"a"]).unwrap();
    let r2: usize = redis.sadd("set", "a").unwrap();
    assert_eq!(r1 as usize, r2);

    // SISMEMBER
    let r1 = redlite.sismember("set", b"a").unwrap();
    let r2: bool = redis.sismember("set", "a").unwrap();
    assert_eq!(r1, r2);

    // SREM
    let r1 = redlite.srem("set", &[b"a"]).unwrap();
    let r2: usize = redis.srem("set", "a").unwrap();
    assert_eq!(r1 as usize, r2);
}

#[test]
fn oracle_sets_smembers_scard() {
    let mut redis = require_redis!();
    let redlite = Db::open_memory().unwrap();
    let _: () = redis::cmd("FLUSHDB").query(&mut redis).unwrap();

    for c in b'a'..=b'e' {
        redlite.sadd("set", &[&[c]]).unwrap();
        let _: usize = redis.sadd("set", std::str::from_utf8(&[c]).unwrap()).unwrap();
    }

    // SCARD
    let r1 = redlite.scard("set").unwrap();
    let r2: usize = redis.scard("set").unwrap();
    assert_eq!(r1 as usize, r2);

    // SMEMBERS
    let mut r1 = redlite.smembers("set").unwrap();
    let mut r2: Vec<Vec<u8>> = redis.smembers("set").unwrap();
    r1.sort();
    r2.sort();
    assert_eq!(r1, r2);
}

#[test]
fn oracle_sets_set_operations() {
    let mut redis = require_redis!();
    let redlite = Db::open_memory().unwrap();
    let _: () = redis::cmd("FLUSHDB").query(&mut redis).unwrap();

    // Create two sets
    redlite.sadd("set1", &[b"a", b"b", b"c"]).unwrap();
    let _: usize = redis.sadd("set1", &["a", "b", "c"]).unwrap();
    redlite.sadd("set2", &[b"b", b"c", b"d"]).unwrap();
    let _: usize = redis.sadd("set2", &["b", "c", "d"]).unwrap();

    // SINTER
    let mut r1 = redlite.sinter(&["set1", "set2"]).unwrap();
    let mut r2: Vec<Vec<u8>> = redis.sinter(&["set1", "set2"]).unwrap();
    r1.sort();
    r2.sort();
    assert_eq!(r1, r2);

    // SUNION
    let mut r1 = redlite.sunion(&["set1", "set2"]).unwrap();
    let mut r2: Vec<Vec<u8>> = redis.sunion(&["set1", "set2"]).unwrap();
    r1.sort();
    r2.sort();
    assert_eq!(r1, r2);

    // SDIFF
    let mut r1 = redlite.sdiff(&["set1", "set2"]).unwrap();
    let mut r2: Vec<Vec<u8>> = redis.sdiff(&["set1", "set2"]).unwrap();
    r1.sort();
    r2.sort();
    assert_eq!(r1, r2);
}

#[test]
fn oracle_sets_store_operations() {
    let mut redis = require_redis!();
    let redlite = Db::open_memory().unwrap();
    let _: () = redis::cmd("FLUSHDB").query(&mut redis).unwrap();

    redlite.sadd("set1", &[b"a", b"b", b"c"]).unwrap();
    let _: usize = redis.sadd("set1", &["a", "b", "c"]).unwrap();
    redlite.sadd("set2", &[b"b", b"c", b"d"]).unwrap();
    let _: usize = redis.sadd("set2", &["b", "c", "d"]).unwrap();

    // SINTERSTORE
    let r1 = redlite.sinterstore("dest_inter", &["set1", "set2"]).unwrap();
    let r2: i64 = redis::cmd("SINTERSTORE").arg("dest_inter").arg("set1").arg("set2").query(&mut redis).unwrap();
    assert_eq!(r1, r2);

    // SUNIONSTORE
    let r1 = redlite.sunionstore("dest_union", &["set1", "set2"]).unwrap();
    let r2: i64 = redis::cmd("SUNIONSTORE").arg("dest_union").arg("set1").arg("set2").query(&mut redis).unwrap();
    assert_eq!(r1, r2);

    // SDIFFSTORE
    let r1 = redlite.sdiffstore("dest_diff", &["set1", "set2"]).unwrap();
    let r2: i64 = redis::cmd("SDIFFSTORE").arg("dest_diff").arg("set1").arg("set2").query(&mut redis).unwrap();
    assert_eq!(r1, r2);
}

#[test]
fn oracle_sets_smove() {
    let mut redis = require_redis!();
    let redlite = Db::open_memory().unwrap();
    let _: () = redis::cmd("FLUSHDB").query(&mut redis).unwrap();

    redlite.sadd("src", &[b"a", b"b"]).unwrap();
    let _: usize = redis.sadd("src", &["a", "b"]).unwrap();

    // SMOVE
    let r1 = redlite.smove("src", "dst", b"a").unwrap();
    let r2: i64 = redis::cmd("SMOVE").arg("src").arg("dst").arg("a").query(&mut redis).unwrap();
    assert_eq!(r1, r2);

    // Verify sets
    let mut r1 = redlite.smembers("src").unwrap();
    let mut r2: Vec<Vec<u8>> = redis.smembers("src").unwrap();
    r1.sort();
    r2.sort();
    assert_eq!(r1, r2);

    let mut r1 = redlite.smembers("dst").unwrap();
    let mut r2: Vec<Vec<u8>> = redis.smembers("dst").unwrap();
    r1.sort();
    r2.sort();
    assert_eq!(r1, r2);
}

#[test]
fn oracle_sets_random_ops() {
    let mut redis = require_redis!();
    let redlite = Db::open_memory().unwrap();
    let _: () = redis::cmd("FLUSHDB").query(&mut redis).unwrap();

    let mut rng = ChaCha8Rng::seed_from_u64(22222);
    let mut divergences = 0;

    for _ in 0..1000 {
        let key = format!("set_{}", rng.gen_range(0..5));
        let member = format!("member_{}", rng.gen_range(0..20));
        let op = rng.gen_range(0..5);

        match op {
            0 => {
                // SADD
                let r1 = redlite.sadd(&key, &[member.as_bytes()]).unwrap() as usize;
                let r2: usize = redis.sadd(&key, &member).unwrap();
                if r1 != r2 { divergences += 1; }
            }
            1 => {
                // SREM
                let r1 = redlite.srem(&key, &[member.as_bytes()]).unwrap() as usize;
                let r2: usize = redis.srem(&key, &member).unwrap();
                if r1 != r2 { divergences += 1; }
            }
            2 => {
                // SISMEMBER
                let r1 = redlite.sismember(&key, member.as_bytes()).unwrap();
                let r2: bool = redis.sismember(&key, &member).unwrap();
                if r1 != r2 { divergences += 1; }
            }
            3 => {
                // SCARD
                let r1 = redlite.scard(&key).unwrap() as usize;
                let r2: usize = redis.scard(&key).unwrap();
                if r1 != r2 { divergences += 1; }
            }
            _ => {
                // SMEMBERS
                let mut r1 = redlite.smembers(&key).unwrap();
                let mut r2: Vec<Vec<u8>> = redis.smembers(&key).unwrap();
                r1.sort();
                r2.sort();
                if r1 != r2 { divergences += 1; }
            }
        }
    }

    assert_eq!(divergences, 0, "Found {} divergences in set operations", divergences);
}

// ============================================================================
// SORTED SET ORACLE TESTS
// ============================================================================

#[test]
fn oracle_zsets_basic() {
    let mut redis = require_redis!();
    let redlite = Db::open_memory().unwrap();
    let _: () = redis::cmd("FLUSHDB").query(&mut redis).unwrap();

    // ZADD
    let r1 = redlite.zadd("zset", &[ZMember::new(1.0, b"a".to_vec())]).unwrap();
    let r2: usize = redis.zadd("zset", "a", 1.0).unwrap();
    assert_eq!(r1 as usize, r2);

    // ZADD update
    let r1 = redlite.zadd("zset", &[ZMember::new(2.0, b"a".to_vec())]).unwrap();
    let r2: usize = redis.zadd("zset", "a", 2.0).unwrap();
    assert_eq!(r1 as usize, r2);

    // ZSCORE
    let r1 = redlite.zscore("zset", b"a").unwrap();
    let r2: Option<f64> = redis.zscore("zset", "a").unwrap();
    assert_eq!(r1, r2);

    // ZREM
    let r1 = redlite.zrem("zset", &[b"a".as_slice()]).unwrap();
    let r2: usize = redis.zrem("zset", "a").unwrap();
    assert_eq!(r1 as usize, r2);
}

#[test]
fn oracle_zsets_ordering() {
    let mut redis = require_redis!();
    let redlite = Db::open_memory().unwrap();
    let _: () = redis::cmd("FLUSHDB").query(&mut redis).unwrap();

    let members = vec![(3.0, "c"), (1.0, "a"), (2.0, "b"), (5.0, "e"), (4.0, "d")];

    for (score, member) in &members {
        redlite.zadd("zset", &[ZMember::new(*score, member.as_bytes().to_vec())]).unwrap();
        let _: usize = redis.zadd("zset", *member, *score).unwrap();
    }

    // ZCARD
    let r1 = redlite.zcard("zset").unwrap();
    let r2: usize = redis.zcard("zset").unwrap();
    assert_eq!(r1 as usize, r2);

    // ZRANGE
    let r1: Vec<Vec<u8>> = redlite.zrange("zset", 0, -1, false).unwrap().into_iter().map(|m| m.member).collect();
    let r2: Vec<Vec<u8>> = redis.zrange("zset", 0, -1).unwrap();
    assert_eq!(r1, r2);

    // ZREVRANGE
    let r1: Vec<Vec<u8>> = redlite.zrevrange("zset", 0, -1, false).unwrap().into_iter().map(|m| m.member).collect();
    let r2: Vec<Vec<u8>> = redis.zrevrange("zset", 0, -1).unwrap();
    assert_eq!(r1, r2);
}

#[test]
fn oracle_zsets_rank() {
    let mut redis = require_redis!();
    let redlite = Db::open_memory().unwrap();
    let _: () = redis::cmd("FLUSHDB").query(&mut redis).unwrap();

    for (score, member) in [(1.0, "a"), (2.0, "b"), (3.0, "c")] {
        redlite.zadd("zset", &[ZMember::new(score, member.as_bytes().to_vec())]).unwrap();
        let _: usize = redis.zadd("zset", member, score).unwrap();
    }

    // ZRANK
    let r1 = redlite.zrank("zset", b"b").unwrap();
    let r2: Option<i64> = redis.zrank("zset", "b").unwrap();
    assert_eq!(r1.map(|x| x as i64), r2);

    // ZREVRANK
    let r1 = redlite.zrevrank("zset", b"b").unwrap();
    let r2: Option<i64> = redis.zrevrank("zset", "b").unwrap();
    assert_eq!(r1.map(|x| x as i64), r2);

    // Non-existent member
    let r1 = redlite.zrank("zset", b"z").unwrap();
    let r2: Option<i64> = redis.zrank("zset", "z").unwrap();
    assert_eq!(r1.map(|x| x as i64), r2);
}

#[test]
fn oracle_zsets_count() {
    let mut redis = require_redis!();
    let redlite = Db::open_memory().unwrap();
    let _: () = redis::cmd("FLUSHDB").query(&mut redis).unwrap();

    for (score, member) in [(1.0, "a"), (2.0, "b"), (3.0, "c"), (4.0, "d"), (5.0, "e")] {
        redlite.zadd("zset", &[ZMember::new(score, member.as_bytes().to_vec())]).unwrap();
        let _: usize = redis.zadd("zset", member, score).unwrap();
    }

    // ZCOUNT
    let r1 = redlite.zcount("zset", 2.0, 4.0).unwrap();
    let r2: i64 = redis.zcount("zset", 2.0, 4.0).unwrap();
    assert_eq!(r1, r2);

    let r1 = redlite.zcount("zset", f64::NEG_INFINITY, f64::INFINITY).unwrap();
    let r2: i64 = redis::cmd("ZCOUNT").arg("zset").arg("-inf").arg("+inf").query(&mut redis).unwrap();
    assert_eq!(r1, r2);
}

#[test]
fn oracle_zsets_zincrby() {
    let mut redis = require_redis!();
    let redlite = Db::open_memory().unwrap();
    let _: () = redis::cmd("FLUSHDB").query(&mut redis).unwrap();

    // ZINCRBY on new member
    let r1 = redlite.zincrby("zset", 1.5, b"a").unwrap();
    let r2: f64 = redis.zincr("zset", "a", 1.5).unwrap();
    assert!((r1 - r2).abs() < 1e-10);

    // ZINCRBY again
    let r1 = redlite.zincrby("zset", 2.5, b"a").unwrap();
    let r2: f64 = redis.zincr("zset", "a", 2.5).unwrap();
    assert!((r1 - r2).abs() < 1e-10);
}

#[test]
fn oracle_zsets_remove_range() {
    let mut redis = require_redis!();
    let redlite = Db::open_memory().unwrap();
    let _: () = redis::cmd("FLUSHDB").query(&mut redis).unwrap();

    for (score, member) in [(1.0, "a"), (2.0, "b"), (3.0, "c"), (4.0, "d"), (5.0, "e")] {
        redlite.zadd("zset", &[ZMember::new(score, member.as_bytes().to_vec())]).unwrap();
        let _: usize = redis.zadd("zset", member, score).unwrap();
    }

    // ZREMRANGEBYRANK
    let r1 = redlite.zremrangebyrank("zset", 0, 1).unwrap();
    let r2: i64 = redis::cmd("ZREMRANGEBYRANK").arg("zset").arg(0).arg(1).query(&mut redis).unwrap();
    assert_eq!(r1, r2);

    // Verify remaining
    let r1: Vec<Vec<u8>> = redlite.zrange("zset", 0, -1, false).unwrap().into_iter().map(|m| m.member).collect();
    let r2: Vec<Vec<u8>> = redis.zrange("zset", 0, -1).unwrap();
    assert_eq!(r1, r2);
}

#[test]
fn oracle_zsets_random_ops() {
    let mut redis = require_redis!();
    let redlite = Db::open_memory().unwrap();
    let _: () = redis::cmd("FLUSHDB").query(&mut redis).unwrap();

    let mut rng = ChaCha8Rng::seed_from_u64(33333);
    let mut divergences = 0;

    for _ in 0..1000 {
        let key = format!("zset_{}", rng.gen_range(0..5));
        let member = format!("member_{}", rng.gen_range(0..20));
        let score: f64 = rng.gen_range(0.0..100.0);
        let op = rng.gen_range(0..5);

        match op {
            0 => {
                // ZADD
                let r1 = redlite.zadd(&key, &[ZMember::new(score, member.as_bytes().to_vec())]).unwrap() as usize;
                let r2: usize = redis.zadd(&key, &member, score).unwrap();
                if r1 != r2 { divergences += 1; }
            }
            1 => {
                // ZSCORE
                let r1 = redlite.zscore(&key, member.as_bytes()).unwrap();
                let r2: Option<f64> = redis.zscore(&key, &member).unwrap();
                if r1 != r2 { divergences += 1; }
            }
            2 => {
                // ZRANK
                let r1 = redlite.zrank(&key, member.as_bytes()).unwrap().map(|x| x as i64);
                let r2: Option<i64> = redis.zrank(&key, &member).unwrap();
                if r1 != r2 { divergences += 1; }
            }
            3 => {
                // ZCARD
                let r1 = redlite.zcard(&key).unwrap() as usize;
                let r2: usize = redis.zcard(&key).unwrap();
                if r1 != r2 { divergences += 1; }
            }
            _ => {
                // ZRANGE
                let r1: Vec<Vec<u8>> = redlite.zrange(&key, 0, -1, false).unwrap().into_iter().map(|m| m.member).collect();
                let r2: Vec<Vec<u8>> = redis.zrange(&key, 0, -1).unwrap();
                if r1 != r2 { divergences += 1; }
            }
        }
    }

    assert_eq!(divergences, 0, "Found {} divergences in sorted set operations", divergences);
}

// ============================================================================
// KEY ORACLE TESTS
// ============================================================================

#[test]
fn oracle_keys_exists_del() {
    let mut redis = require_redis!();
    let redlite = Db::open_memory().unwrap();
    let _: () = redis::cmd("FLUSHDB").query(&mut redis).unwrap();

    // EXISTS on non-existent key
    let r1 = redlite.exists(&["key1"]).unwrap();
    let r2: usize = redis.exists("key1").unwrap();
    assert_eq!(r1 as usize, r2);

    // Create keys
    redlite.set("key1", b"v1", None).unwrap();
    let _: () = redis.set("key1", "v1").unwrap();
    redlite.set("key2", b"v2", None).unwrap();
    let _: () = redis.set("key2", "v2").unwrap();

    // EXISTS multiple keys
    let r1 = redlite.exists(&["key1", "key2", "key3"]).unwrap();
    let r2: usize = redis.exists(&["key1", "key2", "key3"]).unwrap();
    assert_eq!(r1 as usize, r2);

    // DEL
    let r1 = redlite.del(&["key1", "key2", "key3"]).unwrap();
    let r2: usize = redis.del(&["key1", "key2", "key3"]).unwrap();
    assert_eq!(r1 as usize, r2);
}

#[test]
fn oracle_keys_type() {
    let mut redis = require_redis!();
    let redlite = Db::open_memory().unwrap();
    let _: () = redis::cmd("FLUSHDB").query(&mut redis).unwrap();

    redlite.set("str", b"value", None).unwrap();
    let _: () = redis.set("str", "value").unwrap();
    redlite.lpush("list", &[b"item"]).unwrap();
    let _: i64 = redis.lpush("list", "item").unwrap();
    redlite.hset("hash", &[("field", b"value".as_slice())]).unwrap();
    let _: usize = redis.hset("hash", "field", "value").unwrap();
    redlite.sadd("set", &[b"member"]).unwrap();
    let _: usize = redis.sadd("set", "member").unwrap();
    redlite.zadd("zset", &[ZMember::new(1.0, b"member".to_vec())]).unwrap();
    let _: usize = redis.zadd("zset", "member", 1.0).unwrap();

    for (key, _) in [("str", "string"), ("list", "list"), ("hash", "hash"), ("set", "set"), ("zset", "zset")] {
        let r1 = redlite.key_type(key).unwrap();
        let r2: String = redis::cmd("TYPE").arg(key).query(&mut redis).unwrap();
        let r1_str = r1.map(|t| format!("{:?}", t).to_lowercase());
        assert_eq!(r1_str, Some(r2), "TYPE mismatch for key: {}", key);
    }
}

#[test]
fn oracle_keys_ttl_expire() {
    let mut redis = require_redis!();
    let redlite = Db::open_memory().unwrap();
    let _: () = redis::cmd("FLUSHDB").query(&mut redis).unwrap();

    // TTL on non-existent key
    let r1 = redlite.ttl("key").unwrap();
    let r2: i64 = redis.ttl("key").unwrap();
    assert_eq!(r1, r2);

    redlite.set("key", b"value", None).unwrap();
    let _: () = redis.set("key", "value").unwrap();

    // TTL on key without expiration
    let r1 = redlite.ttl("key").unwrap();
    let r2: i64 = redis.ttl("key").unwrap();
    assert_eq!(r1, r2);

    // EXPIRE
    let r1 = redlite.expire("key", 3600).unwrap();
    let r2: bool = redis.expire("key", 3600).unwrap();
    assert_eq!(r1, r2);

    // PTTL
    let r1 = redlite.pttl("key").unwrap();
    let r2: i64 = redis.pttl("key").unwrap();
    assert!((r1 - r2).abs() <= 1000);

    // PERSIST
    let r1 = redlite.persist("key").unwrap();
    let r2: bool = redis.persist("key").unwrap();
    assert_eq!(r1, r2);
}

#[test]
fn oracle_keys_pexpire() {
    let mut redis = require_redis!();
    let redlite = Db::open_memory().unwrap();
    let _: () = redis::cmd("FLUSHDB").query(&mut redis).unwrap();

    redlite.set("key", b"value", None).unwrap();
    let _: () = redis.set("key", "value").unwrap();

    // PEXPIRE
    let r1 = redlite.pexpire("key", 3600000).unwrap();
    let r2: bool = redis::cmd("PEXPIRE").arg("key").arg(3600000).query(&mut redis).unwrap();
    assert_eq!(r1, r2);

    // PTTL
    let r1 = redlite.pttl("key").unwrap();
    let r2: i64 = redis.pttl("key").unwrap();
    assert!((r1 - r2).abs() <= 1000);
}

#[test]
fn oracle_keys_rename() {
    let mut redis = require_redis!();
    let redlite = Db::open_memory().unwrap();
    let _: () = redis::cmd("FLUSHDB").query(&mut redis).unwrap();

    redlite.set("oldkey", b"value", None).unwrap();
    let _: () = redis.set("oldkey", "value").unwrap();

    // RENAME
    redlite.rename("oldkey", "newkey").unwrap();
    let _: () = redis.rename("oldkey", "newkey").unwrap();

    let r1 = redlite.exists(&["oldkey"]).unwrap();
    let r2: usize = redis.exists("oldkey").unwrap();
    assert_eq!(r1 as usize, r2);

    let r1 = redlite.get("newkey").unwrap();
    let r2: Option<Vec<u8>> = redis.get("newkey").unwrap();
    assert_eq!(r1, r2);
}

#[test]
fn oracle_keys_renamenx() {
    let mut redis = require_redis!();
    let redlite = Db::open_memory().unwrap();
    let _: () = redis::cmd("FLUSHDB").query(&mut redis).unwrap();

    redlite.set("key1", b"value1", None).unwrap();
    let _: () = redis.set("key1", "value1").unwrap();
    redlite.set("key2", b"value2", None).unwrap();
    let _: () = redis.set("key2", "value2").unwrap();

    // RENAMENX to non-existent key (should succeed)
    let r1 = redlite.renamenx("key1", "key3").unwrap();
    let r2: bool = redis.rename_nx("key1", "key3").unwrap();
    assert_eq!(r1, r2);

    // RENAMENX to existing key (should fail)
    let r1 = redlite.renamenx("key3", "key2").unwrap();
    let r2: bool = redis.rename_nx("key3", "key2").unwrap();
    assert_eq!(r1, r2);
}

#[test]
fn oracle_keys_scan() {
    let mut redis = require_redis!();
    let redlite = Db::open_memory().unwrap();
    let _: () = redis::cmd("FLUSHDB").query(&mut redis).unwrap();

    // Create some keys
    for i in 0..20 {
        let key = format!("key:{}", i);
        redlite.set(&key, b"value", None).unwrap();
        let _: () = redis.set(&key, "value").unwrap();
    }

    // KEYS pattern
    let mut r1 = redlite.keys("key:*").unwrap();
    let mut r2: Vec<String> = redis.keys("key:*").unwrap();
    r1.sort();
    r2.sort();
    assert_eq!(r1, r2);
}

#[test]
fn oracle_keys_random_ops() {
    let mut redis = require_redis!();
    let redlite = Db::open_memory().unwrap();
    let _: () = redis::cmd("FLUSHDB").query(&mut redis).unwrap();

    let mut rng = ChaCha8Rng::seed_from_u64(44444);
    let mut divergences = 0;

    for _ in 0..500 {
        let key = format!("key_{}", rng.gen_range(0..10));
        let op = rng.gen_range(0..5);

        match op {
            0 => {
                // SET
                let value = format!("value_{}", rng.gen::<u32>());
                redlite.set(&key, value.as_bytes(), None).unwrap();
                let _: () = redis.set(&key, &value).unwrap();
            }
            1 => {
                // EXISTS
                let r1 = redlite.exists(&[&key]).unwrap() as usize;
                let r2: usize = redis.exists(&key).unwrap();
                if r1 != r2 { divergences += 1; }
            }
            2 => {
                // DEL
                let r1 = redlite.del(&[&key]).unwrap() as usize;
                let r2: usize = redis.del(&key).unwrap();
                if r1 != r2 { divergences += 1; }
            }
            3 => {
                // TTL
                let r1 = redlite.ttl(&key).unwrap();
                let r2: i64 = redis.ttl(&key).unwrap();
                if r1 != r2 { divergences += 1; }
            }
            _ => {
                // EXPIRE
                let r1 = redlite.expire(&key, 3600).ok();
                let r2: Option<bool> = redis.expire(&key, 3600).ok();
                if r1 != r2 { divergences += 1; }
            }
        }
    }

    assert_eq!(divergences, 0, "Found {} divergences in key operations", divergences);
}

#[test]
fn oracle_keys_expireat() {
    let mut redis = require_redis!();
    let redlite = Db::open_memory().unwrap();
    let _: () = redis::cmd("FLUSHDB").query(&mut redis).unwrap();

    redlite.set("key", b"value", None).unwrap();
    let _: () = redis.set("key", "value").unwrap();

    // EXPIREAT - set expiry to 1 hour from now
    let timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64 + 3600;

    let r1 = redlite.expireat("key", timestamp).unwrap();
    let r2: bool = redis::cmd("EXPIREAT").arg("key").arg(timestamp).query(&mut redis).unwrap();
    assert_eq!(r1, r2);

    // TTL should be around 3600
    let r1 = redlite.ttl("key").unwrap();
    let r2: i64 = redis.ttl("key").unwrap();
    assert!((r1 - r2).abs() <= 2);
}

#[test]
fn oracle_keys_pexpireat() {
    let mut redis = require_redis!();
    let redlite = Db::open_memory().unwrap();
    let _: () = redis::cmd("FLUSHDB").query(&mut redis).unwrap();

    redlite.set("key", b"value", None).unwrap();
    let _: () = redis.set("key", "value").unwrap();

    // PEXPIREAT - set expiry to 1 hour from now in milliseconds
    let timestamp_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64 + 3600000;

    let r1 = redlite.pexpireat("key", timestamp_ms).unwrap();
    let r2: bool = redis::cmd("PEXPIREAT").arg("key").arg(timestamp_ms).query(&mut redis).unwrap();
    assert_eq!(r1, r2);

    // PTTL should be around 3600000
    let r1 = redlite.pttl("key").unwrap();
    let r2: i64 = redis.pttl("key").unwrap();
    assert!((r1 - r2).abs() <= 1000);
}

#[test]
fn oracle_keys_persist() {
    let mut redis = require_redis!();
    let redlite = Db::open_memory().unwrap();
    let _: () = redis::cmd("FLUSHDB").query(&mut redis).unwrap();

    redlite.set("key", b"value", None).unwrap();
    let _: () = redis.set("key", "value").unwrap();

    // Set expiry
    redlite.expire("key", 3600).unwrap();
    let _: bool = redis.expire("key", 3600).unwrap();

    // PERSIST removes the expiry
    let r1 = redlite.persist("key").unwrap();
    let r2: bool = redis::cmd("PERSIST").arg("key").query(&mut redis).unwrap();
    assert_eq!(r1, r2);

    // TTL should now be -1 (no expiry)
    let r1 = redlite.ttl("key").unwrap();
    let r2: i64 = redis.ttl("key").unwrap();
    assert_eq!(r1, r2);

    // PERSIST on key without expiry
    let r1 = redlite.persist("key").unwrap();
    let r2: bool = redis::cmd("PERSIST").arg("key").query(&mut redis).unwrap();
    assert_eq!(r1, r2);
}

// ============================================================================
// ADDITIONAL LIST ORACLE TESTS
// ============================================================================

#[test]
fn oracle_lists_lpos() {
    let mut redis = require_redis!();
    let redlite = Db::open_memory().unwrap();
    let _: () = redis::cmd("FLUSHDB").query(&mut redis).unwrap();

    // Create list
    for item in &["a", "b", "c", "b", "d", "b"] {
        redlite.rpush("mylist", &[item.as_bytes()]).unwrap();
        let _: usize = redis.rpush("mylist", *item).unwrap();
    }

    // LPOS - find first occurrence (count=1 to get single result)
    let r1 = redlite.lpos("mylist", b"b", None, Some(1), None).unwrap();
    let r2: Option<i64> = redis::cmd("LPOS").arg("mylist").arg("b").query(&mut redis).unwrap();
    assert_eq!(r1.first().copied(), r2);

    // LPOS - element not found
    let r1 = redlite.lpos("mylist", b"z", None, Some(1), None).unwrap();
    let r2: Option<i64> = redis::cmd("LPOS").arg("mylist").arg("z").query(&mut redis).unwrap();
    assert_eq!(r1.first().copied(), r2);
}

#[test]
fn oracle_lists_lmove() {
    let mut redis = require_redis!();
    let redlite = Db::open_memory().unwrap();
    let _: () = redis::cmd("FLUSHDB").query(&mut redis).unwrap();

    // Create source list
    for item in &["a", "b", "c"] {
        redlite.rpush("src", &[item.as_bytes()]).unwrap();
        let _: usize = redis.rpush("src", *item).unwrap();
    }

    // LMOVE LEFT RIGHT
    let r1 = redlite.lmove("src", "dst", ListDirection::Left, ListDirection::Right).unwrap();
    let r2: Option<Vec<u8>> = redis::cmd("LMOVE").arg("src").arg("dst").arg("LEFT").arg("RIGHT").query(&mut redis).unwrap();
    assert_eq!(r1, r2);

    // Check resulting lists
    let r1 = redlite.lrange("src", 0, -1).unwrap();
    let r2: Vec<Vec<u8>> = redis.lrange("src", 0, -1).unwrap();
    assert_eq!(r1, r2);

    let r1 = redlite.lrange("dst", 0, -1).unwrap();
    let r2: Vec<Vec<u8>> = redis.lrange("dst", 0, -1).unwrap();
    assert_eq!(r1, r2);
}

// ============================================================================
// ADDITIONAL SET ORACLE TESTS
// ============================================================================

#[test]
fn oracle_sets_spop() {
    let mut redis = require_redis!();
    let redlite = Db::open_memory().unwrap();
    let _: () = redis::cmd("FLUSHDB").query(&mut redis).unwrap();

    // Create set
    for i in 0..10 {
        let member = format!("m{}", i);
        redlite.sadd("myset", &[member.as_bytes()]).unwrap();
        let _: usize = redis.sadd("myset", &member).unwrap();
    }

    // SPOP - verify it removes one member
    let r1_before = redlite.scard("myset").unwrap();
    let r2_before: usize = redis.scard("myset").unwrap();
    assert_eq!(r1_before as usize, r2_before);

    let popped1 = redlite.spop("myset", None).unwrap();
    let popped2: Vec<Vec<u8>> = redis::cmd("SPOP").arg("myset").query(&mut redis).unwrap();

    // Both should pop exactly one element
    assert_eq!(popped1.len(), 1);
    assert_eq!(popped2.len(), 1);

    let r1_after = redlite.scard("myset").unwrap();
    let r2_after: usize = redis.scard("myset").unwrap();
    assert_eq!(r1_after as usize, r2_after);
    assert_eq!(r1_after as usize, r1_before as usize - 1);
}

#[test]
fn oracle_sets_srandmember() {
    let mut redis = require_redis!();
    let redlite = Db::open_memory().unwrap();
    let _: () = redis::cmd("FLUSHDB").query(&mut redis).unwrap();

    // Create set
    for i in 0..10 {
        let member = format!("m{}", i);
        redlite.sadd("myset", &[member.as_bytes()]).unwrap();
        let _: usize = redis.sadd("myset", &member).unwrap();
    }

    // SRANDMEMBER count=3
    let r1 = redlite.srandmember("myset", Some(3)).unwrap();
    let r2: Vec<Vec<u8>> = redis::cmd("SRANDMEMBER").arg("myset").arg(3).query(&mut redis).unwrap();

    // Both should return 3 members
    assert_eq!(r1.len(), 3);
    assert_eq!(r2.len(), 3);

    // Set should be unchanged
    let r1_card = redlite.scard("myset").unwrap();
    let r2_card: usize = redis.scard("myset").unwrap();
    assert_eq!(r1_card as usize, r2_card);
    assert_eq!(r1_card, 10);
}

#[test]
fn oracle_sets_sscan() {
    let mut redis = require_redis!();
    let redlite = Db::open_memory().unwrap();
    let _: () = redis::cmd("FLUSHDB").query(&mut redis).unwrap();

    // Create set
    for i in 0..20 {
        let member = format!("member:{}", i);
        redlite.sadd("myset", &[member.as_bytes()]).unwrap();
        let _: usize = redis.sadd("myset", &member).unwrap();
    }

    // SSCAN - collect all members
    let mut redlite_members: Vec<Vec<u8>> = Vec::new();
    let mut cursor = "0".to_string();
    loop {
        let (next_cursor, batch) = redlite.sscan("myset", &cursor, None, 100).unwrap();
        redlite_members.extend(batch);
        cursor = next_cursor;
        if cursor == "0" { break; }
    }

    let mut redis_members: Vec<Vec<u8>> = Vec::new();
    let mut redis_cursor: u64 = 0;
    loop {
        let result: (u64, Vec<Vec<u8>>) = redis::cmd("SSCAN").arg("myset").arg(redis_cursor).query(&mut redis).unwrap();
        redis_cursor = result.0;
        redis_members.extend(result.1);
        if redis_cursor == 0 { break; }
    }

    // Sort and compare
    redlite_members.sort();
    redis_members.sort();
    assert_eq!(redlite_members, redis_members);
}

// ============================================================================
// ADDITIONAL HASH ORACLE TESTS
// ============================================================================

#[test]
fn oracle_hashes_hscan() {
    let mut redis = require_redis!();
    let redlite = Db::open_memory().unwrap();
    let _: () = redis::cmd("FLUSHDB").query(&mut redis).unwrap();

    // Create hash
    for i in 0..20 {
        let field = format!("field:{}", i);
        let value = format!("value:{}", i);
        redlite.hset("myhash", &[(&field, value.as_bytes())]).unwrap();
        let _: usize = redis.hset("myhash", &field, &value).unwrap();
    }

    // HSCAN - collect all fields
    let mut redlite_fields: Vec<(String, Vec<u8>)> = Vec::new();
    let mut cursor = "0".to_string();
    loop {
        let (next_cursor, batch) = redlite.hscan("myhash", &cursor, None, 100).unwrap();
        redlite_fields.extend(batch);
        cursor = next_cursor;
        if cursor == "0" { break; }
    }

    let mut redis_fields: Vec<(String, Vec<u8>)> = Vec::new();
    let mut redis_cursor: u64 = 0;
    loop {
        let result: (u64, Vec<(String, Vec<u8>)>) = redis::cmd("HSCAN").arg("myhash").arg(redis_cursor).query(&mut redis).unwrap();
        redis_cursor = result.0;
        redis_fields.extend(result.1);
        if redis_cursor == 0 { break; }
    }

    // Sort and compare
    redlite_fields.sort();
    redis_fields.sort();
    assert_eq!(redlite_fields, redis_fields);
}

// ============================================================================
// ADDITIONAL SORTED SET ORACLE TESTS
// ============================================================================

#[test]
fn oracle_zsets_zscan() {
    let mut redis = require_redis!();
    let redlite = Db::open_memory().unwrap();
    let _: () = redis::cmd("FLUSHDB").query(&mut redis).unwrap();

    // Create sorted set
    for i in 0..20 {
        let member = format!("member:{}", i);
        let score = i as f64;
        redlite.zadd("myzset", &[ZMember::new(score, member.as_bytes().to_vec())]).unwrap();
        let _: usize = redis.zadd("myzset", &member, score).unwrap();
    }

    // ZSCAN - collect all members
    let mut redlite_members: Vec<(Vec<u8>, f64)> = Vec::new();
    let mut cursor = "0".to_string();
    loop {
        let (next_cursor, batch) = redlite.zscan("myzset", &cursor, None, 100).unwrap();
        redlite_members.extend(batch);
        cursor = next_cursor;
        if cursor == "0" { break; }
    }

    let mut redis_members: Vec<(Vec<u8>, f64)> = Vec::new();
    let mut redis_cursor: u64 = 0;
    loop {
        let result: (u64, Vec<(String, f64)>) = redis::cmd("ZSCAN").arg("myzset").arg(redis_cursor).query(&mut redis).unwrap();
        redis_cursor = result.0;
        for (m, s) in result.1 {
            redis_members.push((m.into_bytes(), s));
        }
        if redis_cursor == 0 { break; }
    }

    // Sort by member and compare
    redlite_members.sort_by(|a, b| a.0.cmp(&b.0));
    redis_members.sort_by(|a, b| a.0.cmp(&b.0));
    assert_eq!(redlite_members, redis_members);
}

#[test]
fn oracle_zsets_zrangebyscore() {
    let mut redis = require_redis!();
    let redlite = Db::open_memory().unwrap();
    let _: () = redis::cmd("FLUSHDB").query(&mut redis).unwrap();

    // Create sorted set
    for i in 0..20 {
        let member = format!("m{}", i);
        let score = i as f64;
        redlite.zadd("myzset", &[ZMember::new(score, member.as_bytes().to_vec())]).unwrap();
        let _: usize = redis.zadd("myzset", &member, score).unwrap();
    }

    // ZRANGEBYSCORE
    let r1: Vec<Vec<u8>> = redlite.zrangebyscore("myzset", 5.0, 15.0, None, None).unwrap()
        .into_iter().map(|m| m.member).collect();
    let r2: Vec<Vec<u8>> = redis::cmd("ZRANGEBYSCORE").arg("myzset").arg(5).arg(15).query(&mut redis).unwrap();
    assert_eq!(r1, r2);

    // With LIMIT
    let r1: Vec<Vec<u8>> = redlite.zrangebyscore("myzset", 0.0, 100.0, Some(5), Some(3)).unwrap()
        .into_iter().map(|m| m.member).collect();
    let r2: Vec<Vec<u8>> = redis::cmd("ZRANGEBYSCORE").arg("myzset").arg(0).arg(100).arg("LIMIT").arg(5).arg(3).query(&mut redis).unwrap();
    assert_eq!(r1, r2);
}

#[test]
fn oracle_zsets_zinterstore() {
    let mut redis = require_redis!();
    let redlite = Db::open_memory().unwrap();
    let _: () = redis::cmd("FLUSHDB").query(&mut redis).unwrap();

    // Create two sorted sets
    redlite.zadd("zset1", &[
        ZMember::new(1.0, b"a".to_vec()),
        ZMember::new(2.0, b"b".to_vec()),
        ZMember::new(3.0, b"c".to_vec()),
    ]).unwrap();
    let _: i64 = redis::cmd("ZADD").arg("zset1").arg(1.0).arg("a").arg(2.0).arg("b").arg(3.0).arg("c").query(&mut redis).unwrap();

    redlite.zadd("zset2", &[
        ZMember::new(10.0, b"b".to_vec()),
        ZMember::new(20.0, b"c".to_vec()),
        ZMember::new(30.0, b"d".to_vec()),
    ]).unwrap();
    let _: i64 = redis::cmd("ZADD").arg("zset2").arg(10.0).arg("b").arg(20.0).arg("c").arg(30.0).arg("d").query(&mut redis).unwrap();

    // ZINTERSTORE
    let r1 = redlite.zinterstore("out", &["zset1", "zset2"], None, None).unwrap();
    let r2: i64 = redis::cmd("ZINTERSTORE").arg("out").arg(2).arg("zset1").arg("zset2").query(&mut redis).unwrap();
    assert_eq!(r1, r2);

    // Check result
    let r1: Vec<Vec<u8>> = redlite.zrange("out", 0, -1, false).unwrap()
        .into_iter().map(|m| m.member).collect();
    let r2: Vec<Vec<u8>> = redis.zrange("out", 0, -1).unwrap();
    assert_eq!(r1, r2);
}

#[test]
fn oracle_zsets_zunionstore() {
    let mut redis = require_redis!();
    let redlite = Db::open_memory().unwrap();
    let _: () = redis::cmd("FLUSHDB").query(&mut redis).unwrap();

    // Create two sorted sets
    redlite.zadd("zset1", &[
        ZMember::new(1.0, b"a".to_vec()),
        ZMember::new(2.0, b"b".to_vec()),
    ]).unwrap();
    let _: i64 = redis::cmd("ZADD").arg("zset1").arg(1.0).arg("a").arg(2.0).arg("b").query(&mut redis).unwrap();

    redlite.zadd("zset2", &[
        ZMember::new(10.0, b"b".to_vec()),
        ZMember::new(20.0, b"c".to_vec()),
    ]).unwrap();
    let _: i64 = redis::cmd("ZADD").arg("zset2").arg(10.0).arg("b").arg(20.0).arg("c").query(&mut redis).unwrap();

    // ZUNIONSTORE
    let r1 = redlite.zunionstore("out", &["zset1", "zset2"], None, None).unwrap();
    let r2: i64 = redis::cmd("ZUNIONSTORE").arg("out").arg(2).arg("zset1").arg("zset2").query(&mut redis).unwrap();
    assert_eq!(r1, r2);

    // Check result
    let r1: Vec<Vec<u8>> = redlite.zrange("out", 0, -1, false).unwrap()
        .into_iter().map(|m| m.member).collect();
    let r2: Vec<Vec<u8>> = redis.zrange("out", 0, -1).unwrap();
    assert_eq!(r1, r2);
}

// ============================================================================
// BITMAP ORACLE TESTS
// ============================================================================

#[test]
fn oracle_bitmap_bitop() {
    let mut redis = require_redis!();
    let redlite = Db::open_memory().unwrap();
    let _: () = redis::cmd("FLUSHDB").query(&mut redis).unwrap();

    // Create bitmaps
    redlite.set("key1", b"\xff\x0f", None).unwrap();
    let _: () = redis.set("key1", b"\xff\x0f").unwrap();

    redlite.set("key2", b"\x0f\xff", None).unwrap();
    let _: () = redis.set("key2", b"\x0f\xff").unwrap();

    // BITOP AND
    let r1 = redlite.bitop("AND", "destkey", &["key1", "key2"]).unwrap();
    let r2: i64 = redis::cmd("BITOP").arg("AND").arg("destkey").arg("key1").arg("key2").query(&mut redis).unwrap();
    assert_eq!(r1, r2);

    let r1 = redlite.get("destkey").unwrap();
    let r2: Option<Vec<u8>> = redis.get("destkey").unwrap();
    assert_eq!(r1, r2);

    // BITOP OR
    let r1 = redlite.bitop("OR", "destkey2", &["key1", "key2"]).unwrap();
    let r2: i64 = redis::cmd("BITOP").arg("OR").arg("destkey2").arg("key1").arg("key2").query(&mut redis).unwrap();
    assert_eq!(r1, r2);

    let r1 = redlite.get("destkey2").unwrap();
    let r2: Option<Vec<u8>> = redis.get("destkey2").unwrap();
    assert_eq!(r1, r2);

    // BITOP XOR
    let r1 = redlite.bitop("XOR", "destkey3", &["key1", "key2"]).unwrap();
    let r2: i64 = redis::cmd("BITOP").arg("XOR").arg("destkey3").arg("key1").arg("key2").query(&mut redis).unwrap();
    assert_eq!(r1, r2);

    let r1 = redlite.get("destkey3").unwrap();
    let r2: Option<Vec<u8>> = redis.get("destkey3").unwrap();
    assert_eq!(r1, r2);

    // BITOP NOT
    let r1 = redlite.bitop("NOT", "destkey4", &["key1"]).unwrap();
    let r2: i64 = redis::cmd("BITOP").arg("NOT").arg("destkey4").arg("key1").query(&mut redis).unwrap();
    assert_eq!(r1, r2);

    let r1 = redlite.get("destkey4").unwrap();
    let r2: Option<Vec<u8>> = redis.get("destkey4").unwrap();
    assert_eq!(r1, r2);
}

// NOTE: GEO commands (GEOADD, GEOPOS, GEODIST, GEOHASH, GEOSEARCH) require
// the "geo" feature flag to be enabled. Enable with:
//   cargo test --test oracle --features geo -- --test-threads=1

// ============================================================================
// STREAM ORACLE TESTS
// ============================================================================

#[test]
fn oracle_streams_basic() {
    let mut redis = require_redis!();
    let redlite = Db::open_memory().unwrap();
    let _: () = redis::cmd("FLUSHDB").query(&mut redis).unwrap();

    // XADD with auto-generated ID (None means *)
    let id1 = redlite.xadd("mystream", None, &[(b"field1".as_slice(), b"value1".as_slice()), (b"field2", b"value2")], false, None, None, false).unwrap();
    let id2: String = redis::cmd("XADD").arg("mystream").arg("*").arg("field1").arg("value1").arg("field2").arg("value2").query(&mut redis).unwrap();

    // Both should return IDs
    assert!(id1.is_some());
    assert!(id2.contains('-'));

    // XLEN
    let r1 = redlite.xlen("mystream").unwrap();
    let r2: i64 = redis::cmd("XLEN").arg("mystream").query(&mut redis).unwrap();
    assert_eq!(r1, r2);
}

#[test]
fn oracle_streams_xlen() {
    let mut redis = require_redis!();
    let redlite = Db::open_memory().unwrap();
    let _: () = redis::cmd("FLUSHDB").query(&mut redis).unwrap();

    // Empty stream
    let r1 = redlite.xlen("mystream").unwrap();
    let r2: i64 = redis::cmd("XLEN").arg("mystream").query(&mut redis).unwrap();
    assert_eq!(r1, r2);
    assert_eq!(r1, 0);

    // Add multiple entries
    for i in 0..5 {
        let field = format!("f{}", i);
        let value = format!("v{}", i);
        redlite.xadd("mystream", None, &[(field.as_bytes(), value.as_bytes())], false, None, None, false).unwrap();
        let _: String = redis::cmd("XADD").arg("mystream").arg("*").arg(&field).arg(&value).query(&mut redis).unwrap();
    }

    // Verify length
    let r1 = redlite.xlen("mystream").unwrap();
    let r2: i64 = redis::cmd("XLEN").arg("mystream").query(&mut redis).unwrap();
    assert_eq!(r1, r2);
    assert_eq!(r1, 5);
}

#[test]
fn oracle_streams_xtrim() {
    let mut redis = require_redis!();
    let redlite = Db::open_memory().unwrap();
    let _: () = redis::cmd("FLUSHDB").query(&mut redis).unwrap();

    // Add many entries
    for i in 0..20 {
        let field = format!("i{}", i);
        redlite.xadd("mystream", None, &[(field.as_bytes(), b"val")], false, None, None, false).unwrap();
        let _: String = redis::cmd("XADD").arg("mystream").arg("*").arg(&field).arg("val").query(&mut redis).unwrap();
    }

    // XTRIM MAXLEN
    let r1 = redlite.xtrim("mystream", Some(10), None, false).unwrap();
    let r2: i64 = redis::cmd("XTRIM").arg("mystream").arg("MAXLEN").arg(10).query(&mut redis).unwrap();
    assert_eq!(r1, r2);

    // Verify length
    let r1 = redlite.xlen("mystream").unwrap();
    let r2: i64 = redis::cmd("XLEN").arg("mystream").query(&mut redis).unwrap();
    assert_eq!(r1, r2);
    assert_eq!(r1, 10);
}

// ============================================================================
// SERVER ORACLE TESTS
// ============================================================================

#[test]
fn oracle_server_dbsize_flushdb() {
    let mut redis = require_redis!();
    let redlite = Db::open_memory().unwrap();
    let _: () = redis::cmd("FLUSHDB").query(&mut redis).unwrap();

    // DBSIZE empty
    let r1 = redlite.dbsize().unwrap();
    let r2: i64 = redis::cmd("DBSIZE").query(&mut redis).unwrap();
    assert_eq!(r1, r2);

    // Add some keys
    for i in 0..10 {
        let key = format!("key{}", i);
        redlite.set(&key, b"value", None).unwrap();
        let _: () = redis.set(&key, "value").unwrap();
    }

    // DBSIZE with keys
    let r1 = redlite.dbsize().unwrap();
    let r2: i64 = redis::cmd("DBSIZE").query(&mut redis).unwrap();
    assert_eq!(r1, r2);

    // FLUSHDB
    redlite.flushdb().unwrap();
    let _: () = redis::cmd("FLUSHDB").query(&mut redis).unwrap();

    let r1 = redlite.dbsize().unwrap();
    let r2: i64 = redis::cmd("DBSIZE").query(&mut redis).unwrap();
    assert_eq!(r1, r2);
}

// ============================================================================
// COMPREHENSIVE ORACLE TEST
// ============================================================================

#[test]
fn oracle_comprehensive_mixed_ops() {
    let mut redis = require_redis!();
    let redlite = Db::open_memory().unwrap();
    let _: () = redis::cmd("FLUSHDB").query(&mut redis).unwrap();

    let mut rng = ChaCha8Rng::seed_from_u64(99999);
    let mut divergences = 0;
    let ops = 2000;

    for _ in 0..ops {
        let data_type = rng.gen_range(0..6);

        match data_type {
            0 => {
                // String ops
                let key = format!("str_{}", rng.gen_range(0..10));
                let op = rng.gen_range(0..3);
                match op {
                    0 => {
                        let value = format!("v{}", rng.gen::<u32>());
                        redlite.set(&key, value.as_bytes(), None).unwrap();
                        let _: () = redis.set(&key, &value).unwrap();
                    }
                    1 => {
                        let r1 = redlite.get(&key).unwrap();
                        let r2: Option<Vec<u8>> = redis.get(&key).unwrap();
                        if r1 != r2 { divergences += 1; }
                    }
                    _ => {
                        let r1 = redlite.strlen(&key).unwrap();
                        let r2: i64 = redis.strlen(&key).unwrap();
                        if r1 != r2 { divergences += 1; }
                    }
                }
            }
            1 => {
                // List ops
                let key = format!("list_{}", rng.gen_range(0..5));
                let op = rng.gen_range(0..4);
                match op {
                    0 => {
                        let value = format!("i{}", rng.gen::<u32>());
                        let r1 = redlite.rpush(&key, &[value.as_bytes()]).unwrap();
                        let r2: i64 = redis.rpush(&key, &value).unwrap();
                        if r1 != r2 { divergences += 1; }
                    }
                    1 => {
                        let r1 = redlite.lpop(&key, None).unwrap().into_iter().next();
                        let r2: Option<Vec<u8>> = redis.lpop(&key, None).unwrap();
                        if r1 != r2 { divergences += 1; }
                    }
                    2 => {
                        let r1 = redlite.llen(&key).unwrap();
                        let r2: i64 = redis.llen(&key).unwrap();
                        if r1 != r2 { divergences += 1; }
                    }
                    _ => {
                        let r1 = redlite.lrange(&key, 0, -1).unwrap();
                        let r2: Vec<Vec<u8>> = redis.lrange(&key, 0, -1).unwrap();
                        if r1 != r2 { divergences += 1; }
                    }
                }
            }
            2 => {
                // Hash ops
                let key = format!("hash_{}", rng.gen_range(0..5));
                let field = format!("f{}", rng.gen_range(0..10));
                let op = rng.gen_range(0..3);
                match op {
                    0 => {
                        let value = format!("v{}", rng.gen::<u32>());
                        let r1 = redlite.hset(&key, &[(&field, value.as_bytes())]).unwrap() as usize;
                        let r2: usize = redis.hset(&key, &field, &value).unwrap();
                        if r1 != r2 { divergences += 1; }
                    }
                    1 => {
                        let r1 = redlite.hget(&key, &field).unwrap();
                        let r2: Option<Vec<u8>> = redis.hget(&key, &field).unwrap();
                        if r1 != r2 { divergences += 1; }
                    }
                    _ => {
                        let r1 = redlite.hlen(&key).unwrap() as usize;
                        let r2: usize = redis.hlen(&key).unwrap();
                        if r1 != r2 { divergences += 1; }
                    }
                }
            }
            3 => {
                // Set ops
                let key = format!("set_{}", rng.gen_range(0..5));
                let member = format!("m{}", rng.gen_range(0..20));
                let op = rng.gen_range(0..3);
                match op {
                    0 => {
                        let r1 = redlite.sadd(&key, &[member.as_bytes()]).unwrap() as usize;
                        let r2: usize = redis.sadd(&key, &member).unwrap();
                        if r1 != r2 { divergences += 1; }
                    }
                    1 => {
                        let r1 = redlite.sismember(&key, member.as_bytes()).unwrap();
                        let r2: bool = redis.sismember(&key, &member).unwrap();
                        if r1 != r2 { divergences += 1; }
                    }
                    _ => {
                        let r1 = redlite.scard(&key).unwrap() as usize;
                        let r2: usize = redis.scard(&key).unwrap();
                        if r1 != r2 { divergences += 1; }
                    }
                }
            }
            4 => {
                // Sorted set ops
                let key = format!("zset_{}", rng.gen_range(0..5));
                let member = format!("m{}", rng.gen_range(0..20));
                let score: f64 = rng.gen_range(0.0..100.0);
                let op = rng.gen_range(0..3);
                match op {
                    0 => {
                        let r1 = redlite.zadd(&key, &[ZMember::new(score, member.as_bytes().to_vec())]).unwrap() as usize;
                        let r2: usize = redis.zadd(&key, &member, score).unwrap();
                        if r1 != r2 { divergences += 1; }
                    }
                    1 => {
                        let r1 = redlite.zscore(&key, member.as_bytes()).unwrap();
                        let r2: Option<f64> = redis.zscore(&key, &member).unwrap();
                        if r1 != r2 { divergences += 1; }
                    }
                    _ => {
                        let r1 = redlite.zcard(&key).unwrap() as usize;
                        let r2: usize = redis.zcard(&key).unwrap();
                        if r1 != r2 { divergences += 1; }
                    }
                }
            }
            _ => {
                // Key ops
                let key = format!("key_{}", rng.gen_range(0..10));
                let op = rng.gen_range(0..3);
                match op {
                    0 => {
                        let value = format!("v{}", rng.gen::<u32>());
                        redlite.set(&key, value.as_bytes(), None).unwrap();
                        let _: () = redis.set(&key, &value).unwrap();
                    }
                    1 => {
                        let r1 = redlite.exists(&[&key]).unwrap() as usize;
                        let r2: usize = redis.exists(&key).unwrap();
                        if r1 != r2 { divergences += 1; }
                    }
                    _ => {
                        let r1 = redlite.ttl(&key).unwrap();
                        let r2: i64 = redis.ttl(&key).unwrap();
                        if r1 != r2 { divergences += 1; }
                    }
                }
            }
        }
    }

    assert_eq!(divergences, 0, "Found {} divergences in {} mixed operations", divergences, ops);
}
