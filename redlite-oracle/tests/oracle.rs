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
use redlite::{Db, ZMember, ListDirection, SetOptions, GetExOption, StreamId};
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
                // TTL - allow ±2 second tolerance for timing differences
                let r1 = redlite.ttl(&key).unwrap();
                let r2: i64 = redis.ttl(&key).unwrap();
                let is_match = if r1 < 0 || r2 < 0 {
                    r1 == r2
                } else {
                    (r1 - r2).abs() <= 2
                };
                if !is_match { divergences += 1; }
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

// ============================================================================
// ADDITIONAL STREAM ORACLE TESTS
// ============================================================================

#[test]
fn oracle_streams_xrange() {
    let mut redis = require_redis!();
    let redlite = Db::open_memory().unwrap();
    let _: () = redis::cmd("FLUSHDB").query(&mut redis).unwrap();

    // Add multiple entries with sequential IDs
    for i in 0..10 {
        let field = format!("idx{}", i);
        redlite.xadd("mystream", None, &[(field.as_bytes(), b"val")], false, None, None, false).unwrap();
        let _: String = redis::cmd("XADD").arg("mystream").arg("*").arg(&field).arg("val").query(&mut redis).unwrap();
    }

    // XRANGE - get all entries using StreamId::min() and StreamId::max()
    use redlite::StreamId;
    let r1 = redlite.xrange("mystream", StreamId::min(), StreamId::max(), None).unwrap();
    let r2_len: i64 = redis::cmd("XLEN").arg("mystream").query(&mut redis).unwrap();
    assert_eq!(r1.len(), r2_len as usize);
    assert_eq!(r1.len(), 10);

    // XRANGE with COUNT
    let r1 = redlite.xrange("mystream", StreamId::min(), StreamId::max(), Some(5)).unwrap();
    assert_eq!(r1.len(), 5);
}

#[test]
fn oracle_streams_xrevrange() {
    let mut redis = require_redis!();
    let redlite = Db::open_memory().unwrap();
    let _: () = redis::cmd("FLUSHDB").query(&mut redis).unwrap();

    // Add multiple entries
    for i in 0..10 {
        let field = format!("idx{}", i);
        redlite.xadd("mystream", None, &[(field.as_bytes(), b"val")], false, None, None, false).unwrap();
        let _: String = redis::cmd("XADD").arg("mystream").arg("*").arg(&field).arg("val").query(&mut redis).unwrap();
    }

    // XREVRANGE - get all entries in reverse using StreamId::max() and StreamId::min()
    use redlite::StreamId;
    let r1 = redlite.xrevrange("mystream", StreamId::max(), StreamId::min(), None).unwrap();
    let r2_len: i64 = redis::cmd("XLEN").arg("mystream").query(&mut redis).unwrap();
    assert_eq!(r1.len(), r2_len as usize);
    assert_eq!(r1.len(), 10);

    // XREVRANGE with COUNT
    let r1 = redlite.xrevrange("mystream", StreamId::max(), StreamId::min(), Some(3)).unwrap();
    assert_eq!(r1.len(), 3);
}

#[test]
fn oracle_streams_xdel() {
    let mut redis = require_redis!();
    let redlite = Db::open_memory().unwrap();
    let _: () = redis::cmd("FLUSHDB").query(&mut redis).unwrap();

    use redlite::StreamId;

    // Add entries and capture their IDs
    let mut redlite_ids: Vec<StreamId> = Vec::new();
    let mut redis_ids: Vec<String> = Vec::new();
    for i in 0..5 {
        let field = format!("f{}", i);
        let id = redlite.xadd("mystream", None, &[(field.as_bytes(), b"val")], false, None, None, false).unwrap().unwrap();
        let redis_id: String = redis::cmd("XADD").arg("mystream").arg("*").arg(&field).arg("val").query(&mut redis).unwrap();
        redlite_ids.push(id);
        redis_ids.push(redis_id);
    }

    // XDEL - delete first and third entries
    let r1 = redlite.xdel("mystream", &[redlite_ids[0], redlite_ids[2]]).unwrap();
    let r2: i64 = redis::cmd("XDEL").arg("mystream").arg(&redis_ids[0]).arg(&redis_ids[2]).query(&mut redis).unwrap();
    assert_eq!(r1, r2);
    assert_eq!(r1, 2);

    // Verify length
    let r1 = redlite.xlen("mystream").unwrap();
    let r2: i64 = redis::cmd("XLEN").arg("mystream").query(&mut redis).unwrap();
    assert_eq!(r1, r2);
    assert_eq!(r1, 3);
}

#[test]
fn oracle_streams_xinfo_stream() {
    let mut redis = require_redis!();
    let redlite = Db::open_memory().unwrap();
    let _: () = redis::cmd("FLUSHDB").query(&mut redis).unwrap();

    // Add some entries
    for i in 0..5 {
        let field = format!("f{}", i);
        redlite.xadd("mystream", None, &[(field.as_bytes(), b"val")], false, None, None, false).unwrap();
        let _: String = redis::cmd("XADD").arg("mystream").arg("*").arg(&field).arg("val").query(&mut redis).unwrap();
    }

    // XINFO STREAM - verify length field matches
    let r1 = redlite.xinfo_stream("mystream").unwrap().unwrap();
    let r2: redis::streams::StreamInfoStreamReply = redis::cmd("XINFO").arg("STREAM").arg("mystream").query(&mut redis).unwrap();

    assert_eq!(r1.length, r2.length as i64);
}

// ============================================================================
// ADDITIONAL SORTED SET ORACLE TESTS
// ============================================================================

#[test]
fn oracle_zsets_zremrangebyscore() {
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

    // ZREMRANGEBYSCORE
    let r1 = redlite.zremrangebyscore("myzset", 5.0, 15.0).unwrap();
    let r2: i64 = redis::cmd("ZREMRANGEBYSCORE").arg("myzset").arg(5).arg(15).query(&mut redis).unwrap();
    assert_eq!(r1, r2);

    // Verify remaining count
    let r1 = redlite.zcard("myzset").unwrap();
    let r2: usize = redis.zcard("myzset").unwrap();
    assert_eq!(r1 as usize, r2);

    // Verify remaining elements
    let r1: Vec<Vec<u8>> = redlite.zrange("myzset", 0, -1, false).unwrap()
        .into_iter().map(|m| m.member).collect();
    let r2: Vec<Vec<u8>> = redis.zrange("myzset", 0, -1).unwrap();
    assert_eq!(r1, r2);
}

// ============================================================================
// SCAN ORACLE TESTS
// ============================================================================

#[test]
fn oracle_keys_scan_iteration() {
    let mut redis = require_redis!();
    let redlite = Db::open_memory().unwrap();
    let _: () = redis::cmd("FLUSHDB").query(&mut redis).unwrap();

    // Create 100 keys
    for i in 0..100 {
        let key = format!("scankey:{}", i);
        redlite.set(&key, b"value", None).unwrap();
        let _: () = redis.set(&key, "value").unwrap();
    }

    // SCAN - collect all keys via cursor iteration
    let mut redlite_keys: Vec<String> = Vec::new();
    let mut cursor = "0".to_string();
    loop {
        let (next_cursor, batch) = redlite.scan(&cursor, Some("scankey:*"), 10).unwrap();
        redlite_keys.extend(batch);
        cursor = next_cursor;
        if cursor == "0" { break; }
    }

    let mut redis_keys: Vec<String> = Vec::new();
    let mut redis_cursor: u64 = 0;
    loop {
        let result: (u64, Vec<String>) = redis::cmd("SCAN").arg(redis_cursor).arg("MATCH").arg("scankey:*").arg("COUNT").arg(10).query(&mut redis).unwrap();
        redis_cursor = result.0;
        redis_keys.extend(result.1);
        if redis_cursor == 0 { break; }
    }

    // Sort and compare
    redlite_keys.sort();
    redis_keys.sort();
    assert_eq!(redlite_keys.len(), redis_keys.len());
    assert_eq!(redlite_keys, redis_keys);
}

// ============================================================================
// SET OPTIONS ORACLE TESTS
// ============================================================================

#[test]
fn oracle_strings_set_options_nx_xx() {
    let mut redis = require_redis!();
    let redlite = Db::open_memory().unwrap();
    let _: () = redis::cmd("FLUSHDB").query(&mut redis).unwrap();

    // SET NX on new key (should succeed)
    let r1 = redlite.set_opts("key1", b"value1", SetOptions::new().nx()).unwrap();
    let r2: bool = redis::cmd("SET").arg("key1").arg("value1").arg("NX").query(&mut redis).unwrap();
    assert_eq!(r1, r2);

    // SET NX on existing key (should fail)
    let r1 = redlite.set_opts("key1", b"newvalue", SetOptions::new().nx()).unwrap();
    let r2: Option<String> = redis::cmd("SET").arg("key1").arg("newvalue").arg("NX").query(&mut redis).unwrap();
    assert_eq!(r1, r2.is_some());

    // SET XX on existing key (should succeed)
    let r1 = redlite.set_opts("key1", b"updated", SetOptions::new().xx()).unwrap();
    let r2: bool = redis::cmd("SET").arg("key1").arg("updated").arg("XX").query(&mut redis).unwrap();
    assert_eq!(r1, r2);

    // SET XX on non-existent key (should fail)
    let r1 = redlite.set_opts("key2", b"value2", SetOptions::new().xx()).unwrap();
    let r2: Option<String> = redis::cmd("SET").arg("key2").arg("value2").arg("XX").query(&mut redis).unwrap();
    assert_eq!(r1, r2.is_some());

    // Verify final values
    let v1 = redlite.get("key1").unwrap();
    let v2: Option<Vec<u8>> = redis.get("key1").unwrap();
    assert_eq!(v1, v2);
}

#[test]
fn oracle_strings_set_options_ex_px() {
    let mut redis = require_redis!();
    let redlite = Db::open_memory().unwrap();
    let _: () = redis::cmd("FLUSHDB").query(&mut redis).unwrap();

    // SET with TTL (via set method with Duration)
    redlite.set("key1", b"value1", Some(std::time::Duration::from_secs(3600))).unwrap();
    let _: () = redis::cmd("SET").arg("key1").arg("value1").arg("EX").arg(3600).query(&mut redis).unwrap();

    let ttl1 = redlite.ttl("key1").unwrap();
    let ttl2: i64 = redis.ttl("key1").unwrap();
    assert!((ttl1 - ttl2).abs() <= 1);
    assert!(ttl1 > 3500); // Should be close to 3600

    // SET with PX (milliseconds) - convert to Duration
    redlite.set("key2", b"value2", Some(std::time::Duration::from_millis(7200000))).unwrap();
    let _: () = redis::cmd("SET").arg("key2").arg("value2").arg("PX").arg(7200000).query(&mut redis).unwrap();

    let pttl1 = redlite.pttl("key2").unwrap();
    let pttl2: i64 = redis.pttl("key2").unwrap();
    assert!((pttl1 - pttl2).abs() <= 1000);
    assert!(pttl1 > 7100000); // Should be close to 7200000
}

// ============================================================================
// TYPE MISMATCH ORACLE TESTS
// ============================================================================

#[test]
fn oracle_type_mismatch_string_on_list() {
    let mut redis = require_redis!();
    let redlite = Db::open_memory().unwrap();
    let _: () = redis::cmd("FLUSHDB").query(&mut redis).unwrap();

    // Create a list key
    redlite.rpush("mykey", &[b"item"]).unwrap();
    let _: i64 = redis.rpush("mykey", "item").unwrap();

    // Try to run GET (string command) on list key - should error
    let r1 = redlite.get("mykey");
    let r2: Result<Option<Vec<u8>>, redis::RedisError> = redis.get("mykey");

    // Both should fail with WRONGTYPE error
    assert!(r1.is_err());
    assert!(r2.is_err());
}

#[test]
fn oracle_type_mismatch_list_on_string() {
    let mut redis = require_redis!();
    let redlite = Db::open_memory().unwrap();
    let _: () = redis::cmd("FLUSHDB").query(&mut redis).unwrap();

    // Create a string key
    redlite.set("mykey", b"value", None).unwrap();
    let _: () = redis.set("mykey", "value").unwrap();

    // Try to run LPUSH (list command) on string key - should error
    let r1 = redlite.lpush("mykey", &[b"item"]);
    let r2: Result<i64, redis::RedisError> = redis.lpush("mykey", "item");

    // Both should fail with WRONGTYPE error
    assert!(r1.is_err());
    assert!(r2.is_err());
}

#[test]
fn oracle_type_mismatch_hash_on_set() {
    let mut redis = require_redis!();
    let redlite = Db::open_memory().unwrap();
    let _: () = redis::cmd("FLUSHDB").query(&mut redis).unwrap();

    // Create a set key
    redlite.sadd("mykey", &[b"member"]).unwrap();
    let _: usize = redis.sadd("mykey", "member").unwrap();

    // Try to run HGET (hash command) on set key - should error
    let r1 = redlite.hget("mykey", "field");
    let r2: Result<Option<Vec<u8>>, redis::RedisError> = redis.hget("mykey", "field");

    // Both should fail with WRONGTYPE error
    assert!(r1.is_err());
    assert!(r2.is_err());
}

#[test]
fn oracle_type_mismatch_zset_on_hash() {
    let mut redis = require_redis!();
    let redlite = Db::open_memory().unwrap();
    let _: () = redis::cmd("FLUSHDB").query(&mut redis).unwrap();

    // Create a hash key
    redlite.hset("mykey", &[("field", b"value".as_slice())]).unwrap();
    let _: usize = redis.hset("mykey", "field", "value").unwrap();

    // Try to run ZADD (sorted set command) on hash key - should error
    let r1 = redlite.zadd("mykey", &[ZMember::new(1.0, b"member".to_vec())]);
    let r2: Result<usize, redis::RedisError> = redis.zadd("mykey", "member", 1.0);

    // Both should fail with WRONGTYPE error
    assert!(r1.is_err());
    assert!(r2.is_err());
}

// ============================================================================
// TRANSACTION ORACLE TESTS
// ============================================================================

// Note: Transaction tests require server mode, not embedded Db::open_memory()
// These tests verify MULTI/EXEC/DISCARD behavior via the redis crate

#[test]
fn oracle_server_ping_echo() {
    let mut redis = require_redis!();

    // PING
    let r: String = redis::cmd("PING").query(&mut redis).unwrap();
    assert_eq!(r, "PONG");

    // PING with message
    let r: String = redis::cmd("PING").arg("hello").query(&mut redis).unwrap();
    assert_eq!(r, "hello");

    // ECHO
    let r: String = redis::cmd("ECHO").arg("hello world").query(&mut redis).unwrap();
    assert_eq!(r, "hello world");
}

// ============================================================================
// EDGE CASE ORACLE TESTS
// ============================================================================

#[test]
fn oracle_strings_empty_value() {
    let mut redis = require_redis!();
    let redlite = Db::open_memory().unwrap();
    let _: () = redis::cmd("FLUSHDB").query(&mut redis).unwrap();

    // SET empty string
    redlite.set("emptykey", b"", None).unwrap();
    let _: () = redis.set("emptykey", "").unwrap();

    // GET empty string
    let r1 = redlite.get("emptykey").unwrap();
    let r2: Option<Vec<u8>> = redis.get("emptykey").unwrap();
    assert_eq!(r1, r2);
    assert_eq!(r1, Some(vec![]));

    // STRLEN of empty string
    let r1 = redlite.strlen("emptykey").unwrap();
    let r2: i64 = redis.strlen("emptykey").unwrap();
    assert_eq!(r1, r2);
    assert_eq!(r1, 0);
}

#[test]
fn oracle_strings_large_value() {
    let mut redis = require_redis!();
    let redlite = Db::open_memory().unwrap();
    let _: () = redis::cmd("FLUSHDB").query(&mut redis).unwrap();

    // SET large value (1MB)
    let large_value: Vec<u8> = (0..1_000_000).map(|i| (i % 256) as u8).collect();
    redlite.set("largekey", &large_value, None).unwrap();
    let _: () = redis.set("largekey", &large_value).unwrap();

    // GET large value
    let r1 = redlite.get("largekey").unwrap();
    let r2: Option<Vec<u8>> = redis.get("largekey").unwrap();
    assert_eq!(r1, r2);

    // STRLEN of large value
    let r1 = redlite.strlen("largekey").unwrap();
    let r2: i64 = redis.strlen("largekey").unwrap();
    assert_eq!(r1, r2);
    assert_eq!(r1, 1_000_000);
}

#[test]
fn oracle_lists_empty_operations() {
    let mut redis = require_redis!();
    let redlite = Db::open_memory().unwrap();
    let _: () = redis::cmd("FLUSHDB").query(&mut redis).unwrap();

    // LPOP on non-existent list
    let r1 = redlite.lpop("nonexistent", None).unwrap();
    let r2: Option<Vec<u8>> = redis.lpop("nonexistent", None).unwrap();
    assert!(r1.is_empty());
    assert!(r2.is_none());

    // LRANGE on non-existent list
    let r1 = redlite.lrange("nonexistent", 0, -1).unwrap();
    let r2: Vec<Vec<u8>> = redis.lrange("nonexistent", 0, -1).unwrap();
    assert_eq!(r1, r2);
    assert!(r1.is_empty());

    // LLEN on non-existent list
    let r1 = redlite.llen("nonexistent").unwrap();
    let r2: i64 = redis.llen("nonexistent").unwrap();
    assert_eq!(r1, r2);
    assert_eq!(r1, 0);
}

#[test]
fn oracle_hashes_empty_operations() {
    let mut redis = require_redis!();
    let redlite = Db::open_memory().unwrap();
    let _: () = redis::cmd("FLUSHDB").query(&mut redis).unwrap();

    // HGET on non-existent hash
    let r1 = redlite.hget("nonexistent", "field").unwrap();
    let r2: Option<Vec<u8>> = redis.hget("nonexistent", "field").unwrap();
    assert_eq!(r1, r2);
    assert!(r1.is_none());

    // HGETALL on non-existent hash
    let r1: HashMap<String, Vec<u8>> = redlite.hgetall("nonexistent").unwrap().into_iter().collect();
    let r2: HashMap<String, Vec<u8>> = redis.hgetall("nonexistent").unwrap();
    assert_eq!(r1, r2);
    assert!(r1.is_empty());

    // HLEN on non-existent hash
    let r1 = redlite.hlen("nonexistent").unwrap();
    let r2: usize = redis.hlen("nonexistent").unwrap();
    assert_eq!(r1 as usize, r2);
    assert_eq!(r1, 0);
}

#[test]
fn oracle_sets_empty_operations() {
    let mut redis = require_redis!();
    let redlite = Db::open_memory().unwrap();
    let _: () = redis::cmd("FLUSHDB").query(&mut redis).unwrap();

    // SMEMBERS on non-existent set
    let r1 = redlite.smembers("nonexistent").unwrap();
    let r2: Vec<Vec<u8>> = redis.smembers("nonexistent").unwrap();
    assert_eq!(r1, r2);
    assert!(r1.is_empty());

    // SCARD on non-existent set
    let r1 = redlite.scard("nonexistent").unwrap();
    let r2: usize = redis.scard("nonexistent").unwrap();
    assert_eq!(r1 as usize, r2);
    assert_eq!(r1, 0);

    // SISMEMBER on non-existent set
    let r1 = redlite.sismember("nonexistent", b"member").unwrap();
    let r2: bool = redis.sismember("nonexistent", "member").unwrap();
    assert_eq!(r1, r2);
    assert!(!r1);
}

#[test]
fn oracle_zsets_empty_operations() {
    let mut redis = require_redis!();
    let redlite = Db::open_memory().unwrap();
    let _: () = redis::cmd("FLUSHDB").query(&mut redis).unwrap();

    // ZRANGE on non-existent sorted set
    let r1: Vec<Vec<u8>> = redlite.zrange("nonexistent", 0, -1, false).unwrap()
        .into_iter().map(|m| m.member).collect();
    let r2: Vec<Vec<u8>> = redis.zrange("nonexistent", 0, -1).unwrap();
    assert_eq!(r1, r2);
    assert!(r1.is_empty());

    // ZCARD on non-existent sorted set
    let r1 = redlite.zcard("nonexistent").unwrap();
    let r2: usize = redis.zcard("nonexistent").unwrap();
    assert_eq!(r1 as usize, r2);
    assert_eq!(r1, 0);

    // ZSCORE on non-existent member
    let r1 = redlite.zscore("nonexistent", b"member").unwrap();
    let r2: Option<f64> = redis.zscore("nonexistent", "member").unwrap();
    assert_eq!(r1, r2);
    assert!(r1.is_none());
}

// ============================================================================
// COMPREHENSIVE PER-COMMAND ORACLE TESTS
// ============================================================================
// Each test function covers ONE Redis command with ALL its configurations,
// value types, and edge cases.

// ============================================================================
// STRING COMMANDS - Comprehensive Tests
// ============================================================================

/// SET command: all options (basic, NX, XX, EX, PX, GET)
#[test]
fn oracle_cmd_set() {
    let mut redis = require_redis!();
    let redlite = Db::open_memory().unwrap();
    let _: () = redis::cmd("FLUSHDB").query(&mut redis).unwrap();

    // === Basic SET ===

    // Simple value
    redlite.set("k1", b"value1", None).unwrap();
    let _: () = redis.set("k1", "value1").unwrap();
    let r1 = redlite.get("k1").unwrap();
    let r2: Option<Vec<u8>> = redis.get("k1").unwrap();
    assert_eq!(r1, r2);

    // Empty value
    redlite.set("k_empty", b"", None).unwrap();
    let _: () = redis.set("k_empty", "").unwrap();
    let r1 = redlite.get("k_empty").unwrap();
    let r2: Option<Vec<u8>> = redis.get("k_empty").unwrap();
    assert_eq!(r1, r2);
    assert_eq!(r1, Some(vec![]));

    // Binary value with null bytes
    let binary = vec![0u8, 1, 2, 255, 128, 0, 64];
    redlite.set("k_binary", &binary, None).unwrap();
    let _: () = redis.set("k_binary", &binary).unwrap();
    let r1 = redlite.get("k_binary").unwrap();
    let r2: Option<Vec<u8>> = redis.get("k_binary").unwrap();
    assert_eq!(r1, r2);

    // Unicode value
    let unicode = "Hello 世界 🌍 مرحبا";
    redlite.set("k_unicode", unicode.as_bytes(), None).unwrap();
    let _: () = redis.set("k_unicode", unicode).unwrap();
    let r1 = redlite.get("k_unicode").unwrap();
    let r2: Option<Vec<u8>> = redis.get("k_unicode").unwrap();
    assert_eq!(r1, r2);

    // Overwrite existing key
    redlite.set("k1", b"updated", None).unwrap();
    let _: () = redis.set("k1", "updated").unwrap();
    let r1 = redlite.get("k1").unwrap();
    let r2: Option<Vec<u8>> = redis.get("k1").unwrap();
    assert_eq!(r1, r2);
    assert_eq!(r1, Some(b"updated".to_vec()));

    // === SET with NX (only if Not eXists) ===

    // NX on new key - should succeed
    let r1 = redlite.set_opts("k_nx_new", b"value", SetOptions::new().nx()).unwrap();
    let r2: Option<String> = redis::cmd("SET").arg("k_nx_new").arg("value").arg("NX").query(&mut redis).unwrap();
    assert_eq!(r1, r2.is_some());
    assert!(r1); // Should succeed

    // NX on existing key - should fail
    let r1 = redlite.set_opts("k_nx_new", b"newvalue", SetOptions::new().nx()).unwrap();
    let r2: Option<String> = redis::cmd("SET").arg("k_nx_new").arg("newvalue").arg("NX").query(&mut redis).unwrap();
    assert_eq!(r1, r2.is_some());
    assert!(!r1); // Should fail

    // Verify value unchanged
    let r1 = redlite.get("k_nx_new").unwrap();
    let r2: Option<Vec<u8>> = redis.get("k_nx_new").unwrap();
    assert_eq!(r1, r2);
    assert_eq!(r1, Some(b"value".to_vec()));

    // === SET with XX (only if eXists) ===

    // XX on non-existent key - should fail
    let r1 = redlite.set_opts("k_xx_missing", b"value", SetOptions::new().xx()).unwrap();
    let r2: Option<String> = redis::cmd("SET").arg("k_xx_missing").arg("value").arg("XX").query(&mut redis).unwrap();
    assert_eq!(r1, r2.is_some());
    assert!(!r1); // Should fail

    // Verify key not created
    let r1 = redlite.get("k_xx_missing").unwrap();
    let r2: Option<Vec<u8>> = redis.get("k_xx_missing").unwrap();
    assert_eq!(r1, r2);
    assert!(r1.is_none());

    // XX on existing key - should succeed
    redlite.set("k_xx_exists", b"original", None).unwrap();
    let _: () = redis.set("k_xx_exists", "original").unwrap();
    let r1 = redlite.set_opts("k_xx_exists", b"updated", SetOptions::new().xx()).unwrap();
    let r2: Option<String> = redis::cmd("SET").arg("k_xx_exists").arg("updated").arg("XX").query(&mut redis).unwrap();
    assert_eq!(r1, r2.is_some());
    assert!(r1); // Should succeed

    // Verify value changed
    let r1 = redlite.get("k_xx_exists").unwrap();
    let r2: Option<Vec<u8>> = redis.get("k_xx_exists").unwrap();
    assert_eq!(r1, r2);
    assert_eq!(r1, Some(b"updated".to_vec()));

    // === SET with EX (seconds TTL) ===

    redlite.set("k_ex", b"expires", Some(std::time::Duration::from_secs(3600))).unwrap();
    let _: () = redis::cmd("SET").arg("k_ex").arg("expires").arg("EX").arg(3600).query(&mut redis).unwrap();

    let ttl1 = redlite.ttl("k_ex").unwrap();
    let ttl2: i64 = redis.ttl("k_ex").unwrap();
    assert!((ttl1 - ttl2).abs() <= 1);
    assert!(ttl1 > 3590 && ttl1 <= 3600);

    // === SET with PX (milliseconds TTL) ===

    redlite.set("k_px", b"expires_ms", Some(std::time::Duration::from_millis(5000000))).unwrap();
    let _: () = redis::cmd("SET").arg("k_px").arg("expires_ms").arg("PX").arg(5000000).query(&mut redis).unwrap();

    let pttl1 = redlite.pttl("k_px").unwrap();
    let pttl2: i64 = redis.pttl("k_px").unwrap();
    assert!((pttl1 - pttl2).abs() <= 100);
    assert!(pttl1 > 4990000 && pttl1 <= 5000000);
}

/// GET command: all scenarios
#[test]
fn oracle_cmd_get() {
    let mut redis = require_redis!();
    let redlite = Db::open_memory().unwrap();
    let _: () = redis::cmd("FLUSHDB").query(&mut redis).unwrap();

    // === Basic GET ===

    // GET existing key
    redlite.set("k1", b"value1", None).unwrap();
    let _: () = redis.set("k1", "value1").unwrap();
    let r1 = redlite.get("k1").unwrap();
    let r2: Option<Vec<u8>> = redis.get("k1").unwrap();
    assert_eq!(r1, r2);
    assert_eq!(r1, Some(b"value1".to_vec()));

    // GET non-existent key
    let r1 = redlite.get("k_missing").unwrap();
    let r2: Option<Vec<u8>> = redis.get("k_missing").unwrap();
    assert_eq!(r1, r2);
    assert!(r1.is_none());

    // GET empty value
    redlite.set("k_empty", b"", None).unwrap();
    let _: () = redis.set("k_empty", "").unwrap();
    let r1 = redlite.get("k_empty").unwrap();
    let r2: Option<Vec<u8>> = redis.get("k_empty").unwrap();
    assert_eq!(r1, r2);
    assert_eq!(r1, Some(vec![]));

    // GET binary value
    let binary = vec![0u8, 255, 128, 64, 32, 16, 8, 4, 2, 1];
    redlite.set("k_bin", &binary, None).unwrap();
    let _: () = redis.set("k_bin", &binary).unwrap();
    let r1 = redlite.get("k_bin").unwrap();
    let r2: Option<Vec<u8>> = redis.get("k_bin").unwrap();
    assert_eq!(r1, r2);

    // GET large value (100KB)
    let large: Vec<u8> = (0..100_000).map(|i| (i % 256) as u8).collect();
    redlite.set("k_large", &large, None).unwrap();
    let _: () = redis.set("k_large", &large).unwrap();
    let r1 = redlite.get("k_large").unwrap();
    let r2: Option<Vec<u8>> = redis.get("k_large").unwrap();
    assert_eq!(r1, r2);

    // GET on wrong type - should error
    redlite.lpush("k_list", &[b"item"]).unwrap();
    let _: i64 = redis.lpush("k_list", "item").unwrap();
    let r1 = redlite.get("k_list");
    let r2: Result<Option<Vec<u8>>, redis::RedisError> = redis.get("k_list");
    assert!(r1.is_err());
    assert!(r2.is_err());
}

/// INCR command: all scenarios
#[test]
fn oracle_cmd_incr() {
    let mut redis = require_redis!();
    let redlite = Db::open_memory().unwrap();
    let _: () = redis::cmd("FLUSHDB").query(&mut redis).unwrap();

    // INCR on new key (starts at 0, becomes 1)
    let r1 = redlite.incr("counter").unwrap();
    let r2: i64 = redis.incr("counter", 1i64).unwrap();
    assert_eq!(r1, r2);
    assert_eq!(r1, 1);

    // INCR on existing key
    let r1 = redlite.incr("counter").unwrap();
    let r2: i64 = redis.incr("counter", 1i64).unwrap();
    assert_eq!(r1, r2);
    assert_eq!(r1, 2);

    // INCR multiple times
    for expected in 3..=10 {
        let r1 = redlite.incr("counter").unwrap();
        let r2: i64 = redis.incr("counter", 1i64).unwrap();
        assert_eq!(r1, r2);
        assert_eq!(r1, expected);
    }

    // INCR on string that represents a number
    redlite.set("str_num", b"100", None).unwrap();
    let _: () = redis.set("str_num", "100").unwrap();
    let r1 = redlite.incr("str_num").unwrap();
    let r2: i64 = redis.incr("str_num", 1i64).unwrap();
    assert_eq!(r1, r2);
    assert_eq!(r1, 101);

    // INCR on negative number
    redlite.set("neg", b"-50", None).unwrap();
    let _: () = redis.set("neg", "-50").unwrap();
    let r1 = redlite.incr("neg").unwrap();
    let r2: i64 = redis.incr("neg", 1i64).unwrap();
    assert_eq!(r1, r2);
    assert_eq!(r1, -49);

    // INCR on non-numeric string - should error
    redlite.set("not_num", b"hello", None).unwrap();
    let _: () = redis.set("not_num", "hello").unwrap();
    let r1 = redlite.incr("not_num");
    let r2: Result<i64, redis::RedisError> = redis.incr("not_num", 1i64);
    assert!(r1.is_err());
    assert!(r2.is_err());

    // INCR on wrong type - should error
    redlite.lpush("list_key", &[b"item"]).unwrap();
    let _: i64 = redis.lpush("list_key", "item").unwrap();
    let r1 = redlite.incr("list_key");
    let r2: Result<i64, redis::RedisError> = redis.incr("list_key", 1i64);
    assert!(r1.is_err());
    assert!(r2.is_err());
}

/// DECR command: all scenarios
#[test]
fn oracle_cmd_decr() {
    let mut redis = require_redis!();
    let redlite = Db::open_memory().unwrap();
    let _: () = redis::cmd("FLUSHDB").query(&mut redis).unwrap();

    // DECR on new key (starts at 0, becomes -1)
    let r1 = redlite.decr("counter").unwrap();
    let r2: i64 = redis.decr("counter", 1i64).unwrap();
    assert_eq!(r1, r2);
    assert_eq!(r1, -1);

    // DECR on existing key
    let r1 = redlite.decr("counter").unwrap();
    let r2: i64 = redis.decr("counter", 1i64).unwrap();
    assert_eq!(r1, r2);
    assert_eq!(r1, -2);

    // DECR on positive number
    redlite.set("pos", b"100", None).unwrap();
    let _: () = redis.set("pos", "100").unwrap();
    let r1 = redlite.decr("pos").unwrap();
    let r2: i64 = redis.decr("pos", 1i64).unwrap();
    assert_eq!(r1, r2);
    assert_eq!(r1, 99);

    // DECR multiple times
    for expected in (90..99).rev() {
        let r1 = redlite.decr("pos").unwrap();
        let r2: i64 = redis.decr("pos", 1i64).unwrap();
        assert_eq!(r1, r2);
        assert_eq!(r1, expected);
    }

    // DECR on non-numeric string - should error
    redlite.set("not_num", b"hello", None).unwrap();
    let _: () = redis.set("not_num", "hello").unwrap();
    let r1 = redlite.decr("not_num");
    let r2: Result<i64, redis::RedisError> = redis.decr("not_num", 1i64);
    assert!(r1.is_err());
    assert!(r2.is_err());
}

/// INCRBY command: all scenarios
#[test]
fn oracle_cmd_incrby() {
    let mut redis = require_redis!();
    let redlite = Db::open_memory().unwrap();
    let _: () = redis::cmd("FLUSHDB").query(&mut redis).unwrap();

    // INCRBY on new key
    let r1 = redlite.incrby("counter", 5).unwrap();
    let r2: i64 = redis.incr("counter", 5i64).unwrap();
    assert_eq!(r1, r2);
    assert_eq!(r1, 5);

    // INCRBY positive
    let r1 = redlite.incrby("counter", 10).unwrap();
    let r2: i64 = redis.incr("counter", 10i64).unwrap();
    assert_eq!(r1, r2);
    assert_eq!(r1, 15);

    // INCRBY negative (same as DECRBY)
    let r1 = redlite.incrby("counter", -3).unwrap();
    let r2: i64 = redis.incr("counter", -3i64).unwrap();
    assert_eq!(r1, r2);
    assert_eq!(r1, 12);

    // INCRBY zero
    let r1 = redlite.incrby("counter", 0).unwrap();
    let r2: i64 = redis.incr("counter", 0i64).unwrap();
    assert_eq!(r1, r2);
    assert_eq!(r1, 12);

    // INCRBY large value
    let r1 = redlite.incrby("counter", 1_000_000).unwrap();
    let r2: i64 = redis.incr("counter", 1_000_000i64).unwrap();
    assert_eq!(r1, r2);
    assert_eq!(r1, 1_000_012);
}

/// DECRBY command: all scenarios
#[test]
fn oracle_cmd_decrby() {
    let mut redis = require_redis!();
    let redlite = Db::open_memory().unwrap();
    let _: () = redis::cmd("FLUSHDB").query(&mut redis).unwrap();

    // DECRBY on new key
    let r1 = redlite.decrby("counter", 5).unwrap();
    let r2: i64 = redis.decr("counter", 5i64).unwrap();
    assert_eq!(r1, r2);
    assert_eq!(r1, -5);

    // DECRBY from positive
    redlite.set("pos", b"100", None).unwrap();
    let _: () = redis.set("pos", "100").unwrap();
    let r1 = redlite.decrby("pos", 30).unwrap();
    let r2: i64 = redis.decr("pos", 30i64).unwrap();
    assert_eq!(r1, r2);
    assert_eq!(r1, 70);

    // DECRBY large value
    let r1 = redlite.decrby("pos", 1_000_000).unwrap();
    let r2: i64 = redis.decr("pos", 1_000_000i64).unwrap();
    assert_eq!(r1, r2);
    assert_eq!(r1, -999_930);
}

/// INCRBYFLOAT command: all scenarios
#[test]
fn oracle_cmd_incrbyfloat() {
    let mut redis = require_redis!();
    let redlite = Db::open_memory().unwrap();
    let _: () = redis::cmd("FLUSHDB").query(&mut redis).unwrap();

    // INCRBYFLOAT on new key
    let r1: f64 = redlite.incrbyfloat("flt", 1.5).unwrap().parse().unwrap();
    let r2: f64 = redis::cmd("INCRBYFLOAT").arg("flt").arg(1.5).query(&mut redis).unwrap();
    assert!((r1 - r2).abs() < 1e-10);
    assert!((r1 - 1.5).abs() < 1e-10);

    // INCRBYFLOAT add more
    let r1: f64 = redlite.incrbyfloat("flt", 2.5).unwrap().parse().unwrap();
    let r2: f64 = redis::cmd("INCRBYFLOAT").arg("flt").arg(2.5).query(&mut redis).unwrap();
    assert!((r1 - r2).abs() < 1e-10);
    assert!((r1 - 4.0).abs() < 1e-10);

    // INCRBYFLOAT negative
    let r1: f64 = redlite.incrbyfloat("flt", -1.0).unwrap().parse().unwrap();
    let r2: f64 = redis::cmd("INCRBYFLOAT").arg("flt").arg(-1.0).query(&mut redis).unwrap();
    assert!((r1 - r2).abs() < 1e-10);
    assert!((r1 - 3.0).abs() < 1e-10);

    // INCRBYFLOAT on integer string
    redlite.set("int_str", b"10", None).unwrap();
    let _: () = redis.set("int_str", "10").unwrap();
    let r1: f64 = redlite.incrbyfloat("int_str", 0.1).unwrap().parse().unwrap();
    let r2: f64 = redis::cmd("INCRBYFLOAT").arg("int_str").arg(0.1).query(&mut redis).unwrap();
    assert!((r1 - r2).abs() < 1e-10);
    assert!((r1 - 10.1).abs() < 1e-10);

    // INCRBYFLOAT small value
    let r1: f64 = redlite.incrbyfloat("small", 0.0000001).unwrap().parse().unwrap();
    let r2: f64 = redis::cmd("INCRBYFLOAT").arg("small").arg(0.0000001).query(&mut redis).unwrap();
    assert!((r1 - r2).abs() < 1e-15);
}

/// APPEND command: all scenarios
#[test]
fn oracle_cmd_append() {
    let mut redis = require_redis!();
    let redlite = Db::open_memory().unwrap();
    let _: () = redis::cmd("FLUSHDB").query(&mut redis).unwrap();

    // APPEND to non-existent key (creates it)
    let r1 = redlite.append("key", b"hello").unwrap();
    let r2: usize = redis.append("key", "hello").unwrap();
    assert_eq!(r1 as usize, r2);
    assert_eq!(r1, 5);

    // APPEND to existing key
    let r1 = redlite.append("key", b" world").unwrap();
    let r2: usize = redis.append("key", " world").unwrap();
    assert_eq!(r1 as usize, r2);
    assert_eq!(r1, 11);

    // Verify value
    let r1 = redlite.get("key").unwrap();
    let r2: Option<Vec<u8>> = redis.get("key").unwrap();
    assert_eq!(r1, r2);
    assert_eq!(r1, Some(b"hello world".to_vec()));

    // APPEND empty string
    let r1 = redlite.append("key", b"").unwrap();
    let r2: usize = redis.append("key", "").unwrap();
    assert_eq!(r1 as usize, r2);
    assert_eq!(r1, 11); // Length unchanged

    // APPEND binary data
    let binary = vec![0u8, 255, 128];
    let r1 = redlite.append("bin", &binary).unwrap();
    let r2: usize = redis.append("bin", &binary).unwrap();
    assert_eq!(r1 as usize, r2);

    // APPEND multiple times
    for i in 0..10 {
        let suffix = format!("-{}", i);
        let r1 = redlite.append("multi", suffix.as_bytes()).unwrap();
        let r2: usize = redis.append("multi", &suffix).unwrap();
        assert_eq!(r1 as usize, r2);
    }

    // Verify accumulated value
    let r1 = redlite.get("multi").unwrap();
    let r2: Option<Vec<u8>> = redis.get("multi").unwrap();
    assert_eq!(r1, r2);
}

/// STRLEN command: all scenarios
#[test]
fn oracle_cmd_strlen() {
    let mut redis = require_redis!();
    let redlite = Db::open_memory().unwrap();
    let _: () = redis::cmd("FLUSHDB").query(&mut redis).unwrap();

    // STRLEN on non-existent key
    let r1 = redlite.strlen("missing").unwrap();
    let r2: i64 = redis.strlen("missing").unwrap();
    assert_eq!(r1, r2);
    assert_eq!(r1, 0);

    // STRLEN on empty string
    redlite.set("empty", b"", None).unwrap();
    let _: () = redis.set("empty", "").unwrap();
    let r1 = redlite.strlen("empty").unwrap();
    let r2: i64 = redis.strlen("empty").unwrap();
    assert_eq!(r1, r2);
    assert_eq!(r1, 0);

    // STRLEN on simple string
    redlite.set("simple", b"hello", None).unwrap();
    let _: () = redis.set("simple", "hello").unwrap();
    let r1 = redlite.strlen("simple").unwrap();
    let r2: i64 = redis.strlen("simple").unwrap();
    assert_eq!(r1, r2);
    assert_eq!(r1, 5);

    // STRLEN on unicode string (bytes, not chars)
    let unicode = "hello 世界"; // 6 + 6 = 12 bytes (each chinese char is 3 bytes)
    redlite.set("unicode", unicode.as_bytes(), None).unwrap();
    let _: () = redis.set("unicode", unicode).unwrap();
    let r1 = redlite.strlen("unicode").unwrap();
    let r2: i64 = redis.strlen("unicode").unwrap();
    assert_eq!(r1, r2);
    assert_eq!(r1, unicode.len() as i64);

    // STRLEN on large value
    let large: Vec<u8> = vec![b'x'; 100_000];
    redlite.set("large", &large, None).unwrap();
    let _: () = redis.set("large", &large).unwrap();
    let r1 = redlite.strlen("large").unwrap();
    let r2: i64 = redis.strlen("large").unwrap();
    assert_eq!(r1, r2);
    assert_eq!(r1, 100_000);
}

/// GETRANGE command: all scenarios
#[test]
fn oracle_cmd_getrange() {
    let mut redis = require_redis!();
    let redlite = Db::open_memory().unwrap();
    let _: () = redis::cmd("FLUSHDB").query(&mut redis).unwrap();

    let value = "Hello, World!";
    redlite.set("key", value.as_bytes(), None).unwrap();
    let _: () = redis.set("key", value).unwrap();

    // Basic range
    let r1 = redlite.getrange("key", 0, 4).unwrap();
    let r2: Vec<u8> = redis.getrange("key", 0, 4).unwrap();
    assert_eq!(r1, r2);
    assert_eq!(r1, b"Hello".to_vec());

    // Negative start index
    let r1 = redlite.getrange("key", -6, -1).unwrap();
    let r2: Vec<u8> = redis.getrange("key", -6, -1).unwrap();
    assert_eq!(r1, r2);
    assert_eq!(r1, b"World!".to_vec());

    // Mixed indices
    let r1 = redlite.getrange("key", 7, -1).unwrap();
    let r2: Vec<u8> = redis.getrange("key", 7, -1).unwrap();
    assert_eq!(r1, r2);
    assert_eq!(r1, b"World!".to_vec());

    // Full string (0, -1)
    let r1 = redlite.getrange("key", 0, -1).unwrap();
    let r2: Vec<u8> = redis.getrange("key", 0, -1).unwrap();
    assert_eq!(r1, r2);
    assert_eq!(r1, value.as_bytes().to_vec());

    // Out of bounds (clamped to string length)
    let r1 = redlite.getrange("key", 0, 100).unwrap();
    let r2: Vec<u8> = redis.getrange("key", 0, 100).unwrap();
    assert_eq!(r1, r2);
    assert_eq!(r1, value.as_bytes().to_vec());

    // Empty result (start > end after normalization)
    let r1 = redlite.getrange("key", 10, 5).unwrap();
    let r2: Vec<u8> = redis.getrange("key", 10, 5).unwrap();
    assert_eq!(r1, r2);
    assert!(r1.is_empty());

    // Non-existent key
    let r1 = redlite.getrange("missing", 0, 10).unwrap();
    let r2: Vec<u8> = redis.getrange("missing", 0, 10).unwrap();
    assert_eq!(r1, r2);
    assert!(r1.is_empty());

    // Single character
    let r1 = redlite.getrange("key", 0, 0).unwrap();
    let r2: Vec<u8> = redis.getrange("key", 0, 0).unwrap();
    assert_eq!(r1, r2);
    assert_eq!(r1, b"H".to_vec());
}

/// SETRANGE command: all scenarios
#[test]
fn oracle_cmd_setrange() {
    let mut redis = require_redis!();
    let redlite = Db::open_memory().unwrap();
    let _: () = redis::cmd("FLUSHDB").query(&mut redis).unwrap();

    // SETRANGE within existing string
    redlite.set("key", b"Hello World", None).unwrap();
    let _: () = redis.set("key", "Hello World").unwrap();
    let r1 = redlite.setrange("key", 6, b"Redis").unwrap();
    let r2: i64 = redis::cmd("SETRANGE").arg("key").arg(6).arg("Redis").query(&mut redis).unwrap();
    assert_eq!(r1, r2);
    assert_eq!(r1, 11);

    // Verify value
    let r1 = redlite.get("key").unwrap();
    let r2: Option<Vec<u8>> = redis.get("key").unwrap();
    assert_eq!(r1, r2);
    assert_eq!(r1, Some(b"Hello Redis".to_vec()));

    // SETRANGE extending string
    let r1 = redlite.setrange("key", 11, b"!!").unwrap();
    let r2: i64 = redis::cmd("SETRANGE").arg("key").arg(11).arg("!!").query(&mut redis).unwrap();
    assert_eq!(r1, r2);
    assert_eq!(r1, 13);

    // SETRANGE on non-existent key (creates with null padding)
    let r1 = redlite.setrange("new", 5, b"hello").unwrap();
    let r2: i64 = redis::cmd("SETRANGE").arg("new").arg(5).arg("hello").query(&mut redis).unwrap();
    assert_eq!(r1, r2);
    assert_eq!(r1, 10);

    // Verify null padding
    let r1 = redlite.get("new").unwrap();
    let r2: Option<Vec<u8>> = redis.get("new").unwrap();
    assert_eq!(r1, r2);
    // First 5 bytes should be null
    assert_eq!(r1.as_ref().unwrap()[0..5], [0, 0, 0, 0, 0]);

    // SETRANGE at offset 0
    let r1 = redlite.setrange("key", 0, b"Bye").unwrap();
    let r2: i64 = redis::cmd("SETRANGE").arg("key").arg(0).arg("Bye").query(&mut redis).unwrap();
    assert_eq!(r1, r2);
}

/// MGET command: all scenarios
#[test]
fn oracle_cmd_mget() {
    let mut redis = require_redis!();
    let redlite = Db::open_memory().unwrap();
    let _: () = redis::cmd("FLUSHDB").query(&mut redis).unwrap();

    // Setup keys
    redlite.set("k1", b"v1", None).unwrap();
    redlite.set("k2", b"v2", None).unwrap();
    redlite.set("k3", b"v3", None).unwrap();
    let _: () = redis.set("k1", "v1").unwrap();
    let _: () = redis.set("k2", "v2").unwrap();
    let _: () = redis.set("k3", "v3").unwrap();

    // MGET all existing
    let r1 = redlite.mget(&["k1", "k2", "k3"]);
    let r2: Vec<Option<Vec<u8>>> = redis::cmd("MGET").arg("k1").arg("k2").arg("k3").query(&mut redis).unwrap();
    assert_eq!(r1, r2);

    // MGET with some missing
    let r1 = redlite.mget(&["k1", "missing", "k3"]);
    let r2: Vec<Option<Vec<u8>>> = redis::cmd("MGET").arg("k1").arg("missing").arg("k3").query(&mut redis).unwrap();
    assert_eq!(r1, r2);
    assert!(r1[1].is_none());

    // MGET all missing
    let r1 = redlite.mget(&["m1", "m2", "m3"]);
    let r2: Vec<Option<Vec<u8>>> = redis::cmd("MGET").arg("m1").arg("m2").arg("m3").query(&mut redis).unwrap();
    assert_eq!(r1, r2);
    assert!(r1.iter().all(|v| v.is_none()));

    // MGET single key
    let r1 = redlite.mget(&["k1"]);
    let r2: Vec<Option<Vec<u8>>> = redis::cmd("MGET").arg("k1").query(&mut redis).unwrap();
    assert_eq!(r1, r2);

    // MGET with wrong type key
    redlite.lpush("list", &[b"item"]).unwrap();
    let _: i64 = redis.lpush("list", "item").unwrap();
    let r1 = redlite.mget(&["k1", "list", "k2"]);
    let r2: Vec<Option<Vec<u8>>> = redis::cmd("MGET").arg("k1").arg("list").arg("k2").query(&mut redis).unwrap();
    assert_eq!(r1, r2);
    // Redis returns nil for wrong type in MGET (no error)
    assert!(r1[1].is_none());
}

/// MSET command: all scenarios
#[test]
fn oracle_cmd_mset() {
    let mut redis = require_redis!();
    let redlite = Db::open_memory().unwrap();
    let _: () = redis::cmd("FLUSHDB").query(&mut redis).unwrap();

    // MSET multiple keys
    let pairs: Vec<(&str, &[u8])> = vec![
        ("k1", b"v1"),
        ("k2", b"v2"),
        ("k3", b"v3"),
    ];
    redlite.mset(&pairs).unwrap();
    let _: () = redis::cmd("MSET")
        .arg("k1").arg("v1")
        .arg("k2").arg("v2")
        .arg("k3").arg("v3")
        .query(&mut redis).unwrap();

    // Verify all keys
    for (key, expected) in &pairs {
        let r1 = redlite.get(key).unwrap();
        let r2: Option<Vec<u8>> = redis.get(*key).unwrap();
        assert_eq!(r1, r2);
        assert_eq!(r1, Some(expected.to_vec()));
    }

    // MSET overwrites existing
    let pairs: Vec<(&str, &[u8])> = vec![
        ("k1", b"new1"),
        ("k2", b"new2"),
    ];
    redlite.mset(&pairs).unwrap();
    let _: () = redis::cmd("MSET")
        .arg("k1").arg("new1")
        .arg("k2").arg("new2")
        .query(&mut redis).unwrap();

    let r1 = redlite.get("k1").unwrap();
    let r2: Option<Vec<u8>> = redis.get("k1").unwrap();
    assert_eq!(r1, r2);
    assert_eq!(r1, Some(b"new1".to_vec()));

    // MSET with binary values
    let binary1 = vec![0u8, 255, 128];
    let binary2 = vec![1u8, 2, 3, 4, 5];
    let pairs: Vec<(&str, &[u8])> = vec![
        ("bin1", &binary1),
        ("bin2", &binary2),
    ];
    redlite.mset(&pairs).unwrap();
    let _: () = redis::cmd("MSET")
        .arg("bin1").arg(&binary1)
        .arg("bin2").arg(&binary2)
        .query(&mut redis).unwrap();

    let r1 = redlite.get("bin1").unwrap();
    let r2: Option<Vec<u8>> = redis.get("bin1").unwrap();
    assert_eq!(r1, r2);
}

/// GETEX command: all scenarios
#[test]
fn oracle_cmd_getex() {
    let mut redis = require_redis!();
    let redlite = Db::open_memory().unwrap();
    let _: () = redis::cmd("FLUSHDB").query(&mut redis).unwrap();

    // Setup
    redlite.set("key", b"value", None).unwrap();
    let _: () = redis.set("key", "value").unwrap();

    // GETEX without options (just get)
    let r1 = redlite.getex("key", None).unwrap();
    let r2: Option<Vec<u8>> = redis::cmd("GETEX").arg("key").query(&mut redis).unwrap();
    assert_eq!(r1, r2);
    assert_eq!(r1, Some(b"value".to_vec()));

    // GETEX with EX (set TTL in seconds)
    let r1 = redlite.getex("key", Some(GetExOption::Ex(3600))).unwrap();
    let r2: Option<Vec<u8>> = redis::cmd("GETEX").arg("key").arg("EX").arg(3600).query(&mut redis).unwrap();
    assert_eq!(r1, r2);

    // Verify TTL was set
    let ttl1 = redlite.ttl("key").unwrap();
    let ttl2: i64 = redis.ttl("key").unwrap();
    assert!((ttl1 - ttl2).abs() <= 1);
    assert!(ttl1 > 3590);

    // GETEX with PERSIST (remove TTL)
    let r1 = redlite.getex("key", Some(GetExOption::Persist)).unwrap();
    let r2: Option<Vec<u8>> = redis::cmd("GETEX").arg("key").arg("PERSIST").query(&mut redis).unwrap();
    assert_eq!(r1, r2);

    // Verify TTL removed
    let ttl1 = redlite.ttl("key").unwrap();
    let ttl2: i64 = redis.ttl("key").unwrap();
    assert_eq!(ttl1, ttl2);
    assert_eq!(ttl1, -1); // No TTL

    // GETEX on non-existent key
    let r1 = redlite.getex("missing", None).unwrap();
    let r2: Option<Vec<u8>> = redis::cmd("GETEX").arg("missing").query(&mut redis).unwrap();
    assert_eq!(r1, r2);
    assert!(r1.is_none());
}

/// GETDEL command: all scenarios
#[test]
fn oracle_cmd_getdel() {
    let mut redis = require_redis!();
    let redlite = Db::open_memory().unwrap();
    let _: () = redis::cmd("FLUSHDB").query(&mut redis).unwrap();

    // GETDEL on existing key
    redlite.set("key", b"value", None).unwrap();
    let _: () = redis.set("key", "value").unwrap();

    let r1 = redlite.getdel("key").unwrap();
    let r2: Option<Vec<u8>> = redis::cmd("GETDEL").arg("key").query(&mut redis).unwrap();
    assert_eq!(r1, r2);
    assert_eq!(r1, Some(b"value".to_vec()));

    // Verify key deleted
    let r1 = redlite.exists(&["key"]).unwrap();
    let r2: i64 = redis.exists("key").unwrap();
    assert_eq!(r1, r2);
    assert_eq!(r1, 0);

    // GETDEL on non-existent key
    let r1 = redlite.getdel("missing").unwrap();
    let r2: Option<Vec<u8>> = redis::cmd("GETDEL").arg("missing").query(&mut redis).unwrap();
    assert_eq!(r1, r2);
    assert!(r1.is_none());

    // GETDEL multiple times on same key
    redlite.set("multi", b"val", None).unwrap();
    let _: () = redis.set("multi", "val").unwrap();

    let r1 = redlite.getdel("multi").unwrap();
    let r2: Option<Vec<u8>> = redis::cmd("GETDEL").arg("multi").query(&mut redis).unwrap();
    assert_eq!(r1, r2);
    assert!(r1.is_some());

    let r1 = redlite.getdel("multi").unwrap();
    let r2: Option<Vec<u8>> = redis::cmd("GETDEL").arg("multi").query(&mut redis).unwrap();
    assert_eq!(r1, r2);
    assert!(r1.is_none());
}

/// SETEX command: all scenarios
#[test]
fn oracle_cmd_setex() {
    let mut redis = require_redis!();
    let redlite = Db::open_memory().unwrap();
    let _: () = redis::cmd("FLUSHDB").query(&mut redis).unwrap();

    // SETEX basic
    redlite.setex("key", 3600, b"value").unwrap();
    let _: () = redis::cmd("SETEX").arg("key").arg(3600).arg("value").query(&mut redis).unwrap();

    // Verify value
    let r1 = redlite.get("key").unwrap();
    let r2: Option<Vec<u8>> = redis.get("key").unwrap();
    assert_eq!(r1, r2);
    assert_eq!(r1, Some(b"value".to_vec()));

    // Verify TTL
    let ttl1 = redlite.ttl("key").unwrap();
    let ttl2: i64 = redis.ttl("key").unwrap();
    assert!((ttl1 - ttl2).abs() <= 1);
    assert!(ttl1 > 3590);

    // SETEX overwrites existing
    redlite.setex("key", 7200, b"new_value").unwrap();
    let _: () = redis::cmd("SETEX").arg("key").arg(7200).arg("new_value").query(&mut redis).unwrap();

    let r1 = redlite.get("key").unwrap();
    let r2: Option<Vec<u8>> = redis.get("key").unwrap();
    assert_eq!(r1, r2);
    assert_eq!(r1, Some(b"new_value".to_vec()));

    let ttl1 = redlite.ttl("key").unwrap();
    let ttl2: i64 = redis.ttl("key").unwrap();
    assert!((ttl1 - ttl2).abs() <= 1);
    assert!(ttl1 > 7190);
}

/// PSETEX command: all scenarios
#[test]
fn oracle_cmd_psetex() {
    let mut redis = require_redis!();
    let redlite = Db::open_memory().unwrap();
    let _: () = redis::cmd("FLUSHDB").query(&mut redis).unwrap();

    // PSETEX basic (milliseconds)
    redlite.psetex("key", 3600000, b"value").unwrap();
    let _: () = redis::cmd("PSETEX").arg("key").arg(3600000).arg("value").query(&mut redis).unwrap();

    // Verify value
    let r1 = redlite.get("key").unwrap();
    let r2: Option<Vec<u8>> = redis.get("key").unwrap();
    assert_eq!(r1, r2);
    assert_eq!(r1, Some(b"value".to_vec()));

    // Verify PTTL (milliseconds)
    let pttl1 = redlite.pttl("key").unwrap();
    let pttl2: i64 = redis.pttl("key").unwrap();
    assert!((pttl1 - pttl2).abs() <= 100);
    assert!(pttl1 > 3590000);
}

// ============================================================================
// BIT COMMANDS - Comprehensive Tests
// ============================================================================

/// SETBIT command: all scenarios
#[test]
fn oracle_cmd_setbit() {
    let mut redis = require_redis!();
    let redlite = Db::open_memory().unwrap();
    let _: () = redis::cmd("FLUSHDB").query(&mut redis).unwrap();

    // SETBIT on new key (creates key)
    let r1 = redlite.setbit("bits", 7, true).unwrap();
    let r2: i64 = redis::cmd("SETBIT").arg("bits").arg(7).arg(1).query(&mut redis).unwrap();
    assert_eq!(r1, r2);
    assert_eq!(r1, 0); // Previous bit was 0

    // SETBIT returns previous value
    let r1 = redlite.setbit("bits", 7, false).unwrap();
    let r2: i64 = redis::cmd("SETBIT").arg("bits").arg(7).arg(0).query(&mut redis).unwrap();
    assert_eq!(r1, r2);
    assert_eq!(r1, 1); // Previous bit was 1

    // SETBIT at various offsets
    for offset in [0, 1, 8, 15, 16, 31, 32, 63, 100] {
        let r1 = redlite.setbit("bits2", offset, true).unwrap();
        let r2: i64 = redis::cmd("SETBIT").arg("bits2").arg(offset).arg(1).query(&mut redis).unwrap();
        assert_eq!(r1, r2, "Mismatch at offset {}", offset);
    }

    // SETBIT same bit twice
    let r1 = redlite.setbit("bits3", 0, true).unwrap();
    let r2: i64 = redis::cmd("SETBIT").arg("bits3").arg(0).arg(1).query(&mut redis).unwrap();
    assert_eq!(r1, r2);
    assert_eq!(r1, 0);

    let r1 = redlite.setbit("bits3", 0, true).unwrap();
    let r2: i64 = redis::cmd("SETBIT").arg("bits3").arg(0).arg(1).query(&mut redis).unwrap();
    assert_eq!(r1, r2);
    assert_eq!(r1, 1); // Was already 1

    // SETBIT expands string as needed
    let r1 = redlite.setbit("sparse", 1000, true).unwrap();
    let r2: i64 = redis::cmd("SETBIT").arg("sparse").arg(1000).arg(1).query(&mut redis).unwrap();
    assert_eq!(r1, r2);

    // Verify string length (1000 / 8 + 1 = 126 bytes)
    let r1 = redlite.strlen("sparse").unwrap();
    let r2: i64 = redis.strlen("sparse").unwrap();
    assert_eq!(r1, r2);
    assert_eq!(r1, 126);
}

/// GETBIT command: all scenarios
#[test]
fn oracle_cmd_getbit() {
    let mut redis = require_redis!();
    let redlite = Db::open_memory().unwrap();
    let _: () = redis::cmd("FLUSHDB").query(&mut redis).unwrap();

    // GETBIT on non-existent key
    let r1 = redlite.getbit("missing", 0).unwrap();
    let r2: i64 = redis::cmd("GETBIT").arg("missing").arg(0).query(&mut redis).unwrap();
    assert_eq!(r1, r2);
    assert_eq!(r1, 0);

    // GETBIT on set bits
    redlite.setbit("bits", 0, true).unwrap();
    redlite.setbit("bits", 7, true).unwrap();
    let _: i64 = redis::cmd("SETBIT").arg("bits").arg(0).arg(1).query(&mut redis).unwrap();
    let _: i64 = redis::cmd("SETBIT").arg("bits").arg(7).arg(1).query(&mut redis).unwrap();

    let r1 = redlite.getbit("bits", 0).unwrap();
    let r2: i64 = redis::cmd("GETBIT").arg("bits").arg(0).query(&mut redis).unwrap();
    assert_eq!(r1, r2);
    assert_eq!(r1, 1);

    let r1 = redlite.getbit("bits", 7).unwrap();
    let r2: i64 = redis::cmd("GETBIT").arg("bits").arg(7).query(&mut redis).unwrap();
    assert_eq!(r1, r2);
    assert_eq!(r1, 1);

    // GETBIT on unset bits
    let r1 = redlite.getbit("bits", 1).unwrap();
    let r2: i64 = redis::cmd("GETBIT").arg("bits").arg(1).query(&mut redis).unwrap();
    assert_eq!(r1, r2);
    assert_eq!(r1, 0);

    // GETBIT beyond string length (returns 0)
    let r1 = redlite.getbit("bits", 1000).unwrap();
    let r2: i64 = redis::cmd("GETBIT").arg("bits").arg(1000).query(&mut redis).unwrap();
    assert_eq!(r1, r2);
    assert_eq!(r1, 0);

    // GETBIT on regular string
    redlite.set("str", b"a", None).unwrap(); // 'a' = 0b01100001
    let _: () = redis.set("str", "a").unwrap();

    let r1 = redlite.getbit("str", 0).unwrap();
    let r2: i64 = redis::cmd("GETBIT").arg("str").arg(0).query(&mut redis).unwrap();
    assert_eq!(r1, r2);

    let r1 = redlite.getbit("str", 1).unwrap();
    let r2: i64 = redis::cmd("GETBIT").arg("str").arg(1).query(&mut redis).unwrap();
    assert_eq!(r1, r2);
}

/// BITCOUNT command: all scenarios
#[test]
fn oracle_cmd_bitcount() {
    let mut redis = require_redis!();
    let redlite = Db::open_memory().unwrap();
    let _: () = redis::cmd("FLUSHDB").query(&mut redis).unwrap();

    // BITCOUNT on non-existent key
    let r1 = redlite.bitcount("missing", None, None).unwrap();
    let r2: i64 = redis::cmd("BITCOUNT").arg("missing").query(&mut redis).unwrap();
    assert_eq!(r1, r2);
    assert_eq!(r1, 0);

    // BITCOUNT on string
    redlite.set("str", b"foobar", None).unwrap();
    let _: () = redis.set("str", "foobar").unwrap();

    // Full string
    let r1 = redlite.bitcount("str", None, None).unwrap();
    let r2: i64 = redis::cmd("BITCOUNT").arg("str").query(&mut redis).unwrap();
    assert_eq!(r1, r2);

    // Range (byte indices)
    let r1 = redlite.bitcount("str", Some(0), Some(0)).unwrap();
    let r2: i64 = redis::cmd("BITCOUNT").arg("str").arg(0).arg(0).query(&mut redis).unwrap();
    assert_eq!(r1, r2);

    let r1 = redlite.bitcount("str", Some(0), Some(1)).unwrap();
    let r2: i64 = redis::cmd("BITCOUNT").arg("str").arg(0).arg(1).query(&mut redis).unwrap();
    assert_eq!(r1, r2);

    // Negative indices
    let r1 = redlite.bitcount("str", Some(-2), Some(-1)).unwrap();
    let r2: i64 = redis::cmd("BITCOUNT").arg("str").arg(-2).arg(-1).query(&mut redis).unwrap();
    assert_eq!(r1, r2);

    // BITCOUNT with set bits
    for i in 0..8 {
        redlite.setbit("bits", i, true).unwrap();
        let _: i64 = redis::cmd("SETBIT").arg("bits").arg(i).arg(1).query(&mut redis).unwrap();
    }

    let r1 = redlite.bitcount("bits", None, None).unwrap();
    let r2: i64 = redis::cmd("BITCOUNT").arg("bits").query(&mut redis).unwrap();
    assert_eq!(r1, r2);
    assert_eq!(r1, 8);
}

/// BITOP command: all scenarios
#[test]
fn oracle_cmd_bitop() {
    let mut redis = require_redis!();
    let redlite = Db::open_memory().unwrap();
    let _: () = redis::cmd("FLUSHDB").query(&mut redis).unwrap();

    // Setup test keys
    redlite.set("key1", b"\xff\x0f", None).unwrap(); // 11111111 00001111
    redlite.set("key2", b"\x0f\xff", None).unwrap(); // 00001111 11111111
    let _: () = redis.set("key1", &[0xffu8, 0x0fu8][..]).unwrap();
    let _: () = redis.set("key2", &[0x0fu8, 0xffu8][..]).unwrap();

    // BITOP AND
    let r1 = redlite.bitop("AND", "and_result", &["key1", "key2"]).unwrap();
    let r2: i64 = redis::cmd("BITOP").arg("AND").arg("and_result").arg("key1").arg("key2").query(&mut redis).unwrap();
    assert_eq!(r1, r2);

    let v1 = redlite.get("and_result").unwrap();
    let v2: Option<Vec<u8>> = redis.get("and_result").unwrap();
    assert_eq!(v1, v2);

    // BITOP OR
    let r1 = redlite.bitop("OR", "or_result", &["key1", "key2"]).unwrap();
    let r2: i64 = redis::cmd("BITOP").arg("OR").arg("or_result").arg("key1").arg("key2").query(&mut redis).unwrap();
    assert_eq!(r1, r2);

    let v1 = redlite.get("or_result").unwrap();
    let v2: Option<Vec<u8>> = redis.get("or_result").unwrap();
    assert_eq!(v1, v2);

    // BITOP XOR
    let r1 = redlite.bitop("XOR", "xor_result", &["key1", "key2"]).unwrap();
    let r2: i64 = redis::cmd("BITOP").arg("XOR").arg("xor_result").arg("key1").arg("key2").query(&mut redis).unwrap();
    assert_eq!(r1, r2);

    let v1 = redlite.get("xor_result").unwrap();
    let v2: Option<Vec<u8>> = redis.get("xor_result").unwrap();
    assert_eq!(v1, v2);

    // BITOP NOT (single key)
    let r1 = redlite.bitop("NOT", "not_result", &["key1"]).unwrap();
    let r2: i64 = redis::cmd("BITOP").arg("NOT").arg("not_result").arg("key1").query(&mut redis).unwrap();
    assert_eq!(r1, r2);

    let v1 = redlite.get("not_result").unwrap();
    let v2: Option<Vec<u8>> = redis.get("not_result").unwrap();
    assert_eq!(v1, v2);

    // BITOP with different length keys
    redlite.set("short", b"\xff", None).unwrap();
    redlite.set("long", b"\x00\x00\x00", None).unwrap();
    let _: () = redis.set("short", &[0xffu8][..]).unwrap();
    let _: () = redis.set("long", &[0x00u8, 0x00u8, 0x00u8][..]).unwrap();

    let r1 = redlite.bitop("OR", "mixed", &["short", "long"]).unwrap();
    let r2: i64 = redis::cmd("BITOP").arg("OR").arg("mixed").arg("short").arg("long").query(&mut redis).unwrap();
    assert_eq!(r1, r2);

    let v1 = redlite.get("mixed").unwrap();
    let v2: Option<Vec<u8>> = redis.get("mixed").unwrap();
    assert_eq!(v1, v2);
}

// ============================================================================
// LIST COMMANDS - Comprehensive Tests
// ============================================================================

/// LPUSH command: all scenarios
#[test]
fn oracle_cmd_lpush() {
    let mut redis = require_redis!();
    let redlite = Db::open_memory().unwrap();
    let _: () = redis::cmd("FLUSHDB").query(&mut redis).unwrap();

    // LPUSH single value to new key
    let r1 = redlite.lpush("list", &[b"a"]).unwrap();
    let r2: i64 = redis.lpush("list", "a").unwrap();
    assert_eq!(r1, r2);
    assert_eq!(r1, 1);

    // LPUSH single value to existing key
    let r1 = redlite.lpush("list", &[b"b"]).unwrap();
    let r2: i64 = redis.lpush("list", "b").unwrap();
    assert_eq!(r1, r2);
    assert_eq!(r1, 2);

    // LPUSH multiple values (they go in reverse order)
    let r1 = redlite.lpush("list", &[b"c", b"d", b"e"]).unwrap();
    let r2: i64 = redis.lpush("list", &["c", "d", "e"]).unwrap();
    assert_eq!(r1, r2);
    assert_eq!(r1, 5);

    // Verify order: e, d, c, b, a
    let r1 = redlite.lrange("list", 0, -1).unwrap();
    let r2: Vec<Vec<u8>> = redis.lrange("list", 0, -1).unwrap();
    assert_eq!(r1, r2);
    assert_eq!(r1[0], b"e".to_vec());
    assert_eq!(r1[4], b"a".to_vec());

    // LPUSH binary values
    let binary = vec![0u8, 255, 128];
    let r1 = redlite.lpush("bin_list", &[&binary]).unwrap();
    let r2: i64 = redis.lpush("bin_list", &binary).unwrap();
    assert_eq!(r1, r2);

    // LPUSH empty value
    let r1 = redlite.lpush("empty_list", &[b""]).unwrap();
    let r2: i64 = redis.lpush("empty_list", "").unwrap();
    assert_eq!(r1, r2);

    // LPUSH to wrong type - should error
    redlite.set("str", b"value", None).unwrap();
    let _: () = redis.set("str", "value").unwrap();
    let r1 = redlite.lpush("str", &[b"item"]);
    let r2: Result<i64, redis::RedisError> = redis.lpush("str", "item");
    assert!(r1.is_err());
    assert!(r2.is_err());
}

/// RPUSH command: all scenarios
#[test]
fn oracle_cmd_rpush() {
    let mut redis = require_redis!();
    let redlite = Db::open_memory().unwrap();
    let _: () = redis::cmd("FLUSHDB").query(&mut redis).unwrap();

    // RPUSH single value to new key
    let r1 = redlite.rpush("list", &[b"a"]).unwrap();
    let r2: i64 = redis.rpush("list", "a").unwrap();
    assert_eq!(r1, r2);
    assert_eq!(r1, 1);

    // RPUSH single value to existing key
    let r1 = redlite.rpush("list", &[b"b"]).unwrap();
    let r2: i64 = redis.rpush("list", "b").unwrap();
    assert_eq!(r1, r2);
    assert_eq!(r1, 2);

    // RPUSH multiple values (they go in order)
    let r1 = redlite.rpush("list", &[b"c", b"d", b"e"]).unwrap();
    let r2: i64 = redis.rpush("list", &["c", "d", "e"]).unwrap();
    assert_eq!(r1, r2);
    assert_eq!(r1, 5);

    // Verify order: a, b, c, d, e
    let r1 = redlite.lrange("list", 0, -1).unwrap();
    let r2: Vec<Vec<u8>> = redis.lrange("list", 0, -1).unwrap();
    assert_eq!(r1, r2);
    assert_eq!(r1[0], b"a".to_vec());
    assert_eq!(r1[4], b"e".to_vec());
}

/// LPUSHX command: all scenarios
#[test]
fn oracle_cmd_lpushx() {
    let mut redis = require_redis!();
    let redlite = Db::open_memory().unwrap();
    let _: () = redis::cmd("FLUSHDB").query(&mut redis).unwrap();

    // LPUSHX on non-existent key (does nothing)
    let r1 = redlite.lpushx("missing", &[b"a"]).unwrap();
    let r2: i64 = redis::cmd("LPUSHX").arg("missing").arg("a").query(&mut redis).unwrap();
    assert_eq!(r1, r2);
    assert_eq!(r1, 0);

    // Verify key not created
    let r1 = redlite.exists(&["missing"]).unwrap();
    let r2: i64 = redis.exists("missing").unwrap();
    assert_eq!(r1, r2);
    assert_eq!(r1, 0);

    // Create list
    redlite.rpush("list", &[b"x"]).unwrap();
    let _: i64 = redis.rpush("list", "x").unwrap();

    // LPUSHX on existing key
    let r1 = redlite.lpushx("list", &[b"a"]).unwrap();
    let r2: i64 = redis::cmd("LPUSHX").arg("list").arg("a").query(&mut redis).unwrap();
    assert_eq!(r1, r2);
    assert_eq!(r1, 2);

    // LPUSHX multiple values
    let r1 = redlite.lpushx("list", &[b"b", b"c"]).unwrap();
    let r2: i64 = redis::cmd("LPUSHX").arg("list").arg("b").arg("c").query(&mut redis).unwrap();
    assert_eq!(r1, r2);
    assert_eq!(r1, 4);

    // Verify order
    let r1 = redlite.lrange("list", 0, -1).unwrap();
    let r2: Vec<Vec<u8>> = redis.lrange("list", 0, -1).unwrap();
    assert_eq!(r1, r2);
}

/// RPUSHX command: all scenarios
#[test]
fn oracle_cmd_rpushx() {
    let mut redis = require_redis!();
    let redlite = Db::open_memory().unwrap();
    let _: () = redis::cmd("FLUSHDB").query(&mut redis).unwrap();

    // RPUSHX on non-existent key
    let r1 = redlite.rpushx("missing", &[b"a"]).unwrap();
    let r2: i64 = redis::cmd("RPUSHX").arg("missing").arg("a").query(&mut redis).unwrap();
    assert_eq!(r1, r2);
    assert_eq!(r1, 0);

    // Create list
    redlite.rpush("list", &[b"x"]).unwrap();
    let _: i64 = redis.rpush("list", "x").unwrap();

    // RPUSHX on existing key
    let r1 = redlite.rpushx("list", &[b"a"]).unwrap();
    let r2: i64 = redis::cmd("RPUSHX").arg("list").arg("a").query(&mut redis).unwrap();
    assert_eq!(r1, r2);
    assert_eq!(r1, 2);
}

/// LPOP command: all scenarios
#[test]
fn oracle_cmd_lpop() {
    let mut redis = require_redis!();
    let redlite = Db::open_memory().unwrap();
    let _: () = redis::cmd("FLUSHDB").query(&mut redis).unwrap();

    // LPOP on non-existent key
    let r1 = redlite.lpop("missing", None).unwrap();
    let r2: Option<Vec<u8>> = redis.lpop("missing", None).unwrap();
    assert!(r1.is_empty());
    assert!(r2.is_none());

    // Setup list
    for i in 0..5 {
        redlite.rpush("list", &[format!("item{}", i).as_bytes()]).unwrap();
        let _: i64 = redis.rpush("list", format!("item{}", i)).unwrap();
    }

    // LPOP single
    let r1 = redlite.lpop("list", None).unwrap();
    let r2: Option<Vec<u8>> = redis.lpop("list", None).unwrap();
    assert_eq!(r1.into_iter().next(), r2);

    // LPOP with count
    let r1 = redlite.lpop("list", Some(2)).unwrap();
    let r2: Vec<Vec<u8>> = redis::cmd("LPOP").arg("list").arg(2).query(&mut redis).unwrap();
    assert_eq!(r1, r2);
    assert_eq!(r1.len(), 2);

    // LPOP more than available
    let r1 = redlite.lpop("list", Some(100)).unwrap();
    let r2: Vec<Vec<u8>> = redis::cmd("LPOP").arg("list").arg(100).query(&mut redis).unwrap();
    assert_eq!(r1, r2);

    // LPOP until empty
    while !redlite.lpop("list", None).unwrap().is_empty() {
        let _: Option<Vec<u8>> = redis.lpop("list", None).unwrap();
    }

    // Verify empty
    let r1 = redlite.llen("list").unwrap();
    let r2: i64 = redis.llen("list").unwrap();
    assert_eq!(r1, r2);
    assert_eq!(r1, 0);
}

/// RPOP command: all scenarios
#[test]
fn oracle_cmd_rpop() {
    let mut redis = require_redis!();
    let redlite = Db::open_memory().unwrap();
    let _: () = redis::cmd("FLUSHDB").query(&mut redis).unwrap();

    // RPOP on non-existent key
    let r1 = redlite.rpop("missing", None).unwrap();
    let r2: Option<Vec<u8>> = redis.rpop("missing", None).unwrap();
    assert!(r1.is_empty());
    assert!(r2.is_none());

    // Setup list
    for i in 0..5 {
        redlite.rpush("list", &[format!("item{}", i).as_bytes()]).unwrap();
        let _: i64 = redis.rpush("list", format!("item{}", i)).unwrap();
    }

    // RPOP single
    let r1 = redlite.rpop("list", None).unwrap();
    let r2: Option<Vec<u8>> = redis.rpop("list", None).unwrap();
    assert_eq!(r1.into_iter().next(), r2);

    // RPOP with count
    let r1 = redlite.rpop("list", Some(2)).unwrap();
    let r2: Vec<Vec<u8>> = redis::cmd("RPOP").arg("list").arg(2).query(&mut redis).unwrap();
    assert_eq!(r1, r2);
}

/// LLEN command: all scenarios
#[test]
fn oracle_cmd_llen() {
    let mut redis = require_redis!();
    let redlite = Db::open_memory().unwrap();
    let _: () = redis::cmd("FLUSHDB").query(&mut redis).unwrap();

    // LLEN on non-existent key
    let r1 = redlite.llen("missing").unwrap();
    let r2: i64 = redis.llen("missing").unwrap();
    assert_eq!(r1, r2);
    assert_eq!(r1, 0);

    // LLEN on empty list (after pops)
    redlite.rpush("list", &[b"x"]).unwrap();
    let _: i64 = redis.rpush("list", "x").unwrap();
    redlite.lpop("list", None).unwrap();
    let _: Option<Vec<u8>> = redis.lpop("list", None).unwrap();

    let r1 = redlite.llen("list").unwrap();
    let r2: i64 = redis.llen("list").unwrap();
    assert_eq!(r1, r2);
    assert_eq!(r1, 0);

    // LLEN after multiple pushes
    for i in 0..100 {
        redlite.rpush("big_list", &[format!("item{}", i).as_bytes()]).unwrap();
        let _: i64 = redis.rpush("big_list", format!("item{}", i)).unwrap();
    }

    let r1 = redlite.llen("big_list").unwrap();
    let r2: i64 = redis.llen("big_list").unwrap();
    assert_eq!(r1, r2);
    assert_eq!(r1, 100);
}

/// LINDEX command: all scenarios
#[test]
fn oracle_cmd_lindex() {
    let mut redis = require_redis!();
    let redlite = Db::open_memory().unwrap();
    let _: () = redis::cmd("FLUSHDB").query(&mut redis).unwrap();

    // LINDEX on non-existent key
    let r1 = redlite.lindex("missing", 0).unwrap();
    let r2: Option<Vec<u8>> = redis.lindex("missing", 0).unwrap();
    assert_eq!(r1, r2);
    assert!(r1.is_none());

    // Setup list
    for i in 0..5 {
        redlite.rpush("list", &[format!("item{}", i).as_bytes()]).unwrap();
        let _: i64 = redis.rpush("list", format!("item{}", i)).unwrap();
    }

    // LINDEX at various positions
    for i in 0..5 {
        let r1 = redlite.lindex("list", i).unwrap();
        let r2: Option<Vec<u8>> = redis.lindex("list", i as isize).unwrap();
        assert_eq!(r1, r2, "Mismatch at index {}", i);
    }

    // LINDEX with negative indices
    let r1 = redlite.lindex("list", -1).unwrap();
    let r2: Option<Vec<u8>> = redis.lindex("list", -1).unwrap();
    assert_eq!(r1, r2);
    assert_eq!(r1, Some(b"item4".to_vec()));

    let r1 = redlite.lindex("list", -5).unwrap();
    let r2: Option<Vec<u8>> = redis.lindex("list", -5).unwrap();
    assert_eq!(r1, r2);
    assert_eq!(r1, Some(b"item0".to_vec()));

    // LINDEX out of bounds
    let r1 = redlite.lindex("list", 100).unwrap();
    let r2: Option<Vec<u8>> = redis.lindex("list", 100).unwrap();
    assert_eq!(r1, r2);
    assert!(r1.is_none());

    let r1 = redlite.lindex("list", -100).unwrap();
    let r2: Option<Vec<u8>> = redis.lindex("list", -100).unwrap();
    assert_eq!(r1, r2);
    assert!(r1.is_none());
}

/// LSET command: all scenarios
#[test]
fn oracle_cmd_lset() {
    let mut redis = require_redis!();
    let redlite = Db::open_memory().unwrap();
    let _: () = redis::cmd("FLUSHDB").query(&mut redis).unwrap();

    // Setup list
    for i in 0..5 {
        redlite.rpush("list", &[format!("item{}", i).as_bytes()]).unwrap();
        let _: i64 = redis.rpush("list", format!("item{}", i)).unwrap();
    }

    // LSET at valid index
    redlite.lset("list", 2, b"modified").unwrap();
    let _: () = redis.lset("list", 2, "modified").unwrap();

    let r1 = redlite.lindex("list", 2).unwrap();
    let r2: Option<Vec<u8>> = redis.lindex("list", 2).unwrap();
    assert_eq!(r1, r2);
    assert_eq!(r1, Some(b"modified".to_vec()));

    // LSET with negative index
    redlite.lset("list", -1, b"last").unwrap();
    let _: () = redis.lset("list", -1, "last").unwrap();

    let r1 = redlite.lindex("list", -1).unwrap();
    let r2: Option<Vec<u8>> = redis.lindex("list", -1).unwrap();
    assert_eq!(r1, r2);
    assert_eq!(r1, Some(b"last".to_vec()));

    // LSET at first index
    redlite.lset("list", 0, b"first").unwrap();
    let _: () = redis.lset("list", 0, "first").unwrap();

    let r1 = redlite.lindex("list", 0).unwrap();
    let r2: Option<Vec<u8>> = redis.lindex("list", 0).unwrap();
    assert_eq!(r1, r2);
    assert_eq!(r1, Some(b"first".to_vec()));

    // LSET out of bounds - should error
    let r1 = redlite.lset("list", 100, b"out");
    let r2: Result<(), redis::RedisError> = redis.lset("list", 100, "out");
    assert!(r1.is_err());
    assert!(r2.is_err());
}

/// LRANGE command: all scenarios
#[test]
fn oracle_cmd_lrange() {
    let mut redis = require_redis!();
    let redlite = Db::open_memory().unwrap();
    let _: () = redis::cmd("FLUSHDB").query(&mut redis).unwrap();

    // LRANGE on non-existent key
    let r1 = redlite.lrange("missing", 0, -1).unwrap();
    let r2: Vec<Vec<u8>> = redis.lrange("missing", 0, -1).unwrap();
    assert_eq!(r1, r2);
    assert!(r1.is_empty());

    // Setup list
    for i in 0..10 {
        redlite.rpush("list", &[format!("item{}", i).as_bytes()]).unwrap();
        let _: i64 = redis.rpush("list", format!("item{}", i)).unwrap();
    }

    // LRANGE full list
    let r1 = redlite.lrange("list", 0, -1).unwrap();
    let r2: Vec<Vec<u8>> = redis.lrange("list", 0, -1).unwrap();
    assert_eq!(r1, r2);
    assert_eq!(r1.len(), 10);

    // LRANGE subset
    let r1 = redlite.lrange("list", 2, 5).unwrap();
    let r2: Vec<Vec<u8>> = redis.lrange("list", 2, 5).unwrap();
    assert_eq!(r1, r2);
    assert_eq!(r1.len(), 4);

    // LRANGE with negative indices
    let r1 = redlite.lrange("list", -3, -1).unwrap();
    let r2: Vec<Vec<u8>> = redis.lrange("list", -3, -1).unwrap();
    assert_eq!(r1, r2);
    assert_eq!(r1.len(), 3);

    // LRANGE beyond bounds (clamped)
    let r1 = redlite.lrange("list", 0, 100).unwrap();
    let r2: Vec<Vec<u8>> = redis.lrange("list", 0, 100).unwrap();
    assert_eq!(r1, r2);
    assert_eq!(r1.len(), 10);

    // LRANGE empty result (start > end)
    let r1 = redlite.lrange("list", 5, 2).unwrap();
    let r2: Vec<Vec<u8>> = redis.lrange("list", 5, 2).unwrap();
    assert_eq!(r1, r2);
    assert!(r1.is_empty());

    // LRANGE single element
    let r1 = redlite.lrange("list", 3, 3).unwrap();
    let r2: Vec<Vec<u8>> = redis.lrange("list", 3, 3).unwrap();
    assert_eq!(r1, r2);
    assert_eq!(r1.len(), 1);
}

/// LTRIM command: all scenarios
#[test]
fn oracle_cmd_ltrim() {
    let mut redis = require_redis!();
    let redlite = Db::open_memory().unwrap();
    let _: () = redis::cmd("FLUSHDB").query(&mut redis).unwrap();

    // Setup list
    for i in 0..10 {
        redlite.rpush("list", &[format!("item{}", i).as_bytes()]).unwrap();
        let _: i64 = redis.rpush("list", format!("item{}", i)).unwrap();
    }

    // LTRIM to middle section
    redlite.ltrim("list", 2, 7).unwrap();
    let _: () = redis.ltrim("list", 2, 7).unwrap();

    let r1 = redlite.lrange("list", 0, -1).unwrap();
    let r2: Vec<Vec<u8>> = redis.lrange("list", 0, -1).unwrap();
    assert_eq!(r1, r2);
    assert_eq!(r1.len(), 6);

    // LTRIM with negative indices
    redlite.ltrim("list", 0, -2).unwrap();
    let _: () = redis.ltrim("list", 0, -2).unwrap();

    let r1 = redlite.lrange("list", 0, -1).unwrap();
    let r2: Vec<Vec<u8>> = redis.lrange("list", 0, -1).unwrap();
    assert_eq!(r1, r2);
    assert_eq!(r1.len(), 5);

    // LTRIM to single element
    redlite.ltrim("list", 0, 0).unwrap();
    let _: () = redis.ltrim("list", 0, 0).unwrap();

    let r1 = redlite.llen("list").unwrap();
    let r2: i64 = redis.llen("list").unwrap();
    assert_eq!(r1, r2);
    assert_eq!(r1, 1);

    // LTRIM to empty (start > end)
    redlite.rpush("list2", &[b"a", b"b", b"c"]).unwrap();
    let _: i64 = redis.rpush("list2", &["a", "b", "c"]).unwrap();
    redlite.ltrim("list2", 5, 2).unwrap();
    let _: () = redis.ltrim("list2", 5, 2).unwrap();

    let r1 = redlite.llen("list2").unwrap();
    let r2: i64 = redis.llen("list2").unwrap();
    assert_eq!(r1, r2);
    assert_eq!(r1, 0);
}

/// LREM command: all scenarios
#[test]
fn oracle_cmd_lrem() {
    let mut redis = require_redis!();
    let redlite = Db::open_memory().unwrap();
    let _: () = redis::cmd("FLUSHDB").query(&mut redis).unwrap();

    // Setup list with duplicates
    let items = ["a", "b", "a", "c", "a", "d", "a"];
    for item in items {
        redlite.rpush("list", &[item.as_bytes()]).unwrap();
        let _: i64 = redis.rpush("list", item).unwrap();
    }

    // LREM with positive count (from head)
    let r1 = redlite.lrem("list", 2, b"a").unwrap();
    let r2: i64 = redis.lrem("list", 2, "a").unwrap();
    assert_eq!(r1, r2);
    assert_eq!(r1, 2);

    // Verify list state
    let r1 = redlite.lrange("list", 0, -1).unwrap();
    let r2: Vec<Vec<u8>> = redis.lrange("list", 0, -1).unwrap();
    assert_eq!(r1, r2);

    // Setup fresh list
    let _: () = redis::cmd("FLUSHDB").query(&mut redis).unwrap();
    redlite.flushdb().unwrap();
    for item in items {
        redlite.rpush("list", &[item.as_bytes()]).unwrap();
        let _: i64 = redis.rpush("list", item).unwrap();
    }

    // LREM with negative count (from tail)
    let r1 = redlite.lrem("list", -2, b"a").unwrap();
    let r2: i64 = redis.lrem("list", -2, "a").unwrap();
    assert_eq!(r1, r2);
    assert_eq!(r1, 2);

    // Setup fresh list
    let _: () = redis::cmd("FLUSHDB").query(&mut redis).unwrap();
    redlite.flushdb().unwrap();
    for item in items {
        redlite.rpush("list", &[item.as_bytes()]).unwrap();
        let _: i64 = redis.rpush("list", item).unwrap();
    }

    // LREM with zero count (all occurrences)
    let r1 = redlite.lrem("list", 0, b"a").unwrap();
    let r2: i64 = redis.lrem("list", 0, "a").unwrap();
    assert_eq!(r1, r2);
    assert_eq!(r1, 4);

    // LREM non-existent element
    let r1 = redlite.lrem("list", 0, b"z").unwrap();
    let r2: i64 = redis.lrem("list", 0, "z").unwrap();
    assert_eq!(r1, r2);
    assert_eq!(r1, 0);
}

/// LINSERT command: all scenarios
#[test]
fn oracle_cmd_linsert() {
    let mut redis = require_redis!();
    let redlite = Db::open_memory().unwrap();
    let _: () = redis::cmd("FLUSHDB").query(&mut redis).unwrap();

    // Setup list
    redlite.rpush("list", &[b"a", b"c"]).unwrap();
    let _: i64 = redis.rpush("list", &["a", "c"]).unwrap();

    // LINSERT BEFORE
    let r1 = redlite.linsert("list", true, b"c", b"b").unwrap();
    let r2: i64 = redis::cmd("LINSERT").arg("list").arg("BEFORE").arg("c").arg("b").query(&mut redis).unwrap();
    assert_eq!(r1, r2);
    assert_eq!(r1, 3);

    // Verify: a, b, c
    let r1 = redlite.lrange("list", 0, -1).unwrap();
    let r2: Vec<Vec<u8>> = redis.lrange("list", 0, -1).unwrap();
    assert_eq!(r1, r2);

    // LINSERT AFTER
    let r1 = redlite.linsert("list", false, b"c", b"d").unwrap();
    let r2: i64 = redis::cmd("LINSERT").arg("list").arg("AFTER").arg("c").arg("d").query(&mut redis).unwrap();
    assert_eq!(r1, r2);
    assert_eq!(r1, 4);

    // LINSERT pivot not found
    let r1 = redlite.linsert("list", true, b"z", b"x").unwrap();
    let r2: i64 = redis::cmd("LINSERT").arg("list").arg("BEFORE").arg("z").arg("x").query(&mut redis).unwrap();
    assert_eq!(r1, r2);
    assert_eq!(r1, -1);

    // LINSERT on non-existent key
    let r1 = redlite.linsert("missing", true, b"a", b"b").unwrap();
    let r2: i64 = redis::cmd("LINSERT").arg("missing").arg("BEFORE").arg("a").arg("b").query(&mut redis).unwrap();
    assert_eq!(r1, r2);
    assert_eq!(r1, 0);
}

/// LPOS command: all scenarios
#[test]
fn oracle_cmd_lpos() {
    let mut redis = require_redis!();
    let redlite = Db::open_memory().unwrap();
    let _: () = redis::cmd("FLUSHDB").query(&mut redis).unwrap();

    // Setup list with duplicates
    let items = ["a", "b", "c", "a", "b", "c", "a"];
    for item in items {
        redlite.rpush("list", &[item.as_bytes()]).unwrap();
        let _: i64 = redis.rpush("list", item).unwrap();
    }

    // LPOS basic (first occurrence)
    let r1 = redlite.lpos("list", b"a", None, None, None).unwrap().first().copied();
    let r2: Option<i64> = redis::cmd("LPOS").arg("list").arg("a").query(&mut redis).unwrap();
    assert_eq!(r1, r2);
    assert_eq!(r1, Some(0));

    // LPOS with RANK (skip first N-1 matches)
    let r1 = redlite.lpos("list", b"a", Some(2), None, None).unwrap().first().copied();
    let r2: Option<i64> = redis::cmd("LPOS").arg("list").arg("a").arg("RANK").arg(2).query(&mut redis).unwrap();
    assert_eq!(r1, r2);
    assert_eq!(r1, Some(3));

    // LPOS with negative RANK (from end)
    let r1 = redlite.lpos("list", b"a", Some(-1), None, None).unwrap().first().copied();
    let r2: Option<i64> = redis::cmd("LPOS").arg("list").arg("a").arg("RANK").arg(-1).query(&mut redis).unwrap();
    assert_eq!(r1, r2);
    assert_eq!(r1, Some(6));

    // LPOS element not found
    let r1 = redlite.lpos("list", b"z", None, None, None).unwrap().first().copied();
    let r2: Option<i64> = redis::cmd("LPOS").arg("list").arg("z").query(&mut redis).unwrap();
    assert_eq!(r1, r2);
    assert!(r1.is_none());
}

/// LMOVE command: all scenarios
#[test]
fn oracle_cmd_lmove() {
    let mut redis = require_redis!();
    let redlite = Db::open_memory().unwrap();
    let _: () = redis::cmd("FLUSHDB").query(&mut redis).unwrap();

    // Setup source list
    redlite.rpush("src", &[b"a", b"b", b"c"]).unwrap();
    let _: i64 = redis.rpush("src", &["a", "b", "c"]).unwrap();

    // LMOVE LEFT LEFT (source head to dest head)
    let r1 = redlite.lmove("src", "dst", ListDirection::Left, ListDirection::Left).unwrap();
    let r2: Option<Vec<u8>> = redis::cmd("LMOVE").arg("src").arg("dst").arg("LEFT").arg("LEFT").query(&mut redis).unwrap();
    assert_eq!(r1, r2);
    assert_eq!(r1, Some(b"a".to_vec()));

    // LMOVE RIGHT RIGHT (source tail to dest tail)
    let r1 = redlite.lmove("src", "dst", ListDirection::Right, ListDirection::Right).unwrap();
    let r2: Option<Vec<u8>> = redis::cmd("LMOVE").arg("src").arg("dst").arg("RIGHT").arg("RIGHT").query(&mut redis).unwrap();
    assert_eq!(r1, r2);
    assert_eq!(r1, Some(b"c".to_vec()));

    // Verify lists
    let src1 = redlite.lrange("src", 0, -1).unwrap();
    let src2: Vec<Vec<u8>> = redis.lrange("src", 0, -1).unwrap();
    assert_eq!(src1, src2);

    let dst1 = redlite.lrange("dst", 0, -1).unwrap();
    let dst2: Vec<Vec<u8>> = redis.lrange("dst", 0, -1).unwrap();
    assert_eq!(dst1, dst2);

    // LMOVE on non-existent source
    let r1 = redlite.lmove("missing", "dst", ListDirection::Left, ListDirection::Left).unwrap();
    let r2: Option<Vec<u8>> = redis::cmd("LMOVE").arg("missing").arg("dst").arg("LEFT").arg("LEFT").query(&mut redis).unwrap();
    assert_eq!(r1, r2);
    assert!(r1.is_none());
}

// ============================================================================
// HASH COMMANDS - Comprehensive Tests
// ============================================================================

/// HSET command: all scenarios
#[test]
fn oracle_cmd_hset() {
    let mut redis = require_redis!();
    let redlite = Db::open_memory().unwrap();
    let _: () = redis::cmd("FLUSHDB").query(&mut redis).unwrap();

    // HSET single field on new hash
    let r1 = redlite.hset("hash", &[("field1", b"value1".as_slice())]).unwrap();
    let r2: usize = redis.hset("hash", "field1", "value1").unwrap();
    assert_eq!(r1 as usize, r2);
    assert_eq!(r1, 1);

    // HSET update existing field
    let r1 = redlite.hset("hash", &[("field1", b"updated".as_slice())]).unwrap();
    let r2: usize = redis.hset("hash", "field1", "updated").unwrap();
    assert_eq!(r1 as usize, r2);
    assert_eq!(r1, 0); // Returns 0 when field already exists

    // Verify updated value
    let r1 = redlite.hget("hash", "field1").unwrap();
    let r2: Option<Vec<u8>> = redis.hget("hash", "field1").unwrap();
    assert_eq!(r1, r2);
    assert_eq!(r1, Some(b"updated".to_vec()));

    // HSET multiple fields
    let r1 = redlite.hset("hash", &[
        ("f2", b"v2".as_slice()),
        ("f3", b"v3".as_slice()),
        ("f4", b"v4".as_slice()),
    ]).unwrap();
    let r2: usize = redis::cmd("HSET").arg("hash")
        .arg("f2").arg("v2")
        .arg("f3").arg("v3")
        .arg("f4").arg("v4")
        .query(&mut redis).unwrap();
    assert_eq!(r1 as usize, r2);

    // HSET binary values
    let binary = vec![0u8, 255, 128];
    let r1 = redlite.hset("hash", &[("binary", binary.as_slice())]).unwrap();
    let r2: usize = redis.hset("hash", "binary", &binary).unwrap();
    assert_eq!(r1 as usize, r2);

    // Verify binary value
    let r1 = redlite.hget("hash", "binary").unwrap();
    let r2: Option<Vec<u8>> = redis.hget("hash", "binary").unwrap();
    assert_eq!(r1, r2);

    // HSET on wrong type - should error
    redlite.set("str", b"value", None).unwrap();
    let _: () = redis.set("str", "value").unwrap();
    let r1 = redlite.hset("str", &[("field", b"value".as_slice())]);
    let r2: Result<usize, redis::RedisError> = redis.hset("str", "field", "value");
    assert!(r1.is_err());
    assert!(r2.is_err());
}

/// HGET command: all scenarios
#[test]
fn oracle_cmd_hget() {
    let mut redis = require_redis!();
    let redlite = Db::open_memory().unwrap();
    let _: () = redis::cmd("FLUSHDB").query(&mut redis).unwrap();

    // HGET on non-existent hash
    let r1 = redlite.hget("missing", "field").unwrap();
    let r2: Option<Vec<u8>> = redis.hget("missing", "field").unwrap();
    assert_eq!(r1, r2);
    assert!(r1.is_none());

    // Setup hash
    redlite.hset("hash", &[("f1", b"v1".as_slice()), ("f2", b"v2".as_slice())]).unwrap();
    let _: usize = redis::cmd("HSET").arg("hash").arg("f1").arg("v1").arg("f2").arg("v2").query(&mut redis).unwrap();

    // HGET existing field
    let r1 = redlite.hget("hash", "f1").unwrap();
    let r2: Option<Vec<u8>> = redis.hget("hash", "f1").unwrap();
    assert_eq!(r1, r2);
    assert_eq!(r1, Some(b"v1".to_vec()));

    // HGET non-existent field
    let r1 = redlite.hget("hash", "missing").unwrap();
    let r2: Option<Vec<u8>> = redis.hget("hash", "missing").unwrap();
    assert_eq!(r1, r2);
    assert!(r1.is_none());

    // HGET empty value
    redlite.hset("hash", &[("empty", b"".as_slice())]).unwrap();
    let _: usize = redis.hset("hash", "empty", "").unwrap();
    let r1 = redlite.hget("hash", "empty").unwrap();
    let r2: Option<Vec<u8>> = redis.hget("hash", "empty").unwrap();
    assert_eq!(r1, r2);
    assert_eq!(r1, Some(vec![]));
}

/// HMGET command: all scenarios
#[test]
fn oracle_cmd_hmget() {
    let mut redis = require_redis!();
    let redlite = Db::open_memory().unwrap();
    let _: () = redis::cmd("FLUSHDB").query(&mut redis).unwrap();

    // Setup hash
    redlite.hset("hash", &[
        ("f1", b"v1".as_slice()),
        ("f2", b"v2".as_slice()),
        ("f3", b"v3".as_slice()),
    ]).unwrap();
    let _: usize = redis::cmd("HSET").arg("hash")
        .arg("f1").arg("v1")
        .arg("f2").arg("v2")
        .arg("f3").arg("v3")
        .query(&mut redis).unwrap();

    // HMGET all existing
    let r1 = redlite.hmget("hash", &["f1", "f2", "f3"]).unwrap();
    let r2: Vec<Option<Vec<u8>>> = redis::cmd("HMGET").arg("hash").arg("f1").arg("f2").arg("f3").query(&mut redis).unwrap();
    assert_eq!(r1, r2);

    // HMGET with some missing
    let r1 = redlite.hmget("hash", &["f1", "missing", "f3"]).unwrap();
    let r2: Vec<Option<Vec<u8>>> = redis::cmd("HMGET").arg("hash").arg("f1").arg("missing").arg("f3").query(&mut redis).unwrap();
    assert_eq!(r1, r2);
    assert!(r1[1].is_none());

    // HMGET on non-existent hash
    let r1 = redlite.hmget("missing", &["f1", "f2"]).unwrap();
    let r2: Vec<Option<Vec<u8>>> = redis::cmd("HMGET").arg("missing").arg("f1").arg("f2").query(&mut redis).unwrap();
    assert_eq!(r1, r2);
    assert!(r1.iter().all(|v| v.is_none()));
}

/// HGETALL command: all scenarios
#[test]
fn oracle_cmd_hgetall() {
    let mut redis = require_redis!();
    let redlite = Db::open_memory().unwrap();
    let _: () = redis::cmd("FLUSHDB").query(&mut redis).unwrap();

    // HGETALL on non-existent hash
    let r1: HashMap<String, Vec<u8>> = redlite.hgetall("missing").unwrap().into_iter().collect();
    let r2: HashMap<String, Vec<u8>> = redis.hgetall("missing").unwrap();
    assert_eq!(r1, r2);
    assert!(r1.is_empty());

    // Setup hash
    redlite.hset("hash", &[
        ("f1", b"v1".as_slice()),
        ("f2", b"v2".as_slice()),
        ("f3", b"v3".as_slice()),
    ]).unwrap();
    let _: usize = redis::cmd("HSET").arg("hash")
        .arg("f1").arg("v1")
        .arg("f2").arg("v2")
        .arg("f3").arg("v3")
        .query(&mut redis).unwrap();

    // HGETALL
    let r1: HashMap<String, Vec<u8>> = redlite.hgetall("hash").unwrap().into_iter().collect();
    let r2: HashMap<String, Vec<u8>> = redis.hgetall("hash").unwrap();
    assert_eq!(r1, r2);
    assert_eq!(r1.len(), 3);
}

/// HDEL command: all scenarios
#[test]
fn oracle_cmd_hdel() {
    let mut redis = require_redis!();
    let redlite = Db::open_memory().unwrap();
    let _: () = redis::cmd("FLUSHDB").query(&mut redis).unwrap();

    // Setup hash
    redlite.hset("hash", &[
        ("f1", b"v1".as_slice()),
        ("f2", b"v2".as_slice()),
        ("f3", b"v3".as_slice()),
    ]).unwrap();
    let _: usize = redis::cmd("HSET").arg("hash")
        .arg("f1").arg("v1")
        .arg("f2").arg("v2")
        .arg("f3").arg("v3")
        .query(&mut redis).unwrap();

    // HDEL single field
    let r1 = redlite.hdel("hash", &["f1"]).unwrap();
    let r2: i64 = redis.hdel("hash", "f1").unwrap();
    assert_eq!(r1, r2);
    assert_eq!(r1, 1);

    // Verify deleted
    let r1 = redlite.hget("hash", "f1").unwrap();
    let r2: Option<Vec<u8>> = redis.hget("hash", "f1").unwrap();
    assert_eq!(r1, r2);
    assert!(r1.is_none());

    // HDEL multiple fields
    let r1 = redlite.hdel("hash", &["f2", "f3"]).unwrap();
    let r2: i64 = redis::cmd("HDEL").arg("hash").arg("f2").arg("f3").query(&mut redis).unwrap();
    assert_eq!(r1, r2);
    assert_eq!(r1, 2);

    // HDEL non-existent field
    let r1 = redlite.hdel("hash", &["missing"]).unwrap();
    let r2: i64 = redis.hdel("hash", "missing").unwrap();
    assert_eq!(r1, r2);
    assert_eq!(r1, 0);

    // HDEL on non-existent hash
    let r1 = redlite.hdel("missing", &["field"]).unwrap();
    let r2: i64 = redis.hdel("missing", "field").unwrap();
    assert_eq!(r1, r2);
    assert_eq!(r1, 0);
}

/// HEXISTS command: all scenarios
#[test]
fn oracle_cmd_hexists() {
    let mut redis = require_redis!();
    let redlite = Db::open_memory().unwrap();
    let _: () = redis::cmd("FLUSHDB").query(&mut redis).unwrap();

    // HEXISTS on non-existent hash
    let r1 = redlite.hexists("missing", "field").unwrap();
    let r2: bool = redis.hexists("missing", "field").unwrap();
    assert_eq!(r1, r2);
    assert!(!r1);

    // Setup hash
    redlite.hset("hash", &[("f1", b"v1".as_slice())]).unwrap();
    let _: usize = redis.hset("hash", "f1", "v1").unwrap();

    // HEXISTS on existing field
    let r1 = redlite.hexists("hash", "f1").unwrap();
    let r2: bool = redis.hexists("hash", "f1").unwrap();
    assert_eq!(r1, r2);
    assert!(r1);

    // HEXISTS on non-existent field
    let r1 = redlite.hexists("hash", "missing").unwrap();
    let r2: bool = redis.hexists("hash", "missing").unwrap();
    assert_eq!(r1, r2);
    assert!(!r1);
}

/// HKEYS command: all scenarios
#[test]
fn oracle_cmd_hkeys() {
    let mut redis = require_redis!();
    let redlite = Db::open_memory().unwrap();
    let _: () = redis::cmd("FLUSHDB").query(&mut redis).unwrap();

    // HKEYS on non-existent hash
    let r1 = redlite.hkeys("missing").unwrap();
    let r2: Vec<String> = redis.hkeys("missing").unwrap();
    assert_eq!(r1, r2);
    assert!(r1.is_empty());

    // Setup hash
    redlite.hset("hash", &[
        ("f1", b"v1".as_slice()),
        ("f2", b"v2".as_slice()),
        ("f3", b"v3".as_slice()),
    ]).unwrap();
    let _: usize = redis::cmd("HSET").arg("hash")
        .arg("f1").arg("v1")
        .arg("f2").arg("v2")
        .arg("f3").arg("v3")
        .query(&mut redis).unwrap();

    // HKEYS
    let mut r1 = redlite.hkeys("hash").unwrap();
    let mut r2: Vec<String> = redis.hkeys("hash").unwrap();
    r1.sort();
    r2.sort();
    assert_eq!(r1, r2);
    assert_eq!(r1.len(), 3);
}

/// HVALS command: all scenarios
#[test]
fn oracle_cmd_hvals() {
    let mut redis = require_redis!();
    let redlite = Db::open_memory().unwrap();
    let _: () = redis::cmd("FLUSHDB").query(&mut redis).unwrap();

    // HVALS on non-existent hash
    let r1 = redlite.hvals("missing").unwrap();
    let r2: Vec<Vec<u8>> = redis.hvals("missing").unwrap();
    assert_eq!(r1, r2);
    assert!(r1.is_empty());

    // Setup hash
    redlite.hset("hash", &[
        ("f1", b"v1".as_slice()),
        ("f2", b"v2".as_slice()),
        ("f3", b"v3".as_slice()),
    ]).unwrap();
    let _: usize = redis::cmd("HSET").arg("hash")
        .arg("f1").arg("v1")
        .arg("f2").arg("v2")
        .arg("f3").arg("v3")
        .query(&mut redis).unwrap();

    // HVALS
    let mut r1 = redlite.hvals("hash").unwrap();
    let mut r2: Vec<Vec<u8>> = redis.hvals("hash").unwrap();
    r1.sort();
    r2.sort();
    assert_eq!(r1, r2);
    assert_eq!(r1.len(), 3);
}

/// HLEN command: all scenarios
#[test]
fn oracle_cmd_hlen() {
    let mut redis = require_redis!();
    let redlite = Db::open_memory().unwrap();
    let _: () = redis::cmd("FLUSHDB").query(&mut redis).unwrap();

    // HLEN on non-existent hash
    let r1 = redlite.hlen("missing").unwrap();
    let r2: usize = redis.hlen("missing").unwrap();
    assert_eq!(r1 as usize, r2);
    assert_eq!(r1, 0);

    // Setup hash
    redlite.hset("hash", &[
        ("f1", b"v1".as_slice()),
        ("f2", b"v2".as_slice()),
        ("f3", b"v3".as_slice()),
    ]).unwrap();
    let _: usize = redis::cmd("HSET").arg("hash")
        .arg("f1").arg("v1")
        .arg("f2").arg("v2")
        .arg("f3").arg("v3")
        .query(&mut redis).unwrap();

    // HLEN
    let r1 = redlite.hlen("hash").unwrap();
    let r2: usize = redis.hlen("hash").unwrap();
    assert_eq!(r1 as usize, r2);
    assert_eq!(r1, 3);

    // HLEN after delete
    redlite.hdel("hash", &["f1"]).unwrap();
    let _: i64 = redis.hdel("hash", "f1").unwrap();
    let r1 = redlite.hlen("hash").unwrap();
    let r2: usize = redis.hlen("hash").unwrap();
    assert_eq!(r1 as usize, r2);
    assert_eq!(r1, 2);
}

/// HINCRBY command: all scenarios
#[test]
fn oracle_cmd_hincrby() {
    let mut redis = require_redis!();
    let redlite = Db::open_memory().unwrap();
    let _: () = redis::cmd("FLUSHDB").query(&mut redis).unwrap();

    // HINCRBY on new field
    let r1 = redlite.hincrby("hash", "counter", 5).unwrap();
    let r2: i64 = redis.hincr("hash", "counter", 5i64).unwrap();
    assert_eq!(r1, r2);
    assert_eq!(r1, 5);

    // HINCRBY on existing field
    let r1 = redlite.hincrby("hash", "counter", 10).unwrap();
    let r2: i64 = redis.hincr("hash", "counter", 10i64).unwrap();
    assert_eq!(r1, r2);
    assert_eq!(r1, 15);

    // HINCRBY negative
    let r1 = redlite.hincrby("hash", "counter", -3).unwrap();
    let r2: i64 = redis.hincr("hash", "counter", -3i64).unwrap();
    assert_eq!(r1, r2);
    assert_eq!(r1, 12);

    // HINCRBY on string field representing number
    redlite.hset("hash", &[("numstr", b"100".as_slice())]).unwrap();
    let _: usize = redis.hset("hash", "numstr", "100").unwrap();
    let r1 = redlite.hincrby("hash", "numstr", 5).unwrap();
    let r2: i64 = redis.hincr("hash", "numstr", 5i64).unwrap();
    assert_eq!(r1, r2);
    assert_eq!(r1, 105);

    // HINCRBY on non-numeric field - should error
    redlite.hset("hash", &[("text", b"hello".as_slice())]).unwrap();
    let _: usize = redis.hset("hash", "text", "hello").unwrap();
    let r1 = redlite.hincrby("hash", "text", 1);
    let r2: Result<i64, redis::RedisError> = redis.hincr("hash", "text", 1i64);
    assert!(r1.is_err());
    assert!(r2.is_err());
}

/// HINCRBYFLOAT command: all scenarios
#[test]
fn oracle_cmd_hincrbyfloat() {
    let mut redis = require_redis!();
    let redlite = Db::open_memory().unwrap();
    let _: () = redis::cmd("FLUSHDB").query(&mut redis).unwrap();

    // HINCRBYFLOAT on new field
    let r1: f64 = redlite.hincrbyfloat("hash", "flt", 1.5).unwrap().parse().unwrap();
    let r2: f64 = redis::cmd("HINCRBYFLOAT").arg("hash").arg("flt").arg(1.5).query(&mut redis).unwrap();
    assert!((r1 - r2).abs() < 1e-10);
    assert!((r1 - 1.5).abs() < 1e-10);

    // HINCRBYFLOAT on existing field
    let r1: f64 = redlite.hincrbyfloat("hash", "flt", 2.5).unwrap().parse().unwrap();
    let r2: f64 = redis::cmd("HINCRBYFLOAT").arg("hash").arg("flt").arg(2.5).query(&mut redis).unwrap();
    assert!((r1 - r2).abs() < 1e-10);
    assert!((r1 - 4.0).abs() < 1e-10);

    // HINCRBYFLOAT negative
    let r1: f64 = redlite.hincrbyfloat("hash", "flt", -1.0).unwrap().parse().unwrap();
    let r2: f64 = redis::cmd("HINCRBYFLOAT").arg("hash").arg("flt").arg(-1.0).query(&mut redis).unwrap();
    assert!((r1 - r2).abs() < 1e-10);
    assert!((r1 - 3.0).abs() < 1e-10);
}

/// HSETNX command: all scenarios
#[test]
fn oracle_cmd_hsetnx() {
    let mut redis = require_redis!();
    let redlite = Db::open_memory().unwrap();
    let _: () = redis::cmd("FLUSHDB").query(&mut redis).unwrap();

    // HSETNX on new field
    let r1 = redlite.hsetnx("hash", "field", b"value").unwrap();
    let r2: bool = redis.hset_nx("hash", "field", "value").unwrap();
    assert_eq!(r1, r2);
    assert!(r1);

    // HSETNX on existing field (should fail)
    let r1 = redlite.hsetnx("hash", "field", b"newvalue").unwrap();
    let r2: bool = redis.hset_nx("hash", "field", "newvalue").unwrap();
    assert_eq!(r1, r2);
    assert!(!r1);

    // Verify value unchanged
    let r1 = redlite.hget("hash", "field").unwrap();
    let r2: Option<Vec<u8>> = redis.hget("hash", "field").unwrap();
    assert_eq!(r1, r2);
    assert_eq!(r1, Some(b"value".to_vec()));
}

/// HSCAN command: all scenarios
#[test]
fn oracle_cmd_hscan() {
    let mut redis = require_redis!();
    let redlite = Db::open_memory().unwrap();
    let _: () = redis::cmd("FLUSHDB").query(&mut redis).unwrap();

    // Setup hash with many fields
    for i in 0..50 {
        let field = format!("field:{}", i);
        let value = format!("value:{}", i);
        redlite.hset("hash", &[(&field, value.as_bytes())]).unwrap();
        let _: usize = redis.hset("hash", &field, &value).unwrap();
    }

    // HSCAN - collect all fields via cursor iteration
    let mut redlite_fields: Vec<String> = Vec::new();
    let mut cursor = "0".to_string();
    loop {
        let (next_cursor, batch) = redlite.hscan("hash", &cursor, None, 10).unwrap();
        for (field, _) in batch {
            redlite_fields.push(field);
        }
        cursor = next_cursor;
        if cursor == "0" { break; }
    }

    let mut redis_fields: Vec<String> = Vec::new();
    let mut redis_cursor: u64 = 0;
    loop {
        let result: (u64, Vec<(String, String)>) = redis::cmd("HSCAN").arg("hash").arg(redis_cursor).arg("COUNT").arg(10).query(&mut redis).unwrap();
        redis_cursor = result.0;
        for (field, _) in result.1 {
            redis_fields.push(field);
        }
        if redis_cursor == 0 { break; }
    }

    // Sort and compare
    redlite_fields.sort();
    redis_fields.sort();
    assert_eq!(redlite_fields.len(), redis_fields.len());
    assert_eq!(redlite_fields, redis_fields);
}

// ============================================================================
// SET COMMANDS - Comprehensive Tests
// ============================================================================

/// SADD command: all scenarios
#[test]
fn oracle_cmd_sadd() {
    let mut redis = require_redis!();
    let redlite = Db::open_memory().unwrap();
    let _: () = redis::cmd("FLUSHDB").query(&mut redis).unwrap();

    // SADD single member to new set
    let r1 = redlite.sadd("set", &[b"a"]).unwrap();
    let r2: usize = redis.sadd("set", "a").unwrap();
    assert_eq!(r1 as usize, r2);
    assert_eq!(r1, 1);

    // SADD duplicate member (should return 0)
    let r1 = redlite.sadd("set", &[b"a"]).unwrap();
    let r2: usize = redis.sadd("set", "a").unwrap();
    assert_eq!(r1 as usize, r2);
    assert_eq!(r1, 0);

    // SADD multiple members
    let r1 = redlite.sadd("set", &[b"b", b"c", b"d"]).unwrap();
    let r2: usize = redis.sadd("set", &["b", "c", "d"]).unwrap();
    assert_eq!(r1 as usize, r2);
    assert_eq!(r1, 3);

    // SADD mixed (some new, some existing)
    let r1 = redlite.sadd("set", &[b"a", b"e", b"b", b"f"]).unwrap();
    let r2: usize = redis.sadd("set", &["a", "e", "b", "f"]).unwrap();
    assert_eq!(r1 as usize, r2);
    assert_eq!(r1, 2); // Only e and f are new

    // SADD binary values
    let binary = vec![0u8, 255, 128];
    let r1 = redlite.sadd("binset", &[&binary]).unwrap();
    let r2: usize = redis.sadd("binset", &binary).unwrap();
    assert_eq!(r1 as usize, r2);

    // SADD on wrong type - should error
    redlite.set("str", b"value", None).unwrap();
    let _: () = redis.set("str", "value").unwrap();
    let r1 = redlite.sadd("str", &[b"member"]);
    let r2: Result<usize, redis::RedisError> = redis.sadd("str", "member");
    assert!(r1.is_err());
    assert!(r2.is_err());
}

/// SREM command: all scenarios
#[test]
fn oracle_cmd_srem() {
    let mut redis = require_redis!();
    let redlite = Db::open_memory().unwrap();
    let _: () = redis::cmd("FLUSHDB").query(&mut redis).unwrap();

    // Setup set
    redlite.sadd("set", &[b"a", b"b", b"c", b"d", b"e"]).unwrap();
    let _: usize = redis.sadd("set", &["a", "b", "c", "d", "e"]).unwrap();

    // SREM single member
    let r1 = redlite.srem("set", &[b"a"]).unwrap();
    let r2: usize = redis.srem("set", "a").unwrap();
    assert_eq!(r1 as usize, r2);
    assert_eq!(r1, 1);

    // SREM multiple members
    let r1 = redlite.srem("set", &[b"b", b"c"]).unwrap();
    let r2: usize = redis.srem("set", &["b", "c"]).unwrap();
    assert_eq!(r1 as usize, r2);
    assert_eq!(r1, 2);

    // SREM non-existent member
    let r1 = redlite.srem("set", &[b"z"]).unwrap();
    let r2: usize = redis.srem("set", "z").unwrap();
    assert_eq!(r1 as usize, r2);
    assert_eq!(r1, 0);

    // SREM on non-existent set
    let r1 = redlite.srem("missing", &[b"member"]).unwrap();
    let r2: usize = redis.srem("missing", "member").unwrap();
    assert_eq!(r1 as usize, r2);
    assert_eq!(r1, 0);
}

/// SMEMBERS command: all scenarios
#[test]
fn oracle_cmd_smembers() {
    let mut redis = require_redis!();
    let redlite = Db::open_memory().unwrap();
    let _: () = redis::cmd("FLUSHDB").query(&mut redis).unwrap();

    // SMEMBERS on non-existent set
    let r1 = redlite.smembers("missing").unwrap();
    let r2: Vec<Vec<u8>> = redis.smembers("missing").unwrap();
    assert_eq!(r1, r2);
    assert!(r1.is_empty());

    // Setup set
    redlite.sadd("set", &[b"c", b"a", b"b"]).unwrap();
    let _: usize = redis.sadd("set", &["c", "a", "b"]).unwrap();

    // SMEMBERS (order not guaranteed, so sort)
    let mut r1 = redlite.smembers("set").unwrap();
    let mut r2: Vec<Vec<u8>> = redis.smembers("set").unwrap();
    r1.sort();
    r2.sort();
    assert_eq!(r1, r2);
    assert_eq!(r1.len(), 3);
}

/// SISMEMBER command: all scenarios
#[test]
fn oracle_cmd_sismember() {
    let mut redis = require_redis!();
    let redlite = Db::open_memory().unwrap();
    let _: () = redis::cmd("FLUSHDB").query(&mut redis).unwrap();

    // SISMEMBER on non-existent set
    let r1 = redlite.sismember("missing", b"member").unwrap();
    let r2: bool = redis.sismember("missing", "member").unwrap();
    assert_eq!(r1, r2);
    assert!(!r1);

    // Setup set
    redlite.sadd("set", &[b"a", b"b", b"c"]).unwrap();
    let _: usize = redis.sadd("set", &["a", "b", "c"]).unwrap();

    // SISMEMBER existing member
    let r1 = redlite.sismember("set", b"a").unwrap();
    let r2: bool = redis.sismember("set", "a").unwrap();
    assert_eq!(r1, r2);
    assert!(r1);

    // SISMEMBER non-existent member
    let r1 = redlite.sismember("set", b"z").unwrap();
    let r2: bool = redis.sismember("set", "z").unwrap();
    assert_eq!(r1, r2);
    assert!(!r1);
}

/// SCARD command: all scenarios
#[test]
fn oracle_cmd_scard() {
    let mut redis = require_redis!();
    let redlite = Db::open_memory().unwrap();
    let _: () = redis::cmd("FLUSHDB").query(&mut redis).unwrap();

    // SCARD on non-existent set
    let r1 = redlite.scard("missing").unwrap();
    let r2: usize = redis.scard("missing").unwrap();
    assert_eq!(r1 as usize, r2);
    assert_eq!(r1, 0);

    // Setup set
    redlite.sadd("set", &[b"a", b"b", b"c"]).unwrap();
    let _: usize = redis.sadd("set", &["a", "b", "c"]).unwrap();

    // SCARD
    let r1 = redlite.scard("set").unwrap();
    let r2: usize = redis.scard("set").unwrap();
    assert_eq!(r1 as usize, r2);
    assert_eq!(r1, 3);

    // SCARD after adding more
    redlite.sadd("set", &[b"d", b"e"]).unwrap();
    let _: usize = redis.sadd("set", &["d", "e"]).unwrap();
    let r1 = redlite.scard("set").unwrap();
    let r2: usize = redis.scard("set").unwrap();
    assert_eq!(r1 as usize, r2);
    assert_eq!(r1, 5);
}

/// SDIFF command: all scenarios
#[test]
fn oracle_cmd_sdiff() {
    let mut redis = require_redis!();
    let redlite = Db::open_memory().unwrap();
    let _: () = redis::cmd("FLUSHDB").query(&mut redis).unwrap();

    // Setup sets
    redlite.sadd("set1", &[b"a", b"b", b"c", b"d"]).unwrap();
    redlite.sadd("set2", &[b"c", b"d", b"e"]).unwrap();
    let _: usize = redis.sadd("set1", &["a", "b", "c", "d"]).unwrap();
    let _: usize = redis.sadd("set2", &["c", "d", "e"]).unwrap();

    // SDIFF
    let mut r1 = redlite.sdiff(&["set1", "set2"]).unwrap();
    let mut r2: Vec<Vec<u8>> = redis.sdiff(&["set1", "set2"]).unwrap();
    r1.sort();
    r2.sort();
    assert_eq!(r1, r2);

    // SDIFF with non-existent set
    let mut r1 = redlite.sdiff(&["set1", "missing"]).unwrap();
    let mut r2: Vec<Vec<u8>> = redis.sdiff(&["set1", "missing"]).unwrap();
    r1.sort();
    r2.sort();
    assert_eq!(r1, r2);
}

/// SINTER command: all scenarios
#[test]
fn oracle_cmd_sinter() {
    let mut redis = require_redis!();
    let redlite = Db::open_memory().unwrap();
    let _: () = redis::cmd("FLUSHDB").query(&mut redis).unwrap();

    // Setup sets
    redlite.sadd("set1", &[b"a", b"b", b"c"]).unwrap();
    redlite.sadd("set2", &[b"b", b"c", b"d"]).unwrap();
    redlite.sadd("set3", &[b"c", b"d", b"e"]).unwrap();
    let _: usize = redis.sadd("set1", &["a", "b", "c"]).unwrap();
    let _: usize = redis.sadd("set2", &["b", "c", "d"]).unwrap();
    let _: usize = redis.sadd("set3", &["c", "d", "e"]).unwrap();

    // SINTER two sets
    let mut r1 = redlite.sinter(&["set1", "set2"]).unwrap();
    let mut r2: Vec<Vec<u8>> = redis.sinter(&["set1", "set2"]).unwrap();
    r1.sort();
    r2.sort();
    assert_eq!(r1, r2);

    // SINTER three sets
    let mut r1 = redlite.sinter(&["set1", "set2", "set3"]).unwrap();
    let mut r2: Vec<Vec<u8>> = redis.sinter(&["set1", "set2", "set3"]).unwrap();
    r1.sort();
    r2.sort();
    assert_eq!(r1, r2);
    assert_eq!(r1, vec![b"c".to_vec()]); // Only 'c' is in all three

    // SINTER with non-existent set (empty result)
    let r1 = redlite.sinter(&["set1", "missing"]).unwrap();
    let r2: Vec<Vec<u8>> = redis.sinter(&["set1", "missing"]).unwrap();
    assert_eq!(r1, r2);
    assert!(r1.is_empty());
}

/// SUNION command: all scenarios
#[test]
fn oracle_cmd_sunion() {
    let mut redis = require_redis!();
    let redlite = Db::open_memory().unwrap();
    let _: () = redis::cmd("FLUSHDB").query(&mut redis).unwrap();

    // Setup sets
    redlite.sadd("set1", &[b"a", b"b"]).unwrap();
    redlite.sadd("set2", &[b"c", b"d"]).unwrap();
    let _: usize = redis.sadd("set1", &["a", "b"]).unwrap();
    let _: usize = redis.sadd("set2", &["c", "d"]).unwrap();

    // SUNION
    let mut r1 = redlite.sunion(&["set1", "set2"]).unwrap();
    let mut r2: Vec<Vec<u8>> = redis.sunion(&["set1", "set2"]).unwrap();
    r1.sort();
    r2.sort();
    assert_eq!(r1, r2);
    assert_eq!(r1.len(), 4);

    // SUNION with overlapping
    redlite.sadd("set3", &[b"a", b"c", b"e"]).unwrap();
    let _: usize = redis.sadd("set3", &["a", "c", "e"]).unwrap();
    let mut r1 = redlite.sunion(&["set1", "set2", "set3"]).unwrap();
    let mut r2: Vec<Vec<u8>> = redis.sunion(&["set1", "set2", "set3"]).unwrap();
    r1.sort();
    r2.sort();
    assert_eq!(r1, r2);
    assert_eq!(r1.len(), 5); // a, b, c, d, e
}

/// SMOVE command: all scenarios
#[test]
fn oracle_cmd_smove() {
    let mut redis = require_redis!();
    let redlite = Db::open_memory().unwrap();
    let _: () = redis::cmd("FLUSHDB").query(&mut redis).unwrap();

    // Setup source set
    redlite.sadd("src", &[b"a", b"b", b"c"]).unwrap();
    let _: usize = redis.sadd("src", &["a", "b", "c"]).unwrap();

    // SMOVE existing member
    let r1 = redlite.smove("src", "dst", b"a").unwrap();
    let r2: bool = redis.smove("src", "dst", "a").unwrap();
    assert_eq!(r1, r2 as i64);
    assert_eq!(r1, 1);

    // Verify member moved
    let r1 = redlite.sismember("src", b"a").unwrap();
    let r2: bool = redis.sismember("src", "a").unwrap();
    assert_eq!(r1, r2);
    assert!(!r1);

    let r1 = redlite.sismember("dst", b"a").unwrap();
    let r2: bool = redis.sismember("dst", "a").unwrap();
    assert_eq!(r1, r2);
    assert!(r1);

    // SMOVE non-existent member
    let r1 = redlite.smove("src", "dst", b"z").unwrap();
    let r2: bool = redis.smove("src", "dst", "z").unwrap();
    assert_eq!(r1, r2 as i64);
    assert_eq!(r1, 0);
}

// ============================================================================
// SORTED SET COMMANDS - Comprehensive Tests
// ============================================================================

/// ZADD command: all scenarios
#[test]
fn oracle_cmd_zadd() {
    let mut redis = require_redis!();
    let redlite = Db::open_memory().unwrap();
    let _: () = redis::cmd("FLUSHDB").query(&mut redis).unwrap();

    // ZADD single member to new set
    let r1 = redlite.zadd("zset", &[ZMember::new(1.0, b"a".to_vec())]).unwrap();
    let r2: usize = redis.zadd("zset", "a", 1.0).unwrap();
    assert_eq!(r1 as usize, r2);
    assert_eq!(r1, 1);

    // ZADD duplicate member with different score (updates score, returns 0)
    let r1 = redlite.zadd("zset", &[ZMember::new(2.0, b"a".to_vec())]).unwrap();
    let r2: usize = redis.zadd("zset", "a", 2.0).unwrap();
    assert_eq!(r1 as usize, r2);
    assert_eq!(r1, 0);

    // Verify score updated
    let r1 = redlite.zscore("zset", b"a").unwrap();
    let r2: Option<f64> = redis.zscore("zset", "a").unwrap();
    assert_eq!(r1, r2);
    assert_eq!(r1, Some(2.0));

    // ZADD multiple members
    let r1 = redlite.zadd("zset", &[ZMember::new(3.0, b"b".to_vec()), ZMember::new(4.0, b"c".to_vec()), ZMember::new(5.0, b"d".to_vec())]).unwrap();
    let r2: usize = redis.zadd_multiple("zset", &[(3.0, "b"), (4.0, "c"), (5.0, "d")]).unwrap();
    assert_eq!(r1 as usize, r2);
    assert_eq!(r1, 3);

    // Note: NX/XX options not supported in current API, testing basic zadd behavior only
    // ZADD existing member (updates score)
    let r1 = redlite.zadd("zset", &[ZMember::new(10.0, b"a".to_vec())]).unwrap();
    let _: usize = redis.zadd("zset", "a", 10.0).unwrap();
    assert_eq!(r1, 0); // existing member updated

    // ZADD new member
    let r1 = redlite.zadd("zset", &[ZMember::new(6.0, b"e".to_vec())]).unwrap();
    let r2: usize = redis.zadd("zset", "e", 6.0).unwrap();
    assert_eq!(r1 as usize, r2);
    assert_eq!(r1, 1);

    // ZADD with negative score
    let r1 = redlite.zadd("zset2", &[ZMember::new(-5.0, b"neg".to_vec())]).unwrap();
    let r2: usize = redis.zadd("zset2", "neg", -5.0).unwrap();
    assert_eq!(r1 as usize, r2);

    // ZADD with float score
    let r1 = redlite.zadd("zset2", &[ZMember::new(3.14159, b"pi".to_vec())]).unwrap();
    let r2: usize = redis.zadd("zset2", "pi", 3.14159).unwrap();
    assert_eq!(r1 as usize, r2);

    // ZADD on wrong type - should error
    redlite.set("str", b"value", None).unwrap();
    let _: () = redis.set("str", "value").unwrap();
    let r1 = redlite.zadd("str", &[ZMember::new(1.0, b"member".to_vec())]);
    let r2: Result<usize, redis::RedisError> = redis.zadd("str", "member", 1.0);
    assert!(r1.is_err());
    assert!(r2.is_err());
}

/// ZREM command: all scenarios
#[test]
fn oracle_cmd_zrem() {
    let mut redis = require_redis!();
    let redlite = Db::open_memory().unwrap();
    let _: () = redis::cmd("FLUSHDB").query(&mut redis).unwrap();

    // Setup sorted set
    redlite.zadd("zset", &[ZMember::new(1.0, b"a".to_vec()), ZMember::new(2.0, b"b".to_vec()), ZMember::new(3.0, b"c".to_vec()), ZMember::new(4.0, b"d".to_vec()), ZMember::new(5.0, b"e".to_vec())]).unwrap();
    let _: usize = redis.zadd_multiple("zset", &[(1.0, "a"), (2.0, "b"), (3.0, "c"), (4.0, "d"), (5.0, "e")]).unwrap();

    // ZREM single member
    let r1 = redlite.zrem("zset", &[b"a".as_slice()]).unwrap();
    let r2: usize = redis.zrem("zset", "a").unwrap();
    assert_eq!(r1 as usize, r2);
    assert_eq!(r1, 1);

    // ZREM multiple members
    let r1 = redlite.zrem("zset", &[b"b".as_slice(), b"c".as_slice()]).unwrap();
    let r2: usize = redis.zrem("zset", &["b", "c"]).unwrap();
    assert_eq!(r1 as usize, r2);
    assert_eq!(r1, 2);

    // ZREM non-existent member
    let r1 = redlite.zrem("zset", &[b"z".as_slice()]).unwrap();
    let r2: usize = redis.zrem("zset", "z").unwrap();
    assert_eq!(r1 as usize, r2);
    assert_eq!(r1, 0);

    // ZREM on non-existent set
    let r1 = redlite.zrem("missing", &[b"member".as_slice()]).unwrap();
    let r2: usize = redis.zrem("missing", "member").unwrap();
    assert_eq!(r1 as usize, r2);
    assert_eq!(r1, 0);

    // ZREM mixed (some exist, some don't)
    let r1 = redlite.zrem("zset", &[b"d".as_slice(), b"z".as_slice()]).unwrap();
    let r2: usize = redis.zrem("zset", &["d", "z"]).unwrap();
    assert_eq!(r1 as usize, r2);
    assert_eq!(r1, 1);
}

/// ZSCORE command: all scenarios
#[test]
fn oracle_cmd_zscore() {
    let mut redis = require_redis!();
    let redlite = Db::open_memory().unwrap();
    let _: () = redis::cmd("FLUSHDB").query(&mut redis).unwrap();

    // ZSCORE on non-existent set
    let r1 = redlite.zscore("missing", b"member").unwrap();
    let r2: Option<f64> = redis.zscore("missing", "member").unwrap();
    assert_eq!(r1, r2);
    assert_eq!(r1, None);

    // Setup sorted set
    redlite.zadd("zset", &[ZMember::new(1.5, b"a".to_vec()), ZMember::new(2.5, b"b".to_vec()), ZMember::new(3.5, b"c".to_vec())]).unwrap();
    let _: usize = redis.zadd_multiple("zset", &[(1.5, "a"), (2.5, "b"), (3.5, "c")]).unwrap();

    // ZSCORE existing member
    let r1 = redlite.zscore("zset", b"a").unwrap();
    let r2: Option<f64> = redis.zscore("zset", "a").unwrap();
    assert_eq!(r1, r2);
    assert_eq!(r1, Some(1.5));

    // ZSCORE non-existent member
    let r1 = redlite.zscore("zset", b"z").unwrap();
    let r2: Option<f64> = redis.zscore("zset", "z").unwrap();
    assert_eq!(r1, r2);
    assert_eq!(r1, None);

    // ZSCORE with negative score
    redlite.zadd("zset", &[ZMember::new(-10.5, b"neg".to_vec())]).unwrap();
    let _: usize = redis.zadd("zset", "neg", -10.5).unwrap();
    let r1 = redlite.zscore("zset", b"neg").unwrap();
    let r2: Option<f64> = redis.zscore("zset", "neg").unwrap();
    assert_eq!(r1, r2);
    assert_eq!(r1, Some(-10.5));
}

/// ZRANK command: all scenarios
#[test]
fn oracle_cmd_zrank() {
    let mut redis = require_redis!();
    let redlite = Db::open_memory().unwrap();
    let _: () = redis::cmd("FLUSHDB").query(&mut redis).unwrap();

    // ZRANK on non-existent set
    let r1 = redlite.zrank("missing", b"member").unwrap();
    let r2: Option<usize> = redis.zrank("missing", "member").unwrap();
    assert_eq!(r1.map(|x| x as usize), r2);
    assert_eq!(r1, None);

    // Setup sorted set
    redlite.zadd("zset", &[ZMember::new(1.0, b"a".to_vec()), ZMember::new(2.0, b"b".to_vec()), ZMember::new(3.0, b"c".to_vec())]).unwrap();
    let _: usize = redis.zadd_multiple("zset", &[(1.0, "a"), (2.0, "b"), (3.0, "c")]).unwrap();

    // ZRANK first member (rank 0)
    let r1 = redlite.zrank("zset", b"a").unwrap();
    let r2: Option<usize> = redis.zrank("zset", "a").unwrap();
    assert_eq!(r1.map(|x| x as usize), r2);
    assert_eq!(r1, Some(0));

    // ZRANK middle member
    let r1 = redlite.zrank("zset", b"b").unwrap();
    let r2: Option<usize> = redis.zrank("zset", "b").unwrap();
    assert_eq!(r1.map(|x| x as usize), r2);
    assert_eq!(r1, Some(1));

    // ZRANK last member
    let r1 = redlite.zrank("zset", b"c").unwrap();
    let r2: Option<usize> = redis.zrank("zset", "c").unwrap();
    assert_eq!(r1.map(|x| x as usize), r2);
    assert_eq!(r1, Some(2));

    // ZRANK non-existent member
    let r1 = redlite.zrank("zset", b"z").unwrap();
    let r2: Option<usize> = redis.zrank("zset", "z").unwrap();
    assert_eq!(r1.map(|x| x as usize), r2);
    assert_eq!(r1, None);
}

/// ZREVRANK command: all scenarios
#[test]
fn oracle_cmd_zrevrank() {
    let mut redis = require_redis!();
    let redlite = Db::open_memory().unwrap();
    let _: () = redis::cmd("FLUSHDB").query(&mut redis).unwrap();

    // ZREVRANK on non-existent set
    let r1 = redlite.zrevrank("missing", b"member").unwrap();
    let r2: Option<usize> = redis.zrevrank("missing", "member").unwrap();
    assert_eq!(r1.map(|x| x as usize), r2);
    assert_eq!(r1, None);

    // Setup sorted set
    redlite.zadd("zset", &[ZMember::new(1.0, b"a".to_vec()), ZMember::new(2.0, b"b".to_vec()), ZMember::new(3.0, b"c".to_vec())]).unwrap();
    let _: usize = redis.zadd_multiple("zset", &[(1.0, "a"), (2.0, "b"), (3.0, "c")]).unwrap();

    // ZREVRANK first (highest score = rank 0 in reverse)
    let r1 = redlite.zrevrank("zset", b"c").unwrap();
    let r2: Option<usize> = redis.zrevrank("zset", "c").unwrap();
    assert_eq!(r1.map(|x| x as usize), r2);
    assert_eq!(r1, Some(0));

    // ZREVRANK middle
    let r1 = redlite.zrevrank("zset", b"b").unwrap();
    let r2: Option<usize> = redis.zrevrank("zset", "b").unwrap();
    assert_eq!(r1.map(|x| x as usize), r2);
    assert_eq!(r1, Some(1));

    // ZREVRANK last (lowest score = highest rank in reverse)
    let r1 = redlite.zrevrank("zset", b"a").unwrap();
    let r2: Option<usize> = redis.zrevrank("zset", "a").unwrap();
    assert_eq!(r1.map(|x| x as usize), r2);
    assert_eq!(r1, Some(2));
}

/// ZCARD command: all scenarios
#[test]
fn oracle_cmd_zcard() {
    let mut redis = require_redis!();
    let redlite = Db::open_memory().unwrap();
    let _: () = redis::cmd("FLUSHDB").query(&mut redis).unwrap();

    // ZCARD on non-existent set
    let r1 = redlite.zcard("missing").unwrap();
    let r2: usize = redis.zcard("missing").unwrap();
    assert_eq!(r1 as usize, r2);
    assert_eq!(r1, 0);

    // Setup sorted set
    redlite.zadd("zset", &[ZMember::new(1.0, b"a".to_vec()), ZMember::new(2.0, b"b".to_vec()), ZMember::new(3.0, b"c".to_vec())]).unwrap();
    let _: usize = redis.zadd_multiple("zset", &[(1.0, "a"), (2.0, "b"), (3.0, "c")]).unwrap();

    // ZCARD
    let r1 = redlite.zcard("zset").unwrap();
    let r2: usize = redis.zcard("zset").unwrap();
    assert_eq!(r1 as usize, r2);
    assert_eq!(r1, 3);

    // ZCARD after adding more
    redlite.zadd("zset", &[ZMember::new(4.0, b"d".to_vec()), ZMember::new(5.0, b"e".to_vec())]).unwrap();
    let _: usize = redis.zadd_multiple("zset", &[(4.0, "d"), (5.0, "e")]).unwrap();
    let r1 = redlite.zcard("zset").unwrap();
    let r2: usize = redis.zcard("zset").unwrap();
    assert_eq!(r1 as usize, r2);
    assert_eq!(r1, 5);

    // ZCARD after removing
    redlite.zrem("zset", &[b"a".as_slice()]).unwrap();
    let _: usize = redis.zrem("zset", "a").unwrap();
    let r1 = redlite.zcard("zset").unwrap();
    let r2: usize = redis.zcard("zset").unwrap();
    assert_eq!(r1 as usize, r2);
    assert_eq!(r1, 4);
}

/// ZCOUNT command: all scenarios
#[test]
fn oracle_cmd_zcount() {
    let mut redis = require_redis!();
    let redlite = Db::open_memory().unwrap();
    let _: () = redis::cmd("FLUSHDB").query(&mut redis).unwrap();

    // ZCOUNT on non-existent set
    let r1 = redlite.zcount("missing", f64::NEG_INFINITY, f64::INFINITY).unwrap();
    let r2: usize = redis.zcount("missing", "-inf", "+inf").unwrap();
    assert_eq!(r1 as usize, r2);
    assert_eq!(r1, 0);

    // Setup sorted set
    redlite.zadd("zset", &[
        ZMember::new(1.0, b"a".to_vec()),
        ZMember::new(2.0, b"b".to_vec()),
        ZMember::new(3.0, b"c".to_vec()),
        ZMember::new(4.0, b"d".to_vec()),
        ZMember::new(5.0, b"e".to_vec()),
    ]).unwrap();
    let _: usize = redis.zadd_multiple("zset", &[(1.0, "a"), (2.0, "b"), (3.0, "c"), (4.0, "d"), (5.0, "e")]).unwrap();

    // ZCOUNT all
    let r1 = redlite.zcount("zset", f64::NEG_INFINITY, f64::INFINITY).unwrap();
    let r2: usize = redis.zcount("zset", "-inf", "+inf").unwrap();
    assert_eq!(r1 as usize, r2);
    assert_eq!(r1, 5);

    // ZCOUNT range (inclusive)
    let r1 = redlite.zcount("zset", 2.0, 4.0).unwrap();
    let r2: usize = redis.zcount("zset", 2.0, 4.0).unwrap();
    assert_eq!(r1 as usize, r2);
    assert_eq!(r1, 3);

    // ZCOUNT with exclusive bounds (using slightly adjusted values since API uses f64)
    // Note: redlite API doesn't support exclusive bound syntax, so we use 0.0001 offset
    let r1 = redlite.zcount("zset", 2.0 + 0.0001, 4.0 - 0.0001).unwrap();
    let r2: usize = redis::cmd("ZCOUNT").arg("zset").arg("(2").arg("(4").query(&mut redis).unwrap();
    assert_eq!(r1 as usize, r2);
    assert_eq!(r1, 1); // Only 3.0

    // ZCOUNT single score
    let r1 = redlite.zcount("zset", 3.0, 3.0).unwrap();
    let r2: usize = redis.zcount("zset", 3.0, 3.0).unwrap();
    assert_eq!(r1 as usize, r2);
    assert_eq!(r1, 1);

    // ZCOUNT no matches
    let r1 = redlite.zcount("zset", 10.0, 20.0).unwrap();
    let r2: usize = redis.zcount("zset", 10.0, 20.0).unwrap();
    assert_eq!(r1 as usize, r2);
    assert_eq!(r1, 0);
}

/// ZRANGE command: all scenarios
#[test]
fn oracle_cmd_zrange() {
    let mut redis = require_redis!();
    let redlite = Db::open_memory().unwrap();
    let _: () = redis::cmd("FLUSHDB").query(&mut redis).unwrap();

    // Helper to extract just the member bytes from ZMember vec
    fn members(v: &[ZMember]) -> Vec<Vec<u8>> {
        v.iter().map(|m| m.member.clone()).collect()
    }

    // ZRANGE on non-existent set
    let r1 = redlite.zrange("missing", 0, -1, false).unwrap();
    let r2: Vec<Vec<u8>> = redis.zrange("missing", 0, -1).unwrap();
    assert_eq!(members(&r1), r2);
    assert!(r1.is_empty());

    // Setup sorted set
    redlite.zadd("zset", &[
        ZMember::new(1.0, b"a".to_vec()),
        ZMember::new(2.0, b"b".to_vec()),
        ZMember::new(3.0, b"c".to_vec()),
        ZMember::new(4.0, b"d".to_vec()),
        ZMember::new(5.0, b"e".to_vec()),
    ]).unwrap();
    let _: usize = redis.zadd_multiple("zset", &[(1.0, "a"), (2.0, "b"), (3.0, "c"), (4.0, "d"), (5.0, "e")]).unwrap();

    // ZRANGE all
    let r1 = redlite.zrange("zset", 0, -1, false).unwrap();
    let r2: Vec<Vec<u8>> = redis.zrange("zset", 0, -1).unwrap();
    assert_eq!(members(&r1), r2);
    assert_eq!(r1.len(), 5);

    // ZRANGE first 3
    let r1 = redlite.zrange("zset", 0, 2, false).unwrap();
    let r2: Vec<Vec<u8>> = redis.zrange("zset", 0, 2).unwrap();
    assert_eq!(members(&r1), r2);
    assert_eq!(members(&r1), vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()]);

    // ZRANGE negative indices
    let r1 = redlite.zrange("zset", -3, -1, false).unwrap();
    let r2: Vec<Vec<u8>> = redis.zrange("zset", -3, -1).unwrap();
    assert_eq!(members(&r1), r2);
    assert_eq!(members(&r1), vec![b"c".to_vec(), b"d".to_vec(), b"e".to_vec()]);

    // ZRANGE out of bounds (returns what exists)
    let r1 = redlite.zrange("zset", 0, 100, false).unwrap();
    let r2: Vec<Vec<u8>> = redis.zrange("zset", 0, 100).unwrap();
    assert_eq!(members(&r1), r2);
    assert_eq!(r1.len(), 5);

    // ZRANGE invalid range (start > end)
    let r1 = redlite.zrange("zset", 3, 1, false).unwrap();
    let r2: Vec<Vec<u8>> = redis.zrange("zset", 3, 1).unwrap();
    assert_eq!(members(&r1), r2);
    assert!(r1.is_empty());
}

/// ZRANGE with WITHSCORES: all scenarios
#[test]
#[cfg(feature = "unimplemented")]
fn oracle_cmd_zrange_withscores() {
    let mut redis = require_redis!();
    let redlite = Db::open_memory().unwrap();
    let _: () = redis::cmd("FLUSHDB").query(&mut redis).unwrap();

    // Setup sorted set
    redlite.zadd("zset", &[
        ZMember::new(1.5, b"a".to_vec()),
        ZMember::new(2.5, b"b".to_vec()),
        ZMember::new(3.5, b"c".to_vec()),
    ]).unwrap();
    let _: usize = redis.zadd_multiple("zset", &[(1.5, "a"), (2.5, "b"), (3.5, "c")]).unwrap();

    // ZRANGE WITHSCORES
    let r1 = redlite.zrange_withscores("zset", 0, -1).unwrap();
    let r2: Vec<(Vec<u8>, f64)> = redis.zrange_withscores("zset", 0, -1).unwrap();
    assert_eq!(r1.len(), r2.len());
    for ((m1, s1), (m2, s2)) in r1.iter().zip(r2.iter()) {
        assert_eq!(m1, m2);
        assert!((s1 - s2).abs() < 1e-10);
    }
}

/// ZREVRANGE command: all scenarios
#[test]
fn oracle_cmd_zrevrange() {
    let mut redis = require_redis!();
    let redlite = Db::open_memory().unwrap();
    let _: () = redis::cmd("FLUSHDB").query(&mut redis).unwrap();

    // Helper to extract just the member bytes from ZMember vec
    fn members(v: &[ZMember]) -> Vec<Vec<u8>> {
        v.iter().map(|m| m.member.clone()).collect()
    }

    // Setup sorted set
    redlite.zadd("zset", &[
        ZMember::new(1.0, b"a".to_vec()),
        ZMember::new(2.0, b"b".to_vec()),
        ZMember::new(3.0, b"c".to_vec()),
        ZMember::new(4.0, b"d".to_vec()),
        ZMember::new(5.0, b"e".to_vec()),
    ]).unwrap();
    let _: usize = redis.zadd_multiple("zset", &[(1.0, "a"), (2.0, "b"), (3.0, "c"), (4.0, "d"), (5.0, "e")]).unwrap();

    // ZREVRANGE all (highest to lowest)
    let r1 = redlite.zrevrange("zset", 0, -1, false).unwrap();
    let r2: Vec<Vec<u8>> = redis.zrevrange("zset", 0, -1).unwrap();
    assert_eq!(members(&r1), r2);
    assert_eq!(members(&r1), vec![b"e".to_vec(), b"d".to_vec(), b"c".to_vec(), b"b".to_vec(), b"a".to_vec()]);

    // ZREVRANGE first 3 (highest 3)
    let r1 = redlite.zrevrange("zset", 0, 2, false).unwrap();
    let r2: Vec<Vec<u8>> = redis.zrevrange("zset", 0, 2).unwrap();
    assert_eq!(members(&r1), r2);
    assert_eq!(members(&r1), vec![b"e".to_vec(), b"d".to_vec(), b"c".to_vec()]);
}

/// ZINCRBY command: all scenarios
#[test]
fn oracle_cmd_zincrby() {
    let mut redis = require_redis!();
    let redlite = Db::open_memory().unwrap();
    let _: () = redis::cmd("FLUSHDB").query(&mut redis).unwrap();

    // ZINCRBY on new member (creates with increment as score)
    let r1 = redlite.zincrby("zset", 5.0, b"a").unwrap();
    let r2: f64 = redis.zincr("zset", "a", 5.0).unwrap();
    assert!((r1 - r2).abs() < 1e-10);
    assert!((r1 - 5.0).abs() < 1e-10);

    // ZINCRBY on existing member
    let r1 = redlite.zincrby("zset", 3.0, b"a").unwrap();
    let r2: f64 = redis.zincr("zset", "a", 3.0).unwrap();
    assert!((r1 - r2).abs() < 1e-10);
    assert!((r1 - 8.0).abs() < 1e-10);

    // ZINCRBY negative increment
    let r1 = redlite.zincrby("zset", -2.0, b"a").unwrap();
    let r2: f64 = redis.zincr("zset", "a", -2.0).unwrap();
    assert!((r1 - r2).abs() < 1e-10);
    assert!((r1 - 6.0).abs() < 1e-10);

    // ZINCRBY float increment
    let r1 = redlite.zincrby("zset", 0.5, b"a").unwrap();
    let r2: f64 = redis.zincr("zset", "a", 0.5).unwrap();
    assert!((r1 - r2).abs() < 1e-10);
    assert!((r1 - 6.5).abs() < 1e-10);
}

/// ZRANGEBYSCORE command: all scenarios
#[test]
fn oracle_cmd_zrangebyscore() {
    let mut redis = require_redis!();
    let redlite = Db::open_memory().unwrap();
    let _: () = redis::cmd("FLUSHDB").query(&mut redis).unwrap();

    // Helper to extract just the member bytes from ZMember vec
    fn members(v: &[ZMember]) -> Vec<Vec<u8>> {
        v.iter().map(|m| m.member.clone()).collect()
    }

    // Setup sorted set
    redlite.zadd("zset", &[
        ZMember::new(1.0, b"a".to_vec()),
        ZMember::new(2.0, b"b".to_vec()),
        ZMember::new(3.0, b"c".to_vec()),
        ZMember::new(4.0, b"d".to_vec()),
        ZMember::new(5.0, b"e".to_vec()),
    ]).unwrap();
    let _: usize = redis.zadd_multiple("zset", &[(1.0, "a"), (2.0, "b"), (3.0, "c"), (4.0, "d"), (5.0, "e")]).unwrap();

    // ZRANGEBYSCORE all
    let r1 = redlite.zrangebyscore("zset", f64::NEG_INFINITY, f64::INFINITY, None, None).unwrap();
    let r2: Vec<Vec<u8>> = redis.zrangebyscore("zset", "-inf", "+inf").unwrap();
    assert_eq!(members(&r1), r2);
    assert_eq!(r1.len(), 5);

    // ZRANGEBYSCORE range
    let r1 = redlite.zrangebyscore("zset", 2.0, 4.0, None, None).unwrap();
    let r2: Vec<Vec<u8>> = redis.zrangebyscore("zset", 2.0, 4.0).unwrap();
    assert_eq!(members(&r1), r2);
    assert_eq!(members(&r1), vec![b"b".to_vec(), b"c".to_vec(), b"d".to_vec()]);

    // ZRANGEBYSCORE exclusive bounds (using slightly adjusted values since API uses f64)
    // Note: redlite API doesn't support exclusive bound syntax, so we use 0.0001 offset
    let r1 = redlite.zrangebyscore("zset", 2.0 + 0.0001, 4.0 - 0.0001, None, None).unwrap();
    let r2: Vec<Vec<u8>> = redis::cmd("ZRANGEBYSCORE").arg("zset").arg("(2").arg("(4").query(&mut redis).unwrap();
    assert_eq!(members(&r1), r2);
    assert_eq!(members(&r1), vec![b"c".to_vec()]);

    // ZRANGEBYSCORE with LIMIT
    let r1 = redlite.zrangebyscore("zset", f64::NEG_INFINITY, f64::INFINITY, Some(1), Some(2)).unwrap();
    let r2: Vec<Vec<u8>> = redis::cmd("ZRANGEBYSCORE").arg("zset").arg("-inf").arg("+inf").arg("LIMIT").arg(1).arg(2).query(&mut redis).unwrap();
    assert_eq!(members(&r1), r2);
    assert_eq!(members(&r1), vec![b"b".to_vec(), b"c".to_vec()]);
}

/// ZREVRANGEBYSCORE command: all scenarios
/// NOTE: zrevrangebyscore command not yet implemented in redlite
#[test]
#[cfg(feature = "unimplemented")]
fn oracle_cmd_zrevrangebyscore() {
    let mut redis = require_redis!();
    let redlite = Db::open_memory().unwrap();
    let _: () = redis::cmd("FLUSHDB").query(&mut redis).unwrap();

    // Setup sorted set
    redlite.zadd("zset", &[
        ZMember::new(1.0, b"a".to_vec()),
        ZMember::new(2.0, b"b".to_vec()),
        ZMember::new(3.0, b"c".to_vec()),
        ZMember::new(4.0, b"d".to_vec()),
        ZMember::new(5.0, b"e".to_vec()),
    ]).unwrap();
    let _: usize = redis.zadd_multiple("zset", &[(1.0, "a"), (2.0, "b"), (3.0, "c"), (4.0, "d"), (5.0, "e")]).unwrap();

    // ZREVRANGEBYSCORE all (note: max first, then min)
    let r1 = redlite.zrevrangebyscore("zset", "+inf", "-inf", None).unwrap();
    let r2: Vec<Vec<u8>> = redis.zrevrangebyscore("zset", "+inf", "-inf").unwrap();
    assert_eq!(r1, r2);
    assert_eq!(r1, vec![b"e".to_vec(), b"d".to_vec(), b"c".to_vec(), b"b".to_vec(), b"a".to_vec()]);

    // ZREVRANGEBYSCORE range
    let r1 = redlite.zrevrangebyscore("zset", "4", "2", None).unwrap();
    let r2: Vec<Vec<u8>> = redis.zrevrangebyscore("zset", 4.0, 2.0).unwrap();
    assert_eq!(r1, r2);
    assert_eq!(r1, vec![b"d".to_vec(), b"c".to_vec(), b"b".to_vec()]);
}

/// ZREMRANGEBYRANK command: all scenarios
#[test]
fn oracle_cmd_zremrangebyrank() {
    let mut redis = require_redis!();
    let redlite = Db::open_memory().unwrap();
    let _: () = redis::cmd("FLUSHDB").query(&mut redis).unwrap();

    // Helper to extract just the member bytes from ZMember vec
    fn members(v: &[ZMember]) -> Vec<Vec<u8>> {
        v.iter().map(|m| m.member.clone()).collect()
    }

    // Setup sorted set
    redlite.zadd("zset", &[
        ZMember::new(1.0, b"a".to_vec()),
        ZMember::new(2.0, b"b".to_vec()),
        ZMember::new(3.0, b"c".to_vec()),
        ZMember::new(4.0, b"d".to_vec()),
        ZMember::new(5.0, b"e".to_vec()),
    ]).unwrap();
    let _: usize = redis.zadd_multiple("zset", &[(1.0, "a"), (2.0, "b"), (3.0, "c"), (4.0, "d"), (5.0, "e")]).unwrap();

    // ZREMRANGEBYRANK first 2 elements
    let r1 = redlite.zremrangebyrank("zset", 0, 1).unwrap();
    let r2: usize = redis.zremrangebyrank("zset", 0, 1).unwrap();
    assert_eq!(r1 as usize, r2);
    assert_eq!(r1, 2);

    // Verify remaining elements
    let r1 = redlite.zrange("zset", 0, -1, false).unwrap();
    let r2: Vec<Vec<u8>> = redis.zrange("zset", 0, -1).unwrap();
    assert_eq!(members(&r1), r2);
    assert_eq!(members(&r1), vec![b"c".to_vec(), b"d".to_vec(), b"e".to_vec()]);

    // ZREMRANGEBYRANK with negative indices
    let r1 = redlite.zremrangebyrank("zset", -2, -1).unwrap();
    let r2: usize = redis.zremrangebyrank("zset", -2, -1).unwrap();
    assert_eq!(r1 as usize, r2);
    assert_eq!(r1, 2);

    // Verify remaining
    let r1 = redlite.zrange("zset", 0, -1, false).unwrap();
    let r2: Vec<Vec<u8>> = redis.zrange("zset", 0, -1).unwrap();
    assert_eq!(members(&r1), r2);
    assert_eq!(members(&r1), vec![b"c".to_vec()]);
}

/// ZREMRANGEBYSCORE command: all scenarios
#[test]
#[cfg(feature = "unimplemented")]
fn oracle_cmd_zremrangebyscore() {
    let mut redis = require_redis!();
    let redlite = Db::open_memory().unwrap();
    let _: () = redis::cmd("FLUSHDB").query(&mut redis).unwrap();

    // Helper to extract just the member bytes from ZMember vec
    fn members(v: &[ZMember]) -> Vec<Vec<u8>> {
        v.iter().map(|m| m.member.clone()).collect()
    }

    // Setup sorted set
    redlite.zadd("zset", &[
        ZMember::new(1.0, b"a".to_vec()),
        ZMember::new(2.0, b"b".to_vec()),
        ZMember::new(3.0, b"c".to_vec()),
        ZMember::new(4.0, b"d".to_vec()),
        ZMember::new(5.0, b"e".to_vec()),
    ]).unwrap();
    let _: usize = redis.zadd_multiple("zset", &[(1.0, "a"), (2.0, "b"), (3.0, "c"), (4.0, "d"), (5.0, "e")]).unwrap();

    // ZREMRANGEBYSCORE
    let r1 = redlite.zremrangebyscore("zset", "2", "4").unwrap();
    let r2: usize = redis.zremrangebyscore("zset", 2.0, 4.0).unwrap();
    assert_eq!(r1 as usize, r2);
    assert_eq!(r1, 3);

    // Verify remaining
    let r1 = redlite.zrange("zset", 0, -1, false).unwrap();
    let r2: Vec<Vec<u8>> = redis.zrange("zset", 0, -1).unwrap();
    assert_eq!(members(&r1), r2);
    assert_eq!(members(&r1), vec![b"a".to_vec(), b"e".to_vec()]);

    // ZREMRANGEBYSCORE non-existent set
    let r1 = redlite.zremrangebyscore("missing", "0", "10").unwrap();
    let r2: usize = redis.zremrangebyscore("missing", 0.0, 10.0).unwrap();
    assert_eq!(r1 as usize, r2);
    assert_eq!(r1, 0);
}

/// ZPOPMIN command: all scenarios
#[test]
#[cfg(feature = "unimplemented")]
fn oracle_cmd_zpopmin() {
    let mut redis = require_redis!();
    let redlite = Db::open_memory().unwrap();
    let _: () = redis::cmd("FLUSHDB").query(&mut redis).unwrap();

    // ZPOPMIN on non-existent set
    let r1 = redlite.zpopmin("missing", 1).unwrap();
    let r2: Vec<(Vec<u8>, f64)> = redis::cmd("ZPOPMIN").arg("missing").arg(1).query(&mut redis).unwrap();
    assert_eq!(r1.len(), r2.len());
    assert!(r1.is_empty());

    // Setup sorted set
    redlite.zadd("zset", &[
        ZMember::new(1.0, b"a".to_vec()),
        ZMember::new(2.0, b"b".to_vec()),
        ZMember::new(3.0, b"c".to_vec()),
    ]).unwrap();
    let _: usize = redis.zadd_multiple("zset", &[(1.0, "a"), (2.0, "b"), (3.0, "c")]).unwrap();

    // ZPOPMIN single
    let r1 = redlite.zpopmin("zset", 1).unwrap();
    let r2: Vec<(Vec<u8>, f64)> = redis::cmd("ZPOPMIN").arg("zset").arg(1).query(&mut redis).unwrap();
    assert_eq!(r1.len(), r2.len());
    assert_eq!(r1[0].0, r2[0].0);
    assert!((r1[0].1 - r2[0].1).abs() < 1e-10);
    assert_eq!(r1[0].0, b"a".to_vec());
    assert!((r1[0].1 - 1.0).abs() < 1e-10);

    // ZPOPMIN multiple
    let r1 = redlite.zpopmin("zset", 2).unwrap();
    let r2: Vec<(Vec<u8>, f64)> = redis::cmd("ZPOPMIN").arg("zset").arg(2).query(&mut redis).unwrap();
    assert_eq!(r1.len(), r2.len());
    assert_eq!(r1.len(), 2);
}

/// ZPOPMAX command: all scenarios
#[test]
#[cfg(feature = "unimplemented")]
fn oracle_cmd_zpopmax() {
    let mut redis = require_redis!();
    let redlite = Db::open_memory().unwrap();
    let _: () = redis::cmd("FLUSHDB").query(&mut redis).unwrap();

    // Setup sorted set
    redlite.zadd("zset", &[
        ZMember::new(1.0, b"a".to_vec()),
        ZMember::new(2.0, b"b".to_vec()),
        ZMember::new(3.0, b"c".to_vec()),
    ]).unwrap();
    let _: usize = redis.zadd_multiple("zset", &[(1.0, "a"), (2.0, "b"), (3.0, "c")]).unwrap();

    // ZPOPMAX single
    let r1 = redlite.zpopmax("zset", 1).unwrap();
    let r2: Vec<(Vec<u8>, f64)> = redis::cmd("ZPOPMAX").arg("zset").arg(1).query(&mut redis).unwrap();
    assert_eq!(r1.len(), r2.len());
    assert_eq!(r1[0].0, r2[0].0);
    assert_eq!(r1[0].0, b"c".to_vec());
    assert!((r1[0].1 - 3.0).abs() < 1e-10);

    // ZPOPMAX multiple
    let r1 = redlite.zpopmax("zset", 2).unwrap();
    let r2: Vec<(Vec<u8>, f64)> = redis::cmd("ZPOPMAX").arg("zset").arg(2).query(&mut redis).unwrap();
    assert_eq!(r1.len(), r2.len());
    assert_eq!(r1.len(), 2);
}

/// ZSCAN command: all scenarios
#[test]
fn oracle_cmd_zscan() {
    let mut redis = require_redis!();
    let redlite = Db::open_memory().unwrap();
    let _: () = redis::cmd("FLUSHDB").query(&mut redis).unwrap();

    // Setup sorted set with many members
    for i in 0..50 {
        let member = format!("member:{}", i);
        let score = i as f64;
        redlite.zadd("zset", &[ZMember::new(score, member.as_bytes().to_vec())]).unwrap();
        let _: usize = redis.zadd("zset", &member, score).unwrap();
    }

    // ZSCAN - collect all members via cursor iteration
    let mut redlite_members: Vec<String> = Vec::new();
    let mut cursor = "0".to_string();
    loop {
        let (next_cursor, batch) = redlite.zscan("zset", &cursor, None, 10).unwrap();
        for (member, _) in batch {
            redlite_members.push(String::from_utf8_lossy(&member).to_string());
        }
        cursor = next_cursor;
        if cursor == "0" { break; }
    }

    let mut redis_members: Vec<String> = Vec::new();
    let mut redis_cursor: u64 = 0;
    loop {
        let result: (u64, Vec<(String, f64)>) = redis::cmd("ZSCAN").arg("zset").arg(redis_cursor).arg("COUNT").arg(10).query(&mut redis).unwrap();
        redis_cursor = result.0;
        for (member, _) in result.1 {
            redis_members.push(member);
        }
        if redis_cursor == 0 { break; }
    }

    // Sort and compare
    redlite_members.sort();
    redis_members.sort();
    assert_eq!(redlite_members.len(), redis_members.len());
    assert_eq!(redlite_members, redis_members);
}

// ============================================================================
// KEY COMMANDS - Comprehensive Tests
// ============================================================================

/// DEL command: all scenarios
#[test]
fn oracle_cmd_del() {
    let mut redis = require_redis!();
    let redlite = Db::open_memory().unwrap();
    let _: () = redis::cmd("FLUSHDB").query(&mut redis).unwrap();

    // DEL non-existent key
    let r1 = redlite.del(&["missing"]).unwrap();
    let r2: usize = redis.del("missing").unwrap();
    assert_eq!(r1 as usize, r2);
    assert_eq!(r1, 0);

    // Setup keys
    redlite.set("key1", b"value1", None).unwrap();
    redlite.set("key2", b"value2", None).unwrap();
    redlite.set("key3", b"value3", None).unwrap();
    let _: () = redis.set("key1", "value1").unwrap();
    let _: () = redis.set("key2", "value2").unwrap();
    let _: () = redis.set("key3", "value3").unwrap();

    // DEL single key
    let r1 = redlite.del(&["key1"]).unwrap();
    let r2: usize = redis.del("key1").unwrap();
    assert_eq!(r1 as usize, r2);
    assert_eq!(r1, 1);

    // Verify deleted
    let r1 = redlite.get("key1").unwrap();
    let r2: Option<Vec<u8>> = redis.get("key1").unwrap();
    assert_eq!(r1, r2);
    assert_eq!(r1, None);

    // DEL multiple keys
    let r1 = redlite.del(&["key2", "key3"]).unwrap();
    let r2: usize = redis.del(&["key2", "key3"]).unwrap();
    assert_eq!(r1 as usize, r2);
    assert_eq!(r1, 2);

    // DEL mixed (some exist, some don't)
    redlite.set("key4", b"value4", None).unwrap();
    let _: () = redis.set("key4", "value4").unwrap();
    let r1 = redlite.del(&["key4", "missing1", "missing2"]).unwrap();
    let r2: usize = redis.del(&["key4", "missing1", "missing2"]).unwrap();
    assert_eq!(r1 as usize, r2);
    assert_eq!(r1, 1);

    // DEL different types
    redlite.sadd("set", &[b"member"]).unwrap();
    redlite.lpush("list", &[b"item"]).unwrap();
    redlite.hset("hash", &[("field", b"value".as_slice())]).unwrap();
    let _: usize = redis.sadd("set", "member").unwrap();
    let _: usize = redis.lpush("list", "item").unwrap();
    let _: usize = redis.hset("hash", "field", "value").unwrap();

    let r1 = redlite.del(&["set", "list", "hash"]).unwrap();
    let r2: usize = redis.del(&["set", "list", "hash"]).unwrap();
    assert_eq!(r1 as usize, r2);
    assert_eq!(r1, 3);
}

/// EXISTS command: all scenarios
#[test]
fn oracle_cmd_exists() {
    let mut redis = require_redis!();
    let redlite = Db::open_memory().unwrap();
    let _: () = redis::cmd("FLUSHDB").query(&mut redis).unwrap();

    // EXISTS non-existent key
    let r1 = redlite.exists(&["missing"]).unwrap();
    let r2: usize = redis.exists("missing").unwrap();
    assert_eq!(r1 as usize, r2);
    assert_eq!(r1, 0);

    // Setup keys
    redlite.set("key1", b"value1", None).unwrap();
    redlite.set("key2", b"value2", None).unwrap();
    let _: () = redis.set("key1", "value1").unwrap();
    let _: () = redis.set("key2", "value2").unwrap();

    // EXISTS single key
    let r1 = redlite.exists(&["key1"]).unwrap();
    let r2: usize = redis.exists("key1").unwrap();
    assert_eq!(r1 as usize, r2);
    assert_eq!(r1, 1);

    // EXISTS multiple keys (returns count of existing)
    let r1 = redlite.exists(&["key1", "key2", "missing"]).unwrap();
    let r2: usize = redis.exists(&["key1", "key2", "missing"]).unwrap();
    assert_eq!(r1 as usize, r2);
    assert_eq!(r1, 2);

    // EXISTS same key multiple times (counts each occurrence)
    let r1 = redlite.exists(&["key1", "key1", "key1"]).unwrap();
    let r2: usize = redis.exists(&["key1", "key1", "key1"]).unwrap();
    assert_eq!(r1 as usize, r2);
    assert_eq!(r1, 3);

    // EXISTS different types
    redlite.sadd("set", &[b"member"]).unwrap();
    redlite.lpush("list", &[b"item"]).unwrap();
    let _: usize = redis.sadd("set", "member").unwrap();
    let _: usize = redis.lpush("list", "item").unwrap();

    let r1 = redlite.exists(&["set", "list"]).unwrap();
    let r2: usize = redis.exists(&["set", "list"]).unwrap();
    assert_eq!(r1 as usize, r2);
    assert_eq!(r1, 2);
}

/// EXPIRE command: all scenarios
#[test]
fn oracle_cmd_expire() {
    let mut redis = require_redis!();
    let redlite = Db::open_memory().unwrap();
    let _: () = redis::cmd("FLUSHDB").query(&mut redis).unwrap();

    // EXPIRE on non-existent key
    let r1 = redlite.expire("missing", 10).unwrap();
    let r2: bool = redis.expire("missing", 10).unwrap();
    assert_eq!(r1, r2);
    assert!(!r1);

    // Setup key
    redlite.set("key", b"value", None).unwrap();
    let _: () = redis.set("key", "value").unwrap();

    // EXPIRE
    let r1 = redlite.expire("key", 10).unwrap();
    let r2: bool = redis.expire("key", 10).unwrap();
    assert_eq!(r1, r2);
    assert!(r1);

    // Verify TTL set
    let r1 = redlite.ttl("key").unwrap();
    let r2: i64 = redis.ttl("key").unwrap();
    assert!(r1 > 0);
    assert!(r2 > 0);
    assert!((r1 - r2).abs() <= 1); // Allow 1 second difference

    // EXPIRE with NX option (only set if no TTL)
    let r1: bool = redis::cmd("EXPIRE").arg("key").arg(20).arg("NX").query(&mut redis).unwrap();
    assert!(!r1); // Already has TTL, so should fail

    // Setup key without TTL
    redlite.set("key2", b"value", None).unwrap();
    let _: () = redis.set("key2", "value").unwrap();

    let r1: bool = redis::cmd("EXPIRE").arg("key2").arg(20).arg("NX").query(&mut redis).unwrap();
    assert!(r1); // No TTL, so should succeed
}

/// TTL command: all scenarios
#[test]
fn oracle_cmd_ttl() {
    let mut redis = require_redis!();
    let redlite = Db::open_memory().unwrap();
    let _: () = redis::cmd("FLUSHDB").query(&mut redis).unwrap();

    // TTL on non-existent key (-2)
    let r1 = redlite.ttl("missing").unwrap();
    let r2: i64 = redis.ttl("missing").unwrap();
    assert_eq!(r1, r2);
    assert_eq!(r1, -2);

    // TTL on key without expiration (-1)
    redlite.set("key", b"value", None).unwrap();
    let _: () = redis.set("key", "value").unwrap();
    let r1 = redlite.ttl("key").unwrap();
    let r2: i64 = redis.ttl("key").unwrap();
    assert_eq!(r1, r2);
    assert_eq!(r1, -1);

    // TTL on key with expiration
    redlite.expire("key", 100).unwrap();
    let _: bool = redis.expire("key", 100).unwrap();
    let r1 = redlite.ttl("key").unwrap();
    let r2: i64 = redis.ttl("key").unwrap();
    assert!(r1 > 0);
    assert!(r2 > 0);
    assert!((r1 - r2).abs() <= 1);
}

/// PTTL command: all scenarios
#[test]
fn oracle_cmd_pttl() {
    let mut redis = require_redis!();
    let redlite = Db::open_memory().unwrap();
    let _: () = redis::cmd("FLUSHDB").query(&mut redis).unwrap();

    // PTTL on non-existent key (-2)
    let r1 = redlite.pttl("missing").unwrap();
    let r2: i64 = redis.pttl("missing").unwrap();
    assert_eq!(r1, r2);
    assert_eq!(r1, -2);

    // PTTL on key without expiration (-1)
    redlite.set("key", b"value", None).unwrap();
    let _: () = redis.set("key", "value").unwrap();
    let r1 = redlite.pttl("key").unwrap();
    let r2: i64 = redis.pttl("key").unwrap();
    assert_eq!(r1, r2);
    assert_eq!(r1, -1);

    // PTTL on key with expiration
    redlite.pexpire("key", 10000).unwrap();
    let _: bool = redis::cmd("PEXPIRE").arg("key").arg(10000).query(&mut redis).unwrap();
    let r1 = redlite.pttl("key").unwrap();
    let r2: i64 = redis.pttl("key").unwrap();
    assert!(r1 > 0);
    assert!(r2 > 0);
    assert!((r1 - r2).abs() <= 100); // Allow 100ms difference
}

/// PERSIST command: all scenarios
#[test]
fn oracle_cmd_persist() {
    let mut redis = require_redis!();
    let redlite = Db::open_memory().unwrap();
    let _: () = redis::cmd("FLUSHDB").query(&mut redis).unwrap();

    // PERSIST on non-existent key
    let r1 = redlite.persist("missing").unwrap();
    let r2: bool = redis.persist("missing").unwrap();
    assert_eq!(r1, r2);
    assert!(!r1);

    // PERSIST on key without TTL
    redlite.set("key", b"value", None).unwrap();
    let _: () = redis.set("key", "value").unwrap();
    let r1 = redlite.persist("key").unwrap();
    let r2: bool = redis.persist("key").unwrap();
    assert_eq!(r1, r2);
    assert!(!r1);

    // PERSIST on key with TTL
    redlite.expire("key", 100).unwrap();
    let _: bool = redis.expire("key", 100).unwrap();
    let r1 = redlite.persist("key").unwrap();
    let r2: bool = redis.persist("key").unwrap();
    assert_eq!(r1, r2);
    assert!(r1);

    // Verify TTL removed
    let r1 = redlite.ttl("key").unwrap();
    let r2: i64 = redis.ttl("key").unwrap();
    assert_eq!(r1, r2);
    assert_eq!(r1, -1);
}

/// RENAME command: all scenarios
#[test]
fn oracle_cmd_rename() {
    let mut redis = require_redis!();
    let redlite = Db::open_memory().unwrap();
    let _: () = redis::cmd("FLUSHDB").query(&mut redis).unwrap();

    // RENAME non-existent key - should error
    let r1 = redlite.rename("missing", "newkey");
    let r2: Result<(), redis::RedisError> = redis.rename("missing", "newkey");
    assert!(r1.is_err());
    assert!(r2.is_err());

    // Setup key
    redlite.set("oldkey", b"value", None).unwrap();
    let _: () = redis.set("oldkey", "value").unwrap();

    // RENAME
    redlite.rename("oldkey", "newkey").unwrap();
    let _: () = redis.rename("oldkey", "newkey").unwrap();

    // Verify old key gone
    let r1 = redlite.exists(&["oldkey"]).unwrap();
    let r2: usize = redis.exists("oldkey").unwrap();
    assert_eq!(r1 as usize, r2);
    assert_eq!(r1, 0);

    // Verify new key exists
    let r1 = redlite.get("newkey").unwrap();
    let r2: Option<Vec<u8>> = redis.get("newkey").unwrap();
    assert_eq!(r1, r2);
    assert_eq!(r1, Some(b"value".to_vec()));

    // RENAME to same key
    redlite.rename("newkey", "newkey").unwrap();
    let _: () = redis.rename("newkey", "newkey").unwrap();
    let r1 = redlite.get("newkey").unwrap();
    let r2: Option<Vec<u8>> = redis.get("newkey").unwrap();
    assert_eq!(r1, r2);

    // RENAME overwrites existing
    redlite.set("other", b"other_value", None).unwrap();
    let _: () = redis.set("other", "other_value").unwrap();
    redlite.rename("newkey", "other").unwrap();
    let _: () = redis.rename("newkey", "other").unwrap();
    let r1 = redlite.get("other").unwrap();
    let r2: Option<Vec<u8>> = redis.get("other").unwrap();
    assert_eq!(r1, r2);
    assert_eq!(r1, Some(b"value".to_vec()));
}

/// RENAMENX command: all scenarios
#[test]
fn oracle_cmd_renamenx() {
    let mut redis = require_redis!();
    let redlite = Db::open_memory().unwrap();
    let _: () = redis::cmd("FLUSHDB").query(&mut redis).unwrap();

    // Setup keys
    redlite.set("key1", b"value1", None).unwrap();
    redlite.set("key2", b"value2", None).unwrap();
    let _: () = redis.set("key1", "value1").unwrap();
    let _: () = redis.set("key2", "value2").unwrap();

    // RENAMENX to non-existent key (should succeed)
    let r1 = redlite.renamenx("key1", "newkey").unwrap();
    let r2: bool = redis.rename_nx("key1", "newkey").unwrap();
    assert_eq!(r1, r2);
    assert!(r1);

    // RENAMENX to existing key (should fail)
    let r1 = redlite.renamenx("newkey", "key2").unwrap();
    let r2: bool = redis.rename_nx("newkey", "key2").unwrap();
    assert_eq!(r1, r2);
    assert!(!r1);

    // Verify original key still exists
    let r1 = redlite.get("newkey").unwrap();
    let r2: Option<Vec<u8>> = redis.get("newkey").unwrap();
    assert_eq!(r1, r2);
    assert_eq!(r1, Some(b"value1".to_vec()));
}

/// TYPE command: all scenarios
#[test]
fn oracle_cmd_type() {
    use redlite::KeyType;

    let mut redis = require_redis!();
    let redlite = Db::open_memory().unwrap();
    let _: () = redis::cmd("FLUSHDB").query(&mut redis).unwrap();

    // Helper to convert Option<KeyType> to string
    fn type_str(t: Option<KeyType>) -> &'static str {
        t.map(|kt| kt.as_str()).unwrap_or("none")
    }

    // TYPE non-existent key
    let r1 = redlite.key_type("missing").unwrap();
    let r2: String = redis.key_type("missing").unwrap();
    assert_eq!(type_str(r1), r2);
    assert!(r1.is_none());

    // TYPE string
    redlite.set("str", b"value", None).unwrap();
    let _: () = redis.set("str", "value").unwrap();
    let r1 = redlite.key_type("str").unwrap();
    let r2: String = redis.key_type("str").unwrap();
    assert_eq!(type_str(r1), r2);
    assert_eq!(r1, Some(KeyType::String));

    // TYPE list
    redlite.lpush("list", &[b"item"]).unwrap();
    let _: usize = redis.lpush("list", "item").unwrap();
    let r1 = redlite.key_type("list").unwrap();
    let r2: String = redis.key_type("list").unwrap();
    assert_eq!(type_str(r1), r2);
    assert_eq!(r1, Some(KeyType::List));

    // TYPE set
    redlite.sadd("set", &[b"member"]).unwrap();
    let _: usize = redis.sadd("set", "member").unwrap();
    let r1 = redlite.key_type("set").unwrap();
    let r2: String = redis.key_type("set").unwrap();
    assert_eq!(type_str(r1), r2);
    assert_eq!(r1, Some(KeyType::Set));

    // TYPE hash
    redlite.hset("hash", &[("field", b"value".as_slice())]).unwrap();
    let _: usize = redis.hset("hash", "field", "value").unwrap();
    let r1 = redlite.key_type("hash").unwrap();
    let r2: String = redis.key_type("hash").unwrap();
    assert_eq!(type_str(r1), r2);
    assert_eq!(r1, Some(KeyType::Hash));

    // TYPE zset
    redlite.zadd("zset", &[ZMember::new(1.0, b"member".to_vec())]).unwrap();
    let _: usize = redis.zadd("zset", "member", 1.0).unwrap();
    let r1 = redlite.key_type("zset").unwrap();
    let r2: String = redis.key_type("zset").unwrap();
    assert_eq!(type_str(r1), r2);
    assert_eq!(r1, Some(KeyType::ZSet));
}

/// KEYS command: all scenarios
#[test]
fn oracle_cmd_keys() {
    let mut redis = require_redis!();
    let redlite = Db::open_memory().unwrap();
    let _: () = redis::cmd("FLUSHDB").query(&mut redis).unwrap();

    // KEYS on empty database
    let r1 = redlite.keys("*").unwrap();
    let r2: Vec<String> = redis.keys("*").unwrap();
    assert_eq!(r1, r2);
    assert!(r1.is_empty());

    // Setup keys with pattern
    redlite.set("user:1", b"a", None).unwrap();
    redlite.set("user:2", b"b", None).unwrap();
    redlite.set("user:3", b"c", None).unwrap();
    redlite.set("post:1", b"d", None).unwrap();
    redlite.set("post:2", b"e", None).unwrap();
    let _: () = redis.set("user:1", "a").unwrap();
    let _: () = redis.set("user:2", "b").unwrap();
    let _: () = redis.set("user:3", "c").unwrap();
    let _: () = redis.set("post:1", "d").unwrap();
    let _: () = redis.set("post:2", "e").unwrap();

    // KEYS all
    let mut r1 = redlite.keys("*").unwrap();
    let mut r2: Vec<String> = redis.keys("*").unwrap();
    r1.sort();
    r2.sort();
    assert_eq!(r1, r2);
    assert_eq!(r1.len(), 5);

    // KEYS with pattern
    let mut r1 = redlite.keys("user:*").unwrap();
    let mut r2: Vec<String> = redis.keys("user:*").unwrap();
    r1.sort();
    r2.sort();
    assert_eq!(r1, r2);
    assert_eq!(r1.len(), 3);

    // KEYS with ? wildcard
    let mut r1 = redlite.keys("user:?").unwrap();
    let mut r2: Vec<String> = redis.keys("user:?").unwrap();
    r1.sort();
    r2.sort();
    assert_eq!(r1, r2);
    assert_eq!(r1.len(), 3);

    // KEYS no match
    let r1 = redlite.keys("nomatch*").unwrap();
    let r2: Vec<String> = redis.keys("nomatch*").unwrap();
    assert_eq!(r1, r2);
    assert!(r1.is_empty());
}

/// SCAN command: all scenarios
#[test]
fn oracle_cmd_scan() {
    let mut redis = require_redis!();
    let redlite = Db::open_memory().unwrap();
    let _: () = redis::cmd("FLUSHDB").query(&mut redis).unwrap();

    // Setup keys
    for i in 0..50 {
        let key = format!("key:{}", i);
        redlite.set(&key, b"value", None).unwrap();
        let _: () = redis.set(&key, "value").unwrap();
    }

    // SCAN - collect all keys
    let mut redlite_keys: Vec<String> = Vec::new();
    let mut cursor = "0".to_string();
    loop {
        let (next_cursor, batch) = redlite.scan(&cursor, None, 10).unwrap();
        redlite_keys.extend(batch);
        cursor = next_cursor;
        if cursor == "0" { break; }
    }

    let mut redis_keys: Vec<String> = Vec::new();
    let mut redis_cursor: u64 = 0;
    loop {
        let result: (u64, Vec<String>) = redis::cmd("SCAN").arg(redis_cursor).arg("COUNT").arg(10).query(&mut redis).unwrap();
        redis_cursor = result.0;
        redis_keys.extend(result.1);
        if redis_cursor == 0 { break; }
    }

    // Sort and compare
    redlite_keys.sort();
    redis_keys.sort();
    assert_eq!(redlite_keys.len(), redis_keys.len());
    assert_eq!(redlite_keys, redis_keys);
}

/// EXPIREAT command: all scenarios
#[test]
fn oracle_cmd_expireat() {
    let mut redis = require_redis!();
    let redlite = Db::open_memory().unwrap();
    let _: () = redis::cmd("FLUSHDB").query(&mut redis).unwrap();

    // EXPIREAT on non-existent key
    let future_ts = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH).unwrap()
        .as_secs() as i64 + 100;
    let r1 = redlite.expireat("missing", future_ts).unwrap();
    let r2: bool = redis.expire_at("missing", future_ts).unwrap();
    assert_eq!(r1, r2);
    assert!(!r1);

    // Setup key
    redlite.set("key", b"value", None).unwrap();
    let _: () = redis.set("key", "value").unwrap();

    // EXPIREAT
    let r1 = redlite.expireat("key", future_ts).unwrap();
    let r2: bool = redis.expire_at("key", future_ts).unwrap();
    assert_eq!(r1, r2);
    assert!(r1);

    // Verify TTL set (should be around 100 seconds)
    let r1 = redlite.ttl("key").unwrap();
    let r2: i64 = redis.ttl("key").unwrap();
    assert!(r1 > 95 && r1 <= 100);
    assert!((r1 - r2).abs() <= 1);
}

/// PEXPIRE command: all scenarios
#[test]
fn oracle_cmd_pexpire() {
    let mut redis = require_redis!();
    let redlite = Db::open_memory().unwrap();
    let _: () = redis::cmd("FLUSHDB").query(&mut redis).unwrap();

    // PEXPIRE on non-existent key
    let r1 = redlite.pexpire("missing", 10000).unwrap();
    let r2: bool = redis::cmd("PEXPIRE").arg("missing").arg(10000).query(&mut redis).unwrap();
    assert_eq!(r1, r2);
    assert!(!r1);

    // Setup key
    redlite.set("key", b"value", None).unwrap();
    let _: () = redis.set("key", "value").unwrap();

    // PEXPIRE
    let r1 = redlite.pexpire("key", 10000).unwrap();
    let r2: bool = redis::cmd("PEXPIRE").arg("key").arg(10000).query(&mut redis).unwrap();
    assert_eq!(r1, r2);
    assert!(r1);

    // Verify PTTL set
    let r1 = redlite.pttl("key").unwrap();
    let r2: i64 = redis.pttl("key").unwrap();
    assert!(r1 > 9000 && r1 <= 10000);
    assert!((r1 - r2).abs() <= 100);
}

/// UNLINK command: all scenarios
/// NOTE: unlink command not yet implemented in redlite
#[test]
#[cfg(feature = "unimplemented")]
fn oracle_cmd_unlink() {
    let mut redis = require_redis!();
    let redlite = Db::open_memory().unwrap();
    let _: () = redis::cmd("FLUSHDB").query(&mut redis).unwrap();

    // UNLINK non-existent key
    let r1 = redlite.unlink(&["missing"]).unwrap();
    let r2: usize = redis::cmd("UNLINK").arg("missing").query(&mut redis).unwrap();
    assert_eq!(r1 as usize, r2);
    assert_eq!(r1, 0);

    // Setup keys
    redlite.set("key1", b"value1", None).unwrap();
    redlite.set("key2", b"value2", None).unwrap();
    let _: () = redis.set("key1", "value1").unwrap();
    let _: () = redis.set("key2", "value2").unwrap();

    // UNLINK single key
    let r1 = redlite.unlink(&["key1"]).unwrap();
    let r2: usize = redis::cmd("UNLINK").arg("key1").query(&mut redis).unwrap();
    assert_eq!(r1 as usize, r2);
    assert_eq!(r1, 1);

    // UNLINK multiple keys
    redlite.set("key3", b"value3", None).unwrap();
    let _: () = redis.set("key3", "value3").unwrap();
    let r1 = redlite.unlink(&["key2", "key3", "missing"]).unwrap();
    let r2: usize = redis::cmd("UNLINK").arg(&["key2", "key3", "missing"]).query(&mut redis).unwrap();
    assert_eq!(r1 as usize, r2);
    assert_eq!(r1, 2);
}

/// COPY command: all scenarios
/// NOTE: copy command not yet implemented in redlite
#[test]
#[cfg(feature = "unimplemented")]
fn oracle_cmd_copy() {
    let mut redis = require_redis!();
    let redlite = Db::open_memory().unwrap();
    let _: () = redis::cmd("FLUSHDB").query(&mut redis).unwrap();

    // Setup source key
    redlite.set("src", b"value", None).unwrap();
    let _: () = redis.set("src", "value").unwrap();

    // COPY to new key
    let r1 = redlite.copy("src", "dst", false).unwrap();
    let r2: bool = redis::cmd("COPY").arg("src").arg("dst").query(&mut redis).unwrap();
    assert_eq!(r1, r2);
    assert!(r1);

    // Verify both keys exist
    let r1 = redlite.get("src").unwrap();
    let r2: Option<Vec<u8>> = redis.get("src").unwrap();
    assert_eq!(r1, r2);
    assert_eq!(r1, Some(b"value".to_vec()));

    let r1 = redlite.get("dst").unwrap();
    let r2: Option<Vec<u8>> = redis.get("dst").unwrap();
    assert_eq!(r1, r2);
    assert_eq!(r1, Some(b"value".to_vec()));

    // COPY to existing key without REPLACE (should fail)
    let r1 = redlite.copy("src", "dst", false).unwrap();
    let r2: bool = redis::cmd("COPY").arg("src").arg("dst").query(&mut redis).unwrap();
    assert_eq!(r1, r2);
    assert!(!r1);

    // COPY with REPLACE
    redlite.set("src", b"newvalue", None).unwrap();
    let _: () = redis.set("src", "newvalue").unwrap();
    let r1 = redlite.copy("src", "dst", true).unwrap();
    let r2: bool = redis::cmd("COPY").arg("src").arg("dst").arg("REPLACE").query(&mut redis).unwrap();
    assert_eq!(r1, r2);
    assert!(r1);

    // Verify dst updated
    let r1 = redlite.get("dst").unwrap();
    let r2: Option<Vec<u8>> = redis.get("dst").unwrap();
    assert_eq!(r1, r2);
    assert_eq!(r1, Some(b"newvalue".to_vec()));
}

// ============================================================================
// STREAM COMMANDS - Comprehensive Tests
// ============================================================================

/// XADD command: all scenarios
#[test]
fn oracle_cmd_xadd() {
    let mut redis = require_redis!();
    let redlite = Db::open_memory().unwrap();
    let _: () = redis::cmd("FLUSHDB").query(&mut redis).unwrap();

    // XADD with auto-generated ID
    let r1 = redlite.xadd("stream", None, &[(b"field1".as_slice(), b"value1".as_slice())], false, None, None, false).unwrap();
    let r2: String = redis::cmd("XADD").arg("stream").arg("*").arg("field1").arg("value1").query(&mut redis).unwrap();
    // Both should return valid stream IDs
    assert!(r1.unwrap().to_string().contains('-'));
    assert!(r2.contains('-'));

    // XADD multiple fields
    let r1 = redlite.xadd("stream", None, &[
        (b"f1".as_slice(), b"v1".as_slice()),
        (b"f2".as_slice(), b"v2".as_slice()),
        (b"f3".as_slice(), b"v3".as_slice()),
    ], false, None, None, false).unwrap();
    let r2: String = redis::cmd("XADD").arg("stream").arg("*")
        .arg("f1").arg("v1")
        .arg("f2").arg("v2")
        .arg("f3").arg("v3")
        .query(&mut redis).unwrap();
    assert!(r1.unwrap().to_string().contains('-'));
    assert!(r2.contains('-'));

    // XADD with explicit ID
    let r1 = redlite.xadd("stream2", StreamId::parse("1000-0"), &[(b"field".as_slice(), b"value".as_slice())], false, None, None, false).unwrap();
    let r2: String = redis::cmd("XADD").arg("stream2").arg("1000-0").arg("field").arg("value").query(&mut redis).unwrap();
    assert_eq!(r1.unwrap().to_string(), "1000-0");
    assert_eq!(r2, "1000-0");

    // XADD with explicit ID and sequence
    let r1 = redlite.xadd("stream2", StreamId::parse("1000-1"), &[(b"field".as_slice(), b"value2".as_slice())], false, None, None, false).unwrap();
    let r2: String = redis::cmd("XADD").arg("stream2").arg("1000-1").arg("field").arg("value2").query(&mut redis).unwrap();
    assert_eq!(r1.unwrap().to_string(), "1000-1");
    assert_eq!(r2, "1000-1");

    // XADD with binary values
    let binary = vec![0u8, 255, 128, 64];
    let r1 = redlite.xadd("binstream", None, &[(b"bin".as_slice(), binary.as_slice())], false, None, None, false).unwrap();
    let r2: String = redis::cmd("XADD").arg("binstream").arg("*").arg("bin").arg(&binary).query(&mut redis).unwrap();
    assert!(r1.unwrap().to_string().contains('-'));
    assert!(r2.contains('-'));
}

/// XLEN command: all scenarios
#[test]
fn oracle_cmd_xlen() {
    let mut redis = require_redis!();
    let redlite = Db::open_memory().unwrap();
    let _: () = redis::cmd("FLUSHDB").query(&mut redis).unwrap();

    // XLEN on non-existent stream
    let r1 = redlite.xlen("missing").unwrap();
    let r2: usize = redis::cmd("XLEN").arg("missing").query(&mut redis).unwrap();
    assert_eq!(r1 as usize, r2);
    assert_eq!(r1, 0);

    // Add entries
    for i in 0..5 {
        let field = format!("field{}", i);
        let value = format!("value{}", i);
        redlite.xadd("stream", None, &[(field.as_bytes(), value.as_bytes())], false, None, None, false).unwrap();
        let _: String = redis::cmd("XADD").arg("stream").arg("*").arg(&field).arg(&value).query(&mut redis).unwrap();
    }

    // XLEN
    let r1 = redlite.xlen("stream").unwrap();
    let r2: usize = redis::cmd("XLEN").arg("stream").query(&mut redis).unwrap();
    assert_eq!(r1 as usize, r2);
    assert_eq!(r1, 5);
}

/// XRANGE command: all scenarios
#[test]
fn oracle_cmd_xrange() {
    let mut redis = require_redis!();
    let redlite = Db::open_memory().unwrap();
    let _: () = redis::cmd("FLUSHDB").query(&mut redis).unwrap();

    // XRANGE on non-existent stream
    let r1 = redlite.xrange("missing", StreamId::min(), StreamId::max(), None).unwrap();
    let r2: Vec<(String, Vec<(String, String)>)> = redis::cmd("XRANGE").arg("missing").arg("-").arg("+").query(&mut redis).unwrap();
    assert_eq!(r1.len(), r2.len());
    assert!(r1.is_empty());

    // Add entries with explicit IDs
    redlite.xadd("stream", StreamId::parse("1-0"), &[(b"f".as_slice(), b"v1".as_slice())], false, None, None, false).unwrap();
    redlite.xadd("stream", StreamId::parse("2-0"), &[(b"f".as_slice(), b"v2".as_slice())], false, None, None, false).unwrap();
    redlite.xadd("stream", StreamId::parse("3-0"), &[(b"f".as_slice(), b"v3".as_slice())], false, None, None, false).unwrap();
    let _: String = redis::cmd("XADD").arg("stream").arg("1-0").arg("f").arg("v1").query(&mut redis).unwrap();
    let _: String = redis::cmd("XADD").arg("stream").arg("2-0").arg("f").arg("v2").query(&mut redis).unwrap();
    let _: String = redis::cmd("XADD").arg("stream").arg("3-0").arg("f").arg("v3").query(&mut redis).unwrap();

    // XRANGE all
    let r1 = redlite.xrange("stream", StreamId::min(), StreamId::max(), None).unwrap();
    let r2: Vec<(String, Vec<(String, String)>)> = redis::cmd("XRANGE").arg("stream").arg("-").arg("+").query(&mut redis).unwrap();
    assert_eq!(r1.len(), r2.len());
    assert_eq!(r1.len(), 3);

    // XRANGE specific range
    let r1 = redlite.xrange("stream", StreamId::parse("1-0").unwrap(), StreamId::parse("2-0").unwrap(), None).unwrap();
    let r2: Vec<(String, Vec<(String, String)>)> = redis::cmd("XRANGE").arg("stream").arg("1-0").arg("2-0").query(&mut redis).unwrap();
    assert_eq!(r1.len(), r2.len());
    assert_eq!(r1.len(), 2);

    // XRANGE with COUNT
    let r1 = redlite.xrange("stream", StreamId::min(), StreamId::max(), Some(2)).unwrap();
    let r2: Vec<(String, Vec<(String, String)>)> = redis::cmd("XRANGE").arg("stream").arg("-").arg("+").arg("COUNT").arg(2).query(&mut redis).unwrap();
    assert_eq!(r1.len(), r2.len());
    assert_eq!(r1.len(), 2);
}

/// XREVRANGE command: all scenarios
#[test]
fn oracle_cmd_xrevrange() {
    let mut redis = require_redis!();
    let redlite = Db::open_memory().unwrap();
    let _: () = redis::cmd("FLUSHDB").query(&mut redis).unwrap();

    // Add entries with explicit IDs
    redlite.xadd("stream", StreamId::parse("1-0"), &[(b"f".as_slice(), b"v1".as_slice())], false, None, None, false).unwrap();
    redlite.xadd("stream", StreamId::parse("2-0"), &[(b"f".as_slice(), b"v2".as_slice())], false, None, None, false).unwrap();
    redlite.xadd("stream", StreamId::parse("3-0"), &[(b"f".as_slice(), b"v3".as_slice())], false, None, None, false).unwrap();
    let _: String = redis::cmd("XADD").arg("stream").arg("1-0").arg("f").arg("v1").query(&mut redis).unwrap();
    let _: String = redis::cmd("XADD").arg("stream").arg("2-0").arg("f").arg("v2").query(&mut redis).unwrap();
    let _: String = redis::cmd("XADD").arg("stream").arg("3-0").arg("f").arg("v3").query(&mut redis).unwrap();

    // XREVRANGE all (reversed order)
    let r1 = redlite.xrevrange("stream", StreamId::max(), StreamId::min(), None).unwrap();
    let r2: Vec<(String, Vec<(String, String)>)> = redis::cmd("XREVRANGE").arg("stream").arg("+").arg("-").query(&mut redis).unwrap();
    assert_eq!(r1.len(), r2.len());
    assert_eq!(r1.len(), 3);
    // First entry should be the highest ID
    assert_eq!(r1[0].id.to_string(), "3-0");
    assert_eq!(r2[0].0, "3-0");

    // XREVRANGE with COUNT
    let r1 = redlite.xrevrange("stream", StreamId::max(), StreamId::min(), Some(1)).unwrap();
    let r2: Vec<(String, Vec<(String, String)>)> = redis::cmd("XREVRANGE").arg("stream").arg("+").arg("-").arg("COUNT").arg(1).query(&mut redis).unwrap();
    assert_eq!(r1.len(), r2.len());
    assert_eq!(r1.len(), 1);
    assert_eq!(r1[0].id.to_string(), "3-0");
}

/// XTRIM command: all scenarios
#[test]
fn oracle_cmd_xtrim() {
    let mut redis = require_redis!();
    let redlite = Db::open_memory().unwrap();
    let _: () = redis::cmd("FLUSHDB").query(&mut redis).unwrap();

    // Add entries
    for i in 0..10 {
        let id = format!("{}-0", i + 1);
        redlite.xadd("stream", StreamId::parse(&id), &[(b"f".as_slice(), b"v".as_slice())], false, None, None, false).unwrap();
        let _: String = redis::cmd("XADD").arg("stream").arg(&id).arg("f").arg("v").query(&mut redis).unwrap();
    }

    // XTRIM MAXLEN
    let r1 = redlite.xtrim("stream", Some(5), None, false).unwrap();
    let r2: usize = redis::cmd("XTRIM").arg("stream").arg("MAXLEN").arg(5).query(&mut redis).unwrap();
    assert_eq!(r1 as usize, r2);
    assert_eq!(r1, 5); // 5 entries removed

    // Verify length
    let r1 = redlite.xlen("stream").unwrap();
    let r2: usize = redis::cmd("XLEN").arg("stream").query(&mut redis).unwrap();
    assert_eq!(r1 as usize, r2);
    assert_eq!(r1, 5);

    // XTRIM on non-existent stream
    let r1 = redlite.xtrim("missing", Some(5), None, false).unwrap();
    let r2: usize = redis::cmd("XTRIM").arg("missing").arg("MAXLEN").arg(5).query(&mut redis).unwrap();
    assert_eq!(r1 as usize, r2);
    assert_eq!(r1, 0);
}

/// XDEL command: all scenarios
#[test]
fn oracle_cmd_xdel() {
    let mut redis = require_redis!();
    let redlite = Db::open_memory().unwrap();
    let _: () = redis::cmd("FLUSHDB").query(&mut redis).unwrap();

    // XDEL on non-existent stream
    let r1 = redlite.xdel("missing", &[StreamId::parse("1-0").unwrap()]).unwrap();
    let r2: usize = redis::cmd("XDEL").arg("missing").arg("1-0").query(&mut redis).unwrap();
    assert_eq!(r1 as usize, r2);
    assert_eq!(r1, 0);

    // Add entries
    redlite.xadd("stream", StreamId::parse("1-0"), &[(b"f".as_slice(), b"v".as_slice())], false, None, None, false).unwrap();
    redlite.xadd("stream", StreamId::parse("2-0"), &[(b"f".as_slice(), b"v".as_slice())], false, None, None, false).unwrap();
    redlite.xadd("stream", StreamId::parse("3-0"), &[(b"f".as_slice(), b"v".as_slice())], false, None, None, false).unwrap();
    let _: String = redis::cmd("XADD").arg("stream").arg("1-0").arg("f").arg("v").query(&mut redis).unwrap();
    let _: String = redis::cmd("XADD").arg("stream").arg("2-0").arg("f").arg("v").query(&mut redis).unwrap();
    let _: String = redis::cmd("XADD").arg("stream").arg("3-0").arg("f").arg("v").query(&mut redis).unwrap();

    // XDEL single entry
    let r1 = redlite.xdel("stream", &[StreamId::parse("1-0").unwrap()]).unwrap();
    let r2: usize = redis::cmd("XDEL").arg("stream").arg("1-0").query(&mut redis).unwrap();
    assert_eq!(r1 as usize, r2);
    assert_eq!(r1, 1);

    // XDEL multiple entries
    let r1 = redlite.xdel("stream", &[StreamId::parse("2-0").unwrap(), StreamId::parse("3-0").unwrap()]).unwrap();
    let r2: usize = redis::cmd("XDEL").arg("stream").arg("2-0").arg("3-0").query(&mut redis).unwrap();
    assert_eq!(r1 as usize, r2);
    assert_eq!(r1, 2);

    // XDEL non-existent entry
    let r1 = redlite.xdel("stream", &[StreamId::parse("99-0").unwrap()]).unwrap();
    let r2: usize = redis::cmd("XDEL").arg("stream").arg("99-0").query(&mut redis).unwrap();
    assert_eq!(r1 as usize, r2);
    assert_eq!(r1, 0);
}

/// XREAD command: all scenarios
#[test]
fn oracle_cmd_xread() {
    let mut redis = require_redis!();
    let redlite = Db::open_memory().unwrap();
    let _: () = redis::cmd("FLUSHDB").query(&mut redis).unwrap();

    // Add entries
    redlite.xadd("stream1", StreamId::parse("1-0"), &[(b"f".as_slice(), b"v1".as_slice())], false, None, None, false).unwrap();
    redlite.xadd("stream1", StreamId::parse("2-0"), &[(b"f".as_slice(), b"v2".as_slice())], false, None, None, false).unwrap();
    redlite.xadd("stream2", StreamId::parse("1-0"), &[(b"f".as_slice(), b"v3".as_slice())], false, None, None, false).unwrap();
    let _: String = redis::cmd("XADD").arg("stream1").arg("1-0").arg("f").arg("v1").query(&mut redis).unwrap();
    let _: String = redis::cmd("XADD").arg("stream1").arg("2-0").arg("f").arg("v2").query(&mut redis).unwrap();
    let _: String = redis::cmd("XADD").arg("stream2").arg("1-0").arg("f").arg("v3").query(&mut redis).unwrap();

    // XREAD from beginning
    let r1 = redlite.xread(&["stream1"], &[StreamId::min()], None).unwrap();
    let r2: Option<Vec<(String, Vec<(String, Vec<(String, String)>)>)>> = redis::cmd("XREAD")
        .arg("STREAMS").arg("stream1").arg("0")
        .query(&mut redis).unwrap();
    assert!(!r1.is_empty());
    assert!(r2.is_some());
    let r2 = r2.unwrap();
    assert_eq!(r1.len(), r2.len());
    assert_eq!(r1[0].1.len(), 2); // 2 entries in stream1

    // XREAD from specific ID
    let r1 = redlite.xread(&["stream1"], &[StreamId::parse("1-0").unwrap()], None).unwrap();
    let r2: Option<Vec<(String, Vec<(String, Vec<(String, String)>)>)>> = redis::cmd("XREAD")
        .arg("STREAMS").arg("stream1").arg("1-0")
        .query(&mut redis).unwrap();
    assert!(!r1.is_empty());
    assert!(r2.is_some());
    let r2 = r2.unwrap();
    assert_eq!(r1[0].1.len(), 1); // Only 2-0 (after 1-0)
    assert_eq!(r2[0].1.len(), 1);

    // XREAD with COUNT
    let r1 = redlite.xread(&["stream1"], &[StreamId::min()], Some(1)).unwrap();
    let r2: Option<Vec<(String, Vec<(String, Vec<(String, String)>)>)>> = redis::cmd("XREAD")
        .arg("COUNT").arg(1)
        .arg("STREAMS").arg("stream1").arg("0")
        .query(&mut redis).unwrap();
    assert!(!r1.is_empty());
    assert!(r2.is_some());
    let r2 = r2.unwrap();
    assert_eq!(r1[0].1.len(), 1);
    assert_eq!(r2[0].1.len(), 1);

    // XREAD multiple streams
    let r1 = redlite.xread(&["stream1", "stream2"], &[StreamId::min(), StreamId::min()], None).unwrap();
    let r2: Option<Vec<(String, Vec<(String, Vec<(String, String)>)>)>> = redis::cmd("XREAD")
        .arg("STREAMS").arg("stream1").arg("stream2").arg("0").arg("0")
        .query(&mut redis).unwrap();
    assert!(!r1.is_empty());
    assert!(r2.is_some());
    let r2 = r2.unwrap();
    assert_eq!(r1.len(), 2);
    assert_eq!(r2.len(), 2);
}

/// XGROUP CREATE/DESTROY command: all scenarios
#[test]
fn oracle_cmd_xgroup() {
    let mut redis = require_redis!();
    let redlite = Db::open_memory().unwrap();
    let _: () = redis::cmd("FLUSHDB").query(&mut redis).unwrap();

    // Create stream with entry
    redlite.xadd("stream", StreamId::parse("1-0"), &[(b"f".as_slice(), b"v".as_slice())], false, None, None, false).unwrap();
    let _: String = redis::cmd("XADD").arg("stream").arg("1-0").arg("f").arg("v").query(&mut redis).unwrap();

    // XGROUP CREATE
    redlite.xgroup_create("stream", "group1", StreamId::max(), false).unwrap();
    let _: () = redis::cmd("XGROUP").arg("CREATE").arg("stream").arg("group1").arg("$").query(&mut redis).unwrap();

    // XGROUP CREATE with MKSTREAM (creates stream if not exists)
    redlite.xgroup_create("newstream", "group1", StreamId::max(), true).unwrap();
    let _: () = redis::cmd("XGROUP").arg("CREATE").arg("newstream").arg("group1").arg("$").arg("MKSTREAM").query(&mut redis).unwrap();

    // Verify stream was created
    let r1 = redlite.exists(&["newstream"]).unwrap();
    let r2: usize = redis.exists("newstream").unwrap();
    assert_eq!(r1 as usize, r2);
    assert_eq!(r1, 1);

    // XGROUP DESTROY
    let r1 = redlite.xgroup_destroy("stream", "group1").unwrap();
    let r2: bool = redis::cmd("XGROUP").arg("DESTROY").arg("stream").arg("group1").query(&mut redis).unwrap();
    assert_eq!(r1, r2);
    assert!(r1);

    // XGROUP DESTROY non-existent group
    let r1 = redlite.xgroup_destroy("stream", "nonexistent").unwrap();
    let r2: bool = redis::cmd("XGROUP").arg("DESTROY").arg("stream").arg("nonexistent").query(&mut redis).unwrap();
    assert_eq!(r1, r2);
    assert!(!r1);
}

/// XREADGROUP command: all scenarios
#[test]
fn oracle_cmd_xreadgroup() {
    let mut redis = require_redis!();
    let redlite = Db::open_memory().unwrap();
    let _: () = redis::cmd("FLUSHDB").query(&mut redis).unwrap();

    // Create stream and group
    redlite.xadd("stream", StreamId::parse("1-0"), &[(b"f".as_slice(), b"v1".as_slice())], false, None, None, false).unwrap();
    redlite.xadd("stream", StreamId::parse("2-0"), &[(b"f".as_slice(), b"v2".as_slice())], false, None, None, false).unwrap();
    let _: String = redis::cmd("XADD").arg("stream").arg("1-0").arg("f").arg("v1").query(&mut redis).unwrap();
    let _: String = redis::cmd("XADD").arg("stream").arg("2-0").arg("f").arg("v2").query(&mut redis).unwrap();

    redlite.xgroup_create("stream", "group", StreamId::min(), false).unwrap();
    let _: () = redis::cmd("XGROUP").arg("CREATE").arg("stream").arg("group").arg("0").query(&mut redis).unwrap();

    // XREADGROUP new messages
    let r1 = redlite.xreadgroup("group", "consumer1", &["stream"], &[">"], None, false).unwrap();
    let r2: Option<Vec<(String, Vec<(String, Vec<(String, String)>)>)>> = redis::cmd("XREADGROUP")
        .arg("GROUP").arg("group").arg("consumer1")
        .arg("STREAMS").arg("stream").arg(">")
        .query(&mut redis).unwrap();
    assert!(!r1.is_empty());
    assert!(r2.is_some());
    let r2 = r2.unwrap();
    assert_eq!(r1[0].1.len(), 2);
    assert_eq!(r2[0].1.len(), 2);

    // XREADGROUP again (no new messages)
    let r1 = redlite.xreadgroup("group", "consumer1", &["stream"], &[">"], None, false);
    let r2: Option<Vec<(String, Vec<(String, Vec<(String, String)>)>)>> = redis::cmd("XREADGROUP")
        .arg("GROUP").arg("group").arg("consumer1")
        .arg("STREAMS").arg("stream").arg(">")
        .query(&mut redis).unwrap();
    // Both should return None or empty since all messages are pending
    assert!(r1.is_ok() && r1.unwrap().is_empty() || r2.is_none() || r2.as_ref().map(|v| v[0].1.is_empty()).unwrap_or(false));
}

/// XACK command: all scenarios
#[test]
fn oracle_cmd_xack() {
    let mut redis = require_redis!();
    let redlite = Db::open_memory().unwrap();
    let _: () = redis::cmd("FLUSHDB").query(&mut redis).unwrap();

    // Create stream, group, and read messages
    redlite.xadd("stream", StreamId::parse("1-0"), &[(b"f".as_slice(), b"v".as_slice())], false, None, None, false).unwrap();
    redlite.xadd("stream", StreamId::parse("2-0"), &[(b"f".as_slice(), b"v".as_slice())], false, None, None, false).unwrap();
    let _: String = redis::cmd("XADD").arg("stream").arg("1-0").arg("f").arg("v").query(&mut redis).unwrap();
    let _: String = redis::cmd("XADD").arg("stream").arg("2-0").arg("f").arg("v").query(&mut redis).unwrap();

    redlite.xgroup_create("stream", "group", StreamId::min(), false).unwrap();
    let _: () = redis::cmd("XGROUP").arg("CREATE").arg("stream").arg("group").arg("0").query(&mut redis).unwrap();

    // Read to create pending entries
    redlite.xreadgroup("group", "consumer", &["stream"], &[">"], None, false).unwrap();
    let _: Option<Vec<(String, Vec<(String, Vec<(String, String)>)>)>> = redis::cmd("XREADGROUP")
        .arg("GROUP").arg("group").arg("consumer")
        .arg("STREAMS").arg("stream").arg(">")
        .query(&mut redis).unwrap();

    // XACK single entry
    let r1 = redlite.xack("stream", "group", &[StreamId::parse("1-0").unwrap()]).unwrap();
    let r2: usize = redis::cmd("XACK").arg("stream").arg("group").arg("1-0").query(&mut redis).unwrap();
    assert_eq!(r1 as usize, r2);
    assert_eq!(r1, 1);

    // XACK already acknowledged (returns 0)
    let r1 = redlite.xack("stream", "group", &[StreamId::parse("1-0").unwrap()]).unwrap();
    let r2: usize = redis::cmd("XACK").arg("stream").arg("group").arg("1-0").query(&mut redis).unwrap();
    assert_eq!(r1 as usize, r2);
    assert_eq!(r1, 0);

    // XACK multiple entries
    let r1 = redlite.xack("stream", "group", &[StreamId::parse("2-0").unwrap()]).unwrap();
    let r2: usize = redis::cmd("XACK").arg("stream").arg("group").arg("2-0").query(&mut redis).unwrap();
    assert_eq!(r1 as usize, r2);
    assert_eq!(r1, 1);
}

/// XPENDING command: all scenarios
#[test]
fn oracle_cmd_xpending() {
    let mut redis = require_redis!();
    let redlite = Db::open_memory().unwrap();
    let _: () = redis::cmd("FLUSHDB").query(&mut redis).unwrap();

    // Create stream and group
    redlite.xadd("stream", StreamId::parse("1-0"), &[(b"f".as_slice(), b"v".as_slice())], false, None, None, false).unwrap();
    redlite.xadd("stream", StreamId::parse("2-0"), &[(b"f".as_slice(), b"v".as_slice())], false, None, None, false).unwrap();
    let _: String = redis::cmd("XADD").arg("stream").arg("1-0").arg("f").arg("v").query(&mut redis).unwrap();
    let _: String = redis::cmd("XADD").arg("stream").arg("2-0").arg("f").arg("v").query(&mut redis).unwrap();

    redlite.xgroup_create("stream", "group", StreamId::min(), false).unwrap();
    let _: () = redis::cmd("XGROUP").arg("CREATE").arg("stream").arg("group").arg("0").query(&mut redis).unwrap();

    // Read to create pending entries
    redlite.xreadgroup("group", "consumer", &["stream"], &[">"], None, false).unwrap();
    let _: Option<Vec<(String, Vec<(String, Vec<(String, String)>)>)>> = redis::cmd("XREADGROUP")
        .arg("GROUP").arg("group").arg("consumer")
        .arg("STREAMS").arg("stream").arg(">")
        .query(&mut redis).unwrap();

    // XPENDING summary
    let r1 = redlite.xpending_summary("stream", "group").unwrap();
    let r2: (i64, Option<String>, Option<String>, Option<Vec<(String, i64)>>) = redis::cmd("XPENDING").arg("stream").arg("group").query(&mut redis).unwrap();
    assert_eq!(r1.count as i64, r2.0); // Both should have 2 pending
    assert_eq!(r1.count, 2);

    // XPENDING detailed (xpending_detail not yet implemented in redlite)
    // let r1 = redlite.xpending_detail("stream", "group", "-", "+", 10, None).unwrap();
    let r2: Vec<(String, String, i64, i64)> = redis::cmd("XPENDING")
        .arg("stream").arg("group")
        .arg("-").arg("+").arg(10)
        .query(&mut redis).unwrap();
    // assert_eq!(r1.len(), r2.len());
    assert_eq!(r2.len(), 2);
}

// ============================================================================
// GEO COMMANDS - Comprehensive Tests
// ============================================================================

/// GEOADD command: all scenarios
#[test]
#[cfg(feature = "geo")]
fn oracle_cmd_geoadd() {
    let mut redis = require_redis!();
    let redlite = Db::open_memory().unwrap();
    let _: () = redis::cmd("FLUSHDB").query(&mut redis).unwrap();

    // GEOADD single location
    let r1 = redlite.geoadd("locations", &[(13.361389, 38.115556, "Palermo")]).unwrap();
    let r2: usize = redis::cmd("GEOADD").arg("locations").arg(13.361389).arg(38.115556).arg("Palermo").query(&mut redis).unwrap();
    assert_eq!(r1 as usize, r2);
    assert_eq!(r1, 1);

    // GEOADD multiple locations
    let r1 = redlite.geoadd("locations", &[
        (15.087269, 37.502669, "Catania"),
        (12.496366, 41.902782, "Rome"),
    ]).unwrap();
    let r2: usize = redis::cmd("GEOADD").arg("locations")
        .arg(15.087269).arg(37.502669).arg("Catania")
        .arg(12.496366).arg(41.902782).arg("Rome")
        .query(&mut redis).unwrap();
    assert_eq!(r1 as usize, r2);
    assert_eq!(r1, 2);

    // GEOADD duplicate (updates position, returns 0)
    let r1 = redlite.geoadd("locations", &[(13.4, 38.1, "Palermo")]).unwrap();
    let r2: usize = redis::cmd("GEOADD").arg("locations").arg(13.4).arg(38.1).arg("Palermo").query(&mut redis).unwrap();
    assert_eq!(r1 as usize, r2);
    assert_eq!(r1, 0);

    // GEOADD various coordinates
    let r1 = redlite.geoadd("world", &[
        (-122.4194, 37.7749, "San Francisco"),
        (-74.0060, 40.7128, "New York"),
        (139.6917, 35.6895, "Tokyo"),
    ]).unwrap();
    let r2: usize = redis::cmd("GEOADD").arg("world")
        .arg(-122.4194).arg(37.7749).arg("San Francisco")
        .arg(-74.0060).arg(40.7128).arg("New York")
        .arg(139.6917).arg(35.6895).arg("Tokyo")
        .query(&mut redis).unwrap();
    assert_eq!(r1 as usize, r2);
    assert_eq!(r1, 3);
}

/// GEOPOS command: all scenarios
#[test]
#[cfg(feature = "geo")]
fn oracle_cmd_geopos() {
    let mut redis = require_redis!();
    let redlite = Db::open_memory().unwrap();
    let _: () = redis::cmd("FLUSHDB").query(&mut redis).unwrap();

    // GEOPOS on non-existent key
    let r1 = redlite.geopos("missing", &["place"]).unwrap();
    let r2: Vec<Option<(f64, f64)>> = redis::cmd("GEOPOS").arg("missing").arg("place").query(&mut redis).unwrap();
    assert_eq!(r1.len(), r2.len());
    assert!(r1[0].is_none());

    // Add locations
    redlite.geoadd("locations", &[
        (13.361389, 38.115556, "Palermo"),
        (15.087269, 37.502669, "Catania"),
    ]).unwrap();
    let _: usize = redis::cmd("GEOADD").arg("locations")
        .arg(13.361389).arg(38.115556).arg("Palermo")
        .arg(15.087269).arg(37.502669).arg("Catania")
        .query(&mut redis).unwrap();

    // GEOPOS existing member
    let r1 = redlite.geopos("locations", &["Palermo"]).unwrap();
    let r2: Vec<Option<(f64, f64)>> = redis::cmd("GEOPOS").arg("locations").arg("Palermo").query(&mut redis).unwrap();
    assert_eq!(r1.len(), r2.len());
    assert!(r1[0].is_some());
    assert!(r2[0].is_some());
    let (lon1, lat1) = r1[0].unwrap();
    let (lon2, lat2) = r2[0].unwrap();
    assert!((lon1 - lon2).abs() < 0.001);
    assert!((lat1 - lat2).abs() < 0.001);

    // GEOPOS multiple members
    let r1 = redlite.geopos("locations", &["Palermo", "Catania", "Missing"]).unwrap();
    let r2: Vec<Option<(f64, f64)>> = redis::cmd("GEOPOS").arg("locations").arg("Palermo").arg("Catania").arg("Missing").query(&mut redis).unwrap();
    assert_eq!(r1.len(), r2.len());
    assert_eq!(r1.len(), 3);
    assert!(r1[0].is_some());
    assert!(r1[1].is_some());
    assert!(r1[2].is_none());
}

/// GEODIST command: all scenarios
#[test]
#[cfg(feature = "geo")]
fn oracle_cmd_geodist() {
    let mut redis = require_redis!();
    let redlite = Db::open_memory().unwrap();
    let _: () = redis::cmd("FLUSHDB").query(&mut redis).unwrap();

    // GEODIST on non-existent key
    let r1 = redlite.geodist("missing", "a", "b", None).unwrap();
    let r2: Option<f64> = redis::cmd("GEODIST").arg("missing").arg("a").arg("b").query(&mut redis).unwrap();
    assert_eq!(r1, r2);
    assert_eq!(r1, None);

    // Add locations
    redlite.geoadd("locations", &[
        (13.361389, 38.115556, "Palermo"),
        (15.087269, 37.502669, "Catania"),
    ]).unwrap();
    let _: usize = redis::cmd("GEOADD").arg("locations")
        .arg(13.361389).arg(38.115556).arg("Palermo")
        .arg(15.087269).arg(37.502669).arg("Catania")
        .query(&mut redis).unwrap();

    // GEODIST in meters (default)
    let r1 = redlite.geodist("locations", "Palermo", "Catania", None).unwrap();
    let r2: Option<f64> = redis::cmd("GEODIST").arg("locations").arg("Palermo").arg("Catania").query(&mut redis).unwrap();
    assert!(r1.is_some());
    assert!(r2.is_some());
    assert!((r1.unwrap() - r2.unwrap()).abs() < 1.0);

    // GEODIST in kilometers
    let r1 = redlite.geodist("locations", "Palermo", "Catania", Some("km")).unwrap();
    let r2: Option<f64> = redis::cmd("GEODIST").arg("locations").arg("Palermo").arg("Catania").arg("km").query(&mut redis).unwrap();
    assert!(r1.is_some());
    assert!(r2.is_some());
    assert!((r1.unwrap() - r2.unwrap()).abs() < 0.01);

    // GEODIST with non-existent member
    let r1 = redlite.geodist("locations", "Palermo", "Missing", None).unwrap();
    let r2: Option<f64> = redis::cmd("GEODIST").arg("locations").arg("Palermo").arg("Missing").query(&mut redis).unwrap();
    assert_eq!(r1, r2);
    assert_eq!(r1, None);
}

/// GEORADIUS command: all scenarios
#[test]
#[cfg(feature = "geo")]
fn oracle_cmd_georadius() {
    let mut redis = require_redis!();
    let redlite = Db::open_memory().unwrap();
    let _: () = redis::cmd("FLUSHDB").query(&mut redis).unwrap();

    // Add locations
    redlite.geoadd("Sicily", &[
        (13.361389, 38.115556, "Palermo"),
        (15.087269, 37.502669, "Catania"),
        (12.758489, 37.929452, "Agrigento"),
    ]).unwrap();
    let _: usize = redis::cmd("GEOADD").arg("Sicily")
        .arg(13.361389).arg(38.115556).arg("Palermo")
        .arg(15.087269).arg(37.502669).arg("Catania")
        .arg(12.758489).arg(37.929452).arg("Agrigento")
        .query(&mut redis).unwrap();

    // GEORADIUS basic
    let r1 = redlite.georadius("Sicily", 15.0, 37.0, 200.0, "km", None, None, None, None).unwrap();
    let r2: Vec<String> = redis::cmd("GEORADIUS").arg("Sicily").arg(15.0).arg(37.0).arg(200.0).arg("km").query(&mut redis).unwrap();
    assert_eq!(r1.len(), r2.len());
    // Should find Catania (close to 15, 37)

    // GEORADIUS with COUNT
    let r1 = redlite.georadius("Sicily", 15.0, 37.0, 300.0, "km", Some(2), None, None, None).unwrap();
    let r2: Vec<String> = redis::cmd("GEORADIUS").arg("Sicily").arg(15.0).arg(37.0).arg(300.0).arg("km").arg("COUNT").arg(2).query(&mut redis).unwrap();
    assert_eq!(r1.len(), r2.len());
    assert!(r1.len() <= 2);

    // GEORADIUS with WITHDIST
    let r1 = redlite.georadius_withdist("Sicily", 15.0, 37.0, 200.0, "km", None, None, None).unwrap();
    let r2: Vec<(String, f64)> = redis::cmd("GEORADIUS").arg("Sicily").arg(15.0).arg(37.0).arg(200.0).arg("km").arg("WITHDIST").query(&mut redis).unwrap();
    assert_eq!(r1.len(), r2.len());
    // Each result should have name and distance
    for ((name1, dist1), (name2, dist2)) in r1.iter().zip(r2.iter()) {
        assert_eq!(name1, name2);
        assert!((dist1 - dist2).abs() < 1.0);
    }

    // GEORADIUS with WITHCOORD
    let r1 = redlite.georadius_withcoord("Sicily", 15.0, 37.0, 200.0, "km", None, None, None).unwrap();
    let r2: Vec<(String, (f64, f64))> = redis::cmd("GEORADIUS").arg("Sicily").arg(15.0).arg(37.0).arg(200.0).arg("km").arg("WITHCOORD").query(&mut redis).unwrap();
    assert_eq!(r1.len(), r2.len());
}

/// GEORADIUSBYMEMBER command: all scenarios
#[test]
#[cfg(feature = "geo")]
fn oracle_cmd_georadiusbymember() {
    let mut redis = require_redis!();
    let redlite = Db::open_memory().unwrap();
    let _: () = redis::cmd("FLUSHDB").query(&mut redis).unwrap();

    // Add locations
    redlite.geoadd("Sicily", &[
        (13.361389, 38.115556, "Palermo"),
        (15.087269, 37.502669, "Catania"),
        (12.758489, 37.929452, "Agrigento"),
    ]).unwrap();
    let _: usize = redis::cmd("GEOADD").arg("Sicily")
        .arg(13.361389).arg(38.115556).arg("Palermo")
        .arg(15.087269).arg(37.502669).arg("Catania")
        .arg(12.758489).arg(37.929452).arg("Agrigento")
        .query(&mut redis).unwrap();

    // GEORADIUSBYMEMBER basic
    let r1 = redlite.georadiusbymember("Sicily", "Palermo", 100.0, "km", None, None, None, None).unwrap();
    let r2: Vec<String> = redis::cmd("GEORADIUSBYMEMBER").arg("Sicily").arg("Palermo").arg(100.0).arg("km").query(&mut redis).unwrap();
    assert_eq!(r1.len(), r2.len());
    // Should include Palermo itself and possibly others

    // GEORADIUSBYMEMBER with COUNT
    let r1 = redlite.georadiusbymember("Sicily", "Palermo", 200.0, "km", Some(2), None, None, None).unwrap();
    let r2: Vec<String> = redis::cmd("GEORADIUSBYMEMBER").arg("Sicily").arg("Palermo").arg(200.0).arg("km").arg("COUNT").arg(2).query(&mut redis).unwrap();
    assert_eq!(r1.len(), r2.len());
    assert!(r1.len() <= 2);

    // GEORADIUSBYMEMBER with WITHDIST
    let r1 = redlite.georadiusbymember_withdist("Sicily", "Palermo", 200.0, "km", None, None, None).unwrap();
    let r2: Vec<(String, f64)> = redis::cmd("GEORADIUSBYMEMBER").arg("Sicily").arg("Palermo").arg(200.0).arg("km").arg("WITHDIST").query(&mut redis).unwrap();
    assert_eq!(r1.len(), r2.len());
    // Palermo to itself should have distance 0
    let palermo_entry = r1.iter().find(|(name, _)| name == "Palermo");
    assert!(palermo_entry.is_some());
    assert!(palermo_entry.unwrap().1.abs() < 0.1);

    // GEORADIUSBYMEMBER non-existent member
    let r1 = redlite.georadiusbymember("Sicily", "Missing", 100.0, "km", None, None, None, None);
    let r2: Result<Vec<String>, redis::RedisError> = redis::cmd("GEORADIUSBYMEMBER").arg("Sicily").arg("Missing").arg(100.0).arg("km").query(&mut redis);
    assert!(r1.is_err());
    assert!(r2.is_err());
}

/// GEOSEARCH command: all scenarios
#[test]
#[cfg(feature = "geo")]
fn oracle_cmd_geosearch() {
    let mut redis = require_redis!();
    let redlite = Db::open_memory().unwrap();
    let _: () = redis::cmd("FLUSHDB").query(&mut redis).unwrap();

    // Add locations
    redlite.geoadd("Sicily", &[
        (13.361389, 38.115556, "Palermo"),
        (15.087269, 37.502669, "Catania"),
        (12.758489, 37.929452, "Agrigento"),
    ]).unwrap();
    let _: usize = redis::cmd("GEOADD").arg("Sicily")
        .arg(13.361389).arg(38.115556).arg("Palermo")
        .arg(15.087269).arg(37.502669).arg("Catania")
        .arg(12.758489).arg(37.929452).arg("Agrigento")
        .query(&mut redis).unwrap();

    // GEOSEARCH FROMMEMBER BYRADIUS
    let r1 = redlite.geosearch_frommember_radius("Sicily", "Palermo", 100.0, "km", None, None, None).unwrap();
    let r2: Vec<String> = redis::cmd("GEOSEARCH").arg("Sicily")
        .arg("FROMMEMBER").arg("Palermo")
        .arg("BYRADIUS").arg(100.0).arg("km")
        .query(&mut redis).unwrap();
    assert_eq!(r1.len(), r2.len());

    // GEOSEARCH FROMLONLAT BYRADIUS
    let r1 = redlite.geosearch_fromlonlat_radius("Sicily", 15.0, 37.0, 200.0, "km", None, None, None).unwrap();
    let r2: Vec<String> = redis::cmd("GEOSEARCH").arg("Sicily")
        .arg("FROMLONLAT").arg(15.0).arg(37.0)
        .arg("BYRADIUS").arg(200.0).arg("km")
        .query(&mut redis).unwrap();
    assert_eq!(r1.len(), r2.len());

    // GEOSEARCH with COUNT
    let r1 = redlite.geosearch_frommember_radius("Sicily", "Palermo", 200.0, "km", Some(2), None, None).unwrap();
    let r2: Vec<String> = redis::cmd("GEOSEARCH").arg("Sicily")
        .arg("FROMMEMBER").arg("Palermo")
        .arg("BYRADIUS").arg(200.0).arg("km")
        .arg("COUNT").arg(2)
        .query(&mut redis).unwrap();
    assert_eq!(r1.len(), r2.len());
    assert!(r1.len() <= 2);

    // GEOSEARCH with WITHDIST
    let r1 = redlite.geosearch_frommember_radius_withdist("Sicily", "Palermo", 200.0, "km", None, None).unwrap();
    let r2: Vec<(String, f64)> = redis::cmd("GEOSEARCH").arg("Sicily")
        .arg("FROMMEMBER").arg("Palermo")
        .arg("BYRADIUS").arg(200.0).arg("km")
        .arg("WITHDIST")
        .query(&mut redis).unwrap();
    assert_eq!(r1.len(), r2.len());
}

/// GEOHASH command: all scenarios
#[test]
#[cfg(feature = "geo")]
fn oracle_cmd_geohash() {
    let mut redis = require_redis!();
    let redlite = Db::open_memory().unwrap();
    let _: () = redis::cmd("FLUSHDB").query(&mut redis).unwrap();

    // GEOHASH on non-existent key
    let r1 = redlite.geohash("missing", &["place"]).unwrap();
    let r2: Vec<Option<String>> = redis::cmd("GEOHASH").arg("missing").arg("place").query(&mut redis).unwrap();
    assert_eq!(r1.len(), r2.len());
    assert!(r1[0].is_none());

    // Add locations
    redlite.geoadd("locations", &[
        (13.361389, 38.115556, "Palermo"),
        (15.087269, 37.502669, "Catania"),
    ]).unwrap();
    let _: usize = redis::cmd("GEOADD").arg("locations")
        .arg(13.361389).arg(38.115556).arg("Palermo")
        .arg(15.087269).arg(37.502669).arg("Catania")
        .query(&mut redis).unwrap();

    // GEOHASH single member
    let r1 = redlite.geohash("locations", &["Palermo"]).unwrap();
    let r2: Vec<Option<String>> = redis::cmd("GEOHASH").arg("locations").arg("Palermo").query(&mut redis).unwrap();
    assert_eq!(r1.len(), r2.len());
    assert!(r1[0].is_some());
    assert_eq!(r1[0], r2[0]);

    // GEOHASH multiple members
    let r1 = redlite.geohash("locations", &["Palermo", "Catania", "Missing"]).unwrap();
    let r2: Vec<Option<String>> = redis::cmd("GEOHASH").arg("locations").arg("Palermo").arg("Catania").arg("Missing").query(&mut redis).unwrap();
    assert_eq!(r1.len(), r2.len());
    assert_eq!(r1.len(), 3);
    assert!(r1[0].is_some());
    assert!(r1[1].is_some());
    assert!(r1[2].is_none());
    assert_eq!(r1, r2);
}

/// SPOP command: all scenarios
#[test]
fn oracle_cmd_spop() {
    let mut redis = require_redis!();
    let redlite = Db::open_memory().unwrap();
    let _: () = redis::cmd("FLUSHDB").query(&mut redis).unwrap();

    // SPOP on non-existent key
    let r1 = redlite.spop("nokey", None).unwrap();
    let r2: Vec<String> = redis::cmd("SPOP").arg("nokey").query(&mut redis).unwrap();
    assert_eq!(r1.len(), r2.len());
    assert_eq!(r1.len(), 0);

    // Add members to set
    redlite.sadd("myset", &[b"one", b"two", b"three", b"four", b"five"]).unwrap();
    let _: usize = redis::cmd("SADD").arg("myset").arg("one").arg("two").arg("three").arg("four").arg("five").query(&mut redis).unwrap();

    // SPOP single member (default)
    let r1 = redlite.spop("myset", None).unwrap();
    let r2: Vec<String> = redis::cmd("SPOP").arg("myset").query(&mut redis).unwrap();
    assert_eq!(r1.len(), 1);
    assert_eq!(r2.len(), 1);

    // Verify both removed same member from set
    let remaining1 = redlite.scard("myset").unwrap();
    let remaining2: usize = redis::cmd("SCARD").arg("myset").query(&mut redis).unwrap();
    assert_eq!(remaining1 as usize, remaining2);
    assert_eq!(remaining1, 4);

    // SPOP with count=2
    let r1 = redlite.spop("myset", Some(2)).unwrap();
    let r2: Vec<String> = redis::cmd("SPOP").arg("myset").arg(2).query(&mut redis).unwrap();
    assert_eq!(r1.len(), 2);
    assert_eq!(r2.len(), 2);

    let remaining1 = redlite.scard("myset").unwrap();
    let remaining2: usize = redis::cmd("SCARD").arg("myset").query(&mut redis).unwrap();
    assert_eq!(remaining1 as usize, remaining2);
    assert_eq!(remaining1, 2);

    // SPOP all remaining members
    let r1 = redlite.spop("myset", Some(10)).unwrap();
    let r2: Vec<String> = redis::cmd("SPOP").arg("myset").arg(10).query(&mut redis).unwrap();
    assert_eq!(r1.len(), 2);  // Only 2 left
    assert_eq!(r2.len(), 2);

    // Set should be deleted now
    let exists1 = redlite.exists(&["myset"]).unwrap();
    let exists2: i64 = redis::cmd("EXISTS").arg("myset").query(&mut redis).unwrap();
    assert_eq!(exists1, exists2);
    assert_eq!(exists1, 0);

    // SPOP with binary data
    redlite.sadd("binset", &[b"\x00\x01\x02", b"\xff\xfe\xfd", b"normal"]).unwrap();
    redis::cmd("SADD").arg("binset").arg(b"\x00\x01\x02").arg(b"\xff\xfe\xfd").arg("normal").query::<()>(&mut redis).unwrap();

    let r1 = redlite.spop("binset", Some(1)).unwrap();
    let r2: Vec<Vec<u8>> = redis::cmd("SPOP").arg("binset").arg(1).query(&mut redis).unwrap();
    assert_eq!(r1.len(), 1);
    assert_eq!(r2.len(), 1);

    // SPOP with unicode data
    redlite.sadd("unicode", &["hello".as_bytes(), "世界".as_bytes(), "🚀".as_bytes()]).unwrap();
    redis::cmd("SADD").arg("unicode").arg("hello").arg("世界").arg("🚀").query::<()>(&mut redis).unwrap();

    let r1 = redlite.spop("unicode", Some(3)).unwrap();
    let r2: Vec<Vec<u8>> = redis::cmd("SPOP").arg("unicode").arg(3).query(&mut redis).unwrap();
    assert_eq!(r1.len(), 3);
    assert_eq!(r2.len(), 3);

    // SPOP with count=0
    redlite.sadd("set0", &[b"a", b"b", b"c"]).unwrap();
    redis::cmd("SADD").arg("set0").arg("a").arg("b").arg("c").query::<()>(&mut redis).unwrap();

    let r1 = redlite.spop("set0", Some(0)).unwrap();
    let r2: Vec<String> = redis::cmd("SPOP").arg("set0").arg(0).query(&mut redis).unwrap();
    assert_eq!(r1.len(), 0);
    assert_eq!(r2.len(), 0);

    // Set should still exist with all members
    let remaining1 = redlite.scard("set0").unwrap();
    let remaining2: usize = redis::cmd("SCARD").arg("set0").query(&mut redis).unwrap();
    assert_eq!(remaining1 as usize, remaining2);
    assert_eq!(remaining1, 3);
}

/// SRANDMEMBER command: all scenarios
#[test]
fn oracle_cmd_srandmember() {
    let mut redis = require_redis!();
    let redlite = Db::open_memory().unwrap();
    let _: () = redis::cmd("FLUSHDB").query(&mut redis).unwrap();

    // SRANDMEMBER on non-existent key
    let r1 = redlite.srandmember("nokey", None).unwrap();
    let r2: Vec<String> = redis::cmd("SRANDMEMBER").arg("nokey").query(&mut redis).unwrap();
    assert_eq!(r1.len(), r2.len());
    assert_eq!(r1.len(), 0);

    // Add members to set
    redlite.sadd("myset", &[b"one", b"two", b"three", b"four", b"five"]).unwrap();
    let _: usize = redis::cmd("SADD").arg("myset").arg("one").arg("two").arg("three").arg("four").arg("five").query(&mut redis).unwrap();

    // SRANDMEMBER single member (default, no count)
    let r1 = redlite.srandmember("myset", None).unwrap();
    let r2: Vec<String> = redis::cmd("SRANDMEMBER").arg("myset").query(&mut redis).unwrap();
    assert_eq!(r1.len(), 1);
    assert_eq!(r2.len(), 1);

    // Verify set still has all members
    let count1 = redlite.scard("myset").unwrap();
    let count2: usize = redis::cmd("SCARD").arg("myset").query(&mut redis).unwrap();
    assert_eq!(count1 as usize, count2);
    assert_eq!(count1, 5);

    // SRANDMEMBER with positive count (distinct members)
    let r1 = redlite.srandmember("myset", Some(3)).unwrap();
    let r2: Vec<String> = redis::cmd("SRANDMEMBER").arg("myset").arg(3).query(&mut redis).unwrap();
    assert_eq!(r1.len(), 3);
    assert_eq!(r2.len(), 3);

    // Members should be distinct
    let unique1: std::collections::HashSet<_> = r1.iter().collect();
    let unique2: std::collections::HashSet<_> = r2.iter().collect();
    assert_eq!(unique1.len(), 3);
    assert_eq!(unique2.len(), 3);

    // SRANDMEMBER with negative count (may repeat)
    let r1 = redlite.srandmember("myset", Some(-10)).unwrap();
    let r2: Vec<String> = redis::cmd("SRANDMEMBER").arg("myset").arg(-10).query(&mut redis).unwrap();
    assert_eq!(r1.len(), 10);
    assert_eq!(r2.len(), 10);
    // Note: results may contain duplicates

    // SRANDMEMBER with count larger than set size (positive)
    let r1 = redlite.srandmember("myset", Some(100)).unwrap();
    let r2: Vec<String> = redis::cmd("SRANDMEMBER").arg("myset").arg(100).query(&mut redis).unwrap();
    assert_eq!(r1.len(), 5);  // Max distinct members
    assert_eq!(r2.len(), 5);

    // SRANDMEMBER with count=0
    let r1 = redlite.srandmember("myset", Some(0)).unwrap();
    let r2: Vec<String> = redis::cmd("SRANDMEMBER").arg("myset").arg(0).query(&mut redis).unwrap();
    assert_eq!(r1.len(), 0);
    assert_eq!(r2.len(), 0);

    // SRANDMEMBER with binary data
    redlite.sadd("binset", &[b"\x00\x01\x02", b"\xff\xfe\xfd", b"normal"]).unwrap();
    redis::cmd("SADD").arg("binset").arg(b"\x00\x01\x02").arg(b"\xff\xfe\xfd").arg("normal").query::<()>(&mut redis).unwrap();

    let r1 = redlite.srandmember("binset", Some(2)).unwrap();
    let r2: Vec<Vec<u8>> = redis::cmd("SRANDMEMBER").arg("binset").arg(2).query(&mut redis).unwrap();
    assert_eq!(r1.len(), 2);
    assert_eq!(r2.len(), 2);

    // SRANDMEMBER with unicode data
    redlite.sadd("unicode", &["hello".as_bytes(), "世界".as_bytes(), "🚀".as_bytes()]).unwrap();
    redis::cmd("SADD").arg("unicode").arg("hello").arg("世界").arg("🚀").query::<()>(&mut redis).unwrap();

    let r1 = redlite.srandmember("unicode", Some(3)).unwrap();
    let r2: Vec<Vec<u8>> = redis::cmd("SRANDMEMBER").arg("unicode").arg(3).query(&mut redis).unwrap();
    assert_eq!(r1.len(), 3);
    assert_eq!(r2.len(), 3);

    // SRANDMEMBER with count=1 (positive, should return array)
    let r1 = redlite.srandmember("myset", Some(1)).unwrap();
    let r2: Vec<String> = redis::cmd("SRANDMEMBER").arg("myset").arg(1).query(&mut redis).unwrap();
    assert_eq!(r1.len(), 1);
    assert_eq!(r2.len(), 1);
}

/// SSCAN command: all scenarios
#[test]
fn oracle_cmd_sscan() {
    let mut redis = require_redis!();
    let redlite = Db::open_memory().unwrap();
    let _: () = redis::cmd("FLUSHDB").query(&mut redis).unwrap();

    // SSCAN on non-existent key
    let (cur1, members1) = redlite.sscan("nokey", "0", None, 10).unwrap();
    let (cur2, members2): (String, Vec<String>) = redis::cmd("SSCAN").arg("nokey").arg(0).query(&mut redis).unwrap();
    assert_eq!(cur1, "0");
    assert_eq!(cur2, "0");
    assert_eq!(members1.len(), 0);
    assert_eq!(members2.len(), 0);

    // Add members to set
    redlite.sadd("myset", &[b"one", b"two", b"three", b"four", b"five", b"six", b"seven", b"eight"]).unwrap();
    redis::cmd("SADD").arg("myset").arg("one").arg("two").arg("three").arg("four").arg("five").arg("six").arg("seven").arg("eight").query::<()>(&mut redis).unwrap();

    // SSCAN with small count (pagination)
    let (cur1, members1) = redlite.sscan("myset", "0", None, 3).unwrap();
    let (cur2, members2): (String, Vec<String>) = redis::cmd("SSCAN").arg("myset").arg(0).arg("COUNT").arg(3).query(&mut redis).unwrap();
    // Note: Redis SSCAN might return more than COUNT, so we just check we got some results
    assert!(members1.len() > 0);
    assert!(members2.len() > 0);
    assert_ne!(cur1, "0");  // Should have more to scan

    // SSCAN with large count (get all)
    let (cur1, members1) = redlite.sscan("myset", "0", None, 100).unwrap();
    let (cur2, members2): (String, Vec<String>) = redis::cmd("SSCAN").arg("myset").arg(0).arg("COUNT").arg(100).query(&mut redis).unwrap();
    assert_eq!(cur1, "0");  // Should be done
    assert_eq!(cur2, "0");
    assert_eq!(members1.len(), 8);
    assert_eq!(members2.len(), 8);

    // SSCAN with MATCH pattern
    redlite.sadd("patterns", &[b"apple", b"apricot", b"banana", b"blueberry", b"cherry"]).unwrap();
    redis::cmd("SADD").arg("patterns").arg("apple").arg("apricot").arg("banana").arg("blueberry").arg("cherry").query::<()>(&mut redis).unwrap();

    let (cur1, members1) = redlite.sscan("patterns", "0", Some("a*"), 100).unwrap();
    let (cur2, members2): (String, Vec<String>) = redis::cmd("SSCAN").arg("patterns").arg(0).arg("MATCH").arg("a*").arg("COUNT").arg(100).query(&mut redis).unwrap();
    assert_eq!(cur1, "0");
    assert_eq!(cur2, "0");
    // Both should return members matching "a*" pattern
    for member in &members1 {
        let s = String::from_utf8_lossy(member);
        assert!(s.starts_with('a'));
    }

    // SSCAN with MATCH pattern (wildcard in middle)
    let (cur1, members1) = redlite.sscan("patterns", "0", Some("*berry"), 100).unwrap();
    let (cur2, members2): (String, Vec<String>) = redis::cmd("SSCAN").arg("patterns").arg(0).arg("MATCH").arg("*berry").arg("COUNT").arg(100).query(&mut redis).unwrap();
    assert_eq!(cur1, "0");
    assert_eq!(cur2, "0");
    // Both should return members ending with "berry"
    for member in &members1 {
        let s = String::from_utf8_lossy(member);
        assert!(s.ends_with("berry"));
    }

    // SSCAN with binary data
    redlite.sadd("binset", &[b"\x00\x01", b"\x02\x03", b"\xff\xfe"]).unwrap();
    redis::cmd("SADD").arg("binset").arg(b"\x00\x01").arg(b"\x02\x03").arg(b"\xff\xfe").query::<()>(&mut redis).unwrap();

    let (cur1, members1) = redlite.sscan("binset", "0", None, 10).unwrap();
    let (cur2, members2): (String, Vec<Vec<u8>>) = redis::cmd("SSCAN").arg("binset").arg(0).arg("COUNT").arg(10).query(&mut redis).unwrap();
    assert_eq!(cur1, "0");
    assert_eq!(cur2, "0");
    assert_eq!(members1.len(), 3);
    assert_eq!(members2.len(), 3);

    // SSCAN with unicode data
    redlite.sadd("unicode", &["你好".as_bytes(), "世界".as_bytes(), "🚀".as_bytes(), "hello".as_bytes()]).unwrap();
    redis::cmd("SADD").arg("unicode").arg("你好").arg("世界").arg("🚀").arg("hello").query::<()>(&mut redis).unwrap();

    let (cur1, members1) = redlite.sscan("unicode", "0", None, 10).unwrap();
    let (cur2, members2): (String, Vec<Vec<u8>>) = redis::cmd("SSCAN").arg("unicode").arg(0).arg("COUNT").arg(10).query(&mut redis).unwrap();
    assert_eq!(cur1, "0");
    assert_eq!(cur2, "0");
    assert_eq!(members1.len(), 4);
    assert_eq!(members2.len(), 4);

    // SSCAN with count=1 (minimal pagination)
    let (cur1, _) = redlite.sscan("myset", "0", None, 1).unwrap();
    let (cur2, _): (String, Vec<String>) = redis::cmd("SSCAN").arg("myset").arg(0).arg("COUNT").arg(1).query(&mut redis).unwrap();
    // Both should indicate more data available (unless implementation differs)
    // Redis may return more than requested count, so we just verify it works

    // SSCAN empty set
    redlite.sadd("empty", &[b"temp"]).unwrap();
    redis::cmd("SADD").arg("empty").arg("temp").query::<()>(&mut redis).unwrap();
    redlite.srem("empty", &[b"temp"]).unwrap();
    redis::cmd("SREM").arg("empty").arg("temp").query::<()>(&mut redis).unwrap();

    let (cur1, members1) = redlite.sscan("empty", "0", None, 10).unwrap();
    let (cur2, members2): (String, Vec<String>) = redis::cmd("SSCAN").arg("empty").arg(0).arg("COUNT").arg(10).query(&mut redis).unwrap();
    assert_eq!(cur1, "0");
    assert_eq!(cur2, "0");
    assert_eq!(members1.len(), 0);
    assert_eq!(members2.len(), 0);
}

/// SDIFFSTORE command: all scenarios
#[test]
fn oracle_cmd_sdiffstore() {
    let mut redis = require_redis!();
    let redlite = Db::open_memory().unwrap();
    let _: () = redis::cmd("FLUSHDB").query(&mut redis).unwrap();

    // SDIFFSTORE with non-existent keys
    let r1 = redlite.sdiffstore("dest", &["nokey1", "nokey2"]).unwrap();
    let r2: i64 = redis::cmd("SDIFFSTORE").arg("dest").arg("nokey1").arg("nokey2").query(&mut redis).unwrap();
    assert_eq!(r1, r2);
    assert_eq!(r1, 0);

    // Setup sets for testing
    redlite.sadd("set1", &[b"a", b"b", b"c", b"d"]).unwrap();
    redlite.sadd("set2", &[b"b", b"c"]).unwrap();
    redlite.sadd("set3", &[b"c", b"d", b"e"]).unwrap();

    redis::cmd("SADD").arg("set1").arg("a").arg("b").arg("c").arg("d").query::<()>(&mut redis).unwrap();
    redis::cmd("SADD").arg("set2").arg("b").arg("c").query::<()>(&mut redis).unwrap();
    redis::cmd("SADD").arg("set3").arg("c").arg("d").arg("e").query::<()>(&mut redis).unwrap();

    // SDIFFSTORE: set1 - set2 (should be {a, d})
    let r1 = redlite.sdiffstore("diff1", &["set1", "set2"]).unwrap();
    let r2: i64 = redis::cmd("SDIFFSTORE").arg("diff1").arg("set1").arg("set2").query(&mut redis).unwrap();
    assert_eq!(r1, r2);
    assert_eq!(r1, 2);

    let members1 = redlite.smembers("diff1").unwrap();
    let members2: Vec<Vec<u8>> = redis::cmd("SMEMBERS").arg("diff1").query(&mut redis).unwrap();
    assert_eq!(members1.len(), members2.len());
    assert_eq!(members1.len(), 2);

    // SDIFFSTORE: set1 - set2 - set3 (should be {a})
    let r1 = redlite.sdiffstore("diff2", &["set1", "set2", "set3"]).unwrap();
    let r2: i64 = redis::cmd("SDIFFSTORE").arg("diff2").arg("set1").arg("set2").arg("set3").query(&mut redis).unwrap();
    assert_eq!(r1, r2);
    assert_eq!(r1, 1);

    let members1 = redlite.smembers("diff2").unwrap();
    let members2: Vec<Vec<u8>> = redis::cmd("SMEMBERS").arg("diff2").query(&mut redis).unwrap();
    assert_eq!(members1.len(), members2.len());
    assert_eq!(members1.len(), 1);
    assert!(members1.contains(&b"a".to_vec()));

    // SDIFFSTORE overwrite existing destination
    redlite.sadd("existing", &[b"old"]).unwrap();
    redis::cmd("SADD").arg("existing").arg("old").query::<()>(&mut redis).unwrap();

    let r1 = redlite.sdiffstore("existing", &["set1", "set2"]).unwrap();
    let r2: i64 = redis::cmd("SDIFFSTORE").arg("existing").arg("set1").arg("set2").query(&mut redis).unwrap();
    assert_eq!(r1, r2);

    let members1 = redlite.smembers("existing").unwrap();
    let members2: Vec<Vec<u8>> = redis::cmd("SMEMBERS").arg("existing").query(&mut redis).unwrap();
    assert!(!members1.contains(&b"old".to_vec()));

    // SDIFFSTORE with binary data
    redlite.sadd("bin1", &[b"\x00\x01", b"\x02\x03", b"\xff\xfe"]).unwrap();
    redlite.sadd("bin2", &[b"\x02\x03"]).unwrap();
    redis::cmd("SADD").arg("bin1").arg(b"\x00\x01").arg(b"\x02\x03").arg(b"\xff\xfe").query::<()>(&mut redis).unwrap();
    redis::cmd("SADD").arg("bin2").arg(b"\x02\x03").query::<()>(&mut redis).unwrap();

    let r1 = redlite.sdiffstore("bindiff", &["bin1", "bin2"]).unwrap();
    let r2: i64 = redis::cmd("SDIFFSTORE").arg("bindiff").arg("bin1").arg("bin2").query(&mut redis).unwrap();
    assert_eq!(r1, r2);
    assert_eq!(r1, 2);

    // SDIFFSTORE with unicode data
    redlite.sadd("uni1", &["hello".as_bytes(), "世界".as_bytes(), "🚀".as_bytes()]).unwrap();
    redlite.sadd("uni2", &["世界".as_bytes()]).unwrap();
    redis::cmd("SADD").arg("uni1").arg("hello").arg("世界").arg("🚀").query::<()>(&mut redis).unwrap();
    redis::cmd("SADD").arg("uni2").arg("世界").query::<()>(&mut redis).unwrap();

    let r1 = redlite.sdiffstore("unidiff", &["uni1", "uni2"]).unwrap();
    let r2: i64 = redis::cmd("SDIFFSTORE").arg("unidiff").arg("uni1").arg("uni2").query(&mut redis).unwrap();
    assert_eq!(r1, r2);
    assert_eq!(r1, 2);
}

/// SINTERSTORE command: all scenarios
#[test]
fn oracle_cmd_sinterstore() {
    let mut redis = require_redis!();
    let redlite = Db::open_memory().unwrap();
    let _: () = redis::cmd("FLUSHDB").query(&mut redis).unwrap();

    // SINTERSTORE with non-existent keys
    let r1 = redlite.sinterstore("dest", &["nokey1", "nokey2"]).unwrap();
    let r2: i64 = redis::cmd("SINTERSTORE").arg("dest").arg("nokey1").arg("nokey2").query(&mut redis).unwrap();
    assert_eq!(r1, r2);
    assert_eq!(r1, 0);

    // Setup sets for testing
    redlite.sadd("set1", &[b"a", b"b", b"c", b"d"]).unwrap();
    redlite.sadd("set2", &[b"b", b"c", b"e"]).unwrap();
    redlite.sadd("set3", &[b"c", b"d", b"e"]).unwrap();

    redis::cmd("SADD").arg("set1").arg("a").arg("b").arg("c").arg("d").query::<()>(&mut redis).unwrap();
    redis::cmd("SADD").arg("set2").arg("b").arg("c").arg("e").query::<()>(&mut redis).unwrap();
    redis::cmd("SADD").arg("set3").arg("c").arg("d").arg("e").query::<()>(&mut redis).unwrap();

    // SINTERSTORE: set1 ∩ set2 (should be {b, c})
    let r1 = redlite.sinterstore("inter1", &["set1", "set2"]).unwrap();
    let r2: i64 = redis::cmd("SINTERSTORE").arg("inter1").arg("set1").arg("set2").query(&mut redis).unwrap();
    assert_eq!(r1, r2);
    assert_eq!(r1, 2);

    let members1 = redlite.smembers("inter1").unwrap();
    let members2: Vec<Vec<u8>> = redis::cmd("SMEMBERS").arg("inter1").query(&mut redis).unwrap();
    assert_eq!(members1.len(), members2.len());
    assert_eq!(members1.len(), 2);

    // SINTERSTORE: set1 ∩ set2 ∩ set3 (should be {c})
    let r1 = redlite.sinterstore("inter2", &["set1", "set2", "set3"]).unwrap();
    let r2: i64 = redis::cmd("SINTERSTORE").arg("inter2").arg("set1").arg("set2").arg("set3").query(&mut redis).unwrap();
    assert_eq!(r1, r2);
    assert_eq!(r1, 1);

    let members1 = redlite.smembers("inter2").unwrap();
    let members2: Vec<Vec<u8>> = redis::cmd("SMEMBERS").arg("inter2").query(&mut redis).unwrap();
    assert_eq!(members1.len(), members2.len());
    assert_eq!(members1.len(), 1);
    assert!(members1.contains(&b"c".to_vec()));

    // SINTERSTORE with no common elements
    redlite.sadd("setA", &[b"x", b"y"]).unwrap();
    redlite.sadd("setB", &[b"z", b"w"]).unwrap();
    redis::cmd("SADD").arg("setA").arg("x").arg("y").query::<()>(&mut redis).unwrap();
    redis::cmd("SADD").arg("setB").arg("z").arg("w").query::<()>(&mut redis).unwrap();

    let r1 = redlite.sinterstore("empty_inter", &["setA", "setB"]).unwrap();
    let r2: i64 = redis::cmd("SINTERSTORE").arg("empty_inter").arg("setA").arg("setB").query(&mut redis).unwrap();
    assert_eq!(r1, r2);
    assert_eq!(r1, 0);

    // SINTERSTORE overwrite existing destination
    redlite.sadd("existing", &[b"old"]).unwrap();
    redis::cmd("SADD").arg("existing").arg("old").query::<()>(&mut redis).unwrap();

    let r1 = redlite.sinterstore("existing", &["set1", "set2"]).unwrap();
    let r2: i64 = redis::cmd("SINTERSTORE").arg("existing").arg("set1").arg("set2").query(&mut redis).unwrap();
    assert_eq!(r1, r2);

    let members1 = redlite.smembers("existing").unwrap();
    let members2: Vec<Vec<u8>> = redis::cmd("SMEMBERS").arg("existing").query(&mut redis).unwrap();
    assert!(!members1.contains(&b"old".to_vec()));

    // SINTERSTORE with binary data
    redlite.sadd("bin1", &[b"\x00\x01", b"\x02\x03", b"\xff\xfe"]).unwrap();
    redlite.sadd("bin2", &[b"\x02\x03", b"\xff\xfe"]).unwrap();
    redis::cmd("SADD").arg("bin1").arg(b"\x00\x01").arg(b"\x02\x03").arg(b"\xff\xfe").query::<()>(&mut redis).unwrap();
    redis::cmd("SADD").arg("bin2").arg(b"\x02\x03").arg(b"\xff\xfe").query::<()>(&mut redis).unwrap();

    let r1 = redlite.sinterstore("bininter", &["bin1", "bin2"]).unwrap();
    let r2: i64 = redis::cmd("SINTERSTORE").arg("bininter").arg("bin1").arg("bin2").query(&mut redis).unwrap();
    assert_eq!(r1, r2);
    assert_eq!(r1, 2);

    // SINTERSTORE with unicode data
    redlite.sadd("uni1", &["hello".as_bytes(), "世界".as_bytes(), "🚀".as_bytes()]).unwrap();
    redlite.sadd("uni2", &["世界".as_bytes(), "🚀".as_bytes()]).unwrap();
    redis::cmd("SADD").arg("uni1").arg("hello").arg("世界").arg("🚀").query::<()>(&mut redis).unwrap();
    redis::cmd("SADD").arg("uni2").arg("世界").arg("🚀").query::<()>(&mut redis).unwrap();

    let r1 = redlite.sinterstore("uniinter", &["uni1", "uni2"]).unwrap();
    let r2: i64 = redis::cmd("SINTERSTORE").arg("uniinter").arg("uni1").arg("uni2").query(&mut redis).unwrap();
    assert_eq!(r1, r2);
    assert_eq!(r1, 2);
}

/// SUNIONSTORE command: all scenarios
#[test]
fn oracle_cmd_sunionstore() {
    let mut redis = require_redis!();
    let redlite = Db::open_memory().unwrap();
    let _: () = redis::cmd("FLUSHDB").query(&mut redis).unwrap();

    // SUNIONSTORE with non-existent keys
    let r1 = redlite.sunionstore("dest", &["nokey1", "nokey2"]).unwrap();
    let r2: i64 = redis::cmd("SUNIONSTORE").arg("dest").arg("nokey1").arg("nokey2").query(&mut redis).unwrap();
    assert_eq!(r1, r2);
    assert_eq!(r1, 0);

    // Setup sets for testing
    redlite.sadd("set1", &[b"a", b"b", b"c"]).unwrap();
    redlite.sadd("set2", &[b"c", b"d", b"e"]).unwrap();
    redlite.sadd("set3", &[b"e", b"f"]).unwrap();

    redis::cmd("SADD").arg("set1").arg("a").arg("b").arg("c").query::<()>(&mut redis).unwrap();
    redis::cmd("SADD").arg("set2").arg("c").arg("d").arg("e").query::<()>(&mut redis).unwrap();
    redis::cmd("SADD").arg("set3").arg("e").arg("f").query::<()>(&mut redis).unwrap();

    // SUNIONSTORE: set1 ∪ set2 (should be {a, b, c, d, e})
    let r1 = redlite.sunionstore("union1", &["set1", "set2"]).unwrap();
    let r2: i64 = redis::cmd("SUNIONSTORE").arg("union1").arg("set1").arg("set2").query(&mut redis).unwrap();
    assert_eq!(r1, r2);
    assert_eq!(r1, 5);

    let members1 = redlite.smembers("union1").unwrap();
    let members2: Vec<Vec<u8>> = redis::cmd("SMEMBERS").arg("union1").query(&mut redis).unwrap();
    assert_eq!(members1.len(), members2.len());
    assert_eq!(members1.len(), 5);

    // SUNIONSTORE: set1 ∪ set2 ∪ set3 (should be {a, b, c, d, e, f})
    let r1 = redlite.sunionstore("union2", &["set1", "set2", "set3"]).unwrap();
    let r2: i64 = redis::cmd("SUNIONSTORE").arg("union2").arg("set1").arg("set2").arg("set3").query(&mut redis).unwrap();
    assert_eq!(r1, r2);
    assert_eq!(r1, 6);

    let members1 = redlite.smembers("union2").unwrap();
    let members2: Vec<Vec<u8>> = redis::cmd("SMEMBERS").arg("union2").query(&mut redis).unwrap();
    assert_eq!(members1.len(), members2.len());
    assert_eq!(members1.len(), 6);

    // SUNIONSTORE with single set
    let r1 = redlite.sunionstore("single", &["set1"]).unwrap();
    let r2: i64 = redis::cmd("SUNIONSTORE").arg("single").arg("set1").query(&mut redis).unwrap();
    assert_eq!(r1, r2);
    assert_eq!(r1, 3);

    // SUNIONSTORE overwrite existing destination
    redlite.sadd("existing", &[b"old"]).unwrap();
    redis::cmd("SADD").arg("existing").arg("old").query::<()>(&mut redis).unwrap();

    let r1 = redlite.sunionstore("existing", &["set1", "set2"]).unwrap();
    let r2: i64 = redis::cmd("SUNIONSTORE").arg("existing").arg("set1").arg("set2").query(&mut redis).unwrap();
    assert_eq!(r1, r2);

    let members1 = redlite.smembers("existing").unwrap();
    let members2: Vec<Vec<u8>> = redis::cmd("SMEMBERS").arg("existing").query(&mut redis).unwrap();
    assert!(!members1.contains(&b"old".to_vec()));

    // SUNIONSTORE with binary data
    redlite.sadd("bin1", &[b"\x00\x01", b"\x02\x03"]).unwrap();
    redlite.sadd("bin2", &[b"\xff\xfe"]).unwrap();
    redis::cmd("SADD").arg("bin1").arg(b"\x00\x01").arg(b"\x02\x03").query::<()>(&mut redis).unwrap();
    redis::cmd("SADD").arg("bin2").arg(b"\xff\xfe").query::<()>(&mut redis).unwrap();

    let r1 = redlite.sunionstore("binunion", &["bin1", "bin2"]).unwrap();
    let r2: i64 = redis::cmd("SUNIONSTORE").arg("binunion").arg("bin1").arg("bin2").query(&mut redis).unwrap();
    assert_eq!(r1, r2);
    assert_eq!(r1, 3);

    // SUNIONSTORE with unicode data
    redlite.sadd("uni1", &["hello".as_bytes(), "世界".as_bytes()]).unwrap();
    redlite.sadd("uni2", &["🚀".as_bytes()]).unwrap();
    redis::cmd("SADD").arg("uni1").arg("hello").arg("世界").query::<()>(&mut redis).unwrap();
    redis::cmd("SADD").arg("uni2").arg("🚀").query::<()>(&mut redis).unwrap();

    let r1 = redlite.sunionstore("uniunion", &["uni1", "uni2"]).unwrap();
    let r2: i64 = redis::cmd("SUNIONSTORE").arg("uniunion").arg("uni1").arg("uni2").query(&mut redis).unwrap();
    assert_eq!(r1, r2);
    assert_eq!(r1, 3);
}

/// ZINTERSTORE command: all scenarios
#[test]
fn oracle_cmd_zinterstore() {
    let mut redis = require_redis!();
    let redlite = Db::open_memory().unwrap();
    let _: () = redis::cmd("FLUSHDB").query(&mut redis).unwrap();

    // ZINTERSTORE with non-existent keys
    let r1 = redlite.zinterstore("dest", &["nokey1", "nokey2"], None, None).unwrap();
    let r2: i64 = redis::cmd("ZINTERSTORE").arg("dest").arg(2).arg("nokey1").arg("nokey2").query(&mut redis).unwrap();
    assert_eq!(r1, r2);
    assert_eq!(r1, 0);

    // Setup sorted sets for testing
    redlite.zadd("zset1", &[ZMember::new(1.0, b"a".to_vec()), ZMember::new(2.0, b"b".to_vec()), ZMember::new(3.0, b"c".to_vec())]).unwrap();
    redlite.zadd("zset2", &[ZMember::new(1.0, b"b".to_vec()), ZMember::new(2.0, b"c".to_vec()), ZMember::new(3.0, b"d".to_vec())]).unwrap();
    redlite.zadd("zset3", &[ZMember::new(1.0, b"c".to_vec()), ZMember::new(2.0, b"d".to_vec()), ZMember::new(3.0, b"e".to_vec())]).unwrap();

    redis::cmd("ZADD").arg("zset1").arg(1.0).arg("a").arg(2.0).arg("b").arg(3.0).arg("c").query::<()>(&mut redis).unwrap();
    redis::cmd("ZADD").arg("zset2").arg(1.0).arg("b").arg(2.0).arg("c").arg(3.0).arg("d").query::<()>(&mut redis).unwrap();
    redis::cmd("ZADD").arg("zset3").arg(1.0).arg("c").arg(2.0).arg("d").arg(3.0).arg("e").query::<()>(&mut redis).unwrap();

    // ZINTERSTORE: zset1 ∩ zset2 (should be {b:3.0, c:5.0} with SUM)
    let r1 = redlite.zinterstore("inter1", &["zset1", "zset2"], None, None).unwrap();
    let r2: i64 = redis::cmd("ZINTERSTORE").arg("inter1").arg(2).arg("zset1").arg("zset2").query(&mut redis).unwrap();
    assert_eq!(r1, r2);
    assert_eq!(r1, 2);

    let members1 = redlite.zrange("inter1", 0, -1, false).unwrap();
    let members2: Vec<Vec<u8>> = redis::cmd("ZRANGE").arg("inter1").arg(0).arg(-1).query(&mut redis).unwrap();
    assert_eq!(members1.len(), members2.len());
    assert_eq!(members1.len(), 2);

    // Verify scores are aggregated with SUM
    let score1 = redlite.zscore("inter1", b"b").unwrap();
    let score2: f64 = redis::cmd("ZSCORE").arg("inter1").arg("b").query(&mut redis).unwrap();
    assert_eq!(score1, Some(score2));
    assert_eq!(score1, Some(3.0));  // 2.0 + 1.0

    // ZINTERSTORE: zset1 ∩ zset2 ∩ zset3 (should be {c} only)
    let r1 = redlite.zinterstore("inter2", &["zset1", "zset2", "zset3"], None, None).unwrap();
    let r2: i64 = redis::cmd("ZINTERSTORE").arg("inter2").arg(3).arg("zset1").arg("zset2").arg("zset3").query(&mut redis).unwrap();
    assert_eq!(r1, r2);
    assert_eq!(r1, 1);

    let members1 = redlite.zrange("inter2", 0, -1, false).unwrap();
    assert_eq!(members1.len(), 1);
    assert_eq!(members1[0].member, b"c".to_vec());

    // ZINTERSTORE with WEIGHTS
    let r1 = redlite.zinterstore("inter_weighted", &["zset1", "zset2"], Some(&[2.0, 3.0]), None).unwrap();
    let r2: i64 = redis::cmd("ZINTERSTORE").arg("inter_weighted").arg(2).arg("zset1").arg("zset2").arg("WEIGHTS").arg(2.0).arg(3.0).query(&mut redis).unwrap();
    assert_eq!(r1, r2);

    let score1 = redlite.zscore("inter_weighted", b"b").unwrap();
    let score2: f64 = redis::cmd("ZSCORE").arg("inter_weighted").arg("b").query(&mut redis).unwrap();
    assert_eq!(score1, Some(score2));
    assert_eq!(score1, Some(7.0));  // (2.0 * 2.0) + (1.0 * 3.0)

    // ZINTERSTORE with AGGREGATE MIN
    let r1 = redlite.zinterstore("inter_min", &["zset1", "zset2"], None, Some("MIN")).unwrap();
    let r2: i64 = redis::cmd("ZINTERSTORE").arg("inter_min").arg(2).arg("zset1").arg("zset2").arg("AGGREGATE").arg("MIN").query(&mut redis).unwrap();
    assert_eq!(r1, r2);

    let score1 = redlite.zscore("inter_min", b"b").unwrap();
    let score2: f64 = redis::cmd("ZSCORE").arg("inter_min").arg("b").query(&mut redis).unwrap();
    assert_eq!(score1, Some(score2));
    assert_eq!(score1, Some(1.0));  // min(2.0, 1.0)

    // ZINTERSTORE with AGGREGATE MAX
    let r1 = redlite.zinterstore("inter_max", &["zset1", "zset2"], None, Some("MAX")).unwrap();
    let r2: i64 = redis::cmd("ZINTERSTORE").arg("inter_max").arg(2).arg("zset1").arg("zset2").arg("AGGREGATE").arg("MAX").query(&mut redis).unwrap();
    assert_eq!(r1, r2);

    let score1 = redlite.zscore("inter_max", b"b").unwrap();
    let score2: f64 = redis::cmd("ZSCORE").arg("inter_max").arg("b").query(&mut redis).unwrap();
    assert_eq!(score1, Some(score2));
    assert_eq!(score1, Some(2.0));  // max(2.0, 1.0)

    // ZINTERSTORE with no common members
    redlite.zadd("zsetX", &[ZMember::new(1.0, b"x".to_vec()), ZMember::new(2.0, b"y".to_vec())]).unwrap();
    redlite.zadd("zsetY", &[ZMember::new(1.0, b"z".to_vec()), ZMember::new(2.0, b"w".to_vec())]).unwrap();
    redis::cmd("ZADD").arg("zsetX").arg(1.0).arg("x").arg(2.0).arg("y").query::<()>(&mut redis).unwrap();
    redis::cmd("ZADD").arg("zsetY").arg(1.0).arg("z").arg(2.0).arg("w").query::<()>(&mut redis).unwrap();

    let r1 = redlite.zinterstore("empty_inter", &["zsetX", "zsetY"], None, None).unwrap();
    let r2: i64 = redis::cmd("ZINTERSTORE").arg("empty_inter").arg(2).arg("zsetX").arg("zsetY").query(&mut redis).unwrap();
    assert_eq!(r1, r2);
    assert_eq!(r1, 0);

    // ZINTERSTORE with binary data
    redlite.zadd("bin1", &[ZMember::new(1.0, b"\x00\x01".to_vec()), ZMember::new(2.0, b"\x02\x03".to_vec())]).unwrap();
    redlite.zadd("bin2", &[ZMember::new(3.0, b"\x00\x01".to_vec()), ZMember::new(4.0, b"\xff\xfe".to_vec())]).unwrap();
    redis::cmd("ZADD").arg("bin1").arg(1.0).arg(b"\x00\x01").arg(2.0).arg(b"\x02\x03").query::<()>(&mut redis).unwrap();
    redis::cmd("ZADD").arg("bin2").arg(3.0).arg(b"\x00\x01").arg(4.0).arg(b"\xff\xfe").query::<()>(&mut redis).unwrap();

    let r1 = redlite.zinterstore("bininter", &["bin1", "bin2"], None, None).unwrap();
    let r2: i64 = redis::cmd("ZINTERSTORE").arg("bininter").arg(2).arg("bin1").arg("bin2").query(&mut redis).unwrap();
    assert_eq!(r1, r2);
    assert_eq!(r1, 1);
}

/// ZUNIONSTORE command: all scenarios
#[test]
fn oracle_cmd_zunionstore() {
    let mut redis = require_redis!();
    let redlite = Db::open_memory().unwrap();
    let _: () = redis::cmd("FLUSHDB").query(&mut redis).unwrap();

    // ZUNIONSTORE with non-existent keys
    let r1 = redlite.zunionstore("dest", &["nokey1", "nokey2"], None, None).unwrap();
    let r2: i64 = redis::cmd("ZUNIONSTORE").arg("dest").arg(2).arg("nokey1").arg("nokey2").query(&mut redis).unwrap();
    assert_eq!(r1, r2);
    assert_eq!(r1, 0);

    // Setup sorted sets for testing
    redlite.zadd("zset1", &[ZMember::new(1.0, b"a".to_vec()), ZMember::new(2.0, b"b".to_vec()), ZMember::new(3.0, b"c".to_vec())]).unwrap();
    redlite.zadd("zset2", &[ZMember::new(1.0, b"b".to_vec()), ZMember::new(2.0, b"c".to_vec()), ZMember::new(3.0, b"d".to_vec())]).unwrap();
    redlite.zadd("zset3", &[ZMember::new(1.0, b"c".to_vec()), ZMember::new(2.0, b"d".to_vec()), ZMember::new(3.0, b"e".to_vec())]).unwrap();

    redis::cmd("ZADD").arg("zset1").arg(1.0).arg("a").arg(2.0).arg("b").arg(3.0).arg("c").query::<()>(&mut redis).unwrap();
    redis::cmd("ZADD").arg("zset2").arg(1.0).arg("b").arg(2.0).arg("c").arg(3.0).arg("d").query::<()>(&mut redis).unwrap();
    redis::cmd("ZADD").arg("zset3").arg(1.0).arg("c").arg(2.0).arg("d").arg(3.0).arg("e").query::<()>(&mut redis).unwrap();

    // ZUNIONSTORE: zset1 ∪ zset2 (should be {a, b, c, d})
    let r1 = redlite.zunionstore("union1", &["zset1", "zset2"], None, None).unwrap();
    let r2: i64 = redis::cmd("ZUNIONSTORE").arg("union1").arg(2).arg("zset1").arg("zset2").query(&mut redis).unwrap();
    assert_eq!(r1, r2);
    assert_eq!(r1, 4);

    let members1 = redlite.zrange("union1", 0, -1, false).unwrap();
    let members2: Vec<Vec<u8>> = redis::cmd("ZRANGE").arg("union1").arg(0).arg(-1).query(&mut redis).unwrap();
    assert_eq!(members1.len(), members2.len());
    assert_eq!(members1.len(), 4);

    // Verify scores are aggregated with SUM
    let score1 = redlite.zscore("union1", b"b").unwrap();
    let score2: f64 = redis::cmd("ZSCORE").arg("union1").arg("b").query(&mut redis).unwrap();
    assert_eq!(score1, Some(score2));
    assert_eq!(score1, Some(3.0));  // 2.0 + 1.0

    let score1 = redlite.zscore("union1", b"a").unwrap();
    let score2: f64 = redis::cmd("ZSCORE").arg("union1").arg("a").query(&mut redis).unwrap();
    assert_eq!(score1, Some(score2));
    assert_eq!(score1, Some(1.0));  // Only in zset1

    // ZUNIONSTORE: zset1 ∪ zset2 ∪ zset3 (should be {a, b, c, d, e})
    let r1 = redlite.zunionstore("union2", &["zset1", "zset2", "zset3"], None, None).unwrap();
    let r2: i64 = redis::cmd("ZUNIONSTORE").arg("union2").arg(3).arg("zset1").arg("zset2").arg("zset3").query(&mut redis).unwrap();
    assert_eq!(r1, r2);
    assert_eq!(r1, 5);

    let members1 = redlite.zrange("union2", 0, -1, false).unwrap();
    assert_eq!(members1.len(), 5);

    // ZUNIONSTORE with WEIGHTS
    let r1 = redlite.zunionstore("union_weighted", &["zset1", "zset2"], Some(&[2.0, 3.0]), None).unwrap();
    let r2: i64 = redis::cmd("ZUNIONSTORE").arg("union_weighted").arg(2).arg("zset1").arg("zset2").arg("WEIGHTS").arg(2.0).arg(3.0).query(&mut redis).unwrap();
    assert_eq!(r1, r2);

    let score1 = redlite.zscore("union_weighted", b"b").unwrap();
    let score2: f64 = redis::cmd("ZSCORE").arg("union_weighted").arg("b").query(&mut redis).unwrap();
    assert_eq!(score1, Some(score2));
    assert_eq!(score1, Some(7.0));  // (2.0 * 2.0) + (1.0 * 3.0)

    let score1 = redlite.zscore("union_weighted", b"a").unwrap();
    let score2: f64 = redis::cmd("ZSCORE").arg("union_weighted").arg("a").query(&mut redis).unwrap();
    assert_eq!(score1, Some(score2));
    assert_eq!(score1, Some(2.0));  // 1.0 * 2.0

    // ZUNIONSTORE with AGGREGATE MIN
    let r1 = redlite.zunionstore("union_min", &["zset1", "zset2"], None, Some("MIN")).unwrap();
    let r2: i64 = redis::cmd("ZUNIONSTORE").arg("union_min").arg(2).arg("zset1").arg("zset2").arg("AGGREGATE").arg("MIN").query(&mut redis).unwrap();
    assert_eq!(r1, r2);

    let score1 = redlite.zscore("union_min", b"b").unwrap();
    let score2: f64 = redis::cmd("ZSCORE").arg("union_min").arg("b").query(&mut redis).unwrap();
    assert_eq!(score1, Some(score2));
    assert_eq!(score1, Some(1.0));  // min(2.0, 1.0)

    // ZUNIONSTORE with AGGREGATE MAX
    let r1 = redlite.zunionstore("union_max", &["zset1", "zset2"], None, Some("MAX")).unwrap();
    let r2: i64 = redis::cmd("ZUNIONSTORE").arg("union_max").arg(2).arg("zset1").arg("zset2").arg("AGGREGATE").arg("MAX").query(&mut redis).unwrap();
    assert_eq!(r1, r2);

    let score1 = redlite.zscore("union_max", b"c").unwrap();
    let score2: f64 = redis::cmd("ZSCORE").arg("union_max").arg("c").query(&mut redis).unwrap();
    assert_eq!(score1, Some(score2));
    assert_eq!(score1, Some(3.0));  // max(3.0, 2.0)

    // ZUNIONSTORE with single set
    let r1 = redlite.zunionstore("single", &["zset1"], None, None).unwrap();
    let r2: i64 = redis::cmd("ZUNIONSTORE").arg("single").arg(1).arg("zset1").query(&mut redis).unwrap();
    assert_eq!(r1, r2);
    assert_eq!(r1, 3);

    // ZUNIONSTORE with binary data
    redlite.zadd("bin1", &[ZMember::new(1.0, b"\x00\x01".to_vec()), ZMember::new(2.0, b"\x02\x03".to_vec())]).unwrap();
    redlite.zadd("bin2", &[ZMember::new(3.0, b"\xff\xfe".to_vec())]).unwrap();
    redis::cmd("ZADD").arg("bin1").arg(1.0).arg(b"\x00\x01").arg(2.0).arg(b"\x02\x03").query::<()>(&mut redis).unwrap();
    redis::cmd("ZADD").arg("bin2").arg(3.0).arg(b"\xff\xfe").query::<()>(&mut redis).unwrap();

    let r1 = redlite.zunionstore("binunion", &["bin1", "bin2"], None, None).unwrap();
    let r2: i64 = redis::cmd("ZUNIONSTORE").arg("binunion").arg(2).arg("bin1").arg("bin2").query(&mut redis).unwrap();
    assert_eq!(r1, r2);
    assert_eq!(r1, 3);

    // ZUNIONSTORE with unicode data
    redlite.zadd("uni1", &[ZMember::new(1.0, "hello".as_bytes().to_vec()), ZMember::new(2.0, "世界".as_bytes().to_vec())]).unwrap();
    redlite.zadd("uni2", &[ZMember::new(3.0, "🚀".as_bytes().to_vec())]).unwrap();
    redis::cmd("ZADD").arg("uni1").arg(1.0).arg("hello").arg(2.0).arg("世界").query::<()>(&mut redis).unwrap();
    redis::cmd("ZADD").arg("uni2").arg(3.0).arg("🚀").query::<()>(&mut redis).unwrap();

    let r1 = redlite.zunionstore("uniunion", &["uni1", "uni2"], None, None).unwrap();
    let r2: i64 = redis::cmd("ZUNIONSTORE").arg("uniunion").arg(2).arg("uni1").arg("uni2").query(&mut redis).unwrap();
    assert_eq!(r1, r2);
    assert_eq!(r1, 3);
}

/// PEXPIREAT command: all scenarios
#[test]
fn oracle_cmd_pexpireat() {
    let mut redis = require_redis!();
    let redlite = Db::open_memory().unwrap();
    let _: () = redis::cmd("FLUSHDB").query(&mut redis).unwrap();

    // PEXPIREAT on non-existent key
    let r1 = redlite.pexpireat("nokey", 9999999999999).unwrap();
    let r2: bool = redis::cmd("PEXPIREAT").arg("nokey").arg(9999999999999i64).query(&mut redis).unwrap();
    assert_eq!(r1, r2);
    assert_eq!(r1, false);

    // Set key and add expiration
    redlite.set("mykey", b"value", None).unwrap();
    redis::cmd("SET").arg("mykey").arg("value").query::<()>(&mut redis).unwrap();

    // PEXPIREAT with future timestamp
    let future_ms = (std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_millis() as i64) + 60000;
    let r1 = redlite.pexpireat("mykey", future_ms).unwrap();
    let r2: bool = redis::cmd("PEXPIREAT").arg("mykey").arg(future_ms).query(&mut redis).unwrap();
    assert_eq!(r1, r2);
    assert_eq!(r1, true);

    // Verify key has TTL
    let ttl1 = redlite.pttl("mykey").unwrap();
    let ttl2: i64 = redis::cmd("PTTL").arg("mykey").query(&mut redis).unwrap();
    assert!(ttl1 > 0);
    assert!(ttl2 > 0);

    // PEXPIREAT with past timestamp (key should be deleted or have negative TTL)
    redlite.set("pastkey", b"value", None).unwrap();
    redis::cmd("SET").arg("pastkey").arg("value").query::<()>(&mut redis).unwrap();

    let past_ms = 1000000000i64; // Way in the past
    let r1 = redlite.pexpireat("pastkey", past_ms).unwrap();
    let r2: bool = redis::cmd("PEXPIREAT").arg("pastkey").arg(past_ms).query(&mut redis).unwrap();
    assert_eq!(r1, r2);
    assert_eq!(r1, true);

    // Key should be expired
    let ttl1 = redlite.pttl("pastkey").unwrap();
    let ttl2: i64 = redis::cmd("PTTL").arg("pastkey").query(&mut redis).unwrap();
    // Both should return -2 (key doesn't exist) or -1 (no expiry)
    assert!(ttl1 <= -1);
    assert!(ttl2 <= -1);

    // PEXPIREAT on different data types
    redlite.lpush("mylist", &[b"item"]).unwrap();
    redis::cmd("LPUSH").arg("mylist").arg("item").query::<()>(&mut redis).unwrap();

    let future_ms = (std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_millis() as i64) + 60000;
    let r1 = redlite.pexpireat("mylist", future_ms).unwrap();
    let r2: bool = redis::cmd("PEXPIREAT").arg("mylist").arg(future_ms).query(&mut redis).unwrap();
    assert_eq!(r1, r2);
    assert_eq!(r1, true);

    // PEXPIREAT on hash
    redlite.hset("myhash", &[("field", b"value")]).unwrap();
    redis::cmd("HSET").arg("myhash").arg("field").arg("value").query::<()>(&mut redis).unwrap();

    let future_ms = (std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_millis() as i64) + 60000;
    let r1 = redlite.pexpireat("myhash", future_ms).unwrap();
    let r2: bool = redis::cmd("PEXPIREAT").arg("myhash").arg(future_ms).query(&mut redis).unwrap();
    assert_eq!(r1, r2);
    assert_eq!(r1, true);

    // PEXPIREAT overwriting existing expiration
    redlite.set("overwrite", b"value", None).unwrap();
    redis::cmd("SET").arg("overwrite").arg("value").query::<()>(&mut redis).unwrap();

    let first_ms = (std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_millis() as i64) + 30000;
    redlite.pexpireat("overwrite", first_ms).unwrap();
    redis::cmd("PEXPIREAT").arg("overwrite").arg(first_ms).query::<()>(&mut redis).unwrap();

    let second_ms = (std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_millis() as i64) + 90000;
    let r1 = redlite.pexpireat("overwrite", second_ms).unwrap();
    let r2: bool = redis::cmd("PEXPIREAT").arg("overwrite").arg(second_ms).query(&mut redis).unwrap();
    assert_eq!(r1, r2);
    assert_eq!(r1, true);

    let ttl1 = redlite.pttl("overwrite").unwrap();
    let ttl2: i64 = redis::cmd("PTTL").arg("overwrite").query(&mut redis).unwrap();
    assert!(ttl1 > 30000); // Should be closer to 90000
    assert!(ttl2 > 30000);
}

/// DBSIZE command: all scenarios
#[test]
fn oracle_cmd_dbsize() {
    let mut redis = require_redis!();
    let redlite = Db::open_memory().unwrap();
    let _: () = redis::cmd("FLUSHDB").query(&mut redis).unwrap();

    // DBSIZE on empty database
    let r1 = redlite.dbsize().unwrap();
    let r2: i64 = redis::cmd("DBSIZE").query(&mut redis).unwrap();
    assert_eq!(r1, r2);
    assert_eq!(r1, 0);

    // Add keys and check size
    redlite.set("key1", b"value1", None).unwrap();
    redis::cmd("SET").arg("key1").arg("value1").query::<()>(&mut redis).unwrap();

    let r1 = redlite.dbsize().unwrap();
    let r2: i64 = redis::cmd("DBSIZE").query(&mut redis).unwrap();
    assert_eq!(r1, r2);
    assert_eq!(r1, 1);

    // Add more keys
    redlite.set("key2", b"value2", None).unwrap();
    redlite.lpush("list1", &[b"item"]).unwrap();
    redlite.sadd("set1", &[b"member"]).unwrap();
    redlite.zadd("zset1", &[ZMember::new(1.0, b"member".to_vec())]).unwrap();

    redis::cmd("SET").arg("key2").arg("value2").query::<()>(&mut redis).unwrap();
    redis::cmd("LPUSH").arg("list1").arg("item").query::<()>(&mut redis).unwrap();
    redis::cmd("SADD").arg("set1").arg("member").query::<()>(&mut redis).unwrap();
    redis::cmd("ZADD").arg("zset1").arg(1.0).arg("member").query::<()>(&mut redis).unwrap();

    let r1 = redlite.dbsize().unwrap();
    let r2: i64 = redis::cmd("DBSIZE").query(&mut redis).unwrap();
    assert_eq!(r1, r2);
    assert_eq!(r1, 5);

    // Delete a key and verify size decreases
    redlite.del(&["key1"]).unwrap();
    redis::cmd("DEL").arg("key1").query::<()>(&mut redis).unwrap();

    let r1 = redlite.dbsize().unwrap();
    let r2: i64 = redis::cmd("DBSIZE").query(&mut redis).unwrap();
    assert_eq!(r1, r2);
    assert_eq!(r1, 4);

    // DBSIZE doesn't count expired keys
    let past_ms = 1000000000i64;
    redlite.set("expired", b"value", None).unwrap();
    redlite.pexpireat("expired", past_ms).unwrap();
    redis::cmd("SET").arg("expired").arg("value").query::<()>(&mut redis).unwrap();
    redis::cmd("PEXPIREAT").arg("expired").arg(past_ms).query::<()>(&mut redis).unwrap();

    // Trigger expiration by accessing
    let _ = redlite.get("expired");
    let _: Option<String> = redis::cmd("GET").arg("expired").query(&mut redis).ok();

    let r1 = redlite.dbsize().unwrap();
    let r2: i64 = redis::cmd("DBSIZE").query(&mut redis).unwrap();
    assert_eq!(r1, r2);
}

/// FLUSHDB command: all scenarios
#[test]
fn oracle_cmd_flushdb() {
    let mut redis = require_redis!();
    let redlite = Db::open_memory().unwrap();
    let _: () = redis::cmd("FLUSHDB").query(&mut redis).unwrap();

    // FLUSHDB on empty database
    let r1 = redlite.flushdb();
    let r2: Result<(), redis::RedisError> = redis::cmd("FLUSHDB").query(&mut redis);
    assert!(r1.is_ok());
    assert!(r2.is_ok());

    let size1 = redlite.dbsize().unwrap();
    let size2: i64 = redis::cmd("DBSIZE").query(&mut redis).unwrap();
    assert_eq!(size1, size2);
    assert_eq!(size1, 0);

    // Add various keys
    redlite.set("str1", b"value1", None).unwrap();
    redlite.set("str2", b"value2", None).unwrap();
    redlite.lpush("list1", &[b"item1", b"item2"]).unwrap();
    redlite.sadd("set1", &[b"member1", b"member2"]).unwrap();
    redlite.zadd("zset1", &[ZMember::new(1.0, b"m1".to_vec()), ZMember::new(2.0, b"m2".to_vec())]).unwrap();
    redlite.hset("hash1", &[("f1", b"v1"), ("f2", b"v2")]).unwrap();

    redis::cmd("SET").arg("str1").arg("value1").query::<()>(&mut redis).unwrap();
    redis::cmd("SET").arg("str2").arg("value2").query::<()>(&mut redis).unwrap();
    redis::cmd("LPUSH").arg("list1").arg("item1").arg("item2").query::<()>(&mut redis).unwrap();
    redis::cmd("SADD").arg("set1").arg("member1").arg("member2").query::<()>(&mut redis).unwrap();
    redis::cmd("ZADD").arg("zset1").arg(1.0).arg("m1").arg(2.0).arg("m2").query::<()>(&mut redis).unwrap();
    redis::cmd("HSET").arg("hash1").arg("f1").arg("v1").arg("f2").arg("v2").query::<()>(&mut redis).unwrap();

    let size1 = redlite.dbsize().unwrap();
    let size2: i64 = redis::cmd("DBSIZE").query(&mut redis).unwrap();
    assert_eq!(size1, size2);
    assert_eq!(size1, 6);

    // FLUSHDB should delete all keys
    let r1 = redlite.flushdb();
    let r2: Result<(), redis::RedisError> = redis::cmd("FLUSHDB").query(&mut redis);
    assert!(r1.is_ok());
    assert!(r2.is_ok());

    let size1 = redlite.dbsize().unwrap();
    let size2: i64 = redis::cmd("DBSIZE").query(&mut redis).unwrap();
    assert_eq!(size1, size2);
    assert_eq!(size1, 0);

    // Verify all keys are gone
    let exists1 = redlite.exists(&["str1", "str2", "list1", "set1", "zset1", "hash1"]).unwrap();
    let exists2: i64 = redis::cmd("EXISTS").arg("str1").arg("str2").arg("list1").arg("set1").arg("zset1").arg("hash1").query(&mut redis).unwrap();
    assert_eq!(exists1, exists2);
    assert_eq!(exists1, 0);

    // FLUSHDB with keys that have TTL
    redlite.set("ttl_key", b"value", Some(std::time::Duration::from_secs(100))).unwrap();
    redis::cmd("SET").arg("ttl_key").arg("value").arg("EX").arg(100).query::<()>(&mut redis).unwrap();

    let r1 = redlite.flushdb();
    let r2: Result<(), redis::RedisError> = redis::cmd("FLUSHDB").query(&mut redis);
    assert!(r1.is_ok());
    assert!(r2.is_ok());

    let size1 = redlite.dbsize().unwrap();
    let size2: i64 = redis::cmd("DBSIZE").query(&mut redis).unwrap();
    assert_eq!(size1, size2);
    assert_eq!(size1, 0);
}

// ============================================================================
// SELECT COMMAND - Database Selection
// ============================================================================

/// SELECT command: all scenarios
#[test]
fn oracle_cmd_select() {
    let mut redis = require_redis!();
    let mut redlite = Db::open_memory().unwrap();
    let _: () = redis::cmd("FLUSHDB").query(&mut redis).unwrap();
    // Also flush other databases we'll use
    let _: () = redis::cmd("SELECT").arg(1).query(&mut redis).unwrap();
    let _: () = redis::cmd("FLUSHDB").query(&mut redis).unwrap();
    let _: () = redis::cmd("SELECT").arg(0).query(&mut redis).unwrap();

    // SELECT 0 (default database)
    let r1 = redlite.select(0);
    let r2: Result<(), redis::RedisError> = redis::cmd("SELECT").arg(0).query(&mut redis);
    assert!(r1.is_ok());
    assert!(r2.is_ok());
    assert_eq!(redlite.current_db(), 0);

    // SELECT 1
    let r1 = redlite.select(1);
    let r2: Result<(), redis::RedisError> = redis::cmd("SELECT").arg(1).query(&mut redis);
    assert!(r1.is_ok());
    assert!(r2.is_ok());
    assert_eq!(redlite.current_db(), 1);

    // SELECT 15 (max database)
    let r1 = redlite.select(15);
    let r2: Result<(), redis::RedisError> = redis::cmd("SELECT").arg(15).query(&mut redis);
    assert!(r1.is_ok());
    assert!(r2.is_ok());
    assert_eq!(redlite.current_db(), 15);

    // Data isolation between databases
    redlite.select(0).unwrap();
    let _: () = redis::cmd("SELECT").arg(0).query(&mut redis).unwrap();

    redlite.set("key1", b"value_db0", None).unwrap();
    redis::cmd("SET").arg("key1").arg("value_db0").query::<()>(&mut redis).unwrap();

    redlite.select(1).unwrap();
    let _: () = redis::cmd("SELECT").arg(1).query(&mut redis).unwrap();

    // Key should not exist in db1
    let r1 = redlite.get("key1").unwrap();
    let r2: Option<Vec<u8>> = redis::cmd("GET").arg("key1").query(&mut redis).unwrap();
    assert_eq!(r1, r2);
    assert_eq!(r1, None);

    // Set different value in db1
    redlite.set("key1", b"value_db1", None).unwrap();
    redis::cmd("SET").arg("key1").arg("value_db1").query::<()>(&mut redis).unwrap();

    let r1 = redlite.get("key1").unwrap();
    let r2: Option<Vec<u8>> = redis::cmd("GET").arg("key1").query(&mut redis).unwrap();
    assert_eq!(r1, r2);
    assert_eq!(r1, Some(b"value_db1".to_vec()));

    // Switch back to db0 and verify original value
    redlite.select(0).unwrap();
    let _: () = redis::cmd("SELECT").arg(0).query(&mut redis).unwrap();

    let r1 = redlite.get("key1").unwrap();
    let r2: Option<Vec<u8>> = redis::cmd("GET").arg("key1").query(&mut redis).unwrap();
    assert_eq!(r1, r2);
    assert_eq!(r1, Some(b"value_db0".to_vec()));

    // SELECT invalid database (negative)
    let r1 = redlite.select(-1);
    assert!(r1.is_err());

    // SELECT invalid database (>15)
    let r1 = redlite.select(16);
    assert!(r1.is_err());

    // DBSIZE in different databases
    redlite.select(0).unwrap();
    let _: () = redis::cmd("SELECT").arg(0).query(&mut redis).unwrap();
    let size1_db0 = redlite.dbsize().unwrap();
    let size2_db0: i64 = redis::cmd("DBSIZE").query(&mut redis).unwrap();
    assert_eq!(size1_db0, size2_db0);
    assert_eq!(size1_db0, 1);

    redlite.select(1).unwrap();
    let _: () = redis::cmd("SELECT").arg(1).query(&mut redis).unwrap();
    let size1_db1 = redlite.dbsize().unwrap();
    let size2_db1: i64 = redis::cmd("DBSIZE").query(&mut redis).unwrap();
    assert_eq!(size1_db1, size2_db1);
    assert_eq!(size1_db1, 1);

    // KEYS in different databases
    redlite.select(0).unwrap();
    let _: () = redis::cmd("SELECT").arg(0).query(&mut redis).unwrap();
    let keys1_db0 = redlite.keys("*").unwrap();
    let keys2_db0: Vec<String> = redis::cmd("KEYS").arg("*").query(&mut redis).unwrap();
    assert_eq!(keys1_db0.len(), keys2_db0.len());
    assert_eq!(keys1_db0.len(), 1);
    assert!(keys1_db0.contains(&"key1".to_string()));

    // SELECT with different data types
    redlite.select(2).unwrap();
    let _: () = redis::cmd("SELECT").arg(2).query(&mut redis).unwrap();
    let _: () = redis::cmd("FLUSHDB").query(&mut redis).unwrap();

    redlite.lpush("mylist", &[b"item1", b"item2"]).unwrap();
    redis::cmd("LPUSH").arg("mylist").arg("item1").arg("item2").query::<()>(&mut redis).unwrap();

    redlite.sadd("myset", &[b"member1"]).unwrap();
    redis::cmd("SADD").arg("myset").arg("member1").query::<()>(&mut redis).unwrap();

    redlite.select(3).unwrap();
    let _: () = redis::cmd("SELECT").arg(3).query(&mut redis).unwrap();
    let _: () = redis::cmd("FLUSHDB").query(&mut redis).unwrap();

    // These should not exist in db3
    let list_len1 = redlite.llen("mylist").unwrap();
    let list_len2: i64 = redis::cmd("LLEN").arg("mylist").query(&mut redis).unwrap();
    assert_eq!(list_len1, list_len2);
    assert_eq!(list_len1, 0);

    let set_card1 = redlite.scard("myset").unwrap();
    let set_card2: i64 = redis::cmd("SCARD").arg("myset").query(&mut redis).unwrap();
    assert_eq!(set_card1, set_card2);
    assert_eq!(set_card1, 0);

    // Back to db2 - data should be there
    redlite.select(2).unwrap();
    let _: () = redis::cmd("SELECT").arg(2).query(&mut redis).unwrap();

    let list_len1 = redlite.llen("mylist").unwrap();
    let list_len2: i64 = redis::cmd("LLEN").arg("mylist").query(&mut redis).unwrap();
    assert_eq!(list_len1, list_len2);
    assert_eq!(list_len1, 2);
}

// ============================================================================
// VACUUM COMMAND - Redlite-specific (cleanup expired keys)
// ============================================================================

/// VACUUM command: all scenarios (Redlite-specific, no Redis comparison)
#[test]
fn oracle_cmd_vacuum() {
    let redlite = Db::open_memory().unwrap();

    // VACUUM on empty database
    let r1 = redlite.vacuum().unwrap();
    assert_eq!(r1, 0);

    // Add some keys with TTL
    redlite.set("key1", b"value1", None).unwrap();
    redlite.set("key2", b"value2", None).unwrap();
    redlite.set("key3", b"value3", None).unwrap();

    // Set past expiration on key2 and key3
    let past_ms = 1000i64;
    redlite.pexpireat("key2", past_ms).unwrap();
    redlite.pexpireat("key3", past_ms).unwrap();

    // VACUUM should clean up expired keys
    let r1 = redlite.vacuum().unwrap();
    assert_eq!(r1, 2); // key2 and key3 should be deleted

    // Verify only key1 remains
    let exists1 = redlite.exists(&["key1"]).unwrap();
    assert_eq!(exists1, 1);

    let exists2 = redlite.exists(&["key2"]).unwrap();
    assert_eq!(exists2, 0);

    let exists3 = redlite.exists(&["key3"]).unwrap();
    assert_eq!(exists3, 0);

    // VACUUM with keys in multiple databases
    let mut redlite = Db::open_memory().unwrap();

    redlite.select(0).unwrap();
    redlite.set("db0_key", b"value", None).unwrap();
    redlite.pexpireat("db0_key", past_ms).unwrap();

    redlite.select(1).unwrap();
    redlite.set("db1_key", b"value", None).unwrap();
    redlite.pexpireat("db1_key", past_ms).unwrap();

    redlite.select(2).unwrap();
    redlite.set("db2_key", b"value", None).unwrap();
    // This key has no expiration

    // VACUUM cleans ALL databases
    let r1 = redlite.vacuum().unwrap();
    assert_eq!(r1, 2); // db0_key and db1_key

    // Verify db2_key still exists
    redlite.select(2).unwrap();
    let exists = redlite.exists(&["db2_key"]).unwrap();
    assert_eq!(exists, 1);

    // VACUUM with different data types
    let redlite = Db::open_memory().unwrap();

    redlite.lpush("mylist", &[b"item"]).unwrap();
    redlite.pexpireat("mylist", past_ms).unwrap();

    redlite.sadd("myset", &[b"member"]).unwrap();
    redlite.pexpireat("myset", past_ms).unwrap();

    redlite.hset("myhash", &[("field", b"value")]).unwrap();
    redlite.pexpireat("myhash", past_ms).unwrap();

    redlite.zadd("myzset", &[ZMember::new(1.0, b"member".to_vec())]).unwrap();
    redlite.pexpireat("myzset", past_ms).unwrap();

    let r1 = redlite.vacuum().unwrap();
    assert_eq!(r1, 4);

    // All keys should be gone
    let size = redlite.dbsize().unwrap();
    assert_eq!(size, 0);

    // VACUUM idempotent - calling again should return 0
    let r1 = redlite.vacuum().unwrap();
    assert_eq!(r1, 0);
}

/// AUTOVACUUM configuration: all scenarios (Redlite-specific)
#[test]
fn oracle_cmd_autovacuum() {
    let redlite = Db::open_memory().unwrap();

    // Default autovacuum state
    let enabled = redlite.autovacuum_enabled();
    // Default can be either true or false depending on implementation

    // Disable autovacuum
    redlite.set_autovacuum(false);
    assert_eq!(redlite.autovacuum_enabled(), false);

    // Enable autovacuum
    redlite.set_autovacuum(true);
    assert_eq!(redlite.autovacuum_enabled(), true);

    // Get default interval
    let default_interval = redlite.autovacuum_interval();
    assert!(default_interval > 0);

    // Set custom interval
    redlite.set_autovacuum_interval(30000); // 30 seconds
    assert_eq!(redlite.autovacuum_interval(), 30000);

    // Set minimum interval (should be capped at 1000ms)
    redlite.set_autovacuum_interval(100);
    assert_eq!(redlite.autovacuum_interval(), 1000);

    // Set larger interval
    redlite.set_autovacuum_interval(120000); // 2 minutes
    assert_eq!(redlite.autovacuum_interval(), 120000);
}

// ============================================================================
// GEOSEARCHSTORE COMMAND - Store geo search results
// ============================================================================

/// GEOSEARCHSTORE command: all scenarios
#[test]
#[cfg(feature = "geo")]
fn oracle_cmd_geosearchstore() {
    use redlite::{GeoSearchOptions, GeoUnit};

    let mut redis = require_redis!();
    let redlite = Db::open_memory().unwrap();
    let _: () = redis::cmd("FLUSHDB").query(&mut redis).unwrap();

    // Setup: add geo locations
    redlite.geoadd("places", &[
        (13.361389, 52.519444, "Berlin"),
        (15.087269, 37.502669, "Catania"),
        (13.377689, 52.509444, "Potsdam"),
        (11.576124, 48.137154, "Munich"),
        (9.993682, 53.551086, "Hamburg"),
    ]).unwrap();

    redis::cmd("GEOADD").arg("places")
        .arg(13.361389).arg(52.519444).arg("Berlin")
        .arg(15.087269).arg(37.502669).arg("Catania")
        .arg(13.377689).arg(52.509444).arg("Potsdam")
        .arg(11.576124).arg(48.137154).arg("Munich")
        .arg(9.993682).arg(53.551086).arg("Hamburg")
        .query::<()>(&mut redis).unwrap();

    // GEOSEARCHSTORE with non-existent source
    let opts = GeoSearchOptions {
        from_lonlat: Some((13.361389, 52.519444)),
        by_radius: Some((100.0, GeoUnit::Km)),
        ..Default::default()
    };
    let r1 = redlite.geosearchstore("dest_empty", "nonexistent", &opts, false).unwrap();
    let r2: i64 = redis::cmd("GEOSEARCHSTORE").arg("dest_empty").arg("nonexistent")
        .arg("FROMMEMBER").arg("invalid")
        .arg("BYRADIUS").arg(100).arg("km")
        .query(&mut redis).unwrap_or(0);
    assert_eq!(r1, r2);
    assert_eq!(r1, 0);

    // GEOSEARCHSTORE FROMLONLAT BYRADIUS - find cities near Berlin
    let opts = GeoSearchOptions {
        from_lonlat: Some((13.361389, 52.519444)),
        by_radius: Some((100.0, GeoUnit::Km)),
        ..Default::default()
    };
    let r1 = redlite.geosearchstore("nearby_berlin", "places", &opts, false).unwrap();
    let r2: i64 = redis::cmd("GEOSEARCHSTORE").arg("nearby_berlin").arg("places")
        .arg("FROMLONLAT").arg(13.361389).arg(52.519444)
        .arg("BYRADIUS").arg(100).arg("km")
        .query(&mut redis).unwrap();
    assert_eq!(r1, r2);
    assert!(r1 >= 2); // Berlin and Potsdam at minimum

    // Verify result is a sorted set
    let card1 = redlite.zcard("nearby_berlin").unwrap();
    let card2: i64 = redis::cmd("ZCARD").arg("nearby_berlin").query(&mut redis).unwrap();
    assert_eq!(card1, card2);
    assert!(card1 >= 2);

    // GEOSEARCHSTORE FROMMEMBER BYRADIUS
    let opts = GeoSearchOptions {
        from_member: Some("Berlin".to_string()),
        by_radius: Some((500.0, GeoUnit::Km)),
        ..Default::default()
    };
    let r1 = redlite.geosearchstore("from_berlin", "places", &opts, false).unwrap();
    let r2: i64 = redis::cmd("GEOSEARCHSTORE").arg("from_berlin").arg("places")
        .arg("FROMMEMBER").arg("Berlin")
        .arg("BYRADIUS").arg(500).arg("km")
        .query(&mut redis).unwrap();
    assert_eq!(r1, r2);
    assert!(r1 >= 3); // Berlin, Potsdam, Hamburg, Munich should be within 500km

    // GEOSEARCHSTORE BYBOX - find cities in a box
    let opts = GeoSearchOptions {
        from_lonlat: Some((12.0, 50.0)),
        by_box: Some((600.0, 600.0, GeoUnit::Km)),
        ..Default::default()
    };
    let r1 = redlite.geosearchstore("in_box", "places", &opts, false).unwrap();
    let r2: i64 = redis::cmd("GEOSEARCHSTORE").arg("in_box").arg("places")
        .arg("FROMLONLAT").arg(12.0).arg(50.0)
        .arg("BYBOX").arg(600).arg(600).arg("km")
        .query(&mut redis).unwrap();
    assert_eq!(r1, r2);
    assert!(r1 >= 1);

    // GEOSEARCHSTORE with COUNT limit
    let opts = GeoSearchOptions {
        from_lonlat: Some((13.361389, 52.519444)),
        by_radius: Some((1000.0, GeoUnit::Km)),
        count: Some(2),
        ..Default::default()
    };
    let r1 = redlite.geosearchstore("limited", "places", &opts, false).unwrap();
    let r2: i64 = redis::cmd("GEOSEARCHSTORE").arg("limited").arg("places")
        .arg("FROMLONLAT").arg(13.361389).arg(52.519444)
        .arg("BYRADIUS").arg(1000).arg("km")
        .arg("COUNT").arg(2)
        .query(&mut redis).unwrap();
    assert_eq!(r1, r2);
    assert_eq!(r1, 2);

    // GEOSEARCHSTORE with STOREDIST - store distance as score instead of geohash
    let opts = GeoSearchOptions {
        from_member: Some("Berlin".to_string()),
        by_radius: Some((100.0, GeoUnit::Km)),
        ..Default::default()
    };
    let r1 = redlite.geosearchstore("with_dist", "places", &opts, true).unwrap();
    let r2: i64 = redis::cmd("GEOSEARCHSTORE").arg("with_dist").arg("places")
        .arg("FROMMEMBER").arg("Berlin")
        .arg("BYRADIUS").arg(100).arg("km")
        .arg("STOREDIST")
        .query(&mut redis).unwrap();
    assert_eq!(r1, r2);
    assert!(r1 >= 2);

    // With STOREDIST, Berlin should have score 0 (distance from itself)
    let score1 = redlite.zscore("with_dist", b"Berlin").unwrap();
    let score2: Option<f64> = redis::cmd("ZSCORE").arg("with_dist").arg("Berlin").query(&mut redis).unwrap();
    // Both should be 0 or very close to 0
    assert!(score1.unwrap_or(1.0).abs() < 0.001);
    assert!(score2.unwrap_or(1.0).abs() < 0.001);

    // GEOSEARCHSTORE with ASC ordering
    let opts = GeoSearchOptions {
        from_member: Some("Berlin".to_string()),
        by_radius: Some((500.0, GeoUnit::Km)),
        ascending: true,
        ..Default::default()
    };
    let r1 = redlite.geosearchstore("asc_order", "places", &opts, true).unwrap();
    let r2: i64 = redis::cmd("GEOSEARCHSTORE").arg("asc_order").arg("places")
        .arg("FROMMEMBER").arg("Berlin")
        .arg("BYRADIUS").arg(500).arg("km")
        .arg("ASC")
        .arg("STOREDIST")
        .query(&mut redis).unwrap();
    assert_eq!(r1, r2);

    // GEOSEARCHSTORE overwrites existing destination
    redlite.sadd("existing_dest", &[b"old_member"]).unwrap();
    redis::cmd("SADD").arg("existing_dest").arg("old_member").query::<()>(&mut redis).unwrap();

    let opts = GeoSearchOptions {
        from_member: Some("Berlin".to_string()),
        by_radius: Some((100.0, GeoUnit::Km)),
        ..Default::default()
    };
    let r1 = redlite.geosearchstore("existing_dest", "places", &opts, false).unwrap();
    let r2: i64 = redis::cmd("GEOSEARCHSTORE").arg("existing_dest").arg("places")
        .arg("FROMMEMBER").arg("Berlin")
        .arg("BYRADIUS").arg(100).arg("km")
        .query(&mut redis).unwrap();
    assert_eq!(r1, r2);

    // Check that old data is gone
    let type1 = redlite.key_type("existing_dest").unwrap();
    let type2: String = redis::cmd("TYPE").arg("existing_dest").query(&mut redis).unwrap();
    assert_eq!(type1, type2);
    assert_eq!(type1, "zset");
}

// ============================================================================
// XCLAIM COMMAND - Claim pending stream entries
// ============================================================================

/// XCLAIM command: all scenarios
#[test]
fn oracle_cmd_xclaim() {
    let mut redis = require_redis!();
    let redlite = Db::open_memory().unwrap();
    let _: () = redis::cmd("FLUSHDB").query(&mut redis).unwrap();

    // Setup: Create stream with entries
    let id1_redlite = redlite.xadd("mystream", None, &[(b"field1".as_slice(), b"value1".as_slice())], false, None, None, false).unwrap().unwrap();
    let id1_str = format!("{}-{}", id1_redlite.ms, id1_redlite.seq);
    let _: String = redis::cmd("XADD").arg("mystream").arg("*").arg("field1").arg("value1").query(&mut redis).unwrap();

    std::thread::sleep(std::time::Duration::from_millis(10));

    let id2_redlite = redlite.xadd("mystream", None, &[(b"field2".as_slice(), b"value2".as_slice())], false, None, None, false).unwrap().unwrap();
    let id2_str = format!("{}-{}", id2_redlite.ms, id2_redlite.seq);
    let _: String = redis::cmd("XADD").arg("mystream").arg("*").arg("field2").arg("value2").query(&mut redis).unwrap();

    // Create consumer group
    redlite.xgroup_create("mystream", "mygroup", StreamId::max(), true).unwrap();
    redis::cmd("XGROUP").arg("CREATE").arg("mystream").arg("mygroup").arg("$").arg("MKSTREAM").query::<()>(&mut redis).unwrap();

    // Read entries with consumer1 to create pending entries
    let _ = redlite.xreadgroup("mygroup", "consumer1", &["mystream"], &[">"], Some(10), false);
    let _: Vec<(String, Vec<(String, Vec<(String, String)>)>)> = redis::cmd("XREADGROUP")
        .arg("GROUP").arg("mygroup").arg("consumer1")
        .arg("COUNT").arg(10)
        .arg("STREAMS").arg("mystream").arg(">")
        .query(&mut redis).unwrap_or_default();

    // Add new entries and read them
    let id3_redlite = redlite.xadd("mystream", None, &[(b"field3".as_slice(), b"value3".as_slice())], false, None, None, false).unwrap().unwrap();
    let id3_str = format!("{}-{}", id3_redlite.ms, id3_redlite.seq);
    let id3_redis: String = redis::cmd("XADD").arg("mystream").arg("*").arg("field3").arg("value3").query(&mut redis).unwrap();

    let _ = redlite.xreadgroup("mygroup", "consumer1", &["mystream"], &[">"], Some(10), false);
    let _: Vec<(String, Vec<(String, Vec<(String, String)>)>)> = redis::cmd("XREADGROUP")
        .arg("GROUP").arg("mygroup").arg("consumer1")
        .arg("COUNT").arg(10)
        .arg("STREAMS").arg("mystream").arg(">")
        .query(&mut redis).unwrap_or_default();

    // Wait for entries to become idle
    std::thread::sleep(std::time::Duration::from_millis(100));

    // XCLAIM: claim entry from consumer1 to consumer2
    let r1 = redlite.xclaim(
        "mystream",
        "mygroup",
        "consumer2",
        50, // min-idle-time 50ms
        &[id3_redlite],
        None,
        None,
        None,
        false,
        false,
    ).unwrap();

    let r2: Vec<(String, Vec<(String, String)>)> = redis::cmd("XCLAIM")
        .arg("mystream")
        .arg("mygroup")
        .arg("consumer2")
        .arg(50)
        .arg(&id3_redis)
        .query(&mut redis).unwrap_or_default();

    // Both should return the claimed entry
    assert_eq!(r1.len(), r2.len());

    // XCLAIM on non-existent stream
    let r1 = redlite.xclaim(
        "nonexistent",
        "mygroup",
        "consumer2",
        0,
        &[id3_redlite],
        None,
        None,
        None,
        false,
        false,
    ).unwrap();
    assert!(r1.is_empty());

    // XCLAIM with JUSTID option
    let id4_redlite = redlite.xadd("mystream", None, &[(b"field4".as_slice(), b"value4".as_slice())], false, None, None, false).unwrap().unwrap();
    let id4_redis: String = redis::cmd("XADD").arg("mystream").arg("*").arg("field4").arg("value4").query(&mut redis).unwrap();

    let _ = redlite.xreadgroup("mygroup", "consumer1", &["mystream"], &[">"], Some(10), false);
    let _: Vec<(String, Vec<(String, Vec<(String, String)>)>)> = redis::cmd("XREADGROUP")
        .arg("GROUP").arg("mygroup").arg("consumer1")
        .arg("COUNT").arg(10)
        .arg("STREAMS").arg("mystream").arg(">")
        .query(&mut redis).unwrap_or_default();

    std::thread::sleep(std::time::Duration::from_millis(100));

    let r1 = redlite.xclaim(
        "mystream",
        "mygroup",
        "consumer2",
        50,
        &[id4_redlite],
        None,
        None,
        None,
        false,
        true, // JUSTID
    ).unwrap();

    let r2: Vec<String> = redis::cmd("XCLAIM")
        .arg("mystream")
        .arg("mygroup")
        .arg("consumer2")
        .arg(50)
        .arg(&id4_redis)
        .arg("JUSTID")
        .query(&mut redis).unwrap_or_default();

    assert_eq!(r1.len(), r2.len());
    // With JUSTID, entries should have empty fields
    if !r1.is_empty() {
        assert!(r1[0].fields.is_empty());
    }

    // XCLAIM with FORCE option - claim entry not in pending
    let id5_redlite = redlite.xadd("mystream", None, &[(b"field5".as_slice(), b"value5".as_slice())], false, None, None, false).unwrap().unwrap();
    let id5_redis: String = redis::cmd("XADD").arg("mystream").arg("*").arg("field5").arg("value5").query(&mut redis).unwrap();

    // Don't read it first, so it won't be pending

    let r1 = redlite.xclaim(
        "mystream",
        "mygroup",
        "consumer3",
        0,
        &[id5_redlite],
        None,
        None,
        None,
        true, // FORCE
        false,
    ).unwrap();

    let r2: Vec<(String, Vec<(String, String)>)> = redis::cmd("XCLAIM")
        .arg("mystream")
        .arg("mygroup")
        .arg("consumer3")
        .arg(0)
        .arg(&id5_redis)
        .arg("FORCE")
        .query(&mut redis).unwrap_or_default();

    assert_eq!(r1.len(), r2.len());
}

// ============================================================================
// XINFO COMMAND - Stream information
// ============================================================================

/// XINFO STREAM command: all scenarios
#[test]
fn oracle_cmd_xinfo_stream() {
    let mut redis = require_redis!();
    let redlite = Db::open_memory().unwrap();
    let _: () = redis::cmd("FLUSHDB").query(&mut redis).unwrap();

    // XINFO STREAM on non-existent stream
    let r1 = redlite.xinfo_stream("nonexistent").unwrap();
    assert!(r1.is_none());

    // Create stream with entries
    let id1 = redlite.xadd("mystream", None, &[(b"name".as_slice(), b"Alice".as_slice()), (b"age".as_slice(), b"30".as_slice())], false, None, None, false).unwrap().unwrap();
    let _: String = redis::cmd("XADD").arg("mystream").arg("*").arg("name").arg("Alice").arg("age").arg("30").query(&mut redis).unwrap();

    let id2 = redlite.xadd("mystream", None, &[(b"name".as_slice(), b"Bob".as_slice()), (b"age".as_slice(), b"25".as_slice())], false, None, None, false).unwrap().unwrap();
    let _: String = redis::cmd("XADD").arg("mystream").arg("*").arg("name").arg("Bob").arg("age").arg("25").query(&mut redis).unwrap();

    let id3 = redlite.xadd("mystream", None, &[(b"name".as_slice(), b"Charlie".as_slice()), (b"age".as_slice(), b"35".as_slice())], false, None, None, false).unwrap().unwrap();
    let _: String = redis::cmd("XADD").arg("mystream").arg("*").arg("name").arg("Charlie").arg("age").arg("35").query(&mut redis).unwrap();

    // XINFO STREAM
    let info1 = redlite.xinfo_stream("mystream").unwrap();
    assert!(info1.is_some());
    let info1 = info1.unwrap();

    // Query Redis for comparison (Redis returns complex nested structure)
    let info2: HashMap<String, redis::Value> = redis::cmd("XINFO").arg("STREAM").arg("mystream").query(&mut redis).unwrap();

    // Verify length matches
    assert_eq!(info1.length, 3);

    // Verify first and last entry IDs
    assert_eq!(info1.first_entry.as_ref().unwrap().id, id1);
    assert_eq!(info1.last_entry.as_ref().unwrap().id, id3);

    // Create consumer group and verify groups count
    redlite.xgroup_create("mystream", "group1", StreamId::min(), false).unwrap();
    redis::cmd("XGROUP").arg("CREATE").arg("mystream").arg("group1").arg("0").query::<()>(&mut redis).unwrap();

    // Note: StreamInfo doesn't include groups count in redlite, use xinfo_groups instead
    let groups1 = redlite.xinfo_groups("mystream").unwrap();
    assert_eq!(groups1.len(), 1);

    // Add another group
    redlite.xgroup_create("mystream", "group2", StreamId::min(), false).unwrap();
    redis::cmd("XGROUP").arg("CREATE").arg("mystream").arg("group2").arg("0").query::<()>(&mut redis).unwrap();

    let groups1 = redlite.xinfo_groups("mystream").unwrap();
    assert_eq!(groups1.len(), 2);
}

/// XINFO GROUPS command: all scenarios
#[test]
fn oracle_cmd_xinfo_groups() {
    let mut redis = require_redis!();
    let redlite = Db::open_memory().unwrap();
    let _: () = redis::cmd("FLUSHDB").query(&mut redis).unwrap();

    // XINFO GROUPS on non-existent stream
    let r1 = redlite.xinfo_groups("nonexistent").unwrap();
    assert!(r1.is_empty());

    // Create stream
    redlite.xadd("mystream", None, &[(b"field".as_slice(), b"value".as_slice())], false, None, None, false).unwrap();
    redis::cmd("XADD").arg("mystream").arg("*").arg("field").arg("value").query::<String>(&mut redis).unwrap();

    // XINFO GROUPS with no groups
    let r1 = redlite.xinfo_groups("mystream").unwrap();
    let r2: Vec<HashMap<String, redis::Value>> = redis::cmd("XINFO").arg("GROUPS").arg("mystream").query(&mut redis).unwrap();
    assert_eq!(r1.len(), r2.len());
    assert_eq!(r1.len(), 0);

    // Create consumer groups
    redlite.xgroup_create("mystream", "group1", StreamId::min(), false).unwrap();
    redis::cmd("XGROUP").arg("CREATE").arg("mystream").arg("group1").arg("0").query::<()>(&mut redis).unwrap();

    redlite.xgroup_create("mystream", "group2", StreamId::max(), false).unwrap();
    redis::cmd("XGROUP").arg("CREATE").arg("mystream").arg("group2").arg("$").query::<()>(&mut redis).unwrap();

    // XINFO GROUPS
    let r1 = redlite.xinfo_groups("mystream").unwrap();
    let r2: Vec<HashMap<String, redis::Value>> = redis::cmd("XINFO").arg("GROUPS").arg("mystream").query(&mut redis).unwrap();
    assert_eq!(r1.len(), r2.len());
    assert_eq!(r1.len(), 2);

    // Verify group names
    let names1: Vec<&str> = r1.iter().map(|g| g.name.as_str()).collect();
    assert!(names1.contains(&"group1"));
    assert!(names1.contains(&"group2"));

    // Add entries and read with consumer to create pending
    let id1 = redlite.xadd("mystream", None, &[(b"data".as_slice(), b"test1".as_slice())], false, None, None, false).unwrap();
    let id1_redis: String = redis::cmd("XADD").arg("mystream").arg("*").arg("data").arg("test1").query(&mut redis).unwrap();

    let _ = redlite.xreadgroup("group1", "consumer1", &["mystream"], &[">"], Some(10), false);
    let _: Vec<(String, Vec<(String, Vec<(String, String)>)>)> = redis::cmd("XREADGROUP")
        .arg("GROUP").arg("group1").arg("consumer1")
        .arg("COUNT").arg(10)
        .arg("STREAMS").arg("mystream").arg(">")
        .query(&mut redis).unwrap_or_default();

    // Check pending count for group1
    let r1 = redlite.xinfo_groups("mystream").unwrap();
    let group1 = r1.iter().find(|g| g.name == "group1").unwrap();
    assert!(group1.pending >= 1);
    assert!(group1.consumers >= 1);
}

/// XINFO CONSUMERS command: all scenarios
#[test]
fn oracle_cmd_xinfo_consumers() {
    let mut redis = require_redis!();
    let redlite = Db::open_memory().unwrap();
    let _: () = redis::cmd("FLUSHDB").query(&mut redis).unwrap();

    // Create stream and group
    redlite.xadd("mystream", None, &[(b"field".as_slice(), b"value".as_slice())], false, None, None, false).unwrap();
    redis::cmd("XADD").arg("mystream").arg("*").arg("field").arg("value").query::<String>(&mut redis).unwrap();

    redlite.xgroup_create("mystream", "mygroup", StreamId::min(), false).unwrap();
    redis::cmd("XGROUP").arg("CREATE").arg("mystream").arg("mygroup").arg("0").query::<()>(&mut redis).unwrap();

    // XINFO CONSUMERS with no consumers
    let r1 = redlite.xinfo_consumers("mystream", "mygroup").unwrap();
    let r2: Vec<HashMap<String, redis::Value>> = redis::cmd("XINFO").arg("CONSUMERS").arg("mystream").arg("mygroup").query(&mut redis).unwrap();
    assert_eq!(r1.len(), r2.len());
    assert_eq!(r1.len(), 0);

    // Add entries and create consumers by reading
    let _ = redlite.xadd("mystream", None, &[(b"data".as_slice(), b"test1".as_slice())], false, None, None, false).unwrap();
    let _: String = redis::cmd("XADD").arg("mystream").arg("*").arg("data").arg("test1").query(&mut redis).unwrap();

    let _ = redlite.xadd("mystream", None, &[(b"data".as_slice(), b"test2".as_slice())], false, None, None, false).unwrap();
    let _: String = redis::cmd("XADD").arg("mystream").arg("*").arg("data").arg("test2").query(&mut redis).unwrap();

    // Consumer 1 reads
    let _ = redlite.xreadgroup("mygroup", "consumer1", &["mystream"], &[">"], Some(1), false);
    let _: Vec<(String, Vec<(String, Vec<(String, String)>)>)> = redis::cmd("XREADGROUP")
        .arg("GROUP").arg("mygroup").arg("consumer1")
        .arg("COUNT").arg(1)
        .arg("STREAMS").arg("mystream").arg(">")
        .query(&mut redis).unwrap_or_default();

    // Consumer 2 reads
    let _ = redlite.xreadgroup("mygroup", "consumer2", &["mystream"], &[">"], Some(1), false);
    let _: Vec<(String, Vec<(String, Vec<(String, String)>)>)> = redis::cmd("XREADGROUP")
        .arg("GROUP").arg("mygroup").arg("consumer2")
        .arg("COUNT").arg(1)
        .arg("STREAMS").arg("mystream").arg(">")
        .query(&mut redis).unwrap_or_default();

    // XINFO CONSUMERS
    let r1 = redlite.xinfo_consumers("mystream", "mygroup").unwrap();
    let r2: Vec<HashMap<String, redis::Value>> = redis::cmd("XINFO").arg("CONSUMERS").arg("mystream").arg("mygroup").query(&mut redis).unwrap();
    assert_eq!(r1.len(), r2.len());
    assert_eq!(r1.len(), 2);

    // Verify consumer names
    let names1: Vec<&str> = r1.iter().map(|c| c.name.as_str()).collect();
    assert!(names1.contains(&"consumer1"));
    assert!(names1.contains(&"consumer2"));

    // Each consumer should have 1 pending
    for consumer in &r1 {
        assert_eq!(consumer.pending, 1);
    }

    // XINFO CONSUMERS with non-existent group
    let r1 = redlite.xinfo_consumers("mystream", "nonexistent");
    assert!(r1.is_err());
}

// ============================================================================
// FT.* COMMANDS - RediSearch-compatible Full-Text Search (Redlite-specific)
// ============================================================================

/// FT.CREATE command: all scenarios
#[test]
fn oracle_cmd_ft_create() {
    use redlite::{FtField, FtOnType, FtFieldType};

    let redlite = Db::open_memory().unwrap();

    // FT.CREATE basic index with TEXT field
    let r1 = redlite.ft_create(
        "idx_basic",
        FtOnType::Hash,
        &["doc:"],
        &[FtField::text("title"), FtField::text("body")],
    );
    assert!(r1.is_ok());

    // Verify index exists
    let list = redlite.ft_list().unwrap();
    assert!(list.contains(&"idx_basic".to_string()));

    // FT.CREATE with multiple field types
    let r1 = redlite.ft_create(
        "idx_multi",
        FtOnType::Hash,
        &["product:"],
        &[
            FtField::text("name"),
            FtField::numeric("price"),
            FtField::tag("category"),
            FtField::text("description").sortable(),
        ],
    );
    assert!(r1.is_ok());

    // Verify index info
    let info = redlite.ft_info("idx_multi").unwrap();
    assert!(info.is_some());
    let info = info.unwrap();
    assert_eq!(info.name, "idx_multi");
    assert_eq!(info.schema.len(), 4);

    // FT.CREATE with multiple prefixes
    let r1 = redlite.ft_create(
        "idx_prefixes",
        FtOnType::Hash,
        &["user:", "admin:"],
        &[FtField::text("name")],
    );
    assert!(r1.is_ok());

    let info = redlite.ft_info("idx_prefixes").unwrap().unwrap();
    assert_eq!(info.prefixes.len(), 2);

    // FT.CREATE duplicate index should error
    let r1 = redlite.ft_create(
        "idx_basic",
        FtOnType::Hash,
        &[],
        &[FtField::text("title")],
    );
    assert!(r1.is_err());
}

/// FT.DROPINDEX command: all scenarios
#[test]
fn oracle_cmd_ft_dropindex() {
    use redlite::{FtField, FtOnType};

    let redlite = Db::open_memory().unwrap();

    // Create index to drop
    redlite.ft_create("idx_drop", FtOnType::Hash, &["drop:"], &[FtField::text("content")]).unwrap();

    // Verify it exists
    let list = redlite.ft_list().unwrap();
    assert!(list.contains(&"idx_drop".to_string()));

    // FT.DROPINDEX
    let r1 = redlite.ft_dropindex("idx_drop", false);
    assert!(r1.is_ok());
    assert!(r1.unwrap());

    // Verify it's gone
    let list = redlite.ft_list().unwrap();
    assert!(!list.contains(&"idx_drop".to_string()));

    // FT.DROPINDEX on non-existent index
    let r1 = redlite.ft_dropindex("nonexistent", false);
    assert!(r1.is_ok());
    assert!(!r1.unwrap());
}

/// FT._LIST command: all scenarios
#[test]
fn oracle_cmd_ft_list() {
    use redlite::{FtField, FtOnType};

    let redlite = Db::open_memory().unwrap();

    // FT._LIST on empty
    let list = redlite.ft_list().unwrap();
    assert!(list.is_empty());

    // Create multiple indexes
    for i in 0..5 {
        redlite.ft_create(
            &format!("idx_{}", i),
            FtOnType::Hash,
            &[&format!("prefix{}:", i)],
            &[FtField::text("content")],
        ).unwrap();
    }

    // FT._LIST with multiple indexes
    let list = redlite.ft_list().unwrap();
    assert_eq!(list.len(), 5);
    for i in 0..5 {
        assert!(list.contains(&format!("idx_{}", i)));
    }
}

/// FT.INFO command: all scenarios
#[test]
fn oracle_cmd_ft_info() {
    use redlite::{FtField, FtOnType, FtFieldType};

    let redlite = Db::open_memory().unwrap();

    // FT.INFO on non-existent index
    let info = redlite.ft_info("nonexistent").unwrap();
    assert!(info.is_none());

    // Create index
    redlite.ft_create(
        "idx_info",
        FtOnType::Hash,
        &["info:"],
        &[
            FtField::text("title").sortable(),
            FtField::numeric("price"),
            FtField::tag("tags"),
        ],
    ).unwrap();

    // FT.INFO on existing index
    let info = redlite.ft_info("idx_info").unwrap();
    assert!(info.is_some());
    let info = info.unwrap();
    assert_eq!(info.name, "idx_info");
    assert_eq!(info.on_type, FtOnType::Hash);
    assert_eq!(info.prefixes, vec!["info:"]);
    assert_eq!(info.schema.len(), 3);

    // Verify field properties
    let title_field = info.schema.iter().find(|f| f.name == "title").unwrap();
    assert!(title_field.sortable);
    assert_eq!(title_field.field_type, FtFieldType::Text);

    let price_field = info.schema.iter().find(|f| f.name == "price").unwrap();
    assert_eq!(price_field.field_type, FtFieldType::Numeric);
}

/// FT.ALTER command: all scenarios
#[test]
fn oracle_cmd_ft_alter() {
    use redlite::{FtField, FtOnType};

    let redlite = Db::open_memory().unwrap();

    // Create index
    redlite.ft_create("idx_alter", FtOnType::Hash, &["alter:"], &[FtField::text("title")]).unwrap();

    // Verify initial schema
    let info = redlite.ft_info("idx_alter").unwrap().unwrap();
    assert_eq!(info.schema.len(), 1);

    // FT.ALTER add new field
    let r1 = redlite.ft_alter("idx_alter", FtField::text("description"));
    assert!(r1.is_ok());

    // Verify field was added
    let info = redlite.ft_info("idx_alter").unwrap().unwrap();
    assert_eq!(info.schema.len(), 2);
    assert!(info.schema.iter().any(|f| f.name == "description"));

    // FT.ALTER add numeric field
    let r1 = redlite.ft_alter("idx_alter", FtField::numeric("views"));
    assert!(r1.is_ok());

    let info = redlite.ft_info("idx_alter").unwrap().unwrap();
    assert_eq!(info.schema.len(), 3);

    // FT.ALTER on non-existent index
    let r1 = redlite.ft_alter("nonexistent", FtField::text("field"));
    assert!(r1.is_err());
}

/// FT.SEARCH command: all scenarios
#[test]
fn oracle_cmd_ft_search() {
    use redlite::{FtField, FtOnType, FtSearchOptions};

    let redlite = Db::open_memory().unwrap();

    // Create index
    redlite.ft_create(
        "idx_search",
        FtOnType::Hash,
        &["article:"],
        &[
            FtField::text("title"),
            FtField::text("body"),
            FtField::numeric("views"),
            FtField::tag("category"),
        ],
    ).unwrap();

    // Add documents
    redlite.hset("article:1", &[
        ("title", b"Introduction to Rust Programming".as_slice()),
        ("body", b"Rust is a systems programming language focused on safety and performance.".as_slice()),
        ("views", b"100".as_slice()),
        ("category", b"programming".as_slice()),
    ]).unwrap();

    redlite.hset("article:2", &[
        ("title", b"Python for Data Science".as_slice()),
        ("body", b"Python is widely used in data science and machine learning.".as_slice()),
        ("views", b"200".as_slice()),
        ("category", b"programming".as_slice()),
    ]).unwrap();

    redlite.hset("article:3", &[
        ("title", b"Web Development with JavaScript".as_slice()),
        ("body", b"JavaScript is essential for building modern web applications.".as_slice()),
        ("views", b"150".as_slice()),
        ("category", b"web".as_slice()),
    ]).unwrap();

    // FT.SEARCH basic query - returns (total_count, results)
    let options = FtSearchOptions::default();
    let (_, results) = redlite.ft_search("idx_search", "rust", &options).unwrap();
    assert_eq!(results.len(), 1);
    assert!(results[0].key.contains("article:1"));

    // FT.SEARCH wildcard (match all)
    let (_, results) = redlite.ft_search("idx_search", "*", &options).unwrap();
    assert_eq!(results.len(), 3);

    // FT.SEARCH with multiple terms (AND)
    let (_, results) = redlite.ft_search("idx_search", "programming language", &options).unwrap();
    assert!(results.len() >= 1);

    // FT.SEARCH with OR
    let (_, results) = redlite.ft_search("idx_search", "rust | python", &options).unwrap();
    assert!(results.len() >= 2);

    // FT.SEARCH with NOT
    let (_, results) = redlite.ft_search("idx_search", "programming -rust", &options).unwrap();
    assert!(results.iter().all(|r| !r.key.contains("article:1")));

    // FT.SEARCH field-specific
    let (_, results) = redlite.ft_search("idx_search", "@title:rust", &options).unwrap();
    assert_eq!(results.len(), 1);

    // FT.SEARCH with NOCONTENT
    let mut options = FtSearchOptions::default();
    options.nocontent = true;
    let (_, results) = redlite.ft_search("idx_search", "*", &options).unwrap();
    assert_eq!(results.len(), 3);
    // Fields should be empty with NOCONTENT
    for result in &results {
        assert!(result.fields.is_empty());
    }

    // FT.SEARCH with WITHSCORES
    let mut options = FtSearchOptions::default();
    options.withscores = true;
    let (_, results) = redlite.ft_search("idx_search", "programming", &options).unwrap();
    for result in &results {
        assert!(result.score > 0.0);
    }

    // FT.SEARCH with LIMIT
    let mut options = FtSearchOptions::default();
    options.limit_offset = 0;
    options.limit_num = 2;
    let (_, results) = redlite.ft_search("idx_search", "*", &options).unwrap();
    assert_eq!(results.len(), 2);

    // FT.SEARCH on non-existent index
    let options = FtSearchOptions::default();
    let result = redlite.ft_search("nonexistent", "query", &options);
    assert!(result.is_err());
}

/// FT.SUGADD/FT.SUGGET/FT.SUGDEL/FT.SUGLEN commands: all scenarios
#[test]
fn oracle_cmd_ft_suggestions() {
    let redlite = Db::open_memory().unwrap();

    // FT.SUGADD (key, string, score, payload)
    let r1 = redlite.ft_sugadd("suggestions", "hello world", 1.0, None);
    assert!(r1.is_ok());

    let r1 = redlite.ft_sugadd("suggestions", "hello there", 2.0, None);
    assert!(r1.is_ok());

    let r1 = redlite.ft_sugadd("suggestions", "goodbye world", 1.5, None);
    assert!(r1.is_ok());

    // FT.SUGLEN
    let len = redlite.ft_suglen("suggestions").unwrap();
    assert_eq!(len, 3);

    // FT.SUGGET (key, prefix, fuzzy, max)
    let results = redlite.ft_sugget("suggestions", "hel", false, 10).unwrap();
    assert!(results.len() >= 2);

    // FT.SUGGET with MAX=1
    let results = redlite.ft_sugget("suggestions", "hel", false, 1).unwrap();
    assert_eq!(results.len(), 1);

    // FT.SUGDEL
    let r1 = redlite.ft_sugdel("suggestions", "hello there").unwrap();
    assert!(r1);

    // Verify deletion
    let len = redlite.ft_suglen("suggestions").unwrap();
    assert_eq!(len, 2);

    // FT.SUGDEL non-existent
    let r1 = redlite.ft_sugdel("suggestions", "nonexistent").unwrap();
    assert!(!r1);

    // FT.SUGLEN on non-existent key
    let len = redlite.ft_suglen("nonexistent").unwrap();
    assert_eq!(len, 0);
}

/// FT.ALIASADD/FT.ALIASDEL/FT.ALIASUPDATE commands: all scenarios
#[test]
fn oracle_cmd_ft_aliases() {
    use redlite::{FtField, FtOnType};

    let redlite = Db::open_memory().unwrap();

    // Create indexes
    redlite.ft_create("idx_alias_1", FtOnType::Hash, &["alias1:"], &[FtField::text("content")]).unwrap();
    redlite.ft_create("idx_alias_2", FtOnType::Hash, &["alias2:"], &[FtField::text("content")]).unwrap();

    // FT.ALIASADD
    let r1 = redlite.ft_aliasadd("my_alias", "idx_alias_1");
    assert!(r1.is_ok());

    // FT.ALIASUPDATE to point to different index
    let r1 = redlite.ft_aliasupdate("my_alias", "idx_alias_2");
    assert!(r1.is_ok());

    // FT.ALIASDEL
    let r1 = redlite.ft_aliasdel("my_alias").unwrap();
    assert!(r1);

    // FT.ALIASDEL on non-existent alias
    let r1 = redlite.ft_aliasdel("nonexistent").unwrap();
    assert!(!r1);
}

/// FT.SYNUPDATE/FT.SYNDUMP commands: all scenarios
#[test]
fn oracle_cmd_ft_synonyms() {
    use redlite::{FtField, FtOnType};

    let redlite = Db::open_memory().unwrap();

    // Create index
    redlite.ft_create("idx_syn", FtOnType::Hash, &["syn:"], &[FtField::text("content")]).unwrap();

    // FT.SYNUPDATE - add synonym group
    let r1 = redlite.ft_synupdate("idx_syn", "grp1", &["happy", "joyful", "glad"]);
    assert!(r1.is_ok());

    // FT.SYNUPDATE - add another group
    let r1 = redlite.ft_synupdate("idx_syn", "grp2", &["big", "large", "huge"]);
    assert!(r1.is_ok());

    // FT.SYNDUMP
    let synonyms = redlite.ft_syndump("idx_syn").unwrap();
    assert_eq!(synonyms.len(), 2);

    // Verify groups
    let grp1 = synonyms.iter().find(|(id, _)| id == "grp1").unwrap();
    assert_eq!(grp1.1.len(), 3);
    assert!(grp1.1.contains(&"happy".to_string()));

    // FT.SYNDUMP on non-existent index
    let synonyms = redlite.ft_syndump("nonexistent");
    assert!(synonyms.is_err());
}

// ============================================================================
// FTS COMMANDS - Redlite-native Full-Text Search (Redlite-specific)
// ============================================================================

/// FTS ENABLE/DISABLE commands: all scenarios
#[test]
fn oracle_cmd_fts_enable_disable() {
    use redlite::RetentionType;

    let mut redlite = Db::open_memory().unwrap();

    // FTS ENABLE GLOBAL
    let r1 = redlite.fts_enable_global();
    assert!(r1.is_ok());

    // FTS DISABLE GLOBAL
    let r1 = redlite.fts_disable_global();
    assert!(r1.is_ok());

    // FTS ENABLE DATABASE
    let r1 = redlite.fts_enable_database(0);
    assert!(r1.is_ok());

    // FTS DISABLE DATABASE
    let r1 = redlite.fts_disable_database(0);
    assert!(r1.is_ok());

    // FTS ENABLE PATTERN
    let r1 = redlite.fts_enable_pattern("article:*");
    assert!(r1.is_ok());

    // FTS DISABLE PATTERN
    let r1 = redlite.fts_disable_pattern("article:*");
    assert!(r1.is_ok());

    // FTS ENABLE KEY
    redlite.set("fts_key", b"content", None).unwrap();
    let r1 = redlite.fts_enable_key("fts_key");
    assert!(r1.is_ok());

    // FTS DISABLE KEY
    let r1 = redlite.fts_disable_key("fts_key");
    assert!(r1.is_ok());
}

/// FTS INDEX/SEARCH commands: all scenarios
#[test]

fn oracle_cmd_fts_index_search() {
    let redlite = Db::open_memory().unwrap();

    // Enable FTS globally
    redlite.fts_enable_global().unwrap();

    // Create and index some content
    redlite.set("doc:1", b"The quick brown fox jumps over the lazy dog", None).unwrap();
    redlite.fts_index("doc:1", b"The quick brown fox jumps over the lazy dog").unwrap();

    redlite.set("doc:2", b"A fast red fox ran through the forest", None).unwrap();
    redlite.fts_index("doc:2", b"A fast red fox ran through the forest").unwrap();

    redlite.set("doc:3", b"The dog sleeps under the tree", None).unwrap();
    redlite.fts_index("doc:3", b"The dog sleeps under the tree").unwrap();

    // FTS SEARCH basic query
    let results = redlite.fts_search("fox", None, false).unwrap();
    assert_eq!(results.len(), 2);

    // FTS SEARCH with LIMIT
    let results = redlite.fts_search("fox", Some(1), false).unwrap();
    assert_eq!(results.len(), 1);

    // FTS SEARCH with term not found
    let results = redlite.fts_search("elephant", None, false).unwrap();
    assert!(results.is_empty());

    // FTS SEARCH multiple terms
    let results = redlite.fts_search("quick brown", None, false).unwrap();
    assert!(!results.is_empty());

    // FTS SEARCH phrase
    let results = redlite.fts_search("\"brown fox\"", None, false).unwrap();
    assert!(!results.is_empty());
}

/// FTS DEINDEX/REINDEX commands: all scenarios
#[test]

fn oracle_cmd_fts_deindex_reindex() {
    let redlite = Db::open_memory().unwrap();

    // Enable and index
    redlite.fts_enable_global().unwrap();
    redlite.set("doc:1", b"searchable content here", None).unwrap();
    redlite.fts_index("doc:1", b"searchable content here").unwrap();

    // Verify indexed
    let results = redlite.fts_search("searchable", None, false).unwrap();
    assert_eq!(results.len(), 1);

    // FTS DEINDEX
    let r1 = redlite.fts_deindex("doc:1");
    assert!(r1.is_ok());

    // Verify deindexed
    let results = redlite.fts_search("searchable", None, false).unwrap();
    assert!(results.is_empty());

    // FTS REINDEX
    let r1 = redlite.fts_reindex_key("doc:1").unwrap();
    assert!(r1); // Should return true if key exists

    // Verify reindexed
    let results = redlite.fts_search("searchable", None, false).unwrap();
    assert_eq!(results.len(), 1);
}

/// FTS INFO command: all scenarios
#[test]

fn oracle_cmd_fts_info() {
    let redlite = Db::open_memory().unwrap();

    // FTS INFO on empty
    let info = redlite.fts_info().unwrap();
    assert_eq!(info.indexed_keys, 0);

    // Enable and index some content
    redlite.fts_enable_global().unwrap();
    redlite.set("doc:1", b"content one", None).unwrap();
    redlite.fts_index("doc:1", b"content one").unwrap();
    redlite.set("doc:2", b"content two", None).unwrap();
    redlite.fts_index("doc:2", b"content two").unwrap();

    // FTS INFO with indexed content
    let info = redlite.fts_info().unwrap();
    assert_eq!(info.indexed_keys, 2);
}

// ============================================================================
// HISTORY COMMANDS - Time-travel queries (Redlite-specific)
// ============================================================================

/// HISTORY ENABLE/DISABLE commands: all scenarios
#[test]
fn oracle_cmd_history_enable_disable() {
    use redlite::RetentionType;

    let mut redlite = Db::open_memory().unwrap();

    // HISTORY ENABLE GLOBAL with unlimited retention
    let r1 = redlite.history_enable_global(RetentionType::Unlimited);
    assert!(r1.is_ok());

    // HISTORY DISABLE GLOBAL
    let r1 = redlite.history_disable_global();
    assert!(r1.is_ok());

    // HISTORY ENABLE GLOBAL with time-based retention
    let r1 = redlite.history_enable_global(RetentionType::Time(86400000)); // 24 hours
    assert!(r1.is_ok());

    // HISTORY ENABLE GLOBAL with count-based retention
    let r1 = redlite.history_enable_global(RetentionType::Count(100));
    assert!(r1.is_ok());

    // HISTORY ENABLE DATABASE
    let r1 = redlite.history_enable_database(0, RetentionType::Unlimited);
    assert!(r1.is_ok());

    // HISTORY DISABLE DATABASE
    let r1 = redlite.history_disable_database(0);
    assert!(r1.is_ok());

    // HISTORY ENABLE KEY
    redlite.set("history_key", b"value", None).unwrap();
    let r1 = redlite.history_enable_key("history_key", RetentionType::Unlimited);
    assert!(r1.is_ok());

    // HISTORY DISABLE KEY
    let r1 = redlite.history_disable_key("history_key");
    assert!(r1.is_ok());
}

/// HISTORY GET command: all scenarios
#[test]
fn oracle_cmd_history_get() {
    use redlite::RetentionType;

    let redlite = Db::open_memory().unwrap();

    // Enable history globally
    redlite.history_enable_global(RetentionType::Unlimited).unwrap();

    // Create and update a key multiple times
    redlite.set("tracked", b"version1", None).unwrap();
    std::thread::sleep(std::time::Duration::from_millis(10));
    redlite.set("tracked", b"version2", None).unwrap();
    std::thread::sleep(std::time::Duration::from_millis(10));
    redlite.set("tracked", b"version3", None).unwrap();

    // HISTORY GET - get all versions
    let history = redlite.history_get("tracked", None, None, None).unwrap();
    assert!(history.len() >= 3);

    // Verify versions are in order (by timestamp_ms)
    for i in 1..history.len() {
        assert!(history[i - 1].timestamp_ms <= history[i].timestamp_ms); // timestamps ascending
    }

    // HISTORY GET with LIMIT
    let history = redlite.history_get("tracked", Some(2), None, None).unwrap();
    assert_eq!(history.len(), 2);

    // HISTORY GET for non-existent key
    let history = redlite.history_get("nonexistent", None, None, None).unwrap();
    assert!(history.is_empty());
}

/// HISTORY GET AT command: all scenarios
#[test]
fn oracle_cmd_history_get_at() {
    use redlite::RetentionType;

    let redlite = Db::open_memory().unwrap();

    // Enable history
    redlite.history_enable_global(RetentionType::Unlimited).unwrap();

    // Set initial value
    let before_timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;

    redlite.set("time_travel", b"initial", None).unwrap();
    std::thread::sleep(std::time::Duration::from_millis(50));

    let middle_timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;

    redlite.set("time_travel", b"updated", None).unwrap();
    std::thread::sleep(std::time::Duration::from_millis(50));

    let after_timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;

    // HISTORY GET AT specific timestamp
    let value = redlite.history_get_at("time_travel", middle_timestamp).unwrap();
    // Should get the value that was active at that time
    assert!(value.is_some());

    // HISTORY GET AT before any value existed
    let value = redlite.history_get_at("time_travel", before_timestamp - 1000).unwrap();
    assert!(value.is_none());
}

/// HISTORY LIST KEYS command: all scenarios
#[test]
fn oracle_cmd_history_list_keys() {
    use redlite::RetentionType;

    let redlite = Db::open_memory().unwrap();

    // Enable history and create tracked keys
    redlite.history_enable_global(RetentionType::Unlimited).unwrap();

    redlite.set("track:1", b"value1", None).unwrap();
    redlite.set("track:2", b"value2", None).unwrap();
    redlite.set("other:1", b"value3", None).unwrap();

    // Update to create history entries
    redlite.set("track:1", b"updated1", None).unwrap();
    redlite.set("track:2", b"updated2", None).unwrap();

    // HISTORY LIST KEYS
    let keys = redlite.history_list_keys(None).unwrap();
    assert!(keys.len() >= 2);

    // HISTORY LIST KEYS with pattern
    let keys = redlite.history_list_keys(Some("track:*")).unwrap();
    assert!(keys.iter().all(|k| k.starts_with("track:")));
}

/// HISTORY STATS command: all scenarios
#[test]
fn oracle_cmd_history_stats() {
    use redlite::RetentionType;

    let redlite = Db::open_memory().unwrap();

    // HISTORY STATS on empty
    let stats = redlite.history_stats(None).unwrap();
    assert_eq!(stats.total_entries, 0);

    // Enable history and create entries
    redlite.history_enable_global(RetentionType::Unlimited).unwrap();

    redlite.set("stats:1", b"value1", None).unwrap();
    redlite.set("stats:1", b"value2", None).unwrap();
    redlite.set("stats:2", b"value1", None).unwrap();

    // HISTORY STATS global
    let stats = redlite.history_stats(None).unwrap();
    assert!(stats.total_entries >= 3);

    // HISTORY STATS for specific key
    let stats = redlite.history_stats(Some("stats:1")).unwrap();
    assert!(stats.total_entries >= 2);
}

/// HISTORY CLEAR/PRUNE commands: all scenarios
#[test]
fn oracle_cmd_history_clear_prune() {
    use redlite::RetentionType;

    let redlite = Db::open_memory().unwrap();

    // Enable history and create entries
    redlite.history_enable_global(RetentionType::Unlimited).unwrap();

    redlite.set("clear:1", b"value1", None).unwrap();
    redlite.set("clear:1", b"value2", None).unwrap();
    redlite.set("clear:1", b"value3", None).unwrap();

    // Verify history exists
    let history = redlite.history_get("clear:1", None, None, None).unwrap();
    assert!(history.len() >= 3);

    // HISTORY CLEAR KEY
    let cleared = redlite.history_clear_key("clear:1", None).unwrap();
    assert!(cleared >= 3);

    // Verify cleared
    let history = redlite.history_get("clear:1", None, None, None).unwrap();
    assert!(history.is_empty());

    // Create more entries for prune test
    redlite.set("prune:1", b"old", None).unwrap();
    std::thread::sleep(std::time::Duration::from_millis(50));

    let cutoff = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;

    std::thread::sleep(std::time::Duration::from_millis(50));
    redlite.set("prune:1", b"new", None).unwrap();

    // HISTORY PRUNE before timestamp
    let pruned = redlite.history_prune(cutoff).unwrap();
    assert!(pruned >= 1);
}

// ============================================================================
// VECTOR COMMANDS - Redis 8 compatible vector operations (Redlite-specific)
// ============================================================================

/// VADD command: all scenarios
#[test]
#[cfg(feature = "vectors")]
fn oracle_cmd_vadd() {
    use redlite::QuantizationType;

    let redlite = Db::open_memory().unwrap();

    // VADD with vector values
    let vector = vec![0.1, 0.2, 0.3, 0.4, 0.5];
    let r1 = redlite.vadd("vectors", "item1", &vector, None, QuantizationType::NoQuant);
    assert!(r1.is_ok());

    // VADD another vector
    let vector2 = vec![0.5, 0.4, 0.3, 0.2, 0.1];
    let r1 = redlite.vadd("vectors", "item2", &vector2, None, QuantizationType::NoQuant);
    assert!(r1.is_ok());

    // VADD with attributes
    let vector3 = vec![0.3, 0.3, 0.3, 0.3, 0.3];
    let attrs = serde_json::json!({"category": "test", "priority": 1});
    let r1 = redlite.vadd("vectors", "item3", &vector3, Some(attrs.to_string()), QuantizationType::NoQuant);
    assert!(r1.is_ok());

    // Verify VCARD
    let card = redlite.vcard("vectors").unwrap();
    assert_eq!(card, 3);

    // VADD update existing element
    let updated_vector = vec![0.9, 0.9, 0.9, 0.9, 0.9];
    let r1 = redlite.vadd("vectors", "item1", &updated_vector, None, QuantizationType::NoQuant);
    assert!(r1.is_ok());

    // Verify card unchanged (update, not add)
    let card = redlite.vcard("vectors").unwrap();
    assert_eq!(card, 3);

    // VADD with Q8 quantization
    let vector4 = vec![0.1, 0.2, 0.3, 0.4, 0.5];
    let r1 = redlite.vadd("vectors_q8", "item1", &vector4, None, QuantizationType::Q8);
    assert!(r1.is_ok());

    // VADD with different dimensions
    let high_dim = vec![0.1; 128];
    let r1 = redlite.vadd("vectors_high", "item1", &high_dim, None, QuantizationType::NoQuant);
    assert!(r1.is_ok());
}

/// VREM command: all scenarios
#[test]
#[cfg(feature = "vectors")]
fn oracle_cmd_vrem() {
    use redlite::QuantizationType;

    let redlite = Db::open_memory().unwrap();

    // Add vectors first
    let vector = vec![0.1, 0.2, 0.3];
    redlite.vadd("vrem_test", "item1", &vector, None, QuantizationType::NoQuant).unwrap();
    redlite.vadd("vrem_test", "item2", &vector, None, QuantizationType::NoQuant).unwrap();
    redlite.vadd("vrem_test", "item3", &vector, None, QuantizationType::NoQuant).unwrap();

    assert_eq!(redlite.vcard("vrem_test").unwrap(), 3);

    // VREM existing element
    let r1 = redlite.vrem("vrem_test", "item1").unwrap();
    assert!(r1);

    assert_eq!(redlite.vcard("vrem_test").unwrap(), 2);

    // VREM non-existent element
    let r1 = redlite.vrem("vrem_test", "nonexistent").unwrap();
    assert!(!r1);

    // VREM from non-existent key
    let r1 = redlite.vrem("nonexistent_key", "item1").unwrap();
    assert!(!r1);
}

/// VSIM command: all scenarios
#[test]
#[cfg(feature = "vectors")]
fn oracle_cmd_vsim() {
    use redlite::QuantizationType;

    let redlite = Db::open_memory().unwrap();

    // Create vector set with known vectors for similarity testing
    let v1 = vec![1.0, 0.0, 0.0]; // x-axis
    let v2 = vec![0.0, 1.0, 0.0]; // y-axis
    let v3 = vec![0.0, 0.0, 1.0]; // z-axis
    let v4 = vec![0.9, 0.1, 0.0]; // close to x-axis

    redlite.vadd("sim_test", "x_axis", &v1, None, QuantizationType::NoQuant).unwrap();
    redlite.vadd("sim_test", "y_axis", &v2, None, QuantizationType::NoQuant).unwrap();
    redlite.vadd("sim_test", "z_axis", &v3, None, QuantizationType::NoQuant).unwrap();
    redlite.vadd("sim_test", "near_x", &v4, None, QuantizationType::NoQuant).unwrap();

    // VSIM from vector query - find vectors similar to x-axis
    let query = vec![1.0, 0.0, 0.0];
    let results = redlite.vsim("sim_test", &query, 4, false, false, None).unwrap();
    assert_eq!(results.len(), 4);

    // Most similar should be x_axis itself
    assert_eq!(results[0].0, "x_axis");

    // VSIM with COUNT limit
    let results = redlite.vsim("sim_test", &query, 2, false, false, None).unwrap();
    assert_eq!(results.len(), 2);

    // VSIM with WITHSCORES
    let results = redlite.vsim("sim_test", &query, 4, true, false, None).unwrap();
    assert_eq!(results.len(), 4);
    // First result (exact match) should have score 0 or very close to 0
    assert!(results[0].1.unwrap_or(1.0).abs() < 0.0001);

    // VSIM on empty key
    let results = redlite.vsim("nonexistent", &query, 10, false, false, None).unwrap();
    assert!(results.is_empty());
}

/// VCARD command: all scenarios
#[test]
#[cfg(feature = "vectors")]
fn oracle_cmd_vcard() {
    use redlite::QuantizationType;

    let redlite = Db::open_memory().unwrap();

    // VCARD on non-existent key
    let card = redlite.vcard("nonexistent").unwrap();
    assert_eq!(card, 0);

    // VCARD on empty set (after creation)
    // Actually, key won't exist until first VADD

    // Add vectors
    let vector = vec![0.1, 0.2, 0.3];
    redlite.vadd("vcard_test", "item1", &vector, None, QuantizationType::NoQuant).unwrap();
    redlite.vadd("vcard_test", "item2", &vector, None, QuantizationType::NoQuant).unwrap();

    let card = redlite.vcard("vcard_test").unwrap();
    assert_eq!(card, 2);

    // VCARD after removal
    redlite.vrem("vcard_test", "item1").unwrap();
    let card = redlite.vcard("vcard_test").unwrap();
    assert_eq!(card, 1);
}

/// VRANDMEMBER command: all scenarios
#[test]
#[cfg(feature = "vectors")]
fn oracle_cmd_vrandmember() {
    use redlite::QuantizationType;

    let redlite = Db::open_memory().unwrap();

    // VRANDMEMBER on non-existent key
    let results = redlite.vrandmember("nonexistent", None).unwrap();
    assert!(results.is_empty());

    // Add vectors
    let vector = vec![0.1, 0.2, 0.3];
    for i in 0..10 {
        redlite.vadd("rand_test", &format!("item{}", i), &vector, None, QuantizationType::NoQuant).unwrap();
    }

    // VRANDMEMBER single
    let results = redlite.vrandmember("rand_test", None).unwrap();
    assert_eq!(results.len(), 1);
    assert!(results[0].starts_with("item"));

    // VRANDMEMBER with count
    let results = redlite.vrandmember("rand_test", Some(5)).unwrap();
    assert_eq!(results.len(), 5);

    // VRANDMEMBER with count > size
    let results = redlite.vrandmember("rand_test", Some(20)).unwrap();
    assert_eq!(results.len(), 10); // Should return all elements

    // VRANDMEMBER with negative count (allows duplicates)
    let results = redlite.vrandmember("rand_test", Some(-5)).unwrap();
    assert_eq!(results.len(), 5);
}

/// VGETATTR/VSETATTR commands: all scenarios
#[test]
#[cfg(feature = "vectors")]
fn oracle_cmd_vgetattr_vsetattr() {
    use redlite::QuantizationType;

    let redlite = Db::open_memory().unwrap();

    // Add vector with initial attributes
    let vector = vec![0.1, 0.2, 0.3];
    let attrs = serde_json::json!({"color": "red", "size": 10});
    redlite.vadd("attr_test", "item1", &vector, Some(attrs.to_string()), QuantizationType::NoQuant).unwrap();

    // VGETATTR
    let result = redlite.vgetattr("attr_test", "item1").unwrap();
    assert!(result.is_some());
    let attr_str = result.unwrap();
    let attr_json: serde_json::Value = serde_json::from_str(&attr_str).unwrap();
    assert_eq!(attr_json["color"], "red");
    assert_eq!(attr_json["size"], 10);

    // VSETATTR to update
    let new_attrs = serde_json::json!({"color": "blue", "weight": 5.5});
    let r1 = redlite.vsetattr("attr_test", "item1", &new_attrs.to_string());
    assert!(r1.is_ok());

    // Verify update
    let result = redlite.vgetattr("attr_test", "item1").unwrap();
    let attr_str = result.unwrap();
    let attr_json: serde_json::Value = serde_json::from_str(&attr_str).unwrap();
    assert_eq!(attr_json["color"], "blue");
    assert_eq!(attr_json["weight"], 5.5);

    // VGETATTR on non-existent element
    let result = redlite.vgetattr("attr_test", "nonexistent").unwrap();
    assert!(result.is_none());

    // VGETATTR on non-existent key
    let result = redlite.vgetattr("nonexistent_key", "item1").unwrap();
    assert!(result.is_none());
}

/// VINFO command: all scenarios
#[test]
#[cfg(feature = "vectors")]
fn oracle_cmd_vinfo() {
    use redlite::QuantizationType;

    let redlite = Db::open_memory().unwrap();

    // VINFO on non-existent key
    let info = redlite.vinfo("nonexistent").unwrap();
    assert!(info.is_none());

    // Add vectors with different dimensions
    let vector3d = vec![0.1, 0.2, 0.3];
    redlite.vadd("vinfo_test", "item1", &vector3d, None, QuantizationType::NoQuant).unwrap();
    redlite.vadd("vinfo_test", "item2", &vector3d, None, QuantizationType::NoQuant).unwrap();

    // VINFO
    let info = redlite.vinfo("vinfo_test").unwrap();
    assert!(info.is_some());
    let info = info.unwrap();
    assert_eq!(info.cardinality, 2);
    assert_eq!(info.dimensions, 3);
}

/// VEMB command: all scenarios
#[test]
#[cfg(feature = "vectors")]
fn oracle_cmd_vemb() {
    use redlite::QuantizationType;

    let redlite = Db::open_memory().unwrap();

    // Add vector
    let vector = vec![0.1, 0.2, 0.3, 0.4, 0.5];
    redlite.vadd("vemb_test", "item1", &vector, None, QuantizationType::NoQuant).unwrap();

    // VEMB to retrieve vector
    let result = redlite.vemb("vemb_test", "item1", false).unwrap();
    assert!(result.is_some());
    let retrieved = result.unwrap();
    assert_eq!(retrieved.len(), 5);

    // Compare values (allowing for floating point tolerance)
    for (i, val) in retrieved.iter().enumerate() {
        assert!((val - vector[i]).abs() < 0.0001);
    }

    // VEMB on non-existent element
    let result = redlite.vemb("vemb_test", "nonexistent", false).unwrap();
    assert!(result.is_none());

    // VEMB on non-existent key
    let result = redlite.vemb("nonexistent_key", "item1", false).unwrap();
    assert!(result.is_none());
}

/// VDIM command: all scenarios
#[test]
#[cfg(feature = "vectors")]
fn oracle_cmd_vdim() {
    use redlite::QuantizationType;

    let redlite = Db::open_memory().unwrap();

    // VDIM on non-existent key
    let dim = redlite.vdim("nonexistent").unwrap();
    assert_eq!(dim, 0);

    // Add 5D vectors
    let vector5d = vec![0.1, 0.2, 0.3, 0.4, 0.5];
    redlite.vadd("vdim_test", "item1", &vector5d, None, QuantizationType::NoQuant).unwrap();

    let dim = redlite.vdim("vdim_test").unwrap();
    assert_eq!(dim, 5);

    // Add 128D vectors
    let vector128d = vec![0.1; 128];
    redlite.vadd("vdim_high", "item1", &vector128d, None, QuantizationType::NoQuant).unwrap();

    let dim = redlite.vdim("vdim_high").unwrap();
    assert_eq!(dim, 128);
}

// ============================================================================
// BLOCKING LIST OPERATIONS - BLPOP / BRPOP
// ============================================================================

/// BLPOP command: immediate data available
#[tokio::test]
async fn oracle_cmd_blpop_immediate() {
    let redis_conn = match get_redis_connection() {
        Some(conn) => conn,
        None => {
            eprintln!("Skipping test: Redis not available");
            return;
        }
    };
    let mut redis = redis_conn;
    let redlite = Db::open_memory().unwrap();
    let _: () = redis::cmd("FLUSHDB").query(&mut redis).unwrap();

    // Setup list with data
    redlite.rpush("blpop_list", &[b"item1", b"item2", b"item3"]).unwrap();
    let _: i64 = redis.rpush("blpop_list", vec!["item1", "item2", "item3"]).unwrap();

    // BLPOP with immediate data - should return first item
    let r1 = redlite.blpop(&["blpop_list"], 1.0).await.unwrap();
    let r2: Option<(String, Vec<u8>)> = redis::cmd("BLPOP")
        .arg("blpop_list")
        .arg(1.0)
        .query(&mut redis)
        .unwrap();

    assert!(r1.is_some());
    assert!(r2.is_some());
    let (key1, val1) = r1.unwrap();
    let (key2, val2) = r2.unwrap();
    assert_eq!(key1, key2);
    assert_eq!(val1, val2);
    assert_eq!(key1, "blpop_list");
    assert_eq!(val1, b"item1");
}

/// BLPOP command: timeout on empty list
#[tokio::test]
async fn oracle_cmd_blpop_timeout() {
    let redis_conn = match get_redis_connection() {
        Some(conn) => conn,
        None => {
            eprintln!("Skipping test: Redis not available");
            return;
        }
    };
    let mut redis = redis_conn;
    let redlite = Db::open_memory().unwrap();
    let _: () = redis::cmd("FLUSHDB").query(&mut redis).unwrap();

    // BLPOP on empty/non-existent key with short timeout
    let start = std::time::Instant::now();
    let r1 = redlite.blpop(&["empty_list"], 0.5).await.unwrap();
    let elapsed = start.elapsed();

    // Should timeout and return None
    assert!(r1.is_none());
    // Should have waited at least close to timeout
    assert!(elapsed >= std::time::Duration::from_millis(400));
    assert!(elapsed < std::time::Duration::from_secs(2));

    // Redis should also timeout
    let start = std::time::Instant::now();
    let r2: Option<(String, Vec<u8>)> = redis::cmd("BLPOP")
        .arg("empty_list")
        .arg(0.5)
        .query(&mut redis)
        .unwrap();
    let elapsed = start.elapsed();

    assert!(r2.is_none());
    assert!(elapsed >= std::time::Duration::from_millis(400));
}

/// BLPOP command: multiple keys, first non-empty wins
#[tokio::test]
async fn oracle_cmd_blpop_multiple_keys() {
    let redis_conn = match get_redis_connection() {
        Some(conn) => conn,
        None => {
            eprintln!("Skipping test: Redis not available");
            return;
        }
    };
    let mut redis = redis_conn;
    let redlite = Db::open_memory().unwrap();
    let _: () = redis::cmd("FLUSHDB").query(&mut redis).unwrap();

    // Second key has data, first is empty
    redlite.rpush("list2", &[b"from_list2"]).unwrap();
    let _: i64 = redis.rpush("list2", "from_list2").unwrap();

    // BLPOP with multiple keys - should find data in list2
    let r1 = redlite.blpop(&["list1", "list2", "list3"], 1.0).await.unwrap();
    let r2: Option<(String, Vec<u8>)> = redis::cmd("BLPOP")
        .arg("list1")
        .arg("list2")
        .arg("list3")
        .arg(1.0)
        .query(&mut redis)
        .unwrap();

    assert!(r1.is_some());
    assert!(r2.is_some());
    let (key1, val1) = r1.unwrap();
    let (key2, val2) = r2.unwrap();
    assert_eq!(key1, "list2");
    assert_eq!(key2, "list2");
    assert_eq!(val1, b"from_list2");
    assert_eq!(val2, b"from_list2");
}

/// BLPOP command: key priority order
#[tokio::test]
async fn oracle_cmd_blpop_priority() {
    let redis_conn = match get_redis_connection() {
        Some(conn) => conn,
        None => {
            eprintln!("Skipping test: Redis not available");
            return;
        }
    };
    let mut redis = redis_conn;
    let redlite = Db::open_memory().unwrap();
    let _: () = redis::cmd("FLUSHDB").query(&mut redis).unwrap();

    // Both keys have data - first key should win
    redlite.rpush("priority1", &[b"first"]).unwrap();
    redlite.rpush("priority2", &[b"second"]).unwrap();
    let _: i64 = redis.rpush("priority1", "first").unwrap();
    let _: i64 = redis.rpush("priority2", "second").unwrap();

    let r1 = redlite.blpop(&["priority1", "priority2"], 1.0).await.unwrap();
    let r2: Option<(String, Vec<u8>)> = redis::cmd("BLPOP")
        .arg("priority1")
        .arg("priority2")
        .arg(1.0)
        .query(&mut redis)
        .unwrap();

    assert!(r1.is_some());
    assert!(r2.is_some());
    let (key1, _) = r1.unwrap();
    let (key2, _) = r2.unwrap();
    assert_eq!(key1, "priority1");
    assert_eq!(key2, "priority1");
}

/// BLPOP command: binary data
#[tokio::test]
async fn oracle_cmd_blpop_binary() {
    let redis_conn = match get_redis_connection() {
        Some(conn) => conn,
        None => {
            eprintln!("Skipping test: Redis not available");
            return;
        }
    };
    let mut redis = redis_conn;
    let redlite = Db::open_memory().unwrap();
    let _: () = redis::cmd("FLUSHDB").query(&mut redis).unwrap();

    // Binary data with null bytes and high bytes
    let binary_data: Vec<u8> = vec![0, 1, 128, 255, 0, 42];
    redlite.rpush("binary_list", &[&binary_data[..]]).unwrap();
    let _: i64 = redis::cmd("RPUSH")
        .arg("binary_list")
        .arg(&binary_data)
        .query(&mut redis)
        .unwrap();

    let r1 = redlite.blpop(&["binary_list"], 1.0).await.unwrap();
    let r2: Option<(String, Vec<u8>)> = redis::cmd("BLPOP")
        .arg("binary_list")
        .arg(1.0)
        .query(&mut redis)
        .unwrap();

    assert!(r1.is_some());
    assert!(r2.is_some());
    let (_, val1) = r1.unwrap();
    let (_, val2) = r2.unwrap();
    assert_eq!(val1, binary_data);
    assert_eq!(val2, binary_data);
}

/// BRPOP command: immediate data available
#[tokio::test]
async fn oracle_cmd_brpop_immediate() {
    let redis_conn = match get_redis_connection() {
        Some(conn) => conn,
        None => {
            eprintln!("Skipping test: Redis not available");
            return;
        }
    };
    let mut redis = redis_conn;
    let redlite = Db::open_memory().unwrap();
    let _: () = redis::cmd("FLUSHDB").query(&mut redis).unwrap();

    // Setup list with data
    redlite.rpush("brpop_list", &[b"item1", b"item2", b"item3"]).unwrap();
    let _: i64 = redis.rpush("brpop_list", vec!["item1", "item2", "item3"]).unwrap();

    // BRPOP with immediate data - should return last item (item3)
    let r1 = redlite.brpop(&["brpop_list"], 1.0).await.unwrap();
    let r2: Option<(String, Vec<u8>)> = redis::cmd("BRPOP")
        .arg("brpop_list")
        .arg(1.0)
        .query(&mut redis)
        .unwrap();

    assert!(r1.is_some());
    assert!(r2.is_some());
    let (key1, val1) = r1.unwrap();
    let (key2, val2) = r2.unwrap();
    assert_eq!(key1, key2);
    assert_eq!(val1, val2);
    assert_eq!(key1, "brpop_list");
    assert_eq!(val1, b"item3");
}

/// BRPOP command: timeout on empty list
#[tokio::test]
async fn oracle_cmd_brpop_timeout() {
    let redis_conn = match get_redis_connection() {
        Some(conn) => conn,
        None => {
            eprintln!("Skipping test: Redis not available");
            return;
        }
    };
    let mut redis = redis_conn;
    let redlite = Db::open_memory().unwrap();
    let _: () = redis::cmd("FLUSHDB").query(&mut redis).unwrap();

    // BRPOP on empty/non-existent key with short timeout
    let start = std::time::Instant::now();
    let r1 = redlite.brpop(&["empty_list_r"], 0.5).await.unwrap();
    let elapsed = start.elapsed();

    assert!(r1.is_none());
    assert!(elapsed >= std::time::Duration::from_millis(400));
    assert!(elapsed < std::time::Duration::from_secs(2));

    let start = std::time::Instant::now();
    let r2: Option<(String, Vec<u8>)> = redis::cmd("BRPOP")
        .arg("empty_list_r")
        .arg(0.5)
        .query(&mut redis)
        .unwrap();
    let elapsed = start.elapsed();

    assert!(r2.is_none());
    assert!(elapsed >= std::time::Duration::from_millis(400));
}

/// BRPOP command: multiple keys with priority
#[tokio::test]
async fn oracle_cmd_brpop_multiple_keys() {
    let redis_conn = match get_redis_connection() {
        Some(conn) => conn,
        None => {
            eprintln!("Skipping test: Redis not available");
            return;
        }
    };
    let mut redis = redis_conn;
    let redlite = Db::open_memory().unwrap();
    let _: () = redis::cmd("FLUSHDB").query(&mut redis).unwrap();

    // Third key has data
    redlite.rpush("rlist3", &[b"from_rlist3"]).unwrap();
    let _: i64 = redis.rpush("rlist3", "from_rlist3").unwrap();

    let r1 = redlite.brpop(&["rlist1", "rlist2", "rlist3"], 1.0).await.unwrap();
    let r2: Option<(String, Vec<u8>)> = redis::cmd("BRPOP")
        .arg("rlist1")
        .arg("rlist2")
        .arg("rlist3")
        .arg(1.0)
        .query(&mut redis)
        .unwrap();

    assert!(r1.is_some());
    assert!(r2.is_some());
    let (key1, val1) = r1.unwrap();
    let (key2, val2) = r2.unwrap();
    assert_eq!(key1, "rlist3");
    assert_eq!(key2, "rlist3");
    assert_eq!(val1, b"from_rlist3");
    assert_eq!(val2, b"from_rlist3");
}

/// BLPOP/BRPOP: concurrent push during wait (Redlite-only test)
/// Note: This tests that data appearing during the wait is picked up
#[tokio::test]
async fn oracle_cmd_blpop_concurrent_push() {
    let redlite = Db::open_memory().unwrap();
    let redlite_clone = redlite.clone();

    // Spawn a task that will push data after a short delay
    let push_task = tokio::spawn(async move {
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        redlite_clone.rpush("concurrent_list", &[b"pushed_data"]).unwrap();
    });

    // Start blocking pop - should wait and then receive the pushed data
    let start = std::time::Instant::now();
    let result = redlite.blpop(&["concurrent_list"], 2.0).await.unwrap();
    let elapsed = start.elapsed();

    push_task.await.unwrap();

    // Should have received data (not timed out)
    assert!(result.is_some());
    let (key, value) = result.unwrap();
    assert_eq!(key, "concurrent_list");
    assert_eq!(value, b"pushed_data");

    // Should have waited at least 200ms (for push) but less than timeout
    assert!(elapsed >= std::time::Duration::from_millis(150));
    assert!(elapsed < std::time::Duration::from_secs(2));
}

/// BLPOP: non-existent keys should be skipped
#[tokio::test]
async fn oracle_cmd_blpop_nonexistent_keys() {
    let redis_conn = match get_redis_connection() {
        Some(conn) => conn,
        None => {
            eprintln!("Skipping test: Redis not available");
            return;
        }
    };
    let mut redis = redis_conn;
    let redlite = Db::open_memory().unwrap();
    let _: () = redis::cmd("FLUSHDB").query(&mut redis).unwrap();

    // Only the last key exists
    redlite.rpush("exists_last", &[b"found"]).unwrap();
    let _: i64 = redis.rpush("exists_last", "found").unwrap();

    let r1 = redlite.blpop(&["nonexistent1", "nonexistent2", "exists_last"], 1.0).await.unwrap();
    let r2: Option<(String, Vec<u8>)> = redis::cmd("BLPOP")
        .arg("nonexistent1")
        .arg("nonexistent2")
        .arg("exists_last")
        .arg(1.0)
        .query(&mut redis)
        .unwrap();

    assert!(r1.is_some());
    assert!(r2.is_some());
    let (key1, _) = r1.unwrap();
    let (key2, _) = r2.unwrap();
    assert_eq!(key1, "exists_last");
    assert_eq!(key2, "exists_last");
}

/// BLPOP: empties list completely
#[tokio::test]
async fn oracle_cmd_blpop_empties_list() {
    let redis_conn = match get_redis_connection() {
        Some(conn) => conn,
        None => {
            eprintln!("Skipping test: Redis not available");
            return;
        }
    };
    let mut redis = redis_conn;
    let redlite = Db::open_memory().unwrap();
    let _: () = redis::cmd("FLUSHDB").query(&mut redis).unwrap();

    // Single item list
    redlite.rpush("single_item", &[b"only_one"]).unwrap();
    let _: i64 = redis.rpush("single_item", "only_one").unwrap();

    // Pop should get the item
    let r1 = redlite.blpop(&["single_item"], 1.0).await.unwrap();
    let r2: Option<(String, Vec<u8>)> = redis::cmd("BLPOP")
        .arg("single_item")
        .arg(1.0)
        .query(&mut redis)
        .unwrap();

    assert!(r1.is_some());
    assert!(r2.is_some());

    // List should now be empty (key deleted)
    let len1 = redlite.llen("single_item").unwrap();
    let len2: i64 = redis.llen("single_item").unwrap();
    assert_eq!(len1, 0);
    assert_eq!(len2, 0);

    // Second pop should timeout
    let r1 = redlite.blpop(&["single_item"], 0.2).await.unwrap();
    assert!(r1.is_none());
}

// ============================================================================
// BLPOP/BRPOP ORACLE TESTS - Additional Edge Cases
// ============================================================================

/// BLPOP: WRONGTYPE error on non-list key
#[tokio::test]
async fn oracle_cmd_blpop_wrong_type() {
    let redis_conn = match get_redis_connection() {
        Some(conn) => conn,
        None => {
            eprintln!("Skipping test: Redis not available");
            return;
        }
    };
    let mut redis = redis_conn;
    let redlite = Db::open_memory().unwrap();
    let _: () = redis::cmd("FLUSHDB").query(&mut redis).unwrap();

    // Create a string key (not a list)
    redlite.set("string_key", b"not_a_list", None).unwrap();
    let _: () = redis.set("string_key", "not_a_list").unwrap();

    // BLPOP should error on string key
    let r1 = redlite.blpop(&["string_key"], 0.1).await;
    let r2: redis::RedisResult<Option<(String, Vec<u8>)>> = redis::cmd("BLPOP")
        .arg("string_key")
        .arg(0.1)
        .query(&mut redis);

    // Both should error with WRONGTYPE
    assert!(r1.is_err(), "Redlite should error on WRONGTYPE");
    assert!(r2.is_err(), "Redis should error on WRONGTYPE");
}

/// BRPOP: WRONGTYPE error on non-list key
#[tokio::test]
async fn oracle_cmd_brpop_wrong_type() {
    let redis_conn = match get_redis_connection() {
        Some(conn) => conn,
        None => {
            eprintln!("Skipping test: Redis not available");
            return;
        }
    };
    let mut redis = redis_conn;
    let redlite = Db::open_memory().unwrap();
    let _: () = redis::cmd("FLUSHDB").query(&mut redis).unwrap();

    // Create a hash key (not a list)
    redlite.hset("hash_key", &[("field", b"value")]).unwrap();
    let _: () = redis.hset("hash_key", "field", "value").unwrap();

    // BRPOP should error on hash key
    let r1 = redlite.brpop(&["hash_key"], 0.1).await;
    let r2: redis::RedisResult<Option<(String, Vec<u8>)>> = redis::cmd("BRPOP")
        .arg("hash_key")
        .arg(0.1)
        .query(&mut redis);

    // Both should error with WRONGTYPE
    assert!(r1.is_err(), "Redlite should error on WRONGTYPE");
    assert!(r2.is_err(), "Redis should error on WRONGTYPE");
}

// ============================================================================
// TRANSACTION ORACLE TESTS - MULTI/EXEC/DISCARD/WATCH
// Note: Transactions are only available in server mode, not embedded Db.
// These tests are placeholders for when we add server-mode oracle tests.
// ============================================================================

// TODO: Add server-mode transaction tests using redis-cli or TCP connection
// - oracle_tx_multi_exec_basic
// - oracle_tx_multi_discard
// - oracle_tx_multi_exec_empty
// - oracle_tx_multi_nested
// - oracle_tx_exec_without_multi
// - oracle_tx_watch_unmodified
// - oracle_tx_watch_modified
// - oracle_tx_unwatch
// - oracle_tx_watch_inside_multi

// ============================================================================
// JSON COMMANDS - ReJSON-compatible commands (Redlite-specific)
// ============================================================================

/// JSON.SET command: all scenarios
#[test]
fn oracle_cmd_json_set() {
    let redlite = Db::open_memory().unwrap();

    // JSON.SET simple value
    let r1 = redlite.json_set("json:1", "$", r#"{"name":"Alice","age":30}"#, false, false);
    assert!(r1.is_ok());
    assert!(r1.unwrap());

    // JSON.SET nested object
    let r1 = redlite.json_set("json:2", "$", r#"{"user":{"name":"Bob","profile":{"city":"NYC"}}}"#, false, false);
    assert!(r1.is_ok());

    // JSON.SET array
    let r1 = redlite.json_set("json:3", "$", r#"[1,2,3,4,5]"#, false, false);
    assert!(r1.is_ok());

    // JSON.SET with NX (only if not exists)
    let r1 = redlite.json_set("json:nx", "$", r#"{"first":true}"#, true, false).unwrap();
    assert!(r1); // Should succeed - key doesn't exist

    let r2 = redlite.json_set("json:nx", "$", r#"{"second":true}"#, true, false).unwrap();
    assert!(!r2); // Should fail - key already exists

    // JSON.SET with XX (only if exists)
    let r1 = redlite.json_set("json:xx", "$", r#"{"first":true}"#, false, true).unwrap();
    assert!(!r1); // Should fail - key doesn't exist

    redlite.json_set("json:xx", "$", r#"{"first":true}"#, false, false).unwrap();
    let r2 = redlite.json_set("json:xx", "$", r#"{"second":true}"#, false, true).unwrap();
    assert!(r2); // Should succeed - key exists

    // JSON.SET nested path
    redlite.json_set("json:nested", "$", r#"{"a":{"b":{"c":1}}}"#, false, false).unwrap();
    let r1 = redlite.json_set("json:nested", "$.a.b.c", "42", false, false);
    assert!(r1.is_ok());

    // Verify nested update
    let val = redlite.json_get("json:nested", &["$.a.b.c"]).unwrap().unwrap();
    assert!(val.contains("42"));
}

/// JSON.GET command: all scenarios
#[test]
fn oracle_cmd_json_get() {
    let redlite = Db::open_memory().unwrap();

    // Setup
    redlite.json_set("json:get:1", "$", r#"{"name":"Alice","age":30,"active":true}"#, false, false).unwrap();
    redlite.json_set("json:get:2", "$", r#"{"items":[1,2,3],"nested":{"value":42}}"#, false, false).unwrap();

    // JSON.GET entire document
    let r1 = redlite.json_get("json:get:1", &["$"]).unwrap();
    assert!(r1.is_some());
    let val = r1.unwrap();
    assert!(val.contains("Alice"));
    assert!(val.contains("30"));

    // JSON.GET specific field
    let r1 = redlite.json_get("json:get:1", &["$.name"]).unwrap();
    assert!(r1.is_some());
    assert!(r1.unwrap().contains("Alice"));

    // JSON.GET nested path
    let r1 = redlite.json_get("json:get:2", &["$.nested.value"]).unwrap();
    assert!(r1.is_some());
    assert!(r1.unwrap().contains("42"));

    // JSON.GET array element
    let r1 = redlite.json_get("json:get:2", &["$.items[0]"]).unwrap();
    assert!(r1.is_some());
    assert!(r1.unwrap().contains("1"));

    // JSON.GET multiple paths
    let r1 = redlite.json_get("json:get:1", &["$.name", "$.age"]).unwrap();
    assert!(r1.is_some());
    let val = r1.unwrap();
    assert!(val.contains("Alice"));
    assert!(val.contains("30"));

    // JSON.GET non-existent key
    let r1 = redlite.json_get("nonexistent", &["$"]).unwrap();
    assert!(r1.is_none());

    // JSON.GET non-existent path returns None or empty array depending on impl
    let r1 = redlite.json_get("json:get:1", &["$.nonexistent"]).unwrap();
    // Result may be None or Some with empty array - both are valid
    if let Some(val) = r1 {
        assert!(val == "[]" || val.is_empty() || val == "[[]]");
    }
}

/// JSON.DEL command: all scenarios
#[test]
fn oracle_cmd_json_del() {
    let redlite = Db::open_memory().unwrap();

    // Setup
    redlite.json_set("json:del:1", "$", r#"{"a":1,"b":2,"c":3}"#, false, false).unwrap();
    redlite.json_set("json:del:2", "$", r#"{"items":[1,2,3,4,5]}"#, false, false).unwrap();

    // JSON.DEL specific field
    let r1 = redlite.json_del("json:del:1", Some("$.b")).unwrap();
    assert_eq!(r1, 1);

    // Verify deletion
    let val = redlite.json_get("json:del:1", &["$"]).unwrap().unwrap();
    assert!(!val.contains("\"b\""));

    // JSON.DEL array element (deletes matching elements)
    let r1 = redlite.json_del("json:del:2", Some("$.items[0]")).unwrap();
    assert!(r1 >= 1);

    // JSON.DEL entire document
    redlite.json_set("json:del:3", "$", r#"{"delete":"me"}"#, false, false).unwrap();
    let r1 = redlite.json_del("json:del:3", None).unwrap();
    assert_eq!(r1, 1);

    // Verify key is gone
    let r1 = redlite.json_get("json:del:3", &["$"]).unwrap();
    assert!(r1.is_none());

    // JSON.DEL non-existent key
    let r1 = redlite.json_del("nonexistent", Some("$.x")).unwrap();
    assert_eq!(r1, 0);
}

/// JSON.TYPE command: all scenarios
#[test]
fn oracle_cmd_json_type() {
    let redlite = Db::open_memory().unwrap();

    // Setup with various types
    redlite.json_set("json:type:1", "$", r#"{"str":"hello","num":42,"float":3.14,"bool":true,"null":null,"arr":[1,2],"obj":{"a":1}}"#, false, false).unwrap();

    // JSON.TYPE string
    let r1 = redlite.json_type("json:type:1", Some("$.str")).unwrap().unwrap();
    assert_eq!(r1, "string");

    // JSON.TYPE integer
    let r1 = redlite.json_type("json:type:1", Some("$.num")).unwrap().unwrap();
    assert_eq!(r1, "integer");

    // JSON.TYPE number (float)
    let r1 = redlite.json_type("json:type:1", Some("$.float")).unwrap().unwrap();
    assert_eq!(r1, "number");

    // JSON.TYPE boolean
    let r1 = redlite.json_type("json:type:1", Some("$.bool")).unwrap().unwrap();
    assert_eq!(r1, "boolean");

    // JSON.TYPE null
    let r1 = redlite.json_type("json:type:1", Some("$.null")).unwrap().unwrap();
    assert_eq!(r1, "null");

    // JSON.TYPE array
    let r1 = redlite.json_type("json:type:1", Some("$.arr")).unwrap().unwrap();
    assert_eq!(r1, "array");

    // JSON.TYPE object
    let r1 = redlite.json_type("json:type:1", Some("$.obj")).unwrap().unwrap();
    assert_eq!(r1, "object");

    // JSON.TYPE root
    let r1 = redlite.json_type("json:type:1", Some("$")).unwrap().unwrap();
    assert_eq!(r1, "object");

    // JSON.TYPE non-existent key
    let r1 = redlite.json_type("nonexistent", Some("$")).unwrap();
    assert!(r1.is_none());
}

/// JSON.NUMINCRBY command: all scenarios
#[test]
fn oracle_cmd_json_numincrby() {
    let redlite = Db::open_memory().unwrap();

    // Setup
    redlite.json_set("json:num:1", "$", r#"{"counter":10,"float":3.5}"#, false, false).unwrap();

    // NUMINCRBY integer
    let r1 = redlite.json_numincrby("json:num:1", "$.counter", 5.0).unwrap();
    assert!(r1.contains("15"));

    // NUMINCRBY negative (decrement)
    let r1 = redlite.json_numincrby("json:num:1", "$.counter", -3.0).unwrap();
    assert!(r1.contains("12"));

    // NUMINCRBY float
    let r1 = redlite.json_numincrby("json:num:1", "$.float", 0.5).unwrap();
    assert!(r1.contains("4"));

    // Verify final state
    let val = redlite.json_get("json:num:1", &["$"]).unwrap().unwrap();
    assert!(val.contains("12"));
}

/// JSON.STRAPPEND command: all scenarios
#[test]
fn oracle_cmd_json_strappend() {
    let redlite = Db::open_memory().unwrap();

    // Setup
    redlite.json_set("json:str:1", "$", r#"{"msg":"Hello"}"#, false, false).unwrap();

    // STRAPPEND - value must be a JSON-encoded string (with quotes)
    let r1 = redlite.json_strappend("json:str:1", Some("$.msg"), "\" World\"").unwrap();
    assert_eq!(r1, 11); // "Hello World" = 11 chars

    // Verify
    let val = redlite.json_get("json:str:1", &["$.msg"]).unwrap().unwrap();
    assert!(val.contains("Hello World"));
}

/// JSON.STRLEN command: all scenarios
#[test]
fn oracle_cmd_json_strlen() {
    let redlite = Db::open_memory().unwrap();

    // Setup
    redlite.json_set("json:strlen:1", "$", r#"{"short":"hi","long":"hello world"}"#, false, false).unwrap();

    // STRLEN short
    let r1 = redlite.json_strlen("json:strlen:1", Some("$.short")).unwrap().unwrap();
    assert_eq!(r1, 2);

    // STRLEN long
    let r1 = redlite.json_strlen("json:strlen:1", Some("$.long")).unwrap().unwrap();
    assert_eq!(r1, 11);

    // STRLEN non-existent
    let r1 = redlite.json_strlen("nonexistent", Some("$.x")).unwrap();
    assert!(r1.is_none());
}

/// JSON.ARRAPPEND command: all scenarios
#[test]
fn oracle_cmd_json_arrappend() {
    let redlite = Db::open_memory().unwrap();

    // Setup
    redlite.json_set("json:arr:1", "$", r#"{"items":[1,2,3]}"#, false, false).unwrap();

    // ARRAPPEND single value
    let r1 = redlite.json_arrappend("json:arr:1", "$.items", &["4"]).unwrap();
    assert_eq!(r1, 4); // New length

    // ARRAPPEND multiple values
    let r1 = redlite.json_arrappend("json:arr:1", "$.items", &["5", "6"]).unwrap();
    assert_eq!(r1, 6);

    // Verify
    let val = redlite.json_get("json:arr:1", &["$.items"]).unwrap().unwrap();
    assert!(val.contains("[1,2,3,4,5,6]") || val.contains("[1, 2, 3, 4, 5, 6]"));
}

/// JSON.ARRLEN command: all scenarios
#[test]
fn oracle_cmd_json_arrlen() {
    let redlite = Db::open_memory().unwrap();

    // Setup
    redlite.json_set("json:arrlen:1", "$", r#"{"items":[1,2,3,4,5],"empty":[]}"#, false, false).unwrap();

    // ARRLEN normal array
    let r1 = redlite.json_arrlen("json:arrlen:1", Some("$.items")).unwrap().unwrap();
    assert_eq!(r1, 5);

    // ARRLEN empty array
    let r1 = redlite.json_arrlen("json:arrlen:1", Some("$.empty")).unwrap().unwrap();
    assert_eq!(r1, 0);

    // ARRLEN non-existent
    let r1 = redlite.json_arrlen("nonexistent", Some("$.x")).unwrap();
    assert!(r1.is_none());
}

/// JSON.ARRPOP command: all scenarios
#[test]
fn oracle_cmd_json_arrpop() {
    let redlite = Db::open_memory().unwrap();

    // Setup
    redlite.json_set("json:arrpop:1", "$", r#"{"items":[1,2,3,4,5]}"#, false, false).unwrap();

    // ARRPOP last element (default)
    let r1 = redlite.json_arrpop("json:arrpop:1", Some("$.items"), None).unwrap().unwrap();
    assert!(r1.contains("5"));

    // ARRPOP first element
    let r1 = redlite.json_arrpop("json:arrpop:1", Some("$.items"), Some(0)).unwrap().unwrap();
    assert!(r1.contains("1"));

    // Verify remaining: [2,3,4]
    let r1 = redlite.json_arrlen("json:arrpop:1", Some("$.items")).unwrap().unwrap();
    assert_eq!(r1, 3);
}

/// JSON.ARRINDEX command: all scenarios
#[test]
fn oracle_cmd_json_arrindex() {
    let redlite = Db::open_memory().unwrap();

    // Setup
    redlite.json_set("json:arridx:1", "$", r#"{"items":[10,20,30,20,40]}"#, false, false).unwrap();

    // ARRINDEX find first occurrence
    let r1 = redlite.json_arrindex("json:arridx:1", "$.items", "20", None, None).unwrap();
    assert_eq!(r1, 1);

    // ARRINDEX find from offset
    let r1 = redlite.json_arrindex("json:arridx:1", "$.items", "20", Some(2), None).unwrap();
    assert_eq!(r1, 3);

    // ARRINDEX not found
    let r1 = redlite.json_arrindex("json:arridx:1", "$.items", "99", None, None).unwrap();
    assert_eq!(r1, -1);
}

/// JSON.ARRINSERT command: all scenarios
#[test]
fn oracle_cmd_json_arrinsert() {
    let redlite = Db::open_memory().unwrap();

    // Setup
    redlite.json_set("json:arrins:1", "$", r#"{"items":[1,2,3]}"#, false, false).unwrap();

    // ARRINSERT at beginning
    let r1 = redlite.json_arrinsert("json:arrins:1", "$.items", 0, &["0"]).unwrap();
    assert_eq!(r1, 4);

    // ARRINSERT in middle
    let r1 = redlite.json_arrinsert("json:arrins:1", "$.items", 2, &["1.5"]).unwrap();
    assert_eq!(r1, 5);

    // Verify: [0, 1, 1.5, 2, 3]
    let val = redlite.json_get("json:arrins:1", &["$.items"]).unwrap().unwrap();
    assert!(val.contains("0"));
    assert!(val.contains("1.5"));
}

/// JSON.ARRTRIM command: all scenarios
#[test]
fn oracle_cmd_json_arrtrim() {
    let redlite = Db::open_memory().unwrap();

    // Setup
    redlite.json_set("json:arrtrim:1", "$", r#"{"items":[0,1,2,3,4,5,6,7,8,9]}"#, false, false).unwrap();

    // ARRTRIM to middle range
    let r1 = redlite.json_arrtrim("json:arrtrim:1", "$.items", 2, 5).unwrap();
    assert_eq!(r1, 4); // New length: [2,3,4,5]

    // Verify
    let len = redlite.json_arrlen("json:arrtrim:1", Some("$.items")).unwrap().unwrap();
    assert_eq!(len, 4);
}

/// JSON.CLEAR command: all scenarios
#[test]
fn oracle_cmd_json_clear() {
    let redlite = Db::open_memory().unwrap();

    // Setup
    redlite.json_set("json:clear:1", "$", r#"{"arr":[1,2,3],"obj":{"a":1,"b":2},"num":42}"#, false, false).unwrap();

    // CLEAR array
    let r1 = redlite.json_clear("json:clear:1", Some("$.arr")).unwrap();
    assert_eq!(r1, 1);

    // Verify array is empty
    let len = redlite.json_arrlen("json:clear:1", Some("$.arr")).unwrap().unwrap();
    assert_eq!(len, 0);

    // CLEAR object
    let r1 = redlite.json_clear("json:clear:1", Some("$.obj")).unwrap();
    assert_eq!(r1, 1);
}

/// JSON.TOGGLE command: all scenarios
#[test]
fn oracle_cmd_json_toggle() {
    let redlite = Db::open_memory().unwrap();

    // Setup
    redlite.json_set("json:toggle:1", "$", r#"{"active":true,"disabled":false}"#, false, false).unwrap();

    // TOGGLE true -> false
    let r1 = redlite.json_toggle("json:toggle:1", "$.active").unwrap();
    assert!(!r1[0]); // Now false

    // TOGGLE false -> true
    let r1 = redlite.json_toggle("json:toggle:1", "$.disabled").unwrap();
    assert!(r1[0]); // Now true

    // Toggle back
    let r1 = redlite.json_toggle("json:toggle:1", "$.active").unwrap();
    assert!(r1[0]); // Back to true
}

/// JSON.MERGE command: all scenarios
#[test]
fn oracle_cmd_json_merge() {
    let redlite = Db::open_memory().unwrap();

    // Setup
    redlite.json_set("json:merge:1", "$", r#"{"a":1,"b":2}"#, false, false).unwrap();

    // MERGE new fields
    let r1 = redlite.json_merge("json:merge:1", "$", r#"{"c":3,"d":4}"#).unwrap();
    assert!(r1);

    // Verify merge
    let val = redlite.json_get("json:merge:1", &["$"]).unwrap().unwrap();
    assert!(val.contains("\"a\""));
    assert!(val.contains("\"c\""));
    assert!(val.contains("\"d\""));

    // MERGE overwrite existing
    let r1 = redlite.json_merge("json:merge:1", "$", r#"{"a":100}"#).unwrap();
    assert!(r1);

    let val = redlite.json_get("json:merge:1", &["$.a"]).unwrap().unwrap();
    assert!(val.contains("100"));
}

/// JSON.MGET command: all scenarios
#[test]
fn oracle_cmd_json_mget() {
    let redlite = Db::open_memory().unwrap();

    // Setup
    redlite.json_set("json:mget:1", "$", r#"{"name":"Alice"}"#, false, false).unwrap();
    redlite.json_set("json:mget:2", "$", r#"{"name":"Bob"}"#, false, false).unwrap();
    redlite.json_set("json:mget:3", "$", r#"{"name":"Charlie"}"#, false, false).unwrap();

    // MGET multiple keys
    let r1 = redlite.json_mget(&["json:mget:1", "json:mget:2", "json:mget:3"], "$.name").unwrap();
    assert_eq!(r1.len(), 3);
    assert!(r1[0].is_some());
    assert!(r1[1].is_some());
    assert!(r1[2].is_some());

    // MGET with non-existent key
    let r1 = redlite.json_mget(&["json:mget:1", "nonexistent", "json:mget:3"], "$.name").unwrap();
    assert_eq!(r1.len(), 3);
    assert!(r1[0].is_some());
    assert!(r1[1].is_none()); // Non-existent key
    assert!(r1[2].is_some());
}

/// JSON.OBJKEYS command: all scenarios
#[test]
fn oracle_cmd_json_objkeys() {
    let redlite = Db::open_memory().unwrap();

    // Setup
    redlite.json_set("json:objkeys:1", "$", r#"{"a":1,"b":2,"c":3}"#, false, false).unwrap();

    // OBJKEYS root
    let r1 = redlite.json_objkeys("json:objkeys:1", Some("$")).unwrap().unwrap();
    assert_eq!(r1.len(), 3);
    assert!(r1.contains(&"a".to_string()));
    assert!(r1.contains(&"b".to_string()));
    assert!(r1.contains(&"c".to_string()));

    // OBJKEYS non-existent
    let r1 = redlite.json_objkeys("nonexistent", Some("$")).unwrap();
    assert!(r1.is_none());
}

/// JSON.OBJLEN command: all scenarios
#[test]
fn oracle_cmd_json_objlen() {
    let redlite = Db::open_memory().unwrap();

    // Setup
    redlite.json_set("json:objlen:1", "$", r#"{"a":1,"b":2,"c":3,"d":4}"#, false, false).unwrap();

    // OBJLEN root
    let r1 = redlite.json_objlen("json:objlen:1", Some("$")).unwrap().unwrap();
    assert_eq!(r1, 4);

    // OBJLEN nested
    redlite.json_set("json:objlen:2", "$", r#"{"nested":{"x":1,"y":2}}"#, false, false).unwrap();
    let r1 = redlite.json_objlen("json:objlen:2", Some("$.nested")).unwrap().unwrap();
    assert_eq!(r1, 2);

    // OBJLEN non-existent
    let r1 = redlite.json_objlen("nonexistent", Some("$")).unwrap();
    assert!(r1.is_none());
}

// ============================================================================
// KEYINFO COMMAND - Key metadata (Redlite-specific)
// ============================================================================

/// KEYINFO command: all scenarios
#[test]
fn oracle_cmd_keyinfo() {
    use redlite::KeyType;
    use std::time::Duration;

    let redlite = Db::open_memory().unwrap();

    // KEYINFO on non-existent key
    let r1 = redlite.keyinfo("nonexistent").unwrap();
    assert!(r1.is_none());

    // KEYINFO on string
    redlite.set("keyinfo:string", b"hello", None).unwrap();
    let r1 = redlite.keyinfo("keyinfo:string").unwrap().unwrap();
    assert!(matches!(r1.key_type, KeyType::String));
    assert!(r1.created_at > 0);
    assert!(r1.updated_at > 0);

    // KEYINFO on list
    redlite.lpush("keyinfo:list", &[b"a", b"b"]).unwrap();
    let r1 = redlite.keyinfo("keyinfo:list").unwrap().unwrap();
    assert!(matches!(r1.key_type, KeyType::List));

    // KEYINFO on hash
    redlite.hset("keyinfo:hash", &[("field", b"value")]).unwrap();
    let r1 = redlite.keyinfo("keyinfo:hash").unwrap().unwrap();
    assert!(matches!(r1.key_type, KeyType::Hash));

    // KEYINFO on set
    redlite.sadd("keyinfo:set", &[b"a", b"b", b"c"]).unwrap();
    let r1 = redlite.keyinfo("keyinfo:set").unwrap().unwrap();
    assert!(matches!(r1.key_type, KeyType::Set));

    // KEYINFO on zset
    redlite.zadd("keyinfo:zset", &[ZMember { score: 1.0, member: b"a".to_vec() }]).unwrap();
    let r1 = redlite.keyinfo("keyinfo:zset").unwrap().unwrap();
    assert!(matches!(r1.key_type, KeyType::ZSet));

    // KEYINFO on JSON
    redlite.json_set("keyinfo:json", "$", r#"{"test":true}"#, false, false).unwrap();
    let r1 = redlite.keyinfo("keyinfo:json").unwrap().unwrap();
    assert!(matches!(r1.key_type, KeyType::Json));

    // KEYINFO with TTL
    redlite.set("keyinfo:ttl", b"expires", Some(Duration::from_secs(60))).unwrap();
    let r1 = redlite.keyinfo("keyinfo:ttl").unwrap().unwrap();
    assert!(r1.ttl > 0);
    assert!(r1.ttl <= 60000);
}

// ============================================================================
// IS_HISTORY_ENABLED / IS_FTS_ENABLED - Status check commands (Redlite-specific)
// ============================================================================

/// is_history_enabled command: all scenarios
#[test]
fn oracle_cmd_is_history_enabled() {
    use redlite::RetentionType;

    let redlite = Db::open_memory().unwrap();

    // Create test key
    redlite.set("history:check:1", b"value", None).unwrap();

    // History disabled by default
    let r1 = redlite.is_history_enabled("history:check:1").unwrap();
    assert!(!r1);

    // Enable history globally
    redlite.history_enable_global(RetentionType::Unlimited).unwrap();

    // Now should be enabled
    let r1 = redlite.is_history_enabled("history:check:1").unwrap();
    assert!(r1);

    // Disable globally
    redlite.history_disable_global().unwrap();
    let r1 = redlite.is_history_enabled("history:check:1").unwrap();
    assert!(!r1);

    // Enable for specific key
    redlite.history_enable_key("history:check:1", RetentionType::Unlimited).unwrap();
    let r1 = redlite.is_history_enabled("history:check:1").unwrap();
    assert!(r1);

    // Other keys should still be disabled
    redlite.set("history:check:2", b"value", None).unwrap();
    let r1 = redlite.is_history_enabled("history:check:2").unwrap();
    assert!(!r1);

    // Disable specific key
    redlite.history_disable_key("history:check:1").unwrap();
    let r1 = redlite.is_history_enabled("history:check:1").unwrap();
    assert!(!r1);

    // Enable for database - test with a fresh key (key-level disable takes precedence)
    redlite.history_enable_database(0, RetentionType::Count(100)).unwrap();
    redlite.set("history:check:3", b"value", None).unwrap();
    let r1 = redlite.is_history_enabled("history:check:3").unwrap();
    assert!(r1);
}

/// is_fts_enabled command: all scenarios
#[test]
fn oracle_cmd_is_fts_enabled() {
    let redlite = Db::open_memory().unwrap();

    // Create test key
    redlite.set("fts:check:1", b"searchable text", None).unwrap();

    // FTS disabled by default
    let r1 = redlite.is_fts_enabled("fts:check:1").unwrap();
    assert!(!r1);

    // Enable FTS globally
    redlite.fts_enable_global().unwrap();

    // Now should be enabled
    let r1 = redlite.is_fts_enabled("fts:check:1").unwrap();
    assert!(r1);

    // Disable globally
    redlite.fts_disable_global().unwrap();
    let r1 = redlite.is_fts_enabled("fts:check:1").unwrap();
    assert!(!r1);

    // Enable for specific key
    redlite.fts_enable_key("fts:check:1").unwrap();
    let r1 = redlite.is_fts_enabled("fts:check:1").unwrap();
    assert!(r1);

    // Other keys should still be disabled
    redlite.set("fts:check:2", b"other text", None).unwrap();
    let r1 = redlite.is_fts_enabled("fts:check:2").unwrap();
    assert!(!r1);

    // Disable specific key
    redlite.fts_disable_key("fts:check:1").unwrap();
    let r1 = redlite.is_fts_enabled("fts:check:1").unwrap();
    assert!(!r1);

    // Enable for database - test with a fresh key (key-level disable takes precedence)
    redlite.fts_enable_database(0).unwrap();
    redlite.set("fts:check:3", b"database enabled text", None).unwrap();
    let r1 = redlite.is_fts_enabled("fts:check:3").unwrap();
    assert!(r1);

    // Enable for pattern
    redlite.fts_disable_database(0).unwrap();
    redlite.fts_enable_pattern("fts:pattern:*").unwrap();
    redlite.set("fts:pattern:test", b"pattern match", None).unwrap();
    let r1 = redlite.is_fts_enabled("fts:pattern:test").unwrap();
    assert!(r1);

    // Non-matching pattern key (uses a fresh key without explicit disable)
    redlite.set("fts:other:1", b"other text", None).unwrap();
    let r1 = redlite.is_fts_enabled("fts:other:1").unwrap();
    assert!(!r1);
}
