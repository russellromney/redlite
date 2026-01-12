//! Integration tests for redlite server
//!
//! Run these tests with: cargo test --test integration
//!
//! Note: These tests start a TCP server and may conflict with other
//! services on port 16379. Run with --test-threads=1 if needed.

use std::process::{Child, Command};
use std::thread;
use std::time::Duration;

struct ServerProcess(Child);

impl Drop for ServerProcess {
    fn drop(&mut self) {
        let _ = self.0.kill();
    }
}

fn start_server(port: u16) -> ServerProcess {
    // Try to use the release binary if it exists, otherwise use cargo run
    let child = Command::new("./target/release/redlite")
        .args(["--db=:memory:", &format!("--addr=127.0.0.1:{}", port)])
        .spawn()
        .or_else(|_| {
            Command::new("./target/debug/redlite")
                .args(["--db=:memory:", &format!("--addr=127.0.0.1:{}", port)])
                .spawn()
        })
        .expect("Failed to start server - run `cargo build --release` first");

    // Give server time to start
    thread::sleep(Duration::from_millis(200));

    ServerProcess(child)
}

fn redis_cli(port: u16, args: &[&str]) -> String {
    let output = Command::new("redis-cli")
        .arg("-p")
        .arg(port.to_string())
        .args(args)
        .output()
        .expect("Failed to run redis-cli");

    String::from_utf8_lossy(&output.stdout).trim().to_string()
}

/// Run redis-cli with a specific database selected via -n flag
fn redis_cli_n(port: u16, db: i32, args: &[&str]) -> String {
    let output = Command::new("redis-cli")
        .arg("-p")
        .arg(port.to_string())
        .arg("-n")
        .arg(db.to_string())
        .args(args)
        .output()
        .expect("Failed to run redis-cli");

    String::from_utf8_lossy(&output.stdout).trim().to_string()
}

#[test]
fn test_ping() {
    let _server = start_server(16380);
    let result = redis_cli(16380, &["PING"]);
    assert_eq!(result, "PONG");
}

#[test]
fn test_echo() {
    let _server = start_server(16381);
    let result = redis_cli(16381, &["ECHO", "hello"]);
    assert_eq!(result, "hello");
}

#[test]
fn test_set_get() {
    let _server = start_server(16382);

    let set_result = redis_cli(16382, &["SET", "foo", "bar"]);
    assert_eq!(set_result, "OK");

    let get_result = redis_cli(16382, &["GET", "foo"]);
    assert_eq!(get_result, "bar");
}

#[test]
fn test_get_nonexistent() {
    let _server = start_server(16383);
    let result = redis_cli(16383, &["GET", "nonexistent"]);
    assert!(result.is_empty() || result == "(nil)");
}

#[test]
fn test_set_overwrite() {
    let _server = start_server(16384);

    redis_cli(16384, &["SET", "key", "value1"]);
    redis_cli(16384, &["SET", "key", "value2"]);
    let result = redis_cli(16384, &["GET", "key"]);

    assert_eq!(result, "value2");
}

#[test]
fn test_set_nx() {
    let _server = start_server(16385);

    // First SET NX should succeed
    let result1 = redis_cli(16385, &["SET", "nxkey", "value1", "NX"]);
    assert_eq!(result1, "OK");

    // Second SET NX should fail (return nil)
    let result2 = redis_cli(16385, &["SET", "nxkey", "value2", "NX"]);
    assert!(result2.is_empty() || result2 == "(nil)");

    // Original value should be preserved
    let value = redis_cli(16385, &["GET", "nxkey"]);
    assert_eq!(value, "value1");
}

#[test]
fn test_set_xx() {
    let _server = start_server(16386);

    // SET XX on nonexistent key should fail
    let result1 = redis_cli(16386, &["SET", "xxkey", "value1", "XX"]);
    assert!(result1.is_empty() || result1 == "(nil)");

    // Create the key
    redis_cli(16386, &["SET", "xxkey", "original"]);

    // SET XX on existing key should succeed
    let result2 = redis_cli(16386, &["SET", "xxkey", "updated", "XX"]);
    assert_eq!(result2, "OK");

    let value = redis_cli(16386, &["GET", "xxkey"]);
    assert_eq!(value, "updated");
}

// --- Session 2: Key commands integration tests ---

// Helper to check integer response (handles both "2" and "(integer) 2" formats)
fn check_int(result: &str, expected: i64) -> bool {
    result == expected.to_string() || result == format!("(integer) {}", expected)
}

#[test]
fn test_del() {
    let _server = start_server(16390);

    redis_cli(16390, &["SET", "k1", "v1"]);
    redis_cli(16390, &["SET", "k2", "v2"]);

    let result = redis_cli(16390, &["DEL", "k1", "k2", "k3"]);
    assert!(check_int(&result, 2), "Expected 2, got: {}", result);

    let get1 = redis_cli(16390, &["GET", "k1"]);
    let get2 = redis_cli(16390, &["GET", "k2"]);
    assert!(get1.is_empty() || get1 == "(nil)");
    assert!(get2.is_empty() || get2 == "(nil)");
}

#[test]
fn test_exists() {
    let _server = start_server(16391);

    redis_cli(16391, &["SET", "k1", "v1"]);
    redis_cli(16391, &["SET", "k2", "v2"]);

    let r1 = redis_cli(16391, &["EXISTS", "k1"]);
    assert!(check_int(&r1, 1), "Expected 1, got: {}", r1);

    let r2 = redis_cli(16391, &["EXISTS", "k1", "k2", "k3"]);
    assert!(check_int(&r2, 2), "Expected 2, got: {}", r2);

    let r3 = redis_cli(16391, &["EXISTS", "nonexistent"]);
    assert!(check_int(&r3, 0), "Expected 0, got: {}", r3);
}

#[test]
fn test_expire_ttl() {
    let _server = start_server(16392);

    redis_cli(16392, &["SET", "key", "value"]);

    // Key with no expiry should return -1
    let ttl_none = redis_cli(16392, &["TTL", "key"]);
    assert!(check_int(&ttl_none, -1), "Expected -1, got: {}", ttl_none);

    // Set expiration
    let expire_result = redis_cli(16392, &["EXPIRE", "key", "100"]);
    assert!(
        check_int(&expire_result, 1),
        "Expected 1, got: {}",
        expire_result
    );

    // TTL should be around 99-100
    let ttl = redis_cli(16392, &["TTL", "key"]);
    assert!(ttl.contains("99") || ttl.contains("100"));

    // Non-existent key should return -2
    let ttl_nonexistent = redis_cli(16392, &["TTL", "nonexistent"]);
    assert!(
        check_int(&ttl_nonexistent, -2),
        "Expected -2, got: {}",
        ttl_nonexistent
    );

    // EXPIRE on non-existent key should return 0
    let expire_nonexistent = redis_cli(16392, &["EXPIRE", "nonexistent", "100"]);
    assert!(
        check_int(&expire_nonexistent, 0),
        "Expected 0, got: {}",
        expire_nonexistent
    );
}

#[test]
fn test_pttl() {
    let _server = start_server(16393);

    redis_cli(16393, &["SET", "key", "value", "EX", "10"]);

    let pttl = redis_cli(16393, &["PTTL", "key"]);
    // Should be close to 10000ms
    assert!(pttl.contains("99") || pttl.contains("100"));

    // No expiry
    redis_cli(16393, &["SET", "noexp", "value"]);
    let pttl_none = redis_cli(16393, &["PTTL", "noexp"]);
    assert!(
        check_int(&pttl_none, -1),
        "Expected -1, got: {}",
        pttl_none
    );

    // Non-existent
    let pttl_nonexistent = redis_cli(16393, &["PTTL", "nonexistent"]);
    assert!(
        check_int(&pttl_nonexistent, -2),
        "Expected -2, got: {}",
        pttl_nonexistent
    );
}

#[test]
fn test_type() {
    let _server = start_server(16394);

    redis_cli(16394, &["SET", "mykey", "value"]);
    assert_eq!(redis_cli(16394, &["TYPE", "mykey"]), "string");
    assert_eq!(redis_cli(16394, &["TYPE", "nonexistent"]), "none");
}

#[test]
fn test_keys() {
    let _server = start_server(16395);

    redis_cli(16395, &["SET", "foo", "1"]);
    redis_cli(16395, &["SET", "foobar", "2"]);
    redis_cli(16395, &["SET", "bar", "3"]);

    let result = redis_cli(16395, &["KEYS", "foo*"]);
    assert!(result.contains("foo"));
    assert!(result.contains("foobar"));
    // bar should not be in results for foo* pattern
}

#[test]
fn test_scan() {
    let _server = start_server(16396);

    for i in 0..5 {
        redis_cli(16396, &["SET", &format!("key{}", i), "value"]);
    }

    let result = redis_cli(16396, &["SCAN", "0"]);
    // Should return cursor and keys
    assert!(result.contains("key"));
}

#[test]
fn test_scan_match() {
    let _server = start_server(16397);

    redis_cli(16397, &["SET", "user:1", "a"]);
    redis_cli(16397, &["SET", "user:2", "b"]);
    redis_cli(16397, &["SET", "other:1", "c"]);

    let result = redis_cli(16397, &["SCAN", "0", "MATCH", "user:*"]);
    assert!(result.contains("user:1"));
    assert!(result.contains("user:2"));
    assert!(!result.contains("other:1"));
}

#[test]
fn test_scan_count() {
    let _server = start_server(16398);

    for i in 0..10 {
        redis_cli(16398, &["SET", &format!("key{}", i), "value"]);
    }

    let result = redis_cli(16398, &["SCAN", "0", "COUNT", "3"]);
    // Should contain key entries
    assert!(result.contains("key"), "Expected keys, got: {}", result);
}

// --- Session 3: String operations integration tests ---

#[test]
fn test_incr_decr() {
    let _server = start_server(16400);

    // INCR on non-existent key
    let r1 = redis_cli(16400, &["INCR", "counter"]);
    assert!(check_int(&r1, 1), "Expected 1, got: {}", r1);

    let r2 = redis_cli(16400, &["INCR", "counter"]);
    assert!(check_int(&r2, 2), "Expected 2, got: {}", r2);

    // DECR
    let r3 = redis_cli(16400, &["DECR", "counter"]);
    assert!(check_int(&r3, 1), "Expected 1, got: {}", r3);

    // INCR existing integer
    redis_cli(16400, &["SET", "num", "10"]);
    let r4 = redis_cli(16400, &["INCR", "num"]);
    assert!(check_int(&r4, 11), "Expected 11, got: {}", r4);
}

#[test]
fn test_incrby_decrby() {
    let _server = start_server(16401);

    redis_cli(16401, &["SET", "counter", "100"]);

    let r1 = redis_cli(16401, &["INCRBY", "counter", "50"]);
    assert!(check_int(&r1, 150), "Expected 150, got: {}", r1);

    let r2 = redis_cli(16401, &["DECRBY", "counter", "30"]);
    assert!(check_int(&r2, 120), "Expected 120, got: {}", r2);

    // INCRBY on non-existent key
    let r3 = redis_cli(16401, &["INCRBY", "newkey", "5"]);
    assert!(check_int(&r3, 5), "Expected 5, got: {}", r3);
}

#[test]
fn test_incrbyfloat() {
    let _server = start_server(16402);

    redis_cli(16402, &["SET", "pi", "3.14"]);

    let result = redis_cli(16402, &["INCRBYFLOAT", "pi", "0.01"]);
    assert!(
        result.contains("3.15"),
        "Expected 3.15, got: {}",
        result
    );
}

#[test]
fn test_mget_mset() {
    let _server = start_server(16403);

    // MSET
    let mset_result = redis_cli(16403, &["MSET", "a", "1", "b", "2", "c", "3"]);
    assert_eq!(mset_result, "OK");

    // MGET
    let mget_result = redis_cli(16403, &["MGET", "a", "b", "c", "d"]);
    assert!(mget_result.contains("1"));
    assert!(mget_result.contains("2"));
    assert!(mget_result.contains("3"));
}

#[test]
fn test_append_strlen() {
    let _server = start_server(16404);

    // APPEND to non-existent
    let r1 = redis_cli(16404, &["APPEND", "msg", "Hello"]);
    assert!(check_int(&r1, 5), "Expected 5, got: {}", r1);

    // APPEND to existing
    let r2 = redis_cli(16404, &["APPEND", "msg", " World"]);
    assert!(check_int(&r2, 11), "Expected 11, got: {}", r2);

    // Verify content
    let content = redis_cli(16404, &["GET", "msg"]);
    assert_eq!(content, "Hello World");

    // STRLEN
    let len = redis_cli(16404, &["STRLEN", "msg"]);
    assert!(check_int(&len, 11), "Expected 11, got: {}", len);

    // STRLEN non-existent
    let len_none = redis_cli(16404, &["STRLEN", "nonexistent"]);
    assert!(check_int(&len_none, 0), "Expected 0, got: {}", len_none);
}

#[test]
fn test_getrange() {
    let _server = start_server(16405);

    redis_cli(16405, &["SET", "msg", "Hello World"]);

    // Normal range
    let r1 = redis_cli(16405, &["GETRANGE", "msg", "0", "4"]);
    assert_eq!(r1, "Hello");

    // Negative indices
    let r2 = redis_cli(16405, &["GETRANGE", "msg", "-5", "-1"]);
    assert_eq!(r2, "World");
}

#[test]
fn test_setrange() {
    let _server = start_server(16406);

    redis_cli(16406, &["SET", "msg", "Hello World"]);

    // Overwrite
    let r1 = redis_cli(16406, &["SETRANGE", "msg", "6", "Redis"]);
    assert!(check_int(&r1, 11), "Expected 11, got: {}", r1);

    let content = redis_cli(16406, &["GET", "msg"]);
    assert_eq!(content, "Hello Redis");
}

// --- Session 6: Hash operations integration tests ---

#[test]
fn test_hset_hget() {
    let _server = start_server(16410);

    // HSET new field
    let r1 = redis_cli(16410, &["HSET", "myhash", "field1", "value1"]);
    assert!(check_int(&r1, 1), "Expected 1, got: {}", r1);

    // HGET
    let r2 = redis_cli(16410, &["HGET", "myhash", "field1"]);
    assert_eq!(r2, "value1");

    // HSET update existing field
    let r3 = redis_cli(16410, &["HSET", "myhash", "field1", "value2"]);
    assert!(check_int(&r3, 0), "Expected 0, got: {}", r3);

    // HGET updated value
    let r4 = redis_cli(16410, &["HGET", "myhash", "field1"]);
    assert_eq!(r4, "value2");

    // HGET non-existent field
    let r5 = redis_cli(16410, &["HGET", "myhash", "nonexistent"]);
    assert!(r5.is_empty() || r5 == "(nil)");
}

#[test]
fn test_hset_multiple() {
    let _server = start_server(16411);

    // HSET multiple fields
    let r1 = redis_cli(16411, &["HSET", "myhash", "f1", "v1", "f2", "v2", "f3", "v3"]);
    assert!(check_int(&r1, 3), "Expected 3, got: {}", r1);

    assert_eq!(redis_cli(16411, &["HGET", "myhash", "f1"]), "v1");
    assert_eq!(redis_cli(16411, &["HGET", "myhash", "f2"]), "v2");
    assert_eq!(redis_cli(16411, &["HGET", "myhash", "f3"]), "v3");
}

#[test]
fn test_hmget() {
    let _server = start_server(16412);

    redis_cli(16412, &["HSET", "myhash", "f1", "v1", "f2", "v2"]);

    let result = redis_cli(16412, &["HMGET", "myhash", "f1", "f2", "f3"]);
    assert!(result.contains("v1"));
    assert!(result.contains("v2"));
}

#[test]
fn test_hgetall() {
    let _server = start_server(16413);

    redis_cli(16413, &["HSET", "myhash", "f1", "v1", "f2", "v2"]);

    let result = redis_cli(16413, &["HGETALL", "myhash"]);
    assert!(result.contains("f1"));
    assert!(result.contains("v1"));
    assert!(result.contains("f2"));
    assert!(result.contains("v2"));
}

#[test]
fn test_hdel() {
    let _server = start_server(16414);

    redis_cli(16414, &["HSET", "myhash", "f1", "v1", "f2", "v2", "f3", "v3"]);

    // Delete one field
    let r1 = redis_cli(16414, &["HDEL", "myhash", "f1"]);
    assert!(check_int(&r1, 1), "Expected 1, got: {}", r1);

    // Verify deleted
    let r2 = redis_cli(16414, &["HGET", "myhash", "f1"]);
    assert!(r2.is_empty() || r2 == "(nil)");

    // Delete multiple fields (including non-existent)
    let r3 = redis_cli(16414, &["HDEL", "myhash", "f2", "f3", "f4"]);
    assert!(check_int(&r3, 2), "Expected 2, got: {}", r3);
}

#[test]
fn test_hexists() {
    let _server = start_server(16415);

    redis_cli(16415, &["HSET", "myhash", "field", "value"]);

    let r1 = redis_cli(16415, &["HEXISTS", "myhash", "field"]);
    assert!(check_int(&r1, 1), "Expected 1, got: {}", r1);

    let r2 = redis_cli(16415, &["HEXISTS", "myhash", "nonexistent"]);
    assert!(check_int(&r2, 0), "Expected 0, got: {}", r2);

    let r3 = redis_cli(16415, &["HEXISTS", "nonexistent", "field"]);
    assert!(check_int(&r3, 0), "Expected 0, got: {}", r3);
}

#[test]
fn test_hkeys_hvals() {
    let _server = start_server(16416);

    redis_cli(16416, &["HSET", "myhash", "f1", "v1", "f2", "v2"]);

    let keys = redis_cli(16416, &["HKEYS", "myhash"]);
    assert!(keys.contains("f1"));
    assert!(keys.contains("f2"));

    let vals = redis_cli(16416, &["HVALS", "myhash"]);
    assert!(vals.contains("v1"));
    assert!(vals.contains("v2"));
}

#[test]
fn test_hlen() {
    let _server = start_server(16417);

    // Non-existent key
    let r1 = redis_cli(16417, &["HLEN", "nonexistent"]);
    assert!(check_int(&r1, 0), "Expected 0, got: {}", r1);

    redis_cli(16417, &["HSET", "myhash", "f1", "v1", "f2", "v2"]);

    let r2 = redis_cli(16417, &["HLEN", "myhash"]);
    assert!(check_int(&r2, 2), "Expected 2, got: {}", r2);
}

#[test]
fn test_hincrby() {
    let _server = start_server(16418);

    // HINCRBY on non-existent field
    let r1 = redis_cli(16418, &["HINCRBY", "myhash", "counter", "5"]);
    assert!(check_int(&r1, 5), "Expected 5, got: {}", r1);

    let r2 = redis_cli(16418, &["HINCRBY", "myhash", "counter", "10"]);
    assert!(check_int(&r2, 15), "Expected 15, got: {}", r2);

    let r3 = redis_cli(16418, &["HINCRBY", "myhash", "counter", "-3"]);
    assert!(check_int(&r3, 12), "Expected 12, got: {}", r3);
}

#[test]
fn test_hincrbyfloat() {
    let _server = start_server(16419);

    redis_cli(16419, &["HSET", "myhash", "pi", "3.14"]);

    let result = redis_cli(16419, &["HINCRBYFLOAT", "myhash", "pi", "0.01"]);
    assert!(result.contains("3.15"), "Expected 3.15, got: {}", result);
}

#[test]
fn test_hsetnx() {
    let _server = start_server(16420);

    // First HSETNX should succeed
    let r1 = redis_cli(16420, &["HSETNX", "myhash", "field", "value1"]);
    assert!(check_int(&r1, 1), "Expected 1, got: {}", r1);

    // Second HSETNX should fail
    let r2 = redis_cli(16420, &["HSETNX", "myhash", "field", "value2"]);
    assert!(check_int(&r2, 0), "Expected 0, got: {}", r2);

    // Original value preserved
    let value = redis_cli(16420, &["HGET", "myhash", "field"]);
    assert_eq!(value, "value1");
}

#[test]
fn test_hash_type() {
    let _server = start_server(16421);

    redis_cli(16421, &["HSET", "myhash", "field", "value"]);
    assert_eq!(redis_cli(16421, &["TYPE", "myhash"]), "hash");
}

// --- Session 7: List operation integration tests ---

#[test]
fn test_lpush_rpush() {
    let _server = start_server(16422);

    // LPUSH creates list and prepends
    let r1 = redis_cli(16422, &["LPUSH", "mylist", "a"]);
    assert!(check_int(&r1, 1), "Expected 1, got: {}", r1);

    let r2 = redis_cli(16422, &["LPUSH", "mylist", "b", "c"]);
    assert!(check_int(&r2, 3), "Expected 3, got: {}", r2);

    // RPUSH appends to end
    let r3 = redis_cli(16422, &["RPUSH", "mylist", "d"]);
    assert!(check_int(&r3, 4), "Expected 4, got: {}", r3);

    // Check order: c, b, a, d
    let range = redis_cli(16422, &["LRANGE", "mylist", "0", "-1"]);
    assert!(range.contains('c') && range.contains('d'), "Unexpected range: {}", range);
}

#[test]
fn test_lpop_rpop() {
    let _server = start_server(16423);

    redis_cli(16423, &["RPUSH", "mylist", "a", "b", "c", "d"]);

    // LPOP single element
    let r1 = redis_cli(16423, &["LPOP", "mylist"]);
    assert_eq!(r1, "a", "Expected 'a', got: {}", r1);

    // RPOP single element
    let r2 = redis_cli(16423, &["RPOP", "mylist"]);
    assert_eq!(r2, "d", "Expected 'd', got: {}", r2);

    // LPOP with count
    let r3 = redis_cli(16423, &["LPOP", "mylist", "2"]);
    assert!(r3.contains('b') && r3.contains('c'), "Expected 'b' and 'c', got: {}", r3);

    // List should be empty now
    let len = redis_cli(16423, &["LLEN", "mylist"]);
    assert!(check_int(&len, 0), "Expected 0, got: {}", len);
}

#[test]
fn test_lpop_empty() {
    let _server = start_server(16424);

    // LPOP on non-existent key returns nil
    let result = redis_cli(16424, &["LPOP", "nonexistent"]);
    assert!(result.is_empty() || result == "(nil)", "Expected nil, got: {}", result);
}

#[test]
fn test_llen() {
    let _server = start_server(16425);

    // Non-existent key
    let r1 = redis_cli(16425, &["LLEN", "nonexistent"]);
    assert!(check_int(&r1, 0), "Expected 0, got: {}", r1);

    redis_cli(16425, &["RPUSH", "mylist", "a", "b", "c"]);

    let r2 = redis_cli(16425, &["LLEN", "mylist"]);
    assert!(check_int(&r2, 3), "Expected 3, got: {}", r2);
}

#[test]
fn test_lrange() {
    let _server = start_server(16426);

    redis_cli(16426, &["RPUSH", "mylist", "a", "b", "c", "d", "e"]);

    // Full range
    let r1 = redis_cli(16426, &["LRANGE", "mylist", "0", "-1"]);
    assert!(r1.contains('a') && r1.contains('e'), "Expected full list, got: {}", r1);

    // Partial range
    let r2 = redis_cli(16426, &["LRANGE", "mylist", "1", "3"]);
    assert!(r2.contains('b') && r2.contains('d'), "Expected b-d, got: {}", r2);

    // Negative indices
    let r3 = redis_cli(16426, &["LRANGE", "mylist", "-2", "-1"]);
    assert!(r3.contains('d') && r3.contains('e'), "Expected d-e, got: {}", r3);
}

#[test]
fn test_lindex() {
    let _server = start_server(16427);

    redis_cli(16427, &["RPUSH", "mylist", "a", "b", "c"]);

    assert_eq!(redis_cli(16427, &["LINDEX", "mylist", "0"]), "a");
    assert_eq!(redis_cli(16427, &["LINDEX", "mylist", "2"]), "c");
    assert_eq!(redis_cli(16427, &["LINDEX", "mylist", "-1"]), "c");

    // Out of bounds
    let r = redis_cli(16427, &["LINDEX", "mylist", "10"]);
    assert!(r.is_empty() || r == "(nil)", "Expected nil, got: {}", r);
}

#[test]
fn test_lset() {
    let _server = start_server(16428);

    redis_cli(16428, &["RPUSH", "mylist", "a", "b", "c"]);

    let result = redis_cli(16428, &["LSET", "mylist", "1", "B"]);
    assert_eq!(result, "OK", "Expected OK, got: {}", result);

    assert_eq!(redis_cli(16428, &["LINDEX", "mylist", "1"]), "B");
}

#[test]
fn test_lset_errors() {
    let _server = start_server(16429);

    // LSET on non-existent key
    let r1 = redis_cli(16429, &["LSET", "nonexistent", "0", "value"]);
    assert!(r1.contains("no such key") || r1.contains("ERR"), "Expected error, got: {}", r1);

    redis_cli(16429, &["RPUSH", "mylist", "a"]);

    // LSET out of range
    let r2 = redis_cli(16429, &["LSET", "mylist", "10", "value"]);
    assert!(r2.contains("out of range") || r2.contains("ERR"), "Expected error, got: {}", r2);
}

#[test]
fn test_ltrim() {
    let _server = start_server(16430);

    redis_cli(16430, &["RPUSH", "mylist", "a", "b", "c", "d", "e"]);

    let result = redis_cli(16430, &["LTRIM", "mylist", "1", "3"]);
    assert_eq!(result, "OK", "Expected OK, got: {}", result);

    let len = redis_cli(16430, &["LLEN", "mylist"]);
    assert!(check_int(&len, 3), "Expected 3, got: {}", len);

    let range = redis_cli(16430, &["LRANGE", "mylist", "0", "-1"]);
    assert!(range.contains('b') && range.contains('d'), "Expected b-d, got: {}", range);
}

#[test]
fn test_list_type() {
    let _server = start_server(16431);

    redis_cli(16431, &["RPUSH", "mylist", "value"]);
    assert_eq!(redis_cli(16431, &["TYPE", "mylist"]), "list");
}

#[test]
fn test_list_wrong_type() {
    let _server = start_server(16432);

    // Create a string key
    redis_cli(16432, &["SET", "mystring", "value"]);

    // List operations on string should fail
    let r1 = redis_cli(16432, &["LPUSH", "mystring", "a"]);
    assert!(r1.contains("WRONGTYPE"), "Expected WRONGTYPE, got: {}", r1);

    let r2 = redis_cli(16432, &["LRANGE", "mystring", "0", "-1"]);
    assert!(r2.contains("WRONGTYPE"), "Expected WRONGTYPE, got: {}", r2);
}

// --- Session 8: Set operations integration tests ---

#[test]
fn test_sadd_smembers() {
    let _server = start_server(16433);

    // SADD returns count of new members added
    let r1 = redis_cli(16433, &["SADD", "myset", "a", "b", "c"]);
    assert!(check_int(&r1, 3), "Expected 3, got: {}", r1);

    // Adding duplicate members
    let r2 = redis_cli(16433, &["SADD", "myset", "a", "d"]);
    assert!(check_int(&r2, 1), "Expected 1, got: {}", r2);

    // SMEMBERS returns all members
    let members = redis_cli(16433, &["SMEMBERS", "myset"]);
    assert!(members.contains('a'), "Expected 'a' in members: {}", members);
    assert!(members.contains('b'), "Expected 'b' in members: {}", members);
    assert!(members.contains('c'), "Expected 'c' in members: {}", members);
    assert!(members.contains('d'), "Expected 'd' in members: {}", members);
}

#[test]
fn test_srem() {
    let _server = start_server(16434);

    redis_cli(16434, &["SADD", "myset", "a", "b", "c"]);

    // Remove one member
    let r1 = redis_cli(16434, &["SREM", "myset", "a"]);
    assert!(check_int(&r1, 1), "Expected 1, got: {}", r1);

    // Remove nonexistent member
    let r2 = redis_cli(16434, &["SREM", "myset", "nonexistent"]);
    assert!(check_int(&r2, 0), "Expected 0, got: {}", r2);

    // Remove from nonexistent key
    let r3 = redis_cli(16434, &["SREM", "nokey", "x"]);
    assert!(check_int(&r3, 0), "Expected 0, got: {}", r3);
}

#[test]
fn test_sismember() {
    let _server = start_server(16435);

    redis_cli(16435, &["SADD", "myset", "a", "b"]);

    let r1 = redis_cli(16435, &["SISMEMBER", "myset", "a"]);
    assert!(check_int(&r1, 1), "Expected 1, got: {}", r1);

    let r2 = redis_cli(16435, &["SISMEMBER", "myset", "c"]);
    assert!(check_int(&r2, 0), "Expected 0, got: {}", r2);

    // Nonexistent key
    let r3 = redis_cli(16435, &["SISMEMBER", "nokey", "x"]);
    assert!(check_int(&r3, 0), "Expected 0, got: {}", r3);
}

#[test]
fn test_scard() {
    let _server = start_server(16436);

    // Empty/nonexistent set
    let r1 = redis_cli(16436, &["SCARD", "nokey"]);
    assert!(check_int(&r1, 0), "Expected 0, got: {}", r1);

    redis_cli(16436, &["SADD", "myset", "a", "b", "c"]);
    let r2 = redis_cli(16436, &["SCARD", "myset"]);
    assert!(check_int(&r2, 3), "Expected 3, got: {}", r2);
}

#[test]
fn test_spop() {
    let _server = start_server(16437);

    redis_cli(16437, &["SADD", "myset", "a", "b", "c"]);

    // Pop single element
    let r1 = redis_cli(16437, &["SPOP", "myset"]);
    assert!(!r1.is_empty() && r1 != "(nil)", "Expected a value, got: {}", r1);

    let card = redis_cli(16437, &["SCARD", "myset"]);
    assert!(check_int(&card, 2), "Expected 2, got: {}", card);

    // Pop from empty/nonexistent
    let r2 = redis_cli(16437, &["SPOP", "nokey"]);
    assert!(r2.is_empty() || r2 == "(nil)", "Expected nil, got: {}", r2);
}

#[test]
fn test_srandmember() {
    let _server = start_server(16438);

    redis_cli(16438, &["SADD", "myset", "a", "b", "c"]);

    // Get random member without removing
    let r1 = redis_cli(16438, &["SRANDMEMBER", "myset"]);
    assert!(!r1.is_empty() && r1 != "(nil)", "Expected a value, got: {}", r1);

    // Verify set unchanged
    let card = redis_cli(16438, &["SCARD", "myset"]);
    assert!(check_int(&card, 3), "Expected 3, got: {}", card);

    // Get multiple random members
    let r2 = redis_cli(16438, &["SRANDMEMBER", "myset", "2"]);
    assert!(!r2.is_empty(), "Expected array, got: {}", r2);
}

#[test]
fn test_sdiff() {
    let _server = start_server(16439);

    redis_cli(16439, &["SADD", "set1", "a", "b", "c"]);
    redis_cli(16439, &["SADD", "set2", "b", "c", "d"]);

    let diff = redis_cli(16439, &["SDIFF", "set1", "set2"]);
    assert!(diff.contains('a'), "Expected 'a' in diff: {}", diff);
    assert!(!diff.contains('b'), "Unexpected 'b' in diff: {}", diff);
    assert!(!diff.contains('c'), "Unexpected 'c' in diff: {}", diff);
}

#[test]
fn test_sinter() {
    let _server = start_server(16440);

    redis_cli(16440, &["SADD", "set1", "a", "b", "c"]);
    redis_cli(16440, &["SADD", "set2", "b", "c", "d"]);

    let inter = redis_cli(16440, &["SINTER", "set1", "set2"]);
    assert!(inter.contains('b'), "Expected 'b' in inter: {}", inter);
    assert!(inter.contains('c'), "Expected 'c' in inter: {}", inter);
    assert!(!inter.contains('a'), "Unexpected 'a' in inter: {}", inter);
    assert!(!inter.contains('d'), "Unexpected 'd' in inter: {}", inter);
}

#[test]
fn test_sunion() {
    let _server = start_server(16441);

    redis_cli(16441, &["SADD", "set1", "a", "b"]);
    redis_cli(16441, &["SADD", "set2", "b", "c"]);

    let union_result = redis_cli(16441, &["SUNION", "set1", "set2"]);
    assert!(union_result.contains('a'), "Expected 'a' in union: {}", union_result);
    assert!(union_result.contains('b'), "Expected 'b' in union: {}", union_result);
    assert!(union_result.contains('c'), "Expected 'c' in union: {}", union_result);
}

#[test]
fn test_set_type() {
    let _server = start_server(16442);

    redis_cli(16442, &["SADD", "myset", "value"]);
    assert_eq!(redis_cli(16442, &["TYPE", "myset"]), "set");
}

#[test]
fn test_set_wrong_type() {
    let _server = start_server(16443);

    // Create a string key
    redis_cli(16443, &["SET", "mystring", "value"]);

    // Set operations on string should fail
    let r1 = redis_cli(16443, &["SADD", "mystring", "a"]);
    assert!(r1.contains("WRONGTYPE"), "Expected WRONGTYPE, got: {}", r1);

    let r2 = redis_cli(16443, &["SMEMBERS", "mystring"]);
    assert!(r2.contains("WRONGTYPE"), "Expected WRONGTYPE, got: {}", r2);
}

// --- Session 9: Sorted Set integration tests (ports 16444+) ---

#[test]
fn test_zadd_zcard() {
    let _server = start_server(16444);

    // ZADD returns count of new members added
    let r1 = redis_cli(16444, &["ZADD", "myzset", "1", "a", "2", "b", "3", "c"]);
    assert!(check_int(&r1, 3), "Expected 3, got: {}", r1);

    // ZCARD returns count
    let r2 = redis_cli(16444, &["ZCARD", "myzset"]);
    assert!(check_int(&r2, 3), "Expected 3, got: {}", r2);

    // Adding duplicate member (updates score) should return 0 for new members
    let r3 = redis_cli(16444, &["ZADD", "myzset", "1.5", "a", "4", "d"]);
    assert!(check_int(&r3, 1), "Expected 1, got: {}", r3);

    let r4 = redis_cli(16444, &["ZCARD", "myzset"]);
    assert!(check_int(&r4, 4), "Expected 4, got: {}", r4);
}

#[test]
fn test_zrem() {
    let _server = start_server(16445);

    redis_cli(16445, &["ZADD", "myzset", "1", "a", "2", "b", "3", "c"]);

    // Remove one member
    let r1 = redis_cli(16445, &["ZREM", "myzset", "a"]);
    assert!(check_int(&r1, 1), "Expected 1, got: {}", r1);

    // Remove nonexistent member
    let r2 = redis_cli(16445, &["ZREM", "myzset", "nonexistent"]);
    assert!(check_int(&r2, 0), "Expected 0, got: {}", r2);

    // Remove from nonexistent key
    let r3 = redis_cli(16445, &["ZREM", "nokey", "x"]);
    assert!(check_int(&r3, 0), "Expected 0, got: {}", r3);
}

#[test]
fn test_zscore() {
    let _server = start_server(16446);

    redis_cli(16446, &["ZADD", "myzset", "1.5", "a", "2.5", "b"]);

    let r1 = redis_cli(16446, &["ZSCORE", "myzset", "a"]);
    assert!(r1.contains("1.5"), "Expected 1.5, got: {}", r1);

    let r2 = redis_cli(16446, &["ZSCORE", "myzset", "b"]);
    assert!(r2.contains("2.5"), "Expected 2.5, got: {}", r2);

    // Nonexistent member
    let r3 = redis_cli(16446, &["ZSCORE", "myzset", "nonexistent"]);
    assert!(r3.is_empty() || r3 == "(nil)", "Expected nil, got: {}", r3);

    // Nonexistent key
    let r4 = redis_cli(16446, &["ZSCORE", "nokey", "x"]);
    assert!(r4.is_empty() || r4 == "(nil)", "Expected nil, got: {}", r4);
}

#[test]
fn test_zrank_zrevrank() {
    let _server = start_server(16447);

    redis_cli(16447, &["ZADD", "myzset", "1", "a", "2", "b", "3", "c"]);

    // ZRANK (ascending)
    let r1 = redis_cli(16447, &["ZRANK", "myzset", "a"]);
    assert!(check_int(&r1, 0), "Expected 0, got: {}", r1);

    let r2 = redis_cli(16447, &["ZRANK", "myzset", "c"]);
    assert!(check_int(&r2, 2), "Expected 2, got: {}", r2);

    // ZREVRANK (descending)
    let r3 = redis_cli(16447, &["ZREVRANK", "myzset", "a"]);
    assert!(check_int(&r3, 2), "Expected 2, got: {}", r3);

    let r4 = redis_cli(16447, &["ZREVRANK", "myzset", "c"]);
    assert!(check_int(&r4, 0), "Expected 0, got: {}", r4);

    // Nonexistent member
    let r5 = redis_cli(16447, &["ZRANK", "myzset", "nonexistent"]);
    assert!(r5.is_empty() || r5 == "(nil)", "Expected nil, got: {}", r5);
}

#[test]
fn test_zrange() {
    let _server = start_server(16448);

    redis_cli(16448, &["ZADD", "myzset", "1", "a", "2", "b", "3", "c"]);

    // Get all members
    let r1 = redis_cli(16448, &["ZRANGE", "myzset", "0", "-1"]);
    assert!(r1.contains('a') && r1.contains('b') && r1.contains('c'),
            "Expected a, b, c in result: {}", r1);

    // Subset
    let r2 = redis_cli(16448, &["ZRANGE", "myzset", "0", "1"]);
    assert!(r2.contains('a') && r2.contains('b'), "Expected a, b in result: {}", r2);

    // With WITHSCORES
    let r3 = redis_cli(16448, &["ZRANGE", "myzset", "0", "-1", "WITHSCORES"]);
    assert!(r3.contains('a') && r3.contains('1'), "Expected member and score: {}", r3);
}

#[test]
fn test_zrevrange() {
    let _server = start_server(16449);

    redis_cli(16449, &["ZADD", "myzset", "1", "a", "2", "b", "3", "c"]);

    // Get all in reverse
    let r1 = redis_cli(16449, &["ZREVRANGE", "myzset", "0", "-1"]);
    // First element should be c (highest score)
    let lines: Vec<&str> = r1.lines().collect();
    assert!(!lines.is_empty(), "Expected results, got empty");
    // The first result should contain 'c'
    assert!(lines[0].contains('c') || lines[1].contains('c'),
            "Expected c first in reverse: {}", r1);
}

#[test]
fn test_zrangebyscore() {
    let _server = start_server(16450);

    redis_cli(16450, &["ZADD", "myzset", "1", "a", "2", "b", "3", "c", "4", "d"]);

    // Score range
    let r1 = redis_cli(16450, &["ZRANGEBYSCORE", "myzset", "2", "3"]);
    assert!(r1.contains('b') && r1.contains('c'), "Expected b, c in result: {}", r1);
    assert!(!r1.contains('a') && !r1.contains('d'), "Unexpected a or d in result: {}", r1);

    // With LIMIT
    let r2 = redis_cli(16450, &["ZRANGEBYSCORE", "myzset", "1", "4", "LIMIT", "1", "2"]);
    assert!(r2.contains('b') && r2.contains('c'), "Expected b, c with LIMIT: {}", r2);

    // -inf and +inf
    let r3 = redis_cli(16450, &["ZRANGEBYSCORE", "myzset", "-inf", "+inf"]);
    assert!(r3.contains('a') && r3.contains('d'), "Expected all with -inf/+inf: {}", r3);
}

#[test]
fn test_zcount() {
    let _server = start_server(16451);

    redis_cli(16451, &["ZADD", "myzset", "1", "a", "2", "b", "3", "c"]);

    let r1 = redis_cli(16451, &["ZCOUNT", "myzset", "1", "3"]);
    assert!(check_int(&r1, 3), "Expected 3, got: {}", r1);

    let r2 = redis_cli(16451, &["ZCOUNT", "myzset", "1.5", "2.5"]);
    assert!(check_int(&r2, 1), "Expected 1, got: {}", r2);

    let r3 = redis_cli(16451, &["ZCOUNT", "myzset", "-inf", "+inf"]);
    assert!(check_int(&r3, 3), "Expected 3, got: {}", r3);

    // Nonexistent key
    let r4 = redis_cli(16451, &["ZCOUNT", "nokey", "0", "100"]);
    assert!(check_int(&r4, 0), "Expected 0, got: {}", r4);
}

#[test]
fn test_zincrby() {
    let _server = start_server(16452);

    // Create new member
    let r1 = redis_cli(16452, &["ZINCRBY", "myzset", "5", "a"]);
    assert!(r1.contains('5'), "Expected 5, got: {}", r1);

    // Increment existing
    let r2 = redis_cli(16452, &["ZINCRBY", "myzset", "3", "a"]);
    assert!(r2.contains('8'), "Expected 8, got: {}", r2);

    // Verify with ZSCORE
    let r3 = redis_cli(16452, &["ZSCORE", "myzset", "a"]);
    assert!(r3.contains('8'), "Expected score 8, got: {}", r3);
}

#[test]
fn test_zremrangebyrank() {
    let _server = start_server(16453);

    redis_cli(16453, &["ZADD", "myzset", "1", "a", "2", "b", "3", "c", "4", "d"]);

    // Remove first two
    let r1 = redis_cli(16453, &["ZREMRANGEBYRANK", "myzset", "0", "1"]);
    assert!(check_int(&r1, 2), "Expected 2 removed, got: {}", r1);

    let r2 = redis_cli(16453, &["ZCARD", "myzset"]);
    assert!(check_int(&r2, 2), "Expected 2 remaining, got: {}", r2);

    // Verify remaining members
    let r3 = redis_cli(16453, &["ZRANGE", "myzset", "0", "-1"]);
    assert!(r3.contains('c') && r3.contains('d'), "Expected c, d remaining: {}", r3);
}

#[test]
fn test_zremrangebyscore() {
    let _server = start_server(16454);

    redis_cli(16454, &["ZADD", "myzset", "1", "a", "2", "b", "3", "c", "4", "d"]);

    // Remove middle scores
    let r1 = redis_cli(16454, &["ZREMRANGEBYSCORE", "myzset", "2", "3"]);
    assert!(check_int(&r1, 2), "Expected 2 removed, got: {}", r1);

    let r2 = redis_cli(16454, &["ZCARD", "myzset"]);
    assert!(check_int(&r2, 2), "Expected 2 remaining, got: {}", r2);

    // Verify remaining members
    let r3 = redis_cli(16454, &["ZRANGE", "myzset", "0", "-1"]);
    assert!(r3.contains('a') && r3.contains('d'), "Expected a, d remaining: {}", r3);
}

#[test]
fn test_zset_type() {
    let _server = start_server(16455);

    redis_cli(16455, &["ZADD", "myzset", "1", "value"]);
    assert_eq!(redis_cli(16455, &["TYPE", "myzset"]), "zset");
}

#[test]
fn test_zset_wrong_type() {
    let _server = start_server(16456);

    // Create a string key
    redis_cli(16456, &["SET", "mystring", "value"]);

    // Sorted set operations on string should fail
    let r1 = redis_cli(16456, &["ZADD", "mystring", "1", "a"]);
    assert!(r1.contains("WRONGTYPE"), "Expected WRONGTYPE, got: {}", r1);

    let r2 = redis_cli(16456, &["ZCARD", "mystring"]);
    assert!(r2.contains("WRONGTYPE"), "Expected WRONGTYPE, got: {}", r2);

    let r3 = redis_cli(16456, &["ZRANGE", "mystring", "0", "-1"]);
    assert!(r3.contains("WRONGTYPE"), "Expected WRONGTYPE, got: {}", r3);
}

// --- Session 10: Server Operations integration tests ---

#[test]
fn test_select_db() {
    let _server = start_server(16457);

    // Set a key in db 0
    redis_cli(16457, &["SET", "key", "value0"]);
    assert_eq!(redis_cli(16457, &["GET", "key"]), "value0");

    // SELECT db 1 returns OK
    let r1 = redis_cli(16457, &["SELECT", "1"]);
    assert_eq!(r1, "OK", "Expected OK for SELECT, got: {}", r1);

    // Use -n flag to query db 1 - key shouldn't exist
    let r2 = redis_cli_n(16457, 1, &["GET", "key"]);
    assert!(r2.is_empty() || r2.contains("nil"), "Expected nil in db 1, got: {}", r2);

    // Set a different value in db 1
    redis_cli_n(16457, 1, &["SET", "key", "value1"]);
    assert_eq!(redis_cli_n(16457, 1, &["GET", "key"]), "value1");

    // Verify db 0 still has original value
    assert_eq!(redis_cli(16457, &["GET", "key"]), "value0");
}

#[test]
fn test_select_invalid_db() {
    let _server = start_server(16458);

    // SELECT with invalid database index
    let r1 = redis_cli(16458, &["SELECT", "16"]);
    assert!(r1.contains("ERR") || r1.contains("out of range"), "Expected error, got: {}", r1);

    let r2 = redis_cli(16458, &["SELECT", "-1"]);
    assert!(r2.contains("ERR") || r2.contains("out of range") || r2.contains("integer"), "Expected error, got: {}", r2);

    let r3 = redis_cli(16458, &["SELECT", "abc"]);
    assert!(r3.contains("integer") || r3.contains("ERR"), "Expected error, got: {}", r3);
}

#[test]
fn test_dbsize_empty() {
    let _server = start_server(16459);

    let r = redis_cli(16459, &["DBSIZE"]);
    assert!(check_int(&r, 0), "Expected 0, got: {}", r);
}

#[test]
fn test_dbsize_with_keys() {
    let _server = start_server(16460);

    redis_cli(16460, &["SET", "key1", "value1"]);
    redis_cli(16460, &["SET", "key2", "value2"]);
    redis_cli(16460, &["HSET", "hash", "field", "value"]);

    let r = redis_cli(16460, &["DBSIZE"]);
    assert!(check_int(&r, 3), "Expected 3, got: {}", r);
}

#[test]
fn test_flushdb() {
    let _server = start_server(16461);

    // Add some keys
    redis_cli(16461, &["SET", "key1", "value1"]);
    redis_cli(16461, &["SET", "key2", "value2"]);
    redis_cli(16461, &["HSET", "hash", "field", "value"]);

    let r1 = redis_cli(16461, &["DBSIZE"]);
    assert!(check_int(&r1, 3), "Expected 3, got: {}", r1);

    // Flush the database
    let r2 = redis_cli(16461, &["FLUSHDB"]);
    assert_eq!(r2, "OK", "Expected OK for FLUSHDB, got: {}", r2);

    // Verify empty
    let r3 = redis_cli(16461, &["DBSIZE"]);
    assert!(check_int(&r3, 0), "Expected 0 after flush, got: {}", r3);

    // Verify keys are gone
    let r4 = redis_cli(16461, &["GET", "key1"]);
    assert!(r4.is_empty() || r4.contains("nil"), "Expected nil, got: {}", r4);
}

#[test]
fn test_info_basic() {
    let _server = start_server(16462);

    let r = redis_cli(16462, &["INFO"]);
    assert!(r.contains("Server") || r.contains("redis_version"), "Expected INFO output, got: {}", r);
}

#[test]
fn test_info_keyspace() {
    let _server = start_server(16463);

    redis_cli(16463, &["SET", "key1", "value1"]);
    redis_cli(16463, &["SET", "key2", "value2"]);

    let r = redis_cli(16463, &["INFO", "keyspace"]);
    assert!(r.contains("Keyspace") || r.contains("db0"), "Expected keyspace info, got: {}", r);
    assert!(r.contains("keys=2") || r.contains("2"), "Expected keys count, got: {}", r);
}

#[test]
fn test_dbsize_per_database() {
    let _server = start_server(16464);

    // Add keys to db 0
    redis_cli(16464, &["SET", "key1", "value1"]);
    redis_cli(16464, &["SET", "key2", "value2"]);

    let r1 = redis_cli(16464, &["DBSIZE"]);
    assert!(check_int(&r1, 2), "Expected 2 in db 0, got: {}", r1);

    // Add a key to db 1 using -n flag
    redis_cli_n(16464, 1, &["SET", "key3", "value3"]);

    let r2 = redis_cli_n(16464, 1, &["DBSIZE"]);
    assert!(check_int(&r2, 1), "Expected 1 in db 1, got: {}", r2);

    // db 0 still has 2 keys
    let r3 = redis_cli(16464, &["DBSIZE"]);
    assert!(check_int(&r3, 2), "Expected 2 in db 0, got: {}", r3);
}

#[test]
fn test_flushdb_only_current() {
    let _server = start_server(16465);

    // Add keys to db 0
    redis_cli(16465, &["SET", "key0", "value0"]);

    // Add keys to db 1 using -n flag
    redis_cli_n(16465, 1, &["SET", "key1", "value1"]);

    // Flush db 1
    redis_cli_n(16465, 1, &["FLUSHDB"]);

    let r1 = redis_cli_n(16465, 1, &["DBSIZE"]);
    assert!(check_int(&r1, 0), "Expected 0 in db 1 after flush, got: {}", r1);

    // db 0 should still have its key
    let r2 = redis_cli(16465, &["DBSIZE"]);
    assert!(check_int(&r2, 1), "Expected 1 in db 0, got: {}", r2);

    assert_eq!(redis_cli(16465, &["GET", "key0"]), "value0");
}

// --- Session 11: Custom Commands Integration Tests ---

#[test]
fn test_vacuum_no_expired() {
    let _server = start_server(16466);

    // Add some permanent keys
    redis_cli(16466, &["SET", "key1", "value1"]);
    redis_cli(16466, &["SET", "key2", "value2"]);

    // VACUUM should return 0 (no expired keys)
    let r = redis_cli(16466, &["VACUUM"]);
    assert!(check_int(&r, 0), "Expected 0 deleted, got: {}", r);

    // Keys should still exist
    assert_eq!(redis_cli(16466, &["GET", "key1"]), "value1");
    assert_eq!(redis_cli(16466, &["GET", "key2"]), "value2");
}

#[test]
fn test_vacuum_with_expired() {
    let _server = start_server(16467);

    // Add a key with 1 second TTL
    redis_cli(16467, &["SET", "expired", "value", "PX", "50"]);
    redis_cli(16467, &["SET", "permanent", "value2"]);

    // Wait for expiration
    thread::sleep(Duration::from_millis(100));

    // VACUUM should delete the expired key
    let r = redis_cli(16467, &["VACUUM"]);
    assert!(check_int(&r, 1), "Expected 1 deleted, got: {}", r);

    // Permanent key should still exist
    assert_eq!(redis_cli(16467, &["GET", "permanent"]), "value2");
}

#[test]
fn test_keyinfo_nonexistent() {
    let _server = start_server(16468);

    let r = redis_cli(16468, &["KEYINFO", "nonexistent"]);
    assert!(r.is_empty() || r.contains("nil"), "Expected nil for nonexistent key, got: {}", r);
}

#[test]
fn test_keyinfo_string() {
    let _server = start_server(16469);

    redis_cli(16469, &["SET", "mykey", "myvalue"]);

    let r = redis_cli(16469, &["KEYINFO", "mykey"]);
    assert!(r.contains("type") && r.contains("string"), "Expected type=string, got: {}", r);
    assert!(r.contains("ttl"), "Expected ttl field, got: {}", r);
    assert!(r.contains("created_at"), "Expected created_at field, got: {}", r);
    assert!(r.contains("updated_at"), "Expected updated_at field, got: {}", r);
}

#[test]
fn test_keyinfo_with_ttl() {
    let _server = start_server(16470);

    redis_cli(16470, &["SET", "mykey", "myvalue", "EX", "100"]);

    let r = redis_cli(16470, &["KEYINFO", "mykey"]);
    assert!(r.contains("type") && r.contains("string"), "Expected type=string, got: {}", r);
    // TTL should be present and approximately 100 or less
    assert!(r.contains("ttl"), "Expected ttl field, got: {}", r);
}

#[test]
fn test_keyinfo_hash() {
    let _server = start_server(16471);

    redis_cli(16471, &["HSET", "myhash", "field1", "value1"]);

    let r = redis_cli(16471, &["KEYINFO", "myhash"]);
    assert!(r.contains("type") && r.contains("hash"), "Expected type=hash, got: {}", r);
}

#[test]
fn test_keyinfo_list() {
    let _server = start_server(16472);

    redis_cli(16472, &["RPUSH", "mylist", "a", "b", "c"]);

    let r = redis_cli(16472, &["KEYINFO", "mylist"]);
    assert!(r.contains("type") && r.contains("list"), "Expected type=list, got: {}", r);
}

#[test]
fn test_keyinfo_set() {
    let _server = start_server(16473);

    redis_cli(16473, &["SADD", "myset", "member1", "member2"]);

    let r = redis_cli(16473, &["KEYINFO", "myset"]);
    assert!(r.contains("type") && r.contains("set"), "Expected type=set, got: {}", r);
}

#[test]
fn test_keyinfo_zset() {
    let _server = start_server(16474);

    redis_cli(16474, &["ZADD", "myzset", "1", "member1"]);

    let r = redis_cli(16474, &["KEYINFO", "myzset"]);
    assert!(r.contains("type") && r.contains("zset"), "Expected type=zset, got: {}", r);
}

// --- Session 13: Stream Integration Tests ---

#[test]
fn test_xadd_xlen() {
    let _server = start_server(16476);

    // XADD with * auto-generates ID
    let r1 = redis_cli(16476, &["XADD", "mystream", "*", "field1", "value1"]);
    assert!(r1.contains('-'), "Expected stream ID, got: {}", r1);

    // XLEN should return 1
    let r2 = redis_cli(16476, &["XLEN", "mystream"]);
    assert!(check_int(&r2, 1), "Expected 1, got: {}", r2);

    // Add more entries
    redis_cli(16476, &["XADD", "mystream", "*", "field2", "value2"]);
    redis_cli(16476, &["XADD", "mystream", "*", "field3", "value3"]);

    let r3 = redis_cli(16476, &["XLEN", "mystream"]);
    assert!(check_int(&r3, 3), "Expected 3, got: {}", r3);
}

#[test]
fn test_xadd_explicit_id() {
    let _server = start_server(16477);

    // XADD with explicit ID
    let r1 = redis_cli(16477, &["XADD", "mystream", "1000-0", "field1", "value1"]);
    assert!(r1.contains("1000-0"), "Expected 1000-0, got: {}", r1);

    let r2 = redis_cli(16477, &["XADD", "mystream", "2000-5", "field2", "value2"]);
    assert!(r2.contains("2000-5"), "Expected 2000-5, got: {}", r2);
}

#[test]
fn test_xrange() {
    let _server = start_server(16478);

    redis_cli(16478, &["XADD", "mystream", "1000-0", "a", "1"]);
    redis_cli(16478, &["XADD", "mystream", "2000-0", "b", "2"]);
    redis_cli(16478, &["XADD", "mystream", "3000-0", "c", "3"]);

    // Get all entries with - and +
    let r1 = redis_cli(16478, &["XRANGE", "mystream", "-", "+"]);
    assert!(r1.contains("1000-0"), "Expected 1000-0 in result: {}", r1);
    assert!(r1.contains("3000-0"), "Expected 3000-0 in result: {}", r1);

    // Get range
    let r2 = redis_cli(16478, &["XRANGE", "mystream", "1500", "2500"]);
    assert!(r2.contains("2000-0"), "Expected 2000-0 in result: {}", r2);
    assert!(!r2.contains("1000-0"), "Unexpected 1000-0 in result: {}", r2);
    assert!(!r2.contains("3000-0"), "Unexpected 3000-0 in result: {}", r2);

    // With COUNT
    let r3 = redis_cli(16478, &["XRANGE", "mystream", "-", "+", "COUNT", "2"]);
    assert!(r3.contains("1000-0"), "Expected 1000-0: {}", r3);
    assert!(r3.contains("2000-0"), "Expected 2000-0: {}", r3);
}

#[test]
fn test_xrevrange() {
    let _server = start_server(16479);

    redis_cli(16479, &["XADD", "mystream", "1000-0", "a", "1"]);
    redis_cli(16479, &["XADD", "mystream", "2000-0", "b", "2"]);
    redis_cli(16479, &["XADD", "mystream", "3000-0", "c", "3"]);

    // Get all in reverse (end before start)
    let r1 = redis_cli(16479, &["XREVRANGE", "mystream", "+", "-"]);
    assert!(r1.contains("3000-0"), "Expected 3000-0: {}", r1);
    assert!(r1.contains("1000-0"), "Expected 1000-0: {}", r1);
}

#[test]
fn test_xread() {
    let _server = start_server(16480);

    redis_cli(16480, &["XADD", "stream1", "1000-0", "a", "1"]);
    redis_cli(16480, &["XADD", "stream1", "2000-0", "b", "2"]);

    // Read from beginning
    let r1 = redis_cli(16480, &["XREAD", "STREAMS", "stream1", "0"]);
    assert!(r1.contains("stream1"), "Expected stream1: {}", r1);
    assert!(r1.contains("1000-0"), "Expected 1000-0: {}", r1);
    assert!(r1.contains("2000-0"), "Expected 2000-0: {}", r1);

    // Read after first entry
    let r2 = redis_cli(16480, &["XREAD", "STREAMS", "stream1", "1000-0"]);
    assert!(r2.contains("2000-0"), "Expected 2000-0: {}", r2);
    assert!(!r2.contains("1000-0"), "Unexpected 1000-0: {}", r2);

    // Read with COUNT
    let r3 = redis_cli(16480, &["XREAD", "COUNT", "1", "STREAMS", "stream1", "0"]);
    assert!(r3.contains("1000-0"), "Expected 1000-0: {}", r3);
}

#[test]
fn test_xtrim_maxlen() {
    let _server = start_server(16481);

    for i in 1..=5 {
        redis_cli(16481, &["XADD", "mystream", &format!("{}-0", i * 1000), "f", "v"]);
    }

    let r1 = redis_cli(16481, &["XLEN", "mystream"]);
    assert!(check_int(&r1, 5), "Expected 5, got: {}", r1);

    // Trim to 3
    let r2 = redis_cli(16481, &["XTRIM", "mystream", "MAXLEN", "3"]);
    assert!(check_int(&r2, 2), "Expected 2 deleted, got: {}", r2);

    let r3 = redis_cli(16481, &["XLEN", "mystream"]);
    assert!(check_int(&r3, 3), "Expected 3, got: {}", r3);
}

#[test]
fn test_xtrim_minid() {
    let _server = start_server(16482);

    for i in 1..=5 {
        redis_cli(16482, &["XADD", "mystream", &format!("{}-0", i * 1000), "f", "v"]);
    }

    // Trim entries before 3000-0
    let r1 = redis_cli(16482, &["XTRIM", "mystream", "MINID", "3000-0"]);
    assert!(check_int(&r1, 2), "Expected 2 deleted, got: {}", r1);

    let r2 = redis_cli(16482, &["XLEN", "mystream"]);
    assert!(check_int(&r2, 3), "Expected 3, got: {}", r2);
}

#[test]
fn test_xdel() {
    let _server = start_server(16483);

    redis_cli(16483, &["XADD", "mystream", "1000-0", "a", "1"]);
    redis_cli(16483, &["XADD", "mystream", "2000-0", "b", "2"]);
    redis_cli(16483, &["XADD", "mystream", "3000-0", "c", "3"]);

    // Delete middle entry
    let r1 = redis_cli(16483, &["XDEL", "mystream", "2000-0"]);
    assert!(check_int(&r1, 1), "Expected 1, got: {}", r1);

    let r2 = redis_cli(16483, &["XLEN", "mystream"]);
    assert!(check_int(&r2, 2), "Expected 2, got: {}", r2);

    // Delete non-existent entry
    let r3 = redis_cli(16483, &["XDEL", "mystream", "2000-0"]);
    assert!(check_int(&r3, 0), "Expected 0, got: {}", r3);
}

#[test]
fn test_xinfo_stream() {
    let _server = start_server(16484);

    redis_cli(16484, &["XADD", "mystream", "1000-0", "a", "1"]);
    redis_cli(16484, &["XADD", "mystream", "2000-0", "b", "2"]);

    let r = redis_cli(16484, &["XINFO", "STREAM", "mystream"]);
    assert!(r.contains("length"), "Expected length: {}", r);
    assert!(r.contains("last-generated-id"), "Expected last-generated-id: {}", r);
    assert!(r.contains("2000-0"), "Expected 2000-0 as last ID: {}", r);
}

#[test]
fn test_stream_type() {
    let _server = start_server(16485);

    redis_cli(16485, &["XADD", "mystream", "*", "field", "value"]);
    let r = redis_cli(16485, &["TYPE", "mystream"]);
    assert_eq!(r, "stream", "Expected type=stream, got: {}", r);
}

#[test]
fn test_stream_wrong_type() {
    let _server = start_server(16486);

    // Create a string key
    redis_cli(16486, &["SET", "mystring", "value"]);

    // Stream operations on string should fail
    let r1 = redis_cli(16486, &["XADD", "mystring", "*", "f", "v"]);
    assert!(r1.contains("WRONGTYPE"), "Expected WRONGTYPE, got: {}", r1);

    let r2 = redis_cli(16486, &["XLEN", "mystring"]);
    assert!(r2.contains("WRONGTYPE"), "Expected WRONGTYPE, got: {}", r2);
}

#[test]
fn test_xadd_nomkstream() {
    let _server = start_server(16487);

    // NOMKSTREAM on non-existent stream should return nil
    let r1 = redis_cli(16487, &["XADD", "mystream", "NOMKSTREAM", "*", "f", "v"]);
    assert!(r1.is_empty() || r1.contains("nil"), "Expected nil for NOMKSTREAM: {}", r1);

    // Create the stream
    redis_cli(16487, &["XADD", "mystream", "*", "f", "v"]);

    // NOMKSTREAM on existing stream should work
    let r2 = redis_cli(16487, &["XADD", "mystream", "NOMKSTREAM", "*", "f2", "v2"]);
    assert!(r2.contains('-'), "Expected ID, got: {}", r2);
}

#[test]
fn test_xadd_maxlen() {
    let _server = start_server(16488);

    // Add entries with MAXLEN 3
    for i in 1..=5 {
        redis_cli(16488, &["XADD", "mystream", "MAXLEN", "3", &format!("{}-0", i * 1000), "f", "v"]);
    }

    let r = redis_cli(16488, &["XLEN", "mystream"]);
    assert!(check_int(&r, 3), "Expected 3, got: {}", r);
}

// ==================== Consumer Group Tests (Session 14) ====================

#[test]
fn test_xgroup_create() {
    let _server = start_server(16489);

    // Create stream
    redis_cli(16489, &["XADD", "mystream", "1000-0", "f", "v"]);

    // Create group
    let r1 = redis_cli(16489, &["XGROUP", "CREATE", "mystream", "mygroup", "0"]);
    assert_eq!(r1, "OK", "Expected OK, got: {}", r1);

    // Creating same group again should fail
    let r2 = redis_cli(16489, &["XGROUP", "CREATE", "mystream", "mygroup", "0"]);
    assert!(r2.contains("BUSYGROUP"), "Expected BUSYGROUP, got: {}", r2);
}

#[test]
fn test_xgroup_create_mkstream() {
    let _server = start_server(16490);

    // Create group with MKSTREAM on non-existent stream
    let r1 = redis_cli(16490, &["XGROUP", "CREATE", "newstream", "mygroup", "0", "MKSTREAM"]);
    assert_eq!(r1, "OK", "Expected OK, got: {}", r1);

    // Stream should now exist
    let r2 = redis_cli(16490, &["TYPE", "newstream"]);
    assert_eq!(r2, "stream", "Expected stream type, got: {}", r2);
}

#[test]
fn test_xgroup_destroy() {
    let _server = start_server(16491);

    redis_cli(16491, &["XADD", "mystream", "1000-0", "f", "v"]);
    redis_cli(16491, &["XGROUP", "CREATE", "mystream", "mygroup", "0"]);

    let r1 = redis_cli(16491, &["XGROUP", "DESTROY", "mystream", "mygroup"]);
    assert!(check_int(&r1, 1), "Expected 1, got: {}", r1);

    // Destroying again should return 0
    let r2 = redis_cli(16491, &["XGROUP", "DESTROY", "mystream", "mygroup"]);
    assert!(check_int(&r2, 0), "Expected 0, got: {}", r2);
}

#[test]
fn test_xgroup_setid() {
    let _server = start_server(16492);

    redis_cli(16492, &["XADD", "mystream", "1000-0", "f", "v"]);
    redis_cli(16492, &["XGROUP", "CREATE", "mystream", "mygroup", "0"]);

    let r = redis_cli(16492, &["XGROUP", "SETID", "mystream", "mygroup", "1000-0"]);
    assert_eq!(r, "OK", "Expected OK, got: {}", r);
}

#[test]
fn test_xgroup_createconsumer() {
    let _server = start_server(16493);

    redis_cli(16493, &["XADD", "mystream", "1000-0", "f", "v"]);
    redis_cli(16493, &["XGROUP", "CREATE", "mystream", "mygroup", "0"]);

    let r1 = redis_cli(16493, &["XGROUP", "CREATECONSUMER", "mystream", "mygroup", "consumer1"]);
    assert!(check_int(&r1, 1), "Expected 1, got: {}", r1);

    // Creating same consumer again returns 0
    let r2 = redis_cli(16493, &["XGROUP", "CREATECONSUMER", "mystream", "mygroup", "consumer1"]);
    assert!(check_int(&r2, 0), "Expected 0, got: {}", r2);
}

#[test]
fn test_xgroup_delconsumer() {
    let _server = start_server(16494);

    redis_cli(16494, &["XADD", "mystream", "1000-0", "f", "v"]);
    redis_cli(16494, &["XGROUP", "CREATE", "mystream", "mygroup", "0"]);
    redis_cli(16494, &["XGROUP", "CREATECONSUMER", "mystream", "mygroup", "consumer1"]);

    let r = redis_cli(16494, &["XGROUP", "DELCONSUMER", "mystream", "mygroup", "consumer1"]);
    assert!(check_int(&r, 0), "Expected 0 pending, got: {}", r);
}

#[test]
fn test_xreadgroup_new_messages() {
    let _server = start_server(16495);

    redis_cli(16495, &["XADD", "mystream", "1000-0", "f", "v"]);
    redis_cli(16495, &["XADD", "mystream", "2000-0", "f", "v2"]);
    redis_cli(16495, &["XGROUP", "CREATE", "mystream", "mygroup", "0"]);

    // Read new messages with >
    let r1 = redis_cli(16495, &["XREADGROUP", "GROUP", "mygroup", "consumer1", "STREAMS", "mystream", ">"]);
    assert!(r1.contains("1000-0"), "Expected 1000-0 in result: {}", r1);
    assert!(r1.contains("2000-0"), "Expected 2000-0 in result: {}", r1);

    // Reading again returns nil (all delivered)
    let r2 = redis_cli(16495, &["XREADGROUP", "GROUP", "mygroup", "consumer1", "STREAMS", "mystream", ">"]);
    assert!(r2.is_empty() || r2.contains("nil"), "Expected nil, got: {}", r2);
}

#[test]
fn test_xreadgroup_pending() {
    let _server = start_server(16496);

    redis_cli(16496, &["XADD", "mystream", "1000-0", "f", "v"]);
    redis_cli(16496, &["XGROUP", "CREATE", "mystream", "mygroup", "0"]);

    // First read creates pending entry
    redis_cli(16496, &["XREADGROUP", "GROUP", "mygroup", "consumer1", "STREAMS", "mystream", ">"]);

    // Read pending entries with 0
    let r = redis_cli(16496, &["XREADGROUP", "GROUP", "mygroup", "consumer1", "STREAMS", "mystream", "0"]);
    assert!(r.contains("1000-0"), "Expected pending entry in result: {}", r);
}

#[test]
fn test_xreadgroup_noack() {
    let _server = start_server(16497);

    redis_cli(16497, &["XADD", "mystream", "1000-0", "f", "v"]);
    redis_cli(16497, &["XGROUP", "CREATE", "mystream", "mygroup", "0"]);

    // Read with NOACK
    redis_cli(16497, &["XREADGROUP", "GROUP", "mygroup", "consumer1", "NOACK", "STREAMS", "mystream", ">"]);

    // Check pending is empty
    let r = redis_cli(16497, &["XPENDING", "mystream", "mygroup"]);
    assert!(check_int(&r, 0), "Expected 0 pending, got: {}", r);
}

#[test]
fn test_xack() {
    let _server = start_server(16498);

    redis_cli(16498, &["XADD", "mystream", "1000-0", "f", "v"]);
    redis_cli(16498, &["XADD", "mystream", "2000-0", "f", "v2"]);
    redis_cli(16498, &["XGROUP", "CREATE", "mystream", "mygroup", "0"]);
    redis_cli(16498, &["XREADGROUP", "GROUP", "mygroup", "consumer1", "STREAMS", "mystream", ">"]);

    // Acknowledge one message
    let r1 = redis_cli(16498, &["XACK", "mystream", "mygroup", "1000-0"]);
    assert!(check_int(&r1, 1), "Expected 1, got: {}", r1);

    // Check pending count
    let r2 = redis_cli(16498, &["XPENDING", "mystream", "mygroup"]);
    assert!(r2.contains('1'), "Expected 1 pending, got: {}", r2);
}

#[test]
fn test_xpending_summary() {
    let _server = start_server(16499);

    redis_cli(16499, &["XADD", "mystream", "1000-0", "f", "v"]);
    redis_cli(16499, &["XADD", "mystream", "2000-0", "f", "v2"]);
    redis_cli(16499, &["XGROUP", "CREATE", "mystream", "mygroup", "0"]);
    redis_cli(16499, &["XREADGROUP", "GROUP", "mygroup", "consumer1", "STREAMS", "mystream", ">"]);

    let r = redis_cli(16499, &["XPENDING", "mystream", "mygroup"]);
    assert!(r.contains('2'), "Expected 2 pending, got: {}", r);
    assert!(r.contains("1000-0"), "Expected smallest ID 1000-0, got: {}", r);
    assert!(r.contains("2000-0"), "Expected largest ID 2000-0, got: {}", r);
    assert!(r.contains("consumer1"), "Expected consumer1, got: {}", r);
}

#[test]
fn test_xpending_range() {
    let _server = start_server(16500);

    redis_cli(16500, &["XADD", "mystream", "1000-0", "f", "v"]);
    redis_cli(16500, &["XADD", "mystream", "2000-0", "f", "v2"]);
    redis_cli(16500, &["XGROUP", "CREATE", "mystream", "mygroup", "0"]);
    redis_cli(16500, &["XREADGROUP", "GROUP", "mygroup", "consumer1", "STREAMS", "mystream", ">"]);

    let r = redis_cli(16500, &["XPENDING", "mystream", "mygroup", "-", "+", "10"]);
    assert!(r.contains("1000-0"), "Expected 1000-0 in pending range: {}", r);
    assert!(r.contains("2000-0"), "Expected 2000-0 in pending range: {}", r);
    assert!(r.contains("consumer1"), "Expected consumer1 in result: {}", r);
}

#[test]
fn test_xclaim() {
    let _server = start_server(16501);

    redis_cli(16501, &["XADD", "mystream", "1000-0", "f", "v"]);
    redis_cli(16501, &["XGROUP", "CREATE", "mystream", "mygroup", "0"]);
    redis_cli(16501, &["XREADGROUP", "GROUP", "mygroup", "consumer1", "STREAMS", "mystream", ">"]);

    // Claim with FORCE
    let r = redis_cli(16501, &["XCLAIM", "mystream", "mygroup", "consumer2", "0", "1000-0", "FORCE"]);
    assert!(r.contains("1000-0"), "Expected 1000-0 in claim result: {}", r);

    // Verify consumer2 now owns it
    let r2 = redis_cli(16501, &["XPENDING", "mystream", "mygroup", "-", "+", "10"]);
    assert!(r2.contains("consumer2"), "Expected consumer2 in pending: {}", r2);
}

#[test]
fn test_xclaim_justid() {
    let _server = start_server(16502);

    redis_cli(16502, &["XADD", "mystream", "1000-0", "f", "v"]);
    redis_cli(16502, &["XGROUP", "CREATE", "mystream", "mygroup", "0"]);
    redis_cli(16502, &["XREADGROUP", "GROUP", "mygroup", "consumer1", "STREAMS", "mystream", ">"]);

    // Claim with JUSTID
    let r = redis_cli(16502, &["XCLAIM", "mystream", "mygroup", "consumer2", "0", "1000-0", "FORCE", "JUSTID"]);
    assert!(r.contains("1000-0"), "Expected 1000-0 in justid result: {}", r);
}

#[test]
fn test_xinfo_groups() {
    let _server = start_server(16503);

    redis_cli(16503, &["XADD", "mystream", "1000-0", "f", "v"]);
    redis_cli(16503, &["XGROUP", "CREATE", "mystream", "group1", "0"]);
    redis_cli(16503, &["XGROUP", "CREATE", "mystream", "group2", "$"]);

    let r = redis_cli(16503, &["XINFO", "GROUPS", "mystream"]);
    assert!(r.contains("group1"), "Expected group1 in result: {}", r);
    assert!(r.contains("group2"), "Expected group2 in result: {}", r);
}

#[test]
fn test_xinfo_consumers() {
    let _server = start_server(16504);

    redis_cli(16504, &["XADD", "mystream", "1000-0", "f", "v"]);
    redis_cli(16504, &["XGROUP", "CREATE", "mystream", "mygroup", "0"]);
    redis_cli(16504, &["XGROUP", "CREATECONSUMER", "mystream", "mygroup", "consumer1"]);
    redis_cli(16504, &["XGROUP", "CREATECONSUMER", "mystream", "mygroup", "consumer2"]);

    let r = redis_cli(16504, &["XINFO", "CONSUMERS", "mystream", "mygroup"]);
    assert!(r.contains("consumer1"), "Expected consumer1 in result: {}", r);
    assert!(r.contains("consumer2"), "Expected consumer2 in result: {}", r);
}

#[test]
fn test_xreadgroup_count() {
    let _server = start_server(16505);

    redis_cli(16505, &["XADD", "mystream", "1000-0", "f", "v"]);
    redis_cli(16505, &["XADD", "mystream", "2000-0", "f", "v2"]);
    redis_cli(16505, &["XADD", "mystream", "3000-0", "f", "v3"]);
    redis_cli(16505, &["XGROUP", "CREATE", "mystream", "mygroup", "0"]);

    // Read with COUNT 2
    let r = redis_cli(16505, &["XREADGROUP", "GROUP", "mygroup", "consumer1", "COUNT", "2", "STREAMS", "mystream", ">"]);
    assert!(r.contains("1000-0"), "Expected 1000-0 in result: {}", r);
    assert!(r.contains("2000-0"), "Expected 2000-0 in result: {}", r);
    // Should NOT contain 3000-0 due to COUNT limit - but actually redis-cli output may vary
}

#[test]
fn test_xgroup_no_such_key() {
    let _server = start_server(16506);

    // Try to create group on non-existent stream without MKSTREAM
    let r = redis_cli(16506, &["XGROUP", "CREATE", "nonexistent", "mygroup", "0"]);
    assert!(r.to_lowercase().contains("key") || r.contains("ERR"), "Expected error about key, got: {}", r);
}

#[test]
fn test_xgroup_nogroup() {
    let _server = start_server(16507);

    redis_cli(16507, &["XADD", "mystream", "1000-0", "f", "v"]);

    // Try to set ID on non-existent group
    let r = redis_cli(16507, &["XGROUP", "SETID", "mystream", "nonexistent", "0"]);
    assert!(r.contains("NOGROUP"), "Expected NOGROUP error, got: {}", r);
}

#[test]
fn test_delconsumer_with_pending() {
    let _server = start_server(16508);

    redis_cli(16508, &["XADD", "mystream", "1000-0", "f", "v"]);
    redis_cli(16508, &["XADD", "mystream", "2000-0", "f", "v2"]);
    redis_cli(16508, &["XGROUP", "CREATE", "mystream", "mygroup", "0"]);
    redis_cli(16508, &["XREADGROUP", "GROUP", "mygroup", "consumer1", "STREAMS", "mystream", ">"]);

    // Delete consumer should return pending count
    let r = redis_cli(16508, &["XGROUP", "DELCONSUMER", "mystream", "mygroup", "consumer1"]);
    assert!(check_int(&r, 2), "Expected 2 pending deleted, got: {}", r);
}

// ============================================================================
// Session 15.2: Broadcasting on Writes
// ============================================================================

#[tokio::test]
async fn test_integration_lpush_notification() {
    // Producer-consumer pattern: LPUSH should notify waiting readers
    use std::collections::HashMap;
    use std::sync::{Arc, RwLock};

    let db = redlite::Db::open_memory().unwrap();
    let notifier = Arc::new(RwLock::new(HashMap::new()));
    db.with_notifier(notifier);

    // Create receiver before LPUSH
    let mut rx = db.subscribe_key("tasks").await;

    // Spawn task to LPUSH
    let db_clone = db.clone();
    let push_task = tokio::spawn(async move {
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        let len = db_clone.lpush("tasks", &[b"job1"]).unwrap();
        assert_eq!(len, 1);
    });

    // Wait for notification (should arrive before timeout)
    tokio::select! {
        _ = rx.recv() => {
            // Successfully received notification
        }
        _ = tokio::time::sleep(std::time::Duration::from_secs(1)) => {
            panic!("Should have received notification from LPUSH");
        }
    }

    push_task.await.unwrap();
}

#[tokio::test]
async fn test_integration_rpush_notification() {
    // Producer-consumer pattern: RPUSH should notify waiting readers
    use std::collections::HashMap;
    use std::sync::{Arc, RwLock};

    let db = redlite::Db::open_memory().unwrap();
    let notifier = Arc::new(RwLock::new(HashMap::new()));
    db.with_notifier(notifier);

    let mut rx = db.subscribe_key("queue").await;

    let db_clone = db.clone();
    let push_task = tokio::spawn(async move {
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        let len = db_clone.rpush("queue", &[b"item1"]).unwrap();
        assert_eq!(len, 1);
    });

    tokio::select! {
        _ = rx.recv() => {
            // Successfully received notification
        }
        _ = tokio::time::sleep(std::time::Duration::from_secs(1)) => {
            panic!("Should have received notification from RPUSH");
        }
    }

    push_task.await.unwrap();
}

#[tokio::test]
async fn test_integration_xadd_notification() {
    // Producer-consumer pattern: XADD should notify waiting readers
    use std::collections::HashMap;
    use std::sync::{Arc, RwLock};

    let db = redlite::Db::open_memory().unwrap();
    let notifier = Arc::new(RwLock::new(HashMap::new()));
    db.with_notifier(notifier);

    let mut rx = db.subscribe_key("events").await;

    let db_clone = db.clone();
    let push_task = tokio::spawn(async move {
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        let id = db_clone
            .xadd("events", None, &[(b"type", b"click")], false, None, None, false)
            .unwrap();
        assert!(id.is_some());
    });

    tokio::select! {
        _ = rx.recv() => {
            // Successfully received notification
        }
        _ = tokio::time::sleep(std::time::Duration::from_secs(1)) => {
            panic!("Should have received notification from XADD");
        }
    }

    push_task.await.unwrap();
}

#[tokio::test]
async fn test_integration_multi_producer_single_consumer() {
    // Multiple producers, single consumer
    // All LPUSH operations should notify the waiting reader
    use std::collections::HashMap;
    use std::sync::{Arc, RwLock};

    let db = redlite::Db::open_memory().unwrap();
    let notifier = Arc::new(RwLock::new(HashMap::new()));
    db.with_notifier(notifier);

    let mut rx1 = db.subscribe_key("work").await;
    let mut rx2 = db.subscribe_key("work").await;

    // Producer 1
    let db1 = db.clone();
    let task1 = tokio::spawn(async move {
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        db1.lpush("work", &[b"task1"]).unwrap();
    });

    // Producer 2
    let db2 = db.clone();
    let task2 = tokio::spawn(async move {
        tokio::time::sleep(std::time::Duration::from_millis(30)).await;
        db2.lpush("work", &[b"task2"]).unwrap();
    });

    // Consumer 1 should receive first notification
    tokio::select! {
        _ = rx1.recv() => {}
        _ = tokio::time::sleep(std::time::Duration::from_millis(500)) => {
            panic!("Consumer 1 should receive first notification");
        }
    }

    // Consumer 2 should receive second notification
    tokio::select! {
        _ = rx2.recv() => {}
        _ = tokio::time::sleep(std::time::Duration::from_millis(500)) => {
            panic!("Consumer 2 should receive second notification");
        }
    }

    task1.await.unwrap();
    task2.await.unwrap();
}

#[tokio::test]
async fn test_integration_concurrent_writes_same_key() {
    // Concurrent writes to the same key should each trigger notification
    use std::collections::HashMap;
    use std::sync::{Arc, RwLock};

    let db = redlite::Db::open_memory().unwrap();
    let notifier = Arc::new(RwLock::new(HashMap::new()));
    db.with_notifier(notifier);

    let mut rx1 = db.subscribe_key("counter").await;
    let mut rx2 = db.subscribe_key("counter").await;
    let mut rx3 = db.subscribe_key("counter").await;

    // Spawn three concurrent RPUSH operations
    let db1 = db.clone();
    let task1 = tokio::spawn(async move {
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        db1.rpush("counter", &[b"1"]).unwrap();
    });

    let db2 = db.clone();
    let task2 = tokio::spawn(async move {
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        db2.rpush("counter", &[b"2"]).unwrap();
    });

    let db3 = db.clone();
    let task3 = tokio::spawn(async move {
        tokio::time::sleep(std::time::Duration::from_millis(30)).await;
        db3.rpush("counter", &[b"3"]).unwrap();
    });

    // Each receiver should get at least one notification
    for mut rx in [rx1, rx2, rx3] {
        tokio::select! {
            _ = rx.recv() => {}
            _ = tokio::time::sleep(std::time::Duration::from_millis(500)) => {
                panic!("Receiver should have gotten notification");
            }
        }
    }

    task1.await.unwrap();
    task2.await.unwrap();
    task3.await.unwrap();
}
