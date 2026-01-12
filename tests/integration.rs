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
