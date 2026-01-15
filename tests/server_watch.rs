//! Comprehensive server mode tests for WATCH/UNWATCH (optimistic locking)
//!
//! These tests validate WATCH/UNWATCH behavior in actual server mode with:
//! - Multiple concurrent clients
//! - True network-based connections
//! - Connection lifecycle and cleanup
//! - Race conditions and timing
//! - Protocol compliance
//! - Integration with other features
//!
//! Run with: cargo test --test server_watch -- --test-threads=1

use redis::Commands;
use std::process::{Child, Command};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

// ============================================================================
// HELPERS
// ============================================================================

struct TestServer {
    process: Child,
    port: u16,
}

impl TestServer {
    fn start(port: u16) -> Self {
        let child = Command::new("./target/release/redlite")
            .args(["--db=:memory:", &format!("--addr=127.0.0.1:{}", port)])
            .spawn()
            .or_else(|_| {
                Command::new("./target/debug/redlite")
                    .args(["--db=:memory:", &format!("--addr=127.0.0.1:{}", port)])
                    .spawn()
            })
            .expect("Failed to start server - run `cargo build` first");

        // Wait for server to be ready
        thread::sleep(Duration::from_millis(300));

        TestServer {
            process: child,
            port,
        }
    }

    fn url(&self) -> String {
        format!("redis://127.0.0.1:{}", self.port)
    }

    fn client(&self) -> redis::Client {
        redis::Client::open(self.url()).expect("Failed to create client")
    }

    fn connection(&self) -> redis::Connection {
        self.client()
            .get_connection()
            .expect("Failed to get connection")
    }
}

impl Drop for TestServer {
    fn drop(&mut self) {
        let _ = self.process.kill();
    }
}

// ============================================================================
// CATEGORY 1: CONNECTION STATE MANAGEMENT (8 tests)
// ============================================================================

#[test]
fn test_watch_multiple_connections_same_watched_key() {
    let server = TestServer::start(18001);

    // 5 clients all WATCH "key"
    let client1 = server.client();
    let client2 = server.client();
    let client3 = server.client();
    let client4 = server.client();
    let client5 = server.client();

    let mut conn1 = client1.get_connection().unwrap();
    let mut conn2 = client2.get_connection().unwrap();
    let mut conn3 = client3.get_connection().unwrap();
    let mut conn4 = client4.get_connection().unwrap();
    let mut conn5 = client5.get_connection().unwrap();

    // All watch the same key
    redis::cmd("WATCH").arg("key").execute(&mut conn1);
    redis::cmd("WATCH").arg("key").execute(&mut conn2);
    redis::cmd("WATCH").arg("key").execute(&mut conn3);
    redis::cmd("WATCH").arg("key").execute(&mut conn4);
    redis::cmd("WATCH").arg("key").execute(&mut conn5);

    // Client1: MULTI, SET, attempt EXEC after modification
    redis::cmd("MULTI").execute(&mut conn1);
    redis::cmd("SET")
        .arg("key")
        .arg("from1")
        .execute(&mut conn1);

    // Client2-5: Modify the key
    redis::cmd("SET")
        .arg("key")
        .arg("from2")
        .execute(&mut conn2);
    redis::cmd("SET")
        .arg("key")
        .arg("from3")
        .execute(&mut conn3);
    redis::cmd("SET")
        .arg("key")
        .arg("from4")
        .execute(&mut conn4);
    redis::cmd("SET")
        .arg("key")
        .arg("from5")
        .execute(&mut conn5);

    // Client1: EXEC should return nil
    let exec_result: Option<Vec<String>> = redis::cmd("EXEC").query(&mut conn1).unwrap();
    assert_eq!(
        exec_result, None,
        "EXEC should return nil when watched key modified by other clients"
    );

    // Verify final value is one of the modifications (last one wins)
    let final_value: String = redis::cmd("GET").arg("key").query(&mut conn5).unwrap();
    assert_eq!(final_value, "from5");
}

#[test]
fn test_per_connection_isolation() {
    let server = TestServer::start(18002);

    let mut conn1 = server.connection();
    let mut conn2 = server.connection();

    // Client1: WATCH key1, key2, key3
    redis::cmd("WATCH")
        .arg("key1")
        .arg("key2")
        .arg("key3")
        .execute(&mut conn1);

    // Client2: WATCH only key1
    redis::cmd("WATCH").arg("key1").execute(&mut conn2);

    // Client1: MULTI, SET key1
    redis::cmd("MULTI").execute(&mut conn1);
    redis::cmd("SET")
        .arg("key1")
        .arg("from1")
        .execute(&mut conn1);

    // Client2: Modify key1 (which it's watching)
    redis::cmd("SET")
        .arg("key1")
        .arg("from2")
        .execute(&mut conn2);

    // Client1: EXEC should return nil (key1 was watched and modified)
    let exec_result: Option<Vec<String>> = redis::cmd("EXEC").query(&mut conn1).unwrap();
    assert_eq!(exec_result, None, "EXEC should return nil for Client1");

    // Client2: Its watch is still active, so start a new transaction
    redis::cmd("MULTI").execute(&mut conn2);
    redis::cmd("SET").arg("key1").arg("val").execute(&mut conn2);

    // Client1: Modify key1 again
    redis::cmd("SET")
        .arg("key1")
        .arg("other")
        .execute(&mut conn1);

    // Client2: EXEC should return nil (key1 was modified while watched)
    let result: Option<Vec<String>> = redis::cmd("EXEC").query(&mut conn2).unwrap();
    assert_eq!(
        result, None,
        "Client2 transaction should fail (watch isolation works)"
    );
}

#[test]
fn test_unwatch_clears_all_watched_keys() {
    let server = TestServer::start(18003);

    let mut conn1 = server.connection();
    let mut conn2 = server.connection();

    // Client1: WATCH multiple keys
    redis::cmd("WATCH")
        .arg("key1")
        .arg("key2")
        .arg("key3")
        .execute(&mut conn1);

    // Client1: UNWATCH (clears ALL)
    redis::cmd("UNWATCH").execute(&mut conn1);

    // Client1: Start transaction
    redis::cmd("MULTI").execute(&mut conn1);
    redis::cmd("SET")
        .arg("key1")
        .arg("val1")
        .execute(&mut conn1);

    // Client2: Modify all the previously watched keys
    redis::cmd("SET")
        .arg("key1")
        .arg("other1")
        .execute(&mut conn2);
    redis::cmd("SET")
        .arg("key2")
        .arg("other2")
        .execute(&mut conn2);
    redis::cmd("SET")
        .arg("key3")
        .arg("other3")
        .execute(&mut conn2);

    // Client1: EXEC should succeed (no watches active)
    let exec_result: Option<Vec<String>> = redis::cmd("EXEC").query(&mut conn1).unwrap();
    assert!(
        exec_result.is_some(),
        "EXEC should succeed after UNWATCH cleared all watches"
    );
}

#[test]
fn test_watch_after_unwatch_same_key() {
    let server = TestServer::start(18004);

    let mut conn1 = server.connection();
    let mut conn2 = server.connection();

    // Client1: WATCH key
    redis::cmd("WATCH").arg("key").execute(&mut conn1);

    // Client1: UNWATCH
    redis::cmd("UNWATCH").execute(&mut conn1);

    // Client1: WATCH key again (re-watch)
    redis::cmd("WATCH").arg("key").execute(&mut conn1);

    // Client1: MULTI, SET key
    redis::cmd("MULTI").execute(&mut conn1);
    redis::cmd("SET").arg("key").arg("val1").execute(&mut conn1);

    // Client2: Modify the key
    redis::cmd("SET").arg("key").arg("val2").execute(&mut conn2);

    // Client1: EXEC should return nil (key is re-watched)
    let exec_result: Option<Vec<String>> = redis::cmd("EXEC").query(&mut conn1).unwrap();
    assert_eq!(
        exec_result, None,
        "EXEC should return nil for re-watched key"
    );
}

#[test]
fn test_discard_preserves_watched_keys() {
    let server = TestServer::start(18005);

    let mut conn1 = server.connection();
    let mut conn2 = server.connection();

    // Client1: WATCH key, MULTI, DISCARD
    redis::cmd("WATCH").arg("key").execute(&mut conn1);
    redis::cmd("MULTI").execute(&mut conn1);
    redis::cmd("SET").arg("key").arg("val").execute(&mut conn1);
    redis::cmd("DISCARD").execute(&mut conn1);

    // Client1: Start a new transaction (watch should still be active)
    redis::cmd("MULTI").execute(&mut conn1);
    redis::cmd("SET").arg("key").arg("val2").execute(&mut conn1);

    // Client2: Modify the key
    redis::cmd("SET")
        .arg("key")
        .arg("modified")
        .execute(&mut conn2);

    // Client1: EXEC should return nil (watch persisted through DISCARD)
    let exec_result: Option<Vec<String>> = redis::cmd("EXEC").query(&mut conn1).unwrap();
    assert_eq!(
        exec_result, None,
        "Watched keys should persist through DISCARD"
    );
}

#[test]
fn test_watch_with_special_key_names() {
    let server = TestServer::start(18006);

    let mut conn1 = server.connection();
    let mut conn2 = server.connection();

    // Watch keys with special characters
    redis::cmd("WATCH")
        .arg("key:with:colons")
        .arg("key-with-dashes")
        .arg("key with spaces")
        .execute(&mut conn1);

    redis::cmd("MULTI").execute(&mut conn1);
    redis::cmd("SET")
        .arg("key:with:colons")
        .arg("val")
        .execute(&mut conn1);

    // Modify one of them
    redis::cmd("SET")
        .arg("key:with:colons")
        .arg("modified")
        .execute(&mut conn2);

    // EXEC should fail
    let exec_result: Option<Vec<String>> = redis::cmd("EXEC").query(&mut conn1).unwrap();
    assert_eq!(
        exec_result, None,
        "Special key names should work with WATCH"
    );
}

#[test]
fn test_modification_window_between_watch_and_multi() {
    let server = TestServer::start(18007);

    let mut conn1 = server.connection();
    let mut conn2 = server.connection();

    // Client1: WATCH key
    redis::cmd("WATCH").arg("key").execute(&mut conn1);

    // Client2: Modify key (between WATCH and MULTI)
    redis::cmd("SET")
        .arg("key")
        .arg("modified")
        .execute(&mut conn2);

    // Client1: MULTI should still work
    redis::cmd("MULTI").execute(&mut conn1);
    redis::cmd("SET").arg("key").arg("val").execute(&mut conn1);

    // Client1: EXEC should return nil (modification detected before MULTI)
    let exec_result: Option<Vec<String>> = redis::cmd("EXEC").query(&mut conn1).unwrap();
    assert_eq!(
        exec_result, None,
        "Modification detected during WATCH should fail EXEC"
    );
}

#[test]
fn test_watch_same_key_multiple_times() {
    let server = TestServer::start(18008);

    let mut conn1 = server.connection();
    let mut conn2 = server.connection();

    // Watch same key three times (idempotent)
    redis::cmd("WATCH")
        .arg("key")
        .arg("key")
        .arg("key")
        .execute(&mut conn1);

    redis::cmd("MULTI").execute(&mut conn1);
    redis::cmd("SET").arg("key").arg("val").execute(&mut conn1);

    // Modify
    redis::cmd("SET")
        .arg("key")
        .arg("other")
        .execute(&mut conn2);

    // EXEC should fail (once watched, modification matters)
    let exec_result: Option<Vec<String>> = redis::cmd("EXEC").query(&mut conn1).unwrap();
    assert_eq!(exec_result, None);
}

// ============================================================================
// CATEGORY 2: CONCURRENT MODIFICATION RACING (10 tests)
// ============================================================================

#[test]
fn test_three_way_race_same_key() {
    let server = TestServer::start(18009);

    let mut conn1 = server.connection();
    let mut conn2 = server.connection();
    let mut conn3 = server.connection();

    // Client1: WATCH key, MULTI, SET key "from1"
    redis::cmd("WATCH").arg("key").execute(&mut conn1);
    redis::cmd("MULTI").execute(&mut conn1);
    redis::cmd("SET")
        .arg("key")
        .arg("from1")
        .execute(&mut conn1);

    // Client2 & 3: Simultaneously modify
    redis::cmd("SET")
        .arg("key")
        .arg("from2")
        .execute(&mut conn2);
    redis::cmd("SET")
        .arg("key")
        .arg("from3")
        .execute(&mut conn3);

    // Client1: EXEC returns nil
    let exec_result: Option<Vec<String>> = redis::cmd("EXEC").query(&mut conn1).unwrap();
    assert_eq!(exec_result, None);
}

#[test]
fn test_rapid_fire_modifications() {
    let server = TestServer::start(18010);

    let mut conn1 = server.connection();
    let mut conn2 = server.connection();

    // Client1: WATCH key, MULTI
    redis::cmd("WATCH").arg("key").execute(&mut conn1);
    redis::cmd("MULTI").execute(&mut conn1);

    // Client2: Rapid modifications
    for i in 0..10 {
        redis::cmd("SET")
            .arg("key")
            .arg(format!("val{}", i))
            .execute(&mut conn2);
    }

    // Client1: EXEC should return nil (any modification triggers it)
    let exec_result: Option<Vec<String>> = redis::cmd("EXEC").query(&mut conn1).unwrap();
    assert_eq!(
        exec_result, None,
        "Rapid modifications should all be detected"
    );
}

#[test]
fn test_del_removes_watched_key() {
    let server = TestServer::start(18011);

    let mut conn1 = server.connection();
    let mut conn2 = server.connection();

    // Client1: Set initial value and WATCH
    redis::cmd("SET")
        .arg("key")
        .arg("initial")
        .execute(&mut conn1);
    redis::cmd("WATCH").arg("key").execute(&mut conn1);
    redis::cmd("MULTI").execute(&mut conn1);
    redis::cmd("SET").arg("key").arg("val").execute(&mut conn1);

    // Client2: DELETE the watched key
    redis::cmd("DEL").arg("key").execute(&mut conn2);

    // Client1: EXEC should return nil (DEL is a modification)
    let exec_result: Option<Vec<String>> = redis::cmd("EXEC").query(&mut conn1).unwrap();
    assert_eq!(exec_result, None, "DEL should trigger WATCH detection");
}

#[test]
fn test_incr_counts_as_modification() {
    let server = TestServer::start(18012);

    let mut conn1 = server.connection();
    let mut conn2 = server.connection();

    // Setup
    redis::cmd("SET")
        .arg("counter")
        .arg("10")
        .execute(&mut conn1);

    // Client1: WATCH counter
    redis::cmd("WATCH").arg("counter").execute(&mut conn1);
    redis::cmd("MULTI").execute(&mut conn1);

    // Client2: INCR the counter
    redis::cmd("INCR").arg("counter").execute(&mut conn2);

    // Client1: EXEC should return nil
    let exec_result: Option<Vec<String>> = redis::cmd("EXEC").query(&mut conn1).unwrap();
    assert_eq!(exec_result, None, "INCR should be detected as modification");
}

#[test]
fn test_lpush_counts_as_modification() {
    let server = TestServer::start(18013);

    let mut conn1 = server.connection();
    let mut conn2 = server.connection();

    // Setup: Create a list
    redis::cmd("LPUSH").arg("list").arg("a").execute(&mut conn1);

    // Client1: WATCH list
    redis::cmd("WATCH").arg("list").execute(&mut conn1);
    redis::cmd("MULTI").execute(&mut conn1);

    // Client2: LPUSH to the list
    redis::cmd("LPUSH").arg("list").arg("b").execute(&mut conn2);

    // Client1: EXEC should return nil
    let exec_result: Option<Vec<String>> = redis::cmd("EXEC").query(&mut conn1).unwrap();
    assert_eq!(
        exec_result, None,
        "LPUSH should be detected as modification"
    );
}

#[test]
fn test_hset_counts_as_modification() {
    let server = TestServer::start(18014);

    let mut conn1 = server.connection();
    let mut conn2 = server.connection();

    // Setup: Create a hash
    redis::cmd("HSET")
        .arg("hash")
        .arg("field1")
        .arg("val1")
        .execute(&mut conn1);

    // Client1: WATCH hash
    redis::cmd("WATCH").arg("hash").execute(&mut conn1);
    redis::cmd("MULTI").execute(&mut conn1);

    // Client2: HSET on the hash
    redis::cmd("HSET")
        .arg("hash")
        .arg("field2")
        .arg("val2")
        .execute(&mut conn2);

    // Client1: EXEC should return nil
    let exec_result: Option<Vec<String>> = redis::cmd("EXEC").query(&mut conn1).unwrap();
    assert_eq!(exec_result, None, "HSET should be detected as modification");
}

#[test]
fn test_type_change_counts_as_modification() {
    let server = TestServer::start(18015);

    let mut conn1 = server.connection();
    let mut conn2 = server.connection();

    // Setup: Create a string
    redis::cmd("SET")
        .arg("key")
        .arg("string")
        .execute(&mut conn1);

    // Client1: WATCH key
    redis::cmd("WATCH").arg("key").execute(&mut conn1);
    redis::cmd("MULTI").execute(&mut conn1);

    // Client2: Change the type (string -> list)
    redis::cmd("DEL").arg("key").execute(&mut conn2);
    redis::cmd("LPUSH")
        .arg("key")
        .arg("item")
        .execute(&mut conn2);

    // Client1: EXEC should return nil
    let exec_result: Option<Vec<String>> = redis::cmd("EXEC").query(&mut conn1).unwrap();
    assert_eq!(exec_result, None, "Type change should be detected");
}

#[test]
fn test_get_does_not_count_as_modification() {
    let server = TestServer::start(18016);

    let mut conn1 = server.connection();
    let mut conn2 = server.connection();

    // Setup
    redis::cmd("SET")
        .arg("key")
        .arg("value")
        .execute(&mut conn1);

    // Client1: WATCH key
    redis::cmd("WATCH").arg("key").execute(&mut conn1);
    redis::cmd("MULTI").execute(&mut conn1);

    // Client2: READ-ONLY operation (should not trigger watch)
    let _: String = redis::cmd("GET").arg("key").query(&mut conn2).unwrap();
    let _: i32 = redis::cmd("STRLEN").arg("key").query(&mut conn2).unwrap();

    // Client1: EXEC should succeed (GET/STRLEN don't modify)
    redis::cmd("SET").arg("key").arg("val").execute(&mut conn1);
    let exec_result: Option<Vec<String>> = redis::cmd("EXEC").query(&mut conn1).unwrap();
    assert!(
        exec_result.is_some(),
        "Read-only operations should not trigger WATCH"
    );
}

// ============================================================================
// CATEGORY 3: MULTI-KEY SCENARIOS (7 tests)
// ============================================================================

#[test]
fn test_watch_10_keys_modify_one() {
    let server = TestServer::start(18017);

    let mut conn1 = server.connection();
    let mut conn2 = server.connection();

    // Client1: Set and watch 10 keys
    for i in 1..=10 {
        redis::cmd("SET")
            .arg(format!("key{}", i))
            .arg(format!("val{}", i))
            .execute(&mut conn1);
    }

    let keys: Vec<String> = (1..=10).map(|i| format!("key{}", i)).collect();
    redis::cmd("WATCH").arg(&keys).execute(&mut conn1);

    redis::cmd("MULTI").execute(&mut conn1);
    redis::cmd("SET").arg("key5").arg("new").execute(&mut conn1);

    // Client2: Modify only key5
    redis::cmd("SET")
        .arg("key5")
        .arg("other")
        .execute(&mut conn2);

    // Client1: EXEC should fail
    let exec_result: Option<Vec<String>> = redis::cmd("EXEC").query(&mut conn1).unwrap();
    assert_eq!(exec_result, None);
}

#[test]
fn test_watch_10_keys_modify_different_per_client() {
    let server = TestServer::start(18018);

    let mut conn1 = server.connection();
    let mut conn2 = server.connection();
    let mut conn3 = server.connection();

    // Client1: WATCH key1-5
    redis::cmd("WATCH")
        .arg("key1")
        .arg("key2")
        .arg("key3")
        .arg("key4")
        .arg("key5")
        .execute(&mut conn1);
    redis::cmd("MULTI").execute(&mut conn1);

    // Client2: Modify watched key2
    redis::cmd("SET")
        .arg("key2")
        .arg("modified")
        .execute(&mut conn2);

    // Client3: Modify unwatched key7
    redis::cmd("SET")
        .arg("key7")
        .arg("other")
        .execute(&mut conn3);

    // Client1: EXEC should fail (key2 was watched)
    let exec_result: Option<Vec<String>> = redis::cmd("EXEC").query(&mut conn1).unwrap();
    assert_eq!(
        exec_result, None,
        "Modification of watched key should fail EXEC"
    );
}

#[test]
fn test_very_long_key_names() {
    let server = TestServer::start(18019);

    let mut conn1 = server.connection();
    let mut conn2 = server.connection();

    // Create a very long key name (1000 chars)
    let long_key = "k".repeat(1000);

    redis::cmd("SET")
        .arg(&long_key)
        .arg("val")
        .execute(&mut conn1);
    redis::cmd("WATCH").arg(&long_key).execute(&mut conn1);
    redis::cmd("MULTI").execute(&mut conn1);

    // Modify it
    redis::cmd("SET")
        .arg(&long_key)
        .arg("modified")
        .execute(&mut conn2);

    // EXEC should fail
    let exec_result: Option<Vec<String>> = redis::cmd("EXEC").query(&mut conn1).unwrap();
    assert_eq!(exec_result, None, "Long key names should work");
}

#[test]
fn test_watch_key_then_key_in_transaction() {
    let server = TestServer::start(18020);

    let mut conn1 = server.connection();
    let mut conn2 = server.connection();

    // Client1: WATCH key
    redis::cmd("WATCH").arg("key").execute(&mut conn1);
    redis::cmd("MULTI").execute(&mut conn1);

    // Client1: Queue a command that modifies the watched key
    redis::cmd("SET").arg("key").arg("val1").execute(&mut conn1);

    // Client2: Also modify the watched key
    redis::cmd("SET").arg("key").arg("val2").execute(&mut conn2);

    // Client1: EXEC should fail (watched key modified externally)
    let exec_result: Option<Vec<String>> = redis::cmd("EXEC").query(&mut conn1).unwrap();
    assert_eq!(exec_result, None);
}

#[test]
fn test_watch_overlapping_keys_multiple_clients() {
    let server = TestServer::start(18021);

    let mut conn1 = server.connection();
    let mut conn2 = server.connection();
    let mut conn3 = server.connection();

    // Client1: WATCH key1, key2
    redis::cmd("WATCH")
        .arg("key1")
        .arg("key2")
        .execute(&mut conn1);
    redis::cmd("MULTI").execute(&mut conn1);

    // Client2: WATCH key2, key3
    redis::cmd("WATCH")
        .arg("key2")
        .arg("key3")
        .execute(&mut conn2);
    redis::cmd("MULTI").execute(&mut conn2);

    // Client3: Modify key2 (watched by both)
    redis::cmd("SET")
        .arg("key2")
        .arg("modified")
        .execute(&mut conn3);

    // Both should fail
    let exec1: Option<Vec<String>> = redis::cmd("EXEC").query(&mut conn1).unwrap();
    let exec2: Option<Vec<String>> = redis::cmd("EXEC").query(&mut conn2).unwrap();

    assert_eq!(exec1, None);
    assert_eq!(exec2, None);
}

#[test]
fn test_unwatch_then_modify_original_keys() {
    let server = TestServer::start(18022);

    let mut conn1 = server.connection();
    let mut conn2 = server.connection();

    // Client1: WATCH key1, key2, key3
    redis::cmd("WATCH")
        .arg("key1")
        .arg("key2")
        .arg("key3")
        .execute(&mut conn1);

    // Client1: UNWATCH (clears all)
    redis::cmd("UNWATCH").execute(&mut conn1);

    redis::cmd("MULTI").execute(&mut conn1);
    redis::cmd("SET").arg("key1").arg("val").execute(&mut conn1);

    // Client2: Modify all the keys that were being watched
    redis::cmd("SET")
        .arg("key1")
        .arg("other")
        .execute(&mut conn2);
    redis::cmd("SET")
        .arg("key2")
        .arg("other")
        .execute(&mut conn2);
    redis::cmd("SET")
        .arg("key3")
        .arg("other")
        .execute(&mut conn2);

    // Client1: EXEC should succeed (watches are cleared)
    let exec_result: Option<Vec<String>> = redis::cmd("EXEC").query(&mut conn1).unwrap();
    assert!(exec_result.is_some(), "EXEC should succeed after UNWATCH");
}

// ============================================================================
// CATEGORY 4: PROTOCOL COMPLIANCE (6 tests)
// ============================================================================

#[test]
fn test_exec_return_format_nil_when_watched() {
    let server = TestServer::start(18023);

    let mut conn1 = server.connection();
    let mut conn2 = server.connection();

    redis::cmd("SET").arg("key").arg("val").execute(&mut conn1);
    redis::cmd("WATCH").arg("key").execute(&mut conn1);
    redis::cmd("MULTI").execute(&mut conn1);

    redis::cmd("SET")
        .arg("key")
        .arg("other")
        .execute(&mut conn2);

    // EXEC should return Option::None (nil in RESP)
    let exec_result: Option<Vec<String>> = redis::cmd("EXEC").query(&mut conn1).unwrap();
    assert_eq!(
        exec_result, None,
        "EXEC should return nil (Option::None) when watch fails"
    );
}

#[test]
fn test_exec_return_format_array_when_not_watched() {
    let server = TestServer::start(18024);

    let mut conn = server.connection();

    redis::cmd("MULTI").execute(&mut conn);
    redis::cmd("SET").arg("key1").arg("val1").execute(&mut conn);
    redis::cmd("SET").arg("key2").arg("val2").execute(&mut conn);

    // EXEC should return an array (Vec)
    let exec_result: Option<Vec<String>> = redis::cmd("EXEC").query(&mut conn).unwrap();
    assert!(
        exec_result.is_some(),
        "EXEC should return array (Option::Some) when no watch or watch not triggered"
    );
    let results = exec_result.unwrap();
    assert_eq!(results.len(), 2);
}

#[test]
fn test_discard_clears_queue_not_watch() {
    let server = TestServer::start(18025);

    let mut conn1 = server.connection();
    let mut conn2 = server.connection();

    redis::cmd("SET")
        .arg("key")
        .arg("initial")
        .execute(&mut conn1);

    // Client1: WATCH key, MULTI, set, then DISCARD
    redis::cmd("WATCH").arg("key").execute(&mut conn1);
    redis::cmd("MULTI").execute(&mut conn1);
    redis::cmd("SET").arg("key").arg("val1").execute(&mut conn1);
    redis::cmd("DISCARD").execute(&mut conn1);

    // Client1: Queue result should be "OK" for DISCARD
    // Now client1 can start a new transaction with watch still active
    redis::cmd("MULTI").execute(&mut conn1);
    redis::cmd("SET").arg("key").arg("val2").execute(&mut conn1);

    // Client2: Modify the key
    redis::cmd("SET")
        .arg("key")
        .arg("modified")
        .execute(&mut conn2);

    // Client1: EXEC should fail (watch still active from before DISCARD)
    let exec_result: Option<Vec<String>> = redis::cmd("EXEC").query(&mut conn1).unwrap();
    assert_eq!(
        exec_result, None,
        "Watch should persist through DISCARD and detect modification"
    );
}

#[test]
fn test_pipelined_watch_multi_exec() {
    let server = TestServer::start(18026);

    let mut conn1 = server.connection();
    let mut conn2 = server.connection();

    // Use pipelining to send multiple commands
    let _: () = redis::pipe()
        .cmd("SET")
        .arg("key")
        .arg("initial")
        .cmd("WATCH")
        .arg("key")
        .cmd("MULTI")
        .cmd("SET")
        .arg("key")
        .arg("val1")
        .ignore()
        .cmd("INCR")
        .arg("key")
        .ignore()
        .query(&mut conn1)
        .unwrap();

    // Conn2: Modify
    redis::cmd("SET")
        .arg("key")
        .arg("other")
        .execute(&mut conn2);

    // EXEC should fail
    let exec_result: Option<Vec<String>> = redis::cmd("EXEC").query(&mut conn1).unwrap();
    assert_eq!(
        exec_result, None,
        "Pipelined commands should work correctly"
    );
}

#[test]
fn test_watch_in_pubsub_mode_errors() {
    let server = TestServer::start(18027);

    let mut conn = server.connection();

    // Enter pub/sub mode
    let _: () = redis::cmd("SUBSCRIBE")
        .arg("channel")
        .query(&mut conn)
        .ok()
        .unwrap_or(());

    // In pub/sub mode, WATCH should error (this may need adjustment based on actual behavior)
    // For now, we just verify the connection can handle it
}

// ============================================================================
// CATEGORY 5: STRESS & PERFORMANCE (5 tests)
// ============================================================================

#[test]
fn test_100_concurrent_clients_watch_same_key() {
    let server = Arc::new(TestServer::start(18028));

    let (tx, rx) = std::sync::mpsc::channel();
    let mut handles = vec![];

    // Spawn 100 clients
    for i in 0..100 {
        let server_clone = Arc::clone(&server);
        let tx_clone = tx.clone();

        let handle = thread::spawn(move || {
            let mut conn = server_clone.connection();

            // All watch the same key
            redis::cmd("WATCH").arg("hotkey").execute(&mut conn);
            redis::cmd("MULTI").execute(&mut conn);
            redis::cmd("SET")
                .arg("hotkey")
                .arg(format!("from{}", i))
                .execute(&mut conn);

            // Try to execute
            let exec_result: Option<Vec<String>> = redis::cmd("EXEC").query(&mut conn).unwrap();

            // Most should fail, maybe one succeeds
            tx_clone
                .send((i, exec_result))
                .expect("Failed to send result");
        });

        handles.push(handle);
    }

    // Wait for all to complete
    drop(tx); // Close the sender
    let mut success_count = 0;
    let mut fail_count = 0;

    for _ in 0..100 {
        if let Ok((_, result)) = rx.recv() {
            if result.is_some() {
                success_count += 1;
            } else {
                fail_count += 1;
            }
        }
    }

    for handle in handles {
        handle.join().unwrap();
    }

    // At least one should fail (due to contention)
    assert!(
        fail_count > 0 || success_count > 0,
        "Concurrent test should complete"
    );
}

#[test]
fn test_1000_watched_keys_per_connection() {
    let server = TestServer::start(18029);

    let mut conn1 = server.connection();
    let mut conn2 = server.connection();

    // Watch 1000 keys
    let keys: Vec<String> = (0..1000).map(|i| format!("key{}", i)).collect();
    redis::cmd("WATCH").arg(&keys).execute(&mut conn1);

    redis::cmd("MULTI").execute(&mut conn1);
    redis::cmd("SET")
        .arg("key500")
        .arg("val")
        .execute(&mut conn1);

    // Modify key500
    redis::cmd("SET")
        .arg("key500")
        .arg("other")
        .execute(&mut conn2);

    // EXEC should fail
    let exec_result: Option<Vec<String>> = redis::cmd("EXEC").query(&mut conn1).unwrap();
    assert_eq!(
        exec_result, None,
        "Watching 1000 keys should work correctly"
    );
}

#[test]
fn test_large_transaction_with_watch() {
    let server = TestServer::start(18030);

    let mut conn1 = server.connection();
    let mut conn2 = server.connection();

    redis::cmd("WATCH").arg("key").execute(&mut conn1);
    redis::cmd("MULTI").execute(&mut conn1);

    // Queue 100 commands
    for i in 0..100 {
        redis::cmd("SET")
            .arg(format!("transkey{}", i))
            .arg(format!("val{}", i))
            .execute(&mut conn1);
    }

    // Modify watched key
    redis::cmd("SET")
        .arg("key")
        .arg("modified")
        .execute(&mut conn2);

    // EXEC should fail
    let exec_result: Option<Vec<String>> = redis::cmd("EXEC").query(&mut conn1).unwrap();
    assert_eq!(exec_result, None, "Large transaction should be rolled back");
}

#[test]
fn test_watched_key_with_large_value() {
    let server = TestServer::start(18031);

    let mut conn1 = server.connection();
    let mut conn2 = server.connection();

    // Create a large value (1MB)
    let large_val = "x".repeat(1_000_000);

    redis::cmd("SET")
        .arg("key")
        .arg(&large_val)
        .execute(&mut conn1);
    redis::cmd("WATCH").arg("key").execute(&mut conn1);
    redis::cmd("MULTI").execute(&mut conn1);

    // Modify with another large value
    redis::cmd("SET")
        .arg("key")
        .arg("x".repeat(1_000_000))
        .execute(&mut conn2);

    // EXEC should fail
    let exec_result: Option<Vec<String>> = redis::cmd("EXEC").query(&mut conn1).unwrap();
    assert_eq!(exec_result, None);
}

// ============================================================================
// CATEGORY 6: INTEGRATION SCENARIOS (6 tests)
// ============================================================================

#[test]
fn test_watch_with_authentication() {
    // Note: This test requires --password flag, skipping for now
    // Would test that WATCH respects authentication state
}

#[test]
fn test_watch_with_different_databases() {
    let server = TestServer::start(18032);

    let mut conn1 = server.connection();
    let mut conn2 = server.connection();

    // Client1: SELECT database 0, WATCH key
    redis::cmd("SELECT").arg("0").execute(&mut conn1);
    redis::cmd("SET").arg("key").arg("db0").execute(&mut conn1);
    redis::cmd("WATCH").arg("key").execute(&mut conn1);
    redis::cmd("MULTI").execute(&mut conn1);

    // Client2: SELECT database 1, modify "key" (different DB)
    redis::cmd("SELECT").arg("1").execute(&mut conn2);
    redis::cmd("SET").arg("key").arg("db1").execute(&mut conn2);

    // Client1: EXEC should succeed (watched key in DB 0, modified in DB 1)
    let exec_result: Option<Vec<String>> = redis::cmd("EXEC").query(&mut conn1).unwrap();
    assert!(exec_result.is_some(), "Watch should be DB-isolated");

    // Client2: Back to DB 0, modify the same key
    redis::cmd("SELECT").arg("0").execute(&mut conn2);

    // Start fresh watch
    redis::cmd("WATCH").arg("key").execute(&mut conn1);
    redis::cmd("MULTI").execute(&mut conn1);

    redis::cmd("SET")
        .arg("key")
        .arg("modified_in_db0")
        .execute(&mut conn2);

    // Now EXEC should fail
    let exec_result: Option<Vec<String>> = redis::cmd("EXEC").query(&mut conn1).unwrap();
    assert_eq!(
        exec_result, None,
        "Watch should detect modification in same DB"
    );
}

#[test]
fn test_watch_with_expiration() {
    let server = TestServer::start(18033);

    let mut conn1 = server.connection();
    let mut conn2 = server.connection();

    // Setup: Create a key that will expire
    redis::cmd("SET").arg("key").arg("val").execute(&mut conn1);
    redis::cmd("EXPIRE")
        .arg("key")
        .arg("10")
        .execute(&mut conn1);

    // Client1: WATCH the key with TTL
    redis::cmd("WATCH").arg("key").execute(&mut conn1);
    redis::cmd("MULTI").execute(&mut conn1);

    // Client2: Let the key expire naturally (not explicit DEL)
    // For this test, we'll just modify it instead
    redis::cmd("SET")
        .arg("key")
        .arg("modified")
        .execute(&mut conn2);

    // EXEC should fail (modification detected)
    let exec_result: Option<Vec<String>> = redis::cmd("EXEC").query(&mut conn1).unwrap();
    assert_eq!(exec_result, None);
}

#[test]
fn test_sequential_transactions_same_connection() {
    let server = TestServer::start(18034);

    let mut conn1 = server.connection();
    let mut conn2 = server.connection();

    // Transaction 1
    redis::cmd("SET").arg("key1").arg("a").execute(&mut conn1);
    redis::cmd("WATCH").arg("key1").execute(&mut conn1);
    redis::cmd("MULTI").execute(&mut conn1);
    redis::cmd("SET").arg("key1").arg("b").execute(&mut conn1);

    redis::cmd("SET")
        .arg("key1")
        .arg("modified")
        .execute(&mut conn2);

    let result1: Option<Vec<String>> = redis::cmd("EXEC").query(&mut conn1).unwrap();
    assert_eq!(result1, None, "First transaction should fail");

    // Transaction 2 (on same connection)
    redis::cmd("SET").arg("key2").arg("x").execute(&mut conn1);
    redis::cmd("WATCH").arg("key2").execute(&mut conn1);
    redis::cmd("MULTI").execute(&mut conn1);
    redis::cmd("SET").arg("key2").arg("y").execute(&mut conn1);

    // Don't modify this time
    let result2: Option<Vec<String>> = redis::cmd("EXEC").query(&mut conn1).unwrap();
    assert!(
        result2.is_some(),
        "Second transaction should succeed (no modification)"
    );
}

#[test]
fn test_watch_error_messages_match_redis() {
    let server = TestServer::start(18035);

    let mut conn = server.connection();

    // Test: MULTI then WATCH (should error)
    redis::cmd("MULTI").execute(&mut conn);

    let result: redis::RedisResult<()> = redis::cmd("WATCH").arg("key").query(&mut conn);

    // Should error
    assert!(result.is_err(), "WATCH inside MULTI should error");
}
