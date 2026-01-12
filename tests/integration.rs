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
