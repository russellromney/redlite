use redis::Commands;
use std::sync::Arc;
use redlite::Db;

fn main() {
    println!("\n╔════════════════════════════════════════════╗");
    println!("║   STEP-BY-STEP DIAGNOSTIC TEST              ║");
    println!("╚════════════════════════════════════════════╝\n");

    // Step 1: Test embedded client
    println!("STEP 1: Testing Redlite Embedded (Arc<Db>)");
    let embedded_db = Arc::new(Db::open_memory().unwrap());

    // Populate
    println!("  - Populating 100 keys...");
    for i in 0..100 {
        embedded_db.set(&format!("key_{}", i), b"value", None).unwrap();
    }
    println!("  ✓ Population successful");

    // Get
    println!("  - Testing GET...");
    if let Ok(Some(val)) = embedded_db.get("key_50") {
        println!("  ✓ GET successful: {:?}", String::from_utf8_lossy(&val));
    } else {
        println!("  ✗ GET failed");
    }

    // Clone for async
    println!("  - Testing Arc clone...");
    let cloned = Arc::clone(&embedded_db);
    if let Ok(Some(val)) = cloned.get("key_50") {
        println!("  ✓ Cloned Arc GET successful: {:?}", String::from_utf8_lossy(&val));
    } else {
        println!("  ✗ Cloned Arc GET failed");
    }

    // Step 2: Test Redis connection
    println!("\nSTEP 2: Testing Redis Client (6379)");

    match redis::Client::open("redis://127.0.0.1:6379/") {
        Ok(client) => {
            println!("  ✓ Client created");

            match client.get_connection() {
                Ok(mut conn) => {
                    println!("  ✓ Connection established");

                    // Populate
                    println!("  - Populating 100 keys...");
                    let mut pop_failed = false;
                    for i in 0..100 {
                        if conn.set::<_, _, String>(&format!("redis_key_{}", i), "value").is_err() {
                            println!("  ✗ Population failed at key {}", i);
                            pop_failed = true;
                            break;
                        }
                    }
                    if !pop_failed {
                        println!("  ✓ Population successful");
                    }

                    // Get
                    println!("  - Testing GET...");
                    match conn.get::<_, String>("redis_key_50") {
                        Ok(val) => println!("  ✓ GET successful: {}", val),
                        Err(e) => println!("  ✗ GET failed: {}", e),
                    }

                    // Clone and test
                    let client2 = client.clone();
                    match client2.get_connection() {
                        Ok(mut conn2) => {
                            println!("  ✓ Cloned client connection established");
                            match conn2.get::<_, String>("redis_key_50") {
                                Ok(val) => println!("  ✓ Cloned client GET successful: {}", val),
                                Err(e) => println!("  ✗ Cloned client GET failed: {}", e),
                            }
                        }
                        Err(e) => println!("  ✗ Cloned client connection failed: {}", e),
                    }
                }
                Err(e) => println!("  ✗ Connection failed: {}", e),
            }
        }
        Err(e) => println!("  ✗ Client creation failed: {}", e),
    }

    // Step 3: Test Redlite Server connection
    println!("\nSTEP 3: Testing Redlite Server (7381)");

    match redis::Client::open("redis://127.0.0.1:7381/") {
        Ok(client) => {
            println!("  ✓ Client created");

            match client.get_connection() {
                Ok(mut conn) => {
                    println!("  ✓ Connection established");

                    // Populate
                    println!("  - Populating 100 keys...");
                    let mut pop_failed = false;
                    let mut failed_at = 0;
                    for i in 0..100 {
                        if conn.set::<_, _, String>(&format!("server_key_{}", i), "value").is_err() {
                            println!("  ✗ Population failed at key {}: {:?}", i, conn.set::<_, _, String>(&format!("server_key_{}", i), "value"));
                            pop_failed = true;
                            failed_at = i;
                            break;
                        }
                    }
                    if !pop_failed {
                        println!("  ✓ Population successful");
                    } else {
                        println!("  Failed after key {}", failed_at);
                    }

                    // Get
                    println!("  - Testing GET...");
                    match conn.get::<_, String>("server_key_50") {
                        Ok(val) => println!("  ✓ GET successful: {}", val),
                        Err(e) => println!("  ✗ GET failed: {}", e),
                    }
                }
                Err(e) => {
                    println!("  ✗ Connection failed: {}", e);
                    println!("    Error details: {:?}", e);
                }
            }
        }
        Err(e) => {
            println!("  ✗ Client creation failed: {}", e);
        }
    }

    println!("\n✓ Diagnostic complete\n");
}
