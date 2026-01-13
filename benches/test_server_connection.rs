use redis::Commands;

fn main() {
    println!("Testing Redlite Server connection with Rust redis client...\n");

    // Try to connect to Redlite Server
    match redis::Client::open("redis://127.0.0.1:7381/") {
        Ok(client) => {
            println!("✓ Client created successfully");

            match client.get_connection() {
                Ok(mut conn) => {
                    println!("✓ Connection established");

                    // Try PING
                    match redis::cmd("PING").query::<String>(&mut conn) {
                        Ok(pong) => println!("✓ PING response: {}", pong),
                        Err(e) => println!("✗ PING failed: {}", e),
                    }

                    // Try SET a key
                    match conn.set::<_, _, String>("test_key", "test_value") {
                        Ok(_) => println!("✓ SET succeeded"),
                        Err(e) => println!("✗ SET failed: {}", e),
                    }

                    // Try GET the key
                    match conn.get::<_, String>("test_key") {
                        Ok(val) => println!("✓ GET succeeded: {}", val),
                        Err(e) => println!("✗ GET failed: {}", e),
                    }

                    // Try bulk SET (like populate would do)
                    println!("\nTesting bulk SET operations (100 keys)...");
                    let mut failed_at = None;
                    for i in 0..100 {
                        if let Err(e) = conn.set::<_, _, String>(&format!("key_{}", i), "value") {
                            println!("✗ Failed at key {}: {}", i, e);
                            failed_at = Some(i);
                            break;
                        }
                        if i % 25 == 0 && i > 0 {
                            println!("  {} keys set...", i);
                        }
                    }
                    if failed_at.is_none() {
                        println!("✓ All 100 keys set successfully");
                    }
                }
                Err(e) => println!("✗ Connection failed: {}", e),
            }
        }
        Err(e) => println!("✗ Client creation failed: {}", e),
    }
}
