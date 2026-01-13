use redlite::{Db, ZMember};
use std::sync::Arc;
use std::time::{Instant, Duration};
use std::collections::HashMap;

#[cfg(feature = "turso")]
use redlite::TursoDb;

/// Benchmark result for a single operation
#[derive(Debug, Clone)]
struct BenchResult {
    backend: String,
    size: usize,
    connections: usize,
    operation: String,
    avg_latency_us: f64,
    total_ops: usize,
    throughput_ops_per_sec: f64,
}

/// Supported backends for benchmarking
#[derive(Debug, Clone, Copy)]
enum Backend {
    Redis,
    Dragonfly,
    RedliteEmbeddedMemorySqlite,
    RedliteEmbeddedFileSqlite,
    #[cfg(feature = "turso")]
    RedliteEmbeddedMemoryTurso,
    #[cfg(feature = "turso")]
    RedliteEmbeddedFileTurso,
    RedliteServerMemorySqlite,
    RedliteServerFileSqlite,
    #[cfg(feature = "turso")]
    RedliteServerMemoryTurso,
    #[cfg(feature = "turso")]
    RedliteServerFileTurso,
}

impl Backend {
    fn name(&self) -> &'static str {
        match self {
            Backend::Redis => "Redis",
            Backend::Dragonfly => "Dragonfly",
            Backend::RedliteEmbeddedMemorySqlite => "Redlite Embedded (Memory/SQLite)",
            Backend::RedliteEmbeddedFileSqlite => "Redlite Embedded (File/SQLite)",
            #[cfg(feature = "turso")]
            Backend::RedliteEmbeddedMemoryTurso => "Redlite Embedded (Memory/Turso)",
            #[cfg(feature = "turso")]
            Backend::RedliteEmbeddedFileTurso => "Redlite Embedded (File/Turso)",
            Backend::RedliteServerMemorySqlite => "Redlite Server (Memory/SQLite)",
            Backend::RedliteServerFileSqlite => "Redlite Server (File/SQLite)",
            #[cfg(feature = "turso")]
            Backend::RedliteServerMemoryTurso => "Redlite Server (Memory/Turso)",
            #[cfg(feature = "turso")]
            Backend::RedliteServerFileTurso => "Redlite Server (File/Turso)",
        }
    }

    fn short_name(&self) -> &'static str {
        match self {
            Backend::Redis => "redis",
            Backend::Dragonfly => "dragonfly",
            Backend::RedliteEmbeddedMemorySqlite => "redlite-emb-mem-sqlite",
            Backend::RedliteEmbeddedFileSqlite => "redlite-emb-file-sqlite",
            #[cfg(feature = "turso")]
            Backend::RedliteEmbeddedMemoryTurso => "redlite-emb-mem-turso",
            #[cfg(feature = "turso")]
            Backend::RedliteEmbeddedFileTurso => "redlite-emb-file-turso",
            Backend::RedliteServerMemorySqlite => "redlite-srv-mem-sqlite",
            Backend::RedliteServerFileSqlite => "redlite-srv-file-sqlite",
            #[cfg(feature = "turso")]
            Backend::RedliteServerMemoryTurso => "redlite-srv-mem-turso",
            #[cfg(feature = "turso")]
            Backend::RedliteServerFileTurso => "redlite-srv-file-turso",
        }
    }
}

/// Client abstraction for different backends
enum Client {
    Redis(redis::Client),
    Dragonfly(redis::Client),
    RedliteEmbedded(Arc<Db>),
    #[cfg(feature = "turso")]
    RedliteTursoEmbedded(Arc<TursoDb>),
    RedliteServer(redis::Client),
}

impl Client {
    fn get(&self, key: &str) -> Result<Option<Vec<u8>>, Box<dyn std::error::Error>> {
        match self {
            Client::Redis(client) | Client::Dragonfly(client) | Client::RedliteServer(client) => {
                use redis::Commands;
                let mut conn = client.get_connection()?;
                let result: Option<Vec<u8>> = conn.get(key)?;
                Ok(result)
            }
            Client::RedliteEmbedded(db) => Ok(db.get(key)?),
            #[cfg(feature = "turso")]
            Client::RedliteTursoEmbedded(db) => Ok(db.get(key)?),
        }
    }

    fn set(&self, key: &str, value: &[u8]) -> Result<(), Box<dyn std::error::Error>> {
        match self {
            Client::Redis(client) | Client::Dragonfly(client) | Client::RedliteServer(client) => {
                use redis::Commands;
                let mut conn = client.get_connection()?;
                conn.set(key, value)?;
                Ok(())
            }
            Client::RedliteEmbedded(db) => {
                db.set(key, value, None)?;
                Ok(())
            }
            #[cfg(feature = "turso")]
            Client::RedliteTursoEmbedded(db) => {
                db.set(key, value, None)?;
                Ok(())
            }
        }
    }

    fn incr(&self, key: &str) -> Result<i64, Box<dyn std::error::Error>> {
        match self {
            Client::Redis(client) | Client::Dragonfly(client) | Client::RedliteServer(client) => {
                use redis::Commands;
                let mut conn = client.get_connection()?;
                let result: i64 = conn.incr(key, 1)?;
                Ok(result)
            }
            Client::RedliteEmbedded(db) => Ok(db.incr(key)?),
            #[cfg(feature = "turso")]
            Client::RedliteTursoEmbedded(db) => Ok(db.incr(key)?),
        }
    }

    /// Clone the client for use in async tasks
    fn clone_for_async(&self) -> Client {
        match self {
            Client::Redis(client) => Client::Redis(client.clone()),
            Client::Dragonfly(client) => Client::Dragonfly(client.clone()),
            Client::RedliteEmbedded(db) => Client::RedliteEmbedded(Arc::clone(db)),
            #[cfg(feature = "turso")]
            Client::RedliteTursoEmbedded(db) => Client::RedliteTursoEmbedded(Arc::clone(db)),
            Client::RedliteServer(client) => Client::RedliteServer(client.clone()),
        }
    }
}

/// Try to connect to external services
fn try_connect_redis() -> Option<redis::Client> {
    redis::Client::open("redis://127.0.0.1:6379/")
        .ok()
        .and_then(|client| {
            client.get_connection().ok()?;
            Some(client)
        })
}

fn try_connect_dragonfly() -> Option<redis::Client> {
    redis::Client::open("redis://127.0.0.1:6380/")
        .ok()
        .and_then(|client| {
            client.get_connection().ok()?;
            Some(client)
        })
}

fn try_connect_redlite_server_sqlite_mem() -> Option<redis::Client> {
    redis::Client::open("redis://127.0.0.1:7381/")
        .ok()
        .and_then(|client| {
            client.get_connection().ok()?;
            Some(client)
        })
}

fn try_connect_redlite_server_sqlite_file() -> Option<redis::Client> {
    redis::Client::open("redis://127.0.0.1:7382/")
        .ok()
        .and_then(|client| {
            client.get_connection().ok()?;
            Some(client)
        })
}

fn try_connect_redlite_server_turso_mem() -> Option<redis::Client> {
    redis::Client::open("redis://127.0.0.1:7383/")
        .ok()
        .and_then(|client| {
            client.get_connection().ok()?;
            Some(client)
        })
}

fn try_connect_redlite_server_turso_file() -> Option<redis::Client> {
    redis::Client::open("redis://127.0.0.1:7384/")
        .ok()
        .and_then(|client| {
            client.get_connection().ok()?;
            Some(client)
        })
}

/// Setup client for a given backend
fn setup_client(backend: Backend) -> Option<Client> {
    match backend {
        Backend::Redis => try_connect_redis().map(Client::Redis),
        Backend::Dragonfly => try_connect_dragonfly().map(Client::Dragonfly),
        Backend::RedliteEmbeddedMemorySqlite => {
            Db::open_memory().ok().map(|db| Client::RedliteEmbedded(Arc::new(db)))
        }
        Backend::RedliteEmbeddedFileSqlite => {
            let timestamp = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos();
            let path = format!("/tmp/redlite_bench_{}.db", timestamp);
            Db::open(&path).ok().map(|db| Client::RedliteEmbedded(Arc::new(db)))
        }
        #[cfg(feature = "turso")]
        Backend::RedliteEmbeddedMemoryTurso => {
            TursoDb::open_memory().ok().map(|db| Client::RedliteTursoEmbedded(Arc::new(db)))
        }
        #[cfg(feature = "turso")]
        Backend::RedliteEmbeddedFileTurso => {
            let timestamp = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos();
            let path = format!("/tmp/redlite_turso_bench_{}.db", timestamp);
            TursoDb::open(&path).ok().map(|db| Client::RedliteTursoEmbedded(Arc::new(db)))
        }
        Backend::RedliteServerMemorySqlite => {
            try_connect_redlite_server_sqlite_mem().map(Client::RedliteServer)
        }
        Backend::RedliteServerFileSqlite => {
            try_connect_redlite_server_sqlite_file().map(Client::RedliteServer)
        }
        #[cfg(feature = "turso")]
        Backend::RedliteServerMemoryTurso => {
            try_connect_redlite_server_turso_mem().map(Client::RedliteServer)
        }
        #[cfg(feature = "turso")]
        Backend::RedliteServerFileTurso => {
            try_connect_redlite_server_turso_file().map(Client::RedliteServer)
        }
    }
}

/// Populate database with keys
fn populate(client: &Client, size: usize) -> Result<(), Box<dyn std::error::Error>> {
    match client {
        // For Redis clients, use pipelining to avoid connection exhaustion
        Client::Redis(_) | Client::Dragonfly(_) | Client::RedliteServer(_) => {
            use redis::Commands;
            let redis_client = match client {
                Client::Redis(c) => c,
                Client::Dragonfly(c) => c,
                Client::RedliteServer(c) => c,
                _ => unreachable!(),
            };

            let mut conn = redis_client.get_connection()?;
            for i in 0..size {
                let key = format!("key_{}", i);
                conn.set(&key, b"value_data_here")?;
            }
            Ok(())
        }
        // For embedded clients, just set directly
        Client::RedliteEmbedded(db) => {
            for i in 0..size {
                let key = format!("key_{}", i);
                db.set(&key, b"value_data_here", None)?;
            }
            Ok(())
        }
        #[cfg(feature = "turso")]
        Client::RedliteTursoEmbedded(db) => {
            for i in 0..size {
                let key = format!("key_{}", i);
                db.set(&key, b"value_data_here", None)?;
            }
            Ok(())
        }
    }
}

/// Benchmark GET operations
fn bench_get(
    client: &Client,
    size: usize,
    iterations: usize,
    connections: usize,
) -> Result<BenchResult, Box<dyn std::error::Error>> {
    if connections == 1 {
        // Sequential benchmark
        let start = Instant::now();
        for i in 0..iterations {
            let key = format!("key_{}", i % size);
            let _ = client.get(&key)?;
        }
        let elapsed = start.elapsed();
        let avg_us = (elapsed.as_nanos() as f64 / iterations as f64) / 1000.0;
        let throughput = iterations as f64 / elapsed.as_secs_f64();

        Ok(BenchResult {
            backend: "".to_string(),
            size,
            connections,
            operation: "GET".to_string(),
            avg_latency_us: avg_us,
            total_ops: iterations,
            throughput_ops_per_sec: throughput,
        })
    } else {
        // Multi-connection benchmark with proper async/await
        let rt = tokio::runtime::Runtime::new()?;
        let start = Instant::now();

        rt.block_on(async {
            let mut tasks = vec![];
            let ops_per_conn = iterations / connections;

            for conn_id in 0..connections {
                // Clone client for this task
                let client_clone = client.clone_for_async();
                let size_clone = size;

                let task = tokio::spawn(async move {
                    // Each connection performs its share of operations
                    for i in 0..ops_per_conn {
                        let key = format!("key_{}", (conn_id * (size_clone / connections) + i) % size_clone);
                        // Execute the actual GET operation
                        let _ = client_clone.get(&key);
                    }
                });
                tasks.push(task);
            }

            // Wait for all tasks to complete
            for task in tasks {
                let _ = task.await;
            }
        });

        let elapsed = start.elapsed();
        let avg_us = (elapsed.as_nanos() as f64 / iterations as f64) / 1000.0;
        let throughput = iterations as f64 / elapsed.as_secs_f64();

        Ok(BenchResult {
            backend: "".to_string(),
            size,
            connections,
            operation: "GET".to_string(),
            avg_latency_us: avg_us,
            total_ops: iterations,
            throughput_ops_per_sec: throughput,
        })
    }
}

/// Benchmark SET operations
fn bench_set(
    client: &Client,
    size: usize,
    iterations: usize,
    connections: usize,
) -> Result<BenchResult, Box<dyn std::error::Error>> {
    if connections == 1 {
        let start = Instant::now();
        for i in 0..iterations {
            let key = format!("new_key_{}", size + i);
            client.set(&key, b"value_data_here")?;
        }
        let elapsed = start.elapsed();
        let avg_us = (elapsed.as_nanos() as f64 / iterations as f64) / 1000.0;
        let throughput = iterations as f64 / elapsed.as_secs_f64();

        Ok(BenchResult {
            backend: "".to_string(),
            size,
            connections,
            operation: "SET".to_string(),
            avg_latency_us: avg_us,
            total_ops: iterations,
            throughput_ops_per_sec: throughput,
        })
    } else {
        // Multi-connection benchmark with proper async/await
        let rt = tokio::runtime::Runtime::new()?;
        let start = Instant::now();

        rt.block_on(async {
            let mut tasks = vec![];
            let ops_per_conn = iterations / connections;

            for conn_id in 0..connections {
                // Clone client for this task
                let client_clone = client.clone_for_async();
                let size_clone = size;

                let task = tokio::spawn(async move {
                    // Each connection performs its share of SET operations
                    for i in 0..ops_per_conn {
                        let key = format!("new_key_{}_{}", size_clone + conn_id, i);
                        // Execute the actual SET operation
                        let _ = client_clone.set(&key, b"value_data_here");
                    }
                });
                tasks.push(task);
            }

            // Wait for all tasks to complete
            for task in tasks {
                let _ = task.await;
            }
        });

        let elapsed = start.elapsed();
        let avg_us = (elapsed.as_nanos() as f64 / iterations as f64) / 1000.0;
        let throughput = iterations as f64 / elapsed.as_secs_f64();

        Ok(BenchResult {
            backend: "".to_string(),
            size,
            connections,
            operation: "SET".to_string(),
            avg_latency_us: avg_us,
            total_ops: iterations,
            throughput_ops_per_sec: throughput,
        })
    }
}

fn main() {
    println!("\n╔═══════════════════════════════════════════════════════════════════════════════╗");
    println!("║  COMPREHENSIVE REDIS-LIKE STORE BENCHMARK                                     ║");
    println!("║  Comparing: Redis, Dragonfly, Redlite (All Variants)                         ║");
    println!("╚═══════════════════════════════════════════════════════════════════════════════╝\n");

    // Test all SQLite variants
    let backends = vec![
        Backend::Redis,
        Backend::RedliteEmbeddedMemorySqlite,
        Backend::RedliteEmbeddedFileSqlite,
        Backend::RedliteServerMemorySqlite,
        Backend::RedliteServerFileSqlite,
    ];

    // Start with just one backend and small dataset for testing
    let sizes = vec![1_000]; // 1K dataset to start
    let connection_counts = vec![1, 2, 4, 8]; // Test scaling

    let mut results: Vec<BenchResult> = Vec::new();

    println!("Testing configuration:");
    println!("  Dataset sizes: {:?}", sizes);
    println!("  Connection counts: {:?}", connection_counts);
    println!("  Operations per test: 10,000 (small for quick testing)\n");

    println!("Checking backend availability...");
    for backend in &backends {
        if let Some(_client) = setup_client(*backend) {
            println!("  ✓ {} - Available", backend.name());
        } else {
            println!("  ✗ {} - Not available (skipping)", backend.name());
        }
    }
    println!();

    // Run benchmarks
    for backend in &backends {
        println!("═══ Benchmarking: {} ═══", backend.name());

        if let Some(client) = setup_client(*backend) {
            for &size in &sizes {
                println!("  Dataset size: {}", size);

                // Populate
                print!("    Populating... ");
                std::io::Write::flush(&mut std::io::stdout()).unwrap();
                if let Err(e) = populate(&client, size) {
                    println!("FAILED: {}", e);
                    println!("    Skipping this backend due to population failure");
                    break; // Skip all sizes for this backend if population fails
                }
                println!("✓");

                for &connections in &connection_counts {
                    let iterations = 10_000; // Fixed small number for testing

                    print!("    {} connections: ", connections);
                    std::io::Write::flush(&mut std::io::stdout()).unwrap();

                    // GET benchmark
                    if let Ok(mut result) = bench_get(&client, size, iterations, connections) {
                        result.backend = backend.name().to_string();
                        print!("GET {:.2}µs ", result.avg_latency_us);
                        results.push(result);
                    }

                    // SET benchmark
                    if let Ok(mut result) = bench_set(&client, size, iterations, connections) {
                        result.backend = backend.name().to_string();
                        print!("SET {:.2}µs ", result.avg_latency_us);
                        results.push(result);
                    }

                    println!();
                }
                println!();
            }
        } else {
            println!("  Skipped (backend not available)\n");
        }
    }

    // Print summary table
    println!("\n╔═══════════════════════════════════════════════════════════════════════════════╗");
    println!("║  BENCHMARK SUMMARY                                                            ║");
    println!("╚═══════════════════════════════════════════════════════════════════════════════╝\n");

    // Group by operation and size
    for operation in &["GET", "SET"] {
        println!("=== {} Operation ===\n", operation);

        for &size in &sizes {
            println!("Dataset: {} keys", size);
            println!("{:<40} {:>10} {:>12} {:>15}",
                     "Backend", "Conns", "Latency(µs)", "Throughput(ops/s)");
            println!("{}", "-".repeat(80));

            for result in &results {
                if result.operation == *operation && result.size == size {
                    println!("{:<40} {:>10} {:>12.2} {:>15.0}",
                             result.backend,
                             result.connections,
                             result.avg_latency_us,
                             result.throughput_ops_per_sec);
                }
            }
            println!();
        }
    }

    println!("\n✓ Comprehensive benchmark complete.\n");
}
