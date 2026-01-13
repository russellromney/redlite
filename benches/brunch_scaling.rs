use redlite::{Db, ZMember};
use std::sync::Arc;
use std::time::Instant;

#[cfg(feature = "turso")]
use redlite::TursoDb;

fn main() {
    println!(
        "\n╔═══════════════════════════════════════════════════════════════════════════════╗"
    );
    println!(
        "║  Redlite Comprehensive Benchmarks (1K keys)                                  ║"
    );
    println!(
        "║  Testing: All command types + Single/Multi-connection scenarios              ║"
    );
    println!(
        "║  Backends: SQLite, Turso                                                     ║"
    );
    println!(
        "╚═══════════════════════════════════════════════════════════════════════════════╝\n"
    );

    let size = 1000usize;
    let iterations = 100_000usize;

    println!("=== SINGLE-THREADED BENCHMARKS (1K keys) ===\n");

    // String operations
    bench_string_ops(size, iterations);

    // Hash operations
    bench_hash_ops(size, iterations);

    // List operations
    bench_list_ops(size, iterations);

    // Set operations
    bench_set_ops(size, iterations);

    // Sorted set operations
    bench_sorted_set_ops(size, iterations);

    // Multi-connection benchmarks
    bench_multiconnection_ops(size, iterations);

    println!("\n✓ All benchmarks complete.\n");
}

fn bench_string_ops(size: usize, iterations: usize) {
    println!("STRING OPERATIONS");
    println!("─────────────────────────────────────────────────────────");

    // SQLite Memory
    {
        let db = Db::open_memory().unwrap();
        for i in 0..size {
            let key = format!("key_{}", i);
            db.set(&key, b"value_data", None).unwrap();
        }

        let start = Instant::now();
        let mut idx = 0;
        for _ in 0..iterations {
            let key = format!("key_{}", idx % size);
            let _ = db.get(&key);
            idx += 1;
        }
        let elapsed = start.elapsed();
        let avg_us = (elapsed.as_nanos() as f64 / iterations as f64) / 1000.0;
        println!("  [SQLite/Memory] GET:    {:.3} µs/op", avg_us);

        let start = Instant::now();
        for i in 0..iterations {
            let key = format!("new_key_{}", size + i);
            db.set(&key, b"value_data", None).unwrap();
        }
        let elapsed = start.elapsed();
        let avg_us = (elapsed.as_nanos() as f64 / iterations as f64) / 1000.0;
        println!("  [SQLite/Memory] SET:    {:.3} µs/op", avg_us);

        let start = Instant::now();
        db.set("counter", b"0", None).unwrap();
        for _ in 0..(iterations / 10) {
            let _ = db.incr("counter");
        }
        let elapsed = start.elapsed();
        let avg_us = (elapsed.as_nanos() as f64 / (iterations / 10) as f64) / 1000.0;
        println!("  [SQLite/Memory] INCR:   {:.3} µs/op", avg_us);

        let start = Instant::now();
        db.set("append_key", b"data", None).unwrap();
        for _ in 0..iterations {
            let _ = db.append("append_key", b"x");
        }
        let elapsed = start.elapsed();
        let avg_us = (elapsed.as_nanos() as f64 / iterations as f64) / 1000.0;
        println!("  [SQLite/Memory] APPEND: {:.3} µs/op", avg_us);
    }

    // SQLite File
    {
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = format!("/tmp/redlite_bench_{}.db", timestamp);
        let db = Db::open(&path).unwrap();

        for i in 0..size {
            let key = format!("key_{}", i);
            db.set(&key, b"value_data", None).unwrap();
        }

        let start = Instant::now();
        let mut idx = 0;
        for _ in 0..iterations {
            let key = format!("key_{}", idx % size);
            let _ = db.get(&key);
            idx += 1;
        }
        let elapsed = start.elapsed();
        let avg_us = (elapsed.as_nanos() as f64 / iterations as f64) / 1000.0;
        println!("  [SQLite/File]   GET:    {:.3} µs/op", avg_us);

        let start = Instant::now();
        for i in 0..iterations {
            let key = format!("new_key_{}", size + i);
            db.set(&key, b"value_data", None).unwrap();
        }
        let elapsed = start.elapsed();
        let avg_us = (elapsed.as_nanos() as f64 / iterations as f64) / 1000.0;
        println!("  [SQLite/File]   SET:    {:.3} µs/op", avg_us);

        let _ = std::fs::remove_file(&path);
    }

    // Turso
    #[cfg(feature = "turso")]
    {
        if let Ok(db) = TursoDb::open_memory() {
            for i in 0..size {
                let key = format!("key_{}", i);
                let _ = db.set(&key, b"value_data", None);
            }

            let start = Instant::now();
            let mut idx = 0;
            for _ in 0..iterations {
                let key = format!("key_{}", idx % size);
                let _ = db.get(&key);
                idx += 1;
            }
            let elapsed = start.elapsed();
            let avg_us = (elapsed.as_nanos() as f64 / iterations as f64) / 1000.0;
            println!("  [Turso/Memory]  GET:    {:.3} µs/op", avg_us);

            let start = Instant::now();
            for i in 0..iterations {
                let key = format!("new_key_{}", size + i);
                let _ = db.set(&key, b"value_data", None);
            }
            let elapsed = start.elapsed();
            let avg_us = (elapsed.as_nanos() as f64 / iterations as f64) / 1000.0;
            println!("  [Turso/Memory]  SET:    {:.3} µs/op", avg_us);
        }
    }

    println!();
}

fn bench_hash_ops(size: usize, iterations: usize) {
    println!("HASH OPERATIONS");
    println!("─────────────────────────────────────────────────────────");

    let db = Db::open_memory().unwrap();

    // Setup hash with 10 fields
    let num_fields = 10;
    let mut fields = Vec::new();
    for j in 0..num_fields {
        fields.push((format!("field_{}", j), format!("value_{}", j).into_bytes()));
    }
    let field_refs: Vec<(&str, &[u8])> = fields.iter().map(|(k, v)| (k.as_str(), v.as_slice())).collect();

    let start = Instant::now();
    for i in 0..(iterations / 100) {
        let key = format!("hash_{}", i % size);
        let _ = db.hset(&key, &field_refs);
    }
    let elapsed = start.elapsed();
    let avg_us = (elapsed.as_nanos() as f64 / (iterations / 100) as f64) / 1000.0;
    println!("  [SQLite/Memory] HSET (10 fields): {:.3} µs/op", avg_us);

    // Setup for HGET
    db.hset("test_hash", &field_refs).unwrap();
    let start = Instant::now();
    for _ in 0..iterations {
        let _ = db.hget("test_hash", "field_0");
    }
    let elapsed = start.elapsed();
    let avg_us = (elapsed.as_nanos() as f64 / iterations as f64) / 1000.0;
    println!("  [SQLite/Memory] HGET:             {:.3} µs/op", avg_us);

    // HGETALL
    let start = Instant::now();
    for _ in 0..(iterations / 10) {
        let _ = db.hgetall("test_hash");
    }
    let elapsed = start.elapsed();
    let avg_us = (elapsed.as_nanos() as f64 / (iterations / 10) as f64) / 1000.0;
    println!("  [SQLite/Memory] HGETALL:         {:.3} µs/op", avg_us);

    println!();
}

fn bench_list_ops(size: usize, iterations: usize) {
    println!("LIST OPERATIONS");
    println!("─────────────────────────────────────────────────────────");

    let db = Db::open_memory().unwrap();

    // LPUSH
    let start = Instant::now();
    for i in 0..iterations {
        let key = format!("list_{}", i % size);
        let value = format!("val_{}", i);
        let _ = db.lpush(&key, &[value.as_bytes()]);
    }
    let elapsed = start.elapsed();
    let avg_us = (elapsed.as_nanos() as f64 / iterations as f64) / 1000.0;
    println!("  [SQLite/Memory] LPUSH: {:.3} µs/op", avg_us);

    // Setup for LPOP
    for i in 0..100 {
        let _ = db.lpush(&format!("lpop_list_{}", i), &[format!("val_{}", i).as_bytes()]);
    }

    // LPOP
    let start = Instant::now();
    for i in 0..iterations {
        let key = format!("lpop_list_{}", i % 100);
        let _ = db.lpop(&key, Some(1));
    }
    let elapsed = start.elapsed();
    let avg_us = (elapsed.as_nanos() as f64 / iterations as f64) / 1000.0;
    println!("  [SQLite/Memory] LPOP:  {:.3} µs/op", avg_us);

    println!();
}

fn bench_set_ops(size: usize, iterations: usize) {
    println!("SET OPERATIONS");
    println!("─────────────────────────────────────────────────────────");

    let db = Db::open_memory().unwrap();

    // SADD
    let start = Instant::now();
    for i in 0..iterations {
        let key = format!("set_{}", i % size);
        let member = format!("member_{}", i);
        let _ = db.sadd(&key, &[member.as_bytes()]);
    }
    let elapsed = start.elapsed();
    let avg_us = (elapsed.as_nanos() as f64 / iterations as f64) / 1000.0;
    println!("  [SQLite/Memory] SADD:     {:.3} µs/op", avg_us);

    // Setup for SMEMBERS
    for i in 0..100 {
        let member = format!("member_{}", i);
        let _ = db.sadd("test_set", &[member.as_bytes()]);
    }

    // SMEMBERS
    let start = Instant::now();
    for _ in 0..(iterations / 100) {
        let _ = db.smembers("test_set");
    }
    let elapsed = start.elapsed();
    let avg_us = (elapsed.as_nanos() as f64 / (iterations / 100) as f64) / 1000.0;
    println!("  [SQLite/Memory] SMEMBERS: {:.3} µs/op", avg_us);

    println!();
}

fn bench_sorted_set_ops(size: usize, iterations: usize) {
    println!("SORTED SET OPERATIONS");
    println!("─────────────────────────────────────────────────────────");

    let db = Db::open_memory().unwrap();

    // ZADD
    let start = Instant::now();
    for i in 0..iterations {
        let key = format!("zset_{}", i % size);
        let member = format!("member_{}", i);
        let _ = db.zadd(
            &key,
            &[ZMember {
                score: i as f64,
                member: member.into_bytes(),
            }],
        );
    }
    let elapsed = start.elapsed();
    let avg_us = (elapsed.as_nanos() as f64 / iterations as f64) / 1000.0;
    println!("  [SQLite/Memory] ZADD:   {:.3} µs/op", avg_us);

    // Setup for ZRANGE
    for i in 0..100 {
        let member = format!("member_{}", i);
        let _ = db.zadd(
            "test_zset",
            &[ZMember {
                score: i as f64,
                member: member.into_bytes(),
            }],
        );
    }

    // ZRANGE
    let start = Instant::now();
    for _ in 0..(iterations / 100) {
        let _ = db.zrange("test_zset", 0, -1, false);
    }
    let elapsed = start.elapsed();
    let avg_us = (elapsed.as_nanos() as f64 / (iterations / 100) as f64) / 1000.0;
    println!("  [SQLite/Memory] ZRANGE: {:.3} µs/op", avg_us);

    println!();
}

fn bench_multiconnection_ops(size: usize, iterations: usize) {
    println!("MULTI-CONNECTION BENCHMARKS");
    println!("─────────────────────────────────────────────────────────");

    let rt = tokio::runtime::Runtime::new().unwrap();

    // Single-threaded baseline (sequential)
    {
        let db = Arc::new(Db::open_memory().unwrap());
        for i in 0..size {
            let key = format!("key_{}", i);
            db.set(&key, b"value", None).unwrap();
        }

        let start = Instant::now();
        let mut idx = 0;
        for _ in 0..(iterations / 10) {
            let key = format!("key_{}", idx % size);
            let _ = db.get(&key);
            idx += 1;
        }
        let elapsed = start.elapsed();
        let avg_us = (elapsed.as_nanos() as f64 / (iterations / 10) as f64) / 1000.0;
        println!("  [Sequential] GET:         {:.3} µs/op", avg_us);
    }

    // Concurrent - 2 connections
    {
        let db = Arc::new(Db::open_memory().unwrap());
        for i in 0..size {
            let key = format!("key_{}", i);
            db.set(&key, b"value", None).unwrap();
        }

        let start = Instant::now();
        rt.block_on(async {
            let mut tasks = vec![];
            for conn_id in 0..2 {
                let db = Arc::clone(&db);
                let task = tokio::spawn(async move {
                    for i in 0..(iterations / 20) {
                        let key = format!("key_{}", (conn_id * 500 + i) % size);
                        let _ = db.get(&key);
                    }
                });
                tasks.push(task);
            }
            for task in tasks {
                let _ = task.await;
            }
        });
        let elapsed = start.elapsed();
        let avg_us = (elapsed.as_nanos() as f64 / (iterations / 10) as f64) / 1000.0;
        println!("  [2 Connections] GET:      {:.3} µs/op", avg_us);
    }

    // Concurrent - 4 connections
    {
        let db = Arc::new(Db::open_memory().unwrap());
        for i in 0..size {
            let key = format!("key_{}", i);
            db.set(&key, b"value", None).unwrap();
        }

        let start = Instant::now();
        rt.block_on(async {
            let mut tasks = vec![];
            for conn_id in 0..4 {
                let db = Arc::clone(&db);
                let task = tokio::spawn(async move {
                    for i in 0..(iterations / 40) {
                        let key = format!("key_{}", (conn_id * 250 + i) % size);
                        let _ = db.get(&key);
                    }
                });
                tasks.push(task);
            }
            for task in tasks {
                let _ = task.await;
            }
        });
        let elapsed = start.elapsed();
        let avg_us = (elapsed.as_nanos() as f64 / (iterations / 10) as f64) / 1000.0;
        println!("  [4 Connections] GET:      {:.3} µs/op", avg_us);
    }

    // Concurrent - 8 connections
    {
        let db = Arc::new(Db::open_memory().unwrap());
        for i in 0..size {
            let key = format!("key_{}", i);
            db.set(&key, b"value", None).unwrap();
        }

        let start = Instant::now();
        rt.block_on(async {
            let mut tasks = vec![];
            for conn_id in 0..8 {
                let db = Arc::clone(&db);
                let task = tokio::spawn(async move {
                    for i in 0..(iterations / 80) {
                        let key = format!("key_{}", (conn_id * 125 + i) % size);
                        let _ = db.get(&key);
                    }
                });
                tasks.push(task);
            }
            for task in tasks {
                let _ = task.await;
            }
        });
        let elapsed = start.elapsed();
        let avg_us = (elapsed.as_nanos() as f64 / (iterations / 10) as f64) / 1000.0;
        println!("  [8 Connections] GET:      {:.3} µs/op", avg_us);
    }

    // Mixed workload with concurrent connections
    {
        let db = Arc::new(Db::open_memory().unwrap());
        for i in 0..size {
            let key = format!("key_{}", i);
            db.set(&key, b"value", None).unwrap();
        }

        let start = Instant::now();
        rt.block_on(async {
            let mut tasks = vec![];
            for conn_id in 0..4 {
                let db = Arc::clone(&db);
                let task = tokio::spawn(async move {
                    for i in 0..(iterations / 40) {
                        let idx = (conn_id * 250 + i) % size;
                        if i % 5 == 0 {
                            // 20% writes
                            let key = format!("new_key_{}_{}", conn_id, i);
                            let _ = db.set(&key, b"value", None);
                        } else {
                            // 80% reads
                            let key = format!("key_{}", idx);
                            let _ = db.get(&key);
                        }
                    }
                });
                tasks.push(task);
            }
            for task in tasks {
                let _ = task.await;
            }
        });
        let elapsed = start.elapsed();
        let avg_us = (elapsed.as_nanos() as f64 / (iterations / 10) as f64) / 1000.0;
        println!("  [4 Conn Mixed] 80/20 R/W:  {:.3} µs/op", avg_us);
    }

    println!();
}
