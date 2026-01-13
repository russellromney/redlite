use redlite::Db;
use std::sync::Arc;
use std::time::Instant;

fn main() {
    println!("\n✓ Quick Concurrent Benchmark Test\n");

    // Test 1: Single connection baseline
    println!("Test 1: Single connection baseline (1000 ops)");
    let db = Arc::new(Db::open_memory().unwrap());

    // Populate
    for i in 0..100 {
        db.set(&format!("key_{}", i), b"test_value", None).unwrap();
    }

    let start = Instant::now();
    for i in 0..1000 {
        let _ = db.get(&format!("key_{}", i % 100));
    }
    let elapsed = start.elapsed();
    let avg_us = (elapsed.as_nanos() as f64 / 1000.0) / 1000.0;
    println!("  Avg latency: {:.2}µs", avg_us);
    println!("  Total time: {:.2}ms\n", elapsed.as_secs_f64() * 1000.0);

    // Test 2: Multi-connection async benchmark
    println!("Test 2: 4-connection async benchmark (1000 ops total)");
    let rt = tokio::runtime::Runtime::new().unwrap();
    let db2 = Arc::new(Db::open_memory().unwrap());

    // Populate
    for i in 0..100 {
        db2.set(&format!("key_{}", i), b"test_value", None).unwrap();
    }

    let start = Instant::now();
    rt.block_on(async {
        let mut tasks = vec![];
        let ops_per_conn = 250; // 4 connections * 250 = 1000

        for conn_id in 0..4 {
            let db_clone = Arc::clone(&db2);
            let task = tokio::spawn(async move {
                for i in 0..ops_per_conn {
                    let key = format!("key_{}", (conn_id * 25 + i) % 100);
                    let _ = db_clone.get(&key);
                }
            });
            tasks.push(task);
        }

        for task in tasks {
            let _ = task.await;
        }
    });
    let elapsed = start.elapsed();
    let avg_us = (elapsed.as_nanos() as f64 / 1000.0) / 1000.0;
    println!("  Avg latency: {:.2}µs", avg_us);
    println!("  Total time: {:.2}ms", elapsed.as_secs_f64() * 1000.0);
    println!("  Throughput: {:.0} ops/s\n", 1000.0 / elapsed.as_secs_f64());

    println!("✓ Tests completed successfully!");
}
