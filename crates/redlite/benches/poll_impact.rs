//! Poll Impact Benchmarks
//!
//! Session 35.2: Measure polling overhead for blocking operations.
//!
//! Run with: cargo bench --bench poll_impact
//! Quick test: cargo bench --bench poll_impact -- --test

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use redlite::db::Db;
use redlite::types::PollConfig;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

/// Benchmark 1: Baseline SET/GET throughput (no blocking operations)
fn bench_baseline_set_get(c: &mut Criterion) {
    let db = Db::open_memory().unwrap();

    let mut group = c.benchmark_group("baseline");
    group.throughput(Throughput::Elements(1000));

    group.bench_function("set_get_1k", |b| {
        b.iter(|| {
            for i in 0..1000 {
                let key = format!("key:{}", i);
                db.set(&key, b"value", None).unwrap();
                let _ = black_box(db.get(&key));
            }
        })
    });

    group.finish();
}

/// Benchmark 2: Baseline LPUSH/LPOP throughput
fn bench_baseline_lpush_lpop(c: &mut Criterion) {
    let db = Db::open_memory().unwrap();

    let mut group = c.benchmark_group("baseline");
    group.throughput(Throughput::Elements(1000));

    group.bench_function("lpush_lpop_1k", |b| {
        b.iter(|| {
            for i in 0..1000 {
                let key = format!("list:{}", i % 10);
                db.rpush(&key, &[format!("item{}", i).as_bytes()]).unwrap();
            }
            for i in 0..1000 {
                let key = format!("list:{}", i % 10);
                let _ = black_box(db.lpop(&key, Some(1)));
            }
        })
    });

    group.finish();
}

/// Benchmark 3: Baseline HSET/HGET throughput
fn bench_baseline_hset_hget(c: &mut Criterion) {
    let db = Db::open_memory().unwrap();

    let mut group = c.benchmark_group("baseline");
    group.throughput(Throughput::Elements(1000));

    group.bench_function("hset_hget_1k", |b| {
        b.iter(|| {
            for i in 0..1000 {
                let key = format!("hash:{}", i % 100);
                db.hset(&key, &[("field", format!("value{}", i).as_bytes())])
                    .unwrap();
                let _ = black_box(db.hget(&key, "field"));
            }
        })
    });

    group.finish();
}

/// Helper: spawn waiter threads that poll blpop_sync
fn spawn_waiters(
    db_path: &str,
    count: usize,
    poll_config: PollConfig,
    stop_flag: Arc<AtomicBool>,
) -> Vec<thread::JoinHandle<u64>> {
    (0..count)
        .map(|i| {
            let path = db_path.to_string();
            let flag = stop_flag.clone();
            let config = poll_config.clone();

            thread::spawn(move || {
                let db = Db::open(&path).unwrap();
                db.set_poll_config(config);

                let key = format!("wait_key:{}", i);
                let mut polls = 0u64;

                while !flag.load(Ordering::Relaxed) {
                    // Short timeout so we can check stop flag
                    let _ = db.blpop_sync(&[&key], 0.01);
                    polls += 1;
                }

                polls
            })
        })
        .collect()
}

/// Benchmark 4: Throughput with concurrent waiters
fn bench_throughput_with_waiters(c: &mut Criterion) {
    let tmp = tempfile::NamedTempFile::new().unwrap();
    let db_path = tmp.path().to_str().unwrap();

    // Create database
    let db = Db::open(db_path).unwrap();

    let mut group = c.benchmark_group("throughput_with_waiters");
    group.throughput(Throughput::Elements(1000));
    group.sample_size(20);

    for waiter_count in [1, 10, 50] {
        group.bench_with_input(
            BenchmarkId::new("set_get_1k", format!("{}_waiters", waiter_count)),
            &waiter_count,
            |b, &count| {
                let stop_flag = Arc::new(AtomicBool::new(false));
                let handles = spawn_waiters(db_path, count, PollConfig::default(), stop_flag.clone());

                b.iter(|| {
                    for i in 0..1000 {
                        let key = format!("bench_key:{}", i);
                        db.set(&key, b"value", None).unwrap();
                        let _ = black_box(db.get(&key));
                    }
                });

                stop_flag.store(true, Ordering::Relaxed);
                for h in handles {
                    let _ = h.join();
                }
            },
        );
    }

    group.finish();
}

/// Benchmark 5: Compare polling configs
fn bench_poll_config_comparison(c: &mut Criterion) {
    let tmp = tempfile::NamedTempFile::new().unwrap();
    let db_path = tmp.path().to_str().unwrap();

    let db = Db::open(db_path).unwrap();

    let mut group = c.benchmark_group("poll_config_comparison");
    group.throughput(Throughput::Elements(1000));
    group.sample_size(20);

    let configs = [
        ("aggressive", PollConfig::aggressive()),
        ("default", PollConfig::default()),
        ("relaxed", PollConfig::relaxed()),
    ];

    for (name, config) in configs {
        group.bench_with_input(
            BenchmarkId::new("set_get_10_waiters", name),
            &config,
            |b, cfg| {
                let stop_flag = Arc::new(AtomicBool::new(false));
                let handles = spawn_waiters(db_path, 10, cfg.clone(), stop_flag.clone());

                b.iter(|| {
                    for i in 0..1000 {
                        let key = format!("bench_key:{}", i);
                        db.set(&key, b"value", None).unwrap();
                        let _ = black_box(db.get(&key));
                    }
                });

                stop_flag.store(true, Ordering::Relaxed);
                for h in handles {
                    let _ = h.join();
                }
            },
        );
    }

    group.finish();
}

/// Benchmark 6: Latency when data already exists
fn bench_latency_immediate_data(c: &mut Criterion) {
    let db = Db::open_memory().unwrap();

    // Pre-populate lists
    for i in 0..100 {
        let key = format!("list:{}", i);
        db.rpush(&key, &[b"item"]).unwrap();
    }

    let mut group = c.benchmark_group("latency");
    group.throughput(Throughput::Elements(100));

    group.bench_function("blpop_immediate_100", |b| {
        b.iter(|| {
            for i in 0..100 {
                let key = format!("list:{}", i);
                // Replenish before each pop
                db.rpush(&key, &[b"item"]).unwrap();
                let result = db.blpop_sync(&[&key], 0.001);
                let _ = black_box(result);
            }
        })
    });

    group.finish();
}

/// Benchmark 7: Latency when push arrives during wait
fn bench_latency_push_during_wait(c: &mut Criterion) {
    let tmp = tempfile::NamedTempFile::new().unwrap();
    let db_path = tmp.path().to_str().unwrap();

    let mut group = c.benchmark_group("latency");
    group.sample_size(50);

    for (name, config) in [
        ("aggressive", PollConfig::aggressive()),
        ("default", PollConfig::default()),
        ("relaxed", PollConfig::relaxed()),
    ] {
        group.bench_function(BenchmarkId::new("push_during_wait", name), |b| {
            b.iter_custom(|iters| {
                let mut total = Duration::ZERO;

                for _ in 0..iters {
                    let db_waiter = Db::open(db_path).unwrap();
                    db_waiter.set_poll_config(config.clone());
                    let db_pusher = Db::open(db_path).unwrap();

                    // Clear any existing data
                    let _ = db_waiter.del(&["bench_list"]);

                    let start = Instant::now();

                    // Spawn waiter in background
                    let waiter_handle = thread::spawn(move || {
                        db_waiter.blpop_sync(&["bench_list"], 5.0)
                    });

                    // Small delay then push
                    thread::sleep(Duration::from_micros(500));
                    db_pusher.rpush("bench_list", &[b"data"]).unwrap();

                    // Wait for result
                    let _ = waiter_handle.join();

                    total += start.elapsed();
                }

                total
            })
        });
    }

    group.finish();
}

/// Benchmark 8: Waiter scaling (CPU impact)
fn bench_waiter_scaling(c: &mut Criterion) {
    let tmp = tempfile::NamedTempFile::new().unwrap();
    let db_path = tmp.path().to_str().unwrap();

    let _ = Db::open(db_path).unwrap(); // Initialize

    let mut group = c.benchmark_group("waiter_scaling");
    group.sample_size(10);

    for count in [1, 5, 10, 25, 50] {
        group.bench_with_input(
            BenchmarkId::new("poll_iterations", count),
            &count,
            |b, &waiter_count| {
                b.iter_custom(|iters| {
                    let mut total_polls = 0u64;

                    for _ in 0..iters {
                        let stop_flag = Arc::new(AtomicBool::new(false));
                        let handles =
                            spawn_waiters(db_path, waiter_count, PollConfig::default(), stop_flag.clone());

                        // Let them run for a fixed time
                        thread::sleep(Duration::from_millis(100));

                        stop_flag.store(true, Ordering::Relaxed);
                        for h in handles {
                            total_polls += h.join().unwrap_or(0);
                        }
                    }

                    // Return duration proportional to poll count (for comparison)
                    Duration::from_nanos(total_polls)
                })
            },
        );
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_baseline_set_get,
    bench_baseline_lpush_lpop,
    bench_baseline_hset_hget,
    bench_throughput_with_waiters,
    bench_poll_config_comparison,
    bench_latency_immediate_data,
    bench_latency_push_during_wait,
    bench_waiter_scaling,
);

criterion_main!(benches);
