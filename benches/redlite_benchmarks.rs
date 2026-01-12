use criterion::{black_box, criterion_group, criterion_main, Criterion, BenchmarkId};
use redlite::{Db, ZMember};
use std::sync::Arc;
use std::time::Duration;

// Helper to create a test instance
fn create_db() -> Db {
    Db::open_memory().expect("Failed to create Db instance")
}

// ============================================================================
// STRING OPERATIONS
// ============================================================================

fn bench_string_set(c: &mut Criterion) {
    let db = create_db();

    c.bench_function("string_set_64b", |b| {
        let mut counter = 0;
        let value = "x".repeat(64);
        b.iter(|| {
            db.set(
                black_box(&format!("key_{}", counter)),
                black_box(value.as_bytes()),
                None,
            ).expect("SET failed");
            counter += 1;
        });
    });
}

fn bench_string_get(c: &mut Criterion) {
    let db = create_db();
    db.set("bench_key", b"value", None).expect("SET failed");

    c.bench_function("string_get", |b| {
        b.iter(|| {
            let _result = db.get(black_box("bench_key")).expect("GET failed");
        });
    });
}

fn bench_string_operations(c: &mut Criterion) {
    let mut group = c.benchmark_group("string_operations");

    let db = Arc::new(create_db());

    // Test different value sizes
    for size in [64, 1024, 10240].iter() {
        group.bench_with_input(BenchmarkId::from_parameter(format!("{}B", size)), size, |b, &size| {
            let value = vec![b'x'; size];
            let mut counter = 0;
            let db_clone = Arc::clone(&db);
            b.iter(|| {
                db_clone.set(
                    black_box(&format!("key_{}", counter)),
                    black_box(&value),
                    None,
                ).expect("SET failed");
                counter += 1;
            });
        });
    }

    group.finish();
}

fn bench_string_incr(c: &mut Criterion) {
    let db = create_db();
    db.set("counter", b"0", None).expect("SET failed");

    c.bench_function("string_incr", |b| {
        b.iter(|| {
            db.incr(black_box("counter")).expect("INCR failed");
        });
    });
}

fn bench_string_append(c: &mut Criterion) {
    let db = create_db();
    db.set("append_key", b"initial", None).expect("SET failed");

    c.bench_function("string_append", |b| {
        b.iter(|| {
            db.append(black_box("append_key"), black_box(b"x")).expect("APPEND failed");
        });
    });
}

// ============================================================================
// HASH OPERATIONS
// ============================================================================

fn bench_hash_operations(c: &mut Criterion) {
    let mut group = c.benchmark_group("hash_operations");

    let db = Arc::new(create_db());

    // HSET with varying field counts
    for field_count in [10, 100, 1000].iter() {
        group.bench_with_input(
            BenchmarkId::from_parameter(format!("hset_{}_fields", field_count)),
            field_count,
            |b, &field_count| {
                let mut counter = 0;
                let db_clone = Arc::clone(&db);
                b.iter(|| {
                    let key = format!("hash_key_{}", counter);
                    let mut pairs = Vec::new();
                    for i in 0..field_count {
                        let field = format!("field_{}", i);
                        let value = format!("value_{}", i);
                        pairs.push((field.as_str(), value.as_bytes()));
                    }
                    db_clone.hset(
                        black_box(&key),
                        black_box(&pairs),
                    ).expect("HSET failed");
                    counter += 1;
                });
            },
        );
    }

    // HGET
    {
        let db_clone = Arc::clone(&db);
        db.hset("hash_bench", &[("field", b"value")]).expect("HSET failed");
        group.bench_function("hget", |b| {
            b.iter(|| {
                let _result = db_clone.hget(black_box("hash_bench"), black_box("field")).expect("HGET failed");
            });
        });
    }

    // HGETALL with varying field counts
    for field_count in [10, 100].iter() {
        group.bench_with_input(
            BenchmarkId::from_parameter(format!("hgetall_{}_fields", field_count)),
            field_count,
            |b, &field_count| {
                let key = format!("hgetall_key_{}", field_count);
                let mut pairs = Vec::new();
                for i in 0..field_count {
                    let field = format!("field_{}", i);
                    let value = format!("value_{}", i);
                    pairs.push((field.as_str(), value.as_bytes()));
                }
                db.hset(&key, &pairs).expect("HSET failed");
                let db_clone = Arc::clone(&db);
                b.iter(|| {
                    let _result = db_clone.hgetall(black_box(&key)).expect("HGETALL failed");
                });
            },
        );
    }

    group.finish();
}

// ============================================================================
// LIST OPERATIONS
// ============================================================================

fn bench_list_operations(c: &mut Criterion) {
    let mut group = c.benchmark_group("list_operations");

    let redlite = Arc::new(create_redlite());

    // LPUSH
    group.bench_function("lpush", |b| {
        let mut counter = 0;
        b.iter(|| {
            let key = format!("list_key_{}", counter);
            redlite.lpush(black_box(&key), black_box(&format!("value_{}", counter).as_bytes())).expect("LPUSH failed");
            counter += 1;
        });
    });

    // LPOP
    {
        let redlite_clone = Arc::clone(&redlite);
        for i in 0..100 {
            redlite.lpush(&format!("lpop_key_{}", i), &format!("value_{}", i).as_bytes()).expect("LPUSH failed");
        }
        group.bench_function("lpop", |b| {
            let mut counter = 0;
            b.iter(|| {
                let key = format!("lpop_key_{}", counter);
                let _result = redlite_clone.lpop(black_box(&key), black_box(1)).expect("LPOP failed");
                counter += 1;
            });
        });
    }

    // LRANGE with varying list lengths
    for list_len in [10, 100, 1000].iter() {
        group.bench_with_input(
            BenchmarkId::from_parameter(format!("lrange_{}_items", list_len)),
            list_len,
            |b, &list_len| {
                let key = format!("lrange_key_{}", list_len);
                for i in 0..list_len {
                    redlite.lpush(&key, &format!("value_{}", i).as_bytes()).expect("LPUSH failed");
                }
                let redlite_clone = Arc::clone(&redlite);
                b.iter(|| {
                    let _result = redlite_clone.lrange(black_box(&key), black_box(0), black_box(-1)).expect("LRANGE failed");
                });
            },
        );
    }

    group.finish();
}

// ============================================================================
// SET OPERATIONS
// ============================================================================

fn bench_set_operations(c: &mut Criterion) {
    let mut group = c.benchmark_group("set_operations");

    let redlite = Arc::new(create_redlite());

    // SADD
    group.bench_function("sadd", |b| {
        let mut counter = 0;
        b.iter(|| {
            let key = format!("set_key_{}", counter);
            redlite.sadd(black_box(&key), black_box(&format!("member_{}", counter).as_bytes())).expect("SADD failed");
            counter += 1;
        });
    });

    // SMEMBERS with varying cardinality
    for cardinality in [10, 100, 1000].iter() {
        group.bench_with_input(
            BenchmarkId::from_parameter(format!("smembers_{}_items", cardinality)),
            cardinality,
            |b, &cardinality| {
                let key = format!("smembers_key_{}", cardinality);
                for i in 0..cardinality {
                    redlite.sadd(&key, &format!("member_{}", i).as_bytes()).expect("SADD failed");
                }
                let redlite_clone = Arc::clone(&redlite);
                b.iter(|| {
                    let _result = redlite_clone.smembers(black_box(&key)).expect("SMEMBERS failed");
                });
            },
        );
    }

    group.finish();
}

// ============================================================================
// SORTED SET OPERATIONS
// ============================================================================

fn bench_sorted_set_operations(c: &mut Criterion) {
    let mut group = c.benchmark_group("sorted_set_operations");

    let redlite = Arc::new(create_redlite());

    // ZADD
    group.bench_function("zadd", |b| {
        let mut counter = 0.0;
        b.iter(|| {
            let key = format!("zset_key_{}", counter as i32);
            redlite.zadd(
                black_box(&key),
                black_box(&format!("member_{}", counter as i32)),
                black_box(counter),
            ).expect("ZADD failed");
            counter += 1.0;
        });
    });

    // ZRANGE
    {
        let redlite_clone = Arc::clone(&redlite);
        let key = "zrange_bench";
        for i in 0..100 {
            redlite.zadd(key, &format!("member_{}", i), i as f64).expect("ZADD failed");
        }
        group.bench_function("zrange", |b| {
            b.iter(|| {
                let _result = redlite_clone.zrange(black_box(key), black_box(0), black_box(-1)).expect("ZRANGE failed");
            });
        });
    }

    group.finish();
}

// ============================================================================
// MIXED WORKLOAD (80% reads, 20% writes)
// ============================================================================

fn bench_mixed_workload(c: &mut Criterion) {
    let redlite = Arc::new(create_redlite());

    // Prepare data
    for i in 0..1000 {
        redlite.set(
            &format!("key_{}", i),
            &format!("value_{}", i).as_bytes(),
        ).expect("SET failed");
    }

    c.bench_function("mixed_workload_80_20", |b| {
        let mut counter = 0;
        let redlite_clone = Arc::clone(&redlite);
        b.iter(|| {
            if counter % 5 < 4 {
                // 80% reads
                let _result = redlite_clone.get(black_box(&format!("key_{}", counter % 1000))).expect("GET failed");
            } else {
                // 20% writes
                redlite_clone.set(
                    black_box(&format!("key_{}", counter)),
                    black_box(&format!("value_{}", counter).as_bytes()),
                ).expect("SET failed");
            }
            counter += 1;
        });
    });
}

// ============================================================================
// CONCURRENT OPERATIONS
// ============================================================================

fn bench_concurrent_operations(c: &mut Criterion) {
    let mut group = c.benchmark_group("concurrent_operations");
    group.sample_size(10); // Reduce sample size for concurrent tests

    for thread_count in [1, 4, 8, 16].iter() {
        group.bench_with_input(
            BenchmarkId::from_parameter(format!("{}_threads", thread_count)),
            thread_count,
            |b, &thread_count| {
                let redlite = Arc::new(create_redlite());
                b.iter(|| {
                    let mut handles = vec![];
                    for t in 0..thread_count {
                        let redlite_clone = Arc::clone(&redlite);
                        let handle = std::thread::spawn(move || {
                            for i in 0..100 {
                                let key = format!("key_{}_{}", t, i);
                                redlite_clone.set(black_box(&key), black_box(b"value")).expect("SET failed");
                                let _result = redlite_clone.get(black_box(&key)).expect("GET failed");
                            }
                        });
                        handles.push(handle);
                    }
                    for handle in handles {
                        handle.join().expect("Thread join failed");
                    }
                });
            },
        );
    }

    group.finish();
}

// ============================================================================
// EXPIRATION OPERATIONS
// ============================================================================

fn bench_expiration(c: &mut Criterion) {
    let redlite = create_redlite();

    c.bench_function("expire", |b| {
        let mut counter = 0;
        b.iter(|| {
            let key = format!("key_{}", counter);
            redlite.set(&key, b"value").expect("SET failed");
            redlite.expire(black_box(&key), black_box(60)).expect("EXPIRE failed");
            counter += 1;
        });
    });
}

// ============================================================================
// Define benchmark groups
// ============================================================================

criterion_group!(
    benches,
    bench_string_set,
    bench_string_get,
    bench_string_operations,
    bench_string_incr,
    bench_string_append,
    bench_hash_operations,
    bench_list_operations,
    bench_set_operations,
    bench_sorted_set_operations,
    bench_mixed_workload,
    bench_concurrent_operations,
    bench_expiration,
);

criterion_main!(benches);
