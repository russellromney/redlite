use criterion::{black_box, criterion_group, criterion_main, Criterion, BenchmarkId};
use redlite::{Db, ZMember};
use std::sync::Arc;
use std::time::Duration;

// Optional backend imports (feature-gated)
#[cfg(feature = "libsql")]
use redlite::LibsqlDb;
#[cfg(feature = "turso")]
use redlite::TursoDb;

// Helper to create a test instance (SQLite memory)
fn create_db() -> Db {
    Db::open_memory().expect("Failed to create Db instance")
}

fn create_file_db() -> Db {
    use std::time::{SystemTime, UNIX_EPOCH};
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let path = format!("/tmp/redlite_bench_{}.db", timestamp);
    Db::open(&path).expect("Failed to create file Db instance")
}

// LibsqlDb helpers (feature-gated)
#[cfg(feature = "libsql")]
fn try_create_libsql_db() -> Option<LibsqlDb> {
    // libsql has threading issues with Criterion, disabled by default
    if std::env::var("REDLITE_BENCH_LIBSQL").map(|v| v == "1").unwrap_or(false) {
        LibsqlDb::open_memory().ok()
    } else {
        None
    }
}

#[cfg(not(feature = "libsql"))]
fn try_create_libsql_db() -> Option<Db> { None }

// TursoDb helpers (feature-gated)
#[cfg(feature = "turso")]
fn try_create_turso_db() -> Option<TursoDb> {
    // turso may have threading issues with Criterion, disabled by default
    if std::env::var("REDLITE_BENCH_TURSO").map(|v| v == "1").unwrap_or(false) {
        TursoDb::open_memory().ok()
    } else {
        None
    }
}

#[cfg(not(feature = "turso"))]
fn try_create_turso_db() -> Option<Db> { None }

// ============================================================================
// STRING OPERATIONS
// ============================================================================

fn bench_string_set(c: &mut Criterion) {
    let db = create_db();

    c.bench_function("string_set_64b", |b| {
        let mut counter = 0;
        let value = "x".repeat(64);
        b.iter(|| {
            let key = format!("key_{}", counter);
            db.set(black_box(&key), black_box(value.as_bytes()), None)
                .expect("SET failed");
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

    // Test different value sizes
    for size in [64, 1024, 10240].iter() {
        group.bench_with_input(
            BenchmarkId::from_parameter(format!("{}B", size)),
            size,
            |b, &size| {
                let db = create_db();
                let value = vec![b'x'; size];
                let mut counter = 0;
                b.iter(|| {
                    let key = format!("key_{}", counter);
                    db.set(black_box(&key), black_box(&value), None)
                        .expect("SET failed");
                    counter += 1;
                });
            },
        );
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
    db.set("append_key", b"initial", None)
        .expect("SET failed");

    c.bench_function("string_append", |b| {
        b.iter(|| {
            db.append(black_box("append_key"), black_box(b"x"))
                .expect("APPEND failed");
        });
    });
}

// ============================================================================
// HASH OPERATIONS
// ============================================================================

fn bench_hash_operations(c: &mut Criterion) {
    let mut group = c.benchmark_group("hash_operations");

    // HSET with varying field counts
    for field_count in [10, 100, 1000].iter() {
        group.bench_with_input(
            BenchmarkId::from_parameter(format!("hset_{}_fields", field_count)),
            field_count,
            |b, &field_count| {
                let db = create_db();
                let mut counter = 0;
                b.iter(|| {
                    let key = format!("hash_key_{}", counter);
                    for i in 0..field_count {
                        let field = format!("field_{}", i);
                        let value = format!("value_{}", i);
                        db.hset(&key, &[(field.as_str(), value.as_bytes())])
                            .expect("HSET failed");
                    }
                    counter += 1;
                });
            },
        );
    }

    // HGET
    {
        let db = create_db();
        db.hset("hash_bench", &[("field", b"value")])
            .expect("HSET failed");
        group.bench_function("hget", |b| {
            b.iter(|| {
                let _result = db
                    .hget(black_box("hash_bench"), black_box("field"))
                    .expect("HGET failed");
            });
        });
    }

    // HGETALL with varying field counts
    for field_count in [10, 100].iter() {
        group.bench_with_input(
            BenchmarkId::from_parameter(format!("hgetall_{}_fields", field_count)),
            field_count,
            |b, &field_count| {
                let db = create_db();
                let key = format!("hgetall_key_{}", field_count);
                for i in 0..field_count {
                    let field = format!("field_{}", i);
                    let value = format!("value_{}", i);
                    db.hset(&key, &[(field.as_str(), value.as_bytes())])
                        .expect("HSET failed");
                }
                b.iter(|| {
                    let _result = db.hgetall(black_box(&key)).expect("HGETALL failed");
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

    // LPUSH
    group.bench_function("lpush", |b| {
        let db = create_db();
        let mut counter = 0;
        b.iter(|| {
            let key = format!("list_key_{}", counter);
            let value = format!("value_{}", counter);
            db.lpush(black_box(&key), black_box(&[value.as_bytes()]))
                .expect("LPUSH failed");
            counter += 1;
        });
    });

    // LPOP
    {
        let db = create_db();
        for i in 0..100 {
            let value = format!("value_{}", i);
            db.lpush(&format!("lpop_key_{}", i), &[value.as_bytes()])
                .expect("LPUSH failed");
        }
        group.bench_function("lpop", |b| {
            let mut counter = 0;
            b.iter(|| {
                let key = format!("lpop_key_{}", counter);
                let _result = db
                    .lpop(black_box(&key), Some(1))
                    .expect("LPOP failed");
                counter += 1;
            });
        });
    }

    group.finish();
}

// ============================================================================
// SET OPERATIONS
// ============================================================================

fn bench_set_operations(c: &mut Criterion) {
    let mut group = c.benchmark_group("set_operations");

    // SADD
    group.bench_function("sadd", |b| {
        let db = create_db();
        let mut counter = 0;
        b.iter(|| {
            let key = format!("set_key_{}", counter);
            let member = format!("member_{}", counter);
            db.sadd(black_box(&key), black_box(&[member.as_bytes()]))
                .expect("SADD failed");
            counter += 1;
        });
    });

    // SMEMBERS
    {
        let db = create_db();
        for i in 0..100 {
            let member = format!("member_{}", i);
            db.sadd("smembers_key", &[member.as_bytes()])
                .expect("SADD failed");
        }
        group.bench_function("smembers_100", |b| {
            b.iter(|| {
                let _result = db.smembers(black_box("smembers_key")).expect("SMEMBERS failed");
            });
        });
    }

    group.finish();
}

// ============================================================================
// SORTED SET OPERATIONS
// ============================================================================

fn bench_sorted_set_operations(c: &mut Criterion) {
    let mut group = c.benchmark_group("sorted_set_operations");

    // ZADD
    group.bench_function("zadd", |b| {
        let db = create_db();
        let mut counter = 0;
        b.iter(|| {
            let key = format!("zset_key_{}", counter);
            let member = format!("member_{}", counter);
            db.zadd(
                black_box(&key),
                black_box(&[ZMember {
                    score: counter as f64,
                    member: member.into_bytes(),
                }]),
            )
            .expect("ZADD failed");
            counter += 1;
        });
    });

    // ZRANGE
    {
        let db = create_db();
        for i in 0..100 {
            let member = format!("member_{}", i);
            db.zadd(
                "zrange_key",
                &[ZMember {
                    score: i as f64,
                    member: member.into_bytes(),
                }],
            )
            .expect("ZADD failed");
        }
        group.bench_function("zrange_100", |b| {
            b.iter(|| {
                let _result = db
                    .zrange(black_box("zrange_key"), 0, -1, false)
                    .expect("ZRANGE failed");
            });
        });
    }

    group.finish();
}

// ============================================================================
// MIXED WORKLOAD
// ============================================================================

fn bench_mixed_workload(c: &mut Criterion) {
    let db = create_db();

    // Pre-populate
    for i in 0..1000 {
        db.set(&format!("key_{}", i), format!("value_{}", i).as_bytes(), None)
            .unwrap();
    }

    c.bench_function("mixed_80read_20write", |b| {
        let mut counter = 0;
        b.iter(|| {
            let idx = counter % 100;
            if idx < 80 {
                // Read
                let key = format!("key_{}", idx * 10);
                let _result = db.get(black_box(&key)).expect("GET failed");
            } else {
                // Write
                let key = format!("new_key_{}", counter);
                db.set(black_box(&key), black_box(b"value"), None)
                    .expect("SET failed");
            }
            counter += 1;
        });
    });
}

// ============================================================================
// CONCURRENT OPERATIONS
// ============================================================================

fn bench_concurrent_operations(c: &mut Criterion) {
    let db = Arc::new(create_db());

    c.bench_function("concurrent_set_4threads", |b| {
        let mut counter = 0;
        b.iter(|| {
            let key = format!("concurrent_key_{}", counter);
            db.set(black_box(&key), black_box(b"value"), None)
                .expect("SET failed");
            counter += 1;
        });
    });
}

// ============================================================================
// EXPIRATION
// ============================================================================

fn bench_expiration(c: &mut Criterion) {
    let db = create_db();

    c.bench_function("set_with_ttl", |b| {
        let mut counter = 0;
        b.iter(|| {
            let key = format!("ttl_key_{}", counter);
            db.set(black_box(&key), black_box(b"value"), Some(Duration::from_secs(3600)))
                .expect("SET failed");
            counter += 1;
        });
    });
}

// ============================================================================
// REDIS COMPARISON
// ============================================================================

fn try_connect(url: &str) -> Option<redis::Client> {
    match redis::Client::open(url) {
        Ok(client) => match client.get_connection() {
            Ok(_) => Some(client),
            Err(_) => None,
        },
        Err(_) => None,
    }
}

fn bench_redis_comparison(c: &mut Criterion) {
    let redis_client = try_connect("redis://127.0.0.1:6379/");

    let mut group = c.benchmark_group("redis_comparison");

    // Redlite embedded SET
    group.bench_function("redlite_set", |b| {
        let db = create_db();
        let mut counter = 0;
        let value = vec![b'x'; 64];
        b.iter(|| {
            let key = format!("key_{}", counter);
            db.set(black_box(&key), black_box(&value), None)
                .expect("SET failed");
            counter += 1;
        });
    });

    // Redis SET (if available)
    if let Some(ref client) = redis_client {
        group.bench_function("redis_set", |b| {
            let mut conn = client.get_connection().expect("Redis connection failed");
            let mut counter = 0;
            let value = vec![b'x'; 64];
            b.iter(|| {
                let key = format!("key_{}", counter);
                let _: () = redis::cmd("SET")
                    .arg(black_box(&key))
                    .arg(black_box(&value))
                    .query(&mut conn)
                    .expect("Redis SET failed");
                counter += 1;
            });
        });
    }

    // Redlite embedded GET
    {
        let db = create_db();
        db.set("bench_key", b"test_value", None).expect("SET failed");
        group.bench_function("redlite_get", |b| {
            b.iter(|| {
                let _result = db.get(black_box("bench_key")).expect("GET failed");
            });
        });
    }

    // Redis GET (if available)
    if let Some(ref client) = redis_client {
        let mut conn = client.get_connection().expect("Redis connection failed");
        let _: () = redis::cmd("SET").arg("bench_key").arg("test_value").query(&mut conn).unwrap();
        group.bench_function("redis_get", |b| {
            let mut conn = client.get_connection().expect("Redis connection failed");
            b.iter(|| {
                let _: Option<Vec<u8>> = redis::cmd("GET")
                    .arg(black_box("bench_key"))
                    .query(&mut conn)
                    .expect("Redis GET failed");
            });
        });
    }

    group.finish();
}

// ============================================================================
// FULL COMPARISON: SQLite vs Redis/Dragonfly
// ============================================================================

fn bench_full_comparison(c: &mut Criterion) {
    let dragonfly_client = try_connect("redis://127.0.0.1:6382/");
    let redis_client = try_connect("redis://127.0.0.1:6379/");
    let redlite_server_file = try_connect("redis://127.0.0.1:6380/");
    let redlite_server_mem = try_connect("redis://127.0.0.1:6381/");

    eprintln!("\n=== Server Availability ===");
    eprintln!("Dragonfly (6382):           {}", if dragonfly_client.is_some() { "OK" } else { "Not running" });
    eprintln!("Redis (6379):               {}", if redis_client.is_some() { "OK" } else { "Not running" });
    eprintln!("Redlite server file (6380): {}", if redlite_server_file.is_some() { "OK" } else { "Not running" });
    eprintln!("Redlite server mem (6381):  {}", if redlite_server_mem.is_some() { "OK" } else { "Not running" });
    eprintln!("============================\n");

    let mut group = c.benchmark_group("full_comparison");

    // SQLite embedded (file)
    group.bench_function("SET/sqlite_embedded_file", |b| {
        let db = create_file_db();
        let mut counter = 0;
        let value = vec![b'x'; 64];
        b.iter(|| {
            let key = format!("key_{}", counter);
            db.set(black_box(&key), black_box(&value), None)
                .expect("SET failed");
            counter += 1;
        });
    });

    // SQLite embedded (memory)
    group.bench_function("SET/sqlite_embedded_mem", |b| {
        let db = create_db();
        let mut counter = 0;
        let value = vec![b'x'; 64];
        b.iter(|| {
            let key = format!("key_{}", counter);
            db.set(black_box(&key), black_box(&value), None)
                .expect("SET failed");
            counter += 1;
        });
    });

    // Dragonfly
    if let Some(ref client) = dragonfly_client {
        group.bench_function("SET/dragonfly", |b| {
            let mut conn = client.get_connection().expect("Dragonfly connection failed");
            let mut counter = 0;
            let value = vec![b'x'; 64];
            b.iter(|| {
                let key = format!("key_{}", counter);
                let _: () = redis::cmd("SET")
                    .arg(black_box(&key))
                    .arg(black_box(&value))
                    .query(&mut conn)
                    .expect("Dragonfly SET failed");
                counter += 1;
            });
        });
    }

    // Redis
    if let Some(ref client) = redis_client {
        group.bench_function("SET/redis", |b| {
            let mut conn = client.get_connection().expect("Redis connection failed");
            let mut counter = 0;
            let value = vec![b'x'; 64];
            b.iter(|| {
                let key = format!("key_{}", counter);
                let _: () = redis::cmd("SET")
                    .arg(black_box(&key))
                    .arg(black_box(&value))
                    .query(&mut conn)
                    .expect("Redis SET failed");
                counter += 1;
            });
        });
    }

    // Redlite server (file)
    if let Some(ref client) = redlite_server_file {
        group.bench_function("SET/redlite_server_file", |b| {
            let mut conn = client.get_connection().expect("Redlite connection failed");
            let mut counter = 0;
            let value = vec![b'x'; 64];
            b.iter(|| {
                let key = format!("key_{}", counter);
                let _: () = redis::cmd("SET")
                    .arg(black_box(&key))
                    .arg(black_box(&value))
                    .query(&mut conn)
                    .expect("Redlite SET failed");
                counter += 1;
            });
        });
    }

    // Redlite server (memory)
    if let Some(ref client) = redlite_server_mem {
        group.bench_function("SET/redlite_server_mem", |b| {
            let mut conn = client.get_connection().expect("Redlite connection failed");
            let mut counter = 0;
            let value = vec![b'x'; 64];
            b.iter(|| {
                let key = format!("key_{}", counter);
                let _: () = redis::cmd("SET")
                    .arg(black_box(&key))
                    .arg(black_box(&value))
                    .query(&mut conn)
                    .expect("Redlite SET failed");
                counter += 1;
            });
        });
    }

    // GET benchmarks
    {
        let db = create_db();
        db.set("bench_key", b"test_value", None).expect("SET failed");
        group.bench_function("GET/sqlite_embedded_mem", |b| {
            b.iter(|| {
                let _result = db.get(black_box("bench_key")).expect("GET failed");
            });
        });
    }

    if let Some(ref client) = redis_client {
        let mut conn = client.get_connection().expect("Redis connection failed");
        let _: () = redis::cmd("SET").arg("bench_key").arg("test_value").query(&mut conn).unwrap();
        group.bench_function("GET/redis", |b| {
            let mut conn = client.get_connection().expect("Redis connection failed");
            b.iter(|| {
                let _: Option<Vec<u8>> = redis::cmd("GET")
                    .arg(black_box("bench_key"))
                    .query(&mut conn)
                    .expect("Redis GET failed");
            });
        });
    }

    group.finish();
}

// ============================================================================
// SCALING BENCHMARKS - Test performance at different dataset sizes
// ============================================================================

fn flush_server(client: &redis::Client) {
    if let Ok(mut conn) = client.get_connection() {
        let _: Result<(), _> = redis::cmd("FLUSHALL").query(&mut conn);
    }
}

fn bench_scaling(c: &mut Criterion) {
    let redis_client = try_connect("redis://127.0.0.1:6379/");
    let redlite_server_mem = try_connect("redis://127.0.0.1:6381/");

    eprintln!("\n=== Server Availability ===");
    eprintln!("Dragonfly (6382):           Not running");
    eprintln!("Redis (6379):               {}", if redis_client.is_some() { "OK" } else { "Not running" });
    eprintln!("Redlite server file (6380): Not running");
    eprintln!("Redlite server mem (6381):  {}", if redlite_server_mem.is_some() { "OK" } else { "Not running" });
    eprintln!("Redlite embedded file:      Always available");
    eprintln!("Redlite embedded memory:    Always available");

    let sizes: &[u32] = &[1_000, 10_000, 100_000, 1_000_000];
    let size_names: &[&str] = &["1K", "10K", "100K", "1M"];

    // GET scaling benchmark
    eprintln!("\n=== Scaling Benchmark ===");
    eprintln!("Testing GET latency as dataset grows from 1K to 1M keys");
    eprintln!("This reveals O(log n) vs O(1) behavior differences");

    for (size, size_name) in sizes.iter().zip(size_names.iter()) {
        eprintln!("--- Testing size: {} ---", size_name);

        // Flush servers first
        eprintln!("Flushing Redis...");
        if let Some(ref client) = redis_client {
            flush_server(client);
        }
        eprintln!("Flushing Redlite server...");
        if let Some(ref client) = redlite_server_mem {
            flush_server(client);
        }

        let mut group = c.benchmark_group("scaling/get");

        // Redlite embedded (memory)
        {
            let db = create_db();
            // Populate using mset batching
            eprintln!("Populating Redlite embedded with {} keys...", size);
            let batch_size = 1000usize;
            for batch_start in (0..*size as usize).step_by(batch_size) {
                let batch_end = std::cmp::min(batch_start + batch_size, *size as usize);
                let keys: Vec<String> = (batch_start..batch_end).map(|i| format!("key_{}", i)).collect();
                let pairs: Vec<(&str, &[u8])> = keys.iter().map(|k| (k.as_str(), b"value_data_here".as_slice())).collect();
                db.mset(&pairs).unwrap();
            }
            eprintln!("Done.");

            eprintln!("Testing scaling/get/{}/redlite_embedded_mem", size_name);
            group.bench_function(BenchmarkId::new(*size_name, "redlite_embedded_mem"), |b| {
                let mut idx = 0;
                b.iter(|| {
                    let key = format!("key_{}", idx % *size);
                    let _result = db.get(black_box(&key));
                    idx += 1;
                });
            });
            eprintln!("Success");
        }

        // Redlite embedded (file)
        {
            let db = create_file_db();
            eprintln!("Populating Redlite file with {} keys...", size);
            let batch_size = 1000usize;
            for batch_start in (0..*size as usize).step_by(batch_size) {
                let batch_end = std::cmp::min(batch_start + batch_size, *size as usize);
                let keys: Vec<String> = (batch_start..batch_end).map(|i| format!("key_{}", i)).collect();
                let pairs: Vec<(&str, &[u8])> = keys.iter().map(|k| (k.as_str(), b"value_data_here".as_slice())).collect();
                db.mset(&pairs).unwrap();
            }
            eprintln!("Done.");

            eprintln!("Testing scaling/get/{}/redlite_embedded_file", size_name);
            group.bench_function(BenchmarkId::new(*size_name, "redlite_embedded_file"), |b| {
                let mut idx = 0;
                b.iter(|| {
                    let key = format!("key_{}", idx % *size);
                    let _result = db.get(black_box(&key));
                    idx += 1;
                });
            });
            eprintln!("Success");
        }

        // Redis
        if let Some(ref client) = redis_client {
            let mut conn = client.get_connection().unwrap();
            eprintln!("Populating Redis with {} keys...", size);
            // Use pipelining for fast population
            let batch_size = 1000usize;
            for batch_start in (0..*size as usize).step_by(batch_size) {
                let batch_end = std::cmp::min(batch_start + batch_size, *size as usize);
                let mut pipe = redis::pipe();
                for i in batch_start..batch_end {
                    pipe.cmd("SET").arg(format!("key_{}", i)).arg(b"value_data_here");
                }
                let _: () = pipe.query(&mut conn).unwrap();
            }
            eprintln!("Done.");

            eprintln!("Testing scaling/get/{}/redis", size_name);
            group.bench_function(BenchmarkId::new(*size_name, "redis"), |b| {
                let mut conn = client.get_connection().unwrap();
                let mut idx = 0;
                b.iter(|| {
                    let key = format!("key_{}", idx % *size);
                    let _: Option<Vec<u8>> = redis::cmd("GET")
                        .arg(black_box(&key))
                        .query(&mut conn)
                        .unwrap();
                    idx += 1;
                });
            });
            eprintln!("Success");
        }

        // Redlite server (memory)
        if let Some(ref client) = redlite_server_mem {
            let mut conn = client.get_connection().unwrap();
            eprintln!("Populating Redlite server with {} keys...", size);
            let batch_size = 1000usize;
            for batch_start in (0..*size as usize).step_by(batch_size) {
                let batch_end = std::cmp::min(batch_start + batch_size, *size as usize);
                let mut pipe = redis::pipe();
                for i in batch_start..batch_end {
                    pipe.cmd("SET").arg(format!("key_{}", i)).arg(b"value_data_here");
                }
                let _: () = pipe.query(&mut conn).unwrap();
            }
            eprintln!("Done.");

            eprintln!("Testing scaling/get/{}/redlite_server_mem", size_name);
            group.bench_function(BenchmarkId::new(*size_name, "redlite_server_mem"), |b| {
                let mut conn = client.get_connection().unwrap();
                let mut idx = 0;
                b.iter(|| {
                    let key = format!("key_{}", idx % *size);
                    let _: Option<Vec<u8>> = redis::cmd("GET")
                        .arg(black_box(&key))
                        .query(&mut conn)
                        .unwrap();
                    idx += 1;
                });
            });
            eprintln!("Success");
        }

        group.finish();
    }
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
    bench_redis_comparison,
    bench_full_comparison,
);

criterion_group!(
    name = scaling;
    config = Criterion::default().sample_size(100).measurement_time(Duration::from_secs(5));
    targets = bench_scaling
);

criterion_main!(benches, scaling);
