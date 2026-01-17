//! Eviction & Access Tracking Benchmarks
//!
//! Session 51: Measure performance impact of eviction policies and access tracking.
//!
//! Run with: cargo bench --bench eviction
//! Quick test: cargo bench --bench eviction -- --test

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use redlite::db::Db;
use redlite::EvictionPolicy;

/// Benchmark 1: Access tracking overhead on GET operations
/// Compare performance with persist_access_tracking on vs off
fn bench_access_tracking_overhead(c: &mut Criterion) {
    let mut group = c.benchmark_group("access_tracking");
    group.throughput(Throughput::Elements(1000));

    // Test with access tracking DISABLED
    group.bench_function("get_1k_tracking_off", |b| {
        let db = Db::open_memory().unwrap();
        db.set_persist_access_tracking(false);

        // Pre-populate keys
        for i in 0..100 {
            db.set(&format!("key:{}", i), b"value", None).unwrap();
        }

        b.iter(|| {
            for i in 0..1000 {
                let key = format!("key:{}", i % 100);
                let _ = black_box(db.get(&key));
            }
        })
    });

    // Test with access tracking ENABLED (default for :memory:)
    group.bench_function("get_1k_tracking_on", |b| {
        let db = Db::open_memory().unwrap();
        db.set_persist_access_tracking(true);

        // Pre-populate keys
        for i in 0..100 {
            db.set(&format!("key:{}", i), b"value", None).unwrap();
        }

        b.iter(|| {
            for i in 0..1000 {
                let key = format!("key:{}", i % 100);
                let _ = black_box(db.get(&key));
            }
        })
    });

    group.finish();
}

/// Benchmark 2: SET performance with different eviction policies
fn bench_eviction_policy_set(c: &mut Criterion) {
    let mut group = c.benchmark_group("eviction_policy_set");
    group.throughput(Throughput::Elements(1000));

    let policies = [
        ("noeviction", EvictionPolicy::NoEviction),
        ("allkeys-lru", EvictionPolicy::AllKeysLRU),
        ("allkeys-lfu", EvictionPolicy::AllKeysLFU),
        ("allkeys-random", EvictionPolicy::AllKeysRandom),
    ];

    for (name, policy) in policies {
        group.bench_with_input(
            BenchmarkId::new("set_1k", name),
            &policy,
            |b, &p| {
                let db = Db::open_memory().unwrap();
                db.set_eviction_policy(p);
                // Set high memory limit to avoid actual eviction
                db.set_max_memory(1024 * 1024 * 1024); // 1GB

                b.iter(|| {
                    for i in 0..1000 {
                        let key = format!("key:{}", i);
                        db.set(&key, b"test_value_here", None).unwrap();
                    }
                })
            },
        );
    }

    group.finish();
}

/// Benchmark 3: GET performance with different eviction policies (LRU/LFU tracking)
fn bench_eviction_policy_get(c: &mut Criterion) {
    let mut group = c.benchmark_group("eviction_policy_get");
    group.throughput(Throughput::Elements(1000));

    let policies = [
        ("noeviction", EvictionPolicy::NoEviction),
        ("allkeys-lru", EvictionPolicy::AllKeysLRU),
        ("allkeys-lfu", EvictionPolicy::AllKeysLFU),
    ];

    for (name, policy) in policies {
        group.bench_with_input(
            BenchmarkId::new("get_1k", name),
            &policy,
            |b, &p| {
                let db = Db::open_memory().unwrap();
                db.set_eviction_policy(p);

                // Pre-populate keys
                for i in 0..100 {
                    db.set(&format!("key:{}", i), b"value", None).unwrap();
                }

                b.iter(|| {
                    for i in 0..1000 {
                        let key = format!("key:{}", i % 100);
                        let _ = black_box(db.get(&key));
                    }
                })
            },
        );
    }

    group.finish();
}

/// Benchmark 4: Mixed read/write workload
fn bench_mixed_workload(c: &mut Criterion) {
    let mut group = c.benchmark_group("mixed_workload");
    group.throughput(Throughput::Elements(1000));

    let policies = [
        ("noeviction", EvictionPolicy::NoEviction),
        ("allkeys-lru", EvictionPolicy::AllKeysLRU),
        ("allkeys-lfu", EvictionPolicy::AllKeysLFU),
    ];

    for (name, policy) in policies {
        group.bench_with_input(
            BenchmarkId::new("80_read_20_write", name),
            &policy,
            |b, &p| {
                let db = Db::open_memory().unwrap();
                db.set_eviction_policy(p);
                db.set_max_memory(1024 * 1024 * 1024); // 1GB

                // Pre-populate keys
                for i in 0..100 {
                    db.set(&format!("key:{}", i), b"initial_value", None).unwrap();
                }

                b.iter(|| {
                    for i in 0..1000 {
                        let key = format!("key:{}", i % 100);
                        if i % 5 == 0 {
                            // 20% writes
                            db.set(&key, b"updated_value", None).unwrap();
                        } else {
                            // 80% reads
                            let _ = black_box(db.get(&key));
                        }
                    }
                })
            },
        );
    }

    group.finish();
}

/// Benchmark 5: Hash operations with access tracking
fn bench_hash_with_tracking(c: &mut Criterion) {
    let mut group = c.benchmark_group("hash_tracking");
    group.throughput(Throughput::Elements(1000));

    group.bench_function("hget_1k_tracking_on", |b| {
        let db = Db::open_memory().unwrap();
        db.set_persist_access_tracking(true);
        db.set_eviction_policy(EvictionPolicy::AllKeysLRU);

        // Pre-populate hashes
        for i in 0..100 {
            db.hset(&format!("hash:{}", i), &[("field", b"value")]).unwrap();
        }

        b.iter(|| {
            for i in 0..1000 {
                let key = format!("hash:{}", i % 100);
                let _ = black_box(db.hget(&key, "field"));
            }
        })
    });

    group.bench_function("hget_1k_tracking_off", |b| {
        let db = Db::open_memory().unwrap();
        db.set_persist_access_tracking(false);
        db.set_eviction_policy(EvictionPolicy::NoEviction);

        // Pre-populate hashes
        for i in 0..100 {
            db.hset(&format!("hash:{}", i), &[("field", b"value")]).unwrap();
        }

        b.iter(|| {
            for i in 0..1000 {
                let key = format!("hash:{}", i % 100);
                let _ = black_box(db.hget(&key, "field"));
            }
        })
    });

    group.finish();
}

/// Benchmark 6: List operations with access tracking
fn bench_list_with_tracking(c: &mut Criterion) {
    let mut group = c.benchmark_group("list_tracking");
    group.throughput(Throughput::Elements(1000));

    group.bench_function("lrange_1k_tracking_on", |b| {
        let db = Db::open_memory().unwrap();
        db.set_persist_access_tracking(true);
        db.set_eviction_policy(EvictionPolicy::AllKeysLRU);

        // Pre-populate lists
        for i in 0..100 {
            db.rpush(&format!("list:{}", i), &[b"a", b"b", b"c"]).unwrap();
        }

        b.iter(|| {
            for i in 0..1000 {
                let key = format!("list:{}", i % 100);
                let _ = black_box(db.lrange(&key, 0, -1));
            }
        })
    });

    group.bench_function("lrange_1k_tracking_off", |b| {
        let db = Db::open_memory().unwrap();
        db.set_persist_access_tracking(false);
        db.set_eviction_policy(EvictionPolicy::NoEviction);

        // Pre-populate lists
        for i in 0..100 {
            db.rpush(&format!("list:{}", i), &[b"a", b"b", b"c"]).unwrap();
        }

        b.iter(|| {
            for i in 0..1000 {
                let key = format!("list:{}", i % 100);
                let _ = black_box(db.lrange(&key, 0, -1));
            }
        })
    });

    group.finish();
}

/// Benchmark 7: ZSet operations with access tracking
fn bench_zset_with_tracking(c: &mut Criterion) {
    let mut group = c.benchmark_group("zset_tracking");
    group.throughput(Throughput::Elements(1000));

    group.bench_function("zscore_1k_tracking_on", |b| {
        let db = Db::open_memory().unwrap();
        db.set_persist_access_tracking(true);
        db.set_eviction_policy(EvictionPolicy::AllKeysLRU);

        // Pre-populate sorted sets
        for i in 0..100 {
            db.zadd(&format!("zset:{}", i), &[
                redlite::ZMember { score: 1.0, member: b"member".to_vec() }
            ]).unwrap();
        }

        b.iter(|| {
            for i in 0..1000 {
                let key = format!("zset:{}", i % 100);
                let _ = black_box(db.zscore(&key, b"member"));
            }
        })
    });

    group.bench_function("zscore_1k_tracking_off", |b| {
        let db = Db::open_memory().unwrap();
        db.set_persist_access_tracking(false);
        db.set_eviction_policy(EvictionPolicy::NoEviction);

        // Pre-populate sorted sets
        for i in 0..100 {
            db.zadd(&format!("zset:{}", i), &[
                redlite::ZMember { score: 1.0, member: b"member".to_vec() }
            ]).unwrap();
        }

        b.iter(|| {
            for i in 0..1000 {
                let key = format!("zset:{}", i % 100);
                let _ = black_box(db.zscore(&key, b"member"));
            }
        })
    });

    group.finish();
}

/// Benchmark 8: Flush interval impact
fn bench_flush_interval(c: &mut Criterion) {
    let mut group = c.benchmark_group("flush_interval");
    group.throughput(Throughput::Elements(1000));

    let intervals = [0, 100, 1000, 5000, 60000]; // 0 (immediate), 100ms, 1s, 5s, 1min

    for interval in intervals {
        group.bench_with_input(
            BenchmarkId::new("get_1k_flush", format!("{}ms", interval)),
            &interval,
            |b, &ms| {
                let db = Db::open_memory().unwrap();
                db.set_persist_access_tracking(true);
                db.set_access_flush_interval(ms);
                db.set_eviction_policy(EvictionPolicy::AllKeysLRU);

                // Pre-populate keys
                for i in 0..100 {
                    db.set(&format!("key:{}", i), b"value", None).unwrap();
                }

                b.iter(|| {
                    for i in 0..1000 {
                        let key = format!("key:{}", i % 100);
                        let _ = black_box(db.get(&key));
                    }
                })
            },
        );
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_access_tracking_overhead,
    bench_eviction_policy_set,
    bench_eviction_policy_get,
    bench_mixed_workload,
    bench_hash_with_tracking,
    bench_list_with_tracking,
    bench_zset_with_tracking,
    bench_flush_interval,
);

criterion_main!(benches);
