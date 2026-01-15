//! FT.AGGREGATE Performance Benchmarks
//!
//! Session 37: Performance Benchmarking for FT.AGGREGATE at scale.
//!
//! Run with: cargo bench --bench ft_aggregate
//! Quick test: cargo bench --bench ft_aggregate -- --test

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use redlite::db::Db;
use redlite::types::{
    FtAggregateOptions, FtField, FtGroupBy, FtOnType, FtReduceFunction, FtReducer, FtSearchOptions,
};

/// Generate test documents for benchmarking
fn setup_documents(db: &Db, count: usize, categories: usize) -> Vec<String> {
    let mut keys = Vec::with_capacity(count);

    for i in 0..count {
        let key = format!("product:{}", i);
        let category = format!("category_{}", i % categories);
        let price = (i % 1000) as f64 + 0.99;
        let rating = ((i % 50) as f64) / 10.0;
        let sales = i % 10000;
        let title = format!("Product {} with features and description text", i);

        db.hset(
            &key,
            &[
                ("title", title.as_bytes()),
                ("category", category.as_bytes()),
                ("price", format!("{:.2}", price).as_bytes()),
                ("rating", format!("{:.1}", rating).as_bytes()),
                ("sales", format!("{}", sales).as_bytes()),
            ],
        )
        .unwrap();

        keys.push(key);
    }

    keys
}

/// Create FTS index for benchmarking
fn setup_index(db: &Db) {
    let schema = vec![
        FtField::text("title"),
        FtField::numeric("price"),
        FtField::numeric("rating"),
        FtField::numeric("sales"),
        FtField::tag("category"),
    ];

    let _ = db.ft_dropindex("bench_idx", false);
    db.ft_create("bench_idx", FtOnType::Hash, &["product:"], &schema)
        .expect("Failed to create index");
}

/// Benchmark 1: Simple GROUPBY + COUNT at 1K scale
fn bench_ft_aggregate_1k_simple(c: &mut Criterion) {
    let db = Db::open_memory().unwrap();
    setup_index(&db);
    setup_documents(&db, 1000, 10);

    let mut group = c.benchmark_group("ft_aggregate_1k");
    group.throughput(Throughput::Elements(1000));

    group.bench_function("simple_groupby_count", |b| {
        b.iter(|| {
            let options = FtAggregateOptions {
                group_by: Some(FtGroupBy {
                    fields: vec!["category".to_string()],
                    reducers: vec![FtReducer {
                        function: FtReduceFunction::Count,
                        alias: Some("count".to_string()),
                    }],
                }),
                limit_num: 100,
                ..Default::default()
            };
            let result = db.ft_aggregate("bench_idx", "*", &options);
            black_box(result)
        })
    });

    group.finish();
}

/// Benchmark 2: Complex pipeline at 10K scale with 5 REDUCE functions
fn bench_ft_aggregate_10k_complex(c: &mut Criterion) {
    let db = Db::open_memory().unwrap();
    setup_index(&db);
    setup_documents(&db, 10_000, 100);

    let mut group = c.benchmark_group("ft_aggregate_10k");
    group.throughput(Throughput::Elements(10_000));
    group.sample_size(50); // Fewer samples for longer benchmarks

    group.bench_function("complex_5_reduce", |b| {
        b.iter(|| {
            let options = FtAggregateOptions {
                load_fields: vec![
                    "price".to_string(),
                    "rating".to_string(),
                    "sales".to_string(),
                ],
                group_by: Some(FtGroupBy {
                    fields: vec!["category".to_string()],
                    reducers: vec![
                        FtReducer {
                            function: FtReduceFunction::Count,
                            alias: Some("count".to_string()),
                        },
                        FtReducer {
                            function: FtReduceFunction::Avg("price".to_string()),
                            alias: Some("avg_price".to_string()),
                        },
                        FtReducer {
                            function: FtReduceFunction::Sum("sales".to_string()),
                            alias: Some("total_sales".to_string()),
                        },
                        FtReducer {
                            function: FtReduceFunction::Max("rating".to_string()),
                            alias: Some("max_rating".to_string()),
                        },
                        FtReducer {
                            function: FtReduceFunction::StdDev("price".to_string()),
                            alias: Some("price_stddev".to_string()),
                        },
                    ],
                }),
                filter: Some("@count > 10".to_string()),
                sort_by: vec![("total_sales".to_string(), false)], // DESC
                limit_num: 50,
                ..Default::default()
            };
            let result = db.ft_aggregate("bench_idx", "*", &options);
            black_box(result)
        })
    });

    group.finish();
}

/// Benchmark 3: Scale test at 100K documents
fn bench_ft_aggregate_100k_scale(c: &mut Criterion) {
    let db = Db::open_memory().unwrap();
    setup_index(&db);
    setup_documents(&db, 100_000, 500);

    let mut group = c.benchmark_group("ft_aggregate_100k");
    group.throughput(Throughput::Elements(100_000));
    group.sample_size(20); // Fewer samples for very long benchmarks

    // Simple query at scale
    group.bench_function("simple_count", |b| {
        b.iter(|| {
            let options = FtAggregateOptions {
                group_by: Some(FtGroupBy {
                    fields: vec!["category".to_string()],
                    reducers: vec![FtReducer {
                        function: FtReduceFunction::Count,
                        alias: Some("count".to_string()),
                    }],
                }),
                limit_num: 500,
                ..Default::default()
            };
            let result = db.ft_aggregate("bench_idx", "*", &options);
            black_box(result)
        })
    });

    // Complex query at scale
    group.bench_function("complex_pipeline", |b| {
        b.iter(|| {
            let options = FtAggregateOptions {
                load_fields: vec!["price".to_string(), "sales".to_string()],
                group_by: Some(FtGroupBy {
                    fields: vec!["category".to_string()],
                    reducers: vec![
                        FtReducer {
                            function: FtReduceFunction::Count,
                            alias: Some("count".to_string()),
                        },
                        FtReducer {
                            function: FtReduceFunction::Sum("sales".to_string()),
                            alias: Some("total_sales".to_string()),
                        },
                        FtReducer {
                            function: FtReduceFunction::Avg("price".to_string()),
                            alias: Some("avg_price".to_string()),
                        },
                    ],
                }),
                filter: Some("@count > 50".to_string()),
                sort_by: vec![("total_sales".to_string(), false)],
                limit_num: 100,
                ..Default::default()
            };
            let result = db.ft_aggregate("bench_idx", "*", &options);
            black_box(result)
        })
    });

    group.finish();
}

/// Benchmark 4: FT.SEARCH with BM25 ranking at scale
fn bench_ft_search_bm25(c: &mut Criterion) {
    let db = Db::open_memory().unwrap();
    setup_index(&db);

    // Setup documents with varied text for BM25 testing
    for i in 0..10_000 {
        let key = format!("product:{}", i);
        let title = if i % 3 == 0 {
            format!("Premium {} wireless bluetooth speaker with bass boost", i)
        } else if i % 3 == 1 {
            format!("Standard {} headphones stereo audio", i)
        } else {
            format!("Budget {} earbuds basic sound quality", i)
        };

        db.hset(
            &key,
            &[
                ("title", title.as_bytes()),
                ("category", b"electronics"),
                ("price", b"99.99"),
                ("rating", b"4.5"),
                ("sales", b"100"),
            ],
        )
        .unwrap();
    }

    let mut group = c.benchmark_group("ft_search_bm25");
    group.throughput(Throughput::Elements(10_000));

    // Simple search
    group.bench_function("single_term_10k", |b| {
        let opts = FtSearchOptions {
            limit_offset: 0,
            limit_num: 100,
            withscores: true,
            ..Default::default()
        };
        b.iter(|| {
            let result = db.ft_search("bench_idx", "bluetooth", &opts);
            black_box(result)
        })
    });

    // Multi-term search
    group.bench_function("multi_term_10k", |b| {
        let opts = FtSearchOptions {
            limit_offset: 0,
            limit_num: 100,
            withscores: true,
            ..Default::default()
        };
        b.iter(|| {
            let result = db.ft_search("bench_idx", "wireless bluetooth speaker", &opts);
            black_box(result)
        })
    });

    group.finish();
}

/// Benchmark 5: Scaling comparison across document counts
fn bench_scaling_comparison(c: &mut Criterion) {
    let mut group = c.benchmark_group("ft_aggregate_scaling");

    for size in [1_000, 5_000, 10_000, 25_000] {
        let db = Db::open_memory().unwrap();
        setup_index(&db);
        setup_documents(&db, size, size / 10);

        group.throughput(Throughput::Elements(size as u64));

        group.bench_with_input(BenchmarkId::new("groupby_count", size), &size, |b, _| {
            b.iter(|| {
                let options = FtAggregateOptions {
                    group_by: Some(FtGroupBy {
                        fields: vec!["category".to_string()],
                        reducers: vec![FtReducer {
                            function: FtReduceFunction::Count,
                            alias: Some("count".to_string()),
                        }],
                    }),
                    limit_num: 100,
                    ..Default::default()
                };
                let result = db.ft_aggregate("bench_idx", "*", &options);
                black_box(result)
            })
        });
    }

    group.finish();
}

/// Benchmark 6: Memory pressure test with sustained operations
fn bench_memory_pressure(c: &mut Criterion) {
    let mut group = c.benchmark_group("memory_pressure");
    group.sample_size(10);

    group.bench_function("sustained_10k_ops", |b| {
        b.iter(|| {
            let db = Db::open_memory().unwrap();
            setup_index(&db);

            // Perform many operations
            for i in 0..10_000 {
                let key = format!("doc:{}", i);
                db.hset(
                    &key,
                    &[
                        ("title", format!("Document {}", i).as_bytes()),
                        ("category", format!("cat_{}", i % 100).as_bytes()),
                        ("value", format!("{}", i).as_bytes()),
                    ],
                )
                .unwrap();
            }

            // Run aggregation
            let options = FtAggregateOptions {
                group_by: Some(FtGroupBy {
                    fields: vec!["category".to_string()],
                    reducers: vec![FtReducer {
                        function: FtReduceFunction::Count,
                        alias: Some("count".to_string()),
                    }],
                }),
                limit_num: 100,
                ..Default::default()
            };
            let result = db.ft_aggregate("bench_idx", "*", &options);

            black_box(result)
        })
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_ft_aggregate_1k_simple,
    bench_ft_aggregate_10k_complex,
    bench_ft_aggregate_100k_scale,
    bench_ft_search_bm25,
    bench_scaling_comparison,
    bench_memory_pressure,
);

criterion_main!(benches);
