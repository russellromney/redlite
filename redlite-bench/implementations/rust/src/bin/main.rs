//! redlite-bench CLI
//!
//! Comprehensive Redis protocol benchmark suite

use clap::{Parser, Subcommand};
use redlite_bench::benchmark::{BenchmarkConfig, BenchmarkRunner};
use redlite_bench::client::{RedisClient, RedliteEmbeddedClient};
use redlite_bench::concurrency::ConcurrencyMode;

#[derive(Parser)]
#[command(name = "redlite-bench")]
#[command(about = "Comprehensive Redis protocol benchmark suite", long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Run benchmarks against Redis
    Redis {
        /// Redis connection URL
        #[arg(short, long, default_value = "redis://127.0.0.1:6379")]
        url: String,

        /// Number of iterations per operation
        #[arg(short, long, default_value = "100000")]
        iterations: usize,

        /// Dataset size (number of keys)
        #[arg(short, long, default_value = "10000")]
        dataset_size: usize,

        /// Number of concurrent tasks
        #[arg(short, long, default_value = "1")]
        concurrency: usize,

        /// Concurrency mode: sequential, async, or blocking
        #[arg(long, default_value = "sequential")]
        concurrency_mode: String,

        /// Operations to benchmark (comma-separated)
        #[arg(short, long, default_value = "get,set")]
        operations: String,
    },

    /// Run benchmarks against Redlite embedded
    Redlite {
        /// Use in-memory database
        #[arg(long)]
        memory: bool,

        /// Path to database file (if not using memory)
        #[arg(short, long)]
        path: Option<String>,

        /// Number of iterations per operation
        #[arg(short, long, default_value = "100000")]
        iterations: usize,

        /// Dataset size (number of keys)
        #[arg(short, long, default_value = "10000")]
        dataset_size: usize,

        /// Number of concurrent tasks
        #[arg(short, long, default_value = "1")]
        concurrency: usize,

        /// Concurrency mode: sequential, async, or blocking
        #[arg(long, default_value = "sequential")]
        concurrency_mode: String,

        /// Operations to benchmark (comma-separated)
        #[arg(short, long, default_value = "get,set")]
        operations: String,
    },

    /// Run quick comparison between Redis and Redlite
    Compare {
        /// Redis connection URL
        #[arg(long, default_value = "redis://127.0.0.1:6379")]
        redis_url: String,

        /// Number of iterations per operation
        #[arg(short, long, default_value = "10000")]
        iterations: usize,

        /// Dataset size (number of keys)
        #[arg(short, long, default_value = "1000")]
        dataset_size: usize,

        /// Number of concurrent tasks
        #[arg(short, long, default_value = "1")]
        concurrency: usize,

        /// Concurrency mode: sequential, async, or blocking
        #[arg(long, default_value = "sequential")]
        concurrency_mode: String,
    },
}

fn parse_operations(ops: &str) -> Vec<String> {
    ops.split(',')
        .map(|s| s.trim().to_lowercase())
        .filter(|s| !s.is_empty())
        .collect()
}

fn parse_concurrency_mode(mode: &str) -> ConcurrencyMode {
    match mode.to_lowercase().as_str() {
        "async" => ConcurrencyMode::Async,
        "blocking" => ConcurrencyMode::Blocking,
        _ => ConcurrencyMode::Sequential,
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Commands::Redis {
            url,
            iterations,
            dataset_size,
            concurrency,
            concurrency_mode,
            operations,
        } => {
            println!("Connecting to Redis at {}...", url);
            let client = RedisClient::new(&url)?;

            let mode = parse_concurrency_mode(&concurrency_mode);
            let config = BenchmarkConfig {
                backend_name: "Redis".to_string(),
                dataset_size,
                iterations,
                warmup_iterations: 1000,
                concurrency,
            };

            let runner = BenchmarkRunner::new(client, config);
            let ops = parse_operations(&operations);

            println!("Running benchmarks with {} concurrency in {} mode\n", concurrency, mode);

            for op in ops {
                let result = match op.as_str() {
                    "get" => {
                        if concurrency > 1 {
                            let concurrent = runner.bench_get_concurrent(mode).await?;
                            concurrent.print_summary();
                            continue;
                        } else {
                            runner.bench_get().await?
                        }
                    }
                    "set" => {
                        if concurrency > 1 {
                            let concurrent = runner.bench_set_concurrent(mode).await?;
                            concurrent.print_summary();
                            continue;
                        } else {
                            runner.bench_set().await?
                        }
                    }
                    "incr" => runner.bench_incr().await?,
                    "lpush" => {
                        if concurrency > 1 {
                            let concurrent = runner.bench_lpush_concurrent(mode).await?;
                            concurrent.print_summary();
                            continue;
                        } else {
                            runner.bench_lpush().await?
                        }
                    }
                    "hset" => {
                        if concurrency > 1 {
                            let concurrent = runner.bench_hset_concurrent(mode).await?;
                            concurrent.print_summary();
                            continue;
                        } else {
                            runner.bench_hset().await?
                        }
                    }
                    "sadd" => runner.bench_sadd().await?,
                    "zadd" => runner.bench_zadd().await?,
                    "xadd" => runner.bench_xadd().await?,
                    "history_enable" => runner.bench_history_enable().await?,
                    "history_get" => runner.bench_history_get().await?,
                    "keyinfo" => runner.bench_keyinfo().await?,
                    "vacuum" => runner.bench_vacuum().await?,
                    _ => {
                        println!("Unknown operation: {}", op);
                        continue;
                    }
                };
                result.print_summary();
            }

            runner.cleanup().await?;
        }

        Commands::Redlite {
            memory,
            path,
            iterations,
            dataset_size,
            concurrency,
            concurrency_mode,
            operations,
        } => {
            let client = if memory {
                println!("Using in-memory Redlite database...");
                RedliteEmbeddedClient::new_memory()?
            } else if let Some(p) = path {
                println!("Using Redlite database at {}...", p);
                RedliteEmbeddedClient::new_file(&p)?
            } else {
                println!("Using in-memory Redlite database (default)...");
                RedliteEmbeddedClient::new_memory()?
            };

            let mode = parse_concurrency_mode(&concurrency_mode);
            let config = BenchmarkConfig {
                backend_name: "Redlite (embedded)".to_string(),
                dataset_size,
                iterations,
                warmup_iterations: 1000,
                concurrency,
            };

            let runner = BenchmarkRunner::new(client, config);
            let ops = parse_operations(&operations);

            println!("Running benchmarks with {} concurrency in {} mode\n", concurrency, mode);

            for op in ops {
                let result = match op.as_str() {
                    "get" => {
                        if concurrency > 1 {
                            let concurrent = runner.bench_get_concurrent(mode).await?;
                            concurrent.print_summary();
                            continue;
                        } else {
                            runner.bench_get().await?
                        }
                    }
                    "set" => {
                        if concurrency > 1 {
                            let concurrent = runner.bench_set_concurrent(mode).await?;
                            concurrent.print_summary();
                            continue;
                        } else {
                            runner.bench_set().await?
                        }
                    }
                    "incr" => runner.bench_incr().await?,
                    "lpush" => {
                        if concurrency > 1 {
                            let concurrent = runner.bench_lpush_concurrent(mode).await?;
                            concurrent.print_summary();
                            continue;
                        } else {
                            runner.bench_lpush().await?
                        }
                    }
                    "hset" => {
                        if concurrency > 1 {
                            let concurrent = runner.bench_hset_concurrent(mode).await?;
                            concurrent.print_summary();
                            continue;
                        } else {
                            runner.bench_hset().await?
                        }
                    }
                    "sadd" => runner.bench_sadd().await?,
                    "zadd" => runner.bench_zadd().await?,
                    "xadd" => runner.bench_xadd().await?,
                    "history_enable" => runner.bench_history_enable().await?,
                    "history_get" => runner.bench_history_get().await?,
                    "keyinfo" => runner.bench_keyinfo().await?,
                    "vacuum" => runner.bench_vacuum().await?,
                    _ => {
                        println!("Unknown operation: {}", op);
                        continue;
                    }
                };
                result.print_summary();
            }

            runner.cleanup().await?;
        }

        Commands::Compare {
            redis_url,
            iterations,
            dataset_size,
            concurrency,
            concurrency_mode,
        } => {
            println!("=== Quick Comparison: Redis vs Redlite ===\n");
            let mode = parse_concurrency_mode(&concurrency_mode);

            // Redis benchmarks
            println!("Connecting to Redis at {}...", redis_url);
            let redis_client = RedisClient::new(&redis_url)?;
            let redis_config = BenchmarkConfig {
                backend_name: "Redis".to_string(),
                dataset_size,
                iterations,
                warmup_iterations: 100,
                concurrency,
            };
            let redis_runner = BenchmarkRunner::new(redis_client, redis_config);

            // Redlite benchmarks
            println!("Creating in-memory Redlite database...");
            let redlite_client = RedliteEmbeddedClient::new_memory()?;
            let redlite_config = BenchmarkConfig {
                backend_name: "Redlite (embedded)".to_string(),
                dataset_size,
                iterations,
                warmup_iterations: 100,
                concurrency,
            };
            let redlite_runner = BenchmarkRunner::new(redlite_client, redlite_config);

            // Run GET comparison
            println!("\n--- GET Comparison ---");
            let (redis_get_throughput, redis_get_p50, redis_get_p99) = if concurrency > 1 {
                let redis_get = redis_runner.bench_get_concurrent(mode).await?;
                (redis_get.throughput_ops_sec(), redis_get.p50_latency_us(), redis_get.p99_latency_us())
            } else {
                let redis_get = redis_runner.bench_get().await?;
                (redis_get.throughput_ops_sec(), redis_get.p50_latency_us(), redis_get.p99_latency_us())
            };

            let (redlite_get_throughput, redlite_get_p50, redlite_get_p99) = if concurrency > 1 {
                let redlite_get = redlite_runner.bench_get_concurrent(mode).await?;
                (redlite_get.throughput_ops_sec(), redlite_get.p50_latency_us(), redlite_get.p99_latency_us())
            } else {
                let redlite_get = redlite_runner.bench_get().await?;
                (redlite_get.throughput_ops_sec(), redlite_get.p50_latency_us(), redlite_get.p99_latency_us())
            };

            println!("\nRedis GET:");
            println!("  Throughput: {:.0} ops/sec", redis_get_throughput);
            println!("  P50: {:.2} µs", redis_get_p50);
            println!("  P99: {:.2} µs", redis_get_p99);

            println!("\nRedlite GET:");
            println!("  Throughput: {:.0} ops/sec", redlite_get_throughput);
            println!("  P50: {:.2} µs", redlite_get_p50);
            println!("  P99: {:.2} µs", redlite_get_p99);

            // Run SET comparison
            println!("\n--- SET Comparison ---");
            let (redis_set_throughput, redis_set_p50, redis_set_p99) = if concurrency > 1 {
                let redis_set = redis_runner.bench_set_concurrent(mode).await?;
                (redis_set.throughput_ops_sec(), redis_set.p50_latency_us(), redis_set.p99_latency_us())
            } else {
                let redis_set = redis_runner.bench_set().await?;
                (redis_set.throughput_ops_sec(), redis_set.p50_latency_us(), redis_set.p99_latency_us())
            };

            let (redlite_set_throughput, redlite_set_p50, redlite_set_p99) = if concurrency > 1 {
                let redlite_set = redlite_runner.bench_set_concurrent(mode).await?;
                (redlite_set.throughput_ops_sec(), redlite_set.p50_latency_us(), redlite_set.p99_latency_us())
            } else {
                let redlite_set = redlite_runner.bench_set().await?;
                (redlite_set.throughput_ops_sec(), redlite_set.p50_latency_us(), redlite_set.p99_latency_us())
            };

            println!("\nRedis SET:");
            println!("  Throughput: {:.0} ops/sec", redis_set_throughput);
            println!("  P50: {:.2} µs", redis_set_p50);
            println!("  P99: {:.2} µs", redis_set_p99);

            println!("\nRedlite SET:");
            println!("  Throughput: {:.0} ops/sec", redlite_set_throughput);
            println!("  P50: {:.2} µs", redlite_set_p50);
            println!("  P99: {:.2} µs", redlite_set_p99);

            // Cleanup
            redis_runner.cleanup().await?;
            redlite_runner.cleanup().await?;

            println!("\n=== Comparison Complete ===");
        }
    }

    Ok(())
}
