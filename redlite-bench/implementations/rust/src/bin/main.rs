//! redlite-bench CLI
//!
//! Comprehensive Redis protocol benchmark suite

use clap::{Parser, Subcommand};
use redlite_bench::benchmark::{BenchmarkConfig, BenchmarkRunner};
use redlite_bench::client::{RedisClient, RedliteEmbeddedClient};
use redlite_bench::concurrency::ConcurrencyMode;
use redlite_bench::output::{OutputFormat, format_benchmark_result, write_output};
use redlite_bench::scenarios::{load_scenarios, find_scenario};

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

        /// Output format: console or json
        #[arg(long, default_value = "console")]
        output_format: String,

        /// Output file path (writes to stdout if not specified)
        #[arg(long)]
        output_file: Option<String>,
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

        /// Output format: console or json
        #[arg(long, default_value = "console")]
        output_format: String,

        /// Output file path (writes to stdout if not specified)
        #[arg(long)]
        output_file: Option<String>,
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

        /// Output format: console or json
        #[arg(long, default_value = "console")]
        output_format: String,

        /// Output file path (writes to stdout if not specified)
        #[arg(long)]
        output_file: Option<String>,
    },

    /// Run YAML-defined workload scenarios
    Scenario {
        /// Path to YAML scenario file
        #[arg(short, long)]
        scenario_file: String,

        /// Name of the scenario to run (from the YAML file)
        #[arg(short, long)]
        name: String,

        /// Backend: redis or redlite
        #[arg(short, long, default_value = "redlite")]
        backend: String,

        /// Redis connection URL (when backend=redis)
        #[arg(long, default_value = "redis://127.0.0.1:6379")]
        redis_url: String,

        /// Use in-memory database (when backend=redlite)
        #[arg(long)]
        memory: bool,

        /// Path to database file (when backend=redlite, if not using memory)
        #[arg(long)]
        db_path: Option<String>,

        /// Number of iterations to run
        #[arg(short, long, default_value = "100000")]
        iterations: usize,

        /// Dataset size (number of keys)
        #[arg(short, long, default_value = "10000")]
        dataset_size: usize,

        /// Output format: console or json
        #[arg(long, default_value = "console")]
        output_format: String,

        /// Output file path (writes to stdout if not specified)
        #[arg(long)]
        output_file: Option<String>,
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

fn parse_output_format(format: &str) -> OutputFormat {
    match format.to_lowercase().as_str() {
        "json" => OutputFormat::Json,
        _ => OutputFormat::Console,
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
            output_format,
            output_file,
        } => {
            let out_format = parse_output_format(&output_format);

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

            let mut all_results = Vec::new();

            for op in ops {
                let result = match op.as_str() {
                    "get" => {
                        if concurrency > 1 {
                            let concurrent = runner.bench_get_concurrent(mode).await?;
                            let output = redlite_bench::output::format_concurrent_result(&concurrent, out_format, "Redis")?;
                            all_results.push(output);
                            continue;
                        } else {
                            runner.bench_get().await?
                        }
                    }
                    "set" => {
                        if concurrency > 1 {
                            let concurrent = runner.bench_set_concurrent(mode).await?;
                            let output = redlite_bench::output::format_concurrent_result(&concurrent, out_format, "Redis")?;
                            all_results.push(output);
                            continue;
                        } else {
                            runner.bench_set().await?
                        }
                    }
                    "incr" => runner.bench_incr().await?,
                    "lpush" => {
                        if concurrency > 1 {
                            let concurrent = runner.bench_lpush_concurrent(mode).await?;
                            let output = redlite_bench::output::format_concurrent_result(&concurrent, out_format, "Redis")?;
                            all_results.push(output);
                            continue;
                        } else {
                            runner.bench_lpush().await?
                        }
                    }
                    "hset" => {
                        if concurrency > 1 {
                            let concurrent = runner.bench_hset_concurrent(mode).await?;
                            let output = redlite_bench::output::format_concurrent_result(&concurrent, out_format, "Redis")?;
                            all_results.push(output);
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
                let output = format_benchmark_result(&result, out_format, "Redis")?;
                all_results.push(output);
            }

            // Write accumulated JSON output if needed
            if matches!(out_format, OutputFormat::Json) && !all_results.is_empty() {
                let combined = all_results.join("\n");
                write_output(&combined, output_file.as_deref())?;
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
            output_format,
            output_file,
        } => {
            let out_format = parse_output_format(&output_format);
            let backend_name = "Redlite (embedded)";

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
                backend_name: backend_name.to_string(),
                dataset_size,
                iterations,
                warmup_iterations: 1000,
                concurrency,
            };

            let runner = BenchmarkRunner::new(client, config);
            let ops = parse_operations(&operations);

            println!("Running benchmarks with {} concurrency in {} mode\n", concurrency, mode);

            let mut all_results = Vec::new();

            for op in ops {
                let result = match op.as_str() {
                    "get" => {
                        if concurrency > 1 {
                            let concurrent = runner.bench_get_concurrent(mode).await?;
                            let output = redlite_bench::output::format_concurrent_result(&concurrent, out_format, backend_name)?;
                            all_results.push(output);
                            continue;
                        } else {
                            runner.bench_get().await?
                        }
                    }
                    "set" => {
                        if concurrency > 1 {
                            let concurrent = runner.bench_set_concurrent(mode).await?;
                            let output = redlite_bench::output::format_concurrent_result(&concurrent, out_format, backend_name)?;
                            all_results.push(output);
                            continue;
                        } else {
                            runner.bench_set().await?
                        }
                    }
                    "incr" => runner.bench_incr().await?,
                    "lpush" => {
                        if concurrency > 1 {
                            let concurrent = runner.bench_lpush_concurrent(mode).await?;
                            let output = redlite_bench::output::format_concurrent_result(&concurrent, out_format, backend_name)?;
                            all_results.push(output);
                            continue;
                        } else {
                            runner.bench_lpush().await?
                        }
                    }
                    "hset" => {
                        if concurrency > 1 {
                            let concurrent = runner.bench_hset_concurrent(mode).await?;
                            let output = redlite_bench::output::format_concurrent_result(&concurrent, out_format, backend_name)?;
                            all_results.push(output);
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
                let output = format_benchmark_result(&result, out_format, backend_name)?;
                all_results.push(output);
            }

            // Write accumulated JSON output if needed
            if matches!(out_format, OutputFormat::Json) && !all_results.is_empty() {
                let combined = all_results.join("\n");
                write_output(&combined, output_file.as_deref())?;
            }

            runner.cleanup().await?;
        }

        Commands::Compare {
            redis_url,
            iterations,
            dataset_size,
            concurrency,
            concurrency_mode,
            output_format,
            output_file,
        } => {
            let out_format = parse_output_format(&output_format);
            let is_json = matches!(out_format, OutputFormat::Json);

            if !is_json {
                println!("=== Quick Comparison: Redis vs Redlite ===\n");
            }
            let mode = parse_concurrency_mode(&concurrency_mode);

            // Redis benchmarks
            if !is_json {
                println!("Connecting to Redis at {}...", redis_url);
            }
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
            if !is_json {
                println!("Creating in-memory Redlite database...");
            }
            let redlite_client = RedliteEmbeddedClient::new_memory()?;
            let redlite_config = BenchmarkConfig {
                backend_name: "Redlite (embedded)".to_string(),
                dataset_size,
                iterations,
                warmup_iterations: 100,
                concurrency,
            };
            let redlite_runner = BenchmarkRunner::new(redlite_client, redlite_config);

            let mut all_results = Vec::new();

            // Run GET comparison
            if !is_json {
                println!("\n--- GET Comparison ---");
            }
            let (redis_get_throughput, redis_get_p50, redis_get_p99) = if concurrency > 1 {
                let redis_get = redis_runner.bench_get_concurrent(mode).await?;
                if is_json {
                    all_results.push(redlite_bench::output::format_concurrent_result(&redis_get, out_format, "Redis")?);
                }
                (redis_get.throughput_ops_sec(), redis_get.p50_latency_us(), redis_get.p99_latency_us())
            } else {
                let redis_get = redis_runner.bench_get().await?;
                if is_json {
                    all_results.push(format_benchmark_result(&redis_get, out_format, "Redis")?);
                }
                (redis_get.throughput_ops_sec(), redis_get.p50_latency_us(), redis_get.p99_latency_us())
            };

            let (redlite_get_throughput, redlite_get_p50, redlite_get_p99) = if concurrency > 1 {
                let redlite_get = redlite_runner.bench_get_concurrent(mode).await?;
                if is_json {
                    all_results.push(redlite_bench::output::format_concurrent_result(&redlite_get, out_format, "Redlite (embedded)")?);
                }
                (redlite_get.throughput_ops_sec(), redlite_get.p50_latency_us(), redlite_get.p99_latency_us())
            } else {
                let redlite_get = redlite_runner.bench_get().await?;
                if is_json {
                    all_results.push(format_benchmark_result(&redlite_get, out_format, "Redlite (embedded)")?);
                }
                (redlite_get.throughput_ops_sec(), redlite_get.p50_latency_us(), redlite_get.p99_latency_us())
            };

            if !is_json {
                println!("\nRedis GET:");
                println!("  Throughput: {:.0} ops/sec", redis_get_throughput);
                println!("  P50: {:.2} µs", redis_get_p50);
                println!("  P99: {:.2} µs", redis_get_p99);

                println!("\nRedlite GET:");
                println!("  Throughput: {:.0} ops/sec", redlite_get_throughput);
                println!("  P50: {:.2} µs", redlite_get_p50);
                println!("  P99: {:.2} µs", redlite_get_p99);
            }

            // Run SET comparison
            if !is_json {
                println!("\n--- SET Comparison ---");
            }
            let (redis_set_throughput, redis_set_p50, redis_set_p99) = if concurrency > 1 {
                let redis_set = redis_runner.bench_set_concurrent(mode).await?;
                if is_json {
                    all_results.push(redlite_bench::output::format_concurrent_result(&redis_set, out_format, "Redis")?);
                }
                (redis_set.throughput_ops_sec(), redis_set.p50_latency_us(), redis_set.p99_latency_us())
            } else {
                let redis_set = redis_runner.bench_set().await?;
                if is_json {
                    all_results.push(format_benchmark_result(&redis_set, out_format, "Redis")?);
                }
                (redis_set.throughput_ops_sec(), redis_set.p50_latency_us(), redis_set.p99_latency_us())
            };

            let (redlite_set_throughput, redlite_set_p50, redlite_set_p99) = if concurrency > 1 {
                let redlite_set = redlite_runner.bench_set_concurrent(mode).await?;
                if is_json {
                    all_results.push(redlite_bench::output::format_concurrent_result(&redlite_set, out_format, "Redlite (embedded)")?);
                }
                (redlite_set.throughput_ops_sec(), redlite_set.p50_latency_us(), redlite_set.p99_latency_us())
            } else {
                let redlite_set = redlite_runner.bench_set().await?;
                if is_json {
                    all_results.push(format_benchmark_result(&redlite_set, out_format, "Redlite (embedded)")?);
                }
                (redlite_set.throughput_ops_sec(), redlite_set.p50_latency_us(), redlite_set.p99_latency_us())
            };

            if !is_json {
                println!("\nRedis SET:");
                println!("  Throughput: {:.0} ops/sec", redis_set_throughput);
                println!("  P50: {:.2} µs", redis_set_p50);
                println!("  P99: {:.2} µs", redis_set_p99);

                println!("\nRedlite SET:");
                println!("  Throughput: {:.0} ops/sec", redlite_set_throughput);
                println!("  P50: {:.2} µs", redlite_set_p50);
                println!("  P99: {:.2} µs", redlite_set_p99);
            }

            // Cleanup
            redis_runner.cleanup().await?;
            redlite_runner.cleanup().await?;

            // Write JSON output if needed
            if is_json && !all_results.is_empty() {
                let combined = all_results.join("\n");
                write_output(&combined, output_file.as_deref())?;
            }

            if !is_json {
                println!("\n=== Comparison Complete ===");
            }
        }

        Commands::Scenario {
            scenario_file,
            name,
            backend,
            redis_url,
            memory,
            db_path,
            iterations,
            dataset_size,
            output_format,
            output_file,
        } => {
            let out_format = parse_output_format(&output_format);
            let is_json = matches!(out_format, OutputFormat::Json);

            // Load scenarios from YAML file
            let scenarios = load_scenarios(&scenario_file)?;
            let scenario = find_scenario(&scenarios, &name)
                .ok_or_else(|| anyhow::anyhow!("Scenario '{}' not found in {}", name, scenario_file))?;

            if !is_json {
                println!("Running scenario: {}", scenario.name);
                if let Some(desc) = &scenario.description {
                    println!("Description: {}", desc);
                }
                println!("Operations:");
                for op in &scenario.operations {
                    println!("  - {}: weight {}", op.operation, op.weight);
                }
                println!();
            }

            // Prepare weighted operation selection
            let normalized = scenario.normalized_operations();

            let backend_name = if backend.to_lowercase() == "redis" {
                "Redis"
            } else {
                "Redlite (embedded)"
            };

            // Run scenario based on backend
            let result = match backend.to_lowercase().as_str() {
                "redis" => {
                    if !is_json {
                        println!("Connecting to Redis at {}...", redis_url);
                    }
                    let client = RedisClient::new(&redis_url)?;
                    run_scenario_benchmark(&client, &scenario, &normalized, dataset_size, iterations, backend_name).await?
                }
                _ => {
                    let client = if memory || db_path.is_none() {
                        if !is_json {
                            println!("Using in-memory Redlite database...");
                        }
                        RedliteEmbeddedClient::new_memory()?
                    } else {
                        let path = db_path.unwrap();
                        if !is_json {
                            println!("Using Redlite database at {}...", path);
                        }
                        RedliteEmbeddedClient::new_file(&path)?
                    };
                    run_scenario_benchmark(&client, &scenario, &normalized, dataset_size, iterations, backend_name).await?
                }
            };

            // Output results
            let output = format_benchmark_result(&result, out_format, backend_name)?;
            if is_json {
                write_output(&output, output_file.as_deref())?;
            }
        }
    }

    Ok(())
}

/// Run a scenario benchmark using the dispatcher
async fn run_scenario_benchmark<C: redlite_bench::client::RedisLikeClient>(
    client: &C,
    scenario: &redlite_bench::scenarios::WorkloadScenario,
    normalized: &[(String, f64)],
    dataset_size: usize,
    iterations: usize,
    backend_name: &str,
) -> anyhow::Result<redlite_bench::benchmark::BenchmarkResult> {
    use rand::Rng;
    use std::time::Instant;

    let mut rng = rand::thread_rng();
    let mut result = redlite_bench::benchmark::BenchmarkResult::new(
        &format!("Scenario: {}", scenario.name),
        backend_name,
        dataset_size,
        1,
    );

    // Populate initial data for reads
    let value = redlite_bench::benchmark::generate_value();
    for i in 0..dataset_size {
        client.set(&format!("key_{}", i), &value).await?;
    }

    // Run warmup (1000 iterations)
    for _ in 0..1000 {
        let random_value: f64 = rng.gen();
        if let Some(op_name) = scenario.select_operation(normalized, random_value) {
            let _ = redlite_bench::dispatcher::execute_operation(
                client,
                &op_name,
                dataset_size,
                rng.clone(),
            ).await;
        }
    }

    // Run measured iterations
    let mut latencies = Vec::with_capacity(iterations);
    let start = Instant::now();

    for _ in 0..iterations {
        let random_value: f64 = rng.gen();
        if let Some(op_name) = scenario.select_operation(normalized, random_value) {
            match redlite_bench::dispatcher::execute_operation(
                client,
                &op_name,
                dataset_size,
                rng.clone(),
            ).await {
                Ok(latency_us) => {
                    latencies.push(latency_us);
                    result.successful_ops += 1;
                }
                Err(_) => {
                    result.failed_ops += 1;
                }
            }
        }
    }

    result.duration_secs = start.elapsed().as_secs_f64();
    result.latencies_us = latencies;
    result.iterations = iterations;

    // Cleanup
    client.flushdb().await?;

    Ok(result)
}
