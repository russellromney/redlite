//! redlite-bench CLI
//!
//! Comprehensive Redis protocol benchmark suite

use clap::{Parser, Subcommand};
use redlite_bench::benchmark::{BenchmarkConfig, BenchmarkRunner};
use redlite_bench::client::{RedisClient, RedliteEmbeddedClient, RedisLikeClient};
use redlite_bench::concurrency::ConcurrencyMode;
use redlite_bench::output::{OutputFormat, format_benchmark_result, write_output};
use redlite_bench::scenarios::{load_scenarios, find_scenario, execute_setup};

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

        /// Encryption key/passphrase (requires --features encryption build)
        #[arg(long)]
        encryption_key: Option<String>,

        /// Enable VFS-level compression (requires --features compression build)
        #[arg(long)]
        compression: bool,

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

        /// Skip scenario setup (use with pre-populated databases)
        #[arg(long)]
        skip_setup: bool,

        /// SQLite cache size in MB (default: 64)
        #[arg(long, default_value = "64")]
        cache_mb: i64,
    },

    /// Run comprehensive benchmarks comparing Redis vs Redlite
    RunBenchmarks {
        /// Path to YAML scenario file
        #[arg(short, long, default_value = "scenarios/comprehensive.yaml")]
        scenario_file: String,

        /// Comma-separated list of scenario names to run (runs all if not specified)
        #[arg(long)]
        scenarios: Option<String>,

        /// Redis connection URL
        #[arg(long, default_value = "redis://127.0.0.1:6379")]
        redis_url: String,

        /// Number of iterations per scenario
        #[arg(short, long, default_value = "50000")]
        iterations: usize,

        /// Dataset size (number of keys)
        #[arg(short, long, default_value = "10000")]
        dataset_size: usize,

        /// Report output format: json or markdown
        #[arg(long, default_value = "markdown")]
        report_format: String,

        /// Report file path
        #[arg(long)]
        report_file: Option<String>,
    },

    /// Run configuration matrix benchmarks
    RunMatrix {
        /// Path to matrix YAML file
        #[arg(short, long)]
        matrix_file: String,

        /// Path to scenario YAML file
        #[arg(short, long, default_value = "scenarios/comprehensive.yaml")]
        scenario_file: String,

        /// Report output format: json or markdown
        #[arg(long, default_value = "markdown")]
        report_format: String,

        /// Report file path
        #[arg(long)]
        report_file: Option<String>,

        /// Verbose output
        #[arg(short, long)]
        verbose: bool,
    },

    /// Generate a pre-populated database for benchmarking
    GenerateDb {
        /// Output database path
        #[arg(short, long)]
        output: String,

        /// Path to scenario YAML file for setup specs
        #[arg(short, long, default_value = "scenarios/comprehensive.yaml")]
        scenario_file: String,

        /// Scenario name to use for setup (or "all" for combined setup)
        #[arg(long, default_value = "all")]
        scenario: String,

        /// Number of string keys
        #[arg(long, default_value = "10000")]
        strings: usize,

        /// Number of list keys
        #[arg(long, default_value = "1000")]
        lists: usize,

        /// Items per list
        #[arg(long, default_value = "100")]
        items_per_list: usize,

        /// Number of hash keys
        #[arg(long, default_value = "1000")]
        hashes: usize,

        /// Fields per hash
        #[arg(long, default_value = "20")]
        fields_per_hash: usize,

        /// Number of set keys
        #[arg(long, default_value = "500")]
        sets: usize,

        /// Members per set
        #[arg(long, default_value = "100")]
        members_per_set: usize,

        /// Number of sorted set keys
        #[arg(long, default_value = "500")]
        sorted_sets: usize,

        /// Members per sorted set
        #[arg(long, default_value = "100")]
        members_per_sorted_set: usize,

        /// Verbose output
        #[arg(short, long)]
        verbose: bool,
    },

    /// Run scaling performance tests across different database sizes
    ScaleTest {
        /// Comma-separated sizes: 10k, 100k, 1m, 10m
        #[arg(short, long, default_value = "10k,100k,1m")]
        sizes: String,

        /// Comma-separated scenarios to run
        #[arg(long, default_value = "get_only,set_only,read_heavy")]
        scenarios: String,

        /// Path to scenario YAML file
        #[arg(short = 'f', long, default_value = "scenarios/comprehensive.yaml")]
        scenario_file: String,

        /// Number of iterations per scenario
        #[arg(short, long, default_value = "10000")]
        iterations: usize,

        /// Output directory for generated databases
        #[arg(long, default_value = "/tmp/redlite-scale")]
        db_dir: String,

        /// SQLite cache size in MB (default: 64, try 512+ for large datasets)
        #[arg(long, default_value = "64")]
        cache_mb: i64,

        /// Report output file
        #[arg(long)]
        report_file: Option<String>,

        /// Keep generated databases after test
        #[arg(long)]
        keep_dbs: bool,

        /// Verbose output
        #[arg(short, long)]
        verbose: bool,
    },

    /// Analyze access patterns and recommend cache sizing
    AnalyzeAccess {
        /// Path to database file
        #[arg(long)]
        db_path: String,

        /// Number of operations to sample
        #[arg(short, long, default_value = "100000")]
        iterations: usize,

        /// Dataset size (number of keys in database)
        #[arg(short, long)]
        dataset_size: usize,

        /// Key distribution: uniform, zipfian, temporal
        #[arg(long, default_value = "uniform")]
        distribution: String,

        /// Zipfian skew parameter (0.5-1.5, default 0.99)
        #[arg(long, default_value = "0.99")]
        zipf_skew: f64,

        /// Read percentage (0-100)
        #[arg(long, default_value = "70")]
        read_pct: u32,

        /// Write/update percentage (0-100)
        #[arg(long, default_value = "20")]
        write_pct: u32,

        /// Delete percentage (0-100, remainder after read+write)
        #[arg(long, default_value = "10")]
        delete_pct: u32,

        /// Target cache hit rate for recommendation (0.0-1.0)
        #[arg(long, default_value = "0.95")]
        target_hit_rate: f64,

        /// Average key+value size in bytes (for MB estimation)
        #[arg(long, default_value = "200")]
        entry_size_bytes: usize,
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
            encryption_key,
            compression,
            iterations,
            dataset_size,
            concurrency,
            concurrency_mode,
            operations,
            output_format,
            output_file,
        } => {
            let out_format = parse_output_format(&output_format);

            // Determine backend name based on options
            let backend_name = if encryption_key.is_some() {
                "Redlite (encrypted)"
            } else if compression {
                "Redlite (compressed)"
            } else {
                "Redlite (embedded)"
            };

            let client = if memory {
                if encryption_key.is_some() || compression {
                    anyhow::bail!("Encryption and compression require a file-backed database (use --path)");
                }
                println!("Using in-memory Redlite database...");
                RedliteEmbeddedClient::new_memory()?
            } else if let Some(ref key) = encryption_key {
                #[cfg(feature = "encryption")]
                {
                    let p = path.as_ref().ok_or_else(|| anyhow::anyhow!("--path required for encryption"))?;
                    println!("Using encrypted Redlite database at {}...", p);
                    RedliteEmbeddedClient::new_encrypted(p, key)?
                }
                #[cfg(not(feature = "encryption"))]
                {
                    let _ = key;
                    anyhow::bail!("Encryption support not compiled. Build with: cargo build --features encryption --no-default-features");
                }
            } else if compression {
                #[cfg(feature = "compression")]
                {
                    let p = path.as_ref().ok_or_else(|| anyhow::anyhow!("--path required for compression"))?;
                    println!("Using compressed Redlite database at {}...", p);
                    RedliteEmbeddedClient::new_compressed(p)?
                }
                #[cfg(not(feature = "compression"))]
                {
                    anyhow::bail!("Compression support not compiled. Build with: cargo build --features compression");
                }
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
            skip_setup,
            cache_mb,
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
                    run_scenario_benchmark(&client, &scenario, &normalized, dataset_size, iterations, backend_name, is_json, skip_setup).await?
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
                            println!("Using Redlite database at {} (cache: {}MB)...", path, cache_mb);
                        }
                        // Use open_with_cache for configurable cache size
                        let db = redlite::Db::open_with_cache(&path, cache_mb)?;
                        RedliteEmbeddedClient::from_db(std::sync::Arc::new(db))
                    };
                    run_scenario_benchmark(&client, &scenario, &normalized, dataset_size, iterations, backend_name, is_json, skip_setup).await?
                }
            };

            // Output results
            let output = format_benchmark_result(&result, out_format, backend_name)?;
            if is_json {
                write_output(&output, output_file.as_deref())?;
            }
        }

        Commands::RunBenchmarks {
            scenario_file,
            scenarios,
            redis_url,
            iterations,
            dataset_size,
            report_format,
            report_file,
        } => {
            use redlite_bench::{MultiScenarioRunner, ReportGenerator, ReportFormat};
            use std::str::FromStr;

            println!("Loading scenarios from {}...", scenario_file);
            let all_scenarios = load_scenarios(&scenario_file)?;

            // Filter scenarios if specified
            let scenarios_to_run: Vec<_> = if let Some(scenario_names) = &scenarios {
                let names: Vec<&str> = scenario_names.split(',').map(|s| s.trim()).collect();
                all_scenarios
                    .into_iter()
                    .filter(|s| names.contains(&s.name.as_str()))
                    .collect()
            } else {
                all_scenarios
            };

            if scenarios_to_run.is_empty() {
                println!("No matching scenarios found!");
                return Ok(());
            }

            println!(
                "Running {} scenarios ({} iterations, {} dataset size)...\n",
                scenarios_to_run.len(),
                iterations,
                dataset_size
            );

            // Create scenario metadata for report
            let scenario_metadata: Vec<_> = scenarios_to_run
                .iter()
                .map(|s| (s.name.clone(), s.description.clone()))
                .collect();

            // Connect to backends
            println!("Connecting to Redis at {}...", redis_url);
            let redis_client = match RedisClient::new(&redis_url) {
                Ok(client) => {
                    println!("✓ Redis connected");
                    Some(client)
                }
                Err(e) => {
                    println!("✗ Failed to connect to Redis: {}", e);
                    None
                }
            };

            println!("Creating in-memory Redlite database...");
            let redlite_client = match RedliteEmbeddedClient::new_memory() {
                Ok(client) => {
                    println!("✓ Redlite created");
                    Some(client)
                }
                Err(e) => {
                    println!("✗ Failed to create Redlite: {}", e);
                    None
                }
            };

            // Run benchmarks
            let runner = MultiScenarioRunner::new(scenarios_to_run, iterations, dataset_size);
            let mut comparisons = Vec::new();

            for (idx, scenario) in runner.scenarios.iter().enumerate() {
                println!(
                    "\n[{}/{}] Running scenario: {}",
                    idx + 1,
                    runner.scenarios.len(),
                    scenario.name
                );

                match (&redis_client, &redlite_client) {
                    (Some(redis), Some(redlite)) => {
                        match runner.run_scenario_comparison(scenario, redis, redlite).await {
                            Ok(comparison) => {
                                if let Some((redlite_tps, redis_tps, diff)) = comparison.throughput_diff() {
                                    println!(
                                        "  Redis: {:.0} ops/sec | Redlite: {:.0} ops/sec ({:+.1}%)",
                                        redis_tps, redlite_tps, diff
                                    );
                                } else {
                                    println!("  One or both backends failed");
                                }
                                comparisons.push(comparison);
                            }
                            Err(e) => println!("  Error: {}", e),
                        }
                    }
                    _ => println!("  Skipped: missing backend connection"),
                }
            }

            // Generate report
            println!("\n\nGenerating report...");
            let report = ReportGenerator::generate_report(comparisons, &scenario_metadata);

            // Save report
            let report_fmt = ReportFormat::from_str(&report_format)
                .unwrap_or(ReportFormat::Markdown);

            if let Some(path) = report_file {
                match ReportGenerator::save_report(&report, &path, report_fmt) {
                    Ok(_) => println!("✓ Report saved to: {}", path),
                    Err(e) => println!("✗ Failed to save report: {}", e),
                }
            } else {
                // Print to console
                match report_fmt {
                    ReportFormat::Markdown => {
                        let markdown = ReportGenerator::format_markdown(&report);
                        println!("{}", markdown);
                    }
                    ReportFormat::Json => {
                        match serde_json::to_string_pretty(&report) {
                            Ok(json) => println!("{}", json),
                            Err(e) => println!("Error formatting JSON: {}", e),
                        }
                    }
                }
            }

            println!("\n✓ Benchmark run complete!");
        }

        Commands::RunMatrix {
            matrix_file,
            scenario_file,
            report_format,
            report_file,
            verbose,
        } => {
            use redlite_bench::{load_matrix_spec, MatrixRunner};

            println!("Loading matrix specification from {}...", matrix_file);
            let spec = load_matrix_spec(&matrix_file)?;

            println!(
                "Matrix: {} configurations x {} scenarios",
                spec.configurations.len(),
                if spec.scenarios.is_empty() {
                    "all".to_string()
                } else {
                    spec.scenarios.len().to_string()
                }
            );
            println!(
                "Settings: {} iterations, {} dataset size\n",
                spec.iterations, spec.dataset_size
            );

            let runner = MatrixRunner::new(spec, scenario_file);
            let report = runner.run(verbose).await?;

            // Generate report
            match report_format.to_lowercase().as_str() {
                "json" => {
                    let json = serde_json::to_string_pretty(&report)?;
                    if let Some(path) = report_file {
                        std::fs::write(&path, &json)?;
                        println!("\n✓ JSON report saved to: {}", path);
                    } else {
                        println!("{}", json);
                    }
                }
                _ => {
                    let markdown = report.to_markdown();
                    if let Some(path) = report_file {
                        std::fs::write(&path, &markdown)?;
                        println!("\n✓ Markdown report saved to: {}", path);
                    } else {
                        println!("{}", markdown);
                    }
                }
            }

            println!("\n✓ Matrix benchmark complete!");
            println!(
                "  Tested {} configurations across {} scenarios",
                report.configurations.len(),
                report.scenarios.len()
            );
            println!("  Total time: {:.1}s", report.total_duration_secs);
        }

        Commands::GenerateDb {
            output,
            scenario_file: _,
            scenario: _,
            strings,
            lists,
            items_per_list,
            hashes,
            fields_per_hash,
            sets,
            members_per_set,
            sorted_sets,
            members_per_sorted_set,
            verbose,
        } => {
            use std::time::Instant;

            println!("Generating pre-populated database (raw SQL mode): {}", output);

            // Clean up any existing file
            let _ = std::fs::remove_file(&output);
            let _ = std::fs::remove_file(format!("{}-wal", output));
            let _ = std::fs::remove_file(format!("{}-shm", output));

            let start = Instant::now();

            // Open raw SQLite connection for maximum speed
            let conn = rusqlite::Connection::open(&output)?;

            // Performance optimizations
            conn.execute_batch(r#"
                PRAGMA journal_mode = OFF;
                PRAGMA synchronous = OFF;
                PRAGMA cache_size = -256000;
                PRAGMA temp_store = MEMORY;
                PRAGMA locking_mode = EXCLUSIVE;
            "#)?;

            // Create schema
            conn.execute_batch(include_str!("../../../../../crates/redlite/src/schema.sql"))?;

            let value: Vec<u8> = (0..100).map(|_| b'x').collect();
            let now_ms = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as i64;

            // Type constants from redlite (match KeyType enum)
            const TYPE_STRING: i32 = 1;
            const TYPE_HASH: i32 = 2;
            const TYPE_LIST: i32 = 3;
            const TYPE_SET: i32 = 4;
            const TYPE_ZSET: i32 = 5;

            let mut key_id: i64 = 1;

            // Generate strings - single transaction with batch inserts
            if strings > 0 {
                if verbose { println!("  Creating {} string keys...", strings); }
                conn.execute_batch("BEGIN TRANSACTION")?;
                {
                    let mut key_stmt = conn.prepare_cached(
                        "INSERT INTO keys (id, db, key, type, created_at, updated_at) VALUES (?, 0, ?, ?, ?, ?)"
                    )?;
                    let mut str_stmt = conn.prepare_cached(
                        "INSERT INTO strings (key_id, value) VALUES (?, ?)"
                    )?;
                    for i in 0..strings {
                        let key_name = format!("key_{}", i);
                        key_stmt.execute(rusqlite::params![key_id, key_name, TYPE_STRING, now_ms, now_ms])?;
                        str_stmt.execute(rusqlite::params![key_id, &value])?;
                        key_id += 1;
                    }
                }
                conn.execute_batch("COMMIT")?;
            }

            // Generate lists
            if lists > 0 {
                if verbose { println!("  Creating {} lists with {} items each...", lists, items_per_list); }
                conn.execute_batch("BEGIN TRANSACTION")?;
                {
                    let mut key_stmt = conn.prepare_cached(
                        "INSERT INTO keys (id, db, key, type, created_at, updated_at) VALUES (?, 0, ?, ?, ?, ?)"
                    )?;
                    let mut list_stmt = conn.prepare_cached(
                        "INSERT INTO lists (key_id, pos, value) VALUES (?, ?, ?)"
                    )?;
                    for i in 0..lists {
                        let key_name = format!("list_{}", i);
                        key_stmt.execute(rusqlite::params![key_id, key_name, TYPE_LIST, now_ms, now_ms])?;
                        for pos in 0..items_per_list {
                            list_stmt.execute(rusqlite::params![key_id, pos as i64, &value])?;
                        }
                        key_id += 1;
                    }
                }
                conn.execute_batch("COMMIT")?;
            }

            // Generate hashes
            if hashes > 0 {
                if verbose { println!("  Creating {} hashes with {} fields each...", hashes, fields_per_hash); }
                conn.execute_batch("BEGIN TRANSACTION")?;
                {
                    let mut key_stmt = conn.prepare_cached(
                        "INSERT INTO keys (id, db, key, type, created_at, updated_at) VALUES (?, 0, ?, ?, ?, ?)"
                    )?;
                    let mut hash_stmt = conn.prepare_cached(
                        "INSERT INTO hashes (key_id, field, value) VALUES (?, ?, ?)"
                    )?;
                    for i in 0..hashes {
                        let key_name = format!("hash_{}", i);
                        key_stmt.execute(rusqlite::params![key_id, key_name, TYPE_HASH, now_ms, now_ms])?;
                        for f in 0..fields_per_hash {
                            let field = format!("field_{}", f);
                            hash_stmt.execute(rusqlite::params![key_id, field, &value])?;
                        }
                        key_id += 1;
                    }
                }
                conn.execute_batch("COMMIT")?;
            }

            // Generate sets
            if sets > 0 {
                if verbose { println!("  Creating {} sets with {} members each...", sets, members_per_set); }
                conn.execute_batch("BEGIN TRANSACTION")?;
                {
                    let mut key_stmt = conn.prepare_cached(
                        "INSERT INTO keys (id, db, key, type, created_at, updated_at) VALUES (?, 0, ?, ?, ?, ?)"
                    )?;
                    let mut set_stmt = conn.prepare_cached(
                        "INSERT INTO sets (key_id, member) VALUES (?, ?)"
                    )?;
                    for i in 0..sets {
                        let key_name = format!("set_{}", i);
                        key_stmt.execute(rusqlite::params![key_id, key_name, TYPE_SET, now_ms, now_ms])?;
                        for m in 0..members_per_set {
                            let member = format!("member_{}", m).into_bytes();
                            set_stmt.execute(rusqlite::params![key_id, member])?;
                        }
                        key_id += 1;
                    }
                }
                conn.execute_batch("COMMIT")?;
            }

            // Generate sorted sets
            if sorted_sets > 0 {
                if verbose { println!("  Creating {} sorted sets with {} members each...", sorted_sets, members_per_sorted_set); }
                conn.execute_batch("BEGIN TRANSACTION")?;
                {
                    let mut key_stmt = conn.prepare_cached(
                        "INSERT INTO keys (id, db, key, type, created_at, updated_at) VALUES (?, 0, ?, ?, ?, ?)"
                    )?;
                    let mut zset_stmt = conn.prepare_cached(
                        "INSERT INTO zsets (key_id, member, score) VALUES (?, ?, ?)"
                    )?;
                    for i in 0..sorted_sets {
                        let key_name = format!("zset_{}", i);
                        key_stmt.execute(rusqlite::params![key_id, key_name, TYPE_ZSET, now_ms, now_ms])?;
                        for m in 0..members_per_sorted_set {
                            let member = format!("member_{}", m).into_bytes();
                            zset_stmt.execute(rusqlite::params![key_id, member, m as f64])?;
                        }
                        key_id += 1;
                    }
                }
                conn.execute_batch("COMMIT")?;
            }

            // Final vacuum for optimal file size
            if verbose { println!("  Optimizing database..."); }
            conn.execute_batch("PRAGMA journal_mode = DELETE; VACUUM;")?;

            let elapsed = start.elapsed();
            let file_size = std::fs::metadata(&output).map(|m| m.len()).unwrap_or(0);

            println!("\n✓ Database generated: {}", output);
            println!("  Strings: {}", strings);
            println!("  Lists: {} x {} items = {} total", lists, items_per_list, lists * items_per_list);
            println!("  Hashes: {} x {} fields = {} total", hashes, fields_per_hash, hashes * fields_per_hash);
            println!("  Sets: {} x {} members = {} total", sets, members_per_set, sets * members_per_set);
            println!("  Sorted sets: {} x {} members = {} total", sorted_sets, members_per_sorted_set, sorted_sets * members_per_sorted_set);
            println!("  File size: {:.2} MB", file_size as f64 / 1_048_576.0);
            println!("  Time: {:.2}s", elapsed.as_secs_f64());
        }

        Commands::ScaleTest {
            sizes,
            scenarios,
            scenario_file,
            iterations,
            db_dir,
            cache_mb,
            report_file,
            keep_dbs,
            verbose,
        } => {
            use std::sync::Arc;
            use std::time::Instant;

            // Parse sizes (e.g., "10k,100k,1m" -> [(10000, "10k"), ...])
            let size_specs: Vec<(usize, String)> = sizes
                .split(',')
                .filter_map(|s| {
                    let s = s.trim().to_lowercase();
                    let multiplier = if s.ends_with('m') {
                        1_000_000
                    } else if s.ends_with('k') {
                        1_000
                    } else {
                        1
                    };
                    let num_str = s.trim_end_matches(|c| c == 'k' || c == 'm');
                    num_str.parse::<usize>().ok().map(|n| (n * multiplier, s.clone()))
                })
                .collect();

            if size_specs.is_empty() {
                anyhow::bail!("No valid sizes specified. Use format: 10k,100k,1m,10m");
            }

            // Parse scenario names
            let scenario_names: Vec<String> = scenarios
                .split(',')
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect();

            // Load scenarios from file
            let all_scenarios = load_scenarios(&scenario_file)?;
            let scenarios_to_run: Vec<_> = all_scenarios
                .into_iter()
                .filter(|s| scenario_names.contains(&s.name))
                .collect();

            if scenarios_to_run.is_empty() {
                anyhow::bail!("No matching scenarios found. Available: {:?}",
                    scenario_names);
            }

            // Create output directory
            std::fs::create_dir_all(&db_dir)?;

            println!("=== Redlite Scale Test ===");
            println!("Sizes: {:?}", size_specs.iter().map(|(_, s)| s).collect::<Vec<_>>());
            println!("Scenarios: {:?}", scenarios_to_run.iter().map(|s| &s.name).collect::<Vec<_>>());
            println!("Iterations per test: {}", iterations);
            println!("SQLite cache size: {} MB", cache_mb);
            println!();

            // Results: size -> scenario -> (throughput, p50, p99)
            let mut results: Vec<(String, Vec<(String, f64, f64, f64)>)> = Vec::new();

            let total_start = Instant::now();

            for (size, size_label) in &size_specs {
                println!("\n=== Size: {} ({} keys) ===", size_label, size);

                let db_path = format!("{}/scale_{}.db", db_dir, size_label);

                // Generate database
                if verbose { println!("  Generating database..."); }
                let gen_start = Instant::now();

                // Clean up existing
                let _ = std::fs::remove_file(&db_path);
                let _ = std::fs::remove_file(format!("{}-wal", db_path));
                let _ = std::fs::remove_file(format!("{}-shm", db_path));

                // Use raw SQL generation for speed
                {
                    let conn = rusqlite::Connection::open(&db_path)?;
                    conn.execute_batch(r#"
                        PRAGMA journal_mode = OFF;
                        PRAGMA synchronous = OFF;
                        PRAGMA cache_size = -256000;
                        PRAGMA temp_store = MEMORY;
                        PRAGMA locking_mode = EXCLUSIVE;
                    "#)?;
                    conn.execute_batch(include_str!("../../../../../crates/redlite/src/schema.sql"))?;

                    let value: Vec<u8> = (0..100).map(|_| b'x').collect();
                    let now_ms = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap()
                        .as_millis() as i64;

                    const TYPE_STRING: i32 = 1; // Match KeyType::String

                    conn.execute_batch("BEGIN TRANSACTION")?;
                    {
                        let mut key_stmt = conn.prepare_cached(
                            "INSERT INTO keys (id, db, key, type, created_at, updated_at) VALUES (?, 0, ?, ?, ?, ?)"
                        )?;
                        let mut str_stmt = conn.prepare_cached(
                            "INSERT INTO strings (key_id, value) VALUES (?, ?)"
                        )?;
                        for i in 0..*size {
                            let key_name = format!("key_{}", i);
                            key_stmt.execute(rusqlite::params![i as i64 + 1, key_name, TYPE_STRING, now_ms, now_ms])?;
                            str_stmt.execute(rusqlite::params![i as i64 + 1, &value])?;
                        }
                    }
                    conn.execute_batch("COMMIT")?;
                    conn.execute_batch("PRAGMA journal_mode = DELETE;")?;
                }

                let file_size = std::fs::metadata(&db_path).map(|m| m.len()).unwrap_or(0);
                if verbose {
                    println!("  Generated {} keys in {:.2}s ({:.1} MB)",
                        size, gen_start.elapsed().as_secs_f64(), file_size as f64 / 1_048_576.0);
                }

                // Open with redlite and run scenarios (using configured cache size)
                let db = redlite::Db::open_with_cache(&db_path, cache_mb)?;
                let client = RedliteEmbeddedClient::from_db(Arc::new(db));

                let mut size_results: Vec<(String, f64, f64, f64)> = Vec::new();

                for scenario in &scenarios_to_run {
                    if verbose { print!("  Running {}... ", scenario.name); }

                    let normalized = scenario.normalized_operations();
                    let result = run_scale_scenario(&client, scenario, &normalized, *size, iterations).await?;

                    let throughput = result.throughput_ops_sec();
                    let p50 = result.p50_latency_us();
                    let p99 = result.p99_latency_us();

                    if verbose {
                        println!("{:.0} ops/sec, p50: {:.1}µs, p99: {:.1}µs", throughput, p50, p99);
                    } else {
                        println!("  {}: {:.0} ops/sec", scenario.name, throughput);
                    }

                    size_results.push((scenario.name.clone(), throughput, p50, p99));
                }

                results.push((size_label.clone(), size_results));

                // Cleanup if not keeping
                if !keep_dbs {
                    let _ = std::fs::remove_file(&db_path);
                    let _ = std::fs::remove_file(format!("{}-wal", db_path));
                    let _ = std::fs::remove_file(format!("{}-shm", db_path));
                }
            }

            let total_elapsed = total_start.elapsed();

            // Generate report
            let mut report = String::new();
            report.push_str("# Redlite Scale Test Report\n\n");
            report.push_str(&format!("**Date:** {}\n", chrono::Utc::now().format("%Y-%m-%d %H:%M:%S UTC")));
            report.push_str(&format!("**Duration:** {:.1}s\n", total_elapsed.as_secs_f64()));
            report.push_str(&format!("**Iterations per test:** {}\n", iterations));
            report.push_str(&format!("**SQLite cache size:** {} MB\n\n", cache_mb));

            // Throughput table
            report.push_str("## Throughput (ops/sec)\n\n");
            report.push_str("| Size |");
            for scenario in &scenarios_to_run {
                report.push_str(&format!(" {} |", scenario.name));
            }
            report.push_str("\n|------|");
            for _ in &scenarios_to_run {
                report.push_str("--------:|");
            }
            report.push_str("\n");

            for (size_label, size_results) in &results {
                report.push_str(&format!("| {} |", size_label));
                for (_, throughput, _, _) in size_results {
                    report.push_str(&format!(" {:.0} |", throughput));
                }
                report.push_str("\n");
            }

            // P99 latency table
            report.push_str("\n## P99 Latency (µs)\n\n");
            report.push_str("| Size |");
            for scenario in &scenarios_to_run {
                report.push_str(&format!(" {} |", scenario.name));
            }
            report.push_str("\n|------|");
            for _ in &scenarios_to_run {
                report.push_str("--------:|");
            }
            report.push_str("\n");

            for (size_label, size_results) in &results {
                report.push_str(&format!("| {} |", size_label));
                for (_, _, _, p99) in size_results {
                    report.push_str(&format!(" {:.1} |", p99));
                }
                report.push_str("\n");
            }

            // Scaling analysis
            report.push_str("\n## Scaling Analysis\n\n");
            for scenario in &scenarios_to_run {
                report.push_str(&format!("### {}\n\n", scenario.name));
                let first_throughput = results.first()
                    .and_then(|(_, r)| r.iter().find(|(n, _, _, _)| n == &scenario.name))
                    .map(|(_, t, _, _)| *t);

                if let Some(base) = first_throughput {
                    for (size_label, size_results) in &results {
                        if let Some((_, throughput, _, _)) = size_results.iter().find(|(n, _, _, _)| n == &scenario.name) {
                            let ratio = throughput / base * 100.0;
                            report.push_str(&format!("- **{}**: {:.0} ops/sec ({:.0}% of baseline)\n",
                                size_label, throughput, ratio));
                        }
                    }
                }
                report.push_str("\n");
            }

            // Output report
            if let Some(path) = report_file {
                std::fs::write(&path, &report)?;
                println!("\n✓ Report saved to: {}", path);
            } else {
                println!("\n{}", report);
            }

            println!("✓ Scale test complete in {:.1}s", total_elapsed.as_secs_f64());
        }

        Commands::AnalyzeAccess {
            db_path,
            iterations,
            dataset_size,
            distribution,
            zipf_skew,
            read_pct,
            write_pct,
            delete_pct,
            target_hit_rate,
            entry_size_bytes,
        } => {
            use redlite_bench::access_pattern::{KeyDistribution, KeyGenerator, AccessAnalyzer};
            use rand::Rng;

            println!("=== Access Pattern Analysis ===\n");

            // Normalize percentages
            let total_pct = read_pct + write_pct + delete_pct;
            let read_threshold = read_pct as f64 / total_pct as f64;
            let write_threshold = read_threshold + (write_pct as f64 / total_pct as f64);
            // delete is the remainder

            // Parse distribution
            let key_dist = match distribution.to_lowercase().as_str() {
                "zipfian" | "zipf" => KeyDistribution::Zipfian { skew: zipf_skew },
                "temporal" => KeyDistribution::Temporal { decay: 0.5 },
                "sequential" | "seq" => KeyDistribution::Sequential,
                _ => KeyDistribution::Uniform,
            };

            println!("Configuration:");
            println!("  Database: {}", db_path);
            println!("  Dataset size: {} keys", dataset_size);
            println!("  Distribution: {:?}", key_dist);
            println!("  Operation mix: {}% read / {}% write / {}% delete",
                (read_threshold * 100.0) as u32,
                ((write_threshold - read_threshold) * 100.0) as u32,
                ((1.0 - write_threshold) * 100.0) as u32);
            println!("  Sampling: {} operations", iterations);
            println!();

            // Open database
            let db = redlite::Db::open(&db_path)?;
            let client = RedliteEmbeddedClient::from_db(std::sync::Arc::new(db));

            // Initialize key generator and analyzer
            let mut key_gen = KeyGenerator::new(key_dist, dataset_size);
            let mut analyzer = AccessAnalyzer::new(dataset_size, iterations);
            let mut rng = rand::thread_rng();

            println!("Running workload simulation...");
            let start = std::time::Instant::now();

            let value = redlite_bench::benchmark::generate_value();
            let mut read_count = 0u64;
            let mut write_count = 0u64;
            let mut delete_count = 0u64;

            for _ in 0..iterations {
                let key_idx = key_gen.next_key(&mut rng);
                let key = format!("key_{}", key_idx);

                // Track access
                analyzer.record_access(key_idx);

                // Execute operation based on thresholds
                let op_roll: f64 = rng.gen();
                if op_roll < read_threshold {
                    let _ = client.get(&key).await;
                    read_count += 1;
                } else if op_roll < write_threshold {
                    let _ = client.set(&key, &value).await;
                    write_count += 1;
                } else {
                    let _ = client.del(&[key.as_str()]).await;
                    delete_count += 1;
                }
            }

            let elapsed = start.elapsed();
            println!("Completed in {:.2}s ({:.0} ops/sec)\n",
                elapsed.as_secs_f64(),
                iterations as f64 / elapsed.as_secs_f64());
            println!("Operations: {} reads, {} writes, {} deletes\n",
                read_count, write_count, delete_count);

            // Generate recommendation
            let recommendation = analyzer.recommend_cache_size(entry_size_bytes, target_hit_rate);
            recommendation.print_report();
        }
    }

    Ok(())
}

/// Run a scenario for scale testing (no setup/cleanup, just benchmark)
async fn run_scale_scenario(
    client: &RedliteEmbeddedClient,
    scenario: &redlite_bench::scenarios::WorkloadScenario,
    normalized: &[(String, f64)],
    dataset_size: usize,
    iterations: usize,
) -> anyhow::Result<redlite_bench::benchmark::BenchmarkResult> {
    use rand::Rng;
    use std::time::Instant;

    let mut rng = rand::thread_rng();
    let mut result = redlite_bench::benchmark::BenchmarkResult::new(
        &format!("Scale: {}", scenario.name),
        "Redlite",
        dataset_size,
        1,
    );

    // Warmup (1000 iterations)
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

    // Measured run
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

    Ok(result)
}

/// Run a scenario benchmark using the dispatcher
async fn run_scenario_benchmark<C: redlite_bench::client::RedisLikeClient>(
    client: &C,
    scenario: &redlite_bench::scenarios::WorkloadScenario,
    normalized: &[(String, f64)],
    dataset_size: usize,
    iterations: usize,
    backend_name: &str,
    is_json: bool,
    skip_setup: bool,
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

    // Execute setup unless skip_setup is true (for pre-populated databases)
    if !skip_setup {
        if let Some(ref setup) = scenario.setup {
            if !is_json {
                println!("Executing scenario setup...");
            }
            let stats = execute_setup(client, setup).await?;
            if !is_json {
                println!("Setup complete: {} keys in {}ms", stats.total_keys(), stats.total_duration_ms);
                if stats.strings > 0 { println!("  - {} strings", stats.strings); }
                if stats.counters > 0 { println!("  - {} counters", stats.counters); }
                if stats.lists > 0 { println!("  - {} lists", stats.lists); }
                if stats.hashes > 0 { println!("  - {} hashes", stats.hashes); }
                if stats.sets > 0 { println!("  - {} sets", stats.sets); }
                if stats.sorted_sets > 0 { println!("  - {} sorted sets", stats.sorted_sets); }
                if stats.streams > 0 { println!("  - {} streams", stats.streams); }
                println!();
            }
        } else {
            // Fall back to basic string population for scenarios without setup
            let value = redlite_bench::benchmark::generate_value();
            for i in 0..dataset_size {
                client.set(&format!("key_{}", i), &value).await?;
            }
        }
    } else if !is_json {
        println!("Skipping setup (using pre-populated database)...");
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
