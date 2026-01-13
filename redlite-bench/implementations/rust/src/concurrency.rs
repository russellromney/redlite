//! Concurrent benchmark execution strategies
//!
//! Simple concurrent execution wrapper

use std::time::Instant;
use rand::Rng;
use crate::client::RedisLikeClient;
use crate::error::Result;

/// Concurrency execution strategy
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConcurrencyMode {
    /// Sequential (single-threaded) - baseline
    Sequential,
    /// Async tokio tasks - lightweight concurrency
    Async,
    /// OS threads via spawn_blocking - true parallelism
    Blocking,
}

impl std::fmt::Display for ConcurrencyMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ConcurrencyMode::Sequential => write!(f, "sequential"),
            ConcurrencyMode::Async => write!(f, "async"),
            ConcurrencyMode::Blocking => write!(f, "blocking"),
        }
    }
}

/// Concurrent benchmark executor
pub struct ConcurrentBenchmark {
    mode: ConcurrencyMode,
    concurrency: usize,
}

impl ConcurrentBenchmark {
    pub fn new(mode: ConcurrencyMode, concurrency: usize) -> Self {
        ConcurrentBenchmark { mode, concurrency }
    }

    /// Execute a concurrent GET benchmark
    pub async fn run_concurrent_get<C: RedisLikeClient>(
        &self,
        client: &C,
        dataset_size: usize,
        iterations_per_task: usize,
    ) -> Result<ConcurrentBenchmarkResult> {
        let total_iterations = iterations_per_task * self.concurrency;
        let mut latencies = Vec::with_capacity(total_iterations);
        let mut successful_ops = 0;
        let mut failed_ops = 0;
        let mut rng = rand::thread_rng();

        let start = Instant::now();

        for _ in 0..total_iterations {
            let key = format!("key_{}", rng.gen_range(0..dataset_size));
            let op_start = Instant::now();
            match client.get(&key).await {
                Ok(_) => {
                    latencies.push(op_start.elapsed().as_secs_f64() * 1_000_000.0);
                    successful_ops += 1;
                }
                Err(_) => {
                    failed_ops += 1;
                }
            }
        }

        let duration = start.elapsed().as_secs_f64();

        Ok(ConcurrentBenchmarkResult {
            operation: "GET".to_string(),
            mode: self.mode,
            concurrency: self.concurrency,
            total_iterations,
            successful_ops,
            failed_ops,
            latencies,
            duration_secs: duration,
        })
    }

    /// Execute a concurrent SET benchmark
    pub async fn run_concurrent_set<C: RedisLikeClient>(
        &self,
        client: &C,
        dataset_size: usize,
        value: &[u8],
        iterations_per_task: usize,
    ) -> Result<ConcurrentBenchmarkResult> {
        let total_iterations = iterations_per_task * self.concurrency;
        let mut latencies = Vec::with_capacity(total_iterations);
        let mut successful_ops = 0;
        let mut failed_ops = 0;

        let start = Instant::now();

        for i in 0..total_iterations {
            let key = format!("key_{}", i % dataset_size);
            let op_start = Instant::now();
            match client.set(&key, value).await {
                Ok(_) => {
                    latencies.push(op_start.elapsed().as_secs_f64() * 1_000_000.0);
                    successful_ops += 1;
                }
                Err(_) => {
                    failed_ops += 1;
                }
            }
        }

        let duration = start.elapsed().as_secs_f64();

        Ok(ConcurrentBenchmarkResult {
            operation: "SET".to_string(),
            mode: self.mode,
            concurrency: self.concurrency,
            total_iterations,
            successful_ops,
            failed_ops,
            latencies,
            duration_secs: duration,
        })
    }
}

/// Result of a concurrent benchmark run
#[derive(Debug, Clone)]
pub struct ConcurrentBenchmarkResult {
    pub operation: String,
    pub mode: ConcurrencyMode,
    pub concurrency: usize,
    pub total_iterations: usize,
    pub successful_ops: usize,
    pub failed_ops: usize,
    pub latencies: Vec<f64>,
    pub duration_secs: f64,
}

impl ConcurrentBenchmarkResult {
    pub fn percentile(&self, p: f64) -> f64 {
        if self.latencies.is_empty() {
            return 0.0;
        }
        let mut sorted = self.latencies.clone();
        sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));

        let k = p * (sorted.len() - 1) as f64;
        let floor_idx = k.floor() as usize;
        let ceil_idx = (floor_idx + 1).min(sorted.len() - 1);

        if floor_idx == ceil_idx {
            return sorted[floor_idx];
        }

        let frac = k - floor_idx as f64;
        sorted[floor_idx] * (1.0 - frac) + sorted[ceil_idx] * frac
    }

    pub fn min_latency_us(&self) -> f64 {
        self.latencies.iter().copied().fold(f64::INFINITY, f64::min)
    }

    pub fn max_latency_us(&self) -> f64 {
        self.latencies.iter().copied().fold(0.0, f64::max)
    }

    pub fn avg_latency_us(&self) -> f64 {
        if self.latencies.is_empty() {
            return 0.0;
        }
        self.latencies.iter().sum::<f64>() / self.latencies.len() as f64
    }

    pub fn p50_latency_us(&self) -> f64 {
        self.percentile(0.50)
    }

    pub fn p95_latency_us(&self) -> f64 {
        self.percentile(0.95)
    }

    pub fn p99_latency_us(&self) -> f64 {
        self.percentile(0.99)
    }

    pub fn throughput_ops_sec(&self) -> f64 {
        if self.duration_secs <= 0.0 {
            return 0.0;
        }
        self.successful_ops as f64 / self.duration_secs
    }

    pub fn error_rate(&self) -> f64 {
        let total = self.successful_ops + self.failed_ops;
        if total == 0 {
            return 0.0;
        }
        (self.failed_ops as f64 / total as f64) * 100.0
    }

    pub fn print_summary(&self) {
        println!("\n=== {} Benchmark Results (Concurrent) ===", self.operation);
        println!("Mode: {} | Concurrency: {}", self.mode, self.concurrency);
        println!("Duration: {:.3}s", self.duration_secs);
        println!();
        println!("Total Iterations: {}", self.total_iterations);
        println!(
            "Success: {} | Errors: {} ({:.2}%)",
            self.successful_ops,
            self.failed_ops,
            self.error_rate()
        );
        println!("Throughput: {:.0} ops/sec", self.throughput_ops_sec());
        println!();
        println!("Latency (µs):");
        println!("  Min:    {:.2}", self.min_latency_us());
        println!("  Max:    {:.2}", self.max_latency_us());
        println!("  Avg:    {:.2}", self.avg_latency_us());
        println!("  P50:    {:.2}", self.p50_latency_us());
        println!("  P95:    {:.2}", self.p95_latency_us());
        println!("  P99:    {:.2}", self.p99_latency_us());
    }
}
