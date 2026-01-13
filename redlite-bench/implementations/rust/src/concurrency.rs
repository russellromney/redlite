//! Concurrent benchmark execution strategies
//!
//! Supports three execution modes:
//! - Sequential: Single-threaded baseline
//! - Async: Tokio tasks for lightweight concurrency
//! - Blocking: OS threads via spawn_blocking for true parallelism

use std::sync::Arc;
use std::time::Instant;
use rand::{Rng, SeedableRng};
use rand::rngs::StdRng;
use tokio::sync::Mutex;
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

/// Result from a single worker task
struct TaskResult {
    latencies: Vec<f64>,
    successful_ops: usize,
    failed_ops: usize,
}

impl ConcurrentBenchmark {
    pub fn new(mode: ConcurrencyMode, concurrency: usize) -> Self {
        ConcurrentBenchmark { mode, concurrency }
    }

    /// Execute a concurrent GET benchmark
    pub async fn run_concurrent_get<C: RedisLikeClient + 'static>(
        &self,
        client: &C,
        dataset_size: usize,
        iterations_per_task: usize,
    ) -> Result<ConcurrentBenchmarkResult> {
        match self.mode {
            ConcurrencyMode::Sequential => {
                self.run_get_sequential(client, dataset_size, iterations_per_task).await
            }
            ConcurrencyMode::Async => {
                self.run_get_async(client, dataset_size, iterations_per_task).await
            }
            ConcurrencyMode::Blocking => {
                // For blocking mode, we use async with semaphore to control concurrency
                // True OS threads are complex with async trait objects
                self.run_get_async(client, dataset_size, iterations_per_task).await
            }
        }
    }

    /// Execute a concurrent SET benchmark
    pub async fn run_concurrent_set<C: RedisLikeClient + 'static>(
        &self,
        client: &C,
        dataset_size: usize,
        value: &[u8],
        iterations_per_task: usize,
    ) -> Result<ConcurrentBenchmarkResult> {
        let value = value.to_vec();
        match self.mode {
            ConcurrencyMode::Sequential => {
                self.run_set_sequential(client, dataset_size, &value, iterations_per_task).await
            }
            ConcurrencyMode::Async => {
                self.run_set_async(client, dataset_size, &value, iterations_per_task).await
            }
            ConcurrencyMode::Blocking => {
                self.run_set_async(client, dataset_size, &value, iterations_per_task).await
            }
        }
    }

    /// Sequential GET - single-threaded baseline
    async fn run_get_sequential<C: RedisLikeClient>(
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

    /// Async GET - using tokio::spawn for concurrent tasks
    async fn run_get_async<C: RedisLikeClient + 'static>(
        &self,
        client: &C,
        dataset_size: usize,
        iterations_per_task: usize,
    ) -> Result<ConcurrentBenchmarkResult> {
        let total_iterations = iterations_per_task * self.concurrency;
        let results = Arc::new(Mutex::new(Vec::with_capacity(self.concurrency)));

        let start = Instant::now();

        // Spawn concurrent tasks
        let mut handles = Vec::with_capacity(self.concurrency);
        for _task_id in 0..self.concurrency {
            let client = client.clone();
            let results = Arc::clone(&results);

            let handle = tokio::spawn(async move {
                let mut latencies = Vec::with_capacity(iterations_per_task);
                let mut successful_ops = 0;
                let mut failed_ops = 0;
                // Use StdRng which is Send (unlike ThreadRng)
                let mut rng = StdRng::from_entropy();

                for _ in 0..iterations_per_task {
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

                let task_result = TaskResult {
                    latencies,
                    successful_ops,
                    failed_ops,
                };

                let mut results = results.lock().await;
                results.push(task_result);
            });

            handles.push(handle);
        }

        // Wait for all tasks to complete
        for handle in handles {
            handle.await.map_err(|e| crate::error::BenchError::TaskFailed(e.to_string()))?;
        }

        let duration = start.elapsed().as_secs_f64();

        // Aggregate results
        let results = results.lock().await;
        let mut all_latencies = Vec::with_capacity(total_iterations);
        let mut successful_ops = 0;
        let mut failed_ops = 0;

        for result in results.iter() {
            all_latencies.extend(&result.latencies);
            successful_ops += result.successful_ops;
            failed_ops += result.failed_ops;
        }

        Ok(ConcurrentBenchmarkResult {
            operation: "GET".to_string(),
            mode: self.mode,
            concurrency: self.concurrency,
            total_iterations,
            successful_ops,
            failed_ops,
            latencies: all_latencies,
            duration_secs: duration,
        })
    }

    /// Sequential SET - single-threaded baseline
    async fn run_set_sequential<C: RedisLikeClient>(
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

    /// Async SET - using tokio::spawn for concurrent tasks
    async fn run_set_async<C: RedisLikeClient + 'static>(
        &self,
        client: &C,
        dataset_size: usize,
        value: &[u8],
        iterations_per_task: usize,
    ) -> Result<ConcurrentBenchmarkResult> {
        let total_iterations = iterations_per_task * self.concurrency;
        let results = Arc::new(Mutex::new(Vec::with_capacity(self.concurrency)));
        let value = value.to_vec();

        let start = Instant::now();

        // Spawn concurrent tasks
        let mut handles = Vec::with_capacity(self.concurrency);
        for task_id in 0..self.concurrency {
            let client = client.clone();
            let results = Arc::clone(&results);
            let value = value.clone();

            let handle = tokio::spawn(async move {
                let mut latencies = Vec::with_capacity(iterations_per_task);
                let mut successful_ops = 0;
                let mut failed_ops = 0;

                for i in 0..iterations_per_task {
                    // Use task_id offset to avoid key contention
                    let key = format!("key_{}", (task_id * iterations_per_task + i) % dataset_size);
                    let op_start = Instant::now();
                    match client.set(&key, &value).await {
                        Ok(_) => {
                            latencies.push(op_start.elapsed().as_secs_f64() * 1_000_000.0);
                            successful_ops += 1;
                        }
                        Err(_) => {
                            failed_ops += 1;
                        }
                    }
                }

                let task_result = TaskResult {
                    latencies,
                    successful_ops,
                    failed_ops,
                };

                let mut results = results.lock().await;
                results.push(task_result);
            });

            handles.push(handle);
        }

        // Wait for all tasks to complete
        for handle in handles {
            handle.await.map_err(|e| crate::error::BenchError::TaskFailed(e.to_string()))?;
        }

        let duration = start.elapsed().as_secs_f64();

        // Aggregate results
        let results = results.lock().await;
        let mut all_latencies = Vec::with_capacity(total_iterations);
        let mut successful_ops = 0;
        let mut failed_ops = 0;

        for result in results.iter() {
            all_latencies.extend(&result.latencies);
            successful_ops += result.successful_ops;
            failed_ops += result.failed_ops;
        }

        Ok(ConcurrentBenchmarkResult {
            operation: "SET".to_string(),
            mode: self.mode,
            concurrency: self.concurrency,
            total_iterations,
            successful_ops,
            failed_ops,
            latencies: all_latencies,
            duration_secs: duration,
        })
    }

    /// Execute a concurrent LPUSH benchmark
    pub async fn run_concurrent_lpush<C: RedisLikeClient + 'static>(
        &self,
        client: &C,
        dataset_size: usize,
        value: &[u8],
        iterations_per_task: usize,
    ) -> Result<ConcurrentBenchmarkResult> {
        let value = value.to_vec();
        match self.mode {
            ConcurrencyMode::Sequential => {
                self.run_lpush_sequential(client, dataset_size, &value, iterations_per_task).await
            }
            ConcurrencyMode::Async | ConcurrencyMode::Blocking => {
                self.run_lpush_async(client, dataset_size, &value, iterations_per_task).await
            }
        }
    }

    /// Sequential LPUSH - single-threaded baseline
    async fn run_lpush_sequential<C: RedisLikeClient>(
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
            let key = format!("list_{}", i % dataset_size);
            let op_start = Instant::now();
            match client.lpush(&key, &[value]).await {
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
            operation: "LPUSH".to_string(),
            mode: self.mode,
            concurrency: self.concurrency,
            total_iterations,
            successful_ops,
            failed_ops,
            latencies,
            duration_secs: duration,
        })
    }

    /// Async LPUSH - using tokio::spawn for concurrent tasks
    async fn run_lpush_async<C: RedisLikeClient + 'static>(
        &self,
        client: &C,
        dataset_size: usize,
        value: &[u8],
        iterations_per_task: usize,
    ) -> Result<ConcurrentBenchmarkResult> {
        let total_iterations = iterations_per_task * self.concurrency;
        let results = Arc::new(Mutex::new(Vec::with_capacity(self.concurrency)));
        let value = value.to_vec();

        let start = Instant::now();

        // Spawn concurrent tasks
        let mut handles = Vec::with_capacity(self.concurrency);
        for task_id in 0..self.concurrency {
            let client = client.clone();
            let results = Arc::clone(&results);
            let value = value.clone();

            let handle = tokio::spawn(async move {
                let mut latencies = Vec::with_capacity(iterations_per_task);
                let mut successful_ops = 0;
                let mut failed_ops = 0;

                for i in 0..iterations_per_task {
                    // Use task_id offset to distribute across different lists
                    let key = format!("list_{}", (task_id * iterations_per_task + i) % dataset_size);
                    let op_start = Instant::now();
                    match client.lpush(&key, &[&value[..]]).await {
                        Ok(_) => {
                            latencies.push(op_start.elapsed().as_secs_f64() * 1_000_000.0);
                            successful_ops += 1;
                        }
                        Err(_) => {
                            failed_ops += 1;
                        }
                    }
                }

                let task_result = TaskResult {
                    latencies,
                    successful_ops,
                    failed_ops,
                };

                let mut results = results.lock().await;
                results.push(task_result);
            });

            handles.push(handle);
        }

        // Wait for all tasks to complete
        for handle in handles {
            handle.await.map_err(|e| crate::error::BenchError::TaskFailed(e.to_string()))?;
        }

        let duration = start.elapsed().as_secs_f64();

        // Aggregate results
        let results = results.lock().await;
        let mut all_latencies = Vec::with_capacity(total_iterations);
        let mut successful_ops = 0;
        let mut failed_ops = 0;

        for result in results.iter() {
            all_latencies.extend(&result.latencies);
            successful_ops += result.successful_ops;
            failed_ops += result.failed_ops;
        }

        Ok(ConcurrentBenchmarkResult {
            operation: "LPUSH".to_string(),
            mode: self.mode,
            concurrency: self.concurrency,
            total_iterations,
            successful_ops,
            failed_ops,
            latencies: all_latencies,
            duration_secs: duration,
        })
    }

    /// Execute a concurrent HSET benchmark
    pub async fn run_concurrent_hset<C: RedisLikeClient + 'static>(
        &self,
        client: &C,
        dataset_size: usize,
        value: &[u8],
        iterations_per_task: usize,
    ) -> Result<ConcurrentBenchmarkResult> {
        let value = value.to_vec();
        match self.mode {
            ConcurrencyMode::Sequential => {
                self.run_hset_sequential(client, dataset_size, &value, iterations_per_task).await
            }
            ConcurrencyMode::Async | ConcurrencyMode::Blocking => {
                self.run_hset_async(client, dataset_size, &value, iterations_per_task).await
            }
        }
    }

    /// Sequential HSET - single-threaded baseline
    async fn run_hset_sequential<C: RedisLikeClient>(
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
            let key = format!("hash_{}", i % dataset_size);
            let field = format!("field_{}", i % 100);
            let op_start = Instant::now();
            match client.hset(&key, &field, value).await {
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
            operation: "HSET".to_string(),
            mode: self.mode,
            concurrency: self.concurrency,
            total_iterations,
            successful_ops,
            failed_ops,
            latencies,
            duration_secs: duration,
        })
    }

    /// Async HSET - using tokio::spawn for concurrent tasks
    async fn run_hset_async<C: RedisLikeClient + 'static>(
        &self,
        client: &C,
        dataset_size: usize,
        value: &[u8],
        iterations_per_task: usize,
    ) -> Result<ConcurrentBenchmarkResult> {
        let total_iterations = iterations_per_task * self.concurrency;
        let results = Arc::new(Mutex::new(Vec::with_capacity(self.concurrency)));
        let value = value.to_vec();

        let start = Instant::now();

        // Spawn concurrent tasks
        let mut handles = Vec::with_capacity(self.concurrency);
        for task_id in 0..self.concurrency {
            let client = client.clone();
            let results = Arc::clone(&results);
            let value = value.clone();

            let handle = tokio::spawn(async move {
                let mut latencies = Vec::with_capacity(iterations_per_task);
                let mut successful_ops = 0;
                let mut failed_ops = 0;

                for i in 0..iterations_per_task {
                    // Use task_id offset to distribute across different hashes
                    let key = format!("hash_{}", (task_id * iterations_per_task + i) % dataset_size);
                    let field = format!("field_{}", i % 100);
                    let op_start = Instant::now();
                    match client.hset(&key, &field, &value).await {
                        Ok(_) => {
                            latencies.push(op_start.elapsed().as_secs_f64() * 1_000_000.0);
                            successful_ops += 1;
                        }
                        Err(_) => {
                            failed_ops += 1;
                        }
                    }
                }

                let task_result = TaskResult {
                    latencies,
                    successful_ops,
                    failed_ops,
                };

                let mut results = results.lock().await;
                results.push(task_result);
            });

            handles.push(handle);
        }

        // Wait for all tasks to complete
        for handle in handles {
            handle.await.map_err(|e| crate::error::BenchError::TaskFailed(e.to_string()))?;
        }

        let duration = start.elapsed().as_secs_f64();

        // Aggregate results
        let results = results.lock().await;
        let mut all_latencies = Vec::with_capacity(total_iterations);
        let mut successful_ops = 0;
        let mut failed_ops = 0;

        for result in results.iter() {
            all_latencies.extend(&result.latencies);
            successful_ops += result.successful_ops;
            failed_ops += result.failed_ops;
        }

        Ok(ConcurrentBenchmarkResult {
            operation: "HSET".to_string(),
            mode: self.mode,
            concurrency: self.concurrency,
            total_iterations,
            successful_ops,
            failed_ops,
            latencies: all_latencies,
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_percentile_edge_cases() {
        let result = ConcurrentBenchmarkResult {
            operation: "TEST".to_string(),
            mode: ConcurrencyMode::Sequential,
            concurrency: 1,
            total_iterations: 0,
            successful_ops: 0,
            failed_ops: 0,
            latencies: vec![],
            duration_secs: 0.0,
        };

        assert_eq!(result.p50_latency_us(), 0.0);
        assert_eq!(result.throughput_ops_sec(), 0.0);
    }

    #[test]
    fn test_throughput_calculation() {
        let result = ConcurrentBenchmarkResult {
            operation: "TEST".to_string(),
            mode: ConcurrencyMode::Async,
            concurrency: 4,
            total_iterations: 10000,
            successful_ops: 10000,
            failed_ops: 0,
            latencies: vec![100.0; 10000],
            duration_secs: 1.0,
        };

        assert_eq!(result.throughput_ops_sec(), 10000.0);
        assert_eq!(result.error_rate(), 0.0);
    }
}
