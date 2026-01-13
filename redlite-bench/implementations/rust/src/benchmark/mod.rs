//! Benchmark infrastructure for redlite-bench
//!
//! This module provides:
//! - BenchmarkResult: Per-operation measurement results
//! - BenchmarkRunner: Execute benchmarks against any RedisLikeClient
//! - Statistics: Latency percentiles, throughput calculations

use std::time::Instant;
use rand::prelude::*;
use serde::{Deserialize, Serialize};

use crate::client::RedisLikeClient;
use crate::concurrency::{ConcurrencyMode, ConcurrentBenchmark, ConcurrentBenchmarkResult};
use crate::error::Result;

/// Result of a single benchmark run
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BenchmarkResult {
    pub operation: String,
    pub backend: String,
    pub dataset_size: usize,
    pub concurrency: usize,
    pub iterations: usize,
    pub successful_ops: usize,
    pub failed_ops: usize,
    pub latencies_us: Vec<f64>,
    pub duration_secs: f64,
}

impl BenchmarkResult {
    pub fn new(operation: &str, backend: &str, dataset_size: usize, concurrency: usize) -> Self {
        BenchmarkResult {
            operation: operation.to_string(),
            backend: backend.to_string(),
            dataset_size,
            concurrency,
            iterations: 0,
            successful_ops: 0,
            failed_ops: 0,
            latencies_us: Vec::new(),
            duration_secs: 0.0,
        }
    }

    /// Minimum latency in microseconds
    pub fn min_latency_us(&self) -> f64 {
        self.latencies_us
            .iter()
            .copied()
            .fold(f64::INFINITY, f64::min)
    }

    /// Maximum latency in microseconds
    pub fn max_latency_us(&self) -> f64 {
        self.latencies_us.iter().copied().fold(0.0, f64::max)
    }

    /// Average latency in microseconds
    pub fn avg_latency_us(&self) -> f64 {
        if self.latencies_us.is_empty() {
            return 0.0;
        }
        self.latencies_us.iter().sum::<f64>() / self.latencies_us.len() as f64
    }

    /// Standard deviation of latencies
    pub fn stddev_latency_us(&self) -> f64 {
        if self.latencies_us.len() < 2 {
            return 0.0;
        }
        let avg = self.avg_latency_us();
        let variance = self.latencies_us.iter().map(|x| (x - avg).powi(2)).sum::<f64>()
            / (self.latencies_us.len() - 1) as f64;
        variance.sqrt()
    }

    /// Calculate percentile (0.0 - 1.0)
    fn percentile(&self, p: f64) -> f64 {
        if self.latencies_us.is_empty() {
            return 0.0;
        }
        let mut sorted = self.latencies_us.clone();
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

    /// 50th percentile (median)
    pub fn p50_latency_us(&self) -> f64 {
        self.percentile(0.50)
    }

    /// 95th percentile
    pub fn p95_latency_us(&self) -> f64 {
        self.percentile(0.95)
    }

    /// 99th percentile
    pub fn p99_latency_us(&self) -> f64 {
        self.percentile(0.99)
    }

    /// Error rate as percentage
    pub fn error_rate(&self) -> f64 {
        let total = self.successful_ops + self.failed_ops;
        if total == 0 {
            return 0.0;
        }
        (self.failed_ops as f64 / total as f64) * 100.0
    }

    /// Throughput in operations per second
    pub fn throughput_ops_sec(&self) -> f64 {
        if self.duration_secs <= 0.0 {
            return 0.0;
        }
        self.successful_ops as f64 / self.duration_secs
    }

    /// Print results to console
    pub fn print_summary(&self) {
        println!("\n=== {} Benchmark Results ===", self.operation);
        println!("Backend: {}", self.backend);
        println!("Dataset Size: {}", self.dataset_size);
        println!("Concurrency: {}", self.concurrency);
        println!("Duration: {:.3}s", self.duration_secs);
        println!();
        println!("Iterations: {}", self.iterations);
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
        println!("  Stddev: {:.2}", self.stddev_latency_us());
        println!("  P50:    {:.2}", self.p50_latency_us());
        println!("  P95:    {:.2}", self.p95_latency_us());
        println!("  P99:    {:.2}", self.p99_latency_us());
    }
}

/// Configuration for a benchmark run
#[derive(Debug, Clone)]
pub struct BenchmarkConfig {
    pub backend_name: String,
    pub dataset_size: usize,
    pub iterations: usize,
    pub warmup_iterations: usize,
    pub concurrency: usize,
}

impl Default for BenchmarkConfig {
    fn default() -> Self {
        BenchmarkConfig {
            backend_name: "unknown".to_string(),
            dataset_size: 10_000,
            iterations: 100_000,
            warmup_iterations: 1_000,
            concurrency: 1,
        }
    }
}

/// Generate a 100-byte test value
pub fn generate_value() -> Vec<u8> {
    let mut value = Vec::with_capacity(100);
    value.extend_from_slice(b"value_");
    value.extend(std::iter::repeat(b'x').take(94));
    value
}

/// Benchmark runner for individual operations
pub struct BenchmarkRunner<C: RedisLikeClient> {
    client: C,
    config: BenchmarkConfig,
}

impl<C: RedisLikeClient + 'static> BenchmarkRunner<C> {
    pub fn new(client: C, config: BenchmarkConfig) -> Self {
        BenchmarkRunner { client, config }
    }

    /// Run GET benchmark
    pub async fn bench_get(&self) -> Result<BenchmarkResult> {
        let mut result = BenchmarkResult::new(
            "GET",
            &self.config.backend_name,
            self.config.dataset_size,
            self.config.concurrency,
        );

        // Setup: Populate keys
        let value = generate_value();
        for i in 0..self.config.dataset_size {
            self.client.set(&format!("key_{}", i), &value).await?;
        }

        // Warmup
        let mut rng = thread_rng();
        for _ in 0..self.config.warmup_iterations {
            let key = format!("key_{}", rng.gen_range(0..self.config.dataset_size));
            let _ = self.client.get(&key).await;
        }

        // Measure
        let mut latencies = Vec::with_capacity(self.config.iterations);
        let start = Instant::now();

        for _ in 0..self.config.iterations {
            let key = format!("key_{}", rng.gen_range(0..self.config.dataset_size));
            let op_start = Instant::now();
            match self.client.get(&key).await {
                Ok(_) => {
                    let latency_us = op_start.elapsed().as_secs_f64() * 1_000_000.0;
                    latencies.push(latency_us);
                    result.successful_ops += 1;
                }
                Err(_) => {
                    result.failed_ops += 1;
                }
            }
        }

        result.duration_secs = start.elapsed().as_secs_f64();
        result.latencies_us = latencies;
        result.iterations = self.config.iterations;

        Ok(result)
    }

    /// Run SET benchmark
    pub async fn bench_set(&self) -> Result<BenchmarkResult> {
        let mut result = BenchmarkResult::new(
            "SET",
            &self.config.backend_name,
            self.config.dataset_size,
            self.config.concurrency,
        );

        let value = generate_value();

        // Warmup
        let mut rng = thread_rng();
        for _ in 0..self.config.warmup_iterations {
            let key = format!("warmup_key_{}", rng.gen_range(0..1000));
            let _ = self.client.set(&key, &value).await;
        }

        // Clear warmup data
        self.client.flushdb().await?;

        // Measure
        let mut latencies = Vec::with_capacity(self.config.iterations);
        let start = Instant::now();

        for i in 0..self.config.iterations {
            let key = format!("key_{}", i % self.config.dataset_size);
            let op_start = Instant::now();
            match self.client.set(&key, &value).await {
                Ok(_) => {
                    let latency_us = op_start.elapsed().as_secs_f64() * 1_000_000.0;
                    latencies.push(latency_us);
                    result.successful_ops += 1;
                }
                Err(_) => {
                    result.failed_ops += 1;
                }
            }
        }

        result.duration_secs = start.elapsed().as_secs_f64();
        result.latencies_us = latencies;
        result.iterations = self.config.iterations;

        Ok(result)
    }

    /// Run LPUSH benchmark
    pub async fn bench_lpush(&self) -> Result<BenchmarkResult> {
        let mut result = BenchmarkResult::new(
            "LPUSH",
            &self.config.backend_name,
            self.config.dataset_size,
            self.config.concurrency,
        );

        let value = generate_value();

        // Warmup
        let mut rng = thread_rng();
        for _ in 0..self.config.warmup_iterations {
            let key = format!("warmup_list_{}", rng.gen_range(0..100));
            let _ = self.client.lpush(&key, &[&value[..]]).await;
        }

        self.client.flushdb().await?;

        // Measure
        let mut latencies = Vec::with_capacity(self.config.iterations);
        let start = Instant::now();

        for i in 0..self.config.iterations {
            let key = format!("list_{}", i % self.config.dataset_size);
            let op_start = Instant::now();
            match self.client.lpush(&key, &[&value[..]]).await {
                Ok(_) => {
                    let latency_us = op_start.elapsed().as_secs_f64() * 1_000_000.0;
                    latencies.push(latency_us);
                    result.successful_ops += 1;
                }
                Err(_) => {
                    result.failed_ops += 1;
                }
            }
        }

        result.duration_secs = start.elapsed().as_secs_f64();
        result.latencies_us = latencies;
        result.iterations = self.config.iterations;

        Ok(result)
    }

    /// Run HSET benchmark
    pub async fn bench_hset(&self) -> Result<BenchmarkResult> {
        let mut result = BenchmarkResult::new(
            "HSET",
            &self.config.backend_name,
            self.config.dataset_size,
            self.config.concurrency,
        );

        let value = generate_value();

        // Warmup
        let mut rng = thread_rng();
        for _ in 0..self.config.warmup_iterations {
            let key = format!("warmup_hash_{}", rng.gen_range(0..100));
            let field = format!("field_{}", rng.gen_range(0..100));
            let _ = self.client.hset(&key, &field, &value).await;
        }

        self.client.flushdb().await?;

        // Measure
        let mut latencies = Vec::with_capacity(self.config.iterations);
        let start = Instant::now();

        for i in 0..self.config.iterations {
            let key = format!("hash_{}", i % self.config.dataset_size);
            let field = format!("field_{}", i % 100);
            let op_start = Instant::now();
            match self.client.hset(&key, &field, &value).await {
                Ok(_) => {
                    let latency_us = op_start.elapsed().as_secs_f64() * 1_000_000.0;
                    latencies.push(latency_us);
                    result.successful_ops += 1;
                }
                Err(_) => {
                    result.failed_ops += 1;
                }
            }
        }

        result.duration_secs = start.elapsed().as_secs_f64();
        result.latencies_us = latencies;
        result.iterations = self.config.iterations;

        Ok(result)
    }

    /// Run SADD benchmark
    pub async fn bench_sadd(&self) -> Result<BenchmarkResult> {
        let mut result = BenchmarkResult::new(
            "SADD",
            &self.config.backend_name,
            self.config.dataset_size,
            self.config.concurrency,
        );

        // Warmup
        let mut rng = thread_rng();
        for _ in 0..self.config.warmup_iterations {
            let key = format!("warmup_set_{}", rng.gen_range(0..100));
            let member = format!("member_{}", rng.gen_range(0..1000));
            let _ = self.client.sadd(&key, &[member.as_bytes()]).await;
        }

        self.client.flushdb().await?;

        // Measure
        let mut latencies = Vec::with_capacity(self.config.iterations);
        let start = Instant::now();

        for i in 0..self.config.iterations {
            let key = format!("set_{}", i % self.config.dataset_size);
            let member = format!("member_{}", i);
            let op_start = Instant::now();
            match self.client.sadd(&key, &[member.as_bytes()]).await {
                Ok(_) => {
                    let latency_us = op_start.elapsed().as_secs_f64() * 1_000_000.0;
                    latencies.push(latency_us);
                    result.successful_ops += 1;
                }
                Err(_) => {
                    result.failed_ops += 1;
                }
            }
        }

        result.duration_secs = start.elapsed().as_secs_f64();
        result.latencies_us = latencies;
        result.iterations = self.config.iterations;

        Ok(result)
    }

    /// Run ZADD benchmark
    pub async fn bench_zadd(&self) -> Result<BenchmarkResult> {
        let mut result = BenchmarkResult::new(
            "ZADD",
            &self.config.backend_name,
            self.config.dataset_size,
            self.config.concurrency,
        );

        // Warmup
        let mut rng = thread_rng();
        for _ in 0..self.config.warmup_iterations {
            let key = format!("warmup_zset_{}", rng.gen_range(0..100));
            let member = format!("member_{}", rng.gen_range(0..1000));
            let score: f64 = rng.gen_range(0.0..1000.0);
            let _ = self.client.zadd(&key, &[(score, member.as_bytes())]).await;
        }

        self.client.flushdb().await?;

        // Measure
        let mut latencies = Vec::with_capacity(self.config.iterations);
        let start = Instant::now();

        for i in 0..self.config.iterations {
            let key = format!("zset_{}", i % self.config.dataset_size);
            let member = format!("member_{}", i);
            let score: f64 = rng.gen_range(0.0..1000.0);
            let op_start = Instant::now();
            match self.client.zadd(&key, &[(score, member.as_bytes())]).await {
                Ok(_) => {
                    let latency_us = op_start.elapsed().as_secs_f64() * 1_000_000.0;
                    latencies.push(latency_us);
                    result.successful_ops += 1;
                }
                Err(_) => {
                    result.failed_ops += 1;
                }
            }
        }

        result.duration_secs = start.elapsed().as_secs_f64();
        result.latencies_us = latencies;
        result.iterations = self.config.iterations;

        Ok(result)
    }

    /// Run XADD benchmark (Streams)
    pub async fn bench_xadd(&self) -> Result<BenchmarkResult> {
        let mut result = BenchmarkResult::new(
            "XADD",
            &self.config.backend_name,
            self.config.dataset_size,
            self.config.concurrency,
        );

        let value = generate_value();

        // Warmup
        let mut rng = thread_rng();
        for _ in 0..self.config.warmup_iterations {
            let key = format!("warmup_stream_{}", rng.gen_range(0..100));
            let _ = self.client.xadd(&key, "*", &[("data", &value[..])]).await;
        }

        self.client.flushdb().await?;

        // Measure
        let mut latencies = Vec::with_capacity(self.config.iterations);
        let start = Instant::now();

        for i in 0..self.config.iterations {
            let key = format!("stream_{}", i % self.config.dataset_size);
            let op_start = Instant::now();
            match self.client.xadd(&key, "*", &[("data", &value[..])]).await {
                Ok(_) => {
                    let latency_us = op_start.elapsed().as_secs_f64() * 1_000_000.0;
                    latencies.push(latency_us);
                    result.successful_ops += 1;
                }
                Err(_) => {
                    result.failed_ops += 1;
                }
            }
        }

        result.duration_secs = start.elapsed().as_secs_f64();
        result.latencies_us = latencies;
        result.iterations = self.config.iterations;

        Ok(result)
    }

    /// Run INCR benchmark
    pub async fn bench_incr(&self) -> Result<BenchmarkResult> {
        let mut result = BenchmarkResult::new(
            "INCR",
            &self.config.backend_name,
            self.config.dataset_size,
            self.config.concurrency,
        );

        // Setup: Initialize counters
        for i in 0..self.config.dataset_size {
            self.client.set(&format!("counter_{}", i), b"0").await?;
        }

        // Warmup
        let mut rng = thread_rng();
        for _ in 0..self.config.warmup_iterations {
            let key = format!("counter_{}", rng.gen_range(0..self.config.dataset_size));
            let _ = self.client.incr(&key).await;
        }

        // Measure
        let mut latencies = Vec::with_capacity(self.config.iterations);
        let start = Instant::now();

        for _ in 0..self.config.iterations {
            let key = format!("counter_{}", rng.gen_range(0..self.config.dataset_size));
            let op_start = Instant::now();
            match self.client.incr(&key).await {
                Ok(_) => {
                    let latency_us = op_start.elapsed().as_secs_f64() * 1_000_000.0;
                    latencies.push(latency_us);
                    result.successful_ops += 1;
                }
                Err(_) => {
                    result.failed_ops += 1;
                }
            }
        }

        result.duration_secs = start.elapsed().as_secs_f64();
        result.latencies_us = latencies;
        result.iterations = self.config.iterations;

        Ok(result)
    }

    /// Run concurrent GET benchmark
    pub async fn bench_get_concurrent(
        &self,
        mode: ConcurrencyMode,
    ) -> Result<ConcurrentBenchmarkResult> {
        // Setup: Populate keys
        let value = generate_value();
        for i in 0..self.config.dataset_size {
            self.client.set(&format!("key_{}", i), &value).await?;
        }

        // Calculate iterations per task
        let total_iterations = self.config.iterations;
        let iterations_per_task = (total_iterations + self.config.concurrency - 1) / self.config.concurrency;

        // Create concurrent benchmark executor
        let executor = ConcurrentBenchmark::new(mode, self.config.concurrency);
        let result = executor
            .run_concurrent_get(&self.client, self.config.dataset_size, iterations_per_task)
            .await?;

        Ok(result)
    }

    /// Run concurrent SET benchmark
    pub async fn bench_set_concurrent(
        &self,
        mode: ConcurrencyMode,
    ) -> Result<ConcurrentBenchmarkResult> {
        let value = generate_value();

        // Calculate iterations per task
        let total_iterations = self.config.iterations;
        let iterations_per_task = (total_iterations + self.config.concurrency - 1) / self.config.concurrency;

        // Create concurrent benchmark executor
        let executor = ConcurrentBenchmark::new(mode, self.config.concurrency);
        let result = executor
            .run_concurrent_set(&self.client, self.config.dataset_size, &value, iterations_per_task)
            .await?;

        Ok(result)
    }

    /// Run concurrent LPUSH benchmark
    pub async fn bench_lpush_concurrent(
        &self,
        mode: ConcurrencyMode,
    ) -> Result<ConcurrentBenchmarkResult> {
        let value = generate_value();

        // Calculate iterations per task
        let total_iterations = self.config.iterations;
        let iterations_per_task = (total_iterations + self.config.concurrency - 1) / self.config.concurrency;

        // Create concurrent benchmark executor
        let executor = ConcurrentBenchmark::new(mode, self.config.concurrency);
        let result = executor
            .run_concurrent_lpush(&self.client, self.config.dataset_size, &value, iterations_per_task)
            .await?;

        Ok(result)
    }

    /// Run concurrent HSET benchmark
    pub async fn bench_hset_concurrent(
        &self,
        mode: ConcurrencyMode,
    ) -> Result<ConcurrentBenchmarkResult> {
        let value = generate_value();

        // Calculate iterations per task
        let total_iterations = self.config.iterations;
        let iterations_per_task = (total_iterations + self.config.concurrency - 1) / self.config.concurrency;

        // Create concurrent benchmark executor
        let executor = ConcurrentBenchmark::new(mode, self.config.concurrency);
        let result = executor
            .run_concurrent_hset(&self.client, self.config.dataset_size, &value, iterations_per_task)
            .await?;

        Ok(result)
    }

    // ========== REDLITE-SPECIFIC BENCHMARKS ==========

    /// Benchmark HISTORY ENABLE command
    pub async fn bench_history_enable(&self) -> Result<BenchmarkResult> {
        let mut result = BenchmarkResult::new(
            "HISTORY ENABLE",
            &self.config.backend_name,
            self.config.dataset_size,
            self.config.concurrency,
        );

        // Setup: Populate keys first
        let value = generate_value();
        for i in 0..self.config.dataset_size {
            self.client.set(&format!("key_{}", i), &value).await?;
        }

        // Measure
        let mut latencies = Vec::with_capacity(self.config.iterations);
        let start = Instant::now();

        for i in 0..self.config.iterations {
            let key = format!("key_{}", i % self.config.dataset_size);
            let op_start = Instant::now();
            match self.client.history_enable(&key).await {
                Ok(_) => {
                    latencies.push(op_start.elapsed().as_secs_f64() * 1_000_000.0);
                    result.successful_ops += 1;
                }
                Err(_) => {
                    result.failed_ops += 1;
                }
            }
        }

        result.duration_secs = start.elapsed().as_secs_f64();
        result.latencies_us = latencies;
        result.iterations = self.config.iterations;

        Ok(result)
    }

    /// Benchmark HISTORY GET command
    pub async fn bench_history_get(&self) -> Result<BenchmarkResult> {
        let mut result = BenchmarkResult::new(
            "HISTORY GET",
            &self.config.backend_name,
            self.config.dataset_size,
            self.config.concurrency,
        );

        // Setup: Populate data and enable history
        let value = generate_value();
        for i in 0..self.config.dataset_size {
            let key = format!("key_{}", i);
            self.client.set(&key, &value).await?;
            let _ = self.client.history_enable(&key).await;
        }

        // Warmup
        let mut rng = thread_rng();
        for _ in 0..self.config.warmup_iterations {
            let key = format!("key_{}", rng.gen_range(0..self.config.dataset_size));
            let _ = self.client.history_get(&key).await;
        }

        // Measure
        let mut latencies = Vec::with_capacity(self.config.iterations);
        let start = Instant::now();

        for _ in 0..self.config.iterations {
            let key = format!("key_{}", rng.gen_range(0..self.config.dataset_size));
            let op_start = Instant::now();
            match self.client.history_get(&key).await {
                Ok(_) => {
                    latencies.push(op_start.elapsed().as_secs_f64() * 1_000_000.0);
                    result.successful_ops += 1;
                }
                Err(_) => {
                    result.failed_ops += 1;
                }
            }
        }

        result.duration_secs = start.elapsed().as_secs_f64();
        result.latencies_us = latencies;
        result.iterations = self.config.iterations;

        Ok(result)
    }

    /// Benchmark KEYINFO command
    pub async fn bench_keyinfo(&self) -> Result<BenchmarkResult> {
        let mut result = BenchmarkResult::new(
            "KEYINFO",
            &self.config.backend_name,
            self.config.dataset_size,
            self.config.concurrency,
        );

        // Setup: Populate data
        let value = generate_value();
        for i in 0..self.config.dataset_size {
            self.client.set(&format!("key_{}", i), &value).await?;
        }

        // Warmup
        let mut rng = thread_rng();
        for _ in 0..self.config.warmup_iterations {
            let key = format!("key_{}", rng.gen_range(0..self.config.dataset_size));
            let _ = self.client.keyinfo(&key).await;
        }

        // Measure
        let mut latencies = Vec::with_capacity(self.config.iterations);
        let start = Instant::now();

        for _ in 0..self.config.iterations {
            let key = format!("key_{}", rng.gen_range(0..self.config.dataset_size));
            let op_start = Instant::now();
            match self.client.keyinfo(&key).await {
                Ok(_) => {
                    latencies.push(op_start.elapsed().as_secs_f64() * 1_000_000.0);
                    result.successful_ops += 1;
                }
                Err(_) => {
                    result.failed_ops += 1;
                }
            }
        }

        result.duration_secs = start.elapsed().as_secs_f64();
        result.latencies_us = latencies;
        result.iterations = self.config.iterations;

        Ok(result)
    }

    /// Benchmark VACUUM command
    pub async fn bench_vacuum(&self) -> Result<BenchmarkResult> {
        let mut result = BenchmarkResult::new(
            "VACUUM",
            &self.config.backend_name,
            self.config.dataset_size,
            self.config.concurrency,
        );

        // Setup: Populate data with short TTL for deletion
        let value = generate_value();
        for i in 0..self.config.dataset_size {
            let key = format!("key_{}", i);
            self.client.set(&key, &value).await?;
            let _ = self.client.expire(&key, 1).await;
        }

        // Wait for expiration
        tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;

        // Measure
        let mut latencies = Vec::with_capacity(self.config.iterations);
        let start = Instant::now();

        for _ in 0..self.config.iterations {
            let op_start = Instant::now();
            match self.client.vacuum().await {
                Ok(_) => {
                    latencies.push(op_start.elapsed().as_secs_f64() * 1_000_000.0);
                    result.successful_ops += 1;
                }
                Err(_) => {
                    result.failed_ops += 1;
                }
            }
        }

        result.duration_secs = start.elapsed().as_secs_f64();
        result.latencies_us = latencies;
        result.iterations = self.config.iterations;

        Ok(result)
    }

    /// Clean up after benchmarks
    pub async fn cleanup(&self) -> Result<()> {
        self.client.flushdb().await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_percentile_calculation() {
        let mut result = BenchmarkResult::new("TEST", "test", 100, 1);
        result.latencies_us = (1..=100).map(|x| x as f64).collect();

        assert!((result.p50_latency_us() - 50.0).abs() < 1.0);
        assert!((result.p95_latency_us() - 95.0).abs() < 1.0);
        assert!((result.p99_latency_us() - 99.0).abs() < 1.0);
    }

    #[test]
    fn test_throughput_calculation() {
        let mut result = BenchmarkResult::new("TEST", "test", 100, 1);
        result.successful_ops = 10000;
        result.duration_secs = 1.0;

        assert_eq!(result.throughput_ops_sec(), 10000.0);
    }

    #[test]
    fn test_error_rate() {
        let mut result = BenchmarkResult::new("TEST", "test", 100, 1);
        result.successful_ops = 90;
        result.failed_ops = 10;

        assert_eq!(result.error_rate(), 10.0);
    }
}
