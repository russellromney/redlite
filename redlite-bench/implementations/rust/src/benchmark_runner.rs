//! Multi-scenario benchmark runner
//!
//! Orchestrates running multiple workload scenarios and collecting results
//! for performance comparison between backends

use crate::benchmark::BenchmarkResult;
use crate::client::RedisLikeClient;
use crate::scenarios::{WorkloadScenario, execute_setup};
use crate::dispatcher;
use std::time::Instant;
use anyhow::Result;
use rand::Rng;

/// Results from running a single scenario on a backend
#[derive(Debug, Clone)]
pub struct ScenarioResult {
    pub scenario_name: String,
    pub backend: String,
    pub result: BenchmarkResult,
}

/// Comparison results for a scenario run on both backends
#[derive(Debug)]
pub struct ScenarioComparison {
    pub scenario_name: String,
    pub redis_result: Option<ScenarioResult>,
    pub redlite_result: Option<ScenarioResult>,
}

impl ScenarioComparison {
    /// Calculate throughput difference (Redis vs Redlite)
    /// Returns (redlite_throughput, redis_throughput, percent_diff)
    /// Positive percent_diff means Redlite is faster
    pub fn throughput_diff(&self) -> Option<(f64, f64, f64)> {
        match (&self.redis_result, &self.redlite_result) {
            (Some(redis), Some(redlite)) => {
                let redis_tps = redis.result.throughput_ops_sec();
                let redlite_tps = redlite.result.throughput_ops_sec();
                let percent_diff = ((redlite_tps - redis_tps) / redis_tps) * 100.0;
                Some((redlite_tps, redis_tps, percent_diff))
            }
            _ => None,
        }
    }

    /// Calculate latency difference (P50)
    /// Returns (redlite_p50, redis_p50, percent_diff)
    /// Negative percent_diff means Redlite is faster (lower latency)
    pub fn latency_diff_p50(&self) -> Option<(f64, f64, f64)> {
        match (&self.redis_result, &self.redlite_result) {
            (Some(redis), Some(redlite)) => {
                let redis_p50 = redis.result.p50_latency_us();
                let redlite_p50 = redlite.result.p50_latency_us();
                let percent_diff = ((redlite_p50 - redis_p50) / redis_p50) * 100.0;
                Some((redlite_p50, redis_p50, percent_diff))
            }
            _ => None,
        }
    }

    /// Calculate latency difference (P99)
    pub fn latency_diff_p99(&self) -> Option<(f64, f64, f64)> {
        match (&self.redis_result, &self.redlite_result) {
            (Some(redis), Some(redlite)) => {
                let redis_p99 = redis.result.p99_latency_us();
                let redlite_p99 = redlite.result.p99_latency_us();
                let percent_diff = ((redlite_p99 - redis_p99) / redis_p99) * 100.0;
                Some((redlite_p99, redis_p99, percent_diff))
            }
            _ => None,
        }
    }
}

/// Multi-scenario benchmark runner
pub struct MultiScenarioRunner {
    pub scenarios: Vec<WorkloadScenario>,
    pub iterations: usize,
    pub dataset_size: usize,
}

impl MultiScenarioRunner {
    pub fn new(scenarios: Vec<WorkloadScenario>, iterations: usize, dataset_size: usize) -> Self {
        Self {
            scenarios,
            iterations,
            dataset_size,
        }
    }

    /// Run a single scenario against both Redis and Redlite
    pub async fn run_scenario_comparison<R, L>(
        &self,
        scenario: &WorkloadScenario,
        redis_client: &R,
        redlite_client: &L,
    ) -> Result<ScenarioComparison>
    where
        R: RedisLikeClient + 'static,
        L: RedisLikeClient + 'static,
    {
        let redis_result = self
            .run_scenario_on_backend(scenario, redis_client, "Redis")
            .await
            .ok();

        let redlite_result = self
            .run_scenario_on_backend(scenario, redlite_client, "Redlite (embedded)")
            .await
            .ok();

        Ok(ScenarioComparison {
            scenario_name: scenario.name.clone(),
            redis_result,
            redlite_result,
        })
    }

    /// Run a single scenario on a specific backend
    async fn run_scenario_on_backend<C>(
        &self,
        scenario: &WorkloadScenario,
        client: &C,
        backend: &str,
    ) -> Result<ScenarioResult>
    where
        C: RedisLikeClient,
    {
        let mut rng = rand::thread_rng();
        let mut result = BenchmarkResult::new(
            &format!("Scenario: {}", scenario.name),
            backend,
            self.dataset_size,
            1,
        );

        // Execute setup
        if let Some(ref setup) = scenario.setup {
            let _ = execute_setup(client, setup).await;
        }

        // Prepare normalized operation weights
        let normalized = scenario.normalized_operations();

        // Warmup
        for _ in 0..100 {
            let random_value: f64 = rng.gen();
            if let Some(op_name) = scenario.select_operation(&normalized, random_value) {
                let _ = dispatcher::execute_operation(
                    client,
                    &op_name,
                    self.dataset_size,
                    rng.clone(),
                )
                .await;
            }
        }

        // Measured iterations
        let mut latencies = Vec::with_capacity(self.iterations);
        let start = Instant::now();

        for _ in 0..self.iterations {
            let random_value: f64 = rng.gen();
            if let Some(op_name) = scenario.select_operation(&normalized, random_value) {
                match dispatcher::execute_operation(
                    client,
                    &op_name,
                    self.dataset_size,
                    rng.clone(),
                )
                .await
                {
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
        result.iterations = self.iterations;

        // Cleanup
        client.flushdb().await?;

        Ok(ScenarioResult {
            scenario_name: scenario.name.clone(),
            backend: backend.to_string(),
            result,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_scenario_comparison_throughput_diff() {
        let mut redis_result = BenchmarkResult::new("test", "Redis", 1000, 1);
        redis_result.latencies_us = vec![100.0; 1000];
        redis_result.duration_secs = 0.1;
        redis_result.successful_ops = 1000;

        let mut redlite_result = BenchmarkResult::new("test", "Redlite", 1000, 1);
        redlite_result.latencies_us = vec![50.0; 1000];
        redlite_result.duration_secs = 0.05;
        redlite_result.successful_ops = 1000;

        let comparison = ScenarioComparison {
            scenario_name: "test".to_string(),
            redis_result: Some(ScenarioResult {
                scenario_name: "test".to_string(),
                backend: "Redis".to_string(),
                result: redis_result,
            }),
            redlite_result: Some(ScenarioResult {
                scenario_name: "test".to_string(),
                backend: "Redlite".to_string(),
                result: redlite_result,
            }),
        };

        let (redlite_tps, redis_tps, diff) = comparison.throughput_diff().unwrap();
        assert!(diff > 0.0); // Redlite should be faster
    }
}
