//! MadSim integration module for deterministic simulation testing.
//!
//! This module provides a unified interface for running deterministic simulations,
//! whether using the full MadSim runtime or the simpler ChaCha8Rng-based approach.
//!
//! # Usage
//!
//! Build with MadSim enabled:
//! ```bash
//! RUSTFLAGS="--cfg madsim" cargo build --features madsim
//! ```
//!
//! Run normally (without MadSim):
//! ```bash
//! cargo run -- simulate --seeds 100
//! ```

use rand::{Rng, SeedableRng};
use rand_chacha::ChaCha8Rng;
use std::time::Duration;

/// Configuration for a deterministic simulation run.
#[derive(Debug, Clone)]
pub struct SimConfig {
    /// Base seed for the simulation.
    pub seed: u64,
    /// Maximum number of operations to run.
    pub max_ops: usize,
    /// Whether to enable fault injection.
    pub enable_faults: bool,
    /// Probability of network delay (0.0 - 1.0).
    pub network_delay_prob: f64,
    /// Probability of operation failure (0.0 - 1.0).
    pub failure_prob: f64,
}

impl Default for SimConfig {
    fn default() -> Self {
        Self {
            seed: 0,
            max_ops: 1000,
            enable_faults: false,
            network_delay_prob: 0.0,
            failure_prob: 0.0,
        }
    }
}

impl SimConfig {
    pub fn new(seed: u64) -> Self {
        Self {
            seed,
            ..Default::default()
        }
    }

    pub fn with_ops(mut self, ops: usize) -> Self {
        self.max_ops = ops;
        self
    }

    pub fn with_faults(mut self, enable: bool) -> Self {
        self.enable_faults = enable;
        self
    }

    pub fn with_network_delay(mut self, prob: f64) -> Self {
        self.network_delay_prob = prob.clamp(0.0, 1.0);
        self
    }

    pub fn with_failure_prob(mut self, prob: f64) -> Self {
        self.failure_prob = prob.clamp(0.0, 1.0);
        self
    }
}

/// A deterministic simulation context.
///
/// This provides controlled randomness and optional fault injection
/// for simulation testing.
pub struct SimContext {
    config: SimConfig,
    rng: ChaCha8Rng,
    op_count: usize,
    faults_injected: usize,
}

impl SimContext {
    /// Create a new simulation context with the given configuration.
    pub fn new(config: SimConfig) -> Self {
        let rng = ChaCha8Rng::seed_from_u64(config.seed);
        Self {
            config,
            rng,
            op_count: 0,
            faults_injected: 0,
        }
    }

    /// Get a reference to the configuration.
    pub fn config(&self) -> &SimConfig {
        &self.config
    }

    /// Get the current operation count.
    pub fn op_count(&self) -> usize {
        self.op_count
    }

    /// Get the number of faults injected.
    pub fn faults_injected(&self) -> usize {
        self.faults_injected
    }

    /// Check if we've reached the maximum number of operations.
    pub fn should_stop(&self) -> bool {
        self.op_count >= self.config.max_ops
    }

    /// Record an operation and return true if we should continue.
    pub fn record_op(&mut self) -> bool {
        self.op_count += 1;
        !self.should_stop()
    }

    /// Generate a deterministic random number in range.
    pub fn gen_range<T, R>(&mut self, range: R) -> T
    where
        T: rand::distributions::uniform::SampleUniform,
        R: rand::distributions::uniform::SampleRange<T>,
    {
        self.rng.gen_range(range)
    }

    /// Generate a random boolean with the given probability of being true.
    pub fn gen_bool(&mut self, prob: f64) -> bool {
        self.rng.gen_bool(prob.clamp(0.0, 1.0))
    }

    /// Check if a fault should be injected (based on configuration).
    pub fn should_inject_fault(&mut self) -> bool {
        if !self.config.enable_faults {
            return false;
        }
        if self.gen_bool(self.config.failure_prob) {
            self.faults_injected += 1;
            true
        } else {
            false
        }
    }

    /// Get a deterministic delay duration (for simulating network latency).
    pub fn get_delay(&mut self) -> Option<Duration> {
        if !self.config.enable_faults {
            return None;
        }
        if self.gen_bool(self.config.network_delay_prob) {
            // Generate delay between 1ms and 100ms
            let ms = self.gen_range(1..100);
            Some(Duration::from_millis(ms))
        } else {
            None
        }
    }

    /// Generate a deterministic random key.
    pub fn random_key(&mut self) -> String {
        format!("key_{}", self.gen_range(0u32..1000))
    }

    /// Generate a deterministic random value.
    pub fn random_value(&mut self) -> Vec<u8> {
        let len = self.gen_range(1..100);
        let mut value = vec![0u8; len];
        self.rng.fill(&mut value[..]);
        value
    }
}

/// MadSim-specific simulation runner.
///
/// When compiled with `--cfg madsim`, this uses the MadSim deterministic runtime.
/// Otherwise, it uses a simpler thread-based simulation.
#[cfg(madsim)]
pub mod runtime {
    use super::*;
    use madsim::Config;

    /// Run a simulation using MadSim's deterministic runtime.
    ///
    /// This provides true deterministic async scheduling.
    pub fn run_simulation<F, Fut>(seed: u64, f: F) -> Result<(), String>
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = Result<(), String>>,
    {
        let config = Config::default();
        let mut rt = madsim::runtime::Runtime::with_seed_and_config(seed, config);
        rt.block_on(f())
    }

    /// Create a MadSim runtime with the given seed.
    pub fn create_runtime(seed: u64) -> madsim::runtime::Runtime {
        let config = Config::default();
        madsim::runtime::Runtime::with_seed_and_config(seed, config)
    }

    /// Deterministic sleep using MadSim's simulated time.
    pub async fn sleep(duration: Duration) {
        madsim::time::sleep(duration).await;
    }

    /// Spawn a task on the MadSim runtime.
    pub fn spawn<F>(future: F) -> madsim::task::JoinHandle<F::Output>
    where
        F: std::future::Future + Send + 'static,
        F::Output: Send + 'static,
    {
        madsim::task::spawn(future)
    }

    /// Yield execution to other tasks.
    pub async fn yield_now() {
        madsim::task::yield_now().await;
    }
}

/// Standard runtime (without MadSim).
///
/// Uses tokio with ChaCha8Rng for deterministic randomness,
/// but does not provide deterministic async scheduling.
#[cfg(not(madsim))]
pub mod runtime {
    use super::*;

    /// Run a simulation using standard tokio runtime.
    ///
    /// Note: This does not provide deterministic async scheduling.
    /// Use `--cfg madsim` for true determinism.
    pub fn run_simulation<F, Fut>(seed: u64, f: F) -> Result<(), String>
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = Result<(), String>>,
    {
        let rt = tokio::runtime::Runtime::new().map_err(|e| e.to_string())?;
        rt.block_on(f())
    }

    /// Sleep using tokio's time.
    pub async fn sleep(duration: Duration) {
        tokio::time::sleep(duration).await;
    }

    /// Spawn a task on the tokio runtime.
    pub fn spawn<F>(future: F) -> tokio::task::JoinHandle<F::Output>
    where
        F: std::future::Future + Send + 'static,
        F::Output: Send + 'static,
    {
        tokio::spawn(future)
    }

    /// Yield execution to other tasks.
    pub async fn yield_now() {
        tokio::task::yield_now().await;
    }
}

/// Virtual node for distributed simulation.
///
/// Represents a simulated node in a distributed system.
#[derive(Debug)]
pub struct VirtualNode {
    pub id: u32,
    pub is_alive: bool,
    pub network_partition: bool,
}

impl VirtualNode {
    pub fn new(id: u32) -> Self {
        Self {
            id,
            is_alive: true,
            network_partition: false,
        }
    }

    pub fn kill(&mut self) {
        self.is_alive = false;
    }

    pub fn revive(&mut self) {
        self.is_alive = true;
    }

    pub fn partition(&mut self) {
        self.network_partition = true;
    }

    pub fn heal(&mut self) {
        self.network_partition = false;
    }
}

/// Result of a simulation run.
#[derive(Debug)]
pub struct SimResult {
    pub seed: u64,
    pub ops_executed: usize,
    pub faults_injected: usize,
    pub passed: bool,
    pub error: Option<String>,
    pub duration_ms: u64,
}

impl SimResult {
    pub fn pass(seed: u64, ops: usize, faults: usize, duration_ms: u64) -> Self {
        Self {
            seed,
            ops_executed: ops,
            faults_injected: faults,
            passed: true,
            error: None,
            duration_ms,
        }
    }

    pub fn fail(seed: u64, ops: usize, faults: usize, duration_ms: u64, error: &str) -> Self {
        Self {
            seed,
            ops_executed: ops,
            faults_injected: faults,
            passed: false,
            error: Some(error.to_string()),
            duration_ms,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sim_context_determinism() {
        // Same seed should produce same sequence
        let mut ctx1 = SimContext::new(SimConfig::new(42));
        let mut ctx2 = SimContext::new(SimConfig::new(42));

        for _ in 0..100 {
            assert_eq!(ctx1.gen_range(0..1000u32), ctx2.gen_range(0..1000u32));
        }
    }

    #[test]
    fn test_sim_context_different_seeds() {
        let mut ctx1 = SimContext::new(SimConfig::new(1));
        let mut ctx2 = SimContext::new(SimConfig::new(2));

        // Different seeds should (almost certainly) produce different sequences
        let vals1: Vec<u32> = (0..10).map(|_| ctx1.gen_range(0..1000)).collect();
        let vals2: Vec<u32> = (0..10).map(|_| ctx2.gen_range(0..1000)).collect();

        assert_ne!(vals1, vals2);
    }

    #[test]
    fn test_fault_injection() {
        let config = SimConfig::new(42).with_faults(true).with_failure_prob(0.5);
        let mut ctx = SimContext::new(config);

        let mut fault_count = 0;
        for _ in 0..100 {
            if ctx.should_inject_fault() {
                fault_count += 1;
            }
        }

        // With 50% probability over 100 trials, we should get roughly 50 faults
        // Allow wide margin for randomness
        assert!(fault_count > 20 && fault_count < 80);
    }
}
