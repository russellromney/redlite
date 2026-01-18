pub mod access_pattern;
pub mod benchmark;
pub mod client;
pub mod concurrency;
pub mod error;
pub mod matrix;
pub mod scenarios;
pub mod dispatcher;
pub mod output;
pub mod benchmark_runner;
pub mod report_generator;

pub use benchmark::{BenchmarkConfig, BenchmarkResult, BenchmarkRunner};
pub use client::{ClientError, RedisClient, RedisLikeClient, RedliteEmbeddedClient};
pub use concurrency::{ConcurrencyMode, ConcurrentBenchmark, ConcurrentBenchmarkResult};
pub use error::BenchError;
pub use scenarios::{
    WorkloadScenario, OperationWeight, ScenarioSetup, SetupStats,
    execute_setup, load_scenarios, find_scenario,
};
pub use dispatcher::execute_operation;
pub use benchmark_runner::{MultiScenarioRunner, ScenarioComparison, ScenarioResult};
pub use report_generator::{BenchmarkReport, ReportGenerator, ReportFormat};
pub use matrix::{load_matrix_spec, MatrixRunner};
