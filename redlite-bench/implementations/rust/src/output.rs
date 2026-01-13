//! Output formatting for benchmark results - supports console, JSON, and more
//!
//! Provides flexible output formatting for CI/reporting and data analysis

use serde::{Deserialize, Serialize};
use crate::benchmark::BenchmarkResult;
use crate::concurrency::ConcurrentBenchmarkResult;
use crate::error::Result;
use chrono::Utc;

/// Supported output formats
#[derive(Debug, Clone, Copy)]
pub enum OutputFormat {
    Console,
    Json,
}

impl std::str::FromStr for OutputFormat {
    type Err = String;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "console" => Ok(OutputFormat::Console),
            "json" => Ok(OutputFormat::Json),
            _ => Err(format!("Unknown output format: {}", s)),
        }
    }
}

/// JSON-serializable benchmark result wrapper
#[derive(Debug, Serialize, Deserialize)]
pub struct JsonBenchmarkResult {
    pub metadata: ResultMetadata,
    pub benchmark: BenchmarkData,
    pub latency_percentiles: LatencyPercentiles,
    pub throughput: ThroughputData,
    pub error_rate: f64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ResultMetadata {
    pub timestamp: String,
    pub backend: String,
    pub version: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct BenchmarkData {
    pub operation: String,
    pub dataset_size: usize,
    pub concurrency: usize,
    pub iterations: usize,
    pub successful_ops: usize,
    pub failed_ops: usize,
    pub duration_secs: f64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct LatencyPercentiles {
    pub min_us: f64,
    pub max_us: f64,
    pub avg_us: f64,
    pub stddev_us: f64,
    pub p50_us: f64,
    pub p95_us: f64,
    pub p99_us: f64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ThroughputData {
    pub ops_per_sec: f64,
}

impl JsonBenchmarkResult {
    pub fn from_result(result: &BenchmarkResult, backend: &str) -> Self {
        JsonBenchmarkResult {
            metadata: ResultMetadata {
                timestamp: Utc::now().to_rfc3339(),
                backend: backend.to_string(),
                version: env!("CARGO_PKG_VERSION").to_string(),
            },
            benchmark: BenchmarkData {
                operation: result.operation.clone(),
                dataset_size: result.dataset_size,
                concurrency: result.concurrency,
                iterations: result.iterations,
                successful_ops: result.successful_ops,
                failed_ops: result.failed_ops,
                duration_secs: result.duration_secs,
            },
            latency_percentiles: LatencyPercentiles {
                min_us: result.min_latency_us(),
                max_us: result.max_latency_us(),
                avg_us: result.avg_latency_us(),
                stddev_us: result.stddev_latency_us(),
                p50_us: result.p50_latency_us(),
                p95_us: result.p95_latency_us(),
                p99_us: result.p99_latency_us(),
            },
            throughput: ThroughputData {
                ops_per_sec: result.throughput_ops_sec(),
            },
            error_rate: result.error_rate(),
        }
    }
}

/// JSON-serializable concurrent benchmark result
#[derive(Debug, Serialize, Deserialize)]
pub struct JsonConcurrentResult {
    pub metadata: ResultMetadata,
    pub benchmark: ConcurrentBenchmarkData,
    pub latency_percentiles: LatencyPercentiles,
    pub throughput: ThroughputData,
    pub error_rate: f64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ConcurrentBenchmarkData {
    pub operation: String,
    pub concurrency_mode: String,
    pub concurrency: usize,
    pub total_iterations: usize,
    pub successful_ops: usize,
    pub failed_ops: usize,
    pub duration_secs: f64,
}

impl JsonConcurrentResult {
    pub fn from_result(result: &ConcurrentBenchmarkResult, backend: &str) -> Self {
        JsonConcurrentResult {
            metadata: ResultMetadata {
                timestamp: Utc::now().to_rfc3339(),
                backend: backend.to_string(),
                version: env!("CARGO_PKG_VERSION").to_string(),
            },
            benchmark: ConcurrentBenchmarkData {
                operation: result.operation.clone(),
                concurrency_mode: format!("{}", result.mode),
                concurrency: result.concurrency,
                total_iterations: result.total_iterations,
                successful_ops: result.successful_ops,
                failed_ops: result.failed_ops,
                duration_secs: result.duration_secs,
            },
            latency_percentiles: LatencyPercentiles {
                min_us: result.min_latency_us(),
                max_us: result.max_latency_us(),
                avg_us: result.avg_latency_us(),
                stddev_us: 0.0, // Not computed for concurrent results currently
                p50_us: result.p50_latency_us(),
                p95_us: result.p95_latency_us(),
                p99_us: result.p99_latency_us(),
            },
            throughput: ThroughputData {
                ops_per_sec: result.throughput_ops_sec(),
            },
            error_rate: result.error_rate(),
        }
    }
}

/// Format a benchmark result for output
pub fn format_benchmark_result(
    result: &BenchmarkResult,
    format: OutputFormat,
    backend: &str,
) -> Result<String> {
    match format {
        OutputFormat::Console => {
            result.print_summary();
            Ok(String::new())
        }
        OutputFormat::Json => {
            let json_result = JsonBenchmarkResult::from_result(result, backend);
            serde_json::to_string_pretty(&json_result)
                .map_err(|e| crate::error::BenchError::Serialization(e.to_string()))
        }
    }
}

/// Format a concurrent benchmark result for output
pub fn format_concurrent_result(
    result: &ConcurrentBenchmarkResult,
    format: OutputFormat,
    backend: &str,
) -> Result<String> {
    match format {
        OutputFormat::Console => {
            result.print_summary();
            Ok(String::new())
        }
        OutputFormat::Json => {
            let json_result = JsonConcurrentResult::from_result(result, backend);
            serde_json::to_string_pretty(&json_result)
                .map_err(|e| crate::error::BenchError::Serialization(e.to_string()))
        }
    }
}

/// Write output to stdout or file
pub fn write_output(content: &str, output_file: Option<&str>) -> Result<()> {
    if let Some(path) = output_file {
        std::fs::write(path, content).map_err(crate::error::BenchError::Io)?;
        println!("Output written to {}", path);
    } else if !content.is_empty() {
        println!("{}", content);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_output_format_parsing() {
        use std::str::FromStr;

        assert!(matches!(OutputFormat::from_str("console"), Ok(OutputFormat::Console)));
        assert!(matches!(OutputFormat::from_str("json"), Ok(OutputFormat::Json)));
        assert!(matches!(OutputFormat::from_str("JSON"), Ok(OutputFormat::Json)));
        assert!(OutputFormat::from_str("invalid").is_err());
    }
}
