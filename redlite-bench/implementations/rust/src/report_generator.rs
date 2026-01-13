//! Report generation for benchmark results
//!
//! Formats benchmark data into comprehensive reports comparing
//! Redis and Redlite performance across scenarios

use crate::benchmark_runner::ScenarioComparison;
use serde::{Deserialize, Serialize};
use std::fs;
use anyhow::Result;

/// Complete benchmark report
#[derive(Debug, Serialize, Deserialize)]
pub struct BenchmarkReport {
    pub title: String,
    pub timestamp: String,
    pub summary: ReportSummary,
    pub scenarios: Vec<ScenarioReport>,
    pub conclusions: Vec<String>,
}

/// Summary statistics for the entire report
#[derive(Debug, Serialize, Deserialize)]
pub struct ReportSummary {
    pub total_scenarios: usize,
    pub redis_scenarios_completed: usize,
    pub redlite_scenarios_completed: usize,
    pub redlite_faster_count: usize,
    pub redis_faster_count: usize,
    pub average_redlite_speedup: f64,
    pub average_throughput_improvement: f64,
}

/// Report for a single scenario
#[derive(Debug, Serialize, Deserialize)]
pub struct ScenarioReport {
    pub name: String,
    pub description: Option<String>,
    pub redis: Option<BackendMetrics>,
    pub redlite: Option<BackendMetrics>,
    pub comparison: Option<ComparisonMetrics>,
}

/// Metrics for a single backend execution
#[derive(Debug, Serialize, Deserialize)]
pub struct BackendMetrics {
    pub backend: String,
    pub throughput_ops_sec: f64,
    pub latency_p50_us: f64,
    pub latency_p95_us: f64,
    pub latency_p99_us: f64,
    pub latency_avg_us: f64,
    pub latency_max_us: f64,
    pub error_rate: f64,
    pub total_ops: usize,
    pub duration_secs: f64,
}

/// Comparison between Redis and Redlite
#[derive(Debug, Serialize, Deserialize)]
pub struct ComparisonMetrics {
    pub throughput_improvement_percent: f64,
    pub latency_p50_improvement_percent: f64,
    pub latency_p99_improvement_percent: f64,
    pub winner: String, // "Redis", "Redlite", or "Tie"
    pub verdict: String, // Human-readable comparison
}

pub struct ReportGenerator;

impl ReportGenerator {
    /// Generate a comprehensive report from scenario comparisons
    pub fn generate_report(
        comparisons: Vec<ScenarioComparison>,
        scenarios_metadata: &[(String, Option<String>)],
    ) -> BenchmarkReport {
        let mut scenario_reports = Vec::new();
        let mut redlite_faster = 0;
        let mut redis_faster = 0;
        let mut redlite_completes = 0;
        let mut redis_completes = 0;
        let mut speedups = Vec::new();
        let mut throughput_improvements = Vec::new();

        for comparison in &comparisons {
            let metadata = scenarios_metadata
                .iter()
                .find(|(name, _)| name == &comparison.scenario_name)
                .map(|(_, desc)| desc.clone())
                .flatten();

            let redis_metrics = comparison.redis_result.as_ref().map(|r| {
                redis_completes += 1;
                BackendMetrics {
                    backend: r.backend.clone(),
                    throughput_ops_sec: r.result.throughput_ops_sec(),
                    latency_p50_us: r.result.p50_latency_us(),
                    latency_p95_us: r.result.p95_latency_us(),
                    latency_p99_us: r.result.p99_latency_us(),
                    latency_avg_us: r.result.avg_latency_us(),
                    latency_max_us: r.result.max_latency_us(),
                    error_rate: r.result.error_rate(),
                    total_ops: r.result.successful_ops,
                    duration_secs: r.result.duration_secs,
                }
            });

            let redlite_metrics = comparison.redlite_result.as_ref().map(|r| {
                redlite_completes += 1;
                BackendMetrics {
                    backend: r.backend.clone(),
                    throughput_ops_sec: r.result.throughput_ops_sec(),
                    latency_p50_us: r.result.p50_latency_us(),
                    latency_p95_us: r.result.p95_latency_us(),
                    latency_p99_us: r.result.p99_latency_us(),
                    latency_avg_us: r.result.avg_latency_us(),
                    latency_max_us: r.result.max_latency_us(),
                    error_rate: r.result.error_rate(),
                    total_ops: r.result.successful_ops,
                    duration_secs: r.result.duration_secs,
                }
            });

            let comparison_metrics = match (comparison.throughput_diff(), comparison.latency_diff_p50()) {
                (Some((redlite_tps, redis_tps, tps_diff)), Some((redlite_p50, redis_p50, p50_diff))) => {
                    let p99_diff = comparison.latency_diff_p99()
                        .map(|(_, _, diff)| diff)
                        .unwrap_or(0.0);

                    let winner = if tps_diff > 5.0 {
                        redlite_faster += 1;
                        speedups.push(tps_diff);
                        "Redlite".to_string()
                    } else if tps_diff < -5.0 {
                        redis_faster += 1;
                        "Redis".to_string()
                    } else {
                        "Tie".to_string()
                    };

                    throughput_improvements.push(tps_diff);

                    let verdict = if tps_diff > 5.0 {
                        format!(
                            "Redlite is {:.1}% faster ({:.0} vs {:.0} ops/sec)",
                            tps_diff, redlite_tps, redis_tps
                        )
                    } else if tps_diff < -5.0 {
                        format!(
                            "Redis is {:.1}% faster ({:.0} vs {:.0} ops/sec)",
                            -tps_diff, redis_tps, redlite_tps
                        )
                    } else {
                        format!(
                            "Comparable performance ({:.0} vs {:.0} ops/sec)",
                            redlite_tps, redis_tps
                        )
                    };

                    Some(ComparisonMetrics {
                        throughput_improvement_percent: tps_diff,
                        latency_p50_improvement_percent: -p50_diff,
                        latency_p99_improvement_percent: -p99_diff,
                        winner,
                        verdict,
                    })
                }
                _ => None,
            };

            scenario_reports.push(ScenarioReport {
                name: comparison.scenario_name.clone(),
                description: metadata,
                redis: redis_metrics,
                redlite: redlite_metrics,
                comparison: comparison_metrics,
            });
        }

        let avg_speedup = if !speedups.is_empty() {
            speedups.iter().sum::<f64>() / speedups.len() as f64
        } else {
            0.0
        };

        let avg_throughput = if !throughput_improvements.is_empty() {
            throughput_improvements.iter().sum::<f64>() / throughput_improvements.len() as f64
        } else {
            0.0
        };

        let summary = ReportSummary {
            total_scenarios: comparisons.len(),
            redis_scenarios_completed: redis_completes,
            redlite_scenarios_completed: redlite_completes,
            redlite_faster_count: redlite_faster,
            redis_faster_count: redis_faster,
            average_redlite_speedup: avg_speedup,
            average_throughput_improvement: avg_throughput,
        };

        let conclusions = Self::generate_conclusions(&summary);

        BenchmarkReport {
            title: "redlite-bench: Redis vs Redlite Comprehensive Benchmark Report".to_string(),
            timestamp: chrono::Utc::now().to_rfc3339(),
            summary,
            scenarios: scenario_reports,
            conclusions,
        }
    }

    /// Generate human-readable conclusions from results
    fn generate_conclusions(summary: &ReportSummary) -> Vec<String> {
        let mut conclusions = Vec::new();

        conclusions.push(format!(
            "Completed {} scenarios: {} on Redis, {} on Redlite",
            summary.total_scenarios, summary.redis_scenarios_completed, summary.redlite_scenarios_completed
        ));

        if summary.redlite_faster_count > summary.redis_faster_count {
            conclusions.push(format!(
                "✓ Redlite won {}/{} direct comparisons ({}% of scenarios)",
                summary.redlite_faster_count,
                summary.total_scenarios,
                (summary.redlite_faster_count as f64 / summary.total_scenarios as f64 * 100.0) as i32
            ));
        } else if summary.redis_faster_count > summary.redlite_faster_count {
            conclusions.push(format!(
                "○ Redis won {}/{} direct comparisons ({}% of scenarios)",
                summary.redis_faster_count,
                summary.total_scenarios,
                (summary.redis_faster_count as f64 / summary.total_scenarios as f64 * 100.0) as i32
            ));
        } else {
            conclusions.push("= Comparable performance across scenarios".to_string());
        }

        if summary.average_throughput_improvement.abs() > 1.0 {
            let direction = if summary.average_throughput_improvement > 0.0 {
                "faster"
            } else {
                "slower"
            };
            conclusions.push(format!(
                "Average throughput: Redlite is {:.1}% {} than Redis",
                summary.average_throughput_improvement.abs(),
                direction
            ));
        }

        conclusions
    }

    /// Format report as markdown
    pub fn format_markdown(report: &BenchmarkReport) -> String {
        let mut markdown = String::new();

        markdown.push_str(&format!("# {}\n\n", report.title));
        markdown.push_str(&format!("**Generated**: {}\n\n", report.timestamp));

        // Summary section
        markdown.push_str("## Summary\n\n");
        markdown.push_str(&format!(
            "- **Total Scenarios**: {}\n",
            report.summary.total_scenarios
        ));
        markdown.push_str(&format!(
            "- **Redis Completed**: {}\n",
            report.summary.redis_scenarios_completed
        ));
        markdown.push_str(&format!(
            "- **Redlite Completed**: {}\n\n",
            report.summary.redlite_scenarios_completed
        ));

        markdown.push_str(&format!(
            "- **Redlite Faster**: {} scenarios\n",
            report.summary.redlite_faster_count
        ));
        markdown.push_str(&format!(
            "- **Redis Faster**: {} scenarios\n",
            report.summary.redis_faster_count
        ));
        markdown.push_str(&format!(
            "- **Average Throughput Improvement**: {:.2}%\n\n",
            report.summary.average_throughput_improvement
        ));

        // Conclusions
        if !report.conclusions.is_empty() {
            markdown.push_str("## Key Findings\n\n");
            for conclusion in &report.conclusions {
                markdown.push_str(&format!("- {}\n", conclusion));
            }
            markdown.push_str("\n");
        }

        // Detailed results
        markdown.push_str("## Detailed Results\n\n");

        for scenario in &report.scenarios {
            markdown.push_str(&format!("### {}\n", scenario.name));
            if let Some(desc) = &scenario.description {
                markdown.push_str(&format!("{}\n\n", desc));
            }

            if let Some(redis) = &scenario.redis {
                markdown.push_str("**Redis**\n");
                markdown.push_str(&format!("- Throughput: {:.0} ops/sec\n", redis.throughput_ops_sec));
                markdown.push_str(&format!("- Latency P50: {:.2} µs\n", redis.latency_p50_us));
                markdown.push_str(&format!(
                    "- Latency P99: {:.2} µs\n",
                    redis.latency_p99_us
                ));
                markdown.push_str(&format!(
                    "- Duration: {:.3}s ({} successful ops)\n\n",
                    redis.duration_secs, redis.total_ops
                ));
            } else {
                markdown.push_str("**Redis**: Failed to run\n\n");
            }

            if let Some(redlite) = &scenario.redlite {
                markdown.push_str("**Redlite (embedded)**\n");
                markdown.push_str(&format!(
                    "- Throughput: {:.0} ops/sec\n",
                    redlite.throughput_ops_sec
                ));
                markdown.push_str(&format!("- Latency P50: {:.2} µs\n", redlite.latency_p50_us));
                markdown.push_str(&format!(
                    "- Latency P99: {:.2} µs\n",
                    redlite.latency_p99_us
                ));
                markdown.push_str(&format!(
                    "- Duration: {:.3}s ({} successful ops)\n\n",
                    redlite.duration_secs, redlite.total_ops
                ));
            } else {
                markdown.push_str("**Redlite**: Failed to run\n\n");
            }

            if let Some(cmp) = &scenario.comparison {
                markdown.push_str("**Comparison**\n");
                markdown.push_str(&format!("- {}\n", cmp.verdict));
                markdown.push_str(&format!(
                    "- Throughput improvement: {:.2}%\n",
                    cmp.throughput_improvement_percent
                ));
                markdown.push_str(&format!(
                    "- Latency P50 improvement: {:.2}%\n",
                    cmp.latency_p50_improvement_percent
                ));
                markdown.push_str(&format!(
                    "- Winner: {}\n\n",
                    cmp.winner
                ));
            }
        }

        markdown
    }

    /// Save report to file
    pub fn save_report(report: &BenchmarkReport, path: &str, format: ReportFormat) -> Result<()> {
        match format {
            ReportFormat::Json => {
                let json = serde_json::to_string_pretty(report)?;
                fs::write(path, json)?;
            }
            ReportFormat::Markdown => {
                let markdown = Self::format_markdown(report);
                fs::write(path, markdown)?;
            }
        }
        Ok(())
    }
}

/// Report output format
#[derive(Debug, Clone, Copy)]
pub enum ReportFormat {
    Json,
    Markdown,
}

impl std::str::FromStr for ReportFormat {
    type Err = String;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "json" => Ok(ReportFormat::Json),
            "markdown" | "md" => Ok(ReportFormat::Markdown),
            _ => Err(format!("Unknown report format: {}", s)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_report_generation() {
        let summary = ReportSummary {
            total_scenarios: 5,
            redis_scenarios_completed: 5,
            redlite_scenarios_completed: 5,
            redlite_faster_count: 3,
            redis_faster_count: 2,
            average_redlite_speedup: 10.5,
            average_throughput_improvement: 5.2,
        };

        let conclusions = ReportGenerator::generate_conclusions(&summary);
        assert!(!conclusions.is_empty());
        assert!(conclusions[0].contains("5 scenarios"));
    }
}
