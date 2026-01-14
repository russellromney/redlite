use chrono::Utc;
use serde::Serialize;

use crate::types::{TestResult, TestSummary};

/// JSON report format
#[derive(Debug, Serialize)]
pub struct JsonReport {
    pub metadata: ReportMetadata,
    pub summary: SummaryStats,
    pub results: Vec<TestResultJson>,
    pub failed_seeds: Vec<FailedSeed>,
}

#[derive(Debug, Serialize)]
pub struct ReportMetadata {
    pub tool: String,
    pub version: String,
    pub timestamp: String,
    pub suite: String,
    pub host: String,
}

#[derive(Debug, Serialize)]
pub struct SummaryStats {
    pub total: usize,
    pub passed: usize,
    pub failed: usize,
    pub skipped: usize,
    pub duration_ms: u64,
    pub success_rate: f64,
}

#[derive(Debug, Serialize)]
pub struct TestResultJson {
    pub name: String,
    pub seed: u64,
    pub passed: bool,
    pub duration_ms: u64,
    pub error: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct FailedSeed {
    pub seed: u64,
    pub test: String,
    pub error: String,
    pub replay_command: String,
}

impl JsonReport {
    pub fn from_summary(summary: &TestSummary, results: &[TestResult]) -> Self {
        let failed_seeds: Vec<FailedSeed> = results
            .iter()
            .filter(|r| !r.passed)
            .map(|r| FailedSeed {
                seed: r.seed,
                test: r.test_name.clone(),
                error: r.error.clone().unwrap_or_default(),
                replay_command: format!(
                    "redlite-dst replay --seed {} --test {}",
                    r.seed, r.test_name
                ),
            })
            .collect();

        Self {
            metadata: ReportMetadata {
                tool: "redlite-dst".to_string(),
                version: env!("CARGO_PKG_VERSION").to_string(),
                timestamp: Utc::now().to_rfc3339(),
                suite: summary.suite_name.clone(),
                host: std::env::var("HOSTNAME").unwrap_or_else(|_| "localhost".to_string()),
            },
            summary: SummaryStats {
                total: summary.total_tests,
                passed: summary.passed,
                failed: summary.failed,
                skipped: summary.skipped,
                duration_ms: summary.total_duration_ms,
                success_rate: if summary.total_tests > 0 {
                    (summary.passed as f64 / summary.total_tests as f64) * 100.0
                } else {
                    100.0
                },
            },
            results: results
                .iter()
                .map(|r| TestResultJson {
                    name: r.test_name.clone(),
                    seed: r.seed,
                    passed: r.passed,
                    duration_ms: r.duration_ms,
                    error: r.error.clone(),
                })
                .collect(),
            failed_seeds,
        }
    }

    pub fn to_json(&self) -> String {
        serde_json::to_string_pretty(self).unwrap_or_else(|_| "{}".to_string())
    }
}

/// Generate markdown report
pub fn generate_markdown(summary: &TestSummary, results: &[TestResult]) -> String {
    let mut md = String::new();

    // Header
    md.push_str("# redlite-dst Test Report\n\n");
    md.push_str(&format!("**Generated:** {}\n\n", Utc::now().to_rfc3339()));
    md.push_str(&format!("**Suite:** {}\n\n", summary.suite_name));

    // Summary box
    md.push_str("## Summary\n\n");
    md.push_str("| Metric | Value |\n");
    md.push_str("|--------|-------|\n");
    md.push_str(&format!("| Total Tests | {} |\n", summary.total_tests));
    md.push_str(&format!("| Passed | {} |\n", summary.passed));
    md.push_str(&format!("| Failed | {} |\n", summary.failed));
    md.push_str(&format!("| Skipped | {} |\n", summary.skipped));
    md.push_str(&format!("| Duration | {}ms |\n", summary.total_duration_ms));
    md.push_str(&format!(
        "| Success Rate | {:.1}% |\n",
        if summary.total_tests > 0 {
            (summary.passed as f64 / summary.total_tests as f64) * 100.0
        } else {
            100.0
        }
    ));
    md.push_str("\n");

    // Status badge
    if summary.failed == 0 {
        md.push_str("**Status:** :white_check_mark: All tests passed\n\n");
    } else {
        md.push_str(&format!(
            "**Status:** :x: {} tests failed\n\n",
            summary.failed
        ));
    }

    // Failed tests section
    if summary.failed > 0 {
        md.push_str("## Failed Tests\n\n");
        md.push_str("| Test | Seed | Error | Replay Command |\n");
        md.push_str("|------|------|-------|----------------|\n");

        for result in results.iter().filter(|r| !r.passed) {
            let error = result.error.as_deref().unwrap_or("Unknown");
            let replay = format!(
                "`redlite-dst replay --seed {} --test {}`",
                result.seed, result.test_name
            );
            md.push_str(&format!(
                "| {} | {} | {} | {} |\n",
                result.test_name, result.seed, error, replay
            ));
        }
        md.push_str("\n");

        // Regression seeds to add
        md.push_str("### Add to Regression Bank\n\n");
        md.push_str("```bash\n");
        for result in results.iter().filter(|r| !r.passed) {
            md.push_str(&format!(
                "redlite-dst seeds add --seed {} --description \"{}\"\n",
                result.seed,
                result.error.as_deref().unwrap_or("Unknown failure")
            ));
        }
        md.push_str("```\n\n");
    }

    // All results (collapsed for large runs)
    if results.len() > 20 {
        md.push_str("<details>\n");
        md.push_str("<summary>All Test Results</summary>\n\n");
    } else {
        md.push_str("## All Results\n\n");
    }

    md.push_str("| Test | Seed | Status | Duration |\n");
    md.push_str("|------|------|--------|----------|\n");
    for result in results {
        let status = if result.passed { ":white_check_mark:" } else { ":x:" };
        md.push_str(&format!(
            "| {} | {} | {} | {}ms |\n",
            result.test_name, result.seed, status, result.duration_ms
        ));
    }

    if results.len() > 20 {
        md.push_str("\n</details>\n");
    }

    md.push_str("\n---\n");
    md.push_str("*Generated by redlite-dst*\n");

    md
}
