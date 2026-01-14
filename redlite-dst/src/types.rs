use serde::{Deserialize, Serialize};
use chrono::{DateTime, Utc};

/// Result of a single test run
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TestResult {
    pub test_name: String,
    pub seed: u64,
    pub passed: bool,
    pub duration_ms: u64,
    pub error: Option<String>,
    pub timestamp: DateTime<Utc>,
}

impl TestResult {
    pub fn pass(test_name: &str, seed: u64, duration_ms: u64) -> Self {
        Self {
            test_name: test_name.to_string(),
            seed,
            passed: true,
            duration_ms,
            error: None,
            timestamp: Utc::now(),
        }
    }

    pub fn fail(test_name: &str, seed: u64, duration_ms: u64, error: &str) -> Self {
        Self {
            test_name: test_name.to_string(),
            seed,
            passed: false,
            duration_ms,
            error: Some(error.to_string()),
            timestamp: Utc::now(),
        }
    }
}

/// Summary of a test suite run
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TestSummary {
    pub suite_name: String,
    pub total_tests: usize,
    pub passed: usize,
    pub failed: usize,
    pub skipped: usize,
    pub total_duration_ms: u64,
    pub failed_seeds: Vec<u64>,
    pub timestamp: DateTime<Utc>,
}

impl TestSummary {
    pub fn new(suite_name: &str) -> Self {
        Self {
            suite_name: suite_name.to_string(),
            total_tests: 0,
            passed: 0,
            failed: 0,
            skipped: 0,
            total_duration_ms: 0,
            failed_seeds: Vec::new(),
            timestamp: Utc::now(),
        }
    }

    pub fn add_result(&mut self, result: &TestResult) {
        self.total_tests += 1;
        self.total_duration_ms += result.duration_ms;
        if result.passed {
            self.passed += 1;
        } else {
            self.failed += 1;
            self.failed_seeds.push(result.seed);
        }
    }
}

/// Oracle test statistics
#[derive(Debug, Clone, Default)]
pub struct OracleStats {
    pub operations: usize,
    pub divergences: usize,
}

impl OracleStats {
    pub fn new() -> Self {
        Self::default()
    }
}
