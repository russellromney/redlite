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

/// A regression seed to always test
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegressionSeed {
    pub seed: u64,
    pub test_type: String,
    pub description: String,
    pub added: DateTime<Utc>,
    pub issue_url: Option<String>,
}

/// Fault types for chaos testing
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Fault {
    DiskFull,
    CorruptRead,
    CorruptWrite,
    SlowWrite,
    ConnectionDrop,
    CrashMidWrite,
}

impl Fault {
    pub fn from_str(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "disk_full" => Some(Fault::DiskFull),
            "corrupt_read" => Some(Fault::CorruptRead),
            "corrupt_write" => Some(Fault::CorruptWrite),
            "slow_write" => Some(Fault::SlowWrite),
            "connection_drop" => Some(Fault::ConnectionDrop),
            "crash_mid_write" => Some(Fault::CrashMidWrite),
            _ => None,
        }
    }
}

/// Memory snapshot for soak testing
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MemorySnapshot {
    pub timestamp: DateTime<Utc>,
    pub rss_bytes: u64,
    pub heap_bytes: Option<u64>,
    pub open_fds: Option<u32>,
}
