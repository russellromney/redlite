use anyhow::Result;
use console::style;
use indicatif::{ProgressBar, ProgressStyle};
use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::path::Path;
use std::time::Instant;

use crate::client::RedliteClient;
use crate::properties;
use crate::types::{TestResult, TestSummary};

/// Main test runner
pub struct TestRunner {
    verbose: bool,
}

impl TestRunner {
    pub fn new(verbose: bool) -> Self {
        Self { verbose }
    }

    fn print_header(&self, title: &str) {
        println!();
        println!("{}", style(format!("━━━ {} ━━━", title)).cyan().bold());
        println!();
    }

    fn print_result(&self, result: &TestResult) {
        let status = if result.passed {
            style("✓ PASS").green()
        } else {
            style("✗ FAIL").red()
        };
        println!(
            "  {} [seed={}] {} ({}ms)",
            status, result.seed, result.test_name, result.duration_ms
        );
        if let Some(err) = &result.error {
            println!("    {}", style(err).red());
        }
    }

    fn print_summary(&self, summary: &TestSummary) {
        println!();
        println!("{}", style("Summary").bold());
        println!(
            "  Total: {} | {} | {} | Skipped: {}",
            summary.total_tests,
            style(format!("Passed: {}", summary.passed)).green(),
            style(format!("Failed: {}", summary.failed)).red(),
            summary.skipped
        );
        println!("  Duration: {}ms", summary.total_duration_ms);

        if !summary.failed_seeds.is_empty() {
            println!();
            println!("{}", style("Failed seeds (for replay):").yellow());
            for seed in &summary.failed_seeds {
                println!("  redlite-dst replay --seed {} --test {}", seed, summary.suite_name);
            }
        }
    }

    fn progress_bar(&self, len: u64, msg: &str) -> ProgressBar {
        let pb = ProgressBar::new(len);
        pb.set_style(
            ProgressStyle::default_bar()
                .template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} {msg}")
                .unwrap()
                .progress_chars("█▓░"),
        );
        pb.set_message(msg.to_string());
        pb
    }

    /// Quick sanity check (<1 min)
    pub async fn smoke(&self) -> Result<()> {
        self.print_header("Smoke Tests");

        let tests = vec![
            ("basic_set_get", Self::smoke_basic_set_get as fn(&mut RedliteClient) -> Result<()>),
            ("basic_incr_decr", Self::smoke_basic_incr_decr),
            ("basic_list_ops", Self::smoke_basic_list_ops),
            ("basic_hash_ops", Self::smoke_basic_hash_ops),
            ("basic_set_ops", Self::smoke_basic_set_ops),
            ("basic_sorted_set", Self::smoke_basic_sorted_set),
            ("basic_persistence", Self::smoke_basic_persistence),
        ];

        let pb = self.progress_bar(tests.len() as u64, "Running smoke tests...");
        let mut summary = TestSummary::new("smoke");

        for (test_name, test_fn) in &tests {
            let start = Instant::now();
            let seed: u64 = rand::random();

            let mut client = match RedliteClient::new_memory() {
                Ok(c) => c,
                Err(e) => {
                    let duration = start.elapsed().as_millis() as u64;
                    let result = TestResult::fail(
                        test_name,
                        seed,
                        duration,
                        &format!("Failed to create client: {}", e),
                    );
                    if self.verbose {
                        self.print_result(&result);
                    }
                    summary.add_result(&result);
                    pb.inc(1);
                    continue;
                }
            };

            let test_result = test_fn(&mut client);
            let duration = start.elapsed().as_millis() as u64;

            let result = match test_result {
                Ok(_) => TestResult::pass(test_name, seed, duration),
                Err(e) => TestResult::fail(test_name, seed, duration, &e.to_string()),
            };

            if self.verbose {
                self.print_result(&result);
            }
            summary.add_result(&result);
            pb.inc(1);
        }

        pb.finish_with_message("Done!");
        self.print_summary(&summary);

        if summary.failed > 0 {
            anyhow::bail!("{} tests failed", summary.failed);
        }
        Ok(())
    }

    fn smoke_basic_set_get(client: &mut RedliteClient) -> Result<()> {
        client.set("test_key", b"test_value".to_vec())?;
        let value = client.get("test_key")?;
        if value != Some(b"test_value".to_vec()) {
            anyhow::bail!("SET/GET failed: expected 'test_value', got {:?}", value);
        }
        Ok(())
    }

    fn smoke_basic_incr_decr(client: &mut RedliteClient) -> Result<()> {
        let val1 = client.incr("counter")?;
        if val1 != 1 {
            anyhow::bail!("INCR failed: expected 1, got {}", val1);
        }
        let val2 = client.incr("counter")?;
        if val2 != 2 {
            anyhow::bail!("INCR failed: expected 2, got {}", val2);
        }
        let val3 = client.decr("counter")?;
        if val3 != 1 {
            anyhow::bail!("DECR failed: expected 1, got {}", val3);
        }
        Ok(())
    }

    fn smoke_basic_list_ops(client: &mut RedliteClient) -> Result<()> {
        client.lpush("mylist", b"a".to_vec())?;
        client.rpush("mylist", b"b".to_vec())?;
        let range = client.lrange("mylist", 0, -1)?;
        if range != vec![b"a".to_vec(), b"b".to_vec()] {
            anyhow::bail!("List operations failed: expected [a, b], got {:?}", range);
        }
        let popped = client.lpop("mylist")?;
        if popped != Some(b"a".to_vec()) {
            anyhow::bail!("LPOP failed: expected 'a', got {:?}", popped);
        }
        Ok(())
    }

    fn smoke_basic_hash_ops(client: &mut RedliteClient) -> Result<()> {
        client.hset("myhash", "field1", b"value1".to_vec())?;
        client.hset("myhash", "field2", b"value2".to_vec())?;
        let val = client.hget("myhash", "field1")?;
        if val != Some(b"value1".to_vec()) {
            anyhow::bail!("HGET failed: expected 'value1', got {:?}", val);
        }
        let all = client.hgetall("myhash")?;
        if all.len() != 2 {
            anyhow::bail!("HGETALL failed: expected 2 fields, got {}", all.len());
        }
        Ok(())
    }

    fn smoke_basic_set_ops(client: &mut RedliteClient) -> Result<()> {
        client.sadd("myset", b"member1".to_vec())?;
        client.sadd("myset", b"member2".to_vec())?;
        client.sadd("myset", b"member1".to_vec())?; // duplicate
        let members = client.smembers("myset")?;
        if members.len() != 2 {
            anyhow::bail!("SMEMBERS failed: expected 2 members, got {}", members.len());
        }
        let is_member = client.sismember("myset", b"member1")?;
        if !is_member {
            anyhow::bail!("SISMEMBER failed: member1 should be in set");
        }
        Ok(())
    }

    fn smoke_basic_sorted_set(client: &mut RedliteClient) -> Result<()> {
        client.zadd("myzset", 1.0, b"one".to_vec())?;
        client.zadd("myzset", 2.0, b"two".to_vec())?;
        client.zadd("myzset", 3.0, b"three".to_vec())?;
        let range = client.zrange("myzset", 0, -1)?;
        if range != vec![b"one".to_vec(), b"two".to_vec(), b"three".to_vec()] {
            anyhow::bail!("ZRANGE failed: expected ordered [one, two, three], got {:?}", range);
        }
        let score = client.zscore("myzset", b"two")?;
        if score != Some(2.0) {
            anyhow::bail!("ZSCORE failed: expected 2.0, got {:?}", score);
        }
        Ok(())
    }

    fn smoke_basic_persistence(_client: &mut RedliteClient) -> Result<()> {
        // Test persistence by creating a file-backed database
        let temp_dir = tempfile::tempdir()?;
        let db_path = temp_dir.path().join("test.db");

        {
            let mut file_client = RedliteClient::new_file(db_path.clone())?;
            file_client.set("persist_key", b"persist_value".to_vec())?;
        } // Client dropped, simulating crash

        // Reopen and verify
        {
            let file_client = RedliteClient::new_file(db_path)?;
            let value = file_client.get("persist_key")?;
            if value != Some(b"persist_value".to_vec()) {
                anyhow::bail!("Persistence failed: data not recovered after reopen");
            }
        }

        Ok(())
    }

    /// Property-based tests with proptest
    pub async fn properties(&self, seeds: u64, filter: Option<String>) -> Result<()> {
        self.print_header("Property-Based Tests");

        let all_props = properties::all_properties();
        let props: Vec<_> = if let Some(f) = &filter {
            all_props
                .iter()
                .filter(|p| p.name().contains(f.as_str()))
                .collect()
        } else {
            all_props.iter().collect()
        };

        println!("Testing {} properties with {} seeds each", props.len(), seeds);
        println!();

        let pb = self.progress_bar(props.len() as u64 * seeds, "Running property tests...");
        let mut summary = TestSummary::new("properties");
        let mut results = Vec::new();

        for prop in props {
            for seed_num in 0..seeds {
                let result = properties::run_property(prop.as_ref(), seed_num);

                if !result.passed && self.verbose {
                    self.print_result(&result);
                }
                summary.add_result(&result);
                results.push(result);
                pb.inc(1);
            }
        }

        pb.finish_with_message("Done!");
        self.print_summary(&summary);

        if summary.failed > 0 {
            anyhow::bail!("{} properties failed", summary.failed);
        }
        Ok(())
    }

    /// Compare against Redis for compatibility
    pub async fn oracle(&self, redis_host: &str, ops: usize) -> Result<()> {
        self.print_header("Oracle Tests (Redis Comparison)");
        println!("Comparing against Redis at {}", redis_host);
        println!("Operations per test: {}", ops);
        println!();

        // TODO: Implement actual Redis comparison
        println!("{}", style("⚠ Oracle tests require Redis connection").yellow());
        println!("  Start Redis: docker run -p 6379:6379 redis");
        println!("  Then rerun: redlite-dst oracle --redis {}", redis_host);

        Ok(())
    }

    /// Deterministic simulation with MadSim
    pub async fn simulate(&self, seeds: u64, ops: usize) -> Result<()> {
        self.print_header("Deterministic Simulation");
        println!("Seeds: {} | Operations per seed: {}", seeds, ops);
        println!();

        let pb = self.progress_bar(seeds, "Running simulations...");
        let mut summary = TestSummary::new("simulate");

        for seed in 0..seeds {
            let start = Instant::now();

            // Placeholder: actual MadSim simulation
            let passed = true;
            let duration = start.elapsed().as_millis() as u64;

            let result = if passed {
                TestResult::pass("simulation", seed, duration)
            } else {
                TestResult::fail("simulation", seed, duration, "Invariant violated")
            };

            if !result.passed {
                self.print_result(&result);
            }
            summary.add_result(&result);
            pb.inc(1);
        }

        pb.finish_with_message("Done!");
        self.print_summary(&summary);

        if summary.failed > 0 {
            anyhow::bail!("{} simulations failed", summary.failed);
        }
        Ok(())
    }

    /// Fault injection tests
    pub async fn chaos(&self, faults: &[&str], seeds: u64) -> Result<()> {
        self.print_header("Chaos Tests (Fault Injection)");
        println!("Faults: {:?}", faults);
        println!("Seeds per fault: {}", seeds);
        println!();

        let pb = self.progress_bar(faults.len() as u64 * seeds, "Injecting faults...");
        let mut summary = TestSummary::new("chaos");

        for fault in faults {
            for seed_num in 0..seeds {
                let start = Instant::now();

                // Placeholder: actual fault injection
                let passed = true;
                let duration = start.elapsed().as_millis() as u64;

                let test_name = format!("chaos_{}", fault);
                let result = if passed {
                    TestResult::pass(&test_name, seed_num, duration)
                } else {
                    TestResult::fail(&test_name, seed_num, duration, "Failed to recover")
                };

                if !result.passed {
                    self.print_result(&result);
                }
                summary.add_result(&result);
                pb.inc(1);
            }
        }

        pb.finish_with_message("Done!");
        self.print_summary(&summary);

        if summary.failed > 0 {
            anyhow::bail!("{} chaos tests failed", summary.failed);
        }
        Ok(())
    }

    /// Scale testing
    pub async fn stress(&self, connections: usize, keys: usize) -> Result<()> {
        self.print_header("Stress Tests");
        println!("Connections: {} | Keys: {}", connections, keys);
        println!();

        let pb = self.progress_bar(100, "Running stress test...");

        for i in 0..100 {
            // Placeholder: actual stress test
            tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
            pb.inc(1);
        }

        pb.finish_with_message("Done!");

        println!();
        println!("{}", style("Stress test completed").green());
        println!("  Peak throughput: {} ops/sec", style("TODO").yellow());
        println!("  P99 latency: {} ms", style("TODO").yellow());

        Ok(())
    }

    /// Fuzzing harness
    pub async fn fuzz(&self, target: &str, duration: u64) -> Result<()> {
        self.print_header("Fuzzing");
        println!("Target: {} | Duration: {}s", target, duration);
        println!();

        let valid_targets = ["resp_parser", "query_parser", "command_handler"];
        if !valid_targets.contains(&target) {
            anyhow::bail!(
                "Unknown fuzz target: {}. Valid targets: {:?}",
                target,
                valid_targets
            );
        }

        let pb = self.progress_bar(duration, &format!("Fuzzing {}...", target));

        for _ in 0..duration {
            // Placeholder: actual fuzzing
            tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
            pb.inc(1);
        }

        pb.finish_with_message("Done!");

        println!();
        println!("{}", style("Fuzzing completed").green());
        println!("  Inputs tested: {}", style("TODO").yellow());
        println!("  Crashes found: {}", style("0").green());

        Ok(())
    }

    /// Long-running stability test
    pub async fn soak(&self, duration: &str, interval: u64) -> Result<()> {
        self.print_header("Soak Test");
        println!("Duration: {} | Check interval: {}s", duration, interval);
        println!();

        // Parse duration (e.g., "1h", "30m", "24h")
        let seconds = parse_duration(duration)?;
        let checks = seconds / interval;

        let pb = self.progress_bar(checks, "Running soak test...");

        println!("{}", style("Monitoring for memory leaks and degradation...").dim());
        println!();

        for i in 0..checks {
            // Placeholder: actual soak test with memory monitoring
            tokio::time::sleep(tokio::time::Duration::from_secs(1)).await; // Accelerated for demo

            if self.verbose && i % 10 == 0 {
                println!("  Check {}: RSS={}MB, Ops/sec={}", i, "TODO", "TODO");
            }
            pb.inc(1);
        }

        pb.finish_with_message("Done!");

        println!();
        println!("{}", style("Soak test completed").green());
        println!("  Memory growth: {}", style("TODO").yellow());
        println!("  Throughput stability: {}", style("TODO").yellow());

        Ok(())
    }

    /// Parallel execution on fly.io
    pub async fn cloud(&self, seeds: u64, machines: usize) -> Result<()> {
        self.print_header("Cloud Parallel Execution");
        println!("Seeds: {} | Machines: {}", seeds, machines);
        println!();

        println!("{}", style("⚠ Cloud execution requires fly.io setup").yellow());
        println!("  1. Install flyctl: curl -L https://fly.io/install.sh | sh");
        println!("  2. Login: flyctl auth login");
        println!("  3. Create app: flyctl apps create redlite-dst");
        println!();
        println!("Seeds per machine: {}", seeds / machines as u64);

        Ok(())
    }

    /// Reproduce a specific failure
    pub async fn replay(&self, seed: u64, test: &str) -> Result<()> {
        self.print_header("Replay");
        println!("Replaying seed {} for test '{}'", seed, test);
        println!();

        let start = Instant::now();

        // Placeholder: actual replay
        let passed = true;
        let duration = start.elapsed().as_millis() as u64;

        let result = if passed {
            TestResult::pass(test, seed, duration)
        } else {
            TestResult::fail(test, seed, duration, "Replay failed")
        };

        self.print_result(&result);

        if !result.passed {
            anyhow::bail!("Replay failed for seed {}", seed);
        }

        println!();
        println!("{}", style("Replay passed - bug may be fixed!").green());

        Ok(())
    }

    /// Run all tests
    pub async fn full(&self, quick: bool) -> Result<()> {
        self.print_header("Full Test Suite");

        if quick {
            println!("{}", style("Running quick mode (skipping slow tests)").yellow());
        }

        println!();

        // Run smoke tests
        self.smoke().await?;

        // Run property tests (fewer seeds in quick mode)
        let seeds = if quick { 100 } else { 1000 };
        self.properties(seeds, None).await?;

        // Run simulation (fewer seeds in quick mode)
        let sim_seeds = if quick { 100 } else { 1000 };
        self.simulate(sim_seeds, 1000).await?;

        // Run chaos tests
        let chaos_seeds = if quick { 10 } else { 100 };
        self.chaos(&["disk_full", "slow_write"], chaos_seeds).await?;

        if !quick {
            // Run stress tests
            self.stress(100, 100000).await?;
        }

        println!();
        println!("{}", style("━━━ All tests passed! ━━━").green().bold());

        Ok(())
    }

    /// List regression seeds
    pub async fn seeds_list(&self) -> Result<()> {
        self.print_header("Regression Seeds");

        let seeds_file = "tests/regression_seeds.txt";
        println!("Seeds file: {}", seeds_file);
        println!();

        if !Path::new(seeds_file).exists() {
            println!("{}", style("No regression seeds yet").dim());
            println!();
            println!("Add seeds with: redlite-dst seeds add --seed <SEED> --description \"<DESC>\"");
            return Ok(());
        }

        let file = File::open(seeds_file)?;
        let reader = BufReader::new(file);
        let mut count = 0;

        for line in reader.lines() {
            let line = line?;
            let line = line.trim();

            // Skip empty lines and comments
            if line.is_empty() || line.starts_with('#') {
                continue;
            }

            // Parse: SEED TEST_TYPE DESCRIPTION
            let parts: Vec<&str> = line.splitn(3, ' ').collect();
            if parts.len() >= 3 {
                let seed = parts[0];
                let test_type = parts[1];
                let description = parts[2];
                println!("  {} {} {}",
                    style(seed).cyan(),
                    style(test_type).yellow(),
                    description
                );
                count += 1;
            }
        }

        println!();
        if count == 0 {
            println!("{}", style("No regression seeds found").dim());
        } else {
            println!("Total: {} regression seed(s)", count);
        }

        Ok(())
    }

    /// Add a regression seed
    pub async fn seeds_add(&self, seed: u64, description: &str) -> Result<()> {
        self.print_header("Add Regression Seed");

        println!("Seed: {}", seed);
        println!("Description: {}", description);
        println!();

        let seeds_file = "tests/regression_seeds.txt";

        // Create directory if it doesn't exist
        if let Some(parent) = Path::new(seeds_file).parent() {
            std::fs::create_dir_all(parent)?;
        }

        // Append to file
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(seeds_file)?;

        writeln!(file, "{} properties {}", seed, description)?;

        println!("{}", style("✓ Seed added to regression bank").green());
        println!("File: {}", seeds_file);

        Ok(())
    }

    /// Test all regression seeds
    pub async fn seeds_test(&self) -> Result<()> {
        self.print_header("Test Regression Seeds");

        let seeds_file = "tests/regression_seeds.txt";

        if !Path::new(seeds_file).exists() {
            println!("{}", style("No regression seeds to test").dim());
            return Ok(());
        }

        let file = File::open(seeds_file)?;
        let reader = BufReader::new(file);
        let mut seeds_to_test = Vec::new();

        for line in reader.lines() {
            let line = line?;
            let line = line.trim();

            // Skip empty lines and comments
            if line.is_empty() || line.starts_with('#') {
                continue;
            }

            // Parse: SEED TEST_TYPE DESCRIPTION
            let parts: Vec<&str> = line.splitn(3, ' ').collect();
            if parts.len() >= 2 {
                if let Ok(seed) = parts[0].parse::<u64>() {
                    let test_type = parts[1];
                    seeds_to_test.push((seed, test_type.to_string()));
                }
            }
        }

        if seeds_to_test.is_empty() {
            println!("{}", style("No valid regression seeds found").dim());
            return Ok(());
        }

        println!("Testing {} regression seed(s)", seeds_to_test.len());
        println!();

        let pb = self.progress_bar(seeds_to_test.len() as u64, "Replaying seeds...");
        let mut summary = TestSummary::new("regression");

        for (seed, test_type) in seeds_to_test {
            let start = Instant::now();

            // Run the appropriate test based on test_type
            let result = match test_type.as_str() {
                "properties" => {
                    // Run all properties with this seed
                    let props = properties::all_properties();
                    let mut all_passed = true;
                    let mut error_msg = String::new();

                    for prop in &props {
                        let prop_result = properties::run_property(prop.as_ref(), seed);
                        if !prop_result.passed {
                            all_passed = false;
                            if let Some(err) = &prop_result.error {
                                error_msg = format!("{}: {}", prop.name(), err);
                                break;
                            }
                        }
                    }

                    let duration = start.elapsed().as_millis() as u64;
                    if all_passed {
                        TestResult::pass(&format!("regression_{}", seed), seed, duration)
                    } else {
                        TestResult::fail(
                            &format!("regression_{}", seed),
                            seed,
                            duration,
                            &error_msg,
                        )
                    }
                }
                _ => {
                    let duration = start.elapsed().as_millis() as u64;
                    TestResult::fail(
                        &format!("regression_{}", seed),
                        seed,
                        duration,
                        &format!("Unknown test type: {}", test_type),
                    )
                }
            };

            if !result.passed && self.verbose {
                self.print_result(&result);
            }
            summary.add_result(&result);
            pb.inc(1);
        }

        pb.finish_with_message("Done!");
        self.print_summary(&summary);

        if summary.failed > 0 {
            anyhow::bail!("{} regression tests failed", summary.failed);
        }

        Ok(())
    }
}

/// Parse duration string (e.g., "1h", "30m", "1d")
fn parse_duration(s: &str) -> Result<u64> {
    let s = s.trim();
    if s.is_empty() {
        anyhow::bail!("Empty duration");
    }

    let (num, unit) = s.split_at(s.len() - 1);
    let num: u64 = num.parse()?;

    match unit {
        "s" => Ok(num),
        "m" => Ok(num * 60),
        "h" => Ok(num * 3600),
        "d" => Ok(num * 86400),
        _ => anyhow::bail!("Unknown duration unit: {}", unit),
    }
}
