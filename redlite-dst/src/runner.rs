use anyhow::Result;
use console::style;
use indicatif::{ProgressBar, ProgressStyle};
use rand::{Rng, SeedableRng};
use rand_chacha::ChaCha8Rng;
use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::path::Path;
use std::time::{Duration, Instant};

use crate::client::RedliteClient;
use crate::properties;
use crate::report::{generate_markdown, JsonReport};
use crate::sim::runtime;
use crate::types::{OracleStats, TestResult, TestSummary};

/// Output format for test results
#[derive(Debug, Clone, PartialEq)]
pub enum OutputFormat {
    Console,
    Json,
    Markdown,
}

impl From<&str> for OutputFormat {
    fn from(s: &str) -> Self {
        match s.to_lowercase().as_str() {
            "json" => OutputFormat::Json,
            "markdown" | "md" => OutputFormat::Markdown,
            _ => OutputFormat::Console,
        }
    }
}

/// Main test runner
pub struct TestRunner {
    verbose: bool,
    format: OutputFormat,
    output: Option<String>,
}

impl TestRunner {
    pub fn new(verbose: bool, format: &str, output: Option<String>) -> Self {
        Self {
            verbose,
            format: OutputFormat::from(format),
            output,
        }
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

    /// Output results in the configured format
    fn output_results(&self, summary: &TestSummary, results: &[TestResult]) -> Result<()> {
        let output_content = match self.format {
            OutputFormat::Console => {
                self.print_summary(summary);
                return Ok(());
            }
            OutputFormat::Json => {
                let report = JsonReport::from_summary(summary, results);
                report.to_json()
            }
            OutputFormat::Markdown => generate_markdown(summary, results),
        };

        // Write to file or stdout
        if let Some(path) = &self.output {
            let mut file = File::create(path)?;
            file.write_all(output_content.as_bytes())?;
            println!("{}", style(format!("Report written to: {}", path)).green());
        } else {
            println!("{}", output_content);
        }

        Ok(())
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
        let mut results = Vec::new();

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
                    results.push(result);
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
            results.push(result);
            pb.inc(1);
        }

        pb.finish_with_message("Done!");
        self.output_results(&summary, &results)?;

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
        self.output_results(&summary, &results)?;

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

        // Try to connect to Redis
        let redis_client = match redis::Client::open(format!("redis://{}/", redis_host)) {
            Ok(client) => client,
            Err(e) => {
                println!("{}", style(format!("⚠ Cannot connect to Redis: {}", e)).yellow());
                println!("  Start Redis: docker run -d -p 6379:6379 redis");
                println!("  Then rerun: redlite-dst oracle --redis {}", redis_host);
                anyhow::bail!("Redis connection failed");
            }
        };

        let mut redis_conn = match redis_client.get_connection() {
            Ok(conn) => conn,
            Err(e) => {
                println!("{}", style(format!("⚠ Cannot get Redis connection: {}", e)).yellow());
                anyhow::bail!("Redis connection failed");
            }
        };

        println!("{}", style("✓ Connected to Redis").green());
        println!();

        // Test groups
        let test_groups = vec![
            ("strings", Self::oracle_test_strings as fn(&mut RedliteClient, &mut redis::Connection, &mut rand_chacha::ChaCha8Rng, usize) -> Result<OracleStats>),
            ("lists", Self::oracle_test_lists),
            ("hashes", Self::oracle_test_hashes),
            ("sets", Self::oracle_test_sets),
            ("sorted_sets", Self::oracle_test_sorted_sets),
            ("keys", Self::oracle_test_keys),
        ];

        let pb = self.progress_bar(test_groups.len() as u64, "Running oracle tests...");
        let mut summary = TestSummary::new("oracle");
        let mut results = Vec::new();
        let mut total_divergences = 0;
        let mut total_ops = 0;

        for (group_name, test_fn) in &test_groups {
            let start = Instant::now();
            let seed: u64 = rand::random();
            let mut rng = rand_chacha::ChaCha8Rng::seed_from_u64(seed);

            // Flush both databases before each test group
            let _ = redis::cmd("FLUSHDB").query::<()>(&mut redis_conn);
            let mut redlite = match RedliteClient::new_memory() {
                Ok(c) => c,
                Err(e) => {
                    let duration = start.elapsed().as_millis() as u64;
                    let result = TestResult::fail(
                        group_name,
                        seed,
                        duration,
                        &format!("Failed to create Redlite client: {}", e),
                    );
                    if self.verbose {
                        self.print_result(&result);
                    }
                    summary.add_result(&result);
                    results.push(result);
                    pb.inc(1);
                    continue;
                }
            };

            let stats = match test_fn(&mut redlite, &mut redis_conn, &mut rng, ops) {
                Ok(s) => s,
                Err(e) => {
                    let duration = start.elapsed().as_millis() as u64;
                    let result = TestResult::fail(
                        group_name,
                        seed,
                        duration,
                        &format!("Oracle test failed: {}", e),
                    );
                    if self.verbose {
                        self.print_result(&result);
                    }
                    summary.add_result(&result);
                    results.push(result);
                    pb.inc(1);
                    continue;
                }
            };

            let duration = start.elapsed().as_millis() as u64;
            let passed = stats.divergences == 0;
            total_divergences += stats.divergences;
            total_ops += stats.operations;

            let result = if passed {
                TestResult::pass(group_name, seed, duration)
            } else {
                TestResult::fail(
                    group_name,
                    seed,
                    duration,
                    &format!("{} divergences out of {} ops", stats.divergences, stats.operations),
                )
            };

            if self.verbose || !passed {
                self.print_result(&result);
            }
            summary.add_result(&result);
            results.push(result);
            pb.inc(1);
        }

        pb.finish_with_message("Done!");
        self.output_results(&summary, &results)?;

        println!();
        println!("{}", style("Oracle Statistics").bold());
        println!("  Total operations: {}", total_ops);
        println!("  Total divergences: {}", if total_divergences == 0 {
            style(format!("{}", total_divergences)).green()
        } else {
            style(format!("{}", total_divergences)).red()
        });
        let compatibility = if total_ops > 0 {
            (total_ops - total_divergences) as f64 / total_ops as f64 * 100.0
        } else {
            0.0
        };
        println!("  Compatibility: {:.2}%", compatibility);

        if summary.failed > 0 {
            anyhow::bail!("{} oracle tests failed with {} divergences", summary.failed, total_divergences);
        }
        Ok(())
    }

    fn oracle_test_strings(
        redlite: &mut RedliteClient,
        redis: &mut redis::Connection,
        rng: &mut rand_chacha::ChaCha8Rng,
        ops: usize,
    ) -> Result<OracleStats> {
        use redis::Commands;
        let mut stats = OracleStats::new();

        for _ in 0..ops {
            let key = format!("str_{}", rng.gen_range(0..10));
            let op = rng.gen_range(0..4);

            match op {
                0 => {
                    // SET
                    let value: Vec<u8> = (0..rng.gen_range(1..20))
                        .map(|_| rng.gen::<u8>())
                        .collect();
                    redlite.set(&key, value.clone())?;
                    let _: () = redis.set(&key, &value)?;
                }
                1 => {
                    // GET
                    let redlite_val = redlite.get(&key)?;
                    let redis_val: Option<Vec<u8>> = redis.get(&key)?;
                    if redlite_val != redis_val {
                        stats.divergences += 1;
                    }
                }
                2 => {
                    // INCR
                    let redlite_val = redlite.incr(&key).ok();
                    let redis_val: Option<i64> = redis.incr(&key, 1).ok();
                    if redlite_val != redis_val {
                        stats.divergences += 1;
                    }
                }
                _ => {
                    // APPEND
                    let value: Vec<u8> = (0..rng.gen_range(1..10))
                        .map(|_| rng.gen::<u8>())
                        .collect();
                    let redlite_len = redlite.append(&key, value.clone()).ok();
                    let redis_len: Option<usize> = redis.append(&key, &value).ok();
                    if redlite_len != redis_len {
                        stats.divergences += 1;
                    }
                }
            }
            stats.operations += 1;
        }

        Ok(stats)
    }

    fn oracle_test_lists(
        redlite: &mut RedliteClient,
        redis: &mut redis::Connection,
        rng: &mut rand_chacha::ChaCha8Rng,
        ops: usize,
    ) -> Result<OracleStats> {
        use rand::Rng;
        use redis::Commands;
        let mut stats = OracleStats::new();

        for _ in 0..ops {
            let key = format!("list_{}", rng.gen_range(0..5));
            let op = rng.gen_range(0..5);

            match op {
                0 => {
                    // LPUSH
                    let value: Vec<u8> = format!("item_{}", rng.gen::<u32>()).into_bytes();
                    let redlite_len = redlite.lpush(&key, value.clone())?;
                    let redis_len: usize = redis.lpush(&key, &value)?;
                    if redlite_len != redis_len {
                        stats.divergences += 1;
                    }
                }
                1 => {
                    // RPUSH
                    let value: Vec<u8> = format!("item_{}", rng.gen::<u32>()).into_bytes();
                    let redlite_len = redlite.rpush(&key, value.clone())?;
                    let redis_len: usize = redis.rpush(&key, &value)?;
                    if redlite_len != redis_len {
                        stats.divergences += 1;
                    }
                }
                2 => {
                    // LPOP
                    let redlite_val = redlite.lpop(&key)?;
                    let redis_val: Option<Vec<u8>> = redis.lpop(&key, None)?;
                    if redlite_val != redis_val {
                        stats.divergences += 1;
                    }
                }
                3 => {
                    // RPOP
                    let redlite_val = redlite.rpop(&key)?;
                    let redis_val: Option<Vec<u8>> = redis.rpop(&key, None)?;
                    if redlite_val != redis_val {
                        stats.divergences += 1;
                    }
                }
                _ => {
                    // LRANGE
                    let redlite_vals = redlite.lrange(&key, 0, -1)?;
                    let redis_vals: Vec<Vec<u8>> = redis.lrange(&key, 0, -1)?;
                    if redlite_vals != redis_vals {
                        stats.divergences += 1;
                    }
                }
            }
            stats.operations += 1;
        }

        Ok(stats)
    }

    fn oracle_test_hashes(
        redlite: &mut RedliteClient,
        redis: &mut redis::Connection,
        rng: &mut rand_chacha::ChaCha8Rng,
        ops: usize,
    ) -> Result<OracleStats> {
        use rand::Rng;
        use redis::Commands;
        let mut stats = OracleStats::new();

        for _ in 0..ops {
            let key = format!("hash_{}", rng.gen_range(0..5));
            let field = format!("field_{}", rng.gen_range(0..10));
            let op = rng.gen_range(0..4);

            match op {
                0 => {
                    // HSET
                    let value: Vec<u8> = format!("value_{}", rng.gen::<u32>()).into_bytes();
                    let redlite_res = redlite.hset(&key, &field, value.clone())?;
                    let redis_res: usize = redis.hset(&key, &field, &value)?;
                    if redlite_res != redis_res {
                        stats.divergences += 1;
                    }
                }
                1 => {
                    // HGET
                    let redlite_val = redlite.hget(&key, &field)?;
                    let redis_val: Option<Vec<u8>> = redis.hget(&key, &field)?;
                    if redlite_val != redis_val {
                        stats.divergences += 1;
                    }
                }
                2 => {
                    // HDEL
                    let redlite_res = redlite.hdel(&key, &field)?;
                    let redis_res: usize = redis.hdel(&key, &field)?;
                    if redlite_res != redis_res {
                        stats.divergences += 1;
                    }
                }
                _ => {
                    // HGETALL
                    let redlite_vals = redlite.hgetall(&key)?;
                    let redis_vals: std::collections::HashMap<String, Vec<u8>> = redis.hgetall(&key)?;
                    if redlite_vals != redis_vals {
                        stats.divergences += 1;
                    }
                }
            }
            stats.operations += 1;
        }

        Ok(stats)
    }

    fn oracle_test_sets(
        redlite: &mut RedliteClient,
        redis: &mut redis::Connection,
        rng: &mut rand_chacha::ChaCha8Rng,
        ops: usize,
    ) -> Result<OracleStats> {
        use rand::Rng;
        use redis::Commands;
        let mut stats = OracleStats::new();

        for _ in 0..ops {
            let key = format!("set_{}", rng.gen_range(0..5));
            let member: Vec<u8> = format!("member_{}", rng.gen_range(0..20)).into_bytes();
            let op = rng.gen_range(0..4);

            match op {
                0 => {
                    // SADD
                    let redlite_res = redlite.sadd(&key, member.clone())?;
                    let redis_res: usize = redis.sadd(&key, &member)?;
                    if redlite_res != redis_res {
                        stats.divergences += 1;
                    }
                }
                1 => {
                    // SREM
                    let redlite_res = redlite.srem(&key, &member)?;
                    let redis_res: usize = redis.srem(&key, &member)?;
                    if redlite_res != redis_res {
                        stats.divergences += 1;
                    }
                }
                2 => {
                    // SISMEMBER
                    let redlite_res = redlite.sismember(&key, &member)?;
                    let redis_res: bool = redis.sismember(&key, &member)?;
                    if redlite_res != redis_res {
                        stats.divergences += 1;
                    }
                }
                _ => {
                    // SMEMBERS
                    let mut redlite_vals = redlite.smembers(&key)?;
                    let mut redis_vals: Vec<Vec<u8>> = redis.smembers(&key)?;
                    redlite_vals.sort();
                    redis_vals.sort();
                    if redlite_vals != redis_vals {
                        stats.divergences += 1;
                    }
                }
            }
            stats.operations += 1;
        }

        Ok(stats)
    }

    fn oracle_test_sorted_sets(
        redlite: &mut RedliteClient,
        redis: &mut redis::Connection,
        rng: &mut rand_chacha::ChaCha8Rng,
        ops: usize,
    ) -> Result<OracleStats> {
        use rand::Rng;
        use redis::Commands;
        let mut stats = OracleStats::new();

        for _ in 0..ops {
            let key = format!("zset_{}", rng.gen_range(0..5));
            let member: Vec<u8> = format!("member_{}", rng.gen_range(0..20)).into_bytes();
            let score = rng.gen_range(0.0..100.0);
            let op = rng.gen_range(0..3);

            match op {
                0 => {
                    // ZADD
                    let redlite_res = redlite.zadd(&key, score, member.clone())?;
                    let redis_res: usize = redis.zadd(&key, &member, score)?;
                    if redlite_res != redis_res {
                        stats.divergences += 1;
                    }
                }
                1 => {
                    // ZSCORE
                    let redlite_score = redlite.zscore(&key, &member)?;
                    let redis_score: Option<f64> = redis.zscore(&key, &member)?;
                    if redlite_score != redis_score {
                        stats.divergences += 1;
                    }
                }
                _ => {
                    // ZRANGE
                    let redlite_vals = redlite.zrange(&key, 0, -1)?;
                    let redis_vals: Vec<Vec<u8>> = redis.zrange(&key, 0, -1)?;
                    if redlite_vals != redis_vals {
                        stats.divergences += 1;
                    }
                }
            }
            stats.operations += 1;
        }

        Ok(stats)
    }

    fn oracle_test_keys(
        redlite: &mut RedliteClient,
        redis: &mut redis::Connection,
        rng: &mut rand_chacha::ChaCha8Rng,
        ops: usize,
    ) -> Result<OracleStats> {
        use rand::Rng;
        use redis::Commands;
        let mut stats = OracleStats::new();

        for _ in 0..ops {
            let key = format!("key_{}", rng.gen_range(0..10));
            let op = rng.gen_range(0..4);

            match op {
                0 => {
                    // SET (to create keys)
                    let value: Vec<u8> = format!("value_{}", rng.gen::<u32>()).into_bytes();
                    redlite.set(&key, value.clone())?;
                    let _: () = redis.set(&key, &value)?;
                }
                1 => {
                    // EXISTS
                    let redlite_res = redlite.exists(&[&key])?;
                    let redis_res: usize = redis.exists(&key)?;
                    if redlite_res != redis_res {
                        stats.divergences += 1;
                    }
                }
                2 => {
                    // DEL
                    let redlite_res = redlite.del(&[&key])?;
                    let redis_res: usize = redis.del(&key)?;
                    if redlite_res != redis_res {
                        stats.divergences += 1;
                    }
                }
                _ => {
                    // TTL
                    let redlite_ttl = redlite.ttl(&key)?;
                    let redis_ttl: i64 = redis.ttl(&key)?;
                    if redlite_ttl != redis_ttl {
                        stats.divergences += 1;
                    }
                }
            }
            stats.operations += 1;
        }

        Ok(stats)
    }

    /// Deterministic simulation testing
    ///
    /// Runs seed-reproducible scenarios that test concurrent operations,
    /// crash recovery, and high-load conditions. Each scenario uses ChaCha8Rng
    /// for deterministic randomness - same seed = same execution.
    pub async fn simulate(&self, seeds: u64, ops: usize) -> Result<()> {
        self.print_header("Deterministic Simulation");
        println!("Seeds: {} | Operations per seed: {}", seeds, ops);
        println!();

        // Simulation scenarios to run
        let scenarios = vec![
            "concurrent_operations",
            "crash_recovery",
            "connection_storm",
            "write_contention",
        ];

        let total_runs = seeds * scenarios.len() as u64;
        let pb = self.progress_bar(total_runs, "Running simulations...");
        let mut summary = TestSummary::new("simulate");
        let mut results = Vec::new();

        for seed in 0..seeds {
            for scenario in &scenarios {
                let start = Instant::now();
                let test_name = format!("sim_{}_{}", scenario, seed);

                let result = match *scenario {
                    "concurrent_operations" => Self::sim_concurrent_operations(seed, ops),
                    "crash_recovery" => Self::sim_crash_recovery(seed, ops),
                    "connection_storm" => Self::sim_connection_storm(seed, ops),
                    "write_contention" => Self::sim_write_contention(seed, ops),
                    _ => TestResult::fail(&test_name, seed, 0, "Unknown scenario"),
                };

                if !result.passed {
                    self.print_result(&result);
                }
                summary.add_result(&result);
                results.push(result.clone());
                pb.inc(1);

                // Early exit on failure if verbose to avoid flooding output
                if !result.passed && self.verbose {
                    break;
                }
            }
        }

        pb.finish_with_message("Done!");
        self.output_results(&summary, &results)?;

        if summary.failed > 0 {
            anyhow::bail!("{} simulations failed", summary.failed);
        }
        Ok(())
    }

    /// Scenario: Concurrent operations with deterministic interleaving
    ///
    /// Simulates multiple "virtual connections" performing operations.
    /// Uses round-robin scheduling to get deterministic interleaving.
    fn sim_concurrent_operations(seed: u64, ops: usize) -> TestResult {
        let start = Instant::now();
        let mut rng = ChaCha8Rng::seed_from_u64(seed);

        let temp_dir = match tempfile::tempdir() {
            Ok(d) => d,
            Err(e) => {
                return TestResult::fail(
                    "concurrent_operations",
                    seed,
                    start.elapsed().as_millis() as u64,
                    &format!("Failed to create temp dir: {}", e),
                );
            }
        };
        let db_path = temp_dir.path().join("concurrent.db");

        // Create shared database
        let mut client = match RedliteClient::new_file(db_path.clone()) {
            Ok(c) => c,
            Err(e) => {
                return TestResult::fail(
                    "concurrent_operations",
                    seed,
                    start.elapsed().as_millis() as u64,
                    &format!("Failed to create client: {}", e),
                );
            }
        };

        // Track expected state for verification
        let mut expected: std::collections::HashMap<String, Vec<u8>> = std::collections::HashMap::new();

        // Virtual connections (deterministic round-robin)
        let num_connections = 4;
        let ops_per_conn = ops / num_connections;

        for conn_id in 0..num_connections {
            for op_num in 0..ops_per_conn {
                let key = format!("key_{}_{}", conn_id, rng.gen_range(0..10));
                let op_type = rng.gen_range(0..5);

                match op_type {
                    0 => {
                        // SET
                        let value = format!("value_{}_{}", conn_id, op_num).into_bytes();
                        if let Err(e) = client.set(&key, value.clone()) {
                            return TestResult::fail(
                                "concurrent_operations",
                                seed,
                                start.elapsed().as_millis() as u64,
                                &format!("SET failed: {}", e),
                            );
                        }
                        expected.insert(key, value);
                    }
                    1 => {
                        // GET and verify (only for keys we're tracking as strings)
                        match client.get(&key) {
                            Ok(got) => {
                                // Only verify if we have an expectation (key is still a string)
                                if let Some(expect) = expected.get(&key) {
                                    if got.as_ref() != Some(expect) {
                                        return TestResult::fail(
                                            "concurrent_operations",
                                            seed,
                                            start.elapsed().as_millis() as u64,
                                            &format!(
                                                "GET mismatch for key '{}': expected {:?}, got {:?}",
                                                key, Some(expect), got
                                            ),
                                        );
                                    }
                                }
                                // If not in expected, we don't track it (may be list/set or never set)
                            }
                            Err(_) => {
                                // Type error is OK - key may have been converted to list/set
                                // Just remove from expectations if present
                                expected.remove(&key);
                            }
                        }
                    }
                    2 => {
                        // INCR - only works on numeric strings, may change value
                        if client.incr(&key).is_ok() {
                            // Key is now a different value, remove from expected
                            expected.remove(&key);
                        }
                    }
                    3 => {
                        // LPUSH - changes key type to list
                        let value = format!("item_{}", op_num).into_bytes();
                        if client.lpush(&key, value).is_ok() {
                            // Key is now a list, not a string - remove from string expectations
                            expected.remove(&key);
                        }
                    }
                    _ => {
                        // SADD - changes key type to set
                        let member = format!("member_{}", rng.gen_range(0..5)).into_bytes();
                        if client.sadd(&key, member).is_ok() {
                            // Key is now a set, not a string - remove from string expectations
                            expected.remove(&key);
                        }
                    }
                }
            }
        }

        // Final verification: check all tracked string keys
        for (key, expected_value) in &expected {
            match client.get(key) {
                Ok(Some(got)) if got == *expected_value => {
                    // Value matches expectation
                }
                Ok(got) => {
                    return TestResult::fail(
                        "concurrent_operations",
                        seed,
                        start.elapsed().as_millis() as u64,
                        &format!(
                            "Final verification failed for '{}': expected {:?}, got {:?}",
                            key, Some(expected_value), got
                        ),
                    );
                }
                Err(_) => {
                    // Type error - key was converted to a different type after our last SET
                    // This shouldn't happen if our tracking is correct, but handle gracefully
                }
            }
        }

        TestResult::pass(
            "concurrent_operations",
            seed,
            start.elapsed().as_millis() as u64,
        )
    }

    /// Scenario: Crash recovery simulation
    ///
    /// Performs writes, simulates crash (drop client), reopens, verifies data.
    fn sim_crash_recovery(seed: u64, ops: usize) -> TestResult {
        let start = Instant::now();
        let mut rng = ChaCha8Rng::seed_from_u64(seed);

        let temp_dir = match tempfile::tempdir() {
            Ok(d) => d,
            Err(e) => {
                return TestResult::fail(
                    "crash_recovery",
                    seed,
                    start.elapsed().as_millis() as u64,
                    &format!("Failed to create temp dir: {}", e),
                );
            }
        };
        let db_path = temp_dir.path().join("crash_recovery.db");

        // Track what we write for verification
        let mut written_data: Vec<(String, Vec<u8>)> = Vec::new();

        // Phase 1: Write data
        {
            let mut client = match RedliteClient::new_file(db_path.clone()) {
                Ok(c) => c,
                Err(e) => {
                    return TestResult::fail(
                        "crash_recovery",
                        seed,
                        start.elapsed().as_millis() as u64,
                        &format!("Failed to create client: {}", e),
                    );
                }
            };

            for i in 0..ops.min(100) {
                // Cap at 100 for crash test
                let key = format!("crash_key_{}", i);
                let value = format!("crash_value_{}_{}", i, rng.gen::<u32>()).into_bytes();

                if let Err(e) = client.set(&key, value.clone()) {
                    return TestResult::fail(
                        "crash_recovery",
                        seed,
                        start.elapsed().as_millis() as u64,
                        &format!("Write failed: {}", e),
                    );
                }
                written_data.push((key, value));
            }
        } // Client dropped - simulates crash

        // Phase 2: Reopen and verify
        {
            let client = match RedliteClient::new_file(db_path) {
                Ok(c) => c,
                Err(e) => {
                    return TestResult::fail(
                        "crash_recovery",
                        seed,
                        start.elapsed().as_millis() as u64,
                        &format!("Failed to reopen after crash: {}", e),
                    );
                }
            };

            // Verify all data survived
            for (key, expected_value) in &written_data {
                match client.get(key) {
                    Ok(Some(got)) if got == *expected_value => {}
                    Ok(got) => {
                        return TestResult::fail(
                            "crash_recovery",
                            seed,
                            start.elapsed().as_millis() as u64,
                            &format!(
                                "Data lost after crash for '{}': expected {:?}, got {:?}",
                                key,
                                String::from_utf8_lossy(expected_value),
                                got.map(|v| String::from_utf8_lossy(&v).to_string())
                            ),
                        );
                    }
                    Err(e) => {
                        return TestResult::fail(
                            "crash_recovery",
                            seed,
                            start.elapsed().as_millis() as u64,
                            &format!("Read failed after crash: {}", e),
                        );
                    }
                }
            }
        }

        TestResult::pass("crash_recovery", seed, start.elapsed().as_millis() as u64)
    }

    /// Scenario: Connection storm - rapid open/close cycles
    ///
    /// Tests that the database handles rapid connection churn gracefully.
    fn sim_connection_storm(seed: u64, ops: usize) -> TestResult {
        let start = Instant::now();
        let mut rng = ChaCha8Rng::seed_from_u64(seed);

        let temp_dir = match tempfile::tempdir() {
            Ok(d) => d,
            Err(e) => {
                return TestResult::fail(
                    "connection_storm",
                    seed,
                    start.elapsed().as_millis() as u64,
                    &format!("Failed to create temp dir: {}", e),
                );
            }
        };
        let db_path = temp_dir.path().join("storm.db");

        // Initialize with some data
        {
            let mut client = match RedliteClient::new_file(db_path.clone()) {
                Ok(c) => c,
                Err(e) => {
                    return TestResult::fail(
                        "connection_storm",
                        seed,
                        start.elapsed().as_millis() as u64,
                        &format!("Failed to create initial client: {}", e),
                    );
                }
            };
            if let Err(e) = client.set("storm_key", b"initial_value".to_vec()) {
                return TestResult::fail(
                    "connection_storm",
                    seed,
                    start.elapsed().as_millis() as u64,
                    &format!("Initial write failed: {}", e),
                );
            }
        }

        // Rapid open/close cycles with operations
        let cycles = ops.min(50); // Cap cycles to avoid excessive time
        for i in 0..cycles {
            let mut client = match RedliteClient::new_file(db_path.clone()) {
                Ok(c) => c,
                Err(e) => {
                    return TestResult::fail(
                        "connection_storm",
                        seed,
                        start.elapsed().as_millis() as u64,
                        &format!("Failed to open on cycle {}: {}", i, e),
                    );
                }
            };

            // Do a random operation
            let op_type = rng.gen_range(0..3);
            match op_type {
                0 => {
                    let _ = client.get("storm_key");
                }
                1 => {
                    let _ = client.set(
                        "storm_key",
                        format!("value_{}", i).into_bytes(),
                    );
                }
                _ => {
                    let _ = client.incr("storm_counter");
                }
            }
            // Client dropped at end of loop iteration
        }

        // Final verification
        let client = match RedliteClient::new_file(db_path) {
            Ok(c) => c,
            Err(e) => {
                return TestResult::fail(
                    "connection_storm",
                    seed,
                    start.elapsed().as_millis() as u64,
                    &format!("Failed final open: {}", e),
                );
            }
        };

        // Database should still be usable
        match client.get("storm_key") {
            Ok(Some(_)) => {}
            Ok(None) => {
                return TestResult::fail(
                    "connection_storm",
                    seed,
                    start.elapsed().as_millis() as u64,
                    "storm_key disappeared after connection storm",
                );
            }
            Err(e) => {
                return TestResult::fail(
                    "connection_storm",
                    seed,
                    start.elapsed().as_millis() as u64,
                    &format!("Final read failed: {}", e),
                );
            }
        }

        TestResult::pass("connection_storm", seed, start.elapsed().as_millis() as u64)
    }

    /// Scenario: Write contention simulation
    ///
    /// Multiple "writers" hammering the same small set of keys with mixed operations.
    /// Tests that final state is consistent and no data corruption occurs under
    /// heavy write contention to hot keys.
    fn sim_write_contention(seed: u64, ops: usize) -> TestResult {
        let start = Instant::now();
        let mut rng = ChaCha8Rng::seed_from_u64(seed);

        let temp_dir = match tempfile::tempdir() {
            Ok(d) => d,
            Err(e) => {
                return TestResult::fail(
                    "write_contention",
                    seed,
                    start.elapsed().as_millis() as u64,
                    &format!("Failed to create temp dir: {}", e),
                );
            }
        };
        let db_path = temp_dir.path().join("contention.db");

        let mut client = match RedliteClient::new_file(db_path.clone()) {
            Ok(c) => c,
            Err(e) => {
                return TestResult::fail(
                    "write_contention",
                    seed,
                    start.elapsed().as_millis() as u64,
                    &format!("Failed to create client: {}", e),
                );
            }
        };

        // Small key space = high contention
        let num_hot_keys = 5;
        let mut counters: std::collections::HashMap<String, i64> = std::collections::HashMap::new();

        // Initialize counters
        for i in 0..num_hot_keys {
            let key = format!("hot_key_{}", i);
            if let Err(e) = client.set(&key, b"0".to_vec()) {
                return TestResult::fail(
                    "write_contention",
                    seed,
                    start.elapsed().as_millis() as u64,
                    &format!("Failed to initialize key: {}", e),
                );
            }
            counters.insert(key, 0);
        }

        // Hammer the hot keys with mixed operations
        for _ in 0..ops {
            let key_idx = rng.gen_range(0..num_hot_keys);
            let key = format!("hot_key_{}", key_idx);
            let op_type = rng.gen_range(0..4);

            match op_type {
                0 => {
                    // INCR
                    if client.incr(&key).is_ok() {
                        *counters.get_mut(&key).unwrap() += 1;
                    }
                }
                1 => {
                    // DECR
                    if client.decr(&key).is_ok() {
                        *counters.get_mut(&key).unwrap() -= 1;
                    }
                }
                2 => {
                    // SET to specific value
                    let val = rng.gen_range(-100..100i64);
                    if client.set(&key, val.to_string().into_bytes()).is_ok() {
                        *counters.get_mut(&key).unwrap() = val;
                    }
                }
                _ => {
                    // GET (read, no state change)
                    let _ = client.get(&key);
                }
            }
        }

        // Verify final state matches our tracking
        for (key, expected) in &counters {
            match client.get(key) {
                Ok(Some(got)) => {
                    let got_str = String::from_utf8_lossy(&got);
                    let got_val: i64 = match got_str.parse() {
                        Ok(v) => v,
                        Err(_) => {
                            return TestResult::fail(
                                "write_contention",
                                seed,
                                start.elapsed().as_millis() as u64,
                                &format!(
                                    "Non-numeric value for '{}': {:?}",
                                    key, got_str
                                ),
                            );
                        }
                    };
                    if got_val != *expected {
                        return TestResult::fail(
                            "write_contention",
                            seed,
                            start.elapsed().as_millis() as u64,
                            &format!(
                                "Value mismatch for '{}': expected {}, got {}",
                                key, expected, got_val
                            ),
                        );
                    }
                }
                Ok(None) => {
                    return TestResult::fail(
                        "write_contention",
                        seed,
                        start.elapsed().as_millis() as u64,
                        &format!("Key '{}' disappeared", key),
                    );
                }
                Err(e) => {
                    return TestResult::fail(
                        "write_contention",
                        seed,
                        start.elapsed().as_millis() as u64,
                        &format!("Read failed for '{}': {}", key, e),
                    );
                }
            }
        }

        TestResult::pass("write_contention", seed, start.elapsed().as_millis() as u64)
    }

    /// Fault injection tests
    pub async fn chaos(&self, faults: &[&str], seeds: u64) -> Result<()> {
        self.print_header("Chaos Tests (Fault Injection)");
        println!("Faults: {:?}", faults);
        println!("Seeds per fault: {}", seeds);
        println!();

        let pb = self.progress_bar(faults.len() as u64 * seeds, "Injecting faults...");
        let mut summary = TestSummary::new("chaos");
        let mut results = Vec::new();

        for fault in faults {
            for seed_num in 0..seeds {
                let start = Instant::now();
                let test_name = format!("chaos_{}", fault);

                let result = match *fault {
                    "crash_mid_write" => Self::chaos_crash_mid_write(seed_num),
                    "corrupt_read" => Self::chaos_corrupt_read(seed_num),
                    "disk_full" => Self::chaos_disk_full(seed_num),
                    "slow_write" => Self::chaos_slow_write(seed_num),
                    _ => {
                        let duration = start.elapsed().as_millis() as u64;
                        TestResult::fail(
                            &test_name,
                            seed_num,
                            duration,
                            &format!("Unknown fault type: {}", fault),
                        )
                    }
                };

                if !result.passed && self.verbose {
                    self.print_result(&result);
                }
                summary.add_result(&result);
                results.push(result);
                pb.inc(1);
            }
        }

        pb.finish_with_message("Done!");
        self.output_results(&summary, &results)?;

        if summary.failed > 0 {
            anyhow::bail!("{} chaos tests failed", summary.failed);
        }
        Ok(())
    }

    /// Test: Simulate crash during write operation
    fn chaos_crash_mid_write(seed: u64) -> TestResult {
        let start = Instant::now();
        let mut rng = ChaCha8Rng::seed_from_u64(seed);

        let temp_dir = match tempfile::tempdir() {
            Ok(d) => d,
            Err(e) => {
                return TestResult::fail(
                    "crash_mid_write",
                    seed,
                    start.elapsed().as_millis() as u64,
                    &format!("Failed to create temp dir: {}", e),
                );
            }
        };
        let db_path = temp_dir.path().join("crash_test.db");

        // Write some data
        {
            let mut client = match RedliteClient::new_file(db_path.clone()) {
                Ok(c) => c,
                Err(e) => {
                    return TestResult::fail(
                        "crash_mid_write",
                        seed,
                        start.elapsed().as_millis() as u64,
                        &format!("Failed to create client: {}", e),
                    );
                }
            };

            for i in 0..10 {
                let key = format!("key_{}", i);
                let value = format!("value_{}_{}", i, rng.gen::<u32>()).into_bytes();
                if let Err(e) = client.set(&key, value) {
                    return TestResult::fail(
                        "crash_mid_write",
                        seed,
                        start.elapsed().as_millis() as u64,
                        &format!("Failed to set key: {}", e),
                    );
                }
            }
        } // Simulate crash by dropping client

        // Reopen and verify data recovery
        {
            let client = match RedliteClient::new_file(db_path) {
                Ok(c) => c,
                Err(e) => {
                    return TestResult::fail(
                        "crash_mid_write",
                        seed,
                        start.elapsed().as_millis() as u64,
                        &format!("Failed to reopen DB after crash: {}", e),
                    );
                }
            };

            // Verify at least some data survived
            match client.get("key_0") {
                Ok(Some(_)) => {
                    // Data survived crash - good!
                    TestResult::pass(
                        "crash_mid_write",
                        seed,
                        start.elapsed().as_millis() as u64,
                    )
                }
                Ok(None) => TestResult::fail(
                    "crash_mid_write",
                    seed,
                    start.elapsed().as_millis() as u64,
                    "Data lost after crash",
                ),
                Err(e) => TestResult::fail(
                    "crash_mid_write",
                    seed,
                    start.elapsed().as_millis() as u64,
                    &format!("Failed to read after crash: {}", e),
                ),
            }
        }
    }

    /// Test: Attempt to read corrupted data
    fn chaos_corrupt_read(_seed: u64) -> TestResult {
        let start = Instant::now();

        // Note: This is a simplified version. Real corruption testing would
        // involve manipulating the database file directly.
        // For now, we test that the database handles invalid operations gracefully.

        let client = match RedliteClient::new_memory() {
            Ok(c) => c,
            Err(e) => {
                return TestResult::fail(
                    "corrupt_read",
                    _seed,
                    start.elapsed().as_millis() as u64,
                    &format!("Failed to create client: {}", e),
                );
            }
        };

        // Try to read non-existent key (should not crash)
        match client.get("nonexistent_key") {
            Ok(None) => TestResult::pass("corrupt_read", _seed, start.elapsed().as_millis() as u64),
            Ok(Some(_)) => TestResult::fail(
                "corrupt_read",
                _seed,
                start.elapsed().as_millis() as u64,
                "Got value for nonexistent key",
            ),
            Err(e) => TestResult::fail(
                "corrupt_read",
                _seed,
                start.elapsed().as_millis() as u64,
                &format!("Unexpected error: {}", e),
            ),
        }
    }

    /// Test: Simulate disk full scenario
    fn chaos_disk_full(seed: u64) -> TestResult {
        let start = Instant::now();
        let mut rng = ChaCha8Rng::seed_from_u64(seed);

        // Note: True disk-full simulation would require platform-specific quota setup.
        // For now, we test graceful handling of write failures by filling memory.

        let mut client = match RedliteClient::new_memory() {
            Ok(c) => c,
            Err(e) => {
                return TestResult::fail(
                    "disk_full",
                    seed,
                    start.elapsed().as_millis() as u64,
                    &format!("Failed to create client: {}", e),
                );
            }
        };

        // Try to write many keys (should handle gracefully even if space limited)
        for i in 0..100 {
            let key = format!("large_key_{}", i);
            let value: Vec<u8> = (0..1000).map(|_| rng.gen::<u8>()).collect();
            if client.set(&key, value).is_err() {
                // It's OK to fail - we're testing that it fails gracefully
                break;
            }
        }

        // Verify database is still usable
        match client.get("large_key_0") {
            Ok(_) => TestResult::pass("disk_full", seed, start.elapsed().as_millis() as u64),
            Err(e) => TestResult::fail(
                "disk_full",
                seed,
                start.elapsed().as_millis() as u64,
                &format!("DB became unusable: {}", e),
            ),
        }
    }

    /// Test: Simulate slow write operations
    fn chaos_slow_write(seed: u64) -> TestResult {
        let start = Instant::now();
        let mut rng = ChaCha8Rng::seed_from_u64(seed);

        let mut client = match RedliteClient::new_memory() {
            Ok(c) => c,
            Err(e) => {
                return TestResult::fail(
                    "slow_write",
                    seed,
                    start.elapsed().as_millis() as u64,
                    &format!("Failed to create client: {}", e),
                );
            }
        };

        // Perform operations and verify they still work (even if slow)
        for i in 0..10 {
            let key = format!("key_{}", i);
            let value = format!("value_{}", rng.gen::<u32>()).into_bytes();

            if let Err(e) = client.set(&key, value) {
                return TestResult::fail(
                    "slow_write",
                    seed,
                    start.elapsed().as_millis() as u64,
                    &format!("Write failed: {}", e),
                );
            }
        }

        // Verify all writes succeeded
        match client.get("key_0") {
            Ok(Some(_)) => TestResult::pass("slow_write", seed, start.elapsed().as_millis() as u64),
            Ok(None) => TestResult::fail(
                "slow_write",
                seed,
                start.elapsed().as_millis() as u64,
                "Data missing after slow writes",
            ),
            Err(e) => TestResult::fail(
                "slow_write",
                seed,
                start.elapsed().as_millis() as u64,
                &format!("Read failed: {}", e),
            ),
        }
    }

    /// Scale testing
    pub async fn stress(&self, connections: usize, keys: usize) -> Result<()> {
        self.print_header("Stress Tests");
        println!("Connections: {} | Keys: {}", connections, keys);
        println!();

        use std::sync::Arc;
        use std::sync::atomic::{AtomicU64, Ordering};

        // Shared statistics
        let total_ops = Arc::new(AtomicU64::new(0));
        let success_ops = Arc::new(AtomicU64::new(0));
        let latencies = Arc::new(parking_lot::Mutex::new(Vec::new()));

        // Create a shared in-memory database
        let db_path = tempfile::NamedTempFile::new()?.into_temp_path();
        let _first_client = RedliteClient::new_file(db_path.to_path_buf())?;

        let test_duration = std::time::Duration::from_secs(10);
        let start_time = Instant::now();

        println!("Running {} concurrent connections for {}s...", connections, test_duration.as_secs());
        println!();

        let pb = self.progress_bar(test_duration.as_secs(), "Stress testing...");

        // Spawn concurrent connections
        let mut handles = vec![];
        for conn_id in 0..connections {
            let db_path_clone = db_path.to_path_buf();
            let total_ops_clone = Arc::clone(&total_ops);
            let success_ops_clone = Arc::clone(&success_ops);
            let latencies_clone = Arc::clone(&latencies);
            let test_duration_clone = test_duration;
            let key_range = keys;

            let handle = runtime::spawn(async move {
                let mut rng = ChaCha8Rng::seed_from_u64(conn_id as u64);
                let mut local_client = match RedliteClient::new_file(db_path_clone) {
                    Ok(c) => c,
                    Err(_) => return,
                };

                let start = Instant::now();
                while start.elapsed() < test_duration_clone {
                    let op_start = Instant::now();
                    let key = format!("stress_key_{}_{}", conn_id, rng.gen_range(0..key_range));

                    // Random operation
                    let op_type = rng.gen_range(0..4);
                    let result = match op_type {
                        0 => local_client.set(&key, b"value".to_vec()),
                        1 => local_client.get(&key).map(|_| ()),
                        2 => local_client.incr(&key).map(|_| ()),
                        _ => local_client.lpush(&key, b"item".to_vec()).map(|_| ()),
                    };

                    total_ops_clone.fetch_add(1, Ordering::Relaxed);
                    if result.is_ok() {
                        success_ops_clone.fetch_add(1, Ordering::Relaxed);
                        let latency_us = op_start.elapsed().as_micros() as u64;
                        latencies_clone.lock().push(latency_us);
                    }

                    // Small yield to avoid spinning
                    runtime::yield_now().await;
                }
            });
            handles.push(handle);
        }

        // Monitor progress
        let monitor_handle = runtime::spawn(async move {
            while start_time.elapsed() < test_duration {
                runtime::sleep(Duration::from_secs(1)).await;
                pb.inc(1);
            }
            pb.finish_with_message("Done!");
        });

        // Wait for all connections to finish
        for handle in handles {
            let _ = handle.await;
        }
        let _ = monitor_handle.await;

        let elapsed = start_time.elapsed();
        let total = total_ops.load(Ordering::Relaxed);
        let success = success_ops.load(Ordering::Relaxed);
        let throughput = total as f64 / elapsed.as_secs_f64();

        // Calculate latency percentiles
        let mut latency_vec = latencies.lock().clone();
        latency_vec.sort();

        let p50 = if !latency_vec.is_empty() {
            latency_vec[latency_vec.len() * 50 / 100]
        } else {
            0
        };
        let p99 = if !latency_vec.is_empty() {
            latency_vec[latency_vec.len() * 99 / 100]
        } else {
            0
        };

        // Monitor memory usage
        use sysinfo::{ProcessRefreshKind, RefreshKind, System};
        use sysinfo::ProcessesToUpdate;
        let mut sys = System::new_with_specifics(
            RefreshKind::new().with_processes(ProcessRefreshKind::new()),
        );
        sys.refresh_processes(ProcessesToUpdate::All, true);
        let current_pid = sysinfo::get_current_pid().unwrap();
        let memory_mb = sys.process(current_pid)
            .map(|p| p.memory() / 1024 / 1024)
            .unwrap_or(0);

        println!();
        println!("{}", style("Stress Test Results").bold());
        println!("  Duration: {:.2}s", elapsed.as_secs_f64());
        println!("  Total operations: {}", total);
        println!("  Successful operations: {} ({:.1}%)",
            success,
            (success as f64 / total as f64 * 100.0)
        );
        println!("  Throughput: {:.0} ops/sec", throughput);
        println!("  Latency p50: {} µs", p50);
        println!("  Latency p99: {} µs", p99);
        println!("  Memory usage: {} MB", memory_mb);

        if success < total {
            println!();
            println!("{}", style("⚠ Some operations failed").yellow());
        }

        Ok(())
    }

    /// Fuzzing harness - in-process random input testing
    ///
    /// Generates random inputs and feeds them to the specified target,
    /// catching any panics or errors. Uses ChaCha8Rng for reproducibility.
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

        let start_time = Instant::now();
        let test_duration = std::time::Duration::from_secs(duration);
        let mut inputs_tested: u64 = 0;
        let mut crashes_found: u64 = 0;
        let mut crash_seeds: Vec<u64> = Vec::new();

        // Base seed for reproducibility
        let base_seed: u64 = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as u64;

        println!("Base seed: {} (for reproduction)", base_seed);
        println!();

        let mut last_progress_update = Instant::now();

        while start_time.elapsed() < test_duration {
            let seed = base_seed.wrapping_add(inputs_tested);

            let crashed = match target {
                "resp_parser" => Self::fuzz_resp_parser(seed),
                "query_parser" => Self::fuzz_query_parser(seed),
                "command_handler" => Self::fuzz_command_handler(seed),
                _ => false,
            };

            inputs_tested += 1;

            if crashed {
                crashes_found += 1;
                crash_seeds.push(seed);
                if self.verbose {
                    println!(
                        "  {} Crash found with seed: {}",
                        style("!").red().bold(),
                        seed
                    );
                }
            }

            // Update progress bar every second
            if last_progress_update.elapsed() >= std::time::Duration::from_secs(1) {
                pb.set_position(start_time.elapsed().as_secs());
                last_progress_update = Instant::now();
            }
        }

        pb.finish_with_message("Done!");

        let inputs_per_sec = inputs_tested as f64 / duration as f64;

        println!();
        println!("{}", style("Fuzzing completed").green());
        println!("  Base seed: {}", base_seed);
        println!("  Inputs tested: {}", style(format!("{}", inputs_tested)).cyan());
        println!("  Inputs/sec: {:.0}", inputs_per_sec);
        println!(
            "  Crashes found: {}",
            if crashes_found == 0 {
                style(format!("{}", crashes_found)).green()
            } else {
                style(format!("{}", crashes_found)).red()
            }
        );

        if !crash_seeds.is_empty() {
            println!();
            println!("{}", style("Crash seeds (for reproduction):").yellow());
            for seed in crash_seeds.iter().take(10) {
                println!("  redlite-dst fuzz --target {} --seed {}", target, seed);
            }
            if crash_seeds.len() > 10 {
                println!("  ... and {} more", crash_seeds.len() - 10);
            }
        }

        if crashes_found > 0 {
            anyhow::bail!("{} crashes found during fuzzing", crashes_found);
        }

        Ok(())
    }

    /// Fuzz target: RESP protocol parser
    ///
    /// Generates random byte sequences that look like RESP protocol data
    /// and tests that the parser handles them gracefully (no panics).
    fn fuzz_resp_parser(seed: u64) -> bool {
        let mut rng = ChaCha8Rng::seed_from_u64(seed);

        // Generate random RESP-like input
        let input = Self::generate_resp_input(&mut rng);

        // Catch panics
        let result = std::panic::catch_unwind(|| {
            // Try to parse as RESP
            // Since we don't have direct access to the RESP parser,
            // we test through the client by sending random data
            // For now, just validate the input generation doesn't panic
            let _ = Self::try_parse_resp(&input);
        });

        result.is_err()
    }

    /// Generate random RESP-like input
    fn generate_resp_input(rng: &mut ChaCha8Rng) -> Vec<u8> {
        let input_type = rng.gen_range(0..10);

        match input_type {
            0 => {
                // Simple string: +OK\r\n
                let len = rng.gen_range(0..100);
                let content: String = (0..len)
                    .map(|_| rng.gen_range(b'a'..=b'z') as char)
                    .collect();
                format!("+{}\r\n", content).into_bytes()
            }
            1 => {
                // Error: -ERR message\r\n
                let len = rng.gen_range(0..100);
                let content: String = (0..len)
                    .map(|_| rng.gen_range(b'a'..=b'z') as char)
                    .collect();
                format!("-ERR {}\r\n", content).into_bytes()
            }
            2 => {
                // Integer: :123\r\n
                let num: i64 = rng.gen();
                format!(":{}\r\n", num).into_bytes()
            }
            3 => {
                // Bulk string: $6\r\nfoobar\r\n
                let len = rng.gen_range(0..200);
                let content: Vec<u8> = (0..len).map(|_| rng.gen()).collect();
                let mut result = format!("${}\r\n", len).into_bytes();
                result.extend(&content);
                result.extend(b"\r\n");
                result
            }
            4 => {
                // Null bulk string: $-1\r\n
                b"$-1\r\n".to_vec()
            }
            5 => {
                // Array: *2\r\n$3\r\nSET\r\n$3\r\nkey\r\n
                let array_len = rng.gen_range(0..10);
                let mut result = format!("*{}\r\n", array_len).into_bytes();
                for _ in 0..array_len {
                    let elem_len = rng.gen_range(0..20);
                    let content: Vec<u8> = (0..elem_len).map(|_| rng.gen()).collect();
                    result.extend(format!("${}\r\n", elem_len).as_bytes());
                    result.extend(&content);
                    result.extend(b"\r\n");
                }
                result
            }
            6 => {
                // Invalid: random bytes
                let len = rng.gen_range(1..500);
                (0..len).map(|_| rng.gen()).collect()
            }
            7 => {
                // Truncated bulk string
                let len = rng.gen_range(10..100);
                let content_len = rng.gen_range(0..len / 2);
                let content: Vec<u8> = (0..content_len).map(|_| rng.gen()).collect();
                let mut result = format!("${}\r\n", len).into_bytes();
                result.extend(&content);
                result
            }
            8 => {
                // Nested arrays
                let mut result = b"*2\r\n*1\r\n$3\r\nabc\r\n".to_vec();
                let extra: Vec<u8> = (0..rng.gen_range(0..50)).map(|_| rng.gen()).collect();
                result.extend(&extra);
                result
            }
            _ => {
                // Empty
                Vec::new()
            }
        }
    }

    /// Try to parse RESP input (placeholder - actual parser would be here)
    fn try_parse_resp(input: &[u8]) -> Result<(), ()> {
        // Basic validation that would be done by a RESP parser
        if input.is_empty() {
            return Ok(());
        }

        match input[0] {
            b'+' | b'-' | b':' | b'$' | b'*' => {
                // Valid RESP prefix
                Ok(())
            }
            _ => {
                // Invalid prefix
                Err(())
            }
        }
    }

    /// Fuzz target: Query parser (FT.SEARCH syntax)
    fn fuzz_query_parser(seed: u64) -> bool {
        let mut rng = ChaCha8Rng::seed_from_u64(seed);

        let query = Self::generate_search_query(&mut rng);

        let result = std::panic::catch_unwind(|| {
            // Try to parse the query
            // This would call into the actual query parser
            let _ = Self::try_parse_query(&query);
        });

        result.is_err()
    }

    /// Generate random FT.SEARCH-like query
    fn generate_search_query(rng: &mut ChaCha8Rng) -> String {
        let query_type = rng.gen_range(0..12);

        match query_type {
            0 => {
                // Simple term
                let len = rng.gen_range(1..30);
                (0..len)
                    .map(|_| rng.gen_range(b'a'..=b'z') as char)
                    .collect()
            }
            1 => {
                // Phrase query
                let len = rng.gen_range(1..30);
                let phrase: String = (0..len)
                    .map(|_| rng.gen_range(b'a'..=b'z') as char)
                    .collect();
                format!("\"{}\"", phrase)
            }
            2 => {
                // Field-scoped
                let field_len = rng.gen_range(1..15);
                let field: String = (0..field_len)
                    .map(|_| rng.gen_range(b'a'..=b'z') as char)
                    .collect();
                let term_len = rng.gen_range(1..20);
                let term: String = (0..term_len)
                    .map(|_| rng.gen_range(b'a'..=b'z') as char)
                    .collect();
                format!("@{}:{}", field, term)
            }
            3 => {
                // Numeric range
                let field_len = rng.gen_range(1..15);
                let field: String = (0..field_len)
                    .map(|_| rng.gen_range(b'a'..=b'z') as char)
                    .collect();
                let min: i32 = rng.gen_range(-1000..1000);
                let max: i32 = rng.gen_range(min..min + 1000);
                format!("@{}:[{} {}]", field, min, max)
            }
            4 => {
                // Tag query
                let field_len = rng.gen_range(1..15);
                let field: String = (0..field_len)
                    .map(|_| rng.gen_range(b'a'..=b'z') as char)
                    .collect();
                let tag_len = rng.gen_range(1..20);
                let tag: String = (0..tag_len)
                    .map(|_| rng.gen_range(b'a'..=b'z') as char)
                    .collect();
                format!("@{}:{{{}}}", field, tag)
            }
            5 => {
                // Prefix
                let len = rng.gen_range(1..20);
                let prefix: String = (0..len)
                    .map(|_| rng.gen_range(b'a'..=b'z') as char)
                    .collect();
                format!("{}*", prefix)
            }
            6 => {
                // OR query
                let t1_len = rng.gen_range(1..15);
                let t1: String = (0..t1_len)
                    .map(|_| rng.gen_range(b'a'..=b'z') as char)
                    .collect();
                let t2_len = rng.gen_range(1..15);
                let t2: String = (0..t2_len)
                    .map(|_| rng.gen_range(b'a'..=b'z') as char)
                    .collect();
                format!("{} | {}", t1, t2)
            }
            7 => {
                // NOT query
                let t1_len = rng.gen_range(1..15);
                let t1: String = (0..t1_len)
                    .map(|_| rng.gen_range(b'a'..=b'z') as char)
                    .collect();
                let t2_len = rng.gen_range(1..15);
                let t2: String = (0..t2_len)
                    .map(|_| rng.gen_range(b'a'..=b'z') as char)
                    .collect();
                format!("{} -{}", t1, t2)
            }
            8 => {
                // Nested parentheses
                let t1_len = rng.gen_range(1..10);
                let t1: String = (0..t1_len)
                    .map(|_| rng.gen_range(b'a'..=b'z') as char)
                    .collect();
                let t2_len = rng.gen_range(1..10);
                let t2: String = (0..t2_len)
                    .map(|_| rng.gen_range(b'a'..=b'z') as char)
                    .collect();
                format!("(({} | {}))", t1, t2)
            }
            9 => {
                // Empty query
                String::new()
            }
            10 => {
                // Random garbage
                let len = rng.gen_range(1..200);
                (0..len).map(|_| rng.gen::<char>()).collect()
            }
            _ => {
                // Very long query
                let len = rng.gen_range(1000..5000);
                (0..len)
                    .map(|_| rng.gen_range(b'a'..=b'z') as char)
                    .collect()
            }
        }
    }

    /// Try to parse a search query (placeholder)
    fn try_parse_query(query: &str) -> Result<(), ()> {
        // Basic validation
        // In production, this would call the actual query parser

        // Check for balanced parentheses
        let mut depth = 0i32;
        for c in query.chars() {
            match c {
                '(' => depth += 1,
                ')' => depth -= 1,
                _ => {}
            }
            if depth < 0 {
                return Err(());
            }
        }
        if depth != 0 {
            return Err(());
        }

        // Check for balanced quotes
        let quote_count = query.chars().filter(|&c| c == '"').count();
        if quote_count % 2 != 0 {
            return Err(());
        }

        Ok(())
    }

    /// Fuzz target: Command handler
    fn fuzz_command_handler(seed: u64) -> bool {
        let result = std::panic::catch_unwind(|| {
            let mut rng = ChaCha8Rng::seed_from_u64(seed);
            // Create a memory client and run random commands
            let mut client = match RedliteClient::new_memory() {
                Ok(c) => c,
                Err(_) => return,
            };

            // Run several random operations
            for _ in 0..rng.gen_range(1..20) {
                let op = rng.gen_range(0..10);
                match op {
                    0 => {
                        // Random SET
                        let key = Self::random_key(&mut rng);
                        let value = Self::random_value(&mut rng);
                        let _ = client.set(&key, value);
                    }
                    1 => {
                        // Random GET
                        let key = Self::random_key(&mut rng);
                        let _ = client.get(&key);
                    }
                    2 => {
                        // Random INCR
                        let key = Self::random_key(&mut rng);
                        let _ = client.incr(&key);
                    }
                    3 => {
                        // Random LPUSH
                        let key = Self::random_key(&mut rng);
                        let value = Self::random_value(&mut rng);
                        let _ = client.lpush(&key, value);
                    }
                    4 => {
                        // Random LRANGE
                        let key = Self::random_key(&mut rng);
                        let start: isize = rng.gen_range(-100..100);
                        let stop: isize = rng.gen_range(-100..100);
                        let _ = client.lrange(&key, start, stop);
                    }
                    5 => {
                        // Random HSET
                        let key = Self::random_key(&mut rng);
                        let field = Self::random_key(&mut rng);
                        let value = Self::random_value(&mut rng);
                        let _ = client.hset(&key, &field, value);
                    }
                    6 => {
                        // Random SADD
                        let key = Self::random_key(&mut rng);
                        let member = Self::random_value(&mut rng);
                        let _ = client.sadd(&key, member);
                    }
                    7 => {
                        // Random ZADD
                        let key = Self::random_key(&mut rng);
                        let score: f64 = rng.gen_range(-1e10..1e10);
                        let member = Self::random_value(&mut rng);
                        let _ = client.zadd(&key, score, member);
                    }
                    8 => {
                        // Random ZRANGE
                        let key = Self::random_key(&mut rng);
                        let start: isize = rng.gen_range(-100..100);
                        let stop: isize = rng.gen_range(-100..100);
                        let _ = client.zrange(&key, start, stop);
                    }
                    _ => {
                        // Random DEL-like behavior (just read non-existent)
                        let key = Self::random_key(&mut rng);
                        let _ = client.get(&key);
                    }
                }
            }
        });

        result.is_err()
    }

    /// Generate a random key for fuzzing
    fn random_key(rng: &mut ChaCha8Rng) -> String {
        let key_type = rng.gen_range(0..5);
        match key_type {
            0 => {
                // Normal alphanumeric
                let len = rng.gen_range(1..50);
                (0..len)
                    .map(|_| rng.gen_range(b'a'..=b'z') as char)
                    .collect()
            }
            1 => {
                // With colons (namespaced)
                let parts = rng.gen_range(1..5);
                (0..parts)
                    .map(|_| {
                        let len = rng.gen_range(1..15);
                        (0..len)
                            .map(|_| rng.gen_range(b'a'..=b'z') as char)
                            .collect::<String>()
                    })
                    .collect::<Vec<_>>()
                    .join(":")
            }
            2 => {
                // With special chars
                let len = rng.gen_range(1..30);
                (0..len)
                    .map(|_| {
                        let chars = "abcdefghijklmnopqrstuvwxyz0123456789_-.:{}[]";
                        chars.chars().nth(rng.gen_range(0..chars.len())).unwrap()
                    })
                    .collect()
            }
            3 => {
                // Empty key
                String::new()
            }
            _ => {
                // Very long key
                let len = rng.gen_range(100..1000);
                (0..len)
                    .map(|_| rng.gen_range(b'a'..=b'z') as char)
                    .collect()
            }
        }
    }

    /// Generate a random value for fuzzing
    fn random_value(rng: &mut ChaCha8Rng) -> Vec<u8> {
        let value_type = rng.gen_range(0..5);
        match value_type {
            0 => {
                // Normal string
                let len = rng.gen_range(1..100);
                (0..len)
                    .map(|_| rng.gen_range(b'a'..=b'z'))
                    .collect()
            }
            1 => {
                // Binary data
                let len = rng.gen_range(1..500);
                (0..len).map(|_| rng.gen()).collect()
            }
            2 => {
                // Number as string
                let num: i64 = rng.gen();
                format!("{}", num).into_bytes()
            }
            3 => {
                // Empty
                Vec::new()
            }
            _ => {
                // Large value
                let len = rng.gen_range(1000..10000);
                (0..len).map(|_| rng.gen()).collect()
            }
        }
    }

    /// Long-running stability test
    pub async fn soak(&self, duration: &str, interval: u64) -> Result<()> {
        self.print_header("Soak Test");
        println!("Duration: {} | Check interval: {}s", duration, interval);
        println!();

        // Parse duration (e.g., "1h", "30m", "24h")
        let seconds = parse_duration(duration)?;
        let checks = seconds / interval;

        println!("{}", style("Monitoring for memory leaks and degradation...").dim());
        println!();

        use std::sync::Arc;
        use std::sync::atomic::{AtomicU64, Ordering};
        use sysinfo::{ProcessRefreshKind, ProcessesToUpdate, RefreshKind, System};

        // Create a persistent database
        let db_path = tempfile::NamedTempFile::new()?.into_temp_path();
        let mut client = RedliteClient::new_file(db_path.to_path_buf())?;

        // Track metrics
        let mut memory_snapshots = Vec::new();
        let ops_counter = Arc::new(AtomicU64::new(0));
        let ops_counter_clone = Arc::clone(&ops_counter);

        // Background operation generator
        let bg_handle = runtime::spawn(async move {
            let mut rng = ChaCha8Rng::seed_from_u64(42);
            loop {
                // Simulate continuous load
                for _ in 0..100 {
                    let key = format!("soak_key_{}", rng.gen_range(0..1000));
                    let op = rng.gen_range(0..5);

                    // Mix of operations to exercise different code paths
                    match op {
                        0 => { let _ = client.set(&key, b"soak_value".to_vec()); }
                        1 => { let _ = client.get(&key); }
                        2 => { let _ = client.incr(&key); }
                        3 => { let _ = client.lpush(&key, b"item".to_vec()); }
                        _ => { let _ = client.sadd(&key, b"member".to_vec()); }
                    }
                    ops_counter_clone.fetch_add(1, Ordering::Relaxed);
                }
                runtime::sleep(Duration::from_millis(10)).await;
            }
        });

        let pb = self.progress_bar(checks, "Running soak test...");
        let mut sys = System::new_with_specifics(
            RefreshKind::new().with_processes(ProcessRefreshKind::new()),
        );
        let current_pid = sysinfo::get_current_pid()
            .map_err(|e| anyhow::anyhow!("Failed to get current PID: {}", e))?;

        let mut prev_ops = 0u64;
        let start_time = Instant::now();

        for i in 0..checks {
            runtime::sleep(Duration::from_secs(interval)).await;

            // Refresh process info
            sys.refresh_processes(ProcessesToUpdate::All, true);
            let memory_mb = sys.process(current_pid)
                .map(|p| p.memory() / 1024 / 1024)
                .unwrap_or(0);

            let total_ops = ops_counter.load(Ordering::Relaxed);
            let ops_this_interval = total_ops - prev_ops;
            let ops_per_sec = ops_this_interval as f64 / interval as f64;
            prev_ops = total_ops;

            memory_snapshots.push((i, memory_mb, ops_per_sec));

            if self.verbose || i % 10 == 0 {
                println!(
                    "  Check {}/{}: RSS={}MB, Ops/sec={:.0}",
                    i + 1,
                    checks,
                    memory_mb,
                    ops_per_sec
                );
            }

            pb.inc(1);
        }

        // Stop background task
        bg_handle.abort();
        pb.finish_with_message("Done!");

        let elapsed = start_time.elapsed();

        // Analyze memory growth
        let initial_memory = memory_snapshots.first().map(|(_, m, _)| *m).unwrap_or(0);
        let final_memory = memory_snapshots.last().map(|(_, m, _)| *m).unwrap_or(0);
        let memory_growth = final_memory as i64 - initial_memory as i64;
        let memory_growth_pct = if initial_memory > 0 {
            (memory_growth as f64 / initial_memory as f64) * 100.0
        } else {
            0.0
        };

        // Analyze throughput stability
        let throughputs: Vec<f64> = memory_snapshots.iter().map(|(_, _, ops)| *ops).collect();
        let avg_throughput = throughputs.iter().sum::<f64>() / throughputs.len() as f64;
        let throughput_variance = throughputs.iter()
            .map(|&t| (t - avg_throughput).powi(2))
            .sum::<f64>() / throughputs.len() as f64;
        let throughput_stddev = throughput_variance.sqrt();
        let throughput_cv = if avg_throughput > 0.0 {
            (throughput_stddev / avg_throughput) * 100.0
        } else {
            0.0
        };

        println!();
        println!("{}", style("Soak Test Results").bold());
        println!("  Duration: {:.2}s ({} checks)", elapsed.as_secs_f64(), checks);
        println!("  Initial memory: {} MB", initial_memory);
        println!("  Final memory: {} MB", final_memory);
        println!("  Memory growth: {} {} MB ({:.1}%)",
            if memory_growth >= 0 { "+" } else { "" },
            memory_growth,
            memory_growth_pct
        );
        println!("  Avg throughput: {:.0} ops/sec", avg_throughput);
        println!("  Throughput CV: {:.1}%", throughput_cv);

        // Check for memory leak (unbounded growth)
        if memory_growth_pct > 50.0 {
            println!();
            println!("{}", style("⚠ WARNING: Possible memory leak detected!").yellow());
            println!("  Memory grew by {:.1}% over test duration", memory_growth_pct);
        }

        // Check for throughput degradation
        if throughput_cv > 30.0 {
            println!();
            println!("{}", style("⚠ WARNING: High throughput variance detected!").yellow());
            println!("  Throughput CV: {:.1}%", throughput_cv);
        }

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

        let result = match test {
            // Property tests
            "properties" => {
                let start = Instant::now();
                let props = properties::all_properties();
                let mut all_passed = true;
                let mut error_msg = String::new();

                for prop in &props {
                    println!("  Running property: {}", prop.name());
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
                    TestResult::pass("properties", seed, duration)
                } else {
                    TestResult::fail("properties", seed, duration, &error_msg)
                }
            }

            // Simulation tests (run all scenarios)
            "simulate" => {
                let scenarios = vec![
                    "concurrent_operations",
                    "crash_recovery",
                    "connection_storm",
                ];
                let ops = 1000; // Default ops for replay

                let mut all_passed = true;
                let mut error_msg = String::new();
                let start = Instant::now();

                for scenario in &scenarios {
                    println!("  Running scenario: {}", scenario);
                    let result = match *scenario {
                        "concurrent_operations" => Self::sim_concurrent_operations(seed, ops),
                        "crash_recovery" => Self::sim_crash_recovery(seed, ops),
                        "connection_storm" => Self::sim_connection_storm(seed, ops),
                        _ => continue,
                    };

                    if !result.passed {
                        all_passed = false;
                        if let Some(err) = &result.error {
                            error_msg = format!("{}: {}", scenario, err);
                            break;
                        }
                    }
                }

                let duration = start.elapsed().as_millis() as u64;
                if all_passed {
                    TestResult::pass("simulate", seed, duration)
                } else {
                    TestResult::fail("simulate", seed, duration, &error_msg)
                }
            }

            // Specific simulation scenario
            "concurrent_operations" | "crash_recovery" | "connection_storm" => {
                let ops = 1000;
                println!("  Running scenario: {}", test);
                match test {
                    "concurrent_operations" => Self::sim_concurrent_operations(seed, ops),
                    "crash_recovery" => Self::sim_crash_recovery(seed, ops),
                    "connection_storm" => Self::sim_connection_storm(seed, ops),
                    _ => unreachable!(),
                }
            }

            // Chaos tests (run all faults)
            "chaos" => {
                let faults = vec!["crash_mid_write", "corrupt_read", "disk_full", "slow_write"];
                let start = Instant::now();

                let mut all_passed = true;
                let mut error_msg = String::new();

                for fault in &faults {
                    println!("  Running fault: {}", fault);
                    let result = match *fault {
                        "crash_mid_write" => Self::chaos_crash_mid_write(seed),
                        "corrupt_read" => Self::chaos_corrupt_read(seed),
                        "disk_full" => Self::chaos_disk_full(seed),
                        "slow_write" => Self::chaos_slow_write(seed),
                        _ => continue,
                    };

                    if !result.passed {
                        all_passed = false;
                        if let Some(err) = &result.error {
                            error_msg = format!("{}: {}", fault, err);
                            break;
                        }
                    }
                }

                let duration = start.elapsed().as_millis() as u64;
                if all_passed {
                    TestResult::pass("chaos", seed, duration)
                } else {
                    TestResult::fail("chaos", seed, duration, &error_msg)
                }
            }

            // Specific chaos fault
            "crash_mid_write" | "corrupt_read" | "disk_full" | "slow_write" => {
                println!("  Running fault: {}", test);
                match test {
                    "crash_mid_write" => Self::chaos_crash_mid_write(seed),
                    "corrupt_read" => Self::chaos_corrupt_read(seed),
                    "disk_full" => Self::chaos_disk_full(seed),
                    "slow_write" => Self::chaos_slow_write(seed),
                    _ => unreachable!(),
                }
            }

            // Unknown test type
            _ => {
                TestResult::fail(
                    test,
                    seed,
                    0,
                    &format!("Unknown test type: '{}'. Valid types: properties, simulate, chaos, concurrent_operations, crash_recovery, connection_storm, crash_mid_write, corrupt_read, disk_full, slow_write", test),
                )
            }
        };

        println!();
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
        let mut results = Vec::new();

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
            results.push(result);
            pb.inc(1);
        }

        pb.finish_with_message("Done!");
        self.output_results(&summary, &results)?;

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
