//! Workload scenario management - loads and manages mixed operation workloads from YAML
//!
//! A scenario defines a mix of operations with weighted distributions, enabling
//! realistic workload simulation (e.g., "80% read, 20% write caching pattern")
//!
//! Scenarios can specify setup requirements for pre-populating data.

use serde::{Deserialize, Serialize};
use crate::error::Result;
use crate::client::RedisLikeClient;

/// A single operation in a scenario with its relative weight
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct OperationWeight {
    #[serde(rename = "type")]
    pub operation: String,  // "GET", "SET", "HGET", "LPUSH", etc.
    pub weight: f64,
}

/// Setup configuration for strings (SET keys)
#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct StringSetup {
    pub count: usize,
    pub value_size: usize,
}

/// Setup configuration for lists (LPUSH)
#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct ListSetup {
    pub count: usize,
    pub items_per_list: usize,
}

/// Setup configuration for hashes (HSET)
#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct HashSetup {
    pub count: usize,
    pub fields_per_hash: usize,
}

/// Setup configuration for sets (SADD)
#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct SetSetup {
    pub count: usize,
    pub members_per_set: usize,
}

/// Setup configuration for sorted sets (ZADD)
#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct SortedSetSetup {
    pub count: usize,
    pub members_per_set: usize,
}

/// Setup configuration for streams (XADD)
#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct StreamSetup {
    pub count: usize,
    pub entries_per_stream: usize,
}

/// Setup configuration for counters (SET to "0")
#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct CounterSetup {
    pub count: usize,
}

/// Complete setup specification for a scenario
#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct ScenarioSetup {
    #[serde(default)]
    pub strings: Option<StringSetup>,
    #[serde(default)]
    pub lists: Option<ListSetup>,
    #[serde(default)]
    pub hashes: Option<HashSetup>,
    #[serde(default)]
    pub sets: Option<SetSetup>,
    #[serde(default)]
    pub sorted_sets: Option<SortedSetSetup>,
    #[serde(default)]
    pub streams: Option<StreamSetup>,
    #[serde(default)]
    pub counters: Option<CounterSetup>,
}

/// A workload scenario - a mix of operations with relative weights
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct WorkloadScenario {
    pub name: String,
    pub description: Option<String>,
    #[serde(default)]
    pub setup: Option<ScenarioSetup>,
    pub operations: Vec<OperationWeight>,
}

impl WorkloadScenario {
    /// Normalize weights into cumulative probabilities [0.0, 1.0]
    /// Returns (operation_name, cumulative_probability) pairs
    pub fn normalized_operations(&self) -> Vec<(String, f64)> {
        if self.operations.is_empty() {
            return vec![];
        }

        let total_weight: f64 = self.operations.iter().map(|op| op.weight).sum();
        if total_weight <= 0.0 {
            return vec![];
        }

        let mut cumulative = 0.0;
        self.operations
            .iter()
            .map(|op| {
                cumulative += op.weight / total_weight;
                (op.operation.clone(), cumulative)
            })
            .collect()
    }

    /// Select an operation based on normalized weights
    /// Returns operation name (e.g., "GET", "SET")
    pub fn select_operation(&self, normalized: &[(String, f64)], random_value: f64) -> Option<String> {
        for (op_name, cumulative_prob) in normalized {
            if random_value <= *cumulative_prob {
                return Some(op_name.clone());
            }
        }
        normalized.last().map(|(op_name, _)| op_name.clone())
    }
}

/// Container for all available workload scenarios
#[derive(Debug, Deserialize)]
struct ScenarioSpec {
    workloads: Vec<WorkloadScenario>,
}

/// Load workload scenarios from YAML file
pub fn load_scenarios(path: &str) -> Result<Vec<WorkloadScenario>> {
    let content = std::fs::read_to_string(path)
        .map_err(|e| crate::error::BenchError::Io(e))?;

    let spec: ScenarioSpec = serde_yaml::from_str(&content)
        .map_err(|e| crate::error::BenchError::Serialization(format!("Failed to parse YAML: {}", e)))?;

    Ok(spec.workloads)
}

/// Lookup a scenario by name
pub fn find_scenario(scenarios: &[WorkloadScenario], name: &str) -> Option<WorkloadScenario> {
    scenarios.iter().find(|s| s.name == name).cloned()
}

// ========== SETUP EXECUTION ==========

/// Generate a value of the specified size
fn generate_value(size: usize) -> Vec<u8> {
    let mut value = Vec::with_capacity(size);
    value.extend_from_slice(b"v_");
    value.extend(std::iter::repeat(b'x').take(size.saturating_sub(2)));
    value
}

/// Execute setup for a scenario - pre-populates all required data
/// Uses bulk operations (MSET, multi-value LPUSH, etc.) for speed
pub async fn execute_setup<C: RedisLikeClient>(client: &C, setup: &ScenarioSetup) -> Result<SetupStats> {
    let mut stats = SetupStats::default();
    let start = std::time::Instant::now();

    // Clear existing data
    client.flushdb().await?;

    // Setup strings using MSET in batches
    if let Some(ref string_setup) = setup.strings {
        stats.strings = setup_strings(client, string_setup).await?;
    }

    // Setup counters using MSET
    if let Some(ref counter_setup) = setup.counters {
        stats.counters = setup_counters(client, counter_setup).await?;
    }

    // Setup lists using LPUSH with multiple values
    if let Some(ref list_setup) = setup.lists {
        stats.lists = setup_lists(client, list_setup).await?;
    }

    // Setup hashes using HSET with multiple fields
    if let Some(ref hash_setup) = setup.hashes {
        stats.hashes = setup_hashes(client, hash_setup).await?;
    }

    // Setup sets using SADD with multiple members
    if let Some(ref set_setup) = setup.sets {
        stats.sets = setup_sets(client, set_setup).await?;
    }

    // Setup sorted sets using ZADD with multiple members
    if let Some(ref zset_setup) = setup.sorted_sets {
        stats.sorted_sets = setup_sorted_sets(client, zset_setup).await?;
    }

    // Setup streams using XADD (no bulk option)
    if let Some(ref stream_setup) = setup.streams {
        stats.streams = setup_streams(client, stream_setup).await?;
    }

    stats.total_duration_ms = start.elapsed().as_millis() as u64;
    Ok(stats)
}

/// Statistics from setup execution
#[derive(Debug, Default)]
pub struct SetupStats {
    pub strings: usize,
    pub counters: usize,
    pub lists: usize,
    pub hashes: usize,
    pub sets: usize,
    pub sorted_sets: usize,
    pub streams: usize,
    pub total_duration_ms: u64,
}

impl SetupStats {
    pub fn total_keys(&self) -> usize {
        self.strings + self.counters + self.lists + self.hashes +
        self.sets + self.sorted_sets + self.streams
    }
}

/// Setup strings using MSET in batches of 1000
async fn setup_strings<C: RedisLikeClient>(client: &C, setup: &StringSetup) -> Result<usize> {
    if setup.count == 0 {
        return Ok(0);
    }

    let value = generate_value(setup.value_size);
    const BATCH_SIZE: usize = 1000;

    for batch_start in (0..setup.count).step_by(BATCH_SIZE) {
        let batch_end = (batch_start + BATCH_SIZE).min(setup.count);
        let mut pairs: Vec<(String, Vec<u8>)> = Vec::with_capacity(batch_end - batch_start);

        for i in batch_start..batch_end {
            pairs.push((format!("key_{}", i), value.clone()));
        }

        // Convert to references for mset
        let refs: Vec<(&str, &[u8])> = pairs.iter()
            .map(|(k, v)| (k.as_str(), v.as_slice()))
            .collect();
        client.mset(&refs).await?;
    }

    Ok(setup.count)
}

/// Setup counters using MSET to "0"
async fn setup_counters<C: RedisLikeClient>(client: &C, setup: &CounterSetup) -> Result<usize> {
    if setup.count == 0 {
        return Ok(0);
    }

    const BATCH_SIZE: usize = 1000;

    for batch_start in (0..setup.count).step_by(BATCH_SIZE) {
        let batch_end = (batch_start + BATCH_SIZE).min(setup.count);
        let mut pairs: Vec<(String, Vec<u8>)> = Vec::with_capacity(batch_end - batch_start);

        for i in batch_start..batch_end {
            pairs.push((format!("counter_{}", i), b"0".to_vec()));
        }

        let refs: Vec<(&str, &[u8])> = pairs.iter()
            .map(|(k, v)| (k.as_str(), v.as_slice()))
            .collect();
        client.mset(&refs).await?;
    }

    Ok(setup.count)
}

/// Setup lists using LPUSH with batched values
async fn setup_lists<C: RedisLikeClient>(client: &C, setup: &ListSetup) -> Result<usize> {
    if setup.count == 0 || setup.items_per_list == 0 {
        return Ok(0);
    }

    // Generate values to push
    let values: Vec<Vec<u8>> = (0..setup.items_per_list.min(1000))
        .map(|i| format!("item_{}", i).into_bytes())
        .collect();

    // Push in batches of up to 1000 values per LPUSH
    const BATCH_SIZE: usize = 1000;

    for list_idx in 0..setup.count {
        let key = format!("list_{}", list_idx);
        let mut remaining = setup.items_per_list;

        while remaining > 0 {
            let batch = remaining.min(BATCH_SIZE);
            let value_refs: Vec<&[u8]> = values.iter()
                .take(batch)
                .map(|v| v.as_slice())
                .collect();
            client.lpush(&key, &value_refs).await?;
            remaining -= batch;
        }
    }

    Ok(setup.count)
}

/// Setup hashes using HSET with multiple field/values per call
async fn setup_hashes<C: RedisLikeClient>(client: &C, setup: &HashSetup) -> Result<usize> {
    if setup.count == 0 || setup.fields_per_hash == 0 {
        return Ok(0);
    }

    let value = generate_value(100);

    for hash_idx in 0..setup.count {
        let key = format!("hash_{}", hash_idx);
        // HSET one field at a time (could batch with HMSET but most clients do this)
        for field_idx in 0..setup.fields_per_hash {
            let field = format!("field_{}", field_idx);
            client.hset(&key, &field, &value).await?;
        }
    }

    Ok(setup.count)
}

/// Setup sets using SADD with multiple members
async fn setup_sets<C: RedisLikeClient>(client: &C, setup: &SetSetup) -> Result<usize> {
    if setup.count == 0 || setup.members_per_set == 0 {
        return Ok(0);
    }

    // Pre-generate members
    let members: Vec<Vec<u8>> = (0..setup.members_per_set.min(1000))
        .map(|i| format!("member_{}", i).into_bytes())
        .collect();

    const BATCH_SIZE: usize = 1000;

    for set_idx in 0..setup.count {
        let key = format!("set_{}", set_idx);
        let mut remaining = setup.members_per_set;
        let mut offset = 0;

        while remaining > 0 {
            let batch = remaining.min(BATCH_SIZE);
            let member_refs: Vec<&[u8]> = (0..batch)
                .map(|i| {
                    let idx = (offset + i) % members.len();
                    members[idx].as_slice()
                })
                .collect();
            client.sadd(&key, &member_refs).await?;
            remaining -= batch;
            offset += batch;
        }
    }

    Ok(setup.count)
}

/// Setup sorted sets using ZADD with multiple score/member pairs
async fn setup_sorted_sets<C: RedisLikeClient>(client: &C, setup: &SortedSetSetup) -> Result<usize> {
    if setup.count == 0 || setup.members_per_set == 0 {
        return Ok(0);
    }

    // Pre-generate members
    let members: Vec<Vec<u8>> = (0..setup.members_per_set.min(1000))
        .map(|i| format!("member_{}", i).into_bytes())
        .collect();

    const BATCH_SIZE: usize = 500; // Score + member pairs

    for zset_idx in 0..setup.count {
        let key = format!("zset_{}", zset_idx);
        let mut remaining = setup.members_per_set;
        let mut offset = 0;

        while remaining > 0 {
            let batch = remaining.min(BATCH_SIZE);
            let pairs: Vec<(f64, &[u8])> = (0..batch)
                .map(|i| {
                    let idx = (offset + i) % members.len();
                    let score = (offset + i) as f64;
                    (score, members[idx].as_slice())
                })
                .collect();
            client.zadd(&key, &pairs).await?;
            remaining -= batch;
            offset += batch;
        }
    }

    Ok(setup.count)
}

/// Setup streams using XADD (no bulk option available)
async fn setup_streams<C: RedisLikeClient>(client: &C, setup: &StreamSetup) -> Result<usize> {
    if setup.count == 0 || setup.entries_per_stream == 0 {
        return Ok(0);
    }

    let value = generate_value(100);

    for stream_idx in 0..setup.count {
        let key = format!("stream_{}", stream_idx);
        for _entry_idx in 0..setup.entries_per_stream {
            client.xadd(&key, "*", &[("data", &value[..])]).await?;
        }
    }

    Ok(setup.count)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_weight_normalization() {
        let scenario = WorkloadScenario {
            name: "test".to_string(),
            description: None,
            setup: None,
            operations: vec![
                OperationWeight { operation: "GET".to_string(), weight: 80.0 },
                OperationWeight { operation: "SET".to_string(), weight: 20.0 },
            ],
        };

        let normalized = scenario.normalized_operations();
        assert_eq!(normalized.len(), 2);

        // First operation should have cumulative prob 0.8
        assert!((normalized[0].1 - 0.8).abs() < 0.001);

        // Second operation should have cumulative prob 1.0
        assert!((normalized[1].1 - 1.0).abs() < 0.001);
    }

    #[test]
    fn test_operation_selection() {
        let scenario = WorkloadScenario {
            name: "test".to_string(),
            description: None,
            setup: None,
            operations: vec![
                OperationWeight { operation: "GET".to_string(), weight: 50.0 },
                OperationWeight { operation: "SET".to_string(), weight: 50.0 },
            ],
        };

        let normalized = scenario.normalized_operations();

        // Low random value should select first operation
        assert_eq!(
            scenario.select_operation(&normalized, 0.25).unwrap(),
            "GET"
        );

        // Mid random value should select second operation
        assert_eq!(
            scenario.select_operation(&normalized, 0.75).unwrap(),
            "SET"
        );

        // High random value should select last operation
        assert_eq!(
            scenario.select_operation(&normalized, 0.99).unwrap(),
            "SET"
        );
    }
}
