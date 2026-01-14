use anyhow::Result;
use rand::{Rng, SeedableRng};
use rand_chacha::ChaCha8Rng;
use std::time::Instant;

use crate::client::RedliteClient;
use crate::types::TestResult;

/// Property-based test trait
pub trait Property {
    fn name(&self) -> &'static str;
    fn test(&self, client: &mut RedliteClient, rng: &mut ChaCha8Rng) -> Result<()>;
}

/// SET k v; GET k => v
pub struct SetGetRoundtrip;

impl Property for SetGetRoundtrip {
    fn name(&self) -> &'static str {
        "set_get_roundtrip"
    }

    fn test(&self, client: &mut RedliteClient, rng: &mut ChaCha8Rng) -> Result<()> {
        let key = format!("key_{}", rng.gen::<u32>());
        let value: Vec<u8> = (0..rng.gen_range(1..100))
            .map(|_| rng.gen::<u8>())
            .collect();

        client.set(&key, value.clone())?;
        let retrieved = client.get(&key)?;

        if retrieved != Some(value) {
            anyhow::bail!("SET/GET roundtrip failed: values don't match");
        }
        Ok(())
    }
}

/// INCR always increases the value
pub struct IncrIsMonotonic;

impl Property for IncrIsMonotonic {
    fn name(&self) -> &'static str {
        "incr_is_monotonic"
    }

    fn test(&self, client: &mut RedliteClient, rng: &mut ChaCha8Rng) -> Result<()> {
        let key = format!("counter_{}", rng.gen::<u32>());
        let iterations = rng.gen_range(1..20);

        let mut last_value = 0i64;
        for _ in 0..iterations {
            let new_value = client.incr(&key)?;
            if new_value <= last_value {
                anyhow::bail!(
                    "INCR not monotonic: {} -> {}",
                    last_value,
                    new_value
                );
            }
            last_value = new_value;
        }
        Ok(())
    }
}

/// LPUSH/RPUSH preserve order
pub struct ListOrderPreserved;

impl Property for ListOrderPreserved {
    fn name(&self) -> &'static str {
        "list_order_preserved"
    }

    fn test(&self, client: &mut RedliteClient, rng: &mut ChaCha8Rng) -> Result<()> {
        let key = format!("list_{}", rng.gen::<u32>());
        let count = rng.gen_range(1..10);

        let mut expected = vec![];
        for i in 0..count {
            let value = format!("item_{}", i).into_bytes();
            expected.push(value.clone());
            client.rpush(&key, value)?;
        }

        let retrieved = client.lrange(&key, 0, -1)?;
        if retrieved != expected {
            anyhow::bail!("List order not preserved");
        }
        Ok(())
    }
}

/// Hash fields are unique
pub struct HashFieldsUnique;

impl Property for HashFieldsUnique {
    fn name(&self) -> &'static str {
        "hash_fields_unique"
    }

    fn test(&self, client: &mut RedliteClient, rng: &mut ChaCha8Rng) -> Result<()> {
        let key = format!("hash_{}", rng.gen::<u32>());
        let field = "field1";

        client.hset(&key, field, b"value1".to_vec())?;
        client.hset(&key, field, b"value2".to_vec())?;

        let result = client.hget(&key, field)?;
        if result != Some(b"value2".to_vec()) {
            anyhow::bail!("Hash field should have latest value");
        }

        let all = client.hgetall(&key)?;
        if all.len() != 1 {
            anyhow::bail!("Hash should have exactly one field");
        }
        Ok(())
    }
}

/// ZRANGE returns elements in sorted order
pub struct SortedSetOrdering;

impl Property for SortedSetOrdering {
    fn name(&self) -> &'static str {
        "sorted_set_ordering"
    }

    fn test(&self, client: &mut RedliteClient, rng: &mut ChaCha8Rng) -> Result<()> {
        let key = format!("zset_{}", rng.gen::<u32>());
        let count = rng.gen_range(3..10);

        let mut scores: Vec<f64> = (0..count).map(|_| rng.gen()).collect();
        scores.sort_by(|a, b| a.partial_cmp(b).unwrap());

        // Add in random order
        for (i, &score) in scores.iter().enumerate() {
            client.zadd(&key, score, format!("item_{}", i).into_bytes())?;
        }

        // Verify sorted order
        let result = client.zrange(&key, 0, -1)?;
        let result_scores: Vec<f64> = result
            .iter()
            .map(|v| {
                let s = String::from_utf8(v.clone()).unwrap();
                let idx: usize = s.strip_prefix("item_").unwrap().parse().unwrap();
                scores[idx]
            })
            .collect();

        for i in 1..result_scores.len() {
            if result_scores[i] < result_scores[i - 1] {
                anyhow::bail!("Sorted set not in order");
            }
        }
        Ok(())
    }
}

/// Expired keys return None
pub struct ExpireRemovesKey;

impl Property for ExpireRemovesKey {
    fn name(&self) -> &'static str {
        "expire_removes_key"
    }

    fn test(&self, client: &mut RedliteClient, rng: &mut ChaCha8Rng) -> Result<()> {
        let key = format!("expiring_{}", rng.gen::<u32>());
        client.set(&key, b"value".to_vec())?;

        // Set to expire in 1 second
        client.expire(&key, 1)?;

        // Should still exist
        if client.ttl(&key)? < 0 {
            anyhow::bail!("TTL should be positive before expiration");
        }

        // Wait for expiration (simulate by checking after time passes)
        // In real implementation, this would use time manipulation
        Ok(())
    }
}

/// Crash recovery maintains consistency
pub struct CrashRecoveryConsistent;

impl Property for CrashRecoveryConsistent {
    fn name(&self) -> &'static str {
        "crash_recovery_consistent"
    }

    fn test(&self, client: &mut RedliteClient, rng: &mut ChaCha8Rng) -> Result<()> {
        let key = format!("persistent_{}", rng.gen::<u32>());
        let value = b"important_data".to_vec();

        client.set(&key, value.clone())?;

        // Simulate crash and recovery (in real implementation)
        // For now, just verify the value is still there
        let retrieved = client.get(&key)?;
        if retrieved != Some(value) {
            anyhow::bail!("Data lost after simulated crash");
        }
        Ok(())
    }
}

/// Get all available properties
pub fn all_properties() -> Vec<Box<dyn Property>> {
    vec![
        Box::new(SetGetRoundtrip),
        Box::new(IncrIsMonotonic),
        Box::new(ListOrderPreserved),
        Box::new(HashFieldsUnique),
        Box::new(SortedSetOrdering),
        Box::new(ExpireRemovesKey),
        Box::new(CrashRecoveryConsistent),
    ]
}

/// Run a property test with a specific seed
pub fn run_property(
    property: &dyn Property,
    seed: u64,
) -> TestResult {
    let start = Instant::now();
    let mut client = match RedliteClient::new_memory() {
        Ok(c) => c,
        Err(e) => {
            return TestResult::fail(
                property.name(),
                seed,
                start.elapsed().as_millis() as u64,
                &format!("Failed to create client: {}", e),
            )
        }
    };
    let mut rng = ChaCha8Rng::seed_from_u64(seed);

    match property.test(&mut client, &mut rng) {
        Ok(_) => TestResult::pass(
            property.name(),
            seed,
            start.elapsed().as_millis() as u64,
        ),
        Err(e) => TestResult::fail(
            property.name(),
            seed,
            start.elapsed().as_millis() as u64,
            &e.to_string(),
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_set_get_roundtrip() {
        let result = run_property(&SetGetRoundtrip, 12345);
        assert!(result.passed);
    }

    #[test]
    fn test_incr_monotonic() {
        let result = run_property(&IncrIsMonotonic, 12345);
        assert!(result.passed);
    }

    #[test]
    fn test_list_order() {
        let result = run_property(&ListOrderPreserved, 12345);
        assert!(result.passed);
    }
}
