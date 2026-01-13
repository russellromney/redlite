//! Workload scenario management - loads and manages mixed operation workloads from YAML
//!
//! A scenario defines a mix of operations with weighted distributions, enabling
//! realistic workload simulation (e.g., "80% read, 20% write caching pattern")

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use crate::error::Result;

/// A single operation in a scenario with its relative weight
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct OperationWeight {
    #[serde(rename = "type")]
    pub operation: String,  // "GET", "SET", "HGET", "LPUSH", etc.
    pub weight: f64,
}

/// A workload scenario - a mix of operations with relative weights
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct WorkloadScenario {
    pub name: String,
    pub description: Option<String>,
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_weight_normalization() {
        let scenario = WorkloadScenario {
            name: "test".to_string(),
            description: None,
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
