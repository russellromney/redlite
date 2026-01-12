use std::time::Duration;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i32)]
pub enum KeyType {
    String = 1,
    Hash = 2,
    List = 3,
    Set = 4,
    ZSet = 5,
    Stream = 6,
}

impl KeyType {
    pub fn from_i32(v: i32) -> Option<Self> {
        match v {
            1 => Some(KeyType::String),
            2 => Some(KeyType::Hash),
            3 => Some(KeyType::List),
            4 => Some(KeyType::Set),
            5 => Some(KeyType::ZSet),
            6 => Some(KeyType::Stream),
            _ => None,
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            KeyType::String => "string",
            KeyType::Hash => "hash",
            KeyType::List => "list",
            KeyType::Set => "set",
            KeyType::ZSet => "zset",
            KeyType::Stream => "stream",
        }
    }
}

#[derive(Debug, Clone)]
pub struct ZMember {
    pub score: f64,
    pub member: Vec<u8>,
}

impl ZMember {
    pub fn new(score: f64, member: impl Into<Vec<u8>>) -> Self {
        Self {
            score,
            member: member.into(),
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct SetOptions {
    pub ttl: Option<Duration>,
    pub nx: bool,
    pub xx: bool,
}

impl SetOptions {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn nx(mut self) -> Self {
        self.nx = true;
        self
    }

    pub fn xx(mut self) -> Self {
        self.xx = true;
        self
    }
}

/// Metadata about a key (returned by KEYINFO)
#[derive(Debug, Clone)]
pub struct KeyInfo {
    pub key_type: KeyType,
    pub ttl: i64,        // TTL in seconds (-1 if no expiry)
    pub created_at: i64, // Timestamp in milliseconds
    pub updated_at: i64, // Timestamp in milliseconds
}

/// Stream entry ID (timestamp-sequence)
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct StreamId {
    pub ms: i64,  // Timestamp in milliseconds
    pub seq: i64, // Sequence number within the same millisecond
}

impl StreamId {
    pub fn new(ms: i64, seq: i64) -> Self {
        Self { ms, seq }
    }

    /// Parse stream ID from string like "1234567890123-0" or special values
    pub fn parse(s: &str) -> Option<Self> {
        if s == "-" {
            return Some(Self::min());
        }
        if s == "+" {
            return Some(Self::max());
        }
        if s == "$" {
            // Special: means "last ID" - caller should handle this
            return None;
        }
        if s == ">" {
            // Special: means "new messages only" - caller should handle this
            return None;
        }

        let parts: Vec<&str> = s.split('-').collect();
        if parts.len() == 1 {
            // Just timestamp, seq defaults to 0
            let ms = parts[0].parse().ok()?;
            Some(Self { ms, seq: 0 })
        } else if parts.len() == 2 {
            let ms = parts[0].parse().ok()?;
            let seq = parts[1].parse().ok()?;
            Some(Self { ms, seq })
        } else {
            None
        }
    }

    pub fn min() -> Self {
        Self { ms: 0, seq: 0 }
    }

    pub fn max() -> Self {
        Self {
            ms: i64::MAX,
            seq: i64::MAX,
        }
    }

    pub fn to_string(&self) -> String {
        format!("{}-{}", self.ms, self.seq)
    }
}

impl std::fmt::Display for StreamId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}-{}", self.ms, self.seq)
    }
}

/// A stream entry with ID and field-value pairs
#[derive(Debug, Clone)]
pub struct StreamEntry {
    pub id: StreamId,
    pub fields: Vec<(Vec<u8>, Vec<u8>)>,
}

impl StreamEntry {
    pub fn new(id: StreamId, fields: Vec<(Vec<u8>, Vec<u8>)>) -> Self {
        Self { id, fields }
    }
}

/// Stream info (returned by XINFO STREAM)
#[derive(Debug, Clone)]
pub struct StreamInfo {
    pub length: i64,
    pub radix_tree_keys: i64,      // Not applicable for SQLite, return 0
    pub radix_tree_nodes: i64,     // Not applicable for SQLite, return 0
    pub last_generated_id: StreamId,
    pub first_entry: Option<StreamEntry>,
    pub last_entry: Option<StreamEntry>,
}

/// Consumer group info (returned by XINFO GROUPS)
#[derive(Debug, Clone)]
pub struct ConsumerGroupInfo {
    pub name: String,
    pub consumers: i64,      // Number of consumers in this group
    pub pending: i64,        // Number of pending entries
    pub last_delivered_id: StreamId,
}

/// Consumer info (returned by XINFO CONSUMERS)
#[derive(Debug, Clone)]
pub struct ConsumerInfo {
    pub name: String,
    pub pending: i64,        // Number of pending entries for this consumer
    pub idle: i64,           // Milliseconds since last interaction
}

/// Pending entry info (returned by XPENDING)
#[derive(Debug, Clone)]
pub struct PendingEntry {
    pub id: StreamId,
    pub consumer: String,
    pub idle: i64,           // Milliseconds since last delivery
    pub delivery_count: i64, // Number of times this entry has been delivered
}

/// Summary for XPENDING without range arguments
#[derive(Debug, Clone)]
pub struct PendingSummary {
    pub count: i64,                // Total pending entries in group
    pub smallest_id: Option<StreamId>,
    pub largest_id: Option<StreamId>,
    pub consumers: Vec<(String, i64)>,  // Consumer name -> pending count
}

/// History tracking level (three-tier opt-in)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HistoryLevel {
    /// Global history for all databases
    Global,
    /// History for specific database (0-15)
    Database(i32),
    /// History for specific key
    Key,
}

impl HistoryLevel {
    pub fn as_str(&self) -> &'static str {
        match self {
            HistoryLevel::Global => "global",
            HistoryLevel::Database(_) => "database",
            HistoryLevel::Key => "key",
        }
    }
}

/// Retention policy type for history entries
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RetentionType {
    /// Keep all history entries (no limit)
    Unlimited,
    /// Keep entries for N milliseconds (time-based retention)
    Time(i64),
    /// Keep only the last N versions (count-based retention)
    Count(i64),
}

impl RetentionType {
    pub fn as_str(&self) -> &'static str {
        match self {
            RetentionType::Unlimited => "unlimited",
            RetentionType::Time(_) => "time",
            RetentionType::Count(_) => "count",
        }
    }
}

/// History configuration for a specific level and target
#[derive(Debug, Clone)]
pub struct HistoryConfig {
    pub id: i64,
    pub level: HistoryLevel,
    pub target: String,              // '*' for global, '0-15' for db, 'db:key' for key
    pub enabled: bool,
    pub retention: RetentionType,
    pub created_at: i64,             // Timestamp in milliseconds
}

impl HistoryConfig {
    pub fn new(level: HistoryLevel, target: String, retention: RetentionType) -> Self {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as i64;

        Self {
            id: 0,
            level,
            target,
            enabled: true,
            retention,
            created_at: now,
        }
    }
}

/// A versioned history entry for a key
#[derive(Debug, Clone)]
pub struct HistoryEntry {
    pub id: i64,
    pub key_id: i64,
    pub db: i32,
    pub key: String,
    pub key_type: KeyType,
    pub version_num: i64,
    pub operation: String,           // 'SET', 'DEL', 'HSET', 'LPUSH', etc.
    pub timestamp_ms: i64,
    pub data_snapshot: Option<Vec<u8>>,  // MessagePack encoded current state
    pub expire_at: Option<i64>,      // TTL at time of operation
}

impl HistoryEntry {
    pub fn new(
        db: i32,
        key: String,
        key_type: KeyType,
        version_num: i64,
        operation: String,
        timestamp_ms: i64,
    ) -> Self {
        Self {
            id: 0,
            key_id: 0,
            db,
            key,
            key_type,
            version_num,
            operation,
            timestamp_ms,
            data_snapshot: None,
            expire_at: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_history_level_global() {
        let level = HistoryLevel::Global;
        assert_eq!(level.as_str(), "global");
        assert_eq!(level, HistoryLevel::Global);
    }

    #[test]
    fn test_history_level_database() {
        let level = HistoryLevel::Database(5);
        assert_eq!(level.as_str(), "database");
        assert_eq!(level, HistoryLevel::Database(5));
    }

    #[test]
    fn test_history_level_key() {
        let level = HistoryLevel::Key;
        assert_eq!(level.as_str(), "key");
        assert_eq!(level, HistoryLevel::Key);
    }

    #[test]
    fn test_retention_type_unlimited() {
        let retention = RetentionType::Unlimited;
        assert_eq!(retention.as_str(), "unlimited");
        assert_eq!(retention, RetentionType::Unlimited);
    }

    #[test]
    fn test_retention_type_time() {
        let retention = RetentionType::Time(2592000000); // 30 days in ms
        assert_eq!(retention.as_str(), "time");
        assert_eq!(retention, RetentionType::Time(2592000000));
    }

    #[test]
    fn test_retention_type_count() {
        let retention = RetentionType::Count(100);
        assert_eq!(retention.as_str(), "count");
        assert_eq!(retention, RetentionType::Count(100));
    }

    #[test]
    fn test_history_config_new() {
        let config = HistoryConfig::new(
            HistoryLevel::Global,
            "*".to_string(),
            RetentionType::Time(2592000000),
        );
        assert_eq!(config.level, HistoryLevel::Global);
        assert_eq!(config.target, "*");
        assert_eq!(config.retention, RetentionType::Time(2592000000));
        assert!(config.enabled);
        assert!(config.created_at > 0);
    }

    #[test]
    fn test_history_config_database() {
        let config = HistoryConfig::new(
            HistoryLevel::Database(0),
            "0".to_string(),
            RetentionType::Count(100),
        );
        assert_eq!(config.level, HistoryLevel::Database(0));
        assert_eq!(config.target, "0");
        assert_eq!(config.retention, RetentionType::Count(100));
    }

    #[test]
    fn test_history_config_key() {
        let config = HistoryConfig::new(
            HistoryLevel::Key,
            "0:mykey".to_string(),
            RetentionType::Unlimited,
        );
        assert_eq!(config.level, HistoryLevel::Key);
        assert_eq!(config.target, "0:mykey");
        assert_eq!(config.retention, RetentionType::Unlimited);
    }

    #[test]
    fn test_history_entry_new() {
        let entry = HistoryEntry::new(
            0,
            "mykey".to_string(),
            KeyType::String,
            1,
            "SET".to_string(),
            1673000000000,
        );
        assert_eq!(entry.db, 0);
        assert_eq!(entry.key, "mykey");
        assert_eq!(entry.key_type, KeyType::String);
        assert_eq!(entry.version_num, 1);
        assert_eq!(entry.operation, "SET");
        assert_eq!(entry.timestamp_ms, 1673000000000);
        assert!(entry.data_snapshot.is_none());
        assert!(entry.expire_at.is_none());
    }

    #[test]
    fn test_history_entry_with_snapshot() {
        let mut entry = HistoryEntry::new(
            0,
            "mykey".to_string(),
            KeyType::String,
            1,
            "SET".to_string(),
            1673000000000,
        );
        entry.data_snapshot = Some(vec![1, 2, 3, 4, 5]);
        entry.expire_at = Some(1673000100000);

        assert_eq!(entry.data_snapshot, Some(vec![1, 2, 3, 4, 5]));
        assert_eq!(entry.expire_at, Some(1673000100000));
    }

    #[test]
    fn test_history_entry_different_types() {
        let types = vec![
            KeyType::String,
            KeyType::Hash,
            KeyType::List,
            KeyType::Set,
            KeyType::ZSet,
            KeyType::Stream,
        ];

        for (idx, key_type) in types.iter().enumerate() {
            let entry = HistoryEntry::new(
                0,
                "key".to_string(),
                *key_type,
                idx as i64,
                "OPERATION".to_string(),
                1000000 + idx as i64,
            );
            assert_eq!(entry.key_type, *key_type);
        }
    }
}
