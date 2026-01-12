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
