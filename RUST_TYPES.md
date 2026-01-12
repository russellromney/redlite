# Redlite Rust Types

## Dependencies

```toml
[package]
name = "redlite"
version = "0.1.0"
edition = "2021"
description = "SQLite-backed Redis-compatible KV store"
license = "MIT"

[lib]
name = "redlite"
path = "src/lib.rs"

[[bin]]
name = "redlite"
path = "src/main.rs"

[dependencies]
rusqlite = { version = "0.31", features = ["bundled"] }
tokio = { version = "1", features = ["full"] }
bytes = "1"
thiserror = "1"
tracing = "0.1"
tracing-subscriber = "0.3"
clap = { version = "4", features = ["derive"] }
anyhow = "1"

[dev-dependencies]
redis = "0.25"
tempfile = "3"
tokio-test = "0.4"
```

## Error Types

```rust
// src/error.rs

use thiserror::Error;

#[derive(Error, Debug)]
pub enum KvError {
    #[error("key not found")]
    NotFound,

    #[error("wrong type for key")]
    WrongType,

    #[error("value is not an integer")]
    NotInteger,

    #[error("value is not a float")]
    NotFloat,

    #[error("syntax error")]
    SyntaxError,

    #[error("index out of range")]
    OutOfRange,

    #[error("invalid cursor")]
    InvalidCursor,

    #[error("database error: {0}")]
    Database(#[from] rusqlite::Error),

    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
}

pub type Result<T> = std::result::Result<T, KvError>;
```

## Key Types

```rust
// src/types.rs

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i32)]
pub enum KeyType {
    String = 1,
    Hash = 2,
    List = 3,
    Set = 4,
    ZSet = 5,
}

impl KeyType {
    pub fn from_i32(v: i32) -> Option<Self> {
        match v {
            1 => Some(KeyType::String),
            2 => Some(KeyType::Hash),
            3 => Some(KeyType::List),
            4 => Some(KeyType::Set),
            5 => Some(KeyType::ZSet),
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
        }
    }

    pub fn from_str(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "string" => Some(KeyType::String),
            "hash" => Some(KeyType::Hash),
            "list" => Some(KeyType::List),
            "set" => Some(KeyType::Set),
            "zset" => Some(KeyType::ZSet),
            _ => None,
        }
    }
}
```

## Sorted Set Member

```rust
// src/types.rs (continued)

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
```

## Set Options

```rust
// src/types.rs (continued)

use std::time::Duration;

#[derive(Debug, Clone, Default)]
pub struct SetOptions {
    /// Time-to-live
    pub ttl: Option<Duration>,
    /// Only set if key does not exist
    pub nx: bool,
    /// Only set if key exists
    pub xx: bool,
    /// Return old value
    pub get: bool,
}

impl SetOptions {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn ttl(mut self, ttl: Duration) -> Self {
        self.ttl = Some(ttl);
        self
    }

    pub fn ttl_secs(mut self, secs: u64) -> Self {
        self.ttl = Some(Duration::from_secs(secs));
        self
    }

    pub fn ttl_millis(mut self, ms: u64) -> Self {
        self.ttl = Some(Duration::from_millis(ms));
        self
    }

    pub fn nx(mut self) -> Self {
        self.nx = true;
        self
    }

    pub fn xx(mut self) -> Self {
        self.xx = true;
        self
    }

    pub fn get(mut self) -> Self {
        self.get = true;
        self
    }
}
```

## SCAN Cursor

```rust
// src/scan.rs

use base64::{Engine, engine::general_purpose::STANDARD};
use crate::error::{KvError, Result};

#[derive(Debug, Clone, Default)]
pub struct ScanCursor {
    pub db: i32,
    pub last_key: String,
}

impl ScanCursor {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn encode(&self) -> String {
        if self.last_key.is_empty() {
            return "0".to_string();
        }
        let data = format!("{}:{}", self.db, self.last_key);
        STANDARD.encode(data.as_bytes())
    }

    pub fn decode(s: &str) -> Result<Self> {
        if s == "0" {
            return Ok(Self::default());
        }

        let data = STANDARD.decode(s).map_err(|_| KvError::InvalidCursor)?;
        let s = String::from_utf8(data).map_err(|_| KvError::InvalidCursor)?;

        let (db_str, last_key) = s.split_once(':').ok_or(KvError::InvalidCursor)?;
        let db = db_str.parse().map_err(|_| KvError::InvalidCursor)?;

        Ok(Self {
            db,
            last_key: last_key.to_string(),
        })
    }
}

#[derive(Debug)]
pub struct ScanResult {
    pub cursor: String,
    pub keys: Vec<String>,
}

#[derive(Debug, Default)]
pub struct ScanOptions {
    pub pattern: Option<String>,
    pub count: Option<i64>,
    pub key_type: Option<crate::types::KeyType>,
}
```

## RESP Value

```rust
// src/resp/value.rs

#[derive(Debug, Clone)]
pub enum RespValue {
    SimpleString(String),
    Error(String),
    Integer(i64),
    BulkString(Option<Vec<u8>>),
    Array(Option<Vec<RespValue>>),
}

impl RespValue {
    pub fn ok() -> Self {
        RespValue::SimpleString("OK".to_string())
    }

    pub fn pong() -> Self {
        RespValue::SimpleString("PONG".to_string())
    }

    pub fn null() -> Self {
        RespValue::BulkString(None)
    }

    pub fn null_array() -> Self {
        RespValue::Array(None)
    }

    pub fn error(msg: impl Into<String>) -> Self {
        RespValue::Error(format!("ERR {}", msg.into()))
    }

    pub fn wrong_type() -> Self {
        RespValue::Error("WRONGTYPE Operation against a key holding the wrong kind of value".to_string())
    }

    pub fn wrong_arity(cmd: &str) -> Self {
        RespValue::Error(format!("ERR wrong number of arguments for '{}' command", cmd))
    }

    pub fn from_optional_bytes(opt: Option<Vec<u8>>) -> Self {
        RespValue::BulkString(opt)
    }

    pub fn from_bytes(bytes: Vec<u8>) -> Self {
        RespValue::BulkString(Some(bytes))
    }

    pub fn from_string(s: String) -> Self {
        RespValue::BulkString(Some(s.into_bytes()))
    }

    pub fn from_int(n: i64) -> Self {
        RespValue::Integer(n)
    }

    pub fn from_bool(b: bool) -> Self {
        RespValue::Integer(if b { 1 } else { 0 })
    }

    pub fn from_float(f: f64) -> Self {
        RespValue::BulkString(Some(f.to_string().into_bytes()))
    }

    pub fn from_vec<T: Into<RespValue>>(items: Vec<T>) -> Self {
        RespValue::Array(Some(items.into_iter().map(Into::into).collect()))
    }

    pub fn encode(&self) -> Vec<u8> {
        match self {
            RespValue::SimpleString(s) => format!("+{}\r\n", s).into_bytes(),
            RespValue::Error(s) => format!("-{}\r\n", s).into_bytes(),
            RespValue::Integer(n) => format!(":{}\r\n", n).into_bytes(),
            RespValue::BulkString(None) => b"$-1\r\n".to_vec(),
            RespValue::BulkString(Some(data)) => {
                let mut buf = format!("${}\r\n", data.len()).into_bytes();
                buf.extend_from_slice(data);
                buf.extend_from_slice(b"\r\n");
                buf
            }
            RespValue::Array(None) => b"*-1\r\n".to_vec(),
            RespValue::Array(Some(items)) => {
                let mut buf = format!("*{}\r\n", items.len()).into_bytes();
                for item in items {
                    buf.extend(item.encode());
                }
                buf
            }
        }
    }
}

impl From<()> for RespValue {
    fn from(_: ()) -> Self {
        RespValue::ok()
    }
}

impl From<i64> for RespValue {
    fn from(n: i64) -> Self {
        RespValue::Integer(n)
    }
}

impl From<bool> for RespValue {
    fn from(b: bool) -> Self {
        RespValue::from_bool(b)
    }
}

impl From<String> for RespValue {
    fn from(s: String) -> Self {
        RespValue::from_string(s)
    }
}

impl From<Vec<u8>> for RespValue {
    fn from(bytes: Vec<u8>) -> Self {
        RespValue::from_bytes(bytes)
    }
}

impl From<Option<Vec<u8>>> for RespValue {
    fn from(opt: Option<Vec<u8>>) -> Self {
        RespValue::from_optional_bytes(opt)
    }
}
```

## Constants

```rust
// src/constants.rs

/// Gap between list positions (integer-based)
pub const POS_GAP: i64 = 1_000_000;

/// Minimum gap before rebalancing is needed
pub const MIN_POS_GAP: i64 = 2;

/// Maximum number of databases (SELECT 0-15)
pub const MAX_DBS: i32 = 16;

/// Default SCAN count
pub const DEFAULT_SCAN_COUNT: i64 = 10;

/// Keys expired per expiration daemon tick
pub const EXPIRE_BATCH_SIZE: i64 = 20;

/// Expiration daemon interval in milliseconds
pub const EXPIRE_INTERVAL_MS: u64 = 100;
```
