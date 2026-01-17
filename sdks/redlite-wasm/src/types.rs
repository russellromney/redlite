use serde::{Deserialize, Serialize};
use wasm_bindgen::prelude::*;

/// Key types in Redlite (matching Redis data structure types)
#[wasm_bindgen]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
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

/// Options for SET command
#[wasm_bindgen]
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SetOptions {
    /// Expire time in seconds
    #[wasm_bindgen(skip)]
    pub ex: Option<i64>,
    /// Expire time in milliseconds
    #[wasm_bindgen(skip)]
    pub px: Option<i64>,
    /// Only set if key does not exist
    #[wasm_bindgen(skip)]
    pub nx: bool,
    /// Only set if key exists
    #[wasm_bindgen(skip)]
    pub xx: bool,
}

#[wasm_bindgen]
impl SetOptions {
    #[wasm_bindgen(constructor)]
    pub fn new() -> Self {
        Self::default()
    }

    /// Set expiration in seconds
    #[wasm_bindgen(js_name = "withEx")]
    pub fn with_ex(mut self, seconds: i64) -> Self {
        self.ex = Some(seconds);
        self
    }

    /// Set expiration in milliseconds
    #[wasm_bindgen(js_name = "withPx")]
    pub fn with_px(mut self, millis: i64) -> Self {
        self.px = Some(millis);
        self
    }

    /// Only set if key does not exist
    #[wasm_bindgen(js_name = "withNx")]
    pub fn with_nx(mut self) -> Self {
        self.nx = true;
        self
    }

    /// Only set if key exists
    #[wasm_bindgen(js_name = "withXx")]
    pub fn with_xx(mut self) -> Self {
        self.xx = true;
        self
    }
}

impl SetOptions {
    /// Get TTL in milliseconds
    pub fn ttl_ms(&self) -> Option<i64> {
        if let Some(ex) = self.ex {
            Some(ex * 1000)
        } else {
            self.px
        }
    }
}

/// A sorted set member with score
#[wasm_bindgen]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ZMember {
    #[wasm_bindgen(skip)]
    pub score: f64,
    #[wasm_bindgen(skip)]
    pub member: Vec<u8>,
}

#[wasm_bindgen]
impl ZMember {
    #[wasm_bindgen(constructor)]
    pub fn new(score: f64, member: Vec<u8>) -> Self {
        Self { score, member }
    }

    #[wasm_bindgen(getter)]
    pub fn score(&self) -> f64 {
        self.score
    }

    #[wasm_bindgen(getter)]
    pub fn member(&self) -> Vec<u8> {
        self.member.clone()
    }
}

/// Direction for list operations
#[wasm_bindgen]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ListDirection {
    Left,
    Right,
}

/// Key metadata
#[wasm_bindgen]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KeyInfo {
    #[wasm_bindgen(skip)]
    pub key_type: KeyType,
    #[wasm_bindgen(skip)]
    pub ttl: i64,
    #[wasm_bindgen(skip)]
    pub created_at: i64,
    #[wasm_bindgen(skip)]
    pub updated_at: i64,
}

#[wasm_bindgen]
impl KeyInfo {
    #[wasm_bindgen(getter, js_name = "keyType")]
    pub fn key_type(&self) -> KeyType {
        self.key_type
    }

    #[wasm_bindgen(getter)]
    pub fn ttl(&self) -> i64 {
        self.ttl
    }

    #[wasm_bindgen(getter, js_name = "createdAt")]
    pub fn created_at(&self) -> i64 {
        self.created_at
    }

    #[wasm_bindgen(getter, js_name = "updatedAt")]
    pub fn updated_at(&self) -> i64 {
        self.updated_at
    }
}

/// Scan result for SCAN, HSCAN, SSCAN, ZSCAN commands
#[wasm_bindgen]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScanResult {
    #[wasm_bindgen(skip)]
    pub cursor: i64,
    #[wasm_bindgen(skip)]
    pub keys: Vec<String>,
}

#[wasm_bindgen]
impl ScanResult {
    #[wasm_bindgen(getter)]
    pub fn cursor(&self) -> i64 {
        self.cursor
    }

    #[wasm_bindgen(getter)]
    pub fn keys(&self) -> Vec<String> {
        self.keys.clone()
    }
}
