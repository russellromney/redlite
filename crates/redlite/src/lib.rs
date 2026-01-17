//! Redlite - SQLite-backed Redis-compatible KV store
//!
//! # Example
//!
//! ```
//! use redlite::Db;
//!
//! let db = Db::open_memory().unwrap();
//!
//! // SET/GET
//! db.set("key", b"value", None).unwrap();
//! let value = db.get("key").unwrap();
//! assert_eq!(value, Some(b"value".to_vec()));
//! ```

pub mod backend;
pub mod db;
pub mod error;
pub mod resp;
pub mod search;
pub mod server;
#[cfg(feature = "turso")]
pub mod turso_db;
pub mod types;

pub use backend::Backend;
pub use db::{Db, EvictionPolicy};
pub use error::{KvError, Result};
pub use resp::RespValue;
pub use server::{ConnectionHandler, Server};
#[cfg(feature = "turso")]
pub use turso_db::TursoDb;
pub use types::{
    FtField, FtFieldType, FtIndex, FtIndexInfo, FtOnType, FtSearchOptions, FtSearchResult,
    FtSuggestion, GetExOption, HistoryConfig, HistoryEntry, HistoryLevel, KeyType, ListDirection,
    PollConfig, RetentionType, SetOptions, StreamEntry, StreamId, ZMember,
};
