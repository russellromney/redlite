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
#[cfg(feature = "libsql")]
pub mod libsql_db;
pub mod resp;
pub mod server;
#[cfg(feature = "turso")]
pub mod turso_db;
pub mod types;

pub use backend::Backend;
pub use db::Db;
#[cfg(feature = "libsql")]
pub use libsql_db::LibsqlDb;
#[cfg(feature = "turso")]
pub use turso_db::TursoDb;
pub use error::{KvError, Result};
pub use resp::RespValue;
pub use server::Server;
pub use types::{KeyType, SetOptions, ZMember, HistoryLevel, RetentionType, HistoryConfig, HistoryEntry, StreamId, StreamEntry};
