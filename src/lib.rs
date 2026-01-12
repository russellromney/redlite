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

pub mod db;
pub mod error;
pub mod resp;
pub mod server;
pub mod types;

pub use db::Db;
pub use error::{KvError, Result};
pub use resp::RespValue;
pub use server::Server;
pub use types::{KeyType, SetOptions, ZMember};
