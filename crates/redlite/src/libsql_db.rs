//! libSQL-backed Redis-compatible KV store
//!
//! This is a parallel implementation to Db that uses libsql (the C fork) instead of rusqlite.
//! Useful for embedded replica sync to Turso cloud.
//! Implements the core Redis commands needed for benchmarking.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::RwLock;

use crate::error::{KvError, Result};
use crate::types::KeyType;

/// libSQL-backed database (C fork of SQLite)
pub struct LibsqlDb {
    db: libsql::Database,
    conn: RwLock<libsql::Connection>,
    runtime: tokio::runtime::Handle,
    selected_db: i32,
}

impl LibsqlDb {
    /// Open or create a database at the given path
    pub fn open(path: &str) -> Result<Self> {
        let runtime = tokio::runtime::Handle::try_current()
            .or_else(|_| {
                let rt = tokio::runtime::Runtime::new()
                    .map_err(|e| KvError::Other(format!("Runtime error: {}", e)))?;
                Ok::<_, KvError>(rt.handle().clone())
            })?;

        runtime.block_on(async {
            let db = libsql::Builder::new_local(path)
                .build()
                .await
                .map_err(|e| KvError::Other(format!("Turso open error: {}", e)))?;

            let conn = db.connect()
                .map_err(|e| KvError::Other(format!("Turso connect error: {}", e)))?;

            let turso_db = Self {
                db,
                conn: RwLock::new(conn),
                runtime: runtime.clone(),
                selected_db: 0,
            };

            turso_db.configure().await?;
            turso_db.migrate().await?;

            Ok(turso_db)
        })
    }

    /// Open an in-memory database
    pub fn open_memory() -> Result<Self> {
        Self::open(":memory:")
    }

    async fn configure(&self) -> Result<()> {
        let conn = self.conn.write().await;
        conn.execute("PRAGMA journal_mode = WAL", ())
            .await
            .map_err(|e| KvError::Other(format!("Pragma error: {}", e)))?;
        conn.execute("PRAGMA synchronous = NORMAL", ())
            .await
            .map_err(|e| KvError::Other(format!("Pragma error: {}", e)))?;
        conn.execute("PRAGMA foreign_keys = ON", ())
            .await
            .map_err(|e| KvError::Other(format!("Pragma error: {}", e)))?;
        Ok(())
    }

    async fn migrate(&self) -> Result<()> {
        let conn = self.conn.write().await;

        // Core key metadata
        conn.execute(
            r#"CREATE TABLE IF NOT EXISTS keys (
                id INTEGER PRIMARY KEY,
                db INTEGER NOT NULL DEFAULT 0,
                key TEXT NOT NULL,
                type INTEGER NOT NULL,
                expire_at INTEGER,
                created_at INTEGER NOT NULL DEFAULT (unixepoch('now', 'subsec') * 1000),
                updated_at INTEGER NOT NULL DEFAULT (unixepoch('now', 'subsec') * 1000)
            )"#,
            (),
        ).await.map_err(|e| KvError::Other(format!("Migration error: {}", e)))?;

        conn.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_keys_db_key ON keys(db, key)",
            (),
        ).await.map_err(|e| KvError::Other(format!("Migration error: {}", e)))?;

        // Strings
        conn.execute(
            r#"CREATE TABLE IF NOT EXISTS strings (
                key_id INTEGER PRIMARY KEY REFERENCES keys(id) ON DELETE CASCADE,
                value BLOB NOT NULL
            )"#,
            (),
        ).await.map_err(|e| KvError::Other(format!("Migration error: {}", e)))?;

        // Hashes
        conn.execute(
            r#"CREATE TABLE IF NOT EXISTS hashes (
                key_id INTEGER NOT NULL REFERENCES keys(id) ON DELETE CASCADE,
                field TEXT NOT NULL,
                value BLOB NOT NULL,
                PRIMARY KEY (key_id, field)
            )"#,
            (),
        ).await.map_err(|e| KvError::Other(format!("Migration error: {}", e)))?;

        // Lists
        conn.execute(
            r#"CREATE TABLE IF NOT EXISTS lists (
                key_id INTEGER NOT NULL REFERENCES keys(id) ON DELETE CASCADE,
                pos INTEGER NOT NULL,
                value BLOB NOT NULL,
                PRIMARY KEY (key_id, pos)
            )"#,
            (),
        ).await.map_err(|e| KvError::Other(format!("Migration error: {}", e)))?;

        // Sets
        conn.execute(
            r#"CREATE TABLE IF NOT EXISTS sets (
                key_id INTEGER NOT NULL REFERENCES keys(id) ON DELETE CASCADE,
                member BLOB NOT NULL,
                PRIMARY KEY (key_id, member)
            )"#,
            (),
        ).await.map_err(|e| KvError::Other(format!("Migration error: {}", e)))?;

        // Sorted sets
        conn.execute(
            r#"CREATE TABLE IF NOT EXISTS zsets (
                key_id INTEGER NOT NULL REFERENCES keys(id) ON DELETE CASCADE,
                member BLOB NOT NULL,
                score REAL NOT NULL,
                PRIMARY KEY (key_id, member)
            )"#,
            (),
        ).await.map_err(|e| KvError::Other(format!("Migration error: {}", e)))?;

        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_zsets_score ON zsets(key_id, score, member)",
            (),
        ).await.map_err(|e| KvError::Other(format!("Migration error: {}", e)))?;

        Ok(())
    }

    fn now_ms() -> i64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64
    }

    // =========================================================================
    // STRING OPERATIONS
    // =========================================================================

    /// SET key value
    pub fn set(&self, key: &str, value: &[u8], expire_ms: Option<i64>) -> Result<()> {
        self.runtime.block_on(self.set_async(key, value, expire_ms))
    }

    async fn set_async(&self, key: &str, value: &[u8], expire_ms: Option<i64>) -> Result<()> {
        let conn = self.conn.write().await;
        let db = self.selected_db;
        let now = Self::now_ms();
        let expire_at = expire_ms.map(|ms| now + ms);

        // Check if key exists
        let mut rows = conn.query(
            "SELECT id, type FROM keys WHERE db = ?1 AND key = ?2",
            libsql::params![db, key.to_string()],
        ).await.map_err(|e| KvError::Other(format!("Query error: {}", e)))?;

        if let Some(row) = rows.next().await.map_err(|e| KvError::Other(format!("Row error: {}", e)))? {
            let key_id: i64 = row.get(0).map_err(|e| KvError::Other(format!("Get error: {}", e)))?;
            let key_type: i32 = row.get(1).map_err(|e| KvError::Other(format!("Get error: {}", e)))?;

            if key_type != KeyType::String as i32 {
                return Err(KvError::WrongType);
            }

            // Update existing
            conn.execute(
                "UPDATE keys SET expire_at = ?1, updated_at = ?2 WHERE id = ?3",
                libsql::params![expire_at, now, key_id],
            ).await.map_err(|e| KvError::Other(format!("Update error: {}", e)))?;

            conn.execute(
                "UPDATE strings SET value = ?1 WHERE key_id = ?2",
                libsql::params![value.to_vec(), key_id],
            ).await.map_err(|e| KvError::Other(format!("Update error: {}", e)))?;
        } else {
            // Insert new
            conn.execute(
                "INSERT INTO keys (db, key, type, expire_at, created_at, updated_at) VALUES (?1, ?2, ?3, ?4, ?5, ?5)",
                libsql::params![db, key.to_string(), KeyType::String as i32, expire_at, now],
            ).await.map_err(|e| KvError::Other(format!("Insert error: {}", e)))?;

            let key_id = conn.last_insert_rowid();

            conn.execute(
                "INSERT INTO strings (key_id, value) VALUES (?1, ?2)",
                libsql::params![key_id, value.to_vec()],
            ).await.map_err(|e| KvError::Other(format!("Insert error: {}", e)))?;
        }

        Ok(())
    }

    /// GET key
    pub fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
        self.runtime.block_on(self.get_async(key))
    }

    async fn get_async(&self, key: &str) -> Result<Option<Vec<u8>>> {
        let conn = self.conn.read().await;
        let db = self.selected_db;
        let now = Self::now_ms();

        let mut rows = conn.query(
            "SELECT k.id, k.type, k.expire_at FROM keys k WHERE k.db = ?1 AND k.key = ?2",
            libsql::params![db, key.to_string()],
        ).await.map_err(|e| KvError::Other(format!("Query error: {}", e)))?;

        if let Some(row) = rows.next().await.map_err(|e| KvError::Other(format!("Row error: {}", e)))? {
            let key_id: i64 = row.get(0).map_err(|e| KvError::Other(format!("Get error: {}", e)))?;
            let key_type: i32 = row.get(1).map_err(|e| KvError::Other(format!("Get error: {}", e)))?;
            let expire_at: Option<i64> = match row.get_value(2).map_err(|e| KvError::Other(format!("Get error: {}", e)))? {
                libsql::Value::Null => None,
                libsql::Value::Integer(i) => Some(i),
                _ => None,
            };

            // Check expiration
            if let Some(exp) = expire_at {
                if exp <= now {
                    return Ok(None);
                }
            }

            if key_type != KeyType::String as i32 {
                return Err(KvError::WrongType);
            }

            drop(rows);
            let mut value_rows = conn.query(
                "SELECT value FROM strings WHERE key_id = ?1",
                libsql::params![key_id],
            ).await.map_err(|e| KvError::Other(format!("Query error: {}", e)))?;

            if let Some(value_row) = value_rows.next().await.map_err(|e| KvError::Other(format!("Row error: {}", e)))? {
                let value: Vec<u8> = value_row.get(0).map_err(|e| KvError::Other(format!("Get error: {}", e)))?;
                return Ok(Some(value));
            }
        }

        Ok(None)
    }

    // =========================================================================
    // HASH OPERATIONS
    // =========================================================================

    /// HSET key field1 value1 [field2 value2 ...]
    pub fn hset(&self, key: &str, pairs: &[(&str, &[u8])]) -> Result<i64> {
        self.runtime.block_on(self.hset_async(key, pairs))
    }

    async fn hset_async(&self, key: &str, pairs: &[(&str, &[u8])]) -> Result<i64> {
        if pairs.is_empty() {
            return Ok(0);
        }

        let conn = self.conn.write().await;
        let db = self.selected_db;
        let now = Self::now_ms();

        // Get or create key
        let mut rows = conn.query(
            "SELECT id, type FROM keys WHERE db = ?1 AND key = ?2",
            libsql::params![db, key.to_string()],
        ).await.map_err(|e| KvError::Other(format!("Query error: {}", e)))?;

        let key_id = if let Some(row) = rows.next().await.map_err(|e| KvError::Other(format!("Row error: {}", e)))? {
            let id: i64 = row.get(0).map_err(|e| KvError::Other(format!("Get error: {}", e)))?;
            let key_type: i32 = row.get(1).map_err(|e| KvError::Other(format!("Get error: {}", e)))?;
            if key_type != KeyType::Hash as i32 {
                return Err(KvError::WrongType);
            }
            drop(rows);
            id
        } else {
            drop(rows);
            conn.execute(
                "INSERT INTO keys (db, key, type, created_at, updated_at) VALUES (?1, ?2, ?3, ?4, ?4)",
                libsql::params![db, key.to_string(), KeyType::Hash as i32, now],
            ).await.map_err(|e| KvError::Other(format!("Insert error: {}", e)))?;
            conn.last_insert_rowid()
        };

        let mut new_count = 0i64;

        for (field, value) in pairs {
            // Check if field exists
            let mut field_rows = conn.query(
                "SELECT 1 FROM hashes WHERE key_id = ?1 AND field = ?2",
                libsql::params![key_id, field.to_string()],
            ).await.map_err(|e| KvError::Other(format!("Query error: {}", e)))?;

            let is_new = field_rows.next().await.map_err(|e| KvError::Other(format!("Row error: {}", e)))?.is_none();
            drop(field_rows);

            if is_new {
                conn.execute(
                    "INSERT INTO hashes (key_id, field, value) VALUES (?1, ?2, ?3)",
                    libsql::params![key_id, field.to_string(), value.to_vec()],
                ).await.map_err(|e| KvError::Other(format!("Insert error: {}", e)))?;
                new_count += 1;
            } else {
                conn.execute(
                    "UPDATE hashes SET value = ?1 WHERE key_id = ?2 AND field = ?3",
                    libsql::params![value.to_vec(), key_id, field.to_string()],
                ).await.map_err(|e| KvError::Other(format!("Update error: {}", e)))?;
            }
        }

        Ok(new_count)
    }

    /// HGET key field
    pub fn hget(&self, key: &str, field: &str) -> Result<Option<Vec<u8>>> {
        self.runtime.block_on(self.hget_async(key, field))
    }

    async fn hget_async(&self, key: &str, field: &str) -> Result<Option<Vec<u8>>> {
        let conn = self.conn.read().await;
        let db = self.selected_db;

        let mut rows = conn.query(
            r#"SELECT h.value FROM hashes h
               JOIN keys k ON h.key_id = k.id
               WHERE k.db = ?1 AND k.key = ?2 AND h.field = ?3"#,
            libsql::params![db, key.to_string(), field.to_string()],
        ).await.map_err(|e| KvError::Other(format!("Query error: {}", e)))?;

        if let Some(row) = rows.next().await.map_err(|e| KvError::Other(format!("Row error: {}", e)))? {
            let value: Vec<u8> = row.get(0).map_err(|e| KvError::Other(format!("Get error: {}", e)))?;
            Ok(Some(value))
        } else {
            Ok(None)
        }
    }

    /// HGETALL key
    pub fn hgetall(&self, key: &str) -> Result<Vec<(String, Vec<u8>)>> {
        self.runtime.block_on(self.hgetall_async(key))
    }

    async fn hgetall_async(&self, key: &str) -> Result<Vec<(String, Vec<u8>)>> {
        let conn = self.conn.read().await;
        let db = self.selected_db;

        let mut rows = conn.query(
            r#"SELECT h.field, h.value FROM hashes h
               JOIN keys k ON h.key_id = k.id
               WHERE k.db = ?1 AND k.key = ?2"#,
            libsql::params![db, key.to_string()],
        ).await.map_err(|e| KvError::Other(format!("Query error: {}", e)))?;

        let mut result = Vec::new();
        while let Some(row) = rows.next().await.map_err(|e| KvError::Other(format!("Row error: {}", e)))? {
            let field: String = row.get(0).map_err(|e| KvError::Other(format!("Get error: {}", e)))?;
            let value: Vec<u8> = row.get(1).map_err(|e| KvError::Other(format!("Get error: {}", e)))?;
            result.push((field, value));
        }

        Ok(result)
    }

    // =========================================================================
    // LIST OPERATIONS
    // =========================================================================

    /// LPUSH key value
    pub fn lpush(&self, key: &str, value: &[u8]) -> Result<i64> {
        self.runtime.block_on(self.lpush_async(key, value))
    }

    async fn lpush_async(&self, key: &str, value: &[u8]) -> Result<i64> {
        let conn = self.conn.write().await;
        let db = self.selected_db;
        let now = Self::now_ms();

        // Get or create key
        let mut rows = conn.query(
            "SELECT id, type FROM keys WHERE db = ?1 AND key = ?2",
            libsql::params![db, key.to_string()],
        ).await.map_err(|e| KvError::Other(format!("Query error: {}", e)))?;

        let key_id = if let Some(row) = rows.next().await.map_err(|e| KvError::Other(format!("Row error: {}", e)))? {
            let id: i64 = row.get(0).map_err(|e| KvError::Other(format!("Get error: {}", e)))?;
            let key_type: i32 = row.get(1).map_err(|e| KvError::Other(format!("Get error: {}", e)))?;
            if key_type != KeyType::List as i32 {
                return Err(KvError::WrongType);
            }
            drop(rows);
            id
        } else {
            drop(rows);
            conn.execute(
                "INSERT INTO keys (db, key, type, created_at, updated_at) VALUES (?1, ?2, ?3, ?4, ?4)",
                libsql::params![db, key.to_string(), KeyType::List as i32, now],
            ).await.map_err(|e| KvError::Other(format!("Insert error: {}", e)))?;
            conn.last_insert_rowid()
        };

        // Get min position
        let mut pos_rows = conn.query(
            "SELECT MIN(pos) FROM lists WHERE key_id = ?1",
            libsql::params![key_id],
        ).await.map_err(|e| KvError::Other(format!("Query error: {}", e)))?;

        let min_pos = if let Some(row) = pos_rows.next().await.map_err(|e| KvError::Other(format!("Row error: {}", e)))? {
            match row.get_value(0).map_err(|e| KvError::Other(format!("Get error: {}", e)))? {
                libsql::Value::Integer(i) => i - 1,
                _ => 0,
            }
        } else {
            0
        };
        drop(pos_rows);

        conn.execute(
            "INSERT INTO lists (key_id, pos, value) VALUES (?1, ?2, ?3)",
            libsql::params![key_id, min_pos, value.to_vec()],
        ).await.map_err(|e| KvError::Other(format!("Insert error: {}", e)))?;

        // Return list length
        let mut count_rows = conn.query(
            "SELECT COUNT(*) FROM lists WHERE key_id = ?1",
            libsql::params![key_id],
        ).await.map_err(|e| KvError::Other(format!("Query error: {}", e)))?;

        if let Some(row) = count_rows.next().await.map_err(|e| KvError::Other(format!("Row error: {}", e)))? {
            let count: i64 = row.get(0).map_err(|e| KvError::Other(format!("Get error: {}", e)))?;
            Ok(count)
        } else {
            Ok(1)
        }
    }

    /// LPOP key
    pub fn lpop(&self, key: &str) -> Result<Option<Vec<u8>>> {
        self.runtime.block_on(self.lpop_async(key))
    }

    async fn lpop_async(&self, key: &str) -> Result<Option<Vec<u8>>> {
        let conn = self.conn.write().await;
        let db = self.selected_db;

        let mut rows = conn.query(
            r#"SELECT l.rowid, l.value FROM lists l
               JOIN keys k ON l.key_id = k.id
               WHERE k.db = ?1 AND k.key = ?2
               ORDER BY l.pos ASC LIMIT 1"#,
            libsql::params![db, key.to_string()],
        ).await.map_err(|e| KvError::Other(format!("Query error: {}", e)))?;

        if let Some(row) = rows.next().await.map_err(|e| KvError::Other(format!("Row error: {}", e)))? {
            let rowid: i64 = row.get(0).map_err(|e| KvError::Other(format!("Get error: {}", e)))?;
            let value: Vec<u8> = row.get(1).map_err(|e| KvError::Other(format!("Get error: {}", e)))?;
            drop(rows);

            conn.execute(
                "DELETE FROM lists WHERE rowid = ?1",
                libsql::params![rowid],
            ).await.map_err(|e| KvError::Other(format!("Delete error: {}", e)))?;

            Ok(Some(value))
        } else {
            Ok(None)
        }
    }

    /// LRANGE key start stop
    pub fn lrange(&self, key: &str, start: i64, stop: i64) -> Result<Vec<Vec<u8>>> {
        self.runtime.block_on(self.lrange_async(key, start, stop))
    }

    async fn lrange_async(&self, key: &str, start: i64, stop: i64) -> Result<Vec<Vec<u8>>> {
        let conn = self.conn.read().await;
        let db = self.selected_db;

        // Get list length first
        let mut len_rows = conn.query(
            r#"SELECT COUNT(*) FROM lists l
               JOIN keys k ON l.key_id = k.id
               WHERE k.db = ?1 AND k.key = ?2"#,
            libsql::params![db, key.to_string()],
        ).await.map_err(|e| KvError::Other(format!("Query error: {}", e)))?;

        let len = if let Some(row) = len_rows.next().await.map_err(|e| KvError::Other(format!("Row error: {}", e)))? {
            row.get::<i64>(0).map_err(|e| KvError::Other(format!("Get error: {}", e)))?
        } else {
            0
        };
        drop(len_rows);

        if len == 0 {
            return Ok(Vec::new());
        }

        // Normalize indices
        let real_start = if start < 0 { (len + start).max(0) } else { start.min(len) };
        let real_stop = if stop < 0 { len + stop + 1 } else { (stop + 1).min(len) };

        if real_start >= real_stop {
            return Ok(Vec::new());
        }

        let limit = real_stop - real_start;
        let mut rows = conn.query(
            r#"SELECT l.value FROM lists l
               JOIN keys k ON l.key_id = k.id
               WHERE k.db = ?1 AND k.key = ?2
               ORDER BY l.pos ASC
               LIMIT ?3 OFFSET ?4"#,
            libsql::params![db, key.to_string(), limit, real_start],
        ).await.map_err(|e| KvError::Other(format!("Query error: {}", e)))?;

        let mut result = Vec::new();
        while let Some(row) = rows.next().await.map_err(|e| KvError::Other(format!("Row error: {}", e)))? {
            let value: Vec<u8> = row.get(0).map_err(|e| KvError::Other(format!("Get error: {}", e)))?;
            result.push(value);
        }

        Ok(result)
    }

    // =========================================================================
    // SET OPERATIONS
    // =========================================================================

    /// SADD key member
    pub fn sadd(&self, key: &str, member: &[u8]) -> Result<i64> {
        self.runtime.block_on(self.sadd_async(key, member))
    }

    async fn sadd_async(&self, key: &str, member: &[u8]) -> Result<i64> {
        let conn = self.conn.write().await;
        let db = self.selected_db;
        let now = Self::now_ms();

        // Get or create key
        let mut rows = conn.query(
            "SELECT id, type FROM keys WHERE db = ?1 AND key = ?2",
            libsql::params![db, key.to_string()],
        ).await.map_err(|e| KvError::Other(format!("Query error: {}", e)))?;

        let key_id = if let Some(row) = rows.next().await.map_err(|e| KvError::Other(format!("Row error: {}", e)))? {
            let id: i64 = row.get(0).map_err(|e| KvError::Other(format!("Get error: {}", e)))?;
            let key_type: i32 = row.get(1).map_err(|e| KvError::Other(format!("Get error: {}", e)))?;
            if key_type != KeyType::Set as i32 {
                return Err(KvError::WrongType);
            }
            drop(rows);
            id
        } else {
            drop(rows);
            conn.execute(
                "INSERT INTO keys (db, key, type, created_at, updated_at) VALUES (?1, ?2, ?3, ?4, ?4)",
                libsql::params![db, key.to_string(), KeyType::Set as i32, now],
            ).await.map_err(|e| KvError::Other(format!("Insert error: {}", e)))?;
            conn.last_insert_rowid()
        };

        // Try to insert member
        let result = conn.execute(
            "INSERT OR IGNORE INTO sets (key_id, member) VALUES (?1, ?2)",
            libsql::params![key_id, member.to_vec()],
        ).await.map_err(|e| KvError::Other(format!("Insert error: {}", e)))?;

        Ok(result as i64)
    }

    /// SMEMBERS key
    pub fn smembers(&self, key: &str) -> Result<Vec<Vec<u8>>> {
        self.runtime.block_on(self.smembers_async(key))
    }

    async fn smembers_async(&self, key: &str) -> Result<Vec<Vec<u8>>> {
        let conn = self.conn.read().await;
        let db = self.selected_db;

        let mut rows = conn.query(
            r#"SELECT s.member FROM sets s
               JOIN keys k ON s.key_id = k.id
               WHERE k.db = ?1 AND k.key = ?2"#,
            libsql::params![db, key.to_string()],
        ).await.map_err(|e| KvError::Other(format!("Query error: {}", e)))?;

        let mut result = Vec::new();
        while let Some(row) = rows.next().await.map_err(|e| KvError::Other(format!("Row error: {}", e)))? {
            let member: Vec<u8> = row.get(0).map_err(|e| KvError::Other(format!("Get error: {}", e)))?;
            result.push(member);
        }

        Ok(result)
    }

    // =========================================================================
    // SORTED SET OPERATIONS
    // =========================================================================

    /// ZADD key score member
    pub fn zadd(&self, key: &str, score: f64, member: &[u8]) -> Result<i64> {
        self.runtime.block_on(self.zadd_async(key, score, member))
    }

    async fn zadd_async(&self, key: &str, score: f64, member: &[u8]) -> Result<i64> {
        let conn = self.conn.write().await;
        let db = self.selected_db;
        let now = Self::now_ms();

        // Get or create key
        let mut rows = conn.query(
            "SELECT id, type FROM keys WHERE db = ?1 AND key = ?2",
            libsql::params![db, key.to_string()],
        ).await.map_err(|e| KvError::Other(format!("Query error: {}", e)))?;

        let key_id = if let Some(row) = rows.next().await.map_err(|e| KvError::Other(format!("Row error: {}", e)))? {
            let id: i64 = row.get(0).map_err(|e| KvError::Other(format!("Get error: {}", e)))?;
            let key_type: i32 = row.get(1).map_err(|e| KvError::Other(format!("Get error: {}", e)))?;
            if key_type != KeyType::ZSet as i32 {
                return Err(KvError::WrongType);
            }
            drop(rows);
            id
        } else {
            drop(rows);
            conn.execute(
                "INSERT INTO keys (db, key, type, created_at, updated_at) VALUES (?1, ?2, ?3, ?4, ?4)",
                libsql::params![db, key.to_string(), KeyType::ZSet as i32, now],
            ).await.map_err(|e| KvError::Other(format!("Insert error: {}", e)))?;
            conn.last_insert_rowid()
        };

        // Check if member exists
        let mut member_rows = conn.query(
            "SELECT 1 FROM zsets WHERE key_id = ?1 AND member = ?2",
            libsql::params![key_id, member.to_vec()],
        ).await.map_err(|e| KvError::Other(format!("Query error: {}", e)))?;

        let is_new = member_rows.next().await.map_err(|e| KvError::Other(format!("Row error: {}", e)))?.is_none();
        drop(member_rows);

        if is_new {
            conn.execute(
                "INSERT INTO zsets (key_id, member, score) VALUES (?1, ?2, ?3)",
                libsql::params![key_id, member.to_vec(), score],
            ).await.map_err(|e| KvError::Other(format!("Insert error: {}", e)))?;
            Ok(1)
        } else {
            conn.execute(
                "UPDATE zsets SET score = ?1 WHERE key_id = ?2 AND member = ?3",
                libsql::params![score, key_id, member.to_vec()],
            ).await.map_err(|e| KvError::Other(format!("Update error: {}", e)))?;
            Ok(0)
        }
    }

    /// ZRANGE key start stop
    pub fn zrange(&self, key: &str, start: i64, stop: i64) -> Result<Vec<Vec<u8>>> {
        self.runtime.block_on(self.zrange_async(key, start, stop))
    }

    async fn zrange_async(&self, key: &str, start: i64, stop: i64) -> Result<Vec<Vec<u8>>> {
        let conn = self.conn.read().await;
        let db = self.selected_db;

        // Get zset size
        let mut len_rows = conn.query(
            r#"SELECT COUNT(*) FROM zsets z
               JOIN keys k ON z.key_id = k.id
               WHERE k.db = ?1 AND k.key = ?2"#,
            libsql::params![db, key.to_string()],
        ).await.map_err(|e| KvError::Other(format!("Query error: {}", e)))?;

        let len = if let Some(row) = len_rows.next().await.map_err(|e| KvError::Other(format!("Row error: {}", e)))? {
            row.get::<i64>(0).map_err(|e| KvError::Other(format!("Get error: {}", e)))?
        } else {
            0
        };
        drop(len_rows);

        if len == 0 {
            return Ok(Vec::new());
        }

        // Normalize indices
        let real_start = if start < 0 { (len + start).max(0) } else { start.min(len) };
        let real_stop = if stop < 0 { len + stop + 1 } else { (stop + 1).min(len) };

        if real_start >= real_stop {
            return Ok(Vec::new());
        }

        let limit = real_stop - real_start;
        let mut rows = conn.query(
            r#"SELECT z.member FROM zsets z
               JOIN keys k ON z.key_id = k.id
               WHERE k.db = ?1 AND k.key = ?2
               ORDER BY z.score ASC, z.member ASC
               LIMIT ?3 OFFSET ?4"#,
            libsql::params![db, key.to_string(), limit, real_start],
        ).await.map_err(|e| KvError::Other(format!("Query error: {}", e)))?;

        let mut result = Vec::new();
        while let Some(row) = rows.next().await.map_err(|e| KvError::Other(format!("Row error: {}", e)))? {
            let member: Vec<u8> = row.get(0).map_err(|e| KvError::Other(format!("Get error: {}", e)))?;
            result.push(member);
        }

        Ok(result)
    }

    /// INCR key
    pub fn incr(&self, key: &str) -> Result<i64> {
        self.runtime.block_on(self.incr_async(key))
    }

    async fn incr_async(&self, key: &str) -> Result<i64> {
        // Get current value
        let current = self.get_async(key).await?;

        let new_value = match current {
            Some(bytes) => {
                let s = String::from_utf8(bytes).map_err(|_| KvError::NotInteger)?;
                let num: i64 = s.parse().map_err(|_| KvError::NotInteger)?;
                num + 1
            }
            None => 1,
        };

        self.set_async(key, new_value.to_string().as_bytes(), None).await?;
        Ok(new_value)
    }
}

// Clone implementation - creates a new session
impl Clone for LibsqlDb {
    fn clone(&self) -> Self {
        // Create a new connection for the clone
        Self::open_memory().expect("Failed to clone LibsqlDb")
    }
}
