//! Turso-backed Redis-compatible KV store
//!
//! This uses the turso crate - the Rust-native rewrite of SQLite by Turso.
//! Provides MVCC support for better concurrent write throughput.
//! Implements the core Redis commands needed for benchmarking.

use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::RwLock;

use crate::error::{KvError, Result};
use crate::types::KeyType;

/// Turso-backed database (Rust-native SQLite rewrite)
pub struct TursoDb {
    db: turso::Database,
    conn: RwLock<turso::Connection>,
    runtime: tokio::runtime::Handle,
    selected_db: i32,
}

impl TursoDb {
    /// Open or create a database at the given path
    pub fn open(path: &str) -> Result<Self> {
        let runtime = tokio::runtime::Handle::try_current().or_else(|_| {
            let rt = tokio::runtime::Runtime::new()
                .map_err(|e| KvError::Other(format!("Runtime error: {}", e)))?;
            Ok::<_, KvError>(rt.handle().clone())
        })?;

        runtime.block_on(Self::open_async(path))
    }

    async fn open_async(path: &str) -> Result<Self> {
        let db = turso::Builder::new_local(path)
            .build()
            .await
            .map_err(|e| KvError::Other(format!("Database open error: {}", e)))?;

        let conn = db
            .connect()
            .map_err(|e| KvError::Other(format!("Connection error: {}", e)))?;

        let instance = Self {
            db,
            conn: RwLock::new(conn),
            runtime: tokio::runtime::Handle::current(),
            selected_db: 0,
        };

        instance.migrate().await?;
        Ok(instance)
    }

    /// Open an in-memory database
    pub fn open_memory() -> Result<Self> {
        Self::open(":memory:")
    }

    /// Run schema migrations
    async fn migrate(&self) -> Result<()> {
        let conn = self.conn.write().await;

        // Keys table
        conn.execute(
            r#"CREATE TABLE IF NOT EXISTS keys (
                id INTEGER PRIMARY KEY,
                db INTEGER NOT NULL DEFAULT 0,
                key TEXT NOT NULL,
                type INTEGER NOT NULL,
                expire_at INTEGER,
                created_at INTEGER NOT NULL,
                updated_at INTEGER NOT NULL,
                UNIQUE(db, key)
            )"#,
            (),
        )
        .await
        .map_err(|e| KvError::Other(format!("Migration error: {}", e)))?;

        // Strings table
        conn.execute(
            r#"CREATE TABLE IF NOT EXISTS strings (
                key_id INTEGER PRIMARY KEY REFERENCES keys(id) ON DELETE CASCADE,
                value BLOB NOT NULL
            )"#,
            (),
        )
        .await
        .map_err(|e| KvError::Other(format!("Migration error: {}", e)))?;

        // Hashes table
        conn.execute(
            r#"CREATE TABLE IF NOT EXISTS hashes (
                id INTEGER PRIMARY KEY,
                key_id INTEGER NOT NULL REFERENCES keys(id) ON DELETE CASCADE,
                field TEXT NOT NULL,
                value BLOB NOT NULL,
                UNIQUE(key_id, field)
            )"#,
            (),
        )
        .await
        .map_err(|e| KvError::Other(format!("Migration error: {}", e)))?;

        // Lists table
        conn.execute(
            r#"CREATE TABLE IF NOT EXISTS lists (
                id INTEGER PRIMARY KEY,
                key_id INTEGER NOT NULL REFERENCES keys(id) ON DELETE CASCADE,
                idx REAL NOT NULL,
                value BLOB NOT NULL
            )"#,
            (),
        )
        .await
        .map_err(|e| KvError::Other(format!("Migration error: {}", e)))?;

        // Sets table
        conn.execute(
            r#"CREATE TABLE IF NOT EXISTS sets (
                id INTEGER PRIMARY KEY,
                key_id INTEGER NOT NULL REFERENCES keys(id) ON DELETE CASCADE,
                member BLOB NOT NULL,
                UNIQUE(key_id, member)
            )"#,
            (),
        )
        .await
        .map_err(|e| KvError::Other(format!("Migration error: {}", e)))?;

        // Sorted sets table
        conn.execute(
            r#"CREATE TABLE IF NOT EXISTS zsets (
                id INTEGER PRIMARY KEY,
                key_id INTEGER NOT NULL REFERENCES keys(id) ON DELETE CASCADE,
                member BLOB NOT NULL,
                score REAL NOT NULL,
                UNIQUE(key_id, member)
            )"#,
            (),
        )
        .await
        .map_err(|e| KvError::Other(format!("Migration error: {}", e)))?;

        // Create indexes
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_keys_db_key ON keys(db, key)",
            (),
        )
        .await
        .map_err(|e| KvError::Other(format!("Migration error: {}", e)))?;
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_hashes_key_id ON hashes(key_id)",
            (),
        )
        .await
        .map_err(|e| KvError::Other(format!("Migration error: {}", e)))?;
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_lists_key_id_idx ON lists(key_id, idx)",
            (),
        )
        .await
        .map_err(|e| KvError::Other(format!("Migration error: {}", e)))?;
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_sets_key_id ON sets(key_id)",
            (),
        )
        .await
        .map_err(|e| KvError::Other(format!("Migration error: {}", e)))?;
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_zsets_key_id_score ON zsets(key_id, score)",
            (),
        )
        .await
        .map_err(|e| KvError::Other(format!("Migration error: {}", e)))?;

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

    /// SET key value [EX seconds]
    pub fn set(&self, key: &str, value: &[u8], expire_seconds: Option<u64>) -> Result<()> {
        self.runtime
            .block_on(self.set_async(key, value, expire_seconds))
    }

    async fn set_async(&self, key: &str, value: &[u8], expire_seconds: Option<u64>) -> Result<()> {
        let conn = self.conn.write().await;
        let db = self.selected_db;
        let now = Self::now_ms();
        let expire_at = expire_seconds.map(|s| now + (s as i64 * 1000));

        // Check if key exists
        let mut stmt = conn
            .prepare("SELECT id, type FROM keys WHERE db = ?1 AND key = ?2")
            .await
            .map_err(|e| KvError::Other(format!("Prepare error: {}", e)))?;

        let mut rows = stmt
            .query([db.to_string(), key.to_string()])
            .await
            .map_err(|e| KvError::Other(format!("Query error: {}", e)))?;

        if let Some(row) = rows
            .next()
            .await
            .map_err(|e| KvError::Other(format!("Row error: {}", e)))?
        {
            let key_id: i64 = row
                .get(0)
                .map_err(|e| KvError::Other(format!("Get error: {}", e)))?;
            let key_type: i32 = row
                .get(1)
                .map_err(|e| KvError::Other(format!("Get error: {}", e)))?;

            if key_type != KeyType::String as i32 {
                // Delete old key and create new string
                conn.execute("DELETE FROM keys WHERE id = ?1", [key_id.to_string()])
                    .await
                    .map_err(|e| KvError::Other(format!("Delete error: {}", e)))?;
            } else {
                // Update existing string
                conn.execute(
                    "UPDATE strings SET value = ?1 WHERE key_id = ?2",
                    [
                        String::from_utf8_lossy(value).to_string(),
                        key_id.to_string(),
                    ],
                )
                .await
                .map_err(|e| KvError::Other(format!("Update error: {}", e)))?;

                conn.execute(
                    "UPDATE keys SET updated_at = ?1, expire_at = ?2 WHERE id = ?3",
                    [
                        now.to_string(),
                        expire_at
                            .map(|e| e.to_string())
                            .unwrap_or("NULL".to_string()),
                        key_id.to_string(),
                    ],
                )
                .await
                .map_err(|e| KvError::Other(format!("Update error: {}", e)))?;

                return Ok(());
            }
        }
        drop(rows);
        drop(stmt);

        // Insert new key
        conn.execute(
            "INSERT INTO keys (db, key, type, created_at, updated_at, expire_at) VALUES (?1, ?2, ?3, ?4, ?4, ?5)",
            [db.to_string(), key.to_string(), (KeyType::String as i32).to_string(), now.to_string(), expire_at.map(|e| e.to_string()).unwrap_or("NULL".to_string())],
        ).await.map_err(|e| KvError::Other(format!("Insert error: {}", e)))?;

        // Get the inserted key_id
        let mut stmt = conn
            .prepare("SELECT last_insert_rowid()")
            .await
            .map_err(|e| KvError::Other(format!("Prepare error: {}", e)))?;
        let mut rows = stmt
            .query(())
            .await
            .map_err(|e| KvError::Other(format!("Query error: {}", e)))?;
        let row = rows
            .next()
            .await
            .map_err(|e| KvError::Other(format!("Row error: {}", e)))?
            .ok_or_else(|| KvError::Other("No rowid".to_string()))?;
        let key_id: i64 = row
            .get(0)
            .map_err(|e| KvError::Other(format!("Get error: {}", e)))?;
        drop(rows);
        drop(stmt);

        // Insert string value
        conn.execute(
            "INSERT INTO strings (key_id, value) VALUES (?1, ?2)",
            [
                key_id.to_string(),
                String::from_utf8_lossy(value).to_string(),
            ],
        )
        .await
        .map_err(|e| KvError::Other(format!("Insert error: {}", e)))?;

        Ok(())
    }

    /// GET key
    pub fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
        self.runtime.block_on(self.get_async(key))
    }

    async fn get_async(&self, key: &str) -> Result<Option<Vec<u8>>> {
        let conn = self.conn.read().await;
        let db = self.selected_db;

        let mut stmt = conn
            .prepare(
                r#"SELECT s.value FROM strings s
               JOIN keys k ON s.key_id = k.id
               WHERE k.db = ?1 AND k.key = ?2 AND k.type = ?3"#,
            )
            .await
            .map_err(|e| KvError::Other(format!("Prepare error: {}", e)))?;

        let mut rows = stmt
            .query([
                db.to_string(),
                key.to_string(),
                (KeyType::String as i32).to_string(),
            ])
            .await
            .map_err(|e| KvError::Other(format!("Query error: {}", e)))?;

        if let Some(row) = rows
            .next()
            .await
            .map_err(|e| KvError::Other(format!("Row error: {}", e)))?
        {
            let value: String = row
                .get(0)
                .map_err(|e| KvError::Other(format!("Get error: {}", e)))?;
            return Ok(Some(value.into_bytes()));
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
        let mut stmt = conn
            .prepare("SELECT id, type FROM keys WHERE db = ?1 AND key = ?2")
            .await
            .map_err(|e| KvError::Other(format!("Prepare error: {}", e)))?;
        let mut rows = stmt
            .query([db.to_string(), key.to_string()])
            .await
            .map_err(|e| KvError::Other(format!("Query error: {}", e)))?;

        let key_id = if let Some(row) = rows
            .next()
            .await
            .map_err(|e| KvError::Other(format!("Row error: {}", e)))?
        {
            let id: i64 = row
                .get(0)
                .map_err(|e| KvError::Other(format!("Get error: {}", e)))?;
            let key_type: i32 = row
                .get(1)
                .map_err(|e| KvError::Other(format!("Get error: {}", e)))?;
            if key_type != KeyType::Hash as i32 {
                return Err(KvError::WrongType);
            }
            drop(rows);
            drop(stmt);
            id
        } else {
            drop(rows);
            drop(stmt);
            conn.execute(
                "INSERT INTO keys (db, key, type, created_at, updated_at) VALUES (?1, ?2, ?3, ?4, ?4)",
                [db.to_string(), key.to_string(), (KeyType::Hash as i32).to_string(), now.to_string()],
            ).await.map_err(|e| KvError::Other(format!("Insert error: {}", e)))?;

            // Get last insert rowid
            let mut stmt = conn
                .prepare("SELECT last_insert_rowid()")
                .await
                .map_err(|e| KvError::Other(format!("Prepare error: {}", e)))?;
            let mut rows = stmt
                .query(())
                .await
                .map_err(|e| KvError::Other(format!("Query error: {}", e)))?;
            let row = rows
                .next()
                .await
                .map_err(|e| KvError::Other(format!("Row error: {}", e)))?
                .ok_or_else(|| KvError::Other("No rowid".to_string()))?;
            let id: i64 = row
                .get(0)
                .map_err(|e| KvError::Other(format!("Get error: {}", e)))?;
            drop(rows);
            drop(stmt);
            id
        };

        let mut new_count = 0i64;

        for (field, value) in pairs {
            // Check if field exists
            let mut stmt = conn
                .prepare("SELECT 1 FROM hashes WHERE key_id = ?1 AND field = ?2")
                .await
                .map_err(|e| KvError::Other(format!("Prepare error: {}", e)))?;
            let mut rows = stmt
                .query([key_id.to_string(), field.to_string()])
                .await
                .map_err(|e| KvError::Other(format!("Query error: {}", e)))?;

            let is_new = rows
                .next()
                .await
                .map_err(|e| KvError::Other(format!("Row error: {}", e)))?
                .is_none();
            drop(rows);
            drop(stmt);

            if is_new {
                conn.execute(
                    "INSERT INTO hashes (key_id, field, value) VALUES (?1, ?2, ?3)",
                    [
                        key_id.to_string(),
                        field.to_string(),
                        String::from_utf8_lossy(value).to_string(),
                    ],
                )
                .await
                .map_err(|e| KvError::Other(format!("Insert error: {}", e)))?;
                new_count += 1;
            } else {
                conn.execute(
                    "UPDATE hashes SET value = ?1 WHERE key_id = ?2 AND field = ?3",
                    [
                        String::from_utf8_lossy(value).to_string(),
                        key_id.to_string(),
                        field.to_string(),
                    ],
                )
                .await
                .map_err(|e| KvError::Other(format!("Update error: {}", e)))?;
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

        let mut stmt = conn
            .prepare(
                r#"SELECT h.value FROM hashes h
               JOIN keys k ON h.key_id = k.id
               WHERE k.db = ?1 AND k.key = ?2 AND h.field = ?3"#,
            )
            .await
            .map_err(|e| KvError::Other(format!("Prepare error: {}", e)))?;

        let mut rows = stmt
            .query([db.to_string(), key.to_string(), field.to_string()])
            .await
            .map_err(|e| KvError::Other(format!("Query error: {}", e)))?;

        if let Some(row) = rows
            .next()
            .await
            .map_err(|e| KvError::Other(format!("Row error: {}", e)))?
        {
            let value: String = row
                .get(0)
                .map_err(|e| KvError::Other(format!("Get error: {}", e)))?;
            return Ok(Some(value.into_bytes()));
        }

        Ok(None)
    }

    /// HGETALL key
    pub fn hgetall(&self, key: &str) -> Result<Vec<(String, Vec<u8>)>> {
        self.runtime.block_on(self.hgetall_async(key))
    }

    async fn hgetall_async(&self, key: &str) -> Result<Vec<(String, Vec<u8>)>> {
        let conn = self.conn.read().await;
        let db = self.selected_db;

        let mut stmt = conn
            .prepare(
                r#"SELECT h.field, h.value FROM hashes h
               JOIN keys k ON h.key_id = k.id
               WHERE k.db = ?1 AND k.key = ?2"#,
            )
            .await
            .map_err(|e| KvError::Other(format!("Prepare error: {}", e)))?;

        let mut rows = stmt
            .query([db.to_string(), key.to_string()])
            .await
            .map_err(|e| KvError::Other(format!("Query error: {}", e)))?;

        let mut result = Vec::new();
        while let Some(row) = rows
            .next()
            .await
            .map_err(|e| KvError::Other(format!("Row error: {}", e)))?
        {
            let field: String = row
                .get(0)
                .map_err(|e| KvError::Other(format!("Get error: {}", e)))?;
            let value: String = row
                .get(1)
                .map_err(|e| KvError::Other(format!("Get error: {}", e)))?;
            result.push((field, value.into_bytes()));
        }

        Ok(result)
    }

    // =========================================================================
    // LIST OPERATIONS
    // =========================================================================

    /// LPUSH key value [value ...]
    pub fn lpush(&self, key: &str, values: &[&[u8]]) -> Result<i64> {
        self.runtime.block_on(self.lpush_async(key, values))
    }

    async fn lpush_async(&self, key: &str, values: &[&[u8]]) -> Result<i64> {
        if values.is_empty() {
            return Ok(0);
        }

        let conn = self.conn.write().await;
        let db = self.selected_db;
        let now = Self::now_ms();

        // Get or create key
        let mut stmt = conn
            .prepare("SELECT id, type FROM keys WHERE db = ?1 AND key = ?2")
            .await
            .map_err(|e| KvError::Other(format!("Prepare error: {}", e)))?;
        let mut rows = stmt
            .query([db.to_string(), key.to_string()])
            .await
            .map_err(|e| KvError::Other(format!("Query error: {}", e)))?;

        let key_id = if let Some(row) = rows
            .next()
            .await
            .map_err(|e| KvError::Other(format!("Row error: {}", e)))?
        {
            let id: i64 = row
                .get(0)
                .map_err(|e| KvError::Other(format!("Get error: {}", e)))?;
            let key_type: i32 = row
                .get(1)
                .map_err(|e| KvError::Other(format!("Get error: {}", e)))?;
            if key_type != KeyType::List as i32 {
                return Err(KvError::WrongType);
            }
            drop(rows);
            drop(stmt);
            id
        } else {
            drop(rows);
            drop(stmt);
            conn.execute(
                "INSERT INTO keys (db, key, type, created_at, updated_at) VALUES (?1, ?2, ?3, ?4, ?4)",
                [db.to_string(), key.to_string(), (KeyType::List as i32).to_string(), now.to_string()],
            ).await.map_err(|e| KvError::Other(format!("Insert error: {}", e)))?;

            let mut stmt = conn
                .prepare("SELECT last_insert_rowid()")
                .await
                .map_err(|e| KvError::Other(format!("Prepare error: {}", e)))?;
            let mut rows = stmt
                .query(())
                .await
                .map_err(|e| KvError::Other(format!("Query error: {}", e)))?;
            let row = rows
                .next()
                .await
                .map_err(|e| KvError::Other(format!("Row error: {}", e)))?
                .ok_or_else(|| KvError::Other("No rowid".to_string()))?;
            let id: i64 = row
                .get(0)
                .map_err(|e| KvError::Other(format!("Get error: {}", e)))?;
            drop(rows);
            drop(stmt);
            id
        };

        // Get current min idx
        let mut stmt = conn
            .prepare("SELECT MIN(idx) FROM lists WHERE key_id = ?1")
            .await
            .map_err(|e| KvError::Other(format!("Prepare error: {}", e)))?;
        let mut rows = stmt
            .query([key_id.to_string()])
            .await
            .map_err(|e| KvError::Other(format!("Query error: {}", e)))?;
        let min_idx: f64 = if let Some(row) = rows
            .next()
            .await
            .map_err(|e| KvError::Other(format!("Row error: {}", e)))?
        {
            row.get::<f64>(0).unwrap_or(0.0)
        } else {
            0.0
        };
        drop(rows);
        drop(stmt);

        let mut idx = min_idx;
        for value in values {
            idx -= 1.0;
            conn.execute(
                "INSERT INTO lists (key_id, idx, value) VALUES (?1, ?2, ?3)",
                [
                    key_id.to_string(),
                    idx.to_string(),
                    String::from_utf8_lossy(value).to_string(),
                ],
            )
            .await
            .map_err(|e| KvError::Other(format!("Insert error: {}", e)))?;
        }

        // Get new count
        let mut stmt = conn
            .prepare("SELECT COUNT(*) FROM lists WHERE key_id = ?1")
            .await
            .map_err(|e| KvError::Other(format!("Prepare error: {}", e)))?;
        let mut rows = stmt
            .query([key_id.to_string()])
            .await
            .map_err(|e| KvError::Other(format!("Query error: {}", e)))?;
        let count: i64 = if let Some(row) = rows
            .next()
            .await
            .map_err(|e| KvError::Other(format!("Row error: {}", e)))?
        {
            row.get(0).unwrap_or(0)
        } else {
            0
        };

        Ok(count)
    }

    /// LPOP key [count]
    pub fn lpop(&self, key: &str, count: Option<usize>) -> Result<Vec<Vec<u8>>> {
        self.runtime.block_on(self.lpop_async(key, count))
    }

    async fn lpop_async(&self, key: &str, count: Option<usize>) -> Result<Vec<Vec<u8>>> {
        let conn = self.conn.write().await;
        let db = self.selected_db;
        let count = count.unwrap_or(1);

        let mut stmt = conn
            .prepare(
                r#"SELECT l.id, l.value FROM lists l
               JOIN keys k ON l.key_id = k.id
               WHERE k.db = ?1 AND k.key = ?2
               ORDER BY l.idx ASC LIMIT ?3"#,
            )
            .await
            .map_err(|e| KvError::Other(format!("Prepare error: {}", e)))?;

        let mut rows = stmt
            .query([db.to_string(), key.to_string(), count.to_string()])
            .await
            .map_err(|e| KvError::Other(format!("Query error: {}", e)))?;

        let mut result = Vec::new();
        let mut ids_to_delete = Vec::new();

        while let Some(row) = rows
            .next()
            .await
            .map_err(|e| KvError::Other(format!("Row error: {}", e)))?
        {
            let id: i64 = row
                .get(0)
                .map_err(|e| KvError::Other(format!("Get error: {}", e)))?;
            let value: String = row
                .get(1)
                .map_err(|e| KvError::Other(format!("Get error: {}", e)))?;
            ids_to_delete.push(id);
            result.push(value.into_bytes());
        }
        drop(rows);
        drop(stmt);

        for id in ids_to_delete {
            conn.execute("DELETE FROM lists WHERE id = ?1", [id.to_string()])
                .await
                .map_err(|e| KvError::Other(format!("Delete error: {}", e)))?;
        }

        Ok(result)
    }

    // =========================================================================
    // SET OPERATIONS
    // =========================================================================

    /// SADD key member [member ...]
    pub fn sadd(&self, key: &str, members: &[&[u8]]) -> Result<i64> {
        self.runtime.block_on(self.sadd_async(key, members))
    }

    async fn sadd_async(&self, key: &str, members: &[&[u8]]) -> Result<i64> {
        if members.is_empty() {
            return Ok(0);
        }

        let conn = self.conn.write().await;
        let db = self.selected_db;
        let now = Self::now_ms();

        // Get or create key
        let mut stmt = conn
            .prepare("SELECT id, type FROM keys WHERE db = ?1 AND key = ?2")
            .await
            .map_err(|e| KvError::Other(format!("Prepare error: {}", e)))?;
        let mut rows = stmt
            .query([db.to_string(), key.to_string()])
            .await
            .map_err(|e| KvError::Other(format!("Query error: {}", e)))?;

        let key_id = if let Some(row) = rows
            .next()
            .await
            .map_err(|e| KvError::Other(format!("Row error: {}", e)))?
        {
            let id: i64 = row
                .get(0)
                .map_err(|e| KvError::Other(format!("Get error: {}", e)))?;
            let key_type: i32 = row
                .get(1)
                .map_err(|e| KvError::Other(format!("Get error: {}", e)))?;
            if key_type != KeyType::Set as i32 {
                return Err(KvError::WrongType);
            }
            drop(rows);
            drop(stmt);
            id
        } else {
            drop(rows);
            drop(stmt);
            conn.execute(
                "INSERT INTO keys (db, key, type, created_at, updated_at) VALUES (?1, ?2, ?3, ?4, ?4)",
                [db.to_string(), key.to_string(), (KeyType::Set as i32).to_string(), now.to_string()],
            ).await.map_err(|e| KvError::Other(format!("Insert error: {}", e)))?;

            let mut stmt = conn
                .prepare("SELECT last_insert_rowid()")
                .await
                .map_err(|e| KvError::Other(format!("Prepare error: {}", e)))?;
            let mut rows = stmt
                .query(())
                .await
                .map_err(|e| KvError::Other(format!("Query error: {}", e)))?;
            let row = rows
                .next()
                .await
                .map_err(|e| KvError::Other(format!("Row error: {}", e)))?
                .ok_or_else(|| KvError::Other("No rowid".to_string()))?;
            let id: i64 = row
                .get(0)
                .map_err(|e| KvError::Other(format!("Get error: {}", e)))?;
            drop(rows);
            drop(stmt);
            id
        };

        let mut added = 0i64;
        for member in members {
            // Try to insert, ignore if duplicate
            let result = conn
                .execute(
                    "INSERT OR IGNORE INTO sets (key_id, member) VALUES (?1, ?2)",
                    [
                        key_id.to_string(),
                        String::from_utf8_lossy(member).to_string(),
                    ],
                )
                .await;

            if result.is_ok() {
                added += 1;
            }
        }

        Ok(added)
    }

    /// SMEMBERS key
    pub fn smembers(&self, key: &str) -> Result<Vec<Vec<u8>>> {
        self.runtime.block_on(self.smembers_async(key))
    }

    async fn smembers_async(&self, key: &str) -> Result<Vec<Vec<u8>>> {
        let conn = self.conn.read().await;
        let db = self.selected_db;

        let mut stmt = conn
            .prepare(
                r#"SELECT s.member FROM sets s
               JOIN keys k ON s.key_id = k.id
               WHERE k.db = ?1 AND k.key = ?2"#,
            )
            .await
            .map_err(|e| KvError::Other(format!("Prepare error: {}", e)))?;

        let mut rows = stmt
            .query([db.to_string(), key.to_string()])
            .await
            .map_err(|e| KvError::Other(format!("Query error: {}", e)))?;

        let mut result = Vec::new();
        while let Some(row) = rows
            .next()
            .await
            .map_err(|e| KvError::Other(format!("Row error: {}", e)))?
        {
            let member: String = row
                .get(0)
                .map_err(|e| KvError::Other(format!("Get error: {}", e)))?;
            result.push(member.into_bytes());
        }

        Ok(result)
    }

    // =========================================================================
    // SORTED SET OPERATIONS
    // =========================================================================

    /// ZADD key score member [score member ...]
    pub fn zadd(&self, key: &str, members: &[(f64, &[u8])]) -> Result<i64> {
        self.runtime.block_on(self.zadd_async(key, members))
    }

    async fn zadd_async(&self, key: &str, members: &[(f64, &[u8])]) -> Result<i64> {
        if members.is_empty() {
            return Ok(0);
        }

        let conn = self.conn.write().await;
        let db = self.selected_db;
        let now = Self::now_ms();

        // Get or create key
        let mut stmt = conn
            .prepare("SELECT id, type FROM keys WHERE db = ?1 AND key = ?2")
            .await
            .map_err(|e| KvError::Other(format!("Prepare error: {}", e)))?;
        let mut rows = stmt
            .query([db.to_string(), key.to_string()])
            .await
            .map_err(|e| KvError::Other(format!("Query error: {}", e)))?;

        let key_id = if let Some(row) = rows
            .next()
            .await
            .map_err(|e| KvError::Other(format!("Row error: {}", e)))?
        {
            let id: i64 = row
                .get(0)
                .map_err(|e| KvError::Other(format!("Get error: {}", e)))?;
            let key_type: i32 = row
                .get(1)
                .map_err(|e| KvError::Other(format!("Get error: {}", e)))?;
            if key_type != KeyType::ZSet as i32 {
                return Err(KvError::WrongType);
            }
            drop(rows);
            drop(stmt);
            id
        } else {
            drop(rows);
            drop(stmt);
            conn.execute(
                "INSERT INTO keys (db, key, type, created_at, updated_at) VALUES (?1, ?2, ?3, ?4, ?4)",
                [db.to_string(), key.to_string(), (KeyType::ZSet as i32).to_string(), now.to_string()],
            ).await.map_err(|e| KvError::Other(format!("Insert error: {}", e)))?;

            let mut stmt = conn
                .prepare("SELECT last_insert_rowid()")
                .await
                .map_err(|e| KvError::Other(format!("Prepare error: {}", e)))?;
            let mut rows = stmt
                .query(())
                .await
                .map_err(|e| KvError::Other(format!("Query error: {}", e)))?;
            let row = rows
                .next()
                .await
                .map_err(|e| KvError::Other(format!("Row error: {}", e)))?
                .ok_or_else(|| KvError::Other("No rowid".to_string()))?;
            let id: i64 = row
                .get(0)
                .map_err(|e| KvError::Other(format!("Get error: {}", e)))?;
            drop(rows);
            drop(stmt);
            id
        };

        let mut added = 0i64;
        for (score, member) in members {
            // Use INSERT OR REPLACE to handle updates
            conn.execute(
                "INSERT OR REPLACE INTO zsets (key_id, member, score) VALUES (?1, ?2, ?3)",
                [
                    key_id.to_string(),
                    String::from_utf8_lossy(member).to_string(),
                    score.to_string(),
                ],
            )
            .await
            .map_err(|e| KvError::Other(format!("Insert error: {}", e)))?;
            added += 1;
        }

        Ok(added)
    }

    /// ZRANGE key start stop
    pub fn zrange(&self, key: &str, start: i64, stop: i64) -> Result<Vec<Vec<u8>>> {
        self.runtime.block_on(self.zrange_async(key, start, stop))
    }

    async fn zrange_async(&self, key: &str, start: i64, stop: i64) -> Result<Vec<Vec<u8>>> {
        let conn = self.conn.read().await;
        let db = self.selected_db;

        // For simplicity, just get all members ordered by score
        let mut stmt = conn
            .prepare(
                r#"SELECT z.member FROM zsets z
               JOIN keys k ON z.key_id = k.id
               WHERE k.db = ?1 AND k.key = ?2
               ORDER BY z.score ASC"#,
            )
            .await
            .map_err(|e| KvError::Other(format!("Prepare error: {}", e)))?;

        let mut rows = stmt
            .query([db.to_string(), key.to_string()])
            .await
            .map_err(|e| KvError::Other(format!("Query error: {}", e)))?;

        let mut all_members = Vec::new();
        while let Some(row) = rows
            .next()
            .await
            .map_err(|e| KvError::Other(format!("Row error: {}", e)))?
        {
            let member: String = row
                .get(0)
                .map_err(|e| KvError::Other(format!("Get error: {}", e)))?;
            all_members.push(member.into_bytes());
        }

        // Handle negative indices
        let len = all_members.len() as i64;
        let start = if start < 0 {
            (len + start).max(0)
        } else {
            start.min(len)
        } as usize;
        let stop = if stop < 0 {
            (len + stop).max(0)
        } else {
            stop.min(len - 1)
        } as usize;

        if start > stop || start >= all_members.len() {
            return Ok(Vec::new());
        }

        Ok(all_members[start..=stop.min(all_members.len() - 1)].to_vec())
    }

    // =========================================================================
    // NUMERIC OPERATIONS
    // =========================================================================

    /// INCR key
    pub fn incr(&self, key: &str) -> Result<i64> {
        self.runtime.block_on(self.incr_async(key))
    }

    async fn incr_async(&self, key: &str) -> Result<i64> {
        let current = self.get_async(key).await?;
        let value = match current {
            Some(bytes) => {
                let s = String::from_utf8_lossy(&bytes);
                s.parse::<i64>()
                    .map_err(|_| KvError::Other("value is not an integer".to_string()))?
            }
            None => 0,
        };

        let new_value = value + 1;
        self.set_async(key, new_value.to_string().as_bytes(), None)
            .await?;
        Ok(new_value)
    }
}
