use rusqlite::{params, Connection};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::broadcast;

use crate::error::{KvError, Result};
use crate::types::{ConsumerGroupInfo, ConsumerInfo, KeyInfo, KeyType, PendingEntry, PendingSummary, SetOptions, StreamEntry, StreamId, StreamInfo, ZMember};

/// Default autovacuum interval in milliseconds (60 seconds)
const DEFAULT_AUTOVACUUM_INTERVAL_MS: i64 = 60_000;

/// Shared database backend (SQLite connection)
struct DbCore {
    conn: Mutex<Connection>,
    /// Whether autovacuum is enabled (default: true)
    autovacuum_enabled: AtomicBool,
    /// Last cleanup timestamp in milliseconds (shared across all sessions)
    last_cleanup: AtomicI64,
    /// Autovacuum interval in milliseconds (configurable, default: 60s)
    autovacuum_interval_ms: AtomicI64,
    /// Optional notifier for server mode (None for embedded mode)
    /// Maps key name to broadcast sender for notifications
    /// Uses RwLock to allow updating after creation (for server mode attachment)
    notifier: RwLock<Option<Arc<RwLock<HashMap<String, broadcast::Sender<()>>>>>>,
}

/// Database session with per-instance selected database.
///
/// Each `Db` instance has its own `selected_db` state, allowing multiple
/// sessions to operate on different Redis databases concurrently.
///
/// # Example
/// ```
/// use redlite::Db;
///
/// let db = Db::open(":memory:").unwrap();
/// db.set("key", b"value", None).unwrap();
///
/// // Create another session for a different database
/// let mut db2 = db.session();
/// db2.select(1).unwrap();
/// db2.set("key", b"other", None).unwrap();
///
/// // Original session still sees db 0
/// assert_eq!(db.get("key").unwrap(), Some(b"value".to_vec()));
/// ```
#[derive(Clone)]
pub struct Db {
    core: Arc<DbCore>,
    selected_db: i32,
}

impl Db {
    /// Open or create a database at the given path
    pub fn open(path: &str) -> Result<Self> {
        let conn = Connection::open(path)?;

        // Enable WAL mode and optimize pragmas
        conn.execute_batch(
            "PRAGMA journal_mode = WAL;
             PRAGMA synchronous = NORMAL;
             PRAGMA foreign_keys = ON;
             PRAGMA busy_timeout = 5000;",
        )?;

        let core = Arc::new(DbCore {
            conn: Mutex::new(conn),
            autovacuum_enabled: AtomicBool::new(true),
            last_cleanup: AtomicI64::new(0),
            autovacuum_interval_ms: AtomicI64::new(DEFAULT_AUTOVACUUM_INTERVAL_MS),
            notifier: RwLock::new(None),
        });

        let db = Self {
            core,
            selected_db: 0,
        };

        db.migrate()?;
        Ok(db)
    }

    /// Open an in-memory database (useful for testing)
    pub fn open_memory() -> Result<Self> {
        Self::open(":memory:")
    }

    /// Create a new session sharing the same database backend.
    /// The new session starts at database 0.
    pub fn session(&self) -> Self {
        Self {
            core: Arc::clone(&self.core),
            selected_db: 0,
        }
    }

    /// Run schema migrations
    fn migrate(&self) -> Result<()> {
        let conn = self.core.conn.lock().unwrap_or_else(|e| e.into_inner());
        conn.execute_batch(include_str!("schema.sql"))?;
        Ok(())
    }

    /// Select a database (0-15)
    pub fn select(&mut self, db: i32) -> Result<()> {
        if !(0..=15).contains(&db) {
            return Err(KvError::SyntaxError);
        }
        self.selected_db = db;
        Ok(())
    }

    /// Get current database number
    pub fn current_db(&self) -> i32 {
        self.selected_db
    }

    /// Enable or disable autovacuum (automatic cleanup of expired keys)
    pub fn set_autovacuum(&self, enabled: bool) {
        self.core.autovacuum_enabled.store(enabled, Ordering::Relaxed);
    }

    /// Check if autovacuum is enabled
    pub fn autovacuum_enabled(&self) -> bool {
        self.core.autovacuum_enabled.load(Ordering::Relaxed)
    }

    /// Set autovacuum interval in milliseconds (default: 60000 = 60s)
    pub fn set_autovacuum_interval(&self, interval_ms: i64) {
        self.core
            .autovacuum_interval_ms
            .store(interval_ms.max(1000), Ordering::Relaxed); // Min 1 second
    }

    /// Get current autovacuum interval in milliseconds
    pub fn autovacuum_interval(&self) -> i64 {
        self.core.autovacuum_interval_ms.load(Ordering::Relaxed)
    }

    /// Maybe run autovacuum if enabled and interval has passed.
    /// Called on read operations. Uses atomic compare-exchange to ensure
    /// only one connection does cleanup per interval.
    fn maybe_autovacuum(&self) {
        if !self.core.autovacuum_enabled.load(Ordering::Relaxed) {
            return;
        }

        let now = Self::now_ms();
        let last = self.core.last_cleanup.load(Ordering::Relaxed);
        let interval = self.core.autovacuum_interval_ms.load(Ordering::Relaxed);

        if now - last < interval {
            return;
        }

        // Try to claim cleanup duty (only one connection wins)
        if self
            .core
            .last_cleanup
            .compare_exchange(last, now, Ordering::Relaxed, Ordering::Relaxed)
            .is_ok()
        {
            // We won - delete all expired keys (across all dbs, no SQLite VACUUM)
            let conn = self.core.conn.lock().unwrap_or_else(|e| e.into_inner());
            let _ = conn.execute(
                "DELETE FROM keys WHERE expire_at IS NOT NULL AND expire_at <= ?1",
                params![now],
            );
        }
    }

    /// Current time in milliseconds since epoch
    pub fn now_ms() -> i64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64
    }

    /// GET key
    pub fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
        self.maybe_autovacuum();
        let conn = self.core.conn.lock().unwrap_or_else(|e| e.into_inner());
        let db = self.selected_db;
        let now = Self::now_ms();

        // First check if the key exists and get its type
        let key_info: std::result::Result<(i64, i32, Option<i64>), _> = conn.query_row(
            "SELECT id, type, expire_at FROM keys WHERE db = ?1 AND key = ?2",
            params![db, key],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        );

        match key_info {
            Ok((key_id, key_type, expire_at)) => {
                // Check expiration
                if let Some(exp) = expire_at {
                    if exp <= now {
                        // Lazy delete - drop lock first
                        drop(conn);
                        let _ = self.del(&[key]);
                        return Ok(None);
                    }
                }

                // Check type - must be string
                if key_type != KeyType::String as i32 {
                    return Err(KvError::WrongType);
                }

                // Get the string value
                let result: std::result::Result<Vec<u8>, _> = conn.query_row(
                    "SELECT value FROM strings WHERE key_id = ?1",
                    params![key_id],
                    |row| row.get(0),
                );

                match result {
                    Ok(value) => Ok(Some(value)),
                    Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
                    Err(e) => Err(e.into()),
                }
            }
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    /// SET key value [TTL]
    pub fn set(&self, key: &str, value: &[u8], ttl: Option<Duration>) -> Result<()> {
        self.set_opts(
            key,
            value,
            SetOptions {
                ttl,
                ..Default::default()
            },
        )
        .map(|_| ())
    }

    /// SET with options, returns whether the key was set
    pub fn set_opts(&self, key: &str, value: &[u8], opts: SetOptions) -> Result<bool> {
        let conn = self.core.conn.lock().unwrap_or_else(|e| e.into_inner());
        let db = self.selected_db;
        let now = Self::now_ms();

        let expire_at: Option<i64> = opts.ttl.map(|d| now + d.as_millis() as i64);

        // Check NX/XX conditions
        let exists: bool = conn
            .query_row(
                "SELECT 1 FROM keys WHERE db = ?1 AND key = ?2
                 AND (expire_at IS NULL OR expire_at > ?3)",
                params![db, key, now],
                |_| Ok(true),
            )
            .unwrap_or(false);

        if opts.nx && exists {
            return Ok(false);
        }
        if opts.xx && !exists {
            return Ok(false);
        }

        // Upsert key
        conn.execute(
            "INSERT INTO keys (db, key, type, expire_at, updated_at)
             VALUES (?1, ?2, ?3, ?4, ?5)
             ON CONFLICT(db, key) DO UPDATE SET
                 type = excluded.type,
                 expire_at = excluded.expire_at,
                 updated_at = excluded.updated_at",
            params![db, key, KeyType::String as i32, expire_at, now],
        )?;

        // Get key_id
        let key_id: i64 = conn.query_row(
            "SELECT id FROM keys WHERE db = ?1 AND key = ?2",
            params![db, key],
            |row| row.get(0),
        )?;

        // Upsert value
        conn.execute(
            "INSERT INTO strings (key_id, value) VALUES (?1, ?2)
             ON CONFLICT(key_id) DO UPDATE SET value = excluded.value",
            params![key_id, value],
        )?;

        Ok(true)
    }

    /// DEL key [key ...]
    pub fn del(&self, keys: &[&str]) -> Result<i64> {
        if keys.is_empty() {
            return Ok(0);
        }

        let conn = self.core.conn.lock().unwrap_or_else(|e| e.into_inner());
        let db = self.selected_db;

        let placeholders: String = (0..keys.len())
            .map(|i| format!("?{}", i + 2))
            .collect::<Vec<_>>()
            .join(",");
        let sql = format!(
            "DELETE FROM keys WHERE db = ?1 AND key IN ({})",
            placeholders
        );

        let mut stmt = conn.prepare(&sql)?;

        // Build params: [db, key1, key2, ...]
        let mut params_vec: Vec<&dyn rusqlite::ToSql> = vec![&db];
        for key in keys {
            params_vec.push(key);
        }

        let count = stmt.execute(params_vec.as_slice())?;
        Ok(count as i64)
    }

    /// TYPE key - returns key type or None if not found
    pub fn key_type(&self, key: &str) -> Result<Option<KeyType>> {
        let conn = self.core.conn.lock().unwrap_or_else(|e| e.into_inner());
        let db = self.selected_db;
        let now = Self::now_ms();

        let result: std::result::Result<i32, _> = conn.query_row(
            "SELECT type FROM keys
             WHERE db = ?1 AND key = ?2
             AND (expire_at IS NULL OR expire_at > ?3)",
            params![db, key, now],
            |row| row.get(0),
        );

        match result {
            Ok(type_int) => Ok(KeyType::from_i32(type_int)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    /// TTL key - returns remaining TTL in seconds (-2 if no key, -1 if no expiry)
    pub fn ttl(&self, key: &str) -> Result<i64> {
        let conn = self.core.conn.lock().unwrap_or_else(|e| e.into_inner());
        let db = self.selected_db;
        let now = Self::now_ms();

        let result: std::result::Result<Option<i64>, _> = conn.query_row(
            "SELECT expire_at FROM keys
             WHERE db = ?1 AND key = ?2
             AND (expire_at IS NULL OR expire_at > ?3)",
            params![db, key, now],
            |row| row.get(0),
        );

        match result {
            Ok(Some(expire_at)) => Ok((expire_at - now) / 1000),
            Ok(None) => Ok(-1), // No expiry
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(-2), // Key not found
            Err(e) => Err(e.into()),
        }
    }

    /// PTTL key - returns remaining TTL in milliseconds (-2 if no key, -1 if no expiry)
    pub fn pttl(&self, key: &str) -> Result<i64> {
        let conn = self.core.conn.lock().unwrap_or_else(|e| e.into_inner());
        let db = self.selected_db;
        let now = Self::now_ms();

        let result: std::result::Result<Option<i64>, _> = conn.query_row(
            "SELECT expire_at FROM keys
             WHERE db = ?1 AND key = ?2
             AND (expire_at IS NULL OR expire_at > ?3)",
            params![db, key, now],
            |row| row.get(0),
        );

        match result {
            Ok(Some(expire_at)) => Ok(expire_at - now),
            Ok(None) => Ok(-1), // No expiry
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(-2), // Key not found
            Err(e) => Err(e.into()),
        }
    }

    /// EXISTS key [key ...] - count how many keys exist
    pub fn exists(&self, keys: &[&str]) -> Result<i64> {
        self.maybe_autovacuum();
        if keys.is_empty() {
            return Ok(0);
        }

        let conn = self.core.conn.lock().unwrap_or_else(|e| e.into_inner());
        let db = self.selected_db;
        let now = Self::now_ms();

        // Count each key individually (duplicates count separately per Redis semantics)
        let mut count: i64 = 0;
        for key in keys {
            let exists: bool = conn
                .query_row(
                    "SELECT 1 FROM keys
                     WHERE db = ?1 AND key = ?2
                     AND (expire_at IS NULL OR expire_at > ?3)",
                    params![db, key, now],
                    |_| Ok(true),
                )
                .unwrap_or(false);
            if exists {
                count += 1;
            }
        }

        Ok(count)
    }

    /// EXPIRE key seconds - set TTL on key
    pub fn expire(&self, key: &str, seconds: i64) -> Result<bool> {
        let conn = self.core.conn.lock().unwrap_or_else(|e| e.into_inner());
        let db = self.selected_db;
        let now = Self::now_ms();
        let expire_at = now + (seconds * 1000);

        let count = conn.execute(
            "UPDATE keys
             SET expire_at = ?1, updated_at = ?2
             WHERE db = ?3 AND key = ?4
             AND (expire_at IS NULL OR expire_at > ?2)",
            params![expire_at, now, db, key],
        )?;

        Ok(count > 0)
    }

    /// KEYS pattern - return all keys matching glob pattern
    pub fn keys(&self, pattern: &str) -> Result<Vec<String>> {
        let conn = self.core.conn.lock().unwrap_or_else(|e| e.into_inner());
        let db = self.selected_db;
        let now = Self::now_ms();

        let mut stmt = conn.prepare(
            "SELECT key FROM keys
             WHERE db = ?1
             AND (expire_at IS NULL OR expire_at > ?2)
             AND key GLOB ?3",
        )?;

        let rows = stmt.query_map(params![db, now, pattern], |row| row.get(0))?;

        let mut keys = Vec::new();
        for row in rows {
            keys.push(row?);
        }

        Ok(keys)
    }

    /// SCAN cursor [MATCH pattern] [COUNT count] - cursor-based iteration
    pub fn scan(
        &self,
        cursor: u64,
        pattern: Option<&str>,
        count: usize,
    ) -> Result<(u64, Vec<String>)> {
        let conn = self.core.conn.lock().unwrap_or_else(|e| e.into_inner());
        let db = self.selected_db;
        let now = Self::now_ms();

        let sql = match pattern {
            Some(_) => {
                "SELECT key FROM keys
                 WHERE db = ?1
                 AND (expire_at IS NULL OR expire_at > ?2)
                 AND key GLOB ?3
                 ORDER BY id
                 LIMIT ?4 OFFSET ?5"
            }
            None => {
                "SELECT key FROM keys
                 WHERE db = ?1
                 AND (expire_at IS NULL OR expire_at > ?2)
                 ORDER BY id
                 LIMIT ?3 OFFSET ?4"
            }
        };

        let mut stmt = conn.prepare(sql)?;

        let rows: Vec<String> = match pattern {
            Some(p) => {
                let iter = stmt.query_map(
                    params![db, now, p, count as i64, cursor as i64],
                    |row| row.get(0),
                )?;
                iter.filter_map(|r| r.ok()).collect()
            }
            None => {
                let iter = stmt.query_map(
                    params![db, now, count as i64, cursor as i64],
                    |row| row.get(0),
                )?;
                iter.filter_map(|r| r.ok()).collect()
            }
        };

        // Calculate next cursor
        let next_cursor = if rows.len() < count {
            0 // Done iterating
        } else {
            cursor + count as u64
        };

        Ok((next_cursor, rows))
    }

    // --- Session 3: String Operations ---

    /// INCR key - increment by 1, creates key with value 0 if not exists
    pub fn incr(&self, key: &str) -> Result<i64> {
        self.incrby(key, 1)
    }

    /// DECR key - decrement by 1
    pub fn decr(&self, key: &str) -> Result<i64> {
        self.incrby(key, -1)
    }

    /// INCRBY key increment - increment by integer amount
    pub fn incrby(&self, key: &str, increment: i64) -> Result<i64> {
        let conn = self.core.conn.lock().unwrap_or_else(|e| e.into_inner());
        let db = self.selected_db;
        let now = Self::now_ms();

        // First check if the key exists and get its type
        let key_info: std::result::Result<(i64, i32, Option<i64>), _> = conn.query_row(
            "SELECT id, type, expire_at FROM keys WHERE db = ?1 AND key = ?2",
            params![db, key],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        );

        let (current_val, preserve_ttl): (i64, Option<i64>) = match key_info {
            Ok((key_id, key_type, expire_at)) => {
                // Check expiration
                if let Some(exp) = expire_at {
                    if exp <= now {
                        // Expired - treat as new key
                        (0, None)
                    } else {
                        // Check type - must be string
                        if key_type != KeyType::String as i32 {
                            return Err(KvError::WrongType);
                        }
                        // Get the current value
                        let value: Vec<u8> = conn
                            .query_row(
                                "SELECT value FROM strings WHERE key_id = ?1",
                                params![key_id],
                                |row| row.get(0),
                            )
                            .unwrap_or_default();
                        let s = std::str::from_utf8(&value).map_err(|_| KvError::NotInteger)?;
                        let val = s.parse().map_err(|_| KvError::NotInteger)?;
                        (val, Some(exp))
                    }
                } else {
                    // Check type - must be string
                    if key_type != KeyType::String as i32 {
                        return Err(KvError::WrongType);
                    }
                    // Get the current value
                    let value: Vec<u8> = conn
                        .query_row(
                            "SELECT value FROM strings WHERE key_id = ?1",
                            params![key_id],
                            |row| row.get(0),
                        )
                        .unwrap_or_default();
                    let s = std::str::from_utf8(&value).map_err(|_| KvError::NotInteger)?;
                    let val = s.parse().map_err(|_| KvError::NotInteger)?;
                    (val, None)
                }
            }
            Err(rusqlite::Error::QueryReturnedNoRows) => (0, None),
            Err(e) => return Err(e.into()),
        };

        let new_val = current_val + increment;
        let new_val_bytes = new_val.to_string().into_bytes();

        // Upsert key and get key_id in one statement (RETURNING eliminates extra SELECT)
        let key_id: i64 = conn.query_row(
            "INSERT INTO keys (db, key, type, expire_at, updated_at)
             VALUES (?1, ?2, ?3, ?4, ?5)
             ON CONFLICT(db, key) DO UPDATE SET
                 type = excluded.type,
                 expire_at = excluded.expire_at,
                 updated_at = excluded.updated_at
             RETURNING id",
            params![db, key, KeyType::String as i32, preserve_ttl, now],
            |row| row.get(0),
        )?;

        // Upsert value
        conn.execute(
            "INSERT INTO strings (key_id, value) VALUES (?1, ?2)
             ON CONFLICT(key_id) DO UPDATE SET value = excluded.value",
            params![key_id, new_val_bytes],
        )?;

        Ok(new_val)
    }

    /// DECRBY key decrement - decrement by integer amount
    pub fn decrby(&self, key: &str, decrement: i64) -> Result<i64> {
        self.incrby(key, -decrement)
    }

    /// INCRBYFLOAT key increment - increment by float amount
    pub fn incrbyfloat(&self, key: &str, increment: f64) -> Result<String> {
        let conn = self.core.conn.lock().unwrap_or_else(|e| e.into_inner());
        let db = self.selected_db;
        let now = Self::now_ms();

        // First check if the key exists and get its type
        let key_info: std::result::Result<(i64, i32, Option<i64>), _> = conn.query_row(
            "SELECT id, type, expire_at FROM keys WHERE db = ?1 AND key = ?2",
            params![db, key],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        );

        let (current_val, preserve_ttl): (f64, Option<i64>) = match key_info {
            Ok((key_id, key_type, expire_at)) => {
                // Check expiration
                if let Some(exp) = expire_at {
                    if exp <= now {
                        // Expired - delete the old key first, then treat as non-existent
                        conn.execute("DELETE FROM keys WHERE id = ?1", params![key_id])?;
                        (0.0, None)
                    } else {
                        // Check type - must be string
                        if key_type != KeyType::String as i32 {
                            return Err(KvError::WrongType);
                        }
                        // Get the current value
                        let value: Vec<u8> = conn
                            .query_row(
                                "SELECT value FROM strings WHERE key_id = ?1",
                                params![key_id],
                                |row| row.get(0),
                            )
                            .unwrap_or_default();
                        let s = std::str::from_utf8(&value).map_err(|_| KvError::NotFloat)?;
                        let val = s.parse().map_err(|_| KvError::NotFloat)?;
                        (val, Some(exp))
                    }
                } else {
                    // Check type - must be string
                    if key_type != KeyType::String as i32 {
                        return Err(KvError::WrongType);
                    }
                    // Get the current value
                    let value: Vec<u8> = conn
                        .query_row(
                            "SELECT value FROM strings WHERE key_id = ?1",
                            params![key_id],
                            |row| row.get(0),
                        )
                        .unwrap_or_default();
                    let s = std::str::from_utf8(&value).map_err(|_| KvError::NotFloat)?;
                    let val = s.parse().map_err(|_| KvError::NotFloat)?;
                    (val, None)
                }
            }
            Err(rusqlite::Error::QueryReturnedNoRows) => (0.0, None),
            Err(e) => return Err(e.into()),
        };

        let new_val = current_val + increment;

        // Format like Redis: remove trailing zeros, ensure decimal point for integers
        let formatted = if new_val.fract() == 0.0 {
            format!("{:.0}", new_val)
        } else {
            // Trim trailing zeros but keep at least one decimal place
            let s = format!("{}", new_val);
            s.trim_end_matches('0').trim_end_matches('.').to_string()
        };

        let new_val_bytes = formatted.as_bytes();

        // Upsert key and get key_id in one statement
        let key_id: i64 = conn.query_row(
            "INSERT INTO keys (db, key, type, expire_at, updated_at)
             VALUES (?1, ?2, ?3, ?4, ?5)
             ON CONFLICT(db, key) DO UPDATE SET
                 type = excluded.type,
                 expire_at = excluded.expire_at,
                 updated_at = excluded.updated_at
             RETURNING id",
            params![db, key, KeyType::String as i32, preserve_ttl, now],
            |row| row.get(0),
        )?;

        // Upsert value
        conn.execute(
            "INSERT INTO strings (key_id, value) VALUES (?1, ?2)
             ON CONFLICT(key_id) DO UPDATE SET value = excluded.value",
            params![key_id, new_val_bytes],
        )?;

        Ok(formatted)
    }

    /// MGET key [key ...] - get multiple keys
    pub fn mget(&self, keys: &[&str]) -> Vec<Option<Vec<u8>>> {
        keys.iter()
            .map(|k| self.get(k).unwrap_or(None))
            .collect()
    }

    /// MSET key value [key value ...] - set multiple key-value pairs atomically
    pub fn mset(&self, pairs: &[(&str, &[u8])]) -> Result<()> {
        if pairs.is_empty() {
            return Ok(());
        }

        let conn = self.core.conn.lock().unwrap_or_else(|e| e.into_inner());
        let db = self.selected_db;
        let now = Self::now_ms();

        // Use a transaction for atomicity
        conn.execute("BEGIN IMMEDIATE", [])?;

        let result = (|| -> Result<()> {
            for (key, value) in pairs {
                // Upsert key
                conn.execute(
                    "INSERT INTO keys (db, key, type, expire_at, updated_at)
                     VALUES (?1, ?2, ?3, NULL, ?4)
                     ON CONFLICT(db, key) DO UPDATE SET
                         type = excluded.type,
                         expire_at = excluded.expire_at,
                         updated_at = excluded.updated_at",
                    params![db, key, KeyType::String as i32, now],
                )?;

                // Get key_id
                let key_id: i64 = conn.query_row(
                    "SELECT id FROM keys WHERE db = ?1 AND key = ?2",
                    params![db, key],
                    |row| row.get(0),
                )?;

                // Upsert value
                conn.execute(
                    "INSERT INTO strings (key_id, value) VALUES (?1, ?2)
                     ON CONFLICT(key_id) DO UPDATE SET value = excluded.value",
                    params![key_id, *value],
                )?;
            }
            Ok(())
        })();

        match result {
            Ok(()) => {
                conn.execute("COMMIT", [])?;
                Ok(())
            }
            Err(e) => {
                let _ = conn.execute("ROLLBACK", []);
                Err(e)
            }
        }
    }

    /// APPEND key value - append to string, create if not exists
    pub fn append(&self, key: &str, value: &[u8]) -> Result<i64> {
        let conn = self.core.conn.lock().unwrap_or_else(|e| e.into_inner());
        let db = self.selected_db;
        let now = Self::now_ms();

        // First check if the key exists and get its type
        let key_info: std::result::Result<(i64, i32, Option<i64>), _> = conn.query_row(
            "SELECT id, type, expire_at FROM keys WHERE db = ?1 AND key = ?2",
            params![db, key],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        );

        match key_info {
            Ok((key_id, key_type, expire_at)) => {
                // Check expiration
                if let Some(exp) = expire_at {
                    if exp <= now {
                        // Expired - create new key
                        drop(conn);
                        self.set(key, value, None)?;
                        return Ok(value.len() as i64);
                    }
                }

                // Check type - must be string
                if key_type != KeyType::String as i32 {
                    return Err(KvError::WrongType);
                }

                // Get the current string value
                let current_value: Vec<u8> = conn
                    .query_row(
                        "SELECT value FROM strings WHERE key_id = ?1",
                        params![key_id],
                        |row| row.get(0),
                    )
                    .unwrap_or_default();

                let mut new_value = current_value;
                new_value.extend_from_slice(value);
                let new_len = new_value.len() as i64;

                // Update value (preserve existing TTL - Redis behavior)
                conn.execute(
                    "INSERT INTO strings (key_id, value) VALUES (?1, ?2)
                     ON CONFLICT(key_id) DO UPDATE SET value = excluded.value",
                    params![key_id, new_value],
                )?;

                // Update timestamp but preserve expiration
                conn.execute(
                    "UPDATE keys SET updated_at = ?1 WHERE id = ?2",
                    params![now, key_id],
                )?;

                Ok(new_len)
            }
            Err(rusqlite::Error::QueryReturnedNoRows) => {
                // Create new key
                drop(conn);
                self.set(key, value, None)?;
                Ok(value.len() as i64)
            }
            Err(e) => Err(e.into()),
        }
    }

    /// STRLEN key - get string length (0 if not exists)
    pub fn strlen(&self, key: &str) -> Result<i64> {
        match self.get(key)? {
            Some(value) => Ok(value.len() as i64),
            None => Ok(0),
        }
    }

    /// GETRANGE key start end - get substring (supports negative indices)
    pub fn getrange(&self, key: &str, start: i64, end: i64) -> Result<Vec<u8>> {
        let value = match self.get(key)? {
            Some(v) => v,
            None => return Ok(Vec::new()),
        };

        let len = value.len() as i64;
        if len == 0 {
            return Ok(Vec::new());
        }

        // Convert negative indices
        let start = if start < 0 {
            (len + start).max(0)
        } else {
            start.min(len)
        };

        let end = if end < 0 {
            (len + end).max(0)
        } else {
            end.min(len - 1)
        };

        // Check valid range
        if start > end || start >= len {
            return Ok(Vec::new());
        }

        Ok(value[start as usize..=end as usize].to_vec())
    }

    /// SETRANGE key offset value - overwrite part of string at offset
    pub fn setrange(&self, key: &str, offset: i64, value: &[u8]) -> Result<i64> {
        if offset < 0 {
            return Err(KvError::OutOfRange);
        }

        let offset = offset as usize;
        let current = self.get(key)?.unwrap_or_default();

        // Pad with zeros if needed
        let mut new_value = if current.len() < offset {
            let mut v = current;
            v.resize(offset, 0);
            v
        } else {
            current
        };

        // Extend if value goes beyond current length
        let end = offset + value.len();
        if new_value.len() < end {
            new_value.resize(end, 0);
        }

        // Copy value at offset
        new_value[offset..end].copy_from_slice(value);

        let new_len = new_value.len() as i64;
        self.set(key, &new_value, None)?;

        Ok(new_len)
    }

    // --- Session 6: Hash Operations ---

    /// Helper to get or create a hash key, returns key_id
    fn get_or_create_hash_key(&self, conn: &Connection, key: &str) -> Result<i64> {
        let db = self.selected_db;
        let now = Self::now_ms();

        // Check if key exists and is correct type
        let existing: std::result::Result<(i64, i32, Option<i64>), _> = conn.query_row(
            "SELECT id, type, expire_at FROM keys WHERE db = ?1 AND key = ?2",
            params![db, key],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        );

        match existing {
            Ok((key_id, key_type, expire_at)) => {
                // Check expiration
                if let Some(exp) = expire_at {
                    if exp <= now {
                        // Expired - delete and create new
                        conn.execute("DELETE FROM keys WHERE id = ?1", params![key_id])?;
                        return self.create_hash_key(conn, key);
                    }
                }
                // Check type
                if key_type != KeyType::Hash as i32 {
                    return Err(KvError::WrongType);
                }
                Ok(key_id)
            }
            Err(rusqlite::Error::QueryReturnedNoRows) => self.create_hash_key(conn, key),
            Err(e) => Err(e.into()),
        }
    }

    fn create_hash_key(&self, conn: &Connection, key: &str) -> Result<i64> {
        let db = self.selected_db;
        let now = Self::now_ms();

        conn.execute(
            "INSERT INTO keys (db, key, type, updated_at) VALUES (?1, ?2, ?3, ?4)",
            params![db, key, KeyType::Hash as i32, now],
        )?;

        Ok(conn.last_insert_rowid())
    }

    /// Helper to get hash key_id if it exists and is not expired
    fn get_hash_key_id(&self, conn: &Connection, key: &str) -> Result<Option<i64>> {
        let db = self.selected_db;
        let now = Self::now_ms();

        let result: std::result::Result<(i64, i32, Option<i64>), _> = conn.query_row(
            "SELECT id, type, expire_at FROM keys WHERE db = ?1 AND key = ?2",
            params![db, key],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        );

        match result {
            Ok((key_id, key_type, expire_at)) => {
                // Check expiration
                if let Some(exp) = expire_at {
                    if exp <= now {
                        return Ok(None);
                    }
                }
                // Check type
                if key_type != KeyType::Hash as i32 {
                    return Err(KvError::WrongType);
                }
                Ok(Some(key_id))
            }
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    /// HSET key field value [field value ...] - set hash field(s), returns number of new fields
    pub fn hset(&self, key: &str, pairs: &[(&str, &[u8])]) -> Result<i64> {
        if pairs.is_empty() {
            return Ok(0);
        }

        let conn = self.core.conn.lock().unwrap_or_else(|e| e.into_inner());
        let key_id = self.get_or_create_hash_key(&conn, key)?;

        let mut new_fields = 0i64;
        for (field, value) in pairs {
            // Check if field exists
            let exists: bool = conn
                .query_row(
                    "SELECT 1 FROM hashes WHERE key_id = ?1 AND field = ?2",
                    params![key_id, field],
                    |_| Ok(true),
                )
                .unwrap_or(false);

            if !exists {
                new_fields += 1;
            }

            // Upsert field
            conn.execute(
                "INSERT INTO hashes (key_id, field, value) VALUES (?1, ?2, ?3)
                 ON CONFLICT(key_id, field) DO UPDATE SET value = excluded.value",
                params![key_id, field, value],
            )?;
        }

        // Update key timestamp
        let now = Self::now_ms();
        conn.execute(
            "UPDATE keys SET updated_at = ?1 WHERE id = ?2",
            params![now, key_id],
        )?;

        Ok(new_fields)
    }

    /// HGET key field - get hash field value
    pub fn hget(&self, key: &str, field: &str) -> Result<Option<Vec<u8>>> {
        self.maybe_autovacuum();
        let conn = self.core.conn.lock().unwrap_or_else(|e| e.into_inner());

        let key_id = match self.get_hash_key_id(&conn, key)? {
            Some(id) => id,
            None => return Ok(None),
        };

        let result: std::result::Result<Vec<u8>, _> = conn.query_row(
            "SELECT value FROM hashes WHERE key_id = ?1 AND field = ?2",
            params![key_id, field],
            |row| row.get(0),
        );

        match result {
            Ok(value) => Ok(Some(value)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    /// HMGET key field [field ...] - get multiple hash field values
    pub fn hmget(&self, key: &str, fields: &[&str]) -> Result<Vec<Option<Vec<u8>>>> {
        let conn = self.core.conn.lock().unwrap_or_else(|e| e.into_inner());

        let key_id = match self.get_hash_key_id(&conn, key)? {
            Some(id) => id,
            None => return Ok(vec![None; fields.len()]),
        };

        let mut results = Vec::with_capacity(fields.len());
        for field in fields {
            let result: std::result::Result<Vec<u8>, _> = conn.query_row(
                "SELECT value FROM hashes WHERE key_id = ?1 AND field = ?2",
                params![key_id, field],
                |row| row.get(0),
            );

            match result {
                Ok(value) => results.push(Some(value)),
                Err(rusqlite::Error::QueryReturnedNoRows) => results.push(None),
                Err(e) => return Err(e.into()),
            }
        }

        Ok(results)
    }

    /// HGETALL key - get all field-value pairs
    pub fn hgetall(&self, key: &str) -> Result<Vec<(String, Vec<u8>)>> {
        let conn = self.core.conn.lock().unwrap_or_else(|e| e.into_inner());

        let key_id = match self.get_hash_key_id(&conn, key)? {
            Some(id) => id,
            None => return Ok(Vec::new()),
        };

        let mut stmt = conn.prepare("SELECT field, value FROM hashes WHERE key_id = ?1")?;
        let rows = stmt.query_map(params![key_id], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, Vec<u8>>(1)?))
        })?;

        let mut results = Vec::new();
        for row in rows {
            results.push(row?);
        }

        Ok(results)
    }

    /// HDEL key field [field ...] - delete hash fields, returns count deleted
    pub fn hdel(&self, key: &str, fields: &[&str]) -> Result<i64> {
        if fields.is_empty() {
            return Ok(0);
        }

        let conn = self.core.conn.lock().unwrap_or_else(|e| e.into_inner());

        let key_id = match self.get_hash_key_id(&conn, key)? {
            Some(id) => id,
            None => return Ok(0),
        };

        let placeholders: String = (0..fields.len())
            .map(|i| format!("?{}", i + 2))
            .collect::<Vec<_>>()
            .join(",");
        let sql = format!(
            "DELETE FROM hashes WHERE key_id = ?1 AND field IN ({})",
            placeholders
        );

        let mut stmt = conn.prepare(&sql)?;

        let mut params_vec: Vec<&dyn rusqlite::ToSql> = vec![&key_id];
        for field in fields {
            params_vec.push(field);
        }

        let count = stmt.execute(params_vec.as_slice())?;

        // Check if hash is now empty and delete key if so
        let remaining: i64 = conn.query_row(
            "SELECT COUNT(*) FROM hashes WHERE key_id = ?1",
            params![key_id],
            |row| row.get(0),
        )?;

        if remaining == 0 {
            conn.execute("DELETE FROM keys WHERE id = ?1", params![key_id])?;
        }

        Ok(count as i64)
    }

    /// HEXISTS key field - check if field exists in hash
    pub fn hexists(&self, key: &str, field: &str) -> Result<bool> {
        let conn = self.core.conn.lock().unwrap_or_else(|e| e.into_inner());

        let key_id = match self.get_hash_key_id(&conn, key)? {
            Some(id) => id,
            None => return Ok(false),
        };

        let exists: bool = conn
            .query_row(
                "SELECT 1 FROM hashes WHERE key_id = ?1 AND field = ?2",
                params![key_id, field],
                |_| Ok(true),
            )
            .unwrap_or(false);

        Ok(exists)
    }

    /// HKEYS key - get all field names
    pub fn hkeys(&self, key: &str) -> Result<Vec<String>> {
        let conn = self.core.conn.lock().unwrap_or_else(|e| e.into_inner());

        let key_id = match self.get_hash_key_id(&conn, key)? {
            Some(id) => id,
            None => return Ok(Vec::new()),
        };

        let mut stmt = conn.prepare("SELECT field FROM hashes WHERE key_id = ?1")?;
        let rows = stmt.query_map(params![key_id], |row| row.get(0))?;

        let mut results = Vec::new();
        for row in rows {
            results.push(row?);
        }

        Ok(results)
    }

    /// HVALS key - get all values
    pub fn hvals(&self, key: &str) -> Result<Vec<Vec<u8>>> {
        let conn = self.core.conn.lock().unwrap_or_else(|e| e.into_inner());

        let key_id = match self.get_hash_key_id(&conn, key)? {
            Some(id) => id,
            None => return Ok(Vec::new()),
        };

        let mut stmt = conn.prepare("SELECT value FROM hashes WHERE key_id = ?1")?;
        let rows = stmt.query_map(params![key_id], |row| row.get(0))?;

        let mut results = Vec::new();
        for row in rows {
            results.push(row?);
        }

        Ok(results)
    }

    /// HLEN key - get number of fields in hash
    pub fn hlen(&self, key: &str) -> Result<i64> {
        let conn = self.core.conn.lock().unwrap_or_else(|e| e.into_inner());

        let key_id = match self.get_hash_key_id(&conn, key)? {
            Some(id) => id,
            None => return Ok(0),
        };

        let count: i64 = conn.query_row(
            "SELECT COUNT(*) FROM hashes WHERE key_id = ?1",
            params![key_id],
            |row| row.get(0),
        )?;

        Ok(count)
    }

    /// HINCRBY key field increment - increment hash field by integer
    pub fn hincrby(&self, key: &str, field: &str, increment: i64) -> Result<i64> {
        let conn = self.core.conn.lock().unwrap_or_else(|e| e.into_inner());
        let key_id = self.get_or_create_hash_key(&conn, key)?;

        // Get current value
        let result: std::result::Result<Vec<u8>, _> = conn.query_row(
            "SELECT value FROM hashes WHERE key_id = ?1 AND field = ?2",
            params![key_id, field],
            |row| row.get(0),
        );

        let current_val: i64 = match result {
            Ok(value) => {
                let s = std::str::from_utf8(&value).map_err(|_| KvError::NotInteger)?;
                s.parse().map_err(|_| KvError::NotInteger)?
            }
            Err(rusqlite::Error::QueryReturnedNoRows) => 0,
            Err(e) => return Err(e.into()),
        };

        let new_val = current_val + increment;
        let new_val_bytes = new_val.to_string().into_bytes();

        // Upsert field
        conn.execute(
            "INSERT INTO hashes (key_id, field, value) VALUES (?1, ?2, ?3)
             ON CONFLICT(key_id, field) DO UPDATE SET value = excluded.value",
            params![key_id, field, new_val_bytes],
        )?;

        // Update key timestamp
        let now = Self::now_ms();
        conn.execute(
            "UPDATE keys SET updated_at = ?1 WHERE id = ?2",
            params![now, key_id],
        )?;

        Ok(new_val)
    }

    /// HINCRBYFLOAT key field increment - increment hash field by float
    pub fn hincrbyfloat(&self, key: &str, field: &str, increment: f64) -> Result<String> {
        let conn = self.core.conn.lock().unwrap_or_else(|e| e.into_inner());
        let key_id = self.get_or_create_hash_key(&conn, key)?;

        // Get current value
        let result: std::result::Result<Vec<u8>, _> = conn.query_row(
            "SELECT value FROM hashes WHERE key_id = ?1 AND field = ?2",
            params![key_id, field],
            |row| row.get(0),
        );

        let current_val: f64 = match result {
            Ok(value) => {
                let s = std::str::from_utf8(&value).map_err(|_| KvError::NotFloat)?;
                s.parse().map_err(|_| KvError::NotFloat)?
            }
            Err(rusqlite::Error::QueryReturnedNoRows) => 0.0,
            Err(e) => return Err(e.into()),
        };

        let new_val = current_val + increment;

        // Format like Redis
        let formatted = if new_val.fract() == 0.0 {
            format!("{:.0}", new_val)
        } else {
            let s = format!("{}", new_val);
            s.trim_end_matches('0').trim_end_matches('.').to_string()
        };

        let new_val_bytes = formatted.as_bytes();

        // Upsert field
        conn.execute(
            "INSERT INTO hashes (key_id, field, value) VALUES (?1, ?2, ?3)
             ON CONFLICT(key_id, field) DO UPDATE SET value = excluded.value",
            params![key_id, field, new_val_bytes],
        )?;

        // Update key timestamp
        let now = Self::now_ms();
        conn.execute(
            "UPDATE keys SET updated_at = ?1 WHERE id = ?2",
            params![now, key_id],
        )?;

        Ok(formatted)
    }

    /// HSETNX key field value - set field only if it doesn't exist
    pub fn hsetnx(&self, key: &str, field: &str, value: &[u8]) -> Result<bool> {
        let conn = self.core.conn.lock().unwrap_or_else(|e| e.into_inner());
        let key_id = self.get_or_create_hash_key(&conn, key)?;

        // Check if field exists
        let exists: bool = conn
            .query_row(
                "SELECT 1 FROM hashes WHERE key_id = ?1 AND field = ?2",
                params![key_id, field],
                |_| Ok(true),
            )
            .unwrap_or(false);

        if exists {
            return Ok(false);
        }

        // Insert field
        conn.execute(
            "INSERT INTO hashes (key_id, field, value) VALUES (?1, ?2, ?3)",
            params![key_id, field, value],
        )?;

        // Update key timestamp
        let now = Self::now_ms();
        conn.execute(
            "UPDATE keys SET updated_at = ?1 WHERE id = ?2",
            params![now, key_id],
        )?;

        Ok(true)
    }

    // --- Session 7: List Operations ---

    /// Gap size for list positioning (allows efficient inserts without reindexing)
    const LIST_GAP: i64 = 1_000_000;

    /// Threshold for triggering rebalance (90% of i64 range)
    const LIST_POS_MIN_THRESHOLD: i64 = i64::MIN / 10 * 9; // -8_301_034_833_169_298_227
    const LIST_POS_MAX_THRESHOLD: i64 = i64::MAX / 10 * 9; // 8_301_034_833_169_298_227

    /// Rebalance list positions to prevent overflow
    /// Reassigns all positions starting from 0 with fresh gaps
    fn rebalance_list(&self, conn: &Connection, key_id: i64) -> Result<()> {
        // Get all values in order
        let mut stmt = conn.prepare(
            "SELECT pos, value FROM lists WHERE key_id = ?1 ORDER BY pos ASC",
        )?;
        let rows = stmt.query_map(params![key_id], |row| {
            Ok((row.get::<_, i64>(0)?, row.get::<_, Vec<u8>>(1)?))
        })?;

        let items: Vec<(i64, Vec<u8>)> = rows.filter_map(|r| r.ok()).collect();

        if items.is_empty() {
            return Ok(());
        }

        // Delete all existing entries
        conn.execute("DELETE FROM lists WHERE key_id = ?1", params![key_id])?;

        // Reinsert with fresh positions starting from LIST_GAP
        for (i, (_, value)) in items.iter().enumerate() {
            let new_pos = ((i as i64) + 1) * Self::LIST_GAP;
            conn.execute(
                "INSERT INTO lists (key_id, pos, value) VALUES (?1, ?2, ?3)",
                params![key_id, new_pos, value],
            )?;
        }

        Ok(())
    }

    /// Helper to get or create a list key, returns key_id
    fn get_or_create_list_key(&self, conn: &Connection, key: &str) -> Result<i64> {
        let db = self.selected_db;
        let now = Self::now_ms();

        let existing: std::result::Result<(i64, i32, Option<i64>), _> = conn.query_row(
            "SELECT id, type, expire_at FROM keys WHERE db = ?1 AND key = ?2",
            params![db, key],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        );

        match existing {
            Ok((key_id, key_type, expire_at)) => {
                // Check expiration
                if let Some(exp) = expire_at {
                    if exp <= now {
                        // Expired - delete and create new
                        conn.execute("DELETE FROM keys WHERE id = ?1", params![key_id])?;
                        return self.create_list_key(conn, key);
                    }
                }
                // Check type
                if key_type != KeyType::List as i32 {
                    return Err(KvError::WrongType);
                }
                Ok(key_id)
            }
            Err(rusqlite::Error::QueryReturnedNoRows) => self.create_list_key(conn, key),
            Err(e) => Err(e.into()),
        }
    }

    fn create_list_key(&self, conn: &Connection, key: &str) -> Result<i64> {
        let db = self.selected_db;
        let now = Self::now_ms();

        conn.execute(
            "INSERT INTO keys (db, key, type, updated_at) VALUES (?1, ?2, ?3, ?4)",
            params![db, key, KeyType::List as i32, now],
        )?;

        Ok(conn.last_insert_rowid())
    }

    /// Helper to get list key_id if it exists and is not expired
    fn get_list_key_id(&self, conn: &Connection, key: &str) -> Result<Option<i64>> {
        let db = self.selected_db;
        let now = Self::now_ms();

        let result: std::result::Result<(i64, i32, Option<i64>), _> = conn.query_row(
            "SELECT id, type, expire_at FROM keys WHERE db = ?1 AND key = ?2",
            params![db, key],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        );

        match result {
            Ok((key_id, key_type, expire_at)) => {
                // Check expiration
                if let Some(exp) = expire_at {
                    if exp <= now {
                        return Ok(None);
                    }
                }
                // Check type
                if key_type != KeyType::List as i32 {
                    return Err(KvError::WrongType);
                }
                Ok(Some(key_id))
            }
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    /// LPUSH key element [element ...] - prepend elements to list, returns length
    pub fn lpush(&self, key: &str, values: &[&[u8]]) -> Result<i64> {
        if values.is_empty() {
            return Ok(0);
        }

        let conn = self.core.conn.lock().unwrap_or_else(|e| e.into_inner());
        let key_id = self.get_or_create_list_key(&conn, key)?;

        // Get current min position (or start at LIST_GAP if empty)
        let mut min_pos: i64 = conn
            .query_row(
                "SELECT MIN(pos) FROM lists WHERE key_id = ?1",
                params![key_id],
                |row| row.get::<_, Option<i64>>(0),
            )
            .unwrap_or(None)
            .unwrap_or(Self::LIST_GAP);

        // Check if we would overflow - rebalance if needed
        let new_min = min_pos.saturating_sub((values.len() as i64) * Self::LIST_GAP);
        if new_min < Self::LIST_POS_MIN_THRESHOLD {
            self.rebalance_list(&conn, key_id)?;
            // Get fresh min position after rebalance
            min_pos = conn
                .query_row(
                    "SELECT MIN(pos) FROM lists WHERE key_id = ?1",
                    params![key_id],
                    |row| row.get::<_, Option<i64>>(0),
                )
                .unwrap_or(None)
                .unwrap_or(Self::LIST_GAP);
        }

        // Insert values in reverse order (so first value ends up at head)
        for (i, value) in values.iter().enumerate() {
            let pos = min_pos - ((i as i64 + 1) * Self::LIST_GAP);
            conn.execute(
                "INSERT INTO lists (key_id, pos, value) VALUES (?1, ?2, ?3)",
                params![key_id, pos, value],
            )?;
        }

        // Get and return new length
        let length: i64 = conn.query_row(
            "SELECT COUNT(*) FROM lists WHERE key_id = ?1",
            params![key_id],
            |row| row.get(0),
        )?;

        // Update key timestamp
        let now = Self::now_ms();
        conn.execute(
            "UPDATE keys SET updated_at = ?1 WHERE id = ?2",
            params![now, key_id],
        )?;

        // Release lock before async notification
        drop(conn);

        // Notify waiting readers in server mode
        if self.is_server_mode() {
            let key = key.to_string();
            let db = self.clone();
            tokio::spawn(async move {
                let _ = db.notify_key(&key).await;
            });
        }

        Ok(length)
    }

    /// RPUSH key element [element ...] - append elements to list, returns length
    pub fn rpush(&self, key: &str, values: &[&[u8]]) -> Result<i64> {
        if values.is_empty() {
            return Ok(0);
        }

        let conn = self.core.conn.lock().unwrap_or_else(|e| e.into_inner());
        let key_id = self.get_or_create_list_key(&conn, key)?;

        // Get current max position (or start at 0 if empty)
        let mut max_pos: i64 = conn
            .query_row(
                "SELECT MAX(pos) FROM lists WHERE key_id = ?1",
                params![key_id],
                |row| row.get::<_, Option<i64>>(0),
            )
            .unwrap_or(None)
            .unwrap_or(0);

        // Check if we would overflow - rebalance if needed
        let new_max = max_pos.saturating_add((values.len() as i64) * Self::LIST_GAP);
        if new_max > Self::LIST_POS_MAX_THRESHOLD {
            self.rebalance_list(&conn, key_id)?;
            // Get fresh max position after rebalance
            max_pos = conn
                .query_row(
                    "SELECT MAX(pos) FROM lists WHERE key_id = ?1",
                    params![key_id],
                    |row| row.get::<_, Option<i64>>(0),
                )
                .unwrap_or(None)
                .unwrap_or(0);
        }

        // Insert values in order
        for (i, value) in values.iter().enumerate() {
            let pos = max_pos + ((i as i64 + 1) * Self::LIST_GAP);
            conn.execute(
                "INSERT INTO lists (key_id, pos, value) VALUES (?1, ?2, ?3)",
                params![key_id, pos, value],
            )?;
        }

        // Get and return new length
        let length: i64 = conn.query_row(
            "SELECT COUNT(*) FROM lists WHERE key_id = ?1",
            params![key_id],
            |row| row.get(0),
        )?;

        // Update key timestamp
        let now = Self::now_ms();
        conn.execute(
            "UPDATE keys SET updated_at = ?1 WHERE id = ?2",
            params![now, key_id],
        )?;

        // Release lock before async notification
        drop(conn);

        // Notify waiting readers in server mode
        if self.is_server_mode() {
            let key = key.to_string();
            let db = self.clone();
            tokio::spawn(async move {
                let _ = db.notify_key(&key).await;
            });
        }

        Ok(length)
    }

    /// LPOP key [count] - remove and return elements from head
    pub fn lpop(&self, key: &str, count: Option<usize>) -> Result<Vec<Vec<u8>>> {
        let conn = self.core.conn.lock().unwrap_or_else(|e| e.into_inner());

        let key_id = match self.get_list_key_id(&conn, key)? {
            Some(id) => id,
            None => return Ok(vec![]),
        };

        let count = count.unwrap_or(1);

        // Get elements from head (lowest positions)
        let mut stmt = conn.prepare(
            "SELECT pos, value FROM lists WHERE key_id = ?1 ORDER BY pos ASC LIMIT ?2",
        )?;
        let rows = stmt.query_map(params![key_id, count as i64], |row| {
            Ok((row.get::<_, i64>(0)?, row.get::<_, Vec<u8>>(1)?))
        })?;

        let mut results = Vec::new();
        let mut positions = Vec::new();
        for row in rows {
            let (pos, value) = row?;
            positions.push(pos);
            results.push(value);
        }

        // Delete popped elements
        for pos in &positions {
            conn.execute(
                "DELETE FROM lists WHERE key_id = ?1 AND pos = ?2",
                params![key_id, pos],
            )?;
        }

        // Clean up empty list
        let remaining: i64 = conn.query_row(
            "SELECT COUNT(*) FROM lists WHERE key_id = ?1",
            params![key_id],
            |row| row.get(0),
        )?;

        if remaining == 0 {
            conn.execute("DELETE FROM keys WHERE id = ?1", params![key_id])?;
        } else {
            // Update timestamp
            let now = Self::now_ms();
            conn.execute(
                "UPDATE keys SET updated_at = ?1 WHERE id = ?2",
                params![now, key_id],
            )?;
        }

        Ok(results)
    }

    /// RPOP key [count] - remove and return elements from tail
    pub fn rpop(&self, key: &str, count: Option<usize>) -> Result<Vec<Vec<u8>>> {
        let conn = self.core.conn.lock().unwrap_or_else(|e| e.into_inner());

        let key_id = match self.get_list_key_id(&conn, key)? {
            Some(id) => id,
            None => return Ok(vec![]),
        };

        let count = count.unwrap_or(1);

        // Get elements from tail (highest positions)
        let mut stmt = conn.prepare(
            "SELECT pos, value FROM lists WHERE key_id = ?1 ORDER BY pos DESC LIMIT ?2",
        )?;
        let rows = stmt.query_map(params![key_id, count as i64], |row| {
            Ok((row.get::<_, i64>(0)?, row.get::<_, Vec<u8>>(1)?))
        })?;

        let mut results = Vec::new();
        let mut positions = Vec::new();
        for row in rows {
            let (pos, value) = row?;
            positions.push(pos);
            results.push(value);
        }

        // Delete popped elements
        for pos in &positions {
            conn.execute(
                "DELETE FROM lists WHERE key_id = ?1 AND pos = ?2",
                params![key_id, pos],
            )?;
        }

        // Clean up empty list
        let remaining: i64 = conn.query_row(
            "SELECT COUNT(*) FROM lists WHERE key_id = ?1",
            params![key_id],
            |row| row.get(0),
        )?;

        if remaining == 0 {
            conn.execute("DELETE FROM keys WHERE id = ?1", params![key_id])?;
        } else {
            // Update timestamp
            let now = Self::now_ms();
            conn.execute(
                "UPDATE keys SET updated_at = ?1 WHERE id = ?2",
                params![now, key_id],
            )?;
        }

        Ok(results)
    }

    /// LLEN key - get list length
    pub fn llen(&self, key: &str) -> Result<i64> {
        let conn = self.core.conn.lock().unwrap_or_else(|e| e.into_inner());

        let key_id = match self.get_list_key_id(&conn, key)? {
            Some(id) => id,
            None => return Ok(0),
        };

        let count: i64 = conn.query_row(
            "SELECT COUNT(*) FROM lists WHERE key_id = ?1",
            params![key_id],
            |row| row.get(0),
        )?;

        Ok(count)
    }

    /// LRANGE key start stop - get range of elements
    pub fn lrange(&self, key: &str, start: i64, stop: i64) -> Result<Vec<Vec<u8>>> {
        self.maybe_autovacuum();
        let conn = self.core.conn.lock().unwrap_or_else(|e| e.into_inner());

        let key_id = match self.get_list_key_id(&conn, key)? {
            Some(id) => id,
            None => return Ok(vec![]),
        };

        // Get list length
        let len: i64 = conn.query_row(
            "SELECT COUNT(*) FROM lists WHERE key_id = ?1",
            params![key_id],
            |row| row.get(0),
        )?;

        if len == 0 {
            return Ok(vec![]);
        }

        // Convert negative indices to positive
        let start = if start < 0 {
            (len + start).max(0)
        } else {
            start.min(len - 1)
        };

        let stop = if stop < 0 {
            (len + stop).max(0)
        } else {
            stop.min(len - 1)
        };

        if start > stop {
            return Ok(vec![]);
        }

        let count = stop - start + 1;

        // Get elements by logical index (ordered by position)
        let mut stmt = conn.prepare(
            "SELECT value FROM lists WHERE key_id = ?1 ORDER BY pos ASC LIMIT ?2 OFFSET ?3",
        )?;
        let rows = stmt.query_map(params![key_id, count, start], |row| row.get::<_, Vec<u8>>(0))?;

        let mut results = Vec::new();
        for row in rows {
            results.push(row?);
        }

        Ok(results)
    }

    /// LINDEX key index - get element by index
    pub fn lindex(&self, key: &str, index: i64) -> Result<Option<Vec<u8>>> {
        let conn = self.core.conn.lock().unwrap_or_else(|e| e.into_inner());

        let key_id = match self.get_list_key_id(&conn, key)? {
            Some(id) => id,
            None => return Ok(None),
        };

        // Get list length
        let len: i64 = conn.query_row(
            "SELECT COUNT(*) FROM lists WHERE key_id = ?1",
            params![key_id],
            |row| row.get(0),
        )?;

        if len == 0 {
            return Ok(None);
        }

        // Convert negative index
        let index = if index < 0 { len + index } else { index };

        if index < 0 || index >= len {
            return Ok(None);
        }

        // Get element at logical index
        let result: std::result::Result<Vec<u8>, _> = conn.query_row(
            "SELECT value FROM lists WHERE key_id = ?1 ORDER BY pos ASC LIMIT 1 OFFSET ?2",
            params![key_id, index],
            |row| row.get(0),
        );

        match result {
            Ok(value) => Ok(Some(value)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    /// LSET key index element - set element at index
    pub fn lset(&self, key: &str, index: i64, value: &[u8]) -> Result<()> {
        let conn = self.core.conn.lock().unwrap_or_else(|e| e.into_inner());

        let key_id = match self.get_list_key_id(&conn, key)? {
            Some(id) => id,
            None => return Err(KvError::NoSuchKey),
        };

        // Get list length
        let len: i64 = conn.query_row(
            "SELECT COUNT(*) FROM lists WHERE key_id = ?1",
            params![key_id],
            |row| row.get(0),
        )?;

        if len == 0 {
            return Err(KvError::OutOfRange);
        }

        // Convert negative index
        let index = if index < 0 { len + index } else { index };

        if index < 0 || index >= len {
            return Err(KvError::OutOfRange);
        }

        // Get position at logical index
        let pos: i64 = conn.query_row(
            "SELECT pos FROM lists WHERE key_id = ?1 ORDER BY pos ASC LIMIT 1 OFFSET ?2",
            params![key_id, index],
            |row| row.get(0),
        )?;

        // Update value
        conn.execute(
            "UPDATE lists SET value = ?1 WHERE key_id = ?2 AND pos = ?3",
            params![value, key_id, pos],
        )?;

        // Update key timestamp
        let now = Self::now_ms();
        conn.execute(
            "UPDATE keys SET updated_at = ?1 WHERE id = ?2",
            params![now, key_id],
        )?;

        Ok(())
    }

    /// LTRIM key start stop - trim list to specified range
    pub fn ltrim(&self, key: &str, start: i64, stop: i64) -> Result<()> {
        let conn = self.core.conn.lock().unwrap_or_else(|e| e.into_inner());

        let key_id = match self.get_list_key_id(&conn, key)? {
            Some(id) => id,
            None => return Ok(()), // Non-existent key is OK for LTRIM
        };

        // Get list length
        let len: i64 = conn.query_row(
            "SELECT COUNT(*) FROM lists WHERE key_id = ?1",
            params![key_id],
            |row| row.get(0),
        )?;

        if len == 0 {
            return Ok(());
        }

        // Convert negative indices
        let start = if start < 0 {
            (len + start).max(0)
        } else {
            start
        };

        let stop = if stop < 0 {
            len + stop
        } else {
            stop.min(len - 1)
        };

        // If range is invalid or empty, delete all elements
        if start > stop || start >= len {
            conn.execute("DELETE FROM lists WHERE key_id = ?1", params![key_id])?;
            conn.execute("DELETE FROM keys WHERE id = ?1", params![key_id])?;
            return Ok(());
        }

        // Get positions to keep
        let mut stmt = conn.prepare(
            "SELECT pos FROM lists WHERE key_id = ?1 ORDER BY pos ASC",
        )?;
        let rows = stmt.query_map(params![key_id], |row| row.get::<_, i64>(0))?;

        let positions: Vec<i64> = rows.filter_map(|r| r.ok()).collect();

        // Collect positions to delete (outside the keep range)
        let mut to_delete = Vec::new();
        for (i, &pos) in positions.iter().enumerate() {
            let idx = i as i64;
            if idx < start || idx > stop {
                to_delete.push(pos);
            }
        }

        // Delete positions outside range
        for pos in &to_delete {
            conn.execute(
                "DELETE FROM lists WHERE key_id = ?1 AND pos = ?2",
                params![key_id, pos],
            )?;
        }

        // Check if list is now empty
        let remaining: i64 = conn.query_row(
            "SELECT COUNT(*) FROM lists WHERE key_id = ?1",
            params![key_id],
            |row| row.get(0),
        )?;

        if remaining == 0 {
            conn.execute("DELETE FROM keys WHERE id = ?1", params![key_id])?;
        } else {
            // Update timestamp
            let now = Self::now_ms();
            conn.execute(
                "UPDATE keys SET updated_at = ?1 WHERE id = ?2",
                params![now, key_id],
            )?;
        }

        Ok(())
    }

    // --- Session 8: Set operations ---

    /// Helper to create a new set key
    fn create_set_key(&self, conn: &Connection, key: &str) -> Result<i64> {
        let db = self.selected_db;
        let now = Self::now_ms();

        conn.execute(
            "INSERT INTO keys (db, key, type, created_at, updated_at)
             VALUES (?1, ?2, ?3, ?4, ?4)",
            params![db, key, KeyType::Set as i32, now],
        )?;

        let key_id: i64 = conn.query_row(
            "SELECT id FROM keys WHERE db = ?1 AND key = ?2",
            params![db, key],
            |row| row.get(0),
        )?;

        Ok(key_id)
    }

    /// Get or create a set key, returns key_id
    fn get_or_create_set_key(&self, conn: &Connection, key: &str) -> Result<i64> {
        let db = self.selected_db;
        let now = Self::now_ms();

        let existing: std::result::Result<(i64, i32, Option<i64>), _> = conn.query_row(
            "SELECT id, type, expire_at FROM keys WHERE db = ?1 AND key = ?2",
            params![db, key],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        );

        match existing {
            Ok((key_id, key_type, expire_at)) => {
                // Check expiration
                if let Some(exp) = expire_at {
                    if exp <= now {
                        // Expired - delete and create new
                        conn.execute("DELETE FROM keys WHERE id = ?1", params![key_id])?;
                        return self.create_set_key(conn, key);
                    }
                }
                // Check type
                if key_type != KeyType::Set as i32 {
                    return Err(KvError::WrongType);
                }
                Ok(key_id)
            }
            Err(rusqlite::Error::QueryReturnedNoRows) => self.create_set_key(conn, key),
            Err(e) => Err(e.into()),
        }
    }

    /// Get set key_id if it exists and is valid
    fn get_set_key_id(&self, conn: &Connection, key: &str) -> Result<Option<i64>> {
        let db = self.selected_db;
        let now = Self::now_ms();

        let result: std::result::Result<(i64, i32, Option<i64>), _> = conn.query_row(
            "SELECT id, type, expire_at FROM keys WHERE db = ?1 AND key = ?2",
            params![db, key],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        );

        match result {
            Ok((key_id, key_type, expire_at)) => {
                // Check expiration
                if let Some(exp) = expire_at {
                    if exp <= now {
                        return Ok(None);
                    }
                }
                // Check type
                if key_type != KeyType::Set as i32 {
                    return Err(KvError::WrongType);
                }
                Ok(Some(key_id))
            }
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    /// SADD key member [member ...] - add members to set, returns count of new members added
    pub fn sadd(&self, key: &str, members: &[&[u8]]) -> Result<i64> {
        if members.is_empty() {
            return Ok(0);
        }

        let conn = self.core.conn.lock().unwrap_or_else(|e| e.into_inner());
        let key_id = self.get_or_create_set_key(&conn, key)?;

        let mut added = 0i64;
        for member in members {
            let result = conn.execute(
                "INSERT OR IGNORE INTO sets (key_id, member) VALUES (?1, ?2)",
                params![key_id, member],
            )?;
            added += result as i64;
        }

        // Update timestamp
        let now = Self::now_ms();
        conn.execute(
            "UPDATE keys SET updated_at = ?1 WHERE id = ?2",
            params![now, key_id],
        )?;

        Ok(added)
    }

    /// SREM key member [member ...] - remove members from set, returns count removed
    pub fn srem(&self, key: &str, members: &[&[u8]]) -> Result<i64> {
        if members.is_empty() {
            return Ok(0);
        }

        let conn = self.core.conn.lock().unwrap_or_else(|e| e.into_inner());

        let key_id = match self.get_set_key_id(&conn, key)? {
            Some(id) => id,
            None => return Ok(0),
        };

        let mut removed = 0i64;
        for member in members {
            let result = conn.execute(
                "DELETE FROM sets WHERE key_id = ?1 AND member = ?2",
                params![key_id, member],
            )?;
            removed += result as i64;
        }

        // Clean up empty set
        let remaining: i64 = conn.query_row(
            "SELECT COUNT(*) FROM sets WHERE key_id = ?1",
            params![key_id],
            |row| row.get(0),
        )?;

        if remaining == 0 {
            conn.execute("DELETE FROM keys WHERE id = ?1", params![key_id])?;
        } else {
            // Update timestamp
            let now = Self::now_ms();
            conn.execute(
                "UPDATE keys SET updated_at = ?1 WHERE id = ?2",
                params![now, key_id],
            )?;
        }

        Ok(removed)
    }

    /// SMEMBERS key - get all members of set
    pub fn smembers(&self, key: &str) -> Result<Vec<Vec<u8>>> {
        self.maybe_autovacuum();
        let conn = self.core.conn.lock().unwrap_or_else(|e| e.into_inner());

        let key_id = match self.get_set_key_id(&conn, key)? {
            Some(id) => id,
            None => return Ok(vec![]),
        };

        let mut stmt = conn.prepare(
            "SELECT member FROM sets WHERE key_id = ?1",
        )?;

        let rows = stmt.query_map(params![key_id], |row| row.get::<_, Vec<u8>>(0))?;

        let mut members = Vec::new();
        for row in rows {
            members.push(row?);
        }

        Ok(members)
    }

    /// SISMEMBER key member - check if member exists in set (returns true/false)
    pub fn sismember(&self, key: &str, member: &[u8]) -> Result<bool> {
        let conn = self.core.conn.lock().unwrap_or_else(|e| e.into_inner());

        let key_id = match self.get_set_key_id(&conn, key)? {
            Some(id) => id,
            None => return Ok(false),
        };

        let exists: bool = conn
            .query_row(
                "SELECT 1 FROM sets WHERE key_id = ?1 AND member = ?2",
                params![key_id, member],
                |_| Ok(true),
            )
            .unwrap_or(false);

        Ok(exists)
    }

    /// SCARD key - get cardinality (number of members) of set
    pub fn scard(&self, key: &str) -> Result<i64> {
        let conn = self.core.conn.lock().unwrap_or_else(|e| e.into_inner());

        let key_id = match self.get_set_key_id(&conn, key)? {
            Some(id) => id,
            None => return Ok(0),
        };

        let count: i64 = conn.query_row(
            "SELECT COUNT(*) FROM sets WHERE key_id = ?1",
            params![key_id],
            |row| row.get(0),
        )?;

        Ok(count)
    }

    /// SPOP key [count] - remove and return random member(s)
    pub fn spop(&self, key: &str, count: Option<usize>) -> Result<Vec<Vec<u8>>> {
        let conn = self.core.conn.lock().unwrap_or_else(|e| e.into_inner());

        let key_id = match self.get_set_key_id(&conn, key)? {
            Some(id) => id,
            None => return Ok(vec![]),
        };

        let count = count.unwrap_or(1);

        // Get random members using SQLite's RANDOM()
        let mut stmt = conn.prepare(
            "SELECT rowid, member FROM sets WHERE key_id = ?1 ORDER BY RANDOM() LIMIT ?2",
        )?;
        let rows = stmt.query_map(params![key_id, count as i64], |row| {
            Ok((row.get::<_, i64>(0)?, row.get::<_, Vec<u8>>(1)?))
        })?;

        let mut results = Vec::new();
        let mut rowids = Vec::new();
        for row in rows {
            let (rowid, member) = row?;
            rowids.push(rowid);
            results.push(member);
        }

        // Delete popped members
        for rowid in &rowids {
            conn.execute("DELETE FROM sets WHERE rowid = ?1", params![rowid])?;
        }

        // Clean up empty set
        let remaining: i64 = conn.query_row(
            "SELECT COUNT(*) FROM sets WHERE key_id = ?1",
            params![key_id],
            |row| row.get(0),
        )?;

        if remaining == 0 {
            conn.execute("DELETE FROM keys WHERE id = ?1", params![key_id])?;
        } else if !results.is_empty() {
            // Update timestamp
            let now = Self::now_ms();
            conn.execute(
                "UPDATE keys SET updated_at = ?1 WHERE id = ?2",
                params![now, key_id],
            )?;
        }

        Ok(results)
    }

    /// SRANDMEMBER key [count] - get random member(s) without removing
    pub fn srandmember(&self, key: &str, count: Option<i64>) -> Result<Vec<Vec<u8>>> {
        let conn = self.core.conn.lock().unwrap_or_else(|e| e.into_inner());

        let key_id = match self.get_set_key_id(&conn, key)? {
            Some(id) => id,
            None => return Ok(vec![]),
        };

        // Handle count semantics:
        // - No count: return single element
        // - Positive count: return up to count distinct elements
        // - Negative count: return |count| elements (may repeat)
        let count = count.unwrap_or(1);
        let allow_repeats = count < 0;
        let limit = count.abs();

        if allow_repeats {
            // With negative count, we may need to return duplicates
            // Get all members and sample with replacement
            let mut stmt = conn.prepare(
                "SELECT member FROM sets WHERE key_id = ?1",
            )?;
            let rows = stmt.query_map(params![key_id], |row| row.get::<_, Vec<u8>>(0))?;

            let all_members: Vec<Vec<u8>> = rows.filter_map(|r| r.ok()).collect();

            if all_members.is_empty() {
                return Ok(vec![]);
            }

            // Sample with replacement using random indices
            let mut results = Vec::new();
            for _ in 0..limit {
                // Use a simple random selection by querying with RANDOM() each time
                let member: Vec<u8> = conn.query_row(
                    "SELECT member FROM sets WHERE key_id = ?1 ORDER BY RANDOM() LIMIT 1",
                    params![key_id],
                    |row| row.get(0),
                )?;
                results.push(member);
            }
            Ok(results)
        } else {
            // Positive count: distinct elements
            let mut stmt = conn.prepare(
                "SELECT member FROM sets WHERE key_id = ?1 ORDER BY RANDOM() LIMIT ?2",
            )?;
            let rows = stmt.query_map(params![key_id, limit], |row| row.get::<_, Vec<u8>>(0))?;

            let mut results = Vec::new();
            for row in rows {
                results.push(row?);
            }
            Ok(results)
        }
    }

    /// SDIFF key [key ...] - return members in first set but not in subsequent sets
    pub fn sdiff(&self, keys: &[&str]) -> Result<Vec<Vec<u8>>> {
        if keys.is_empty() {
            return Ok(vec![]);
        }

        // Get members of first set
        let mut result = self.smembers(keys[0])?;

        // Remove members that exist in subsequent sets
        for key in &keys[1..] {
            let other_members = self.smembers(key)?;
            result.retain(|m| !other_members.contains(m));
        }

        Ok(result)
    }

    /// SINTER key [key ...] - return intersection of all sets
    pub fn sinter(&self, keys: &[&str]) -> Result<Vec<Vec<u8>>> {
        if keys.is_empty() {
            return Ok(vec![]);
        }

        // Get members of first set
        let mut result = self.smembers(keys[0])?;

        // Keep only members that exist in all subsequent sets
        for key in &keys[1..] {
            let other_members = self.smembers(key)?;
            result.retain(|m| other_members.contains(m));
        }

        Ok(result)
    }

    /// SUNION key [key ...] - return union of all sets
    pub fn sunion(&self, keys: &[&str]) -> Result<Vec<Vec<u8>>> {
        let mut result: Vec<Vec<u8>> = Vec::new();

        for key in keys {
            let members = self.smembers(key)?;
            for member in members {
                if !result.contains(&member) {
                    result.push(member);
                }
            }
        }

        Ok(result)
    }

    // --- Session 9: Sorted Set operations ---

    /// Helper to create a new sorted set key
    fn create_zset_key(&self, conn: &Connection, key: &str) -> Result<i64> {
        let db = self.selected_db;
        let now = Self::now_ms();

        conn.execute(
            "INSERT INTO keys (db, key, type, created_at, updated_at)
             VALUES (?1, ?2, ?3, ?4, ?4)",
            params![db, key, KeyType::ZSet as i32, now],
        )?;

        let key_id: i64 = conn.query_row(
            "SELECT id FROM keys WHERE db = ?1 AND key = ?2",
            params![db, key],
            |row| row.get(0),
        )?;

        Ok(key_id)
    }

    /// Get or create a sorted set key, returns key_id
    fn get_or_create_zset_key(&self, conn: &Connection, key: &str) -> Result<i64> {
        let db = self.selected_db;
        let now = Self::now_ms();

        let existing: std::result::Result<(i64, i32, Option<i64>), _> = conn.query_row(
            "SELECT id, type, expire_at FROM keys WHERE db = ?1 AND key = ?2",
            params![db, key],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        );

        match existing {
            Ok((key_id, key_type, expire_at)) => {
                // Check expiration
                if let Some(exp) = expire_at {
                    if exp <= now {
                        // Expired - delete and create new
                        conn.execute("DELETE FROM keys WHERE id = ?1", params![key_id])?;
                        return self.create_zset_key(conn, key);
                    }
                }
                // Check type
                if key_type != KeyType::ZSet as i32 {
                    return Err(KvError::WrongType);
                }
                Ok(key_id)
            }
            Err(rusqlite::Error::QueryReturnedNoRows) => self.create_zset_key(conn, key),
            Err(e) => Err(e.into()),
        }
    }

    /// Get sorted set key_id if it exists and is valid
    fn get_zset_key_id(&self, conn: &Connection, key: &str) -> Result<Option<i64>> {
        let db = self.selected_db;
        let now = Self::now_ms();

        let result: std::result::Result<(i64, i32, Option<i64>), _> = conn.query_row(
            "SELECT id, type, expire_at FROM keys WHERE db = ?1 AND key = ?2",
            params![db, key],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        );

        match result {
            Ok((key_id, key_type, expire_at)) => {
                // Check expiration
                if let Some(exp) = expire_at {
                    if exp <= now {
                        return Ok(None);
                    }
                }
                // Check type
                if key_type != KeyType::ZSet as i32 {
                    return Err(KvError::WrongType);
                }
                Ok(Some(key_id))
            }
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    /// ZADD key score member [score member ...] - add members with scores, returns count added
    pub fn zadd(&self, key: &str, members: &[ZMember]) -> Result<i64> {
        if members.is_empty() {
            return Ok(0);
        }

        let conn = self.core.conn.lock().unwrap_or_else(|e| e.into_inner());
        let key_id = self.get_or_create_zset_key(&conn, key)?;

        let mut added = 0i64;
        for m in members {
            // Check if member already exists
            let exists: bool = conn
                .query_row(
                    "SELECT 1 FROM zsets WHERE key_id = ?1 AND member = ?2",
                    params![key_id, &m.member],
                    |_| Ok(true),
                )
                .unwrap_or(false);

            if exists {
                // Update score
                conn.execute(
                    "UPDATE zsets SET score = ?1 WHERE key_id = ?2 AND member = ?3",
                    params![m.score, key_id, &m.member],
                )?;
            } else {
                // Insert new member
                conn.execute(
                    "INSERT INTO zsets (key_id, member, score) VALUES (?1, ?2, ?3)",
                    params![key_id, &m.member, m.score],
                )?;
                added += 1;
            }
        }

        // Update timestamp
        let now = Self::now_ms();
        conn.execute(
            "UPDATE keys SET updated_at = ?1 WHERE id = ?2",
            params![now, key_id],
        )?;

        Ok(added)
    }

    /// ZREM key member [member ...] - remove members, returns count removed
    pub fn zrem(&self, key: &str, members: &[&[u8]]) -> Result<i64> {
        if members.is_empty() {
            return Ok(0);
        }

        let conn = self.core.conn.lock().unwrap_or_else(|e| e.into_inner());

        let key_id = match self.get_zset_key_id(&conn, key)? {
            Some(id) => id,
            None => return Ok(0),
        };

        let mut removed = 0i64;
        for member in members {
            let result = conn.execute(
                "DELETE FROM zsets WHERE key_id = ?1 AND member = ?2",
                params![key_id, member],
            )?;
            removed += result as i64;
        }

        // Clean up empty set
        let remaining: i64 = conn.query_row(
            "SELECT COUNT(*) FROM zsets WHERE key_id = ?1",
            params![key_id],
            |row| row.get(0),
        )?;

        if remaining == 0 {
            conn.execute("DELETE FROM keys WHERE id = ?1", params![key_id])?;
        } else {
            // Update timestamp
            let now = Self::now_ms();
            conn.execute(
                "UPDATE keys SET updated_at = ?1 WHERE id = ?2",
                params![now, key_id],
            )?;
        }

        Ok(removed)
    }

    /// ZSCORE key member - get score of member
    pub fn zscore(&self, key: &str, member: &[u8]) -> Result<Option<f64>> {
        let conn = self.core.conn.lock().unwrap_or_else(|e| e.into_inner());

        let key_id = match self.get_zset_key_id(&conn, key)? {
            Some(id) => id,
            None => return Ok(None),
        };

        let score: std::result::Result<f64, _> = conn.query_row(
            "SELECT score FROM zsets WHERE key_id = ?1 AND member = ?2",
            params![key_id, member],
            |row| row.get(0),
        );

        match score {
            Ok(s) => Ok(Some(s)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    /// ZRANK key member - get rank (0-based, ascending by score)
    pub fn zrank(&self, key: &str, member: &[u8]) -> Result<Option<i64>> {
        let conn = self.core.conn.lock().unwrap_or_else(|e| e.into_inner());

        let key_id = match self.get_zset_key_id(&conn, key)? {
            Some(id) => id,
            None => return Ok(None),
        };

        // First check if member exists and get its score
        let member_score: std::result::Result<f64, _> = conn.query_row(
            "SELECT score FROM zsets WHERE key_id = ?1 AND member = ?2",
            params![key_id, member],
            |row| row.get(0),
        );

        let score = match member_score {
            Ok(s) => s,
            Err(rusqlite::Error::QueryReturnedNoRows) => return Ok(None),
            Err(e) => return Err(e.into()),
        };

        // Count members with lower score, or same score but lexicographically smaller member
        let rank: i64 = conn.query_row(
            "SELECT COUNT(*) FROM zsets WHERE key_id = ?1 AND (score < ?2 OR (score = ?2 AND member < ?3))",
            params![key_id, score, member],
            |row| row.get(0),
        )?;

        Ok(Some(rank))
    }

    /// ZREVRANK key member - get rank (descending by score)
    pub fn zrevrank(&self, key: &str, member: &[u8]) -> Result<Option<i64>> {
        let conn = self.core.conn.lock().unwrap_or_else(|e| e.into_inner());

        let key_id = match self.get_zset_key_id(&conn, key)? {
            Some(id) => id,
            None => return Ok(None),
        };

        // First check if member exists and get its score
        let member_score: std::result::Result<f64, _> = conn.query_row(
            "SELECT score FROM zsets WHERE key_id = ?1 AND member = ?2",
            params![key_id, member],
            |row| row.get(0),
        );

        let score = match member_score {
            Ok(s) => s,
            Err(rusqlite::Error::QueryReturnedNoRows) => return Ok(None),
            Err(e) => return Err(e.into()),
        };

        // Count members with higher score, or same score but lexicographically larger member
        let rank: i64 = conn.query_row(
            "SELECT COUNT(*) FROM zsets WHERE key_id = ?1 AND (score > ?2 OR (score = ?2 AND member > ?3))",
            params![key_id, score, member],
            |row| row.get(0),
        )?;

        Ok(Some(rank))
    }

    /// ZCARD key - get cardinality
    pub fn zcard(&self, key: &str) -> Result<i64> {
        let conn = self.core.conn.lock().unwrap_or_else(|e| e.into_inner());

        let key_id = match self.get_zset_key_id(&conn, key)? {
            Some(id) => id,
            None => return Ok(0),
        };

        let count: i64 = conn.query_row(
            "SELECT COUNT(*) FROM zsets WHERE key_id = ?1",
            params![key_id],
            |row| row.get(0),
        )?;

        Ok(count)
    }

    /// ZRANGE key start stop [WITHSCORES] - get members by rank range (ascending)
    pub fn zrange(&self, key: &str, start: i64, stop: i64, with_scores: bool) -> Result<Vec<ZMember>> {
        self.maybe_autovacuum();
        let conn = self.core.conn.lock().unwrap_or_else(|e| e.into_inner());

        let key_id = match self.get_zset_key_id(&conn, key)? {
            Some(id) => id,
            None => return Ok(vec![]),
        };

        // Get total count for negative index handling
        let total: i64 = conn.query_row(
            "SELECT COUNT(*) FROM zsets WHERE key_id = ?1",
            params![key_id],
            |row| row.get(0),
        )?;

        if total == 0 {
            return Ok(vec![]);
        }

        // Convert negative indices to positive
        let start = if start < 0 {
            (total + start).max(0)
        } else {
            start.min(total)
        };

        let stop = if stop < 0 {
            (total + stop).max(0)
        } else {
            stop.min(total - 1)
        };

        if start > stop || start >= total {
            return Ok(vec![]);
        }

        let limit = stop - start + 1;

        let mut stmt = conn.prepare(
            "SELECT member, score FROM zsets WHERE key_id = ?1 ORDER BY score ASC, member ASC LIMIT ?2 OFFSET ?3",
        )?;

        let rows = stmt.query_map(params![key_id, limit, start], |row| {
            Ok(ZMember {
                member: row.get(0)?,
                score: row.get(1)?,
            })
        })?;

        let mut results = Vec::new();
        for row in rows {
            let m = row?;
            if with_scores {
                results.push(m);
            } else {
                results.push(ZMember {
                    member: m.member,
                    score: 0.0, // Score not needed but struct requires it
                });
            }
        }

        Ok(results)
    }

    /// ZREVRANGE key start stop [WITHSCORES] - get members by rank range (descending)
    pub fn zrevrange(&self, key: &str, start: i64, stop: i64, with_scores: bool) -> Result<Vec<ZMember>> {
        let conn = self.core.conn.lock().unwrap_or_else(|e| e.into_inner());

        let key_id = match self.get_zset_key_id(&conn, key)? {
            Some(id) => id,
            None => return Ok(vec![]),
        };

        // Get total count for negative index handling
        let total: i64 = conn.query_row(
            "SELECT COUNT(*) FROM zsets WHERE key_id = ?1",
            params![key_id],
            |row| row.get(0),
        )?;

        if total == 0 {
            return Ok(vec![]);
        }

        // Convert negative indices to positive
        let start = if start < 0 {
            (total + start).max(0)
        } else {
            start.min(total)
        };

        let stop = if stop < 0 {
            (total + stop).max(0)
        } else {
            stop.min(total - 1)
        };

        if start > stop || start >= total {
            return Ok(vec![]);
        }

        let limit = stop - start + 1;

        let mut stmt = conn.prepare(
            "SELECT member, score FROM zsets WHERE key_id = ?1 ORDER BY score DESC, member DESC LIMIT ?2 OFFSET ?3",
        )?;

        let rows = stmt.query_map(params![key_id, limit, start], |row| {
            Ok(ZMember {
                member: row.get(0)?,
                score: row.get(1)?,
            })
        })?;

        let mut results = Vec::new();
        for row in rows {
            let m = row?;
            if with_scores {
                results.push(m);
            } else {
                results.push(ZMember {
                    member: m.member,
                    score: 0.0,
                });
            }
        }

        Ok(results)
    }

    /// ZRANGEBYSCORE key min max [LIMIT offset count] - get members by score range
    pub fn zrangebyscore(
        &self,
        key: &str,
        min: f64,
        max: f64,
        offset: Option<i64>,
        count: Option<i64>,
    ) -> Result<Vec<ZMember>> {
        let conn = self.core.conn.lock().unwrap_or_else(|e| e.into_inner());

        let key_id = match self.get_zset_key_id(&conn, key)? {
            Some(id) => id,
            None => return Ok(vec![]),
        };

        let offset = offset.unwrap_or(0);
        let count = count.unwrap_or(i64::MAX);

        let mut stmt = conn.prepare(
            "SELECT member, score FROM zsets WHERE key_id = ?1 AND score >= ?2 AND score <= ?3
             ORDER BY score ASC, member ASC LIMIT ?4 OFFSET ?5",
        )?;

        let rows = stmt.query_map(params![key_id, min, max, count, offset], |row| {
            Ok(ZMember {
                member: row.get(0)?,
                score: row.get(1)?,
            })
        })?;

        let mut results = Vec::new();
        for row in rows {
            results.push(row?);
        }

        Ok(results)
    }

    /// ZCOUNT key min max - count members in score range
    pub fn zcount(&self, key: &str, min: f64, max: f64) -> Result<i64> {
        let conn = self.core.conn.lock().unwrap_or_else(|e| e.into_inner());

        let key_id = match self.get_zset_key_id(&conn, key)? {
            Some(id) => id,
            None => return Ok(0),
        };

        let count: i64 = conn.query_row(
            "SELECT COUNT(*) FROM zsets WHERE key_id = ?1 AND score >= ?2 AND score <= ?3",
            params![key_id, min, max],
            |row| row.get(0),
        )?;

        Ok(count)
    }

    /// ZINCRBY key increment member - increment score of member
    pub fn zincrby(&self, key: &str, increment: f64, member: &[u8]) -> Result<f64> {
        let conn = self.core.conn.lock().unwrap_or_else(|e| e.into_inner());
        let key_id = self.get_or_create_zset_key(&conn, key)?;

        // Check if member exists
        let existing_score: std::result::Result<f64, _> = conn.query_row(
            "SELECT score FROM zsets WHERE key_id = ?1 AND member = ?2",
            params![key_id, member],
            |row| row.get(0),
        );

        let new_score = match existing_score {
            Ok(score) => {
                let new_score = score + increment;
                conn.execute(
                    "UPDATE zsets SET score = ?1 WHERE key_id = ?2 AND member = ?3",
                    params![new_score, key_id, member],
                )?;
                new_score
            }
            Err(rusqlite::Error::QueryReturnedNoRows) => {
                conn.execute(
                    "INSERT INTO zsets (key_id, member, score) VALUES (?1, ?2, ?3)",
                    params![key_id, member, increment],
                )?;
                increment
            }
            Err(e) => return Err(e.into()),
        };

        // Update timestamp
        let now = Self::now_ms();
        conn.execute(
            "UPDATE keys SET updated_at = ?1 WHERE id = ?2",
            params![now, key_id],
        )?;

        Ok(new_score)
    }

    /// ZREMRANGEBYRANK key start stop - remove members by rank range
    pub fn zremrangebyrank(&self, key: &str, start: i64, stop: i64) -> Result<i64> {
        let conn = self.core.conn.lock().unwrap_or_else(|e| e.into_inner());

        let key_id = match self.get_zset_key_id(&conn, key)? {
            Some(id) => id,
            None => return Ok(0),
        };

        // Get total count for negative index handling
        let total: i64 = conn.query_row(
            "SELECT COUNT(*) FROM zsets WHERE key_id = ?1",
            params![key_id],
            |row| row.get(0),
        )?;

        if total == 0 {
            return Ok(0);
        }

        // Convert negative indices to positive
        let start = if start < 0 {
            (total + start).max(0)
        } else {
            start.min(total)
        };

        let stop = if stop < 0 {
            (total + stop).max(0)
        } else {
            stop.min(total - 1)
        };

        if start > stop || start >= total {
            return Ok(0);
        }

        let limit = stop - start + 1;

        // Get rowids to delete
        let mut stmt = conn.prepare(
            "SELECT rowid FROM zsets WHERE key_id = ?1 ORDER BY score ASC, member ASC LIMIT ?2 OFFSET ?3",
        )?;

        let rowids: Vec<i64> = stmt
            .query_map(params![key_id, limit, start], |row| row.get(0))?
            .filter_map(|r| r.ok())
            .collect();

        let removed = rowids.len() as i64;

        // Delete by rowids
        for rowid in &rowids {
            conn.execute("DELETE FROM zsets WHERE rowid = ?1", params![rowid])?;
        }

        // Clean up empty set
        let remaining: i64 = conn.query_row(
            "SELECT COUNT(*) FROM zsets WHERE key_id = ?1",
            params![key_id],
            |row| row.get(0),
        )?;

        if remaining == 0 {
            conn.execute("DELETE FROM keys WHERE id = ?1", params![key_id])?;
        } else if removed > 0 {
            // Update timestamp
            let now = Self::now_ms();
            conn.execute(
                "UPDATE keys SET updated_at = ?1 WHERE id = ?2",
                params![now, key_id],
            )?;
        }

        Ok(removed)
    }

    /// ZREMRANGEBYSCORE key min max - remove members by score range
    pub fn zremrangebyscore(&self, key: &str, min: f64, max: f64) -> Result<i64> {
        let conn = self.core.conn.lock().unwrap_or_else(|e| e.into_inner());

        let key_id = match self.get_zset_key_id(&conn, key)? {
            Some(id) => id,
            None => return Ok(0),
        };

        let removed = conn.execute(
            "DELETE FROM zsets WHERE key_id = ?1 AND score >= ?2 AND score <= ?3",
            params![key_id, min, max],
        )? as i64;

        // Clean up empty set
        let remaining: i64 = conn.query_row(
            "SELECT COUNT(*) FROM zsets WHERE key_id = ?1",
            params![key_id],
            |row| row.get(0),
        )?;

        if remaining == 0 {
            conn.execute("DELETE FROM keys WHERE id = ?1", params![key_id])?;
        } else if removed > 0 {
            // Update timestamp
            let now = Self::now_ms();
            conn.execute(
                "UPDATE keys SET updated_at = ?1 WHERE id = ?2",
                params![now, key_id],
            )?;
        }

        Ok(removed)
    }

    // --- Session 10: Server Operations ---

    /// DBSIZE - Return the number of keys in the currently selected database
    pub fn dbsize(&self) -> Result<i64> {
        let conn = self.core.conn.lock().unwrap_or_else(|e| e.into_inner());
        let db = self.selected_db;
        let now = Self::now_ms();

        let count: i64 = conn.query_row(
            "SELECT COUNT(*) FROM keys WHERE db = ?1 AND (expire_at IS NULL OR expire_at > ?2)",
            params![db, now],
            |row| row.get(0),
        )?;

        Ok(count)
    }

    /// FLUSHDB - Delete all keys in the currently selected database
    pub fn flushdb(&self) -> Result<()> {
        let conn = self.core.conn.lock().unwrap_or_else(|e| e.into_inner());
        let db = self.selected_db;

        conn.execute("DELETE FROM keys WHERE db = ?1", params![db])?;

        Ok(())
    }

    // --- Session 11: Custom Commands ---

    /// VACUUM - Delete all expired keys across all databases and reclaim disk space.
    /// Returns the number of expired keys that were deleted.
    pub fn vacuum(&self) -> Result<i64> {
        let conn = self.core.conn.lock().unwrap_or_else(|e| e.into_inner());
        let now = Self::now_ms();

        // Delete all expired keys (across all databases)
        let deleted = conn.execute(
            "DELETE FROM keys WHERE expire_at IS NOT NULL AND expire_at <= ?1",
            params![now],
        )? as i64;

        // Run SQLite VACUUM to reclaim disk space
        conn.execute_batch("VACUUM")?;

        Ok(deleted)
    }

    /// KEYINFO key - Get metadata about a key.
    /// Returns None if the key doesn't exist.
    pub fn keyinfo(&self, key: &str) -> Result<Option<KeyInfo>> {
        let conn = self.core.conn.lock().unwrap_or_else(|e| e.into_inner());
        let db = self.selected_db;
        let now = Self::now_ms();

        let result: std::result::Result<(i32, Option<i64>, i64, i64), _> = conn.query_row(
            "SELECT type, expire_at, created_at, updated_at
             FROM keys
             WHERE db = ?1 AND key = ?2",
            params![db, key],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        );

        match result {
            Ok((type_int, expire_at, created_at, updated_at)) => {
                // Check if key is expired
                if let Some(exp) = expire_at {
                    if exp <= now {
                        // Key is expired - delete it and return None
                        drop(conn);
                        let _ = self.del(&[key]);
                        return Ok(None);
                    }
                }

                let key_type = KeyType::from_i32(type_int).ok_or(KvError::InvalidData)?;

                // Calculate TTL in seconds (-1 if no expiry)
                let ttl = match expire_at {
                    Some(exp) => ((exp - now) / 1000).max(0),
                    None => -1,
                };

                Ok(Some(KeyInfo {
                    key_type,
                    ttl,
                    created_at,
                    updated_at,
                }))
            }
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    // --- Session 13: Stream Operations ---

    /// Get or create a stream key, returning the key_id
    fn get_or_create_stream_key(&self, conn: &Connection, key: &str) -> Result<i64> {
        let db = self.selected_db;
        let now = Self::now_ms();

        // Check if key exists and is correct type
        let existing: std::result::Result<(i64, i32, Option<i64>), _> = conn.query_row(
            "SELECT id, type, expire_at FROM keys WHERE db = ?1 AND key = ?2",
            params![db, key],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        );

        match existing {
            Ok((key_id, key_type, expire_at)) => {
                // Check expiration
                if let Some(exp) = expire_at {
                    if exp <= now {
                        // Expired - delete and create new
                        conn.execute("DELETE FROM keys WHERE id = ?1", params![key_id])?;
                        return self.create_stream_key(conn, key);
                    }
                }
                // Check type
                if key_type != KeyType::Stream as i32 {
                    return Err(KvError::WrongType);
                }
                Ok(key_id)
            }
            Err(rusqlite::Error::QueryReturnedNoRows) => self.create_stream_key(conn, key),
            Err(e) => Err(e.into()),
        }
    }

    fn create_stream_key(&self, conn: &Connection, key: &str) -> Result<i64> {
        let db = self.selected_db;
        let now = Self::now_ms();

        conn.execute(
            "INSERT INTO keys (db, key, type, updated_at) VALUES (?1, ?2, ?3, ?4)",
            params![db, key, KeyType::Stream as i32, now],
        )?;

        Ok(conn.last_insert_rowid())
    }

    /// Get stream key_id if it exists and is the correct type
    fn get_stream_key_id(&self, conn: &Connection, key: &str) -> Result<Option<i64>> {
        let db = self.selected_db;
        let now = Self::now_ms();

        let result: std::result::Result<(i64, i32, Option<i64>), _> = conn.query_row(
            "SELECT id, type, expire_at FROM keys WHERE db = ?1 AND key = ?2",
            params![db, key],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        );

        match result {
            Ok((key_id, key_type, expire_at)) => {
                // Check expiration
                if let Some(exp) = expire_at {
                    if exp <= now {
                        return Ok(None);
                    }
                }
                // Check type
                if key_type != KeyType::Stream as i32 {
                    return Err(KvError::WrongType);
                }
                Ok(Some(key_id))
            }
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    /// Encode field-value pairs as MessagePack
    fn encode_stream_fields(fields: &[(&[u8], &[u8])]) -> Vec<u8> {
        // Convert to owned vecs for serialization
        let data: Vec<(Vec<u8>, Vec<u8>)> = fields
            .iter()
            .map(|(k, v)| (k.to_vec(), v.to_vec()))
            .collect();
        rmp_serde::to_vec(&data).unwrap_or_default()
    }

    /// Decode MessagePack to field-value pairs
    fn decode_stream_fields(data: &[u8]) -> Vec<(Vec<u8>, Vec<u8>)> {
        rmp_serde::from_slice(data).unwrap_or_default()
    }

    /// Get the last entry ID for a stream, or (0, 0) if empty
    fn get_last_stream_id(&self, conn: &Connection, key_id: i64) -> StreamId {
        let result: std::result::Result<(i64, i64), _> = conn.query_row(
            "SELECT entry_ms, entry_seq FROM streams WHERE key_id = ?1 ORDER BY entry_ms DESC, entry_seq DESC LIMIT 1",
            params![key_id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        );

        match result {
            Ok((ms, seq)) => StreamId::new(ms, seq),
            Err(_) => StreamId::new(0, 0),
        }
    }

    /// XADD key [NOMKSTREAM] [MAXLEN|MINID [=|~] threshold] *|id field value [field value ...]
    /// Returns the entry ID that was added
    pub fn xadd(
        &self,
        key: &str,
        id: Option<StreamId>,  // None means auto-generate with *
        fields: &[(&[u8], &[u8])],
        nomkstream: bool,
        maxlen: Option<i64>,
        minid: Option<StreamId>,
        approximate: bool,
    ) -> Result<Option<StreamId>> {
        if fields.is_empty() {
            return Err(KvError::SyntaxError);
        }

        let conn = self.core.conn.lock().unwrap_or_else(|e| e.into_inner());
        let now = Self::now_ms();

        // Check if stream exists
        let existing_key_id = self.get_stream_key_id(&conn, key)?;

        // Handle NOMKSTREAM - don't create if stream doesn't exist
        if nomkstream && existing_key_id.is_none() {
            return Ok(None);
        }

        let key_id = match existing_key_id {
            Some(id) => id,
            None => self.create_stream_key(&conn, key)?,
        };

        // Determine the entry ID
        let last_id = self.get_last_stream_id(&conn, key_id);
        let entry_id = match id {
            Some(explicit_id) => {
                // Explicit ID must be greater than last ID
                if explicit_id <= last_id && last_id != StreamId::new(0, 0) {
                    return Err(KvError::InvalidData); // ID is equal or smaller than last
                }
                if explicit_id.ms == 0 && explicit_id.seq == 0 {
                    return Err(KvError::InvalidData); // 0-0 is not allowed
                }
                explicit_id
            }
            None => {
                // Auto-generate: use current time, increment seq if same ms
                if now > last_id.ms {
                    StreamId::new(now, 0)
                } else {
                    StreamId::new(last_id.ms, last_id.seq + 1)
                }
            }
        };

        // Encode fields as MessagePack
        let data = Self::encode_stream_fields(fields);

        // Insert the entry
        conn.execute(
            "INSERT INTO streams (key_id, entry_ms, entry_seq, data, created_at) VALUES (?1, ?2, ?3, ?4, ?5)",
            params![key_id, entry_id.ms, entry_id.seq, data, now],
        )?;

        // Update key timestamp
        conn.execute(
            "UPDATE keys SET updated_at = ?1 WHERE id = ?2",
            params![now, key_id],
        )?;

        // Apply MAXLEN trimming
        if let Some(max) = maxlen {
            self.trim_stream_maxlen(&conn, key_id, max, approximate)?;
        }

        // Apply MINID trimming
        if let Some(min) = minid {
            self.trim_stream_minid(&conn, key_id, min)?;
        }

        // Release lock before async notification
        drop(conn);

        // Notify waiting readers in server mode
        if self.is_server_mode() {
            let key = key.to_string();
            let db = self.clone();
            tokio::spawn(async move {
                let _ = db.notify_key(&key).await;
            });
        }

        Ok(Some(entry_id))
    }

    /// Trim stream to MAXLEN
    fn trim_stream_maxlen(
        &self,
        conn: &Connection,
        key_id: i64,
        maxlen: i64,
        _approximate: bool, // For now, we always do exact trimming
    ) -> Result<i64> {
        // Delete oldest entries to keep at most maxlen entries
        let deleted = conn.execute(
            "DELETE FROM streams WHERE key_id = ?1 AND id NOT IN (
                SELECT id FROM streams WHERE key_id = ?1
                ORDER BY entry_ms DESC, entry_seq DESC
                LIMIT ?2
            )",
            params![key_id, maxlen],
        )?;
        Ok(deleted as i64)
    }

    /// Trim stream by MINID
    fn trim_stream_minid(&self, conn: &Connection, key_id: i64, minid: StreamId) -> Result<i64> {
        // Delete entries with ID less than minid
        let deleted = conn.execute(
            "DELETE FROM streams WHERE key_id = ?1 AND (entry_ms < ?2 OR (entry_ms = ?2 AND entry_seq < ?3))",
            params![key_id, minid.ms, minid.seq],
        )?;
        Ok(deleted as i64)
    }

    /// XLEN key - get number of entries in stream
    pub fn xlen(&self, key: &str) -> Result<i64> {
        self.maybe_autovacuum();
        let conn = self.core.conn.lock().unwrap_or_else(|e| e.into_inner());

        let key_id = match self.get_stream_key_id(&conn, key)? {
            Some(id) => id,
            None => return Ok(0),
        };

        let count: i64 = conn.query_row(
            "SELECT COUNT(*) FROM streams WHERE key_id = ?1",
            params![key_id],
            |row| row.get(0),
        )?;

        Ok(count)
    }

    /// XRANGE key start end [COUNT count] - get entries in ID range
    pub fn xrange(
        &self,
        key: &str,
        start: StreamId,
        end: StreamId,
        count: Option<i64>,
    ) -> Result<Vec<StreamEntry>> {
        self.maybe_autovacuum();
        let conn = self.core.conn.lock().unwrap_or_else(|e| e.into_inner());

        let key_id = match self.get_stream_key_id(&conn, key)? {
            Some(id) => id,
            None => return Ok(vec![]),
        };

        // Use a large default limit if no count specified
        let limit = count.unwrap_or(i64::MAX);

        let mut stmt = conn.prepare(
            "SELECT entry_ms, entry_seq, data FROM streams
             WHERE key_id = ?1
             AND (entry_ms > ?2 OR (entry_ms = ?2 AND entry_seq >= ?3))
             AND (entry_ms < ?4 OR (entry_ms = ?4 AND entry_seq <= ?5))
             ORDER BY entry_ms ASC, entry_seq ASC
             LIMIT ?6",
        )?;

        let entries: Vec<StreamEntry> = stmt
            .query_map(
                params![key_id, start.ms, start.seq, end.ms, end.seq, limit],
                |row| {
                    let ms: i64 = row.get(0)?;
                    let seq: i64 = row.get(1)?;
                    let data: Vec<u8> = row.get(2)?;
                    Ok(StreamEntry::new(
                        StreamId::new(ms, seq),
                        Self::decode_stream_fields(&data),
                    ))
                },
            )?
            .filter_map(|r| r.ok())
            .collect();

        Ok(entries)
    }

    /// XREVRANGE key end start [COUNT count] - get entries in reverse ID range
    pub fn xrevrange(
        &self,
        key: &str,
        end: StreamId,
        start: StreamId,
        count: Option<i64>,
    ) -> Result<Vec<StreamEntry>> {
        self.maybe_autovacuum();
        let conn = self.core.conn.lock().unwrap_or_else(|e| e.into_inner());

        let key_id = match self.get_stream_key_id(&conn, key)? {
            Some(id) => id,
            None => return Ok(vec![]),
        };

        // Use a large default limit if no count specified
        let limit = count.unwrap_or(i64::MAX);

        let mut stmt = conn.prepare(
            "SELECT entry_ms, entry_seq, data FROM streams
             WHERE key_id = ?1
             AND (entry_ms > ?2 OR (entry_ms = ?2 AND entry_seq >= ?3))
             AND (entry_ms < ?4 OR (entry_ms = ?4 AND entry_seq <= ?5))
             ORDER BY entry_ms DESC, entry_seq DESC
             LIMIT ?6",
        )?;

        let entries: Vec<StreamEntry> = stmt
            .query_map(
                params![key_id, start.ms, start.seq, end.ms, end.seq, limit],
                |row| {
                    let ms: i64 = row.get(0)?;
                    let seq: i64 = row.get(1)?;
                    let data: Vec<u8> = row.get(2)?;
                    Ok(StreamEntry::new(
                        StreamId::new(ms, seq),
                        Self::decode_stream_fields(&data),
                    ))
                },
            )?
            .filter_map(|r| r.ok())
            .collect();

        Ok(entries)
    }

    /// XREAD [COUNT count] STREAMS key [key ...] id [id ...]
    /// Returns entries from multiple streams starting after the given IDs
    pub fn xread(
        &self,
        keys: &[&str],
        ids: &[StreamId],
        count: Option<i64>,
    ) -> Result<Vec<(String, Vec<StreamEntry>)>> {
        if keys.len() != ids.len() {
            return Err(KvError::SyntaxError);
        }

        self.maybe_autovacuum();
        let mut results = Vec::new();

        for (key, id) in keys.iter().zip(ids.iter()) {
            // Get entries after the given ID (exclusive)
            let start = if id.seq == i64::MAX {
                StreamId::new(id.ms + 1, 0)
            } else {
                StreamId::new(id.ms, id.seq + 1)
            };
            let entries = self.xrange(key, start, StreamId::max(), count)?;
            if !entries.is_empty() {
                results.push(((*key).to_string(), entries));
            }
        }

        Ok(results)
    }

    /// XTRIM key MAXLEN|MINID [=|~] threshold
    pub fn xtrim(
        &self,
        key: &str,
        maxlen: Option<i64>,
        minid: Option<StreamId>,
        _approximate: bool,
    ) -> Result<i64> {
        let conn = self.core.conn.lock().unwrap_or_else(|e| e.into_inner());

        let key_id = match self.get_stream_key_id(&conn, key)? {
            Some(id) => id,
            None => return Ok(0),
        };

        let deleted = if let Some(max) = maxlen {
            self.trim_stream_maxlen(&conn, key_id, max, _approximate)?
        } else if let Some(min) = minid {
            self.trim_stream_minid(&conn, key_id, min)?
        } else {
            0
        };

        // Update key timestamp
        let now = Self::now_ms();
        conn.execute(
            "UPDATE keys SET updated_at = ?1 WHERE id = ?2",
            params![now, key_id],
        )?;

        Ok(deleted)
    }

    /// XDEL key id [id ...] - delete specific entries
    pub fn xdel(&self, key: &str, ids: &[StreamId]) -> Result<i64> {
        if ids.is_empty() {
            return Ok(0);
        }

        let conn = self.core.conn.lock().unwrap_or_else(|e| e.into_inner());

        let key_id = match self.get_stream_key_id(&conn, key)? {
            Some(id) => id,
            None => return Ok(0),
        };

        let mut deleted = 0i64;
        for id in ids {
            let count = conn.execute(
                "DELETE FROM streams WHERE key_id = ?1 AND entry_ms = ?2 AND entry_seq = ?3",
                params![key_id, id.ms, id.seq],
            )?;
            deleted += count as i64;
        }

        // Update key timestamp
        let now = Self::now_ms();
        conn.execute(
            "UPDATE keys SET updated_at = ?1 WHERE id = ?2",
            params![now, key_id],
        )?;

        Ok(deleted)
    }

    /// XINFO STREAM key - get stream info
    pub fn xinfo_stream(&self, key: &str) -> Result<Option<StreamInfo>> {
        self.maybe_autovacuum();
        let conn = self.core.conn.lock().unwrap_or_else(|e| e.into_inner());

        let key_id = match self.get_stream_key_id(&conn, key)? {
            Some(id) => id,
            None => return Ok(None),
        };

        // Get length
        let length: i64 = conn.query_row(
            "SELECT COUNT(*) FROM streams WHERE key_id = ?1",
            params![key_id],
            |row| row.get(0),
        )?;

        // Get last generated ID
        let last_id = self.get_last_stream_id(&conn, key_id);

        // Get first entry
        let first_entry: Option<StreamEntry> = conn
            .query_row(
                "SELECT entry_ms, entry_seq, data FROM streams WHERE key_id = ?1 ORDER BY entry_ms ASC, entry_seq ASC LIMIT 1",
                params![key_id],
                |row| {
                    let ms: i64 = row.get(0)?;
                    let seq: i64 = row.get(1)?;
                    let data: Vec<u8> = row.get(2)?;
                    Ok(StreamEntry::new(StreamId::new(ms, seq), Self::decode_stream_fields(&data)))
                },
            )
            .ok();

        // Get last entry
        let last_entry: Option<StreamEntry> = conn
            .query_row(
                "SELECT entry_ms, entry_seq, data FROM streams WHERE key_id = ?1 ORDER BY entry_ms DESC, entry_seq DESC LIMIT 1",
                params![key_id],
                |row| {
                    let ms: i64 = row.get(0)?;
                    let seq: i64 = row.get(1)?;
                    let data: Vec<u8> = row.get(2)?;
                    Ok(StreamEntry::new(StreamId::new(ms, seq), Self::decode_stream_fields(&data)))
                },
            )
            .ok();

        Ok(Some(StreamInfo {
            length,
            radix_tree_keys: 0,
            radix_tree_nodes: 0,
            last_generated_id: last_id,
            first_entry,
            last_entry,
        }))
    }

    // ==================== Consumer Group Operations ====================

    /// XGROUP CREATE key groupname id|$ [MKSTREAM]
    /// Creates a consumer group for a stream
    pub fn xgroup_create(
        &self,
        key: &str,
        group: &str,
        id: StreamId,
        mkstream: bool,
    ) -> Result<bool> {
        let conn = self.core.conn.lock().unwrap_or_else(|e| e.into_inner());

        // Check if stream exists
        let key_id = match self.get_stream_key_id(&conn, key)? {
            Some(id) => id,
            None => {
                if mkstream {
                    // Create empty stream
                    self.create_stream_key(&conn, key)?
                } else {
                    return Err(KvError::NoSuchKey);
                }
            }
        };

        // Check if group already exists
        let exists: bool = conn.query_row(
            "SELECT 1 FROM stream_groups WHERE key_id = ?1 AND name = ?2",
            params![key_id, group],
            |_| Ok(true),
        ).unwrap_or(false);

        if exists {
            return Err(KvError::BusyGroup);
        }

        // Create the group
        conn.execute(
            "INSERT INTO stream_groups (key_id, name, last_ms, last_seq) VALUES (?1, ?2, ?3, ?4)",
            params![key_id, group, id.ms, id.seq],
        )?;

        Ok(true)
    }

    /// XGROUP DESTROY key groupname
    /// Destroys a consumer group
    pub fn xgroup_destroy(&self, key: &str, group: &str) -> Result<bool> {
        let conn = self.core.conn.lock().unwrap_or_else(|e| e.into_inner());

        let key_id = match self.get_stream_key_id(&conn, key)? {
            Some(id) => id,
            None => return Ok(false),
        };

        let deleted = conn.execute(
            "DELETE FROM stream_groups WHERE key_id = ?1 AND name = ?2",
            params![key_id, group],
        )?;

        Ok(deleted > 0)
    }

    /// XGROUP SETID key groupname id|$
    /// Sets the last delivered ID for a consumer group
    pub fn xgroup_setid(&self, key: &str, group: &str, id: StreamId) -> Result<bool> {
        let conn = self.core.conn.lock().unwrap_or_else(|e| e.into_inner());

        let key_id = match self.get_stream_key_id(&conn, key)? {
            Some(id) => id,
            None => return Err(KvError::NoSuchKey),
        };

        let updated = conn.execute(
            "UPDATE stream_groups SET last_ms = ?1, last_seq = ?2 WHERE key_id = ?3 AND name = ?4",
            params![id.ms, id.seq, key_id, group],
        )?;

        if updated == 0 {
            return Err(KvError::NoGroup);
        }

        Ok(true)
    }

    /// XGROUP CREATECONSUMER key groupname consumername
    /// Creates a consumer in a consumer group
    pub fn xgroup_createconsumer(
        &self,
        key: &str,
        group: &str,
        consumer: &str,
    ) -> Result<bool> {
        let conn = self.core.conn.lock().unwrap_or_else(|e| e.into_inner());
        let now = Self::now_ms();

        let key_id = match self.get_stream_key_id(&conn, key)? {
            Some(id) => id,
            None => return Err(KvError::NoSuchKey),
        };

        // Get group_id
        let group_id: i64 = match conn.query_row(
            "SELECT id FROM stream_groups WHERE key_id = ?1 AND name = ?2",
            params![key_id, group],
            |row| row.get(0),
        ) {
            Ok(id) => id,
            Err(_) => return Err(KvError::NoGroup),
        };

        // Check if consumer exists
        let exists: bool = conn.query_row(
            "SELECT 1 FROM stream_consumers WHERE group_id = ?1 AND name = ?2",
            params![group_id, consumer],
            |_| Ok(true),
        ).unwrap_or(false);

        if exists {
            return Ok(false); // Consumer already exists
        }

        // Create consumer
        conn.execute(
            "INSERT INTO stream_consumers (group_id, name, seen_time) VALUES (?1, ?2, ?3)",
            params![group_id, consumer, now],
        )?;

        Ok(true)
    }

    /// XGROUP DELCONSUMER key groupname consumername
    /// Deletes a consumer from a consumer group
    /// Returns the number of pending entries that were deleted
    pub fn xgroup_delconsumer(
        &self,
        key: &str,
        group: &str,
        consumer: &str,
    ) -> Result<i64> {
        let conn = self.core.conn.lock().unwrap_or_else(|e| e.into_inner());

        let key_id = match self.get_stream_key_id(&conn, key)? {
            Some(id) => id,
            None => return Err(KvError::NoSuchKey),
        };

        // Get group_id
        let group_id: i64 = match conn.query_row(
            "SELECT id FROM stream_groups WHERE key_id = ?1 AND name = ?2",
            params![key_id, group],
            |row| row.get(0),
        ) {
            Ok(id) => id,
            Err(_) => return Err(KvError::NoGroup),
        };

        // Count pending entries for this consumer
        let pending_count: i64 = conn.query_row(
            "SELECT COUNT(*) FROM stream_pending WHERE group_id = ?1 AND consumer = ?2",
            params![group_id, consumer],
            |row| row.get(0),
        ).unwrap_or(0);

        // Delete pending entries for this consumer
        conn.execute(
            "DELETE FROM stream_pending WHERE group_id = ?1 AND consumer = ?2",
            params![group_id, consumer],
        )?;

        // Delete consumer
        conn.execute(
            "DELETE FROM stream_consumers WHERE group_id = ?1 AND name = ?2",
            params![group_id, consumer],
        )?;

        Ok(pending_count)
    }

    /// Helper: Get or create a consumer in a group
    fn get_or_create_consumer(&self, conn: &Connection, group_id: i64, consumer: &str) -> Result<i64> {
        let now = Self::now_ms();

        // Try to get existing consumer
        let result = conn.query_row(
            "SELECT id FROM stream_consumers WHERE group_id = ?1 AND name = ?2",
            params![group_id, consumer],
            |row| row.get(0),
        );

        match result {
            Ok(id) => {
                // Update seen_time
                conn.execute(
                    "UPDATE stream_consumers SET seen_time = ?1 WHERE id = ?2",
                    params![now, id],
                )?;
                Ok(id)
            }
            Err(_) => {
                // Create new consumer
                conn.execute(
                    "INSERT INTO stream_consumers (group_id, name, seen_time) VALUES (?1, ?2, ?3)",
                    params![group_id, consumer, now],
                )?;
                Ok(conn.last_insert_rowid())
            }
        }
    }

    /// Helper: Get group info (id, last_ms, last_seq) for a stream
    fn get_group_info(&self, conn: &Connection, key_id: i64, group: &str) -> Result<Option<(i64, i64, i64)>> {
        let result = conn.query_row(
            "SELECT id, last_ms, last_seq FROM stream_groups WHERE key_id = ?1 AND name = ?2",
            params![key_id, group],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        );

        match result {
            Ok(info) => Ok(Some(info)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    /// Helper: Get stream entry by stream_id (ms, seq)
    fn get_stream_entry_id(&self, conn: &Connection, key_id: i64, stream_id: &StreamId) -> Option<i64> {
        conn.query_row(
            "SELECT id FROM streams WHERE key_id = ?1 AND entry_ms = ?2 AND entry_seq = ?3",
            params![key_id, stream_id.ms, stream_id.seq],
            |row| row.get(0),
        ).ok()
    }

    /// XREADGROUP GROUP group consumer [COUNT count] [NOACK] STREAMS key [key ...] id [id ...]
    /// Reads from streams as part of a consumer group
    pub fn xreadgroup(
        &self,
        group: &str,
        consumer: &str,
        keys: &[&str],
        ids: &[&str],  // ">" means new, other IDs mean pending
        count: Option<i64>,
        noack: bool,
    ) -> Result<Vec<(String, Vec<StreamEntry>)>> {
        if keys.len() != ids.len() {
            return Err(KvError::SyntaxError);
        }

        self.maybe_autovacuum();
        let conn = self.core.conn.lock().unwrap_or_else(|e| e.into_inner());
        let now = Self::now_ms();
        let mut results = Vec::new();

        for (key, id_str) in keys.iter().zip(ids.iter()) {
            let key_id = match self.get_stream_key_id(&conn, key)? {
                Some(id) => id,
                None => continue,
            };

            let (group_id, last_ms, last_seq) = match self.get_group_info(&conn, key_id, group)? {
                Some(info) => info,
                None => return Err(KvError::NoGroup),
            };

            // Ensure consumer exists
            self.get_or_create_consumer(&conn, group_id, consumer)?;

            let entries: Vec<StreamEntry> = if *id_str == ">" {
                // Read new entries (after last_delivered_id)
                let start = if last_seq == i64::MAX {
                    StreamId::new(last_ms + 1, 0)
                } else {
                    StreamId::new(last_ms, last_seq + 1)
                };

                let limit = count.unwrap_or(i64::MAX);

                let mut stmt = conn.prepare(
                    "SELECT id, entry_ms, entry_seq, data FROM streams
                     WHERE key_id = ?1
                     AND (entry_ms > ?2 OR (entry_ms = ?2 AND entry_seq >= ?3))
                     ORDER BY entry_ms ASC, entry_seq ASC
                     LIMIT ?4",
                )?;

                let entries: Vec<(i64, StreamEntry)> = stmt
                    .query_map(
                        params![key_id, start.ms, start.seq, limit],
                        |row| {
                            let db_id: i64 = row.get(0)?;
                            let ms: i64 = row.get(1)?;
                            let seq: i64 = row.get(2)?;
                            let data: Vec<u8> = row.get(3)?;
                            Ok((db_id, StreamEntry::new(
                                StreamId::new(ms, seq),
                                Self::decode_stream_fields(&data),
                            )))
                        },
                    )?
                    .filter_map(|r| r.ok())
                    .collect();

                // Update last_delivered_id and add to pending (unless NOACK)
                if !entries.is_empty() {
                    let last_entry = entries.last().unwrap();
                    conn.execute(
                        "UPDATE stream_groups SET last_ms = ?1, last_seq = ?2 WHERE id = ?3",
                        params![last_entry.1.id.ms, last_entry.1.id.seq, group_id],
                    )?;

                    if !noack {
                        for (db_id, _entry) in &entries {
                            conn.execute(
                                "INSERT OR REPLACE INTO stream_pending
                                 (key_id, group_id, entry_id, consumer, delivered_at, delivery_count)
                                 VALUES (?1, ?2, ?3, ?4, ?5, 1)",
                                params![key_id, group_id, db_id, consumer, now],
                            )?;
                        }
                    }
                }

                entries.into_iter().map(|(_, e)| e).collect()
            } else {
                // Read pending entries for this consumer (re-delivery)
                let start_id = if *id_str == "0" || *id_str == "0-0" {
                    StreamId::min()
                } else {
                    match StreamId::parse(id_str) {
                        Some(id) => id,
                        None => return Err(KvError::SyntaxError),
                    }
                };

                let limit = count.unwrap_or(i64::MAX);

                // Get pending entries for this consumer
                let mut stmt = conn.prepare(
                    "SELECT s.id, s.entry_ms, s.entry_seq, s.data, sp.id as pending_id
                     FROM stream_pending sp
                     JOIN streams s ON s.id = sp.entry_id
                     WHERE sp.group_id = ?1 AND sp.consumer = ?2
                     AND (s.entry_ms > ?3 OR (s.entry_ms = ?3 AND s.entry_seq >= ?4))
                     ORDER BY s.entry_ms ASC, s.entry_seq ASC
                     LIMIT ?5",
                )?;

                let entries: Vec<(i64, StreamEntry, i64)> = stmt
                    .query_map(
                        params![group_id, consumer, start_id.ms, start_id.seq, limit],
                        |row| {
                            let db_id: i64 = row.get(0)?;
                            let ms: i64 = row.get(1)?;
                            let seq: i64 = row.get(2)?;
                            let data: Vec<u8> = row.get(3)?;
                            let pending_id: i64 = row.get(4)?;
                            Ok((db_id, StreamEntry::new(
                                StreamId::new(ms, seq),
                                Self::decode_stream_fields(&data),
                            ), pending_id))
                        },
                    )?
                    .filter_map(|r| r.ok())
                    .collect();

                // Update delivery count and time
                for (_, _, pending_id) in &entries {
                    conn.execute(
                        "UPDATE stream_pending SET delivered_at = ?1, delivery_count = delivery_count + 1 WHERE id = ?2",
                        params![now, pending_id],
                    )?;
                }

                entries.into_iter().map(|(_, e, _)| e).collect()
            };

            if !entries.is_empty() {
                results.push(((*key).to_string(), entries));
            }
        }

        Ok(results)
    }

    /// XACK key group id [id ...]
    /// Acknowledges messages, removing them from the pending list
    pub fn xack(&self, key: &str, group: &str, ids: &[StreamId]) -> Result<i64> {
        if ids.is_empty() {
            return Ok(0);
        }

        let conn = self.core.conn.lock().unwrap_or_else(|e| e.into_inner());

        let key_id = match self.get_stream_key_id(&conn, key)? {
            Some(id) => id,
            None => return Ok(0),
        };

        let group_id: i64 = match conn.query_row(
            "SELECT id FROM stream_groups WHERE key_id = ?1 AND name = ?2",
            params![key_id, group],
            |row| row.get(0),
        ) {
            Ok(id) => id,
            Err(_) => return Ok(0), // Group doesn't exist
        };

        let mut acked = 0i64;
        for id in ids {
            // Find the stream entry id
            if let Some(entry_id) = self.get_stream_entry_id(&conn, key_id, id) {
                let deleted = conn.execute(
                    "DELETE FROM stream_pending WHERE group_id = ?1 AND entry_id = ?2",
                    params![group_id, entry_id],
                )?;
                acked += deleted as i64;
            }
        }

        Ok(acked)
    }

    /// XPENDING key group [[IDLE min-idle-time] start end count [consumer]]
    /// Returns pending entries info
    pub fn xpending_summary(&self, key: &str, group: &str) -> Result<PendingSummary> {
        let conn = self.core.conn.lock().unwrap_or_else(|e| e.into_inner());

        let key_id = match self.get_stream_key_id(&conn, key)? {
            Some(id) => id,
            None => return Ok(PendingSummary {
                count: 0,
                smallest_id: None,
                largest_id: None,
                consumers: vec![],
            }),
        };

        let group_id: i64 = match conn.query_row(
            "SELECT id FROM stream_groups WHERE key_id = ?1 AND name = ?2",
            params![key_id, group],
            |row| row.get(0),
        ) {
            Ok(id) => id,
            Err(_) => return Err(KvError::NoGroup),
        };

        // Get total count
        let count: i64 = conn.query_row(
            "SELECT COUNT(*) FROM stream_pending WHERE group_id = ?1",
            params![group_id],
            |row| row.get(0),
        )?;

        if count == 0 {
            return Ok(PendingSummary {
                count: 0,
                smallest_id: None,
                largest_id: None,
                consumers: vec![],
            });
        }

        // Get smallest ID
        let smallest_id: Option<StreamId> = conn.query_row(
            "SELECT s.entry_ms, s.entry_seq FROM stream_pending sp
             JOIN streams s ON s.id = sp.entry_id
             WHERE sp.group_id = ?1
             ORDER BY s.entry_ms ASC, s.entry_seq ASC LIMIT 1",
            params![group_id],
            |row| Ok(StreamId::new(row.get(0)?, row.get(1)?)),
        ).ok();

        // Get largest ID
        let largest_id: Option<StreamId> = conn.query_row(
            "SELECT s.entry_ms, s.entry_seq FROM stream_pending sp
             JOIN streams s ON s.id = sp.entry_id
             WHERE sp.group_id = ?1
             ORDER BY s.entry_ms DESC, s.entry_seq DESC LIMIT 1",
            params![group_id],
            |row| Ok(StreamId::new(row.get(0)?, row.get(1)?)),
        ).ok();

        // Get per-consumer counts
        let mut stmt = conn.prepare(
            "SELECT consumer, COUNT(*) FROM stream_pending WHERE group_id = ?1 GROUP BY consumer",
        )?;
        let consumers: Vec<(String, i64)> = stmt
            .query_map(params![group_id], |row| Ok((row.get(0)?, row.get(1)?)))?
            .filter_map(|r| r.ok())
            .collect();

        Ok(PendingSummary {
            count,
            smallest_id,
            largest_id,
            consumers,
        })
    }

    /// XPENDING key group [IDLE min-idle-time] start end count [consumer]
    /// Returns detailed pending entries
    pub fn xpending_range(
        &self,
        key: &str,
        group: &str,
        start: StreamId,
        end: StreamId,
        count: i64,
        consumer: Option<&str>,
        idle_time: Option<i64>,
    ) -> Result<Vec<PendingEntry>> {
        let conn = self.core.conn.lock().unwrap_or_else(|e| e.into_inner());
        let now = Self::now_ms();

        let key_id = match self.get_stream_key_id(&conn, key)? {
            Some(id) => id,
            None => return Ok(vec![]),
        };

        let group_id: i64 = match conn.query_row(
            "SELECT id FROM stream_groups WHERE key_id = ?1 AND name = ?2",
            params![key_id, group],
            |row| row.get(0),
        ) {
            Ok(id) => id,
            Err(_) => return Err(KvError::NoGroup),
        };

        // Build query based on options
        let (sql, params_vec): (String, Vec<Box<dyn rusqlite::ToSql>>) = if let Some(c) = consumer {
            if let Some(idle) = idle_time {
                let idle_cutoff = now - idle;
                (
                    "SELECT s.entry_ms, s.entry_seq, sp.consumer, sp.delivered_at, sp.delivery_count
                     FROM stream_pending sp
                     JOIN streams s ON s.id = sp.entry_id
                     WHERE sp.group_id = ?1 AND sp.consumer = ?2 AND sp.delivered_at <= ?3
                     AND (s.entry_ms > ?4 OR (s.entry_ms = ?4 AND s.entry_seq >= ?5))
                     AND (s.entry_ms < ?6 OR (s.entry_ms = ?6 AND s.entry_seq <= ?7))
                     ORDER BY s.entry_ms ASC, s.entry_seq ASC
                     LIMIT ?8".to_string(),
                    vec![
                        Box::new(group_id) as Box<dyn rusqlite::ToSql>,
                        Box::new(c.to_string()),
                        Box::new(idle_cutoff),
                        Box::new(start.ms),
                        Box::new(start.seq),
                        Box::new(end.ms),
                        Box::new(end.seq),
                        Box::new(count),
                    ],
                )
            } else {
                (
                    "SELECT s.entry_ms, s.entry_seq, sp.consumer, sp.delivered_at, sp.delivery_count
                     FROM stream_pending sp
                     JOIN streams s ON s.id = sp.entry_id
                     WHERE sp.group_id = ?1 AND sp.consumer = ?2
                     AND (s.entry_ms > ?3 OR (s.entry_ms = ?3 AND s.entry_seq >= ?4))
                     AND (s.entry_ms < ?5 OR (s.entry_ms = ?5 AND s.entry_seq <= ?6))
                     ORDER BY s.entry_ms ASC, s.entry_seq ASC
                     LIMIT ?7".to_string(),
                    vec![
                        Box::new(group_id) as Box<dyn rusqlite::ToSql>,
                        Box::new(c.to_string()),
                        Box::new(start.ms),
                        Box::new(start.seq),
                        Box::new(end.ms),
                        Box::new(end.seq),
                        Box::new(count),
                    ],
                )
            }
        } else if let Some(idle) = idle_time {
            let idle_cutoff = now - idle;
            (
                "SELECT s.entry_ms, s.entry_seq, sp.consumer, sp.delivered_at, sp.delivery_count
                 FROM stream_pending sp
                 JOIN streams s ON s.id = sp.entry_id
                 WHERE sp.group_id = ?1 AND sp.delivered_at <= ?2
                 AND (s.entry_ms > ?3 OR (s.entry_ms = ?3 AND s.entry_seq >= ?4))
                 AND (s.entry_ms < ?5 OR (s.entry_ms = ?5 AND s.entry_seq <= ?6))
                 ORDER BY s.entry_ms ASC, s.entry_seq ASC
                 LIMIT ?7".to_string(),
                vec![
                    Box::new(group_id) as Box<dyn rusqlite::ToSql>,
                    Box::new(idle_cutoff),
                    Box::new(start.ms),
                    Box::new(start.seq),
                    Box::new(end.ms),
                    Box::new(end.seq),
                    Box::new(count),
                ],
            )
        } else {
            (
                "SELECT s.entry_ms, s.entry_seq, sp.consumer, sp.delivered_at, sp.delivery_count
                 FROM stream_pending sp
                 JOIN streams s ON s.id = sp.entry_id
                 WHERE sp.group_id = ?1
                 AND (s.entry_ms > ?2 OR (s.entry_ms = ?2 AND s.entry_seq >= ?3))
                 AND (s.entry_ms < ?4 OR (s.entry_ms = ?4 AND s.entry_seq <= ?5))
                 ORDER BY s.entry_ms ASC, s.entry_seq ASC
                 LIMIT ?6".to_string(),
                vec![
                    Box::new(group_id) as Box<dyn rusqlite::ToSql>,
                    Box::new(start.ms),
                    Box::new(start.seq),
                    Box::new(end.ms),
                    Box::new(end.seq),
                    Box::new(count),
                ],
            )
        };

        let mut stmt = conn.prepare(&sql)?;
        let params_refs: Vec<&dyn rusqlite::ToSql> = params_vec.iter().map(|p| p.as_ref()).collect();

        let entries: Vec<PendingEntry> = stmt
            .query_map(params_refs.as_slice(), |row| {
                let ms: i64 = row.get(0)?;
                let seq: i64 = row.get(1)?;
                let consumer: String = row.get(2)?;
                let delivered_at: i64 = row.get(3)?;
                let delivery_count: i64 = row.get(4)?;
                Ok(PendingEntry {
                    id: StreamId::new(ms, seq),
                    consumer,
                    idle: now - delivered_at,
                    delivery_count,
                })
            })?
            .filter_map(|r| r.ok())
            .collect();

        Ok(entries)
    }

    /// XCLAIM key group consumer min-idle-time id [id ...] [IDLE ms] [TIME ms] [RETRYCOUNT count] [FORCE] [JUSTID]
    /// Claims pending entries and transfers them to a different consumer
    pub fn xclaim(
        &self,
        key: &str,
        group: &str,
        consumer: &str,
        min_idle_time: i64,
        ids: &[StreamId],
        idle_ms: Option<i64>,
        time_ms: Option<i64>,
        retry_count: Option<i64>,
        force: bool,
        justid: bool,
    ) -> Result<Vec<StreamEntry>> {
        let conn = self.core.conn.lock().unwrap_or_else(|e| e.into_inner());
        let now = Self::now_ms();

        let key_id = match self.get_stream_key_id(&conn, key)? {
            Some(id) => id,
            None => return Ok(vec![]),
        };

        let group_id: i64 = match conn.query_row(
            "SELECT id FROM stream_groups WHERE key_id = ?1 AND name = ?2",
            params![key_id, group],
            |row| row.get(0),
        ) {
            Ok(id) => id,
            Err(_) => return Err(KvError::NoGroup),
        };

        // Ensure consumer exists
        self.get_or_create_consumer(&conn, group_id, consumer)?;

        let idle_cutoff = now - min_idle_time;
        let new_delivered_at = time_ms.unwrap_or(now);
        let new_idle = if let Some(idle) = idle_ms {
            now - idle
        } else {
            new_delivered_at
        };

        let mut claimed = Vec::new();

        for id in ids {
            // Get the stream entry
            let entry_result: std::result::Result<(i64, Vec<u8>), _> = conn.query_row(
                "SELECT id, data FROM streams WHERE key_id = ?1 AND entry_ms = ?2 AND entry_seq = ?3",
                params![key_id, id.ms, id.seq],
                |row| Ok((row.get(0)?, row.get(1)?)),
            );

            let (entry_id, data) = match entry_result {
                Ok((eid, d)) => (eid, d),
                Err(_) => continue, // Entry doesn't exist
            };

            // Check if entry is in pending list and meets idle requirement
            let pending_info: Option<(i64, i64, i64)> = conn.query_row(
                "SELECT id, delivered_at, delivery_count FROM stream_pending
                 WHERE group_id = ?1 AND entry_id = ?2",
                params![group_id, entry_id],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            ).ok();

            match pending_info {
                Some((pending_id, delivered_at, old_count)) => {
                    // Check if idle enough
                    if delivered_at > idle_cutoff && !force {
                        continue; // Not idle enough
                    }

                    // Update pending entry
                    let new_count = retry_count.unwrap_or(old_count + 1);
                    conn.execute(
                        "UPDATE stream_pending SET consumer = ?1, delivered_at = ?2, delivery_count = ?3 WHERE id = ?4",
                        params![consumer, new_idle, new_count, pending_id],
                    )?;
                }
                None => {
                    // Entry not in pending list
                    if force {
                        // Add to pending list
                        let new_count = retry_count.unwrap_or(1);
                        conn.execute(
                            "INSERT INTO stream_pending (key_id, group_id, entry_id, consumer, delivered_at, delivery_count)
                             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                            params![key_id, group_id, entry_id, consumer, new_idle, new_count],
                        )?;
                    } else {
                        continue;
                    }
                }
            }

            if !justid {
                claimed.push(StreamEntry::new(*id, Self::decode_stream_fields(&data)));
            } else {
                claimed.push(StreamEntry::new(*id, vec![]));
            }
        }

        Ok(claimed)
    }

    /// XINFO GROUPS key - get consumer groups for a stream
    pub fn xinfo_groups(&self, key: &str) -> Result<Vec<ConsumerGroupInfo>> {
        self.maybe_autovacuum();
        let conn = self.core.conn.lock().unwrap_or_else(|e| e.into_inner());

        let key_id = match self.get_stream_key_id(&conn, key)? {
            Some(id) => id,
            None => return Ok(vec![]),
        };

        let mut stmt = conn.prepare(
            "SELECT sg.id, sg.name, sg.last_ms, sg.last_seq FROM stream_groups sg WHERE sg.key_id = ?1",
        )?;

        let groups: Vec<(i64, String, i64, i64)> = stmt
            .query_map(params![key_id], |row| {
                Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?))
            })?
            .filter_map(|r| r.ok())
            .collect();

        let mut result = Vec::new();
        for (group_id, name, last_ms, last_seq) in groups {
            // Count consumers
            let consumers: i64 = conn.query_row(
                "SELECT COUNT(*) FROM stream_consumers WHERE group_id = ?1",
                params![group_id],
                |row| row.get(0),
            ).unwrap_or(0);

            // Count pending
            let pending: i64 = conn.query_row(
                "SELECT COUNT(*) FROM stream_pending WHERE group_id = ?1",
                params![group_id],
                |row| row.get(0),
            ).unwrap_or(0);

            result.push(ConsumerGroupInfo {
                name,
                consumers,
                pending,
                last_delivered_id: StreamId::new(last_ms, last_seq),
            });
        }

        Ok(result)
    }

    /// XINFO CONSUMERS key groupname - get consumers in a group
    pub fn xinfo_consumers(&self, key: &str, group: &str) -> Result<Vec<ConsumerInfo>> {
        self.maybe_autovacuum();
        let conn = self.core.conn.lock().unwrap_or_else(|e| e.into_inner());
        let now = Self::now_ms();

        let key_id = match self.get_stream_key_id(&conn, key)? {
            Some(id) => id,
            None => return Err(KvError::NoSuchKey),
        };

        let group_id: i64 = match conn.query_row(
            "SELECT id FROM stream_groups WHERE key_id = ?1 AND name = ?2",
            params![key_id, group],
            |row| row.get(0),
        ) {
            Ok(id) => id,
            Err(_) => return Err(KvError::NoGroup),
        };

        let mut stmt = conn.prepare(
            "SELECT sc.name, sc.seen_time FROM stream_consumers sc WHERE sc.group_id = ?1",
        )?;

        let consumers: Vec<(String, i64)> = stmt
            .query_map(params![group_id], |row| {
                Ok((row.get(0)?, row.get(1)?))
            })?
            .filter_map(|r| r.ok())
            .collect();

        let mut result = Vec::new();
        for (name, seen_time) in consumers {
            // Count pending for this consumer
            let pending: i64 = conn.query_row(
                "SELECT COUNT(*) FROM stream_pending WHERE group_id = ?1 AND consumer = ?2",
                params![group_id, name],
                |row| row.get(0),
            ).unwrap_or(0);

            result.push(ConsumerInfo {
                name: name.clone(),
                pending,
                idle: now - seen_time,
            });
        }

        Ok(result)
    }

    /// Check if database is in server mode (has notifier attached)
    pub fn is_server_mode(&self) -> bool {
        self.core.notifier.read().unwrap().is_some()
    }

    /// Send notification to all subscribers of a key
    pub async fn notify_key(&self, key: &str) -> Result<()> {
        if let Some(notifier) = self.core.notifier.read().unwrap().as_ref() {
            let map = notifier.read().unwrap();
            if let Some(sender) = map.get(key) {
                let _ = sender.send(());
            }
        }
        Ok(())
    }

    /// Subscribe to notifications for a key
    pub async fn subscribe_key(&self, key: &str) -> broadcast::Receiver<()> {
        if let Some(notifier) = self.core.notifier.read().unwrap().as_ref() {
            let notifier = Arc::clone(notifier);
            let mut map = notifier.write().unwrap();
            let sender = map
                .entry(key.to_string())
                .or_insert_with(|| {
                    let (tx, _) = broadcast::channel(128);
                    tx
                })
                .clone();
            sender.subscribe()
        } else {
            // For embedded mode - return a never-firing receiver
            let (tx, rx) = broadcast::channel(1);
            drop(tx); // Sender dropped, receiver will never fire
            rx
        }
    }

    /// Attach notifier to database for server mode
    pub fn with_notifier(
        &self,
        notifier: Arc<RwLock<HashMap<String, broadcast::Sender<()>>>>,
    ) {
        *self.core.notifier.write().unwrap() = Some(notifier);
    }

    /// BLPOP key [key ...] timeout
    /// Block and pop from the left (head) of lists
    pub async fn blpop(&self, keys: &[&str], timeout: f64) -> Result<Option<(String, Vec<u8>)>> {
        let deadline = if timeout == 0.0 {
            // 0 means block indefinitely (very far in future)
            tokio::time::Instant::now() + Duration::from_secs(u64::MAX / 2)
        } else {
            tokio::time::Instant::now() + Duration::from_secs_f64(timeout)
        };

        loop {
            // Try immediate pop on all keys (in order)
            for key in keys {
                if let Ok(values) = self.lpop(key, Some(1)) {
                    if !values.is_empty() {
                        return Ok(Some(((*key).to_string(), values[0].clone())));
                    }
                }
            }

            // Check timeout
            if tokio::time::Instant::now() >= deadline {
                return Ok(None);
            }

            // Subscribe to all keys and wait for any notification with short timeout
            let mut receivers = Vec::new();
            for key in keys {
                receivers.push(self.subscribe_key(key).await);
            }

            // Wait for the first notification or a short sleep (100ms)
            let remaining = deadline - tokio::time::Instant::now();
            let wait_duration = std::cmp::min(remaining, Duration::from_millis(100));

            // Set up the first few select branches (up to 5 keys)
            let mut rx_iter = receivers.iter_mut();

            if let Some(rx0) = rx_iter.next() {
                match rx_iter.next() {
                    Some(rx1) => {
                        match rx_iter.next() {
                            Some(rx2) => {
                                match rx_iter.next() {
                                    Some(rx3) => {
                                        match rx_iter.next() {
                                            Some(rx4) => {
                                                tokio::select! {
                                                    _ = rx0.recv() => {},
                                                    _ = rx1.recv() => {},
                                                    _ = rx2.recv() => {},
                                                    _ = rx3.recv() => {},
                                                    _ = rx4.recv() => {},
                                                    _ = tokio::time::sleep(wait_duration) => {},
                                                }
                                            }
                                            None => {
                                                tokio::select! {
                                                    _ = rx0.recv() => {},
                                                    _ = rx1.recv() => {},
                                                    _ = rx2.recv() => {},
                                                    _ = rx3.recv() => {},
                                                    _ = tokio::time::sleep(wait_duration) => {},
                                                }
                                            }
                                        }
                                    }
                                    None => {
                                        tokio::select! {
                                            _ = rx0.recv() => {},
                                            _ = rx1.recv() => {},
                                            _ = rx2.recv() => {},
                                            _ = tokio::time::sleep(wait_duration) => {},
                                        }
                                    }
                                }
                            }
                            None => {
                                tokio::select! {
                                    _ = rx0.recv() => {},
                                    _ = rx1.recv() => {},
                                    _ = tokio::time::sleep(wait_duration) => {},
                                }
                            }
                        }
                    }
                    None => {
                        tokio::select! {
                            _ = rx0.recv() => {},
                            _ = tokio::time::sleep(wait_duration) => {},
                        }
                    }
                }
            } else {
                // No keys provided (shouldn't happen, but handle it)
                tokio::time::sleep(wait_duration).await;
            }

            // Loop continues and retries
        }
    }

    /// BRPOP key [key ...] timeout
    /// Block and pop from the right (tail) of lists
    pub async fn brpop(&self, keys: &[&str], timeout: f64) -> Result<Option<(String, Vec<u8>)>> {
        let deadline = if timeout == 0.0 {
            // 0 means block indefinitely (very far in future)
            tokio::time::Instant::now() + Duration::from_secs(u64::MAX / 2)
        } else {
            tokio::time::Instant::now() + Duration::from_secs_f64(timeout)
        };

        loop {
            // Try immediate pop on all keys (in order)
            for key in keys {
                if let Ok(values) = self.rpop(key, Some(1)) {
                    if !values.is_empty() {
                        return Ok(Some(((*key).to_string(), values[0].clone())));
                    }
                }
            }

            // Check timeout
            if tokio::time::Instant::now() >= deadline {
                return Ok(None);
            }

            // Subscribe to all keys and wait for any notification with short timeout
            let mut receivers = Vec::new();
            for key in keys {
                receivers.push(self.subscribe_key(key).await);
            }

            // Wait for the first notification or a short sleep (100ms)
            let remaining = deadline - tokio::time::Instant::now();
            let wait_duration = std::cmp::min(remaining, Duration::from_millis(100));

            // Set up the first few select branches (up to 5 keys)
            let mut rx_iter = receivers.iter_mut();

            if let Some(rx0) = rx_iter.next() {
                match rx_iter.next() {
                    Some(rx1) => {
                        match rx_iter.next() {
                            Some(rx2) => {
                                match rx_iter.next() {
                                    Some(rx3) => {
                                        match rx_iter.next() {
                                            Some(rx4) => {
                                                tokio::select! {
                                                    _ = rx0.recv() => {},
                                                    _ = rx1.recv() => {},
                                                    _ = rx2.recv() => {},
                                                    _ = rx3.recv() => {},
                                                    _ = rx4.recv() => {},
                                                    _ = tokio::time::sleep(wait_duration) => {},
                                                }
                                            }
                                            None => {
                                                tokio::select! {
                                                    _ = rx0.recv() => {},
                                                    _ = rx1.recv() => {},
                                                    _ = rx2.recv() => {},
                                                    _ = rx3.recv() => {},
                                                    _ = tokio::time::sleep(wait_duration) => {},
                                                }
                                            }
                                        }
                                    }
                                    None => {
                                        tokio::select! {
                                            _ = rx0.recv() => {},
                                            _ = rx1.recv() => {},
                                            _ = rx2.recv() => {},
                                            _ = tokio::time::sleep(wait_duration) => {},
                                        }
                                    }
                                }
                            }
                            None => {
                                tokio::select! {
                                    _ = rx0.recv() => {},
                                    _ = rx1.recv() => {},
                                    _ = tokio::time::sleep(wait_duration) => {},
                                }
                            }
                        }
                    }
                    None => {
                        tokio::select! {
                            _ = rx0.recv() => {},
                            _ = tokio::time::sleep(wait_duration) => {},
                        }
                    }
                }
            } else {
                // No keys provided (shouldn't happen, but handle it)
                tokio::time::sleep(wait_duration).await;
            }

            // Loop continues and retries
        }
    }

    /// XREAD BLOCK timeout [COUNT count] STREAMS key [key ...] id [id ...]
    /// Block and read from streams
    pub async fn xread_block(
        &self,
        keys: &[&str],
        ids: &[StreamId],
        count: Option<i64>,
        timeout_ms: i64,
    ) -> Result<Vec<(String, Vec<StreamEntry>)>> {
        let deadline = if timeout_ms == 0 {
            // 0 means block indefinitely (very far in future)
            tokio::time::Instant::now() + Duration::from_secs(u64::MAX / 2)
        } else {
            tokio::time::Instant::now() + Duration::from_millis(timeout_ms as u64)
        };

        loop {
            // Try immediate read
            if let Ok(results) = self.xread(keys, ids, count) {
                if !results.is_empty() {
                    return Ok(results);
                }
            }

            // Check timeout
            if tokio::time::Instant::now() >= deadline {
                return Ok(vec![]);
            }

            // Subscribe to all keys and wait for any notification with short timeout
            let mut receivers = Vec::new();
            for key in keys {
                receivers.push(self.subscribe_key(key).await);
            }

            // Wait for the first notification or a short sleep (100ms)
            let remaining = deadline - tokio::time::Instant::now();
            let wait_duration = std::cmp::min(remaining, Duration::from_millis(100));

            // Set up the first few select branches (up to 5 keys)
            let mut rx_iter = receivers.iter_mut();

            if let Some(rx0) = rx_iter.next() {
                match rx_iter.next() {
                    Some(rx1) => {
                        match rx_iter.next() {
                            Some(rx2) => {
                                match rx_iter.next() {
                                    Some(rx3) => {
                                        match rx_iter.next() {
                                            Some(rx4) => {
                                                tokio::select! {
                                                    _ = rx0.recv() => {},
                                                    _ = rx1.recv() => {},
                                                    _ = rx2.recv() => {},
                                                    _ = rx3.recv() => {},
                                                    _ = rx4.recv() => {},
                                                    _ = tokio::time::sleep(wait_duration) => {},
                                                }
                                            }
                                            None => {
                                                tokio::select! {
                                                    _ = rx0.recv() => {},
                                                    _ = rx1.recv() => {},
                                                    _ = rx2.recv() => {},
                                                    _ = rx3.recv() => {},
                                                    _ = tokio::time::sleep(wait_duration) => {},
                                                }
                                            }
                                        }
                                    }
                                    None => {
                                        tokio::select! {
                                            _ = rx0.recv() => {},
                                            _ = rx1.recv() => {},
                                            _ = rx2.recv() => {},
                                            _ = tokio::time::sleep(wait_duration) => {},
                                        }
                                    }
                                }
                            }
                            None => {
                                tokio::select! {
                                    _ = rx0.recv() => {},
                                    _ = rx1.recv() => {},
                                    _ = tokio::time::sleep(wait_duration) => {},
                                }
                            }
                        }
                    }
                    None => {
                        tokio::select! {
                            _ = rx0.recv() => {},
                            _ = tokio::time::sleep(wait_duration) => {},
                        }
                    }
                }
            } else {
                // No keys provided (shouldn't happen, but handle it)
                tokio::time::sleep(wait_duration).await;
            }

            // Loop continues and retries
        }
    }

    /// XREADGROUP BLOCK timeout GROUP group consumer STREAMS key [key ...] id [id ...]
    /// Block and read from streams with consumer groups
    pub async fn xreadgroup_block(
        &self,
        group: &str,
        consumer: &str,
        keys: &[&str],
        ids: &[&str],
        count: Option<i64>,
        noack: bool,
        timeout_ms: i64,
    ) -> Result<Vec<(String, Vec<StreamEntry>)>> {
        let deadline = if timeout_ms == 0 {
            // 0 means block indefinitely (very far in future)
            tokio::time::Instant::now() + Duration::from_secs(u64::MAX / 2)
        } else {
            tokio::time::Instant::now() + Duration::from_millis(timeout_ms as u64)
        };

        loop {
            // Try immediate read
            if let Ok(results) = self.xreadgroup(group, consumer, keys, ids, count, noack) {
                if !results.is_empty() {
                    return Ok(results);
                }
            }

            // Check timeout
            if tokio::time::Instant::now() >= deadline {
                return Ok(vec![]);
            }

            // Subscribe to all keys and wait for any notification with short timeout
            let mut receivers = Vec::new();
            for key in keys {
                receivers.push(self.subscribe_key(key).await);
            }

            // Wait for the first notification or a short sleep (100ms)
            let remaining = deadline - tokio::time::Instant::now();
            let wait_duration = std::cmp::min(remaining, Duration::from_millis(100));

            // Set up the first few select branches (up to 5 keys)
            let mut rx_iter = receivers.iter_mut();

            if let Some(rx0) = rx_iter.next() {
                match rx_iter.next() {
                    Some(rx1) => {
                        match rx_iter.next() {
                            Some(rx2) => {
                                match rx_iter.next() {
                                    Some(rx3) => {
                                        match rx_iter.next() {
                                            Some(rx4) => {
                                                tokio::select! {
                                                    _ = rx0.recv() => {},
                                                    _ = rx1.recv() => {},
                                                    _ = rx2.recv() => {},
                                                    _ = rx3.recv() => {},
                                                    _ = rx4.recv() => {},
                                                    _ = tokio::time::sleep(wait_duration) => {},
                                                }
                                            }
                                            None => {
                                                tokio::select! {
                                                    _ = rx0.recv() => {},
                                                    _ = rx1.recv() => {},
                                                    _ = rx2.recv() => {},
                                                    _ = rx3.recv() => {},
                                                    _ = tokio::time::sleep(wait_duration) => {},
                                                }
                                            }
                                        }
                                    }
                                    None => {
                                        tokio::select! {
                                            _ = rx0.recv() => {},
                                            _ = rx1.recv() => {},
                                            _ = rx2.recv() => {},
                                            _ = tokio::time::sleep(wait_duration) => {},
                                        }
                                    }
                                }
                            }
                            None => {
                                tokio::select! {
                                    _ = rx0.recv() => {},
                                    _ = rx1.recv() => {},
                                    _ = tokio::time::sleep(wait_duration) => {},
                                }
                            }
                        }
                    }
                    None => {
                        tokio::select! {
                            _ = rx0.recv() => {},
                            _ = tokio::time::sleep(wait_duration) => {},
                        }
                    }
                }
            } else {
                // No keys provided (shouldn't happen, but handle it)
                tokio::time::sleep(wait_duration).await;
            }

            // Loop continues and retries
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_open_memory() {
        let db = Db::open_memory().unwrap();
        assert_eq!(db.current_db(), 0);
    }

    #[test]
    fn test_set_get() {
        let db = Db::open_memory().unwrap();

        db.set("foo", b"bar", None).unwrap();
        let value = db.get("foo").unwrap();
        assert_eq!(value, Some(b"bar".to_vec()));
    }

    #[test]
    fn test_get_nonexistent() {
        let db = Db::open_memory().unwrap();
        let value = db.get("nonexistent").unwrap();
        assert_eq!(value, None);
    }

    #[test]
    fn test_set_overwrite() {
        let db = Db::open_memory().unwrap();

        db.set("key", b"value1", None).unwrap();
        db.set("key", b"value2", None).unwrap();

        let value = db.get("key").unwrap();
        assert_eq!(value, Some(b"value2".to_vec()));
    }

    #[test]
    fn test_del() {
        let db = Db::open_memory().unwrap();

        db.set("key", b"value", None).unwrap();
        let count = db.del(&["key"]).unwrap();
        assert_eq!(count, 1);

        let value = db.get("key").unwrap();
        assert_eq!(value, None);
    }

    #[test]
    fn test_del_multiple() {
        let db = Db::open_memory().unwrap();

        db.set("k1", b"v1", None).unwrap();
        db.set("k2", b"v2", None).unwrap();
        db.set("k3", b"v3", None).unwrap();

        let count = db.del(&["k1", "k2", "k4"]).unwrap();
        assert_eq!(count, 2); // k1 and k2 deleted, k4 doesn't exist

        assert_eq!(db.get("k1").unwrap(), None);
        assert_eq!(db.get("k2").unwrap(), None);
        assert_eq!(db.get("k3").unwrap(), Some(b"v3".to_vec()));
    }

    #[test]
    fn test_set_nx() {
        let db = Db::open_memory().unwrap();

        let set1 = db.set_opts("key", b"v1", SetOptions::new().nx()).unwrap();
        assert!(set1);

        let set2 = db.set_opts("key", b"v2", SetOptions::new().nx()).unwrap();
        assert!(!set2);

        assert_eq!(db.get("key").unwrap(), Some(b"v1".to_vec()));
    }

    #[test]
    fn test_set_xx() {
        let db = Db::open_memory().unwrap();

        let set1 = db.set_opts("key", b"v1", SetOptions::new().xx()).unwrap();
        assert!(!set1);

        db.set("key", b"v1", None).unwrap();

        let set2 = db.set_opts("key", b"v2", SetOptions::new().xx()).unwrap();
        assert!(set2);

        assert_eq!(db.get("key").unwrap(), Some(b"v2".to_vec()));
    }

    #[test]
    fn test_expiration() {
        let db = Db::open_memory().unwrap();

        db.set("key", b"value", Some(Duration::from_millis(50)))
            .unwrap();

        assert!(db.get("key").unwrap().is_some());

        std::thread::sleep(Duration::from_millis(100));

        assert!(db.get("key").unwrap().is_none());
    }

    #[test]
    fn test_select_db() {
        let mut db = Db::open_memory().unwrap();

        db.set("key", b"value", None).unwrap();

        db.select(1).unwrap();
        assert_eq!(db.get("key").unwrap(), None);

        db.select(0).unwrap();
        assert_eq!(db.get("key").unwrap(), Some(b"value".to_vec()));
    }

    #[test]
    fn test_binary_data() {
        let db = Db::open_memory().unwrap();

        let binary_data = vec![0u8, 1, 2, 255, 254, 253];
        db.set("binary", &binary_data, None).unwrap();

        assert_eq!(db.get("binary").unwrap(), Some(binary_data));
    }

    // --- Disk-based tests ---

    fn temp_db_path() -> String {
        use std::time::{SystemTime, UNIX_EPOCH};
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        format!("/tmp/redlite_test_{}.db", timestamp)
    }

    fn cleanup_db(path: &str) {
        let _ = std::fs::remove_file(path);
        let _ = std::fs::remove_file(format!("{}-wal", path));
        let _ = std::fs::remove_file(format!("{}-shm", path));
    }

    #[test]
    fn test_disk_open_and_persist() {
        let path = temp_db_path();

        // Create database and set a value
        {
            let db = Db::open(&path).unwrap();
            db.set("persistent_key", b"persistent_value", None).unwrap();
        }

        // Reopen and verify data persisted
        {
            let db = Db::open(&path).unwrap();
            let value = db.get("persistent_key").unwrap();
            assert_eq!(value, Some(b"persistent_value".to_vec()));
        }

        cleanup_db(&path);
    }

    #[test]
    fn test_disk_set_get() {
        let path = temp_db_path();
        let db = Db::open(&path).unwrap();

        db.set("foo", b"bar", None).unwrap();
        let value = db.get("foo").unwrap();
        assert_eq!(value, Some(b"bar".to_vec()));

        cleanup_db(&path);
    }

    #[test]
    fn test_disk_multiple_keys() {
        let path = temp_db_path();
        let db = Db::open(&path).unwrap();

        db.set("key1", b"value1", None).unwrap();
        db.set("key2", b"value2", None).unwrap();
        db.set("key3", b"value3", None).unwrap();

        assert_eq!(db.get("key1").unwrap(), Some(b"value1".to_vec()));
        assert_eq!(db.get("key2").unwrap(), Some(b"value2".to_vec()));
        assert_eq!(db.get("key3").unwrap(), Some(b"value3".to_vec()));

        cleanup_db(&path);
    }

    #[test]
    fn test_disk_expiration() {
        let path = temp_db_path();
        let db = Db::open(&path).unwrap();

        db.set("key", b"value", Some(Duration::from_millis(50)))
            .unwrap();

        assert!(db.get("key").unwrap().is_some());

        std::thread::sleep(Duration::from_millis(100));

        assert!(db.get("key").unwrap().is_none());

        cleanup_db(&path);
    }

    #[test]
    fn test_disk_del() {
        let path = temp_db_path();
        let db = Db::open(&path).unwrap();

        db.set("key", b"value", None).unwrap();
        assert_eq!(db.del(&["key"]).unwrap(), 1);
        assert_eq!(db.get("key").unwrap(), None);

        cleanup_db(&path);
    }

    #[test]
    fn test_disk_binary_data() {
        let path = temp_db_path();
        let db = Db::open(&path).unwrap();

        let binary_data = vec![0u8, 1, 2, 255, 254, 253];
        db.set("binary", &binary_data, None).unwrap();
        assert_eq!(db.get("binary").unwrap(), Some(binary_data));

        cleanup_db(&path);
    }

    #[test]
    fn test_disk_select_db() {
        let path = temp_db_path();
        let mut db = Db::open(&path).unwrap();

        db.set("key", b"value", None).unwrap();

        db.select(1).unwrap();
        assert_eq!(db.get("key").unwrap(), None);

        db.select(0).unwrap();
        assert_eq!(db.get("key").unwrap(), Some(b"value".to_vec()));

        cleanup_db(&path);
    }

    // --- Session 2: Key commands tests ---

    #[test]
    fn test_key_type() {
        let db = Db::open_memory().unwrap();

        // Non-existent key
        assert!(db.key_type("nonexistent").unwrap().is_none());

        // String type
        db.set("key", b"value", None).unwrap();
        assert_eq!(db.key_type("key").unwrap(), Some(KeyType::String));
    }

    #[test]
    fn test_key_type_expired() {
        let db = Db::open_memory().unwrap();

        db.set("key", b"value", Some(Duration::from_millis(50)))
            .unwrap();
        assert_eq!(db.key_type("key").unwrap(), Some(KeyType::String));

        std::thread::sleep(Duration::from_millis(100));
        assert!(db.key_type("key").unwrap().is_none());
    }

    #[test]
    fn test_ttl() {
        let db = Db::open_memory().unwrap();

        // Non-existent key
        assert_eq!(db.ttl("nonexistent").unwrap(), -2);

        // Key with no expiry
        db.set("key", b"value", None).unwrap();
        assert_eq!(db.ttl("key").unwrap(), -1);

        // Key with expiry
        db.set("expiring", b"value", Some(Duration::from_secs(10)))
            .unwrap();
        let ttl = db.ttl("expiring").unwrap();
        assert!(ttl >= 9 && ttl <= 10);
    }

    #[test]
    fn test_pttl() {
        let db = Db::open_memory().unwrap();

        // Non-existent key
        assert_eq!(db.pttl("nonexistent").unwrap(), -2);

        // Key with no expiry
        db.set("key", b"value", None).unwrap();
        assert_eq!(db.pttl("key").unwrap(), -1);

        // Key with expiry
        db.set("expiring", b"value", Some(Duration::from_secs(10)))
            .unwrap();
        let pttl = db.pttl("expiring").unwrap();
        assert!(pttl >= 9900 && pttl <= 10000);
    }

    #[test]
    fn test_exists() {
        let db = Db::open_memory().unwrap();

        // Non-existent key
        assert_eq!(db.exists(&["nonexistent"]).unwrap(), 0);

        db.set("k1", b"v1", None).unwrap();
        db.set("k2", b"v2", None).unwrap();

        assert_eq!(db.exists(&["k1"]).unwrap(), 1);
        assert_eq!(db.exists(&["k1", "k2"]).unwrap(), 2);
        assert_eq!(db.exists(&["k1", "k2", "k3"]).unwrap(), 2);
        // Duplicates count separately
        assert_eq!(db.exists(&["k1", "k1"]).unwrap(), 2);
    }

    #[test]
    fn test_exists_expired() {
        let db = Db::open_memory().unwrap();

        db.set("key", b"value", Some(Duration::from_millis(50)))
            .unwrap();
        assert_eq!(db.exists(&["key"]).unwrap(), 1);

        std::thread::sleep(Duration::from_millis(100));
        assert_eq!(db.exists(&["key"]).unwrap(), 0);
    }

    #[test]
    fn test_expire() {
        let db = Db::open_memory().unwrap();

        // Non-existent key
        assert!(!db.expire("nonexistent", 10).unwrap());

        // Existing key
        db.set("key", b"value", None).unwrap();
        assert!(db.expire("key", 1).unwrap());

        // Verify expiration works
        std::thread::sleep(Duration::from_millis(1100));
        assert!(db.get("key").unwrap().is_none());
    }

    #[test]
    fn test_expire_negative() {
        let db = Db::open_memory().unwrap();

        db.set("key", b"value", None).unwrap();
        assert!(db.expire("key", -1).unwrap());
        // Key should be immediately expired
        assert!(db.get("key").unwrap().is_none());
    }

    #[test]
    fn test_keys() {
        let db = Db::open_memory().unwrap();

        db.set("foo", b"1", None).unwrap();
        db.set("foobar", b"2", None).unwrap();
        db.set("bar", b"3", None).unwrap();

        let all = db.keys("*").unwrap();
        assert_eq!(all.len(), 3);

        let foo_keys = db.keys("foo*").unwrap();
        assert_eq!(foo_keys.len(), 2);
        assert!(foo_keys.contains(&"foo".to_string()));
        assert!(foo_keys.contains(&"foobar".to_string()));

        let bar_keys = db.keys("*bar").unwrap();
        assert_eq!(bar_keys.len(), 2);
        assert!(bar_keys.contains(&"bar".to_string()));
        assert!(bar_keys.contains(&"foobar".to_string()));
    }

    #[test]
    fn test_keys_expired() {
        let db = Db::open_memory().unwrap();

        db.set("key", b"value", Some(Duration::from_millis(50)))
            .unwrap();

        assert_eq!(db.keys("*").unwrap().len(), 1);
        std::thread::sleep(Duration::from_millis(100));
        assert_eq!(db.keys("*").unwrap().len(), 0);
    }

    #[test]
    fn test_scan_basic() {
        let db = Db::open_memory().unwrap();

        for i in 0..25 {
            db.set(&format!("key{:02}", i), b"value", None).unwrap();
        }

        // First scan
        let (cursor, keys) = db.scan(0, None, 10).unwrap();
        assert_eq!(keys.len(), 10);
        assert!(cursor > 0);

        // Continue scanning
        let (cursor2, keys2) = db.scan(cursor, None, 10).unwrap();
        assert_eq!(keys2.len(), 10);

        // Final scan
        let (cursor3, keys3) = db.scan(cursor2, None, 10).unwrap();
        assert_eq!(keys3.len(), 5);
        assert_eq!(cursor3, 0); // Done
    }

    #[test]
    fn test_scan_match() {
        let db = Db::open_memory().unwrap();

        db.set("user:1", b"v", None).unwrap();
        db.set("user:2", b"v", None).unwrap();
        db.set("other:1", b"v", None).unwrap();

        let (_, keys) = db.scan(0, Some("user:*"), 100).unwrap();
        assert_eq!(keys.len(), 2);
        assert!(keys.contains(&"user:1".to_string()));
        assert!(keys.contains(&"user:2".to_string()));
    }

    #[test]
    fn test_scan_empty() {
        let db = Db::open_memory().unwrap();

        let (cursor, keys) = db.scan(0, None, 10).unwrap();
        assert_eq!(cursor, 0);
        assert!(keys.is_empty());
    }

    // --- Session 3: String operations tests ---

    #[test]
    fn test_incr() {
        let db = Db::open_memory().unwrap();

        // Increment non-existent key (starts at 0)
        assert_eq!(db.incr("counter").unwrap(), 1);
        assert_eq!(db.incr("counter").unwrap(), 2);
        assert_eq!(db.incr("counter").unwrap(), 3);

        // Increment existing integer
        db.set("num", b"10", None).unwrap();
        assert_eq!(db.incr("num").unwrap(), 11);
    }

    #[test]
    fn test_incr_not_integer() {
        let db = Db::open_memory().unwrap();

        db.set("str", b"hello", None).unwrap();
        assert!(db.incr("str").is_err());
    }

    #[test]
    fn test_decr() {
        let db = Db::open_memory().unwrap();

        // Decrement non-existent key (starts at 0)
        assert_eq!(db.decr("counter").unwrap(), -1);
        assert_eq!(db.decr("counter").unwrap(), -2);

        // Decrement existing integer
        db.set("num", b"10", None).unwrap();
        assert_eq!(db.decr("num").unwrap(), 9);
    }

    #[test]
    fn test_incrby() {
        let db = Db::open_memory().unwrap();

        assert_eq!(db.incrby("counter", 5).unwrap(), 5);
        assert_eq!(db.incrby("counter", 10).unwrap(), 15);
        assert_eq!(db.incrby("counter", -3).unwrap(), 12);
    }

    #[test]
    fn test_decrby() {
        let db = Db::open_memory().unwrap();

        db.set("num", b"100", None).unwrap();
        assert_eq!(db.decrby("num", 30).unwrap(), 70);
        assert_eq!(db.decrby("num", 80).unwrap(), -10);
    }

    #[test]
    fn test_incrbyfloat() {
        let db = Db::open_memory().unwrap();

        // Start from 0
        let result = db.incrbyfloat("float", 0.1).unwrap();
        assert!(result.starts_with("0.1"));

        // Increment existing float
        db.set("pi", b"3.14", None).unwrap();
        let result = db.incrbyfloat("pi", 0.01).unwrap();
        assert!(result.starts_with("3.15"));
    }

    #[test]
    fn test_mget() {
        let db = Db::open_memory().unwrap();

        db.set("a", b"1", None).unwrap();
        db.set("b", b"2", None).unwrap();
        db.set("c", b"3", None).unwrap();

        let values = db.mget(&["a", "b", "c", "d"]);
        assert_eq!(values.len(), 4);
        assert_eq!(values[0], Some(b"1".to_vec()));
        assert_eq!(values[1], Some(b"2".to_vec()));
        assert_eq!(values[2], Some(b"3".to_vec()));
        assert_eq!(values[3], None);
    }

    #[test]
    fn test_mset() {
        let db = Db::open_memory().unwrap();

        db.mset(&[("a", b"1"), ("b", b"2"), ("c", b"3")]).unwrap();

        assert_eq!(db.get("a").unwrap(), Some(b"1".to_vec()));
        assert_eq!(db.get("b").unwrap(), Some(b"2".to_vec()));
        assert_eq!(db.get("c").unwrap(), Some(b"3".to_vec()));
    }

    #[test]
    fn test_append() {
        let db = Db::open_memory().unwrap();

        // Append to non-existent key (creates it)
        assert_eq!(db.append("msg", b"Hello").unwrap(), 5);
        assert_eq!(db.get("msg").unwrap(), Some(b"Hello".to_vec()));

        // Append to existing key
        assert_eq!(db.append("msg", b" World").unwrap(), 11);
        assert_eq!(db.get("msg").unwrap(), Some(b"Hello World".to_vec()));
    }

    #[test]
    fn test_strlen() {
        let db = Db::open_memory().unwrap();

        // Non-existent key
        assert_eq!(db.strlen("nonexistent").unwrap(), 0);

        // Existing key
        db.set("msg", b"Hello", None).unwrap();
        assert_eq!(db.strlen("msg").unwrap(), 5);

        // Empty string
        db.set("empty", b"", None).unwrap();
        assert_eq!(db.strlen("empty").unwrap(), 0);
    }

    #[test]
    fn test_getrange() {
        let db = Db::open_memory().unwrap();

        db.set("msg", b"Hello World", None).unwrap();

        // Normal range
        assert_eq!(db.getrange("msg", 0, 4).unwrap(), b"Hello".to_vec());

        // Negative indices
        assert_eq!(db.getrange("msg", -5, -1).unwrap(), b"World".to_vec());

        // Out of range
        assert_eq!(db.getrange("msg", 0, 100).unwrap(), b"Hello World".to_vec());

        // Non-existent key
        assert_eq!(db.getrange("nonexistent", 0, 10).unwrap(), Vec::<u8>::new());
    }

    #[test]
    fn test_setrange() {
        let db = Db::open_memory().unwrap();

        // Set on non-existent key (pads with zeros)
        assert_eq!(db.setrange("key", 5, b"Hello").unwrap(), 10);
        let value = db.get("key").unwrap().unwrap();
        assert_eq!(value.len(), 10);
        assert_eq!(&value[5..], b"Hello");

        // Overwrite existing
        db.set("msg", b"Hello World", None).unwrap();
        assert_eq!(db.setrange("msg", 6, b"Redis").unwrap(), 11);
        assert_eq!(db.get("msg").unwrap(), Some(b"Hello Redis".to_vec()));

        // Extend existing
        assert_eq!(db.setrange("msg", 11, b"!!!").unwrap(), 14);
        assert_eq!(db.get("msg").unwrap(), Some(b"Hello Redis!!!".to_vec()));
    }

    #[test]
    fn test_setrange_negative_offset() {
        let db = Db::open_memory().unwrap();

        assert!(db.setrange("key", -1, b"value").is_err());
    }

    // --- Session 3: Disk tests for string operations ---

    #[test]
    fn test_disk_incr_decr() {
        let path = temp_db_path();
        let db = Db::open(&path).unwrap();

        // INCR on non-existent creates with value 1
        assert_eq!(db.incr("counter").unwrap(), 1);
        assert_eq!(db.incr("counter").unwrap(), 2);

        // DECR
        assert_eq!(db.decr("counter").unwrap(), 1);
        assert_eq!(db.decr("counter").unwrap(), 0);
        assert_eq!(db.decr("counter").unwrap(), -1);

        cleanup_db(&path);
    }

    #[test]
    fn test_disk_incrby() {
        let path = temp_db_path();
        let db = Db::open(&path).unwrap();

        db.set("counter", b"100", None).unwrap();

        assert_eq!(db.incrby("counter", 50).unwrap(), 150);
        assert_eq!(db.decrby("counter", 30).unwrap(), 120);
        assert_eq!(db.incrby("counter", -20).unwrap(), 100);

        cleanup_db(&path);
    }

    #[test]
    fn test_disk_incrbyfloat() {
        let path = temp_db_path();
        let db = Db::open(&path).unwrap();

        db.set("pi", b"3.14", None).unwrap();

        let result: f64 = db.incrbyfloat("pi", 0.01).unwrap().parse().unwrap();
        assert!((result - 3.15).abs() < 0.001);

        // Negative increment
        let result: f64 = db.incrbyfloat("pi", -0.15).unwrap().parse().unwrap();
        assert!((result - 3.0).abs() < 0.001);

        cleanup_db(&path);
    }

    #[test]
    fn test_disk_mget_mset() {
        let path = temp_db_path();
        let db = Db::open(&path).unwrap();

        // MSET
        db.mset(&[("a", b"1".as_slice()), ("b", b"2"), ("c", b"3")])
            .unwrap();

        // MGET
        let results = db.mget(&["a", "b", "c", "d"]);
        assert_eq!(results[0], Some(b"1".to_vec()));
        assert_eq!(results[1], Some(b"2".to_vec()));
        assert_eq!(results[2], Some(b"3".to_vec()));
        assert_eq!(results[3], None);

        cleanup_db(&path);
    }

    #[test]
    fn test_disk_append_strlen() {
        let path = temp_db_path();
        let db = Db::open(&path).unwrap();

        // APPEND to non-existent creates key
        assert_eq!(db.append("msg", b"Hello").unwrap(), 5);
        assert_eq!(db.append("msg", b" World").unwrap(), 11);
        assert_eq!(db.get("msg").unwrap(), Some(b"Hello World".to_vec()));

        // STRLEN
        assert_eq!(db.strlen("msg").unwrap(), 11);
        assert_eq!(db.strlen("nonexistent").unwrap(), 0);

        cleanup_db(&path);
    }

    #[test]
    fn test_disk_getrange_setrange() {
        let path = temp_db_path();
        let db = Db::open(&path).unwrap();

        db.set("msg", b"Hello World", None).unwrap();

        // GETRANGE
        assert_eq!(db.getrange("msg", 0, 4).unwrap(), b"Hello".to_vec());
        assert_eq!(db.getrange("msg", -5, -1).unwrap(), b"World".to_vec());

        // SETRANGE
        assert_eq!(db.setrange("msg", 6, b"Redis").unwrap(), 11);
        assert_eq!(db.get("msg").unwrap(), Some(b"Hello Redis".to_vec()));

        cleanup_db(&path);
    }

    #[test]
    fn test_string_wrong_type() {
        let db = Db::open_memory().unwrap();

        // Create a hash key
        db.hset("myhash", &[("field", b"value".as_slice())]).unwrap();

        // String operations on hash should fail with WrongType
        assert!(matches!(db.get("myhash"), Err(KvError::WrongType)));
        assert!(matches!(db.incr("myhash"), Err(KvError::WrongType)));
        assert!(matches!(db.incrby("myhash", 5), Err(KvError::WrongType)));
        assert!(matches!(db.incrbyfloat("myhash", 1.5), Err(KvError::WrongType)));
        assert!(matches!(db.append("myhash", b"test"), Err(KvError::WrongType)));

        // Create a list key
        db.lpush("mylist", &[b"a"]).unwrap();

        // String operations on list should fail with WrongType
        assert!(matches!(db.get("mylist"), Err(KvError::WrongType)));
        assert!(matches!(db.incr("mylist"), Err(KvError::WrongType)));
        assert!(matches!(db.append("mylist", b"test"), Err(KvError::WrongType)));

        // Create a set key
        db.sadd("myset", &[b"member"]).unwrap();

        // String operations on set should fail with WrongType
        assert!(matches!(db.get("myset"), Err(KvError::WrongType)));
        assert!(matches!(db.incr("myset"), Err(KvError::WrongType)));
    }

    // --- Session 6: Hash operations tests ---

    #[test]
    fn test_hset_hget() {
        let db = Db::open_memory().unwrap();

        // HSET creates new fields
        let count = db.hset("myhash", &[("field1", b"value1".as_slice())]).unwrap();
        assert_eq!(count, 1);

        // HGET retrieves field
        let value = db.hget("myhash", "field1").unwrap();
        assert_eq!(value, Some(b"value1".to_vec()));

        // HGET non-existent field
        let value = db.hget("myhash", "nonexistent").unwrap();
        assert_eq!(value, None);

        // HGET non-existent key
        let value = db.hget("nonexistent", "field1").unwrap();
        assert_eq!(value, None);
    }

    #[test]
    fn test_hset_multiple() {
        let db = Db::open_memory().unwrap();

        // Set multiple fields at once
        let count = db.hset("myhash", &[
            ("f1", b"v1".as_slice()),
            ("f2", b"v2"),
            ("f3", b"v3"),
        ]).unwrap();
        assert_eq!(count, 3);

        assert_eq!(db.hget("myhash", "f1").unwrap(), Some(b"v1".to_vec()));
        assert_eq!(db.hget("myhash", "f2").unwrap(), Some(b"v2".to_vec()));
        assert_eq!(db.hget("myhash", "f3").unwrap(), Some(b"v3".to_vec()));
    }

    #[test]
    fn test_hset_update() {
        let db = Db::open_memory().unwrap();

        // Set initial value
        let count1 = db.hset("myhash", &[("field", b"value1".as_slice())]).unwrap();
        assert_eq!(count1, 1);

        // Update existing field (returns 0 new fields)
        let count2 = db.hset("myhash", &[("field", b"value2".as_slice())]).unwrap();
        assert_eq!(count2, 0);

        assert_eq!(db.hget("myhash", "field").unwrap(), Some(b"value2".to_vec()));
    }

    #[test]
    fn test_hmget() {
        let db = Db::open_memory().unwrap();

        db.hset("myhash", &[
            ("f1", b"v1".as_slice()),
            ("f2", b"v2"),
        ]).unwrap();

        let values = db.hmget("myhash", &["f1", "f2", "f3"]).unwrap();
        assert_eq!(values.len(), 3);
        assert_eq!(values[0], Some(b"v1".to_vec()));
        assert_eq!(values[1], Some(b"v2".to_vec()));
        assert_eq!(values[2], None);
    }

    #[test]
    fn test_hmget_nonexistent_key() {
        let db = Db::open_memory().unwrap();

        let values = db.hmget("nonexistent", &["f1", "f2"]).unwrap();
        assert_eq!(values.len(), 2);
        assert_eq!(values[0], None);
        assert_eq!(values[1], None);
    }

    #[test]
    fn test_hgetall() {
        let db = Db::open_memory().unwrap();

        db.hset("myhash", &[
            ("f1", b"v1".as_slice()),
            ("f2", b"v2"),
        ]).unwrap();

        let all = db.hgetall("myhash").unwrap();
        assert_eq!(all.len(), 2);

        // Convert to hashmap for easier checking
        let map: std::collections::HashMap<_, _> = all.into_iter().collect();
        assert_eq!(map.get("f1"), Some(&b"v1".to_vec()));
        assert_eq!(map.get("f2"), Some(&b"v2".to_vec()));
    }

    #[test]
    fn test_hgetall_nonexistent() {
        let db = Db::open_memory().unwrap();

        let all = db.hgetall("nonexistent").unwrap();
        assert!(all.is_empty());
    }

    #[test]
    fn test_hdel() {
        let db = Db::open_memory().unwrap();

        db.hset("myhash", &[
            ("f1", b"v1".as_slice()),
            ("f2", b"v2"),
            ("f3", b"v3"),
        ]).unwrap();

        // Delete one field
        let count = db.hdel("myhash", &["f1"]).unwrap();
        assert_eq!(count, 1);
        assert_eq!(db.hget("myhash", "f1").unwrap(), None);

        // Delete multiple fields (including non-existent)
        let count = db.hdel("myhash", &["f2", "f3", "f4"]).unwrap();
        assert_eq!(count, 2);
    }

    #[test]
    fn test_hdel_removes_empty_key() {
        let db = Db::open_memory().unwrap();

        db.hset("myhash", &[("field", b"value".as_slice())]).unwrap();

        // Verify key exists
        assert_eq!(db.key_type("myhash").unwrap(), Some(KeyType::Hash));

        // Delete the only field
        db.hdel("myhash", &["field"]).unwrap();

        // Key should be removed
        assert_eq!(db.key_type("myhash").unwrap(), None);
    }

    #[test]
    fn test_hdel_nonexistent() {
        let db = Db::open_memory().unwrap();

        let count = db.hdel("nonexistent", &["field"]).unwrap();
        assert_eq!(count, 0);
    }

    #[test]
    fn test_hexists() {
        let db = Db::open_memory().unwrap();

        db.hset("myhash", &[("field", b"value".as_slice())]).unwrap();

        assert!(db.hexists("myhash", "field").unwrap());
        assert!(!db.hexists("myhash", "nonexistent").unwrap());
        assert!(!db.hexists("nonexistent", "field").unwrap());
    }

    #[test]
    fn test_hkeys() {
        let db = Db::open_memory().unwrap();

        db.hset("myhash", &[
            ("f1", b"v1".as_slice()),
            ("f2", b"v2"),
            ("f3", b"v3"),
        ]).unwrap();

        let keys = db.hkeys("myhash").unwrap();
        assert_eq!(keys.len(), 3);
        assert!(keys.contains(&"f1".to_string()));
        assert!(keys.contains(&"f2".to_string()));
        assert!(keys.contains(&"f3".to_string()));
    }

    #[test]
    fn test_hkeys_nonexistent() {
        let db = Db::open_memory().unwrap();

        let keys = db.hkeys("nonexistent").unwrap();
        assert!(keys.is_empty());
    }

    #[test]
    fn test_hvals() {
        let db = Db::open_memory().unwrap();

        db.hset("myhash", &[
            ("f1", b"v1".as_slice()),
            ("f2", b"v2"),
        ]).unwrap();

        let vals = db.hvals("myhash").unwrap();
        assert_eq!(vals.len(), 2);
        assert!(vals.contains(&b"v1".to_vec()));
        assert!(vals.contains(&b"v2".to_vec()));
    }

    #[test]
    fn test_hvals_nonexistent() {
        let db = Db::open_memory().unwrap();

        let vals = db.hvals("nonexistent").unwrap();
        assert!(vals.is_empty());
    }

    #[test]
    fn test_hlen() {
        let db = Db::open_memory().unwrap();

        assert_eq!(db.hlen("nonexistent").unwrap(), 0);

        db.hset("myhash", &[("f1", b"v1".as_slice())]).unwrap();
        assert_eq!(db.hlen("myhash").unwrap(), 1);

        db.hset("myhash", &[("f2", b"v2".as_slice()), ("f3", b"v3")]).unwrap();
        assert_eq!(db.hlen("myhash").unwrap(), 3);
    }

    #[test]
    fn test_hincrby() {
        let db = Db::open_memory().unwrap();

        // HINCRBY on non-existent field starts at 0
        assert_eq!(db.hincrby("myhash", "counter", 5).unwrap(), 5);
        assert_eq!(db.hincrby("myhash", "counter", 10).unwrap(), 15);
        assert_eq!(db.hincrby("myhash", "counter", -3).unwrap(), 12);

        // HINCRBY on existing integer
        db.hset("myhash", &[("num", b"100".as_slice())]).unwrap();
        assert_eq!(db.hincrby("myhash", "num", 50).unwrap(), 150);
    }

    #[test]
    fn test_hincrby_not_integer() {
        let db = Db::open_memory().unwrap();

        db.hset("myhash", &[("str", b"hello".as_slice())]).unwrap();
        assert!(db.hincrby("myhash", "str", 1).is_err());
    }

    #[test]
    fn test_hincrbyfloat() {
        let db = Db::open_memory().unwrap();

        // Start from 0
        let result = db.hincrbyfloat("myhash", "float", 0.1).unwrap();
        assert!(result.starts_with("0.1"));

        // Increment existing
        db.hset("myhash", &[("pi", b"3.14".as_slice())]).unwrap();
        let result = db.hincrbyfloat("myhash", "pi", 0.01).unwrap();
        assert!(result.starts_with("3.15"));
    }

    #[test]
    fn test_hincrbyfloat_not_float() {
        let db = Db::open_memory().unwrap();

        db.hset("myhash", &[("str", b"hello".as_slice())]).unwrap();
        assert!(db.hincrbyfloat("myhash", "str", 1.0).is_err());
    }

    #[test]
    fn test_hsetnx() {
        let db = Db::open_memory().unwrap();

        // First HSETNX should succeed
        assert!(db.hsetnx("myhash", "field", b"value1").unwrap());
        assert_eq!(db.hget("myhash", "field").unwrap(), Some(b"value1".to_vec()));

        // Second HSETNX should fail
        assert!(!db.hsetnx("myhash", "field", b"value2").unwrap());
        assert_eq!(db.hget("myhash", "field").unwrap(), Some(b"value1".to_vec()));
    }

    #[test]
    fn test_hash_type() {
        let db = Db::open_memory().unwrap();

        db.hset("myhash", &[("field", b"value".as_slice())]).unwrap();
        assert_eq!(db.key_type("myhash").unwrap(), Some(KeyType::Hash));
    }

    #[test]
    fn test_hash_wrong_type() {
        let db = Db::open_memory().unwrap();

        // Create a string key
        db.set("mystring", b"value", None).unwrap();

        // Try hash operations on string key - should fail
        assert!(db.hset("mystring", &[("field", b"value".as_slice())]).is_err());
        assert!(db.hget("mystring", "field").is_err());
        assert!(db.hdel("mystring", &["field"]).is_err());
    }

    // --- Session 6: Disk tests for hash operations ---

    #[test]
    fn test_disk_hset_hget() {
        let path = temp_db_path();
        let db = Db::open(&path).unwrap();

        db.hset("myhash", &[
            ("f1", b"v1".as_slice()),
            ("f2", b"v2"),
        ]).unwrap();

        assert_eq!(db.hget("myhash", "f1").unwrap(), Some(b"v1".to_vec()));
        assert_eq!(db.hget("myhash", "f2").unwrap(), Some(b"v2".to_vec()));
        assert_eq!(db.hget("myhash", "f3").unwrap(), None);

        cleanup_db(&path);
    }

    #[test]
    fn test_disk_hmget_hgetall() {
        let path = temp_db_path();
        let db = Db::open(&path).unwrap();

        db.hset("myhash", &[
            ("f1", b"v1".as_slice()),
            ("f2", b"v2"),
            ("f3", b"v3"),
        ]).unwrap();

        // HMGET
        let values = db.hmget("myhash", &["f1", "f3", "f4"]).unwrap();
        assert_eq!(values[0], Some(b"v1".to_vec()));
        assert_eq!(values[1], Some(b"v3".to_vec()));
        assert_eq!(values[2], None);

        // HGETALL
        let all = db.hgetall("myhash").unwrap();
        assert_eq!(all.len(), 3);

        cleanup_db(&path);
    }

    #[test]
    fn test_disk_hdel_hexists() {
        let path = temp_db_path();
        let db = Db::open(&path).unwrap();

        db.hset("myhash", &[
            ("f1", b"v1".as_slice()),
            ("f2", b"v2"),
        ]).unwrap();

        assert!(db.hexists("myhash", "f1").unwrap());
        assert!(!db.hexists("myhash", "f3").unwrap());

        let count = db.hdel("myhash", &["f1", "f3"]).unwrap();
        assert_eq!(count, 1);

        assert!(!db.hexists("myhash", "f1").unwrap());

        cleanup_db(&path);
    }

    #[test]
    fn test_disk_hkeys_hvals_hlen() {
        let path = temp_db_path();
        let db = Db::open(&path).unwrap();

        db.hset("myhash", &[
            ("f1", b"v1".as_slice()),
            ("f2", b"v2"),
        ]).unwrap();

        assert_eq!(db.hlen("myhash").unwrap(), 2);

        let keys = db.hkeys("myhash").unwrap();
        assert!(keys.contains(&"f1".to_string()));
        assert!(keys.contains(&"f2".to_string()));

        let vals = db.hvals("myhash").unwrap();
        assert!(vals.contains(&b"v1".to_vec()));
        assert!(vals.contains(&b"v2".to_vec()));

        cleanup_db(&path);
    }

    #[test]
    fn test_disk_hincrby() {
        let path = temp_db_path();
        let db = Db::open(&path).unwrap();

        assert_eq!(db.hincrby("myhash", "counter", 10).unwrap(), 10);
        assert_eq!(db.hincrby("myhash", "counter", 5).unwrap(), 15);
        assert_eq!(db.hincrby("myhash", "counter", -20).unwrap(), -5);

        cleanup_db(&path);
    }

    #[test]
    fn test_disk_hincrbyfloat() {
        let path = temp_db_path();
        let db = Db::open(&path).unwrap();

        let r1: f64 = db.hincrbyfloat("myhash", "float", 1.5).unwrap().parse().unwrap();
        assert!((r1 - 1.5).abs() < 0.001);

        let r2: f64 = db.hincrbyfloat("myhash", "float", 0.5).unwrap().parse().unwrap();
        assert!((r2 - 2.0).abs() < 0.001);

        cleanup_db(&path);
    }

    #[test]
    fn test_disk_hsetnx() {
        let path = temp_db_path();
        let db = Db::open(&path).unwrap();

        assert!(db.hsetnx("myhash", "field", b"value1").unwrap());
        assert!(!db.hsetnx("myhash", "field", b"value2").unwrap());
        assert_eq!(db.hget("myhash", "field").unwrap(), Some(b"value1".to_vec()));

        cleanup_db(&path);
    }

    // --- Session 7: List operation tests (memory mode) ---

    #[test]
    fn test_lpush_rpush() {
        let db = Db::open_memory().unwrap();

        // LPUSH creates new list
        assert_eq!(db.lpush("mylist", &[b"a"]).unwrap(), 1);
        assert_eq!(db.lpush("mylist", &[b"b"]).unwrap(), 2);
        assert_eq!(db.lpush("mylist", &[b"c", b"d"]).unwrap(), 4);

        // List should be: d, c, b, a
        let items = db.lrange("mylist", 0, -1).unwrap();
        assert_eq!(items, vec![b"d".to_vec(), b"c".to_vec(), b"b".to_vec(), b"a".to_vec()]);

        // RPUSH appends to end
        assert_eq!(db.rpush("mylist", &[b"e", b"f"]).unwrap(), 6);

        // List should be: d, c, b, a, e, f
        let items = db.lrange("mylist", 0, -1).unwrap();
        assert_eq!(items.len(), 6);
        assert_eq!(items[0], b"d".to_vec());
        assert_eq!(items[5], b"f".to_vec());
    }

    #[test]
    fn test_lpush_creates_list() {
        let db = Db::open_memory().unwrap();

        // RPUSH on non-existent key creates new list
        assert_eq!(db.rpush("newlist", &[b"first", b"second"]).unwrap(), 2);
        assert_eq!(db.llen("newlist").unwrap(), 2);
    }

    #[test]
    fn test_lpop_rpop() {
        let db = Db::open_memory().unwrap();

        db.rpush("mylist", &[b"a", b"b", b"c", b"d"]).unwrap();

        // LPOP single element
        let popped = db.lpop("mylist", None).unwrap();
        assert_eq!(popped, vec![b"a".to_vec()]);
        assert_eq!(db.llen("mylist").unwrap(), 3);

        // RPOP single element
        let popped = db.rpop("mylist", None).unwrap();
        assert_eq!(popped, vec![b"d".to_vec()]);
        assert_eq!(db.llen("mylist").unwrap(), 2);

        // LPOP with count
        let popped = db.lpop("mylist", Some(2)).unwrap();
        assert_eq!(popped, vec![b"b".to_vec(), b"c".to_vec()]);
        assert_eq!(db.llen("mylist").unwrap(), 0);
    }

    #[test]
    fn test_pop_empty_list() {
        let db = Db::open_memory().unwrap();

        // Pop from non-existent list
        let popped = db.lpop("nonexistent", None).unwrap();
        assert!(popped.is_empty());

        let popped = db.rpop("nonexistent", None).unwrap();
        assert!(popped.is_empty());
    }

    #[test]
    fn test_pop_deletes_empty_list() {
        let db = Db::open_memory().unwrap();

        db.rpush("mylist", &[b"only"]).unwrap();
        db.lpop("mylist", None).unwrap();

        // Key should be deleted
        assert_eq!(db.llen("mylist").unwrap(), 0);
        assert!(db.lindex("mylist", 0).unwrap().is_none());
    }

    #[test]
    fn test_llen() {
        let db = Db::open_memory().unwrap();

        // Non-existent list
        assert_eq!(db.llen("nonexistent").unwrap(), 0);

        db.rpush("mylist", &[b"a", b"b", b"c"]).unwrap();
        assert_eq!(db.llen("mylist").unwrap(), 3);
    }

    #[test]
    fn test_lrange() {
        let db = Db::open_memory().unwrap();

        db.rpush("mylist", &[b"a", b"b", b"c", b"d", b"e"]).unwrap();

        // Full range
        let items = db.lrange("mylist", 0, -1).unwrap();
        assert_eq!(items.len(), 5);

        // Partial range
        let items = db.lrange("mylist", 1, 3).unwrap();
        assert_eq!(items, vec![b"b".to_vec(), b"c".to_vec(), b"d".to_vec()]);

        // Negative indices
        let items = db.lrange("mylist", -3, -1).unwrap();
        assert_eq!(items, vec![b"c".to_vec(), b"d".to_vec(), b"e".to_vec()]);

        // Out of bounds clamped
        let items = db.lrange("mylist", 0, 100).unwrap();
        assert_eq!(items.len(), 5);

        // Invalid range returns empty
        let items = db.lrange("mylist", 3, 1).unwrap();
        assert!(items.is_empty());
    }

    #[test]
    fn test_lindex() {
        let db = Db::open_memory().unwrap();

        db.rpush("mylist", &[b"a", b"b", b"c"]).unwrap();

        // Positive indices
        assert_eq!(db.lindex("mylist", 0).unwrap(), Some(b"a".to_vec()));
        assert_eq!(db.lindex("mylist", 1).unwrap(), Some(b"b".to_vec()));
        assert_eq!(db.lindex("mylist", 2).unwrap(), Some(b"c".to_vec()));

        // Negative indices
        assert_eq!(db.lindex("mylist", -1).unwrap(), Some(b"c".to_vec()));
        assert_eq!(db.lindex("mylist", -3).unwrap(), Some(b"a".to_vec()));

        // Out of bounds
        assert_eq!(db.lindex("mylist", 5).unwrap(), None);
        assert_eq!(db.lindex("mylist", -10).unwrap(), None);

        // Non-existent key
        assert_eq!(db.lindex("nonexistent", 0).unwrap(), None);
    }

    #[test]
    fn test_lset() {
        let db = Db::open_memory().unwrap();

        db.rpush("mylist", &[b"a", b"b", b"c"]).unwrap();

        // Set middle element
        db.lset("mylist", 1, b"B").unwrap();
        assert_eq!(db.lindex("mylist", 1).unwrap(), Some(b"B".to_vec()));

        // Set with negative index
        db.lset("mylist", -1, b"C").unwrap();
        assert_eq!(db.lindex("mylist", 2).unwrap(), Some(b"C".to_vec()));
    }

    #[test]
    fn test_lset_errors() {
        let db = Db::open_memory().unwrap();

        // LSET on non-existent key
        let result = db.lset("nonexistent", 0, b"value");
        assert!(matches!(result, Err(KvError::NoSuchKey)));

        db.rpush("mylist", &[b"a"]).unwrap();

        // LSET out of range
        let result = db.lset("mylist", 5, b"value");
        assert!(matches!(result, Err(KvError::OutOfRange)));
    }

    #[test]
    fn test_ltrim() {
        let db = Db::open_memory().unwrap();

        db.rpush("mylist", &[b"a", b"b", b"c", b"d", b"e"]).unwrap();

        // Trim to middle
        db.ltrim("mylist", 1, 3).unwrap();
        let items = db.lrange("mylist", 0, -1).unwrap();
        assert_eq!(items, vec![b"b".to_vec(), b"c".to_vec(), b"d".to_vec()]);
    }

    #[test]
    fn test_ltrim_negative_indices() {
        let db = Db::open_memory().unwrap();

        db.rpush("mylist", &[b"a", b"b", b"c", b"d", b"e"]).unwrap();

        // Trim using negative indices
        db.ltrim("mylist", 0, -2).unwrap();
        let items = db.lrange("mylist", 0, -1).unwrap();
        assert_eq!(items, vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec(), b"d".to_vec()]);
    }

    #[test]
    fn test_ltrim_deletes_key() {
        let db = Db::open_memory().unwrap();

        db.rpush("mylist", &[b"a", b"b", b"c"]).unwrap();

        // Trim to empty (invalid range)
        db.ltrim("mylist", 10, 20).unwrap();
        assert_eq!(db.llen("mylist").unwrap(), 0);
    }

    #[test]
    fn test_ltrim_nonexistent() {
        let db = Db::open_memory().unwrap();

        // LTRIM on non-existent key is OK
        db.ltrim("nonexistent", 0, 1).unwrap();
    }

    #[test]
    fn test_list_wrong_type() {
        let db = Db::open_memory().unwrap();

        // Create a string key
        db.set("mystring", b"value", None).unwrap();

        // Try list operations on string key - should fail
        assert!(matches!(db.lpush("mystring", &[b"a"]), Err(KvError::WrongType)));
        assert!(matches!(db.rpush("mystring", &[b"a"]), Err(KvError::WrongType)));
        assert!(matches!(db.lpop("mystring", None), Err(KvError::WrongType)));
        assert!(matches!(db.rpop("mystring", None), Err(KvError::WrongType)));
        assert!(matches!(db.llen("mystring"), Err(KvError::WrongType)));
        assert!(matches!(db.lrange("mystring", 0, -1), Err(KvError::WrongType)));
        assert!(matches!(db.lindex("mystring", 0), Err(KvError::WrongType)));
        assert!(matches!(db.lset("mystring", 0, b"a"), Err(KvError::WrongType)));
        assert!(matches!(db.ltrim("mystring", 0, 1), Err(KvError::WrongType)));
    }

    #[test]
    fn test_list_binary_data() {
        let db = Db::open_memory().unwrap();

        let binary = vec![0u8, 1, 2, 255, 254];
        db.rpush("mylist", &[&binary[..]]).unwrap();

        let items = db.lrange("mylist", 0, -1).unwrap();
        assert_eq!(items[0], binary);
    }

    // --- Session 7: Disk tests for list operations ---

    #[test]
    fn test_disk_lpush_rpush() {
        let path = temp_db_path();
        let db = Db::open(&path).unwrap();

        db.lpush("mylist", &[b"c", b"b", b"a"]).unwrap();
        db.rpush("mylist", &[b"d", b"e"]).unwrap();

        let items = db.lrange("mylist", 0, -1).unwrap();
        assert_eq!(items.len(), 5);
        assert_eq!(items[0], b"a".to_vec());
        assert_eq!(items[4], b"e".to_vec());

        cleanup_db(&path);
    }

    #[test]
    fn test_disk_lpop_rpop() {
        let path = temp_db_path();
        let db = Db::open(&path).unwrap();

        db.rpush("mylist", &[b"a", b"b", b"c", b"d"]).unwrap();

        let l = db.lpop("mylist", Some(2)).unwrap();
        assert_eq!(l, vec![b"a".to_vec(), b"b".to_vec()]);

        let r = db.rpop("mylist", Some(1)).unwrap();
        assert_eq!(r, vec![b"d".to_vec()]);

        assert_eq!(db.llen("mylist").unwrap(), 1);

        cleanup_db(&path);
    }

    #[test]
    fn test_disk_lrange_lindex() {
        let path = temp_db_path();
        let db = Db::open(&path).unwrap();

        db.rpush("mylist", &[b"a", b"b", b"c", b"d", b"e"]).unwrap();

        let range = db.lrange("mylist", 1, 3).unwrap();
        assert_eq!(range, vec![b"b".to_vec(), b"c".to_vec(), b"d".to_vec()]);

        assert_eq!(db.lindex("mylist", 2).unwrap(), Some(b"c".to_vec()));
        assert_eq!(db.lindex("mylist", -1).unwrap(), Some(b"e".to_vec()));

        cleanup_db(&path);
    }

    #[test]
    fn test_disk_lset() {
        let path = temp_db_path();
        let db = Db::open(&path).unwrap();

        db.rpush("mylist", &[b"a", b"b", b"c"]).unwrap();
        db.lset("mylist", 1, b"B").unwrap();

        assert_eq!(db.lindex("mylist", 1).unwrap(), Some(b"B".to_vec()));

        cleanup_db(&path);
    }

    #[test]
    fn test_disk_ltrim() {
        let path = temp_db_path();
        let db = Db::open(&path).unwrap();

        db.rpush("mylist", &[b"a", b"b", b"c", b"d", b"e"]).unwrap();
        db.ltrim("mylist", 1, -2).unwrap();

        let items = db.lrange("mylist", 0, -1).unwrap();
        assert_eq!(items, vec![b"b".to_vec(), b"c".to_vec(), b"d".to_vec()]);

        cleanup_db(&path);
    }

    #[test]
    fn test_disk_list_persistence() {
        let path = temp_db_path();

        // Create list and close
        {
            let db = Db::open(&path).unwrap();
            db.rpush("mylist", &[b"persisted"]).unwrap();
        }

        // Reopen and verify
        {
            let db = Db::open(&path).unwrap();
            let items = db.lrange("mylist", 0, -1).unwrap();
            assert_eq!(items, vec![b"persisted".to_vec()]);
        }

        cleanup_db(&path);
    }

    // --- Session 8: Set operation tests (memory mode) ---

    #[test]
    fn test_sadd_smembers() {
        let db = Db::open_memory().unwrap();

        // SADD returns count of new members
        assert_eq!(db.sadd("myset", &[b"a", b"b", b"c"]).unwrap(), 3);

        // Adding duplicates should not increase count
        assert_eq!(db.sadd("myset", &[b"a", b"d"]).unwrap(), 1);

        // Check members
        let members = db.smembers("myset").unwrap();
        assert_eq!(members.len(), 4);
        assert!(members.contains(&b"a".to_vec()));
        assert!(members.contains(&b"b".to_vec()));
        assert!(members.contains(&b"c".to_vec()));
        assert!(members.contains(&b"d".to_vec()));
    }

    #[test]
    fn test_sadd_creates_set() {
        let db = Db::open_memory().unwrap();

        db.sadd("myset", &[b"member"]).unwrap();
        assert_eq!(db.key_type("myset").unwrap(), Some(KeyType::Set));
    }

    #[test]
    fn test_srem() {
        let db = Db::open_memory().unwrap();

        db.sadd("myset", &[b"a", b"b", b"c"]).unwrap();

        // Remove one member
        assert_eq!(db.srem("myset", &[b"a"]).unwrap(), 1);
        assert_eq!(db.scard("myset").unwrap(), 2);

        // Remove multiple members (one exists, one doesn't)
        assert_eq!(db.srem("myset", &[b"b", b"nonexistent"]).unwrap(), 1);
        assert_eq!(db.scard("myset").unwrap(), 1);

        // Remove from nonexistent key
        assert_eq!(db.srem("nokey", &[b"x"]).unwrap(), 0);
    }

    #[test]
    fn test_srem_deletes_empty_set() {
        let db = Db::open_memory().unwrap();

        db.sadd("myset", &[b"only"]).unwrap();
        db.srem("myset", &[b"only"]).unwrap();

        // Key should be deleted
        assert_eq!(db.key_type("myset").unwrap(), None);
    }

    #[test]
    fn test_sismember() {
        let db = Db::open_memory().unwrap();

        db.sadd("myset", &[b"a", b"b"]).unwrap();

        assert!(db.sismember("myset", b"a").unwrap());
        assert!(db.sismember("myset", b"b").unwrap());
        assert!(!db.sismember("myset", b"c").unwrap());

        // Nonexistent key
        assert!(!db.sismember("nokey", b"x").unwrap());
    }

    #[test]
    fn test_scard() {
        let db = Db::open_memory().unwrap();

        assert_eq!(db.scard("myset").unwrap(), 0);

        db.sadd("myset", &[b"a", b"b", b"c"]).unwrap();
        assert_eq!(db.scard("myset").unwrap(), 3);

        db.sadd("myset", &[b"d"]).unwrap();
        assert_eq!(db.scard("myset").unwrap(), 4);
    }

    #[test]
    fn test_spop() {
        let db = Db::open_memory().unwrap();

        db.sadd("myset", &[b"a", b"b", b"c"]).unwrap();

        // Pop single
        let popped = db.spop("myset", None).unwrap();
        assert_eq!(popped.len(), 1);
        assert_eq!(db.scard("myset").unwrap(), 2);

        // Pop multiple
        let popped = db.spop("myset", Some(2)).unwrap();
        assert_eq!(popped.len(), 2);
        assert_eq!(db.scard("myset").unwrap(), 0);

        // Key should be deleted when empty
        assert_eq!(db.key_type("myset").unwrap(), None);
    }

    #[test]
    fn test_spop_empty() {
        let db = Db::open_memory().unwrap();

        let result = db.spop("nokey", None).unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn test_srandmember() {
        let db = Db::open_memory().unwrap();

        db.sadd("myset", &[b"a", b"b", b"c"]).unwrap();

        // Get random member without removing
        let result = db.srandmember("myset", None).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(db.scard("myset").unwrap(), 3); // Still 3 members

        // Get multiple distinct
        let result = db.srandmember("myset", Some(2)).unwrap();
        assert_eq!(result.len(), 2);

        // Positive count larger than set size
        let result = db.srandmember("myset", Some(10)).unwrap();
        assert_eq!(result.len(), 3);

        // Negative count allows duplicates
        let result = db.srandmember("myset", Some(-5)).unwrap();
        assert_eq!(result.len(), 5);
    }

    #[test]
    fn test_srandmember_empty() {
        let db = Db::open_memory().unwrap();

        let result = db.srandmember("nokey", None).unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn test_sdiff() {
        let db = Db::open_memory().unwrap();

        db.sadd("set1", &[b"a", b"b", b"c"]).unwrap();
        db.sadd("set2", &[b"b", b"c", b"d"]).unwrap();
        db.sadd("set3", &[b"c", b"e"]).unwrap();

        // Diff of set1 - set2
        let diff = db.sdiff(&["set1", "set2"]).unwrap();
        assert_eq!(diff.len(), 1);
        assert!(diff.contains(&b"a".to_vec()));

        // Diff of set1 - set2 - set3
        let diff = db.sdiff(&["set1", "set2", "set3"]).unwrap();
        assert_eq!(diff.len(), 1);
        assert!(diff.contains(&b"a".to_vec()));

        // Diff with nonexistent key
        let diff = db.sdiff(&["set1", "nokey"]).unwrap();
        assert_eq!(diff.len(), 3);
    }

    #[test]
    fn test_sinter() {
        let db = Db::open_memory().unwrap();

        db.sadd("set1", &[b"a", b"b", b"c"]).unwrap();
        db.sadd("set2", &[b"b", b"c", b"d"]).unwrap();
        db.sadd("set3", &[b"c", b"e"]).unwrap();

        // Intersection of set1 and set2
        let inter = db.sinter(&["set1", "set2"]).unwrap();
        assert_eq!(inter.len(), 2);
        assert!(inter.contains(&b"b".to_vec()));
        assert!(inter.contains(&b"c".to_vec()));

        // Intersection of all three
        let inter = db.sinter(&["set1", "set2", "set3"]).unwrap();
        assert_eq!(inter.len(), 1);
        assert!(inter.contains(&b"c".to_vec()));

        // Intersection with nonexistent key returns empty
        let inter = db.sinter(&["set1", "nokey"]).unwrap();
        assert!(inter.is_empty());
    }

    #[test]
    fn test_sunion() {
        let db = Db::open_memory().unwrap();

        db.sadd("set1", &[b"a", b"b"]).unwrap();
        db.sadd("set2", &[b"b", b"c"]).unwrap();

        let union = db.sunion(&["set1", "set2"]).unwrap();
        assert_eq!(union.len(), 3);
        assert!(union.contains(&b"a".to_vec()));
        assert!(union.contains(&b"b".to_vec()));
        assert!(union.contains(&b"c".to_vec()));

        // Union with nonexistent key
        let union = db.sunion(&["set1", "nokey"]).unwrap();
        assert_eq!(union.len(), 2);
    }

    #[test]
    fn test_set_type() {
        let db = Db::open_memory().unwrap();

        db.sadd("myset", &[b"value"]).unwrap();
        assert_eq!(db.key_type("myset").unwrap(), Some(KeyType::Set));
    }

    #[test]
    fn test_set_wrong_type() {
        let db = Db::open_memory().unwrap();

        // Create a string key
        db.set("mystring", b"value", None).unwrap();

        // Set operations on string should fail with WrongType
        assert!(matches!(
            db.sadd("mystring", &[b"a"]),
            Err(KvError::WrongType)
        ));
        assert!(matches!(db.smembers("mystring"), Err(KvError::WrongType)));
        assert!(matches!(
            db.sismember("mystring", b"a"),
            Err(KvError::WrongType)
        ));
        assert!(matches!(db.scard("mystring"), Err(KvError::WrongType)));
        assert!(matches!(db.spop("mystring", None), Err(KvError::WrongType)));
        assert!(matches!(
            db.srandmember("mystring", None),
            Err(KvError::WrongType)
        ));
    }

    #[test]
    fn test_set_binary_data() {
        let db = Db::open_memory().unwrap();

        let binary = vec![0u8, 1, 2, 255, 254, 253];
        db.sadd("myset", &[&binary]).unwrap();

        let members = db.smembers("myset").unwrap();
        assert_eq!(members.len(), 1);
        assert_eq!(members[0], binary);
    }

    // --- Session 8: Disk tests for set operations ---

    #[test]
    fn test_disk_sadd_smembers() {
        let path = temp_db_path();
        let db = Db::open(&path).unwrap();

        db.sadd("myset", &[b"a", b"b", b"c"]).unwrap();
        let members = db.smembers("myset").unwrap();
        assert_eq!(members.len(), 3);

        cleanup_db(&path);
    }

    #[test]
    fn test_disk_srem_scard() {
        let path = temp_db_path();
        let db = Db::open(&path).unwrap();

        db.sadd("myset", &[b"a", b"b", b"c"]).unwrap();
        assert_eq!(db.scard("myset").unwrap(), 3);

        db.srem("myset", &[b"a"]).unwrap();
        assert_eq!(db.scard("myset").unwrap(), 2);

        cleanup_db(&path);
    }

    #[test]
    fn test_disk_sismember() {
        let path = temp_db_path();
        let db = Db::open(&path).unwrap();

        db.sadd("myset", &[b"member"]).unwrap();
        assert!(db.sismember("myset", b"member").unwrap());
        assert!(!db.sismember("myset", b"other").unwrap());

        cleanup_db(&path);
    }

    #[test]
    fn test_disk_spop() {
        let path = temp_db_path();
        let db = Db::open(&path).unwrap();

        db.sadd("myset", &[b"a", b"b", b"c"]).unwrap();
        let popped = db.spop("myset", Some(2)).unwrap();
        assert_eq!(popped.len(), 2);
        assert_eq!(db.scard("myset").unwrap(), 1);

        cleanup_db(&path);
    }

    #[test]
    fn test_disk_srandmember() {
        let path = temp_db_path();
        let db = Db::open(&path).unwrap();

        db.sadd("myset", &[b"a", b"b", b"c"]).unwrap();
        let result = db.srandmember("myset", Some(2)).unwrap();
        assert_eq!(result.len(), 2);
        // Verify set unchanged
        assert_eq!(db.scard("myset").unwrap(), 3);

        cleanup_db(&path);
    }

    #[test]
    fn test_disk_sdiff_sinter_sunion() {
        let path = temp_db_path();
        let db = Db::open(&path).unwrap();

        db.sadd("set1", &[b"a", b"b", b"c"]).unwrap();
        db.sadd("set2", &[b"b", b"c", b"d"]).unwrap();

        let diff = db.sdiff(&["set1", "set2"]).unwrap();
        assert_eq!(diff.len(), 1);
        assert!(diff.contains(&b"a".to_vec()));

        let inter = db.sinter(&["set1", "set2"]).unwrap();
        assert_eq!(inter.len(), 2);

        let union = db.sunion(&["set1", "set2"]).unwrap();
        assert_eq!(union.len(), 4);

        cleanup_db(&path);
    }

    #[test]
    fn test_disk_set_persistence() {
        let path = temp_db_path();

        // Create set and close
        {
            let db = Db::open(&path).unwrap();
            db.sadd("myset", &[b"persisted"]).unwrap();
        }

        // Reopen and verify
        {
            let db = Db::open(&path).unwrap();
            let members = db.smembers("myset").unwrap();
            assert_eq!(members, vec![b"persisted".to_vec()]);
        }

        cleanup_db(&path);
    }

    // --- Session 9: Sorted Set operation tests (memory mode) ---

    #[test]
    fn test_zadd_zcard() {
        let db = Db::open_memory().unwrap();

        // ZADD returns count of new members
        assert_eq!(
            db.zadd(
                "myzset",
                &[
                    ZMember::new(1.0, "a"),
                    ZMember::new(2.0, "b"),
                    ZMember::new(3.0, "c"),
                ]
            )
            .unwrap(),
            3
        );

        // ZCARD returns count
        assert_eq!(db.zcard("myzset").unwrap(), 3);

        // Adding duplicate members (updates score) should not increase count
        assert_eq!(
            db.zadd("myzset", &[ZMember::new(1.5, "a"), ZMember::new(4.0, "d"),]).unwrap(),
            1
        );
        assert_eq!(db.zcard("myzset").unwrap(), 4);
    }

    #[test]
    fn test_zadd_creates_zset() {
        let db = Db::open_memory().unwrap();

        db.zadd("myzset", &[ZMember::new(1.0, "member")]).unwrap();
        assert_eq!(db.key_type("myzset").unwrap(), Some(KeyType::ZSet));
    }

    #[test]
    fn test_zrem() {
        let db = Db::open_memory().unwrap();

        db.zadd(
            "myzset",
            &[
                ZMember::new(1.0, "a"),
                ZMember::new(2.0, "b"),
                ZMember::new(3.0, "c"),
            ],
        )
        .unwrap();

        // Remove one member
        assert_eq!(db.zrem("myzset", &[b"a"]).unwrap(), 1);
        assert_eq!(db.zcard("myzset").unwrap(), 2);

        // Remove multiple members (one exists, one doesn't)
        assert_eq!(db.zrem("myzset", &[b"b", b"nonexistent"]).unwrap(), 1);
        assert_eq!(db.zcard("myzset").unwrap(), 1);

        // Remove from nonexistent key
        assert_eq!(db.zrem("nokey", &[b"x"]).unwrap(), 0);
    }

    #[test]
    fn test_zrem_deletes_empty_zset() {
        let db = Db::open_memory().unwrap();

        db.zadd("myzset", &[ZMember::new(1.0, "only")]).unwrap();
        db.zrem("myzset", &[b"only"]).unwrap();

        // Key should be deleted
        assert_eq!(db.key_type("myzset").unwrap(), None);
    }

    #[test]
    fn test_zscore() {
        let db = Db::open_memory().unwrap();

        db.zadd(
            "myzset",
            &[ZMember::new(1.5, "a"), ZMember::new(2.5, "b")],
        )
        .unwrap();

        assert_eq!(db.zscore("myzset", b"a").unwrap(), Some(1.5));
        assert_eq!(db.zscore("myzset", b"b").unwrap(), Some(2.5));
        assert_eq!(db.zscore("myzset", b"c").unwrap(), None);

        // Nonexistent key
        assert_eq!(db.zscore("nokey", b"x").unwrap(), None);
    }

    #[test]
    fn test_zrank_zrevrank() {
        let db = Db::open_memory().unwrap();

        db.zadd(
            "myzset",
            &[
                ZMember::new(1.0, "a"),
                ZMember::new(2.0, "b"),
                ZMember::new(3.0, "c"),
            ],
        )
        .unwrap();

        // ZRANK (ascending)
        assert_eq!(db.zrank("myzset", b"a").unwrap(), Some(0));
        assert_eq!(db.zrank("myzset", b"b").unwrap(), Some(1));
        assert_eq!(db.zrank("myzset", b"c").unwrap(), Some(2));
        assert_eq!(db.zrank("myzset", b"nonexistent").unwrap(), None);

        // ZREVRANK (descending)
        assert_eq!(db.zrevrank("myzset", b"a").unwrap(), Some(2));
        assert_eq!(db.zrevrank("myzset", b"b").unwrap(), Some(1));
        assert_eq!(db.zrevrank("myzset", b"c").unwrap(), Some(0));
        assert_eq!(db.zrevrank("myzset", b"nonexistent").unwrap(), None);

        // Nonexistent key
        assert_eq!(db.zrank("nokey", b"x").unwrap(), None);
        assert_eq!(db.zrevrank("nokey", b"x").unwrap(), None);
    }

    #[test]
    fn test_zrange() {
        let db = Db::open_memory().unwrap();

        db.zadd(
            "myzset",
            &[
                ZMember::new(1.0, "a"),
                ZMember::new(2.0, "b"),
                ZMember::new(3.0, "c"),
            ],
        )
        .unwrap();

        // Get all
        let members = db.zrange("myzset", 0, -1, false).unwrap();
        assert_eq!(members.len(), 3);
        assert_eq!(members[0].member, b"a");
        assert_eq!(members[1].member, b"b");
        assert_eq!(members[2].member, b"c");

        // Subset
        let members = db.zrange("myzset", 0, 1, false).unwrap();
        assert_eq!(members.len(), 2);
        assert_eq!(members[0].member, b"a");
        assert_eq!(members[1].member, b"b");

        // Negative indices
        let members = db.zrange("myzset", -2, -1, false).unwrap();
        assert_eq!(members.len(), 2);
        assert_eq!(members[0].member, b"b");
        assert_eq!(members[1].member, b"c");

        // With scores
        let members = db.zrange("myzset", 0, -1, true).unwrap();
        assert_eq!(members[0].score, 1.0);
        assert_eq!(members[1].score, 2.0);
        assert_eq!(members[2].score, 3.0);

        // Empty/nonexistent key
        let members = db.zrange("nokey", 0, -1, false).unwrap();
        assert!(members.is_empty());
    }

    #[test]
    fn test_zrevrange() {
        let db = Db::open_memory().unwrap();

        db.zadd(
            "myzset",
            &[
                ZMember::new(1.0, "a"),
                ZMember::new(2.0, "b"),
                ZMember::new(3.0, "c"),
            ],
        )
        .unwrap();

        // Get all in reverse
        let members = db.zrevrange("myzset", 0, -1, false).unwrap();
        assert_eq!(members.len(), 3);
        assert_eq!(members[0].member, b"c");
        assert_eq!(members[1].member, b"b");
        assert_eq!(members[2].member, b"a");

        // With scores
        let members = db.zrevrange("myzset", 0, 1, true).unwrap();
        assert_eq!(members.len(), 2);
        assert_eq!(members[0].member, b"c");
        assert_eq!(members[0].score, 3.0);
    }

    #[test]
    fn test_zrangebyscore() {
        let db = Db::open_memory().unwrap();

        db.zadd(
            "myzset",
            &[
                ZMember::new(1.0, "a"),
                ZMember::new(2.0, "b"),
                ZMember::new(3.0, "c"),
                ZMember::new(4.0, "d"),
            ],
        )
        .unwrap();

        // Score range
        let members = db.zrangebyscore("myzset", 2.0, 3.0, None, None).unwrap();
        assert_eq!(members.len(), 2);
        assert_eq!(members[0].member, b"b");
        assert_eq!(members[1].member, b"c");

        // With LIMIT
        let members = db.zrangebyscore("myzset", 1.0, 4.0, Some(1), Some(2)).unwrap();
        assert_eq!(members.len(), 2);
        assert_eq!(members[0].member, b"b");
        assert_eq!(members[1].member, b"c");

        // Empty range
        let members = db.zrangebyscore("myzset", 10.0, 20.0, None, None).unwrap();
        assert!(members.is_empty());
    }

    #[test]
    fn test_zcount() {
        let db = Db::open_memory().unwrap();

        db.zadd(
            "myzset",
            &[
                ZMember::new(1.0, "a"),
                ZMember::new(2.0, "b"),
                ZMember::new(3.0, "c"),
            ],
        )
        .unwrap();

        assert_eq!(db.zcount("myzset", 1.0, 3.0).unwrap(), 3);
        assert_eq!(db.zcount("myzset", 1.5, 2.5).unwrap(), 1);
        assert_eq!(db.zcount("myzset", 10.0, 20.0).unwrap(), 0);

        // Nonexistent key
        assert_eq!(db.zcount("nokey", 0.0, 100.0).unwrap(), 0);
    }

    #[test]
    fn test_zincrby() {
        let db = Db::open_memory().unwrap();

        // Create new member
        let score = db.zincrby("myzset", 5.0, b"a").unwrap();
        assert_eq!(score, 5.0);
        assert_eq!(db.zscore("myzset", b"a").unwrap(), Some(5.0));

        // Increment existing
        let score = db.zincrby("myzset", 3.5, b"a").unwrap();
        assert_eq!(score, 8.5);

        // Negative increment
        let score = db.zincrby("myzset", -2.0, b"a").unwrap();
        assert_eq!(score, 6.5);
    }

    #[test]
    fn test_zremrangebyrank() {
        let db = Db::open_memory().unwrap();

        db.zadd(
            "myzset",
            &[
                ZMember::new(1.0, "a"),
                ZMember::new(2.0, "b"),
                ZMember::new(3.0, "c"),
                ZMember::new(4.0, "d"),
            ],
        )
        .unwrap();

        // Remove first two
        assert_eq!(db.zremrangebyrank("myzset", 0, 1).unwrap(), 2);
        assert_eq!(db.zcard("myzset").unwrap(), 2);

        let members = db.zrange("myzset", 0, -1, false).unwrap();
        assert_eq!(members[0].member, b"c");
        assert_eq!(members[1].member, b"d");

        // Nonexistent key
        assert_eq!(db.zremrangebyrank("nokey", 0, 10).unwrap(), 0);
    }

    #[test]
    fn test_zremrangebyscore() {
        let db = Db::open_memory().unwrap();

        db.zadd(
            "myzset",
            &[
                ZMember::new(1.0, "a"),
                ZMember::new(2.0, "b"),
                ZMember::new(3.0, "c"),
                ZMember::new(4.0, "d"),
            ],
        )
        .unwrap();

        // Remove middle scores
        assert_eq!(db.zremrangebyscore("myzset", 2.0, 3.0).unwrap(), 2);
        assert_eq!(db.zcard("myzset").unwrap(), 2);

        let members = db.zrange("myzset", 0, -1, false).unwrap();
        assert_eq!(members[0].member, b"a");
        assert_eq!(members[1].member, b"d");

        // Nonexistent key
        assert_eq!(db.zremrangebyscore("nokey", 0.0, 100.0).unwrap(), 0);
    }

    #[test]
    fn test_zset_type() {
        let db = Db::open_memory().unwrap();

        db.zadd("myzset", &[ZMember::new(1.0, "value")]).unwrap();
        assert_eq!(db.key_type("myzset").unwrap(), Some(KeyType::ZSet));
    }

    #[test]
    fn test_zset_wrong_type() {
        let db = Db::open_memory().unwrap();

        // Create a string key
        db.set("mystring", b"value", None).unwrap();

        // Sorted set operations on string should fail with WrongType
        assert!(matches!(
            db.zadd("mystring", &[ZMember::new(1.0, "a")]),
            Err(KvError::WrongType)
        ));
        assert!(matches!(db.zcard("mystring"), Err(KvError::WrongType)));
        assert!(matches!(db.zscore("mystring", b"a"), Err(KvError::WrongType)));
        assert!(matches!(db.zrank("mystring", b"a"), Err(KvError::WrongType)));
        assert!(matches!(db.zrevrank("mystring", b"a"), Err(KvError::WrongType)));
        assert!(matches!(
            db.zrange("mystring", 0, -1, false),
            Err(KvError::WrongType)
        ));
        assert!(matches!(
            db.zrevrange("mystring", 0, -1, false),
            Err(KvError::WrongType)
        ));
        assert!(matches!(
            db.zrangebyscore("mystring", 0.0, 10.0, None, None),
            Err(KvError::WrongType)
        ));
        assert!(matches!(
            db.zcount("mystring", 0.0, 10.0),
            Err(KvError::WrongType)
        ));
        assert!(matches!(
            db.zincrby("mystring", 1.0, b"a"),
            Err(KvError::WrongType)
        ));
        assert!(matches!(
            db.zremrangebyrank("mystring", 0, -1),
            Err(KvError::WrongType)
        ));
        assert!(matches!(
            db.zremrangebyscore("mystring", 0.0, 10.0),
            Err(KvError::WrongType)
        ));
    }

    #[test]
    fn test_zset_score_ties() {
        let db = Db::open_memory().unwrap();

        // Same score, different members - should be ordered lexicographically
        db.zadd(
            "myzset",
            &[
                ZMember::new(1.0, "c"),
                ZMember::new(1.0, "a"),
                ZMember::new(1.0, "b"),
            ],
        )
        .unwrap();

        let members = db.zrange("myzset", 0, -1, false).unwrap();
        assert_eq!(members[0].member, b"a");
        assert_eq!(members[1].member, b"b");
        assert_eq!(members[2].member, b"c");
    }

    // --- Session 9: Disk tests for sorted set operations ---

    #[test]
    fn test_disk_zadd_zcard() {
        let path = temp_db_path();
        let db = Db::open(&path).unwrap();

        db.zadd(
            "myzset",
            &[
                ZMember::new(1.0, "a"),
                ZMember::new(2.0, "b"),
                ZMember::new(3.0, "c"),
            ],
        )
        .unwrap();
        assert_eq!(db.zcard("myzset").unwrap(), 3);

        cleanup_db(&path);
    }

    #[test]
    fn test_disk_zrem() {
        let path = temp_db_path();
        let db = Db::open(&path).unwrap();

        db.zadd(
            "myzset",
            &[ZMember::new(1.0, "a"), ZMember::new(2.0, "b")],
        )
        .unwrap();
        db.zrem("myzset", &[b"a"]).unwrap();
        assert_eq!(db.zcard("myzset").unwrap(), 1);

        cleanup_db(&path);
    }

    #[test]
    fn test_disk_zscore_zrank() {
        let path = temp_db_path();
        let db = Db::open(&path).unwrap();

        db.zadd(
            "myzset",
            &[
                ZMember::new(1.0, "a"),
                ZMember::new(2.0, "b"),
                ZMember::new(3.0, "c"),
            ],
        )
        .unwrap();

        assert_eq!(db.zscore("myzset", b"b").unwrap(), Some(2.0));
        assert_eq!(db.zrank("myzset", b"b").unwrap(), Some(1));
        assert_eq!(db.zrevrank("myzset", b"b").unwrap(), Some(1));

        cleanup_db(&path);
    }

    #[test]
    fn test_disk_zrange_zrevrange() {
        let path = temp_db_path();
        let db = Db::open(&path).unwrap();

        db.zadd(
            "myzset",
            &[
                ZMember::new(1.0, "a"),
                ZMember::new(2.0, "b"),
                ZMember::new(3.0, "c"),
            ],
        )
        .unwrap();

        let members = db.zrange("myzset", 0, -1, false).unwrap();
        assert_eq!(members.len(), 3);
        assert_eq!(members[0].member, b"a");

        let members = db.zrevrange("myzset", 0, 0, false).unwrap();
        assert_eq!(members.len(), 1);
        assert_eq!(members[0].member, b"c");

        cleanup_db(&path);
    }

    #[test]
    fn test_disk_zrangebyscore_zcount() {
        let path = temp_db_path();
        let db = Db::open(&path).unwrap();

        db.zadd(
            "myzset",
            &[
                ZMember::new(1.0, "a"),
                ZMember::new(2.0, "b"),
                ZMember::new(3.0, "c"),
            ],
        )
        .unwrap();

        let members = db.zrangebyscore("myzset", 1.5, 2.5, None, None).unwrap();
        assert_eq!(members.len(), 1);

        assert_eq!(db.zcount("myzset", 1.0, 3.0).unwrap(), 3);

        cleanup_db(&path);
    }

    #[test]
    fn test_disk_zincrby() {
        let path = temp_db_path();
        let db = Db::open(&path).unwrap();

        db.zincrby("myzset", 5.0, b"a").unwrap();
        db.zincrby("myzset", 3.0, b"a").unwrap();
        assert_eq!(db.zscore("myzset", b"a").unwrap(), Some(8.0));

        cleanup_db(&path);
    }

    #[test]
    fn test_disk_zremrange() {
        let path = temp_db_path();
        let db = Db::open(&path).unwrap();

        db.zadd(
            "myzset",
            &[
                ZMember::new(1.0, "a"),
                ZMember::new(2.0, "b"),
                ZMember::new(3.0, "c"),
            ],
        )
        .unwrap();

        assert_eq!(db.zremrangebyrank("myzset", 0, 0).unwrap(), 1);
        assert_eq!(db.zcard("myzset").unwrap(), 2);

        assert_eq!(db.zremrangebyscore("myzset", 3.0, 3.0).unwrap(), 1);
        assert_eq!(db.zcard("myzset").unwrap(), 1);

        cleanup_db(&path);
    }

    #[test]
    fn test_disk_zset_persistence() {
        let path = temp_db_path();

        // Create sorted set and close
        {
            let db = Db::open(&path).unwrap();
            db.zadd(
                "myzset",
                &[
                    ZMember::new(1.0, "a"),
                    ZMember::new(2.0, "b"),
                ],
            )
            .unwrap();
        }

        // Reopen and verify
        {
            let db = Db::open(&path).unwrap();
            assert_eq!(db.zcard("myzset").unwrap(), 2);
            let members = db.zrange("myzset", 0, -1, true).unwrap();
            assert_eq!(members[0].member, b"a");
            assert_eq!(members[0].score, 1.0);
            assert_eq!(members[1].member, b"b");
            assert_eq!(members[1].score, 2.0);
        }

        cleanup_db(&path);
    }

    // --- Session 10: Server Operations tests ---

    #[test]
    fn test_dbsize_empty() {
        let db = Db::open_memory().unwrap();
        assert_eq!(db.dbsize().unwrap(), 0);
    }

    #[test]
    fn test_dbsize_with_keys() {
        let db = Db::open_memory().unwrap();
        db.set("key1", b"value1", None).unwrap();
        db.set("key2", b"value2", None).unwrap();
        db.set("key3", b"value3", None).unwrap();
        assert_eq!(db.dbsize().unwrap(), 3);
    }

    #[test]
    fn test_dbsize_excludes_expired() {
        let db = Db::open_memory().unwrap();
        db.set("key1", b"value1", None).unwrap();
        db.set(
            "key2",
            b"value2",
            Some(std::time::Duration::from_millis(1)),
        )
        .unwrap();
        std::thread::sleep(std::time::Duration::from_millis(10));
        assert_eq!(db.dbsize().unwrap(), 1);
    }

    #[test]
    fn test_dbsize_multiple_types() {
        let db = Db::open_memory().unwrap();
        db.set("string", b"value", None).unwrap();
        db.hset("hash", &[("field", b"value".as_slice())]).unwrap();
        db.lpush("list", &[b"item".as_slice()]).unwrap();
        db.sadd("set", &[b"member".as_slice()]).unwrap();
        db.zadd("zset", &[ZMember::new(1.0, "member")]).unwrap();
        assert_eq!(db.dbsize().unwrap(), 5);
    }

    #[test]
    fn test_flushdb_empty() {
        let db = Db::open_memory().unwrap();
        db.flushdb().unwrap();
        assert_eq!(db.dbsize().unwrap(), 0);
    }

    #[test]
    fn test_flushdb_with_keys() {
        let db = Db::open_memory().unwrap();
        db.set("key1", b"value1", None).unwrap();
        db.set("key2", b"value2", None).unwrap();
        db.hset("hash", &[("field", b"value".as_slice())]).unwrap();
        assert_eq!(db.dbsize().unwrap(), 3);

        db.flushdb().unwrap();
        assert_eq!(db.dbsize().unwrap(), 0);
        assert!(db.get("key1").unwrap().is_none());
    }

    #[test]
    fn test_select_valid() {
        let mut db = Db::open_memory().unwrap();
        assert_eq!(db.current_db(), 0);
        db.select(5).unwrap();
        assert_eq!(db.current_db(), 5);
        db.select(15).unwrap();
        assert_eq!(db.current_db(), 15);
        db.select(0).unwrap();
        assert_eq!(db.current_db(), 0);
    }

    #[test]
    fn test_select_invalid() {
        let mut db = Db::open_memory().unwrap();
        assert!(db.select(-1).is_err());
        assert!(db.select(16).is_err());
        assert!(db.select(100).is_err());
    }

    #[test]
    fn test_select_database_isolation() {
        let mut db = Db::open_memory().unwrap();

        // Set key in db 0
        db.set("key", b"value0", None).unwrap();
        assert_eq!(db.get("key").unwrap(), Some(b"value0".to_vec()));

        // Switch to db 1 - key shouldn't exist
        db.select(1).unwrap();
        assert!(db.get("key").unwrap().is_none());

        // Set different value in db 1
        db.set("key", b"value1", None).unwrap();
        assert_eq!(db.get("key").unwrap(), Some(b"value1".to_vec()));

        // Switch back to db 0 - original value should still be there
        db.select(0).unwrap();
        assert_eq!(db.get("key").unwrap(), Some(b"value0".to_vec()));
    }

    #[test]
    fn test_flushdb_only_current_db() {
        let mut db = Db::open_memory().unwrap();

        // Set key in db 0
        db.set("key0", b"value0", None).unwrap();

        // Set key in db 1
        db.select(1).unwrap();
        db.set("key1", b"value1", None).unwrap();

        // Flush db 1
        db.flushdb().unwrap();
        assert_eq!(db.dbsize().unwrap(), 0);
        assert!(db.get("key1").unwrap().is_none());

        // db 0 should still have its key
        db.select(0).unwrap();
        assert_eq!(db.dbsize().unwrap(), 1);
        assert_eq!(db.get("key0").unwrap(), Some(b"value0".to_vec()));
    }

    #[test]
    fn test_dbsize_per_database() {
        let mut db = Db::open_memory().unwrap();

        // Add keys to db 0
        db.set("key1", b"value1", None).unwrap();
        db.set("key2", b"value2", None).unwrap();
        assert_eq!(db.dbsize().unwrap(), 2);

        // Switch to db 1 and add keys
        db.select(1).unwrap();
        assert_eq!(db.dbsize().unwrap(), 0);
        db.set("key3", b"value3", None).unwrap();
        assert_eq!(db.dbsize().unwrap(), 1);

        // Verify db 0 still has 2 keys
        db.select(0).unwrap();
        assert_eq!(db.dbsize().unwrap(), 2);
    }

    // --- Session 11: Custom Commands Tests ---

    #[test]
    fn test_vacuum_no_expired_keys() {
        let db = Db::open_memory().unwrap();
        db.set("key1", b"value1", None).unwrap();
        db.set("key2", b"value2", None).unwrap();

        let deleted = db.vacuum().unwrap();
        assert_eq!(deleted, 0);

        // Keys should still exist
        assert_eq!(db.get("key1").unwrap(), Some(b"value1".to_vec()));
        assert_eq!(db.get("key2").unwrap(), Some(b"value2".to_vec()));
    }

    #[test]
    fn test_vacuum_with_expired_keys() {
        let db = Db::open_memory().unwrap();

        // Set keys with TTL (will expire immediately with 1ms)
        db.set("expired1", b"v1", Some(Duration::from_millis(1)))
            .unwrap();
        db.set("expired2", b"v2", Some(Duration::from_millis(1)))
            .unwrap();
        db.set("permanent", b"v3", None).unwrap();

        // Wait for expiration
        std::thread::sleep(Duration::from_millis(10));

        let deleted = db.vacuum().unwrap();
        assert_eq!(deleted, 2);

        // Permanent key should still exist
        assert_eq!(db.get("permanent").unwrap(), Some(b"v3".to_vec()));
    }

    #[test]
    fn test_vacuum_across_databases() {
        let mut db = Db::open_memory().unwrap();

        // Set expired key in db 0
        db.set("expired0", b"v0", Some(Duration::from_millis(1)))
            .unwrap();

        // Set expired key in db 1
        db.select(1).unwrap();
        db.set("expired1", b"v1", Some(Duration::from_millis(1)))
            .unwrap();

        // Wait for expiration
        std::thread::sleep(Duration::from_millis(10));

        // VACUUM should delete keys from ALL databases
        let deleted = db.vacuum().unwrap();
        assert_eq!(deleted, 2);
    }

    #[test]
    fn test_keyinfo_nonexistent() {
        let db = Db::open_memory().unwrap();
        let info = db.keyinfo("nonexistent").unwrap();
        assert!(info.is_none());
    }

    #[test]
    fn test_keyinfo_string() {
        let db = Db::open_memory().unwrap();
        db.set("mykey", b"myvalue", None).unwrap();

        let info = db.keyinfo("mykey").unwrap().unwrap();
        assert_eq!(info.key_type, KeyType::String);
        assert_eq!(info.ttl, -1); // No expiry
        assert!(info.created_at > 0);
        assert!(info.updated_at > 0);
    }

    #[test]
    fn test_keyinfo_with_ttl() {
        let db = Db::open_memory().unwrap();
        db.set("mykey", b"myvalue", Some(Duration::from_secs(100)))
            .unwrap();

        let info = db.keyinfo("mykey").unwrap().unwrap();
        assert_eq!(info.key_type, KeyType::String);
        // TTL should be approximately 100 seconds (allow some tolerance)
        assert!(info.ttl >= 99 && info.ttl <= 100);
    }

    #[test]
    fn test_keyinfo_hash() {
        let db = Db::open_memory().unwrap();
        db.hset("myhash", &[("field1", b"value1".as_slice())])
            .unwrap();

        let info = db.keyinfo("myhash").unwrap().unwrap();
        assert_eq!(info.key_type, KeyType::Hash);
        assert_eq!(info.ttl, -1);
    }

    #[test]
    fn test_keyinfo_list() {
        let db = Db::open_memory().unwrap();
        db.rpush("mylist", &[b"a", b"b", b"c"]).unwrap();

        let info = db.keyinfo("mylist").unwrap().unwrap();
        assert_eq!(info.key_type, KeyType::List);
        assert_eq!(info.ttl, -1);
    }

    #[test]
    fn test_keyinfo_set() {
        let db = Db::open_memory().unwrap();
        db.sadd("myset", &[b"member1", b"member2"]).unwrap();

        let info = db.keyinfo("myset").unwrap().unwrap();
        assert_eq!(info.key_type, KeyType::Set);
        assert_eq!(info.ttl, -1);
    }

    #[test]
    fn test_keyinfo_zset() {
        let db = Db::open_memory().unwrap();
        db.zadd("myzset", &[ZMember::new(1.0, b"member1".to_vec())])
            .unwrap();

        let info = db.keyinfo("myzset").unwrap().unwrap();
        assert_eq!(info.key_type, KeyType::ZSet);
        assert_eq!(info.ttl, -1);
    }

    #[test]
    fn test_keyinfo_expired_key() {
        let db = Db::open_memory().unwrap();
        db.set("expired", b"value", Some(Duration::from_millis(1)))
            .unwrap();

        // Wait for expiration
        std::thread::sleep(Duration::from_millis(10));

        // KEYINFO should return None for expired keys (and delete them)
        let info = db.keyinfo("expired").unwrap();
        assert!(info.is_none());
    }

    // --- Session 13: Stream Tests ---

    #[test]
    fn test_xadd_auto_id() {
        let db = Db::open_memory().unwrap();

        let fields: Vec<(&[u8], &[u8])> = vec![(b"field1", b"value1")];
        let id = db
            .xadd("mystream", None, &fields, false, None, None, false)
            .unwrap()
            .unwrap();

        assert!(id.ms > 0);
        assert_eq!(id.seq, 0);
    }

    #[test]
    fn test_xadd_explicit_id() {
        let db = Db::open_memory().unwrap();

        let explicit_id = StreamId::new(1000, 5);
        let fields: Vec<(&[u8], &[u8])> = vec![(b"field1", b"value1")];
        let id = db
            .xadd("mystream", Some(explicit_id), &fields, false, None, None, false)
            .unwrap()
            .unwrap();

        assert_eq!(id.ms, 1000);
        assert_eq!(id.seq, 5);
    }

    #[test]
    fn test_xadd_nomkstream() {
        let db = Db::open_memory().unwrap();

        let fields: Vec<(&[u8], &[u8])> = vec![(b"field1", b"value1")];
        // NOMKSTREAM should return None if stream doesn't exist
        let result = db
            .xadd("nonexistent", None, &fields, true, None, None, false)
            .unwrap();

        assert!(result.is_none());
    }

    #[test]
    fn test_xadd_multiple_entries() {
        let db = Db::open_memory().unwrap();

        let fields1: Vec<(&[u8], &[u8])> = vec![(b"field1", b"value1")];
        let fields2: Vec<(&[u8], &[u8])> = vec![(b"field2", b"value2")];

        let id1 = db
            .xadd("mystream", None, &fields1, false, None, None, false)
            .unwrap()
            .unwrap();
        let id2 = db
            .xadd("mystream", None, &fields2, false, None, None, false)
            .unwrap()
            .unwrap();

        // Second ID should be greater than first
        assert!(id2 > id1);
    }

    #[test]
    fn test_xlen() {
        let db = Db::open_memory().unwrap();

        assert_eq!(db.xlen("mystream").unwrap(), 0);

        let fields: Vec<(&[u8], &[u8])> = vec![(b"f", b"v")];
        db.xadd("mystream", None, &fields, false, None, None, false)
            .unwrap();
        assert_eq!(db.xlen("mystream").unwrap(), 1);

        db.xadd("mystream", None, &fields, false, None, None, false)
            .unwrap();
        assert_eq!(db.xlen("mystream").unwrap(), 2);
    }

    #[test]
    fn test_xrange() {
        let db = Db::open_memory().unwrap();

        let id1 = StreamId::new(1000, 0);
        let id2 = StreamId::new(2000, 0);
        let id3 = StreamId::new(3000, 0);

        let fields1: Vec<(&[u8], &[u8])> = vec![(b"a", b"1")];
        let fields2: Vec<(&[u8], &[u8])> = vec![(b"b", b"2")];
        let fields3: Vec<(&[u8], &[u8])> = vec![(b"c", b"3")];

        db.xadd("s", Some(id1), &fields1, false, None, None, false).unwrap();
        db.xadd("s", Some(id2), &fields2, false, None, None, false).unwrap();
        db.xadd("s", Some(id3), &fields3, false, None, None, false).unwrap();

        // Get all entries
        let entries = db.xrange("s", StreamId::min(), StreamId::max(), None).unwrap();
        assert_eq!(entries.len(), 3);
        assert_eq!(entries[0].id, id1);
        assert_eq!(entries[1].id, id2);
        assert_eq!(entries[2].id, id3);

        // Get range
        let entries = db.xrange("s", id1, id2, None).unwrap();
        assert_eq!(entries.len(), 2);

        // Get with count
        let entries = db.xrange("s", StreamId::min(), StreamId::max(), Some(2)).unwrap();
        assert_eq!(entries.len(), 2);
    }

    #[test]
    fn test_xrevrange() {
        let db = Db::open_memory().unwrap();

        let id1 = StreamId::new(1000, 0);
        let id2 = StreamId::new(2000, 0);
        let id3 = StreamId::new(3000, 0);

        let fields: Vec<(&[u8], &[u8])> = vec![(b"f", b"v")];

        db.xadd("s", Some(id1), &fields, false, None, None, false).unwrap();
        db.xadd("s", Some(id2), &fields, false, None, None, false).unwrap();
        db.xadd("s", Some(id3), &fields, false, None, None, false).unwrap();

        // Get all entries in reverse
        let entries = db.xrevrange("s", StreamId::max(), StreamId::min(), None).unwrap();
        assert_eq!(entries.len(), 3);
        assert_eq!(entries[0].id, id3);
        assert_eq!(entries[1].id, id2);
        assert_eq!(entries[2].id, id1);
    }

    #[test]
    fn test_xread() {
        let db = Db::open_memory().unwrap();

        let id1 = StreamId::new(1000, 0);
        let id2 = StreamId::new(2000, 0);

        let fields1: Vec<(&[u8], &[u8])> = vec![(b"a", b"1")];
        let fields2: Vec<(&[u8], &[u8])> = vec![(b"b", b"2")];

        db.xadd("s", Some(id1), &fields1, false, None, None, false).unwrap();
        db.xadd("s", Some(id2), &fields2, false, None, None, false).unwrap();

        // Read from beginning
        let results = db.xread(&["s"], &[StreamId::new(0, 0)], None).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].0, "s");
        assert_eq!(results[0].1.len(), 2);

        // Read after id1
        let results = db.xread(&["s"], &[id1], None).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].1.len(), 1);
        assert_eq!(results[0].1[0].id, id2);
    }

    #[test]
    fn test_xtrim_maxlen() {
        let db = Db::open_memory().unwrap();

        let fields: Vec<(&[u8], &[u8])> = vec![(b"f", b"v")];
        for i in 1..=5 {
            db.xadd("s", Some(StreamId::new(i * 1000, 0)), &fields, false, None, None, false).unwrap();
        }

        assert_eq!(db.xlen("s").unwrap(), 5);

        // Trim to 3 entries
        let deleted = db.xtrim("s", Some(3), None, false).unwrap();
        assert_eq!(deleted, 2);
        assert_eq!(db.xlen("s").unwrap(), 3);

        // Verify oldest entries were removed
        let entries = db.xrange("s", StreamId::min(), StreamId::max(), None).unwrap();
        assert_eq!(entries[0].id.ms, 3000);
    }

    #[test]
    fn test_xtrim_minid() {
        let db = Db::open_memory().unwrap();

        let fields: Vec<(&[u8], &[u8])> = vec![(b"f", b"v")];
        for i in 1..=5 {
            db.xadd("s", Some(StreamId::new(i * 1000, 0)), &fields, false, None, None, false).unwrap();
        }

        // Trim entries before 3000-0
        let deleted = db.xtrim("s", None, Some(StreamId::new(3000, 0)), false).unwrap();
        assert_eq!(deleted, 2);
        assert_eq!(db.xlen("s").unwrap(), 3);
    }

    #[test]
    fn test_xdel() {
        let db = Db::open_memory().unwrap();

        let id1 = StreamId::new(1000, 0);
        let id2 = StreamId::new(2000, 0);
        let id3 = StreamId::new(3000, 0);

        let fields: Vec<(&[u8], &[u8])> = vec![(b"f", b"v")];
        db.xadd("s", Some(id1), &fields, false, None, None, false).unwrap();
        db.xadd("s", Some(id2), &fields, false, None, None, false).unwrap();
        db.xadd("s", Some(id3), &fields, false, None, None, false).unwrap();

        // Delete middle entry
        let deleted = db.xdel("s", &[id2]).unwrap();
        assert_eq!(deleted, 1);
        assert_eq!(db.xlen("s").unwrap(), 2);

        // Delete non-existent entry
        let deleted = db.xdel("s", &[id2]).unwrap();
        assert_eq!(deleted, 0);
    }

    #[test]
    fn test_xinfo_stream() {
        let db = Db::open_memory().unwrap();

        let id1 = StreamId::new(1000, 0);
        let id2 = StreamId::new(2000, 0);

        let fields1: Vec<(&[u8], &[u8])> = vec![(b"a", b"1")];
        let fields2: Vec<(&[u8], &[u8])> = vec![(b"b", b"2")];

        db.xadd("s", Some(id1), &fields1, false, None, None, false).unwrap();
        db.xadd("s", Some(id2), &fields2, false, None, None, false).unwrap();

        let info = db.xinfo_stream("s").unwrap().unwrap();
        assert_eq!(info.length, 2);
        assert_eq!(info.last_generated_id, id2);
        assert_eq!(info.first_entry.as_ref().unwrap().id, id1);
        assert_eq!(info.last_entry.as_ref().unwrap().id, id2);
    }

    #[test]
    fn test_xadd_maxlen() {
        let db = Db::open_memory().unwrap();

        let fields: Vec<(&[u8], &[u8])> = vec![(b"f", b"v")];

        // Add entries with MAXLEN 3
        for i in 1..=5 {
            db.xadd("s", Some(StreamId::new(i * 1000, 0)), &fields, false, Some(3), None, false).unwrap();
        }

        // Should only have 3 entries
        assert_eq!(db.xlen("s").unwrap(), 3);

        // Should have the latest 3
        let entries = db.xrange("s", StreamId::min(), StreamId::max(), None).unwrap();
        assert_eq!(entries[0].id.ms, 3000);
    }

    #[test]
    fn test_stream_type() {
        let db = Db::open_memory().unwrap();

        let fields: Vec<(&[u8], &[u8])> = vec![(b"f", b"v")];
        db.xadd("mystream", None, &fields, false, None, None, false).unwrap();

        let key_type = db.key_type("mystream").unwrap();
        assert_eq!(key_type, Some(KeyType::Stream));
    }

    #[test]
    fn test_stream_wrong_type() {
        let db = Db::open_memory().unwrap();

        // Create a string key
        db.set("mykey", b"value", None).unwrap();

        // Try to use stream operations on it
        let fields: Vec<(&[u8], &[u8])> = vec![(b"f", b"v")];
        let result = db.xadd("mykey", None, &fields, false, None, None, false);
        assert!(matches!(result, Err(KvError::WrongType)));

        let result = db.xlen("mykey");
        assert!(matches!(result, Err(KvError::WrongType)));
    }

    #[test]
    fn test_stream_id_parse() {
        // Basic ID
        let id = StreamId::parse("1234-5").unwrap();
        assert_eq!(id.ms, 1234);
        assert_eq!(id.seq, 5);

        // Just timestamp
        let id = StreamId::parse("1234").unwrap();
        assert_eq!(id.ms, 1234);
        assert_eq!(id.seq, 0);

        // Min/max
        let id = StreamId::parse("-").unwrap();
        assert_eq!(id, StreamId::min());

        let id = StreamId::parse("+").unwrap();
        assert_eq!(id, StreamId::max());

        // Special values return None
        assert!(StreamId::parse("$").is_none());
        assert!(StreamId::parse(">").is_none());
    }

    #[test]
    fn test_stream_entry_fields() {
        let db = Db::open_memory().unwrap();

        let fields: Vec<(&[u8], &[u8])> = vec![
            (b"field1", b"value1"),
            (b"field2", b"value2"),
            (b"field3", b"value3"),
        ];
        let id = StreamId::new(1000, 0);
        db.xadd("s", Some(id), &fields, false, None, None, false).unwrap();

        let entries = db.xrange("s", StreamId::min(), StreamId::max(), None).unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].fields.len(), 3);
        assert_eq!(entries[0].fields[0].0, b"field1");
        assert_eq!(entries[0].fields[0].1, b"value1");
    }

    // ==================== Consumer Group Tests (Session 14) ====================

    #[test]
    fn test_xgroup_create() {
        let db = Db::open_memory().unwrap();

        // Create stream first
        let fields: Vec<(&[u8], &[u8])> = vec![(b"f", b"v")];
        db.xadd("mystream", Some(StreamId::new(1000, 0)), &fields, false, None, None, false).unwrap();

        // Create group
        let result = db.xgroup_create("mystream", "mygroup", StreamId::new(0, 0), false);
        assert!(result.is_ok());

        // Group already exists
        let result = db.xgroup_create("mystream", "mygroup", StreamId::new(0, 0), false);
        assert!(matches!(result, Err(KvError::BusyGroup)));

        // Stream doesn't exist
        let result = db.xgroup_create("nonexistent", "mygroup", StreamId::new(0, 0), false);
        assert!(matches!(result, Err(KvError::NoSuchKey)));

        // MKSTREAM creates stream if it doesn't exist
        let result = db.xgroup_create("newstream", "mygroup", StreamId::new(0, 0), true);
        assert!(result.is_ok());
    }

    #[test]
    fn test_xgroup_destroy() {
        let db = Db::open_memory().unwrap();

        let fields: Vec<(&[u8], &[u8])> = vec![(b"f", b"v")];
        db.xadd("mystream", Some(StreamId::new(1000, 0)), &fields, false, None, None, false).unwrap();
        db.xgroup_create("mystream", "mygroup", StreamId::new(0, 0), false).unwrap();

        let result = db.xgroup_destroy("mystream", "mygroup");
        assert!(matches!(result, Ok(true)));

        // Destroying non-existent group returns false
        let result = db.xgroup_destroy("mystream", "mygroup");
        assert!(matches!(result, Ok(false)));
    }

    #[test]
    fn test_xgroup_setid() {
        let db = Db::open_memory().unwrap();

        let fields: Vec<(&[u8], &[u8])> = vec![(b"f", b"v")];
        db.xadd("mystream", Some(StreamId::new(1000, 0)), &fields, false, None, None, false).unwrap();
        db.xgroup_create("mystream", "mygroup", StreamId::new(0, 0), false).unwrap();

        let result = db.xgroup_setid("mystream", "mygroup", StreamId::new(1000, 0));
        assert!(result.is_ok());

        // Non-existent group
        let result = db.xgroup_setid("mystream", "nonexistent", StreamId::new(0, 0));
        assert!(matches!(result, Err(KvError::NoGroup)));

        // Non-existent key
        let result = db.xgroup_setid("nonexistent", "mygroup", StreamId::new(0, 0));
        assert!(matches!(result, Err(KvError::NoSuchKey)));
    }

    #[test]
    fn test_xgroup_createconsumer() {
        let db = Db::open_memory().unwrap();

        let fields: Vec<(&[u8], &[u8])> = vec![(b"f", b"v")];
        db.xadd("mystream", Some(StreamId::new(1000, 0)), &fields, false, None, None, false).unwrap();
        db.xgroup_create("mystream", "mygroup", StreamId::new(0, 0), false).unwrap();

        // Create consumer
        let result = db.xgroup_createconsumer("mystream", "mygroup", "consumer1");
        assert!(matches!(result, Ok(true)));

        // Consumer already exists
        let result = db.xgroup_createconsumer("mystream", "mygroup", "consumer1");
        assert!(matches!(result, Ok(false)));

        // Non-existent group
        let result = db.xgroup_createconsumer("mystream", "nonexistent", "consumer1");
        assert!(matches!(result, Err(KvError::NoGroup)));
    }

    #[test]
    fn test_xgroup_delconsumer() {
        let db = Db::open_memory().unwrap();

        let fields: Vec<(&[u8], &[u8])> = vec![(b"f", b"v")];
        db.xadd("mystream", Some(StreamId::new(1000, 0)), &fields, false, None, None, false).unwrap();
        db.xgroup_create("mystream", "mygroup", StreamId::new(0, 0), false).unwrap();
        db.xgroup_createconsumer("mystream", "mygroup", "consumer1").unwrap();

        // Delete consumer (returns pending count)
        let result = db.xgroup_delconsumer("mystream", "mygroup", "consumer1");
        assert!(matches!(result, Ok(0)));
    }

    #[test]
    fn test_xreadgroup_new_messages() {
        let db = Db::open_memory().unwrap();

        let fields: Vec<(&[u8], &[u8])> = vec![(b"f", b"v")];
        db.xadd("mystream", Some(StreamId::new(1000, 0)), &fields, false, None, None, false).unwrap();
        db.xadd("mystream", Some(StreamId::new(2000, 0)), &fields, false, None, None, false).unwrap();
        db.xgroup_create("mystream", "mygroup", StreamId::new(0, 0), false).unwrap();

        // Read new messages with >
        let results = db.xreadgroup("mygroup", "consumer1", &["mystream"], &[">"], None, false).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].0, "mystream");
        assert_eq!(results[0].1.len(), 2);
        assert_eq!(results[0].1[0].id, StreamId::new(1000, 0));
        assert_eq!(results[0].1[1].id, StreamId::new(2000, 0));

        // Reading again should return nothing (all delivered)
        let results = db.xreadgroup("mygroup", "consumer1", &["mystream"], &[">"], None, false).unwrap();
        assert!(results.is_empty());
    }

    #[test]
    fn test_xreadgroup_pending() {
        let db = Db::open_memory().unwrap();

        let fields: Vec<(&[u8], &[u8])> = vec![(b"f", b"v")];
        db.xadd("mystream", Some(StreamId::new(1000, 0)), &fields, false, None, None, false).unwrap();
        db.xadd("mystream", Some(StreamId::new(2000, 0)), &fields, false, None, None, false).unwrap();
        db.xgroup_create("mystream", "mygroup", StreamId::new(0, 0), false).unwrap();

        // First read creates pending entries
        db.xreadgroup("mygroup", "consumer1", &["mystream"], &[">"], None, false).unwrap();

        // Read pending entries with 0
        let results = db.xreadgroup("mygroup", "consumer1", &["mystream"], &["0"], None, false).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].1.len(), 2);
    }

    #[test]
    fn test_xreadgroup_noack() {
        let db = Db::open_memory().unwrap();

        let fields: Vec<(&[u8], &[u8])> = vec![(b"f", b"v")];
        db.xadd("mystream", Some(StreamId::new(1000, 0)), &fields, false, None, None, false).unwrap();
        db.xgroup_create("mystream", "mygroup", StreamId::new(0, 0), false).unwrap();

        // Read with NOACK - should not add to pending
        db.xreadgroup("mygroup", "consumer1", &["mystream"], &[">"], None, true).unwrap();

        // Check pending is empty
        let summary = db.xpending_summary("mystream", "mygroup").unwrap();
        assert_eq!(summary.count, 0);
    }

    #[test]
    fn test_xack() {
        let db = Db::open_memory().unwrap();

        let fields: Vec<(&[u8], &[u8])> = vec![(b"f", b"v")];
        db.xadd("mystream", Some(StreamId::new(1000, 0)), &fields, false, None, None, false).unwrap();
        db.xadd("mystream", Some(StreamId::new(2000, 0)), &fields, false, None, None, false).unwrap();
        db.xgroup_create("mystream", "mygroup", StreamId::new(0, 0), false).unwrap();

        // Read messages to create pending entries
        db.xreadgroup("mygroup", "consumer1", &["mystream"], &[">"], None, false).unwrap();

        // Acknowledge one message
        let acked = db.xack("mystream", "mygroup", &[StreamId::new(1000, 0)]).unwrap();
        assert_eq!(acked, 1);

        // Check pending
        let summary = db.xpending_summary("mystream", "mygroup").unwrap();
        assert_eq!(summary.count, 1);

        // Acknowledge already acked message returns 0
        let acked = db.xack("mystream", "mygroup", &[StreamId::new(1000, 0)]).unwrap();
        assert_eq!(acked, 0);
    }

    #[test]
    fn test_xpending_summary() {
        let db = Db::open_memory().unwrap();

        let fields: Vec<(&[u8], &[u8])> = vec![(b"f", b"v")];
        db.xadd("mystream", Some(StreamId::new(1000, 0)), &fields, false, None, None, false).unwrap();
        db.xadd("mystream", Some(StreamId::new(2000, 0)), &fields, false, None, None, false).unwrap();
        db.xgroup_create("mystream", "mygroup", StreamId::new(0, 0), false).unwrap();

        // Read messages
        db.xreadgroup("mygroup", "consumer1", &["mystream"], &[">"], None, false).unwrap();

        let summary = db.xpending_summary("mystream", "mygroup").unwrap();
        assert_eq!(summary.count, 2);
        assert_eq!(summary.smallest_id, Some(StreamId::new(1000, 0)));
        assert_eq!(summary.largest_id, Some(StreamId::new(2000, 0)));
        assert_eq!(summary.consumers.len(), 1);
        assert_eq!(summary.consumers[0].0, "consumer1");
        assert_eq!(summary.consumers[0].1, 2);
    }

    #[test]
    fn test_xpending_range() {
        let db = Db::open_memory().unwrap();

        let fields: Vec<(&[u8], &[u8])> = vec![(b"f", b"v")];
        db.xadd("mystream", Some(StreamId::new(1000, 0)), &fields, false, None, None, false).unwrap();
        db.xadd("mystream", Some(StreamId::new(2000, 0)), &fields, false, None, None, false).unwrap();
        db.xgroup_create("mystream", "mygroup", StreamId::new(0, 0), false).unwrap();

        db.xreadgroup("mygroup", "consumer1", &["mystream"], &[">"], None, false).unwrap();

        let entries = db.xpending_range("mystream", "mygroup", StreamId::min(), StreamId::max(), 10, None, None).unwrap();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].id, StreamId::new(1000, 0));
        assert_eq!(entries[0].consumer, "consumer1");
        assert_eq!(entries[0].delivery_count, 1);
    }

    #[test]
    fn test_xclaim() {
        let db = Db::open_memory().unwrap();

        let fields: Vec<(&[u8], &[u8])> = vec![(b"f", b"v")];
        db.xadd("mystream", Some(StreamId::new(1000, 0)), &fields, false, None, None, false).unwrap();
        db.xgroup_create("mystream", "mygroup", StreamId::new(0, 0), false).unwrap();

        // Read message with consumer1
        db.xreadgroup("mygroup", "consumer1", &["mystream"], &[">"], None, false).unwrap();

        // Claim with consumer2 using FORCE (no min-idle-time requirement)
        let claimed = db.xclaim(
            "mystream", "mygroup", "consumer2", 0,
            &[StreamId::new(1000, 0)],
            None, None, None, true, false
        ).unwrap();
        assert_eq!(claimed.len(), 1);
        assert_eq!(claimed[0].id, StreamId::new(1000, 0));

        // Check that pending now shows consumer2
        let entries = db.xpending_range("mystream", "mygroup", StreamId::min(), StreamId::max(), 10, None, None).unwrap();
        assert_eq!(entries[0].consumer, "consumer2");
    }

    #[test]
    fn test_xclaim_justid() {
        let db = Db::open_memory().unwrap();

        let fields: Vec<(&[u8], &[u8])> = vec![(b"f", b"v")];
        db.xadd("mystream", Some(StreamId::new(1000, 0)), &fields, false, None, None, false).unwrap();
        db.xgroup_create("mystream", "mygroup", StreamId::new(0, 0), false).unwrap();

        db.xreadgroup("mygroup", "consumer1", &["mystream"], &[">"], None, false).unwrap();

        // Claim with JUSTID - should return empty fields
        let claimed = db.xclaim(
            "mystream", "mygroup", "consumer2", 0,
            &[StreamId::new(1000, 0)],
            None, None, None, true, true
        ).unwrap();
        assert_eq!(claimed.len(), 1);
        assert_eq!(claimed[0].id, StreamId::new(1000, 0));
        assert!(claimed[0].fields.is_empty());
    }

    #[test]
    fn test_xinfo_groups() {
        let db = Db::open_memory().unwrap();

        let fields: Vec<(&[u8], &[u8])> = vec![(b"f", b"v")];
        db.xadd("mystream", Some(StreamId::new(1000, 0)), &fields, false, None, None, false).unwrap();
        db.xgroup_create("mystream", "group1", StreamId::new(0, 0), false).unwrap();
        db.xgroup_create("mystream", "group2", StreamId::new(1000, 0), false).unwrap();

        let groups = db.xinfo_groups("mystream").unwrap();
        assert_eq!(groups.len(), 2);
    }

    #[test]
    fn test_xinfo_consumers() {
        let db = Db::open_memory().unwrap();

        let fields: Vec<(&[u8], &[u8])> = vec![(b"f", b"v")];
        db.xadd("mystream", Some(StreamId::new(1000, 0)), &fields, false, None, None, false).unwrap();
        db.xgroup_create("mystream", "mygroup", StreamId::new(0, 0), false).unwrap();
        db.xgroup_createconsumer("mystream", "mygroup", "consumer1").unwrap();
        db.xgroup_createconsumer("mystream", "mygroup", "consumer2").unwrap();

        let consumers = db.xinfo_consumers("mystream", "mygroup").unwrap();
        assert_eq!(consumers.len(), 2);
    }

    #[test]
    fn test_is_server_mode_embedded() {
        // Embedded mode: no notifier attached
        let db = Db::open_memory().unwrap();
        assert!(!db.is_server_mode());
    }

    #[test]
    fn test_is_server_mode_server() {
        // Server mode: notifier attached
        let db = Db::open_memory().unwrap();
        let notifier = Arc::new(RwLock::new(HashMap::new()));
        db.with_notifier(notifier);
        assert!(db.is_server_mode());
    }

    #[tokio::test]
    async fn test_subscribe_key_embedded_mode() {
        // Embedded mode subscribe should return a receiver that's immediately closed
        // (because the sender is dropped after creating the channel)
        let db = Db::open_memory().unwrap();
        let mut rx = db.subscribe_key("mykey").await;

        // In embedded mode, recv() will return Err(Closed) immediately
        // because we don't hold any senders
        let result = rx.recv().await;
        assert!(matches!(result, Err(tokio::sync::broadcast::error::RecvError::Closed)));
    }

    #[tokio::test]
    async fn test_subscribe_key_server_mode() {
        // Server mode subscribe should create channel
        let db = Db::open_memory().unwrap();
        let notifier = Arc::new(RwLock::new(HashMap::new()));
        db.with_notifier(notifier.clone());

        let mut rx = db.subscribe_key("mykey").await;

        // Verify channel was created in notifier map
        {
            let map = notifier.read().unwrap();
            assert!(map.contains_key("mykey"));
        }

        // Notify and verify receiver fires
        db.notify_key("mykey").await.unwrap();
        tokio::select! {
            result = rx.recv() => {
                assert!(result.is_ok());
            }
            _ = tokio::time::sleep(std::time::Duration::from_millis(100)) => {
                panic!("Receiver should have fired after notification");
            }
        }
    }

    #[tokio::test]
    async fn test_notify_key_creates_channel() {
        // Notifying a key that doesn't have a channel yet
        let db = Db::open_memory().unwrap();
        let notifier = Arc::new(RwLock::new(HashMap::new()));
        db.with_notifier(notifier.clone());

        // Subscribe to create channel
        let mut rx = db.subscribe_key("key1").await;

        // Notify should send to the channel
        db.notify_key("key1").await.unwrap();

        tokio::select! {
            result = rx.recv() => {
                assert!(result.is_ok());
            }
            _ = tokio::time::sleep(std::time::Duration::from_millis(100)) => {
                panic!("Receiver should have fired");
            }
        }
    }

    #[tokio::test]
    async fn test_multiple_subscribers_same_key() {
        // Multiple subscribers should all receive notifications
        let db = Db::open_memory().unwrap();
        let notifier = Arc::new(RwLock::new(HashMap::new()));
        db.with_notifier(notifier);

        let mut rx1 = db.subscribe_key("shared").await;
        let mut rx2 = db.subscribe_key("shared").await;

        // Send notification
        db.notify_key("shared").await.unwrap();

        // Both should receive
        tokio::select! {
            r1 = rx1.recv() => {
                assert!(r1.is_ok());
            }
            _ = tokio::time::sleep(std::time::Duration::from_millis(100)) => {
                panic!("rx1 should have fired");
            }
        }

        tokio::select! {
            r2 = rx2.recv() => {
                assert!(r2.is_ok());
            }
            _ = tokio::time::sleep(std::time::Duration::from_millis(100)) => {
                panic!("rx2 should have fired");
            }
        }
    }

    #[tokio::test]
    async fn test_different_keys_isolated() {
        // Notifying one key shouldn't affect others
        let db = Db::open_memory().unwrap();
        let notifier = Arc::new(RwLock::new(HashMap::new()));
        db.with_notifier(notifier);

        let mut rx_key1 = db.subscribe_key("key1").await;
        let mut rx_key2 = db.subscribe_key("key2").await;

        // Notify only key1
        db.notify_key("key1").await.unwrap();

        // key1 should fire
        tokio::select! {
            result = rx_key1.recv() => {
                assert!(result.is_ok());
            }
            _ = tokio::time::sleep(std::time::Duration::from_millis(50)) => {
                panic!("key1 should have fired");
            }
        }

        // key2 should NOT fire
        tokio::select! {
            _result = rx_key2.recv() => {
                panic!("key2 should not have fired");
            }
            _ = tokio::time::sleep(std::time::Duration::from_millis(50)) => {
                // Expected - key2 doesn't fire
            }
        }
    }

    #[tokio::test]
    async fn test_lpush_broadcasts_in_server_mode() {
        // LPUSH should send notification after successful insert
        let db = Db::open_memory().unwrap();
        let notifier = Arc::new(RwLock::new(HashMap::new()));
        db.with_notifier(notifier);

        let mut rx = db.subscribe_key("mylist").await;

        // LPUSH should broadcast
        let len = db.lpush("mylist", &[b"value1"]).unwrap();
        assert_eq!(len, 1);

        // Wait for notification
        tokio::select! {
            result = rx.recv() => {
                assert!(result.is_ok());
            }
            _ = tokio::time::sleep(std::time::Duration::from_millis(100)) => {
                panic!("LPUSH should have broadcast notification");
            }
        }
    }

    #[tokio::test]
    async fn test_rpush_broadcasts_in_server_mode() {
        // RPUSH should send notification after successful insert
        let db = Db::open_memory().unwrap();
        let notifier = Arc::new(RwLock::new(HashMap::new()));
        db.with_notifier(notifier);

        let mut rx = db.subscribe_key("mylist").await;

        // RPUSH should broadcast
        let len = db.rpush("mylist", &[b"value1"]).unwrap();
        assert_eq!(len, 1);

        // Wait for notification
        tokio::select! {
            result = rx.recv() => {
                assert!(result.is_ok());
            }
            _ = tokio::time::sleep(std::time::Duration::from_millis(100)) => {
                panic!("RPUSH should have broadcast notification");
            }
        }
    }

    #[tokio::test]
    async fn test_xadd_broadcasts_in_server_mode() {
        // XADD should send notification after stream entry added
        let db = Db::open_memory().unwrap();
        let notifier = Arc::new(RwLock::new(HashMap::new()));
        db.with_notifier(notifier);

        let mut rx = db.subscribe_key("mystream").await;

        // XADD should broadcast
        let id = db
            .xadd(
                "mystream",
                None,
                &[(b"field", b"value")],
                false,
                None,
                None,
                false,
            )
            .unwrap();
        assert!(id.is_some());

        // Wait for notification
        tokio::select! {
            result = rx.recv() => {
                assert!(result.is_ok());
            }
            _ = tokio::time::sleep(std::time::Duration::from_millis(100)) => {
                panic!("XADD should have broadcast notification");
            }
        }
    }

    #[test]
    fn test_lpush_embedded_mode_no_crash() {
        // LPUSH in embedded mode should not crash
        let db = Db::open_memory().unwrap();
        // Don't attach notifier - this is embedded mode

        // Should complete without error
        let len = db.lpush("mylist", &[b"value1"]).unwrap();
        assert_eq!(len, 1);
    }

    #[test]
    fn test_rpush_embedded_mode_no_crash() {
        // RPUSH in embedded mode should not crash
        let db = Db::open_memory().unwrap();
        // Don't attach notifier - this is embedded mode

        // Should complete without error
        let len = db.rpush("mylist", &[b"value1"]).unwrap();
        assert_eq!(len, 1);
    }

    #[test]
    fn test_xadd_embedded_mode_no_crash() {
        // XADD in embedded mode should not crash
        let db = Db::open_memory().unwrap();
        // Don't attach notifier - this is embedded mode

        // Should complete without error
        let id = db
            .xadd(
                "mystream",
                None,
                &[(b"field", b"value")],
                false,
                None,
                None,
                false,
            )
            .unwrap();
        assert!(id.is_some());
    }

    #[tokio::test]
    async fn test_multiple_lpush_broadcasts() {
        // Multiple LPUSH operations should each trigger notifications
        let db = Db::open_memory().unwrap();
        let notifier = Arc::new(RwLock::new(HashMap::new()));
        db.with_notifier(notifier);

        let mut rx1 = db.subscribe_key("mylist").await;
        let mut rx2 = db.subscribe_key("mylist").await;

        // First LPUSH
        db.lpush("mylist", &[b"value1"]).unwrap();
        tokio::select! {
            result = rx1.recv() => {
                assert!(result.is_ok());
            }
            _ = tokio::time::sleep(std::time::Duration::from_millis(50)) => {
                panic!("First LPUSH should broadcast");
            }
        }

        // Second LPUSH
        db.lpush("mylist", &[b"value2"]).unwrap();
        tokio::select! {
            result = rx2.recv() => {
                assert!(result.is_ok());
            }
            _ = tokio::time::sleep(std::time::Duration::from_millis(50)) => {
                panic!("Second LPUSH should broadcast");
            }
        }
    }
}
