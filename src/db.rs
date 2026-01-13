use rusqlite::{params, Connection, OptionalExtension};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::broadcast;

use crate::error::{KvError, Result};
use crate::types::{
    ConsumerGroupInfo, ConsumerInfo, FtsLevel, FtsResult, FtsStats, HistoryEntry, HistoryStats,
    KeyInfo, KeyType, PendingEntry, PendingSummary, RetentionType, SetOptions, StreamEntry,
    StreamId, StreamInfo, ZMember,
};
#[cfg(feature = "vectors")]
use crate::types::{DistanceMetric, VectorEntry, VectorLevel, VectorSearchResult, VectorStats};

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

/// Simple glob pattern matching (supports *, ?, and [abc])
fn glob_match(pattern: &str, text: &str) -> bool {
    let mut text_idx = 0;
    let mut pattern_idx = 0;
    let text_bytes = text.as_bytes();
    let pattern_bytes = pattern.as_bytes();

    while pattern_idx < pattern_bytes.len() {
        match pattern_bytes[pattern_idx] {
            b'*' => {
                // Skip consecutive '*'
                while pattern_idx < pattern_bytes.len() && pattern_bytes[pattern_idx] == b'*' {
                    pattern_idx += 1;
                }
                // If pattern ends with '*', it matches the rest
                if pattern_idx >= pattern_bytes.len() {
                    return true;
                }
                // Find next match position
                let remaining_pattern =
                    std::str::from_utf8(&pattern_bytes[pattern_idx..]).unwrap_or("");
                while text_idx <= text_bytes.len() {
                    if glob_match(remaining_pattern, &text[text_idx..]) {
                        return true;
                    }
                    text_idx += 1;
                }
                return false;
            }
            b'?' => {
                if text_idx >= text_bytes.len() {
                    return false;
                }
                text_idx += 1;
                pattern_idx += 1;
            }
            b'[' => {
                if text_idx >= text_bytes.len() {
                    return false;
                }
                pattern_idx += 1;
                let mut matched = false;
                let mut negated = false;
                if pattern_idx < pattern_bytes.len() && pattern_bytes[pattern_idx] == b'^' {
                    negated = true;
                    pattern_idx += 1;
                }
                while pattern_idx < pattern_bytes.len() && pattern_bytes[pattern_idx] != b']' {
                    if pattern_bytes[pattern_idx] == text_bytes[text_idx] {
                        matched = true;
                    }
                    pattern_idx += 1;
                }
                if pattern_idx < pattern_bytes.len() {
                    pattern_idx += 1; // Skip ']'
                }
                if matched == negated {
                    return false;
                }
                text_idx += 1;
            }
            c => {
                if text_idx >= text_bytes.len() || text_bytes[text_idx] != c {
                    return false;
                }
                text_idx += 1;
                pattern_idx += 1;
            }
        }
    }
    text_idx == text_bytes.len()
}

impl Db {
    /// Open or create a database at the given path
    pub fn open(path: &str) -> Result<Self> {
        let conn = Connection::open(path)?;

        // Enable WAL mode and optimize pragmas for performance
        conn.execute_batch(
            "PRAGMA journal_mode = WAL;
             PRAGMA synchronous = NORMAL;
             PRAGMA foreign_keys = ON;
             PRAGMA busy_timeout = 5000;
             PRAGMA cache_size = -64000;
             PRAGMA mmap_size = 268435456;
             PRAGMA temp_store = MEMORY;",
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

    /// Open a database with a specific cache size in MB.
    ///
    /// Larger cache = more reads served from RAM = faster.
    /// Default is 64MB. Set to your available RAM for best performance.
    ///
    /// # Example
    /// ```
    /// use redlite::Db;
    ///
    /// // Use 1GB cache for high-performance reads
    /// let db = Db::open_with_cache("mydata.db", 1024).unwrap();
    ///
    /// // Use 256MB cache
    /// let db = Db::open_with_cache("mydata.db", 256).unwrap();
    /// ```
    pub fn open_with_cache(path: &str, cache_mb: i64) -> Result<Self> {
        let conn = Connection::open(path)?;

        // Base pragmas (WAL mode, etc.)
        conn.execute_batch(
            "PRAGMA journal_mode = WAL;
             PRAGMA synchronous = NORMAL;
             PRAGMA foreign_keys = ON;
             PRAGMA busy_timeout = 5000;
             PRAGMA temp_store = MEMORY;",
        )?;

        // Apply cache size (negative = KB, so multiply by 1000)
        let cache_kb = cache_mb * 1000;
        conn.execute(&format!("PRAGMA cache_size = -{};", cache_kb), [])?;

        // Set mmap to 4x cache size (reasonable default)
        let mmap_bytes = cache_mb * 4 * 1024 * 1024;
        conn.execute(&format!("PRAGMA mmap_size = {};", mmap_bytes), [])?;

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

    /// Set the cache size in MB at runtime.
    ///
    /// Larger cache = more reads served from RAM = faster.
    ///
    /// # Example
    /// ```
    /// use redlite::Db;
    ///
    /// let db = Db::open("mydata.db").unwrap();
    /// db.set_cache_mb(1024); // Use 1GB cache
    /// ```
    pub fn set_cache_mb(&self, cache_mb: i64) -> Result<()> {
        let conn = self.core.conn.lock().unwrap_or_else(|e| e.into_inner());
        let cache_kb = cache_mb * 1000;
        conn.execute(&format!("PRAGMA cache_size = -{};", cache_kb), [])?;
        let mmap_bytes = cache_mb * 4 * 1024 * 1024;
        conn.execute(&format!("PRAGMA mmap_size = {};", mmap_bytes), [])?;
        Ok(())
    }

    /// Get current cache size in MB.
    pub fn cache_mb(&self) -> Result<i64> {
        let conn = self.core.conn.lock().unwrap_or_else(|e| e.into_inner());
        let cache_size: i64 = conn.query_row("PRAGMA cache_size", [], |r| r.get(0))?;
        // Negative means KB
        Ok(if cache_size < 0 {
            -cache_size / 1000
        } else {
            cache_size * 4 / 1000
        })
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
        conn.execute_batch(include_str!("schema_history.sql"))?;
        conn.execute_batch(include_str!("schema_fts.sql"))?;
        conn.execute_batch(include_str!("schema_ft.sql"))?; // RediSearch-compatible indexes
        #[cfg(feature = "vectors")]
        conn.execute_batch(include_str!("schema_vectors.sql"))?;

        // Migration: Add version column to keys table if it doesn't exist
        // SQLite doesn't support ADD COLUMN IF NOT EXISTS, so we check first
        let has_version: bool = conn
            .query_row(
                "SELECT COUNT(*) FROM pragma_table_info('keys') WHERE name = 'version'",
                [],
                |row| row.get::<_, i32>(0).map(|c| c > 0),
            )
            .unwrap_or(false);

        if !has_version {
            conn.execute(
                "ALTER TABLE keys ADD COLUMN version INTEGER NOT NULL DEFAULT 0",
                [],
            )?;
        }

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
        self.core
            .autovacuum_enabled
            .store(enabled, Ordering::Relaxed);
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

        // Upsert key (increment version on every write for WATCH/UNWATCH support)
        conn.execute(
            "INSERT INTO keys (db, key, type, expire_at, updated_at, version)
             VALUES (?1, ?2, ?3, ?4, ?5, 1)
             ON CONFLICT(db, key) DO UPDATE SET
                 type = excluded.type,
                 expire_at = excluded.expire_at,
                 updated_at = excluded.updated_at,
                 version = version + 1",
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

        // Release connection before recording history
        drop(conn);

        // Record history for SET operation
        let _ = self.record_history(db, key, "SET", None);

        // Index for FTS if enabled
        let _ = self.fts_index(key, value);

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

        // Release statement and connection before recording history
        drop(stmt);
        drop(conn);

        // Record history and deindex FTS for each deleted key
        for key in keys {
            let _ = self.record_history(db, key, "DEL", None);
            let _ = self.fts_deindex(key);
        }

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
            Ok(None) => Ok(-1),                                  // No expiry
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
            Ok(None) => Ok(-1),                                  // No expiry
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

    /// Get the version number of a key for WATCH/UNWATCH optimistic locking.
    /// Returns 0 if the key doesn't exist (watching a non-existent key).
    pub fn get_version(&self, key: &str) -> Result<u64> {
        let conn = self.core.conn.lock().unwrap_or_else(|e| e.into_inner());
        let db = self.selected_db;
        let now = Self::now_ms();

        // Get version of the key (return 0 if key doesn't exist or is expired)
        let version: u64 = conn
            .query_row(
                "SELECT version FROM keys
                 WHERE db = ?1 AND key = ?2
                 AND (expire_at IS NULL OR expire_at > ?3)",
                params![db, key, now],
                |row| row.get::<_, i64>(0).map(|v| v as u64),
            )
            .unwrap_or(0);

        Ok(version)
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
                let iter = stmt
                    .query_map(params![db, now, p, count as i64, cursor as i64], |row| {
                        row.get(0)
                    })?;
                iter.filter_map(|r| r.ok()).collect()
            }
            None => {
                let iter = stmt
                    .query_map(params![db, now, count as i64, cursor as i64], |row| {
                        row.get(0)
                    })?;
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
            "INSERT INTO keys (db, key, type, expire_at, updated_at, version)
             VALUES (?1, ?2, ?3, ?4, ?5, 1)
             ON CONFLICT(db, key) DO UPDATE SET
                 type = excluded.type,
                 expire_at = excluded.expire_at,
                 updated_at = excluded.updated_at,
                 version = version + 1
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
            "INSERT INTO keys (db, key, type, expire_at, updated_at, version)
             VALUES (?1, ?2, ?3, ?4, ?5, 1)
             ON CONFLICT(db, key) DO UPDATE SET
                 type = excluded.type,
                 expire_at = excluded.expire_at,
                 updated_at = excluded.updated_at,
                 version = version + 1
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
        keys.iter().map(|k| self.get(k).unwrap_or(None)).collect()
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
                    "INSERT INTO keys (db, key, type, expire_at, updated_at, version)
                     VALUES (?1, ?2, ?3, NULL, ?4, 1)
                     ON CONFLICT(db, key) DO UPDATE SET
                         type = excluded.type,
                         expire_at = excluded.expire_at,
                         updated_at = excluded.updated_at,
                         version = version + 1",
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
                    "UPDATE keys SET updated_at = ?1, version = version + 1 WHERE id = ?2",
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
            "INSERT INTO keys (db, key, type, updated_at, version) VALUES (?1, ?2, ?3, ?4, 1)",
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

        // Count new fields with per-field existence checks (simple, fast for typical use)
        let mut new_fields = 0i64;
        for (field, value) in pairs {
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

            conn.execute(
                "INSERT INTO hashes (key_id, field, value) VALUES (?1, ?2, ?3)
                 ON CONFLICT(key_id, field) DO UPDATE SET value = excluded.value",
                params![key_id, field, value],
            )?;
        }

        // Update key timestamp and version
        let now = Self::now_ms();
        conn.execute(
            "UPDATE keys SET updated_at = ?1, version = version + 1 WHERE id = ?2",
            params![now, key_id],
        )?;

        // Release connection before recording history
        drop(conn);

        // Record history for HSET operation
        let _ = self.record_history(self.selected_db, key, "HSET", None);

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
            "UPDATE keys SET updated_at = ?1, version = version + 1 WHERE id = ?2",
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
            "UPDATE keys SET updated_at = ?1, version = version + 1 WHERE id = ?2",
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
            "UPDATE keys SET updated_at = ?1, version = version + 1 WHERE id = ?2",
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
        let mut stmt =
            conn.prepare("SELECT pos, value FROM lists WHERE key_id = ?1 ORDER BY pos ASC")?;
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
            "INSERT INTO keys (db, key, type, updated_at, version) VALUES (?1, ?2, ?3, ?4, 1)",
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
            "UPDATE keys SET updated_at = ?1, version = version + 1 WHERE id = ?2",
            params![now, key_id],
        )?;

        // Release lock before async notification
        drop(conn);

        // Record history for LPUSH operation
        let _ = self.record_history(self.selected_db, key, "LPUSH", None);

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
            "UPDATE keys SET updated_at = ?1, version = version + 1 WHERE id = ?2",
            params![now, key_id],
        )?;

        // Release lock before async notification
        drop(conn);

        // Record history for RPUSH operation
        let _ = self.record_history(self.selected_db, key, "RPUSH", None);

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
        let mut stmt = conn
            .prepare("SELECT pos, value FROM lists WHERE key_id = ?1 ORDER BY pos ASC LIMIT ?2")?;
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
                "UPDATE keys SET updated_at = ?1, version = version + 1 WHERE id = ?2",
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
        let mut stmt = conn
            .prepare("SELECT pos, value FROM lists WHERE key_id = ?1 ORDER BY pos DESC LIMIT ?2")?;
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
                "UPDATE keys SET updated_at = ?1, version = version + 1 WHERE id = ?2",
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

        // Only query COUNT if we have negative indices (optimization)
        let len: i64 = if start < 0 || stop < 0 {
            conn.query_row(
                "SELECT COUNT(*) FROM lists WHERE key_id = ?1",
                params![key_id],
                |row| row.get(0),
            )?
        } else {
            // For positive indices, we can skip the COUNT and rely on LIMIT/OFFSET
            -1i64 // sentinel value indicating COUNT was not needed
        };

        // Convert negative indices to positive (only if len was queried)
        let (start, stop, needs_bounds) = if len >= 0 {
            if len == 0 {
                return Ok(vec![]);
            }

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

            (start, stop, true)
        } else {
            // For positive indices, don't apply bounds yet
            (start, stop, false)
        };

        // For positive indices, start > stop means empty range
        if start > stop {
            return Ok(vec![]);
        }

        let count = stop - start + 1;

        // Get elements by logical index (ordered by position)
        let mut stmt = conn.prepare(
            "SELECT value FROM lists WHERE key_id = ?1 ORDER BY pos ASC LIMIT ?2 OFFSET ?3",
        )?;
        let rows = stmt.query_map(params![key_id, count, start], |row| {
            row.get::<_, Vec<u8>>(0)
        })?;

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

        // Only query list length if we have negative index (optimization)
        let index = if index < 0 {
            // Need to query length for negative index conversion
            let len: i64 = conn.query_row(
                "SELECT COUNT(*) FROM lists WHERE key_id = ?1",
                params![key_id],
                |row| row.get(0),
            )?;

            if len == 0 {
                return Ok(None);
            }

            len + index
        } else {
            // For non-negative indices, use as-is
            index
        };

        if index < 0 {
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
            "UPDATE keys SET updated_at = ?1, version = version + 1 WHERE id = ?2",
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
        let mut stmt = conn.prepare("SELECT pos FROM lists WHERE key_id = ?1 ORDER BY pos ASC")?;
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
                "UPDATE keys SET updated_at = ?1, version = version + 1 WHERE id = ?2",
                params![now, key_id],
            )?;
        }

        Ok(())
    }

    /// LREM key count element - remove elements equal to element, returns count removed
    /// count > 0: remove first count occurrences from head to tail
    /// count < 0: remove first |count| occurrences from tail to head
    /// count = 0: remove all occurrences
    pub fn lrem(&self, key: &str, count: i64, element: &[u8]) -> Result<i64> {
        let conn = self.core.conn.lock().unwrap_or_else(|e| e.into_inner());

        let key_id = match self.get_list_key_id(&conn, key)? {
            Some(id) => id,
            None => return Ok(0), // Non-existent key returns 0
        };

        // Get all list entries to find matching positions
        let mut stmt = conn.prepare("SELECT pos FROM lists WHERE key_id = ?1 ORDER BY pos ASC")?;
        let rows = stmt.query_map(params![key_id], |row| row.get::<_, i64>(0))?;

        let all_positions: Vec<i64> = rows.filter_map(|r| r.ok()).collect();

        if all_positions.is_empty() {
            return Ok(0);
        }

        // Find indices of matching values
        let mut matching_indices: Vec<usize> = Vec::new();
        for (idx, pos) in all_positions.iter().enumerate() {
            let value: Vec<u8> = conn.query_row(
                "SELECT value FROM lists WHERE key_id = ?1 AND pos = ?2",
                params![key_id, pos],
                |row| row.get(0),
            )?;
            if value == element {
                matching_indices.push(idx);
            }
        }

        if matching_indices.is_empty() {
            return Ok(0);
        }

        // Determine which indices to delete based on count
        let indices_to_delete: Vec<usize> = if count == 0 {
            // Delete all matches
            matching_indices
        } else if count > 0 {
            // Delete first count matches from head (take from start)
            matching_indices.into_iter().take(count as usize).collect()
        } else {
            // Delete first |count| matches from tail (take from end)
            let abs_count = (-count) as usize;
            let len = matching_indices.len();
            if abs_count >= len {
                matching_indices
            } else {
                matching_indices.into_iter().skip(len - abs_count).collect()
            }
        };

        let removed_count = indices_to_delete.len() as i64;

        // Sort indices in reverse order for safe deletion
        let mut sorted_indices = indices_to_delete;
        sorted_indices.sort_by(|a, b| b.cmp(a));

        // Delete in reverse order to maintain correct indices
        for idx in sorted_indices {
            let pos = all_positions[idx];
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
                "UPDATE keys SET updated_at = ?1, version = version + 1 WHERE id = ?2",
                params![now, key_id],
            )?;

            // Record history for LREM operation
            let _ = self.record_history(self.selected_db, key, "LREM", None);

            // Notify watchers
            if self.is_server_mode() {
                let key = key.to_string();
                let db = self.clone();
                tokio::spawn(async move {
                    let _ = db.notify_key(&key).await;
                });
            }
        }

        Ok(removed_count)
    }

    /// LINSERT key BEFORE|AFTER pivot element - insert element before or after pivot
    /// Returns: length of list after insert, 0 if key doesn't exist, -1 if pivot not found
    pub fn linsert(&self, key: &str, before: bool, pivot: &[u8], element: &[u8]) -> Result<i64> {
        let conn = self.core.conn.lock().unwrap_or_else(|e| e.into_inner());

        let key_id = match self.get_list_key_id(&conn, key)? {
            Some(id) => id,
            None => return Ok(0), // Key doesn't exist
        };

        // Find the position of the pivot element
        let mut stmt = conn.prepare("SELECT pos FROM lists WHERE key_id = ?1 ORDER BY pos ASC")?;
        let rows = stmt.query_map(params![key_id], |row| row.get::<_, i64>(0))?;

        let positions: Vec<i64> = rows.filter_map(|r| r.ok()).collect();
        let mut pivot_pos: Option<i64> = None;

        for pos in &positions {
            let value: Vec<u8> = conn.query_row(
                "SELECT value FROM lists WHERE key_id = ?1 AND pos = ?2",
                params![key_id, pos],
                |row| row.get(0),
            )?;
            if value == pivot {
                pivot_pos = Some(*pos);
                break;
            }
        }

        let pivot_pos = match pivot_pos {
            Some(p) => p,
            None => return Ok(-1), // Pivot not found
        };

        // Find the next and previous positions to insert between
        let target_pos = if before {
            // Insert before pivot: use position between previous and pivot
            let prev_idx = positions.iter().position(|&p| p == pivot_pos).unwrap_or(0);
            if prev_idx == 0 {
                // Insert at head: use position less than pivot
                pivot_pos - Self::LIST_GAP
            } else {
                // Insert between previous and pivot
                let prev_pos = positions[prev_idx - 1];
                let next_pos = pivot_pos;
                if prev_pos < next_pos - 1 {
                    (prev_pos + next_pos) / 2
                } else {
                    // Need to rebalance
                    self.rebalance_list(&conn, key_id)?;
                    // Re-find pivot and positions after rebalancing
                    let mut stmt =
                        conn.prepare("SELECT pos FROM lists WHERE key_id = ?1 ORDER BY pos ASC")?;
                    let rows = stmt.query_map(params![key_id], |row| row.get::<_, i64>(0))?;
                    let new_positions: Vec<i64> = rows.filter_map(|r| r.ok()).collect();
                    let prev_idx = new_positions
                        .iter()
                        .position(|&p| {
                            let v: std::result::Result<Vec<u8>, _> = conn.query_row(
                                "SELECT value FROM lists WHERE key_id = ?1 AND pos = ?2",
                                params![key_id, p],
                                |row| row.get(0),
                            );
                            v.ok().as_ref() == Some(&pivot.to_vec())
                        })
                        .unwrap_or(0);
                    if prev_idx == 0 {
                        new_positions[0] - Self::LIST_GAP
                    } else {
                        let prev = new_positions[prev_idx - 1];
                        let curr = new_positions[prev_idx];
                        (prev + curr) / 2
                    }
                }
            }
        } else {
            // Insert after pivot: use position between pivot and next
            let pivot_idx = positions.iter().position(|&p| p == pivot_pos).unwrap_or(0);
            if pivot_idx == positions.len() - 1 {
                // Insert at tail: use position greater than pivot
                pivot_pos + Self::LIST_GAP
            } else {
                // Insert between pivot and next
                let curr_pos = pivot_pos;
                let next_pos = positions[pivot_idx + 1];
                if curr_pos < next_pos - 1 {
                    (curr_pos + next_pos) / 2
                } else {
                    // Need to rebalance
                    self.rebalance_list(&conn, key_id)?;
                    // Re-find pivot and positions after rebalancing
                    let mut stmt =
                        conn.prepare("SELECT pos FROM lists WHERE key_id = ?1 ORDER BY pos ASC")?;
                    let rows = stmt.query_map(params![key_id], |row| row.get::<_, i64>(0))?;
                    let new_positions: Vec<i64> = rows.filter_map(|r| r.ok()).collect();
                    let pivot_idx = new_positions
                        .iter()
                        .position(|&p| {
                            let v: std::result::Result<Vec<u8>, _> = conn.query_row(
                                "SELECT value FROM lists WHERE key_id = ?1 AND pos = ?2",
                                params![key_id, p],
                                |row| row.get(0),
                            );
                            v.ok().as_ref() == Some(&pivot.to_vec())
                        })
                        .unwrap_or(0);
                    if pivot_idx == new_positions.len() - 1 {
                        new_positions[pivot_idx] + Self::LIST_GAP
                    } else {
                        let curr = new_positions[pivot_idx];
                        let next = new_positions[pivot_idx + 1];
                        (curr + next) / 2
                    }
                }
            }
        };

        // Insert the element
        conn.execute(
            "INSERT INTO lists (key_id, pos, value) VALUES (?1, ?2, ?3)",
            params![key_id, target_pos, element],
        )?;

        // Get new list length
        let length: i64 = conn.query_row(
            "SELECT COUNT(*) FROM lists WHERE key_id = ?1",
            params![key_id],
            |row| row.get(0),
        )?;

        // Update key timestamp
        let now = Self::now_ms();
        conn.execute(
            "UPDATE keys SET updated_at = ?1, version = version + 1 WHERE id = ?2",
            params![now, key_id],
        )?;

        // Record history for LINSERT operation
        let _ = self.record_history(self.selected_db, key, "LINSERT", None);

        // Notify watchers
        if self.is_server_mode() {
            let key = key.to_string();
            let db = self.clone();
            tokio::spawn(async move {
                let _ = db.notify_key(&key).await;
            });
        }

        Ok(length)
    }

    // --- Session 8: Set operations ---

    /// Helper to create a new set key
    fn create_set_key(&self, conn: &Connection, key: &str) -> Result<i64> {
        let db = self.selected_db;
        let now = Self::now_ms();

        conn.execute(
            "INSERT INTO keys (db, key, type, created_at, updated_at, version)
             VALUES (?1, ?2, ?3, ?4, ?4, 1)",
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
            "UPDATE keys SET updated_at = ?1, version = version + 1 WHERE id = ?2",
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
                "UPDATE keys SET updated_at = ?1, version = version + 1 WHERE id = ?2",
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

        let mut stmt = conn.prepare("SELECT member FROM sets WHERE key_id = ?1")?;

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
                "UPDATE keys SET updated_at = ?1, version = version + 1 WHERE id = ?2",
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
            let mut stmt = conn.prepare("SELECT member FROM sets WHERE key_id = ?1")?;
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
            let mut stmt = conn
                .prepare("SELECT member FROM sets WHERE key_id = ?1 ORDER BY RANDOM() LIMIT ?2")?;
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

    /// SMOVE source destination member - atomically move member from source to destination
    /// Returns: 1 if member was moved, 0 if member not in source or source doesn't exist
    pub fn smove(&self, source: &str, destination: &str, member: &[u8]) -> Result<i64> {
        let conn = self.core.conn.lock().unwrap_or_else(|e| e.into_inner());

        // Get source set key_id
        let source_key_id = match self.get_set_key_id(&conn, source)? {
            Some(id) => id,
            None => return Ok(0), // Source doesn't exist
        };

        // Check if member exists in source
        let mut stmt =
            conn.prepare("SELECT 1 FROM sets WHERE key_id = ?1 AND member = ?2 LIMIT 1")?;
        let exists: std::result::Result<i32, _> =
            stmt.query_row(params![source_key_id, member], |_| Ok(1));

        if exists.is_err() {
            return Ok(0); // Member not in source
        }

        // Get or create destination set key_id
        let dest_key_id = self.get_or_create_set_key(&conn, destination)?;

        // Remove from source
        conn.execute(
            "DELETE FROM sets WHERE key_id = ?1 AND member = ?2",
            params![source_key_id, member],
        )?;

        // Add to destination (using INSERT OR IGNORE to avoid duplicate if already exists)
        conn.execute(
            "INSERT OR IGNORE INTO sets (key_id, member) VALUES (?1, ?2)",
            params![dest_key_id, member],
        )?;

        // Clean up empty source set
        let remaining: i64 = conn.query_row(
            "SELECT COUNT(*) FROM sets WHERE key_id = ?1",
            params![source_key_id],
            |row| row.get(0),
        )?;

        if remaining == 0 {
            conn.execute("DELETE FROM keys WHERE id = ?1", params![source_key_id])?;
        } else {
            // Update source timestamp
            let now = Self::now_ms();
            conn.execute(
                "UPDATE keys SET updated_at = ?1, version = version + 1 WHERE id = ?2",
                params![now, source_key_id],
            )?;
        }

        // Update destination timestamp
        let now = Self::now_ms();
        conn.execute(
            "UPDATE keys SET updated_at = ?1, version = version + 1 WHERE id = ?2",
            params![now, dest_key_id],
        )?;

        Ok(1)
    }

    /// SDIFFSTORE destination key [key ...] - compute set difference and store result
    /// Returns: number of elements in resulting set
    pub fn sdiffstore(&self, destination: &str, keys: &[&str]) -> Result<i64> {
        if keys.is_empty() {
            return Ok(0);
        }

        // Compute the difference
        let diff_result = self.sdiff(keys)?;

        // Clear destination if it exists
        if let Some(dest_key_id) = self.get_set_key_id(
            &self.core.conn.lock().unwrap_or_else(|e| e.into_inner()),
            destination,
        )? {
            let conn = self.core.conn.lock().unwrap_or_else(|e| e.into_inner());
            conn.execute("DELETE FROM sets WHERE key_id = ?1", params![dest_key_id])?;
            conn.execute("DELETE FROM keys WHERE id = ?1", params![dest_key_id])?;
        }

        // If result is empty, delete destination and return 0
        if diff_result.is_empty() {
            return Ok(0);
        }

        // Store the result in destination
        let conn = self.core.conn.lock().unwrap_or_else(|e| e.into_inner());
        let dest_key_id = self.get_or_create_set_key(&conn, destination)?;

        for member in &diff_result {
            conn.execute(
                "INSERT INTO sets (key_id, member) VALUES (?1, ?2)",
                params![dest_key_id, member],
            )?;
        }

        // Update timestamp
        let now = Self::now_ms();
        conn.execute(
            "UPDATE keys SET updated_at = ?1, version = version + 1 WHERE id = ?2",
            params![now, dest_key_id],
        )?;

        Ok(diff_result.len() as i64)
    }

    /// SINTERSTORE destination key [key ...] - compute set intersection and store result
    /// Returns: number of elements in resulting set
    pub fn sinterstore(&self, destination: &str, keys: &[&str]) -> Result<i64> {
        if keys.is_empty() {
            return Ok(0);
        }

        // Compute the intersection
        let inter_result = self.sinter(keys)?;

        // Clear destination if it exists
        if let Some(dest_key_id) = self.get_set_key_id(
            &self.core.conn.lock().unwrap_or_else(|e| e.into_inner()),
            destination,
        )? {
            let conn = self.core.conn.lock().unwrap_or_else(|e| e.into_inner());
            conn.execute("DELETE FROM sets WHERE key_id = ?1", params![dest_key_id])?;
            conn.execute("DELETE FROM keys WHERE id = ?1", params![dest_key_id])?;
        }

        // If result is empty, delete destination and return 0
        if inter_result.is_empty() {
            return Ok(0);
        }

        // Store the result in destination
        let conn = self.core.conn.lock().unwrap_or_else(|e| e.into_inner());
        let dest_key_id = self.get_or_create_set_key(&conn, destination)?;

        for member in &inter_result {
            conn.execute(
                "INSERT INTO sets (key_id, member) VALUES (?1, ?2)",
                params![dest_key_id, member],
            )?;
        }

        // Update timestamp
        let now = Self::now_ms();
        conn.execute(
            "UPDATE keys SET updated_at = ?1, version = version + 1 WHERE id = ?2",
            params![now, dest_key_id],
        )?;

        Ok(inter_result.len() as i64)
    }

    /// SUNIONSTORE destination key [key ...] - compute set union and store result
    /// Returns: number of elements in resulting set
    pub fn sunionstore(&self, destination: &str, keys: &[&str]) -> Result<i64> {
        if keys.is_empty() {
            return Ok(0);
        }

        // Compute the union
        let union_result = self.sunion(keys)?;

        // Clear destination if it exists
        if let Some(dest_key_id) = self.get_set_key_id(
            &self.core.conn.lock().unwrap_or_else(|e| e.into_inner()),
            destination,
        )? {
            let conn = self.core.conn.lock().unwrap_or_else(|e| e.into_inner());
            conn.execute("DELETE FROM sets WHERE key_id = ?1", params![dest_key_id])?;
            conn.execute("DELETE FROM keys WHERE id = ?1", params![dest_key_id])?;
        }

        // If result is empty, delete destination and return 0
        if union_result.is_empty() {
            return Ok(0);
        }

        // Store the result in destination
        let conn = self.core.conn.lock().unwrap_or_else(|e| e.into_inner());
        let dest_key_id = self.get_or_create_set_key(&conn, destination)?;

        for member in &union_result {
            conn.execute(
                "INSERT INTO sets (key_id, member) VALUES (?1, ?2)",
                params![dest_key_id, member],
            )?;
        }

        // Update timestamp
        let now = Self::now_ms();
        conn.execute(
            "UPDATE keys SET updated_at = ?1, version = version + 1 WHERE id = ?2",
            params![now, dest_key_id],
        )?;

        Ok(union_result.len() as i64)
    }

    // --- Session 9: Sorted Set operations ---

    /// Helper to create a new sorted set key
    fn create_zset_key(&self, conn: &Connection, key: &str) -> Result<i64> {
        let db = self.selected_db;
        let now = Self::now_ms();

        conn.execute(
            "INSERT INTO keys (db, key, type, created_at, updated_at, version)
             VALUES (?1, ?2, ?3, ?4, ?4, 1)",
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

        // Count new members with per-member existence checks (simple, fast for typical use)
        let mut added = 0i64;
        for m in members {
            let exists: bool = conn
                .query_row(
                    "SELECT 1 FROM zsets WHERE key_id = ?1 AND member = ?2",
                    params![key_id, m.member],
                    |_| Ok(true),
                )
                .unwrap_or(false);

            if !exists {
                added += 1;
            }

            conn.execute(
                "INSERT INTO zsets (key_id, member, score) VALUES (?1, ?2, ?3)
                 ON CONFLICT(key_id, member) DO UPDATE SET score = excluded.score",
                params![key_id, m.member, m.score],
            )?;
        }

        // Update timestamp
        let now = Self::now_ms();
        conn.execute(
            "UPDATE keys SET updated_at = ?1, version = version + 1 WHERE id = ?2",
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
                "UPDATE keys SET updated_at = ?1, version = version + 1 WHERE id = ?2",
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
    pub fn zrange(
        &self,
        key: &str,
        start: i64,
        stop: i64,
        with_scores: bool,
    ) -> Result<Vec<ZMember>> {
        self.maybe_autovacuum();
        let conn = self.core.conn.lock().unwrap_or_else(|e| e.into_inner());

        let key_id = match self.get_zset_key_id(&conn, key)? {
            Some(id) => id,
            None => return Ok(vec![]),
        };

        // Only query total count if we have negative indices (optimization)
        let total: i64 = if start < 0 || stop < 0 {
            conn.query_row(
                "SELECT COUNT(*) FROM zsets WHERE key_id = ?1",
                params![key_id],
                |row| row.get(0),
            )?
        } else {
            // For positive indices, use sentinel value
            -1i64
        };

        // Convert negative indices to positive (only if total was queried)
        let (start, stop, needs_bounds) = if total >= 0 {
            if total == 0 {
                return Ok(vec![]);
            }

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

            (start, stop, true)
        } else {
            (start, stop, false)
        };

        if needs_bounds && (start > stop || start >= total) {
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
    pub fn zrevrange(
        &self,
        key: &str,
        start: i64,
        stop: i64,
        with_scores: bool,
    ) -> Result<Vec<ZMember>> {
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
            "UPDATE keys SET updated_at = ?1, version = version + 1 WHERE id = ?2",
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
                "UPDATE keys SET updated_at = ?1, version = version + 1 WHERE id = ?2",
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
                "UPDATE keys SET updated_at = ?1, version = version + 1 WHERE id = ?2",
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
            "INSERT INTO keys (db, key, type, updated_at, version) VALUES (?1, ?2, ?3, ?4, 1)",
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
        id: Option<StreamId>, // None means auto-generate with *
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
            "UPDATE keys SET updated_at = ?1, version = version + 1 WHERE id = ?2",
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

        // Record history for XADD operation
        let _ = self.record_history(self.selected_db, key, "XADD", None);

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
            "UPDATE keys SET updated_at = ?1, version = version + 1 WHERE id = ?2",
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
            "UPDATE keys SET updated_at = ?1, version = version + 1 WHERE id = ?2",
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
        let exists: bool = conn
            .query_row(
                "SELECT 1 FROM stream_groups WHERE key_id = ?1 AND name = ?2",
                params![key_id, group],
                |_| Ok(true),
            )
            .unwrap_or(false);

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
    pub fn xgroup_createconsumer(&self, key: &str, group: &str, consumer: &str) -> Result<bool> {
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
        let exists: bool = conn
            .query_row(
                "SELECT 1 FROM stream_consumers WHERE group_id = ?1 AND name = ?2",
                params![group_id, consumer],
                |_| Ok(true),
            )
            .unwrap_or(false);

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
    pub fn xgroup_delconsumer(&self, key: &str, group: &str, consumer: &str) -> Result<i64> {
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
        let pending_count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM stream_pending WHERE group_id = ?1 AND consumer = ?2",
                params![group_id, consumer],
                |row| row.get(0),
            )
            .unwrap_or(0);

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
    fn get_or_create_consumer(
        &self,
        conn: &Connection,
        group_id: i64,
        consumer: &str,
    ) -> Result<i64> {
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
    fn get_group_info(
        &self,
        conn: &Connection,
        key_id: i64,
        group: &str,
    ) -> Result<Option<(i64, i64, i64)>> {
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
    fn get_stream_entry_id(
        &self,
        conn: &Connection,
        key_id: i64,
        stream_id: &StreamId,
    ) -> Option<i64> {
        conn.query_row(
            "SELECT id FROM streams WHERE key_id = ?1 AND entry_ms = ?2 AND entry_seq = ?3",
            params![key_id, stream_id.ms, stream_id.seq],
            |row| row.get(0),
        )
        .ok()
    }

    /// XREADGROUP GROUP group consumer [COUNT count] [NOACK] STREAMS key [key ...] id [id ...]
    /// Reads from streams as part of a consumer group
    pub fn xreadgroup(
        &self,
        group: &str,
        consumer: &str,
        keys: &[&str],
        ids: &[&str], // ">" means new, other IDs mean pending
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
                    .query_map(params![key_id, start.ms, start.seq, limit], |row| {
                        let db_id: i64 = row.get(0)?;
                        let ms: i64 = row.get(1)?;
                        let seq: i64 = row.get(2)?;
                        let data: Vec<u8> = row.get(3)?;
                        Ok((
                            db_id,
                            StreamEntry::new(
                                StreamId::new(ms, seq),
                                Self::decode_stream_fields(&data),
                            ),
                        ))
                    })?
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
                            Ok((
                                db_id,
                                StreamEntry::new(
                                    StreamId::new(ms, seq),
                                    Self::decode_stream_fields(&data),
                                ),
                                pending_id,
                            ))
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
            None => {
                return Ok(PendingSummary {
                    count: 0,
                    smallest_id: None,
                    largest_id: None,
                    consumers: vec![],
                })
            }
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
        let smallest_id: Option<StreamId> = conn
            .query_row(
                "SELECT s.entry_ms, s.entry_seq FROM stream_pending sp
             JOIN streams s ON s.id = sp.entry_id
             WHERE sp.group_id = ?1
             ORDER BY s.entry_ms ASC, s.entry_seq ASC LIMIT 1",
                params![group_id],
                |row| Ok(StreamId::new(row.get(0)?, row.get(1)?)),
            )
            .ok();

        // Get largest ID
        let largest_id: Option<StreamId> = conn
            .query_row(
                "SELECT s.entry_ms, s.entry_seq FROM stream_pending sp
             JOIN streams s ON s.id = sp.entry_id
             WHERE sp.group_id = ?1
             ORDER BY s.entry_ms DESC, s.entry_seq DESC LIMIT 1",
                params![group_id],
                |row| Ok(StreamId::new(row.get(0)?, row.get(1)?)),
            )
            .ok();

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
        let (sql, params_vec): (String, Vec<Box<dyn rusqlite::ToSql>>) =
            if let Some(c) = consumer {
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
        let params_refs: Vec<&dyn rusqlite::ToSql> =
            params_vec.iter().map(|p| p.as_ref()).collect();

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
            let pending_info: Option<(i64, i64, i64)> = conn
                .query_row(
                    "SELECT id, delivered_at, delivery_count FROM stream_pending
                 WHERE group_id = ?1 AND entry_id = ?2",
                    params![group_id, entry_id],
                    |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
                )
                .ok();

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
            let consumers: i64 = conn
                .query_row(
                    "SELECT COUNT(*) FROM stream_consumers WHERE group_id = ?1",
                    params![group_id],
                    |row| row.get(0),
                )
                .unwrap_or(0);

            // Count pending
            let pending: i64 = conn
                .query_row(
                    "SELECT COUNT(*) FROM stream_pending WHERE group_id = ?1",
                    params![group_id],
                    |row| row.get(0),
                )
                .unwrap_or(0);

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
            .query_map(params![group_id], |row| Ok((row.get(0)?, row.get(1)?)))?
            .filter_map(|r| r.ok())
            .collect();

        let mut result = Vec::new();
        for (name, seen_time) in consumers {
            // Count pending for this consumer
            let pending: i64 = conn
                .query_row(
                    "SELECT COUNT(*) FROM stream_pending WHERE group_id = ?1 AND consumer = ?2",
                    params![group_id, name],
                    |row| row.get(0),
                )
                .unwrap_or(0);

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
    pub fn with_notifier(&self, notifier: Arc<RwLock<HashMap<String, broadcast::Sender<()>>>>) {
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
                    Some(rx1) => match rx_iter.next() {
                        Some(rx2) => match rx_iter.next() {
                            Some(rx3) => match rx_iter.next() {
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
                            },
                            None => {
                                tokio::select! {
                                    _ = rx0.recv() => {},
                                    _ = rx1.recv() => {},
                                    _ = rx2.recv() => {},
                                    _ = tokio::time::sleep(wait_duration) => {},
                                }
                            }
                        },
                        None => {
                            tokio::select! {
                                _ = rx0.recv() => {},
                                _ = rx1.recv() => {},
                                _ = tokio::time::sleep(wait_duration) => {},
                            }
                        }
                    },
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
                    Some(rx1) => match rx_iter.next() {
                        Some(rx2) => match rx_iter.next() {
                            Some(rx3) => match rx_iter.next() {
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
                            },
                            None => {
                                tokio::select! {
                                    _ = rx0.recv() => {},
                                    _ = rx1.recv() => {},
                                    _ = rx2.recv() => {},
                                    _ = tokio::time::sleep(wait_duration) => {},
                                }
                            }
                        },
                        None => {
                            tokio::select! {
                                _ = rx0.recv() => {},
                                _ = rx1.recv() => {},
                                _ = tokio::time::sleep(wait_duration) => {},
                            }
                        }
                    },
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
                    Some(rx1) => match rx_iter.next() {
                        Some(rx2) => match rx_iter.next() {
                            Some(rx3) => match rx_iter.next() {
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
                            },
                            None => {
                                tokio::select! {
                                    _ = rx0.recv() => {},
                                    _ = rx1.recv() => {},
                                    _ = rx2.recv() => {},
                                    _ = tokio::time::sleep(wait_duration) => {},
                                }
                            }
                        },
                        None => {
                            tokio::select! {
                                _ = rx0.recv() => {},
                                _ = rx1.recv() => {},
                                _ = tokio::time::sleep(wait_duration) => {},
                            }
                        }
                    },
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
                    Some(rx1) => match rx_iter.next() {
                        Some(rx2) => match rx_iter.next() {
                            Some(rx3) => match rx_iter.next() {
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
                            },
                            None => {
                                tokio::select! {
                                    _ = rx0.recv() => {},
                                    _ = rx1.recv() => {},
                                    _ = rx2.recv() => {},
                                    _ = tokio::time::sleep(wait_duration) => {},
                                }
                            }
                        },
                        None => {
                            tokio::select! {
                                _ = rx0.recv() => {},
                                _ = rx1.recv() => {},
                                _ = tokio::time::sleep(wait_duration) => {},
                            }
                        }
                    },
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

    // ===== Session 17.2: Configuration Methods =====

    /// Enable history tracking globally for all databases
    pub fn history_enable_global(&self, retention: RetentionType) -> Result<()> {
        let conn = self.core.conn.lock().unwrap_or_else(|e| e.into_inner());
        let retention_type = retention.as_str();
        let retention_value = match retention {
            RetentionType::Unlimited => None,
            RetentionType::Time(ms) => Some(ms),
            RetentionType::Count(n) => Some(n),
        };

        conn.execute(
            "INSERT OR REPLACE INTO history_config (level, target, enabled, retention_type, retention_value)
             VALUES ('global', '*', 1, ?, ?)",
            params![retention_type, retention_value],
        )?;
        Ok(())
    }

    /// Enable history tracking for a specific database
    pub fn history_enable_database(&self, db_num: i32, retention: RetentionType) -> Result<()> {
        if !(0..=15).contains(&db_num) {
            return Err(KvError::SyntaxError);
        }
        let conn = self.core.conn.lock().unwrap_or_else(|e| e.into_inner());
        let target = db_num.to_string();
        let retention_type = retention.as_str();
        let retention_value = match retention {
            RetentionType::Unlimited => None,
            RetentionType::Time(ms) => Some(ms),
            RetentionType::Count(n) => Some(n),
        };

        conn.execute(
            "INSERT OR REPLACE INTO history_config (level, target, enabled, retention_type, retention_value)
             VALUES ('database', ?, 1, ?, ?)",
            params![target, retention_type, retention_value],
        )?;
        Ok(())
    }

    /// Enable history tracking for a specific key
    pub fn history_enable_key(&self, key: &str, retention: RetentionType) -> Result<()> {
        let conn = self.core.conn.lock().unwrap_or_else(|e| e.into_inner());
        let target = format!("{}:{}", self.selected_db, key);
        let retention_type = retention.as_str();
        let retention_value = match retention {
            RetentionType::Unlimited => None,
            RetentionType::Time(ms) => Some(ms),
            RetentionType::Count(n) => Some(n),
        };

        conn.execute(
            "INSERT OR REPLACE INTO history_config (level, target, enabled, retention_type, retention_value)
             VALUES ('key', ?, 1, ?, ?)",
            params![target, retention_type, retention_value],
        )?;
        Ok(())
    }

    /// Disable history tracking globally
    pub fn history_disable_global(&self) -> Result<()> {
        let conn = self.core.conn.lock().unwrap_or_else(|e| e.into_inner());
        conn.execute(
            "UPDATE history_config SET enabled = 0 WHERE level = 'global' AND target = '*'",
            [],
        )?;
        Ok(())
    }

    /// Disable history tracking for a specific database
    pub fn history_disable_database(&self, db_num: i32) -> Result<()> {
        if !(0..=15).contains(&db_num) {
            return Err(KvError::SyntaxError);
        }
        let conn = self.core.conn.lock().unwrap_or_else(|e| e.into_inner());
        let target = db_num.to_string();
        conn.execute(
            "UPDATE history_config SET enabled = 0 WHERE level = 'database' AND target = ?",
            params![target],
        )?;
        Ok(())
    }

    /// Disable history tracking for a specific key
    pub fn history_disable_key(&self, key: &str) -> Result<()> {
        let conn = self.core.conn.lock().unwrap_or_else(|e| e.into_inner());
        let target = format!("{}:{}", self.selected_db, key);
        conn.execute(
            "UPDATE history_config SET enabled = 0 WHERE level = 'key' AND target = ?",
            params![target],
        )?;
        Ok(())
    }

    /// Check if history is enabled for a key (three-tier lookup: key > db > global)
    pub fn is_history_enabled(&self, key: &str) -> Result<bool> {
        let conn = self.core.conn.lock().unwrap_or_else(|e| e.into_inner());

        // First check key-level config
        let key_target = format!("{}:{}", self.selected_db, key);
        let key_enabled: Option<bool> = conn
            .query_row(
                "SELECT enabled FROM history_config WHERE level = 'key' AND target = ? LIMIT 1",
                params![key_target],
                |row| row.get(0),
            )
            .optional()?;

        if let Some(enabled) = key_enabled {
            return Ok(enabled);
        }

        // Fall back to database-level config
        let db_enabled: Option<bool> = conn
            .query_row(
                "SELECT enabled FROM history_config WHERE level = 'database' AND target = ? LIMIT 1",
                params![self.selected_db.to_string()],
                |row| row.get(0),
            )
            .optional()?;

        if let Some(enabled) = db_enabled {
            return Ok(enabled);
        }

        // Fall back to global config
        let global_enabled: Option<bool> = conn
            .query_row(
                "SELECT enabled FROM history_config WHERE level = 'global' AND target = '*' LIMIT 1",
                params![],
                |row| row.get(0),
            )
            .optional()?;

        Ok(global_enabled.unwrap_or(false))
    }

    // ===== Session 17.3: Recording & Retention =====

    /// Get the current version number for a key (create if doesn't exist)
    fn get_or_create_key_id(&self, db: i32, key: &str) -> Result<i64> {
        let conn = self.core.conn.lock().unwrap_or_else(|e| e.into_inner());

        // Try to get existing key
        if let Ok(key_id) = conn.query_row(
            "SELECT id FROM keys WHERE db = ? AND key = ? LIMIT 1",
            params![db, key],
            |row| row.get(0),
        ) {
            return Ok(key_id);
        }

        // Create new key entry if it doesn't exist
        conn.execute(
            "INSERT INTO keys (db, key, type, created_at, version) VALUES (?, ?, ?, ?, 1)",
            params![db, key, 0, Self::now_ms()],
        )?;

        let key_id = conn.last_insert_rowid();
        Ok(key_id)
    }

    /// Get the next version number for a key
    fn increment_version(&self, key_id: i64) -> Result<i64> {
        let conn = self.core.conn.lock().unwrap_or_else(|e| e.into_inner());
        let version: Option<i64> = conn
            .query_row(
                "SELECT MAX(version_num) FROM key_history WHERE key_id = ?",
                params![key_id],
                |row| row.get(0),
            )
            .optional()?;

        Ok(version.unwrap_or(0) + 1)
    }

    /// Record a history entry for an operation
    fn record_history(
        &self,
        db: i32,
        key: &str,
        operation: &str,
        data_snapshot: Option<Vec<u8>>,
    ) -> Result<()> {
        // Check if history is enabled
        if !self.is_history_enabled(key)? {
            return Ok(());
        }

        let conn = self.core.conn.lock().unwrap_or_else(|e| e.into_inner());
        let key_id = self.get_or_create_key_id(db, key)?;
        let version = self.increment_version(key_id)?;
        let timestamp_ms = Self::now_ms();

        // Get current key type from the keys table
        let key_type: i32 = conn
            .query_row(
                "SELECT type FROM keys WHERE id = ?",
                params![key_id],
                |row| row.get(0),
            )
            .unwrap_or(0);

        conn.execute(
            "INSERT INTO key_history (key_id, db, key, key_type, version_num, operation, timestamp_ms, data_snapshot)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            params![key_id, db, key, key_type, version, operation, timestamp_ms, data_snapshot],
        )?;

        // Apply retention policy
        self.apply_retention_policy(db, key)?;

        Ok(())
    }

    /// Apply retention policy to a key's history
    fn apply_retention_policy(&self, db: i32, key: &str) -> Result<()> {
        let conn = self.core.conn.lock().unwrap_or_else(|e| e.into_inner());

        // Get the key's ID
        let key_id: Option<i64> = conn
            .query_row(
                "SELECT id FROM keys WHERE db = ? AND key = ? LIMIT 1",
                params![db, key],
                |row| row.get(0),
            )
            .optional()?;

        let key_id = match key_id {
            Some(id) => id,
            None => return Ok(()),
        };

        // Check key-level retention first
        let key_target = format!("{}:{}", db, key);
        let key_retention: Option<(String, Option<i64>)> = conn
            .query_row(
                "SELECT retention_type, retention_value FROM history_config WHERE level = 'key' AND target = ?",
                params![key_target],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()?;

        if let Some((retention_type, retention_value)) = key_retention {
            match retention_type.as_str() {
                "time" => {
                    let cutoff = Self::now_ms() - retention_value.unwrap_or(0);
                    conn.execute(
                        "DELETE FROM key_history WHERE key_id = ? AND timestamp_ms < ?",
                        params![key_id, cutoff],
                    )?;
                }
                "count" => {
                    let count = retention_value.unwrap_or(100);
                    conn.execute(
                        "DELETE FROM key_history WHERE key_id = ? AND version_num <= (
                            SELECT COALESCE(MAX(version_num) - ?, 0) FROM key_history WHERE key_id = ?
                        )",
                        params![key_id, count - 1, key_id],
                    )?;
                }
                _ => {} // unlimited
            }
            return Ok(());
        }

        // Fall back to database-level retention
        let db_retention: Option<(String, Option<i64>)> = conn
            .query_row(
                "SELECT retention_type, retention_value FROM history_config WHERE level = 'database' AND target = ?",
                params![db.to_string()],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()?;

        if let Some((retention_type, retention_value)) = db_retention {
            match retention_type.as_str() {
                "time" => {
                    let cutoff = Self::now_ms() - retention_value.unwrap_or(0);
                    conn.execute(
                        "DELETE FROM key_history WHERE key_id = ? AND timestamp_ms < ?",
                        params![key_id, cutoff],
                    )?;
                }
                "count" => {
                    let count = retention_value.unwrap_or(100);
                    conn.execute(
                        "DELETE FROM key_history WHERE key_id = ? AND version_num <= (
                            SELECT COALESCE(MAX(version_num) - ?, 0) FROM key_history WHERE key_id = ?
                        )",
                        params![key_id, count - 1, key_id],
                    )?;
                }
                _ => {} // unlimited
            }
            return Ok(());
        }

        // Fall back to global retention
        let global_retention: Option<(String, Option<i64>)> = conn
            .query_row(
                "SELECT retention_type, retention_value FROM history_config WHERE level = 'global' AND target = '*'",
                params![],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()?;

        if let Some((retention_type, retention_value)) = global_retention {
            match retention_type.as_str() {
                "time" => {
                    let cutoff = Self::now_ms() - retention_value.unwrap_or(0);
                    conn.execute(
                        "DELETE FROM key_history WHERE key_id = ? AND timestamp_ms < ?",
                        params![key_id, cutoff],
                    )?;
                }
                "count" => {
                    let count = retention_value.unwrap_or(100);
                    conn.execute(
                        "DELETE FROM key_history WHERE key_id = ? AND version_num <= (
                            SELECT COALESCE(MAX(version_num) - ?, 0) FROM key_history WHERE key_id = ?
                        )",
                        params![key_id, count - 1, key_id],
                    )?;
                }
                _ => {} // unlimited
            }
        }

        Ok(())
    }

    // ===== Session 17.4: Query Methods =====

    /// Get history entries for a key with optional filters
    pub fn history_get(
        &self,
        key: &str,
        limit: Option<i64>,
        since: Option<i64>,
        until: Option<i64>,
    ) -> Result<Vec<HistoryEntry>> {
        let conn = self.core.conn.lock().unwrap_or_else(|e| e.into_inner());

        match (since, until, limit) {
            (Some(s), Some(u), Some(l)) => {
                let mut stmt = conn.prepare(
                    "SELECT id, key_id, db, key, key_type, version_num, operation, timestamp_ms, data_snapshot, expire_at
                     FROM key_history WHERE db = ? AND key = ? AND timestamp_ms >= ? AND timestamp_ms <= ?
                     ORDER BY timestamp_ms DESC LIMIT ?")?;
                Self::query_to_history_entries(&mut stmt, params![self.selected_db, key, s, u, l])
            }
            (Some(s), Some(u), None) => {
                let mut stmt = conn.prepare(
                    "SELECT id, key_id, db, key, key_type, version_num, operation, timestamp_ms, data_snapshot, expire_at
                     FROM key_history WHERE db = ? AND key = ? AND timestamp_ms >= ? AND timestamp_ms <= ?
                     ORDER BY timestamp_ms DESC")?;
                Self::query_to_history_entries(&mut stmt, params![self.selected_db, key, s, u])
            }
            (Some(s), None, Some(l)) => {
                let mut stmt = conn.prepare(
                    "SELECT id, key_id, db, key, key_type, version_num, operation, timestamp_ms, data_snapshot, expire_at
                     FROM key_history WHERE db = ? AND key = ? AND timestamp_ms >= ?
                     ORDER BY timestamp_ms DESC LIMIT ?")?;
                Self::query_to_history_entries(&mut stmt, params![self.selected_db, key, s, l])
            }
            (Some(s), None, None) => {
                let mut stmt = conn.prepare(
                    "SELECT id, key_id, db, key, key_type, version_num, operation, timestamp_ms, data_snapshot, expire_at
                     FROM key_history WHERE db = ? AND key = ? AND timestamp_ms >= ?
                     ORDER BY timestamp_ms DESC")?;
                Self::query_to_history_entries(&mut stmt, params![self.selected_db, key, s])
            }
            (None, Some(u), Some(l)) => {
                let mut stmt = conn.prepare(
                    "SELECT id, key_id, db, key, key_type, version_num, operation, timestamp_ms, data_snapshot, expire_at
                     FROM key_history WHERE db = ? AND key = ? AND timestamp_ms <= ?
                     ORDER BY timestamp_ms DESC LIMIT ?")?;
                Self::query_to_history_entries(&mut stmt, params![self.selected_db, key, u, l])
            }
            (None, Some(u), None) => {
                let mut stmt = conn.prepare(
                    "SELECT id, key_id, db, key, key_type, version_num, operation, timestamp_ms, data_snapshot, expire_at
                     FROM key_history WHERE db = ? AND key = ? AND timestamp_ms <= ?
                     ORDER BY timestamp_ms DESC")?;
                Self::query_to_history_entries(&mut stmt, params![self.selected_db, key, u])
            }
            (None, None, Some(l)) => {
                let mut stmt = conn.prepare(
                    "SELECT id, key_id, db, key, key_type, version_num, operation, timestamp_ms, data_snapshot, expire_at
                     FROM key_history WHERE db = ? AND key = ?
                     ORDER BY timestamp_ms DESC LIMIT ?")?;
                Self::query_to_history_entries(&mut stmt, params![self.selected_db, key, l])
            }
            (None, None, None) => {
                let mut stmt = conn.prepare(
                    "SELECT id, key_id, db, key, key_type, version_num, operation, timestamp_ms, data_snapshot, expire_at
                     FROM key_history WHERE db = ? AND key = ?
                     ORDER BY timestamp_ms DESC")?;
                Self::query_to_history_entries(&mut stmt, params![self.selected_db, key])
            }
        }
    }

    /// Helper to convert query results to HistoryEntry vec
    fn query_to_history_entries(
        stmt: &mut rusqlite::Statement,
        params: impl rusqlite::Params,
    ) -> Result<Vec<HistoryEntry>> {
        let entries = stmt.query_map(params, |row| {
            Ok(HistoryEntry {
                id: row.get(0)?,
                key_id: row.get(1)?,
                db: row.get(2)?,
                key: row.get(3)?,
                key_type: KeyType::from_i32(row.get(4)?).unwrap_or(KeyType::String),
                version_num: row.get(5)?,
                operation: row.get(6)?,
                timestamp_ms: row.get(7)?,
                data_snapshot: row.get(8)?,
                expire_at: row.get(9)?,
            })
        })?;

        let mut results = Vec::new();
        for entry in entries {
            results.push(entry?);
        }
        Ok(results)
    }

    /// Get the value of a key at a specific point in time (time-travel query)
    pub fn history_get_at(&self, key: &str, timestamp: i64) -> Result<Option<Vec<u8>>> {
        let conn = self.core.conn.lock().unwrap_or_else(|e| e.into_inner());

        let snapshot: Option<Vec<u8>> = conn
            .query_row(
                "SELECT data_snapshot FROM key_history
                 WHERE db = ? AND key = ? AND timestamp_ms <= ?
                 ORDER BY timestamp_ms DESC LIMIT 1",
                params![self.selected_db, key, timestamp],
                |row| row.get(0),
            )
            .optional()?;

        Ok(snapshot)
    }

    /// List keys that have history tracking enabled
    pub fn history_list_keys(&self, pattern: Option<&str>) -> Result<Vec<String>> {
        let conn = self.core.conn.lock().unwrap_or_else(|e| e.into_inner());

        let mut query = "SELECT DISTINCT key FROM key_history WHERE db = ?".to_string();

        if let Some(pat) = pattern {
            query.push_str(" AND key GLOB ?");
            let mut stmt = conn.prepare(&query)?;
            let keys = stmt.query_map(params![self.selected_db, pat], |row| row.get(0))?;

            let mut results = Vec::new();
            for key in keys {
                results.push(key?);
            }
            Ok(results)
        } else {
            let mut stmt = conn.prepare(&query)?;
            let keys = stmt.query_map(params![self.selected_db], |row| row.get(0))?;

            let mut results = Vec::new();
            for key in keys {
                results.push(key?);
            }
            Ok(results)
        }
    }

    /// Get statistics about history for a key or globally
    pub fn history_stats(&self, key: Option<&str>) -> Result<HistoryStats> {
        let conn = self.core.conn.lock().unwrap_or_else(|e| e.into_inner());

        let (total, oldest, newest, storage) = if let Some(k) = key {
            let row = conn.query_row(
                "SELECT COUNT(*), MIN(timestamp_ms), MAX(timestamp_ms), COALESCE(SUM(LENGTH(data_snapshot)), 0)
                 FROM key_history WHERE db = ? AND key = ?",
                params![self.selected_db, k],
                |row| Ok((row.get::<_, i64>(0)?, row.get::<_, Option<i64>>(1)?, row.get::<_, Option<i64>>(2)?, row.get::<_, i64>(3)?)),
            )?;
            (row.0, row.1, row.2, row.3)
        } else {
            let row = conn.query_row(
                "SELECT COUNT(*), MIN(timestamp_ms), MAX(timestamp_ms), COALESCE(SUM(LENGTH(data_snapshot)), 0)
                 FROM key_history WHERE db = ?",
                params![self.selected_db],
                |row| Ok((row.get::<_, i64>(0)?, row.get::<_, Option<i64>>(1)?, row.get::<_, Option<i64>>(2)?, row.get::<_, i64>(3)?)),
            )?;
            (row.0, row.1, row.2, row.3)
        };

        use crate::types::HistoryStats;
        Ok(HistoryStats::new(total, oldest, newest, storage))
    }

    /// Clear history entries for a key before an optional timestamp
    pub fn history_clear_key(&self, key: &str, before: Option<i64>) -> Result<i64> {
        let conn = self.core.conn.lock().unwrap_or_else(|e| e.into_inner());

        if let Some(timestamp) = before {
            conn.execute(
                "DELETE FROM key_history WHERE db = ? AND key = ? AND timestamp_ms < ?",
                params![self.selected_db, key, timestamp],
            )?;
        } else {
            conn.execute(
                "DELETE FROM key_history WHERE db = ? AND key = ?",
                params![self.selected_db, key],
            )?;
        }

        Ok(conn.changes() as i64)
    }

    /// Prune all history entries before a given timestamp
    pub fn history_prune(&self, before_timestamp: i64) -> Result<i64> {
        let conn = self.core.conn.lock().unwrap_or_else(|e| e.into_inner());

        conn.execute(
            "DELETE FROM key_history WHERE timestamp_ms < ?",
            params![before_timestamp],
        )?;

        Ok(conn.changes() as i64)
    }

    // ===== Session 24.1: Full-Text Search =====

    /// Enable FTS globally (all databases)
    pub fn fts_enable_global(&self) -> Result<()> {
        let conn = self.core.conn.lock().unwrap_or_else(|e| e.into_inner());
        conn.execute(
            "INSERT OR REPLACE INTO fts_settings (level, target, enabled) VALUES ('global', '*', 1)",
            [],
        )?;
        Ok(())
    }

    /// Enable FTS for a specific database (0-15)
    pub fn fts_enable_database(&self, db_num: i32) -> Result<()> {
        if !(0..=15).contains(&db_num) {
            return Err(KvError::SyntaxError);
        }
        let conn = self.core.conn.lock().unwrap_or_else(|e| e.into_inner());
        conn.execute(
            "INSERT OR REPLACE INTO fts_settings (level, target, enabled) VALUES ('database', ?, 1)",
            params![db_num.to_string()],
        )?;
        Ok(())
    }

    /// Enable FTS for keys matching a glob pattern
    pub fn fts_enable_pattern(&self, pattern: &str) -> Result<()> {
        let conn = self.core.conn.lock().unwrap_or_else(|e| e.into_inner());
        let target = format!("{}:{}", self.selected_db, pattern);
        conn.execute(
            "INSERT OR REPLACE INTO fts_settings (level, target, enabled) VALUES ('pattern', ?, 1)",
            params![target],
        )?;
        Ok(())
    }

    /// Enable FTS for a specific key
    pub fn fts_enable_key(&self, key: &str) -> Result<()> {
        let conn = self.core.conn.lock().unwrap_or_else(|e| e.into_inner());
        let target = format!("{}:{}", self.selected_db, key);
        conn.execute(
            "INSERT OR REPLACE INTO fts_settings (level, target, enabled) VALUES ('key', ?, 1)",
            params![target],
        )?;
        Ok(())
    }

    /// Disable FTS globally
    pub fn fts_disable_global(&self) -> Result<()> {
        let conn = self.core.conn.lock().unwrap_or_else(|e| e.into_inner());
        conn.execute(
            "UPDATE fts_settings SET enabled = 0 WHERE level = 'global' AND target = '*'",
            [],
        )?;
        Ok(())
    }

    /// Disable FTS for a specific database
    pub fn fts_disable_database(&self, db_num: i32) -> Result<()> {
        if !(0..=15).contains(&db_num) {
            return Err(KvError::SyntaxError);
        }
        let conn = self.core.conn.lock().unwrap_or_else(|e| e.into_inner());
        conn.execute(
            "UPDATE fts_settings SET enabled = 0 WHERE level = 'database' AND target = ?",
            params![db_num.to_string()],
        )?;
        Ok(())
    }

    /// Disable FTS for a specific pattern
    pub fn fts_disable_pattern(&self, pattern: &str) -> Result<()> {
        let conn = self.core.conn.lock().unwrap_or_else(|e| e.into_inner());
        let target = format!("{}:{}", self.selected_db, pattern);
        conn.execute(
            "UPDATE fts_settings SET enabled = 0 WHERE level = 'pattern' AND target = ?",
            params![target],
        )?;
        Ok(())
    }

    /// Disable FTS for a specific key
    pub fn fts_disable_key(&self, key: &str) -> Result<()> {
        let conn = self.core.conn.lock().unwrap_or_else(|e| e.into_inner());
        let target = format!("{}:{}", self.selected_db, key);
        conn.execute(
            "UPDATE fts_settings SET enabled = 0 WHERE level = 'key' AND target = ?",
            params![target],
        )?;
        Ok(())
    }

    /// Check if FTS is enabled for a specific key (four-tier lookup)
    pub fn is_fts_enabled(&self, key: &str) -> Result<bool> {
        let conn = self.core.conn.lock().unwrap_or_else(|e| e.into_inner());

        // 1. Check key-level config
        let key_target = format!("{}:{}", self.selected_db, key);
        let key_enabled: Option<bool> = conn
            .query_row(
                "SELECT enabled FROM fts_settings WHERE level = 'key' AND target = ? LIMIT 1",
                params![key_target],
                |row| row.get(0),
            )
            .optional()?;

        if let Some(enabled) = key_enabled {
            return Ok(enabled);
        }

        // 2. Check pattern-level configs (match glob patterns)
        let mut stmt = conn.prepare(
            "SELECT target, enabled FROM fts_settings WHERE level = 'pattern' AND target LIKE ?",
        )?;
        let db_prefix = format!("{}:%", self.selected_db);
        let patterns: Vec<(String, bool)> = stmt
            .query_map(params![db_prefix], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, bool>(1)?))
            })?
            .filter_map(|r| r.ok())
            .collect();

        // Check if key matches any pattern (strip db prefix from target)
        for (target, enabled) in patterns {
            if let Some(pattern) = target.strip_prefix(&format!("{}:", self.selected_db)) {
                if glob_match(pattern, key) {
                    return Ok(enabled);
                }
            }
        }

        // 3. Check database-level config
        let db_enabled: Option<bool> = conn
            .query_row(
                "SELECT enabled FROM fts_settings WHERE level = 'database' AND target = ? LIMIT 1",
                params![self.selected_db.to_string()],
                |row| row.get(0),
            )
            .optional()?;

        if let Some(enabled) = db_enabled {
            return Ok(enabled);
        }

        // 4. Fall back to global config
        let global_enabled: Option<bool> = conn
            .query_row(
                "SELECT enabled FROM fts_settings WHERE level = 'global' AND target = '*' LIMIT 1",
                params![],
                |row| row.get(0),
            )
            .optional()?;

        Ok(global_enabled.unwrap_or(false))
    }

    /// Index a key's content in FTS (internal, called on SET when FTS enabled)
    pub fn fts_index(&self, key: &str, content: &[u8]) -> Result<()> {
        // Only index if FTS is enabled for this key
        if !self.is_fts_enabled(key)? {
            return Ok(());
        }

        let conn = self.core.conn.lock().unwrap_or_else(|e| e.into_inner());

        // Get or create key_id
        let key_id: Option<i64> = conn
            .query_row(
                "SELECT id FROM keys WHERE db = ? AND key = ? LIMIT 1",
                params![self.selected_db, key],
                |row| row.get(0),
            )
            .optional()?;

        let key_id = match key_id {
            Some(id) => id,
            None => return Ok(()), // Key doesn't exist yet, will be indexed on actual SET
        };

        // Convert content to string for FTS indexing
        let content_str = String::from_utf8_lossy(content);

        // Check if already indexed
        let existing_rowid: Option<i64> = conn
            .query_row(
                "SELECT rowid FROM fts_keys WHERE db = ? AND key = ? LIMIT 1",
                params![self.selected_db, key],
                |row| row.get(0),
            )
            .optional()?;

        if let Some(rowid) = existing_rowid {
            // Update existing index
            conn.execute(
                "UPDATE fts SET key_text = ?, content = ? WHERE rowid = ?",
                params![key, content_str.as_ref(), rowid],
            )?;
        } else {
            // Insert new FTS entry
            conn.execute(
                "INSERT INTO fts (key_text, content) VALUES (?, ?)",
                params![key, content_str.as_ref()],
            )?;
            let rowid = conn.last_insert_rowid();
            // Map rowid to key_id
            conn.execute(
                "INSERT INTO fts_keys (rowid, key_id, db, key) VALUES (?, ?, ?, ?)",
                params![rowid, key_id, self.selected_db, key],
            )?;
        }

        Ok(())
    }

    /// Remove a key from the FTS index (called on DEL)
    pub fn fts_deindex(&self, key: &str) -> Result<()> {
        let conn = self.core.conn.lock().unwrap_or_else(|e| e.into_inner());

        // Get rowid from fts_keys
        let rowid: Option<i64> = conn
            .query_row(
                "SELECT rowid FROM fts_keys WHERE db = ? AND key = ? LIMIT 1",
                params![self.selected_db, key],
                |row| row.get(0),
            )
            .optional()?;

        if let Some(rowid) = rowid {
            // Delete from FTS (contentless, so just delete the metadata)
            conn.execute("DELETE FROM fts WHERE rowid = ?", params![rowid])?;
            conn.execute("DELETE FROM fts_keys WHERE rowid = ?", params![rowid])?;
        }

        Ok(())
    }

    /// Search FTS index
    pub fn fts_search(
        &self,
        query: &str,
        limit: Option<i64>,
        highlight: bool,
    ) -> Result<Vec<FtsResult>> {
        let conn = self.core.conn.lock().unwrap_or_else(|e| e.into_inner());
        let limit = limit.unwrap_or(10);

        let sql = if highlight {
            // Use highlight() function for snippets
            "SELECT fk.db, fk.key, s.value, bm25(fts) as rank,
                    highlight(fts, 1, '<b>', '</b>') as snippet
             FROM fts
             JOIN fts_keys fk ON fts.rowid = fk.rowid
             JOIN keys k ON fk.key_id = k.id
             JOIN strings s ON k.id = s.key_id
             WHERE fts MATCH ? AND fk.db = ?
             ORDER BY rank
             LIMIT ?"
        } else {
            "SELECT fk.db, fk.key, s.value, bm25(fts) as rank, NULL as snippet
             FROM fts
             JOIN fts_keys fk ON fts.rowid = fk.rowid
             JOIN keys k ON fk.key_id = k.id
             JOIN strings s ON k.id = s.key_id
             WHERE fts MATCH ? AND fk.db = ?
             ORDER BY rank
             LIMIT ?"
        };

        let mut stmt = conn.prepare(sql)?;
        let results: Vec<FtsResult> = stmt
            .query_map(params![query, self.selected_db, limit], |row| {
                let db: i32 = row.get(0)?;
                let key: String = row.get(1)?;
                let content: Vec<u8> = row.get(2)?;
                let rank: f64 = row.get(3)?;
                let snippet: Option<String> = row.get(4)?;
                let mut result = FtsResult::new(db, key, content, rank);
                result.snippet = snippet;
                Ok(result)
            })?
            .filter_map(|r| r.ok())
            .collect();

        Ok(results)
    }

    /// Reindex a specific key (force re-indexing)
    pub fn fts_reindex_key(&self, key: &str) -> Result<bool> {
        // Get current value
        let value = self.get(key)?;
        if let Some(v) = value {
            // Remove and re-add to FTS
            self.fts_deindex(key)?;
            // Temporarily force-enable for this operation
            let conn = self.core.conn.lock().unwrap_or_else(|e| e.into_inner());

            // Get key_id
            let key_id: Option<i64> = conn
                .query_row(
                    "SELECT id FROM keys WHERE db = ? AND key = ? LIMIT 1",
                    params![self.selected_db, key],
                    |row| row.get(0),
                )
                .optional()?;

            if let Some(key_id) = key_id {
                let content_str = String::from_utf8_lossy(&v);
                conn.execute(
                    "INSERT INTO fts (key_text, content) VALUES (?, ?)",
                    params![key, content_str.as_ref()],
                )?;
                let rowid = conn.last_insert_rowid();
                conn.execute(
                    "INSERT INTO fts_keys (rowid, key_id, db, key) VALUES (?, ?, ?, ?)",
                    params![rowid, key_id, self.selected_db, key],
                )?;
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// Get FTS statistics
    pub fn fts_info(&self) -> Result<FtsStats> {
        let conn = self.core.conn.lock().unwrap_or_else(|e| e.into_inner());

        // Count indexed keys in current database
        let indexed_keys: i64 = conn.query_row(
            "SELECT COUNT(*) FROM fts_keys WHERE db = ?",
            params![self.selected_db],
            |row| row.get(0),
        )?;

        // Get total tokens (approximate via FTS metadata if available)
        // FTS5 doesn't expose token count directly, so we estimate
        let total_tokens: i64 = 0; // Placeholder - would need custom implementation

        // Get storage bytes (approximate)
        let storage_bytes: i64 = conn
            .query_row(
                "SELECT page_count * page_size FROM pragma_page_count(), pragma_page_size()",
                [],
                |row| row.get(0),
            )
            .unwrap_or(0);

        let mut stats = FtsStats::new(indexed_keys, total_tokens, storage_bytes);

        // Get all FTS configs for current db
        let mut stmt = conn.prepare(
            "SELECT id, level, target, enabled, created_at FROM fts_settings ORDER BY level, target"
        )?;
        let configs: Vec<crate::types::FtsConfig> = stmt
            .query_map([], |row| {
                let id: i64 = row.get(0)?;
                let level_str: String = row.get(1)?;
                let target: String = row.get(2)?;
                let enabled: bool = row.get(3)?;
                let created_at: i64 = row.get(4)?;

                let level = match level_str.as_str() {
                    "global" => FtsLevel::Global,
                    "database" => {
                        let db_num: i32 = target.parse().unwrap_or(0);
                        FtsLevel::Database(db_num)
                    }
                    "pattern" => FtsLevel::Pattern(target.clone()),
                    "key" => FtsLevel::Key,
                    _ => FtsLevel::Global,
                };

                Ok(crate::types::FtsConfig {
                    id,
                    level,
                    target,
                    enabled,
                    created_at,
                })
            })?
            .filter_map(|r| r.ok())
            .collect();

        stats.configs = configs;
        Ok(stats)
    }

    // ============================================================================
    // RediSearch-Compatible Methods (Session 23)
    // ============================================================================

    /// Create a RediSearch-compatible index
    /// FT.CREATE index ON HASH|JSON PREFIX n prefix... SCHEMA field type...
    pub fn ft_create(
        &self,
        name: &str,
        on_type: crate::types::FtOnType,
        prefixes: &[&str],
        schema: &[crate::types::FtField],
    ) -> Result<()> {
        use crate::types::FtFieldType;

        let conn = self.core.conn.lock().unwrap_or_else(|e| e.into_inner());

        // Check if index already exists
        let exists: bool = conn
            .query_row(
                "SELECT 1 FROM ft_indexes WHERE name = ? LIMIT 1",
                params![name],
                |_| Ok(true),
            )
            .optional()?
            .unwrap_or(false);

        if exists {
            return Err(KvError::Other(format!("Index already exists: {}", name)));
        }

        // Serialize prefixes and schema to JSON
        let prefixes_json =
            serde_json::to_string(&prefixes.iter().map(|s| s.to_string()).collect::<Vec<_>>())
                .map_err(|e| KvError::Other(e.to_string()))?;

        let schema_json: Vec<serde_json::Value> = schema
            .iter()
            .map(|f| {
                serde_json::json!({
                    "name": f.name,
                    "type": f.field_type.as_str(),
                    "sortable": f.sortable,
                    "noindex": f.noindex,
                    "nostem": f.nostem,
                    "weight": f.weight,
                    "separator": f.separator.to_string(),
                    "case_sensitive": f.case_sensitive,
                })
            })
            .collect();
        let schema_str =
            serde_json::to_string(&schema_json).map_err(|e| KvError::Other(e.to_string()))?;

        // Insert index definition
        conn.execute(
            "INSERT INTO ft_indexes (name, on_type, prefixes, schema, language, created_at)
             VALUES (?, ?, ?, ?, 'english', ?)",
            params![
                name,
                on_type.as_str(),
                prefixes_json,
                schema_str,
                Self::now_ms()
            ],
        )?;

        let index_id = conn.last_insert_rowid();

        // Create dynamic FTS5 table for this index's TEXT fields
        let text_fields: Vec<&str> = schema
            .iter()
            .filter(|f| matches!(f.field_type, FtFieldType::Text))
            .map(|f| f.name.as_str())
            .collect();

        if !text_fields.is_empty() {
            // Create FTS5 table with columns for each TEXT field
            let columns: Vec<String> = text_fields.iter().map(|f| format!("\"{}\"", f)).collect();
            let create_fts = format!(
                "CREATE VIRTUAL TABLE IF NOT EXISTS fts_idx_{} USING fts5({}, content='', contentless_delete=1, tokenize='porter unicode61')",
                index_id,
                columns.join(", ")
            );
            conn.execute(&create_fts, [])?;
        }

        Ok(())
    }

    /// Drop a RediSearch index
    /// FT.DROPINDEX index [DD]
    pub fn ft_dropindex(&self, name: &str, delete_docs: bool) -> Result<bool> {
        let conn = self.core.conn.lock().unwrap_or_else(|e| e.into_inner());

        // Get index ID
        let index_id: Option<i64> = conn
            .query_row(
                "SELECT id FROM ft_indexes WHERE name = ? LIMIT 1",
                params![name],
                |row| row.get(0),
            )
            .optional()?;

        let index_id = match index_id {
            Some(id) => id,
            None => return Ok(false),
        };

        // Drop the dynamic FTS5 table if it exists
        let drop_fts = format!("DROP TABLE IF EXISTS fts_idx_{}", index_id);
        conn.execute(&drop_fts, [])?;

        // Delete indexed docs if DD flag is set
        if delete_docs {
            // Get all keys that were indexed
            let mut stmt = conn.prepare(
                "SELECT k.key FROM ft_indexed_docs d
                 JOIN keys k ON d.key_id = k.id
                 WHERE d.index_id = ?",
            )?;
            let keys: Vec<String> = stmt
                .query_map(params![index_id], |row| row.get(0))?
                .filter_map(|r| r.ok())
                .collect();

            // Delete the keys
            for key in &keys {
                self.del(&[key.as_str()])?;
            }
        }

        // Delete index (cascade deletes ft_indexed_docs, ft_numeric_fields, ft_tag_fields)
        conn.execute("DELETE FROM ft_indexes WHERE id = ?", params![index_id])?;

        Ok(true)
    }

    /// List all RediSearch indexes
    /// FT._LIST
    pub fn ft_list(&self) -> Result<Vec<String>> {
        let conn = self.core.conn.lock().unwrap_or_else(|e| e.into_inner());

        let mut stmt = conn.prepare("SELECT name FROM ft_indexes ORDER BY name")?;
        let names: Vec<String> = stmt
            .query_map([], |row| row.get(0))?
            .filter_map(|r| r.ok())
            .collect();

        Ok(names)
    }

    /// Get info about a RediSearch index
    /// FT.INFO index
    pub fn ft_info(&self, name: &str) -> Result<Option<crate::types::FtIndexInfo>> {
        use crate::types::{FtField, FtFieldType, FtIndexInfo, FtOnType};

        let conn = self.core.conn.lock().unwrap_or_else(|e| e.into_inner());

        // Get index definition
        let result: Option<(i64, String, String, String, String, i64)> = conn
            .query_row(
                "SELECT id, name, on_type, prefixes, schema, created_at
                 FROM ft_indexes WHERE name = ? LIMIT 1",
                params![name],
                |row| {
                    Ok((
                        row.get(0)?,
                        row.get(1)?,
                        row.get(2)?,
                        row.get(3)?,
                        row.get(4)?,
                        row.get(5)?,
                    ))
                },
            )
            .optional()?;

        let (index_id, index_name, on_type_str, prefixes_json, schema_json, _created_at) =
            match result {
                Some(r) => r,
                None => return Ok(None),
            };

        // Parse on_type
        let on_type = FtOnType::from_str(&on_type_str).unwrap_or(FtOnType::Hash);

        // Parse prefixes
        let prefixes: Vec<String> = serde_json::from_str(&prefixes_json).unwrap_or_default();

        // Parse schema
        let schema_values: Vec<serde_json::Value> =
            serde_json::from_str(&schema_json).unwrap_or_default();
        let schema: Vec<FtField> = schema_values
            .iter()
            .filter_map(|v| {
                let name = v.get("name")?.as_str()?;
                let type_str = v.get("type")?.as_str()?;
                let field_type = FtFieldType::from_str(type_str)?;
                let mut field = FtField::new(name, field_type);
                if let Some(sortable) = v.get("sortable").and_then(|v| v.as_bool()) {
                    field.sortable = sortable;
                }
                if let Some(noindex) = v.get("noindex").and_then(|v| v.as_bool()) {
                    field.noindex = noindex;
                }
                if let Some(nostem) = v.get("nostem").and_then(|v| v.as_bool()) {
                    field.nostem = nostem;
                }
                if let Some(weight) = v.get("weight").and_then(|v| v.as_f64()) {
                    field.weight = weight;
                }
                if let Some(sep) = v
                    .get("separator")
                    .and_then(|v| v.as_str())
                    .and_then(|s| s.chars().next())
                {
                    field.separator = sep;
                }
                if let Some(case_sensitive) = v.get("case_sensitive").and_then(|v| v.as_bool()) {
                    field.case_sensitive = case_sensitive;
                }
                Some(field)
            })
            .collect();

        // Count indexed documents
        let num_docs: i64 = conn.query_row(
            "SELECT COUNT(*) FROM ft_indexed_docs WHERE index_id = ?",
            params![index_id],
            |row| row.get(0),
        )?;

        // Get max doc ID
        let max_doc_id: i64 = conn.query_row(
            "SELECT COALESCE(MAX(id), 0) FROM ft_indexed_docs WHERE index_id = ?",
            params![index_id],
            |row| row.get(0),
        )?;

        Ok(Some(FtIndexInfo {
            name: index_name,
            on_type,
            prefixes,
            schema,
            num_docs,
            num_terms: 0,   // Would need FTS5 vocab query
            num_records: 0, // Would need FTS5 metadata
            inverted_sz_mb: 0.0,
            total_inverted_index_blocks: 0,
            max_doc_id,
        }))
    }

    /// Add a field to an existing index
    /// FT.ALTER index SCHEMA ADD field type [options...]
    pub fn ft_alter(&self, name: &str, field: crate::types::FtField) -> Result<()> {
        let conn = self.core.conn.lock().unwrap_or_else(|e| e.into_inner());

        // Get index
        let result: Option<(i64, String)> = conn
            .query_row(
                "SELECT id, schema FROM ft_indexes WHERE name = ? LIMIT 1",
                params![name],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()?;

        let (index_id, schema_json) = match result {
            Some(r) => r,
            None => return Err(KvError::Other(format!("Unknown index: {}", name))),
        };

        // Parse existing schema
        let mut schema_values: Vec<serde_json::Value> =
            serde_json::from_str(&schema_json).unwrap_or_default();

        // Check if field already exists
        let field_exists = schema_values
            .iter()
            .any(|v| v.get("name").and_then(|n| n.as_str()) == Some(&field.name));

        if field_exists {
            return Err(KvError::Other(format!(
                "Field already exists: {}",
                field.name
            )));
        }

        // Add new field to schema
        schema_values.push(serde_json::json!({
            "name": field.name,
            "type": field.field_type.as_str(),
            "sortable": field.sortable,
            "noindex": field.noindex,
            "nostem": field.nostem,
            "weight": field.weight,
            "separator": field.separator.to_string(),
            "case_sensitive": field.case_sensitive,
        }));

        let new_schema_json =
            serde_json::to_string(&schema_values).map_err(|e| KvError::Other(e.to_string()))?;

        // Update index schema
        conn.execute(
            "UPDATE ft_indexes SET schema = ? WHERE id = ?",
            params![new_schema_json, index_id],
        )?;

        // If it's a TEXT field, we need to alter the FTS5 table (SQLite limitation: can't easily add columns to FTS5)
        // For now, we'll need to recreate the index or handle this differently
        // This is a known limitation of FTS5

        Ok(())
    }

    /// Add or update an alias for an index
    /// FT.ALIASADD alias index
    pub fn ft_aliasadd(&self, alias: &str, index_name: &str) -> Result<()> {
        let conn = self.core.conn.lock().unwrap_or_else(|e| e.into_inner());

        // Get index ID
        let index_id: Option<i64> = conn
            .query_row(
                "SELECT id FROM ft_indexes WHERE name = ? LIMIT 1",
                params![index_name],
                |row| row.get(0),
            )
            .optional()?;

        let index_id = match index_id {
            Some(id) => id,
            None => return Err(KvError::Other(format!("Unknown index: {}", index_name))),
        };

        conn.execute(
            "INSERT OR REPLACE INTO ft_aliases (alias, index_id) VALUES (?, ?)",
            params![alias, index_id],
        )?;

        Ok(())
    }

    /// Delete an alias
    /// FT.ALIASDEL alias
    pub fn ft_aliasdel(&self, alias: &str) -> Result<bool> {
        let conn = self.core.conn.lock().unwrap_or_else(|e| e.into_inner());
        conn.execute("DELETE FROM ft_aliases WHERE alias = ?", params![alias])?;
        Ok(conn.changes() > 0)
    }

    /// Update an alias to point to a different index
    /// FT.ALIASUPDATE alias index
    pub fn ft_aliasupdate(&self, alias: &str, index_name: &str) -> Result<()> {
        // Same as aliasadd (INSERT OR REPLACE)
        self.ft_aliasadd(alias, index_name)
    }

    /// Add a synonym group
    /// FT.SYNUPDATE index group_id term...
    pub fn ft_synupdate(&self, index_name: &str, group_id: &str, terms: &[&str]) -> Result<()> {
        let conn = self.core.conn.lock().unwrap_or_else(|e| e.into_inner());

        // Get index ID
        let index_id: Option<i64> = conn
            .query_row(
                "SELECT id FROM ft_indexes WHERE name = ? LIMIT 1",
                params![index_name],
                |row| row.get(0),
            )
            .optional()?;

        let index_id = match index_id {
            Some(id) => id,
            None => return Err(KvError::Other(format!("Unknown index: {}", index_name))),
        };

        // Insert terms
        for term in terms {
            conn.execute(
                "INSERT OR IGNORE INTO ft_synonyms (index_id, group_id, term) VALUES (?, ?, ?)",
                params![index_id, group_id, term],
            )?;
        }

        Ok(())
    }

    /// Dump all synonym groups for an index
    /// FT.SYNDUMP index
    pub fn ft_syndump(&self, index_name: &str) -> Result<Vec<(String, Vec<String>)>> {
        let conn = self.core.conn.lock().unwrap_or_else(|e| e.into_inner());

        // Get index ID
        let index_id: Option<i64> = conn
            .query_row(
                "SELECT id FROM ft_indexes WHERE name = ? LIMIT 1",
                params![index_name],
                |row| row.get(0),
            )
            .optional()?;

        let index_id = match index_id {
            Some(id) => id,
            None => return Err(KvError::Other(format!("Unknown index: {}", index_name))),
        };

        // Get all synonyms grouped by group_id
        let mut stmt = conn.prepare(
            "SELECT group_id, term FROM ft_synonyms WHERE index_id = ? ORDER BY group_id, term",
        )?;

        let rows: Vec<(String, String)> = stmt
            .query_map(params![index_id], |row| Ok((row.get(0)?, row.get(1)?)))?
            .filter_map(|r| r.ok())
            .collect();

        // Group by group_id
        let mut result: Vec<(String, Vec<String>)> = Vec::new();
        let mut current_group: Option<String> = None;
        let mut current_terms: Vec<String> = Vec::new();

        for (group_id, term) in rows {
            if Some(&group_id) != current_group.as_ref() {
                if let Some(g) = current_group.take() {
                    result.push((g, std::mem::take(&mut current_terms)));
                }
                current_group = Some(group_id);
            }
            current_terms.push(term);
        }

        if let Some(g) = current_group {
            result.push((g, current_terms));
        }

        Ok(result)
    }

    /// Add a suggestion to an autocomplete dictionary
    /// FT.SUGADD key string score [PAYLOAD payload]
    pub fn ft_sugadd(
        &self,
        key: &str,
        string: &str,
        score: f64,
        payload: Option<&str>,
    ) -> Result<i64> {
        let conn = self.core.conn.lock().unwrap_or_else(|e| e.into_inner());

        conn.execute(
            "INSERT OR REPLACE INTO ft_suggestions (key, string, score, payload) VALUES (?, ?, ?, ?)",
            params![key, string, score, payload],
        )?;

        // Return total count for this key
        let count: i64 = conn.query_row(
            "SELECT COUNT(*) FROM ft_suggestions WHERE key = ?",
            params![key],
            |row| row.get(0),
        )?;

        Ok(count)
    }

    /// Get suggestions from an autocomplete dictionary
    /// FT.SUGGET key prefix [FUZZY] [WITHSCORES] [WITHPAYLOADS] [MAX n]
    pub fn ft_sugget(
        &self,
        key: &str,
        prefix: &str,
        fuzzy: bool,
        max: i64,
    ) -> Result<Vec<crate::types::FtSuggestion>> {
        use crate::types::FtSuggestion;

        let conn = self.core.conn.lock().unwrap_or_else(|e| e.into_inner());

        // For fuzzy matching, we'd need trigrams or Levenshtein distance
        // For now, implement prefix matching
        let pattern = if fuzzy {
            format!("%{}%", prefix) // Simple contains for fuzzy
        } else {
            format!("{}%", prefix) // Prefix match
        };

        let mut stmt = conn.prepare(
            "SELECT string, score, payload FROM ft_suggestions
             WHERE key = ? AND string LIKE ?
             ORDER BY score DESC
             LIMIT ?",
        )?;

        let suggestions: Vec<FtSuggestion> = stmt
            .query_map(params![key, pattern, max], |row| {
                Ok(FtSuggestion {
                    string: row.get(0)?,
                    score: row.get(1)?,
                    payload: row.get(2)?,
                })
            })?
            .filter_map(|r| r.ok())
            .collect();

        Ok(suggestions)
    }

    /// Delete a suggestion from an autocomplete dictionary
    /// FT.SUGDEL key string
    pub fn ft_sugdel(&self, key: &str, string: &str) -> Result<bool> {
        let conn = self.core.conn.lock().unwrap_or_else(|e| e.into_inner());
        conn.execute(
            "DELETE FROM ft_suggestions WHERE key = ? AND string = ?",
            params![key, string],
        )?;
        Ok(conn.changes() > 0)
    }

    /// Get the size of an autocomplete dictionary
    /// FT.SUGLEN key
    pub fn ft_suglen(&self, key: &str) -> Result<i64> {
        let conn = self.core.conn.lock().unwrap_or_else(|e| e.into_inner());
        let count: i64 = conn.query_row(
            "SELECT COUNT(*) FROM ft_suggestions WHERE key = ?",
            params![key],
            |row| row.get(0),
        )?;
        Ok(count)
    }

    /// Search a RediSearch index
    /// FT.SEARCH index query [NOCONTENT] [VERBATIM] [NOSTOPWORDS] [WITHSCORES]
    ///           [WITHPAYLOADS] [LIMIT offset num] [SORTBY field [ASC|DESC]]
    ///           [RETURN count field ...] [HIGHLIGHT] [SUMMARIZE]
    pub fn ft_search(
        &self,
        index_name: &str,
        query: &str,
        options: &crate::types::FtSearchOptions,
    ) -> Result<(i64, Vec<crate::types::FtSearchResult>)> {
        use crate::search::{parse_query, NumericBound};
        use crate::types::{FtField, FtFieldType, FtOnType, FtSearchResult};

        let conn = self.core.conn.lock().unwrap_or_else(|e| e.into_inner());

        // Resolve index name (could be an alias)
        let index_id: Option<i64> = conn
            .query_row(
                "SELECT id FROM ft_indexes WHERE name = ? LIMIT 1",
                params![index_name],
                |row| row.get(0),
            )
            .optional()?;

        let index_id = match index_id {
            Some(id) => id,
            None => {
                // Try alias
                let alias_index_id: Option<i64> = conn
                    .query_row(
                        "SELECT index_id FROM ft_aliases WHERE alias = ? LIMIT 1",
                        params![index_name],
                        |row| row.get(0),
                    )
                    .optional()?;
                match alias_index_id {
                    Some(id) => id,
                    None => return Err(KvError::Other(format!("Unknown index: {}", index_name))),
                }
            }
        };

        // Get index definition
        let (on_type_str, prefixes_json, schema_json): (String, String, String) = conn.query_row(
            "SELECT on_type, prefixes, schema FROM ft_indexes WHERE id = ?",
            params![index_id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )?;

        let _on_type = FtOnType::from_str(&on_type_str).unwrap_or(FtOnType::Hash);
        let prefixes: Vec<String> = serde_json::from_str(&prefixes_json).unwrap_or_default();
        let schema_values: Vec<serde_json::Value> =
            serde_json::from_str(&schema_json).unwrap_or_default();

        // Parse schema to get field names and types
        let schema: Vec<FtField> = schema_values
            .iter()
            .filter_map(|v| {
                let name = v.get("name")?.as_str()?;
                let type_str = v.get("type")?.as_str()?;
                let field_type = FtFieldType::from_str(type_str)?;
                let mut field = FtField::new(name, field_type);
                if let Some(sortable) = v.get("sortable").and_then(|v| v.as_bool()) {
                    field.sortable = sortable;
                }
                if let Some(noindex) = v.get("noindex").and_then(|v| v.as_bool()) {
                    field.noindex = noindex;
                }
                if let Some(nostem) = v.get("nostem").and_then(|v| v.as_bool()) {
                    field.nostem = nostem;
                }
                if let Some(weight) = v.get("weight").and_then(|v| v.as_f64()) {
                    field.weight = weight;
                }
                Some(field)
            })
            .collect();

        // Get text field names for FTS5
        let text_fields: Vec<&str> = schema
            .iter()
            .filter(|f| matches!(f.field_type, FtFieldType::Text) && !f.noindex)
            .map(|f| f.name.as_str())
            .collect();

        // Parse the query
        let parsed = parse_query(query, options.verbatim)
            .map_err(|e| KvError::Other(format!("Query parse error: {}", e)))?;

        // Find matching keys with the index prefixes
        // For now, scan all hashes matching the prefixes
        let mut all_matching_keys: Vec<(i64, String)> = Vec::new();
        let db = self.selected_db;
        let now = Self::now_ms();

        for prefix in &prefixes {
            let like_pattern = format!("{}%", prefix.replace('%', "\\%").replace('_', "\\_"));
            let mut stmt = conn.prepare(
                "SELECT id, key FROM keys WHERE db = ? AND key LIKE ? ESCAPE '\\' AND type = 2
                 AND (expire_at IS NULL OR expire_at > ?)",
            )?;
            let rows = stmt.query_map(params![db, like_pattern, now], |row| {
                Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?))
            })?;
            for row in rows {
                if let Ok((key_id, key_name)) = row {
                    all_matching_keys.push((key_id, key_name));
                }
            }
        }

        // If no matching keys, return empty
        if all_matching_keys.is_empty() {
            return Ok((0, Vec::new()));
        }

        // For each key, check if it matches the query
        let mut results: Vec<FtSearchResult> = Vec::new();

        for (key_id, key_name) in &all_matching_keys {
            // Get all fields for this hash
            let mut stmt = conn.prepare("SELECT field, value FROM hashes WHERE key_id = ?")?;
            let field_rows = stmt.query_map(params![key_id], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, Vec<u8>>(1)?))
            })?;

            let mut fields: HashMap<String, Vec<u8>> = HashMap::new();
            for row in field_rows {
                if let Ok((field, value)) = row {
                    fields.insert(field, value);
                }
            }

            // Check numeric filters
            let mut passes_numeric = true;
            for filter in &parsed.numeric_filters {
                if let Some(value_bytes) = fields.get(&filter.field) {
                    if let Ok(value_str) = std::str::from_utf8(value_bytes) {
                        if let Ok(value) = value_str.parse::<f64>() {
                            let min_ok = match &filter.min {
                                NumericBound::Inclusive(min) => value >= *min,
                                NumericBound::Exclusive(min) => value > *min,
                                NumericBound::Unbounded => true,
                            };
                            let max_ok = match &filter.max {
                                NumericBound::Inclusive(max) => value <= *max,
                                NumericBound::Exclusive(max) => value < *max,
                                NumericBound::Unbounded => true,
                            };
                            if !min_ok || !max_ok {
                                passes_numeric = false;
                                break;
                            }
                        } else {
                            passes_numeric = false;
                            break;
                        }
                    } else {
                        passes_numeric = false;
                        break;
                    }
                } else {
                    passes_numeric = false;
                    break;
                }
            }

            if !passes_numeric {
                continue;
            }

            // Check tag filters
            let mut passes_tag = true;
            for filter in &parsed.tag_filters {
                if let Some(value_bytes) = fields.get(&filter.field) {
                    if let Ok(value_str) = std::str::from_utf8(value_bytes) {
                        // Tags are comma-separated by default
                        let doc_tags: Vec<&str> = value_str.split(',').map(|s| s.trim()).collect();
                        let matches_any = filter
                            .tags
                            .iter()
                            .any(|t| doc_tags.iter().any(|dt| dt.eq_ignore_ascii_case(t)));
                        if !matches_any {
                            passes_tag = false;
                            break;
                        }
                    } else {
                        passes_tag = false;
                        break;
                    }
                } else {
                    passes_tag = false;
                    break;
                }
            }

            if !passes_tag {
                continue;
            }

            // Check text search (FTS5 or in-memory)
            let mut passes_text = true;
            let mut score = 1.0;

            if let Some(fts_query) = &parsed.fts_query {
                // Try FTS5 search on the index's table
                let fts_table = format!("fts_idx_{}", index_id);

                // Check if this document is in the FTS5 table by rowid
                // For now, do in-memory text matching as a fallback
                // Build searchable content from text fields
                let mut searchable_content = String::new();
                for field_name in &text_fields {
                    if let Some(value_bytes) = fields.get(*field_name) {
                        if let Ok(value_str) = std::str::from_utf8(value_bytes) {
                            searchable_content.push_str(value_str);
                            searchable_content.push(' ');
                        }
                    }
                }

                // Simple in-memory text matching
                // This is a fallback - proper FTS5 search should be used when documents are indexed
                passes_text = self.simple_text_match(&searchable_content, fts_query);

                // Calculate a simple score based on term frequency
                if passes_text {
                    score = self.calculate_simple_score(&searchable_content, fts_query);
                }
            }

            if !passes_text {
                continue;
            }

            // Build result
            let mut result = FtSearchResult::new(key_name.clone(), score);

            if !options.nocontent {
                // Determine which fields to return
                let fields_to_return: Vec<&String> = if options.return_fields.is_empty() {
                    fields.keys().collect()
                } else {
                    options
                        .return_fields
                        .iter()
                        .filter(|f| fields.contains_key(*f))
                        .collect()
                };

                for field_name in fields_to_return {
                    if let Some(value) = fields.get(field_name) {
                        result.fields.push((field_name.clone(), value.clone()));
                    }
                }
            }

            results.push(result);
        }

        // Sort results
        if let Some((sort_field, ascending)) = &options.sortby {
            results.sort_by(|a, b| {
                let a_val = a
                    .fields
                    .iter()
                    .find(|(f, _)| f == sort_field)
                    .map(|(_, v)| v);
                let b_val = b
                    .fields
                    .iter()
                    .find(|(f, _)| f == sort_field)
                    .map(|(_, v)| v);

                let cmp = match (a_val, b_val) {
                    (Some(av), Some(bv)) => {
                        // Try numeric comparison first
                        let a_num = std::str::from_utf8(av)
                            .ok()
                            .and_then(|s| s.parse::<f64>().ok());
                        let b_num = std::str::from_utf8(bv)
                            .ok()
                            .and_then(|s| s.parse::<f64>().ok());
                        match (a_num, b_num) {
                            (Some(an), Some(bn)) => {
                                an.partial_cmp(&bn).unwrap_or(std::cmp::Ordering::Equal)
                            }
                            _ => av.cmp(bv),
                        }
                    }
                    (Some(_), None) => std::cmp::Ordering::Less,
                    (None, Some(_)) => std::cmp::Ordering::Greater,
                    (None, None) => std::cmp::Ordering::Equal,
                };

                if *ascending {
                    cmp
                } else {
                    cmp.reverse()
                }
            });
        } else if options.withscores {
            // Sort by score descending by default
            results.sort_by(|a, b| {
                b.score
                    .partial_cmp(&a.score)
                    .unwrap_or(std::cmp::Ordering::Equal)
            });
        }

        let total_count = results.len() as i64;

        // Apply pagination
        let offset = options.limit_offset as usize;
        let limit = options.limit_num as usize;
        let paginated: Vec<FtSearchResult> = results.into_iter().skip(offset).take(limit).collect();

        Ok((total_count, paginated))
    }

    /// Simple in-memory text matching for FTS5 queries
    /// This is a fallback when documents are not indexed in FTS5
    fn simple_text_match(&self, content: &str, query: &str) -> bool {
        let content_lower = content.to_lowercase();

        // Simple parsing of FTS5 query patterns
        // Handle AND, OR, NOT, phrases, and prefixes

        // Remove parentheses for simple matching
        let query_clean = query.replace('(', " ").replace(')', " ").replace("\"", "");

        // Split into terms
        let terms: Vec<&str> = query_clean.split_whitespace().collect();

        let mut i = 0;
        let mut result = true;
        let mut current_op = "AND";

        while i < terms.len() {
            let term = terms[i];

            match term.to_uppercase().as_str() {
                "AND" => {
                    current_op = "AND";
                    i += 1;
                    continue;
                }
                "OR" => {
                    current_op = "OR";
                    i += 1;
                    continue;
                }
                "NOT" => {
                    i += 1;
                    if i < terms.len() {
                        let not_term = terms[i].to_lowercase();
                        let not_term_clean = not_term.trim_end_matches('*');
                        if not_term.ends_with('*') {
                            // Prefix NOT
                            if content_lower.contains(not_term_clean) {
                                return false;
                            }
                        } else if content_lower.contains(&not_term) {
                            return false;
                        }
                    }
                    i += 1;
                    continue;
                }
                _ => {}
            }

            // Handle field-scoped terms like "field":term
            let actual_term = if term.contains(':') {
                term.split(':').last().unwrap_or(term)
            } else {
                term
            };

            let term_lower = actual_term.to_lowercase();

            // Handle prefix search
            let matches = if term_lower.ends_with('*') {
                let prefix = term_lower.trim_end_matches('*');
                content_lower
                    .split_whitespace()
                    .any(|w| w.starts_with(prefix))
            } else {
                content_lower.contains(&term_lower)
            };

            match current_op {
                "AND" => result = result && matches,
                "OR" => result = result || matches,
                _ => result = result && matches,
            }

            current_op = "AND"; // Reset to default
            i += 1;
        }

        result
    }

    /// Calculate a simple relevance score based on term frequency
    fn calculate_simple_score(&self, content: &str, query: &str) -> f64 {
        let content_lower = content.to_lowercase();
        let words: Vec<&str> = content_lower.split_whitespace().collect();
        let word_count = words.len() as f64;

        if word_count == 0.0 {
            return 0.0;
        }

        // Extract terms from query (simplified)
        let query_clean = query
            .replace('(', " ")
            .replace(')', " ")
            .replace("\"", "")
            .to_lowercase();

        let terms: Vec<&str> = query_clean
            .split_whitespace()
            .filter(|t| {
                let upper = t.to_uppercase();
                upper != "AND" && upper != "OR" && upper != "NOT"
            })
            .collect();

        let mut total_freq = 0.0;
        for term in &terms {
            let term_clean = term.trim_end_matches('*');
            if term_clean.is_empty() {
                continue;
            }

            // Count occurrences
            let count = if term.ends_with('*') {
                words.iter().filter(|w| w.starts_with(term_clean)).count()
            } else {
                words.iter().filter(|w| **w == term_clean).count()
            };

            total_freq += count as f64;
        }

        // Simple TF score
        total_freq / word_count
    }

    // ============================================================================
    // Vector Search Methods (Session 24.2) - Feature-gated
    // ============================================================================

    /// Enable vectors globally with specified dimensions
    #[cfg(feature = "vectors")]
    pub fn vector_enable_global(&self, dimensions: i32) -> Result<()> {
        if dimensions <= 0 {
            return Err(KvError::SyntaxError);
        }
        let conn = self.core.conn.lock().unwrap_or_else(|e| e.into_inner());
        conn.execute(
            "INSERT OR REPLACE INTO vector_settings (level, target, enabled, dimensions) VALUES ('global', '*', 1, ?)",
            params![dimensions],
        )?;
        Ok(())
    }

    /// Enable vectors for a specific database (0-15) with specified dimensions
    #[cfg(feature = "vectors")]
    pub fn vector_enable_database(&self, db_num: i32, dimensions: i32) -> Result<()> {
        if !(0..=15).contains(&db_num) || dimensions <= 0 {
            return Err(KvError::SyntaxError);
        }
        let conn = self.core.conn.lock().unwrap_or_else(|e| e.into_inner());
        conn.execute(
            "INSERT OR REPLACE INTO vector_settings (level, target, enabled, dimensions) VALUES ('database', ?, 1, ?)",
            params![db_num.to_string(), dimensions],
        )?;
        Ok(())
    }

    /// Enable vectors for keys matching a glob pattern with specified dimensions
    #[cfg(feature = "vectors")]
    pub fn vector_enable_pattern(&self, pattern: &str, dimensions: i32) -> Result<()> {
        if dimensions <= 0 {
            return Err(KvError::SyntaxError);
        }
        let conn = self.core.conn.lock().unwrap_or_else(|e| e.into_inner());
        let target = format!("{}:{}", self.selected_db, pattern);
        conn.execute(
            "INSERT OR REPLACE INTO vector_settings (level, target, enabled, dimensions) VALUES ('pattern', ?, 1, ?)",
            params![target, dimensions],
        )?;
        Ok(())
    }

    /// Enable vectors for a specific key with specified dimensions
    #[cfg(feature = "vectors")]
    pub fn vector_enable_key(&self, key: &str, dimensions: i32) -> Result<()> {
        if dimensions <= 0 {
            return Err(KvError::SyntaxError);
        }
        let conn = self.core.conn.lock().unwrap_or_else(|e| e.into_inner());
        let target = format!("{}:{}", self.selected_db, key);
        conn.execute(
            "INSERT OR REPLACE INTO vector_settings (level, target, enabled, dimensions) VALUES ('key', ?, 1, ?)",
            params![target, dimensions],
        )?;
        Ok(())
    }

    /// Disable vectors globally
    #[cfg(feature = "vectors")]
    pub fn vector_disable_global(&self) -> Result<()> {
        let conn = self.core.conn.lock().unwrap_or_else(|e| e.into_inner());
        conn.execute(
            "UPDATE vector_settings SET enabled = 0 WHERE level = 'global' AND target = '*'",
            [],
        )?;
        Ok(())
    }

    /// Disable vectors for a specific database
    #[cfg(feature = "vectors")]
    pub fn vector_disable_database(&self, db_num: i32) -> Result<()> {
        if !(0..=15).contains(&db_num) {
            return Err(KvError::SyntaxError);
        }
        let conn = self.core.conn.lock().unwrap_or_else(|e| e.into_inner());
        conn.execute(
            "UPDATE vector_settings SET enabled = 0 WHERE level = 'database' AND target = ?",
            params![db_num.to_string()],
        )?;
        Ok(())
    }

    /// Disable vectors for a pattern
    #[cfg(feature = "vectors")]
    pub fn vector_disable_pattern(&self, pattern: &str) -> Result<()> {
        let conn = self.core.conn.lock().unwrap_or_else(|e| e.into_inner());
        let target = format!("{}:{}", self.selected_db, pattern);
        conn.execute(
            "UPDATE vector_settings SET enabled = 0 WHERE level = 'pattern' AND target = ?",
            params![target],
        )?;
        Ok(())
    }

    /// Disable vectors for a specific key
    #[cfg(feature = "vectors")]
    pub fn vector_disable_key(&self, key: &str) -> Result<()> {
        let conn = self.core.conn.lock().unwrap_or_else(|e| e.into_inner());
        let target = format!("{}:{}", self.selected_db, key);
        conn.execute(
            "UPDATE vector_settings SET enabled = 0 WHERE level = 'key' AND target = ?",
            params![target],
        )?;
        Ok(())
    }

    /// Check if vectors are enabled for a key, returns dimensions if enabled
    #[cfg(feature = "vectors")]
    pub fn is_vector_enabled(&self, key: &str) -> Result<Option<i32>> {
        let conn = self.core.conn.lock().unwrap_or_else(|e| e.into_inner());

        // 1. Check key-level config
        let key_target = format!("{}:{}", self.selected_db, key);
        let key_config: Option<(bool, i32)> = conn
            .query_row(
                "SELECT enabled, dimensions FROM vector_settings WHERE level = 'key' AND target = ? LIMIT 1",
                params![key_target],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()?;

        if let Some((enabled, dimensions)) = key_config {
            return Ok(if enabled { Some(dimensions) } else { None });
        }

        // 2. Check pattern-level configs (match glob patterns)
        let mut stmt = conn.prepare(
            "SELECT target, enabled, dimensions FROM vector_settings WHERE level = 'pattern' AND target LIKE ?"
        )?;
        let db_prefix = format!("{}:%", self.selected_db);
        let patterns: Vec<(String, bool, i32)> = stmt
            .query_map(params![db_prefix], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, bool>(1)?,
                    row.get::<_, i32>(2)?,
                ))
            })?
            .filter_map(|r| r.ok())
            .collect();

        // Check if key matches any pattern
        for (target, enabled, dimensions) in patterns {
            if let Some(pattern) = target.strip_prefix(&format!("{}:", self.selected_db)) {
                if glob_match(pattern, key) {
                    return Ok(if enabled { Some(dimensions) } else { None });
                }
            }
        }

        // 3. Check database-level config
        let db_config: Option<(bool, i32)> = conn
            .query_row(
                "SELECT enabled, dimensions FROM vector_settings WHERE level = 'database' AND target = ? LIMIT 1",
                params![self.selected_db.to_string()],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()?;

        if let Some((enabled, dimensions)) = db_config {
            return Ok(if enabled { Some(dimensions) } else { None });
        }

        // 4. Check global config
        let global_config: Option<(bool, i32)> = conn
            .query_row(
                "SELECT enabled, dimensions FROM vector_settings WHERE level = 'global' AND target = '*' LIMIT 1",
                params![],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()?;

        if let Some((enabled, dimensions)) = global_config {
            return Ok(if enabled { Some(dimensions) } else { None });
        }

        // No config found, vectors not enabled
        Ok(None)
    }

    /// Add a vector to a key
    #[cfg(feature = "vectors")]
    pub fn vadd(
        &self,
        key: &str,
        vector_id: &str,
        embedding: &[f32],
        metadata: Option<&str>,
    ) -> Result<bool> {
        // Check if vectors are enabled for this key
        let dimensions = match self.is_vector_enabled(key)? {
            Some(d) => d,
            None => {
                return Err(KvError::Other(
                    "vectors not enabled for this key".to_string(),
                ))
            }
        };

        // Validate dimensions
        if embedding.len() != dimensions as usize {
            return Err(KvError::Other(format!(
                "vector dimension mismatch: expected {}, got {}",
                dimensions,
                embedding.len()
            )));
        }

        let conn = self.core.conn.lock().unwrap_or_else(|e| e.into_inner());

        // Get or create key_id
        let key_id: i64 = match conn
            .query_row(
                "SELECT id FROM keys WHERE db = ? AND key = ? LIMIT 1",
                params![self.selected_db, key],
                |row| row.get(0),
            )
            .optional()?
        {
            Some(id) => id,
            None => {
                // Create the key as a special "vector" type (using String type for now)
                conn.execute(
                    "INSERT INTO keys (db, key, key_type, created_at, updated_at) VALUES (?, ?, ?, ?, ?)",
                    params![
                        self.selected_db,
                        key,
                        KeyType::String as i32,
                        Self::now_ms(),
                        Self::now_ms()
                    ],
                )?;
                conn.last_insert_rowid()
            }
        };

        // Convert embedding to bytes
        let embedding_bytes: Vec<u8> = embedding.iter().flat_map(|f| f.to_le_bytes()).collect();

        // Insert or replace vector
        let rows = conn.execute(
            "INSERT OR REPLACE INTO vectors (key_id, vector_id, embedding, dimensions, metadata, created_at)
             VALUES (?, ?, ?, ?, ?, ?)",
            params![
                key_id,
                vector_id,
                embedding_bytes,
                dimensions,
                metadata,
                Self::now_ms()
            ],
        )?;

        Ok(rows > 0)
    }

    /// Get a vector by key and vector_id
    #[cfg(feature = "vectors")]
    pub fn vget(&self, key: &str, vector_id: &str) -> Result<Option<VectorEntry>> {
        let conn = self.core.conn.lock().unwrap_or_else(|e| e.into_inner());

        let result: Option<(i64, i64, String, Vec<u8>, i32, Option<String>, i64)> = conn
            .query_row(
                "SELECT v.id, v.key_id, v.vector_id, v.embedding, v.dimensions, v.metadata, v.created_at
                 FROM vectors v
                 JOIN keys k ON v.key_id = k.id
                 WHERE k.db = ? AND k.key = ? AND v.vector_id = ?
                 LIMIT 1",
                params![self.selected_db, key, vector_id],
                |row| Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                    row.get(5)?,
                    row.get(6)?,
                )),
            )
            .optional()?;

        match result {
            Some((id, key_id, vid, embedding_bytes, dimensions, metadata, created_at)) => {
                // Convert bytes back to f32 vector
                let embedding: Vec<f32> = embedding_bytes
                    .chunks_exact(4)
                    .map(|chunk| f32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]))
                    .collect();

                Ok(Some(VectorEntry {
                    id,
                    key_id,
                    vector_id: vid,
                    embedding,
                    dimensions,
                    metadata,
                    created_at,
                }))
            }
            None => Ok(None),
        }
    }

    /// Delete a vector by key and vector_id
    #[cfg(feature = "vectors")]
    pub fn vdel(&self, key: &str, vector_id: &str) -> Result<bool> {
        let conn = self.core.conn.lock().unwrap_or_else(|e| e.into_inner());

        let rows = conn.execute(
            "DELETE FROM vectors WHERE key_id IN (
                SELECT id FROM keys WHERE db = ? AND key = ?
             ) AND vector_id = ?",
            params![self.selected_db, key, vector_id],
        )?;

        Ok(rows > 0)
    }

    /// Count vectors for a key
    #[cfg(feature = "vectors")]
    pub fn vcount(&self, key: &str) -> Result<i64> {
        let conn = self.core.conn.lock().unwrap_or_else(|e| e.into_inner());

        let count: i64 = conn.query_row(
            "SELECT COUNT(*) FROM vectors v
                 JOIN keys k ON v.key_id = k.id
                 WHERE k.db = ? AND k.key = ?",
            params![self.selected_db, key],
            |row| row.get(0),
        )?;

        Ok(count)
    }

    /// Search for similar vectors using K-NN
    /// Note: This uses a brute-force approach. For production with large datasets,
    /// sqlite-vec should be loaded and used for efficient HNSW search.
    #[cfg(feature = "vectors")]
    pub fn vsearch(
        &self,
        key: &str,
        query_vector: &[f32],
        k: i64,
        metric: DistanceMetric,
    ) -> Result<Vec<VectorSearchResult>> {
        // Check if vectors are enabled and get expected dimensions
        let expected_dims = match self.is_vector_enabled(key)? {
            Some(d) => d,
            None => {
                return Err(KvError::Other(
                    "vectors not enabled for this key".to_string(),
                ))
            }
        };

        if query_vector.len() != expected_dims as usize {
            return Err(KvError::Other(format!(
                "query vector dimension mismatch: expected {}, got {}",
                expected_dims,
                query_vector.len()
            )));
        }

        let conn = self.core.conn.lock().unwrap_or_else(|e| e.into_inner());

        // Get all vectors for the key
        let mut stmt = conn.prepare(
            "SELECT v.vector_id, v.embedding, v.metadata
             FROM vectors v
             JOIN keys k ON v.key_id = k.id
             WHERE k.db = ? AND k.key = ?",
        )?;

        let mut results: Vec<VectorSearchResult> = stmt
            .query_map(params![self.selected_db, key], |row| {
                let vector_id: String = row.get(0)?;
                let embedding_bytes: Vec<u8> = row.get(1)?;
                let metadata: Option<String> = row.get(2)?;

                // Convert bytes to f32 vector
                let embedding: Vec<f32> = embedding_bytes
                    .chunks_exact(4)
                    .map(|chunk| f32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]))
                    .collect();

                // Calculate distance based on metric
                let distance = match metric {
                    DistanceMetric::L2 => {
                        // Euclidean distance
                        let sum: f32 = query_vector
                            .iter()
                            .zip(embedding.iter())
                            .map(|(a, b)| (a - b).powi(2))
                            .sum();
                        sum.sqrt() as f64
                    }
                    DistanceMetric::Cosine => {
                        // Cosine distance = 1 - cosine_similarity
                        let dot: f32 = query_vector
                            .iter()
                            .zip(embedding.iter())
                            .map(|(a, b)| a * b)
                            .sum();
                        let norm_a: f32 =
                            query_vector.iter().map(|x| x.powi(2)).sum::<f32>().sqrt();
                        let norm_b: f32 = embedding.iter().map(|x| x.powi(2)).sum::<f32>().sqrt();
                        if norm_a == 0.0 || norm_b == 0.0 {
                            1.0
                        } else {
                            1.0 - (dot / (norm_a * norm_b)) as f64
                        }
                    }
                    DistanceMetric::IP => {
                        // Inner product (negative for ranking - higher IP = lower distance)
                        let dot: f32 = query_vector
                            .iter()
                            .zip(embedding.iter())
                            .map(|(a, b)| a * b)
                            .sum();
                        -dot as f64
                    }
                };

                Ok(VectorSearchResult {
                    vector_id,
                    distance,
                    metadata,
                })
            })?
            .filter_map(|r| r.ok())
            .collect();

        // Sort by distance (ascending) and take top k
        results.sort_by(|a, b| {
            a.distance
                .partial_cmp(&b.distance)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        results.truncate(k as usize);

        Ok(results)
    }

    /// Get vector statistics
    #[cfg(feature = "vectors")]
    pub fn vector_info(&self) -> Result<VectorStats> {
        let conn = self.core.conn.lock().unwrap_or_else(|e| e.into_inner());

        // Count total vectors in current database
        let total_vectors: i64 = conn.query_row(
            "SELECT COUNT(*) FROM vectors v
             JOIN keys k ON v.key_id = k.id
             WHERE k.db = ?",
            params![self.selected_db],
            |row| row.get(0),
        )?;

        // Count unique keys with vectors
        let total_keys: i64 = conn.query_row(
            "SELECT COUNT(DISTINCT k.id) FROM vectors v
             JOIN keys k ON v.key_id = k.id
             WHERE k.db = ?",
            params![self.selected_db],
            |row| row.get(0),
        )?;

        // Estimate storage bytes
        let storage_bytes: i64 = conn
            .query_row(
                "SELECT COALESCE(SUM(LENGTH(embedding)), 0) FROM vectors v
             JOIN keys k ON v.key_id = k.id
             WHERE k.db = ?",
                params![self.selected_db],
                |row| row.get(0),
            )
            .unwrap_or(0);

        let mut stats = VectorStats::new(total_vectors, total_keys, storage_bytes);

        // Get all vector configs
        let mut stmt = conn.prepare(
            "SELECT id, level, target, enabled, dimensions, created_at FROM vector_settings ORDER BY level, target"
        )?;
        let configs: Vec<crate::types::VectorConfig> = stmt
            .query_map([], |row| {
                let id: i64 = row.get(0)?;
                let level_str: String = row.get(1)?;
                let target: String = row.get(2)?;
                let enabled: bool = row.get(3)?;
                let dimensions: i32 = row.get(4)?;
                let created_at: i64 = row.get(5)?;

                let level = match level_str.as_str() {
                    "global" => VectorLevel::Global,
                    "database" => {
                        let db_num: i32 = target.parse().unwrap_or(0);
                        VectorLevel::Database(db_num)
                    }
                    "pattern" => VectorLevel::Pattern(target.clone()),
                    "key" => VectorLevel::Key,
                    _ => VectorLevel::Global,
                };

                Ok(crate::types::VectorConfig {
                    id,
                    level,
                    target,
                    enabled,
                    dimensions,
                    created_at,
                })
            })?
            .filter_map(|r| r.ok())
            .collect();

        stats.configs = configs;
        Ok(stats)
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
        db.hset("myhash", &[("field", b"value".as_slice())])
            .unwrap();

        // String operations on hash should fail with WrongType
        assert!(matches!(db.get("myhash"), Err(KvError::WrongType)));
        assert!(matches!(db.incr("myhash"), Err(KvError::WrongType)));
        assert!(matches!(db.incrby("myhash", 5), Err(KvError::WrongType)));
        assert!(matches!(
            db.incrbyfloat("myhash", 1.5),
            Err(KvError::WrongType)
        ));
        assert!(matches!(
            db.append("myhash", b"test"),
            Err(KvError::WrongType)
        ));

        // Create a list key
        db.lpush("mylist", &[b"a"]).unwrap();

        // String operations on list should fail with WrongType
        assert!(matches!(db.get("mylist"), Err(KvError::WrongType)));
        assert!(matches!(db.incr("mylist"), Err(KvError::WrongType)));
        assert!(matches!(
            db.append("mylist", b"test"),
            Err(KvError::WrongType)
        ));

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
        let count = db
            .hset("myhash", &[("field1", b"value1".as_slice())])
            .unwrap();
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
        let count = db
            .hset(
                "myhash",
                &[("f1", b"v1".as_slice()), ("f2", b"v2"), ("f3", b"v3")],
            )
            .unwrap();
        assert_eq!(count, 3);

        assert_eq!(db.hget("myhash", "f1").unwrap(), Some(b"v1".to_vec()));
        assert_eq!(db.hget("myhash", "f2").unwrap(), Some(b"v2".to_vec()));
        assert_eq!(db.hget("myhash", "f3").unwrap(), Some(b"v3".to_vec()));
    }

    #[test]
    fn test_hset_update() {
        let db = Db::open_memory().unwrap();

        // Set initial value
        let count1 = db
            .hset("myhash", &[("field", b"value1".as_slice())])
            .unwrap();
        assert_eq!(count1, 1);

        // Update existing field (returns 0 new fields)
        let count2 = db
            .hset("myhash", &[("field", b"value2".as_slice())])
            .unwrap();
        assert_eq!(count2, 0);

        assert_eq!(
            db.hget("myhash", "field").unwrap(),
            Some(b"value2".to_vec())
        );
    }

    #[test]
    fn test_hmget() {
        let db = Db::open_memory().unwrap();

        db.hset("myhash", &[("f1", b"v1".as_slice()), ("f2", b"v2")])
            .unwrap();

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

        db.hset("myhash", &[("f1", b"v1".as_slice()), ("f2", b"v2")])
            .unwrap();

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

        db.hset(
            "myhash",
            &[("f1", b"v1".as_slice()), ("f2", b"v2"), ("f3", b"v3")],
        )
        .unwrap();

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

        db.hset("myhash", &[("field", b"value".as_slice())])
            .unwrap();

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

        db.hset("myhash", &[("field", b"value".as_slice())])
            .unwrap();

        assert!(db.hexists("myhash", "field").unwrap());
        assert!(!db.hexists("myhash", "nonexistent").unwrap());
        assert!(!db.hexists("nonexistent", "field").unwrap());
    }

    #[test]
    fn test_hkeys() {
        let db = Db::open_memory().unwrap();

        db.hset(
            "myhash",
            &[("f1", b"v1".as_slice()), ("f2", b"v2"), ("f3", b"v3")],
        )
        .unwrap();

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

        db.hset("myhash", &[("f1", b"v1".as_slice()), ("f2", b"v2")])
            .unwrap();

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

        db.hset("myhash", &[("f2", b"v2".as_slice()), ("f3", b"v3")])
            .unwrap();
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
        assert_eq!(
            db.hget("myhash", "field").unwrap(),
            Some(b"value1".to_vec())
        );

        // Second HSETNX should fail
        assert!(!db.hsetnx("myhash", "field", b"value2").unwrap());
        assert_eq!(
            db.hget("myhash", "field").unwrap(),
            Some(b"value1".to_vec())
        );
    }

    #[test]
    fn test_hash_type() {
        let db = Db::open_memory().unwrap();

        db.hset("myhash", &[("field", b"value".as_slice())])
            .unwrap();
        assert_eq!(db.key_type("myhash").unwrap(), Some(KeyType::Hash));
    }

    #[test]
    fn test_hash_wrong_type() {
        let db = Db::open_memory().unwrap();

        // Create a string key
        db.set("mystring", b"value", None).unwrap();

        // Try hash operations on string key - should fail
        assert!(db
            .hset("mystring", &[("field", b"value".as_slice())])
            .is_err());
        assert!(db.hget("mystring", "field").is_err());
        assert!(db.hdel("mystring", &["field"]).is_err());
    }

    // --- Session 6: Disk tests for hash operations ---

    #[test]
    fn test_disk_hset_hget() {
        let path = temp_db_path();
        let db = Db::open(&path).unwrap();

        db.hset("myhash", &[("f1", b"v1".as_slice()), ("f2", b"v2")])
            .unwrap();

        assert_eq!(db.hget("myhash", "f1").unwrap(), Some(b"v1".to_vec()));
        assert_eq!(db.hget("myhash", "f2").unwrap(), Some(b"v2".to_vec()));
        assert_eq!(db.hget("myhash", "f3").unwrap(), None);

        cleanup_db(&path);
    }

    #[test]
    fn test_disk_hmget_hgetall() {
        let path = temp_db_path();
        let db = Db::open(&path).unwrap();

        db.hset(
            "myhash",
            &[("f1", b"v1".as_slice()), ("f2", b"v2"), ("f3", b"v3")],
        )
        .unwrap();

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

        db.hset("myhash", &[("f1", b"v1".as_slice()), ("f2", b"v2")])
            .unwrap();

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

        db.hset("myhash", &[("f1", b"v1".as_slice()), ("f2", b"v2")])
            .unwrap();

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

        let r1: f64 = db
            .hincrbyfloat("myhash", "float", 1.5)
            .unwrap()
            .parse()
            .unwrap();
        assert!((r1 - 1.5).abs() < 0.001);

        let r2: f64 = db
            .hincrbyfloat("myhash", "float", 0.5)
            .unwrap()
            .parse()
            .unwrap();
        assert!((r2 - 2.0).abs() < 0.001);

        cleanup_db(&path);
    }

    #[test]
    fn test_disk_hsetnx() {
        let path = temp_db_path();
        let db = Db::open(&path).unwrap();

        assert!(db.hsetnx("myhash", "field", b"value1").unwrap());
        assert!(!db.hsetnx("myhash", "field", b"value2").unwrap());
        assert_eq!(
            db.hget("myhash", "field").unwrap(),
            Some(b"value1".to_vec())
        );

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
        assert_eq!(
            items,
            vec![b"d".to_vec(), b"c".to_vec(), b"b".to_vec(), b"a".to_vec()]
        );

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
        assert_eq!(
            items,
            vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec(), b"d".to_vec()]
        );
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
        assert!(matches!(
            db.lpush("mystring", &[b"a"]),
            Err(KvError::WrongType)
        ));
        assert!(matches!(
            db.rpush("mystring", &[b"a"]),
            Err(KvError::WrongType)
        ));
        assert!(matches!(db.lpop("mystring", None), Err(KvError::WrongType)));
        assert!(matches!(db.rpop("mystring", None), Err(KvError::WrongType)));
        assert!(matches!(db.llen("mystring"), Err(KvError::WrongType)));
        assert!(matches!(
            db.lrange("mystring", 0, -1),
            Err(KvError::WrongType)
        ));
        assert!(matches!(db.lindex("mystring", 0), Err(KvError::WrongType)));
        assert!(matches!(
            db.lset("mystring", 0, b"a"),
            Err(KvError::WrongType)
        ));
        assert!(matches!(
            db.ltrim("mystring", 0, 1),
            Err(KvError::WrongType)
        ));
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
            db.zadd("myzset", &[ZMember::new(1.5, "a"), ZMember::new(4.0, "d"),])
                .unwrap(),
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

        db.zadd("myzset", &[ZMember::new(1.5, "a"), ZMember::new(2.5, "b")])
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
        let members = db
            .zrangebyscore("myzset", 1.0, 4.0, Some(1), Some(2))
            .unwrap();
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
        assert!(matches!(
            db.zscore("mystring", b"a"),
            Err(KvError::WrongType)
        ));
        assert!(matches!(
            db.zrank("mystring", b"a"),
            Err(KvError::WrongType)
        ));
        assert!(matches!(
            db.zrevrank("mystring", b"a"),
            Err(KvError::WrongType)
        ));
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

        db.zadd("myzset", &[ZMember::new(1.0, "a"), ZMember::new(2.0, "b")])
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
            db.zadd("myzset", &[ZMember::new(1.0, "a"), ZMember::new(2.0, "b")])
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
        db.set("key2", b"value2", Some(std::time::Duration::from_millis(1)))
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
            .xadd(
                "mystream",
                Some(explicit_id),
                &fields,
                false,
                None,
                None,
                false,
            )
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

        db.xadd("s", Some(id1), &fields1, false, None, None, false)
            .unwrap();
        db.xadd("s", Some(id2), &fields2, false, None, None, false)
            .unwrap();
        db.xadd("s", Some(id3), &fields3, false, None, None, false)
            .unwrap();

        // Get all entries
        let entries = db
            .xrange("s", StreamId::min(), StreamId::max(), None)
            .unwrap();
        assert_eq!(entries.len(), 3);
        assert_eq!(entries[0].id, id1);
        assert_eq!(entries[1].id, id2);
        assert_eq!(entries[2].id, id3);

        // Get range
        let entries = db.xrange("s", id1, id2, None).unwrap();
        assert_eq!(entries.len(), 2);

        // Get with count
        let entries = db
            .xrange("s", StreamId::min(), StreamId::max(), Some(2))
            .unwrap();
        assert_eq!(entries.len(), 2);
    }

    #[test]
    fn test_xrevrange() {
        let db = Db::open_memory().unwrap();

        let id1 = StreamId::new(1000, 0);
        let id2 = StreamId::new(2000, 0);
        let id3 = StreamId::new(3000, 0);

        let fields: Vec<(&[u8], &[u8])> = vec![(b"f", b"v")];

        db.xadd("s", Some(id1), &fields, false, None, None, false)
            .unwrap();
        db.xadd("s", Some(id2), &fields, false, None, None, false)
            .unwrap();
        db.xadd("s", Some(id3), &fields, false, None, None, false)
            .unwrap();

        // Get all entries in reverse
        let entries = db
            .xrevrange("s", StreamId::max(), StreamId::min(), None)
            .unwrap();
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

        db.xadd("s", Some(id1), &fields1, false, None, None, false)
            .unwrap();
        db.xadd("s", Some(id2), &fields2, false, None, None, false)
            .unwrap();

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
            db.xadd(
                "s",
                Some(StreamId::new(i * 1000, 0)),
                &fields,
                false,
                None,
                None,
                false,
            )
            .unwrap();
        }

        assert_eq!(db.xlen("s").unwrap(), 5);

        // Trim to 3 entries
        let deleted = db.xtrim("s", Some(3), None, false).unwrap();
        assert_eq!(deleted, 2);
        assert_eq!(db.xlen("s").unwrap(), 3);

        // Verify oldest entries were removed
        let entries = db
            .xrange("s", StreamId::min(), StreamId::max(), None)
            .unwrap();
        assert_eq!(entries[0].id.ms, 3000);
    }

    #[test]
    fn test_xtrim_minid() {
        let db = Db::open_memory().unwrap();

        let fields: Vec<(&[u8], &[u8])> = vec![(b"f", b"v")];
        for i in 1..=5 {
            db.xadd(
                "s",
                Some(StreamId::new(i * 1000, 0)),
                &fields,
                false,
                None,
                None,
                false,
            )
            .unwrap();
        }

        // Trim entries before 3000-0
        let deleted = db
            .xtrim("s", None, Some(StreamId::new(3000, 0)), false)
            .unwrap();
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
        db.xadd("s", Some(id1), &fields, false, None, None, false)
            .unwrap();
        db.xadd("s", Some(id2), &fields, false, None, None, false)
            .unwrap();
        db.xadd("s", Some(id3), &fields, false, None, None, false)
            .unwrap();

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

        db.xadd("s", Some(id1), &fields1, false, None, None, false)
            .unwrap();
        db.xadd("s", Some(id2), &fields2, false, None, None, false)
            .unwrap();

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
            db.xadd(
                "s",
                Some(StreamId::new(i * 1000, 0)),
                &fields,
                false,
                Some(3),
                None,
                false,
            )
            .unwrap();
        }

        // Should only have 3 entries
        assert_eq!(db.xlen("s").unwrap(), 3);

        // Should have the latest 3
        let entries = db
            .xrange("s", StreamId::min(), StreamId::max(), None)
            .unwrap();
        assert_eq!(entries[0].id.ms, 3000);
    }

    #[test]
    fn test_stream_type() {
        let db = Db::open_memory().unwrap();

        let fields: Vec<(&[u8], &[u8])> = vec![(b"f", b"v")];
        db.xadd("mystream", None, &fields, false, None, None, false)
            .unwrap();

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
        db.xadd("s", Some(id), &fields, false, None, None, false)
            .unwrap();

        let entries = db
            .xrange("s", StreamId::min(), StreamId::max(), None)
            .unwrap();
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
        db.xadd(
            "mystream",
            Some(StreamId::new(1000, 0)),
            &fields,
            false,
            None,
            None,
            false,
        )
        .unwrap();

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
        db.xadd(
            "mystream",
            Some(StreamId::new(1000, 0)),
            &fields,
            false,
            None,
            None,
            false,
        )
        .unwrap();
        db.xgroup_create("mystream", "mygroup", StreamId::new(0, 0), false)
            .unwrap();

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
        db.xadd(
            "mystream",
            Some(StreamId::new(1000, 0)),
            &fields,
            false,
            None,
            None,
            false,
        )
        .unwrap();
        db.xgroup_create("mystream", "mygroup", StreamId::new(0, 0), false)
            .unwrap();

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
        db.xadd(
            "mystream",
            Some(StreamId::new(1000, 0)),
            &fields,
            false,
            None,
            None,
            false,
        )
        .unwrap();
        db.xgroup_create("mystream", "mygroup", StreamId::new(0, 0), false)
            .unwrap();

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
        db.xadd(
            "mystream",
            Some(StreamId::new(1000, 0)),
            &fields,
            false,
            None,
            None,
            false,
        )
        .unwrap();
        db.xgroup_create("mystream", "mygroup", StreamId::new(0, 0), false)
            .unwrap();
        db.xgroup_createconsumer("mystream", "mygroup", "consumer1")
            .unwrap();

        // Delete consumer (returns pending count)
        let result = db.xgroup_delconsumer("mystream", "mygroup", "consumer1");
        assert!(matches!(result, Ok(0)));
    }

    #[test]
    fn test_xreadgroup_new_messages() {
        let db = Db::open_memory().unwrap();

        let fields: Vec<(&[u8], &[u8])> = vec![(b"f", b"v")];
        db.xadd(
            "mystream",
            Some(StreamId::new(1000, 0)),
            &fields,
            false,
            None,
            None,
            false,
        )
        .unwrap();
        db.xadd(
            "mystream",
            Some(StreamId::new(2000, 0)),
            &fields,
            false,
            None,
            None,
            false,
        )
        .unwrap();
        db.xgroup_create("mystream", "mygroup", StreamId::new(0, 0), false)
            .unwrap();

        // Read new messages with >
        let results = db
            .xreadgroup("mygroup", "consumer1", &["mystream"], &[">"], None, false)
            .unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].0, "mystream");
        assert_eq!(results[0].1.len(), 2);
        assert_eq!(results[0].1[0].id, StreamId::new(1000, 0));
        assert_eq!(results[0].1[1].id, StreamId::new(2000, 0));

        // Reading again should return nothing (all delivered)
        let results = db
            .xreadgroup("mygroup", "consumer1", &["mystream"], &[">"], None, false)
            .unwrap();
        assert!(results.is_empty());
    }

    #[test]
    fn test_xreadgroup_pending() {
        let db = Db::open_memory().unwrap();

        let fields: Vec<(&[u8], &[u8])> = vec![(b"f", b"v")];
        db.xadd(
            "mystream",
            Some(StreamId::new(1000, 0)),
            &fields,
            false,
            None,
            None,
            false,
        )
        .unwrap();
        db.xadd(
            "mystream",
            Some(StreamId::new(2000, 0)),
            &fields,
            false,
            None,
            None,
            false,
        )
        .unwrap();
        db.xgroup_create("mystream", "mygroup", StreamId::new(0, 0), false)
            .unwrap();

        // First read creates pending entries
        db.xreadgroup("mygroup", "consumer1", &["mystream"], &[">"], None, false)
            .unwrap();

        // Read pending entries with 0
        let results = db
            .xreadgroup("mygroup", "consumer1", &["mystream"], &["0"], None, false)
            .unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].1.len(), 2);
    }

    #[test]
    fn test_xreadgroup_noack() {
        let db = Db::open_memory().unwrap();

        let fields: Vec<(&[u8], &[u8])> = vec![(b"f", b"v")];
        db.xadd(
            "mystream",
            Some(StreamId::new(1000, 0)),
            &fields,
            false,
            None,
            None,
            false,
        )
        .unwrap();
        db.xgroup_create("mystream", "mygroup", StreamId::new(0, 0), false)
            .unwrap();

        // Read with NOACK - should not add to pending
        db.xreadgroup("mygroup", "consumer1", &["mystream"], &[">"], None, true)
            .unwrap();

        // Check pending is empty
        let summary = db.xpending_summary("mystream", "mygroup").unwrap();
        assert_eq!(summary.count, 0);
    }

    #[test]
    fn test_xack() {
        let db = Db::open_memory().unwrap();

        let fields: Vec<(&[u8], &[u8])> = vec![(b"f", b"v")];
        db.xadd(
            "mystream",
            Some(StreamId::new(1000, 0)),
            &fields,
            false,
            None,
            None,
            false,
        )
        .unwrap();
        db.xadd(
            "mystream",
            Some(StreamId::new(2000, 0)),
            &fields,
            false,
            None,
            None,
            false,
        )
        .unwrap();
        db.xgroup_create("mystream", "mygroup", StreamId::new(0, 0), false)
            .unwrap();

        // Read messages to create pending entries
        db.xreadgroup("mygroup", "consumer1", &["mystream"], &[">"], None, false)
            .unwrap();

        // Acknowledge one message
        let acked = db
            .xack("mystream", "mygroup", &[StreamId::new(1000, 0)])
            .unwrap();
        assert_eq!(acked, 1);

        // Check pending
        let summary = db.xpending_summary("mystream", "mygroup").unwrap();
        assert_eq!(summary.count, 1);

        // Acknowledge already acked message returns 0
        let acked = db
            .xack("mystream", "mygroup", &[StreamId::new(1000, 0)])
            .unwrap();
        assert_eq!(acked, 0);
    }

    #[test]
    fn test_xpending_summary() {
        let db = Db::open_memory().unwrap();

        let fields: Vec<(&[u8], &[u8])> = vec![(b"f", b"v")];
        db.xadd(
            "mystream",
            Some(StreamId::new(1000, 0)),
            &fields,
            false,
            None,
            None,
            false,
        )
        .unwrap();
        db.xadd(
            "mystream",
            Some(StreamId::new(2000, 0)),
            &fields,
            false,
            None,
            None,
            false,
        )
        .unwrap();
        db.xgroup_create("mystream", "mygroup", StreamId::new(0, 0), false)
            .unwrap();

        // Read messages
        db.xreadgroup("mygroup", "consumer1", &["mystream"], &[">"], None, false)
            .unwrap();

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
        db.xadd(
            "mystream",
            Some(StreamId::new(1000, 0)),
            &fields,
            false,
            None,
            None,
            false,
        )
        .unwrap();
        db.xadd(
            "mystream",
            Some(StreamId::new(2000, 0)),
            &fields,
            false,
            None,
            None,
            false,
        )
        .unwrap();
        db.xgroup_create("mystream", "mygroup", StreamId::new(0, 0), false)
            .unwrap();

        db.xreadgroup("mygroup", "consumer1", &["mystream"], &[">"], None, false)
            .unwrap();

        let entries = db
            .xpending_range(
                "mystream",
                "mygroup",
                StreamId::min(),
                StreamId::max(),
                10,
                None,
                None,
            )
            .unwrap();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].id, StreamId::new(1000, 0));
        assert_eq!(entries[0].consumer, "consumer1");
        assert_eq!(entries[0].delivery_count, 1);
    }

    #[test]
    fn test_xclaim() {
        let db = Db::open_memory().unwrap();

        let fields: Vec<(&[u8], &[u8])> = vec![(b"f", b"v")];
        db.xadd(
            "mystream",
            Some(StreamId::new(1000, 0)),
            &fields,
            false,
            None,
            None,
            false,
        )
        .unwrap();
        db.xgroup_create("mystream", "mygroup", StreamId::new(0, 0), false)
            .unwrap();

        // Read message with consumer1
        db.xreadgroup("mygroup", "consumer1", &["mystream"], &[">"], None, false)
            .unwrap();

        // Claim with consumer2 using FORCE (no min-idle-time requirement)
        let claimed = db
            .xclaim(
                "mystream",
                "mygroup",
                "consumer2",
                0,
                &[StreamId::new(1000, 0)],
                None,
                None,
                None,
                true,
                false,
            )
            .unwrap();
        assert_eq!(claimed.len(), 1);
        assert_eq!(claimed[0].id, StreamId::new(1000, 0));

        // Check that pending now shows consumer2
        let entries = db
            .xpending_range(
                "mystream",
                "mygroup",
                StreamId::min(),
                StreamId::max(),
                10,
                None,
                None,
            )
            .unwrap();
        assert_eq!(entries[0].consumer, "consumer2");
    }

    #[test]
    fn test_xclaim_justid() {
        let db = Db::open_memory().unwrap();

        let fields: Vec<(&[u8], &[u8])> = vec![(b"f", b"v")];
        db.xadd(
            "mystream",
            Some(StreamId::new(1000, 0)),
            &fields,
            false,
            None,
            None,
            false,
        )
        .unwrap();
        db.xgroup_create("mystream", "mygroup", StreamId::new(0, 0), false)
            .unwrap();

        db.xreadgroup("mygroup", "consumer1", &["mystream"], &[">"], None, false)
            .unwrap();

        // Claim with JUSTID - should return empty fields
        let claimed = db
            .xclaim(
                "mystream",
                "mygroup",
                "consumer2",
                0,
                &[StreamId::new(1000, 0)],
                None,
                None,
                None,
                true,
                true,
            )
            .unwrap();
        assert_eq!(claimed.len(), 1);
        assert_eq!(claimed[0].id, StreamId::new(1000, 0));
        assert!(claimed[0].fields.is_empty());
    }

    #[test]
    fn test_xinfo_groups() {
        let db = Db::open_memory().unwrap();

        let fields: Vec<(&[u8], &[u8])> = vec![(b"f", b"v")];
        db.xadd(
            "mystream",
            Some(StreamId::new(1000, 0)),
            &fields,
            false,
            None,
            None,
            false,
        )
        .unwrap();
        db.xgroup_create("mystream", "group1", StreamId::new(0, 0), false)
            .unwrap();
        db.xgroup_create("mystream", "group2", StreamId::new(1000, 0), false)
            .unwrap();

        let groups = db.xinfo_groups("mystream").unwrap();
        assert_eq!(groups.len(), 2);
    }

    #[test]
    fn test_xinfo_consumers() {
        let db = Db::open_memory().unwrap();

        let fields: Vec<(&[u8], &[u8])> = vec![(b"f", b"v")];
        db.xadd(
            "mystream",
            Some(StreamId::new(1000, 0)),
            &fields,
            false,
            None,
            None,
            false,
        )
        .unwrap();
        db.xgroup_create("mystream", "mygroup", StreamId::new(0, 0), false)
            .unwrap();
        db.xgroup_createconsumer("mystream", "mygroup", "consumer1")
            .unwrap();
        db.xgroup_createconsumer("mystream", "mygroup", "consumer2")
            .unwrap();

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
        assert!(matches!(
            result,
            Err(tokio::sync::broadcast::error::RecvError::Closed)
        ));
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

    // --- Session 17: History Tracking Schema & Types Tests ---

    #[test]
    fn test_history_schema_created() {
        // Verify that history tables are created with migrations
        let db = Db::open_memory().unwrap();
        let conn = db.core.conn.lock().unwrap_or_else(|e| e.into_inner());

        // Check history_config table exists
        let result: rusqlite::Result<i32> = conn.query_row(
            "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='history_config'",
            [],
            |row| row.get(0),
        );
        assert!(
            result.is_ok() && result.unwrap() > 0,
            "history_config table should exist"
        );

        // Check key_history table exists
        let result: rusqlite::Result<i32> = conn.query_row(
            "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='key_history'",
            [],
            |row| row.get(0),
        );
        assert!(
            result.is_ok() && result.unwrap() > 0,
            "key_history table should exist"
        );

        // Check history_config indexes
        let result: rusqlite::Result<i32> = conn.query_row(
            "SELECT COUNT(*) FROM sqlite_master WHERE type='index' AND name='idx_history_config_level_target'",
            [],
            |row| row.get(0),
        );
        assert!(
            result.is_ok() && result.unwrap() > 0,
            "history_config index should exist"
        );

        // Check key_history indexes
        let result: rusqlite::Result<i32> = conn.query_row(
            "SELECT COUNT(*) FROM sqlite_master WHERE type='index' AND name='idx_history_key_time'",
            [],
            |row| row.get(0),
        );
        assert!(
            result.is_ok() && result.unwrap() > 0,
            "key_history time index should exist"
        );

        let result: rusqlite::Result<i32> = conn.query_row(
            "SELECT COUNT(*) FROM sqlite_master WHERE type='index' AND name='idx_history_db_key_time'",
            [],
            |row| row.get(0),
        );
        assert!(
            result.is_ok() && result.unwrap() > 0,
            "key_history db_key_time index should exist"
        );
    }

    #[test]
    fn test_history_config_table_schema() {
        // Verify history_config table has correct columns
        let db = Db::open_memory().unwrap();
        let conn = db.core.conn.lock().unwrap_or_else(|e| e.into_inner());

        let mut stmt = conn
            .prepare("PRAGMA table_info(history_config)")
            .expect("Should be able to query table schema");

        let columns: Vec<String> = stmt
            .query_map([], |row| row.get::<_, String>(1))
            .expect("Should be able to iterate columns")
            .map(|r| r.expect("Should be able to get column name"))
            .collect();

        assert!(columns.contains(&"id".to_string()));
        assert!(columns.contains(&"level".to_string()));
        assert!(columns.contains(&"target".to_string()));
        assert!(columns.contains(&"enabled".to_string()));
        assert!(columns.contains(&"retention_type".to_string()));
        assert!(columns.contains(&"retention_value".to_string()));
        assert!(columns.contains(&"created_at".to_string()));
    }

    #[test]
    fn test_key_history_table_schema() {
        // Verify key_history table has correct columns
        let db = Db::open_memory().unwrap();
        let conn = db.core.conn.lock().unwrap_or_else(|e| e.into_inner());

        let mut stmt = conn
            .prepare("PRAGMA table_info(key_history)")
            .expect("Should be able to query table schema");

        let columns: Vec<String> = stmt
            .query_map([], |row| row.get::<_, String>(1))
            .expect("Should be able to iterate columns")
            .map(|r| r.expect("Should be able to get column name"))
            .collect();

        assert!(columns.contains(&"id".to_string()));
        assert!(columns.contains(&"key_id".to_string()));
        assert!(columns.contains(&"db".to_string()));
        assert!(columns.contains(&"key".to_string()));
        assert!(columns.contains(&"key_type".to_string()));
        assert!(columns.contains(&"version_num".to_string()));
        assert!(columns.contains(&"operation".to_string()));
        assert!(columns.contains(&"timestamp_ms".to_string()));
        assert!(columns.contains(&"data_snapshot".to_string()));
        assert!(columns.contains(&"expire_at".to_string()));
    }

    // =========================================================================
    // FT.* RediSearch-compatible tests (Session 23)
    // =========================================================================

    #[test]
    fn test_ft_create_basic() {
        use crate::types::{FtField, FtOnType};

        let db = Db::open_memory().unwrap();

        let schema = vec![FtField::text("title"), FtField::text("body")];

        db.ft_create("idx", FtOnType::Hash, &["doc:"], &schema)
            .unwrap();

        // Verify index was created
        let indexes = db.ft_list().unwrap();
        assert_eq!(indexes, vec!["idx"]);
    }

    #[test]
    fn test_ft_create_duplicate_error() {
        use crate::types::{FtField, FtOnType};

        let db = Db::open_memory().unwrap();

        let schema = vec![FtField::text("title")];
        db.ft_create("idx", FtOnType::Hash, &["doc:"], &schema)
            .unwrap();

        // Creating same index again should fail
        let result = db.ft_create("idx", FtOnType::Hash, &["doc:"], &schema);
        assert!(result.is_err());
    }

    #[test]
    fn test_ft_create_with_multiple_field_types() {
        use crate::types::{FtField, FtFieldType, FtOnType};

        let db = Db::open_memory().unwrap();

        let schema = vec![
            FtField::text("title").sortable(),
            FtField::numeric("price"),
            FtField::tag("category"),
        ];

        db.ft_create("products", FtOnType::Hash, &["product:"], &schema)
            .unwrap();

        let info = db.ft_info("products").unwrap().unwrap();
        assert_eq!(info.name, "products");
        assert_eq!(info.schema.len(), 3);
        assert!(info
            .schema
            .iter()
            .any(|f| f.name == "title" && matches!(f.field_type, FtFieldType::Text)));
        assert!(info
            .schema
            .iter()
            .any(|f| f.name == "price" && matches!(f.field_type, FtFieldType::Numeric)));
        assert!(info
            .schema
            .iter()
            .any(|f| f.name == "category" && matches!(f.field_type, FtFieldType::Tag)));
    }

    #[test]
    fn test_ft_dropindex() {
        use crate::types::{FtField, FtOnType};

        let db = Db::open_memory().unwrap();

        let schema = vec![FtField::text("title")];
        db.ft_create("idx", FtOnType::Hash, &["doc:"], &schema)
            .unwrap();

        assert_eq!(db.ft_list().unwrap().len(), 1);

        let dropped = db.ft_dropindex("idx", false).unwrap();
        assert!(dropped);

        assert_eq!(db.ft_list().unwrap().len(), 0);
    }

    #[test]
    fn test_ft_dropindex_nonexistent() {
        let db = Db::open_memory().unwrap();

        let dropped = db.ft_dropindex("nonexistent", false).unwrap();
        assert!(!dropped);
    }

    #[test]
    fn test_ft_list_empty() {
        let db = Db::open_memory().unwrap();

        let indexes = db.ft_list().unwrap();
        assert!(indexes.is_empty());
    }

    #[test]
    fn test_ft_list_multiple() {
        use crate::types::{FtField, FtOnType};

        let db = Db::open_memory().unwrap();

        let schema = vec![FtField::text("content")];
        db.ft_create("idx1", FtOnType::Hash, &["a:"], &schema)
            .unwrap();
        db.ft_create("idx2", FtOnType::Hash, &["b:"], &schema)
            .unwrap();
        db.ft_create("idx3", FtOnType::Json, &["c:"], &schema)
            .unwrap();

        let indexes = db.ft_list().unwrap();
        assert_eq!(indexes.len(), 3);
        assert!(indexes.contains(&"idx1".to_string()));
        assert!(indexes.contains(&"idx2".to_string()));
        assert!(indexes.contains(&"idx3".to_string()));
    }

    #[test]
    fn test_ft_info() {
        use crate::types::{FtField, FtOnType};

        let db = Db::open_memory().unwrap();

        let schema = vec![FtField::text("title").sortable(), FtField::text("body")];
        db.ft_create("myidx", FtOnType::Hash, &["doc:", "article:"], &schema)
            .unwrap();

        let info = db.ft_info("myidx").unwrap().unwrap();
        assert_eq!(info.name, "myidx");
        assert_eq!(info.on_type, FtOnType::Hash);
        assert_eq!(info.prefixes, vec!["doc:", "article:"]);
        assert_eq!(info.schema.len(), 2);
        assert!(info.schema[0].sortable); // title is sortable
    }

    #[test]
    fn test_ft_info_nonexistent() {
        let db = Db::open_memory().unwrap();

        let info = db.ft_info("nonexistent").unwrap();
        assert!(info.is_none());
    }

    #[test]
    fn test_ft_alter_add_field() {
        use crate::types::{FtField, FtFieldType, FtOnType};

        let db = Db::open_memory().unwrap();

        let schema = vec![FtField::text("title")];
        db.ft_create("idx", FtOnType::Hash, &["doc:"], &schema)
            .unwrap();

        // Add a new field
        db.ft_alter("idx", FtField::numeric("views")).unwrap();

        let info = db.ft_info("idx").unwrap().unwrap();
        assert_eq!(info.schema.len(), 2);
        assert!(info
            .schema
            .iter()
            .any(|f| f.name == "views" && matches!(f.field_type, FtFieldType::Numeric)));
    }

    #[test]
    fn test_ft_alter_duplicate_field_error() {
        use crate::types::{FtField, FtOnType};

        let db = Db::open_memory().unwrap();

        let schema = vec![FtField::text("title")];
        db.ft_create("idx", FtOnType::Hash, &["doc:"], &schema)
            .unwrap();

        // Adding same field again should fail
        let result = db.ft_alter("idx", FtField::text("title"));
        assert!(result.is_err());
    }

    #[test]
    fn test_ft_alias_add_del() {
        use crate::types::{FtField, FtOnType};

        let db = Db::open_memory().unwrap();

        let schema = vec![FtField::text("title")];
        db.ft_create("myindex", FtOnType::Hash, &["doc:"], &schema)
            .unwrap();

        // Add alias
        db.ft_aliasadd("myalias", "myindex").unwrap();

        // Delete alias
        let deleted = db.ft_aliasdel("myalias").unwrap();
        assert!(deleted);

        // Delete again should return false
        let deleted2 = db.ft_aliasdel("myalias").unwrap();
        assert!(!deleted2);
    }

    #[test]
    fn test_ft_alias_update() {
        use crate::types::{FtField, FtOnType};

        let db = Db::open_memory().unwrap();

        let schema = vec![FtField::text("title")];
        db.ft_create("idx1", FtOnType::Hash, &["a:"], &schema)
            .unwrap();
        db.ft_create("idx2", FtOnType::Hash, &["b:"], &schema)
            .unwrap();

        db.ft_aliasadd("alias", "idx1").unwrap();

        // Update alias to point to idx2
        db.ft_aliasupdate("alias", "idx2").unwrap();

        // Should succeed (alias exists and idx2 exists)
    }

    #[test]
    fn test_ft_synonyms() {
        use crate::types::{FtField, FtOnType};

        let db = Db::open_memory().unwrap();

        let schema = vec![FtField::text("content")];
        db.ft_create("idx", FtOnType::Hash, &["doc:"], &schema)
            .unwrap();

        // Add synonyms
        db.ft_synupdate("idx", "grp1", &["happy", "joyful", "glad"])
            .unwrap();
        db.ft_synupdate("idx", "grp2", &["sad", "unhappy", "melancholy"])
            .unwrap();

        // Dump synonyms
        let groups = db.ft_syndump("idx").unwrap();
        assert_eq!(groups.len(), 2);

        let grp1 = groups.iter().find(|(id, _)| id == "grp1").unwrap();
        assert_eq!(grp1.1.len(), 3);
        assert!(grp1.1.contains(&"happy".to_string()));
        assert!(grp1.1.contains(&"joyful".to_string()));
        assert!(grp1.1.contains(&"glad".to_string()));
    }

    #[test]
    fn test_ft_suggestions_basic() {
        let db = Db::open_memory().unwrap();

        // Add suggestions
        db.ft_sugadd("autocomplete", "hello world", 1.0, None)
            .unwrap();
        db.ft_sugadd("autocomplete", "hello there", 2.0, None)
            .unwrap();
        db.ft_sugadd("autocomplete", "goodbye", 1.0, None).unwrap();

        // Get suggestions
        let suggestions = db.ft_sugget("autocomplete", "hel", false, 10).unwrap();
        assert_eq!(suggestions.len(), 2);
        // Higher score should come first
        assert_eq!(suggestions[0].string, "hello there");
        assert_eq!(suggestions[1].string, "hello world");
    }

    #[test]
    fn test_ft_suggestions_fuzzy() {
        let db = Db::open_memory().unwrap();

        db.ft_sugadd("ac", "hello world", 1.0, None).unwrap();
        db.ft_sugadd("ac", "world hello", 1.0, None).unwrap();

        // Fuzzy search (contains "ello")
        let suggestions = db.ft_sugget("ac", "ello", true, 10).unwrap();
        assert_eq!(suggestions.len(), 2);
    }

    #[test]
    fn test_ft_suggestions_with_payload() {
        let db = Db::open_memory().unwrap();

        db.ft_sugadd("ac", "suggestion1", 1.0, Some("payload1"))
            .unwrap();

        let suggestions = db.ft_sugget("ac", "sug", false, 10).unwrap();
        assert_eq!(suggestions.len(), 1);
        assert_eq!(suggestions[0].payload, Some("payload1".to_string()));
    }

    #[test]
    fn test_ft_sugdel() {
        let db = Db::open_memory().unwrap();

        db.ft_sugadd("ac", "hello", 1.0, None).unwrap();
        db.ft_sugadd("ac", "world", 1.0, None).unwrap();

        assert_eq!(db.ft_suglen("ac").unwrap(), 2);

        let deleted = db.ft_sugdel("ac", "hello").unwrap();
        assert!(deleted);

        assert_eq!(db.ft_suglen("ac").unwrap(), 1);

        // Delete non-existent
        let deleted2 = db.ft_sugdel("ac", "nonexistent").unwrap();
        assert!(!deleted2);
    }

    #[test]
    fn test_ft_suglen() {
        let db = Db::open_memory().unwrap();

        assert_eq!(db.ft_suglen("ac").unwrap(), 0);

        db.ft_sugadd("ac", "one", 1.0, None).unwrap();
        assert_eq!(db.ft_suglen("ac").unwrap(), 1);

        db.ft_sugadd("ac", "two", 1.0, None).unwrap();
        assert_eq!(db.ft_suglen("ac").unwrap(), 2);

        db.ft_sugadd("ac", "three", 1.0, None).unwrap();
        assert_eq!(db.ft_suglen("ac").unwrap(), 3);
    }

    #[test]
    fn test_ft_sugadd_update_score() {
        let db = Db::open_memory().unwrap();

        // Add with score 1.0
        db.ft_sugadd("ac", "hello", 1.0, None).unwrap();

        // Add same string with higher score - should update
        db.ft_sugadd("ac", "hello", 5.0, None).unwrap();

        // Should still be just 1 entry
        assert_eq!(db.ft_suglen("ac").unwrap(), 1);

        let suggestions = db.ft_sugget("ac", "hel", false, 10).unwrap();
        assert_eq!(suggestions[0].score, 5.0);
    }

    #[test]
    fn test_ft_create_json_type() {
        use crate::types::{FtField, FtOnType};

        let db = Db::open_memory().unwrap();

        let schema = vec![FtField::text("$.name"), FtField::numeric("$.price")];

        db.ft_create("jsonidx", FtOnType::Json, &["product:"], &schema)
            .unwrap();

        let info = db.ft_info("jsonidx").unwrap().unwrap();
        assert_eq!(info.on_type, FtOnType::Json);
    }

    #[test]
    fn test_ft_field_options() {
        use crate::types::{FtField, FtOnType};

        let db = Db::open_memory().unwrap();

        let mut tag_field = FtField::tag("tags");
        tag_field.separator = ';';
        tag_field.case_sensitive = true;

        let mut text_field = FtField::text("content");
        text_field.nostem = true;
        text_field.weight = 2.0;

        let schema = vec![tag_field, text_field];
        db.ft_create("idx", FtOnType::Hash, &["doc:"], &schema)
            .unwrap();

        let info = db.ft_info("idx").unwrap().unwrap();
        let tag = info.schema.iter().find(|f| f.name == "tags").unwrap();
        assert_eq!(tag.separator, ';');
        assert!(tag.case_sensitive);

        let text = info.schema.iter().find(|f| f.name == "content").unwrap();
        assert!(text.nostem);
        assert_eq!(text.weight, 2.0);
    }

    // =========================================================================
    // FT.SEARCH tests (Session 23.2)
    // =========================================================================

    #[test]
    fn test_ft_search_basic() {
        use crate::types::{FtField, FtOnType, FtSearchOptions};

        let db = Db::open_memory().unwrap();

        // Create index
        let schema = vec![FtField::text("title"), FtField::text("body")];
        db.ft_create("idx", FtOnType::Hash, &["doc:"], &schema)
            .unwrap();

        // Create some documents
        db.hset("doc:1", &[("title", b"Hello World"), ("body", b"This is a test document")])
            .unwrap();
        db.hset("doc:2", &[("title", b"Goodbye World"), ("body", b"Another document here")])
            .unwrap();
        db.hset("doc:3", &[("title", b"Testing Search"), ("body", b"Search functionality test")])
            .unwrap();

        // Search for "Hello"
        let options = FtSearchOptions::new();
        let (total, results) = db.ft_search("idx", "Hello", &options).unwrap();

        assert_eq!(total, 1);
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].key, "doc:1");
    }

    #[test]
    fn test_ft_search_multiple_results() {
        use crate::types::{FtField, FtOnType, FtSearchOptions};

        let db = Db::open_memory().unwrap();

        let schema = vec![FtField::text("title"), FtField::text("body")];
        db.ft_create("idx", FtOnType::Hash, &["doc:"], &schema)
            .unwrap();

        db.hset("doc:1", &[("title", b"Hello World"), ("body", b"Test document")])
            .unwrap();
        db.hset("doc:2", &[("title", b"World News"), ("body", b"World events today")])
            .unwrap();
        db.hset("doc:3", &[("title", b"Local News"), ("body", b"Local events")])
            .unwrap();

        // Search for "World" - should match doc:1 and doc:2
        let options = FtSearchOptions::new();
        let (total, results) = db.ft_search("idx", "World", &options).unwrap();

        assert_eq!(total, 2);
        let keys: Vec<&str> = results.iter().map(|r| r.key.as_str()).collect();
        assert!(keys.contains(&"doc:1"));
        assert!(keys.contains(&"doc:2"));
    }

    #[test]
    fn test_ft_search_nocontent() {
        use crate::types::{FtField, FtOnType, FtSearchOptions};

        let db = Db::open_memory().unwrap();

        let schema = vec![FtField::text("title")];
        db.ft_create("idx", FtOnType::Hash, &["doc:"], &schema)
            .unwrap();

        db.hset("doc:1", &[("title", b"Hello World")])
            .unwrap();

        let mut options = FtSearchOptions::new();
        options.nocontent = true;

        let (total, results) = db.ft_search("idx", "Hello", &options).unwrap();

        assert_eq!(total, 1);
        assert!(results[0].fields.is_empty()); // No content returned
    }

    #[test]
    fn test_ft_search_numeric_range() {
        use crate::types::{FtField, FtFieldType, FtOnType, FtSearchOptions};

        let db = Db::open_memory().unwrap();

        let schema = vec![
            FtField::text("title"),
            FtField::new("price", FtFieldType::Numeric),
        ];
        db.ft_create("idx", FtOnType::Hash, &["product:"], &schema)
            .unwrap();

        db.hset("product:1", &[("title", b"Cheap Item"), ("price", b"10")])
            .unwrap();
        db.hset("product:2", &[("title", b"Medium Item"), ("price", b"50")])
            .unwrap();
        db.hset("product:3", &[("title", b"Expensive Item"), ("price", b"100")])
            .unwrap();

        // Search for items with price between 20 and 80
        let options = FtSearchOptions::new();
        let (total, results) = db.ft_search("idx", "@price:[20 80]", &options).unwrap();

        assert_eq!(total, 1);
        assert_eq!(results[0].key, "product:2");
    }

    #[test]
    fn test_ft_search_tag_filter() {
        use crate::types::{FtField, FtFieldType, FtOnType, FtSearchOptions};

        let db = Db::open_memory().unwrap();

        let schema = vec![
            FtField::text("title"),
            FtField::new("category", FtFieldType::Tag),
        ];
        db.ft_create("idx", FtOnType::Hash, &["item:"], &schema)
            .unwrap();

        db.hset("item:1", &[("title", b"Phone"), ("category", b"electronics")])
            .unwrap();
        db.hset("item:2", &[("title", b"Book"), ("category", b"books")])
            .unwrap();
        db.hset("item:3", &[("title", b"Laptop"), ("category", b"electronics")])
            .unwrap();

        // Search for electronics
        let options = FtSearchOptions::new();
        let (total, results) = db.ft_search("idx", "@category:{electronics}", &options).unwrap();

        assert_eq!(total, 2);
        let keys: Vec<&str> = results.iter().map(|r| r.key.as_str()).collect();
        assert!(keys.contains(&"item:1"));
        assert!(keys.contains(&"item:3"));
    }

    #[test]
    fn test_ft_search_prefix() {
        use crate::types::{FtField, FtOnType, FtSearchOptions};

        let db = Db::open_memory().unwrap();

        let schema = vec![FtField::text("title")];
        db.ft_create("idx", FtOnType::Hash, &["doc:"], &schema)
            .unwrap();

        db.hset("doc:1", &[("title", b"testing search")])
            .unwrap();
        db.hset("doc:2", &[("title", b"testable code")])
            .unwrap();
        db.hset("doc:3", &[("title", b"other content")])
            .unwrap();

        // Prefix search for "test*"
        let options = FtSearchOptions::new();
        let (total, results) = db.ft_search("idx", "test*", &options).unwrap();

        assert_eq!(total, 2);
        let keys: Vec<&str> = results.iter().map(|r| r.key.as_str()).collect();
        assert!(keys.contains(&"doc:1"));
        assert!(keys.contains(&"doc:2"));
    }

    #[test]
    fn test_ft_search_pagination() {
        use crate::types::{FtField, FtOnType, FtSearchOptions};

        let db = Db::open_memory().unwrap();

        let schema = vec![FtField::text("title")];
        db.ft_create("idx", FtOnType::Hash, &["doc:"], &schema)
            .unwrap();

        // Create 10 documents
        for i in 1..=10 {
            db.hset(&format!("doc:{}", i), &[("title", format!("Test document {}", i).as_bytes())])
                .unwrap();
        }

        // Get first page (3 results)
        let mut options = FtSearchOptions::new();
        options.limit_offset = 0;
        options.limit_num = 3;

        let (total, results) = db.ft_search("idx", "Test", &options).unwrap();

        assert_eq!(total, 10); // Total count includes all matches
        assert_eq!(results.len(), 3); // But only 3 returned

        // Get second page
        options.limit_offset = 3;
        let (total2, results2) = db.ft_search("idx", "Test", &options).unwrap();

        assert_eq!(total2, 10);
        assert_eq!(results2.len(), 3);

        // Ensure no overlap
        let page1_keys: Vec<&str> = results.iter().map(|r| r.key.as_str()).collect();
        let page2_keys: Vec<&str> = results2.iter().map(|r| r.key.as_str()).collect();
        for key in &page1_keys {
            assert!(!page2_keys.contains(key));
        }
    }

    #[test]
    fn test_ft_search_return_fields() {
        use crate::types::{FtField, FtOnType, FtSearchOptions};

        let db = Db::open_memory().unwrap();

        let schema = vec![FtField::text("title"), FtField::text("body"), FtField::text("author")];
        db.ft_create("idx", FtOnType::Hash, &["doc:"], &schema)
            .unwrap();

        db.hset("doc:1", &[("title", b"Hello"), ("body", b"World content"), ("author", b"John")])
            .unwrap();

        // Only return title field
        let mut options = FtSearchOptions::new();
        options.return_fields = vec!["title".to_string()];

        let (_, results) = db.ft_search("idx", "Hello", &options).unwrap();

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].fields.len(), 1);
        assert_eq!(results[0].fields[0].0, "title");
    }

    #[test]
    fn test_ft_search_match_all() {
        use crate::types::{FtField, FtOnType, FtSearchOptions};

        let db = Db::open_memory().unwrap();

        let schema = vec![FtField::text("title")];
        db.ft_create("idx", FtOnType::Hash, &["doc:"], &schema)
            .unwrap();

        db.hset("doc:1", &[("title", b"First")])
            .unwrap();
        db.hset("doc:2", &[("title", b"Second")])
            .unwrap();
        db.hset("doc:3", &[("title", b"Third")])
            .unwrap();

        // Match all with *
        let options = FtSearchOptions::new();
        let (total, _) = db.ft_search("idx", "*", &options).unwrap();

        assert_eq!(total, 3);
    }

    #[test]
    fn test_ft_search_via_alias() {
        use crate::types::{FtField, FtOnType, FtSearchOptions};

        let db = Db::open_memory().unwrap();

        let schema = vec![FtField::text("title")];
        db.ft_create("myindex", FtOnType::Hash, &["doc:"], &schema)
            .unwrap();

        // Create an alias
        db.ft_aliasadd("idx", "myindex").unwrap();

        db.hset("doc:1", &[("title", b"Hello World")])
            .unwrap();

        // Search via alias
        let options = FtSearchOptions::new();
        let (total, results) = db.ft_search("idx", "Hello", &options).unwrap();

        assert_eq!(total, 1);
        assert_eq!(results[0].key, "doc:1");
    }

    #[test]
    fn test_ft_search_nonexistent_index() {
        use crate::types::FtSearchOptions;

        let db = Db::open_memory().unwrap();

        let options = FtSearchOptions::new();
        let result = db.ft_search("nonexistent", "hello", &options);

        assert!(result.is_err());
    }
}
