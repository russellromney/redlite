# Redlite Database Implementation

## Core Db Struct (Session 15.1+)

The database uses a two-tier architecture:
- **DbCore**: Shared backend (SQLite connection, autovacuum state, optional notifier)
- **Db**: Per-session wrapper (selected database, notifier reference)

```rust
// src/db.rs

use rusqlite::{Connection, params};
use std::sync::{Arc, Mutex, RwLock};
use std::collections::HashMap;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::broadcast;

use crate::error::{KvError, Result};
use crate::types::{KeyType, SetOptions};

/// Shared database backend
struct DbCore {
    conn: Mutex<Connection>,
    autovacuum_enabled: AtomicBool,
    last_cleanup: AtomicI64,
    autovacuum_interval_ms: AtomicI64,
    /// Optional notifier for server mode (None for embedded mode)
    /// Maps key name to broadcast sender for notifications (Session 15.1+)
    notifier: RwLock<Option<Arc<RwLock<HashMap<String, broadcast::Sender<()>>>>>>,
}

/// Per-session database wrapper
#[derive(Clone)]
pub struct Db {
    core: Arc<DbCore>,
    selected_db: i32,  // Current database (0-15)
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
             PRAGMA busy_timeout = 5000;"
        )?;

        let core = Arc::new(DbCore {
            conn: Mutex::new(conn),
            autovacuum_enabled: AtomicBool::new(true),
            last_cleanup: AtomicI64::new(0),
            autovacuum_interval_ms: AtomicI64::new(DEFAULT_AUTOVACUUM_INTERVAL_MS),
            notifier: RwLock::new(None),  // Embedded mode by default
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

    /// Create a new session sharing the same database backend
    /// The new session starts at database 0
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
    pub fn select(&self, db: i32) -> Result<()> {
        if db < 0 || db > 15 {
            return Err(KvError::SyntaxError);
        }
        *self.current_db.lock().unwrap() = db;
        Ok(())
    }

    /// Get current database number
    pub fn current_db(&self) -> i32 {
        *self.current_db.lock().unwrap()
    }

    /// Current time in milliseconds since epoch
    pub fn now_ms() -> i64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64
    }
}
```

## String Commands

```rust
// src/commands/strings.rs

impl Db {
    /// GET key
    pub fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
        let conn = self.conn.lock().unwrap();
        let db = self.current_db();
        let now = Self::now_ms();

        let result: std::result::Result<(Vec<u8>, Option<i64>), _> = conn.query_row(
            "SELECT s.value, k.expire_at
             FROM keys k
             JOIN strings s ON s.key_id = k.id
             WHERE k.db = ?1 AND k.key = ?2",
            params![db, key],
            |row| Ok((row.get(0)?, row.get(1)?)),
        );

        match result {
            Ok((value, expire_at)) => {
                // Check expiration
                if let Some(exp) = expire_at {
                    if exp <= now {
                        // Lazy delete - drop lock first
                        drop(conn);
                        let _ = self.del(&[key]);
                        return Ok(None);
                    }
                }
                Ok(Some(value))
            }
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    /// SET key value [EX seconds] [PX ms] [NX|XX]
    pub fn set(&self, key: &str, value: &[u8], ttl: Option<Duration>) -> Result<()> {
        self.set_opts(key, value, SetOptions { ttl, ..Default::default() })
            .map(|_| ())
    }

    /// SET with options, returns whether the key was set
    pub fn set_opts(&self, key: &str, value: &[u8], opts: SetOptions) -> Result<bool> {
        let conn = self.conn.lock().unwrap();
        let db = self.current_db();
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

    /// INCR key
    pub fn incr(&self, key: &str) -> Result<i64> {
        self.incr_by(key, 1)
    }

    /// DECR key
    pub fn decr(&self, key: &str) -> Result<i64> {
        self.incr_by(key, -1)
    }

    /// INCRBY key delta
    pub fn incr_by(&self, key: &str, delta: i64) -> Result<i64> {
        let current = self.get(key)?;

        let val: i64 = match current {
            Some(bytes) => {
                let s = std::str::from_utf8(&bytes).map_err(|_| KvError::NotInteger)?;
                s.parse().map_err(|_| KvError::NotInteger)?
            }
            None => 0,
        };

        let new_val = val + delta;
        self.set(key, new_val.to_string().as_bytes(), None)?;
        Ok(new_val)
    }

    /// INCRBYFLOAT key delta
    pub fn incr_by_float(&self, key: &str, delta: f64) -> Result<f64> {
        let current = self.get(key)?;

        let val: f64 = match current {
            Some(bytes) => {
                let s = std::str::from_utf8(&bytes).map_err(|_| KvError::NotFloat)?;
                s.parse().map_err(|_| KvError::NotFloat)?
            }
            None => 0.0,
        };

        let new_val = val + delta;
        self.set(key, new_val.to_string().as_bytes(), None)?;
        Ok(new_val)
    }

    /// MGET key [key ...]
    pub fn mget(&self, keys: &[&str]) -> Result<Vec<Option<Vec<u8>>>> {
        keys.iter().map(|k| self.get(k)).collect()
    }

    /// MSET key value [key value ...]
    pub fn mset(&self, pairs: &[(&str, &[u8])]) -> Result<()> {
        for (key, value) in pairs {
            self.set(key, value, None)?;
        }
        Ok(())
    }

    /// APPEND key value
    pub fn append(&self, key: &str, value: &[u8]) -> Result<i64> {
        let current = self.get(key)?.unwrap_or_default();
        let mut new_value = current;
        new_value.extend_from_slice(value);
        let len = new_value.len() as i64;
        self.set(key, &new_value, None)?;
        Ok(len)
    }

    /// STRLEN key
    pub fn strlen(&self, key: &str) -> Result<i64> {
        Ok(self.get(key)?.map(|v| v.len() as i64).unwrap_or(0))
    }

    /// GETSET key value (deprecated but still used)
    pub fn getset(&self, key: &str, value: &[u8]) -> Result<Option<Vec<u8>>> {
        let old = self.get(key)?;
        self.set(key, value, None)?;
        Ok(old)
    }

    /// SETNX key value
    pub fn setnx(&self, key: &str, value: &[u8]) -> Result<bool> {
        self.set_opts(key, value, SetOptions::new().nx())
    }

    /// SETEX key seconds value
    pub fn setex(&self, key: &str, seconds: u64, value: &[u8]) -> Result<()> {
        self.set(key, value, Some(Duration::from_secs(seconds)))
    }

    /// PSETEX key milliseconds value
    pub fn psetex(&self, key: &str, ms: u64, value: &[u8]) -> Result<()> {
        self.set(key, value, Some(Duration::from_millis(ms)))
    }
}
```

## Key Commands

```rust
// src/commands/keys.rs

impl Db {
    /// DEL key [key ...]
    pub fn del(&self, keys: &[&str]) -> Result<i64> {
        if keys.is_empty() {
            return Ok(0);
        }

        let conn = self.conn.lock().unwrap();
        let db = self.current_db();

        let placeholders: String = (0..keys.len()).map(|i| format!("?{}", i + 2)).collect::<Vec<_>>().join(",");
        let sql = format!("DELETE FROM keys WHERE db = ?1 AND key IN ({})", placeholders);

        let mut stmt = conn.prepare(&sql)?;

        // Build params: [db, key1, key2, ...]
        let mut params_vec: Vec<&dyn rusqlite::ToSql> = vec![&db];
        for key in keys {
            params_vec.push(key);
        }

        let count = stmt.execute(params_vec.as_slice())?;
        Ok(count as i64)
    }

    /// EXISTS key [key ...]
    pub fn exists(&self, keys: &[&str]) -> Result<i64> {
        if keys.is_empty() {
            return Ok(0);
        }

        let conn = self.conn.lock().unwrap();
        let db = self.current_db();
        let now = Self::now_ms();

        let mut count = 0i64;
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

    /// EXPIRE key seconds
    pub fn expire(&self, key: &str, ttl: Duration) -> Result<bool> {
        let conn = self.conn.lock().unwrap();
        let db = self.current_db();
        let now = Self::now_ms();
        let expire_at = now + ttl.as_millis() as i64;

        let count = conn.execute(
            "UPDATE keys SET expire_at = ?1, updated_at = ?2
             WHERE db = ?3 AND key = ?4
               AND (expire_at IS NULL OR expire_at > ?2)",
            params![expire_at, now, db, key],
        )?;

        Ok(count > 0)
    }

    /// EXPIREAT key timestamp
    pub fn expire_at(&self, key: &str, timestamp_secs: i64) -> Result<bool> {
        let conn = self.conn.lock().unwrap();
        let db = self.current_db();
        let now = Self::now_ms();
        let expire_at = timestamp_secs * 1000;

        let count = conn.execute(
            "UPDATE keys SET expire_at = ?1, updated_at = ?2
             WHERE db = ?3 AND key = ?4",
            params![expire_at, now, db, key],
        )?;

        Ok(count > 0)
    }

    /// PEXPIRE key milliseconds
    pub fn pexpire(&self, key: &str, ttl_ms: i64) -> Result<bool> {
        self.expire(key, Duration::from_millis(ttl_ms as u64))
    }

    /// PEXPIREAT key timestamp_ms
    pub fn pexpire_at(&self, key: &str, timestamp_ms: i64) -> Result<bool> {
        let conn = self.conn.lock().unwrap();
        let db = self.current_db();
        let now = Self::now_ms();

        let count = conn.execute(
            "UPDATE keys SET expire_at = ?1, updated_at = ?2
             WHERE db = ?3 AND key = ?4",
            params![timestamp_ms, now, db, key],
        )?;

        Ok(count > 0)
    }

    /// TTL key (returns seconds, -1 if no expiry, -2 if not found)
    pub fn ttl(&self, key: &str) -> Result<i64> {
        let pttl = self.pttl(key)?;
        match pttl {
            -1 | -2 => Ok(pttl),
            ms => Ok(ms / 1000),
        }
    }

    /// PTTL key (returns milliseconds, -1 if no expiry, -2 if not found)
    pub fn pttl(&self, key: &str) -> Result<i64> {
        let conn = self.conn.lock().unwrap();
        let db = self.current_db();
        let now = Self::now_ms();

        let result: std::result::Result<Option<i64>, _> = conn.query_row(
            "SELECT expire_at FROM keys WHERE db = ?1 AND key = ?2",
            params![db, key],
            |row| row.get(0),
        );

        match result {
            Ok(Some(expire_at)) => {
                let remaining = expire_at - now;
                if remaining <= 0 {
                    Ok(-2) // Expired
                } else {
                    Ok(remaining)
                }
            }
            Ok(None) => Ok(-1), // No expiration
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(-2), // Key doesn't exist
            Err(e) => Err(e.into()),
        }
    }

    /// PERSIST key (remove expiration)
    pub fn persist(&self, key: &str) -> Result<bool> {
        let conn = self.conn.lock().unwrap();
        let db = self.current_db();
        let now = Self::now_ms();

        let count = conn.execute(
            "UPDATE keys SET expire_at = NULL, updated_at = ?1
             WHERE db = ?2 AND key = ?3 AND expire_at IS NOT NULL",
            params![now, db, key],
        )?;

        Ok(count > 0)
    }

    /// RENAME key newkey
    pub fn rename(&self, key: &str, new_key: &str) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        let db = self.current_db();
        let now = Self::now_ms();

        // Check if source key exists
        let exists: bool = conn
            .query_row(
                "SELECT 1 FROM keys WHERE db = ?1 AND key = ?2
                 AND (expire_at IS NULL OR expire_at > ?3)",
                params![db, key, now],
                |_| Ok(true),
            )
            .unwrap_or(false);

        if !exists {
            return Err(KvError::NotFound);
        }

        // Delete target key if exists (Redis behavior)
        conn.execute(
            "DELETE FROM keys WHERE db = ?1 AND key = ?2",
            params![db, new_key],
        )?;

        // Rename
        conn.execute(
            "UPDATE keys SET key = ?1, updated_at = ?2 WHERE db = ?3 AND key = ?4",
            params![new_key, now, db, key],
        )?;

        Ok(())
    }

    /// RENAMENX key newkey
    pub fn renamenx(&self, key: &str, new_key: &str) -> Result<bool> {
        let conn = self.conn.lock().unwrap();
        let db = self.current_db();
        let now = Self::now_ms();

        // Check if target exists
        let target_exists: bool = conn
            .query_row(
                "SELECT 1 FROM keys WHERE db = ?1 AND key = ?2
                 AND (expire_at IS NULL OR expire_at > ?3)",
                params![db, new_key, now],
                |_| Ok(true),
            )
            .unwrap_or(false);

        if target_exists {
            return Ok(false);
        }

        // Check source exists
        let source_exists: bool = conn
            .query_row(
                "SELECT 1 FROM keys WHERE db = ?1 AND key = ?2
                 AND (expire_at IS NULL OR expire_at > ?3)",
                params![db, key, now],
                |_| Ok(true),
            )
            .unwrap_or(false);

        if !source_exists {
            return Err(KvError::NotFound);
        }

        conn.execute(
            "UPDATE keys SET key = ?1, updated_at = ?2 WHERE db = ?3 AND key = ?4",
            params![new_key, now, db, key],
        )?;

        Ok(true)
    }

    /// TYPE key
    pub fn key_type(&self, key: &str) -> Result<Option<KeyType>> {
        let conn = self.conn.lock().unwrap();
        let db = self.current_db();
        let now = Self::now_ms();

        let result: std::result::Result<i32, _> = conn.query_row(
            "SELECT type FROM keys
             WHERE db = ?1 AND key = ?2
               AND (expire_at IS NULL OR expire_at > ?3)",
            params![db, key, now],
            |row| row.get(0),
        );

        match result {
            Ok(t) => Ok(KeyType::from_i32(t)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    /// KEYS pattern
    pub fn keys(&self, pattern: &str) -> Result<Vec<String>> {
        let conn = self.conn.lock().unwrap();
        let db = self.current_db();
        let now = Self::now_ms();

        let mut stmt = conn.prepare(
            "SELECT key FROM keys
             WHERE db = ?1 AND (expire_at IS NULL OR expire_at > ?2) AND key GLOB ?3
             ORDER BY key",
        )?;

        let keys: Vec<String> = stmt
            .query_map(params![db, now, pattern], |row| row.get(0))?
            .filter_map(|r| r.ok())
            .collect();

        Ok(keys)
    }

    /// DBSIZE
    pub fn dbsize(&self) -> Result<i64> {
        let conn = self.conn.lock().unwrap();
        let db = self.current_db();
        let now = Self::now_ms();

        let count: i64 = conn.query_row(
            "SELECT COUNT(*) FROM keys
             WHERE db = ?1 AND (expire_at IS NULL OR expire_at > ?2)",
            params![db, now],
            |row| row.get(0),
        )?;

        Ok(count)
    }

    /// FLUSHDB
    pub fn flushdb(&self) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        let db = self.current_db();
        conn.execute("DELETE FROM keys WHERE db = ?1", params![db])?;
        Ok(())
    }

    /// RANDOMKEY
    pub fn randomkey(&self) -> Result<Option<String>> {
        let conn = self.conn.lock().unwrap();
        let db = self.current_db();
        let now = Self::now_ms();

        let result: std::result::Result<String, _> = conn.query_row(
            "SELECT key FROM keys
             WHERE db = ?1 AND (expire_at IS NULL OR expire_at > ?2)
             ORDER BY RANDOM() LIMIT 1",
            params![db, now],
            |row| row.get(0),
        );

        match result {
            Ok(key) => Ok(Some(key)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    // Server mode notification methods (Session 15.1+)

    /// Check if database is in server mode (has notifier attached)
    pub fn is_server_mode(&self) -> bool {
        self.core.notifier.read().unwrap().is_some()
    }

    /// Send notification to all subscribers of a key
    /// (no-op if not in server mode)
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
    /// - In server mode: returns active broadcast receiver
    /// - In embedded mode: returns closed receiver (fails immediately)
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
            // For embedded mode - return a closed receiver
            let (tx, rx) = broadcast::channel(1);
            drop(tx); // Sender dropped, receiver returns Closed
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
}
```

## Expiration Daemon

```rust
// src/expire.rs

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use crate::db::Db;
use crate::error::Result;

impl Db {
    /// Expire up to `limit` keys
    pub fn expire_some(&self, limit: i64) -> Result<i64> {
        let conn = self.conn.lock().unwrap();
        let now = Self::now_ms();

        let count = conn.execute(
            "DELETE FROM keys
             WHERE id IN (
                 SELECT id FROM keys
                 WHERE expire_at IS NOT NULL AND expire_at <= ?1
                 LIMIT ?2
             )",
            params![now, limit],
        )?;

        Ok(count as i64)
    }

    /// Start background expiration daemon
    pub fn start_expiration_daemon(
        self: &Arc<Self>,
        shutdown: Arc<AtomicBool>,
    ) -> thread::JoinHandle<()> {
        let db = Arc::clone(self);

        thread::spawn(move || {
            while !shutdown.load(Ordering::Relaxed) {
                let _ = db.expire_some(20);
                thread::sleep(Duration::from_millis(100));
            }
        })
    }
}
```

## Helper Methods

```rust
// src/db.rs (continued)

impl Db {
    /// Ensure a key exists with the given type, creating if needed
    pub(crate) fn ensure_key(&self, conn: &Connection, key: &str, key_type: KeyType) -> Result<i64> {
        let db = self.current_db();
        let now = Self::now_ms();

        conn.execute(
            "INSERT INTO keys (db, key, type, updated_at)
             VALUES (?1, ?2, ?3, ?4)
             ON CONFLICT(db, key) DO UPDATE SET updated_at = excluded.updated_at",
            params![db, key, key_type as i32, now],
        )?;

        let key_id: i64 = conn.query_row(
            "SELECT id FROM keys WHERE db = ?1 AND key = ?2",
            params![db, key],
            |row| row.get(0),
        )?;

        // Verify type matches
        let actual_type: i32 = conn.query_row(
            "SELECT type FROM keys WHERE id = ?1",
            params![key_id],
            |row| row.get(0),
        )?;

        if actual_type != key_type as i32 {
            return Err(KvError::WrongType);
        }

        Ok(key_id)
    }

    /// Get key_id if it exists and has the expected type
    pub(crate) fn get_key_id(&self, conn: &Connection, key: &str, expected_type: KeyType) -> Result<Option<i64>> {
        let db = self.current_db();
        let now = Self::now_ms();

        let result: std::result::Result<(i64, i32), _> = conn.query_row(
            "SELECT id, type FROM keys
             WHERE db = ?1 AND key = ?2
               AND (expire_at IS NULL OR expire_at > ?3)",
            params![db, key, now],
            |row| Ok((row.get(0)?, row.get(1)?)),
        );

        match result {
            Ok((id, t)) => {
                if t != expected_type as i32 {
                    Err(KvError::WrongType)
                } else {
                    Ok(Some(id))
                }
            }
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }
}
```
