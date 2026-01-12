use rusqlite::{params, Connection};
use std::sync::Mutex;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::error::{KvError, Result};
use crate::types::{KeyType, SetOptions};

pub struct Db {
    conn: Mutex<Connection>,
    current_db: Mutex<i32>,
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

        let db = Self {
            conn: Mutex::new(conn),
            current_db: Mutex::new(0),
        };

        db.migrate()?;
        Ok(db)
    }

    /// Open an in-memory database (useful for testing)
    pub fn open_memory() -> Result<Self> {
        Self::open(":memory:")
    }

    /// Run schema migrations
    fn migrate(&self) -> Result<()> {
        let conn = self.conn.lock().unwrap_or_else(|e| e.into_inner());
        conn.execute_batch(include_str!("schema.sql"))?;
        Ok(())
    }

    /// Select a database (0-15)
    pub fn select(&self, db: i32) -> Result<()> {
        if !(0..=15).contains(&db) {
            return Err(KvError::SyntaxError);
        }
        *self.current_db.lock().unwrap_or_else(|e| e.into_inner()) = db;
        Ok(())
    }

    /// Get current database number
    pub fn current_db(&self) -> i32 {
        *self.current_db.lock().unwrap_or_else(|e| e.into_inner())
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
        let conn = self.conn.lock().unwrap_or_else(|e| e.into_inner());
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
        let conn = self.conn.lock().unwrap_or_else(|e| e.into_inner());
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

    /// DEL key [key ...]
    pub fn del(&self, keys: &[&str]) -> Result<i64> {
        if keys.is_empty() {
            return Ok(0);
        }

        let conn = self.conn.lock().unwrap_or_else(|e| e.into_inner());
        let db = self.current_db();

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
        let conn = self.conn.lock().unwrap_or_else(|e| e.into_inner());
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
            Ok(type_int) => Ok(KeyType::from_i32(type_int)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    /// TTL key - returns remaining TTL in seconds (-2 if no key, -1 if no expiry)
    pub fn ttl(&self, key: &str) -> Result<i64> {
        let conn = self.conn.lock().unwrap_or_else(|e| e.into_inner());
        let db = self.current_db();
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
        let conn = self.conn.lock().unwrap_or_else(|e| e.into_inner());
        let db = self.current_db();
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
        if keys.is_empty() {
            return Ok(0);
        }

        let conn = self.conn.lock().unwrap_or_else(|e| e.into_inner());
        let db = self.current_db();
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
        let conn = self.conn.lock().unwrap_or_else(|e| e.into_inner());
        let db = self.current_db();
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
        let conn = self.conn.lock().unwrap_or_else(|e| e.into_inner());
        let db = self.current_db();
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
        let conn = self.conn.lock().unwrap_or_else(|e| e.into_inner());
        let db = self.current_db();
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
        let conn = self.conn.lock().unwrap_or_else(|e| e.into_inner());
        let db = self.current_db();
        let now = Self::now_ms();

        // Get current value and TTL
        let result: std::result::Result<(Vec<u8>, Option<i64>), _> = conn.query_row(
            "SELECT s.value, k.expire_at
             FROM keys k
             JOIN strings s ON s.key_id = k.id
             WHERE k.db = ?1 AND k.key = ?2",
            params![db, key],
            |row| Ok((row.get(0)?, row.get(1)?)),
        );

        let (current_val, preserve_ttl): (i64, Option<i64>) = match result {
            Ok((value, expire_at)) => {
                // Check expiration
                if let Some(exp) = expire_at {
                    if exp <= now {
                        (0, None) // Expired, treat as non-existent
                    } else {
                        // Parse as integer, preserve TTL
                        let s = std::str::from_utf8(&value).map_err(|_| KvError::NotInteger)?;
                        let val = s.parse().map_err(|_| KvError::NotInteger)?;
                        (val, Some(exp))
                    }
                } else {
                    let s = std::str::from_utf8(&value).map_err(|_| KvError::NotInteger)?;
                    let val = s.parse().map_err(|_| KvError::NotInteger)?;
                    (val, None) // No TTL to preserve
                }
            }
            Err(rusqlite::Error::QueryReturnedNoRows) => (0, None),
            Err(e) => return Err(e.into()),
        };

        let new_val = current_val + increment;
        let new_val_bytes = new_val.to_string().into_bytes();

        // Upsert key (preserve existing TTL if key existed)
        conn.execute(
            "INSERT INTO keys (db, key, type, expire_at, updated_at)
             VALUES (?1, ?2, ?3, ?4, ?5)
             ON CONFLICT(db, key) DO UPDATE SET
                 type = excluded.type,
                 updated_at = excluded.updated_at",
            params![db, key, KeyType::String as i32, preserve_ttl, now],
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
        let conn = self.conn.lock().unwrap_or_else(|e| e.into_inner());
        let db = self.current_db();
        let now = Self::now_ms();

        // Get current value and TTL
        let result: std::result::Result<(Vec<u8>, Option<i64>), _> = conn.query_row(
            "SELECT s.value, k.expire_at
             FROM keys k
             JOIN strings s ON s.key_id = k.id
             WHERE k.db = ?1 AND k.key = ?2",
            params![db, key],
            |row| Ok((row.get(0)?, row.get(1)?)),
        );

        let (current_val, preserve_ttl): (f64, Option<i64>) = match result {
            Ok((value, expire_at)) => {
                // Check expiration
                if let Some(exp) = expire_at {
                    if exp <= now {
                        (0.0, None) // Expired, treat as non-existent
                    } else {
                        let s = std::str::from_utf8(&value).map_err(|_| KvError::NotFloat)?;
                        let val = s.parse().map_err(|_| KvError::NotFloat)?;
                        (val, Some(exp))
                    }
                } else {
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

        // Upsert key (preserve existing TTL if key existed)
        conn.execute(
            "INSERT INTO keys (db, key, type, expire_at, updated_at)
             VALUES (?1, ?2, ?3, ?4, ?5)
             ON CONFLICT(db, key) DO UPDATE SET
                 type = excluded.type,
                 updated_at = excluded.updated_at",
            params![db, key, KeyType::String as i32, preserve_ttl, now],
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

        let conn = self.conn.lock().unwrap_or_else(|e| e.into_inner());
        let db = self.current_db();
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
        let conn = self.conn.lock().unwrap_or_else(|e| e.into_inner());
        let db = self.current_db();
        let now = Self::now_ms();

        // Get current value
        let result: std::result::Result<(i64, Vec<u8>, Option<i64>), _> = conn.query_row(
            "SELECT k.id, s.value, k.expire_at
             FROM keys k
             JOIN strings s ON s.key_id = k.id
             WHERE k.db = ?1 AND k.key = ?2",
            params![db, key],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        );

        match result {
            Ok((key_id, current_value, expire_at)) => {
                // Check expiration
                let current = if let Some(exp) = expire_at {
                    if exp <= now {
                        Vec::new() // Expired, treat as empty
                    } else {
                        current_value
                    }
                } else {
                    current_value
                };

                let mut new_value = current;
                new_value.extend_from_slice(value);
                let new_len = new_value.len() as i64;

                // Update value (preserve existing TTL - Redis behavior)
                conn.execute(
                    "UPDATE strings SET value = ?1 WHERE key_id = ?2",
                    params![new_value, key_id],
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
        let db = self.current_db();
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
        let db = self.current_db();
        let now = Self::now_ms();

        conn.execute(
            "INSERT INTO keys (db, key, type, updated_at) VALUES (?1, ?2, ?3, ?4)",
            params![db, key, KeyType::Hash as i32, now],
        )?;

        Ok(conn.last_insert_rowid())
    }

    /// Helper to get hash key_id if it exists and is not expired
    fn get_hash_key_id(&self, conn: &Connection, key: &str) -> Result<Option<i64>> {
        let db = self.current_db();
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

        let conn = self.conn.lock().unwrap_or_else(|e| e.into_inner());
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
        let conn = self.conn.lock().unwrap_or_else(|e| e.into_inner());

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
        let conn = self.conn.lock().unwrap_or_else(|e| e.into_inner());

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
        let conn = self.conn.lock().unwrap_or_else(|e| e.into_inner());

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

        let conn = self.conn.lock().unwrap_or_else(|e| e.into_inner());

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
        let conn = self.conn.lock().unwrap_or_else(|e| e.into_inner());

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
        let conn = self.conn.lock().unwrap_or_else(|e| e.into_inner());

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
        let conn = self.conn.lock().unwrap_or_else(|e| e.into_inner());

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
        let conn = self.conn.lock().unwrap_or_else(|e| e.into_inner());

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
        let conn = self.conn.lock().unwrap_or_else(|e| e.into_inner());
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
        let conn = self.conn.lock().unwrap_or_else(|e| e.into_inner());
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
        let conn = self.conn.lock().unwrap_or_else(|e| e.into_inner());
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

    /// Helper to get or create a list key, returns key_id
    fn get_or_create_list_key(&self, conn: &Connection, key: &str) -> Result<i64> {
        let db = self.current_db();
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
        let db = self.current_db();
        let now = Self::now_ms();

        conn.execute(
            "INSERT INTO keys (db, key, type, updated_at) VALUES (?1, ?2, ?3, ?4)",
            params![db, key, KeyType::List as i32, now],
        )?;

        Ok(conn.last_insert_rowid())
    }

    /// Helper to get list key_id if it exists and is not expired
    fn get_list_key_id(&self, conn: &Connection, key: &str) -> Result<Option<i64>> {
        let db = self.current_db();
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

        let conn = self.conn.lock().unwrap_or_else(|e| e.into_inner());
        let key_id = self.get_or_create_list_key(&conn, key)?;

        // Get current min position (or start at 0 if empty)
        let min_pos: i64 = conn
            .query_row(
                "SELECT MIN(pos) FROM lists WHERE key_id = ?1",
                params![key_id],
                |row| row.get::<_, Option<i64>>(0),
            )
            .unwrap_or(None)
            .unwrap_or(Self::LIST_GAP);

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

        Ok(length)
    }

    /// RPUSH key element [element ...] - append elements to list, returns length
    pub fn rpush(&self, key: &str, values: &[&[u8]]) -> Result<i64> {
        if values.is_empty() {
            return Ok(0);
        }

        let conn = self.conn.lock().unwrap_or_else(|e| e.into_inner());
        let key_id = self.get_or_create_list_key(&conn, key)?;

        // Get current max position (or start at 0 if empty)
        let max_pos: i64 = conn
            .query_row(
                "SELECT MAX(pos) FROM lists WHERE key_id = ?1",
                params![key_id],
                |row| row.get::<_, Option<i64>>(0),
            )
            .unwrap_or(None)
            .unwrap_or(0);

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

        Ok(length)
    }

    /// LPOP key [count] - remove and return elements from head
    pub fn lpop(&self, key: &str, count: Option<usize>) -> Result<Vec<Vec<u8>>> {
        let conn = self.conn.lock().unwrap_or_else(|e| e.into_inner());

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
        let conn = self.conn.lock().unwrap_or_else(|e| e.into_inner());

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
        let conn = self.conn.lock().unwrap_or_else(|e| e.into_inner());

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
        let conn = self.conn.lock().unwrap_or_else(|e| e.into_inner());

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
        let conn = self.conn.lock().unwrap_or_else(|e| e.into_inner());

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
        let conn = self.conn.lock().unwrap_or_else(|e| e.into_inner());

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
        let conn = self.conn.lock().unwrap_or_else(|e| e.into_inner());

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
        let db = Db::open_memory().unwrap();

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
        let db = Db::open(&path).unwrap();

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
}
