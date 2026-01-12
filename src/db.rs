use rusqlite::{params, Connection};
use std::sync::Mutex;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::error::{KvError, Result};
use crate::types::{KeyType, SetOptions, ZMember};

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
                // Check expiration - if expired, treat as 0 and clear TTL
                // Lazy expiration: upsert will overwrite the expired data
                if let Some(exp) = expire_at {
                    if exp <= now {
                        (0, None)
                    } else {
                        let s = std::str::from_utf8(&value).map_err(|_| KvError::NotInteger)?;
                        let val = s.parse().map_err(|_| KvError::NotInteger)?;
                        (val, Some(exp))
                    }
                } else {
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
        let conn = self.conn.lock().unwrap_or_else(|e| e.into_inner());
        let db = self.current_db();
        let now = Self::now_ms();

        // Get current value, TTL, and key_id
        let result: std::result::Result<(i64, Vec<u8>, Option<i64>), _> = conn.query_row(
            "SELECT k.id, s.value, k.expire_at
             FROM keys k
             JOIN strings s ON s.key_id = k.id
             WHERE k.db = ?1 AND k.key = ?2",
            params![db, key],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        );

        let (current_val, preserve_ttl): (f64, Option<i64>) = match result {
            Ok((key_id, value, expire_at)) => {
                // Check expiration
                if let Some(exp) = expire_at {
                    if exp <= now {
                        // Expired - delete the old key first, then treat as non-existent
                        conn.execute("DELETE FROM keys WHERE id = ?1", params![key_id])?;
                        (0.0, None)
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

    // --- Session 8: Set operations ---

    /// Helper to create a new set key
    fn create_set_key(&self, conn: &Connection, key: &str) -> Result<i64> {
        let db = self.current_db();
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

        let conn = self.conn.lock().unwrap_or_else(|e| e.into_inner());
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

        let conn = self.conn.lock().unwrap_or_else(|e| e.into_inner());

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
        let conn = self.conn.lock().unwrap_or_else(|e| e.into_inner());

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
        let conn = self.conn.lock().unwrap_or_else(|e| e.into_inner());

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
        let conn = self.conn.lock().unwrap_or_else(|e| e.into_inner());

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
        let conn = self.conn.lock().unwrap_or_else(|e| e.into_inner());

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
        let conn = self.conn.lock().unwrap_or_else(|e| e.into_inner());

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
        let db = self.current_db();
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

        let conn = self.conn.lock().unwrap_or_else(|e| e.into_inner());
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

        let conn = self.conn.lock().unwrap_or_else(|e| e.into_inner());

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
        let conn = self.conn.lock().unwrap_or_else(|e| e.into_inner());

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
        let conn = self.conn.lock().unwrap_or_else(|e| e.into_inner());

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
        let conn = self.conn.lock().unwrap_or_else(|e| e.into_inner());

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
        let conn = self.conn.lock().unwrap_or_else(|e| e.into_inner());

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
        let conn = self.conn.lock().unwrap_or_else(|e| e.into_inner());

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
        let conn = self.conn.lock().unwrap_or_else(|e| e.into_inner());

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
        let conn = self.conn.lock().unwrap_or_else(|e| e.into_inner());

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
        let conn = self.conn.lock().unwrap_or_else(|e| e.into_inner());

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
        let conn = self.conn.lock().unwrap_or_else(|e| e.into_inner());
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
        let conn = self.conn.lock().unwrap_or_else(|e| e.into_inner());

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
        let conn = self.conn.lock().unwrap_or_else(|e| e.into_inner());

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
}
