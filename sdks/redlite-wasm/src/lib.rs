mod db;
mod error;
mod schema;
mod types;

use db::DbCore;
use error::{Result, WasmError};
use types::{HistoryRetention, JsonSetOptions, KeyInfo, KeyType, SetOptions, ZMember};
use wasm_bindgen::prelude::*;

/// Redlite WASM - Redis-compatible embedded database for WebAssembly
#[wasm_bindgen]
pub struct RedliteWasm {
    core: DbCore,
}

#[wasm_bindgen]
impl RedliteWasm {
    /// Create a new in-memory database
    #[wasm_bindgen(constructor)]
    pub fn new() -> std::result::Result<RedliteWasm, JsError> {
        let core = DbCore::new()?;
        Ok(Self { core })
    }

    /// Select a database (0-15, like Redis)
    pub fn select(&mut self, db: i32) -> std::result::Result<(), JsError> {
        self.core.select(db)?;
        Ok(())
    }

    // ========================================================================
    // String Commands
    // ========================================================================

    /// GET key - Get the value of a key
    pub fn get(&self, key: &str) -> std::result::Result<Option<Vec<u8>>, JsError> {
        let key_id = match self.core.get_key_id_typed(key, KeyType::String)? {
            Some(id) => id,
            None => return Ok(None),
        };

        let sql = "SELECT value FROM strings WHERE key_id = ?1";
        let mut stmt = self.core.prepare(sql)?;
        stmt.bind_int64(1, key_id)?;

        if stmt.step()? {
            Ok(Some(stmt.column_blob(0)))
        } else {
            Ok(None)
        }
    }

    /// SET key value [EX seconds] [PX milliseconds] [NX|XX]
    pub fn set(
        &mut self,
        key: &str,
        value: &[u8],
        options: Option<SetOptions>,
    ) -> std::result::Result<bool, JsError> {
        let opts = options.unwrap_or_default();

        // Check NX/XX conditions
        let existing_id = self.core.get_key_id(key)?;
        if opts.nx && existing_id.is_some() {
            return Ok(false);
        }
        if opts.xx && existing_id.is_none() {
            return Ok(false);
        }

        let key_id = self.core.upsert_key(key, KeyType::String, opts.ttl_ms())?;

        let sql = r#"
            INSERT INTO strings (key_id, value) VALUES (?1, ?2)
            ON CONFLICT(key_id) DO UPDATE SET value = excluded.value
        "#;
        let mut stmt = self.core.prepare(sql)?;
        stmt.bind_int64(1, key_id)?;
        stmt.bind_blob(2, value)?;
        stmt.step()?;

        Ok(true)
    }

    /// DEL key [key ...] - Delete one or more keys
    pub fn del(&mut self, keys: Vec<String>) -> std::result::Result<i64, JsError> {
        let mut deleted = 0i64;
        for key in keys {
            if self.core.delete_key_by_name(&key)? {
                deleted += 1;
            }
        }
        Ok(deleted)
    }

    /// EXISTS key [key ...] - Check if keys exist
    pub fn exists(&self, keys: Vec<String>) -> std::result::Result<i64, JsError> {
        let mut count = 0i64;
        for key in keys {
            if self.core.get_key_id(&key)?.is_some() {
                count += 1;
            }
        }
        Ok(count)
    }

    /// INCR key - Increment the integer value of a key by one
    pub fn incr(&mut self, key: &str) -> std::result::Result<i64, JsError> {
        self.incrby(key, 1)
    }

    /// INCRBY key increment - Increment the integer value of a key
    pub fn incrby(&mut self, key: &str, increment: i64) -> std::result::Result<i64, JsError> {
        let current = match self.get(key)? {
            Some(data) => {
                let s = String::from_utf8(data).map_err(|_| WasmError::NotInteger)?;
                s.parse::<i64>().map_err(|_| WasmError::NotInteger)?
            }
            None => 0,
        };

        let new_value = current + increment;
        self.set(key, new_value.to_string().as_bytes(), None)?;
        Ok(new_value)
    }

    /// DECR key - Decrement the integer value of a key by one
    pub fn decr(&mut self, key: &str) -> std::result::Result<i64, JsError> {
        self.incrby(key, -1)
    }

    /// DECRBY key decrement - Decrement the integer value of a key
    pub fn decrby(&mut self, key: &str, decrement: i64) -> std::result::Result<i64, JsError> {
        self.incrby(key, -decrement)
    }

    /// INCRBYFLOAT key increment - Increment the float value of a key
    pub fn incrbyfloat(&mut self, key: &str, increment: f64) -> std::result::Result<f64, JsError> {
        let current = match self.get(key)? {
            Some(data) => {
                let s = String::from_utf8(data).map_err(|_| WasmError::NotFloat)?;
                s.parse::<f64>().map_err(|_| WasmError::NotFloat)?
            }
            None => 0.0,
        };

        let new_value = current + increment;
        self.set(key, new_value.to_string().as_bytes(), None)?;
        Ok(new_value)
    }

    /// APPEND key value - Append a value to a key
    pub fn append(&mut self, key: &str, value: &[u8]) -> std::result::Result<i64, JsError> {
        let current = self.get(key)?.unwrap_or_default();
        let mut new_value = current;
        new_value.extend_from_slice(value);
        let len = new_value.len() as i64;
        self.set(key, &new_value, None)?;
        Ok(len)
    }

    /// STRLEN key - Get the length of the value stored at a key
    pub fn strlen(&self, key: &str) -> std::result::Result<i64, JsError> {
        match self.get(key)? {
            Some(data) => Ok(data.len() as i64),
            None => Ok(0),
        }
    }

    /// SETNX key value - Set the value of a key only if it does not exist
    pub fn setnx(&mut self, key: &str, value: &[u8]) -> std::result::Result<bool, JsError> {
        let opts = SetOptions::new().with_nx();
        self.set(key, value, Some(opts))
    }

    /// SETEX key seconds value - Set the value and expiration of a key
    pub fn setex(
        &mut self,
        key: &str,
        seconds: i64,
        value: &[u8],
    ) -> std::result::Result<(), JsError> {
        let opts = SetOptions::new().with_ex(seconds);
        self.set(key, value, Some(opts))?;
        Ok(())
    }

    /// PSETEX key milliseconds value - Set the value and expiration in milliseconds
    pub fn psetex(
        &mut self,
        key: &str,
        millis: i64,
        value: &[u8],
    ) -> std::result::Result<(), JsError> {
        let opts = SetOptions::new().with_px(millis);
        self.set(key, value, Some(opts))?;
        Ok(())
    }

    /// GETSET key value - Set the string value and return the old value
    pub fn getset(
        &mut self,
        key: &str,
        value: &[u8],
    ) -> std::result::Result<Option<Vec<u8>>, JsError> {
        let old = self.get(key)?;
        self.set(key, value, None)?;
        Ok(old)
    }

    /// MGET key [key ...] - Get the values of all given keys
    pub fn mget(&self, keys: Vec<String>) -> std::result::Result<Vec<Option<Vec<u8>>>, JsError> {
        let mut results = Vec::with_capacity(keys.len());
        for key in keys {
            results.push(self.get(&key)?);
        }
        Ok(results)
    }

    /// MSET key value [key value ...] - Set multiple keys to multiple values
    pub fn mset(&mut self, pairs: Vec<JsValue>) -> std::result::Result<(), JsError> {
        // pairs is [key1, value1, key2, value2, ...]
        if pairs.len() % 2 != 0 {
            return Err(WasmError::InvalidArgument("MSET requires key-value pairs".to_string()).into());
        }

        for i in (0..pairs.len()).step_by(2) {
            let key = pairs[i]
                .as_string()
                .ok_or_else(|| WasmError::InvalidArgument("key must be a string".to_string()))?;
            let value: Vec<u8> = serde_wasm_bindgen::from_value(pairs[i + 1].clone())
                .map_err(|e| WasmError::InvalidArgument(e.to_string()))?;
            self.set(&key, &value, None)?;
        }
        Ok(())
    }

    // ========================================================================
    // Key Commands
    // ========================================================================

    /// KEYS pattern - Find all keys matching the given pattern
    pub fn keys(&self, pattern: &str) -> std::result::Result<Vec<String>, JsError> {
        // Convert Redis glob pattern to SQL LIKE pattern
        let like_pattern = pattern
            .replace('%', "\\%")
            .replace('_', "\\_")
            .replace('*', "%")
            .replace('?', "_");

        let now = DbCore::now_ms();
        let sql = r#"
            SELECT key FROM keys
            WHERE db = ?1 AND key LIKE ?2 ESCAPE '\'
            AND (expire_at IS NULL OR expire_at > ?3)
        "#;

        let mut stmt = self.core.prepare(sql)?;
        stmt.bind_int(1, self.core.selected_db())?;
        stmt.bind_text(2, &like_pattern)?;
        stmt.bind_int64(3, now)?;

        let mut keys = Vec::new();
        while stmt.step()? {
            keys.push(stmt.column_text(0));
        }
        Ok(keys)
    }

    /// EXPIRE key seconds - Set a key's time to live in seconds
    pub fn expire(&mut self, key: &str, seconds: i64) -> std::result::Result<bool, JsError> {
        self.pexpire(key, seconds * 1000)
    }

    /// PEXPIRE key milliseconds - Set a key's time to live in milliseconds
    pub fn pexpire(&mut self, key: &str, millis: i64) -> std::result::Result<bool, JsError> {
        let key_id = match self.core.get_key_id(key)? {
            Some(id) => id,
            None => return Ok(false),
        };

        let expire_at = DbCore::now_ms() + millis;
        let sql = "UPDATE keys SET expire_at = ?1 WHERE id = ?2";
        let mut stmt = self.core.prepare(sql)?;
        stmt.bind_int64(1, expire_at)?;
        stmt.bind_int64(2, key_id)?;
        stmt.step()?;
        Ok(true)
    }

    /// EXPIREAT key timestamp - Set the expiration for a key as a Unix timestamp
    pub fn expireat(&mut self, key: &str, timestamp: i64) -> std::result::Result<bool, JsError> {
        self.pexpireat(key, timestamp * 1000)
    }

    /// PEXPIREAT key timestamp - Set key expiration as Unix timestamp in milliseconds
    pub fn pexpireat(&mut self, key: &str, timestamp_ms: i64) -> std::result::Result<bool, JsError> {
        let key_id = match self.core.get_key_id(key)? {
            Some(id) => id,
            None => return Ok(false),
        };

        let sql = "UPDATE keys SET expire_at = ?1 WHERE id = ?2";
        let mut stmt = self.core.prepare(sql)?;
        stmt.bind_int64(1, timestamp_ms)?;
        stmt.bind_int64(2, key_id)?;
        stmt.step()?;
        Ok(true)
    }

    /// TTL key - Get the time to live for a key in seconds
    pub fn ttl(&self, key: &str) -> std::result::Result<i64, JsError> {
        let pttl = self.pttl(key)?;
        if pttl < 0 {
            Ok(pttl)
        } else {
            Ok(pttl / 1000)
        }
    }

    /// PTTL key - Get the time to live for a key in milliseconds
    pub fn pttl(&self, key: &str) -> std::result::Result<i64, JsError> {
        let sql = "SELECT expire_at FROM keys WHERE db = ?1 AND key = ?2";
        let mut stmt = self.core.prepare(sql)?;
        stmt.bind_int(1, self.core.selected_db())?;
        stmt.bind_text(2, key)?;

        if stmt.step()? {
            match stmt.column_int64_opt(0) {
                Some(expire_at) => {
                    let now = DbCore::now_ms();
                    if expire_at <= now {
                        Ok(-2) // Key has expired
                    } else {
                        Ok(expire_at - now)
                    }
                }
                None => Ok(-1), // Key exists but has no expiry
            }
        } else {
            Ok(-2) // Key does not exist
        }
    }

    /// PERSIST key - Remove the expiration from a key
    pub fn persist(&mut self, key: &str) -> std::result::Result<bool, JsError> {
        let key_id = match self.core.get_key_id(key)? {
            Some(id) => id,
            None => return Ok(false),
        };

        let sql = "UPDATE keys SET expire_at = NULL WHERE id = ?1 AND expire_at IS NOT NULL";
        let mut stmt = self.core.prepare(sql)?;
        stmt.bind_int64(1, key_id)?;
        stmt.step()?;
        Ok(true)
    }

    /// TYPE key - Determine the type stored at key
    #[wasm_bindgen(js_name = "type")]
    pub fn key_type(&self, key: &str) -> std::result::Result<Option<String>, JsError> {
        let sql = "SELECT type FROM keys WHERE db = ?1 AND key = ?2";
        let mut stmt = self.core.prepare(sql)?;
        stmt.bind_int(1, self.core.selected_db())?;
        stmt.bind_text(2, key)?;

        if stmt.step()? {
            let type_id = stmt.column_int(0);
            match KeyType::from_i32(type_id) {
                Some(kt) => Ok(Some(kt.as_str().to_string())),
                None => Ok(Some("unknown".to_string())),
            }
        } else {
            Ok(None)
        }
    }

    /// RENAME key newkey - Rename a key
    pub fn rename(&mut self, key: &str, newkey: &str) -> std::result::Result<(), JsError> {
        let key_id = match self.core.get_key_id(key)? {
            Some(id) => id,
            None => return Err(WasmError::NoSuchKey.into()),
        };

        // Delete newkey if it exists
        self.core.delete_key_by_name(newkey)?;

        // Rename the key
        let sql = "UPDATE keys SET key = ?1 WHERE id = ?2";
        let mut stmt = self.core.prepare(sql)?;
        stmt.bind_text(1, newkey)?;
        stmt.bind_int64(2, key_id)?;
        stmt.step()?;
        Ok(())
    }

    /// RENAMENX key newkey - Rename a key only if the new key does not exist
    pub fn renamenx(&mut self, key: &str, newkey: &str) -> std::result::Result<bool, JsError> {
        if self.core.get_key_id(newkey)?.is_some() {
            return Ok(false);
        }
        self.rename(key, newkey)?;
        Ok(true)
    }

    /// DBSIZE - Return the number of keys in the selected database
    pub fn dbsize(&self) -> std::result::Result<i64, JsError> {
        let now = DbCore::now_ms();
        let sql = "SELECT COUNT(*) FROM keys WHERE db = ?1 AND (expire_at IS NULL OR expire_at > ?2)";
        let mut stmt = self.core.prepare(sql)?;
        stmt.bind_int(1, self.core.selected_db())?;
        stmt.bind_int64(2, now)?;
        stmt.step()?;
        Ok(stmt.column_int64(0))
    }

    /// FLUSHDB - Remove all keys from the current database
    pub fn flushdb(&mut self) -> std::result::Result<(), JsError> {
        let sql = "DELETE FROM keys WHERE db = ?1";
        let mut stmt = self.core.prepare(sql)?;
        stmt.bind_int(1, self.core.selected_db())?;
        stmt.step()?;
        Ok(())
    }

    /// FLUSHALL - Remove all keys from all databases
    pub fn flushall(&mut self) -> std::result::Result<(), JsError> {
        self.core.execute_batch("DELETE FROM keys")?;
        Ok(())
    }

    // ========================================================================
    // Hash Commands
    // ========================================================================

    /// HSET key field value [field value ...] - Set hash field(s)
    pub fn hset(
        &mut self,
        key: &str,
        field: &str,
        value: &[u8],
    ) -> std::result::Result<bool, JsError> {
        let key_id = self.core.upsert_key(key, KeyType::Hash, None)?;

        let sql = r#"
            INSERT INTO hashes (key_id, field, value) VALUES (?1, ?2, ?3)
            ON CONFLICT(key_id, field) DO UPDATE SET value = excluded.value
        "#;
        let mut stmt = self.core.prepare(sql)?;
        stmt.bind_int64(1, key_id)?;
        stmt.bind_text(2, field)?;
        stmt.bind_blob(3, value)?;
        stmt.step()?;
        Ok(true)
    }

    /// HGET key field - Get the value of a hash field
    pub fn hget(&self, key: &str, field: &str) -> std::result::Result<Option<Vec<u8>>, JsError> {
        let key_id = match self.core.get_key_id_typed(key, KeyType::Hash)? {
            Some(id) => id,
            None => return Ok(None),
        };

        let sql = "SELECT value FROM hashes WHERE key_id = ?1 AND field = ?2";
        let mut stmt = self.core.prepare(sql)?;
        stmt.bind_int64(1, key_id)?;
        stmt.bind_text(2, field)?;

        if stmt.step()? {
            Ok(Some(stmt.column_blob(0)))
        } else {
            Ok(None)
        }
    }

    /// HDEL key field [field ...] - Delete one or more hash fields
    pub fn hdel(&mut self, key: &str, fields: Vec<String>) -> std::result::Result<i64, JsError> {
        let key_id = match self.core.get_key_id_typed(key, KeyType::Hash)? {
            Some(id) => id,
            None => return Ok(0),
        };

        let mut deleted = 0i64;
        for field in fields {
            let sql = "DELETE FROM hashes WHERE key_id = ?1 AND field = ?2";
            let mut stmt = self.core.prepare(sql)?;
            stmt.bind_int64(1, key_id)?;
            stmt.bind_text(2, &field)?;
            stmt.step()?;
            if unsafe { sqlite_wasm_rs::sqlite3_changes(std::ptr::null_mut()) } > 0 {
                deleted += 1;
            }
        }
        Ok(deleted)
    }

    /// HEXISTS key field - Check if a hash field exists
    pub fn hexists(&self, key: &str, field: &str) -> std::result::Result<bool, JsError> {
        Ok(self.hget(key, field)?.is_some())
    }

    /// HGETALL key - Get all fields and values in a hash
    pub fn hgetall(&self, key: &str) -> std::result::Result<Vec<JsValue>, JsError> {
        let key_id = match self.core.get_key_id_typed(key, KeyType::Hash)? {
            Some(id) => id,
            None => return Ok(Vec::new()),
        };

        let sql = "SELECT field, value FROM hashes WHERE key_id = ?1";
        let mut stmt = self.core.prepare(sql)?;
        stmt.bind_int64(1, key_id)?;

        let mut result = Vec::new();
        while stmt.step()? {
            let field = stmt.column_text(0);
            let value = stmt.column_blob(1);
            result.push(JsValue::from_str(&field));
            result.push(
                serde_wasm_bindgen::to_value(&value)
                    .map_err(|e| WasmError::Other(e.to_string()))?,
            );
        }
        Ok(result)
    }

    /// HKEYS key - Get all field names in a hash
    pub fn hkeys(&self, key: &str) -> std::result::Result<Vec<String>, JsError> {
        let key_id = match self.core.get_key_id_typed(key, KeyType::Hash)? {
            Some(id) => id,
            None => return Ok(Vec::new()),
        };

        let sql = "SELECT field FROM hashes WHERE key_id = ?1";
        let mut stmt = self.core.prepare(sql)?;
        stmt.bind_int64(1, key_id)?;

        let mut fields = Vec::new();
        while stmt.step()? {
            fields.push(stmt.column_text(0));
        }
        Ok(fields)
    }

    /// HVALS key - Get all values in a hash
    pub fn hvals(&self, key: &str) -> std::result::Result<Vec<Vec<u8>>, JsError> {
        let key_id = match self.core.get_key_id_typed(key, KeyType::Hash)? {
            Some(id) => id,
            None => return Ok(Vec::new()),
        };

        let sql = "SELECT value FROM hashes WHERE key_id = ?1";
        let mut stmt = self.core.prepare(sql)?;
        stmt.bind_int64(1, key_id)?;

        let mut values = Vec::new();
        while stmt.step()? {
            values.push(stmt.column_blob(0));
        }
        Ok(values)
    }

    /// HLEN key - Get the number of fields in a hash
    pub fn hlen(&self, key: &str) -> std::result::Result<i64, JsError> {
        let key_id = match self.core.get_key_id_typed(key, KeyType::Hash)? {
            Some(id) => id,
            None => return Ok(0),
        };

        let sql = "SELECT COUNT(*) FROM hashes WHERE key_id = ?1";
        let mut stmt = self.core.prepare(sql)?;
        stmt.bind_int64(1, key_id)?;
        stmt.step()?;
        Ok(stmt.column_int64(0))
    }

    /// HINCRBY key field increment - Increment hash field integer value
    pub fn hincrby(
        &mut self,
        key: &str,
        field: &str,
        increment: i64,
    ) -> std::result::Result<i64, JsError> {
        let current = match self.hget(key, field)? {
            Some(data) => {
                let s = String::from_utf8(data).map_err(|_| WasmError::NotInteger)?;
                s.parse::<i64>().map_err(|_| WasmError::NotInteger)?
            }
            None => 0,
        };

        let new_value = current + increment;
        self.hset(key, field, new_value.to_string().as_bytes())?;
        Ok(new_value)
    }

    /// HINCRBYFLOAT key field increment - Increment hash field float value
    pub fn hincrbyfloat(
        &mut self,
        key: &str,
        field: &str,
        increment: f64,
    ) -> std::result::Result<f64, JsError> {
        let current = match self.hget(key, field)? {
            Some(data) => {
                let s = String::from_utf8(data).map_err(|_| WasmError::NotFloat)?;
                s.parse::<f64>().map_err(|_| WasmError::NotFloat)?
            }
            None => 0.0,
        };

        let new_value = current + increment;
        self.hset(key, field, new_value.to_string().as_bytes())?;
        Ok(new_value)
    }

    /// HSETNX key field value - Set hash field only if it doesn't exist
    pub fn hsetnx(
        &mut self,
        key: &str,
        field: &str,
        value: &[u8],
    ) -> std::result::Result<bool, JsError> {
        if self.hexists(key, field)? {
            return Ok(false);
        }
        self.hset(key, field, value)?;
        Ok(true)
    }

    // ========================================================================
    // List Commands
    // ========================================================================

    /// LPUSH key element [element ...] - Prepend one or multiple elements to a list
    pub fn lpush(&mut self, key: &str, values: Vec<Vec<u8>>) -> std::result::Result<i64, JsError> {
        let key_id = self.core.upsert_key(key, KeyType::List, None)?;

        // Get minimum position
        let sql = "SELECT MIN(pos) FROM lists WHERE key_id = ?1";
        let mut stmt = self.core.prepare(sql)?;
        stmt.bind_int64(1, key_id)?;
        stmt.step()?;
        let min_pos = stmt.column_int64_opt(0).unwrap_or(0);
        drop(stmt);

        // Insert elements
        let insert_sql = "INSERT INTO lists (key_id, pos, value) VALUES (?1, ?2, ?3)";
        for (i, value) in values.iter().enumerate() {
            let pos = min_pos - (i as i64) - 1;
            let mut stmt = self.core.prepare(insert_sql)?;
            stmt.bind_int64(1, key_id)?;
            stmt.bind_int64(2, pos)?;
            stmt.bind_blob(3, value)?;
            stmt.step()?;
        }

        self.llen(key)
    }

    /// RPUSH key element [element ...] - Append one or multiple elements to a list
    pub fn rpush(&mut self, key: &str, values: Vec<Vec<u8>>) -> std::result::Result<i64, JsError> {
        let key_id = self.core.upsert_key(key, KeyType::List, None)?;

        // Get maximum position
        let sql = "SELECT MAX(pos) FROM lists WHERE key_id = ?1";
        let mut stmt = self.core.prepare(sql)?;
        stmt.bind_int64(1, key_id)?;
        stmt.step()?;
        let max_pos = stmt.column_int64_opt(0).unwrap_or(-1);
        drop(stmt);

        // Insert elements
        let insert_sql = "INSERT INTO lists (key_id, pos, value) VALUES (?1, ?2, ?3)";
        for (i, value) in values.iter().enumerate() {
            let pos = max_pos + (i as i64) + 1;
            let mut stmt = self.core.prepare(insert_sql)?;
            stmt.bind_int64(1, key_id)?;
            stmt.bind_int64(2, pos)?;
            stmt.bind_blob(3, value)?;
            stmt.step()?;
        }

        self.llen(key)
    }

    /// LPOP key [count] - Remove and get the first element(s) in a list
    pub fn lpop(&mut self, key: &str, count: Option<i64>) -> std::result::Result<Vec<Vec<u8>>, JsError> {
        let key_id = match self.core.get_key_id_typed(key, KeyType::List)? {
            Some(id) => id,
            None => return Ok(Vec::new()),
        };

        let limit = count.unwrap_or(1);
        let sql = r#"
            DELETE FROM lists WHERE key_id = ?1 AND pos IN (
                SELECT pos FROM lists WHERE key_id = ?1 ORDER BY pos ASC LIMIT ?2
            )
            RETURNING value
        "#;
        let mut stmt = self.core.prepare(sql)?;
        stmt.bind_int64(1, key_id)?;
        stmt.bind_int64(2, limit)?;

        let mut values = Vec::new();
        while stmt.step()? {
            values.push(stmt.column_blob(0));
        }
        Ok(values)
    }

    /// RPOP key [count] - Remove and get the last element(s) in a list
    pub fn rpop(&mut self, key: &str, count: Option<i64>) -> std::result::Result<Vec<Vec<u8>>, JsError> {
        let key_id = match self.core.get_key_id_typed(key, KeyType::List)? {
            Some(id) => id,
            None => return Ok(Vec::new()),
        };

        let limit = count.unwrap_or(1);
        let sql = r#"
            DELETE FROM lists WHERE key_id = ?1 AND pos IN (
                SELECT pos FROM lists WHERE key_id = ?1 ORDER BY pos DESC LIMIT ?2
            )
            RETURNING value
        "#;
        let mut stmt = self.core.prepare(sql)?;
        stmt.bind_int64(1, key_id)?;
        stmt.bind_int64(2, limit)?;

        let mut values = Vec::new();
        while stmt.step()? {
            values.push(stmt.column_blob(0));
        }
        Ok(values)
    }

    /// LLEN key - Get the length of a list
    pub fn llen(&self, key: &str) -> std::result::Result<i64, JsError> {
        let key_id = match self.core.get_key_id_typed(key, KeyType::List)? {
            Some(id) => id,
            None => return Ok(0),
        };

        let sql = "SELECT COUNT(*) FROM lists WHERE key_id = ?1";
        let mut stmt = self.core.prepare(sql)?;
        stmt.bind_int64(1, key_id)?;
        stmt.step()?;
        Ok(stmt.column_int64(0))
    }

    /// LRANGE key start stop - Get a range of elements from a list
    pub fn lrange(&self, key: &str, start: i64, stop: i64) -> std::result::Result<Vec<Vec<u8>>, JsError> {
        let key_id = match self.core.get_key_id_typed(key, KeyType::List)? {
            Some(id) => id,
            None => return Ok(Vec::new()),
        };

        let len = self.llen(key)?;
        if len == 0 {
            return Ok(Vec::new());
        }

        // Normalize negative indices
        let start = if start < 0 { (len + start).max(0) } else { start.min(len - 1) };
        let stop = if stop < 0 { (len + stop).max(0) } else { stop.min(len - 1) };

        if start > stop {
            return Ok(Vec::new());
        }

        let offset = start;
        let limit = stop - start + 1;

        let sql = "SELECT value FROM lists WHERE key_id = ?1 ORDER BY pos ASC LIMIT ?2 OFFSET ?3";
        let mut stmt = self.core.prepare(sql)?;
        stmt.bind_int64(1, key_id)?;
        stmt.bind_int64(2, limit)?;
        stmt.bind_int64(3, offset)?;

        let mut values = Vec::new();
        while stmt.step()? {
            values.push(stmt.column_blob(0));
        }
        Ok(values)
    }

    /// LINDEX key index - Get an element from a list by its index
    pub fn lindex(&self, key: &str, index: i64) -> std::result::Result<Option<Vec<u8>>, JsError> {
        let values = self.lrange(key, index, index)?;
        Ok(values.into_iter().next())
    }

    // ========================================================================
    // Set Commands
    // ========================================================================

    /// SADD key member [member ...] - Add one or more members to a set
    pub fn sadd(&mut self, key: &str, members: Vec<Vec<u8>>) -> std::result::Result<i64, JsError> {
        let key_id = self.core.upsert_key(key, KeyType::Set, None)?;

        let sql = "INSERT OR IGNORE INTO sets (key_id, member) VALUES (?1, ?2)";
        let mut added = 0i64;
        for member in members {
            let mut stmt = self.core.prepare(sql)?;
            stmt.bind_int64(1, key_id)?;
            stmt.bind_blob(2, &member)?;
            stmt.step()?;
            added += unsafe { sqlite_wasm_rs::sqlite3_changes(std::ptr::null_mut()) } as i64;
        }
        Ok(added)
    }

    /// SREM key member [member ...] - Remove one or more members from a set
    pub fn srem(&mut self, key: &str, members: Vec<Vec<u8>>) -> std::result::Result<i64, JsError> {
        let key_id = match self.core.get_key_id_typed(key, KeyType::Set)? {
            Some(id) => id,
            None => return Ok(0),
        };

        let sql = "DELETE FROM sets WHERE key_id = ?1 AND member = ?2";
        let mut removed = 0i64;
        for member in members {
            let mut stmt = self.core.prepare(sql)?;
            stmt.bind_int64(1, key_id)?;
            stmt.bind_blob(2, &member)?;
            stmt.step()?;
            removed += unsafe { sqlite_wasm_rs::sqlite3_changes(std::ptr::null_mut()) } as i64;
        }
        Ok(removed)
    }

    /// SMEMBERS key - Get all members in a set
    pub fn smembers(&self, key: &str) -> std::result::Result<Vec<Vec<u8>>, JsError> {
        let key_id = match self.core.get_key_id_typed(key, KeyType::Set)? {
            Some(id) => id,
            None => return Ok(Vec::new()),
        };

        let sql = "SELECT member FROM sets WHERE key_id = ?1";
        let mut stmt = self.core.prepare(sql)?;
        stmt.bind_int64(1, key_id)?;

        let mut members = Vec::new();
        while stmt.step()? {
            members.push(stmt.column_blob(0));
        }
        Ok(members)
    }

    /// SISMEMBER key member - Check if a member is in a set
    pub fn sismember(&self, key: &str, member: &[u8]) -> std::result::Result<bool, JsError> {
        let key_id = match self.core.get_key_id_typed(key, KeyType::Set)? {
            Some(id) => id,
            None => return Ok(false),
        };

        let sql = "SELECT 1 FROM sets WHERE key_id = ?1 AND member = ?2";
        let mut stmt = self.core.prepare(sql)?;
        stmt.bind_int64(1, key_id)?;
        stmt.bind_blob(2, member)?;
        Ok(stmt.step()?)
    }

    /// SCARD key - Get the number of members in a set
    pub fn scard(&self, key: &str) -> std::result::Result<i64, JsError> {
        let key_id = match self.core.get_key_id_typed(key, KeyType::Set)? {
            Some(id) => id,
            None => return Ok(0),
        };

        let sql = "SELECT COUNT(*) FROM sets WHERE key_id = ?1";
        let mut stmt = self.core.prepare(sql)?;
        stmt.bind_int64(1, key_id)?;
        stmt.step()?;
        Ok(stmt.column_int64(0))
    }

    // ========================================================================
    // Sorted Set Commands
    // ========================================================================

    /// ZADD key score member [score member ...] - Add one or more members to a sorted set
    pub fn zadd(
        &mut self,
        key: &str,
        members: Vec<JsValue>,
    ) -> std::result::Result<i64, JsError> {
        if members.len() % 2 != 0 {
            return Err(WasmError::InvalidArgument("ZADD requires score-member pairs".to_string()).into());
        }

        let key_id = self.core.upsert_key(key, KeyType::ZSet, None)?;

        let sql = r#"
            INSERT INTO zsets (key_id, member, score) VALUES (?1, ?2, ?3)
            ON CONFLICT(key_id, member) DO UPDATE SET score = excluded.score
        "#;

        let mut added = 0i64;
        for i in (0..members.len()).step_by(2) {
            let score = members[i]
                .as_f64()
                .ok_or_else(|| WasmError::InvalidArgument("score must be a number".to_string()))?;
            let member: Vec<u8> = serde_wasm_bindgen::from_value(members[i + 1].clone())
                .map_err(|e| WasmError::InvalidArgument(e.to_string()))?;

            let mut stmt = self.core.prepare(sql)?;
            stmt.bind_int64(1, key_id)?;
            stmt.bind_blob(2, &member)?;
            stmt.bind_double(3, score)?;
            stmt.step()?;
            added += 1;
        }
        Ok(added)
    }

    /// ZREM key member [member ...] - Remove one or more members from a sorted set
    pub fn zrem(&mut self, key: &str, members: Vec<Vec<u8>>) -> std::result::Result<i64, JsError> {
        let key_id = match self.core.get_key_id_typed(key, KeyType::ZSet)? {
            Some(id) => id,
            None => return Ok(0),
        };

        let sql = "DELETE FROM zsets WHERE key_id = ?1 AND member = ?2";
        let mut removed = 0i64;
        for member in members {
            let mut stmt = self.core.prepare(sql)?;
            stmt.bind_int64(1, key_id)?;
            stmt.bind_blob(2, &member)?;
            stmt.step()?;
            removed += unsafe { sqlite_wasm_rs::sqlite3_changes(std::ptr::null_mut()) } as i64;
        }
        Ok(removed)
    }

    /// ZSCORE key member - Get the score associated with the given member
    pub fn zscore(&self, key: &str, member: &[u8]) -> std::result::Result<Option<f64>, JsError> {
        let key_id = match self.core.get_key_id_typed(key, KeyType::ZSet)? {
            Some(id) => id,
            None => return Ok(None),
        };

        let sql = "SELECT score FROM zsets WHERE key_id = ?1 AND member = ?2";
        let mut stmt = self.core.prepare(sql)?;
        stmt.bind_int64(1, key_id)?;
        stmt.bind_blob(2, member)?;

        if stmt.step()? {
            Ok(Some(stmt.column_double(0)))
        } else {
            Ok(None)
        }
    }

    /// ZRANK key member - Determine the index of a member in a sorted set
    pub fn zrank(&self, key: &str, member: &[u8]) -> std::result::Result<Option<i64>, JsError> {
        let key_id = match self.core.get_key_id_typed(key, KeyType::ZSet)? {
            Some(id) => id,
            None => return Ok(None),
        };

        // Get the score of the member first
        let score = match self.zscore(key, member)? {
            Some(s) => s,
            None => return Ok(None),
        };

        // Count members with lower scores
        let sql = r#"
            SELECT COUNT(*) FROM zsets
            WHERE key_id = ?1 AND (score < ?2 OR (score = ?2 AND member < ?3))
        "#;
        let mut stmt = self.core.prepare(sql)?;
        stmt.bind_int64(1, key_id)?;
        stmt.bind_double(2, score)?;
        stmt.bind_blob(3, member)?;
        stmt.step()?;
        Ok(Some(stmt.column_int64(0)))
    }

    /// ZRANGE key start stop [WITHSCORES] - Return a range of members in a sorted set
    pub fn zrange(
        &self,
        key: &str,
        start: i64,
        stop: i64,
        withscores: bool,
    ) -> std::result::Result<Vec<JsValue>, JsError> {
        let key_id = match self.core.get_key_id_typed(key, KeyType::ZSet)? {
            Some(id) => id,
            None => return Ok(Vec::new()),
        };

        let len = self.zcard(key)?;
        if len == 0 {
            return Ok(Vec::new());
        }

        // Normalize negative indices
        let start = if start < 0 { (len + start).max(0) } else { start.min(len - 1) };
        let stop = if stop < 0 { (len + stop).max(0) } else { stop.min(len - 1) };

        if start > stop {
            return Ok(Vec::new());
        }

        let offset = start;
        let limit = stop - start + 1;

        let sql = "SELECT member, score FROM zsets WHERE key_id = ?1 ORDER BY score ASC, member ASC LIMIT ?2 OFFSET ?3";
        let mut stmt = self.core.prepare(sql)?;
        stmt.bind_int64(1, key_id)?;
        stmt.bind_int64(2, limit)?;
        stmt.bind_int64(3, offset)?;

        let mut result = Vec::new();
        while stmt.step()? {
            let member = stmt.column_blob(0);
            result.push(
                serde_wasm_bindgen::to_value(&member)
                    .map_err(|e| WasmError::Other(e.to_string()))?,
            );
            if withscores {
                result.push(JsValue::from_f64(stmt.column_double(1)));
            }
        }
        Ok(result)
    }

    /// ZCARD key - Get the number of members in a sorted set
    pub fn zcard(&self, key: &str) -> std::result::Result<i64, JsError> {
        let key_id = match self.core.get_key_id_typed(key, KeyType::ZSet)? {
            Some(id) => id,
            None => return Ok(0),
        };

        let sql = "SELECT COUNT(*) FROM zsets WHERE key_id = ?1";
        let mut stmt = self.core.prepare(sql)?;
        stmt.bind_int64(1, key_id)?;
        stmt.step()?;
        Ok(stmt.column_int64(0))
    }

    /// ZINCRBY key increment member - Increment the score of a member
    pub fn zincrby(
        &mut self,
        key: &str,
        increment: f64,
        member: &[u8],
    ) -> std::result::Result<f64, JsError> {
        let current = self.zscore(key, member)?.unwrap_or(0.0);
        let new_score = current + increment;

        let key_id = self.core.upsert_key(key, KeyType::ZSet, None)?;
        let sql = r#"
            INSERT INTO zsets (key_id, member, score) VALUES (?1, ?2, ?3)
            ON CONFLICT(key_id, member) DO UPDATE SET score = excluded.score
        "#;
        let mut stmt = self.core.prepare(sql)?;
        stmt.bind_int64(1, key_id)?;
        stmt.bind_blob(2, member)?;
        stmt.bind_double(3, new_score)?;
        stmt.step()?;

        Ok(new_score)
    }

    /// ZCOUNT key min max - Count members in a sorted set with scores within the given values
    pub fn zcount(&self, key: &str, min: f64, max: f64) -> std::result::Result<i64, JsError> {
        let key_id = match self.core.get_key_id_typed(key, KeyType::ZSet)? {
            Some(id) => id,
            None => return Ok(0),
        };

        let sql = "SELECT COUNT(*) FROM zsets WHERE key_id = ?1 AND score >= ?2 AND score <= ?3";
        let mut stmt = self.core.prepare(sql)?;
        stmt.bind_int64(1, key_id)?;
        stmt.bind_double(2, min)?;
        stmt.bind_double(3, max)?;
        stmt.step()?;
        Ok(stmt.column_int64(0))
    }

    // ========================================================================
    // KeyInfo Command
    // ========================================================================

    /// KEYINFO key - Get detailed information about a key
    pub fn keyinfo(&self, key: &str) -> std::result::Result<Option<KeyInfo>, JsError> {
        let sql = "SELECT type, expire_at, created_at, updated_at FROM keys WHERE db = ?1 AND key = ?2";
        let mut stmt = self.core.prepare(sql)?;
        stmt.bind_int(1, self.core.selected_db())?;
        stmt.bind_text(2, key)?;

        if stmt.step()? {
            let type_id = stmt.column_int(0);
            let expire_at = stmt.column_int64_opt(1);
            let created_at = stmt.column_int64(2);
            let updated_at = stmt.column_int64(3);

            // Calculate TTL
            let ttl = match expire_at {
                Some(exp) => {
                    let now = DbCore::now_ms();
                    if exp <= now {
                        -2 // Expired
                    } else {
                        (exp - now) / 1000 // Convert to seconds
                    }
                }
                None => -1, // No TTL
            };

            let key_type = KeyType::from_i32(type_id).unwrap_or(KeyType::String);

            Ok(Some(KeyInfo {
                key_type,
                ttl,
                created_at,
                updated_at,
            }))
        } else {
            Ok(None)
        }
    }

    // ========================================================================
    // JSON Commands (ReJSON-compatible)
    // ========================================================================

    /// JSON.SET key path value [NX|XX] - Set a JSON value at the specified path
    #[wasm_bindgen(js_name = "jsonSet")]
    pub fn json_set(
        &mut self,
        key: &str,
        path: &str,
        value: &str,
        options: Option<JsonSetOptions>,
    ) -> std::result::Result<bool, JsError> {
        Err(WasmError::NotImplemented("JSON.SET".to_string()).into())
    }

    /// JSON.GET key [path ...] - Get JSON values at the specified paths
    #[wasm_bindgen(js_name = "jsonGet")]
    pub fn json_get(&self, key: &str, paths: Vec<String>) -> std::result::Result<Option<String>, JsError> {
        Err(WasmError::NotImplemented("JSON.GET".to_string()).into())
    }

    /// JSON.DEL key [path] - Delete JSON values at the specified path
    #[wasm_bindgen(js_name = "jsonDel")]
    pub fn json_del(&mut self, key: &str, path: &str) -> std::result::Result<i64, JsError> {
        Err(WasmError::NotImplemented("JSON.DEL".to_string()).into())
    }

    /// JSON.TYPE key [path] - Get the type of JSON value at the specified path
    #[wasm_bindgen(js_name = "jsonType")]
    pub fn json_type(&self, key: &str, path: &str) -> std::result::Result<Option<String>, JsError> {
        Err(WasmError::NotImplemented("JSON.TYPE".to_string()).into())
    }

    /// JSON.NUMINCRBY key path increment - Increment a JSON number
    #[wasm_bindgen(js_name = "jsonNumIncrBy")]
    pub fn json_num_incr_by(
        &mut self,
        key: &str,
        path: &str,
        increment: f64,
    ) -> std::result::Result<Option<String>, JsError> {
        Err(WasmError::NotImplemented("JSON.NUMINCRBY".to_string()).into())
    }

    /// JSON.STRAPPEND key path value - Append to a JSON string
    #[wasm_bindgen(js_name = "jsonStrAppend")]
    pub fn json_str_append(
        &mut self,
        key: &str,
        path: &str,
        value: &str,
    ) -> std::result::Result<i64, JsError> {
        Err(WasmError::NotImplemented("JSON.STRAPPEND".to_string()).into())
    }

    /// JSON.STRLEN key [path] - Get the length of a JSON string
    #[wasm_bindgen(js_name = "jsonStrLen")]
    pub fn json_str_len(&self, key: &str, path: &str) -> std::result::Result<i64, JsError> {
        Err(WasmError::NotImplemented("JSON.STRLEN".to_string()).into())
    }

    /// JSON.ARRAPPEND key path value [value ...] - Append to a JSON array
    #[wasm_bindgen(js_name = "jsonArrAppend")]
    pub fn json_arr_append(
        &mut self,
        key: &str,
        path: &str,
        values: Vec<String>,
    ) -> std::result::Result<i64, JsError> {
        Err(WasmError::NotImplemented("JSON.ARRAPPEND".to_string()).into())
    }

    /// JSON.ARRLEN key [path] - Get the length of a JSON array
    #[wasm_bindgen(js_name = "jsonArrLen")]
    pub fn json_arr_len(&self, key: &str, path: &str) -> std::result::Result<i64, JsError> {
        Err(WasmError::NotImplemented("JSON.ARRLEN".to_string()).into())
    }

    /// JSON.ARRPOP key [path [index]] - Pop an element from a JSON array
    #[wasm_bindgen(js_name = "jsonArrPop")]
    pub fn json_arr_pop(
        &mut self,
        key: &str,
        path: &str,
        index: Option<i64>,
    ) -> std::result::Result<Option<String>, JsError> {
        Err(WasmError::NotImplemented("JSON.ARRPOP".to_string()).into())
    }

    /// JSON.CLEAR key [path] - Clear JSON arrays or objects
    #[wasm_bindgen(js_name = "jsonClear")]
    pub fn json_clear(&mut self, key: &str, path: &str) -> std::result::Result<i64, JsError> {
        Err(WasmError::NotImplemented("JSON.CLEAR".to_string()).into())
    }

    // ========================================================================
    // History Enable/Disable Commands
    // ========================================================================

    /// Enable history tracking globally
    #[wasm_bindgen(js_name = "historyEnableGlobal")]
    pub fn history_enable_global(
        &mut self,
        retention: HistoryRetention,
        retention_value: i64,
    ) -> std::result::Result<(), JsError> {
        Err(WasmError::NotImplemented("HISTORY.ENABLE GLOBAL".to_string()).into())
    }

    /// Enable history tracking for a specific database
    #[wasm_bindgen(js_name = "historyEnableDb")]
    pub fn history_enable_db(
        &mut self,
        db_num: i32,
        retention: HistoryRetention,
        retention_value: i64,
    ) -> std::result::Result<(), JsError> {
        Err(WasmError::NotImplemented("HISTORY.ENABLE DB".to_string()).into())
    }

    /// Enable history tracking for a specific key
    #[wasm_bindgen(js_name = "historyEnableKey")]
    pub fn history_enable_key(
        &mut self,
        key: &str,
        retention: HistoryRetention,
        retention_value: i64,
    ) -> std::result::Result<(), JsError> {
        Err(WasmError::NotImplemented("HISTORY.ENABLE KEY".to_string()).into())
    }

    /// Disable history tracking globally
    #[wasm_bindgen(js_name = "historyDisableGlobal")]
    pub fn history_disable_global(&mut self) -> std::result::Result<(), JsError> {
        Err(WasmError::NotImplemented("HISTORY.DISABLE GLOBAL".to_string()).into())
    }

    /// Disable history tracking for a specific database
    #[wasm_bindgen(js_name = "historyDisableDb")]
    pub fn history_disable_db(&mut self, db_num: i32) -> std::result::Result<(), JsError> {
        Err(WasmError::NotImplemented("HISTORY.DISABLE DB".to_string()).into())
    }

    /// Disable history tracking for a specific key
    #[wasm_bindgen(js_name = "historyDisableKey")]
    pub fn history_disable_key(&mut self, key: &str) -> std::result::Result<(), JsError> {
        Err(WasmError::NotImplemented("HISTORY.DISABLE KEY".to_string()).into())
    }

    /// Check if history tracking is enabled for a key
    #[wasm_bindgen(js_name = "historyIsEnabled")]
    pub fn history_is_enabled(&self, key: &str) -> std::result::Result<bool, JsError> {
        Err(WasmError::NotImplemented("HISTORY.ISENABLED".to_string()).into())
    }

    // ========================================================================
    // FTS Enable/Disable Commands
    // ========================================================================

    /// Enable FTS indexing globally
    #[wasm_bindgen(js_name = "ftsEnableGlobal")]
    pub fn fts_enable_global(&mut self) -> std::result::Result<(), JsError> {
        Err(WasmError::NotImplemented("FTS.ENABLE GLOBAL".to_string()).into())
    }

    /// Enable FTS indexing for a specific database
    #[wasm_bindgen(js_name = "ftsEnableDb")]
    pub fn fts_enable_db(&mut self, db_num: i32) -> std::result::Result<(), JsError> {
        Err(WasmError::NotImplemented("FTS.ENABLE DB".to_string()).into())
    }

    /// Enable FTS indexing for a key pattern
    #[wasm_bindgen(js_name = "ftsEnablePattern")]
    pub fn fts_enable_pattern(&mut self, pattern: &str) -> std::result::Result<(), JsError> {
        Err(WasmError::NotImplemented("FTS.ENABLE PATTERN".to_string()).into())
    }

    /// Enable FTS indexing for a specific key
    #[wasm_bindgen(js_name = "ftsEnableKey")]
    pub fn fts_enable_key(&mut self, key: &str) -> std::result::Result<(), JsError> {
        Err(WasmError::NotImplemented("FTS.ENABLE KEY".to_string()).into())
    }

    /// Disable FTS indexing globally
    #[wasm_bindgen(js_name = "ftsDisableGlobal")]
    pub fn fts_disable_global(&mut self) -> std::result::Result<(), JsError> {
        Err(WasmError::NotImplemented("FTS.DISABLE GLOBAL".to_string()).into())
    }

    /// Disable FTS indexing for a specific database
    #[wasm_bindgen(js_name = "ftsDisableDb")]
    pub fn fts_disable_db(&mut self, db_num: i32) -> std::result::Result<(), JsError> {
        Err(WasmError::NotImplemented("FTS.DISABLE DB".to_string()).into())
    }

    /// Disable FTS indexing for a key pattern
    #[wasm_bindgen(js_name = "ftsDisablePattern")]
    pub fn fts_disable_pattern(&mut self, pattern: &str) -> std::result::Result<(), JsError> {
        Err(WasmError::NotImplemented("FTS.DISABLE PATTERN".to_string()).into())
    }

    /// Disable FTS indexing for a specific key
    #[wasm_bindgen(js_name = "ftsDisableKey")]
    pub fn fts_disable_key(&mut self, key: &str) -> std::result::Result<(), JsError> {
        Err(WasmError::NotImplemented("FTS.DISABLE KEY".to_string()).into())
    }

    /// Check if FTS indexing is enabled for a key
    #[wasm_bindgen(js_name = "ftsIsEnabled")]
    pub fn fts_is_enabled(&self, key: &str) -> std::result::Result<bool, JsError> {
        Err(WasmError::NotImplemented("FTS.ISENABLED".to_string()).into())
    }
}
