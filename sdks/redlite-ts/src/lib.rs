#![deny(clippy::all)]

use napi::bindgen_prelude::*;
use napi_derive::napi;
use redlite::Db as RedliteDb;
use std::time::Duration;

/// Redlite database - Redis API with SQLite durability
#[napi(js_name = "RedliteDb")]
pub struct JsDb {
    inner: RedliteDb,
}

/// Set options for SET command
#[napi(object)]
pub struct SetOptions {
    /// Expiration in seconds
    pub ex: Option<i64>,
    /// Expiration in milliseconds
    pub px: Option<i64>,
    /// Only set if key does not exist
    pub nx: Option<bool>,
    /// Only set if key exists
    pub xx: Option<bool>,
}

/// Sorted set member with score
#[napi(object)]
pub struct ZMember {
    pub score: f64,
    pub member: Buffer,
}

#[napi]
impl JsDb {
    /// Open a database at the given path
    /// Use ":memory:" for an in-memory database
    #[napi(constructor)]
    pub fn new(path: String) -> Result<Self> {
        let inner = RedliteDb::open(&path)
            .map_err(|e| Error::from_reason(format!("Failed to open database: {}", e)))?;
        Ok(Self { inner })
    }

    /// Open an in-memory database
    #[napi(factory)]
    pub fn open_memory() -> Result<Self> {
        let inner = RedliteDb::open_memory()
            .map_err(|e| Error::from_reason(format!("Failed to open memory database: {}", e)))?;
        Ok(Self { inner })
    }

    /// Open a database with custom cache size in MB
    #[napi(factory)]
    pub fn open_with_cache(path: String, cache_mb: i64) -> Result<Self> {
        let inner = RedliteDb::open_with_cache(&path, cache_mb)
            .map_err(|e| Error::from_reason(format!("Failed to open database: {}", e)))?;
        Ok(Self { inner })
    }

    // =========================================================================
    // String Commands
    // =========================================================================

    /// Get the value of a key
    #[napi]
    pub fn get(&self, key: String) -> Result<Option<Buffer>> {
        self.inner
            .get(&key)
            .map(|opt| opt.map(|v| Buffer::from(v)))
            .map_err(|e| Error::from_reason(e.to_string()))
    }

    /// Set a key-value pair with optional expiration
    #[napi]
    pub fn set(&self, key: String, value: Buffer, options: Option<SetOptions>) -> Result<bool> {
        match options {
            Some(o) => {
                let ttl = if let Some(ex) = o.ex {
                    Some(Duration::from_secs(ex as u64))
                } else if let Some(px) = o.px {
                    Some(Duration::from_millis(px as u64))
                } else {
                    None
                };
                let opts = redlite::SetOptions {
                    ttl,
                    nx: o.nx.unwrap_or(false),
                    xx: o.xx.unwrap_or(false),
                };
                self.inner
                    .set_opts(&key, value.as_ref(), opts)
                    .map_err(|e| Error::from_reason(e.to_string()))
            }
            None => {
                self.inner
                    .set(&key, value.as_ref(), None)
                    .map(|_| true)
                    .map_err(|e| Error::from_reason(e.to_string()))
            }
        }
    }

    /// Set key with expiration in seconds
    #[napi]
    pub fn setex(&self, key: String, seconds: i64, value: Buffer) -> Result<bool> {
        self.inner
            .setex(&key, seconds, value.as_ref())
            .map(|_| true)
            .map_err(|e| Error::from_reason(e.to_string()))
    }

    /// Set key with expiration in milliseconds
    #[napi]
    pub fn psetex(&self, key: String, milliseconds: i64, value: Buffer) -> Result<bool> {
        self.inner
            .psetex(&key, milliseconds, value.as_ref())
            .map(|_| true)
            .map_err(|e| Error::from_reason(e.to_string()))
    }

    /// Get and delete a key
    #[napi]
    pub fn getdel(&self, key: String) -> Result<Option<Buffer>> {
        self.inner
            .getdel(&key)
            .map(|opt| opt.map(|v| Buffer::from(v)))
            .map_err(|e| Error::from_reason(e.to_string()))
    }

    /// Append value to a key
    #[napi]
    pub fn append(&self, key: String, value: Buffer) -> Result<i64> {
        self.inner
            .append(&key, value.as_ref())
            .map(|n| n as i64)
            .map_err(|e| Error::from_reason(e.to_string()))
    }

    /// Get string length of a key
    #[napi]
    pub fn strlen(&self, key: String) -> Result<i64> {
        self.inner
            .strlen(&key)
            .map(|n| n as i64)
            .map_err(|e| Error::from_reason(e.to_string()))
    }

    /// Get substring of a string value
    #[napi]
    pub fn getrange(&self, key: String, start: i64, end: i64) -> Result<Buffer> {
        self.inner
            .getrange(&key, start, end)
            .map(Buffer::from)
            .map_err(|e| Error::from_reason(e.to_string()))
    }

    /// Overwrite part of a string at key starting at offset
    #[napi]
    pub fn setrange(&self, key: String, offset: i64, value: Buffer) -> Result<i64> {
        self.inner
            .setrange(&key, offset, value.as_ref())
            .map_err(|e| Error::from_reason(e.to_string()))
    }

    /// Increment the integer value of a key by one
    #[napi]
    pub fn incr(&self, key: String) -> Result<i64> {
        self.inner
            .incr(&key)
            .map_err(|e| Error::from_reason(e.to_string()))
    }

    /// Decrement the integer value of a key by one
    #[napi]
    pub fn decr(&self, key: String) -> Result<i64> {
        self.inner
            .decr(&key)
            .map_err(|e| Error::from_reason(e.to_string()))
    }

    /// Increment the integer value of a key by amount
    #[napi]
    pub fn incrby(&self, key: String, increment: i64) -> Result<i64> {
        self.inner
            .incrby(&key, increment)
            .map_err(|e| Error::from_reason(e.to_string()))
    }

    /// Decrement the integer value of a key by amount
    #[napi]
    pub fn decrby(&self, key: String, decrement: i64) -> Result<i64> {
        self.inner
            .decrby(&key, decrement)
            .map_err(|e| Error::from_reason(e.to_string()))
    }

    /// Increment the float value of a key by amount
    #[napi]
    pub fn incrbyfloat(&self, key: String, increment: f64) -> Result<f64> {
        self.inner
            .incrbyfloat(&key, increment)
            .and_then(|s| s.parse::<f64>().map_err(|_| redlite::KvError::NotFloat))
            .map_err(|e| Error::from_reason(e.to_string()))
    }

    // =========================================================================
    // Key Commands
    // =========================================================================

    /// Delete one or more keys
    #[napi]
    pub fn del(&self, keys: Vec<String>) -> Result<i64> {
        let key_refs: Vec<&str> = keys.iter().map(|s| s.as_str()).collect();
        self.inner
            .del(&key_refs)
            .map(|n| n as i64)
            .map_err(|e| Error::from_reason(e.to_string()))
    }

    /// Check if keys exist
    #[napi]
    pub fn exists(&self, keys: Vec<String>) -> Result<i64> {
        let key_refs: Vec<&str> = keys.iter().map(|s| s.as_str()).collect();
        self.inner
            .exists(&key_refs)
            .map(|n| n as i64)
            .map_err(|e| Error::from_reason(e.to_string()))
    }

    /// Get the type of a key
    #[napi(js_name = "type")]
    pub fn key_type(&self, key: String) -> Result<String> {
        self.inner
            .key_type(&key)
            .map(|opt| match opt {
                Some(redlite::KeyType::String) => "string".to_string(),
                Some(redlite::KeyType::List) => "list".to_string(),
                Some(redlite::KeyType::Set) => "set".to_string(),
                Some(redlite::KeyType::Hash) => "hash".to_string(),
                Some(redlite::KeyType::ZSet) => "zset".to_string(),
                Some(redlite::KeyType::Stream) => "stream".to_string(),
                None => "none".to_string(),
            })
            .map_err(|e| Error::from_reason(e.to_string()))
    }

    /// Get TTL in seconds
    #[napi]
    pub fn ttl(&self, key: String) -> Result<i64> {
        self.inner
            .ttl(&key)
            .map_err(|e| Error::from_reason(e.to_string()))
    }

    /// Get TTL in milliseconds
    #[napi]
    pub fn pttl(&self, key: String) -> Result<i64> {
        self.inner
            .pttl(&key)
            .map_err(|e| Error::from_reason(e.to_string()))
    }

    /// Set expiration in seconds
    #[napi]
    pub fn expire(&self, key: String, seconds: i64) -> Result<bool> {
        self.inner
            .expire(&key, seconds)
            .map_err(|e| Error::from_reason(e.to_string()))
    }

    /// Set expiration in milliseconds
    #[napi]
    pub fn pexpire(&self, key: String, milliseconds: i64) -> Result<bool> {
        self.inner
            .pexpire(&key, milliseconds)
            .map_err(|e| Error::from_reason(e.to_string()))
    }

    /// Set expiration at Unix timestamp (seconds)
    #[napi]
    pub fn expireat(&self, key: String, unix_time: i64) -> Result<bool> {
        self.inner
            .expireat(&key, unix_time)
            .map_err(|e| Error::from_reason(e.to_string()))
    }

    /// Set expiration at Unix timestamp (milliseconds)
    #[napi]
    pub fn pexpireat(&self, key: String, unix_time_ms: i64) -> Result<bool> {
        self.inner
            .pexpireat(&key, unix_time_ms)
            .map_err(|e| Error::from_reason(e.to_string()))
    }

    /// Remove expiration from a key
    #[napi]
    pub fn persist(&self, key: String) -> Result<bool> {
        self.inner
            .persist(&key)
            .map_err(|e| Error::from_reason(e.to_string()))
    }

    /// Rename a key
    #[napi]
    pub fn rename(&self, key: String, newkey: String) -> Result<bool> {
        self.inner
            .rename(&key, &newkey)
            .map(|_| true)
            .map_err(|e| Error::from_reason(e.to_string()))
    }

    /// Rename a key only if newkey does not exist
    #[napi]
    pub fn renamenx(&self, key: String, newkey: String) -> Result<bool> {
        self.inner
            .renamenx(&key, &newkey)
            .map_err(|e| Error::from_reason(e.to_string()))
    }

    /// Find all keys matching a pattern
    #[napi]
    pub fn keys(&self, pattern: Option<String>) -> Result<Vec<String>> {
        let pat = pattern.as_deref().unwrap_or("*");
        self.inner
            .keys(pat)
            .map_err(|e| Error::from_reason(e.to_string()))
    }

    /// Get the number of keys in the database
    #[napi]
    pub fn dbsize(&self) -> Result<i64> {
        self.inner
            .dbsize()
            .map(|n| n as i64)
            .map_err(|e| Error::from_reason(e.to_string()))
    }

    /// Delete all keys in the current database
    #[napi]
    pub fn flushdb(&self) -> Result<bool> {
        self.inner
            .flushdb()
            .map(|_| true)
            .map_err(|e| Error::from_reason(e.to_string()))
    }

    /// Select a database by index
    #[napi]
    pub fn select(&mut self, db: i32) -> Result<bool> {
        self.inner
            .select(db)
            .map(|_| true)
            .map_err(|e| Error::from_reason(e.to_string()))
    }

    // =========================================================================
    // Hash Commands
    // =========================================================================

    /// Set a single hash field
    #[napi]
    pub fn hset(&self, key: String, field: String, value: Buffer) -> Result<i64> {
        let pairs: Vec<(&str, &[u8])> = vec![(&field, value.as_ref())];
        self.inner
            .hset(&key, &pairs)
            .map_err(|e| Error::from_reason(e.to_string()))
    }

    /// Get a hash field value
    #[napi]
    pub fn hget(&self, key: String, field: String) -> Result<Option<Buffer>> {
        self.inner
            .hget(&key, &field)
            .map(|opt| opt.map(|v| Buffer::from(v)))
            .map_err(|e| Error::from_reason(e.to_string()))
    }

    /// Delete hash fields
    #[napi]
    pub fn hdel(&self, key: String, fields: Vec<String>) -> Result<i64> {
        let field_refs: Vec<&str> = fields.iter().map(|s| s.as_str()).collect();
        self.inner
            .hdel(&key, &field_refs)
            .map(|n| n as i64)
            .map_err(|e| Error::from_reason(e.to_string()))
    }

    /// Check if hash field exists
    #[napi]
    pub fn hexists(&self, key: String, field: String) -> Result<bool> {
        self.inner
            .hexists(&key, &field)
            .map_err(|e| Error::from_reason(e.to_string()))
    }

    /// Get number of fields in a hash
    #[napi]
    pub fn hlen(&self, key: String) -> Result<i64> {
        self.inner
            .hlen(&key)
            .map(|n| n as i64)
            .map_err(|e| Error::from_reason(e.to_string()))
    }

    /// Get all field names in a hash
    #[napi]
    pub fn hkeys(&self, key: String) -> Result<Vec<String>> {
        self.inner
            .hkeys(&key)
            .map_err(|e| Error::from_reason(e.to_string()))
    }

    /// Get all values in a hash
    #[napi]
    pub fn hvals(&self, key: String) -> Result<Vec<Buffer>> {
        self.inner
            .hvals(&key)
            .map(|v| v.into_iter().map(Buffer::from).collect())
            .map_err(|e| Error::from_reason(e.to_string()))
    }

    /// Increment hash field by integer
    #[napi]
    pub fn hincrby(&self, key: String, field: String, increment: i64) -> Result<i64> {
        self.inner
            .hincrby(&key, &field, increment)
            .map_err(|e| Error::from_reason(e.to_string()))
    }

    // =========================================================================
    // List Commands
    // =========================================================================

    /// Push values to the left of a list
    #[napi]
    pub fn lpush(&self, key: String, values: Vec<Buffer>) -> Result<i64> {
        let value_refs: Vec<&[u8]> = values.iter().map(|b| b.as_ref()).collect();
        self.inner
            .lpush(&key, &value_refs)
            .map(|n| n as i64)
            .map_err(|e| Error::from_reason(e.to_string()))
    }

    /// Push values to the right of a list
    #[napi]
    pub fn rpush(&self, key: String, values: Vec<Buffer>) -> Result<i64> {
        let value_refs: Vec<&[u8]> = values.iter().map(|b| b.as_ref()).collect();
        self.inner
            .rpush(&key, &value_refs)
            .map(|n| n as i64)
            .map_err(|e| Error::from_reason(e.to_string()))
    }

    /// Pop values from the left of a list
    #[napi]
    pub fn lpop(&self, key: String, count: Option<i64>) -> Result<Vec<Buffer>> {
        let cnt = count.map(|c| c as usize);
        self.inner
            .lpop(&key, cnt)
            .map(|v| v.into_iter().map(Buffer::from).collect())
            .map_err(|e| Error::from_reason(e.to_string()))
    }

    /// Pop values from the right of a list
    #[napi]
    pub fn rpop(&self, key: String, count: Option<i64>) -> Result<Vec<Buffer>> {
        let cnt = count.map(|c| c as usize);
        self.inner
            .rpop(&key, cnt)
            .map(|v| v.into_iter().map(Buffer::from).collect())
            .map_err(|e| Error::from_reason(e.to_string()))
    }

    /// Get list length
    #[napi]
    pub fn llen(&self, key: String) -> Result<i64> {
        self.inner
            .llen(&key)
            .map(|n| n as i64)
            .map_err(|e| Error::from_reason(e.to_string()))
    }

    /// Get a range of elements from a list
    #[napi]
    pub fn lrange(&self, key: String, start: i64, stop: i64) -> Result<Vec<Buffer>> {
        self.inner
            .lrange(&key, start, stop)
            .map(|v| v.into_iter().map(Buffer::from).collect())
            .map_err(|e| Error::from_reason(e.to_string()))
    }

    /// Get element at index in a list
    #[napi]
    pub fn lindex(&self, key: String, index: i64) -> Result<Option<Buffer>> {
        self.inner
            .lindex(&key, index)
            .map(|opt| opt.map(|v| Buffer::from(v)))
            .map_err(|e| Error::from_reason(e.to_string()))
    }

    // =========================================================================
    // Set Commands
    // =========================================================================

    /// Add members to a set
    #[napi]
    pub fn sadd(&self, key: String, members: Vec<Buffer>) -> Result<i64> {
        let member_refs: Vec<&[u8]> = members.iter().map(|b| b.as_ref()).collect();
        self.inner
            .sadd(&key, &member_refs)
            .map(|n| n as i64)
            .map_err(|e| Error::from_reason(e.to_string()))
    }

    /// Remove members from a set
    #[napi]
    pub fn srem(&self, key: String, members: Vec<Buffer>) -> Result<i64> {
        let member_refs: Vec<&[u8]> = members.iter().map(|b| b.as_ref()).collect();
        self.inner
            .srem(&key, &member_refs)
            .map(|n| n as i64)
            .map_err(|e| Error::from_reason(e.to_string()))
    }

    /// Get all members of a set
    #[napi]
    pub fn smembers(&self, key: String) -> Result<Vec<Buffer>> {
        self.inner
            .smembers(&key)
            .map(|v| v.into_iter().map(Buffer::from).collect())
            .map_err(|e| Error::from_reason(e.to_string()))
    }

    /// Check if member is in a set
    #[napi]
    pub fn sismember(&self, key: String, member: Buffer) -> Result<bool> {
        self.inner
            .sismember(&key, member.as_ref())
            .map_err(|e| Error::from_reason(e.to_string()))
    }

    /// Get number of members in a set
    #[napi]
    pub fn scard(&self, key: String) -> Result<i64> {
        self.inner
            .scard(&key)
            .map(|n| n as i64)
            .map_err(|e| Error::from_reason(e.to_string()))
    }

    // =========================================================================
    // Sorted Set Commands
    // =========================================================================

    /// Add members with scores to a sorted set
    #[napi]
    pub fn zadd(&self, key: String, members: Vec<ZMember>) -> Result<i64> {
        let zmembers: Vec<redlite::ZMember> = members
            .into_iter()
            .map(|m| redlite::ZMember {
                score: m.score,
                member: m.member.to_vec(),
            })
            .collect();
        self.inner
            .zadd(&key, &zmembers)
            .map(|n| n as i64)
            .map_err(|e| Error::from_reason(e.to_string()))
    }

    /// Remove members from a sorted set
    #[napi]
    pub fn zrem(&self, key: String, members: Vec<Buffer>) -> Result<i64> {
        let member_refs: Vec<&[u8]> = members.iter().map(|b| b.as_ref()).collect();
        self.inner
            .zrem(&key, &member_refs)
            .map(|n| n as i64)
            .map_err(|e| Error::from_reason(e.to_string()))
    }

    /// Get score of a member in a sorted set
    #[napi]
    pub fn zscore(&self, key: String, member: Buffer) -> Result<Option<f64>> {
        self.inner
            .zscore(&key, member.as_ref())
            .map_err(|e| Error::from_reason(e.to_string()))
    }

    /// Get number of members in a sorted set
    #[napi]
    pub fn zcard(&self, key: String) -> Result<i64> {
        self.inner
            .zcard(&key)
            .map(|n| n as i64)
            .map_err(|e| Error::from_reason(e.to_string()))
    }

    /// Count members in a sorted set within a score range
    #[napi]
    pub fn zcount(&self, key: String, min: f64, max: f64) -> Result<i64> {
        self.inner
            .zcount(&key, min, max)
            .map(|n| n as i64)
            .map_err(|e| Error::from_reason(e.to_string()))
    }

    /// Increment score of a member in a sorted set
    #[napi]
    pub fn zincrby(&self, key: String, increment: f64, member: Buffer) -> Result<f64> {
        self.inner
            .zincrby(&key, increment, member.as_ref())
            .map_err(|e| Error::from_reason(e.to_string()))
    }

    // =========================================================================
    // Server Commands
    // =========================================================================

    /// Run SQLite VACUUM to reclaim space
    #[napi]
    pub fn vacuum(&self) -> Result<i64> {
        self.inner
            .vacuum()
            .map(|n| n as i64)
            .map_err(|e| Error::from_reason(e.to_string()))
    }
}
