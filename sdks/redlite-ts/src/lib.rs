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

/// Stream ID (ms-seq format)
#[napi(object)]
pub struct StreamId {
    pub ms: i64,
    pub seq: i64,
}

impl From<redlite::StreamId> for StreamId {
    fn from(id: redlite::StreamId) -> Self {
        Self { ms: id.ms, seq: id.seq }
    }
}

impl From<&StreamId> for redlite::StreamId {
    fn from(id: &StreamId) -> Self {
        redlite::StreamId::new(id.ms, id.seq)
    }
}

/// KeyInfo result
#[napi(object)]
pub struct KeyInfoJs {
    pub key_type: String,
    pub ttl: i64,
    pub created_at: i64,
    pub updated_at: i64,
}

/// History entry for JavaScript
#[napi(object)]
pub struct HistoryEntryJs {
    pub timestamp_ms: i64,
    pub data_snapshot: Option<Buffer>,
    pub key: String,
    pub key_type: String,
    pub operation: String,
}

/// History statistics for JavaScript
#[napi(object)]
pub struct HistoryStatsJs {
    pub total_entries: i64,
    pub oldest_timestamp: Option<i64>,
    pub newest_timestamp: Option<i64>,
    pub storage_bytes: i64,
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

    /// Get values of multiple keys
    #[napi]
    pub fn mget(&self, keys: Vec<String>) -> Result<Vec<Option<Buffer>>> {
        let key_refs: Vec<&str> = keys.iter().map(|s| s.as_str()).collect();
        let results = self.inner.mget(&key_refs);
        Ok(results.into_iter().map(|opt| opt.map(Buffer::from)).collect())
    }

    /// Set multiple key-value pairs atomically
    #[napi]
    pub fn mset(&self, pairs: Vec<Vec<Buffer>>) -> Result<bool> {
        // pairs is [[key, value], [key, value], ...]
        let string_pairs: Vec<(String, Vec<u8>)> = pairs
            .into_iter()
            .filter_map(|pair| {
                if pair.len() == 2 {
                    let key = String::from_utf8_lossy(pair[0].as_ref()).to_string();
                    let value = pair[1].to_vec();
                    Some((key, value))
                } else {
                    None
                }
            })
            .collect();

        let pair_refs: Vec<(&str, &[u8])> = string_pairs
            .iter()
            .map(|(k, v)| (k.as_str(), v.as_slice()))
            .collect();

        self.inner
            .mset(&pair_refs)
            .map(|_| true)
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
                Some(redlite::KeyType::Json) => "ReJSON-RL".to_string(),
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

    /// Get all fields and values in a hash
    #[napi]
    pub fn hgetall(&self, key: String) -> Result<Vec<Vec<Buffer>>> {
        self.inner
            .hgetall(&key)
            .map(|pairs| {
                pairs
                    .into_iter()
                    .map(|(field, value)| vec![Buffer::from(field.into_bytes()), Buffer::from(value)])
                    .collect()
            })
            .map_err(|e| Error::from_reason(e.to_string()))
    }

    /// Get values of multiple hash fields
    #[napi]
    pub fn hmget(&self, key: String, fields: Vec<String>) -> Result<Vec<Option<Buffer>>> {
        let field_refs: Vec<&str> = fields.iter().map(|s| s.as_str()).collect();
        self.inner
            .hmget(&key, &field_refs)
            .map(|values| values.into_iter().map(|opt| opt.map(Buffer::from)).collect())
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

    /// Push values to the left of a list, only if key exists
    #[napi]
    pub fn lpushx(&self, key: String, values: Vec<Buffer>) -> Result<i64> {
        let value_refs: Vec<&[u8]> = values.iter().map(|b| b.as_ref()).collect();
        self.inner
            .lpushx(&key, &value_refs)
            .map(|n| n as i64)
            .map_err(|e| Error::from_reason(e.to_string()))
    }

    /// Push values to the right of a list, only if key exists
    #[napi]
    pub fn rpushx(&self, key: String, values: Vec<Buffer>) -> Result<i64> {
        let value_refs: Vec<&[u8]> = values.iter().map(|b| b.as_ref()).collect();
        self.inner
            .rpushx(&key, &value_refs)
            .map(|n| n as i64)
            .map_err(|e| Error::from_reason(e.to_string()))
    }

    /// Move element between lists atomically
    #[napi]
    pub fn lmove(
        &self,
        source: String,
        destination: String,
        wherefrom: String,
        whereto: String,
    ) -> Result<Option<Buffer>> {
        let from_dir = match wherefrom.to_uppercase().as_str() {
            "LEFT" => redlite::ListDirection::Left,
            "RIGHT" => redlite::ListDirection::Right,
            _ => return Err(Error::from_reason("wherefrom must be 'LEFT' or 'RIGHT'")),
        };
        let to_dir = match whereto.to_uppercase().as_str() {
            "LEFT" => redlite::ListDirection::Left,
            "RIGHT" => redlite::ListDirection::Right,
            _ => return Err(Error::from_reason("whereto must be 'LEFT' or 'RIGHT'")),
        };
        self.inner
            .lmove(&source, &destination, from_dir, to_dir)
            .map(|opt| opt.map(Buffer::from))
            .map_err(|e| Error::from_reason(e.to_string()))
    }

    /// Find positions of element in list
    #[napi]
    pub fn lpos(
        &self,
        key: String,
        element: Buffer,
        rank: Option<i64>,
        count: Option<i64>,
        maxlen: Option<i64>,
    ) -> Result<Vec<i64>> {
        self.inner
            .lpos(
                &key,
                element.as_ref(),
                rank,
                count.map(|c| c as usize),
                maxlen.map(|m| m as usize),
            )
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

    /// Get a range of members from a sorted set by index
    #[napi]
    pub fn zrange(&self, key: String, start: i64, stop: i64, with_scores: Option<bool>) -> Result<Vec<Buffer>> {
        let ws = with_scores.unwrap_or(false);
        self.inner
            .zrange(&key, start, stop, ws)
            .map(|members| {
                if ws {
                    // Return interleaved [member, score, member, score, ...]
                    members
                        .into_iter()
                        .flat_map(|m| {
                            vec![
                                Buffer::from(m.member),
                                Buffer::from(m.score.to_string().into_bytes()),
                            ]
                        })
                        .collect()
                } else {
                    members.into_iter().map(|m| Buffer::from(m.member)).collect()
                }
            })
            .map_err(|e| Error::from_reason(e.to_string()))
    }

    /// Get a range of members from a sorted set by index, in reverse order
    #[napi]
    pub fn zrevrange(&self, key: String, start: i64, stop: i64, with_scores: Option<bool>) -> Result<Vec<Buffer>> {
        let ws = with_scores.unwrap_or(false);
        self.inner
            .zrevrange(&key, start, stop, ws)
            .map(|members| {
                if ws {
                    // Return interleaved [member, score, member, score, ...]
                    members
                        .into_iter()
                        .flat_map(|m| {
                            vec![
                                Buffer::from(m.member),
                                Buffer::from(m.score.to_string().into_bytes()),
                            ]
                        })
                        .collect()
                } else {
                    members.into_iter().map(|m| Buffer::from(m.member)).collect()
                }
            })
            .map_err(|e| Error::from_reason(e.to_string()))
    }

    /// Intersect sorted sets and store result
    #[napi]
    pub fn zinterstore(
        &self,
        destination: String,
        keys: Vec<String>,
        weights: Option<Vec<f64>>,
        aggregate: Option<String>,
    ) -> Result<i64> {
        let key_refs: Vec<&str> = keys.iter().map(|s| s.as_str()).collect();
        let weights_ref = weights.as_ref().map(|w| w.as_slice());
        let aggregate_ref = aggregate.as_ref().map(|s| s.as_str());
        self.inner
            .zinterstore(&destination, &key_refs, weights_ref, aggregate_ref)
            .map(|n| n as i64)
            .map_err(|e| Error::from_reason(e.to_string()))
    }

    /// Union sorted sets and store result
    #[napi]
    pub fn zunionstore(
        &self,
        destination: String,
        keys: Vec<String>,
        weights: Option<Vec<f64>>,
        aggregate: Option<String>,
    ) -> Result<i64> {
        let key_refs: Vec<&str> = keys.iter().map(|s| s.as_str()).collect();
        let weights_ref = weights.as_ref().map(|w| w.as_slice());
        let aggregate_ref = aggregate.as_ref().map(|s| s.as_str());
        self.inner
            .zunionstore(&destination, &key_refs, weights_ref, aggregate_ref)
            .map(|n| n as i64)
            .map_err(|e| Error::from_reason(e.to_string()))
    }

    // =========================================================================
    // Stream Commands
    // =========================================================================

    /// Set consumer group last delivered ID
    #[napi]
    pub fn xgroup_setid(&self, key: String, group: String, id: StreamId) -> Result<bool> {
        self.inner
            .xgroup_setid(&key, &group, (&id).into())
            .map_err(|e| Error::from_reason(e.to_string()))
    }

    /// Create consumer in group
    #[napi]
    pub fn xgroup_createconsumer(&self, key: String, group: String, consumer: String) -> Result<bool> {
        self.inner
            .xgroup_createconsumer(&key, &group, &consumer)
            .map_err(|e| Error::from_reason(e.to_string()))
    }

    /// Delete consumer from group
    #[napi]
    pub fn xgroup_delconsumer(&self, key: String, group: String, consumer: String) -> Result<i64> {
        self.inner
            .xgroup_delconsumer(&key, &group, &consumer)
            .map(|n| n as i64)
            .map_err(|e| Error::from_reason(e.to_string()))
    }

    // Note: xclaim, xinfo_stream, xinfo_groups, xinfo_consumers require more complex types
    // These will be added in a future update when stream types are fully implemented

    // =========================================================================
    // History Commands
    // =========================================================================

    /// Query historical entries with filters
    #[napi(js_name = "historyGet")]
    pub fn history_get(
        &self,
        key: String,
        limit: Option<i64>,
        since: Option<i64>,
        until: Option<i64>,
    ) -> Result<Vec<HistoryEntryJs>> {
        self.inner
            .history_get(&key, limit, since, until)
            .map(|entries| {
                entries
                    .into_iter()
                    .map(|e| HistoryEntryJs {
                        timestamp_ms: e.timestamp_ms,
                        data_snapshot: e.data_snapshot.map(Buffer::from),
                        key: e.key,
                        key_type: format!("{:?}", e.key_type),
                        operation: e.operation,
                    })
                    .collect()
            })
            .map_err(|e| Error::from_reason(e.to_string()))
    }

    /// Time-travel query to specific timestamp
    #[napi(js_name = "historyGetAt")]
    pub fn history_get_at(&self, key: String, timestamp: i64) -> Result<Option<Buffer>> {
        self.inner
            .history_get_at(&key, timestamp)
            .map(|opt| opt.map(Buffer::from))
            .map_err(|e| Error::from_reason(e.to_string()))
    }

    /// List all tracked keys
    #[napi(js_name = "historyListKeys")]
    pub fn history_list_keys(&self, pattern: Option<String>) -> Result<Vec<String>> {
        self.inner
            .history_list_keys(pattern.as_deref())
            .map_err(|e| Error::from_reason(e.to_string()))
    }

    /// Get history tracking statistics
    #[napi(js_name = "historyStats")]
    pub fn history_stats(&self, key: Option<String>) -> Result<HistoryStatsJs> {
        self.inner
            .history_stats(key.as_deref())
            .map(|stats| HistoryStatsJs {
                total_entries: stats.total_entries,
                oldest_timestamp: stats.oldest_timestamp,
                newest_timestamp: stats.newest_timestamp,
                storage_bytes: stats.storage_bytes,
            })
            .map_err(|e| Error::from_reason(e.to_string()))
    }

    /// Clear history for a key
    #[napi(js_name = "historyClearKey")]
    pub fn history_clear_key(&self, key: String, before: Option<i64>) -> Result<i64> {
        self.inner
            .history_clear_key(&key, before)
            .map(|n| n as i64)
            .map_err(|e| Error::from_reason(e.to_string()))
    }

    /// Prune old history entries globally
    #[napi(js_name = "historyPrune")]
    pub fn history_prune(&self, before_timestamp: i64) -> Result<i64> {
        self.inner
            .history_prune(before_timestamp)
            .map(|n| n as i64)
            .map_err(|e| Error::from_reason(e.to_string()))
    }

    // =========================================================================
    // Geospatial Commands
    // =========================================================================

    // Note: Geospatial commands (geoadd, geopos, geodist, geohash, geosearch, geosearchstore)
    // are currently in the core with &str member types, which don't work well with Buffer.
    // These will be added in a future update when the core API is finalized for binary-safe members.

    // =========================================================================
    // JSON Commands
    // =========================================================================

    /// JSON.SET key path value [NX] [XX]
    /// Set a JSON value at path. Returns true on success.
    #[napi(js_name = "jsonSet")]
    pub fn json_set(&self, key: String, path: String, value: String, nx: Option<bool>, xx: Option<bool>) -> Result<bool> {
        self.inner
            .json_set(&key, &path, &value, nx.unwrap_or(false), xx.unwrap_or(false))
            .map_err(|e| Error::from_reason(e.to_string()))
    }

    /// JSON.GET key [path...]
    /// Get JSON value at path(s). Returns JSON string or null.
    #[napi(js_name = "jsonGet")]
    pub fn json_get(&self, key: String, paths: Option<Vec<String>>) -> Result<Option<String>> {
        let path_vec: Vec<&str> = match &paths {
            Some(p) => p.iter().map(|s| s.as_str()).collect(),
            None => vec!["$"],
        };
        self.inner
            .json_get(&key, &path_vec)
            .map_err(|e| Error::from_reason(e.to_string()))
    }

    /// JSON.DEL key [path]
    /// Delete JSON value at path. Returns number of values deleted.
    #[napi(js_name = "jsonDel")]
    pub fn json_del(&self, key: String, path: Option<String>) -> Result<i64> {
        self.inner
            .json_del(&key, path.as_deref())
            .map_err(|e| Error::from_reason(e.to_string()))
    }

    /// JSON.TYPE key [path]
    /// Get the type of JSON value at path.
    #[napi(js_name = "jsonType")]
    pub fn json_type(&self, key: String, path: Option<String>) -> Result<Option<String>> {
        self.inner
            .json_type(&key, path.as_deref())
            .map_err(|e| Error::from_reason(e.to_string()))
    }

    /// JSON.NUMINCRBY key path increment
    /// Increment numeric value at path. Returns new value as string.
    #[napi(js_name = "jsonNumIncrBy")]
    pub fn json_numincrby(&self, key: String, path: String, increment: f64) -> Result<String> {
        self.inner
            .json_numincrby(&key, &path, increment)
            .map_err(|e| Error::from_reason(e.to_string()))
    }

    /// JSON.STRAPPEND key [path] value
    /// Append string to JSON string at path. Returns new length.
    #[napi(js_name = "jsonStrAppend")]
    pub fn json_strappend(&self, key: String, value: String, path: Option<String>) -> Result<i64> {
        self.inner
            .json_strappend(&key, path.as_deref(), &value)
            .map_err(|e| Error::from_reason(e.to_string()))
    }

    /// JSON.STRLEN key [path]
    /// Get length of JSON string at path.
    #[napi(js_name = "jsonStrLen")]
    pub fn json_strlen(&self, key: String, path: Option<String>) -> Result<Option<i64>> {
        self.inner
            .json_strlen(&key, path.as_deref())
            .map_err(|e| Error::from_reason(e.to_string()))
    }

    /// JSON.ARRAPPEND key path value [value...]
    /// Append values to JSON array. Returns new array length.
    #[napi(js_name = "jsonArrAppend")]
    pub fn json_arrappend(&self, key: String, path: String, values: Vec<String>) -> Result<i64> {
        let value_refs: Vec<&str> = values.iter().map(|s| s.as_str()).collect();
        self.inner
            .json_arrappend(&key, &path, &value_refs)
            .map_err(|e| Error::from_reason(e.to_string()))
    }

    /// JSON.ARRLEN key [path]
    /// Get length of JSON array at path.
    #[napi(js_name = "jsonArrLen")]
    pub fn json_arrlen(&self, key: String, path: Option<String>) -> Result<Option<i64>> {
        self.inner
            .json_arrlen(&key, path.as_deref())
            .map_err(|e| Error::from_reason(e.to_string()))
    }

    /// JSON.ARRPOP key [path [index]]
    /// Pop element from JSON array. Returns the popped element.
    #[napi(js_name = "jsonArrPop")]
    pub fn json_arrpop(&self, key: String, path: Option<String>, index: Option<i64>) -> Result<Option<String>> {
        self.inner
            .json_arrpop(&key, path.as_deref(), index)
            .map_err(|e| Error::from_reason(e.to_string()))
    }

    /// JSON.CLEAR key [path]
    /// Clear container values (arrays/objects). Returns count of cleared values.
    #[napi(js_name = "jsonClear")]
    pub fn json_clear(&self, key: String, path: Option<String>) -> Result<i64> {
        self.inner
            .json_clear(&key, path.as_deref())
            .map_err(|e| Error::from_reason(e.to_string()))
    }

    // =========================================================================
    // History Enable/Disable Commands
    // =========================================================================

    /// Enable history tracking globally.
    /// retention: 0=Unlimited, positive=Time(ms), negative=Count(-n)
    #[napi(js_name = "historyEnableGlobal")]
    pub fn history_enable_global(&self, retention: Option<i64>) -> Result<()> {
        let ret = parse_retention_ts(retention);
        self.inner
            .history_enable_global(ret)
            .map_err(|e| Error::from_reason(e.to_string()))
    }

    /// Enable history tracking for a specific database.
    #[napi(js_name = "historyEnableDatabase")]
    pub fn history_enable_database(&self, db_num: i32, retention: Option<i64>) -> Result<()> {
        let ret = parse_retention_ts(retention);
        self.inner
            .history_enable_database(db_num, ret)
            .map_err(|e| Error::from_reason(e.to_string()))
    }

    /// Enable history tracking for a specific key.
    #[napi(js_name = "historyEnableKey")]
    pub fn history_enable_key(&self, key: String, retention: Option<i64>) -> Result<()> {
        let ret = parse_retention_ts(retention);
        self.inner
            .history_enable_key(&key, ret)
            .map_err(|e| Error::from_reason(e.to_string()))
    }

    /// Disable history tracking globally.
    #[napi(js_name = "historyDisableGlobal")]
    pub fn history_disable_global(&self) -> Result<()> {
        self.inner
            .history_disable_global()
            .map_err(|e| Error::from_reason(e.to_string()))
    }

    /// Disable history tracking for a specific database.
    #[napi(js_name = "historyDisableDatabase")]
    pub fn history_disable_database(&self, db_num: i32) -> Result<()> {
        self.inner
            .history_disable_database(db_num)
            .map_err(|e| Error::from_reason(e.to_string()))
    }

    /// Disable history tracking for a specific key.
    #[napi(js_name = "historyDisableKey")]
    pub fn history_disable_key(&self, key: String) -> Result<()> {
        self.inner
            .history_disable_key(&key)
            .map_err(|e| Error::from_reason(e.to_string()))
    }

    /// Check if history is enabled for a key.
    #[napi(js_name = "isHistoryEnabled")]
    pub fn is_history_enabled(&self, key: String) -> Result<bool> {
        self.inner
            .is_history_enabled(&key)
            .map_err(|e| Error::from_reason(e.to_string()))
    }

    // =========================================================================
    // FTS Enable/Disable Commands
    // =========================================================================

    /// Enable full-text search indexing globally.
    #[napi(js_name = "ftsEnableGlobal")]
    pub fn fts_enable_global(&self) -> Result<()> {
        self.inner
            .fts_enable_global()
            .map_err(|e| Error::from_reason(e.to_string()))
    }

    /// Enable full-text search indexing for a specific database.
    #[napi(js_name = "ftsEnableDatabase")]
    pub fn fts_enable_database(&self, db_num: i32) -> Result<()> {
        self.inner
            .fts_enable_database(db_num)
            .map_err(|e| Error::from_reason(e.to_string()))
    }

    /// Enable full-text search indexing for keys matching a pattern.
    #[napi(js_name = "ftsEnablePattern")]
    pub fn fts_enable_pattern(&self, pattern: String) -> Result<()> {
        self.inner
            .fts_enable_pattern(&pattern)
            .map_err(|e| Error::from_reason(e.to_string()))
    }

    /// Enable full-text search indexing for a specific key.
    #[napi(js_name = "ftsEnableKey")]
    pub fn fts_enable_key(&self, key: String) -> Result<()> {
        self.inner
            .fts_enable_key(&key)
            .map_err(|e| Error::from_reason(e.to_string()))
    }

    /// Disable full-text search indexing globally.
    #[napi(js_name = "ftsDisableGlobal")]
    pub fn fts_disable_global(&self) -> Result<()> {
        self.inner
            .fts_disable_global()
            .map_err(|e| Error::from_reason(e.to_string()))
    }

    /// Disable full-text search indexing for a specific database.
    #[napi(js_name = "ftsDisableDatabase")]
    pub fn fts_disable_database(&self, db_num: i32) -> Result<()> {
        self.inner
            .fts_disable_database(db_num)
            .map_err(|e| Error::from_reason(e.to_string()))
    }

    /// Disable full-text search indexing for keys matching a pattern.
    #[napi(js_name = "ftsDisablePattern")]
    pub fn fts_disable_pattern(&self, pattern: String) -> Result<()> {
        self.inner
            .fts_disable_pattern(&pattern)
            .map_err(|e| Error::from_reason(e.to_string()))
    }

    /// Disable full-text search indexing for a specific key.
    #[napi(js_name = "ftsDisableKey")]
    pub fn fts_disable_key(&self, key: String) -> Result<()> {
        self.inner
            .fts_disable_key(&key)
            .map_err(|e| Error::from_reason(e.to_string()))
    }

    /// Check if FTS is enabled for a key.
    #[napi(js_name = "isFtsEnabled")]
    pub fn is_fts_enabled(&self, key: String) -> Result<bool> {
        self.inner
            .is_fts_enabled(&key)
            .map_err(|e| Error::from_reason(e.to_string()))
    }

    // =========================================================================
    // KeyInfo Command
    // =========================================================================

    /// Get detailed information about a key.
    #[napi]
    pub fn keyinfo(&self, key: String) -> Result<Option<KeyInfoJs>> {
        self.inner
            .keyinfo(&key)
            .map(|opt| opt.map(|info| {
                let type_str = match info.key_type {
                    redlite::KeyType::String => "string",
                    redlite::KeyType::List => "list",
                    redlite::KeyType::Set => "set",
                    redlite::KeyType::Hash => "hash",
                    redlite::KeyType::ZSet => "zset",
                    redlite::KeyType::Stream => "stream",
                    redlite::KeyType::Json => "ReJSON-RL",
                };
                KeyInfoJs {
                    key_type: type_str.to_string(),
                    ttl: info.ttl,
                    created_at: info.created_at,
                    updated_at: info.updated_at,
                }
            }))
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

/// Parse retention value for TypeScript:
/// - None or 0: Unlimited
/// - Positive: Time in milliseconds
/// - Negative: Count (absolute value)
fn parse_retention_ts(retention: Option<i64>) -> redlite::RetentionType {
    match retention {
        None | Some(0) => redlite::RetentionType::Unlimited,
        Some(n) if n > 0 => redlite::RetentionType::Time(n),
        Some(n) => redlite::RetentionType::Count(-n),
    }
}
