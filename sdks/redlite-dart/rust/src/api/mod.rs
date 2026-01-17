//! Flutter/Dart bindings for Redlite using flutter_rust_bridge.
//!
//! This module provides the public API exposed to Dart.

use std::sync::Arc;
use std::time::Duration;

use flutter_rust_bridge::frb;

/// Error type for Redlite operations
#[derive(Debug)]
pub struct RedliteError {
    pub message: String,
}

impl From<redlite::KvError> for RedliteError {
    fn from(err: redlite::KvError) -> Self {
        RedliteError {
            message: err.to_string(),
        }
    }
}

/// Set options for SET command
#[frb(dart_metadata = ("freezed"))]
pub struct SetOptions {
    /// Expiration in seconds
    pub ex: Option<i64>,
    /// Expiration in milliseconds
    pub px: Option<i64>,
    /// Only set if key does not exist
    pub nx: bool,
    /// Only set if key exists
    pub xx: bool,
}

impl Default for SetOptions {
    fn default() -> Self {
        Self {
            ex: None,
            px: None,
            nx: false,
            xx: false,
        }
    }
}

/// Sorted set member with score
#[frb(dart_metadata = ("freezed"))]
pub struct ZMember {
    pub score: f64,
    pub member: Vec<u8>,
}

/// Key type enum
#[frb]
pub enum KeyType {
    String,
    List,
    Set,
    Hash,
    ZSet,
    Stream,
    None,
}

impl From<Option<redlite::KeyType>> for KeyType {
    fn from(kt: Option<redlite::KeyType>) -> Self {
        match kt {
            Some(redlite::KeyType::String) => KeyType::String,
            Some(redlite::KeyType::List) => KeyType::List,
            Some(redlite::KeyType::Set) => KeyType::Set,
            Some(redlite::KeyType::Hash) => KeyType::Hash,
            Some(redlite::KeyType::ZSet) => KeyType::ZSet,
            Some(redlite::KeyType::Stream) => KeyType::Stream,
            None => KeyType::None,
        }
    }
}

/// Redlite embedded database - Redis API with SQLite durability.
///
/// Provides direct access to the Redlite database without network overhead.
#[frb(opaque)]
pub struct Db {
    inner: Arc<redlite::Db>,
}

impl Db {
    /// Open a database at the given path.
    /// Use ":memory:" for an in-memory database.
    #[frb(sync)]
    pub fn open(path: String) -> Result<Db, RedliteError> {
        redlite::Db::open(&path)
            .map(|db| Db {
                inner: Arc::new(db),
            })
            .map_err(Into::into)
    }

    /// Open an in-memory database.
    #[frb(sync)]
    pub fn open_memory() -> Result<Db, RedliteError> {
        redlite::Db::open_memory()
            .map(|db| Db {
                inner: Arc::new(db),
            })
            .map_err(Into::into)
    }

    /// Open a database with custom cache size in MB.
    #[frb(sync)]
    pub fn open_with_cache(path: String, cache_mb: i64) -> Result<Db, RedliteError> {
        redlite::Db::open_with_cache(&path, cache_mb)
            .map(|db| Db {
                inner: Arc::new(db),
            })
            .map_err(Into::into)
    }

    // =========================================================================
    // String Commands
    // =========================================================================

    /// Get the value of a key.
    pub fn get(&self, key: String) -> Result<Option<Vec<u8>>, RedliteError> {
        self.inner.get(&key).map_err(Into::into)
    }

    /// Set a key-value pair with optional TTL in seconds.
    pub fn set(
        &self,
        key: String,
        value: Vec<u8>,
        ttl_seconds: Option<i64>,
    ) -> Result<(), RedliteError> {
        let ttl = ttl_seconds.map(|s| Duration::from_secs(s as u64));
        self.inner.set(&key, &value, ttl).map_err(Into::into)
    }

    /// Set a key-value pair with options (NX, XX, EX, PX).
    pub fn set_opts(
        &self,
        key: String,
        value: Vec<u8>,
        options: SetOptions,
    ) -> Result<bool, RedliteError> {
        let ttl = if let Some(ex) = options.ex {
            Some(Duration::from_secs(ex as u64))
        } else if let Some(px) = options.px {
            Some(Duration::from_millis(px as u64))
        } else {
            None
        };
        let opts = redlite::SetOptions {
            ttl,
            nx: options.nx,
            xx: options.xx,
        };
        self.inner.set_opts(&key, &value, opts).map_err(Into::into)
    }

    /// Set key with expiration in seconds.
    pub fn setex(&self, key: String, seconds: i64, value: Vec<u8>) -> Result<(), RedliteError> {
        self.inner.setex(&key, seconds, &value).map_err(Into::into)
    }

    /// Set key with expiration in milliseconds.
    pub fn psetex(
        &self,
        key: String,
        milliseconds: i64,
        value: Vec<u8>,
    ) -> Result<(), RedliteError> {
        self.inner
            .psetex(&key, milliseconds, &value)
            .map_err(Into::into)
    }

    /// Get and delete a key.
    pub fn getdel(&self, key: String) -> Result<Option<Vec<u8>>, RedliteError> {
        self.inner.getdel(&key).map_err(Into::into)
    }

    /// Append value to a key, return new length.
    pub fn append(&self, key: String, value: Vec<u8>) -> Result<i64, RedliteError> {
        self.inner
            .append(&key, &value)
            .map(|n| n as i64)
            .map_err(Into::into)
    }

    /// Get the length of the value stored at key.
    pub fn strlen(&self, key: String) -> Result<i64, RedliteError> {
        self.inner
            .strlen(&key)
            .map(|n| n as i64)
            .map_err(Into::into)
    }

    /// Get a substring of the value stored at key.
    pub fn getrange(&self, key: String, start: i64, end: i64) -> Result<Vec<u8>, RedliteError> {
        self.inner.getrange(&key, start, end).map_err(Into::into)
    }

    /// Overwrite part of a string at key starting at offset.
    pub fn setrange(&self, key: String, offset: i64, value: Vec<u8>) -> Result<i64, RedliteError> {
        self.inner
            .setrange(&key, offset, &value)
            .map_err(Into::into)
    }

    /// Increment the integer value of a key by one.
    pub fn incr(&self, key: String) -> Result<i64, RedliteError> {
        self.inner.incr(&key).map_err(Into::into)
    }

    /// Decrement the integer value of a key by one.
    pub fn decr(&self, key: String) -> Result<i64, RedliteError> {
        self.inner.decr(&key).map_err(Into::into)
    }

    /// Increment the integer value of a key by amount.
    pub fn incrby(&self, key: String, increment: i64) -> Result<i64, RedliteError> {
        self.inner.incrby(&key, increment).map_err(Into::into)
    }

    /// Decrement the integer value of a key by amount.
    pub fn decrby(&self, key: String, decrement: i64) -> Result<i64, RedliteError> {
        self.inner.decrby(&key, decrement).map_err(Into::into)
    }

    /// Increment the float value of a key by amount.
    pub fn incrbyfloat(&self, key: String, increment: f64) -> Result<f64, RedliteError> {
        self.inner
            .incrbyfloat(&key, increment)
            .and_then(|s| s.parse::<f64>().map_err(|_| redlite::KvError::NotFloat))
            .map_err(Into::into)
    }

    // =========================================================================
    // Key Commands
    // =========================================================================

    /// Delete one or more keys, return count deleted.
    pub fn del(&self, keys: Vec<String>) -> Result<i64, RedliteError> {
        let key_refs: Vec<&str> = keys.iter().map(|s| s.as_str()).collect();
        self.inner
            .del(&key_refs)
            .map(|n| n as i64)
            .map_err(Into::into)
    }

    /// Check if keys exist, return count of existing keys.
    pub fn exists(&self, keys: Vec<String>) -> Result<i64, RedliteError> {
        let key_refs: Vec<&str> = keys.iter().map(|s| s.as_str()).collect();
        self.inner
            .exists(&key_refs)
            .map(|n| n as i64)
            .map_err(Into::into)
    }

    /// Get the type of a key.
    pub fn key_type(&self, key: String) -> Result<KeyType, RedliteError> {
        self.inner
            .key_type(&key)
            .map(|kt| kt.into())
            .map_err(Into::into)
    }

    /// Get TTL in seconds.
    pub fn ttl(&self, key: String) -> Result<i64, RedliteError> {
        self.inner.ttl(&key).map_err(Into::into)
    }

    /// Get TTL in milliseconds.
    pub fn pttl(&self, key: String) -> Result<i64, RedliteError> {
        self.inner.pttl(&key).map_err(Into::into)
    }

    /// Set expiration in seconds.
    pub fn expire(&self, key: String, seconds: i64) -> Result<bool, RedliteError> {
        self.inner.expire(&key, seconds).map_err(Into::into)
    }

    /// Set expiration in milliseconds.
    pub fn pexpire(&self, key: String, milliseconds: i64) -> Result<bool, RedliteError> {
        self.inner.pexpire(&key, milliseconds).map_err(Into::into)
    }

    /// Set expiration at Unix timestamp (seconds).
    pub fn expireat(&self, key: String, unix_time: i64) -> Result<bool, RedliteError> {
        self.inner.expireat(&key, unix_time).map_err(Into::into)
    }

    /// Set expiration at Unix timestamp (milliseconds).
    pub fn pexpireat(&self, key: String, unix_time_ms: i64) -> Result<bool, RedliteError> {
        self.inner
            .pexpireat(&key, unix_time_ms)
            .map_err(Into::into)
    }

    /// Remove expiration from a key.
    pub fn persist(&self, key: String) -> Result<bool, RedliteError> {
        self.inner.persist(&key).map_err(Into::into)
    }

    /// Rename a key.
    pub fn rename(&self, key: String, newkey: String) -> Result<(), RedliteError> {
        self.inner.rename(&key, &newkey).map_err(Into::into)
    }

    /// Rename a key only if newkey does not exist.
    pub fn renamenx(&self, key: String, newkey: String) -> Result<bool, RedliteError> {
        self.inner.renamenx(&key, &newkey).map_err(Into::into)
    }

    /// Find all keys matching a pattern.
    pub fn keys(&self, pattern: String) -> Result<Vec<String>, RedliteError> {
        self.inner.keys(&pattern).map_err(Into::into)
    }

    /// Get the number of keys in the database.
    pub fn dbsize(&self) -> Result<i64, RedliteError> {
        self.inner.dbsize().map(|n| n as i64).map_err(Into::into)
    }

    /// Delete all keys in the current database.
    pub fn flushdb(&self) -> Result<(), RedliteError> {
        self.inner.flushdb().map_err(Into::into)
    }

    // =========================================================================
    // Hash Commands
    // =========================================================================

    /// Set a single hash field.
    pub fn hset(&self, key: String, field: String, value: Vec<u8>) -> Result<i64, RedliteError> {
        let pairs: Vec<(&str, &[u8])> = vec![(&field, &value)];
        self.inner.hset(&key, &pairs).map_err(Into::into)
    }

    /// Set multiple hash fields.
    pub fn hmset(
        &self,
        key: String,
        mapping: Vec<(String, Vec<u8>)>,
    ) -> Result<i64, RedliteError> {
        let pairs: Vec<(&str, &[u8])> = mapping
            .iter()
            .map(|(f, v)| (f.as_str(), v.as_slice()))
            .collect();
        self.inner.hset(&key, &pairs).map_err(Into::into)
    }

    /// Get a hash field value.
    pub fn hget(&self, key: String, field: String) -> Result<Option<Vec<u8>>, RedliteError> {
        self.inner.hget(&key, &field).map_err(Into::into)
    }

    /// Delete hash fields.
    pub fn hdel(&self, key: String, fields: Vec<String>) -> Result<i64, RedliteError> {
        let field_refs: Vec<&str> = fields.iter().map(|s| s.as_str()).collect();
        self.inner
            .hdel(&key, &field_refs)
            .map(|n| n as i64)
            .map_err(Into::into)
    }

    /// Check if hash field exists.
    pub fn hexists(&self, key: String, field: String) -> Result<bool, RedliteError> {
        self.inner.hexists(&key, &field).map_err(Into::into)
    }

    /// Get number of fields in a hash.
    pub fn hlen(&self, key: String) -> Result<i64, RedliteError> {
        self.inner.hlen(&key).map(|n| n as i64).map_err(Into::into)
    }

    /// Get all field names in a hash.
    pub fn hkeys(&self, key: String) -> Result<Vec<String>, RedliteError> {
        self.inner.hkeys(&key).map_err(Into::into)
    }

    /// Get all values in a hash.
    pub fn hvals(&self, key: String) -> Result<Vec<Vec<u8>>, RedliteError> {
        self.inner.hvals(&key).map_err(Into::into)
    }

    /// Increment hash field by integer.
    pub fn hincrby(&self, key: String, field: String, increment: i64) -> Result<i64, RedliteError> {
        self.inner
            .hincrby(&key, &field, increment)
            .map_err(Into::into)
    }

    /// Get all fields and values in a hash.
    pub fn hgetall(&self, key: String) -> Result<Vec<(String, Vec<u8>)>, RedliteError> {
        self.inner.hgetall(&key).map_err(Into::into)
    }

    /// Get values of multiple hash fields.
    pub fn hmget(
        &self,
        key: String,
        fields: Vec<String>,
    ) -> Result<Vec<Option<Vec<u8>>>, RedliteError> {
        let field_refs: Vec<&str> = fields.iter().map(|s| s.as_str()).collect();
        self.inner.hmget(&key, &field_refs).map_err(Into::into)
    }

    // =========================================================================
    // List Commands
    // =========================================================================

    /// Push values to the left of a list.
    pub fn lpush(&self, key: String, values: Vec<Vec<u8>>) -> Result<i64, RedliteError> {
        let value_refs: Vec<&[u8]> = values.iter().map(|v| v.as_slice()).collect();
        self.inner
            .lpush(&key, &value_refs)
            .map(|n| n as i64)
            .map_err(Into::into)
    }

    /// Push values to the right of a list.
    pub fn rpush(&self, key: String, values: Vec<Vec<u8>>) -> Result<i64, RedliteError> {
        let value_refs: Vec<&[u8]> = values.iter().map(|v| v.as_slice()).collect();
        self.inner
            .rpush(&key, &value_refs)
            .map(|n| n as i64)
            .map_err(Into::into)
    }

    /// Pop values from the left of a list.
    pub fn lpop(&self, key: String, count: Option<i64>) -> Result<Vec<Vec<u8>>, RedliteError> {
        let cnt = count.map(|c| c as usize);
        self.inner.lpop(&key, cnt).map_err(Into::into)
    }

    /// Pop values from the right of a list.
    pub fn rpop(&self, key: String, count: Option<i64>) -> Result<Vec<Vec<u8>>, RedliteError> {
        let cnt = count.map(|c| c as usize);
        self.inner.rpop(&key, cnt).map_err(Into::into)
    }

    /// Get list length.
    pub fn llen(&self, key: String) -> Result<i64, RedliteError> {
        self.inner.llen(&key).map(|n| n as i64).map_err(Into::into)
    }

    /// Get a range of elements from a list.
    pub fn lrange(&self, key: String, start: i64, stop: i64) -> Result<Vec<Vec<u8>>, RedliteError> {
        self.inner.lrange(&key, start, stop).map_err(Into::into)
    }

    /// Get element at index in a list.
    pub fn lindex(&self, key: String, index: i64) -> Result<Option<Vec<u8>>, RedliteError> {
        self.inner.lindex(&key, index).map_err(Into::into)
    }

    /// Trim list to specified range.
    pub fn ltrim(&self, key: String, start: i64, stop: i64) -> Result<(), RedliteError> {
        self.inner.ltrim(&key, start, stop).map_err(Into::into)
    }

    /// Set element at index in a list.
    pub fn lset(&self, key: String, index: i64, value: Vec<u8>) -> Result<(), RedliteError> {
        self.inner.lset(&key, index, &value).map_err(Into::into)
    }

    // =========================================================================
    // Set Commands
    // =========================================================================

    /// Add members to a set.
    pub fn sadd(&self, key: String, members: Vec<Vec<u8>>) -> Result<i64, RedliteError> {
        let member_refs: Vec<&[u8]> = members.iter().map(|m| m.as_slice()).collect();
        self.inner
            .sadd(&key, &member_refs)
            .map(|n| n as i64)
            .map_err(Into::into)
    }

    /// Remove members from a set.
    pub fn srem(&self, key: String, members: Vec<Vec<u8>>) -> Result<i64, RedliteError> {
        let member_refs: Vec<&[u8]> = members.iter().map(|m| m.as_slice()).collect();
        self.inner
            .srem(&key, &member_refs)
            .map(|n| n as i64)
            .map_err(Into::into)
    }

    /// Get all members of a set.
    pub fn smembers(&self, key: String) -> Result<Vec<Vec<u8>>, RedliteError> {
        self.inner.smembers(&key).map_err(Into::into)
    }

    /// Check if member is in a set.
    pub fn sismember(&self, key: String, member: Vec<u8>) -> Result<bool, RedliteError> {
        self.inner.sismember(&key, &member).map_err(Into::into)
    }

    /// Get number of members in a set.
    pub fn scard(&self, key: String) -> Result<i64, RedliteError> {
        self.inner.scard(&key).map(|n| n as i64).map_err(Into::into)
    }

    /// Get difference between sets.
    pub fn sdiff(&self, keys: Vec<String>) -> Result<Vec<Vec<u8>>, RedliteError> {
        let key_refs: Vec<&str> = keys.iter().map(|s| s.as_str()).collect();
        self.inner.sdiff(&key_refs).map_err(Into::into)
    }

    /// Get intersection of sets.
    pub fn sinter(&self, keys: Vec<String>) -> Result<Vec<Vec<u8>>, RedliteError> {
        let key_refs: Vec<&str> = keys.iter().map(|s| s.as_str()).collect();
        self.inner.sinter(&key_refs).map_err(Into::into)
    }

    /// Get union of sets.
    pub fn sunion(&self, keys: Vec<String>) -> Result<Vec<Vec<u8>>, RedliteError> {
        let key_refs: Vec<&str> = keys.iter().map(|s| s.as_str()).collect();
        self.inner.sunion(&key_refs).map_err(Into::into)
    }

    // =========================================================================
    // Sorted Set Commands
    // =========================================================================

    /// Add members with scores to a sorted set.
    pub fn zadd(&self, key: String, members: Vec<ZMember>) -> Result<i64, RedliteError> {
        let zmembers: Vec<redlite::ZMember> = members
            .into_iter()
            .map(|m| redlite::ZMember {
                score: m.score,
                member: m.member,
            })
            .collect();
        self.inner
            .zadd(&key, &zmembers)
            .map(|n| n as i64)
            .map_err(Into::into)
    }

    /// Remove members from a sorted set.
    pub fn zrem(&self, key: String, members: Vec<Vec<u8>>) -> Result<i64, RedliteError> {
        let member_refs: Vec<&[u8]> = members.iter().map(|m| m.as_slice()).collect();
        self.inner
            .zrem(&key, &member_refs)
            .map(|n| n as i64)
            .map_err(Into::into)
    }

    /// Get score of a member in a sorted set.
    pub fn zscore(&self, key: String, member: Vec<u8>) -> Result<Option<f64>, RedliteError> {
        self.inner.zscore(&key, &member).map_err(Into::into)
    }

    /// Get number of members in a sorted set.
    pub fn zcard(&self, key: String) -> Result<i64, RedliteError> {
        self.inner.zcard(&key).map(|n| n as i64).map_err(Into::into)
    }

    /// Count members in a sorted set within a score range.
    pub fn zcount(&self, key: String, min_score: f64, max_score: f64) -> Result<i64, RedliteError> {
        self.inner
            .zcount(&key, min_score, max_score)
            .map(|n| n as i64)
            .map_err(Into::into)
    }

    /// Increment score of a member in a sorted set.
    pub fn zincrby(
        &self,
        key: String,
        increment: f64,
        member: Vec<u8>,
    ) -> Result<f64, RedliteError> {
        self.inner
            .zincrby(&key, increment, &member)
            .map_err(Into::into)
    }

    /// Get members by rank range (ascending order).
    pub fn zrange(
        &self,
        key: String,
        start: i64,
        stop: i64,
        with_scores: bool,
    ) -> Result<Vec<ZMember>, RedliteError> {
        self.inner
            .zrange(&key, start, stop, with_scores)
            .map(|members| {
                members
                    .into_iter()
                    .map(|m| ZMember {
                        score: m.score,
                        member: m.member,
                    })
                    .collect()
            })
            .map_err(Into::into)
    }

    /// Get members by rank range (descending order).
    pub fn zrevrange(
        &self,
        key: String,
        start: i64,
        stop: i64,
        with_scores: bool,
    ) -> Result<Vec<ZMember>, RedliteError> {
        self.inner
            .zrevrange(&key, start, stop, with_scores)
            .map(|members| {
                members
                    .into_iter()
                    .map(|m| ZMember {
                        score: m.score,
                        member: m.member,
                    })
                    .collect()
            })
            .map_err(Into::into)
    }

    /// Get rank of a member in a sorted set (ascending order).
    pub fn zrank(&self, key: String, member: Vec<u8>) -> Result<Option<i64>, RedliteError> {
        self.inner.zrank(&key, &member).map_err(Into::into)
    }

    /// Get rank of a member in a sorted set (descending order).
    pub fn zrevrank(&self, key: String, member: Vec<u8>) -> Result<Option<i64>, RedliteError> {
        self.inner.zrevrank(&key, &member).map_err(Into::into)
    }

    // =========================================================================
    // Multi-key Commands
    // =========================================================================

    /// Get values of multiple keys.
    pub fn mget(&self, keys: Vec<String>) -> Vec<Option<Vec<u8>>> {
        let key_refs: Vec<&str> = keys.iter().map(|s| s.as_str()).collect();
        self.inner.mget(&key_refs)
    }

    /// Set multiple key-value pairs atomically.
    pub fn mset(&self, pairs: Vec<(String, Vec<u8>)>) -> Result<(), RedliteError> {
        let pair_refs: Vec<(&str, &[u8])> = pairs
            .iter()
            .map(|(k, v)| (k.as_str(), v.as_slice()))
            .collect();
        self.inner.mset(&pair_refs).map_err(Into::into)
    }

    // =========================================================================
    // Scan Commands
    // =========================================================================

    /// Incrementally iterate keys matching a pattern.
    pub fn scan(
        &self,
        cursor: String,
        pattern: Option<String>,
        count: i64,
    ) -> Result<(String, Vec<String>), RedliteError> {
        self.inner
            .scan(&cursor, pattern.as_deref(), count as usize)
            .map_err(Into::into)
    }

    /// Incrementally iterate hash fields.
    pub fn hscan(
        &self,
        key: String,
        cursor: String,
        pattern: Option<String>,
        count: i64,
    ) -> Result<(String, Vec<(String, Vec<u8>)>), RedliteError> {
        self.inner
            .hscan(&key, &cursor, pattern.as_deref(), count as usize)
            .map_err(Into::into)
    }

    /// Incrementally iterate set members.
    pub fn sscan(
        &self,
        key: String,
        cursor: String,
        pattern: Option<String>,
        count: i64,
    ) -> Result<(String, Vec<Vec<u8>>), RedliteError> {
        self.inner
            .sscan(&key, &cursor, pattern.as_deref(), count as usize)
            .map_err(Into::into)
    }

    /// Incrementally iterate sorted set members with scores.
    pub fn zscan(
        &self,
        key: String,
        cursor: String,
        pattern: Option<String>,
        count: i64,
    ) -> Result<(String, Vec<(Vec<u8>, f64)>), RedliteError> {
        self.inner
            .zscan(&key, &cursor, pattern.as_deref(), count as usize)
            .map_err(Into::into)
    }

    // =========================================================================
    // Server Commands
    // =========================================================================

    /// Run SQLite VACUUM to reclaim space.
    pub fn vacuum(&self) -> Result<i64, RedliteError> {
        self.inner.vacuum().map(|n| n as i64).map_err(Into::into)
    }
}
