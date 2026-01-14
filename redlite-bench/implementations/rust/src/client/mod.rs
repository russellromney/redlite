use async_trait::async_trait;
use crate::error::ClientResult;

pub mod redis_adapter;
pub mod redlite_embedded_adapter;

pub use redis_adapter::RedisClient;
pub use redlite_embedded_adapter::RedliteEmbeddedClient;

pub use crate::error::ClientError;

/// Represents a single stream entry returned by XREAD/XRANGE
#[derive(Debug, Clone)]
pub struct StreamEntry {
    pub id: String,
    pub fields: Vec<(String, Vec<u8>)>,
}

/// Core trait for Redis-like operations
/// Supports all 48 operations across 6 data types
#[async_trait]
pub trait RedisLikeClient: Send + Sync + Clone {
    // ========== STRING OPERATIONS (7) ==========

    /// GET key - Get value associated with key
    async fn get(&self, key: &str) -> ClientResult<Option<Vec<u8>>>;

    /// SET key value - Set key to hold value
    async fn set(&self, key: &str, value: &[u8]) -> ClientResult<()>;

    /// INCR key - Increment value (assumes numeric)
    async fn incr(&self, key: &str) -> ClientResult<i64>;

    /// APPEND key value - Append value to existing string
    async fn append(&self, key: &str, value: &[u8]) -> ClientResult<usize>;

    /// STRLEN key - Get length of string value
    async fn strlen(&self, key: &str) -> ClientResult<usize>;

    /// MGET keys - Get multiple values
    async fn mget(&self, keys: &[&str]) -> ClientResult<Vec<Option<Vec<u8>>>>;

    /// MSET key value key value - Set multiple key-value pairs
    async fn mset(&self, pairs: &[(&str, &[u8])]) -> ClientResult<()>;

    // ========== LIST OPERATIONS (7) ==========

    /// LPUSH key values - Push elements to head of list
    async fn lpush(&self, key: &str, values: &[&[u8]]) -> ClientResult<i64>;

    /// RPUSH key values - Push elements to tail of list
    async fn rpush(&self, key: &str, values: &[&[u8]]) -> ClientResult<i64>;

    /// LPOP key - Pop element from head of list
    async fn lpop(&self, key: &str, count: Option<usize>) -> ClientResult<Vec<Vec<u8>>>;

    /// RPOP key - Pop element from tail of list
    async fn rpop(&self, key: &str, count: Option<usize>) -> ClientResult<Vec<Vec<u8>>>;

    /// LLEN key - Get length of list
    async fn llen(&self, key: &str) -> ClientResult<i64>;

    /// LRANGE key start stop - Get range of elements from list
    async fn lrange(&self, key: &str, start: i64, stop: i64) -> ClientResult<Vec<Vec<u8>>>;

    /// LINDEX key index - Get element at index
    async fn lindex(&self, key: &str, index: i64) -> ClientResult<Option<Vec<u8>>>;

    // ========== HASH OPERATIONS (7) ==========

    /// HSET key field value - Set field in hash
    async fn hset(&self, key: &str, field: &str, value: &[u8]) -> ClientResult<i64>;

    /// HGET key field - Get field value from hash
    async fn hget(&self, key: &str, field: &str) -> ClientResult<Option<Vec<u8>>>;

    /// HGETALL key - Get all fields and values from hash
    async fn hgetall(&self, key: &str) -> ClientResult<Vec<(String, Vec<u8>)>>;

    /// HMGET key fields - Get multiple field values from hash
    async fn hmget(&self, key: &str, fields: &[&str]) -> ClientResult<Vec<Option<Vec<u8>>>>;

    /// HLEN key - Get number of fields in hash
    async fn hlen(&self, key: &str) -> ClientResult<i64>;

    /// HDEL key fields - Delete fields from hash
    async fn hdel(&self, key: &str, fields: &[&str]) -> ClientResult<i64>;

    /// HINCRBY key field increment - Increment field value
    async fn hincrby(&self, key: &str, field: &str, increment: i64) -> ClientResult<i64>;

    // ========== SET OPERATIONS (7) ==========

    /// SADD key members - Add members to set
    async fn sadd(&self, key: &str, members: &[&[u8]]) -> ClientResult<i64>;

    /// SREM key members - Remove members from set
    async fn srem(&self, key: &str, members: &[&[u8]]) -> ClientResult<i64>;

    /// SMEMBERS key - Get all members of set
    async fn smembers(&self, key: &str) -> ClientResult<Vec<Vec<u8>>>;

    /// SISMEMBER key member - Check if member in set
    async fn sismember(&self, key: &str, member: &[u8]) -> ClientResult<bool>;

    /// SCARD key - Get cardinality of set
    async fn scard(&self, key: &str) -> ClientResult<i64>;

    /// SPOP key count - Remove and return random members
    async fn spop(&self, key: &str, count: Option<usize>) -> ClientResult<Vec<Vec<u8>>>;

    /// SRANDMEMBER key count - Return random members without removing
    async fn srandmember(&self, key: &str, count: Option<usize>) -> ClientResult<Vec<Vec<u8>>>;

    // ========== SORTED SET OPERATIONS (8) ==========

    /// ZADD key members - Add members with scores to sorted set
    async fn zadd(&self, key: &str, members: &[(f64, &[u8])]) -> ClientResult<i64>;

    /// ZREM key members - Remove members from sorted set
    async fn zrem(&self, key: &str, members: &[&[u8]]) -> ClientResult<i64>;

    /// ZRANGE key start stop - Get range of members by index
    async fn zrange(&self, key: &str, start: i64, stop: i64) -> ClientResult<Vec<Vec<u8>>>;

    /// ZRANGEBYSCORE key min max - Get range of members by score
    async fn zrangebyscore(&self, key: &str, min: f64, max: f64) -> ClientResult<Vec<Vec<u8>>>;

    /// ZSCORE key member - Get score of member
    async fn zscore(&self, key: &str, member: &[u8]) -> ClientResult<Option<f64>>;

    /// ZRANK key member - Get rank of member
    async fn zrank(&self, key: &str, member: &[u8]) -> ClientResult<Option<i64>>;

    /// ZCARD key - Get cardinality of sorted set
    async fn zcard(&self, key: &str) -> ClientResult<i64>;

    /// ZCOUNT key min max - Count members in score range
    async fn zcount(&self, key: &str, min: f64, max: f64) -> ClientResult<i64>;

    // ========== STREAM OPERATIONS (7) ==========

    /// XADD key id fields - Add entry to stream
    /// id can be "*" for auto-generated ID
    async fn xadd(&self, key: &str, id: &str, fields: &[(&str, &[u8])]) -> ClientResult<String>;

    /// XLEN key - Get length of stream
    async fn xlen(&self, key: &str) -> ClientResult<i64>;

    /// XRANGE key start end - Get range of entries by ID
    async fn xrange(&self, key: &str, start: &str, end: &str) -> ClientResult<Vec<StreamEntry>>;

    /// XREVRANGE key end start - Get range of entries in reverse
    async fn xrevrange(&self, key: &str, end: &str, start: &str) -> ClientResult<Vec<StreamEntry>>;

    /// XREAD keys ids - Read from streams starting at IDs
    async fn xread(&self, keys: &[&str], ids: &[&str]) -> ClientResult<Vec<StreamEntry>>;

    /// XDEL key ids - Delete entries from stream
    async fn xdel(&self, key: &str, ids: &[&str]) -> ClientResult<i64>;

    /// XTRIM key maxlen - Trim stream to approximate length
    async fn xtrim(&self, key: &str, maxlen: i64) -> ClientResult<i64>;

    // ========== KEY OPERATIONS (5) ==========

    /// DEL keys - Delete keys
    async fn del(&self, keys: &[&str]) -> ClientResult<i64>;

    /// EXISTS keys - Check if keys exist
    async fn exists(&self, keys: &[&str]) -> ClientResult<i64>;

    /// TYPE key - Get type of key
    async fn key_type(&self, key: &str) -> ClientResult<String>;

    /// EXPIRE key seconds - Set expiration on key
    async fn expire(&self, key: &str, seconds: usize) -> ClientResult<bool>;

    /// TTL key - Get time to live in seconds
    async fn ttl(&self, key: &str) -> ClientResult<i64>;

    // ========== UTILITY OPERATIONS (2) ==========

    /// FLUSHDB - Clear all data
    async fn flushdb(&self) -> ClientResult<()>;

    /// PING - Test connection
    async fn ping(&self) -> ClientResult<()>;

    // ========== REDLITE-SPECIFIC OPERATIONS (11) ==========

    /// HISTORY ENABLE key - Enable history tracking for a key
    /// Returns Ok(()) on success, or error if not supported (e.g., Redis)
    async fn history_enable(&self, key: &str) -> ClientResult<()>;

    /// HISTORY GET key - Get all history entries for a key
    /// Returns a count of entries, or error if not supported
    async fn history_get(&self, key: &str) -> ClientResult<i64>;

    /// HISTORY GETAT key timestamp - Time-travel query to get value at specific time
    /// Returns 1 if found, 0 if not, or error if not supported
    async fn history_getat(&self, key: &str, timestamp: i64) -> ClientResult<i64>;

    /// HISTORY STATS key - Get statistics about history for a key
    /// Returns a count of stats entries, or error if not supported
    async fn history_stats(&self, key: &str) -> ClientResult<i64>;

    /// HISTORY DISABLE key - Disable history tracking for a key
    /// Returns Ok(()) on success, or error if not supported
    async fn history_disable(&self, key: &str) -> ClientResult<()>;

    /// HISTORY CLEAR key - Clear all history entries for a key
    /// Returns count of entries cleared, or error if not supported
    async fn history_clear(&self, key: &str) -> ClientResult<i64>;

    /// HISTORY PRUNE timestamp - Delete all history before timestamp across all keys
    /// Returns count of entries pruned, or error if not supported
    async fn history_prune(&self, timestamp: i64) -> ClientResult<i64>;

    /// HISTORY LIST - List all keys with history tracking enabled
    /// Returns count of keys with history, or error if not supported
    async fn history_list(&self) -> ClientResult<i64>;

    /// KEYINFO key - Get metadata about a key
    /// Returns a count of metadata fields, or error if not supported
    async fn keyinfo(&self, key: &str) -> ClientResult<i64>;

    /// AUTOVACUUM - Check or configure autovacuum status
    /// Returns 1 if enabled, 0 if disabled, or error if not supported
    async fn autovacuum(&self) -> ClientResult<i64>;

    /// VACUUM - Run storage optimization/cleanup
    /// Returns count of deleted entries, or error if not supported
    async fn vacuum(&self) -> ClientResult<i64>;

    // ========== SIZE/MEMORY MEASUREMENT (optional) ==========

    /// Get approximate database size in bytes (if available)
    /// Returns None if not measurable for this backend
    async fn get_db_size_bytes(&self) -> ClientResult<Option<u64>>;
}

#[cfg(test)]
mod tests {
    use super::*;

    // These are placeholder tests - actual tests will be in integration tests
    #[test]
    fn test_trait_compiles() {
        // If this compiles, the trait is properly defined
        // We can't instantiate a trait, but we can verify it's Send + Sync
        fn assert_send_sync<T: Send + Sync>() {}
        // Just verifying the trait system works
    }
}
