use async_trait::async_trait;
use redlite::{Db, StreamId, ZMember};
use std::path::PathBuf;
use std::sync::Arc;

use super::{ClientError, ClientResult, RedisLikeClient, StreamEntry};

/// Redlite embedded client - in-process Arc<Db>
/// Direct access to SQLite-backed storage without network overhead
#[derive(Clone)]
pub struct RedliteEmbeddedClient {
    db: Arc<Db>,
    /// Path to database file (None for in-memory)
    db_path: Option<PathBuf>,
}

impl RedliteEmbeddedClient {
    /// Create an in-memory Redlite database
    pub fn new_memory() -> ClientResult<Self> {
        let db = Db::open_memory().map_err(|e| ClientError::Connection(e.to_string()))?;
        Ok(RedliteEmbeddedClient {
            db: Arc::new(db),
            db_path: None,
        })
    }

    /// Create a file-backed Redlite database
    pub fn new_file(path: &str) -> ClientResult<Self> {
        let db = Db::open(path).map_err(|e| ClientError::Connection(e.to_string()))?;
        Ok(RedliteEmbeddedClient {
            db: Arc::new(db),
            db_path: Some(PathBuf::from(path)),
        })
    }

    /// Create an encrypted Redlite database using SQLCipher.
    /// Requires building with `--features encryption` (and `--no-default-features` to avoid bundled SQLite conflict).
    #[cfg(feature = "encryption")]
    pub fn new_encrypted(path: &str, key: &str) -> ClientResult<Self> {
        let db = Db::open_encrypted(path, key)
            .map_err(|e| ClientError::Connection(format!("Encryption error: {}", e)))?;
        Ok(RedliteEmbeddedClient {
            db: Arc::new(db),
            db_path: Some(PathBuf::from(path)),
        })
    }

    /// Create a compressed Redlite database using VFS-level compression.
    /// Requires building with `--features compression`.
    /// Compressor is selected at compile time via sqlite-compress-vfs features.
    #[cfg(feature = "compression")]
    pub fn new_compressed(path: &str) -> ClientResult<Self> {
        let db = Db::open_compressed(path)
            .map_err(|e| ClientError::Connection(format!("Compression error: {}", e)))?;
        Ok(RedliteEmbeddedClient {
            db: Arc::new(db),
            db_path: Some(PathBuf::from(path)),
        })
    }

    /// Wrap an existing Db instance (no path available for size measurement)
    pub fn from_db(db: Arc<Db>) -> Self {
        RedliteEmbeddedClient { db, db_path: None }
    }

    fn handle_error(e: redlite::KvError) -> ClientError {
        let msg = e.to_string();
        if msg.contains("not found") || msg.contains("does not exist") {
            ClientError::KeyNotFound(msg)
        } else if msg.contains("WRONGTYPE") || msg.contains("wrong type") {
            ClientError::TypeError(msg)
        } else if msg.contains("out of range") {
            ClientError::OutOfRange(msg)
        } else {
            ClientError::Operation(msg)
        }
    }
}

#[async_trait]
impl RedisLikeClient for RedliteEmbeddedClient {
    // ========== STRING OPERATIONS ==========

    async fn get(&self, key: &str) -> ClientResult<Option<Vec<u8>>> {
        self.db.get(key).map_err(Self::handle_error)
    }

    async fn set(&self, key: &str, value: &[u8]) -> ClientResult<()> {
        self.db.set(key, value, None).map_err(Self::handle_error)
    }

    async fn incr(&self, key: &str) -> ClientResult<i64> {
        self.db.incr(key).map_err(Self::handle_error)
    }

    async fn append(&self, key: &str, value: &[u8]) -> ClientResult<usize> {
        self.db
            .append(key, value)
            .map(|n| n as usize)
            .map_err(Self::handle_error)
    }

    async fn strlen(&self, key: &str) -> ClientResult<usize> {
        self.db
            .strlen(key)
            .map(|n| n as usize)
            .map_err(Self::handle_error)
    }

    async fn mget(&self, keys: &[&str]) -> ClientResult<Vec<Option<Vec<u8>>>> {
        Ok(self.db.mget(keys))
    }

    async fn mset(&self, pairs: &[(&str, &[u8])]) -> ClientResult<()> {
        self.db.mset(pairs).map_err(Self::handle_error)
    }

    // ========== LIST OPERATIONS ==========

    async fn lpush(&self, key: &str, values: &[&[u8]]) -> ClientResult<i64> {
        self.db.lpush(key, values).map_err(Self::handle_error)
    }

    async fn rpush(&self, key: &str, values: &[&[u8]]) -> ClientResult<i64> {
        self.db.rpush(key, values).map_err(Self::handle_error)
    }

    async fn lpop(&self, key: &str, count: Option<usize>) -> ClientResult<Vec<Vec<u8>>> {
        self.db.lpop(key, count).map_err(Self::handle_error)
    }

    async fn rpop(&self, key: &str, count: Option<usize>) -> ClientResult<Vec<Vec<u8>>> {
        self.db.rpop(key, count).map_err(Self::handle_error)
    }

    async fn llen(&self, key: &str) -> ClientResult<i64> {
        self.db.llen(key).map_err(Self::handle_error)
    }

    async fn lrange(&self, key: &str, start: i64, stop: i64) -> ClientResult<Vec<Vec<u8>>> {
        self.db.lrange(key, start, stop).map_err(Self::handle_error)
    }

    async fn lindex(&self, key: &str, index: i64) -> ClientResult<Option<Vec<u8>>> {
        self.db.lindex(key, index).map_err(Self::handle_error)
    }

    // ========== HASH OPERATIONS ==========

    async fn hset(&self, key: &str, field: &str, value: &[u8]) -> ClientResult<i64> {
        self.db
            .hset(key, &[(field, value)])
            .map_err(Self::handle_error)
    }

    async fn hget(&self, key: &str, field: &str) -> ClientResult<Option<Vec<u8>>> {
        self.db.hget(key, field).map_err(Self::handle_error)
    }

    async fn hgetall(&self, key: &str) -> ClientResult<Vec<(String, Vec<u8>)>> {
        self.db.hgetall(key).map_err(Self::handle_error)
    }

    async fn hmget(&self, key: &str, fields: &[&str]) -> ClientResult<Vec<Option<Vec<u8>>>> {
        self.db.hmget(key, fields).map_err(Self::handle_error)
    }

    async fn hlen(&self, key: &str) -> ClientResult<i64> {
        self.db.hlen(key).map_err(Self::handle_error)
    }

    async fn hdel(&self, key: &str, fields: &[&str]) -> ClientResult<i64> {
        self.db.hdel(key, fields).map_err(Self::handle_error)
    }

    async fn hincrby(&self, key: &str, field: &str, increment: i64) -> ClientResult<i64> {
        self.db
            .hincrby(key, field, increment)
            .map_err(Self::handle_error)
    }

    // ========== SET OPERATIONS ==========

    async fn sadd(&self, key: &str, members: &[&[u8]]) -> ClientResult<i64> {
        self.db.sadd(key, members).map_err(Self::handle_error)
    }

    async fn srem(&self, key: &str, members: &[&[u8]]) -> ClientResult<i64> {
        self.db.srem(key, members).map_err(Self::handle_error)
    }

    async fn smembers(&self, key: &str) -> ClientResult<Vec<Vec<u8>>> {
        self.db.smembers(key).map_err(Self::handle_error)
    }

    async fn sismember(&self, key: &str, member: &[u8]) -> ClientResult<bool> {
        self.db.sismember(key, member).map_err(Self::handle_error)
    }

    async fn scard(&self, key: &str) -> ClientResult<i64> {
        self.db.scard(key).map_err(Self::handle_error)
    }

    async fn spop(&self, key: &str, count: Option<usize>) -> ClientResult<Vec<Vec<u8>>> {
        self.db.spop(key, count).map_err(Self::handle_error)
    }

    async fn srandmember(&self, key: &str, count: Option<usize>) -> ClientResult<Vec<Vec<u8>>> {
        // Redlite uses Option<i64> for srandmember count
        let c = count.map(|n| n as i64);
        self.db.srandmember(key, c).map_err(Self::handle_error)
    }

    // ========== SORTED SET OPERATIONS ==========

    async fn zadd(&self, key: &str, members: &[(f64, &[u8])]) -> ClientResult<i64> {
        let zmembers: Vec<ZMember> = members
            .iter()
            .map(|(score, member)| ZMember::new(*score, member.to_vec()))
            .collect();
        self.db.zadd(key, &zmembers).map_err(Self::handle_error)
    }

    async fn zrem(&self, key: &str, members: &[&[u8]]) -> ClientResult<i64> {
        self.db.zrem(key, members).map_err(Self::handle_error)
    }

    async fn zrange(&self, key: &str, start: i64, stop: i64) -> ClientResult<Vec<Vec<u8>>> {
        let results = self
            .db
            .zrange(key, start, stop, false)
            .map_err(Self::handle_error)?;
        Ok(results.into_iter().map(|z| z.member).collect())
    }

    async fn zrangebyscore(&self, key: &str, min: f64, max: f64) -> ClientResult<Vec<Vec<u8>>> {
        let results = self
            .db
            .zrangebyscore(key, min, max, None, None)
            .map_err(Self::handle_error)?;
        Ok(results.into_iter().map(|z| z.member).collect())
    }

    async fn zscore(&self, key: &str, member: &[u8]) -> ClientResult<Option<f64>> {
        self.db.zscore(key, member).map_err(Self::handle_error)
    }

    async fn zrank(&self, key: &str, member: &[u8]) -> ClientResult<Option<i64>> {
        self.db.zrank(key, member).map_err(Self::handle_error)
    }

    async fn zcard(&self, key: &str) -> ClientResult<i64> {
        self.db.zcard(key).map_err(Self::handle_error)
    }

    async fn zcount(&self, key: &str, min: f64, max: f64) -> ClientResult<i64> {
        self.db.zcount(key, min, max).map_err(Self::handle_error)
    }

    // ========== STREAM OPERATIONS ==========

    async fn xadd(
        &self,
        key: &str,
        id: &str,
        fields: &[(&str, &[u8])],
    ) -> ClientResult<String> {
        // Convert fields from (&str, &[u8]) to (&[u8], &[u8])
        let field_pairs: Vec<(&[u8], &[u8])> = fields
            .iter()
            .map(|(k, v)| (k.as_bytes() as &[u8], *v as &[u8]))
            .collect();

        // Parse ID - "*" means auto-generate
        let stream_id = if id == "*" {
            None
        } else {
            StreamId::parse(id)
        };

        // xadd(key, id, fields, nomkstream, maxlen, minid, approximate)
        // Returns Option<StreamId> - None only if nomkstream=true and stream doesn't exist
        self.db
            .xadd(key, stream_id, &field_pairs, false, None, None, false)
            .map_err(Self::handle_error)?
            .map(|id| id.to_string())
            .ok_or_else(|| ClientError::Operation("Stream not created".to_string()))
    }

    async fn xlen(&self, key: &str) -> ClientResult<i64> {
        self.db.xlen(key).map_err(Self::handle_error)
    }

    async fn xrange(&self, key: &str, start: &str, end: &str) -> ClientResult<Vec<StreamEntry>> {
        let start_id = StreamId::parse(start).unwrap_or(StreamId::min());
        let end_id = StreamId::parse(end).unwrap_or(StreamId::max());

        let entries = self
            .db
            .xrange(key, start_id, end_id, None)
            .map_err(Self::handle_error)?;

        Ok(entries
            .into_iter()
            .map(|e| StreamEntry {
                id: e.id.to_string(),
                fields: e
                    .fields
                    .into_iter()
                    .map(|(k, v)| (String::from_utf8_lossy(&k).to_string(), v))
                    .collect(),
            })
            .collect())
    }

    async fn xrevrange(&self, key: &str, end: &str, start: &str) -> ClientResult<Vec<StreamEntry>> {
        let start_id = StreamId::parse(start).unwrap_or(StreamId::min());
        let end_id = StreamId::parse(end).unwrap_or(StreamId::max());

        let entries = self
            .db
            .xrevrange(key, end_id, start_id, None)
            .map_err(Self::handle_error)?;

        Ok(entries
            .into_iter()
            .map(|e| StreamEntry {
                id: e.id.to_string(),
                fields: e
                    .fields
                    .into_iter()
                    .map(|(k, v)| (String::from_utf8_lossy(&k).to_string(), v))
                    .collect(),
            })
            .collect())
    }

    async fn xread(&self, keys: &[&str], ids: &[&str]) -> ClientResult<Vec<StreamEntry>> {
        // Build the keys and IDs for xread
        let stream_ids: Vec<StreamId> = ids
            .iter()
            .map(|id| StreamId::parse(id).unwrap_or(StreamId::min()))
            .collect();

        // xread(keys, ids, count)
        let results = self
            .db
            .xread(keys, &stream_ids, None)
            .map_err(Self::handle_error)?;

        // Flatten all stream entries
        let mut entries = Vec::new();
        for (_key, stream_entries) in results {
            for e in stream_entries {
                entries.push(StreamEntry {
                    id: e.id.to_string(),
                    fields: e
                        .fields
                        .into_iter()
                        .map(|(k, v)| (String::from_utf8_lossy(&k).to_string(), v))
                        .collect(),
                });
            }
        }
        Ok(entries)
    }

    async fn xdel(&self, key: &str, ids: &[&str]) -> ClientResult<i64> {
        let stream_ids: Vec<StreamId> = ids
            .iter()
            .filter_map(|id| StreamId::parse(id))
            .collect();

        self.db.xdel(key, &stream_ids).map_err(Self::handle_error)
    }

    async fn xtrim(&self, key: &str, maxlen: i64) -> ClientResult<i64> {
        // xtrim(key, maxlen: Option<i64>, minid: Option<StreamId>, approximate: bool)
        self.db
            .xtrim(key, Some(maxlen), None, false)
            .map_err(Self::handle_error)
    }

    // ========== KEY OPERATIONS ==========

    async fn del(&self, keys: &[&str]) -> ClientResult<i64> {
        self.db.del(keys).map_err(Self::handle_error)
    }

    async fn exists(&self, keys: &[&str]) -> ClientResult<i64> {
        self.db.exists(keys).map_err(Self::handle_error)
    }

    async fn key_type(&self, key: &str) -> ClientResult<String> {
        match self.db.key_type(key).map_err(Self::handle_error)? {
            Some(kt) => Ok(kt.as_str().to_string()),
            None => Ok("none".to_string()),
        }
    }

    async fn expire(&self, key: &str, seconds: usize) -> ClientResult<bool> {
        self.db
            .expire(key, seconds as i64)
            .map_err(Self::handle_error)
    }

    async fn ttl(&self, key: &str) -> ClientResult<i64> {
        self.db.ttl(key).map_err(Self::handle_error)
    }

    // ========== UTILITY OPERATIONS ==========

    async fn flushdb(&self) -> ClientResult<()> {
        self.db.flushdb().map_err(Self::handle_error)
    }

    async fn ping(&self) -> ClientResult<()> {
        // For embedded, ping always succeeds if we have a db reference
        Ok(())
    }

    // ========== REDLITE-SPECIFIC OPERATIONS ==========

    async fn history_enable(&self, key: &str) -> ClientResult<()> {
        use redlite::types::RetentionType;
        self.db
            .history_enable_key(key, RetentionType::Unlimited)
            .map_err(Self::handle_error)
    }

    async fn history_get(&self, key: &str) -> ClientResult<i64> {
        self.db
            .history_get(key, None, None, None)
            .map(|entries| entries.len() as i64)
            .map_err(Self::handle_error)
    }

    async fn history_getat(&self, key: &str, timestamp: i64) -> ClientResult<i64> {
        self.db
            .history_get_at(key, timestamp)
            .map(|val| if val.is_some() { 1i64 } else { 0i64 })
            .map_err(Self::handle_error)
    }

    async fn history_stats(&self, key: &str) -> ClientResult<i64> {
        self.db
            .history_stats(Some(key))
            .map(|_stats| {
                // Return a count representing available statistics fields
                4i64 // total_entries, oldest_timestamp, newest_timestamp, storage_bytes
            })
            .map_err(Self::handle_error)
    }

    async fn history_disable(&self, key: &str) -> ClientResult<()> {
        self.db
            .history_disable_key(key)
            .map_err(Self::handle_error)
    }

    async fn history_clear(&self, key: &str) -> ClientResult<i64> {
        self.db
            .history_clear_key(key, None)
            .map_err(Self::handle_error)
    }

    async fn history_prune(&self, timestamp: i64) -> ClientResult<i64> {
        self.db
            .history_prune(timestamp)
            .map_err(Self::handle_error)
    }

    async fn history_list(&self) -> ClientResult<i64> {
        self.db
            .history_list_keys(None)
            .map(|keys| keys.len() as i64)
            .map_err(Self::handle_error)
    }

    async fn keyinfo(&self, key: &str) -> ClientResult<i64> {
        self.db
            .keyinfo(key)
            .map(|info| {
                // Return a count representing available metadata fields
                if info.is_some() {
                    4i64 // type, ttl, created_at, updated_at
                } else {
                    0i64
                }
            })
            .map_err(Self::handle_error)
    }

    async fn autovacuum(&self) -> ClientResult<i64> {
        Ok(if self.db.autovacuum_enabled() { 1i64 } else { 0i64 })
    }

    async fn vacuum(&self) -> ClientResult<i64> {
        self.db.vacuum().map_err(Self::handle_error)
    }

    // ========== SIZE/MEMORY MEASUREMENT ==========

    async fn get_db_size_bytes(&self) -> ClientResult<Option<u64>> {
        // For file-backed databases, measure total disk usage (db + WAL + shm)
        // For in-memory databases, return None
        match &self.db_path {
            Some(path) => {
                let mut total_bytes: u64 = 0;

                // Main database file
                if let Ok(meta) = std::fs::metadata(path) {
                    total_bytes += meta.len();
                }

                // WAL file (write-ahead log)
                let wal_path = path.with_extension("db-wal");
                if let Ok(meta) = std::fs::metadata(&wal_path) {
                    total_bytes += meta.len();
                }

                // SHM file (shared memory)
                let shm_path = path.with_extension("db-shm");
                if let Ok(meta) = std::fs::metadata(&shm_path) {
                    total_bytes += meta.len();
                }

                Ok(Some(total_bytes))
            }
            None => Ok(None), // In-memory DB has no file size
        }
    }

    async fn get_history_count(&self) -> ClientResult<Option<(i64, i64)>> {
        // Get global history stats (total_entries, storage_bytes)
        match self.db.history_stats(None) {
            Ok(stats) => Ok(Some((stats.total_entries, stats.storage_bytes))),
            Err(_) => Ok(Some((0, 0))), // No history entries
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_string_operations() {
        let client = RedliteEmbeddedClient::new_memory().unwrap();

        // SET/GET
        client.set("key1", b"value1").await.unwrap();
        let result = client.get("key1").await.unwrap();
        assert_eq!(result, Some(b"value1".to_vec()));

        // GET non-existent
        let result = client.get("nonexistent").await.unwrap();
        assert_eq!(result, None);

        // INCR
        client.set("counter", b"10").await.unwrap();
        let result = client.incr("counter").await.unwrap();
        assert_eq!(result, 11);
    }

    #[tokio::test]
    async fn test_list_operations() {
        let client = RedliteEmbeddedClient::new_memory().unwrap();

        // LPUSH
        let len = client.lpush("list1", &[b"a", b"b", b"c"]).await.unwrap();
        assert_eq!(len, 3);

        // LLEN
        let len = client.llen("list1").await.unwrap();
        assert_eq!(len, 3);

        // LRANGE
        let items = client.lrange("list1", 0, -1).await.unwrap();
        assert_eq!(items.len(), 3);

        // LPOP
        let popped = client.lpop("list1", Some(1)).await.unwrap();
        assert_eq!(popped.len(), 1);
    }

    #[tokio::test]
    async fn test_hash_operations() {
        let client = RedliteEmbeddedClient::new_memory().unwrap();

        // HSET
        client.hset("hash1", "field1", b"value1").await.unwrap();

        // HGET
        let result = client.hget("hash1", "field1").await.unwrap();
        assert_eq!(result, Some(b"value1".to_vec()));

        // HGETALL
        let all = client.hgetall("hash1").await.unwrap();
        assert_eq!(all.len(), 1);

        // HLEN
        let len = client.hlen("hash1").await.unwrap();
        assert_eq!(len, 1);
    }

    #[tokio::test]
    async fn test_set_operations() {
        let client = RedliteEmbeddedClient::new_memory().unwrap();

        // SADD
        let added = client.sadd("set1", &[b"a", b"b", b"c"]).await.unwrap();
        assert_eq!(added, 3);

        // SCARD
        let card = client.scard("set1").await.unwrap();
        assert_eq!(card, 3);

        // SISMEMBER
        let is_member = client.sismember("set1", b"a").await.unwrap();
        assert!(is_member);

        // SMEMBERS
        let members = client.smembers("set1").await.unwrap();
        assert_eq!(members.len(), 3);
    }

    #[tokio::test]
    async fn test_sorted_set_operations() {
        let client = RedliteEmbeddedClient::new_memory().unwrap();

        // ZADD
        let added = client
            .zadd("zset1", &[(1.0, b"a"), (2.0, b"b"), (3.0, b"c")])
            .await
            .unwrap();
        assert_eq!(added, 3);

        // ZCARD
        let card = client.zcard("zset1").await.unwrap();
        assert_eq!(card, 3);

        // ZSCORE
        let score = client.zscore("zset1", b"b").await.unwrap();
        assert_eq!(score, Some(2.0));

        // ZRANGE
        let members = client.zrange("zset1", 0, -1).await.unwrap();
        assert_eq!(members.len(), 3);
    }

    #[tokio::test]
    async fn test_key_operations() {
        let client = RedliteEmbeddedClient::new_memory().unwrap();

        client.set("key1", b"value1").await.unwrap();

        // EXISTS
        let exists = client.exists(&["key1"]).await.unwrap();
        assert_eq!(exists, 1);

        // TYPE
        let key_type = client.key_type("key1").await.unwrap();
        assert_eq!(key_type, "string");

        // DEL
        let deleted = client.del(&["key1"]).await.unwrap();
        assert_eq!(deleted, 1);

        // Verify deleted
        let exists = client.exists(&["key1"]).await.unwrap();
        assert_eq!(exists, 0);
    }

    #[tokio::test]
    async fn test_utility_operations() {
        let client = RedliteEmbeddedClient::new_memory().unwrap();

        // PING
        client.ping().await.unwrap();

        // Set some data
        client.set("key1", b"value1").await.unwrap();

        // FLUSHDB
        client.flushdb().await.unwrap();

        // Verify flushed
        let result = client.get("key1").await.unwrap();
        assert_eq!(result, None);
    }
}
