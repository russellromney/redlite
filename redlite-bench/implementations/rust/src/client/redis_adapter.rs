use async_trait::async_trait;
use redis::aio::Connection;
use redis::{Client, RedisError};

use super::{ClientError, ClientResult, RedisLikeClient, StreamEntry};

#[derive(Clone)]
pub struct RedisClient {
    client: Client,
}

impl RedisClient {
    pub fn new(url: &str) -> ClientResult<Self> {
        let client = Client::open(url).map_err(|e| ClientError::Connection(e.to_string()))?;

        Ok(RedisClient { client })
    }

    async fn get_connection(&self) -> ClientResult<Connection> {
        self.client
            .get_async_connection()
            .await
            .map_err(|e| ClientError::Connection(e.to_string()))
    }

    fn handle_redis_error(e: RedisError) -> ClientError {
        let msg = e.to_string();
        if msg.contains("not found") {
            ClientError::KeyNotFound(msg)
        } else if msg.contains("WRONGTYPE") {
            ClientError::TypeError(msg)
        } else if msg.contains("timeout") {
            ClientError::Timeout(msg)
        } else {
            ClientError::Operation(msg)
        }
    }
}

#[async_trait]
impl RedisLikeClient for RedisClient {
    // ========== STRING OPERATIONS ==========

    async fn get(&self, key: &str) -> ClientResult<Option<Vec<u8>>> {
        let mut conn = self.get_connection().await?;
        redis::cmd("GET")
            .arg(key)
            .query_async(&mut conn)
            .await
            .map_err(Self::handle_redis_error)
    }

    async fn set(&self, key: &str, value: &[u8]) -> ClientResult<()> {
        let mut conn = self.get_connection().await?;
        redis::cmd("SET")
            .arg(key)
            .arg(value)
            .query_async::<_, ()>(&mut conn)
            .await
            .map_err(Self::handle_redis_error)
    }

    async fn incr(&self, key: &str) -> ClientResult<i64> {
        let mut conn = self.get_connection().await?;
        redis::cmd("INCR")
            .arg(key)
            .query_async(&mut conn)
            .await
            .map_err(Self::handle_redis_error)
    }

    async fn append(&self, key: &str, value: &[u8]) -> ClientResult<usize> {
        let mut conn = self.get_connection().await?;
        redis::cmd("APPEND")
            .arg(key)
            .arg(value)
            .query_async(&mut conn)
            .await
            .map_err(Self::handle_redis_error)
    }

    async fn strlen(&self, key: &str) -> ClientResult<usize> {
        let mut conn = self.get_connection().await?;
        redis::cmd("STRLEN")
            .arg(key)
            .query_async(&mut conn)
            .await
            .map_err(Self::handle_redis_error)
    }

    async fn mget(&self, keys: &[&str]) -> ClientResult<Vec<Option<Vec<u8>>>> {
        let mut conn = self.get_connection().await?;
        redis::cmd("MGET")
            .arg(keys)
            .query_async(&mut conn)
            .await
            .map_err(Self::handle_redis_error)
    }

    async fn mset(&self, pairs: &[(&str, &[u8])]) -> ClientResult<()> {
        let mut conn = self.get_connection().await?;
        let mut cmd = redis::cmd("MSET");
        for (k, v) in pairs {
            cmd.arg(k).arg(v);
        }
        cmd.query_async::<_, ()>(&mut conn)
            .await
            .map_err(Self::handle_redis_error)
    }

    // ========== LIST OPERATIONS ==========

    async fn lpush(&self, key: &str, values: &[&[u8]]) -> ClientResult<i64> {
        let mut conn = self.get_connection().await?;
        let mut cmd = redis::cmd("LPUSH");
        cmd.arg(key);
        for v in values {
            cmd.arg(v);
        }
        cmd.query_async(&mut conn)
            .await
            .map_err(Self::handle_redis_error)
    }

    async fn rpush(&self, key: &str, values: &[&[u8]]) -> ClientResult<i64> {
        let mut conn = self.get_connection().await?;
        let mut cmd = redis::cmd("RPUSH");
        cmd.arg(key);
        for v in values {
            cmd.arg(v);
        }
        cmd.query_async(&mut conn)
            .await
            .map_err(Self::handle_redis_error)
    }

    async fn lpop(&self, key: &str, count: Option<usize>) -> ClientResult<Vec<Vec<u8>>> {
        let mut conn = self.get_connection().await?;
        let mut cmd = redis::cmd("LPOP");
        cmd.arg(key);
        if let Some(c) = count {
            cmd.arg(c);
        }
        cmd.query_async(&mut conn)
            .await
            .map_err(Self::handle_redis_error)
    }

    async fn rpop(&self, key: &str, count: Option<usize>) -> ClientResult<Vec<Vec<u8>>> {
        let mut conn = self.get_connection().await?;
        let mut cmd = redis::cmd("RPOP");
        cmd.arg(key);
        if let Some(c) = count {
            cmd.arg(c);
        }
        cmd.query_async(&mut conn)
            .await
            .map_err(Self::handle_redis_error)
    }

    async fn llen(&self, key: &str) -> ClientResult<i64> {
        let mut conn = self.get_connection().await?;
        redis::cmd("LLEN")
            .arg(key)
            .query_async(&mut conn)
            .await
            .map_err(Self::handle_redis_error)
    }

    async fn lrange(&self, key: &str, start: i64, stop: i64) -> ClientResult<Vec<Vec<u8>>> {
        let mut conn = self.get_connection().await?;
        redis::cmd("LRANGE")
            .arg(key)
            .arg(start)
            .arg(stop)
            .query_async(&mut conn)
            .await
            .map_err(Self::handle_redis_error)
    }

    async fn lindex(&self, key: &str, index: i64) -> ClientResult<Option<Vec<u8>>> {
        let mut conn = self.get_connection().await?;
        redis::cmd("LINDEX")
            .arg(key)
            .arg(index)
            .query_async(&mut conn)
            .await
            .map_err(Self::handle_redis_error)
    }

    // ========== HASH OPERATIONS ==========

    async fn hset(&self, key: &str, field: &str, value: &[u8]) -> ClientResult<i64> {
        let mut conn = self.get_connection().await?;
        redis::cmd("HSET")
            .arg(key)
            .arg(field)
            .arg(value)
            .query_async(&mut conn)
            .await
            .map_err(Self::handle_redis_error)
    }

    async fn hget(&self, key: &str, field: &str) -> ClientResult<Option<Vec<u8>>> {
        let mut conn = self.get_connection().await?;
        redis::cmd("HGET")
            .arg(key)
            .arg(field)
            .query_async(&mut conn)
            .await
            .map_err(Self::handle_redis_error)
    }

    async fn hgetall(&self, key: &str) -> ClientResult<Vec<(String, Vec<u8>)>> {
        let mut conn = self.get_connection().await?;
        let result: std::collections::HashMap<String, Vec<u8>> =
            redis::cmd("HGETALL")
                .arg(key)
                .query_async(&mut conn)
                .await
                .map_err(Self::handle_redis_error)?;

        Ok(result.into_iter().collect())
    }

    async fn hmget(&self, key: &str, fields: &[&str]) -> ClientResult<Vec<Option<Vec<u8>>>> {
        let mut conn = self.get_connection().await?;
        redis::cmd("HMGET")
            .arg(key)
            .arg(fields)
            .query_async(&mut conn)
            .await
            .map_err(Self::handle_redis_error)
    }

    async fn hlen(&self, key: &str) -> ClientResult<i64> {
        let mut conn = self.get_connection().await?;
        redis::cmd("HLEN")
            .arg(key)
            .query_async(&mut conn)
            .await
            .map_err(Self::handle_redis_error)
    }

    async fn hdel(&self, key: &str, fields: &[&str]) -> ClientResult<i64> {
        let mut conn = self.get_connection().await?;
        redis::cmd("HDEL")
            .arg(key)
            .arg(fields)
            .query_async(&mut conn)
            .await
            .map_err(Self::handle_redis_error)
    }

    async fn hincrby(&self, key: &str, field: &str, increment: i64) -> ClientResult<i64> {
        let mut conn = self.get_connection().await?;
        redis::cmd("HINCRBY")
            .arg(key)
            .arg(field)
            .arg(increment)
            .query_async(&mut conn)
            .await
            .map_err(Self::handle_redis_error)
    }

    // ========== SET OPERATIONS ==========

    async fn sadd(&self, key: &str, members: &[&[u8]]) -> ClientResult<i64> {
        let mut conn = self.get_connection().await?;
        let mut cmd = redis::cmd("SADD");
        cmd.arg(key);
        for m in members {
            cmd.arg(m);
        }
        cmd.query_async(&mut conn)
            .await
            .map_err(Self::handle_redis_error)
    }

    async fn srem(&self, key: &str, members: &[&[u8]]) -> ClientResult<i64> {
        let mut conn = self.get_connection().await?;
        let mut cmd = redis::cmd("SREM");
        cmd.arg(key);
        for m in members {
            cmd.arg(m);
        }
        cmd.query_async(&mut conn)
            .await
            .map_err(Self::handle_redis_error)
    }

    async fn smembers(&self, key: &str) -> ClientResult<Vec<Vec<u8>>> {
        let mut conn = self.get_connection().await?;
        redis::cmd("SMEMBERS")
            .arg(key)
            .query_async(&mut conn)
            .await
            .map_err(Self::handle_redis_error)
    }

    async fn sismember(&self, key: &str, member: &[u8]) -> ClientResult<bool> {
        let mut conn = self.get_connection().await?;
        redis::cmd("SISMEMBER")
            .arg(key)
            .arg(member)
            .query_async(&mut conn)
            .await
            .map_err(Self::handle_redis_error)
    }

    async fn scard(&self, key: &str) -> ClientResult<i64> {
        let mut conn = self.get_connection().await?;
        redis::cmd("SCARD")
            .arg(key)
            .query_async(&mut conn)
            .await
            .map_err(Self::handle_redis_error)
    }

    async fn spop(&self, key: &str, count: Option<usize>) -> ClientResult<Vec<Vec<u8>>> {
        let mut conn = self.get_connection().await?;
        let mut cmd = redis::cmd("SPOP");
        cmd.arg(key);
        if let Some(c) = count {
            cmd.arg(c);
        }
        cmd.query_async(&mut conn)
            .await
            .map_err(Self::handle_redis_error)
    }

    async fn srandmember(&self, key: &str, count: Option<usize>) -> ClientResult<Vec<Vec<u8>>> {
        let mut conn = self.get_connection().await?;
        let mut cmd = redis::cmd("SRANDMEMBER");
        cmd.arg(key);
        if let Some(c) = count {
            cmd.arg(c);
        }
        cmd.query_async(&mut conn)
            .await
            .map_err(Self::handle_redis_error)
    }

    // ========== SORTED SET OPERATIONS ==========

    async fn zadd(&self, key: &str, members: &[(f64, &[u8])]) -> ClientResult<i64> {
        let mut conn = self.get_connection().await?;
        let mut cmd = redis::cmd("ZADD");
        cmd.arg(key);
        for (score, member) in members {
            cmd.arg(score).arg(member);
        }
        cmd.query_async(&mut conn)
            .await
            .map_err(Self::handle_redis_error)
    }

    async fn zrem(&self, key: &str, members: &[&[u8]]) -> ClientResult<i64> {
        let mut conn = self.get_connection().await?;
        let mut cmd = redis::cmd("ZREM");
        cmd.arg(key);
        for m in members {
            cmd.arg(m);
        }
        cmd.query_async(&mut conn)
            .await
            .map_err(Self::handle_redis_error)
    }

    async fn zrange(&self, key: &str, start: i64, stop: i64) -> ClientResult<Vec<Vec<u8>>> {
        let mut conn = self.get_connection().await?;
        redis::cmd("ZRANGE")
            .arg(key)
            .arg(start)
            .arg(stop)
            .query_async(&mut conn)
            .await
            .map_err(Self::handle_redis_error)
    }

    async fn zrangebyscore(&self, key: &str, min: f64, max: f64) -> ClientResult<Vec<Vec<u8>>> {
        let mut conn = self.get_connection().await?;
        redis::cmd("ZRANGEBYSCORE")
            .arg(key)
            .arg(min)
            .arg(max)
            .query_async(&mut conn)
            .await
            .map_err(Self::handle_redis_error)
    }

    async fn zscore(&self, key: &str, member: &[u8]) -> ClientResult<Option<f64>> {
        let mut conn = self.get_connection().await?;
        redis::cmd("ZSCORE")
            .arg(key)
            .arg(member)
            .query_async(&mut conn)
            .await
            .map_err(Self::handle_redis_error)
    }

    async fn zrank(&self, key: &str, member: &[u8]) -> ClientResult<Option<i64>> {
        let mut conn = self.get_connection().await?;
        redis::cmd("ZRANK")
            .arg(key)
            .arg(member)
            .query_async(&mut conn)
            .await
            .map_err(Self::handle_redis_error)
    }

    async fn zcard(&self, key: &str) -> ClientResult<i64> {
        let mut conn = self.get_connection().await?;
        redis::cmd("ZCARD")
            .arg(key)
            .query_async(&mut conn)
            .await
            .map_err(Self::handle_redis_error)
    }

    async fn zcount(&self, key: &str, min: f64, max: f64) -> ClientResult<i64> {
        let mut conn = self.get_connection().await?;
        redis::cmd("ZCOUNT")
            .arg(key)
            .arg(min)
            .arg(max)
            .query_async(&mut conn)
            .await
            .map_err(Self::handle_redis_error)
    }

    // ========== STREAM OPERATIONS ==========

    async fn xadd(
        &self,
        key: &str,
        id: &str,
        fields: &[(&str, &[u8])],
    ) -> ClientResult<String> {
        let mut conn = self.get_connection().await?;
        let mut cmd = redis::cmd("XADD");
        cmd.arg(key).arg(id);
        for (k, v) in fields {
            cmd.arg(k).arg(v);
        }
        cmd.query_async(&mut conn)
            .await
            .map_err(Self::handle_redis_error)
    }

    async fn xlen(&self, key: &str) -> ClientResult<i64> {
        let mut conn = self.get_connection().await?;
        redis::cmd("XLEN")
            .arg(key)
            .query_async(&mut conn)
            .await
            .map_err(Self::handle_redis_error)
    }

    async fn xrange(&self, key: &str, start: &str, end: &str) -> ClientResult<Vec<StreamEntry>> {
        let mut conn = self.get_connection().await?;
        let result: Vec<(String, Vec<(String, Vec<u8>)>)> = redis::cmd("XRANGE")
            .arg(key)
            .arg(start)
            .arg(end)
            .query_async(&mut conn)
            .await
            .map_err(Self::handle_redis_error)?;

        Ok(result
            .into_iter()
            .map(|(id, fields)| StreamEntry { id, fields })
            .collect())
    }

    async fn xrevrange(
        &self,
        key: &str,
        end: &str,
        start: &str,
    ) -> ClientResult<Vec<StreamEntry>> {
        let mut conn = self.get_connection().await?;
        let result: Vec<(String, Vec<(String, Vec<u8>)>)> = redis::cmd("XREVRANGE")
            .arg(key)
            .arg(end)
            .arg(start)
            .query_async(&mut conn)
            .await
            .map_err(Self::handle_redis_error)?;

        Ok(result
            .into_iter()
            .map(|(id, fields)| StreamEntry { id, fields })
            .collect())
    }

    async fn xread(&self, keys: &[&str], ids: &[&str]) -> ClientResult<Vec<StreamEntry>> {
        let mut conn = self.get_connection().await?;
        let mut cmd = redis::cmd("XREAD");
        cmd.arg("STREAMS");
        for k in keys {
            cmd.arg(k);
        }
        for id in ids {
            cmd.arg(id);
        }

        let result: Vec<(String, Vec<(String, Vec<(String, Vec<u8>)>)>)> =
            cmd.query_async(&mut conn)
                .await
                .map_err(Self::handle_redis_error)?;

        let mut entries = Vec::new();
        for (_key, stream_entries) in result {
            for (id, fields) in stream_entries {
                entries.push(StreamEntry { id, fields });
            }
        }
        Ok(entries)
    }

    async fn xdel(&self, key: &str, ids: &[&str]) -> ClientResult<i64> {
        let mut conn = self.get_connection().await?;
        let mut cmd = redis::cmd("XDEL");
        cmd.arg(key);
        for id in ids {
            cmd.arg(id);
        }
        cmd.query_async(&mut conn)
            .await
            .map_err(Self::handle_redis_error)
    }

    async fn xtrim(&self, key: &str, maxlen: i64) -> ClientResult<i64> {
        let mut conn = self.get_connection().await?;
        redis::cmd("XTRIM")
            .arg(key)
            .arg("MAXLEN")
            .arg(maxlen)
            .query_async(&mut conn)
            .await
            .map_err(Self::handle_redis_error)
    }

    // ========== KEY OPERATIONS ==========

    async fn del(&self, keys: &[&str]) -> ClientResult<i64> {
        let mut conn = self.get_connection().await?;
        redis::cmd("DEL")
            .arg(keys)
            .query_async(&mut conn)
            .await
            .map_err(Self::handle_redis_error)
    }

    async fn exists(&self, keys: &[&str]) -> ClientResult<i64> {
        let mut conn = self.get_connection().await?;
        redis::cmd("EXISTS")
            .arg(keys)
            .query_async(&mut conn)
            .await
            .map_err(Self::handle_redis_error)
    }

    async fn key_type(&self, key: &str) -> ClientResult<String> {
        let mut conn = self.get_connection().await?;
        redis::cmd("TYPE")
            .arg(key)
            .query_async(&mut conn)
            .await
            .map_err(Self::handle_redis_error)
    }

    async fn expire(&self, key: &str, seconds: usize) -> ClientResult<bool> {
        let mut conn = self.get_connection().await?;
        redis::cmd("EXPIRE")
            .arg(key)
            .arg(seconds)
            .query_async(&mut conn)
            .await
            .map_err(Self::handle_redis_error)
    }

    async fn ttl(&self, key: &str) -> ClientResult<i64> {
        let mut conn = self.get_connection().await?;
        redis::cmd("TTL")
            .arg(key)
            .query_async(&mut conn)
            .await
            .map_err(Self::handle_redis_error)
    }

    // ========== UTILITY OPERATIONS ==========

    async fn flushdb(&self) -> ClientResult<()> {
        let mut conn = self.get_connection().await?;
        redis::cmd("FLUSHDB")
            .query_async::<_, ()>(&mut conn)
            .await
            .map_err(Self::handle_redis_error)
    }

    async fn ping(&self) -> ClientResult<()> {
        let mut conn = self.get_connection().await?;
        redis::cmd("PING")
            .query_async::<_, ()>(&mut conn)
            .await
            .map_err(Self::handle_redis_error)
    }

    // ========== REDLITE-SPECIFIC OPERATIONS ==========

    async fn history_enable(&self, _key: &str) -> ClientResult<()> {
        Err(ClientError::Operation(
            "HISTORY command not supported on Redis".to_string(),
        ))
    }

    async fn history_get(&self, _key: &str) -> ClientResult<i64> {
        Err(ClientError::Operation(
            "HISTORY command not supported on Redis".to_string(),
        ))
    }

    async fn keyinfo(&self, _key: &str) -> ClientResult<i64> {
        Err(ClientError::Operation(
            "KEYINFO command not supported on Redis".to_string(),
        ))
    }

    async fn vacuum(&self) -> ClientResult<i64> {
        Err(ClientError::Operation(
            "VACUUM command not supported on Redis".to_string(),
        ))
    }
}
