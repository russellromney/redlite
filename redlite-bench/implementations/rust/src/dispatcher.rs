//! Runtime operation dispatcher for executing operations by name
//!
//! This module provides a dynamic dispatcher that executes any supported operation
//! given its string name, enabling workload scenarios with weighted operation mixes.

use std::time::Instant;
use rand::Rng;

use crate::client::RedisLikeClient;
use crate::error::{Result, BenchError};

/// Execute a single operation by name and return latency in microseconds
///
/// Supports all 52 operations: 48 standard Redis operations + 4 Redlite-specific
pub async fn execute_operation<C: RedisLikeClient>(
    client: &C,
    operation: &str,
    dataset_size: usize,
    mut rng: rand::rngs::ThreadRng,
) -> Result<f64> {
    let op_upper = operation.to_uppercase();
    let key = format!("key_{}", rng.gen_range(0..dataset_size));
    let value = super::benchmark::generate_value();

    let t_start = Instant::now();

    match op_upper.as_str() {
        // STRING OPERATIONS
        "GET" => {
            client.get(&key).await?;
        }
        "SET" => {
            client.set(&key, &value).await?;
        }
        "INCR" => {
            client.incr(&key).await?;
        }
        "APPEND" => {
            client.append(&key, &value).await?;
        }
        "STRLEN" => {
            client.strlen(&key).await?;
        }
        "MGET" => {
            // Multi-get multiple keys
            for i in 0..10.min(dataset_size) {
                let k = format!("key_{}", i);
                let _ = client.get(&k).await;
            }
        }
        "MSET" => {
            // Multi-set multiple key-value pairs
            for i in 0..10.min(dataset_size) {
                let k = format!("key_{}", i);
                let _ = client.set(&k, &value).await;
            }
        }

        // LIST OPERATIONS
        "LPUSH" => {
            let list_key = format!("list_{}", rng.gen_range(0..dataset_size));
            client.lpush(&list_key, &[&value[..]]).await?;
        }
        "RPUSH" => {
            let list_key = format!("list_{}", rng.gen_range(0..dataset_size));
            client.rpush(&list_key, &[&value[..]]).await?;
        }
        "LPOP" => {
            let list_key = format!("list_{}", rng.gen_range(0..dataset_size));
            let _ = client.lpop(&list_key, None).await;
        }
        "RPOP" => {
            let list_key = format!("list_{}", rng.gen_range(0..dataset_size));
            let _ = client.rpop(&list_key, None).await;
        }
        "LLEN" => {
            let list_key = format!("list_{}", rng.gen_range(0..dataset_size));
            client.llen(&list_key).await?;
        }
        "LRANGE" => {
            let list_key = format!("list_{}", rng.gen_range(0..dataset_size));
            let _ = client.lrange(&list_key, 0, 99).await;
        }
        "LINDEX" => {
            let list_key = format!("list_{}", rng.gen_range(0..dataset_size));
            let _ = client.lindex(&list_key, rng.gen_range(0..100) as i64).await;
        }

        // HASH OPERATIONS
        "HSET" => {
            let hash_key = format!("hash_{}", rng.gen_range(0..dataset_size));
            let field = format!("field_{}", rng.gen_range(0..100));
            client.hset(&hash_key, &field, &value).await?;
        }
        "HGET" => {
            let hash_key = format!("hash_{}", rng.gen_range(0..dataset_size));
            let field = format!("field_{}", rng.gen_range(0..100));
            let _ = client.hget(&hash_key, &field).await;
        }
        "HGETALL" => {
            let hash_key = format!("hash_{}", rng.gen_range(0..dataset_size));
            let _ = client.hgetall(&hash_key).await;
        }
        "HMGET" => {
            let hash_key = format!("hash_{}", rng.gen_range(0..dataset_size));
            // Multi-get hash fields
            for i in 0..10 {
                let field = format!("field_{}", i);
                let _ = client.hget(&hash_key, &field).await;
            }
        }
        "HLEN" => {
            let hash_key = format!("hash_{}", rng.gen_range(0..dataset_size));
            client.hlen(&hash_key).await?;
        }
        "HDEL" => {
            let hash_key = format!("hash_{}", rng.gen_range(0..dataset_size));
            let field = format!("field_{}", rng.gen_range(0..100));
            let _ = client.hdel(&hash_key, &[field.as_str()]).await;
        }
        "HINCRBY" => {
            let hash_key = format!("hash_{}", rng.gen_range(0..dataset_size));
            let field = format!("counter_{}", rng.gen_range(0..100));
            let _ = client.hincrby(&hash_key, &field, 1).await;
        }

        // SET OPERATIONS
        "SADD" => {
            let set_key = format!("set_{}", rng.gen_range(0..dataset_size));
            let member = format!("member_{}", rng.gen_range(0..1000));
            client.sadd(&set_key, &[member.as_bytes()]).await?;
        }
        "SREM" => {
            let set_key = format!("set_{}", rng.gen_range(0..dataset_size));
            let member = format!("member_{}", rng.gen_range(0..1000));
            let _ = client.srem(&set_key, &[member.as_bytes()]).await;
        }
        "SMEMBERS" => {
            let set_key = format!("set_{}", rng.gen_range(0..dataset_size));
            let _ = client.smembers(&set_key).await;
        }
        "SISMEMBER" => {
            let set_key = format!("set_{}", rng.gen_range(0..dataset_size));
            let member = format!("member_{}", rng.gen_range(0..1000));
            let _ = client.sismember(&set_key, member.as_bytes()).await;
        }
        "SCARD" => {
            let set_key = format!("set_{}", rng.gen_range(0..dataset_size));
            client.scard(&set_key).await?;
        }
        "SPOP" => {
            let set_key = format!("set_{}", rng.gen_range(0..dataset_size));
            let _ = client.spop(&set_key, None).await;
        }
        "SRANDMEMBER" => {
            let set_key = format!("set_{}", rng.gen_range(0..dataset_size));
            let _ = client.srandmember(&set_key, None).await;
        }

        // SORTED SET OPERATIONS
        "ZADD" => {
            let zset_key = format!("zset_{}", rng.gen_range(0..dataset_size));
            let member = format!("member_{}", rng.gen_range(0..1000));
            let score: f64 = rng.gen_range(0.0..1000.0);
            client.zadd(&zset_key, &[(score, member.as_bytes())]).await?;
        }
        "ZREM" => {
            let zset_key = format!("zset_{}", rng.gen_range(0..dataset_size));
            let member = format!("member_{}", rng.gen_range(0..1000));
            let _ = client.zrem(&zset_key, &[member.as_bytes()]).await;
        }
        "ZRANGE" => {
            let zset_key = format!("zset_{}", rng.gen_range(0..dataset_size));
            let _ = client.zrange(&zset_key, 0, 99).await;
        }
        "ZRANGEBYSCORE" => {
            let zset_key = format!("zset_{}", rng.gen_range(0..dataset_size));
            let _ = client.zrangebyscore(&zset_key, 100.0, 200.0).await;
        }
        "ZSCORE" => {
            let zset_key = format!("zset_{}", rng.gen_range(0..dataset_size));
            let member = format!("member_{}", rng.gen_range(0..1000));
            let _ = client.zscore(&zset_key, member.as_bytes()).await;
        }
        "ZRANK" => {
            let zset_key = format!("zset_{}", rng.gen_range(0..dataset_size));
            let member = format!("member_{}", rng.gen_range(0..1000));
            let _ = client.zrank(&zset_key, member.as_bytes()).await;
        }
        "ZCARD" => {
            let zset_key = format!("zset_{}", rng.gen_range(0..dataset_size));
            client.zcard(&zset_key).await?;
        }
        "ZCOUNT" => {
            let zset_key = format!("zset_{}", rng.gen_range(0..dataset_size));
            let _ = client.zcount(&zset_key, 100.0, 200.0).await;
        }

        // STREAM OPERATIONS
        "XADD" => {
            let stream_key = format!("stream_{}", rng.gen_range(0..dataset_size));
            let _ = client.xadd(&stream_key, "*", &[("data", &value[..])]).await;
        }
        "XLEN" => {
            let stream_key = format!("stream_{}", rng.gen_range(0..dataset_size));
            client.xlen(&stream_key).await?;
        }
        "XRANGE" => {
            let stream_key = format!("stream_{}", rng.gen_range(0..dataset_size));
            let _ = client.xrange(&stream_key, "-", "+").await;
        }
        "XREVRANGE" => {
            let stream_key = format!("stream_{}", rng.gen_range(0..dataset_size));
            let _ = client.xrevrange(&stream_key, "+", "-").await;
        }
        "XREAD" => {
            let stream_key = format!("stream_{}", rng.gen_range(0..dataset_size));
            let _ = client.xread(&[stream_key.as_str()], &["0"]).await;
        }
        "XDEL" => {
            let stream_key = format!("stream_{}", rng.gen_range(0..dataset_size));
            let _ = client.xdel(&stream_key, &["0"]).await;
        }
        "XTRIM" => {
            let stream_key = format!("stream_{}", rng.gen_range(0..dataset_size));
            let _ = client.xtrim(&stream_key, 500).await;
        }

        // KEY OPERATIONS
        "DEL" => {
            let del_key = format!("key_{}", rng.gen_range(0..dataset_size));
            let _ = client.del(&[del_key.as_str()]).await;
        }
        "EXISTS" => {
            let exists_key = format!("key_{}", rng.gen_range(0..dataset_size));
            let _ = client.exists(&[exists_key.as_str()]).await;
        }
        "TYPE" => {
            let type_key = format!("key_{}", rng.gen_range(0..dataset_size));
            let _ = client.key_type(&type_key).await;
        }
        "EXPIRE" => {
            let expire_key = format!("key_{}", rng.gen_range(0..dataset_size));
            let _ = client.expire(&expire_key, 10).await;
        }
        "TTL" => {
            let ttl_key = format!("key_{}", rng.gen_range(0..dataset_size));
            let _ = client.ttl(&ttl_key).await;
        }

        // REDLITE-SPECIFIC OPERATIONS
        "HISTORY ENABLE" => {
            client.history_enable(&key).await?;
        }
        "HISTORY GET" => {
            let _ = client.history_get(&key).await;
        }
        "KEYINFO" => {
            let _ = client.keyinfo(&key).await;
        }
        "VACUUM" => {
            client.vacuum().await?;
        }

        _ => {
            return Err(BenchError::Configuration(format!(
                "Unknown operation: {}",
                op_upper
            )));
        }
    }

    Ok(t_start.elapsed().as_secs_f64() * 1_000_000.0)
}
