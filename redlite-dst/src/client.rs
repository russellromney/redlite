use anyhow::Result;
use redlite::{Db, ZMember};
use std::collections::HashMap;
use std::path::PathBuf;

/// Wrapper around actual Redlite database for testing
pub struct RedliteClient {
    db: Db,
    path: Option<PathBuf>,
}

impl RedliteClient {
    pub fn new_memory() -> Result<Self> {
        let db = Db::open_memory()?;
        Ok(Self { db, path: None })
    }

    pub fn new_file(path: PathBuf) -> Result<Self> {
        let db = Db::open(path.to_str().unwrap())?;
        Ok(Self {
            db,
            path: Some(path),
        })
    }

    // String operations
    pub fn set(&mut self, key: &str, value: Vec<u8>) -> Result<()> {
        self.db.set(key, &value, None)?;
        Ok(())
    }

    pub fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
        Ok(self.db.get(key)?)
    }

    pub fn incr(&mut self, key: &str) -> Result<i64> {
        Ok(self.db.incr(key)?)
    }

    pub fn decr(&mut self, key: &str) -> Result<i64> {
        Ok(self.db.decr(key)?)
    }

    pub fn append(&mut self, key: &str, value: Vec<u8>) -> Result<usize> {
        Ok(self.db.append(key, &value)? as usize)
    }

    // List operations
    pub fn lpush(&mut self, key: &str, value: Vec<u8>) -> Result<usize> {
        Ok(self.db.lpush(key, &[&value])? as usize)
    }

    pub fn rpush(&mut self, key: &str, value: Vec<u8>) -> Result<usize> {
        Ok(self.db.rpush(key, &[&value])? as usize)
    }

    pub fn lpop(&mut self, key: &str) -> Result<Option<Vec<u8>>> {
        let result = self.db.lpop(key, None)?;
        Ok(result.into_iter().next())
    }

    pub fn rpop(&mut self, key: &str) -> Result<Option<Vec<u8>>> {
        let result = self.db.rpop(key, None)?;
        Ok(result.into_iter().next())
    }

    pub fn lrange(&self, key: &str, start: isize, stop: isize) -> Result<Vec<Vec<u8>>> {
        Ok(self.db.lrange(key, start as i64, stop as i64)?)
    }

    // Hash operations
    pub fn hset(&mut self, key: &str, field: &str, value: Vec<u8>) -> Result<usize> {
        Ok(self.db.hset(key, &[(field, &value[..])])? as usize)
    }

    pub fn hget(&self, key: &str, field: &str) -> Result<Option<Vec<u8>>> {
        Ok(self.db.hget(key, field)?)
    }

    pub fn hgetall(&self, key: &str) -> Result<HashMap<String, Vec<u8>>> {
        let pairs = self.db.hgetall(key)?;
        Ok(pairs.into_iter().collect())
    }

    pub fn hdel(&mut self, key: &str, field: &str) -> Result<usize> {
        Ok(self.db.hdel(key, &[field])? as usize)
    }

    // Set operations
    pub fn sadd(&mut self, key: &str, value: Vec<u8>) -> Result<usize> {
        Ok(self.db.sadd(key, &[&value])? as usize)
    }

    pub fn srem(&mut self, key: &str, value: &[u8]) -> Result<usize> {
        Ok(self.db.srem(key, &[value])? as usize)
    }

    pub fn smembers(&self, key: &str) -> Result<Vec<Vec<u8>>> {
        Ok(self.db.smembers(key)?)
    }

    pub fn sismember(&self, key: &str, value: &[u8]) -> Result<bool> {
        Ok(self.db.sismember(key, value)?)
    }

    // Sorted set operations
    pub fn zadd(&mut self, key: &str, score: f64, value: Vec<u8>) -> Result<usize> {
        Ok(self.db.zadd(key, &[ZMember { score, member: value }])? as usize)
    }

    pub fn zrange(&self, key: &str, start: isize, stop: isize) -> Result<Vec<Vec<u8>>> {
        let members = self.db.zrange(key, start as i64, stop as i64, false)?;
        Ok(members.into_iter().map(|m| m.member).collect())
    }

    pub fn zscore(&self, key: &str, value: &[u8]) -> Result<Option<f64>> {
        Ok(self.db.zscore(key, value)?)
    }

    // Expiration
    pub fn expire(&mut self, key: &str, seconds: u64) -> Result<bool> {
        Ok(self.db.expire(key, seconds as i64)?)
    }

    pub fn ttl(&self, key: &str) -> Result<i64> {
        Ok(self.db.ttl(key)?)
    }

    // Cleanup
    pub fn flush(&mut self) -> Result<()> {
        self.db.flushdb()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_set_get() {
        let mut client = RedliteClient::new_memory().unwrap();
        client.set("key", b"value".to_vec()).unwrap();
        assert_eq!(client.get("key").unwrap(), Some(b"value".to_vec()));
    }

    #[test]
    fn test_incr() {
        let mut client = RedliteClient::new_memory().unwrap();
        assert_eq!(client.incr("counter").unwrap(), 1);
        assert_eq!(client.incr("counter").unwrap(), 2);
        assert_eq!(client.incr("counter").unwrap(), 3);
    }

    #[test]
    fn test_list_ops() {
        let mut client = RedliteClient::new_memory().unwrap();
        client.lpush("list", b"a".to_vec()).unwrap();
        client.rpush("list", b"b".to_vec()).unwrap();
        assert_eq!(client.lpop("list").unwrap(), Some(b"a".to_vec()));
        assert_eq!(client.rpop("list").unwrap(), Some(b"b".to_vec()));
    }
}
