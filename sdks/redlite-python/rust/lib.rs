//! Python bindings for Redlite using PyO3.
//!
//! This module provides direct Rust -> Python bindings via PyO3,
//! eliminating the need for C FFI and CFFI intermediate layers.

use pyo3::prelude::*;
use pyo3::types::PyBytes;
use redlite::Db as RedliteDb;
use std::time::Duration;

/// Error conversion helper
fn to_py_err<E: std::fmt::Display>(e: E) -> PyErr {
    PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string())
}

/// Set options for SET command
#[pyclass]
#[derive(Clone, Default)]
pub struct SetOptions {
    /// Expiration in seconds
    #[pyo3(get, set)]
    pub ex: Option<i64>,
    /// Expiration in milliseconds
    #[pyo3(get, set)]
    pub px: Option<i64>,
    /// Only set if key does not exist
    #[pyo3(get, set)]
    pub nx: Option<bool>,
    /// Only set if key exists
    #[pyo3(get, set)]
    pub xx: Option<bool>,
}

#[pymethods]
impl SetOptions {
    #[new]
    #[pyo3(signature = (ex=None, px=None, nx=None, xx=None))]
    fn new(ex: Option<i64>, px: Option<i64>, nx: Option<bool>, xx: Option<bool>) -> Self {
        Self { ex, px, nx, xx }
    }
}

/// Sorted set member with score
#[pyclass]
#[derive(Clone)]
pub struct ZMember {
    #[pyo3(get, set)]
    pub score: f64,
    #[pyo3(get, set)]
    pub member: Vec<u8>,
}

#[pymethods]
impl ZMember {
    #[new]
    fn new(score: f64, member: Vec<u8>) -> Self {
        Self { score, member }
    }
}

/// Redlite embedded database - Redis API with SQLite durability.
///
/// This class provides direct access to the Redlite database without
/// network overhead. Use Redlite class in client.py for unified API
/// that supports both embedded and server modes.
#[pyclass]
pub struct EmbeddedDb {
    inner: RedliteDb,
}

#[pymethods]
impl EmbeddedDb {
    /// Open a database at the given path.
    /// Use ":memory:" for an in-memory database.
    #[new]
    fn new(path: &str) -> PyResult<Self> {
        RedliteDb::open(path)
            .map(|db| Self { inner: db })
            .map_err(to_py_err)
    }

    /// Open an in-memory database.
    #[staticmethod]
    fn open_memory() -> PyResult<Self> {
        RedliteDb::open_memory()
            .map(|db| Self { inner: db })
            .map_err(to_py_err)
    }

    /// Open a database with custom cache size in MB.
    #[staticmethod]
    fn open_with_cache(path: &str, cache_mb: i64) -> PyResult<Self> {
        RedliteDb::open_with_cache(path, cache_mb)
            .map(|db| Self { inner: db })
            .map_err(to_py_err)
    }

    // =========================================================================
    // String Commands
    // =========================================================================

    /// Get the value of a key.
    fn get<'py>(&self, py: Python<'py>, key: &str) -> PyResult<Option<Py<PyBytes>>> {
        self.inner
            .get(key)
            .map(|opt| opt.map(|v| PyBytes::new_bound(py, &v).unbind()))
            .map_err(to_py_err)
    }

    /// Set a key-value pair with optional TTL in seconds.
    #[pyo3(signature = (key, value, ttl_seconds=None))]
    fn set(&self, key: &str, value: &[u8], ttl_seconds: Option<i64>) -> PyResult<bool> {
        let ttl = ttl_seconds.map(|s| Duration::from_secs(s as u64));
        self.inner
            .set(key, value, ttl)
            .map(|_| true)
            .map_err(to_py_err)
    }

    /// Set a key-value pair with options (NX, XX, EX, PX).
    #[pyo3(signature = (key, value, options=None))]
    fn set_opts(&self, key: &str, value: &[u8], options: Option<SetOptions>) -> PyResult<bool> {
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
                self.inner.set_opts(key, value, opts).map_err(to_py_err)
            }
            None => self
                .inner
                .set(key, value, None)
                .map(|_| true)
                .map_err(to_py_err),
        }
    }

    /// Set key with expiration in seconds.
    fn setex(&self, key: &str, seconds: i64, value: &[u8]) -> PyResult<bool> {
        self.inner
            .setex(key, seconds, value)
            .map(|_| true)
            .map_err(to_py_err)
    }

    /// Set key with expiration in milliseconds.
    fn psetex(&self, key: &str, milliseconds: i64, value: &[u8]) -> PyResult<bool> {
        self.inner
            .psetex(key, milliseconds, value)
            .map(|_| true)
            .map_err(to_py_err)
    }

    /// Get and delete a key.
    fn getdel<'py>(&self, py: Python<'py>, key: &str) -> PyResult<Option<Py<PyBytes>>> {
        self.inner
            .getdel(key)
            .map(|opt| opt.map(|v| PyBytes::new_bound(py, &v).unbind()))
            .map_err(to_py_err)
    }

    /// Append value to a key, return new length.
    fn append(&self, key: &str, value: &[u8]) -> PyResult<i64> {
        self.inner
            .append(key, value)
            .map(|n| n as i64)
            .map_err(to_py_err)
    }

    /// Get the length of the value stored at key.
    fn strlen(&self, key: &str) -> PyResult<i64> {
        self.inner
            .strlen(key)
            .map(|n| n as i64)
            .map_err(to_py_err)
    }

    /// Get a substring of the value stored at key.
    fn getrange<'py>(&self, py: Python<'py>, key: &str, start: i64, end: i64) -> PyResult<Py<PyBytes>> {
        self.inner
            .getrange(key, start, end)
            .map(|v| PyBytes::new_bound(py, &v).unbind())
            .map_err(to_py_err)
    }

    /// Overwrite part of a string at key starting at offset.
    fn setrange(&self, key: &str, offset: i64, value: &[u8]) -> PyResult<i64> {
        self.inner.setrange(key, offset, value).map_err(to_py_err)
    }

    /// Increment the integer value of a key by one.
    fn incr(&self, key: &str) -> PyResult<i64> {
        self.inner.incr(key).map_err(to_py_err)
    }

    /// Decrement the integer value of a key by one.
    fn decr(&self, key: &str) -> PyResult<i64> {
        self.inner.decr(key).map_err(to_py_err)
    }

    /// Increment the integer value of a key by amount.
    fn incrby(&self, key: &str, increment: i64) -> PyResult<i64> {
        self.inner.incrby(key, increment).map_err(to_py_err)
    }

    /// Decrement the integer value of a key by amount.
    fn decrby(&self, key: &str, decrement: i64) -> PyResult<i64> {
        self.inner.decrby(key, decrement).map_err(to_py_err)
    }

    /// Increment the float value of a key by amount.
    fn incrbyfloat(&self, key: &str, increment: f64) -> PyResult<f64> {
        self.inner
            .incrbyfloat(key, increment)
            .and_then(|s| s.parse::<f64>().map_err(|_| redlite::KvError::NotFloat))
            .map_err(to_py_err)
    }

    // =========================================================================
    // Key Commands
    // =========================================================================

    /// Delete one or more keys, return count deleted.
    fn delete(&self, keys: Vec<String>) -> PyResult<i64> {
        let key_refs: Vec<&str> = keys.iter().map(|s| s.as_str()).collect();
        self.inner
            .del(&key_refs)
            .map(|n| n as i64)
            .map_err(to_py_err)
    }

    /// Check if keys exist, return count of existing keys.
    fn exists(&self, keys: Vec<String>) -> PyResult<i64> {
        let key_refs: Vec<&str> = keys.iter().map(|s| s.as_str()).collect();
        self.inner
            .exists(&key_refs)
            .map(|n| n as i64)
            .map_err(to_py_err)
    }

    /// Get the type of a key.
    fn key_type(&self, key: &str) -> PyResult<String> {
        self.inner
            .key_type(key)
            .map(|opt| match opt {
                Some(redlite::KeyType::String) => "string".to_string(),
                Some(redlite::KeyType::List) => "list".to_string(),
                Some(redlite::KeyType::Set) => "set".to_string(),
                Some(redlite::KeyType::Hash) => "hash".to_string(),
                Some(redlite::KeyType::ZSet) => "zset".to_string(),
                Some(redlite::KeyType::Stream) => "stream".to_string(),
                None => "none".to_string(),
            })
            .map_err(to_py_err)
    }

    /// Get TTL in seconds.
    fn ttl(&self, key: &str) -> PyResult<i64> {
        self.inner.ttl(key).map_err(to_py_err)
    }

    /// Get TTL in milliseconds.
    fn pttl(&self, key: &str) -> PyResult<i64> {
        self.inner.pttl(key).map_err(to_py_err)
    }

    /// Set expiration in seconds.
    fn expire(&self, key: &str, seconds: i64) -> PyResult<bool> {
        self.inner.expire(key, seconds).map_err(to_py_err)
    }

    /// Set expiration in milliseconds.
    fn pexpire(&self, key: &str, milliseconds: i64) -> PyResult<bool> {
        self.inner.pexpire(key, milliseconds).map_err(to_py_err)
    }

    /// Set expiration at Unix timestamp (seconds).
    fn expireat(&self, key: &str, unix_time: i64) -> PyResult<bool> {
        self.inner.expireat(key, unix_time).map_err(to_py_err)
    }

    /// Set expiration at Unix timestamp (milliseconds).
    fn pexpireat(&self, key: &str, unix_time_ms: i64) -> PyResult<bool> {
        self.inner.pexpireat(key, unix_time_ms).map_err(to_py_err)
    }

    /// Remove expiration from a key.
    fn persist(&self, key: &str) -> PyResult<bool> {
        self.inner.persist(key).map_err(to_py_err)
    }

    /// Rename a key.
    fn rename(&self, key: &str, newkey: &str) -> PyResult<bool> {
        self.inner.rename(key, newkey).map(|_| true).map_err(to_py_err)
    }

    /// Rename a key only if newkey does not exist.
    fn renamenx(&self, key: &str, newkey: &str) -> PyResult<bool> {
        self.inner.renamenx(key, newkey).map_err(to_py_err)
    }

    /// Find all keys matching a pattern.
    #[pyo3(signature = (pattern="*"))]
    fn keys(&self, pattern: &str) -> PyResult<Vec<String>> {
        self.inner.keys(pattern).map_err(to_py_err)
    }

    /// Get the number of keys in the database.
    fn dbsize(&self) -> PyResult<i64> {
        self.inner.dbsize().map(|n| n as i64).map_err(to_py_err)
    }

    /// Delete all keys in the current database.
    fn flushdb(&self) -> PyResult<bool> {
        self.inner.flushdb().map(|_| true).map_err(to_py_err)
    }

    /// Select a database by index.
    fn select(&mut self, db: i32) -> PyResult<bool> {
        self.inner.select(db).map(|_| true).map_err(to_py_err)
    }

    // =========================================================================
    // Hash Commands
    // =========================================================================

    /// Set a single hash field.
    fn hset(&self, key: &str, field: &str, value: &[u8]) -> PyResult<i64> {
        let pairs: Vec<(&str, &[u8])> = vec![(field, value)];
        self.inner.hset(key, &pairs).map_err(to_py_err)
    }

    /// Set multiple hash fields.
    fn hmset(&self, key: &str, mapping: Vec<(String, Vec<u8>)>) -> PyResult<i64> {
        let pairs: Vec<(&str, &[u8])> = mapping
            .iter()
            .map(|(f, v)| (f.as_str(), v.as_slice()))
            .collect();
        self.inner.hset(key, &pairs).map_err(to_py_err)
    }

    /// Get a hash field value.
    fn hget<'py>(&self, py: Python<'py>, key: &str, field: &str) -> PyResult<Option<Py<PyBytes>>> {
        self.inner
            .hget(key, field)
            .map(|opt| opt.map(|v| PyBytes::new_bound(py, &v).unbind()))
            .map_err(to_py_err)
    }

    /// Delete hash fields.
    fn hdel(&self, key: &str, fields: Vec<String>) -> PyResult<i64> {
        let field_refs: Vec<&str> = fields.iter().map(|s| s.as_str()).collect();
        self.inner
            .hdel(key, &field_refs)
            .map(|n| n as i64)
            .map_err(to_py_err)
    }

    /// Check if hash field exists.
    fn hexists(&self, key: &str, field: &str) -> PyResult<bool> {
        self.inner.hexists(key, field).map_err(to_py_err)
    }

    /// Get number of fields in a hash.
    fn hlen(&self, key: &str) -> PyResult<i64> {
        self.inner.hlen(key).map(|n| n as i64).map_err(to_py_err)
    }

    /// Get all field names in a hash.
    fn hkeys(&self, key: &str) -> PyResult<Vec<String>> {
        self.inner.hkeys(key).map_err(to_py_err)
    }

    /// Get all values in a hash.
    fn hvals<'py>(&self, py: Python<'py>, key: &str) -> PyResult<Vec<Py<PyBytes>>> {
        self.inner
            .hvals(key)
            .map(|v| v.into_iter().map(|b| PyBytes::new_bound(py, &b).unbind()).collect())
            .map_err(to_py_err)
    }

    /// Increment hash field by integer.
    fn hincrby(&self, key: &str, field: &str, increment: i64) -> PyResult<i64> {
        self.inner.hincrby(key, field, increment).map_err(to_py_err)
    }

    /// Get all fields and values in a hash.
    fn hgetall<'py>(&self, py: Python<'py>, key: &str) -> PyResult<Vec<(String, Py<PyBytes>)>> {
        self.inner
            .hgetall(key)
            .map(|pairs| {
                pairs
                    .into_iter()
                    .map(|(field, value)| (field, PyBytes::new_bound(py, &value).unbind()))
                    .collect()
            })
            .map_err(to_py_err)
    }

    /// Get values of multiple hash fields.
    fn hmget<'py>(&self, py: Python<'py>, key: &str, fields: Vec<String>) -> PyResult<Vec<Option<Py<PyBytes>>>> {
        let field_refs: Vec<&str> = fields.iter().map(|s| s.as_str()).collect();
        self.inner
            .hmget(key, &field_refs)
            .map(|results| {
                results
                    .into_iter()
                    .map(|opt| opt.map(|v| PyBytes::new_bound(py, &v).unbind()))
                    .collect()
            })
            .map_err(to_py_err)
    }

    // =========================================================================
    // List Commands
    // =========================================================================

    /// Push values to the left of a list.
    fn lpush(&self, key: &str, values: Vec<Vec<u8>>) -> PyResult<i64> {
        let value_refs: Vec<&[u8]> = values.iter().map(|v| v.as_slice()).collect();
        self.inner
            .lpush(key, &value_refs)
            .map(|n| n as i64)
            .map_err(to_py_err)
    }

    /// Push values to the right of a list.
    fn rpush(&self, key: &str, values: Vec<Vec<u8>>) -> PyResult<i64> {
        let value_refs: Vec<&[u8]> = values.iter().map(|v| v.as_slice()).collect();
        self.inner
            .rpush(key, &value_refs)
            .map(|n| n as i64)
            .map_err(to_py_err)
    }

    /// Pop values from the left of a list.
    #[pyo3(signature = (key, count=None))]
    fn lpop<'py>(&self, py: Python<'py>, key: &str, count: Option<i64>) -> PyResult<Vec<Py<PyBytes>>> {
        let cnt = count.map(|c| c as usize);
        self.inner
            .lpop(key, cnt)
            .map(|v| v.into_iter().map(|b| PyBytes::new_bound(py, &b).unbind()).collect())
            .map_err(to_py_err)
    }

    /// Pop values from the right of a list.
    #[pyo3(signature = (key, count=None))]
    fn rpop<'py>(&self, py: Python<'py>, key: &str, count: Option<i64>) -> PyResult<Vec<Py<PyBytes>>> {
        let cnt = count.map(|c| c as usize);
        self.inner
            .rpop(key, cnt)
            .map(|v| v.into_iter().map(|b| PyBytes::new_bound(py, &b).unbind()).collect())
            .map_err(to_py_err)
    }

    /// Get list length.
    fn llen(&self, key: &str) -> PyResult<i64> {
        self.inner.llen(key).map(|n| n as i64).map_err(to_py_err)
    }

    /// Get a range of elements from a list.
    fn lrange<'py>(&self, py: Python<'py>, key: &str, start: i64, stop: i64) -> PyResult<Vec<Py<PyBytes>>> {
        self.inner
            .lrange(key, start, stop)
            .map(|v| v.into_iter().map(|b| PyBytes::new_bound(py, &b).unbind()).collect())
            .map_err(to_py_err)
    }

    /// Get element at index in a list.
    fn lindex<'py>(&self, py: Python<'py>, key: &str, index: i64) -> PyResult<Option<Py<PyBytes>>> {
        self.inner
            .lindex(key, index)
            .map(|opt| opt.map(|v| PyBytes::new_bound(py, &v).unbind()))
            .map_err(to_py_err)
    }

    // =========================================================================
    // Set Commands
    // =========================================================================

    /// Add members to a set.
    fn sadd(&self, key: &str, members: Vec<Vec<u8>>) -> PyResult<i64> {
        let member_refs: Vec<&[u8]> = members.iter().map(|m| m.as_slice()).collect();
        self.inner
            .sadd(key, &member_refs)
            .map(|n| n as i64)
            .map_err(to_py_err)
    }

    /// Remove members from a set.
    fn srem(&self, key: &str, members: Vec<Vec<u8>>) -> PyResult<i64> {
        let member_refs: Vec<&[u8]> = members.iter().map(|m| m.as_slice()).collect();
        self.inner
            .srem(key, &member_refs)
            .map(|n| n as i64)
            .map_err(to_py_err)
    }

    /// Get all members of a set.
    fn smembers<'py>(&self, py: Python<'py>, key: &str) -> PyResult<Vec<Py<PyBytes>>> {
        self.inner
            .smembers(key)
            .map(|v| v.into_iter().map(|b| PyBytes::new_bound(py, &b).unbind()).collect())
            .map_err(to_py_err)
    }

    /// Check if member is in a set.
    fn sismember(&self, key: &str, member: &[u8]) -> PyResult<bool> {
        self.inner.sismember(key, member).map_err(to_py_err)
    }

    /// Get number of members in a set.
    fn scard(&self, key: &str) -> PyResult<i64> {
        self.inner.scard(key).map(|n| n as i64).map_err(to_py_err)
    }

    // =========================================================================
    // Sorted Set Commands
    // =========================================================================

    /// Add members with scores to a sorted set.
    fn zadd(&self, key: &str, members: Vec<(f64, Vec<u8>)>) -> PyResult<i64> {
        let zmembers: Vec<redlite::ZMember> = members
            .into_iter()
            .map(|(score, member)| redlite::ZMember { score, member })
            .collect();
        self.inner
            .zadd(key, &zmembers)
            .map(|n| n as i64)
            .map_err(to_py_err)
    }

    /// Remove members from a sorted set.
    fn zrem(&self, key: &str, members: Vec<Vec<u8>>) -> PyResult<i64> {
        let member_refs: Vec<&[u8]> = members.iter().map(|m| m.as_slice()).collect();
        self.inner
            .zrem(key, &member_refs)
            .map(|n| n as i64)
            .map_err(to_py_err)
    }

    /// Get score of a member in a sorted set.
    fn zscore(&self, key: &str, member: &[u8]) -> PyResult<Option<f64>> {
        self.inner.zscore(key, member).map_err(to_py_err)
    }

    /// Get number of members in a sorted set.
    fn zcard(&self, key: &str) -> PyResult<i64> {
        self.inner.zcard(key).map(|n| n as i64).map_err(to_py_err)
    }

    /// Count members in a sorted set within a score range.
    fn zcount(&self, key: &str, min_score: f64, max_score: f64) -> PyResult<i64> {
        self.inner
            .zcount(key, min_score, max_score)
            .map(|n| n as i64)
            .map_err(to_py_err)
    }

    /// Increment score of a member in a sorted set.
    fn zincrby(&self, key: &str, increment: f64, member: &[u8]) -> PyResult<f64> {
        self.inner.zincrby(key, increment, member).map_err(to_py_err)
    }

    /// Get members by rank range (ascending order).
    #[pyo3(signature = (key, start, stop, with_scores=false))]
    fn zrange<'py>(&self, py: Python<'py>, key: &str, start: i64, stop: i64, with_scores: bool) -> PyResult<Vec<(Py<PyBytes>, f64)>> {
        self.inner
            .zrange(key, start, stop, with_scores)
            .map(|members| {
                members
                    .into_iter()
                    .map(|m| (PyBytes::new_bound(py, &m.member).unbind(), m.score))
                    .collect()
            })
            .map_err(to_py_err)
    }

    /// Get members by rank range (descending order).
    #[pyo3(signature = (key, start, stop, with_scores=false))]
    fn zrevrange<'py>(&self, py: Python<'py>, key: &str, start: i64, stop: i64, with_scores: bool) -> PyResult<Vec<(Py<PyBytes>, f64)>> {
        self.inner
            .zrevrange(key, start, stop, with_scores)
            .map(|members| {
                members
                    .into_iter()
                    .map(|m| (PyBytes::new_bound(py, &m.member).unbind(), m.score))
                    .collect()
            })
            .map_err(to_py_err)
    }

    // =========================================================================
    // Multi-key Commands
    // =========================================================================

    /// Get values of multiple keys.
    fn mget<'py>(&self, py: Python<'py>, keys: Vec<String>) -> PyResult<Vec<Option<Py<PyBytes>>>> {
        let key_refs: Vec<&str> = keys.iter().map(|s| s.as_str()).collect();
        let results = self.inner.mget(&key_refs);
        Ok(results
            .into_iter()
            .map(|opt| opt.map(|v| PyBytes::new_bound(py, &v).unbind()))
            .collect())
    }

    /// Set multiple key-value pairs atomically.
    fn mset(&self, pairs: Vec<(String, Vec<u8>)>) -> PyResult<bool> {
        let pair_refs: Vec<(&str, &[u8])> = pairs
            .iter()
            .map(|(k, v)| (k.as_str(), v.as_slice()))
            .collect();
        self.inner
            .mset(&pair_refs)
            .map(|_| true)
            .map_err(to_py_err)
    }

    // =========================================================================
    // Scan Commands
    // =========================================================================

    /// Incrementally iterate keys matching a pattern.
    #[pyo3(signature = (cursor, pattern=None, count=10))]
    fn scan(&self, cursor: &str, pattern: Option<&str>, count: usize) -> PyResult<(String, Vec<String>)> {
        self.inner
            .scan(cursor, pattern, count)
            .map_err(to_py_err)
    }

    /// Incrementally iterate hash fields.
    #[pyo3(signature = (key, cursor, pattern=None, count=10))]
    fn hscan<'py>(&self, py: Python<'py>, key: &str, cursor: &str, pattern: Option<&str>, count: usize) -> PyResult<(String, Vec<(String, Py<PyBytes>)>)> {
        self.inner
            .hscan(key, cursor, pattern, count)
            .map(|(next_cursor, items)| {
                let py_items: Vec<(String, Py<PyBytes>)> = items
                    .into_iter()
                    .map(|(field, value)| (field, PyBytes::new_bound(py, &value).unbind()))
                    .collect();
                (next_cursor, py_items)
            })
            .map_err(to_py_err)
    }

    /// Incrementally iterate set members.
    #[pyo3(signature = (key, cursor, pattern=None, count=10))]
    fn sscan<'py>(&self, py: Python<'py>, key: &str, cursor: &str, pattern: Option<&str>, count: usize) -> PyResult<(String, Vec<Py<PyBytes>>)> {
        self.inner
            .sscan(key, cursor, pattern, count)
            .map(|(next_cursor, members)| {
                let py_members: Vec<Py<PyBytes>> = members
                    .into_iter()
                    .map(|m| PyBytes::new_bound(py, &m).unbind())
                    .collect();
                (next_cursor, py_members)
            })
            .map_err(to_py_err)
    }

    /// Incrementally iterate sorted set members with scores.
    #[pyo3(signature = (key, cursor, pattern=None, count=10))]
    fn zscan<'py>(&self, py: Python<'py>, key: &str, cursor: &str, pattern: Option<&str>, count: usize) -> PyResult<(String, Vec<(Py<PyBytes>, f64)>)> {
        self.inner
            .zscan(key, cursor, pattern, count)
            .map(|(next_cursor, members)| {
                let py_members: Vec<(Py<PyBytes>, f64)> = members
                    .into_iter()
                    .map(|(member, score)| (PyBytes::new_bound(py, &member).unbind(), score))
                    .collect();
                (next_cursor, py_members)
            })
            .map_err(to_py_err)
    }

    // =========================================================================
    // Server Commands
    // =========================================================================

    /// Run SQLite VACUUM to reclaim space.
    fn vacuum(&self) -> PyResult<i64> {
        self.inner.vacuum().map(|n| n as i64).map_err(to_py_err)
    }
}

/// Python module definition
#[pymodule]
fn _native(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<EmbeddedDb>()?;
    m.add_class::<SetOptions>()?;
    m.add_class::<ZMember>()?;
    Ok(())
}
