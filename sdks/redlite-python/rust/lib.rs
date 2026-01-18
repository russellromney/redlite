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

/// Stream ID (ms-seq format)
#[pyclass]
#[derive(Clone)]
pub struct StreamId {
    #[pyo3(get, set)]
    pub ms: i64,
    #[pyo3(get, set)]
    pub seq: i64,
}

#[pymethods]
impl StreamId {
    #[new]
    fn new(ms: i64, seq: i64) -> Self {
        Self { ms, seq }
    }

    fn __str__(&self) -> String {
        format!("{}-{}", self.ms, self.seq)
    }
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
                Some(redlite::KeyType::Json) => "ReJSON-RL".to_string(),
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

    /// Push values to the left of a list, only if key exists.
    fn lpushx(&self, key: &str, values: Vec<Vec<u8>>) -> PyResult<i64> {
        let value_refs: Vec<&[u8]> = values.iter().map(|v| v.as_slice()).collect();
        self.inner
            .lpushx(key, &value_refs)
            .map(|n| n as i64)
            .map_err(to_py_err)
    }

    /// Push values to the right of a list, only if key exists.
    fn rpushx(&self, key: &str, values: Vec<Vec<u8>>) -> PyResult<i64> {
        let value_refs: Vec<&[u8]> = values.iter().map(|v| v.as_slice()).collect();
        self.inner
            .rpushx(key, &value_refs)
            .map(|n| n as i64)
            .map_err(to_py_err)
    }

    /// Move element between lists atomically.
    fn lmove<'py>(
        &self,
        py: Python<'py>,
        source: &str,
        destination: &str,
        wherefrom: &str,
        whereto: &str,
    ) -> PyResult<Option<Py<PyBytes>>> {
        let from_dir = match wherefrom.to_uppercase().as_str() {
            "LEFT" => redlite::ListDirection::Left,
            "RIGHT" => redlite::ListDirection::Right,
            _ => return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                "wherefrom must be 'LEFT' or 'RIGHT'"
            )),
        };
        let to_dir = match whereto.to_uppercase().as_str() {
            "LEFT" => redlite::ListDirection::Left,
            "RIGHT" => redlite::ListDirection::Right,
            _ => return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                "whereto must be 'LEFT' or 'RIGHT'"
            )),
        };
        self.inner
            .lmove(source, destination, from_dir, to_dir)
            .map(|opt| opt.map(|v| PyBytes::new_bound(py, &v).unbind()))
            .map_err(to_py_err)
    }

    /// Find positions of element in list.
    #[pyo3(signature = (key, element, rank=None, count=None, maxlen=None))]
    fn lpos(
        &self,
        key: &str,
        element: &[u8],
        rank: Option<i64>,
        count: Option<usize>,
        maxlen: Option<usize>,
    ) -> PyResult<Vec<i64>> {
        self.inner
            .lpos(key, element, rank, count, maxlen)
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

    /// Intersect sorted sets and store result.
    #[pyo3(signature = (destination, keys, weights=None, aggregate=None))]
    fn zinterstore(
        &self,
        destination: &str,
        keys: Vec<String>,
        weights: Option<Vec<f64>>,
        aggregate: Option<String>,
    ) -> PyResult<i64> {
        let key_refs: Vec<&str> = keys.iter().map(|s| s.as_str()).collect();
        let weights_ref = weights.as_ref().map(|w| w.as_slice());
        let aggregate_ref = aggregate.as_ref().map(|s| s.as_str());
        self.inner
            .zinterstore(destination, &key_refs, weights_ref, aggregate_ref)
            .map(|n| n as i64)
            .map_err(to_py_err)
    }

    /// Union sorted sets and store result.
    #[pyo3(signature = (destination, keys, weights=None, aggregate=None))]
    fn zunionstore(
        &self,
        destination: &str,
        keys: Vec<String>,
        weights: Option<Vec<f64>>,
        aggregate: Option<String>,
    ) -> PyResult<i64> {
        let key_refs: Vec<&str> = keys.iter().map(|s| s.as_str()).collect();
        let weights_ref = weights.as_ref().map(|w| w.as_slice());
        let aggregate_ref = aggregate.as_ref().map(|s| s.as_str());
        self.inner
            .zunionstore(destination, &key_refs, weights_ref, aggregate_ref)
            .map(|n| n as i64)
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
    // Stream Commands
    // =========================================================================

    /// Set consumer group last delivered ID.
    fn xgroup_setid(&self, key: &str, group: &str, id: &StreamId) -> PyResult<bool> {
        self.inner
            .xgroup_setid(key, group, id.into())
            .map_err(to_py_err)
    }

    /// Create consumer in group.
    fn xgroup_createconsumer(&self, key: &str, group: &str, consumer: &str) -> PyResult<bool> {
        self.inner
            .xgroup_createconsumer(key, group, consumer)
            .map_err(to_py_err)
    }

    /// Delete consumer from group.
    fn xgroup_delconsumer(&self, key: &str, group: &str, consumer: &str) -> PyResult<i64> {
        self.inner
            .xgroup_delconsumer(key, group, consumer)
            .map(|n| n as i64)
            .map_err(to_py_err)
    }

    // Note: xclaim, xinfo_stream, xinfo_groups, xinfo_consumers require more complex types
    // These will be added in a future update when stream types are fully implemented

    // =========================================================================
    // History Commands
    // =========================================================================

    /// Query historical entries with filters.
    #[pyo3(signature = (key, limit=None, since=None, until=None))]
    fn history_get<'py>(
        &self,
        py: Python<'py>,
        key: &str,
        limit: Option<i64>,
        since: Option<i64>,
        until: Option<i64>,
    ) -> PyResult<Vec<(i64, Option<Py<PyBytes>>)>> {
        self.inner
            .history_get(key, limit, since, until)
            .map(|entries| {
                entries
                    .into_iter()
                    .map(|e| (e.timestamp_ms, e.data_snapshot.map(|v| PyBytes::new_bound(py, &v).unbind())))
                    .collect()
            })
            .map_err(to_py_err)
    }

    /// Time-travel query to specific timestamp.
    fn history_get_at<'py>(&self, py: Python<'py>, key: &str, timestamp: i64) -> PyResult<Option<Py<PyBytes>>> {
        self.inner
            .history_get_at(key, timestamp)
            .map(|opt| opt.map(|v| PyBytes::new_bound(py, &v).unbind()))
            .map_err(to_py_err)
    }

    /// List all tracked keys.
    #[pyo3(signature = (pattern=None))]
    fn history_list_keys(&self, pattern: Option<&str>) -> PyResult<Vec<String>> {
        self.inner.history_list_keys(pattern).map_err(to_py_err)
    }

    /// Get history tracking statistics.
    #[pyo3(signature = (key=None))]
    fn history_stats(&self, key: Option<&str>) -> PyResult<(i64, Option<i64>, Option<i64>, i64)> {
        self.inner
            .history_stats(key)
            .map(|stats| (stats.total_entries, stats.oldest_timestamp, stats.newest_timestamp, stats.storage_bytes))
            .map_err(to_py_err)
    }

    /// Clear history for a key.
    #[pyo3(signature = (key, before=None))]
    fn history_clear(&self, key: &str, before: Option<i64>) -> PyResult<i64> {
        self.inner
            .history_clear_key(key, before)
            .map_err(to_py_err)
    }

    /// Prune old history entries globally.
    fn history_prune(&self, before_timestamp: i64) -> PyResult<i64> {
        self.inner
            .history_prune(before_timestamp)
            .map_err(to_py_err)
    }

    // =========================================================================
    // Geospatial Commands (requires geo feature)
    // =========================================================================

    /// Add geospatial items with coordinates.
    #[pyo3(signature = (key, items, nx=false, xx=false, ch=false))]
    fn geoadd(&self, key: &str, items: Vec<(f64, f64, String)>, nx: bool, xx: bool, ch: bool) -> PyResult<i64> {
        let geo_items: Vec<(f64, f64, &str)> = items
            .iter()
            .map(|(lon, lat, member)| (*lon, *lat, member.as_str()))
            .collect();
        self.inner
            .geoadd(key, &geo_items, nx, xx, ch)
            .map_err(to_py_err)
    }

    /// Get coordinates of members.
    fn geopos(&self, key: &str, members: Vec<String>) -> PyResult<Vec<Option<(f64, f64)>>> {
        let member_refs: Vec<&str> = members.iter().map(|m| m.as_str()).collect();
        self.inner.geopos(key, &member_refs).map_err(to_py_err)
    }

    /// Calculate distance between members.
    #[pyo3(signature = (key, member1, member2, unit=None))]
    fn geodist(&self, key: &str, member1: &str, member2: &str, unit: Option<&str>) -> PyResult<Option<f64>> {
        let geo_unit = match unit {
            Some("m") | None => redlite::types::GeoUnit::Meters,
            Some("km") => redlite::types::GeoUnit::Kilometers,
            Some("mi") => redlite::types::GeoUnit::Miles,
            Some("ft") => redlite::types::GeoUnit::Feet,
            Some(u) => return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                format!("Invalid unit '{}'. Use 'm', 'km', 'mi', or 'ft'", u)
            )),
        };
        self.inner
            .geodist(key, member1, member2, geo_unit)
            .map_err(to_py_err)
    }

    /// Get geohash strings for members.
    fn geohash(&self, key: &str, members: Vec<String>) -> PyResult<Vec<Option<String>>> {
        let member_refs: Vec<&str> = members.iter().map(|m| m.as_str()).collect();
        self.inner.geohash(key, &member_refs).map_err(to_py_err)
    }

    // Note: geosearch and geosearchstore require more complex GeoSearchOpts types
    // These will be added when geo types are fully implemented

    // =========================================================================
    // JSON Commands
    // =========================================================================

    /// JSON.SET key path value [NX] [XX]
    /// Set a JSON value at path. Returns True on success.
    #[pyo3(signature = (key, path, value, nx=false, xx=false))]
    fn json_set(&self, key: &str, path: &str, value: &str, nx: bool, xx: bool) -> PyResult<bool> {
        self.inner
            .json_set(key, path, value, nx, xx)
            .map_err(to_py_err)
    }

    /// JSON.GET key [path...]
    /// Get JSON value at path(s). Returns JSON string or None.
    #[pyo3(signature = (key, paths=None))]
    fn json_get(&self, key: &str, paths: Option<Vec<String>>) -> PyResult<Option<String>> {
        let path_vec: Vec<&str> = match &paths {
            Some(p) => p.iter().map(|s| s.as_str()).collect(),
            None => vec!["$"],
        };
        self.inner
            .json_get(key, &path_vec)
            .map_err(to_py_err)
    }

    /// JSON.DEL key [path]
    /// Delete JSON value at path. Returns number of values deleted.
    #[pyo3(signature = (key, path=None))]
    fn json_del(&self, key: &str, path: Option<&str>) -> PyResult<i64> {
        self.inner
            .json_del(key, path)
            .map_err(to_py_err)
    }

    /// JSON.TYPE key [path]
    /// Get the type of JSON value at path.
    #[pyo3(signature = (key, path=None))]
    fn json_type(&self, key: &str, path: Option<&str>) -> PyResult<Option<String>> {
        self.inner
            .json_type(key, path)
            .map_err(to_py_err)
    }

    /// JSON.NUMINCRBY key path increment
    /// Increment numeric value at path. Returns new value as string.
    fn json_numincrby(&self, key: &str, path: &str, increment: f64) -> PyResult<String> {
        self.inner
            .json_numincrby(key, path, increment)
            .map_err(to_py_err)
    }

    /// JSON.STRAPPEND key [path] value
    /// Append string to JSON string at path. Returns new length.
    #[pyo3(signature = (key, value, path=None))]
    fn json_strappend(&self, key: &str, value: &str, path: Option<&str>) -> PyResult<i64> {
        self.inner
            .json_strappend(key, path, value)
            .map_err(to_py_err)
    }

    /// JSON.STRLEN key [path]
    /// Get length of JSON string at path.
    #[pyo3(signature = (key, path=None))]
    fn json_strlen(&self, key: &str, path: Option<&str>) -> PyResult<Option<i64>> {
        self.inner
            .json_strlen(key, path)
            .map_err(to_py_err)
    }

    /// JSON.ARRAPPEND key path value [value...]
    /// Append values to JSON array. Returns new array length.
    fn json_arrappend(&self, key: &str, path: &str, values: Vec<String>) -> PyResult<i64> {
        let value_refs: Vec<&str> = values.iter().map(|s| s.as_str()).collect();
        self.inner
            .json_arrappend(key, path, &value_refs)
            .map_err(to_py_err)
    }

    /// JSON.ARRLEN key [path]
    /// Get length of JSON array at path.
    #[pyo3(signature = (key, path=None))]
    fn json_arrlen(&self, key: &str, path: Option<&str>) -> PyResult<Option<i64>> {
        self.inner
            .json_arrlen(key, path)
            .map_err(to_py_err)
    }

    /// JSON.ARRPOP key [path [index]]
    /// Pop element from JSON array. Returns the popped element.
    #[pyo3(signature = (key, path=None, index=None))]
    fn json_arrpop(&self, key: &str, path: Option<&str>, index: Option<i64>) -> PyResult<Option<String>> {
        self.inner
            .json_arrpop(key, path, index)
            .map_err(to_py_err)
    }

    /// JSON.CLEAR key [path]
    /// Clear container values (arrays/objects). Returns count of cleared values.
    #[pyo3(signature = (key, path=None))]
    fn json_clear(&self, key: &str, path: Option<&str>) -> PyResult<i64> {
        self.inner
            .json_clear(key, path)
            .map_err(to_py_err)
    }

    // =========================================================================
    // History Enable/Disable Commands
    // =========================================================================

    /// Enable history tracking globally.
    /// retention_type: "unlimited", "time:<ms>", or "count:<n>"
    #[pyo3(signature = (retention="unlimited"))]
    fn history_enable_global(&self, retention: &str) -> PyResult<()> {
        let ret = parse_retention(retention)?;
        self.inner
            .history_enable_global(ret)
            .map_err(to_py_err)
    }

    /// Enable history tracking for a specific database.
    #[pyo3(signature = (db_num, retention="unlimited"))]
    fn history_enable_database(&self, db_num: i32, retention: &str) -> PyResult<()> {
        let ret = parse_retention(retention)?;
        self.inner
            .history_enable_database(db_num, ret)
            .map_err(to_py_err)
    }

    /// Enable history tracking for a specific key.
    #[pyo3(signature = (key, retention="unlimited"))]
    fn history_enable_key(&self, key: &str, retention: &str) -> PyResult<()> {
        let ret = parse_retention(retention)?;
        self.inner
            .history_enable_key(key, ret)
            .map_err(to_py_err)
    }

    /// Disable history tracking globally.
    fn history_disable_global(&self) -> PyResult<()> {
        self.inner
            .history_disable_global()
            .map_err(to_py_err)
    }

    /// Disable history tracking for a specific database.
    fn history_disable_database(&self, db_num: i32) -> PyResult<()> {
        self.inner
            .history_disable_database(db_num)
            .map_err(to_py_err)
    }

    /// Disable history tracking for a specific key.
    fn history_disable_key(&self, key: &str) -> PyResult<()> {
        self.inner
            .history_disable_key(key)
            .map_err(to_py_err)
    }

    /// Check if history is enabled for a key.
    fn is_history_enabled(&self, key: &str) -> PyResult<bool> {
        self.inner
            .is_history_enabled(key)
            .map_err(to_py_err)
    }

    // =========================================================================
    // FTS Enable/Disable Commands
    // =========================================================================

    /// Enable full-text search indexing globally.
    fn fts_enable_global(&self) -> PyResult<()> {
        self.inner
            .fts_enable_global()
            .map_err(to_py_err)
    }

    /// Enable full-text search indexing for a specific database.
    fn fts_enable_database(&self, db_num: i32) -> PyResult<()> {
        self.inner
            .fts_enable_database(db_num)
            .map_err(to_py_err)
    }

    /// Enable full-text search indexing for keys matching a pattern.
    fn fts_enable_pattern(&self, pattern: &str) -> PyResult<()> {
        self.inner
            .fts_enable_pattern(pattern)
            .map_err(to_py_err)
    }

    /// Enable full-text search indexing for a specific key.
    fn fts_enable_key(&self, key: &str) -> PyResult<()> {
        self.inner
            .fts_enable_key(key)
            .map_err(to_py_err)
    }

    /// Disable full-text search indexing globally.
    fn fts_disable_global(&self) -> PyResult<()> {
        self.inner
            .fts_disable_global()
            .map_err(to_py_err)
    }

    /// Disable full-text search indexing for a specific database.
    fn fts_disable_database(&self, db_num: i32) -> PyResult<()> {
        self.inner
            .fts_disable_database(db_num)
            .map_err(to_py_err)
    }

    /// Disable full-text search indexing for keys matching a pattern.
    fn fts_disable_pattern(&self, pattern: &str) -> PyResult<()> {
        self.inner
            .fts_disable_pattern(pattern)
            .map_err(to_py_err)
    }

    /// Disable full-text search indexing for a specific key.
    fn fts_disable_key(&self, key: &str) -> PyResult<()> {
        self.inner
            .fts_disable_key(key)
            .map_err(to_py_err)
    }

    /// Check if FTS is enabled for a key.
    fn is_fts_enabled(&self, key: &str) -> PyResult<bool> {
        self.inner
            .is_fts_enabled(key)
            .map_err(to_py_err)
    }

    // =========================================================================
    // KeyInfo Command
    // =========================================================================

    /// Get detailed information about a key.
    /// Returns (type, ttl, created_at_ms, updated_at_ms) or None if key doesn't exist.
    fn keyinfo(&self, key: &str) -> PyResult<Option<(String, i64, i64, i64)>> {
        self.inner
            .keyinfo(key)
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
                (type_str.to_string(), info.ttl, info.created_at, info.updated_at)
            }))
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

/// Parse retention string into RetentionType
fn parse_retention(retention: &str) -> PyResult<redlite::RetentionType> {
    if retention == "unlimited" {
        Ok(redlite::RetentionType::Unlimited)
    } else if let Some(ms) = retention.strip_prefix("time:") {
        ms.parse::<i64>()
            .map(redlite::RetentionType::Time)
            .map_err(|_| PyErr::new::<pyo3::exceptions::PyValueError, _>(
                "Invalid time value. Use 'time:<milliseconds>'"
            ))
    } else if let Some(count) = retention.strip_prefix("count:") {
        count.parse::<i64>()
            .map(redlite::RetentionType::Count)
            .map_err(|_| PyErr::new::<pyo3::exceptions::PyValueError, _>(
                "Invalid count value. Use 'count:<n>'"
            ))
    } else {
        Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
            "Invalid retention. Use 'unlimited', 'time:<ms>', or 'count:<n>'"
        ))
    }
}

/// Python module definition
#[pymodule]
fn _native(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<EmbeddedDb>()?;
    m.add_class::<SetOptions>()?;
    m.add_class::<ZMember>()?;
    m.add_class::<StreamId>()?;
    Ok(())
}
