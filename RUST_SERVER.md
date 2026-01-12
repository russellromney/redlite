# Redlite Server Implementation

## RESP Reader

```rust
// src/resp/reader.rs

use tokio::io::{AsyncBufReadExt, AsyncReadExt, BufReader};
use std::io;

pub struct RespReader<R> {
    reader: BufReader<R>,
}

impl<R: AsyncReadExt + Unpin> RespReader<R> {
    pub fn new(reader: R) -> Self {
        Self {
            reader: BufReader::new(reader),
        }
    }

    /// Read a Redis command (array of bulk strings)
    pub async fn read_command(&mut self) -> io::Result<Option<Vec<Vec<u8>>>> {
        let mut line = String::new();
        let n = self.reader.read_line(&mut line).await?;
        if n == 0 {
            return Ok(None); // EOF
        }

        if !line.starts_with('*') {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "expected array",
            ));
        }

        let count: usize = line[1..].trim().parse().map_err(|_| {
            io::Error::new(io::ErrorKind::InvalidData, "invalid array length")
        })?;

        let mut args = Vec::with_capacity(count);
        for _ in 0..count {
            let arg = self.read_bulk_string().await?;
            args.push(arg);
        }

        Ok(Some(args))
    }

    async fn read_bulk_string(&mut self) -> io::Result<Vec<u8>> {
        let mut line = String::new();
        self.reader.read_line(&mut line).await?;

        if !line.starts_with('$') {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "expected bulk string",
            ));
        }

        let len: i64 = line[1..].trim().parse().map_err(|_| {
            io::Error::new(io::ErrorKind::InvalidData, "invalid bulk string length")
        })?;

        if len == -1 {
            return Ok(vec![]); // Null bulk string
        }

        let len = len as usize;
        let mut data = vec![0u8; len + 2]; // +2 for \r\n
        self.reader.read_exact(&mut data).await?;

        data.truncate(len); // Remove \r\n
        Ok(data)
    }
}
```

## Command Router

```rust
// src/server/router.rs

use std::collections::HashMap;
use std::sync::Arc;

use crate::db::Db;
use crate::error::{KvError, Result};
use crate::resp::RespValue;
use crate::types::{KeyType, SetOptions, ZMember};

type CommandFn = fn(&Db, &[Vec<u8>]) -> Result<RespValue>;

pub struct CommandRouter {
    commands: HashMap<String, CommandFn>,
}

impl CommandRouter {
    pub fn new() -> Self {
        let mut commands: HashMap<String, CommandFn> = HashMap::new();

        // Strings
        commands.insert("GET".into(), cmd_get);
        commands.insert("SET".into(), cmd_set);
        commands.insert("MGET".into(), cmd_mget);
        commands.insert("MSET".into(), cmd_mset);
        commands.insert("INCR".into(), cmd_incr);
        commands.insert("INCRBY".into(), cmd_incrby);
        commands.insert("DECR".into(), cmd_decr);
        commands.insert("DECRBY".into(), cmd_decrby);
        commands.insert("INCRBYFLOAT".into(), cmd_incrbyfloat);
        commands.insert("APPEND".into(), cmd_append);
        commands.insert("STRLEN".into(), cmd_strlen);
        commands.insert("GETSET".into(), cmd_getset);
        commands.insert("SETNX".into(), cmd_setnx);
        commands.insert("SETEX".into(), cmd_setex);
        commands.insert("PSETEX".into(), cmd_psetex);

        // Keys
        commands.insert("DEL".into(), cmd_del);
        commands.insert("EXISTS".into(), cmd_exists);
        commands.insert("EXPIRE".into(), cmd_expire);
        commands.insert("EXPIREAT".into(), cmd_expireat);
        commands.insert("PEXPIRE".into(), cmd_pexpire);
        commands.insert("PEXPIREAT".into(), cmd_pexpireat);
        commands.insert("TTL".into(), cmd_ttl);
        commands.insert("PTTL".into(), cmd_pttl);
        commands.insert("PERSIST".into(), cmd_persist);
        commands.insert("RENAME".into(), cmd_rename);
        commands.insert("RENAMENX".into(), cmd_renamenx);
        commands.insert("TYPE".into(), cmd_type);
        commands.insert("KEYS".into(), cmd_keys);
        commands.insert("SCAN".into(), cmd_scan);
        commands.insert("DBSIZE".into(), cmd_dbsize);
        commands.insert("FLUSHDB".into(), cmd_flushdb);
        commands.insert("RANDOMKEY".into(), cmd_randomkey);

        // Hashes
        commands.insert("HGET".into(), cmd_hget);
        commands.insert("HSET".into(), cmd_hset);
        commands.insert("HMSET".into(), cmd_hset); // Same as HSET
        commands.insert("HMGET".into(), cmd_hmget);
        commands.insert("HGETALL".into(), cmd_hgetall);
        commands.insert("HDEL".into(), cmd_hdel);
        commands.insert("HEXISTS".into(), cmd_hexists);
        commands.insert("HKEYS".into(), cmd_hkeys);
        commands.insert("HVALS".into(), cmd_hvals);
        commands.insert("HLEN".into(), cmd_hlen);
        commands.insert("HINCRBY".into(), cmd_hincrby);
        commands.insert("HINCRBYFLOAT".into(), cmd_hincrbyfloat);
        commands.insert("HSETNX".into(), cmd_hsetnx);

        // Lists
        commands.insert("LPUSH".into(), cmd_lpush);
        commands.insert("RPUSH".into(), cmd_rpush);
        commands.insert("LPOP".into(), cmd_lpop);
        commands.insert("RPOP".into(), cmd_rpop);
        commands.insert("LLEN".into(), cmd_llen);
        commands.insert("LRANGE".into(), cmd_lrange);
        commands.insert("LINDEX".into(), cmd_lindex);
        commands.insert("LSET".into(), cmd_lset);
        commands.insert("LTRIM".into(), cmd_ltrim);

        // Sets
        commands.insert("SADD".into(), cmd_sadd);
        commands.insert("SREM".into(), cmd_srem);
        commands.insert("SMEMBERS".into(), cmd_smembers);
        commands.insert("SISMEMBER".into(), cmd_sismember);
        commands.insert("SCARD".into(), cmd_scard);
        commands.insert("SPOP".into(), cmd_spop);
        commands.insert("SRANDMEMBER".into(), cmd_srandmember);
        commands.insert("SINTER".into(), cmd_sinter);
        commands.insert("SUNION".into(), cmd_sunion);
        commands.insert("SDIFF".into(), cmd_sdiff);
        commands.insert("SMOVE".into(), cmd_smove);

        // Sorted Sets
        commands.insert("ZADD".into(), cmd_zadd);
        commands.insert("ZREM".into(), cmd_zrem);
        commands.insert("ZSCORE".into(), cmd_zscore);
        commands.insert("ZRANK".into(), cmd_zrank);
        commands.insert("ZREVRANK".into(), cmd_zrevrank);
        commands.insert("ZCARD".into(), cmd_zcard);
        commands.insert("ZRANGE".into(), cmd_zrange);
        commands.insert("ZRANGEBYSCORE".into(), cmd_zrangebyscore);
        commands.insert("ZCOUNT".into(), cmd_zcount);
        commands.insert("ZINCRBY".into(), cmd_zincrby);
        commands.insert("ZREMRANGEBYRANK".into(), cmd_zremrangebyrank);
        commands.insert("ZREMRANGEBYSCORE".into(), cmd_zremrangebyscore);

        // Server
        commands.insert("PING".into(), cmd_ping);
        commands.insert("ECHO".into(), cmd_echo);
        commands.insert("SELECT".into(), cmd_select);
        commands.insert("INFO".into(), cmd_info);
        commands.insert("QUIT".into(), cmd_quit);
        commands.insert("COMMAND".into(), cmd_command);

        Self { commands }
    }

    pub fn execute(&self, db: &Db, args: &[Vec<u8>]) -> RespValue {
        if args.is_empty() {
            return RespValue::error("empty command");
        }

        let cmd = String::from_utf8_lossy(&args[0]).to_uppercase();

        match self.commands.get(&cmd) {
            Some(handler) => match handler(db, &args[1..]) {
                Ok(v) => v,
                Err(KvError::WrongType) => RespValue::wrong_type(),
                Err(e) => RespValue::error(&e.to_string()),
            },
            None => RespValue::error(&format!("unknown command '{}'", cmd)),
        }
    }
}

impl Default for CommandRouter {
    fn default() -> Self {
        Self::new()
    }
}
```

## Command Implementations

```rust
// src/server/commands.rs

use std::time::Duration;

// Helper to parse string argument
fn arg_str(args: &[Vec<u8>], index: usize) -> Result<&str> {
    args.get(index)
        .ok_or(KvError::SyntaxError)
        .and_then(|a| std::str::from_utf8(a).map_err(|_| KvError::SyntaxError))
}

// Helper to parse i64 argument
fn arg_i64(args: &[Vec<u8>], index: usize) -> Result<i64> {
    arg_str(args, index)?
        .parse()
        .map_err(|_| KvError::NotInteger)
}

// Helper to parse f64 argument
fn arg_f64(args: &[Vec<u8>], index: usize) -> Result<f64> {
    arg_str(args, index)?
        .parse()
        .map_err(|_| KvError::NotFloat)
}

// --- Server commands ---

fn cmd_ping(_db: &Db, args: &[Vec<u8>]) -> Result<RespValue> {
    if args.is_empty() {
        Ok(RespValue::pong())
    } else {
        Ok(RespValue::BulkString(Some(args[0].clone())))
    }
}

fn cmd_echo(_db: &Db, args: &[Vec<u8>]) -> Result<RespValue> {
    if args.is_empty() {
        return Err(KvError::SyntaxError);
    }
    Ok(RespValue::BulkString(Some(args[0].clone())))
}

fn cmd_select(db: &Db, args: &[Vec<u8>]) -> Result<RespValue> {
    let n = arg_i64(args, 0)? as i32;
    db.select(n)?;
    Ok(RespValue::ok())
}

fn cmd_quit(_db: &Db, _args: &[Vec<u8>]) -> Result<RespValue> {
    Ok(RespValue::ok())
}

fn cmd_info(_db: &Db, _args: &[Vec<u8>]) -> Result<RespValue> {
    let info = "# Server\r\nredlite_version:0.1.0\r\n";
    Ok(RespValue::BulkString(Some(info.as_bytes().to_vec())))
}

fn cmd_command(_db: &Db, _args: &[Vec<u8>]) -> Result<RespValue> {
    // Minimal implementation for client compatibility
    Ok(RespValue::Array(Some(vec![])))
}

// --- String commands ---

fn cmd_get(db: &Db, args: &[Vec<u8>]) -> Result<RespValue> {
    let key = arg_str(args, 0)?;
    Ok(db.get(key)?.into())
}

fn cmd_set(db: &Db, args: &[Vec<u8>]) -> Result<RespValue> {
    if args.len() < 2 {
        return Err(KvError::SyntaxError);
    }

    let key = arg_str(args, 0)?;
    let value = &args[1];

    let mut opts = SetOptions::default();
    let mut i = 2;

    while i < args.len() {
        let opt = String::from_utf8_lossy(&args[i]).to_uppercase();
        match opt.as_str() {
            "EX" => {
                i += 1;
                let secs = arg_i64(args, i)? as u64;
                opts.ttl = Some(Duration::from_secs(secs));
            }
            "PX" => {
                i += 1;
                let ms = arg_i64(args, i)? as u64;
                opts.ttl = Some(Duration::from_millis(ms));
            }
            "NX" => opts.nx = true,
            "XX" => opts.xx = true,
            _ => return Err(KvError::SyntaxError),
        }
        i += 1;
    }

    let ok = db.set_opts(key, value, opts)?;
    if ok {
        Ok(RespValue::ok())
    } else {
        Ok(RespValue::null())
    }
}

fn cmd_mget(db: &Db, args: &[Vec<u8>]) -> Result<RespValue> {
    let keys: Vec<&str> = args
        .iter()
        .map(|a| std::str::from_utf8(a))
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|_| KvError::SyntaxError)?;

    let values = db.mget(&keys)?;
    let items: Vec<RespValue> = values.into_iter().map(|v| v.into()).collect();
    Ok(RespValue::Array(Some(items)))
}

fn cmd_mset(db: &Db, args: &[Vec<u8>]) -> Result<RespValue> {
    if args.len() < 2 || args.len() % 2 != 0 {
        return Err(KvError::SyntaxError);
    }

    let mut pairs = vec![];
    for i in (0..args.len()).step_by(2) {
        let key = std::str::from_utf8(&args[i]).map_err(|_| KvError::SyntaxError)?;
        let value = &args[i + 1];
        pairs.push((key, value.as_slice()));
    }

    db.mset(&pairs)?;
    Ok(RespValue::ok())
}

fn cmd_incr(db: &Db, args: &[Vec<u8>]) -> Result<RespValue> {
    let key = arg_str(args, 0)?;
    Ok(db.incr(key)?.into())
}

fn cmd_incrby(db: &Db, args: &[Vec<u8>]) -> Result<RespValue> {
    let key = arg_str(args, 0)?;
    let delta = arg_i64(args, 1)?;
    Ok(db.incr_by(key, delta)?.into())
}

fn cmd_decr(db: &Db, args: &[Vec<u8>]) -> Result<RespValue> {
    let key = arg_str(args, 0)?;
    Ok(db.decr(key)?.into())
}

fn cmd_decrby(db: &Db, args: &[Vec<u8>]) -> Result<RespValue> {
    let key = arg_str(args, 0)?;
    let delta = arg_i64(args, 1)?;
    Ok(db.incr_by(key, -delta)?.into())
}

fn cmd_incrbyfloat(db: &Db, args: &[Vec<u8>]) -> Result<RespValue> {
    let key = arg_str(args, 0)?;
    let delta = arg_f64(args, 1)?;
    Ok(RespValue::from_float(db.incr_by_float(key, delta)?))
}

fn cmd_append(db: &Db, args: &[Vec<u8>]) -> Result<RespValue> {
    let key = arg_str(args, 0)?;
    let value = args.get(1).ok_or(KvError::SyntaxError)?;
    Ok(db.append(key, value)?.into())
}

fn cmd_strlen(db: &Db, args: &[Vec<u8>]) -> Result<RespValue> {
    let key = arg_str(args, 0)?;
    Ok(db.strlen(key)?.into())
}

fn cmd_getset(db: &Db, args: &[Vec<u8>]) -> Result<RespValue> {
    let key = arg_str(args, 0)?;
    let value = args.get(1).ok_or(KvError::SyntaxError)?;
    Ok(db.getset(key, value)?.into())
}

fn cmd_setnx(db: &Db, args: &[Vec<u8>]) -> Result<RespValue> {
    let key = arg_str(args, 0)?;
    let value = args.get(1).ok_or(KvError::SyntaxError)?;
    Ok(RespValue::from_bool(db.setnx(key, value)?))
}

fn cmd_setex(db: &Db, args: &[Vec<u8>]) -> Result<RespValue> {
    let key = arg_str(args, 0)?;
    let secs = arg_i64(args, 1)? as u64;
    let value = args.get(2).ok_or(KvError::SyntaxError)?;
    db.setex(key, secs, value)?;
    Ok(RespValue::ok())
}

fn cmd_psetex(db: &Db, args: &[Vec<u8>]) -> Result<RespValue> {
    let key = arg_str(args, 0)?;
    let ms = arg_i64(args, 1)? as u64;
    let value = args.get(2).ok_or(KvError::SyntaxError)?;
    db.psetex(key, ms, value)?;
    Ok(RespValue::ok())
}

// --- Key commands ---

fn cmd_del(db: &Db, args: &[Vec<u8>]) -> Result<RespValue> {
    let keys: Vec<&str> = args
        .iter()
        .map(|a| std::str::from_utf8(a))
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|_| KvError::SyntaxError)?;

    Ok(db.del(&keys)?.into())
}

fn cmd_exists(db: &Db, args: &[Vec<u8>]) -> Result<RespValue> {
    let keys: Vec<&str> = args
        .iter()
        .map(|a| std::str::from_utf8(a))
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|_| KvError::SyntaxError)?;

    Ok(db.exists(&keys)?.into())
}

fn cmd_expire(db: &Db, args: &[Vec<u8>]) -> Result<RespValue> {
    let key = arg_str(args, 0)?;
    let secs = arg_i64(args, 1)? as u64;
    Ok(RespValue::from_bool(db.expire(key, Duration::from_secs(secs))?))
}

fn cmd_expireat(db: &Db, args: &[Vec<u8>]) -> Result<RespValue> {
    let key = arg_str(args, 0)?;
    let timestamp = arg_i64(args, 1)?;
    Ok(RespValue::from_bool(db.expire_at(key, timestamp)?))
}

fn cmd_pexpire(db: &Db, args: &[Vec<u8>]) -> Result<RespValue> {
    let key = arg_str(args, 0)?;
    let ms = arg_i64(args, 1)?;
    Ok(RespValue::from_bool(db.pexpire(key, ms)?))
}

fn cmd_pexpireat(db: &Db, args: &[Vec<u8>]) -> Result<RespValue> {
    let key = arg_str(args, 0)?;
    let timestamp_ms = arg_i64(args, 1)?;
    Ok(RespValue::from_bool(db.pexpire_at(key, timestamp_ms)?))
}

fn cmd_ttl(db: &Db, args: &[Vec<u8>]) -> Result<RespValue> {
    let key = arg_str(args, 0)?;
    Ok(db.ttl(key)?.into())
}

fn cmd_pttl(db: &Db, args: &[Vec<u8>]) -> Result<RespValue> {
    let key = arg_str(args, 0)?;
    Ok(db.pttl(key)?.into())
}

fn cmd_persist(db: &Db, args: &[Vec<u8>]) -> Result<RespValue> {
    let key = arg_str(args, 0)?;
    Ok(RespValue::from_bool(db.persist(key)?))
}

fn cmd_rename(db: &Db, args: &[Vec<u8>]) -> Result<RespValue> {
    let key = arg_str(args, 0)?;
    let new_key = arg_str(args, 1)?;
    db.rename(key, new_key)?;
    Ok(RespValue::ok())
}

fn cmd_renamenx(db: &Db, args: &[Vec<u8>]) -> Result<RespValue> {
    let key = arg_str(args, 0)?;
    let new_key = arg_str(args, 1)?;
    Ok(RespValue::from_bool(db.renamenx(key, new_key)?))
}

fn cmd_type(db: &Db, args: &[Vec<u8>]) -> Result<RespValue> {
    let key = arg_str(args, 0)?;
    let type_str = db.key_type(key)?
        .map(|t| t.as_str())
        .unwrap_or("none");
    Ok(RespValue::SimpleString(type_str.to_string()))
}

fn cmd_keys(db: &Db, args: &[Vec<u8>]) -> Result<RespValue> {
    let pattern = arg_str(args, 0)?;
    let keys = db.keys(pattern)?;
    let items: Vec<RespValue> = keys.into_iter().map(RespValue::from_string).collect();
    Ok(RespValue::Array(Some(items)))
}

fn cmd_scan(db: &Db, args: &[Vec<u8>]) -> Result<RespValue> {
    use crate::scan::{ScanCursor, ScanOptions};

    let cursor_str = arg_str(args, 0)?;
    let cursor = ScanCursor::decode(cursor_str)?;

    let mut opts = ScanOptions::default();
    let mut i = 1;
    while i < args.len() {
        let opt = String::from_utf8_lossy(&args[i]).to_uppercase();
        match opt.as_str() {
            "MATCH" => {
                i += 1;
                opts.pattern = Some(arg_str(args, i)?.to_string());
            }
            "COUNT" => {
                i += 1;
                opts.count = Some(arg_i64(args, i)?);
            }
            "TYPE" => {
                i += 1;
                opts.key_type = KeyType::from_str(arg_str(args, i)?);
            }
            _ => {}
        }
        i += 1;
    }

    let result = db.scan(&cursor_str, opts.pattern.as_deref(), opts.count, opts.key_type)?;

    let keys: Vec<RespValue> = result.keys.into_iter().map(RespValue::from_string).collect();
    Ok(RespValue::Array(Some(vec![
        RespValue::BulkString(Some(result.cursor.into_bytes())),
        RespValue::Array(Some(keys)),
    ])))
}

fn cmd_dbsize(db: &Db, _args: &[Vec<u8>]) -> Result<RespValue> {
    Ok(db.dbsize()?.into())
}

fn cmd_flushdb(db: &Db, _args: &[Vec<u8>]) -> Result<RespValue> {
    db.flushdb()?;
    Ok(RespValue::ok())
}

fn cmd_randomkey(db: &Db, _args: &[Vec<u8>]) -> Result<RespValue> {
    Ok(db.randomkey()?.map(|s| s.into_bytes()).into())
}

// --- Hash commands ---

fn cmd_hget(db: &Db, args: &[Vec<u8>]) -> Result<RespValue> {
    let key = arg_str(args, 0)?;
    let field = arg_str(args, 1)?;
    Ok(db.hget(key, field)?.into())
}

fn cmd_hset(db: &Db, args: &[Vec<u8>]) -> Result<RespValue> {
    if args.len() < 3 || (args.len() - 1) % 2 != 0 {
        return Err(KvError::SyntaxError);
    }

    let key = arg_str(args, 0)?;
    let mut fields = vec![];
    for i in (1..args.len()).step_by(2) {
        let field = std::str::from_utf8(&args[i]).map_err(|_| KvError::SyntaxError)?;
        let value = &args[i + 1];
        fields.push((field, value.as_slice()));
    }

    Ok(db.hset(key, &fields)?.into())
}

fn cmd_hmget(db: &Db, args: &[Vec<u8>]) -> Result<RespValue> {
    let key = arg_str(args, 0)?;
    let fields: Vec<&str> = args[1..]
        .iter()
        .map(|a| std::str::from_utf8(a))
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|_| KvError::SyntaxError)?;

    let values = db.hmget(key, &fields)?;
    let items: Vec<RespValue> = values.into_iter().map(|v| v.into()).collect();
    Ok(RespValue::Array(Some(items)))
}

fn cmd_hgetall(db: &Db, args: &[Vec<u8>]) -> Result<RespValue> {
    let key = arg_str(args, 0)?;
    let map = db.hgetall(key)?;

    let mut items = vec![];
    for (field, value) in map {
        items.push(RespValue::from_string(field));
        items.push(RespValue::from_bytes(value));
    }

    Ok(RespValue::Array(Some(items)))
}

fn cmd_hdel(db: &Db, args: &[Vec<u8>]) -> Result<RespValue> {
    let key = arg_str(args, 0)?;
    let fields: Vec<&str> = args[1..]
        .iter()
        .map(|a| std::str::from_utf8(a))
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|_| KvError::SyntaxError)?;

    Ok(db.hdel(key, &fields)?.into())
}

fn cmd_hexists(db: &Db, args: &[Vec<u8>]) -> Result<RespValue> {
    let key = arg_str(args, 0)?;
    let field = arg_str(args, 1)?;
    Ok(RespValue::from_bool(db.hexists(key, field)?))
}

fn cmd_hkeys(db: &Db, args: &[Vec<u8>]) -> Result<RespValue> {
    let key = arg_str(args, 0)?;
    let keys = db.hkeys(key)?;
    let items: Vec<RespValue> = keys.into_iter().map(RespValue::from_string).collect();
    Ok(RespValue::Array(Some(items)))
}

fn cmd_hvals(db: &Db, args: &[Vec<u8>]) -> Result<RespValue> {
    let key = arg_str(args, 0)?;
    let values = db.hvals(key)?;
    let items: Vec<RespValue> = values.into_iter().map(RespValue::from_bytes).collect();
    Ok(RespValue::Array(Some(items)))
}

fn cmd_hlen(db: &Db, args: &[Vec<u8>]) -> Result<RespValue> {
    let key = arg_str(args, 0)?;
    Ok(db.hlen(key)?.into())
}

fn cmd_hincrby(db: &Db, args: &[Vec<u8>]) -> Result<RespValue> {
    let key = arg_str(args, 0)?;
    let field = arg_str(args, 1)?;
    let delta = arg_i64(args, 2)?;
    Ok(db.hincrby(key, field, delta)?.into())
}

fn cmd_hincrbyfloat(db: &Db, args: &[Vec<u8>]) -> Result<RespValue> {
    let key = arg_str(args, 0)?;
    let field = arg_str(args, 1)?;
    let delta = arg_f64(args, 2)?;
    Ok(RespValue::from_float(db.hincrbyfloat(key, field, delta)?))
}

fn cmd_hsetnx(db: &Db, args: &[Vec<u8>]) -> Result<RespValue> {
    let key = arg_str(args, 0)?;
    let field = arg_str(args, 1)?;
    let value = args.get(2).ok_or(KvError::SyntaxError)?;
    Ok(RespValue::from_bool(db.hsetnx(key, field, value)?))
}

// --- List commands ---

fn cmd_lpush(db: &Db, args: &[Vec<u8>]) -> Result<RespValue> {
    let key = arg_str(args, 0)?;
    let values: Vec<&[u8]> = args[1..].iter().map(|v| v.as_slice()).collect();
    Ok(db.lpush(key, &values)?.into())
}

fn cmd_rpush(db: &Db, args: &[Vec<u8>]) -> Result<RespValue> {
    let key = arg_str(args, 0)?;
    let values: Vec<&[u8]> = args[1..].iter().map(|v| v.as_slice()).collect();
    Ok(db.rpush(key, &values)?.into())
}

fn cmd_lpop(db: &Db, args: &[Vec<u8>]) -> Result<RespValue> {
    let key = arg_str(args, 0)?;
    Ok(db.lpop(key)?.into())
}

fn cmd_rpop(db: &Db, args: &[Vec<u8>]) -> Result<RespValue> {
    let key = arg_str(args, 0)?;
    Ok(db.rpop(key)?.into())
}

fn cmd_llen(db: &Db, args: &[Vec<u8>]) -> Result<RespValue> {
    let key = arg_str(args, 0)?;
    Ok(db.llen(key)?.into())
}

fn cmd_lrange(db: &Db, args: &[Vec<u8>]) -> Result<RespValue> {
    let key = arg_str(args, 0)?;
    let start = arg_i64(args, 1)?;
    let stop = arg_i64(args, 2)?;

    let values = db.lrange(key, start, stop)?;
    let items: Vec<RespValue> = values.into_iter().map(RespValue::from_bytes).collect();
    Ok(RespValue::Array(Some(items)))
}

fn cmd_lindex(db: &Db, args: &[Vec<u8>]) -> Result<RespValue> {
    let key = arg_str(args, 0)?;
    let index = arg_i64(args, 1)?;
    Ok(db.lindex(key, index)?.into())
}

fn cmd_lset(db: &Db, args: &[Vec<u8>]) -> Result<RespValue> {
    let key = arg_str(args, 0)?;
    let index = arg_i64(args, 1)?;
    let value = args.get(2).ok_or(KvError::SyntaxError)?;
    db.lset(key, index, value)?;
    Ok(RespValue::ok())
}

fn cmd_ltrim(db: &Db, args: &[Vec<u8>]) -> Result<RespValue> {
    let key = arg_str(args, 0)?;
    let start = arg_i64(args, 1)?;
    let stop = arg_i64(args, 2)?;
    db.ltrim(key, start, stop)?;
    Ok(RespValue::ok())
}

// --- Set commands ---

fn cmd_sadd(db: &Db, args: &[Vec<u8>]) -> Result<RespValue> {
    let key = arg_str(args, 0)?;
    let members: Vec<&[u8]> = args[1..].iter().map(|v| v.as_slice()).collect();
    Ok(db.sadd(key, &members)?.into())
}

fn cmd_srem(db: &Db, args: &[Vec<u8>]) -> Result<RespValue> {
    let key = arg_str(args, 0)?;
    let members: Vec<&[u8]> = args[1..].iter().map(|v| v.as_slice()).collect();
    Ok(db.srem(key, &members)?.into())
}

fn cmd_smembers(db: &Db, args: &[Vec<u8>]) -> Result<RespValue> {
    let key = arg_str(args, 0)?;
    let members = db.smembers(key)?;
    let items: Vec<RespValue> = members.into_iter().map(RespValue::from_bytes).collect();
    Ok(RespValue::Array(Some(items)))
}

fn cmd_sismember(db: &Db, args: &[Vec<u8>]) -> Result<RespValue> {
    let key = arg_str(args, 0)?;
    let member = args.get(1).ok_or(KvError::SyntaxError)?;
    Ok(RespValue::from_bool(db.sismember(key, member)?))
}

fn cmd_scard(db: &Db, args: &[Vec<u8>]) -> Result<RespValue> {
    let key = arg_str(args, 0)?;
    Ok(db.scard(key)?.into())
}

fn cmd_spop(db: &Db, args: &[Vec<u8>]) -> Result<RespValue> {
    let key = arg_str(args, 0)?;
    Ok(db.spop(key)?.into())
}

fn cmd_srandmember(db: &Db, args: &[Vec<u8>]) -> Result<RespValue> {
    let key = arg_str(args, 0)?;
    let count = if args.len() > 1 { Some(arg_i64(args, 1)?) } else { None };

    let members = db.srandmember(key, count)?;
    if count.is_none() && members.len() == 1 {
        Ok(RespValue::from_bytes(members.into_iter().next().unwrap()))
    } else {
        let items: Vec<RespValue> = members.into_iter().map(RespValue::from_bytes).collect();
        Ok(RespValue::Array(Some(items)))
    }
}

fn cmd_sinter(db: &Db, args: &[Vec<u8>]) -> Result<RespValue> {
    let keys: Vec<&str> = args
        .iter()
        .map(|a| std::str::from_utf8(a))
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|_| KvError::SyntaxError)?;

    let members = db.sinter(&keys)?;
    let items: Vec<RespValue> = members.into_iter().map(RespValue::from_bytes).collect();
    Ok(RespValue::Array(Some(items)))
}

fn cmd_sunion(db: &Db, args: &[Vec<u8>]) -> Result<RespValue> {
    let keys: Vec<&str> = args
        .iter()
        .map(|a| std::str::from_utf8(a))
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|_| KvError::SyntaxError)?;

    let members = db.sunion(&keys)?;
    let items: Vec<RespValue> = members.into_iter().map(RespValue::from_bytes).collect();
    Ok(RespValue::Array(Some(items)))
}

fn cmd_sdiff(db: &Db, args: &[Vec<u8>]) -> Result<RespValue> {
    let keys: Vec<&str> = args
        .iter()
        .map(|a| std::str::from_utf8(a))
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|_| KvError::SyntaxError)?;

    let members = db.sdiff(&keys)?;
    let items: Vec<RespValue> = members.into_iter().map(RespValue::from_bytes).collect();
    Ok(RespValue::Array(Some(items)))
}

fn cmd_smove(db: &Db, args: &[Vec<u8>]) -> Result<RespValue> {
    let source = arg_str(args, 0)?;
    let dest = arg_str(args, 1)?;
    let member = args.get(2).ok_or(KvError::SyntaxError)?;
    Ok(RespValue::from_bool(db.smove(source, dest, member)?))
}

// --- Sorted set commands ---

fn cmd_zadd(db: &Db, args: &[Vec<u8>]) -> Result<RespValue> {
    if args.len() < 3 || (args.len() - 1) % 2 != 0 {
        return Err(KvError::SyntaxError);
    }

    let key = arg_str(args, 0)?;
    let mut members = vec![];
    for i in (1..args.len()).step_by(2) {
        let score = arg_f64(args, i)?;
        let member = args[i + 1].clone();
        members.push(ZMember { score, member });
    }

    Ok(db.zadd(key, &members)?.into())
}

fn cmd_zrem(db: &Db, args: &[Vec<u8>]) -> Result<RespValue> {
    let key = arg_str(args, 0)?;
    let members: Vec<&[u8]> = args[1..].iter().map(|v| v.as_slice()).collect();
    Ok(db.zrem(key, &members)?.into())
}

fn cmd_zscore(db: &Db, args: &[Vec<u8>]) -> Result<RespValue> {
    let key = arg_str(args, 0)?;
    let member = args.get(1).ok_or(KvError::SyntaxError)?;
    match db.zscore(key, member)? {
        Some(score) => Ok(RespValue::from_float(score)),
        None => Ok(RespValue::null()),
    }
}

fn cmd_zrank(db: &Db, args: &[Vec<u8>]) -> Result<RespValue> {
    let key = arg_str(args, 0)?;
    let member = args.get(1).ok_or(KvError::SyntaxError)?;
    match db.zrank(key, member)? {
        Some(rank) => Ok(rank.into()),
        None => Ok(RespValue::null()),
    }
}

fn cmd_zrevrank(db: &Db, args: &[Vec<u8>]) -> Result<RespValue> {
    let key = arg_str(args, 0)?;
    let member = args.get(1).ok_or(KvError::SyntaxError)?;
    match db.zrevrank(key, member)? {
        Some(rank) => Ok(rank.into()),
        None => Ok(RespValue::null()),
    }
}

fn cmd_zcard(db: &Db, args: &[Vec<u8>]) -> Result<RespValue> {
    let key = arg_str(args, 0)?;
    Ok(db.zcard(key)?.into())
}

fn cmd_zrange(db: &Db, args: &[Vec<u8>]) -> Result<RespValue> {
    let key = arg_str(args, 0)?;
    let start = arg_i64(args, 1)?;
    let stop = arg_i64(args, 2)?;

    let with_scores = args.get(3)
        .map(|a| String::from_utf8_lossy(a).to_uppercase() == "WITHSCORES")
        .unwrap_or(false);

    let members = db.zrange(key, start, stop, with_scores)?;

    if with_scores {
        let mut items = vec![];
        for m in members {
            items.push(RespValue::from_bytes(m.member));
            items.push(RespValue::from_float(m.score));
        }
        Ok(RespValue::Array(Some(items)))
    } else {
        let items: Vec<RespValue> = members.into_iter()
            .map(|m| RespValue::from_bytes(m.member))
            .collect();
        Ok(RespValue::Array(Some(items)))
    }
}

fn cmd_zrangebyscore(db: &Db, args: &[Vec<u8>]) -> Result<RespValue> {
    let key = arg_str(args, 0)?;
    let min = parse_score(arg_str(args, 1)?)?;
    let max = parse_score(arg_str(args, 2)?)?;

    let mut offset = None;
    let mut count = None;
    let mut with_scores = false;

    let mut i = 3;
    while i < args.len() {
        let opt = String::from_utf8_lossy(&args[i]).to_uppercase();
        match opt.as_str() {
            "WITHSCORES" => with_scores = true,
            "LIMIT" => {
                i += 1;
                offset = Some(arg_i64(args, i)?);
                i += 1;
                count = Some(arg_i64(args, i)?);
            }
            _ => {}
        }
        i += 1;
    }

    let members = db.zrangebyscore(key, min, max, offset, count)?;

    if with_scores {
        let mut items = vec![];
        for m in members {
            items.push(RespValue::from_bytes(m.member));
            items.push(RespValue::from_float(m.score));
        }
        Ok(RespValue::Array(Some(items)))
    } else {
        let items: Vec<RespValue> = members.into_iter()
            .map(|m| RespValue::from_bytes(m.member))
            .collect();
        Ok(RespValue::Array(Some(items)))
    }
}

fn cmd_zcount(db: &Db, args: &[Vec<u8>]) -> Result<RespValue> {
    let key = arg_str(args, 0)?;
    let min = parse_score(arg_str(args, 1)?)?;
    let max = parse_score(arg_str(args, 2)?)?;
    Ok(db.zcount(key, min, max)?.into())
}

fn cmd_zincrby(db: &Db, args: &[Vec<u8>]) -> Result<RespValue> {
    let key = arg_str(args, 0)?;
    let delta = arg_f64(args, 1)?;
    let member = args.get(2).ok_or(KvError::SyntaxError)?;
    Ok(RespValue::from_float(db.zincrby(key, delta, member)?))
}

fn cmd_zremrangebyrank(db: &Db, args: &[Vec<u8>]) -> Result<RespValue> {
    let key = arg_str(args, 0)?;
    let start = arg_i64(args, 1)?;
    let stop = arg_i64(args, 2)?;
    Ok(db.zremrangebyrank(key, start, stop)?.into())
}

fn cmd_zremrangebyscore(db: &Db, args: &[Vec<u8>]) -> Result<RespValue> {
    let key = arg_str(args, 0)?;
    let min = parse_score(arg_str(args, 1)?)?;
    let max = parse_score(arg_str(args, 2)?)?;
    Ok(db.zremrangebyscore(key, min, max)?.into())
}

fn parse_score(s: &str) -> Result<f64> {
    match s.to_lowercase().as_str() {
        "-inf" => Ok(f64::NEG_INFINITY),
        "+inf" | "inf" => Ok(f64::INFINITY),
        _ => {
            if s.starts_with('(') {
                // Exclusive score - just parse the number
                s[1..].parse().map_err(|_| KvError::SyntaxError)
            } else {
                s.parse().map_err(|_| KvError::SyntaxError)
            }
        }
    }
}
```

## TCP Server

```rust
// src/server/mod.rs

use std::sync::Arc;
use tokio::io::AsyncWriteExt;
use tokio::net::{TcpListener, TcpStream};

use crate::db::Db;
use crate::resp::RespReader;

mod commands;
mod router;

pub use router::CommandRouter;

pub struct Server {
    db: Arc<Db>,
    router: CommandRouter,
}

impl Server {
    pub fn new(db: Arc<Db>) -> Self {
        Self {
            db,
            router: CommandRouter::new(),
        }
    }

    pub async fn run(&self, addr: &str) -> std::io::Result<()> {
        let listener = TcpListener::bind(addr).await?;
        tracing::info!("Redlite listening on {}", addr);

        loop {
            let (socket, peer_addr) = listener.accept().await?;
            tracing::debug!("Connection from {}", peer_addr);

            let db = Arc::clone(&self.db);
            let router = CommandRouter::new();

            tokio::spawn(async move {
                if let Err(e) = handle_connection(socket, db, router).await {
                    tracing::error!("Connection error: {}", e);
                }
            });
        }
    }
}

async fn handle_connection(
    socket: TcpStream,
    db: Arc<Db>,
    router: CommandRouter,
) -> std::io::Result<()> {
    let (reader, mut writer) = socket.into_split();
    let mut reader = RespReader::new(reader);

    loop {
        match reader.read_command().await? {
            Some(args) => {
                let response = router.execute(&db, &args);
                writer.write_all(&response.encode()).await?;
                writer.flush().await?;

                // Check for QUIT
                if !args.is_empty() {
                    let cmd = String::from_utf8_lossy(&args[0]).to_uppercase();
                    if cmd == "QUIT" {
                        break;
                    }
                }
            }
            None => break, // EOF
        }
    }

    Ok(())
}
```

## Main Entry Point

```rust
// src/main.rs

use clap::Parser;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

mod db;
mod error;
mod resp;
mod scan;
mod server;
mod types;

use db::Db;
use server::Server;

#[derive(Parser)]
#[command(name = "redlite")]
#[command(about = "SQLite-backed Redis-compatible KV store")]
struct Args {
    /// Database file path
    #[arg(short, long, default_value = "redlite.db")]
    db: String,

    /// Listen address
    #[arg(short, long, default_value = "127.0.0.1:6379")]
    addr: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let args = Args::parse();

    let db = Arc::new(Db::open(&args.db)?);
    tracing::info!("Opened database: {}", args.db);

    // Start expiration daemon
    let shutdown = Arc::new(AtomicBool::new(false));
    let _expiration_handle = db.start_expiration_daemon(Arc::clone(&shutdown));

    // Handle shutdown signal
    let shutdown_clone = Arc::clone(&shutdown);
    tokio::spawn(async move {
        tokio::signal::ctrl_c().await.ok();
        tracing::info!("Shutting down...");
        shutdown_clone.store(true, Ordering::Relaxed);
    });

    let server = Server::new(db);
    server.run(&args.addr).await?;

    Ok(())
}
```
