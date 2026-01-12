use std::sync::Arc;
use tokio::io::AsyncWriteExt;
use tokio::net::{TcpListener, TcpStream};

use crate::db::Db;
use crate::error::KvError;
use crate::resp::{RespReader, RespValue};

pub struct Server {
    db: Arc<Db>,
}

impl Server {
    pub fn new(db: Arc<Db>) -> Self {
        Self { db }
    }

    pub async fn run(&self, addr: &str) -> std::io::Result<()> {
        let listener = TcpListener::bind(addr).await?;
        tracing::info!("Redlite listening on {}", addr);

        loop {
            let (socket, peer_addr) = listener.accept().await?;
            tracing::debug!("Connection from {}", peer_addr);

            let db = Arc::clone(&self.db);

            tokio::spawn(async move {
                if let Err(e) = handle_connection(socket, db).await {
                    tracing::error!("Connection error: {}", e);
                }
            });
        }
    }
}

async fn handle_connection(socket: TcpStream, db: Arc<Db>) -> std::io::Result<()> {
    let (reader, mut writer) = socket.into_split();
    let mut reader = RespReader::new(reader);

    loop {
        match reader.read_command().await? {
            Some(args) => {
                let response = execute_command(&db, &args);
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

fn execute_command(db: &Db, args: &[Vec<u8>]) -> RespValue {
    if args.is_empty() {
        return RespValue::error("empty command");
    }

    let cmd = String::from_utf8_lossy(&args[0]).to_uppercase();
    let cmd_args = &args[1..];

    match cmd.as_str() {
        "PING" => cmd_ping(cmd_args),
        "ECHO" => cmd_echo(cmd_args),
        "COMMAND" => cmd_command(),
        "QUIT" => RespValue::ok(),
        "GET" => cmd_get(db, cmd_args),
        "SET" => cmd_set(db, cmd_args),
        "DEL" => cmd_del(db, cmd_args),
        "TYPE" => cmd_type(db, cmd_args),
        "TTL" => cmd_ttl(db, cmd_args),
        "PTTL" => cmd_pttl(db, cmd_args),
        "EXISTS" => cmd_exists(db, cmd_args),
        "EXPIRE" => cmd_expire(db, cmd_args),
        "KEYS" => cmd_keys(db, cmd_args),
        "SCAN" => cmd_scan(db, cmd_args),
        // String operations
        "INCR" => cmd_incr(db, cmd_args),
        "DECR" => cmd_decr(db, cmd_args),
        "INCRBY" => cmd_incrby(db, cmd_args),
        "DECRBY" => cmd_decrby(db, cmd_args),
        "INCRBYFLOAT" => cmd_incrbyfloat(db, cmd_args),
        "MGET" => cmd_mget(db, cmd_args),
        "MSET" => cmd_mset(db, cmd_args),
        "APPEND" => cmd_append(db, cmd_args),
        "STRLEN" => cmd_strlen(db, cmd_args),
        "GETRANGE" => cmd_getrange(db, cmd_args),
        "SETRANGE" => cmd_setrange(db, cmd_args),
        // Hash operations
        "HSET" => cmd_hset(db, cmd_args),
        "HGET" => cmd_hget(db, cmd_args),
        "HMGET" => cmd_hmget(db, cmd_args),
        "HGETALL" => cmd_hgetall(db, cmd_args),
        "HDEL" => cmd_hdel(db, cmd_args),
        "HEXISTS" => cmd_hexists(db, cmd_args),
        "HKEYS" => cmd_hkeys(db, cmd_args),
        "HVALS" => cmd_hvals(db, cmd_args),
        "HLEN" => cmd_hlen(db, cmd_args),
        "HINCRBY" => cmd_hincrby(db, cmd_args),
        "HINCRBYFLOAT" => cmd_hincrbyfloat(db, cmd_args),
        "HSETNX" => cmd_hsetnx(db, cmd_args),
        // List operations
        "LPUSH" => cmd_lpush(db, cmd_args),
        "RPUSH" => cmd_rpush(db, cmd_args),
        "LPOP" => cmd_lpop(db, cmd_args),
        "RPOP" => cmd_rpop(db, cmd_args),
        "LLEN" => cmd_llen(db, cmd_args),
        "LRANGE" => cmd_lrange(db, cmd_args),
        "LINDEX" => cmd_lindex(db, cmd_args),
        "LSET" => cmd_lset(db, cmd_args),
        "LTRIM" => cmd_ltrim(db, cmd_args),
        _ => RespValue::error(format!("unknown command '{}'", cmd)),
    }
}

// --- Server commands ---

fn cmd_ping(args: &[Vec<u8>]) -> RespValue {
    if args.is_empty() {
        RespValue::pong()
    } else {
        RespValue::BulkString(Some(args[0].clone()))
    }
}

fn cmd_echo(args: &[Vec<u8>]) -> RespValue {
    if args.is_empty() {
        return RespValue::error("wrong number of arguments for 'echo' command");
    }
    RespValue::BulkString(Some(args[0].clone()))
}

fn cmd_command() -> RespValue {
    // Minimal implementation for client compatibility
    RespValue::Array(Some(vec![]))
}

// --- String commands ---

fn cmd_get(db: &Db, args: &[Vec<u8>]) -> RespValue {
    if args.is_empty() {
        return RespValue::error("wrong number of arguments for 'get' command");
    }

    let key = match std::str::from_utf8(&args[0]) {
        Ok(k) => k,
        Err(_) => return RespValue::error("invalid key"),
    };

    match db.get(key) {
        Ok(value) => value.into(),
        Err(KvError::WrongType) => RespValue::wrong_type(),
        Err(e) => RespValue::error(e.to_string()),
    }
}

fn cmd_set(db: &Db, args: &[Vec<u8>]) -> RespValue {
    if args.len() < 2 {
        return RespValue::error("wrong number of arguments for 'set' command");
    }

    let key = match std::str::from_utf8(&args[0]) {
        Ok(k) => k,
        Err(_) => return RespValue::error("invalid key"),
    };

    let value = &args[1];

    // Parse options (EX, PX, NX, XX) - basic support
    let mut ttl = None;
    let mut nx = false;
    let mut xx = false;
    let mut i = 2;

    while i < args.len() {
        let opt = String::from_utf8_lossy(&args[i]).to_uppercase();
        match opt.as_str() {
            "EX" => {
                i += 1;
                if i >= args.len() {
                    return RespValue::error("syntax error");
                }
                match std::str::from_utf8(&args[i])
                    .ok()
                    .and_then(|s| s.parse::<u64>().ok())
                {
                    Some(secs) => ttl = Some(std::time::Duration::from_secs(secs)),
                    None => return RespValue::error("value is not an integer or out of range"),
                }
            }
            "PX" => {
                i += 1;
                if i >= args.len() {
                    return RespValue::error("syntax error");
                }
                match std::str::from_utf8(&args[i])
                    .ok()
                    .and_then(|s| s.parse::<u64>().ok())
                {
                    Some(ms) => ttl = Some(std::time::Duration::from_millis(ms)),
                    None => return RespValue::error("value is not an integer or out of range"),
                }
            }
            "NX" => nx = true,
            "XX" => xx = true,
            _ => return RespValue::error("syntax error"),
        }
        i += 1;
    }

    let opts = crate::types::SetOptions { ttl, nx, xx };

    match db.set_opts(key, value, opts) {
        Ok(true) => RespValue::ok(),
        Ok(false) => RespValue::null(), // NX/XX condition not met
        Err(e) => RespValue::error(e.to_string()),
    }
}

fn cmd_del(db: &Db, args: &[Vec<u8>]) -> RespValue {
    if args.is_empty() {
        return RespValue::error("wrong number of arguments for 'del' command");
    }

    let keys: Vec<&str> = args
        .iter()
        .filter_map(|k| std::str::from_utf8(k).ok())
        .collect();

    if keys.len() != args.len() {
        return RespValue::error("invalid key");
    }

    match db.del(&keys) {
        Ok(count) => RespValue::Integer(count),
        Err(e) => RespValue::error(e.to_string()),
    }
}

// --- Key commands ---

fn cmd_type(db: &Db, args: &[Vec<u8>]) -> RespValue {
    if args.len() != 1 {
        return RespValue::error("wrong number of arguments for 'type' command");
    }

    let key = match std::str::from_utf8(&args[0]) {
        Ok(k) => k,
        Err(_) => return RespValue::error("invalid key"),
    };

    match db.key_type(key) {
        Ok(Some(kt)) => RespValue::SimpleString(kt.as_str().to_string()),
        Ok(None) => RespValue::SimpleString("none".to_string()),
        Err(e) => RespValue::error(e.to_string()),
    }
}

fn cmd_ttl(db: &Db, args: &[Vec<u8>]) -> RespValue {
    if args.len() != 1 {
        return RespValue::error("wrong number of arguments for 'ttl' command");
    }

    let key = match std::str::from_utf8(&args[0]) {
        Ok(k) => k,
        Err(_) => return RespValue::error("invalid key"),
    };

    match db.ttl(key) {
        Ok(ttl) => RespValue::Integer(ttl),
        Err(e) => RespValue::error(e.to_string()),
    }
}

fn cmd_pttl(db: &Db, args: &[Vec<u8>]) -> RespValue {
    if args.len() != 1 {
        return RespValue::error("wrong number of arguments for 'pttl' command");
    }

    let key = match std::str::from_utf8(&args[0]) {
        Ok(k) => k,
        Err(_) => return RespValue::error("invalid key"),
    };

    match db.pttl(key) {
        Ok(pttl) => RespValue::Integer(pttl),
        Err(e) => RespValue::error(e.to_string()),
    }
}

fn cmd_exists(db: &Db, args: &[Vec<u8>]) -> RespValue {
    if args.is_empty() {
        return RespValue::error("wrong number of arguments for 'exists' command");
    }

    let keys: Vec<&str> = args
        .iter()
        .filter_map(|k| std::str::from_utf8(k).ok())
        .collect();

    if keys.len() != args.len() {
        return RespValue::error("invalid key");
    }

    match db.exists(&keys) {
        Ok(count) => RespValue::Integer(count),
        Err(e) => RespValue::error(e.to_string()),
    }
}

fn cmd_expire(db: &Db, args: &[Vec<u8>]) -> RespValue {
    if args.len() != 2 {
        return RespValue::error("wrong number of arguments for 'expire' command");
    }

    let key = match std::str::from_utf8(&args[0]) {
        Ok(k) => k,
        Err(_) => return RespValue::error("invalid key"),
    };

    let seconds: i64 = match std::str::from_utf8(&args[1])
        .ok()
        .and_then(|s| s.parse().ok())
    {
        Some(s) => s,
        None => return RespValue::error("value is not an integer or out of range"),
    };

    match db.expire(key, seconds) {
        Ok(true) => RespValue::Integer(1),
        Ok(false) => RespValue::Integer(0),
        Err(e) => RespValue::error(e.to_string()),
    }
}

fn cmd_keys(db: &Db, args: &[Vec<u8>]) -> RespValue {
    if args.len() != 1 {
        return RespValue::error("wrong number of arguments for 'keys' command");
    }

    let pattern = match std::str::from_utf8(&args[0]) {
        Ok(p) => p,
        Err(_) => return RespValue::error("invalid pattern"),
    };

    match db.keys(pattern) {
        Ok(keys) => {
            let values: Vec<RespValue> = keys
                .into_iter()
                .map(|k| RespValue::BulkString(Some(k.into_bytes())))
                .collect();
            RespValue::Array(Some(values))
        }
        Err(e) => RespValue::error(e.to_string()),
    }
}

fn cmd_scan(db: &Db, args: &[Vec<u8>]) -> RespValue {
    if args.is_empty() {
        return RespValue::error("wrong number of arguments for 'scan' command");
    }

    // Parse cursor
    let cursor: u64 = match std::str::from_utf8(&args[0])
        .ok()
        .and_then(|s| s.parse().ok())
    {
        Some(c) => c,
        None => return RespValue::error("invalid cursor"),
    };

    // Parse optional MATCH and COUNT arguments
    let mut pattern: Option<&str> = None;
    let mut count: usize = 10; // Default count
    let mut i = 1;

    while i < args.len() {
        let opt = String::from_utf8_lossy(&args[i]).to_uppercase();
        match opt.as_str() {
            "MATCH" => {
                i += 1;
                if i >= args.len() {
                    return RespValue::error("syntax error");
                }
                match std::str::from_utf8(&args[i]) {
                    Ok(p) => pattern = Some(p),
                    Err(_) => return RespValue::error("invalid pattern"),
                }
            }
            "COUNT" => {
                i += 1;
                if i >= args.len() {
                    return RespValue::error("syntax error");
                }
                match std::str::from_utf8(&args[i])
                    .ok()
                    .and_then(|s| s.parse().ok())
                {
                    Some(c) => count = c,
                    None => return RespValue::error("value is not an integer or out of range"),
                }
            }
            _ => return RespValue::error("syntax error"),
        }
        i += 1;
    }

    match db.scan(cursor, pattern, count) {
        Ok((next_cursor, keys)) => {
            let keys_array: Vec<RespValue> = keys
                .into_iter()
                .map(|k| RespValue::BulkString(Some(k.into_bytes())))
                .collect();
            RespValue::Array(Some(vec![
                RespValue::BulkString(Some(next_cursor.to_string().into_bytes())),
                RespValue::Array(Some(keys_array)),
            ]))
        }
        Err(e) => RespValue::error(e.to_string()),
    }
}

// --- String operations ---

fn cmd_incr(db: &Db, args: &[Vec<u8>]) -> RespValue {
    if args.len() != 1 {
        return RespValue::error("wrong number of arguments for 'incr' command");
    }

    let key = match std::str::from_utf8(&args[0]) {
        Ok(k) => k,
        Err(_) => return RespValue::error("invalid key"),
    };

    match db.incr(key) {
        Ok(val) => RespValue::Integer(val),
        Err(e) => RespValue::error(e.to_string()),
    }
}

fn cmd_decr(db: &Db, args: &[Vec<u8>]) -> RespValue {
    if args.len() != 1 {
        return RespValue::error("wrong number of arguments for 'decr' command");
    }

    let key = match std::str::from_utf8(&args[0]) {
        Ok(k) => k,
        Err(_) => return RespValue::error("invalid key"),
    };

    match db.decr(key) {
        Ok(val) => RespValue::Integer(val),
        Err(e) => RespValue::error(e.to_string()),
    }
}

fn cmd_incrby(db: &Db, args: &[Vec<u8>]) -> RespValue {
    if args.len() != 2 {
        return RespValue::error("wrong number of arguments for 'incrby' command");
    }

    let key = match std::str::from_utf8(&args[0]) {
        Ok(k) => k,
        Err(_) => return RespValue::error("invalid key"),
    };

    let increment: i64 = match std::str::from_utf8(&args[1])
        .ok()
        .and_then(|s| s.parse().ok())
    {
        Some(i) => i,
        None => return RespValue::error("value is not an integer or out of range"),
    };

    match db.incrby(key, increment) {
        Ok(val) => RespValue::Integer(val),
        Err(e) => RespValue::error(e.to_string()),
    }
}

fn cmd_decrby(db: &Db, args: &[Vec<u8>]) -> RespValue {
    if args.len() != 2 {
        return RespValue::error("wrong number of arguments for 'decrby' command");
    }

    let key = match std::str::from_utf8(&args[0]) {
        Ok(k) => k,
        Err(_) => return RespValue::error("invalid key"),
    };

    let decrement: i64 = match std::str::from_utf8(&args[1])
        .ok()
        .and_then(|s| s.parse().ok())
    {
        Some(i) => i,
        None => return RespValue::error("value is not an integer or out of range"),
    };

    match db.decrby(key, decrement) {
        Ok(val) => RespValue::Integer(val),
        Err(e) => RespValue::error(e.to_string()),
    }
}

fn cmd_incrbyfloat(db: &Db, args: &[Vec<u8>]) -> RespValue {
    if args.len() != 2 {
        return RespValue::error("wrong number of arguments for 'incrbyfloat' command");
    }

    let key = match std::str::from_utf8(&args[0]) {
        Ok(k) => k,
        Err(_) => return RespValue::error("invalid key"),
    };

    let increment: f64 = match std::str::from_utf8(&args[1])
        .ok()
        .and_then(|s| s.parse().ok())
    {
        Some(f) => f,
        None => return RespValue::error("value is not a valid float"),
    };

    match db.incrbyfloat(key, increment) {
        Ok(val) => RespValue::BulkString(Some(val.into_bytes())),
        Err(e) => RespValue::error(e.to_string()),
    }
}

fn cmd_mget(db: &Db, args: &[Vec<u8>]) -> RespValue {
    if args.is_empty() {
        return RespValue::error("wrong number of arguments for 'mget' command");
    }

    let keys: Vec<&str> = args
        .iter()
        .filter_map(|k| std::str::from_utf8(k).ok())
        .collect();

    if keys.len() != args.len() {
        return RespValue::error("invalid key");
    }

    let values = db.mget(&keys);
    let resp_values: Vec<RespValue> = values
        .into_iter()
        .map(|v| match v {
            Some(bytes) => RespValue::BulkString(Some(bytes)),
            None => RespValue::null(),
        })
        .collect();

    RespValue::Array(Some(resp_values))
}

fn cmd_mset(db: &Db, args: &[Vec<u8>]) -> RespValue {
    if args.len() < 2 || args.len() % 2 != 0 {
        return RespValue::error("wrong number of arguments for 'mset' command");
    }

    let mut pairs: Vec<(&str, &[u8])> = Vec::new();
    for i in (0..args.len()).step_by(2) {
        let key = match std::str::from_utf8(&args[i]) {
            Ok(k) => k,
            Err(_) => return RespValue::error("invalid key"),
        };
        pairs.push((key, &args[i + 1]));
    }

    match db.mset(&pairs) {
        Ok(()) => RespValue::ok(),
        Err(e) => RespValue::error(e.to_string()),
    }
}

fn cmd_append(db: &Db, args: &[Vec<u8>]) -> RespValue {
    if args.len() != 2 {
        return RespValue::error("wrong number of arguments for 'append' command");
    }

    let key = match std::str::from_utf8(&args[0]) {
        Ok(k) => k,
        Err(_) => return RespValue::error("invalid key"),
    };

    match db.append(key, &args[1]) {
        Ok(len) => RespValue::Integer(len),
        Err(e) => RespValue::error(e.to_string()),
    }
}

fn cmd_strlen(db: &Db, args: &[Vec<u8>]) -> RespValue {
    if args.len() != 1 {
        return RespValue::error("wrong number of arguments for 'strlen' command");
    }

    let key = match std::str::from_utf8(&args[0]) {
        Ok(k) => k,
        Err(_) => return RespValue::error("invalid key"),
    };

    match db.strlen(key) {
        Ok(len) => RespValue::Integer(len),
        Err(e) => RespValue::error(e.to_string()),
    }
}

fn cmd_getrange(db: &Db, args: &[Vec<u8>]) -> RespValue {
    if args.len() != 3 {
        return RespValue::error("wrong number of arguments for 'getrange' command");
    }

    let key = match std::str::from_utf8(&args[0]) {
        Ok(k) => k,
        Err(_) => return RespValue::error("invalid key"),
    };

    let start: i64 = match std::str::from_utf8(&args[1])
        .ok()
        .and_then(|s| s.parse().ok())
    {
        Some(i) => i,
        None => return RespValue::error("value is not an integer or out of range"),
    };

    let end: i64 = match std::str::from_utf8(&args[2])
        .ok()
        .and_then(|s| s.parse().ok())
    {
        Some(i) => i,
        None => return RespValue::error("value is not an integer or out of range"),
    };

    match db.getrange(key, start, end) {
        Ok(bytes) => RespValue::BulkString(Some(bytes)),
        Err(e) => RespValue::error(e.to_string()),
    }
}

fn cmd_setrange(db: &Db, args: &[Vec<u8>]) -> RespValue {
    if args.len() != 3 {
        return RespValue::error("wrong number of arguments for 'setrange' command");
    }

    let key = match std::str::from_utf8(&args[0]) {
        Ok(k) => k,
        Err(_) => return RespValue::error("invalid key"),
    };

    let offset: i64 = match std::str::from_utf8(&args[1])
        .ok()
        .and_then(|s| s.parse().ok())
    {
        Some(i) => i,
        None => return RespValue::error("value is not an integer or out of range"),
    };

    match db.setrange(key, offset, &args[2]) {
        Ok(len) => RespValue::Integer(len),
        Err(e) => RespValue::error(e.to_string()),
    }
}

// --- Hash operations ---

fn cmd_hset(db: &Db, args: &[Vec<u8>]) -> RespValue {
    if args.len() < 3 || args.len() % 2 == 0 {
        return RespValue::error("wrong number of arguments for 'hset' command");
    }

    let key = match std::str::from_utf8(&args[0]) {
        Ok(k) => k,
        Err(_) => return RespValue::error("invalid key"),
    };

    let mut pairs: Vec<(&str, &[u8])> = Vec::new();
    for i in (1..args.len()).step_by(2) {
        let field = match std::str::from_utf8(&args[i]) {
            Ok(f) => f,
            Err(_) => return RespValue::error("invalid field"),
        };
        pairs.push((field, &args[i + 1]));
    }

    match db.hset(key, &pairs) {
        Ok(count) => RespValue::Integer(count),
        Err(KvError::WrongType) => RespValue::wrong_type(),
        Err(e) => RespValue::error(e.to_string()),
    }
}

fn cmd_hget(db: &Db, args: &[Vec<u8>]) -> RespValue {
    if args.len() != 2 {
        return RespValue::error("wrong number of arguments for 'hget' command");
    }

    let key = match std::str::from_utf8(&args[0]) {
        Ok(k) => k,
        Err(_) => return RespValue::error("invalid key"),
    };

    let field = match std::str::from_utf8(&args[1]) {
        Ok(f) => f,
        Err(_) => return RespValue::error("invalid field"),
    };

    match db.hget(key, field) {
        Ok(value) => value.into(),
        Err(KvError::WrongType) => RespValue::wrong_type(),
        Err(e) => RespValue::error(e.to_string()),
    }
}

fn cmd_hmget(db: &Db, args: &[Vec<u8>]) -> RespValue {
    if args.len() < 2 {
        return RespValue::error("wrong number of arguments for 'hmget' command");
    }

    let key = match std::str::from_utf8(&args[0]) {
        Ok(k) => k,
        Err(_) => return RespValue::error("invalid key"),
    };

    let fields: Vec<&str> = match args[1..]
        .iter()
        .map(|f| std::str::from_utf8(f))
        .collect::<std::result::Result<Vec<_>, _>>()
    {
        Ok(f) => f,
        Err(_) => return RespValue::error("invalid field"),
    };

    match db.hmget(key, &fields) {
        Ok(values) => {
            let resp_values: Vec<RespValue> = values
                .into_iter()
                .map(|v| match v {
                    Some(bytes) => RespValue::BulkString(Some(bytes)),
                    None => RespValue::null(),
                })
                .collect();
            RespValue::Array(Some(resp_values))
        }
        Err(KvError::WrongType) => RespValue::wrong_type(),
        Err(e) => RespValue::error(e.to_string()),
    }
}

fn cmd_hgetall(db: &Db, args: &[Vec<u8>]) -> RespValue {
    if args.len() != 1 {
        return RespValue::error("wrong number of arguments for 'hgetall' command");
    }

    let key = match std::str::from_utf8(&args[0]) {
        Ok(k) => k,
        Err(_) => return RespValue::error("invalid key"),
    };

    match db.hgetall(key) {
        Ok(pairs) => {
            let mut resp_values: Vec<RespValue> = Vec::with_capacity(pairs.len() * 2);
            for (field, value) in pairs {
                resp_values.push(RespValue::BulkString(Some(field.into_bytes())));
                resp_values.push(RespValue::BulkString(Some(value)));
            }
            RespValue::Array(Some(resp_values))
        }
        Err(KvError::WrongType) => RespValue::wrong_type(),
        Err(e) => RespValue::error(e.to_string()),
    }
}

fn cmd_hdel(db: &Db, args: &[Vec<u8>]) -> RespValue {
    if args.len() < 2 {
        return RespValue::error("wrong number of arguments for 'hdel' command");
    }

    let key = match std::str::from_utf8(&args[0]) {
        Ok(k) => k,
        Err(_) => return RespValue::error("invalid key"),
    };

    let fields: Vec<&str> = match args[1..]
        .iter()
        .map(|f| std::str::from_utf8(f))
        .collect::<std::result::Result<Vec<_>, _>>()
    {
        Ok(f) => f,
        Err(_) => return RespValue::error("invalid field"),
    };

    match db.hdel(key, &fields) {
        Ok(count) => RespValue::Integer(count),
        Err(KvError::WrongType) => RespValue::wrong_type(),
        Err(e) => RespValue::error(e.to_string()),
    }
}

fn cmd_hexists(db: &Db, args: &[Vec<u8>]) -> RespValue {
    if args.len() != 2 {
        return RespValue::error("wrong number of arguments for 'hexists' command");
    }

    let key = match std::str::from_utf8(&args[0]) {
        Ok(k) => k,
        Err(_) => return RespValue::error("invalid key"),
    };

    let field = match std::str::from_utf8(&args[1]) {
        Ok(f) => f,
        Err(_) => return RespValue::error("invalid field"),
    };

    match db.hexists(key, field) {
        Ok(true) => RespValue::Integer(1),
        Ok(false) => RespValue::Integer(0),
        Err(KvError::WrongType) => RespValue::wrong_type(),
        Err(e) => RespValue::error(e.to_string()),
    }
}

fn cmd_hkeys(db: &Db, args: &[Vec<u8>]) -> RespValue {
    if args.len() != 1 {
        return RespValue::error("wrong number of arguments for 'hkeys' command");
    }

    let key = match std::str::from_utf8(&args[0]) {
        Ok(k) => k,
        Err(_) => return RespValue::error("invalid key"),
    };

    match db.hkeys(key) {
        Ok(keys) => {
            let resp_values: Vec<RespValue> = keys
                .into_iter()
                .map(|k| RespValue::BulkString(Some(k.into_bytes())))
                .collect();
            RespValue::Array(Some(resp_values))
        }
        Err(KvError::WrongType) => RespValue::wrong_type(),
        Err(e) => RespValue::error(e.to_string()),
    }
}

fn cmd_hvals(db: &Db, args: &[Vec<u8>]) -> RespValue {
    if args.len() != 1 {
        return RespValue::error("wrong number of arguments for 'hvals' command");
    }

    let key = match std::str::from_utf8(&args[0]) {
        Ok(k) => k,
        Err(_) => return RespValue::error("invalid key"),
    };

    match db.hvals(key) {
        Ok(vals) => {
            let resp_values: Vec<RespValue> = vals
                .into_iter()
                .map(|v| RespValue::BulkString(Some(v)))
                .collect();
            RespValue::Array(Some(resp_values))
        }
        Err(KvError::WrongType) => RespValue::wrong_type(),
        Err(e) => RespValue::error(e.to_string()),
    }
}

fn cmd_hlen(db: &Db, args: &[Vec<u8>]) -> RespValue {
    if args.len() != 1 {
        return RespValue::error("wrong number of arguments for 'hlen' command");
    }

    let key = match std::str::from_utf8(&args[0]) {
        Ok(k) => k,
        Err(_) => return RespValue::error("invalid key"),
    };

    match db.hlen(key) {
        Ok(len) => RespValue::Integer(len),
        Err(KvError::WrongType) => RespValue::wrong_type(),
        Err(e) => RespValue::error(e.to_string()),
    }
}

fn cmd_hincrby(db: &Db, args: &[Vec<u8>]) -> RespValue {
    if args.len() != 3 {
        return RespValue::error("wrong number of arguments for 'hincrby' command");
    }

    let key = match std::str::from_utf8(&args[0]) {
        Ok(k) => k,
        Err(_) => return RespValue::error("invalid key"),
    };

    let field = match std::str::from_utf8(&args[1]) {
        Ok(f) => f,
        Err(_) => return RespValue::error("invalid field"),
    };

    let increment: i64 = match std::str::from_utf8(&args[2])
        .ok()
        .and_then(|s| s.parse().ok())
    {
        Some(i) => i,
        None => return RespValue::error("value is not an integer or out of range"),
    };

    match db.hincrby(key, field, increment) {
        Ok(val) => RespValue::Integer(val),
        Err(KvError::WrongType) => RespValue::wrong_type(),
        Err(e) => RespValue::error(e.to_string()),
    }
}

fn cmd_hincrbyfloat(db: &Db, args: &[Vec<u8>]) -> RespValue {
    if args.len() != 3 {
        return RespValue::error("wrong number of arguments for 'hincrbyfloat' command");
    }

    let key = match std::str::from_utf8(&args[0]) {
        Ok(k) => k,
        Err(_) => return RespValue::error("invalid key"),
    };

    let field = match std::str::from_utf8(&args[1]) {
        Ok(f) => f,
        Err(_) => return RespValue::error("invalid field"),
    };

    let increment: f64 = match std::str::from_utf8(&args[2])
        .ok()
        .and_then(|s| s.parse().ok())
    {
        Some(f) => f,
        None => return RespValue::error("value is not a valid float"),
    };

    match db.hincrbyfloat(key, field, increment) {
        Ok(val) => RespValue::BulkString(Some(val.into_bytes())),
        Err(KvError::WrongType) => RespValue::wrong_type(),
        Err(e) => RespValue::error(e.to_string()),
    }
}

fn cmd_hsetnx(db: &Db, args: &[Vec<u8>]) -> RespValue {
    if args.len() != 3 {
        return RespValue::error("wrong number of arguments for 'hsetnx' command");
    }

    let key = match std::str::from_utf8(&args[0]) {
        Ok(k) => k,
        Err(_) => return RespValue::error("invalid key"),
    };

    let field = match std::str::from_utf8(&args[1]) {
        Ok(f) => f,
        Err(_) => return RespValue::error("invalid field"),
    };

    match db.hsetnx(key, field, &args[2]) {
        Ok(true) => RespValue::Integer(1),
        Ok(false) => RespValue::Integer(0),
        Err(KvError::WrongType) => RespValue::wrong_type(),
        Err(e) => RespValue::error(e.to_string()),
    }
}

// --- List operations ---

fn cmd_lpush(db: &Db, args: &[Vec<u8>]) -> RespValue {
    if args.len() < 2 {
        return RespValue::error("wrong number of arguments for 'lpush' command");
    }

    let key = match std::str::from_utf8(&args[0]) {
        Ok(k) => k,
        Err(_) => return RespValue::error("invalid key"),
    };

    let values: Vec<&[u8]> = args[1..].iter().map(|v| v.as_slice()).collect();

    match db.lpush(key, &values) {
        Ok(len) => RespValue::Integer(len),
        Err(KvError::WrongType) => RespValue::wrong_type(),
        Err(e) => RespValue::error(e.to_string()),
    }
}

fn cmd_rpush(db: &Db, args: &[Vec<u8>]) -> RespValue {
    if args.len() < 2 {
        return RespValue::error("wrong number of arguments for 'rpush' command");
    }

    let key = match std::str::from_utf8(&args[0]) {
        Ok(k) => k,
        Err(_) => return RespValue::error("invalid key"),
    };

    let values: Vec<&[u8]> = args[1..].iter().map(|v| v.as_slice()).collect();

    match db.rpush(key, &values) {
        Ok(len) => RespValue::Integer(len),
        Err(KvError::WrongType) => RespValue::wrong_type(),
        Err(e) => RespValue::error(e.to_string()),
    }
}

fn cmd_lpop(db: &Db, args: &[Vec<u8>]) -> RespValue {
    if args.is_empty() || args.len() > 2 {
        return RespValue::error("wrong number of arguments for 'lpop' command");
    }

    let key = match std::str::from_utf8(&args[0]) {
        Ok(k) => k,
        Err(_) => return RespValue::error("invalid key"),
    };

    let count: Option<usize> = if args.len() == 2 {
        match std::str::from_utf8(&args[1])
            .ok()
            .and_then(|s| s.parse().ok())
        {
            Some(c) => Some(c),
            None => return RespValue::error("value is not an integer or out of range"),
        }
    } else {
        None
    };

    match db.lpop(key, count) {
        Ok(values) => {
            if args.len() == 1 {
                // Single pop - return single value or nil
                if values.is_empty() {
                    RespValue::null()
                } else {
                    RespValue::BulkString(Some(values.into_iter().next().unwrap()))
                }
            } else {
                // Count specified - return array
                if values.is_empty() {
                    RespValue::null()
                } else {
                    RespValue::Array(Some(
                        values
                            .into_iter()
                            .map(|v| RespValue::BulkString(Some(v)))
                            .collect(),
                    ))
                }
            }
        }
        Err(KvError::WrongType) => RespValue::wrong_type(),
        Err(e) => RespValue::error(e.to_string()),
    }
}

fn cmd_rpop(db: &Db, args: &[Vec<u8>]) -> RespValue {
    if args.is_empty() || args.len() > 2 {
        return RespValue::error("wrong number of arguments for 'rpop' command");
    }

    let key = match std::str::from_utf8(&args[0]) {
        Ok(k) => k,
        Err(_) => return RespValue::error("invalid key"),
    };

    let count: Option<usize> = if args.len() == 2 {
        match std::str::from_utf8(&args[1])
            .ok()
            .and_then(|s| s.parse().ok())
        {
            Some(c) => Some(c),
            None => return RespValue::error("value is not an integer or out of range"),
        }
    } else {
        None
    };

    match db.rpop(key, count) {
        Ok(values) => {
            if args.len() == 1 {
                // Single pop - return single value or nil
                if values.is_empty() {
                    RespValue::null()
                } else {
                    RespValue::BulkString(Some(values.into_iter().next().unwrap()))
                }
            } else {
                // Count specified - return array
                if values.is_empty() {
                    RespValue::null()
                } else {
                    RespValue::Array(Some(
                        values
                            .into_iter()
                            .map(|v| RespValue::BulkString(Some(v)))
                            .collect(),
                    ))
                }
            }
        }
        Err(KvError::WrongType) => RespValue::wrong_type(),
        Err(e) => RespValue::error(e.to_string()),
    }
}

fn cmd_llen(db: &Db, args: &[Vec<u8>]) -> RespValue {
    if args.len() != 1 {
        return RespValue::error("wrong number of arguments for 'llen' command");
    }

    let key = match std::str::from_utf8(&args[0]) {
        Ok(k) => k,
        Err(_) => return RespValue::error("invalid key"),
    };

    match db.llen(key) {
        Ok(len) => RespValue::Integer(len),
        Err(KvError::WrongType) => RespValue::wrong_type(),
        Err(e) => RespValue::error(e.to_string()),
    }
}

fn cmd_lrange(db: &Db, args: &[Vec<u8>]) -> RespValue {
    if args.len() != 3 {
        return RespValue::error("wrong number of arguments for 'lrange' command");
    }

    let key = match std::str::from_utf8(&args[0]) {
        Ok(k) => k,
        Err(_) => return RespValue::error("invalid key"),
    };

    let start: i64 = match std::str::from_utf8(&args[1])
        .ok()
        .and_then(|s| s.parse().ok())
    {
        Some(i) => i,
        None => return RespValue::error("value is not an integer or out of range"),
    };

    let stop: i64 = match std::str::from_utf8(&args[2])
        .ok()
        .and_then(|s| s.parse().ok())
    {
        Some(i) => i,
        None => return RespValue::error("value is not an integer or out of range"),
    };

    match db.lrange(key, start, stop) {
        Ok(values) => RespValue::Array(Some(
            values
                .into_iter()
                .map(|v| RespValue::BulkString(Some(v)))
                .collect(),
        )),
        Err(KvError::WrongType) => RespValue::wrong_type(),
        Err(e) => RespValue::error(e.to_string()),
    }
}

fn cmd_lindex(db: &Db, args: &[Vec<u8>]) -> RespValue {
    if args.len() != 2 {
        return RespValue::error("wrong number of arguments for 'lindex' command");
    }

    let key = match std::str::from_utf8(&args[0]) {
        Ok(k) => k,
        Err(_) => return RespValue::error("invalid key"),
    };

    let index: i64 = match std::str::from_utf8(&args[1])
        .ok()
        .and_then(|s| s.parse().ok())
    {
        Some(i) => i,
        None => return RespValue::error("value is not an integer or out of range"),
    };

    match db.lindex(key, index) {
        Ok(value) => value.into(),
        Err(KvError::WrongType) => RespValue::wrong_type(),
        Err(e) => RespValue::error(e.to_string()),
    }
}

fn cmd_lset(db: &Db, args: &[Vec<u8>]) -> RespValue {
    if args.len() != 3 {
        return RespValue::error("wrong number of arguments for 'lset' command");
    }

    let key = match std::str::from_utf8(&args[0]) {
        Ok(k) => k,
        Err(_) => return RespValue::error("invalid key"),
    };

    let index: i64 = match std::str::from_utf8(&args[1])
        .ok()
        .and_then(|s| s.parse().ok())
    {
        Some(i) => i,
        None => return RespValue::error("value is not an integer or out of range"),
    };

    match db.lset(key, index, &args[2]) {
        Ok(()) => RespValue::ok(),
        Err(KvError::WrongType) => RespValue::wrong_type(),
        Err(KvError::NoSuchKey) => RespValue::error("no such key"),
        Err(KvError::OutOfRange) => RespValue::error("index out of range"),
        Err(e) => RespValue::error(e.to_string()),
    }
}

fn cmd_ltrim(db: &Db, args: &[Vec<u8>]) -> RespValue {
    if args.len() != 3 {
        return RespValue::error("wrong number of arguments for 'ltrim' command");
    }

    let key = match std::str::from_utf8(&args[0]) {
        Ok(k) => k,
        Err(_) => return RespValue::error("invalid key"),
    };

    let start: i64 = match std::str::from_utf8(&args[1])
        .ok()
        .and_then(|s| s.parse().ok())
    {
        Some(i) => i,
        None => return RespValue::error("value is not an integer or out of range"),
    };

    let stop: i64 = match std::str::from_utf8(&args[2])
        .ok()
        .and_then(|s| s.parse().ok())
    {
        Some(i) => i,
        None => return RespValue::error("value is not an integer or out of range"),
    };

    match db.ltrim(key, start, stop) {
        Ok(()) => RespValue::ok(),
        Err(KvError::WrongType) => RespValue::wrong_type(),
        Err(e) => RespValue::error(e.to_string()),
    }
}
