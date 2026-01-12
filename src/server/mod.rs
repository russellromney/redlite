use tokio::io::AsyncWriteExt;
use tokio::net::{TcpListener, TcpStream};

use crate::db::Db;
use crate::error::KvError;
use crate::resp::{RespReader, RespValue};
use crate::types::ZMember;

pub struct Server {
    db: Db,
}

impl Server {
    pub fn new(db: Db) -> Self {
        Self { db }
    }

    pub async fn run(&self, addr: &str) -> std::io::Result<()> {
        let listener = TcpListener::bind(addr).await?;
        tracing::info!("Redlite listening on {}", addr);

        loop {
            let (socket, peer_addr) = listener.accept().await?;
            tracing::debug!("Connection from {}", peer_addr);

            // Create a new session for this connection
            let session = self.db.session();

            tokio::spawn(async move {
                if let Err(e) = handle_connection(socket, session).await {
                    tracing::error!("Connection error: {}", e);
                }
            });
        }
    }
}

async fn handle_connection(socket: TcpStream, mut db: Db) -> std::io::Result<()> {
    let (reader, mut writer) = socket.into_split();
    let mut reader = RespReader::new(reader);

    loop {
        match reader.read_command().await? {
            Some(args) => {
                let response = execute_command(&mut db, &args);
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

fn execute_command(db: &mut Db, args: &[Vec<u8>]) -> RespValue {
    if args.is_empty() {
        return RespValue::error("empty command");
    }

    let cmd = String::from_utf8_lossy(&args[0]).to_uppercase();
    let cmd_args = &args[1..];

    match cmd.as_str() {
        // Server commands
        "PING" => cmd_ping(cmd_args),
        "ECHO" => cmd_echo(cmd_args),
        "COMMAND" => cmd_command(),
        "QUIT" => RespValue::ok(),
        "SELECT" => cmd_select(db, cmd_args),
        "DBSIZE" => cmd_dbsize(db),
        "FLUSHDB" => cmd_flushdb(db),
        "INFO" => cmd_info(db, cmd_args),
        // String commands
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
        // Set operations
        "SADD" => cmd_sadd(db, cmd_args),
        "SREM" => cmd_srem(db, cmd_args),
        "SMEMBERS" => cmd_smembers(db, cmd_args),
        "SISMEMBER" => cmd_sismember(db, cmd_args),
        "SCARD" => cmd_scard(db, cmd_args),
        "SPOP" => cmd_spop(db, cmd_args),
        "SRANDMEMBER" => cmd_srandmember(db, cmd_args),
        "SDIFF" => cmd_sdiff(db, cmd_args),
        "SINTER" => cmd_sinter(db, cmd_args),
        "SUNION" => cmd_sunion(db, cmd_args),
        // Sorted set operations
        "ZADD" => cmd_zadd(db, cmd_args),
        "ZREM" => cmd_zrem(db, cmd_args),
        "ZSCORE" => cmd_zscore(db, cmd_args),
        "ZRANK" => cmd_zrank(db, cmd_args),
        "ZREVRANK" => cmd_zrevrank(db, cmd_args),
        "ZCARD" => cmd_zcard(db, cmd_args),
        "ZRANGE" => cmd_zrange(db, cmd_args),
        "ZREVRANGE" => cmd_zrevrange(db, cmd_args),
        "ZRANGEBYSCORE" => cmd_zrangebyscore(db, cmd_args),
        "ZCOUNT" => cmd_zcount(db, cmd_args),
        "ZINCRBY" => cmd_zincrby(db, cmd_args),
        "ZREMRANGEBYRANK" => cmd_zremrangebyrank(db, cmd_args),
        "ZREMRANGEBYSCORE" => cmd_zremrangebyscore(db, cmd_args),
        // Custom commands
        "VACUUM" => cmd_vacuum(db),
        "KEYINFO" => cmd_keyinfo(db, cmd_args),
        "AUTOVACUUM" => cmd_autovacuum(db, cmd_args),
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

fn cmd_select(db: &mut Db, args: &[Vec<u8>]) -> RespValue {
    if args.len() != 1 {
        return RespValue::error("wrong number of arguments for 'select' command");
    }

    let db_index: i32 = match std::str::from_utf8(&args[0])
        .ok()
        .and_then(|s| s.parse().ok())
    {
        Some(i) => i,
        None => return RespValue::error("value is not an integer or out of range"),
    };

    match db.select(db_index) {
        Ok(()) => RespValue::ok(),
        Err(_) => RespValue::error("DB index is out of range"),
    }
}

fn cmd_dbsize(db: &Db) -> RespValue {
    match db.dbsize() {
        Ok(count) => RespValue::Integer(count),
        Err(e) => RespValue::error(e.to_string()),
    }
}

fn cmd_flushdb(db: &Db) -> RespValue {
    match db.flushdb() {
        Ok(()) => RespValue::ok(),
        Err(e) => RespValue::error(e.to_string()),
    }
}

fn cmd_info(db: &Db, args: &[Vec<u8>]) -> RespValue {
    // Parse optional section argument
    let section = if args.is_empty() {
        None
    } else {
        std::str::from_utf8(&args[0]).ok()
    };

    let mut info = String::new();

    // Server section
    if section.is_none() || section == Some("server") {
        info.push_str("# Server\r\n");
        info.push_str("redis_version:7.0.0-redlite\r\n");
        info.push_str("redis_mode:standalone\r\n");
        info.push_str("\r\n");
    }

    // Keyspace section
    if section.is_none() || section == Some("keyspace") {
        info.push_str("# Keyspace\r\n");
        // Show keys count for current database
        if let Ok(count) = db.dbsize() {
            let db_num = db.current_db();
            info.push_str(&format!("db{}:keys={},expires=0\r\n", db_num, count));
        }
        info.push_str("\r\n");
    }

    RespValue::BulkString(Some(info.into_bytes()))
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

// --- Set operations ---

fn cmd_sadd(db: &Db, args: &[Vec<u8>]) -> RespValue {
    if args.len() < 2 {
        return RespValue::error("wrong number of arguments for 'sadd' command");
    }

    let key = match std::str::from_utf8(&args[0]) {
        Ok(k) => k,
        Err(_) => return RespValue::error("invalid key"),
    };

    let members: Vec<&[u8]> = args[1..].iter().map(|v| v.as_slice()).collect();

    match db.sadd(key, &members) {
        Ok(count) => RespValue::Integer(count),
        Err(KvError::WrongType) => RespValue::wrong_type(),
        Err(e) => RespValue::error(e.to_string()),
    }
}

fn cmd_srem(db: &Db, args: &[Vec<u8>]) -> RespValue {
    if args.len() < 2 {
        return RespValue::error("wrong number of arguments for 'srem' command");
    }

    let key = match std::str::from_utf8(&args[0]) {
        Ok(k) => k,
        Err(_) => return RespValue::error("invalid key"),
    };

    let members: Vec<&[u8]> = args[1..].iter().map(|v| v.as_slice()).collect();

    match db.srem(key, &members) {
        Ok(count) => RespValue::Integer(count),
        Err(KvError::WrongType) => RespValue::wrong_type(),
        Err(e) => RespValue::error(e.to_string()),
    }
}

fn cmd_smembers(db: &Db, args: &[Vec<u8>]) -> RespValue {
    if args.len() != 1 {
        return RespValue::error("wrong number of arguments for 'smembers' command");
    }

    let key = match std::str::from_utf8(&args[0]) {
        Ok(k) => k,
        Err(_) => return RespValue::error("invalid key"),
    };

    match db.smembers(key) {
        Ok(members) => RespValue::Array(Some(
            members
                .into_iter()
                .map(|m| RespValue::BulkString(Some(m)))
                .collect(),
        )),
        Err(KvError::WrongType) => RespValue::wrong_type(),
        Err(e) => RespValue::error(e.to_string()),
    }
}

fn cmd_sismember(db: &Db, args: &[Vec<u8>]) -> RespValue {
    if args.len() != 2 {
        return RespValue::error("wrong number of arguments for 'sismember' command");
    }

    let key = match std::str::from_utf8(&args[0]) {
        Ok(k) => k,
        Err(_) => return RespValue::error("invalid key"),
    };

    match db.sismember(key, &args[1]) {
        Ok(true) => RespValue::Integer(1),
        Ok(false) => RespValue::Integer(0),
        Err(KvError::WrongType) => RespValue::wrong_type(),
        Err(e) => RespValue::error(e.to_string()),
    }
}

fn cmd_scard(db: &Db, args: &[Vec<u8>]) -> RespValue {
    if args.len() != 1 {
        return RespValue::error("wrong number of arguments for 'scard' command");
    }

    let key = match std::str::from_utf8(&args[0]) {
        Ok(k) => k,
        Err(_) => return RespValue::error("invalid key"),
    };

    match db.scard(key) {
        Ok(count) => RespValue::Integer(count),
        Err(KvError::WrongType) => RespValue::wrong_type(),
        Err(e) => RespValue::error(e.to_string()),
    }
}

fn cmd_spop(db: &Db, args: &[Vec<u8>]) -> RespValue {
    if args.is_empty() || args.len() > 2 {
        return RespValue::error("wrong number of arguments for 'spop' command");
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

    match db.spop(key, count) {
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
                RespValue::Array(Some(
                    values
                        .into_iter()
                        .map(|v| RespValue::BulkString(Some(v)))
                        .collect(),
                ))
            }
        }
        Err(KvError::WrongType) => RespValue::wrong_type(),
        Err(e) => RespValue::error(e.to_string()),
    }
}

fn cmd_srandmember(db: &Db, args: &[Vec<u8>]) -> RespValue {
    if args.is_empty() || args.len() > 2 {
        return RespValue::error("wrong number of arguments for 'srandmember' command");
    }

    let key = match std::str::from_utf8(&args[0]) {
        Ok(k) => k,
        Err(_) => return RespValue::error("invalid key"),
    };

    let count: Option<i64> = if args.len() == 2 {
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

    match db.srandmember(key, count) {
        Ok(values) => {
            if args.len() == 1 {
                // No count - return single value or nil
                if values.is_empty() {
                    RespValue::null()
                } else {
                    RespValue::BulkString(Some(values.into_iter().next().unwrap()))
                }
            } else {
                // Count specified - return array
                RespValue::Array(Some(
                    values
                        .into_iter()
                        .map(|v| RespValue::BulkString(Some(v)))
                        .collect(),
                ))
            }
        }
        Err(KvError::WrongType) => RespValue::wrong_type(),
        Err(e) => RespValue::error(e.to_string()),
    }
}

fn cmd_sdiff(db: &Db, args: &[Vec<u8>]) -> RespValue {
    if args.is_empty() {
        return RespValue::error("wrong number of arguments for 'sdiff' command");
    }

    let keys: Vec<&str> = match args
        .iter()
        .map(|k| std::str::from_utf8(k))
        .collect::<std::result::Result<Vec<_>, _>>()
    {
        Ok(k) => k,
        Err(_) => return RespValue::error("invalid key"),
    };

    match db.sdiff(&keys) {
        Ok(members) => RespValue::Array(Some(
            members
                .into_iter()
                .map(|m| RespValue::BulkString(Some(m)))
                .collect(),
        )),
        Err(KvError::WrongType) => RespValue::wrong_type(),
        Err(e) => RespValue::error(e.to_string()),
    }
}

fn cmd_sinter(db: &Db, args: &[Vec<u8>]) -> RespValue {
    if args.is_empty() {
        return RespValue::error("wrong number of arguments for 'sinter' command");
    }

    let keys: Vec<&str> = match args
        .iter()
        .map(|k| std::str::from_utf8(k))
        .collect::<std::result::Result<Vec<_>, _>>()
    {
        Ok(k) => k,
        Err(_) => return RespValue::error("invalid key"),
    };

    match db.sinter(&keys) {
        Ok(members) => RespValue::Array(Some(
            members
                .into_iter()
                .map(|m| RespValue::BulkString(Some(m)))
                .collect(),
        )),
        Err(KvError::WrongType) => RespValue::wrong_type(),
        Err(e) => RespValue::error(e.to_string()),
    }
}

fn cmd_sunion(db: &Db, args: &[Vec<u8>]) -> RespValue {
    if args.is_empty() {
        return RespValue::error("wrong number of arguments for 'sunion' command");
    }

    let keys: Vec<&str> = match args
        .iter()
        .map(|k| std::str::from_utf8(k))
        .collect::<std::result::Result<Vec<_>, _>>()
    {
        Ok(k) => k,
        Err(_) => return RespValue::error("invalid key"),
    };

    match db.sunion(&keys) {
        Ok(members) => RespValue::Array(Some(
            members
                .into_iter()
                .map(|m| RespValue::BulkString(Some(m)))
                .collect(),
        )),
        Err(KvError::WrongType) => RespValue::wrong_type(),
        Err(e) => RespValue::error(e.to_string()),
    }
}

// --- Session 9: Sorted Set command handlers ---

fn cmd_zadd(db: &Db, args: &[Vec<u8>]) -> RespValue {
    if args.len() < 3 || (args.len() - 1) % 2 != 0 {
        return RespValue::error("wrong number of arguments for 'zadd' command");
    }

    let key = match std::str::from_utf8(&args[0]) {
        Ok(k) => k,
        Err(_) => return RespValue::error("invalid key"),
    };

    // Parse score-member pairs
    let mut members = Vec::new();
    let mut i = 1;
    while i < args.len() {
        let score_str = match std::str::from_utf8(&args[i]) {
            Ok(s) => s,
            Err(_) => return RespValue::error("invalid score"),
        };
        let score: f64 = match score_str.parse() {
            Ok(s) => s,
            Err(_) => return RespValue::error("value is not a valid float"),
        };
        let member = args[i + 1].clone();
        members.push(ZMember::new(score, member));
        i += 2;
    }

    match db.zadd(key, &members) {
        Ok(count) => RespValue::Integer(count),
        Err(KvError::WrongType) => RespValue::wrong_type(),
        Err(e) => RespValue::error(e.to_string()),
    }
}

fn cmd_zrem(db: &Db, args: &[Vec<u8>]) -> RespValue {
    if args.len() < 2 {
        return RespValue::error("wrong number of arguments for 'zrem' command");
    }

    let key = match std::str::from_utf8(&args[0]) {
        Ok(k) => k,
        Err(_) => return RespValue::error("invalid key"),
    };

    let members: Vec<&[u8]> = args[1..].iter().map(|v| v.as_slice()).collect();

    match db.zrem(key, &members) {
        Ok(count) => RespValue::Integer(count),
        Err(KvError::WrongType) => RespValue::wrong_type(),
        Err(e) => RespValue::error(e.to_string()),
    }
}

fn cmd_zscore(db: &Db, args: &[Vec<u8>]) -> RespValue {
    if args.len() != 2 {
        return RespValue::error("wrong number of arguments for 'zscore' command");
    }

    let key = match std::str::from_utf8(&args[0]) {
        Ok(k) => k,
        Err(_) => return RespValue::error("invalid key"),
    };

    match db.zscore(key, &args[1]) {
        Ok(Some(score)) => RespValue::BulkString(Some(score.to_string().into_bytes())),
        Ok(None) => RespValue::BulkString(None),
        Err(KvError::WrongType) => RespValue::wrong_type(),
        Err(e) => RespValue::error(e.to_string()),
    }
}

fn cmd_zrank(db: &Db, args: &[Vec<u8>]) -> RespValue {
    if args.len() != 2 {
        return RespValue::error("wrong number of arguments for 'zrank' command");
    }

    let key = match std::str::from_utf8(&args[0]) {
        Ok(k) => k,
        Err(_) => return RespValue::error("invalid key"),
    };

    match db.zrank(key, &args[1]) {
        Ok(Some(rank)) => RespValue::Integer(rank),
        Ok(None) => RespValue::BulkString(None),
        Err(KvError::WrongType) => RespValue::wrong_type(),
        Err(e) => RespValue::error(e.to_string()),
    }
}

fn cmd_zrevrank(db: &Db, args: &[Vec<u8>]) -> RespValue {
    if args.len() != 2 {
        return RespValue::error("wrong number of arguments for 'zrevrank' command");
    }

    let key = match std::str::from_utf8(&args[0]) {
        Ok(k) => k,
        Err(_) => return RespValue::error("invalid key"),
    };

    match db.zrevrank(key, &args[1]) {
        Ok(Some(rank)) => RespValue::Integer(rank),
        Ok(None) => RespValue::BulkString(None),
        Err(KvError::WrongType) => RespValue::wrong_type(),
        Err(e) => RespValue::error(e.to_string()),
    }
}

fn cmd_zcard(db: &Db, args: &[Vec<u8>]) -> RespValue {
    if args.len() != 1 {
        return RespValue::error("wrong number of arguments for 'zcard' command");
    }

    let key = match std::str::from_utf8(&args[0]) {
        Ok(k) => k,
        Err(_) => return RespValue::error("invalid key"),
    };

    match db.zcard(key) {
        Ok(count) => RespValue::Integer(count),
        Err(KvError::WrongType) => RespValue::wrong_type(),
        Err(e) => RespValue::error(e.to_string()),
    }
}

fn cmd_zrange(db: &Db, args: &[Vec<u8>]) -> RespValue {
    if args.len() < 3 || args.len() > 4 {
        return RespValue::error("wrong number of arguments for 'zrange' command");
    }

    let key = match std::str::from_utf8(&args[0]) {
        Ok(k) => k,
        Err(_) => return RespValue::error("invalid key"),
    };

    let start: i64 = match std::str::from_utf8(&args[1]).ok().and_then(|s| s.parse().ok()) {
        Some(v) => v,
        None => return RespValue::error("value is not an integer or out of range"),
    };

    let stop: i64 = match std::str::from_utf8(&args[2]).ok().and_then(|s| s.parse().ok()) {
        Some(v) => v,
        None => return RespValue::error("value is not an integer or out of range"),
    };

    let with_scores = args.len() == 4
        && std::str::from_utf8(&args[3])
            .map(|s| s.eq_ignore_ascii_case("WITHSCORES"))
            .unwrap_or(false);

    match db.zrange(key, start, stop, with_scores) {
        Ok(members) => {
            if with_scores {
                let mut result = Vec::new();
                for m in members {
                    result.push(RespValue::BulkString(Some(m.member)));
                    result.push(RespValue::BulkString(Some(m.score.to_string().into_bytes())));
                }
                RespValue::Array(Some(result))
            } else {
                RespValue::Array(Some(
                    members
                        .into_iter()
                        .map(|m| RespValue::BulkString(Some(m.member)))
                        .collect(),
                ))
            }
        }
        Err(KvError::WrongType) => RespValue::wrong_type(),
        Err(e) => RespValue::error(e.to_string()),
    }
}

fn cmd_zrevrange(db: &Db, args: &[Vec<u8>]) -> RespValue {
    if args.len() < 3 || args.len() > 4 {
        return RespValue::error("wrong number of arguments for 'zrevrange' command");
    }

    let key = match std::str::from_utf8(&args[0]) {
        Ok(k) => k,
        Err(_) => return RespValue::error("invalid key"),
    };

    let start: i64 = match std::str::from_utf8(&args[1]).ok().and_then(|s| s.parse().ok()) {
        Some(v) => v,
        None => return RespValue::error("value is not an integer or out of range"),
    };

    let stop: i64 = match std::str::from_utf8(&args[2]).ok().and_then(|s| s.parse().ok()) {
        Some(v) => v,
        None => return RespValue::error("value is not an integer or out of range"),
    };

    let with_scores = args.len() == 4
        && std::str::from_utf8(&args[3])
            .map(|s| s.eq_ignore_ascii_case("WITHSCORES"))
            .unwrap_or(false);

    match db.zrevrange(key, start, stop, with_scores) {
        Ok(members) => {
            if with_scores {
                let mut result = Vec::new();
                for m in members {
                    result.push(RespValue::BulkString(Some(m.member)));
                    result.push(RespValue::BulkString(Some(m.score.to_string().into_bytes())));
                }
                RespValue::Array(Some(result))
            } else {
                RespValue::Array(Some(
                    members
                        .into_iter()
                        .map(|m| RespValue::BulkString(Some(m.member)))
                        .collect(),
                ))
            }
        }
        Err(KvError::WrongType) => RespValue::wrong_type(),
        Err(e) => RespValue::error(e.to_string()),
    }
}

fn cmd_zrangebyscore(db: &Db, args: &[Vec<u8>]) -> RespValue {
    if args.len() < 3 {
        return RespValue::error("wrong number of arguments for 'zrangebyscore' command");
    }

    let key = match std::str::from_utf8(&args[0]) {
        Ok(k) => k,
        Err(_) => return RespValue::error("invalid key"),
    };

    // Parse min score (supports -inf)
    let min_str = match std::str::from_utf8(&args[1]) {
        Ok(s) => s,
        Err(_) => return RespValue::error("invalid min score"),
    };
    let min: f64 = if min_str.eq_ignore_ascii_case("-inf") {
        f64::NEG_INFINITY
    } else {
        match min_str.parse() {
            Ok(v) => v,
            Err(_) => return RespValue::error("min is not a float"),
        }
    };

    // Parse max score (supports +inf)
    let max_str = match std::str::from_utf8(&args[2]) {
        Ok(s) => s,
        Err(_) => return RespValue::error("invalid max score"),
    };
    let max: f64 = if max_str.eq_ignore_ascii_case("+inf") || max_str.eq_ignore_ascii_case("inf") {
        f64::INFINITY
    } else {
        match max_str.parse() {
            Ok(v) => v,
            Err(_) => return RespValue::error("max is not a float"),
        }
    };

    // Parse optional LIMIT offset count
    let mut offset: Option<i64> = None;
    let mut count: Option<i64> = None;

    let mut i = 3;
    while i < args.len() {
        let opt = match std::str::from_utf8(&args[i]) {
            Ok(s) => s,
            Err(_) => return RespValue::error("invalid option"),
        };
        if opt.eq_ignore_ascii_case("LIMIT") {
            if i + 2 >= args.len() {
                return RespValue::error("syntax error");
            }
            offset = match std::str::from_utf8(&args[i + 1]).ok().and_then(|s| s.parse().ok()) {
                Some(v) => Some(v),
                None => return RespValue::error("value is not an integer or out of range"),
            };
            count = match std::str::from_utf8(&args[i + 2]).ok().and_then(|s| s.parse().ok()) {
                Some(v) => Some(v),
                None => return RespValue::error("value is not an integer or out of range"),
            };
            i += 3;
        } else {
            i += 1;
        }
    }

    match db.zrangebyscore(key, min, max, offset, count) {
        Ok(members) => RespValue::Array(Some(
            members
                .into_iter()
                .map(|m| RespValue::BulkString(Some(m.member)))
                .collect(),
        )),
        Err(KvError::WrongType) => RespValue::wrong_type(),
        Err(e) => RespValue::error(e.to_string()),
    }
}

fn cmd_zcount(db: &Db, args: &[Vec<u8>]) -> RespValue {
    if args.len() != 3 {
        return RespValue::error("wrong number of arguments for 'zcount' command");
    }

    let key = match std::str::from_utf8(&args[0]) {
        Ok(k) => k,
        Err(_) => return RespValue::error("invalid key"),
    };

    // Parse min score (supports -inf)
    let min_str = match std::str::from_utf8(&args[1]) {
        Ok(s) => s,
        Err(_) => return RespValue::error("invalid min score"),
    };
    let min: f64 = if min_str.eq_ignore_ascii_case("-inf") {
        f64::NEG_INFINITY
    } else {
        match min_str.parse() {
            Ok(v) => v,
            Err(_) => return RespValue::error("min is not a float"),
        }
    };

    // Parse max score (supports +inf)
    let max_str = match std::str::from_utf8(&args[2]) {
        Ok(s) => s,
        Err(_) => return RespValue::error("invalid max score"),
    };
    let max: f64 = if max_str.eq_ignore_ascii_case("+inf") || max_str.eq_ignore_ascii_case("inf") {
        f64::INFINITY
    } else {
        match max_str.parse() {
            Ok(v) => v,
            Err(_) => return RespValue::error("max is not a float"),
        }
    };

    match db.zcount(key, min, max) {
        Ok(count) => RespValue::Integer(count),
        Err(KvError::WrongType) => RespValue::wrong_type(),
        Err(e) => RespValue::error(e.to_string()),
    }
}

fn cmd_zincrby(db: &Db, args: &[Vec<u8>]) -> RespValue {
    if args.len() != 3 {
        return RespValue::error("wrong number of arguments for 'zincrby' command");
    }

    let key = match std::str::from_utf8(&args[0]) {
        Ok(k) => k,
        Err(_) => return RespValue::error("invalid key"),
    };

    let increment: f64 = match std::str::from_utf8(&args[1]).ok().and_then(|s| s.parse().ok()) {
        Some(v) => v,
        None => return RespValue::error("value is not a valid float"),
    };

    match db.zincrby(key, increment, &args[2]) {
        Ok(new_score) => RespValue::BulkString(Some(new_score.to_string().into_bytes())),
        Err(KvError::WrongType) => RespValue::wrong_type(),
        Err(e) => RespValue::error(e.to_string()),
    }
}

fn cmd_zremrangebyrank(db: &Db, args: &[Vec<u8>]) -> RespValue {
    if args.len() != 3 {
        return RespValue::error("wrong number of arguments for 'zremrangebyrank' command");
    }

    let key = match std::str::from_utf8(&args[0]) {
        Ok(k) => k,
        Err(_) => return RespValue::error("invalid key"),
    };

    let start: i64 = match std::str::from_utf8(&args[1]).ok().and_then(|s| s.parse().ok()) {
        Some(v) => v,
        None => return RespValue::error("value is not an integer or out of range"),
    };

    let stop: i64 = match std::str::from_utf8(&args[2]).ok().and_then(|s| s.parse().ok()) {
        Some(v) => v,
        None => return RespValue::error("value is not an integer or out of range"),
    };

    match db.zremrangebyrank(key, start, stop) {
        Ok(count) => RespValue::Integer(count),
        Err(KvError::WrongType) => RespValue::wrong_type(),
        Err(e) => RespValue::error(e.to_string()),
    }
}

fn cmd_zremrangebyscore(db: &Db, args: &[Vec<u8>]) -> RespValue {
    if args.len() != 3 {
        return RespValue::error("wrong number of arguments for 'zremrangebyscore' command");
    }

    let key = match std::str::from_utf8(&args[0]) {
        Ok(k) => k,
        Err(_) => return RespValue::error("invalid key"),
    };

    // Parse min score (supports -inf)
    let min_str = match std::str::from_utf8(&args[1]) {
        Ok(s) => s,
        Err(_) => return RespValue::error("invalid min score"),
    };
    let min: f64 = if min_str.eq_ignore_ascii_case("-inf") {
        f64::NEG_INFINITY
    } else {
        match min_str.parse() {
            Ok(v) => v,
            Err(_) => return RespValue::error("min is not a float"),
        }
    };

    // Parse max score (supports +inf)
    let max_str = match std::str::from_utf8(&args[2]) {
        Ok(s) => s,
        Err(_) => return RespValue::error("invalid max score"),
    };
    let max: f64 = if max_str.eq_ignore_ascii_case("+inf") || max_str.eq_ignore_ascii_case("inf") {
        f64::INFINITY
    } else {
        match max_str.parse() {
            Ok(v) => v,
            Err(_) => return RespValue::error("max is not a float"),
        }
    };

    match db.zremrangebyscore(key, min, max) {
        Ok(count) => RespValue::Integer(count),
        Err(KvError::WrongType) => RespValue::wrong_type(),
        Err(e) => RespValue::error(e.to_string()),
    }
}

// --- Session 11: Custom command handlers ---

fn cmd_vacuum(db: &Db) -> RespValue {
    match db.vacuum() {
        Ok(deleted) => RespValue::Integer(deleted),
        Err(e) => RespValue::error(e.to_string()),
    }
}

fn cmd_keyinfo(db: &Db, args: &[Vec<u8>]) -> RespValue {
    if args.len() != 1 {
        return RespValue::error("wrong number of arguments for 'keyinfo' command");
    }

    let key = match std::str::from_utf8(&args[0]) {
        Ok(k) => k,
        Err(_) => return RespValue::error("invalid key"),
    };

    match db.keyinfo(key) {
        Ok(Some(info)) => {
            // Return as array of field-value pairs (like HGETALL)
            RespValue::Array(Some(vec![
                RespValue::BulkString(Some(b"type".to_vec())),
                RespValue::BulkString(Some(info.key_type.as_str().as_bytes().to_vec())),
                RespValue::BulkString(Some(b"ttl".to_vec())),
                RespValue::Integer(info.ttl),
                RespValue::BulkString(Some(b"created_at".to_vec())),
                RespValue::Integer(info.created_at),
                RespValue::BulkString(Some(b"updated_at".to_vec())),
                RespValue::Integer(info.updated_at),
            ]))
        }
        Ok(None) => RespValue::null(),
        Err(e) => RespValue::error(e.to_string()),
    }
}

fn cmd_autovacuum(db: &Db, args: &[Vec<u8>]) -> RespValue {
    if args.is_empty() {
        // Return current state: enabled and interval
        let enabled = if db.autovacuum_enabled() { "ON" } else { "OFF" };
        let interval = db.autovacuum_interval();
        return RespValue::Array(Some(vec![
            RespValue::BulkString(Some(b"enabled".to_vec())),
            RespValue::BulkString(Some(enabled.as_bytes().to_vec())),
            RespValue::BulkString(Some(b"interval_ms".to_vec())),
            RespValue::Integer(interval),
        ]));
    }

    let arg = match std::str::from_utf8(&args[0]) {
        Ok(s) => s.to_uppercase(),
        Err(_) => return RespValue::error("invalid argument"),
    };

    match arg.as_str() {
        "ON" | "1" | "TRUE" => {
            db.set_autovacuum(true);
            RespValue::ok()
        }
        "OFF" | "0" | "FALSE" => {
            db.set_autovacuum(false);
            RespValue::ok()
        }
        "INTERVAL" => {
            // AUTOVACUUM INTERVAL <ms>
            if args.len() != 2 {
                return RespValue::error("AUTOVACUUM INTERVAL requires a value in milliseconds");
            }
            let interval_str = match std::str::from_utf8(&args[1]) {
                Ok(s) => s,
                Err(_) => return RespValue::error("invalid interval value"),
            };
            match interval_str.parse::<i64>() {
                Ok(ms) => {
                    db.set_autovacuum_interval(ms);
                    RespValue::ok()
                }
                Err(_) => RespValue::error("interval must be an integer (milliseconds)"),
            }
        }
        _ => RespValue::error("argument must be ON, OFF, or INTERVAL <ms>"),
    }
}
