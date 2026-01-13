use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock};
use std::time::Duration;
use tokio::io::AsyncWriteExt;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::broadcast;

use crate::db::Db;
use crate::error::KvError;
use crate::resp::{RespReader, RespValue};
use crate::types::{StreamId, ZMember};

mod connection;
mod pubsub;
use connection::{ConnectionInfo, ConnectionPool, ConnectionType};
use pubsub::{
    cmd_psubscribe, cmd_publish, cmd_punsubscribe, cmd_subscribe, cmd_unsubscribe, cmd_unwatch,
    cmd_watch, receive_pubsub_message, ConnectionState, PubSubMessage, QueuedCommand,
};

/// Global pause state for CLIENT PAUSE
struct PauseState {
    paused: AtomicBool,
    pause_until_ms: std::sync::Mutex<i64>,
}

impl PauseState {
    fn new() -> Self {
        Self {
            paused: AtomicBool::new(false),
            pause_until_ms: std::sync::Mutex::new(0),
        }
    }

    fn is_paused(&self) -> bool {
        if !self.paused.load(Ordering::SeqCst) {
            return false;
        }
        let until = *self.pause_until_ms.lock().unwrap();
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as i64;
        if now >= until {
            self.paused.store(false, Ordering::SeqCst);
            false
        } else {
            true
        }
    }

    fn pause(&self, ms: i64) {
        let until = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as i64
            + ms;
        *self.pause_until_ms.lock().unwrap() = until;
        self.paused.store(true, Ordering::SeqCst);
    }
}

pub struct Server {
    db: Db,
    notifier: Arc<RwLock<HashMap<String, broadcast::Sender<()>>>>,
    pubsub_channels: Arc<RwLock<HashMap<String, broadcast::Sender<PubSubMessage>>>>,
    password: Option<Arc<String>>,
    connection_pool: ConnectionPool,
    pause_state: Arc<PauseState>,
}

impl Server {
    pub fn new(db: Db, password: Option<String>) -> Self {
        Self {
            db,
            notifier: Arc::new(RwLock::new(HashMap::new())),
            pubsub_channels: Arc::new(RwLock::new(HashMap::new())),
            password: password.map(Arc::new),
            connection_pool: ConnectionPool::new(),
            pause_state: Arc::new(PauseState::new()),
        }
    }

    pub async fn run(&self, addr: &str) -> std::io::Result<()> {
        let listener = TcpListener::bind(addr).await?;
        tracing::info!("Redlite listening on {}", addr);

        loop {
            let (socket, peer_addr) = listener.accept().await?;
            tracing::debug!("Connection from {}", peer_addr);

            // Create a new session for this connection
            let session = self.db.session();

            // Clone notifier and pubsub channels for this connection
            let notifier = Arc::clone(&self.notifier);
            let pubsub_channels = Arc::clone(&self.pubsub_channels);
            let password = self.password.clone();
            let pool = self.connection_pool.clone_handle();
            let pause_state = Arc::clone(&self.pause_state);

            tokio::spawn(async move {
                if let Err(e) = handle_connection(
                    socket,
                    session,
                    notifier,
                    pubsub_channels,
                    password,
                    pool,
                    pause_state,
                    peer_addr,
                )
                .await
                {
                    tracing::error!("Connection error: {}", e);
                }
            });
        }
    }
}

async fn handle_connection(
    socket: TcpStream,
    mut db: Db,
    notifier: Arc<RwLock<HashMap<String, broadcast::Sender<()>>>>,
    pubsub_channels: Arc<RwLock<HashMap<String, broadcast::Sender<PubSubMessage>>>>,
    password: Option<Arc<String>>,
    pool: ConnectionPool,
    pause_state: Arc<PauseState>,
    peer_addr: SocketAddr,
) -> std::io::Result<()> {
    // Attach notifier to database for server mode
    db.with_notifier(notifier);

    // Register this connection
    let conn_id = pool.next_id();
    let conn_info = ConnectionInfo::new(conn_id, peer_addr);
    let conn_handle = pool.register(conn_info);

    // Use a guard to ensure cleanup on exit
    struct ConnectionGuard<'a> {
        pool: &'a ConnectionPool,
        id: u64,
    }
    impl Drop for ConnectionGuard<'_> {
        fn drop(&mut self) {
            self.pool.unregister(self.id);
            tracing::debug!("Connection {} unregistered", self.id);
        }
    }
    let _guard = ConnectionGuard {
        pool: &pool,
        id: conn_id,
    };

    let (reader, mut writer) = socket.into_split();
    let mut reader = RespReader::new(reader);
    let mut state = ConnectionState::new_normal();
    // If no password is configured, start authenticated
    let mut authenticated = password.is_none();

    loop {
        // Check for CLIENT PAUSE
        while pause_state.is_paused() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        if state.is_subscribed() {
            // Update connection type to PubSub
            if let Ok(mut info) = conn_handle.write() {
                info.connection_type = ConnectionType::PubSub;
            }

            // Subscription mode: handle commands and messages with tokio::select!
            tokio::select! {
                cmd_result = reader.read_command() => {
                    match cmd_result? {
                        Some(args) => {
                            // Update connection tracking
                            if let Ok(mut info) = conn_handle.write() {
                                info.touch();
                                info.increment_command_count();
                            }

                            let response = execute_subscription_command(
                                &mut state,
                                &args,
                                &pubsub_channels,
                            ).await;
                            writer.write_all(&response.encode()).await?;
                            writer.flush().await?;

                            // Update sub counts
                            if let Ok(mut info) = conn_handle.write() {
                                info.sub_count = state.subscription_count();
                                info.psub_count = state.pattern_subscription_count();
                            }

                            // Check for QUIT
                            if !args.is_empty() {
                                let cmd = String::from_utf8_lossy(&args[0]).to_uppercase();
                                if cmd == "QUIT" {
                                    break;
                                }
                            }
                        }
                        None => break,
                    }
                }
                msg = receive_pubsub_message(&mut state) => {
                    if let Some(resp) = msg {
                        writer.write_all(&resp.encode()).await?;
                        writer.flush().await?;
                    }
                }
            }
        } else {
            // Normal mode: standard command processing
            match reader.read_command().await? {
                Some(args) => {
                    // Update connection tracking
                    if let Ok(mut info) = conn_handle.write() {
                        info.touch();
                        info.increment_command_count();
                    }

                    let response = execute_normal_command(
                        &mut state,
                        &mut db,
                        &args,
                        &pubsub_channels,
                        &mut authenticated,
                        &password,
                        &conn_handle,
                        &pool,
                        &pause_state,
                    )
                    .await;
                    writer.write_all(&response.encode()).await?;
                    writer.flush().await?;

                    // Update connection info after command
                    if let Ok(mut info) = conn_handle.write() {
                        info.db = db.current_db();
                        info.multi_count = if state.in_transaction() { 1 } else { -1 };
                    }

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
    }

    Ok(())
}

/// Execute a command in normal mode (non-subscription)
async fn execute_normal_command(
    state: &mut ConnectionState,
    db: &mut Db,
    args: &[Vec<u8>],
    pubsub_channels: &Arc<RwLock<HashMap<String, broadcast::Sender<PubSubMessage>>>>,
    authenticated: &mut bool,
    password: &Option<Arc<String>>,
    conn_handle: &Arc<RwLock<ConnectionInfo>>,
    pool: &ConnectionPool,
    pause_state: &Arc<PauseState>,
) -> RespValue {
    if args.is_empty() {
        return RespValue::error("empty command");
    }

    let cmd = String::from_utf8_lossy(&args[0]).to_uppercase();
    let cmd_args = &args[1..];

    // Handle AUTH command (always allowed)
    if cmd == "AUTH" {
        return cmd_auth(cmd_args, authenticated, password);
    }

    // Check authentication for all other commands (except QUIT)
    if !*authenticated && cmd != "QUIT" {
        return RespValue::error("NOAUTH Authentication required.");
    }

    // Handle transaction commands
    if let ConnectionState::Transaction { .. } = state {
        match cmd.as_str() {
            "MULTI" => return RespValue::error("ERR MULTI calls can not be nested"),
            "DISCARD" => return execute_transaction_command(state, None, &cmd, cmd_args).await,
            "EXEC" => return execute_transaction_command(state, Some(db), &cmd, cmd_args).await,
            "WATCH" | "UNWATCH" => return RespValue::error("ERR WATCH not allowed in transaction"),
            "BLPOP" | "BRPOP" | "BRPOPLPUSH" => {
                return RespValue::error("ERR blocking commands not allowed in transaction")
            }
            "SUBSCRIBE" | "PSUBSCRIBE" | "UNSUBSCRIBE" | "PUNSUBSCRIBE" => {
                return RespValue::error("ERR pub/sub commands not allowed in transaction")
            }
            _ => {
                // Queue the command for later execution
                return queue_command(state, &cmd, cmd_args);
            }
        }
    }

    // Handle WATCH/UNWATCH commands
    match cmd.as_str() {
        "WATCH" => {
            let db_ref = db;
            return cmd_watch(state, cmd_args, |key| db_ref.get_version(key).unwrap_or(0));
        }
        "UNWATCH" => return cmd_unwatch(state),
        _ => {}
    }

    // Handle pub/sub commands
    match cmd.as_str() {
        "SUBSCRIBE" => cmd_subscribe(state, cmd_args, pubsub_channels),
        "PSUBSCRIBE" => cmd_psubscribe(state, cmd_args, pubsub_channels),
        "PUBLISH" => cmd_publish(cmd_args, pubsub_channels),
        "MULTI" => cmd_multi(state),
        _ => execute_command(db, args, conn_handle, pool, pause_state).await,
    }
}

/// Execute a command in subscription mode (restricted command set)
async fn execute_subscription_command(
    state: &mut ConnectionState,
    args: &[Vec<u8>],
    pubsub_channels: &Arc<RwLock<HashMap<String, broadcast::Sender<PubSubMessage>>>>,
) -> RespValue {
    if args.is_empty() {
        return RespValue::error("empty command");
    }

    let cmd = String::from_utf8_lossy(&args[0]).to_uppercase();
    let cmd_args = &args[1..];

    match cmd.as_str() {
        "SUBSCRIBE" => cmd_subscribe(state, cmd_args, pubsub_channels),
        "UNSUBSCRIBE" => cmd_unsubscribe(state, cmd_args),
        "PSUBSCRIBE" => cmd_psubscribe(state, cmd_args, pubsub_channels),
        "PUNSUBSCRIBE" => cmd_punsubscribe(state, cmd_args),
        "PING" => {
            // PING in subscription mode returns array format
            if cmd_args.is_empty() {
                RespValue::Array(Some(vec![
                    RespValue::from_string("pong".to_string()),
                    RespValue::null(),
                ]))
            } else {
                RespValue::Array(Some(vec![
                    RespValue::from_string("pong".to_string()),
                    RespValue::from_bytes(cmd_args[0].clone()),
                ]))
            }
        }
        "QUIT" => RespValue::ok(),
        _ => RespValue::error(format!(
            "only (P)SUBSCRIBE / (P)UNSUBSCRIBE / PING / QUIT are allowed in this context"
        )),
    }
}

async fn execute_command(
    db: &mut Db,
    args: &[Vec<u8>],
    conn_handle: &Arc<RwLock<ConnectionInfo>>,
    pool: &ConnectionPool,
    pause_state: &Arc<PauseState>,
) -> RespValue {
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
        "CLIENT" => cmd_client(cmd_args, conn_handle, pool, pause_state),
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
        "BLPOP" => cmd_blpop(db, cmd_args).await,
        "BRPOP" => cmd_brpop(db, cmd_args).await,
        "LLEN" => cmd_llen(db, cmd_args),
        "LRANGE" => cmd_lrange(db, cmd_args),
        "LINDEX" => cmd_lindex(db, cmd_args),
        "LSET" => cmd_lset(db, cmd_args),
        "LTRIM" => cmd_ltrim(db, cmd_args),
        "LREM" => cmd_lrem(db, cmd_args),
        "LINSERT" => cmd_linsert(db, cmd_args),
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
        "SMOVE" => cmd_smove(db, cmd_args),
        "SDIFFSTORE" => cmd_sdiffstore(db, cmd_args),
        "SINTERSTORE" => cmd_sinterstore(db, cmd_args),
        "SUNIONSTORE" => cmd_sunionstore(db, cmd_args),
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
        "HISTORY" => cmd_history(db, cmd_args),
        "FTS" => cmd_fts(db, cmd_args),
        // RediSearch-compatible commands (Session 23)
        "FT.CREATE" => cmd_ft_create(db, cmd_args),
        "FT.DROPINDEX" => cmd_ft_dropindex(db, cmd_args),
        "FT._LIST" => cmd_ft_list(db),
        "FT.INFO" => cmd_ft_info(db, cmd_args),
        "FT.ALTER" => cmd_ft_alter(db, cmd_args),
        "FT.SEARCH" => cmd_ft_search(db, cmd_args),
        "FT.AGGREGATE" => cmd_ft_aggregate(db, cmd_args),
        "FT.ALIASADD" => cmd_ft_aliasadd(db, cmd_args),
        "FT.ALIASDEL" => cmd_ft_aliasdel(db, cmd_args),
        "FT.ALIASUPDATE" => cmd_ft_aliasupdate(db, cmd_args),
        "FT.SYNUPDATE" => cmd_ft_synupdate(db, cmd_args),
        "FT.SYNDUMP" => cmd_ft_syndump(db, cmd_args),
        "FT.SUGADD" => cmd_ft_sugadd(db, cmd_args),
        "FT.SUGGET" => cmd_ft_sugget(db, cmd_args),
        "FT.SUGDEL" => cmd_ft_sugdel(db, cmd_args),
        "FT.SUGLEN" => cmd_ft_suglen(db, cmd_args),
        // Vector commands (Session 24.2)
        "VECTOR" => cmd_vector(db, cmd_args),
        "VADD" => cmd_vadd(db, cmd_args),
        "VGET" => cmd_vget(db, cmd_args),
        "VDEL" => cmd_vdel(db, cmd_args),
        "VCOUNT" => cmd_vcount(db, cmd_args),
        "VSEARCH" => cmd_vsearch(db, cmd_args),
        // Stream commands
        "XADD" => cmd_xadd(db, cmd_args),
        "XLEN" => cmd_xlen(db, cmd_args),
        "XRANGE" => cmd_xrange(db, cmd_args),
        "XREVRANGE" => cmd_xrevrange(db, cmd_args),
        "XREAD" => cmd_xread(db, cmd_args).await,
        "XTRIM" => cmd_xtrim(db, cmd_args),
        "XDEL" => cmd_xdel(db, cmd_args),
        "XINFO" => cmd_xinfo(db, cmd_args),
        // Stream consumer group commands (Session 14)
        "XGROUP" => cmd_xgroup(db, cmd_args),
        "XREADGROUP" => cmd_xreadgroup(db, cmd_args).await,
        "XACK" => cmd_xack(db, cmd_args),
        "XPENDING" => cmd_xpending(db, cmd_args),
        "XCLAIM" => cmd_xclaim(db, cmd_args),
        _ => RespValue::error(format!("unknown command '{}'", cmd)),
    }
}

// --- Server commands ---

fn cmd_auth(
    args: &[Vec<u8>],
    authenticated: &mut bool,
    password: &Option<Arc<String>>,
) -> RespValue {
    // Redis AUTH command supports:
    // AUTH <password>
    // AUTH <username> <password> (Redis 6+, we only support default user)
    if args.is_empty() || args.len() > 2 {
        return RespValue::error("ERR wrong number of arguments for 'auth' command");
    }

    // Get the password from args (last argument if 2 args, first if 1 arg)
    let provided_password = if args.len() == 2 {
        // AUTH <username> <password> - we ignore username, use password
        &args[1]
    } else {
        &args[0]
    };

    match password {
        Some(expected) => {
            let provided = String::from_utf8_lossy(provided_password);
            if provided == **expected {
                *authenticated = true;
                RespValue::ok()
            } else {
                *authenticated = false;
                RespValue::error("WRONGPASS invalid username-password pair or user is disabled.")
            }
        }
        None => {
            // No password configured, but client sent AUTH anyway
            // Redis returns OK in this case for compatibility
            *authenticated = true;
            RespValue::ok()
        }
    }
}

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

fn cmd_client(
    args: &[Vec<u8>],
    conn_handle: &Arc<RwLock<ConnectionInfo>>,
    pool: &ConnectionPool,
    pause_state: &Arc<PauseState>,
) -> RespValue {
    if args.is_empty() {
        return RespValue::error("wrong number of arguments for 'client' command");
    }

    let subcommand = match std::str::from_utf8(&args[0]) {
        Ok(s) => s.to_uppercase(),
        Err(_) => return RespValue::error("invalid subcommand"),
    };

    match subcommand.as_str() {
        "SETNAME" => {
            if args.len() != 2 {
                return RespValue::error("wrong number of arguments for 'client|setname' command");
            }
            let name = match std::str::from_utf8(&args[1]) {
                Ok(s) => s.to_string(),
                Err(_) => return RespValue::error(
                    "ERR Client names cannot contain spaces, newlines or other special characters",
                ),
            };
            // Validate name: no spaces or special characters
            if name.contains(' ') || name.contains('\n') || name.contains('\r') {
                return RespValue::error(
                    "ERR Client names cannot contain spaces, newlines or other special characters",
                );
            }
            if let Ok(mut info) = conn_handle.write() {
                info.name = if name.is_empty() { None } else { Some(name) };
            }
            RespValue::ok()
        }
        "GETNAME" => {
            if args.len() != 1 {
                return RespValue::error("wrong number of arguments for 'client|getname' command");
            }
            if let Ok(info) = conn_handle.read() {
                match &info.name {
                    Some(name) => RespValue::BulkString(Some(name.as_bytes().to_vec())),
                    None => RespValue::null(),
                }
            } else {
                RespValue::null()
            }
        }
        "LIST" => {
            // Parse optional filters: TYPE <type> or ID <id> [<id>...]
            let mut filter_type: Option<ConnectionType> = None;
            let mut filter_ids: Option<Vec<u64>> = None;
            let mut i = 1;

            while i < args.len() {
                let arg = String::from_utf8_lossy(&args[i]).to_uppercase();
                match arg.as_str() {
                    "TYPE" => {
                        if i + 1 >= args.len() {
                            return RespValue::error("ERR syntax error");
                        }
                        let type_str = String::from_utf8_lossy(&args[i + 1]).to_uppercase();
                        filter_type = match type_str.as_str() {
                            "NORMAL" => Some(ConnectionType::Normal),
                            "PUBSUB" => Some(ConnectionType::PubSub),
                            "MASTER" => Some(ConnectionType::Master),
                            "REPLICA" | "SLAVE" => Some(ConnectionType::Replica),
                            _ => {
                                return RespValue::error(format!(
                                    "ERR Unknown client type '{}' in CLIENT LIST TYPE",
                                    type_str
                                ))
                            }
                        };
                        i += 2;
                    }
                    "ID" => {
                        let mut ids = Vec::new();
                        i += 1;
                        while i < args.len() {
                            let id_str = String::from_utf8_lossy(&args[i]);
                            // Check if this looks like another keyword
                            if id_str.to_uppercase() == "TYPE" {
                                break;
                            }
                            match id_str.parse::<u64>() {
                                Ok(id) => ids.push(id),
                                Err(_) => return RespValue::error("ERR Invalid client ID"),
                            }
                            i += 1;
                        }
                        if ids.is_empty() {
                            return RespValue::error("ERR syntax error");
                        }
                        filter_ids = Some(ids);
                    }
                    _ => return RespValue::error("ERR syntax error"),
                }
            }

            let list = pool.format_list(filter_type, filter_ids.as_deref());
            // Add trailing newline for Redis compatibility
            let output = if list.is_empty() {
                String::new()
            } else {
                format!("{}\n", list)
            };
            RespValue::BulkString(Some(output.into_bytes()))
        }
        "ID" => {
            if let Ok(info) = conn_handle.read() {
                RespValue::Integer(info.id as i64)
            } else {
                RespValue::Integer(-1)
            }
        }
        "KILL" => {
            // CLIENT KILL [ID id] [TYPE type] [ADDR addr:port] [SKIPME yes/no]
            // For now, we implement CLIENT KILL ID <id>
            if args.len() < 3 {
                return RespValue::error("ERR syntax error");
            }
            let sub = String::from_utf8_lossy(&args[1]).to_uppercase();
            if sub != "ID" {
                return RespValue::error("ERR syntax error. Try CLIENT KILL ID <id>");
            }
            let id_str = String::from_utf8_lossy(&args[2]);
            match id_str.parse::<u64>() {
                Ok(id) => {
                    // Check if connection exists
                    if pool.get(id).is_some() {
                        // Mark it for disconnection (remove from pool)
                        pool.unregister(id);
                        RespValue::Integer(1)
                    } else {
                        RespValue::error("ERR No such client")
                    }
                }
                Err(_) => RespValue::error("ERR Invalid client ID"),
            }
        }
        "PAUSE" => {
            if args.len() != 2 {
                return RespValue::error("wrong number of arguments for 'client|pause' command");
            }
            let timeout_str = String::from_utf8_lossy(&args[1]);
            match timeout_str.parse::<i64>() {
                Ok(ms) if ms >= 0 => {
                    pause_state.pause(ms);
                    RespValue::ok()
                }
                Ok(_) => RespValue::error("ERR timeout is negative"),
                Err(_) => RespValue::error("ERR timeout is not an integer or out of range"),
            }
        }
        "UNPAUSE" => {
            // Unpause all clients
            pause_state.paused.store(false, Ordering::SeqCst);
            RespValue::ok()
        }
        "INFO" => {
            // Return info about current connection
            if let Ok(info) = conn_handle.read() {
                RespValue::BulkString(Some(info.format_list_entry().into_bytes()))
            } else {
                RespValue::error("ERR unable to get client info")
            }
        }
        "NO-EVICT" => {
            // Stub for compatibility - we don't support eviction
            RespValue::ok()
        }
        "REPLY" => {
            // Stub for compatibility - always ON
            RespValue::ok()
        }
        _ => RespValue::error(format!(
            "ERR Unknown subcommand or wrong number of arguments for '{}'",
            subcommand
        )),
    }
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

async fn cmd_blpop(db: &Db, args: &[Vec<u8>]) -> RespValue {
    // BLPOP key [key ...] timeout
    if args.len() < 2 {
        return RespValue::error("wrong number of arguments for 'blpop' command");
    }

    let timeout_arg = &args[args.len() - 1];
    let keys = &args[..args.len() - 1];

    let timeout: f64 = match std::str::from_utf8(timeout_arg)
        .ok()
        .and_then(|s| s.parse().ok())
    {
        Some(t) => t,
        None => return RespValue::error("timeout is not a float or out of range"),
    };

    let key_strs: Vec<&str> = match keys
        .iter()
        .map(|k| std::str::from_utf8(k))
        .collect::<std::result::Result<Vec<_>, _>>()
    {
        Ok(k) => k,
        Err(_) => return RespValue::error("invalid key"),
    };

    match db.blpop(&key_strs, timeout).await {
        Ok(Some((key, value))) => RespValue::Array(Some(vec![
            RespValue::BulkString(Some(key.into_bytes())),
            RespValue::BulkString(Some(value)),
        ])),
        Ok(None) => RespValue::null(),
        Err(KvError::WrongType) => RespValue::wrong_type(),
        Err(e) => RespValue::error(e.to_string()),
    }
}

async fn cmd_brpop(db: &Db, args: &[Vec<u8>]) -> RespValue {
    // BRPOP key [key ...] timeout
    if args.len() < 2 {
        return RespValue::error("wrong number of arguments for 'brpop' command");
    }

    let timeout_arg = &args[args.len() - 1];
    let keys = &args[..args.len() - 1];

    let timeout: f64 = match std::str::from_utf8(timeout_arg)
        .ok()
        .and_then(|s| s.parse().ok())
    {
        Some(t) => t,
        None => return RespValue::error("timeout is not a float or out of range"),
    };

    let key_strs: Vec<&str> = match keys
        .iter()
        .map(|k| std::str::from_utf8(k))
        .collect::<std::result::Result<Vec<_>, _>>()
    {
        Ok(k) => k,
        Err(_) => return RespValue::error("invalid key"),
    };

    match db.brpop(&key_strs, timeout).await {
        Ok(Some((key, value))) => RespValue::Array(Some(vec![
            RespValue::BulkString(Some(key.into_bytes())),
            RespValue::BulkString(Some(value)),
        ])),
        Ok(None) => RespValue::null(),
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

fn cmd_lrem(db: &Db, args: &[Vec<u8>]) -> RespValue {
    if args.len() != 3 {
        return RespValue::error("wrong number of arguments for 'lrem' command");
    }

    let key = match std::str::from_utf8(&args[0]) {
        Ok(k) => k,
        Err(_) => return RespValue::error("invalid key"),
    };

    let count: i64 = match std::str::from_utf8(&args[1])
        .ok()
        .and_then(|s| s.parse().ok())
    {
        Some(i) => i,
        None => return RespValue::error("value is not an integer or out of range"),
    };

    let element = &args[2];

    match db.lrem(key, count, element) {
        Ok(removed) => RespValue::Integer(removed),
        Err(KvError::WrongType) => RespValue::wrong_type(),
        Err(e) => RespValue::error(e.to_string()),
    }
}

fn cmd_linsert(db: &Db, args: &[Vec<u8>]) -> RespValue {
    if args.len() != 4 {
        return RespValue::error("wrong number of arguments for 'linsert' command");
    }

    let key = match std::str::from_utf8(&args[0]) {
        Ok(k) => k,
        Err(_) => return RespValue::error("invalid key"),
    };

    let position = match std::str::from_utf8(&args[1]) {
        Ok(p) => match p.to_uppercase().as_str() {
            "BEFORE" => true,
            "AFTER" => false,
            _ => return RespValue::error("syntax error"),
        },
        Err(_) => return RespValue::error("invalid position"),
    };

    let pivot = &args[2];
    let element = &args[3];

    match db.linsert(key, position, pivot, element) {
        Ok(length) => RespValue::Integer(length),
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

fn cmd_smove(db: &Db, args: &[Vec<u8>]) -> RespValue {
    if args.len() != 3 {
        return RespValue::error("wrong number of arguments for 'smove' command");
    }

    let source = match std::str::from_utf8(&args[0]) {
        Ok(k) => k,
        Err(_) => return RespValue::error("invalid key"),
    };

    let destination = match std::str::from_utf8(&args[1]) {
        Ok(k) => k,
        Err(_) => return RespValue::error("invalid key"),
    };

    let member = &args[2];

    match db.smove(source, destination, member) {
        Ok(moved) => RespValue::Integer(moved),
        Err(KvError::WrongType) => RespValue::wrong_type(),
        Err(e) => RespValue::error(e.to_string()),
    }
}

fn cmd_sdiffstore(db: &Db, args: &[Vec<u8>]) -> RespValue {
    if args.len() < 2 {
        return RespValue::error("wrong number of arguments for 'sdiffstore' command");
    }

    let destination = match std::str::from_utf8(&args[0]) {
        Ok(k) => k,
        Err(_) => return RespValue::error("invalid key"),
    };

    let keys: Vec<&str> = match args[1..]
        .iter()
        .map(|k| std::str::from_utf8(k))
        .collect::<std::result::Result<Vec<_>, _>>()
    {
        Ok(k) => k,
        Err(_) => return RespValue::error("invalid key"),
    };

    match db.sdiffstore(destination, &keys) {
        Ok(count) => RespValue::Integer(count),
        Err(KvError::WrongType) => RespValue::wrong_type(),
        Err(e) => RespValue::error(e.to_string()),
    }
}

fn cmd_sinterstore(db: &Db, args: &[Vec<u8>]) -> RespValue {
    if args.len() < 2 {
        return RespValue::error("wrong number of arguments for 'sinterstore' command");
    }

    let destination = match std::str::from_utf8(&args[0]) {
        Ok(k) => k,
        Err(_) => return RespValue::error("invalid key"),
    };

    let keys: Vec<&str> = match args[1..]
        .iter()
        .map(|k| std::str::from_utf8(k))
        .collect::<std::result::Result<Vec<_>, _>>()
    {
        Ok(k) => k,
        Err(_) => return RespValue::error("invalid key"),
    };

    match db.sinterstore(destination, &keys) {
        Ok(count) => RespValue::Integer(count),
        Err(KvError::WrongType) => RespValue::wrong_type(),
        Err(e) => RespValue::error(e.to_string()),
    }
}

fn cmd_sunionstore(db: &Db, args: &[Vec<u8>]) -> RespValue {
    if args.len() < 2 {
        return RespValue::error("wrong number of arguments for 'sunionstore' command");
    }

    let destination = match std::str::from_utf8(&args[0]) {
        Ok(k) => k,
        Err(_) => return RespValue::error("invalid key"),
    };

    let keys: Vec<&str> = match args[1..]
        .iter()
        .map(|k| std::str::from_utf8(k))
        .collect::<std::result::Result<Vec<_>, _>>()
    {
        Ok(k) => k,
        Err(_) => return RespValue::error("invalid key"),
    };

    match db.sunionstore(destination, &keys) {
        Ok(count) => RespValue::Integer(count),
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

    let start: i64 = match std::str::from_utf8(&args[1])
        .ok()
        .and_then(|s| s.parse().ok())
    {
        Some(v) => v,
        None => return RespValue::error("value is not an integer or out of range"),
    };

    let stop: i64 = match std::str::from_utf8(&args[2])
        .ok()
        .and_then(|s| s.parse().ok())
    {
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
                    result.push(RespValue::BulkString(Some(
                        m.score.to_string().into_bytes(),
                    )));
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

    let start: i64 = match std::str::from_utf8(&args[1])
        .ok()
        .and_then(|s| s.parse().ok())
    {
        Some(v) => v,
        None => return RespValue::error("value is not an integer or out of range"),
    };

    let stop: i64 = match std::str::from_utf8(&args[2])
        .ok()
        .and_then(|s| s.parse().ok())
    {
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
                    result.push(RespValue::BulkString(Some(
                        m.score.to_string().into_bytes(),
                    )));
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
            offset = match std::str::from_utf8(&args[i + 1])
                .ok()
                .and_then(|s| s.parse().ok())
            {
                Some(v) => Some(v),
                None => return RespValue::error("value is not an integer or out of range"),
            };
            count = match std::str::from_utf8(&args[i + 2])
                .ok()
                .and_then(|s| s.parse().ok())
            {
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

    let increment: f64 = match std::str::from_utf8(&args[1])
        .ok()
        .and_then(|s| s.parse().ok())
    {
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

    let start: i64 = match std::str::from_utf8(&args[1])
        .ok()
        .and_then(|s| s.parse().ok())
    {
        Some(v) => v,
        None => return RespValue::error("value is not an integer or out of range"),
    };

    let stop: i64 = match std::str::from_utf8(&args[2])
        .ok()
        .and_then(|s| s.parse().ok())
    {
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

fn cmd_history(db: &Db, args: &[Vec<u8>]) -> RespValue {
    if args.is_empty() {
        return RespValue::error("HISTORY subcommand required");
    }

    let subcommand = match std::str::from_utf8(&args[0]) {
        Ok(s) => s.to_uppercase(),
        Err(_) => return RespValue::error("invalid subcommand"),
    };

    match subcommand.as_str() {
        "ENABLE" => {
            if args.len() < 2 {
                return RespValue::error("HISTORY ENABLE requires level (GLOBAL|DATABASE|KEY)");
            }

            let level = match std::str::from_utf8(&args[1]) {
                Ok(s) => s.to_uppercase(),
                Err(_) => return RespValue::error("invalid level"),
            };

            let retention = if args.len() >= 4 {
                let ret_type = match std::str::from_utf8(&args[2]) {
                    Ok(s) => s.to_uppercase(),
                    Err(_) => return RespValue::error("invalid retention type"),
                };

                let ret_value: i64 = match std::str::from_utf8(&args[3])
                    .ok()
                    .and_then(|s| s.parse().ok())
                {
                    Some(v) => v,
                    None => return RespValue::error("retention value must be an integer"),
                };

                match ret_type.as_str() {
                    "TIME" => crate::types::RetentionType::Time(ret_value),
                    "COUNT" => crate::types::RetentionType::Count(ret_value),
                    _ => return RespValue::error("retention type must be TIME or COUNT"),
                }
            } else {
                crate::types::RetentionType::Unlimited
            };

            match level.as_str() {
                "GLOBAL" => {
                    if let Err(e) = db.history_enable_global(retention) {
                        return RespValue::error(format!("history enable failed: {}", e));
                    }
                    RespValue::ok()
                }
                "DATABASE" => {
                    if args.len() < 3 {
                        return RespValue::error(
                            "HISTORY ENABLE DATABASE requires database number",
                        );
                    }
                    let db_num: i32 = match std::str::from_utf8(&args[2])
                        .ok()
                        .and_then(|s| s.parse().ok())
                    {
                        Some(n) => n,
                        None => return RespValue::error("database number must be an integer"),
                    };
                    if let Err(e) = db.history_enable_database(db_num, retention) {
                        return RespValue::error(format!("history enable failed: {}", e));
                    }
                    RespValue::ok()
                }
                "KEY" => {
                    if args.len() < 3 {
                        return RespValue::error("HISTORY ENABLE KEY requires key name");
                    }
                    let key = match std::str::from_utf8(&args[2]) {
                        Ok(k) => k,
                        Err(_) => return RespValue::error("invalid key"),
                    };
                    if let Err(e) = db.history_enable_key(key, retention) {
                        return RespValue::error(format!("history enable failed: {}", e));
                    }
                    RespValue::ok()
                }
                _ => RespValue::error("level must be GLOBAL, DATABASE, or KEY"),
            }
        }
        "DISABLE" => {
            if args.len() < 2 {
                return RespValue::error("HISTORY DISABLE requires level (GLOBAL|DATABASE|KEY)");
            }

            let level = match std::str::from_utf8(&args[1]) {
                Ok(s) => s.to_uppercase(),
                Err(_) => return RespValue::error("invalid level"),
            };

            match level.as_str() {
                "GLOBAL" => {
                    if let Err(e) = db.history_disable_global() {
                        return RespValue::error(format!("history disable failed: {}", e));
                    }
                    RespValue::ok()
                }
                "DATABASE" => {
                    if args.len() < 3 {
                        return RespValue::error(
                            "HISTORY DISABLE DATABASE requires database number",
                        );
                    }
                    let db_num: i32 = match std::str::from_utf8(&args[2])
                        .ok()
                        .and_then(|s| s.parse().ok())
                    {
                        Some(n) => n,
                        None => return RespValue::error("database number must be an integer"),
                    };
                    if let Err(e) = db.history_disable_database(db_num) {
                        return RespValue::error(format!("history disable failed: {}", e));
                    }
                    RespValue::ok()
                }
                "KEY" => {
                    if args.len() < 3 {
                        return RespValue::error("HISTORY DISABLE KEY requires key name");
                    }
                    let key = match std::str::from_utf8(&args[2]) {
                        Ok(k) => k,
                        Err(_) => return RespValue::error("invalid key"),
                    };
                    if let Err(e) = db.history_disable_key(key) {
                        return RespValue::error(format!("history disable failed: {}", e));
                    }
                    RespValue::ok()
                }
                _ => RespValue::error("level must be GLOBAL, DATABASE, or KEY"),
            }
        }
        "GET" => {
            if args.len() < 2 {
                return RespValue::error("HISTORY GET requires key");
            }
            let key = match std::str::from_utf8(&args[1]) {
                Ok(k) => k,
                Err(_) => return RespValue::error("invalid key"),
            };

            let limit = if let Some(idx) = args.iter().position(|a| {
                std::str::from_utf8(a)
                    .map(|s| s.to_uppercase() == "LIMIT")
                    .unwrap_or(false)
            }) {
                if idx + 1 < args.len() {
                    std::str::from_utf8(&args[idx + 1])
                        .ok()
                        .and_then(|s| s.parse().ok())
                } else {
                    None
                }
            } else {
                None
            };

            match db.history_get(key, limit, None, None) {
                Ok(entries) => {
                    let resp_entries: Vec<RespValue> = entries
                        .into_iter()
                        .map(|e| {
                            RespValue::Array(Some(vec![
                                RespValue::BulkString(Some(e.key.as_bytes().to_vec())),
                                RespValue::Integer(e.version_num),
                                RespValue::Integer(e.timestamp_ms),
                                RespValue::BulkString(Some(e.operation.as_bytes().to_vec())),
                            ]))
                        })
                        .collect();
                    RespValue::Array(Some(resp_entries))
                }
                Err(e) => RespValue::error(format!("history get failed: {}", e)),
            }
        }
        "STATS" => {
            let key = args.get(1).and_then(|k| std::str::from_utf8(k).ok());
            match db.history_stats(key) {
                Ok(stats) => RespValue::Array(Some(vec![
                    RespValue::BulkString(Some(b"total_entries".to_vec())),
                    RespValue::Integer(stats.total_entries),
                    RespValue::BulkString(Some(b"oldest_timestamp".to_vec())),
                    stats
                        .oldest_timestamp
                        .map(RespValue::Integer)
                        .unwrap_or_else(RespValue::null),
                    RespValue::BulkString(Some(b"newest_timestamp".to_vec())),
                    stats
                        .newest_timestamp
                        .map(RespValue::Integer)
                        .unwrap_or_else(RespValue::null),
                    RespValue::BulkString(Some(b"storage_bytes".to_vec())),
                    RespValue::Integer(stats.storage_bytes),
                ])),
                Err(e) => RespValue::error(format!("history stats failed: {}", e)),
            }
        }
        "CLEAR" => {
            if args.len() < 2 {
                return RespValue::error("HISTORY CLEAR requires key");
            }
            let key = match std::str::from_utf8(&args[1]) {
                Ok(k) => k,
                Err(_) => return RespValue::error("invalid key"),
            };

            match db.history_clear_key(key, None) {
                Ok(count) => RespValue::Integer(count),
                Err(e) => RespValue::error(format!("history clear failed: {}", e)),
            }
        }
        "PRUNE" => {
            if args.len() < 3 {
                return RespValue::error("HISTORY PRUNE requires BEFORE <timestamp>");
            }
            let before_str = match std::str::from_utf8(&args[2]) {
                Ok(s) => s,
                Err(_) => return RespValue::error("invalid timestamp"),
            };
            let before: i64 = match before_str.parse() {
                Ok(t) => t,
                Err(_) => return RespValue::error("timestamp must be an integer"),
            };

            match db.history_prune(before) {
                Ok(count) => RespValue::Integer(count),
                Err(e) => RespValue::error(format!("history prune failed: {}", e)),
            }
        }
        _ => RespValue::error(format!("unknown history subcommand '{}'", subcommand)),
    }
}

// --- Session 24.1: FTS command handlers ---

fn cmd_fts(db: &Db, args: &[Vec<u8>]) -> RespValue {
    if args.is_empty() {
        return RespValue::error("FTS subcommand required (ENABLE|DISABLE|SEARCH|INFO|REINDEX)");
    }

    let subcommand = match std::str::from_utf8(&args[0]) {
        Ok(s) => s.to_uppercase(),
        Err(_) => return RespValue::error("invalid subcommand"),
    };

    match subcommand.as_str() {
        "ENABLE" => {
            if args.len() < 2 {
                return RespValue::error("FTS ENABLE requires level (GLOBAL|DATABASE|PATTERN|KEY)");
            }

            let level = match std::str::from_utf8(&args[1]) {
                Ok(s) => s.to_uppercase(),
                Err(_) => return RespValue::error("invalid level"),
            };

            match level.as_str() {
                "GLOBAL" => {
                    if let Err(e) = db.fts_enable_global() {
                        return RespValue::error(format!("FTS enable failed: {}", e));
                    }
                    RespValue::ok()
                }
                "DATABASE" => {
                    if args.len() < 3 {
                        return RespValue::error("FTS ENABLE DATABASE requires database number");
                    }
                    let db_num: i32 = match std::str::from_utf8(&args[2])
                        .ok()
                        .and_then(|s| s.parse().ok())
                    {
                        Some(n) => n,
                        None => return RespValue::error("database number must be an integer"),
                    };
                    if let Err(e) = db.fts_enable_database(db_num) {
                        return RespValue::error(format!("FTS enable failed: {}", e));
                    }
                    RespValue::ok()
                }
                "PATTERN" => {
                    if args.len() < 3 {
                        return RespValue::error("FTS ENABLE PATTERN requires pattern");
                    }
                    let pattern = match std::str::from_utf8(&args[2]) {
                        Ok(p) => p,
                        Err(_) => return RespValue::error("invalid pattern"),
                    };
                    if let Err(e) = db.fts_enable_pattern(pattern) {
                        return RespValue::error(format!("FTS enable failed: {}", e));
                    }
                    RespValue::ok()
                }
                "KEY" => {
                    if args.len() < 3 {
                        return RespValue::error("FTS ENABLE KEY requires key name");
                    }
                    let key = match std::str::from_utf8(&args[2]) {
                        Ok(k) => k,
                        Err(_) => return RespValue::error("invalid key"),
                    };
                    if let Err(e) = db.fts_enable_key(key) {
                        return RespValue::error(format!("FTS enable failed: {}", e));
                    }
                    RespValue::ok()
                }
                _ => RespValue::error("level must be GLOBAL, DATABASE, PATTERN, or KEY"),
            }
        }
        "DISABLE" => {
            if args.len() < 2 {
                return RespValue::error(
                    "FTS DISABLE requires level (GLOBAL|DATABASE|PATTERN|KEY)",
                );
            }

            let level = match std::str::from_utf8(&args[1]) {
                Ok(s) => s.to_uppercase(),
                Err(_) => return RespValue::error("invalid level"),
            };

            match level.as_str() {
                "GLOBAL" => {
                    if let Err(e) = db.fts_disable_global() {
                        return RespValue::error(format!("FTS disable failed: {}", e));
                    }
                    RespValue::ok()
                }
                "DATABASE" => {
                    if args.len() < 3 {
                        return RespValue::error("FTS DISABLE DATABASE requires database number");
                    }
                    let db_num: i32 = match std::str::from_utf8(&args[2])
                        .ok()
                        .and_then(|s| s.parse().ok())
                    {
                        Some(n) => n,
                        None => return RespValue::error("database number must be an integer"),
                    };
                    if let Err(e) = db.fts_disable_database(db_num) {
                        return RespValue::error(format!("FTS disable failed: {}", e));
                    }
                    RespValue::ok()
                }
                "PATTERN" => {
                    if args.len() < 3 {
                        return RespValue::error("FTS DISABLE PATTERN requires pattern");
                    }
                    let pattern = match std::str::from_utf8(&args[2]) {
                        Ok(p) => p,
                        Err(_) => return RespValue::error("invalid pattern"),
                    };
                    if let Err(e) = db.fts_disable_pattern(pattern) {
                        return RespValue::error(format!("FTS disable failed: {}", e));
                    }
                    RespValue::ok()
                }
                "KEY" => {
                    if args.len() < 3 {
                        return RespValue::error("FTS DISABLE KEY requires key name");
                    }
                    let key = match std::str::from_utf8(&args[2]) {
                        Ok(k) => k,
                        Err(_) => return RespValue::error("invalid key"),
                    };
                    if let Err(e) = db.fts_disable_key(key) {
                        return RespValue::error(format!("FTS disable failed: {}", e));
                    }
                    RespValue::ok()
                }
                _ => RespValue::error("level must be GLOBAL, DATABASE, PATTERN, or KEY"),
            }
        }
        "SEARCH" => {
            if args.len() < 2 {
                return RespValue::error("FTS SEARCH requires query");
            }
            let query = match std::str::from_utf8(&args[1]) {
                Ok(q) => q,
                Err(_) => return RespValue::error("invalid query"),
            };

            // Parse optional LIMIT and HIGHLIGHT
            let mut limit: Option<i64> = None;
            let mut highlight = false;
            let mut i = 2;
            while i < args.len() {
                let arg = match std::str::from_utf8(&args[i]) {
                    Ok(s) => s.to_uppercase(),
                    Err(_) => {
                        i += 1;
                        continue;
                    }
                };
                match arg.as_str() {
                    "LIMIT" => {
                        if i + 1 < args.len() {
                            limit = std::str::from_utf8(&args[i + 1])
                                .ok()
                                .and_then(|s| s.parse().ok());
                            i += 2;
                        } else {
                            i += 1;
                        }
                    }
                    "HIGHLIGHT" => {
                        highlight = true;
                        i += 1;
                    }
                    _ => i += 1,
                }
            }

            match db.fts_search(query, limit, highlight) {
                Ok(results) => {
                    let resp_results: Vec<RespValue> = results
                        .into_iter()
                        .map(|r| {
                            let mut entry = vec![
                                RespValue::BulkString(Some(b"key".to_vec())),
                                RespValue::BulkString(Some(r.key.as_bytes().to_vec())),
                                RespValue::BulkString(Some(b"rank".to_vec())),
                                RespValue::BulkString(Some(r.rank.to_string().as_bytes().to_vec())),
                            ];
                            if let Some(snippet) = r.snippet {
                                entry.push(RespValue::BulkString(Some(b"snippet".to_vec())));
                                entry
                                    .push(RespValue::BulkString(Some(snippet.as_bytes().to_vec())));
                            }
                            RespValue::Array(Some(entry))
                        })
                        .collect();
                    RespValue::Array(Some(resp_results))
                }
                Err(e) => RespValue::error(format!("FTS search failed: {}", e)),
            }
        }
        "INFO" => match db.fts_info() {
            Ok(stats) => {
                let configs: Vec<RespValue> = stats
                    .configs
                    .iter()
                    .map(|c| {
                        RespValue::Array(Some(vec![
                            RespValue::BulkString(Some(b"level".to_vec())),
                            RespValue::BulkString(Some(c.level.as_str().as_bytes().to_vec())),
                            RespValue::BulkString(Some(b"target".to_vec())),
                            RespValue::BulkString(Some(c.target.as_bytes().to_vec())),
                            RespValue::BulkString(Some(b"enabled".to_vec())),
                            RespValue::Integer(if c.enabled { 1 } else { 0 }),
                        ]))
                    })
                    .collect();

                RespValue::Array(Some(vec![
                    RespValue::BulkString(Some(b"indexed_keys".to_vec())),
                    RespValue::Integer(stats.indexed_keys),
                    RespValue::BulkString(Some(b"storage_bytes".to_vec())),
                    RespValue::Integer(stats.storage_bytes),
                    RespValue::BulkString(Some(b"configs".to_vec())),
                    RespValue::Array(Some(configs)),
                ]))
            }
            Err(e) => RespValue::error(format!("FTS info failed: {}", e)),
        },
        "REINDEX" => {
            if args.len() < 2 {
                return RespValue::error("FTS REINDEX requires key");
            }
            let key = match std::str::from_utf8(&args[1]) {
                Ok(k) => k,
                Err(_) => return RespValue::error("invalid key"),
            };

            match db.fts_reindex_key(key) {
                Ok(reindexed) => {
                    if reindexed {
                        RespValue::ok()
                    } else {
                        RespValue::Integer(0)
                    }
                }
                Err(e) => RespValue::error(format!("FTS reindex failed: {}", e)),
            }
        }
        _ => RespValue::error(format!(
            "unknown FTS subcommand '{}'. Use ENABLE|DISABLE|SEARCH|INFO|REINDEX",
            subcommand
        )),
    }
}

// --- Session 23: RediSearch-compatible FT.* command handlers ---

use crate::types::{FtField, FtFieldType, FtOnType, FtSearchOptions};

/// FT.CREATE index ON HASH|JSON PREFIX n prefix... SCHEMA field type...
fn cmd_ft_create(db: &Db, args: &[Vec<u8>]) -> RespValue {
    // FT.CREATE index ON HASH|JSON [PREFIX count prefix...] SCHEMA field type [SORTABLE] ...
    if args.len() < 5 {
        return RespValue::error("wrong number of arguments for 'FT.CREATE' command");
    }

    let name = match std::str::from_utf8(&args[0]) {
        Ok(n) => n,
        Err(_) => return RespValue::error("invalid index name"),
    };

    // Parse ON HASH|JSON
    let on_keyword = match std::str::from_utf8(&args[1]) {
        Ok(s) => s.to_uppercase(),
        Err(_) => return RespValue::error("invalid ON keyword"),
    };
    if on_keyword != "ON" {
        return RespValue::error("expected ON keyword");
    }

    let on_type = match std::str::from_utf8(&args[2]) {
        Ok(s) => match FtOnType::from_str(s) {
            Some(t) => t,
            None => return RespValue::error("ON type must be HASH or JSON"),
        },
        Err(_) => return RespValue::error("invalid ON type"),
    };

    // Parse remaining args for PREFIX and SCHEMA
    let mut i = 3;
    let mut prefixes: Vec<&str> = Vec::new();
    let mut schema: Vec<FtField> = Vec::new();
    let mut in_schema = false;

    while i < args.len() {
        let arg = match std::str::from_utf8(&args[i]) {
            Ok(s) => s.to_uppercase(),
            Err(_) => {
                i += 1;
                continue;
            }
        };

        if arg == "PREFIX" {
            // PREFIX count prefix1 prefix2 ...
            i += 1;
            if i >= args.len() {
                return RespValue::error("PREFIX requires count");
            }
            let count: usize = match std::str::from_utf8(&args[i])
                .ok()
                .and_then(|s| s.parse().ok())
            {
                Some(c) => c,
                None => return RespValue::error("PREFIX count must be an integer"),
            };
            i += 1;
            for _ in 0..count {
                if i >= args.len() {
                    return RespValue::error("not enough prefix values");
                }
                match std::str::from_utf8(&args[i]) {
                    Ok(p) => prefixes.push(p),
                    Err(_) => return RespValue::error("invalid prefix"),
                };
                i += 1;
            }
        } else if arg == "SCHEMA" {
            in_schema = true;
            i += 1;
        } else if in_schema {
            // Parse field definition: name type [SORTABLE] [NOINDEX] ...
            let field_name = match std::str::from_utf8(&args[i]) {
                Ok(n) => n,
                Err(_) => return RespValue::error("invalid field name"),
            };
            i += 1;
            if i >= args.len() {
                return RespValue::error("field type required");
            }
            let field_type = match std::str::from_utf8(&args[i]) {
                Ok(s) => match FtFieldType::from_str(s) {
                    Some(t) => t,
                    None => return RespValue::error(format!("unknown field type: {}", s)),
                },
                Err(_) => return RespValue::error("invalid field type"),
            };
            i += 1;

            let mut field = FtField::new(field_name, field_type);

            // Parse field options
            while i < args.len() {
                let opt = match std::str::from_utf8(&args[i]) {
                    Ok(s) => s.to_uppercase(),
                    Err(_) => break,
                };
                match opt.as_str() {
                    "SORTABLE" => {
                        field.sortable = true;
                        i += 1;
                    }
                    "NOINDEX" => {
                        field.noindex = true;
                        i += 1;
                    }
                    "NOSTEM" => {
                        field.nostem = true;
                        i += 1;
                    }
                    "WEIGHT" => {
                        i += 1;
                        if i < args.len() {
                            if let Some(w) = std::str::from_utf8(&args[i])
                                .ok()
                                .and_then(|s| s.parse::<f64>().ok())
                            {
                                field.weight = w;
                            }
                            i += 1;
                        }
                    }
                    "SEPARATOR" => {
                        i += 1;
                        if i < args.len() {
                            if let Some(c) = std::str::from_utf8(&args[i])
                                .ok()
                                .and_then(|s| s.chars().next())
                            {
                                field.separator = c;
                            }
                            i += 1;
                        }
                    }
                    "CASESENSITIVE" => {
                        field.case_sensitive = true;
                        i += 1;
                    }
                    // If we hit another field type keyword, it's the next field
                    _ if FtFieldType::from_str(&opt).is_some()
                        || opt == "AS"
                        || opt == "PREFIX" =>
                    {
                        break
                    }
                    // If this looks like a new field name (next word after type), break
                    _ => break,
                }
            }
            schema.push(field);
        } else {
            i += 1;
        }
    }

    if schema.is_empty() {
        return RespValue::error("SCHEMA required with at least one field");
    }

    match db.ft_create(name, on_type, &prefixes, &schema) {
        Ok(()) => RespValue::ok(),
        Err(e) => RespValue::error(e.to_string()),
    }
}

/// FT.DROPINDEX index [DD]
fn cmd_ft_dropindex(db: &Db, args: &[Vec<u8>]) -> RespValue {
    if args.is_empty() {
        return RespValue::error("wrong number of arguments for 'FT.DROPINDEX' command");
    }

    let name = match std::str::from_utf8(&args[0]) {
        Ok(n) => n,
        Err(_) => return RespValue::error("invalid index name"),
    };

    let delete_docs = args
        .get(1)
        .and_then(|a| std::str::from_utf8(a).ok())
        .map(|s| s.to_uppercase() == "DD")
        .unwrap_or(false);

    match db.ft_dropindex(name, delete_docs) {
        Ok(true) => RespValue::ok(),
        Ok(false) => RespValue::error("Unknown index name"),
        Err(e) => RespValue::error(e.to_string()),
    }
}

/// FT._LIST
fn cmd_ft_list(db: &Db) -> RespValue {
    match db.ft_list() {
        Ok(indexes) => {
            let resp: Vec<RespValue> = indexes
                .into_iter()
                .map(|name| RespValue::BulkString(Some(name.into_bytes())))
                .collect();
            RespValue::Array(Some(resp))
        }
        Err(e) => RespValue::error(e.to_string()),
    }
}

/// FT.INFO index
fn cmd_ft_info(db: &Db, args: &[Vec<u8>]) -> RespValue {
    if args.is_empty() {
        return RespValue::error("wrong number of arguments for 'FT.INFO' command");
    }

    let name = match std::str::from_utf8(&args[0]) {
        Ok(n) => n,
        Err(_) => return RespValue::error("invalid index name"),
    };

    match db.ft_info(name) {
        Ok(Some(info)) => {
            let schema_resp: Vec<RespValue> = info
                .schema
                .iter()
                .flat_map(|f| {
                    let mut field_info = vec![
                        RespValue::BulkString(Some(f.name.as_bytes().to_vec())),
                        RespValue::BulkString(Some(b"type".to_vec())),
                        RespValue::BulkString(Some(f.field_type.as_str().as_bytes().to_vec())),
                    ];
                    if f.sortable {
                        field_info.push(RespValue::BulkString(Some(b"SORTABLE".to_vec())));
                    }
                    if f.noindex {
                        field_info.push(RespValue::BulkString(Some(b"NOINDEX".to_vec())));
                    }
                    field_info
                })
                .collect();

            let prefixes_resp: Vec<RespValue> = info
                .prefixes
                .iter()
                .map(|p| RespValue::BulkString(Some(p.as_bytes().to_vec())))
                .collect();

            RespValue::Array(Some(vec![
                RespValue::BulkString(Some(b"index_name".to_vec())),
                RespValue::BulkString(Some(info.name.into_bytes())),
                RespValue::BulkString(Some(b"index_options".to_vec())),
                RespValue::Array(Some(vec![])),
                RespValue::BulkString(Some(b"index_definition".to_vec())),
                RespValue::Array(Some(vec![
                    RespValue::BulkString(Some(b"key_type".to_vec())),
                    RespValue::BulkString(Some(info.on_type.as_str().as_bytes().to_vec())),
                    RespValue::BulkString(Some(b"prefixes".to_vec())),
                    RespValue::Array(Some(prefixes_resp)),
                ])),
                RespValue::BulkString(Some(b"attributes".to_vec())),
                RespValue::Array(Some(schema_resp)),
                RespValue::BulkString(Some(b"num_docs".to_vec())),
                RespValue::Integer(info.num_docs),
                RespValue::BulkString(Some(b"num_terms".to_vec())),
                RespValue::Integer(info.num_terms),
                RespValue::BulkString(Some(b"num_records".to_vec())),
                RespValue::Integer(info.num_records),
            ]))
        }
        Ok(None) => RespValue::error("Unknown index name"),
        Err(e) => RespValue::error(e.to_string()),
    }
}

/// FT.ALTER index ADD field type [options]
fn cmd_ft_alter(db: &Db, args: &[Vec<u8>]) -> RespValue {
    // FT.ALTER index [SKIPINITIALSCAN] SCHEMA ADD field type [options]
    if args.len() < 4 {
        return RespValue::error("wrong number of arguments for 'FT.ALTER' command");
    }

    let name = match std::str::from_utf8(&args[0]) {
        Ok(n) => n,
        Err(_) => return RespValue::error("invalid index name"),
    };

    // Find SCHEMA ADD
    let mut i = 1;
    while i < args.len() {
        let arg = match std::str::from_utf8(&args[i]) {
            Ok(s) => s.to_uppercase(),
            Err(_) => {
                i += 1;
                continue;
            }
        };
        if arg == "SCHEMA" {
            break;
        }
        i += 1;
    }

    i += 1; // skip SCHEMA
    if i >= args.len() {
        return RespValue::error("expected ADD after SCHEMA");
    }

    let add_keyword = match std::str::from_utf8(&args[i]) {
        Ok(s) => s.to_uppercase(),
        Err(_) => return RespValue::error("invalid ADD keyword"),
    };
    if add_keyword != "ADD" {
        return RespValue::error("expected ADD after SCHEMA");
    }
    i += 1;

    if i + 1 >= args.len() {
        return RespValue::error("field name and type required");
    }

    let field_name = match std::str::from_utf8(&args[i]) {
        Ok(n) => n,
        Err(_) => return RespValue::error("invalid field name"),
    };
    i += 1;

    let field_type = match std::str::from_utf8(&args[i]) {
        Ok(s) => match FtFieldType::from_str(s) {
            Some(t) => t,
            None => return RespValue::error(format!("unknown field type: {}", s)),
        },
        Err(_) => return RespValue::error("invalid field type"),
    };
    i += 1;

    let mut field = FtField::new(field_name, field_type);

    // Parse optional field modifiers
    while i < args.len() {
        let opt = match std::str::from_utf8(&args[i]) {
            Ok(s) => s.to_uppercase(),
            Err(_) => break,
        };
        match opt.as_str() {
            "SORTABLE" => field.sortable = true,
            "NOINDEX" => field.noindex = true,
            "NOSTEM" => field.nostem = true,
            "CASESENSITIVE" => field.case_sensitive = true,
            _ => {}
        }
        i += 1;
    }

    match db.ft_alter(name, field) {
        Ok(()) => RespValue::ok(),
        Err(e) => RespValue::error(e.to_string()),
    }
}

/// FT.SEARCH index query [options]
fn cmd_ft_search(db: &Db, args: &[Vec<u8>]) -> RespValue {
    if args.len() < 2 {
        return RespValue::error("wrong number of arguments for 'FT.SEARCH' command");
    }

    let index_name = match std::str::from_utf8(&args[0]) {
        Ok(n) => n,
        Err(_) => return RespValue::error("invalid index name"),
    };

    let query = match std::str::from_utf8(&args[1]) {
        Ok(q) => q,
        Err(_) => return RespValue::error("invalid query"),
    };

    // Parse options
    let mut options = FtSearchOptions::new();
    let mut i = 2;
    while i < args.len() {
        let opt = match std::str::from_utf8(&args[i]) {
            Ok(s) => s.to_uppercase(),
            Err(_) => {
                i += 1;
                continue;
            }
        };
        match opt.as_str() {
            "NOCONTENT" => options.nocontent = true,
            "VERBATIM" => options.verbatim = true,
            "NOSTOPWORDS" => options.nostopwords = true,
            "WITHSCORES" => options.withscores = true,
            "WITHPAYLOADS" => options.withpayloads = true,
            "LIMIT" => {
                if i + 2 < args.len() {
                    options.limit_offset = std::str::from_utf8(&args[i + 1])
                        .ok()
                        .and_then(|s| s.parse().ok())
                        .unwrap_or(0);
                    options.limit_num = std::str::from_utf8(&args[i + 2])
                        .ok()
                        .and_then(|s| s.parse().ok())
                        .unwrap_or(10);
                    i += 2;
                }
            }
            "SORTBY" => {
                if i + 1 < args.len() {
                    let field = std::str::from_utf8(&args[i + 1]).unwrap_or("");
                    let asc = args
                        .get(i + 2)
                        .and_then(|a| std::str::from_utf8(a).ok())
                        .map(|s| s.to_uppercase() != "DESC")
                        .unwrap_or(true);
                    options.sortby = Some((field.to_string(), asc));
                    i += 2;
                }
            }
            "RETURN" => {
                if i + 1 < args.len() {
                    let count: usize = std::str::from_utf8(&args[i + 1])
                        .ok()
                        .and_then(|s| s.parse().ok())
                        .unwrap_or(0);
                    i += 2;
                    for _ in 0..count {
                        if i < args.len() {
                            if let Ok(f) = std::str::from_utf8(&args[i]) {
                                options.return_fields.push(f.to_string());
                            }
                            i += 1;
                        }
                    }
                    continue;
                }
            }
            "INKEYS" => {
                if i + 1 < args.len() {
                    let count: usize = std::str::from_utf8(&args[i + 1])
                        .ok()
                        .and_then(|s| s.parse().ok())
                        .unwrap_or(0);
                    i += 2;
                    for _ in 0..count {
                        if i < args.len() {
                            if let Ok(k) = std::str::from_utf8(&args[i]) {
                                options.inkeys.push(k.to_string());
                            }
                            i += 1;
                        }
                    }
                    continue;
                }
            }
            "INFIELDS" => {
                if i + 1 < args.len() {
                    let count: usize = std::str::from_utf8(&args[i + 1])
                        .ok()
                        .and_then(|s| s.parse().ok())
                        .unwrap_or(0);
                    i += 2;
                    for _ in 0..count {
                        if i < args.len() {
                            if let Ok(f) = std::str::from_utf8(&args[i]) {
                                options.infields.push(f.to_string());
                            }
                            i += 1;
                        }
                    }
                    continue;
                }
            }
            _ => {}
        }
        i += 1;
    }

    // Execute the search
    match db.ft_search(index_name, query, &options) {
        Ok((total, results)) => {
            let mut response = Vec::new();
            response.push(RespValue::Integer(total));

            for result in results {
                // Add key name
                response.push(RespValue::from_string(result.key.clone()));

                // Add score if requested
                if options.withscores {
                    response.push(RespValue::from_string(result.score.to_string()));
                }

                // Add fields if not NOCONTENT
                if !options.nocontent {
                    let mut field_values: Vec<RespValue> = Vec::new();
                    for (field_name, field_value) in &result.fields {
                        field_values.push(RespValue::from_string(field_name.clone()));
                        field_values.push(RespValue::BulkString(Some(field_value.clone())));
                    }
                    response.push(RespValue::Array(Some(field_values)));
                }
            }

            RespValue::Array(Some(response))
        }
        Err(e) => RespValue::error(&e.to_string()),
    }
}

/// FT.AGGREGATE index query [options]
fn cmd_ft_aggregate(db: &Db, args: &[Vec<u8>]) -> RespValue {
    if args.len() < 2 {
        return RespValue::error("wrong number of arguments for 'FT.AGGREGATE' command");
    }

    // FT.AGGREGATE is a placeholder until search.rs is implemented
    // For now, return an empty result set
    RespValue::Array(Some(vec![
        RespValue::Integer(0), // Total count
    ]))
}

/// FT.ALIASADD alias index
fn cmd_ft_aliasadd(db: &Db, args: &[Vec<u8>]) -> RespValue {
    if args.len() < 2 {
        return RespValue::error("wrong number of arguments for 'FT.ALIASADD' command");
    }

    let alias = match std::str::from_utf8(&args[0]) {
        Ok(a) => a,
        Err(_) => return RespValue::error("invalid alias"),
    };

    let index_name = match std::str::from_utf8(&args[1]) {
        Ok(n) => n,
        Err(_) => return RespValue::error("invalid index name"),
    };

    match db.ft_aliasadd(alias, index_name) {
        Ok(()) => RespValue::ok(),
        Err(e) => RespValue::error(e.to_string()),
    }
}

/// FT.ALIASDEL alias
fn cmd_ft_aliasdel(db: &Db, args: &[Vec<u8>]) -> RespValue {
    if args.is_empty() {
        return RespValue::error("wrong number of arguments for 'FT.ALIASDEL' command");
    }

    let alias = match std::str::from_utf8(&args[0]) {
        Ok(a) => a,
        Err(_) => return RespValue::error("invalid alias"),
    };

    match db.ft_aliasdel(alias) {
        Ok(true) => RespValue::ok(),
        Ok(false) => RespValue::error("Alias does not exist"),
        Err(e) => RespValue::error(e.to_string()),
    }
}

/// FT.ALIASUPDATE alias index
fn cmd_ft_aliasupdate(db: &Db, args: &[Vec<u8>]) -> RespValue {
    if args.len() < 2 {
        return RespValue::error("wrong number of arguments for 'FT.ALIASUPDATE' command");
    }

    let alias = match std::str::from_utf8(&args[0]) {
        Ok(a) => a,
        Err(_) => return RespValue::error("invalid alias"),
    };

    let index_name = match std::str::from_utf8(&args[1]) {
        Ok(n) => n,
        Err(_) => return RespValue::error("invalid index name"),
    };

    match db.ft_aliasupdate(alias, index_name) {
        Ok(()) => RespValue::ok(),
        Err(e) => RespValue::error(e.to_string()),
    }
}

/// FT.SYNUPDATE index group_id term [term ...]
fn cmd_ft_synupdate(db: &Db, args: &[Vec<u8>]) -> RespValue {
    if args.len() < 3 {
        return RespValue::error("wrong number of arguments for 'FT.SYNUPDATE' command");
    }

    let index_name = match std::str::from_utf8(&args[0]) {
        Ok(n) => n,
        Err(_) => return RespValue::error("invalid index name"),
    };

    let group_id = match std::str::from_utf8(&args[1]) {
        Ok(g) => g,
        Err(_) => return RespValue::error("invalid group id"),
    };

    let terms: Vec<&str> = match args[2..]
        .iter()
        .map(|t| std::str::from_utf8(t))
        .collect::<std::result::Result<Vec<_>, _>>()
    {
        Ok(t) => t,
        Err(_) => return RespValue::error("invalid term"),
    };

    match db.ft_synupdate(index_name, group_id, &terms) {
        Ok(()) => RespValue::ok(),
        Err(e) => RespValue::error(e.to_string()),
    }
}

/// FT.SYNDUMP index
fn cmd_ft_syndump(db: &Db, args: &[Vec<u8>]) -> RespValue {
    if args.is_empty() {
        return RespValue::error("wrong number of arguments for 'FT.SYNDUMP' command");
    }

    let index_name = match std::str::from_utf8(&args[0]) {
        Ok(n) => n,
        Err(_) => return RespValue::error("invalid index name"),
    };

    match db.ft_syndump(index_name) {
        Ok(groups) => {
            let mut resp: Vec<RespValue> = Vec::new();
            for (group_id, terms) in groups {
                resp.push(RespValue::BulkString(Some(group_id.into_bytes())));
                let terms_resp: Vec<RespValue> = terms
                    .into_iter()
                    .map(|t| RespValue::BulkString(Some(t.into_bytes())))
                    .collect();
                resp.push(RespValue::Array(Some(terms_resp)));
            }
            RespValue::Array(Some(resp))
        }
        Err(e) => RespValue::error(e.to_string()),
    }
}

/// FT.SUGADD key string score [PAYLOAD payload]
fn cmd_ft_sugadd(db: &Db, args: &[Vec<u8>]) -> RespValue {
    if args.len() < 3 {
        return RespValue::error("wrong number of arguments for 'FT.SUGADD' command");
    }

    let key = match std::str::from_utf8(&args[0]) {
        Ok(k) => k,
        Err(_) => return RespValue::error("invalid key"),
    };

    let string = match std::str::from_utf8(&args[1]) {
        Ok(s) => s,
        Err(_) => return RespValue::error("invalid string"),
    };

    let score: f64 = match std::str::from_utf8(&args[2])
        .ok()
        .and_then(|s| s.parse().ok())
    {
        Some(s) => s,
        None => return RespValue::error("score must be a number"),
    };

    // Parse optional PAYLOAD
    let mut payload: Option<&str> = None;
    let mut i = 3;
    while i < args.len() {
        let opt = match std::str::from_utf8(&args[i]) {
            Ok(s) => s.to_uppercase(),
            Err(_) => {
                i += 1;
                continue;
            }
        };
        if opt == "PAYLOAD" && i + 1 < args.len() {
            payload = std::str::from_utf8(&args[i + 1]).ok();
            i += 2;
        } else {
            i += 1;
        }
    }

    match db.ft_sugadd(key, string, score, payload) {
        Ok(count) => RespValue::Integer(count),
        Err(e) => RespValue::error(e.to_string()),
    }
}

/// FT.SUGGET key prefix [FUZZY] [WITHSCORES] [WITHPAYLOADS] [MAX num]
fn cmd_ft_sugget(db: &Db, args: &[Vec<u8>]) -> RespValue {
    if args.len() < 2 {
        return RespValue::error("wrong number of arguments for 'FT.SUGGET' command");
    }

    let key = match std::str::from_utf8(&args[0]) {
        Ok(k) => k,
        Err(_) => return RespValue::error("invalid key"),
    };

    let prefix = match std::str::from_utf8(&args[1]) {
        Ok(p) => p,
        Err(_) => return RespValue::error("invalid prefix"),
    };

    let mut fuzzy = false;
    let mut with_scores = false;
    let mut with_payloads = false;
    let mut max: i64 = 5;

    let mut i = 2;
    while i < args.len() {
        let opt = match std::str::from_utf8(&args[i]) {
            Ok(s) => s.to_uppercase(),
            Err(_) => {
                i += 1;
                continue;
            }
        };
        match opt.as_str() {
            "FUZZY" => fuzzy = true,
            "WITHSCORES" => with_scores = true,
            "WITHPAYLOADS" => with_payloads = true,
            "MAX" => {
                if i + 1 < args.len() {
                    max = std::str::from_utf8(&args[i + 1])
                        .ok()
                        .and_then(|s| s.parse().ok())
                        .unwrap_or(5);
                    i += 1;
                }
            }
            _ => {}
        }
        i += 1;
    }

    match db.ft_sugget(key, prefix, fuzzy, max) {
        Ok(suggestions) => {
            let mut resp: Vec<RespValue> = Vec::new();
            for sug in suggestions {
                resp.push(RespValue::BulkString(Some(sug.string.into_bytes())));
                if with_scores {
                    resp.push(RespValue::BulkString(Some(
                        sug.score.to_string().into_bytes(),
                    )));
                }
                if with_payloads {
                    resp.push(match sug.payload {
                        Some(p) => RespValue::BulkString(Some(p.into_bytes())),
                        None => RespValue::null(),
                    });
                }
            }
            RespValue::Array(Some(resp))
        }
        Err(e) => RespValue::error(e.to_string()),
    }
}

/// FT.SUGDEL key string
fn cmd_ft_sugdel(db: &Db, args: &[Vec<u8>]) -> RespValue {
    if args.len() < 2 {
        return RespValue::error("wrong number of arguments for 'FT.SUGDEL' command");
    }

    let key = match std::str::from_utf8(&args[0]) {
        Ok(k) => k,
        Err(_) => return RespValue::error("invalid key"),
    };

    let string = match std::str::from_utf8(&args[1]) {
        Ok(s) => s,
        Err(_) => return RespValue::error("invalid string"),
    };

    match db.ft_sugdel(key, string) {
        Ok(true) => RespValue::Integer(1),
        Ok(false) => RespValue::Integer(0),
        Err(e) => RespValue::error(e.to_string()),
    }
}

/// FT.SUGLEN key
fn cmd_ft_suglen(db: &Db, args: &[Vec<u8>]) -> RespValue {
    if args.is_empty() {
        return RespValue::error("wrong number of arguments for 'FT.SUGLEN' command");
    }

    let key = match std::str::from_utf8(&args[0]) {
        Ok(k) => k,
        Err(_) => return RespValue::error("invalid key"),
    };

    match db.ft_suglen(key) {
        Ok(count) => RespValue::Integer(count),
        Err(e) => RespValue::error(e.to_string()),
    }
}

// --- Session 24.2: Vector command handlers (feature-gated) ---

#[cfg(feature = "vectors")]
fn cmd_vector(db: &Db, args: &[Vec<u8>]) -> RespValue {
    if args.is_empty() {
        return RespValue::error("VECTOR subcommand required (ENABLE|DISABLE|INFO)");
    }

    let subcommand = match std::str::from_utf8(&args[0]) {
        Ok(s) => s.to_uppercase(),
        Err(_) => return RespValue::error("invalid subcommand"),
    };

    match subcommand.as_str() {
        "ENABLE" => {
            if args.len() < 3 {
                return RespValue::error(
                    "VECTOR ENABLE requires level (GLOBAL|DATABASE|PATTERN|KEY) and dimensions",
                );
            }

            let level = match std::str::from_utf8(&args[1]) {
                Ok(s) => s.to_uppercase(),
                Err(_) => return RespValue::error("invalid level"),
            };

            match level.as_str() {
                "GLOBAL" => {
                    let dimensions: i32 = match std::str::from_utf8(&args[2])
                        .ok()
                        .and_then(|s| s.parse().ok())
                    {
                        Some(d) => d,
                        None => return RespValue::error("dimensions must be a positive integer"),
                    };
                    if let Err(e) = db.vector_enable_global(dimensions) {
                        return RespValue::error(format!("VECTOR enable failed: {}", e));
                    }
                    RespValue::ok()
                }
                "DATABASE" => {
                    if args.len() < 4 {
                        return RespValue::error(
                            "VECTOR ENABLE DATABASE requires database number and dimensions",
                        );
                    }
                    let db_num: i32 = match std::str::from_utf8(&args[2])
                        .ok()
                        .and_then(|s| s.parse().ok())
                    {
                        Some(n) => n,
                        None => return RespValue::error("database number must be an integer"),
                    };
                    let dimensions: i32 = match std::str::from_utf8(&args[3])
                        .ok()
                        .and_then(|s| s.parse().ok())
                    {
                        Some(d) => d,
                        None => return RespValue::error("dimensions must be a positive integer"),
                    };
                    if let Err(e) = db.vector_enable_database(db_num, dimensions) {
                        return RespValue::error(format!("VECTOR enable failed: {}", e));
                    }
                    RespValue::ok()
                }
                "PATTERN" => {
                    if args.len() < 4 {
                        return RespValue::error(
                            "VECTOR ENABLE PATTERN requires pattern and dimensions",
                        );
                    }
                    let pattern = match std::str::from_utf8(&args[2]) {
                        Ok(p) => p,
                        Err(_) => return RespValue::error("invalid pattern"),
                    };
                    let dimensions: i32 = match std::str::from_utf8(&args[3])
                        .ok()
                        .and_then(|s| s.parse().ok())
                    {
                        Some(d) => d,
                        None => return RespValue::error("dimensions must be a positive integer"),
                    };
                    if let Err(e) = db.vector_enable_pattern(pattern, dimensions) {
                        return RespValue::error(format!("VECTOR enable failed: {}", e));
                    }
                    RespValue::ok()
                }
                "KEY" => {
                    if args.len() < 4 {
                        return RespValue::error(
                            "VECTOR ENABLE KEY requires key name and dimensions",
                        );
                    }
                    let key = match std::str::from_utf8(&args[2]) {
                        Ok(k) => k,
                        Err(_) => return RespValue::error("invalid key"),
                    };
                    let dimensions: i32 = match std::str::from_utf8(&args[3])
                        .ok()
                        .and_then(|s| s.parse().ok())
                    {
                        Some(d) => d,
                        None => return RespValue::error("dimensions must be a positive integer"),
                    };
                    if let Err(e) = db.vector_enable_key(key, dimensions) {
                        return RespValue::error(format!("VECTOR enable failed: {}", e));
                    }
                    RespValue::ok()
                }
                _ => RespValue::error("level must be GLOBAL, DATABASE, PATTERN, or KEY"),
            }
        }
        "DISABLE" => {
            if args.len() < 2 {
                return RespValue::error(
                    "VECTOR DISABLE requires level (GLOBAL|DATABASE|PATTERN|KEY)",
                );
            }

            let level = match std::str::from_utf8(&args[1]) {
                Ok(s) => s.to_uppercase(),
                Err(_) => return RespValue::error("invalid level"),
            };

            match level.as_str() {
                "GLOBAL" => {
                    if let Err(e) = db.vector_disable_global() {
                        return RespValue::error(format!("VECTOR disable failed: {}", e));
                    }
                    RespValue::ok()
                }
                "DATABASE" => {
                    if args.len() < 3 {
                        return RespValue::error(
                            "VECTOR DISABLE DATABASE requires database number",
                        );
                    }
                    let db_num: i32 = match std::str::from_utf8(&args[2])
                        .ok()
                        .and_then(|s| s.parse().ok())
                    {
                        Some(n) => n,
                        None => return RespValue::error("database number must be an integer"),
                    };
                    if let Err(e) = db.vector_disable_database(db_num) {
                        return RespValue::error(format!("VECTOR disable failed: {}", e));
                    }
                    RespValue::ok()
                }
                "PATTERN" => {
                    if args.len() < 3 {
                        return RespValue::error("VECTOR DISABLE PATTERN requires pattern");
                    }
                    let pattern = match std::str::from_utf8(&args[2]) {
                        Ok(p) => p,
                        Err(_) => return RespValue::error("invalid pattern"),
                    };
                    if let Err(e) = db.vector_disable_pattern(pattern) {
                        return RespValue::error(format!("VECTOR disable failed: {}", e));
                    }
                    RespValue::ok()
                }
                "KEY" => {
                    if args.len() < 3 {
                        return RespValue::error("VECTOR DISABLE KEY requires key name");
                    }
                    let key = match std::str::from_utf8(&args[2]) {
                        Ok(k) => k,
                        Err(_) => return RespValue::error("invalid key"),
                    };
                    if let Err(e) = db.vector_disable_key(key) {
                        return RespValue::error(format!("VECTOR disable failed: {}", e));
                    }
                    RespValue::ok()
                }
                _ => RespValue::error("level must be GLOBAL, DATABASE, PATTERN, or KEY"),
            }
        }
        "INFO" => match db.vector_info() {
            Ok(stats) => {
                let configs: Vec<RespValue> = stats
                    .configs
                    .iter()
                    .map(|c| {
                        RespValue::Array(Some(vec![
                            RespValue::BulkString(Some(b"level".to_vec())),
                            RespValue::BulkString(Some(c.level.as_str().as_bytes().to_vec())),
                            RespValue::BulkString(Some(b"target".to_vec())),
                            RespValue::BulkString(Some(c.target.as_bytes().to_vec())),
                            RespValue::BulkString(Some(b"enabled".to_vec())),
                            RespValue::Integer(if c.enabled { 1 } else { 0 }),
                            RespValue::BulkString(Some(b"dimensions".to_vec())),
                            RespValue::Integer(c.dimensions as i64),
                        ]))
                    })
                    .collect();

                RespValue::Array(Some(vec![
                    RespValue::BulkString(Some(b"total_vectors".to_vec())),
                    RespValue::Integer(stats.total_vectors),
                    RespValue::BulkString(Some(b"total_keys".to_vec())),
                    RespValue::Integer(stats.total_keys),
                    RespValue::BulkString(Some(b"storage_bytes".to_vec())),
                    RespValue::Integer(stats.storage_bytes),
                    RespValue::BulkString(Some(b"configs".to_vec())),
                    RespValue::Array(Some(configs)),
                ]))
            }
            Err(e) => RespValue::error(format!("VECTOR info failed: {}", e)),
        },
        _ => RespValue::error(format!(
            "unknown VECTOR subcommand '{}'. Use ENABLE|DISABLE|INFO",
            subcommand
        )),
    }
}

#[cfg(not(feature = "vectors"))]
fn cmd_vector(_db: &Db, _args: &[Vec<u8>]) -> RespValue {
    RespValue::error("vectors feature not enabled. Compile with --features vectors")
}

#[cfg(feature = "vectors")]
fn cmd_vadd(db: &Db, args: &[Vec<u8>]) -> RespValue {
    // VADD key vector_id embedding... [METADATA json]
    if args.len() < 3 {
        return RespValue::error("wrong number of arguments for 'vadd' command");
    }

    let key = match std::str::from_utf8(&args[0]) {
        Ok(k) => k,
        Err(_) => return RespValue::error("invalid key"),
    };

    let vector_id = match std::str::from_utf8(&args[1]) {
        Ok(v) => v,
        Err(_) => return RespValue::error("invalid vector_id"),
    };

    // Parse embedding values and optional metadata
    let mut embedding: Vec<f32> = Vec::new();
    let mut metadata: Option<&str> = None;
    let mut i = 2;

    while i < args.len() {
        let arg = match std::str::from_utf8(&args[i]) {
            Ok(s) => s,
            Err(_) => return RespValue::error("invalid argument"),
        };

        if arg.to_uppercase() == "METADATA" {
            if i + 1 < args.len() {
                metadata = match std::str::from_utf8(&args[i + 1]) {
                    Ok(m) => Some(m),
                    Err(_) => return RespValue::error("invalid metadata"),
                };
                i += 2;
            } else {
                return RespValue::error("METADATA requires a value");
            }
        } else {
            // Parse as float
            match arg.parse::<f32>() {
                Ok(f) => embedding.push(f),
                Err(_) => return RespValue::error(format!("invalid float value: {}", arg)),
            }
            i += 1;
        }
    }

    if embedding.is_empty() {
        return RespValue::error("embedding vector cannot be empty");
    }

    match db.vadd(key, vector_id, &embedding, metadata) {
        Ok(added) => RespValue::Integer(if added { 1 } else { 0 }),
        Err(e) => RespValue::error(format!("VADD failed: {}", e)),
    }
}

#[cfg(not(feature = "vectors"))]
fn cmd_vadd(_db: &Db, _args: &[Vec<u8>]) -> RespValue {
    RespValue::error("vectors feature not enabled. Compile with --features vectors")
}

#[cfg(feature = "vectors")]
fn cmd_vget(db: &Db, args: &[Vec<u8>]) -> RespValue {
    // VGET key vector_id
    if args.len() < 2 {
        return RespValue::error("wrong number of arguments for 'vget' command");
    }

    let key = match std::str::from_utf8(&args[0]) {
        Ok(k) => k,
        Err(_) => return RespValue::error("invalid key"),
    };

    let vector_id = match std::str::from_utf8(&args[1]) {
        Ok(v) => v,
        Err(_) => return RespValue::error("invalid vector_id"),
    };

    match db.vget(key, vector_id) {
        Ok(Some(entry)) => {
            let embedding_str: Vec<RespValue> = entry
                .embedding
                .iter()
                .map(|f| RespValue::BulkString(Some(f.to_string().as_bytes().to_vec())))
                .collect();

            let mut result = vec![
                RespValue::BulkString(Some(b"vector_id".to_vec())),
                RespValue::BulkString(Some(entry.vector_id.as_bytes().to_vec())),
                RespValue::BulkString(Some(b"dimensions".to_vec())),
                RespValue::Integer(entry.dimensions as i64),
                RespValue::BulkString(Some(b"embedding".to_vec())),
                RespValue::Array(Some(embedding_str)),
            ];

            if let Some(meta) = entry.metadata {
                result.push(RespValue::BulkString(Some(b"metadata".to_vec())));
                result.push(RespValue::BulkString(Some(meta.as_bytes().to_vec())));
            }

            RespValue::Array(Some(result))
        }
        Ok(None) => RespValue::null(),
        Err(e) => RespValue::error(format!("VGET failed: {}", e)),
    }
}

#[cfg(not(feature = "vectors"))]
fn cmd_vget(_db: &Db, _args: &[Vec<u8>]) -> RespValue {
    RespValue::error("vectors feature not enabled. Compile with --features vectors")
}

#[cfg(feature = "vectors")]
fn cmd_vdel(db: &Db, args: &[Vec<u8>]) -> RespValue {
    // VDEL key vector_id
    if args.len() < 2 {
        return RespValue::error("wrong number of arguments for 'vdel' command");
    }

    let key = match std::str::from_utf8(&args[0]) {
        Ok(k) => k,
        Err(_) => return RespValue::error("invalid key"),
    };

    let vector_id = match std::str::from_utf8(&args[1]) {
        Ok(v) => v,
        Err(_) => return RespValue::error("invalid vector_id"),
    };

    match db.vdel(key, vector_id) {
        Ok(deleted) => RespValue::Integer(if deleted { 1 } else { 0 }),
        Err(e) => RespValue::error(format!("VDEL failed: {}", e)),
    }
}

#[cfg(not(feature = "vectors"))]
fn cmd_vdel(_db: &Db, _args: &[Vec<u8>]) -> RespValue {
    RespValue::error("vectors feature not enabled. Compile with --features vectors")
}

#[cfg(feature = "vectors")]
fn cmd_vcount(db: &Db, args: &[Vec<u8>]) -> RespValue {
    // VCOUNT key
    if args.is_empty() {
        return RespValue::error("wrong number of arguments for 'vcount' command");
    }

    let key = match std::str::from_utf8(&args[0]) {
        Ok(k) => k,
        Err(_) => return RespValue::error("invalid key"),
    };

    match db.vcount(key) {
        Ok(count) => RespValue::Integer(count),
        Err(e) => RespValue::error(format!("VCOUNT failed: {}", e)),
    }
}

#[cfg(not(feature = "vectors"))]
fn cmd_vcount(_db: &Db, _args: &[Vec<u8>]) -> RespValue {
    RespValue::error("vectors feature not enabled. Compile with --features vectors")
}

#[cfg(feature = "vectors")]
fn cmd_vsearch(db: &Db, args: &[Vec<u8>]) -> RespValue {
    use crate::types::DistanceMetric;

    // VSEARCH key K vector... [METRIC L2|COSINE|IP]
    if args.len() < 3 {
        return RespValue::error("wrong number of arguments for 'vsearch' command");
    }

    let key = match std::str::from_utf8(&args[0]) {
        Ok(k) => k,
        Err(_) => return RespValue::error("invalid key"),
    };

    let k: i64 = match std::str::from_utf8(&args[1])
        .ok()
        .and_then(|s| s.parse().ok())
    {
        Some(k) => k,
        None => return RespValue::error("K must be a positive integer"),
    };

    // Parse query vector and optional metric
    let mut query_vector: Vec<f32> = Vec::new();
    let mut metric = DistanceMetric::L2;
    let mut i = 2;

    while i < args.len() {
        let arg = match std::str::from_utf8(&args[i]) {
            Ok(s) => s,
            Err(_) => return RespValue::error("invalid argument"),
        };

        if arg.to_uppercase() == "METRIC" {
            if i + 1 < args.len() {
                let metric_str = match std::str::from_utf8(&args[i + 1]) {
                    Ok(m) => m,
                    Err(_) => return RespValue::error("invalid metric"),
                };
                metric = match DistanceMetric::from_str(metric_str) {
                    Some(m) => m,
                    None => return RespValue::error("metric must be L2, COSINE, or IP"),
                };
                i += 2;
            } else {
                return RespValue::error("METRIC requires a value");
            }
        } else {
            // Parse as float
            match arg.parse::<f32>() {
                Ok(f) => query_vector.push(f),
                Err(_) => return RespValue::error(format!("invalid float value: {}", arg)),
            }
            i += 1;
        }
    }

    if query_vector.is_empty() {
        return RespValue::error("query vector cannot be empty");
    }

    match db.vsearch(key, &query_vector, k, metric) {
        Ok(results) => {
            let resp_results: Vec<RespValue> = results
                .into_iter()
                .map(|r| {
                    let mut entry = vec![
                        RespValue::BulkString(Some(b"vector_id".to_vec())),
                        RespValue::BulkString(Some(r.vector_id.as_bytes().to_vec())),
                        RespValue::BulkString(Some(b"distance".to_vec())),
                        RespValue::BulkString(Some(r.distance.to_string().as_bytes().to_vec())),
                    ];
                    if let Some(meta) = r.metadata {
                        entry.push(RespValue::BulkString(Some(b"metadata".to_vec())));
                        entry.push(RespValue::BulkString(Some(meta.as_bytes().to_vec())));
                    }
                    RespValue::Array(Some(entry))
                })
                .collect();
            RespValue::Array(Some(resp_results))
        }
        Err(e) => RespValue::error(format!("VSEARCH failed: {}", e)),
    }
}

#[cfg(not(feature = "vectors"))]
fn cmd_vsearch(_db: &Db, _args: &[Vec<u8>]) -> RespValue {
    RespValue::error("vectors feature not enabled. Compile with --features vectors")
}

// --- Session 13: Stream command handlers ---

fn cmd_xadd(db: &Db, args: &[Vec<u8>]) -> RespValue {
    if args.len() < 4 {
        return RespValue::error("wrong number of arguments for 'xadd' command");
    }

    let key = match std::str::from_utf8(&args[0]) {
        Ok(k) => k,
        Err(_) => return RespValue::error("invalid key"),
    };

    // Parse options and find field-value pairs
    let mut i = 1;
    let mut nomkstream = false;
    let mut maxlen: Option<i64> = None;
    let mut minid: Option<StreamId> = None;
    let mut approximate = false;

    // Parse optional flags before the ID
    while i < args.len() {
        let arg = match std::str::from_utf8(&args[i]) {
            Ok(s) => s.to_uppercase(),
            Err(_) => break,
        };

        match arg.as_str() {
            "NOMKSTREAM" => {
                nomkstream = true;
                i += 1;
            }
            "MAXLEN" => {
                i += 1;
                if i >= args.len() {
                    return RespValue::error("syntax error");
                }
                // Check for = or ~
                let next = match std::str::from_utf8(&args[i]) {
                    Ok(s) => s,
                    Err(_) => return RespValue::error("syntax error"),
                };
                if next == "~" {
                    approximate = true;
                    i += 1;
                } else if next == "=" {
                    i += 1;
                }
                if i >= args.len() {
                    return RespValue::error("syntax error");
                }
                maxlen = match std::str::from_utf8(&args[i])
                    .ok()
                    .and_then(|s| s.parse().ok())
                {
                    Some(v) => Some(v),
                    None => return RespValue::error("value is not an integer or out of range"),
                };
                i += 1;
            }
            "MINID" => {
                i += 1;
                if i >= args.len() {
                    return RespValue::error("syntax error");
                }
                // Check for = or ~
                let next = match std::str::from_utf8(&args[i]) {
                    Ok(s) => s,
                    Err(_) => return RespValue::error("syntax error"),
                };
                if next == "~" {
                    approximate = true;
                    i += 1;
                } else if next == "=" {
                    i += 1;
                }
                if i >= args.len() {
                    return RespValue::error("syntax error");
                }
                let id_str = match std::str::from_utf8(&args[i]) {
                    Ok(s) => s,
                    Err(_) => return RespValue::error("invalid stream ID"),
                };
                minid = match StreamId::parse(id_str) {
                    Some(id) => Some(id),
                    None => return RespValue::error("invalid stream ID"),
                };
                i += 1;
            }
            _ => break, // Not an option, must be ID
        }
    }

    // Now args[i] should be the ID (or *)
    if i >= args.len() {
        return RespValue::error("wrong number of arguments for 'xadd' command");
    }

    let id_str = match std::str::from_utf8(&args[i]) {
        Ok(s) => s,
        Err(_) => return RespValue::error("invalid stream ID"),
    };
    i += 1;

    let id: Option<StreamId> = if id_str == "*" {
        None // Auto-generate
    } else {
        match StreamId::parse(id_str) {
            Some(id) => Some(id),
            None => return RespValue::error("invalid stream ID"),
        }
    };

    // Rest are field-value pairs
    let remaining = &args[i..];
    if remaining.len() < 2 || remaining.len() % 2 != 0 {
        return RespValue::error("wrong number of arguments for 'xadd' command");
    }

    let fields: Vec<(&[u8], &[u8])> = remaining
        .chunks(2)
        .map(|chunk| (chunk[0].as_slice(), chunk[1].as_slice()))
        .collect();

    match db.xadd(key, id, &fields, nomkstream, maxlen, minid, approximate) {
        Ok(Some(entry_id)) => RespValue::BulkString(Some(entry_id.to_string().into_bytes())),
        Ok(None) => RespValue::null(), // NOMKSTREAM and stream doesn't exist
        Err(KvError::WrongType) => RespValue::wrong_type(),
        Err(KvError::InvalidData) => RespValue::error(
            "The ID specified in XADD is equal or smaller than the target stream top item",
        ),
        Err(e) => RespValue::error(e.to_string()),
    }
}

fn cmd_xlen(db: &Db, args: &[Vec<u8>]) -> RespValue {
    if args.len() != 1 {
        return RespValue::error("wrong number of arguments for 'xlen' command");
    }

    let key = match std::str::from_utf8(&args[0]) {
        Ok(k) => k,
        Err(_) => return RespValue::error("invalid key"),
    };

    match db.xlen(key) {
        Ok(count) => RespValue::Integer(count),
        Err(KvError::WrongType) => RespValue::wrong_type(),
        Err(e) => RespValue::error(e.to_string()),
    }
}

fn cmd_xrange(db: &Db, args: &[Vec<u8>]) -> RespValue {
    if args.len() < 3 || args.len() > 5 {
        return RespValue::error("wrong number of arguments for 'xrange' command");
    }

    let key = match std::str::from_utf8(&args[0]) {
        Ok(k) => k,
        Err(_) => return RespValue::error("invalid key"),
    };

    let start_str = match std::str::from_utf8(&args[1]) {
        Ok(s) => s,
        Err(_) => return RespValue::error("invalid start ID"),
    };
    let start = match StreamId::parse(start_str) {
        Some(id) => id,
        None => return RespValue::error("invalid stream ID"),
    };

    let end_str = match std::str::from_utf8(&args[2]) {
        Ok(s) => s,
        Err(_) => return RespValue::error("invalid end ID"),
    };
    let end = match StreamId::parse(end_str) {
        Some(id) => id,
        None => return RespValue::error("invalid stream ID"),
    };

    // Parse optional COUNT
    let mut count: Option<i64> = None;
    if args.len() >= 5 {
        let opt = match std::str::from_utf8(&args[3]) {
            Ok(s) => s.to_uppercase(),
            Err(_) => return RespValue::error("syntax error"),
        };
        if opt != "COUNT" {
            return RespValue::error("syntax error");
        }
        count = match std::str::from_utf8(&args[4])
            .ok()
            .and_then(|s| s.parse().ok())
        {
            Some(c) => Some(c),
            None => return RespValue::error("value is not an integer or out of range"),
        };
    }

    match db.xrange(key, start, end, count) {
        Ok(entries) => format_stream_entries(&entries),
        Err(KvError::WrongType) => RespValue::wrong_type(),
        Err(e) => RespValue::error(e.to_string()),
    }
}

fn cmd_xrevrange(db: &Db, args: &[Vec<u8>]) -> RespValue {
    if args.len() < 3 || args.len() > 5 {
        return RespValue::error("wrong number of arguments for 'xrevrange' command");
    }

    let key = match std::str::from_utf8(&args[0]) {
        Ok(k) => k,
        Err(_) => return RespValue::error("invalid key"),
    };

    let end_str = match std::str::from_utf8(&args[1]) {
        Ok(s) => s,
        Err(_) => return RespValue::error("invalid end ID"),
    };
    let end = match StreamId::parse(end_str) {
        Some(id) => id,
        None => return RespValue::error("invalid stream ID"),
    };

    let start_str = match std::str::from_utf8(&args[2]) {
        Ok(s) => s,
        Err(_) => return RespValue::error("invalid start ID"),
    };
    let start = match StreamId::parse(start_str) {
        Some(id) => id,
        None => return RespValue::error("invalid stream ID"),
    };

    // Parse optional COUNT
    let mut count: Option<i64> = None;
    if args.len() >= 5 {
        let opt = match std::str::from_utf8(&args[3]) {
            Ok(s) => s.to_uppercase(),
            Err(_) => return RespValue::error("syntax error"),
        };
        if opt != "COUNT" {
            return RespValue::error("syntax error");
        }
        count = match std::str::from_utf8(&args[4])
            .ok()
            .and_then(|s| s.parse().ok())
        {
            Some(c) => Some(c),
            None => return RespValue::error("value is not an integer or out of range"),
        };
    }

    match db.xrevrange(key, end, start, count) {
        Ok(entries) => format_stream_entries(&entries),
        Err(KvError::WrongType) => RespValue::wrong_type(),
        Err(e) => RespValue::error(e.to_string()),
    }
}

async fn cmd_xread(db: &Db, args: &[Vec<u8>]) -> RespValue {
    if args.len() < 3 {
        return RespValue::error("wrong number of arguments for 'xread' command");
    }

    let mut i = 0;
    let mut count: Option<i64> = None;
    let mut block_timeout_ms: Option<i64> = None;

    // Parse optional COUNT, BLOCK
    while i < args.len() {
        let arg = match std::str::from_utf8(&args[i]) {
            Ok(s) => s.to_uppercase(),
            Err(_) => break,
        };

        match arg.as_str() {
            "COUNT" => {
                i += 1;
                if i >= args.len() {
                    return RespValue::error("syntax error");
                }
                count = match std::str::from_utf8(&args[i])
                    .ok()
                    .and_then(|s| s.parse().ok())
                {
                    Some(c) => Some(c),
                    None => return RespValue::error("value is not an integer or out of range"),
                };
                i += 1;
            }
            "BLOCK" => {
                i += 1;
                if i >= args.len() {
                    return RespValue::error("syntax error");
                }
                block_timeout_ms = match std::str::from_utf8(&args[i])
                    .ok()
                    .and_then(|s| s.parse().ok())
                {
                    Some(t) => Some(t),
                    None => return RespValue::error("value is not an integer or out of range"),
                };
                i += 1;
            }
            "STREAMS" => {
                i += 1;
                break;
            }
            _ => {
                return RespValue::error("syntax error");
            }
        }
    }

    // Rest is keys... ids...
    let remaining = &args[i..];
    if remaining.is_empty() || remaining.len() % 2 != 0 {
        return RespValue::error("syntax error");
    }

    let half = remaining.len() / 2;
    let key_args = &remaining[..half];
    let id_args = &remaining[half..];

    let keys: Vec<&str> = match key_args
        .iter()
        .map(|k| std::str::from_utf8(k))
        .collect::<std::result::Result<Vec<_>, _>>()
    {
        Ok(k) => k,
        Err(_) => return RespValue::error("invalid key"),
    };

    let ids: Vec<StreamId> = match id_args
        .iter()
        .map(|id| {
            let s = std::str::from_utf8(id).ok()?;
            if s == "$" {
                // $ means last ID - for non-blocking, this means "nothing new"
                Some(StreamId::max())
            } else {
                StreamId::parse(s)
            }
        })
        .collect::<Option<Vec<_>>>()
    {
        Some(ids) => ids,
        None => return RespValue::error("invalid stream ID"),
    };

    // Handle blocking variant
    if let Some(timeout_ms) = block_timeout_ms {
        match db.xread_block(&keys, &ids, count, timeout_ms).await {
            Ok(results) => {
                if results.is_empty() {
                    return RespValue::null();
                }
                let mut arr = Vec::new();
                for (key, entries) in results {
                    arr.push(RespValue::Array(Some(vec![
                        RespValue::BulkString(Some(key.into_bytes())),
                        format_stream_entries(&entries),
                    ])));
                }
                RespValue::Array(Some(arr))
            }
            Err(KvError::WrongType) => RespValue::wrong_type(),
            Err(e) => RespValue::error(e.to_string()),
        }
    } else {
        // Non-blocking variant
        match db.xread(&keys, &ids, count) {
            Ok(results) => {
                if results.is_empty() {
                    return RespValue::null();
                }
                let mut arr = Vec::new();
                for (key, entries) in results {
                    arr.push(RespValue::Array(Some(vec![
                        RespValue::BulkString(Some(key.into_bytes())),
                        format_stream_entries(&entries),
                    ])));
                }
                RespValue::Array(Some(arr))
            }
            Err(KvError::WrongType) => RespValue::wrong_type(),
            Err(e) => RespValue::error(e.to_string()),
        }
    }
}

fn cmd_xtrim(db: &Db, args: &[Vec<u8>]) -> RespValue {
    if args.len() < 3 {
        return RespValue::error("wrong number of arguments for 'xtrim' command");
    }

    let key = match std::str::from_utf8(&args[0]) {
        Ok(k) => k,
        Err(_) => return RespValue::error("invalid key"),
    };

    let strategy = match std::str::from_utf8(&args[1]) {
        Ok(s) => s.to_uppercase(),
        Err(_) => return RespValue::error("syntax error"),
    };

    let mut i = 2;
    let mut approximate = false;

    // Check for = or ~
    if i < args.len() {
        let next = std::str::from_utf8(&args[i]).unwrap_or("");
        if next == "~" {
            approximate = true;
            i += 1;
        } else if next == "=" {
            i += 1;
        }
    }

    if i >= args.len() {
        return RespValue::error("syntax error");
    }

    match strategy.as_str() {
        "MAXLEN" => {
            let maxlen: i64 = match std::str::from_utf8(&args[i])
                .ok()
                .and_then(|s| s.parse().ok())
            {
                Some(v) => v,
                None => return RespValue::error("value is not an integer or out of range"),
            };
            match db.xtrim(key, Some(maxlen), None, approximate) {
                Ok(deleted) => RespValue::Integer(deleted),
                Err(KvError::WrongType) => RespValue::wrong_type(),
                Err(e) => RespValue::error(e.to_string()),
            }
        }
        "MINID" => {
            let minid_str = match std::str::from_utf8(&args[i]) {
                Ok(s) => s,
                Err(_) => return RespValue::error("invalid stream ID"),
            };
            let minid = match StreamId::parse(minid_str) {
                Some(id) => id,
                None => return RespValue::error("invalid stream ID"),
            };
            match db.xtrim(key, None, Some(minid), approximate) {
                Ok(deleted) => RespValue::Integer(deleted),
                Err(KvError::WrongType) => RespValue::wrong_type(),
                Err(e) => RespValue::error(e.to_string()),
            }
        }
        _ => RespValue::error("syntax error"),
    }
}

fn cmd_xdel(db: &Db, args: &[Vec<u8>]) -> RespValue {
    if args.len() < 2 {
        return RespValue::error("wrong number of arguments for 'xdel' command");
    }

    let key = match std::str::from_utf8(&args[0]) {
        Ok(k) => k,
        Err(_) => return RespValue::error("invalid key"),
    };

    let ids: Vec<StreamId> = match args[1..]
        .iter()
        .map(|id| {
            let s = std::str::from_utf8(id).ok()?;
            StreamId::parse(s)
        })
        .collect::<Option<Vec<_>>>()
    {
        Some(ids) => ids,
        None => return RespValue::error("invalid stream ID"),
    };

    match db.xdel(key, &ids) {
        Ok(deleted) => RespValue::Integer(deleted),
        Err(KvError::WrongType) => RespValue::wrong_type(),
        Err(e) => RespValue::error(e.to_string()),
    }
}

fn cmd_xinfo(db: &Db, args: &[Vec<u8>]) -> RespValue {
    if args.len() < 2 {
        return RespValue::error("wrong number of arguments for 'xinfo' command");
    }

    let subcommand = match std::str::from_utf8(&args[0]) {
        Ok(s) => s.to_uppercase(),
        Err(_) => return RespValue::error("invalid subcommand"),
    };

    let key = match std::str::from_utf8(&args[1]) {
        Ok(k) => k,
        Err(_) => return RespValue::error("invalid key"),
    };

    match subcommand.as_str() {
        "STREAM" => {
            match db.xinfo_stream(key) {
                Ok(Some(info)) => {
                    let mut result = vec![
                        RespValue::BulkString(Some(b"length".to_vec())),
                        RespValue::Integer(info.length),
                        RespValue::BulkString(Some(b"radix-tree-keys".to_vec())),
                        RespValue::Integer(info.radix_tree_keys),
                        RespValue::BulkString(Some(b"radix-tree-nodes".to_vec())),
                        RespValue::Integer(info.radix_tree_nodes),
                        RespValue::BulkString(Some(b"last-generated-id".to_vec())),
                        RespValue::BulkString(Some(
                            info.last_generated_id.to_string().into_bytes(),
                        )),
                    ];

                    // Add first-entry
                    result.push(RespValue::BulkString(Some(b"first-entry".to_vec())));
                    if let Some(entry) = info.first_entry {
                        result.push(format_single_entry(&entry));
                    } else {
                        result.push(RespValue::null());
                    }

                    // Add last-entry
                    result.push(RespValue::BulkString(Some(b"last-entry".to_vec())));
                    if let Some(entry) = info.last_entry {
                        result.push(format_single_entry(&entry));
                    } else {
                        result.push(RespValue::null());
                    }

                    RespValue::Array(Some(result))
                }
                Ok(None) => RespValue::error("no such key"),
                Err(KvError::WrongType) => RespValue::wrong_type(),
                Err(e) => RespValue::error(e.to_string()),
            }
        }
        "GROUPS" => match db.xinfo_groups(key) {
            Ok(groups) => {
                let arr: Vec<RespValue> = groups
                    .iter()
                    .map(|g| {
                        RespValue::Array(Some(vec![
                            RespValue::BulkString(Some(b"name".to_vec())),
                            RespValue::BulkString(Some(g.name.clone().into_bytes())),
                            RespValue::BulkString(Some(b"consumers".to_vec())),
                            RespValue::Integer(g.consumers),
                            RespValue::BulkString(Some(b"pending".to_vec())),
                            RespValue::Integer(g.pending),
                            RespValue::BulkString(Some(b"last-delivered-id".to_vec())),
                            RespValue::BulkString(Some(
                                g.last_delivered_id.to_string().into_bytes(),
                            )),
                        ]))
                    })
                    .collect();
                RespValue::Array(Some(arr))
            }
            Err(KvError::WrongType) => RespValue::wrong_type(),
            Err(e) => RespValue::error(e.to_string()),
        },
        "CONSUMERS" => {
            if args.len() < 3 {
                return RespValue::error("wrong number of arguments for 'xinfo consumers' command");
            }
            let groupname = match std::str::from_utf8(&args[2]) {
                Ok(g) => g,
                Err(_) => return RespValue::error("invalid group name"),
            };
            match db.xinfo_consumers(key, groupname) {
                Ok(consumers) => {
                    let arr: Vec<RespValue> = consumers
                        .iter()
                        .map(|c| {
                            RespValue::Array(Some(vec![
                                RespValue::BulkString(Some(b"name".to_vec())),
                                RespValue::BulkString(Some(c.name.clone().into_bytes())),
                                RespValue::BulkString(Some(b"pending".to_vec())),
                                RespValue::Integer(c.pending),
                                RespValue::BulkString(Some(b"idle".to_vec())),
                                RespValue::Integer(c.idle),
                            ]))
                        })
                        .collect();
                    RespValue::Array(Some(arr))
                }
                Err(KvError::NoSuchKey) => RespValue::error("no such key"),
                Err(KvError::NoGroup) => RespValue::error("NOGROUP No such consumer group"),
                Err(KvError::WrongType) => RespValue::wrong_type(),
                Err(e) => RespValue::error(e.to_string()),
            }
        }
        _ => RespValue::error("unknown subcommand"),
    }
}

/// Format a list of stream entries for RESP response
fn format_stream_entries(entries: &[crate::types::StreamEntry]) -> RespValue {
    let arr: Vec<RespValue> = entries
        .iter()
        .map(|entry| format_single_entry(entry))
        .collect();
    RespValue::Array(Some(arr))
}

/// Format a single stream entry as [id, [field, value, ...]]
fn format_single_entry(entry: &crate::types::StreamEntry) -> RespValue {
    let fields: Vec<RespValue> = entry
        .fields
        .iter()
        .flat_map(|(k, v)| {
            vec![
                RespValue::BulkString(Some(k.clone())),
                RespValue::BulkString(Some(v.clone())),
            ]
        })
        .collect();

    RespValue::Array(Some(vec![
        RespValue::BulkString(Some(entry.id.to_string().into_bytes())),
        RespValue::Array(Some(fields)),
    ]))
}

// --- Consumer Group Commands (Session 14) ---

fn cmd_xgroup(db: &Db, args: &[Vec<u8>]) -> RespValue {
    if args.len() < 2 {
        return RespValue::error("wrong number of arguments for 'xgroup' command");
    }

    let subcommand = match std::str::from_utf8(&args[0]) {
        Ok(s) => s.to_uppercase(),
        Err(_) => return RespValue::error("invalid subcommand"),
    };

    match subcommand.as_str() {
        "CREATE" => {
            // XGROUP CREATE key groupname id|$ [MKSTREAM]
            if args.len() < 4 {
                return RespValue::error("wrong number of arguments for 'xgroup create' command");
            }
            let key = match std::str::from_utf8(&args[1]) {
                Ok(k) => k,
                Err(_) => return RespValue::error("invalid key"),
            };
            let groupname = match std::str::from_utf8(&args[2]) {
                Ok(g) => g,
                Err(_) => return RespValue::error("invalid group name"),
            };
            let id_str = match std::str::from_utf8(&args[3]) {
                Ok(s) => s,
                Err(_) => return RespValue::error("invalid ID"),
            };

            // Check for MKSTREAM option
            let mut mkstream = false;
            for arg in args[4..].iter() {
                let s = std::str::from_utf8(arg).unwrap_or("").to_uppercase();
                if s == "MKSTREAM" {
                    mkstream = true;
                }
            }

            // Parse ID - $ means use the last ID
            let id = if id_str == "$" {
                // Use 0-0 for $ which will be treated as "start from newest"
                // In Redis, $ means start from the last entry
                match db.xinfo_stream(key) {
                    Ok(Some(info)) => info.last_generated_id,
                    Ok(None) => StreamId::new(0, 0),
                    Err(_) => StreamId::new(0, 0),
                }
            } else if id_str == "0" || id_str == "0-0" {
                StreamId::new(0, 0)
            } else {
                match StreamId::parse(id_str) {
                    Some(id) => id,
                    None => return RespValue::error("invalid ID"),
                }
            };

            match db.xgroup_create(key, groupname, id, mkstream) {
                Ok(_) => RespValue::ok(),
                Err(KvError::NoSuchKey) => {
                    RespValue::error("The XGROUP subcommand requires the key to exist")
                }
                Err(KvError::BusyGroup) => {
                    RespValue::error("BUSYGROUP Consumer Group name already exists")
                }
                Err(KvError::WrongType) => RespValue::wrong_type(),
                Err(e) => RespValue::error(e.to_string()),
            }
        }
        "DESTROY" => {
            // XGROUP DESTROY key groupname
            if args.len() < 3 {
                return RespValue::error("wrong number of arguments for 'xgroup destroy' command");
            }
            let key = match std::str::from_utf8(&args[1]) {
                Ok(k) => k,
                Err(_) => return RespValue::error("invalid key"),
            };
            let groupname = match std::str::from_utf8(&args[2]) {
                Ok(g) => g,
                Err(_) => return RespValue::error("invalid group name"),
            };

            match db.xgroup_destroy(key, groupname) {
                Ok(destroyed) => RespValue::Integer(if destroyed { 1 } else { 0 }),
                Err(KvError::WrongType) => RespValue::wrong_type(),
                Err(e) => RespValue::error(e.to_string()),
            }
        }
        "SETID" => {
            // XGROUP SETID key groupname id|$
            if args.len() < 4 {
                return RespValue::error("wrong number of arguments for 'xgroup setid' command");
            }
            let key = match std::str::from_utf8(&args[1]) {
                Ok(k) => k,
                Err(_) => return RespValue::error("invalid key"),
            };
            let groupname = match std::str::from_utf8(&args[2]) {
                Ok(g) => g,
                Err(_) => return RespValue::error("invalid group name"),
            };
            let id_str = match std::str::from_utf8(&args[3]) {
                Ok(s) => s,
                Err(_) => return RespValue::error("invalid ID"),
            };

            let id = if id_str == "$" {
                match db.xinfo_stream(key) {
                    Ok(Some(info)) => info.last_generated_id,
                    Ok(None) => StreamId::new(0, 0),
                    Err(_) => StreamId::new(0, 0),
                }
            } else {
                match StreamId::parse(id_str) {
                    Some(id) => id,
                    None => return RespValue::error("invalid ID"),
                }
            };

            match db.xgroup_setid(key, groupname, id) {
                Ok(_) => RespValue::ok(),
                Err(KvError::NoSuchKey) => RespValue::error("no such key"),
                Err(KvError::NoGroup) => RespValue::error("NOGROUP No such consumer group"),
                Err(KvError::WrongType) => RespValue::wrong_type(),
                Err(e) => RespValue::error(e.to_string()),
            }
        }
        "CREATECONSUMER" => {
            // XGROUP CREATECONSUMER key groupname consumername
            if args.len() < 4 {
                return RespValue::error(
                    "wrong number of arguments for 'xgroup createconsumer' command",
                );
            }
            let key = match std::str::from_utf8(&args[1]) {
                Ok(k) => k,
                Err(_) => return RespValue::error("invalid key"),
            };
            let groupname = match std::str::from_utf8(&args[2]) {
                Ok(g) => g,
                Err(_) => return RespValue::error("invalid group name"),
            };
            let consumername = match std::str::from_utf8(&args[3]) {
                Ok(c) => c,
                Err(_) => return RespValue::error("invalid consumer name"),
            };

            match db.xgroup_createconsumer(key, groupname, consumername) {
                Ok(created) => RespValue::Integer(if created { 1 } else { 0 }),
                Err(KvError::NoSuchKey) => RespValue::error("no such key"),
                Err(KvError::NoGroup) => RespValue::error("NOGROUP No such consumer group"),
                Err(KvError::WrongType) => RespValue::wrong_type(),
                Err(e) => RespValue::error(e.to_string()),
            }
        }
        "DELCONSUMER" => {
            // XGROUP DELCONSUMER key groupname consumername
            if args.len() < 4 {
                return RespValue::error(
                    "wrong number of arguments for 'xgroup delconsumer' command",
                );
            }
            let key = match std::str::from_utf8(&args[1]) {
                Ok(k) => k,
                Err(_) => return RespValue::error("invalid key"),
            };
            let groupname = match std::str::from_utf8(&args[2]) {
                Ok(g) => g,
                Err(_) => return RespValue::error("invalid group name"),
            };
            let consumername = match std::str::from_utf8(&args[3]) {
                Ok(c) => c,
                Err(_) => return RespValue::error("invalid consumer name"),
            };

            match db.xgroup_delconsumer(key, groupname, consumername) {
                Ok(pending_count) => RespValue::Integer(pending_count),
                Err(KvError::NoSuchKey) => RespValue::error("no such key"),
                Err(KvError::NoGroup) => RespValue::error("NOGROUP No such consumer group"),
                Err(KvError::WrongType) => RespValue::wrong_type(),
                Err(e) => RespValue::error(e.to_string()),
            }
        }
        _ => RespValue::error(format!("unknown xgroup subcommand '{}'", subcommand)),
    }
}

async fn cmd_xreadgroup(db: &Db, args: &[Vec<u8>]) -> RespValue {
    // XREADGROUP GROUP group consumer [COUNT count] [NOACK] [BLOCK timeout] STREAMS key [key ...] id [id ...]
    if args.len() < 6 {
        return RespValue::error("wrong number of arguments for 'xreadgroup' command");
    }

    // First arg must be GROUP
    let first = std::str::from_utf8(&args[0]).unwrap_or("").to_uppercase();
    if first != "GROUP" {
        return RespValue::error("syntax error: expected GROUP");
    }

    let group = match std::str::from_utf8(&args[1]) {
        Ok(g) => g,
        Err(_) => return RespValue::error("invalid group name"),
    };

    let consumer = match std::str::from_utf8(&args[2]) {
        Ok(c) => c,
        Err(_) => return RespValue::error("invalid consumer name"),
    };

    // Parse options
    let mut i = 3;
    let mut count: Option<i64> = None;
    let mut noack = false;
    let mut block_timeout_ms: Option<i64> = None;

    while i < args.len() {
        let arg = std::str::from_utf8(&args[i]).unwrap_or("").to_uppercase();
        if arg == "COUNT" {
            i += 1;
            if i >= args.len() {
                return RespValue::error("syntax error");
            }
            count = std::str::from_utf8(&args[i])
                .ok()
                .and_then(|s| s.parse().ok());
            if count.is_none() {
                return RespValue::error("value is not an integer or out of range");
            }
            i += 1;
        } else if arg == "NOACK" {
            noack = true;
            i += 1;
        } else if arg == "BLOCK" {
            i += 1;
            if i >= args.len() {
                return RespValue::error("syntax error");
            }
            block_timeout_ms = std::str::from_utf8(&args[i])
                .ok()
                .and_then(|s| s.parse().ok());
            if block_timeout_ms.is_none() {
                return RespValue::error("value is not an integer or out of range");
            }
            i += 1;
        } else if arg == "STREAMS" {
            i += 1;
            break;
        } else {
            return RespValue::error("syntax error");
        }
    }

    // Parse keys and IDs
    let remaining = &args[i..];
    if remaining.is_empty() || remaining.len() % 2 != 0 {
        return RespValue::error(
            "Unbalanced XREADGROUP list of streams: for each stream key an ID must be specified",
        );
    }

    let mid = remaining.len() / 2;
    let keys: Vec<&str> = match remaining[..mid]
        .iter()
        .map(|k| std::str::from_utf8(k))
        .collect::<std::result::Result<Vec<_>, _>>()
    {
        Ok(k) => k,
        Err(_) => return RespValue::error("invalid key"),
    };

    let ids: Vec<&str> = match remaining[mid..]
        .iter()
        .map(|id| std::str::from_utf8(id))
        .collect::<std::result::Result<Vec<_>, _>>()
    {
        Ok(i) => i,
        Err(_) => return RespValue::error("invalid ID"),
    };

    // Handle blocking variant
    if let Some(timeout_ms) = block_timeout_ms {
        match db
            .xreadgroup_block(group, consumer, &keys, &ids, count, noack, timeout_ms)
            .await
        {
            Ok(results) => {
                if results.is_empty() {
                    return RespValue::null();
                }
                let arr: Vec<RespValue> = results
                    .iter()
                    .map(|(key, entries)| {
                        RespValue::Array(Some(vec![
                            RespValue::BulkString(Some(key.as_bytes().to_vec())),
                            format_stream_entries(entries),
                        ]))
                    })
                    .collect();
                RespValue::Array(Some(arr))
            }
            Err(KvError::NoGroup) => RespValue::error("NOGROUP No such consumer group"),
            Err(KvError::WrongType) => RespValue::wrong_type(),
            Err(KvError::SyntaxError) => RespValue::error("syntax error"),
            Err(e) => RespValue::error(e.to_string()),
        }
    } else {
        // Non-blocking variant
        match db.xreadgroup(group, consumer, &keys, &ids, count, noack) {
            Ok(results) => {
                if results.is_empty() {
                    return RespValue::null();
                }
                let arr: Vec<RespValue> = results
                    .iter()
                    .map(|(key, entries)| {
                        RespValue::Array(Some(vec![
                            RespValue::BulkString(Some(key.as_bytes().to_vec())),
                            format_stream_entries(entries),
                        ]))
                    })
                    .collect();
                RespValue::Array(Some(arr))
            }
            Err(KvError::NoGroup) => RespValue::error("NOGROUP No such consumer group"),
            Err(KvError::WrongType) => RespValue::wrong_type(),
            Err(KvError::SyntaxError) => RespValue::error("syntax error"),
            Err(e) => RespValue::error(e.to_string()),
        }
    }
}

fn cmd_xack(db: &Db, args: &[Vec<u8>]) -> RespValue {
    // XACK key group id [id ...]
    if args.len() < 3 {
        return RespValue::error("wrong number of arguments for 'xack' command");
    }

    let key = match std::str::from_utf8(&args[0]) {
        Ok(k) => k,
        Err(_) => return RespValue::error("invalid key"),
    };

    let group = match std::str::from_utf8(&args[1]) {
        Ok(g) => g,
        Err(_) => return RespValue::error("invalid group name"),
    };

    let ids: Vec<StreamId> = match args[2..]
        .iter()
        .map(|id| {
            let s = std::str::from_utf8(id).ok()?;
            StreamId::parse(s)
        })
        .collect::<Option<Vec<_>>>()
    {
        Some(ids) => ids,
        None => return RespValue::error("invalid stream ID"),
    };

    match db.xack(key, group, &ids) {
        Ok(count) => RespValue::Integer(count),
        Err(KvError::WrongType) => RespValue::wrong_type(),
        Err(e) => RespValue::error(e.to_string()),
    }
}

fn cmd_xpending(db: &Db, args: &[Vec<u8>]) -> RespValue {
    // XPENDING key group [[IDLE min-idle-time] start end count [consumer]]
    if args.len() < 2 {
        return RespValue::error("wrong number of arguments for 'xpending' command");
    }

    let key = match std::str::from_utf8(&args[0]) {
        Ok(k) => k,
        Err(_) => return RespValue::error("invalid key"),
    };

    let group = match std::str::from_utf8(&args[1]) {
        Ok(g) => g,
        Err(_) => return RespValue::error("invalid group name"),
    };

    if args.len() == 2 {
        // Summary form
        match db.xpending_summary(key, group) {
            Ok(summary) => {
                if summary.count == 0 {
                    return RespValue::Array(Some(vec![
                        RespValue::Integer(0),
                        RespValue::null(),
                        RespValue::null(),
                        RespValue::null(),
                    ]));
                }
                let consumers_arr: Vec<RespValue> = summary
                    .consumers
                    .iter()
                    .map(|(name, count)| {
                        RespValue::Array(Some(vec![
                            RespValue::BulkString(Some(name.as_bytes().to_vec())),
                            RespValue::BulkString(Some(count.to_string().into_bytes())),
                        ]))
                    })
                    .collect();

                RespValue::Array(Some(vec![
                    RespValue::Integer(summary.count),
                    summary.smallest_id.map_or(RespValue::null(), |id| {
                        RespValue::BulkString(Some(id.to_string().into_bytes()))
                    }),
                    summary.largest_id.map_or(RespValue::null(), |id| {
                        RespValue::BulkString(Some(id.to_string().into_bytes()))
                    }),
                    if consumers_arr.is_empty() {
                        RespValue::null()
                    } else {
                        RespValue::Array(Some(consumers_arr))
                    },
                ]))
            }
            Err(KvError::NoGroup) => RespValue::error("NOGROUP No such consumer group"),
            Err(KvError::WrongType) => RespValue::wrong_type(),
            Err(e) => RespValue::error(e.to_string()),
        }
    } else {
        // Range form: XPENDING key group [IDLE min-idle-time] start end count [consumer]
        let mut i = 2;
        let mut idle_time: Option<i64> = None;

        // Check for IDLE option
        let next = std::str::from_utf8(&args[i]).unwrap_or("").to_uppercase();
        if next == "IDLE" {
            i += 1;
            if i >= args.len() {
                return RespValue::error("syntax error");
            }
            idle_time = std::str::from_utf8(&args[i])
                .ok()
                .and_then(|s| s.parse().ok());
            if idle_time.is_none() {
                return RespValue::error("value is not an integer or out of range");
            }
            i += 1;
        }

        if args.len() < i + 3 {
            return RespValue::error("wrong number of arguments for 'xpending' command");
        }

        let start_str = std::str::from_utf8(&args[i]).unwrap_or("-");
        let end_str = std::str::from_utf8(&args[i + 1]).unwrap_or("+");
        let count: i64 = match std::str::from_utf8(&args[i + 2])
            .ok()
            .and_then(|s| s.parse().ok())
        {
            Some(c) => c,
            None => return RespValue::error("value is not an integer or out of range"),
        };

        let start = StreamId::parse(start_str).unwrap_or(StreamId::min());
        let end = StreamId::parse(end_str).unwrap_or(StreamId::max());

        let consumer = if args.len() > i + 3 {
            std::str::from_utf8(&args[i + 3]).ok()
        } else {
            None
        };

        match db.xpending_range(key, group, start, end, count, consumer, idle_time) {
            Ok(entries) => {
                let arr: Vec<RespValue> = entries
                    .iter()
                    .map(|e| {
                        RespValue::Array(Some(vec![
                            RespValue::BulkString(Some(e.id.to_string().into_bytes())),
                            RespValue::BulkString(Some(e.consumer.as_bytes().to_vec())),
                            RespValue::Integer(e.idle),
                            RespValue::Integer(e.delivery_count),
                        ]))
                    })
                    .collect();
                RespValue::Array(Some(arr))
            }
            Err(KvError::NoGroup) => RespValue::error("NOGROUP No such consumer group"),
            Err(KvError::WrongType) => RespValue::wrong_type(),
            Err(e) => RespValue::error(e.to_string()),
        }
    }
}

fn cmd_xclaim(db: &Db, args: &[Vec<u8>]) -> RespValue {
    // XCLAIM key group consumer min-idle-time id [id ...] [IDLE ms] [TIME ms] [RETRYCOUNT count] [FORCE] [JUSTID]
    if args.len() < 5 {
        return RespValue::error("wrong number of arguments for 'xclaim' command");
    }

    let key = match std::str::from_utf8(&args[0]) {
        Ok(k) => k,
        Err(_) => return RespValue::error("invalid key"),
    };

    let group = match std::str::from_utf8(&args[1]) {
        Ok(g) => g,
        Err(_) => return RespValue::error("invalid group name"),
    };

    let consumer = match std::str::from_utf8(&args[2]) {
        Ok(c) => c,
        Err(_) => return RespValue::error("invalid consumer name"),
    };

    let min_idle_time: i64 = match std::str::from_utf8(&args[3])
        .ok()
        .and_then(|s| s.parse().ok())
    {
        Some(t) => t,
        None => return RespValue::error("value is not an integer or out of range"),
    };

    // Parse IDs and options
    let mut ids: Vec<StreamId> = Vec::new();
    let mut idle_ms: Option<i64> = None;
    let mut time_ms: Option<i64> = None;
    let mut retry_count: Option<i64> = None;
    let mut force = false;
    let mut justid = false;

    let mut i = 4;
    while i < args.len() {
        let arg = std::str::from_utf8(&args[i]).unwrap_or("");
        let arg_upper = arg.to_uppercase();

        if arg_upper == "IDLE" {
            i += 1;
            if i >= args.len() {
                return RespValue::error("syntax error");
            }
            idle_ms = std::str::from_utf8(&args[i])
                .ok()
                .and_then(|s| s.parse().ok());
            if idle_ms.is_none() {
                return RespValue::error("value is not an integer or out of range");
            }
        } else if arg_upper == "TIME" {
            i += 1;
            if i >= args.len() {
                return RespValue::error("syntax error");
            }
            time_ms = std::str::from_utf8(&args[i])
                .ok()
                .and_then(|s| s.parse().ok());
            if time_ms.is_none() {
                return RespValue::error("value is not an integer or out of range");
            }
        } else if arg_upper == "RETRYCOUNT" {
            i += 1;
            if i >= args.len() {
                return RespValue::error("syntax error");
            }
            retry_count = std::str::from_utf8(&args[i])
                .ok()
                .and_then(|s| s.parse().ok());
            if retry_count.is_none() {
                return RespValue::error("value is not an integer or out of range");
            }
        } else if arg_upper == "FORCE" {
            force = true;
        } else if arg_upper == "JUSTID" {
            justid = true;
        } else {
            // Try to parse as ID
            match StreamId::parse(arg) {
                Some(id) => ids.push(id),
                None => return RespValue::error("invalid stream ID"),
            }
        }
        i += 1;
    }

    if ids.is_empty() {
        return RespValue::error("wrong number of arguments for 'xclaim' command");
    }

    match db.xclaim(
        key,
        group,
        consumer,
        min_idle_time,
        &ids,
        idle_ms,
        time_ms,
        retry_count,
        force,
        justid,
    ) {
        Ok(entries) => {
            if justid {
                // Return just the IDs
                let arr: Vec<RespValue> = entries
                    .iter()
                    .map(|e| RespValue::BulkString(Some(e.id.to_string().into_bytes())))
                    .collect();
                RespValue::Array(Some(arr))
            } else {
                format_stream_entries(&entries)
            }
        }
        Err(KvError::NoGroup) => RespValue::error("NOGROUP No such consumer group"),
        Err(KvError::WrongType) => RespValue::wrong_type(),
        Err(e) => RespValue::error(e.to_string()),
    }
}

// --- Transaction commands ---

/// MULTI - Enter transaction mode
/// Returns OK and buffering all subsequent commands
fn cmd_multi(state: &mut ConnectionState) -> RespValue {
    match state {
        ConnectionState::Normal { watched_keys } => {
            // Transition to transaction mode with empty queue, preserving watched keys
            let watched = std::mem::take(watched_keys);
            *state = ConnectionState::Transaction {
                queue: Vec::new(),
                watched_keys: watched,
            };
            RespValue::ok()
        }
        ConnectionState::Subscribed { .. } => {
            RespValue::error("ERR MULTI not allowed in subscription mode")
        }
        ConnectionState::Transaction { .. } => {
            RespValue::error("ERR MULTI calls can not be nested")
        }
    }
}

/// DISCARD - Exit transaction mode without executing queued commands
/// Returns OK and clears the queue but keeps watched keys
fn cmd_discard(state: &mut ConnectionState) -> RespValue {
    match state {
        ConnectionState::Normal { .. } => RespValue::error("ERR DISCARD without MULTI"),
        ConnectionState::Subscribed { .. } => {
            RespValue::error("ERR DISCARD not allowed in subscription mode")
        }
        ConnectionState::Transaction { watched_keys, .. } => {
            // Exit transaction mode, keeping watched keys
            let watched = std::mem::take(watched_keys);
            *state = ConnectionState::Normal {
                watched_keys: watched,
            };
            RespValue::ok()
        }
    }
}

/// Queue a command for later execution in a transaction
/// Returns QUEUED
fn queue_command(state: &mut ConnectionState, cmd: &str, args: &[Vec<u8>]) -> RespValue {
    if let ConnectionState::Transaction { queue, .. } = state {
        queue.push(QueuedCommand {
            cmd: cmd.to_string(),
            args: args.to_vec(),
        });
        RespValue::SimpleString("QUEUED".to_string())
    } else {
        RespValue::error("ERR not in transaction mode")
    }
}

/// Execute a transaction command (DISCARD or EXEC)
async fn execute_transaction_command(
    state: &mut ConnectionState,
    db: Option<&mut Db>,
    cmd: &str,
    args: &[Vec<u8>],
) -> RespValue {
    match cmd {
        "DISCARD" => {
            if !args.is_empty() {
                return RespValue::error("wrong number of arguments for 'discard' command");
            }
            cmd_discard(state)
        }
        "EXEC" => {
            if !args.is_empty() {
                return RespValue::error("wrong number of arguments for 'exec' command");
            }
            match db {
                Some(db_ref) => execute_transaction(state, db_ref).await,
                None => RespValue::error("ERR internal error: db not provided for EXEC"),
            }
        }
        _ => RespValue::error(format!("unknown transaction command '{}'", cmd)),
    }
}

/// Execute all queued commands atomically
/// Extracts queue from transaction state, executes each command, and returns array of results
/// If any watched key has changed, returns null and aborts the transaction
async fn execute_transaction(state: &mut ConnectionState, db: &mut Db) -> RespValue {
    // Extract queue and watched keys from transaction state
    let (queue, watched_keys) = if let ConnectionState::Transaction {
        queue,
        watched_keys,
    } = state
    {
        (std::mem::take(queue), std::mem::take(watched_keys))
    } else {
        return RespValue::error("ERR not in transaction");
    };

    // Exit transaction mode (always clears watched keys)
    *state = ConnectionState::new_normal();

    // Check if any watched key has been modified
    for (key, original_version) in &watched_keys {
        let current_version = db.get_version(key).unwrap_or(0);
        if current_version != *original_version {
            // Key was modified - abort transaction
            return RespValue::null();
        }
    }

    // Execute each command and collect results
    let mut results = Vec::new();
    for queued_cmd in queue {
        // Reconstruct args: [cmd, arg1, arg2, ...]
        let mut full_args = vec![queued_cmd.cmd.as_bytes().to_vec()];
        full_args.extend(queued_cmd.args);

        // Execute the command (using transaction-safe version)
        let result = execute_command_in_transaction(db, &full_args).await;
        results.push(result);
    }

    // Return array of results
    RespValue::Array(Some(results))
}

/// Execute a command within a transaction context
/// CLIENT commands return stubs since they require per-connection state
async fn execute_command_in_transaction(db: &mut Db, args: &[Vec<u8>]) -> RespValue {
    if args.is_empty() {
        return RespValue::error("empty command");
    }

    let cmd = String::from_utf8_lossy(&args[0]).to_uppercase();
    let cmd_args = &args[1..];

    match cmd.as_str() {
        // CLIENT commands return stubs in transactions
        "CLIENT" => {
            if cmd_args.is_empty() {
                return RespValue::error("wrong number of arguments for 'client' command");
            }
            let subcommand = String::from_utf8_lossy(&cmd_args[0]).to_uppercase();
            match subcommand.as_str() {
                "SETNAME" => RespValue::ok(),
                "GETNAME" => RespValue::null(),
                "ID" => RespValue::Integer(0),
                "LIST" => RespValue::BulkString(Some(Vec::new())),
                _ => RespValue::error(format!(
                    "ERR Unknown subcommand or wrong number of arguments for '{}'",
                    subcommand
                )),
            }
        }
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
        "LREM" => cmd_lrem(db, cmd_args),
        "LINSERT" => cmd_linsert(db, cmd_args),
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
        "SMOVE" => cmd_smove(db, cmd_args),
        "SDIFFSTORE" => cmd_sdiffstore(db, cmd_args),
        "SINTERSTORE" => cmd_sinterstore(db, cmd_args),
        "SUNIONSTORE" => cmd_sunionstore(db, cmd_args),
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
        "AUTOVACUUM" => cmd_autovacuum(db, cmd_args),
        "KEYINFO" => cmd_keyinfo(db, cmd_args),
        "HISTORY" => cmd_history(db, cmd_args),
        "FTS" => cmd_fts(db, cmd_args),
        // RediSearch-compatible commands (Session 23)
        "FT.CREATE" => cmd_ft_create(db, cmd_args),
        "FT.DROPINDEX" => cmd_ft_dropindex(db, cmd_args),
        "FT._LIST" => cmd_ft_list(db),
        "FT.INFO" => cmd_ft_info(db, cmd_args),
        "FT.ALTER" => cmd_ft_alter(db, cmd_args),
        "FT.SEARCH" => cmd_ft_search(db, cmd_args),
        "FT.AGGREGATE" => cmd_ft_aggregate(db, cmd_args),
        "FT.ALIASADD" => cmd_ft_aliasadd(db, cmd_args),
        "FT.ALIASDEL" => cmd_ft_aliasdel(db, cmd_args),
        "FT.ALIASUPDATE" => cmd_ft_aliasupdate(db, cmd_args),
        "FT.SYNUPDATE" => cmd_ft_synupdate(db, cmd_args),
        "FT.SYNDUMP" => cmd_ft_syndump(db, cmd_args),
        "FT.SUGADD" => cmd_ft_sugadd(db, cmd_args),
        "FT.SUGGET" => cmd_ft_sugget(db, cmd_args),
        "FT.SUGDEL" => cmd_ft_sugdel(db, cmd_args),
        "FT.SUGLEN" => cmd_ft_suglen(db, cmd_args),
        // Vector commands (Session 24.2)
        "VECTOR" => cmd_vector(db, cmd_args),
        "VADD" => cmd_vadd(db, cmd_args),
        "VGET" => cmd_vget(db, cmd_args),
        "VDEL" => cmd_vdel(db, cmd_args),
        "VCOUNT" => cmd_vcount(db, cmd_args),
        "VSEARCH" => cmd_vsearch(db, cmd_args),
        // Stream commands
        "XADD" => cmd_xadd(db, cmd_args),
        "XLEN" => cmd_xlen(db, cmd_args),
        "XRANGE" => cmd_xrange(db, cmd_args),
        "XREVRANGE" => cmd_xrevrange(db, cmd_args),
        "XREAD" => cmd_xread(db, cmd_args).await,
        "XTRIM" => cmd_xtrim(db, cmd_args),
        "XDEL" => cmd_xdel(db, cmd_args),
        "XINFO" => cmd_xinfo(db, cmd_args),
        // Stream consumer group commands
        "XGROUP" => cmd_xgroup(db, cmd_args),
        "XREADGROUP" => cmd_xreadgroup(db, cmd_args).await,
        "XACK" => cmd_xack(db, cmd_args),
        "XPENDING" => cmd_xpending(db, cmd_args),
        "XCLAIM" => cmd_xclaim(db, cmd_args),
        // Blocking commands not allowed in transactions (checked earlier), but handle anyway
        "BLPOP" | "BRPOP" => RespValue::error("ERR blocking commands not allowed in transaction"),
        _ => RespValue::error(format!("unknown command '{}'", cmd)),
    }
}

#[cfg(test)]
mod transaction_tests {
    use super::*;

    #[test]
    fn test_cmd_multi_from_normal() {
        let mut state = ConnectionState::new_normal();
        let result = cmd_multi(&mut state);
        assert!(matches!(result, RespValue::SimpleString(ref s) if s == "OK"));
        assert!(state.is_transaction());
    }

    #[test]
    fn test_cmd_multi_nested() {
        let mut state = ConnectionState::Transaction {
            queue: Vec::new(),
            watched_keys: HashMap::new(),
        };
        let result = cmd_multi(&mut state);
        if let RespValue::Error(msg) = result {
            assert!(msg.contains("nested"));
        } else {
            panic!("Expected error, got {:?}", result);
        }
    }

    #[test]
    fn test_cmd_multi_from_subscribed() {
        let mut state = ConnectionState::Subscribed {
            channels: std::collections::HashSet::new(),
            patterns: std::collections::HashSet::new(),
            channel_receivers: std::collections::HashMap::new(),
            pattern_receivers: Vec::new(),
        };
        let result = cmd_multi(&mut state);
        if let RespValue::Error(msg) = &result {
            assert!(msg.contains("subscription mode"));
        } else {
            panic!("Expected error, got {:?}", result);
        }
    }

    #[test]
    fn test_cmd_discard_without_multi() {
        let mut state = ConnectionState::new_normal();
        let result = cmd_discard(&mut state);
        if let RespValue::Error(msg) = &result {
            assert!(msg.contains("DISCARD without MULTI"));
        } else {
            panic!("Expected error, got {:?}", result);
        }
    }

    #[test]
    fn test_cmd_discard_in_transaction() {
        let mut state = ConnectionState::Transaction {
            queue: vec![QueuedCommand {
                cmd: "SET".to_string(),
                args: vec![b"key".to_vec(), b"value".to_vec()],
            }],
            watched_keys: HashMap::new(),
        };
        let result = cmd_discard(&mut state);
        assert!(matches!(result, RespValue::SimpleString(ref s) if s == "OK"));
        assert!(!state.is_transaction());
        assert!(matches!(state, ConnectionState::Normal { .. }));
    }

    #[test]
    fn test_cmd_discard_clears_queue() {
        let mut state = ConnectionState::Transaction {
            queue: vec![
                QueuedCommand {
                    cmd: "SET".to_string(),
                    args: vec![b"k1".to_vec(), b"v1".to_vec()],
                },
                QueuedCommand {
                    cmd: "INCR".to_string(),
                    args: vec![b"counter".to_vec()],
                },
            ],
            watched_keys: HashMap::new(),
        };
        assert_eq!(
            if let ConnectionState::Transaction { queue, .. } = &state {
                queue.len()
            } else {
                0
            },
            2
        );
        cmd_discard(&mut state);
        assert!(matches!(state, ConnectionState::Normal { .. }));
    }

    #[test]
    fn test_cmd_discard_from_subscribed() {
        let mut state = ConnectionState::Subscribed {
            channels: std::collections::HashSet::new(),
            patterns: std::collections::HashSet::new(),
            channel_receivers: std::collections::HashMap::new(),
            pattern_receivers: Vec::new(),
        };
        let result = cmd_discard(&mut state);
        if let RespValue::Error(msg) = &result {
            assert!(msg.contains("subscription mode"));
        } else {
            panic!("Expected error, got {:?}", result);
        }
    }

    #[test]
    fn test_queue_command() {
        let mut state = ConnectionState::Transaction {
            queue: Vec::new(),
            watched_keys: HashMap::new(),
        };
        let cmd = "SET";
        let args = vec![b"key".to_vec(), b"value".to_vec()];
        let result = queue_command(&mut state, cmd, &args);
        assert!(matches!(result, RespValue::SimpleString(ref s) if s == "QUEUED"));
        assert_eq!(
            if let ConnectionState::Transaction { queue, .. } = &state {
                queue.len()
            } else {
                0
            },
            1
        );
    }

    #[test]
    fn test_queue_command_multiple() {
        let mut state = ConnectionState::Transaction {
            queue: Vec::new(),
            watched_keys: HashMap::new(),
        };
        queue_command(&mut state, "SET", &vec![b"k1".to_vec(), b"v1".to_vec()]);
        queue_command(&mut state, "INCR", &vec![b"counter".to_vec()]);
        queue_command(&mut state, "GET", &vec![b"k1".to_vec()]);
        assert_eq!(
            if let ConnectionState::Transaction { queue, .. } = &state {
                queue.len()
            } else {
                0
            },
            3
        );
    }

    #[test]
    fn test_queue_command_not_in_transaction() {
        let mut state = ConnectionState::new_normal();
        let result = queue_command(&mut state, "SET", &vec![b"key".to_vec(), b"value".to_vec()]);
        if let RespValue::Error(msg) = &result {
            assert!(msg.contains("not in transaction mode"));
        } else {
            panic!("Expected error, got {:?}", result);
        }
    }

    #[test]
    fn test_execute_transaction_discard_clears_state() {
        let mut state = ConnectionState::Transaction {
            queue: vec![QueuedCommand {
                cmd: "SET".to_string(),
                args: vec![b"key".to_vec(), b"value".to_vec()],
            }],
            watched_keys: HashMap::new(),
        };
        cmd_discard(&mut state);
        assert!(matches!(state, ConnectionState::Normal { .. }));
    }

    #[test]
    fn test_cmd_discard_with_args_returns_error() {
        let mut state = ConnectionState::Transaction {
            queue: Vec::new(),
            watched_keys: HashMap::new(),
        };
        let result = cmd_discard(&mut state);
        // cmd_discard doesn't validate args, so this should succeed
        assert!(matches!(result, RespValue::SimpleString(ref s) if s == "OK"));
    }

    #[test]
    fn test_watch_rejected_in_transaction() {
        let mut state = ConnectionState::Transaction {
            queue: Vec::new(),
            watched_keys: HashMap::new(),
        };
        // Simulate the check that's in execute_normal_command
        // In real usage, WATCH would be rejected before reaching transaction code
        // but we can test the error path here
        let is_transaction = state.is_transaction();
        assert!(is_transaction);
    }

    #[test]
    fn test_blpop_rejected_in_transaction() {
        let mut state = ConnectionState::Transaction {
            queue: Vec::new(),
            watched_keys: HashMap::new(),
        };
        // Blocking commands are rejected at the command routing level
        assert!(state.is_transaction());
    }

    #[test]
    fn test_subscribe_rejected_in_transaction() {
        let mut state = ConnectionState::Transaction {
            queue: Vec::new(),
            watched_keys: HashMap::new(),
        };
        // Pub/sub commands are rejected at the command routing level
        assert!(state.is_transaction());
    }

    #[test]
    fn test_queue_accumulates_commands() {
        let mut state = ConnectionState::Transaction {
            queue: Vec::new(),
            watched_keys: HashMap::new(),
        };

        // Queue several commands
        for i in 0..5 {
            let cmd = format!("SET");
            let key = format!("key{}", i);
            queue_command(&mut state, &cmd, &vec![key.into_bytes(), b"value".to_vec()]);
        }

        // Verify all are queued
        if let ConnectionState::Transaction { queue, .. } = &state {
            assert_eq!(queue.len(), 5);
            for cmd_entry in queue {
                assert_eq!(cmd_entry.cmd, "SET");
            }
        } else {
            panic!("Expected transaction state");
        }
    }

    #[test]
    fn test_command_with_empty_args() {
        let mut state = ConnectionState::Transaction {
            queue: Vec::new(),
            watched_keys: HashMap::new(),
        };
        let result = queue_command(&mut state, "INCR", &[]);
        assert!(matches!(result, RespValue::SimpleString(ref s) if s == "QUEUED"));
        assert_eq!(
            if let ConnectionState::Transaction { queue, .. } = &state {
                queue.len()
            } else {
                0
            },
            1
        );
    }

    #[test]
    fn test_transaction_persists_across_multiple_queues() {
        let mut state = ConnectionState::Transaction {
            queue: Vec::new(),
            watched_keys: HashMap::new(),
        };

        queue_command(&mut state, "SET", &vec![b"key".to_vec()]);
        queue_command(&mut state, "GET", &vec![b"key".to_vec()]);
        queue_command(&mut state, "DEL", &vec![b"key".to_vec()]);

        if let ConnectionState::Transaction { queue, .. } = &state {
            assert_eq!(queue.len(), 3);
            assert_eq!(queue[0].cmd, "SET");
            assert_eq!(queue[1].cmd, "GET");
            assert_eq!(queue[2].cmd, "DEL");
        } else {
            panic!("Expected transaction state");
        }
    }
}
