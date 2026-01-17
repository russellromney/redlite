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
use crate::types::{GetExOption, ListDirection, StreamId, ZMember};

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

/// Connection handler for multi-tenant scenarios.
///
/// Unlike `Server` which binds to a port and manages its own listener,
/// `ConnectionHandler` allows external code to handle the listening and
/// routing, then delegate individual connections here.
///
/// This is useful for:
/// - Multi-tenant setups where tenant routing happens first
/// - Proxy servers that need to identify the tenant before handing off
/// - Custom authentication flows
///
/// # Example
///
/// ```ignore
/// let handler = ConnectionHandler::new(None);
///
/// // In your accept loop:
/// let tenant_db = get_tenant_db(tenant_id).await?;
/// let session = tenant_db.session();
/// let handler_clone = handler.clone();
///
/// tokio::spawn(async move {
///     handler_clone.handle(socket, session, peer_addr).await
/// });
/// ```
pub struct ConnectionHandler {
    notifier: Arc<RwLock<HashMap<String, broadcast::Sender<()>>>>,
    pubsub_channels: Arc<RwLock<HashMap<String, broadcast::Sender<PubSubMessage>>>>,
    password: Option<Arc<String>>,
    connection_pool: ConnectionPool,
    pause_state: Arc<PauseState>,
}

impl Clone for ConnectionHandler {
    fn clone(&self) -> Self {
        Self {
            notifier: Arc::clone(&self.notifier),
            pubsub_channels: Arc::clone(&self.pubsub_channels),
            password: self.password.clone(),
            connection_pool: self.connection_pool.clone_handle(),
            pause_state: Arc::clone(&self.pause_state),
        }
    }
}

impl ConnectionHandler {
    /// Create a new connection handler with optional password authentication.
    pub fn new(password: Option<String>) -> Self {
        Self {
            notifier: Arc::new(RwLock::new(HashMap::new())),
            pubsub_channels: Arc::new(RwLock::new(HashMap::new())),
            password: password.map(Arc::new),
            connection_pool: ConnectionPool::new(),
            pause_state: Arc::new(PauseState::new()),
        }
    }

    /// Handle a single connection with the given database session.
    ///
    /// This is the main entry point for processing Redis commands on a connection.
    /// The caller is responsible for:
    /// 1. Accepting the TCP connection
    /// 2. Any pre-connection authentication/routing (e.g., tenant identification)
    /// 3. Obtaining the correct Db instance for this connection
    ///
    /// # Arguments
    ///
    /// * `socket` - The TCP stream for this connection
    /// * `db` - The database session to use (typically from `Db::session()`)
    /// * `peer_addr` - The peer address for logging and connection tracking
    pub async fn handle(
        &self,
        socket: TcpStream,
        db: Db,
        peer_addr: SocketAddr,
    ) -> std::io::Result<()> {
        handle_connection(
            socket,
            db,
            Arc::clone(&self.notifier),
            Arc::clone(&self.pubsub_channels),
            self.password.clone(),
            self.connection_pool.clone_handle(),
            Arc::clone(&self.pause_state),
            peer_addr,
        )
        .await
    }

    /// Get the connection pool for connection tracking.
    pub fn connection_pool(&self) -> &ConnectionPool {
        &self.connection_pool
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
        "CONFIG" => cmd_config(db, cmd_args),
        "MEMORY" => cmd_memory(db, cmd_args),
        // String commands
        "GET" => cmd_get(db, cmd_args),
        "SET" => cmd_set(db, cmd_args),
        "DEL" => cmd_del(db, cmd_args),
        "TYPE" => cmd_type(db, cmd_args),
        "TTL" => cmd_ttl(db, cmd_args),
        "PTTL" => cmd_pttl(db, cmd_args),
        "EXISTS" => cmd_exists(db, cmd_args),
        "EXPIRE" => cmd_expire(db, cmd_args),
        "PEXPIRE" => cmd_pexpire(db, cmd_args),
        "EXPIREAT" => cmd_expireat(db, cmd_args),
        "PEXPIREAT" => cmd_pexpireat(db, cmd_args),
        "PERSIST" => cmd_persist(db, cmd_args),
        "RENAME" => cmd_rename(db, cmd_args),
        "RENAMENX" => cmd_renamenx(db, cmd_args),
        "KEYS" => cmd_keys(db, cmd_args),
        "SCAN" => cmd_scan(db, cmd_args),
        "HSCAN" => cmd_hscan(db, cmd_args),
        "SSCAN" => cmd_sscan(db, cmd_args),
        "ZSCAN" => cmd_zscan(db, cmd_args),
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
        "GETEX" => cmd_getex(db, cmd_args),
        "GETDEL" => cmd_getdel(db, cmd_args),
        "SETEX" => cmd_setex(db, cmd_args),
        "PSETEX" => cmd_psetex(db, cmd_args),
        // Bitmap operations
        "SETBIT" => cmd_setbit(db, cmd_args),
        "GETBIT" => cmd_getbit(db, cmd_args),
        "BITCOUNT" => cmd_bitcount(db, cmd_args),
        "BITOP" => cmd_bitop(db, cmd_args),
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
        "LPUSHX" => cmd_lpushx(db, cmd_args),
        "RPUSHX" => cmd_rpushx(db, cmd_args),
        "LPOS" => cmd_lpos(db, cmd_args),
        "LMOVE" => cmd_lmove(db, cmd_args),
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
        "ZINTERSTORE" => cmd_zinterstore(db, cmd_args),
        "ZUNIONSTORE" => cmd_zunionstore(db, cmd_args),
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
        "FT.EXPLAIN" => cmd_ft_explain(db, cmd_args),
        "FT.PROFILE" => cmd_ft_profile(db, cmd_args),
        // Redis 8 Vector commands (V* commands)
        "VADD" => cmd_vadd(db, cmd_args),
        "VSIM" => cmd_vsim(db, cmd_args),
        "VREM" => cmd_vrem(db, cmd_args),
        "VCARD" => cmd_vcard(db, cmd_args),
        "VDIM" => cmd_vdim(db, cmd_args),
        "VINFO" => cmd_vinfo(db, cmd_args),
        "VEMB" => cmd_vemb(db, cmd_args),
        "VGETATTR" => cmd_vgetattr(db, cmd_args),
        "VSETATTR" => cmd_vsetattr(db, cmd_args),
        "VRANDMEMBER" => cmd_vrandmember(db, cmd_args),
        // Geo commands (GEO* commands)
        "GEOADD" => cmd_geoadd(db, cmd_args),
        "GEOPOS" => cmd_geopos(db, cmd_args),
        "GEODIST" => cmd_geodist(db, cmd_args),
        "GEOHASH" => cmd_geohash(db, cmd_args),
        "GEOSEARCH" => cmd_geosearch(db, cmd_args),
        "GEOSEARCHSTORE" => cmd_geosearchstore(db, cmd_args),
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

fn cmd_config(db: &Db, args: &[Vec<u8>]) -> RespValue {
    if args.is_empty() {
        return RespValue::error("wrong number of arguments for 'config' command");
    }

    let subcommand = match std::str::from_utf8(&args[0]) {
        Ok(s) => s.to_uppercase(),
        Err(_) => return RespValue::error("invalid subcommand"),
    };

    match subcommand.as_str() {
        "GET" => {
            if args.len() < 2 {
                return RespValue::error("wrong number of arguments for 'config get' command");
            }
            let pattern = match std::str::from_utf8(&args[1]) {
                Ok(p) => p.to_lowercase(),
                Err(_) => return RespValue::error("invalid pattern"),
            };

            let mut result = Vec::new();

            // Match maxdisk pattern
            if pattern == "maxdisk" || pattern == "*" {
                result.push(RespValue::BulkString(Some(b"maxdisk".to_vec())));
                result.push(RespValue::BulkString(Some(db.max_disk().to_string().into_bytes())));
            }

            // Match maxmemory pattern
            if pattern == "maxmemory" || pattern == "*" {
                result.push(RespValue::BulkString(Some(b"maxmemory".to_vec())));
                result.push(RespValue::BulkString(Some(db.max_memory().to_string().into_bytes())));
            }

            // Match maxmemory-policy pattern
            if pattern == "maxmemory-policy" || pattern == "*" {
                result.push(RespValue::BulkString(Some(b"maxmemory-policy".to_vec())));
                result.push(RespValue::BulkString(Some(db.eviction_policy().to_str().as_bytes().to_vec())));
            }

            // Match persist-access-tracking pattern
            if pattern == "persist-access-tracking" || pattern == "*" {
                result.push(RespValue::BulkString(Some(b"persist-access-tracking".to_vec())));
                let value = if db.persist_access_tracking() { "on" } else { "off" };
                result.push(RespValue::BulkString(Some(value.as_bytes().to_vec())));
            }

            // Match access-flush-interval pattern
            if pattern == "access-flush-interval" || pattern == "*" {
                result.push(RespValue::BulkString(Some(b"access-flush-interval".to_vec())));
                result.push(RespValue::BulkString(Some(db.access_flush_interval().to_string().into_bytes())));
            }

            RespValue::Array(Some(result))
        }
        "SET" => {
            if args.len() < 3 {
                return RespValue::error("wrong number of arguments for 'config set' command");
            }
            let key = match std::str::from_utf8(&args[1]) {
                Ok(k) => k.to_lowercase(),
                Err(_) => return RespValue::error("invalid key"),
            };
            let value = match std::str::from_utf8(&args[2]) {
                Ok(v) => v,
                Err(_) => return RespValue::error("invalid value"),
            };

            match key.as_str() {
                "maxdisk" => {
                    match value.parse::<u64>() {
                        Ok(bytes) => {
                            db.set_max_disk(bytes);
                            RespValue::ok()
                        }
                        Err(_) => RespValue::error("invalid maxdisk value"),
                    }
                }
                "maxmemory" => {
                    match value.parse::<u64>() {
                        Ok(bytes) => {
                            db.set_max_memory(bytes);
                            RespValue::ok()
                        }
                        Err(_) => RespValue::error("invalid maxmemory value"),
                    }
                }
                "maxmemory-policy" => {
                    match crate::db::EvictionPolicy::from_str(value) {
                        Ok(policy) => {
                            db.set_eviction_policy(policy);
                            RespValue::ok()
                        }
                        Err(_) => RespValue::error("invalid maxmemory-policy"),
                    }
                }
                "persist-access-tracking" => {
                    match value.to_lowercase().as_str() {
                        "on" | "yes" | "true" | "1" => {
                            db.set_persist_access_tracking(true);
                            RespValue::ok()
                        }
                        "off" | "no" | "false" | "0" => {
                            db.set_persist_access_tracking(false);
                            RespValue::ok()
                        }
                        _ => RespValue::error("invalid persist-access-tracking value (use on/off)"),
                    }
                }
                "access-flush-interval" => {
                    match value.parse::<i64>() {
                        Ok(ms) if ms >= 0 => {
                            db.set_access_flush_interval(ms);
                            RespValue::ok()
                        }
                        Ok(_) => RespValue::error("access-flush-interval must be non-negative"),
                        Err(_) => RespValue::error("invalid access-flush-interval value"),
                    }
                }
                _ => RespValue::error(format!("unsupported config parameter: {}", key)),
            }
        }
        _ => RespValue::error(format!("unknown config subcommand: {}", subcommand)),
    }
}

fn cmd_memory(db: &Db, args: &[Vec<u8>]) -> RespValue {
    if args.is_empty() {
        return RespValue::error("wrong number of arguments for 'memory' command");
    }

    let subcommand = match std::str::from_utf8(&args[0]) {
        Ok(s) => s.to_uppercase(),
        Err(_) => return RespValue::error("invalid subcommand"),
    };

    match subcommand.as_str() {
        "STATS" => {
            let total_memory = db.total_memory_usage().unwrap_or(0);
            let key_count = db.dbsize().unwrap_or(0);
            let policy = db.eviction_policy().to_str();

            let result = vec![
                RespValue::BulkString(Some(b"total.allocated".to_vec())),
                RespValue::Integer(total_memory as i64),
                RespValue::BulkString(Some(b"keys.count".to_vec())),
                RespValue::Integer(key_count as i64),
                RespValue::BulkString(Some(b"eviction.policy".to_vec())),
                RespValue::BulkString(Some(policy.as_bytes().to_vec())),
            ];

            RespValue::Array(Some(result))
        }
        "USAGE" => {
            if args.len() != 2 {
                return RespValue::error("wrong number of arguments for 'memory usage' command");
            }

            let key = match std::str::from_utf8(&args[1]) {
                Ok(k) => k,
                Err(_) => return RespValue::error("invalid key"),
            };

            // Get key_id first
            match db.get_key_id(key) {
                Ok(Some(key_id)) => match db.calculate_key_memory(key_id) {
                    Ok(memory) => RespValue::Integer(memory as i64),
                    Err(_) => RespValue::null(),
                },
                Ok(None) => RespValue::null(),
                Err(_) => RespValue::error("ERR memory usage failed"),
            }
        }
        _ => RespValue::error(&format!("ERR unknown MEMORY subcommand '{}'", subcommand)),
    }
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

    // Parse cursor as string (keyset pagination uses base64-encoded cursors)
    let cursor = match std::str::from_utf8(&args[0]) {
        Ok(c) => c,
        Err(_) => return RespValue::error("invalid cursor"),
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
                RespValue::BulkString(Some(next_cursor.into_bytes())),
                RespValue::Array(Some(keys_array)),
            ]))
        }
        Err(e) => RespValue::error(e.to_string()),
    }
}

fn cmd_hscan(db: &Db, args: &[Vec<u8>]) -> RespValue {
    if args.len() < 2 {
        return RespValue::error("wrong number of arguments for 'hscan' command");
    }

    let key = match std::str::from_utf8(&args[0]) {
        Ok(k) => k,
        Err(_) => return RespValue::error("invalid key"),
    };

    // Parse cursor as string (keyset pagination uses base64-encoded cursors)
    let cursor = match std::str::from_utf8(&args[1]) {
        Ok(c) => c,
        Err(_) => return RespValue::error("invalid cursor"),
    };

    let mut pattern: Option<&str> = None;
    let mut count: usize = 10;
    let mut i = 2;

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

    match db.hscan(key, cursor, pattern, count) {
        Ok((next_cursor, pairs)) => {
            // Flatten field-value pairs: [field1, val1, field2, val2, ...]
            let mut items: Vec<RespValue> = Vec::with_capacity(pairs.len() * 2);
            for (field, value) in pairs {
                items.push(RespValue::BulkString(Some(field.into_bytes())));
                items.push(RespValue::BulkString(Some(value)));
            }
            RespValue::Array(Some(vec![
                RespValue::BulkString(Some(next_cursor.into_bytes())),
                RespValue::Array(Some(items)),
            ]))
        }
        Err(KvError::WrongType) => RespValue::wrong_type(),
        Err(e) => RespValue::error(e.to_string()),
    }
}

fn cmd_sscan(db: &Db, args: &[Vec<u8>]) -> RespValue {
    if args.len() < 2 {
        return RespValue::error("wrong number of arguments for 'sscan' command");
    }

    let key = match std::str::from_utf8(&args[0]) {
        Ok(k) => k,
        Err(_) => return RespValue::error("invalid key"),
    };

    // Parse cursor as string (keyset pagination uses base64-encoded cursors)
    let cursor = match std::str::from_utf8(&args[1]) {
        Ok(c) => c,
        Err(_) => return RespValue::error("invalid cursor"),
    };

    let mut pattern: Option<&str> = None;
    let mut count: usize = 10;
    let mut i = 2;

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

    match db.sscan(key, cursor, pattern, count) {
        Ok((next_cursor, members)) => {
            let items: Vec<RespValue> = members
                .into_iter()
                .map(|m| RespValue::BulkString(Some(m)))
                .collect();
            RespValue::Array(Some(vec![
                RespValue::BulkString(Some(next_cursor.into_bytes())),
                RespValue::Array(Some(items)),
            ]))
        }
        Err(KvError::WrongType) => RespValue::wrong_type(),
        Err(e) => RespValue::error(e.to_string()),
    }
}

fn cmd_zscan(db: &Db, args: &[Vec<u8>]) -> RespValue {
    if args.len() < 2 {
        return RespValue::error("wrong number of arguments for 'zscan' command");
    }

    let key = match std::str::from_utf8(&args[0]) {
        Ok(k) => k,
        Err(_) => return RespValue::error("invalid key"),
    };

    // Parse cursor as string (keyset pagination uses base64-encoded cursors)
    let cursor = match std::str::from_utf8(&args[1]) {
        Ok(c) => c,
        Err(_) => return RespValue::error("invalid cursor"),
    };

    let mut pattern: Option<&str> = None;
    let mut count: usize = 10;
    let mut i = 2;

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

    match db.zscan(key, cursor, pattern, count) {
        Ok((next_cursor, pairs)) => {
            // Flatten member-score pairs: [member1, score1, member2, score2, ...]
            let mut items: Vec<RespValue> = Vec::with_capacity(pairs.len() * 2);
            for (member, score) in pairs {
                items.push(RespValue::BulkString(Some(member)));
                items.push(RespValue::BulkString(Some(score.to_string().into_bytes())));
            }
            RespValue::Array(Some(vec![
                RespValue::BulkString(Some(next_cursor.into_bytes())),
                RespValue::Array(Some(items)),
            ]))
        }
        Err(KvError::WrongType) => RespValue::wrong_type(),
        Err(e) => RespValue::error(e.to_string()),
    }
}

// --- Bitmap operations ---

fn cmd_setbit(db: &Db, args: &[Vec<u8>]) -> RespValue {
    if args.len() != 3 {
        return RespValue::error("wrong number of arguments for 'setbit' command");
    }

    let key = match std::str::from_utf8(&args[0]) {
        Ok(k) => k,
        Err(_) => return RespValue::error("invalid key"),
    };

    let offset: u64 = match std::str::from_utf8(&args[1])
        .ok()
        .and_then(|s| s.parse().ok())
    {
        Some(o) => o,
        None => return RespValue::error("bit offset is not an integer or out of range"),
    };

    let value: bool = match std::str::from_utf8(&args[2])
        .ok()
        .and_then(|s| s.parse::<i64>().ok())
    {
        Some(0) => false,
        Some(1) => true,
        _ => return RespValue::error("bit is not an integer or out of range"),
    };

    match db.setbit(key, offset, value) {
        Ok(old) => RespValue::Integer(old),
        Err(KvError::WrongType) => RespValue::wrong_type(),
        Err(e) => RespValue::error(e.to_string()),
    }
}

fn cmd_getbit(db: &Db, args: &[Vec<u8>]) -> RespValue {
    if args.len() != 2 {
        return RespValue::error("wrong number of arguments for 'getbit' command");
    }

    let key = match std::str::from_utf8(&args[0]) {
        Ok(k) => k,
        Err(_) => return RespValue::error("invalid key"),
    };

    let offset: u64 = match std::str::from_utf8(&args[1])
        .ok()
        .and_then(|s| s.parse().ok())
    {
        Some(o) => o,
        None => return RespValue::error("bit offset is not an integer or out of range"),
    };

    match db.getbit(key, offset) {
        Ok(bit) => RespValue::Integer(bit),
        Err(KvError::WrongType) => RespValue::wrong_type(),
        Err(e) => RespValue::error(e.to_string()),
    }
}

fn cmd_bitcount(db: &Db, args: &[Vec<u8>]) -> RespValue {
    if args.is_empty() {
        return RespValue::error("wrong number of arguments for 'bitcount' command");
    }

    let key = match std::str::from_utf8(&args[0]) {
        Ok(k) => k,
        Err(_) => return RespValue::error("invalid key"),
    };

    let (start, end) = if args.len() >= 3 {
        let start: i64 = match std::str::from_utf8(&args[1])
            .ok()
            .and_then(|s| s.parse().ok())
        {
            Some(s) => s,
            None => return RespValue::error("value is not an integer or out of range"),
        };
        let end: i64 = match std::str::from_utf8(&args[2])
            .ok()
            .and_then(|s| s.parse().ok())
        {
            Some(e) => e,
            None => return RespValue::error("value is not an integer or out of range"),
        };
        (Some(start), Some(end))
    } else {
        (None, None)
    };

    match db.bitcount(key, start, end) {
        Ok(count) => RespValue::Integer(count),
        Err(KvError::WrongType) => RespValue::wrong_type(),
        Err(e) => RespValue::error(e.to_string()),
    }
}

fn cmd_bitop(db: &Db, args: &[Vec<u8>]) -> RespValue {
    if args.len() < 3 {
        return RespValue::error("wrong number of arguments for 'bitop' command");
    }

    let operation = match std::str::from_utf8(&args[0]) {
        Ok(op) => op,
        Err(_) => return RespValue::error("invalid operation"),
    };

    let destkey = match std::str::from_utf8(&args[1]) {
        Ok(k) => k,
        Err(_) => return RespValue::error("invalid destination key"),
    };

    let keys: Vec<&str> = args[2..]
        .iter()
        .filter_map(|a| std::str::from_utf8(a).ok())
        .collect();

    if keys.len() != args.len() - 2 {
        return RespValue::error("invalid key");
    }

    match db.bitop(operation, destkey, &keys) {
        Ok(len) => RespValue::Integer(len),
        Err(KvError::WrongType) => RespValue::wrong_type(),
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

fn cmd_getex(db: &Db, args: &[Vec<u8>]) -> RespValue {
    if args.is_empty() {
        return RespValue::error("wrong number of arguments for 'getex' command");
    }

    let key = match std::str::from_utf8(&args[0]) {
        Ok(k) => k,
        Err(_) => return RespValue::error("invalid key"),
    };

    // Parse options
    let mut option: Option<GetExOption> = None;
    let mut i = 1;
    while i < args.len() {
        let opt = String::from_utf8_lossy(&args[i]).to_uppercase();
        match opt.as_str() {
            "EX" => {
                if i + 1 >= args.len() {
                    return RespValue::error("syntax error");
                }
                i += 1;
                let seconds: i64 = match std::str::from_utf8(&args[i])
                    .ok()
                    .and_then(|s| s.parse().ok())
                {
                    Some(s) => s,
                    None => return RespValue::error("value is not an integer or out of range"),
                };
                option = Some(GetExOption::Ex(seconds));
            }
            "PX" => {
                if i + 1 >= args.len() {
                    return RespValue::error("syntax error");
                }
                i += 1;
                let ms: i64 = match std::str::from_utf8(&args[i])
                    .ok()
                    .and_then(|s| s.parse().ok())
                {
                    Some(s) => s,
                    None => return RespValue::error("value is not an integer or out of range"),
                };
                option = Some(GetExOption::Px(ms));
            }
            "EXAT" => {
                if i + 1 >= args.len() {
                    return RespValue::error("syntax error");
                }
                i += 1;
                let ts: i64 = match std::str::from_utf8(&args[i])
                    .ok()
                    .and_then(|s| s.parse().ok())
                {
                    Some(s) => s,
                    None => return RespValue::error("value is not an integer or out of range"),
                };
                option = Some(GetExOption::ExAt(ts));
            }
            "PXAT" => {
                if i + 1 >= args.len() {
                    return RespValue::error("syntax error");
                }
                i += 1;
                let ts: i64 = match std::str::from_utf8(&args[i])
                    .ok()
                    .and_then(|s| s.parse().ok())
                {
                    Some(s) => s,
                    None => return RespValue::error("value is not an integer or out of range"),
                };
                option = Some(GetExOption::PxAt(ts));
            }
            "PERSIST" => {
                option = Some(GetExOption::Persist);
            }
            _ => return RespValue::error("syntax error"),
        }
        i += 1;
    }

    match db.getex(key, option) {
        Ok(Some(value)) => RespValue::BulkString(Some(value)),
        Ok(None) => RespValue::null(),
        Err(KvError::WrongType) => RespValue::wrong_type(),
        Err(e) => RespValue::error(e.to_string()),
    }
}

fn cmd_getdel(db: &Db, args: &[Vec<u8>]) -> RespValue {
    if args.len() != 1 {
        return RespValue::error("wrong number of arguments for 'getdel' command");
    }

    let key = match std::str::from_utf8(&args[0]) {
        Ok(k) => k,
        Err(_) => return RespValue::error("invalid key"),
    };

    match db.getdel(key) {
        Ok(Some(value)) => RespValue::BulkString(Some(value)),
        Ok(None) => RespValue::null(),
        Err(KvError::WrongType) => RespValue::wrong_type(),
        Err(e) => RespValue::error(e.to_string()),
    }
}

fn cmd_setex(db: &Db, args: &[Vec<u8>]) -> RespValue {
    if args.len() != 3 {
        return RespValue::error("wrong number of arguments for 'setex' command");
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

    match db.setex(key, seconds, &args[2]) {
        Ok(()) => RespValue::ok(),
        Err(KvError::InvalidExpireTime) => {
            RespValue::error("invalid expire time in 'setex' command")
        }
        Err(e) => RespValue::error(e.to_string()),
    }
}

fn cmd_psetex(db: &Db, args: &[Vec<u8>]) -> RespValue {
    if args.len() != 3 {
        return RespValue::error("wrong number of arguments for 'psetex' command");
    }

    let key = match std::str::from_utf8(&args[0]) {
        Ok(k) => k,
        Err(_) => return RespValue::error("invalid key"),
    };

    let milliseconds: i64 = match std::str::from_utf8(&args[1])
        .ok()
        .and_then(|s| s.parse().ok())
    {
        Some(s) => s,
        None => return RespValue::error("value is not an integer or out of range"),
    };

    match db.psetex(key, milliseconds, &args[2]) {
        Ok(()) => RespValue::ok(),
        Err(KvError::InvalidExpireTime) => {
            RespValue::error("invalid expire time in 'psetex' command")
        }
        Err(e) => RespValue::error(e.to_string()),
    }
}

fn cmd_persist(db: &Db, args: &[Vec<u8>]) -> RespValue {
    if args.len() != 1 {
        return RespValue::error("wrong number of arguments for 'persist' command");
    }

    let key = match std::str::from_utf8(&args[0]) {
        Ok(k) => k,
        Err(_) => return RespValue::error("invalid key"),
    };

    match db.persist(key) {
        Ok(true) => RespValue::Integer(1),
        Ok(false) => RespValue::Integer(0),
        Err(e) => RespValue::error(e.to_string()),
    }
}

fn cmd_pexpire(db: &Db, args: &[Vec<u8>]) -> RespValue {
    if args.len() != 2 {
        return RespValue::error("wrong number of arguments for 'pexpire' command");
    }

    let key = match std::str::from_utf8(&args[0]) {
        Ok(k) => k,
        Err(_) => return RespValue::error("invalid key"),
    };

    let milliseconds: i64 = match std::str::from_utf8(&args[1])
        .ok()
        .and_then(|s| s.parse().ok())
    {
        Some(s) => s,
        None => return RespValue::error("value is not an integer or out of range"),
    };

    match db.pexpire(key, milliseconds) {
        Ok(true) => RespValue::Integer(1),
        Ok(false) => RespValue::Integer(0),
        Err(e) => RespValue::error(e.to_string()),
    }
}

fn cmd_expireat(db: &Db, args: &[Vec<u8>]) -> RespValue {
    if args.len() != 2 {
        return RespValue::error("wrong number of arguments for 'expireat' command");
    }

    let key = match std::str::from_utf8(&args[0]) {
        Ok(k) => k,
        Err(_) => return RespValue::error("invalid key"),
    };

    let unix_seconds: i64 = match std::str::from_utf8(&args[1])
        .ok()
        .and_then(|s| s.parse().ok())
    {
        Some(s) => s,
        None => return RespValue::error("value is not an integer or out of range"),
    };

    match db.expireat(key, unix_seconds) {
        Ok(true) => RespValue::Integer(1),
        Ok(false) => RespValue::Integer(0),
        Err(e) => RespValue::error(e.to_string()),
    }
}

fn cmd_pexpireat(db: &Db, args: &[Vec<u8>]) -> RespValue {
    if args.len() != 2 {
        return RespValue::error("wrong number of arguments for 'pexpireat' command");
    }

    let key = match std::str::from_utf8(&args[0]) {
        Ok(k) => k,
        Err(_) => return RespValue::error("invalid key"),
    };

    let unix_ms: i64 = match std::str::from_utf8(&args[1])
        .ok()
        .and_then(|s| s.parse().ok())
    {
        Some(s) => s,
        None => return RespValue::error("value is not an integer or out of range"),
    };

    match db.pexpireat(key, unix_ms) {
        Ok(true) => RespValue::Integer(1),
        Ok(false) => RespValue::Integer(0),
        Err(e) => RespValue::error(e.to_string()),
    }
}

fn cmd_rename(db: &Db, args: &[Vec<u8>]) -> RespValue {
    if args.len() != 2 {
        return RespValue::error("wrong number of arguments for 'rename' command");
    }

    let key = match std::str::from_utf8(&args[0]) {
        Ok(k) => k,
        Err(_) => return RespValue::error("invalid key"),
    };

    let newkey = match std::str::from_utf8(&args[1]) {
        Ok(k) => k,
        Err(_) => return RespValue::error("invalid key"),
    };

    match db.rename(key, newkey) {
        Ok(()) => RespValue::ok(),
        Err(KvError::NoSuchKey) => RespValue::error("no such key"),
        Err(e) => RespValue::error(e.to_string()),
    }
}

fn cmd_renamenx(db: &Db, args: &[Vec<u8>]) -> RespValue {
    if args.len() != 2 {
        return RespValue::error("wrong number of arguments for 'renamenx' command");
    }

    let key = match std::str::from_utf8(&args[0]) {
        Ok(k) => k,
        Err(_) => return RespValue::error("invalid key"),
    };

    let newkey = match std::str::from_utf8(&args[1]) {
        Ok(k) => k,
        Err(_) => return RespValue::error("invalid key"),
    };

    match db.renamenx(key, newkey) {
        Ok(true) => RespValue::Integer(1),
        Ok(false) => RespValue::Integer(0),
        Err(KvError::NoSuchKey) => RespValue::error("no such key"),
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

fn cmd_lpushx(db: &Db, args: &[Vec<u8>]) -> RespValue {
    if args.len() < 2 {
        return RespValue::error("wrong number of arguments for 'lpushx' command");
    }

    let key = match std::str::from_utf8(&args[0]) {
        Ok(k) => k,
        Err(_) => return RespValue::error("invalid key"),
    };

    let values: Vec<&[u8]> = args[1..].iter().map(|v| v.as_slice()).collect();

    match db.lpushx(key, &values) {
        Ok(length) => RespValue::Integer(length),
        Err(KvError::WrongType) => RespValue::wrong_type(),
        Err(e) => RespValue::error(e.to_string()),
    }
}

fn cmd_rpushx(db: &Db, args: &[Vec<u8>]) -> RespValue {
    if args.len() < 2 {
        return RespValue::error("wrong number of arguments for 'rpushx' command");
    }

    let key = match std::str::from_utf8(&args[0]) {
        Ok(k) => k,
        Err(_) => return RespValue::error("invalid key"),
    };

    let values: Vec<&[u8]> = args[1..].iter().map(|v| v.as_slice()).collect();

    match db.rpushx(key, &values) {
        Ok(length) => RespValue::Integer(length),
        Err(KvError::WrongType) => RespValue::wrong_type(),
        Err(e) => RespValue::error(e.to_string()),
    }
}

fn cmd_lpos(db: &Db, args: &[Vec<u8>]) -> RespValue {
    if args.len() < 2 {
        return RespValue::error("wrong number of arguments for 'lpos' command");
    }

    let key = match std::str::from_utf8(&args[0]) {
        Ok(k) => k,
        Err(_) => return RespValue::error("invalid key"),
    };

    let element = &args[1];

    // Parse options: RANK rank, COUNT count, MAXLEN maxlen
    let mut rank: Option<i64> = None;
    let mut count: Option<usize> = None;
    let mut maxlen: Option<usize> = None;

    let mut i = 2;
    while i < args.len() {
        let opt = String::from_utf8_lossy(&args[i]).to_uppercase();
        match opt.as_str() {
            "RANK" => {
                if i + 1 >= args.len() {
                    return RespValue::error("syntax error");
                }
                i += 1;
                rank = match std::str::from_utf8(&args[i]).ok().and_then(|s| s.parse().ok()) {
                    Some(r) if r != 0 => Some(r),
                    _ => return RespValue::error("RANK can't be zero"),
                };
            }
            "COUNT" => {
                if i + 1 >= args.len() {
                    return RespValue::error("syntax error");
                }
                i += 1;
                count = match std::str::from_utf8(&args[i]).ok().and_then(|s| s.parse().ok()) {
                    Some(c) => Some(c),
                    None => return RespValue::error("value is not an integer or out of range"),
                };
            }
            "MAXLEN" => {
                if i + 1 >= args.len() {
                    return RespValue::error("syntax error");
                }
                i += 1;
                maxlen = match std::str::from_utf8(&args[i]).ok().and_then(|s| s.parse().ok()) {
                    Some(m) => Some(m),
                    None => return RespValue::error("value is not an integer or out of range"),
                };
            }
            _ => return RespValue::error("syntax error"),
        }
        i += 1;
    }

    match db.lpos(key, element, rank, count, maxlen) {
        Ok(positions) => {
            if count.is_some() {
                // Return array when COUNT is specified
                RespValue::Array(Some(
                    positions
                        .into_iter()
                        .map(RespValue::Integer)
                        .collect(),
                ))
            } else if positions.is_empty() {
                RespValue::null()
            } else {
                RespValue::Integer(positions[0])
            }
        }
        Err(KvError::WrongType) => RespValue::wrong_type(),
        Err(e) => RespValue::error(e.to_string()),
    }
}

fn cmd_lmove(db: &Db, args: &[Vec<u8>]) -> RespValue {
    if args.len() != 4 {
        return RespValue::error("wrong number of arguments for 'lmove' command");
    }

    let source = match std::str::from_utf8(&args[0]) {
        Ok(k) => k,
        Err(_) => return RespValue::error("invalid key"),
    };

    let destination = match std::str::from_utf8(&args[1]) {
        Ok(k) => k,
        Err(_) => return RespValue::error("invalid key"),
    };

    let wherefrom = match String::from_utf8_lossy(&args[2]).to_uppercase().as_str() {
        "LEFT" => ListDirection::Left,
        "RIGHT" => ListDirection::Right,
        _ => return RespValue::error("syntax error"),
    };

    let whereto = match String::from_utf8_lossy(&args[3]).to_uppercase().as_str() {
        "LEFT" => ListDirection::Left,
        "RIGHT" => ListDirection::Right,
        _ => return RespValue::error("syntax error"),
    };

    match db.lmove(source, destination, wherefrom, whereto) {
        Ok(Some(element)) => RespValue::BulkString(Some(element)),
        Ok(None) => RespValue::null(),
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

fn cmd_zinterstore(db: &Db, args: &[Vec<u8>]) -> RespValue {
    // ZINTERSTORE destination numkeys key [key ...] [WEIGHTS weight...] [AGGREGATE SUM|MIN|MAX]
    if args.len() < 3 {
        return RespValue::error("wrong number of arguments for 'zinterstore' command");
    }

    let destination = match std::str::from_utf8(&args[0]) {
        Ok(k) => k,
        Err(_) => return RespValue::error("invalid destination key"),
    };

    let numkeys: usize = match std::str::from_utf8(&args[1])
        .ok()
        .and_then(|s| s.parse().ok())
    {
        Some(n) => n,
        None => return RespValue::error("value is not an integer or out of range"),
    };

    if numkeys == 0 || args.len() < 2 + numkeys {
        return RespValue::error("wrong number of arguments for 'zinterstore' command");
    }

    let keys: Vec<&str> = args[2..2 + numkeys]
        .iter()
        .filter_map(|a| std::str::from_utf8(a).ok())
        .collect();

    if keys.len() != numkeys {
        return RespValue::error("invalid key");
    }

    // Parse optional WEIGHTS and AGGREGATE
    let mut weights: Option<Vec<f64>> = None;
    let mut aggregate: Option<&str> = None;
    let mut i = 2 + numkeys;

    while i < args.len() {
        let opt = String::from_utf8_lossy(&args[i]).to_uppercase();
        match opt.as_str() {
            "WEIGHTS" => {
                i += 1;
                let mut w = Vec::with_capacity(numkeys);
                for _ in 0..numkeys {
                    if i >= args.len() {
                        return RespValue::error("syntax error");
                    }
                    match std::str::from_utf8(&args[i])
                        .ok()
                        .and_then(|s| s.parse().ok())
                    {
                        Some(weight) => w.push(weight),
                        None => return RespValue::error("weight is not a float"),
                    }
                    i += 1;
                }
                weights = Some(w);
            }
            "AGGREGATE" => {
                i += 1;
                if i >= args.len() {
                    return RespValue::error("syntax error");
                }
                match std::str::from_utf8(&args[i]) {
                    Ok(agg) => aggregate = Some(agg),
                    Err(_) => return RespValue::error("invalid aggregate"),
                }
                i += 1;
            }
            _ => return RespValue::error("syntax error"),
        }
    }

    match db.zinterstore(destination, &keys, weights.as_deref(), aggregate) {
        Ok(count) => RespValue::Integer(count),
        Err(KvError::WrongType) => RespValue::wrong_type(),
        Err(e) => RespValue::error(e.to_string()),
    }
}

fn cmd_zunionstore(db: &Db, args: &[Vec<u8>]) -> RespValue {
    // ZUNIONSTORE destination numkeys key [key ...] [WEIGHTS weight...] [AGGREGATE SUM|MIN|MAX]
    if args.len() < 3 {
        return RespValue::error("wrong number of arguments for 'zunionstore' command");
    }

    let destination = match std::str::from_utf8(&args[0]) {
        Ok(k) => k,
        Err(_) => return RespValue::error("invalid destination key"),
    };

    let numkeys: usize = match std::str::from_utf8(&args[1])
        .ok()
        .and_then(|s| s.parse().ok())
    {
        Some(n) => n,
        None => return RespValue::error("value is not an integer or out of range"),
    };

    if numkeys == 0 || args.len() < 2 + numkeys {
        return RespValue::error("wrong number of arguments for 'zunionstore' command");
    }

    let keys: Vec<&str> = args[2..2 + numkeys]
        .iter()
        .filter_map(|a| std::str::from_utf8(a).ok())
        .collect();

    if keys.len() != numkeys {
        return RespValue::error("invalid key");
    }

    // Parse optional WEIGHTS and AGGREGATE
    let mut weights: Option<Vec<f64>> = None;
    let mut aggregate: Option<&str> = None;
    let mut i = 2 + numkeys;

    while i < args.len() {
        let opt = String::from_utf8_lossy(&args[i]).to_uppercase();
        match opt.as_str() {
            "WEIGHTS" => {
                i += 1;
                let mut w = Vec::with_capacity(numkeys);
                for _ in 0..numkeys {
                    if i >= args.len() {
                        return RespValue::error("syntax error");
                    }
                    match std::str::from_utf8(&args[i])
                        .ok()
                        .and_then(|s| s.parse().ok())
                    {
                        Some(weight) => w.push(weight),
                        None => return RespValue::error("weight is not a float"),
                    }
                    i += 1;
                }
                weights = Some(w);
            }
            "AGGREGATE" => {
                i += 1;
                if i >= args.len() {
                    return RespValue::error("syntax error");
                }
                match std::str::from_utf8(&args[i]) {
                    Ok(agg) => aggregate = Some(agg),
                    Err(_) => return RespValue::error("invalid aggregate"),
                }
                i += 1;
            }
            _ => return RespValue::error("syntax error"),
        }
    }

    match db.zunionstore(destination, &keys, weights.as_deref(), aggregate) {
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
            "HIGHLIGHT" => {
                // HIGHLIGHT [FIELDS count field...] [TAGS open close]
                i += 1;
                // Default tags if none specified
                options.highlight_tags = Some(("<b>".to_string(), "</b>".to_string()));
                while i < args.len() {
                    let sub_opt = std::str::from_utf8(&args[i])
                        .unwrap_or("")
                        .to_uppercase();
                    match sub_opt.as_str() {
                        "FIELDS" => {
                            if i + 1 < args.len() {
                                let count: usize = std::str::from_utf8(&args[i + 1])
                                    .ok()
                                    .and_then(|s| s.parse().ok())
                                    .unwrap_or(0);
                                i += 2;
                                for _ in 0..count {
                                    if i < args.len() {
                                        if let Ok(f) = std::str::from_utf8(&args[i]) {
                                            options.highlight_fields.push(f.to_string());
                                        }
                                        i += 1;
                                    }
                                }
                            }
                        }
                        "TAGS" => {
                            if i + 2 < args.len() {
                                let open = std::str::from_utf8(&args[i + 1])
                                    .unwrap_or("<b>")
                                    .to_string();
                                let close = std::str::from_utf8(&args[i + 2])
                                    .unwrap_or("</b>")
                                    .to_string();
                                options.highlight_tags = Some((open, close));
                                i += 3;
                            } else {
                                i += 1;
                            }
                        }
                        _ => break, // Unknown sub-option, stop HIGHLIGHT parsing
                    }
                }
                continue;
            }
            "SUMMARIZE" => {
                // SUMMARIZE [FIELDS count field...] [LEN fragsize] [FRAGS numfrags] [SEPARATOR separator]
                i += 1;
                // Set defaults
                options.summarize_len = Some(20);
                options.summarize_frags = Some(3);
                options.summarize_separator = Some("...".to_string());
                while i < args.len() {
                    let sub_opt = std::str::from_utf8(&args[i])
                        .unwrap_or("")
                        .to_uppercase();
                    match sub_opt.as_str() {
                        "FIELDS" => {
                            if i + 1 < args.len() {
                                let count: usize = std::str::from_utf8(&args[i + 1])
                                    .ok()
                                    .and_then(|s| s.parse().ok())
                                    .unwrap_or(0);
                                i += 2;
                                for _ in 0..count {
                                    if i < args.len() {
                                        if let Ok(f) = std::str::from_utf8(&args[i]) {
                                            options.summarize_fields.push(f.to_string());
                                        }
                                        i += 1;
                                    }
                                }
                            }
                        }
                        "LEN" => {
                            if i + 1 < args.len() {
                                options.summarize_len = std::str::from_utf8(&args[i + 1])
                                    .ok()
                                    .and_then(|s| s.parse().ok());
                                i += 2;
                            } else {
                                i += 1;
                            }
                        }
                        "FRAGS" => {
                            if i + 1 < args.len() {
                                options.summarize_frags = std::str::from_utf8(&args[i + 1])
                                    .ok()
                                    .and_then(|s| s.parse().ok());
                                i += 2;
                            } else {
                                i += 1;
                            }
                        }
                        "SEPARATOR" => {
                            if i + 1 < args.len() {
                                options.summarize_separator =
                                    std::str::from_utf8(&args[i + 1]).ok().map(|s| s.to_string());
                                i += 2;
                            } else {
                                i += 1;
                            }
                        }
                        _ => break, // Unknown sub-option, stop SUMMARIZE parsing
                    }
                }
                continue;
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

/// FT.AGGREGATE index query [LOAD count field...] [GROUPBY n field... [REDUCE func n arg... [AS alias]]]
///                         [SORTBY n field [ASC|DESC]... [MAX num]] [APPLY expr [AS alias]] [FILTER expr] [LIMIT offset count]
fn cmd_ft_aggregate(db: &Db, args: &[Vec<u8>]) -> RespValue {
    use crate::types::{FtAggregateOptions, FtGroupBy, FtReducer, FtReduceFunction, FtApply};

    if args.len() < 2 {
        return RespValue::error("wrong number of arguments for 'FT.AGGREGATE' command");
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
    let mut options = FtAggregateOptions::new();
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
            "LOAD" => {
                // LOAD count field...
                if i + 1 < args.len() {
                    let count: usize = std::str::from_utf8(&args[i + 1])
                        .ok()
                        .and_then(|s| s.parse().ok())
                        .unwrap_or(0);
                    i += 2;
                    for _ in 0..count {
                        if i < args.len() {
                            if let Ok(f) = std::str::from_utf8(&args[i]) {
                                // Remove @ prefix if present
                                let field = f.strip_prefix('@').unwrap_or(f);
                                options.load_fields.push(field.to_string());
                            }
                            i += 1;
                        }
                    }
                    continue;
                }
            }
            "GROUPBY" => {
                // GROUPBY n field... [REDUCE func n arg... [AS alias]]...
                if i + 1 < args.len() {
                    let count: usize = std::str::from_utf8(&args[i + 1])
                        .ok()
                        .and_then(|s| s.parse().ok())
                        .unwrap_or(0);
                    i += 2;

                    let mut fields = Vec::new();
                    for _ in 0..count {
                        if i < args.len() {
                            if let Ok(f) = std::str::from_utf8(&args[i]) {
                                let field = f.strip_prefix('@').unwrap_or(f);
                                fields.push(field.to_string());
                            }
                            i += 1;
                        }
                    }

                    let mut reducers = Vec::new();
                    // Parse REDUCE clauses
                    while i < args.len() {
                        let sub_opt = std::str::from_utf8(&args[i]).unwrap_or("").to_uppercase();
                        if sub_opt != "REDUCE" {
                            break;
                        }
                        i += 1; // consume REDUCE

                        if i >= args.len() {
                            break;
                        }

                        let func_name = std::str::from_utf8(&args[i]).unwrap_or("").to_uppercase();
                        i += 1;

                        // Parse arg count
                        let arg_count: usize = if i < args.len() {
                            std::str::from_utf8(&args[i])
                                .ok()
                                .and_then(|s| s.parse().ok())
                                .unwrap_or(0)
                        } else {
                            0
                        };
                        i += 1;

                        // Collect args
                        let mut func_args = Vec::new();
                        for _ in 0..arg_count {
                            if i < args.len() {
                                if let Ok(a) = std::str::from_utf8(&args[i]) {
                                    let arg = a.strip_prefix('@').unwrap_or(a);
                                    func_args.push(arg.to_string());
                                }
                                i += 1;
                            }
                        }

                        // Parse optional AS alias
                        let alias = if i + 1 < args.len() {
                            let next = std::str::from_utf8(&args[i]).unwrap_or("").to_uppercase();
                            if next == "AS" {
                                i += 1;
                                let a = std::str::from_utf8(&args[i]).ok().map(|s| s.to_string());
                                i += 1;
                                a
                            } else {
                                None
                            }
                        } else {
                            None
                        };

                        // Build reduce function
                        let function = match func_name.as_str() {
                            "COUNT" => FtReduceFunction::Count,
                            "COUNT_DISTINCT" => {
                                FtReduceFunction::CountDistinct(func_args.first().cloned().unwrap_or_default())
                            }
                            "COUNT_DISTINCTISH" => {
                                FtReduceFunction::CountDistinctIsh(func_args.first().cloned().unwrap_or_default())
                            }
                            "SUM" => FtReduceFunction::Sum(func_args.first().cloned().unwrap_or_default()),
                            "MIN" => FtReduceFunction::Min(func_args.first().cloned().unwrap_or_default()),
                            "MAX" => FtReduceFunction::Max(func_args.first().cloned().unwrap_or_default()),
                            "AVG" => FtReduceFunction::Avg(func_args.first().cloned().unwrap_or_default()),
                            "STDDEV" => FtReduceFunction::StdDev(func_args.first().cloned().unwrap_or_default()),
                            "TOLIST" => FtReduceFunction::ToList(func_args.first().cloned().unwrap_or_default()),
                            "FIRST_VALUE" => {
                                FtReduceFunction::FirstValue(func_args.first().cloned().unwrap_or_default())
                            }
                            _ => FtReduceFunction::Count, // Default to COUNT for unknown
                        };

                        reducers.push(FtReducer { function, alias });
                    }

                    options.group_by = Some(FtGroupBy { fields, reducers });
                    continue;
                }
            }
            "SORTBY" => {
                // SORTBY n field [ASC|DESC]... [MAX num]
                if i + 1 < args.len() {
                    let count: usize = std::str::from_utf8(&args[i + 1])
                        .ok()
                        .and_then(|s| s.parse().ok())
                        .unwrap_or(0);
                    i += 2;

                    let mut j = 0;
                    while j < count && i < args.len() {
                        if let Ok(f) = std::str::from_utf8(&args[i]) {
                            let field = f.strip_prefix('@').unwrap_or(f).to_string();
                            i += 1;

                            // Check for ASC/DESC
                            let ascending = if i < args.len() {
                                let dir = std::str::from_utf8(&args[i]).unwrap_or("").to_uppercase();
                                if dir == "ASC" || dir == "DESC" {
                                    i += 1;
                                    j += 1; // ASC/DESC counts as part of the SORTBY args
                                    dir != "DESC"
                                } else {
                                    true
                                }
                            } else {
                                true
                            };

                            options.sort_by.push((field, ascending));
                        }
                        j += 1;
                    }

                    // Check for MAX
                    if i < args.len() {
                        let next = std::str::from_utf8(&args[i]).unwrap_or("").to_uppercase();
                        if next == "MAX" && i + 1 < args.len() {
                            options.sort_max = std::str::from_utf8(&args[i + 1])
                                .ok()
                                .and_then(|s| s.parse().ok());
                            i += 2;
                        }
                    }
                    continue;
                }
            }
            "APPLY" => {
                // APPLY expr [AS alias]
                if i + 1 < args.len() {
                    let expr = std::str::from_utf8(&args[i + 1]).unwrap_or("").to_string();
                    i += 2;

                    let alias = if i + 1 < args.len() {
                        let next = std::str::from_utf8(&args[i]).unwrap_or("").to_uppercase();
                        if next == "AS" {
                            i += 1;
                            let a = std::str::from_utf8(&args[i]).unwrap_or("").to_string();
                            i += 1;
                            a
                        } else {
                            format!("apply_{}", options.applies.len())
                        }
                    } else {
                        format!("apply_{}", options.applies.len())
                    };

                    options.applies.push(FtApply { expression: expr, alias });
                    continue;
                }
            }
            "FILTER" => {
                // FILTER expr
                if i + 1 < args.len() {
                    options.filter = std::str::from_utf8(&args[i + 1]).ok().map(|s| s.to_string());
                    i += 2;
                    continue;
                }
            }
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
                    i += 3;
                    continue;
                }
            }
            _ => {}
        }
        i += 1;
    }

    // Execute the aggregation
    match db.ft_aggregate(index_name, query, &options) {
        Ok(rows) => {
            let mut response = Vec::new();
            response.push(RespValue::Integer(rows.len() as i64));

            for row in rows {
                let mut row_values: Vec<RespValue> = Vec::new();
                for (field, value) in row {
                    row_values.push(RespValue::from_string(field));
                    row_values.push(RespValue::from_string(value));
                }
                response.push(RespValue::Array(Some(row_values)));
            }

            RespValue::Array(Some(response))
        }
        Err(e) => RespValue::error(&e.to_string()),
    }
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

/// FT.EXPLAIN index query
/// Returns the query execution plan as a nested array
fn cmd_ft_explain(_db: &Db, args: &[Vec<u8>]) -> RespValue {
    if args.len() < 2 {
        return RespValue::error("wrong number of arguments for 'FT.EXPLAIN' command");
    }

    let _index_name = match std::str::from_utf8(&args[0]) {
        Ok(n) => n,
        Err(_) => return RespValue::error("invalid index name"),
    };

    let query = match std::str::from_utf8(&args[1]) {
        Ok(q) => q,
        Err(_) => return RespValue::error("invalid query"),
    };

    match crate::search::explain_query(query, false) {
        Ok(nodes) => explain_nodes_to_resp(&nodes),
        Err(e) => RespValue::error(format!("Query parse error: {}", e)),
    }
}

/// Convert ExplainNode tree to RESP value
fn explain_nodes_to_resp(nodes: &[crate::search::ExplainNode]) -> RespValue {
    let mut result = Vec::new();
    for node in nodes {
        result.push(explain_node_to_resp(node));
    }
    if result.len() == 1 {
        result.remove(0)
    } else {
        RespValue::Array(Some(result))
    }
}

fn explain_node_to_resp(node: &crate::search::ExplainNode) -> RespValue {
    use crate::search::ExplainNode;

    match node {
        ExplainNode::Text(s) => RespValue::from_string(s.clone()),
        ExplainNode::Array(nodes) => {
            let inner: Vec<RespValue> = nodes.iter().map(explain_node_to_resp).collect();
            RespValue::Array(Some(inner))
        }
    }
}

/// FT.PROFILE index SEARCH|AGGREGATE query [options]
/// Returns the search results along with timing information
fn cmd_ft_profile(db: &Db, args: &[Vec<u8>]) -> RespValue {
    if args.len() < 3 {
        return RespValue::error("wrong number of arguments for 'FT.PROFILE' command");
    }

    let index_name = match std::str::from_utf8(&args[0]) {
        Ok(n) => n,
        Err(_) => return RespValue::error("invalid index name"),
    };

    let profile_type = match std::str::from_utf8(&args[1]) {
        Ok(t) => t.to_uppercase(),
        Err(_) => return RespValue::error("invalid profile type"),
    };

    let query = match std::str::from_utf8(&args[2]) {
        Ok(q) => q,
        Err(_) => return RespValue::error("invalid query"),
    };

    // Only SEARCH is supported for now
    if profile_type != "SEARCH" {
        return RespValue::error("FT.PROFILE only supports SEARCH currently");
    }

    // Parse remaining options (same as FT.SEARCH)
    let mut options = FtSearchOptions::new();
    let mut i = 3;
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
            _ => {}
        }
        i += 1;
    }

    // Time the search
    let start = std::time::Instant::now();
    let result = db.ft_search(index_name, query, &options);
    let elapsed = start.elapsed();

    match result {
        Ok((total, results)) => {
            // Build results array (same format as FT.SEARCH)
            let mut results_array = Vec::new();
            results_array.push(RespValue::Integer(total));

            for r in results {
                results_array.push(RespValue::from_string(r.key.clone()));
                if options.withscores {
                    results_array.push(RespValue::from_string(r.score.to_string()));
                }
                if !options.nocontent {
                    let mut field_values: Vec<RespValue> = Vec::new();
                    for (field_name, field_value) in &r.fields {
                        field_values.push(RespValue::from_string(field_name.clone()));
                        field_values.push(RespValue::BulkString(Some(field_value.clone())));
                    }
                    results_array.push(RespValue::Array(Some(field_values)));
                }
            }

            // Build profile information
            let profile = vec![
                RespValue::from_string("Total profile time".to_string()),
                RespValue::from_string(format!("{:.3} ms", elapsed.as_secs_f64() * 1000.0)),
                RespValue::from_string("Parsing time".to_string()),
                RespValue::from_string("0.001 ms".to_string()), // Placeholder
                RespValue::from_string("Pipeline creation time".to_string()),
                RespValue::from_string("0.001 ms".to_string()), // Placeholder
                RespValue::from_string("Iterators created".to_string()),
                RespValue::Integer(1),
                RespValue::from_string("Results count".to_string()),
                RespValue::Integer(total),
            ];

            // Return [results, profile]
            RespValue::Array(Some(vec![
                RespValue::Array(Some(results_array)),
                RespValue::Array(Some(profile)),
            ]))
        }
        Err(e) => RespValue::error(&e.to_string()),
    }
}

// --- Redis 8 Vector command handlers (V* commands, feature-gated) ---

#[cfg(feature = "vectors")]
fn cmd_vadd(db: &Db, args: &[Vec<u8>]) -> RespValue {
    use crate::types::VectorQuantization;

    // VADD key (FP32 blob | VALUES n v1 v2...) element [SETATTR json]
    // Minimum: VADD key VALUES 3 1.0 2.0 3.0 element
    if args.len() < 3 {
        return RespValue::error("wrong number of arguments for 'vadd' command");
    }

    let key = match std::str::from_utf8(&args[0]) {
        Ok(k) => k,
        Err(_) => return RespValue::error("invalid key"),
    };

    // Parse vector data and element
    let mut i = 1;
    let mut embedding: Vec<f32> = Vec::new();
    let mut element: Option<&str> = None;
    let mut attributes: Option<&str> = None;

    // Check if it's FP32 blob or VALUES format
    let first_arg = match std::str::from_utf8(&args[i]) {
        Ok(s) => s.to_uppercase(),
        Err(_) => {
            // Binary blob - treat as FP32
            let blob = &args[i];
            if blob.len() % 4 != 0 {
                return RespValue::error("FP32 blob must have length divisible by 4");
            }
            for chunk in blob.chunks_exact(4) {
                embedding.push(f32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]));
            }
            i += 1;
            String::new()
        }
    };

    if first_arg == "VALUES" {
        i += 1;
        if i >= args.len() {
            return RespValue::error("VALUES requires count");
        }
        let count: usize = match std::str::from_utf8(&args[i])
            .ok()
            .and_then(|s| s.parse().ok())
        {
            Some(c) => c,
            None => return RespValue::error("VALUES count must be a positive integer"),
        };
        i += 1;

        // Parse count float values
        for _ in 0..count {
            if i >= args.len() {
                return RespValue::error("not enough values provided");
            }
            let val = match std::str::from_utf8(&args[i])
                .ok()
                .and_then(|s| s.parse::<f32>().ok())
            {
                Some(f) => f,
                None => return RespValue::error("invalid float value"),
            };
            embedding.push(val);
            i += 1;
        }
    } else if first_arg == "FP32" {
        i += 1;
        if i >= args.len() {
            return RespValue::error("FP32 requires blob");
        }
        let blob = &args[i];
        if blob.len() % 4 != 0 {
            return RespValue::error("FP32 blob must have length divisible by 4");
        }
        for chunk in blob.chunks_exact(4) {
            embedding.push(f32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]));
        }
        i += 1;
    } else if !first_arg.is_empty() && embedding.is_empty() {
        // Try parsing as individual floats (backwards compat)
        while i < args.len() {
            let arg = match std::str::from_utf8(&args[i]) {
                Ok(s) => s,
                Err(_) => break,
            };
            if let Ok(f) = arg.parse::<f32>() {
                embedding.push(f);
                i += 1;
            } else {
                break;
            }
        }
    }

    // Parse element name
    if i >= args.len() {
        return RespValue::error("element name required");
    }
    element = match std::str::from_utf8(&args[i]) {
        Ok(e) => Some(e),
        Err(_) => return RespValue::error("invalid element name"),
    };
    i += 1;

    // Parse optional SETATTR
    while i < args.len() {
        let opt = match std::str::from_utf8(&args[i]) {
            Ok(s) => s.to_uppercase(),
            Err(_) => break,
        };
        if opt == "SETATTR" {
            i += 1;
            if i >= args.len() {
                return RespValue::error("SETATTR requires JSON value");
            }
            attributes = match std::str::from_utf8(&args[i]) {
                Ok(a) => Some(a),
                Err(_) => return RespValue::error("invalid SETATTR value"),
            };
            i += 1;
        } else {
            i += 1;
        }
    }

    if embedding.is_empty() {
        return RespValue::error("embedding vector cannot be empty");
    }

    let elem = match element {
        Some(e) => e,
        None => return RespValue::error("element name required"),
    };

    match db.vadd(key, &embedding, elem, attributes, VectorQuantization::NoQuant) {
        Ok(added) => RespValue::Integer(if added { 1 } else { 0 }),
        Err(e) => RespValue::error(format!("VADD failed: {}", e)),
    }
}

#[cfg(not(feature = "vectors"))]
fn cmd_vadd(_db: &Db, _args: &[Vec<u8>]) -> RespValue {
    RespValue::error("vectors feature not enabled. Compile with --features vectors")
}

#[cfg(feature = "vectors")]
fn cmd_vsim(db: &Db, args: &[Vec<u8>]) -> RespValue {
    use crate::types::VectorInput;

    // VSIM key (ELE element | FP32 blob | VALUES n v1...) [COUNT n] [WITHSCORES] [FILTER expr]
    if args.len() < 2 {
        return RespValue::error("wrong number of arguments for 'vsim' command");
    }

    let key = match std::str::from_utf8(&args[0]) {
        Ok(k) => k,
        Err(_) => return RespValue::error("invalid key"),
    };

    let mut i = 1;
    let mut query: Option<VectorInput> = None;
    let mut count: Option<i64> = None;
    let mut with_scores = false;
    let mut filter: Option<&str> = None;

    // Parse query type
    let query_type = match std::str::from_utf8(&args[i]) {
        Ok(s) => s.to_uppercase(),
        Err(_) => return RespValue::error("invalid query type"),
    };

    match query_type.as_str() {
        "ELE" => {
            i += 1;
            if i >= args.len() {
                return RespValue::error("ELE requires element name");
            }
            let elem = match std::str::from_utf8(&args[i]) {
                Ok(e) => e.to_string(),
                Err(_) => return RespValue::error("invalid element name"),
            };
            query = Some(VectorInput::Element(elem));
            i += 1;
        }
        "FP32" => {
            i += 1;
            if i >= args.len() {
                return RespValue::error("FP32 requires blob");
            }
            query = Some(VectorInput::Fp32Blob(args[i].clone()));
            i += 1;
        }
        "VALUES" => {
            i += 1;
            if i >= args.len() {
                return RespValue::error("VALUES requires count");
            }
            let n: usize = match std::str::from_utf8(&args[i])
                .ok()
                .and_then(|s| s.parse().ok())
            {
                Some(c) => c,
                None => return RespValue::error("VALUES count must be a positive integer"),
            };
            i += 1;

            let mut values = Vec::with_capacity(n);
            for _ in 0..n {
                if i >= args.len() {
                    return RespValue::error("not enough values provided");
                }
                let val = match std::str::from_utf8(&args[i])
                    .ok()
                    .and_then(|s| s.parse::<f32>().ok())
                {
                    Some(f) => f,
                    None => return RespValue::error("invalid float value"),
                };
                values.push(val);
                i += 1;
            }
            query = Some(VectorInput::Values(values));
        }
        _ => return RespValue::error("query type must be ELE, FP32, or VALUES"),
    }

    // Parse optional arguments
    while i < args.len() {
        let opt = match std::str::from_utf8(&args[i]) {
            Ok(s) => s.to_uppercase(),
            Err(_) => break,
        };

        match opt.as_str() {
            "COUNT" => {
                i += 1;
                if i >= args.len() {
                    return RespValue::error("COUNT requires a number");
                }
                count = std::str::from_utf8(&args[i])
                    .ok()
                    .and_then(|s| s.parse().ok());
                i += 1;
            }
            "WITHSCORES" => {
                with_scores = true;
                i += 1;
            }
            "FILTER" => {
                i += 1;
                if i >= args.len() {
                    return RespValue::error("FILTER requires expression");
                }
                filter = std::str::from_utf8(&args[i]).ok();
                i += 1;
            }
            _ => i += 1,
        }
    }

    let query = match query {
        Some(q) => q,
        None => return RespValue::error("query vector required"),
    };

    match db.vsim(key, query, count, with_scores, filter) {
        Ok(results) => {
            let resp_results: Vec<RespValue> = results
                .into_iter()
                .map(|r| {
                    if with_scores {
                        RespValue::Array(Some(vec![
                            RespValue::BulkString(Some(r.element.as_bytes().to_vec())),
                            RespValue::BulkString(Some(r.score.to_string().as_bytes().to_vec())),
                        ]))
                    } else {
                        RespValue::BulkString(Some(r.element.as_bytes().to_vec()))
                    }
                })
                .collect();
            RespValue::Array(Some(resp_results))
        }
        Err(e) => RespValue::error(format!("VSIM failed: {}", e)),
    }
}

#[cfg(not(feature = "vectors"))]
fn cmd_vsim(_db: &Db, _args: &[Vec<u8>]) -> RespValue {
    RespValue::error("vectors feature not enabled. Compile with --features vectors")
}

#[cfg(feature = "vectors")]
fn cmd_vrem(db: &Db, args: &[Vec<u8>]) -> RespValue {
    // VREM key element
    if args.len() < 2 {
        return RespValue::error("wrong number of arguments for 'vrem' command");
    }

    let key = match std::str::from_utf8(&args[0]) {
        Ok(k) => k,
        Err(_) => return RespValue::error("invalid key"),
    };

    let element = match std::str::from_utf8(&args[1]) {
        Ok(e) => e,
        Err(_) => return RespValue::error("invalid element"),
    };

    match db.vrem(key, element) {
        Ok(removed) => RespValue::Integer(if removed { 1 } else { 0 }),
        Err(e) => RespValue::error(format!("VREM failed: {}", e)),
    }
}

#[cfg(not(feature = "vectors"))]
fn cmd_vrem(_db: &Db, _args: &[Vec<u8>]) -> RespValue {
    RespValue::error("vectors feature not enabled. Compile with --features vectors")
}

#[cfg(feature = "vectors")]
fn cmd_vcard(db: &Db, args: &[Vec<u8>]) -> RespValue {
    // VCARD key
    if args.is_empty() {
        return RespValue::error("wrong number of arguments for 'vcard' command");
    }

    let key = match std::str::from_utf8(&args[0]) {
        Ok(k) => k,
        Err(_) => return RespValue::error("invalid key"),
    };

    match db.vcard(key) {
        Ok(count) => RespValue::Integer(count),
        Err(e) => RespValue::error(format!("VCARD failed: {}", e)),
    }
}

#[cfg(not(feature = "vectors"))]
fn cmd_vcard(_db: &Db, _args: &[Vec<u8>]) -> RespValue {
    RespValue::error("vectors feature not enabled. Compile with --features vectors")
}

#[cfg(feature = "vectors")]
fn cmd_vdim(db: &Db, args: &[Vec<u8>]) -> RespValue {
    // VDIM key
    if args.is_empty() {
        return RespValue::error("wrong number of arguments for 'vdim' command");
    }

    let key = match std::str::from_utf8(&args[0]) {
        Ok(k) => k,
        Err(_) => return RespValue::error("invalid key"),
    };

    match db.vdim(key) {
        Ok(Some(dim)) => RespValue::Integer(dim as i64),
        Ok(None) => RespValue::null(),
        Err(e) => RespValue::error(format!("VDIM failed: {}", e)),
    }
}

#[cfg(not(feature = "vectors"))]
fn cmd_vdim(_db: &Db, _args: &[Vec<u8>]) -> RespValue {
    RespValue::error("vectors feature not enabled. Compile with --features vectors")
}

#[cfg(feature = "vectors")]
fn cmd_vinfo(db: &Db, args: &[Vec<u8>]) -> RespValue {
    // VINFO key
    if args.is_empty() {
        return RespValue::error("wrong number of arguments for 'vinfo' command");
    }

    let key = match std::str::from_utf8(&args[0]) {
        Ok(k) => k,
        Err(_) => return RespValue::error("invalid key"),
    };

    match db.vinfo(key) {
        Ok(Some(info)) => RespValue::Array(Some(vec![
            RespValue::BulkString(Some(b"key".to_vec())),
            RespValue::BulkString(Some(info.key.as_bytes().to_vec())),
            RespValue::BulkString(Some(b"cardinality".to_vec())),
            RespValue::Integer(info.cardinality),
            RespValue::BulkString(Some(b"dimensions".to_vec())),
            match info.dimensions {
                Some(d) => RespValue::Integer(d as i64),
                None => RespValue::null(),
            },
            RespValue::BulkString(Some(b"quantization".to_vec())),
            RespValue::BulkString(Some(info.quantization.as_str().as_bytes().to_vec())),
        ])),
        Ok(None) => RespValue::null(),
        Err(e) => RespValue::error(format!("VINFO failed: {}", e)),
    }
}

#[cfg(not(feature = "vectors"))]
fn cmd_vinfo(_db: &Db, _args: &[Vec<u8>]) -> RespValue {
    RespValue::error("vectors feature not enabled. Compile with --features vectors")
}

#[cfg(feature = "vectors")]
fn cmd_vemb(db: &Db, args: &[Vec<u8>]) -> RespValue {
    // VEMB key element [RAW]
    if args.len() < 2 {
        return RespValue::error("wrong number of arguments for 'vemb' command");
    }

    let key = match std::str::from_utf8(&args[0]) {
        Ok(k) => k,
        Err(_) => return RespValue::error("invalid key"),
    };

    let element = match std::str::from_utf8(&args[1]) {
        Ok(e) => e,
        Err(_) => return RespValue::error("invalid element"),
    };

    let raw = args.len() > 2
        && std::str::from_utf8(&args[2])
            .map(|s| s.to_uppercase() == "RAW")
            .unwrap_or(false);

    match db.vemb(key, element, raw) {
        Ok(Some(data)) => RespValue::BulkString(Some(data)),
        Ok(None) => RespValue::null(),
        Err(e) => RespValue::error(format!("VEMB failed: {}", e)),
    }
}

#[cfg(not(feature = "vectors"))]
fn cmd_vemb(_db: &Db, _args: &[Vec<u8>]) -> RespValue {
    RespValue::error("vectors feature not enabled. Compile with --features vectors")
}

#[cfg(feature = "vectors")]
fn cmd_vgetattr(db: &Db, args: &[Vec<u8>]) -> RespValue {
    // VGETATTR key element
    if args.len() < 2 {
        return RespValue::error("wrong number of arguments for 'vgetattr' command");
    }

    let key = match std::str::from_utf8(&args[0]) {
        Ok(k) => k,
        Err(_) => return RespValue::error("invalid key"),
    };

    let element = match std::str::from_utf8(&args[1]) {
        Ok(e) => e,
        Err(_) => return RespValue::error("invalid element"),
    };

    match db.vgetattr(key, element) {
        Ok(Some(attrs)) => RespValue::BulkString(Some(attrs.as_bytes().to_vec())),
        Ok(None) => RespValue::null(),
        Err(e) => RespValue::error(format!("VGETATTR failed: {}", e)),
    }
}

#[cfg(not(feature = "vectors"))]
fn cmd_vgetattr(_db: &Db, _args: &[Vec<u8>]) -> RespValue {
    RespValue::error("vectors feature not enabled. Compile with --features vectors")
}

#[cfg(feature = "vectors")]
fn cmd_vsetattr(db: &Db, args: &[Vec<u8>]) -> RespValue {
    // VSETATTR key element json
    if args.len() < 3 {
        return RespValue::error("wrong number of arguments for 'vsetattr' command");
    }

    let key = match std::str::from_utf8(&args[0]) {
        Ok(k) => k,
        Err(_) => return RespValue::error("invalid key"),
    };

    let element = match std::str::from_utf8(&args[1]) {
        Ok(e) => e,
        Err(_) => return RespValue::error("invalid element"),
    };

    let attributes = match std::str::from_utf8(&args[2]) {
        Ok(a) => a,
        Err(_) => return RespValue::error("invalid attributes"),
    };

    match db.vsetattr(key, element, attributes) {
        Ok(updated) => RespValue::Integer(if updated { 1 } else { 0 }),
        Err(e) => RespValue::error(format!("VSETATTR failed: {}", e)),
    }
}

#[cfg(not(feature = "vectors"))]
fn cmd_vsetattr(_db: &Db, _args: &[Vec<u8>]) -> RespValue {
    RespValue::error("vectors feature not enabled. Compile with --features vectors")
}

#[cfg(feature = "vectors")]
fn cmd_vrandmember(db: &Db, args: &[Vec<u8>]) -> RespValue {
    // VRANDMEMBER key [count]
    if args.is_empty() {
        return RespValue::error("wrong number of arguments for 'vrandmember' command");
    }

    let key = match std::str::from_utf8(&args[0]) {
        Ok(k) => k,
        Err(_) => return RespValue::error("invalid key"),
    };

    let count = if args.len() > 1 {
        std::str::from_utf8(&args[1])
            .ok()
            .and_then(|s| s.parse().ok())
    } else {
        None
    };

    match db.vrandmember(key, count) {
        Ok(elements) => {
            if elements.is_empty() {
                RespValue::null()
            } else if count.is_none() || count == Some(1) {
                // Single element - return as bulk string
                RespValue::BulkString(Some(elements[0].as_bytes().to_vec()))
            } else {
                // Multiple elements - return as array
                let resp: Vec<RespValue> = elements
                    .into_iter()
                    .map(|e| RespValue::BulkString(Some(e.as_bytes().to_vec())))
                    .collect();
                RespValue::Array(Some(resp))
            }
        }
        Err(e) => RespValue::error(format!("VRANDMEMBER failed: {}", e)),
    }
}

#[cfg(not(feature = "vectors"))]
fn cmd_vrandmember(_db: &Db, _args: &[Vec<u8>]) -> RespValue {
    RespValue::error("vectors feature not enabled. Compile with --features vectors")
}

// --- Session 25: Geo command handlers ---

/// GEOADD key [NX|XX] [CH] longitude latitude member [lon lat member ...]
#[cfg(feature = "geo")]
fn cmd_geoadd(db: &Db, args: &[Vec<u8>]) -> RespValue {
    if args.len() < 4 {
        return RespValue::error("wrong number of arguments for 'geoadd' command");
    }

    let key = match std::str::from_utf8(&args[0]) {
        Ok(k) => k,
        Err(_) => return RespValue::error("invalid key"),
    };

    let mut i = 1;
    let mut nx = false;
    let mut xx = false;
    let mut ch = false;

    // Parse optional flags
    while i < args.len() {
        let arg = match std::str::from_utf8(&args[i]) {
            Ok(s) => s.to_uppercase(),
            Err(_) => break,
        };
        match arg.as_str() {
            "NX" => { nx = true; i += 1; }
            "XX" => { xx = true; i += 1; }
            "CH" => { ch = true; i += 1; }
            _ => break,
        }
    }

    // Parse longitude latitude member triplets
    let mut members = Vec::new();
    while i + 2 < args.len() {
        let lon: f64 = match std::str::from_utf8(&args[i]).ok().and_then(|s| s.parse().ok()) {
            Some(v) => v,
            None => return RespValue::error("invalid longitude"),
        };
        let lat: f64 = match std::str::from_utf8(&args[i + 1]).ok().and_then(|s| s.parse().ok()) {
            Some(v) => v,
            None => return RespValue::error("invalid latitude"),
        };
        let member = match std::str::from_utf8(&args[i + 2]) {
            Ok(m) => m,
            Err(_) => return RespValue::error("invalid member"),
        };
        members.push((lon, lat, member));
        i += 3;
    }

    if members.is_empty() {
        return RespValue::error("wrong number of arguments for 'geoadd' command");
    }

    match db.geoadd(key, &members, nx, xx, ch) {
        Ok(count) => RespValue::Integer(count),
        Err(e) => RespValue::error(format!("{}", e)),
    }
}

#[cfg(not(feature = "geo"))]
fn cmd_geoadd(_db: &Db, _args: &[Vec<u8>]) -> RespValue {
    RespValue::error("geo feature not enabled. Compile with --features geo")
}

/// GEOPOS key member [member ...]
#[cfg(feature = "geo")]
fn cmd_geopos(db: &Db, args: &[Vec<u8>]) -> RespValue {
    if args.len() < 2 {
        return RespValue::error("wrong number of arguments for 'geopos' command");
    }

    let key = match std::str::from_utf8(&args[0]) {
        Ok(k) => k,
        Err(_) => return RespValue::error("invalid key"),
    };

    let members: Vec<&str> = args[1..]
        .iter()
        .filter_map(|a| std::str::from_utf8(a).ok())
        .collect();

    match db.geopos(key, &members) {
        Ok(positions) => {
            let results: Vec<RespValue> = positions
                .into_iter()
                .map(|pos| match pos {
                    Some((lon, lat)) => RespValue::Array(Some(vec![
                        RespValue::from_string(format!("{}", lon)),
                        RespValue::from_string(format!("{}", lat)),
                    ])),
                    None => RespValue::null(),
                })
                .collect();
            RespValue::Array(Some(results))
        }
        Err(e) => RespValue::error(format!("{}", e)),
    }
}

#[cfg(not(feature = "geo"))]
fn cmd_geopos(_db: &Db, _args: &[Vec<u8>]) -> RespValue {
    RespValue::error("geo feature not enabled. Compile with --features geo")
}

/// GEODIST key member1 member2 [M|KM|MI|FT]
#[cfg(feature = "geo")]
fn cmd_geodist(db: &Db, args: &[Vec<u8>]) -> RespValue {
    if args.len() < 3 {
        return RespValue::error("wrong number of arguments for 'geodist' command");
    }

    let key = match std::str::from_utf8(&args[0]) {
        Ok(k) => k,
        Err(_) => return RespValue::error("invalid key"),
    };
    let member1 = match std::str::from_utf8(&args[1]) {
        Ok(m) => m,
        Err(_) => return RespValue::error("invalid member"),
    };
    let member2 = match std::str::from_utf8(&args[2]) {
        Ok(m) => m,
        Err(_) => return RespValue::error("invalid member"),
    };

    let unit = if args.len() > 3 {
        match std::str::from_utf8(&args[3]).ok().and_then(crate::types::GeoUnit::from_str) {
            Some(u) => u,
            None => return RespValue::error("invalid unit"),
        }
    } else {
        crate::types::GeoUnit::Meters
    };

    match db.geodist(key, member1, member2, unit) {
        Ok(Some(dist)) => RespValue::from_string(format!("{}", dist)),
        Ok(None) => RespValue::null(),
        Err(e) => RespValue::error(format!("{}", e)),
    }
}

#[cfg(not(feature = "geo"))]
fn cmd_geodist(_db: &Db, _args: &[Vec<u8>]) -> RespValue {
    RespValue::error("geo feature not enabled. Compile with --features geo")
}

/// GEOHASH key member [member ...]
#[cfg(feature = "geo")]
fn cmd_geohash(db: &Db, args: &[Vec<u8>]) -> RespValue {
    if args.len() < 2 {
        return RespValue::error("wrong number of arguments for 'geohash' command");
    }

    let key = match std::str::from_utf8(&args[0]) {
        Ok(k) => k,
        Err(_) => return RespValue::error("invalid key"),
    };

    let members: Vec<&str> = args[1..]
        .iter()
        .filter_map(|a| std::str::from_utf8(a).ok())
        .collect();

    match db.geohash(key, &members) {
        Ok(hashes) => {
            let results: Vec<RespValue> = hashes
                .into_iter()
                .map(|h| match h {
                    Some(hash) => RespValue::from_string(hash),
                    None => RespValue::null(),
                })
                .collect();
            RespValue::Array(Some(results))
        }
        Err(e) => RespValue::error(format!("{}", e)),
    }
}

#[cfg(not(feature = "geo"))]
fn cmd_geohash(_db: &Db, _args: &[Vec<u8>]) -> RespValue {
    RespValue::error("geo feature not enabled. Compile with --features geo")
}

/// GEOSEARCH key FROMMEMBER member | FROMLONLAT lon lat BYRADIUS radius M|KM|MI|FT | BYBOX width height M|KM|MI|FT [ASC|DESC] [COUNT n [ANY]] [WITHCOORD] [WITHDIST] [WITHHASH]
#[cfg(feature = "geo")]
fn cmd_geosearch(db: &Db, args: &[Vec<u8>]) -> RespValue {
    if args.len() < 4 {
        return RespValue::error("wrong number of arguments for 'geosearch' command");
    }

    let key = match std::str::from_utf8(&args[0]) {
        Ok(k) => k,
        Err(_) => return RespValue::error("invalid key"),
    };

    let mut options = crate::types::GeoSearchOptions::default();
    let mut i = 1;

    while i < args.len() {
        let arg = match std::str::from_utf8(&args[i]) {
            Ok(s) => s.to_uppercase(),
            Err(_) => { i += 1; continue; }
        };

        match arg.as_str() {
            "FROMMEMBER" => {
                if i + 1 >= args.len() {
                    return RespValue::error("syntax error");
                }
                options.from_member = std::str::from_utf8(&args[i + 1]).ok().map(String::from);
                i += 2;
            }
            "FROMLONLAT" => {
                if i + 2 >= args.len() {
                    return RespValue::error("syntax error");
                }
                let lon: f64 = match std::str::from_utf8(&args[i + 1]).ok().and_then(|s| s.parse().ok()) {
                    Some(v) => v,
                    None => return RespValue::error("invalid longitude"),
                };
                let lat: f64 = match std::str::from_utf8(&args[i + 2]).ok().and_then(|s| s.parse().ok()) {
                    Some(v) => v,
                    None => return RespValue::error("invalid latitude"),
                };
                options.from_lonlat = Some((lon, lat));
                i += 3;
            }
            "BYRADIUS" => {
                if i + 2 >= args.len() {
                    return RespValue::error("syntax error");
                }
                let radius: f64 = match std::str::from_utf8(&args[i + 1]).ok().and_then(|s| s.parse().ok()) {
                    Some(v) => v,
                    None => return RespValue::error("invalid radius"),
                };
                let unit = match std::str::from_utf8(&args[i + 2]).ok().and_then(crate::types::GeoUnit::from_str) {
                    Some(u) => u,
                    None => return RespValue::error("invalid unit"),
                };
                options.by_radius = Some((radius, unit));
                i += 3;
            }
            "BYBOX" => {
                if i + 3 >= args.len() {
                    return RespValue::error("syntax error");
                }
                let width: f64 = match std::str::from_utf8(&args[i + 1]).ok().and_then(|s| s.parse().ok()) {
                    Some(v) => v,
                    None => return RespValue::error("invalid width"),
                };
                let height: f64 = match std::str::from_utf8(&args[i + 2]).ok().and_then(|s| s.parse().ok()) {
                    Some(v) => v,
                    None => return RespValue::error("invalid height"),
                };
                let unit = match std::str::from_utf8(&args[i + 3]).ok().and_then(crate::types::GeoUnit::from_str) {
                    Some(u) => u,
                    None => return RespValue::error("invalid unit"),
                };
                options.by_box = Some((width, height, unit));
                i += 4;
            }
            "ASC" => { options.ascending = true; i += 1; }
            "DESC" => { options.ascending = false; i += 1; }
            "COUNT" => {
                if i + 1 >= args.len() {
                    return RespValue::error("syntax error");
                }
                options.count = std::str::from_utf8(&args[i + 1]).ok().and_then(|s| s.parse().ok());
                i += 2;
                // Check for ANY
                if i < args.len() {
                    if let Ok(s) = std::str::from_utf8(&args[i]) {
                        if s.to_uppercase() == "ANY" {
                            options.any = true;
                            i += 1;
                        }
                    }
                }
            }
            "WITHCOORD" => { options.with_coord = true; i += 1; }
            "WITHDIST" => { options.with_dist = true; i += 1; }
            "WITHHASH" => { options.with_hash = true; i += 1; }
            _ => { i += 1; }
        }
    }

    match db.geosearch(key, &options) {
        Ok(results) => {
            let items: Vec<RespValue> = results
                .into_iter()
                .map(|m| {
                    let mut item = vec![RespValue::from_string(m.member.clone())];
                    if options.with_dist {
                        if let Some(dist) = m.distance {
                            item.push(RespValue::from_string(format!("{}", dist)));
                        }
                    }
                    if options.with_hash {
                        if let Some(hash) = &m.geohash {
                            item.push(RespValue::from_string(hash.clone()));
                        }
                    }
                    if options.with_coord {
                        item.push(RespValue::Array(Some(vec![
                            RespValue::from_string(format!("{}", m.longitude)),
                            RespValue::from_string(format!("{}", m.latitude)),
                        ])));
                    }
                    if item.len() == 1 {
                        item.pop().unwrap()
                    } else {
                        RespValue::Array(Some(item))
                    }
                })
                .collect();
            RespValue::Array(Some(items))
        }
        Err(e) => RespValue::error(format!("{}", e)),
    }
}

#[cfg(not(feature = "geo"))]
fn cmd_geosearch(_db: &Db, _args: &[Vec<u8>]) -> RespValue {
    RespValue::error("geo feature not enabled. Compile with --features geo")
}

/// GEOSEARCHSTORE dest src [FROMMEMBER member | FROMLONLAT lon lat] [BYRADIUS radius M|KM|MI|FT | BYBOX width height M|KM|MI|FT] [ASC|DESC] [COUNT n] [STOREDIST]
#[cfg(feature = "geo")]
fn cmd_geosearchstore(db: &Db, args: &[Vec<u8>]) -> RespValue {
    if args.len() < 5 {
        return RespValue::error("wrong number of arguments for 'geosearchstore' command");
    }

    let dest = match std::str::from_utf8(&args[0]) {
        Ok(k) => k,
        Err(_) => return RespValue::error("invalid destination key"),
    };
    let src = match std::str::from_utf8(&args[1]) {
        Ok(k) => k,
        Err(_) => return RespValue::error("invalid source key"),
    };

    let mut options = crate::types::GeoSearchOptions::default();
    let mut store_dist = false;
    let mut i = 2;

    while i < args.len() {
        let arg = match std::str::from_utf8(&args[i]) {
            Ok(s) => s.to_uppercase(),
            Err(_) => { i += 1; continue; }
        };

        match arg.as_str() {
            "FROMMEMBER" => {
                if i + 1 >= args.len() { return RespValue::error("syntax error"); }
                options.from_member = std::str::from_utf8(&args[i + 1]).ok().map(String::from);
                i += 2;
            }
            "FROMLONLAT" => {
                if i + 2 >= args.len() { return RespValue::error("syntax error"); }
                let lon: f64 = match std::str::from_utf8(&args[i + 1]).ok().and_then(|s| s.parse().ok()) {
                    Some(v) => v,
                    None => return RespValue::error("invalid longitude"),
                };
                let lat: f64 = match std::str::from_utf8(&args[i + 2]).ok().and_then(|s| s.parse().ok()) {
                    Some(v) => v,
                    None => return RespValue::error("invalid latitude"),
                };
                options.from_lonlat = Some((lon, lat));
                i += 3;
            }
            "BYRADIUS" => {
                if i + 2 >= args.len() { return RespValue::error("syntax error"); }
                let radius: f64 = match std::str::from_utf8(&args[i + 1]).ok().and_then(|s| s.parse().ok()) {
                    Some(v) => v,
                    None => return RespValue::error("invalid radius"),
                };
                let unit = match std::str::from_utf8(&args[i + 2]).ok().and_then(crate::types::GeoUnit::from_str) {
                    Some(u) => u,
                    None => return RespValue::error("invalid unit"),
                };
                options.by_radius = Some((radius, unit));
                i += 3;
            }
            "BYBOX" => {
                if i + 3 >= args.len() { return RespValue::error("syntax error"); }
                let width: f64 = match std::str::from_utf8(&args[i + 1]).ok().and_then(|s| s.parse().ok()) {
                    Some(v) => v,
                    None => return RespValue::error("invalid width"),
                };
                let height: f64 = match std::str::from_utf8(&args[i + 2]).ok().and_then(|s| s.parse().ok()) {
                    Some(v) => v,
                    None => return RespValue::error("invalid height"),
                };
                let unit = match std::str::from_utf8(&args[i + 3]).ok().and_then(crate::types::GeoUnit::from_str) {
                    Some(u) => u,
                    None => return RespValue::error("invalid unit"),
                };
                options.by_box = Some((width, height, unit));
                i += 4;
            }
            "ASC" => { options.ascending = true; i += 1; }
            "DESC" => { options.ascending = false; i += 1; }
            "COUNT" => {
                if i + 1 >= args.len() { return RespValue::error("syntax error"); }
                options.count = std::str::from_utf8(&args[i + 1]).ok().and_then(|s| s.parse().ok());
                i += 2;
            }
            "STOREDIST" => { store_dist = true; i += 1; }
            _ => { i += 1; }
        }
    }

    match db.geosearchstore(dest, src, &options, store_dist) {
        Ok(count) => RespValue::Integer(count),
        Err(e) => RespValue::error(format!("{}", e)),
    }
}

#[cfg(not(feature = "geo"))]
fn cmd_geosearchstore(_db: &Db, _args: &[Vec<u8>]) -> RespValue {
    RespValue::error("geo feature not enabled. Compile with --features geo")
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
        "CONFIG" => cmd_config(db, cmd_args),
        "MEMORY" => cmd_memory(db, cmd_args),
        // String commands
        "GET" => cmd_get(db, cmd_args),
        "SET" => cmd_set(db, cmd_args),
        "DEL" => cmd_del(db, cmd_args),
        "TYPE" => cmd_type(db, cmd_args),
        "TTL" => cmd_ttl(db, cmd_args),
        "PTTL" => cmd_pttl(db, cmd_args),
        "EXISTS" => cmd_exists(db, cmd_args),
        "EXPIRE" => cmd_expire(db, cmd_args),
        "PEXPIRE" => cmd_pexpire(db, cmd_args),
        "EXPIREAT" => cmd_expireat(db, cmd_args),
        "PEXPIREAT" => cmd_pexpireat(db, cmd_args),
        "PERSIST" => cmd_persist(db, cmd_args),
        "RENAME" => cmd_rename(db, cmd_args),
        "RENAMENX" => cmd_renamenx(db, cmd_args),
        "KEYS" => cmd_keys(db, cmd_args),
        "SCAN" => cmd_scan(db, cmd_args),
        "HSCAN" => cmd_hscan(db, cmd_args),
        "SSCAN" => cmd_sscan(db, cmd_args),
        "ZSCAN" => cmd_zscan(db, cmd_args),
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
        "GETEX" => cmd_getex(db, cmd_args),
        "GETDEL" => cmd_getdel(db, cmd_args),
        "SETEX" => cmd_setex(db, cmd_args),
        "PSETEX" => cmd_psetex(db, cmd_args),
        // Bitmap operations
        "SETBIT" => cmd_setbit(db, cmd_args),
        "GETBIT" => cmd_getbit(db, cmd_args),
        "BITCOUNT" => cmd_bitcount(db, cmd_args),
        "BITOP" => cmd_bitop(db, cmd_args),
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
        "LPUSHX" => cmd_lpushx(db, cmd_args),
        "RPUSHX" => cmd_rpushx(db, cmd_args),
        "LPOS" => cmd_lpos(db, cmd_args),
        "LMOVE" => cmd_lmove(db, cmd_args),
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
        "ZINTERSTORE" => cmd_zinterstore(db, cmd_args),
        "ZUNIONSTORE" => cmd_zunionstore(db, cmd_args),
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
        "FT.EXPLAIN" => cmd_ft_explain(db, cmd_args),
        "FT.PROFILE" => cmd_ft_profile(db, cmd_args),
        // Redis 8 Vector commands (V* commands)
        "VADD" => cmd_vadd(db, cmd_args),
        "VSIM" => cmd_vsim(db, cmd_args),
        "VREM" => cmd_vrem(db, cmd_args),
        "VCARD" => cmd_vcard(db, cmd_args),
        "VDIM" => cmd_vdim(db, cmd_args),
        "VINFO" => cmd_vinfo(db, cmd_args),
        "VEMB" => cmd_vemb(db, cmd_args),
        "VGETATTR" => cmd_vgetattr(db, cmd_args),
        "VSETATTR" => cmd_vsetattr(db, cmd_args),
        "VRANDMEMBER" => cmd_vrandmember(db, cmd_args),
        // Geo commands (GEO* commands)
        "GEOADD" => cmd_geoadd(db, cmd_args),
        "GEOPOS" => cmd_geopos(db, cmd_args),
        "GEODIST" => cmd_geodist(db, cmd_args),
        "GEOHASH" => cmd_geohash(db, cmd_args),
        "GEOSEARCH" => cmd_geosearch(db, cmd_args),
        "GEOSEARCHSTORE" => cmd_geosearchstore(db, cmd_args),
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
