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
