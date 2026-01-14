use clap::Parser;

mod db;
mod error;
mod resp;
mod search;
mod server;
mod types;

use db::Db;
use server::Server;

#[cfg(feature = "turso")]
mod turso_db;
#[cfg(feature = "turso")]
use turso_db::TursoDb;

#[derive(Parser)]
#[command(name = "redlite")]
#[command(about = "SQLite-backed Redis-compatible KV store")]
struct Args {
    /// Database file path (ignored if --storage=memory)
    #[arg(short, long, default_value = "redlite.db")]
    db: String,

    /// Listen address
    #[arg(short, long, default_value = "127.0.0.1:6379")]
    addr: String,

    /// Require password for connections (like Redis requirepass)
    #[arg(long)]
    password: Option<String>,

    /// Backend type: sqlite or turso
    #[arg(long, default_value = "sqlite")]
    backend: String,

    /// Storage type: file or memory
    #[arg(long, default_value = "file")]
    storage: String,

    /// Cache size in MB (default: 64). Larger = faster reads.
    /// Set to available RAM for best performance (e.g., 1024 for 1GB).
    #[arg(long, default_value = "64")]
    cache: i64,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let args = Args::parse();

    let backend = args.backend.to_lowercase();
    let storage = args.storage.to_lowercase();

    match backend.as_str() {
        "sqlite" => {
            let db = match storage.as_str() {
                "memory" => {
                    tracing::info!("Using SQLite in-memory database (cache: {}MB)", args.cache);
                    let db = Db::open_memory()?;
                    db.set_cache_mb(args.cache)?;
                    db
                }
                "file" => {
                    tracing::info!(
                        "Using SQLite file database: {} (cache: {}MB)",
                        args.db,
                        args.cache
                    );
                    Db::open_with_cache(&args.db, args.cache)?
                }
                _ => {
                    anyhow::bail!("Invalid storage type: {}. Use 'file' or 'memory'", storage);
                }
            };

            if args.password.is_some() {
                tracing::info!("Authentication enabled");
            }

            let server = Server::new(db, args.password);
            server.run(&args.addr).await?;
        }
        #[cfg(feature = "turso")]
        "turso" => {
            // TursoDb currently only supports a minimal subset of Redis commands
            // and doesn't implement the full interface needed for server mode
            // (missing: session(), with_notifier(), select(), keys(), expire(), etc.)
            // Use --backend=sqlite for full server functionality.
            anyhow::bail!(
                "Turso backend is not yet supported in server mode. \
                TursoDb implements a minimal Redis subset for embedded/benchmark use only. \
                Use --backend=sqlite for full server functionality."
            );
        }
        #[cfg(not(feature = "turso"))]
        "turso" => {
            anyhow::bail!("Turso backend not available. Rebuild with --features turso");
        }
        _ => {
            anyhow::bail!("Invalid backend type: {}. Use 'sqlite' or 'turso'", backend);
        }
    }

    Ok(())
}
