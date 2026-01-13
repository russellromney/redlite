use clap::Parser;

mod db;
mod error;
mod resp;
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

    /// Require password for connections (like Redis requirepass)
    #[arg(long)]
    password: Option<String>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let args = Args::parse();

    let db = Db::open(&args.db)?;
    tracing::info!("Opened database: {}", args.db);

    if args.password.is_some() {
        tracing::info!("Authentication enabled");
    }

    let server = Server::new(db, args.password);
    server.run(&args.addr).await?;

    Ok(())
}
