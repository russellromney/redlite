use anyhow::Result;
use clap::{Parser, Subcommand};

mod client;
mod properties;
mod report;
mod runner;
mod types;

use runner::TestRunner;

#[derive(Parser)]
#[command(name = "redlite-dst")]
#[command(about = "Deterministic Simulation Testing for Redlite")]
#[command(version)]
#[command(
    long_about = "A comprehensive testing suite for finding bugs that would take months to surface in production.\n\n\
    Inspired by TigerBeetle VOPR, sled simulation, and MadSim.\n\
    Every failure is reproducible with a seed."
)]
struct Cli {
    #[command(subcommand)]
    command: Commands,

    /// Output format (console, json, markdown)
    #[arg(long, global = true, default_value = "console")]
    format: String,

    /// Output file (optional, defaults to stdout)
    #[arg(long, short, global = true)]
    output: Option<String>,

    /// Verbose output
    #[arg(long, short, global = true)]
    verbose: bool,
}

#[derive(Subcommand)]
enum Commands {
    /// Quick sanity check (<1 min)
    Smoke,

    /// Property-based tests with proptest
    Properties {
        /// Number of seeds to test
        #[arg(long, short, default_value = "1000")]
        seeds: u64,

        /// Specific properties to test (comma-separated)
        #[arg(long, short)]
        filter: Option<String>,
    },

    /// Compare against Redis for compatibility
    Oracle {
        /// Redis host:port
        #[arg(long, default_value = "localhost:6379")]
        redis: String,

        /// Number of operations per test
        #[arg(long, short, default_value = "1000")]
        ops: usize,
    },

    /// Deterministic simulation with MadSim
    Simulate {
        /// Number of seeds to test
        #[arg(long, short, default_value = "1000")]
        seeds: u64,

        /// Operations per seed
        #[arg(long, default_value = "10000")]
        ops: usize,
    },

    /// Fault injection tests
    Chaos {
        /// Faults to inject (comma-separated: disk_full,corrupt_read,slow_write)
        #[arg(long, short, default_value = "disk_full,corrupt_read,slow_write")]
        faults: String,

        /// Number of seeds
        #[arg(long, short, default_value = "100")]
        seeds: u64,
    },

    /// Scale testing
    Stress {
        /// Number of concurrent connections
        #[arg(long, short, default_value = "100")]
        connections: usize,

        /// Number of keys
        #[arg(long, short, default_value = "100000")]
        keys: usize,
    },

    /// Fuzzing harness
    Fuzz {
        /// Target to fuzz (resp_parser, query_parser)
        #[arg(long, short)]
        target: String,

        /// Duration in seconds
        #[arg(long, short, default_value = "60")]
        duration: u64,
    },

    /// Long-running stability test
    Soak {
        /// Duration (e.g., "1h", "24h")
        #[arg(long, short, default_value = "1h")]
        duration: String,

        /// Check interval in seconds
        #[arg(long, default_value = "60")]
        interval: u64,
    },

    /// Parallel execution on fly.io
    Cloud {
        /// Number of seeds to test
        #[arg(long, short, default_value = "100000")]
        seeds: u64,

        /// Number of machines
        #[arg(long, short, default_value = "10")]
        machines: usize,
    },

    /// Reproduce a specific failure
    Replay {
        /// Seed to replay
        #[arg(long, short)]
        seed: u64,

        /// Test type that failed
        #[arg(long, short)]
        test: String,
    },

    /// Run all tests
    Full {
        /// Skip slow tests
        #[arg(long)]
        quick: bool,
    },

    /// Manage regression seeds
    Seeds {
        #[command(subcommand)]
        action: SeedsAction,
    },
}

#[derive(Subcommand)]
enum SeedsAction {
    /// List known regression seeds
    List,
    /// Add a new regression seed
    Add {
        #[arg(long, short)]
        seed: u64,
        #[arg(long, short)]
        description: String,
    },
    /// Test all regression seeds
    Test,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    let runner = TestRunner::new(cli.verbose);

    match cli.command {
        Commands::Smoke => {
            runner.smoke().await?;
        }

        Commands::Properties { seeds, filter } => {
            runner.properties(seeds, filter).await?;
        }

        Commands::Oracle { redis, ops } => {
            runner.oracle(&redis, ops).await?;
        }

        Commands::Simulate { seeds, ops } => {
            runner.simulate(seeds, ops).await?;
        }

        Commands::Chaos { faults, seeds } => {
            let faults: Vec<&str> = faults.split(',').collect();
            runner.chaos(&faults, seeds).await?;
        }

        Commands::Stress { connections, keys } => {
            runner.stress(connections, keys).await?;
        }

        Commands::Fuzz { target, duration } => {
            runner.fuzz(&target, duration).await?;
        }

        Commands::Soak { duration, interval } => {
            runner.soak(&duration, interval).await?;
        }

        Commands::Cloud { seeds, machines } => {
            runner.cloud(seeds, machines).await?;
        }

        Commands::Replay { seed, test } => {
            runner.replay(seed, &test).await?;
        }

        Commands::Full { quick } => {
            runner.full(quick).await?;
        }

        Commands::Seeds { action } => match action {
            SeedsAction::List => runner.seeds_list().await?,
            SeedsAction::Add { seed, description } => {
                runner.seeds_add(seed, &description).await?
            }
            SeedsAction::Test => runner.seeds_test().await?,
        },
    }

    Ok(())
}
