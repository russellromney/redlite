---
title: Configuration
description: Configuration options for Redlite
---

Redlite is designed for zero-configuration operation. However, there are a few options available.

## Server Configuration

### Command Line Arguments

| Argument | Short | Default | Description |
|----------|-------|---------|-------------|
| `--db` | `-d` | `redlite.db` | Database file path |
| `--addr` | `-a` | `127.0.0.1:6379` | Listen address and port |
| `--password` | | (none) | Require password for connections (like Redis `requirepass`) |
| `--storage` | | `file` | Storage type: `file` or `memory` |
| `--backend` | | `sqlite` | Backend type: `sqlite` or `turso` |
| `--cache` | | `64` | SQLite page cache size in MB (larger = faster reads) |

### Database Path

```bash
# Persistent file
./redlite --db /var/lib/redlite/data.db

# In-memory (no persistence)
./redlite --storage memory
```

### Network Binding

```bash
# Localhost only (default, secure)
./redlite --addr 127.0.0.1:6379

# All interfaces (for remote access)
./redlite --addr 0.0.0.0:6379

# Custom port
./redlite --addr 127.0.0.1:6380
```

### Authentication

```bash
# Require password for all connections
./redlite --db mydata.db --password secret
```

### Performance Tuning

```bash
# Use 1GB cache for high-performance reads
./redlite --db mydata.db --cache 1024

# Default: 64MB cache
./redlite --db mydata.db
```

The `--cache` flag sets SQLite's page cache size. Larger values keep more data in RAM for faster reads while maintaining full durability.

## Library Configuration

When using Redlite as an embedded library, configuration is done through code:

```rust
use redlite::Db;

// Persistent database
let db = Db::open("/path/to/data.db")?;

// In-memory database
let db = Db::open_memory()?;

// With custom cache size (1GB)
let db = Db::open_with_cache("/path/to/data.db", 1024)?;

// Or set cache at runtime
let db = Db::open("/path/to/data.db")?;
db.set_cache_mb(1024)?;

// Select database (0-15)
let mut db = Db::open("/path/to/data.db")?;
db.select(1)?;
```

## SQLite Settings

Redlite configures SQLite with sensible defaults:

| Setting | Value | Purpose |
|---------|-------|---------|
| `journal_mode` | `WAL` | Concurrent reads, better performance |
| `synchronous` | `NORMAL` | Balance of safety and speed |
| `foreign_keys` | `ON` | Enforce referential integrity |
| `busy_timeout` | `5000ms` | Wait for locks before failing |

These settings are applied automatically when opening a database and cannot currently be changed.

## Environment Variables

Currently, Redlite does not use environment variables. All configuration is done via command line arguments or programmatically.

## Logging

Redlite uses the `tracing` crate for logging. Configure with standard Rust logging environment variables:

```bash
# Enable info logging
RUST_LOG=info ./redlite --db mydata.db

# Debug logging
RUST_LOG=debug ./redlite --db mydata.db

# Trace logging (very verbose)
RUST_LOG=trace ./redlite --db mydata.db
```

## Database Limits

| Limit | Value |
|-------|-------|
| Max databases | 16 (0-15) |
| Max key size | ~1GB (SQLite blob limit) |
| Max value size | ~1GB (SQLite blob limit) |
| Max database size | ~281TB (SQLite limit) |

In practice, you'll hit disk space limits long before SQLite limits.
