---
title: Configuration
description: Configuration options for Redlite
---

Redlite operates with default settings that can be overridden via command-line arguments or API calls.

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
| `--max-disk` | | `0` | Maximum disk size in bytes (0 = unlimited). Evicts oldest keys when exceeded |

### Database Path

```bash
# Persistent file
./redlite --db /var/lib/redlite/data.db

# In-memory (no persistence)
./redlite --storage memory
```

### Network Binding

```bash
# Localhost only (default)
./redlite --addr 127.0.0.1:6379

# All interfaces
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

### Disk Eviction

```bash
# Limit database to 100MB on disk
./redlite --db mydata.db --max-disk 104857600
```

When `--max-disk` is set, redlite automatically evicts the oldest keys (by creation time) when disk usage exceeds the limit. Eviction checks run every second during write operations.

Runtime adjustment:
```bash
# Via redis-cli or any Redis client
CONFIG SET maxdisk 52428800  # Change to 50MB
CONFIG GET maxdisk            # Check current limit
```

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

Redlite configures SQLite with these defaults:

| Setting | Value | Purpose |
|---------|-------|---------|
| `journal_mode` | `WAL` | Write-ahead logging for concurrent readers |
| `synchronous` | `NORMAL` | Fsync at checkpoints, not every commit |
| `foreign_keys` | `ON` | Enforce foreign key constraints |
| `busy_timeout` | `5000ms` | Wait duration for locked database |

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
