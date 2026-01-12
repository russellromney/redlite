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
| `--addr` | `-a` | `127.0.0.1:6767` | Listen address and port |

### Database Path

```bash
# Persistent file
./redlite --db=/var/lib/redlite/data.db

# In-memory (no persistence)
./redlite --db=:memory:
```

### Network Binding

```bash
# Localhost only (default, secure)
./redlite --addr=127.0.0.1:6767

# All interfaces (for remote access)
./redlite --addr=0.0.0.0:6767

# Custom port
./redlite --addr=127.0.0.1:6379
```

## Library Configuration

When using Redlite as an embedded library, configuration is done through code:

```rust
use redlite::Db;

// Persistent database
let db = Db::open("/path/to/data.db")?;

// In-memory database
let db = Db::open_memory()?;

// Select database (0-15)
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
RUST_LOG=info ./redlite --db=mydata.db

# Debug logging
RUST_LOG=debug ./redlite --db=mydata.db

# Trace logging (very verbose)
RUST_LOG=trace ./redlite --db=mydata.db
```

## Database Limits

| Limit | Value |
|-------|-------|
| Max databases | 16 (0-15) |
| Max key size | ~1GB (SQLite blob limit) |
| Max value size | ~1GB (SQLite blob limit) |
| Max database size | ~281TB (SQLite limit) |

In practice, you'll hit disk space limits long before SQLite limits.
