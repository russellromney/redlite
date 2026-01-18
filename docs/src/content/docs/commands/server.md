---
title: Server & Connection
description: Server and connection commands in Redlite
---

Server and connection commands for managing the Redis protocol connection, authentication, and server information.

## Commands

| Command | Syntax | Description |
|---------|--------|-------------|
| PING | `PING [message]` | Test connection, returns PONG or echoes message |
| ECHO | `ECHO message` | Echo the given message |
| INFO | `INFO [section]` | Get server information and statistics |
| SELECT | `SELECT db` | Select database (0-15) |
| DBSIZE | `DBSIZE` | Return number of keys in current database |
| FLUSHDB | `FLUSHDB` | Delete all keys in current database |
| QUIT | `QUIT` | Close the connection |
| AUTH | `AUTH password` | Authenticate with password |
| COMMAND | `COMMAND` | List all supported commands |
| CONFIG | `CONFIG GET/SET option` | Get or set configuration |

## Examples

### Connection Testing

```bash
# Simple ping
127.0.0.1:6379> PING
PONG

# Ping with message
127.0.0.1:6379> PING "hello"
"hello"

# Echo message
127.0.0.1:6379> ECHO "testing"
"testing"
```

### Database Selection

```bash
# Redlite supports databases 0-15 (like Redis)
127.0.0.1:6379> SELECT 0
OK

127.0.0.1:6379> SET key1 "in db 0"
OK

127.0.0.1:6379> SELECT 1
OK

127.0.0.1:6379> SET key1 "in db 1"
OK

127.0.0.1:6379> GET key1
"in db 1"

127.0.0.1:6379> SELECT 0
OK

127.0.0.1:6379> GET key1
"in db 0"
```

### Database Statistics

```bash
# Count keys in current database
127.0.0.1:6379> DBSIZE
(integer) 42

# Clear current database
127.0.0.1:6379> FLUSHDB
OK

127.0.0.1:6379> DBSIZE
(integer) 0
```

### Server Information

```bash
127.0.0.1:6379> INFO
# Server
redis_version:7.0.0
redlite_version:0.1.0
os:darwin
arch:aarch64

# Clients
connected_clients:1

# Memory
used_memory:1048576
used_memory_human:1.00M

# Stats
total_connections_received:5
total_commands_processed:100

# Keyspace
db0:keys=42,expires=5
```

### Authentication

```bash
# Start server with password
$ redlite --db mydata.db --password mysecret

# Client must authenticate
127.0.0.1:6379> GET key
(error) NOAUTH Authentication required

127.0.0.1:6379> AUTH mysecret
OK

127.0.0.1:6379> GET key
"value"
```

### Configuration

```bash
# Get configuration values
127.0.0.1:6379> CONFIG GET maxmemory
1) "maxmemory"
2) "0"

127.0.0.1:6379> CONFIG GET maxmemory-policy
1) "maxmemory-policy"
2) "noeviction"

# Set configuration
127.0.0.1:6379> CONFIG SET maxmemory 104857600
OK

127.0.0.1:6379> CONFIG SET maxmemory-policy allkeys-lru
OK

# Get disk limit
127.0.0.1:6379> CONFIG GET maxdisk
1) "maxdisk"
2) "0"
```

### List Supported Commands

```bash
127.0.0.1:6379> COMMAND
1) "GET"
2) "SET"
3) "DEL"
4) "MGET"
5) "MSET"
... (200+ commands)
```

## Configuration Options

| Option | Description | Default |
|--------|-------------|---------|
| `maxmemory` | Maximum memory in bytes (0 = unlimited) | 0 |
| `maxmemory-policy` | Eviction policy when limit reached | noeviction |
| `maxdisk` | Maximum disk size in bytes (0 = unlimited) | 0 |
| `persist-access-tracking` | Persist LRU/LFU tracking to disk | auto |
| `access-flush-interval` | Tracking flush interval in ms | 300000 |

### Eviction Policies

| Policy | Description |
|--------|-------------|
| `noeviction` | Return error when memory limit reached |
| `allkeys-lru` | Evict least recently used keys |
| `allkeys-lfu` | Evict least frequently used keys |
| `allkeys-random` | Evict random keys |
| `volatile-lru` | Evict LRU keys with TTL set |
| `volatile-lfu` | Evict LFU keys with TTL set |
| `volatile-ttl` | Evict keys with shortest TTL |
| `volatile-random` | Evict random keys with TTL set |

## Library Mode (Rust)

```rust
use redlite::Db;

// Open database (equivalent to connecting)
let db = Db::open("mydata.db")?;

// Select database (0-15)
db.select(1)?;

// Get database size
let count = db.dbsize()?;

// Clear database
db.flushdb()?;

// Configuration
db.set_max_memory(100 * 1024 * 1024);  // 100MB
db.set_eviction_policy(EvictionPolicy::AllKeysLru);
```

## CLI Options

```bash
# Start server with options
redlite --db mydata.db \
        --host 127.0.0.1 \
        --port 6379 \
        --password mysecret \
        --max-memory 100mb \
        --max-disk 1gb
```

| Option | Description |
|--------|-------------|
| `--db` | Database file path |
| `--host` | Bind address (default: 127.0.0.1) |
| `--port` | Port number (default: 6379) |
| `--password` | Require authentication |
| `--max-memory` | Memory limit for eviction |
| `--max-disk` | Disk limit for eviction |

## Use Cases

### Health Check

```bash
# Simple liveness check
PING

# With custom message for identification
PING "worker-1"
```

### Multi-Tenant Isolation

```bash
# Each tenant gets a separate database
SELECT 0  # Tenant A
SET user:1 "Alice"

SELECT 1  # Tenant B
SET user:1 "Bob"

# Data is isolated
SELECT 0
GET user:1  # "Alice"
```

### Development Reset

```bash
# Clear all test data
SELECT 15  # Use db 15 for tests
FLUSHDB
# Run tests...
```
