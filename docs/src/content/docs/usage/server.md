---
title: Server Mode
description: Running Redlite as a standalone Redis-compatible server
---

Redlite includes a standalone server that implements the Redis protocol over TCP. Standard Redis clients can connect to it.

## Starting the Server

### Basic Usage

```bash
# Default: persistent storage, port 6379
./redlite --db mydata.db

# In-memory mode (no persistence)
./redlite --storage memory

# Alternative: SQLite's :memory: syntax also works
./redlite --db :memory:
```

### Custom Port

```bash
# Custom port
./redlite --db mydata.db --addr 127.0.0.1:6380

# Bind to all interfaces
./redlite --db mydata.db --addr 0.0.0.0:6379
```

### Command Line Options

| Option | Short | Default | Description |
|--------|-------|---------|-------------|
| `--db` | `-d` | `redlite.db` | Database file path |
| `--addr` | `-a` | `127.0.0.1:6379` | Listen address and port |
| `--password` | | (none) | Require password for connections (like Redis `requirepass`) |
| `--storage` | | `file` | Storage type: `file` or `memory` |
| `--backend` | | `sqlite` | Backend type: `sqlite` or `turso` |
| `--cache` | | `64` | SQLite page cache size in MB (larger = faster reads) |
| `--max-disk` | | `0` | Maximum disk size in bytes (0 = unlimited). Evicts oldest keys when exceeded |

## Connecting with redis-cli

```bash
$ redis-cli

127.0.0.1:6379> PING
PONG

127.0.0.1:6379> SET name "Redlite"
OK

127.0.0.1:6379> GET name
"Redlite"

127.0.0.1:6379> SET temp "expires" PX 5000
OK

127.0.0.1:6379> GET temp
"expires"

# Wait 5 seconds...

127.0.0.1:6379> GET temp
(nil)
```

### With Authentication

```bash
# Start server with password
./redlite --db mydata.db --password secret

# Connect with password
$ redis-cli -a secret
127.0.0.1:6379> PING
PONG
```

## Using Redis Client Libraries

### Python

```python
import redis

r = redis.Redis(host='localhost', port=6379)

# String operations
r.set('foo', 'bar')
print(r.get('foo'))  # b'bar'

# With expiration
r.setex('session', 60, 'user_data')  # expires in 60 seconds
```

### Node.js

```javascript
import { createClient } from 'redis';

const client = createClient({ url: 'redis://localhost:6379' });
await client.connect();

// String operations
await client.set('foo', 'bar');
console.log(await client.get('foo')); // 'bar'

// With expiration
await client.setEx('session', 60, 'user_data');
```

### Go

```go
package main

import (
    "context"
    "github.com/redis/go-redis/v9"
)

func main() {
    rdb := redis.NewClient(&redis.Options{
        Addr: "localhost:6379",
    })

    ctx := context.Background()

    rdb.Set(ctx, "foo", "bar", 0)
    val, _ := rdb.Get(ctx, "foo").Result()
    // val == "bar"
}
```

### Rust

```rust
use redis::Commands;

fn main() -> redis::RedisResult<()> {
    let client = redis::Client::open("redis://127.0.0.1:6379/")?;
    let mut con = client.get_connection()?;

    con.set("foo", "bar")?;
    let value: String = con.get("foo")?;
    // value == "bar"

    Ok(())
}
```

## Running as a Service

### systemd (Linux)

Create `/etc/systemd/system/redlite.service`:

```ini
[Unit]
Description=Redlite KV Store
After=network.target

[Service]
Type=simple
ExecStart=/usr/local/bin/redlite --db=/var/lib/redlite/data.db
Restart=always
User=redlite
Group=redlite

[Install]
WantedBy=multi-user.target
```

```bash
sudo systemctl enable redlite
sudo systemctl start redlite
```

### Docker

```dockerfile
FROM rust:1.75 as builder
WORKDIR /app
COPY . .
RUN cargo build --release

FROM debian:bookworm-slim
COPY --from=builder /app/target/release/redlite /usr/local/bin/
EXPOSE 6379
CMD ["redlite", "--db=/data/redlite.db", "--addr=0.0.0.0:6379"]
```

```bash
docker build -t redlite .
docker run -p 6379:6379 -v redlite-data:/data redlite
```

## Custom Commands

Redlite adds these commands on top of Redis:

- **HISTORY** - Track and query historical data with time-travel queries. See [History Tracking](/reference/history) for full documentation.
- **KEYINFO** - Get detailed key metadata (type, TTL, created/updated timestamps)
- **VACUUM** - Delete expired keys and reclaim disk space

## Differences from Redis

When using server mode, be aware of these differences:

1. **Persistence** - Persisted to disk by default (or in-memory with `--storage memory`)
2. **Memory** - Not bounded by RAM; uses disk storage
3. **Commands** - Only subset of Redis commands supported (see [Commands](/commands/overview))
4. **Clustering** - No cluster mode; single-node only
5. **Pub/Sub** - Supported (server mode only)
