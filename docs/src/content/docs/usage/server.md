---
title: Server Mode
description: Running Redlite as a standalone Redis-compatible server
---

While Redlite is designed as an embedded library, it also includes a standalone server that speaks the Redis protocol. This lets you use any Redis client to connect.

## Starting the Server

### Basic Usage

```bash
# Default: persistent storage, port 6767
./redlite --db=mydata.db

# In-memory mode
./redlite --db=:memory:
```

### Custom Port

```bash
# Use Redis default port
./redlite --db=mydata.db --addr=127.0.0.1:6379

# Bind to all interfaces
./redlite --db=mydata.db --addr=0.0.0.0:6767
```

### Command Line Options

| Option | Default | Description |
|--------|---------|-------------|
| `--db`, `-d` | `redlite.db` | Database file path (`:memory:` for in-memory) |
| `--addr`, `-a` | `127.0.0.1:6767` | Listen address and port |

## Connecting with redis-cli

```bash
$ redis-cli -p 6767

127.0.0.1:6767> PING
PONG

127.0.0.1:6767> SET name "Redlite"
OK

127.0.0.1:6767> GET name
"Redlite"

127.0.0.1:6767> SET temp "expires" PX 5000
OK

127.0.0.1:6767> GET temp
"expires"

# Wait 5 seconds...

127.0.0.1:6767> GET temp
(nil)
```

## Using Redis Client Libraries

### Python

```python
import redis

r = redis.Redis(host='localhost', port=6767)

# String operations
r.set('foo', 'bar')
print(r.get('foo'))  # b'bar'

# With expiration
r.setex('session', 60, 'user_data')  # expires in 60 seconds
```

### Node.js

```javascript
import { createClient } from 'redis';

const client = createClient({ url: 'redis://localhost:6767' });
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
        Addr: "localhost:6767",
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
    let client = redis::Client::open("redis://127.0.0.1:6767/")?;
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
EXPOSE 6767
CMD ["redlite", "--db=/data/redlite.db", "--addr=0.0.0.0:6767"]
```

```bash
docker build -t redlite .
docker run -p 6767:6767 -v redlite-data:/data redlite
```

## Custom Commands

Redlite adds these commands on top of Redis:

- **HISTORY** - Track and query historical data with time-travel queries. See [History Tracking](/reference/history) for full documentation.
- **KEYINFO** - Get detailed key metadata (type, TTL, created/updated timestamps)
- **VACUUM** - Delete expired keys and reclaim disk space

## Differences from Redis

When using server mode, be aware of these differences:

1. **Persistence** - Data is always persisted (unless using `:memory:`)
2. **Memory** - Not bounded by RAM; uses disk storage
3. **Commands** - Only subset of Redis commands supported (see [Commands](/commands/strings))
4. **Clustering** - No cluster mode; single-node only
5. **Pub/Sub** - Supported (server mode only)
