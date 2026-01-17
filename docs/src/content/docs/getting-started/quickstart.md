---
title: Quick Start
description: Basic setup and usage examples
---

## Embedded Mode (Library)

```rust
use redlite::Db;
use std::time::Duration;

fn main() -> redlite::Result<()> {
    // Open a persistent database
    let db = Db::open("mydata.db")?;

    // Or use in-memory for testing
    // let db = Db::open_memory()?;

    // Basic SET/GET
    db.set("name", b"Alice", None)?;
    let name = db.get("name")?;
    println!("{:?}", name); // Some([65, 108, 105, 99, 101])

    // With TTL (expires in 60 seconds)
    db.set("session", b"abc123", Some(Duration::from_secs(60)))?;

    // SET with NX (only if not exists)
    use redlite::SetOptions;
    db.set_opts("counter", b"0", SetOptions::new().nx())?;

    // Delete keys
    db.del(&["name", "session"])?;

    Ok(())
}
```

## Server Mode

Start the server:

```bash
# With persistent storage
./target/release/redlite --db mydata.db

# In-memory mode (no persistence)
./target/release/redlite --storage memory

# Custom port
./target/release/redlite --db mydata.db --addr 127.0.0.1:6380
```

Connect with `redis-cli`:

```bash
$ redis-cli

127.0.0.1:6379> PING
PONG

127.0.0.1:6379> SET greeting "Hello, World!"
OK

127.0.0.1:6379> GET greeting
"Hello, World!"

127.0.0.1:6379> SET temp "expires soon" PX 5000
OK

127.0.0.1:6379> GET temp
"expires soon"

# Wait 5 seconds...

127.0.0.1:6379> GET temp
(nil)
```

## Using with Redis Libraries

Redlite is compatible with standard Redis clients. Example with Python:

```python
import redis

r = redis.Redis(host='localhost', port=6379)
r.set('foo', 'bar')
print(r.get('foo'))  # b'bar'
```

Or Node.js:

```javascript
import { createClient } from 'redis';

const client = createClient({ url: 'redis://localhost:6379' });
await client.connect();

await client.set('foo', 'bar');
console.log(await client.get('foo')); // 'bar'
```
