# redlite

Embedded Redis on SQLite in Rust.

**Docs:** [redlite.dev](https://redlite.dev)

> Early alpha. Don't use in production yet. 

## Quick Start

**Library (recommended):**
```rust
use redlite::Db;

let db = Db::open("mydata.db")?;
db.set("user:1", b"alice", None)?;
let name = db.get("user:1")?;
```


**Server:**
```bash
cargo install redlite
redlite --db mydata.db
redis-cli SET foo bar
```

## When to Use

| Use Case | Redlite | Redis |
|----------|---------|-------|
| Single app KV store | Best choice | Overkill |
| Memory-constrained | Disk-backed | RAM-bound |
| Persistence required | Always durable | Needs config |
| Multi-app sharing | Server mode | Native |
| Clustered/Replicated | Use [Litestream](https://github.com/benbjohnson/litestream) for  replication. | Native |

## Embedded vs Server Mode

**Embedded mode** = Direct library calls, no network. Microsecond latency.

**Server mode** = Redis protocol over TCP. Works with any Redis client.

| Scenario | Use |
|----------|-----|
| Single process | Embedded `:memory:` or file |
| Multiple processes, same machine | Embedded file (SQLite WAL handles concurrency) |
| Multiple servers over network | Server mode |
| Hybrid | Primary app uses embedded (fast), other services hit redlite server (shared SQLite .db) |

```python
# All processes on same machine can share a file
db = Redlite("/shared/cache.db", cache_mb=50000)  # Everyone gets microsecond reads

# Hybrid: primary goes brr, secondaries use server
primary = Redlite("/data/cache.db")               # Direct FFI
secondary = Redlite("redis://localhost:6379")     # Via server
```

**Sweet spot**: Embedded file mode with large cache. 50GB hot cache, terabytes on disk. Multiple processes, one file. Litestream to S3. No server, no Docker, no ops.

## Embedded Blocking Operations

Blocking operations work in embedded mode via efficient polling:

```rust
use redlite::{Db, PollConfig};

let db = Db::open("mydata.db")?;

// Blocking pop (waits up to 30 seconds for data)
if let Some((key, value)) = db.blpop_sync(&["queue"], 30.0)? {
    println!("Got {} from {}", String::from_utf8_lossy(&value), key);
}

// Configure polling behavior
db.set_poll_config(PollConfig::aggressive()); // Low latency (100μs-500μs)
db.set_poll_config(PollConfig::relaxed());    // Low CPU (1ms-10ms)
```

**Cross-process coordination**: Multiple processes can share a SQLite file. Process A blocks on `blpop_sync`, Process B pushes with `rpush` - A unblocks immediately.

**Available methods**: `blpop_sync`, `brpop_sync`, `xread_block_sync`, `xreadgroup_block_sync`

## Why Redlite

- **Embedded-first** — No separate server/process needed
- **Disk-backed** — No RAM constraints
- **SQLite foundation** — ACID, durable, zero config
- **Redis compatible** — Standard clients work

## Install

```bash
cargo add redlite                      # Core features
cargo add redlite --features geo       # With geospatial commands
cargo add redlite --features vectors   # With vector search
cargo add redlite --features full      # Everything (geo + vectors)
```

## Commands

All standard Redis commands for strings, hashes, lists, sets, sorted sets, and streams. See [docs](https://redlite.dev/commands/overview) for full list.

**Server-only:** Pub/Sub, blocking reads (BLPOP, BRPOP, XREAD BLOCK)

**RediSearch:** FT.CREATE, FT.SEARCH, FT.INFO, FT.ALTER, FT.DROPINDEX, FT.EXPLAIN, FT.PROFILE, FT.AGGREGATE, aliases, synonyms, suggestions

**Redis 8 Vectors:** VADD, VSIM, VSIMBATCH, VREM, VCARD, VEXISTS, VDIM, VGET, VGETALL, VGETATTRIBUTES, VSETATTRIBUTES, VDELATTRIBUTES

**Geospatial (R*Tree):** GEOADD, GEOPOS, GEODIST, GEOHASH, GEOSEARCH, GEOSEARCHSTORE

```bash
# Add locations with coordinates
redis-cli GEOADD locations -122.4194 37.7749 "San Francisco"
redis-cli GEOADD locations -73.9857 40.7484 "New York"

# Calculate distance between cities
redis-cli GEODIST locations "San Francisco" "New York" KM

# Search within radius
redis-cli GEOSEARCH locations FROMMEMBER "San Francisco" BYRADIUS 5000 KM WITHCOORD WITHDIST
```

**Extensions:** `VACUUM`, `KEYINFO`, `AUTOVACUUM`, `HISTORY` (time-travel queries)

## Server Options

```bash
redlite --db mydata.db              # Persistent
redlite --storage memory            # In-memory
redlite --password secret           # Auth
redlite --cache 1024                # 1GB cache (faster reads)
```

## Performance

Embedded mode eliminates network overhead:

| Op | Redlite | Redis | Speedup |
|----|---------|-------|---------|
| GET | 232k/s | 1.9k/s | 122x |
| SET | 53k/s | 3.4k/s | 15x |

Tune with `--cache` for near-memory reads with SQLite durability.

## Testing

Redlite includes a comprehensive deterministic simulation testing framework ([redlite-dst](./redlite-dst)) inspired by TigerBeetle VOPR and sled simulation:

```bash
cd redlite-dst
cargo run -- smoke              # Quick sanity checks
cargo run -- properties         # Property-based tests
cargo run -- seeds test         # Regression seed bank
```

Every test failure includes a reproducible seed for debugging.

## Backups

Use [walsync](https://github.com/russellromney/walsync) or [Litestream](https://litestream.io) for continuous backups.

## License

Apache 2.0
