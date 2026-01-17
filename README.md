# redlite

Redis-compatible key-value store on SQLite in Rust.

**Docs:** [redlite.dev](https://redlite.dev)

> Early alpha. API may change before stable release.

## Quick Start

**Library:**
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

## Architecture

Redlite implements the Redis protocol on top of SQLite's storage engine. It operates in two modes:

**Embedded mode:** Direct library calls via FFI bindings without network I/O.

**Server mode:** Redis protocol over TCP. Compatible with standard Redis clients.

### Mode Selection

| Configuration | Mode |
|--------------|------|
| Single process | Embedded (`:memory:` or file) |
| Multiple processes, same machine | Embedded file (SQLite WAL mode) |
| Network access required | Server mode |
| Mixed access | Embedded + server (shared SQLite file) |

```python
# Shared file access across processes
db = Redlite("/shared/cache.db")

# Hybrid: embedded + server on shared file
primary = Redlite("/data/cache.db")               # Direct access
secondary = Redlite("redis://localhost:6379")     # Via server
```

### Storage Characteristics

- **Persistence:** All writes are durable (SQLite WAL mode)
- **Memory:** Configurable page cache (default: 64MB)
- **Disk:** Grows with dataset size, no upper limit
- **Replication:** Use [Litestream](https://github.com/benbjohnson/litestream) for continuous backup to S3

## Embedded Blocking Operations

Blocking operations in embedded mode use polling on the SQLite database:

```rust
use redlite::{Db, PollConfig};

let db = Db::open("mydata.db")?;

// Blocks up to 30 seconds waiting for data
if let Some((key, value)) = db.blpop_sync(&["queue"], 30.0)? {
    println!("Got {} from {}", String::from_utf8_lossy(&value), key);
}

// Configure polling intervals
db.set_poll_config(PollConfig::aggressive()); // 100μs-500μs intervals
db.set_poll_config(PollConfig::relaxed());    // 1ms-10ms intervals
```

**Cross-process synchronization:** SQLite file can be shared across processes. Process A blocks on `blpop_sync` while Process B calls `rpush` - A unblocks on next poll cycle.

**Available methods:** `blpop_sync`, `brpop_sync`, `xread_block_sync`, `xreadgroup_block_sync`

## Features

- Embedded library and standalone server
- Disk-backed storage with configurable memory cache
- SQLite storage engine (ACID guarantees)
- Redis protocol compatibility

## Install

```bash
cargo add redlite                      # Core features
cargo add redlite --features geo       # With geospatial commands
cargo add redlite --features vectors   # With vector search
cargo add redlite --features full      # Everything (geo + vectors)
```

## Commands

### Core Data Types
Strings, hashes, lists, sets, sorted sets, streams. See [docs](https://redlite.dev/commands/overview) for complete command list.

### Server-Only Commands
Pub/Sub, blocking operations (BLPOP, BRPOP, XREAD BLOCK)

### Search (RediSearch compatible)
FT.CREATE, FT.SEARCH, FT.INFO, FT.ALTER, FT.DROPINDEX, FT.EXPLAIN, FT.PROFILE, FT.AGGREGATE, aliases, synonyms, suggestions

### Vector Search (Redis 8)
VADD, VSIM, VSIMBATCH, VREM, VCARD, VEXISTS, VDIM, VGET, VGETALL, VGETATTRIBUTES, VSETATTRIBUTES, VDELATTRIBUTES

### Geospatial (R*Tree index)
GEOADD, GEOPOS, GEODIST, GEOHASH, GEOSEARCH, GEOSEARCHSTORE

```bash
# Geospatial example
redis-cli GEOADD locations -122.4194 37.7749 "San Francisco"
redis-cli GEOADD locations -73.9857 40.7484 "New York"
redis-cli GEODIST locations "San Francisco" "New York" KM
redis-cli GEOSEARCH locations FROMMEMBER "San Francisco" BYRADIUS 5000 KM WITHCOORD WITHDIST
```

### SQLite Extensions
`VACUUM`, `KEYINFO`, `AUTOVACUUM`, `HISTORY` (time-travel queries)

## Server Options

```bash
redlite --db mydata.db              # File-backed storage
redlite --storage memory            # In-memory only
redlite --password secret           # Require AUTH
redlite --cache 1024                # 1GB page cache
redlite --max-disk 104857600        # 100MB disk limit
```

**Eviction policy:** When `--max-disk` is set, oldest keys (by creation time) are evicted when disk usage exceeds the limit. Eviction check runs every second during write operations. Adjust at runtime with `CONFIG SET maxdisk <bytes>`.

## Performance

Embedded mode eliminates network round-trip latency:

| Operation | Embedded | TCP (localhost) |
|-----------|----------|-----------------|
| GET | 232k/s | 1.9k/s |
| SET | 53k/s | 3.4k/s |

Increase `--cache` to keep more data in memory.

## Testing

Redlite has two comprehensive test suites:

### Oracle Tests - Redis Compatibility

Differential testing against Redis to verify identical behavior for all supported commands.

```bash
# Start Redis (native or Docker)
redis-server &
# Or: docker run -d -p 6379:6379 redis

# Run oracle tests (must be sequential)
cd redlite-oracle
cargo test -- --test-threads=1
```

**Coverage:** Strings, lists, hashes, sets, sorted sets, keys, streams, and bitmaps. Reports behavioral divergence.

**For SDK developers:** See [redlite-oracle/README.md](./redlite-oracle/README.md#adding-a-new-sdk-oracle-suite) for how to add oracle tests to your SDK.

### DST - Deterministic Simulation Testing

Deterministic simulation for concurrency testing, crash recovery verification, and fault injection. All failures are reproducible via seed values.

```bash
cd redlite-dst
cargo run -- smoke              # Quick sanity checks
cargo run -- properties         # Property-based invariants
cargo run -- simulate           # Concurrent operations
cargo run -- chaos              # Fault injection
cargo run -- seeds test         # Regression bank
```

Inspired by [TigerBeetle VOPR](https://github.com/tigerbeetle/tigerbeetle/blob/main/docs/internals/vopr.md), [sled simulation](https://sled.rs/simulation.html), and [MadSim](https://github.com/madsim-rs/madsim).

See [redlite-dst/README.md](./redlite-dst/README.md) for full details.

## Backups

Continuous backup to S3/object storage:
- [Litestream](https://litestream.io)
- [walsync](https://github.com/russellromney/walsync)

## License

Apache 2.0
