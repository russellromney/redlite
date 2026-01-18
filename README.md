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

## Blocking Operations

Embedded mode supports blocking operations via polling. Server mode uses async/await. See [documentation](https://redlite.dev/usage/embedded) for details.

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

### Pub/Sub (Server Only)
PUBLISH, SUBSCRIBE, PSUBSCRIBE, UNSUBSCRIBE, PUNSUBSCRIBE

### Blocking Operations
BLPOP, BRPOP, XREAD BLOCK, XREADGROUP BLOCK — embedded mode uses polling, server mode uses async/await.

### Search (RediSearch compatible)
FT.CREATE, FT.SEARCH, FT.INFO, FT.ALTER, FT.DROPINDEX, FT.EXPLAIN, FT.PROFILE, FT.AGGREGATE, aliases, synonyms, suggestions

### Vector Search (Redis 8)
VADD, VSIM, VSIMBATCH, VREM, VCARD, VEXISTS, VDIM, VGET, VGETALL, VGETATTRIBUTES, VSETATTRIBUTES, VDELATTRIBUTES

### Geospatial (requires `--features geo`)
GEOADD, GEOPOS, GEODIST, GEOHASH, GEOSEARCH, GEOSEARCHSTORE

```bash
# Install with geo feature
cargo install redlite --features geo

# Example commands
redis-cli GEOADD locations -122.4194 37.7749 "San Francisco"
redis-cli GEODIST locations "San Francisco" "New York" KM
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

**Eviction:** When `--max-disk` is set, oldest keys are evicted when disk usage exceeds the limit. Adjust with `CONFIG SET maxdisk <bytes>`.

## Performance

| Operation | Embedded (file) |
|-----------|-----------------|
| GET | 200k+ ops/sec |
| SET | 45k+ ops/sec |
| Mixed (read-heavy) | 90k ops/sec |

*Benchmark: macOS M1, 64MB cache. See [redlite-bench](./redlite-bench/) for comprehensive benchmarks.*

**Note:** History tracking, FTS indexing, and vector indexing add write overhead.

## Testing

**Oracle tests:** Differential testing against Redis for compatibility verification. See [redlite-oracle/README.md](./redlite-oracle/README.md).

**DST:** Deterministic simulation testing for concurrency and crash recovery. See [redlite-dst/README.md](./redlite-dst/README.md).

## Backups

Continuous backup to S3/object storage with [Litestream](https://litestream.io).

## License

Apache 2.0
