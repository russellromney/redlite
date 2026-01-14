# redlite

SQLite-backed Redis-compatible embedded key-value store.

**Docs:** [redlite.dev](https://redlite.dev)

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
| Clusters | Use [walsync](https://github.com/russellromney/walsync) | Native |

## Why Redlite

- **Embedded-first** — No separate server needed
- **Disk-backed** — No RAM constraints
- **SQLite foundation** — ACID, durable, zero config
- **Redis compatible** — Standard clients work

## Install

```bash
cargo add redlite
```

## Commands

All standard Redis commands for strings, hashes, lists, sets, sorted sets, and streams. See [docs](https://redlite.dev/commands/overview) for full list.

**Server-only:** Pub/Sub, blocking reads (BLPOP, BRPOP, XREAD BLOCK)

**RediSearch:** FT.CREATE, FT.SEARCH, FT.INFO, FT.ALTER, FT.DROPINDEX, aliases, synonyms, suggestions

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
