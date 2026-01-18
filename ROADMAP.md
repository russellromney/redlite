# Roadmap

See [CHANGELOG.md](./CHANGELOG.md) for completed work.

---

## Planned

### Mobile SDKs (Local-First)

**Goal**: Enable redlite as the local cache/store for mobile apps - Redis semantics without a server.

**Value Proposition**:
- Structured data (hashes, lists, sorted sets) without SQL boilerplate
- Automatic TTL expiration for cache invalidation
- Familiar Redis API for developers
- Single file, zero config, works offline

#### SDK Priority Order

| Priority | Language | Binding Tool | Platform | Status |
|----------|----------|--------------|----------|--------|
| 1 | **Dart** | [flutter_rust_bridge](https://github.com/aspect-build/aspect-dev) | iOS + Android + Web | Planned |
| 2 | **Swift** | [swift-bridge](https://github.com/chinedufn/swift-bridge) | Native iOS | Planned |
| 3 | **Kotlin** | [jni-rs](https://github.com/jni-rs/jni-rs) | Native Android | Planned |
| 4 | **Ruby** | [magnus](https://github.com/matsadler/magnus) | Server-side | Planned |

#### Dart/Flutter SDK (Highest Priority)

**Rationale**: Single codebase covers iOS, Android, and Web. flutter_rust_bridge auto-generates Dart bindings from Rust with minimal boilerplate.

**Architecture**:
```
Flutter App
    │
    ▼
redlite_flutter (Dart package)
    │
    ▼
flutter_rust_bridge (generated FFI)
    │
    ▼
redlite-dart (Rust crate)
    │
    ▼
redlite core
```

**API Design**:
```dart
final db = await Redlite.open('app_cache.db');

// Caching with TTL
await db.setex('user:123', jsonEncode(userData), 3600); // 1 hour TTL
final user = await db.get('user:123');

// Sorted sets for leaderboards
await db.zadd('leaderboard', {'alice': 100, 'bob': 85});
final top10 = await db.zrevrange('leaderboard', 0, 9);

// Lists for activity feeds
await db.lpush('feed:123', newActivity);
final feed = await db.lrange('feed:123', 0, 49);
```

**Build**:
```bash
cd sdks/redlite-dart
flutter_rust_bridge_codegen generate
flutter pub get
```

#### Swift SDK

**Binding Options**:
1. **swift-bridge** - Pure Rust, generates Swift bindings
2. **C FFI** - Use existing libredlite_ffi.dylib with Swift C interop
3. **UniFFI** - Mozilla's cross-language binding generator

**Recommendation**: swift-bridge for best Swift ergonomics.

#### Kotlin SDK

**Binding Options**:
1. **jni-rs** - Direct JNI bindings from Rust
2. **UniFFI** - Generates Kotlin bindings from UDL
3. **JNI via C** - Use existing C FFI layer

**Recommendation**: jni-rs for direct Rust→Kotlin path.

#### Ruby SDK

**Binding Options**:
1. **magnus** - Modern Ruby bindings, similar to PyO3
2. **rutie** - Alternative Ruby bindings
3. **C FFI** - Use existing libredlite_ffi.dylib with Ruby FFI gem

**Recommendation**: magnus for PyO3-like developer experience.

---

### Server Mode HA (High Availability)

**Goal**: Dead-simple failover for server mode with ~5 second recovery time.

**Design Philosophy**: Redis Sentinel takes 10-30 seconds for failover. We can beat that with a simpler design that uses S3 as the coordination layer (already there for Litestream).

#### Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                     Redlite HA                               │
│                                                              │
│   Leader ───────► S3 config (check every 5s for followers)  │
│      │                                                       │
│      │  heartbeat (1s)                                      │
│      ▼                                                       │
│   Follower ◄──── Litestream restore (continuous from S3)    │
│      │                                                       │
│      └────► watches leader, takes over if missing 5s        │
│                                                              │
└─────────────────────────────────────────────────────────────┘
```

#### Protocol

**Leader responsibilities:**
1. Send heartbeat to follower every 1 second
2. Check S3 config every 5 seconds for new followers
3. Stream WAL to S3 via Litestream
4. If network issues prevent heartbeat delivery → step down

**Follower responsibilities:**
1. Receive heartbeat from leader, update last-seen timestamp
2. Continuously restore from Litestream (always ~1s behind)
3. If no heartbeat for 5 seconds:
   - Grab S3 lease (prevents split-brain)
   - Try to notify old leader (best effort)
   - Promote self to leader
   - Start Litestream replication (now source of truth)

**Old leader recovery:**
- When old leader comes back online, it sees lease is held by another node
- Automatically demotes to follower
- Starts Litestream restore from S3

#### Constraints

- **Single follower only** — No race condition for lease, simpler protocol
- **S3 as coordination** — Lease file with conditional writes prevents split-brain
- **Litestream for data** — No custom replication protocol needed

#### Failover Timeline

```
0s     - Leader dies (or network partition)
1-5s   - Follower detects missing heartbeats
5s     - Follower grabs S3 lease
5.1s   - Follower promotes, starts serving
─────────────────────────────────────────
Total: ~5 seconds (vs Redis 10-30 seconds)
```

#### S3 Lease File

```json
{
  "holder": "node-abc123",
  "timestamp": "2024-01-15T10:30:00Z",
  "expires": "2024-01-15T10:30:15Z"
}
```

Conditional PUT (ETag/If-Match) ensures only one node can grab the lease.

#### Implementation

```rust
struct HaNode {
    role: Role,              // Leader or Follower
    node_id: String,
    litestream: LitestreamHandle,
    s3_client: S3Client,
    follower_addr: Option<SocketAddr>,
}

impl HaNode {
    // Leader: send heartbeats
    async fn heartbeat_loop(&self) {
        loop {
            if let Some(addr) = &self.follower_addr {
                self.send_heartbeat(addr).await;
            }
            sleep(Duration::from_secs(1)).await;
        }
    }

    // Follower: watch for leader death
    async fn watch_leader(&self) {
        loop {
            if self.last_heartbeat.elapsed() > Duration::from_secs(5) {
                if self.try_grab_lease().await.is_ok() {
                    self.promote().await;
                }
            }
            sleep(Duration::from_millis(500)).await;
        }
    }

    async fn promote(&mut self) {
        self.litestream.stop_restore().await;
        self.litestream.start_replicate().await;
        self.role = Role::Leader;
    }

    async fn demote(&mut self) {
        self.litestream.stop_replicate().await;
        self.litestream.start_restore().await;
        self.role = Role::Follower;
    }
}
```

#### Fly.io Integration

On Fly, the proxy can serve as health check routing:

```toml
# fly.toml
[[services]]
  internal_port = 6379

  [[services.http_checks]]
    path = "/health"
    interval = "2s"
    timeout = "1s"
```

Health endpoint returns 200 if leader, 503 if follower. Fly routes to healthy node.

#### Edge Cases

1. **S3 down** — Both nodes continue with last known role until S3 recovers
2. **Litestream lag** — Follower may be missing last ~1s of writes on promotion (acceptable for cache)
3. **Both nodes start simultaneously** — First to grab lease wins, other becomes follower
4. **Leader notification fails** — Old leader will discover via S3 lease check

#### Cost Comparison

```
Redis Sentinel HA:     3+ nodes, all in memory     $500-2000/mo
Redlite HA:            2 nodes, data on disk       $50-100/mo
                       + S3 pennies
```

#### Success Criteria

- [ ] Leader/follower mode implemented
- [ ] 1s heartbeat protocol working
- [ ] S3 lease grab with conditional writes
- [ ] Automatic promotion on leader failure
- [ ] Automatic demotion when old leader returns
- [ ] Litestream integration (replicate/restore switching)
- [ ] <5 second failover time
- [ ] Tests: leader death, follower promotion, old leader recovery

---

### HyperLogLog (Probabilistic Cardinality)

Approximate COUNT DISTINCT with O(1) memory per key. Useful for unique visitor counts, distinct element estimation.

**Commands:**
```bash
PFADD key element [element ...]    # Add elements to HLL
PFCOUNT key [key ...]              # Get cardinality estimate (0.81% error)
PFMERGE destkey sourcekey [sourcekey ...]  # Merge HLLs
PFDEBUG DECODE|ENCODING|GETREG key  # Debug commands (optional)
```

**Implementation:**
- Build our own in Rust (~100 lines, algorithm is well-documented)
- Store 16KB register array per key as BLOB
- Use 14-bit prefix (16384 registers) like Redis
- Compare against [sqlite_hll](https://github.com/wperron/sqlite_hll) for correctness verification
- Reference: [hyperloglog-rs](https://github.com/LucaCappelletti94/hyperloglog-rs) (MIT) for algorithm details

**Schema:**
```sql
CREATE TABLE IF NOT EXISTS hll (
    id INTEGER PRIMARY KEY,
    key_id INTEGER NOT NULL REFERENCES keys(id) ON DELETE CASCADE,
    registers BLOB NOT NULL,  -- 16KB packed registers
    UNIQUE(key_id)
);
```

**Why build our own:** Avoid crate dependency for ~100 lines of code. Protects against supply chain issues. Algorithm is public domain (Flajolet et al. 2007).

---

### Bloom Filters (Probabilistic Set Membership)

Probabilistic "is this possibly in the set?" with configurable false positive rate. O(1) memory per filter.

**Commands:**
```bash
BF.ADD key item                    # Add item to filter
BF.EXISTS key item                 # Check if possibly present
BF.MADD key item [item ...]        # Batch add
BF.MEXISTS key item [item ...]     # Batch check
BF.RESERVE key error_rate capacity # Create with specific params
BF.INFO key                        # Get filter info
BF.CARD key                        # Estimated cardinality
```

**Implementation:**
- Bit array stored as BLOB in SQLite
- Configurable hash count (k) and size (m) based on desired error rate
- Default: 1% false positive rate
- ~100 lines of Rust

**Schema:**
```sql
CREATE TABLE IF NOT EXISTS bloom_filters (
    id INTEGER PRIMARY KEY,
    key_id INTEGER NOT NULL REFERENCES keys(id) ON DELETE CASCADE,
    bits BLOB NOT NULL,
    size INTEGER NOT NULL,        -- m: bit array size
    num_hashes INTEGER NOT NULL,  -- k: hash function count
    items_added INTEGER DEFAULT 0,
    UNIQUE(key_id)
);
```

**Why implement:** Command parity with Redis Stack. Users migrating from Redis shouldn't have to rewrite deduplication logic.

---

### Time Series

High-frequency time-stamped data with aggregation and retention.

**Phase 1 (Now): Sorted Set Sugar**

Time series as sorted sets with timestamp scores:
```bash
TS.ADD key timestamp value [LABELS label value ...]
  → ZADD key timestamp value (+ metadata in hash)

TS.RANGE key fromTimestamp toTimestamp [AGGREGATION type bucketSize]
  → ZRANGEBYSCORE key from to (+ post-processing for aggregation)

TS.GET key
  → ZRANGE key -1 -1 WITHSCORES

TS.INFO key
  → ZCARD + metadata
```

**Phase 2 (Future): Native Time Series Extension**

SQLite extension optimized for append-only time series:
- Append-only B-tree (no rebalancing on insert)
- Automatic time-based partitioning
- Built-in downsampling (1s → 1m → 1h → 1d)
- Retention policies (auto-delete old data)
- Compression (delta encoding timestamps, gorilla for values)
- Aggregation queries: AVG, SUM, MIN, MAX, COUNT, FIRST, LAST, RANGE

**Open source opportunity:** SQLite time series extension doesn't exist in a good form. Could be a standalone project.

---

## Maybe

- Lua scripting (EVAL/EVALSHA)
- XAUTOCLAIM
- ACL system
- Nightly CI for battle tests (`.github/workflows/battle-test.yml`, 1M seeds)

### SDK-Assisted Failover

Optimize the HA design with client-side awareness for sub-second failover.

**Concept**: The basic HA design uses 5-second heartbeat timeout for failover. Client SDKs can detect leader failure immediately (request timeout) and trigger faster promotion.

**Client Behavior:**
```python
class RedliteClient:
    def __init__(self, leader: str, follower: str, timeout_ms: int = 100):
        self.leader = leader
        self.follower = follower
        self.active = self.leader
        self.timeout = timeout_ms

    def request(self, cmd):
        try:
            return self.conn(self.active).execute(cmd, timeout=self.timeout)
        except Timeout:
            # Leader timed out - try follower
            self.active = self.follower if self.active == self.leader else self.leader
            return self.conn(self.active).execute(cmd)
```

**Failover Scenarios:**

| Scenario | Server-Only HA | SDK-Assisted |
|----------|---------------|--------------|
| Leader dead | ~5 seconds | **~200ms** |
| Network blip to leader | ~5 seconds | **0ms** (retry succeeds) |
| False alarm | Protected by S3 lease | Protected by S3 lease |

**Safety**: S3 lease prevents split-brain. Even with aggressive client retries, only one node can grab the lease.

### Soft Delete + PURGE

Mark keys as deleted without removing data. Enables recovery and audit trails.

**Concept:**
```bash
SOFT DEL key [key ...]     # Mark as deleted (recoverable)
UNDELETE key               # Recover soft-deleted key
PURGE key [key ...]        # Permanently delete
PURGE DELETED BEFORE timestamp  # Bulk purge old deletions
```

**Implementation consideration:** Similar to TTL filtering - we already filter on `expire_at`. Could add `deleted_at`:
- NULL = not deleted
- timestamp = soft deleted at this time

**Who uses this:**
- Audit/compliance (retain deleted data for N days)
- Undo functionality
- Debugging ("what happened to this key?")
- Paranoid users who want recoverability

---

## Not Planned

- **Cluster mode** — Use [Litestream](https://litestream.io) for replication
- **Sentinel** — Single-node focus with simple HA (see above)
- **Redis Modules** — Use native Rust extensions instead

---

## Feature Flags

**Default (no flags needed):**
- All core Redis commands (strings, hashes, lists, sets, zsets, streams)
- Full-text search: FT.*, FTS commands (uses SQLite's built-in FTS5)
- English stemming (porter, built into FTS5)

**Optional extensions:**
```toml
[features]
geo = []          # GEO* commands — uses SQLite's built-in R*Tree (no extra deps)
vectors = []      # V* commands — adds sqlite-vector (~500KB)
fuzzy = []        # Trigram tokenizer + Levenshtein distance for fuzzy search
spellcheck = []   # FT.SPELLCHECK, FT.DICT* — adds spellfix1 (~50KB)
languages = []    # Non-English stemmers — adds Snowball (~200KB)
geoshape = []     # GEOSHAPE field type — enables Geopoly

full = ["vectors", "geo", "fuzzy"]  # Production-ready features
```

**Installation:**
```bash
# Default: full Redis + Search (no geo, no vectors)
cargo install redlite

# With geospatial commands
cargo install redlite --features geo

# With vector search
cargo install redlite --features vectors

# Everything (vectors + geo)
cargo install redlite --features full
```

---

## Principles

1. **Embedded-first** — Library mode is primary
2. **Disk is cheap** — Don't optimize for memory like Redis
3. **SQLite foundation** — ACID, durability, zero config
4. **Redis-compatible** — Existing clients should work
5. **Extend thoughtfully** — Add features Redis doesn't have
