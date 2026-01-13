# Roadmap

See [CHANGELOG.md](./CHANGELOG.md) for completed features.

## In Progress

### Sessions 23-24: Search & Vectors Implementation

**See [IMPLEMENTATION_PLAN.md](./IMPLEMENTATION_PLAN.md) for full details.**

Currently implementing RediSearch-compatible FT.* and Redis 8-compatible V* commands.

#### Phase 1: RediSearch Core (Session 23.1) - COMPLETE
- [x] Create `src/schema_ft.sql` with RediSearch tables
- [x] Update db.rs migrations to include schema_ft.sql
- [x] Add FtIndex, FtField types to types.rs
- [x] Implement FT.CREATE in db.rs
- [x] Implement FT.DROPINDEX, FT._LIST, FT.INFO in db.rs
- [x] Implement FT.ALTER in db.rs
- [x] Implement FT.ALIASADD/DEL/UPDATE, FT.SYNUPDATE/DUMP, FT.SUGADD/GET/DEL/LEN
- [x] Add FT.* command routing in server/mod.rs
- [x] Add comprehensive unit tests (22 tests for FT.* db methods)

#### Phase 2: RediSearch Search (Session 23.2) - COMPLETE
- [x] Create `src/search.rs` query parser module
- [x] Implement RediSearch -> FTS5 query translation (AND/OR/NOT, phrase, prefix, field-scoped)
- [x] Implement FT.SEARCH with core options (NOCONTENT, VERBATIM, WITHSCORES, LIMIT, SORTBY, RETURN)
- [x] Support numeric range queries (@field:[min max])
- [x] Support tag exact match queries (@field:{tag1|tag2})
- [x] Add 26 unit tests (14 query parser + 12 ft_search integration)
- [ ] Add HIGHLIGHT, SUMMARIZE support (parsed but not applied)
- [ ] Implement FT.EXPLAIN and FT.PROFILE
- [ ] Auto-index documents into FTS5 on HSET

#### Phase 3: RediSearch Aggregations (Session 23.3)
- [ ] Implement FT.AGGREGATE with LOAD, GROUPBY, REDUCE, SORTBY, APPLY, FILTER, LIMIT

#### Phase 4: Redis 8 Vectors with sqlite-vec (Session 24)
- [ ] Replace schema_vectors.sql with Redis 8-compatible vector_sets schema
- [ ] Remove old redlite-native vector code (VECTOR ENABLE/DISABLE, etc.)
- [ ] Add sqlite-vec extension loading
- [ ] Implement VADD (FP32 blob + VALUES input modes)
- [ ] Implement VSIM (K-NN using sqlite-vec)
- [ ] Implement VREM, VCARD, VDIM, VINFO, VEMB, VGETATTR, VSETATTR, VRANDMEMBER

---

### Sessions 19-21: Language SDKs

**Strategy: Thin wrappers around existing Redis clients**

For server mode, existing Redis clients (redis-py, ioredis, go-redis) work as-is via RESP protocol.
For embedded mode, wrap existing clients + embed redlite via local socket or direct FFI.

**Python (Session 19)** — `redlite-py` wrapping redis-py
```python
from redlite import Redlite

# Server mode (uses redis-py under the hood)
db = Redlite.connect("localhost:6379")

# Embedded mode (starts internal server, no network)
db = Redlite.open("mydata.db")  # or :memory:

# All redis-py methods work via delegation
db.set("key", "value")
db.hset("user:1", mapping={"name": "Alice"})

# Redlite-specific namespaces for extra features
db.fts.enable(pattern="article:*")
db.fts.search("hello world", limit=10)
db.history.get("mykey", version=3)
db.geo.search("locations", lat=40.7, lon=-74.0, radius=10, unit="km")
```

**Node.js/Bun (Session 20)** — `redlite` wrapping ioredis
```typescript
import { Redlite } from 'redlite';

// Server or embedded
const db = await Redlite.open('mydata.db');
await db.set('key', 'value');
await db.fts.search('hello world');
```

**Go (Session 21)** — `redlite-go` wrapping go-redis
```go
import "github.com/russellromney/redlite-go"

db := redlite.Open("mydata.db")
db.Set(ctx, "key", "value", 0)
db.FTS.Search(ctx, "hello world")
```

**Implementation approach:**
1. Embed redlite binary/library
2. Start internal Unix socket server (or use FFI for hot path)
3. Wrap existing Redis client pointing to internal socket
4. Add namespace classes for redlite-specific commands (FTS, History, Vector, Geo)
5. Delegate all standard Redis methods to underlying client

## Planned

### Session 23: Full-Text Search (RediSearch compatible)

SQLite FTS5 backend with dual API: redlite-native (simple) + RediSearch-compatible (FT.*).
**Included by default** — uses SQLite's built-in FTS5 (no extra dependencies).

**Redlite-native API (already implemented):**
```bash
FTS ENABLE GLOBAL|DATABASE n|PATTERN pat|KEY key   # Four-tier opt-in
FTS DISABLE GLOBAL|DATABASE n|PATTERN pat|KEY key
FTS SEARCH "query" [LIMIT n] [HIGHLIGHT]           # Search with BM25
FTS REINDEX key                                     # Force re-index
FTS INFO                                            # Stats
```

**RediSearch-compatible API (to implement):**

*Index Management:*
```bash
FT.CREATE index ON HASH|JSON PREFIX n prefix... SCHEMA field TEXT|NUMERIC|TAG|GEO [SORTABLE] ...
FT.ALTER index SCHEMA ADD field type
FT.DROPINDEX index [DD]
FT.INFO index
FT._LIST
FT.ALIASADD alias index
FT.ALIASDEL alias
FT.ALIASUPDATE alias index
```

*Search:*
```bash
FT.SEARCH index "query" [NOCONTENT] [VERBATIM] [NOSTOPWORDS]
    [WITHSCORES] [WITHSORTKEYS] [FILTER field min max]
    [GEOFILTER field lon lat radius M|KM|MI|FT]
    [INKEYS n key...] [INFIELDS n field...]
    [RETURN n field [AS alias]...] [SUMMARIZE ...] [HIGHLIGHT ...]
    [SLOP n] [INORDER] [LANGUAGE lang] [SORTBY field ASC|DESC]
    [LIMIT offset num] [TIMEOUT ms] [PARAMS n name value...]
FT.EXPLAIN index query                              # Show query parse tree
FT.PROFILE index SEARCH|AGGREGATE QUERY query       # Run with timing stats
```

*Aggregations:*
```bash
FT.AGGREGATE index query
    [LOAD n field...]
    [GROUPBY n field... REDUCE func nargs arg... [AS name]...]
    [SORTBY n field ASC|DESC...]
    [APPLY expr AS alias]
    [FILTER expr]
    [LIMIT offset num]
    [TIMEOUT ms]
```

*REDUCE functions:* COUNT, COUNT_DISTINCT, SUM, AVG, MIN, MAX, TOLIST, FIRST_VALUE, STDDEV, QUANTILE, RANDOM_SAMPLE

*Suggestions (uses FTS5 vocab table):*
```bash
FT.SUGADD key string score [PAYLOAD payload]
FT.SUGGET key prefix [FUZZY] [WITHSCORES] [WITHPAYLOADS] [MAX n]
FT.SUGDEL key string
FT.SUGLEN key
```

*Synonyms:*
```bash
FT.SYNUPDATE index group_id term...
FT.SYNDUMP index
```

**Query Syntax (converted to FTS5 MATCH):**
- `word1 word2` → AND
- `word1 | word2` → OR
- `-word` → NOT
- `"exact phrase"` → phrase match
- `prefix*` → prefix search
- `@field:term` → field-scoped
- `@field:[min max]` → numeric range
- `@field:{tag1|tag2}` → TAG exact match
- `~word` → optional (boost score if present)

**Field Types:**
| Type | SQLite Mapping |
|------|----------------|
| TEXT | FTS5 column with stemming |
| NUMERIC | Regular column, range queries via WHERE |
| TAG | Exact match (no tokenization) |
| GEO | R*Tree integration (Session 25) |
| VECTOR | sqlite-vector (Session 24) |

**Implementation notes:**
- FT.CREATE creates index metadata + enables FTS for matching prefixes
- FT.SEARCH parses RediSearch query syntax, converts to FTS5 MATCH
- FT.AGGREGATE maps to SQL GROUP BY with aggregate functions
- APPLY expressions parsed and converted to SQL expressions
- Field-level indexing: index hash fields separately (not just string values)
- Existing FTS commands continue to work unchanged
- Timeout via `sqlite3_progress_handler` (works for reads and writes)

### Session 24: Vector Search (Redis 8 compatible)

Redis 8 introduced vector sets as a native data type. Full V* command set for redis-py compatibility.
**Optional** — requires `--features vectors` (adds ~500KB for sqlite-vector).

**Commands:**
```bash
VADD key (FP32 blob | VALUES n v1 v2...) element [REDUCE dim] [NOQUANT|Q8|BIN] [EF n] [SETATTR json] [M n]
VSIM key (ELE element | FP32 blob | VALUES n v1...) [WITHSCORES] [WITHATTRIBS] [COUNT n] [FILTER expr]
VREM key element
VCARD key                    # Count elements
VDIM key                     # Get dimensions
VINFO key                    # Index metadata
VEMB key element [RAW]       # Get element's vector
VGETATTR key element         # Get JSON attributes
VSETATTR key element json    # Set JSON attributes
VRANDMEMBER key [count]      # Random sampling
```

**Implementation:**
- Backend: [sqlite-vector](https://github.com/sqliteai/sqlite-vector) for SIMD-accelerated distance
- Storage: Vectors as BLOBs (no virtual tables)
- Quantization: int8 (Q8 default), binary (BIN), or full precision (NOQUANT)
- Metrics: L2 (Euclidean), Cosine, Inner Product
- FILTER expressions parsed same as FT.AGGREGATE APPLY

**Schema (`src/schema_vectors.sql`):**
```sql
CREATE TABLE IF NOT EXISTS vector_sets (
    id INTEGER PRIMARY KEY,
    key_id INTEGER NOT NULL REFERENCES keys(id) ON DELETE CASCADE,
    element TEXT NOT NULL,
    embedding BLOB NOT NULL,
    quantization TEXT DEFAULT 'Q8',
    attributes TEXT,                -- JSON for FILTER/VGETATTR
    UNIQUE(key_id, element)
);
CREATE INDEX IF NOT EXISTS idx_vector_sets_key ON vector_sets(key_id);
```

### Session 25: Geospatial (R*Tree)

Redis-compatible geo commands using SQLite's built-in R*Tree extension.
**Included by default** — R*Tree is built into standard SQLite.

**Commands:**
```bash
GEOADD key [NX|XX] [CH] longitude latitude member [lon lat member ...]
GEOPOS key member [member ...]
GEODIST key member1 member2 [M|KM|MI|FT]
GEOHASH key member [member ...]
GEOSEARCH key <FROMMEMBER member | FROMLONLAT lon lat>
              <BYRADIUS radius M|KM|MI|FT | BYBOX width height M|KM|MI|FT>
              [ASC|DESC] [COUNT n [ANY]] [WITHCOORD] [WITHDIST] [WITHHASH]
GEOSEARCHSTORE dest src <FROMMEMBER|FROMLONLAT> <BYRADIUS|BYBOX> [STOREDIST]
```

**Implementation:**
- R*Tree virtual table for bounding-box pre-filtering
- Haversine formula for precise distance calculations
- Geohash encoding for GEOHASH command
- Results stored as sorted sets (for GEOSEARCHSTORE compatibility)

**Schema (`src/schema_geo.sql`):**
```sql
CREATE TABLE IF NOT EXISTS geo_data (
    id INTEGER PRIMARY KEY,
    key_id INTEGER NOT NULL REFERENCES keys(id) ON DELETE CASCADE,
    member TEXT NOT NULL,
    longitude REAL NOT NULL,
    latitude REAL NOT NULL,
    geohash TEXT,                   -- Pre-computed 11-char geohash
    UNIQUE(key_id, member)
);
CREATE INDEX IF NOT EXISTS idx_geo_data_key ON geo_data(key_id);

CREATE VIRTUAL TABLE IF NOT EXISTS geo_rtree USING rtree(
    id, minLon, maxLon, minLat, maxLat
);
```

**Query pattern:**
1. Compute bounding box from center + radius
2. Query R*Tree for candidates
3. Apply Haversine for precise distance filtering
4. Sort by distance, apply COUNT limit

**Distance units:** M (default), KM, MI, FT

**Coordinate limits:** Longitude -180 to 180, Latitude -85.05112878 to 85.05112878

**GEOSHAPE (optional `--features geoshape`):**

For polygon queries, enable Geopoly extension:
```bash
# In FT.CREATE schema
FT.CREATE idx ON HASH SCHEMA location GEOSHAPE

# Polygon queries in FT.SEARCH
FT.SEARCH idx "@location:[WITHIN $poly]" PARAMS 2 poly "POLYGON((...))"
FT.SEARCH idx "@location:[CONTAINS $point]" PARAMS 4 point "POINT(lon lat)"
```

Uses SQLite Geopoly for polygon contains/intersects operations. R*Tree handles MBR filtering, Geopoly does exact polygon math.

### Session 26: Additional Commands

- GETEX, GETDEL, SETEX, PSETEX
- LPUSHX, RPUSHX, LPOS, LMOVE
- BITCOUNT, BITFIELD, BITOP, SETBIT, GETBIT
- RENAME, RENAMENX
- HSCAN, SSCAN, ZSCAN
- ZINTERSTORE, ZUNIONSTORE

### Future

- In-memory mode with periodic snapshots (like Redis RDB)
- HISTORY REPLAY/DIFF for state reconstruction
- Background expiration daemon

## Maybe

- Lua scripting (EVAL/EVALSHA)
- XAUTOCLAIM
- ACL system

## Not Planned

- Cluster mode — Use [walsync](https://github.com/russellromney/walsync) for replication
- Sentinel
- Redis Modules

## Feature Flags

**Default (no flags needed):**
- All core Redis commands (strings, hashes, lists, sets, zsets, streams)
- Full-text search: FT.*, FTS commands (uses SQLite's built-in FTS5)
- Geospatial: GEO* commands (uses SQLite's built-in R*Tree)
- English stemming (porter, built into FTS5)

**Optional extensions:**
```toml
[features]
vectors = []      # V* commands — adds sqlite-vector (~500KB)
fuzzy = []        # Trigram tokenizer for approximate matching
spellcheck = []   # FT.SPELLCHECK, FT.DICT* — adds spellfix1 (~50KB)
languages = []    # Non-English stemmers — adds Snowball (~200KB)
geoshape = []     # GEOSHAPE field type — enables Geopoly

full = ["vectors", "fuzzy", "spellcheck", "languages", "geoshape"]
```

**Installation:**
```bash
# Default: full Redis + Search + Geo
cargo install redlite

# With vector search
cargo install redlite --features vectors

# Everything
cargo install redlite --features full
```

## Principles

1. **Embedded-first** — Library mode is primary
2. **Disk is cheap** — Don't optimize for memory like Redis
3. **SQLite foundation** — ACID, durability, zero config
4. **Redis-compatible** — Existing clients should work
5. **Extend thoughtfully** — Add features Redis doesn't have
