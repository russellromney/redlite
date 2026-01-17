# Supported Commands

Quick reference for all implemented Redis commands. For detailed progress, see [ROADMAP.md](./ROADMAP.md).

## Implemented Commands (Sessions 1-23)

### Strings (Core KV) ✅

| Command | Description |
|---------|-------------|
| GET | Get value |
| SET | Set value (with EX/PX/NX/XX options) |
| MGET | Get multiple keys |
| MSET | Set multiple keys |
| INCR | Increment integer |
| INCRBY | Increment by amount |
| INCRBYFLOAT | Increment float |
| DECR | Decrement |
| DECRBY | Decrement by amount |
| SETNX | Set if not exists |
| SETEX | Set with expiration (seconds) |
| PSETEX | Set with expiration (ms) |
| APPEND | Append to string |
| STRLEN | Get string length |

### Key Management ✅

| Command | Description |
|---------|-------------|
| DEL | Delete key(s) |
| EXISTS | Check if key exists |
| EXPIRE | Set TTL in seconds |
| EXPIREAT | Set TTL as unix timestamp |
| PEXPIRE | Set TTL in ms |
| PEXPIREAT | Set TTL as unix ms |
| TTL | Get remaining TTL (seconds) |
| PTTL | Get remaining TTL (ms) |
| PERSIST | Remove expiration |
| TYPE | Get key type |
| KEYS | Find keys by pattern |
| SCAN | Iterate keys |
| DBSIZE | Key count |
| FLUSHDB | Delete all keys in current db |

### Hashes ✅

| Command | Description |
|---------|-------------|
| HSET | Set field(s) |
| HGET | Get field |
| HMGET | Get multiple fields |
| HGETALL | Get all fields/values |
| HDEL | Delete field(s) |
| HEXISTS | Check field exists |
| HKEYS | Get all field names |
| HVALS | Get all values |
| HLEN | Count fields |
| HINCRBY | Increment field int |
| HINCRBYFLOAT | Increment field float |
| HSETNX | Set field if not exists |

### Lists ✅

| Command | Description |
|---------|-------------|
| LPUSH | Prepend value(s) |
| RPUSH | Append value(s) |
| LPOP | Pop from front (optional count) |
| RPOP | Pop from back (optional count) |
| LLEN | List length |
| LRANGE | Get range |
| LINDEX | Get by index |
| LSET | Set by index |
| LTRIM | Trim list to range |

### Sets ✅

| Command | Description |
|---------|-------------|
| SADD | Add member(s), returns count added |
| SREM | Remove member(s), returns count removed |
| SMEMBERS | Get all members |
| SISMEMBER | Check membership (0/1) |
| SCARD | Count members |
| SPOP | Pop random member(s) (optional count) |
| SRANDMEMBER | Get random member(s) (optional count, negative allows repeats) |
| SDIFF | Difference of sets |
| SINTER | Intersection of sets |
| SUNION | Union of sets |

### Sorted Sets ✅

| Command | Description |
|---------|-------------|
| ZADD | Add member with score |
| ZREM | Remove member(s) |
| ZSCORE | Get score |
| ZRANK | Get rank (ascending) |
| ZREVRANK | Get rank (descending) |
| ZRANGE | Get by rank range |
| ZREVRANGE | Get by rank range (descending) |
| ZRANGEBYSCORE | Get by score range |
| ZCOUNT | Count by score range |
| ZCARD | Count members |
| ZINCRBY | Increment score |
| ZREMRANGEBYRANK | Remove by rank range |
| ZREMRANGEBYSCORE | Remove by score range |

### Server/Connection ✅

| Command | Description |
|---------|-------------|
| PING | Health check |
| ECHO | Echo message |
| INFO | Server info |
| SELECT | Select database (0-15) |
| QUIT | Close connection |
| AUTH | Authenticate with password |
| COMMAND | List supported commands |

### Client Commands (Session 23) ✅

| Command | Description |
|---------|-------------|
| CLIENT SETNAME | Set connection name |
| CLIENT GETNAME | Get connection name |
| CLIENT LIST | List all connections |
| CLIENT ID | Get connection ID |
| CLIENT INFO | Get current connection info |
| CLIENT KILL | Kill a connection |
| CLIENT PAUSE | Pause all clients |
| CLIENT UNPAUSE | Resume paused clients |

### Streams (Session 13) ✅

| Command | Description |
|---------|-------------|
| XADD | Add entry to stream |
| XREAD | Read from stream(s) |
| XRANGE | Get entries by ID range |
| XREVRANGE | Get entries in reverse |
| XLEN | Stream length |
| XTRIM | Trim stream by max length |
| XDEL | Delete entry |

### Consumer Groups (Session 14) ✅

| Command | Description |
|---------|-------------|
| XGROUP CREATE | Create consumer group |
| XGROUP DESTROY | Delete consumer group |
| XREADGROUP | Read from group (with consumer tracking) |
| XACK | Acknowledge message |
| XPENDING | Get pending messages |

### Transactions (Session 16, 22) ✅

| Command | Description |
|---------|-------------|
| MULTI | Start transaction (batch commands) |
| EXEC | Execute transaction |
| DISCARD | Abort transaction |
| WATCH | Watch keys for changes (optimistic locking) |
| UNWATCH | Clear watched keys |

### Pub/Sub (Session 15) ✅ - Server Mode Only

| Command | Description |
|---------|-------------|
| SUBSCRIBE | Subscribe to channel(s) |
| UNSUBSCRIBE | Unsubscribe from channel(s) |
| PUBLISH | Publish message to channel |
| PSUBSCRIBE | Subscribe to pattern |
| PUNSUBSCRIBE | Unsubscribe from pattern |

### Blocking Operations (Session 15) ✅ - Server Mode Only

| Command | Description |
|---------|-------------|
| BLPOP | Blocking pop from front |
| BRPOP | Blocking pop from back |
| BRPOPLPUSH | Blocking pop/push between lists |
| XREAD BLOCK | Blocking stream read |
| BLMOVE | Blocking list move |

### Redlite Custom Commands

| Command | Description |
|---------|-------------|
| VACUUM | Delete expired keys, run SQLite VACUUM |
| KEYINFO | Get key metadata (type, ttl, created_at, updated_at) |

### History Tracking (Session 17) ✅

| Command | Description |
|---------|-------------|
| HISTORY ENABLE | Enable history tracking (global/database/key level) with optional retention |
| HISTORY DISABLE | Disable history tracking (global/database/key level) |
| HISTORY GET | Query historical entries for a key (with LIMIT, SINCE, UNTIL filters) |
| HISTORY GETAT | Time-travel query: get state of key at specific timestamp |
| HISTORY LIST | List keys with history tracking enabled (with optional PATTERN) |
| HISTORY STATS | Get history statistics (total entries, timestamps, storage bytes) |
| HISTORY CLEAR | Clear history for a key (optionally before timestamp) |
| HISTORY PRUNE | Delete all history before timestamp across all keys |

### RediSearch Full-Text Search (Session 23) ✅

| Command | Description |
|---------|-------------|
| FT.CREATE | Create search index with schema (TEXT, NUMERIC, TAG fields) |
| FT.DROPINDEX | Drop search index (with optional DD to delete docs) |
| FT._LIST | List all indexes |
| FT.INFO | Get index metadata |
| FT.ALTER | Add field to existing index |
| FT.ALIASADD | Create index alias |
| FT.ALIASDEL | Delete index alias |
| FT.ALIASUPDATE | Update alias to point to different index |
| FT.SYNUPDATE | Add terms to synonym group |
| FT.SYNDUMP | Get all synonym groups |
| FT.SUGADD | Add autocomplete suggestion |
| FT.SUGGET | Get autocomplete suggestions (with FUZZY, WITHSCORES) |
| FT.SUGDEL | Delete autocomplete suggestion |
| FT.SUGLEN | Count suggestions in dictionary |
| FT.SEARCH | Search index with query (NOCONTENT, WITHSCORES, LIMIT, SORTBY, RETURN) |

**Query Syntax:**
- `word1 word2` - AND (implicit)
- `word1 | word2` - OR
- `-word` - NOT
- `"exact phrase"` - Phrase match
- `word*` - Prefix search
- `@field:term` - Field-scoped
- `@field:[min max]` - Numeric range
- `@field:{tag1|tag2}` - Tag exact match

### Additional List & Set Operations ✅

| Command | Description |
|---------|-------------|
| LINSERT | Insert before/after pivot |
| LREM | Remove elements by value |
| SMOVE | Move member between sets |
| SDIFFSTORE | Store difference |
| SINTERSTORE | Store intersection |
| SUNIONSTORE | Store union |

### Planned Commands (Not Yet Implemented)

| Command | Description |
|---------|-------------|
| RENAME | Rename key |
| RENAMENX | Rename if target doesn't exist |
| LMOVE | Pop from one list, push to another |
| ZINTERSTORE | Store sorted set intersection |
| ZUNIONSTORE | Store sorted set union |
| HSCAN | Iterate hash fields |
| SSCAN | Iterate set members |
| ZSCAN | Iterate sorted set members |

---

## Not Supported

These Redis features are intentionally omitted:

| Feature | Reason |
|---------|--------|
| EVAL/EVALSHA | Lua scripting is out of scope |
| CLUSTER * | Not the use case — Redlite is for embedded/single-node |
| BITFIELD/BITOP | Niche operations |
| GETSET | Deprecated in Redis, use `SET key value GET` |
| RPOPLPUSH | Deprecated in Redis, use LMOVE |

## Geospatial Commands

Geospatial commands require the `geo` feature flag:

```bash
cargo add redlite --features geo
```

| Command | Description |
|---------|-------------|
| GEOADD | Add geospatial items (longitude, latitude, member) |
| GEOPOS | Get coordinates of members |
| GEODIST | Calculate distance between two members |
| GEOHASH | Get geohash string of members |
| GEOSEARCH | Search for members within radius or box |
| GEOSEARCHSTORE | GEOSEARCH with result storage |

Implementation uses R*Tree spatial indexing for efficient radius queries.

**Note:** Blocking operations (BLPOP, BRPOP, XREAD BLOCK) and Pub/Sub are implemented in server mode (Session 15+) but unavailable in library mode.

---

## Notes

### On Transactions (MULTI/EXEC)

Implemented in Session 16. Commands queued with MULTI are executed atomically in EXEC.

```
MULTI
SET key1 value1
SET key2 value2
EXEC  -- Both executed together
```

For even stronger guarantees, library mode offers SQLite transactions:
```rust
db.with_transaction(|tx| {
    tx.set("key1", b"value1")?;
    tx.set("key2", b"value2")?;
    Ok(())
})?;
```

### On Blocking Operations (BLPOP/BRPOP/XREAD BLOCK)

Implemented in Session 15 for server mode only. Blocking reads wait for data with configurable timeout:

```
BLPOP mylist 5  -- Wait 5 seconds for data
XREAD BLOCK 1000 STREAMS mystream 0  -- Wait 1s for stream entries
```

Not available in embedded library mode due to async I/O requirements.

### On Pub/Sub

Implemented in Session 15 for server mode only. At-most-once publish-subscribe semantics:

```
SUBSCRIBE channel1
PUBLISH channel1 "message"
```

Each subscriber receives published messages via RESP3 push notifications. Messages are lost if no active subscribers.

### On History Tracking (HISTORY)

Implemented in Session 17. Track value changes per key with time-travel queries.

```
# Enable history for a key (keep last 100 versions)
HISTORY ENABLE KEY mykey RETENTION COUNT 100

# Write values
SET mykey "v1"
SET mykey "v2"
SET mykey "v3"

# Query history
HISTORY GET mykey LIMIT 10
→ [version 1, 2, 3, ...]

# Time-travel query
HISTORY GETAT mykey <timestamp>
→ State of key at that timestamp

# Cleanup
HISTORY CLEAR mykey BEFORE <timestamp>
HISTORY PRUNE BEFORE <timestamp>
```

**Three-tier opt-in:** Configure at global, database (0-15), or key level with independent retention policies (unlimited, time-based, count-based).

**Available in:** Both embedded library mode and server mode (Session 17+).

See [History Tracking](/reference/history) documentation for complete details.
