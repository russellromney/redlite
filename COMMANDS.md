# Supported Commands

## V1 Commands

### Strings (Core KV)

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

### Key Management

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

### Hashes

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

### Lists

| Command | Description |
|---------|-------------|
| LPUSH | Prepend value(s) |
| RPUSH | Append value(s) |
| LPOP | Pop from front |
| RPOP | Pop from back |
| LLEN | List length |
| LRANGE | Get range |
| LINDEX | Get by index |
| LSET | Set by index |
| LTRIM | Trim list to range |

### Sets

| Command | Description |
|---------|-------------|
| SADD | Add member(s) |
| SREM | Remove member(s) |
| SMEMBERS | Get all members |
| SISMEMBER | Check membership |
| SCARD | Count members |
| SPOP | Pop random member |
| SRANDMEMBER | Get random member(s) |
| SDIFF | Difference of sets |
| SINTER | Intersection of sets |
| SUNION | Union of sets |

### Sorted Sets

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

### Server/Connection

| Command | Description |
|---------|-------------|
| PING | Health check |
| ECHO | Echo message |
| INFO | Server info |
| SELECT | Select database (0-15) |
| QUIT | Close connection |

### Redlite Custom Commands

| Command | Description |
|---------|-------------|
| VACUUM | Delete expired keys, run SQLite VACUUM |
| KEYINFO | Get key metadata (type, ttl, created_at, updated_at) |

---

## V2 Commands (Roadmap)

| Command | Description |
|---------|-------------|
| MULTI | Start transaction |
| EXEC | Execute transaction |
| DISCARD | Abort transaction |
| RENAME | Rename key |
| RENAMENX | Rename if target doesn't exist |
| LINSERT | Insert before/after pivot |
| LREM | Remove elements by value |
| LMOVE | Pop from one list, push to another |
| SMOVE | Move member between sets |
| SDIFFSTORE | Store difference |
| SINTERSTORE | Store intersection |
| SUNIONSTORE | Store union |
| ZINTERSTORE | Store sorted set intersection |
| ZUNIONSTORE | Store sorted set union |
| HSCAN | Iterate hash fields |
| SSCAN | Iterate set members |
| ZSCAN | Iterate sorted set members |
| SUBSCRIBE | Subscribe to channel (embedded pub/sub) |
| PUBLISH | Publish to channel |
| UNSUBSCRIBE | Unsubscribe from channel |

---

## Not Supported

These Redis features are intentionally omitted:

| Feature | Reason |
|---------|--------|
| WATCH/UNWATCH | Use SQLite transactions in library mode. Optimistic locking adds complexity for minimal benefit in an embedded store. May reconsider for V3 if requested. |
| BLPOP/BRPOP/BLMOVE | Blocking operations require different architecture |
| EVAL/EVALSHA | Lua scripting is out of scope |
| CLUSTER * | Not the use case — Redlite is for embedded/single-node |
| STREAMS | Different data model, may consider for V3 |
| GEO* | Use PostGIS or specialized geo library |
| BITFIELD/BITOP | Niche operations |
| GETSET | Deprecated in Redis, use `SET key value GET` |
| RPOPLPUSH | Deprecated in Redis, use LMOVE (V2) |

---

## Notes

### On WATCH

Redis WATCH enables optimistic locking:
```
WATCH mykey
val = GET mykey
MULTI
SET mykey (val + 1)
EXEC  -- fails if mykey changed since WATCH
```

In Redlite, you don't need WATCH because:

1. **Library mode:** Use SQLite transactions directly
   ```rust
   db.with_transaction(|tx| {
       let val = tx.get("counter")?;
       tx.set("counter", val + 1)?;
       Ok(())
   })?;
   ```

2. **Server mode:** SQLite serializes writes (single writer), so contention is lower than Redis

3. **Simple cases:** Use INCR/INCRBY for atomic counter operations

If there's significant demand, WATCH may be added in V3.

### On Transactions (MULTI/EXEC)

V1 has per-command atomicity. Each command is atomic.

V2 will add MULTI/EXEC for command batching (reduces round-trips). Note: this is command queuing, not true ACID transactions across commands. For true transactions, use library mode with SQLite transactions.
