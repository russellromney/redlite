# Changelog

## Sessions 1-23.2 (Complete)

### Benchmark Suite Enhancements
- File-backed database size measurement (db + WAL + shm files)
- History entry count and storage bytes tracking
- `get_history_count()` trait method for global history stats
- `bytes_per_history_entry` calculation in BenchmarkResult
- Enhanced `print_summary()` with history metrics output

### Session 23.2: FT.SEARCH Implementation
- `src/search.rs` query parser module for RediSearch syntax
- Query translation: AND/OR/NOT, phrases, prefix, field-scoped
- Numeric range queries (@field:[min max])
- Tag exact match queries (@field:{tag1|tag2})
- FT.SEARCH with NOCONTENT, VERBATIM, WITHSCORES, LIMIT, SORTBY, RETURN
- In-memory text matching fallback for unindexed documents
- 26 new tests (14 query parser + 12 ft_search integration)

### Session 23.1: RediSearch Index Management
- FT.CREATE, FT.DROPINDEX, FT._LIST, FT.INFO, FT.ALTER
- FT.ALIASADD/DEL/UPDATE for index aliases
- FT.SYNUPDATE/DUMP for synonym groups
- FT.SUGADD/GET/DEL/LEN for autocomplete suggestions
- Schema support: TEXT, NUMERIC, TAG field types
- 22 comprehensive unit tests

### Session 23: Per-Connection State & CLIENT Commands
- CLIENT LIST with TYPE/ID filters
- CLIENT INFO, CLIENT KILL, CLIENT PAUSE/UNPAUSE
- Connection lifecycle management with ConnectionPool

### Session 22: Redis Ecosystem Compatibility
- `--password` flag and AUTH command
- `--backend` (sqlite/turso), `--storage` (file/memory) flags
- WATCH/UNWATCH optimistic locking with version tracking
- CLIENT SETNAME/GETNAME/LIST/ID
- LREM, LINSERT list operations
- SMOVE, SDIFFSTORE, SINTERSTORE, SUNIONSTORE set operations

### Session 18: Performance & Cache Configuration
- `redlite-bench` benchmarking suite (35+ scenarios)
- `--cache` flag for SQLite page cache tuning
- `Db::open_with_cache()` and `db.set_cache_mb()` API

### Session 17: History Tracking & Time-Travel
- Three-tier opt-in (global, database, key)
- HISTORY ENABLE/DISABLE/GET/GETAT/STATS/CLEAR/PRUNE/LIST
- MessagePack serialization for efficient storage
- Configurable retention (time-based, count-based)

### Session 16: Transactions
- MULTI/EXEC/DISCARD command batching
- WATCH/UNWATCH optimistic locking
- Command queueing with validation

### Session 15: Blocking & Pub/Sub (Server Mode)
- BLPOP, BRPOP blocking list operations
- XREAD BLOCK, XREADGROUP BLOCK stream operations
- SUBSCRIBE/UNSUBSCRIBE/PUBLISH/PSUBSCRIBE/PUNSUBSCRIBE
- Tokio broadcast channels for notifications

### Session 14: Stream Consumer Groups
- XGROUP CREATE/DESTROY/SETID/CREATECONSUMER/DELCONSUMER
- XREADGROUP with consumer tracking
- XACK, XPENDING, XCLAIM
- XINFO GROUPS/CONSUMERS

### Session 13: Streams
- XADD, XLEN, XRANGE, XREVRANGE, XREAD
- XTRIM, XDEL, XINFO STREAM
- MessagePack field encoding
- Entry ID format: `{timestamp}-{seq}`

### Session 11-12: Custom Commands & Polish
- VACUUM (delete expired + SQLite VACUUM)
- KEYINFO (type, ttl, created_at, updated_at)
- AUTOVACUUM ON/OFF/INTERVAL
- Error message Redis compatibility

### Session 10: Server Operations
- SELECT (multiple databases 0-15)
- INFO, DBSIZE, FLUSHDB
- Per-connection database isolation

### Session 9: Sorted Sets
- ZADD, ZREM, ZSCORE, ZRANK, ZREVRANK
- ZRANGE, ZREVRANGE, ZRANGEBYSCORE
- ZCOUNT, ZCARD, ZINCRBY
- ZREMRANGEBYRANK, ZREMRANGEBYSCORE

### Session 8: Sets
- SADD, SREM, SMEMBERS, SISMEMBER, SCARD
- SPOP, SRANDMEMBER (with count)
- SDIFF, SINTER, SUNION

### Session 7: Lists
- LPUSH, RPUSH, LPOP, RPOP (with count)
- LLEN, LRANGE, LINDEX, LSET, LTRIM
- Integer gap positioning for O(1) operations

### Session 6: Hashes
- HSET, HGET, HMGET, HGETALL
- HDEL, HEXISTS, HKEYS, HVALS, HLEN
- HINCRBY, HINCRBYFLOAT, HSETNX

### Session 3: String Operations
- INCR, DECR, INCRBY, DECRBY, INCRBYFLOAT
- MGET, MSET, APPEND, STRLEN
- GETRANGE, SETRANGE

### Session 2: Key Management
- DEL, EXISTS, TYPE
- KEYS (glob pattern), SCAN (with MATCH, COUNT)
- TTL, PTTL, EXPIRE, PERSIST

### Session 1: Foundation
- GET, SET (with EX/PX/NX/XX)
- RESP protocol parser
- TCP server mode
- Lazy expiration
- SQLite schema with WAL mode
