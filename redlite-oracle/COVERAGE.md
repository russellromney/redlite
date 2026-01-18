# Oracle Test Coverage

This document tracks which Redis commands are tested in the oracle suite.

## Current Coverage: 241 Tests

### ✅ Strings (14 tests)
- SET, GET, SETNX, SETEX, PSETEX
- INCR, DECR, INCRBY, DECRBY, INCRBYFLOAT
- APPEND, STRLEN
- GETRANGE, SETRANGE
- MGET, MSET
- GETEX, GETDEL
- Bit operations (SETBIT, GETBIT, BITCOUNT, BITPOS, BITOP)

### ✅ Lists (8 tests)
- LPUSH, RPUSH, LPOP, RPOP
- LPUSHX, RPUSHX
- LLEN, LINDEX, LSET
- LRANGE, LTRIM, LREM
- LINSERT

### ✅ Hashes (6 tests)
- HSET, HGET, HDEL
- HMSET, HMGET
- HGETALL, HKEYS, HVALS, HLEN
- HEXISTS
- HINCRBY, HINCRBYFLOAT
- HSETNX

### ✅ Sets (6 tests)
- SADD, SREM, SISMEMBER
- SMEMBERS, SCARD
- SINTER, SUNION, SDIFF
- SINTERSTORE, SUNIONSTORE, SDIFFSTORE
- SMOVE

### ✅ Sorted Sets (7 tests)
- ZADD, ZREM, ZSCORE
- ZRANGE, ZREVRANGE, ZRANGEBYSCORE
- ZRANK, ZREVRANK
- ZCOUNT, ZCARD
- ZINCRBY
- ZREMRANGEBYRANK, ZREMRANGEBYSCORE

### ✅ Keys (10 tests)
- EXISTS, DEL, TYPE
- EXPIRE, PEXPIRE, TTL, PTL
- EXPIREAT, PEXPIREAT
- RENAME, RENAMENX
- SCAN

### ✅ Streams (7 tests)
- XADD, XREAD, XRANGE, XREVRANGE
- XLEN, XTRIM, XDEL
- XINFO STREAM

### ✅ Bitmaps (1 test)
- BITOP

## Missing Coverage

These Redis commands are supported by redlite but not yet in oracle tests:

### Geo Commands (feature flag)
- [ ] GEOADD
- [ ] GEOPOS
- [ ] GEODIST
- [ ] GEOHASH
- [ ] GEOSEARCH
- [ ] GEOSEARCHSTORE

### Vector Commands (feature flag)
- [ ] VADD, VREM, VCARD, VEXISTS
- [ ] VSIM, VSIMBATCH
- [ ] VDIM, VGET, VGETALL
- [ ] VGETATTRIBUTES, VSETATTRIBUTES, VDELATTRIBUTES

### Search Commands
- [ ] FT.CREATE, FT.DROPINDEX
- [ ] FT.SEARCH, FT.AGGREGATE
- [ ] FT.INFO, FT.EXPLAIN, FT.PROFILE
- [ ] FT.ALTER
- [ ] FT.SUGADD, FT.SUGGET, FT.SUGLEN, FT.SUGDEL
- [ ] FT.SYNUPDATE, FT.SYNDUMP

### Stream Groups (not in oracle yet)
- [ ] XGROUP CREATE, XGROUP DESTROY
- [ ] XREADGROUP
- [ ] XACK, XPENDING
- [ ] XCLAIM, XAUTOCLAIM

### Additional String Commands
- [ ] GETSET
- [ ] SETRANGE edge cases

### Additional List Commands
- [ ] LMOVE, LMPOP
- [ ] BLPOP, BRPOP, BLMOVE (server-only)

### Additional Set Commands
- [ ] SRANDMEMBER, SPOP

### Additional Sorted Set Commands
- [ ] ZPOPMIN, ZPOPMAX
- [ ] ZINTERSTORE, ZUNIONSTORE
- [ ] ZMSCORE
- [ ] BZPOPMIN, BZPOPMAX (server-only)

### Additional Hash Commands
- [ ] HRANDFIELD
- [ ] HSCAN

### Additional Key Commands
- [ ] PERSIST
- [ ] EXPIRETIME, PEXPIRETIME
- [ ] COPY, UNLINK
- [ ] DUMP, RESTORE

### Server Commands
- [ ] FLUSHDB, FLUSHALL
- [ ] DBSIZE
- [ ] CONFIG GET, CONFIG SET
- [ ] INFO
- [ ] PING

## Priority for Adding Tests

**High Priority** (core commands used frequently):
1. Stream groups (XGROUP, XREADGROUP, XACK)
2. Missing set/zset commands (SRANDMEMBER, SPOP, ZPOPMIN, ZPOPMAX)
3. Missing key commands (PERSIST, COPY, UNLINK)
4. Server commands (FLUSHDB, DBSIZE, CONFIG, INFO)

**Medium Priority** (feature-flagged):
1. Search commands (FT.*)
2. Geo commands (GEO*)
3. Vector commands (V*)

**Low Priority** (edge cases, advanced features):
1. Advanced hash/list commands
2. Blocking operations (tested separately in DST)

## How to Add Tests

1. Add test function to `tests/oracle.rs`
2. Follow existing pattern:
   ```rust
   #[test]
   fn oracle_<type>_<command>() {
       let mut redis = require_redis!();
       let redlite = Db::open_memory().unwrap();
       let _: () = redis::cmd("FLUSHDB").query(&mut redis).unwrap();

       // Execute command on both
       redlite.<command>(...)?;
       redis.<command>(...)?;

       // Assert outputs match
       assert_eq!(redlite_result, redis_result);
   }
   ```
3. Run test: `cargo test oracle_<type>_<command> -- --test-threads=1`
4. Update this coverage doc

## Notes

- Oracle tests require Redis running at `localhost:6379`
- Tests must run sequentially (`--test-threads=1`) since they share Redis state
- Feature-flagged commands (geo, vectors) need feature flags enabled for testing
- Blocking operations (BLPOP, etc.) are better tested in DST suite
