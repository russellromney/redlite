# redlite-oracle

**Redis Compatibility Oracle Tests**

Verify that Redlite behaves identically to Redis for all supported commands.

## Purpose

Catch implementation bugs by comparing Redlite's behavior against a real Redis instance. Perfect for validating LLM-generated code or catching regressions.

## Quick Start

```bash
# Start Redis (required)
docker run -d -p 6379:6379 redis
# Or: redis-server &

# Run all oracle tests (must be sequential)
cd redlite-oracle
cargo test -- --test-threads=1

# Run specific test categories
cargo test oracle_strings -- --test-threads=1
cargo test oracle_lists -- --test-threads=1
cargo test oracle_hashes -- --test-threads=1
```

## Test Coverage

**Total: 241 comprehensive tests** across 8 Redis data types:

1. **Strings** (14 tests) - SET, GET, INCR, DECR, APPEND, GETRANGE, SETRANGE, MGET, MSET, GETEX, GETDEL, SETBIT, GETBIT, BITCOUNT, BITPOS, BITOP
2. **Lists** (8 tests) - LPUSH, RPUSH, LPOP, RPOP, LRANGE, LINDEX, LSET, LREM, LTRIM, LINSERT, LPUSHX, RPUSHX, LLEN
3. **Hashes** (6 tests) - HSET, HGET, HDEL, HMSET, HMGET, HGETALL, HKEYS, HVALS, HLEN, HEXISTS, HINCRBY, HINCRBYFLOAT, HSETNX
4. **Sets** (6 tests) - SADD, SREM, SMEMBERS, SISMEMBER, SCARD, SINTER, SUNION, SDIFF, SINTERSTORE, SUNIONSTORE, SDIFFSTORE, SMOVE
5. **Sorted Sets** (7 tests) - ZADD, ZREM, ZSCORE, ZRANGE, ZREVRANGE, ZRANGEBYSCORE, ZRANK, ZREVRANK, ZCOUNT, ZCARD, ZINCRBY, ZREMRANGEBYRANK, ZREMRANGEBYSCORE
6. **Keys** (10 tests) - EXISTS, DEL, TYPE, EXPIRE, PEXPIRE, TTL, PTL, EXPIREAT, PEXPIREAT, RENAME, RENAMENX, SCAN
7. **Streams** (7 tests) - XADD, XREAD, XRANGE, XREVRANGE, XLEN, XTRIM, XDEL, XINFO STREAM
8. **Bitmaps** (1 test) - BITOP

See [COVERAGE.md](./COVERAGE.md) for detailed command coverage and gaps.

## How It Works

Each test:
1. Executes same command on both Redlite and Redis
2. Compares outputs byte-for-byte
3. Reports any divergence with clear diff

Example test:
```rust
#[test]
fn oracle_strings_set_get() {
    let mut redis = require_redis!();
    let redlite = Db::open_memory().unwrap();

    redlite.set("key", b"value", None).unwrap();
    redis.set("key", b"value").unwrap();

    let redlite_val = redlite.get("key").unwrap();
    let redis_val: Option<Vec<u8>> = redis.get("key").unwrap();

    assert_eq!(redlite_val, redis_val, "Mismatch for key");
}
```

## Output Format

Clear, actionable error messages for LLM fixes:

```
FAIL oracle_strings_incr
  Expected (Redis): 42
  Actual (Redlite): 41
  Key: counter
  Command: INCR

  Location: tests/oracle.rs:85
```

## Requirements

- **Redis instance** running at `localhost:6379`
- Or set `REDIS_URL` environment variable:
  ```bash
  REDIS_URL=redis://myhost:6380 cargo test -- --test-threads=1
  ```

## Why Sequential Execution?

Tests share Redis state for efficiency. Run with `--test-threads=1` to avoid conflicts.

## CI Integration

```yaml
# .github/workflows/oracle.yml
- name: Start Redis
  run: docker run -d -p 6379:6379 redis

- name: Oracle Tests
  run: |
    cd redlite-oracle
    cargo test -- --test-threads=1
```

## When to Run

- **On every PR** - catch regressions immediately
- **After LLM code changes** - verify correctness
- **Before release** - ensure Redis compatibility

## Zero Divergences Policy

All tests must pass with **zero divergences**. Any mismatch indicates a bug in Redlite's implementation.

## SDK Oracle Testing

Each language SDK should have its own oracle tests following the same pattern.

**Rust (this suite):** The canonical reference - 241 tests covering all 8 data types.

**Other SDKs:** Create `sdks/<sdk-name>/tests/oracle_test.<ext>` following this pattern:
1. Start Redis and Redlite servers
2. Execute same command on both
3. Assert outputs match byte-for-byte
4. Report any divergences

See [Adding a New SDK Oracle Suite](#adding-a-new-sdk-oracle-suite) below for implementation templates.

**Why SDK-level oracle tests?**
- Verify SDK correctly implements Redis protocol
- Catch SDK-specific bugs (serialization, parsing, etc.)
- Test that redlite server matches Redis behavior
- Validate LLM-generated SDK code works correctly

## Adding a New SDK Oracle Suite

### Quick Start

Create `sdks/<sdk-name>/tests/oracle_test.<ext>` following this pattern:

1. **Setup**: Connect to both Redis (6379) and Redlite (6380)
2. **Execute**: Run same command on both servers
3. **Assert**: Outputs must match byte-for-byte
4. **Report**: Clear error messages on divergence

### Language Examples

<details>
<summary><strong>Python Example</strong></summary>

```python
import pytest
import redis

@pytest.fixture
def redis_client():
    """Connect to Redis server"""
    client = redis.Redis(host='localhost', port=6379, decode_responses=False)
    client.flushdb()
    return client

@pytest.fixture
def redlite_client():
    """Connect to Redlite server"""
    client = redis.Redis(host='localhost', port=6380, decode_responses=False)
    client.flushdb()
    return client

def test_oracle_strings_set_get(redis_client, redlite_client):
    """Verify SET/GET behavior matches"""
    # SET on both
    redis_client.set('key', 'value')
    redlite_client.set('key', 'value')

    # GET from both
    redis_val = redis_client.get('key')
    redlite_val = redlite_client.get('key')

    # Must match exactly
    assert redis_val == redlite_val, f"Divergence: Redis={redis_val}, Redlite={redlite_val}"

def test_oracle_lists_push_pop(redis_client, redlite_client):
    """Verify list operations match"""
    # LPUSH
    redis_len = redis_client.lpush('list', 'a', 'b', 'c')
    redlite_len = redlite_client.lpush('list', 'a', 'b', 'c')
    assert redis_len == redlite_len

    # LRANGE
    redis_vals = redis_client.lrange('list', 0, -1)
    redlite_vals = redlite_client.lrange('list', 0, -1)
    assert redis_vals == redlite_vals

    # LPOP
    redis_val = redis_client.lpop('list')
    redlite_val = redlite_client.lpop('list')
    assert redis_val == redlite_val
```

**Run:** `pytest tests/oracle_test.py`
</details>

<details>
<summary><strong>TypeScript/JavaScript Example</strong></summary>

```typescript
import Redis from 'ioredis';
import { describe, it, beforeEach, expect } from 'vitest';

describe('Oracle Tests', () => {
  let redis: Redis;
  let redlite: Redis;

  beforeEach(async () => {
    redis = new Redis({ host: 'localhost', port: 6379 });
    redlite = new Redis({ host: 'localhost', port: 6380 });
    await redis.flushdb();
    await redlite.flushdb();
  });

  it('oracle: strings SET/GET', async () => {
    await redis.set('key', 'value');
    await redlite.set('key', 'value');

    const redisVal = await redis.get('key');
    const redliteVal = await redlite.get('key');

    expect(redisVal).toBe(redliteVal);
  });

  it('oracle: hashes HSET/HGET', async () => {
    await redis.hset('hash', 'field', 'value');
    await redlite.hset('hash', 'field', 'value');

    const redisVal = await redis.hget('hash', 'field');
    const redliteVal = await redlite.hget('hash', 'field');

    expect(redisVal).toBe(redliteVal);
  });
});
```

**Run:** `vitest tests/oracle_test.ts`
</details>

<details>
<summary><strong>Go Example</strong></summary>

```go
package tests

import (
    "testing"
    "github.com/go-redis/redis/v8"
    "context"
)

func setupRedis(t *testing.T) *redis.Client {
    client := redis.NewClient(&redis.Options{
        Addr: "localhost:6379",
    })
    client.FlushDB(context.Background())
    return client
}

func setupRedlite(t *testing.T) *redis.Client {
    client := redis.NewClient(&redis.Options{
        Addr: "localhost:6380",
    })
    client.FlushDB(context.Background())
    return client
}

func TestOracleStringsSETGET(t *testing.T) {
    ctx := context.Background()
    redisClient := setupRedis(t)
    redliteClient := setupRedlite(t)

    // SET on both
    redisClient.Set(ctx, "key", "value", 0)
    redliteClient.Set(ctx, "key", "value", 0)

    // GET from both
    redisVal, _ := redisClient.Get(ctx, "key").Result()
    redliteVal, _ := redliteClient.Get(ctx, "key").Result()

    if redisVal != redliteVal {
        t.Errorf("Divergence: Redis=%s, Redlite=%s", redisVal, redliteVal)
    }
}
```

**Run:** `go test -v ./tests/oracle_test.go`
</details>

### Prerequisites

```bash
# Terminal 1: Start Redis
redis-server --port 6379 &

# Terminal 2: Start Redlite
redlite --port 6380 --storage memory &
```

### Minimum Test Coverage

Each SDK should test these core commands at minimum:

**Required:**
- [ ] Strings: SET, GET, INCR, APPEND, MGET, MSET
- [ ] Lists: LPUSH, RPUSH, LPOP, RPOP, LRANGE
- [ ] Hashes: HSET, HGET, HMSET, HGETALL
- [ ] Sets: SADD, SREM, SMEMBERS, SINTER
- [ ] Sorted Sets: ZADD, ZRANGE, ZSCORE, ZRANK
- [ ] Keys: EXISTS, DEL, EXPIRE, TTL

**Recommended:**
- [ ] Streams: XADD, XREAD, XRANGE
- [ ] Bitmaps: SETBIT, GETBIT, BITCOUNT
- [ ] All commands from your SDK's API surface

See [COVERAGE.md](./COVERAGE.md) for the full command list tested in the Rust suite.

### Best Practices

1. **Flush between tests** - Use FLUSHDB to ensure clean state
2. **Test edge cases** - Empty strings, large values, special characters, binary data
3. **Binary safety** - Test with non-UTF8 bytes, null bytes
4. **Deterministic order** - Sort unordered results (e.g., SMEMBERS) before comparing
5. **Exact comparison** - Compare byte-for-byte, not semantic equivalence
6. **Clear errors** - Report command, expected output, actual output on failure

### CI Integration

```yaml
# .github/workflows/oracle.yml
name: SDK Oracle Tests

on: [push, pull_request]

jobs:
  oracle:
    runs-on: ubuntu-latest
    services:
      redis:
        image: redis:7
        ports:
          - 6379:6379

    steps:
      - uses: actions/checkout@v4

      - name: Build Redlite
        run: cargo build --release

      - name: Start Redlite
        run: ./target/release/redlite --port 6380 --storage memory &

      - name: Setup <language>
        uses: ...

      - name: Run Oracle Tests
        run: <test-command>
```

### FAQ

**Q: Do I need to test every command?**
A: Test at minimum the core commands your SDK exposes. More coverage = better bug detection.

**Q: Should I test server-only commands like BLPOP?**
A: No - blocking operations are tested separately in the DST suite.

**Q: What if Redis and Redlite diverge for valid reasons?**
A: Document the divergence and skip that test. This should be rare - redlite aims for exact compatibility.

**Q: Can I run tests in parallel?**
A: No - tests share Redis/Redlite state. Run sequentially.

**Q: How do I debug a failing test?**
A: Check the error message for expected vs actual output. Use `redis-cli` to manually verify behavior on both servers.
