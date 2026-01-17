# Redlite WASM

Redis-compatible embedded database compiled to WebAssembly. Run a full Redis API with SQLite durability directly in your browser or Node.js.

## Features

- **Redis API** - Familiar commands: GET, SET, HSET, LPUSH, SADD, ZADD, and more
- **In-Browser Database** - No server required, runs entirely client-side
- **SQLite Durability** - Built on SQLite for reliable data storage
- **TypeScript Support** - Full type definitions generated automatically
- **Lightweight** - Optimized WASM binary for fast loading

## Installation

```bash
npm install @redlite/wasm
```

Or build from source:

```bash
# Install wasm-pack if not already installed
cargo install wasm-pack

# Build for web
wasm-pack build --target web --release

# Build for Node.js
wasm-pack build --target nodejs --release
```

## Quick Start

### Browser

```javascript
import init, { RedliteWasm } from '@redlite/wasm';

async function main() {
    await init();
    const db = new RedliteWasm();

    // String commands
    db.set('name', new TextEncoder().encode('Redlite'), null);
    const name = db.get('name');
    console.log(new TextDecoder().decode(name)); // "Redlite"

    // Hash commands
    db.hset('user:1', 'email', new TextEncoder().encode('user@example.com'));

    // List commands
    db.rpush('queue', [
        new TextEncoder().encode('task1'),
        new TextEncoder().encode('task2'),
    ]);
}

main();
```

### Node.js

```javascript
import { RedliteWasm } from '@redlite/wasm';

const db = new RedliteWasm();
db.set('key', Buffer.from('value'), null);
const value = db.get('key');
console.log(value.toString()); // "value"
```

## API Reference

### String Commands

| Command | Description |
|---------|-------------|
| `get(key)` | Get the value of a key |
| `set(key, value, options?)` | Set key to hold the value |
| `del(keys[])` | Delete one or more keys |
| `exists(keys[])` | Check if keys exist |
| `incr(key)` | Increment integer value by one |
| `incrby(key, increment)` | Increment integer value |
| `decr(key)` | Decrement integer value by one |
| `decrby(key, decrement)` | Decrement integer value |
| `incrbyfloat(key, increment)` | Increment float value |
| `append(key, value)` | Append to a key |
| `strlen(key)` | Get length of value |
| `setnx(key, value)` | Set if not exists |
| `setex(key, seconds, value)` | Set with expiration |
| `psetex(key, millis, value)` | Set with ms expiration |
| `getset(key, value)` | Set and return old value |
| `mget(keys[])` | Get multiple keys |
| `mset(pairs[])` | Set multiple keys |

### Key Commands

| Command | Description |
|---------|-------------|
| `keys(pattern)` | Find keys matching pattern |
| `expire(key, seconds)` | Set TTL in seconds |
| `pexpire(key, milliseconds)` | Set TTL in milliseconds |
| `expireat(key, timestamp)` | Set expiration timestamp |
| `pexpireat(key, timestamp_ms)` | Set ms expiration timestamp |
| `ttl(key)` | Get TTL in seconds |
| `pttl(key)` | Get TTL in milliseconds |
| `persist(key)` | Remove expiration |
| `type(key)` | Get key type |
| `rename(key, newkey)` | Rename a key |
| `renamenx(key, newkey)` | Rename if new key doesn't exist |
| `dbsize()` | Number of keys in database |
| `flushdb()` | Remove all keys in current db |
| `flushall()` | Remove all keys in all dbs |
| `select(db)` | Select database (0-15) |

### Hash Commands

| Command | Description |
|---------|-------------|
| `hset(key, field, value)` | Set hash field |
| `hget(key, field)` | Get hash field value |
| `hdel(key, fields[])` | Delete hash fields |
| `hexists(key, field)` | Check if field exists |
| `hgetall(key)` | Get all fields and values |
| `hkeys(key)` | Get all field names |
| `hvals(key)` | Get all values |
| `hlen(key)` | Number of fields |
| `hincrby(key, field, increment)` | Increment field integer |
| `hincrbyfloat(key, field, increment)` | Increment field float |
| `hsetnx(key, field, value)` | Set if field doesn't exist |

### List Commands

| Command | Description |
|---------|-------------|
| `lpush(key, values[])` | Prepend elements |
| `rpush(key, values[])` | Append elements |
| `lpop(key, count?)` | Remove and get first elements |
| `rpop(key, count?)` | Remove and get last elements |
| `llen(key)` | Get list length |
| `lrange(key, start, stop)` | Get range of elements |
| `lindex(key, index)` | Get element by index |

### Set Commands

| Command | Description |
|---------|-------------|
| `sadd(key, members[])` | Add members |
| `srem(key, members[])` | Remove members |
| `smembers(key)` | Get all members |
| `sismember(key, member)` | Check membership |
| `scard(key)` | Get set size |

### Sorted Set Commands

| Command | Description |
|---------|-------------|
| `zadd(key, [score, member, ...])` | Add members with scores |
| `zrem(key, members[])` | Remove members |
| `zscore(key, member)` | Get member score |
| `zrank(key, member)` | Get member rank |
| `zrange(key, start, stop, withscores)` | Get range by rank |
| `zcard(key)` | Get sorted set size |
| `zincrby(key, increment, member)` | Increment member score |
| `zcount(key, min, max)` | Count members in score range |

## SetOptions

Configure SET behavior:

```javascript
const options = new SetOptions()
    .withEx(3600)  // Expire in 1 hour
    .withNx();     // Only set if not exists

db.set('key', value, options);
```

| Method | Description |
|--------|-------------|
| `withEx(seconds)` | Set expiration in seconds |
| `withPx(milliseconds)` | Set expiration in milliseconds |
| `withNx()` | Only set if key doesn't exist |
| `withXx()` | Only set if key exists |

## Persistence

The WASM version uses in-memory SQLite. For persistence, export the database:

```javascript
// Export not yet implemented in this version
// Use the existing sdks/wasm TypeScript SDK for export/import
```

## Limitations

- **No FTS5** - Full-text search not available in WASM build
- **No Threading** - Single-threaded only (WASM limitation)
- **Memory Limit** - 4GB max (WASM limitation)
- **No Blocking Commands** - BLPOP, BRPOP, XREAD BLOCK not available

## Building from Source

```bash
# Development build
wasm-pack build --target web --dev

# Release build
wasm-pack build --target web --release

# Run tests
wasm-pack test --headless --chrome
```

## Examples

See the `examples/` directory:

- `examples/web/index.html` - Browser demo with interactive UI
- `examples/node/index.mjs` - Node.js demo with all commands

## License

Apache-2.0
