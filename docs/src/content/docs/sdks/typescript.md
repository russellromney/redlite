---
title: TypeScript SDK
description: Redlite SDK for TypeScript/Node.js
---

TypeScript SDK with NAPI native bindings for Node.js applications.

## Installation

```bash
npm install redlite

# With server mode support
npm install redlite ioredis
```

## Quick Start

```typescript
import { Redlite } from 'redlite';

// Auto-detects mode from URL
const db = Redlite.create(':memory:');           // Embedded (FFI)
const db = Redlite.create('/path/to/db.db');     // Embedded file
const db = await Redlite.createAsync('redis://localhost:6379'); // Server

// Same API either way
db.set('key', 'value');
db.get('key'); // Buffer('value')

// All data types
db.hset('user:1', { name: 'Alice', age: '30' });
db.lpush('queue', 'job1', 'job2');
db.sadd('tags', 'redis', 'sqlite');
db.zadd('scores', { alice: 100, bob: 95 });

db.close();
```

## API Overview

**Strings**: `set`, `get`, `incr`, `decr`, `append`, `mget`, `mset`

**Keys**: `del`, `exists`, `type`, `ttl`, `expire`, `keys`

**Hashes**: `hset`, `hget`, `hdel`, `hgetall`, `hmget`

**Lists**: `lpush`, `rpush`, `lpop`, `rpop`, `llen`, `lrange`

**Sets**: `sadd`, `srem`, `smembers`, `sismember`, `scard`

**Sorted Sets**: `zadd`, `zrem`, `zscore`, `zrange`, `zrevrange`

**Extensions**: `fts`, `vector`, `geo`

## Testing

```bash
cd sdks/redlite-ts
npm test
```

## Links

- [Full README](https://github.com/russellromney/redlite/tree/main/sdks/redlite-ts)
- [Source Code](https://github.com/russellromney/redlite/tree/main/sdks/redlite-ts)
