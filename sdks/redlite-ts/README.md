# Redlite TypeScript SDK

TypeScript SDK for Redlite - Redis API + SQLite durability.

## Install

```bash
npm install redlite

# With server mode support
npm install redlite ioredis
```

## Usage

```typescript
import { Redlite } from 'redlite';

// Auto-detects mode from URL
const db = Redlite.create(':memory:');           // Embedded (FFI)
const db = Redlite.create('/path/to/db.db');     // Embedded file
const db = await Redlite.createAsync('redis://localhost:6379'); // Server mode

// Same API either way
db.set('key', 'value');
db.get('key'); // Buffer('value')

// Check mode
db.mode; // 'embedded' or 'server'

db.close();
```

### All Data Types

```typescript
const db = Redlite.create(':memory:');

// Strings
db.set('hello', 'world');
db.get('hello'); // Buffer('world')
db.incr('counter');

// Hashes
db.hset('user:1', { name: 'Alice', age: '30' });
db.hget('user:1', 'name'); // Buffer('Alice')

// Lists
db.lpush('queue', 'job1', 'job2');
db.rpop('queue'); // Buffer('job1')

// Sets
db.sadd('tags', 'redis', 'sqlite');
db.smembers('tags'); // Set<Buffer>

// Sorted Sets
db.zadd('scores', { alice: 100, bob: 95 });
db.zscore('scores', 'alice'); // 100

db.close();
```

### Redlite-Specific Features

```typescript
// Full-text search
db.fts.create('idx', { title: 'TEXT' }, { prefix: 'doc:' });
db.fts.search('idx', 'hello world');

// Vector search
db.vector.add('embeddings', 'doc1', [0.1, 0.2, 0.3]);
db.vector.sim('embeddings', [0.1, 0.2, 0.3], { count: 5 });

// Geospatial
db.geo.add('locations', [-122.4, 37.8, 'sf']);
db.geo.search('locations', -122.4, 37.8, 100, { unit: 'km' });
```

## Build

Requires the native library for embedded mode:

```bash
# Build FFI library
cd crates/redlite-ffi && cargo build --release

# Set library path
export REDLITE_LIB_PATH=/path/to/target/release/libredlite_ffi.dylib
```

## Test

```bash
npm test
```
