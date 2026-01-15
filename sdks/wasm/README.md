# WASM SDK

> **Redis API + SQLite durability = embedded database for browsers**
>
> Embedded Redis-compatible database. No server needed. No backend. No bullshit.
> Just `import` and go.

## Why redlite?

**You want Redis in the browser. You don't want a server.**

```typescript
import { Redlite } from 'redlite';

const db = await Redlite.open();

// Then do Redis commands like normal.
// That's it. You now have Redis in the browser.
db.set('key', 'value');
db.lpush('queue', 'job1', 'job2');
db.hset('user:1', { name: 'Alice' });
```

**What you get:**
- ✅ **Browser-native** - Runs entirely in WebAssembly
- ✅ **No server** - No backend required, works offline
- ✅ **Persistable** - Export to localStorage, IndexedDB, or files
- ✅ **Redis-compatible** - Your Redis knowledge still works
- ✅ **SQLite storage** - ACID transactions under the hood
- ✅ **Bonus features** - Full-text search, vectors, history (free!)

**Perfect for:**
- Offline-first web apps
- Browser-based data storage
- Local-first applications
- Prototypes that work without a backend
- PWAs with persistent state
- Browser extensions

## Installation

```bash
npm install redlite
```

Works in browsers and Node.js. Uses sql.js (SQLite compiled to WebAssembly).

## Quick Start

### The 30-Second Demo

```typescript
import { Redlite } from 'redlite';

// Open a database (in-memory)
const db = await Redlite.open();

// Use Redis commands - all of them
db.set('session:abc', 'user123', { ex: 3600 });
db.lpush('tasks', 'email', 'notify');
db.hset('user:1', { name: 'Alice', points: '100' });
db.zadd('scores', { player1: 100, player2: 200 });

// That's it. You have Redis in the browser.

// Save to localStorage
const data = db.export();
localStorage.setItem('redlite', btoa(String.fromCharCode(...data)));
```

### Persistence Example

```typescript
import { Redlite } from 'redlite';

// Load from localStorage if exists
function loadDatabase(): Promise<Redlite> {
  const saved = localStorage.getItem('redlite');
  if (saved) {
    const data = Uint8Array.from(atob(saved), c => c.charCodeAt(0));
    return Redlite.open(data);
  }
  return Redlite.open();
}

// Save on changes
function saveDatabase(db: Redlite) {
  const data = db.export();
  localStorage.setItem('redlite', btoa(String.fromCharCode(...data)));
}

// Usage
const db = await loadDatabase();
db.set('user:name', 'Alice');
saveDatabase(db);
```

## All Redis Commands Work

Seriously, almost all of them:

```typescript
// Strings
db.set('key', 'value');
db.get('key');
db.incr('counter');
db.setex('temp', 60, 'data');

// Lists (perfect for queues)
db.lpush('jobs', 'job1', 'job2');
db.rpop('jobs');
db.lrange('jobs', 0, -1);

// Hashes (perfect for objects)
db.hset('user:1', { name: 'Alice', age: '30' });
db.hgetall('user:1');
db.hincrby('user:1', 'points', 10);

// Sets
db.sadd('tags', 'redis', 'database');
db.smembers('tags');
db.sinter('tags1', 'tags2');

// Sorted Sets (leaderboards!)
db.zadd('scores', { alice: 100, bob: 200 });
db.zrange('scores', 0, -1, { withscores: true });
```

## Bonus: Features Redis Doesn't Have

### Full-Text Search (Built-in!)

```typescript
// Enable search on your data
db.fts.enable({ global: true });

// Add documents
db.set('doc:1', 'The quick brown fox');
db.set('doc:2', 'jumps over the lazy dog');

// Search with ranking and highlights
const results = db.fts.search('quick fox', { limit: 10, highlight: true });
// => [{ key: 'doc:1', rank: 0.85, snippet: 'The <b>quick</b> brown <b>fox</b>' }]

// No Elasticsearch. No Algolia. Just works.
```

### History / Time Travel

```typescript
// Enable version tracking
db.history.enable({ global: true, retention: { type: 'count', value: 100 } });

// Make changes
db.set('important:config', 'v1');
db.set('important:config', 'v2');
db.set('important:config', 'v3');

// View history
const versions = db.history.list('important:config');
// => [{ version: 3, operation: 'SET', ... }, { version: 2, ... }, ...]

// Get old version
const oldEntry = db.history.version('important:config', 1);
// => { version: 1, data: 'v1', ... }

// Undo mistakes. Time travel debugging. For free.
```

### Vector Search

```typescript
// Add embeddings
db.vector.add('docs', 'doc1', [0.1, 0.2, 0.3, ...], { attributes: { title: '...' } });

// Find similar items
const similar = db.vector.search('docs', queryEmbedding, { count: 5 });
// => [{ element: 'doc1', distance: 0.05, attributes: {...} }, ...]

// No Pinecone. No Weaviate. Just works.
```

## Real-World Use Cases

### Offline-First Web App

```typescript
import { Redlite } from 'redlite';

class OfflineStore {
  private db: Redlite | null = null;

  async init() {
    // Load from IndexedDB
    const data = await this.loadFromIndexedDB();
    this.db = await Redlite.open(data);
  }

  async sync() {
    // Save to IndexedDB
    const data = this.db!.export();
    await this.saveToIndexedDB(data);
  }

  // Your Redis-like API
  get(key: string) { return this.db!.get(key); }
  set(key: string, value: string) { this.db!.set(key, value); }
  // ...
}
```

### Browser Extension Storage

```typescript
// popup.ts
const db = await Redlite.open();

// Store settings
db.hset('settings', { theme: 'dark', notifications: 'on' });

// Recent items
db.lpush('recent', window.location.href);
db.ltrim('recent', 0, 99); // Keep last 100

// Save to chrome.storage
chrome.storage.local.set({
  redlite: Array.from(db.export())
});
```

### Local-First Todo App

```typescript
import { Redlite } from 'redlite';

const db = await Redlite.open();

// Add todo
function addTodo(text: string) {
  const id = crypto.randomUUID();
  db.hset(`todo:${id}`, {
    text,
    done: 'false',
    created: Date.now().toString()
  });
  db.lpush('todos:list', id);
  return id;
}

// Get all todos
function getTodos() {
  const ids = db.lrange('todos:list', 0, -1);
  return ids.map(id => ({
    id,
    ...db.hgetall(`todo:${id}`)
  }));
}

// Toggle complete
function toggleTodo(id: string) {
  const current = db.hget(`todo:${id}`, 'done');
  db.hset(`todo:${id}`, 'done', current === 'true' ? 'false' : 'true');
}
```

## Framework Integration

### React Hook

```typescript
import { useState, useEffect } from 'react';
import { Redlite } from 'redlite';

let dbPromise: Promise<Redlite> | null = null;

export function useRedlite() {
  const [db, setDb] = useState<Redlite | null>(null);

  useEffect(() => {
    if (!dbPromise) {
      dbPromise = Redlite.open();
    }
    dbPromise.then(setDb);
  }, []);

  return db;
}

// Usage
function MyComponent() {
  const db = useRedlite();

  if (!db) return <div>Loading...</div>;

  const count = db.get('counter') ?? '0';
  return (
    <button onClick={() => db.set('counter', (parseInt(count) + 1).toString())}>
      Count: {count}
    </button>
  );
}
```

### Vue Composable

```typescript
import { ref, onMounted } from 'vue';
import { Redlite } from 'redlite';

export function useRedlite() {
  const db = ref<Redlite | null>(null);

  onMounted(async () => {
    db.value = await Redlite.open();
  });

  return db;
}
```

## Best Practices

1. **Load once, use everywhere** - Open database on app init
2. **Save periodically** - Export and persist to storage
3. **Use keys wisely** - Same patterns as Redis (user:1, session:abc)
4. **Enable FTS for search** - Much faster than filtering
5. **Use history for undo** - Built-in version tracking

## API Reference

### Redlite Class

| Method | Description |
|--------|-------------|
| `Redlite.open(data?)` | Open database (optionally with existing data) |
| `db.export()` | Export database as Uint8Array |
| `db.close()` | Close the database |

### Namespaces

| Namespace | Description |
|-----------|-------------|
| `db.fts` | Full-text search |
| `db.history` | Version history tracking |
| `db.vector` | Vector similarity search |
| `db.geo` | Geospatial commands |

### Supported Commands (163 total)

**Strings:** GET, SET, APPEND, GETRANGE, SETRANGE, STRLEN, GETEX, GETDEL, SETEX, PSETEX, SETNX, MGET, MSET, INCR, INCRBY, INCRBYFLOAT, DECR, DECRBY

**Bits:** GETBIT, SETBIT, BITCOUNT, BITOP

**Keys:** DEL, EXISTS, KEYS, TYPE, RENAME, RENAMENX, EXPIRE, EXPIREAT, PEXPIRE, PEXPIREAT, TTL, PTTL, PERSIST, SCAN, ECHO, KEYINFO

**Hashes:** HSET, HGET, HGETALL, HDEL, HEXISTS, HLEN, HKEYS, HVALS, HMGET, HSETNX, HINCRBY, HINCRBYFLOAT, HSCAN

**Lists:** LPUSH, RPUSH, LPOP, RPOP, LRANGE, LLEN, LINDEX, LINSERT, LSET, LREM, LTRIM, LPUSHX, RPUSHX, LPOS, LMOVE

**Sets:** SADD, SREM, SMEMBERS, SISMEMBER, SCARD, SPOP, SRANDMEMBER, SMOVE, SDIFF, SDIFFSTORE, SINTER, SINTERSTORE, SUNION, SUNIONSTORE, SSCAN

**Sorted Sets:** ZADD, ZREM, ZSCORE, ZRANGE, ZCARD, ZCOUNT, ZINCRBY, ZRANK, ZREVRANK, ZREVRANGE, ZRANGEBYSCORE, ZREMRANGEBYRANK, ZREMRANGEBYSCORE, ZINTERSTORE, ZUNIONSTORE, ZSCAN

**Geo:** GEOADD, GEODIST, GEOHASH, GEOPOS, GEOSEARCH, GEOSEARCHSTORE

**Server:** PING, FLUSHDB, DBSIZE, INFO, SELECT

**Redlite-specific:** VACUUM, AUTOVACUUM, KEYINFO

**Note:** Pub/Sub and blocking commands (BLPOP, BRPOP, XREAD BLOCK) are not available in embedded WASM mode.

## Why Not Just Use Redis?

**Redis is great! But:**
- Requires a server
- Can't run in browsers
- Overkill for local storage

**Why Not Just Use localStorage?**
- No data structures (just strings)
- No queries or search
- Limited to 5-10MB

**Why Not Just Use IndexedDB?**
- Complex async API
- No Redis commands
- Different mental model

**redlite = Redis API in the browser**

## Source & Support

- [GitHub](https://github.com/russellromney/redlite)
- [npm](https://www.npmjs.com/package/redlite)
- [Issues](https://github.com/russellromney/redlite/issues)
- [Discussions](https://github.com/russellromney/redlite/discussions)

---

**Built with Claude Code** | MIT License
