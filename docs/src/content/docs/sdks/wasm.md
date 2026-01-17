---
title: WebAssembly SDK
description: Redlite SDK for browsers via WebAssembly
---

Experimental WebAssembly bindings for browser environments.

## Status

**Experimental** - API may change.

## Installation

```bash
npm install @redlite/wasm
```

## Quick Start

```javascript
import init, { Redlite } from '@redlite/wasm';

async function main() {
  await init();

  // Create in-memory database
  const db = new Redlite();

  // String operations
  db.set('key', 'value');
  const val = db.get('key');
  console.log(val);  // "value"

  // Hash operations
  db.hset('user:1', 'name', 'Alice');
  const name = db.hget('user:1', 'name');

  // List operations
  db.lpush('queue', 'job1', 'job2');
  const job = db.rpop('queue');

  db.close();
}

main();
```

## Limitations

- In-memory only (no persistence in browser)
- Single-threaded
- Limited to ~2GB memory (WASM limitation)

## Building from Source

```bash
cd sdks/redlite-wasm
wasm-pack build --target web
```

## Links

- [Source Code](https://github.com/russellromney/redlite/tree/main/sdks/redlite-wasm)
