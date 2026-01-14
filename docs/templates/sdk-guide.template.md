---
# SDK Documentation Template
# LLM observes SDK code and generates real docs from this structure
---

# {{LANGUAGE}} SDK

> **Redis API + SQLite durability = 🔥**
>
> Embedded Redis-compatible database. No server needed. No Docker. No bullshit.
> Just `import` and go.

## Why redlite?

**You want Redis. You don't want another server to manage.**

```{{LANGUAGE}}
db = open(":memory:")
# OR
db = open("redlite.db")

# then do Redis commands like normal. 
# That's it. You now have Redis.
# In file mode, you have a durable cache now! 
db.set("key", "value")
db.lpush("queue", ["job1", "job2"])
db.hset("user:1", {"name": "Alice"})
```

**What you get:**
- ✅ **Files** - No worries about memory usage
- ✅ **Performance** - Use in-memory SQLite cache + file to match or exceed normal Redis read performance for many commands; writes are fast too
- ✅ **Redis-compatible** - Your Redis knowledge still works
- ✅ **SQLite storage** - ACID transactions, not memory-only (in file mode)
- ✅ **Single file** - `data.db` is your entire database
- ✅ **Crash-safe** - SQLite's battle-tested durability
- ✅ **No extra process** - You don't need supervisor, systemd etc. to run another process
- ✅ **Bonus features** - Full-text search, vectors, history (free!)
- ✅ **SQLite ecosystem** - do replication with existing SQLite tooling

**Perfect for:**
- CLI/Desktop/mobile apps that need data
- Memory-constrained environments
- Serverless functions with state
- Cache testing without Docker or new ports open
- Prototypes that become production
- Per-user or per-tenant caches
- High-cost cache misses
- Cost-constraints

## Installation

```{{PACKAGE_MANAGER}}
{{INSTALLATION_COMMAND}}
```

Binary included. No external dependencies. Works offline.

## Quick Start

### The 30-Second Demo

```{{LANGUAGE}}
// Open a database (file or memory)
db = open("app.db")  // or ":memory:" for non-druable

// Use Redis commands - all of them
db.set("session:abc", "user123", ex=3600)
db.lpush("tasks", ["email", "notify"])
db.hset("user:1", {"name": "Alice", "points": "100"})
db.zadd("scores", {"player1": 100, "player2": 200})

// That's it. You have Redis.
db.close()
```

### Production Example

```{{LANGUAGE}}
// Real-world CLI tool with embedded cache

cache = open("~/.myapp/cache.db")

def get_user(id):
    # Try cache first
    cached = cache.get(f"user:{id}")
    if cached:
        return json.loads(cached)

    # Fetch from API
    user = api.fetch_user(id)

    # Cache for 1 hour
    cache.set(f"user:{id}", json.dumps(user), ex=3600)
    return user

// No Redis server. No Docker. Just works.
```

## All Redis Commands Work

Seriously, almost all of them:

```{{LANGUAGE}}
// Strings
db.set("key", "value")
db.get("key")
db.incr("counter")
db.setex("temp", 60, "data")

// Lists (perfect for queues)
db.lpush("jobs", ["job1", "job2"])
db.rpop("jobs")
db.lrange("jobs", 0, -1)

// Hashes (perfect for objects)
db.hset("user:1", {"name": "Alice", "age": "30"})
db.hgetall("user:1")
db.hincrby("user:1", "points", 10)

// Sets
db.sadd("tags", ["redis", "database"])
db.smembers("tags")
db.sinter("tags1", "tags2")

// Sorted Sets (leaderboards!)
db.zadd("scores", {"alice": 100, "bob": 200})
db.zrange("scores", 0, -1, withscores=true)
db.zrevrange("scores", 0, 9)  // Top 10

// Pipelines (batch operations)
pipe = db.pipeline()
pipe.incr("views")
pipe.lpush("recent", [page])
pipe.execute()
```

| Note: some complex features only work in server mode. E.g.: Pub/Sub, Stream consumer groups. This should be self-evident

## Bonus: Features Redis Doesn't Have

### Full-Text Search (Built-in!)

```{{LANGUAGE}}
// Enable search on your data
db.fts.enable(global=true)

// Add documents
db.set("doc:1", "The quick brown fox")
db.set("doc:2", "jumps over the lazy dog")

// Search with ranking and highlights
results = db.fts.search("quick fox", limit=10, highlight=true)
// => [{"key": "doc:1", "score": 0.85, "snippet": "The <b>quick</b> brown <b>fox</b>"}]

// No Elasticsearch. No Algolia. Just works.
```

### History / Time Travel

```{{LANGUAGE}}
// Enable version tracking
db.history.enable(pattern="important:*")

// Make changes
db.set("important:config", "v1")
db.set("important:config", "v2")
db.set("important:config", "v3")

// View history
versions = db.history.list("important:config")
// => [v1, v2, v3] with timestamps

// Get old version
old_value = db.history.get("important:config", version=1)
// => "v1"

// Undo mistakes. Time travel debugging. For free.
```

### Vector Search

```{{LANGUAGE}}
// Enable vector similarity search
db.vector.enable(dimensions=128)

// Add embeddings
db.vector.add("docs", "doc1", embedding_array, metadata={"title": "..."})

// Find similar items
similar = db.vector.search("docs", query_embedding, k=5)
// => top 5 most similar vectors

// No Pinecone. No Weaviate. Just works.
```

## Real-World Use Cases

### CLI Tool with Persistent Cache

```{{LANGUAGE}}
// Your CLI tool now has fast, persistent cache

import os
cache = open(os.path.expanduser("~/.myapp/cache.db"))

@cache_decorator(ttl=3600)
def expensive_api_call(param):
    return api.fetch(param)

// Works offline. Survives restarts. Zero config.
```

### Desktop App Data Storage

```{{LANGUAGE}}
// Electron/Tauri app with embedded database

db = open(app.getPath("userData") + "/app.db")

// Save user settings
db.hset("settings", {"theme": "dark", "lang": "en"})

// Recent files list
db.lpush("recent_files", ["/path/to/file"])

// No SQLite boilerplate. Just Redis commands.
```

### Serverless Function State

```{{LANGUAGE}}
// AWS Lambda / Cloudflare Workers with state

db = open("/tmp/state.db")  // Lambda /tmp is persistent

// Rate limiting
count = db.incr(f"rate:{ip}")
if count > 100:
    return "Too many requests"

// Session storage
db.hset(f"session:{id}", user_data)
db.expire(f"session:{id}", 3600)

// No ElastiCache. No DynamoDB. Just works.
```

### Testing Without Docker

```{{LANGUAGE}}
// Integration tests with real Redis commands

def test_user_flow():
    db = open(":memory:")  // Fresh DB per test

    // Your real code that uses Redis
    user_service = UserService(db)
    user = user_service.create("alice")

    assert db.exists(f"user:{user.id}")

// No docker-compose up. No waiting. Fast tests.
```

## Server Mode (Optional)

Don't want embedded? Need server features? Run as a server, either as a file or in memory (like redis):

```bash
# Start server
redlite --db data.db --addr 127.0.0.1:6379 # file
redlite --db :memory: --addr 127.0.0.1:6379 # memory

# Connect from any Redis client
redis-cli -p 6379
```

```{{LANGUAGE}}
// Connect to server instead
db = connect("localhost:6379")

// Same API. Pick what works for you.
```

## Configuration

```{{LANGUAGE}}
// Tweak performance
db = open("data.db",
    cache_mb=1024,        // 1GB cache (default: 64MB)
    binary_path="...",    // Custom binary location
    startup_timeout=10    // Server startup timeout
)
```

## Framework Integration

### {{LANGUAGE_FRAMEWORK}} Example

```{{LANGUAGE}}
// Real framework example for this language
// FastAPI for Python, Express for Node, Axum for Rust, etc.

app = create_app()
db = open("cache.db")

@app.get("/items/{id}")
async def get_item(id):
    # Check cache
    cached = db.get(f"item:{id}")
    if cached:
        return json.loads(cached)

    # Fetch from database
    item = database.fetch(id)

    # Cache for 5 minutes
    db.set(f"item:{id}", json.dumps(item), ex=300)
    return item

// Instant performance boost. No Redis cluster needed.
```

## Best Practices

1. **Use `:memory:` for tests** - Fast, isolated, deterministic
2. **Use file for CLI/desktop** - Persistent across restarts
3. **Use pipelines for batches** - 10x faster for bulk operations
4. **Set appropriate cache** - More cache = faster reads
5. **Reuse connections** - Open once, use many times

## Migration from Redis

Already using Redis? Drop-in replacement:

```{{LANGUAGE}}
// Before: Redis client
-redis = Redis("redis://prod-server:6379")
+db = open("data.db")  // Same API below

redis.set("key", "value")
redis.lpush("queue", ["job"])
// ... all commands work identically
```

**Differences:**
- ✅ No network = faster
- ✅ No server = simpler
- ✅ SQLite = more durable
- ⚠️ Single-process only (use server mode for multi-process)

## Why Not Just Use Redis?

**Redis is great! But:**
- Requires a server process
- Memory-only (persistence is async)
- Can't bundle with your app
- Overkill for dev/test/CLI

**Why Not Just Use SQLite?**
- Requires learning SQL
- No Redis commands (LIST, HASH, SORTED SET, etc.)
- No pub/sub, no pipelines
- Different mental model

**redlite = Best of both worlds**

## API Reference

See [{{LANGUAGE}} API Reference](/reference/{{LANGUAGE}}-api) for complete documentation.

## Source & Support

- [GitHub]({{REPO_URL}})
- [Package]({{PACKAGE_URL}})
- [Issues](https://github.com/russellromney/redlite/issues)
- [Discussions](https://github.com/russellromney/redlite/discussions)

---

**Built with Claude Code** 🤖 | MIT License
