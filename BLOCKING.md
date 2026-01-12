# Blocking Reads in Redlite

Blocking read operations are available in **server mode only** (Session 15+).

## Overview

Blocking operations allow clients to wait for data to become available on one or more keys. If data is already available, the command returns immediately. If not, the client blocks until:
1. Data becomes available (another client writes to a watched key), or
2. The timeout expires (returns null)

## Server Mode vs Embedded Mode

**Server Mode** - Full support for blocking operations:
```bash
redlite --db=data.db --addr=127.0.0.1:6379
redis-cli -p 6379 BLPOP mylist 5  # Blocks up to 5 seconds
```

**Embedded Mode** - Blocking operations return an error:
```rust
use redlite::Db;
let db = Db::open("data.db")?;
// This will error: "BLPOP is only supported in server mode"
db.blpop(&["key"], 5.0)?;
```

## Commands

### BLPOP / BRPOP

Block and pop from lists:

```bash
# Block up to 5 seconds waiting for data on key1 or key2
BLPOP key1 key2 5

# Returns: [key, value] when data available
# Returns: (nil) if timeout expires
```

**Behavior:**
- Tries all keys in order (first available wins)
- Returns `[key, value]` immediately if any key has data
- Blocks indefinitely if timeout is 0
- Returns nil if timeout expires

### XREAD BLOCK

Block and read from streams:

```bash
# Block up to 1000ms waiting for new stream entries
XREAD BLOCK 1000 STREAMS mystream $

# Or with multiple streams
XREAD BLOCK 1000 COUNT 10 STREAMS stream1 stream2 id1 id2
```

### XREADGROUP BLOCK

Block and read from streams with consumer groups:

```bash
# Block up to 5000ms for unread messages from consumer group
XREADGROUP GROUP mygroup consumer1 BLOCK 5000 STREAMS mystream >
```

## Implementation (Session 15 Roadmap)

### Session 15.1: Notification Infrastructure ✅
- [x] Add notification system to `Server` struct using `tokio::sync::broadcast`
- [x] Add optional notifier field to `Db` for server mode detection
- [x] Implement helper methods: `is_server_mode()`, `notify_key()`, `subscribe_key()`
- [x] Attach notifier in `handle_connection()` for each server connection
- [x] 7 unit tests covering all notification paths, 340 total tests passing

### Session 15.2: Broadcasting on Writes (Next)
- [ ] Make LPUSH broadcast notification after insert
- [ ] Make RPUSH broadcast notification after insert
- [ ] Make XADD broadcast notification after insert
- [ ] Implement channel cleanup (remove unused channels)
- [ ] Add integration tests with concurrent writes

### Session 15.3: Blocking Commands (After 15.2)
- [ ] Make execute_command async
- [ ] Implement BLPOP, BRPOP with timeout handling
- [ ] Implement XREAD BLOCK with timeout handling
- [ ] Implement XREADGROUP BLOCK with timeout handling
- [ ] Add comprehensive integration tests with multiple clients

## Architecture

Blocking reads use tokio's `broadcast` channels:

```rust
// Server holds a map of key → broadcast channel
notifier: Arc<RwLock<HashMap<String, broadcast::Sender<()>>>>

// Database attached to notifier in server mode (Session 15.1 ✅)
db.with_notifier(notifier);  // Enables server mode features

// When a write happens (LPUSH, XADD) — coming in Session 15.2:
db.notify_key("mykey").await;  // Wake all waiters

// When blocking (BLPOP) — coming in Session 15.3:
let mut rx = db.subscribe_key("mylist").await;
loop {
    // Try immediate pop
    if let Some(value) = db.lpop(key, None)? {
        return value;
    }
    // Wait for notification or timeout
    tokio::select! {
        _ = rx.recv() => continue,
        _ = tokio::time::sleep_until(deadline) => return nil,
    }
}
```

### Current Status (Session 15.1)
- ✅ Notification infrastructure ready
- ✅ Server mode detection via `is_server_mode()`
- ✅ Key subscription channels via `subscribe_key()`
- ✅ Notification sending via `notify_key()` (channels created lazily)
- ✅ Embedded mode safely returns closed channels
- ⏳ Blocking commands implementation (coming in 15.2-15.3)

## Edge Cases

1. **Timeout = 0** - Blocks indefinitely (infinite wait)
2. **Multiple blockers on same key** - First to pop wins, others continue waiting
3. **Key doesn't exist yet** - Blocks until key is created with data
4. **Connection drops during block** - Cleanly aborted, no issues
5. **Server shutdown** - Blocks cancelled gracefully

## Testing Strategy

- Unit tests for notification system
- Integration tests for concurrent producers/consumers
- Tests for timeout behavior
- Tests for multi-key blocking
- Manual testing with redis-cli

## Performance Notes

- Notification system has minimal overhead (only active for blocking commands)
- Channels cleaned up automatically when no longer needed
- Non-blocking operations unchanged (backward compatible)

## Future Enhancements

- Pub/Sub (similar architecture, fire-and-forget messaging)
- WATCH/UNWATCH (optimistic locking for transactions)
- XAUTOCLAIM (auto-reassign stuck messages)
