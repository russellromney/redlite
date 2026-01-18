---
title: Transactions
description: Transaction commands in Redlite for atomic operations
---

Transaction commands for grouping multiple operations into atomic units with optimistic locking support.

## Commands

| Command | Syntax | Description |
|---------|--------|-------------|
| MULTI | `MULTI` | Start a transaction block |
| EXEC | `EXEC` | Execute all queued commands atomically |
| DISCARD | `DISCARD` | Abort transaction and discard queued commands |
| WATCH | `WATCH key [key ...]` | Watch keys for changes (optimistic locking) |
| UNWATCH | `UNWATCH` | Clear all watched keys |

## How Transactions Work

1. `MULTI` starts a transaction - commands are queued, not executed
2. Each command returns `QUEUED` instead of its result
3. `EXEC` executes all queued commands atomically and returns results
4. `DISCARD` aborts and clears the queue

## Examples

### Basic Transaction

```bash
127.0.0.1:6379> MULTI
OK
127.0.0.1:6379> SET user:1:name "Alice"
QUEUED
127.0.0.1:6379> SET user:1:email "alice@example.com"
QUEUED
127.0.0.1:6379> INCR user:count
QUEUED
127.0.0.1:6379> EXEC
1) OK
2) OK
3) (integer) 1
```

### Abort Transaction

```bash
127.0.0.1:6379> MULTI
OK
127.0.0.1:6379> SET key1 "value1"
QUEUED
127.0.0.1:6379> SET key2 "value2"
QUEUED
127.0.0.1:6379> DISCARD
OK

# Nothing was set - transaction aborted
127.0.0.1:6379> GET key1
(nil)
```

### Optimistic Locking with WATCH

WATCH enables optimistic locking - the transaction aborts if any watched key changes before EXEC:

```bash
# Terminal 1: Watch and modify
127.0.0.1:6379> SET balance 100
OK
127.0.0.1:6379> WATCH balance
OK
127.0.0.1:6379> GET balance
"100"
127.0.0.1:6379> MULTI
OK
127.0.0.1:6379> DECRBY balance 50
QUEUED

# Before EXEC, Terminal 2 modifies balance...

127.0.0.1:6379> EXEC
(nil)  # Transaction aborted - balance was modified!
```

```bash
# Terminal 2: Concurrent modification
127.0.0.1:6379> SET balance 200
OK
```

### Check-and-Set Pattern

```bash
# Increment only if value is what we expect
127.0.0.1:6379> SET counter 10
OK
127.0.0.1:6379> WATCH counter
OK
127.0.0.1:6379> GET counter
"10"

# Only proceed if counter is still 10
127.0.0.1:6379> MULTI
OK
127.0.0.1:6379> SET counter 11
QUEUED
127.0.0.1:6379> EXEC
1) OK  # Success - counter wasn't modified
```

### Clear Watches

```bash
127.0.0.1:6379> WATCH key1 key2 key3
OK
127.0.0.1:6379> UNWATCH
OK
# All watches cleared
```

### Transfer Between Accounts

```bash
127.0.0.1:6379> SET account:A 1000
OK
127.0.0.1:6379> SET account:B 500
OK

# Atomic transfer of 200 from A to B
127.0.0.1:6379> WATCH account:A account:B
OK
127.0.0.1:6379> MULTI
OK
127.0.0.1:6379> DECRBY account:A 200
QUEUED
127.0.0.1:6379> INCRBY account:B 200
QUEUED
127.0.0.1:6379> EXEC
1) (integer) 800
2) (integer) 700
```

## Library Mode (Rust)

For embedded library mode, Redlite provides SQLite-level transactions which offer even stronger guarantees:

```rust
use redlite::Db;

let db = Db::open("mydata.db")?;

// SQLite transaction - all-or-nothing
db.with_transaction(|tx| {
    tx.set("user:1:name", b"Alice")?;
    tx.set("user:1:email", b"alice@example.com")?;
    tx.incr("user:count")?;
    Ok(())
})?;

// Transaction with rollback on error
db.with_transaction(|tx| {
    tx.decrby("account:A", 200)?;

    // Check balance
    let balance = tx.get("account:A")?.unwrap_or_default();
    let balance: i64 = String::from_utf8_lossy(&balance).parse().unwrap_or(0);

    if balance < 0 {
        return Err(redlite::error::KvError::Custom("Insufficient funds".into()));
        // Transaction automatically rolls back
    }

    tx.incrby("account:B", 200)?;
    Ok(())
})?;
```

## Important Notes

### WATCH Behavior

- WATCH must be called **before** MULTI
- Watches persist until EXEC, DISCARD, or UNWATCH
- If any watched key is modified by another client, EXEC returns `nil`
- EXEC clears all watches automatically

### Server Mode Only

- MULTI/EXEC/DISCARD/WATCH/UNWATCH are only available in server mode
- For embedded library mode, use `db.with_transaction()` for SQLite transactions

### Transaction Scope

- Commands in a transaction see the database state at EXEC time
- No reads inside a transaction see uncommitted writes from the same transaction
- Use WATCH for read-modify-write patterns

## Use Cases

### Atomic Counter with Limit

```bash
WATCH counter
GET counter
# Check if counter < 100 in application
MULTI
INCR counter
EXEC
```

### Inventory Management

```bash
WATCH inventory:widget
GET inventory:widget
# Check if inventory >= order_quantity
MULTI
DECRBY inventory:widget 5
LPUSH orders:pending order:123
EXEC
```

### Session Management

```bash
MULTI
SET session:abc123 '{"user":"alice"}'
EXPIRE session:abc123 3600
SADD active_sessions "abc123"
EXEC
```
