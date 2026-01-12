---
title: String Commands
description: Redis string commands supported by Redlite
---

String commands operate on string values. In Redlite, strings are stored as binary data (bytes).

## GET

Get the value of a key.

```
GET key
```

**Returns:**
- The value if key exists
- `nil` if key does not exist

**Example:**
```bash
127.0.0.1:6767> SET greeting "Hello"
OK
127.0.0.1:6767> GET greeting
"Hello"
127.0.0.1:6767> GET nonexistent
(nil)
```

**Library:**
```rust
let value = db.get("key")?;  // Option<Vec<u8>>
```

## SET

Set the value of a key.

```
SET key value [EX seconds | PX milliseconds] [NX | XX]
```

**Options:**
- `EX seconds` - Set expiration in seconds
- `PX milliseconds` - Set expiration in milliseconds
- `NX` - Only set if key does NOT exist
- `XX` - Only set if key DOES exist

**Returns:**
- `OK` if successful
- `nil` if NX/XX condition not met

**Examples:**
```bash
# Basic set
127.0.0.1:6767> SET name "Alice"
OK

# With expiration (60 seconds)
127.0.0.1:6767> SET session "abc123" EX 60
OK

# With expiration (5000 milliseconds)
127.0.0.1:6767> SET temp "quick" PX 5000
OK

# Only if not exists (NX)
127.0.0.1:6767> SET counter 0 NX
OK
127.0.0.1:6767> SET counter 1 NX
(nil)

# Only if exists (XX)
127.0.0.1:6767> SET counter 10 XX
OK
127.0.0.1:6767> SET newkey value XX
(nil)
```

**Library:**
```rust
use std::time::Duration;
use redlite::SetOptions;

// Basic
db.set("key", b"value", None)?;

// With TTL
db.set("key", b"value", Some(Duration::from_secs(60)))?;

// With options
db.set_opts("key", b"value", SetOptions::new().nx())?;
db.set_opts("key", b"value", SetOptions::new().xx().ex(Duration::from_secs(60)))?;
```

## DEL

Delete one or more keys.

```
DEL key [key ...]
```

**Returns:**
- Number of keys deleted

**Example:**
```bash
127.0.0.1:6767> SET key1 "a"
OK
127.0.0.1:6767> SET key2 "b"
OK
127.0.0.1:6767> DEL key1 key2 key3
(integer) 2
```

**Library:**
```rust
db.del(&["key1", "key2", "key3"])?;
```

## Connection Commands

### PING

Test server connectivity.

```
PING [message]
```

**Returns:**
- `PONG` if no message provided
- The message if provided

**Example:**
```bash
127.0.0.1:6767> PING
PONG
127.0.0.1:6767> PING "hello"
"hello"
```

### ECHO

Echo the given message.

```
ECHO message
```

**Returns:**
- The message

**Example:**
```bash
127.0.0.1:6767> ECHO "Hello, World!"
"Hello, World!"
```

### COMMAND

Get information about Redis commands.

```
COMMAND
```

**Returns:**
- Array of command information

**Example:**
```bash
127.0.0.1:6767> COMMAND
1) 1) "ping"
   2) (integer) -1
...
```

## Expiration Behavior

Keys with TTL are lazily expired:
- Expiration is checked on read (GET)
- Expired keys return `nil` and are deleted

```bash
127.0.0.1:6767> SET temp "value" PX 100
OK
127.0.0.1:6767> GET temp
"value"
# Wait 100ms
127.0.0.1:6767> GET temp
(nil)
```
