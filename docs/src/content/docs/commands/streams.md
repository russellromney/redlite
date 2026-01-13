---
title: Streams
description: Stream commands in Redlite
---

Stream commands for append-only log data structures with consumer groups.

## Commands

### Basic Operations

| Command | Syntax | Description |
|---------|--------|-------------|
| XADD | `XADD key [NOMKSTREAM] [MAXLEN\|MINID [=\|~] threshold] *\|ID field value [field value ...]` | Add entry |
| XLEN | `XLEN key` | Get stream length |
| XRANGE | `XRANGE key start end [COUNT count]` | Get entries by ID range |
| XREVRANGE | `XREVRANGE key end start [COUNT count]` | Get entries in reverse |
| XREAD | `XREAD [COUNT count] [BLOCK ms] STREAMS key [key ...] ID [ID ...]` | Read from streams |
| XTRIM | `XTRIM key MAXLEN\|MINID [=\|~] threshold` | Trim stream |
| XDEL | `XDEL key ID [ID ...]` | Delete entries |
| XINFO STREAM | `XINFO STREAM key` | Get stream info |

### Consumer Groups

| Command | Syntax | Description |
|---------|--------|-------------|
| XGROUP CREATE | `XGROUP CREATE key groupname ID [MKSTREAM]` | Create consumer group |
| XGROUP DESTROY | `XGROUP DESTROY key groupname` | Delete consumer group |
| XGROUP SETID | `XGROUP SETID key groupname ID` | Set group's last ID |
| XREADGROUP | `XREADGROUP GROUP group consumer [COUNT count] [BLOCK ms] [NOACK] STREAMS key [key ...] ID [ID ...]` | Read as consumer |
| XACK | `XACK key group ID [ID ...]` | Acknowledge entries |
| XPENDING | `XPENDING key group [start end count [consumer]]` | Get pending entries |
| XCLAIM | `XCLAIM key group consumer min-idle-time ID [ID ...] [IDLE ms] [TIME ms] [RETRYCOUNT count] [FORCE]` | Claim pending entries |
| XINFO GROUPS | `XINFO GROUPS key` | List consumer groups |
| XINFO CONSUMERS | `XINFO CONSUMERS key groupname` | List consumers |

## Examples

### Basic Operations

```bash
# Add entry (auto-generated ID)
127.0.0.1:6379> XADD mystream * sensor_id "123" temperature "25.5"
"1704067200000-0"

# Add entry with specific ID
127.0.0.1:6379> XADD mystream 1704067201000-0 sensor_id "123" temperature "26.0"
"1704067201000-0"

# Get stream length
127.0.0.1:6379> XLEN mystream
(integer) 2

# Read all entries
127.0.0.1:6379> XRANGE mystream - +
1) 1) "1704067200000-0"
   2) 1) "sensor_id"
      2) "123"
      3) "temperature"
      4) "25.5"
2) 1) "1704067201000-0"
   2) 1) "sensor_id"
      2) "123"
      3) "temperature"
      4) "26.0"
```

### Range Queries

```bash
# Get last 5 entries
127.0.0.1:6379> XREVRANGE mystream + - COUNT 5

# Get entries after specific ID
127.0.0.1:6379> XRANGE mystream 1704067200000-0 +

# Get entries before specific ID
127.0.0.1:6379> XRANGE mystream - 1704067201000-0
```

### Reading Streams

```bash
# Read new entries (from last seen ID)
127.0.0.1:6379> XREAD STREAMS mystream 0
1) 1) "mystream"
   2) 1) 1) "1704067200000-0"
         2) 1) "sensor_id"
            2) "123"
            3) "temperature"
            4) "25.5"

# Read from multiple streams
127.0.0.1:6379> XREAD STREAMS stream1 stream2 0 0

# Blocking read (server mode only)
127.0.0.1:6379> XREAD BLOCK 5000 STREAMS mystream $
# Waits up to 5 seconds for new entries
```

### Stream Management

```bash
# Trim to max 1000 entries
127.0.0.1:6379> XTRIM mystream MAXLEN 1000
(integer) 500  # Entries removed

# Approximate trim (faster)
127.0.0.1:6379> XTRIM mystream MAXLEN ~ 1000

# Delete specific entry
127.0.0.1:6379> XDEL mystream 1704067200000-0
(integer) 1

# Get stream info
127.0.0.1:6379> XINFO STREAM mystream
```

### Consumer Groups

```bash
# Create consumer group (start from beginning)
127.0.0.1:6379> XGROUP CREATE mystream mygroup 0
OK

# Create group starting from now
127.0.0.1:6379> XGROUP CREATE mystream mygroup $ MKSTREAM
OK

# Read as consumer
127.0.0.1:6379> XREADGROUP GROUP mygroup consumer1 STREAMS mystream >
1) 1) "mystream"
   2) 1) 1) "1704067200000-0"
         2) 1) "sensor_id"
            2) "123"
            3) "temperature"
            4) "25.5"

# Acknowledge processed entries
127.0.0.1:6379> XACK mystream mygroup 1704067200000-0
(integer) 1

# Check pending entries
127.0.0.1:6379> XPENDING mystream mygroup
1) (integer) 5  # Total pending
2) "1704067200000-0"  # Smallest ID
3) "1704067205000-0"  # Largest ID
4) 1) 1) "consumer1"
      2) "5"
```

### Claiming Messages

```bash
# Claim entries idle for more than 1 minute
127.0.0.1:6379> XCLAIM mystream mygroup consumer2 60000 1704067200000-0
1) 1) "1704067200000-0"
   2) 1) "sensor_id"
      2) "123"
```

## Library Mode (Rust)

```rust
use redlite::Db;

let db = Db::open("mydata.db")?;

// Add entry
let id = db.xadd("mystream", "*", &[("field1", b"value1")])?;

// Read entries
let entries = db.xrange("mystream", "-", "+", None)?;

// Read from multiple streams
let results = db.xread(&[("mystream", "0")])?;

// Stream info
let info = db.xinfo_stream("mystream")?;

// Consumer groups (require mutable reference)
db.xgroup_create("mystream", "mygroup", "0", false)?;
let entries = db.xreadgroup("mygroup", "consumer1", &[("mystream", ">")], None)?;
db.xack("mystream", "mygroup", &["1704067200000-0"])?;
```

## Use Cases

### Event Sourcing

```bash
# Record events
XADD events * type "order_created" order_id "123" user_id "456"
XADD events * type "payment_received" order_id "123" amount "99.99"
XADD events * type "order_shipped" order_id "123" tracking "ABC123"

# Replay events to rebuild state
XRANGE events - +
```

### Real-Time Analytics

```bash
# Log page views
XADD pageviews * url "/products" user_id "u:100" timestamp "2024-01-01T12:00:00Z"

# Read recent activity
XREVRANGE pageviews + - COUNT 100
```

### Message Queues with Acknowledgment

```bash
# Producer adds messages
XADD tasks * type "email" to "user@example.com" subject "Hello"

# Consumer group for reliable processing
XGROUP CREATE tasks workers 0 MKSTREAM

# Worker reads and processes
XREADGROUP GROUP workers worker1 COUNT 1 STREAMS tasks >
# ... process message ...
XACK tasks workers 1704067200000-0
```

### IoT Sensor Data

```bash
# Sensors push data
XADD sensor:temp:room1 * value "23.5" unit "celsius"
XADD sensor:humidity:room1 * value "45" unit "percent"

# Keep last 24 hours of data
XTRIM sensor:temp:room1 MINID (now-86400000)
```
