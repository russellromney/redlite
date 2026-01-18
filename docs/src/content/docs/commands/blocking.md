---
title: Blocking Operations
description: Blocking commands in Redlite for queue-based patterns
---

Blocking commands that wait for data to become available, enabling queue patterns and real-time processing.

:::note
**Embedded mode:** Uses polling (periodically checks for data).
**Server mode:** Uses async/await (efficient waiting on TCP connections).
:::

## Commands

| Command | Syntax | Description |
|---------|--------|-------------|
| BLPOP | `BLPOP key [key ...] timeout` | Block until element available at list head |
| BRPOP | `BRPOP key [key ...] timeout` | Block until element available at list tail |
| BLMOVE | `BLMOVE src dst LEFT\|RIGHT LEFT\|RIGHT timeout` | Block until element can be moved |
| BRPOPLPUSH | `BRPOPLPUSH src dst timeout` | Block pop from src, push to dst (deprecated) |
| XREAD BLOCK | `XREAD BLOCK ms STREAMS key id` | Block until new stream entries |

## How Blocking Works

1. Client sends blocking command with timeout
2. If data exists, returns immediately
3. If no data, server waits until:
   - Data becomes available (another client pushes)
   - Timeout expires (returns nil)
   - Connection closes

## Examples

### BLPOP - Blocking List Pop

**Consumer (blocks waiting for work):**
```bash
127.0.0.1:6379> BLPOP queue:jobs 30
# ... waits up to 30 seconds ...
1) "queue:jobs"
2) "job:123"
```

**Producer (adds work):**
```bash
127.0.0.1:6379> LPUSH queue:jobs "job:123"
(integer) 1
# Consumer immediately receives the job
```

### Multiple Queues

Check multiple queues, return from first with data:

```bash
# Wait on multiple queues (priority order)
127.0.0.1:6379> BLPOP queue:high queue:medium queue:low 10
# Returns from first non-empty queue
1) "queue:high"
2) "urgent-task"
```

### BRPOP - Pop from Tail

```bash
# FIFO queue - push left, pop right (blocking)
127.0.0.1:6379> BRPOP queue:tasks 0
# 0 = wait forever
```

### BLMOVE - Reliable Queue

Move element atomically between lists:

```bash
# Worker: take job from pending, move to processing
127.0.0.1:6379> BLMOVE jobs:pending jobs:processing LEFT RIGHT 30
"job:456"

# After job complete, remove from processing
127.0.0.1:6379> LREM jobs:processing 1 "job:456"
(integer) 1
```

### XREAD BLOCK - Stream Blocking Read

```bash
# Wait for new stream entries
127.0.0.1:6379> XREAD BLOCK 5000 STREAMS events $
# Waits up to 5 seconds for new entries after current time

# When entry arrives:
1) 1) "events"
   2) 1) 1) "1234567890123-0"
         2) 1) "type"
            2) "click"
            3) "user"
            4) "123"
```

### Timeout Handling

```bash
# 5 second timeout
127.0.0.1:6379> BLPOP empty:queue 5
(nil)  # Nothing after 5 seconds

# Immediate return if data exists
127.0.0.1:6379> LPUSH myqueue "item"
(integer) 1
127.0.0.1:6379> BLPOP myqueue 30
1) "myqueue"
2) "item"  # Returns immediately, doesn't wait
```

### Zero Timeout (Wait Forever)

```bash
# Wait indefinitely for work
127.0.0.1:6379> BLPOP queue:jobs 0
# Blocks until data or connection closed
```

## Queue Patterns

### Simple Work Queue

```bash
# Producer: add jobs
LPUSH queue:jobs "job:1" "job:2" "job:3"

# Consumer: process jobs (blocking)
while true:
    BLPOP queue:jobs 0  # Wait forever
    # Process job...
```

### Priority Queue

```bash
# Producer: add to appropriate queue
LPUSH queue:critical "urgent-job"
LPUSH queue:normal "regular-job"
LPUSH queue:low "background-job"

# Consumer: check in priority order
BLPOP queue:critical queue:normal queue:low 10
```

### Reliable Queue with BLMOVE

```bash
# Step 1: Worker claims job
job = BLMOVE pending processing LEFT RIGHT 30

# Step 2: Process job
# ... do work ...

# Step 3a: Success - remove from processing
LREM processing 1 job

# Step 3b: Failure - move back to pending
LMOVE processing pending LEFT LEFT
```

### Pub/Sub Alternative with Lists

```bash
# Subscriber (blocking receive)
while true:
    message = BLPOP channel:notifications 0
    # Handle message...

# Publisher (non-blocking send)
LPUSH channel:notifications '{"event":"update"}'
```

## Stream Blocking Examples

### Real-Time Event Processing

```bash
# Consumer: wait for events
127.0.0.1:6379> XREAD BLOCK 0 STREAMS events $
# $ = only new entries

# Producer: add events
127.0.0.1:6379> XADD events * type "click" page "/home"
"1234567890123-0"
```

### Consumer Group with Blocking

```bash
# Create group
127.0.0.1:6379> XGROUP CREATE events mygroup $ MKSTREAM

# Consumer: blocking read from group
127.0.0.1:6379> XREADGROUP GROUP mygroup consumer-1 BLOCK 5000 STREAMS events >
```

### Multiple Streams

```bash
# Wait on multiple streams
127.0.0.1:6379> XREAD BLOCK 10000 STREAMS orders:new orders:updated 0-0 0-0
```

## Important Notes

### Connection Handling

- Blocking commands tie up the connection
- Use separate connections for blocking and regular commands
- Set appropriate timeouts to avoid resource exhaustion

### Timeout Values

| Value | Behavior |
|-------|----------|
| `0` | Wait forever |
| `N` | Wait N seconds (BLPOP/BRPOP) or N milliseconds (XREAD) |

### Mode Differences

| Mode | Implementation | Best For |
|------|----------------|----------|
| Embedded | Polling with configurable interval | Single-process apps |
| Server | Async/await on TCP connections | Multi-client coordination |

### Fairness

When multiple clients wait on the same key:
- First client to wait is first to receive
- FIFO ordering for blocked clients

## Use Cases

### Job Queue

```bash
# Multiple workers competing for jobs
# Worker 1:
BLPOP jobs 0

# Worker 2:
BLPOP jobs 0

# Producer adds job - one worker gets it
LPUSH jobs "task:123"
```

### Rate Limiting with Blocking

```bash
# Token bucket pattern
# Consumer waits for tokens
BLPOP rate:tokens 1

# Background process refills tokens
LPUSH rate:tokens "token"
EXPIRE rate:tokens 1
```

### Delayed Processing

```bash
# Add job with delay using sorted set + blocking list
ZADD delayed:jobs <future_timestamp> "job:123"

# Background process moves ready jobs
while true:
    # Check for ready jobs
    ready = ZRANGEBYSCORE delayed:jobs 0 <now> LIMIT 0 1
    if ready:
        ZREM delayed:jobs ready
        LPUSH queue:jobs ready
    sleep 100ms

# Workers process from blocking queue
BLPOP queue:jobs 0
```

### Event Aggregation

```bash
# Collect events, process in batches
# Producer adds events
LPUSH events:buffer '{"event":"click"}'

# Consumer waits then processes batch
while true:
    event = BLPOP events:buffer 5  # 5 second timeout
    if event:
        batch.append(event)
    if len(batch) >= 100 or timeout:
        process_batch(batch)
        batch = []
```
