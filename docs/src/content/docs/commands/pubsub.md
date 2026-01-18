---
title: Pub/Sub
description: Publish/Subscribe messaging commands in Redlite
---

Pub/Sub commands for real-time messaging between clients. Messages are broadcast to all subscribers of a channel.

:::note
Pub/Sub is only available in **server mode**. It is not available in embedded library mode.
:::

## Commands

| Command | Syntax | Description |
|---------|--------|-------------|
| SUBSCRIBE | `SUBSCRIBE channel [channel ...]` | Subscribe to channels |
| UNSUBSCRIBE | `UNSUBSCRIBE [channel ...]` | Unsubscribe from channels |
| PUBLISH | `PUBLISH channel message` | Publish message to channel |
| PSUBSCRIBE | `PSUBSCRIBE pattern [pattern ...]` | Subscribe to pattern-matched channels |
| PUNSUBSCRIBE | `PUNSUBSCRIBE [pattern ...]` | Unsubscribe from patterns |

## How Pub/Sub Works

1. Clients subscribe to channels using `SUBSCRIBE` or `PSUBSCRIBE`
2. Publishers send messages with `PUBLISH`
3. All subscribed clients receive the message
4. Messages are **not persisted** - if no subscribers, message is lost

## Examples

### Basic Subscribe/Publish

**Terminal 1 (Subscriber):**
```bash
127.0.0.1:6379> SUBSCRIBE news
Reading messages... (press Ctrl-C to quit)
1) "subscribe"
2) "news"
3) (integer) 1

# When message arrives:
1) "message"
2) "news"
3) "Breaking: Redlite 1.0 released!"
```

**Terminal 2 (Publisher):**
```bash
127.0.0.1:6379> PUBLISH news "Breaking: Redlite 1.0 released!"
(integer) 1  # Number of subscribers who received it
```

### Multiple Channels

**Subscriber:**
```bash
127.0.0.1:6379> SUBSCRIBE sports weather news
Reading messages...
1) "subscribe"
2) "sports"
3) (integer) 1
1) "subscribe"
2) "weather"
3) (integer) 2
1) "subscribe"
4) "news"
3) (integer) 3

# Messages from any channel:
1) "message"
2) "sports"
3) "Goal!"

1) "message"
2) "weather"
3) "Sunny, 72F"
```

### Pattern Subscriptions

Subscribe to all channels matching a pattern:

**Subscriber:**
```bash
127.0.0.1:6379> PSUBSCRIBE news:*
Reading messages...
1) "psubscribe"
2) "news:*"
3) (integer) 1

# Matches news:sports, news:tech, news:local, etc.
1) "pmessage"
2) "news:*"
3) "news:tech"
4) "New iPhone announced"

1) "pmessage"
2) "news:*"
3) "news:sports"
4) "Championship finals tonight"
```

**Publisher:**
```bash
127.0.0.1:6379> PUBLISH news:tech "New iPhone announced"
(integer) 1

127.0.0.1:6379> PUBLISH news:sports "Championship finals tonight"
(integer) 1

127.0.0.1:6379> PUBLISH weather "Sunny"
(integer) 0  # No pattern match
```

### Unsubscribe

```bash
# Unsubscribe from specific channels
127.0.0.1:6379> UNSUBSCRIBE news weather
1) "unsubscribe"
2) "news"
3) (integer) 1
1) "unsubscribe"
2) "weather"
3) (integer) 0

# Unsubscribe from all channels
127.0.0.1:6379> UNSUBSCRIBE
1) "unsubscribe"
2) (nil)
3) (integer) 0

# Unsubscribe from patterns
127.0.0.1:6379> PUNSUBSCRIBE news:*
1) "punsubscribe"
2) "news:*"
3) (integer) 0
```

### Check Subscriber Count

```bash
# PUBLISH returns number of subscribers
127.0.0.1:6379> PUBLISH mychannel "test"
(integer) 3  # 3 clients received the message

127.0.0.1:6379> PUBLISH emptychannel "test"
(integer) 0  # No subscribers
```

## Pattern Syntax

| Pattern | Matches |
|---------|---------|
| `*` | Any sequence of characters |
| `?` | Any single character |
| `[abc]` | Any character in brackets |

Examples:
- `news:*` matches `news:sports`, `news:tech`, `news:local`
- `user:?` matches `user:1`, `user:a` but not `user:12`
- `log:[we]rror` matches `log:error`, `log:wrror`

## Message Format

### Subscribe Confirmation
```
1) "subscribe"      # Message type
2) "channel-name"   # Channel subscribed to
3) (integer) N      # Total subscriptions for this client
```

### Regular Message
```
1) "message"        # Message type
2) "channel-name"   # Source channel
3) "message-body"   # The message content
```

### Pattern Message
```
1) "pmessage"       # Message type (pattern)
2) "pattern"        # Pattern that matched
3) "channel-name"   # Actual channel
4) "message-body"   # The message content
```

## Important Notes

### At-Most-Once Delivery

- Messages are delivered to current subscribers only
- If no subscribers exist, the message is **lost**
- No message persistence or history
- No acknowledgment or retry mechanism

### Subscription Mode

Once a client enters subscription mode:
- Only `SUBSCRIBE`, `UNSUBSCRIBE`, `PSUBSCRIBE`, `PUNSUBSCRIBE`, and `QUIT` are allowed
- Regular commands like `GET`, `SET` are not available
- Use a separate connection for commands

### Server Mode Only

Pub/Sub requires server mode because:
- Requires persistent TCP connections
- Real-time message delivery
- Multiple client coordination

## Use Cases

### Real-Time Notifications

```bash
# User activity channel
SUBSCRIBE user:123:notifications

# Send notification
PUBLISH user:123:notifications '{"type":"message","from":"user:456"}'
```

### Chat Rooms

```bash
# Join room
SUBSCRIBE room:general

# Send message
PUBLISH room:general '{"user":"alice","text":"Hello everyone!"}'
```

### Cache Invalidation

```bash
# All app instances subscribe
PSUBSCRIBE cache:invalidate:*

# When data changes
PUBLISH cache:invalidate:users "user:123"
PUBLISH cache:invalidate:products "product:456"
```

### Event Broadcasting

```bash
# Subscribe to all events
PSUBSCRIBE events:*

# Publish events
PUBLISH events:user:created '{"id":123}'
PUBLISH events:order:completed '{"id":456}'
```

### Distributed Coordination

```bash
# Leader election channel
SUBSCRIBE leader:election

# Announce leadership
PUBLISH leader:election '{"node":"node-1","action":"claim"}'
```

## Comparison with Streams

| Feature | Pub/Sub | Streams |
|---------|---------|---------|
| Persistence | No | Yes |
| Message history | No | Yes |
| Consumer groups | No | Yes |
| Acknowledgment | No | Yes |
| Blocking read | Subscribe-based | XREAD BLOCK |
| Use case | Real-time broadcast | Reliable queuing |

Use **Pub/Sub** for fire-and-forget broadcasting.
Use **Streams** when you need reliability and history.
