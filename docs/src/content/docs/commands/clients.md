---
title: Client Commands
description: Client connection management commands in Redlite
---

Client commands for managing connections, debugging, and controlling client behavior.

## Commands

| Command | Syntax | Description |
|---------|--------|-------------|
| CLIENT SETNAME | `CLIENT SETNAME name` | Set connection name |
| CLIENT GETNAME | `CLIENT GETNAME` | Get connection name |
| CLIENT LIST | `CLIENT LIST` | List all connected clients |
| CLIENT ID | `CLIENT ID` | Get current connection ID |
| CLIENT INFO | `CLIENT INFO` | Get current connection info |
| CLIENT KILL | `CLIENT KILL ID id` | Kill a client connection |
| CLIENT PAUSE | `CLIENT PAUSE ms` | Pause all clients |
| CLIENT UNPAUSE | `CLIENT UNPAUSE` | Resume paused clients |

## Examples

### Connection Naming

Set a meaningful name for debugging:

```bash
127.0.0.1:6379> CLIENT SETNAME worker-1
OK

127.0.0.1:6379> CLIENT GETNAME
"worker-1"
```

### Get Connection ID

```bash
127.0.0.1:6379> CLIENT ID
(integer) 42
```

### Current Connection Info

```bash
127.0.0.1:6379> CLIENT INFO
id=42 addr=127.0.0.1:52341 name=worker-1 db=0 cmd=client
```

### List All Clients

```bash
127.0.0.1:6379> CLIENT LIST
id=1 addr=127.0.0.1:52340 name=admin db=0 cmd=client
id=42 addr=127.0.0.1:52341 name=worker-1 db=0 cmd=get
id=43 addr=127.0.0.1:52342 name=worker-2 db=0 cmd=blpop
id=44 addr=127.0.0.1:52343 name= db=1 cmd=set
```

Each line shows:
- `id` - Unique connection ID
- `addr` - Client IP and port
- `name` - Client name (if set)
- `db` - Selected database
- `cmd` - Last command executed

### Kill a Client

```bash
# Find the client
127.0.0.1:6379> CLIENT LIST
id=99 addr=192.168.1.100:12345 name=rogue-client db=0 cmd=keys

# Kill by ID
127.0.0.1:6379> CLIENT KILL ID 99
OK
```

### Pause All Clients

Temporarily pause all client operations (useful for maintenance):

```bash
# Pause for 5 seconds
127.0.0.1:6379> CLIENT PAUSE 5000
OK

# All other clients block on commands...

# Or resume early
127.0.0.1:6379> CLIENT UNPAUSE
OK
```

## Practical Examples

### Identify Slow Clients

```bash
# List clients to see what they're doing
127.0.0.1:6379> CLIENT LIST
id=1 addr=127.0.0.1:52340 name=web-1 cmd=get
id=2 addr=127.0.0.1:52341 name=web-2 cmd=get
id=3 addr=127.0.0.1:52342 name=batch-job cmd=keys  # Slow KEYS command!
```

### Connection Debugging

```bash
# Set descriptive name on connect
CLIENT SETNAME "api-server-pod-abc123"

# Later, in monitoring:
CLIENT LIST
# Can identify which pod/service each connection belongs to
```

### Graceful Maintenance

```bash
# 1. Pause writes during backup
CLIENT PAUSE 10000

# 2. Perform backup
# ... backup operations ...

# 3. Resume
CLIENT UNPAUSE
```

### Kill Misbehaving Clients

```bash
# Find clients doing expensive operations
127.0.0.1:6379> CLIENT LIST
id=50 addr=10.0.0.5:43210 name= db=0 cmd=keys  # KEYS * is bad!

# Terminate the connection
127.0.0.1:6379> CLIENT KILL ID 50
OK
```

## Client Info Fields

| Field | Description |
|-------|-------------|
| `id` | Unique client connection ID |
| `addr` | Client address (IP:port) |
| `name` | Client name from CLIENT SETNAME |
| `db` | Currently selected database (0-15) |
| `cmd` | Last command executed |

## Use Cases

### Application Monitoring

```bash
# Each app instance names itself
CLIENT SETNAME "app-instance-${HOSTNAME}"

# Ops team can see all connections
CLIENT LIST
```

### Connection Limits

```bash
# Check how many clients connected
CLIENT LIST
# Count lines to see total connections

# Kill oldest/idle connections if needed
CLIENT KILL ID <oldest_id>
```

### Debugging Connection Issues

```bash
# Which database is this connection using?
CLIENT INFO
# Shows: db=3

# What was the last command?
CLIENT INFO
# Shows: cmd=hgetall
```

### Blue-Green Deployments

```bash
# During deployment:
# 1. Pause old clients
CLIENT PAUSE 30000

# 2. Deploy new version

# 3. Resume (or let timeout expire)
CLIENT UNPAUSE
```

## Important Notes

### Server Mode Only

CLIENT commands are only available in server mode - they manage TCP connections which don't exist in embedded library mode.

### CLIENT PAUSE Behavior

- All clients (except the one issuing PAUSE) block
- Read and write commands are paused
- Pub/Sub messages are still delivered
- Timeout in milliseconds

### Connection Naming

- Names have no spaces or special characters
- Empty string clears the name
- Names are for debugging only, no security implications

### Killing Connections

- CLIENT KILL immediately closes the TCP connection
- Client will need to reconnect
- Use for misbehaving clients or maintenance
