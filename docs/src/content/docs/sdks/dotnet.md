---
title: .NET SDK
description: Redlite SDK for .NET/C#
---

.NET SDK with P/Invoke bindings for .NET 6.0+.

## Installation

```bash
dotnet add package Redlite
```

## Quick Start

```csharp
using Redlite;

// Open in-memory database
using var db = new RedliteDb(":memory:");

// Or file-based
using var db = new RedliteDb("/path/to/db.db");

// String operations
db.Set("key", "value");
var val = db.Get("key");
Console.WriteLine(Encoding.UTF8.GetString(val));  // "value"

// Hash operations
db.HSet("user:1", "name", "Alice");
db.HSet("user:1", "age", "30");
var user = db.HGetAll("user:1");

// List operations
db.LPush("queue", "job1", "job2");
var job = db.RPop("queue");

// Set operations
db.SAdd("tags", "redis", "sqlite");
var members = db.SMembers("tags");

// Sorted sets
db.ZAdd("scores", (100.0, "player1"), (85.0, "player2"));
var top = db.ZRevRange("scores", 0, 9);
```

## API Overview

**Strings**: `Set`, `Get`, `Incr`, `Decr`, `Append`, `MGet`, `MSet`

**Keys**: `Del`, `Exists`, `Type`, `TTL`, `Expire`, `Keys`

**Hashes**: `HSet`, `HGet`, `HDel`, `HGetAll`, `HMGet`

**Lists**: `LPush`, `RPush`, `LPop`, `RPop`, `LLen`, `LRange`

**Sets**: `SAdd`, `SRem`, `SMembers`, `SIsMember`, `SCard`

**Sorted Sets**: `ZAdd`, `ZRem`, `ZScore`, `ZRange`, `ZRevRange`

## Testing

```bash
cd sdks/redlite-dotnet
dotnet test
```

## Links

- [Full README](https://github.com/russellromney/redlite/tree/main/sdks/redlite-dotnet)
- [Source Code](https://github.com/russellromney/redlite/tree/main/sdks/redlite-dotnet)
