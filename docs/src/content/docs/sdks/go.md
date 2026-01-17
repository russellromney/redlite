---
title: Go SDK
description: Redlite SDK for Go
---

Go SDK with FFI bindings for Go applications.

## Installation

```bash
go get github.com/russellromney/redlite/sdks/redlite-go
```

Requires CGO and the Redlite FFI library.

## Quick Start

```go
package main

import (
    "fmt"
    "github.com/russellromney/redlite/sdks/redlite-go/redlite"
)

func main() {
    // Open in-memory database
    db, err := redlite.Open(":memory:")
    if err != nil {
        panic(err)
    }
    defer db.Close()

    // String operations
    db.Set("key", []byte("value"))
    val, _ := db.Get("key")
    fmt.Println(string(val)) // "value"

    // Hash operations
    db.HSet("user:1", "name", []byte("Alice"))
    db.HSet("user:1", "age", []byte("30"))

    // List operations
    db.LPush("queue", []byte("job1"), []byte("job2"))
    job, _ := db.RPop("queue")

    // Set operations
    db.SAdd("tags", []byte("redis"), []byte("sqlite"))

    // Sorted set operations
    db.ZAdd("scores", 100.0, []byte("player1"))
}
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
cd sdks/redlite-go
go test -v ./...
```

## Links

- [Full README](https://github.com/russellromney/redlite/tree/main/sdks/redlite-go)
- [Source Code](https://github.com/russellromney/redlite/tree/main/sdks/redlite-go)
