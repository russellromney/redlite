---
title: Zig SDK
description: Redlite SDK for Zig
---

Experimental Zig SDK with FFI bindings.

## Status

**Experimental** - API may change.

## Installation

Add as a dependency in your `build.zig.zon`:

```zig
.dependencies = .{
    .redlite = .{
        .url = "https://github.com/russellromney/redlite/archive/main.tar.gz",
        .path = "sdks/redlite-zig",
    },
},
```

## Quick Start

```zig
const std = @import("std");
const redlite = @import("redlite");

pub fn main() !void {
    // Open in-memory database
    var db = try redlite.open(":memory:");
    defer db.close();

    // String operations
    try db.set("key", "value");
    const val = try db.get("key");
    std.debug.print("{s}\n", .{val});

    // Hash operations
    try db.hset("user:1", "name", "Alice");
    const name = try db.hget("user:1", "name");

    // List operations
    try db.lpush("queue", &.{ "job1", "job2" });
    const job = try db.rpop("queue");

    // Set operations
    try db.sadd("tags", &.{ "redis", "sqlite" });
    const members = try db.smembers("tags");
}
```

## Building

```bash
cd sdks/redlite-zig
zig build
zig build test
```

## Links

- [Source Code](https://github.com/russellromney/redlite/tree/main/sdks/redlite-zig)
