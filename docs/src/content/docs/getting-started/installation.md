---
title: Installation
description: How to install Redlite
---

## As a Library (Recommended)

Add Redlite to your `Cargo.toml`:

```bash
cargo add redlite
```

Or manually add to `Cargo.toml`:

```toml
[dependencies]
redlite = "0.1"
```

## Building from Source

```bash
git clone https://github.com/russellromney/redlite
cd redlite
cargo build --release
```

The binary will be at `./target/release/redlite`.

## Pre-built Binaries

Coming soon. For now, build from source.

## Requirements

- Rust 1.70+ (for building)
- SQLite is bundled — no system dependencies needed
- `redis-cli` (optional, for testing server mode)
