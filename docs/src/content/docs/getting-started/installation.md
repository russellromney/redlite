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

Pre-built binaries are available for major platforms via GitHub Releases. Download the latest release for your platform:

- **macOS** (Apple Silicon): `redlite-aarch64-apple-darwin`
- **macOS** (Intel): `redlite-x86_64-apple-darwin`
- **Linux** (x86_64): `redlite-x86_64-unknown-linux-gnu`

Or build from source for other platforms.

## Requirements

- **Rust** (latest stable recommended, for building from source)
- **SQLite** is bundled — no system dependencies needed
- **redis-cli** (optional, for testing server mode)
