# Redlite SDKs

Multi-language SDKs for Redlite with direct Rust bindings for embedded mode.

## Overview

Redlite SDKs provide two operational modes:

1. **Embedded Mode** - Direct Rust bindings (PyO3, napi-rs, JNI, etc.) for in-process database
2. **Server Mode** - Connect to standalone Redlite/Redis server via TCP

Most SDKs use native bindings for optimal performance in embedded mode.

---

## SDK Status

### ✅ Complete (10 SDKs)

| Language | Directory | Binding Type | Package |
|----------|-----------|--------------|---------|
| Python | `redlite-python/` | PyO3 | `redlite` (PyPI) |
| TypeScript | `redlite-ts/` | napi-rs | `redlite` (npm) |
| Go | `redlite-go/` | CGO | `github.com/redlite/redlite-go` |
| C++ | `redlite-cpp/` | C++17 header-only | Header-only |
| Swift | `redlite-swift/` | C FFI | Swift Package |
| C#/.NET | `redlite-dotnet/` | P/Invoke | NuGet |
| Dart | `redlite-dart/` | FFI | pub.dev |
| Kotlin | `redlite-kotlin/` | JNI | Maven |
| Java | `redlite-java/` | JNI | Maven |
| Rust | *(use crate directly)* | Native | `redlite` crate |

### 🔧 Needs Update (6 SDKs)

These exist but need updates for latest FFI bindings (Phase 1 complete: 114 FFI functions, 70% coverage):

| Language | Directory | Binding Type | Status |
|----------|-----------|--------------|--------|
| Ruby | `redlite-ruby/` | FFI gem | Needs 25 new commands from Session 2+3 |
| Lua | `redlite-lua/` | LuaJIT FFI | Needs 25 new commands from Session 2+3 |
| Zig | `redlite-zig/` | C ABI | Needs 25 new commands from Session 2+3 |
| Elixir | `redlite-elixir/` | Rustler NIFs | Needs 25 new commands from Session 2+3 |
| PHP | `redlite-php/` | PHP FFI | Needs 25 new commands from Session 2+3 |
| WASM | `redlite-wasm/` | wasm-bindgen | Needs 25 new commands from Session 2+3 |

### 📋 Planned (38+ SDKs)

See `ROADMAP.md` for the full list including Scala, Haskell, OCaml, and esoteric languages.

---

## Architecture

### Direct Rust Bindings (Embedded Mode)

SDKs use language-specific bindings to the Rust core:

```
Python:     PyO3 → Rust core (1 layer)
TypeScript: napi-rs → Rust core (1 layer)
Go:         CGO → C FFI → Rust core (2 layers)
Swift:      C FFI → Rust core (2 layers)
C#:         P/Invoke → C FFI → Rust core (2 layers)
Dart:       FFI → C FFI → Rust core (2 layers)
```

**Before** (deprecated):
```
Python: CFFI → libredlite_ffi.dylib → Rust core (3 layers)
```

### Server Mode

All SDKs can connect to a standalone Redlite server via Redis protocol (RESP):

```
SDK → TCP → Redlite Server
```

Uses standard Redis client libraries (`redis-py`, `ioredis`, `go-redis`, etc.)

---

## Directory Structure

```
sdks/
├── ROADMAP.md                 # SDK development roadmap
├── redlite-python/            # Python SDK (PyO3)
├── redlite-ts/                # TypeScript SDK (napi-rs)
├── redlite-go/                # Go SDK (CGO)
├── redlite-cpp/               # C++ SDK (header-only)
├── redlite-swift/             # Swift SDK (C FFI)
├── redlite-dotnet/            # C#/.NET SDK (P/Invoke)
├── redlite-dart/              # Dart SDK (FFI)
├── redlite-kotlin/            # Kotlin SDK (JNI)
├── redlite-java/              # Java SDK (JNI)
├── redlite-ruby/              # Ruby SDK (FFI gem)
├── redlite-lua/               # Lua SDK (LuaJIT FFI)
├── redlite-zig/               # Zig SDK (C ABI)
├── redlite-elixir/            # Elixir SDK (Rustler NIFs)
├── redlite-php/               # PHP SDK (PHP FFI)
├── redlite-wasm/              # WASM SDK (wasm-bindgen)
└── oracle/                    # Cross-SDK testing framework
    ├── spec/                  # YAML test specifications
    ├── runners/               # SDK test runners
    └── README.md              # Oracle testing docs
```

---

## FFI Layer

**Current Status** (as of Session 2026-01-17):
- **114 FFI functions** (70% coverage)
- **Phase 1 Complete** - Bit ops, SCAN, Streams, Consumer Groups

All C FFI bindings live in `crates/redlite-ffi/`:
- `src/lib.rs` - FFI function implementations
- `redlite.h` - Auto-generated C header

When FFI is updated, all SDKs that use C FFI benefit automatically.

See `ROADMAP.md` for missing commands and Phase 2/3 plans.

---

## Oracle Testing

**Location**: `sdks/oracle/`

Cross-SDK consistency testing using YAML specifications.

**Current Coverage**: 137 tests across 6 spec files (strings, hashes, keys, lists, sets, zsets)

### Quick Start

```bash
cd sdks/oracle

# Run all SDKs
make test

# Run specific SDK
make test-python
make test-ts
make test-go

# Verbose output
make test-verbose
```

See [oracle/README.md](oracle/README.md) for details on writing tests and adding runners.

---

## Adding a New SDK

1. **Choose a binding strategy**:
   - Prefer direct bindings (PyO3, napi-rs) over C FFI when available
   - Use C FFI for languages without direct Rust support

2. **Create SDK directory**:
   ```bash
   mkdir sdks/redlite-yourlang
   cd sdks/redlite-yourlang
   ```

3. **Implement core commands** (see checklist in `ROADMAP.md`):
   - String commands (GET, SET, INCR, etc.)
   - Key commands (DEL, EXISTS, TTL, etc.)
   - Hash, List, Set, Sorted Set commands
   - Scan operations (SCAN, HSCAN, SSCAN, ZSCAN)
   - Stream operations (XADD, XREAD, XRANGE, etc.)

4. **Add oracle test runner**:
   ```bash
   # Create runner in oracle/runners/
   touch sdks/oracle/runners/yourlang_runner.ext
   ```

5. **Run oracle tests**:
   ```bash
   cd sdks/oracle
   make test-yourlang
   ```

6. **Document and package**:
   - Add README with usage examples
   - Set up package manager (pip, npm, cargo, etc.)
   - Update `sdks/ROADMAP.md` SDK status table

---

## Testing Philosophy

### Oracle Tests (Cross-SDK Consistency)

- **Purpose**: Ensure all SDKs produce identical results
- **Location**: `sdks/oracle/spec/*.yaml`
- **Coverage**: Core Redis commands
- **Example**: Same YAML spec runs on Python, TypeScript, Go, etc.

### SDK-Specific Tests

- **Purpose**: Test language-specific features
- **Location**: Each SDK's `tests/` directory
- **Coverage**:
  - Type coercion (e.g., `db.set("key", 42)` auto-converts to bytes)
  - Language idioms (Python kwargs, TypeScript options objects)
  - Error handling patterns
  - Async/concurrency behavior

---

## SDK Architecture Pattern

Each SDK follows this structure:

```
redlite-<lang>/
├── src/                    # Source code
│   ├── lib.<ext>          # Main client class
│   ├── native.<ext>       # Native bindings (embedded mode)
│   └── namespaces/        # Redlite-specific features
│       ├── fts.<ext>      # Full-text search
│       ├── vector.<ext>   # Vector similarity
│       ├── geo.<ext>      # Geospatial
│       └── history.<ext>  # Version history
├── tests/                  # SDK-specific unit tests
├── examples/               # Usage examples
├── README.md               # SDK documentation
├── Makefile                # Build and test commands
└── <package-config>        # Language-specific (package.json, Cargo.toml, etc.)
```

---

## Namespace API

All SDKs expose Redlite-specific features via namespaces:

```python
# Python example
from redlite import Redlite

db = Redlite.open(":memory:")

# Full-text search
db.fts.create("idx", ["title", "body"])
db.fts.search("idx", "query")

# Vector similarity
db.vector.add("vec", [0.1, 0.2, 0.3], {"label": "A"})
db.vector.sim("vec", [0.1, 0.2, 0.3], k=10)

# Geospatial
db.geo.add("locations", [{"name": "SF", "lat": 37.77, "lon": -122.41}])
db.geo.search("locations", 37.77, -122.41, radius=10)

# Version history
db.history.enable("key")
db.set("key", "v1")
db.set("key", "v2")
db.history.getat("key", timestamp)  # Time-travel query
```

---

## Philosophy

**Direct bindings, not protocol wrappers.**

We don't reimplement the Redis protocol. Instead:

1. **Embedded mode**: Direct Rust bindings via PyO3, napi-rs, JNI, etc.
2. **Server mode**: Delegate to battle-tested Redis clients
3. **Namespaces**: Add Redlite-specific features as language-idiomatic APIs

This approach:
- ✅ Minimizes translation layers (1-2 layers vs 3+)
- ✅ Leverages Rust's memory safety and performance
- ✅ Reuses existing Redis ecosystem for server mode
- ✅ Reduces maintenance burden (fix once in Rust core)

---

## Resources

- [ROADMAP.md](ROADMAP.md) - SDK development plan and FFI coverage
- [oracle/README.md](oracle/README.md) - Oracle testing documentation
- [FFI Layer](../crates/redlite-ffi/) - C FFI bindings
- [Rust Core](../crates/redlite/) - Core database implementation
