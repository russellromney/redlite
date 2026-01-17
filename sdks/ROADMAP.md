# Redlite SDKs - Direct Rust Bindings Conversion

## Overview

**Goal**: Convert Python and Go SDKs from C FFI bindings to direct Rust bindings for minimal translation layers.

### Current Architecture (3 layers each)
```
Python: Rust → C FFI → libredlite_ffi.dylib → CFFI → Python
Go:     Rust → C FFI → libredlite_ffi.dylib → CGO → Go
TypeScript: Rust → napi-rs → JS (1 layer) ✓
```

### Target Architecture (1 layer each)
```
Python:     Rust → PyO3 → Python    (1 layer)
Go:         Rust → uniffi-rs → Go   (1 layer) OR keep CGO (standard for Go)
TypeScript: Rust → napi-rs → JS     (1 layer) ✓
```

---

## Session Summary - Python SDK Conversion Complete

**Date**: 2026-01-14
**Status**: ✅ COMPLETE

### What Was Done

1. **Created PyO3 Rust bindings** (`rust/lib.rs` - ~565 lines)
   - `EmbeddedDb` class with 55+ methods
   - `SetOptions` for SET command options (NX, XX, EX, PX)
   - `ZMember` for sorted set operations

2. **Updated build system**
   - `Cargo.toml` with PyO3 0.22 (supports Python 3.13)
   - `pyproject.toml` updated to use maturin backend
   - Removed CFFI dependency

3. **Updated Python wrapper** (`src/redlite/client.py`)
   - Uses `_native` module for embedded mode
   - Uses `redis-py` for server mode
   - Unified API across both modes

4. **Removed old CFFI code**
   - Deleted `src/redlite/_ffi.py`

### Test Results

```
Mode: embedded
get foo: b'bar'
counter after 2 incr: b'2'
lrange mylist: [b'c', b'b', b'a']
hget name: b'Alice'
smembers myset: {b'y', b'x', b'z'}
zscore a: 1.0
All tests passed!
```

### Architecture Change

**Before** (3 layers):
```
Python → CFFI → libredlite_ffi.dylib → Rust core
```

**After** (1 layer):
```
Python → PyO3 native module → Rust core
```

### Build Commands

```bash
cd sdks/redlite-python
maturin develop          # Development build
maturin build --release  # Release wheel
uv run pytest tests/ -v  # Run tests
```

---

## Task 1: Convert Python SDK to PyO3

**Status**: ✅ COMPLETE
**Priority**: HIGH
**Location**: `sdks/redlite-python/`

### Current State Analysis

**Files to Replace**:
- `src/redlite/_ffi.py` (~271 lines) - CFFI wrapper, library loading, C definitions
- `src/redlite/client.py` (~1105 lines) - Python client using CFFI

**Files to Keep/Update**:
- `src/redlite/__init__.py` - Just update exports
- `tests/*` - Keep existing tests, they should pass unchanged
- `pyproject.toml` - Update build system for maturin

**Reference Pattern**: `sdks/redlite-ts/src/lib.rs` (NAPI pattern to follow)

### Phase 1.1: Set Up PyO3 Project Structure

Create Rust crate for Python bindings:

```
sdks/redlite-python/
├── Cargo.toml              # PyO3 + maturin config
├── pyproject.toml          # Updated for maturin backend
├── src/
│   ├── lib.rs              # PyO3 bindings (~600 lines)
│   └── redlite/
│       ├── __init__.py     # Re-export PyO3 module + namespaces
│       ├── client.py       # Keep server mode + namespaces
│       └── _native.pyi     # Type stubs for IDE support
└── tests/                  # Unchanged
```

**Cargo.toml**:
```toml
[package]
name = "redlite-python"
version = "0.1.0"
edition = "2021"

[lib]
name = "redlite"
crate-type = ["cdylib"]

[dependencies]
pyo3 = { version = "0.21", features = ["extension-module"] }
redlite = { path = "../../crates/redlite" }

[build-dependencies]
pyo3-build-config = "0.21"
```

**pyproject.toml** (updated):
```toml
[build-system]
requires = ["maturin>=1.5,<2.0"]
build-backend = "maturin"

[project]
name = "redlite"
requires-python = ">=3.9"
dependencies = ["redis"]  # For server mode only

[tool.maturin]
python-source = "src"
module-name = "redlite._native"
features = ["pyo3/extension-module"]
```

### Phase 1.2: Implement Core PyO3 Bindings

**src/lib.rs** structure:
```rust
use pyo3::prelude::*;
use pyo3::types::PyBytes;
use redlite::Db as RedliteDb;

/// SetOptions for SET command
#[pyclass]
#[derive(Clone, Default)]
pub struct SetOptions {
    #[pyo3(get, set)]
    pub ex: Option<i64>,
    #[pyo3(get, set)]
    pub px: Option<i64>,
    #[pyo3(get, set)]
    pub nx: Option<bool>,
    #[pyo3(get, set)]
    pub xx: Option<bool>,
}

/// ZMember for sorted sets
#[pyclass]
#[derive(Clone)]
pub struct ZMember {
    #[pyo3(get, set)]
    pub score: f64,
    #[pyo3(get, set)]
    pub member: Vec<u8>,
}

/// Main database class
#[pyclass]
pub struct EmbeddedDb {
    inner: RedliteDb,
}

#[pymethods]
impl EmbeddedDb {
    #[new]
    fn new(path: &str) -> PyResult<Self> {
        RedliteDb::open(path)
            .map(|db| Self { inner: db })
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))
    }

    #[staticmethod]
    fn open_memory() -> PyResult<Self> {
        RedliteDb::open_memory()
            .map(|db| Self { inner: db })
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))
    }

    #[staticmethod]
    fn open_with_cache(path: &str, cache_mb: i64) -> PyResult<Self> {
        RedliteDb::open_with_cache(path, cache_mb)
            .map(|db| Self { inner: db })
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))
    }

    // String commands
    fn get(&self, py: Python<'_>, key: &str) -> PyResult<Option<PyObject>> {
        self.inner.get(key)
            .map(|opt| opt.map(|v| PyBytes::new(py, &v).into()))
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))
    }

    fn set(&self, key: &str, value: &[u8], ttl_seconds: Option<i64>) -> PyResult<bool> {
        let ttl = ttl_seconds.map(|s| std::time::Duration::from_secs(s as u64));
        self.inner.set(key, value, ttl)
            .map(|_| true)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))
    }

    // ... 40+ more methods following TypeScript SDK pattern
}

#[pymodule]
fn _native(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<EmbeddedDb>()?;
    m.add_class::<SetOptions>()?;
    m.add_class::<ZMember>()?;
    Ok(())
}
```

### Phase 1.3: Implement All Commands

Commands to implement (following TypeScript SDK):

**String Commands** (15 methods):
- `get`, `set`, `setex`, `psetex`, `getdel`
- `append`, `strlen`, `getrange`, `setrange`
- `incr`, `decr`, `incrby`, `decrby`, `incrbyfloat`

**Key Commands** (13 methods):
- `del`, `exists`, `type`, `ttl`, `pttl`
- `expire`, `pexpire`, `expireat`, `pexpireat`, `persist`
- `rename`, `renamenx`, `keys`, `dbsize`, `flushdb`, `select`

**Hash Commands** (8 methods):
- `hset`, `hget`, `hdel`, `hexists`
- `hlen`, `hkeys`, `hvals`, `hincrby`

**List Commands** (7 methods):
- `lpush`, `rpush`, `lpop`, `rpop`
- `llen`, `lrange`, `lindex`

**Set Commands** (5 methods):
- `sadd`, `srem`, `smembers`, `sismember`, `scard`

**Sorted Set Commands** (6 methods):
- `zadd`, `zrem`, `zscore`, `zcard`, `zcount`, `zincrby`

**Server Commands** (1 method):
- `vacuum`

**Total**: ~55 methods (matching TypeScript SDK)

### Phase 1.4: Update Python Wrapper

Update `src/redlite/__init__.py`:
```python
from redlite._native import EmbeddedDb, SetOptions, ZMember
from redlite.client import Redlite, FTSNamespace, VectorNamespace, GeoNamespace

__all__ = [
    "Redlite",
    "EmbeddedDb",
    "SetOptions",
    "ZMember",
    "FTSNamespace",
    "VectorNamespace",
    "GeoNamespace",
]
```

Update `src/redlite/client.py` to use native module for embedded mode:
```python
class Redlite:
    def __init__(self, url: str = ":memory:", cache_mb: int = 64):
        if url.startswith(("redis://", "rediss://")):
            # Server mode - use redis-py
            self._mode = "server"
            import redis
            self._redis = redis.from_url(url)
            self._native = None
        else:
            # Embedded mode - use PyO3 native module
            self._mode = "embedded"
            from redlite._native import EmbeddedDb
            if url == ":memory:":
                self._native = EmbeddedDb.open_memory()
            else:
                self._native = EmbeddedDb.open_with_cache(url, cache_mb)
            self._redis = None
```

### Phase 1.5: Build & Test

**Build with maturin**:
```bash
cd sdks/redlite-python
pip install maturin
maturin develop  # Development build
maturin build --release  # Release wheel
```

**Run existing tests**:
```bash
uv run pytest tests/ -v
```

**Expected changes**:
- Remove `_ffi.py` entirely
- Simplify `client.py` (remove CFFI calls, use `_native`)
- Tests should pass unchanged (API is the same)

---

## Task 2: Go SDK Conversion (Optional)

**Status**: EVALUATION NEEDED
**Priority**: MEDIUM
**Location**: `sdks/redlite-go/`

### Current State

Go SDK uses CGO with `#cgo` directives linking to `libredlite_ffi.dylib`. This is actually fairly standard for Go and works well.

### Options

**Option A: Keep CGO (Recommended)**
- CGO is the standard way Go interfaces with C/Rust
- Current implementation works and is well-tested
- Lower maintenance burden
- No additional tooling needed

**Option B: Convert to uniffi-rs**
- [uniffi-rs](https://github.com/mozilla/uniffi-rs) generates bindings from IDL
- More complex setup (need IDL file + build process)
- May not be worth the added complexity for Go

**Option C: Pure Go implementation**
- Re-implement SQLite operations in Go directly
- Loses Rust core consistency across SDKs
- Not recommended

### Recommendation

**Keep CGO** for now. The Go SDK works well, and CGO is the idiomatic way for Go to interface with native code. The effort to convert to uniffi-rs doesn't provide enough benefit over the current solution.

If we do decide to convert later, here's the approach:

**uniffi-rs Setup**:
```toml
# Cargo.toml for uniffi
[dependencies]
uniffi = "0.27"

[build-dependencies]
uniffi = { version = "0.27", features = ["build"] }
```

**redlite.udl** (Interface Definition):
```
namespace redlite {
    [Throws=RedliteError]
    Db open(string path);
};

interface Db {
    [Throws=RedliteError]
    bytes? get(string key);

    [Throws=RedliteError]
    boolean set(string key, bytes value, i64? ttl_seconds);

    // ... more methods
};
```

---

## Implementation Timeline

### Session 1: Python SDK Conversion - ✅ COMPLETE (2026-01-14)
- [x] Plan created in ROADMAP.md
- [x] Create Cargo.toml with PyO3 dependencies
- [x] Create rust/lib.rs with EmbeddedDb class (~565 lines)
- [x] Implement constructor and basic get/set
- [x] Implement all string commands (get, set, setex, psetex, getdel, append, strlen, getrange, setrange, incr, decr, incrby, decrby, incrbyfloat)
- [x] Implement all key commands (delete, exists, key_type, ttl, pttl, expire, pexpire, expireat, pexpireat, persist, rename, renamenx, keys, dbsize, flushdb, select)
- [x] Implement hash commands (hset, hmset, hget, hdel, hexists, hlen, hkeys, hvals, hincrby)
- [x] Implement list commands (lpush, rpush, lpop, rpop, llen, lrange, lindex)
- [x] Implement set commands (sadd, srem, smembers, sismember, scard)
- [x] Implement sorted set commands (zadd, zrem, zscore, zcard, zcount, zincrby)
- [x] Update pyproject.toml for maturin
- [x] Update client.py to use _native module
- [x] Remove _ffi.py
- [x] Build and test with maturin

### Session 2 (Optional): Go SDK Evaluation
- [ ] Benchmark current CGO performance
- [ ] Evaluate uniffi-rs benefits
- [ ] Decide go/no-go

---

## Success Criteria

### Python SDK
- [x] All existing tests pass
- [x] No CFFI dependency
- [x] Native module loads correctly on macOS (arm64)
- [ ] Native module loads correctly on macOS (x86_64) - needs testing
- [ ] Native module loads correctly on Linux (x86_64) - needs testing
- [ ] Performance: at least 2x faster than CFFI path - needs benchmarking

### Go SDK
- [x] Current CGO implementation remains stable
- [x] Document decision on uniffi-rs conversion (Recommendation: keep CGO)

---

## Task 3: Cross-SDK Oracle Testing

**Status**: IN PROGRESS (Phase 3.1 + 3.2 complete)
**Priority**: HIGH
**Location**: `sdks/oracle/`

### Problem

Each SDK currently has its own test suite with duplicated test logic. This leads to:
- Tests may diverge over time (different assertions for same operations)
- Maintenance burden multiplies with each SDK
- No guarantee SDKs produce identical output for same operations

### Solution: Shared Oracle Test Specification

Create a YAML-based test specification that all SDKs execute against, comparing outputs to ensure consistency.

### Architecture

```
sdks/oracle/
├── spec/
│   ├── strings.yaml      # String command tests
│   ├── hashes.yaml       # Hash command tests
│   ├── lists.yaml        # List command tests
│   ├── sets.yaml         # Set command tests
│   ├── zsets.yaml        # Sorted set command tests
│   ├── keys.yaml         # Key/TTL command tests
│   └── scan.yaml         # Scan command tests
├── runners/
│   ├── python_runner.py  # Python SDK executor
│   ├── ts_runner.ts      # TypeScript SDK executor
│   └── rust_runner.rs    # Rust core executor (baseline)
├── Makefile              # make test-oracle
└── README.md
```

### Test Specification Format

```yaml
# spec/strings.yaml
name: String Commands
tests:
  - name: SET and GET roundtrip
    setup: []
    operations:
      - { cmd: SET, args: ["key", "value"], expect: true }
      - { cmd: GET, args: ["key"], expect: "value" }

  - name: INCR on new key
    setup: []
    operations:
      - { cmd: INCR, args: ["counter"], expect: 1 }
      - { cmd: INCR, args: ["counter"], expect: 2 }
      - { cmd: GET, args: ["counter"], expect: "2" }

  - name: MGET multiple keys
    setup:
      - { cmd: SET, args: ["k1", "v1"] }
      - { cmd: SET, args: ["k2", "v2"] }
    operations:
      - { cmd: MGET, args: ["k1", "k2", "k3"], expect: ["v1", "v2", null] }

  - name: SET with EX expiration
    setup: []
    operations:
      - { cmd: SET, args: ["key", "value"], kwargs: { ex: 60 }, expect: true }
      - { cmd: TTL, args: ["key"], expect: { range: [59, 60] } }
```

### Comparison Modes

```yaml
# Expectation types
expect: "value"                    # Exact match (string)
expect: 42                         # Exact match (int)
expect: 3.14                       # Approximate match (float, ±0.001)
expect: true                       # Boolean
expect: null                       # None/nil
expect: ["a", "b", "c"]           # Ordered list
expect: { set: ["a", "b", "c"] }  # Unordered set
expect: { range: [59, 60] }       # Numeric range
expect: { approx: 3.14, tol: 0.01 }  # Float with tolerance
expect: { type: "bytes" }         # Type check only
expect: { contains: "error" }     # Substring match (for errors)
```

### Runner Implementation

**Python Runner** (`runners/python_runner.py`):
```python
import yaml
from redlite import Redlite

def run_spec(spec_file: str) -> dict:
    with open(spec_file) as f:
        spec = yaml.safe_load(f)

    results = {"passed": 0, "failed": 0, "errors": []}

    for test in spec["tests"]:
        with Redlite.open(":memory:") as db:
            # Run setup
            for op in test.get("setup", []):
                execute_cmd(db, op)

            # Run operations and compare
            for op in test["operations"]:
                actual = execute_cmd(db, op)
                if not compare(actual, op["expect"]):
                    results["failed"] += 1
                    results["errors"].append({
                        "test": test["name"],
                        "cmd": op["cmd"],
                        "expected": op["expect"],
                        "actual": actual
                    })
                else:
                    results["passed"] += 1

    return results
```

### Cross-SDK Comparison

The oracle can run in two modes:

**1. Spec Validation Mode** (default):
```bash
make test-oracle-python   # Run specs against Python SDK
make test-oracle-ts       # Run specs against TypeScript SDK
make test-oracle          # Run all SDKs, compare results
```

**2. Direct Comparison Mode**:
```bash
# Run same random operations on all SDKs, compare outputs directly
make test-oracle-compare --seed 12345
```

### Phase 3.1: Create Spec Format & Parser - ✅ COMPLETE (2026-01-14)

- [x] Design YAML spec format (see `oracle/spec/*.yaml`)
- [x] Implement spec parser (Python runner)
- [x] Create initial specs: 73 tests across 3 spec files
  - `strings.yaml` - 29 tests (GET, SET, MGET, MSET, INCR, etc.)
  - `hashes.yaml` - 18 tests (HSET, HGET, HGETALL, HMGET, etc.)
  - `keys.yaml` - 26 tests (DEL, EXISTS, TYPE, TTL, EXPIRE, etc.)

### Phase 3.2: Implement Runners - ✅ COMPLETE (2026-01-15)

- [x] Python runner (`runners/python_runner.py`)
- [x] TypeScript runner (`runners/ts_runner.js`)
- [ ] Rust runner (baseline reference) - optional for future

### Phase 3.3: Add Data Structure Specs - ✅ COMPLETE (2026-01-15)

- [x] `lists.yaml` - 22 tests (LPUSH, RPUSH, LPOP, RPOP, LLEN, LRANGE, LINDEX)
- [x] `sets.yaml` - 16 tests (SADD, SREM, SMEMBERS, SISMEMBER, SCARD)
- [x] `zsets.yaml` - 26 tests (ZADD, ZREM, ZSCORE, ZCARD, ZCOUNT, ZINCRBY, ZRANGE, ZREVRANGE)
- [x] Runners normalized to match Redis behavior (LPOP/RPOP single value vs array)

**Total**: 137 tests passing for both Python and TypeScript SDKs

### Phase 3.4: CI Integration

- [x] Add `make test-oracle-python` to oracle/Makefile
- [x] Add `make test-ts` for TypeScript SDK
- [x] Add `make test` to run all SDK oracle tests
- [ ] Run on all PRs that touch SDK code
- [ ] Generate comparison report

### Phase 3.5: Future Specs

- [ ] `scan.yaml` - SCAN, HSCAN, SSCAN, ZSCAN cursor iteration
- [ ] Migrate SDK-specific tests to oracle specs
- [ ] Remove duplicate test code from SDKs

### Benefits

1. **Single source of truth** for expected behavior
2. **Automatic consistency** across Python/TypeScript/Go SDKs
3. **Less maintenance** as commands are added
4. **Catch regressions** when one SDK diverges
5. **Documentation** - specs serve as executable docs

### Keep SDK-Specific Tests For

- Type coercion edge cases (`db.set("key", 42)` → bytes)
- Language-idiomatic APIs (`db.hset("h", a="1")` kwargs)
- Error handling / closed connection behavior
- Async/concurrency behavior (SDK-specific)
- Performance benchmarks

---

---

## Upcoming SDKs

### SDK Status Overview

| Language | Status | Binding Type | Priority |
|----------|--------|--------------|----------|
| **Rust** | ✅ Native | Use `redlite` crate directly | - |
| **Python** | ✅ Complete | PyO3 | - |
| **TypeScript** | ✅ Complete | napi-rs | - |
| **Go** | ✅ Complete | CGO | - |
| **Dart** | ✅ Complete | FFI | - |
| **Kotlin** | ✅ Complete | JNI | - |
| **Java** | ✅ Complete | JNI | - |
| **Swift** | ✅ Complete | C FFI | - |
| **C#/.NET** | ✅ Complete | P/Invoke | - |
| **WASM** | 📋 Planned | wasm-bindgen | MEDIUM |
| **Ruby** | 📋 Planned | FFI gem / magnus | MEDIUM |
| **C++** | ✅ Complete | C++17 header-only | - |
| **Zig** | 📋 Planned | C ABI | LOW |
| **PHP** | 📋 Planned | PHP FFI | LOW |
| **Elixir** | 📋 Planned | Rustler NIFs | LOW |
| **Lua** | 📋 Planned | LuaJIT FFI | LOW |
| **Scala** | 📋 Planned | JNI (reuse Java) | LOW |
| **Clojure** | 📋 Planned | JNI (reuse Java) | LOW |
| **F#** | 📋 Planned | P/Invoke (reuse .NET) | LOW |
| **OCaml** | 📋 Planned | ctypes | LOW |
| **Haskell** | 📋 Planned | C FFI | LOW |
| **Julia** | 📋 Planned | ccall | LOW |
| **R** | 📋 Planned | .Call / extendr | LOW |
| **Nim** | 📋 Planned | C FFI | LOW |
| **Crystal** | 📋 Planned | C bindings | LOW |
| **V** | 📋 Planned | C interop | LOW |
| **D** | 📋 Planned | extern(C) | LOW |
| **Perl** | 📋 Planned | FFI::Platypus | LOW |
| **Common Lisp** | 📋 Planned | CFFI | LOW |
| **Racket** | 📋 Planned | FFI | LOW |
| **Erlang** | 📋 Planned | NIFs | LOW |
| **Objective-C** | 📋 Planned | C interop | LOW |
| **Fortran** | 📋 Planned | ISO_C_BINDING | LOW |
| **COBOL** | 📋 Planned | GnuCOBOL C interop | ENTERPRISE |
| **Ada** | 📋 Planned | pragma Import | LOW |
| **Prolog** | 📋 Planned | SWI-Prolog FFI | LOW |
| **Tcl** | 📋 Planned | Tcl C extension | LOW |
| **APL/J/K** | 📋 Planned | Dyalog FFI | LOW |
| **Forth** | 📋 Planned | C FFI | LOW |
| **MATLAB** | 📋 Planned | MEX | MEDIUM |
| **PowerShell** | 📋 Planned | .NET wrapper | LOW |
| **Bash** | 📋 Planned | CLI/builtin | LOW |
| **GDScript** | 📋 Planned | GDExtension | LOW |
| **x86 Assembly** | 📋 Planned | C ABI | HARDCORE |
| **Brainfuck** | 📋 Planned | C transpiler | MEME |
| **LOLCODE** | 📋 Planned | Interpreter ext | MEME |
| **Rockstar** | 📋 Planned | Interpreter ext | MEME |
| **Shakespeare** | 📋 Planned | Transpiler | MEME |
| **Piet** | 📋 Planned | Image generator | MEME |
| **Whitespace** | 📋 Planned | Interpreter ext | MEME |
| **Scratch** | 📋 Planned | Scratch Extension | EDUCATIONAL |
| **Tabloid** | 📋 Planned | Interpreter ext | MEME |

**Total: 7 complete + 47 planned = 54 SDKs**

### Planned SDK Details

---

#### Swift SDK

**Status**: ✅ COMPLETE (Session 45)
**Priority**: -
**Location**: `sdks/redlite-swift/`
**Binding Type**: Swift Package with C FFI (module map)

**Structure**:
```
sdks/redlite-swift/
├── Package.swift                      # Swift Package Manager manifest
├── Makefile                           # Build/test commands
├── README.md                          # Documentation
├── Sources/
│   ├── CRedlite/                      # C module for FFI bridging
│   │   ├── include/
│   │   │   ├── module.modulemap
│   │   │   └── redlite.h
│   │   └── shim.c
│   └── Redlite/
│       ├── Database.swift             # Main Database class (~80 lines)
│       ├── RedliteError.swift         # Error types (~30 lines)
│       ├── FFI/
│       │   ├── FFITypes.swift         # RAII wrappers for C types (~130 lines)
│       │   └── FFIHelpers.swift       # Memory management utilities (~110 lines)
│       └── Commands/
│           ├── StringCommands.swift   # GET, SET, INCR, etc. (~300 lines)
│           ├── KeyCommands.swift      # DEL, EXISTS, TTL, etc. (~180 lines)
│           ├── HashCommands.swift     # HGET, HSET, etc. (~200 lines)
│           ├── ListCommands.swift     # LPUSH, RPUSH, etc. (~160 lines)
│           ├── SetCommands.swift      # SADD, SMEMBERS, etc. (~110 lines)
│           └── SortedSetCommands.swift # ZADD, ZRANGE, etc. (~200 lines)
├── Tests/
│   └── RedliteTests/
│       ├── StringCommandsTests.swift
│       ├── KeyCommandsTests.swift
│       ├── HashCommandsTests.swift
│       ├── ListCommandsTests.swift
│       ├── SetCommandsTests.swift
│       └── SortedSetCommandsTests.swift
├── Frameworks/                        # Pre-built XCFramework (optional)
└── scripts/
    └── create-xcframework.sh          # Build script for all Apple platforms
```

**Swift API Design**:
```swift
import Redlite

// Open database
let db = try Database.openMemory()
let db = try Database(path: "/path/to/db.sqlite")
let db = try Database.open(path: "/path/to/db.sqlite", cacheMB: 128)

// Strings
try db.set("key", value: "value")
let val = try db.getString("key")  // "value"
try db.incr("counter")

// Hashes
try db.hset("user", ["name": "Alice", "age": "30"])
let all = try db.hgetallStrings("user")

// Lists, Sets, Sorted Sets
try db.rpush("list", "a", "b", "c")
try db.sadd("set", "x", "y", "z")
try db.zadd("zset", (1.0, "a"), (2.0, "b"))
```

**Implementation Features**:
- Thread-safe via NSLock + @unchecked Sendable
- RAII-style memory management (deinit calls redlite_close)
- Returns Optional<T> for nullable values
- Throwing functions with RedliteError
- Supports iOS 13+, macOS 10.15+, tvOS 13+, watchOS 6+
- 55+ methods across 6 command categories
- XCTest unit tests for all commands
- XCFramework build script for distribution

**Build Commands**:
```bash
# Build FFI library first
make build-ffi

# Build Swift package
make build

# Run tests
make test

# Build XCFramework for all Apple platforms
make build-xcframework
```

---

#### C#/.NET SDK

**Status**: ✅ COMPLETE (Session 44)
**Priority**: -
**Location**: `sdks/redlite-dotnet/`
**Binding Type**: P/Invoke with native library

**Structure**:
```
sdks/redlite-dotnet/
├── Redlite.csproj              # .NET 6/7/8 project
├── src/
│   ├── RedliteDb.cs            # Main database class (~900 lines)
│   ├── NativeMethods.cs        # P/Invoke declarations (~400 lines)
│   ├── SetOptions.cs           # SET command options
│   ├── ZMember.cs              # Sorted set member struct
│   └── RedliteException.cs     # Custom exception
├── tests/
│   ├── Redlite.Tests.csproj
│   ├── StringTests.cs          # 17 tests
│   ├── KeyTests.cs             # 15 tests
│   ├── HashTests.cs            # 10 tests
│   ├── ListTests.cs            # 12 tests
│   ├── SetTests.cs             # 7 tests
│   └── ZSetTests.cs            # 12 tests
└── README.md
```

**C# API Design**:
```csharp
using Redlite;

using var db = RedliteDb.OpenMemory();

// Strings
db.Set("key", "value");
var val = db.GetString("key");  // "value"
db.Incr("counter");

// Hashes
db.HSet("user", new Dictionary<string, string> {
    {"name", "Alice"}, {"age", "30"}
});
var all = db.HGetAll("user");

// Lists, Sets, Sorted Sets
db.RPush("list", "a", "b", "c");
db.SAdd("set", "x", "y", "z");
db.ZAdd("zset", new ZMember(1.0, "a"), new ZMember(2.0, "b"));
```

**Implementation Features**:
- IDisposable pattern for resource cleanup
- Nullable reference types enabled (C# 8+)
- Return `T?` for nullable results
- 73 unit tests using xUnit
- Oracle test runner for cross-SDK validation

---

#### Ruby SDK

**Status**: PLANNED
**Priority**: MEDIUM
**Location**: `sdks/redlite-ruby/`
**Binding Type**: FFI gem or native C extension

**Structure**:
```
sdks/redlite-ruby/
├── redlite.gemspec
├── lib/
│   ├── redlite.rb               # Main entry
│   ├── redlite/
│   │   ├── client.rb            # Unified client
│   │   ├── embedded_db.rb       # FFI wrapper
│   │   ├── namespaces/
│   │   │   ├── fts.rb
│   │   │   ├── vector.rb
│   │   │   ├── geo.rb
│   │   │   └── history.rb
│   │   └── version.rb
├── ext/                         # If using native extension
│   └── redlite/
│       └── extconf.rb
└── spec/
    ├── strings_spec.rb
    ├── hashes_spec.rb
    └── ...
```

**Implementation Notes**:
- Option A: `ffi` gem (simpler, portable)
- Option B: Native C extension with `rb_define_*` (faster)
- Consider magnus (Rust → Ruby bindings) as alternative
- Gem should include prebuilt binaries for common platforms

---

#### C++ SDK

**Status**: ✅ COMPLETE (Session 43)
**Priority**: MEDIUM
**Location**: `sdks/redlite-cpp/`
**Binding Type**: Header-only C++17 wrapper around C FFI

**Structure**:
```
sdks/redlite-cpp/
├── include/
│   └── redlite/
│       └── redlite.hpp          # Modern C++17 API
├── tests/
│   ├── test_strings.cpp         # 20 tests
│   ├── test_keys.cpp            # 12 tests
│   ├── test_hashes.cpp          # 12 tests
│   ├── test_lists.cpp           # 13 tests
│   ├── test_sets.cpp            # 8 tests
│   └── test_zsets.cpp           # 12 tests
├── examples/
│   └── basic.cpp
├── CMakeLists.txt
├── Makefile
├── README.md
└── redlite.pc.in
```

**C++ API Design**:
```cpp
#include <redlite/redlite.hpp>
using namespace redlite;

auto db = Database::open_memory();

// Strings
db.set("key", "value");
auto val = db.get("key");  // std::optional<std::string>
db.incr("counter");

// Hashes
db.hset("user", {{"name", "Alice"}, {"age", "30"}});
auto all = db.hgetall("user");

// Lists, Sets, Sorted Sets
db.rpush("list", {"a", "b", "c"});
db.sadd("set", {"x", "y", "z"});
db.zadd("zset", {{1.0, "a"}, {2.0, "b"}});
```

**Implementation Features**:
- Header-only library (880+ lines)
- RAII resource management
- Modern C++17 API: `std::optional`, `std::string_view`, move semantics
- 77 unit tests using Catch2
- CMake + pkg-config support

---

#### C SDK (Raw FFI)

**Status**: ✅ COMPLETE (via redlite-ffi)
**Location**: `crates/redlite-ffi/redlite.h`
**Binding Type**: Auto-generated C header

The C FFI is already available in `crates/redlite-ffi/`. It provides the low-level
C API that the C++ SDK and other language bindings use.

**Usage**:
```c
#include "redlite.h"

RedliteDb* db = redlite_open_memory();
redlite_set(db, "key", (uint8_t*)"value", 5, 0);
RedliteBytes result = redlite_get(db, "key");
// use result.data, result.len
redlite_free_bytes(result);
redlite_close(db);
```

---

#### Zig SDK

**Status**: PLANNED
**Priority**: LOW
**Location**: `sdks/redlite-zig/`
**Binding Type**: C ABI interop

**Structure**:
```
sdks/redlite-zig/
├── build.zig
├── src/
│   ├── redlite.zig              # Main module
│   └── c.zig                    # C bindings import
└── tests/
    └── test_basic.zig
```

**Implementation Notes**:
- Zig has excellent C interop, use C SDK as base
- `@cImport` to import redlite.h directly
- Provide Zig-idiomatic wrapper with slices and optionals
- Cross-compile support for all targets

---

#### PHP SDK

**Status**: PLANNED
**Priority**: LOW
**Location**: `sdks/redlite-php/`
**Binding Type**: PHP FFI (PHP 7.4+) or native extension

**Structure**:
```
sdks/redlite-php/
├── composer.json
├── src/
│   ├── Redlite.php              # Main client
│   ├── EmbeddedDb.php           # FFI wrapper
│   └── Namespaces/
│       ├── FTSNamespace.php
│       ├── VectorNamespace.php
│       ├── GeoNamespace.php
│       └── HistoryNamespace.php
└── tests/
    ├── StringsTest.php
    └── ...
```

**Implementation Notes**:
- PHP FFI is cleanest approach (PHP 7.4+)
- Alternative: PECL extension (more complex, better performance)
- Composer package with bundled binaries
- Consider Laravel/Symfony integration

---

#### Elixir SDK

**Status**: PLANNED
**Priority**: LOW
**Location**: `sdks/redlite-elixir/`
**Binding Type**: Rustler (Rust NIFs)

**Structure**:
```
sdks/redlite-elixir/
├── mix.exs
├── lib/
│   ├── redlite.ex               # Main module
│   ├── redlite/
│   │   ├── native.ex            # NIF wrapper
│   │   └── namespaces/
│   │       ├── fts.ex
│   │       ├── vector.ex
│   │       ├── geo.ex
│   │       └── history.ex
├── native/
│   └── redlite_nif/
│       ├── Cargo.toml
│       └── src/lib.rs           # Rustler NIFs
└── test/
    ├── strings_test.exs
    └── ...
```

**Implementation Notes**:
- Rustler is the standard for Rust → Elixir
- NIFs run in BEAM VM, need to be careful about blocking
- Consider dirty schedulers for long operations
- Hex package with precompiled NIFs

---

#### Lua SDK

**Status**: PLANNED
**Priority**: LOW
**Location**: `sdks/redlite-lua/`
**Binding Type**: LuaJIT FFI or C module

**Structure**:
```
sdks/redlite-lua/
├── redlite.lua                  # FFI wrapper (LuaJIT)
├── redlite/
│   ├── init.lua
│   ├── client.lua
│   └── namespaces/
│       ├── fts.lua
│       ├── vector.lua
│       ├── geo.lua
│       └── history.lua
├── src/                         # If using C module
│   └── redlite.c
└── spec/
    └── redlite_spec.lua
```

**Implementation Notes**:
- LuaJIT FFI for best performance (LuaJIT only)
- C module for standard Lua 5.x compatibility
- LuaRocks package
- Common use case: Redis replacement in game servers

---

---

#### Scala SDK

**Status**: PLANNED
**Priority**: LOW
**Location**: `sdks/redlite-scala/`
**Binding Type**: JNI (reuse Java bindings)

**Structure**:
```
sdks/redlite-scala/
├── build.sbt
├── src/main/scala/com/redlite/
│   ├── Redlite.scala
│   ├── EmbeddedDb.scala
│   └── namespaces/
└── src/test/scala/com/redlite/
```

**Implementation Notes**:
- Reuse `redlite-jni` native library from Java SDK
- Scala-idiomatic wrapper with Option, Try, implicits
- sbt build with Maven Central publishing

---

#### Clojure SDK

**Status**: PLANNED
**Priority**: LOW
**Location**: `sdks/redlite-clojure/`
**Binding Type**: JNI (reuse Java bindings)

**Structure**:
```
sdks/redlite-clojure/
├── deps.edn
├── src/redlite/
│   ├── core.clj
│   └── namespaces.clj
└── test/redlite/
```

**Implementation Notes**:
- Wrap Java SDK with Clojure idioms
- Use `with-open` for resource management
- Keywords and maps for options
- Clojars publishing

---

#### F# SDK

**Status**: PLANNED
**Priority**: LOW
**Location**: `sdks/redlite-fsharp/`
**Binding Type**: P/Invoke (reuse .NET bindings)

**Structure**:
```
sdks/redlite-fsharp/
├── Redlite.FSharp.fsproj
├── src/
│   ├── Redlite.fs
│   └── Namespaces.fs
└── tests/
```

**Implementation Notes**:
- Wrap C# SDK with F# idioms
- Option types, Result types, computation expressions
- Async workflows support
- NuGet package

---

#### OCaml SDK

**Status**: PLANNED
**Priority**: LOW
**Location**: `sdks/redlite-ocaml/`
**Binding Type**: C FFI via ctypes

**Structure**:
```
sdks/redlite-ocaml/
├── dune-project
├── lib/
│   ├── redlite.ml
│   ├── redlite.mli
│   └── stubs/
└── test/
```

**Implementation Notes**:
- Use `ctypes` library for C FFI
- Or `ocaml-rust` for direct Rust bindings
- opam package

---

#### Haskell SDK

**Status**: PLANNED
**Priority**: LOW
**Location**: `sdks/redlite-haskell/`
**Binding Type**: C FFI

**Structure**:
```
sdks/redlite-haskell/
├── redlite.cabal
├── src/
│   ├── Database/Redlite.hs
│   ├── Database/Redlite/FFI.hs
│   └── Database/Redlite/Commands.hs
└── test/
```

**Implementation Notes**:
- Use `hsc2hs` or inline-c for FFI
- ByteString for binary data
- ResourceT for safe resource management
- Hackage publishing

---

#### Julia SDK

**Status**: PLANNED
**Priority**: LOW
**Location**: `sdks/redlite-julia/`
**Binding Type**: ccall / CBinding.jl

**Structure**:
```
sdks/redlite-julia/
├── Project.toml
├── src/
│   ├── Redlite.jl
│   └── commands.jl
└── test/
```

**Implementation Notes**:
- Julia's `ccall` is excellent for C interop
- Good for data science / ML workflows
- JuliaHub / General registry publishing

---

#### R SDK

**Status**: PLANNED
**Priority**: LOW
**Location**: `sdks/redlite-r/`
**Binding Type**: C via .Call interface

**Structure**:
```
sdks/redlite-r/
├── DESCRIPTION
├── NAMESPACE
├── R/
│   ├── redlite.R
│   └── commands.R
├── src/
│   └── init.c
└── tests/
```

**Implementation Notes**:
- Use `.Call` interface to C
- Or `extendr` for Rust → R bindings
- CRAN package
- Pairs well with data.table / tidyverse workflows

---

#### Nim SDK

**Status**: PLANNED
**Priority**: LOW
**Location**: `sdks/redlite-nim/`
**Binding Type**: C FFI

**Structure**:
```
sdks/redlite-nim/
├── redlite.nimble
├── src/
│   ├── redlite.nim
│   └── redlite/
│       ├── ffi.nim
│       └── commands.nim
└── tests/
```

**Implementation Notes**:
- Nim has excellent C interop with `{.importc.}`
- Nimble package

---

#### Crystal SDK

**Status**: PLANNED
**Priority**: LOW
**Location**: `sdks/redlite-crystal/`
**Binding Type**: C bindings

**Structure**:
```
sdks/redlite-crystal/
├── shard.yml
├── src/
│   ├── redlite.cr
│   └── redlite/
│       ├── lib.cr
│       └── commands.cr
└── spec/
```

**Implementation Notes**:
- Crystal has clean C binding syntax with `@[Link]`
- Ruby-like syntax, easy to learn
- shards package manager

---

#### V SDK

**Status**: PLANNED
**Priority**: LOW
**Location**: `sdks/redlite-v/`
**Binding Type**: C interop

**Structure**:
```
sdks/redlite-v/
├── v.mod
├── redlite.v
└── tests/
```

**Implementation Notes**:
- V has simple C interop
- Single file is often sufficient
- Very fast compilation

---

#### D SDK

**Status**: PLANNED
**Priority**: LOW
**Location**: `sdks/redlite-d/`
**Binding Type**: C interface / extern(C)

**Structure**:
```
sdks/redlite-d/
├── dub.json
├── source/
│   └── redlite.d
└── tests/
```

**Implementation Notes**:
- D has excellent C interop
- `extern(C)` for function declarations
- DUB package registry

---

#### Perl SDK

**Status**: PLANNED
**Priority**: LOW
**Location**: `sdks/redlite-perl/`
**Binding Type**: XS or FFI::Platypus

**Structure**:
```
sdks/redlite-perl/
├── Makefile.PL
├── lib/
│   └── Redlite.pm
├── xs/                          # If using XS
│   └── Redlite.xs
└── t/
```

**Implementation Notes**:
- FFI::Platypus is cleaner than XS
- CPAN distribution

---

#### Common Lisp SDK

**Status**: PLANNED
**Priority**: LOW
**Location**: `sdks/redlite-cl/`
**Binding Type**: CFFI

**Structure**:
```
sdks/redlite-cl/
├── redlite.asd
├── src/
│   ├── package.lisp
│   ├── ffi.lisp
│   └── redlite.lisp
└── tests/
```

**Implementation Notes**:
- CFFI is the standard for C interop
- Works across SBCL, CCL, ECL, etc.
- Quicklisp distribution

---

#### Racket SDK

**Status**: PLANNED
**Priority**: LOW
**Location**: `sdks/redlite-racket/`
**Binding Type**: FFI

**Structure**:
```
sdks/redlite-racket/
├── info.rkt
├── main.rkt
├── private/
│   └── ffi.rkt
└── tests/
```

**Implementation Notes**:
- Racket FFI is well-documented
- Package server distribution

---

#### Erlang SDK

**Status**: PLANNED
**Priority**: LOW
**Location**: `sdks/redlite-erlang/`
**Binding Type**: NIFs (like Elixir)

**Structure**:
```
sdks/redlite-erlang/
├── rebar.config
├── src/
│   ├── redlite.erl
│   └── redlite_nif.erl
├── c_src/
│   └── redlite_nif.c
└── test/
```

**Implementation Notes**:
- Similar to Elixir but pure Erlang
- Rustler also supports Erlang
- hex.pm publishing

---

#### WASM SDK

**Status**: PLANNED
**Priority**: MEDIUM
**Location**: `sdks/redlite-wasm/`
**Binding Type**: wasm-bindgen

**Structure**:
```
sdks/redlite-wasm/
├── Cargo.toml
├── src/
│   └── lib.rs
├── pkg/                         # Generated
│   ├── redlite.js
│   ├── redlite.d.ts
│   └── redlite_bg.wasm
└── examples/
    └── web/
```

**Implementation Notes**:
- Compile Redlite to WebAssembly
- wasm-bindgen for JS interop
- In-browser embedded database
- npm package for distribution
- Consider memory limitations

---

#### Objective-C SDK

**Status**: PLANNED
**Priority**: LOW
**Location**: `sdks/redlite-objc/`
**Binding Type**: C interop (bridging header)

**Structure**:
```
sdks/redlite-objc/
├── Redlite.xcodeproj
├── Sources/
│   ├── RDLDatabase.h
│   ├── RDLDatabase.m
│   └── Redlite-Bridging-Header.h
└── Tests/
```

**Implementation Notes**:
- Objective-C has seamless C interop
- CocoaPods / SPM distribution
- Legacy iOS/macOS support (Swift preferred for new projects)

---

#### Fortran SDK

**Status**: PLANNED
**Priority**: LOW
**Location**: `sdks/redlite-fortran/`
**Binding Type**: ISO_C_BINDING

**Structure**:
```
sdks/redlite-fortran/
├── fpm.toml
├── src/
│   └── redlite.f90
└── test/
```

**Implementation Notes**:
- Modern Fortran (2003+) has good C interop via ISO_C_BINDING
- Niche but useful for scientific computing
- fpm (Fortran Package Manager)

---

---

### Esoteric & Unusual SDKs

These languages rarely (or never) get Redis client support. Why not be first?

---

#### Brainfuck SDK

**Status**: PLANNED
**Priority**: MEME
**Location**: `sdks/redlite-brainfuck/`
**Binding Type**: Transpile to C, link with C SDK

**Example Usage**:
```brainfuck
Memory layout:
[0] = db pointer (via C interop hack)
[1] = key buffer
[2] = value buffer

SET "a" "hello":
++++++++[>+++++++++++++<-]>. (print 'h')
... (this would be insane)
```

**Implementation Notes**:
- Serious answer: Write a Brainfuck-to-C transpiler, link with C SDK
- Or: Create a Brainfuck interpreter in Rust that has Redlite built-in
- The real SDK is the friends we made along the way
- Would be world's first Brainfuck Redis client

---

#### COBOL SDK

**Status**: PLANNED
**Priority**: ENTERPRISE
**Location**: `sdks/redlite-cobol/`
**Binding Type**: C interop via GnuCOBOL

**Structure**:
```
sdks/redlite-cobol/
├── copybooks/
│   └── REDLITE.cpy
├── src/
│   └── REDLITE.cob
└── examples/
    └── SETGET.cob
```

**Example Usage**:
```cobol
       IDENTIFICATION DIVISION.
       PROGRAM-ID. REDIS-EXAMPLE.
       DATA DIVISION.
       WORKING-STORAGE SECTION.
       01 WS-KEY    PIC X(50) VALUE "CUSTOMER-001".
       01 WS-VALUE  PIC X(100) VALUE "JOHN DOE".
       01 WS-RESULT PIC X(100).
       PROCEDURE DIVISION.
           CALL "REDLITE-SET" USING WS-KEY WS-VALUE.
           CALL "REDLITE-GET" USING WS-KEY WS-RESULT.
           DISPLAY "VALUE: " WS-RESULT.
           STOP RUN.
```

**Implementation Notes**:
- GnuCOBOL compiles to C, so C interop is possible
- Banks still run COBOL - they might actually want this
- First Redis client for mainframe migration projects

---

#### Ada SDK

**Status**: PLANNED
**Priority**: LOW
**Location**: `sdks/redlite-ada/`
**Binding Type**: C interface pragma

**Structure**:
```
sdks/redlite-ada/
├── redlite.gpr
├── src/
│   ├── redlite.ads
│   └── redlite.adb
└── tests/
```

**Implementation Notes**:
- Ada has strong C interop via `pragma Import`
- Used in aerospace, defense, rail systems
- SPARK subset for formally verified Redis operations (overkill but cool)

---

#### Prolog SDK

**Status**: PLANNED
**Priority**: LOW
**Location**: `sdks/redlite-prolog/`
**Binding Type**: SWI-Prolog FFI

**Example Usage**:
```prolog
:- use_module(redlite).

store_user(Id, Name) :-
    redlite_set(user(Id), Name).

get_user(Id, Name) :-
    redlite_get(user(Id), Name).

?- store_user(1, "Alice"), get_user(1, X).
X = "Alice".
```

**Implementation Notes**:
- SWI-Prolog has C FFI
- Logic programming + key-value store = interesting patterns
- Query your Redis with backtracking!

---

#### Tcl SDK

**Status**: PLANNED
**Priority**: LOW
**Location**: `sdks/redlite-tcl/`
**Binding Type**: Tcl C extension

**Example Usage**:
```tcl
package require redlite

set db [redlite::open ":memory:"]
$db set mykey "hello world"
puts [$db get mykey]
$db close
```

**Implementation Notes**:
- Tcl is still heavily used in EDA (chip design) tools
- Simple C extension API
- Could replace Redis in Tcl-based automation scripts

---

#### APL/J/K SDK

**Status**: PLANNED
**Priority**: LOW
**Location**: `sdks/redlite-apl/`
**Binding Type**: Dyalog APL FFI or J/K C interface

**Example Usage (J)**:
```j
load 'redlite'
db =: redlite_open ':memory:'
'key' redlite_set db 'value'
redlite_get db 'key'
```

**Implementation Notes**:
- Array languages used in finance
- Dyalog APL has decent C FFI
- J and K also have C interfaces
- Store vectors and matrices efficiently

---

#### Forth SDK

**Status**: PLANNED
**Priority**: LOW
**Location**: `sdks/redlite-forth/`
**Binding Type**: C FFI

**Example Usage**:
```forth
: test-redlite
  s" :memory:" redlite-open  ( db )
  dup s" key" s" value" redlite-set drop
  dup s" key" redlite-get type cr
  redlite-close ;
```

**Implementation Notes**:
- Stack-based, very minimal
- Used in embedded systems, boot loaders
- gforth has C interface

---

#### LOLCODE SDK

**Status**: PLANNED
**Priority**: MEME
**Location**: `sdks/redlite-lolcode/`
**Binding Type**: Interpreter extension

**Example Usage**:
```lolcode
HAI 1.2
  CAN HAS REDLITE?

  I HAS A db ITZ OPENZ ":memory:"
  db SETZ "kitteh" 2 "cheezburger"

  I HAS A val
  val R db GETZ "kitteh"
  VISIBLE val BTW prints "cheezburger"

  db CLOSEZ
KTHXBYE
```

**Implementation Notes**:
- Would need to extend a LOLCODE interpreter
- Or transpile to another language
- Internet points guaranteed

---

#### Rockstar SDK

**Status**: PLANNED
**Priority**: MEME
**Location**: `sdks/redlite-rockstar/`
**Binding Type**: Interpreter extension

**Example Usage**:
```rockstar
Redis is calling
The database is mysterious
Put ":memory:" into the path
Knock on the database with the path

My key is "love"
My value is "all you need"
Whisper my key, my value to the database

The answer is silence
Listen to the database for my key
Say the answer
```

**Implementation Notes**:
- Rockstar: write code that looks like song lyrics
- Would need interpreter extension
- Perfect for DevOps karaoke

---

#### Shakespeare SDK

**Status**: PLANNED
**Priority**: MEME
**Location**: `sdks/redlite-shakespeare/`
**Binding Type**: Transpiler

**Example Usage**:
```
The Tragedy of Redis, a Key-Value Play.

Romeo, a key.
Juliet, a value.
The Database, a persistent store.

Act I: The Setting.
Scene I: The Connection.

[Enter The Database]

The Database:
  Open thyself to memory!

[Enter Romeo and Juliet]

Romeo:
  Thou art as lovely as the sum of thyself and a warm summer's day.

Juliet:
  Remember thyself.

The Database:
  Store Romeo's essence with Juliet's heart!
```

**Implementation Notes**:
- Shakespeare Programming Language is Turing-complete
- Transpile to C or another language
- Redis, but make it theatrical

---

#### Piet SDK

**Status**: PLANNED
**Priority**: MEME
**Location**: `sdks/redlite-piet/`
**Binding Type**: Image-based programming

**Implementation Notes**:
- Piet: programs are images
- Would need to generate images that represent Redis commands
- `SET key value` = specific color pattern
- The most beautiful Redis client ever made

---

#### Whitespace SDK

**Status**: PLANNED
**Priority**: MEME
**Location**: `sdks/redlite-whitespace/`
**Binding Type**: Interpreter extension

**Implementation Notes**:
- Whitespace: only spaces, tabs, and newlines
- The invisible Redis client
- Code reviews become impossible

---

#### MATLAB SDK

**Status**: PLANNED
**Priority**: MEDIUM
**Location**: `sdks/redlite-matlab/`
**Binding Type**: MEX (MATLAB Executable)

**Example Usage**:
```matlab
db = redlite.open(':memory:');
db.set('matrix', magic(5));
result = db.get('matrix');
disp(result);
db.close();
```

**Implementation Notes**:
- MEX files link C code into MATLAB
- Huge in academia and engineering
- Store matrices, time series, experiment data

---

#### Scratch SDK

**Status**: PLANNED
**Priority**: EDUCATIONAL
**Location**: `sdks/redlite-scratch/`
**Binding Type**: Scratch Extension

**Implementation Notes**:
- Visual programming for kids
- Custom Scratch blocks for Redis operations
- "When green flag clicked → SET score to 0"
- Learn databases at age 8

---

#### PowerShell SDK

**Status**: PLANNED
**Priority**: LOW
**Location**: `sdks/redlite-powershell/`
**Binding Type**: P/Invoke or C# wrapper

**Example Usage**:
```powershell
Import-Module Redlite

$db = Open-RedliteDb -Path ":memory:"
Set-RedliteValue -Db $db -Key "server" -Value "DC01"
Get-RedliteValue -Db $db -Key "server"
Close-RedliteDb -Db $db
```

**Implementation Notes**:
- Wrap the .NET SDK
- Native PowerShell cmdlets
- Useful for Windows automation

---

#### Bash SDK

**Status**: PLANNED
**Priority**: LOW
**Location**: `sdks/redlite-bash/`
**Binding Type**: CLI wrapper or loadable builtin

**Example Usage**:
```bash
source redlite.sh

redlite_open db ":memory:"
redlite_set $db "key" "value"
result=$(redlite_get $db "key")
echo "$result"
redlite_close $db
```

**Implementation Notes**:
- Option A: Shell functions wrapping CLI
- Option B: Bash loadable builtin (enable -f)
- For the truly dedicated shell scripter

---

#### GDScript SDK (Godot)

**Status**: PLANNED
**Priority**: LOW
**Location**: `sdks/redlite-godot/`
**Binding Type**: GDExtension (C++)

**Example Usage**:
```gdscript
extends Node

var db: Redlite

func _ready():
    db = Redlite.open(":memory:")
    db.set("player_score", 100)
    print(db.get("player_score"))

func _exit_tree():
    db.close()
```

**Implementation Notes**:
- GDExtension for Godot 4.x
- Embedded database for games
- Save games, leaderboards, game state

---

#### Tabloid SDK

**Status**: PLANNED
**Priority**: MEME
**Location**: `sdks/redlite-tabloid/`
**Binding Type**: Interpreter extension

**Example Usage**:
```tabloid
DISCOVER HOW TO testRedis WITH nothing
RUMOR HAS IT
    EXPERTS CLAIM db TO BE REDLITE OPEN OF ":memory:"

    YOU WON'T BELIEVE WHAT HAPPENS WHEN
        db SET OF "user" AND "Alice"
    END OF STORY

    EXPERTS CLAIM result TO BE db GET OF "user"

    YOU WON'T WANT TO MISS "SHOCKING: Database returned " PLUS result

    WHAT IF result IS ACTUALLY "Alice"
    RUMOR HAS IT
        YOU WON'T WANT TO MISS "TOTALLY RIGHT: Test passed!"
    END OF STORY
    LIES!
    RUMOR HAS IT
        YOU WON'T WANT TO MISS "COMPLETELY WRONG: Test failed!"
    END OF STORY

    db CLOSE OF nothing
END OF STORY

testRedis OF nothing
```

**Implementation Notes**:
- Tabloid: clickbait-headline programming language by @thesephist
- Extend interpreter with Redis bindings
- `YOU WON'T BELIEVE WHAT HAPPENS WHEN` for mutations
- `EXPERTS CLAIM` for variable assignment
- Perfect for engagement-driven development
- Your Redis operations will go VIRAL

---

#### x86 Assembly SDK

**Status**: PLANNED
**Priority**: HARDCORE
**Location**: `sdks/redlite-asm/`
**Binding Type**: Direct C ABI calls

**Example Usage (NASM)**:
```nasm
section .data
    path db ":memory:", 0
    key db "count", 0
    value db "42", 0

section .text
    extern redlite_open
    extern redlite_set
    extern redlite_get
    extern redlite_close

    global _start

_start:
    ; Open database
    mov rdi, path
    call redlite_open
    mov [db_handle], rax

    ; SET key value
    mov rdi, [db_handle]
    mov rsi, key
    mov rdx, value
    mov rcx, 2
    call redlite_set

    ; ... more assembly ...
```

**Implementation Notes**:
- Pure assembly, calling C ABI
- For embedded systems or the criminally insane
- Maximum performance, minimum sanity

---

### Rust Usage (No Separate SDK Needed)

Rust applications use the `redlite` crate directly:

```toml
# Cargo.toml
[dependencies]
redlite = { path = "../crates/redlite" }
# or when published:
# redlite = "0.1"
```

```rust
use redlite::Db;

fn main() -> Result<(), redlite::Error> {
    let db = Db::open(":memory:")?;

    db.set("key", b"value", None)?;
    let value = db.get("key")?;

    Ok(())
}
```

No wrapper SDK is needed since Rust is the native implementation.

---

### SDK Implementation Checklist Template

For each new SDK:

- [ ] Project structure and build configuration
- [ ] Native bindings (FFI/JNI/etc.)
- [ ] Main client class with mode detection (embedded vs server)
- [ ] String commands (GET, SET, MGET, MSET, INCR, etc.)
- [ ] Key commands (DEL, EXISTS, TYPE, TTL, EXPIRE, etc.)
- [ ] Hash commands (HSET, HGET, HGETALL, etc.)
- [ ] List commands (LPUSH, RPUSH, LPOP, RPOP, LRANGE, etc.)
- [ ] Set commands (SADD, SREM, SMEMBERS, etc.)
- [ ] Sorted set commands (ZADD, ZREM, ZSCORE, ZRANGE, etc.)
- [ ] Namespace classes (FTS, Vector, Geo, History)
- [ ] Oracle test runner
- [ ] Unit tests
- [ ] Documentation / README
- [ ] Package/distribution setup

---

## References

- [PyO3 User Guide](https://pyo3.rs)
- [Maturin Documentation](https://maturin.rs)
- [uniffi-rs Documentation](https://mozilla.github.io/uniffi-rs/)
- [napi-rs (TypeScript SDK reference)](https://napi.rs)
- [Rustler (Elixir NIFs)](https://github.com/rusterlium/rustler)
- [magnus (Ruby bindings)](https://github.com/matsadler/magnus)
- [cbindgen (C header generation)](https://github.com/mozilla/cbindgen)
- TypeScript SDK implementation: `sdks/redlite-ts/src/lib.rs`
- redlite-dst Oracle Tests: `redlite-dst/tests/oracle.rs`
