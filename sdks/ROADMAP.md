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

**Status**: PLANNED
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

### Phase 3.1: Create Spec Format & Parser

- [ ] Design YAML spec format
- [ ] Implement spec parser (Python, can be reused)
- [ ] Create initial specs from existing tests (~50 tests)

### Phase 3.2: Implement Runners

- [ ] Python runner with pytest integration
- [ ] TypeScript runner with vitest integration
- [ ] Rust runner (baseline reference)

### Phase 3.3: Integrate with CI

- [ ] Add `make test-oracle` to Makefile
- [ ] Run on all PRs that touch SDK code
- [ ] Generate comparison report

### Phase 3.4: Migrate Tests

- [ ] Identify SDK-specific tests to keep (type coercion, error handling)
- [ ] Migrate shared tests to oracle specs
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

## References

- [PyO3 User Guide](https://pyo3.rs)
- [Maturin Documentation](https://maturin.rs)
- [uniffi-rs Documentation](https://mozilla.github.io/uniffi-rs/)
- [napi-rs (TypeScript SDK reference)](https://napi.rs)
- TypeScript SDK implementation: `sdks/redlite-ts/src/lib.rs`
- redlite-dst Oracle Tests: `redlite-dst/tests/oracle.rs`
