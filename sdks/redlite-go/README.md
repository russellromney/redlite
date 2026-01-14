# Redlite Go SDK

Go SDK for Redlite - Redis API + SQLite durability.

## Install

```bash
go get github.com/russellromney/redlite/sdks/redlite-go
```

## Usage

### Unified API (Recommended)

```go
import "github.com/russellromney/redlite/sdks/redlite-go"

// Auto-detects mode from URL
db, _ := redlite.New(":memory:")              // Embedded (FFI)
db, _ := redlite.New("/path/to/db.db")        // Embedded file
db, _ := redlite.New("redis://localhost:6379") // Server mode

defer db.Close()

ctx := context.Background()
db.Set(ctx, "key", []byte("value"), 0)
val, _ := db.Get(ctx, "key")

// Check mode
db.Mode()  // "embedded" or "server"
```

### Direct Embedded (FFI)

```go
// No context, simpler API
db, _ := redlite.OpenEmbedded(":memory:")
db, _ := redlite.OpenEmbeddedWithCache("/path/to/db.db", 1000)  // 1GB cache

db.Set("key", []byte("value"), 0)
val, _ := db.Get("key")

db.HSet("hash", map[string][]byte{"field": []byte("value")})
db.LPush("list", []byte("a"), []byte("b"))
db.SAdd("set", []byte("member"))
db.ZAdd("zset", redlite.ZMemberScore{Score: 1.0, Member: []byte("a")})
```

### Server Mode (go-redis wrapper)

```go
// Full go-redis API + Redlite namespaces
r, _ := redlite.Connect("redis://localhost:6379")

r.Set(ctx, "key", "value", 0)
r.FTS.Search(ctx, "quick fox")
r.FT.Create(ctx, "idx", map[string]string{"title": "TEXT"})
r.Vector.Add(ctx, "embeddings", "doc1", []float64{0.1, 0.2})
```

## Build

Requires the native library:

```bash
# Build FFI library first
cd crates/redlite-ffi && cargo build --release

# Set library path
export CGO_LDFLAGS="-L/path/to/target/release"
export DYLD_LIBRARY_PATH="/path/to/target/release"  # macOS
export LD_LIBRARY_PATH="/path/to/target/release"    # Linux

# Build
cd sdks/redlite-go && go build ./...
```

## Test

```bash
go test -v ./...
```
