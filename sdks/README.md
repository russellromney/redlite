# Redlite SDKs

Language SDKs for redlite. Each SDK wraps an existing Redis client for the target language.

## Structure

```
sdks/
├── COMMANDS.md       # Auto-generated list of supported commands
├── TEMPLATE.md       # README template for all SDKs
├── python/           # Python SDK (redlite-py)
├── go/               # Go SDK (redlite-go) - planned
└── wasm/             # WASM bindings - experimental
```

## SDK Architecture

Each SDK follows this pattern:

1. **Wrap existing Redis client** - Don't reimplement the protocol
   - Python: wraps `redis-py`
   - Go: wraps `go-redis`
   - Node: wraps `ioredis` (removed - spawn issues)

2. **Two modes:**
   - **Server mode**: Connect to existing redlite/Redis server
   - **Embedded mode**: Start bundled redlite binary, connect via socket/FFI

3. **Namespace API for redlite features:**
   - `db.fts.*` - Full-text search
   - `db.history.*` - Version history
   - `db.vector.*` - Vector search
   - `db.geo.*` - Geospatial

## Updating SDKs

### Generate command list

```bash
make sdk-commands
```

Extracts all commands from `crates/redlite/src/server/mod.rs` → `sdks/COMMANDS.md`

### Update a specific SDK

```bash
make sdk-update lang=python
```

Uses Claude to update the SDK based on:
- `sdks/COMMANDS.md` - What commands to support
- `sdks/TEMPLATE.md` - README structure and voice

### Update all SDKs

```bash
make sdk-sync
```

Regenerates COMMANDS.md and updates all SDKs.

## Adding a New SDK

1. Create directory: `sdks/yourlang/`
2. Run: `make sdk-update lang=yourlang`
3. Claude will generate initial structure following the template

## Testing

Each SDK should have:
- Unit tests for namespaces
- Integration tests against real redlite server
- Example usage in README

## Current Status

| SDK | Status | Package | Server Mode | Embedded Mode |
|-----|--------|---------|-------------|---------------|
| Python | ✅ Complete | `redlite` on PyPI | ✅ | ✅ |
| Go | 📋 Planned | - | - | - |
| Node | ❌ Removed | - | - | Spawn issues |
| WASM | 🧪 Experimental | - | - | - |

## Philosophy

**Thin wrappers, not reimplementations.**

We don't rewrite the Redis protocol. We wrap battle-tested clients and add:
1. Embedded server management
2. Convenience APIs for redlite-specific features
3. Idiomatic language patterns

The heavy lifting (RESP protocol, connection pooling, etc.) is delegated to existing Redis clients.
