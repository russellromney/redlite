# LLM-Powered Documentation Generation

**TL;DR:** We don't manually write SDK docs. An LLM reads the SDK source code and generates docs from templates.

## Why?

- **Less maintenance** - Change code, regenerate docs
- **Always accurate** - LLM reads actual source, not stale examples
- **Cheaper than time** - $0.10 to regenerate vs hours of manual work
- **Multi-language consistency** - Same template → consistent docs

## How It Works

1. **Template** (`sdk-guide.template.md`) - Pseudocode structure with placeholders
2. **SDK Source** - Real code in `bindings/{language}/`
3. **LLM** - Claude reads both, generates real docs
4. **Output** - `docs/src/content/docs/sdks/{language}.md`

## Usage

```bash
# Generate Python docs
npm run generate:docs python

# Generate TypeScript docs
npm run generate:docs typescript

# Generate all SDK docs
npm run generate:all
```

Requires `ANTHROPIC_API_KEY` environment variable.

## Template Structure

The template uses pseudocode that works across languages:

```markdown
## Quick Start

```{{LANGUAGE}}
// Universal pseudocode
db = open("data.db")
db.set("key", "value")
value = db.get("key")
```

The LLM transforms this into language-specific syntax:

**Python:**
```python
db = Redlite.open('data.db')
db.set('key', 'value')
value = db.get('key')
```

**TypeScript:**
```typescript
const db = await Redlite.open('data.db');
await db.set('key', 'value');
const value = await db.get('key');
```

**Rust:**
```rust
let db = Db::open("data.db")?;
db.set("key", b"value", None)?;
let value = db.get("key")?;
```

## What the LLM Does

Given the template and SDK source, Claude:

1. **Replaces pseudocode** with actual language syntax
2. **Matches API signatures** from source code
3. **Adds framework examples** (FastAPI, Express, Axum, etc.)
4. **Includes language idioms** (decorators in Python, async in TS, error handling in Rust)
5. **Ensures correctness** - All code examples are runnable

## Cost

- **~$0.10 per SDK** (4000 input tokens, 8000 output tokens)
- **~$0.30 to regenerate all docs**
- **Compare to:** 4+ hours of manual writing per SDK

## When to Regenerate

- After adding new SDK features
- After changing API signatures
- Before releases
- When examples get stale

## Adding New SDKs

1. Add SDK config to `scripts/generate-docs.js`:
   ```javascript
   go: {
     language: 'Go',
     installCommand: 'go get github.com/you/redlite-go',
     sourcePath: 'bindings/go/src',
     mainFiles: ['client.go', 'embedded.go'],
   }
   ```

2. Generate docs:
   ```bash
   npm run generate:docs go
   ```

That's it.

## Editing Docs

**DON'T** edit the generated markdown files directly. They'll be overwritten.

**DO** edit:
- `templates/sdk-guide.template.md` - Structure and pseudocode
- Source code - LLM reads this for real examples
- `scripts/generate-docs.js` - Generation logic

Then regenerate.

## Examples

The template emphasizes **use cases over features**:

- ❌ "redlite supports HSET, HGET, HGETALL..."
- ✅ "Your CLI tool now has fast, persistent cache"

Because nobody gives a fuck about another Redis clone. They care that they can bundle a database without Docker.
