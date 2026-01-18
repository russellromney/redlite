---
title: JSON
description: JSON document commands in Redlite (RedisJSON-compatible)
---

JSON commands for storing and manipulating JSON documents. Compatible with RedisJSON.

## Commands

| Command | Syntax | Description |
|---------|--------|-------------|
| JSON.SET | `JSON.SET key path value [NX\|XX]` | Set JSON value at path |
| JSON.GET | `JSON.GET key [path ...]` | Get JSON value(s) at path(s) |
| JSON.DEL | `JSON.DEL key [path]` | Delete value at path (or entire doc) |
| JSON.MGET | `JSON.MGET key [key ...] path` | Get same path from multiple keys |
| JSON.MSET | `JSON.MSET key path value [key path value ...]` | Set multiple key/path/values |
| JSON.TYPE | `JSON.TYPE key [path]` | Get JSON type at path |
| JSON.MERGE | `JSON.MERGE key path value` | RFC 7386 merge patch |
| JSON.CLEAR | `JSON.CLEAR key [path]` | Clear arrays/objects to empty |
| JSON.TOGGLE | `JSON.TOGGLE key path` | Toggle boolean value |
| JSON.NUMINCRBY | `JSON.NUMINCRBY key path number` | Increment number |
| JSON.STRAPPEND | `JSON.STRAPPEND key path string` | Append to string |
| JSON.STRLEN | `JSON.STRLEN key [path]` | Get string length |
| JSON.ARRAPPEND | `JSON.ARRAPPEND key path value [value ...]` | Append to array |
| JSON.ARRINDEX | `JSON.ARRINDEX key path value [start [stop]]` | Find index of value |
| JSON.ARRINSERT | `JSON.ARRINSERT key path index value [value ...]` | Insert at index |
| JSON.ARRLEN | `JSON.ARRLEN key [path]` | Get array length |
| JSON.ARRPOP | `JSON.ARRPOP key [path [index]]` | Pop from array |
| JSON.ARRTRIM | `JSON.ARRTRIM key path start stop` | Trim array to range |
| JSON.OBJKEYS | `JSON.OBJKEYS key [path]` | Get object keys |
| JSON.OBJLEN | `JSON.OBJLEN key [path]` | Get object key count |

## Path Syntax

Redlite supports JSONPath syntax:

| Path | Meaning |
|------|---------|
| `$` | Root document |
| `$.foo` | Field "foo" at root |
| `$.foo.bar` | Nested field |
| `$.items[0]` | Array index |
| `$.items[*]` | All array elements |

## Examples

### Basic SET/GET

```bash
127.0.0.1:6379> JSON.SET user:1 $ '{"name": "Alice", "age": 30}'
OK
127.0.0.1:6379> JSON.GET user:1
{"name":"Alice","age":30}
127.0.0.1:6379> JSON.GET user:1 $.name
["Alice"]
```

### Nested Documents

```bash
127.0.0.1:6379> JSON.SET config $ '{"server": {"host": "localhost", "port": 8080}}'
OK
127.0.0.1:6379> JSON.GET config $.server.host
["localhost"]
127.0.0.1:6379> JSON.SET config $.server.port 9090
OK
```

### Conditional SET

```bash
# Only set if key doesn't exist (NX)
127.0.0.1:6379> JSON.SET user:2 $ '{"name": "Bob"}' NX
OK
127.0.0.1:6379> JSON.SET user:2 $ '{"name": "Charlie"}' NX
(nil)

# Only set if key exists (XX)
127.0.0.1:6379> JSON.SET user:2 $.age 25 XX
OK
```

### Numeric Operations

```bash
127.0.0.1:6379> JSON.SET counter $ '{"value": 10}'
OK
127.0.0.1:6379> JSON.NUMINCRBY counter $.value 5
[15]
127.0.0.1:6379> JSON.NUMINCRBY counter $.value -3
[12]
```

### Array Operations

```bash
127.0.0.1:6379> JSON.SET list $ '{"items": ["a", "b"]}'
OK
127.0.0.1:6379> JSON.ARRAPPEND list $.items '"c"' '"d"'
[4]
127.0.0.1:6379> JSON.ARRLEN list $.items
[4]
127.0.0.1:6379> JSON.ARRPOP list $.items
"d"
127.0.0.1:6379> JSON.ARRINDEX list $.items '"b"'
[1]
```

### String Operations

```bash
127.0.0.1:6379> JSON.SET msg $ '{"text": "Hello"}'
OK
127.0.0.1:6379> JSON.STRAPPEND msg $.text '" World"'
[11]
127.0.0.1:6379> JSON.GET msg $.text
["Hello World"]
127.0.0.1:6379> JSON.STRLEN msg $.text
[11]
```

### Object Inspection

```bash
127.0.0.1:6379> JSON.SET obj $ '{"a": 1, "b": 2, "c": 3}'
OK
127.0.0.1:6379> JSON.OBJKEYS obj
["a","b","c"]
127.0.0.1:6379> JSON.OBJLEN obj
3
```

### Merge (RFC 7386)

```bash
127.0.0.1:6379> JSON.SET user $ '{"name": "Alice", "age": 30}'
OK
127.0.0.1:6379> JSON.MERGE user $ '{"age": 31, "city": "NYC"}'
OK
127.0.0.1:6379> JSON.GET user
{"name":"Alice","age":31,"city":"NYC"}
```

### Type Checking

```bash
127.0.0.1:6379> JSON.SET data $ '{"str": "hello", "num": 42, "arr": [1,2,3], "obj": {}}'
OK
127.0.0.1:6379> JSON.TYPE data $.str
string
127.0.0.1:6379> JSON.TYPE data $.num
integer
127.0.0.1:6379> JSON.TYPE data $.arr
array
127.0.0.1:6379> JSON.TYPE data $.obj
object
```

### Multi-Key Operations

```bash
127.0.0.1:6379> JSON.SET user:1 $ '{"name": "Alice"}'
OK
127.0.0.1:6379> JSON.SET user:2 $ '{"name": "Bob"}'
OK
127.0.0.1:6379> JSON.MGET user:1 user:2 $.name
["Alice","Bob"]
```

## FTS Integration

JSON documents can be indexed with FT.CREATE for full-text search:

```bash
# Create index on JSON documents
127.0.0.1:6379> FT.CREATE idx ON JSON PREFIX 1 product: SCHEMA $.name TEXT $.price NUMERIC

# Add documents
127.0.0.1:6379> JSON.SET product:1 $ '{"name": "Gaming Laptop", "price": 999}'
OK
127.0.0.1:6379> JSON.SET product:2 $ '{"name": "Office Laptop", "price": 599}'
OK

# Search
127.0.0.1:6379> FT.SEARCH idx "Laptop"
1) (integer) 2
2) "product:1"
3) 1) "$.name"
   2) "Gaming Laptop"
4) "product:2"
5) 1) "$.name"
   2) "Office Laptop"
```

Documents are automatically re-indexed when modified via JSON.SET, JSON.MERGE, and other mutation commands.

## Embedded API

```rust
use redlite::Db;

let db = Db::open_memory()?;

// Set JSON document
db.json_set("user:1", "$", r#"{"name": "Alice", "age": 30}"#, false, false)?;

// Get value at path
let name = db.json_get("user:1", &["$.name"])?;

// Increment number
db.json_numincrby("user:1", "$.age", 1.0)?;

// Array operations
db.json_set("list", "$", r#"{"items": []}"#, false, false)?;
db.json_arrappend("list", "$.items", &[r#""item1""#, r#""item2""#])?;
```

## See Also

- [RediSearch](/commands/redisearch/) - Full-text search on JSON documents
- [Strings](/commands/strings/) - Simple key-value storage
- [Hashes](/commands/hashes/) - Field-value pairs (alternative to JSON for flat data)
