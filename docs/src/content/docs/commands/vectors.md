---
title: Vector Search
description: Redis 8-compatible vector commands for similarity search
---

Redlite supports Redis 8-compatible vector sets for K-NN similarity search. Requires `--features vectors` when compiling.

## Installation

```bash
cargo add redlite --features vectors
# or
cargo install redlite --features vectors
```

## Commands

### VADD

Add a vector element to a set.

```bash
VADD key VALUES n v1 v2 ... element [NOQUANT|Q8|BF16] [SETATTR json]
```

**Parameters:**
- `key` - Vector set key
- `VALUES n v1 v2 ...` - Vector dimensions (n floats)
- `element` - Element identifier (string)
- `NOQUANT|Q8|BF16` - Optional quantization (default: NOQUANT)
- `SETATTR json` - Optional JSON attributes

**Examples:**

```bash
# Add a 3-dimensional vector
127.0.0.1:6379> VADD embeddings VALUES 3 0.1 0.2 0.3 item1
(integer) 1

# Add with attributes
127.0.0.1:6379> VADD embeddings VALUES 3 0.4 0.5 0.6 item2 SETATTR '{"category":"books"}'
(integer) 1

# Add with quantization (8-bit)
127.0.0.1:6379> VADD embeddings VALUES 3 0.7 0.8 0.9 item3 Q8
(integer) 1
```

### VSIM

Find similar vectors using K-NN search.

```bash
VSIM key (ELE element | VALUES n v1 v2 ...) [COUNT n] [WITHSCORES] [WITHATTRIBS] [FILTER expr]
```

**Parameters:**
- `key` - Vector set key
- `ELE element` - Reference element in the set
- `VALUES n v1 v2 ...` - Query vector
- `COUNT n` - Maximum results to return (default: 10)
- `WITHSCORES` - Include distance scores
- `WITHATTRIBS` - Include element attributes
- `FILTER expr` - Filter by attributes

**Examples:**

```bash
# Find 5 nearest neighbors
127.0.0.1:6379> VSIM embeddings VALUES 3 0.15 0.25 0.35 COUNT 5
1) "item1"
2) "item2"

# With distance scores
127.0.0.1:6379> VSIM embeddings VALUES 3 0.15 0.25 0.35 COUNT 5 WITHSCORES
1) "item1"
2) "0.0087"
3) "item2"
4) "0.1875"

# Using existing element as query
127.0.0.1:6379> VSIM embeddings ELE item1 COUNT 3
1) "item1"
2) "item2"
3) "item3"

# With attribute filter
127.0.0.1:6379> VSIM embeddings VALUES 3 0.1 0.2 0.3 FILTER '@category == "books"'
1) "item2"
```

### VREM

Remove an element from a vector set.

```bash
VREM key element
```

**Example:**

```bash
127.0.0.1:6379> VREM embeddings item1
(integer) 1
```

### VCARD

Get the number of elements in a vector set.

```bash
VCARD key
```

**Example:**

```bash
127.0.0.1:6379> VCARD embeddings
(integer) 3
```

### VDIM

Get the dimensions of vectors in a set.

```bash
VDIM key
```

**Example:**

```bash
127.0.0.1:6379> VDIM embeddings
(integer) 3
```

### VEXISTS

Check if an element exists in a vector set.

```bash
VEXISTS key element
```

**Example:**

```bash
127.0.0.1:6379> VEXISTS embeddings item1
(integer) 1
127.0.0.1:6379> VEXISTS embeddings nonexistent
(integer) 0
```

### VGET

Get the embedding vector for an element.

```bash
VGET key element
```

**Example:**

```bash
127.0.0.1:6379> VGET embeddings item1
1) "0.1"
2) "0.2"
3) "0.3"
```

### VGETALL

Get all elements and their embeddings.

```bash
VGETALL key
```

**Example:**

```bash
127.0.0.1:6379> VGETALL embeddings
1) 1) "item1"
   2) 1) "0.1"
      2) "0.2"
      3) "0.3"
2) 1) "item2"
   2) 1) "0.4"
      2) "0.5"
      3) "0.6"
```

### VGETATTRIBUTES

Get JSON attributes for elements.

```bash
VGETATTRIBUTES key element [element ...]
```

**Example:**

```bash
127.0.0.1:6379> VGETATTRIBUTES embeddings item2
1) "item2"
2) "{\"category\":\"books\"}"
```

### VSETATTRIBUTES

Set JSON attributes for an element.

```bash
VSETATTRIBUTES key element json
```

**Example:**

```bash
127.0.0.1:6379> VSETATTRIBUTES embeddings item1 '{"tags":["new","featured"]}'
OK
```

### VDELATTRIBUTES

Delete attributes from an element.

```bash
VDELATTRIBUTES key element
```

**Example:**

```bash
127.0.0.1:6379> VDELATTRIBUTES embeddings item1
OK
```

### VSIMBATCH

Batch similarity search across multiple vector sets.

```bash
VSIMBATCH n key1 key2 ... VALUES m v1 v2 ... [COUNT n] [WITHSCORES]
```

**Example:**

```bash
127.0.0.1:6379> VSIMBATCH 2 embeddings1 embeddings2 VALUES 3 0.1 0.2 0.3 COUNT 5
1) 1) "embeddings1"
   2) 1) "item1"
      2) "item2"
2) 1) "embeddings2"
   2) 1) "doc1"
```

## Use Cases

### Semantic Search

```bash
# Index document embeddings
VADD docs VALUES 384 0.1 0.2 ... doc:1 SETATTR '{"title":"Introduction to AI"}'
VADD docs VALUES 384 0.15 0.25 ... doc:2 SETATTR '{"title":"Machine Learning Basics"}'

# Search by query embedding
VSIM docs VALUES 384 0.12 0.22 ... COUNT 10 WITHATTRIBS
```

### Image Similarity

```bash
# Index image feature vectors
VADD images VALUES 512 ... img:1 SETATTR '{"path":"/uploads/cat.jpg"}'
VADD images VALUES 512 ... img:2 SETATTR '{"path":"/uploads/dog.jpg"}'

# Find similar images
VSIM images ELE img:1 COUNT 5 WITHSCORES WITHATTRIBS
```

### Recommendation Engine

```bash
# Index user embeddings
VADD users VALUES 128 ... user:1
VADD users VALUES 128 ... user:2

# Find similar users for recommendations
VSIM users ELE user:1 COUNT 10
```

## Quantization

Redlite supports three quantization modes to trade off precision for storage:

| Mode | Description | Storage | Precision |
|------|-------------|---------|-----------|
| `NOQUANT` | Full precision (f32) | 4 bytes/dim | Highest |
| `Q8` | 8-bit quantization | 1 byte/dim | Good |
| `BF16` | BFloat16 | 2 bytes/dim | High |

Use quantization for large-scale deployments to reduce storage while maintaining search quality.

## Implementation Notes

- **Backend**: sqlite-vec extension for SIMD-accelerated distance calculations
- **Storage**: Vectors stored as BLOBs in SQLite
- **Distance metric**: L2 (Euclidean) distance
- **Auto-dimension**: Vector dimensions are auto-detected from the first element
