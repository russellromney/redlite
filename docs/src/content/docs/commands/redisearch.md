---
title: RediSearch
description: Full-text search with RediSearch-compatible commands
---

Redlite implements RediSearch-compatible full-text search using SQLite's FTS5 (Full-Text Search) extension.

## Commands

| Command | Syntax | Description |
|---------|--------|-------------|
| FT.CREATE | `FT.CREATE index [ON HASH\|JSON] SCHEMA field [TEXT\|TAG\|NUMERIC] ...` | Create search index |
| FT.SEARCH | `FT.SEARCH index query [LIMIT offset count]` | Search index |
| FT.INFO | `FT.INFO index` | Get index information |
| FT.ALTER | `FT.ALTER index SCHEMA ADD field [TEXT\|TAG\|NUMERIC]` | Add field to index |
| FT.DROPINDEX | `FT.DROPINDEX index [DD]` | Delete index |
| FT.EXPLAIN | `FT.EXPLAIN index query` | Explain query execution |
| FT.PROFILE | `FT.PROFILE index SEARCH query` | Profile query performance |
| FT.AGGREGATE | `FT.AGGREGATE index query ...` | Aggregate search results |

## Creating Indexes

### Basic Index

```bash
# Create index on hash keys matching pattern "doc:*"
127.0.0.1:6379> FT.CREATE idx:docs ON HASH PREFIX 1 doc: SCHEMA title TEXT body TEXT
OK
```

### Index with Multiple Field Types

```bash
127.0.0.1:6379> FT.CREATE idx:products ON HASH PREFIX 1 product: SCHEMA name TEXT WEIGHT 5.0 description TEXT category TAG price NUMERIC SORTABLE
OK
```

## Indexing Documents

```bash
# Add documents as hashes
127.0.0.1:6379> HSET doc:1 title "Introduction to Databases" body "SQLite is a lightweight database..."
(integer) 2

127.0.0.1:6379> HSET doc:2 title "Redis Guide" body "Redis is an in-memory data store..."
(integer) 2

127.0.0.1:6379> HSET doc:3 title "Database Comparison" body "Comparing SQLite, Redis, and PostgreSQL..."
(integer) 2
```

## Searching

### Basic Search

```bash
# Search for "database"
127.0.0.1:6379> FT.SEARCH idx:docs "database"
1) (integer) 2
2) "doc:1"
3) 1) "title"
   2) "Introduction to Databases"
   3) "body"
   4) "SQLite is a lightweight database..."
4) "doc:3"
5) 1) "title"
   2) "Database Comparison"
   3) "body"
   4) "Comparing SQLite, Redis, and PostgreSQL..."
```

### Field-Specific Search

```bash
# Search in title field only
127.0.0.1:6379> FT.SEARCH idx:docs "@title:redis"
1) (integer) 1
2) "doc:2"
3) 1) "title"
   2) "Redis Guide"
   3) "body"
   4) "Redis is an in-memory data store..."
```

### Boolean Queries

```bash
# AND query
127.0.0.1:6379> FT.SEARCH idx:docs "database redis"
1) (integer) 1
2) "doc:3"

# OR query
127.0.0.1:6379> FT.SEARCH idx:docs "sqlite | redis"
1) (integer) 3

# NOT query
127.0.0.1:6379> FT.SEARCH idx:docs "database -redis"
1) (integer) 1
2) "doc:1"
```

### Phrase Search

```bash
# Exact phrase
127.0.0.1:6379> FT.SEARCH idx:docs "\"in-memory data store\""
1) (integer) 1
2) "doc:2"
```

### Prefix Search

```bash
# Prefix matching
127.0.0.1:6379> FT.SEARCH idx:docs "data*"
1) (integer) 2
```

### Pagination

```bash
# Get results 10-20
127.0.0.1:6379> FT.SEARCH idx:docs "database" LIMIT 10 10
```

## TAG Fields

```bash
# Create index with tag field
127.0.0.1:6379> FT.CREATE idx:articles ON HASH PREFIX 1 article: SCHEMA title TEXT tags TAG
OK

# Add documents with tags
127.0.0.1:6379> HSET article:1 title "Python Tutorial" tags "programming,python,tutorial"
(integer) 2

# Search by tag
127.0.0.1:6379> FT.SEARCH idx:articles "@tags:{python}"
1) (integer) 1
2) "article:1"

# Multiple tags (OR)
127.0.0.1:6379> FT.SEARCH idx:articles "@tags:{python | javascript}"

# Multiple tags (AND)
127.0.0.1:6379> FT.SEARCH idx:articles "@tags:{python} @tags:{tutorial}"
```

## NUMERIC Fields

```bash
# Create index with numeric field
127.0.0.1:6379> FT.CREATE idx:products ON HASH PREFIX 1 product: SCHEMA name TEXT price NUMERIC
OK

# Add products
127.0.0.1:6379> HSET product:1 name "Laptop" price 999.99
(integer) 2
127.0.0.1:6379> HSET product:2 name "Mouse" price 29.99
(integer) 2

# Numeric range query
127.0.0.1:6379> FT.SEARCH idx:products "@price:[0 100]"
1) (integer) 1
2) "product:2"

# Greater than
127.0.0.1:6379> FT.SEARCH idx:products "@price:[500 +inf]"
1) (integer) 1
2) "product:1"
```

## Index Management

### Get Index Info

```bash
127.0.0.1:6379> FT.INFO idx:docs
 1) "index_name"
 2) "idx:docs"
 3) "index_definition"
 4) 1) "key_type"
    2) "HASH"
    3) "prefixes"
    4) 1) "doc:"
 5) "attributes"
 6) 1) 1) "identifier"
       2) "title"
       3) "type"
       4) "TEXT"
    2) 1) "identifier"
       2) "body"
       3) "type"
       4) "TEXT"
 7) "num_docs"
 8) "3"
```

### Alter Index

```bash
# Add new field to existing index
127.0.0.1:6379> FT.ALTER idx:docs SCHEMA ADD author TEXT
OK
```

### Drop Index

```bash
# Drop index only (keep documents)
127.0.0.1:6379> FT.DROPINDEX idx:docs
OK

# Drop index and delete documents
127.0.0.1:6379> FT.DROPINDEX idx:docs DD
OK
```

## Aggregation

```bash
# Count documents by category
127.0.0.1:6379> FT.AGGREGATE idx:products "*" GROUPBY 1 @category REDUCE COUNT 0 AS count
```

## Query Explanation

```bash
# Explain query execution plan
127.0.0.1:6379> FT.EXPLAIN idx:docs "database redis"
1) "AND("
2) "  TERM(database)"
3) "  TERM(redis)"
4) ")"
```

## Performance Profiling

```bash
# Profile search query
127.0.0.1:6379> FT.PROFILE idx:docs SEARCH QUERY "database"
1) 1) "Parsing time"
   2) "0.05ms"
2) 1) "Search time"
   2) "0.15ms"
```

## Library Mode (Rust)

```rust
use redlite::Db;

let db = Db::open("mydata.db")?;

// Create index
db.ft_create("idx:docs", "HASH", &["doc:"], &[
    ("title", "TEXT"),
    ("body", "TEXT"),
])?;

// Add documents
db.hset("doc:1", &[("title", "Database Guide"), ("body", "...")])?;

// Search
let results = db.ft_search("idx:docs", "database", None, None)?;
for (key, fields) in results {
    println!("{}: {:?}", key, fields);
}

// Drop index
db.ft_dropindex("idx:docs", false)?;
```

## Use Cases

### Documentation Search

```bash
FT.CREATE idx:docs ON HASH PREFIX 1 doc: SCHEMA title TEXT WEIGHT 5.0 content TEXT tags TAG
FT.SEARCH idx:docs "@tags:{api} @title:authentication"
```

### E-commerce Product Search

```bash
FT.CREATE idx:products ON HASH PREFIX 1 product: SCHEMA name TEXT description TEXT category TAG price NUMERIC
FT.SEARCH idx:products "@category:{electronics} @price:[100 500]"
```

### Blog/CMS Search

```bash
FT.CREATE idx:posts ON HASH PREFIX 1 post: SCHEMA title TEXT author TEXT content TEXT published NUMERIC
FT.SEARCH idx:posts "@author:john @published:[1704067200 +inf]"
```

## Implementation

- **Backend**: SQLite FTS5 (Full-Text Search) extension
- **Tokenization**: Unicode61 tokenizer with case-folding
- **Ranking**: BM25 ranking algorithm
- **Storage**: Inverted index in SQLite tables
- **Stemming**: Porter stemmer support
- **Languages**: Unicode support for all languages
