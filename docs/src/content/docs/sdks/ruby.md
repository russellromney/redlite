---
title: Ruby SDK
description: Redlite SDK for Ruby
---

Ruby SDK with FFI bindings for Ruby applications.

## Installation

```ruby
gem install redlite
```

Or add to your Gemfile:

```ruby
gem 'redlite'
```

## Quick Start

```ruby
require 'redlite'

# Open in-memory database
db = Redlite.new(':memory:')

# Or file-based
db = Redlite.new('/path/to/db.db')

# String operations
db.set('key', 'value')
db.get('key')  # => "value"

# Hash operations
db.hset('user:1', 'name', 'Alice')
db.hset('user:1', 'age', '30')
db.hgetall('user:1')  # => {"name" => "Alice", "age" => "30"}

# List operations
db.lpush('queue', 'job1', 'job2')
db.rpop('queue')

# Set operations
db.sadd('tags', 'redis', 'sqlite')
db.smembers('tags')

# Sorted sets
db.zadd('scores', 100, 'player1')
db.zrange('scores', 0, -1)

db.close
```

## API Overview

**Strings**: `set`, `get`, `incr`, `decr`, `append`, `mget`, `mset`

**Keys**: `del`, `exists`, `type`, `ttl`, `expire`, `keys`

**Hashes**: `hset`, `hget`, `hdel`, `hgetall`, `hmget`

**Lists**: `lpush`, `rpush`, `lpop`, `rpop`, `llen`, `lrange`

**Sets**: `sadd`, `srem`, `smembers`, `sismember`, `scard`

**Sorted Sets**: `zadd`, `zrem`, `zscore`, `zrange`, `zrevrange`

## Testing

```bash
cd sdks/redlite-ruby
bundle exec rspec
```

## Links

- [Full README](https://github.com/russellromney/redlite/tree/main/sdks/redlite-ruby)
- [Source Code](https://github.com/russellromney/redlite/tree/main/sdks/redlite-ruby)
