---
title: PHP SDK
description: Redlite SDK for PHP
---

PHP SDK with FFI bindings (PHP 7.4+).

## Installation

```bash
composer require redlite/redlite
```

Requires PHP 7.4+ with FFI extension enabled.

## Quick Start

```php
<?php
require_once 'vendor/autoload.php';

use Redlite\Redlite;

// Open in-memory database
$db = new Redlite(':memory:');

// Or file-based
$db = new Redlite('/path/to/db.db');

// String operations
$db->set('key', 'value');
$val = $db->get('key');
echo $val;  // "value"

// Hash operations
$db->hset('user:1', 'name', 'Alice');
$db->hset('user:1', 'age', '30');
$user = $db->hgetall('user:1');

// List operations
$db->lpush('queue', 'job1', 'job2');
$job = $db->rpop('queue');

// Set operations
$db->sadd('tags', 'redis', 'sqlite');
$members = $db->smembers('tags');

// Sorted sets
$db->zadd('scores', 100, 'player1');
$db->zadd('scores', 85, 'player2');
$top = $db->zrevrange('scores', 0, 9);

$db->close();
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
cd sdks/redlite-php
make install && make test
```

## Links

- [Full README](https://github.com/russellromney/redlite/tree/main/sdks/redlite-php)
- [Source Code](https://github.com/russellromney/redlite/tree/main/sdks/redlite-php)
