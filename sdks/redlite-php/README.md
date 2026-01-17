# Redlite PHP SDK

Redis API + SQLite durability. Embedded Redis-compatible database for PHP.

## Requirements

- PHP 7.4+ with FFI extension enabled
- Native Redlite library (included for supported platforms)

## Installation

```bash
composer require redlite/redlite
```

## Quick Start

```php
<?php
use Redlite\Redlite;

// Open in-memory database
$db = new Redlite(':memory:');

// Or open a persistent database
$db = new Redlite('/path/to/database.db');

// With custom cache size (MB)
$db = new Redlite('/path/to/database.db', 100);

// String operations
$db->set('key', 'value');
echo $db->get('key');  // "value"

// With TTL (seconds)
$db->set('session', 'data', 3600);

// Always close when done
$db->close();
```

## Commands

### String Commands

```php
$db->set('key', 'value');           // Set a value
$db->set('key', 'value', 60);       // Set with 60s TTL
$db->get('key');                     // Get a value (null if missing)
$db->getdel('key');                  // Get and delete
$db->append('key', 'more');          // Append to value
$db->strlen('key');                  // Get string length
$db->getrange('key', 0, 5);          // Get substring
$db->setrange('key', 0, 'new');      // Set substring

// Counters
$db->incr('counter');
$db->decr('counter');
$db->incrby('counter', 10);
$db->decrby('counter', 5);
$db->incrbyfloat('counter', 1.5);

// Multi-key operations
$db->mset(['k1' => 'v1', 'k2' => 'v2']);
$values = $db->mget('k1', 'k2', 'k3');  // ['v1', 'v2', null]
```

### Key Commands

```php
$db->delete('key1', 'key2');         // Delete keys
$db->exists('key1', 'key2');         // Count existing keys
$db->type('key');                    // Get type (string, list, set, etc.)
$db->keys('user:*');                 // Find matching keys

// TTL operations
$db->expire('key', 60);              // Set TTL in seconds
$db->pexpire('key', 60000);          // Set TTL in milliseconds
$db->ttl('key');                     // Get TTL in seconds
$db->pttl('key');                    // Get TTL in milliseconds
$db->persist('key');                 // Remove TTL

// Rename
$db->rename('old', 'new');
$db->renamenx('old', 'new');         // Only if 'new' doesn't exist

// Database operations
$db->dbsize();                       // Number of keys
$db->flushdb();                      // Delete all keys
```

### Hash Commands

```php
$db->hset('user:1', ['name' => 'Alice', 'age' => '30']);
$db->hget('user:1', 'name');         // 'Alice'
$db->hdel('user:1', 'age');
$db->hexists('user:1', 'name');      // true
$db->hlen('user:1');                 // Number of fields
$db->hkeys('user:1');                // ['name']
$db->hvals('user:1');                // ['Alice']
$db->hgetall('user:1');              // ['name' => 'Alice']
$db->hmget('user:1', 'name', 'age'); // ['Alice', null]
$db->hincrby('user:1', 'score', 10);
```

### List Commands

```php
$db->lpush('queue', 'a', 'b', 'c');  // Push to left
$db->rpush('queue', 'd', 'e');       // Push to right
$db->lpop('queue');                  // Pop from left
$db->rpop('queue');                  // Pop from right
$db->lpop('queue', 2);               // Pop 2 from left
$db->llen('queue');                  // List length
$db->lrange('queue', 0, -1);         // Get all elements
$db->lindex('queue', 0);             // Get by index
```

### Set Commands

```php
$db->sadd('tags', 'redis', 'sqlite', 'php');
$db->srem('tags', 'php');
$db->smembers('tags');               // ['redis', 'sqlite']
$db->sismember('tags', 'redis');     // true
$db->scard('tags');                  // 2
```

### Sorted Set Commands

```php
$db->zadd('scores', [
    'alice' => 100,
    'bob' => 95,
    'carol' => 98
]);
$db->zrem('scores', 'bob');
$db->zscore('scores', 'alice');      // 100.0
$db->zcard('scores');                // 2
$db->zcount('scores', 90, 100);      // Count in score range
$db->zincrby('scores', 5, 'alice');  // Increment score
$db->zrange('scores', 0, -1);        // Ascending order
$db->zrevrange('scores', 0, -1);     // Descending order

// With scores
$db->zrange('scores', 0, -1, true);  // ['carol' => 98, 'alice' => 105]
```

### Utility Commands

```php
$db->vacuum();                       // Compact the database
Redlite::version();                  // Get library version
```

## Custom Library Path

If the native library is in a non-standard location:

```php
Redlite::setLibraryPath('/path/to/libredlite.dylib');
$db = new Redlite(':memory:');
```

## Error Handling

```php
use Redlite\RedliteException;

try {
    $db = new Redlite('/invalid/path.db');
} catch (RedliteException $e) {
    echo "Failed to open database: " . $e->getMessage();
}
```

## Testing

```bash
composer install
./vendor/bin/phpunit
```

## Platform Support

Pre-built binaries are included for:
- macOS (Apple Silicon / arm64)
- macOS (Intel / x86_64)
- Linux (x86_64)
- Windows (x86_64)

## License

MIT
