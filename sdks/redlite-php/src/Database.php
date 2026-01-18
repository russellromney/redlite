<?php

declare(strict_types=1);

namespace Redlite;

use FFI;
use RuntimeException;

/**
 * Low-level FFI wrapper for the Redlite native library.
 *
 * This class handles memory management and FFI calls to the native library.
 * Use the Redlite class for the high-level API.
 */
class Database
{
    private static ?FFI $ffi = null;
    private static ?string $libraryPath = null;

    /** @var FFI\CData|null */
    private $handle;

    private bool $closed = false;

    private function __construct($handle)
    {
        $this->handle = $handle;
    }

    public static function setLibraryPath(string $path): void
    {
        self::$libraryPath = $path;
        self::$ffi = null; // Reset FFI to reload with new path
    }

    private static function getFFI(): FFI
    {
        if (self::$ffi !== null) {
            return self::$ffi;
        }

        $header = self::getHeader();
        $libraryPath = self::findLibrary();

        self::$ffi = FFI::cdef($header, $libraryPath);
        return self::$ffi;
    }

    private static function getHeader(): string
    {
        return <<<'HEADER'
typedef struct RedliteDb RedliteDb;

typedef struct RedliteBytes {
    uint8_t *data;
    size_t len;
} RedliteBytes;

typedef struct RedliteStringArray {
    char **strings;
    size_t len;
} RedliteStringArray;

typedef struct RedliteBytesArray {
    RedliteBytes *items;
    size_t len;
} RedliteBytesArray;

typedef struct RedliteKV {
    const char *key;
    const uint8_t *value;
    size_t value_len;
} RedliteKV;

typedef struct RedliteZMember {
    double score;
    const uint8_t *member;
    size_t member_len;
} RedliteZMember;

// Database management
RedliteDb *redlite_open(const char *path);
RedliteDb *redlite_open_memory(void);
RedliteDb *redlite_open_with_cache(const char *path, int64_t cache_mb);
void redlite_close(RedliteDb *db);
char *redlite_last_error(void);
char *redlite_version(void);

// Memory management
void redlite_free_string(char *s);
void redlite_free_bytes(RedliteBytes bytes);
void redlite_free_string_array(RedliteStringArray arr);
void redlite_free_bytes_array(RedliteBytesArray arr);

// String commands
RedliteBytes redlite_get(RedliteDb *db, const char *key);
int redlite_set(RedliteDb *db, const char *key, const uint8_t *value, size_t value_len, int64_t ttl_seconds);
int redlite_setex(RedliteDb *db, const char *key, int64_t seconds, const uint8_t *value, size_t value_len);
int redlite_psetex(RedliteDb *db, const char *key, int64_t milliseconds, const uint8_t *value, size_t value_len);
RedliteBytes redlite_getdel(RedliteDb *db, const char *key);
int64_t redlite_append(RedliteDb *db, const char *key, const uint8_t *value, size_t value_len);
int64_t redlite_strlen(RedliteDb *db, const char *key);
RedliteBytes redlite_getrange(RedliteDb *db, const char *key, int64_t start, int64_t end);
int64_t redlite_setrange(RedliteDb *db, const char *key, int64_t offset, const uint8_t *value, size_t value_len);
int64_t redlite_incr(RedliteDb *db, const char *key);
int64_t redlite_decr(RedliteDb *db, const char *key);
int64_t redlite_incrby(RedliteDb *db, const char *key, int64_t increment);
int64_t redlite_decrby(RedliteDb *db, const char *key, int64_t decrement);
char *redlite_incrbyfloat(RedliteDb *db, const char *key, double increment);

// Multi-key commands
RedliteBytesArray redlite_mget(RedliteDb *db, const char **keys, size_t keys_len);
int redlite_mset(RedliteDb *db, const RedliteKV *pairs, size_t pairs_len);

// Key commands
int64_t redlite_del(RedliteDb *db, const char **keys, size_t keys_len);
int64_t redlite_exists(RedliteDb *db, const char **keys, size_t keys_len);
char *redlite_type(RedliteDb *db, const char *key);
int64_t redlite_ttl(RedliteDb *db, const char *key);
int64_t redlite_pttl(RedliteDb *db, const char *key);
int redlite_expire(RedliteDb *db, const char *key, int64_t seconds);
int redlite_pexpire(RedliteDb *db, const char *key, int64_t milliseconds);
int redlite_expireat(RedliteDb *db, const char *key, int64_t unix_seconds);
int redlite_pexpireat(RedliteDb *db, const char *key, int64_t unix_ms);
int redlite_persist(RedliteDb *db, const char *key);
int redlite_rename(RedliteDb *db, const char *key, const char *newkey);
int redlite_renamenx(RedliteDb *db, const char *key, const char *newkey);
RedliteStringArray redlite_keys(RedliteDb *db, const char *pattern);
int64_t redlite_dbsize(RedliteDb *db);
int redlite_flushdb(RedliteDb *db);
int redlite_select(RedliteDb *db, int db_num);

// Hash commands
int64_t redlite_hset(RedliteDb *db, const char *key, const char **fields, const RedliteBytes *values, size_t count);
RedliteBytes redlite_hget(RedliteDb *db, const char *key, const char *field);
int64_t redlite_hdel(RedliteDb *db, const char *key, const char **fields, size_t fields_len);
int redlite_hexists(RedliteDb *db, const char *key, const char *field);
int64_t redlite_hlen(RedliteDb *db, const char *key);
RedliteStringArray redlite_hkeys(RedliteDb *db, const char *key);
RedliteBytesArray redlite_hvals(RedliteDb *db, const char *key);
int64_t redlite_hincrby(RedliteDb *db, const char *key, const char *field, int64_t increment);
RedliteBytesArray redlite_hgetall(RedliteDb *db, const char *key);
RedliteBytesArray redlite_hmget(RedliteDb *db, const char *key, const char **fields, size_t fields_len);

// List commands
int64_t redlite_lpush(RedliteDb *db, const char *key, const RedliteBytes *values, size_t values_len);
int64_t redlite_rpush(RedliteDb *db, const char *key, const RedliteBytes *values, size_t values_len);
int64_t redlite_lpushx(RedliteDb *db, const char *key, const RedliteBytes *values, size_t values_len);
int64_t redlite_rpushx(RedliteDb *db, const char *key, const RedliteBytes *values, size_t values_len);
RedliteBytesArray redlite_lpop(RedliteDb *db, const char *key, size_t count);
RedliteBytesArray redlite_rpop(RedliteDb *db, const char *key, size_t count);
int64_t redlite_llen(RedliteDb *db, const char *key);
RedliteBytesArray redlite_lrange(RedliteDb *db, const char *key, int64_t start, int64_t stop);
RedliteBytes redlite_lindex(RedliteDb *db, const char *key, int64_t index);
RedliteBytes redlite_lmove(RedliteDb *db, const char *source, const char *destination, int wherefrom, int whereto);
RedliteBytesArray redlite_lpos(RedliteDb *db, const char *key, const uint8_t *element, size_t element_len, int64_t rank, size_t count, size_t maxlen);

// Set commands
int64_t redlite_sadd(RedliteDb *db, const char *key, const RedliteBytes *members, size_t members_len);
int64_t redlite_srem(RedliteDb *db, const char *key, const RedliteBytes *members, size_t members_len);
RedliteBytesArray redlite_smembers(RedliteDb *db, const char *key);
int redlite_sismember(RedliteDb *db, const char *key, const uint8_t *member, size_t member_len);
int64_t redlite_scard(RedliteDb *db, const char *key);

// Sorted set commands
int64_t redlite_zadd(RedliteDb *db, const char *key, const RedliteZMember *members, size_t members_len);
int64_t redlite_zrem(RedliteDb *db, const char *key, const RedliteBytes *members, size_t members_len);
double redlite_zscore(RedliteDb *db, const char *key, const uint8_t *member, size_t member_len);
int64_t redlite_zcard(RedliteDb *db, const char *key);
int64_t redlite_zcount(RedliteDb *db, const char *key, double min, double max);
double redlite_zincrby(RedliteDb *db, const char *key, double increment, const uint8_t *member, size_t member_len);
RedliteBytesArray redlite_zrange(RedliteDb *db, const char *key, int64_t start, int64_t stop, int with_scores);
RedliteBytesArray redlite_zrevrange(RedliteDb *db, const char *key, int64_t start, int64_t stop, int with_scores);
int64_t redlite_zinterstore(RedliteDb *db, const char *destination, const char **keys, size_t keys_len, const double *weights, size_t weights_len, const char *aggregate);
int64_t redlite_zunionstore(RedliteDb *db, const char *destination, const char **keys, size_t keys_len, const double *weights, size_t weights_len, const char *aggregate);

// Utility
int64_t redlite_vacuum(RedliteDb *db);

// JSON commands
int redlite_json_set(RedliteDb *db, const char *key, const char *path, const char *value, int nx, int xx);
char *redlite_json_get(RedliteDb *db, const char *key, const char **paths, size_t paths_len);
int64_t redlite_json_del(RedliteDb *db, const char *key, const char *path);
char *redlite_json_type(RedliteDb *db, const char *key, const char *path);
char *redlite_json_numincrby(RedliteDb *db, const char *key, const char *path, double increment);
int64_t redlite_json_strappend(RedliteDb *db, const char *key, const char *path, const char *value);
int64_t redlite_json_strlen(RedliteDb *db, const char *key, const char *path);
int64_t redlite_json_arrappend(RedliteDb *db, const char *key, const char *path, const char **values, size_t values_len);
int64_t redlite_json_arrlen(RedliteDb *db, const char *key, const char *path);
char *redlite_json_arrpop(RedliteDb *db, const char *key, const char *path, int64_t index);
int64_t redlite_json_clear(RedliteDb *db, const char *key, const char *path);

// History enable/disable
int redlite_history_enable_global(RedliteDb *db, const char *retention_type, int64_t retention_value);
int redlite_history_enable_database(RedliteDb *db, int db_num, const char *retention_type, int64_t retention_value);
int redlite_history_enable_key(RedliteDb *db, const char *key, const char *retention_type, int64_t retention_value);
int redlite_history_disable_global(RedliteDb *db);
int redlite_history_disable_database(RedliteDb *db, int db_num);
int redlite_history_disable_key(RedliteDb *db, const char *key);
int redlite_is_history_enabled(RedliteDb *db, const char *key);

// FTS enable/disable
int redlite_fts_enable_global(RedliteDb *db);
int redlite_fts_enable_database(RedliteDb *db, int db_num);
int redlite_fts_enable_pattern(RedliteDb *db, const char *pattern);
int redlite_fts_enable_key(RedliteDb *db, const char *key);
int redlite_fts_disable_global(RedliteDb *db);
int redlite_fts_disable_database(RedliteDb *db, int db_num);
int redlite_fts_disable_pattern(RedliteDb *db, const char *pattern);
int redlite_fts_disable_key(RedliteDb *db, const char *key);
int redlite_is_fts_enabled(RedliteDb *db, const char *key);

// KeyInfo
typedef struct RedliteKeyInfo {
    char *key_type;
    int64_t ttl;
    int64_t created_at;
    int64_t updated_at;
    int valid;
} RedliteKeyInfo;
RedliteKeyInfo redlite_keyinfo(RedliteDb *db, const char *key);
void redlite_free_keyinfo(RedliteKeyInfo info);
HEADER;
    }

    private static function findLibrary(): string
    {
        if (self::$libraryPath !== null && file_exists(self::$libraryPath)) {
            return self::$libraryPath;
        }

        // Determine platform-specific library name
        $os = PHP_OS_FAMILY;
        $arch = php_uname('m');

        if ($os === 'Darwin') {
            $libName = 'libredlite.dylib';
            $archDir = $arch === 'arm64' ? 'darwin-aarch64' : 'darwin-x86_64';
        } elseif ($os === 'Linux') {
            $libName = 'libredlite.so';
            $archDir = $arch === 'aarch64' ? 'linux-aarch64' : 'linux-x86_64';
        } elseif ($os === 'Windows') {
            $libName = 'redlite.dll';
            $archDir = 'windows-x86_64';
        } else {
            throw new RuntimeException("Unsupported platform: {$os}");
        }

        // Search paths for the library
        $searchPaths = [
            __DIR__ . "/../native/{$archDir}/{$libName}",
            __DIR__ . "/../../native/{$archDir}/{$libName}",
            __DIR__ . "/../../../target/release/{$libName}",
            "/usr/local/lib/{$libName}",
            "/usr/lib/{$libName}",
        ];

        foreach ($searchPaths as $path) {
            if (file_exists($path)) {
                return realpath($path);
            }
        }

        throw new RuntimeException(
            "Redlite native library not found. Searched paths: " . implode(', ', $searchPaths)
        );
    }

    public static function open(string $path): self
    {
        $ffi = self::getFFI();
        $handle = $ffi->redlite_open($path);

        if ($handle === null) {
            $error = self::getLastError();
            throw new RedliteException("Failed to open database: {$error}");
        }

        return new self($handle);
    }

    public static function openMemory(): self
    {
        $ffi = self::getFFI();
        $handle = $ffi->redlite_open_memory();

        if ($handle === null) {
            $error = self::getLastError();
            throw new RedliteException("Failed to open in-memory database: {$error}");
        }

        return new self($handle);
    }

    public static function openWithCache(string $path, int $cacheMb): self
    {
        $ffi = self::getFFI();
        $handle = $ffi->redlite_open_with_cache($path, $cacheMb);

        if ($handle === null) {
            $error = self::getLastError();
            throw new RedliteException("Failed to open database: {$error}");
        }

        return new self($handle);
    }

    public static function version(): string
    {
        $ffi = self::getFFI();
        $ptr = $ffi->redlite_version();
        $version = FFI::string($ptr);
        $ffi->redlite_free_string($ptr);
        return $version;
    }

    private static function getLastError(): string
    {
        $ffi = self::getFFI();
        $ptr = $ffi->redlite_last_error();
        if ($ptr === null) {
            return 'Unknown error';
        }
        $error = FFI::string($ptr);
        $ffi->redlite_free_string($ptr);
        return $error;
    }

    public function close(): void
    {
        if ($this->closed || $this->handle === null) {
            return;
        }

        self::getFFI()->redlite_close($this->handle);
        $this->handle = null;
        $this->closed = true;
    }

    public function __destruct()
    {
        $this->close();
    }

    private function ensureOpen(): void
    {
        if ($this->closed || $this->handle === null) {
            throw new RedliteException('Database is closed');
        }
    }

    // -------------------------------------------------------------------------
    // String commands
    // -------------------------------------------------------------------------

    public function get(string $key): ?string
    {
        $this->ensureOpen();
        $ffi = self::getFFI();

        $result = $ffi->redlite_get($this->handle, $key);

        if ($result->data === null) {
            return null;
        }

        $value = FFI::string($result->data, $result->len);
        $ffi->redlite_free_bytes($result);
        return $value;
    }

    public function set(string $key, string $value, int $ttlSeconds = 0): bool
    {
        $this->ensureOpen();
        $ffi = self::getFFI();

        $result = $ffi->redlite_set($this->handle, $key, $value, strlen($value), $ttlSeconds);
        return $result === 0;
    }

    public function setex(string $key, int $seconds, string $value): bool
    {
        $this->ensureOpen();
        $ffi = self::getFFI();

        $result = $ffi->redlite_setex($this->handle, $key, $seconds, $value, strlen($value));
        return $result === 0;
    }

    public function psetex(string $key, int $milliseconds, string $value): bool
    {
        $this->ensureOpen();
        $ffi = self::getFFI();

        $result = $ffi->redlite_psetex($this->handle, $key, $milliseconds, $value, strlen($value));
        return $result === 0;
    }

    public function getdel(string $key): ?string
    {
        $this->ensureOpen();
        $ffi = self::getFFI();

        $result = $ffi->redlite_getdel($this->handle, $key);

        if ($result->data === null) {
            return null;
        }

        $value = FFI::string($result->data, $result->len);
        $ffi->redlite_free_bytes($result);
        return $value;
    }

    public function append(string $key, string $value): int
    {
        $this->ensureOpen();
        $ffi = self::getFFI();

        return $ffi->redlite_append($this->handle, $key, $value, strlen($value));
    }

    public function strlen(string $key): int
    {
        $this->ensureOpen();
        $ffi = self::getFFI();

        return $ffi->redlite_strlen($this->handle, $key);
    }

    public function getrange(string $key, int $start, int $end): ?string
    {
        $this->ensureOpen();
        $ffi = self::getFFI();

        $result = $ffi->redlite_getrange($this->handle, $key, $start, $end);

        if ($result->data === null) {
            return null;
        }

        $value = FFI::string($result->data, $result->len);
        $ffi->redlite_free_bytes($result);
        return $value;
    }

    public function setrange(string $key, int $offset, string $value): int
    {
        $this->ensureOpen();
        $ffi = self::getFFI();

        return $ffi->redlite_setrange($this->handle, $key, $offset, $value, strlen($value));
    }

    public function incr(string $key): int
    {
        $this->ensureOpen();
        $ffi = self::getFFI();

        return $ffi->redlite_incr($this->handle, $key);
    }

    public function decr(string $key): int
    {
        $this->ensureOpen();
        $ffi = self::getFFI();

        return $ffi->redlite_decr($this->handle, $key);
    }

    public function incrby(string $key, int $increment): int
    {
        $this->ensureOpen();
        $ffi = self::getFFI();

        return $ffi->redlite_incrby($this->handle, $key, $increment);
    }

    public function decrby(string $key, int $decrement): int
    {
        $this->ensureOpen();
        $ffi = self::getFFI();

        return $ffi->redlite_decrby($this->handle, $key, $decrement);
    }

    public function incrbyfloat(string $key, float $increment): ?float
    {
        $this->ensureOpen();
        $ffi = self::getFFI();

        $ptr = $ffi->redlite_incrbyfloat($this->handle, $key, $increment);

        if ($ptr === null) {
            return null;
        }

        $result = (float) FFI::string($ptr);
        $ffi->redlite_free_string($ptr);
        return $result;
    }

    // -------------------------------------------------------------------------
    // Multi-key commands
    // -------------------------------------------------------------------------

    /**
     * @param string[] $keys
     * @return array<string|null>
     */
    public function mget(array $keys): array
    {
        $this->ensureOpen();
        $ffi = self::getFFI();

        $count = count($keys);
        if ($count === 0) {
            return [];
        }

        // Create array of char* pointers
        $keysArray = $ffi->new("char*[{$count}]");
        foreach ($keys as $i => $key) {
            $keysArray[$i] = $key;
        }

        $result = $ffi->redlite_mget($this->handle, $keysArray, $count);

        $values = [];
        for ($i = 0; $i < $result->len; $i++) {
            $item = $result->items[$i];
            if ($item->data === null) {
                $values[] = null;
            } else {
                $values[] = FFI::string($item->data, $item->len);
            }
        }

        $ffi->redlite_free_bytes_array($result);
        return $values;
    }

    /**
     * @param array<string, string> $pairs
     */
    public function mset(array $pairs): bool
    {
        $this->ensureOpen();
        $ffi = self::getFFI();

        $count = count($pairs);
        if ($count === 0) {
            return true;
        }

        $kvArray = $ffi->new("RedliteKV[{$count}]");
        $i = 0;
        foreach ($pairs as $key => $value) {
            $kvArray[$i]->key = $key;
            $kvArray[$i]->value = $value;
            $kvArray[$i]->value_len = strlen($value);
            $i++;
        }

        $result = $ffi->redlite_mset($this->handle, $kvArray, $count);
        return $result === 0;
    }

    // -------------------------------------------------------------------------
    // Key commands
    // -------------------------------------------------------------------------

    /**
     * @param string[] $keys
     */
    public function del(array $keys): int
    {
        $this->ensureOpen();
        $ffi = self::getFFI();

        $count = count($keys);
        if ($count === 0) {
            return 0;
        }

        $keysArray = $ffi->new("char*[{$count}]");
        foreach ($keys as $i => $key) {
            $keysArray[$i] = $key;
        }

        return $ffi->redlite_del($this->handle, $keysArray, $count);
    }

    /**
     * @param string[] $keys
     */
    public function exists(array $keys): int
    {
        $this->ensureOpen();
        $ffi = self::getFFI();

        $count = count($keys);
        if ($count === 0) {
            return 0;
        }

        $keysArray = $ffi->new("char*[{$count}]");
        foreach ($keys as $i => $key) {
            $keysArray[$i] = $key;
        }

        return $ffi->redlite_exists($this->handle, $keysArray, $count);
    }

    public function type(string $key): ?string
    {
        $this->ensureOpen();
        $ffi = self::getFFI();

        $ptr = $ffi->redlite_type($this->handle, $key);

        if ($ptr === null) {
            return null;
        }

        $type = FFI::string($ptr);
        $ffi->redlite_free_string($ptr);
        return $type;
    }

    public function ttl(string $key): int
    {
        $this->ensureOpen();
        $ffi = self::getFFI();

        return $ffi->redlite_ttl($this->handle, $key);
    }

    public function pttl(string $key): int
    {
        $this->ensureOpen();
        $ffi = self::getFFI();

        return $ffi->redlite_pttl($this->handle, $key);
    }

    public function expire(string $key, int $seconds): bool
    {
        $this->ensureOpen();
        $ffi = self::getFFI();

        return $ffi->redlite_expire($this->handle, $key, $seconds) === 1;
    }

    public function pexpire(string $key, int $milliseconds): bool
    {
        $this->ensureOpen();
        $ffi = self::getFFI();

        return $ffi->redlite_pexpire($this->handle, $key, $milliseconds) === 1;
    }

    public function expireat(string $key, int $unixSeconds): bool
    {
        $this->ensureOpen();
        $ffi = self::getFFI();

        return $ffi->redlite_expireat($this->handle, $key, $unixSeconds) === 1;
    }

    public function pexpireat(string $key, int $unixMs): bool
    {
        $this->ensureOpen();
        $ffi = self::getFFI();

        return $ffi->redlite_pexpireat($this->handle, $key, $unixMs) === 1;
    }

    public function persist(string $key): bool
    {
        $this->ensureOpen();
        $ffi = self::getFFI();

        return $ffi->redlite_persist($this->handle, $key) === 1;
    }

    public function rename(string $key, string $newkey): bool
    {
        $this->ensureOpen();
        $ffi = self::getFFI();

        return $ffi->redlite_rename($this->handle, $key, $newkey) === 0;
    }

    public function renamenx(string $key, string $newkey): bool
    {
        $this->ensureOpen();
        $ffi = self::getFFI();

        return $ffi->redlite_renamenx($this->handle, $key, $newkey) === 1;
    }

    /**
     * @return string[]
     */
    public function keys(string $pattern): array
    {
        $this->ensureOpen();
        $ffi = self::getFFI();

        $result = $ffi->redlite_keys($this->handle, $pattern);

        $keys = [];
        for ($i = 0; $i < $result->len; $i++) {
            $keys[] = FFI::string($result->strings[$i]);
        }

        $ffi->redlite_free_string_array($result);
        return $keys;
    }

    public function dbsize(): int
    {
        $this->ensureOpen();
        $ffi = self::getFFI();

        return $ffi->redlite_dbsize($this->handle);
    }

    public function flushdb(): bool
    {
        $this->ensureOpen();
        $ffi = self::getFFI();

        return $ffi->redlite_flushdb($this->handle) === 0;
    }

    public function select(int $dbNum): bool
    {
        $this->ensureOpen();
        $ffi = self::getFFI();

        return $ffi->redlite_select($this->handle, $dbNum) === 0;
    }

    // -------------------------------------------------------------------------
    // Hash commands
    // -------------------------------------------------------------------------

    /**
     * @param array<string, string> $fieldValues
     */
    public function hset(string $key, array $fieldValues): int
    {
        $this->ensureOpen();
        $ffi = self::getFFI();

        $count = count($fieldValues);
        if ($count === 0) {
            return 0;
        }

        $fields = $ffi->new("char*[{$count}]");
        $values = $ffi->new("RedliteBytes[{$count}]");

        $i = 0;
        foreach ($fieldValues as $field => $value) {
            $fields[$i] = (string) $field;
            $values[$i]->data = $value;
            $values[$i]->len = strlen($value);
            $i++;
        }

        return $ffi->redlite_hset($this->handle, $key, $fields, $values, $count);
    }

    public function hget(string $key, string $field): ?string
    {
        $this->ensureOpen();
        $ffi = self::getFFI();

        $result = $ffi->redlite_hget($this->handle, $key, $field);

        if ($result->data === null) {
            return null;
        }

        $value = FFI::string($result->data, $result->len);
        $ffi->redlite_free_bytes($result);
        return $value;
    }

    /**
     * @param string[] $fields
     */
    public function hdel(string $key, array $fields): int
    {
        $this->ensureOpen();
        $ffi = self::getFFI();

        $count = count($fields);
        if ($count === 0) {
            return 0;
        }

        $fieldsArray = $ffi->new("char*[{$count}]");
        foreach ($fields as $i => $field) {
            $fieldsArray[$i] = $field;
        }

        return $ffi->redlite_hdel($this->handle, $key, $fieldsArray, $count);
    }

    public function hexists(string $key, string $field): bool
    {
        $this->ensureOpen();
        $ffi = self::getFFI();

        return $ffi->redlite_hexists($this->handle, $key, $field) === 1;
    }

    public function hlen(string $key): int
    {
        $this->ensureOpen();
        $ffi = self::getFFI();

        return $ffi->redlite_hlen($this->handle, $key);
    }

    /**
     * @return string[]
     */
    public function hkeys(string $key): array
    {
        $this->ensureOpen();
        $ffi = self::getFFI();

        $result = $ffi->redlite_hkeys($this->handle, $key);

        $keys = [];
        for ($i = 0; $i < $result->len; $i++) {
            $keys[] = FFI::string($result->strings[$i]);
        }

        $ffi->redlite_free_string_array($result);
        return $keys;
    }

    /**
     * @return string[]
     */
    public function hvals(string $key): array
    {
        $this->ensureOpen();
        $ffi = self::getFFI();

        $result = $ffi->redlite_hvals($this->handle, $key);

        $values = [];
        for ($i = 0; $i < $result->len; $i++) {
            $item = $result->items[$i];
            if ($item->data !== null) {
                $values[] = FFI::string($item->data, $item->len);
            }
        }

        $ffi->redlite_free_bytes_array($result);
        return $values;
    }

    public function hincrby(string $key, string $field, int $increment): int
    {
        $this->ensureOpen();
        $ffi = self::getFFI();

        return $ffi->redlite_hincrby($this->handle, $key, $field, $increment);
    }

    /**
     * @return array<string, string>
     */
    public function hgetall(string $key): array
    {
        $this->ensureOpen();
        $ffi = self::getFFI();

        $result = $ffi->redlite_hgetall($this->handle, $key);

        $hash = [];
        for ($i = 0; $i < $result->len; $i += 2) {
            $fieldItem = $result->items[$i];
            $valueItem = $result->items[$i + 1];

            if ($fieldItem->data !== null && $valueItem->data !== null) {
                $field = FFI::string($fieldItem->data, $fieldItem->len);
                $value = FFI::string($valueItem->data, $valueItem->len);
                $hash[$field] = $value;
            }
        }

        $ffi->redlite_free_bytes_array($result);
        return $hash;
    }

    /**
     * @param string[] $fields
     * @return array<string|null>
     */
    public function hmget(string $key, array $fields): array
    {
        $this->ensureOpen();
        $ffi = self::getFFI();

        $count = count($fields);
        if ($count === 0) {
            return [];
        }

        $fieldsArray = $ffi->new("char*[{$count}]");
        foreach ($fields as $i => $field) {
            $fieldsArray[$i] = $field;
        }

        $result = $ffi->redlite_hmget($this->handle, $key, $fieldsArray, $count);

        $values = [];
        for ($i = 0; $i < $result->len; $i++) {
            $item = $result->items[$i];
            if ($item->data === null) {
                $values[] = null;
            } else {
                $values[] = FFI::string($item->data, $item->len);
            }
        }

        $ffi->redlite_free_bytes_array($result);
        return $values;
    }

    // -------------------------------------------------------------------------
    // List commands
    // -------------------------------------------------------------------------

    /**
     * @param string[] $values
     */
    public function lpush(string $key, array $values): int
    {
        $this->ensureOpen();
        $ffi = self::getFFI();

        $count = count($values);
        if ($count === 0) {
            return 0;
        }

        $valuesArray = $ffi->new("RedliteBytes[{$count}]");
        foreach ($values as $i => $value) {
            $valuesArray[$i]->data = $value;
            $valuesArray[$i]->len = strlen($value);
        }

        return $ffi->redlite_lpush($this->handle, $key, $valuesArray, $count);
    }

    /**
     * @param string[] $values
     */
    public function rpush(string $key, array $values): int
    {
        $this->ensureOpen();
        $ffi = self::getFFI();

        $count = count($values);
        if ($count === 0) {
            return 0;
        }

        $valuesArray = $ffi->new("RedliteBytes[{$count}]");
        foreach ($values as $i => $value) {
            $valuesArray[$i]->data = $value;
            $valuesArray[$i]->len = strlen($value);
        }

        return $ffi->redlite_rpush($this->handle, $key, $valuesArray, $count);
    }

    /**
     * @return string[]
     */
    public function lpop(string $key, int $count = 1): array
    {
        $this->ensureOpen();
        $ffi = self::getFFI();

        $result = $ffi->redlite_lpop($this->handle, $key, $count);

        $values = [];
        for ($i = 0; $i < $result->len; $i++) {
            $item = $result->items[$i];
            if ($item->data !== null) {
                $values[] = FFI::string($item->data, $item->len);
            }
        }

        $ffi->redlite_free_bytes_array($result);
        return $values;
    }

    /**
     * @return string[]
     */
    public function rpop(string $key, int $count = 1): array
    {
        $this->ensureOpen();
        $ffi = self::getFFI();

        $result = $ffi->redlite_rpop($this->handle, $key, $count);

        $values = [];
        for ($i = 0; $i < $result->len; $i++) {
            $item = $result->items[$i];
            if ($item->data !== null) {
                $values[] = FFI::string($item->data, $item->len);
            }
        }

        $ffi->redlite_free_bytes_array($result);
        return $values;
    }

    public function llen(string $key): int
    {
        $this->ensureOpen();
        $ffi = self::getFFI();

        return $ffi->redlite_llen($this->handle, $key);
    }

    /**
     * @return string[]
     */
    public function lrange(string $key, int $start, int $stop): array
    {
        $this->ensureOpen();
        $ffi = self::getFFI();

        $result = $ffi->redlite_lrange($this->handle, $key, $start, $stop);

        $values = [];
        for ($i = 0; $i < $result->len; $i++) {
            $item = $result->items[$i];
            if ($item->data !== null) {
                $values[] = FFI::string($item->data, $item->len);
            }
        }

        $ffi->redlite_free_bytes_array($result);
        return $values;
    }

    public function lindex(string $key, int $index): ?string
    {
        $this->ensureOpen();
        $ffi = self::getFFI();

        $result = $ffi->redlite_lindex($this->handle, $key, $index);

        if ($result->data === null) {
            return null;
        }

        $value = FFI::string($result->data, $result->len);
        $ffi->redlite_free_bytes($result);
        return $value;
    }

    /**
     * Prepend values to a list only if the key exists.
     *
     * @param string[] $values
     */
    public function lpushx(string $key, array $values): int
    {
        $this->ensureOpen();
        $ffi = self::getFFI();

        $count = count($values);
        if ($count === 0) {
            return 0;
        }

        $valuesArray = $ffi->new("RedliteBytes[{$count}]");
        foreach ($values as $i => $value) {
            $valuesArray[$i]->data = $value;
            $valuesArray[$i]->len = strlen($value);
        }

        return $ffi->redlite_lpushx($this->handle, $key, $valuesArray, $count);
    }

    /**
     * Append values to a list only if the key exists.
     *
     * @param string[] $values
     */
    public function rpushx(string $key, array $values): int
    {
        $this->ensureOpen();
        $ffi = self::getFFI();

        $count = count($values);
        if ($count === 0) {
            return 0;
        }

        $valuesArray = $ffi->new("RedliteBytes[{$count}]");
        foreach ($values as $i => $value) {
            $valuesArray[$i]->data = $value;
            $valuesArray[$i]->len = strlen($value);
        }

        return $ffi->redlite_rpushx($this->handle, $key, $valuesArray, $count);
    }

    /**
     * Atomically move an element from source list to destination list.
     *
     * @param string $wherefrom "LEFT" or "RIGHT"
     * @param string $whereto "LEFT" or "RIGHT"
     */
    public function lmove(string $source, string $destination, string $wherefrom, string $whereto): ?string
    {
        $this->ensureOpen();
        $ffi = self::getFFI();

        $fromLeft = strtoupper($wherefrom) === 'LEFT' ? 0 : 1;
        $toLeft = strtoupper($whereto) === 'LEFT' ? 0 : 1;

        $result = $ffi->redlite_lmove($this->handle, $source, $destination, $fromLeft, $toLeft);

        if ($result->data === null) {
            return null;
        }

        $value = FFI::string($result->data, $result->len);
        $ffi->redlite_free_bytes($result);
        return $value;
    }

    /**
     * Find the position(s) of an element in a list.
     *
     * @return int[]
     */
    public function lpos(string $key, string $element, int $rank = 0, int $count = 0, int $maxlen = 0): array
    {
        $this->ensureOpen();
        $ffi = self::getFFI();

        $result = $ffi->redlite_lpos($this->handle, $key, $element, strlen($element), $rank, $count, $maxlen);

        $positions = [];
        for ($i = 0; $i < $result->len; $i++) {
            $item = $result->items[$i];
            if ($item->data !== null) {
                $posStr = FFI::string($item->data, $item->len);
                $positions[] = (int) $posStr;
            }
        }

        $ffi->redlite_free_bytes_array($result);
        return $positions;
    }

    // -------------------------------------------------------------------------
    // Set commands
    // -------------------------------------------------------------------------

    /**
     * @param string[] $members
     */
    public function sadd(string $key, array $members): int
    {
        $this->ensureOpen();
        $ffi = self::getFFI();

        $count = count($members);
        if ($count === 0) {
            return 0;
        }

        $membersArray = $ffi->new("RedliteBytes[{$count}]");
        foreach ($members as $i => $member) {
            $membersArray[$i]->data = $member;
            $membersArray[$i]->len = strlen($member);
        }

        return $ffi->redlite_sadd($this->handle, $key, $membersArray, $count);
    }

    /**
     * @param string[] $members
     */
    public function srem(string $key, array $members): int
    {
        $this->ensureOpen();
        $ffi = self::getFFI();

        $count = count($members);
        if ($count === 0) {
            return 0;
        }

        $membersArray = $ffi->new("RedliteBytes[{$count}]");
        foreach ($members as $i => $member) {
            $membersArray[$i]->data = $member;
            $membersArray[$i]->len = strlen($member);
        }

        return $ffi->redlite_srem($this->handle, $key, $membersArray, $count);
    }

    /**
     * @return string[]
     */
    public function smembers(string $key): array
    {
        $this->ensureOpen();
        $ffi = self::getFFI();

        $result = $ffi->redlite_smembers($this->handle, $key);

        $members = [];
        for ($i = 0; $i < $result->len; $i++) {
            $item = $result->items[$i];
            if ($item->data !== null) {
                $members[] = FFI::string($item->data, $item->len);
            }
        }

        $ffi->redlite_free_bytes_array($result);
        return $members;
    }

    public function sismember(string $key, string $member): bool
    {
        $this->ensureOpen();
        $ffi = self::getFFI();

        return $ffi->redlite_sismember($this->handle, $key, $member, strlen($member)) === 1;
    }

    public function scard(string $key): int
    {
        $this->ensureOpen();
        $ffi = self::getFFI();

        return $ffi->redlite_scard($this->handle, $key);
    }

    // -------------------------------------------------------------------------
    // Sorted set commands
    // -------------------------------------------------------------------------

    /**
     * @param array<string, float> $members member => score
     */
    public function zadd(string $key, array $members): int
    {
        $this->ensureOpen();
        $ffi = self::getFFI();

        $count = count($members);
        if ($count === 0) {
            return 0;
        }

        $membersArray = $ffi->new("RedliteZMember[{$count}]");
        $i = 0;
        foreach ($members as $member => $score) {
            $memberStr = (string) $member;
            $membersArray[$i]->score = (float) $score;
            $membersArray[$i]->member = $memberStr;
            $membersArray[$i]->member_len = strlen($memberStr);
            $i++;
        }

        return $ffi->redlite_zadd($this->handle, $key, $membersArray, $count);
    }

    /**
     * @param string[] $members
     */
    public function zrem(string $key, array $members): int
    {
        $this->ensureOpen();
        $ffi = self::getFFI();

        $count = count($members);
        if ($count === 0) {
            return 0;
        }

        $membersArray = $ffi->new("RedliteBytes[{$count}]");
        foreach ($members as $i => $member) {
            $membersArray[$i]->data = $member;
            $membersArray[$i]->len = strlen($member);
        }

        return $ffi->redlite_zrem($this->handle, $key, $membersArray, $count);
    }

    public function zscore(string $key, string $member): ?float
    {
        $this->ensureOpen();
        $ffi = self::getFFI();

        $result = $ffi->redlite_zscore($this->handle, $key, $member, strlen($member));

        if (is_nan($result)) {
            return null;
        }

        return $result;
    }

    public function zcard(string $key): int
    {
        $this->ensureOpen();
        $ffi = self::getFFI();

        return $ffi->redlite_zcard($this->handle, $key);
    }

    public function zcount(string $key, float $min, float $max): int
    {
        $this->ensureOpen();
        $ffi = self::getFFI();

        return $ffi->redlite_zcount($this->handle, $key, $min, $max);
    }

    public function zincrby(string $key, float $increment, string $member): float
    {
        $this->ensureOpen();
        $ffi = self::getFFI();

        return $ffi->redlite_zincrby($this->handle, $key, $increment, $member, strlen($member));
    }

    /**
     * @return string[]|array<string, float>
     */
    public function zrange(string $key, int $start, int $stop, bool $withScores = false): array
    {
        $this->ensureOpen();
        $ffi = self::getFFI();

        $result = $ffi->redlite_zrange($this->handle, $key, $start, $stop, $withScores ? 1 : 0);

        if ($withScores) {
            $values = [];
            for ($i = 0; $i < $result->len; $i += 2) {
                $memberItem = $result->items[$i];
                $scoreItem = $result->items[$i + 1];

                if ($memberItem->data !== null && $scoreItem->data !== null) {
                    $member = FFI::string($memberItem->data, $memberItem->len);
                    $score = (float) FFI::string($scoreItem->data, $scoreItem->len);
                    $values[$member] = $score;
                }
            }
        } else {
            $values = [];
            for ($i = 0; $i < $result->len; $i++) {
                $item = $result->items[$i];
                if ($item->data !== null) {
                    $values[] = FFI::string($item->data, $item->len);
                }
            }
        }

        $ffi->redlite_free_bytes_array($result);
        return $values;
    }

    /**
     * @return string[]|array<string, float>
     */
    public function zrevrange(string $key, int $start, int $stop, bool $withScores = false): array
    {
        $this->ensureOpen();
        $ffi = self::getFFI();

        $result = $ffi->redlite_zrevrange($this->handle, $key, $start, $stop, $withScores ? 1 : 0);

        if ($withScores) {
            $values = [];
            for ($i = 0; $i < $result->len; $i += 2) {
                $memberItem = $result->items[$i];
                $scoreItem = $result->items[$i + 1];

                if ($memberItem->data !== null && $scoreItem->data !== null) {
                    $member = FFI::string($memberItem->data, $memberItem->len);
                    $score = (float) FFI::string($scoreItem->data, $scoreItem->len);
                    $values[$member] = $score;
                }
            }
        } else {
            $values = [];
            for ($i = 0; $i < $result->len; $i++) {
                $item = $result->items[$i];
                if ($item->data !== null) {
                    $values[] = FFI::string($item->data, $item->len);
                }
            }
        }

        $ffi->redlite_free_bytes_array($result);
        return $values;
    }

    /**
     * Intersect sorted sets and store the result in a new key.
     *
     * @param string[] $keys Source keys
     * @param float[]|null $weights Optional weights for each key
     * @param string|null $aggregate "SUM", "MIN", or "MAX"
     */
    public function zinterstore(string $destination, array $keys, ?array $weights = null, ?string $aggregate = null): int
    {
        $this->ensureOpen();
        $ffi = self::getFFI();

        $keysCount = count($keys);
        if ($keysCount === 0) {
            return 0;
        }

        // Build keys array
        $keysArray = $ffi->new("char*[{$keysCount}]");
        foreach ($keys as $i => $key) {
            $keysArray[$i] = $key;
        }

        // Build weights array if provided
        $weightsPtr = null;
        $weightsLen = 0;
        if ($weights !== null && count($weights) > 0) {
            $weightsLen = count($weights);
            $weightsArray = $ffi->new("double[{$weightsLen}]");
            foreach ($weights as $i => $weight) {
                $weightsArray[$i] = (float) $weight;
            }
            $weightsPtr = $weightsArray;
        }

        return $ffi->redlite_zinterstore(
            $this->handle,
            $destination,
            $keysArray,
            $keysCount,
            $weightsPtr,
            $weightsLen,
            $aggregate
        );
    }

    /**
     * Union sorted sets and store the result in a new key.
     *
     * @param string[] $keys Source keys
     * @param float[]|null $weights Optional weights for each key
     * @param string|null $aggregate "SUM", "MIN", or "MAX"
     */
    public function zunionstore(string $destination, array $keys, ?array $weights = null, ?string $aggregate = null): int
    {
        $this->ensureOpen();
        $ffi = self::getFFI();

        $keysCount = count($keys);
        if ($keysCount === 0) {
            return 0;
        }

        // Build keys array
        $keysArray = $ffi->new("char*[{$keysCount}]");
        foreach ($keys as $i => $key) {
            $keysArray[$i] = $key;
        }

        // Build weights array if provided
        $weightsPtr = null;
        $weightsLen = 0;
        if ($weights !== null && count($weights) > 0) {
            $weightsLen = count($weights);
            $weightsArray = $ffi->new("double[{$weightsLen}]");
            foreach ($weights as $i => $weight) {
                $weightsArray[$i] = (float) $weight;
            }
            $weightsPtr = $weightsArray;
        }

        return $ffi->redlite_zunionstore(
            $this->handle,
            $destination,
            $keysArray,
            $keysCount,
            $weightsPtr,
            $weightsLen,
            $aggregate
        );
    }

    // -------------------------------------------------------------------------
    // Utility commands
    // -------------------------------------------------------------------------

    public function vacuum(): int
    {
        $this->ensureOpen();
        $ffi = self::getFFI();

        return $ffi->redlite_vacuum($this->handle);
    }

    // -------------------------------------------------------------------------
    // JSON commands (ReJSON-compatible)
    // -------------------------------------------------------------------------

    /**
     * Set a JSON value at a path.
     *
     * @param bool $nx Only set if key does not exist
     * @param bool $xx Only set if key already exists
     */
    public function jsonSet(string $key, string $path, string $value, bool $nx = false, bool $xx = false): bool
    {
        $this->ensureOpen();
        $ffi = self::getFFI();

        $result = $ffi->redlite_json_set($this->handle, $key, $path, $value, $nx ? 1 : 0, $xx ? 1 : 0);
        return $result === 0;
    }

    /**
     * Get JSON value(s) at path(s).
     *
     * @param string[] $paths Paths to get (defaults to "$" if empty)
     */
    public function jsonGet(string $key, array $paths = []): ?string
    {
        $this->ensureOpen();
        $ffi = self::getFFI();

        if (empty($paths)) {
            $paths = ['$'];
        }

        $count = count($paths);
        $pathsArray = $ffi->new("char*[{$count}]");
        foreach ($paths as $i => $path) {
            $pathsArray[$i] = $path;
        }

        $ptr = $ffi->redlite_json_get($this->handle, $key, $pathsArray, $count);

        if ($ptr === null) {
            return null;
        }

        $result = FFI::string($ptr);
        $ffi->redlite_free_string($ptr);
        return $result;
    }

    /**
     * Delete JSON value at path.
     *
     * @return int Number of paths deleted
     */
    public function jsonDel(string $key, ?string $path = null): int
    {
        $this->ensureOpen();
        $ffi = self::getFFI();

        return $ffi->redlite_json_del($this->handle, $key, $path ?? '$');
    }

    /**
     * Get the type of JSON value at path.
     */
    public function jsonType(string $key, ?string $path = null): ?string
    {
        $this->ensureOpen();
        $ffi = self::getFFI();

        $ptr = $ffi->redlite_json_type($this->handle, $key, $path ?? '$');

        if ($ptr === null) {
            return null;
        }

        $result = FFI::string($ptr);
        $ffi->redlite_free_string($ptr);
        return $result;
    }

    /**
     * Increment numeric value at path.
     *
     * @return string|null New value as string
     */
    public function jsonNumIncrBy(string $key, string $path, float $increment): ?string
    {
        $this->ensureOpen();
        $ffi = self::getFFI();

        $ptr = $ffi->redlite_json_numincrby($this->handle, $key, $path, $increment);

        if ($ptr === null) {
            return null;
        }

        $result = FFI::string($ptr);
        $ffi->redlite_free_string($ptr);
        return $result;
    }

    /**
     * Append to JSON string at path.
     *
     * @return int New string length
     */
    public function jsonStrAppend(string $key, string $path, string $value): int
    {
        $this->ensureOpen();
        $ffi = self::getFFI();

        return $ffi->redlite_json_strappend($this->handle, $key, $path, $value);
    }

    /**
     * Get length of JSON string at path.
     */
    public function jsonStrLen(string $key, ?string $path = null): int
    {
        $this->ensureOpen();
        $ffi = self::getFFI();

        return $ffi->redlite_json_strlen($this->handle, $key, $path ?? '$');
    }

    /**
     * Append values to JSON array.
     *
     * @param string[] $values JSON-encoded values to append
     * @return int New array length
     */
    public function jsonArrAppend(string $key, string $path, array $values): int
    {
        $this->ensureOpen();
        $ffi = self::getFFI();

        $count = count($values);
        if ($count === 0) {
            return 0;
        }

        $valuesArray = $ffi->new("char*[{$count}]");
        foreach ($values as $i => $value) {
            $valuesArray[$i] = $value;
        }

        return $ffi->redlite_json_arrappend($this->handle, $key, $path, $valuesArray, $count);
    }

    /**
     * Get length of JSON array at path.
     */
    public function jsonArrLen(string $key, ?string $path = null): int
    {
        $this->ensureOpen();
        $ffi = self::getFFI();

        return $ffi->redlite_json_arrlen($this->handle, $key, $path ?? '$');
    }

    /**
     * Pop element from JSON array.
     *
     * @param int $index Index to pop from (-1 = last element)
     * @return string|null Popped element as JSON string
     */
    public function jsonArrPop(string $key, ?string $path = null, int $index = -1): ?string
    {
        $this->ensureOpen();
        $ffi = self::getFFI();

        $ptr = $ffi->redlite_json_arrpop($this->handle, $key, $path ?? '$', $index);

        if ($ptr === null) {
            return null;
        }

        $result = FFI::string($ptr);
        $ffi->redlite_free_string($ptr);
        return $result;
    }

    /**
     * Clear container values (arrays/objects).
     *
     * @return int Number of containers cleared
     */
    public function jsonClear(string $key, ?string $path = null): int
    {
        $this->ensureOpen();
        $ffi = self::getFFI();

        return $ffi->redlite_json_clear($this->handle, $key, $path ?? '$');
    }

    // -------------------------------------------------------------------------
    // History commands
    // -------------------------------------------------------------------------

    /**
     * Enable history tracking globally.
     *
     * @param string $retentionType "unlimited", "time", or "count"
     * @param int $retentionValue Value for time (ms) or count retention
     */
    public function historyEnableGlobal(string $retentionType = 'unlimited', int $retentionValue = 0): bool
    {
        $this->ensureOpen();
        $ffi = self::getFFI();

        $result = $ffi->redlite_history_enable_global($this->handle, $retentionType, $retentionValue);
        return $result === 0;
    }

    /**
     * Enable history tracking for a specific database.
     */
    public function historyEnableDatabase(int $dbNum, string $retentionType = 'unlimited', int $retentionValue = 0): bool
    {
        $this->ensureOpen();
        $ffi = self::getFFI();

        $result = $ffi->redlite_history_enable_database($this->handle, $dbNum, $retentionType, $retentionValue);
        return $result === 0;
    }

    /**
     * Enable history tracking for a specific key.
     */
    public function historyEnableKey(string $key, string $retentionType = 'unlimited', int $retentionValue = 0): bool
    {
        $this->ensureOpen();
        $ffi = self::getFFI();

        $result = $ffi->redlite_history_enable_key($this->handle, $key, $retentionType, $retentionValue);
        return $result === 0;
    }

    /**
     * Disable history tracking globally.
     */
    public function historyDisableGlobal(): bool
    {
        $this->ensureOpen();
        $ffi = self::getFFI();

        $result = $ffi->redlite_history_disable_global($this->handle);
        return $result === 0;
    }

    /**
     * Disable history tracking for a specific database.
     */
    public function historyDisableDatabase(int $dbNum): bool
    {
        $this->ensureOpen();
        $ffi = self::getFFI();

        $result = $ffi->redlite_history_disable_database($this->handle, $dbNum);
        return $result === 0;
    }

    /**
     * Disable history tracking for a specific key.
     */
    public function historyDisableKey(string $key): bool
    {
        $this->ensureOpen();
        $ffi = self::getFFI();

        $result = $ffi->redlite_history_disable_key($this->handle, $key);
        return $result === 0;
    }

    /**
     * Check if history tracking is enabled for a key.
     */
    public function isHistoryEnabled(string $key): bool
    {
        $this->ensureOpen();
        $ffi = self::getFFI();

        return $ffi->redlite_is_history_enabled($this->handle, $key) === 1;
    }

    // -------------------------------------------------------------------------
    // FTS (Full-Text Search) commands
    // -------------------------------------------------------------------------

    /**
     * Enable full-text search globally.
     */
    public function ftsEnableGlobal(): bool
    {
        $this->ensureOpen();
        $ffi = self::getFFI();

        $result = $ffi->redlite_fts_enable_global($this->handle);
        return $result === 0;
    }

    /**
     * Enable full-text search for a specific database.
     */
    public function ftsEnableDatabase(int $dbNum): bool
    {
        $this->ensureOpen();
        $ffi = self::getFFI();

        $result = $ffi->redlite_fts_enable_database($this->handle, $dbNum);
        return $result === 0;
    }

    /**
     * Enable full-text search for keys matching a pattern.
     */
    public function ftsEnablePattern(string $pattern): bool
    {
        $this->ensureOpen();
        $ffi = self::getFFI();

        $result = $ffi->redlite_fts_enable_pattern($this->handle, $pattern);
        return $result === 0;
    }

    /**
     * Enable full-text search for a specific key.
     */
    public function ftsEnableKey(string $key): bool
    {
        $this->ensureOpen();
        $ffi = self::getFFI();

        $result = $ffi->redlite_fts_enable_key($this->handle, $key);
        return $result === 0;
    }

    /**
     * Disable full-text search globally.
     */
    public function ftsDisableGlobal(): bool
    {
        $this->ensureOpen();
        $ffi = self::getFFI();

        $result = $ffi->redlite_fts_disable_global($this->handle);
        return $result === 0;
    }

    /**
     * Disable full-text search for a specific database.
     */
    public function ftsDisableDatabase(int $dbNum): bool
    {
        $this->ensureOpen();
        $ffi = self::getFFI();

        $result = $ffi->redlite_fts_disable_database($this->handle, $dbNum);
        return $result === 0;
    }

    /**
     * Disable full-text search for keys matching a pattern.
     */
    public function ftsDisablePattern(string $pattern): bool
    {
        $this->ensureOpen();
        $ffi = self::getFFI();

        $result = $ffi->redlite_fts_disable_pattern($this->handle, $pattern);
        return $result === 0;
    }

    /**
     * Disable full-text search for a specific key.
     */
    public function ftsDisableKey(string $key): bool
    {
        $this->ensureOpen();
        $ffi = self::getFFI();

        $result = $ffi->redlite_fts_disable_key($this->handle, $key);
        return $result === 0;
    }

    /**
     * Check if full-text search is enabled for a key.
     */
    public function isFtsEnabled(string $key): bool
    {
        $this->ensureOpen();
        $ffi = self::getFFI();

        return $ffi->redlite_is_fts_enabled($this->handle, $key) === 1;
    }

    // -------------------------------------------------------------------------
    // KeyInfo command
    // -------------------------------------------------------------------------

    /**
     * Get detailed information about a key.
     *
     * @return array{type: string, ttl: int, created_at: int, updated_at: int}|null
     */
    public function keyinfo(string $key): ?array
    {
        $this->ensureOpen();
        $ffi = self::getFFI();

        $info = $ffi->redlite_keyinfo($this->handle, $key);

        if ($info->valid === 0) {
            $ffi->redlite_free_keyinfo($info);
            return null;
        }

        $result = [
            'type' => FFI::string($info->key_type),
            'ttl' => $info->ttl,
            'created_at' => $info->created_at,
            'updated_at' => $info->updated_at,
        ];

        $ffi->redlite_free_keyinfo($info);
        return $result;
    }
}
