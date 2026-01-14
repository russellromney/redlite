"""
CFFI bindings for redlite.

This module provides direct FFI access to the redlite shared library.
"""

import os
import sys
import platform
from pathlib import Path
from typing import Optional

import cffi

# C definitions matching redlite.h
_CDEF = """
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

typedef struct RedliteZMember {
    double score;
    const uint8_t *member;
    size_t member_len;
} RedliteZMember;

typedef struct RedliteKV {
    const char *key;
    const uint8_t *value;
    size_t value_len;
} RedliteKV;

// Lifecycle
RedliteDb *redlite_open(const char *path);
RedliteDb *redlite_open_memory(void);
RedliteDb *redlite_open_with_cache(const char *path, int64_t cache_mb);
void redlite_close(RedliteDb *db);
char *redlite_last_error(void);

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

// List commands
int64_t redlite_lpush(RedliteDb *db, const char *key, const RedliteBytes *values, size_t values_len);
int64_t redlite_rpush(RedliteDb *db, const char *key, const RedliteBytes *values, size_t values_len);
RedliteBytesArray redlite_lpop(RedliteDb *db, const char *key, size_t count);
RedliteBytesArray redlite_rpop(RedliteDb *db, const char *key, size_t count);
int64_t redlite_llen(RedliteDb *db, const char *key);
RedliteBytesArray redlite_lrange(RedliteDb *db, const char *key, int64_t start, int64_t stop);
RedliteBytes redlite_lindex(RedliteDb *db, const char *key, int64_t index);

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

// Server commands
int64_t redlite_vacuum(RedliteDb *db);
char *redlite_version(void);
"""

ffi = cffi.FFI()
ffi.cdef(_CDEF)

_lib: Optional[object] = None


def _find_library() -> str:
    """Find the redlite shared library."""
    system = platform.system().lower()
    machine = platform.machine().lower()

    # Normalize architecture names
    if machine in ("x86_64", "amd64"):
        machine = "x86_64"
    elif machine in ("arm64", "aarch64"):
        machine = "aarch64"

    # Library extension by platform
    if system == "darwin":
        ext = "dylib"
    elif system == "windows":
        ext = "dll"
    else:
        ext = "so"

    lib_name = f"libredlite_ffi.{ext}"

    # Search paths (in order of priority)
    search_paths = []

    # 1. REDLITE_LIB_PATH environment variable
    env_path = os.environ.get("REDLITE_LIB_PATH")
    if env_path:
        search_paths.append(Path(env_path))

    # 2. Next to this file (bundled with package)
    pkg_dir = Path(__file__).parent
    search_paths.extend([
        pkg_dir / lib_name,
        pkg_dir / "_lib" / f"{system}-{machine}" / lib_name,
        pkg_dir / "_lib" / lib_name,
    ])

    # 3. Development paths (relative to sdks/python)
    dev_paths = [
        pkg_dir.parent.parent.parent.parent / "crates" / "redlite-ffi" / "target" / "release" / lib_name,
        pkg_dir.parent.parent.parent.parent / "target" / "release" / lib_name,
    ]
    search_paths.extend(dev_paths)

    # 4. System paths
    if system == "darwin":
        search_paths.extend([
            Path("/usr/local/lib") / lib_name,
            Path("/opt/homebrew/lib") / lib_name,
            Path.home() / ".local" / "lib" / lib_name,
        ])
    elif system == "linux":
        search_paths.extend([
            Path("/usr/local/lib") / lib_name,
            Path("/usr/lib") / lib_name,
            Path.home() / ".local" / "lib" / lib_name,
        ])

    # Find first existing path
    for path in search_paths:
        if path.is_file():
            return str(path)

    raise OSError(
        f"Could not find {lib_name}. "
        "Set REDLITE_LIB_PATH environment variable or install the library."
    )


def get_lib():
    """Get or load the redlite FFI library."""
    global _lib
    if _lib is None:
        lib_path = _find_library()
        _lib = ffi.dlopen(lib_path)
    return _lib


def get_ffi():
    """Get the CFFI FFI object."""
    return ffi


class RedliteError(Exception):
    """Error from redlite library."""
    pass


def check_error():
    """Check and raise any pending error."""
    lib = get_lib()
    err = lib.redlite_last_error()
    if err != ffi.NULL:
        msg = ffi.string(err).decode("utf-8")
        lib.redlite_free_string(err)
        raise RedliteError(msg)


def bytes_to_python(rb) -> Optional[bytes]:
    """Convert RedliteBytes to Python bytes."""
    if rb.data == ffi.NULL or rb.len == 0:
        return None
    result = ffi.buffer(rb.data, rb.len)[:]
    get_lib().redlite_free_bytes(rb)
    return result


def string_array_to_python(arr) -> list:
    """Convert RedliteStringArray to Python list."""
    lib = get_lib()
    result = []
    if arr.strings != ffi.NULL and arr.len > 0:
        for i in range(arr.len):
            s = arr.strings[i]
            if s != ffi.NULL:
                result.append(ffi.string(s).decode("utf-8"))
        lib.redlite_free_string_array(arr)
    return result


def bytes_array_to_python(arr) -> list:
    """Convert RedliteBytesArray to Python list."""
    lib = get_lib()
    result = []
    if arr.items != ffi.NULL and arr.len > 0:
        for i in range(arr.len):
            item = arr.items[i]
            if item.data != ffi.NULL and item.len > 0:
                result.append(ffi.buffer(item.data, item.len)[:])
            else:
                result.append(None)
        lib.redlite_free_bytes_array(arr)
    return result
