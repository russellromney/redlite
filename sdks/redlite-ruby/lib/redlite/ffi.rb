# frozen_string_literal: true

require "ffi"
require "rbconfig"

module Redlite
  # FFI bindings to the Redlite C library
  module FFI
    extend ::FFI::Library

    class << self
      # Find the appropriate library path based on platform
      def find_library_path
        # Check environment variable first
        if ENV["REDLITE_LIB_PATH"]
          return ENV["REDLITE_LIB_PATH"]
        end

        # Determine library extension based on platform
        ext = case RbConfig::CONFIG["host_os"]
              when /darwin/i then "dylib"
              when /linux/i then "so"
              when /mswin|mingw|cygwin/i then "dll"
              else "so"
              end

        lib_name = ext == "dll" ? "redlite_ffi.#{ext}" : "libredlite_ffi.#{ext}"

        # Search paths in order of preference
        search_paths = [
          # Relative to the SDK (development) - crates/redlite-ffi/target
          File.expand_path("../../../../crates/redlite-ffi/target/release/#{lib_name}", __dir__),
          File.expand_path("../../../../crates/redlite-ffi/target/debug/#{lib_name}", __dir__),
          # Workspace target (if using cargo workspace)
          File.expand_path("../../../../target/release/#{lib_name}", __dir__),
          File.expand_path("../../../../target/debug/#{lib_name}", __dir__),
          # Bundled with gem
          File.expand_path("../../_binary/#{RbConfig::CONFIG['host_cpu']}-#{RbConfig::CONFIG['host_os']}/#{lib_name}", __dir__),
          # System paths
          "/usr/local/lib/#{lib_name}",
          "/usr/lib/#{lib_name}",
        ]

        search_paths.each do |path|
          return path if File.exist?(path)
        end

        # Fall back to letting FFI find it
        lib_name
      end
    end

    ffi_lib_flags :now, :global
    ffi_lib find_library_path

    # =========================================================================
    # Struct Definitions
    # =========================================================================

    # Result of operations that return bytes
    class RedliteBytes < ::FFI::Struct
      layout :data, :pointer,
             :len, :size_t
    end

    # Result of operations that return a string array
    class RedliteStringArray < ::FFI::Struct
      layout :strings, :pointer,
             :len, :size_t
    end

    # Result of operations that return bytes array
    class RedliteBytesArray < ::FFI::Struct
      layout :items, :pointer,
             :len, :size_t
    end

    # Key-value pair for hash/mset operations
    class RedliteKV < ::FFI::Struct
      layout :key, :pointer,
             :value, :pointer,
             :value_len, :size_t
    end

    # Sorted set member
    class RedliteZMember < ::FFI::Struct
      layout :score, :double,
             :member, :pointer,
             :member_len, :size_t
    end

    # =========================================================================
    # Lifecycle Functions
    # =========================================================================

    # Open a database at the given path
    attach_function :redlite_open, [:string], :pointer

    # Open an in-memory database
    attach_function :redlite_open_memory, [], :pointer

    # Open a database with custom cache size
    attach_function :redlite_open_with_cache, [:string, :int64], :pointer

    # Close a database and free resources
    attach_function :redlite_close, [:pointer], :void

    # Get the last error message (NULL if no error)
    attach_function :redlite_last_error, [], :pointer

    # =========================================================================
    # Memory Management Functions
    # =========================================================================

    # Free a string returned by redlite functions
    attach_function :redlite_free_string, [:pointer], :void

    # Free bytes returned by redlite functions
    attach_function :redlite_free_bytes, [RedliteBytes.by_value], :void

    # Free a string array returned by redlite functions
    attach_function :redlite_free_string_array, [RedliteStringArray.by_value], :void

    # Free a bytes array returned by redlite functions
    attach_function :redlite_free_bytes_array, [RedliteBytesArray.by_value], :void

    # =========================================================================
    # String Commands
    # =========================================================================

    # GET key
    attach_function :redlite_get, [:pointer, :string], RedliteBytes.by_value

    # SET key value [ttl_seconds]
    attach_function :redlite_set, [:pointer, :string, :pointer, :size_t, :int64], :int

    # SETEX key seconds value
    attach_function :redlite_setex, [:pointer, :string, :int64, :pointer, :size_t], :int

    # PSETEX key milliseconds value
    attach_function :redlite_psetex, [:pointer, :string, :int64, :pointer, :size_t], :int

    # GETDEL key
    attach_function :redlite_getdel, [:pointer, :string], RedliteBytes.by_value

    # APPEND key value
    attach_function :redlite_append, [:pointer, :string, :pointer, :size_t], :int64

    # STRLEN key
    attach_function :redlite_strlen, [:pointer, :string], :int64

    # GETRANGE key start end
    attach_function :redlite_getrange, [:pointer, :string, :int64, :int64], RedliteBytes.by_value

    # SETRANGE key offset value
    attach_function :redlite_setrange, [:pointer, :string, :int64, :pointer, :size_t], :int64

    # INCR key
    attach_function :redlite_incr, [:pointer, :string], :int64

    # DECR key
    attach_function :redlite_decr, [:pointer, :string], :int64

    # INCRBY key increment
    attach_function :redlite_incrby, [:pointer, :string, :int64], :int64

    # DECRBY key decrement
    attach_function :redlite_decrby, [:pointer, :string, :int64], :int64

    # INCRBYFLOAT key increment - returns string (caller must free)
    attach_function :redlite_incrbyfloat, [:pointer, :string, :double], :pointer

    # MGET key [key ...]
    attach_function :redlite_mget, [:pointer, :pointer, :size_t], RedliteBytesArray.by_value

    # MSET key value [key value ...]
    attach_function :redlite_mset, [:pointer, :pointer, :size_t], :int

    # SETNX key value - set only if not exists
    attach_function :redlite_setnx, [:pointer, :string, :pointer, :size_t], :int

    # GETEX key [EX seconds] [PX milliseconds] [EXAT timestamp] [PXAT ms-timestamp] [PERSIST]
    attach_function :redlite_getex, [:pointer, :string, :int64, :int64, :int64, :int64, :int], RedliteBytes.by_value

    # GETBIT key offset
    attach_function :redlite_getbit, [:pointer, :string, :uint64], :int64

    # SETBIT key offset value
    attach_function :redlite_setbit, [:pointer, :string, :uint64, :int], :int64

    # BITCOUNT key [start end]
    attach_function :redlite_bitcount, [:pointer, :string, :int64, :int64, :int], :int64

    # BITOP operation destkey key [key ...]
    attach_function :redlite_bitop, [:pointer, :string, :string, :pointer, :size_t], :int64

    # =========================================================================
    # Key Commands
    # =========================================================================

    # DEL key [key ...]
    attach_function :redlite_del, [:pointer, :pointer, :size_t], :int64

    # EXISTS key [key ...]
    attach_function :redlite_exists, [:pointer, :pointer, :size_t], :int64

    # TYPE key
    attach_function :redlite_type, [:pointer, :string], :pointer

    # TTL key
    attach_function :redlite_ttl, [:pointer, :string], :int64

    # PTTL key
    attach_function :redlite_pttl, [:pointer, :string], :int64

    # EXPIRE key seconds
    attach_function :redlite_expire, [:pointer, :string, :int64], :int

    # PEXPIRE key milliseconds
    attach_function :redlite_pexpire, [:pointer, :string, :int64], :int

    # EXPIREAT key unix_timestamp
    attach_function :redlite_expireat, [:pointer, :string, :int64], :int

    # PEXPIREAT key unix_timestamp_ms
    attach_function :redlite_pexpireat, [:pointer, :string, :int64], :int

    # PERSIST key
    attach_function :redlite_persist, [:pointer, :string], :int

    # RENAME key newkey
    attach_function :redlite_rename, [:pointer, :string, :string], :int

    # RENAMENX key newkey
    attach_function :redlite_renamenx, [:pointer, :string, :string], :int

    # KEYS pattern
    attach_function :redlite_keys, [:pointer, :string], RedliteStringArray.by_value

    # DBSIZE
    attach_function :redlite_dbsize, [:pointer], :int64

    # FLUSHDB
    attach_function :redlite_flushdb, [:pointer], :int

    # SELECT db
    attach_function :redlite_select, [:pointer, :int], :int

    # =========================================================================
    # Hash Commands
    # =========================================================================

    # HSET key field value [field value ...]
    attach_function :redlite_hset, [:pointer, :string, :pointer, :pointer, :size_t], :int64

    # HGET key field
    attach_function :redlite_hget, [:pointer, :string, :string], RedliteBytes.by_value

    # HDEL key field [field ...]
    attach_function :redlite_hdel, [:pointer, :string, :pointer, :size_t], :int64

    # HEXISTS key field
    attach_function :redlite_hexists, [:pointer, :string, :string], :int

    # HLEN key
    attach_function :redlite_hlen, [:pointer, :string], :int64

    # HKEYS key
    attach_function :redlite_hkeys, [:pointer, :string], RedliteStringArray.by_value

    # HVALS key
    attach_function :redlite_hvals, [:pointer, :string], RedliteBytesArray.by_value

    # HINCRBY key field increment
    attach_function :redlite_hincrby, [:pointer, :string, :string, :int64], :int64

    # HGETALL key
    attach_function :redlite_hgetall, [:pointer, :string], RedliteBytesArray.by_value

    # HMGET key field [field ...]
    attach_function :redlite_hmget, [:pointer, :string, :pointer, :size_t], RedliteBytesArray.by_value

    # HSETNX key field value
    attach_function :redlite_hsetnx, [:pointer, :string, :string, :pointer, :size_t], :int

    # HINCRBYFLOAT key field increment
    attach_function :redlite_hincrbyfloat, [:pointer, :string, :string, :double], :pointer

    # =========================================================================
    # List Commands
    # =========================================================================

    # LPUSH key value [value ...]
    attach_function :redlite_lpush, [:pointer, :string, :pointer, :size_t], :int64

    # RPUSH key value [value ...]
    attach_function :redlite_rpush, [:pointer, :string, :pointer, :size_t], :int64

    # LPOP key [count]
    attach_function :redlite_lpop, [:pointer, :string, :size_t], RedliteBytesArray.by_value

    # RPOP key [count]
    attach_function :redlite_rpop, [:pointer, :string, :size_t], RedliteBytesArray.by_value

    # LLEN key
    attach_function :redlite_llen, [:pointer, :string], :int64

    # LRANGE key start stop
    attach_function :redlite_lrange, [:pointer, :string, :int64, :int64], RedliteBytesArray.by_value

    # LINDEX key index
    attach_function :redlite_lindex, [:pointer, :string, :int64], RedliteBytes.by_value

    # LPUSHX key value [value ...] - push only if list exists
    attach_function :redlite_lpushx, [:pointer, :string, :pointer, :size_t], :int64

    # RPUSHX key value [value ...] - push only if list exists
    attach_function :redlite_rpushx, [:pointer, :string, :pointer, :size_t], :int64

    # LMOVE source destination LEFT|RIGHT LEFT|RIGHT
    # wherefrom: 0 for LEFT, 1 for RIGHT
    # whereto: 0 for LEFT, 1 for RIGHT
    attach_function :redlite_lmove, [:pointer, :string, :string, :int, :int], RedliteBytes.by_value

    # LPOS key element [RANK rank] [COUNT count] [MAXLEN maxlen]
    attach_function :redlite_lpos, [:pointer, :string, :pointer, :size_t, :int64, :size_t, :size_t], RedliteBytesArray.by_value

    # LSET key index element
    attach_function :redlite_lset, [:pointer, :string, :int64, :pointer, :size_t], :int

    # LTRIM key start stop
    attach_function :redlite_ltrim, [:pointer, :string, :int64, :int64], :int

    # LREM key count element
    attach_function :redlite_lrem, [:pointer, :string, :int64, :pointer, :size_t], :int64

    # LINSERT key BEFORE|AFTER pivot element
    attach_function :redlite_linsert, [:pointer, :string, :int, :pointer, :size_t, :pointer, :size_t], :int64

    # =========================================================================
    # Set Commands
    # =========================================================================

    # SADD key member [member ...]
    attach_function :redlite_sadd, [:pointer, :string, :pointer, :size_t], :int64

    # SREM key member [member ...]
    attach_function :redlite_srem, [:pointer, :string, :pointer, :size_t], :int64

    # SMEMBERS key
    attach_function :redlite_smembers, [:pointer, :string], RedliteBytesArray.by_value

    # SISMEMBER key member
    attach_function :redlite_sismember, [:pointer, :string, :pointer, :size_t], :int

    # SCARD key
    attach_function :redlite_scard, [:pointer, :string], :int64

    # SPOP key [count]
    attach_function :redlite_spop, [:pointer, :string, :size_t], RedliteBytesArray.by_value

    # SRANDMEMBER key [count]
    attach_function :redlite_srandmember, [:pointer, :string, :int64], RedliteBytesArray.by_value

    # SDIFF key [key ...]
    attach_function :redlite_sdiff, [:pointer, :pointer, :size_t], RedliteBytesArray.by_value

    # SINTER key [key ...]
    attach_function :redlite_sinter, [:pointer, :pointer, :size_t], RedliteBytesArray.by_value

    # SUNION key [key ...]
    attach_function :redlite_sunion, [:pointer, :pointer, :size_t], RedliteBytesArray.by_value

    # SDIFFSTORE destination key [key ...]
    attach_function :redlite_sdiffstore, [:pointer, :string, :pointer, :size_t], :int64

    # SINTERSTORE destination key [key ...]
    attach_function :redlite_sinterstore, [:pointer, :string, :pointer, :size_t], :int64

    # SUNIONSTORE destination key [key ...]
    attach_function :redlite_sunionstore, [:pointer, :string, :pointer, :size_t], :int64

    # SMOVE source destination member
    attach_function :redlite_smove, [:pointer, :string, :string, :pointer, :size_t], :int

    # =========================================================================
    # Sorted Set Commands
    # =========================================================================

    # ZADD key score member [score member ...]
    attach_function :redlite_zadd, [:pointer, :string, :pointer, :size_t], :int64

    # ZREM key member [member ...]
    attach_function :redlite_zrem, [:pointer, :string, :pointer, :size_t], :int64

    # ZSCORE key member
    attach_function :redlite_zscore, [:pointer, :string, :pointer, :size_t], :double

    # ZCARD key
    attach_function :redlite_zcard, [:pointer, :string], :int64

    # ZCOUNT key min max
    attach_function :redlite_zcount, [:pointer, :string, :double, :double], :int64

    # ZINCRBY key increment member
    attach_function :redlite_zincrby, [:pointer, :string, :double, :pointer, :size_t], :double

    # ZRANGE key start stop [withscores]
    attach_function :redlite_zrange, [:pointer, :string, :int64, :int64, :int], RedliteBytesArray.by_value

    # ZREVRANGE key start stop [withscores]
    attach_function :redlite_zrevrange, [:pointer, :string, :int64, :int64, :int], RedliteBytesArray.by_value

    # ZRANK key member
    attach_function :redlite_zrank, [:pointer, :string, :pointer, :size_t], :int64

    # ZREVRANK key member
    attach_function :redlite_zrevrank, [:pointer, :string, :pointer, :size_t], :int64

    # ZRANGEBYSCORE key min max [LIMIT offset count]
    attach_function :redlite_zrangebyscore, [:pointer, :string, :double, :double, :int64, :size_t], RedliteBytesArray.by_value

    # ZREMRANGEBYRANK key start stop
    attach_function :redlite_zremrangebyrank, [:pointer, :string, :int64, :int64], :int64

    # ZREMRANGEBYSCORE key min max
    attach_function :redlite_zremrangebyscore, [:pointer, :string, :double, :double], :int64

    # ZINTERSTORE destination numkeys key [key ...] [WEIGHTS weight ...] [AGGREGATE SUM|MIN|MAX]
    attach_function :redlite_zinterstore, [:pointer, :string, :pointer, :size_t, :pointer, :size_t, :string], :int64

    # ZUNIONSTORE destination numkeys key [key ...] [WEIGHTS weight ...] [AGGREGATE SUM|MIN|MAX]
    attach_function :redlite_zunionstore, [:pointer, :string, :pointer, :size_t, :pointer, :size_t, :string], :int64

    # =========================================================================
    # JSON Commands
    # =========================================================================

    # JSON.SET key path value [NX] [XX]
    attach_function :redlite_json_set, [:pointer, :string, :string, :string, :int, :int], :int

    # JSON.GET key path - returns string (caller must free)
    attach_function :redlite_json_get, [:pointer, :string, :pointer, :size_t], :pointer

    # JSON.DEL key [path]
    attach_function :redlite_json_del, [:pointer, :string, :string], :int64

    # JSON.TYPE key [path]
    attach_function :redlite_json_type, [:pointer, :string, :string], :pointer

    # JSON.NUMINCRBY key path increment - returns string (caller must free)
    attach_function :redlite_json_numincrby, [:pointer, :string, :string, :double], :pointer

    # JSON.STRAPPEND key path value
    attach_function :redlite_json_strappend, [:pointer, :string, :string, :string], :int64

    # JSON.STRLEN key [path]
    attach_function :redlite_json_strlen, [:pointer, :string, :string], :int64

    # JSON.ARRAPPEND key path value [value ...]
    attach_function :redlite_json_arrappend, [:pointer, :string, :string, :pointer, :size_t], :int64

    # JSON.ARRLEN key [path]
    attach_function :redlite_json_arrlen, [:pointer, :string, :string], :int64

    # JSON.ARRPOP key [path [index]]
    attach_function :redlite_json_arrpop, [:pointer, :string, :string, :int64], :pointer

    # JSON.CLEAR key [path]
    attach_function :redlite_json_clear, [:pointer, :string, :string], :int64

    # =========================================================================
    # History Enable/Disable Commands
    # =========================================================================

    # HISTORY.ENABLE GLOBAL [retention_type retention_value]
    attach_function :redlite_history_enable_global, [:pointer, :string, :int64], :int

    # HISTORY.ENABLE DATABASE db_num [retention_type retention_value]
    attach_function :redlite_history_enable_database, [:pointer, :int, :string, :int64], :int

    # HISTORY.ENABLE KEY key [retention_type retention_value]
    attach_function :redlite_history_enable_key, [:pointer, :string, :string, :int64], :int

    # HISTORY.DISABLE GLOBAL
    attach_function :redlite_history_disable_global, [:pointer], :int

    # HISTORY.DISABLE DATABASE db_num
    attach_function :redlite_history_disable_database, [:pointer, :int], :int

    # HISTORY.DISABLE KEY key
    attach_function :redlite_history_disable_key, [:pointer, :string], :int

    # Check if history is enabled for a key
    attach_function :redlite_is_history_enabled, [:pointer, :string], :int

    # =========================================================================
    # FTS Enable/Disable Commands
    # =========================================================================

    # FTS.ENABLE GLOBAL
    attach_function :redlite_fts_enable_global, [:pointer], :int

    # FTS.ENABLE DATABASE db_num
    attach_function :redlite_fts_enable_database, [:pointer, :int], :int

    # FTS.ENABLE PATTERN pattern
    attach_function :redlite_fts_enable_pattern, [:pointer, :string], :int

    # FTS.ENABLE KEY key
    attach_function :redlite_fts_enable_key, [:pointer, :string], :int

    # FTS.DISABLE GLOBAL
    attach_function :redlite_fts_disable_global, [:pointer], :int

    # FTS.DISABLE DATABASE db_num
    attach_function :redlite_fts_disable_database, [:pointer, :int], :int

    # FTS.DISABLE PATTERN pattern
    attach_function :redlite_fts_disable_pattern, [:pointer, :string], :int

    # FTS.DISABLE KEY key
    attach_function :redlite_fts_disable_key, [:pointer, :string], :int

    # Check if FTS is enabled for a key
    attach_function :redlite_is_fts_enabled, [:pointer, :string], :int

    # =========================================================================
    # KeyInfo Command
    # =========================================================================

    # KeyInfo struct
    class RedliteKeyInfo < ::FFI::Struct
      layout :key_type, :pointer,
             :ttl, :int64,
             :created_at, :int64,
             :updated_at, :int64,
             :valid, :int
    end

    # KEYINFO key
    attach_function :redlite_keyinfo, [:pointer, :string], RedliteKeyInfo.by_value

    # Free keyinfo struct
    attach_function :redlite_free_keyinfo, [RedliteKeyInfo.by_value], :void

    # =========================================================================
    # Server Commands
    # =========================================================================

    # VACUUM - compact the database
    attach_function :redlite_vacuum, [:pointer], :int64

    # Get library version (caller must free)
    attach_function :redlite_version, [], :pointer

    # =========================================================================
    # Helper Methods
    # =========================================================================

    class << self
      # Get version as a Ruby string
      def redlite_version_string
        ptr = redlite_version
        return nil if ptr.null?
        result = ptr.read_string
        redlite_free_string(ptr)
        result
      end

      # Get last error as a Ruby string
      def last_error_string
        ptr = redlite_last_error
        return nil if ptr.null?
        result = ptr.read_string
        redlite_free_string(ptr)
        result
      end
    end
  end
end
