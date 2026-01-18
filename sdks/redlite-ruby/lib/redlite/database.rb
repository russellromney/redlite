# frozen_string_literal: true

module Redlite
  # Main database class providing Redis-compatible operations
  class Database
    # @return [Boolean] Whether the database is open
    attr_reader :open

    # Open a database with optional block syntax
    #
    # @param path [String] Database path, or ":memory:" for in-memory
    # @param cache_mb [Integer] Cache size in megabytes (default: 64)
    # @yield [Database] The opened database
    # @return [Database, Object] Database instance or block return value
    def self.open(path = ":memory:", cache_mb: 64)
      db = new(path, cache_mb: cache_mb)
      return db unless block_given?

      begin
        yield db
      ensure
        db.close
      end
    end

    # Create a new database connection
    #
    # @param path [String] Database path, or ":memory:" for in-memory
    # @param cache_mb [Integer] Cache size in megabytes (default: 64)
    def initialize(path = ":memory:", cache_mb: 64)
      @handle = if path == ":memory:"
                  FFI.redlite_open_memory
                else
                  FFI.redlite_open_with_cache(path, cache_mb)
                end

      if @handle.null?
        error = FFI.last_error_string || "Failed to open database"
        raise Error, error
      end

      @open = true
    end

    # Close the database connection
    def close
      return unless @open

      FFI.redlite_close(@handle)
      @handle = nil
      @open = false
      nil
    end

    # Check if database is closed
    def closed?
      !@open
    end

    # =========================================================================
    # String Commands
    # =========================================================================

    # Get the value of a key
    #
    # @param key [String] The key
    # @return [String, nil] The value, or nil if key doesn't exist
    def get(key)
      check_open!
      bytes = FFI.redlite_get(@handle, key.to_s)
      result = bytes_to_string(bytes)
      FFI.redlite_free_bytes(bytes)
      result
    end

    # Set the value of a key
    #
    # @param key [String] The key
    # @param value [String] The value
    # @param ex [Integer, nil] Expiration time in seconds
    # @return [Boolean] true on success
    def set(key, value, ex: nil)
      check_open!
      value_bytes = value.to_s
      ttl = ex || 0
      result = FFI.redlite_set(@handle, key.to_s, value_bytes, value_bytes.bytesize, ttl)
      raise_if_error(result)
      true
    end

    # Set key with expiration in seconds
    #
    # @param key [String] The key
    # @param seconds [Integer] Expiration time in seconds
    # @param value [String] The value
    # @return [Boolean] true on success
    def setex(key, seconds, value)
      check_open!
      value_bytes = value.to_s
      result = FFI.redlite_setex(@handle, key.to_s, seconds.to_i, value_bytes, value_bytes.bytesize)
      raise_if_error(result)
      true
    end

    # Set key with expiration in milliseconds
    #
    # @param key [String] The key
    # @param milliseconds [Integer] Expiration time in milliseconds
    # @param value [String] The value
    # @return [Boolean] true on success
    def psetex(key, milliseconds, value)
      check_open!
      value_bytes = value.to_s
      result = FFI.redlite_psetex(@handle, key.to_s, milliseconds.to_i, value_bytes, value_bytes.bytesize)
      raise_if_error(result)
      true
    end

    # Get the value of a key and delete it
    #
    # @param key [String] The key
    # @return [String, nil] The value, or nil if key doesn't exist
    def getdel(key)
      check_open!
      bytes = FFI.redlite_getdel(@handle, key.to_s)
      result = bytes_to_string(bytes)
      FFI.redlite_free_bytes(bytes)
      result
    end

    # Append a value to a key
    #
    # @param key [String] The key
    # @param value [String] The value to append
    # @return [Integer] Length of the string after append
    def append(key, value)
      check_open!
      value_bytes = value.to_s
      FFI.redlite_append(@handle, key.to_s, value_bytes, value_bytes.bytesize)
    end

    # Get the length of a string value
    #
    # @param key [String] The key
    # @return [Integer] Length of the string, 0 if key doesn't exist
    def strlen(key)
      check_open!
      FFI.redlite_strlen(@handle, key.to_s)
    end

    # Get a substring of the string stored at a key
    #
    # @param key [String] The key
    # @param start_pos [Integer] Start index
    # @param end_pos [Integer] End index
    # @return [String] The substring
    def getrange(key, start_pos, end_pos)
      check_open!
      bytes = FFI.redlite_getrange(@handle, key.to_s, start_pos.to_i, end_pos.to_i)
      result = bytes_to_string(bytes) || ""
      FFI.redlite_free_bytes(bytes)
      result
    end

    # Overwrite part of a string at key starting at the specified offset
    #
    # @param key [String] The key
    # @param offset [Integer] The offset
    # @param value [String] The value
    # @return [Integer] Length of the string after modification
    def setrange(key, offset, value)
      check_open!
      value_bytes = value.to_s
      FFI.redlite_setrange(@handle, key.to_s, offset.to_i, value_bytes, value_bytes.bytesize)
    end

    # Increment the integer value of a key by one
    #
    # @param key [String] The key
    # @return [Integer] The value after increment
    # @raise [Error] If the value is not an integer or out of range
    def incr(key)
      check_open!
      result = FFI.redlite_incr(@handle, key.to_s)
      raise_if_i64_min_error(result)
      result
    end

    # Decrement the integer value of a key by one
    #
    # @param key [String] The key
    # @return [Integer] The value after decrement
    # @raise [Error] If the value is not an integer or out of range
    def decr(key)
      check_open!
      result = FFI.redlite_decr(@handle, key.to_s)
      raise_if_i64_min_error(result)
      result
    end

    # Increment the integer value of a key by the given amount
    #
    # @param key [String] The key
    # @param increment [Integer] The increment
    # @return [Integer] The value after increment
    # @raise [Error] If the value is not an integer or out of range
    def incrby(key, increment)
      check_open!
      result = FFI.redlite_incrby(@handle, key.to_s, increment.to_i)
      raise_if_i64_min_error(result)
      result
    end

    # Decrement the integer value of a key by the given amount
    #
    # @param key [String] The key
    # @param decrement [Integer] The decrement
    # @return [Integer] The value after decrement
    # @raise [Error] If the value is not an integer or out of range
    def decrby(key, decrement)
      check_open!
      result = FFI.redlite_decrby(@handle, key.to_s, decrement.to_i)
      raise_if_i64_min_error(result)
      result
    end

    # Increment the float value of a key by the given amount
    #
    # @param key [String] The key
    # @param increment [Float] The increment
    # @return [Float] The value after increment
    def incrbyfloat(key, increment)
      check_open!
      ptr = FFI.redlite_incrbyfloat(@handle, key.to_s, increment.to_f)
      if ptr.null?
        raise Error, FFI.last_error_string || "INCRBYFLOAT failed"
      end
      result = ptr.read_string.to_f
      FFI.redlite_free_string(ptr)
      result
    end

    # Get the values of all the given keys
    #
    # @param keys [Array<String>] The keys
    # @return [Array<String, nil>] Array of values (nil for missing keys)
    def mget(*keys)
      check_open!
      keys = keys.flatten
      return [] if keys.empty?

      keys_ptr = string_array_to_ptr(keys)
      arr = FFI.redlite_mget(@handle, keys_ptr, keys.length)
      result = bytes_array_to_strings(arr)
      FFI.redlite_free_bytes_array(arr)
      result
    end

    # Set multiple keys to multiple values
    #
    # @param mapping [Hash] Key-value pairs
    # @return [Boolean] true on success
    def mset(mapping)
      check_open!
      return true if mapping.empty?

      pairs = mapping.map do |k, v|
        kv = FFI::RedliteKV.new
        kv[:key] = ::FFI::MemoryPointer.from_string(k.to_s)
        value_bytes = v.to_s
        kv[:value] = ::FFI::MemoryPointer.from_string(value_bytes)
        kv[:value_len] = value_bytes.bytesize
        kv
      end

      # Pack structs into contiguous memory
      pairs_ptr = ::FFI::MemoryPointer.new(FFI::RedliteKV, pairs.length)
      pairs.each_with_index do |kv, i|
        pairs_ptr[i].put_bytes(0, kv.to_ptr.read_bytes(FFI::RedliteKV.size))
      end

      result = FFI.redlite_mset(@handle, pairs_ptr, pairs.length)
      raise_if_error(result)
      true
    end

    # Set a key's value only if the key does not exist
    #
    # @param key [String] The key
    # @param value [String] The value
    # @return [Boolean] true if set, false if key already exists
    def setnx(key, value)
      check_open!
      value_bytes = value.to_s
      result = FFI.redlite_setnx(@handle, key.to_s, value_bytes, value_bytes.bytesize)
      result == 1
    end

    # Get the value of a key and optionally set its expiration
    #
    # @param key [String] The key
    # @param ex [Integer, nil] Set expiration in seconds
    # @param px [Integer, nil] Set expiration in milliseconds
    # @param exat [Integer, nil] Set expiration as Unix timestamp in seconds
    # @param pxat [Integer, nil] Set expiration as Unix timestamp in milliseconds
    # @param persist [Boolean] Remove the TTL
    # @return [String, nil] The value, or nil if key doesn't exist
    def getex(key, ex: nil, px: nil, exat: nil, pxat: nil, persist: false)
      check_open!
      bytes = FFI.redlite_getex(@handle, key.to_s, ex || 0, px || 0, exat || 0, pxat || 0, persist ? 1 : 0)
      result = bytes_to_string(bytes)
      FFI.redlite_free_bytes(bytes)
      result
    end

    # Get a bit value at offset in the string value stored at key
    #
    # @param key [String] The key
    # @param offset [Integer] The bit offset
    # @return [Integer] 0 or 1
    def getbit(key, offset)
      check_open!
      FFI.redlite_getbit(@handle, key.to_s, offset.to_i)
    end

    # Set or clear a bit at offset in the string value stored at key
    #
    # @param key [String] The key
    # @param offset [Integer] The bit offset
    # @param value [Integer] 0 or 1
    # @return [Integer] The original bit value
    def setbit(key, offset, value)
      check_open!
      FFI.redlite_setbit(@handle, key.to_s, offset.to_i, value.to_i)
    end

    # Count the number of set bits in a string
    #
    # @param key [String] The key
    # @param start_pos [Integer, nil] Start byte position
    # @param end_pos [Integer, nil] End byte position
    # @return [Integer] Number of bits set to 1
    def bitcount(key, start_pos = nil, end_pos = nil)
      check_open!
      use_range = (start_pos && end_pos) ? 1 : 0
      FFI.redlite_bitcount(@handle, key.to_s, start_pos || 0, end_pos || 0, use_range)
    end

    # Perform a bitwise operation between strings
    #
    # @param operation [String] AND, OR, XOR, or NOT
    # @param destkey [String] The destination key
    # @param keys [Array<String>] The source keys
    # @return [Integer] The size of the resulting string
    def bitop(operation, destkey, *keys)
      check_open!
      keys = keys.flatten
      return 0 if keys.empty?

      keys_ptr = string_array_to_ptr(keys)
      FFI.redlite_bitop(@handle, operation.to_s.upcase, destkey.to_s, keys_ptr, keys.length)
    end

    # =========================================================================
    # Key Commands
    # =========================================================================

    # Delete one or more keys (alias: del)
    #
    # @param keys [Array<String>] The keys to delete
    # @return [Integer] The number of keys deleted
    def delete(*keys)
      check_open!
      keys = keys.flatten
      return 0 if keys.empty?

      keys_ptr = string_array_to_ptr(keys)
      FFI.redlite_del(@handle, keys_ptr, keys.length)
    end
    alias del delete

    # Check if one or more keys exist
    #
    # @param keys [Array<String>] The keys to check
    # @return [Integer] The number of keys that exist
    def exists(*keys)
      check_open!
      keys = keys.flatten
      return 0 if keys.empty?

      keys_ptr = string_array_to_ptr(keys)
      FFI.redlite_exists(@handle, keys_ptr, keys.length)
    end

    # Get the type of a key
    #
    # @param key [String] The key
    # @return [String] The type: "string", "list", "set", "zset", "hash", or "none"
    def type(key)
      check_open!
      ptr = FFI.redlite_type(@handle, key.to_s)
      return "none" if ptr.null?
      result = ptr.read_string
      FFI.redlite_free_string(ptr)
      result
    end

    # Get the time to live for a key in seconds
    #
    # @param key [String] The key
    # @return [Integer] TTL in seconds, -2 if key doesn't exist, -1 if no TTL
    def ttl(key)
      check_open!
      FFI.redlite_ttl(@handle, key.to_s)
    end

    # Get the time to live for a key in milliseconds
    #
    # @param key [String] The key
    # @return [Integer] TTL in milliseconds, -2 if key doesn't exist, -1 if no TTL
    def pttl(key)
      check_open!
      FFI.redlite_pttl(@handle, key.to_s)
    end

    # Set a key's time to live in seconds
    #
    # @param key [String] The key
    # @param seconds [Integer] Expiration time in seconds
    # @return [Boolean] true if timeout was set, false if key doesn't exist
    def expire(key, seconds)
      check_open!
      result = FFI.redlite_expire(@handle, key.to_s, seconds.to_i)
      result == 1
    end

    # Set a key's time to live in milliseconds
    #
    # @param key [String] The key
    # @param milliseconds [Integer] Expiration time in milliseconds
    # @return [Boolean] true if timeout was set, false if key doesn't exist
    def pexpire(key, milliseconds)
      check_open!
      result = FFI.redlite_pexpire(@handle, key.to_s, milliseconds.to_i)
      result == 1
    end

    # Set the expiration for a key as a UNIX timestamp
    #
    # @param key [String] The key
    # @param unix_time [Integer] UNIX timestamp in seconds
    # @return [Boolean] true if timeout was set, false if key doesn't exist
    def expireat(key, unix_time)
      check_open!
      result = FFI.redlite_expireat(@handle, key.to_s, unix_time.to_i)
      result == 1
    end

    # Set the expiration for a key as a UNIX timestamp in milliseconds
    #
    # @param key [String] The key
    # @param unix_time_ms [Integer] UNIX timestamp in milliseconds
    # @return [Boolean] true if timeout was set, false if key doesn't exist
    def pexpireat(key, unix_time_ms)
      check_open!
      result = FFI.redlite_pexpireat(@handle, key.to_s, unix_time_ms.to_i)
      result == 1
    end

    # Remove the expiration from a key
    #
    # @param key [String] The key
    # @return [Boolean] true if timeout was removed, false if key doesn't exist or has no TTL
    def persist(key)
      check_open!
      result = FFI.redlite_persist(@handle, key.to_s)
      result == 1
    end

    # Rename a key
    #
    # @param key [String] The old key name
    # @param newkey [String] The new key name
    # @return [Boolean] true on success
    def rename(key, newkey)
      check_open!
      result = FFI.redlite_rename(@handle, key.to_s, newkey.to_s)
      raise_if_error(result)
      true
    end

    # Rename a key only if the new key does not exist
    #
    # @param key [String] The old key name
    # @param newkey [String] The new key name
    # @return [Boolean] true if renamed, false if newkey already exists
    def renamenx(key, newkey)
      check_open!
      result = FFI.redlite_renamenx(@handle, key.to_s, newkey.to_s)
      result == 1
    end

    # Find all keys matching the given pattern
    #
    # @param pattern [String] The pattern (supports * and ?)
    # @return [Array<String>] Matching keys
    def keys(pattern = "*")
      check_open!
      arr = FFI.redlite_keys(@handle, pattern.to_s)
      result = string_array_to_strings(arr)
      FFI.redlite_free_string_array(arr)
      result
    end

    # Return the number of keys in the database
    #
    # @return [Integer] Number of keys
    def dbsize
      check_open!
      FFI.redlite_dbsize(@handle)
    end

    # Delete all keys in the current database
    #
    # @return [Boolean] true on success
    def flushdb
      check_open!
      result = FFI.redlite_flushdb(@handle)
      raise_if_error(result)
      true
    end

    # Select a different database
    #
    # @param db_num [Integer] Database number
    # @return [Boolean] true on success
    def select(db_num)
      check_open!
      result = FFI.redlite_select(@handle, db_num.to_i)
      raise_if_error(result)
      true
    end

    # =========================================================================
    # Hash Commands
    # =========================================================================

    # Set fields in a hash
    #
    # @param key [String] The hash key
    # @param mapping [Hash] Field-value pairs
    # @return [Integer] Number of fields added (not updated)
    def hset(key, mapping)
      check_open!
      return 0 if mapping.empty?

      fields = mapping.keys.map(&:to_s)
      values = mapping.values.map(&:to_s)

      fields_ptr = string_array_to_ptr(fields)

      # Create array of RedliteBytes for values
      values_ptr = ::FFI::MemoryPointer.new(FFI::RedliteBytes, values.length)
      values.each_with_index do |v, i|
        bytes = FFI::RedliteBytes.new(values_ptr[i])
        bytes[:data] = ::FFI::MemoryPointer.from_string(v)
        bytes[:len] = v.bytesize
      end

      FFI.redlite_hset(@handle, key.to_s, fields_ptr, values_ptr, mapping.size)
    end

    # Get the value of a hash field
    #
    # @param key [String] The hash key
    # @param field [String] The field name
    # @return [String, nil] The value, or nil if not found
    def hget(key, field)
      check_open!
      bytes = FFI.redlite_hget(@handle, key.to_s, field.to_s)
      result = bytes_to_string(bytes)
      FFI.redlite_free_bytes(bytes)
      result
    end

    # Delete one or more hash fields
    #
    # @param key [String] The hash key
    # @param fields [Array<String>] The fields to delete
    # @return [Integer] Number of fields deleted
    def hdel(key, *fields)
      check_open!
      fields = fields.flatten
      return 0 if fields.empty?

      fields_ptr = string_array_to_ptr(fields)
      FFI.redlite_hdel(@handle, key.to_s, fields_ptr, fields.length)
    end

    # Check if a hash field exists
    #
    # @param key [String] The hash key
    # @param field [String] The field name
    # @return [Boolean] true if field exists
    def hexists(key, field)
      check_open!
      result = FFI.redlite_hexists(@handle, key.to_s, field.to_s)
      result == 1
    end

    # Get the number of fields in a hash
    #
    # @param key [String] The hash key
    # @return [Integer] Number of fields
    def hlen(key)
      check_open!
      FFI.redlite_hlen(@handle, key.to_s)
    end

    # Get all field names in a hash
    #
    # @param key [String] The hash key
    # @return [Array<String>] Field names
    def hkeys(key)
      check_open!
      arr = FFI.redlite_hkeys(@handle, key.to_s)
      result = string_array_to_strings(arr)
      FFI.redlite_free_string_array(arr)
      result
    end

    # Get all values in a hash
    #
    # @param key [String] The hash key
    # @return [Array<String>] Field values
    def hvals(key)
      check_open!
      arr = FFI.redlite_hvals(@handle, key.to_s)
      result = bytes_array_to_strings(arr)
      FFI.redlite_free_bytes_array(arr)
      result
    end

    # Increment the integer value of a hash field
    #
    # @param key [String] The hash key
    # @param field [String] The field name
    # @param increment [Integer] The increment
    # @return [Integer] The value after increment
    # @raise [Error] If the value is not an integer or out of range
    def hincrby(key, field, increment)
      check_open!
      result = FFI.redlite_hincrby(@handle, key.to_s, field.to_s, increment.to_i)
      raise_if_i64_min_error(result)
      result
    end

    # Get all fields and values in a hash
    #
    # @param key [String] The hash key
    # @return [Hash] Field-value pairs
    def hgetall(key)
      check_open!
      arr = FFI.redlite_hgetall(@handle, key.to_s)
      items = bytes_array_to_strings(arr)
      FFI.redlite_free_bytes_array(arr)

      # Convert alternating array to hash
      result = {}
      items.each_slice(2) do |field, value|
        result[field] = value if field
      end
      result
    end

    # Get the values of multiple hash fields
    #
    # @param key [String] The hash key
    # @param fields [Array<String>] The field names
    # @return [Array<String, nil>] Field values (nil for missing fields)
    def hmget(key, *fields)
      check_open!
      fields = fields.flatten
      return [] if fields.empty?

      fields_ptr = string_array_to_ptr(fields)
      arr = FFI.redlite_hmget(@handle, key.to_s, fields_ptr, fields.length)
      result = bytes_array_to_strings(arr)
      FFI.redlite_free_bytes_array(arr)
      result
    end

    # Set a hash field only if it does not exist
    #
    # @param key [String] The hash key
    # @param field [String] The field name
    # @param value [String] The value
    # @return [Boolean] true if set, false if field already exists
    def hsetnx(key, field, value)
      check_open!
      value_bytes = value.to_s
      result = FFI.redlite_hsetnx(@handle, key.to_s, field.to_s, value_bytes, value_bytes.bytesize)
      result == 1
    end

    # Increment the float value of a hash field
    #
    # @param key [String] The hash key
    # @param field [String] The field name
    # @param increment [Float] The increment
    # @return [Float] The value after increment
    def hincrbyfloat(key, field, increment)
      check_open!
      ptr = FFI.redlite_hincrbyfloat(@handle, key.to_s, field.to_s, increment.to_f)
      if ptr.null?
        raise Error, FFI.last_error_string || "HINCRBYFLOAT failed"
      end
      result = ptr.read_string.to_f
      FFI.redlite_free_string(ptr)
      result
    end

    # =========================================================================
    # List Commands
    # =========================================================================

    # Prepend values to a list
    #
    # @param key [String] The list key
    # @param values [Array<String>] Values to prepend
    # @return [Integer] Length of list after operation
    def lpush(key, *values)
      check_open!
      values = values.flatten
      return 0 if values.empty?

      bytes_arr = values_to_bytes_array(values)
      FFI.redlite_lpush(@handle, key.to_s, bytes_arr, values.length)
    end

    # Append values to a list
    #
    # @param key [String] The list key
    # @param values [Array<String>] Values to append
    # @return [Integer] Length of list after operation
    def rpush(key, *values)
      check_open!
      values = values.flatten
      return 0 if values.empty?

      bytes_arr = values_to_bytes_array(values)
      FFI.redlite_rpush(@handle, key.to_s, bytes_arr, values.length)
    end

    # Remove and get the first element(s) of a list
    #
    # @param key [String] The list key
    # @param count [Integer] Number of elements to pop (default: 1)
    # @return [String, Array<String>, nil] The popped element(s), or nil if list is empty
    def lpop(key, count = nil)
      check_open!
      pop_count = count || 1
      arr = FFI.redlite_lpop(@handle, key.to_s, pop_count)
      result = bytes_array_to_strings(arr)
      FFI.redlite_free_bytes_array(arr)

      return nil if result.empty?
      count.nil? ? result.first : result
    end

    # Remove and get the last element(s) of a list
    #
    # @param key [String] The list key
    # @param count [Integer] Number of elements to pop (default: 1)
    # @return [String, Array<String>, nil] The popped element(s), or nil if list is empty
    def rpop(key, count = nil)
      check_open!
      pop_count = count || 1
      arr = FFI.redlite_rpop(@handle, key.to_s, pop_count)
      result = bytes_array_to_strings(arr)
      FFI.redlite_free_bytes_array(arr)

      return nil if result.empty?
      count.nil? ? result.first : result
    end

    # Get the length of a list
    #
    # @param key [String] The list key
    # @return [Integer] Length of the list
    def llen(key)
      check_open!
      FFI.redlite_llen(@handle, key.to_s)
    end

    # Get a range of elements from a list
    #
    # @param key [String] The list key
    # @param start [Integer] Start index
    # @param stop [Integer] Stop index
    # @return [Array<String>] Elements in range
    def lrange(key, start, stop)
      check_open!
      arr = FFI.redlite_lrange(@handle, key.to_s, start.to_i, stop.to_i)
      result = bytes_array_to_strings(arr)
      FFI.redlite_free_bytes_array(arr)
      result
    end

    # Get an element from a list by its index
    #
    # @param key [String] The list key
    # @param index [Integer] The index
    # @return [String, nil] The element, or nil if out of range
    def lindex(key, index)
      check_open!
      bytes = FFI.redlite_lindex(@handle, key.to_s, index.to_i)
      result = bytes_to_string(bytes)
      FFI.redlite_free_bytes(bytes)
      result
    end

    # Push value(s) to the head of a list only if it exists.
    # Returns the length after the push, or 0 if the list doesn't exist.
    #
    # @param key [String] The key
    # @param values [Array<String>, String] The value(s) to push
    # @return [Integer] The new list length, or 0 if list doesn't exist
    def lpushx(key, *values)
      check_open!
      values = values.flatten.map(&:to_s)
      return 0 if values.empty?

      values_ptr = values_to_bytes_array(values)
      result = FFI.redlite_lpushx(@handle, key.to_s, values_ptr, values.length)
      check_error! if result < 0
      result
    end

    # Push value(s) to the tail of a list only if it exists.
    # Returns the length after the push, or 0 if the list doesn't exist.
    #
    # @param key [String] The key
    # @param values [Array<String>, String] The value(s) to push
    # @return [Integer] The new list length, or 0 if list doesn't exist
    def rpushx(key, *values)
      check_open!
      values = values.flatten.map(&:to_s)
      return 0 if values.empty?

      values_ptr = values_to_bytes_array(values)
      result = FFI.redlite_rpushx(@handle, key.to_s, values_ptr, values.length)
      check_error! if result < 0
      result
    end

    # Atomically move an element from one list to another.
    #
    # @param source [String] The source list key
    # @param destination [String] The destination list key
    # @param wherefrom [:left, :right] Where to pop from source (default :left)
    # @param whereto [:left, :right] Where to push to destination (default :left)
    # @return [String, nil] The moved element, or nil if source is empty
    def lmove(source, destination, wherefrom: :left, whereto: :left)
      check_open!
      from_int = wherefrom == :left ? 0 : 1
      to_int = whereto == :left ? 0 : 1
      bytes = FFI.redlite_lmove(@handle, source.to_s, destination.to_s, from_int, to_int)
      result = bytes_to_string(bytes)
      FFI.redlite_free_bytes(bytes)
      result
    end

    # Find the position(s) of an element in a list.
    #
    # @param key [String] The key
    # @param element [String] The element to search for
    # @param rank [Integer, nil] Search direction (positive=left-to-right, negative=right-to-left)
    # @param count [Integer, nil] Number of matches to return (nil for first match only)
    # @param maxlen [Integer, nil] Max elements to scan (nil for all)
    # @return [Integer, Array<Integer>, nil] Position(s) of the element
    def lpos(key, element, rank: nil, count: nil, maxlen: nil)
      check_open!
      element_str = element.to_s
      element_ptr = ::FFI::MemoryPointer.from_string(element_str)

      rank_val = rank || 0
      count_val = count || 0
      maxlen_val = maxlen || 0

      result = FFI.redlite_lpos(@handle, key.to_s, element_ptr, element_str.bytesize, rank_val, count_val, maxlen_val)

      if result[:items].null? || result[:len] == 0
        FFI.redlite_free_bytes_array(result)
        return nil
      end

      # Parse positions from bytes array (each position is 8-byte little-endian i64)
      positions = []
      result[:len].times do |i|
        item_ptr = result[:items] + (i * FFI::RedliteBytes.size)
        item = FFI::RedliteBytes.new(item_ptr)
        if !item[:data].null? && item[:len] == 8
          positions << item[:data].read_string(8).unpack1("q<")
        end
      end
      FFI.redlite_free_bytes_array(result)

      # Return single value if count was nil or 1, otherwise return array
      if count.nil? || count == 1
        positions.first
      else
        positions
      end
    end

    # Set the value of an element in a list by its index
    #
    # @param key [String] The list key
    # @param index [Integer] The index
    # @param value [String] The value
    # @return [Boolean] true on success
    def lset(key, index, value)
      check_open!
      value_bytes = value.to_s
      result = FFI.redlite_lset(@handle, key.to_s, index.to_i, value_bytes, value_bytes.bytesize)
      raise_if_error(result)
      true
    end

    # Trim a list to the specified range
    #
    # @param key [String] The list key
    # @param start [Integer] Start index
    # @param stop [Integer] Stop index
    # @return [Boolean] true on success
    def ltrim(key, start, stop)
      check_open!
      result = FFI.redlite_ltrim(@handle, key.to_s, start.to_i, stop.to_i)
      raise_if_error(result)
      true
    end

    # Remove elements from a list
    #
    # @param key [String] The list key
    # @param count [Integer] Number of elements to remove (0 = all, positive = from head, negative = from tail)
    # @param element [String] The element to remove
    # @return [Integer] Number of elements removed
    def lrem(key, count, element)
      check_open!
      element_bytes = element.to_s
      FFI.redlite_lrem(@handle, key.to_s, count.to_i, element_bytes, element_bytes.bytesize)
    end

    # Insert an element before or after another element in a list
    #
    # @param key [String] The list key
    # @param where [:before, :after] Insert position
    # @param pivot [String] The pivot element
    # @param element [String] The element to insert
    # @return [Integer] The list length after insert, or -1 if pivot not found
    def linsert(key, where, pivot, element)
      check_open!
      before = where.to_sym == :before ? 1 : 0
      pivot_bytes = pivot.to_s
      element_bytes = element.to_s
      FFI.redlite_linsert(@handle, key.to_s, before, pivot_bytes, pivot_bytes.bytesize, element_bytes, element_bytes.bytesize)
    end

    # =========================================================================
    # Set Commands
    # =========================================================================

    # Add members to a set
    #
    # @param key [String] The set key
    # @param members [Array<String>] Members to add
    # @return [Integer] Number of elements added (not already present)
    def sadd(key, *members)
      check_open!
      members = members.flatten
      return 0 if members.empty?

      bytes_arr = values_to_bytes_array(members)
      FFI.redlite_sadd(@handle, key.to_s, bytes_arr, members.length)
    end

    # Remove members from a set
    #
    # @param key [String] The set key
    # @param members [Array<String>] Members to remove
    # @return [Integer] Number of elements removed
    def srem(key, *members)
      check_open!
      members = members.flatten
      return 0 if members.empty?

      bytes_arr = values_to_bytes_array(members)
      FFI.redlite_srem(@handle, key.to_s, bytes_arr, members.length)
    end

    # Get all members of a set
    #
    # @param key [String] The set key
    # @return [Array<String>] Set members
    def smembers(key)
      check_open!
      arr = FFI.redlite_smembers(@handle, key.to_s)
      result = bytes_array_to_strings(arr)
      FFI.redlite_free_bytes_array(arr)
      result
    end

    # Check if a value is a member of a set
    #
    # @param key [String] The set key
    # @param member [String] The member to check
    # @return [Boolean] true if member exists
    def sismember(key, member)
      check_open!
      member_bytes = member.to_s
      result = FFI.redlite_sismember(@handle, key.to_s, member_bytes, member_bytes.bytesize)
      result == 1
    end

    # Get the number of members in a set
    #
    # @param key [String] The set key
    # @return [Integer] Number of members
    def scard(key)
      check_open!
      FFI.redlite_scard(@handle, key.to_s)
    end

    # Remove and return random member(s) from a set
    #
    # @param key [String] The set key
    # @param count [Integer, nil] Number of members to pop (default: 1)
    # @return [String, Array<String>, nil] The popped member(s), or nil if set is empty
    def spop(key, count = nil)
      check_open!
      pop_count = count || 1
      arr = FFI.redlite_spop(@handle, key.to_s, pop_count)
      result = bytes_array_to_strings(arr)
      FFI.redlite_free_bytes_array(arr)

      return nil if result.empty?
      count.nil? ? result.first : result
    end

    # Get random member(s) from a set without removing
    #
    # @param key [String] The set key
    # @param count [Integer, nil] Number of members to return (default: 1)
    # @return [String, Array<String>, nil] The random member(s), or nil if set is empty
    def srandmember(key, count = nil)
      check_open!
      member_count = count || 1
      arr = FFI.redlite_srandmember(@handle, key.to_s, member_count)
      result = bytes_array_to_strings(arr)
      FFI.redlite_free_bytes_array(arr)

      return nil if result.empty?
      count.nil? ? result.first : result
    end

    # Get the difference between the first set and all successive sets
    #
    # @param keys [Array<String>] The set keys
    # @return [Array<String>] Members in the difference
    def sdiff(*keys)
      check_open!
      keys = keys.flatten
      return [] if keys.empty?

      keys_ptr = string_array_to_ptr(keys)
      arr = FFI.redlite_sdiff(@handle, keys_ptr, keys.length)
      result = bytes_array_to_strings(arr)
      FFI.redlite_free_bytes_array(arr)
      result
    end

    # Get the intersection of all the given sets
    #
    # @param keys [Array<String>] The set keys
    # @return [Array<String>] Members in the intersection
    def sinter(*keys)
      check_open!
      keys = keys.flatten
      return [] if keys.empty?

      keys_ptr = string_array_to_ptr(keys)
      arr = FFI.redlite_sinter(@handle, keys_ptr, keys.length)
      result = bytes_array_to_strings(arr)
      FFI.redlite_free_bytes_array(arr)
      result
    end

    # Get the union of all the given sets
    #
    # @param keys [Array<String>] The set keys
    # @return [Array<String>] Members in the union
    def sunion(*keys)
      check_open!
      keys = keys.flatten
      return [] if keys.empty?

      keys_ptr = string_array_to_ptr(keys)
      arr = FFI.redlite_sunion(@handle, keys_ptr, keys.length)
      result = bytes_array_to_strings(arr)
      FFI.redlite_free_bytes_array(arr)
      result
    end

    # Store the difference of sets in a new set
    #
    # @param destination [String] The destination key
    # @param keys [Array<String>] The source set keys
    # @return [Integer] Number of elements in the resulting set
    def sdiffstore(destination, *keys)
      check_open!
      keys = keys.flatten
      return 0 if keys.empty?

      keys_ptr = string_array_to_ptr(keys)
      FFI.redlite_sdiffstore(@handle, destination.to_s, keys_ptr, keys.length)
    end

    # Store the intersection of sets in a new set
    #
    # @param destination [String] The destination key
    # @param keys [Array<String>] The source set keys
    # @return [Integer] Number of elements in the resulting set
    def sinterstore(destination, *keys)
      check_open!
      keys = keys.flatten
      return 0 if keys.empty?

      keys_ptr = string_array_to_ptr(keys)
      FFI.redlite_sinterstore(@handle, destination.to_s, keys_ptr, keys.length)
    end

    # Store the union of sets in a new set
    #
    # @param destination [String] The destination key
    # @param keys [Array<String>] The source set keys
    # @return [Integer] Number of elements in the resulting set
    def sunionstore(destination, *keys)
      check_open!
      keys = keys.flatten
      return 0 if keys.empty?

      keys_ptr = string_array_to_ptr(keys)
      FFI.redlite_sunionstore(@handle, destination.to_s, keys_ptr, keys.length)
    end

    # Move a member from one set to another
    #
    # @param source [String] The source set key
    # @param destination [String] The destination set key
    # @param member [String] The member to move
    # @return [Boolean] true if moved, false if member not found in source
    def smove(source, destination, member)
      check_open!
      member_bytes = member.to_s
      result = FFI.redlite_smove(@handle, source.to_s, destination.to_s, member_bytes, member_bytes.bytesize)
      result == 1
    end

    # =========================================================================
    # Sorted Set Commands
    # =========================================================================

    # Add members to a sorted set
    #
    # @param key [String] The sorted set key
    # @param mapping [Hash] Member-score pairs (member => score)
    # @return [Integer] Number of elements added (not updated)
    def zadd(key, mapping)
      check_open!
      return 0 if mapping.empty?

      # Create array of RedliteZMember
      members_ptr = ::FFI::MemoryPointer.new(FFI::RedliteZMember, mapping.size)
      mapping.each_with_index do |(member, score), i|
        zm = FFI::RedliteZMember.new(members_ptr[i])
        zm[:score] = score.to_f
        member_bytes = member.to_s
        zm[:member] = ::FFI::MemoryPointer.from_string(member_bytes)
        zm[:member_len] = member_bytes.bytesize
      end

      FFI.redlite_zadd(@handle, key.to_s, members_ptr, mapping.size)
    end

    # Remove members from a sorted set
    #
    # @param key [String] The sorted set key
    # @param members [Array<String>] Members to remove
    # @return [Integer] Number of elements removed
    def zrem(key, *members)
      check_open!
      members = members.flatten
      return 0 if members.empty?

      bytes_arr = values_to_bytes_array(members)
      FFI.redlite_zrem(@handle, key.to_s, bytes_arr, members.length)
    end

    # Get the score of a member in a sorted set
    #
    # @param key [String] The sorted set key
    # @param member [String] The member
    # @return [Float, nil] The score, or nil if member doesn't exist
    def zscore(key, member)
      check_open!
      member_bytes = member.to_s
      result = FFI.redlite_zscore(@handle, key.to_s, member_bytes, member_bytes.bytesize)
      result.nan? ? nil : result
    end

    # Get the number of members in a sorted set
    #
    # @param key [String] The sorted set key
    # @return [Integer] Number of members
    def zcard(key)
      check_open!
      FFI.redlite_zcard(@handle, key.to_s)
    end

    # Count members in a sorted set within a score range
    #
    # @param key [String] The sorted set key
    # @param min [Float] Minimum score
    # @param max [Float] Maximum score
    # @return [Integer] Number of members in range
    def zcount(key, min, max)
      check_open!
      FFI.redlite_zcount(@handle, key.to_s, min.to_f, max.to_f)
    end

    # Increment the score of a member in a sorted set
    #
    # @param key [String] The sorted set key
    # @param increment [Float] The increment
    # @param member [String] The member
    # @return [Float] The new score
    def zincrby(key, increment, member)
      check_open!
      member_bytes = member.to_s
      FFI.redlite_zincrby(@handle, key.to_s, increment.to_f, member_bytes, member_bytes.bytesize)
    end

    # Get a range of members from a sorted set by index
    #
    # @param key [String] The sorted set key
    # @param start [Integer] Start index
    # @param stop [Integer] Stop index
    # @param with_scores [Boolean] Include scores in result
    # @return [Array<String>, Array<Array>] Members or [member, score] pairs
    def zrange(key, start, stop, with_scores: false)
      check_open!
      arr = FFI.redlite_zrange(@handle, key.to_s, start.to_i, stop.to_i, with_scores ? 1 : 0)
      items = bytes_array_to_strings(arr)
      FFI.redlite_free_bytes_array(arr)

      if with_scores
        # Convert alternating member-score array to [[member, score], ...]
        items.each_slice(2).map { |member, score| [member, score.to_f] }
      else
        items
      end
    end

    # Get a range of members from a sorted set by index, in reverse order
    #
    # @param key [String] The sorted set key
    # @param start [Integer] Start index
    # @param stop [Integer] Stop index
    # @param with_scores [Boolean] Include scores in result
    # @return [Array<String>, Array<Array>] Members or [member, score] pairs
    def zrevrange(key, start, stop, with_scores: false)
      check_open!
      arr = FFI.redlite_zrevrange(@handle, key.to_s, start.to_i, stop.to_i, with_scores ? 1 : 0)
      items = bytes_array_to_strings(arr)
      FFI.redlite_free_bytes_array(arr)

      if with_scores
        items.each_slice(2).map { |member, score| [member, score.to_f] }
      else
        items
      end
    end

    # Get the rank of a member in a sorted set (0-indexed, lowest score first)
    #
    # @param key [String] The sorted set key
    # @param member [String] The member
    # @return [Integer, nil] The rank, or nil if member doesn't exist
    def zrank(key, member)
      check_open!
      member_bytes = member.to_s
      result = FFI.redlite_zrank(@handle, key.to_s, member_bytes, member_bytes.bytesize)
      result < 0 ? nil : result
    end

    # Get the rank of a member in a sorted set (0-indexed, highest score first)
    #
    # @param key [String] The sorted set key
    # @param member [String] The member
    # @return [Integer, nil] The rank, or nil if member doesn't exist
    def zrevrank(key, member)
      check_open!
      member_bytes = member.to_s
      result = FFI.redlite_zrevrank(@handle, key.to_s, member_bytes, member_bytes.bytesize)
      result < 0 ? nil : result
    end

    # Get members in a sorted set within a score range
    #
    # @param key [String] The sorted set key
    # @param min [Float] Minimum score
    # @param max [Float] Maximum score
    # @param offset [Integer] Number of elements to skip
    # @param count [Integer] Maximum number of elements to return (0 for unlimited)
    # @return [Array<String>] Members in the range
    def zrangebyscore(key, min, max, offset: 0, count: 0)
      check_open!
      arr = FFI.redlite_zrangebyscore(@handle, key.to_s, min.to_f, max.to_f, offset.to_i, count.to_i)
      result = bytes_array_to_strings(arr)
      FFI.redlite_free_bytes_array(arr)
      result
    end

    # Remove members in a sorted set within a rank range
    #
    # @param key [String] The sorted set key
    # @param start [Integer] Start rank
    # @param stop [Integer] Stop rank
    # @return [Integer] Number of elements removed
    def zremrangebyrank(key, start, stop)
      check_open!
      FFI.redlite_zremrangebyrank(@handle, key.to_s, start.to_i, stop.to_i)
    end

    # Remove members in a sorted set within a score range
    #
    # @param key [String] The sorted set key
    # @param min [Float] Minimum score
    # @param max [Float] Maximum score
    # @return [Integer] Number of elements removed
    def zremrangebyscore(key, min, max)
      check_open!
      FFI.redlite_zremrangebyscore(@handle, key.to_s, min.to_f, max.to_f)
    end

    # Compute the intersection of sorted sets and store the result.
    #
    # @param destination [String] The destination key
    # @param keys [Array<String>] Source set keys
    # @param weights [Array<Float>, nil] Weight multipliers for each set (default: 1.0)
    # @param aggregate [String, nil] Aggregation function: "SUM", "MIN", or "MAX" (default: "SUM")
    # @return [Integer] Number of elements in the resulting set
    def zinterstore(destination, keys, weights: nil, aggregate: nil)
      check_open!
      keys = Array(keys)
      return 0 if keys.empty?

      keys_ptr = strings_to_pointer_array(keys)

      weights_ptr = nil
      weights_len = 0
      if weights && !weights.empty?
        weights_ptr = ::FFI::MemoryPointer.new(:double, weights.length)
        weights.each_with_index { |w, i| weights_ptr.put_double(i * 8, w.to_f) }
        weights_len = weights.length
      end

      result = FFI.redlite_zinterstore(@handle, destination.to_s, keys_ptr, keys.length, weights_ptr, weights_len, aggregate)
      check_error! if result < 0
      result
    end

    # Compute the union of sorted sets and store the result.
    #
    # @param destination [String] The destination key
    # @param keys [Array<String>] Source set keys
    # @param weights [Array<Float>, nil] Weight multipliers for each set (default: 1.0)
    # @param aggregate [String, nil] Aggregation function: "SUM", "MIN", or "MAX" (default: "SUM")
    # @return [Integer] Number of elements in the resulting set
    def zunionstore(destination, keys, weights: nil, aggregate: nil)
      check_open!
      keys = Array(keys)
      return 0 if keys.empty?

      keys_ptr = strings_to_pointer_array(keys)

      weights_ptr = nil
      weights_len = 0
      if weights && !weights.empty?
        weights_ptr = ::FFI::MemoryPointer.new(:double, weights.length)
        weights.each_with_index { |w, i| weights_ptr.put_double(i * 8, w.to_f) }
        weights_len = weights.length
      end

      result = FFI.redlite_zunionstore(@handle, destination.to_s, keys_ptr, keys.length, weights_ptr, weights_len, aggregate)
      check_error! if result < 0
      result
    end

    # =========================================================================
    # JSON Commands
    # =========================================================================

    # Set a JSON value at path
    #
    # @param key [String] The key
    # @param path [String] JSON path (e.g., "$", "$.name")
    # @param value [String] JSON value string
    # @param nx [Boolean] Only set if path does not exist
    # @param xx [Boolean] Only set if path exists
    # @return [Boolean] true on success
    def json_set(key, path, value, nx: false, xx: false)
      check_open!
      result = FFI.redlite_json_set(@handle, key.to_s, path.to_s, value.to_s, nx ? 1 : 0, xx ? 1 : 0)
      raise_if_error(result)
      result == 1
    end

    # Get a JSON value at path
    #
    # @param key [String] The key
    # @param paths [Array<String>] JSON paths (default: ["$"])
    # @return [String, nil] JSON string, or nil if key doesn't exist
    def json_get(key, *paths)
      check_open!
      paths = ["$"] if paths.empty?
      paths_ptr = string_array_to_ptr(paths)
      ptr = FFI.redlite_json_get(@handle, key.to_s, paths_ptr, paths.length)
      return nil if ptr.null?
      result = ptr.read_string
      FFI.redlite_free_string(ptr)
      result
    end

    # Delete a JSON value at path
    #
    # @param key [String] The key
    # @param path [String, nil] JSON path (default: "$")
    # @return [Integer] Number of values deleted
    def json_del(key, path = nil)
      check_open!
      FFI.redlite_json_del(@handle, key.to_s, path&.to_s)
    end

    # Get the type of JSON value at path
    #
    # @param key [String] The key
    # @param path [String, nil] JSON path (default: "$")
    # @return [String, nil] Type string (object, array, string, integer, number, boolean, null)
    def json_type(key, path = nil)
      check_open!
      ptr = FFI.redlite_json_type(@handle, key.to_s, path&.to_s)
      return nil if ptr.null?
      result = ptr.read_string
      FFI.redlite_free_string(ptr)
      result
    end

    # Increment a numeric JSON value at path
    #
    # @param key [String] The key
    # @param path [String] JSON path
    # @param increment [Float] The increment
    # @return [String] New value as string
    def json_numincrby(key, path, increment)
      check_open!
      ptr = FFI.redlite_json_numincrby(@handle, key.to_s, path.to_s, increment.to_f)
      if ptr.null?
        raise Error, FFI.last_error_string || "JSON.NUMINCRBY failed"
      end
      result = ptr.read_string
      FFI.redlite_free_string(ptr)
      result
    end

    # Append a string to a JSON string at path
    #
    # @param key [String] The key
    # @param value [String] String to append (must be JSON-encoded, e.g., '"text"')
    # @param path [String, nil] JSON path (default: "$")
    # @return [Integer] New string length
    def json_strappend(key, value, path = nil)
      check_open!
      FFI.redlite_json_strappend(@handle, key.to_s, path&.to_s, value.to_s)
    end

    # Get the length of a JSON string at path
    #
    # @param key [String] The key
    # @param path [String, nil] JSON path (default: "$")
    # @return [Integer, nil] String length
    def json_strlen(key, path = nil)
      check_open!
      result = FFI.redlite_json_strlen(@handle, key.to_s, path&.to_s)
      result < 0 ? nil : result
    end

    # Append values to a JSON array at path
    #
    # @param key [String] The key
    # @param path [String] JSON path
    # @param values [Array<String>] JSON values to append
    # @return [Integer] New array length
    def json_arrappend(key, path, *values)
      check_open!
      values = values.flatten
      return 0 if values.empty?
      values_ptr = string_array_to_ptr(values.map(&:to_s))
      FFI.redlite_json_arrappend(@handle, key.to_s, path.to_s, values_ptr, values.length)
    end

    # Get the length of a JSON array at path
    #
    # @param key [String] The key
    # @param path [String, nil] JSON path (default: "$")
    # @return [Integer, nil] Array length
    def json_arrlen(key, path = nil)
      check_open!
      result = FFI.redlite_json_arrlen(@handle, key.to_s, path&.to_s)
      result < 0 ? nil : result
    end

    # Pop an element from a JSON array at path
    #
    # @param key [String] The key
    # @param path [String, nil] JSON path
    # @param index [Integer, nil] Index to pop (default: -1, last element)
    # @return [String, nil] Popped element as JSON string
    def json_arrpop(key, path = nil, index = -1)
      check_open!
      ptr = FFI.redlite_json_arrpop(@handle, key.to_s, path&.to_s, index.to_i)
      return nil if ptr.null?
      result = ptr.read_string
      FFI.redlite_free_string(ptr)
      result
    end

    # Clear container values (arrays become [], objects become {})
    #
    # @param key [String] The key
    # @param path [String, nil] JSON path (default: "$")
    # @return [Integer] Number of values cleared
    def json_clear(key, path = nil)
      check_open!
      FFI.redlite_json_clear(@handle, key.to_s, path&.to_s)
    end

    # =========================================================================
    # History Enable/Disable Commands
    # =========================================================================

    # Enable history tracking globally
    #
    # @param retention [String] Retention type: "unlimited", "time:<ms>", or "count:<n>"
    # @return [Boolean] true on success
    def history_enable_global(retention = "unlimited")
      check_open!
      ret_type, ret_value = parse_retention(retention)
      result = FFI.redlite_history_enable_global(@handle, ret_type, ret_value)
      raise_if_error(result)
      true
    end

    # Enable history tracking for a specific database
    #
    # @param db_num [Integer] Database number
    # @param retention [String] Retention type: "unlimited", "time:<ms>", or "count:<n>"
    # @return [Boolean] true on success
    def history_enable_database(db_num, retention = "unlimited")
      check_open!
      ret_type, ret_value = parse_retention(retention)
      result = FFI.redlite_history_enable_database(@handle, db_num.to_i, ret_type, ret_value)
      raise_if_error(result)
      true
    end

    # Enable history tracking for a specific key
    #
    # @param key [String] The key
    # @param retention [String] Retention type: "unlimited", "time:<ms>", or "count:<n>"
    # @return [Boolean] true on success
    def history_enable_key(key, retention = "unlimited")
      check_open!
      ret_type, ret_value = parse_retention(retention)
      result = FFI.redlite_history_enable_key(@handle, key.to_s, ret_type, ret_value)
      raise_if_error(result)
      true
    end

    # Disable history tracking globally
    #
    # @return [Boolean] true on success
    def history_disable_global
      check_open!
      result = FFI.redlite_history_disable_global(@handle)
      raise_if_error(result)
      true
    end

    # Disable history tracking for a specific database
    #
    # @param db_num [Integer] Database number
    # @return [Boolean] true on success
    def history_disable_database(db_num)
      check_open!
      result = FFI.redlite_history_disable_database(@handle, db_num.to_i)
      raise_if_error(result)
      true
    end

    # Disable history tracking for a specific key
    #
    # @param key [String] The key
    # @return [Boolean] true on success
    def history_disable_key(key)
      check_open!
      result = FFI.redlite_history_disable_key(@handle, key.to_s)
      raise_if_error(result)
      true
    end

    # Check if history is enabled for a key
    #
    # @param key [String] The key
    # @return [Boolean] true if enabled
    def history_enabled?(key)
      check_open!
      result = FFI.redlite_is_history_enabled(@handle, key.to_s)
      result == 1
    end

    # =========================================================================
    # FTS Enable/Disable Commands
    # =========================================================================

    # Enable full-text search indexing globally
    #
    # @return [Boolean] true on success
    def fts_enable_global
      check_open!
      result = FFI.redlite_fts_enable_global(@handle)
      raise_if_error(result)
      true
    end

    # Enable full-text search indexing for a specific database
    #
    # @param db_num [Integer] Database number
    # @return [Boolean] true on success
    def fts_enable_database(db_num)
      check_open!
      result = FFI.redlite_fts_enable_database(@handle, db_num.to_i)
      raise_if_error(result)
      true
    end

    # Enable full-text search indexing for keys matching a pattern
    #
    # @param pattern [String] Glob pattern
    # @return [Boolean] true on success
    def fts_enable_pattern(pattern)
      check_open!
      result = FFI.redlite_fts_enable_pattern(@handle, pattern.to_s)
      raise_if_error(result)
      true
    end

    # Enable full-text search indexing for a specific key
    #
    # @param key [String] The key
    # @return [Boolean] true on success
    def fts_enable_key(key)
      check_open!
      result = FFI.redlite_fts_enable_key(@handle, key.to_s)
      raise_if_error(result)
      true
    end

    # Disable full-text search indexing globally
    #
    # @return [Boolean] true on success
    def fts_disable_global
      check_open!
      result = FFI.redlite_fts_disable_global(@handle)
      raise_if_error(result)
      true
    end

    # Disable full-text search indexing for a specific database
    #
    # @param db_num [Integer] Database number
    # @return [Boolean] true on success
    def fts_disable_database(db_num)
      check_open!
      result = FFI.redlite_fts_disable_database(@handle, db_num.to_i)
      raise_if_error(result)
      true
    end

    # Disable full-text search indexing for keys matching a pattern
    #
    # @param pattern [String] Glob pattern
    # @return [Boolean] true on success
    def fts_disable_pattern(pattern)
      check_open!
      result = FFI.redlite_fts_disable_pattern(@handle, pattern.to_s)
      raise_if_error(result)
      true
    end

    # Disable full-text search indexing for a specific key
    #
    # @param key [String] The key
    # @return [Boolean] true on success
    def fts_disable_key(key)
      check_open!
      result = FFI.redlite_fts_disable_key(@handle, key.to_s)
      raise_if_error(result)
      true
    end

    # Check if FTS is enabled for a key
    #
    # @param key [String] The key
    # @return [Boolean] true if enabled
    def fts_enabled?(key)
      check_open!
      result = FFI.redlite_is_fts_enabled(@handle, key.to_s)
      result == 1
    end

    # =========================================================================
    # KeyInfo Command
    # =========================================================================

    # Get detailed information about a key
    #
    # @param key [String] The key
    # @return [Hash, nil] Key info with :type, :ttl, :created_at, :updated_at, or nil if key doesn't exist
    def keyinfo(key)
      check_open!
      info = FFI.redlite_keyinfo(@handle, key.to_s)
      return nil if info[:valid] == 0

      result = {
        type: info[:key_type].null? ? "none" : info[:key_type].read_string,
        ttl: info[:ttl],
        created_at: info[:created_at],
        updated_at: info[:updated_at]
      }
      FFI.redlite_free_keyinfo(info)
      result
    end

    # =========================================================================
    # Server Commands
    # =========================================================================

    # Compact the database
    #
    # @return [Integer] Bytes freed
    def vacuum
      check_open!
      FFI.redlite_vacuum(@handle)
    end

    # Get the library version
    #
    # @return [String] Version string
    def version
      FFI.redlite_version_string
    end

    private

    # Parse retention string into type and value
    # @param retention [String] "unlimited", "time:<ms>", or "count:<n>"
    # @return [Array] [type_string, value]
    def parse_retention(retention)
      case retention
      when "unlimited"
        ["unlimited", 0]
      when /^time:(\d+)$/
        ["time", $1.to_i]
      when /^count:(\d+)$/
        ["count", $1.to_i]
      else
        raise ArgumentError, "Invalid retention: #{retention}. Use 'unlimited', 'time:<ms>', or 'count:<n>'"
      end
    end

    def check_open!
      raise ConnectionClosedError, "Database is closed" unless @open
    end

    def raise_if_error(result)
      return if result >= 0
      error = FFI.last_error_string || "Unknown error"
      raise Error, error
    end

    def check_error!
      error = FFI.last_error_string
      raise Error, error if error
    end

    # Check for i64::MIN which is used as error sentinel in FFI layer
    I64_MIN = -9223372036854775808

    def raise_if_i64_min_error(result)
      return unless result == I64_MIN
      error = FFI.last_error_string
      raise Error, error if error
    end

    # Convert RedliteBytes to Ruby String
    def bytes_to_string(bytes)
      return nil if bytes[:data].null?
      bytes[:data].read_string(bytes[:len])
    end

    # Convert RedliteBytesArray to Array of Strings
    def bytes_array_to_strings(arr)
      return [] if arr[:len].zero?

      result = []
      arr[:len].times do |i|
        bytes_ptr = arr[:items] + (i * FFI::RedliteBytes.size)
        bytes = FFI::RedliteBytes.new(bytes_ptr)
        result << bytes_to_string(bytes)
      end
      result
    end

    # Convert RedliteStringArray to Array of Strings
    def string_array_to_strings(arr)
      return [] if arr[:len].zero?

      result = []
      arr[:len].times do |i|
        str_ptr = (arr[:strings] + (i * ::FFI::TYPE_POINTER.size)).read_pointer
        result << (str_ptr.null? ? nil : str_ptr.read_string)
      end
      result
    end

    # Convert Ruby string array to pointer array for FFI
    def string_array_to_ptr(strings)
      ptr = ::FFI::MemoryPointer.new(:pointer, strings.length)
      strings.each_with_index do |s, i|
        ptr[i].put_pointer(0, ::FFI::MemoryPointer.from_string(s.to_s))
      end
      ptr
    end
    alias strings_to_pointer_array string_array_to_ptr

    # Convert Ruby values to RedliteBytes array for FFI
    def values_to_bytes_array(values)
      bytes_ptr = ::FFI::MemoryPointer.new(FFI::RedliteBytes, values.length)
      values.each_with_index do |v, i|
        bytes = FFI::RedliteBytes.new(bytes_ptr[i])
        v_bytes = v.to_s
        bytes[:data] = ::FFI::MemoryPointer.from_string(v_bytes)
        bytes[:len] = v_bytes.bytesize
      end
      bytes_ptr
    end
  end
end
