# frozen_string_literal: true

require_relative "redlite/version"
require_relative "redlite/errors"
require_relative "redlite/ffi"
require_relative "redlite/database"

module Redlite
  class << self
    # Open a database with optional block syntax
    #
    # @param path [String] Database path, or ":memory:" for in-memory
    # @param cache_mb [Integer] Cache size in megabytes (default: 64)
    # @yield [Database] The opened database
    # @return [Database, Object] Database instance or block return value
    def open(path = ":memory:", cache_mb: 64, &block)
      Database.open(path, cache_mb: cache_mb, &block)
    end

    # Get the library version
    #
    # @return [String] Version string
    def version
      FFI.redlite_version_string
    end
  end
end
