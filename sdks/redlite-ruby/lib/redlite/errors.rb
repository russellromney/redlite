# frozen_string_literal: true

module Redlite
  # Base error class for all Redlite errors
  class Error < StandardError; end

  # Raised when the database connection is closed
  class ConnectionClosedError < Error; end

  # Raised when a key operation fails
  class KeyError < Error; end

  # Raised when a type mismatch occurs
  class TypeError < Error; end
end
