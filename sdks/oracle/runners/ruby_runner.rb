#!/usr/bin/env ruby
# frozen_string_literal: true

# Oracle Test Runner for Ruby SDK
#
# Executes YAML test specifications against the Redlite Ruby SDK
# and reports pass/fail results with detailed error messages.
#
# Usage:
#   ruby ruby_runner.rb                    # Run all specs
#   ruby ruby_runner.rb spec/strings.yaml  # Run single spec
#   ruby ruby_runner.rb -v                 # Verbose output

require "yaml"
require "optparse"

# Add the Ruby SDK to load path
sdk_path = File.expand_path("../../redlite-ruby/lib", __dir__)
$LOAD_PATH.unshift(sdk_path)

require "redlite"

# Oracle test runner that executes YAML specs against the Ruby SDK
class OracleRunner
  def initialize(verbose: false)
    @verbose = verbose
    @passed = 0
    @failed = 0
    @errors = []
  end

  attr_reader :passed, :failed, :errors

  def run_spec_file(spec_path)
    spec = YAML.load_file(spec_path)
    spec_name = spec["name"] || File.basename(spec_path)
    tests = spec["tests"] || []

    if @verbose
      puts
      puts "=" * 60
      puts "Running: #{spec_name} (#{tests.length} tests)"
      puts "=" * 60
    end

    tests.each do |test|
      run_test(test, spec_name)
    end

    @errors.empty?
  end

  def summary
    total = @passed + @failed
    "#{@passed}/#{total} passed, #{@failed} failed"
  end

  private

  def run_test(test, spec_name)
    test_name = test["name"] || "unnamed"

    print "  #{test_name}... " if @verbose

    db = Redlite::Database.new(":memory:")

    begin
      # Run setup operations (no assertions)
      (test["setup"] || []).each do |op|
        execute_cmd(db, op)
      end

      # Run test operations and check expectations
      test["operations"].each do |op|
        actual = execute_cmd(db, op)
        expected = op["expect"]

        unless compare(actual, expected)
          @failed += 1
          @errors << {
            spec: spec_name,
            test: test_name,
            cmd: op["cmd"],
            args: op["args"] || [],
            expected: expected,
            actual: serialize(actual)
          }
          if @verbose
            puts "FAILED"
            puts "      Expected: #{expected.inspect}"
            puts "      Actual:   #{serialize(actual).inspect}"
          end
          return
        end
      end

      @passed += 1
      puts "PASSED" if @verbose

    rescue StandardError => e
      @failed += 1
      @errors << {
        spec: spec_name,
        test: test_name,
        error: e.message
      }
      puts "ERROR: #{e.message}" if @verbose
    ensure
      db&.close
    end
  end

  def execute_cmd(db, op)
    cmd = op["cmd"].downcase
    args = (op["args"] || []).map { |a| process_arg(a) }

    # Handle API differences between generic Redis spec and Ruby SDK

    # Commands that take *args instead of array
    case cmd
    when "del"
      # Spec: DEL [["k1", "k2"]] -> Ruby: delete("k1", "k2")
      args = args.first if args.length == 1 && args.first.is_a?(Array)
      return db.delete(*args)

    when "exists"
      args = args.first if args.length == 1 && args.first.is_a?(Array)
      return db.exists(*args)

    when "mget"
      args = args.first if args.length == 1 && args.first.is_a?(Array)
      return db.mget(*args)

    when "mset"
      # Spec: MSET [["k1", "v1"], ["k2", "v2"]]
      # Ruby: mset({"k1" => "v1", "k2" => "v2"})
      if args.all? { |a| a.is_a?(Array) && a.length == 2 }
        mapping = args.to_h
        return db.mset(mapping)
      end
      return db.mset(args.first)

    when "hset"
      # Spec: HSET ["hash", "field", "value"]
      # Ruby: hset("hash", {"field" => "value"})
      if args.length == 3
        key, field, value = args
        return db.hset(key, { field => value })
      end
      return db.hset(*args)

    when "hdel"
      # Spec: HDEL ["hash", ["f1", "f2"]]
      # Ruby: hdel("hash", "f1", "f2")
      if args.length == 2 && args[1].is_a?(Array)
        key, fields = args
        return db.hdel(key, *fields)
      end
      return db.hdel(*args)

    when "hmget"
      # Spec: HMGET ["hash", ["f1", "f2"]]
      # Ruby: hmget("hash", "f1", "f2")
      if args.length == 2 && args[1].is_a?(Array)
        key, fields = args
        return db.hmget(key, *fields)
      end
      return db.hmget(*args)

    when "zadd"
      # Spec: ZADD ["zset", [[1.0, "member"]]]
      # Ruby: zadd("zset", {"member" => 1.0})
      if args.length == 2 && args[1].is_a?(Array)
        key, members = args
        mapping = {}
        members.each do |item|
          if item.is_a?(Array) && item.length == 2
            score, member = item
            mapping[member.to_s] = score
          end
        end
        return db.zadd(key, mapping)
      end
      return db.zadd(*args)

    when "zrem"
      # Spec: ZREM ["zset", ["m1", "m2"]]
      # Ruby: zrem("zset", "m1", "m2")
      if args.length == 2 && args[1].is_a?(Array)
        key, members = args
        return db.zrem(key, *members)
      end
      return db.zrem(*args)

    when "sadd", "srem"
      # Spec: SADD ["set", ["m1", "m2"]]
      # Ruby: sadd("set", "m1", "m2")
      if args.length == 2 && args[1].is_a?(Array)
        key, members = args
        method = cmd == "sadd" ? :sadd : :srem
        return db.send(method, key, *members)
      end
      method = cmd == "sadd" ? :sadd : :srem
      return db.send(method, *args)

    when "lpush", "rpush"
      # Spec: LPUSH ["list", ["v1", "v2"]]
      # Ruby: lpush("list", "v1", "v2")
      if args.length == 2 && args[1].is_a?(Array)
        key, values = args
        method = cmd == "lpush" ? :lpush : :rpush
        return db.send(method, key, *values)
      end
      method = cmd == "lpush" ? :lpush : :rpush
      return db.send(method, *args)

    when "zrange"
      # Handle with_scores option
      key, start_idx, stop_idx = args[0..2]
      with_scores = args[3] == true || op["kwargs"]&.dig("withscores")
      return db.zrange(key, start_idx, stop_idx, with_scores: with_scores)

    when "zrevrange"
      key, start_idx, stop_idx = args[0..2]
      with_scores = args[3] == true || op["kwargs"]&.dig("withscores")
      return db.zrevrange(key, start_idx, stop_idx, with_scores: with_scores)
    end

    # Standard method map for remaining commands
    method_map = {
      # String commands
      "get" => :get,
      "set" => :set,
      "setex" => :setex,
      "psetex" => :psetex,
      "getdel" => :getdel,
      "append" => :append,
      "strlen" => :strlen,
      "getrange" => :getrange,
      "setrange" => :setrange,
      "incr" => :incr,
      "decr" => :decr,
      "incrby" => :incrby,
      "decrby" => :decrby,
      "incrbyfloat" => :incrbyfloat,
      # Key commands
      "type" => :type,
      "ttl" => :ttl,
      "pttl" => :pttl,
      "expire" => :expire,
      "pexpire" => :pexpire,
      "expireat" => :expireat,
      "pexpireat" => :pexpireat,
      "persist" => :persist,
      "rename" => :rename,
      "renamenx" => :renamenx,
      "keys" => :keys,
      "dbsize" => :dbsize,
      "flushdb" => :flushdb,
      # Hash commands
      "hget" => :hget,
      "hexists" => :hexists,
      "hlen" => :hlen,
      "hkeys" => :hkeys,
      "hvals" => :hvals,
      "hincrby" => :hincrby,
      "hgetall" => :hgetall,
      # List commands
      "lpop" => :lpop,
      "rpop" => :rpop,
      "llen" => :llen,
      "lrange" => :lrange,
      "lindex" => :lindex,
      # Set commands
      "smembers" => :smembers,
      "sismember" => :sismember,
      "scard" => :scard,
      # Sorted set commands
      "zscore" => :zscore,
      "zcard" => :zcard,
      "zcount" => :zcount,
      "zincrby" => :zincrby
    }

    method = method_map[cmd]
    raise "Unknown command: #{cmd}" unless method

    db.send(method, *args)
  end

  def process_arg(arg)
    if arg.is_a?(Hash) && arg.key?("bytes")
      # Convert bytes array to string
      arg["bytes"].pack("C*")
    elsif arg.is_a?(Array)
      arg.map { |a| process_arg(a) }
    else
      arg
    end
  end

  def compare(actual, expected)
    return actual.nil? if expected.nil?

    if expected.is_a?(Hash)
      return compare_special(actual, expected)
    end

    if expected == true || expected == false
      return actual == expected
    end

    if expected.is_a?(Integer)
      return actual == expected
    end

    if expected.is_a?(Float)
      return (actual - expected).abs < 0.001
    end

    if expected.is_a?(String)
      # String comparison - actual might be bytes
      return actual.to_s == expected
    end

    if expected.is_a?(Array)
      return false unless actual.is_a?(Array)
      return false unless actual.length == expected.length
      return actual.zip(expected).all? { |a, e| compare(a, e) }
    end

    actual == expected
  end

  def compare_special(actual, expected)
    if expected.key?("bytes")
      exp_bytes = expected["bytes"].pack("C*")
      return actual == exp_bytes
    end

    if expected.key?("set")
      # Unordered set comparison
      exp_set = Set.new(expected["set"])
      actual_set = if actual.is_a?(Set)
                     Set.new(actual.map(&:to_s))
                   elsif actual.is_a?(Array)
                     Set.new(actual.map(&:to_s))
                   else
                     return false
                   end
      return actual_set == exp_set
    end

    if expected.key?("dict")
      # Dictionary comparison
      exp_dict = expected["dict"]
      return false unless actual.is_a?(Hash)

      actual_dict = actual.transform_keys(&:to_s).transform_values(&:to_s)
      return actual_dict == exp_dict.transform_keys(&:to_s).transform_values(&:to_s)
    end

    if expected.key?("range")
      # Numeric range comparison
      low, high = expected["range"]
      return actual >= low && actual <= high
    end

    if expected.key?("approx")
      # Float approximation
      target = expected["approx"]
      tol = expected["tol"] || 0.001
      return (actual - target).abs <= tol
    end

    if expected.key?("type")
      # Type check only
      type_map = {
        "bytes" => String,
        "str" => String,
        "int" => Integer,
        "float" => Float,
        "list" => Array,
        "dict" => Hash,
        "set" => [Set, Array]
      }
      exp_types = type_map[expected["type"]]
      exp_types = [exp_types] unless exp_types.is_a?(Array)
      return exp_types.any? { |t| actual.is_a?(t) }
    end

    if expected.key?("contains")
      # Substring match
      return actual.to_s.include?(expected["contains"])
    end

    false
  end

  def serialize(value)
    case value
    when String
      # Try to interpret as UTF-8, fall back to bytes representation
      if value.encoding == Encoding::BINARY || !value.valid_encoding?
        "<bytes: #{value.bytes}>"
      else
        value
      end
    when Array
      value.map { |v| serialize(v) }
    when Hash
      value.transform_keys { |k| serialize(k) }.transform_values { |v| serialize(v) }
    when Set
      value.map { |v| serialize(v) }.to_set
    else
      value
    end
  end
end

def main
  options = { verbose: false }
  specs = []

  OptionParser.new do |opts|
    opts.banner = "Usage: #{$PROGRAM_NAME} [options] [spec_files...]"

    opts.on("-v", "--verbose", "Verbose output") do
      options[:verbose] = true
    end

    opts.on("-h", "--help", "Show help") do
      puts opts
      exit
    end
  end.parse!(into: options)

  specs = ARGV.dup

  spec_dir = File.expand_path("../spec", __dir__)

  if specs.empty?
    specs = Dir.glob(File.join(spec_dir, "*.yaml")).sort
  else
    specs = specs.map { |s| File.expand_path(s) }
  end

  runner = OracleRunner.new(verbose: options[:verbose])

  specs.each do |spec_file|
    runner.run_spec_file(spec_file)
  end

  # Print summary
  puts
  puts "=" * 60
  puts "Oracle Test Results: #{runner.summary}"
  puts "=" * 60

  unless runner.errors.empty?
    puts
    puts "Failures:"
    runner.errors.each do |err|
      if err[:error]
        puts "  - #{err[:spec]} / #{err[:test]}: #{err[:error]}"
      else
        puts "  - #{err[:spec]} / #{err[:test]} / #{err[:cmd]}"
        puts "      Expected: #{err[:expected].inspect}"
        puts "      Actual:   #{err[:actual].inspect}"
      end
    end
    exit 1
  end

  exit 0
end

main if __FILE__ == $PROGRAM_NAME
