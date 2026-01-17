#!/usr/bin/env elixir

# Oracle Test Runner for Elixir SDK
#
# Executes YAML test specifications against the Redlite Elixir SDK
# and reports pass/fail results with detailed error messages.
#
# Usage:
#   elixir elixir_runner.exs                      # Run all specs
#   elixir elixir_runner.exs spec/strings.yaml    # Run single spec
#   elixir elixir_runner.exs -v                   # Verbose output

Mix.install([
  {:yaml_elixir, "~> 2.9"}
])

# Add the Elixir SDK to the code path
sdk_path = Path.join([__DIR__, "..", "..", "redlite-elixir", "_build", "dev", "lib", "redlite", "ebin"])
Code.prepend_path(sdk_path)

defmodule OracleRunner do
  @moduledoc """
  Executes oracle test specifications against the Elixir SDK.
  """

  defstruct [:verbose, passed: 0, failed: 0, errors: []]

  def run(args) do
    {opts, spec_files} = parse_args(args)
    verbose = Keyword.get(opts, :verbose, false)

    spec_files = if Enum.empty?(spec_files) do
      Path.wildcard(Path.join([__DIR__, "..", "spec", "*.yaml"]))
    else
      spec_files
    end

    runner = %__MODULE__{verbose: verbose}

    runner = Enum.reduce(spec_files, runner, fn spec_path, acc ->
      run_spec_file(acc, spec_path)
    end)

    # Print summary
    IO.puts("\n" <> String.duplicate("=", 60))
    IO.puts("SUMMARY: #{runner.passed} passed, #{runner.failed} failed")
    IO.puts(String.duplicate("=", 60))

    if runner.failed > 0 do
      IO.puts("\nFailed tests:")
      for error <- Enum.reverse(runner.errors) do
        IO.puts("  - #{error.spec} / #{error.test}")
        if Map.has_key?(error, :cmd) do
          IO.puts("    Command: #{error.cmd} #{inspect(error.args)}")
          IO.puts("    Expected: #{inspect(error.expected)}")
          IO.puts("    Actual: #{inspect(error.actual)}")
        else
          IO.puts("    Error: #{error.error}")
        end
      end
    end

    # Exit with appropriate code
    if runner.failed > 0, do: System.halt(1), else: System.halt(0)
  end

  defp parse_args(args) do
    {opts, rest, _} = OptionParser.parse(args, switches: [verbose: :boolean], aliases: [v: :verbose])
    {opts, rest}
  end

  defp run_spec_file(runner, spec_path) do
    spec = YamlElixir.read_from_file!(spec_path)
    spec_name = spec["name"] || Path.basename(spec_path)
    tests = spec["tests"] || []

    if runner.verbose do
      IO.puts("\n" <> String.duplicate("=", 60))
      IO.puts("Running: #{spec_name} (#{length(tests)} tests)")
      IO.puts(String.duplicate("=", 60))
    end

    Enum.reduce(tests, runner, fn test, acc ->
      run_test(acc, test, spec_name)
    end)
  end

  defp run_test(runner, test, spec_name) do
    test_name = test["name"] || "unnamed"

    if runner.verbose do
      IO.write("\n  #{test_name}... ")
    end

    # Create fresh in-memory database for each test
    {:ok, db} = Redlite.open(":memory:")

    try do
      # Run setup operations
      for op <- test["setup"] || [] do
        execute_cmd(db, op)
      end

      # Run test operations and check expectations
      result = Enum.reduce_while(test["operations"], {:ok, runner}, fn op, {:ok, acc} ->
        actual = execute_cmd(db, op)
        expected = op["expect"]

        if compare(actual, expected) do
          {:cont, {:ok, acc}}
        else
          error = %{
            spec: spec_name,
            test: test_name,
            cmd: op["cmd"],
            args: op["args"] || [],
            expected: expected,
            actual: serialize(actual)
          }

          acc = %{acc |
            failed: acc.failed + 1,
            errors: [error | acc.errors]
          }

          if runner.verbose do
            IO.puts("FAILED")
            IO.puts("      Expected: #{inspect(expected)}")
            IO.puts("      Actual:   #{inspect(serialize(actual))}")
          end

          {:halt, {:failed, acc}}
        end
      end)

      case result do
        {:ok, acc} ->
          if runner.verbose, do: IO.puts("PASSED")
          %{acc | passed: acc.passed + 1}

        {:failed, acc} ->
          acc
      end

    rescue
      e ->
        error = %{
          spec: spec_name,
          test: test_name,
          error: Exception.message(e)
        }

        if runner.verbose do
          IO.puts("ERROR: #{Exception.message(e)}")
        end

        %{runner |
          failed: runner.failed + 1,
          errors: [error | runner.errors]
        }
    end
  end

  defp execute_cmd(db, op) do
    cmd = String.downcase(op["cmd"])
    args = process_args(op["args"] || [])

    case cmd do
      # String commands
      "get" ->
        [key] = args
        case Redlite.get(db, key) do
          {:ok, nil} -> nil
          {:ok, value} -> value
          {:error, _} = err -> err
        end

      "set" ->
        case args do
          [key, value] ->
            case Redlite.set(db, key, value) do
              :ok -> true
              {:error, _} = err -> err
            end
          [key, value, "EX", seconds] ->
            case Redlite.set(db, key, value, ttl: seconds) do
              :ok -> true
              {:error, _} = err -> err
            end
        end

      "mget" ->
        [keys] = args
        case Redlite.mget(db, keys) do
          {:ok, values} -> values
          {:error, _} = err -> err
        end

      "mset" ->
        pairs = args
        map_pairs = Enum.map(pairs, fn [k, v] -> {k, v} end)
        case Redlite.mset(db, map_pairs) do
          :ok -> true
          {:error, _} = err -> err
        end

      "incr" ->
        [key] = args
        case Redlite.incr(db, key) do
          {:ok, value} -> value
          {:error, _} = err -> err
        end

      "decr" ->
        [key] = args
        case Redlite.decr(db, key) do
          {:ok, value} -> value
          {:error, _} = err -> err
        end

      "incrby" ->
        [key, increment] = args
        case Redlite.incrby(db, key, increment) do
          {:ok, value} -> value
          {:error, _} = err -> err
        end

      "decrby" ->
        [key, decrement] = args
        case Redlite.decrby(db, key, decrement) do
          {:ok, value} -> value
          {:error, _} = err -> err
        end

      "incrbyfloat" ->
        [key, increment] = args
        case Redlite.incrbyfloat(db, key, increment) do
          {:ok, value} -> value
          {:error, _} = err -> err
        end

      "append" ->
        [key, value] = args
        case Redlite.append(db, key, value) do
          {:ok, len} -> len
          {:error, _} = err -> err
        end

      "strlen" ->
        [key] = args
        case Redlite.strlen(db, key) do
          {:ok, len} -> len
          {:error, _} = err -> err
        end

      "getrange" ->
        [key, start, stop] = args
        case Redlite.getrange(db, key, start, stop) do
          {:ok, value} -> value
          {:error, _} = err -> err
        end

      "setrange" ->
        [key, offset, value] = args
        case Redlite.setrange(db, key, offset, value) do
          {:ok, len} -> len
          {:error, _} = err -> err
        end

      # Key commands
      "del" ->
        keys = List.flatten(args)
        case Redlite.del(db, keys) do
          {:ok, count} -> count
          {:error, _} = err -> err
        end

      "exists" ->
        keys = List.flatten(args)
        case Redlite.exists(db, keys) do
          {:ok, count} -> count
          {:error, _} = err -> err
        end

      "type" ->
        [key] = args
        case Redlite.type(db, key) do
          {:ok, type} -> Atom.to_string(type)
          {:error, _} = err -> err
        end

      "ttl" ->
        [key] = args
        case Redlite.ttl(db, key) do
          {:ok, ttl} -> ttl
          {:error, _} = err -> err
        end

      "pttl" ->
        [key] = args
        case Redlite.pttl(db, key) do
          {:ok, pttl} -> pttl
          {:error, _} = err -> err
        end

      "expire" ->
        [key, seconds] = args
        case Redlite.expire(db, key, seconds) do
          {:ok, true} -> 1
          {:ok, false} -> 0
          {:error, _} = err -> err
        end

      "persist" ->
        [key] = args
        case Redlite.persist(db, key) do
          {:ok, true} -> 1
          {:ok, false} -> 0
          {:error, _} = err -> err
        end

      "rename" ->
        [key, newkey] = args
        case Redlite.rename(db, key, newkey) do
          :ok -> true
          {:error, _} = err -> err
        end

      "renamenx" ->
        [key, newkey] = args
        case Redlite.renamenx(db, key, newkey) do
          {:ok, true} -> 1
          {:ok, false} -> 0
          {:error, _} = err -> err
        end

      "keys" ->
        [pattern] = args
        case Redlite.keys(db, pattern) do
          {:ok, keys} -> Enum.sort(keys)
          {:error, _} = err -> err
        end

      "dbsize" ->
        case Redlite.dbsize(db) do
          {:ok, size} -> size
          {:error, _} = err -> err
        end

      "flushdb" ->
        case Redlite.flushdb(db) do
          :ok -> true
          {:error, _} = err -> err
        end

      # Hash commands
      "hset" ->
        case args do
          [key, field, value] ->
            case Redlite.hset(db, key, field, value) do
              {:ok, count} -> count
              {:error, _} = err -> err
            end
          [key | pairs] when rem(length(pairs), 2) == 0 ->
            mapping = pairs
              |> Enum.chunk_every(2)
              |> Enum.into(%{}, fn [f, v] -> {f, v} end)
            case Redlite.hset(db, key, mapping) do
              {:ok, count} -> count
              {:error, _} = err -> err
            end
        end

      "hget" ->
        [key, field] = args
        case Redlite.hget(db, key, field) do
          {:ok, nil} -> nil
          {:ok, value} -> value
          {:error, _} = err -> err
        end

      "hdel" ->
        [key | fields] = args
        fields = List.flatten(fields)
        case Redlite.hdel(db, key, fields) do
          {:ok, count} -> count
          {:error, _} = err -> err
        end

      "hexists" ->
        [key, field] = args
        case Redlite.hexists(db, key, field) do
          {:ok, true} -> 1
          {:ok, false} -> 0
          {:error, _} = err -> err
        end

      "hlen" ->
        [key] = args
        case Redlite.hlen(db, key) do
          {:ok, len} -> len
          {:error, _} = err -> err
        end

      "hkeys" ->
        [key] = args
        case Redlite.hkeys(db, key) do
          {:ok, keys} -> Enum.sort(keys)
          {:error, _} = err -> err
        end

      "hvals" ->
        [key] = args
        case Redlite.hvals(db, key) do
          {:ok, vals} -> Enum.sort(vals)
          {:error, _} = err -> err
        end

      "hgetall" ->
        [key] = args
        case Redlite.hgetall(db, key) do
          {:ok, pairs} ->
            pairs
            |> Enum.flat_map(fn {k, v} -> [k, v] end)
            |> Enum.chunk_every(2)
            |> Enum.sort()
            |> List.flatten()
          {:error, _} = err -> err
        end

      "hmget" ->
        [key, fields] = args
        case Redlite.hmget(db, key, fields) do
          {:ok, values} -> values
          {:error, _} = err -> err
        end

      "hincrby" ->
        [key, field, increment] = args
        case Redlite.hincrby(db, key, field, increment) do
          {:ok, value} -> value
          {:error, _} = err -> err
        end

      # List commands
      "lpush" ->
        [key | values] = args
        values = List.flatten(values)
        case Redlite.lpush(db, key, values) do
          {:ok, len} -> len
          {:error, _} = err -> err
        end

      "rpush" ->
        [key | values] = args
        values = List.flatten(values)
        case Redlite.rpush(db, key, values) do
          {:ok, len} -> len
          {:error, _} = err -> err
        end

      "lpop" ->
        case args do
          [key] ->
            case Redlite.lpop(db, key) do
              {:ok, nil} -> nil
              {:ok, value} -> value
              {:error, _} = err -> err
            end
          [key, count] ->
            case Redlite.lpop(db, key, count) do
              {:ok, nil} -> nil
              {:ok, values} -> values
              {:error, _} = err -> err
            end
        end

      "rpop" ->
        case args do
          [key] ->
            case Redlite.rpop(db, key) do
              {:ok, nil} -> nil
              {:ok, value} -> value
              {:error, _} = err -> err
            end
          [key, count] ->
            case Redlite.rpop(db, key, count) do
              {:ok, nil} -> nil
              {:ok, values} -> values
              {:error, _} = err -> err
            end
        end

      "llen" ->
        [key] = args
        case Redlite.llen(db, key) do
          {:ok, len} -> len
          {:error, _} = err -> err
        end

      "lrange" ->
        [key, start, stop] = args
        case Redlite.lrange(db, key, start, stop) do
          {:ok, values} -> values
          {:error, _} = err -> err
        end

      "lindex" ->
        [key, index] = args
        case Redlite.lindex(db, key, index) do
          {:ok, nil} -> nil
          {:ok, value} -> value
          {:error, _} = err -> err
        end

      # Set commands
      "sadd" ->
        [key | members] = args
        members = List.flatten(members)
        case Redlite.sadd(db, key, members) do
          {:ok, count} -> count
          {:error, _} = err -> err
        end

      "srem" ->
        [key | members] = args
        members = List.flatten(members)
        case Redlite.srem(db, key, members) do
          {:ok, count} -> count
          {:error, _} = err -> err
        end

      "smembers" ->
        [key] = args
        case Redlite.smembers(db, key) do
          {:ok, members} -> Enum.sort(members)
          {:error, _} = err -> err
        end

      "sismember" ->
        [key, member] = args
        case Redlite.sismember(db, key, member) do
          {:ok, true} -> 1
          {:ok, false} -> 0
          {:error, _} = err -> err
        end

      "scard" ->
        [key] = args
        case Redlite.scard(db, key) do
          {:ok, count} -> count
          {:error, _} = err -> err
        end

      # Sorted set commands
      "zadd" ->
        [key | score_members] = args
        members = score_members
          |> Enum.chunk_every(2)
          |> Enum.map(fn [score, member] -> {score, member} end)
        case Redlite.zadd(db, key, members) do
          {:ok, count} -> count
          {:error, _} = err -> err
        end

      "zrem" ->
        [key | members] = args
        members = List.flatten(members)
        case Redlite.zrem(db, key, members) do
          {:ok, count} -> count
          {:error, _} = err -> err
        end

      "zscore" ->
        [key, member] = args
        case Redlite.zscore(db, key, member) do
          {:ok, nil} -> nil
          {:ok, score} -> score
          {:error, _} = err -> err
        end

      "zcard" ->
        [key] = args
        case Redlite.zcard(db, key) do
          {:ok, count} -> count
          {:error, _} = err -> err
        end

      "zcount" ->
        [key, min, max] = args
        min = parse_score(min)
        max = parse_score(max)
        case Redlite.zcount(db, key, min, max) do
          {:ok, count} -> count
          {:error, _} = err -> err
        end

      "zincrby" ->
        [key, increment, member] = args
        case Redlite.zincrby(db, key, increment, member) do
          {:ok, score} -> score
          {:error, _} = err -> err
        end

      "zrange" ->
        case args do
          [key, start, stop] ->
            case Redlite.zrange(db, key, start, stop) do
              {:ok, members} -> members
              {:error, _} = err -> err
            end
          [key, start, stop, "WITHSCORES"] ->
            case Redlite.zrange(db, key, start, stop, with_scores: true) do
              {:ok, pairs} ->
                pairs
                |> Enum.flat_map(fn {m, s} -> [m, s] end)
              {:error, _} = err -> err
            end
        end

      "zrevrange" ->
        case args do
          [key, start, stop] ->
            case Redlite.zrevrange(db, key, start, stop) do
              {:ok, members} -> members
              {:error, _} = err -> err
            end
          [key, start, stop, "WITHSCORES"] ->
            case Redlite.zrevrange(db, key, start, stop, with_scores: true) do
              {:ok, pairs} ->
                pairs
                |> Enum.flat_map(fn {m, s} -> [m, s] end)
              {:error, _} = err -> err
            end
        end

      _ ->
        {:error, "Unknown command: #{cmd}"}
    end
  end

  defp process_args(args) do
    Enum.map(args, fn
      %{"bytes" => bytes} -> :binary.list_to_bin(bytes)
      other -> other
    end)
  end

  defp parse_score("-inf"), do: :neg_inf
  defp parse_score("+inf"), do: :pos_inf
  defp parse_score("inf"), do: :pos_inf
  defp parse_score(score) when is_number(score), do: score * 1.0
  defp parse_score(score) when is_binary(score), do: String.to_float(score)

  defp compare(actual, expected) do
    cond do
      expected == nil -> actual == nil
      is_map(expected) && Map.has_key?(expected, "bytes") ->
        expected_bytes = :binary.list_to_bin(expected["bytes"])
        actual == expected_bytes
      is_map(expected) && Map.has_key?(expected, "set") ->
        is_list(actual) && Enum.sort(actual) == Enum.sort(expected["set"])
      is_map(expected) && Map.has_key?(expected, "range") ->
        [min, max] = expected["range"]
        is_number(actual) && actual >= min && actual <= max
      is_map(expected) && Map.has_key?(expected, "approx") ->
        approx = expected["approx"]
        tol = expected["tol"] || 0.001
        is_number(actual) && abs(actual - approx) <= tol
      is_list(expected) ->
        is_list(actual) && length(actual) == length(expected) &&
          Enum.zip(actual, expected)
          |> Enum.all?(fn {a, e} -> compare(a, e) end)
      true ->
        normalize(actual) == normalize(expected)
    end
  end

  defp normalize(value) when is_binary(value), do: value
  defp normalize(value) when is_integer(value), do: value
  defp normalize(value) when is_float(value), do: value
  defp normalize(true), do: true
  defp normalize(false), do: false
  defp normalize(nil), do: nil
  defp normalize(value), do: value

  defp serialize(value) when is_binary(value), do: value
  defp serialize(value) when is_list(value), do: Enum.map(value, &serialize/1)
  defp serialize(value), do: value
end

# Run the oracle tests
OracleRunner.run(System.argv())
