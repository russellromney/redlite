defmodule Redlite do
  @moduledoc """
  Redis-compatible embedded database with SQLite durability.

  Redlite provides a Redis-compatible API with automatic persistence via SQLite.
  It supports all major Redis data types: strings, hashes, lists, sets, and sorted sets.

  ## Usage

  ### Direct API (recommended for simple cases)

      {:ok, db} = Redlite.open(":memory:")
      :ok = Redlite.set(db, "key", "value")
      {:ok, "value"} = Redlite.get(db, "key")

  ### GenServer wrapper (for process isolation)

      {:ok, pid} = Redlite.start_link(path: ":memory:", name: MyCache)
      :ok = Redlite.set(MyCache, "key", "value")
      {:ok, "value"} = Redlite.get(MyCache, "key")

  ## Opening databases

      # In-memory database
      {:ok, db} = Redlite.open(":memory:")

      # File-based database
      {:ok, db} = Redlite.open("/path/to/db.sqlite")

      # With custom cache size (MB)
      {:ok, db} = Redlite.open_with_cache("/path/to/db.sqlite", 256)
  """

  use GenServer

  alias Redlite.Native

  # =============================================================================
  # Types
  # =============================================================================

  @type db :: reference()
  @type key :: String.t()
  @type value :: binary()
  @type field :: String.t()
  @type member :: binary()
  @type score :: float()
  @type pattern :: String.t()
  @type cursor :: String.t()
  @type server :: GenServer.server()

  # =============================================================================
  # Structs
  # =============================================================================

  defmodule SetOptions do
    @moduledoc "Options for SET command with NX/XX/EX/PX flags"
    defstruct ex: nil, px: nil, nx: false, xx: false

    @type t :: %__MODULE__{
            ex: non_neg_integer() | nil,
            px: non_neg_integer() | nil,
            nx: boolean(),
            xx: boolean()
          }
  end

  defmodule ZMember do
    @moduledoc "Sorted set member with score"
    defstruct score: 0.0, member: <<>>

    @type t :: %__MODULE__{
            score: float(),
            member: binary()
          }
  end

  # =============================================================================
  # Database Lifecycle
  # =============================================================================

  @doc """
  Opens a database at the given path.

  Use `:memory:` for an in-memory database.

  ## Examples

      {:ok, db} = Redlite.open(":memory:")
      {:ok, db} = Redlite.open("/path/to/database.db")
  """
  @spec open(String.t()) :: {:ok, db()} | {:error, term()}
  def open(":memory:"), do: Native.open_memory()
  def open(path), do: Native.open(path)

  @doc """
  Opens a database with a custom cache size.

  ## Examples

      {:ok, db} = Redlite.open_with_cache("/path/to/db.db", 256)
  """
  @spec open_with_cache(String.t(), non_neg_integer()) :: {:ok, db()} | {:error, term()}
  def open_with_cache(path, cache_mb), do: Native.open_with_cache(path, cache_mb)

  # =============================================================================
  # GenServer API
  # =============================================================================

  @doc """
  Starts a Redlite GenServer.

  ## Options

    * `:path` - Database path (default: `:memory:`)
    * `:cache_mb` - Cache size in MB (optional)
    * `:name` - GenServer name for registration

  ## Examples

      {:ok, pid} = Redlite.start_link(path: ":memory:")
      {:ok, pid} = Redlite.start_link(path: "/path/to/db.db", name: MyCache)
  """
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    {name, opts} = Keyword.pop(opts, :name)
    gen_opts = if name, do: [name: name], else: []
    GenServer.start_link(__MODULE__, opts, gen_opts)
  end

  @impl true
  def init(opts) do
    path = Keyword.get(opts, :path, ":memory:")
    cache_mb = Keyword.get(opts, :cache_mb)

    result =
      if cache_mb do
        open_with_cache(path, cache_mb)
      else
        open(path)
      end

    case result do
      {:ok, db} -> {:ok, %{db: db}}
      {:error, reason} -> {:stop, reason}
    end
  end

  @impl true
  def handle_call({:command, fun}, _from, %{db: db} = state) do
    result = fun.(db)
    {:reply, result, state}
  end

  # Helper to call through GenServer or directly
  defp call(db_or_server, fun) when is_reference(db_or_server) do
    fun.(db_or_server)
  end

  defp call(server, fun) do
    GenServer.call(server, {:command, fun})
  end

  # =============================================================================
  # String Commands
  # =============================================================================

  @doc """
  Get the value of a key.

  Returns `{:ok, value}` if the key exists, `{:ok, nil}` if it doesn't.
  """
  @spec get(server() | db(), key()) :: {:ok, value() | nil} | {:error, term()}
  def get(db, key), do: call(db, &Native.get(&1, key))

  @doc """
  Set a key-value pair with optional TTL in seconds.
  """
  @spec set(server() | db(), key(), value(), keyword()) :: :ok | {:error, term()}
  def set(db, key, value, opts \\ []) do
    ttl = Keyword.get(opts, :ttl)
    value = to_binary(value)

    case call(db, &Native.set(&1, key, value, ttl)) do
      {:ok, true} -> :ok
      {:ok, false} -> {:error, :condition_not_met}
      error -> error
    end
  end

  @doc """
  Set a key with options (NX, XX, EX, PX).
  """
  @spec set_opts(server() | db(), key(), value(), SetOptions.t()) ::
          {:ok, boolean()} | {:error, term()}
  def set_opts(db, key, value, %SetOptions{} = opts) do
    value = to_binary(value)
    call(db, &Native.set_opts(&1, key, value, opts))
  end

  @doc """
  Set key with expiration in seconds.
  """
  @spec setex(server() | db(), key(), non_neg_integer(), value()) :: :ok | {:error, term()}
  def setex(db, key, seconds, value) do
    value = to_binary(value)

    case call(db, &Native.setex(&1, key, seconds, value)) do
      {:ok, true} -> :ok
      error -> error
    end
  end

  @doc """
  Set key with expiration in milliseconds.
  """
  @spec psetex(server() | db(), key(), non_neg_integer(), value()) :: :ok | {:error, term()}
  def psetex(db, key, milliseconds, value) do
    value = to_binary(value)

    case call(db, &Native.psetex(&1, key, milliseconds, value)) do
      {:ok, true} -> :ok
      error -> error
    end
  end

  @doc """
  Get and delete a key atomically.
  """
  @spec getdel(server() | db(), key()) :: {:ok, value() | nil} | {:error, term()}
  def getdel(db, key), do: call(db, &Native.getdel(&1, key))

  @doc """
  Append value to a key, returns new length.
  """
  @spec append(server() | db(), key(), value()) :: {:ok, non_neg_integer()} | {:error, term()}
  def append(db, key, value) do
    value = to_binary(value)
    call(db, &Native.append(&1, key, value))
  end

  @doc """
  Get the length of the value stored at key.
  """
  @spec strlen(server() | db(), key()) :: {:ok, non_neg_integer()} | {:error, term()}
  def strlen(db, key), do: call(db, &Native.strlen(&1, key))

  @doc """
  Get a substring of the value stored at key.
  """
  @spec getrange(server() | db(), key(), integer(), integer()) ::
          {:ok, binary()} | {:error, term()}
  def getrange(db, key, start, stop), do: call(db, &Native.getrange(&1, key, start, stop))

  @doc """
  Overwrite part of a string at key starting at offset.
  """
  @spec setrange(server() | db(), key(), non_neg_integer(), value()) ::
          {:ok, non_neg_integer()} | {:error, term()}
  def setrange(db, key, offset, value) do
    value = to_binary(value)
    call(db, &Native.setrange(&1, key, offset, value))
  end

  @doc """
  Increment the integer value of a key by one.
  """
  @spec incr(server() | db(), key()) :: {:ok, integer()} | {:error, term()}
  def incr(db, key), do: call(db, &Native.incr(&1, key))

  @doc """
  Decrement the integer value of a key by one.
  """
  @spec decr(server() | db(), key()) :: {:ok, integer()} | {:error, term()}
  def decr(db, key), do: call(db, &Native.decr(&1, key))

  @doc """
  Increment the integer value of a key by the given amount.
  """
  @spec incrby(server() | db(), key(), integer()) :: {:ok, integer()} | {:error, term()}
  def incrby(db, key, increment), do: call(db, &Native.incrby(&1, key, increment))

  @doc """
  Decrement the integer value of a key by the given amount.
  """
  @spec decrby(server() | db(), key(), integer()) :: {:ok, integer()} | {:error, term()}
  def decrby(db, key, decrement), do: call(db, &Native.decrby(&1, key, decrement))

  @doc """
  Increment the float value of a key by the given amount.
  """
  @spec incrbyfloat(server() | db(), key(), float()) :: {:ok, float()} | {:error, term()}
  def incrbyfloat(db, key, increment), do: call(db, &Native.incrbyfloat(&1, key, increment))

  @doc """
  Get the values of multiple keys.
  """
  @spec mget(server() | db(), [key()]) :: {:ok, [value() | nil]} | {:error, term()}
  def mget(db, keys), do: call(db, &Native.mget(&1, keys))

  @doc """
  Set multiple key-value pairs atomically.
  """
  @spec mset(server() | db(), [{key(), value()}] | %{key() => value()}) ::
          :ok | {:error, term()}
  def mset(db, pairs) when is_map(pairs) do
    pairs = Enum.map(pairs, fn {k, v} -> {k, to_binary(v)} end)

    case call(db, &Native.mset(&1, pairs)) do
      {:ok, true} -> :ok
      error -> error
    end
  end

  def mset(db, pairs) when is_list(pairs) do
    pairs = Enum.map(pairs, fn {k, v} -> {k, to_binary(v)} end)

    case call(db, &Native.mset(&1, pairs)) do
      {:ok, true} -> :ok
      error -> error
    end
  end

  @doc """
  Set key-value only if key does not exist.
  Returns true if set, false if key already exists.
  """
  @spec setnx(server() | db(), key(), value()) :: {:ok, boolean()} | {:error, term()}
  def setnx(db, key, value) do
    value = to_binary(value)
    call(db, &Native.setnx(&1, key, value))
  end

  @doc """
  Get value and optionally set expiration.

  ## Options
    * `:ex` - Set expiration in seconds
    * `:px` - Set expiration in milliseconds
    * `:exat` - Set expiration at Unix timestamp (seconds)
    * `:pxat` - Set expiration at Unix timestamp (milliseconds)
    * `:persist` - Remove expiration
  """
  @spec getex(server() | db(), key(), keyword()) :: {:ok, value() | nil} | {:error, term()}
  def getex(db, key, opts \\ []) do
    ex = Keyword.get(opts, :ex)
    px = Keyword.get(opts, :px)
    exat = Keyword.get(opts, :exat)
    pxat = Keyword.get(opts, :pxat)
    persist = Keyword.get(opts, :persist, false)
    call(db, &Native.getex(&1, key, ex, px, exat, pxat, persist))
  end

  @doc """
  Get the bit value at offset in the string value stored at key.
  """
  @spec getbit(server() | db(), key(), non_neg_integer()) ::
          {:ok, 0 | 1} | {:error, term()}
  def getbit(db, key, offset), do: call(db, &Native.getbit(&1, key, offset))

  @doc """
  Set or clear the bit at offset in the string value stored at key.
  Returns the original bit value at that offset.
  """
  @spec setbit(server() | db(), key(), non_neg_integer(), boolean()) ::
          {:ok, 0 | 1} | {:error, term()}
  def setbit(db, key, offset, value), do: call(db, &Native.setbit(&1, key, offset, value))

  @doc """
  Count the number of set bits (population counting) in a string.

  ## Options
    * `:start` - Start byte position (inclusive)
    * `:end` - End byte position (inclusive)
  """
  @spec bitcount(server() | db(), key(), keyword()) ::
          {:ok, non_neg_integer()} | {:error, term()}
  def bitcount(db, key, opts \\ []) do
    start_pos = Keyword.get(opts, :start)
    end_pos = Keyword.get(opts, :end)
    call(db, &Native.bitcount(&1, key, start_pos, end_pos))
  end

  @doc """
  Perform bitwise operations between strings.

  ## Operations
    * "AND" - bitwise AND
    * "OR" - bitwise OR
    * "XOR" - bitwise XOR
    * "NOT" - bitwise NOT (only one source key)

  Returns the size of the string stored in the destination key.
  """
  @spec bitop(server() | db(), String.t(), key(), [key()]) ::
          {:ok, non_neg_integer()} | {:error, term()}
  def bitop(db, operation, destkey, keys) do
    call(db, &Native.bitop(&1, operation, destkey, keys))
  end

  # =============================================================================
  # Key Commands
  # =============================================================================

  @doc """
  Delete one or more keys, returns count of deleted keys.
  """
  @spec del(server() | db(), key() | [key()]) :: {:ok, non_neg_integer()} | {:error, term()}
  def del(db, key) when is_binary(key), do: del(db, [key])
  def del(db, keys) when is_list(keys), do: call(db, &Native.del(&1, keys))

  @doc """
  Check if keys exist, returns count of existing keys.
  """
  @spec exists(server() | db(), key() | [key()]) :: {:ok, non_neg_integer()} | {:error, term()}
  def exists(db, key) when is_binary(key), do: exists(db, [key])
  def exists(db, keys) when is_list(keys), do: call(db, &Native.exists(&1, keys))

  @doc """
  Get the type of a key.
  """
  @spec type(server() | db(), key()) ::
          {:ok, :string | :list | :set | :zset | :hash | :stream | :none} | {:error, term()}
  def type(db, key), do: call(db, &Native.key_type(&1, key))

  @doc """
  Get the TTL of a key in seconds.

  Returns -2 if the key doesn't exist, -1 if no TTL is set.
  """
  @spec ttl(server() | db(), key()) :: {:ok, integer()} | {:error, term()}
  def ttl(db, key), do: call(db, &Native.ttl(&1, key))

  @doc """
  Get the TTL of a key in milliseconds.
  """
  @spec pttl(server() | db(), key()) :: {:ok, integer()} | {:error, term()}
  def pttl(db, key), do: call(db, &Native.pttl(&1, key))

  @doc """
  Set expiration in seconds.
  """
  @spec expire(server() | db(), key(), non_neg_integer()) ::
          {:ok, boolean()} | {:error, term()}
  def expire(db, key, seconds), do: call(db, &Native.expire(&1, key, seconds))

  @doc """
  Set expiration in milliseconds.
  """
  @spec pexpire(server() | db(), key(), non_neg_integer()) ::
          {:ok, boolean()} | {:error, term()}
  def pexpire(db, key, milliseconds), do: call(db, &Native.pexpire(&1, key, milliseconds))

  @doc """
  Set expiration at Unix timestamp (seconds).
  """
  @spec expireat(server() | db(), key(), non_neg_integer()) ::
          {:ok, boolean()} | {:error, term()}
  def expireat(db, key, unix_seconds), do: call(db, &Native.expireat(&1, key, unix_seconds))

  @doc """
  Set expiration at Unix timestamp (milliseconds).
  """
  @spec pexpireat(server() | db(), key(), non_neg_integer()) ::
          {:ok, boolean()} | {:error, term()}
  def pexpireat(db, key, unix_ms), do: call(db, &Native.pexpireat(&1, key, unix_ms))

  @doc """
  Remove expiration from a key.
  """
  @spec persist(server() | db(), key()) :: {:ok, boolean()} | {:error, term()}
  def persist(db, key), do: call(db, &Native.persist(&1, key))

  @doc """
  Rename a key.
  """
  @spec rename(server() | db(), key(), key()) :: :ok | {:error, term()}
  def rename(db, key, newkey) do
    case call(db, &Native.rename(&1, key, newkey)) do
      {:ok, true} -> :ok
      error -> error
    end
  end

  @doc """
  Rename a key only if the new key doesn't exist.
  """
  @spec renamenx(server() | db(), key(), key()) :: {:ok, boolean()} | {:error, term()}
  def renamenx(db, key, newkey), do: call(db, &Native.renamenx(&1, key, newkey))

  @doc """
  Find all keys matching a pattern.
  """
  @spec keys(server() | db(), pattern()) :: {:ok, [key()]} | {:error, term()}
  def keys(db, pattern \\ "*"), do: call(db, &Native.keys(&1, pattern))

  @doc """
  Get the number of keys in the database.
  """
  @spec dbsize(server() | db()) :: {:ok, non_neg_integer()} | {:error, term()}
  def dbsize(db), do: call(db, &Native.dbsize(&1))

  @doc """
  Delete all keys in the current database.
  """
  @spec flushdb(server() | db()) :: :ok | {:error, term()}
  def flushdb(db) do
    case call(db, &Native.flushdb(&1)) do
      {:ok, true} -> :ok
      error -> error
    end
  end

  @doc """
  Select a database by index (0-15).
  """
  @spec select(server() | db(), non_neg_integer()) :: :ok | {:error, term()}
  def select(db, db_num) do
    case call(db, &Native.select(&1, db_num)) do
      {:ok, true} -> :ok
      error -> error
    end
  end

  # =============================================================================
  # Hash Commands
  # =============================================================================

  @doc """
  Set hash field(s).

  ## Examples

      Redlite.hset(db, "hash", "field", "value")
      Redlite.hset(db, "hash", %{"f1" => "v1", "f2" => "v2"})
  """
  @spec hset(server() | db(), key(), field(), value()) ::
          {:ok, non_neg_integer()} | {:error, term()}
  @spec hset(server() | db(), key(), %{field() => value()}) ::
          {:ok, non_neg_integer()} | {:error, term()}
  def hset(db, key, field, value) when is_binary(field) do
    value = to_binary(value)
    call(db, &Native.hset(&1, key, [{field, value}]))
  end

  def hset(db, key, mapping) when is_map(mapping) do
    pairs = Enum.map(mapping, fn {f, v} -> {f, to_binary(v)} end)
    call(db, &Native.hset(&1, key, pairs))
  end

  @doc """
  Get a hash field value.
  """
  @spec hget(server() | db(), key(), field()) :: {:ok, value() | nil} | {:error, term()}
  def hget(db, key, field), do: call(db, &Native.hget(&1, key, field))

  @doc """
  Delete hash field(s).
  """
  @spec hdel(server() | db(), key(), field() | [field()]) ::
          {:ok, non_neg_integer()} | {:error, term()}
  def hdel(db, key, field) when is_binary(field), do: hdel(db, key, [field])
  def hdel(db, key, fields) when is_list(fields), do: call(db, &Native.hdel(&1, key, fields))

  @doc """
  Check if a hash field exists.
  """
  @spec hexists(server() | db(), key(), field()) :: {:ok, boolean()} | {:error, term()}
  def hexists(db, key, field), do: call(db, &Native.hexists(&1, key, field))

  @doc """
  Get the number of fields in a hash.
  """
  @spec hlen(server() | db(), key()) :: {:ok, non_neg_integer()} | {:error, term()}
  def hlen(db, key), do: call(db, &Native.hlen(&1, key))

  @doc """
  Get all field names in a hash.
  """
  @spec hkeys(server() | db(), key()) :: {:ok, [field()]} | {:error, term()}
  def hkeys(db, key), do: call(db, &Native.hkeys(&1, key))

  @doc """
  Get all values in a hash.
  """
  @spec hvals(server() | db(), key()) :: {:ok, [value()]} | {:error, term()}
  def hvals(db, key), do: call(db, &Native.hvals(&1, key))

  @doc """
  Increment a hash field by an integer.
  """
  @spec hincrby(server() | db(), key(), field(), integer()) ::
          {:ok, integer()} | {:error, term()}
  def hincrby(db, key, field, increment) do
    call(db, &Native.hincrby(&1, key, field, increment))
  end

  @doc """
  Get all fields and values in a hash.
  """
  @spec hgetall(server() | db(), key()) :: {:ok, [{field(), value()}]} | {:error, term()}
  def hgetall(db, key), do: call(db, &Native.hgetall(&1, key))

  @doc """
  Get values of multiple hash fields.
  """
  @spec hmget(server() | db(), key(), [field()]) :: {:ok, [value() | nil]} | {:error, term()}
  def hmget(db, key, fields), do: call(db, &Native.hmget(&1, key, fields))

  @doc """
  Set a hash field only if it does not exist.
  Returns true if set, false if field already exists.
  """
  @spec hsetnx(server() | db(), key(), field(), value()) ::
          {:ok, boolean()} | {:error, term()}
  def hsetnx(db, key, field, value) do
    value = to_binary(value)
    call(db, &Native.hsetnx(&1, key, field, value))
  end

  @doc """
  Increment a hash field by a float value.
  """
  @spec hincrbyfloat(server() | db(), key(), field(), float()) ::
          {:ok, float()} | {:error, term()}
  def hincrbyfloat(db, key, field, increment) do
    call(db, &Native.hincrbyfloat(&1, key, field, increment * 1.0))
  end

  # =============================================================================
  # List Commands
  # =============================================================================

  @doc """
  Push value(s) to the left of a list.
  """
  @spec lpush(server() | db(), key(), value() | [value()]) ::
          {:ok, non_neg_integer()} | {:error, term()}
  def lpush(db, key, value) when is_binary(value), do: lpush(db, key, [value])

  def lpush(db, key, values) when is_list(values) do
    values = Enum.map(values, &to_binary/1)
    call(db, &Native.lpush(&1, key, values))
  end

  @doc """
  Push value(s) to the right of a list.
  """
  @spec rpush(server() | db(), key(), value() | [value()]) ::
          {:ok, non_neg_integer()} | {:error, term()}
  def rpush(db, key, value) when is_binary(value), do: rpush(db, key, [value])

  def rpush(db, key, values) when is_list(values) do
    values = Enum.map(values, &to_binary/1)
    call(db, &Native.rpush(&1, key, values))
  end

  @doc """
  Pop value(s) from the left of a list.
  """
  @spec lpop(server() | db(), key(), non_neg_integer() | nil) ::
          {:ok, value() | [value()] | nil} | {:error, term()}
  def lpop(db, key, count \\ nil), do: call(db, &Native.lpop(&1, key, count))

  @doc """
  Pop value(s) from the right of a list.
  """
  @spec rpop(server() | db(), key(), non_neg_integer() | nil) ::
          {:ok, value() | [value()] | nil} | {:error, term()}
  def rpop(db, key, count \\ nil), do: call(db, &Native.rpop(&1, key, count))

  @doc """
  Get the length of a list.
  """
  @spec llen(server() | db(), key()) :: {:ok, non_neg_integer()} | {:error, term()}
  def llen(db, key), do: call(db, &Native.llen(&1, key))

  @doc """
  Get a range of elements from a list.
  """
  @spec lrange(server() | db(), key(), integer(), integer()) ::
          {:ok, [value()]} | {:error, term()}
  def lrange(db, key, start, stop), do: call(db, &Native.lrange(&1, key, start, stop))

  @doc """
  Get an element at an index in a list.
  """
  @spec lindex(server() | db(), key(), integer()) :: {:ok, value() | nil} | {:error, term()}
  def lindex(db, key, index), do: call(db, &Native.lindex(&1, key, index))

  @doc """
  Set the list element at index to value.
  """
  @spec lset(server() | db(), key(), integer(), value()) :: :ok | {:error, term()}
  def lset(db, key, index, value) do
    value = to_binary(value)
    case call(db, &Native.lset(&1, key, index, value)) do
      {:ok, true} -> :ok
      error -> error
    end
  end

  @doc """
  Trim a list to the specified range.
  """
  @spec ltrim(server() | db(), key(), integer(), integer()) :: :ok | {:error, term()}
  def ltrim(db, key, start, stop) do
    case call(db, &Native.ltrim(&1, key, start, stop)) do
      {:ok, true} -> :ok
      error -> error
    end
  end

  @doc """
  Remove elements from a list.

  * count > 0: Remove elements equal to element moving from head to tail.
  * count < 0: Remove elements equal to element moving from tail to head.
  * count = 0: Remove all elements equal to element.

  Returns the number of removed elements.
  """
  @spec lrem(server() | db(), key(), integer(), value()) ::
          {:ok, non_neg_integer()} | {:error, term()}
  def lrem(db, key, count, element) do
    element = to_binary(element)
    call(db, &Native.lrem(&1, key, count, element))
  end

  @doc """
  Insert an element before or after a pivot element.

  Returns the length of the list after the insert, or -1 when the pivot is not found.
  """
  @spec linsert(server() | db(), key(), :before | :after, value(), value()) ::
          {:ok, integer()} | {:error, term()}
  def linsert(db, key, where, pivot, element) do
    before = where == :before
    pivot = to_binary(pivot)
    element = to_binary(element)
    call(db, &Native.linsert(&1, key, before, pivot, element))
  end

  @doc """
  Push value(s) to the head of a list only if it exists.
  Returns the length after the push, or 0 if the list doesn't exist.
  """
  @spec lpushx(server() | db(), key(), value() | [value()]) ::
          {:ok, non_neg_integer()} | {:error, term()}
  def lpushx(db, key, value) when is_binary(value), do: lpushx(db, key, [value])

  def lpushx(db, key, values) when is_list(values) do
    values = Enum.map(values, &to_binary/1)
    call(db, &Native.lpushx(&1, key, values))
  end

  @doc """
  Push value(s) to the tail of a list only if it exists.
  Returns the length after the push, or 0 if the list doesn't exist.
  """
  @spec rpushx(server() | db(), key(), value() | [value()]) ::
          {:ok, non_neg_integer()} | {:error, term()}
  def rpushx(db, key, value) when is_binary(value), do: rpushx(db, key, [value])

  def rpushx(db, key, values) when is_list(values) do
    values = Enum.map(values, &to_binary/1)
    call(db, &Native.rpushx(&1, key, values))
  end

  @doc """
  Atomically move an element from one list to another.

  ## Options
    * `wherefrom` - `:left` or `:right` (where to pop from source)
    * `whereto` - `:left` or `:right` (where to push to destination)

  Returns the moved element, or nil if source is empty.
  """
  @spec lmove(server() | db(), key(), key(), :left | :right, :left | :right) ::
          {:ok, value() | nil} | {:error, term()}
  def lmove(db, source, destination, wherefrom, whereto) do
    from_int = if wherefrom == :left, do: 0, else: 1
    to_int = if whereto == :left, do: 0, else: 1
    call(db, &Native.lmove(&1, source, destination, from_int, to_int))
  end

  @doc """
  Find the position(s) of an element in a list.

  ## Options
    * `rank` - search direction and starting point (default 1 = left-to-right from start)
    * `count` - number of matches to return (default 1)
    * `maxlen` - max elements to scan (default nil = scan all)

  Returns position(s) of the element, or nil if not found.
  """
  @spec lpos(server() | db(), key(), value(), keyword()) ::
          {:ok, integer() | [integer()] | nil} | {:error, term()}
  def lpos(db, key, element, opts \\ []) do
    element = to_binary(element)
    rank = Keyword.get(opts, :rank)
    count = Keyword.get(opts, :count)
    maxlen = Keyword.get(opts, :maxlen)
    call(db, &Native.lpos(&1, key, element, rank, count, maxlen))
  end

  # =============================================================================
  # Set Commands
  # =============================================================================

  @doc """
  Add member(s) to a set.
  """
  @spec sadd(server() | db(), key(), member() | [member()]) ::
          {:ok, non_neg_integer()} | {:error, term()}
  def sadd(db, key, member) when is_binary(member), do: sadd(db, key, [member])

  def sadd(db, key, members) when is_list(members) do
    members = Enum.map(members, &to_binary/1)
    call(db, &Native.sadd(&1, key, members))
  end

  @doc """
  Remove member(s) from a set.
  """
  @spec srem(server() | db(), key(), member() | [member()]) ::
          {:ok, non_neg_integer()} | {:error, term()}
  def srem(db, key, member) when is_binary(member), do: srem(db, key, [member])

  def srem(db, key, members) when is_list(members) do
    members = Enum.map(members, &to_binary/1)
    call(db, &Native.srem(&1, key, members))
  end

  @doc """
  Get all members of a set.
  """
  @spec smembers(server() | db(), key()) :: {:ok, [member()]} | {:error, term()}
  def smembers(db, key), do: call(db, &Native.smembers(&1, key))

  @doc """
  Check if a member is in a set.
  """
  @spec sismember(server() | db(), key(), member()) :: {:ok, boolean()} | {:error, term()}
  def sismember(db, key, member) do
    member = to_binary(member)
    call(db, &Native.sismember(&1, key, member))
  end

  @doc """
  Get the number of members in a set.
  """
  @spec scard(server() | db(), key()) :: {:ok, non_neg_integer()} | {:error, term()}
  def scard(db, key), do: call(db, &Native.scard(&1, key))

  @doc """
  Remove and return one or more random members from a set.
  """
  @spec spop(server() | db(), key(), non_neg_integer() | nil) ::
          {:ok, member() | [member()] | nil} | {:error, term()}
  def spop(db, key, count \\ nil), do: call(db, &Native.spop(&1, key, count))

  @doc """
  Get one or more random members from a set without removing them.

  If count is positive, returns up to count distinct members.
  If count is negative, may return duplicate members.
  """
  @spec srandmember(server() | db(), key(), integer() | nil) ::
          {:ok, member() | [member()] | nil} | {:error, term()}
  def srandmember(db, key, count \\ nil), do: call(db, &Native.srandmember(&1, key, count))

  @doc """
  Return members of the set resulting from the difference between the first set and all successive sets.
  """
  @spec sdiff(server() | db(), [key()]) :: {:ok, [member()]} | {:error, term()}
  def sdiff(db, keys), do: call(db, &Native.sdiff(&1, keys))

  @doc """
  Return members of the set resulting from the intersection of all given sets.
  """
  @spec sinter(server() | db(), [key()]) :: {:ok, [member()]} | {:error, term()}
  def sinter(db, keys), do: call(db, &Native.sinter(&1, keys))

  @doc """
  Return members of the set resulting from the union of all given sets.
  """
  @spec sunion(server() | db(), [key()]) :: {:ok, [member()]} | {:error, term()}
  def sunion(db, keys), do: call(db, &Native.sunion(&1, keys))

  @doc """
  Store the difference of sets in a destination key.
  Returns the number of elements in the resulting set.
  """
  @spec sdiffstore(server() | db(), key(), [key()]) ::
          {:ok, non_neg_integer()} | {:error, term()}
  def sdiffstore(db, destination, keys), do: call(db, &Native.sdiffstore(&1, destination, keys))

  @doc """
  Store the intersection of sets in a destination key.
  Returns the number of elements in the resulting set.
  """
  @spec sinterstore(server() | db(), key(), [key()]) ::
          {:ok, non_neg_integer()} | {:error, term()}
  def sinterstore(db, destination, keys), do: call(db, &Native.sinterstore(&1, destination, keys))

  @doc """
  Store the union of sets in a destination key.
  Returns the number of elements in the resulting set.
  """
  @spec sunionstore(server() | db(), key(), [key()]) ::
          {:ok, non_neg_integer()} | {:error, term()}
  def sunionstore(db, destination, keys), do: call(db, &Native.sunionstore(&1, destination, keys))

  @doc """
  Move a member from one set to another.
  Returns true if the element was moved, false if it wasn't a member of source.
  """
  @spec smove(server() | db(), key(), key(), member()) :: {:ok, boolean()} | {:error, term()}
  def smove(db, source, destination, member) do
    member = to_binary(member)

    case call(db, &Native.smove(&1, source, destination, member)) do
      {:ok, 1} -> {:ok, true}
      {:ok, 0} -> {:ok, false}
      {:ok, true} -> {:ok, true}
      {:ok, false} -> {:ok, false}
      error -> error
    end
  end

  # =============================================================================
  # Sorted Set Commands
  # =============================================================================

  @doc """
  Add member(s) with score(s) to a sorted set.

  ## Examples

      Redlite.zadd(db, "zset", 1.0, "member")
      Redlite.zadd(db, "zset", [{1.0, "a"}, {2.0, "b"}])
  """
  @spec zadd(server() | db(), key(), score(), member()) ::
          {:ok, non_neg_integer()} | {:error, term()}
  @spec zadd(server() | db(), key(), [{score(), member()}]) ::
          {:ok, non_neg_integer()} | {:error, term()}
  def zadd(db, key, score, member) when is_number(score) do
    member = to_binary(member)
    call(db, &Native.zadd(&1, key, [{score * 1.0, member}]))
  end

  def zadd(db, key, members) when is_list(members) do
    members = Enum.map(members, fn {score, member} -> {score * 1.0, to_binary(member)} end)
    call(db, &Native.zadd(&1, key, members))
  end

  @doc """
  Remove member(s) from a sorted set.
  """
  @spec zrem(server() | db(), key(), member() | [member()]) ::
          {:ok, non_neg_integer()} | {:error, term()}
  def zrem(db, key, member) when is_binary(member), do: zrem(db, key, [member])

  def zrem(db, key, members) when is_list(members) do
    members = Enum.map(members, &to_binary/1)
    call(db, &Native.zrem(&1, key, members))
  end

  @doc """
  Get the score of a member in a sorted set.
  """
  @spec zscore(server() | db(), key(), member()) :: {:ok, score() | nil} | {:error, term()}
  def zscore(db, key, member) do
    member = to_binary(member)
    call(db, &Native.zscore(&1, key, member))
  end

  @doc """
  Get the number of members in a sorted set.
  """
  @spec zcard(server() | db(), key()) :: {:ok, non_neg_integer()} | {:error, term()}
  def zcard(db, key), do: call(db, &Native.zcard(&1, key))

  @doc """
  Count members in a sorted set within a score range.
  """
  @spec zcount(server() | db(), key(), score(), score()) ::
          {:ok, non_neg_integer()} | {:error, term()}
  def zcount(db, key, min, max), do: call(db, &Native.zcount(&1, key, min * 1.0, max * 1.0))

  @doc """
  Increment the score of a member in a sorted set.
  """
  @spec zincrby(server() | db(), key(), score(), member()) ::
          {:ok, score()} | {:error, term()}
  def zincrby(db, key, increment, member) do
    member = to_binary(member)
    call(db, &Native.zincrby(&1, key, increment * 1.0, member))
  end

  @doc """
  Get members by rank range (ascending order).

  ## Options

    * `:with_scores` - Include scores in the result (default: false)
  """
  @spec zrange(server() | db(), key(), integer(), integer(), keyword()) ::
          {:ok, [member()] | [{member(), score()}]} | {:error, term()}
  def zrange(db, key, start, stop, opts \\ []) do
    with_scores = Keyword.get(opts, :with_scores, false)
    call(db, &Native.zrange(&1, key, start, stop, with_scores))
  end

  @doc """
  Get members by rank range (descending order).
  """
  @spec zrevrange(server() | db(), key(), integer(), integer(), keyword()) ::
          {:ok, [member()] | [{member(), score()}]} | {:error, term()}
  def zrevrange(db, key, start, stop, opts \\ []) do
    with_scores = Keyword.get(opts, :with_scores, false)
    call(db, &Native.zrevrange(&1, key, start, stop, with_scores))
  end

  @doc """
  Get the rank of a member in a sorted set (ascending order, 0-based).
  Returns nil if the member doesn't exist.
  """
  @spec zrank(server() | db(), key(), member()) ::
          {:ok, non_neg_integer() | nil} | {:error, term()}
  def zrank(db, key, member) do
    member = to_binary(member)
    call(db, &Native.zrank(&1, key, member))
  end

  @doc """
  Get the rank of a member in a sorted set (descending order, 0-based).
  Returns nil if the member doesn't exist.
  """
  @spec zrevrank(server() | db(), key(), member()) ::
          {:ok, non_neg_integer() | nil} | {:error, term()}
  def zrevrank(db, key, member) do
    member = to_binary(member)
    call(db, &Native.zrevrank(&1, key, member))
  end

  @doc """
  Get members by score range.

  ## Options
    * `:offset` - Skip this many elements (for pagination)
    * `:count` - Return at most this many elements

  Returns members with scores between min and max (inclusive).
  """
  @spec zrangebyscore(server() | db(), key(), score(), score(), keyword()) ::
          {:ok, [member()]} | {:error, term()}
  def zrangebyscore(db, key, min, max, opts \\ []) do
    offset = Keyword.get(opts, :offset)
    count = Keyword.get(opts, :count)
    call(db, &Native.zrangebyscore(&1, key, min * 1.0, max * 1.0, offset, count))
  end

  @doc """
  Remove all members in a sorted set within the given rank range.
  Returns the number of elements removed.
  """
  @spec zremrangebyrank(server() | db(), key(), integer(), integer()) ::
          {:ok, non_neg_integer()} | {:error, term()}
  def zremrangebyrank(db, key, start, stop) do
    call(db, &Native.zremrangebyrank(&1, key, start, stop))
  end

  @doc """
  Remove all members in a sorted set within the given score range.
  Returns the number of elements removed.
  """
  @spec zremrangebyscore(server() | db(), key(), score(), score()) ::
          {:ok, non_neg_integer()} | {:error, term()}
  def zremrangebyscore(db, key, min, max) do
    call(db, &Native.zremrangebyscore(&1, key, min * 1.0, max * 1.0))
  end

  @doc """
  Compute the intersection of sorted sets and store the result.

  ## Options
    * `:weights` - List of weight multipliers for each set (default: 1.0 for all)
    * `:aggregate` - Aggregation function: "SUM", "MIN", or "MAX" (default: "SUM")

  Returns the number of elements in the resulting set.
  """
  @spec zinterstore(server() | db(), key(), [key()], keyword()) ::
          {:ok, non_neg_integer()} | {:error, term()}
  def zinterstore(db, destination, keys, opts \\ []) do
    weights = Keyword.get(opts, :weights)
    aggregate = Keyword.get(opts, :aggregate)
    call(db, &Native.zinterstore(&1, destination, keys, weights, aggregate))
  end

  @doc """
  Compute the union of sorted sets and store the result.

  ## Options
    * `:weights` - List of weight multipliers for each set (default: 1.0 for all)
    * `:aggregate` - Aggregation function: "SUM", "MIN", or "MAX" (default: "SUM")

  Returns the number of elements in the resulting set.
  """
  @spec zunionstore(server() | db(), key(), [key()], keyword()) ::
          {:ok, non_neg_integer()} | {:error, term()}
  def zunionstore(db, destination, keys, opts \\ []) do
    weights = Keyword.get(opts, :weights)
    aggregate = Keyword.get(opts, :aggregate)
    call(db, &Native.zunionstore(&1, destination, keys, weights, aggregate))
  end

  # =============================================================================
  # Scan Commands
  # =============================================================================

  @doc """
  Incrementally iterate keys matching a pattern.
  """
  @spec scan(server() | db(), cursor(), keyword()) ::
          {:ok, {cursor(), [key()]}} | {:error, term()}
  def scan(db, cursor \\ "0", opts \\ []) do
    pattern = Keyword.get(opts, :match)
    count = Keyword.get(opts, :count, 10)
    call(db, &Native.scan(&1, cursor, pattern, count))
  end

  @doc """
  Incrementally iterate hash fields.
  """
  @spec hscan(server() | db(), key(), cursor(), keyword()) ::
          {:ok, {cursor(), [{field(), value()}]}} | {:error, term()}
  def hscan(db, key, cursor \\ "0", opts \\ []) do
    pattern = Keyword.get(opts, :match)
    count = Keyword.get(opts, :count, 10)
    call(db, &Native.hscan(&1, key, cursor, pattern, count))
  end

  @doc """
  Incrementally iterate set members.
  """
  @spec sscan(server() | db(), key(), cursor(), keyword()) ::
          {:ok, {cursor(), [member()]}} | {:error, term()}
  def sscan(db, key, cursor \\ "0", opts \\ []) do
    pattern = Keyword.get(opts, :match)
    count = Keyword.get(opts, :count, 10)
    call(db, &Native.sscan(&1, key, cursor, pattern, count))
  end

  @doc """
  Incrementally iterate sorted set members with scores.
  """
  @spec zscan(server() | db(), key(), cursor(), keyword()) ::
          {:ok, {cursor(), [{member(), score()}]}} | {:error, term()}
  def zscan(db, key, cursor \\ "0", opts \\ []) do
    pattern = Keyword.get(opts, :match)
    count = Keyword.get(opts, :count, 10)
    call(db, &Native.zscan(&1, key, cursor, pattern, count))
  end

  # =============================================================================
  # Server Commands
  # =============================================================================

  @doc """
  Run SQLite VACUUM to reclaim space.
  """
  @spec vacuum(server() | db()) :: {:ok, non_neg_integer()} | {:error, term()}
  def vacuum(db), do: call(db, &Native.vacuum(&1))

  # =============================================================================
  # Private Helpers
  # =============================================================================

  defp to_binary(value) when is_binary(value), do: value
  defp to_binary(value) when is_integer(value), do: Integer.to_string(value)
  defp to_binary(value) when is_float(value), do: Float.to_string(value)
  defp to_binary(value) when is_atom(value), do: Atom.to_string(value)
end
