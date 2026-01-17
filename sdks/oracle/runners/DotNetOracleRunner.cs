/**
 * .NET Oracle Test Runner
 * Validates C# SDK against oracle test specifications
 *
 * Build: dotnet build
 * Usage: dotnet run -- [-v] ../spec/strings.yaml
 */

using System.Globalization;
using System.Text;
using YamlDotNet.RepresentationModel;
using Redlite;

class OracleRunner
{
    private bool _verbose;
    private int _passed;
    private int _failed;
    private List<(string TestName, string Error)> _errors = new();

    public OracleRunner(bool verbose = false)
    {
        _verbose = verbose;
    }

    public void RunSpecFile(string path)
    {
        using var reader = new StreamReader(path);
        var yaml = new YamlStream();
        yaml.Load(reader);

        var root = (YamlMappingNode)yaml.Documents[0].RootNode;
        var specName = ((YamlScalarNode)root["name"]).Value!;

        if (_verbose)
            Console.WriteLine($"Running spec: {specName}");

        var tests = (YamlSequenceNode)root["tests"];
        foreach (YamlMappingNode test in tests)
        {
            RunTest(test, specName);
        }
    }

    private void RunTest(YamlMappingNode test, string specName)
    {
        var testName = ((YamlScalarNode)test["name"]).Value!;
        var fullName = $"{specName} :: {testName}";

        try
        {
            using var db = RedliteDb.OpenMemory();

            // Run setup operations
            if (test.Children.ContainsKey("setup"))
            {
                foreach (YamlMappingNode op in (YamlSequenceNode)test["setup"])
                {
                    ExecuteCmd(db, op);
                }
            }

            // Run test operations
            var operations = (YamlSequenceNode)test["operations"];
            foreach (YamlMappingNode op in operations)
            {
                var actual = ExecuteCmd(db, op);

                if (op.Children.ContainsKey("expect"))
                {
                    var expected = op["expect"];
                    if (!Compare(actual, expected))
                    {
                        throw new Exception($"Expected: {YamlToString(expected)}, Got: {ValueToString(actual)}");
                    }
                }
            }

            _passed++;
            if (_verbose)
                Console.WriteLine($"  ✓ {testName}");
        }
        catch (Exception ex)
        {
            _failed++;
            _errors.Add((fullName, ex.Message));
            if (_verbose)
                Console.WriteLine($"  ✗ {testName}: {ex.Message}");
        }
    }

    private object? ExecuteCmd(RedliteDb db, YamlMappingNode op)
    {
        var cmd = ((YamlScalarNode)op["cmd"]).Value!;
        var args = op.Children.ContainsKey("args") ? op["args"] as YamlSequenceNode : null;
        var kwargs = op.Children.ContainsKey("kwargs") ? (YamlMappingNode)op["kwargs"] : null;

        return cmd switch
        {
            // String commands
            "GET" => db.GetString(GetScalarArg(args, 0)),
            "SET" => SetWithOptions(db, args!, kwargs),
            "SETEX" => db.SetEx(GetScalarArg(args, 0), GetLongArg(args, 1), GetStringOrBytesArg(args, 2)),
            "PSETEX" => db.PSetEx(GetScalarArg(args, 0), GetLongArg(args, 1), GetStringOrBytesArg(args, 2)),
            "GETDEL" => db.GetDelString(GetScalarArg(args, 0)),
            "APPEND" => db.Append(GetScalarArg(args, 0), GetScalarArg(args, 1)),
            "STRLEN" => db.StrLen(GetScalarArg(args, 0)),
            "GETRANGE" => db.GetRange(GetScalarArg(args, 0), GetLongArg(args, 1), GetLongArg(args, 2)),
            "SETRANGE" => db.SetRange(GetScalarArg(args, 0), GetLongArg(args, 1), GetScalarArg(args, 2)),
            "INCR" => db.Incr(GetScalarArg(args, 0)),
            "DECR" => db.Decr(GetScalarArg(args, 0)),
            "INCRBY" => db.IncrBy(GetScalarArg(args, 0), GetLongArg(args, 1)),
            "DECRBY" => db.DecrBy(GetScalarArg(args, 0), GetLongArg(args, 1)),
            "INCRBYFLOAT" => db.IncrByFloat(GetScalarArg(args, 0), GetDoubleArg(args, 1)),
            "MGET" => MGetFromArgs(db, args!),
            "MSET" => MSetFromArgs(db, args!),

            // Key commands
            "DEL" => DelFromArgs(db, args!),
            "EXISTS" => ExistsFromArgs(db, args!),
            "TYPE" => db.Type(GetScalarArg(args, 0)),
            "TTL" => db.Ttl(GetScalarArg(args, 0)),
            "PTTL" => db.PTtl(GetScalarArg(args, 0)),
            "EXPIRE" => db.Expire(GetScalarArg(args, 0), GetLongArg(args, 1)) ? 1L : 0L,
            "PEXPIRE" => db.PExpire(GetScalarArg(args, 0), GetLongArg(args, 1)) ? 1L : 0L,
            "EXPIREAT" => db.ExpireAt(GetScalarArg(args, 0), GetFutureTimestamp(args![1])) ? 1L : 0L,
            "PEXPIREAT" => db.PExpireAt(GetScalarArg(args, 0), GetFutureTimestampMs(args![1])) ? 1L : 0L,
            "PERSIST" => db.Persist(GetScalarArg(args, 0)) ? 1L : 0L,
            "RENAME" => db.Rename(GetScalarArg(args, 0), GetScalarArg(args, 1)),
            "RENAMENX" => db.RenameNx(GetScalarArg(args, 0), GetScalarArg(args, 1)) ? 1L : 0L,
            "KEYS" => db.Keys(args != null && args.Children.Count > 0 ? GetScalarArg(args, 0) : "*"),
            "DBSIZE" => db.DbSize(),
            "FLUSHDB" => db.FlushDb(),
            "SELECT" => db.Select(GetIntArg(args, 0)),
            "VACUUM" => db.Vacuum(),

            // Hash commands
            "HSET" => HSetFromArgs(db, args!),
            "HGET" => db.HGet(GetScalarArg(args, 0), GetScalarArg(args, 1)),
            "HDEL" => HDelFromArgs(db, args!),
            "HEXISTS" => db.HExists(GetScalarArg(args, 0), GetScalarArg(args, 1)) ? 1L : 0L,
            "HLEN" => db.HLen(GetScalarArg(args, 0)),
            "HKEYS" => db.HKeys(GetScalarArg(args, 0)),
            "HVALS" => db.HVals(GetScalarArg(args, 0)),
            "HINCRBY" => db.HIncrBy(GetScalarArg(args, 0), GetScalarArg(args, 1), GetLongArg(args, 2)),
            "HGETALL" => db.HGetAll(GetScalarArg(args, 0)),
            "HMGET" => HMGetFromArgs(db, args!),

            // List commands
            "LPUSH" => LPushFromArgs(db, args!),
            "RPUSH" => RPushFromArgs(db, args!),
            "LPOP" => LPopFromArgs(db, args!),
            "RPOP" => RPopFromArgs(db, args!),
            "LLEN" => db.LLen(GetScalarArg(args, 0)),
            "LRANGE" => db.LRange(GetScalarArg(args, 0), GetLongArg(args, 1), GetLongArg(args, 2)),
            "LINDEX" => db.LIndex(GetScalarArg(args, 0), GetLongArg(args, 1)),

            // Set commands
            "SADD" => SAddFromArgs(db, args!),
            "SREM" => SRemFromArgs(db, args!),
            "SMEMBERS" => db.SMembers(GetScalarArg(args, 0)),
            "SISMEMBER" => db.SIsMember(GetScalarArg(args, 0), GetScalarArg(args, 1)) ? 1L : 0L,
            "SCARD" => db.SCard(GetScalarArg(args, 0)),

            // Sorted set commands
            "ZADD" => ZAddFromArgs(db, args!),
            "ZREM" => ZRemFromArgs(db, args!),
            "ZSCORE" => db.ZScore(GetScalarArg(args, 0), GetScalarArg(args, 1)),
            "ZCARD" => db.ZCard(GetScalarArg(args, 0)),
            "ZCOUNT" => db.ZCount(GetScalarArg(args, 0), GetDoubleArg(args, 1), GetDoubleArg(args, 2)),
            "ZINCRBY" => db.ZIncrBy(GetScalarArg(args, 0), GetDoubleArg(args, 1), GetScalarArg(args, 2)),
            "ZRANGE" => db.ZRange(GetScalarArg(args, 0), GetLongArg(args, 1), GetLongArg(args, 2)),
            "ZREVRANGE" => db.ZRevRange(GetScalarArg(args, 0), GetLongArg(args, 1), GetLongArg(args, 2)),

            _ => throw new Exception($"Unknown command: {cmd}")
        };
    }

    // Helper to get scalar string arg at index
    private static string GetScalarArg(YamlSequenceNode? args, int index)
    {
        var node = args![index];
        if (node is YamlScalarNode scalar)
            return scalar.Value ?? "";
        throw new Exception($"Expected scalar at index {index}, got {node.NodeType}");
    }

    // Helper to get long arg at index
    private static long GetLongArg(YamlSequenceNode? args, int index)
    {
        var node = args![index];
        if (node is YamlScalarNode scalar)
            return long.Parse(scalar.Value!);
        throw new Exception($"Expected scalar at index {index}, got {node.NodeType}");
    }

    // Helper to get int arg at index
    private static int GetIntArg(YamlSequenceNode? args, int index)
    {
        var node = args![index];
        if (node is YamlScalarNode scalar)
            return int.Parse(scalar.Value!);
        throw new Exception($"Expected scalar at index {index}, got {node.NodeType}");
    }

    // Helper to get double arg at index
    private static double GetDoubleArg(YamlSequenceNode? args, int index)
    {
        var node = args![index];
        if (node is YamlScalarNode scalar)
            return double.Parse(scalar.Value!, CultureInfo.InvariantCulture);
        throw new Exception($"Expected scalar at index {index}, got {node.NodeType}");
    }

    // Helper to get string or bytes value (for binary data support)
    private static string GetStringOrBytesArg(YamlSequenceNode args, int index)
    {
        var node = args[index];
        if (node is YamlScalarNode scalar)
            return scalar.Value ?? "";
        if (node is YamlMappingNode map && map.Children.ContainsKey("bytes"))
        {
            var bytes = (YamlSequenceNode)map["bytes"];
            var byteArray = bytes.Select(b => byte.Parse(((YamlScalarNode)b).Value!)).ToArray();
            return Encoding.Latin1.GetString(byteArray);
        }
        throw new Exception($"Expected scalar or bytes at index {index}");
    }

    // Get future timestamp from {future_seconds: N} format
    private static long GetFutureTimestamp(YamlNode node)
    {
        if (node is YamlMappingNode map && map.Children.ContainsKey("future_seconds"))
        {
            var seconds = long.Parse(((YamlScalarNode)map["future_seconds"]).Value!);
            return DateTimeOffset.UtcNow.ToUnixTimeSeconds() + seconds;
        }
        if (node is YamlScalarNode scalar)
            return long.Parse(scalar.Value!);
        throw new Exception("Expected future_seconds or scalar for EXPIREAT");
    }

    // Get future timestamp ms from {future_ms: N} format
    private static long GetFutureTimestampMs(YamlNode node)
    {
        if (node is YamlMappingNode map && map.Children.ContainsKey("future_ms"))
        {
            var ms = long.Parse(((YamlScalarNode)map["future_ms"]).Value!);
            return DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() + ms;
        }
        if (node is YamlScalarNode scalar)
            return long.Parse(scalar.Value!);
        throw new Exception("Expected future_ms or scalar for PEXPIREAT");
    }

    private bool SetWithOptions(RedliteDb db, YamlSequenceNode args, YamlMappingNode? kwargs)
    {
        var key = GetScalarArg(args, 0);
        var value = GetStringOrBytesArg(args, 1);
        long ttl = 0;
        if (kwargs != null && kwargs.Children.ContainsKey("ex"))
        {
            ttl = long.Parse(((YamlScalarNode)kwargs["ex"]).Value!);
        }
        return db.Set(key, value, ttl);
    }

    // MSET: args = [["k1", "v1"], ["k2", "v2"], ...]
    private bool MSetFromArgs(RedliteDb db, YamlSequenceNode args)
    {
        var pairs = new List<(string, string)>();
        foreach (YamlSequenceNode pair in args)
        {
            var key = ((YamlScalarNode)pair[0]).Value!;
            var val = ((YamlScalarNode)pair[1]).Value!;
            pairs.Add((key, val));
        }
        return db.MSet(pairs.ToArray());
    }

    // MGET: args = [["k1", "k2", ...]]
    private string?[] MGetFromArgs(RedliteDb db, YamlSequenceNode args)
    {
        var keys = ((YamlSequenceNode)args[0]).Select(k => ((YamlScalarNode)k).Value!).ToArray();
        return db.MGet(keys);
    }

    // DEL: args = [["k1", "k2"]] or ["k1", "k2"]
    private long DelFromArgs(RedliteDb db, YamlSequenceNode args)
    {
        if (args[0] is YamlSequenceNode seq)
        {
            var keys = seq.Select(k => ((YamlScalarNode)k).Value!).ToArray();
            return db.Del(keys);
        }
        else
        {
            var keys = args.Select(k => ((YamlScalarNode)k).Value!).ToArray();
            return db.Del(keys);
        }
    }

    // EXISTS: args = [["k1", "k2"]] or ["k1", "k2"]
    private long ExistsFromArgs(RedliteDb db, YamlSequenceNode args)
    {
        if (args[0] is YamlSequenceNode seq)
        {
            var keys = seq.Select(k => ((YamlScalarNode)k).Value!).ToArray();
            return db.Exists(keys);
        }
        else
        {
            var keys = args.Select(k => ((YamlScalarNode)k).Value!).ToArray();
            return db.Exists(keys);
        }
    }

    // HSET: args = ["key", [["f1", "v1"], ["f2", "v2"]]] or ["key", "f1", "v1", ...]
    private long HSetFromArgs(RedliteDb db, YamlSequenceNode args)
    {
        var key = GetScalarArg(args, 0);
        var dict = new Dictionary<string, string>();

        if (args[1] is YamlSequenceNode pairs)
        {
            // Nested format: [["f1", "v1"], ["f2", "v2"]]
            foreach (YamlSequenceNode pair in pairs)
            {
                dict[((YamlScalarNode)pair[0]).Value!] = ((YamlScalarNode)pair[1]).Value!;
            }
        }
        else
        {
            // Flat format: ["f1", "v1", "f2", "v2"]
            for (int i = 1; i + 1 < args.Children.Count; i += 2)
            {
                dict[GetScalarArg(args, i)] = GetScalarArg(args, i + 1);
            }
        }
        return db.HSet(key, dict);
    }

    // HDEL: args = ["key", ["f1", "f2"]] or ["key", "f1", "f2"]
    private long HDelFromArgs(RedliteDb db, YamlSequenceNode args)
    {
        var key = GetScalarArg(args, 0);
        if (args[1] is YamlSequenceNode fields)
        {
            var fieldArr = fields.Select(f => ((YamlScalarNode)f).Value!).ToArray();
            return db.HDel(key, fieldArr);
        }
        else
        {
            var fieldArr = args.Skip(1).Select(f => ((YamlScalarNode)f).Value!).ToArray();
            return db.HDel(key, fieldArr);
        }
    }

    // HMGET: args = ["key", ["f1", "f2"]] or ["key", "f1", "f2"]
    private string?[] HMGetFromArgs(RedliteDb db, YamlSequenceNode args)
    {
        var key = GetScalarArg(args, 0);
        if (args[1] is YamlSequenceNode fields)
        {
            var fieldArr = fields.Select(f => ((YamlScalarNode)f).Value!).ToArray();
            return db.HMGet(key, fieldArr);
        }
        else
        {
            var fieldArr = args.Skip(1).Select(f => ((YamlScalarNode)f).Value!).ToArray();
            return db.HMGet(key, fieldArr);
        }
    }

    // LPUSH: args = ["key", ["v1", "v2"]] or ["key", "v1", "v2"]
    private long LPushFromArgs(RedliteDb db, YamlSequenceNode args)
    {
        var key = GetScalarArg(args, 0);
        if (args[1] is YamlSequenceNode values)
        {
            var valArr = values.Select(v => ((YamlScalarNode)v).Value!).ToArray();
            return db.LPush(key, valArr);
        }
        else
        {
            var valArr = args.Skip(1).Select(v => ((YamlScalarNode)v).Value!).ToArray();
            return db.LPush(key, valArr);
        }
    }

    // RPUSH: args = ["key", ["v1", "v2"]] or ["key", "v1", "v2"]
    private long RPushFromArgs(RedliteDb db, YamlSequenceNode args)
    {
        var key = GetScalarArg(args, 0);
        if (args[1] is YamlSequenceNode values)
        {
            var valArr = values.Select(v => ((YamlScalarNode)v).Value!).ToArray();
            return db.RPush(key, valArr);
        }
        else
        {
            var valArr = args.Skip(1).Select(v => ((YamlScalarNode)v).Value!).ToArray();
            return db.RPush(key, valArr);
        }
    }

    // LPOP: args = ["key"] or ["key", count]
    private object? LPopFromArgs(RedliteDb db, YamlSequenceNode args)
    {
        var key = GetScalarArg(args, 0);
        if (args.Children.Count > 1)
        {
            var count = GetLongArg(args, 1);
            return db.LPop(key, (int)count);
        }
        return db.LPop(key);
    }

    // RPOP: args = ["key"] or ["key", count]
    private object? RPopFromArgs(RedliteDb db, YamlSequenceNode args)
    {
        var key = GetScalarArg(args, 0);
        if (args.Children.Count > 1)
        {
            var count = GetLongArg(args, 1);
            return db.RPop(key, (int)count);
        }
        return db.RPop(key);
    }

    // SADD: args = ["key", ["m1", "m2"]] or ["key", "m1", "m2"]
    private long SAddFromArgs(RedliteDb db, YamlSequenceNode args)
    {
        var key = GetScalarArg(args, 0);
        if (args[1] is YamlSequenceNode members)
        {
            var memArr = members.Select(m => ((YamlScalarNode)m).Value!).ToArray();
            return db.SAdd(key, memArr);
        }
        else
        {
            var memArr = args.Skip(1).Select(m => ((YamlScalarNode)m).Value!).ToArray();
            return db.SAdd(key, memArr);
        }
    }

    // SREM: args = ["key", ["m1", "m2"]] or ["key", "m1", "m2"]
    private long SRemFromArgs(RedliteDb db, YamlSequenceNode args)
    {
        var key = GetScalarArg(args, 0);
        if (args[1] is YamlSequenceNode members)
        {
            var memArr = members.Select(m => ((YamlScalarNode)m).Value!).ToArray();
            return db.SRem(key, memArr);
        }
        else
        {
            var memArr = args.Skip(1).Select(m => ((YamlScalarNode)m).Value!).ToArray();
            return db.SRem(key, memArr);
        }
    }

    // ZADD: args = ["key", [[score, member], ...]]
    private long ZAddFromArgs(RedliteDb db, YamlSequenceNode args)
    {
        var key = GetScalarArg(args, 0);
        var members = new List<ZMember>();

        if (args[1] is YamlSequenceNode pairs)
        {
            foreach (YamlSequenceNode pair in pairs)
            {
                var score = double.Parse(((YamlScalarNode)pair[0]).Value!, CultureInfo.InvariantCulture);
                var member = ((YamlScalarNode)pair[1]).Value!;
                members.Add(new ZMember(score, member));
            }
        }
        return db.ZAdd(key, members.ToArray());
    }

    // ZREM: args = ["key", ["m1", "m2"]]
    private long ZRemFromArgs(RedliteDb db, YamlSequenceNode args)
    {
        var key = GetScalarArg(args, 0);
        if (args[1] is YamlSequenceNode members)
        {
            var memArr = members.Select(m => ((YamlScalarNode)m).Value!).ToArray();
            return db.ZRem(key, memArr);
        }
        else
        {
            var memArr = args.Skip(1).Select(m => ((YamlScalarNode)m).Value!).ToArray();
            return db.ZRem(key, memArr);
        }
    }

    private bool Compare(object? actual, YamlNode expected)
    {
        // Handle explicit null expectation
        if (expected is YamlScalarNode scalar)
        {
            var val = scalar.Value;

            // Null check - YAML null is represented as empty Value or literal "null"
            if (val == null || val == "null" || scalar.Tag == "tag:yaml.org,2002:null")
            {
                return actual == null;
            }

            // Empty string
            if (val == "")
            {
                if (actual is string s)
                    return s == "";
                return actual == null;
            }

            // Boolean
            if (val == "true") return actual is bool b && b || actual is long l && l != 0;
            if (val == "false") return actual is bool b2 && !b2 || actual is long l2 && l2 == 0;

            // Integer
            if (long.TryParse(val, out var expectedLong))
            {
                if (actual is long actualLong) return actualLong == expectedLong;
                if (actual is int actualInt) return actualInt == expectedLong;
            }

            // Float
            if (double.TryParse(val, NumberStyles.Any, CultureInfo.InvariantCulture, out var expectedDouble))
            {
                if (actual is double actualDouble)
                    return Math.Abs(actualDouble - expectedDouble) < 0.001;
            }

            // String
            if (actual is string actualStr)
                return actualStr == val;

            return false;
        }

        // Special expectations: {set: [...]}, {range: [a, b]}, {dict: {...}}, {bytes: [...]}
        if (expected is YamlMappingNode map)
            return CompareSpecial(actual, map);

        // Sequence (list)
        if (expected is YamlSequenceNode seq)
            return CompareSequence(actual, seq);

        return false;
    }

    private bool CompareSequence(object? actual, YamlSequenceNode expected)
    {
        // Handle string[] result
        if (actual is string[] actualArr)
        {
            if (actualArr.Length != expected.Children.Count) return false;
            for (int i = 0; i < expected.Children.Count; i++)
            {
                var node = expected[i];
                if (node is YamlScalarNode s)
                {
                    if (s.Value == null || s.Value == "null")
                    {
                        // String array can't contain null - check if it's empty?
                        continue;
                    }
                    if (actualArr[i] != s.Value) return false;
                }
            }
            return true;
        }

        // Handle string?[] result (from MGet, HMGet)
        if (actual is string?[] actualNullableArr)
        {
            if (actualNullableArr.Length != expected.Children.Count) return false;
            for (int i = 0; i < expected.Children.Count; i++)
            {
                var node = expected[i];
                if (node is YamlScalarNode s)
                {
                    if (s.Value == null || s.Value == "null")
                    {
                        if (actualNullableArr[i] != null) return false;
                    }
                    else
                    {
                        if (actualNullableArr[i] != s.Value) return false;
                    }
                }
            }
            return true;
        }

        return false;
    }

    private bool CompareSpecial(object? actual, YamlMappingNode expected)
    {
        // {set: ["a", "b"]} - unordered set comparison
        if (expected.Children.ContainsKey("set"))
        {
            if (actual is not string[] actualArr) return false;
            var actualSet = new HashSet<string>(actualArr);
            var expectedSet = new HashSet<string>();
            foreach (YamlScalarNode item in (YamlSequenceNode)expected["set"])
            {
                expectedSet.Add(item.Value!);
            }
            return actualSet.SetEquals(expectedSet);
        }

        // {dict: {"k": "v"}} - dictionary comparison
        if (expected.Children.ContainsKey("dict"))
        {
            if (actual is not Dictionary<string, string> actualDict) return false;
            var expectedDict = (YamlMappingNode)expected["dict"];
            if (actualDict.Count != expectedDict.Children.Count) return false;
            foreach (var pair in expectedDict)
            {
                var key = ((YamlScalarNode)pair.Key).Value!;
                var val = ((YamlScalarNode)pair.Value).Value!;
                if (!actualDict.TryGetValue(key, out var actualVal) || actualVal != val)
                    return false;
            }
            return true;
        }

        // {range: [min, max]} - numeric range
        if (expected.Children.ContainsKey("range"))
        {
            var range = (YamlSequenceNode)expected["range"];
            var min = long.Parse(((YamlScalarNode)range[0]).Value!);
            var max = long.Parse(((YamlScalarNode)range[1]).Value!);
            if (actual is long val)
                return val >= min && val <= max;
        }

        // {approx: val, tol: tolerance} - float with tolerance
        if (expected.Children.ContainsKey("approx"))
        {
            var expectedVal = double.Parse(((YamlScalarNode)expected["approx"]).Value!, CultureInfo.InvariantCulture);
            var tol = expected.Children.ContainsKey("tol")
                ? double.Parse(((YamlScalarNode)expected["tol"]).Value!, CultureInfo.InvariantCulture)
                : 0.001;
            if (actual is double actualVal)
                return Math.Abs(actualVal - expectedVal) <= tol;
        }

        // {bytes: [0, 1, 255]} - binary data comparison
        if (expected.Children.ContainsKey("bytes"))
        {
            var bytes = (YamlSequenceNode)expected["bytes"];
            var expectedBytes = bytes.Select(b => byte.Parse(((YamlScalarNode)b).Value!)).ToArray();
            var expectedStr = Encoding.Latin1.GetString(expectedBytes);

            if (actual is string actualStr)
                return actualStr == expectedStr;
            if (actual is byte[] actualBytes)
                return actualBytes.SequenceEqual(expectedBytes);
        }

        // {type: "integer"} - type check only
        if (expected.Children.ContainsKey("type"))
        {
            var expectedType = ((YamlScalarNode)expected["type"]).Value;
            return expectedType switch
            {
                "integer" => actual is long or int,
                "string" => actual is string,
                "bytes" => actual is byte[],
                "array" => actual is Array,
                "boolean" => actual is bool,
                _ => false
            };
        }

        return false;
    }

    private static string YamlToString(YamlNode node)
    {
        return node switch
        {
            YamlScalarNode s => s.Value ?? "null",
            YamlSequenceNode seq => "[" + string.Join(", ", seq.Select(YamlToString)) + "]",
            YamlMappingNode => "{...}",
            _ => "???"
        };
    }

    private static string ValueToString(object? val)
    {
        if (val == null) return "null";
        if (val is bool b) return b.ToString().ToLower();
        if (val is string s) return s == "" ? "(empty string)" : s;
        if (val is string[] arr) return "[" + string.Join(", ", arr) + "]";
        if (val is string?[] nArr) return "[" + string.Join(", ", nArr.Select(x => x ?? "null")) + "]";
        if (val is Dictionary<string, string> dict) return "{" + string.Join(", ", dict.Select(kv => $"{kv.Key}: {kv.Value}")) + "}";
        return val.ToString() ?? "null";
    }

    public void PrintSummary()
    {
        Console.WriteLine("\n=== Results ===");
        Console.WriteLine($"Passed: {_passed}");
        Console.WriteLine($"Failed: {_failed}");

        if (_errors.Count > 0)
        {
            Console.WriteLine("\nErrors:");
            foreach (var (name, error) in _errors)
            {
                Console.WriteLine($"  - {name}: {error}");
            }
        }
    }

    public int GetExitCode() => _failed > 0 ? 1 : 0;

    public static void Main(string[] args)
    {
        var verbose = false;
        var specFiles = new List<string>();

        foreach (var arg in args)
        {
            if (arg == "-v" || arg == "--verbose")
                verbose = true;
            else
                specFiles.Add(arg);
        }

        if (specFiles.Count == 0)
        {
            Console.Error.WriteLine("Usage: DotNetOracleRunner [-v] <spec.yaml> [spec2.yaml ...]");
            Console.Error.WriteLine("       DotNetOracleRunner [-v] ../spec/    (run all specs in directory)");
            Environment.Exit(1);
        }

        var runner = new OracleRunner(verbose);

        foreach (var path in specFiles)
        {
            if (Directory.Exists(path))
            {
                foreach (var file in Directory.GetFiles(path, "*.yaml"))
                {
                    runner.RunSpecFile(file);
                }
            }
            else
            {
                runner.RunSpecFile(path);
            }
        }

        runner.PrintSummary();
        Environment.Exit(runner.GetExitCode());
    }
}
