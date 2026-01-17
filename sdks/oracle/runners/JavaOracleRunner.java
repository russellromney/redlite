package com.redlite.oracle;

import com.redlite.Redlite;
import com.redlite.ZMember;
import org.yaml.snakeyaml.Yaml;

import java.io.File;
import java.io.FileReader;
import java.nio.charset.StandardCharsets;
import java.util.*;

/**
 * Oracle Test Runner for Java SDK.
 *
 * Executes YAML test specifications against the Redlite Java SDK
 * and reports pass/fail results with detailed error messages.
 *
 * Usage:
 *     java JavaOracleRunner                    # Run all specs
 *     java JavaOracleRunner spec/strings.yaml  # Run single spec
 *     java JavaOracleRunner -v                 # Verbose output
 */
public class JavaOracleRunner {
    private final boolean verbose;
    private int passed = 0;
    private int failed = 0;
    private int skipped = 0;
    private final List<TestError> errors = new ArrayList<>();

    record TestError(String spec, String test, String cmd, Object expected, Object actual) {}

    public JavaOracleRunner(boolean verbose) {
        this.verbose = verbose;
    }

    public boolean runSpecFile(String specPath) throws Exception {
        Yaml yaml = new Yaml();
        @SuppressWarnings("unchecked")
        Map<String, Object> spec = yaml.load(new FileReader(specPath));
        String specName = (String) spec.getOrDefault("name", new File(specPath).getName());
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> tests = (List<Map<String, Object>>) spec.getOrDefault("tests", List.of());

        if (verbose) {
            System.out.println("=".repeat(60));
            System.out.println("Running: " + specName + " (" + tests.size() + " tests)");
            System.out.println("=".repeat(60));
        }

        for (var test : tests) {
            runTest(test, specName);
        }

        return errors.isEmpty();
    }

    @SuppressWarnings("unchecked")
    private void runTest(Map<String, Object> test, String specName) {
        String testName = (String) test.getOrDefault("name", "unnamed");

        if (verbose) {
            System.out.print("  " + testName + "... ");
        }

        try (Redlite db = new Redlite(":memory:")) {
            // Run setup operations
            var setup = (List<Map<String, Object>>) test.get("setup");
            if (setup != null) {
                for (var op : setup) {
                    executeCmd(db, op);
                }
            }

            // Run test operations and check expectations
            var operations = (List<Map<String, Object>>) test.getOrDefault("operations", List.of());
            for (var op : operations) {
                Object actual = executeCmd(db, op);
                Object expected = op.get("expect");

                if (!compare(actual, expected)) {
                    failed++;
                    errors.add(new TestError(
                            specName,
                            testName,
                            (String) op.getOrDefault("cmd", "unknown"),
                            expected,
                            serialize(actual)
                    ));
                    if (verbose) {
                        System.out.println("FAILED");
                        System.out.println("      Expected: " + expected);
                        System.out.println("      Actual:   " + serialize(actual));
                    }
                    return;
                }
            }

            passed++;
            if (verbose) {
                System.out.println("PASSED");
            }
        } catch (Exception e) {
            failed++;
            errors.add(new TestError(
                    specName,
                    testName,
                    "unknown",
                    null,
                    "ERROR: " + e.getMessage()
            ));
            if (verbose) {
                System.out.println("ERROR: " + e.getMessage());
            }
        }
    }

    @SuppressWarnings("unchecked")
    private Object executeCmd(Redlite db, Map<String, Object> op) {
        String cmd = ((String) op.getOrDefault("cmd", "")).toLowerCase();
        List<Object> args = (List<Object>) op.getOrDefault("args", List.of());
        List<Object> processedArgs = args.stream().map(this::processArg).toList();

        return switch (cmd) {
            // String commands
            case "get" -> {
                byte[] result = db.get((String) processedArgs.get(0));
                yield result != null ? new String(result, StandardCharsets.UTF_8) : null;
            }
            case "set" -> {
                String key = (String) processedArgs.get(0);
                byte[] value = toBytes(processedArgs.get(1));
                yield db.set(key, value);
            }
            case "setex" -> {
                String key = (String) processedArgs.get(0);
                long seconds = ((Number) processedArgs.get(1)).longValue();
                byte[] value = toBytes(processedArgs.get(2));
                yield db.setex(key, seconds, value);
            }
            case "incr" -> db.incr((String) processedArgs.get(0));
            case "decr" -> db.decr((String) processedArgs.get(0));
            case "incrby" -> db.incrby((String) processedArgs.get(0), ((Number) processedArgs.get(1)).longValue());
            case "decrby" -> db.decrby((String) processedArgs.get(0), ((Number) processedArgs.get(1)).longValue());
            case "incrbyfloat" -> db.incrbyfloat((String) processedArgs.get(0), ((Number) processedArgs.get(1)).doubleValue());
            case "append" -> db.append((String) processedArgs.get(0), toBytes(processedArgs.get(1)));
            case "strlen" -> db.strlen((String) processedArgs.get(0));
            case "getrange" -> {
                byte[] result = db.getrange(
                        (String) processedArgs.get(0),
                        ((Number) processedArgs.get(1)).longValue(),
                        ((Number) processedArgs.get(2)).longValue()
                );
                yield new String(result, StandardCharsets.UTF_8);
            }

            // Key commands
            case "del", "delete" -> {
                String[] keys = getStringArray(processedArgs, 0);
                yield db.delete(keys);
            }
            case "exists" -> {
                String[] keys = getStringArray(processedArgs, 0);
                yield db.exists(keys);
            }
            case "type" -> db.type((String) processedArgs.get(0));
            case "ttl" -> db.ttl((String) processedArgs.get(0));
            case "pttl" -> db.pttl((String) processedArgs.get(0));
            case "expire" -> db.expire((String) processedArgs.get(0), ((Number) processedArgs.get(1)).longValue());
            case "persist" -> db.persist((String) processedArgs.get(0));
            case "rename" -> db.rename((String) processedArgs.get(0), (String) processedArgs.get(1));
            case "keys" -> db.keys(processedArgs.isEmpty() ? "*" : (String) processedArgs.get(0));
            case "dbsize" -> db.dbsize();
            case "flushdb" -> db.flushdb();

            // Hash commands
            case "hset" -> {
                String key = (String) processedArgs.get(0);
                String field = (String) processedArgs.get(1);
                byte[] value = toBytes(processedArgs.get(2));
                yield db.hset(key, field, value);
            }
            case "hget" -> {
                byte[] result = db.hget((String) processedArgs.get(0), (String) processedArgs.get(1));
                yield result != null ? new String(result, StandardCharsets.UTF_8) : null;
            }
            case "hdel" -> {
                String key = (String) processedArgs.get(0);
                String[] fields = getStringArray(processedArgs, 1);
                yield db.hdel(key, fields);
            }
            case "hexists" -> db.hexists((String) processedArgs.get(0), (String) processedArgs.get(1));
            case "hlen" -> db.hlen((String) processedArgs.get(0));
            case "hkeys" -> db.hkeys((String) processedArgs.get(0));
            case "hincrby" -> db.hincrby(
                    (String) processedArgs.get(0),
                    (String) processedArgs.get(1),
                    ((Number) processedArgs.get(2)).longValue()
            );
            case "hgetall" -> {
                Map<String, byte[]> result = db.hgetall((String) processedArgs.get(0));
                Map<String, String> stringMap = new LinkedHashMap<>();
                result.forEach((k, v) -> stringMap.put(k, new String(v, StandardCharsets.UTF_8)));
                yield stringMap;
            }

            // List commands
            case "lpush" -> {
                String key = (String) processedArgs.get(0);
                byte[][] values = getByteArrays(processedArgs.get(1));
                yield db.lpush(key, values);
            }
            case "rpush" -> {
                String key = (String) processedArgs.get(0);
                byte[][] values = getByteArrays(processedArgs.get(1));
                yield db.rpush(key, values);
            }
            case "lpop" -> {
                int count = processedArgs.size() > 1 ? ((Number) processedArgs.get(1)).intValue() : 1;
                List<byte[]> result = db.lpop((String) processedArgs.get(0), count);
                if (count == 1) {
                    yield result.isEmpty() ? null : new String(result.get(0), StandardCharsets.UTF_8);
                }
                yield result.stream().map(b -> new String(b, StandardCharsets.UTF_8)).toList();
            }
            case "rpop" -> {
                int count = processedArgs.size() > 1 ? ((Number) processedArgs.get(1)).intValue() : 1;
                List<byte[]> result = db.rpop((String) processedArgs.get(0), count);
                if (count == 1) {
                    yield result.isEmpty() ? null : new String(result.get(0), StandardCharsets.UTF_8);
                }
                yield result.stream().map(b -> new String(b, StandardCharsets.UTF_8)).toList();
            }
            case "llen" -> db.llen((String) processedArgs.get(0));
            case "lrange" -> {
                List<byte[]> result = db.lrange(
                        (String) processedArgs.get(0),
                        ((Number) processedArgs.get(1)).longValue(),
                        ((Number) processedArgs.get(2)).longValue()
                );
                yield result.stream().map(b -> new String(b, StandardCharsets.UTF_8)).toList();
            }
            case "lindex" -> {
                byte[] result = db.lindex((String) processedArgs.get(0), ((Number) processedArgs.get(1)).longValue());
                yield result != null ? new String(result, StandardCharsets.UTF_8) : null;
            }

            // Set commands
            case "sadd" -> {
                String key = (String) processedArgs.get(0);
                byte[][] members = getByteArrays(processedArgs.get(1));
                yield db.sadd(key, members);
            }
            case "srem" -> {
                String key = (String) processedArgs.get(0);
                byte[][] members = getByteArrays(processedArgs.get(1));
                yield db.srem(key, members);
            }
            case "smembers" -> {
                Set<byte[]> result = db.smembers((String) processedArgs.get(0));
                yield result.stream().map(b -> new String(b, StandardCharsets.UTF_8)).collect(java.util.stream.Collectors.toSet());
            }
            case "sismember" -> db.sismember((String) processedArgs.get(0), toBytes(processedArgs.get(1)));
            case "scard" -> db.scard((String) processedArgs.get(0));

            // Sorted set commands
            case "zadd" -> {
                String key = (String) processedArgs.get(0);
                List<?> members = (List<?>) processedArgs.get(1);
                ZMember[] zMembers = members.stream().map(item -> {
                    List<?> pair = (List<?>) item;
                    double score = ((Number) pair.get(0)).doubleValue();
                    byte[] member = toBytes(pair.get(1));
                    return new ZMember(score, member);
                }).toArray(ZMember[]::new);
                yield db.zadd(key, zMembers);
            }
            case "zscore" -> db.zscore((String) processedArgs.get(0), toBytes(processedArgs.get(1)));
            case "zcard" -> db.zcard((String) processedArgs.get(0));
            case "zcount" -> db.zcount(
                    (String) processedArgs.get(0),
                    ((Number) processedArgs.get(1)).doubleValue(),
                    ((Number) processedArgs.get(2)).doubleValue()
            );
            case "zrange" -> {
                List<byte[]> result = db.zrange(
                        (String) processedArgs.get(0),
                        ((Number) processedArgs.get(1)).longValue(),
                        ((Number) processedArgs.get(2)).longValue()
                );
                yield result.stream().map(b -> new String(b, StandardCharsets.UTF_8)).toList();
            }

            // Multi-key commands
            case "mget" -> {
                String[] keys = getStringArray(processedArgs, 0);
                List<byte[]> result = db.mget(keys);
                yield result.stream().map(b -> b != null ? new String(b, StandardCharsets.UTF_8) : null).toList();
            }
            case "mset" -> {
                List<?> pairs = (List<?>) processedArgs.get(0);
                if (pairs.get(0) instanceof List) {
                    Map<String, byte[]> map = new LinkedHashMap<>();
                    for (Object pair : pairs) {
                        List<?> p = (List<?>) pair;
                        map.put((String) p.get(0), toBytes(p.get(1)));
                    }
                    yield db.mset(map);
                }
                // Handle flat list format: process all items as key-value pairs
                Map<String, byte[]> map = new LinkedHashMap<>();
                for (int i = 0; i < processedArgs.size(); i++) {
                    List<?> pair = (List<?>) processedArgs.get(i);
                    map.put((String) pair.get(0), toBytes(pair.get(1)));
                }
                yield db.mset(map);
            }

            default -> throw new IllegalArgumentException("Unknown command: " + cmd);
        };
    }

    @SuppressWarnings("unchecked")
    private Object processArg(Object arg) {
        if (arg instanceof Map<?, ?> map) {
            if (map.containsKey("bytes")) {
                List<Number> bytes = (List<Number>) map.get("bytes");
                byte[] result = new byte[bytes.size()];
                for (int i = 0; i < bytes.size(); i++) {
                    result[i] = bytes.get(i).byteValue();
                }
                return result;
            }
        }
        return arg;
    }

    private byte[] toBytes(Object value) {
        if (value instanceof byte[] b) return b;
        if (value instanceof String s) return s.getBytes(StandardCharsets.UTF_8);
        return value.toString().getBytes(StandardCharsets.UTF_8);
    }

    @SuppressWarnings("unchecked")
    private String[] getStringArray(List<Object> args, int index) {
        Object arg = args.get(index);
        if (arg instanceof List<?> list) {
            return list.stream().map(Object::toString).toArray(String[]::new);
        }
        return new String[]{arg.toString()};
    }

    @SuppressWarnings("unchecked")
    private byte[][] getByteArrays(Object arg) {
        if (arg instanceof List<?> list) {
            return list.stream().map(this::toBytes).toArray(byte[][]::new);
        }
        return new byte[][]{toBytes(arg)};
    }

    @SuppressWarnings("unchecked")
    private boolean compare(Object actual, Object expected) {
        if (expected == null) {
            return actual == null || (actual instanceof byte[] b && b.length == 0);
        }

        if (expected instanceof Map<?, ?> map) {
            return compareSpecial(actual, map);
        }
        if (expected instanceof Boolean b) {
            return actual != null && actual.equals(b);
        }
        if (expected instanceof Number n) {
            return compareNumber(actual, n);
        }
        if (expected instanceof String s) {
            return compareString(actual, s);
        }
        if (expected instanceof List<?> list) {
            return compareList(actual, list);
        }

        return Objects.equals(actual, expected);
    }

    private boolean compareSpecial(Object actual, Map<?, ?> expected) {
        if (expected.containsKey("range")) {
            @SuppressWarnings("unchecked")
            List<Number> bounds = (List<Number>) expected.get("range");
            if (!(actual instanceof Number n)) return false;
            long value = n.longValue();
            return value >= bounds.get(0).longValue() && value <= bounds.get(1).longValue();
        }
        if (expected.containsKey("approx")) {
            double target = ((Number) expected.get("approx")).doubleValue();
            double tol = expected.containsKey("tol") ? ((Number) expected.get("tol")).doubleValue() : 0.001;
            if (!(actual instanceof Number n)) return false;
            return Math.abs(n.doubleValue() - target) <= tol;
        }
        if (expected.containsKey("set")) {
            @SuppressWarnings("unchecked")
            Set<String> expSet = new HashSet<>(((List<?>) expected.get("set")).stream().map(Object::toString).toList());
            Set<String> actSet;
            if (actual instanceof Set<?> s) {
                actSet = s.stream().map(o -> o instanceof byte[] b ? new String(b, StandardCharsets.UTF_8) : o.toString()).collect(java.util.stream.Collectors.toSet());
            } else if (actual instanceof List<?> l) {
                actSet = l.stream().map(o -> o instanceof byte[] b ? new String(b, StandardCharsets.UTF_8) : o.toString()).collect(java.util.stream.Collectors.toSet());
            } else {
                return false;
            }
            return actSet.equals(expSet);
        }
        if (expected.containsKey("dict")) {
            @SuppressWarnings("unchecked")
            Map<?, ?> expDict = (Map<?, ?>) expected.get("dict");
            if (!(actual instanceof Map<?, ?> actDict)) return false;
            for (var entry : expDict.entrySet()) {
                Object actVal = actDict.get(entry.getKey());
                String expStr = entry.getValue().toString();
                String actStr = actVal instanceof byte[] b ? new String(b, StandardCharsets.UTF_8) : (actVal != null ? actVal.toString() : null);
                if (!Objects.equals(actStr, expStr)) return false;
            }
            return true;
        }
        if (expected.containsKey("bytes")) {
            @SuppressWarnings("unchecked")
            List<Number> expBytes = (List<Number>) expected.get("bytes");
            if (!(actual instanceof byte[] actBytes)) return false;
            if (actBytes.length != expBytes.size()) return false;
            for (int i = 0; i < expBytes.size(); i++) {
                if (actBytes[i] != expBytes.get(i).byteValue()) return false;
            }
            return true;
        }
        return false;
    }

    private boolean compareString(Object actual, String expected) {
        if (actual instanceof byte[] b) {
            return new String(b, StandardCharsets.UTF_8).equals(expected);
        }
        if (actual instanceof String s) {
            return s.equals(expected);
        }
        return false;
    }

    private boolean compareNumber(Object actual, Number expected) {
        if (actual instanceof Number n) {
            return n.longValue() == expected.longValue();
        }
        return false;
    }

    private boolean compareList(Object actual, List<?> expected) {
        if (!(actual instanceof List<?> actList)) return false;
        if (actList.size() != expected.size()) return false;
        for (int i = 0; i < expected.size(); i++) {
            if (!compare(actList.get(i), expected.get(i))) return false;
        }
        return true;
    }

    private Object serialize(Object value) {
        if (value instanceof byte[] b) return new String(b, StandardCharsets.UTF_8);
        if (value instanceof List<?> l) return l.stream().map(this::serialize).toList();
        if (value instanceof Set<?> s) return s.stream().map(this::serialize).collect(java.util.stream.Collectors.toSet());
        if (value instanceof Map<?, ?> m) {
            Map<Object, Object> result = new LinkedHashMap<>();
            m.forEach((k, v) -> result.put(k, serialize(v)));
            return result;
        }
        return value;
    }

    public String summary() {
        int total = passed + failed;
        if (skipped > 0) {
            return passed + "/" + total + " passed, " + failed + " failed, " + skipped + " skipped";
        }
        return passed + "/" + total + " passed, " + failed + " failed";
    }

    public void printErrors() {
        if (!errors.isEmpty()) {
            System.out.println("\nFailed tests:");
            for (var error : errors) {
                System.out.println("  " + error.spec + " / " + error.test);
                System.out.println("    Command: " + error.cmd);
                System.out.println("    Expected: " + error.expected);
                System.out.println("    Actual: " + error.actual);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        boolean verbose = Arrays.asList(args).contains("-v") || Arrays.asList(args).contains("--verbose");
        List<String> specFiles = Arrays.stream(args)
                .filter(a -> !a.startsWith("-"))
                .toList();

        if (specFiles.isEmpty()) {
            File specDir = new File("../spec");
            if (specDir.exists()) {
                specFiles = Arrays.stream(Objects.requireNonNull(specDir.listFiles()))
                        .filter(f -> f.getName().endsWith(".yaml"))
                        .map(File::getPath)
                        .toList();
            }
        }

        JavaOracleRunner runner = new JavaOracleRunner(verbose);
        for (String specFile : specFiles) {
            runner.runSpecFile(specFile);
        }

        System.out.println("=".repeat(60));
        System.out.println("Oracle Test Results: " + runner.summary());
        System.out.println("=".repeat(60));
        runner.printErrors();

        System.exit(runner.summary().contains("0 failed") ? 0 : 1);
    }
}
