#!/usr/bin/env php
<?php
/**
 * Oracle Test Runner for PHP SDK.
 *
 * Executes YAML test specifications against the Redlite PHP SDK
 * and reports pass/fail results with detailed error messages.
 *
 * Usage:
 *     php php_runner.php                    # Run all specs
 *     php php_runner.php spec/strings.yaml  # Run single spec
 *     php php_runner.php -v                 # Verbose output
 */

declare(strict_types=1);

// Add the PHP SDK to path
require_once __DIR__ . '/../../redlite-php/vendor/autoload.php';

use Redlite\Redlite;

class OracleRunner
{
    private bool $verbose;
    private int $passed = 0;
    private int $failed = 0;
    /** @var array<array<string, mixed>> */
    private array $errors = [];

    public function __construct(bool $verbose = false)
    {
        $this->verbose = $verbose;
    }

    public function runSpecFile(string $specPath): bool
    {
        $content = file_get_contents($specPath);
        $spec = yaml_parse($content);

        $specName = $spec['name'] ?? basename($specPath);
        $tests = $spec['tests'] ?? [];

        if ($this->verbose) {
            echo "\n" . str_repeat('=', 60) . "\n";
            echo "Running: {$specName} (" . count($tests) . " tests)\n";
            echo str_repeat('=', 60) . "\n";
        }

        foreach ($tests as $test) {
            $this->runTest($test, $specName);
        }

        return count($this->errors) === 0;
    }

    private function runTest(array $test, string $specName): void
    {
        $testName = $test['name'] ?? 'unnamed';

        if ($this->verbose) {
            echo "\n  {$testName}... ";
        }

        // Create fresh in-memory database for each test
        $db = new Redlite(':memory:');

        try {
            // Run setup operations
            foreach ($test['setup'] ?? [] as $op) {
                $this->executeCmd($db, $op);
            }

            // Run test operations and check expectations
            foreach ($test['operations'] as $op) {
                $actual = $this->executeCmd($db, $op);
                $expected = $op['expect'] ?? null;

                if (!$this->compare($actual, $expected)) {
                    $this->failed++;
                    $this->errors[] = [
                        'spec' => $specName,
                        'test' => $testName,
                        'cmd' => $op['cmd'],
                        'args' => $op['args'] ?? [],
                        'expected' => $expected,
                        'actual' => $this->serialize($actual),
                    ];
                    if ($this->verbose) {
                        echo "FAILED\n";
                        echo "      Expected: " . json_encode($expected) . "\n";
                        echo "      Actual:   " . json_encode($this->serialize($actual)) . "\n";
                    }
                    return;
                }
            }

            $this->passed++;
            if ($this->verbose) {
                echo "PASSED\n";
            }

        } catch (\Throwable $e) {
            $this->failed++;
            $this->errors[] = [
                'spec' => $specName,
                'test' => $testName,
                'error' => $e->getMessage(),
            ];
            if ($this->verbose) {
                echo "ERROR: " . $e->getMessage() . "\n";
            }
        } finally {
            $db->close();
        }
    }

    /**
     * @return mixed
     */
    private function executeCmd(Redlite $db, array $op)
    {
        $cmd = strtolower($op['cmd']);
        $args = $op['args'] ?? [];
        $kwargs = $op['kwargs'] ?? [];

        // Process args to handle special types like bytes
        $args = array_map([$this, 'processArg'], $args);

        // Handle API differences between generic Redis spec and PHP SDK
        switch ($cmd) {
            case 'del':
            case 'exists':
            case 'mget':
                // Spec: DEL [["k1", "k2"]] -> PHP: delete("k1", "k2")
                if ($args && is_array($args[0])) {
                    $args = $args[0];
                }
                if ($cmd === 'del') {
                    return $db->delete(...$args);
                } elseif ($cmd === 'exists') {
                    return $db->exists(...$args);
                } else {
                    return $db->mget(...$args);
                }

            case 'mset':
                // Spec: MSET [["k1", "v1"], ["k2", "v2"]]
                // PHP: mset(["k1" => "v1", "k2" => "v2"])
                if ($args && is_array($args[0]) && count($args[0]) === 2) {
                    $mapping = [];
                    foreach ($args as $pair) {
                        $mapping[$pair[0]] = $pair[1];
                    }
                    return $db->mset($mapping);
                }
                return $db->mset($kwargs);

            case 'hset':
                // Spec: HSET ["hash", "field", "value"]
                // PHP: hset("hash", ["field" => "value"])
                if (count($args) === 3) {
                    [$key, $field, $value] = $args;
                    return $db->hset($key, [$field => $value]);
                }
                return $db->hset($args[0], $args[1]);

            case 'hdel':
                // Spec: HDEL ["hash", ["f1", "f2"]]
                // PHP: hdel("hash", "f1", "f2")
                if (count($args) === 2 && is_array($args[1])) {
                    [$key, $fields] = $args;
                    return $db->hdel($key, ...$fields);
                }
                return $db->hdel(...$args);

            case 'hmget':
                // Spec: HMGET ["hash", ["f1", "f2"]]
                // PHP: hmget("hash", "f1", "f2")
                if (count($args) === 2 && is_array($args[1])) {
                    [$key, $fields] = $args;
                    return $db->hmget($key, ...$fields);
                }
                return $db->hmget(...$args);

            case 'zadd':
                // Spec: ZADD ["zset", [[1.0, "member"]]]
                // PHP: zadd("zset", ["member" => 1.0])
                if (count($args) === 2 && is_array($args[1])) {
                    [$key, $members] = $args;
                    $mapping = [];
                    foreach ($members as $item) {
                        if (is_array($item) && count($item) === 2) {
                            [$score, $member] = $item;
                            $mapping[$member] = $score;
                        }
                    }
                    return $db->zadd($key, $mapping);
                }
                return $db->zadd($args[0], $args[1]);

            case 'zrem':
                // Spec: ZREM ["zset", ["m1", "m2"]]
                // PHP: zrem("zset", "m1", "m2")
                if (count($args) === 2 && is_array($args[1])) {
                    [$key, $members] = $args;
                    return $db->zrem($key, ...$members);
                }
                return $db->zrem(...$args);

            case 'sadd':
            case 'srem':
                // Spec: SADD ["set", ["m1", "m2"]]
                // PHP: sadd("set", "m1", "m2")
                if (count($args) === 2 && is_array($args[1])) {
                    [$key, $members] = $args;
                    return $cmd === 'sadd' ? $db->sadd($key, ...$members) : $db->srem($key, ...$members);
                }
                return $cmd === 'sadd' ? $db->sadd(...$args) : $db->srem(...$args);

            case 'lpush':
            case 'rpush':
                // Spec: LPUSH ["list", ["v1", "v2"]]
                // PHP: lpush("list", "v1", "v2")
                if (count($args) === 2 && is_array($args[1])) {
                    [$key, $values] = $args;
                    return $cmd === 'lpush' ? $db->lpush($key, ...$values) : $db->rpush($key, ...$values);
                }
                return $cmd === 'lpush' ? $db->lpush(...$args) : $db->rpush(...$args);

            // Standard commands
            default:
                $methodMap = [
                    // String commands
                    'get' => 'get',
                    'set' => 'set',
                    'setex' => 'setex',
                    'psetex' => 'psetex',
                    'getdel' => 'getdel',
                    'append' => 'append',
                    'strlen' => 'strlen',
                    'getrange' => 'getrange',
                    'setrange' => 'setrange',
                    'incr' => 'incr',
                    'decr' => 'decr',
                    'incrby' => 'incrby',
                    'decrby' => 'decrby',
                    'incrbyfloat' => 'incrbyfloat',
                    // Key commands
                    'type' => 'type',
                    'ttl' => 'ttl',
                    'pttl' => 'pttl',
                    'expire' => 'expire',
                    'pexpire' => 'pexpire',
                    'expireat' => 'expireat',
                    'pexpireat' => 'pexpireat',
                    'persist' => 'persist',
                    'rename' => 'rename',
                    'renamenx' => 'renamenx',
                    'keys' => 'keys',
                    'dbsize' => 'dbsize',
                    'flushdb' => 'flushdb',
                    // Hash commands
                    'hget' => 'hget',
                    'hexists' => 'hexists',
                    'hlen' => 'hlen',
                    'hkeys' => 'hkeys',
                    'hvals' => 'hvals',
                    'hincrby' => 'hincrby',
                    'hgetall' => 'hgetall',
                    // List commands
                    'lpop' => 'lpop',
                    'rpop' => 'rpop',
                    'llen' => 'llen',
                    'lrange' => 'lrange',
                    'lindex' => 'lindex',
                    // Set commands
                    'smembers' => 'smembers',
                    'sismember' => 'sismember',
                    'scard' => 'scard',
                    // Sorted set commands
                    'zscore' => 'zscore',
                    'zcard' => 'zcard',
                    'zcount' => 'zcount',
                    'zincrby' => 'zincrby',
                    'zrange' => 'zrange',
                    'zrevrange' => 'zrevrange',
                ];

                if (!isset($methodMap[$cmd])) {
                    throw new \RuntimeException("Unknown command: {$cmd}");
                }

                $method = $methodMap[$cmd];
                return $db->$method(...$args);
        }
    }

    /**
     * @param mixed $arg
     * @return mixed
     */
    private function processArg($arg)
    {
        if (is_array($arg)) {
            if (isset($arg['bytes'])) {
                // Convert bytes array to string
                return implode('', array_map('chr', $arg['bytes']));
            }
            return array_map([$this, 'processArg'], $arg);
        }
        return $arg;
    }

    /**
     * @param mixed $actual
     * @param mixed $expected
     */
    private function compare($actual, $expected): bool
    {
        if ($expected === null) {
            return $actual === null;
        }

        if (is_array($expected) && isset($expected['set'])) {
            return $this->compareSet($actual, $expected['set']);
        }

        if (is_array($expected) && isset($expected['dict'])) {
            return $this->compareDict($actual, $expected['dict']);
        }

        if (is_array($expected) && isset($expected['range'])) {
            [$low, $high] = $expected['range'];
            return $actual >= $low && $actual <= $high;
        }

        if (is_array($expected) && isset($expected['approx'])) {
            $target = $expected['approx'];
            $tol = $expected['tol'] ?? 0.001;
            return abs($actual - $target) <= $tol;
        }

        if (is_array($expected) && isset($expected['type'])) {
            return $this->compareType($actual, $expected['type']);
        }

        if (is_array($expected) && isset($expected['contains'])) {
            return strpos((string) $actual, $expected['contains']) !== false;
        }

        if (is_array($expected) && isset($expected['bytes'])) {
            $expBytes = implode('', array_map('chr', $expected['bytes']));
            return $actual === $expBytes;
        }

        if (is_bool($expected)) {
            return $actual === $expected;
        }

        if (is_int($expected)) {
            return $actual === $expected;
        }

        if (is_float($expected)) {
            return abs($actual - $expected) < 0.001;
        }

        if (is_string($expected)) {
            return (string) $actual === $expected;
        }

        if (is_array($expected)) {
            if (!is_array($actual)) {
                return false;
            }
            if (count($actual) !== count($expected)) {
                return false;
            }
            foreach ($expected as $i => $exp) {
                if (!$this->compare($actual[$i] ?? null, $exp)) {
                    return false;
                }
            }
            return true;
        }

        return $actual === $expected;
    }

    private function compareSet($actual, array $expected): bool
    {
        if (!is_array($actual)) {
            return false;
        }

        $actualSet = array_map('strval', $actual);
        $expectedSet = array_map('strval', $expected);

        sort($actualSet);
        sort($expectedSet);

        return $actualSet === $expectedSet;
    }

    private function compareDict($actual, array $expected): bool
    {
        if (!is_array($actual)) {
            return false;
        }

        ksort($actual);
        ksort($expected);

        return $actual == $expected;
    }

    /**
     * @param mixed $actual
     */
    private function compareType($actual, string $expectedType): bool
    {
        $typeMap = [
            'bytes' => 'string',
            'str' => 'string',
            'int' => 'integer',
            'float' => 'double',
            'list' => 'array',
            'dict' => 'array',
            'set' => 'array',
        ];

        $phpType = $typeMap[$expectedType] ?? $expectedType;
        return gettype($actual) === $phpType;
    }

    /**
     * @param mixed $value
     * @return mixed
     */
    private function serialize($value)
    {
        if (is_array($value)) {
            return array_map([$this, 'serialize'], $value);
        }
        return $value;
    }

    public function summary(): string
    {
        $total = $this->passed + $this->failed;
        return "{$this->passed}/{$total} passed, {$this->failed} failed";
    }

    /**
     * @return array<array<string, mixed>>
     */
    public function getErrors(): array
    {
        return $this->errors;
    }
}

function main(): void
{
    global $argv;

    $verbose = false;
    $specs = [];

    foreach (array_slice($argv, 1) as $arg) {
        if ($arg === '-v' || $arg === '--verbose') {
            $verbose = true;
        } else {
            $specs[] = $arg;
        }
    }

    $specDir = __DIR__ . '/../spec';

    if (empty($specs)) {
        $specs = glob("{$specDir}/*.yaml");
    }

    $runner = new OracleRunner($verbose);

    foreach ($specs as $specFile) {
        $runner->runSpecFile($specFile);
    }

    // Print summary
    echo "\n" . str_repeat('=', 60) . "\n";
    echo "Oracle Test Results: " . $runner->summary() . "\n";
    echo str_repeat('=', 60) . "\n";

    $errors = $runner->getErrors();
    if (!empty($errors)) {
        echo "\nFailures:\n";
        foreach ($errors as $err) {
            if (isset($err['error'])) {
                echo "  - {$err['spec']} / {$err['test']}: {$err['error']}\n";
            } else {
                echo "  - {$err['spec']} / {$err['test']} / {$err['cmd']}\n";
                echo "      Expected: " . json_encode($err['expected']) . "\n";
                echo "      Actual:   " . json_encode($err['actual']) . "\n";
            }
        }
        exit(1);
    }

    exit(0);
}

main();
