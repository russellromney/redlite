#!/usr/bin/env python3
"""
Oracle Test Runner for Python SDK.

Executes YAML test specifications against the Redlite Python SDK
and reports pass/fail results with detailed error messages.

Usage:
    python python_runner.py                    # Run all specs
    python python_runner.py spec/strings.yaml  # Run single spec
    python python_runner.py -v                 # Verbose output
"""

import argparse
import sys
from pathlib import Path
from typing import Any

import yaml

# Add the Python SDK to path
SDK_PATH = Path(__file__).parent.parent.parent / "redlite-python" / "src"
sys.path.insert(0, str(SDK_PATH))

from redlite import Redlite


class OracleRunner:
    """Executes oracle test specifications against the Python SDK."""

    def __init__(self, verbose: bool = False):
        self.verbose = verbose
        self.passed = 0
        self.failed = 0
        self.errors: list[dict] = []

    def run_spec_file(self, spec_path: Path) -> bool:
        """Run all tests in a specification file."""
        with open(spec_path) as f:
            spec = yaml.safe_load(f)

        spec_name = spec.get("name", spec_path.name)
        tests = spec.get("tests", [])

        if self.verbose:
            print(f"\n{'=' * 60}")
            print(f"Running: {spec_name} ({len(tests)} tests)")
            print("=" * 60)

        for test in tests:
            self._run_test(test, spec_name)

        return len(self.errors) == 0

    def _run_test(self, test: dict, spec_name: str) -> None:
        """Run a single test case."""
        test_name = test.get("name", "unnamed")

        if self.verbose:
            print(f"\n  {test_name}...", end=" ")

        # Create fresh in-memory database for each test
        db = Redlite(":memory:")

        try:
            # Run setup operations
            for op in test.get("setup", []):
                self._execute_cmd(db, op)

            # Run test operations and check expectations
            for op in test["operations"]:
                actual = self._execute_cmd(db, op)
                expected = op.get("expect")

                if not self._compare(actual, expected):
                    self.failed += 1
                    self.errors.append({
                        "spec": spec_name,
                        "test": test_name,
                        "cmd": op["cmd"],
                        "args": op.get("args", []),
                        "expected": expected,
                        "actual": self._serialize(actual),
                    })
                    if self.verbose:
                        print("FAILED")
                        print(f"      Expected: {expected}")
                        print(f"      Actual:   {self._serialize(actual)}")
                    return

            self.passed += 1
            if self.verbose:
                print("PASSED")

        except Exception as e:
            self.failed += 1
            self.errors.append({
                "spec": spec_name,
                "test": test_name,
                "error": str(e),
            })
            if self.verbose:
                print(f"ERROR: {e}")

        finally:
            db.close()

    def _execute_cmd(self, db: Redlite, op: dict) -> Any:
        """Execute a command against the database."""
        cmd = op["cmd"].lower()
        args = op.get("args", [])
        kwargs = op.get("kwargs", {})

        # Process args to handle special types like bytes
        args = [self._process_arg(a) for a in args]

        # Handle API differences between generic Redis spec and Python SDK
        # The spec uses Redis-style args, but Python SDK has pythonic APIs

        # Commands that take *args instead of list
        if cmd in ("del", "exists", "mget"):
            # Spec: DEL [["k1", "k2"]] -> Python: delete("k1", "k2")
            # Spec: MGET [["k1", "k2"]] -> Python: mget("k1", "k2")
            if args and isinstance(args[0], list):
                args = args[0]
            method = {"del": db.delete, "exists": db.exists, "mget": db.mget}[cmd]
            return method(*args)

        # MSET takes dict, not list of pairs
        if cmd == "mset":
            # Spec: MSET [["k1", "v1"], ["k2", "v2"]]
            # Python: mset({"k1": "v1", "k2": "v2"})
            # args = [["k1", "v1"], ["k2", "v2"], ...] where each is a [key, value] pair
            if args and all(isinstance(a, list) and len(a) == 2 for a in args):
                mapping = {k: v for k, v in args}
                return db.mset(mapping)
            return db.mset(**kwargs)

        # HSET takes (key, mapping) not (key, field, value)
        if cmd == "hset":
            # Spec: HSET ["hash", "field", "value"]
            # Python: hset("hash", {"field": "value"})
            if len(args) == 3:
                key, field, value = args
                return db.hset(key, {field: value})
            return db.hset(*args, **kwargs)

        # HDEL takes (key, *fields)
        if cmd == "hdel":
            # Spec: HDEL ["hash", ["f1", "f2"]]
            # Python: hdel("hash", "f1", "f2")
            if len(args) == 2 and isinstance(args[1], list):
                key, fields = args
                return db.hdel(key, *fields)
            return db.hdel(*args)

        # HMGET takes (key, *fields)
        if cmd == "hmget":
            # Spec: HMGET ["hash", ["f1", "f2"]]
            # Python: hmget("hash", "f1", "f2")
            if len(args) == 2 and isinstance(args[1], list):
                key, fields = args
                return db.hmget(key, *fields)
            return db.hmget(*args)

        # ZADD takes (key, {member: score}) not (key, [[score, member], ...])
        if cmd == "zadd":
            # Spec: ZADD ["zset", [[1.0, "member"]]]
            # Python: zadd("zset", {"member": 1.0})
            if len(args) == 2 and isinstance(args[1], list):
                key, members = args
                mapping = {}
                for item in members:
                    if isinstance(item, (list, tuple)) and len(item) == 2:
                        score, member = item
                        # Handle bytes member
                        if isinstance(member, bytes):
                            member = member.decode("utf-8")
                        mapping[member] = score
                return db.zadd(key, mapping)
            return db.zadd(*args, **kwargs)

        # ZREM takes (key, *members)
        if cmd == "zrem":
            # Spec: ZREM ["zset", ["m1", "m2"]]
            # Python: zrem("zset", "m1", "m2")
            if len(args) == 2 and isinstance(args[1], list):
                key, members = args
                return db.zrem(key, *members)
            return db.zrem(*args)

        # SADD/SREM take (key, *members)
        if cmd in ("sadd", "srem"):
            # Spec: SADD ["set", ["m1", "m2"]]
            # Python: sadd("set", "m1", "m2")
            if len(args) == 2 and isinstance(args[1], list):
                key, members = args
                method = db.sadd if cmd == "sadd" else db.srem
                return method(key, *members)
            method = db.sadd if cmd == "sadd" else db.srem
            return method(*args)

        # LPUSH/RPUSH take (key, *values)
        if cmd in ("lpush", "rpush"):
            # Spec: LPUSH ["list", ["v1", "v2"]]
            # Python: lpush("list", "v1", "v2")
            if len(args) == 2 and isinstance(args[1], list):
                key, values = args
                method = db.lpush if cmd == "lpush" else db.rpush
                return method(key, *values)
            method = db.lpush if cmd == "lpush" else db.rpush
            return method(*args)

        # Standard method map for remaining commands
        method_map = {
            # String commands
            "get": db.get,
            "set": db.set,
            "setex": db.setex,
            "psetex": db.psetex,
            "getdel": db.getdel,
            "append": db.append,
            "strlen": db.strlen,
            "getrange": db.getrange,
            "setrange": db.setrange,
            "incr": db.incr,
            "decr": db.decr,
            "incrby": db.incrby,
            "decrby": db.decrby,
            "incrbyfloat": db.incrbyfloat,
            # Key commands
            "type": db.type,
            "ttl": db.ttl,
            "pttl": db.pttl,
            "expire": db.expire,
            "pexpire": db.pexpire,
            "expireat": db.expireat,
            "pexpireat": db.pexpireat,
            "persist": db.persist,
            "rename": db.rename,
            "renamenx": db.renamenx,
            "keys": db.keys,
            "dbsize": db.dbsize,
            "flushdb": db.flushdb,
            # Hash commands
            "hget": db.hget,
            "hexists": db.hexists,
            "hlen": db.hlen,
            "hkeys": db.hkeys,
            "hvals": db.hvals,
            "hincrby": db.hincrby,
            "hgetall": db.hgetall,
            # List commands
            "lpop": db.lpop,
            "rpop": db.rpop,
            "llen": db.llen,
            "lrange": db.lrange,
            "lindex": db.lindex,
            # Set commands
            "smembers": db.smembers,
            "sismember": db.sismember,
            "scard": db.scard,
            # Sorted set commands
            "zscore": db.zscore,
            "zcard": db.zcard,
            "zcount": db.zcount,
            "zincrby": db.zincrby,
            "zrange": db.zrange,
            "zrevrange": db.zrevrange,
        }

        if cmd not in method_map:
            raise ValueError(f"Unknown command: {cmd}")

        method = method_map[cmd]
        return method(*args, **kwargs)

    def _process_arg(self, arg: Any) -> Any:
        """Process argument, handling special types."""
        if isinstance(arg, dict):
            if "bytes" in arg:
                return bytes(arg["bytes"])
        if isinstance(arg, list):
            # Could be list of args or list of tuples for mset/zadd
            return [self._process_arg(a) for a in arg]
        return arg

    def _compare(self, actual: Any, expected: Any) -> bool:
        """Compare actual result with expected value."""
        if expected is None:
            return actual is None

        if isinstance(expected, dict):
            return self._compare_special(actual, expected)

        if isinstance(expected, bool):
            return actual is expected or actual == expected

        if isinstance(expected, int):
            return actual == expected

        if isinstance(expected, float):
            return abs(actual - expected) < 0.001

        if isinstance(expected, str):
            # String comparison - actual might be bytes
            if isinstance(actual, bytes):
                return actual.decode("utf-8", errors="replace") == expected
            return str(actual) == expected

        if isinstance(expected, list):
            if not isinstance(actual, (list, tuple)):
                return False
            if len(actual) != len(expected):
                return False
            return all(self._compare(a, e) for a, e in zip(actual, expected))

        return actual == expected

    def _compare_special(self, actual: Any, expected: dict) -> bool:
        """Compare with special expectation types."""
        if "bytes" in expected:
            exp_bytes = bytes(expected["bytes"])
            if isinstance(actual, bytes):
                return actual == exp_bytes
            return False

        if "set" in expected:
            # Unordered set comparison
            exp_set = set(expected["set"])
            if isinstance(actual, set):
                # Convert bytes to strings for comparison
                actual_set = {
                    v.decode("utf-8") if isinstance(v, bytes) else v for v in actual
                }
                return actual_set == exp_set
            if isinstance(actual, (list, tuple)):
                actual_set = {
                    v.decode("utf-8") if isinstance(v, bytes) else v for v in actual
                }
                return actual_set == exp_set
            return False

        if "dict" in expected:
            # Dictionary comparison
            exp_dict = expected["dict"]
            if isinstance(actual, dict):
                # Convert bytes keys/values to strings
                actual_dict = {}
                for k, v in actual.items():
                    k = k.decode("utf-8") if isinstance(k, bytes) else k
                    v = v.decode("utf-8") if isinstance(v, bytes) else v
                    actual_dict[k] = v
                return actual_dict == exp_dict
            return False

        if "range" in expected:
            # Numeric range comparison
            low, high = expected["range"]
            return low <= actual <= high

        if "approx" in expected:
            # Float approximation
            target = expected["approx"]
            tol = expected.get("tol", 0.001)
            return abs(actual - target) <= tol

        if "type" in expected:
            # Type check only
            type_map = {
                "bytes": bytes,
                "str": str,
                "int": int,
                "float": float,
                "list": (list, tuple),
                "dict": dict,
                "set": set,
            }
            exp_type = type_map.get(expected["type"])
            return isinstance(actual, exp_type)

        if "contains" in expected:
            # Substring match
            return expected["contains"] in str(actual)

        return False

    def _serialize(self, value: Any) -> Any:
        """Serialize value for error reporting."""
        if isinstance(value, bytes):
            try:
                return value.decode("utf-8")
            except UnicodeDecodeError:
                return f"<bytes: {list(value)}>"
        if isinstance(value, (list, tuple)):
            return [self._serialize(v) for v in value]
        if isinstance(value, dict):
            return {self._serialize(k): self._serialize(v) for k, v in value.items()}
        if isinstance(value, set):
            return {self._serialize(v) for v in value}
        return value

    def summary(self) -> str:
        """Return test summary."""
        total = self.passed + self.failed
        return f"{self.passed}/{total} passed, {self.failed} failed"


def main():
    parser = argparse.ArgumentParser(description="Run oracle tests for Python SDK")
    parser.add_argument("specs", nargs="*", help="Spec files to run (default: all)")
    parser.add_argument("-v", "--verbose", action="store_true", help="Verbose output")
    args = parser.parse_args()

    spec_dir = Path(__file__).parent.parent / "spec"

    if args.specs:
        spec_files = [Path(s) for s in args.specs]
    else:
        spec_files = sorted(spec_dir.glob("*.yaml"))

    runner = OracleRunner(verbose=args.verbose)

    for spec_file in spec_files:
        runner.run_spec_file(spec_file)

    # Print summary
    print(f"\n{'=' * 60}")
    print(f"Oracle Test Results: {runner.summary()}")
    print("=" * 60)

    if runner.errors:
        print("\nFailures:")
        for err in runner.errors:
            if "error" in err:
                print(f"  - {err['spec']} / {err['test']}: {err['error']}")
            else:
                print(f"  - {err['spec']} / {err['test']} / {err['cmd']}")
                print(f"      Expected: {err['expected']}")
                print(f"      Actual:   {err['actual']}")
        sys.exit(1)

    sys.exit(0)


if __name__ == "__main__":
    main()
