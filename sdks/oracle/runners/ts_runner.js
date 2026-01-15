#!/usr/bin/env node
/**
 * Oracle Test Runner for TypeScript SDK.
 *
 * Executes YAML test specifications against the Redlite TypeScript SDK
 * and reports pass/fail results with detailed error messages.
 *
 * Usage:
 *   node ts_runner.js                    # Run all specs
 *   node ts_runner.js spec/strings.yaml  # Run single spec
 *   node ts_runner.js -v                 # Verbose output
 */

const fs = require("fs");
const path = require("path");
const yaml = require("yaml");

// Import the SDK - path relative to oracle/runners/
const { RedliteDb } = require("../../redlite-ts/index.js");

class OracleRunner {
  constructor(verbose = false) {
    this.verbose = verbose;
    this.passed = 0;
    this.failed = 0;
    this.errors = [];
  }

  runSpecFile(specPath) {
    const content = fs.readFileSync(specPath, "utf-8");
    const spec = yaml.parse(content);
    const specName = spec.name || path.basename(specPath);
    const tests = spec.tests || [];

    if (this.verbose) {
      console.log(`\n${"=".repeat(60)}`);
      console.log(`Running: ${specName} (${tests.length} tests)`);
      console.log("=".repeat(60));
    }

    for (const test of tests) {
      this.runTest(test, specName);
    }

    return this.errors.length === 0;
  }

  runTest(test, specName) {
    const testName = test.name || "unnamed";

    if (this.verbose) {
      process.stdout.write(`\n  ${testName}... `);
    }

    // Create fresh in-memory database for each test
    const db = RedliteDb.openMemory();

    try {
      // Run setup operations
      for (const op of test.setup || []) {
        this.executeCmd(db, op);
      }

      // Run test operations and check expectations
      for (const op of test.operations) {
        const actual = this.executeCmd(db, op);
        const expected = op.expect;

        if (!this.compare(actual, expected)) {
          this.failed++;
          this.errors.push({
            spec: specName,
            test: testName,
            cmd: op.cmd,
            args: op.args || [],
            expected,
            actual: this.serialize(actual),
          });
          if (this.verbose) {
            console.log("FAILED");
            console.log(`      Expected: ${JSON.stringify(expected)}`);
            console.log(`      Actual:   ${JSON.stringify(this.serialize(actual))}`);
          }
          return;
        }
      }

      this.passed++;
      if (this.verbose) {
        console.log("PASSED");
      }
    } catch (e) {
      this.failed++;
      this.errors.push({
        spec: specName,
        test: testName,
        error: e.message || String(e),
      });
      if (this.verbose) {
        console.log(`ERROR: ${e.message || e}`);
      }
    }
  }

  executeCmd(db, op) {
    const cmd = op.cmd.toLowerCase();
    let args = (op.args || []).map((a) => this.processArg(a));

    // Handle API differences between generic Redis spec and TypeScript SDK

    // DEL/EXISTS take array of keys
    if (cmd === "del" || cmd === "exists") {
      if (args.length === 1 && Array.isArray(args[0])) {
        args = args;
      }
      return cmd === "del" ? db.del(args[0]) : db.exists(args[0]);
    }

    // MGET takes array of keys
    if (cmd === "mget") {
      const keys = args.length === 1 && Array.isArray(args[0]) ? args[0] : args;
      return db.mget(keys);
    }

    // MSET takes array of [key, value] pairs as buffers
    if (cmd === "mset") {
      const pairs = args.map((pair) => {
        return [Buffer.from(String(pair[0])), Buffer.from(String(pair[1]))];
      });
      return db.mset(pairs);
    }

    // HSET takes (key, field, value)
    if (cmd === "hset") {
      const [key, field, value] = args;
      return db.hset(key, field, Buffer.from(String(value)));
    }

    // HDEL takes (key, fields[])
    if (cmd === "hdel") {
      const [key, fields] = args;
      return db.hdel(key, Array.isArray(fields) ? fields : [fields]);
    }

    // HMGET takes (key, fields[])
    if (cmd === "hmget") {
      const [key, fields] = args;
      return db.hmget(key, Array.isArray(fields) ? fields : [fields]);
    }

    // ZADD takes (key, ZMember[])
    if (cmd === "zadd") {
      const [key, members] = args;
      const zmembers = members.map((m) => ({
        score: m[0],
        member: Buffer.from(String(m[1])),
      }));
      return db.zadd(key, zmembers);
    }

    // ZREM takes (key, members[])
    if (cmd === "zrem") {
      const [key, members] = args;
      return db.zrem(
        key,
        members.map((m) => Buffer.from(String(m)))
      );
    }

    // SADD/SREM take (key, members[])
    if (cmd === "sadd" || cmd === "srem") {
      const [key, members] = args;
      const bufMembers = (Array.isArray(members) ? members : [members]).map(
        (m) => Buffer.from(String(m))
      );
      return cmd === "sadd" ? db.sadd(key, bufMembers) : db.srem(key, bufMembers);
    }

    // LPUSH/RPUSH take (key, values[])
    if (cmd === "lpush" || cmd === "rpush") {
      const [key, values] = args;
      const bufValues = (Array.isArray(values) ? values : [values]).map(
        (v) => Buffer.from(String(v))
      );
      return cmd === "lpush" ? db.lpush(key, bufValues) : db.rpush(key, bufValues);
    }

    // LPOP/RPOP: TS SDK always returns array, but Redis returns single value for no count
    if (cmd === "lpop" || cmd === "rpop") {
      const [key, count] = args;
      const result = cmd === "lpop" ? db.lpop(key, count) : db.rpop(key, count);
      // Normalize to Redis behavior: no count = single value or null
      if (count === undefined || count === null) {
        return result && result.length > 0 ? result[0] : null;
      }
      return result;
    }

    // SET with value as buffer
    if (cmd === "set") {
      const [key, value] = args;
      return db.set(key, Buffer.from(value instanceof Buffer ? value : String(value)));
    }

    // SETEX/PSETEX with value as buffer
    if (cmd === "setex") {
      const [key, seconds, value] = args;
      return db.setex(key, seconds, Buffer.from(String(value)));
    }
    if (cmd === "psetex") {
      const [key, ms, value] = args;
      return db.psetex(key, ms, Buffer.from(String(value)));
    }

    // APPEND with value as buffer
    if (cmd === "append") {
      const [key, value] = args;
      return db.append(key, Buffer.from(String(value)));
    }

    // SETRANGE with value as buffer
    if (cmd === "setrange") {
      const [key, offset, value] = args;
      return db.setrange(key, offset, Buffer.from(String(value)));
    }

    // SISMEMBER with member as buffer
    if (cmd === "sismember") {
      const [key, member] = args;
      return db.sismember(key, Buffer.from(String(member)));
    }

    // ZSCORE with member as buffer
    if (cmd === "zscore") {
      const [key, member] = args;
      return db.zscore(key, Buffer.from(String(member)));
    }

    // ZINCRBY with member as buffer
    if (cmd === "zincrby") {
      const [key, increment, member] = args;
      return db.zincrby(key, increment, Buffer.from(String(member)));
    }

    // TYPE command
    if (cmd === "type") {
      return db.type(args[0]);
    }

    // Standard commands
    const methodMap = {
      get: db.get.bind(db),
      getdel: db.getdel.bind(db),
      strlen: db.strlen.bind(db),
      getrange: db.getrange.bind(db),
      incr: db.incr.bind(db),
      decr: db.decr.bind(db),
      incrby: db.incrby.bind(db),
      decrby: db.decrby.bind(db),
      incrbyfloat: db.incrbyfloat.bind(db),
      ttl: db.ttl.bind(db),
      pttl: db.pttl.bind(db),
      expire: db.expire.bind(db),
      pexpire: db.pexpire.bind(db),
      expireat: db.expireat.bind(db),
      pexpireat: db.pexpireat.bind(db),
      persist: db.persist.bind(db),
      rename: db.rename.bind(db),
      renamenx: db.renamenx.bind(db),
      keys: db.keys.bind(db),
      dbsize: db.dbsize.bind(db),
      flushdb: db.flushdb.bind(db),
      hget: db.hget.bind(db),
      hexists: db.hexists.bind(db),
      hlen: db.hlen.bind(db),
      hkeys: db.hkeys.bind(db),
      hvals: db.hvals.bind(db),
      hincrby: db.hincrby.bind(db),
      hgetall: db.hgetall.bind(db),
      // lpop/rpop handled above for Redis behavior normalization
      llen: db.llen.bind(db),
      lrange: db.lrange.bind(db),
      lindex: db.lindex.bind(db),
      smembers: db.smembers.bind(db),
      scard: db.scard.bind(db),
      zcard: db.zcard.bind(db),
      zcount: db.zcount.bind(db),
      zrange: db.zrange.bind(db),
      zrevrange: db.zrevrange.bind(db),
    };

    if (!(cmd in methodMap)) {
      throw new Error(`Unknown command: ${cmd}`);
    }

    return methodMap[cmd](...args);
  }

  processArg(arg) {
    if (arg && typeof arg === "object") {
      if ("bytes" in arg) {
        return Buffer.from(arg.bytes);
      }
    }
    if (Array.isArray(arg)) {
      return arg.map((a) => this.processArg(a));
    }
    return arg;
  }

  compare(actual, expected) {
    if (expected === null || expected === undefined) {
      return actual === null || actual === undefined;
    }

    if (typeof expected === "object" && !Array.isArray(expected)) {
      return this.compareSpecial(actual, expected);
    }

    if (typeof expected === "boolean") {
      return actual === expected;
    }

    if (typeof expected === "number") {
      if (Number.isInteger(expected)) {
        return actual === expected;
      }
      return Math.abs(actual - expected) < 0.001;
    }

    if (typeof expected === "string") {
      if (actual instanceof Buffer) {
        return actual.toString("utf-8") === expected;
      }
      return String(actual) === expected;
    }

    if (Array.isArray(expected)) {
      if (!Array.isArray(actual)) return false;
      if (actual.length !== expected.length) return false;
      return expected.every((e, i) => this.compare(actual[i], e));
    }

    return actual === expected;
  }

  compareSpecial(actual, expected) {
    if ("bytes" in expected) {
      const expBytes = Buffer.from(expected.bytes);
      if (actual instanceof Buffer) {
        return actual.equals(expBytes);
      }
      return false;
    }

    if ("set" in expected) {
      const expSet = new Set(expected.set);
      const actualArray = Array.isArray(actual) ? actual : [];
      const actualSet = new Set(
        actualArray.map((v) =>
          v instanceof Buffer ? v.toString("utf-8") : String(v)
        )
      );
      if (actualSet.size !== expSet.size) return false;
      for (const v of expSet) {
        if (!actualSet.has(v)) return false;
      }
      return true;
    }

    if ("dict" in expected) {
      const expDict = expected.dict;
      // hgetall returns [[field, value], ...]
      if (!Array.isArray(actual)) return false;
      const actualDict = {};
      for (const pair of actual) {
        if (Array.isArray(pair) && pair.length === 2) {
          const k = pair[0] instanceof Buffer ? pair[0].toString() : String(pair[0]);
          const v = pair[1] instanceof Buffer ? pair[1].toString() : String(pair[1]);
          actualDict[k] = v;
        }
      }
      const expKeys = Object.keys(expDict);
      const actKeys = Object.keys(actualDict);
      if (expKeys.length !== actKeys.length) return false;
      for (const k of expKeys) {
        if (actualDict[k] !== expDict[k]) return false;
      }
      return true;
    }

    if ("range" in expected) {
      const [low, high] = expected.range;
      return actual >= low && actual <= high;
    }

    if ("approx" in expected) {
      const target = expected.approx;
      const tol = expected.tol || 0.001;
      return Math.abs(actual - target) <= tol;
    }

    if ("type" in expected) {
      if (expected.type === "bytes") {
        return actual instanceof Buffer;
      }
      return true; // Simplified type check
    }

    if ("contains" in expected) {
      return String(actual).includes(expected.contains);
    }

    return false;
  }

  serialize(value) {
    if (value instanceof Buffer) {
      try {
        return value.toString("utf-8");
      } catch {
        return `<Buffer: ${Array.from(value)}>`;
      }
    }
    if (Array.isArray(value)) {
      return value.map((v) => this.serialize(v));
    }
    if (value && typeof value === "object") {
      const result = {};
      for (const [k, v] of Object.entries(value)) {
        result[this.serialize(k)] = this.serialize(v);
      }
      return result;
    }
    return value;
  }

  summary() {
    const total = this.passed + this.failed;
    return `${this.passed}/${total} passed, ${this.failed} failed`;
  }
}

// Main
function main() {
  const args = process.argv.slice(2);
  const verbose = args.includes("-v") || args.includes("--verbose");
  const specArgs = args.filter((a) => !a.startsWith("-"));

  const specDir = path.join(__dirname, "..", "spec");

  let specFiles;
  if (specArgs.length > 0) {
    specFiles = specArgs.map((s) => (path.isAbsolute(s) ? s : path.join(process.cwd(), s)));
  } else {
    specFiles = fs
      .readdirSync(specDir)
      .filter((f) => f.endsWith(".yaml"))
      .sort()
      .map((f) => path.join(specDir, f));
  }

  const runner = new OracleRunner(verbose);

  for (const specFile of specFiles) {
    runner.runSpecFile(specFile);
  }

  // Print summary
  console.log(`\n${"=".repeat(60)}`);
  console.log(`Oracle Test Results: ${runner.summary()}`);
  console.log("=".repeat(60));

  const errors = runner.errors;
  if (errors.length > 0) {
    console.log("\nFailures:");
    for (const err of errors) {
      if (err.error) {
        console.log(`  - ${err.spec} / ${err.test}: ${err.error}`);
      } else {
        console.log(`  - ${err.spec} / ${err.test} / ${err.cmd}`);
        console.log(`      Expected: ${JSON.stringify(err.expected)}`);
        console.log(`      Actual:   ${JSON.stringify(err.actual)}`);
      }
    }
    process.exit(1);
  }

  process.exit(0);
}

main();
