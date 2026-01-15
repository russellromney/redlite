#!/usr/bin/env npx ts-node
/**
 * Oracle Test Runner for TypeScript SDK.
 *
 * Executes YAML test specifications against the Redlite TypeScript SDK
 * and reports pass/fail results with detailed error messages.
 *
 * Usage:
 *   npx ts-node ts_runner.ts                    # Run all specs
 *   npx ts-node ts_runner.ts spec/strings.yaml  # Run single spec
 *   npx ts-node ts_runner.ts -v                 # Verbose output
 */

import * as fs from "fs";
import * as path from "path";
import * as yaml from "yaml";

// Import the SDK - path relative to oracle/runners/
const { RedliteDb } = require("../../redlite-ts/index.js");

interface Operation {
  cmd: string;
  args?: any[];
  kwargs?: Record<string, any>;
  expect?: any;
}

interface TestCase {
  name: string;
  setup?: Operation[];
  operations: Operation[];
}

interface Spec {
  name: string;
  version?: string;
  tests: TestCase[];
}

interface ErrorInfo {
  spec: string;
  test: string;
  cmd?: string;
  args?: any[];
  expected?: any;
  actual?: any;
  error?: string;
}

class OracleRunner {
  private verbose: boolean;
  private passed: number = 0;
  private failed: number = 0;
  private errors: ErrorInfo[] = [];

  constructor(verbose: boolean = false) {
    this.verbose = verbose;
  }

  runSpecFile(specPath: string): boolean {
    const content = fs.readFileSync(specPath, "utf-8");
    const spec: Spec = yaml.parse(content);
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

  private runTest(test: TestCase, specName: string): void {
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
    } catch (e: any) {
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

  private executeCmd(db: any, op: Operation): any {
    const cmd = op.cmd.toLowerCase();
    let args = (op.args || []).map((a) => this.processArg(a));

    // Handle API differences between generic Redis spec and TypeScript SDK

    // DEL/EXISTS take array of keys
    if (cmd === "del" || cmd === "exists") {
      // Spec: DEL [["k1", "k2"]] -> TS: del(["k1", "k2"])
      if (args.length === 1 && Array.isArray(args[0])) {
        args = args;
      }
      return cmd === "del" ? db.del(args[0]) : db.exists(args[0]);
    }

    // MGET takes array of keys
    if (cmd === "mget") {
      // Spec: MGET [["k1", "k2"]] -> TS: mget(["k1", "k2"])
      const keys = args.length === 1 && Array.isArray(args[0]) ? args[0] : args;
      return db.mget(keys);
    }

    // MSET takes array of [key, value] pairs as buffers
    if (cmd === "mset") {
      // Spec: MSET [["k1", "v1"], ["k2", "v2"]]
      // TS: mset([[Buffer, Buffer], ...])
      const pairs = args.map((pair: any[]) => {
        return [Buffer.from(String(pair[0])), Buffer.from(String(pair[1]))];
      });
      return db.mset(pairs);
    }

    // HSET takes (key, field, value)
    if (cmd === "hset") {
      // Spec: HSET ["hash", "field", "value"]
      const [key, field, value] = args;
      return db.hset(key, field, Buffer.from(String(value)));
    }

    // HDEL takes (key, fields[])
    if (cmd === "hdel") {
      // Spec: HDEL ["hash", ["f1", "f2"]]
      const [key, fields] = args;
      return db.hdel(key, Array.isArray(fields) ? fields : [fields]);
    }

    // HMGET takes (key, fields[])
    if (cmd === "hmget") {
      // Spec: HMGET ["hash", ["f1", "f2"]]
      const [key, fields] = args;
      return db.hmget(key, Array.isArray(fields) ? fields : [fields]);
    }

    // ZADD takes (key, ZMember[])
    if (cmd === "zadd") {
      // Spec: ZADD ["zset", [[1.0, "member"]]]
      const [key, members] = args;
      const zmembers = members.map((m: any[]) => ({
        score: m[0],
        member: Buffer.from(String(m[1])),
      }));
      return db.zadd(key, zmembers);
    }

    // ZREM takes (key, members[])
    if (cmd === "zrem") {
      // Spec: ZREM ["zset", ["m1", "m2"]]
      const [key, members] = args;
      return db.zrem(
        key,
        members.map((m: any) => Buffer.from(String(m)))
      );
    }

    // SADD/SREM take (key, members[])
    if (cmd === "sadd" || cmd === "srem") {
      const [key, members] = args;
      const bufMembers = (Array.isArray(members) ? members : [members]).map(
        (m: any) => Buffer.from(String(m))
      );
      return cmd === "sadd" ? db.sadd(key, bufMembers) : db.srem(key, bufMembers);
    }

    // LPUSH/RPUSH take (key, values[])
    if (cmd === "lpush" || cmd === "rpush") {
      const [key, values] = args;
      const bufValues = (Array.isArray(values) ? values : [values]).map(
        (v: any) => Buffer.from(String(v))
      );
      return cmd === "lpush" ? db.lpush(key, bufValues) : db.rpush(key, bufValues);
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

    // TYPE command is renamed to type in JS
    if (cmd === "type") {
      return db.type(args[0]);
    }

    // Standard commands
    const methodMap: Record<string, Function> = {
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
      lpop: db.lpop.bind(db),
      rpop: db.rpop.bind(db),
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

  private processArg(arg: any): any {
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

  private compare(actual: any, expected: any): boolean {
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

  private compareSpecial(actual: any, expected: Record<string, any>): boolean {
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
        actualArray.map((v: any) =>
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
      const actualDict: Record<string, string> = {};
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
      const typeMap: Record<string, string> = {
        bytes: "Buffer",
        str: "string",
        int: "number",
        float: "number",
        list: "object",
        dict: "object",
        set: "object",
      };
      if (expected.type === "bytes") {
        return actual instanceof Buffer;
      }
      return typeof actual === typeMap[expected.type];
    }

    if ("contains" in expected) {
      return String(actual).includes(expected.contains);
    }

    return false;
  }

  private serialize(value: any): any {
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
      const result: Record<string, any> = {};
      for (const [k, v] of Object.entries(value)) {
        result[this.serialize(k)] = this.serialize(v);
      }
      return result;
    }
    return value;
  }

  summary(): string {
    const total = this.passed + this.failed;
    return `${this.passed}/${total} passed, ${this.failed} failed`;
  }

  getErrors(): ErrorInfo[] {
    return this.errors;
  }
}

// Main
function main() {
  const args = process.argv.slice(2);
  const verbose = args.includes("-v") || args.includes("--verbose");
  const specArgs = args.filter((a) => !a.startsWith("-"));

  const specDir = path.join(__dirname, "..", "spec");

  let specFiles: string[];
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

  const errors = runner.getErrors();
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
