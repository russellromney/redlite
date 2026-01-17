//! Oracle Test Runner for Rust Core
//!
//! Executes YAML test specifications directly against the Redlite Rust core
//! as the baseline reference implementation.
//!
//! Usage:
//!   cargo run                              # Run all specs
//!   cargo run -- spec/strings.yaml         # Run single spec
//!   cargo run -- -v                        # Verbose output

use redlite::Db;
use serde::Deserialize;
use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::Path;
use std::time::Duration;

#[derive(Debug, Deserialize)]
struct Spec {
    name: String,
    #[serde(default)]
    version: String,
    tests: Vec<Test>,
}

#[derive(Debug, Deserialize)]
struct Test {
    name: String,
    #[serde(default)]
    setup: Vec<Operation>,
    operations: Vec<Operation>,
}

#[derive(Debug, Deserialize)]
struct Operation {
    cmd: String,
    #[serde(default)]
    args: Vec<serde_yaml::Value>,
    #[serde(default)]
    kwargs: HashMap<String, serde_yaml::Value>,
    expect: Option<serde_yaml::Value>,
}

#[derive(Debug)]
enum Value {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    String(String),
    Bytes(Vec<u8>),
    List(Vec<Value>),
    Set(HashSet<String>),
    Dict(HashMap<String, String>),
}

struct Runner {
    verbose: bool,
    passed: usize,
    failed: usize,
    errors: Vec<TestError>,
}

struct TestError {
    spec: String,
    test: String,
    cmd: String,
    expected: String,
    actual: String,
    error: Option<String>,
}

impl Runner {
    fn new(verbose: bool) -> Self {
        Self {
            verbose,
            passed: 0,
            failed: 0,
            errors: Vec::new(),
        }
    }

    fn run_spec_file(&mut self, spec_path: &Path) -> bool {
        let content = match fs::read_to_string(spec_path) {
            Ok(c) => c,
            Err(e) => {
                eprintln!("Error reading spec file: {}", e);
                return false;
            }
        };

        let spec: Spec = match serde_yaml::from_str(&content) {
            Ok(s) => s,
            Err(e) => {
                eprintln!("Error parsing spec file: {}", e);
                return false;
            }
        };

        let spec_name = if spec.name.is_empty() {
            spec_path.file_name().unwrap().to_string_lossy().to_string()
        } else {
            spec.name.clone()
        };

        if self.verbose {
            println!("\n{}", "=".repeat(60));
            println!("Running: {} ({} tests)", spec_name, spec.tests.len());
            println!("{}", "=".repeat(60));
        }

        for test in &spec.tests {
            self.run_test(test, &spec_name);
        }

        self.errors.is_empty()
    }

    fn run_test(&mut self, test: &Test, spec_name: &str) {
        if self.verbose {
            print!("\n  {}... ", test.name);
        }

        // Create fresh in-memory database
        let mut db = match Db::open_memory() {
            Ok(db) => db,
            Err(e) => {
                self.failed += 1;
                self.errors.push(TestError {
                    spec: spec_name.to_string(),
                    test: test.name.clone(),
                    cmd: String::new(),
                    expected: String::new(),
                    actual: String::new(),
                    error: Some(format!("Failed to open database: {}", e)),
                });
                if self.verbose {
                    println!("ERROR: {}", e);
                }
                return;
            }
        };

        // Run setup operations
        for op in &test.setup {
            if let Err(e) = self.execute_cmd(&mut db, op) {
                self.failed += 1;
                self.errors.push(TestError {
                    spec: spec_name.to_string(),
                    test: test.name.clone(),
                    cmd: op.cmd.clone(),
                    expected: String::new(),
                    actual: String::new(),
                    error: Some(format!("Setup failed: {}", e)),
                });
                if self.verbose {
                    println!("ERROR: setup {} failed: {}", op.cmd, e);
                }
                return;
            }
        }

        // Run test operations
        for op in &test.operations {
            let actual = match self.execute_cmd(&mut db, op) {
                Ok(v) => v,
                Err(e) => {
                    self.failed += 1;
                    self.errors.push(TestError {
                        spec: spec_name.to_string(),
                        test: test.name.clone(),
                        cmd: op.cmd.clone(),
                        expected: String::new(),
                        actual: String::new(),
                        error: Some(e),
                    });
                    if self.verbose {
                        println!("ERROR");
                    }
                    return;
                }
            };

            if let Some(ref expected) = op.expect {
                if !self.compare(&actual, expected) {
                    self.failed += 1;
                    self.errors.push(TestError {
                        spec: spec_name.to_string(),
                        test: test.name.clone(),
                        cmd: op.cmd.clone(),
                        expected: format!("{:?}", expected),
                        actual: format!("{:?}", actual),
                        error: None,
                    });
                    if self.verbose {
                        println!("FAILED");
                        println!("      Expected: {:?}", expected);
                        println!("      Actual:   {:?}", actual);
                    }
                    return;
                }
            }
        }

        self.passed += 1;
        if self.verbose {
            println!("PASSED");
        }
    }

    fn execute_cmd(&self, db: &mut Db, op: &Operation) -> Result<Value, String> {
        let cmd = op.cmd.to_lowercase();
        let args = &op.args;

        match cmd.as_str() {
            // String commands
            "get" => {
                let key = get_string(args, 0)?;
                match db.get(&key) {
                    Ok(Some(v)) => Ok(Value::Bytes(v)),
                    Ok(None) => Ok(Value::Null),
                    Err(e) => Err(e.to_string()),
                }
            }
            "set" => {
                let key = get_string(args, 0)?;
                let value = get_bytes(args, 1)?;
                db.set(&key, &value, None).map_err(|e| e.to_string())?;
                Ok(Value::Bool(true))
            }
            "setex" => {
                let key = get_string(args, 0)?;
                let seconds = get_i64(args, 1)?;
                let value = get_bytes(args, 2)?;
                db.set(&key, &value, Some(Duration::from_secs(seconds as u64)))
                    .map_err(|e| e.to_string())?;
                Ok(Value::Bool(true))
            }
            "psetex" => {
                let key = get_string(args, 0)?;
                let ms = get_i64(args, 1)?;
                let value = get_bytes(args, 2)?;
                db.set(&key, &value, Some(Duration::from_millis(ms as u64)))
                    .map_err(|e| e.to_string())?;
                Ok(Value::Bool(true))
            }
            "getdel" => {
                let key = get_string(args, 0)?;
                match db.getdel(&key) {
                    Ok(Some(v)) => Ok(Value::Bytes(v)),
                    Ok(None) => Ok(Value::Null),
                    Err(e) => Err(e.to_string()),
                }
            }
            "mget" => {
                let keys = get_string_list(args, 0)?;
                let key_refs: Vec<&str> = keys.iter().map(|s| s.as_str()).collect();
                let values = db.mget(&key_refs);
                let list: Vec<Value> = values
                    .into_iter()
                    .map(|v| match v {
                        Some(bytes) => Value::Bytes(bytes),
                        None => Value::Null,
                    })
                    .collect();
                Ok(Value::List(list))
            }
            "mset" => {
                let pairs: Vec<(String, Vec<u8>)> = args
                    .iter()
                    .map(|v| {
                        let arr = v.as_sequence().unwrap();
                        let key = yaml_to_string(&arr[0]);
                        let val = yaml_to_bytes(&arr[1]);
                        (key, val)
                    })
                    .collect();
                let pair_refs: Vec<(&str, &[u8])> = pairs
                    .iter()
                    .map(|(k, v)| (k.as_str(), v.as_slice()))
                    .collect();
                db.mset(&pair_refs).map_err(|e| e.to_string())?;
                Ok(Value::Bool(true))
            }
            "incr" => {
                let key = get_string(args, 0)?;
                db.incr(&key).map(Value::Int).map_err(|e| e.to_string())
            }
            "decr" => {
                let key = get_string(args, 0)?;
                db.decr(&key).map(Value::Int).map_err(|e| e.to_string())
            }
            "incrby" => {
                let key = get_string(args, 0)?;
                let amount = get_i64(args, 1)?;
                db.incrby(&key, amount)
                    .map(Value::Int)
                    .map_err(|e| e.to_string())
            }
            "decrby" => {
                let key = get_string(args, 0)?;
                let amount = get_i64(args, 1)?;
                db.decrby(&key, amount)
                    .map(Value::Int)
                    .map_err(|e| e.to_string())
            }
            "incrbyfloat" => {
                let key = get_string(args, 0)?;
                let amount = get_f64(args, 1)?;
                match db.incrbyfloat(&key, amount) {
                    Ok(s) => Ok(Value::Float(s.parse().unwrap_or(0.0))),
                    Err(e) => Err(e.to_string()),
                }
            }
            "append" => {
                let key = get_string(args, 0)?;
                let value = get_bytes(args, 1)?;
                db.append(&key, &value)
                    .map(Value::Int)
                    .map_err(|e| e.to_string())
            }
            "strlen" => {
                let key = get_string(args, 0)?;
                db.strlen(&key).map(Value::Int).map_err(|e| e.to_string())
            }
            "getrange" => {
                let key = get_string(args, 0)?;
                let start = get_i64(args, 1)?;
                let end = get_i64(args, 2)?;
                db.getrange(&key, start, end)
                    .map(Value::Bytes)
                    .map_err(|e| e.to_string())
            }
            "setrange" => {
                let key = get_string(args, 0)?;
                let offset = get_i64(args, 1)?;
                let value = get_bytes(args, 2)?;
                db.setrange(&key, offset, &value)
                    .map(Value::Int)
                    .map_err(|e| e.to_string())
            }

            // Key commands
            "del" => {
                let keys = get_string_list(args, 0)?;
                let key_refs: Vec<&str> = keys.iter().map(|s| s.as_str()).collect();
                db.del(&key_refs).map(Value::Int).map_err(|e| e.to_string())
            }
            "exists" => {
                let keys = get_string_list(args, 0)?;
                let key_refs: Vec<&str> = keys.iter().map(|s| s.as_str()).collect();
                db.exists(&key_refs)
                    .map(Value::Int)
                    .map_err(|e| e.to_string())
            }
            "type" => {
                let key = get_string(args, 0)?;
                match db.key_type(&key) {
                    Ok(Some(kt)) => Ok(Value::String(kt.as_str().to_string())),
                    Ok(None) => Ok(Value::String("none".to_string())),
                    Err(e) => Err(e.to_string()),
                }
            }
            "ttl" => {
                let key = get_string(args, 0)?;
                db.ttl(&key).map(Value::Int).map_err(|e| e.to_string())
            }
            "pttl" => {
                let key = get_string(args, 0)?;
                db.pttl(&key).map(Value::Int).map_err(|e| e.to_string())
            }
            "expire" => {
                let key = get_string(args, 0)?;
                let seconds = get_i64(args, 1)?;
                db.expire(&key, seconds)
                    .map(Value::Bool)
                    .map_err(|e| e.to_string())
            }
            "pexpire" => {
                let key = get_string(args, 0)?;
                let ms = get_i64(args, 1)?;
                db.pexpire(&key, ms)
                    .map(Value::Bool)
                    .map_err(|e| e.to_string())
            }
            "persist" => {
                let key = get_string(args, 0)?;
                db.persist(&key)
                    .map(Value::Bool)
                    .map_err(|e| e.to_string())
            }
            "expireat" => {
                let key = get_string(args, 0)?;
                let timestamp = get_timestamp_seconds(args, 1)?;
                db.expireat(&key, timestamp)
                    .map(Value::Bool)
                    .map_err(|e| e.to_string())
            }
            "pexpireat" => {
                let key = get_string(args, 0)?;
                let timestamp = get_timestamp_ms(args, 1)?;
                db.pexpireat(&key, timestamp)
                    .map(Value::Bool)
                    .map_err(|e| e.to_string())
            }
            "rename" => {
                let key = get_string(args, 0)?;
                let newkey = get_string(args, 1)?;
                db.rename(&key, &newkey).map_err(|e| e.to_string())?;
                Ok(Value::Bool(true))
            }
            "renamenx" => {
                let key = get_string(args, 0)?;
                let newkey = get_string(args, 1)?;
                db.renamenx(&key, &newkey)
                    .map(Value::Bool)
                    .map_err(|e| e.to_string())
            }
            "keys" => {
                let pattern = get_string(args, 0)?;
                match db.keys(&pattern) {
                    Ok(keys) => {
                        let set: HashSet<String> = keys.into_iter().collect();
                        Ok(Value::Set(set))
                    }
                    Err(e) => Err(e.to_string()),
                }
            }
            "dbsize" => db.dbsize().map(Value::Int).map_err(|e| e.to_string()),
            "flushdb" => {
                db.flushdb().map_err(|e| e.to_string())?;
                Ok(Value::Bool(true))
            }

            // Hash commands
            "hset" => {
                let key = get_string(args, 0)?;
                let field = get_string(args, 1)?;
                let value = get_bytes(args, 2)?;
                db.hset(&key, &[(&field, value.as_slice())])
                    .map(Value::Int)
                    .map_err(|e| e.to_string())
            }
            "hget" => {
                let key = get_string(args, 0)?;
                let field = get_string(args, 1)?;
                match db.hget(&key, &field) {
                    Ok(Some(v)) => Ok(Value::Bytes(v)),
                    Ok(None) => Ok(Value::Null),
                    Err(e) => Err(e.to_string()),
                }
            }
            "hdel" => {
                let key = get_string(args, 0)?;
                let fields = get_string_list(args, 1)?;
                let field_refs: Vec<&str> = fields.iter().map(|s| s.as_str()).collect();
                db.hdel(&key, &field_refs)
                    .map(Value::Int)
                    .map_err(|e| e.to_string())
            }
            "hexists" => {
                let key = get_string(args, 0)?;
                let field = get_string(args, 1)?;
                db.hexists(&key, &field)
                    .map(Value::Bool)
                    .map_err(|e| e.to_string())
            }
            "hlen" => {
                let key = get_string(args, 0)?;
                db.hlen(&key).map(Value::Int).map_err(|e| e.to_string())
            }
            "hkeys" => {
                let key = get_string(args, 0)?;
                match db.hkeys(&key) {
                    Ok(keys) => {
                        let set: HashSet<String> = keys.into_iter().collect();
                        Ok(Value::Set(set))
                    }
                    Err(e) => Err(e.to_string()),
                }
            }
            "hvals" => {
                let key = get_string(args, 0)?;
                match db.hvals(&key) {
                    Ok(vals) => {
                        let set: HashSet<String> = vals
                            .into_iter()
                            .map(|v| String::from_utf8_lossy(&v).to_string())
                            .collect();
                        Ok(Value::Set(set))
                    }
                    Err(e) => Err(e.to_string()),
                }
            }
            "hincrby" => {
                let key = get_string(args, 0)?;
                let field = get_string(args, 1)?;
                let amount = get_i64(args, 2)?;
                db.hincrby(&key, &field, amount)
                    .map(Value::Int)
                    .map_err(|e| e.to_string())
            }
            "hgetall" => {
                let key = get_string(args, 0)?;
                match db.hgetall(&key) {
                    Ok(pairs) => {
                        let dict: HashMap<String, String> = pairs
                            .into_iter()
                            .map(|(k, v)| (k, String::from_utf8_lossy(&v).to_string()))
                            .collect();
                        Ok(Value::Dict(dict))
                    }
                    Err(e) => Err(e.to_string()),
                }
            }
            "hmget" => {
                let key = get_string(args, 0)?;
                let fields = get_string_list(args, 1)?;
                let field_refs: Vec<&str> = fields.iter().map(|s| s.as_str()).collect();
                match db.hmget(&key, &field_refs) {
                    Ok(values) => {
                        let list: Vec<Value> = values
                            .into_iter()
                            .map(|v| match v {
                                Some(bytes) => Value::Bytes(bytes),
                                None => Value::Null,
                            })
                            .collect();
                        Ok(Value::List(list))
                    }
                    Err(e) => Err(e.to_string()),
                }
            }

            // List commands
            "lpush" => {
                let key = get_string(args, 0)?;
                let values = get_bytes_list(args, 1)?;
                let value_refs: Vec<&[u8]> = values.iter().map(|v| v.as_slice()).collect();
                db.lpush(&key, &value_refs)
                    .map(Value::Int)
                    .map_err(|e| e.to_string())
            }
            "rpush" => {
                let key = get_string(args, 0)?;
                let values = get_bytes_list(args, 1)?;
                let value_refs: Vec<&[u8]> = values.iter().map(|v| v.as_slice()).collect();
                db.rpush(&key, &value_refs)
                    .map(Value::Int)
                    .map_err(|e| e.to_string())
            }
            "lpop" => {
                let key = get_string(args, 0)?;
                let count = if args.len() > 1 {
                    Some(get_i64(args, 1)? as usize)
                } else {
                    None
                };
                match db.lpop(&key, count) {
                    Ok(values) => {
                        if count.is_none() {
                            // Single value mode
                            if values.is_empty() {
                                Ok(Value::Null)
                            } else {
                                Ok(Value::Bytes(values.into_iter().next().unwrap()))
                            }
                        } else {
                            let list: Vec<Value> = values.into_iter().map(Value::Bytes).collect();
                            Ok(Value::List(list))
                        }
                    }
                    Err(e) => Err(e.to_string()),
                }
            }
            "rpop" => {
                let key = get_string(args, 0)?;
                let count = if args.len() > 1 {
                    Some(get_i64(args, 1)? as usize)
                } else {
                    None
                };
                match db.rpop(&key, count) {
                    Ok(values) => {
                        if count.is_none() {
                            if values.is_empty() {
                                Ok(Value::Null)
                            } else {
                                Ok(Value::Bytes(values.into_iter().next().unwrap()))
                            }
                        } else {
                            let list: Vec<Value> = values.into_iter().map(Value::Bytes).collect();
                            Ok(Value::List(list))
                        }
                    }
                    Err(e) => Err(e.to_string()),
                }
            }
            "llen" => {
                let key = get_string(args, 0)?;
                db.llen(&key).map(Value::Int).map_err(|e| e.to_string())
            }
            "lrange" => {
                let key = get_string(args, 0)?;
                let start = get_i64(args, 1)?;
                let stop = get_i64(args, 2)?;
                match db.lrange(&key, start, stop) {
                    Ok(values) => {
                        let list: Vec<Value> = values.into_iter().map(Value::Bytes).collect();
                        Ok(Value::List(list))
                    }
                    Err(e) => Err(e.to_string()),
                }
            }
            "lindex" => {
                let key = get_string(args, 0)?;
                let index = get_i64(args, 1)?;
                match db.lindex(&key, index) {
                    Ok(Some(v)) => Ok(Value::Bytes(v)),
                    Ok(None) => Ok(Value::Null),
                    Err(e) => Err(e.to_string()),
                }
            }

            // Set commands
            "sadd" => {
                let key = get_string(args, 0)?;
                let members = get_bytes_list(args, 1)?;
                let member_refs: Vec<&[u8]> = members.iter().map(|v| v.as_slice()).collect();
                db.sadd(&key, &member_refs)
                    .map(Value::Int)
                    .map_err(|e| e.to_string())
            }
            "srem" => {
                let key = get_string(args, 0)?;
                let members = get_bytes_list(args, 1)?;
                let member_refs: Vec<&[u8]> = members.iter().map(|v| v.as_slice()).collect();
                db.srem(&key, &member_refs)
                    .map(Value::Int)
                    .map_err(|e| e.to_string())
            }
            "smembers" => {
                let key = get_string(args, 0)?;
                match db.smembers(&key) {
                    Ok(members) => {
                        let set: HashSet<String> = members
                            .into_iter()
                            .map(|v| String::from_utf8_lossy(&v).to_string())
                            .collect();
                        Ok(Value::Set(set))
                    }
                    Err(e) => Err(e.to_string()),
                }
            }
            "sismember" => {
                let key = get_string(args, 0)?;
                let member = get_bytes(args, 1)?;
                db.sismember(&key, &member)
                    .map(Value::Bool)
                    .map_err(|e| e.to_string())
            }
            "scard" => {
                let key = get_string(args, 0)?;
                db.scard(&key).map(Value::Int).map_err(|e| e.to_string())
            }

            // Sorted set commands
            "zadd" => {
                let key = get_string(args, 0)?;
                let members_raw = args[1].as_sequence().ok_or("Expected sequence")?;
                let members: Vec<redlite::ZMember> = members_raw
                    .iter()
                    .map(|m| {
                        let arr = m.as_sequence().unwrap();
                        let score = yaml_to_f64(&arr[0]);
                        let member = yaml_to_bytes(&arr[1]);
                        redlite::ZMember { score, member }
                    })
                    .collect();
                db.zadd(&key, &members)
                    .map(Value::Int)
                    .map_err(|e| e.to_string())
            }
            "zrem" => {
                let key = get_string(args, 0)?;
                let members = get_bytes_list(args, 1)?;
                let member_refs: Vec<&[u8]> = members.iter().map(|v| v.as_slice()).collect();
                db.zrem(&key, &member_refs)
                    .map(Value::Int)
                    .map_err(|e| e.to_string())
            }
            "zscore" => {
                let key = get_string(args, 0)?;
                let member = get_bytes(args, 1)?;
                match db.zscore(&key, &member) {
                    Ok(Some(score)) => Ok(Value::Float(score)),
                    Ok(None) => Ok(Value::Null),
                    Err(e) => Err(e.to_string()),
                }
            }
            "zcard" => {
                let key = get_string(args, 0)?;
                db.zcard(&key).map(Value::Int).map_err(|e| e.to_string())
            }
            "zcount" => {
                let key = get_string(args, 0)?;
                let min = get_f64(args, 1)?;
                let max = get_f64(args, 2)?;
                db.zcount(&key, min, max)
                    .map(Value::Int)
                    .map_err(|e| e.to_string())
            }
            "zincrby" => {
                let key = get_string(args, 0)?;
                let increment = get_f64(args, 1)?;
                let member = get_bytes(args, 2)?;
                db.zincrby(&key, increment, &member)
                    .map(Value::Float)
                    .map_err(|e| e.to_string())
            }
            "zrange" => {
                let key = get_string(args, 0)?;
                let start = get_i64(args, 1)?;
                let stop = get_i64(args, 2)?;
                match db.zrange(&key, start, stop, false) {
                    Ok(members) => {
                        let list: Vec<Value> = members.into_iter().map(|m| Value::Bytes(m.member)).collect();
                        Ok(Value::List(list))
                    }
                    Err(e) => Err(e.to_string()),
                }
            }
            "zrevrange" => {
                let key = get_string(args, 0)?;
                let start = get_i64(args, 1)?;
                let stop = get_i64(args, 2)?;
                match db.zrevrange(&key, start, stop, false) {
                    Ok(members) => {
                        let list: Vec<Value> = members.into_iter().map(|m| Value::Bytes(m.member)).collect();
                        Ok(Value::List(list))
                    }
                    Err(e) => Err(e.to_string()),
                }
            }
            "select" => {
                let db_num = get_i64(args, 0)?;
                db.select(db_num as i32).map_err(|e| e.to_string())?;
                Ok(Value::Bool(true))
            }
            "vacuum" => {
                db.vacuum()
                    .map(Value::Int)
                    .map_err(|e| e.to_string())
            }

            _ => Err(format!("Unknown command: {}", cmd)),
        }
    }

    fn compare(&self, actual: &Value, expected: &serde_yaml::Value) -> bool {
        match expected {
            serde_yaml::Value::Null => matches!(actual, Value::Null),
            serde_yaml::Value::Bool(b) => matches!(actual, Value::Bool(v) if v == b),
            serde_yaml::Value::Number(n) => {
                if let Some(i) = n.as_i64() {
                    matches!(actual, Value::Int(v) if *v == i)
                } else if let Some(f) = n.as_f64() {
                    matches!(actual, Value::Float(v) if (v - f).abs() < 0.001)
                        || matches!(actual, Value::Int(v) if *v as f64 == f)
                } else {
                    false
                }
            }
            serde_yaml::Value::String(s) => match actual {
                Value::String(v) => v == s,
                Value::Bytes(v) => String::from_utf8_lossy(v) == *s,
                _ => false,
            },
            serde_yaml::Value::Sequence(seq) => {
                if let Value::List(list) = actual {
                    if list.len() != seq.len() {
                        return false;
                    }
                    list.iter().zip(seq.iter()).all(|(a, e)| self.compare(a, e))
                } else {
                    false
                }
            }
            serde_yaml::Value::Mapping(map) => self.compare_special(actual, map),
            _ => false,
        }
    }

    fn compare_special(&self, actual: &Value, map: &serde_yaml::Mapping) -> bool {
        // Handle bytes format: {"bytes": [0, 1, 255, 128]}
        if let Some(bytes_val) = map.get(&serde_yaml::Value::String("bytes".to_string())) {
            if let (Value::Bytes(actual_bytes), serde_yaml::Value::Sequence(expected_seq)) =
                (actual, bytes_val)
            {
                let expected_bytes: Vec<u8> = expected_seq
                    .iter()
                    .map(|v| yaml_to_i64(v) as u8)
                    .collect();
                return actual_bytes == &expected_bytes;
            }
            return false;
        }

        if let Some(set_val) = map.get(&serde_yaml::Value::String("set".to_string())) {
            if let (Value::Set(actual_set), serde_yaml::Value::Sequence(expected_seq)) =
                (actual, set_val)
            {
                let expected_set: HashSet<String> = expected_seq
                    .iter()
                    .map(|v| yaml_to_string(v))
                    .collect();
                return actual_set == &expected_set;
            }
            return false;
        }

        if let Some(dict_val) = map.get(&serde_yaml::Value::String("dict".to_string())) {
            if let (Value::Dict(actual_dict), serde_yaml::Value::Mapping(expected_map)) =
                (actual, dict_val)
            {
                let expected_dict: HashMap<String, String> = expected_map
                    .iter()
                    .map(|(k, v)| (yaml_to_string(k), yaml_to_string(v)))
                    .collect();
                return actual_dict == &expected_dict;
            }
            return false;
        }

        if let Some(range_val) = map.get(&serde_yaml::Value::String("range".to_string())) {
            if let (Value::Int(v), serde_yaml::Value::Sequence(bounds)) = (actual, range_val) {
                if bounds.len() == 2 {
                    let low = yaml_to_i64(&bounds[0]);
                    let high = yaml_to_i64(&bounds[1]);
                    return *v >= low && *v <= high;
                }
            }
            return false;
        }

        if let Some(approx_val) = map.get(&serde_yaml::Value::String("approx".to_string())) {
            let target = yaml_to_f64(approx_val);
            let tol = map
                .get(&serde_yaml::Value::String("tol".to_string()))
                .map(yaml_to_f64)
                .unwrap_or(0.001);
            if let Value::Float(v) = actual {
                return (v - target).abs() <= tol;
            }
            return false;
        }

        // Handle type assertions: {"type": "integer"}, {"type": "string"}, etc.
        if let Some(type_val) = map.get(&serde_yaml::Value::String("type".to_string())) {
            if let serde_yaml::Value::String(expected_type) = type_val {
                return match expected_type.as_str() {
                    "integer" => matches!(actual, Value::Int(_)),
                    "float" => matches!(actual, Value::Float(_)),
                    "string" => matches!(actual, Value::String(_) | Value::Bytes(_)),
                    "bool" => matches!(actual, Value::Bool(_)),
                    "null" => matches!(actual, Value::Null),
                    "list" => matches!(actual, Value::List(_)),
                    "set" => matches!(actual, Value::Set(_)),
                    "dict" => matches!(actual, Value::Dict(_)),
                    _ => false,
                };
            }
            return false;
        }

        false
    }

    fn summary(&self) -> String {
        let total = self.passed + self.failed;
        format!("{}/{} passed, {} failed", self.passed, total, self.failed)
    }
}

// Helper functions
fn get_string(args: &[serde_yaml::Value], idx: usize) -> Result<String, String> {
    args.get(idx)
        .map(yaml_to_string)
        .ok_or_else(|| format!("Missing argument at index {}", idx))
}

fn get_bytes(args: &[serde_yaml::Value], idx: usize) -> Result<Vec<u8>, String> {
    args.get(idx)
        .map(yaml_to_bytes)
        .ok_or_else(|| format!("Missing argument at index {}", idx))
}

fn get_i64(args: &[serde_yaml::Value], idx: usize) -> Result<i64, String> {
    args.get(idx)
        .map(yaml_to_i64)
        .ok_or_else(|| format!("Missing argument at index {}", idx))
}

fn get_f64(args: &[serde_yaml::Value], idx: usize) -> Result<f64, String> {
    args.get(idx)
        .map(yaml_to_f64)
        .ok_or_else(|| format!("Missing argument at index {}", idx))
}

fn get_string_list(args: &[serde_yaml::Value], idx: usize) -> Result<Vec<String>, String> {
    args.get(idx)
        .and_then(|v| v.as_sequence())
        .map(|seq| seq.iter().map(yaml_to_string).collect())
        .ok_or_else(|| format!("Missing list argument at index {}", idx))
}

fn get_bytes_list(args: &[serde_yaml::Value], idx: usize) -> Result<Vec<Vec<u8>>, String> {
    args.get(idx)
        .and_then(|v| v.as_sequence())
        .map(|seq| seq.iter().map(yaml_to_bytes).collect())
        .ok_or_else(|| format!("Missing list argument at index {}", idx))
}

fn get_timestamp_seconds(args: &[serde_yaml::Value], idx: usize) -> Result<i64, String> {
    let val = args
        .get(idx)
        .ok_or_else(|| format!("Missing argument at index {}", idx))?;

    // Check if it's a special { future_seconds: N } object
    if let serde_yaml::Value::Mapping(map) = val {
        if let Some(future_val) = map.get(&serde_yaml::Value::String("future_seconds".to_string())) {
            let offset = yaml_to_i64(future_val);
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs() as i64;
            return Ok(now + offset);
        }
    }

    // Otherwise, treat as direct timestamp
    Ok(yaml_to_i64(val))
}

fn get_timestamp_ms(args: &[serde_yaml::Value], idx: usize) -> Result<i64, String> {
    let val = args
        .get(idx)
        .ok_or_else(|| format!("Missing argument at index {}", idx))?;

    // Check if it's a special { future_ms: N } object
    if let serde_yaml::Value::Mapping(map) = val {
        if let Some(future_val) = map.get(&serde_yaml::Value::String("future_ms".to_string())) {
            let offset = yaml_to_i64(future_val);
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as i64;
            return Ok(now + offset);
        }
    }

    // Otherwise, treat as direct timestamp
    Ok(yaml_to_i64(val))
}

fn yaml_to_string(v: &serde_yaml::Value) -> String {
    match v {
        serde_yaml::Value::String(s) => s.clone(),
        serde_yaml::Value::Number(n) => n.to_string(),
        serde_yaml::Value::Bool(b) => b.to_string(),
        _ => String::new(),
    }
}

fn yaml_to_bytes(v: &serde_yaml::Value) -> Vec<u8> {
    match v {
        serde_yaml::Value::String(s) => s.as_bytes().to_vec(),
        serde_yaml::Value::Number(n) => n.to_string().into_bytes(),
        serde_yaml::Value::Mapping(m) => {
            if let Some(bytes_val) = m.get(&serde_yaml::Value::String("bytes".to_string())) {
                if let serde_yaml::Value::Sequence(seq) = bytes_val {
                    return seq.iter().map(|x| yaml_to_i64(x) as u8).collect();
                }
            }
            Vec::new()
        }
        _ => Vec::new(),
    }
}

fn yaml_to_i64(v: &serde_yaml::Value) -> i64 {
    match v {
        serde_yaml::Value::Number(n) => n.as_i64().unwrap_or(0),
        _ => 0,
    }
}

fn yaml_to_f64(v: &serde_yaml::Value) -> f64 {
    match v {
        serde_yaml::Value::Number(n) => n.as_f64().unwrap_or(0.0),
        _ => 0.0,
    }
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let mut verbose = false;
    let mut spec_args: Vec<String> = Vec::new();

    for arg in args.iter().skip(1) {
        if arg == "-v" || arg == "--verbose" {
            verbose = true;
        } else {
            spec_args.push(arg.clone());
        }
    }

    // Find spec directory
    let spec_dir = if Path::new("spec").exists() {
        "spec"
    } else if Path::new("../../spec").exists() {
        "../../spec"
    } else {
        eprintln!("Could not find spec directory");
        std::process::exit(1);
    };

    // Helper to read yaml files from a directory
    fn read_yaml_files(dir: &str) -> Vec<String> {
        let mut files: Vec<String> = fs::read_dir(dir)
            .expect("Failed to read spec directory")
            .filter_map(|e| e.ok())
            .filter(|e| e.path().extension().map(|s| s == "yaml").unwrap_or(false))
            .map(|e| e.path().to_string_lossy().to_string())
            .collect();
        files.sort();
        files
    }

    let spec_files: Vec<String> = if !spec_args.is_empty() {
        // If the first argument is a directory, read yaml files from it
        if spec_args.len() == 1 && Path::new(&spec_args[0]).is_dir() {
            read_yaml_files(&spec_args[0])
        } else {
            spec_args
        }
    } else {
        read_yaml_files(spec_dir)
    };

    let mut runner = Runner::new(verbose);

    for spec_file in &spec_files {
        runner.run_spec_file(Path::new(spec_file));
    }

    // Print summary
    println!("\n{}", "=".repeat(60));
    println!("Oracle Test Results: {}", runner.summary());
    println!("{}", "=".repeat(60));

    if !runner.errors.is_empty() {
        println!("\nFailures:");
        for err in &runner.errors {
            if let Some(ref e) = err.error {
                println!("  - {} / {}: {}", err.spec, err.test, e);
            } else {
                println!("  - {} / {} / {}", err.spec, err.test, err.cmd);
                println!("      Expected: {}", err.expected);
                println!("      Actual:   {}", err.actual);
            }
        }
        std::process::exit(1);
    }

    std::process::exit(0);
}
