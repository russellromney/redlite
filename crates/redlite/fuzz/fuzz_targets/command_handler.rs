#![no_main]

use arbitrary::Arbitrary;
use libfuzzer_sys::fuzz_target;
use redlite::Db;

/// Represents a fuzzed Redis command
#[derive(Debug, Arbitrary)]
enum FuzzCommand {
    // String commands
    Set { key: String, value: Vec<u8> },
    Get { key: String },
    Append { key: String, value: Vec<u8> },
    Incr { key: String },
    Decr { key: String },
    IncrBy { key: String, delta: i64 },

    // List commands
    LPush { key: String, value: Vec<u8> },
    RPush { key: String, value: Vec<u8> },
    LPop { key: String },
    RPop { key: String },
    LRange { key: String, start: i64, stop: i64 },
    LLen { key: String },

    // Set commands
    SAdd { key: String, member: Vec<u8> },
    SRem { key: String, member: Vec<u8> },
    SIsMember { key: String, member: Vec<u8> },
    SMembers { key: String },
    SCard { key: String },

    // Hash commands
    HSet { key: String, field: String, value: Vec<u8> },
    HGet { key: String, field: String },
    HDel { key: String, field: String },
    HGetAll { key: String },
    HLen { key: String },
    HIncrBy { key: String, field: String, delta: i64 },

    // Sorted set commands
    ZAdd { key: String, score: f64, member: Vec<u8> },
    ZRem { key: String, member: Vec<u8> },
    ZScore { key: String, member: Vec<u8> },
    ZRange { key: String, start: i64, stop: i64 },
    ZCard { key: String },
    ZIncrBy { key: String, delta: f64, member: Vec<u8> },

    // Key commands
    Del { key: String },
    Exists { key: String },
    Type { key: String },
    Expire { key: String, seconds: i64 },
    Ttl { key: String },
    Persist { key: String },
}

fn sanitize_key(key: &str) -> String {
    // Limit key length and filter out problematic characters
    key.chars()
        .filter(|c| c.is_alphanumeric() || *c == ':' || *c == '_' || *c == '-')
        .take(128)
        .collect()
}

fn sanitize_field(field: &str) -> String {
    field.chars()
        .filter(|c| c.is_alphanumeric() || *c == '_')
        .take(64)
        .collect()
}

fn execute_command(db: &Db, cmd: FuzzCommand) {
    match cmd {
        // String commands
        FuzzCommand::Set { key, value } => {
            let key = sanitize_key(&key);
            if !key.is_empty() && value.len() < 1024 * 1024 {
                let _ = db.set(&key, &value, None);
            }
        }
        FuzzCommand::Get { key } => {
            let key = sanitize_key(&key);
            if !key.is_empty() {
                let _ = db.get(&key);
            }
        }
        FuzzCommand::Append { key, value } => {
            let key = sanitize_key(&key);
            if !key.is_empty() && value.len() < 1024 * 1024 {
                let _ = db.append(&key, &value);
            }
        }
        FuzzCommand::Incr { key } => {
            let key = sanitize_key(&key);
            if !key.is_empty() {
                let _ = db.incr(&key);
            }
        }
        FuzzCommand::Decr { key } => {
            let key = sanitize_key(&key);
            if !key.is_empty() {
                let _ = db.decr(&key);
            }
        }
        FuzzCommand::IncrBy { key, delta } => {
            let key = sanitize_key(&key);
            if !key.is_empty() {
                let _ = db.incrby(&key, delta);
            }
        }

        // List commands
        FuzzCommand::LPush { key, value } => {
            let key = sanitize_key(&key);
            if !key.is_empty() && value.len() < 1024 * 1024 {
                let _ = db.lpush(&key, &value);
            }
        }
        FuzzCommand::RPush { key, value } => {
            let key = sanitize_key(&key);
            if !key.is_empty() && value.len() < 1024 * 1024 {
                let _ = db.rpush(&key, &value);
            }
        }
        FuzzCommand::LPop { key } => {
            let key = sanitize_key(&key);
            if !key.is_empty() {
                let _ = db.lpop(&key, None);
            }
        }
        FuzzCommand::RPop { key } => {
            let key = sanitize_key(&key);
            if !key.is_empty() {
                let _ = db.rpop(&key, None);
            }
        }
        FuzzCommand::LRange { key, start, stop } => {
            let key = sanitize_key(&key);
            if !key.is_empty() {
                let _ = db.lrange(&key, start, stop);
            }
        }
        FuzzCommand::LLen { key } => {
            let key = sanitize_key(&key);
            if !key.is_empty() {
                let _ = db.llen(&key);
            }
        }

        // Set commands
        FuzzCommand::SAdd { key, member } => {
            let key = sanitize_key(&key);
            if !key.is_empty() && member.len() < 1024 * 1024 {
                let _ = db.sadd(&key, &member);
            }
        }
        FuzzCommand::SRem { key, member } => {
            let key = sanitize_key(&key);
            if !key.is_empty() {
                let _ = db.srem(&key, &member);
            }
        }
        FuzzCommand::SIsMember { key, member } => {
            let key = sanitize_key(&key);
            if !key.is_empty() {
                let _ = db.sismember(&key, &member);
            }
        }
        FuzzCommand::SMembers { key } => {
            let key = sanitize_key(&key);
            if !key.is_empty() {
                let _ = db.smembers(&key);
            }
        }
        FuzzCommand::SCard { key } => {
            let key = sanitize_key(&key);
            if !key.is_empty() {
                let _ = db.scard(&key);
            }
        }

        // Hash commands
        FuzzCommand::HSet { key, field, value } => {
            let key = sanitize_key(&key);
            let field = sanitize_field(&field);
            if !key.is_empty() && !field.is_empty() && value.len() < 1024 * 1024 {
                let _ = db.hset(&key, &field, &value);
            }
        }
        FuzzCommand::HGet { key, field } => {
            let key = sanitize_key(&key);
            let field = sanitize_field(&field);
            if !key.is_empty() && !field.is_empty() {
                let _ = db.hget(&key, &field);
            }
        }
        FuzzCommand::HDel { key, field } => {
            let key = sanitize_key(&key);
            let field = sanitize_field(&field);
            if !key.is_empty() && !field.is_empty() {
                let _ = db.hdel(&key, &field);
            }
        }
        FuzzCommand::HGetAll { key } => {
            let key = sanitize_key(&key);
            if !key.is_empty() {
                let _ = db.hgetall(&key);
            }
        }
        FuzzCommand::HLen { key } => {
            let key = sanitize_key(&key);
            if !key.is_empty() {
                let _ = db.hlen(&key);
            }
        }
        FuzzCommand::HIncrBy { key, field, delta } => {
            let key = sanitize_key(&key);
            let field = sanitize_field(&field);
            if !key.is_empty() && !field.is_empty() {
                let _ = db.hincrby(&key, &field, delta);
            }
        }

        // Sorted set commands
        FuzzCommand::ZAdd { key, score, member } => {
            let key = sanitize_key(&key);
            if !key.is_empty() && member.len() < 1024 * 1024 && score.is_finite() {
                let _ = db.zadd(&key, score, &member);
            }
        }
        FuzzCommand::ZRem { key, member } => {
            let key = sanitize_key(&key);
            if !key.is_empty() {
                let _ = db.zrem(&key, &member);
            }
        }
        FuzzCommand::ZScore { key, member } => {
            let key = sanitize_key(&key);
            if !key.is_empty() {
                let _ = db.zscore(&key, &member);
            }
        }
        FuzzCommand::ZRange { key, start, stop } => {
            let key = sanitize_key(&key);
            if !key.is_empty() {
                let _ = db.zrange(&key, start, stop);
            }
        }
        FuzzCommand::ZCard { key } => {
            let key = sanitize_key(&key);
            if !key.is_empty() {
                let _ = db.zcard(&key);
            }
        }
        FuzzCommand::ZIncrBy { key, delta, member } => {
            let key = sanitize_key(&key);
            if !key.is_empty() && delta.is_finite() {
                let _ = db.zincrby(&key, delta, &member);
            }
        }

        // Key commands
        FuzzCommand::Del { key } => {
            let key = sanitize_key(&key);
            if !key.is_empty() {
                let _ = db.del(&[key]);
            }
        }
        FuzzCommand::Exists { key } => {
            let key = sanitize_key(&key);
            if !key.is_empty() {
                let _ = db.exists(&[key]);
            }
        }
        FuzzCommand::Type { key } => {
            let key = sanitize_key(&key);
            if !key.is_empty() {
                let _ = db.key_type(&key);
            }
        }
        FuzzCommand::Expire { key, seconds } => {
            let key = sanitize_key(&key);
            if !key.is_empty() && seconds >= 0 && seconds < 3600 * 24 * 365 {
                let _ = db.expire(&key, seconds);
            }
        }
        FuzzCommand::Ttl { key } => {
            let key = sanitize_key(&key);
            if !key.is_empty() {
                let _ = db.ttl(&key);
            }
        }
        FuzzCommand::Persist { key } => {
            let key = sanitize_key(&key);
            if !key.is_empty() {
                let _ = db.persist(&key);
            }
        }
    }
}

fuzz_target!(|commands: Vec<FuzzCommand>| {
    // Limit number of commands per run
    if commands.len() > 100 {
        return;
    }

    // Create an in-memory database for each fuzz run
    let db = match Db::open_memory() {
        Ok(db) => db,
        Err(_) => return,
    };

    // Execute all commands - none should panic
    for cmd in commands {
        execute_command(&db, cmd);
    }
});
